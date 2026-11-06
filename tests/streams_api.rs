//! The DynamoDB Streams wire API: `ListStreams`, `DescribeStream`,
//! `GetShardIterator` and `GetRecords`, including the limits of each.

#![cfg(feature = "streams")]

use std::collections::HashSet;

use mock_ddb::{
    item, n, s, CreateTableRequest, DeleteItemRequest, DescribeStreamRequest, Error,
    GetRecordsRequest, GetShardIteratorRequest, Item, KeySchema, KeyType, ListStreamsRequest,
    MockDdb, PutItemRequest, ShardIteratorType, StreamViewType, MAX_RECORDS_PER_GET,
    MAX_SHARDS_PER_DESCRIBE, MAX_STREAMS_PER_LIST, STREAM_CREATION_EPOCH_SECONDS, STREAM_REGION,
};

/// A mock holding `count` stream-enabled tables named `table_1`, `table_2`, …
/// and returning their ARNs in table order.
async fn streamed(count: usize, view_type: StreamViewType) -> (MockDdb, Vec<String>) {
    let mock = MockDdb::new();
    let mut arns = Vec::new();
    for index in 1..=count {
        let name = format!("table_{index}");
        let table = mock
            .create_table(
                CreateTableRequest::new(&name, KeySchema::hash("pk", KeyType::String))
                    .with_stream(view_type),
            )
            .await
            .unwrap();
        arns.push(table.latest_stream_arn.expect("a stream ARN"));
    }
    (mock, arns)
}

/// One table with a stream, its ARN and its only shard id.
async fn one_stream(view_type: StreamViewType) -> (MockDdb, String, String) {
    let (mock, arns) = streamed(1, view_type).await;
    let arn = arns[0].clone();
    let described = mock
        .streams()
        .describe_stream(DescribeStreamRequest::new(&arn))
        .await
        .unwrap();
    let shard_id = described.stream_description.shards[0].shard_id.clone();
    (mock, arn, shard_id)
}

fn user(id: &str) -> Item {
    item([("pk", s(id))])
}

async fn put(mock: &MockDdb, id: &str) {
    mock.put_item(PutItemRequest::new("table_1", user(id)))
        .await
        .unwrap();
}

#[tokio::test]
async fn list_streams_reports_every_stream_with_its_label() {
    let (mock, arns) = streamed(2, StreamViewType::KeysOnly).await;

    let page = mock
        .streams()
        .list_streams(ListStreamsRequest::new())
        .await
        .unwrap();
    assert_eq!(
        page.streams
            .iter()
            .map(|stream| stream.stream_arn.clone())
            .collect::<Vec<_>>(),
        arns
    );
    assert_eq!(page.streams[0].table_name, "table_1");
    assert_eq!(page.streams[0].stream_label, "2026-01-01T00:00:00.000");
    assert_eq!(page.streams[0].stream_status, "ENABLED");
    assert!(page.last_evaluated_stream_arn.is_none());
}

#[tokio::test]
async fn list_streams_pages_with_the_last_evaluated_arn() {
    let (mock, arns) = streamed(3, StreamViewType::KeysOnly).await;
    let streams = mock.streams();

    let first = streams
        .list_streams(ListStreamsRequest::new().with_limit(2))
        .await
        .unwrap();
    assert_eq!(first.streams.len(), 2);
    assert_eq!(
        first.last_evaluated_stream_arn.as_deref(),
        Some(arns[1].as_str())
    );

    let second = streams
        .list_streams(
            ListStreamsRequest::new()
                .with_exclusive_start_stream_arn(arns[1].clone())
                .with_limit(2),
        )
        .await
        .unwrap();
    assert_eq!(second.streams.len(), 1);
    // The last page ends the listing, so there is no cursor.
    assert!(second.last_evaluated_stream_arn.is_none());
}

#[tokio::test]
async fn list_streams_accepts_the_limit_ceiling_and_rejects_one_past_it() {
    let (mock, _) = streamed(1, StreamViewType::KeysOnly).await;
    let streams = mock.streams();

    let at_ceiling = streams
        .list_streams(ListStreamsRequest::new().with_limit(MAX_STREAMS_PER_LIST))
        .await
        .unwrap();
    assert_eq!(at_ceiling.streams.len(), 1);

    let past_ceiling = streams
        .list_streams(ListStreamsRequest::new().with_limit(MAX_STREAMS_PER_LIST + 1))
        .await
        .unwrap_err();
    assert_eq!(past_ceiling.code(), "ValidationException");

    let zero = streams
        .list_streams(ListStreamsRequest::new().with_limit(0))
        .await
        .unwrap_err();
    assert_eq!(zero.code(), "ValidationException");
}

#[tokio::test]
async fn list_streams_rejects_an_unknown_cursor() {
    let (mock, _) = streamed(1, StreamViewType::KeysOnly).await;
    let error = mock
        .streams()
        .list_streams(
            ListStreamsRequest::new().with_exclusive_start_stream_arn("arn:aws:dynamodb:ddb-local"),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "ValidationException");
}

#[tokio::test]
async fn describe_stream_reports_the_shard_and_the_view_type() {
    let (mock, arn, shard_id) = one_stream(StreamViewType::NewAndOldImages).await;
    put(&mock, "u#1").await;

    let description = mock
        .streams()
        .describe_stream(DescribeStreamRequest::new(&arn))
        .await
        .unwrap()
        .stream_description;
    assert_eq!(description.stream_arn, arn);
    assert_eq!(description.stream_label, "2026-01-01T00:00:00.000");
    assert_eq!(description.stream_status, "ENABLED");
    assert_eq!(description.table_name, "table_1");
    assert_eq!(
        description.stream_view_type,
        StreamViewType::NewAndOldImages
    );
    assert_eq!(
        description.creation_request_date_time,
        STREAM_CREATION_EPOCH_SECONDS
    );
    assert_eq!(description.shards.len(), 1);
    assert_eq!(description.shards[0].shard_id, shard_id);
    assert!(description.shards[0].parent_shard_id.is_none());
    assert!(description.shards[0]
        .sequence_number_range
        .ending_sequence_number
        .is_none());
    // The shard starts where the first record sits.
    let first = mock.get_records(&arn).await.unwrap();
    assert_eq!(
        description.shards[0]
            .sequence_number_range
            .starting_sequence_number,
        first[0].sequence_number
    );
    assert!(description.last_evaluated_shard_id.is_none());
}

#[tokio::test]
async fn describe_stream_past_its_only_shard_returns_an_empty_page() {
    let (mock, arn, shard_id) = one_stream(StreamViewType::KeysOnly).await;
    let page = mock
        .streams()
        .describe_stream(
            DescribeStreamRequest::new(&arn).with_exclusive_start_shard_id(shard_id.clone()),
        )
        .await
        .unwrap();
    assert_eq!(page.stream_description.shards, []);
    assert!(page.stream_description.last_evaluated_shard_id.is_none());

    let unknown = mock
        .streams()
        .describe_stream(
            DescribeStreamRequest::new(&arn).with_exclusive_start_shard_id("shardId-0000-9999"),
        )
        .await
        .unwrap_err();
    assert_eq!(unknown.code(), "ValidationException");
}

#[tokio::test]
async fn describe_stream_checks_its_limit_and_the_stream_arn() {
    let (mock, arn, _) = one_stream(StreamViewType::KeysOnly).await;
    let streams = mock.streams();

    let at_ceiling = streams
        .describe_stream(DescribeStreamRequest::new(&arn).with_limit(MAX_SHARDS_PER_DESCRIBE))
        .await
        .unwrap();
    assert_eq!(at_ceiling.stream_description.shards.len(), 1);

    let past_ceiling = streams
        .describe_stream(DescribeStreamRequest::new(&arn).with_limit(MAX_SHARDS_PER_DESCRIBE + 1))
        .await
        .unwrap_err();
    assert_eq!(past_ceiling.code(), "ValidationException");

    let missing = streams
        .describe_stream(DescribeStreamRequest::new(
            "arn:aws:dynamodb:ddb-local:000000000000:table/nothing/stream/2026-01-01T00:00:00.000",
        ))
        .await
        .unwrap_err();
    assert_eq!(missing.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn trim_horizon_reads_the_whole_log_and_latest_only_what_follows() {
    let (mock, arn, shard_id) = one_stream(StreamViewType::NewImage).await;
    put(&mock, "u#1").await;
    put(&mock, "u#2").await;
    let streams = mock.streams();

    let horizon = streams
        .get_shard_iterator(GetShardIteratorRequest::new(
            &arn,
            shard_id.clone(),
            ShardIteratorType::TrimHorizon,
        ))
        .await
        .unwrap();
    let page = streams
        .get_records(GetRecordsRequest::new(horizon.shard_iterator))
        .await
        .unwrap();
    assert_eq!(page.records.len(), 2);

    let latest = streams
        .get_shard_iterator(GetShardIteratorRequest::new(
            &arn,
            shard_id,
            ShardIteratorType::Latest,
        ))
        .await
        .unwrap();
    let empty = streams
        .get_records(GetRecordsRequest::new(latest.shard_iterator.clone()))
        .await
        .unwrap();
    assert_eq!(empty.records, []);

    put(&mock, "u#3").await;
    let after_write = streams
        .get_records(GetRecordsRequest::new(latest.shard_iterator))
        .await
        .unwrap();
    assert_eq!(after_write.records.len(), 1);
    assert_eq!(
        after_write.records[0]
            .dynamodb
            .as_ref()
            .unwrap()
            .keys
            .get("pk"),
        Some(&s("u#3"))
    );
}

#[tokio::test]
async fn at_and_after_sequence_number_differ_by_one_record() {
    let (mock, arn, shard_id) = one_stream(StreamViewType::KeysOnly).await;
    for id in ["u#1", "u#2", "u#3"] {
        put(&mock, id).await;
    }
    let sequences: Vec<String> = mock
        .get_records(&arn)
        .await
        .unwrap()
        .into_iter()
        .map(|record| record.sequence_number)
        .collect();
    let streams = mock.streams();

    let at = streams
        .get_shard_iterator(
            GetShardIteratorRequest::new(
                &arn,
                shard_id.clone(),
                ShardIteratorType::AtSequenceNumber,
            )
            .with_sequence_number(sequences[1].clone()),
        )
        .await
        .unwrap();
    let at_page = streams
        .get_records(GetRecordsRequest::new(at.shard_iterator.clone()))
        .await
        .unwrap();
    assert_eq!(at_page.records.len(), 2);
    assert_eq!(
        at_page.records[0]
            .dynamodb
            .as_ref()
            .unwrap()
            .sequence_number,
        sequences[1]
    );

    let after = streams
        .get_shard_iterator(
            GetShardIteratorRequest::new(
                &arn,
                shard_id.clone(),
                ShardIteratorType::AfterSequenceNumber,
            )
            .with_sequence_number(sequences[1].clone()),
        )
        .await
        .unwrap();
    let after_page = streams
        .get_records(GetRecordsRequest::new(after.shard_iterator))
        .await
        .unwrap();
    assert_eq!(after_page.records.len(), 1);
    assert_eq!(
        after_page.records[0]
            .dynamodb
            .as_ref()
            .unwrap()
            .sequence_number,
        sequences[2]
    );

    // An unpadded number finds the same record: the padding is presentation.
    let unpadded = sequences[1].trim_start_matches('0').to_string();
    let at_unpadded = streams
        .get_shard_iterator(
            GetShardIteratorRequest::new(&arn, shard_id, ShardIteratorType::AtSequenceNumber)
                .with_sequence_number(unpadded),
        )
        .await
        .unwrap();
    assert_eq!(at_unpadded, at);
}

#[tokio::test]
async fn iterator_requests_are_validated_before_anything_is_read() {
    let (mock, arn, shard_id) = one_stream(StreamViewType::KeysOnly).await;
    put(&mock, "u#1").await;
    let streams = mock.streams();

    let missing_sequence = streams
        .get_shard_iterator(GetShardIteratorRequest::new(
            &arn,
            shard_id.clone(),
            ShardIteratorType::AtSequenceNumber,
        ))
        .await
        .unwrap_err();
    assert_eq!(missing_sequence.code(), "ValidationException");

    let unexpected_sequence = streams
        .get_shard_iterator(
            GetShardIteratorRequest::new(&arn, shard_id.clone(), ShardIteratorType::TrimHorizon)
                .with_sequence_number("1"),
        )
        .await
        .unwrap_err();
    assert_eq!(unexpected_sequence.code(), "ValidationException");

    let unknown_sequence = streams
        .get_shard_iterator(
            GetShardIteratorRequest::new(
                &arn,
                shard_id.clone(),
                ShardIteratorType::AtSequenceNumber,
            )
            .with_sequence_number("999999"),
        )
        .await
        .unwrap_err();
    assert_eq!(unknown_sequence.code(), "ValidationException");

    let unknown_shard = streams
        .get_shard_iterator(GetShardIteratorRequest::new(
            &arn,
            "shardId-000000000000-ffffffff",
            ShardIteratorType::TrimHorizon,
        ))
        .await
        .unwrap_err();
    assert_eq!(unknown_shard.code(), "ValidationException");

    let unknown_stream = streams
        .get_shard_iterator(GetShardIteratorRequest::new(
            "arn:aws:dynamodb:ddb-local:000000000000:table/nothing/stream/label",
            shard_id,
            ShardIteratorType::TrimHorizon,
        ))
        .await
        .unwrap_err();
    assert_eq!(unknown_stream.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn get_records_pages_with_the_limit_and_chains_the_next_iterator() {
    let (mock, arn, shard_id) = one_stream(StreamViewType::NewImage).await;
    for id in ["u#1", "u#2", "u#3"] {
        put(&mock, id).await;
    }
    let streams = mock.streams();
    let iterator = streams
        .get_shard_iterator(GetShardIteratorRequest::new(
            &arn,
            shard_id,
            ShardIteratorType::TrimHorizon,
        ))
        .await
        .unwrap()
        .shard_iterator;

    let first = streams
        .get_records(GetRecordsRequest::new(iterator).with_limit(1))
        .await
        .unwrap();
    assert_eq!(first.records.len(), 1);
    let second = streams
        .get_records(GetRecordsRequest::new(
            first.next_shard_iterator.clone().unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(second.records.len(), 2);
    assert_ne!(first.records[0].event_id, second.records[0].event_id);

    let tail = streams
        .get_records(GetRecordsRequest::new(second.next_shard_iterator.unwrap()))
        .await
        .unwrap();
    assert_eq!(tail.records, []);
    assert!(tail.next_shard_iterator.is_some());
}

#[tokio::test]
async fn event_ids_are_disjoint_across_two_tables_writing_in_the_same_millisecond() {
    // Two tables write interleaved, so both streams count through the same
    // sequence values in the same millisecond: the ids must still be distinct,
    // or a consumer that deduplicates on `event_id` drops one of the writes.
    let (mock, arns) = streamed(2, StreamViewType::KeysOnly).await;
    let tables = ["table_1", "table_2"];
    let mut written = 0;
    for round in 0..8 {
        for table in tables {
            mock.put_item(PutItemRequest::new(
                table,
                item([("pk", s(format!("{table}#{round}")))]),
            ))
            .await
            .unwrap();
            written += 1;
        }
    }

    let mut ids = HashSet::new();
    let mut sequences: Vec<HashSet<String>> = Vec::new();
    for arn in &arns {
        let records = mock.get_records(arn).await.unwrap();
        assert_eq!(records.len(), written / tables.len());
        for record in &records {
            assert!(
                ids.insert(record.event_id.clone()),
                "duplicate event id {}",
                record.event_id
            );
        }
        sequences.push(
            records
                .iter()
                .map(|record| record.sequence_number.clone())
                .collect(),
        );
    }
    assert_eq!(ids.len(), written);

    // The premise of the test: the two streams issue the same sequence
    // numbers, so the sequence number alone cannot tell their records apart.
    assert!(
        sequences[0].intersection(&sequences[1]).next().is_some(),
        "the two streams must cover the same sequence numbers"
    );

    // The wire shape carries the same ids, so a Lambda event source mapping
    // sees the disjoint set too.
    let streams = mock.streams();
    let described = streams
        .describe_stream(DescribeStreamRequest::new(&arns[1]))
        .await
        .unwrap();
    let iterator = streams
        .get_shard_iterator(GetShardIteratorRequest::new(
            &arns[1],
            described.stream_description.shards[0].shard_id.clone(),
            ShardIteratorType::TrimHorizon,
        ))
        .await
        .unwrap()
        .shard_iterator;
    let page = streams
        .get_records(GetRecordsRequest::new(iterator))
        .await
        .unwrap();
    assert_eq!(page.records.len(), written / tables.len());
    let first_stream_ids: HashSet<String> = mock
        .get_records(&arns[0])
        .await
        .unwrap()
        .into_iter()
        .map(|record| record.event_id)
        .collect();
    for record in page.records {
        assert!(ids.contains(&record.event_id));
        assert!(
            !first_stream_ids.contains(&record.event_id),
            "the wire record repeats an id the other stream issued"
        );
    }
}

#[tokio::test]
async fn get_records_accepts_the_limit_ceiling_and_rejects_one_past_it() {
    let (mock, arn, shard_id) = one_stream(StreamViewType::KeysOnly).await;
    put(&mock, "u#1").await;
    let streams = mock.streams();
    let iterator = streams
        .get_shard_iterator(GetShardIteratorRequest::new(
            &arn,
            shard_id,
            ShardIteratorType::TrimHorizon,
        ))
        .await
        .unwrap()
        .shard_iterator;

    let at_ceiling = streams
        .get_records(GetRecordsRequest::new(iterator.clone()).with_limit(MAX_RECORDS_PER_GET))
        .await
        .unwrap();
    assert_eq!(at_ceiling.records.len(), 1);

    let past_ceiling = streams
        .get_records(GetRecordsRequest::new(iterator.clone()).with_limit(MAX_RECORDS_PER_GET + 1))
        .await
        .unwrap_err();
    assert_eq!(past_ceiling.code(), "ValidationException");

    let zero = streams
        .get_records(GetRecordsRequest::new(iterator).with_limit(0))
        .await
        .unwrap_err();
    assert_eq!(zero.code(), "ValidationException");
}

#[tokio::test]
async fn get_records_rejects_a_token_the_mock_did_not_issue() {
    let (mock, _) = streamed(1, StreamViewType::KeysOnly).await;
    let streams = mock.streams();

    for token in [
        "",
        "not-a-token",
        "v1|arn|shardId-000000000000-00000000|nope",
    ] {
        let error = streams
            .get_records(GetRecordsRequest::new(token))
            .await
            .unwrap_err();
        assert_eq!(error.code(), "ValidationException", "token {token:?}");
    }
}

#[tokio::test]
async fn a_deleted_stream_takes_its_iterators_with_it() {
    let (mock, arn, shard_id) = one_stream(StreamViewType::KeysOnly).await;
    put(&mock, "u#1").await;
    let iterator = mock
        .streams()
        .get_shard_iterator(GetShardIteratorRequest::new(
            &arn,
            shard_id,
            ShardIteratorType::TrimHorizon,
        ))
        .await
        .unwrap()
        .shard_iterator;

    mock.delete_table("table_1").await.unwrap();
    assert_eq!(mock.list_streams(), Vec::<String>::new());

    let error = mock
        .streams()
        .get_records(GetRecordsRequest::new(iterator))
        .await
        .unwrap_err();
    assert_eq!(error.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn records_carry_the_kinesis_adapter_shape() {
    let (mock, arn, shard_id) = one_stream(StreamViewType::NewAndOldImages).await;
    put(&mock, "u#1").await;
    mock.put_item(PutItemRequest::new(
        "table_1",
        item([("pk", s("u#1")), ("total", n("42"))]),
    ))
    .await
    .unwrap();
    mock.delete_item(DeleteItemRequest::new("table_1", user("u#1")))
        .await
        .unwrap();

    let page = mock
        .streams()
        .get_records(GetRecordsRequest::new(
            mock.streams()
                .get_shard_iterator(GetShardIteratorRequest::new(
                    &arn,
                    shard_id,
                    ShardIteratorType::TrimHorizon,
                ))
                .await
                .unwrap()
                .shard_iterator,
        ))
        .await
        .unwrap();
    assert_eq!(page.records.len(), 3);

    let insert = &page.records[0];
    assert_eq!(insert.event_name.as_str(), "INSERT");
    assert_eq!(insert.event_source, "aws:dynamodb");
    assert_eq!(insert.event_version, "1.1");
    assert_eq!(insert.aws_region, STREAM_REGION);
    let data = insert.dynamodb.as_ref().expect("the dynamodb member");
    assert_eq!(data.keys, user("u#1"));
    assert_eq!(data.new_image, Some(user("u#1")));
    assert!(data.old_image.is_none());
    assert_eq!(data.stream_view_type, StreamViewType::NewAndOldImages);
    // `pk` + `u#1` in the keys, then the same again in the new image.
    assert_eq!(data.size_bytes, 10);
    assert!(data.approximate_creation_date_time > 0);
    assert_eq!(data.sequence_number.len(), 21);

    let remove = &page.records[2];
    assert_eq!(remove.event_name.as_str(), "REMOVE");
    let removed = remove.dynamodb.as_ref().unwrap();
    assert!(removed.new_image.is_none());
    assert_eq!(
        removed.old_image,
        Some(item([("pk", s("u#1")), ("total", n("42"))]))
    );
}

#[tokio::test]
async fn keys_only_records_carry_no_images() {
    let (mock, arn, shard_id) = one_stream(StreamViewType::KeysOnly).await;
    put(&mock, "u#1").await;

    let page = mock
        .streams()
        .get_records(GetRecordsRequest::new(
            mock.streams()
                .get_shard_iterator(GetShardIteratorRequest::new(
                    &arn,
                    shard_id,
                    ShardIteratorType::TrimHorizon,
                ))
                .await
                .unwrap()
                .shard_iterator,
        ))
        .await
        .unwrap();
    let data = page.records[0].dynamodb.as_ref().unwrap();
    assert_eq!(data.keys, user("u#1"));
    assert!(data.new_image.is_none());
    assert!(data.old_image.is_none());
    assert_eq!(data.size_bytes, 5);
}

#[tokio::test]
async fn thirty_two_tasks_read_one_shared_stream() {
    let (mock, arn, shard_id) = one_stream(StreamViewType::NewImage).await;
    for index in 0..5 {
        put(&mock, &format!("u#{index}")).await;
    }
    let iterator = mock
        .streams()
        .get_shard_iterator(GetShardIteratorRequest::new(
            &arn,
            shard_id,
            ShardIteratorType::TrimHorizon,
        ))
        .await
        .unwrap()
        .shard_iterator;

    let mut tasks = Vec::new();
    for _ in 0..32 {
        let mock = mock.clone();
        let iterator = iterator.clone();
        tasks.push(tokio::spawn(async move {
            let listed = mock
                .streams()
                .list_streams(ListStreamsRequest::new())
                .await
                .unwrap();
            let page = mock
                .streams()
                .get_records(GetRecordsRequest::new(iterator))
                .await
                .unwrap();
            (listed.streams.len(), page.records.len())
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), (1, 5));
    }
}

#[tokio::test]
async fn errors_keep_the_service_class() {
    let (mock, _) = streamed(0, StreamViewType::KeysOnly).await;
    let empty = mock
        .streams()
        .list_streams(ListStreamsRequest::new())
        .await
        .unwrap();
    assert_eq!(empty.streams, []);

    let error = mock
        .streams()
        .describe_stream(DescribeStreamRequest::new("arn:aws:dynamodb:ddb-local"))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::ResourceNotFound { .. }));
    assert!(!error.is_retryable());
}
