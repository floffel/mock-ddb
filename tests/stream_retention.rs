//! Bounded stream retention: a stream keeps its most recent records instead of
//! growing with the whole write history, `clear_records` empties the log, and a
//! shard iterator keeps reading the right place across a trim.

#![cfg(feature = "streams")]

use mock_ddb::{
    item, s, CreateTableRequest, DescribeStreamRequest, Error, GetRecordsRequest,
    GetShardIteratorRequest, KeySchema, KeyType, MockDdb, PutItemRequest, ShardIteratorType,
    StreamViewType, MAX_RETAINED_STREAM_RECORDS,
};

/// A mock with one streamed table called `events`, plus that stream's ARN.
async fn streamed() -> (MockDdb, String) {
    let mock = MockDdb::new();
    let table = mock
        .create_table(
            CreateTableRequest::new("events", KeySchema::hash("pk", KeyType::String))
                .with_stream(StreamViewType::KeysOnly),
        )
        .await
        .unwrap();
    (mock, table.latest_stream_arn.unwrap())
}

/// Write `count` items, `e{from}` upwards, one put each.
async fn write(mock: &MockDdb, from: usize, count: usize) {
    for id in from..from + count {
        mock.put_item(PutItemRequest::new(
            "events",
            item([("pk", s(format!("e{id}")))]),
        ))
        .await
        .unwrap();
    }
}

/// The `pk` of one record, as the log reported it.
fn pk_of(record: &mock_ddb::StreamRecord) -> String {
    match record.keys.get("pk") {
        Some(mock_ddb::AttributeValue::S(value)) => value.clone(),
        other => panic!("one string key, got {other:?}"),
    }
}

/// The shard id of one stream.
async fn shard_id(mock: &MockDdb, arn: &str) -> String {
    mock.streams()
        .describe_stream(DescribeStreamRequest::new(arn))
        .await
        .unwrap()
        .stream_description
        .shards[0]
        .shard_id
        .clone()
}

/// One page from a fresh `TRIM_HORIZON` iterator.
async fn first_page(mock: &MockDdb, arn: &str, limit: Option<u32>) -> (String, usize, Vec<String>) {
    let iterator = mock
        .streams()
        .get_shard_iterator(GetShardIteratorRequest::new(
            arn,
            shard_id(mock, arn).await,
            ShardIteratorType::TrimHorizon,
        ))
        .await
        .unwrap();
    let request = match limit {
        Some(limit) => GetRecordsRequest::new(iterator.shard_iterator).with_limit(limit),
        None => GetRecordsRequest::new(iterator.shard_iterator),
    };
    let page = mock.streams().get_records(request).await.unwrap();
    let sequences = page
        .records
        .iter()
        .map(|record| {
            record
                .dynamodb
                .as_ref()
                .expect("the dynamodb member")
                .sequence_number
                .clone()
        })
        .collect();
    (
        page.next_shard_iterator.expect("a next iterator"),
        page.records.len(),
        sequences,
    )
}

/// A log that holds exactly the cap keeps every record: nothing is dropped
/// before the append that overflows it.
#[tokio::test]
async fn a_log_at_the_cap_keeps_every_record() {
    let (mock, arn) = streamed().await;
    write(&mock, 0, MAX_RETAINED_STREAM_RECORDS).await;

    let records = mock.get_records(&arn).await.unwrap();
    assert_eq!(records.len(), MAX_RETAINED_STREAM_RECORDS);
    assert_eq!(pk_of(&records[0]), "e0");
    assert_eq!(
        pk_of(&records[MAX_RETAINED_STREAM_RECORDS - 1]),
        format!("e{}", MAX_RETAINED_STREAM_RECORDS - 1)
    );
}

/// The append after the cap drops the oldest record and only that one, so the
/// read cost and the memory are bounded by the cap rather than by the number of
/// writes.
#[tokio::test]
async fn one_append_past_the_cap_drops_the_oldest_record() {
    let (mock, arn) = streamed().await;
    write(&mock, 0, MAX_RETAINED_STREAM_RECORDS + 1).await;

    let records = mock.get_records(&arn).await.unwrap();
    assert_eq!(records.len(), MAX_RETAINED_STREAM_RECORDS);
    assert_eq!(pk_of(&records[0]), "e1");
    assert_eq!(
        pk_of(&records[MAX_RETAINED_STREAM_RECORDS - 1]),
        format!("e{MAX_RETAINED_STREAM_RECORDS}")
    );
    // Sequence numbers keep counting across the trim: the surviving window still
    // identifies the writes it holds.
    let first = records[0].sequence_number.parse::<u64>().unwrap();
    let second = records[1].sequence_number.parse::<u64>().unwrap();
    assert_eq!(second, first + 1);
}

/// A `TRIM_HORIZON` iterator starts at the oldest record still retained, not at
/// a position the trim has removed.
#[tokio::test]
async fn trim_horizon_starts_at_the_oldest_retained_record() {
    let (mock, arn) = streamed().await;
    write(&mock, 0, 3).await;
    let untrimmed = mock.get_records(&arn).await.unwrap()[0]
        .sequence_number
        .clone();

    write(&mock, 3, MAX_RETAINED_STREAM_RECORDS).await;
    let expected = mock.get_records(&arn).await.unwrap()[0]
        .sequence_number
        .clone();
    assert_ne!(expected, untrimmed, "the trim must have moved the horizon");

    let (_, count, sequences) = first_page(&mock, &arn, Some(1)).await;
    assert_eq!(count, 1);
    assert_eq!(sequences, vec![expected]);
}

/// A shard iterator taken before a trim resumes at the oldest retained record
/// instead of reading past the position the trimmed window lost.
#[tokio::test]
async fn an_iterator_from_before_a_trim_resumes_at_the_horizon() {
    let (mock, arn) = streamed().await;
    write(&mock, 0, 2).await;
    let (next, count, _) = first_page(&mock, &arn, Some(1)).await;
    assert_eq!(count, 1);

    // Enough writes to trim the record that token names.
    write(&mock, 2, MAX_RETAINED_STREAM_RECORDS).await;

    let page = mock
        .streams()
        .get_records(GetRecordsRequest::new(next))
        .await
        .unwrap();
    assert_eq!(
        page.records.len(),
        usize::try_from(mock_ddb::MAX_RECORDS_PER_GET).unwrap()
    );

    let resumed = mock.get_records(&arn).await.unwrap();
    assert_eq!(
        page.records[0]
            .dynamodb
            .as_ref()
            .expect("the dynamodb member")
            .sequence_number,
        resumed[0].sequence_number
    );
}

/// `LATEST` follows the log's end across a trim: it names a position one past
/// the newest record, so it stays empty until the next write.
#[tokio::test]
async fn latest_follows_the_end_across_a_trim() {
    let (mock, arn) = streamed().await;
    write(&mock, 0, MAX_RETAINED_STREAM_RECORDS + 5).await;

    let iterator = mock
        .streams()
        .get_shard_iterator(GetShardIteratorRequest::new(
            &arn,
            shard_id(&mock, &arn).await,
            ShardIteratorType::Latest,
        ))
        .await
        .unwrap();
    let empty = mock
        .streams()
        .get_records(GetRecordsRequest::new(iterator.shard_iterator))
        .await
        .unwrap();
    assert!(empty.records.is_empty());

    let tail = empty.next_shard_iterator.expect("a next iterator");
    write(&mock, 100, 1).await;
    let page = mock
        .streams()
        .get_records(GetRecordsRequest::new(tail))
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);
}

/// `clear_records` empties the log without resetting the sequence numbers, and
/// a token that named a cleared record reads nothing.
#[tokio::test]
async fn clear_records_empties_the_log() {
    let (mock, arn) = streamed().await;
    write(&mock, 0, 2).await;
    let (next, count, _) = first_page(&mock, &arn, Some(1)).await;
    assert_eq!(count, 1);
    let last_before = mock.get_records(&arn).await.unwrap()[1]
        .sequence_number
        .clone();

    mock.clear_records(&arn).unwrap();
    assert!(mock.get_records(&arn).await.unwrap().is_empty());

    let stale = mock
        .streams()
        .get_records(GetRecordsRequest::new(next))
        .await
        .unwrap();
    assert!(
        stale.records.is_empty(),
        "a cleared record must not come back: {:?}",
        stale.records
    );

    write(&mock, 50, 1).await;
    let after = mock.get_records(&arn).await.unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(pk_of(&after[0]), "e50");
    assert!(
        after[0].sequence_number > last_before,
        "sequence numbers must keep running: {} vs {last_before}",
        after[0].sequence_number
    );
}

/// Clearing a stream that does not exist is the same `ResourceNotFound` every
/// other unknown-stream call answers.
#[tokio::test]
async fn clear_records_rejects_an_unknown_stream() {
    let (mock, _) = streamed().await;
    let error = mock
        .clear_records("arn:aws:dynamodb:ddb-local:000000000000:table/nope/stream/0")
        .unwrap_err();
    assert!(
        matches!(error, Error::ResourceNotFound { .. }),
        "expected ResourceNotFound, got {error:?}"
    );
}
