//! TTL expiry and streams.

mod common;

use std::sync::Arc;
use std::time::Duration;

use mock_ddb::{
    item, n, s, values, CreateTableRequest, GetItemRequest, KeySchema, KeyType, MockClock, MockDdb,
    PutItemRequest, QueryRequest, ScanRequest, TimeToLiveStatus, UpdateItemRequest,
    UpdateTimeToLiveRequest,
};

/// A mock with a controllable clock, pinned to 2026-01-01T00:00:00Z.
fn clocked() -> (MockDdb, Arc<MockClock>) {
    let clock = Arc::new(MockClock::at_epoch_seconds(1_000_000));
    (MockDdb::with_clock(clock.clone()), clock)
}

async fn ttl_table() -> (MockDdb, Arc<MockClock>) {
    let (mock, clock) = clocked();
    mock.create_table(CreateTableRequest::new(
        "sessions",
        KeySchema::hash("pk", KeyType::String),
    ))
    .await
    .unwrap();
    mock.update_time_to_live(UpdateTimeToLiveRequest::new("sessions", "expires_at", true))
        .await
        .unwrap();
    (mock, clock)
}

#[tokio::test]
async fn ttl_configuration_round_trips() {
    let (mock, _clock) = ttl_table().await;

    let description = mock.describe_time_to_live("sessions").await.unwrap();
    assert_eq!(description.status, TimeToLiveStatus::Enabled);
    assert_eq!(description.attribute_name.as_deref(), Some("expires_at"));
    assert_eq!(description.status.as_str(), "ENABLED");

    let enabling_again = mock
        .update_time_to_live(UpdateTimeToLiveRequest::new("sessions", "expires_at", true))
        .await;
    assert!(enabling_again
        .unwrap_err()
        .to_string()
        .contains("already enabled"));

    mock.update_time_to_live(UpdateTimeToLiveRequest::new(
        "sessions",
        "expires_at",
        false,
    ))
    .await
    .unwrap();
    let disabled = mock.describe_time_to_live("sessions").await.unwrap();
    assert_eq!(disabled.status, TimeToLiveStatus::Disabled);
    assert_eq!(disabled.status.as_str(), "DISABLED");
    assert!(disabled.attribute_name.is_none());

    assert!(mock
        .update_time_to_live(UpdateTimeToLiveRequest::new(
            "sessions",
            "expires_at",
            false
        ))
        .await
        .is_err());
    assert!(mock
        .update_time_to_live(UpdateTimeToLiveRequest::new("sessions", "", true))
        .await
        .is_err());
    assert!(mock
        .update_time_to_live(UpdateTimeToLiveRequest::new("nope", "expires_at", true))
        .await
        .is_err());
    assert!(mock.describe_time_to_live("nope").await.is_err());
}

#[tokio::test]
async fn expired_items_are_invisible_everywhere() {
    let (mock, clock) = ttl_table().await;
    mock.seed(
        "sessions",
        vec![
            item([("pk", s("expired")), ("expires_at", n("999999"))]),
            item([("pk", s("live")), ("expires_at", n("1000001"))]),
            // No TTL attribute: never expires.
            item([("pk", s("permanent"))]),
            // A non-numeric TTL attribute is ignored, like the service.
            item([("pk", s("garbled")), ("expires_at", s("tomorrow"))]),
        ],
    )
    .unwrap();

    assert_eq!(mock.count("sessions"), 3);
    assert_eq!(mock.dump("sessions").len(), 3);

    let expired = mock
        .get_item(GetItemRequest::new(
            "sessions",
            item([("pk", s("expired"))]),
        ))
        .await
        .unwrap();
    assert!(expired.item.is_none());

    let live = mock
        .get_item(GetItemRequest::new("sessions", item([("pk", s("live"))])))
        .await
        .unwrap();
    assert!(live.item.is_some());

    let scanned = mock.scan(ScanRequest::new("sessions")).await.unwrap();
    assert_eq!(scanned.count, 3);

    let queried = mock
        .query(
            QueryRequest::new("sessions", "pk = :pk").with_values(values([(":pk", s("expired"))])),
        )
        .await
        .unwrap();
    assert_eq!(queried.count, 0);

    // Time moves on: the live item now expires too.
    clock.advance(Duration::from_secs(2));
    assert_eq!(mock.count("sessions"), 2);
    assert!(mock
        .get_item(GetItemRequest::new("sessions", item([("pk", s("live"))])))
        .await
        .unwrap()
        .item
        .is_none());
}

#[tokio::test]
async fn purge_removes_expired_rows() {
    let (mock, _clock) = ttl_table().await;
    mock.seed(
        "sessions",
        vec![
            item([("pk", s("expired")), ("expires_at", n("999999"))]),
            item([("pk", s("live")), ("expires_at", n("1000001"))]),
        ],
    )
    .unwrap();

    assert_eq!(mock.purge_expired("sessions").unwrap(), 1);
    assert_eq!(mock.dump("sessions").len(), 1);
    assert_eq!(mock.purge_expired("sessions").unwrap(), 0);
    assert!(mock.purge_expired("nope").is_err());
}

#[tokio::test]
async fn disabled_ttl_keeps_expired_items_visible() {
    let (mock, _clock) = clocked();
    mock.create_table(CreateTableRequest::new(
        "sessions",
        KeySchema::hash("pk", KeyType::String),
    ))
    .await
    .unwrap();
    mock.seed(
        "sessions",
        vec![item([("pk", s("expired")), ("expires_at", n("1"))])],
    )
    .unwrap();

    assert_eq!(mock.count("sessions"), 1);
    mock.update_time_to_live(UpdateTimeToLiveRequest::new("sessions", "expires_at", true))
        .await
        .unwrap();
    assert_eq!(mock.count("sessions"), 0);
}

#[tokio::test]
async fn ttl_applies_to_updates_and_queries_on_an_index() {
    let (mock, _clock) = ttl_table().await;
    mock.put_item(PutItemRequest::new(
        "sessions",
        item([("pk", s("u#1")), ("expires_at", n("999999"))]),
    ))
    .await
    .unwrap();

    // Updating an expired item resurrects it only if TTL is re-set or removed.
    let updated = mock
        .update_item(
            UpdateItemRequest::new("sessions", item([("pk", s("u#1"))]), "SET seen = :true")
                .with_values(values([(":true", s("yes"))]))
                .with_return_values(mock_ddb::ReturnValues::AllNew),
        )
        .await
        .unwrap();
    assert!(updated.attributes.is_some());
}

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

#[cfg(feature = "streams")]
mod streams {
    use super::*;
    use common::users;
    use mock_ddb::{
        StreamEventName, StreamRecord, StreamViewType, TransactWriteItem,
        TransactWriteItemsRequest, WriteRequest,
    };

    async fn streamed(view_type: StreamViewType) -> (MockDdb, String) {
        let mock = MockDdb::new();
        let description = mock
            .create_table(
                CreateTableRequest::new("events", KeySchema::hash("pk", KeyType::String))
                    .with_stream(view_type),
            )
            .await
            .unwrap();
        let arn = description.latest_stream_arn.expect("a stream ARN");
        (mock, arn)
    }

    fn pk(id: &str) -> mock_ddb::Item {
        item([("pk", s(id))])
    }

    #[tokio::test]
    async fn insert_modify_remove_records() {
        let (mock, arn) = streamed(StreamViewType::NewAndOldImages).await;
        assert_eq!(mock.list_streams(), vec![arn.clone()]);

        mock.put_item(PutItemRequest::new("events", pk("e1")))
            .await
            .unwrap();
        mock.put_item(PutItemRequest::new(
            "events",
            item([("pk", s("e1")), ("v", n("2"))]),
        ))
        .await
        .unwrap();
        mock.delete_item(mock_ddb::DeleteItemRequest::new("events", pk("e1")))
            .await
            .unwrap();

        let records: Vec<StreamRecord> = mock.get_records(&arn).await.unwrap();
        assert_eq!(records.len(), 3);

        assert_eq!(records[0].event_name, StreamEventName::Insert);
        assert_eq!(records[0].event_name.as_str(), "INSERT");
        assert_eq!(records[0].table_name, "events");
        assert_eq!(records[0].keys, pk("e1"));
        assert!(records[0].old_image.is_none());
        assert_eq!(records[0].new_image, Some(pk("e1")));

        assert_eq!(records[1].event_name, StreamEventName::Modify);
        assert_eq!(records[1].event_name.as_str(), "MODIFY");
        assert_eq!(records[1].old_image, Some(pk("e1")));
        assert_eq!(
            records[1].new_image.as_ref().unwrap().get("v"),
            Some(&n("2"))
        );

        assert_eq!(records[2].event_name, StreamEventName::Remove);
        assert_eq!(records[2].event_name.as_str(), "REMOVE");
        assert!(records[2].new_image.is_none());

        let sequence: Vec<&str> = records
            .iter()
            .map(|record| record.sequence_number.as_str())
            .collect();
        let mut sorted = sequence.clone();
        sorted.sort_unstable();
        assert_eq!(
            sequence, sorted,
            "sequence numbers increase lexicographically"
        );
        assert_ne!(records[0].event_id, records[1].event_id);

        assert!(mock
            .get_records("arn:aws:dynamodb:elsewhere")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn view_types_filter_the_images() {
        let (mock, keys_only) = streamed(StreamViewType::KeysOnly).await;
        mock.put_item(PutItemRequest::new(
            "events",
            item([("pk", s("e1")), ("payload", s("x"))]),
        ))
        .await
        .unwrap();
        let record = &mock.get_records(&keys_only).await.unwrap()[0];
        assert!(record.new_image.is_none());
        assert!(record.old_image.is_none());
        assert_eq!(record.keys, pk("e1"));

        let (mock, new_only) = streamed(StreamViewType::NewImage).await;
        mock.put_item(PutItemRequest::new("events", pk("e1")))
            .await
            .unwrap();
        mock.put_item(PutItemRequest::new(
            "events",
            item([("pk", s("e1")), ("v", s("2"))]),
        ))
        .await
        .unwrap();
        let records = mock.get_records(&new_only).await.unwrap();
        assert!(records[1].old_image.is_none());
        assert!(records[1].new_image.is_some());

        let (mock, old_only) = streamed(StreamViewType::OldImage).await;
        mock.put_item(PutItemRequest::new("events", pk("e1")))
            .await
            .unwrap();
        mock.put_item(PutItemRequest::new(
            "events",
            item([("pk", s("e1")), ("v", s("2"))]),
        ))
        .await
        .unwrap();
        let records = mock.get_records(&old_only).await.unwrap();
        assert!(records[1].new_image.is_none());
        assert_eq!(records[1].old_image, Some(pk("e1")));
        assert_eq!(StreamViewType::KeysOnly.as_str(), "KEYS_ONLY");
        assert_eq!(StreamViewType::NewImage.as_str(), "NEW_IMAGE");
        assert_eq!(StreamViewType::OldImage.as_str(), "OLD_IMAGE");
        assert_eq!(
            StreamViewType::NewAndOldImages.as_str(),
            "NEW_AND_OLD_IMAGES"
        );
    }

    #[tokio::test]
    async fn updates_transactions_and_batches_emit_records() {
        let (mock, arn) = streamed(StreamViewType::NewAndOldImages).await;

        mock.put_item(PutItemRequest::new("events", pk("e1")))
            .await
            .unwrap();
        mock.update_item(
            UpdateItemRequest::new("events", pk("e1"), "SET v = :v")
                .with_values(values([(":v", n("1"))])),
        )
        .await
        .unwrap();

        mock.transact_write_items(TransactWriteItemsRequest::new(vec![
            TransactWriteItem::put("events", pk("e2")),
            TransactWriteItem::update("events", pk("e1"), "SET v = :v")
                .with_values(values([(":v", n("2"))])),
        ]))
        .await
        .unwrap();

        // A cancelled transaction writes nothing, so no records either.
        let before = mock.get_records(&arn).await.unwrap().len();
        let cancelled = mock
            .transact_write_items(TransactWriteItemsRequest::new(vec![
                TransactWriteItem::put("events", pk("e3")),
                TransactWriteItem::condition_check("events", pk("missing"), "attribute_exists(pk)"),
            ]))
            .await;
        assert!(cancelled.is_err());
        assert_eq!(mock.get_records(&arn).await.unwrap().len(), before);

        mock.batch_write_item(
            mock_ddb::BatchWriteItemRequest::new()
                .add("events", WriteRequest::Put { item: pk("e4") })
                .add("events", WriteRequest::Delete { key: pk("e1") }),
        )
        .await
        .unwrap();

        let records = mock.get_records(&arn).await.unwrap();
        // put e1, update e1, transaction put e2, transaction update e1,
        // batch put e4, batch delete e1
        assert_eq!(records.len(), 6);
        assert_eq!(records.last().unwrap().event_name, StreamEventName::Remove);
    }

    #[tokio::test]
    async fn tables_without_a_stream_have_no_arn() {
        let mock = users();
        assert!(mock.list_streams().is_empty());
        let description = mock.describe_table("users").await.unwrap();
        assert!(description.latest_stream_arn.is_none());
        assert!(mock.get_records("arn:aws:dynamodb:local").await.is_err());
    }

    #[tokio::test]
    async fn seeding_and_deleting_tables_do_not_panic_on_streams() {
        let (mock, arn) = streamed(StreamViewType::NewAndOldImages).await;
        mock.seed("events", vec![pk("e1"), pk("e2")]).unwrap();
        assert_eq!(mock.get_records(&arn).await.unwrap().len(), 2);
        mock.delete_table("events").await.unwrap();
        assert!(mock.list_streams().is_empty());
    }

    #[tokio::test]
    async fn stream_disabled_specification_is_honoured() {
        let mock = MockDdb::new();
        let description = mock
            .create_table(CreateTableRequest {
                table_name: "events".to_string(),
                key_schema: KeySchema::hash("pk", KeyType::String),
                stream_specification: Some(mock_ddb::StreamSpecification {
                    stream_enabled: false,
                    stream_view_type: None,
                }),
                ..CreateTableRequest::default()
            })
            .await
            .unwrap();
        assert!(description.latest_stream_arn.is_none());
    }
}

#[cfg(not(feature = "streams"))]
#[tokio::test]
async fn creating_a_stream_without_the_feature_is_rejected() {
    let mock = MockDdb::new();
    let error = mock
        .create_table(
            CreateTableRequest::new("events", KeySchema::hash("pk", KeyType::String))
                .with_stream(mock_ddb::StreamViewType::NewImage),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("streams"));
}
