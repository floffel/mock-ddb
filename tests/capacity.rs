//! Consumed capacity and item collection metrics: the numbers, their limits,
//! and the fields the service leaves absent.

mod common;

use std::collections::BTreeMap;

use common::{orders, users};
use mock_ddb::{
    item, n, s, values, BatchGetItemRequest, BatchWriteItemRequest, Capacity, CreateTableRequest,
    DeleteItemRequest, GetItemRequest, Item, KeySchema, KeyType, KeysAndAttributes, MockDdb,
    PutItemRequest, QueryRequest, ReturnConsumedCapacity, ReturnItemCollectionMetrics,
    ReturnValues, ScanRequest, SecondaryIndex, Select, TransactGet, TransactGetItemsRequest,
    TransactWriteItem, TransactWriteItemsRequest, UpdateItemRequest, WriteRequest,
};
use proptest::prelude::*;

/// Exact unit counts, spelled out because `clippy::float_cmp` rejects `==`.
fn same(left: f64, right: f64) -> bool {
    (left - right).abs() < f64::EPSILON
}

/// The table's partition key attribute, padded to make an item of exactly
/// `bytes` bytes: an item of one attribute costs `name.len() + value.len()`.
fn item_of_bytes(bytes: usize) -> Item {
    let padding = bytes - "pk".len();
    item([("pk", s("x".repeat(padding)))])
}

async fn table_with_indexes() -> MockDdb {
    let mock = MockDdb::new();
    mock.create_table(
        CreateTableRequest::new(
            "orders",
            KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::Number),
        )
        .with_global_index(SecondaryIndex::new(
            "by-total",
            KeySchema::hash("total", KeyType::Number),
        ))
        .with_local_index(SecondaryIndex::new(
            "by-status",
            KeySchema::hash_and_range("pk", KeyType::String, "status", KeyType::String),
        )),
    )
    .await
    .unwrap();
    mock
}

#[tokio::test]
async fn a_write_reports_total_or_the_index_breakdown() {
    let mock = table_with_indexes().await;

    let total = mock
        .put_item(
            PutItemRequest::new(
                "orders",
                item([
                    ("pk", s("c#1")),
                    ("sk", n("1")),
                    ("total", n("10")),
                    ("status", s("open")),
                ]),
            )
            .with_return_consumed_capacity(ReturnConsumedCapacity::Total),
        )
        .await
        .unwrap();
    let capacity = total.consumed_capacity.unwrap();
    assert_eq!(capacity.table_name.as_deref(), Some("orders"));
    // The table plus both indexes the item lands in: three write units.
    assert!(same(capacity.capacity_units.unwrap(), 3.0));
    assert!(same(capacity.write_capacity_units.unwrap(), 3.0));
    assert!(
        capacity.table.is_none(),
        "TOTAL reports no per-index breakdown"
    );

    let breakdown = mock
        .update_item(
            UpdateItemRequest::new(
                "orders",
                item([("pk", s("c#1")), ("sk", n("1"))]),
                "SET status = :open",
            )
            .with_values(mock_ddb::values([(":open", s("closed"))]))
            .with_return_consumed_capacity(ReturnConsumedCapacity::Indexes),
        )
        .await
        .unwrap()
        .consumed_capacity
        .unwrap();
    assert!(same(breakdown.capacity_units.unwrap(), 3.0));
    assert_eq!(
        breakdown.table,
        Some(Capacity {
            read_capacity_units: None,
            write_capacity_units: Some(1.0),
            capacity_units: Some(1.0),
        })
    );
    assert_eq!(breakdown.global_secondary_indexes.len(), 1);
    assert_eq!(breakdown.local_secondary_indexes.len(), 1);
    assert!(same(
        breakdown.global_secondary_indexes["by-total"]
            .capacity_units
            .unwrap(),
        1.0
    ));
}

#[tokio::test]
async fn a_write_unit_covers_exactly_one_kilobyte() {
    let mock = users();

    let at_limit = mock
        .put_item(
            PutItemRequest::new("users", item_of_bytes(1024))
                .with_return_consumed_capacity(ReturnConsumedCapacity::Total),
        )
        .await
        .unwrap()
        .consumed_capacity
        .unwrap();
    assert!(same(at_limit.capacity_units.unwrap(), 1.0));

    let over_limit = mock
        .put_item(
            PutItemRequest::new("users", item_of_bytes(1025))
                .with_return_consumed_capacity(ReturnConsumedCapacity::Total),
        )
        .await
        .unwrap()
        .consumed_capacity
        .unwrap();
    assert!(same(over_limit.capacity_units.unwrap(), 2.0));
}

#[tokio::test]
async fn a_read_unit_covers_exactly_four_kilobytes() {
    let mock = users();
    mock.put_item(PutItemRequest::new("users", item_of_bytes(4096)))
        .await
        .unwrap();
    mock.put_item(PutItemRequest::new(
        "users",
        item([("pk", s("y".repeat(4095)))]),
    ))
    .await
    .unwrap();

    let at_limit = mock
        .get_item(
            GetItemRequest::new("users", item([("pk", s("x".repeat(4094)))]))
                .with_return_consumed_capacity(ReturnConsumedCapacity::Total),
        )
        .await
        .unwrap()
        .consumed_capacity
        .unwrap();
    assert!(same(at_limit.capacity_units.unwrap(), 1.0));

    let over_limit = mock
        .get_item(
            GetItemRequest::new("users", item([("pk", s("y".repeat(4095)))]))
                .with_return_consumed_capacity(ReturnConsumedCapacity::Total),
        )
        .await
        .unwrap()
        .consumed_capacity
        .unwrap();
    assert!(same(over_limit.capacity_units.unwrap(), 2.0));
}

#[tokio::test]
async fn a_key_that_does_not_exist_still_costs_one_unit() {
    let mock = users();

    let missing = mock
        .get_item(
            GetItemRequest::new("users", item([("pk", s("absent"))]))
                .with_return_consumed_capacity(ReturnConsumedCapacity::Total),
        )
        .await
        .unwrap();
    assert!(missing.item.is_none());
    assert!(same(
        missing.consumed_capacity.unwrap().capacity_units.unwrap(),
        1.0
    ));

    let deleted = mock
        .delete_item(
            DeleteItemRequest::new("users", item([("pk", s("absent"))]))
                .with_return_consumed_capacity(ReturnConsumedCapacity::Total),
        )
        .await
        .unwrap();
    assert!(same(
        deleted.consumed_capacity.unwrap().capacity_units.unwrap(),
        1.0
    ));

    let batch = mock
        .batch_get_item(
            BatchGetItemRequest::new()
                .add(
                    "users",
                    KeysAndAttributes::new(vec![
                        item([("pk", s("a"))]),
                        item([("pk", s("b"))]),
                        item([("pk", s("c"))]),
                    ]),
                )
                .with_return_consumed_capacity(ReturnConsumedCapacity::Total),
        )
        .await
        .unwrap();
    assert_eq!(batch.consumed_capacity.len(), 1);
    assert!(same(
        batch.consumed_capacity[0].capacity_units.unwrap(),
        3.0
    ));
}

#[tokio::test]
async fn an_empty_page_still_costs_one_read_unit() {
    let mock = orders();
    let page = mock
        .query(
            QueryRequest::new("orders", "pk = :pk")
                .with_values(values([(":pk", s("nobody"))]))
                .with_return_consumed_capacity(ReturnConsumedCapacity::Total),
        )
        .await
        .unwrap();
    assert_eq!(page.count, 0);
    assert!(same(
        page.consumed_capacity.unwrap().capacity_units.unwrap(),
        1.0
    ));

    let scanned = mock
        .scan(
            ScanRequest::new("orders").with_return_consumed_capacity(ReturnConsumedCapacity::Total),
        )
        .await
        .unwrap();
    assert!(same(
        scanned.consumed_capacity.unwrap().capacity_units.unwrap(),
        1.0
    ));
}

#[tokio::test]
async fn a_query_charges_every_item_it_examines_including_filtered_ones() {
    let mock = orders();
    mock.seed(
        "orders",
        vec![
            item([("pk", s("c#1")), ("sk", n("1")), ("total", n("10"))]),
            item([("pk", s("c#1")), ("sk", n("2")), ("total", n("20"))]),
            item([("pk", s("c#1")), ("sk", n("3")), ("total", n("30"))]),
        ],
    )
    .unwrap();

    let filtered = mock
        .query(
            QueryRequest::new("orders", "pk = :pk")
                .with_values(values([(":pk", s("c#1")), (":ten", n("10"))]))
                .with_filter("total > :ten")
                .with_projection("sk")
                .with_return_consumed_capacity(ReturnConsumedCapacity::Total),
        )
        .await
        .unwrap();
    assert_eq!(filtered.count, 2);
    assert_eq!(filtered.scanned_count, 3);
    // Three items examined at one unit each, not the two returned.
    assert!(same(
        filtered.consumed_capacity.unwrap().capacity_units.unwrap(),
        3.0
    ));

    let counted = mock
        .query(
            QueryRequest::new("orders", "pk = :pk")
                .with_values(values([(":pk", s("c#1"))]))
                .with_select(Select::Count)
                .with_return_consumed_capacity(ReturnConsumedCapacity::Total),
        )
        .await
        .unwrap();
    assert_eq!(counted.items, []);
    assert!(same(
        counted.consumed_capacity.unwrap().capacity_units.unwrap(),
        3.0
    ));
}

#[tokio::test]
async fn an_index_read_charges_the_index_and_not_the_table() {
    let mock = table_with_indexes().await;
    mock.put_item(PutItemRequest::new(
        "orders",
        item([("pk", s("c#1")), ("sk", n("1")), ("total", n("10"))]),
    ))
    .await
    .unwrap();

    let page = mock
        .query(
            QueryRequest::new("orders", "total = :total")
                .with_index("by-total")
                .with_values(values([(":total", n("10"))]))
                .with_return_consumed_capacity(ReturnConsumedCapacity::Indexes),
        )
        .await
        .unwrap();
    let capacity = page.consumed_capacity.unwrap();
    assert!(same(capacity.capacity_units.unwrap(), 1.0));
    assert!(same(capacity.table.unwrap().capacity_units.unwrap(), 0.0));
    assert!(same(
        capacity.global_secondary_indexes["by-total"]
            .read_capacity_units
            .unwrap(),
        1.0
    ));
}

#[tokio::test]
async fn a_sparse_index_an_item_does_not_reach_is_not_charged() {
    let mock = MockDdb::new();
    mock.create_table(
        CreateTableRequest::new("users", KeySchema::hash("pk", KeyType::String)).with_global_index(
            SecondaryIndex::new("by-email", KeySchema::hash("email", KeyType::String)),
        ),
    )
    .await
    .unwrap();

    let capacity = mock
        .put_item(
            PutItemRequest::new("users", item([("pk", s("u#1"))]))
                .with_return_consumed_capacity(ReturnConsumedCapacity::Indexes),
        )
        .await
        .unwrap()
        .consumed_capacity
        .unwrap();
    assert!(same(capacity.capacity_units.unwrap(), 1.0));
    assert_eq!(
        capacity.global_secondary_indexes,
        BTreeMap::new(),
        "an item without the index key costs the index nothing"
    );
}

#[tokio::test]
async fn item_collection_metrics_need_a_local_secondary_index() {
    let without = users();
    let absent = without
        .put_item(
            PutItemRequest::new("users", item([("pk", s("u#1"))]))
                .with_return_item_collection_metrics(ReturnItemCollectionMetrics::Size),
        )
        .await
        .unwrap();
    assert!(
        absent.item_collection_metrics.is_none(),
        "a table without a local secondary index reports no collection"
    );

    let mock = table_with_indexes().await;
    let first = mock
        .put_item(
            PutItemRequest::new(
                "orders",
                item([("pk", s("c#1")), ("sk", n("1")), ("status", s("open"))]),
            )
            .with_return_item_collection_metrics(ReturnItemCollectionMetrics::Size),
        )
        .await
        .unwrap()
        .item_collection_metrics
        .unwrap();
    assert_eq!(first.item_collection_key, Some(item([("pk", s("c#1"))])));
    assert_eq!(first.size_estimate_range_gb, Some([0.0, 1.0]));

    // A second item in the same collection reports the same key, and the
    // estimate stays a range of whole gibibytes.
    let second = mock
        .put_item(
            PutItemRequest::new(
                "orders",
                item([("pk", s("c#1")), ("sk", n("2")), ("status", s("open"))]),
            )
            .with_return_item_collection_metrics(ReturnItemCollectionMetrics::Size),
        )
        .await
        .unwrap()
        .item_collection_metrics
        .unwrap();
    assert_eq!(second.item_collection_key, first.item_collection_key);
    assert_eq!(second.size_estimate_range_gb, Some([0.0, 1.0]));

    // A delete reports the collection it emptied, keyed by the same partition.
    let deleted = mock
        .delete_item(
            DeleteItemRequest::new("orders", item([("pk", s("c#1")), ("sk", n("1"))]))
                .with_return_values(ReturnValues::AllOld)
                .with_return_item_collection_metrics(ReturnItemCollectionMetrics::Size),
        )
        .await
        .unwrap();
    assert_eq!(
        deleted.item_collection_metrics.unwrap().item_collection_key,
        Some(item([("pk", s("c#1"))]))
    );

    // Nothing is reported when the request does not ask for it.
    let silent = mock
        .put_item(PutItemRequest::new(
            "orders",
            item([("pk", s("c#2")), ("sk", n("1"))]),
        ))
        .await
        .unwrap();
    assert!(silent.item_collection_metrics.is_none());
    assert!(silent.consumed_capacity.is_none());
}

#[tokio::test]
async fn a_batch_write_reports_one_entry_per_table() {
    let mock = orders();
    let response = mock
        .batch_write_item(
            BatchWriteItemRequest::new()
                .add(
                    "orders",
                    WriteRequest::Put {
                        item: item([("pk", s("c#1")), ("sk", n("1"))]),
                    },
                )
                .add(
                    "orders",
                    WriteRequest::Delete {
                        key: item([("pk", s("c#1")), ("sk", n("2"))]),
                    },
                )
                .with_return_consumed_capacity(ReturnConsumedCapacity::Total),
        )
        .await
        .unwrap();
    assert_eq!(response.consumed_capacity.len(), 1);
    assert!(same(
        response.consumed_capacity[0].capacity_units.unwrap(),
        2.0
    ));
    assert_eq!(
        response.consumed_capacity[0].table_name.as_deref(),
        Some("orders")
    );
}

#[tokio::test]
async fn a_transaction_reports_a_read_and_a_write_unit_per_table() {
    let mock = users();
    mock.put_item(PutItemRequest::new("users", item([("pk", s("checked"))])))
        .await
        .unwrap();

    let response = mock
        .transact_write_items(
            TransactWriteItemsRequest::new(vec![
                TransactWriteItem::put("users", item([("pk", s("written"))])),
                TransactWriteItem::condition_check(
                    "users",
                    item([("pk", s("checked"))]),
                    "attribute_exists(pk)",
                ),
            ])
            .with_return_consumed_capacity(ReturnConsumedCapacity::Total),
        )
        .await
        .unwrap();
    assert_eq!(response.consumed_capacity.len(), 1);
    let capacity = &response.consumed_capacity[0];
    assert!(same(capacity.read_capacity_units.unwrap(), 1.0));
    assert!(same(capacity.write_capacity_units.unwrap(), 1.0));
    assert!(same(capacity.capacity_units.unwrap(), 2.0));

    let read = mock
        .transact_get_items(
            TransactGetItemsRequest::new(vec![
                TransactGet::new("users", item([("pk", s("written"))])),
                TransactGet::new("users", item([("pk", s("absent"))])),
            ])
            .with_return_consumed_capacity(ReturnConsumedCapacity::Total),
        )
        .await
        .unwrap();
    assert_eq!(read.responses.len(), 2);
    assert_eq!(read.consumed_capacity.len(), 1);
    assert!(same(read.consumed_capacity[0].capacity_units.unwrap(), 2.0));
}

#[tokio::test]
async fn the_default_reports_nothing() {
    let mock = users();
    let response = mock
        .put_item(PutItemRequest::new("users", item([("pk", s("u#1"))])))
        .await
        .unwrap();
    assert!(response.consumed_capacity.is_none());
    assert!(response.item_collection_metrics.is_none());

    let read = mock
        .get_item(GetItemRequest::new("users", item([("pk", s("u#1"))])))
        .await
        .unwrap();
    assert!(read.consumed_capacity.is_none());
}

proptest! {
    /// A larger item never costs fewer units, and every unit count matches the
    /// documented formula recomputed in integer arithmetic.
    #[test]
    fn capacity_units_follow_the_documented_formula(
        left in 0_usize..2_000_000,
        right in 0_usize..2_000_000,
    ) {
        let (bigger, smaller) = if left >= right { (left, right) } else { (right, left) };

        let read = mock_ddb::read_units(bigger);
        prop_assert!(read >= 1.0);
        prop_assert!(same(read, expected_units(bigger, mock_ddb::READ_UNIT_BYTES)));
        prop_assert!(read >= mock_ddb::read_units(smaller));

        let write = mock_ddb::write_units(bigger);
        prop_assert!(write >= 1.0);
        prop_assert!(same(write, expected_units(bigger, mock_ddb::WRITE_UNIT_BYTES)));
        prop_assert!(write >= mock_ddb::write_units(smaller));
    }

    /// An estimate range is always one gibibyte wide and starts at the whole
    /// gibibyte below the actual size.
    #[test]
    fn size_estimates_stay_one_gibibyte_wide(bytes in 0_usize..4_000_000_000) {
        let [lower, upper] = mock_ddb::size_estimate_range(bytes);
        let whole = bytes / mock_ddb::COLLECTION_ESTIMATE_STEP_BYTES;
        let whole = f64::from(u32::try_from(whole).unwrap());
        prop_assert!(same(lower, whole));
        prop_assert!(same(upper, whole + 1.0));
    }
}

/// The documented unit formula, in integer arithmetic.
fn expected_units(bytes: usize, unit_bytes: usize) -> f64 {
    f64::from(u32::try_from(bytes.div_ceil(unit_bytes).max(1)).unwrap())
}
