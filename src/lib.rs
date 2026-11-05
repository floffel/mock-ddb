//! An in-process DynamoDB for tests — tables, expressions, queries, batches,
//! transactions, TTL, streams and PartiQL, with no network and no AWS account.
//!
//! `aws_sdk_dynamodb::Client` speaks to the service; this crate speaks to a
//! `HashMap` in the test's address space and implements the same request and
//! response shapes, so production code can run against it unchanged. It is not
//! a stub: expressions are parsed, key conditions are validated, conditions are
//! evaluated, transactions are atomic and indexes are maintained on every
//! write.
//!
//! ```
//! use mock_ddb::{
//!     item, n, s, values, CreateTableRequest, GetItemRequest, KeySchema, KeyType, MockDdb,
//!     PutItemRequest, QueryRequest, UpdateItemRequest,
//! };
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mock = MockDdb::new();
//! mock.create_table(CreateTableRequest::new(
//!     "orders",
//!     KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::Number),
//! ))
//! .await?;
//!
//! mock.put_item(PutItemRequest::new("orders", item([
//!     ("pk", s("customer#1")),
//!     ("sk", n("1")),
//!     ("total", n("42.50")),
//! ])))
//! .await?;
//!
//! mock.update_item(
//!     UpdateItemRequest::new("orders", item([("pk", s("customer#1")), ("sk", n("1"))]), "SET total = total + :tip")
//!         .with_values(values([(":tip", n("7.50"))])),
//! )
//! .await?;
//!
//! let page = mock
//!     .query(
//!         QueryRequest::new("orders", "pk = :pk")
//!             .with_values(values([(":pk", s("customer#1"))])),
//!     )
//!     .await?;
//! assert_eq!(page.items[0]["total"], n("50"));
//!
//! let single = mock
//!     .get_item(GetItemRequest::new("orders", item([("pk", s("customer#1")), ("sk", n("1"))])))
//!     .await?;
//! assert!(single.item.is_some());
//! # Ok(())
//! # }
//! ```
//!
//! # What it supports
//!
//! | Area | Operations |
//! | --- | --- |
//! | Tables | `create_table`, `delete_table`, `describe_table`, `list_tables` |
//! | Items | `put_item`, `get_item`, `update_item`, `delete_item` |
//! | Reads | `query`, `scan` (filter, limit, cursor, projection, segments, counts, indexes) |
//! | Batches | `batch_write_item` (25), `batch_get_item` (100) |
//! | Transactions | `transact_write_items` (100, atomic, cancellation reasons, idempotency token), `transact_get_items` |
//! | Expressions | conditions, updates (`SET`/`REMOVE`/`ADD`/`DELETE`), projections |
//! | Capacity | `ConsumedCapacity` (`TOTAL`/`INDEXES`) and `ItemCollectionMetrics` on every data-plane response, computed from the real item sizes |
//! | TTL | `update_time_to_live`, `describe_time_to_live`, lazy expiry, `purge_expired` |
//! | Streams | one ordered log per table, `get_records`, `list_streams` (feature `streams`) |
//! | Streams API | `ListStreams`, `DescribeStream`, `GetShardIterator` and `GetRecords` with their wire shapes, limits and iterators, through [`MockDdb::streams`] |
//! | PartiQL | `execute_statement` (`SELECT`/`INSERT`/`UPDATE`/`DELETE`) and `batch_execute_statement`, lowered onto the native calls |
//!
//! # Why not an existing crate
//!
//! A crates.io lookup on 2026-11-05 (`in-process DynamoDB mock for tests`,
//! ranked by adoption) returned `skyzen-services`, `rusty-gasket` and
//! `winterbaume-partiql` — none of them an in-process DynamoDB double — so no
//! maintained crate does this job and the implementation is local on purpose.
//! The doubles that do exist are services, not libraries: `DynamoDB Local` is a
//! Java process (or a container), `LocalStack` a Python container and
//! `dynalite` a Node.js server, so each brings a runtime, a port and start/stop
//! sequencing into a test suite that should need none of them, while
//! `aws_sdk_dynamodb` needs a real endpoint and real credentials. A `HashMap`
//! in the test's own address space has neither.
//!
//! # Deliberate differences from the service
//!
//! - Writes are synchronous: a table exists in state `ACTIVE` as soon as
//!   `create_table` returns, and there is no `CREATING`/`DELETING` lifecycle.
//! - Consistent reads are accepted and ignored, because every read is strongly
//!   consistent.
//! - TTL expiry hides an item from every read immediately and deletes it when
//!   it is read or purged, where the service deletes within 48 hours.
//! - Capacity is reported, never enforced. `return_consumed_capacity` fills
//!   `consumed_capacity` with units computed from the real item sizes — one read
//!   unit per started 4 KiB of an item read, one write unit per started 1 KiB of
//!   an item written, split per table and per index for `INDEXES` — and
//!   `return_item_collection_metrics` reports the collection a write touched,
//!   but there is no provisioned throughput, no throttling, no
//!   `ProvisionedThroughputExceededException` and no 10 GB collection limit.
//! - A `Query` without a key condition returns every item instead of failing.
//! - An idempotency token is remembered for ten minutes on the injected clock,
//!   as on the service, and a resubmitted token with different actions is
//!   refused with `IdempotentParameterMismatchException`. A replay returns an
//!   empty response rather than the original `ConsumedCapacity` and
//!   `ItemCollectionMetrics`, and only a transaction that applied spends its
//!   token: a cancelled one records nothing, so a retry re-evaluates its
//!   conditions instead of replaying the cancellation.
//! - Streams have a single open shard and unlimited retention: shard iterators
//!   never expire, never split and never trim, where the service keeps 24 hours
//!   and closes a shard after four hours of inactivity. The Streams API
//!   (`ListStreams`, `DescribeStream`, `GetShardIterator`, `GetRecords`) is
//!   modelled in full, including its paging and limits; the short
//!   [`MockDdb::get_records`] reader stays for tests that do not care about
//!   iterators.
//!
//! The README carries the full supported/not-supported matrix and the reasoning
//! behind each difference.
//!
//! # Thread safety
//!
//! [`MockDdb`] is `Send + Sync` and every method takes `&self`; one instance can
//! be shared by any number of tasks, exactly like the SDK client. Writes hold a
//! single lock for their duration, so transactions are atomic and batch writes
//! do not leave a half-applied state behind when validation fails.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod capacity;
mod clock;
mod error;
mod expr;
mod mock;
mod number;
mod partiql;
mod request;
mod schema;
#[cfg(feature = "streams")]
mod stream;
mod table;
mod value;

use std::collections::{BTreeMap, HashMap};

pub use aws_sdk_dynamodb::primitives::Blob;
pub use aws_sdk_dynamodb::types::AttributeValue;

pub use capacity::{
    read_units, size_estimate_range, write_units, Capacity, ConsumedCapacity,
    ItemCollectionMetrics, ReturnConsumedCapacity, ReturnItemCollectionMetrics,
    COLLECTION_ESTIMATE_STEP_BYTES, READ_UNIT_BYTES, WRITE_UNIT_BYTES,
};
pub use clock::{Clock, MockClock, SystemClock};
pub use error::{n, s, CancellationReason, CancellationReasonCode, Error, Result};
pub use expr::{ConditionExpression, ProjectionExpression, UpdateExpression};
pub use mock::MockDdb;
pub use partiql::{
    BatchExecuteStatementOutput, BatchExecuteStatementRequest, BatchStatementError,
    BatchStatementErrorCode, BatchStatementRequest, BatchStatementResponse, ExecuteStatementOutput,
    ExecuteStatementRequest, PartiqlStatement, ReturnValuesOnConditionCheckFailure, StatementKind,
    MAX_BATCH_STATEMENTS, MAX_STATEMENT_LENGTH,
};
pub use request::{
    item, names, values, BatchGetItemOutput, BatchGetItemRequest, BatchWriteItemOutput,
    BatchWriteItemRequest, DeleteItemOutput, DeleteItemRequest, DescribeTableOutput, GetItemOutput,
    GetItemRequest, KeysAndAttributes, ListTablesOutput, ListTablesRequest, PutItemOutput,
    PutItemRequest, QueryOutput, QueryRequest, ReturnValues, ScanOutput, ScanRequest, Select,
    TimeToLiveDescription, TimeToLiveStatus, TransactConditionCheck, TransactDelete, TransactGet,
    TransactGetItemsOutput, TransactGetItemsRequest, TransactPut, TransactUpdate,
    TransactWriteItem, TransactWriteItemsOutput, TransactWriteItemsRequest, UpdateItemOutput,
    UpdateItemRequest, UpdateTimeToLiveRequest, WriteRequest,
};
pub use schema::{
    AttributeDefinition, BillingMode, CreateTableRequest, KeySchema, KeySchemaElement, KeyType,
    Projection, SecondaryIndex, StreamSpecification, StreamViewType, TableDescription,
};
#[cfg(feature = "streams")]
pub use stream::StreamEventName;
#[cfg(feature = "streams")]
pub use stream::{
    DescribeStreamOutput, DescribeStreamRequest, GetRecordsOutput, GetRecordsRequest,
    GetShardIteratorOutput, GetShardIteratorRequest, ListStreamsOutput, ListStreamsRequest,
    SequenceNumberRange, Shard, ShardIteratorType, StreamDescription, StreamRecord, StreamSummary,
    StreamsApi, StreamsRecord, StreamsRecordData, MAX_RECORDS_PER_GET, MAX_SHARDS_PER_DESCRIBE,
    MAX_STREAMS_PER_LIST, STREAM_CREATION_EPOCH_SECONDS, STREAM_REGION,
};

/// A DynamoDB item: attribute name → value.
///
/// The alias is the SDK's own map type, so items flow between production code
/// and the mock without conversion.
///
/// # Attribute order
///
/// `Item` is a `HashMap` with `RandomState`, so its iteration order — and with
/// it the `Debug` form of an item, or any serialization a caller builds on top
/// of it — varies from one process to the next. DynamoDB does not define an
/// attribute order either, and equality needs none: `HashMap`'s `PartialEq`
/// compares entries rather than order, so `assert_eq!` on two items holds
/// however each of them was built. An assertion that *renders* an item — a
/// golden file, a logged response — wants [`ordered_item`], which lists the
/// attributes by name and therefore prints the same bytes on every run.
pub type Item = HashMap<String, AttributeValue>;

/// An item's attributes in name order, borrowing the values instead of copying
/// them.
///
/// The result is a `BTreeMap`, so its `Debug` output is the same on every run,
/// which is what a golden or log-based assertion needs from an [`Item`]. The
/// values are borrowed: sorting a 400 KiB document allocates one map of
/// references, not a second document. Equality does not need this form —
/// comparing items with `==` is already order-insensitive.
///
/// ```
/// use mock_ddb::{item, n, ordered_item, s};
///
/// let written = item([("pk", s("u#1")), ("age", n("42"))]);
/// let read = item([("age", n("42")), ("pk", s("u#1"))]);
/// assert_eq!(written, read);
/// assert_eq!(
///     format!("{:#?}", ordered_item(&written)),
///     format!("{:#?}", ordered_item(&read))
/// );
/// assert_eq!(
///     ordered_item(&written).keys().copied().collect::<Vec<_>>(),
///     ["age", "pk"]
/// );
/// ```
#[must_use]
pub fn ordered_item(item: &Item) -> BTreeMap<&str, &AttributeValue> {
    item.iter()
        .map(|(name, value)| (name.as_str(), value))
        .collect()
}

/// Wrap an item as an `AttributeValue::M`, the shape DynamoDB uses when an item
/// (or part of one) is embedded in a document attribute.
///
/// ```
/// use mock_ddb::{item, item_to_value, s, AttributeValue};
///
/// let wrapped = item_to_value(item([("pk", s("u#1"))]));
/// assert!(matches!(wrapped, AttributeValue::M(_)));
/// ```
#[must_use]
pub fn item_to_value(item: Item) -> AttributeValue {
    AttributeValue::M(item)
}

/// Borrow the map behind an `AttributeValue::M` as an item, without copying it.
/// Returns `None` for any other value type, so a caller cannot mistake a
/// scalar for a document.
///
/// This is the form to use when reading: the borrow keeps the attribute alive
/// and hands out the stored entries themselves, so inspecting an attribute of
/// any size costs nothing. The embedded document is whatever the caller stored;
/// neither this function nor [`into_item`] validates its size or its nesting
/// depth.
///
/// ```
/// use mock_ddb::{s, item_ref_from_value, AttributeValue};
///
/// let document = AttributeValue::M(std::collections::HashMap::from([(
///     "pk".to_string(),
///     s("u#1"),
/// )]));
/// assert_eq!(
///     item_ref_from_value(&document).and_then(|item| item.get("pk")),
///     Some(&s("u#1"))
/// );
/// assert!(item_ref_from_value(&AttributeValue::Null(true)).is_none());
/// ```
#[must_use]
pub fn item_ref_from_value(value: &AttributeValue) -> Option<&Item> {
    match value {
        AttributeValue::M(entries) => Some(entries),
        _ => None,
    }
}

/// Consume an `AttributeValue::M` and take ownership of the item it holds.
/// Returns `None` for any other value type, so a caller cannot mistake a
/// scalar for a document.
///
/// Unlike [`item_from_value`] this moves the map out of the value instead of
/// cloning it, so unwrapping a document the caller already owns is free. The
/// size and nesting depth of the document are the caller's to bound — neither
/// this function nor [`item_ref_from_value`] validates them.
///
/// ```
/// use mock_ddb::{item_to_value, into_item, item, s};
///
/// let wrapped = item_to_value(item([("pk", s("u#1"))]));
/// assert_eq!(into_item(wrapped).map(|item| item.len()), Some(1));
/// assert!(into_item(s("scalar")).is_none());
/// ```
#[must_use]
pub fn into_item(value: AttributeValue) -> Option<Item> {
    match value {
        AttributeValue::M(entries) => Some(entries),
        _ => None,
    }
}

/// Unwrap an `AttributeValue::M` back into an owned item, cloning it. Returns
/// `None` for any other value type, so a caller cannot mistake a scalar for a
/// document.
///
/// The clone copies the whole subtree the document holds, nested documents and
/// binary payloads included, so a read path that only inspects an attribute
/// wants [`item_ref_from_value`], and a value the caller already owns wants
/// [`into_item`]. Neither form validates the size or the nesting depth of the
/// document.
///
/// ```
/// use mock_ddb::{s, item_from_value, AttributeValue};
///
/// let document = AttributeValue::M(std::collections::HashMap::from([(
///     "pk".to_string(),
///     s("u#1"),
/// )]));
/// assert_eq!(item_from_value(&document).map(|item| item.len()), Some(1));
/// assert!(item_from_value(&s("scalar")).is_none());
/// ```
#[must_use]
pub fn item_from_value(value: &AttributeValue) -> Option<Item> {
    item_ref_from_value(value).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn items_convert_to_maps_and_back() {
        let record = request::item([("pk", s("u#1")), ("n", n("2"))]);
        let wrapped = item_to_value(record.clone());
        assert!(matches!(wrapped, AttributeValue::M(_)));
        assert_eq!(item_from_value(&wrapped), Some(record));
        assert_eq!(item_from_value(&s("scalar")), None);
    }

    #[test]
    fn item_ref_borrows_the_stored_entries_instead_of_copying_them() {
        let record = request::item([
            ("pk", s("u#1")),
            (
                "nested",
                item_to_value(request::item([("inner", s("deep"))])),
            ),
            ("blob", AttributeValue::B(Blob::new(vec![0_u8; 64]))),
        ]);
        let wrapped = item_to_value(record.clone());
        assert_eq!(item_ref_from_value(&wrapped), Some(&record));

        let AttributeValue::M(entries) = &wrapped else {
            panic!("item_to_value must produce a document");
        };
        // The borrowed form hands out the entries the value itself holds, so a
        // read allocates no second document; `into_item` moves them out (a
        // `HashMap` moves), which the content check below pins down.
        assert!(std::ptr::eq(
            item_ref_from_value(&wrapped).expect("a document"),
            entries
        ));

        let owned = into_item(wrapped).expect("a document");
        assert_eq!(owned, record);
    }

    #[test]
    fn every_accessor_rejects_a_non_document_and_accepts_an_empty_one() {
        let empty = AttributeValue::M(HashMap::new());
        assert_eq!(item_ref_from_value(&empty), Some(&Item::default()));
        assert_eq!(into_item(empty.clone()), Some(Item::default()));
        assert_eq!(item_from_value(&empty), Some(Item::default()));

        let not_a_document = [
            s("scalar"),
            n("2"),
            AttributeValue::Null(true),
            AttributeValue::Bool(false),
            AttributeValue::L(Vec::new()),
            AttributeValue::Ss(Vec::new()),
            AttributeValue::B(Blob::new(Vec::new())),
        ];
        for value in &not_a_document {
            assert_eq!(item_ref_from_value(value), None, "{value:?}");
            assert_eq!(into_item(value.clone()), None, "{value:?}");
            assert_eq!(item_from_value(value), None, "{value:?}");
        }
    }

    #[test]
    fn ordered_item_sorts_names_and_borrows_the_values() {
        let record = request::item([
            ("z", s("last")),
            ("ä", s("umlaut")),
            ("a", s("first")),
            (
                "nested",
                item_to_value(request::item([("inner", s("deep"))])),
            ),
        ]);
        let ordered = ordered_item(&record);
        // Byte order, which for UTF-8 is code-point order: `ä` sorts after `z`.
        assert_eq!(
            ordered.keys().copied().collect::<Vec<_>>(),
            ["a", "nested", "z", "ä"]
        );
        for (name, value) in &ordered {
            let stored = record.get(*name).expect("the attribute it was built from");
            assert_eq!(stored, *value, "{name}");
            // The view borrows the stored values; a clone would move the pointer.
            assert!(std::ptr::eq(*value, stored), "{name}");
        }
    }

    #[test]
    fn ordered_item_pins_the_empty_and_single_attribute_edges() {
        let nothing = Item::new();
        let empty = ordered_item(&nothing);
        assert!(empty.is_empty());
        assert_eq!(empty.keys().next(), None);
        // An absent name is not an attribute, not an empty one.
        assert_eq!(empty.get(""), None);

        let single = request::item([("pk", s("u#1"))]);
        let ordered = ordered_item(&single);
        assert_eq!(ordered.len(), 1);
        assert_eq!(ordered.get("pk").copied(), Some(&s("u#1")));
        assert_eq!(ordered.get("sk"), None);
    }
}
