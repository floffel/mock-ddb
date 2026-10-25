//! An in-process DynamoDB for tests — tables, expressions, queries, batches,
//! transactions, TTL and streams, with no network and no AWS account.
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
//! | TTL | `update_time_to_live`, `describe_time_to_live`, lazy expiry, `purge_expired` |
//! | Streams | one ordered log per table, `get_records`, `list_streams` (feature `streams`) |
//!
//! # Deliberate differences from the service
//!
//! - Writes are synchronous: a table exists in state `ACTIVE` as soon as
//!   `create_table` returns, and there is no `CREATING`/`DELETING` lifecycle.
//! - Consistent reads are accepted and ignored, because every read is strongly
//!   consistent.
//! - TTL expiry hides an item from every read immediately and deletes it when
//!   it is read or purged, where the service deletes within 48 hours.
//! - Capacity, throttling, `ConsumedCapacity` and item-collection size limits
//!   are not modelled.
//! - A `Query` without a key condition returns every item instead of failing.
//! - Streams are read through [`MockDdb::get_records`] rather than the
//!   `GetRecords`/shard-iterator API, and every table has a single shard.
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

mod clock;
mod error;
mod expr;
mod mock;
mod number;
mod request;
mod schema;
#[cfg(feature = "streams")]
mod stream;
mod table;
mod value;

use std::collections::HashMap;

pub use aws_sdk_dynamodb::primitives::Blob;
pub use aws_sdk_dynamodb::types::AttributeValue;

pub use clock::{Clock, MockClock, SystemClock};
pub use error::{n, s, CancellationReason, CancellationReasonCode, Error, Result};
pub use expr::{ConditionExpression, ProjectionExpression, UpdateExpression};
pub use mock::MockDdb;
pub use request::{
    item, names, values, BatchGetItemOutput, BatchGetItemRequest, BatchWriteItemOutput,
    BatchWriteItemRequest, DeleteItemOutput, DeleteItemRequest, DescribeTableOutput, GetItemOutput,
    GetItemRequest, KeysAndAttributes, ListTablesOutput, ListTablesRequest, PutItemOutput,
    PutItemRequest, QueryOutput, QueryRequest, ReturnValues, ScanOutput, ScanRequest, Select,
    TimeToLiveDescription, TimeToLiveStatus, TransactConditionCheck, TransactDelete, TransactGet,
    TransactGetItemsOutput, TransactGetItemsRequest, TransactPut, TransactUpdate,
    TransactWriteItem, TransactWriteItemsRequest, UpdateItemOutput, UpdateItemRequest,
    UpdateTimeToLiveRequest, WriteRequest,
};
pub use schema::{
    AttributeDefinition, BillingMode, CreateTableRequest, KeySchema, KeySchemaElement, KeyType,
    Projection, SecondaryIndex, StreamSpecification, StreamViewType, TableDescription,
};
#[cfg(feature = "streams")]
pub use stream::{StreamEventName, StreamRecord};

/// A DynamoDB item: attribute name → value.
///
/// The alias is the SDK's own map type, so items flow between production code
/// and the mock without conversion.
pub type Item = HashMap<String, AttributeValue>;
