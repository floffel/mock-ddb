//! The mock client: control plane, data plane, transactions and TTL.
//!
//! Every operation takes `&self` and runs under one `RwLock` over the whole
//! store, so a clone of the mock can be shared by any number of tasks and each
//! operation sees a consistent snapshot. Write operations hold the write lock
//! for their whole duration, which is what makes transactions atomic and batch
//! writes all-or-nothing on validation errors.
//!
//! The injected [`Clock`] sits in its own slot, outside that lock. Every path
//! reads the current instant *before* taking the store lock, so a clock written
//! by test code — one that blocks on I/O or a channel, or that calls back into
//! the mock — cannot stretch the critical section or deadlock a task sharing a
//! clone, and the write lock only ever covers the state transition itself.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::capacity::{
    read_units, size_estimate_range, write_units, Breakdown, ItemCollectionMetrics,
    ReturnConsumedCapacity, ReturnItemCollectionMetrics, Units,
};
use crate::clock::{epoch_seconds, Clock, SystemClock};
use crate::error::{CancellationReason, CancellationReasonCode, Error, Result};
use crate::expr::{
    CmpOp, Condition, ConditionExpression, NameMap, Operand, ProjectionExpression,
    ResolvedProjection, Segment, UpdateExpression, ValueMap,
};
use crate::partiql::{
    BatchExecuteStatementOutput, BatchExecuteStatementRequest, ExecuteStatementOutput,
    ExecuteStatementRequest,
};
use crate::request::{
    BatchGetItemOutput, BatchGetItemRequest, BatchWriteItemOutput, BatchWriteItemRequest,
    DeleteItemOutput, DeleteItemRequest, GetItemOutput, GetItemRequest, KeysAndAttributes,
    ListTablesOutput, ListTablesRequest, PutItemOutput, PutItemRequest, QueryOutput, QueryRequest,
    ReturnValues, ScanOutput, ScanRequest, Select, TimeToLiveDescription, TimeToLiveStatus,
    TransactGetItemsOutput, TransactGetItemsRequest, TransactWriteItem, TransactWriteItemsOutput,
    TransactWriteItemsRequest, UpdateItemOutput, UpdateItemRequest, UpdateTimeToLiveRequest,
    WriteRequest,
};
use crate::schema::{CreateTableRequest, KeySchema, TableDescription};
use crate::table::{index_key, IndexKey, ItemKey, Table, TableDefinition, Ttl};
use crate::value::{item_size, Scalar};
use crate::Item;

#[cfg(feature = "streams")]
use crate::stream::{
    wire_record, DescribeStreamOutput, DescribeStreamRequest, GetRecordsOutput, GetRecordsRequest,
    GetShardIteratorOutput, GetShardIteratorRequest, ListStreamsOutput, ListStreamsRequest, Shard,
    StreamDescription, StreamRecord, StreamState, StreamSummary, StreamsApi, StreamsRecord,
    MAX_RECORDS_PER_GET, MAX_SHARDS_PER_DESCRIBE, MAX_STREAMS_PER_LIST,
    STREAM_CREATION_EPOCH_SECONDS,
};

/// Batch calls a test has throttled, waiting for their next call to spend it.
///
/// Kept outside [`Inner`] for the same reason as the clock: a `BatchGetItem`
/// runs under the store's read lock and must still be able to clear its own
/// one-shot throttle, which a read guard cannot do — and no batch call has to
/// hold the store lock in order to arm or spend a throttle.
#[derive(Debug, Default)]
struct BatchThrottle {
    /// Writes the next `BatchWriteItem` leaves unprocessed.
    writes: Option<usize>,
    /// Keys the next `BatchGetItem` leaves unprocessed.
    keys: Option<usize>,
}

/// One table of a batch read, validated before any key is answered.
///
/// Built in a first pass over the request, so a table that does not exist, a
/// malformed key or a malformed projection is refused before the call reads
/// anything, charges any capacity or spends an armed throttle.
struct BatchReadStep<'a> {
    table: &'a Table,
    table_name: &'a str,
    attributes: &'a KeysAndAttributes,
    /// The request's keys, normalised to the table's key attributes.
    keys: Vec<ItemKey>,
    /// The projection, parsed and its `#alias`es resolved once for the table.
    projection: Option<ResolvedProjection>,
}

/// Validate every write in a batch and return its keys in request order.
///
/// Nothing is applied here: the caller validates the whole batch first, so a
/// batch that fails is rejected without a partial write, and only then decides
/// which of the validated keys a throttled response leaves behind.
fn validated_batch_keys(
    inner: &Inner,
    request: &BatchWriteItemRequest,
) -> Result<Vec<(String, ItemKey)>> {
    let total: usize = request.request_items.values().map(Vec::len).sum();
    let mut seen: BTreeSet<(String, ItemKey)> = BTreeSet::new();
    let mut ordered: Vec<(String, ItemKey)> = Vec::with_capacity(total);
    for (table_name, writes) in &request.request_items {
        let table = inner
            .tables
            .get(table_name)
            .ok_or_else(|| Error::table_not_found(table_name))?;
        for write in writes {
            let key = match write {
                WriteRequest::Put { item } => {
                    table.validate_item(item)?;
                    table.item_key(item)?
                }
                WriteRequest::Delete { key } => table.lookup_key(key)?,
            };
            let slot = (table_name.clone(), key);
            if !seen.insert(slot.clone()) {
                return Err(Error::validation(
                    "Provided list of item keys contains duplicates",
                ));
            }
            ordered.push(slot);
        }
    }
    Ok(ordered)
}

/// The mutable state behind the client.
struct Inner {
    tables: BTreeMap<String, Table>,
    /// Counter that seeds stream sequence numbers, so records from two tables
    /// never compare equal.
    sequence: u64,
    /// Client request tokens that have already been applied, each with the
    /// transaction that spent it and the moment it was spent.
    ///
    /// A token names a request, not a slot: the service compares a resubmitted
    /// token against the request that consumed it, refuses the transaction when
    /// the two differ, and forgets the token ten minutes after it spent it. The
    /// mock keeps the same record, so a caller that reuses one constant token
    /// cannot have a different write silently discarded, and the map cannot grow
    /// without bound.
    applied_tokens: BTreeMap<String, AppliedToken>,
    /// PartiQL `NextToken` → the key the next page starts after.
    ///
    /// A `NextToken` is opaque state on the service's side, so it is opaque
    /// state here too: the mock keeps the key it parked and hands out a token
    /// that names it. Nothing is serialised, so a token cannot outlive the mock
    /// — and only the most recent [`MAX_NEXT_TOKENS`] are kept, so a test that
    /// pages forever cannot grow the map forever.
    next_tokens: VecDeque<(String, Item)>,
    /// Counter that names the next `NextToken`.
    token_sequence: u64,
}

/// How many PartiQL paging tokens one mock remembers.
const MAX_NEXT_TOKENS: usize = 256;

/// How long one mock remembers a `ClientRequestToken`, in seconds.
///
/// The service forgets a token ten minutes after the transaction that spent it,
/// so a request that resubmits the token later is a new request rather than a
/// replay. Age is measured against the injected [`Clock`], so a test can walk up
/// to the boundary instead of sleeping.
const TOKEN_EXPIRY_SECONDS: i64 = 600;

/// A `ClientRequestToken` and the transaction that consumed it.
struct AppliedToken {
    /// The transaction as the token remembers it: its actions, in request
    /// order. The token itself and the flags that only shape the response are
    /// cleared, because they do not describe the write.
    payload: TransactWriteItemsRequest,
    /// Epoch seconds at which the transaction consumed the token.
    applied_at: i64,
}

/// The part of a transaction a token remembers, with the token and the
/// response-shaping flags cleared.
fn transaction_payload(request: &TransactWriteItemsRequest) -> TransactWriteItemsRequest {
    TransactWriteItemsRequest {
        transact_items: request.transact_items.clone(),
        ..TransactWriteItemsRequest::default()
    }
}

/// An in-process DynamoDB.
///
/// Cloneable, `Send + Sync`, and shared behind an `Arc` exactly like
/// `aws_sdk_dynamodb::Client`. Every method is `async` to match the SDK, so the
/// code under test does not need a different call shape in tests.
///
/// ```
/// use mock_ddb::{s, GetItemRequest, KeySchema, KeyType, MockDdb, PutItemRequest, item};
///
/// # #[tokio::main]
/// # async fn main() {
/// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
/// mock.put_item(PutItemRequest::new("users", item([("pk", s("u#1"))])))
///     .await
///     .unwrap();
/// let found = mock
///     .get_item(GetItemRequest::new("users", item([("pk", s("u#1"))])))
///     .await
///     .unwrap();
/// assert!(found.item.is_some());
/// # }
/// ```
#[derive(Clone)]
pub struct MockDdb {
    inner: Arc<RwLock<Inner>>,
    /// The injected clock, in its own slot rather than inside [`Inner`].
    ///
    /// [`Clock`] is implemented by test code, so `Clock::now` may block on I/O,
    /// wait on a channel, or call back into this mock. Keeping the clock out of
    /// the store means no such call can run while the store lock is held: a
    /// write path clones the `Arc` out of this slot under a short read guard,
    /// releases it, and only then asks the clock for the time — so a slow clock
    /// cannot stall other tasks, and a re-entrant one cannot deadlock.
    clock: Arc<RwLock<Arc<dyn Clock>>>,
    /// One-shot batch throttles, in a slot of their own for the same reason as
    /// the clock: a batch read holds the store's read guard while it runs and
    /// could not clear its own throttle from inside it.
    batch_throttle: Arc<Mutex<BatchThrottle>>,
}

impl Default for MockDdb {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MockDdb {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Copy the names out and drop the guard before formatting: the caller
        // that reaches `Debug` may be holding nothing we know about, and a
        // formatting call is no place to keep the store lock.
        let tables: Vec<String> = {
            let inner = self.inner.read();
            inner.tables.keys().cloned().collect()
        };
        let clock = self.clock.read().clone();
        formatter
            .debug_struct("MockDdb")
            .field("tables", &tables)
            .field("clock", &clock)
            .field("batch_throttle", &*self.batch_throttle.lock())
            .finish()
    }
}

// Every method is `async` on purpose: callers' production code does
// `client.put_item(...).send().await`, and a test that swaps in the mock must
// not have to drop the `.await`. There is nothing to await inside, so clippy's
// `unused_async` (and its newer `unused_async_trait_impl` spelling) is expected
// here; `unknown_lints` keeps older clippy versions, which do not know the newer
// name, from rejecting the attribute.
#[allow(unknown_lints, clippy::unused_async, clippy::unused_async_trait_impl)]
impl MockDdb {
    /// A mock with no tables, using the real clock.
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::MockDdb;
    ///
    /// let mock = MockDdb::new();
    /// assert_eq!(mock.count("nothing"), 0);
    /// ```
    pub fn new() -> Self {
        Self::with_clock(Arc::new(SystemClock))
    }

    /// A mock with no tables, using `clock` for TTL decisions.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use mock_ddb::{MockClock, MockDdb};
    ///
    /// let clock = Arc::new(MockClock::at_epoch_seconds(1_700_000_000));
    /// let mock = MockDdb::with_clock(clock.clone());
    /// clock.advance(std::time::Duration::from_secs(1));
    /// # let _ = mock;
    /// ```
    #[must_use]
    pub fn with_clock(clock: Arc<dyn Clock>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                tables: BTreeMap::new(),
                sequence: 1,
                applied_tokens: BTreeMap::new(),
                next_tokens: VecDeque::new(),
                token_sequence: 0,
            })),
            clock: Arc::new(RwLock::new(clock)),
            batch_throttle: Arc::new(Mutex::new(BatchThrottle::default())),
        }
    }

    /// A mock with one table, ready to seed.
    ///
    /// ```
    /// use mock_ddb::{KeySchema, KeyType, MockDdb};
    ///
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// assert_eq!(mock.count("users"), 0);
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`](crate::Error::Validation) when the table
    /// definition is invalid, e.g. a duplicate index name.
    pub fn with_table(table_name: impl Into<String>, key_schema: KeySchema) -> Result<Self> {
        let mock = Self::new();
        let request = CreateTableRequest::new(table_name, key_schema);
        let definition = TableDefinition::from_request(&request)?;
        let now = mock.clock_now();
        mock.insert_table(definition, now)?;
        Ok(mock)
    }

    ///
    /// ```
    /// use std::sync::Arc;
    /// use mock_ddb::{MockClock, MockDdb};
    ///
    /// let mock = MockDdb::new();
    /// mock.set_clock(Arc::new(MockClock::at_epoch_seconds(1_000)));
    /// assert_eq!(mock.now_seconds(), 1_000);
    /// ```
    /// Replace the clock, e.g. to pin time in the middle of a test.
    pub fn set_clock(&self, clock: Arc<dyn Clock>) {
        *self.clock.write() = clock;
    }

    /// The injected clock, with the guard already released.
    ///
    /// Nothing in this module calls `Clock::now` from inside a lock: a test's
    /// clock may block on I/O or a channel, or call back into the mock through
    /// [`now_seconds`](Self::now_seconds) or [`count`](Self::count), and
    /// `parking_lot::RwLock` is not reentrant.
    fn clock_now(&self) -> std::time::SystemTime {
        let clock = self.clock.read().clone();
        clock.now()
    }

    /// The clock's current instant in whole epoch seconds.
    ///
    /// Every write path calls this before taking the store lock, so the
    /// critical section holds the state transition and nothing that could
    /// block on user code.
    fn epoch_now(&self) -> i64 {
        epoch_seconds(self.clock_now())
    }

    /// The clock's current instant, in whole seconds since the Unix epoch.
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::MockDdb;
    ///
    /// let mock = MockDdb::new();
    /// assert!(mock.now_seconds() > 0);
    /// ```
    pub fn now_seconds(&self) -> i64 {
        self.epoch_now()
    }

    /// Seed items into an existing table without conditions.
    ///
    /// This is the shortcut a test uses instead of a loop of `put_item` calls:
    /// the items are validated and indexed exactly as a real write would be.
    /// Validation happens for every item before the first one is stored — the
    /// same all-or-nothing order [`batch_write_item`](Self::batch_write_item)
    /// uses — so a bad fixture leaves the table exactly as it was.
    ///
    /// ```
    /// use mock_ddb::{item, s, KeySchema, KeyType, MockDdb};
    ///
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// mock.seed("users", vec![item([("pk", s("u#1"))])]).unwrap();
    /// assert_eq!(mock.count("users"), 1);
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::ResourceNotFound`] for an unknown table and
    /// [`Error::Validation`] for an item that does not match the schema.
    pub fn seed(&self, table_name: &str, items: Vec<Item>) -> Result<()> {
        let now = self.epoch_now();
        let mut inner = self.inner.write();
        let table = inner
            .tables
            .get_mut(table_name)
            .ok_or_else(|| Error::table_not_found(table_name))?;
        // Validate-then-apply, like `batch_write_item_inner`: a fixture whose
        // last item is invalid must not leave the earlier ones behind.
        let mut keys = Vec::with_capacity(items.len());
        for item in &items {
            table.validate_item(item)?;
            keys.push(table.item_key(item)?);
        }
        for (key, item) in keys.into_iter().zip(items) {
            let previous = table.items.get(&key).cloned();
            // An expired row is logically absent: the stream sees an INSERT,
            // exactly what the same write through `put_item` produces.
            let previous_live = previous
                .clone()
                .filter(|existing| !table.is_expired(existing, now));
            table.store(key, item.clone(), previous.as_ref());
            if !table.is_expired(&item, now) {
                table.on_put(previous_live.as_ref(), &item);
            }
        }
        Ok(())
    }

    /// Number of live items in a table. `0` for an unknown table.
    ///
    /// Items whose TTL has elapsed are not counted, even though the mock only
    /// removes them when they are read or [`purge_expired`](Self::purge_expired)
    /// runs.
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::{item, s, KeySchema, KeyType, MockDdb};
    ///
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// mock.seed("users", vec![item([("pk", s("u#1"))])]).unwrap();
    /// assert_eq!(mock.count("users"), 1);
    /// ```
    pub fn count(&self, table_name: &str) -> usize {
        let now = self.epoch_now();
        let inner = self.inner.read();
        inner.tables.get(table_name).map_or(0, |table| {
            usize::try_from(table.live_count(now)).unwrap_or(usize::MAX)
        })
    }

    /// Every live item of a table, ordered by primary key.
    ///
    /// ```
    /// use mock_ddb::{item, n, s, KeySchema, KeyType, MockDdb};
    ///
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// mock.seed("users", vec![item([("pk", s("b")), ("n", n("2"))]), item([("pk", s("a"))])])
    ///     .unwrap();
    /// let keys: Vec<_> = mock.dump("users").into_iter().map(|item| item["pk"].clone()).collect();
    /// assert_eq!(keys, vec![s("a"), s("b")]);
    /// ```
    #[must_use]
    pub fn dump(&self, table_name: &str) -> Vec<Item> {
        let now = self.epoch_now();
        let inner = self.inner.read();
        inner.tables.get(table_name).map_or_else(Vec::new, |table| {
            table
                .items
                .values()
                .filter(|item| !table.is_expired(item, now))
                .cloned()
                .collect()
        })
    }

    ///
    /// ```
    /// use mock_ddb::{item, s, KeySchema, KeyType, MockDdb};
    ///
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// mock.seed("users", vec![item([("pk", s("u#1"))])]).unwrap();
    /// mock.clear();
    /// assert_eq!(mock.count("users"), 0);
    /// ```
    /// Drop every table.
    pub fn clear(&self) {
        let mut inner = self.inner.write();
        inner.tables.clear();
        inner.applied_tokens.clear();
        inner.next_tokens.clear();
    }

    // -- control plane ------------------------------------------------------

    /// Create a table.
    ///
    /// ```
    /// use mock_ddb::{CreateTableRequest, KeySchema, KeyType, MockDdb};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::new();
    /// let table = mock
    ///     .create_table(CreateTableRequest::new("users", KeySchema::hash("pk", KeyType::String)))
    ///     .await
    ///     .unwrap();
    /// assert_eq!(table.table_name, "users");
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::ResourceInUse`] when the table exists and
    /// [`Error::Validation`] for an invalid definition.
    pub async fn create_table(&self, request: CreateTableRequest) -> Result<TableDescription> {
        let definition = TableDefinition::from_request(&request)?;
        let now = self.clock_now();
        self.insert_table(definition, now)
    }

    fn insert_table(
        &self,
        definition: TableDefinition,
        now: std::time::SystemTime,
    ) -> Result<TableDescription> {
        let mut inner = self.inner.write();
        if inner.tables.contains_key(&definition.table_name) {
            return Err(Error::ResourceInUse {
                message: format!("Cannot create preexisting table: {}", definition.table_name),
            });
        }
        let sequence = inner.sequence;
        inner.sequence += 1;
        let table = Table::new(definition, now, sequence);
        let description = table.describe(0);
        inner.tables.insert(description.table_name.clone(), table);
        Ok(description)
    }

    ///
    /// ```
    /// use mock_ddb::{KeySchema, KeyType, MockDdb};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// let removed = mock.delete_table("users").await.unwrap();
    /// assert_eq!(removed.table_name, "users");
    /// # }
    /// ```
    /// Delete a table and return its final description.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ResourceNotFound`] when the table does not exist.
    pub async fn delete_table(&self, table_name: &str) -> Result<TableDescription> {
        let now = self.epoch_now();
        let mut inner = self.inner.write();
        let table = inner
            .tables
            .remove(table_name)
            .ok_or_else(|| Error::table_not_found(table_name))?;
        Ok(table.describe(table.live_count(now)))
    }

    ///
    /// ```
    /// use mock_ddb::{KeySchema, KeyType, MockDdb};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// assert_eq!(mock.describe_table("users").await.unwrap().table_status, "ACTIVE");
    /// # }
    /// ```
    /// Describe a table.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ResourceNotFound`] when the table does not exist.
    pub async fn describe_table(&self, table_name: &str) -> Result<TableDescription> {
        let now = self.epoch_now();
        let inner = self.inner.read();
        let table = inner
            .tables
            .get(table_name)
            .ok_or_else(|| Error::table_not_found(table_name))?;
        Ok(table.describe(table.live_count(now)))
    }

    ///
    /// ```
    /// use mock_ddb::{KeySchema, KeyType, ListTablesRequest, MockDdb};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// let listing = mock.list_tables(ListTablesRequest::default()).await.unwrap();
    /// assert_eq!(listing.table_names, vec!["users"]);
    /// # }
    /// ```
    /// List table names, sorted.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] when `limit` is outside `1..=100`.
    pub async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesOutput> {
        let limit = request.limit.unwrap_or(100);
        if limit == 0 || limit > 100 {
            return Err(Error::validation(
                "1 validation error detected: Value at 'limit' failed to satisfy constraint: Member must have value between 1 and 100",
            ));
        }
        let inner = self.inner.read();
        let start = request.exclusive_start_table_name.unwrap_or_default();
        let names: Vec<String> = inner
            .tables
            .keys()
            .filter(|name| name.as_str() > start.as_str())
            .cloned()
            .collect();
        let limit = usize::try_from(limit).unwrap_or(100);
        let truncated = names.len() > limit;
        let mut table_names: Vec<String> = names.into_iter().take(limit).collect();
        let last_evaluated_table_name = if truncated {
            table_names.last().cloned()
        } else {
            None
        };
        table_names.shrink_to_fit();
        Ok(ListTablesOutput {
            table_names,
            last_evaluated_table_name,
        })
    }

    // -- data plane ---------------------------------------------------------

    /// Write an item.
    ///
    /// ```
    /// use mock_ddb::{item, s, KeySchema, KeyType, MockDdb, PutItemRequest};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// mock.put_item(
    ///     PutItemRequest::new("users", item([("pk", s("u#1"))]))
    ///         .with_condition("attribute_not_exists(pk)"),
    /// )
    /// .await
    /// .unwrap();
    ///
    /// let second = mock
    ///     .put_item(
    ///         PutItemRequest::new("users", item([("pk", s("u#1"))]))
    ///             .with_condition("attribute_not_exists(pk)"),
    ///     )
    ///     .await;
    /// assert!(second.is_err());
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::ResourceNotFound`] for an unknown table,
    /// [`Error::Validation`] for an invalid item or expression, and
    /// [`Error::ConditionalCheckFailed`] when the condition is false.
    pub async fn put_item(&self, request: PutItemRequest) -> Result<PutItemOutput> {
        self.put_item_inner(&request)
    }

    pub(crate) fn put_item_inner(&self, request: &PutItemRequest) -> Result<PutItemOutput> {
        let now = self.epoch_now();
        let mut inner = self.inner.write();
        let table = inner
            .tables
            .get_mut(&request.table_name)
            .ok_or_else(|| Error::table_not_found(&request.table_name))?;
        table.validate_item(&request.item)?;
        let key = table.item_key(&request.item)?;
        let previous = table.items.get(&key).cloned();
        let previous_live = previous.clone().filter(|item| !table.is_expired(item, now));

        if let Some(expression) = &request.condition_expression {
            let names = request
                .expression_attribute_names
                .clone()
                .unwrap_or_default();
            let values = request
                .expression_attribute_values
                .clone()
                .unwrap_or_default();
            let condition = ConditionExpression::parse(expression)?;
            if !condition.evaluate(previous_live.as_ref(), &names, &values)? {
                let item = if request.return_values_on_condition_check_failure {
                    previous_live
                } else {
                    None
                };
                return Err(Error::conditional_failed(
                    &request.table_name,
                    expression,
                    item,
                ));
            }
        }

        table.store(key, request.item.clone(), previous.as_ref());
        table.on_put(previous_live.as_ref(), &request.item);
        let mut breakdown = Breakdown::new();
        charge_write(
            table,
            &mut breakdown,
            [previous_live.as_ref(), Some(&request.item)],
        );
        let item_collection_metrics = collection_metrics(
            table,
            &request.item,
            request.return_item_collection_metrics,
            now,
        );
        Ok(PutItemOutput {
            attributes: match request.return_values {
                // An expired item is logically absent, so it is not "the old
                // value" even though its row is still in the store.
                ReturnValues::AllOld => previous_live,
                _ => None,
            },
            consumed_capacity: breakdown
                .consume(&request.table_name, request.return_consumed_capacity),
            item_collection_metrics,
        })
    }

    ///
    /// ```
    /// use mock_ddb::{item, s, GetItemRequest, KeySchema, KeyType, MockDdb, PutItemRequest};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// mock.put_item(PutItemRequest::new("users", item([("pk", s("u#1"))])))
    ///     .await
    ///     .unwrap();
    /// let found = mock
    ///     .get_item(GetItemRequest::new("users", item([("pk", s("u#1"))])))
    ///     .await
    ///     .unwrap();
    /// assert!(found.item.is_some());
    /// # }
    /// ```
    /// Read an item.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ResourceNotFound`] for an unknown table,
    /// [`Error::Validation`] for a key that does not match the schema, and for
    /// a malformed projection or an undefined `#name` placeholder. The
    /// projection is parsed before the key is looked up, so a missing key does
    /// not turn a bad expression into `None`.
    pub async fn get_item(&self, request: GetItemRequest) -> Result<GetItemOutput> {
        self.get_item_inner(&request)
    }

    fn get_item_inner(&self, request: &GetItemRequest) -> Result<GetItemOutput> {
        let now = self.epoch_now();
        let inner = self.inner.read();
        let table = inner
            .tables
            .get(&request.table_name)
            .ok_or_else(|| Error::table_not_found(&request.table_name))?;
        let key = table.lookup_key(&request.key)?;
        // The projection is built before the key is looked up, so a malformed
        // expression or an undefined `#alias` is rejected on a key the table
        // does not hold too — the service validates the request, not the
        // response.
        let projection = match request.projection_expression.as_deref() {
            None => None,
            Some(expression) => {
                let names = request
                    .expression_attribute_names
                    .clone()
                    .unwrap_or_default();
                Some(ResolvedProjection::new(
                    &ProjectionExpression::parse(expression)?,
                    &names,
                )?)
            }
        };
        let stored = table
            .items
            .get(&key)
            .filter(|item| !table.is_expired(item, now));

        let mut breakdown = Breakdown::new();
        charge_read(
            table,
            &mut breakdown,
            None,
            read_units(stored.map_or(0, item_size)),
        );
        let item = stored.map(|item| match &projection {
            None => item.clone(),
            Some(projection) => projection.project(item),
        });
        Ok(GetItemOutput {
            item,
            consumed_capacity: breakdown
                .consume(&request.table_name, request.return_consumed_capacity),
        })
    }

    ///
    /// ```
    /// use mock_ddb::{item, s, DeleteItemRequest, KeySchema, KeyType, MockDdb, PutItemRequest, ReturnValues};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// mock.put_item(PutItemRequest::new("users", item([("pk", s("u#1"))])))
    ///     .await
    ///     .unwrap();
    /// let removed = mock
    ///     .delete_item(
    ///         DeleteItemRequest::new("users", item([("pk", s("u#1"))]))
    ///             .with_return_values(ReturnValues::AllOld),
    ///     )
    ///     .await
    ///     .unwrap();
    /// assert!(removed.attributes.is_some());
    /// # }
    /// ```
    /// Delete an item.
    ///
    /// Deleting a key that does not exist succeeds and returns no attributes,
    /// exactly like the service.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ResourceNotFound`] for an unknown table,
    /// [`Error::Validation`] for an invalid key or expression, and
    /// [`Error::ConditionalCheckFailed`] when the condition is false.
    pub async fn delete_item(&self, request: DeleteItemRequest) -> Result<DeleteItemOutput> {
        self.delete_item_inner(&request)
    }

    pub(crate) fn delete_item_inner(
        &self,
        request: &DeleteItemRequest,
    ) -> Result<DeleteItemOutput> {
        let now = self.epoch_now();
        let mut inner = self.inner.write();
        let table = inner
            .tables
            .get_mut(&request.table_name)
            .ok_or_else(|| Error::table_not_found(&request.table_name))?;
        let key = table.lookup_key(&request.key)?;
        let previous = table.items.get(&key).cloned();
        let previous_live = previous.clone().filter(|item| !table.is_expired(item, now));

        if let Some(expression) = &request.condition_expression {
            let names = request
                .expression_attribute_names
                .clone()
                .unwrap_or_default();
            let values = request
                .expression_attribute_values
                .clone()
                .unwrap_or_default();
            let condition = ConditionExpression::parse(expression)?;
            if !condition.evaluate(previous_live.as_ref(), &names, &values)? {
                let item = if request.return_values_on_condition_check_failure {
                    previous_live
                } else {
                    None
                };
                return Err(Error::conditional_failed(
                    &request.table_name,
                    expression,
                    item,
                ));
            }
        }

        let removed = table.discard(&key);
        if let Some(item) = &previous_live {
            table.on_delete(item);
        }
        let mut breakdown = Breakdown::new();
        charge_write(table, &mut breakdown, [previous_live.as_ref(), None]);
        let item_collection_metrics = previous_live.as_ref().and_then(|item| {
            collection_metrics(table, item, request.return_item_collection_metrics, now)
        });
        Ok(DeleteItemOutput {
            attributes: match request.return_values {
                ReturnValues::AllOld => removed.filter(|_| previous_live.is_some()),
                _ => None,
            },
            consumed_capacity: breakdown
                .consume(&request.table_name, request.return_consumed_capacity),
            item_collection_metrics,
        })
    }

    /// Update an item, creating it when the key does not exist.
    ///
    /// ```
    /// use mock_ddb::{item, n, s, KeySchema, KeyType, MockDdb, ReturnValues, UpdateItemRequest};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::with_table("counters", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// let response = mock
    ///     .update_item(
    ///         UpdateItemRequest::new("counters", item([("pk", s("c#1"))]), "SET hits = if_not_exists(hits, :zero) + :one")
    ///             .with_values(mock_ddb::values([(":zero", n("0")), (":one", n("1"))]))
    ///             .with_return_values(ReturnValues::AllNew),
    ///     )
    ///     .await
    ///     .unwrap();
    /// assert_eq!(response.attributes.unwrap().get("hits").cloned(), Some(n("1")));
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::ResourceNotFound`] for an unknown table,
    /// [`Error::Validation`] when the update expression is invalid, targets a
    /// key attribute or produces an oversized item, and
    /// [`Error::ConditionalCheckFailed`] when the condition is false.
    pub async fn update_item(&self, request: UpdateItemRequest) -> Result<UpdateItemOutput> {
        self.update_item_inner(&request)
    }

    pub(crate) fn update_item_inner(
        &self,
        request: &UpdateItemRequest,
    ) -> Result<UpdateItemOutput> {
        let now = self.epoch_now();
        let mut inner = self.inner.write();
        let table = inner
            .tables
            .get_mut(&request.table_name)
            .ok_or_else(|| Error::table_not_found(&request.table_name))?;
        let key = table.lookup_key(&request.key)?;
        let previous = table.items.get(&key).cloned();
        let previous_live = previous.clone().filter(|item| !table.is_expired(item, now));

        if let Some(expression) = &request.condition_expression {
            let names = request
                .expression_attribute_names
                .clone()
                .unwrap_or_default();
            let values = request
                .expression_attribute_values
                .clone()
                .unwrap_or_default();
            let condition = ConditionExpression::parse(expression)?;
            if !condition.evaluate(previous_live.as_ref(), &names, &values)? {
                let item = if request.return_values_on_condition_check_failure {
                    previous_live
                } else {
                    None
                };
                return Err(Error::conditional_failed(
                    &request.table_name,
                    expression,
                    item,
                ));
            }
        }

        let names = request
            .expression_attribute_names
            .clone()
            .unwrap_or_default();
        let values = request
            .expression_attribute_values
            .clone()
            .unwrap_or_default();
        let update = UpdateExpression::parse(&request.update_expression)?;
        let mut item = match &previous_live {
            Some(existing) => existing.clone(),
            None => key_only_item(table.key_schema(), &request.key),
        };
        let touched = update.apply(&mut item, table.key_schema(), &names, &values)?;
        table.validate_item(&item)?;
        table.store(key, item.clone(), previous.as_ref());
        table.on_update(previous_live.as_ref(), &item);

        let mut breakdown = Breakdown::new();
        charge_write(table, &mut breakdown, [previous_live.as_ref(), Some(&item)]);
        let item_collection_metrics =
            collection_metrics(table, &item, request.return_item_collection_metrics, now);

        let attributes = match request.return_values {
            ReturnValues::None => None,
            ReturnValues::AllNew => Some(item),
            ReturnValues::AllOld => previous_live,
            ReturnValues::UpdatedNew => Some(select_attributes(&item, &touched)),
            ReturnValues::UpdatedOld => previous_live
                .as_ref()
                .map(|old| select_attributes(old, &touched)),
        };
        Ok(UpdateItemOutput {
            attributes,
            consumed_capacity: breakdown
                .consume(&request.table_name, request.return_consumed_capacity),
            item_collection_metrics,
        })
    }

    /// Query a table or index by key.
    ///
    /// ```
    /// use mock_ddb::{item, n, s, KeySchema, KeyType, MockDdb, QueryRequest};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::with_table(
    ///     "events",
    ///     KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::Number),
    /// )
    /// .unwrap();
    /// mock.seed(
    ///     "events",
    ///     vec![
    ///         item([("pk", s("a")), ("sk", n("2"))]),
    ///         item([("pk", s("a")), ("sk", n("1"))]),
    ///     ],
    /// )
    /// .unwrap();
    ///
    /// let page = mock
    ///     .query(
    ///         QueryRequest::new("events", "pk = :pk")
    ///             .with_values(mock_ddb::values([(":pk", s("a"))])),
    ///     )
    ///     .await
    ///     .unwrap();
    /// assert_eq!(page.count, 2);
    /// assert_eq!(page.items[0]["sk"], n("1"));
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::ResourceNotFound`] for an unknown table or index,
    /// [`Error::Validation`] for a key condition that does not constrain the
    /// partition key or references a non-key attribute, and for a malformed
    /// filter or projection.
    ///
    /// The expressions and every `#name`/`:value` they use are validated before
    /// the first item is read, so a page that reaches no item — an empty table,
    /// or one where the key condition or TTL leaves nothing — still rejects them
    /// instead of returning an empty `Ok`.
    pub async fn query(&self, request: QueryRequest) -> Result<QueryOutput> {
        self.query_inner(&request)
    }

    pub(crate) fn query_inner(&self, request: &QueryRequest) -> Result<QueryOutput> {
        validate_limit(request.limit)?;
        let now = self.epoch_now();
        let inner = self.inner.read();
        let table = inner
            .tables
            .get(&request.table_name)
            .ok_or_else(|| Error::table_not_found(&request.table_name))?;
        let index_name = request.index_name.as_deref();
        let key_schema = match index_name {
            None => table.key_schema(),
            Some(name) => &table.definition.index(name)?.key_schema,
        };
        let names = request
            .expression_attribute_names
            .clone()
            .unwrap_or_default();
        let values = request
            .expression_attribute_values
            .clone()
            .unwrap_or_default();

        let key_condition = match &request.key_condition_expression {
            None => None,
            Some(expression) => {
                let condition = ConditionExpression::parse(expression)?;
                validate_key_condition(condition.condition(), key_schema, &names)?;
                condition.validate_maps(&names, &values)?;
                Some(condition)
            }
        };
        let (filter, projection) = read_expressions(
            request.filter_expression.as_deref(),
            request.projection_expression.as_deref(),
            &names,
            &values,
        )?;

        let page = Page {
            limit: request.limit,
            filter: filter.as_ref(),
            key_condition: key_condition.as_ref(),
            projection: projection.as_ref(),
            select: request.select,
            names: &names,
            values: &values,
            descending: !request.scan_index_forward.unwrap_or(true),
        };
        let collected = collect_page(
            table,
            index_name,
            request.exclusive_start_key.as_ref(),
            &page,
            now,
            None,
        )?;
        let mut breakdown = Breakdown::new();
        charge_read(table, &mut breakdown, index_name, collected.read_units);
        Ok(QueryOutput {
            items: collected.items,
            count: collected.count,
            scanned_count: collected.scanned_count,
            last_evaluated_key: collected.last_evaluated_key,
            consumed_capacity: breakdown
                .consume(&request.table_name, request.return_consumed_capacity),
        })
    }

    /// Scan a table or index.
    ///
    /// ```
    /// use mock_ddb::{item, n, s, KeySchema, KeyType, MockDdb, ScanRequest};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// mock.seed("users", vec![item([("pk", s("a")), ("n", n("1"))])]).unwrap();
    ///
    /// let page = mock
    ///     .scan(ScanRequest::new("users").with_filter("n > :zero").with_values(mock_ddb::values([(":zero", n("0"))])))
    ///     .await
    ///     .unwrap();
    /// assert_eq!(page.count, 1);
    /// assert_eq!(page.scanned_count, 1);
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::ResourceNotFound`] for an unknown table or index,
    /// [`Error::Validation`] for an invalid segment range, filter, projection or
    /// limit, and for `Select::SpecificAttributes` without a projection.
    ///
    /// The filter, the projection and every `#name`/`:value` they use are
    /// validated before the first item is read, so an empty table or an empty
    /// segment still rejects them instead of returning an empty `Ok`.
    pub async fn scan(&self, request: ScanRequest) -> Result<ScanOutput> {
        self.scan_inner(&request)
    }

    pub(crate) fn scan_inner(&self, request: &ScanRequest) -> Result<ScanOutput> {
        validate_limit(request.limit)?;
        let segment = match (request.segment, request.total_segments) {
            (None, None) => None,
            (Some(segment), Some(total)) => {
                if total == 0 || total > 1_000_000 {
                    return Err(Error::validation(
                        "1 validation error detected: Value at 'totalSegments' failed to satisfy constraint: Member must have value between 1 and 1000000",
                    ));
                }
                if segment >= total {
                    return Err(Error::validation(
                        "The Segment parameter is zero-based and must be less than parameter TotalSegments",
                    ));
                }
                Some((segment, total))
            }
            _ => {
                return Err(Error::validation(
                    "The TotalSegments parameter is required but was not present in the request when Segment parameter is present, or vice versa",
                ));
            }
        };

        let now = self.epoch_now();
        let inner = self.inner.read();
        let table = inner
            .tables
            .get(&request.table_name)
            .ok_or_else(|| Error::table_not_found(&request.table_name))?;
        let index_name = request.index_name.as_deref();
        if let Some(name) = index_name {
            let _ = table.definition.index(name)?;
        }
        let names = request
            .expression_attribute_names
            .clone()
            .unwrap_or_default();
        let values = request
            .expression_attribute_values
            .clone()
            .unwrap_or_default();

        let (filter, projection) = read_expressions(
            request.filter_expression.as_deref(),
            request.projection_expression.as_deref(),
            &names,
            &values,
        )?;

        let page = Page {
            limit: request.limit,
            filter: filter.as_ref(),
            key_condition: None,
            projection: projection.as_ref(),
            select: request.select,
            names: &names,
            values: &values,
            descending: false,
        };
        let collected = collect_page(
            table,
            index_name,
            request.exclusive_start_key.as_ref(),
            &page,
            now,
            segment,
        )?;
        let mut breakdown = Breakdown::new();
        charge_read(table, &mut breakdown, index_name, collected.read_units);
        Ok(ScanOutput {
            items: collected.items,
            count: collected.count,
            scanned_count: collected.scanned_count,
            last_evaluated_key: collected.last_evaluated_key,
            consumed_capacity: breakdown
                .consume(&request.table_name, request.return_consumed_capacity),
        })
    }

    /// Arm the next [`batch_write_item`](Self::batch_write_item) to report up
    /// to `unprocessed` writes as unprocessed.
    ///
    /// ```
    /// use mock_ddb::{item, s, BatchWriteItemRequest, KeySchema, KeyType, MockDdb, WriteRequest};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// mock.throttle_next_batch_write(1);
    ///
    /// let batch = BatchWriteItemRequest::new()
    ///     .add("users", WriteRequest::Put { item: item([("pk", s("u#1"))]) })
    ///     .add("users", WriteRequest::Put { item: item([("pk", s("u#2"))]) });
    /// let first = mock.batch_write_item(batch).await.unwrap();
    /// assert_eq!(first.unprocessed_items["users"].len(), 1);
    /// assert_eq!(mock.count("users"), 1);
    ///
    /// // A real caller retries the remainder; the knob is already spent.
    /// let retry = BatchWriteItemRequest {
    ///     request_items: first.unprocessed_items,
    ///     ..BatchWriteItemRequest::default()
    /// };
    /// assert!(mock.batch_write_item(retry).await.unwrap().unprocessed_items.is_empty());
    /// assert_eq!(mock.count("users"), 2);
    /// # }
    /// ```
    ///
    /// The next batch write applies every write it does not report: the request
    /// is validated as a whole first, then the first `unprocessed` writes in
    /// request order (by table name, then position in that table's list) are
    /// handed back in [`BatchWriteItemOutput::unprocessed_items`] untouched and
    /// charged no write capacity. The knob is **one-shot** — the call that
    /// spends it clears it, so the caller's retry runs unthrottled and a test
    /// can assert the remainder lands. A `count` above that batch's size leaves
    /// the whole batch unprocessed, and `0` leaves nothing unprocessed; arming
    /// again before the next call replaces the pending count. A batch the mock
    /// rejects outright — empty, over 25 writes, unknown table, an item that
    /// does not match it, a repeated key — leaves the count armed, so a test's
    /// setup is not eaten by a request that never ran.
    ///
    /// Without an armed knob the default holds: a batch write is all-or-nothing
    /// and `unprocessed_items` is empty.
    pub fn throttle_next_batch_write(&self, unprocessed: usize) {
        self.batch_throttle.lock().writes = Some(unprocessed);
    }

    /// Arm the next [`batch_get_item`](Self::batch_get_item) to report up to
    /// `unprocessed` keys as unprocessed.
    ///
    /// ```
    /// use mock_ddb::{item, s, BatchGetItemRequest, KeySchema, KeyType, KeysAndAttributes, MockDdb};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// mock.seed("users", vec![item([("pk", s("u#1"))]), item([("pk", s("u#2"))])]).unwrap();
    /// mock.throttle_next_batch_get(1);
    ///
    /// let batch = BatchGetItemRequest::new().add(
    ///     "users",
    ///     KeysAndAttributes::new(vec![item([("pk", s("u#1"))]), item([("pk", s("u#2"))])]),
    /// ).unwrap();
    /// let first = mock.batch_get_item(batch).await.unwrap();
    /// assert_eq!(first.responses["users"].len(), 1);
    /// assert_eq!(first.unprocessed_keys["users"].keys.len(), 1);
    ///
    /// let retry = BatchGetItemRequest::new().add("users", first.unprocessed_keys["users"].clone()).unwrap();
    /// let second = mock.batch_get_item(retry).await.unwrap();
    /// assert_eq!(second.responses["users"].len(), 1);
    /// assert!(second.unprocessed_keys.is_empty());
    /// # }
    /// ```
    ///
    /// The next batch read answers every key it does not report: the request is
    /// validated as a whole first, then the first `unprocessed` keys in request
    /// order (by table name, then position in that table's key list) are handed
    /// back in [`BatchGetItemOutput::unprocessed_keys`] with the projection and
    /// alias map they came with, and cost no read capacity. The knob is
    /// **one-shot** — the call that spends it clears it, so the caller's retry
    /// runs unthrottled. A `count` above that batch's size leaves the whole
    /// batch unprocessed, and `0` leaves nothing unprocessed; arming again
    /// before the next call replaces the pending count. A batch the mock rejects
    /// outright — empty, over 100 keys, unknown table, a malformed key or
    /// projection — leaves the count armed, so a test's setup is not eaten by a
    /// request that never ran.
    ///
    /// Without an armed knob the default holds: `unprocessed_keys` is empty.
    pub fn throttle_next_batch_get(&self, unprocessed: usize) {
        self.batch_throttle.lock().keys = Some(unprocessed);
    }

    /// Spend the armed write throttle, clamped to the batch that is running.
    fn take_throttled_writes(&self, batch: usize) -> usize {
        let mut throttle = self.batch_throttle.lock();
        throttle.writes.take().map_or(0, |count| count.min(batch))
    }

    /// Spend the armed key throttle, clamped to the batch that is running.
    fn take_throttled_keys(&self, batch: usize) -> usize {
        let mut throttle = self.batch_throttle.lock();
        throttle.keys.take().map_or(0, |count| count.min(batch))
    }

    ///
    /// ```
    /// use mock_ddb::{item, s, BatchWriteItemRequest, KeySchema, KeyType, MockDdb, WriteRequest};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// let request = BatchWriteItemRequest::new()
    ///     .add("users", WriteRequest::Put { item: item([("pk", s("u#1"))]) })
    ///     .add("users", WriteRequest::Delete { key: item([("pk", s("u#2"))]) });
    /// assert!(mock.batch_write_item(request).await.unwrap().unprocessed_items.is_empty());
    /// assert_eq!(mock.count("users"), 1);
    /// # }
    /// ```
    /// Write up to 25 items in one non-atomic batch.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] when the batch is empty, has more than 25
    /// writes, repeats a key, or contains an item that does not match its
    /// table, and [`Error::ResourceNotFound`] for an unknown table. Nothing is
    /// written when any write fails validation.
    ///
    /// `unprocessed_items` is empty unless the call was throttled with
    /// [`throttle_next_batch_write`](Self::throttle_next_batch_write).
    pub async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
    ) -> Result<BatchWriteItemOutput> {
        self.batch_write_item_inner(request)
    }

    fn batch_write_item_inner(
        &self,
        request: BatchWriteItemRequest,
    ) -> Result<BatchWriteItemOutput> {
        let now = self.epoch_now();
        let total: usize = request.request_items.values().map(Vec::len).sum();
        if total == 0 || total > 25 {
            return Err(Error::validation(format!(
                "1 validation error detected: Value at 'requestItems' failed to satisfy constraint: Member must have length between 1 and 25, got {total}"
            )));
        }
        let mut inner = self.inner.write();
        let ordered = validated_batch_keys(&inner, &request)?;
        // Spend the throttle only now: a batch rejected above (size, unknown
        // table, item that does not match, duplicate key) leaves the armed count
        // for the next call instead of eating a test's setup. This is the one
        // place that takes the throttle lock while the store lock is held; the
        // order is fixed (store, then throttle) and the throttle lock is never
        // held while calling out, so the pair cannot deadlock.
        let leave = self.take_throttled_writes(total);
        // The deferred writes are a prefix of `ordered`, so the caller's retry
        // covers exactly what the response left out, and nothing else.
        let mut deferred: BTreeSet<(String, ItemKey)> = ordered.into_iter().take(leave).collect();

        let mut unprocessed_items: BTreeMap<String, Vec<WriteRequest>> = BTreeMap::new();
        let mut breakdowns: BTreeMap<String, Breakdown> = BTreeMap::new();
        let mut collections: BTreeMap<String, ItemCollectionMetrics> = BTreeMap::new();
        for (table_name, writes) in request.request_items {
            let table = inner
                .tables
                .get_mut(&table_name)
                .ok_or_else(|| Error::table_not_found(&table_name))?;
            for write in writes {
                let unprocessed = match &write {
                    WriteRequest::Put { item } => {
                        deferred.remove(&(table_name.clone(), table.item_key(item)?))
                    }
                    WriteRequest::Delete { key } => {
                        deferred.remove(&(table_name.clone(), table.lookup_key(key)?))
                    }
                };
                if unprocessed {
                    // Neither applied nor charged: the service reports the work
                    // it did not do, and a retry has to pay for it.
                    unprocessed_items
                        .entry(table_name.clone())
                        .or_default()
                        .push(write);
                    continue;
                }
                let mut breakdown = Breakdown::new();
                match write {
                    WriteRequest::Put { item } => {
                        let key = table.item_key(&item)?;
                        let previous = table.items.get(&key).cloned();
                        let previous_live = previous
                            .clone()
                            .filter(|existing| !table.is_expired(existing, now));
                        charge_write(table, &mut breakdown, [previous_live.as_ref(), Some(&item)]);
                        table.store(key, item.clone(), previous.as_ref());
                        table.on_put(previous_live.as_ref(), &item);
                        note_collection(
                            table,
                            &item,
                            request.return_item_collection_metrics,
                            now,
                            &table_name,
                            &mut collections,
                        );
                    }
                    WriteRequest::Delete { key } => {
                        let key = table.lookup_key(&key)?;
                        let previous = table.items.get(&key).cloned();
                        let previous_live = previous
                            .clone()
                            .filter(|existing| !table.is_expired(existing, now));
                        charge_write(table, &mut breakdown, [previous_live.as_ref(), None]);
                        if let Some(previous) = table.discard(&key) {
                            if !table.is_expired(&previous, now) {
                                table.on_delete(&previous);
                            }
                            note_collection(
                                table,
                                &previous,
                                request.return_item_collection_metrics,
                                now,
                                &table_name,
                                &mut collections,
                            );
                        }
                    }
                }
                breakdowns
                    .entry(table_name.clone())
                    .or_default()
                    .merge(&breakdown);
            }
        }
        let consumed_capacity = breakdowns
            .iter()
            .filter_map(|(table_name, breakdown)| {
                breakdown.consume(table_name, request.return_consumed_capacity)
            })
            .collect();
        Ok(BatchWriteItemOutput {
            unprocessed_items,
            consumed_capacity,
            item_collection_metrics: collections,
        })
    }

    ///
    /// ```
    /// use mock_ddb::{item, s, BatchGetItemRequest, KeySchema, KeyType, KeysAndAttributes, MockDdb};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// mock.seed("users", vec![item([("pk", s("u#1"))])]).unwrap();
    /// let response = mock
    ///     .batch_get_item(
    ///         BatchGetItemRequest::new()
    ///             .add("users", KeysAndAttributes::new(vec![item([("pk", s("u#1"))])]))
    ///             .unwrap(),
    ///     )
    ///     .await
    ///     .unwrap();
    /// assert_eq!(response.responses["users"].len(), 1);
    /// # }
    /// ```
    /// Read up to 100 keys in one batch. Missing keys are omitted.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] when the batch is empty, asks for more than
    /// 100 keys, or carries a malformed key or projection, and
    /// [`Error::ResourceNotFound`] for an unknown table.
    ///
    /// `unprocessed_keys` is empty unless the call was throttled with
    /// [`throttle_next_batch_get`](Self::throttle_next_batch_get).
    pub async fn batch_get_item(&self, request: BatchGetItemRequest) -> Result<BatchGetItemOutput> {
        self.batch_get_item_inner(&request)
    }

    fn batch_get_item_inner(&self, request: &BatchGetItemRequest) -> Result<BatchGetItemOutput> {
        let now = self.epoch_now();
        let total: usize = request
            .request_items
            .values()
            .map(|attributes| attributes.keys.len())
            .sum();
        if total == 0 || total > 100 {
            return Err(Error::validation(format!(
                "1 validation error detected: Value at 'requestItems' failed to satisfy constraint: Member must have length between 1 and 100, got {total}"
            )));
        }
        let inner = self.inner.read();

        // Validate the whole request — tables, keys, projections — before a
        // single key is answered: a malformed projection is refused even when
        // the table holds none of the keys it names, and a rejected batch never
        // eats a test's armed partial response.
        let mut steps: Vec<BatchReadStep<'_>> = Vec::with_capacity(request.request_items.len());
        for (table_name, attributes) in &request.request_items {
            let table = inner
                .tables
                .get(table_name)
                .ok_or_else(|| Error::table_not_found(table_name))?;
            let names = attributes
                .expression_attribute_names
                .clone()
                .unwrap_or_default();
            let projection = match &attributes.projection_expression {
                None => None,
                Some(expression) => {
                    let parsed = ProjectionExpression::parse(expression)?;
                    Some(ResolvedProjection::new(&parsed, &names)?)
                }
            };
            let mut keys = Vec::with_capacity(attributes.keys.len());
            for key in &attributes.keys {
                keys.push(table.lookup_key(key)?);
            }
            steps.push(BatchReadStep {
                table,
                table_name,
                attributes,
                keys,
                projection,
            });
        }

        // Only now spend what the request was armed with, so that a batch
        // rejected above leaves the pending count for the next call. The count of
        // deferred keys runs across tables, in request order.
        let mut deferred = self.take_throttled_keys(total);

        let mut responses: BTreeMap<String, Vec<Item>> = BTreeMap::new();
        let mut unprocessed_keys: BTreeMap<String, KeysAndAttributes> = BTreeMap::new();
        let mut breakdowns: BTreeMap<String, Breakdown> = BTreeMap::new();
        for step in &steps {
            let mut found = Vec::new();
            let mut leftover: Vec<Item> = Vec::new();
            let mut breakdown = Breakdown::new();
            for (position, key) in step.keys.iter().enumerate() {
                if deferred > 0 {
                    deferred -= 1;
                    // Handed back as the request carried it, and not charged: the
                    // service reports the work it did not do, and a retry pays.
                    leftover.push(step.attributes.keys[position].clone());
                    continue;
                }
                let stored = step
                    .table
                    .items
                    .get(key)
                    .filter(|item| !step.table.is_expired(item, now));
                // A key the table does not hold still costs a read unit.
                breakdown.charge_table(Units::read(read_units(stored.map_or(0, item_size))));
                let Some(item) = stored else {
                    continue;
                };
                let item = match &step.projection {
                    None => item.clone(),
                    Some(projection) => projection.project(item),
                };
                found.push(item);
            }
            if !leftover.is_empty() {
                unprocessed_keys.insert(
                    step.table_name.to_string(),
                    KeysAndAttributes {
                        keys: leftover,
                        ..step.attributes.clone()
                    },
                );
            }
            if !found.is_empty() {
                responses.insert(step.table_name.to_string(), found);
            }
            breakdowns.insert(step.table_name.to_string(), breakdown);
        }
        let consumed_capacity = breakdowns
            .iter()
            .filter_map(|(table_name, breakdown)| {
                breakdown.consume(table_name, request.return_consumed_capacity)
            })
            .collect();
        Ok(BatchGetItemOutput {
            responses,
            unprocessed_keys,
            consumed_capacity,
        })
    }

    /// Run one PartiQL statement — `SELECT`, `INSERT`, `UPDATE` or `DELETE`.
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use mock_ddb::{s, ExecuteStatementRequest, KeySchema, KeyType, MockDdb};
    ///
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// mock.execute_statement(
    ///     ExecuteStatementRequest::new("INSERT INTO \"users\" VALUE {'pk': ?, 'name': 'Ada'}")
    ///         .with_parameters(vec![s("u#1")]),
    /// )
    /// .await
    /// .unwrap();
    ///
    /// let rows = mock
    ///     .execute_statement(
    ///         ExecuteStatementRequest::new("SELECT * FROM \"users\" WHERE pk = ?")
    ///             .with_parameters(vec![s("u#1")]),
    ///     )
    ///     .await
    ///     .unwrap();
    /// assert_eq!(rows.items.expect("a read returns rows").len(), 1);
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] for a statement that is empty, longer than
    /// [`MAX_STATEMENT_LENGTH`](crate::MAX_STATEMENT_LENGTH) or malformed, for a
    /// parameter list that does not match the statement's `?` placeholders, and
    /// for an `UPDATE`/`DELETE` whose `WHERE` clause does not name every key
    /// attribute; [`Error::ResourceNotFound`] for an unknown table;
    /// [`Error::DuplicateItem`] for an `INSERT` whose key already exists; and
    /// [`Error::ConditionalCheckFailed`] when the `WHERE` clause of a write does
    /// not hold.
    pub async fn execute_statement(
        &self,
        request: ExecuteStatementRequest,
    ) -> Result<ExecuteStatementOutput> {
        crate::partiql::execute_statement(self, &request)
    }

    /// Run a batch of PartiQL statements, up to
    /// [`MAX_BATCH_STATEMENTS`](crate::MAX_BATCH_STATEMENTS).
    ///
    /// The batch must be all reads or all writes, like the service's, and a
    /// read must pin the whole primary key so it matches at most one item. One
    /// member's failure does not roll the others back: it is reported in that
    /// member's response, exactly as the service does.
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use mock_ddb::{
    ///     s, BatchExecuteStatementRequest, BatchStatementRequest, KeySchema, KeyType, MockDdb,
    /// };
    ///
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// let output = mock
    ///     .batch_execute_statement(
    ///         BatchExecuteStatementRequest::new()
    ///             .with_statement(
    ///                 BatchStatementRequest::new("INSERT INTO \"users\" VALUE {'pk': ?}")
    ///                     .with_parameters(vec![s("u#1")]),
    ///             )
    ///             .with_statement(
    ///                 BatchStatementRequest::new("INSERT INTO \"users\" VALUE {'pk': ?}")
    ///                     .with_parameters(vec![s("u#1")]),
    ///             ),
    ///     )
    ///     .await
    ///     .unwrap();
    /// assert!(output.responses[0].error.is_none());
    /// assert_eq!(
    ///     output.responses[1].error.as_ref().map(|error| error.code.as_str()),
    ///     Some("DuplicateItem")
    /// );
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] for an empty batch, for more than
    /// [`MAX_BATCH_STATEMENTS`](crate::MAX_BATCH_STATEMENTS) members, for a
    /// batch that mixes reads and writes, and for a member that does not parse;
    /// [`Error::ResourceNotFound`] when a member names a table that does not
    /// exist.
    pub async fn batch_execute_statement(
        &self,
        request: BatchExecuteStatementRequest,
    ) -> Result<BatchExecuteStatementOutput> {
        crate::partiql::batch_execute_statement(self, &request)
    }

    /// The key schema of a table or one of its indexes.
    pub(crate) fn key_schema_of(
        &self,
        table_name: &str,
        index_name: Option<&str>,
    ) -> Result<KeySchema> {
        let inner = self.inner.read();
        let table = inner
            .tables
            .get(table_name)
            .ok_or_else(|| Error::table_not_found(table_name))?;
        match index_name {
            None => Ok(table.key_schema().clone()),
            Some(name) => Ok(table.definition.index(name)?.key_schema.clone()),
        }
    }

    /// Park the key a PartiQL read stopped at and return the token naming it.
    pub(crate) fn store_next_token(&self, key: &Item) -> String {
        let mut inner = self.inner.write();
        inner.token_sequence += 1;
        let token = format!("v1|partiql|{}", inner.token_sequence);
        inner.next_tokens.push_back((token.clone(), key.clone()));
        while inner.next_tokens.len() > MAX_NEXT_TOKENS {
            inner.next_tokens.pop_front();
        }
        token
    }

    /// Resolve a `NextToken` this mock issued back to the key it parked.
    pub(crate) fn take_next_token(&self, token: &str) -> Result<Item> {
        let inner = self.inner.read();
        inner
            .next_tokens
            .iter()
            .find(|(issued, _)| issued == token)
            .map(|(_, key)| key.clone())
            .ok_or_else(|| Error::validation(format!("Invalid NextToken: {token}")))
    }

    ///
    /// ```
    /// use mock_ddb::{item, s, KeySchema, KeyType, MockDdb, TransactWriteItem, TransactWriteItemsRequest};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// mock.transact_write_items(TransactWriteItemsRequest::new(vec![
    ///     TransactWriteItem::put("users", item([("pk", s("u#1"))])),
    ///     TransactWriteItem::condition_check("users", item([("pk", s("u#2"))]), "attribute_not_exists(pk)"),
    /// ]))
    /// .await
    /// .unwrap();
    /// assert_eq!(mock.count("users"), 1);
    /// # }
    /// ```
    /// Run an atomic transaction of up to 100 actions.
    ///
    /// A [`client_request_token`](TransactWriteItemsRequest::client_request_token)
    /// makes the call idempotent for ten minutes, measured on the injected
    /// clock: a second transaction that carries the same token and the same
    /// actions writes nothing and returns an empty output, while the same token
    /// with different actions is refused instead of silently discarding the
    /// second write.
    ///
    /// ```rust
    /// use mock_ddb::{
    ///     item, s, KeySchema, KeyType, MockDdb, TransactWriteItem, TransactWriteItemsRequest,
    /// };
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String))?;
    /// let write = |value: &'static str| {
    ///     TransactWriteItemsRequest::new(vec![TransactWriteItem::put(
    ///         "users",
    ///         item([("pk", s("u#1")), ("v", s(value))]),
    ///     )])
    ///     .with_client_request_token("token-1")
    /// };
    ///
    /// // Inside: the same token and the same actions are a replay, not a write.
    /// mock.transact_write_items(write("a")).await?;
    /// mock.transact_write_items(write("a")).await?;
    ///
    /// // Outside: the same token with different actions is refused.
    /// let error = mock.transact_write_items(write("b")).await.unwrap_err();
    /// assert_eq!(error.code(), "IdempotentParameterMismatchException");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A token is between 1 and 36 **characters** long, counted the way the
    /// service counts them: 36 four-byte characters are 144 bytes and still
    /// accepted.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] for an empty transaction, more than 100
    /// actions, two actions on the same item, a token that is empty or longer
    /// than 36 characters, or a malformed expression; [`Error::ResourceNotFound`]
    /// for an unknown table;
    /// [`Error::IdempotentParameterMismatch`] when the token was already spent
    /// on a different transaction; and [`Error::TransactionCanceled`] when any
    /// condition fails. A cancelled transaction writes nothing.
    pub async fn transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
    ) -> Result<TransactWriteItemsOutput> {
        self.transact_write_items_inner(&request)
    }

    fn transact_write_items_inner(
        &self,
        request: &TransactWriteItemsRequest,
    ) -> Result<TransactWriteItemsOutput> {
        let now = self.epoch_now();
        let mut inner = self.inner.write();
        if request.transact_items.is_empty() || request.transact_items.len() > 100 {
            return Err(Error::validation(format!(
                "1 validation error detected: Value at 'transactItems' failed to satisfy constraint: Member must have length between 1 and 100, got {}",
                request.transact_items.len()
            )));
        }
        if let Some(token) = &request.client_request_token {
            // The constraint counts characters, not UTF-8 bytes: a token of 36
            // four-byte characters is 144 bytes and still valid upstream.
            if token.is_empty() || token.chars().count() > 36 {
                return Err(Error::validation(
                    "1 validation error detected: Value at 'clientRequestToken' failed to satisfy constraint: Member must have length between 1 and 36",
                ));
            }
            // The service forgets a token ten minutes after the transaction
            // that spent it, so drop the spent ones before consulting the map:
            // the answer for an expired token is "a new request", and the map
            // stays bounded by the traffic of the last ten minutes.
            inner
                .applied_tokens
                .retain(|_, applied| now.saturating_sub(applied.applied_at) < TOKEN_EXPIRY_SECONDS);
            if let Some(applied) = inner.applied_tokens.get(token) {
                if applied.payload != transaction_payload(request) {
                    return Err(Error::idempotent_parameter_mismatch());
                }
                return Ok(TransactWriteItemsOutput::default());
            }
        }

        let mut prepared: Vec<PreparedAction> = Vec::with_capacity(request.transact_items.len());
        let mut seen: BTreeSet<(String, ItemKey)> = BTreeSet::new();
        for action in &request.transact_items {
            let action = prepare_action(&inner, action, now)?;
            if !seen.insert((action.table_name.clone(), action.key.clone())) {
                return Err(Error::validation(
                    "Transaction request cannot include multiple operations on one item",
                ));
            }
            prepared.push(action);
        }

        let mut failing: Option<usize> = None;
        for (position, action) in prepared.iter().enumerate() {
            if let Some(condition) = &action.condition {
                let holds =
                    condition.evaluate(action.previous.as_ref(), &action.names, &action.values)?;
                if !holds {
                    failing = Some(position);
                    break;
                }
            }
        }

        if let Some(failing) = failing {
            let reasons = prepared
                .iter()
                .enumerate()
                .map(|(position, action)| {
                    if position == failing {
                        let message = format!(
                            "The conditional request failed for table {}",
                            action.table_name
                        );
                        CancellationReason::new(
                            CancellationReasonCode::ConditionalCheckFailed,
                            message,
                        )
                        .with_item(if action.wants_old_image {
                            action.previous.clone()
                        } else {
                            None
                        })
                    } else {
                        CancellationReason::new(CancellationReasonCode::None, String::new())
                    }
                })
                .collect();
            return Err(Error::TransactionCanceled {
                message:
                    "Transaction cancelled, please refer cancellation reasons for specific reasons"
                        .to_string(),
                reasons,
            });
        }

        let output = apply_transaction(
            &mut inner,
            prepared,
            request.return_consumed_capacity,
            request.return_item_collection_metrics,
            now,
        )?;

        if let Some(token) = &request.client_request_token {
            inner.applied_tokens.insert(
                token.clone(),
                AppliedToken {
                    payload: transaction_payload(request),
                    applied_at: now,
                },
            );
        }
        Ok(output)
    }

    ///
    /// ```
    /// use mock_ddb::{item, s, KeySchema, KeyType, MockDdb, TransactGet, TransactGetItemsRequest};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// mock.seed("users", vec![item([("pk", s("u#1"))])]).unwrap();
    /// let response = mock
    ///     .transact_get_items(TransactGetItemsRequest::new(vec![
    ///         TransactGet::new("users", item([("pk", s("u#1"))])),
    ///         TransactGet::new("users", item([("pk", s("u#2"))])),
    ///     ]))
    ///     .await
    ///     .unwrap();
    /// assert!(response.responses[0].is_some());
    /// assert!(response.responses[1].is_none());
    /// # }
    /// ```
    /// Read up to 100 items atomically. The response keeps request order and
    /// carries `None` for a missing key.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] for an empty transaction, more than 100
    /// reads, or a malformed key or projection, and [`Error::ResourceNotFound`]
    /// for an unknown table.
    pub async fn transact_get_items(
        &self,
        request: TransactGetItemsRequest,
    ) -> Result<TransactGetItemsOutput> {
        self.transact_get_items_inner(&request)
    }

    fn transact_get_items_inner(
        &self,
        request: &TransactGetItemsRequest,
    ) -> Result<TransactGetItemsOutput> {
        let now = self.epoch_now();
        let inner = self.inner.read();
        if request.transact_items.is_empty() || request.transact_items.len() > 100 {
            return Err(Error::validation(format!(
                "1 validation error detected: Value at 'transactItems' failed to satisfy constraint: Member must have length between 1 and 100, got {}",
                request.transact_items.len()
            )));
        }
        let mut responses = Vec::with_capacity(request.transact_items.len());
        let mut breakdowns: BTreeMap<String, Breakdown> = BTreeMap::new();
        for get in &request.transact_items {
            let table = inner
                .tables
                .get(&get.table_name)
                .ok_or_else(|| Error::table_not_found(&get.table_name))?;
            let key = table.lookup_key(&get.key)?;
            let stored = table
                .items
                .get(&key)
                .filter(|item| !table.is_expired(item, now));
            breakdowns
                .entry(get.table_name.clone())
                .or_default()
                .charge_table(Units::read(read_units(stored.map_or(0, item_size))));
            let item = match stored {
                None => None,
                Some(item) => match &get.projection_expression {
                    None => Some(item.clone()),
                    Some(expression) => {
                        let names = get.expression_attribute_names.clone().unwrap_or_default();
                        Some(ProjectionExpression::parse(expression)?.project(item, &names)?)
                    }
                },
            };
            responses.push(item);
        }
        let consumed_capacity = breakdowns
            .iter()
            .filter_map(|(table_name, breakdown)| {
                breakdown.consume(table_name, request.return_consumed_capacity)
            })
            .collect();
        Ok(TransactGetItemsOutput {
            responses,
            consumed_capacity,
        })
    }

    // -- TTL ----------------------------------------------------------------

    ///
    /// ```
    /// use mock_ddb::{KeySchema, KeyType, MockDdb, TimeToLiveStatus, UpdateTimeToLiveRequest};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// let description = mock
    ///     .update_time_to_live(UpdateTimeToLiveRequest::new("users", "expires_at", true))
    ///     .await
    ///     .unwrap();
    /// assert_eq!(description.status, TimeToLiveStatus::Enabled);
    /// # }
    /// ```
    /// Enable or disable TTL on a table.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ResourceNotFound`] for an unknown table,
    /// [`Error::Validation`] for an empty attribute name, and
    /// [`Error::Validation`] when TTL is already in the requested state, which
    /// is what the service reports.
    pub async fn update_time_to_live(
        &self,
        request: UpdateTimeToLiveRequest,
    ) -> Result<TimeToLiveDescription> {
        let mut inner = self.inner.write();
        let table = inner
            .tables
            .get_mut(&request.table_name)
            .ok_or_else(|| Error::table_not_found(&request.table_name))?;
        if request.attribute_name.is_empty() {
            return Err(Error::validation(
                "1 validation error detected: Value at 'timeToLiveSpecification.attributeName' failed to satisfy constraint: Member must have length greater than or equal to 1",
            ));
        }
        let already_enabled = table.ttl.as_ref().is_some_and(|ttl| ttl.enabled);
        if request.enabled == already_enabled {
            return Err(Error::validation(format!(
                "TimeToLive is already {}",
                if request.enabled {
                    "enabled"
                } else {
                    "disabled"
                }
            )));
        }
        table.ttl = Some(Ttl {
            attribute_name: request.attribute_name.clone(),
            enabled: request.enabled,
        });
        Ok(TimeToLiveDescription {
            status: if request.enabled {
                TimeToLiveStatus::Enabled
            } else {
                TimeToLiveStatus::Disabled
            },
            attribute_name: Some(request.attribute_name),
        })
    }

    ///
    /// ```
    /// use mock_ddb::{KeySchema, KeyType, MockDdb};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// let description = mock.describe_time_to_live("users").await.unwrap();
    /// assert!(description.attribute_name.is_none());
    /// # }
    /// ```
    /// Report a table's TTL configuration.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ResourceNotFound`] for an unknown table.
    pub async fn describe_time_to_live(&self, table_name: &str) -> Result<TimeToLiveDescription> {
        let inner = self.inner.read();
        let table = inner
            .tables
            .get(table_name)
            .ok_or_else(|| Error::table_not_found(table_name))?;
        Ok(match &table.ttl {
            Some(ttl) if ttl.enabled => TimeToLiveDescription {
                status: TimeToLiveStatus::Enabled,
                attribute_name: Some(ttl.attribute_name.clone()),
            },
            _ => TimeToLiveDescription {
                status: TimeToLiveStatus::Disabled,
                attribute_name: None,
            },
        })
    }

    ///
    /// ```
    /// use mock_ddb::{item, n, s, KeySchema, KeyType, MockDdb, UpdateTimeToLiveRequest};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    /// mock.update_time_to_live(UpdateTimeToLiveRequest::new("users", "expires_at", true))
    ///     .await
    ///     .unwrap();
    /// mock.seed("users", vec![item([("pk", s("u#1")), ("expires_at", n("1"))])])
    ///     .unwrap();
    /// assert_eq!(mock.purge_expired("users").unwrap(), 1);
    /// # }
    /// ```
    /// Delete every expired item in a table and report how many were removed.
    ///
    /// The real service deletes asynchronously within 48 hours of expiry; the
    /// mock hides expired items from every read immediately and only removes
    /// them when they are read or purged.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ResourceNotFound`] for an unknown table.
    pub fn purge_expired(&self, table_name: &str) -> Result<usize> {
        let now = self.epoch_now();
        let mut inner = self.inner.write();
        let table = inner
            .tables
            .get_mut(table_name)
            .ok_or_else(|| Error::table_not_found(table_name))?;
        Ok(table.purge_expired(now))
    }

    // -- streams ------------------------------------------------------------

    /// Every stream ARN, sorted by table name.
    #[cfg(feature = "streams")]
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::{CreateTableRequest, KeySchema, KeyType, MockDdb, StreamViewType};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::new();
    /// mock.create_table(
    ///     CreateTableRequest::new("users", KeySchema::hash("pk", KeyType::String))
    ///         .with_stream(StreamViewType::NewImage),
    /// )
    /// .await
    /// .unwrap();
    /// assert_eq!(mock.list_streams().len(), 1);
    /// # }
    /// ```
    pub fn list_streams(&self) -> Vec<String> {
        let inner = self.inner.read();
        inner
            .tables
            .values()
            .filter_map(|table| table.stream.as_ref().map(|stream| stream.arn.clone()))
            .collect()
    }

    /// Read a stream's records, in write order.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ResourceNotFound`] when no table has this stream ARN.
    #[cfg(feature = "streams")]
    ///
    /// ```
    /// use mock_ddb::{item, s, CreateTableRequest, KeySchema, KeyType, MockDdb, PutItemRequest, StreamViewType};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::new();
    /// let table = mock
    ///     .create_table(
    ///         CreateTableRequest::new("users", KeySchema::hash("pk", KeyType::String))
    ///             .with_stream(StreamViewType::NewImage),
    ///     )
    ///     .await
    ///     .unwrap();
    /// mock.put_item(PutItemRequest::new("users", item([("pk", s("u#1"))])))
    ///     .await
    ///     .unwrap();
    /// let arn = table.latest_stream_arn.unwrap();
    /// assert_eq!(mock.get_records(&arn).await.unwrap().len(), 1);
    /// # }
    /// ```
    pub async fn get_records(&self, stream_arn: &str) -> Result<Vec<StreamRecord>> {
        let inner = self.inner.read();
        for table in inner.tables.values() {
            if let Some(stream) = &table.stream {
                if stream.arn == stream_arn {
                    return Ok(stream.records.clone());
                }
            }
        }
        Err(Error::ResourceNotFound {
            resource: format!("stream `{stream_arn}`"),
            message: format!("Requested resource not found: Stream: {stream_arn} not found"),
        })
    }

    /// The DynamoDB Streams API over this mock: `ListStreams`,
    /// `DescribeStream`, `GetShardIterator` and `GetRecords` in the wire shapes
    /// the service (and the DynamoDB Streams Kinesis Adapter) uses.
    ///
    /// [`MockDdb::list_streams`] and [`MockDdb::get_records`] stay the short
    /// in-process readers; this handle is for code that is written against the
    /// Streams client, shard iterators and paging included.
    #[cfg(feature = "streams")]
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::{item, s, CreateTableRequest, KeySchema, KeyType, ListStreamsRequest, MockDdb, PutItemRequest, StreamViewType};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::new();
    /// mock.create_table(
    ///     CreateTableRequest::new("users", KeySchema::hash("pk", KeyType::String))
    ///         .with_stream(StreamViewType::NewImage),
    /// )
    /// .await
    /// .unwrap();
    /// mock.put_item(PutItemRequest::new("users", item([("pk", s("u#1"))])))
    ///     .await
    ///     .unwrap();
    ///
    /// let streams = mock.streams();
    /// let listed = streams.list_streams(ListStreamsRequest::new()).await.unwrap();
    /// assert_eq!(listed.streams.len(), 1);
    /// # }
    /// ```
    pub fn streams(&self) -> StreamsApi<'_> {
        StreamsApi { mock: self }
    }
}

// ---------------------------------------------------------------------------
// The DynamoDB Streams API
// ---------------------------------------------------------------------------

#[cfg(feature = "streams")]
#[allow(unknown_lints, clippy::unused_async, clippy::unused_async_trait_impl)]
impl StreamsApi<'_> {
    /// `ListStreams`: every stream this mock holds, one page at a time,
    /// sorted by ARN.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] when `limit` is outside `1..=100` or when
    /// `exclusive_start_stream_arn` names a stream that does not exist.
    ///
    /// ```
    /// use mock_ddb::{CreateTableRequest, KeySchema, KeyType, ListStreamsRequest, MockDdb, StreamViewType};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::new();
    /// for name in ["orders", "users"] {
    ///     mock.create_table(
    ///         CreateTableRequest::new(name, KeySchema::hash("pk", KeyType::String))
    ///             .with_stream(StreamViewType::KeysOnly),
    ///     )
    ///     .await
    ///     .unwrap();
    /// }
    ///
    /// let page = mock.streams().list_streams(ListStreamsRequest::new()).await.unwrap();
    /// assert_eq!(page.streams.len(), 2);
    /// assert_eq!(page.streams[0].table_name, "orders");
    /// # }
    /// ```
    pub async fn list_streams(&self, request: ListStreamsRequest) -> Result<ListStreamsOutput> {
        let limit = checked_page_limit(request.limit, MAX_STREAMS_PER_LIST)?;
        let inner = self.mock.inner.read();
        let mut streams: Vec<StreamSummary> = inner
            .tables
            .values()
            .filter_map(|table| {
                table.stream.as_ref().map(|stream| StreamSummary {
                    stream_arn: stream.arn.clone(),
                    stream_label: stream.label().to_string(),
                    table_name: table.definition.table_name.clone(),
                    stream_status: "ENABLED".to_string(),
                })
            })
            .collect();
        streams.sort_by(|left, right| left.stream_arn.cmp(&right.stream_arn));

        let start = match &request.exclusive_start_stream_arn {
            Some(arn) => streams
                .iter()
                .position(|stream| &stream.stream_arn == arn)
                .map_or_else(
                    || {
                        Err(Error::validation(format!(
                            "Invalid ExclusiveStartStreamArn: stream {arn} not found"
                        )))
                    },
                    |position| Ok(position + 1),
                )?,
            None => 0,
        };
        let page: Vec<StreamSummary> = streams.into_iter().skip(start).take(limit).collect();
        let last_evaluated_stream_arn = page.last().and_then(|stream| {
            let index = start + page.len();
            (index < inner_stream_count(&inner)).then(|| stream.stream_arn.clone())
        });
        Ok(ListStreamsOutput {
            streams: page,
            last_evaluated_stream_arn,
        })
    }

    /// `DescribeStream`: the stream's label, view type, table and its single
    /// shard.
    ///
    /// # Errors
    ///
    /// [`Error::ResourceNotFound`] for an unknown stream ARN,
    /// [`Error::Validation`] for a `limit` outside `1..=100` or an
    /// `exclusive_start_shard_id` that names no shard.
    ///
    /// ```
    /// use mock_ddb::{CreateTableRequest, DescribeStreamRequest, KeySchema, KeyType, MockDdb, StreamViewType};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::new();
    /// let table = mock
    ///     .create_table(
    ///         CreateTableRequest::new("users", KeySchema::hash("pk", KeyType::String))
    ///             .with_stream(StreamViewType::NewAndOldImages),
    ///     )
    ///     .await
    ///     .unwrap();
    /// let arn = table.latest_stream_arn.unwrap();
    ///
    /// let described = mock
    ///     .streams()
    ///     .describe_stream(DescribeStreamRequest::new(&arn))
    ///     .await
    ///     .unwrap();
    /// assert_eq!(described.stream_description.shards.len(), 1);
    /// # }
    /// ```
    pub async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> Result<DescribeStreamOutput> {
        let limit = checked_page_limit(request.limit, MAX_SHARDS_PER_DESCRIBE)?;
        let inner = self.mock.inner.read();
        let (table_name, stream) = stream_for(&inner, &request.stream_arn)?;
        let start = match &request.exclusive_start_shard_id {
            Some(shard_id) if shard_id == StreamState::SHARD_ID => 1,
            Some(shard_id) => {
                return Err(Error::validation(format!(
                    "Invalid ShardId: {shard_id} is not a shard of {}",
                    request.stream_arn
                )))
            }
            None => 0,
        };
        let shards = if start == 0 && limit > 0 {
            vec![Shard {
                shard_id: StreamState::SHARD_ID.to_string(),
                parent_shard_id: None,
                sequence_number_range: crate::stream::SequenceNumberRange {
                    starting_sequence_number: starting_sequence_number(stream),
                    ending_sequence_number: None,
                },
            }]
        } else {
            Vec::new()
        };
        Ok(DescribeStreamOutput {
            stream_description: StreamDescription {
                stream_arn: stream.arn.clone(),
                stream_label: stream.label().to_string(),
                stream_status: "ENABLED".to_string(),
                stream_view_type: stream.view_type(),
                creation_request_date_time: STREAM_CREATION_EPOCH_SECONDS,
                table_name,
                shards,
                last_evaluated_shard_id: None,
            },
        })
    }

    /// `GetShardIterator`: a token that [`StreamsApi::get_records`] reads from.
    ///
    /// # Errors
    ///
    /// [`Error::ResourceNotFound`] for an unknown stream ARN,
    /// [`Error::Validation`] for an unknown shard, for a sequence number that
    /// is missing where the type needs one (or given where it must not be),
    /// and for a sequence number the stream never issued.
    ///
    /// ```
    /// use mock_ddb::{item, s, CreateTableRequest, GetShardIteratorRequest, KeySchema, KeyType, MockDdb, PutItemRequest, ShardIteratorType, StreamViewType};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::new();
    /// let table = mock
    ///     .create_table(
    ///         CreateTableRequest::new("users", KeySchema::hash("pk", KeyType::String))
    ///             .with_stream(StreamViewType::KeysOnly),
    ///     )
    ///     .await
    ///     .unwrap();
    /// mock.put_item(PutItemRequest::new("users", item([("pk", s("u#1"))])))
    ///     .await
    ///     .unwrap();
    ///
    /// let arn = table.latest_stream_arn.unwrap();
    /// let described = mock.streams().describe_stream(
    ///     mock_ddb::DescribeStreamRequest::new(&arn),
    /// ).await.unwrap();
    /// let shard_id = described.stream_description.shards[0].shard_id.clone();
    ///
    /// let iterator = mock
    ///     .streams()
    ///     .get_shard_iterator(GetShardIteratorRequest::new(&arn, shard_id, ShardIteratorType::TrimHorizon))
    ///     .await
    ///     .unwrap();
    /// let page = mock
    ///     .streams()
    ///     .get_records(mock_ddb::GetRecordsRequest::new(iterator.shard_iterator))
    ///     .await
    ///     .unwrap();
    /// assert_eq!(page.records.len(), 1);
    /// # }
    /// ```
    pub async fn get_shard_iterator(
        &self,
        request: GetShardIteratorRequest,
    ) -> Result<GetShardIteratorOutput> {
        let inner = self.mock.inner.read();
        let (_, stream) = stream_for(&inner, &request.stream_arn)?;
        if request.shard_id != StreamState::SHARD_ID {
            return Err(Error::validation(format!(
                "Invalid ShardId: {} is not a shard of {}",
                request.shard_id, request.stream_arn
            )));
        }
        let kind = request.shard_iterator_type;
        let position = match (kind.requires_sequence_number(), &request.sequence_number) {
            (true, Some(sequence)) => {
                let found = stream.position_of(sequence).ok_or_else(|| {
                    Error::validation(format!(
                        "The sequence number {sequence} does not exist in shard {}",
                        StreamState::SHARD_ID
                    ))
                })?;
                if kind == crate::stream::ShardIteratorType::AfterSequenceNumber {
                    found.saturating_add(1)
                } else {
                    found
                }
            }
            (true, None) => {
                return Err(Error::validation(
                    "1 validation error detected: Value null at 'sequenceNumber' failed to satisfy constraint: Member must not be null for AT_SEQUENCE_NUMBER and AFTER_SEQUENCE_NUMBER",
                ))
            }
            (false, Some(_)) => {
                return Err(Error::validation(
                    "1 validation error detected: 'sequenceNumber' is only valid with AT_SEQUENCE_NUMBER and AFTER_SEQUENCE_NUMBER",
                ))
            }
            (false, None) => match kind {
                crate::stream::ShardIteratorType::Latest => stream.records.len(),
                _ => 0,
            },
        };
        Ok(GetShardIteratorOutput {
            shard_iterator: encode_iterator(&stream.arn, position),
        })
    }

    /// `GetRecords`: the next page of a shard, in write order, plus the token
    /// for the call after it.
    ///
    /// # Errors
    ///
    /// [`Error::ResourceNotFound`] when the iterator's stream no longer exists,
    /// [`Error::Validation`] for an iterator this mock did not issue and for a
    /// `limit` outside `1..=1000`.
    ///
    /// ```
    /// use mock_ddb::{item, s, CreateTableRequest, DescribeStreamRequest, GetRecordsRequest, GetShardIteratorRequest, KeySchema, KeyType, MockDdb, PutItemRequest, ShardIteratorType, StreamViewType};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mock = MockDdb::new();
    /// let table = mock
    ///     .create_table(
    ///         CreateTableRequest::new("users", KeySchema::hash("pk", KeyType::String))
    ///             .with_stream(StreamViewType::NewImage),
    ///     )
    ///     .await
    ///     .unwrap();
    /// for id in ["u#1", "u#2"] {
    ///     mock.put_item(PutItemRequest::new("users", item([("pk", s(id))])))
    ///         .await
    ///         .unwrap();
    /// }
    /// let arn = table.latest_stream_arn.unwrap();
    /// let described = mock
    ///     .streams()
    ///     .describe_stream(DescribeStreamRequest::new(&arn))
    ///     .await
    ///     .unwrap();
    /// let shard_id = described.stream_description.shards[0].shard_id.clone();
    ///
    /// let iterator = mock
    ///     .streams()
    ///     .get_shard_iterator(GetShardIteratorRequest::new(
    ///         &arn,
    ///         shard_id,
    ///         ShardIteratorType::TrimHorizon,
    ///     ))
    ///     .await
    ///     .unwrap();
    /// let first = mock
    ///     .streams()
    ///     .get_records(GetRecordsRequest::new(iterator.shard_iterator).with_limit(1))
    ///     .await
    ///     .unwrap();
    /// assert_eq!(first.records.len(), 1);
    /// assert_eq!(first.records[0].event_name.as_str(), "INSERT");
    ///
    /// // The next page continues after the record just read.
    /// let second = mock
    ///     .streams()
    ///     .get_records(GetRecordsRequest::new(first.next_shard_iterator.unwrap()))
    ///     .await
    ///     .unwrap();
    /// assert_eq!(second.records.len(), 1);
    /// # }
    /// ```
    pub async fn get_records(&self, request: GetRecordsRequest) -> Result<GetRecordsOutput> {
        let limit = checked_page_limit(request.limit, MAX_RECORDS_PER_GET)?;
        let (stream_arn, position) = decode_iterator(&request.shard_iterator)?;
        let inner = self.mock.inner.read();
        let (_, stream) = stream_for(&inner, &stream_arn)?;
        let start = position.min(stream.records.len());
        let records: Vec<StreamsRecord> = stream.records[start..]
            .iter()
            .take(limit)
            .map(|record| wire_record(record, stream.view_type()))
            .collect();
        let next = encode_iterator(&stream_arn, start + records.len());
        Ok(GetRecordsOutput {
            records,
            next_shard_iterator: Some(next),
        })
    }
}

/// The only shard id a stream exposes.
#[cfg(feature = "streams")]
const fn shard_id() -> &'static str {
    StreamState::SHARD_ID
}

/// Resolve a stream ARN to its table name and stream state.
#[cfg(feature = "streams")]
fn stream_for<'inner>(
    inner: &'inner Inner,
    stream_arn: &str,
) -> Result<(String, &'inner StreamState)> {
    inner
        .tables
        .values()
        .find_map(|table| {
            table
                .stream
                .as_ref()
                .filter(|stream| stream.arn == stream_arn)
                .map(|stream| (table.definition.table_name.clone(), stream))
        })
        .ok_or_else(|| Error::ResourceNotFound {
            resource: format!("stream `{stream_arn}`"),
            message: format!("Requested resource not found: Stream: {stream_arn} not found"),
        })
}

/// The number of streams the mock holds.
#[cfg(feature = "streams")]
fn inner_stream_count(inner: &Inner) -> usize {
    inner
        .tables
        .values()
        .filter(|table| table.stream.is_some())
        .count()
}

/// The first sequence number a shard can return.
#[cfg(feature = "streams")]
fn starting_sequence_number(stream: &StreamState) -> String {
    stream.records.first().map_or_else(
        || format!("{:021}", stream.next_sequence),
        |record| record.sequence_number.clone(),
    )
}

/// Validate a page limit against the service's range for the operation.
#[cfg(feature = "streams")]
fn checked_page_limit(limit: Option<u32>, maximum: u32) -> Result<usize> {
    match limit {
        None => Ok(usize::try_from(maximum).unwrap_or(usize::MAX)),
        Some(limit) if limit == 0 => Err(Error::validation(format!(
            "1 validation error detected: Value '{limit}' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"
        ))),
        Some(limit) if limit > maximum => Err(Error::validation(format!(
            "1 validation error detected: Value '{limit}' at 'limit' failed to satisfy constraint: Member must have value less than or equal to {maximum}"
        ))),
        Some(limit) => Ok(usize::try_from(limit).unwrap_or(1)),
    }
}

/// Encode an opaque shard iterator: version, stream, shard and log position.
#[cfg(feature = "streams")]
fn encode_iterator(stream_arn: &str, position: usize) -> String {
    format!("v1|{stream_arn}|{}|{position}", shard_id())
}

/// Decode a shard iterator this mock issued.
#[cfg(feature = "streams")]
fn decode_iterator(token: &str) -> Result<(String, usize)> {
    let invalid = || {
        let shown: String = token.chars().take(80).collect();
        Error::validation(format!("Invalid ShardIterator: {shown}"))
    };
    let mut parts = token.split('|');
    let (Some("v1"), Some(stream_arn), Some(shard), Some(position), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return Err(invalid());
    };
    if shard != shard_id() {
        return Err(invalid());
    }
    position
        .parse::<usize>()
        .map(|position| (stream_arn.to_string(), position))
        .map_err(|_| invalid())
}

// ---------------------------------------------------------------------------
// Read path helpers
// ---------------------------------------------------------------------------

/// One page of a query or scan.
///
/// The filter and the projection are the *parsed* forms: a page parses neither,
/// so the expression text is read once per request and the pre-parsed tree is
/// evaluated (or applied) per item.
struct Page<'a> {
    limit: Option<u32>,
    filter: Option<&'a ConditionExpression>,
    key_condition: Option<&'a ConditionExpression>,
    projection: Option<&'a ResolvedProjection>,
    select: Select,
    names: &'a NameMap,
    values: &'a ValueMap,
    descending: bool,
}

/// One page of a query or scan, plus what reading it charged.
struct PageResult {
    items: Vec<Item>,
    count: usize,
    scanned_count: usize,
    last_evaluated_key: Option<Item>,
    /// Read units the page consumed: one per started 4 KiB of every item it
    /// examined, filtered ones included, and one in total when it examined
    /// nothing.
    read_units: f64,
}

/// The position of an item in a query result.
#[derive(Clone, Debug, PartialEq, Eq)]
enum CursorKey {
    /// A table query: the primary key.
    Primary(ItemKey),
    /// An index query: the index key, then the position within that index key.
    Indexed(IndexKey, GroupPosition),
}

/// Where a cursor sits inside one index key.
///
/// A cursor built from a key map that lacks the table key cannot name a single
/// item, so it points past the whole index-key group (`End`).
#[derive(Clone, Debug, PartialEq, Eq)]
enum GroupPosition {
    At(ItemKey),
    End,
}

impl Ord for GroupPosition {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::At(left), Self::At(right)) => left.cmp(right),
            (Self::At(_), Self::End) => std::cmp::Ordering::Less,
            (Self::End, Self::At(_)) => std::cmp::Ordering::Greater,
            (Self::End, Self::End) => std::cmp::Ordering::Equal,
        }
    }
}

impl PartialOrd for GroupPosition {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CursorKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Primary(left), Self::Primary(right)) => left.cmp(right),
            (Self::Primary(_), Self::Indexed(..)) => std::cmp::Ordering::Less,
            (Self::Indexed(..), Self::Primary(_)) => std::cmp::Ordering::Greater,
            (Self::Indexed(left_key, left_position), Self::Indexed(right_key, right_position)) => {
                left_key
                    .cmp(right_key)
                    .then_with(|| left_position.cmp(right_position))
            }
        }
    }
}

impl PartialOrd for CursorKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn validate_limit(limit: Option<u32>) -> Result<()> {
    if limit == Some(0) {
        return Err(Error::validation(
            "1 validation error detected: Value at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1",
        ));
    }
    Ok(())
}

/// Reject a key condition that the service would reject.
fn validate_key_condition(
    condition: &Condition,
    key_schema: &KeySchema,
    names: &NameMap,
) -> Result<()> {
    let empty_values = ValueMap::new();
    let context = crate::expr::Ctx {
        names,
        values: &empty_values,
    };
    for path in condition.paths() {
        let resolved = context.resolve(path)?;
        let Some(Segment::Name(name)) = resolved.first() else {
            return Err(Error::validation(
                "Query key condition not supported: a key condition must reference a key attribute",
            ));
        };
        if !key_schema.contains(name) {
            return Err(Error::validation(format!(
                "Query key condition not supported: {name} is not a key attribute of the queried table or index"
            )));
        }
        if resolved.len() > 1 {
            return Err(Error::validation(
                "Query key condition not supported: a key condition cannot reference a nested path",
            ));
        }
    }

    let partition = key_schema.partition_key.attribute_name.as_str();
    let pinned = condition.conjuncts().iter().any(|conjunct| {
        let Condition::Compare(Operand::Path(path), CmpOp::Eq, _) = conjunct else {
            return false;
        };
        context
            .resolve(path)
            .ok()
            .and_then(|resolved| resolved.first().cloned())
            .is_some_and(|segment| matches!(segment, Segment::Name(name) if name == partition))
    });
    if !pinned {
        return Err(Error::validation(
            "Query key condition not supported: the partition key must be constrained with =",
        ));
    }
    Ok(())
}

/// Build the ordered candidate list for a query or scan.
fn candidates<'a>(table: &'a Table, index_name: Option<&str>) -> Vec<(CursorKey, &'a Item)> {
    match index_name {
        None => table
            .items
            .iter()
            .map(|(key, item)| (CursorKey::Primary(key.clone()), item))
            .collect(),
        Some(name) => {
            let mut entries = Vec::new();
            if let Some(index) = table.indexes.get(name) {
                for (index_key, keys) in index {
                    for item_key in keys {
                        if let Some(item) = table.items.get(item_key) {
                            entries.push((
                                CursorKey::Indexed(
                                    index_key.clone(),
                                    GroupPosition::At(item_key.clone()),
                                ),
                                item,
                            ));
                        }
                    }
                }
            }
            entries
        }
    }
}

/// Turn an exclusive-start-key map into a cursor.
fn start_cursor(table: &Table, index_name: Option<&str>, key: &Item) -> Result<CursorKey> {
    match index_name {
        None => Ok(CursorKey::Primary(table.lookup_key(key)?)),
        Some(name) => {
            let definition = table.definition.index(name)?;
            let index_key = index_key(&definition.key_schema, key).ok_or_else(|| {
                Error::validation(
                    "The provided starting key is invalid: it does not contain the index key attributes",
                )
            })?;
            let position = table
                .cursor_key(key)
                .map_or(GroupPosition::End, GroupPosition::At);
            Ok(CursorKey::Indexed(index_key, position))
        }
    }
}

/// The key map that resumes a query or scan after `item`.
fn cursor_for(table: &Table, index_name: Option<&str>, item: &Item) -> Item {
    let mut cursor = Item::new();
    for element in table.key_schema().elements() {
        if let Some(value) = item.get(&element.attribute_name) {
            cursor.insert(element.attribute_name.clone(), value.clone());
        }
    }
    if let Some(name) = index_name {
        if let Ok(definition) = table.definition.index(name) {
            for element in definition.key_schema.elements() {
                if let Some(value) = item.get(&element.attribute_name) {
                    cursor.insert(element.attribute_name.clone(), value.clone());
                }
            }
        }
    }
    cursor
}

/// Parse the filter and projection of a read, and validate both against the
/// request's maps, before any item is reached.
///
/// The service validates the expressions and every `#name`/`:value` they use
/// before it reads anything, so a page that ends up empty — an empty table, or
/// one where the key condition, TTL or a segment leaves nothing — still rejects
/// a malformed filter or an undefined placeholder. Parsing once per request
/// also means a page over n items lexes and parses the same text once, not n
/// times.
fn read_expressions(
    filter: Option<&str>,
    projection: Option<&str>,
    names: &NameMap,
    values: &ValueMap,
) -> Result<(Option<ConditionExpression>, Option<ResolvedProjection>)> {
    let filter = match filter {
        None => None,
        Some(expression) => {
            let condition = ConditionExpression::parse(expression)?;
            condition.validate_maps(names, values)?;
            Some(condition)
        }
    };
    let projection = match projection {
        None => None,
        Some(expression) => Some(ResolvedProjection::new(
            &ProjectionExpression::parse(expression)?,
            names,
        )?),
    };
    Ok((filter, projection))
}

/// Apply an index's projection, then an explicit projection expression.
fn projected_item(
    table: &Table,
    index_name: Option<&str>,
    projection: Option<&ResolvedProjection>,
    item: &Item,
) -> Item {
    let base = match index_name.and_then(|name| table.projected_attributes(name)) {
        None => item.clone(),
        Some(allowed) => item
            .iter()
            .filter(|(name, _)| allowed.contains(*name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
    };
    match projection {
        None => base,
        Some(projection) => projection.project(&base),
    }
}

/// Collect one page of a query or scan.
fn collect_page(
    table: &Table,
    index_name: Option<&str>,
    start_key: Option<&Item>,
    page: &Page<'_>,
    now: i64,
    segment: Option<(u32, u32)>,
) -> Result<PageResult> {
    if page.select == Select::SpecificAttributes && page.projection.is_none() {
        return Err(Error::validation(
            "Select type SPECIFIC_ATTRIBUTES requires a projection expression",
        ));
    }
    let mut entries = candidates(table, index_name);
    if let Some((segment, total)) = segment {
        entries.retain(|(cursor, _)| match cursor {
            CursorKey::Primary(key) | CursorKey::Indexed(_, GroupPosition::At(key)) => {
                segment_of(key, total) == segment
            }
            CursorKey::Indexed(_, GroupPosition::End) => false,
        });
    }
    if page.descending {
        entries.reverse();
    }
    let start = match start_key {
        None => None,
        Some(key) => Some(start_cursor(table, index_name, key)?),
    };
    let limit = page
        .limit
        .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

    let mut items = Vec::new();
    let mut count = 0_usize;
    let mut scanned = 0_usize;
    let mut last_evaluated_key = None;
    let mut charged = 0.0_f64;

    for (position, (cursor, item)) in entries.iter().enumerate() {
        if let Some(start) = &start {
            let passed = if page.descending {
                cursor >= start
            } else {
                cursor <= start
            };
            if passed {
                continue;
            }
        }
        if let Some(condition) = page.key_condition {
            if !condition.evaluate(Some(item), page.names, page.values)? {
                continue;
            }
        }
        if table.is_expired(item, now) {
            continue;
        }
        scanned += 1;
        charged += read_units(item_size(item));
        let matches = match page.filter {
            None => true,
            Some(condition) => condition.evaluate(Some(item), page.names, page.values)?,
        };
        if matches {
            count += 1;
            if page.select != Select::Count {
                items.push(projected_item(table, index_name, page.projection, item));
            }
        }
        if limit.is_some_and(|limit| scanned >= limit) {
            let has_more = entries[position + 1..]
                .iter()
                .any(|(_, item)| !table.is_expired(item, now));
            if has_more {
                last_evaluated_key = Some(cursor_for(table, index_name, item));
            }
            break;
        }
    }

    Ok(PageResult {
        items,
        count,
        scanned_count: scanned,
        last_evaluated_key,
        // A page that examined no item still costs the minimum one unit.
        read_units: if charged > 0.0 {
            charged
        } else {
            read_units(0)
        },
    })
}

/// The 64-bit FNV-1a offset basis, from the FNV specification.
///
/// See <http://www.isthe.com/chongo/tech/comp/fnv/index.html>.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;

/// The 64-bit FNV-1a prime, from the FNV specification.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Plain 64-bit FNV-1a over the concatenation of `chunks`, hashed as one
/// stream: no separator and no length prefix between the chunks.
///
/// Concatenating is what makes [`segment_of`] independent of whether a key has
/// a sort scalar at all — the partition bytes alone hash the way they would as
/// the prefix of a longer key — and it is pinned to the published vectors by
/// `fnv1a_matches_the_specification_vectors`.
fn fnv1a_64<'a>(chunks: impl IntoIterator<Item = &'a [u8]>) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for bytes in chunks {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
    }
    hash
}

/// FNV-1a over the key's bytes, used to assign items to parallel scan segments.
///
/// The partition scalar's bytes are hashed, then the sort scalar's bytes, as a
/// single stream (see [`fnv1a_64`]). An item therefore lands in the same
/// segment on every call and in every release of this crate: the digest is
/// fixed by the FNV specification, and `fnv1a_matches_the_specification_vectors`
/// holds it to the published test vectors.
///
/// The loop is local on purpose, and the reason is the dependency, not the
/// algorithm. `hashkit` 0.1.5 (`fnv::fnv1a_64`) and `fnv` 1.0.7 both compute
/// exactly this digest, so either would be a drop-in; but `hashkit` is a beta
/// 0.1 release that unconditionally pulls `blake3`, `sha2`, `base64` and
/// `constant_time_eq` — and with them a C build step — into the dependency
/// tree of a test double, and `fnv` is a dormant 2020 crate pulled in for ten
/// lines that a golden test already covers. A test-only mock that is dropped
/// into someone else's test build should not widen that build's tree for a
/// non-cryptographic bucket index.
///
/// The spread of the buckets is a separate question and is currently poor for
/// keys whose last attribute value ends in a decimal digit: the reduction below
/// reads the low bits of the digest, which FNV-1a does not mix well. That is
/// tracked in issue #148 and does not change the choice of digest.
fn segment_of(key: &ItemKey, total_segments: u32) -> u32 {
    let hash = fnv1a_64(
        [Some(&key.0), key.1.as_ref()]
            .into_iter()
            .flatten()
            .map(|scalar| match scalar {
                Scalar::S(text) | Scalar::N(text) => text.as_bytes(),
                Scalar::B(bytes) => bytes.as_slice(),
            }),
    );
    // The remainder is below `total_segments`, itself a `u32`, so the
    // conversion back cannot fail.
    u32::try_from(hash % u64::from(total_segments)).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Transaction helpers
// ---------------------------------------------------------------------------

/// What a prepared transaction action will do.
enum ActionKind {
    Put(Item),
    Update(Item),
    Delete,
    ConditionCheck,
}

/// A transaction action validated against the current store state.
struct PreparedAction {
    table_name: String,
    key: ItemKey,
    /// The logical previous item: `None` when the key is free or the row has
    /// expired. Conditions, old-value reporting and capacity use this one.
    previous: Option<Item>,
    /// The raw row the store holds for `key`, expired or not. Only index
    /// maintenance needs it: an expired row is logically absent but still
    /// occupies index entries that a rewrite has to remove.
    stored: Option<Item>,
    condition: Option<ConditionExpression>,
    names: NameMap,
    values: ValueMap,
    kind: ActionKind,
    wants_old_image: bool,
}

/// The table a transaction action targets.
fn action_table<'a>(inner: &'a Inner, table_name: &str) -> Result<&'a Table> {
    inner
        .tables
        .get(table_name)
        .ok_or_else(|| Error::table_not_found(table_name))
}

fn prepare_action(inner: &Inner, action: &TransactWriteItem, now: i64) -> Result<PreparedAction> {
    match action {
        TransactWriteItem::Put(put) => {
            let table = action_table(inner, &put.table_name)?;
            table.validate_item(&put.item)?;
            let key = table.item_key(&put.item)?;
            Ok(PreparedAction {
                table_name: put.table_name.clone(),
                previous: live_item(table, &key, now),
                stored: stored_item(table, &key),
                key,
                condition: parse_optional_condition(put.condition_expression.as_deref())?,
                names: put.expression_attribute_names.clone().unwrap_or_default(),
                values: put.expression_attribute_values.clone().unwrap_or_default(),
                kind: ActionKind::Put(put.item.clone()),
                wants_old_image: put.return_values_on_condition_check_failure,
            })
        }
        TransactWriteItem::Update(update) => {
            let table = action_table(inner, &update.table_name)?;
            let key = table.lookup_key(&update.key)?;
            let previous = live_item(table, &key, now);
            let stored = stored_item(table, &key);
            let names = update
                .expression_attribute_names
                .clone()
                .unwrap_or_default();
            let values = update
                .expression_attribute_values
                .clone()
                .unwrap_or_default();
            let expression = UpdateExpression::parse(&update.update_expression)?;
            let mut item = match &previous {
                Some(existing) => existing.clone(),
                None => key_only_item(table.key_schema(), &update.key),
            };
            expression.apply(&mut item, table.key_schema(), &names, &values)?;
            table.validate_item(&item)?;
            Ok(PreparedAction {
                table_name: update.table_name.clone(),
                key,
                previous,
                stored,
                condition: parse_optional_condition(update.condition_expression.as_deref())?,
                names,
                values,
                kind: ActionKind::Update(item),
                wants_old_image: update.return_values_on_condition_check_failure,
            })
        }
        TransactWriteItem::Delete(delete) => {
            let table = action_table(inner, &delete.table_name)?;
            let key = table.lookup_key(&delete.key)?;
            Ok(PreparedAction {
                table_name: delete.table_name.clone(),
                previous: live_item(table, &key, now),
                stored: stored_item(table, &key),
                key,
                condition: parse_optional_condition(delete.condition_expression.as_deref())?,
                names: delete
                    .expression_attribute_names
                    .clone()
                    .unwrap_or_default(),
                values: delete
                    .expression_attribute_values
                    .clone()
                    .unwrap_or_default(),
                kind: ActionKind::Delete,
                wants_old_image: delete.return_values_on_condition_check_failure,
            })
        }
        TransactWriteItem::ConditionCheck(check) => {
            let table = action_table(inner, &check.table_name)?;
            let key = table.lookup_key(&check.key)?;
            Ok(PreparedAction {
                table_name: check.table_name.clone(),
                previous: live_item(table, &key, now),
                stored: stored_item(table, &key),
                key,
                condition: Some(ConditionExpression::parse(&check.condition_expression)?),
                names: check.expression_attribute_names.clone().unwrap_or_default(),
                values: check
                    .expression_attribute_values
                    .clone()
                    .unwrap_or_default(),
                kind: ActionKind::ConditionCheck,
                wants_old_image: check.return_values_on_condition_check_failure,
            })
        }
    }
}

fn parse_optional_condition(expression: Option<&str>) -> Result<Option<ConditionExpression>> {
    match expression {
        None => Ok(None),
        Some(expression) => Ok(Some(ConditionExpression::parse(expression)?)),
    }
}

/// Read the raw row the store holds at a key, expired or not.
///
/// An expired row is logically absent, so conditions, old-value reporting and
/// capacity work from [`live_item`]. Its secondary-index entries are still
/// there, though, so rewriting the key must unindex this value: that is what
/// [`Table::store`] takes as its `previous` argument.
fn stored_item(table: &Table, key: &ItemKey) -> Option<Item> {
    table.items.get(key).cloned()
}

/// Read the live (non-expired) item at a key.
fn live_item(table: &Table, key: &ItemKey, now: i64) -> Option<Item> {
    stored_item(table, key).filter(|item| !table.is_expired(item, now))
}

/// Build an item from just the key attributes, which is what an update against
/// a missing item starts from.
fn key_only_item(key_schema: &KeySchema, key: &Item) -> Item {
    let mut item = Item::new();
    for element in key_schema.elements() {
        if let Some(value) = key.get(&element.attribute_name) {
            item.insert(element.attribute_name.clone(), value.clone());
        }
    }
    item
}

/// Apply the validated transaction actions and report what they charged.
///
/// The actions are already validated and their conditions hold, so this only
/// writes, maintains every index, and accumulates the capacity per table. One
/// entry per table, because a transaction can span tables.
fn apply_transaction(
    inner: &mut Inner,
    prepared: Vec<PreparedAction>,
    mode: ReturnConsumedCapacity,
    metrics_mode: ReturnItemCollectionMetrics,
    now: i64,
) -> Result<TransactWriteItemsOutput> {
    let mut breakdowns: BTreeMap<String, Breakdown> = BTreeMap::new();
    let mut collections: BTreeMap<String, ItemCollectionMetrics> = BTreeMap::new();
    for action in prepared {
        let table = inner
            .tables
            .get_mut(&action.table_name)
            .ok_or_else(|| Error::table_not_found(&action.table_name))?;
        let mut breakdown = Breakdown::new();
        match action.kind {
            ActionKind::Put(item) => {
                charge_write(
                    table,
                    &mut breakdown,
                    [action.previous.as_ref(), Some(&item)],
                );
                table.store(action.key, item.clone(), action.stored.as_ref());
                table.on_put(action.previous.as_ref(), &item);
                note_collection(
                    table,
                    &item,
                    metrics_mode,
                    now,
                    &action.table_name,
                    &mut collections,
                );
            }
            ActionKind::Update(item) => {
                charge_write(
                    table,
                    &mut breakdown,
                    [action.previous.as_ref(), Some(&item)],
                );
                table.store(action.key, item.clone(), action.stored.as_ref());
                table.on_update(action.previous.as_ref(), &item);
                note_collection(
                    table,
                    &item,
                    metrics_mode,
                    now,
                    &action.table_name,
                    &mut collections,
                );
            }
            ActionKind::Delete => {
                charge_write(table, &mut breakdown, [action.previous.as_ref(), None]);
                let _ = table.discard(&action.key);
                if let Some(previous) = &action.previous {
                    table.on_delete(previous);
                    note_collection(
                        table,
                        previous,
                        metrics_mode,
                        now,
                        &action.table_name,
                        &mut collections,
                    );
                }
            }
            ActionKind::ConditionCheck => {
                // A condition check reads the item and writes nothing.
                let bytes = action.previous.as_ref().map_or(0, item_size);
                breakdown.charge_table(Units::read(read_units(bytes)));
            }
        }
        breakdowns
            .entry(action.table_name.clone())
            .or_default()
            .merge(&breakdown);
    }
    let consumed_capacity = breakdowns
        .iter()
        .filter_map(|(table_name, breakdown)| breakdown.consume(table_name, mode))
        .collect();
    Ok(TransactWriteItemsOutput {
        consumed_capacity,
        item_collection_metrics: collections,
    })
}

/// Record the collection a write touched, keeping the first one per table: the
/// response map is keyed by table name, and the service reports one entry per
/// table.
fn note_collection(
    table: &Table,
    item: &Item,
    mode: ReturnItemCollectionMetrics,
    now: i64,
    table_name: &str,
    collections: &mut BTreeMap<String, ItemCollectionMetrics>,
) {
    if let Some(metrics) = collection_metrics(table, item, mode, now) {
        collections.entry(table_name.to_string()).or_insert(metrics);
    }
}

/// Keep only the named attributes that exist.
fn select_attributes(item: &Item, names: &BTreeSet<String>) -> Item {
    item.iter()
        .filter(|(name, _)| names.contains(*name))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

// ---------------------------------------------------------------------------
// Capacity helpers
// ---------------------------------------------------------------------------

/// Charge one write: the larger of the old and the new image, on the table and
/// on every index that holds either image.
///
/// The service charges the larger of the two sizes, because removing the old
/// entry and adding the new one both cost capacity, and it does not charge an
/// index the item does not appear in — which is how a sparse index stays free.
fn charge_write(table: &Table, breakdown: &mut Breakdown, images: [Option<&Item>; 2]) {
    let bytes = images
        .iter()
        .flatten()
        .map(|item| item_size(item))
        .max()
        .unwrap_or(0);
    let units = Units::write(write_units(bytes));
    breakdown.charge_table(units);
    let mut names: BTreeSet<&str> = BTreeSet::new();
    for image in images.into_iter().flatten() {
        names.extend(table.index_membership(image));
    }
    for name in names {
        if let Some(scope) = table.index_scope(name) {
            breakdown.charge_index(scope, name, units);
        }
    }
}

/// Charge one read: the index that was read when the operation names one,
/// otherwise the table.
fn charge_read(table: &Table, breakdown: &mut Breakdown, index_name: Option<&str>, units: f64) {
    let units = Units::read(units);
    match index_name.and_then(|name| table.index_scope(name).map(|scope| (scope, name))) {
        Some((scope, name)) => breakdown.charge_index(scope, name, units),
        None => breakdown.charge_table(units),
    }
}

/// The item collection a write touched, when the request asks for it and the
/// table has a local secondary index — the service's own rule, because only
/// then does it track a collection's size.
fn collection_metrics(
    table: &Table,
    item: &Item,
    mode: ReturnItemCollectionMetrics,
    now: i64,
) -> Option<ItemCollectionMetrics> {
    if mode == ReturnItemCollectionMetrics::None || !table.has_local_secondary_indexes() {
        return None;
    }
    let (partition, _) = table.item_key(item).ok()?;
    let name = table.key_schema().partition_key.attribute_name.clone();
    let value = item.get(&name)?.clone();
    let mut key = Item::new();
    key.insert(name, value);
    Some(ItemCollectionMetrics {
        item_collection_key: Some(key),
        size_estimate_range_gb: Some(size_estimate_range(table.collection_size(&partition, now))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::KeyType;
    use crate::{item, n, s};
    use proptest::prelude::*;

    fn mock() -> MockDdb {
        MockDdb::with_table(
            "events",
            KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::Number),
        )
        .unwrap()
    }

    #[test]
    fn segment_assignment_is_stable_and_bounded() {
        let key: ItemKey = (Scalar::S("a".into()), Some(Scalar::N("1".into())));
        let first = segment_of(&key, 4);
        assert!(first < 4);
        assert_eq!(first, segment_of(&key, 4));
    }

    #[test]
    fn fnv1a_matches_the_specification_vectors() {
        // The vectors published at <http://www.isthe.com/chongo/tech/comp/fnv/index.html>.
        assert_eq!(fnv1a_64([b"".as_slice()]), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a_64([b"a".as_slice()]), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a_64([b"foobar".as_slice()]), 0x8594_4171_f739_67e8);
        // Chunks are an unseparated stream: `["foo", "bar"]` is `"foobar"`.
        assert_eq!(
            fnv1a_64([b"foo".as_slice(), b"bar".as_slice()]),
            fnv1a_64([b"foobar".as_slice()])
        );
    }

    #[test]
    fn segment_assignment_uses_the_partition_bytes_alone_for_a_key_only_item() {
        let partition = Scalar::S("answer".into());
        let sort = Scalar::N("42".into());
        let key_only: ItemKey = (partition.clone(), None);
        let full: ItemKey = (partition.clone(), Some(sort.clone()));
        // Hashing the partition bytes alone is the same as hashing them as the
        // prefix of the longer key, so the two keys agree on a one-segment scan.
        assert_eq!(segment_of(&key_only, 1), 0);
        assert_eq!(segment_of(&full, 1), 0);
        // A sort scalar does move the item once there is more than one segment
        // to choose from, which is what splits one partition across segments.
        let differs =
            (2..=64).any(|total| segment_of(&key_only, total) != segment_of(&full, total));
        assert!(differs);
    }

    #[test]
    fn segment_assignment_holds_both_ends_of_the_validated_range() {
        // `totalSegments` is validated to 1..=1_000_000 with `segment < total`
        // (see `parallel_scan_segments_are_disjoint_and_complete`), so those are
        // the just-inside values this function has to survive.
        let partitions = ["", "a", "zzz", "\u{1F600}"];
        let sorts = [
            None,
            Some(Scalar::N("0".into())),
            Some(Scalar::N("-1".into())),
        ];
        for partition in partitions {
            for sort in &sorts {
                let key: ItemKey = (Scalar::S(partition.into()), sort.clone());
                // Just inside, lower bound: the only segment is 0.
                assert_eq!(segment_of(&key, 1), 0);
                // Just inside, upper bound: every index stays below the total.
                assert!(segment_of(&key, 1_000_000) < 1_000_000);
                // A binary key hashes its raw bytes, and they are part of the
                // key space too.
                let binary: ItemKey = (Scalar::B(vec![0x00, 0xff]), sort.clone());
                assert!(segment_of(&binary, 1_000_000) < 1_000_000);
                // Nothing above the total leaks out for any value a caller can
                // reach: the invariant is `segment < total`, not `< 2^32`.
                for total in [2, 3, 7, 4_096, 65_536] {
                    assert!(segment_of(&key, total) < total);
                }
            }
        }
    }

    proptest! {
        /// Every key lands in a segment below the total for every total the
        /// caller can reach, and the same key always lands in the same one.
        #[test]
        fn segment_assignment_is_total_and_deterministic(
            partition in ".*",
            sort in proptest::option::of("[0-9]{1,18}"),
            total in 1u32..=1_000_000u32,
        ) {
            let key: ItemKey = (Scalar::S(partition), sort.map(Scalar::N));
            let segment = segment_of(&key, total);
            prop_assert!(segment < total);
            prop_assert_eq!(segment, segment_of(&key, total));
        }
    }

    #[test]
    fn key_only_item_picks_the_key_attributes() {
        let schema = KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::Number);
        let key = item([("pk", s("a")), ("sk", n("1"))]);
        let built = key_only_item(&schema, &key);
        assert_eq!(built.len(), 2);
        assert_eq!(built.get("pk"), Some(&s("a")));
    }

    #[test]
    fn select_attributes_filters_by_name() {
        let record = item([("pk", s("a")), ("other", s("b"))]);
        let names = BTreeSet::from(["pk".to_string()]);
        let selected = select_attributes(&record, &names);
        assert_eq!(selected.len(), 1);
        assert!(selected.contains_key("pk"));
    }

    #[test]
    fn cursor_ordering_covers_every_position() {
        use std::cmp::Ordering;
        let group = |name: &str| (Scalar::S(name.to_string()), None);
        let at = |name: &str, item: &str| {
            CursorKey::Indexed(
                group(name),
                GroupPosition::At((Scalar::S(item.to_string()), None)),
            )
        };
        let end = |name: &str| CursorKey::Indexed(group(name), GroupPosition::End);

        assert!(at("k", "1") < at("k", "2"));
        assert!(at("k", "2") < end("k"));
        assert_eq!(end("k"), end("k"));
        assert_eq!(
            GroupPosition::End.cmp(&GroupPosition::At((Scalar::S("z".into()), None))),
            Ordering::Greater
        );
        assert_eq!(
            GroupPosition::At((Scalar::S("a".into()), None)).cmp(&GroupPosition::End),
            Ordering::Less
        );
        assert!(CursorKey::Primary(group("a")) < at("a", "1"));
        assert!(at("a", "1") > CursorKey::Primary(group("a")));
        assert!(CursorKey::Primary(group("a")) < CursorKey::Primary(group("b")));
        assert_eq!(
            CursorKey::Primary(group("a")).partial_cmp(&CursorKey::Primary(group("b"))),
            Some(Ordering::Less)
        );
        assert_eq!(
            GroupPosition::End.partial_cmp(&GroupPosition::End),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn debug_lists_table_names_only() {
        let mock = mock();
        let rendered = format!("{mock:?}");
        assert!(rendered.contains("events"));
        assert!(!rendered.contains("secret-value"));
    }

    /// Two writes, `pk` over `keys`, `sk` fixed: the batch tests only vary the
    /// partition key, which keeps the expected order easy to read.
    fn two_writes(keys: [&str; 2]) -> BatchWriteItemRequest {
        let mut request = BatchWriteItemRequest::new();
        for key in keys {
            request = request.add(
                "events",
                WriteRequest::Put {
                    item: item([("pk", s(key)), ("sk", n("1"))]),
                },
            );
        }
        request
    }

    /// Two keys, in request order.
    fn two_keys(keys: [&str; 2]) -> KeysAndAttributes {
        KeysAndAttributes::new(
            keys.into_iter()
                .map(|key| item([("pk", s(key)), ("sk", n("1"))]))
                .collect(),
        )
    }

    #[tokio::test]
    async fn throttled_batch_write_defers_a_prefix_and_the_retry_lands_it() {
        let mock = mock();
        let mut request = BatchWriteItemRequest::new()
            .with_return_consumed_capacity(ReturnConsumedCapacity::Total);
        for key in ["a", "b", "c"] {
            request = request.add(
                "events",
                WriteRequest::Put {
                    item: item([("pk", s(key)), ("sk", n("1"))]),
                },
            );
        }

        mock.throttle_next_batch_write(2);
        let first = mock.batch_write_item(request).await.unwrap();
        // Applied: exactly the writes that were not reported, and only those are
        // charged.
        assert_eq!(mock.count("events"), 1);
        assert_eq!(first.consumed_capacity[0].capacity_units, Some(1.0));
        // Deferred: the first two of the batch, in request order.
        assert_eq!(
            first.unprocessed_items["events"],
            vec![
                WriteRequest::Put {
                    item: item([("pk", s("a")), ("sk", n("1"))])
                },
                WriteRequest::Put {
                    item: item([("pk", s("b")), ("sk", n("1"))])
                },
            ]
        );

        let retry = BatchWriteItemRequest {
            request_items: first.unprocessed_items,
            return_consumed_capacity: ReturnConsumedCapacity::Total,
            ..BatchWriteItemRequest::default()
        };
        let second = mock.batch_write_item(retry).await.unwrap();
        assert!(second.unprocessed_items.is_empty());
        assert_eq!(mock.count("events"), 3);
    }

    #[tokio::test]
    async fn write_throttle_is_clamped_and_spent_once() {
        let mock = mock();

        // Just inside: every write of a batch of two is left unprocessed.
        mock.throttle_next_batch_write(2);
        let all = mock.batch_write_item(two_writes(["a", "b"])).await.unwrap();
        assert_eq!(all.unprocessed_items["events"].len(), 2);
        assert_eq!(mock.count("events"), 0);

        // Just outside: asking for more than the batch can carry clamps to it.
        mock.throttle_next_batch_write(3);
        let over = mock.batch_write_item(two_writes(["c", "d"])).await.unwrap();
        assert_eq!(over.unprocessed_items["events"].len(), 2);
        assert_eq!(mock.count("events"), 0);

        // Zero, and no arm at all: nothing is left unprocessed.
        mock.throttle_next_batch_write(0);
        let none = mock.batch_write_item(two_writes(["e", "f"])).await.unwrap();
        assert!(none.unprocessed_items.is_empty());
        let plain = mock.batch_write_item(two_writes(["g", "h"])).await.unwrap();
        assert!(plain.unprocessed_items.is_empty());
        assert_eq!(mock.count("events"), 4);
    }

    #[tokio::test]
    async fn throttled_batch_get_defers_keys_and_the_retry_reads_them() {
        let mock = mock();
        mock.seed(
            "events",
            vec![
                item([("pk", s("a")), ("sk", n("1"))]),
                item([("pk", s("b")), ("sk", n("1"))]),
            ],
        )
        .unwrap();
        let request = BatchGetItemRequest::new()
            .add("events", two_keys(["a", "b"]))
            .unwrap()
            .with_return_consumed_capacity(ReturnConsumedCapacity::Total);

        mock.throttle_next_batch_get(1);
        let first = mock.batch_get_item(request).await.unwrap();
        // Only the answered key is charged, and the deferred one is the first.
        assert_eq!(first.consumed_capacity[0].capacity_units, Some(1.0));
        assert_eq!(
            first.unprocessed_keys["events"].keys,
            vec![item([("pk", s("a")), ("sk", n("1"))])]
        );
        assert_eq!(
            first.responses["events"],
            vec![item([("pk", s("b")), ("sk", n("1"))])]
        );

        let retry = BatchGetItemRequest::new()
            .add("events", first.unprocessed_keys["events"].clone())
            .unwrap();
        let second = mock.batch_get_item(retry).await.unwrap();
        assert!(second.unprocessed_keys.is_empty());
        assert_eq!(second.responses["events"].len(), 1);
    }

    #[tokio::test]
    async fn a_deferred_key_carries_the_projection_and_aliases_it_came_with() {
        use std::collections::HashMap;

        let mock = mock();
        mock.seed(
            "events",
            vec![item([("pk", s("a")), ("sk", n("1")), ("note", s("x"))])],
        )
        .unwrap();
        let mut names = HashMap::new();
        names.insert("#n".to_string(), "note".to_string());
        let attributes = KeysAndAttributes {
            keys: vec![item([("pk", s("a")), ("sk", n("1"))])],
            projection_expression: Some("#n".to_string()),
            expression_attribute_names: Some(names),
            consistent_read: true,
        };

        mock.throttle_next_batch_get(1);
        let first = mock
            .batch_get_item(
                BatchGetItemRequest::new()
                    .add("events", attributes.clone())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(first.responses.is_empty());
        assert_eq!(first.unprocessed_keys["events"], attributes);

        let retry = mock
            .batch_get_item(
                BatchGetItemRequest::new()
                    .add("events", first.unprocessed_keys["events"].clone())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retry.responses["events"], vec![item([("note", s("x"))])]);
    }

    #[tokio::test]
    async fn a_rejected_batch_keeps_the_armed_throttle() {
        let mock = mock();
        mock.throttle_next_batch_write(1);

        // Unknown table and duplicate key are refused before anything is
        // deferred, so the knob still applies to the next well-formed batch.
        let unknown = BatchWriteItemRequest::new().add(
            "nope",
            WriteRequest::Put {
                item: item([("pk", s("a")), ("sk", n("1"))]),
            },
        );
        assert!(mock.batch_write_item(unknown).await.is_err());

        let duplicate = two_writes(["a", "a"]);
        assert!(mock.batch_write_item(duplicate).await.is_err());
        assert_eq!(mock.count("events"), 0);

        let throttled = mock.batch_write_item(two_writes(["a", "b"])).await.unwrap();
        assert_eq!(throttled.unprocessed_items["events"].len(), 1);
        assert_eq!(mock.count("events"), 1);

        // A malformed projection is refused even when every key is deferred.
        mock.throttle_next_batch_get(1);
        let mut attributes = two_keys(["a", "b"]);
        attributes.projection_expression = Some("pk, #missing".to_string());
        assert!(mock
            .batch_get_item(
                BatchGetItemRequest::new()
                    .add("events", attributes)
                    .unwrap()
            )
            .await
            .is_err());
        let still_armed = mock
            .batch_get_item(
                BatchGetItemRequest::new()
                    .add("events", two_keys(["a", "b"]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(still_armed.unprocessed_keys["events"].keys.len(), 1);
    }

    #[tokio::test]
    async fn the_two_throttles_are_independent() {
        let mock = mock();
        mock.seed("events", vec![item([("pk", s("a")), ("sk", n("1"))])])
            .unwrap();
        mock.throttle_next_batch_write(1);
        mock.throttle_next_batch_get(1);

        // A read spends the read knob only, so the write knob still applies.
        let read = mock
            .batch_get_item(
                BatchGetItemRequest::new()
                    .add("events", two_keys(["a", "b"]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(read.unprocessed_keys["events"].keys.len(), 1);

        let write = mock.batch_write_item(two_writes(["c", "d"])).await.unwrap();
        assert_eq!(write.unprocessed_items["events"].len(), 1);

        // Both knobs are spent: these calls are whole again.
        assert!(mock
            .batch_get_item(
                BatchGetItemRequest::new()
                    .add("events", two_keys(["a", "b"]))
                    .unwrap()
            )
            .await
            .unwrap()
            .unprocessed_keys
            .is_empty());
        assert!(mock
            .batch_write_item(two_writes(["e", "f"]))
            .await
            .unwrap()
            .unprocessed_items
            .is_empty());
    }
}
