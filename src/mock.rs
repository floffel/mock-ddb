//! The mock client: control plane, data plane, transactions and TTL.
//!
//! Every operation takes `&self` and runs under one `RwLock` over the whole
//! store, so a clone of the mock can be shared by any number of tasks and each
//! operation sees a consistent snapshot. Write operations hold the write lock
//! for their whole duration, which is what makes transactions atomic and batch
//! writes all-or-nothing on validation errors.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::clock::{epoch_seconds, Clock, SystemClock};
use crate::error::{CancellationReason, CancellationReasonCode, Error, Result};
use crate::expr::{
    CmpOp, Condition, ConditionExpression, NameMap, Operand, ProjectionExpression, Segment,
    UpdateExpression, ValueMap,
};
use crate::request::{
    BatchGetItemOutput, BatchGetItemRequest, BatchWriteItemOutput, BatchWriteItemRequest,
    DeleteItemOutput, DeleteItemRequest, GetItemOutput, GetItemRequest, ListTablesOutput,
    ListTablesRequest, PutItemOutput, PutItemRequest, QueryOutput, QueryRequest, ReturnValues,
    ScanOutput, ScanRequest, Select, TimeToLiveDescription, TimeToLiveStatus,
    TransactGetItemsOutput, TransactGetItemsRequest, TransactWriteItem, TransactWriteItemsRequest,
    UpdateItemOutput, UpdateItemRequest, UpdateTimeToLiveRequest, WriteRequest,
};
use crate::schema::{CreateTableRequest, KeySchema, TableDescription};
use crate::table::{index_key, IndexKey, ItemKey, Table, TableDefinition, Ttl};
use crate::value::Scalar;
use crate::Item;

#[cfg(feature = "streams")]
use crate::stream::StreamRecord;

/// The mutable state behind the client.
struct Inner {
    tables: BTreeMap<String, Table>,
    clock: Arc<dyn Clock>,
    /// Counter that seeds stream sequence numbers, so records from two tables
    /// never compare equal.
    sequence: u64,
    /// Client request tokens that have already been applied.
    applied_tokens: BTreeSet<String>,
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
}

impl Default for MockDdb {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MockDdb {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.read();
        formatter
            .debug_struct("MockDdb")
            .field("tables", &inner.tables.keys().collect::<Vec<_>>())
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
                clock,
                sequence: 1,
                applied_tokens: BTreeSet::new(),
            })),
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
        let now = mock.inner.read().clock.now();
        mock.insert_table(definition, now)?;
        Ok(mock)
    }

    /// Replace the clock, e.g. to pin time in the middle of a test.
    pub fn set_clock(&self, clock: Arc<dyn Clock>) {
        self.inner.write().clock = clock;
    }

    /// The clock's current instant, in whole seconds since the Unix epoch.
    #[must_use]
    pub fn now_seconds(&self) -> i64 {
        epoch_seconds(self.inner.read().clock.now())
    }

    /// Seed items into an existing table without conditions.
    ///
    /// This is the shortcut a test uses instead of a loop of `put_item` calls:
    /// the items are validated and indexed exactly as a real write would be.
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
        let mut inner = self.inner.write();
        let now = epoch_seconds(inner.clock.now());
        let table = inner
            .tables
            .get_mut(table_name)
            .ok_or_else(|| Error::table_not_found(table_name))?;
        for item in items {
            table.validate_item(&item)?;
            let key = table.item_key(&item)?;
            let previous = table.items.get(&key).cloned();
            table.store(key, item.clone(), previous.as_ref());
            if !table.is_expired(&item, now) {
                table.on_put(previous.as_ref(), &item);
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
    pub fn count(&self, table_name: &str) -> usize {
        let inner = self.inner.read();
        let now = epoch_seconds(inner.clock.now());
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
        let inner = self.inner.read();
        let now = epoch_seconds(inner.clock.now());
        inner.tables.get(table_name).map_or_else(Vec::new, |table| {
            table
                .items
                .values()
                .filter(|item| !table.is_expired(item, now))
                .cloned()
                .collect()
        })
    }

    /// Drop every table.
    pub fn clear(&self) {
        let mut inner = self.inner.write();
        inner.tables.clear();
        inner.applied_tokens.clear();
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
        let now = self.inner.read().clock.now();
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

    /// Delete a table and return its final description.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ResourceNotFound`] when the table does not exist.
    pub async fn delete_table(&self, table_name: &str) -> Result<TableDescription> {
        let mut inner = self.inner.write();
        let now = epoch_seconds(inner.clock.now());
        let table = inner
            .tables
            .remove(table_name)
            .ok_or_else(|| Error::table_not_found(table_name))?;
        Ok(table.describe(table.live_count(now)))
    }

    /// Describe a table.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ResourceNotFound`] when the table does not exist.
    pub async fn describe_table(&self, table_name: &str) -> Result<TableDescription> {
        let inner = self.inner.read();
        let now = epoch_seconds(inner.clock.now());
        let table = inner
            .tables
            .get(table_name)
            .ok_or_else(|| Error::table_not_found(table_name))?;
        Ok(table.describe(table.live_count(now)))
    }

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

    fn put_item_inner(&self, request: &PutItemRequest) -> Result<PutItemOutput> {
        let mut inner = self.inner.write();
        let now = epoch_seconds(inner.clock.now());
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
        Ok(PutItemOutput {
            attributes: match request.return_values {
                // An expired item is logically absent, so it is not "the old
                // value" even though its row is still in the store.
                ReturnValues::AllOld => previous_live,
                _ => None,
            },
        })
    }

    /// Read an item.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ResourceNotFound`] for an unknown table,
    /// [`Error::Validation`] for a key that does not match the schema, and for
    /// a malformed projection.
    pub async fn get_item(&self, request: GetItemRequest) -> Result<GetItemOutput> {
        self.get_item_inner(&request)
    }

    fn get_item_inner(&self, request: &GetItemRequest) -> Result<GetItemOutput> {
        let inner = self.inner.read();
        let now = epoch_seconds(inner.clock.now());
        let table = inner
            .tables
            .get(&request.table_name)
            .ok_or_else(|| Error::table_not_found(&request.table_name))?;
        let key = table.lookup_key(&request.key)?;
        let item = table
            .items
            .get(&key)
            .filter(|item| !table.is_expired(item, now));

        let item = match item {
            None => None,
            Some(item) => match &request.projection_expression {
                None => Some(item.clone()),
                Some(expression) => {
                    let names = request
                        .expression_attribute_names
                        .clone()
                        .unwrap_or_default();
                    Some(ProjectionExpression::parse(expression)?.project(item, &names)?)
                }
            },
        };
        Ok(GetItemOutput { item })
    }

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

    fn delete_item_inner(&self, request: &DeleteItemRequest) -> Result<DeleteItemOutput> {
        let mut inner = self.inner.write();
        let now = epoch_seconds(inner.clock.now());
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
        Ok(DeleteItemOutput {
            attributes: match request.return_values {
                ReturnValues::AllOld => removed.filter(|_| previous_live.is_some()),
                _ => None,
            },
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

    fn update_item_inner(&self, request: &UpdateItemRequest) -> Result<UpdateItemOutput> {
        let mut inner = self.inner.write();
        let now = epoch_seconds(inner.clock.now());
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
        let touched = update.apply_to_table(&mut item, &names, &values, table.key_schema())?;
        table.validate_item(&item)?;
        table.store(key, item.clone(), previous.as_ref());
        table.on_update(previous_live.as_ref(), &item);

        let attributes = match request.return_values {
            ReturnValues::None => None,
            ReturnValues::AllNew => Some(item),
            ReturnValues::AllOld => previous_live,
            ReturnValues::UpdatedNew => Some(select_attributes(&item, &touched)),
            ReturnValues::UpdatedOld => previous_live
                .as_ref()
                .map(|old| select_attributes(old, &touched)),
        };
        Ok(UpdateItemOutput { attributes })
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
    pub async fn query(&self, request: QueryRequest) -> Result<QueryOutput> {
        self.query_inner(&request)
    }

    fn query_inner(&self, request: &QueryRequest) -> Result<QueryOutput> {
        validate_limit(request.limit)?;
        let inner = self.inner.read();
        let now = epoch_seconds(inner.clock.now());
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
                Some(condition)
            }
        };

        let page = Page {
            limit: request.limit,
            filter: request.filter_expression.as_deref(),
            key_condition: key_condition.as_ref(),
            projection: request.projection_expression.as_deref(),
            select: request.select,
            names: &names,
            values: &values,
            descending: !request.scan_index_forward.unwrap_or(true),
        };
        let (items, count, scanned_count, last_evaluated_key) = collect_page(
            table,
            index_name,
            request.exclusive_start_key.as_ref(),
            &page,
            now,
            None,
        )?;
        Ok(QueryOutput {
            items,
            count,
            scanned_count,
            last_evaluated_key,
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
    pub async fn scan(&self, request: ScanRequest) -> Result<ScanOutput> {
        self.scan_inner(&request)
    }

    fn scan_inner(&self, request: &ScanRequest) -> Result<ScanOutput> {
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

        let inner = self.inner.read();
        let now = epoch_seconds(inner.clock.now());
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

        let page = Page {
            limit: request.limit,
            filter: request.filter_expression.as_deref(),
            key_condition: None,
            projection: request.projection_expression.as_deref(),
            select: request.select,
            names: &names,
            values: &values,
            descending: false,
        };
        let (items, count, scanned_count, last_evaluated_key) = collect_page(
            table,
            index_name,
            request.exclusive_start_key.as_ref(),
            &page,
            now,
            segment,
        )?;
        Ok(ScanOutput {
            items,
            count,
            scanned_count,
            last_evaluated_key,
        })
    }

    /// Write up to 25 items in one non-atomic batch.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] when the batch is empty, has more than 25
    /// writes, repeats a key, or contains an item that does not match its
    /// table, and [`Error::ResourceNotFound`] for an unknown table. Nothing is
    /// written when any write fails validation.
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
        let mut inner = self.inner.write();
        let now = epoch_seconds(inner.clock.now());
        let total: usize = request.request_items.values().map(Vec::len).sum();
        if total == 0 || total > 25 {
            return Err(Error::validation(format!(
                "1 validation error detected: Value at 'requestItems' failed to satisfy constraint: Member must have length between 1 and 25, got {total}"
            )));
        }

        let mut seen: BTreeSet<(String, ItemKey)> = BTreeSet::new();
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
                if !seen.insert((table_name.clone(), key)) {
                    return Err(Error::validation(
                        "Provided list of item keys contains duplicates",
                    ));
                }
            }
        }

        for (table_name, writes) in request.request_items {
            let table = inner
                .tables
                .get_mut(&table_name)
                .ok_or_else(|| Error::table_not_found(&table_name))?;
            for write in writes {
                match write {
                    WriteRequest::Put { item } => {
                        let key = table.item_key(&item)?;
                        let previous = table.items.get(&key).cloned();
                        let previous_live = previous
                            .clone()
                            .filter(|existing| !table.is_expired(existing, now));
                        table.store(key, item.clone(), previous.as_ref());
                        table.on_put(previous_live.as_ref(), &item);
                    }
                    WriteRequest::Delete { key } => {
                        let key = table.lookup_key(&key)?;
                        if let Some(previous) = table.discard(&key) {
                            if !table.is_expired(&previous, now) {
                                table.on_delete(&previous);
                            }
                        }
                    }
                }
            }
        }
        Ok(BatchWriteItemOutput::default())
    }

    /// Read up to 100 keys in one batch. Missing keys are omitted.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] when the batch is empty, asks for more than
    /// 100 keys, or carries a malformed key or projection, and
    /// [`Error::ResourceNotFound`] for an unknown table.
    pub async fn batch_get_item(&self, request: BatchGetItemRequest) -> Result<BatchGetItemOutput> {
        self.batch_get_item_inner(&request)
    }

    fn batch_get_item_inner(&self, request: &BatchGetItemRequest) -> Result<BatchGetItemOutput> {
        let inner = self.inner.read();
        let now = epoch_seconds(inner.clock.now());
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

        let mut responses: BTreeMap<String, Vec<Item>> = BTreeMap::new();
        for (table_name, attributes) in &request.request_items {
            let table = inner
                .tables
                .get(table_name)
                .ok_or_else(|| Error::table_not_found(table_name))?;
            let names = attributes
                .expression_attribute_names
                .clone()
                .unwrap_or_default();
            let mut found = Vec::new();
            for key in &attributes.keys {
                let key = table.lookup_key(key)?;
                let Some(item) = table
                    .items
                    .get(&key)
                    .filter(|item| !table.is_expired(item, now))
                else {
                    continue;
                };
                let item = match &attributes.projection_expression {
                    None => item.clone(),
                    Some(expression) => {
                        ProjectionExpression::parse(expression)?.project(item, &names)?
                    }
                };
                found.push(item);
            }
            if !found.is_empty() {
                responses.insert(table_name.clone(), found);
            }
        }
        Ok(BatchGetItemOutput {
            responses,
            unprocessed_keys: BTreeMap::new(),
        })
    }

    /// Run an atomic transaction of up to 100 actions.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] for an empty transaction, more than 100
    /// actions, two actions on the same item, an oversized token or a malformed
    /// expression; [`Error::ResourceNotFound`] for an unknown table; and
    /// [`Error::TransactionCanceled`] when any condition fails. A cancelled
    /// transaction writes nothing.
    pub async fn transact_write_items(&self, request: TransactWriteItemsRequest) -> Result<()> {
        self.transact_write_items_inner(request)
    }

    fn transact_write_items_inner(&self, request: TransactWriteItemsRequest) -> Result<()> {
        let mut inner = self.inner.write();
        let now = epoch_seconds(inner.clock.now());
        if request.transact_items.is_empty() || request.transact_items.len() > 100 {
            return Err(Error::validation(format!(
                "1 validation error detected: Value at 'transactItems' failed to satisfy constraint: Member must have length between 1 and 100, got {}",
                request.transact_items.len()
            )));
        }
        if let Some(token) = &request.client_request_token {
            if token.is_empty() || token.len() > 36 {
                return Err(Error::validation(
                    "1 validation error detected: Value at 'clientRequestToken' failed to satisfy constraint: Member must have length between 1 and 36",
                ));
            }
            if inner.applied_tokens.contains(token) {
                return Ok(());
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

        for action in prepared {
            let table = inner
                .tables
                .get_mut(&action.table_name)
                .ok_or_else(|| Error::table_not_found(&action.table_name))?;
            match action.kind {
                ActionKind::Put(item) => {
                    table.store(action.key, item.clone(), action.previous.as_ref());
                    table.on_put(action.previous.as_ref(), &item);
                }
                ActionKind::Update(item) => {
                    table.store(action.key, item.clone(), action.previous.as_ref());
                    table.on_update(action.previous.as_ref(), &item);
                }
                ActionKind::Delete => {
                    let _ = table.discard(&action.key);
                    if let Some(previous) = &action.previous {
                        table.on_delete(previous);
                    }
                }
                ActionKind::ConditionCheck => {}
            }
        }

        if let Some(token) = request.client_request_token {
            inner.applied_tokens.insert(token);
        }
        Ok(())
    }

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
        let inner = self.inner.read();
        let now = epoch_seconds(inner.clock.now());
        if request.transact_items.is_empty() || request.transact_items.len() > 100 {
            return Err(Error::validation(format!(
                "1 validation error detected: Value at 'transactItems' failed to satisfy constraint: Member must have length between 1 and 100, got {}",
                request.transact_items.len()
            )));
        }
        let mut responses = Vec::with_capacity(request.transact_items.len());
        for get in &request.transact_items {
            let table = inner
                .tables
                .get(&get.table_name)
                .ok_or_else(|| Error::table_not_found(&get.table_name))?;
            let key = table.lookup_key(&get.key)?;
            let item = table
                .items
                .get(&key)
                .filter(|item| !table.is_expired(item, now));
            let item = match item {
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
        Ok(TransactGetItemsOutput { responses })
    }

    // -- TTL ----------------------------------------------------------------

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
        let mut inner = self.inner.write();
        let now = epoch_seconds(inner.clock.now());
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
}

// ---------------------------------------------------------------------------
// Read path helpers
// ---------------------------------------------------------------------------

/// One page of a query or scan.
struct Page<'a> {
    limit: Option<u32>,
    filter: Option<&'a str>,
    key_condition: Option<&'a ConditionExpression>,
    projection: Option<&'a str>,
    select: Select,
    names: &'a NameMap,
    values: &'a ValueMap,
    descending: bool,
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

/// Apply an index's projection, then an explicit projection expression.
fn projected_item(
    table: &Table,
    index_name: Option<&str>,
    expression: Option<&str>,
    item: &Item,
    names: &NameMap,
) -> Result<Item> {
    let base = match index_name.and_then(|name| table.projected_attributes(name)) {
        None => item.clone(),
        Some(allowed) => item
            .iter()
            .filter(|(name, _)| allowed.contains(*name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
    };
    match expression {
        None => Ok(base),
        Some(expression) => ProjectionExpression::parse(expression)?.project(&base, names),
    }
}

/// Collect one page of a query or scan.
#[allow(clippy::type_complexity)]
fn collect_page(
    table: &Table,
    index_name: Option<&str>,
    start_key: Option<&Item>,
    page: &Page<'_>,
    now: i64,
    segment: Option<(u32, u32)>,
) -> Result<(Vec<Item>, usize, usize, Option<Item>)> {
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
        let matches = match page.filter {
            None => true,
            Some(expression) => ConditionExpression::parse(expression)?.evaluate(
                Some(item),
                page.names,
                page.values,
            )?,
        };
        if matches {
            count += 1;
            if page.select != Select::Count {
                items.push(projected_item(
                    table,
                    index_name,
                    page.projection,
                    item,
                    page.names,
                )?);
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

    Ok((items, count, scanned, last_evaluated_key))
}

/// FNV-1a over the key's bytes, used to assign items to parallel scan segments.
fn segment_of(key: &ItemKey, total_segments: u32) -> u32 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for scalar in [Some(&key.0), key.1.as_ref()].into_iter().flatten() {
        let bytes: &[u8] = match scalar {
            Scalar::S(text) | Scalar::N(text) => text.as_bytes(),
            Scalar::B(bytes) => bytes,
        };
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
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
    previous: Option<Item>,
    condition: Option<ConditionExpression>,
    names: NameMap,
    values: ValueMap,
    kind: ActionKind,
    wants_old_image: bool,
}

fn prepare_action(inner: &Inner, action: &TransactWriteItem, now: i64) -> Result<PreparedAction> {
    match action {
        TransactWriteItem::Put(put) => {
            let table = inner
                .tables
                .get(&put.table_name)
                .ok_or_else(|| Error::table_not_found(&put.table_name))?;
            table.validate_item(&put.item)?;
            let key = table.item_key(&put.item)?;
            Ok(PreparedAction {
                table_name: put.table_name.clone(),
                previous: live_item(table, &key, now),
                key,
                condition: parse_optional_condition(put.condition_expression.as_deref())?,
                names: put.expression_attribute_names.clone().unwrap_or_default(),
                values: put.expression_attribute_values.clone().unwrap_or_default(),
                kind: ActionKind::Put(put.item.clone()),
                wants_old_image: put.return_values_on_condition_check_failure,
            })
        }
        TransactWriteItem::Update(update) => {
            let table = inner
                .tables
                .get(&update.table_name)
                .ok_or_else(|| Error::table_not_found(&update.table_name))?;
            let key = table.lookup_key(&update.key)?;
            let previous = live_item(table, &key, now);
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
            expression.apply_to_table(&mut item, &names, &values, table.key_schema())?;
            table.validate_item(&item)?;
            Ok(PreparedAction {
                table_name: update.table_name.clone(),
                key,
                previous,
                condition: parse_optional_condition(update.condition_expression.as_deref())?,
                names,
                values,
                kind: ActionKind::Update(item),
                wants_old_image: update.return_values_on_condition_check_failure,
            })
        }
        TransactWriteItem::Delete(delete) => {
            let table = inner
                .tables
                .get(&delete.table_name)
                .ok_or_else(|| Error::table_not_found(&delete.table_name))?;
            let key = table.lookup_key(&delete.key)?;
            Ok(PreparedAction {
                table_name: delete.table_name.clone(),
                previous: live_item(table, &key, now),
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
            let table = inner
                .tables
                .get(&check.table_name)
                .ok_or_else(|| Error::table_not_found(&check.table_name))?;
            let key = table.lookup_key(&check.key)?;
            Ok(PreparedAction {
                table_name: check.table_name.clone(),
                previous: live_item(table, &key, now),
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

/// Read the live (non-expired) item at a key.
fn live_item(table: &Table, key: &ItemKey, now: i64) -> Option<Item> {
    table
        .items
        .get(key)
        .filter(|item| !table.is_expired(item, now))
        .cloned()
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

/// Keep only the named attributes that exist.
fn select_attributes(item: &Item, names: &BTreeSet<String>) -> Item {
    item.iter()
        .filter(|(name, _)| names.contains(*name))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::KeyType;
    use crate::{item, n, s};

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
}
