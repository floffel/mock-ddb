//! Request and response types, shaped like the AWS SDK's.
//!
//! Every field keeps the SDK's name (`condition_expression`,
//! `expression_attribute_values`, `scan_index_forward`, …) and the item type is
//! the SDK's own `HashMap<String, AttributeValue>`, so moving a test from the
//! real client to the mock is a matter of calling `mock.put_item(request)`
//! instead of `client.put_item().set_…().send()`.
//!
//! ```
//! use mock_ddb::{GetItemRequest, KeySchema, KeyType, MockDdb, PutItemRequest, item, s};
//!
//! # #[tokio::main]
//! # async fn main() {
//! let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
//! mock.put_item(PutItemRequest::new("users", item([("pk", s("u#1"))])))
//!     .await
//!     .unwrap();
//!
//! let response = mock
//!     .get_item(GetItemRequest::new("users", item([("pk", s("u#1"))])))
//!     .await
//!     .unwrap();
//! assert!(response.item.is_some());
//! # }
//! ```

use std::collections::{BTreeMap, HashMap};

use aws_sdk_dynamodb::types::AttributeValue;

use crate::schema::TableDescription;
use crate::Item;

/// Build an item from `(name, value)` pairs.
///
/// ```
/// use mock_ddb::{item, n, s};
/// let record = item([("pk", s("u#1")), ("age", n("42"))]);
/// assert_eq!(record.len(), 2);
/// ```
pub fn item<I, K>(entries: I) -> Item
where
    I: IntoIterator<Item = (K, AttributeValue)>,
    K: Into<String>,
{
    entries
        .into_iter()
        .map(|(name, value)| (name.into(), value))
        .collect()
}

/// Build an expression-attribute-name map (`#alias` → attribute name).
///
/// ```
/// use mock_ddb::names;
/// let map = names([("#n", "name")]);
/// assert_eq!(map["#n"], "name");
/// ```
pub fn names<I, K, V>(entries: I) -> HashMap<String, String>
where
    I: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<String>,
{
    entries
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect()
}

/// Build an expression-attribute-value map (`:placeholder` → value).
///
/// ```
/// use mock_ddb::{s, values};
/// let map = values([(":v", s("x"))]);
/// assert_eq!(map.len(), 1);
/// ```
pub fn values<I, K>(entries: I) -> HashMap<String, AttributeValue>
where
    I: IntoIterator<Item = (K, AttributeValue)>,
    K: Into<String>,
{
    entries
        .into_iter()
        .map(|(key, value)| (key.into(), value))
        .collect()
}

/// Which item attributes an operation returns.
///
/// ```
/// use mock_ddb::ReturnValues;
/// assert_eq!(ReturnValues::default(), ReturnValues::None);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReturnValues {
    /// Return nothing (the default).
    #[default]
    None,
    /// Every attribute as it was before the write.
    AllOld,
    /// The attributes the write changed, as they were before.
    UpdatedOld,
    /// Every attribute after the write.
    AllNew,
    /// The attributes the write changed, after the write.
    UpdatedNew,
}

/// What a query or scan returns.
///
/// ```
/// use mock_ddb::Select;
/// assert_eq!(Select::default(), Select::AllAttributes);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Select {
    /// Every projected attribute.
    #[default]
    AllAttributes,
    /// Only the counts.
    Count,
    /// Only the attributes named in `projection_expression`.
    SpecificAttributes,
}

/// `PutItem` input.
///
/// ```
/// use mock_ddb::{item, s, PutItemRequest};
/// let request = PutItemRequest::new("users", item([("pk", s("u#1"))]))
///     .with_condition("attribute_not_exists(pk)");
/// assert!(request.condition_expression.is_some());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PutItemRequest {
    /// Table to write to.
    pub table_name: String,
    /// The item. Must carry every key attribute.
    pub item: Item,
    /// Optional condition that must hold for the write to happen.
    pub condition_expression: Option<String>,
    /// `#alias` → attribute name.
    pub expression_attribute_names: Option<HashMap<String, String>>,
    /// `:placeholder` → value.
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    /// Which attributes to return.
    pub return_values: ReturnValues,
    /// Return the existing item when the condition fails.
    pub return_values_on_condition_check_failure: bool,
}

impl PutItemRequest {
    /// A request that writes `item` to `table_name`.
    #[must_use]
    pub fn new(table_name: impl Into<String>, item: Item) -> Self {
        Self {
            table_name: table_name.into(),
            item,
            ..Self::default()
        }
    }

    /// Require `expression` to hold before writing.
    #[must_use]
    pub fn with_condition(mut self, expression: impl Into<String>) -> Self {
        self.condition_expression = Some(expression.into());
        self
    }

    /// Attach expression-attribute names.
    #[must_use]
    pub fn with_names(mut self, names: HashMap<String, String>) -> Self {
        self.expression_attribute_names = Some(names);
        self
    }

    /// Attach expression-attribute values.
    #[must_use]
    pub fn with_values(mut self, values: HashMap<String, AttributeValue>) -> Self {
        self.expression_attribute_values = Some(values);
        self
    }

    /// Choose what the write returns.
    #[must_use]
    pub fn with_return_values(mut self, return_values: ReturnValues) -> Self {
        self.return_values = return_values;
        self
    }

    /// Include the existing item in the failure when the condition does not
    /// hold.
    #[must_use]
    pub fn with_return_values_on_condition_check_failure(mut self) -> Self {
        self.return_values_on_condition_check_failure = true;
        self
    }
}

/// `PutItem` output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PutItemOutput {
    /// The previous item, when `ReturnValues::AllOld` was requested.
    pub attributes: Option<Item>,
}

/// `GetItem` input.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GetItemRequest {
    /// Table to read from.
    pub table_name: String,
    /// The full primary key.
    pub key: Item,
    /// Accepted and ignored: the mock is always strongly consistent.
    pub consistent_read: bool,
    /// Optional projection expression.
    pub projection_expression: Option<String>,
    /// `#alias` → attribute name.
    pub expression_attribute_names: Option<HashMap<String, String>>,
}

impl GetItemRequest {
    /// A read of `key` from `table_name`.
    #[must_use]
    pub fn new(table_name: impl Into<String>, key: Item) -> Self {
        Self {
            table_name: table_name.into(),
            key,
            ..Self::default()
        }
    }

    /// Project the response down to `expression`.
    #[must_use]
    pub fn with_projection(mut self, expression: impl Into<String>) -> Self {
        self.projection_expression = Some(expression.into());
        self
    }

    /// Attach expression-attribute names for the projection.
    #[must_use]
    pub fn with_names(mut self, names: HashMap<String, String>) -> Self {
        self.expression_attribute_names = Some(names);
        self
    }
}

/// `GetItem` output. `item` is `None` when the key is absent or expired.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GetItemOutput {
    /// The item, if it exists.
    pub item: Option<Item>,
}

/// `DeleteItem` input.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DeleteItemRequest {
    /// Table to delete from.
    pub table_name: String,
    /// The full primary key.
    pub key: Item,
    /// Optional condition that must hold for the delete to happen.
    pub condition_expression: Option<String>,
    /// `#alias` → attribute name.
    pub expression_attribute_names: Option<HashMap<String, String>>,
    /// `:placeholder` → value.
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    /// Which attributes to return.
    pub return_values: ReturnValues,
    /// Return the existing item when the condition fails.
    pub return_values_on_condition_check_failure: bool,
}

impl DeleteItemRequest {
    /// A delete of `key` from `table_name`.
    #[must_use]
    pub fn new(table_name: impl Into<String>, key: Item) -> Self {
        Self {
            table_name: table_name.into(),
            key,
            ..Self::default()
        }
    }

    /// Require `expression` to hold before deleting.
    #[must_use]
    pub fn with_condition(mut self, expression: impl Into<String>) -> Self {
        self.condition_expression = Some(expression.into());
        self
    }

    /// Attach expression-attribute names.
    #[must_use]
    pub fn with_names(mut self, names: HashMap<String, String>) -> Self {
        self.expression_attribute_names = Some(names);
        self
    }

    /// Attach expression-attribute values.
    #[must_use]
    pub fn with_values(mut self, values: HashMap<String, AttributeValue>) -> Self {
        self.expression_attribute_values = Some(values);
        self
    }

    /// Choose what the delete returns.
    #[must_use]
    pub fn with_return_values(mut self, return_values: ReturnValues) -> Self {
        self.return_values = return_values;
        self
    }

    /// Include the existing item in the failure when the condition does not
    /// hold.
    #[must_use]
    pub fn with_return_values_on_condition_check_failure(mut self) -> Self {
        self.return_values_on_condition_check_failure = true;
        self
    }
}

/// `DeleteItem` output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DeleteItemOutput {
    /// The deleted item, when `ReturnValues::AllOld` was requested.
    pub attributes: Option<Item>,
}

/// `UpdateItem` input.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UpdateItemRequest {
    /// Table to update.
    pub table_name: String,
    /// The full primary key.
    pub key: Item,
    /// The update expression.
    pub update_expression: String,
    /// Optional condition that must hold for the update to happen.
    pub condition_expression: Option<String>,
    /// `#alias` → attribute name.
    pub expression_attribute_names: Option<HashMap<String, String>>,
    /// `:placeholder` → value.
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    /// Which attributes to return.
    pub return_values: ReturnValues,
    /// Return the existing item when the condition fails.
    pub return_values_on_condition_check_failure: bool,
}

impl UpdateItemRequest {
    /// An update of `key` in `table_name` with `expression`.
    #[must_use]
    pub fn new(table_name: impl Into<String>, key: Item, expression: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            key,
            update_expression: expression.into(),
            ..Self::default()
        }
    }

    /// Require `expression` to hold before updating.
    #[must_use]
    pub fn with_condition(mut self, expression: impl Into<String>) -> Self {
        self.condition_expression = Some(expression.into());
        self
    }

    /// Attach expression-attribute names.
    #[must_use]
    pub fn with_names(mut self, names: HashMap<String, String>) -> Self {
        self.expression_attribute_names = Some(names);
        self
    }

    /// Attach expression-attribute values.
    #[must_use]
    pub fn with_values(mut self, values: HashMap<String, AttributeValue>) -> Self {
        self.expression_attribute_values = Some(values);
        self
    }

    /// Choose what the update returns.
    #[must_use]
    pub fn with_return_values(mut self, return_values: ReturnValues) -> Self {
        self.return_values = return_values;
        self
    }

    /// Include the existing item in the failure when the condition does not
    /// hold.
    #[must_use]
    pub fn with_return_values_on_condition_check_failure(mut self) -> Self {
        self.return_values_on_condition_check_failure = true;
        self
    }
}

/// `UpdateItem` output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UpdateItemOutput {
    /// The attributes selected by `return_values`.
    pub attributes: Option<Item>,
}

/// `Query` input.
///
/// ```
/// use mock_ddb::QueryRequest;
/// let request = QueryRequest::new("users", "pk = :pk").with_scan_index_forward(false);
/// assert_eq!(request.scan_index_forward, Some(false));
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryRequest {
    /// Table to query.
    pub table_name: String,
    /// Index to query instead of the table.
    pub index_name: Option<String>,
    /// The key condition. The mock also accepts `None` and returns every item.
    pub key_condition_expression: Option<String>,
    /// Optional filter applied after the key condition.
    pub filter_expression: Option<String>,
    /// `#alias` → attribute name.
    pub expression_attribute_names: Option<HashMap<String, String>>,
    /// `:placeholder` → value.
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    /// Optional projection expression.
    pub projection_expression: Option<String>,
    /// `true` (the default) sorts by ascending sort key.
    pub scan_index_forward: Option<bool>,
    /// Maximum number of items to evaluate.
    pub limit: Option<u32>,
    /// Continue after this key.
    pub exclusive_start_key: Option<Item>,
    /// Accepted and ignored: the mock is always strongly consistent.
    pub consistent_read: bool,
    /// What to return.
    pub select: Select,
}

impl QueryRequest {
    /// A query of `table_name` with `key_condition`.
    #[must_use]
    pub fn new(table_name: impl Into<String>, key_condition: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            key_condition_expression: Some(key_condition.into()),
            ..Self::default()
        }
    }

    /// Query a secondary index.
    #[must_use]
    pub fn with_index(mut self, index_name: impl Into<String>) -> Self {
        self.index_name = Some(index_name.into());
        self
    }

    /// Apply a filter after the key condition.
    #[must_use]
    pub fn with_filter(mut self, expression: impl Into<String>) -> Self {
        self.filter_expression = Some(expression.into());
        self
    }

    /// Set the sort direction.
    #[must_use]
    pub fn with_scan_index_forward(mut self, ascending: bool) -> Self {
        self.scan_index_forward = Some(ascending);
        self
    }

    /// Cap the number of evaluated items.
    #[must_use]
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Continue after `key`.
    #[must_use]
    pub fn with_exclusive_start_key(mut self, key: Item) -> Self {
        self.exclusive_start_key = Some(key);
        self
    }

    /// Project the response.
    #[must_use]
    pub fn with_projection(mut self, expression: impl Into<String>) -> Self {
        self.projection_expression = Some(expression.into());
        self
    }

    /// Attach expression-attribute names.
    #[must_use]
    pub fn with_names(mut self, names: HashMap<String, String>) -> Self {
        self.expression_attribute_names = Some(names);
        self
    }

    /// Attach expression-attribute values.
    #[must_use]
    pub fn with_values(mut self, values: HashMap<String, AttributeValue>) -> Self {
        self.expression_attribute_values = Some(values);
        self
    }

    /// Select counts or specific attributes.
    #[must_use]
    pub fn with_select(mut self, select: Select) -> Self {
        self.select = select;
        self
    }
}

/// `Query` output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryOutput {
    /// Matching items, in key order.
    pub items: Vec<Item>,
    /// Number of items returned.
    pub count: usize,
    /// Number of items evaluated before the filter.
    pub scanned_count: usize,
    /// Cursor for the next page, when the limit stopped the read early.
    pub last_evaluated_key: Option<Item>,
}

/// `Scan` input.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ScanRequest {
    /// Table to scan.
    pub table_name: String,
    /// Index to scan instead of the table.
    pub index_name: Option<String>,
    /// Optional filter.
    pub filter_expression: Option<String>,
    /// `#alias` → attribute name.
    pub expression_attribute_names: Option<HashMap<String, String>>,
    /// `:placeholder` → value.
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    /// Optional projection expression.
    pub projection_expression: Option<String>,
    /// Maximum number of items to evaluate.
    pub limit: Option<u32>,
    /// Continue after this key.
    pub exclusive_start_key: Option<Item>,
    /// Which parallel segment to scan.
    pub segment: Option<u32>,
    /// How many segments the scan is split into.
    pub total_segments: Option<u32>,
    /// Accepted and ignored: the mock is always strongly consistent.
    pub consistent_read: bool,
    /// What to return.
    pub select: Select,
}

impl ScanRequest {
    /// A scan of `table_name`.
    #[must_use]
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            ..Self::default()
        }
    }

    /// Apply a filter.
    #[must_use]
    pub fn with_filter(mut self, expression: impl Into<String>) -> Self {
        self.filter_expression = Some(expression.into());
        self
    }

    /// Cap the number of evaluated items.
    #[must_use]
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Continue after `key`.
    #[must_use]
    pub fn with_exclusive_start_key(mut self, key: Item) -> Self {
        self.exclusive_start_key = Some(key);
        self
    }

    /// Scan one segment of a parallel scan.
    #[must_use]
    pub fn with_segment(mut self, segment: u32, total_segments: u32) -> Self {
        self.segment = Some(segment);
        self.total_segments = Some(total_segments);
        self
    }

    /// Scan a secondary index.
    #[must_use]
    pub fn with_index(mut self, index_name: impl Into<String>) -> Self {
        self.index_name = Some(index_name.into());
        self
    }

    /// Project the response.
    #[must_use]
    pub fn with_projection(mut self, expression: impl Into<String>) -> Self {
        self.projection_expression = Some(expression.into());
        self
    }

    /// Attach expression-attribute names.
    #[must_use]
    pub fn with_names(mut self, names: HashMap<String, String>) -> Self {
        self.expression_attribute_names = Some(names);
        self
    }

    /// Attach expression-attribute values.
    #[must_use]
    pub fn with_values(mut self, values: HashMap<String, AttributeValue>) -> Self {
        self.expression_attribute_values = Some(values);
        self
    }

    /// Select counts or specific attributes.
    #[must_use]
    pub fn with_select(mut self, select: Select) -> Self {
        self.select = select;
        self
    }
}

/// `Scan` output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ScanOutput {
    /// Matching items.
    pub items: Vec<Item>,
    /// Number of items returned.
    pub count: usize,
    /// Number of items evaluated before the filter.
    pub scanned_count: usize,
    /// Cursor for the next page, when the limit stopped the scan early.
    pub last_evaluated_key: Option<Item>,
}

/// One write in a `BatchWriteItem` request.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum WriteRequest {
    /// Write (or overwrite) an item.
    Put {
        /// The item to write.
        item: Item,
    },
    /// Delete an item by key.
    Delete {
        /// The key to delete.
        key: Item,
    },
}

/// `BatchWriteItem` input.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BatchWriteItemRequest {
    /// Table name → writes. At most 25 writes in total.
    pub request_items: BTreeMap<String, Vec<WriteRequest>>,
}

impl BatchWriteItemRequest {
    /// An empty request, to be filled with [`add`](Self::add).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a write for `table_name`.
    #[must_use]
    pub fn add(mut self, table_name: impl Into<String>, write: WriteRequest) -> Self {
        self.request_items
            .entry(table_name.into())
            .or_default()
            .push(write);
        self
    }
}

/// `BatchWriteItem` output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BatchWriteItemOutput {
    /// Writes the service did not process. Always empty in the mock.
    pub unprocessed_items: BTreeMap<String, Vec<WriteRequest>>,
}

/// The keys and options for one table in a `BatchGetItem` request.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct KeysAndAttributes {
    /// Keys to read.
    pub keys: Vec<Item>,
    /// Optional projection expression.
    pub projection_expression: Option<String>,
    /// `#alias` → attribute name.
    pub expression_attribute_names: Option<HashMap<String, String>>,
    /// Accepted and ignored: the mock is always strongly consistent.
    pub consistent_read: bool,
}

impl KeysAndAttributes {
    /// Keys with no projection.
    #[must_use]
    pub fn new(keys: Vec<Item>) -> Self {
        Self {
            keys,
            ..Self::default()
        }
    }

    /// Project each returned item.
    #[must_use]
    pub fn with_projection(mut self, expression: impl Into<String>) -> Self {
        self.projection_expression = Some(expression.into());
        self
    }
}

/// `BatchGetItem` input.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BatchGetItemRequest {
    /// Table name → keys. At most 100 keys in total.
    pub request_items: BTreeMap<String, KeysAndAttributes>,
}

impl BatchGetItemRequest {
    /// An empty request, to be filled with [`add`](Self::add).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add keys for one table.
    #[must_use]
    pub fn add(mut self, table_name: impl Into<String>, keys: KeysAndAttributes) -> Self {
        self.request_items.insert(table_name.into(), keys);
        self
    }
}

/// `BatchGetItem` output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BatchGetItemOutput {
    /// Table name → found items, in request order. Missing keys are omitted.
    pub responses: BTreeMap<String, Vec<Item>>,
    /// Keys the service did not process. Always empty in the mock.
    pub unprocessed_keys: BTreeMap<String, KeysAndAttributes>,
}

/// The `Put` action of a transaction.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TransactPut {
    /// Table to write to.
    pub table_name: String,
    /// The item to write.
    pub item: Item,
    /// Optional condition.
    pub condition_expression: Option<String>,
    /// `#alias` → attribute name.
    pub expression_attribute_names: Option<HashMap<String, String>>,
    /// `:placeholder` → value.
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    /// Include the existing item in the cancellation reason.
    pub return_values_on_condition_check_failure: bool,
}

/// The `Update` action of a transaction.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TransactUpdate {
    /// Table to update.
    pub table_name: String,
    /// The key to update.
    pub key: Item,
    /// The update expression.
    pub update_expression: String,
    /// Optional condition.
    pub condition_expression: Option<String>,
    /// `#alias` → attribute name.
    pub expression_attribute_names: Option<HashMap<String, String>>,
    /// `:placeholder` → value.
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    /// Include the existing item in the cancellation reason.
    pub return_values_on_condition_check_failure: bool,
}

/// The `Delete` action of a transaction.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TransactDelete {
    /// Table to delete from.
    pub table_name: String,
    /// The key to delete.
    pub key: Item,
    /// Optional condition.
    pub condition_expression: Option<String>,
    /// `#alias` → attribute name.
    pub expression_attribute_names: Option<HashMap<String, String>>,
    /// `:placeholder` → value.
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    /// Include the existing item in the cancellation reason.
    pub return_values_on_condition_check_failure: bool,
}

/// The `ConditionCheck` action of a transaction: assert something, write
/// nothing.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TransactConditionCheck {
    /// Table the item lives in.
    pub table_name: String,
    /// The key to check.
    pub key: Item,
    /// The condition, which must be present.
    pub condition_expression: String,
    /// `#alias` → attribute name.
    pub expression_attribute_names: Option<HashMap<String, String>>,
    /// `:placeholder` → value.
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    /// Include the existing item in the cancellation reason.
    pub return_values_on_condition_check_failure: bool,
}

/// One action of a transaction. Exactly one variant per member.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum TransactWriteItem {
    /// Write an item.
    Put(TransactPut),
    /// Update an item.
    Update(TransactUpdate),
    /// Delete an item.
    Delete(TransactDelete),
    /// Assert a condition without writing.
    ConditionCheck(TransactConditionCheck),
}

impl TransactWriteItem {
    /// A `Put` action.
    #[must_use]
    pub fn put(table_name: impl Into<String>, item: Item) -> Self {
        Self::Put(TransactPut {
            table_name: table_name.into(),
            item,
            ..TransactPut::default()
        })
    }

    /// An `Update` action.
    #[must_use]
    pub fn update(table_name: impl Into<String>, key: Item, expression: impl Into<String>) -> Self {
        Self::Update(TransactUpdate {
            table_name: table_name.into(),
            key,
            update_expression: expression.into(),
            ..TransactUpdate::default()
        })
    }

    /// A `Delete` action.
    #[must_use]
    pub fn delete(table_name: impl Into<String>, key: Item) -> Self {
        Self::Delete(TransactDelete {
            table_name: table_name.into(),
            key,
            ..TransactDelete::default()
        })
    }

    /// A `ConditionCheck` action.
    #[must_use]
    pub fn condition_check(
        table_name: impl Into<String>,
        key: Item,
        expression: impl Into<String>,
    ) -> Self {
        Self::ConditionCheck(TransactConditionCheck {
            table_name: table_name.into(),
            key,
            condition_expression: expression.into(),
            ..TransactConditionCheck::default()
        })
    }

    /// The table this action targets.
    #[must_use]
    pub fn table_name(&self) -> &str {
        match self {
            Self::Put(action) => &action.table_name,
            Self::Update(action) => &action.table_name,
            Self::Delete(action) => &action.table_name,
            Self::ConditionCheck(action) => &action.table_name,
        }
    }

    /// Attach expression-attribute names to whichever action this is.
    #[must_use]
    pub fn with_names(mut self, names: HashMap<String, String>) -> Self {
        match &mut self {
            Self::Put(action) => action.expression_attribute_names = Some(names),
            Self::Update(action) => action.expression_attribute_names = Some(names),
            Self::Delete(action) => action.expression_attribute_names = Some(names),
            Self::ConditionCheck(action) => action.expression_attribute_names = Some(names),
        }
        self
    }

    /// Attach expression-attribute values to whichever action this is.
    #[must_use]
    pub fn with_values(mut self, values: HashMap<String, AttributeValue>) -> Self {
        match &mut self {
            Self::Put(action) => action.expression_attribute_values = Some(values),
            Self::Update(action) => action.expression_attribute_values = Some(values),
            Self::Delete(action) => action.expression_attribute_values = Some(values),
            Self::ConditionCheck(action) => action.expression_attribute_values = Some(values),
        }
        self
    }

    /// Attach a condition to whichever action this is.
    #[must_use]
    pub fn with_condition(mut self, condition: impl Into<String>) -> Self {
        match &mut self {
            Self::Put(action) => action.condition_expression = Some(condition.into()),
            Self::Update(action) => action.condition_expression = Some(condition.into()),
            Self::Delete(action) => action.condition_expression = Some(condition.into()),
            Self::ConditionCheck(action) => action.condition_expression = condition.into(),
        }
        self
    }
}

/// `TransactWriteItems` input.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TransactWriteItemsRequest {
    /// The actions. At most 100, at most one per item.
    pub transact_items: Vec<TransactWriteItem>,
    /// Idempotency token: repeating a request with the same token is a no-op.
    pub client_request_token: Option<String>,
}

impl TransactWriteItemsRequest {
    /// A transaction over `transact_items`.
    #[must_use]
    pub fn new(transact_items: Vec<TransactWriteItem>) -> Self {
        Self {
            transact_items,
            client_request_token: None,
        }
    }

    /// Make the transaction idempotent.
    #[must_use]
    pub fn with_client_request_token(mut self, token: impl Into<String>) -> Self {
        self.client_request_token = Some(token.into());
        self
    }
}

/// One `Get` action of a transaction.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TransactGet {
    /// Table to read.
    pub table_name: String,
    /// The key to read.
    pub key: Item,
    /// Optional projection expression.
    pub projection_expression: Option<String>,
    /// `#alias` → attribute name.
    pub expression_attribute_names: Option<HashMap<String, String>>,
}

impl TransactGet {
    /// A read of `key` from `table_name`.
    #[must_use]
    pub fn new(table_name: impl Into<String>, key: Item) -> Self {
        Self {
            table_name: table_name.into(),
            key,
            ..Self::default()
        }
    }
}

/// `TransactGetItems` input.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TransactGetItemsRequest {
    /// The reads. At most 100.
    pub transact_items: Vec<TransactGet>,
}

impl TransactGetItemsRequest {
    /// A transaction over `transact_items`.
    #[must_use]
    pub fn new(transact_items: Vec<TransactGet>) -> Self {
        Self { transact_items }
    }
}

/// `TransactGetItems` output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TransactGetItemsOutput {
    /// One entry per request, in order. `None` when the key was absent.
    pub responses: Vec<Option<Item>>,
}

/// `UpdateTimeToLive` input.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UpdateTimeToLiveRequest {
    /// Table to configure.
    pub table_name: String,
    /// Attribute holding the expiry, as epoch seconds.
    pub attribute_name: String,
    /// Whether TTL is on.
    pub enabled: bool,
}

impl UpdateTimeToLiveRequest {
    /// Enable or disable TTL for `attribute_name`.
    #[must_use]
    pub fn new(
        table_name: impl Into<String>,
        attribute_name: impl Into<String>,
        enabled: bool,
    ) -> Self {
        Self {
            table_name: table_name.into(),
            attribute_name: attribute_name.into(),
            enabled,
        }
    }
}

/// Whether TTL is on for a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TimeToLiveStatus {
    /// TTL is on.
    Enabled,
    /// TTL is off.
    Disabled,
}

impl TimeToLiveStatus {
    /// The string the service reports.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "ENABLED",
            Self::Disabled => "DISABLED",
        }
    }
}

/// What `describe_time_to_live` reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeToLiveDescription {
    /// Whether TTL is on.
    pub status: TimeToLiveStatus,
    /// The TTL attribute, when one is configured.
    pub attribute_name: Option<String>,
}

/// `ListTables` input.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ListTablesRequest {
    /// Continue after this table name.
    pub exclusive_start_table_name: Option<String>,
    /// Maximum number of names to return.
    pub limit: Option<u32>,
}

/// `ListTables` output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ListTablesOutput {
    /// Table names, sorted.
    pub table_names: Vec<String>,
    /// Cursor for the next page, when the limit stopped the listing early.
    pub last_evaluated_table_name: Option<String>,
}

/// `DescribeTable` output, re-exported here for discoverability.
pub type DescribeTableOutput = TableDescription;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_actions_carry_their_expressions() {
        let names = names([("#v", "value")]);
        let attribute_values = values([(":v", crate::s("x"))]);

        let put = TransactWriteItem::put("t", item([("pk", crate::s("a"))]))
            .with_names(names.clone())
            .with_values(attribute_values.clone())
            .with_condition("attribute_not_exists(pk)");
        match put {
            TransactWriteItem::Put(action) => {
                assert_eq!(action.table_name, "t");
                assert!(action.expression_attribute_names.is_some());
                assert!(action.expression_attribute_values.is_some());
                assert_eq!(
                    action.condition_expression.as_deref(),
                    Some("attribute_not_exists(pk)")
                );
            }
            other => panic!("expected a put, got {other:?}"),
        }

        let delete = TransactWriteItem::delete("t", item([("pk", crate::s("a"))]))
            .with_names(names.clone())
            .with_values(attribute_values.clone())
            .with_condition("attribute_exists(pk)");
        match delete {
            TransactWriteItem::Delete(action) => {
                assert_eq!(action.table_name, "t");
                assert!(action.expression_attribute_names.is_some());
                assert!(action.expression_attribute_values.is_some());
            }
            other => panic!("expected a delete, got {other:?}"),
        }

        let update = TransactWriteItem::update("t", item([("pk", crate::s("a"))]), "SET v = :v")
            .with_names(names)
            .with_values(attribute_values)
            .with_condition("attribute_exists(pk)");
        match update {
            TransactWriteItem::Update(action) => {
                assert_eq!(action.table_name, "t");
                assert!(action.expression_attribute_values.is_some());
            }
            other => panic!("expected an update, got {other:?}"),
        }

        let check = TransactWriteItem::condition_check(
            "t",
            item([("pk", crate::s("a"))]),
            "attribute_exists(pk)",
        )
        .with_condition("attribute_exists(pk)")
        .with_names(HashMap::new())
        .with_values(HashMap::new());
        match check {
            TransactWriteItem::ConditionCheck(action) => {
                assert_eq!(action.table_name, "t");
                assert_eq!(action.condition_expression, "attribute_exists(pk)");
            }
            other => panic!("expected a condition check, got {other:?}"),
        }
    }
}
