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

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};

use aws_sdk_dynamodb::types::AttributeValue;

use crate::capacity::{
    ConsumedCapacity, ItemCollectionMetrics, ReturnConsumedCapacity, ReturnItemCollectionMetrics,
};
use crate::schema::TableDescription;
use crate::{Error, Item, Result};

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
    /// Whether to report the capacity the write consumed.
    pub return_consumed_capacity: ReturnConsumedCapacity,
    /// Whether to report the item collection the write touched.
    pub return_item_collection_metrics: ReturnItemCollectionMetrics,
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

    /// Choose what the response reports about consumed capacity.
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }

    /// Choose whether the response reports the written item's collection size.
    #[must_use]
    pub fn with_return_item_collection_metrics(
        mut self,
        mode: ReturnItemCollectionMetrics,
    ) -> Self {
        self.return_item_collection_metrics = mode;
        self
    }
}

/// `PutItem` output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PutItemOutput {
    /// The previous item, when `ReturnValues::AllOld` was requested.
    pub attributes: Option<Item>,
    /// The capacity the write consumed, when requested.
    pub consumed_capacity: Option<ConsumedCapacity>,
    /// The written item collection, when requested and the table has a local
    /// secondary index.
    pub item_collection_metrics: Option<ItemCollectionMetrics>,
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
    /// Whether to report the capacity the read consumed.
    pub return_consumed_capacity: ReturnConsumedCapacity,
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

    /// Choose what the response reports about consumed capacity.
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }
}

/// `GetItem` output. `item` is `None` when the key is absent or expired.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GetItemOutput {
    /// The item, if it exists.
    pub item: Option<Item>,
    /// The capacity the read consumed, when requested.
    pub consumed_capacity: Option<ConsumedCapacity>,
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
    /// Whether to report the capacity the write consumed.
    pub return_consumed_capacity: ReturnConsumedCapacity,
    /// Whether to report the item collection the write touched.
    pub return_item_collection_metrics: ReturnItemCollectionMetrics,
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

    /// Choose what the response reports about consumed capacity.
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }

    /// Choose whether the response reports the written item's collection size.
    #[must_use]
    pub fn with_return_item_collection_metrics(
        mut self,
        mode: ReturnItemCollectionMetrics,
    ) -> Self {
        self.return_item_collection_metrics = mode;
        self
    }
}

/// `DeleteItem` output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DeleteItemOutput {
    /// The deleted item, when `ReturnValues::AllOld` was requested.
    pub attributes: Option<Item>,
    /// The capacity the write consumed, when requested.
    pub consumed_capacity: Option<ConsumedCapacity>,
    /// The touched item collection, when requested and the table has a local
    /// secondary index.
    pub item_collection_metrics: Option<ItemCollectionMetrics>,
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
    /// Whether to report the capacity the write consumed.
    pub return_consumed_capacity: ReturnConsumedCapacity,
    /// Whether to report the item collection the write touched.
    pub return_item_collection_metrics: ReturnItemCollectionMetrics,
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

    /// Choose what the response reports about consumed capacity.
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }

    /// Choose whether the response reports the written item's collection size.
    #[must_use]
    pub fn with_return_item_collection_metrics(
        mut self,
        mode: ReturnItemCollectionMetrics,
    ) -> Self {
        self.return_item_collection_metrics = mode;
        self
    }
}

/// `UpdateItem` output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UpdateItemOutput {
    /// The attributes selected by `return_values`.
    pub attributes: Option<Item>,
    /// The capacity the write consumed, when requested.
    pub consumed_capacity: Option<ConsumedCapacity>,
    /// The touched item collection, when requested and the table has a local
    /// secondary index.
    pub item_collection_metrics: Option<ItemCollectionMetrics>,
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
    /// The key condition, required exactly as the service requires it: a
    /// `Query` without one is rejected with a `ValidationException`, and
    /// `scan` is the way to read a whole table.
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
    /// Whether to report the capacity the read consumed.
    pub return_consumed_capacity: ReturnConsumedCapacity,
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

    /// Choose what the response reports about consumed capacity.
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
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
    /// The capacity the read consumed, when requested.
    pub consumed_capacity: Option<ConsumedCapacity>,
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
    /// Whether to report the capacity the read consumed.
    pub return_consumed_capacity: ReturnConsumedCapacity,
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

    /// Choose what the response reports about consumed capacity.
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
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
    /// The capacity the read consumed, when requested.
    pub consumed_capacity: Option<ConsumedCapacity>,
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
    /// Whether to report the capacity each table consumed.
    pub return_consumed_capacity: ReturnConsumedCapacity,
    /// Whether to report the item collections the writes touched.
    pub return_item_collection_metrics: ReturnItemCollectionMetrics,
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

    /// Choose what the response reports about consumed capacity.
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }

    /// Choose whether the response reports the touched item collections.
    #[must_use]
    pub fn with_return_item_collection_metrics(
        mut self,
        mode: ReturnItemCollectionMetrics,
    ) -> Self {
        self.return_item_collection_metrics = mode;
        self
    }
}

/// `BatchWriteItem` output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BatchWriteItemOutput {
    /// Writes the call did not process. Empty unless the mock was throttled
    /// with [`MockDdb::throttle_next_batch_write`](crate::MockDdb::throttle_next_batch_write).
    pub unprocessed_items: BTreeMap<String, Vec<WriteRequest>>,
    /// One entry per table the batch wrote to, when requested.
    pub consumed_capacity: Vec<ConsumedCapacity>,
    /// Table name → the collection its writes touched, when requested.
    pub item_collection_metrics: BTreeMap<String, ItemCollectionMetrics>,
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

/// Merge the alias map a second [`BatchGetItemRequest::add`] brings for a
/// table into the one the first call already sent.
///
/// The lookup options belong to the table, not to the call, so a second `add`
/// may only widen them: aliases it repeats have to mean the same attribute, and
/// any alias it adds is carried into the merged map. Returns the map the merged
/// entry keeps.
fn merged_aliases(
    table_name: &str,
    existing: &KeysAndAttributes,
    added: &KeysAndAttributes,
) -> Result<Option<HashMap<String, String>>> {
    let Some(added_names) = added.expression_attribute_names.as_ref() else {
        return Ok(existing.expression_attribute_names.clone());
    };
    let mut merged = existing
        .expression_attribute_names
        .clone()
        .unwrap_or_default();
    for (alias, name) in added_names {
        if let Some(previous) = merged.get(alias) {
            if previous != name {
                return Err(Error::validation(format!(
                    "1 validation error detected: Value at 'requestItems.{table_name}.expressionAttributeNames' failed to satisfy constraint: the alias `{alias}` names both `{previous}` and `{name}`"
                )));
            }
        }
        merged.insert(alias.clone(), name.clone());
    }
    Ok(Some(merged))
}

/// `BatchGetItem` input.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BatchGetItemRequest {
    /// Table name → keys. At most 100 keys in total.
    pub request_items: BTreeMap<String, KeysAndAttributes>,
    /// Whether to report the capacity each table consumed.
    pub return_consumed_capacity: ReturnConsumedCapacity,
}

impl BatchGetItemRequest {
    /// An empty request, to be filled with [`add`](Self::add).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append keys for one table.
    ///
    /// Two calls for the same table add up: the keys of the second call follow
    /// the keys of the first instead of replacing them, the way
    /// [`BatchWriteItemRequest::add`] appends its writes. A batch built in
    /// steps therefore reads every key it names; before, the second call
    /// overwrote the first one's `KeysAndAttributes` and the request silently
    /// shrank — a partly built batch that still passed the 1–100 key check.
    ///
    /// The lookup options belong to the table rather than to the call, so the
    /// first call sets them and a second call has to agree: a different
    /// `projection_expression`, or an alias that already names another
    /// attribute, is a `ValidationException` instead of a silent override.
    /// Aliases that do not overlap are merged, and `consistent_read` may
    /// differ because the mock is always strongly consistent.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] when this call's lookup options contradict
    /// the ones an earlier call already sent for the same table: a different
    /// `projection_expression` (including one call naming a projection and the
    /// other none), or an alias that already maps to another attribute. The
    /// keys are never silently dropped, so refusing the second call is the only
    /// way to keep both calls' intent.
    ///
    /// ```
    /// use mock_ddb::{BatchGetItemRequest, KeysAndAttributes, item, s};
    ///
    /// let batch = BatchGetItemRequest::new()
    ///     .add("users", KeysAndAttributes::new(vec![item([("pk", s("u#1"))])]))
    ///     .unwrap()
    ///     .add("users", KeysAndAttributes::new(vec![item([("pk", s("u#2"))])]))
    ///     .unwrap();
    /// assert_eq!(batch.request_items["users"].keys.len(), 2);
    ///
    /// let conflict = BatchGetItemRequest::new()
    ///     .add("users", KeysAndAttributes::new(vec![]).with_projection("pk"))
    ///     .unwrap()
    ///     .add(
    ///         "users",
    ///         KeysAndAttributes::new(vec![]).with_projection("pk, email"),
    ///     );
    /// assert_eq!(conflict.unwrap_err().code(), "ValidationException");
    /// ```
    pub fn add(mut self, table_name: impl Into<String>, keys: KeysAndAttributes) -> Result<Self> {
        match self.request_items.entry(table_name.into()) {
            Entry::Vacant(slot) => {
                slot.insert(keys);
            }
            Entry::Occupied(mut slot) => {
                let table_name = slot.key().clone();
                let existing = slot.get_mut();
                if existing.projection_expression != keys.projection_expression {
                    return Err(Error::validation(format!(
                        "1 validation error detected: Value at 'requestItems.{table_name}.projectionExpression' failed to satisfy constraint: two calls for one table have to name the same projection"
                    )));
                }
                let aliases = merged_aliases(&table_name, existing, &keys)?;
                existing.expression_attribute_names = aliases;
                existing.keys.extend(keys.keys);
                existing.consistent_read |= keys.consistent_read;
            }
        }
        Ok(self)
    }

    /// Choose what the response reports about consumed capacity.
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }
}

/// `BatchGetItem` output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BatchGetItemOutput {
    /// Table name → found items, in request order. Missing keys are omitted.
    pub responses: BTreeMap<String, Vec<Item>>,
    /// Keys the call did not process. Empty unless the mock was throttled with
    /// [`MockDdb::throttle_next_batch_get`](crate::MockDdb::throttle_next_batch_get).
    pub unprocessed_keys: BTreeMap<String, KeysAndAttributes>,
    /// One entry per table the batch read from, when requested.
    pub consumed_capacity: Vec<ConsumedCapacity>,
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
    /// Whether to report the capacity each table consumed.
    pub return_consumed_capacity: ReturnConsumedCapacity,
    /// Whether to report the item collections the writes touched.
    pub return_item_collection_metrics: ReturnItemCollectionMetrics,
}

impl TransactWriteItemsRequest {
    /// A transaction over `transact_items`.
    #[must_use]
    pub fn new(transact_items: Vec<TransactWriteItem>) -> Self {
        Self {
            transact_items,
            client_request_token: None,
            ..Self::default()
        }
    }

    /// Make the transaction idempotent.
    #[must_use]
    pub fn with_client_request_token(mut self, token: impl Into<String>) -> Self {
        self.client_request_token = Some(token.into());
        self
    }

    /// Choose what the response reports about consumed capacity.
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }

    /// Choose whether the response reports the touched item collections.
    #[must_use]
    pub fn with_return_item_collection_metrics(
        mut self,
        mode: ReturnItemCollectionMetrics,
    ) -> Self {
        self.return_item_collection_metrics = mode;
        self
    }
}

/// `TransactWriteItems` output.
///
/// A transaction writes to several tables at once, so the capacity is one entry
/// per table, in table-name order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TransactWriteItemsOutput {
    /// One entry per table the transaction wrote to, when requested.
    pub consumed_capacity: Vec<ConsumedCapacity>,
    /// Table name → the collection its writes touched, when requested.
    pub item_collection_metrics: BTreeMap<String, ItemCollectionMetrics>,
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
    /// Whether to report the capacity each table consumed.
    pub return_consumed_capacity: ReturnConsumedCapacity,
}

impl TransactGetItemsRequest {
    /// A transaction over `transact_items`.
    #[must_use]
    pub fn new(transact_items: Vec<TransactGet>) -> Self {
        Self {
            transact_items,
            ..Self::default()
        }
    }

    /// Choose what the response reports about consumed capacity.
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }
}

/// `TransactGetItems` output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TransactGetItemsOutput {
    /// One entry per request, in order. `None` when the key was absent.
    pub responses: Vec<Option<Item>>,
    /// One entry per table the transaction read from, when requested.
    pub consumed_capacity: Vec<ConsumedCapacity>,
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

    /// One key for `name` in the table the batch tests use.
    fn batch_keys(name: &str) -> KeysAndAttributes {
        KeysAndAttributes::new(vec![item([("pk", crate::s(name))])])
    }

    /// The same `KeysAndAttributes` with an `#alias` → attribute pair.
    fn with_aliases(mut keys: KeysAndAttributes, pairs: &[(&str, &str)]) -> KeysAndAttributes {
        let map = keys
            .expression_attribute_names
            .get_or_insert_with(HashMap::new);
        for (alias, name) in pairs {
            map.insert((*alias).to_string(), (*name).to_string());
        }
        keys
    }

    #[test]
    fn two_adds_for_one_table_append_their_keys() {
        let request = BatchGetItemRequest::new()
            .add("t", batch_keys("a"))
            .unwrap()
            .add("t", batch_keys("b"))
            .unwrap()
            .add("t", batch_keys("c"))
            .unwrap();

        // One entry, three keys: the second and third `add` extend instead of
        // replacing the first one's `KeysAndAttributes`.
        assert_eq!(request.request_items.len(), 1);
        assert_eq!(request.request_items["t"].keys.len(), 3);
        assert_eq!(request.request_items["t"].keys[0]["pk"], crate::s("a"));
        assert_eq!(request.request_items["t"].keys[2]["pk"], crate::s("c"));
    }

    #[test]
    fn a_second_add_may_repeat_the_projection_but_not_change_it() {
        let repeated = BatchGetItemRequest::new()
            .add("t", batch_keys("a").with_projection("pk"))
            .unwrap()
            .add("t", batch_keys("b").with_projection("pk"))
            .unwrap();
        assert_eq!(repeated.request_items["t"].keys.len(), 2);
        assert_eq!(
            repeated.request_items["t"].projection_expression.as_deref(),
            Some("pk")
        );

        // None against Some is as much a conflict as two different
        // expressions: one of the two calls would have had its projection
        // dropped.
        let first_none = BatchGetItemRequest::new()
            .add("t", batch_keys("a"))
            .unwrap()
            .add("t", batch_keys("b").with_projection("pk"))
            .unwrap_err();
        assert_eq!(first_none.code(), "ValidationException");
        assert!(first_none.to_string().contains("projectionExpression"));

        let second_none = BatchGetItemRequest::new()
            .add("t", batch_keys("a").with_projection("pk"))
            .unwrap()
            .add("t", batch_keys("b"))
            .unwrap_err();
        assert_eq!(second_none.code(), "ValidationException");

        let different = BatchGetItemRequest::new()
            .add("t", batch_keys("a").with_projection("pk"))
            .unwrap()
            .add("t", batch_keys("b").with_projection("pk, email"))
            .unwrap_err();
        assert_eq!(different.code(), "ValidationException");
    }

    #[test]
    fn a_second_add_merges_aliases_it_does_not_repeat() {
        let disjoint = BatchGetItemRequest::new()
            .add(
                "t",
                with_aliases(batch_keys("a").with_projection("#n"), &[("#n", "note")]),
            )
            .unwrap()
            .add(
                "t",
                with_aliases(
                    batch_keys("b").with_projection("#n"),
                    &[("#n", "note"), ("#t", "title")],
                ),
            )
            .unwrap();
        let aliases = disjoint.request_items["t"]
            .expression_attribute_names
            .as_ref()
            .expect("the merged entry keeps the alias map");
        assert_eq!(aliases.len(), 2);
        assert_eq!(aliases["#t"], "title");
        assert_eq!(disjoint.request_items["t"].keys.len(), 2);
    }

    #[test]
    fn a_second_add_refuses_an_alias_that_already_names_another_attribute() {
        let conflict = BatchGetItemRequest::new()
            .add(
                "t",
                with_aliases(batch_keys("a").with_projection("#n"), &[("#n", "note")]),
            )
            .unwrap()
            .add(
                "t",
                with_aliases(batch_keys("b").with_projection("#n"), &[("#n", "title")]),
            )
            .unwrap_err();
        assert_eq!(conflict.code(), "ValidationException");
        assert!(conflict.to_string().contains("expressionAttributeNames"));
    }

    #[test]
    fn a_second_add_keeps_a_consistent_read_the_first_one_asked_for() {
        let mut first = batch_keys("a");
        first.consistent_read = true;
        let mut second = batch_keys("b");
        second.consistent_read = false;

        let request = BatchGetItemRequest::new()
            .add("t", first)
            .unwrap()
            .add("t", second)
            .unwrap();
        assert!(request.request_items["t"].consistent_read);
        assert_eq!(request.request_items["t"].keys.len(), 2);
    }

    #[test]
    fn add_still_refuses_nothing_for_a_fresh_table() {
        let request = BatchGetItemRequest::new()
            .add("t", KeysAndAttributes::new(Vec::new()))
            .unwrap();
        assert!(request.request_items["t"].keys.is_empty());
    }

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
