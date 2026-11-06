//! Request and response types, shaped like the AWS SDK's.
//!
//! Every field keeps the SDK's name (`condition_expression`,
//! `expression_attribute_values`, `scan_index_forward`, …) and the item type is
//! the SDK's own `HashMap<String, AttributeValue>`, so moving a test from the
//! real client to the mock is a matter of calling `mock.put_item(request)`
//! instead of `client.put_item().set_…().send()`.
//!
//! Responses are `#[non_exhaustive]`: every field is public and readable, but a
//! caller builds one with `Default` and field assignment, not with a struct
//! literal. A response field the service adds later — another capacity
//! breakdown, another cursor — is therefore a minor release instead of a
//! breaking one for every test that spelled the literal out.
//!
//! ```
//! use mock_ddb::QueryOutput;
//!
//! let mut output = QueryOutput::default();
//! output.count = 3;
//! assert_eq!(output.count, 3);
//! assert!(output.consumed_capacity.is_none());
//! ```
//!
//! The literal form is rejected, which is what makes the guarantee above hold:
//!
//! ```compile_fail
//! use mock_ddb::QueryOutput;
//!
//! let output = QueryOutput {
//!     items: Vec::new(),
//!     count: 0,
//!     scanned_count: 0,
//!     last_evaluated_key: None,
//!     consumed_capacity: None,
//! };
//! ```
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
    ///
    /// ```
    /// use mock_ddb::{item, s, PutItemRequest};
    /// let request = PutItemRequest::new("users", item([("pk", s("u#1"))]));
    /// assert_eq!(request.table_name, "users");
    /// ```
    #[must_use]
    pub fn new(table_name: impl Into<String>, item: Item) -> Self {
        Self {
            table_name: table_name.into(),
            item,
            ..Self::default()
        }
    }

    /// Require `expression` to hold before writing.
    ///
    /// ```
    /// use mock_ddb::{item, s, PutItemRequest};
    /// let request =
    ///     PutItemRequest::new("users", item([("pk", s("u#1"))])).with_condition("attribute_exists(pk)");
    /// assert_eq!(request.condition_expression.as_deref(), Some("attribute_exists(pk)"));
    /// ```
    #[must_use]
    pub fn with_condition(mut self, expression: impl Into<String>) -> Self {
        self.condition_expression = Some(expression.into());
        self
    }

    /// Attach expression-attribute names.
    ///
    /// ```
    /// use mock_ddb::{item, names, s, PutItemRequest};
    /// let request =
    ///     PutItemRequest::new("users", item([("pk", s("u#1"))])).with_names(names([("#n", "name")]));
    /// assert_eq!(request.expression_attribute_names.as_ref().unwrap()["#n"], "name");
    /// ```
    #[must_use]
    pub fn with_names(mut self, names: HashMap<String, String>) -> Self {
        self.expression_attribute_names = Some(names);
        self
    }

    /// Attach expression-attribute values.
    ///
    /// ```
    /// use mock_ddb::{item, s, values, PutItemRequest};
    /// let request =
    ///     PutItemRequest::new("users", item([("pk", s("u#1"))])).with_values(values([(":n", s("x"))]));
    /// assert_eq!(request.expression_attribute_values.as_ref().unwrap().len(), 1);
    /// ```
    #[must_use]
    pub fn with_values(mut self, values: HashMap<String, AttributeValue>) -> Self {
        self.expression_attribute_values = Some(values);
        self
    }

    /// Choose what the write returns.
    ///
    /// ```
    /// use mock_ddb::{item, s, PutItemRequest, ReturnValues};
    /// let request = PutItemRequest::new("users", item([("pk", s("u#1"))]))
    ///     .with_return_values(ReturnValues::AllOld);
    /// assert_eq!(request.return_values, ReturnValues::AllOld);
    /// ```
    #[must_use]
    pub fn with_return_values(mut self, return_values: ReturnValues) -> Self {
        self.return_values = return_values;
        self
    }

    /// Include the existing item in the failure when the condition does not
    /// hold.
    ///
    /// ```
    /// use mock_ddb::{item, s, PutItemRequest};
    /// let request = PutItemRequest::new("users", item([("pk", s("u#1"))]))
    ///     .with_return_values_on_condition_check_failure();
    /// assert!(request.return_values_on_condition_check_failure);
    /// ```
    #[must_use]
    pub fn with_return_values_on_condition_check_failure(mut self) -> Self {
        self.return_values_on_condition_check_failure = true;
        self
    }

    /// Choose what the response reports about consumed capacity.
    ///
    /// ```
    /// use mock_ddb::{item, s, PutItemRequest, ReturnConsumedCapacity};
    /// let request = PutItemRequest::new("users", item([("pk", s("u#1"))]))
    ///     .with_return_consumed_capacity(ReturnConsumedCapacity::Total);
    /// assert_eq!(request.return_consumed_capacity, ReturnConsumedCapacity::Total);
    /// ```
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }

    /// Choose whether the response reports the written item's collection size.
    ///
    /// ```
    /// use mock_ddb::{item, s, PutItemRequest, ReturnItemCollectionMetrics};
    /// let request = PutItemRequest::new("users", item([("pk", s("u#1"))]))
    ///     .with_return_item_collection_metrics(ReturnItemCollectionMetrics::Size);
    /// assert_eq!(
    ///     request.return_item_collection_metrics,
    ///     ReturnItemCollectionMetrics::Size
    /// );
    /// ```
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
///
/// ```
/// use mock_ddb::{item, s, PutItemOutput};
/// let mut output = PutItemOutput::default();
/// output.attributes = Some(item([("pk", s("u#1"))]));
/// assert_eq!(output.attributes.as_ref().unwrap().len(), 1);
/// assert!(output.consumed_capacity.is_none());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
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
///
/// ```
/// use mock_ddb::{item, s, GetItemRequest};
/// let request = GetItemRequest::new("users", item([("pk", s("u#1"))]));
/// assert_eq!(request.table_name, "users");
/// ```
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
    ///
    /// ```
    /// use mock_ddb::{item, s, GetItemRequest};
    /// let request = GetItemRequest::new("users", item([("pk", s("u#1"))]));
    /// assert_eq!(request.key.len(), 1);
    /// ```
    #[must_use]
    pub fn new(table_name: impl Into<String>, key: Item) -> Self {
        Self {
            table_name: table_name.into(),
            key,
            ..Self::default()
        }
    }

    /// Project the response down to `expression`.
    ///
    /// ```
    /// use mock_ddb::{item, s, GetItemRequest};
    /// let request =
    ///     GetItemRequest::new("users", item([("pk", s("u#1"))])).with_projection("#n, age");
    /// assert_eq!(request.projection_expression.as_deref(), Some("#n, age"));
    /// ```
    #[must_use]
    pub fn with_projection(mut self, expression: impl Into<String>) -> Self {
        self.projection_expression = Some(expression.into());
        self
    }

    /// Attach expression-attribute names for the projection.
    ///
    /// ```
    /// use mock_ddb::{item, names, s, GetItemRequest};
    /// let request =
    ///     GetItemRequest::new("users", item([("pk", s("u#1"))])).with_names(names([("#n", "name")]));
    /// assert_eq!(request.expression_attribute_names.as_ref().unwrap()["#n"], "name");
    /// ```
    #[must_use]
    pub fn with_names(mut self, names: HashMap<String, String>) -> Self {
        self.expression_attribute_names = Some(names);
        self
    }

    /// Choose what the response reports about consumed capacity.
    ///
    /// ```
    /// use mock_ddb::{item, s, GetItemRequest, ReturnConsumedCapacity};
    /// let request = GetItemRequest::new("users", item([("pk", s("u#1"))]))
    ///     .with_return_consumed_capacity(ReturnConsumedCapacity::Indexes);
    /// assert_eq!(request.return_consumed_capacity, ReturnConsumedCapacity::Indexes);
    /// ```
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }
}

/// `GetItem` output. `item` is `None` when the key is absent or expired.
///
/// ```
/// use mock_ddb::GetItemOutput;
/// let output = GetItemOutput::default();
/// assert!(output.item.is_none());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct GetItemOutput {
    /// The item, if it exists.
    pub item: Option<Item>,
    /// The capacity the read consumed, when requested.
    pub consumed_capacity: Option<ConsumedCapacity>,
}

/// `DeleteItem` input.
///
/// ```
/// use mock_ddb::{item, s, DeleteItemRequest};
/// let request = DeleteItemRequest::new("users", item([("pk", s("u#1"))]));
/// assert_eq!(request.key.len(), 1);
/// ```
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
    ///
    /// ```
    /// use mock_ddb::{item, s, DeleteItemRequest};
    /// let request = DeleteItemRequest::new("users", item([("pk", s("u#1"))]));
    /// assert_eq!(request.table_name, "users");
    /// ```
    #[must_use]
    pub fn new(table_name: impl Into<String>, key: Item) -> Self {
        Self {
            table_name: table_name.into(),
            key,
            ..Self::default()
        }
    }

    /// Require `expression` to hold before deleting.
    ///
    /// ```
    /// use mock_ddb::{item, s, DeleteItemRequest};
    /// let request = DeleteItemRequest::new("users", item([("pk", s("u#1"))]))
    ///     .with_condition("attribute_exists(pk)");
    /// assert_eq!(request.condition_expression.as_deref(), Some("attribute_exists(pk)"));
    /// ```
    #[must_use]
    pub fn with_condition(mut self, expression: impl Into<String>) -> Self {
        self.condition_expression = Some(expression.into());
        self
    }

    /// Attach expression-attribute names.
    ///
    /// ```
    /// use mock_ddb::{item, names, s, DeleteItemRequest};
    /// let request = DeleteItemRequest::new("users", item([("pk", s("u#1"))]))
    ///     .with_names(names([("#n", "name")]));
    /// assert_eq!(request.expression_attribute_names.as_ref().unwrap()["#n"], "name");
    /// ```
    #[must_use]
    pub fn with_names(mut self, names: HashMap<String, String>) -> Self {
        self.expression_attribute_names = Some(names);
        self
    }

    /// Attach expression-attribute values.
    ///
    /// ```
    /// use mock_ddb::{item, s, values, DeleteItemRequest};
    /// let request = DeleteItemRequest::new("users", item([("pk", s("u#1"))]))
    ///     .with_values(values([(":n", s("x"))]));
    /// assert_eq!(request.expression_attribute_values.as_ref().unwrap().len(), 1);
    /// ```
    #[must_use]
    pub fn with_values(mut self, values: HashMap<String, AttributeValue>) -> Self {
        self.expression_attribute_values = Some(values);
        self
    }

    /// Choose what the delete returns.
    ///
    /// ```
    /// use mock_ddb::{item, s, DeleteItemRequest, ReturnValues};
    /// let request = DeleteItemRequest::new("users", item([("pk", s("u#1"))]))
    ///     .with_return_values(ReturnValues::AllOld);
    /// assert_eq!(request.return_values, ReturnValues::AllOld);
    /// ```
    #[must_use]
    pub fn with_return_values(mut self, return_values: ReturnValues) -> Self {
        self.return_values = return_values;
        self
    }

    /// Include the existing item in the failure when the condition does not
    /// hold.
    ///
    /// ```
    /// use mock_ddb::{item, s, DeleteItemRequest};
    /// let request = DeleteItemRequest::new("users", item([("pk", s("u#1"))]))
    ///     .with_return_values_on_condition_check_failure();
    /// assert!(request.return_values_on_condition_check_failure);
    /// ```
    #[must_use]
    pub fn with_return_values_on_condition_check_failure(mut self) -> Self {
        self.return_values_on_condition_check_failure = true;
        self
    }

    /// Choose what the response reports about consumed capacity.
    ///
    /// ```
    /// use mock_ddb::{item, s, DeleteItemRequest, ReturnConsumedCapacity};
    /// let request = DeleteItemRequest::new("users", item([("pk", s("u#1"))]))
    ///     .with_return_consumed_capacity(ReturnConsumedCapacity::Total);
    /// assert_eq!(request.return_consumed_capacity, ReturnConsumedCapacity::Total);
    /// ```
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }

    /// Choose whether the response reports the written item's collection size.
    ///
    /// ```
    /// use mock_ddb::{item, s, DeleteItemRequest, ReturnItemCollectionMetrics};
    /// let request = DeleteItemRequest::new("users", item([("pk", s("u#1"))]))
    ///     .with_return_item_collection_metrics(ReturnItemCollectionMetrics::Size);
    /// assert_eq!(
    ///     request.return_item_collection_metrics,
    ///     ReturnItemCollectionMetrics::Size
    /// );
    /// ```
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
///
/// ```
/// use mock_ddb::DeleteItemOutput;
/// let output = DeleteItemOutput::default();
/// assert!(output.attributes.is_none());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
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
///
/// ```
/// use mock_ddb::{item, s, UpdateItemRequest};
/// let request = UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "SET age = :a");
/// assert_eq!(request.update_expression, "SET age = :a");
/// ```
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
    ///
    /// ```
    /// use mock_ddb::{item, s, UpdateItemRequest};
    /// let request = UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "SET age = :a");
    /// assert_eq!(request.update_expression, "SET age = :a");
    /// ```
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
    ///
    /// ```
    /// use mock_ddb::{item, s, UpdateItemRequest};
    /// let request = UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "SET age = :a")
    ///     .with_condition("attribute_exists(pk)");
    /// assert_eq!(request.condition_expression.as_deref(), Some("attribute_exists(pk)"));
    /// ```
    #[must_use]
    pub fn with_condition(mut self, expression: impl Into<String>) -> Self {
        self.condition_expression = Some(expression.into());
        self
    }

    /// Attach expression-attribute names.
    ///
    /// ```
    /// use mock_ddb::{item, names, s, UpdateItemRequest};
    /// let request = UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "SET #a = :a")
    ///     .with_names(names([("#a", "age")]));
    /// assert_eq!(request.expression_attribute_names.as_ref().unwrap()["#a"], "age");
    /// ```
    #[must_use]
    pub fn with_names(mut self, names: HashMap<String, String>) -> Self {
        self.expression_attribute_names = Some(names);
        self
    }

    /// Attach expression-attribute values.
    ///
    /// ```
    /// use mock_ddb::{item, s, values, UpdateItemRequest};
    /// let request = UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "SET age = :a")
    ///     .with_values(values([(":a", s("42"))]));
    /// assert_eq!(request.expression_attribute_values.as_ref().unwrap().len(), 1);
    /// ```
    #[must_use]
    pub fn with_values(mut self, values: HashMap<String, AttributeValue>) -> Self {
        self.expression_attribute_values = Some(values);
        self
    }

    /// Choose what the update returns.
    ///
    /// ```
    /// use mock_ddb::{item, s, ReturnValues, UpdateItemRequest};
    /// let request = UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "SET age = :a")
    ///     .with_return_values(ReturnValues::UpdatedNew);
    /// assert_eq!(request.return_values, ReturnValues::UpdatedNew);
    /// ```
    #[must_use]
    pub fn with_return_values(mut self, return_values: ReturnValues) -> Self {
        self.return_values = return_values;
        self
    }

    /// Include the existing item in the failure when the condition does not
    /// hold.
    ///
    /// ```
    /// use mock_ddb::{item, s, UpdateItemRequest};
    /// let request = UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "SET age = :a")
    ///     .with_return_values_on_condition_check_failure();
    /// assert!(request.return_values_on_condition_check_failure);
    /// ```
    #[must_use]
    pub fn with_return_values_on_condition_check_failure(mut self) -> Self {
        self.return_values_on_condition_check_failure = true;
        self
    }

    /// Choose what the response reports about consumed capacity.
    ///
    /// ```
    /// use mock_ddb::{item, s, ReturnConsumedCapacity, UpdateItemRequest};
    /// let request = UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "SET age = :a")
    ///     .with_return_consumed_capacity(ReturnConsumedCapacity::Total);
    /// assert_eq!(request.return_consumed_capacity, ReturnConsumedCapacity::Total);
    /// ```
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }

    /// Choose whether the response reports the written item's collection size.
    ///
    /// ```
    /// use mock_ddb::{item, s, ReturnItemCollectionMetrics, UpdateItemRequest};
    /// let request = UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "SET age = :a")
    ///     .with_return_item_collection_metrics(ReturnItemCollectionMetrics::Size);
    /// assert_eq!(
    ///     request.return_item_collection_metrics,
    ///     ReturnItemCollectionMetrics::Size
    /// );
    /// ```
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
///
/// ```
/// use mock_ddb::UpdateItemOutput;
/// let output = UpdateItemOutput::default();
/// assert!(output.attributes.is_none());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
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
    ///
    /// ```
    /// use mock_ddb::QueryRequest;
    /// let request = QueryRequest::new("users", "pk = :pk");
    /// assert_eq!(request.key_condition_expression.as_deref(), Some("pk = :pk"));
    /// ```
    #[must_use]
    pub fn new(table_name: impl Into<String>, key_condition: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            key_condition_expression: Some(key_condition.into()),
            ..Self::default()
        }
    }

    /// Query a secondary index.
    ///
    /// ```
    /// use mock_ddb::QueryRequest;
    /// let request = QueryRequest::new("users", "pk = :pk").with_index("by_email");
    /// assert_eq!(request.index_name.as_deref(), Some("by_email"));
    /// ```
    #[must_use]
    pub fn with_index(mut self, index_name: impl Into<String>) -> Self {
        self.index_name = Some(index_name.into());
        self
    }

    /// Apply a filter after the key condition.
    ///
    /// ```
    /// use mock_ddb::QueryRequest;
    /// let request = QueryRequest::new("users", "pk = :pk").with_filter("age > :a");
    /// assert_eq!(request.filter_expression.as_deref(), Some("age > :a"));
    /// ```
    #[must_use]
    pub fn with_filter(mut self, expression: impl Into<String>) -> Self {
        self.filter_expression = Some(expression.into());
        self
    }

    /// Set the sort direction.
    ///
    /// ```
    /// use mock_ddb::QueryRequest;
    /// let request = QueryRequest::new("users", "pk = :pk").with_scan_index_forward(false);
    /// assert_eq!(request.scan_index_forward, Some(false));
    /// ```
    #[must_use]
    pub fn with_scan_index_forward(mut self, ascending: bool) -> Self {
        self.scan_index_forward = Some(ascending);
        self
    }

    /// Cap the number of evaluated items.
    ///
    /// ```
    /// use mock_ddb::QueryRequest;
    /// let request = QueryRequest::new("users", "pk = :pk").with_limit(5);
    /// assert_eq!(request.limit, Some(5));
    /// ```
    #[must_use]
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Continue after `key`.
    ///
    /// ```
    /// use mock_ddb::{item, s, QueryRequest};
    /// let request = QueryRequest::new("users", "pk = :pk")
    ///     .with_exclusive_start_key(item([("pk", s("u#9"))]));
    /// assert!(request.exclusive_start_key.is_some());
    /// ```
    #[must_use]
    pub fn with_exclusive_start_key(mut self, key: Item) -> Self {
        self.exclusive_start_key = Some(key);
        self
    }

    /// Project the response.
    ///
    /// ```
    /// use mock_ddb::QueryRequest;
    /// let request = QueryRequest::new("users", "pk = :pk").with_projection("#n");
    /// assert_eq!(request.projection_expression.as_deref(), Some("#n"));
    /// ```
    #[must_use]
    pub fn with_projection(mut self, expression: impl Into<String>) -> Self {
        self.projection_expression = Some(expression.into());
        self
    }

    /// Attach expression-attribute names.
    ///
    /// ```
    /// use mock_ddb::{names, QueryRequest};
    /// let request = QueryRequest::new("users", "pk = :pk").with_names(names([("#n", "name")]));
    /// assert_eq!(request.expression_attribute_names.as_ref().unwrap()["#n"], "name");
    /// ```
    #[must_use]
    pub fn with_names(mut self, names: HashMap<String, String>) -> Self {
        self.expression_attribute_names = Some(names);
        self
    }

    /// Attach expression-attribute values.
    ///
    /// ```
    /// use mock_ddb::{s, values, QueryRequest};
    /// let request = QueryRequest::new("users", "pk = :pk").with_values(values([(":pk", s("u#1"))]));
    /// assert_eq!(request.expression_attribute_values.as_ref().unwrap().len(), 1);
    /// ```
    #[must_use]
    pub fn with_values(mut self, values: HashMap<String, AttributeValue>) -> Self {
        self.expression_attribute_values = Some(values);
        self
    }

    /// Select counts or specific attributes.
    ///
    /// ```
    /// use mock_ddb::{QueryRequest, Select};
    /// let request = QueryRequest::new("users", "pk = :pk").with_select(Select::Count);
    /// assert_eq!(request.select, Select::Count);
    /// ```
    #[must_use]
    pub fn with_select(mut self, select: Select) -> Self {
        self.select = select;
        self
    }

    /// Choose what the response reports about consumed capacity.
    ///
    /// ```
    /// use mock_ddb::{QueryRequest, ReturnConsumedCapacity};
    /// let request = QueryRequest::new("users", "pk = :pk")
    ///     .with_return_consumed_capacity(ReturnConsumedCapacity::Total);
    /// assert_eq!(request.return_consumed_capacity, ReturnConsumedCapacity::Total);
    /// ```
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }
}

/// `Query` output.
///
/// ```
/// use mock_ddb::QueryOutput;
/// let mut output = QueryOutput::default();
/// output.count = 3;
/// assert!(output.items.is_empty());
/// assert_eq!(output.scanned_count, 0);
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
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
///
/// ```
/// use mock_ddb::ScanRequest;
/// let request = ScanRequest::new("users");
/// assert_eq!(request.table_name, "users");
/// assert!(request.filter_expression.is_none());
/// ```
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
    ///
    /// ```
    /// use mock_ddb::ScanRequest;
    /// let request = ScanRequest::new("users");
    /// assert_eq!(request.table_name, "users");
    /// ```
    #[must_use]
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            ..Self::default()
        }
    }

    /// Apply a filter.
    ///
    /// ```
    /// use mock_ddb::ScanRequest;
    /// let request = ScanRequest::new("users").with_filter("age > :a");
    /// assert_eq!(request.filter_expression.as_deref(), Some("age > :a"));
    /// ```
    #[must_use]
    pub fn with_filter(mut self, expression: impl Into<String>) -> Self {
        self.filter_expression = Some(expression.into());
        self
    }

    /// Cap the number of evaluated items.
    ///
    /// ```
    /// use mock_ddb::ScanRequest;
    /// let request = ScanRequest::new("users").with_limit(10);
    /// assert_eq!(request.limit, Some(10));
    /// ```
    #[must_use]
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Continue after `key`.
    ///
    /// ```
    /// use mock_ddb::{item, s, ScanRequest};
    /// let request = ScanRequest::new("users").with_exclusive_start_key(item([("pk", s("u#9"))]));
    /// assert!(request.exclusive_start_key.is_some());
    /// ```
    #[must_use]
    pub fn with_exclusive_start_key(mut self, key: Item) -> Self {
        self.exclusive_start_key = Some(key);
        self
    }

    /// Scan one segment of a parallel scan.
    ///
    /// ```
    /// use mock_ddb::ScanRequest;
    /// let request = ScanRequest::new("users").with_segment(1, 4);
    /// assert_eq!(request.segment, Some(1));
    /// assert_eq!(request.total_segments, Some(4));
    /// ```
    #[must_use]
    pub fn with_segment(mut self, segment: u32, total_segments: u32) -> Self {
        self.segment = Some(segment);
        self.total_segments = Some(total_segments);
        self
    }

    /// Scan a secondary index.
    ///
    /// ```
    /// use mock_ddb::ScanRequest;
    /// let request = ScanRequest::new("users").with_index("by_email");
    /// assert_eq!(request.index_name.as_deref(), Some("by_email"));
    /// ```
    #[must_use]
    pub fn with_index(mut self, index_name: impl Into<String>) -> Self {
        self.index_name = Some(index_name.into());
        self
    }

    /// Project the response.
    ///
    /// ```
    /// use mock_ddb::ScanRequest;
    /// let request = ScanRequest::new("users").with_projection("#n");
    /// assert_eq!(request.projection_expression.as_deref(), Some("#n"));
    /// ```
    #[must_use]
    pub fn with_projection(mut self, expression: impl Into<String>) -> Self {
        self.projection_expression = Some(expression.into());
        self
    }

    /// Attach expression-attribute names.
    ///
    /// ```
    /// use mock_ddb::{names, ScanRequest};
    /// let request = ScanRequest::new("users").with_names(names([("#n", "name")]));
    /// assert_eq!(request.expression_attribute_names.as_ref().unwrap()["#n"], "name");
    /// ```
    #[must_use]
    pub fn with_names(mut self, names: HashMap<String, String>) -> Self {
        self.expression_attribute_names = Some(names);
        self
    }

    /// Attach expression-attribute values.
    ///
    /// ```
    /// use mock_ddb::{s, values, ScanRequest};
    /// let request = ScanRequest::new("users").with_values(values([(":a", s("30"))]));
    /// assert_eq!(request.expression_attribute_values.as_ref().unwrap().len(), 1);
    /// ```
    #[must_use]
    pub fn with_values(mut self, values: HashMap<String, AttributeValue>) -> Self {
        self.expression_attribute_values = Some(values);
        self
    }

    /// Select counts or specific attributes.
    ///
    /// ```
    /// use mock_ddb::{ScanRequest, Select};
    /// let request = ScanRequest::new("users").with_select(Select::Count);
    /// assert_eq!(request.select, Select::Count);
    /// ```
    #[must_use]
    pub fn with_select(mut self, select: Select) -> Self {
        self.select = select;
        self
    }

    /// Choose what the response reports about consumed capacity.
    ///
    /// ```
    /// use mock_ddb::{ReturnConsumedCapacity, ScanRequest};
    /// let request =
    ///     ScanRequest::new("users").with_return_consumed_capacity(ReturnConsumedCapacity::Total);
    /// assert_eq!(request.return_consumed_capacity, ReturnConsumedCapacity::Total);
    /// ```
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }
}

/// `Scan` output.
///
/// ```
/// use mock_ddb::ScanOutput;
/// let output = ScanOutput::default();
/// assert!(output.last_evaluated_key.is_none());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
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
///
/// ```
/// use mock_ddb::{item, s, WriteRequest};
/// let delete = WriteRequest::Delete {
///     key: item([("pk", s("u#1"))]),
/// };
/// assert!(matches!(delete, WriteRequest::Delete { .. }));
/// ```
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
///
/// ```
/// use mock_ddb::{item, s, BatchWriteItemRequest, WriteRequest};
/// let batch = BatchWriteItemRequest::new().add(
///     "users",
///     WriteRequest::Put {
///         item: item([("pk", s("u#1"))]),
///     },
/// );
/// assert_eq!(batch.request_items["users"].len(), 1);
/// ```
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
    ///
    /// ```
    /// use mock_ddb::BatchWriteItemRequest;
    /// let request = BatchWriteItemRequest::new();
    /// assert!(request.request_items.is_empty());
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a write for `table_name`.
    ///
    /// ```
    /// use mock_ddb::{item, s, BatchWriteItemRequest, WriteRequest};
    /// let batch = BatchWriteItemRequest::new()
    ///     .add("users", WriteRequest::Put { item: item([("pk", s("u#1"))]) })
    ///     .add("users", WriteRequest::Delete { key: item([("pk", s("u#2"))]) })
    ///     .add("orders", WriteRequest::Delete { key: item([("pk", s("o#1"))]) });
    /// assert_eq!(batch.request_items["users"].len(), 2);
    /// assert_eq!(batch.request_items["orders"].len(), 1);
    /// ```
    #[must_use]
    pub fn add(mut self, table_name: impl Into<String>, write: WriteRequest) -> Self {
        self.request_items
            .entry(table_name.into())
            .or_default()
            .push(write);
        self
    }

    /// Choose what the response reports about consumed capacity.
    ///
    /// ```
    /// use mock_ddb::{BatchWriteItemRequest, ReturnConsumedCapacity};
    /// let request =
    ///     BatchWriteItemRequest::new().with_return_consumed_capacity(ReturnConsumedCapacity::Total);
    /// assert_eq!(request.return_consumed_capacity, ReturnConsumedCapacity::Total);
    /// ```
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }

    /// Choose whether the response reports the touched item collections.
    ///
    /// ```
    /// use mock_ddb::{BatchWriteItemRequest, ReturnItemCollectionMetrics};
    /// let request = BatchWriteItemRequest::new()
    ///     .with_return_item_collection_metrics(ReturnItemCollectionMetrics::Size);
    /// assert_eq!(
    ///     request.return_item_collection_metrics,
    ///     ReturnItemCollectionMetrics::Size
    /// );
    /// ```
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
///
/// ```
/// use mock_ddb::BatchWriteItemOutput;
/// let output = BatchWriteItemOutput::default();
/// assert!(output.unprocessed_items.is_empty());
/// assert!(output.consumed_capacity.is_empty());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
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
///
/// ```
/// use mock_ddb::{item, s, KeysAndAttributes};
/// let keys = KeysAndAttributes::new(vec![item([("pk", s("u#1"))])]);
/// assert_eq!(keys.keys.len(), 1);
/// assert!(keys.projection_expression.is_none());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct KeysAndAttributes {
    /// Keys to read.
    ///
    /// One table may not name the same primary key twice; the whole batch is
    /// refused with a `ValidationException` then, the way the service refuses
    /// it. The comparison is on the primary key the request resolves to, not on
    /// the attribute maps as they were spelled.
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
    ///
    /// ```
    /// use mock_ddb::{item, s, KeysAndAttributes};
    /// let keys = KeysAndAttributes::new(vec![item([("pk", s("u#1"))]), item([("pk", s("u#2"))])]);
    /// assert_eq!(keys.keys.len(), 2);
    /// ```
    #[must_use]
    pub fn new(keys: Vec<Item>) -> Self {
        Self {
            keys,
            ..Self::default()
        }
    }

    /// Project each returned item.
    ///
    /// ```
    /// use mock_ddb::{item, s, KeysAndAttributes};
    /// let keys = KeysAndAttributes::new(vec![item([("pk", s("u#1"))])]).with_projection("pk, #n");
    /// assert_eq!(keys.projection_expression.as_deref(), Some("pk, #n"));
    /// ```
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
///
/// ```
/// use mock_ddb::{item, s, BatchGetItemRequest, KeysAndAttributes};
/// let batch = BatchGetItemRequest::new()
///     .add("users", KeysAndAttributes::new(vec![item([("pk", s("u#1"))])]))
///     .unwrap();
/// assert_eq!(batch.request_items.len(), 1);
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BatchGetItemRequest {
    /// Table name → keys. At most 100 keys in total.
    pub request_items: BTreeMap<String, KeysAndAttributes>,
    /// Whether to report the capacity each table consumed.
    pub return_consumed_capacity: ReturnConsumedCapacity,
}

impl BatchGetItemRequest {
    /// An empty request, to be filled with [`add`](Self::add).
    ///
    /// ```
    /// use mock_ddb::BatchGetItemRequest;
    /// let request = BatchGetItemRequest::new();
    /// assert!(request.request_items.is_empty());
    /// ```
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
    ///
    /// ```
    /// use mock_ddb::{BatchGetItemRequest, ReturnConsumedCapacity};
    /// let request =
    ///     BatchGetItemRequest::new().with_return_consumed_capacity(ReturnConsumedCapacity::Indexes);
    /// assert_eq!(request.return_consumed_capacity, ReturnConsumedCapacity::Indexes);
    /// ```
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }
}

/// `BatchGetItem` output.
///
/// ```
/// use mock_ddb::BatchGetItemOutput;
/// let output = BatchGetItemOutput::default();
/// assert!(output.responses.is_empty());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
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
///
/// ```
/// use mock_ddb::{item, s, TransactPut};
/// let action = TransactPut {
///     table_name: "users".to_string(),
///     item: item([("pk", s("u#1"))]),
///     ..TransactPut::default()
/// };
/// assert!(action.condition_expression.is_none());
/// ```
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
///
/// ```
/// use mock_ddb::{item, s, TransactUpdate};
/// let action = TransactUpdate {
///     table_name: "users".to_string(),
///     key: item([("pk", s("u#1"))]),
///     update_expression: "SET age = :a".to_string(),
///     ..TransactUpdate::default()
/// };
/// assert_eq!(action.update_expression, "SET age = :a");
/// ```
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
///
/// ```
/// use mock_ddb::{item, s, TransactDelete};
/// let action = TransactDelete {
///     table_name: "users".to_string(),
///     key: item([("pk", s("u#1"))]),
///     ..TransactDelete::default()
/// };
/// assert_eq!(action.table_name, "users");
/// ```
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
///
/// ```
/// use mock_ddb::{item, s, TransactConditionCheck};
/// let check = TransactConditionCheck {
///     table_name: "users".to_string(),
///     key: item([("pk", s("u#1"))]),
///     condition_expression: "attribute_exists(pk)".to_string(),
///     ..TransactConditionCheck::default()
/// };
/// assert_eq!(check.condition_expression, "attribute_exists(pk)");
/// ```
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
///
/// ```
/// use mock_ddb::{item, s, TransactWriteItem};
/// let action = TransactWriteItem::delete("users", item([("pk", s("u#1"))]));
/// assert_eq!(action.table_name(), "users");
/// assert!(matches!(action, TransactWriteItem::Delete(_)));
/// ```
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
    ///
    /// ```
    /// use mock_ddb::{item, s, TransactWriteItem};
    /// let action = TransactWriteItem::put("users", item([("pk", s("u#1"))]));
    /// assert!(matches!(action, TransactWriteItem::Put(_)));
    /// ```
    #[must_use]
    pub fn put(table_name: impl Into<String>, item: Item) -> Self {
        Self::Put(TransactPut {
            table_name: table_name.into(),
            item,
            ..TransactPut::default()
        })
    }

    /// An `Update` action.
    ///
    /// ```
    /// use mock_ddb::{item, s, TransactWriteItem};
    /// let action = TransactWriteItem::update("users", item([("pk", s("u#1"))]), "SET age = :a");
    /// let update = match action {
    ///     TransactWriteItem::Update(update) => update,
    ///     other => panic!("expected an update, got {other:?}"),
    /// };
    /// assert_eq!(update.update_expression, "SET age = :a");
    /// ```
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
    ///
    /// ```
    /// use mock_ddb::{item, s, TransactWriteItem};
    /// let action = TransactWriteItem::delete("users", item([("pk", s("u#1"))]));
    /// assert!(matches!(action, TransactWriteItem::Delete(_)));
    /// ```
    #[must_use]
    pub fn delete(table_name: impl Into<String>, key: Item) -> Self {
        Self::Delete(TransactDelete {
            table_name: table_name.into(),
            key,
            ..TransactDelete::default()
        })
    }

    /// A `ConditionCheck` action.
    ///
    /// ```
    /// use mock_ddb::{item, s, TransactWriteItem};
    /// let action =
    ///     TransactWriteItem::condition_check("users", item([("pk", s("u#1"))]), "attribute_exists(pk)");
    /// let check = match action {
    ///     TransactWriteItem::ConditionCheck(check) => check,
    ///     other => panic!("expected a condition check, got {other:?}"),
    /// };
    /// assert_eq!(check.condition_expression, "attribute_exists(pk)");
    /// ```
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
    ///
    /// ```
    /// use mock_ddb::{item, s, TransactWriteItem};
    /// let action = TransactWriteItem::put("users", item([("pk", s("u#1"))]));
    /// assert_eq!(action.table_name(), "users");
    /// ```
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
    ///
    /// ```
    /// use mock_ddb::{item, names, s, TransactWriteItem};
    /// let action =
    ///     TransactWriteItem::put("users", item([("pk", s("u#1"))])).with_names(names([("#n", "name")]));
    /// let put = match action {
    ///     TransactWriteItem::Put(put) => put,
    ///     other => panic!("expected a put, got {other:?}"),
    /// };
    /// assert_eq!(put.expression_attribute_names.as_ref().unwrap()["#n"], "name");
    /// ```
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
    ///
    /// ```
    /// use mock_ddb::{item, s, values, TransactWriteItem};
    /// let action = TransactWriteItem::put("users", item([("pk", s("u#1"))]))
    ///     .with_values(values([(":v", s("x"))]));
    /// let put = match action {
    ///     TransactWriteItem::Put(put) => put,
    ///     other => panic!("expected a put, got {other:?}"),
    /// };
    /// assert_eq!(put.expression_attribute_values.as_ref().unwrap().len(), 1);
    /// ```
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
    ///
    /// ```
    /// use mock_ddb::{item, s, TransactWriteItem};
    /// let action = TransactWriteItem::put("users", item([("pk", s("u#1"))]))
    ///     .with_condition("attribute_not_exists(pk)");
    /// let put = match action {
    ///     TransactWriteItem::Put(put) => put,
    ///     other => panic!("expected a put, got {other:?}"),
    /// };
    /// assert_eq!(put.condition_expression.as_deref(), Some("attribute_not_exists(pk)"));
    /// ```
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
///
/// ```
/// use mock_ddb::{item, s, TransactWriteItem, TransactWriteItemsRequest};
/// let request =
///     TransactWriteItemsRequest::new(vec![TransactWriteItem::put("users", item([("pk", s("u#1"))]))]);
/// assert_eq!(request.transact_items.len(), 1);
/// ```
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
    ///
    /// ```
    /// use mock_ddb::{item, s, TransactWriteItem, TransactWriteItemsRequest};
    /// let request =
    ///     TransactWriteItemsRequest::new(vec![TransactWriteItem::put("users", item([("pk", s("u#1"))]))]);
    /// assert_eq!(request.transact_items.len(), 1);
    /// assert!(request.client_request_token.is_none());
    /// ```
    #[must_use]
    pub fn new(transact_items: Vec<TransactWriteItem>) -> Self {
        Self {
            transact_items,
            client_request_token: None,
            ..Self::default()
        }
    }

    /// Make the transaction idempotent.
    ///
    /// ```
    /// use mock_ddb::{item, s, TransactWriteItem, TransactWriteItemsRequest};
    /// let request =
    ///     TransactWriteItemsRequest::new(vec![TransactWriteItem::put("users", item([("pk", s("u#1"))]))])
    ///         .with_client_request_token("charge-7f3a");
    /// assert_eq!(request.client_request_token.as_deref(), Some("charge-7f3a"));
    /// ```
    #[must_use]
    pub fn with_client_request_token(mut self, token: impl Into<String>) -> Self {
        self.client_request_token = Some(token.into());
        self
    }

    /// Choose what the response reports about consumed capacity.
    ///
    /// ```
    /// use mock_ddb::{TransactWriteItemsRequest, ReturnConsumedCapacity};
    /// let request = TransactWriteItemsRequest::new(Vec::new())
    ///     .with_return_consumed_capacity(ReturnConsumedCapacity::Total);
    /// assert_eq!(request.return_consumed_capacity, ReturnConsumedCapacity::Total);
    /// ```
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }

    /// Choose whether the response reports the touched item collections.
    ///
    /// ```
    /// use mock_ddb::{ReturnItemCollectionMetrics, TransactWriteItemsRequest};
    /// let request = TransactWriteItemsRequest::new(Vec::new())
    ///     .with_return_item_collection_metrics(ReturnItemCollectionMetrics::Size);
    /// assert_eq!(
    ///     request.return_item_collection_metrics,
    ///     ReturnItemCollectionMetrics::Size
    /// );
    /// ```
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
///
/// ```
/// use mock_ddb::TransactWriteItemsOutput;
/// let output = TransactWriteItemsOutput::default();
/// assert!(output.consumed_capacity.is_empty());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct TransactWriteItemsOutput {
    /// One entry per table the transaction wrote to, when requested.
    pub consumed_capacity: Vec<ConsumedCapacity>,
    /// Table name → the collection its writes touched, when requested.
    pub item_collection_metrics: BTreeMap<String, ItemCollectionMetrics>,
}

/// One `Get` action of a transaction.
///
/// ```
/// use mock_ddb::{item, s, TransactGet};
/// let get = TransactGet::new("users", item([("pk", s("u#1"))]));
/// assert_eq!(get.table_name, "users");
/// ```
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
    ///
    /// ```
    /// use mock_ddb::{item, s, TransactGet};
    /// let get = TransactGet::new("users", item([("pk", s("u#1"))]));
    /// assert_eq!(get.key.len(), 1);
    /// assert!(get.projection_expression.is_none());
    /// ```
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
///
/// ```
/// use mock_ddb::{item, s, TransactGet, TransactGetItemsRequest};
/// let request =
///     TransactGetItemsRequest::new(vec![TransactGet::new("users", item([("pk", s("u#1"))]))]);
/// assert_eq!(request.transact_items.len(), 1);
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TransactGetItemsRequest {
    /// The reads. At most 100.
    pub transact_items: Vec<TransactGet>,
    /// Whether to report the capacity each table consumed.
    pub return_consumed_capacity: ReturnConsumedCapacity,
}

impl TransactGetItemsRequest {
    /// A transaction over `transact_items`.
    ///
    /// ```
    /// use mock_ddb::{item, s, TransactGet, TransactGetItemsRequest};
    /// let request = TransactGetItemsRequest::new(vec![TransactGet::new("users", item([("pk", s("u#1"))]))]);
    /// assert_eq!(request.transact_items.len(), 1);
    /// ```
    #[must_use]
    pub fn new(transact_items: Vec<TransactGet>) -> Self {
        Self {
            transact_items,
            ..Self::default()
        }
    }

    /// Choose what the response reports about consumed capacity.
    ///
    /// ```
    /// use mock_ddb::{ReturnConsumedCapacity, TransactGetItemsRequest};
    /// let request = TransactGetItemsRequest::new(Vec::new())
    ///     .with_return_consumed_capacity(ReturnConsumedCapacity::Indexes);
    /// assert_eq!(request.return_consumed_capacity, ReturnConsumedCapacity::Indexes);
    /// ```
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }
}

/// `TransactGetItems` output.
///
/// ```
/// use mock_ddb::TransactGetItemsOutput;
/// let output = TransactGetItemsOutput::default();
/// assert!(output.responses.is_empty());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct TransactGetItemsOutput {
    /// One entry per request, in order. `None` when the key was absent.
    pub responses: Vec<Option<Item>>,
    /// One entry per table the transaction read from, when requested.
    pub consumed_capacity: Vec<ConsumedCapacity>,
}

/// `UpdateTimeToLive` input.
///
/// ```
/// use mock_ddb::UpdateTimeToLiveRequest;
/// let request = UpdateTimeToLiveRequest::new("users", "expires_at", true);
/// assert!(request.enabled);
/// ```
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
    ///
    /// ```
    /// use mock_ddb::UpdateTimeToLiveRequest;
    /// let request = UpdateTimeToLiveRequest::new("users", "expires_at", false);
    /// assert_eq!(request.attribute_name, "expires_at");
    /// assert!(!request.enabled);
    /// ```
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
///
/// ```
/// use mock_ddb::TimeToLiveStatus;
/// assert_eq!(TimeToLiveStatus::Enabled.as_str(), "ENABLED");
/// ```
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
    ///
    /// ```
    /// use mock_ddb::TimeToLiveStatus;
    /// assert_eq!(TimeToLiveStatus::Disabled.as_str(), "DISABLED");
    /// assert_eq!(TimeToLiveStatus::Enabled.as_str(), "ENABLED");
    /// ```
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "ENABLED",
            Self::Disabled => "DISABLED",
        }
    }
}

/// What `describe_time_to_live` reports.
///
/// ```
/// use mock_ddb::{TimeToLiveDescription, TimeToLiveStatus};
/// let description = TimeToLiveDescription {
///     status: TimeToLiveStatus::Disabled,
///     attribute_name: None,
/// };
/// assert_eq!(description.status.as_str(), "DISABLED");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeToLiveDescription {
    /// Whether TTL is on.
    pub status: TimeToLiveStatus,
    /// The TTL attribute, when one is configured.
    pub attribute_name: Option<String>,
}

/// `ListTables` input.
///
/// ```
/// use mock_ddb::ListTablesRequest;
/// let request = ListTablesRequest {
///     limit: Some(10),
///     ..ListTablesRequest::default()
/// };
/// assert_eq!(request.limit, Some(10));
/// assert!(request.exclusive_start_table_name.is_none());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ListTablesRequest {
    /// Continue after this table name.
    pub exclusive_start_table_name: Option<String>,
    /// Maximum number of names to return.
    pub limit: Option<u32>,
}

/// `ListTables` output.
///
/// ```
/// use mock_ddb::ListTablesOutput;
/// let output = ListTablesOutput::default();
/// assert!(output.table_names.is_empty());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct ListTablesOutput {
    /// Table names, sorted.
    pub table_names: Vec<String>,
    /// Cursor for the next page, when the limit stopped the listing early.
    pub last_evaluated_table_name: Option<String>,
}

/// `DescribeTable` output, re-exported here for discoverability.
///
/// ```
/// use mock_ddb::{DescribeTableOutput, KeySchema, KeyType, MockDdb};
///
/// # #[tokio::main]
/// # async fn main() {
/// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
/// let described: DescribeTableOutput = mock.describe_table("users").await.unwrap();
/// assert_eq!(described.table_name, "users");
/// # }
/// ```
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
        assert_eq!(request.request_items["t"].keys, Vec::new());
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
