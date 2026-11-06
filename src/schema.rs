//! Table and index definitions.
//!
//! The mock models a table the way the service does: a key schema, the
//! attribute definitions those key schemas refer to, and optional secondary
//! indexes. Unlike a product-specific mock, nothing here assumes a partition
//! key called `pk` or a sort key called `sk` — a table is told its own schema
//! and every operation validates against it.
//!
//! ```
//! use mock_ddb::{CreateTableRequest, KeySchema, KeyType};
//!
//! let request = CreateTableRequest::new(
//!     "users",
//!     KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::String),
//! );
//! assert_eq!(request.key_schema.sort_key.unwrap().attribute_name, "sk");
//! ```

use std::borrow::Cow;
use std::time::SystemTime;

use crate::{Error, Result};

/// The type of a key attribute.
///
/// DynamoDB allows strings, numbers and binary as key types; nothing else is
/// a valid key.
///
/// ```
/// use mock_ddb::KeyType;
///
/// assert_eq!(KeyType::String.to_string(), "S");
/// assert_eq!(KeyType::Number.to_string(), "N");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum KeyType {
    /// `S` — a UTF-8 string.
    String,
    /// `N` — a decimal number carried as a string.
    Number,
    /// `B` — opaque bytes.
    Binary,
}

impl KeyType {
    /// The single-letter type name DynamoDB uses on the wire.
    ///
    /// ```
    /// use mock_ddb::KeyType;
    ///
    /// assert_eq!(KeyType::Number.as_str(), "N");
    /// assert_eq!(KeyType::Binary.as_str(), "B");
    /// ```
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::String => "S",
            Self::Number => "N",
            Self::Binary => "B",
        }
    }
}

impl std::fmt::Display for KeyType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// One key attribute of a table or index.
///
/// ```
/// use mock_ddb::{KeySchemaElement, KeyType};
///
/// let element = KeySchemaElement::new("pk", KeyType::String);
/// assert_eq!(element.attribute_name, "pk");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeySchemaElement {
    /// Name of the item attribute that holds this key part.
    pub attribute_name: String,
    /// Its type.
    pub key_type: KeyType,
}

impl KeySchemaElement {
    /// Build a key element.
    #[must_use]
    pub fn new(attribute_name: impl Into<String>, key_type: KeyType) -> Self {
        Self {
            attribute_name: attribute_name.into(),
            key_type,
        }
    }
}

/// A table's or index's key schema: a partition key and an optional sort key.
///
/// ```
/// use mock_ddb::{KeySchema, KeyType};
///
/// let hash_only = KeySchema::hash("id", KeyType::String);
/// assert!(hash_only.sort_key.is_none());
///
/// let composite = KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::Number);
/// assert_eq!(composite.sort_key.unwrap().key_type, KeyType::Number);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeySchema {
    /// The partition (hash) key.
    pub partition_key: KeySchemaElement,
    /// The sort (range) key, if the table or index has one.
    pub sort_key: Option<KeySchemaElement>,
}

impl KeySchema {
    /// A schema with a partition key and no sort key.
    #[must_use]
    pub fn hash(attribute_name: impl Into<String>, key_type: KeyType) -> Self {
        Self {
            partition_key: KeySchemaElement::new(attribute_name, key_type),
            sort_key: None,
        }
    }

    /// A schema with both a partition and a sort key.
    ///
    /// The two must name different attributes: one attribute cannot hold both
    /// key roles, and [`crate::MockDdb::create_table`] rejects such a schema.
    #[must_use]
    pub fn hash_and_range(
        partition_name: impl Into<String>,
        partition_type: KeyType,
        sort_name: impl Into<String>,
        sort_type: KeyType,
    ) -> Self {
        Self {
            partition_key: KeySchemaElement::new(partition_name, partition_type),
            sort_key: Some(KeySchemaElement::new(sort_name, sort_type)),
        }
    }

    /// The key elements, partition first.
    pub(crate) fn elements(&self) -> Vec<&KeySchemaElement> {
        let mut elements = vec![&self.partition_key];
        if let Some(sort_key) = &self.sort_key {
            elements.push(sort_key);
        }
        elements
    }

    /// Whether `attribute` names one of the key attributes.
    pub(crate) fn contains(&self, attribute: &str) -> bool {
        self.elements()
            .iter()
            .any(|element| element.attribute_name == attribute)
    }
}

impl Default for KeySchema {
    /// `pk` of type `S`, with no sort key.
    ///
    /// This exists so that request types holding a key schema can derive
    /// `Default`; it is not a table's schema and is never a substitute for one.
    /// Nothing in this crate validates against a defaulted schema — see
    /// [`UpdateExpression::apply`](crate::UpdateExpression::apply), which
    /// requires the caller's schema — so a caller with a table passes
    /// `table.key_schema()` or the schema it created the table with.
    fn default() -> Self {
        Self::hash("pk", KeyType::String)
    }
}

/// One entry of the table's `AttributeDefinitions` list.
///
/// ```
/// use mock_ddb::{AttributeDefinition, KeyType};
///
/// let definition = AttributeDefinition::new("pk", KeyType::String);
/// assert_eq!(definition.attribute_type, KeyType::String);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributeDefinition {
    /// Attribute name.
    pub attribute_name: String,
    /// Declared type; must match the type an item stores there.
    pub attribute_type: KeyType,
}

impl AttributeDefinition {
    /// Build an attribute definition.
    #[must_use]
    pub fn new(attribute_name: impl Into<String>, attribute_type: KeyType) -> Self {
        Self {
            attribute_name: attribute_name.into(),
            attribute_type,
        }
    }
}

/// What an index copies from the table.
///
/// ```
/// use mock_ddb::Projection;
///
/// assert_eq!(Projection::default(), Projection::All);
/// assert_ne!(Projection::KeysOnly, Projection::Include(vec!["email".into()]));
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Projection {
    /// Every attribute (the default for a global secondary index).
    #[default]
    All,
    /// Only the table's and index's key attributes.
    KeysOnly,
    /// The keys plus the named non-key attributes.
    Include(Vec<String>),
}

/// A global or local secondary index definition.
///
/// ```
/// use mock_ddb::{KeySchema, KeyType, Projection, SecondaryIndex};
///
/// let index = SecondaryIndex::new(
///     "by-email",
///     KeySchema::hash("email", KeyType::String),
/// )
/// .with_projection(Projection::KeysOnly);
/// assert_eq!(index.index_name, "by-email");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecondaryIndex {
    /// Index name, unique within the table and subject to the service's name
    /// rule (3–255 characters from `[a-zA-Z0-9_.-]`, like a table name).
    pub index_name: String,
    /// The index's key schema. A global secondary index may use any key
    /// attributes; a local one must reuse the table's partition key.
    pub key_schema: KeySchema,
    /// Which attributes the index projects.
    pub projection: Projection,
}

impl SecondaryIndex {
    /// Build an index that projects all attributes.
    #[must_use]
    pub fn new(index_name: impl Into<String>, key_schema: KeySchema) -> Self {
        Self {
            index_name: index_name.into(),
            key_schema,
            projection: Projection::All,
        }
    }

    /// Replace the projection.
    #[must_use]
    pub fn with_projection(mut self, projection: Projection) -> Self {
        self.projection = projection;
        self
    }
}

/// How a table is billed. The mock never throttles, but the setting is
/// reported back so a describe assertion matches production.
///
/// ```
/// use mock_ddb::BillingMode;
/// assert_eq!(BillingMode::default(), BillingMode::PayPerRequest);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum BillingMode {
    /// `PROVISIONED` — the table has read/write capacity units.
    Provisioned,
    /// `PAY_PER_REQUEST` — on-demand.
    #[default]
    PayPerRequest,
}

impl BillingMode {
    /// The string the service reports.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Provisioned => "PROVISIONED",
            Self::PayPerRequest => "PAY_PER_REQUEST",
        }
    }
}

/// Which item images a stream records.
///
/// ```
/// use mock_ddb::StreamViewType;
/// assert_eq!(StreamViewType::default(), StreamViewType::NewAndOldImages);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamViewType {
    /// Only the new image.
    NewImage,
    /// Only the old image.
    OldImage,
    /// Both images.
    #[default]
    NewAndOldImages,
    /// Only the key attributes.
    KeysOnly,
}

impl StreamViewType {
    /// The string the service reports.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NewImage => "NEW_IMAGE",
            Self::OldImage => "OLD_IMAGE",
            Self::NewAndOldImages => "NEW_AND_OLD_IMAGES",
            Self::KeysOnly => "KEYS_ONLY",
        }
    }
}

/// Stream configuration for a table.
///
/// ```
/// use mock_ddb::{StreamSpecification, StreamViewType};
///
/// let specification = StreamSpecification {
///     stream_enabled: true,
///     stream_view_type: Some(StreamViewType::KeysOnly),
/// };
/// assert!(specification.stream_enabled);
/// ```
///
/// The two fields are kept consistent by [`crate::MockDdb::create_table`]: a stream
/// that is on needs a view type, and a view type without a stream is refused
/// rather than silently dropped.
///
/// ```
/// use mock_ddb::{CreateTableRequest, KeySchema, KeyType, MockDdb, StreamSpecification};
///
/// # #[tokio::main]
/// # async fn main() {
/// let request = CreateTableRequest {
///     stream_specification: Some(StreamSpecification {
///         stream_enabled: true,
///         stream_view_type: None,
///     }),
///     ..CreateTableRequest::new("events", KeySchema::hash("pk", KeyType::String))
/// };
/// let error = MockDdb::new().create_table(request).await.unwrap_err();
/// assert!(error.to_string().contains("StreamViewType"), "{error}");
/// # }
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamSpecification {
    /// Whether the stream is on.
    pub stream_enabled: bool,
    /// Which images to record. Required when `stream_enabled` is true.
    pub stream_view_type: Option<StreamViewType>,
}

/// Everything needed to create a table.
///
/// `attribute_definitions` may be left empty: the mock derives it from the key
/// schemas, which is what every caller would otherwise write by hand. When it
/// is supplied it is validated strictly, including DynamoDB's rule that an
/// attribute definition which no key schema uses is an error.
///
/// ```
/// use mock_ddb::{CreateTableRequest, KeySchema, KeyType, Projection, SecondaryIndex};
///
/// let request = CreateTableRequest::new(
///     "orders",
///     KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::String),
/// )
/// .with_global_index(
///     SecondaryIndex::new("by-customer", KeySchema::hash("customer", KeyType::String))
///         .with_projection(Projection::All),
/// );
/// assert_eq!(request.global_secondary_indexes.len(), 1);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateTableRequest {
    /// Table name.
    pub table_name: String,
    /// The table's key schema.
    pub key_schema: KeySchema,
    /// Declared attribute types. Empty means "derive from the key schemas".
    pub attribute_definitions: Vec<AttributeDefinition>,
    /// Global secondary indexes.
    pub global_secondary_indexes: Vec<SecondaryIndex>,
    /// Local secondary indexes.
    pub local_secondary_indexes: Vec<SecondaryIndex>,
    /// Billing mode.
    pub billing_mode: BillingMode,
    /// Stream configuration, if any.
    pub stream_specification: Option<StreamSpecification>,
}

impl CreateTableRequest {
    /// Build a request for `table_name` with the given key schema.
    #[must_use]
    pub fn new(table_name: impl Into<String>, key_schema: KeySchema) -> Self {
        Self {
            table_name: table_name.into(),
            key_schema,
            ..Self::default()
        }
    }

    /// Add a global secondary index.
    #[must_use]
    pub fn with_global_index(mut self, index: SecondaryIndex) -> Self {
        self.global_secondary_indexes.push(index);
        self
    }

    /// Add a local secondary index.
    ///
    /// The table must declare a sort key: DynamoDB offers local secondary
    /// indexes only on tables that have one, so [`crate::MockDdb::create_table`]
    /// rejects a hash-only table that carries a local index.
    ///
    /// ```
    /// use mock_ddb::{CreateTableRequest, KeySchema, KeyType, MockDdb, SecondaryIndex};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let request = CreateTableRequest::new("orders", KeySchema::hash("pk", KeyType::String))
    ///     .with_local_index(SecondaryIndex::new(
    ///         "by-status",
    ///         KeySchema::hash_and_range("pk", KeyType::String, "status", KeyType::String),
    ///     ));
    /// let error = MockDdb::new().create_table(request).await.unwrap_err();
    /// assert!(error.to_string().contains("range key"), "{error}");
    /// # }
    /// ```
    #[must_use]
    pub fn with_local_index(mut self, index: SecondaryIndex) -> Self {
        self.local_secondary_indexes.push(index);
        self
    }

    /// Turn a stream on with the given view type.
    #[must_use]
    pub fn with_stream(mut self, view_type: StreamViewType) -> Self {
        self.stream_specification = Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(view_type),
        });
        self
    }

    /// Validate the definition and return the attribute definitions the table
    /// should carry.
    pub(crate) fn validate(&self) -> Result<Vec<AttributeDefinition>> {
        validate_table_name(&self.table_name)?;
        self.validate_attribute_names()?;
        self.validate_key_schemas()?;
        self.validate_index_limits()?;
        let derived = self.derive_attribute_definitions()?;
        self.validate_index_names()?;
        self.validate_local_indexes()?;
        self.validate_stream_specification()?;
        if self.attribute_definitions.is_empty() {
            return Ok(derived);
        }
        self.validate_declared_definitions(&derived)?;
        Ok(self.attribute_definitions.clone())
    }

    /// Every attribute name a table definition carries must fit the service's
    /// 1–255 character window.
    ///
    /// Attribute names do not share the table/index charset restriction, so
    /// this deliberately checks length only. It covers the names in the table
    /// key schema, every secondary-index key schema, and every explicitly
    /// declared attribute definition.
    fn validate_attribute_names(&self) -> Result<()> {
        validate_key_schema_attribute_names(&self.key_schema, "keySchema")?;
        for (position, index) in self.global_secondary_indexes.iter().enumerate() {
            validate_key_schema_attribute_names(
                &index.key_schema,
                &format!("globalSecondaryIndexes.{}.member.keySchema", position + 1),
            )?;
        }
        for (position, index) in self.local_secondary_indexes.iter().enumerate() {
            validate_key_schema_attribute_names(
                &index.key_schema,
                &format!("localSecondaryIndexes.{}.member.keySchema", position + 1),
            )?;
        }
        for (position, definition) in self.attribute_definitions.iter().enumerate() {
            validate_attribute_name(
                &definition.attribute_name,
                &format!("attributeDefinitions.{}.member.attributeName", position + 1),
            )?;
        }
        Ok(())
    }

    /// Every key schema must name two different attributes on its two roles.
    ///
    /// One attribute cannot be both the partition and the sort key: the
    /// resulting primary key would be ambiguous, and the derived definitions
    /// hide the duplicate because they collapse same-named attributes.
    fn validate_key_schemas(&self) -> Result<()> {
        let key_schemas = std::iter::once(("the table", &self.key_schema))
            .chain(
                self.global_secondary_indexes
                    .iter()
                    .map(|index| (index.index_name.as_str(), &index.key_schema)),
            )
            .chain(
                self.local_secondary_indexes
                    .iter()
                    .map(|index| (index.index_name.as_str(), &index.key_schema)),
            );
        for (owner, schema) in key_schemas {
            let Some(sort_key) = &schema.sort_key else {
                continue;
            };
            if sort_key.attribute_name == schema.partition_key.attribute_name {
                return Err(Error::validation(format!(
                    "One or more parameter values were invalid: Key schema of {owner} uses attribute {} as both partition and sort key",
                    sort_key.attribute_name
                )));
            }
        }
        Ok(())
    }

    /// Enforce the per-table index limits.
    fn validate_index_limits(&self) -> Result<()> {
        if self.global_secondary_indexes.len() > 20 {
            return Err(Error::validation(
                "One or more parameter values were invalid: Number of global secondary indexes exceeds the per table limit (20)",
            ));
        }
        if self.local_secondary_indexes.len() > 5 {
            return Err(Error::validation(
                "One or more parameter values were invalid: Number of local secondary indexes exceeds the per table limit (5)",
            ));
        }
        Ok(())
    }

    /// Collect the attribute definitions the key schemas imply, rejecting a
    /// key attribute that two schemas declare with different types.
    fn derive_attribute_definitions(&self) -> Result<Vec<AttributeDefinition>> {
        let mut derived: Vec<AttributeDefinition> = Vec::new();
        let key_schemas = std::iter::once(("the table", &self.key_schema))
            .chain(
                self.global_secondary_indexes
                    .iter()
                    .map(|index| (index.index_name.as_str(), &index.key_schema)),
            )
            .chain(
                self.local_secondary_indexes
                    .iter()
                    .map(|index| (index.index_name.as_str(), &index.key_schema)),
            );
        for (owner, schema) in key_schemas {
            for element in schema.elements() {
                match derived
                    .iter_mut()
                    .find(|definition| definition.attribute_name == element.attribute_name)
                {
                    Some(existing) if existing.attribute_type != element.key_type => {
                        return Err(Error::validation(format!(
                            "One or more parameter values were invalid: AttributeDefinitions mismatch for attribute {} ({owner} uses {} but another key schema uses {})",
                            element.attribute_name, element.key_type, existing.attribute_type
                        )));
                    }
                    Some(_) => {}
                    None => derived.push(AttributeDefinition::new(
                        element.attribute_name.clone(),
                        element.key_type,
                    )),
                }
            }
        }
        Ok(derived)
    }

    /// Every index needs a name the service would accept, unique within the
    /// table.
    ///
    /// The index name carries the same documented rule as the table name
    /// (3–255 characters out of `[a-zA-Z0-9_.-]+`), so it goes through the
    /// same predicate; an index name the service rejects must not yield a mock
    /// table that a deploy would fail to create. The message names the request
    /// path, which is how the service reports a bad list member.
    fn validate_index_names(&self) -> Result<()> {
        for (position, index) in self.global_secondary_indexes.iter().enumerate() {
            validate_name(
                &index.index_name,
                &format!("globalSecondaryIndexes.{}.member.indexName", position + 1),
            )?;
        }
        for (position, index) in self.local_secondary_indexes.iter().enumerate() {
            validate_name(
                &index.index_name,
                &format!("localSecondaryIndexes.{}.member.indexName", position + 1),
            )?;
        }
        for index in self
            .global_secondary_indexes
            .iter()
            .chain(&self.local_secondary_indexes)
        {
            let duplicates = self
                .global_secondary_indexes
                .iter()
                .chain(&self.local_secondary_indexes)
                .filter(|candidate| candidate.index_name == index.index_name)
                .count();
            if duplicates > 1 {
                return Err(Error::validation(format!(
                    "One or more parameter values were invalid: Duplicate index name {}",
                    index.index_name
                )));
            }
        }
        Ok(())
    }

    /// A local index shares the table partition key and needs its own sort key.
    ///
    /// The table itself needs a sort key first: DynamoDB only offers local
    /// secondary indexes on tables that have a range key, so a hash-only
    /// table with a local index is rejected before any per-index check.
    fn validate_local_indexes(&self) -> Result<()> {
        if self.key_schema.sort_key.is_none() && !self.local_secondary_indexes.is_empty() {
            return Err(Error::validation(
                "One or more parameter values were invalid: Table KeySchema does not have a range key, which is required when specifying a LocalSecondaryIndex",
            ));
        }
        for index in &self.local_secondary_indexes {
            if index.key_schema.partition_key.attribute_name
                != self.key_schema.partition_key.attribute_name
            {
                return Err(Error::validation(format!(
                    "One or more parameter values were invalid: Local secondary index {} must use the table partition key {}",
                    index.index_name, self.key_schema.partition_key.attribute_name
                )));
            }
            let Some(index_sort) = &index.key_schema.sort_key else {
                return Err(Error::validation(format!(
                    "One or more parameter values were invalid: Local secondary index {} requires a sort key",
                    index.index_name
                )));
            };
            if self
                .key_schema
                .sort_key
                .as_ref()
                .is_some_and(|table_sort| table_sort.attribute_name == index_sort.attribute_name)
            {
                return Err(Error::validation(format!(
                    "One or more parameter values were invalid: Local secondary index {} sort key must differ from the table sort key",
                    index.index_name
                )));
            }
        }
        Ok(())
    }

    /// A stream specification must be coherent: an enabled stream needs a view
    /// type, and a view type has no meaning without an enabled stream.
    fn validate_stream_specification(&self) -> Result<()> {
        let Some(specification) = &self.stream_specification else {
            return Ok(());
        };
        if specification.stream_enabled && specification.stream_view_type.is_none() {
            return Err(Error::validation(
                "One or more parameter values were invalid: StreamViewType must be specified when StreamEnabled is true",
            ));
        }
        if !specification.stream_enabled && specification.stream_view_type.is_some() {
            return Err(Error::validation(
                "One or more parameter values were invalid: StreamViewType must not be specified when StreamEnabled is false",
            ));
        }
        Ok(())
    }

    /// When definitions are supplied they must match the key schemas exactly:
    /// no duplicates, no unused entries, no missing key attributes.
    fn validate_declared_definitions(&self, derived: &[AttributeDefinition]) -> Result<()> {
        for definition in &self.attribute_definitions {
            let duplicate = self
                .attribute_definitions
                .iter()
                .filter(|candidate| candidate.attribute_name == definition.attribute_name)
                .count();
            if duplicate > 1 {
                return Err(Error::validation(format!(
                    "One or more parameter values were invalid: Duplicate AttributeDefinition for {}",
                    definition.attribute_name
                )));
            }
            let Some(expected) = derived
                .iter()
                .find(|candidate| candidate.attribute_name == definition.attribute_name)
            else {
                return Err(Error::validation(format!(
                    "One or more parameter values were invalid: Some AttributeDefinitions are not used: {}",
                    definition.attribute_name
                )));
            };
            if expected.attribute_type != definition.attribute_type {
                return Err(Error::validation(format!(
                    "One or more parameter values were invalid: Type mismatch for attribute {} (expected {}, got {})",
                    definition.attribute_name, expected.attribute_type, definition.attribute_type
                )));
            }
        }
        for expected in derived {
            if !self
                .attribute_definitions
                .iter()
                .any(|definition| definition.attribute_name == expected.attribute_name)
            {
                return Err(Error::validation(format!(
                    "One or more parameter values were invalid: Missing AttributeDefinition for key attribute {}",
                    expected.attribute_name
                )));
            }
        }
        Ok(())
    }
}

/// What `describe_table` reports.
///
/// `table_status` is always `ACTIVE`: the mock has no async table lifecycle, so
/// a table exists the moment `create_table` returns.
///
/// Like every other response type it is `#[non_exhaustive]`: every field is
/// readable and the mock is the only place that builds one, so a field the
/// service reports later does not break a caller.
///
/// ```
/// use mock_ddb::{KeyType, MockDdb, CreateTableRequest, KeySchema};
///
/// # #[tokio::main]
/// # async fn main() {
/// let mock = MockDdb::new();
/// mock.create_table(CreateTableRequest::new(
///     "users",
///     KeySchema::hash("pk", KeyType::String),
/// ))
/// .await
/// .unwrap();
/// let description = mock.describe_table("users").await.unwrap();
/// assert_eq!(description.table_status, "ACTIVE");
/// assert_eq!(description.item_count, 0);
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TableDescription {
    /// Table name.
    pub table_name: String,
    /// The table's key schema.
    pub key_schema: KeySchema,
    /// Declared attribute types.
    pub attribute_definitions: Vec<AttributeDefinition>,
    /// Global secondary indexes.
    pub global_secondary_indexes: Vec<SecondaryIndex>,
    /// Local secondary indexes.
    pub local_secondary_indexes: Vec<SecondaryIndex>,
    /// Billing mode.
    pub billing_mode: BillingMode,
    /// Always `ACTIVE`.
    pub table_status: String,
    /// Live item count. The real service refreshes this every few hours; the
    /// mock reports the exact number on every call.
    pub item_count: u64,
    /// Stream ARN, when streams are enabled.
    pub latest_stream_arn: Option<String>,
    /// When the table was created, on the mock's clock.
    pub creation_date_time: Option<SystemTime>,
}

/// Shortest name the service accepts for a table or an index.
const MIN_NAME_LEN: usize = 3;

/// Longest name the service accepts for a table or an index.
const MAX_NAME_LEN: usize = 255;

/// Shortest attribute name the service accepts.
const MIN_ATTRIBUTE_NAME_LEN: usize = 1;

/// Longest attribute name the service accepts.
const MAX_ATTRIBUTE_NAME_LEN: usize = 255;

/// Longest slice of an untrusted name an error message echoes back.
///
/// The service quotes the offending value verbatim; a mock must not, because
/// the value is caller-controlled and, until a length check has run, unbounded
/// — a megabyte-long table name would otherwise be cloned into the `Error`,
/// and from there into whatever log or test report prints it.
const MAX_ECHO_CHARS: usize = 64;

/// Validate a table name against DynamoDB's documented constraints.
pub(crate) fn validate_table_name(name: &str) -> Result<()> {
    validate_name(name, "tableName")
}

/// Validate every attribute name in a key schema, numbering list members from
/// one as the service does.
fn validate_key_schema_attribute_names(schema: &KeySchema, location: &str) -> Result<()> {
    for (position, element) in schema.elements().into_iter().enumerate() {
        validate_attribute_name(
            &element.attribute_name,
            &format!("{location}.{}.member.attributeName", position + 1),
        )?;
    }
    Ok(())
}

/// Check an attribute name against the service's documented 1–255 character
/// length window.
///
/// Unlike table and index names, attribute names have no restricted character
/// set: only their length is checked. The message keeps the service's shape and
/// uses the bounded echo shared with table-name errors.
fn validate_attribute_name(name: &str, location: &str) -> Result<()> {
    let length = name.chars().count();
    if length < MIN_ATTRIBUTE_NAME_LEN {
        let value = echoed_name(name);
        return Err(Error::validation(format!(
            "1 validation error detected: Value '{value}' at '{location}' failed to satisfy constraint: Member must have length greater than or equal to {MIN_ATTRIBUTE_NAME_LEN}"
        )));
    }
    if length > MAX_ATTRIBUTE_NAME_LEN {
        let value = echoed_name(name);
        return Err(Error::validation(format!(
            "1 validation error detected: Value '{value}' at '{location}' failed to satisfy constraint: Member must have length less than or equal to {MAX_ATTRIBUTE_NAME_LEN}"
        )));
    }
    Ok(())
}

/// Check `name` against the one name rule the service documents for both table
/// and index names: 3 to 255 characters drawn from `[a-zA-Z0-9_.-]`.
///
/// `location` is the request path the service points at in its message, e.g.
/// `tableName` or `globalSecondaryIndexes.1.member.indexName` (list members are
/// numbered from 1). Keeping the predicate in a single place is what stops the
/// table name and the index names from drifting apart: before it existed the
/// table name was bounded and an index name was only checked for emptiness, so
/// a mock table could carry a name the service rejects.
///
/// The length is counted in characters, not bytes — the constraint is on the
/// value's length, and a multi-byte name is rejected by the charset rule below
/// either way. The echoed value is elided: a name longer than [`MAX_ECHO_CHARS`]
/// characters is quoted as its first [`MAX_ECHO_CHARS`] characters followed by a
/// `...` marker, so the message says which value was rejected without copying an
/// unbounded caller string into it. A name of exactly [`MAX_ECHO_CHARS`]
/// characters carries no marker and is quoted whole.
fn validate_name(name: &str, location: &str) -> Result<()> {
    let length = name.chars().count();
    if length < MIN_NAME_LEN {
        let value = echoed_name(name);
        return Err(Error::validation(format!(
            "1 validation error detected: Value '{value}' at '{location}' failed to satisfy constraint: Member must have length greater than or equal to {MIN_NAME_LEN}"
        )));
    }
    if length > MAX_NAME_LEN {
        let value = echoed_name(name);
        return Err(Error::validation(format!(
            "1 validation error detected: Value '{value}' at '{location}' failed to satisfy constraint: Member must have length less than or equal to {MAX_NAME_LEN}"
        )));
    }
    if !name.chars().all(is_name_character) {
        let value = echoed_name(name);
        return Err(Error::validation(format!(
            "1 validation error detected: Value '{value}' at '{location}' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
        )));
    }
    Ok(())
}

/// Render an untrusted name for an error message, eliding anything past
/// [`MAX_ECHO_CHARS`] characters.
///
/// The elision is measured in characters, so a multi-byte value is never cut
/// through a scalar value. Values within the budget are returned unchanged;
/// longer ones keep their first [`MAX_ECHO_CHARS`] characters and gain a `...`
/// marker, which puts the result at most `MAX_ECHO_CHARS + 3` characters long.
/// It is called on the rejection path only, so a valid name pays nothing.
fn echoed_name(name: &str) -> Cow<'_, str> {
    if name.chars().count() <= MAX_ECHO_CHARS {
        return Cow::Borrowed(name);
    }
    let kept: String = name.chars().take(MAX_ECHO_CHARS).collect();
    Cow::Owned(format!("{kept}..."))
}

/// Whether `character` belongs to the set the service allows in a name.
fn is_name_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{
        echoed_name, validate_attribute_name, validate_table_name, MAX_ATTRIBUTE_NAME_LEN,
        MAX_ECHO_CHARS, MAX_NAME_LEN, MIN_ATTRIBUTE_NAME_LEN, MIN_NAME_LEN,
    };

    #[test]
    fn table_name_length_window_is_inclusive_on_both_ends() {
        // One character short of the minimum, then exactly the minimum.
        let too_short = validate_table_name(&"a".repeat(MIN_NAME_LEN - 1))
            .expect_err("a name below the minimum has to be rejected")
            .to_string();
        assert!(
            too_short.contains("length greater than or equal to 3"),
            "{too_short}"
        );
        assert!(validate_table_name(&"a".repeat(MIN_NAME_LEN)).is_ok());

        // The empty name belongs to the same lower-bound rule.
        let empty = validate_table_name("")
            .expect_err("an empty name has to be rejected")
            .to_string();
        assert!(
            empty.contains("length greater than or equal to 3"),
            "{empty}"
        );

        // Exactly the maximum is accepted, one character over is not.
        assert!(validate_table_name(&"a".repeat(MAX_NAME_LEN)).is_ok());
        let too_long = validate_table_name(&"a".repeat(MAX_NAME_LEN + 1))
            .expect_err("a name above the maximum has to be rejected")
            .to_string();
        assert!(
            too_long.contains("length less than or equal to 255"),
            "{too_long}"
        );
        assert!(
            !too_long.contains("greater than or equal to"),
            "the upper bound must not borrow the lower bound's wording: {too_long}"
        );
    }

    #[test]
    fn name_length_is_counted_in_characters_not_bytes() {
        // 200 scalars are 400 bytes: inside the character window, outside a byte
        // window. The charset rule has to be the one that rejects this — a
        // byte-based length check would report it as too long instead.
        let wide = "ä".repeat(200);
        let error = validate_table_name(&wide)
            .expect_err("a non-ASCII name has to be rejected")
            .to_string();
        assert!(
            error.contains("regular expression pattern: [a-zA-Z0-9_.-]+"),
            "{error}"
        );
        assert!(!error.contains("length"), "{error}");

        // The same name at the minimum width: three scalars, six bytes.
        let narrow = "äöü";
        let error = validate_table_name(narrow)
            .expect_err("a non-ASCII name has to be rejected")
            .to_string();
        assert!(
            error.contains("regular expression pattern: [a-zA-Z0-9_.-]+"),
            "{error}"
        );
    }

    /// Names drawn from the allowed charset, at a length that straddles both
    /// bounds of the window.
    fn ascii_name() -> impl Strategy<Value = String> {
        prop::collection::vec(prop::sample::select(b"abcXYZ019_.-".as_slice()), 0..=300)
            .prop_map(|characters| characters.into_iter().map(char::from).collect())
    }

    proptest! {
        #[test]
        fn attribute_names_are_accepted_exactly_inside_the_length_window(name in any::<String>()) {
            let characters = name.chars().count();
            let inside = (MIN_ATTRIBUTE_NAME_LEN..=MAX_ATTRIBUTE_NAME_LEN).contains(&characters);
            prop_assert_eq!(
                validate_attribute_name(&name, "attributeName").is_ok(),
                inside,
                "{} characters: {:?}",
                characters,
                name
            );
        }

        #[test]
        fn every_charset_conforming_name_is_accepted_exactly_inside_the_window(name in ascii_name()) {
            let characters = name.chars().count();
            let inside = (MIN_NAME_LEN..=MAX_NAME_LEN).contains(&characters);
            prop_assert_eq!(
                validate_table_name(&name).is_ok(),
                inside,
                "{} characters: {:?}",
                characters,
                name
            );
        }

        #[test]
        fn a_multi_byte_name_is_rejected_by_the_charset_rule(width in MIN_NAME_LEN..=MAX_NAME_LEN) {
            let name = "ä".repeat(width);
            let error = validate_table_name(&name)
                .expect_err("a non-ASCII name has to be rejected")
                .to_string();
            prop_assert!(
                error.contains("regular expression pattern"),
                "{}",
                error
            );
            prop_assert!(!error.contains("length"), "{}", error);
        }
    }

    #[test]
    fn echoes_a_value_that_fits_unchanged() {
        // Just inside the budget: quoted whole, no marker.
        let at_limit = "a".repeat(MAX_ECHO_CHARS);
        assert_eq!(echoed_name(&at_limit), at_limit);

        // One character over: the first MAX_ECHO_CHARS characters plus the
        // marker, and the result is bounded.
        let over = "a".repeat(MAX_ECHO_CHARS + 1);
        let rendered = echoed_name(&over);
        assert_eq!(rendered, format!("{}...", "a".repeat(MAX_ECHO_CHARS)));
        assert_eq!(rendered.chars().count(), MAX_ECHO_CHARS + 3);
        assert!(!rendered.contains(&over), "{rendered}");
    }

    #[test]
    fn elides_on_a_character_boundary_not_a_byte_one() {
        // Multi-byte scalars: a byte-based cut would split a character and
        // return invalid UTF-8, so the count has to be in characters.
        let over = "ä".repeat(MAX_ECHO_CHARS + 1);
        let rendered = echoed_name(&over);
        assert!(rendered.ends_with("..."), "{rendered}");
        assert_eq!(
            rendered.chars().count(),
            MAX_ECHO_CHARS + 3,
            "the marker must not eat into the budget"
        );
        assert!(rendered.starts_with(&"ä".repeat(MAX_ECHO_CHARS)));
    }

    #[test]
    fn keeps_short_and_empty_values_untouched() {
        assert_eq!(echoed_name(""), "");
        assert_eq!(echoed_name("orders"), "orders");
        assert_eq!(echoed_name("ab"), "ab");
    }

    proptest! {
        #[test]
        fn elision_is_bounded_prefix_preserving_and_idempotent(value in any::<String>()) {
            let rendered = echoed_name(&value);
            let characters = value.chars().count();

            prop_assert!(rendered.chars().count() <= MAX_ECHO_CHARS + 3);
            if characters <= MAX_ECHO_CHARS {
                prop_assert_eq!(rendered.as_ref(), value.as_str());
            } else {
                let kept: String = value.chars().take(MAX_ECHO_CHARS).collect();
                prop_assert_eq!(rendered.as_ref(), format!("{kept}..."));
                prop_assert!(rendered.ends_with("..."));
            }
            // Eliding an already-elided value changes nothing.
            prop_assert_eq!(echoed_name(&rendered), rendered.clone());
        }
    }
}
