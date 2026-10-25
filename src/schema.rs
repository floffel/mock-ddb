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

use std::time::SystemTime;

use crate::{Error, Result};

/// The type of a key attribute.
///
/// DynamoDB allows strings, numbers and binary as key types; nothing else is
/// a valid key.
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
    /// `pk` of type `S`, with no sort key — the layout most hand-written
    /// tables use, and the one the pre-extraction mock hard-coded.
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
    /// Index name, unique within the table.
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
        self.validate_index_limits()?;
        let derived = self.derive_attribute_definitions()?;
        self.validate_index_names()?;
        self.validate_local_indexes()?;
        if self.attribute_definitions.is_empty() {
            return Ok(derived);
        }
        self.validate_declared_definitions(&derived)?;
        Ok(self.attribute_definitions.clone())
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

    /// Every index needs a non-empty, unique name.
    fn validate_index_names(&self) -> Result<()> {
        for index in self
            .global_secondary_indexes
            .iter()
            .chain(&self.local_secondary_indexes)
        {
            if index.index_name.is_empty() {
                return Err(Error::validation(
                    "One or more parameter values were invalid: Index name must not be empty",
                ));
            }
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
    fn validate_local_indexes(&self) -> Result<()> {
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

/// Validate a table name against DynamoDB's documented constraints.
pub(crate) fn validate_table_name(name: &str) -> Result<()> {
    if name.len() < 3 || name.len() > 255 {
        return Err(Error::validation(format!(
            "1 validation error detected: Value '{name}' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3"
        )));
    }
    if !name
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.'))
    {
        return Err(Error::validation(format!(
            "1 validation error detected: Value '{name}' at 'tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
        )));
    }
    Ok(())
}
