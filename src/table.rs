//! Per-table storage: items ordered by key, secondary indexes, TTL and stream.
//!
//! One [`Table`] owns everything that belongs to a table name. Items live in a
//! `BTreeMap` keyed by their (partition, sort) scalars, which gives DynamoDB's
//! query order for free: partition key first, then sort key, both in the key
//! type's natural order.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::SystemTime;

use crate::schema::{
    BillingMode, CreateTableRequest, KeySchema, KeySchemaElement, Projection, SecondaryIndex,
    StreamViewType, TableDescription,
};
#[cfg(feature = "streams")]
use crate::stream::{StreamEventName, StreamState};
use crate::value::{item_size, item_too_large, type_name, validate_value, Scalar, MAX_ITEM_SIZE};
use crate::{Error, Item, Result};

/// A primary key: partition scalar plus optional sort scalar.
pub(crate) type ItemKey = (Scalar, Option<Scalar>);

/// A secondary-index key: same shape as a primary key.
pub(crate) type IndexKey = (Scalar, Option<Scalar>);

/// Items of one table, ordered by primary key.
pub(crate) type ItemMap = BTreeMap<ItemKey, Item>;

/// Index name → (index key → set of primary keys).
pub(crate) type IndexMap = HashMap<String, BTreeMap<IndexKey, BTreeSet<ItemKey>>>;

/// The most expired keys one purge pass may collect and delete.
const MAX_TTL_PURGE_BATCH: usize = 1_024;

/// How a successful store should appear in the table's stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StoreKind {
    /// A normal put: INSERT when the previous row was logically absent.
    Put,
    /// An update: MODIFY even when it creates the row.
    Update,
    /// A fixture write: record only a logically visible new item.
    Seed,
}

/// Which removals should append a REMOVE stream record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeleteKind {
    /// `DeleteItem` and batch/transaction deletes skip logically expired rows.
    LiveOnly,
    /// TTL purge reports the removal of its expired row.
    Expired,
}

/// A validated table definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableDefinition {
    pub(crate) table_name: String,
    pub(crate) key_schema: KeySchema,
    pub(crate) attribute_definitions: Vec<crate::schema::AttributeDefinition>,
    pub(crate) global_secondary_indexes: Vec<SecondaryIndex>,
    pub(crate) local_secondary_indexes: Vec<SecondaryIndex>,
    pub(crate) billing_mode: BillingMode,
    pub(crate) stream_specification: Option<(bool, Option<StreamViewType>)>,
}

impl TableDefinition {
    /// Validate a create request and derive its effective definition.
    pub(crate) fn from_request(request: &CreateTableRequest) -> Result<Self> {
        let attribute_definitions = request.validate()?;
        if request.stream_specification.is_some() && cfg!(not(feature = "streams")) {
            return Err(Error::validation(
                "Streams are not available in this build of mock-ddb (enable the `streams` feature)",
            ));
        }
        Ok(Self {
            table_name: request.table_name.clone(),
            key_schema: request.key_schema.clone(),
            attribute_definitions,
            global_secondary_indexes: request.global_secondary_indexes.clone(),
            local_secondary_indexes: request.local_secondary_indexes.clone(),
            billing_mode: request.billing_mode,
            stream_specification: request.stream_specification.as_ref().map(|specification| {
                (specification.stream_enabled, specification.stream_view_type)
            }),
        })
    }

    /// Every index, global first.
    pub(crate) fn indexes(&self) -> impl Iterator<Item = &SecondaryIndex> {
        self.global_secondary_indexes
            .iter()
            .chain(&self.local_secondary_indexes)
    }

    /// Look up an index by name.
    pub(crate) fn index(&self, name: &str) -> Result<&SecondaryIndex> {
        self.indexes()
            .find(|index| index.index_name == name)
            .ok_or_else(|| {
                Error::validation(format!(
                    "The table does not have the specified index: {name}"
                ))
            })
    }
}

/// Which list of a table definition an index lives in.
///
/// Capacity is reported separately for the two, so the breakdown needs to know
/// where an index belongs; a name is unique across both lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IndexScope {
    /// A global secondary index: its own provisioned throughput.
    Global,
    /// A local secondary index: shares the table's partition.
    Local,
}

/// A table's TTL setting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Ttl {
    pub(crate) attribute_name: String,
    pub(crate) enabled: bool,
}

/// One table: definition, items, indexes, TTL and stream.
#[derive(Debug)]
pub(crate) struct Table {
    pub(crate) definition: TableDefinition,
    pub(crate) items: ItemMap,
    pub(crate) indexes: IndexMap,
    pub(crate) ttl: Option<Ttl>,
    #[cfg(feature = "streams")]
    pub(crate) stream: Option<StreamState>,
    pub(crate) created_at: SystemTime,
}

impl Table {
    pub(crate) fn new(
        definition: TableDefinition,
        created_at: SystemTime,
        sequence_origin: u64,
    ) -> Result<Self> {
        #[cfg(not(feature = "streams"))]
        let _ = sequence_origin;
        let mut indexes = IndexMap::new();
        for index in definition.indexes() {
            indexes.insert(index.index_name.clone(), BTreeMap::new());
        }
        #[cfg(feature = "streams")]
        let stream = match definition.stream_specification {
            Some((true, view_type)) => Some(StreamState::new(
                &definition.table_name,
                view_type.unwrap_or_default(),
                sequence_origin,
                created_at,
            )?),
            Some((false, _)) | None => None,
        };
        Ok(Self {
            definition,
            items: ItemMap::new(),
            indexes,
            ttl: None,
            #[cfg(feature = "streams")]
            stream,
            created_at,
        })
    }

    /// The table's key schema.
    pub(crate) fn key_schema(&self) -> &KeySchema {
        &self.definition.key_schema
    }

    /// Build the description `describe_table` returns.
    pub(crate) fn describe(&self, item_count: u64) -> TableDescription {
        TableDescription {
            table_name: self.definition.table_name.clone(),
            key_schema: self.definition.key_schema.clone(),
            attribute_definitions: self.definition.attribute_definitions.clone(),
            global_secondary_indexes: self.definition.global_secondary_indexes.clone(),
            local_secondary_indexes: self.definition.local_secondary_indexes.clone(),
            billing_mode: self.definition.billing_mode,
            table_status: "ACTIVE".to_string(),
            item_count,
            #[cfg(feature = "streams")]
            latest_stream_arn: self.stream.as_ref().map(|stream| stream.arn.clone()),
            #[cfg(not(feature = "streams"))]
            latest_stream_arn: None,
            creation_date_time: Some(self.created_at),
        }
    }

    /// Extract and validate the primary key from a whole item.
    pub(crate) fn item_key(&self, item: &Item) -> Result<ItemKey> {
        let partition = scalar_from(
            item,
            &self.definition.key_schema.partition_key,
            KeyExpectation::Item,
        )?;
        let sort = match &self.definition.key_schema.sort_key {
            Some(element) => Some(scalar_from(item, element, KeyExpectation::Item)?),
            None => None,
        };
        Ok((partition, sort))
    }

    /// Extract and validate the primary key from a key map, rejecting key maps
    /// that name attributes outside the schema.
    pub(crate) fn lookup_key(&self, key: &Item) -> Result<ItemKey> {
        for element in self.definition.key_schema.elements() {
            if !key.contains_key(&element.attribute_name) {
                return Err(Error::validation(format!(
                    "The provided key element does not match the schema: missing {}",
                    element.attribute_name
                )));
            }
        }
        for name in key.keys() {
            if !self.definition.key_schema.contains(name) {
                return Err(Error::validation(
                    "The provided key element does not match the schema: the key contains attributes that are not part of the table key",
                ));
            }
        }
        self.item_key(key)
    }

    /// Validate an item before it is stored: key, value types, index key types
    /// and the 400 KiB size limit.
    pub(crate) fn validate_item(&self, item: &Item) -> Result<()> {
        let _ = self.item_key(item)?;
        for (name, value) in item {
            validate_value(value, name)?;
        }
        for index in self.definition.indexes() {
            for element in index.key_schema.elements() {
                if let Some(value) = item.get(&element.attribute_name) {
                    let actual = Scalar::from_value(value).filter(|scalar| {
                        scalar.type_name() == element.key_type.as_str() && !scalar.is_empty()
                    });
                    if actual.is_none() {
                        return Err(Error::validation(format!(
                            "One or more parameter values were invalid: Type mismatch for Index Key {} Expected: {} Actual: {} IndexName: {}",
                            element.attribute_name,
                            element.key_type,
                            type_name(value),
                            index.index_name
                        )));
                    }
                }
            }
        }
        let size = item_size(item);
        if size > MAX_ITEM_SIZE {
            return Err(item_too_large(size));
        }
        Ok(())
    }

    /// Store an item, maintaining every index and the stream in one operation.
    pub(crate) fn store(
        &mut self,
        key: ItemKey,
        item: Item,
        kind: StoreKind,
        now_seconds: i64,
        now_time: SystemTime,
    ) -> Result<()> {
        let previous = self.items.get(&key).cloned();
        let previous_live = previous
            .as_ref()
            .filter(|existing| !self.is_expired(existing, now_seconds));

        #[cfg(feature = "streams")]
        {
            let event = match kind {
                StoreKind::Put | StoreKind::Seed if previous_live.is_none() => {
                    StreamEventName::Insert
                }
                StoreKind::Put | StoreKind::Seed | StoreKind::Update => StreamEventName::Modify,
            };
            let record = match kind {
                StoreKind::Seed => !self.is_expired(&item, now_seconds),
                StoreKind::Put | StoreKind::Update => true,
            };
            if record {
                self.record(event, previous_live, Some(&item), now_time)?;
            }
        }
        #[cfg(not(feature = "streams"))]
        {
            let _ = (kind, previous_live, now_time);
        }

        if let Some(previous) = previous.as_ref() {
            self.unindex(&key, previous);
        }
        let entries = index_entries(&self.definition, &item);
        for (index_name, index_key) in entries {
            self.indexes
                .entry(index_name)
                .or_default()
                .entry(index_key)
                .or_default()
                .insert(key.clone());
        }
        self.items.insert(key, item);
        Ok(())
    }

    /// Remove an item, maintaining every index and the requested stream event.
    pub(crate) fn delete(
        &mut self,
        key: &ItemKey,
        kind: DeleteKind,
        now_seconds: i64,
        now_time: SystemTime,
    ) -> Result<Option<Item>> {
        let Some(item) = self.items.get(key).cloned() else {
            return Ok(None);
        };
        let expired = self.is_expired(&item, now_seconds);

        #[cfg(feature = "streams")]
        if kind == DeleteKind::Expired || !expired {
            self.record(StreamEventName::Remove, Some(&item), None, now_time)?;
        }
        #[cfg(not(feature = "streams"))]
        {
            let _ = (kind, expired, now_time);
        }

        self.unindex(key, &item);
        self.items.remove(key);
        Ok(Some(item))
    }

    /// Drop an item's entries from every index.
    fn unindex(&mut self, key: &ItemKey, item: &Item) {
        let entries = index_entries(&self.definition, item);
        for (index_name, index_key) in entries {
            if let Some(index) = self.indexes.get_mut(&index_name) {
                if let Some(keys) = index.get_mut(&index_key) {
                    keys.remove(key);
                    if keys.is_empty() {
                        index.remove(&index_key);
                    }
                }
            }
        }
    }

    /// Whether an item's TTL attribute has expired at `now_seconds`.
    pub(crate) fn is_expired(&self, item: &Item, now_seconds: i64) -> bool {
        self.expiry(item)
            .is_some_and(|deadline| deadline <= now_seconds)
    }

    /// The epoch second at which an item expires, if it carries a TTL.
    pub(crate) fn expiry(&self, item: &Item) -> Option<i64> {
        let ttl = self.ttl.as_ref().filter(|ttl| ttl.enabled)?;
        let value = item.get(&ttl.attribute_name)?;
        let AttributeNumber::Valid(seconds) = attribute_number(value) else {
            return None;
        };
        Some(seconds)
    }

    /// Number of live (non-expired) items.
    pub(crate) fn live_count(&self, now_seconds: i64) -> u64 {
        self.items
            .values()
            .filter(|item| !self.is_expired(item, now_seconds))
            .count()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    /// Remove up to `MAX_TTL_PURGE_BATCH` expired items.
    pub(crate) fn purge_expired(
        &mut self,
        now_seconds: i64,
        now_time: SystemTime,
    ) -> Result<usize> {
        let expired: Vec<ItemKey> = self
            .items
            .iter()
            .filter(|(_, item)| self.is_expired(item, now_seconds))
            .map(|(key, _)| key.clone())
            .take(MAX_TTL_PURGE_BATCH)
            .collect();
        let mut removed = 0;
        for key in expired {
            if self
                .delete(&key, DeleteKind::Expired, now_seconds, now_time)?
                .is_some()
            {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Append a stream record for a mutation, when the table has a stream.
    #[cfg(feature = "streams")]
    pub(crate) fn record(
        &mut self,
        event_name: StreamEventName,
        old_item: Option<&Item>,
        new_item: Option<&Item>,
        now: SystemTime,
    ) -> Result<()> {
        let table_name = self.definition.table_name.clone();
        let key_schema = self.definition.key_schema.clone();
        if let Some(stream) = self.stream.as_mut() {
            stream.append(
                &table_name,
                &key_schema,
                event_name,
                old_item,
                new_item,
                now,
            )?;
        }
        Ok(())
    }

    /// Read an item's key attributes without rejecting extra attributes, which
    /// is what a pagination cursor containing index keys needs.
    pub(crate) fn cursor_key(&self, map: &Item) -> Result<Option<ItemKey>> {
        let partition_element = &self.definition.key_schema.partition_key;
        if !map.contains_key(&partition_element.attribute_name) {
            return Ok(None);
        }
        let partition = scalar_from(map, partition_element, KeyExpectation::Cursor)?;
        let sort = match &self.definition.key_schema.sort_key {
            Some(element) if map.contains_key(&element.attribute_name) => {
                Some(scalar_from(map, element, KeyExpectation::Cursor)?)
            }
            Some(_) => return Ok(None),
            None => None,
        };
        Ok(Some((partition, sort)))
    }

    /// Whether an item can belong to an item collection the service tracks:
    /// only a table with a local secondary index groups items by partition key
    /// for the collection-size metric.
    pub(crate) fn has_local_secondary_indexes(&self) -> bool {
        !self.definition.local_secondary_indexes.is_empty()
    }

    /// The name of every index that holds this item image. A sparse index an
    /// item does not qualify for is not charged.
    pub(crate) fn index_membership(&self, item: &Item) -> Vec<&str> {
        self.definition
            .indexes()
            .filter(|index| index_key(&index.key_schema, item).is_some())
            .map(|index| index.index_name.as_str())
            .collect()
    }

    /// Whether `name` is a global or a local index, or neither.
    pub(crate) fn index_scope(&self, name: &str) -> Option<IndexScope> {
        if self
            .definition
            .global_secondary_indexes
            .iter()
            .any(|index| index.index_name == name)
        {
            Some(IndexScope::Global)
        } else if self
            .definition
            .local_secondary_indexes
            .iter()
            .any(|index| index.index_name == name)
        {
            Some(IndexScope::Local)
        } else {
            None
        }
    }

    /// The bytes of every live item that shares `partition`: one item
    /// collection, which is what the size estimate covers.
    pub(crate) fn collection_size(&self, partition: &Scalar, now_seconds: i64) -> usize {
        self.items
            .iter()
            .filter(|((hash, _), _)| hash == partition)
            .filter(|(_, item)| !self.is_expired(item, now_seconds))
            .map(|(_, item)| item_size(item))
            .sum()
    }

    /// The projected attributes an index exposes, or `None` for "all".
    pub(crate) fn projected_attributes(
        &self,
        index_name: &str,
    ) -> Result<Option<BTreeSet<String>>> {
        let index = self.definition.index(index_name)?;
        let mut names: BTreeSet<String> = self
            .definition
            .key_schema
            .elements()
            .iter()
            .map(|element| element.attribute_name.clone())
            .collect();
        names.extend(
            index
                .key_schema
                .elements()
                .iter()
                .map(|element| element.attribute_name.clone()),
        );
        match &index.projection {
            Projection::All => Ok(None),
            Projection::KeysOnly => Ok(Some(names)),
            Projection::Include(extra) => {
                names.extend(extra.iter().cloned());
                Ok(Some(names))
            }
        }
    }
}

/// Where a key value is coming from, which decides the error wording.
#[derive(Debug, Clone, Copy)]
enum KeyExpectation {
    /// A whole item about to be written.
    Item,
    /// A pagination cursor, whose missing table key means "past this group"
    /// only when the caller explicitly handles that absence.
    Cursor,
}

/// Read one key attribute and check its type.
fn scalar_from(
    item: &Item,
    element: &KeySchemaElement,
    expectation: KeyExpectation,
) -> Result<Scalar> {
    let Some(value) = item.get(&element.attribute_name) else {
        return Err(Error::validation(match expectation {
            KeyExpectation::Item => format!(
                "One or more parameter values were invalid: Missing the key {} in the item",
                element.attribute_name
            ),
            KeyExpectation::Cursor => {
                "The provided starting key is invalid: it does not contain the table key attributes"
                    .to_string()
            }
        }));
    };
    let Some(scalar) = Scalar::from_value(value) else {
        return Err(Error::validation(format!(
            "One or more parameter values were invalid: Type mismatch for key {} expected: {} actual: {}",
            element.attribute_name,
            element.key_type,
            type_name(value)
        )));
    };
    if scalar.type_name() != element.key_type.as_str() {
        return Err(Error::validation(format!(
            "One or more parameter values were invalid: Type mismatch for key {} expected: {} actual: {}",
            element.attribute_name,
            element.key_type,
            scalar.type_name()
        )));
    }
    if scalar.is_empty() {
        return Err(Error::validation(format!(
            "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: {}",
            element.attribute_name
        )));
    }
    Ok(scalar)
}

/// The TTL attribute interpreted as a number.
enum AttributeNumber {
    Valid(i64),
    Invalid,
    Absent,
}

fn attribute_number(value: &aws_sdk_dynamodb::types::AttributeValue) -> AttributeNumber {
    match value {
        aws_sdk_dynamodb::types::AttributeValue::N(text) => {
            match crate::number::Number::parse(text).and_then(|number| number.as_i64()) {
                Some(seconds) => AttributeNumber::Valid(seconds),
                None => AttributeNumber::Invalid,
            }
        }
        _ => AttributeNumber::Absent,
    }
}

/// Compute the index entries an item belongs to.
fn index_entries(definition: &TableDefinition, item: &Item) -> Vec<(String, IndexKey)> {
    let mut entries = Vec::new();
    for index in definition.indexes() {
        if let Some(key) = index_key(&index.key_schema, item) {
            entries.push((index.index_name.clone(), key));
        }
    }
    entries
}

/// Compute an item's key within one index. `None` when the item does not carry
/// every index key attribute, which is how sparse indexes work.
pub(crate) fn index_key(schema: &KeySchema, item: &Item) -> Option<IndexKey> {
    let partition = item
        .get(&schema.partition_key.attribute_name)
        .and_then(Scalar::from_value)?;
    let sort = match &schema.sort_key {
        Some(element) => Some(
            item.get(&element.attribute_name)
                .and_then(Scalar::from_value)?,
        ),
        None => None,
    };
    Some((partition, sort))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{CreateTableRequest, KeySchema, KeyType};
    use proptest::prelude::*;

    fn table() -> Table {
        let request = CreateTableRequest::new(
            "users",
            KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::String),
        );
        let definition = TableDefinition::from_request(&request).unwrap();
        Table::new(definition, SystemTime::UNIX_EPOCH, 1).unwrap()
    }

    #[test]
    fn stores_and_discards_without_leaking_index_entries() {
        let mut table = Table::new(
            TableDefinition {
                global_secondary_indexes: vec![SecondaryIndex::new(
                    "by-email",
                    KeySchema::hash("email", KeyType::String),
                )],
                ..table().definition
            },
            SystemTime::UNIX_EPOCH,
            1,
        )
        .unwrap();

        let mut item = Item::new();
        item.insert("pk".into(), crate::s("u#1"));
        item.insert("sk".into(), crate::s("profile"));
        item.insert("email".into(), crate::s("a@example.com"));
        table.validate_item(&item).unwrap();
        let key = table.item_key(&item).unwrap();
        table
            .store(key.clone(), item, StoreKind::Put, 0, SystemTime::UNIX_EPOCH)
            .unwrap();
        assert_eq!(table.items.len(), 1);
        assert_eq!(
            table.indexes["by-email"].len(),
            1,
            "write must populate the index"
        );
        assert!(table
            .delete(&key, DeleteKind::LiveOnly, 0, SystemTime::UNIX_EPOCH)
            .unwrap()
            .is_some());
        assert!(
            table.indexes["by-email"].is_empty(),
            "discard must de-index"
        );
    }

    #[test]
    fn store_reads_and_replaces_the_current_index_entry() {
        let mut table = Table::new(
            TableDefinition {
                global_secondary_indexes: vec![SecondaryIndex::new(
                    "by-email",
                    KeySchema::hash("email", KeyType::String),
                )],
                ..table().definition
            },
            SystemTime::UNIX_EPOCH,
            1,
        )
        .unwrap();

        let mut first = item();
        first.insert("email".into(), crate::s("old@example.com"));
        table
            .store(
                table.item_key(&first).unwrap(),
                first,
                StoreKind::Put,
                0,
                SystemTime::UNIX_EPOCH,
            )
            .unwrap();

        let mut replacement = item();
        replacement.insert("email".into(), crate::s("new@example.com"));
        table
            .store(
                table.item_key(&replacement).unwrap(),
                replacement,
                StoreKind::Put,
                0,
                SystemTime::UNIX_EPOCH,
            )
            .unwrap();

        let old_key = (Scalar::S("old@example.com".into()), None);
        let new_key = (Scalar::S("new@example.com".into()), None);
        assert!(!table.indexes["by-email"].contains_key(&old_key));
        assert_eq!(
            table.indexes["by-email"][&new_key].len(),
            1,
            "the replacement must own exactly its current index entry"
        );
    }

    #[test]
    fn purge_is_bounded_and_counts_only_deleted_rows() {
        let mut table = table();
        table.ttl = Some(Ttl {
            attribute_name: "expires_at".into(),
            enabled: true,
        });
        for id in 0..(MAX_TTL_PURGE_BATCH + 2) {
            let mut row = item();
            row.insert("pk".into(), crate::s(format!("u#{id:04}")));
            row.insert("sk".into(), crate::s("profile"));
            row.insert("expires_at".into(), crate::n("10"));
            table
                .store(
                    table.item_key(&row).unwrap(),
                    row,
                    StoreKind::Seed,
                    0,
                    SystemTime::UNIX_EPOCH,
                )
                .unwrap();
        }

        assert_eq!(
            table.purge_expired(11, SystemTime::UNIX_EPOCH).unwrap(),
            MAX_TTL_PURGE_BATCH
        );
        assert_eq!(table.items.len(), 2);
        assert_eq!(table.purge_expired(11, SystemTime::UNIX_EPOCH).unwrap(), 2);
        assert!(table.items.is_empty());
        assert_eq!(table.purge_expired(11, SystemTime::UNIX_EPOCH).unwrap(), 0);
    }

    proptest! {
        #[test]
        fn overwrites_keep_exactly_one_current_index_entry(
            emails in prop::collection::vec("[a-z]{1,12}@example\\.com", 1..20)
        ) {
            let mut table = Table::new(
                TableDefinition {
                    global_secondary_indexes: vec![SecondaryIndex::new(
                        "by-email",
                        KeySchema::hash("email", KeyType::String),
                    )],
                    ..table().definition
                },
                SystemTime::UNIX_EPOCH,
                1,
            ).unwrap();

            for email in emails {
                let mut row = item();
                row.insert("email".into(), crate::s(email.clone()));
                let key = table.item_key(&row).unwrap();
                table
                    .store(key.clone(), row, StoreKind::Put, 0, SystemTime::UNIX_EPOCH)
                    .unwrap();

                prop_assert_eq!(table.indexes["by-email"].len(), 1);
                let index_key = (Scalar::S(email), None);
                prop_assert_eq!(
                    table.indexes["by-email"][&index_key].iter().cloned().collect::<Vec<_>>(),
                    vec![key],
                );
            }
        }
    }

    fn item() -> Item {
        let mut item = Item::new();
        item.insert("pk".into(), crate::s("u#1"));
        item.insert("sk".into(), crate::s("profile"));
        item
    }

    #[test]
    fn cursor_keys_validate_type_and_empty_values_like_item_keys() {
        let table = table();

        let typed = table
            .cursor_key(&crate::item([("pk", crate::s("a")), ("sk", crate::n("1"))]))
            .unwrap_err();
        assert!(
            typed.to_string().contains("Type mismatch for key sk"),
            "unexpected error: {typed}"
        );

        let empty = table
            .cursor_key(&crate::item([("pk", crate::s("")), ("sk", crate::s("a"))]))
            .unwrap_err();
        assert!(
            empty
                .to_string()
                .contains("cannot contain an empty string value"),
            "unexpected error: {empty}"
        );

        assert_eq!(
            table
                .cursor_key(&crate::item([("pk", crate::s("a")), ("sk", crate::s("b"))]))
                .unwrap(),
            Some((Scalar::S("a".to_string()), Some(Scalar::S("b".to_string()))))
        );
        assert_eq!(
            table
                .cursor_key(&crate::item([("pk", crate::s("a"))]))
                .unwrap(),
            None,
            "a cursor without the full table key names a group end"
        );
    }

    #[test]
    fn rejects_oversized_items_and_bad_keys() {
        let table = table();
        let mut item = Item::new();
        item.insert("pk".into(), crate::s("a"));
        item.insert("sk".into(), crate::s("b"));
        item.insert("blob".into(), crate::s("x".repeat(MAX_ITEM_SIZE)));
        assert!(table.validate_item(&item).is_err());

        let mut missing = Item::new();
        missing.insert("pk".into(), crate::s("a"));
        assert!(table.validate_item(&missing).is_err());

        let mut wrong_type = Item::new();
        wrong_type.insert("pk".into(), crate::n("1"));
        wrong_type.insert("sk".into(), crate::s("b"));
        assert!(table.validate_item(&wrong_type).is_err());
    }

    #[test]
    fn ttl_expiry_reads_numbers_only() {
        let mut table = table();
        table.ttl = Some(Ttl {
            attribute_name: "expires_at".into(),
            enabled: true,
        });
        let mut item = Item::new();
        item.insert("pk".into(), crate::s("a"));
        item.insert("expires_at".into(), crate::n("100"));
        assert_eq!(table.expiry(&item), Some(100));
        assert!(table.is_expired(&item, 100));
        assert!(!table.is_expired(&item, 99));
        item.insert("expires_at".into(), crate::s("not a number"));
        assert_eq!(table.expiry(&item), None);
        assert!(!table.is_expired(&item, 1_000));
    }
}
