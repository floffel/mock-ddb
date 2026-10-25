//! DynamoDB Streams emulation (behind the `streams` feature).
//!
//! A stream is a single ordered log per table. Every successful mutation
//! appends one record; a cancelled transaction appends none. The mock exposes
//! the log through [`MockDdb::get_records`](crate::MockDdb::get_records) rather
//! than modelling the `GetRecords` HTTP API and its shard iterators.
//!
//! ```
//! use mock_ddb::{s, CreateTableRequest, KeySchema, KeyType, MockDdb, PutItemRequest, StreamViewType, item};
//!
//! # #[tokio::main]
//! # async fn main() {
//! let mock = MockDdb::new();
//! mock.create_table(
//!     CreateTableRequest::new("users", KeySchema::hash("pk", KeyType::String))
//!         .with_stream(StreamViewType::NewAndOldImages),
//! )
//! .await
//! .unwrap();
//! mock.put_item(PutItemRequest::new("users", item([("pk", s("u1"))])))
//!     .await
//!     .unwrap();
//!
//! let arn = mock.describe_table("users").await.unwrap().latest_stream_arn.unwrap();
//! let records = mock.get_records(&arn).await.unwrap();
//! assert_eq!(records.len(), 1);
//! # }
//! ```

use std::collections::HashMap;

use crate::schema::{KeySchema, StreamViewType};
use crate::Item;

/// What a stream record reports happened to an item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamEventName {
    /// The item did not exist before the write.
    Insert,
    /// The item existed and was overwritten or updated.
    Modify,
    /// The item was deleted.
    Remove,
}

impl StreamEventName {
    /// The name the service uses: `INSERT`, `MODIFY`, `REMOVE`.
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::StreamEventName;
    ///
    /// assert_eq!(StreamEventName::Insert.as_str(), "INSERT");
    /// assert_eq!(StreamEventName::Modify.as_str(), "MODIFY");
    /// assert_eq!(StreamEventName::Remove.as_str(), "REMOVE");
    /// ```
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Modify => "MODIFY",
            Self::Remove => "REMOVE",
        }
    }
}

/// One entry of a table's stream.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamRecord {
    /// Event identifier, unique within the process.
    pub event_id: String,
    /// What happened.
    pub event_name: StreamEventName,
    /// Table the change happened on.
    pub table_name: String,
    /// The item's key attributes, as the service reports them.
    pub keys: Item,
    /// Monotonically increasing sequence number, zero-padded so it sorts
    /// lexicographically as well as numerically.
    pub sequence_number: String,
    /// The item after the write, subject to the stream's view type.
    pub new_image: Option<Item>,
    /// The item before the write, subject to the stream's view type.
    pub old_image: Option<Item>,
}

/// The mutable state of one table's stream.
#[derive(Debug)]
pub(crate) struct StreamState {
    pub(crate) arn: String,
    pub(crate) records: Vec<StreamRecord>,
    pub(crate) next_sequence: u64,
    pub(crate) view_type: StreamViewType,
}

impl StreamState {
    pub(crate) fn new(table_name: &str, view_type: StreamViewType, sequence_origin: u64) -> Self {
        Self {
            arn: format!(
                "arn:aws:dynamodb:ddb-local:000000000000:table/{table_name}/stream/2026-01-01T00:00:00.000"
            ),
            records: Vec::new(),
            next_sequence: sequence_origin,
            view_type,
        }
    }

    /// Append one record, filtering the images by the configured view type.
    pub(crate) fn append(
        &mut self,
        table_name: &str,
        key_schema: &KeySchema,
        event_name: StreamEventName,
        old_item: Option<&Item>,
        new_item: Option<&Item>,
    ) {
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.records.push(StreamRecord {
            event_id: format!("{}-{sequence}", date_time_suffix()),
            event_name,
            table_name: table_name.to_string(),
            keys: key_map(key_schema, new_item.or(old_item)),
            sequence_number: format!("{sequence:021}"),
            new_image: self.view_type.new_image(new_item),
            old_image: self.view_type.old_image(old_item),
        });
    }
}

/// Milliseconds since the Unix epoch, used to make event ids look like the
/// service's.
fn date_time_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

impl StreamViewType {
    fn new_image(self, item: Option<&Item>) -> Option<Item> {
        match self {
            Self::NewImage | Self::NewAndOldImages => item.cloned(),
            Self::OldImage | Self::KeysOnly => None,
        }
    }

    fn old_image(self, item: Option<&Item>) -> Option<Item> {
        match self {
            Self::OldImage | Self::NewAndOldImages => item.cloned(),
            Self::NewImage | Self::KeysOnly => None,
        }
    }
}

/// Extract the key attributes from an item, as a stream record reports them.
fn key_map(key_schema: &KeySchema, item: Option<&Item>) -> Item {
    let mut keys = HashMap::new();
    if let Some(item) = item {
        for element in key_schema.elements() {
            if let Some(value) = item.get(&element.attribute_name) {
                keys.insert(element.attribute_name.clone(), value.clone());
            }
        }
    }
    keys
}
