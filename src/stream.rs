//! DynamoDB Streams emulation (behind the `streams` feature).
//!
//! A stream is a single ordered log per table. Every successful mutation
//! appends one record; a cancelled transaction appends none. The log is
//! readable two ways: the short [`MockDdb::get_records`](crate::MockDdb::get_records)
//! helper, and the wire-shaped API in [`StreamsApi`] — `ListStreams`,
//! `DescribeStream`, `GetShardIterator` and `GetRecords`, with the shard
//! iterators and paging the DynamoDB Streams Kinesis Adapter speaks.
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
    /// When the change was appended, in seconds since the Unix epoch.
    ///
    /// The service rounds `ApproximateCreationDateTime` to whole seconds; so
    /// does the mock, using the wall clock, because the stream log is written
    /// outside the TTL clock path.
    pub approximate_creation_date_time: i64,
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
        let epoch_millis = date_time_suffix();
        self.records.push(StreamRecord {
            event_id: format!("{epoch_millis}-{sequence}"),
            event_name,
            table_name: table_name.to_string(),
            keys: key_map(key_schema, new_item.or(old_item)),
            sequence_number: format!("{sequence:021}"),
            approximate_creation_date_time: i64::try_from(epoch_millis / 1_000).unwrap_or(i64::MAX),
            new_image: self.view_type.new_image(new_item),
            old_image: self.view_type.old_image(old_item),
        });
    }

    /// The shard this stream exposes. The mock gives every stream one open
    /// shard, so the id is constant per stream.
    pub(crate) const SHARD_ID: &'static str = "shardId-000000000000-00000000";

    /// The label the service derives from the stream's creation time.
    pub(crate) fn label(&self) -> &str {
        self.arn
            .rsplit_once('/')
            .map_or(self.arn.as_str(), |(_, label)| label)
    }

    /// The view type this stream records.
    pub(crate) fn view_type(&self) -> StreamViewType {
        self.view_type
    }

    /// The log position of the record with `sequence`.
    ///
    /// The number is parsed, so an unpadded `"7"` finds the record whose own
    /// sequence number is `"000000000000000000007"`.
    pub(crate) fn position_of(&self, sequence: &str) -> Option<usize> {
        let wanted = sequence.parse::<u64>().ok()?;
        self.records
            .iter()
            .position(|record| record.sequence_number.parse::<u64>() == Ok(wanted))
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

// ---------------------------------------------------------------------------
// The DynamoDB Streams API
// ---------------------------------------------------------------------------

/// Maximum number of records one `GetRecords` call returns; the service's
/// `Limit` ceiling.
pub const MAX_RECORDS_PER_GET: u32 = 1_000;

/// Maximum number of streams one `ListStreams` call returns.
pub const MAX_STREAMS_PER_LIST: u32 = 100;

/// Maximum number of shards one `DescribeStream` call returns.
pub const MAX_SHARDS_PER_DESCRIBE: u32 = 100;

/// The region every mock stream ARN names, and the `awsRegion` its records
/// carry.
pub const STREAM_REGION: &str = "ddb-local";

/// The creation time every mock stream reports, in seconds since the Unix
/// epoch: the `2026-01-01T00:00:00.000` the stream ARN ends with.
pub const STREAM_CREATION_EPOCH_SECONDS: i64 = 1_767_225_600;

/// Where [`GetShardIteratorRequest`] starts reading.
///
/// ```
/// use mock_ddb::ShardIteratorType;
///
/// assert_eq!(ShardIteratorType::TrimHorizon.as_str(), "TRIM_HORIZON");
/// assert!(ShardIteratorType::AtSequenceNumber.requires_sequence_number());
/// assert!(!ShardIteratorType::Latest.requires_sequence_number());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShardIteratorType {
    /// Start at the oldest record the shard still holds.
    TrimHorizon,
    /// Start after the newest record, so the next append is the first thing
    /// read.
    Latest,
    /// Start at the record with the given sequence number, whose first read
    /// returns that record again.
    AtSequenceNumber,
    /// Start after the record with the given sequence number.
    AfterSequenceNumber,
}

impl ShardIteratorType {
    /// The name the service uses: `TRIM_HORIZON`, `LATEST`,
    /// `AT_SEQUENCE_NUMBER`, `AFTER_SEQUENCE_NUMBER`.
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::ShardIteratorType;
    ///
    /// let types = [
    ///     ShardIteratorType::TrimHorizon,
    ///     ShardIteratorType::Latest,
    ///     ShardIteratorType::AtSequenceNumber,
    ///     ShardIteratorType::AfterSequenceNumber,
    /// ];
    /// let names: Vec<&str> = types.iter().map(|kind| kind.as_str()).collect();
    /// assert_eq!(
    ///     names,
    ///     ["TRIM_HORIZON", "LATEST", "AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER"]
    /// );
    /// ```
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TrimHorizon => "TRIM_HORIZON",
            Self::Latest => "LATEST",
            Self::AtSequenceNumber => "AT_SEQUENCE_NUMBER",
            Self::AfterSequenceNumber => "AFTER_SEQUENCE_NUMBER",
        }
    }

    /// Whether the type needs a `sequence_number` in the request.
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::ShardIteratorType;
    ///
    /// assert!(ShardIteratorType::AfterSequenceNumber.requires_sequence_number());
    /// assert!(!ShardIteratorType::TrimHorizon.requires_sequence_number());
    /// ```
    pub fn requires_sequence_number(self) -> bool {
        matches!(self, Self::AtSequenceNumber | Self::AfterSequenceNumber)
    }
}

/// `ListStreams` request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListStreamsRequest {
    /// Continue after this stream ARN.
    pub exclusive_start_stream_arn: Option<String>,
    /// Maximum number of streams to return, 1 to [`MAX_STREAMS_PER_LIST`].
    pub limit: Option<u32>,
}

impl ListStreamsRequest {
    /// A request for the first page of every stream.
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::ListStreamsRequest;
    ///
    /// let request = ListStreamsRequest::new().with_limit(10);
    /// assert_eq!(request.limit, Some(10));
    /// assert!(request.exclusive_start_stream_arn.is_none());
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Continue after `stream_arn`.
    #[must_use]
    pub fn with_exclusive_start_stream_arn(mut self, stream_arn: impl Into<String>) -> Self {
        self.exclusive_start_stream_arn = Some(stream_arn.into());
        self
    }

    /// Cap the page size.
    #[must_use]
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// One stream as `ListStreams` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSummary {
    /// The stream's ARN.
    pub stream_arn: String,
    /// The stream's label, taken from its creation time.
    pub stream_label: String,
    /// The table the stream belongs to.
    pub table_name: String,
    /// Always `ENABLED`: the mock has no `ENABLING` lifecycle.
    pub stream_status: String,
}

/// `ListStreams` output.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListStreamsOutput {
    /// The page of streams, sorted by ARN.
    pub streams: Vec<StreamSummary>,
    /// Cursor for the next page, when the limit stopped the listing early.
    pub last_evaluated_stream_arn: Option<String>,
}

/// `DescribeStream` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeStreamRequest {
    /// The stream to describe.
    pub stream_arn: String,
    /// Continue after this shard id.
    pub exclusive_start_shard_id: Option<String>,
    /// Maximum number of shards to return, 1 to [`MAX_SHARDS_PER_DESCRIBE`].
    pub limit: Option<u32>,
}

impl DescribeStreamRequest {
    /// A request for the description of `stream_arn`.
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::DescribeStreamRequest;
    ///
    /// let request = DescribeStreamRequest::new("arn:aws:dynamodb:ddb-local:000000000000:table/users/stream/label");
    /// assert_eq!(request.limit, None);
    /// ```
    pub fn new(stream_arn: impl Into<String>) -> Self {
        Self {
            stream_arn: stream_arn.into(),
            exclusive_start_shard_id: None,
            limit: None,
        }
    }

    /// Continue after `shard_id`.
    #[must_use]
    pub fn with_exclusive_start_shard_id(mut self, shard_id: impl Into<String>) -> Self {
        self.exclusive_start_shard_id = Some(shard_id.into());
        self
    }

    /// Cap the page size.
    #[must_use]
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// The sequence numbers a shard covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceNumberRange {
    /// The first sequence number the shard can return.
    pub starting_sequence_number: String,
    /// The last one, when the shard is closed. The mock's shard never closes.
    pub ending_sequence_number: Option<String>,
}

/// One shard of a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shard {
    /// The shard's id, stable per stream.
    pub shard_id: String,
    /// Set when the shard was split from another one; always `None` here.
    pub parent_shard_id: Option<String>,
    /// The sequence numbers this shard covers.
    pub sequence_number_range: SequenceNumberRange,
}

/// What `DescribeStream` reports about one stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamDescription {
    /// The stream's ARN.
    pub stream_arn: String,
    /// The stream's label.
    pub stream_label: String,
    /// Always `ENABLED`.
    pub stream_status: String,
    /// Which images the stream records.
    pub stream_view_type: StreamViewType,
    /// Creation time in seconds since the Unix epoch.
    pub creation_request_date_time: i64,
    /// The table the stream belongs to.
    pub table_name: String,
    /// The page of shards. The mock gives every stream exactly one.
    pub shards: Vec<Shard>,
    /// Cursor for the next page, when the limit stopped the listing early.
    pub last_evaluated_shard_id: Option<String>,
}

/// `DescribeStream` output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeStreamOutput {
    /// The stream's description.
    pub stream_description: StreamDescription,
}

/// `GetShardIterator` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetShardIteratorRequest {
    /// The stream to read.
    pub stream_arn: String,
    /// The shard to read, from [`DescribeStream`](DescribeStreamRequest).
    pub shard_id: String,
    /// Where to start.
    pub shard_iterator_type: ShardIteratorType,
    /// The anchor for `AT_SEQUENCE_NUMBER` and `AFTER_SEQUENCE_NUMBER`;
    /// rejected for the other two types.
    pub sequence_number: Option<String>,
}

impl GetShardIteratorRequest {
    /// A request for an iterator of `shard_iterator_type` at the start of
    /// `shard_id`.
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::{GetShardIteratorRequest, ShardIteratorType};
    ///
    /// let request = GetShardIteratorRequest::new(
    ///     "arn:aws:dynamodb:ddb-local:000000000000:table/users/stream/label",
    ///     "shardId-000000000000-00000000",
    ///     ShardIteratorType::Latest,
    /// );
    /// assert_eq!(request.shard_iterator_type.as_str(), "LATEST");
    /// ```
    pub fn new(
        stream_arn: impl Into<String>,
        shard_id: impl Into<String>,
        shard_iterator_type: ShardIteratorType,
    ) -> Self {
        Self {
            stream_arn: stream_arn.into(),
            shard_id: shard_id.into(),
            shard_iterator_type,
            sequence_number: None,
        }
    }

    /// Anchor the iterator at `sequence_number`.
    #[must_use]
    pub fn with_sequence_number(mut self, sequence_number: impl Into<String>) -> Self {
        self.sequence_number = Some(sequence_number.into());
        self
    }
}

/// `GetShardIterator` output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetShardIteratorOutput {
    /// The opaque token [`GetRecordsRequest`] takes. Feed it back unchanged;
    /// its shape is not part of the API.
    pub shard_iterator: String,
}

/// `GetRecords` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetRecordsRequest {
    /// A token from [`GetShardIteratorOutput`] or an earlier
    /// [`GetRecordsOutput::next_shard_iterator`].
    pub shard_iterator: String,
    /// Maximum number of records to return, 1 to [`MAX_RECORDS_PER_GET`].
    pub limit: Option<u32>,
}

impl GetRecordsRequest {
    /// Read from `shard_iterator` with the default page size.
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::GetRecordsRequest;
    ///
    /// let request = GetRecordsRequest::new("opaque-token").with_limit(25);
    /// assert_eq!(request.limit, Some(25));
    /// ```
    pub fn new(shard_iterator: impl Into<String>) -> Self {
        Self {
            shard_iterator: shard_iterator.into(),
            limit: None,
        }
    }

    /// Cap the page size.
    #[must_use]
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// The `dynamodb` member of one stream [`StreamsRecord`].
///
/// ```
/// use mock_ddb::{item, s, StreamsRecordData, StreamViewType};
///
/// let data = StreamsRecordData {
///     approximate_creation_date_time: 1_767_225_600,
///     keys: item([("pk", s("u#1"))]),
///     new_image: Some(item([("pk", s("u#1"))])),
///     old_image: None,
///     sequence_number: "000000000000000000001".to_string(),
///     size_bytes: 12,
///     stream_view_type: StreamViewType::NewImage,
/// };
/// assert_eq!(data.stream_view_type.as_str(), "NEW_IMAGE");
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct StreamsRecordData {
    /// When the change happened, in seconds since the Unix epoch.
    pub approximate_creation_date_time: i64,
    /// The item's key attributes.
    pub keys: Item,
    /// The item after the write, subject to the stream's view type.
    pub new_image: Option<Item>,
    /// The item before the write, subject to the stream's view type.
    pub old_image: Option<Item>,
    /// The record's sequence number, zero-padded to sort lexicographically.
    pub sequence_number: String,
    /// Stored bytes of the keys and the images this record carries. The
    /// service counts the item's encoded size; the mock sums attribute names
    /// and stored values, which is the same number up to the service's
    /// per-record overhead.
    pub size_bytes: u64,
    /// Which images the stream recorded.
    pub stream_view_type: StreamViewType,
}

/// One record as the Streams API (and therefore the Kinesis adapter) returns
/// it.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamsRecord {
    /// Event identifier, unique within the process.
    pub event_id: String,
    /// What happened.
    pub event_name: StreamEventName,
    /// Always `aws:dynamodb`.
    pub event_source: String,
    /// The record format version, `1.1`.
    pub event_version: String,
    /// The region the stream lives in, `ddb-local`.
    pub aws_region: String,
    /// The change itself.
    pub dynamodb: Option<StreamsRecordData>,
}

/// `GetRecords` output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GetRecordsOutput {
    /// The page of records, in write order.
    pub records: Vec<StreamsRecord>,
    /// The token to pass to the next call. Always `Some`, because the mock's
    /// shard never closes.
    pub next_shard_iterator: Option<String>,
}

/// The DynamoDB Streams API over a [`MockDdb`](crate::MockDdb), reached through
/// [`MockDdb::streams`](crate::MockDdb::streams).
///
/// The four operations are `ListStreams`, `DescribeStream`,
/// `GetShardIterator` and `GetRecords`; between them they are the whole wire
/// surface the DynamoDB Streams Kinesis Adapter speaks. The borrow keeps the
/// handle tied to the mock it reads from.
///
/// ```
/// use mock_ddb::{s, CreateTableRequest, KeySchema, KeyType, ListStreamsRequest, MockDdb, StreamViewType};
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
/// let page = mock.streams().list_streams(ListStreamsRequest::new()).await.unwrap();
/// assert_eq!(page.streams[0].table_name, "users");
/// # }
/// ```
#[derive(Debug)]
pub struct StreamsApi<'a> {
    pub(crate) mock: &'a crate::MockDdb,
}

/// Build the wire shape of one log entry.
pub(crate) fn wire_record(record: &StreamRecord, view_type: StreamViewType) -> StreamsRecord {
    StreamsRecord {
        event_id: record.event_id.clone(),
        event_name: record.event_name,
        event_source: "aws:dynamodb".to_string(),
        event_version: "1.1".to_string(),
        aws_region: STREAM_REGION.to_string(),
        dynamodb: Some(StreamsRecordData {
            approximate_creation_date_time: record.approximate_creation_date_time,
            keys: record.keys.clone(),
            new_image: record.new_image.clone(),
            old_image: record.old_image.clone(),
            sequence_number: record.sequence_number.clone(),
            size_bytes: record_size(record),
            stream_view_type: view_type,
        }),
    }
}

/// Stored bytes of a record: attribute names plus the bytes of every value it
/// carries.
fn record_size(record: &StreamRecord) -> u64 {
    let mut total = item_size(&record.keys);
    if let Some(image) = &record.new_image {
        total = total.saturating_add(item_size(image));
    }
    if let Some(image) = &record.old_image {
        total = total.saturating_add(item_size(image));
    }
    total
}

/// Attribute names plus stored value bytes of one item.
fn item_size(item: &Item) -> u64 {
    item.iter()
        .map(|(name, value)| byte_len(name).saturating_add(value_size(value)))
        .fold(0, u64::saturating_add)
}

/// The bytes one attribute value stores.
fn value_size(value: &aws_sdk_dynamodb::types::AttributeValue) -> u64 {
    use aws_sdk_dynamodb::types::AttributeValue;
    match value {
        AttributeValue::S(text) => byte_len(text),
        AttributeValue::N(number) => byte_len(number),
        AttributeValue::B(blob) => byte_len(blob.as_ref()),
        AttributeValue::Bool(_) | AttributeValue::Null(_) => 1,
        AttributeValue::Ss(set) | AttributeValue::Ns(set) => byte_len_sum(set.iter()),
        AttributeValue::Bs(set) => set
            .iter()
            .fold(0, |sum, blob| sum.saturating_add(byte_len(blob.as_ref()))),
        AttributeValue::L(list) => list.iter().map(value_size).fold(0, u64::saturating_add),
        AttributeValue::M(map) => item_size(map),
        // The SDK's union is `non_exhaustive`; a variant this build does not
        // know contributes nothing rather than failing the call.
        _ => 0,
    }
}

fn byte_len(bytes: impl AsRef<[u8]>) -> u64 {
    u64::try_from(bytes.as_ref().len()).unwrap_or(u64::MAX)
}

fn byte_len_sum<'b>(values: impl Iterator<Item = &'b String>) -> u64 {
    values.fold(0, |sum, value| sum.saturating_add(byte_len(value)))
}
