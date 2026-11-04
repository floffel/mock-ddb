# Changelog

All notable changes to this crate are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the crate
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `ConsumedCapacity` and `ItemCollectionMetrics` on every data-plane response.
  `PutItemRequest`, `GetItemRequest`, `DeleteItemRequest`, `UpdateItemRequest`,
  `QueryRequest`, `ScanRequest`, `BatchWriteItemRequest`, `BatchGetItemRequest`,
  `TransactWriteItemsRequest` and `TransactGetItemsRequest` take
  `return_consumed_capacity` (`NONE`, `TOTAL`, `INDEXES`) and the writes also
  take `return_item_collection_metrics` (`NONE`, `SIZE`), and the matching
  outputs carry the result. The units are computed from the item sizes the mock
  actually stores — one read unit per started 4 KiB of an item read, one write
  unit per started 1 KiB of an item written, the larger image of a replace, one
  unit minimum per operation and per key — and `INDEXES` splits them per table,
  per local and per global secondary index, charging a sparse index only for the
  items that reach it. `ItemCollectionMetrics` reports the collection key and a
  whole-gibibyte size estimate for tables with a local secondary index, which is
  the service's own rule. `read_units`, `write_units` and `size_estimate_range`
  expose the arithmetic, `READ_UNIT_BYTES`, `WRITE_UNIT_BYTES` and
  `COLLECTION_ESTIMATE_STEP_BYTES` the limits. Nothing is enforced: there is
  still no provisioned throughput, no throttling and no 10 GB collection limit.
  `MockDdb::transact_write_items` now returns `TransactWriteItemsOutput` instead
  of `()` — a statement that discarded the value keeps compiling.
- The DynamoDB Streams wire API, reached through `MockDdb::streams()`:
  `ListStreams`, `DescribeStream`, `GetShardIterator` and `GetRecords`, with
  paging cursors, the service's limits (`1..=100` streams and shards,
  `1..=1000` records) validated before anything is read, opaque shard iterators
  for `TRIM_HORIZON`, `LATEST`, `AT_SEQUENCE_NUMBER` and
  `AFTER_SEQUENCE_NUMBER`, and records in the Kinesis adapter's shape
  (`eventSource`, `eventVersion`, `awsRegion`, and a `dynamodb` member carrying
  keys, images, sequence number and size). `StreamRecord` also reports
  `approximate_creation_date_time` now. The in-process `get_records`/
  `list_streams` helpers stay as they were.
- An `Internal` failure can now carry the lower-layer error that caused it.
  `Error::internal_from(message, cause)` stores the cause as
  `std::error::Error::source`, so a caller walks from the rendered message to the
  real failure instead of parsing a flattened string; `Error::internal(message)`
  still builds the variant without a cause and its chain ends there. `PartialEq`
  is now hand-written (a boxed cause is neither `Clone`-comparable nor
  `PartialEq`) and compares the rendered cause alongside the message, so
  `Error` keeps the structural equality callers already relied on.

### Fixed

- `MockClock::epoch_seconds` reports the real negative second before the Unix
  epoch instead of the fixed sentinel `-1`, so a clock set an hour early no
  longer claims to be one second early. `-1` is itself a legal timestamp
  (1969-12-31T23:59:59Z), which made the two indistinguishable, and because
  TTL expiry compares a stored deadline against that value, items with a TTL
  never expired while the clock ran before the epoch: the second stuck at `-1`
  however far `advance` moved the instant, so no deadline was ever crossed. The
  value is now derived from `duration_since` on both sides of the epoch,
  truncated in magnitude (a fraction of a second on either side reports `0`)
  and saturating at `i64::MIN` / `i64::MAX` at the far ends of the clock.
- `Error::is_retryable` now follows the error code the failure reports instead
  of a blanket `false`. An `Internal` failure — `InternalServerError`, the 5xx
  class a retry layer and the AWS SDK retry by default — answered "terminal", so
  a test could assert both "we do not retry 5xx blindly" and "5xx is retried"
  against the same mock. `CancellationReasonCode::ThrottlingError`, documented
  as "the request exceeded the table's throughput", was non-retryable for the
  same reason and now is, both on its own through the new
  `CancellationReasonCode::is_retryable` and inside a cancelled transaction that
  reports nothing but throttled members. A cancelled transaction still needs
  every member to be retryable: one rejected condition, malformed member or
  exhausted item collection keeps the whole answer terminal, and an empty reason
  list stays terminal too. `code` and `is_retryable` now read one classification
  table, so a code that names a retryable class and a verdict that calls it
  terminal can no longer be written side by side.
- A `ConditionalCheckFailedException` now renders the table it was raised for:
  every variant formatted as its bare `message`, so a logged rejection read
  "condition `attribute_not_exists(pk)` evaluated to false" with no hint which
  table refused the write. `Display` is now "The conditional request failed for
  table users: condition `attribute_not_exists(pk)` evaluated to false".
- `Error::resource` and `Error::table` expose the identifiers the variants
  already carried. `ResourceNotFound.resource` was written by the crate and read
  by nothing — not by `Display`, not by an accessor — so it was a write-only
  field a test could not assert against; the new accessors return `None` for
  every variant that carries no such identifier.

- The document path in an update error message is now unambiguous and bounded.
  `render` separated segments only while the rendered text was non-empty, so a
  path whose first name was empty swallowed the separator and read exactly like
  the top-level attribute `b`; an attribute actually named `a.b` read exactly
  like the nested path `a` → `b`, and a caller-supplied name travelled into the
  message verbatim with no length limit and no escaping. Segments are now
  separated by position, a name that is empty or carries `.`, `[`, `]`, a quote,
  a backslash or a control character is quoted and escaped (`"a.b"`), and the
  rendering stops after 256 bytes with an ellipsis.
- An undefined `#alias` or `:value` placeholder is now reported even when
  `AND`/`OR` short-circuiting never evaluates the branch that names it.
  `ConditionExpression::evaluate` checks both maps against the whole expression
  before it reads anything, the way the service validates a request statically:
  `attribute_not_exists(pk) OR #typo = :v` used to succeed against the mock and
  fail with a `ValidationException` for every call against DynamoDB. The lazy
  resolution inside evaluation stays, so the common case still allocates
  nothing.
- A wrongly typed operand of `begins_with` or `contains` is now a
  `ValidationException` instead of a condition that evaluates to false.
  `begins_with(count, :s)`, `begins_with(blob, :s)`, `begins_with(tags, :s)` and
  `contains(count, :s)` are all requests the service rejects with "Incorrect
  operand type for operator or function"; the mock answered "condition not met",
  so a test asserting that a conditional write is refused passed for the wrong
  reason and the same expression failed against DynamoDB. A *missing* attribute
  still satisfies nothing and stays false — there is no operand to compare — and
  `contains` still accepts every needle a list can hold.
- `UpdateExpression::apply` now takes the table's `KeySchema` and validates
  against it, instead of substituting a defaulted `pk`-only schema: an
  expression that rewrites a key attribute of the table it belongs to is
  rejected wherever it is applied, not only through `MockDdb`. The previous
  signature accepted `SET sk = :v` on a `pk`/`sk` table and let the write reach
  the item, so an expression a test called valid could be rejected at runtime.
  `apply_to_table` is gone — `apply` is the one entry point and it is the
  protected one.
- Overlap validation for update target paths now resolves `#alias` segments
  first: `SET #n = :a, n = :b`, `SET #a = :x, #b = :y` with both aliases naming
  one attribute, and `SET #n.#m = :x, n.m = :y` are rejected with the service's
  ValidationException instead of silently discarding one of the writes. The
  check runs before the first mutation, so a rejected update leaves the item
  untouched.
- `list_append` now accepts the function forms the service accepts in its
  arguments: `list_append(if_not_exists(items, :empty), :more)` and nested
  `list_append` calls parse instead of failing with "expected an attribute path
  or a value placeholder", so appending to a list that may not exist yet works.
  Both arguments are parsed as full set values; arithmetic inside them is still
  rejected.
- An expression string longer than the service's 4096-byte limit is now rejected
  before it is tokenized: no `ConditionExpression`, `UpdateExpression` or
  `ProjectionExpression` above 4096 bytes is accepted, and the oversized text is
  never copied or parsed. One `REMOVE` naming tens of thousands of paths can no
  longer burn a Lambda's CPU budget before it fails.
- The overlap check for update target paths is `O(n log n)`: the paths are
  sorted and only neighbours are compared, instead of comparing every pair
  (which cost `n²` `Vec` walks for one large `REMOVE`/`SET` clause). The
  verdict is unchanged — same path, or an attribute and anything below it — and
  a property test pins it to an exhaustive pairwise scan.
- A `ProjectionExpression` now follows a document path to its end instead of
  stopping at the first list index. `items[0].v` returns the element at index 0
  with only `v` instead of every element of `items` with every field, so a test
  that asserts on a projected list element compares the same shape the service
  returns. Named indices are kept in ascending order and without duplicates; an
  index the list does not have contributes nothing.

### Changed

- Number values are backed by `bigdecimal::BigDecimal` instead of a local
  hand-written decimal: ordering, equality, normalisation and `+`/`-` are the
  maintained crate's, so `"1.0"` equals `"1"` and 38-significant-digit
  comparisons are exact by construction rather than by inspection. The
  DynamoDB-specific parts stay — the literal grammar (the crate accepts
  `1_000`, the service does not), the 38-digit and `1E-130` … `9.99E+125`
  limits, the alignment guard against absurd exponent gaps, and the padding
  bound in `Number::to_plain_string`. Nothing observable changed: canonical
  forms, key identity, TTL conversion and every error message are as before.

## [0.1.1] - 2026-10-25

### Fixed

- Numbers outside DynamoDB's documented exponent range (`1E-130` …
  `9.99E+125`) are rejected at validation time instead of being accepted and
  then rendered. A five-byte value such as `1e2147483647` previously reached
  `Number::to_plain_string` and asked for `2^31` zero bytes.
- `Number::to_plain_string` is now fallible and bounded: it returns `None`
  rather than padding more than 130 zeros, which covers the whole documented
  range. Rendering an out-of-range number in an update expression is a
  validation error.
- `Number::add` computes exponent differences in `i64`. Subtracting two accepted
  `i32` exponents underflowed before the alignment guard ran
  (`i32::MAX - i32::MIN`), which panicked in debug builds.

## [0.1.0] - 2026-10-24

First published version: an in-process `DynamoDB` for tests — tables, items,
condition and update expressions, queries, scans, batches, transactions, TTL
handling and a `Streams`-style change log.
