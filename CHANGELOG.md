# Changelog

All notable changes to this crate are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the crate
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

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
