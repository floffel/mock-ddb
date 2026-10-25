# Changelog

All notable changes to this crate are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the crate
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
