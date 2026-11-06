//! Fuzz target: attribute values arrive from imported data and user input, so
//! the write path has to decide every shape without panicking, allocating
//! without bound, or recursing without bound.
//!
//! The value builder is bounded independently of the validator: it consumes a
//! finite input and stops at the depth and arity limits below. It deliberately
//! includes malformed numbers, empty and duplicate set members, empty sets and
//! nested lists/maps. Those values go through the same public validation seam a
//! caller uses, `MockDdb::seed`, rather than calling a crate-private helper.
//!
//! Run with `cargo fuzz run value_validation`.

#![no_main]

use std::collections::HashMap;

use libfuzzer_sys::fuzz_target;
use mock_ddb::{item, s, AttributeValue, Blob, Item, KeySchema, KeyType, MockDdb};

/// Deepest value the target builds. The validator accepts 32 nested levels; a
/// few extra exercise the rejection path too.
const MAX_BUILD_DEPTH: usize = 36;

/// Maximum collection size per level, keeping the tree small enough for a fuzz
/// iteration to finish even when every branch recurses.
const MAX_ARITY: usize = 6;

/// Consume up to `limit` bytes and advance the cursor.
fn take<'a>(data: &'a [u8], cursor: &mut usize, limit: usize) -> &'a [u8] {
    let len = data
        .get(*cursor)
        .map_or(0, |byte| usize::from(*byte) % (limit + 1));
    *cursor += 1;
    let start = (*cursor).min(data.len());
    let end = start.saturating_add(len).min(data.len());
    *cursor = end;
    &data[start..end]
}

fn next_byte(data: &[u8], cursor: &mut usize) -> u8 {
    let byte = data.get(*cursor).copied().unwrap_or(0);
    *cursor = (*cursor).saturating_add(1);
    byte
}

fn set_member(data: &[u8], cursor: &mut usize, kind: u8) -> String {
    // Preserve malformed spellings in numeric members as well as valid ones.
    // The write path must reject them before storing a set it cannot compare.
    let limit = if kind % 3 == 1 { 12 } else { 8 };
    String::from_utf8_lossy(take(data, cursor, limit)).into_owned()
}

fn build_set(data: &[u8], cursor: &mut usize) -> AttributeValue {
    let kind = next_byte(data, cursor);
    let arity = usize::from(next_byte(data, cursor)) % (MAX_ARITY + 1);
    let members: Vec<String> = (0..arity)
        .map(|_| set_member(data, cursor, kind))
        .collect();
    match kind % 3 {
        0 => AttributeValue::Ss(members),
        1 => AttributeValue::Ns(members),
        _ => AttributeValue::Bs(members.into_iter().map(String::into_bytes).map(Blob::new).collect()),
    }
}

fn build_value(data: &[u8], depth: usize, cursor: &mut usize) -> AttributeValue {
    let tag = next_byte(data, cursor);
    match tag % 10 {
        0 => AttributeValue::S(String::from_utf8_lossy(take(data, cursor, 16)).into_owned()),
        1 => AttributeValue::N(String::from_utf8_lossy(take(data, cursor, 24)).into_owned()),
        2 => AttributeValue::B(Blob::new(take(data, cursor, 16))),
        3 => AttributeValue::Bool(tag % 2 == 0),
        4 => AttributeValue::Null(tag % 2 == 0),
        5 | 6 => build_set(data, cursor),
        7 if depth < MAX_BUILD_DEPTH => {
            let arity = usize::from(next_byte(data, cursor)) % (MAX_ARITY + 1);
            AttributeValue::L(
                (0..arity)
                    .map(|_| build_value(data, depth + 1, cursor))
                    .collect(),
            )
        }
        8 if depth < MAX_BUILD_DEPTH => {
            let arity = usize::from(next_byte(data, cursor)) % (MAX_ARITY + 1);
            AttributeValue::M(
                (0..arity)
                    .map(|index| {
                        (
                            format!("field{index}"),
                            build_value(data, depth + 1, cursor),
                        )
                    })
                    .collect::<HashMap<_, _>>(),
            )
        }
        _ => AttributeValue::Null(true),
    }
}

fn build_item(data: &[u8], cursor: &mut usize) -> Item {
    let arity = usize::from(next_byte(data, cursor)) % 4;
    let mut attributes = vec![("pk".to_string(), s("fuzz"))];
    for index in 0..arity {
        attributes.push((format!("value{index}"), build_value(data, 0, cursor)));
    }
    item(attributes)
}

fuzz_target!(|data: &[u8]| {
    let mock = MockDdb::with_table("values", KeySchema::hash("pk", KeyType::String))
        .expect("the fixed table is valid");
    let mut cursor = 0;
    let candidate = build_item(data, &mut cursor);

    // The public write validation seam must always return. Accepted values are
    // stored; rejected ones leave the table unchanged.
    let accepted = mock.seed("values", vec![candidate.clone()]).is_ok();
    assert_eq!(mock.count("values"), usize::from(accepted));

    // A deliberately over-deep list must be rejected without marching through
    // an unbounded number of levels.
    let mut deep = s("leaf");
    for _ in 1..=MAX_BUILD_DEPTH {
        deep = AttributeValue::L(vec![deep]);
    }
    assert!(mock
        .seed("values", vec![item([("pk", s("deep")), ("value", deep)])])
        .is_err());
});
