//! Fuzz target: the document wrapper and its three accessors are pure, so the
//! only guarantee arbitrary input can break is their single contract — a
//! document comes back exactly as it went in, and no scalar comes back as one.
//!
//! Items nest in real tables (an embedded address inside an order, a list of
//! line items inside a cart), so the input builds nested documents rather than a
//! flat map: a helper that drops, flattens or reorders a level is a bug every
//! test using the mock would inherit.
//!
//! Run with `cargo fuzz run item_document`.

#![no_main]

use std::collections::HashMap;

use libfuzzer_sys::fuzz_target;
use mock_ddb::{
    into_item, item_from_value, item_ref_from_value, item_to_value, AttributeValue, Blob, Item,
};

/// Deepest document the target builds. Every clone walks the whole subtree, so
/// deeper nesting buys coverage at a stack cost; past this point the accessors
/// add no code path.
const MAX_DEPTH: usize = 8;

/// Attributes per item, so a long input cannot build an item whose clones take
/// seconds each.
const MAX_ATTRIBUTES: usize = 8;

/// Consume up to four bytes for a scalar payload.
fn take<'a>(data: &'a [u8], cursor: &mut usize) -> &'a [u8] {
    if *cursor >= data.len() {
        return &[];
    }
    let len = usize::from(data[*cursor] % 4);
    let start = *cursor;
    let end = (start + len).min(data.len());
    *cursor = end;
    &data[start..end]
}

/// Build one item: attributes named `a0`, `a1`, … with values drawn from
/// `data`, nested documents included.
fn build_item(data: &[u8], depth: usize, cursor: &mut usize) -> Item {
    let mut item = Item::new();
    while *cursor < data.len() && item.len() < MAX_ATTRIBUTES {
        let value = build_value(data, depth, cursor);
        let name = format!("a{}", item.len());
        item.insert(name, value);
    }
    item
}

/// Build one attribute value from the next tag byte and whatever payload
/// follows it.
fn build_value(data: &[u8], depth: usize, cursor: &mut usize) -> AttributeValue {
    let tag = data[*cursor];
    *cursor += 1;
    match tag % 8 {
        0 => AttributeValue::S(String::from_utf8_lossy(take(data, cursor)).into_owned()),
        1 => AttributeValue::N(i64::from(tag).to_string()),
        2 => AttributeValue::B(Blob::new(take(data, cursor))),
        3 => AttributeValue::Bool(tag % 2 == 0),
        4 => AttributeValue::Null(true),
        5 => {
            let mut members = Vec::new();
            for _ in 0..usize::from(tag % 3) {
                if depth >= MAX_DEPTH || *cursor >= data.len() {
                    break;
                }
                members.push(build_value(data, depth + 1, cursor));
            }
            AttributeValue::L(members)
        }
        6 if depth < MAX_DEPTH => AttributeValue::M(build_item(data, depth + 1, cursor)),
        7 => AttributeValue::Ss(Vec::new()),
        _ => AttributeValue::Null(false),
    }
}

fn check_round_trip(item: Item) {
    let wrapped = item_to_value(item.clone());
    assert!(
        matches!(&wrapped, AttributeValue::M(_)),
        "a wrapped item is a document"
    );
    assert_eq!(
        item_ref_from_value(&wrapped),
        Some(&item),
        "the borrowed form must see the stored entries"
    );
    assert_eq!(item_from_value(&wrapped), Some(item.clone()));
    assert_eq!(into_item(wrapped), Some(item));
}

fuzz_target!(|data: &[u8]| {
    // The empty item is a document too, and it is the one shape no input byte
    // can reach.
    check_round_trip(Item::new());

    if !data.is_empty() {
        let mut cursor = 0;
        check_round_trip(build_item(data, 0, &mut cursor));
    }

    // Scalars never pass as documents; empty sets and empty lists are the
    // shapes a size-based check would wave through.
    for scalar in [
        AttributeValue::Null(true),
        AttributeValue::Null(false),
        AttributeValue::Bool(false),
        AttributeValue::Ss(Vec::new()),
        AttributeValue::Ns(Vec::new()),
        AttributeValue::Bs(Vec::new()),
        AttributeValue::L(Vec::new()),
        AttributeValue::S(String::new()),
        AttributeValue::N("0".to_string()),
        AttributeValue::B(Blob::new(Vec::new())),
    ] {
        assert_eq!(item_ref_from_value(&scalar), None, "{scalar:?}");
        assert_eq!(item_from_value(&scalar), None, "{scalar:?}");
        assert_eq!(into_item(scalar), None);
    }

    // A document nested to the cap comes back at the depth it was built, not
    // flattened on the way through.
    let mut deep = item_to_value(Item::new());
    for _ in 0..MAX_DEPTH {
        deep = item_to_value(HashMap::from([("doc".to_string(), deep)]));
    }
    let mut level = &deep;
    for _ in 0..MAX_DEPTH {
        level = &item_ref_from_value(level).expect("every level is a document")["doc"];
    }
    assert_eq!(item_from_value(level), Some(Item::new()));
});
