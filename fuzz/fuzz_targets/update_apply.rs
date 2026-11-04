//! Fuzz target: an update expression and the item it runs against both come
//! from untrusted input in real systems. Whatever pair is thrown at
//! `UpdateExpression::apply`, three documented guarantees have to hold: it never
//! panics, a rejected update leaves the item exactly as it was passed in, and an
//! accepted one leaves an item the store can still read — a stored number that
//! no longer parses, or a set the update cannot classify, is a broken invariant
//! of the mock and must not be reachable through an expression.
//!
//! The expressions are built from a table of clauses instead of being parsed out
//! of the input directly: a purely random string almost never parses, so the
//! structured table is what gets the writes exercised, and the bytes decide
//! which of them run.
//!
//! Run with `cargo fuzz run update_apply`.

#![no_main]

use std::collections::HashMap;

use libfuzzer_sys::fuzz_target;
use mock_ddb::{n, s, AttributeValue, Blob, Item, KeySchema, KeyType, UpdateExpression};

/// Assignments an update can carry, one per shape the update path handles.
const ASSIGNMENTS: [&str; 9] = [
    "a = :v",
    "b = :v",
    "count = :n",
    "count = count + :n",
    "a = if_not_exists(a, :v)",
    "rows = list_append(rows, :l)",
    "rows[0] = :v",
    "profile.city = :v",
    "tags = :v",
];

/// Paths a `REMOVE` can name, including the nested and indexed ones.
const REMOVALS: [&str; 5] = ["a", "b", "count", "rows[0]", "profile.city"];

/// `ADD` pairs: a number and a set are the two it accepts.
const ADDITIONS: [&str; 3] = ["count :n", "tags :v", "counts :n"];

/// `DELETE` pairs: a set of the stored member type, and one of another type so
/// the mismatch path runs too.
const DELETIONS: [&str; 3] = ["tags :v", "tags :numbers", "absent :v"];

/// The attribute name a generated item uses at a given position.
fn attribute_name(index: usize) -> String {
    match index % 6 {
        0 => "a".to_string(),
        1 => "b".to_string(),
        2 => "count".to_string(),
        3 => "tags".to_string(),
        4 => "rows".to_string(),
        _ => "profile".to_string(),
    }
}

/// Turn a byte slice into an item shaped like the expressions expect: a string,
/// two numbers, a string set, a list and a map with a nested attribute.
///
/// The numbers are built from a byte, not from the text: an item the store could
/// never hold (`N` with a spelling no read can parse) is not a pair this target
/// is about, and it would make the read-back check below report the input rather
/// than the update.
fn build_item(data: &[u8]) -> Item {
    let mut item = Item::new();
    for index in 0..6 {
        let chunk = data.get(index * 4..index * 4 + 4).unwrap_or(&[]);
        let text = String::from_utf8_lossy(chunk).to_string();
        let value = match index {
            0 => s(&text),
            1 | 2 => n(&(i32::from(chunk.first().copied().unwrap_or(0)) - 128).to_string()),
            3 => AttributeValue::Ss(vec![text, "seed".to_string()]),
            4 => AttributeValue::L(vec![s(&text), n("1")]),
            _ => AttributeValue::M(HashMap::from([("city".to_string(), s(&text))])),
        };
        item.insert(attribute_name(index), value);
    }
    item
}

/// The expression attributes drawn from the input: a string set, a number set, a
/// list and a plain string, so every operand shape the clauses name exists.
fn build_values(data: &[u8]) -> HashMap<String, AttributeValue> {
    let text = String::from_utf8_lossy(data).to_string();
    HashMap::from([
        (
            ":v".to_string(),
            AttributeValue::Ss(vec![text.clone(), "seed".to_string()]),
        ),
        (
            ":numbers".to_string(),
            AttributeValue::Ns(vec!["1".to_string(), "2".to_string()]),
        ),
        (
            ":l".to_string(),
            AttributeValue::L(vec![s(&text), AttributeValue::B(Blob::new(data.to_vec()))]),
        ),
        (":n".to_string(), n("1")),
    ])
}

/// Build one update expression out of the tables, using the input bytes as the
/// selector for which clauses it holds.
fn build_expression(data: &[u8]) -> String {
    let pick =
        |index: usize, len: usize| data.get(index).map_or(0, |byte| usize::from(*byte)) % len;

    let assignments: Vec<&str> = (0..1 + pick(0, 3))
        .map(|step| ASSIGNMENTS[pick(1 + step, ASSIGNMENTS.len())])
        .collect();
    let mut expression = format!("SET {}", assignments.join(", "));
    if pick(4, 2) == 0 {
        expression.push_str(&format!(" REMOVE {}", REMOVALS[pick(5, REMOVALS.len())]));
    }
    if pick(6, 2) == 0 {
        expression.push_str(&format!(" ADD {}", ADDITIONS[pick(7, ADDITIONS.len())]));
    }
    if pick(8, 2) == 0 {
        expression.push_str(&format!(" DELETE {}", DELETIONS[pick(9, DELETIONS.len())]));
    }
    expression
}

fuzz_target!(|data: &[u8]| {
    let item = build_item(data);
    let values = build_values(data);
    let names = HashMap::new();
    let schema = KeySchema::hash("pk", KeyType::String);
    let Ok(update) = UpdateExpression::parse(&build_expression(data)) else {
        return;
    };

    let mut target = item.clone();
    match update.apply(&mut target, &schema, &names, &values) {
        // A rejected update writes nothing: the caller keeps exactly the item it
        // passed in, not a half-applied one.
        Err(_) => assert_eq!(target, item, "a rejected update changed the item"),
        Ok(_) => {
            // The store has to be able to read back what landed. `ADD` is the
            // read that surfaces an unparseable number as an internal error, so
            // any internal error here means the update stored a value the mock
            // itself cannot use.
            for name in target.keys() {
                let probe = format!("ADD {name} :n");
                let probe =
                    UpdateExpression::parse(&probe).expect("the probe is a fixed expression");
                let mut copy = target.clone();
                if let Err(error) = probe.apply(&mut copy, &schema, &names, &values) {
                    assert_ne!(
                        error.code(),
                        "InternalServerError",
                        "the update stored a value the store cannot read at `{name}`: {error}"
                    );
                }
            }
        }
    }
});
