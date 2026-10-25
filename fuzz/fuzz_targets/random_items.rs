//! Fuzz target: attribute values come from untrusted data in real systems too
//! (user input, imported files). Whatever shape an item takes, evaluating a
//! condition or applying an update to it must not panic.
//!
//! Run with `cargo fuzz run random_items`.

#![no_main]

use std::collections::HashMap;

use libfuzzer_sys::fuzz_target;
use mock_ddb::{
    n, s, AttributeValue, Blob, ConditionExpression, Item, ProjectionExpression, UpdateExpression,
};

/// Turn a byte slice into an item with string, number, binary, list and map
/// attributes derived from the input.
fn build_item(data: &[u8]) -> Item {
    let mut item = Item::new();
    for (index, chunk) in data.chunks(3).take(8).enumerate() {
        let name = format!("a{index}");
        let text = String::from_utf8_lossy(chunk).to_string();
        let value = match index % 5 {
            0 => s(text),
            1 => n(text),
            2 => AttributeValue::B(Blob::new(chunk.to_vec())),
            3 => AttributeValue::L(vec![s(text), n("1"), AttributeValue::Null(true)]),
            _ => AttributeValue::M(HashMap::from([("inner".to_string(), s(text))])),
        };
        item.insert(name, value);
    }
    item
}

fuzz_target!(|data: &[u8]| {
    let item = build_item(data);
    let names = HashMap::from([
        ("#a".to_string(), "a0".to_string()),
        ("#b".to_string(), "a1".to_string()),
        ("#c".to_string(), "a3".to_string()),
    ]);
    let values = HashMap::from([
        (":v".to_string(), s(String::from_utf8_lossy(data).to_string())),
        (":n".to_string(), n("1")),
        (":t".to_string(), s("S")),
    ]);

    for expression in [
        "#a = :v",
        "#a <> :v",
        "#b BETWEEN :n AND :n",
        "#a IN (:v, :t)",
        "size(#c) >= :n",
        "contains(#c, :v)",
        "begins_with(#a, :v)",
        "attribute_type(#a, :t)",
        "attribute_not_exists(#a) OR attribute_exists(#b)",
        "NOT (#a = :v AND #b = :n)",
    ] {
        let condition = ConditionExpression::parse(expression).expect("fixed condition parses");
        let _ = condition.evaluate(Some(&item), &names, &values);
    }

    for expression in [
        "SET #a = :v",
        "SET #c[0] = :v",
        "SET #c = list_append(#c, :v)",
        "SET #b = #b + :n",
        "SET #c = if_not_exists(#c, :v)",
        "REMOVE #a",
        "REMOVE #c[1]",
        "ADD #b :n",
        "DELETE #c :v",
    ] {
        let update = UpdateExpression::parse(expression).expect("fixed update parses");
        let mut copy = item.clone();
        let _ = update.apply(&mut copy, &names, &values);
    }

    let projection = ProjectionExpression::parse("#a, #c[0]").expect("fixed projection parses");
    let _ = projection.project(&item, &names);
});
