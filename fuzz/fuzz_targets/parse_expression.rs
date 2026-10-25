//! Fuzz target: the expression parser and evaluator must survive arbitrary
//! bytes — no panic, no unbounded recursion, no hang.
//!
//! Run with `cargo fuzz run parse_expression`.

#![no_main]

use std::collections::HashMap;

use libfuzzer_sys::fuzz_target;
use mock_ddb::{
    n, s, AttributeValue, ConditionExpression, Item, ProjectionExpression, UpdateExpression,
};

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };

    let mut item: Item = Item::new();
    item.insert("a".to_string(), s("x"));
    item.insert("b".to_string(), n("1"));
    item.insert(
        "l".to_string(),
        AttributeValue::L(vec![s("x"), n("2"), AttributeValue::Null(true)]),
    );
    item.insert(
        "m".to_string(),
        AttributeValue::M(HashMap::from([("k".to_string(), s("v"))])),
    );
    item.insert(
        "tags".to_string(),
        AttributeValue::Ss(vec!["x".to_string(), "y".to_string()]),
    );

    let names = HashMap::from([
        ("#a".to_string(), "a".to_string()),
        ("#b".to_string(), "b".to_string()),
        ("#l".to_string(), "l".to_string()),
    ]);
    let values = HashMap::from([
        (":v".to_string(), s("x")),
        (":n".to_string(), n("1")),
        (":t".to_string(), s("S")),
    ]);

    if let Ok(condition) = ConditionExpression::parse(text) {
        let _ = condition.evaluate(Some(&item), &names, &values);
        let _ = condition.evaluate(None, &names, &values);
    }
    if let Ok(update) = UpdateExpression::parse(text) {
        let mut copy = item.clone();
        let _ = update.apply(&mut copy, &names, &values);
        // A second application checks that a partially applied update leaves a
        // value the evaluator can still read.
        let _ = update.apply(&mut copy, &names, &values);
    }
    if let Ok(projection) = ProjectionExpression::parse(text) {
        let _ = projection.project(&item, &names);
    }
});
