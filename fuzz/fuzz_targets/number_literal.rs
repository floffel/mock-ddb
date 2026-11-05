//! Fuzz target: every number the store sees arrives as a literal, and
//! `Number::parse` is the only door it comes through — an `N` in a key, in a
//! written item, or in a condition's value map, which is caller input in any
//! suite that builds that map from data.
//!
//! The parser is crate-private, so the target reaches it the way a caller does:
//! a stored `AttributeValue::N` compared by a `ConditionExpression`. Four
//! guarantees have to hold whatever the bytes are: comparing a value with
//! itself cannot panic and is true, `=` and `<>` are each other's negation, the
//! ordered outcomes `=`, `<`, `>` are exclusive for a literal the parser
//! accepts, and such a literal compares equal to its whitespace-padded spelling
//! — a `parse` that trims must not let the padding around a value change it,
//! and one that stopped trimming would.
//!
//! `random_items` splits its input into three-byte number chunks, so a literal
//! with a sign, a fraction or an exponent only reaches the grammar here.
//!
//! Run with `cargo fuzz run number_literal`.

#![no_main]

use std::collections::HashMap;
use std::sync::LazyLock;

use libfuzzer_sys::fuzz_target;
use mock_ddb::{item, n, AttributeValue, ConditionExpression, Error, Item};

/// Longest literal `Number::parse` looks at, in bytes. A longer string is not a
/// number at all — it is refused before the grammar — so padding one is not
/// expected to leave it comparable.
const MAX_LITERAL_BYTES: usize = 1024;

/// `#n = :v`, the reflexive and symmetric comparison.
static EQUAL: LazyLock<(ConditionExpression, HashMap<String, String>)> = LazyLock::new(|| {
    (
        ConditionExpression::parse("#n = :v").expect("a valid condition"),
        HashMap::from([("#n".to_string(), "n".to_string())]),
    )
});

/// `#n <> :v`, the negation of [`EQUAL`].
static NOT_EQUAL: LazyLock<(ConditionExpression, HashMap<String, String>)> = LazyLock::new(|| {
    (
        ConditionExpression::parse("#n <> :v").expect("a valid condition"),
        HashMap::from([("#n".to_string(), "n".to_string())]),
    )
});

/// `#n < :v`, one of the three ordered outcomes.
static LESS: LazyLock<(ConditionExpression, HashMap<String, String>)> = LazyLock::new(|| {
    (
        ConditionExpression::parse("#n < :v").expect("a valid condition"),
        HashMap::from([("#n".to_string(), "n".to_string())]),
    )
});

/// `#n > :v`, the other ordered outcome.
static GREATER: LazyLock<(ConditionExpression, HashMap<String, String>)> = LazyLock::new(|| {
    (
        ConditionExpression::parse("#n > :v").expect("a valid condition"),
        HashMap::from([("#n".to_string(), "n".to_string())]),
    )
});

/// Evaluate one of the prepared conditions with `value` bound to `:v`.
fn holds(
    prepared: &(ConditionExpression, HashMap<String, String>),
    item: &Item,
    value: &AttributeValue,
) -> Result<bool, Error> {
    let (expression, names) = prepared;
    let values = HashMap::from([(":v".to_string(), value.clone())]);
    expression.evaluate(Some(item), names, &values)
}

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let item = item([("n", n(text))]);

    // A value equals itself, whatever it is: a literal the grammar rejects is
    // still compared as the string it is, not as a number.
    assert!(holds(&EQUAL, &item, &n(text)).expect("equality never fails"));
    assert!(!holds(&NOT_EQUAL, &item, &n(text)).expect("equality never fails"));

    // Against zero the ordering is total, but only for a literal the parser
    // accepts: an unparsable one makes the ordered comparison refuse (the
    // mock's type check), which is the documented answer and not a finding.
    let zero = n("0");
    let equal = holds(&EQUAL, &item, &zero).expect("equality never fails");
    let Ok(less) = holds(&LESS, &item, &zero) else {
        return;
    };
    let Ok(greater) = holds(&GREATER, &item, &zero) else {
        return;
    };
    assert_eq!(
        usize::from(equal) + usize::from(less) + usize::from(greater),
        1,
        "exactly one of =, < and > must hold for {text:?}"
    );

    // The literal parses (it was ordered against zero), so padding it must not
    // change the value: `parse` trims before it reads the grammar. A literal
    // past the length cap is refused before the trim, so only a shorter one is
    // padded — four bytes are added.
    if text.len() + 4 <= MAX_LITERAL_BYTES {
        let padded = format!(" \t{text}\t ");
        assert!(
            holds(&EQUAL, &item, &n(padded)).expect("equality never fails"),
            "a padded literal must be the same number: {text:?}"
        );
    }
});
