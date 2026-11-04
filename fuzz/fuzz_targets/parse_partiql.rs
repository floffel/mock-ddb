//! Fuzz target: the PartiQL parser must survive arbitrary bytes — no panic, no
//! unbounded recursion, no hang — and must stay insensitive to surrounding
//! whitespace.
//!
//! Run with `cargo fuzz run parse_partiql`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mock_ddb::PartiqlStatement;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };

    let Ok(statement) = PartiqlStatement::parse(text) else {
        return;
    };

    // Every accessor is reachable on a statement that parsed, whatever the
    // input looked like.
    let kind = statement.kind();
    let table = statement.table_name().to_string();
    let _ = statement.parameters();

    // The lexer skips whitespace, so padding a statement cannot change what it
    // means. A disagreement here is a tokenizer that reads position or
    // lookahead across the whitespace it skipped.
    let padded = format!(" \t\n{text} \t\n");
    let padded_statement =
        PartiqlStatement::parse(&padded).expect("a padded statement parses like the original");
    assert_eq!(padded_statement.kind(), kind);
    assert_eq!(padded_statement.table_name(), table);
});
