//! PartiQL: `ExecuteStatement` and `BatchExecuteStatement`.
//!
//! The tests run the statements a caller would write and assert on the native
//! surface afterwards, so a PartiQL write is checked to be a real write — one
//! the stream log, TTL and indexes see — and not a second, parallel store.

mod common;

use common::{order, order_key, orders, user, user_key, users};
use mock_ddb::{
    n, s, BatchExecuteStatementRequest, BatchStatementErrorCode, BatchStatementRequest, Error,
    ExecuteStatementRequest, GetItemRequest, Item, KeySchema, KeyType, MockDdb, PartiqlStatement,
    ReturnConsumedCapacity, StatementKind, MAX_BATCH_STATEMENTS, MAX_STATEMENT_LENGTH,
};

/// Run one statement with the given parameters.
async fn run(mock: &MockDdb, statement: &str, parameters: Vec<mock_ddb::AttributeValue>) -> Item {
    let output = mock
        .execute_statement(ExecuteStatementRequest::new(statement).with_parameters(parameters))
        .await
        .unwrap();
    output
        .items
        .expect("a read returns items")
        .into_iter()
        .next()
        .expect("one row")
}

#[tokio::test]
async fn a_statement_covers_the_whole_lifecycle() {
    let mock = orders();

    mock.execute_statement(
        ExecuteStatementRequest::new(
            "INSERT INTO \"orders\" VALUE {'pk': ?, 'sk': ?, 'total': 10, 'tags': ['a']}",
        )
        .with_parameters(vec![s("customer#1"), n("7")]),
    )
    .await
    .unwrap();
    assert_eq!(mock.count("orders"), 1);

    // The row is a real row: the native read finds it.
    let stored = mock
        .get_item(GetItemRequest::new("orders", order_key("1", 7)))
        .await
        .unwrap();
    assert_eq!(stored.item.expect("the insert landed")["total"], n("10"));

    let read = run(
        &mock,
        "SELECT * FROM \"orders\" WHERE pk = ? AND sk = ?",
        vec![s("customer#1"), n("7")],
    )
    .await;
    assert_eq!(read["pk"], s("customer#1"));

    mock.execute_statement(
        ExecuteStatementRequest::new(
            "UPDATE \"orders\" SET total = total + ?, extra = 'x' REMOVE tags WHERE pk = ? AND sk = ?",
        )
        .with_parameters(vec![n("5"), s("customer#1"), n("7")]),
    )
    .await
    .unwrap();
    let updated = run(
        &mock,
        "SELECT * FROM \"orders\" WHERE pk = ? AND sk = ?",
        vec![s("customer#1"), n("7")],
    )
    .await;
    assert_eq!(updated["total"], n("15"));
    assert_eq!(updated["extra"], s("x"));
    assert!(!updated.contains_key("tags"));

    mock.execute_statement(
        ExecuteStatementRequest::new("DELETE FROM \"orders\" WHERE pk = ? AND sk = ?")
            .with_parameters(vec![s("customer#1"), n("7")]),
    )
    .await
    .unwrap();
    assert_eq!(mock.count("orders"), 0);
}

#[tokio::test]
async fn a_select_projects_and_filters() {
    let mock = orders();
    mock.seed(
        "orders",
        vec![
            order("1", 1, "10"),
            order("1", 2, "99"),
            order("2", 1, "50"),
        ],
    )
    .unwrap();

    let page = mock
        .execute_statement(
            ExecuteStatementRequest::new(
                "SELECT pk, total FROM \"orders\" WHERE pk = ? AND total > ?",
            )
            .with_parameters(vec![s("customer#1"), n("50")]),
        )
        .await
        .unwrap();
    let items = page.items.expect("items");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].len(), 2);
    assert_eq!(items[0]["total"], n("99"));
}

#[tokio::test]
async fn a_select_without_a_key_condition_scans() {
    let mock = orders();
    mock.seed("orders", vec![order("1", 1, "10"), order("2", 1, "20")])
        .unwrap();

    let page = mock
        .execute_statement(
            ExecuteStatementRequest::new("SELECT * FROM \"orders\" WHERE total = ?")
                .with_parameters(vec![n("20")]),
        )
        .await
        .unwrap();
    assert_eq!(page.items.expect("items").len(), 1);
}

#[tokio::test]
async fn a_select_pages_through_next_token() {
    let mock = orders();
    mock.seed(
        "orders",
        vec![order("1", 1, "1"), order("1", 2, "2"), order("1", 3, "3")],
    )
    .unwrap();

    let first = mock
        .execute_statement(
            ExecuteStatementRequest::new("SELECT * FROM \"orders\" WHERE pk = ?")
                .with_parameters(vec![s("customer#1")])
                .with_limit(2),
        )
        .await
        .unwrap();
    assert_eq!(first.items.as_ref().map(Vec::len), Some(2));
    let token = first.next_token.expect("a second page is left");
    assert!(first.last_evaluated_key.is_some());

    let second = mock
        .execute_statement(
            ExecuteStatementRequest::new("SELECT * FROM \"orders\" WHERE pk = ?")
                .with_parameters(vec![s("customer#1")])
                .with_limit(2)
                .with_next_token(token),
        )
        .await
        .unwrap();
    assert_eq!(second.items.as_ref().map(Vec::len), Some(1));
    assert!(second.next_token.is_none());
}

#[tokio::test]
async fn a_token_from_another_mock_is_rejected() {
    let mock = orders();
    mock.seed("orders", vec![order("1", 1, "1"), order("1", 2, "2")])
        .unwrap();
    let other = orders();
    other
        .seed("orders", vec![order("1", 1, "1"), order("1", 2, "2")])
        .unwrap();
    let first = mock
        .execute_statement(
            ExecuteStatementRequest::new("SELECT * FROM \"orders\" WHERE pk = ?")
                .with_parameters(vec![s("customer#1")])
                .with_limit(1),
        )
        .await
        .unwrap();
    let token = first.next_token.expect("a token");

    let error = other
        .execute_statement(
            ExecuteStatementRequest::new("SELECT * FROM \"orders\" WHERE pk = ?")
                .with_parameters(vec![s("customer#1")])
                .with_next_token(token),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Invalid NextToken"), "{error}");
}

#[tokio::test]
async fn an_insert_refuses_an_existing_key() {
    let mock = users();
    mock.execute_statement(
        ExecuteStatementRequest::new("INSERT INTO \"users\" VALUE {'pk': ?, 'email': 'a'}")
            .with_parameters(vec![s("u#1")]),
    )
    .await
    .unwrap();

    let error = mock
        .execute_statement(
            ExecuteStatementRequest::new("INSERT INTO \"users\" VALUE {'pk': ?}")
                .with_parameters(vec![s("u#1")]),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "DuplicateItemException");
    // The refusal left the stored row alone.
    assert_eq!(mock.count("users"), 1);
}

#[tokio::test]
async fn a_write_needs_the_whole_key() {
    let mock = orders();
    mock.seed("orders", vec![order("1", 1, "10")]).unwrap();

    for (statement, parameters) in [
        (
            "UPDATE \"orders\" SET total = 1 WHERE pk = ?",
            vec![s("customer#1")],
        ),
        ("DELETE FROM \"orders\" WHERE pk = ?", vec![s("customer#1")]),
        ("DELETE FROM \"orders\"", Vec::new()),
    ] {
        let error = mock
            .execute_statement(ExecuteStatementRequest::new(statement).with_parameters(parameters))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("mandatory equality on all key attributes"),
            "{statement}: {error}"
        );
    }
}

#[tokio::test]
async fn a_failing_where_clause_is_a_conditional_check() {
    let mock = users();
    mock.execute_statement(
        ExecuteStatementRequest::new("INSERT INTO \"users\" VALUE {'pk': ?, 'stale': false}")
            .with_parameters(vec![s("u#1")]),
    )
    .await
    .unwrap();

    let error = mock
        .execute_statement(
            ExecuteStatementRequest::new(
                "UPDATE \"users\" SET stale = true WHERE pk = ? AND stale = ?",
            )
            .with_parameters(vec![s("u#1"), mock_ddb::AttributeValue::Bool(true)]),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "ConditionalCheckFailedException");
}

#[tokio::test]
async fn the_parameter_count_must_match() {
    let mock = users();
    for parameters in [vec![], vec![s("a"), s("b")]] {
        let error = mock
            .execute_statement(
                ExecuteStatementRequest::new("SELECT * FROM \"users\" WHERE pk = ?")
                    .with_parameters(parameters.clone()),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Validation { .. }));
        assert!(
            error
                .to_string()
                .contains("Number of parameters in request and statement does not match"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn a_statement_at_the_length_limit_is_accepted_and_one_over_is_not() {
    // A statement of exactly the limit: one long attribute name pads it out.
    let prefix = "SELECT * FROM t WHERE ";
    let suffix = " = 1";
    let padding = "a".repeat(MAX_STATEMENT_LENGTH - prefix.len() - suffix.len());
    let inside = format!("{prefix}{padding}{suffix}");
    assert_eq!(inside.chars().count(), MAX_STATEMENT_LENGTH);
    assert_eq!(PartiqlStatement::parse(&inside).unwrap().parameters(), 0);

    let outside = format!("{inside} ");
    assert_eq!(outside.chars().count(), MAX_STATEMENT_LENGTH + 1);
    let error = PartiqlStatement::parse(&outside).unwrap_err();
    assert!(error.to_string().contains("8192"), "{error}");
}

#[tokio::test]
async fn a_batch_at_the_limit_is_accepted_and_one_over_is_not() {
    let mock = users();
    let statement = || {
        BatchStatementRequest::new("INSERT INTO \"users\" VALUE {'pk': ?}")
            .with_parameters(vec![s("u#1")])
    };
    let full = (0..MAX_BATCH_STATEMENTS).fold(BatchExecuteStatementRequest::new(), |batch, _| {
        batch.with_statement(statement())
    });
    let output = mock.batch_execute_statement(full).await.unwrap();
    assert_eq!(output.responses.len(), MAX_BATCH_STATEMENTS);

    let oversized = (0..=MAX_BATCH_STATEMENTS)
        .fold(BatchExecuteStatementRequest::new(), |batch, _| {
            batch.with_statement(statement())
        });
    let error = mock.batch_execute_statement(oversized).await.unwrap_err();
    assert!(error.to_string().contains("25"), "{error}");
}

#[tokio::test]
async fn a_batch_cannot_mix_reads_and_writes() {
    let mock = users();
    let error = mock
        .batch_execute_statement(
            BatchExecuteStatementRequest::new()
                .with_statement(
                    BatchStatementRequest::new("SELECT * FROM \"users\" WHERE pk = ?")
                        .with_parameters(vec![s("u#1")]),
                )
                .with_statement(
                    BatchStatementRequest::new("DELETE FROM \"users\" WHERE pk = ?")
                        .with_parameters(vec![s("u#1")]),
                ),
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("either read statements or write statements"),
        "{error}"
    );
}

#[tokio::test]
async fn a_batch_read_needs_the_whole_key() {
    let mock = orders();
    mock.seed("orders", vec![order("1", 1, "10")]).unwrap();
    let output = mock
        .batch_execute_statement(
            BatchExecuteStatementRequest::new()
                .with_statement(
                    BatchStatementRequest::new("SELECT * FROM \"orders\" WHERE pk = ?")
                        .with_parameters(vec![s("customer#1")]),
                )
                .with_statement(
                    BatchStatementRequest::new("SELECT * FROM \"orders\" WHERE pk = ? AND sk = ?")
                        .with_parameters(vec![s("customer#1"), n("1")]),
                ),
        )
        .await
        .unwrap();

    assert_eq!(
        output.responses[0].error.as_ref().map(|error| error.code),
        Some(BatchStatementErrorCode::ValidationError)
    );
    assert!(output.responses[1].error.is_none());
    assert_eq!(
        output.responses[1]
            .item
            .as_ref()
            .map(|item| item["total"].clone()),
        Some(n("10"))
    );
}

#[tokio::test]
async fn a_batch_member_failure_does_not_roll_back_its_neighbours() {
    let mock = users();
    let output = mock
        .batch_execute_statement(
            BatchExecuteStatementRequest::new()
                .with_statement(
                    BatchStatementRequest::new("INSERT INTO \"users\" VALUE {'pk': ?}")
                        .with_parameters(vec![s("u#1")]),
                )
                .with_statement(
                    BatchStatementRequest::new("INSERT INTO \"users\" VALUE {'pk': ?}")
                        .with_parameters(vec![s("u#1")]),
                )
                .with_statement(
                    BatchStatementRequest::new("INSERT INTO \"users\" VALUE {'pk': ?}")
                        .with_parameters(vec![s("u#2")]),
                ),
        )
        .await
        .unwrap();
    assert!(output.responses[0].error.is_none());
    assert_eq!(
        output.responses[1].error.as_ref().map(|error| error.code),
        Some(BatchStatementErrorCode::DuplicateItem)
    );
    assert!(output.responses[2].error.is_none());
    assert_eq!(mock.count("users"), 2);
}

#[tokio::test]
async fn an_unknown_table_is_not_found() {
    let mock = MockDdb::new();
    let error = mock
        .execute_statement(
            ExecuteStatementRequest::new("SELECT * FROM \"missing\" WHERE pk = ?")
                .with_parameters(vec![s("u#1")]),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::ResourceNotFound { .. }), "{error}");
    assert_eq!(error.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn syntax_errors_name_the_statement_class() {
    for statement in ["", "SELECT * FROM", "PATCH x", "SELECT * FROM t WHERE"] {
        let error = PartiqlStatement::parse(statement).unwrap_err();
        assert_eq!(error.code(), "ValidationException");
    }
}

#[tokio::test]
async fn statements_are_typed_by_kind_and_table() {
    let cases = [
        (
            "SELECT * FROM \"users\" WHERE pk = ?",
            StatementKind::Select,
        ),
        (
            "INSERT INTO \"users\" VALUE {'pk': 1}",
            StatementKind::Insert,
        ),
        (
            "UPDATE \"users\" SET a = 1 WHERE pk = ?",
            StatementKind::Update,
        ),
        ("DELETE FROM \"users\" WHERE pk = ?", StatementKind::Delete),
    ];
    for (statement, kind) in cases {
        let parsed = PartiqlStatement::parse(statement).unwrap();
        assert_eq!(parsed.kind(), kind, "{statement}");
        assert_eq!(parsed.table_name(), "users", "{statement}");
    }
}

#[tokio::test]
async fn a_select_can_read_an_index() {
    let mock = MockDdb::new();
    mock.create_table(
        mock_ddb::CreateTableRequest::new("users", KeySchema::hash("pk", KeyType::String))
            .with_global_index(mock_ddb::SecondaryIndex::new(
                "by-email",
                KeySchema::hash("email", KeyType::String),
            )),
    )
    .await
    .unwrap();
    mock.seed(
        "users",
        vec![user("u#1", "a@example.com"), user("u#2", "b@example.com")],
    )
    .unwrap();

    let row = run(
        &mock,
        "SELECT * FROM \"users\".\"by-email\" WHERE email = ?",
        vec![s("a@example.com")],
    )
    .await;
    assert_eq!(row["pk"], s("u#1"));
}

#[tokio::test]
async fn capacity_is_reported_when_asked_for() {
    let mock = users();
    let written = mock
        .execute_statement(
            ExecuteStatementRequest::new("INSERT INTO \"users\" VALUE {'pk': ?, 'pad': 'xxxx'}")
                .with_parameters(vec![s("u#1")])
                .with_return_consumed_capacity(ReturnConsumedCapacity::Total),
        )
        .await
        .unwrap();
    assert!(written.consumed_capacity.is_some());

    let read = mock
        .execute_statement(
            ExecuteStatementRequest::new("SELECT * FROM \"users\" WHERE pk = ?")
                .with_parameters(vec![s("u#1")])
                .with_return_consumed_capacity(ReturnConsumedCapacity::Total),
        )
        .await
        .unwrap();
    assert!(read.consumed_capacity.is_some());
}

#[cfg(feature = "streams")]
#[tokio::test]
async fn a_partiql_write_lands_in_the_stream_log() {
    let mock = MockDdb::new();
    mock.create_table(
        mock_ddb::CreateTableRequest::new("users", KeySchema::hash("pk", KeyType::String))
            .with_stream(mock_ddb::StreamViewType::NewAndOldImages),
    )
    .await
    .unwrap();

    mock.execute_statement(
        ExecuteStatementRequest::new("INSERT INTO \"users\" VALUE {'pk': ?}")
            .with_parameters(vec![s("u#1")]),
    )
    .await
    .unwrap();

    let arn = mock.list_streams().first().cloned().expect("a stream");
    let records = mock.get_records(&arn).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].keys["pk"], s("u#1"));
}

#[tokio::test]
async fn a_nested_document_round_trips() {
    let mock = users();
    mock.execute_statement(
        ExecuteStatementRequest::new(
            "INSERT INTO \"users\" VALUE {'pk': ?, 'meta': {'age': 42, 'ok': true}, 'tags': ['a', 'b'], 'missing': null}",
        )
        .with_parameters(vec![s("u#1")]),
    )
    .await
    .unwrap();

    let stored = mock
        .get_item(GetItemRequest::new("users", user_key("u#1")))
        .await
        .unwrap()
        .item
        .expect("the row");
    match &stored["meta"] {
        mock_ddb::AttributeValue::M(map) => {
            assert_eq!(map["age"], n("42"));
            assert_eq!(map["ok"], mock_ddb::AttributeValue::Bool(true));
        }
        other => panic!("expected a map, got {other:?}"),
    }
    assert!(matches!(stored["tags"], mock_ddb::AttributeValue::L(_)));
    assert_eq!(stored["missing"], mock_ddb::AttributeValue::Null(true));
    assert_eq!(stored["pk"], s("u#1"));
}

/// The three rows the `WHERE` cases run against: `u#1` has no nickname, `u#2`
/// has a string one and `u#3` a stored NULL.
fn where_fixture() -> MockDdb {
    use std::collections::HashMap;

    use mock_ddb::{item, AttributeValue};

    let mock = users();
    mock.seed(
        "users",
        vec![
            item([
                ("pk", s("u#1")),
                ("name", s("Ada")),
                ("age", n("36")),
                ("tags", AttributeValue::L(vec![s("a"), s("b")])),
                (
                    "meta",
                    AttributeValue::M(HashMap::from([("age".to_string(), n("36"))])),
                ),
                (
                    "items",
                    AttributeValue::L(vec![AttributeValue::M(HashMap::from([(
                        "sku".to_string(),
                        s("x"),
                    )]))]),
                ),
            ]),
            item([
                ("pk", s("u#2")),
                ("name", s("Bob")),
                ("age", n("20")),
                ("tags", AttributeValue::L(vec![s("b")])),
                ("nickname", s("bobby")),
            ]),
            item([
                ("pk", s("u#3")),
                ("name", s("Cid")),
                ("age", n("50")),
                ("tags", AttributeValue::L(Vec::new())),
                ("nickname", AttributeValue::Null(true)),
            ]),
        ],
    )
    .unwrap();
    mock
}

/// Run each `(statement, parameters, expected keys)` case and compare the
/// partition keys it returns.
async fn assert_cases(
    mock: &MockDdb,
    cases: Vec<(
        &'static str,
        Vec<mock_ddb::AttributeValue>,
        Vec<&'static str>,
    )>,
) {
    for (statement, parameters, expected) in cases {
        let output = mock
            .execute_statement(ExecuteStatementRequest::new(statement).with_parameters(parameters))
            .await
            .unwrap();
        let mut keys: Vec<String> = output
            .items
            .expect("a read returns items")
            .into_iter()
            .map(|row| match &row["pk"] {
                mock_ddb::AttributeValue::S(key) => key.clone(),
                other => panic!("expected a string key, got {other:?}"),
            })
            .collect();
        keys.sort();
        assert_eq!(
            keys.iter().map(String::as_str).collect::<Vec<_>>(),
            expected,
            "{statement}"
        );
    }
}

/// One `WHERE` construct per row of the documented table, against the same
/// three rows: every case names the partition keys it must return.
#[tokio::test]
async fn every_documented_where_construct_selects_the_same_rows() {
    let mock = where_fixture();
    assert_cases(&mock, documented_where_cases()).await;
}

/// One case per comparison, connective and function the documented `WHERE`
/// clause accepts, plus the two `IS` forms that ask different questions.
fn documented_where_cases() -> Vec<(
    &'static str,
    Vec<mock_ddb::AttributeValue>,
    Vec<&'static str>,
)> {
    use mock_ddb::AttributeValue;

    vec![
        (
            "SELECT * FROM \"users\" WHERE pk = ? AND age BETWEEN ? AND ?",
            vec![s("u#1"), n("30"), n("40")],
            vec!["u#1"],
        ),
        (
            "SELECT * FROM \"users\" WHERE pk = ? AND age > ?",
            vec![s("u#1"), n("36")],
            vec![],
        ),
        (
            "SELECT * FROM \"users\" WHERE pk = ? AND name IN (?, ?)",
            vec![s("u#2"), s("Bob"), s("Zoe")],
            vec!["u#2"],
        ),
        (
            "SELECT * FROM \"users\" WHERE pk = ? AND nickname IS MISSING",
            vec![s("u#1")],
            vec!["u#1"],
        ),
        (
            "SELECT * FROM \"users\" WHERE pk = ? AND nickname IS NULL",
            vec![s("u#3")],
            vec!["u#3"],
        ),
        (
            "SELECT * FROM \"users\" WHERE pk = ? AND nickname IS NOT MISSING",
            vec![s("u#3")],
            vec!["u#3"],
        ),
        (
            "SELECT * FROM \"users\" WHERE nickname IS NOT MISSING",
            vec![],
            vec!["u#2", "u#3"],
        ),
        (
            "SELECT * FROM \"users\" WHERE NOT (age > ?)",
            vec![n("30")],
            vec!["u#2"],
        ),
        (
            "SELECT * FROM \"users\" WHERE pk = ? AND (age > ? OR name = ?)",
            vec![s("u#1"), n("100"), s("Ada")],
            vec!["u#1"],
        ),
        (
            "SELECT * FROM \"users\" WHERE pk = ? AND begins_with(name, ?)",
            vec![s("u#2"), s("Bo")],
            vec!["u#2"],
        ),
        (
            "SELECT * FROM \"users\" WHERE pk = ? AND contains(tags, ?)",
            vec![s("u#2"), s("b")],
            vec!["u#2"],
        ),
        (
            "SELECT * FROM \"users\" WHERE pk = ? AND size(tags) > ?",
            vec![s("u#3"), n("0")],
            vec![],
        ),
        (
            "SELECT * FROM \"users\" WHERE pk = ? AND attribute_exists(nickname)",
            vec![s("u#2")],
            vec!["u#2"],
        ),
        (
            "SELECT * FROM \"users\" WHERE pk = ? AND attribute_type(name, ?)",
            vec![s("u#1"), s("S")],
            vec!["u#1"],
        ),
        (
            "SELECT * FROM \"users\" WHERE pk = ? AND meta.age = ?",
            vec![s("u#1"), n("36")],
            vec!["u#1"],
        ),
        (
            "SELECT * FROM \"users\" WHERE pk = ? AND items[0].sku = ?",
            vec![s("u#1"), s("x")],
            vec!["u#1"],
        ),
        // A stored NULL is not a missing attribute: `IS NOT NULL` has to reject
        // the row whose `nickname` holds one, and `= NULL` is not `IS NULL`.
        (
            "SELECT * FROM \"users\" WHERE pk = ? AND nickname IS NOT NULL",
            vec![s("u#3")],
            vec![],
        ),
        (
            "SELECT * FROM \"users\" WHERE pk = ? AND nickname = ?",
            vec![s("u#3"), AttributeValue::Null(true)],
            vec!["u#3"],
        ),
    ]
}

/// A `WHERE` clause that matches no row changes nothing and reports the same
/// condition class the native call does; an `UPDATE` never creates the row it
/// was aimed at.
#[tokio::test]
async fn a_write_that_matches_no_row_changes_nothing() {
    let mock = users();
    mock.seed("users", vec![user("u#1", "ada@example.com")])
        .unwrap();

    let error = mock
        .execute_statement(
            ExecuteStatementRequest::new(
                "UPDATE \"users\" SET email = ? WHERE pk = ? AND email = ?",
            )
            .with_parameters(vec![
                s("x@example.com"),
                s("u#1"),
                s("nobody@example.com"),
            ]),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "ConditionalCheckFailedException");

    let error = mock
        .execute_statement(
            ExecuteStatementRequest::new("DELETE FROM \"users\" WHERE pk = ? AND email = ?")
                .with_parameters(vec![s("u#1"), s("nobody@example.com")]),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "ConditionalCheckFailedException");

    let stored = mock
        .get_item(GetItemRequest::new("users", user_key("u#1")))
        .await
        .unwrap()
        .item
        .expect("the row");
    assert_eq!(stored["email"], s("ada@example.com"));
    assert_eq!(mock.count("users"), 1);

    // An `UPDATE` without a `WHERE` cannot name a row, so it is rejected before
    // anything is read — never an implicit insert, which the native call would
    // do with a bare key.
    let error = mock
        .execute_statement(
            ExecuteStatementRequest::new("UPDATE \"users\" SET email = ?")
                .with_parameters(vec![s("x@example.com")]),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "ValidationException");
    assert_eq!(mock.count("users"), 1);
}

/// `IS NULL` asks about an attribute's value, so a literal on its left is
/// rejected instead of being compared with something that cannot hold a NULL.
#[tokio::test]
async fn is_null_needs_an_attribute_path() {
    let mock = users();
    let error = mock
        .execute_statement(ExecuteStatementRequest::new(
            "SELECT * FROM \"users\" WHERE 'x' IS NULL",
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), "ValidationException");
    assert!(error.to_string().contains("attribute path"), "{error}");
}
