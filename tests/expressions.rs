//! The expression language, end to end: conditions, updates and projections.

mod common;

use std::collections::HashMap;

use common::users;
use mock_ddb::{
    item, n, s, values, AttributeValue, ConditionExpression, GetItemRequest, Item, MockDdb,
    ProjectionExpression, PutItemRequest, UpdateExpression, UpdateItemRequest,
};

/// An item exercising every value shape the expression evaluator handles.
fn rich_item() -> Item {
    item([
        ("pk", s("u#1")),
        ("count", n("3")),
        ("price", n("19.99")),
        ("name", s("Alpha Bravo")),
        (
            "tags",
            AttributeValue::Ss(vec!["red".into(), "blue".into()]),
        ),
        ("scores", AttributeValue::Ns(vec!["1".into(), "2.5".into()])),
        (
            "blobs",
            AttributeValue::Bs(vec![mock_ddb::Blob::new(vec![1_u8, 2])]),
        ),
        (
            "profile",
            AttributeValue::M(HashMap::from([
                ("city".to_string(), s("Berlin")),
                ("zip".to_string(), s("10115")),
            ])),
        ),
        ("items", AttributeValue::L(vec![s("first"), s("second")])),
        ("active", AttributeValue::Bool(true)),
        ("nothing", AttributeValue::Null(true)),
    ])
}

fn check(expression: &str, values: &[(&str, AttributeValue)]) -> bool {
    let item = rich_item();
    let attribute_values: HashMap<String, AttributeValue> = values
        .iter()
        .map(|(key, value)| (key.to_string(), value.clone()))
        .collect();
    ConditionExpression::parse(expression)
        .unwrap()
        .evaluate(Some(&item), &HashMap::new(), &attribute_values)
        .unwrap()
}

#[test]
fn comparison_operators() {
    assert!(check("count = :v", &[(":v", n("3.0"))]));
    assert!(check("count <> :v", &[(":v", n("4"))]));
    assert!(check("count < :v", &[(":v", n("4"))]));
    assert!(check("count <= :v", &[(":v", n("3"))]));
    assert!(check("count > :v", &[(":v", n("2"))]));
    assert!(check("count >= :v", &[(":v", n("3"))]));
    assert!(check("name >= :v", &[(":v", s("Alpha"))]));
    assert!(!check("name < :v", &[(":v", s("Alpha"))]));
    // Missing attributes never satisfy a comparison.
    assert!(!check("absent = :v", &[(":v", s("x"))]));
    assert!(!check("absent <> :v", &[(":v", s("x"))]));
    assert!(!check("absent < :v", &[(":v", s("x"))]));
}

#[test]
fn between_and_in_lists() {
    assert!(check(
        "price BETWEEN :low AND :high",
        &[(":low", n("10")), (":high", n("20"))]
    ));
    assert!(!check(
        "price BETWEEN :low AND :high",
        &[(":low", n("20")), (":high", n("30"))]
    ));
    assert!(check(
        "name IN (:a, :b)",
        &[(":a", s("Alpha Bravo")), (":b", s("other"))]
    ));
    assert!(!check("count IN (:a)", &[(":a", n("4"))]));
    assert!(!check("absent IN (:a)", &[(":a", n("4"))]));
}

#[test]
fn existence_type_and_functions() {
    assert!(check("attribute_exists(name)", &[]));
    assert!(!check("attribute_exists(absent)", &[]));
    assert!(check("attribute_not_exists(absent)", &[]));
    assert!(!check("attribute_not_exists(name)", &[]));
    assert!(check("attribute_type(name, :t)", &[(":t", s("S"))]));
    assert!(check("attribute_type(tags, :t)", &[(":t", s("SS"))]));
    assert!(check("attribute_type(nothing, :t)", &[(":t", s("NULL"))]));
    assert!(check("attribute_type(active, :t)", &[(":t", s("BOOL"))]));
    assert!(check("attribute_type(profile, :t)", &[(":t", s("M"))]));
    assert!(!check("attribute_type(name, :t)", &[(":t", s("N"))]));

    assert!(check("begins_with(name, :p)", &[(":p", s("Alpha"))]));
    assert!(!check("begins_with(name, :p)", &[(":p", s("Beta"))]));
    assert!(check("contains(name, :p)", &[(":p", s("Bravo"))]));
    assert!(check("contains(tags, :p)", &[(":p", s("red"))]));
    assert!(check("contains(scores, :p)", &[(":p", n("2.5"))]));
    assert!(check("contains(items, :p)", &[(":p", s("second"))]));
    assert!(check(
        "contains(blobs, :p)",
        &[(":p", AttributeValue::B(mock_ddb::Blob::new(vec![1_u8, 2])))]
    ));
    assert!(!check("contains(absent, :p)", &[(":p", s("x"))]));
    assert!(!check("contains(count, :p)", &[(":p", s("3"))]));
}

#[test]
fn size_operand_compares_as_a_number() {
    assert!(check("size(name) = :v", &[(":v", n("11"))]));
    assert!(check("size(tags) = :v", &[(":v", n("2"))]));
    assert!(check("size(profile) = :v", &[(":v", n("2"))]));
    assert!(check("size(items) > :v", &[(":v", n("1"))]));
    assert!(!check("size(absent) > :v", &[(":v", n("0"))]));
}

#[test]
fn boolean_connectives_and_precedence() {
    assert!(check(
        "count = :three AND name = :name",
        &[(":three", n("3")), (":name", s("Alpha Bravo"))]
    ));
    assert!(check(
        "count = :four OR name = :name",
        &[(":four", n("4")), (":name", s("Alpha Bravo"))]
    ));
    assert!(check("NOT count = :four", &[(":four", n("4"))]));
    // AND binds tighter than OR: (false AND false) OR true.
    assert!(check(
        "count = :four AND name = :name OR count = :three",
        &[(":four", n("4")), (":name", s("nope")), (":three", n("3"))]
    ));
    assert!(check(
        "(count = :four OR count = :three) AND name = :name",
        &[
            (":four", n("4")),
            (":three", n("3")),
            (":name", s("Alpha Bravo"))
        ]
    ));
    assert!(check(
        "NOT (count = :four OR count = :five)",
        &[(":four", n("4")), (":five", n("5"))]
    ));
}

#[test]
fn nested_paths_in_conditions() {
    assert!(check("profile.city = :v", &[(":v", s("Berlin"))]));
    assert!(check("profile.zip = :v", &[(":v", s("10115"))]));
    assert!(!check("profile.missing = :v", &[(":v", s("x"))]));
    assert!(check("items[0] = :v", &[(":v", s("first"))]));
    assert!(!check("items[5] = :v", &[(":v", s("first"))]));
}

#[test]
fn aliases_resolve_in_conditions() {
    let item = rich_item();
    let names = mock_ddb::names([("#c", "count"), ("#p", "profile"), ("#city", "city")]);
    let attribute_values = values([(":three", n("3"))]);
    let condition = ConditionExpression::parse("#c = :three AND #p.#city = :berlin").unwrap();
    let attribute_values = {
        let mut map = attribute_values;
        map.insert(":berlin".to_string(), s("Berlin"));
        map
    };
    assert!(condition
        .evaluate(Some(&item), &names, &attribute_values)
        .unwrap());
}

#[test]
fn undefined_placeholders_and_incomparable_types_are_errors() {
    let item = rich_item();
    let empty_names = HashMap::new();
    let empty_values = HashMap::new();

    let undefined_name = ConditionExpression::parse("#missing = :v").unwrap();
    assert!(undefined_name
        .evaluate(Some(&item), &empty_names, &empty_values)
        .is_err());

    let undefined_value = ConditionExpression::parse("count = :missing").unwrap();
    assert!(undefined_value
        .evaluate(Some(&item), &empty_names, &empty_values)
        .is_err());

    let mixed = ConditionExpression::parse("count < :text").unwrap();
    let attribute_values = values([(":text", s("abc"))]);
    assert!(mixed
        .evaluate(Some(&item), &empty_names, &attribute_values)
        .is_err());

    let type_operand = ConditionExpression::parse("attribute_type(name, :t)").unwrap();
    let attribute_values = values([(":t", n("1"))]);
    assert!(type_operand
        .evaluate(Some(&item), &empty_names, &attribute_values)
        .is_err());
}

#[tokio::test]
async fn update_clauses_apply_in_one_expression() {
    let mock = users();
    mock.put_item(PutItemRequest::new("users", rich_item()))
        .await
        .unwrap();

    mock.update_item(
        UpdateItemRequest::new(
            "users",
            item([("pk", s("u#1"))]),
            "SET #n = :name, count = count + :one, items = list_append(items, :more), \
             added = if_not_exists(added, :fallback) REMOVE tags ADD scores :extra DELETE blobs :gone",
        )
        .with_names(mock_ddb::names([("#n", "name")]))
        .with_values(values([
            (":name", s("Renamed")),
            (":one", n("1")),
            (":more", AttributeValue::L(vec![s("third")])),
            (":fallback", s("default")),
            (":extra", AttributeValue::Ns(vec!["9".into()])),
            (
                ":gone",
                AttributeValue::Bs(vec![mock_ddb::Blob::new(vec![1_u8, 2])]),
            ),
        ])),
    )
    .await
    .unwrap();

    let stored = mock
        .get_item(GetItemRequest::new("users", item([("pk", s("u#1"))])))
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(stored.get("name"), Some(&s("Renamed")));
    assert_eq!(stored.get("count"), Some(&n("4")));
    assert_eq!(
        stored.get("items"),
        Some(&AttributeValue::L(vec![
            s("first"),
            s("second"),
            s("third")
        ]))
    );
    assert_eq!(stored.get("added"), Some(&s("default")));
    assert!(!stored.contains_key("tags"));
    assert_eq!(
        stored.get("scores"),
        Some(&AttributeValue::Ns(vec![
            "1".into(),
            "2.5".into(),
            "9".into()
        ]))
    );
    // Deleting every member leaves no attribute behind.
    assert!(!stored.contains_key("blobs"));
}

#[tokio::test]
async fn nested_writes_and_list_indices() {
    let mock = users();
    mock.put_item(PutItemRequest::new("users", rich_item()))
        .await
        .unwrap();

    mock.update_item(
        UpdateItemRequest::new(
            "users",
            item([("pk", s("u#1"))]),
            "SET profile.city = :city, items[1] = :second",
        )
        .with_values(values([(":city", s("Munich")), (":second", s("SECOND"))])),
    )
    .await
    .unwrap();

    let stored = mock
        .get_item(GetItemRequest::new("users", item([("pk", s("u#1"))])))
        .await
        .unwrap()
        .item
        .unwrap();
    let profile = stored.get("profile").unwrap().as_m().unwrap();
    assert_eq!(profile.get("city"), Some(&s("Munich")));
    let items = stored.get("items").unwrap().as_l().unwrap();
    assert_eq!(items[1], s("SECOND"));

    let missing_parent = mock
        .update_item(
            UpdateItemRequest::new(
                "users",
                item([("pk", s("u#1"))]),
                "SET no_such_map.key = :v",
            )
            .with_values(values([(":v", s("x"))])),
        )
        .await;
    assert!(missing_parent.is_err());

    let out_of_range = mock
        .update_item(
            UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "SET items[9] = :v")
                .with_values(values([(":v", s("x"))])),
        )
        .await;
    assert!(out_of_range.is_err());
}

#[tokio::test]
async fn add_and_delete_type_rules() {
    let mock = users();
    mock.put_item(PutItemRequest::new(
        "users",
        item([
            ("pk", s("u#1")),
            ("count", n("1")),
            ("tags", AttributeValue::Ss(vec!["a".into()])),
        ]),
    ))
    .await
    .unwrap();

    mock.update_item(
        UpdateItemRequest::new(
            "users",
            item([("pk", s("u#1"))]),
            "ADD count :two, tags :b, fresh :start",
        )
        .with_values(values([
            (":two", n("2")),
            (":b", AttributeValue::Ss(vec!["b".into()])),
            (":start", n("100")),
        ])),
    )
    .await
    .unwrap();

    let stored = mock
        .get_item(GetItemRequest::new("users", item([("pk", s("u#1"))])))
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(stored.get("count"), Some(&n("3")));
    assert_eq!(stored.get("fresh"), Some(&n("100")));
    let tags = stored.get("tags").unwrap().as_ss().unwrap();
    assert_eq!(tags.len(), 2);

    let wrong_type = mock
        .update_item(
            UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "ADD count :text")
                .with_values(values([(":text", s("x"))])),
        )
        .await;
    assert!(wrong_type.is_err());

    let delete_wrong_type = mock
        .update_item(
            UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "DELETE count :text")
                .with_values(values([(":text", s("x"))])),
        )
        .await;
    assert!(delete_wrong_type.is_err());
}

#[tokio::test]
async fn set_against_missing_attribute_and_missing_parent() {
    let mock = users();
    mock.put_item(PutItemRequest::new("users", item([("pk", s("u#1"))])))
        .await
        .unwrap();

    let missing_rhs = mock
        .update_item(UpdateItemRequest::new(
            "users",
            item([("pk", s("u#1"))]),
            "SET a = absent",
        ))
        .await;
    assert!(missing_rhs.is_err());

    let missing_operand = mock
        .update_item(UpdateItemRequest::new(
            "users",
            item([("pk", s("u#1"))]),
            "SET a = :absent",
        ))
        .await;
    assert!(missing_operand.is_err());

    // A no-op assignment is still valid.
    mock.update_item(
        UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "SET a = :v")
            .with_values(values([(":v", s("x"))])),
    )
    .await
    .unwrap();
}

#[test]
fn update_expressions_are_idempotent_where_semantics_say_so() {
    let names = HashMap::new();
    let attribute_values = values([(":v", s("fixed")), (":one", n("1"))]);

    let once = UpdateExpression::parse("SET state = :v, version = :one").unwrap();
    let twice = once.clone();
    let mut item = item([("pk", s("u#1"))]);
    once.apply(&mut item, &names, &attribute_values).unwrap();
    let after_once = item.clone();
    twice.apply(&mut item, &names, &attribute_values).unwrap();
    assert_eq!(item, after_once);

    // Orphaned Idempotent invariants are exactly what the mock must preserve.
    let read_once = mock_ddb::ConditionExpression::parse("state = :v").unwrap();
    assert!(read_once
        .evaluate(Some(&item), &names, &attribute_values)
        .unwrap());
}

#[test]
fn arithmetic_and_list_append_type_errors() {
    let mut item = item([("count", n("1")), ("name", s("x"))]);
    let names = HashMap::new();
    let attribute_values = values([(":two", n("2")), (":list", AttributeValue::L(vec![s("a")]))]);

    let add = UpdateExpression::parse("SET count = count + :two").unwrap();
    add.apply(&mut item, &names, &attribute_values).unwrap();
    assert_eq!(item.get("count"), Some(&n("3")));

    let sub = UpdateExpression::parse("SET count = count - :two").unwrap();
    sub.apply(&mut item, &names, &attribute_values).unwrap();
    assert_eq!(item.get("count"), Some(&n("1")));

    let bad_add = UpdateExpression::parse("SET name = name + :two").unwrap();
    assert!(bad_add
        .apply(&mut item.clone(), &names, &attribute_values)
        .is_err());

    let bad_append = UpdateExpression::parse("SET name = list_append(name, :list)").unwrap();
    assert!(bad_append
        .apply(&mut item.clone(), &names, &attribute_values)
        .is_err());
}

#[test]
fn projection_expressions_keep_nested_and_list_paths() {
    let item = rich_item();
    let projection = ProjectionExpression::parse("profile.city, items[0], count").unwrap();
    let projected = projection.project(&item, &HashMap::new()).unwrap();
    assert_eq!(projected.len(), 3);
    assert_eq!(projected.get("count"), Some(&n("3")));
    let profile = projected.get("profile").unwrap().as_m().unwrap();
    assert_eq!(profile.get("city"), Some(&s("Berlin")));
    assert!(profile.get("zip").is_none());
    // A list index projects the containing list.
    assert!(projected.get("items").unwrap().as_l().is_ok());

    assert!(ProjectionExpression::parse("").is_err());
    assert!(ProjectionExpression::parse("a,,b").is_err());
    assert!(ProjectionExpression::parse("#undefined")
        .unwrap()
        .project(&item, &HashMap::new())
        .is_err());
}

#[tokio::test]
async fn conditions_apply_to_creation_and_deletion() {
    let mock = MockDdb::with_table(
        "users",
        mock_ddb::KeySchema::hash("pk", mock_ddb::KeyType::String),
    )
    .unwrap();

    mock.put_item(
        PutItemRequest::new("users", item([("pk", s("u#1"))]))
            .with_condition("attribute_not_exists(pk)"),
    )
    .await
    .unwrap();

    mock.delete_item(
        mock_ddb::DeleteItemRequest::new("users", item([("pk", s("u#1"))]))
            .with_condition("attribute_exists(pk)"),
    )
    .await
    .unwrap();
    assert_eq!(mock.count("users"), 0);
}
