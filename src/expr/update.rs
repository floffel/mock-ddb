//! Applying a parsed update expression to an item.
//!
//! All right-hand sides are evaluated against a snapshot of the item taken
//! before the first clause, so `SET a = b, b = a` swaps the two attributes, as
//! DynamoDB's single atomic update does. Writes are then applied in source
//! order; overlapping paths were rejected before the first write — by the
//! parser where the spellings already collide, and by the alias-resolved check
//! here for the rest — so the order cannot change the result.
//!
//! The writes land on a scratch copy that replaces the caller's item only once
//! all of them have succeeded: a write can still fail on its own — a nested
//! path whose parent does not exist, a list index past the end — and the
//! service writes nothing at all when it rejects an update. A caller that gets
//! an error therefore keeps exactly the item it passed in.

use std::borrow::Cow;
use std::collections::BTreeSet;

use aws_sdk_dynamodb::types::AttributeValue;

use super::eval::Ctx;
use super::parse::{reject_overlapping, ArithOp, Operand, Path, SetValue, Update, UpdateClause};
use super::path::{get_path, invalid_nested_path, remove_path, render, root, set_path};
use crate::number::Number;
use crate::schema::KeySchema;
use crate::value::{is_set, set_difference, set_union, type_name};
use crate::{Error, Item, Result};

/// A single write, resolved against the pre-update snapshot.
enum Mutation {
    Set(Path, AttributeValue),
    Remove(Path),
}

/// Reject an update whose target paths overlap once `#alias` segments are
/// resolved.
///
/// The parser compares the spellings it holds, so `SET #n = :a, n = :b` — or
/// two aliases that name one attribute — gets past it and would leave the item
/// with whichever write came last. Resolving first makes those spellings
/// collide, which is what the service reports instead.
fn reject_overlapping_targets(update: &Update, ctx: &Ctx<'_>) -> Result<()> {
    let resolved = update
        .target_paths()
        .into_iter()
        .map(|path| ctx.resolve(path).map(Cow::into_owned))
        .collect::<Result<Vec<Path>>>()?;
    let mut paths: Vec<&Path> = resolved.iter().collect();
    reject_overlapping(&mut paths)
}

/// Apply an update expression, returning the top-level attributes it touched.
///
/// `key_schema` is consulted so an expression cannot rewrite a key attribute:
/// the service rejects that, and silently allowing it would leave an item
/// indexed under a key it no longer carries.
///
/// The writes are staged on a copy of `item`, which is replaced only when every
/// one of them succeeded; on `Err` the caller's item is untouched.
pub(crate) fn apply(
    update: &Update,
    item: &mut Item,
    ctx: &Ctx<'_>,
    key_schema: &KeySchema,
) -> Result<BTreeSet<String>> {
    reject_overlapping_targets(update, ctx)?;
    let mut work = item.clone();
    let mut mutations: Vec<Mutation> = Vec::new();
    let mut touched: BTreeSet<String> = BTreeSet::new();

    for clause in &update.clauses {
        match clause {
            UpdateClause::Set(assignments) => {
                for (path, value) in assignments {
                    let resolved = ctx.resolve(path)?.into_owned();
                    let name = root(&resolved)
                        .ok_or_else(|| invalid_nested_path(&resolved))?
                        .to_string();
                    reject_key_attribute(&name, key_schema)?;
                    let value = evaluate_set_value(value, &work, ctx)?;
                    touched.insert(name);
                    mutations.push(Mutation::Set(resolved, value));
                }
            }
            UpdateClause::Remove(paths) => {
                for path in paths {
                    let resolved = ctx.resolve(path)?.into_owned();
                    let name = root(&resolved)
                        .ok_or_else(|| invalid_nested_path(&resolved))?
                        .to_string();
                    reject_key_attribute(&name, key_schema)?;
                    touched.insert(name);
                    mutations.push(Mutation::Remove(resolved));
                }
            }
            UpdateClause::Add(pairs) => {
                for (path, operand) in pairs {
                    let resolved = ctx.resolve(path)?.into_owned();
                    let name = root(&resolved)
                        .ok_or_else(|| invalid_nested_path(&resolved))?
                        .to_string();
                    reject_key_attribute(&name, key_schema)?;
                    let delta = ctx
                        .operand_value(operand, Some(&work))?
                        .map(Cow::into_owned)
                        .ok_or_else(|| undefined_operand(operand))?;
                    let current = get_path(&work, &resolved).cloned();
                    let updated = match current {
                        None => {
                            if !matches!(
                                delta,
                                AttributeValue::N(_)
                                    | AttributeValue::Ss(_)
                                    | AttributeValue::Ns(_)
                                    | AttributeValue::Bs(_)
                            ) {
                                return Err(add_type_error(&delta));
                            }
                            delta
                        }
                        Some(current) => add_values(&current, &delta)?,
                    };
                    touched.insert(name);
                    mutations.push(Mutation::Set(resolved, updated));
                }
            }
            UpdateClause::Delete(pairs) => {
                for (path, operand) in pairs {
                    let resolved = ctx.resolve(path)?.into_owned();
                    let name = root(&resolved)
                        .ok_or_else(|| invalid_nested_path(&resolved))?
                        .to_string();
                    reject_key_attribute(&name, key_schema)?;
                    let delta = ctx
                        .operand_value(operand, Some(&work))?
                        .map(Cow::into_owned)
                        .ok_or_else(|| undefined_operand(operand))?;
                    let Some(current) = get_path(&work, &resolved).cloned() else {
                        continue;
                    };
                    let remaining = set_difference(&current, &delta).ok_or_else(|| {
                        Error::validation(format!(
                            "Invalid UpdateExpression: Incorrect operand type for operator or function; operator: DELETE, operand type: {}",
                            type_name(&delta)
                        ))
                    })?;
                    touched.insert(name);
                    if is_empty_set(&remaining) {
                        mutations.push(Mutation::Remove(resolved));
                    } else {
                        mutations.push(Mutation::Set(resolved, remaining));
                    }
                }
            }
        }
    }

    for mutation in mutations {
        match mutation {
            Mutation::Set(path, value) => set_path(&mut work, &path, value)?,
            Mutation::Remove(path) => remove_path(&mut work, &path),
        }
    }
    *item = work;
    Ok(touched)
}

/// Evaluate the right-hand side of a `SET`.
fn evaluate_set_value(value: &SetValue, item: &Item, ctx: &Ctx<'_>) -> Result<AttributeValue> {
    match value {
        SetValue::Operand(operand) => ctx
            .operand_value(operand, Some(item))?
            .map(Cow::into_owned)
            .ok_or_else(|| undefined_operand(operand)),
        SetValue::IfNotExists(path, fallback) => match ctx.path_value(path, Some(item))? {
            Some(existing) => Ok(existing.clone()),
            None => evaluate_set_value(fallback, item, ctx),
        },
        SetValue::ListAppend(first, second) => {
            let first = evaluate_set_value(first, item, ctx)?;
            let second = evaluate_set_value(second, item, ctx)?;
            match (first, second) {
                (AttributeValue::L(mut left), AttributeValue::L(right)) => {
                    left.extend(right);
                    Ok(AttributeValue::L(left))
                }
                (left, right) => Err(Error::validation(format!(
                    "Invalid UpdateExpression: Incorrect operand type for operator or function; operator or function: list_append, operand type: {} and {}",
                    type_name(&left),
                    type_name(&right)
                ))),
            }
        }
        SetValue::Arithmetic { first, rest } => {
            let mut value = evaluate_set_value(first, item, ctx)?;
            for (operator, term) in rest {
                let right = evaluate_set_value(term, item, ctx)?;
                value = arithmetic(&value, &right, *operator)?;
            }
            Ok(value)
        }
    }
}

/// Apply one arithmetic step of a `SET` chain to the running total.
fn arithmetic(
    left: &AttributeValue,
    right: &AttributeValue,
    operator: ArithOp,
) -> Result<AttributeValue> {
    let subtract = operator == ArithOp::Sub;
    let (AttributeValue::N(left), AttributeValue::N(right)) = (left, right) else {
        return Err(Error::validation(format!(
            "Invalid UpdateExpression: Incorrect operand type for operator or function; operator: {}, operand type: {} and {}",
            if subtract { "-" } else { "+" },
            type_name(left),
            type_name(right)
        )));
    };
    let (Some(left), Some(right)) = (Number::parse(left), Number::parse(right)) else {
        return Err(Error::validation(
            "Invalid UpdateExpression: a number operand could not be parsed",
        ));
    };
    let result = if subtract {
        left.sub(&right)
    } else {
        left.add(&right)
    }
    .ok_or_else(|| Error::validation("Invalid UpdateExpression: number out of range"))?;
    let rendered = result
        .to_plain_string()
        .ok_or_else(|| Error::validation("Invalid UpdateExpression: number out of range"))?;
    Ok(AttributeValue::N(rendered))
}

/// `ADD` semantics: numbers add, sets union.
fn add_values(current: &AttributeValue, delta: &AttributeValue) -> Result<AttributeValue> {
    match (current, delta) {
        (AttributeValue::N(left), AttributeValue::N(right)) => {
            let (Some(left), Some(right)) = (Number::parse(left), Number::parse(right)) else {
                return Err(Error::validation(
                    "Invalid UpdateExpression: a number operand could not be parsed",
                ));
            };
            let sum = left.add(&right).ok_or_else(|| {
                Error::validation("Invalid UpdateExpression: number out of range")
            })?;
            let rendered = sum.to_plain_string().ok_or_else(|| {
                Error::validation("Invalid UpdateExpression: number out of range")
            })?;
            Ok(AttributeValue::N(rendered))
        }
        _ => set_union(current, delta).ok_or_else(|| add_type_error(delta)),
    }
}

fn add_type_error(delta: &AttributeValue) -> Error {
    Error::validation(format!(
        "Invalid UpdateExpression: Incorrect operand type for operator or function; operator: ADD, operand type: {}",
        type_name(delta)
    ))
}

fn undefined_operand(operand: &Operand) -> Error {
    match operand {
        Operand::Path(path) => Error::validation(format!(
            "Invalid UpdateExpression: The provided expression refers to an attribute that does not exist in the item: {}",
            render(path)
        )),
        _ => Error::validation(
            "Invalid UpdateExpression: The provided expression refers to an attribute that does not exist in the item",
        ),
    }
}

/// Reject an update that targets a key attribute.
fn reject_key_attribute(name: &str, key_schema: &KeySchema) -> Result<()> {
    if key_schema.contains(name) {
        return Err(Error::validation(format!(
            "One or more parameter values were invalid: Cannot update attribute {name}. This attribute is part of the key"
        )));
    }
    Ok(())
}

fn is_empty_set(value: &AttributeValue) -> bool {
    match value {
        // The three arms cannot merge: each set type has its own member type.
        #[allow(clippy::match_same_arms)]
        AttributeValue::Ss(members) => members.is_empty(),
        AttributeValue::Ns(members) => members.is_empty(),
        AttributeValue::Bs(members) => members.is_empty(),
        _ => !is_set(value),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::expr::parse::{parse_update, Operand, Segment, SetValue};
    use crate::expr::{NameMap, ValueMap};
    use crate::{item, n, s};

    fn key_schema() -> KeySchema {
        KeySchema::hash("pk", crate::schema::KeyType::String)
    }

    fn apply(expression: &str, item: &mut Item, values: &ValueMap) -> Result<BTreeSet<String>> {
        apply_named(expression, item, &NameMap::new(), values)
    }

    fn apply_named(
        expression: &str,
        item: &mut Item,
        names: &NameMap,
        values: &ValueMap,
    ) -> Result<BTreeSet<String>> {
        let update = parse_update(expression).unwrap();
        let context = Ctx { names, values };
        super::apply(&update, item, &context, &key_schema())
    }

    #[test]
    fn rejects_a_placeholder_and_its_literal_spelling() {
        let names = NameMap::from([("#n".to_string(), "n".to_string())]);
        let values = crate::values([(":a", s("first")), (":b", s("second"))]);
        let mut target = item([("pk", s("a"))]);
        let error = apply_named("SET #n = :a, n = :b", &mut target, &names, &values).unwrap_err();
        assert!(
            error.to_string().contains("Two document paths overlap"),
            "unexpected error: {error}"
        );
        assert!(!target.contains_key("n"));

        // Just inside: a name that merely shares a prefix is a second path.
        let mut target = item([("pk", s("a"))]);
        apply_named("SET #n = :a, nn = :b", &mut target, &names, &values).unwrap();
        assert_eq!(target.get("n"), Some(&s("first")));
        assert_eq!(target.get("nn"), Some(&s("second")));
    }

    #[test]
    fn rejects_two_placeholders_that_name_one_attribute() {
        let names = NameMap::from([
            ("#a".to_string(), "n".to_string()),
            ("#b".to_string(), "n".to_string()),
        ]);
        let values = crate::values([(":x", s("x")), (":y", s("y"))]);
        let mut target = item([("pk", s("a"))]);
        let error = apply_named("SET #a = :x, #b = :y", &mut target, &names, &values).unwrap_err();
        assert!(
            error.to_string().contains("Two document paths overlap"),
            "unexpected error: {error}"
        );

        // Just outside: the same expression with the second alias pointing at
        // another attribute is two distinct paths.
        let names = NameMap::from([
            ("#a".to_string(), "n".to_string()),
            ("#b".to_string(), "m".to_string()),
        ]);
        let mut target = item([("pk", s("a"))]);
        apply_named("SET #a = :x, #b = :y", &mut target, &names, &values).unwrap();
        assert_eq!(target.get("n"), Some(&s("x")));
        assert_eq!(target.get("m"), Some(&s("y")));
    }

    #[test]
    fn rejects_nested_paths_that_collide_only_once_resolved() {
        let names = NameMap::from([
            ("#n".to_string(), "n".to_string()),
            ("#m".to_string(), "m".to_string()),
        ]);
        let values = crate::values([(":x", s("x")), (":y", s("y"))]);
        let mut target = item([("pk", s("a"))]);
        let error =
            apply_named("SET #n.#m = :x, n.m = :y", &mut target, &names, &values).unwrap_err();
        assert!(
            error.to_string().contains("Two document paths overlap"),
            "unexpected error: {error}"
        );

        // Just inside: the literal spelling is a sibling, not the same path.
        let mut target = item([("pk", s("a")), ("n", AttributeValue::M(HashMap::new()))]);
        apply_named("SET #n.#m = :x, n.mm = :y", &mut target, &names, &values).unwrap();
        assert_eq!(
            target.get("n"),
            Some(&AttributeValue::M(HashMap::from([
                ("m".to_string(), s("x")),
                ("mm".to_string(), s("y")),
            ])))
        );
    }

    #[test]
    fn delete_keeps_the_members_that_were_not_removed() {
        let mut target = item([
            ("pk", s("a")),
            ("tags", AttributeValue::Ss(vec!["x".into(), "y".into()])),
        ]);
        let values = crate::values([(":one", AttributeValue::Ss(vec!["x".into()]))]);
        apply("DELETE tags :one", &mut target, &values).unwrap();
        assert_eq!(
            target.get("tags"),
            Some(&AttributeValue::Ss(vec!["y".into()]))
        );
    }

    #[test]
    fn delete_of_the_last_members_removes_the_attribute() {
        for (start, delta) in [
            (
                AttributeValue::Ss(vec!["x".into()]),
                AttributeValue::Ss(vec!["x".into()]),
            ),
            (
                AttributeValue::Ns(vec!["1".into()]),
                AttributeValue::Ns(vec!["1.0".into()]),
            ),
            (
                AttributeValue::Bs(vec![crate::Blob::new(vec![1])]),
                AttributeValue::Bs(vec![crate::Blob::new(vec![1])]),
            ),
        ] {
            let mut target = item([("pk", s("a")), ("members", start)]);
            let values = crate::values([(":members", delta)]);
            apply("DELETE members :members", &mut target, &values).unwrap();
            assert!(!target.contains_key("members"));
        }
    }

    #[test]
    fn if_not_exists_keeps_an_existing_value() {
        let mut target = item([("pk", s("a")), ("v", s("kept"))]);
        let values = crate::values([(":fallback", s("other"))]);
        apply("SET v = if_not_exists(v, :fallback)", &mut target, &values).unwrap();
        assert_eq!(target.get("v"), Some(&s("kept")));
    }

    #[test]
    fn add_rejects_an_unparseable_number() {
        let mut target = item([("pk", s("a")), ("count", n("1"))]);
        let values = crate::values([(":bad", n("not-a-number"))]);
        assert!(apply("ADD count :bad", &mut target, &values).is_err());
    }

    #[test]
    fn paths_must_start_with_an_attribute_name() {
        let update = parse_update("SET a = :v").unwrap();
        let broken = Update {
            clauses: vec![UpdateClause::Set(vec![(
                vec![Segment::Index(0)],
                SetValue::Operand(Operand::Value("v".to_string())),
            )])],
        };
        let names = NameMap::new();
        let values = crate::values([(":v", s("x"))]);
        let context = Ctx {
            names: &names,
            values: &values,
        };
        let mut target = item([("pk", s("a"))]);
        assert!(super::apply(&broken, &mut target, &context, &key_schema()).is_err());
        // The well-formed expression still applies.
        super::apply(&update, &mut target, &context, &key_schema()).unwrap();
    }

    #[test]
    fn undefined_size_operand_is_reported() {
        let mut target = item([("pk", s("a"))]);
        let values = ValueMap::new();
        let error = apply("SET v = size(absent)", &mut target, &values).unwrap_err();
        assert!(error.to_string().contains("does not exist"));
    }

    #[test]
    fn a_rejected_write_leaves_the_item_unchanged() {
        // The first clause writes, the second names a nested path whose parent
        // is not there, so the failure lands after a write was staged.
        let mut target = item([("pk", s("a")), ("v", s("old"))]);
        let original = target.clone();
        let values = crate::values([(":v", s("new"))]);
        let error = apply("SET v = :v, gone.nested = :v", &mut target, &values).unwrap_err();
        assert!(
            error.to_string().contains("invalid for update"),
            "unexpected error: {error}"
        );
        assert_eq!(target, original);

        // Just inside: with the parent present the same two clauses land.
        target.insert("gone".into(), AttributeValue::M(HashMap::new()));
        let original = target.clone();
        apply("SET v = :v, gone.nested = :v", &mut target, &values).unwrap();
        assert_eq!(target.get("v"), Some(&s("new")));
        assert_eq!(
            get_path(
                &target,
                &[Segment::Name("gone".into()), Segment::Name("nested".into())]
            ),
            Some(&s("new"))
        );
        // The clone above is only a guard: the successful run did change it.
        assert_ne!(target, original);
    }

    #[test]
    fn an_out_of_range_index_leaves_the_item_unchanged() {
        // `rows` holds one element, so index 0 is just inside and 1 is past the
        // end; the write before the failing one must not stick either.
        let mut target = item([("pk", s("a")), ("rows", AttributeValue::L(vec![s("x")]))]);
        let values = crate::values([(":v", s("y"))]);

        let original = target.clone();
        let error = apply("SET v = :v, rows[1] = :v", &mut target, &values).unwrap_err();
        assert!(
            error.to_string().contains("rows[1]"),
            "unexpected error: {error}"
        );
        assert_eq!(target, original);

        apply("SET v = :v, rows[0] = :v", &mut target, &values).unwrap();
        assert_eq!(target.get("v"), Some(&s("y")));
        assert_eq!(target.get("rows"), Some(&AttributeValue::L(vec![s("y")])));
    }

    #[test]
    fn empty_paths_are_internal_errors() {
        let mut target = item([("pk", s("a"))]);
        let names = NameMap::new();
        let values = ValueMap::new();
        let context = Ctx {
            names: &names,
            values: &values,
        };
        let update = parse_update("REMOVE nothing").unwrap();
        super::apply(&update, &mut target, &context, &key_schema()).unwrap();
        assert!(!target.contains_key("nothing"));
        let empty = Update { clauses: vec![] };
        let touched = super::apply(&empty, &mut target, &context, &key_schema()).unwrap();
        assert_eq!(touched.len(), 0);
    }
}
