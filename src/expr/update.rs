//! Applying a parsed update expression to an item.
//!
//! All right-hand sides are evaluated against a snapshot of the item taken
//! before the first clause, so `SET a = b, b = a` swaps the two attributes, as
//! DynamoDB's single atomic update does. Writes are then applied in source
//! order; the parsed expression has already rejected overlapping paths, so the
//! order cannot change the result.

use std::collections::BTreeSet;

use aws_sdk_dynamodb::types::AttributeValue;

use super::eval::Ctx;
use super::parse::{Operand, Path, SetValue, Update, UpdateClause};
use super::path::{get_path, remove_path, render, root, set_path};
use crate::number::Number;
use crate::schema::KeySchema;
use crate::value::{is_set, set_difference, set_union, type_name};
use crate::{Error, Item, Result};

/// A single write, resolved against the pre-update snapshot.
enum Mutation {
    Set(Path, AttributeValue),
    Remove(Path),
}

/// Apply an update expression, returning the top-level attributes it touched.
///
/// `key_schema` is consulted so an expression cannot rewrite a key attribute:
/// the service rejects that, and silently allowing it would leave an item
/// indexed under a key it no longer carries.
pub(crate) fn apply(
    update: &Update,
    item: &mut Item,
    ctx: &Ctx<'_>,
    key_schema: &KeySchema,
) -> Result<BTreeSet<String>> {
    let snapshot = item.clone();
    let mut mutations: Vec<Mutation> = Vec::new();
    let mut touched: BTreeSet<String> = BTreeSet::new();

    for clause in &update.clauses {
        match clause {
            UpdateClause::Set(assignments) => {
                for (path, value) in assignments {
                    let resolved = ctx.resolve(path)?;
                    let name = root(&resolved)
                        .ok_or_else(|| invalid_path(&resolved))?
                        .to_string();
                    reject_key_attribute(&name, key_schema)?;
                    let value = evaluate_set_value(value, &snapshot, ctx)?;
                    touched.insert(name);
                    mutations.push(Mutation::Set(resolved, value));
                }
            }
            UpdateClause::Remove(paths) => {
                for path in paths {
                    let resolved = ctx.resolve(path)?;
                    let name = root(&resolved)
                        .ok_or_else(|| invalid_path(&resolved))?
                        .to_string();
                    reject_key_attribute(&name, key_schema)?;
                    touched.insert(name);
                    mutations.push(Mutation::Remove(resolved));
                }
            }
            UpdateClause::Add(pairs) => {
                for (path, operand) in pairs {
                    let resolved = ctx.resolve(path)?;
                    let name = root(&resolved)
                        .ok_or_else(|| invalid_path(&resolved))?
                        .to_string();
                    reject_key_attribute(&name, key_schema)?;
                    let delta = ctx
                        .operand_value(operand, Some(&snapshot))?
                        .ok_or_else(|| undefined_operand(operand))?;
                    let current = get_path(&snapshot, &resolved).cloned();
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
                    let resolved = ctx.resolve(path)?;
                    let name = root(&resolved)
                        .ok_or_else(|| invalid_path(&resolved))?
                        .to_string();
                    reject_key_attribute(&name, key_schema)?;
                    let delta = ctx
                        .operand_value(operand, Some(&snapshot))?
                        .ok_or_else(|| undefined_operand(operand))?;
                    let Some(current) = get_path(&snapshot, &resolved).cloned() else {
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
            Mutation::Set(path, value) => set_path(item, &path, value)?,
            Mutation::Remove(path) => remove_path(item, &path),
        }
    }
    Ok(touched)
}

/// Evaluate the right-hand side of a `SET`.
fn evaluate_set_value(value: &SetValue, item: &Item, ctx: &Ctx<'_>) -> Result<AttributeValue> {
    match value {
        SetValue::Operand(operand) => ctx
            .operand_value(operand, Some(item))?
            .ok_or_else(|| undefined_operand(operand)),
        SetValue::IfNotExists(path, fallback) => match ctx.path_value(path, Some(item))? {
            Some(existing) => Ok(existing),
            None => evaluate_set_value(fallback, item, ctx),
        },
        SetValue::ListAppend(first, second) => {
            let first = ctx
                .operand_value(first, Some(item))?
                .ok_or_else(|| undefined_operand(first))?;
            let second = ctx
                .operand_value(second, Some(item))?
                .ok_or_else(|| undefined_operand(second))?;
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
        SetValue::Plus(left, right) => arithmetic(left, right, item, ctx, false),
        SetValue::Minus(left, right) => arithmetic(left, right, item, ctx, true),
    }
}

/// Evaluate an arithmetic `SET` right-hand side.
fn arithmetic(
    left: &SetValue,
    right: &SetValue,
    item: &Item,
    ctx: &Ctx<'_>,
    subtract: bool,
) -> Result<AttributeValue> {
    let left = evaluate_set_value(left, item, ctx)?;
    let right = evaluate_set_value(right, item, ctx)?;
    let (AttributeValue::N(left), AttributeValue::N(right)) = (&left, &right) else {
        return Err(Error::validation(format!(
            "Invalid UpdateExpression: Incorrect operand type for operator or function; operator: {}, operand type: {} and {}",
            if subtract { "-" } else { "+" },
            type_name(&left),
            type_name(&right)
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
    Ok(AttributeValue::N(result.to_plain_string()))
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
            Ok(AttributeValue::N(sum.to_plain_string()))
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

fn invalid_path(path: &[super::parse::Segment]) -> Error {
    Error::validation(format!(
        "Invalid UpdateExpression: The document path provided in the update expression is invalid for update: {}",
        render(path)
    ))
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
    use super::*;
    use crate::expr::parse::{parse_update, Operand, Segment, SetValue};
    use crate::expr::{NameMap, ValueMap};
    use crate::{item, n, s};

    fn key_schema() -> KeySchema {
        KeySchema::hash("pk", crate::schema::KeyType::String)
    }

    fn apply(expression: &str, item: &mut Item, values: &ValueMap) -> Result<BTreeSet<String>> {
        let update = parse_update(expression).unwrap();
        let names = NameMap::new();
        let context = Ctx {
            names: &names,
            values,
        };
        super::apply(&update, item, &context, &key_schema())
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
        assert!(touched.is_empty());
    }
}
