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
//! an error therefore keeps exactly the item it passed in. The copy is taken
//! after the right-hand sides are evaluated, so a rejected expression never
//! allocates one.
//!
//! Nothing here may grow a value without a bound. `list_append(a, a)` doubles a
//! list, and a nested one doubles again per level, so the concatenation is
//! measured *before* it is built and refused once it would pass the item limit;
//! the finished item is measured again before it replaces the caller's. An
//! expression can therefore never allocate an item the service would refuse to
//! store.

use std::borrow::Cow;
use std::collections::BTreeSet;

use aws_sdk_dynamodb::types::AttributeValue;

use super::eval::Ctx;
use super::parse::{reject_overlapping, ArithOp, Operand, Path, SetValue, Update, UpdateClause};
use super::path::{get_path, invalid_nested_path, remove_path, render, root, set_path};
use crate::number::Number;
use crate::schema::KeySchema;
use crate::value::{
    is_set, item_size, item_too_large, list_size, set_difference, set_union, type_name,
    MAX_ITEM_SIZE,
};
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
                    let value = evaluate_set_value(value, item, ctx, &render(&resolved))?.value;
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
                        .operand_value(operand, Some(item))?
                        .map(Cow::into_owned)
                        .ok_or_else(|| undefined_operand(operand))?;
                    let rendered = render(&resolved);
                    let delta = validate_add_delta(&delta, &rendered)?;
                    let current = get_path(item, &resolved).cloned();
                    let updated = match current {
                        None => delta,
                        Some(current) => add_values(&current, &delta, &rendered)?,
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
                        .operand_value(operand, Some(item))?
                        .map(Cow::into_owned)
                        .ok_or_else(|| undefined_operand(operand))?;
                    let Some(current) = get_path(item, &resolved).cloned() else {
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

    let mut work = item.clone();
    for mutation in mutations {
        match mutation {
            Mutation::Set(path, value) => set_path(&mut work, &path, value)?,
            Mutation::Remove(path) => remove_path(&mut work, &path),
        }
    }
    // The service refuses an item past the limit, so the caller gets the
    // refusal instead of an item it could never have stored itself.
    let size = item_size(&work);
    if size > MAX_ITEM_SIZE {
        return Err(item_too_large(size));
    }
    *item = work;
    Ok(touched)
}

/// An evaluated `SET` right-hand side, plus where the numbers in it came from.
///
/// The origin decides the error class when a number cannot be read, so it is
/// carried from the operand that produced the value up to the arithmetic that
/// rejects it (see [`unusable_number`]).
#[derive(Debug)]
struct Evaluated {
    /// The value the expression produced.
    value: AttributeValue,
    /// Whether it was read out of the item or carried by the request.
    origin: Origin,
}

/// Whether an evaluated value was read out of the item or carried by the
/// request.
///
/// A `:placeholder` the caller sent may hold a number the mock cannot read, and
/// that is a rejected request. A value read out of the item cannot be
/// unreadable — every write validates what it stores — so the same failure
/// there is a broken invariant of the store and must not be reported as the
/// caller's mistake.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Origin {
    /// Read out of the item: an attribute path, or `size()` of one.
    Item,
    /// Carried by the request: a `:placeholder` value.
    Request,
}

impl Origin {
    /// The origin of a value built out of two sides.
    ///
    /// The item-derived side wins: it is the one the request cannot have
    /// corrupted, so a result built from it inherits its provenance rather than
    /// hiding it behind a request value.
    fn both(self, other: Self) -> Self {
        if self == Self::Item || other == Self::Item {
            Self::Item
        } else {
            Self::Request
        }
    }
}

/// Evaluate the right-hand side of a `SET`.
///
/// `path` is the rendered assignment target. Numbers travel as text, so a
/// value that does not parse is only discovered where the arithmetic reads it;
/// naming the attribute there is the difference between a test failure that
/// points at one attribute and one that points at the expression.
fn evaluate_set_value(
    value: &SetValue,
    item: &Item,
    ctx: &Ctx<'_>,
    path: &str,
) -> Result<Evaluated> {
    match value {
        SetValue::Operand(operand) => {
            let origin = match operand {
                Operand::Value(_) => Origin::Request,
                Operand::Path(_) | Operand::Size(_) => Origin::Item,
            };
            let value = ctx
                .operand_value(operand, Some(item))?
                .map(Cow::into_owned)
                .ok_or_else(|| undefined_operand(operand))?;
            Ok(Evaluated { value, origin })
        }
        SetValue::IfNotExists(absolute, fallback) => match ctx.path_value(absolute, Some(item))? {
            Some(existing) => Ok(Evaluated {
                value: existing.clone(),
                origin: Origin::Item,
            }),
            None => evaluate_set_value(fallback, item, ctx, path),
        },
        SetValue::ListAppend(first, second) => {
            let first = evaluate_set_value(first, item, ctx, path)?;
            let second = evaluate_set_value(second, item, ctx, path)?;
            let origin = first.origin.both(second.origin);
            match (first.value, second.value) {
                (AttributeValue::L(mut left), AttributeValue::L(right)) => {
                    // Measure before joining: `list_append(a, a)` on an item
                    // near the limit would otherwise allocate a value the
                    // service can never store, and each nesting level would
                    // double again. The sum is the exact size of the result,
                    // so the boundary falls where the item limit does.
                    let grown = list_size(&left) + list_size(&right);
                    if grown > MAX_ITEM_SIZE {
                        return Err(item_too_large(grown));
                    }
                    left.extend(right);
                    Ok(Evaluated {
                        value: AttributeValue::L(left),
                        origin,
                    })
                }
                (left, right) => Err(Error::validation(format!(
                    "Invalid UpdateExpression: Incorrect operand type for operator or function; operator or function: list_append, operand type: {} and {}",
                    type_name(&left),
                    type_name(&right)
                ))),
            }
        }
        SetValue::Arithmetic { first, rest } => {
            let mut total = evaluate_set_value(first, item, ctx, path)?;
            for (operator, term) in rest {
                let right = evaluate_set_value(term, item, ctx, path)?;
                total = arithmetic(&total, &right, *operator, path)?;
            }
            Ok(total)
        }
    }
}

/// Apply one arithmetic step of a `SET` chain to the running total.
fn arithmetic(
    left: &Evaluated,
    right: &Evaluated,
    operator: ArithOp,
    path: &str,
) -> Result<Evaluated> {
    let subtract = operator == ArithOp::Sub;
    let sign = if subtract { "-" } else { "+" };
    let (AttributeValue::N(left_text), AttributeValue::N(right_text)) = (&left.value, &right.value)
    else {
        return Err(Error::validation(format!(
            "Invalid UpdateExpression: Incorrect operand type for operator or function; operator: {sign}, operand type: {} and {}",
            type_name(&left.value),
            type_name(&right.value)
        )));
    };
    let left_number =
        Number::parse(left_text).ok_or_else(|| unusable_number(left_text, left.origin, path))?;
    let right_number =
        Number::parse(right_text).ok_or_else(|| unusable_number(right_text, right.origin, path))?;
    let result = if subtract {
        left_number.sub(&right_number)
    } else {
        left_number.add(&right_number)
    }
    .ok_or_else(|| number_overflow(path, &format!("{left_text} {sign} {right_text}")))?;
    let rendered = result
        .to_plain_string()
        .ok_or_else(|| number_overflow(path, &format!("{left_text} {sign} {right_text}")))?;
    Ok(Evaluated {
        value: AttributeValue::N(rendered),
        origin: left.origin.both(right.origin),
    })
}

/// Validate an `ADD` operand and return the value the item stores.
///
/// `ADD` on an absent attribute stores its delta as it stands, so the checks
/// belong here rather than on the arithmetic path alone: an unparseable number
/// or an empty set would otherwise land in the item, where `PutItem` would have
/// refused the same value and every later arithmetic read of it fails. Both
/// arms run through this, so a present and an absent attribute reject the same
/// operands with the same errors.
///
/// A number comes back rendered plain (`1e3` → `1000`), the form the sum path
/// produces. Set members keep their spelling, as they do on the union path.
fn validate_add_delta(delta: &AttributeValue, path: &str) -> Result<AttributeValue> {
    match delta {
        AttributeValue::N(text) => Ok(AttributeValue::N(render_number(text, path)?)),
        AttributeValue::Ns(members) => {
            refuse_empty_set(members.is_empty(), "number", path)?;
            match members
                .iter()
                .find(|member| Number::parse(member).is_none())
            {
                Some(member) => Err(unusable_number(member, Origin::Request, path)),
                None => Ok(delta.clone()),
            }
        }
        AttributeValue::Ss(members) => {
            refuse_empty_set(members.is_empty(), "string", path)?;
            Ok(delta.clone())
        }
        AttributeValue::Bs(members) => {
            refuse_empty_set(members.is_empty(), "binary", path)?;
            Ok(delta.clone())
        }
        _ => Err(add_type_error(delta)),
    }
}

/// The error for a number the arithmetic cannot read.
///
/// Where the text came from decides the class, because it decides who has to
/// act. A `:placeholder` the caller sent is a rejected request, worded the way
/// `PutItem` words the same text. A number read out of the item cannot be
/// unreadable — `ADD` and `SET` both run their result through the same
/// validation `PutItem` uses — so the mismatch is a broken invariant of the
/// store: answering with a `ValidationException` would tell the caller to fix a
/// request that is fine, and would leave the corrupt value invisible.
///
/// Both branches name the attribute and the value, because that is what a
/// failing test has to point at.
fn unusable_number(text: &str, origin: Origin, path: &str) -> Error {
    match origin {
        Origin::Request => Error::validation(format!(
            "The parameter cannot be converted to a numeric value: {text} (attribute: {path})"
        )),
        Origin::Item => Error::internal(format!(
            "the item holds `{text}` at `{path}`, which is not a number"
        )),
    }
}

/// The error for a result outside DynamoDB's number range.
///
/// The service words an overflow as a `ValidationException`: both operands were
/// storable on their own, the sum is not, and the request is what asked for it.
/// `detail` names the operands so the failure says which sum ran out of range
/// instead of only that one did.
fn number_overflow(path: &str, detail: &str) -> Error {
    Error::validation(format!(
        "Number overflow. Attempting to store a number with magnitude larger than supported range (attribute: {path}, {detail})"
    ))
}

/// Parse a number operand and render it the way the item stores it.
fn render_number(text: &str, path: &str) -> Result<String> {
    let number = Number::parse(text).ok_or_else(|| unusable_number(text, Origin::Request, path))?;
    number
        .to_plain_string()
        .ok_or_else(|| number_overflow(path, text))
}

/// Reject an empty set operand, with the wording `PutItem` uses for one.
fn refuse_empty_set(is_empty: bool, kind: &str, path: &str) -> Result<()> {
    if is_empty {
        return Err(Error::validation(format!(
            "One or more parameter values were invalid: An {kind} set may not be empty ({path})"
        )));
    }
    Ok(())
}

/// `ADD` semantics: numbers add, sets union.
///
/// The stored side is read here, so it is the one place a corrupt number in the
/// item can surface: it is reported as the broken invariant it is rather than as
/// the caller's mistake. The delta was parsed by [`validate_add_delta`], so its
/// arm cannot fire; it is spelled out so a future caller cannot lose the class.
fn add_values(
    current: &AttributeValue,
    delta: &AttributeValue,
    path: &str,
) -> Result<AttributeValue> {
    match (current, delta) {
        (AttributeValue::N(left), AttributeValue::N(right)) => {
            let stored =
                Number::parse(left).ok_or_else(|| unusable_number(left, Origin::Item, path))?;
            let addition = Number::parse(right)
                .ok_or_else(|| unusable_number(right, Origin::Request, path))?;
            let sum = stored
                .add(&addition)
                .ok_or_else(|| number_overflow(path, &format!("{left} + {right}")))?;
            Ok(AttributeValue::N(sum.to_plain_string().ok_or_else(
                || number_overflow(path, &format!("{left} + {right}")),
            )?))
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
    fn a_set_is_refused_once_the_item_passes_the_limit() {
        // pk: name 2 + value 1 = 3 bytes, so a payload of MAX - 7 fills the
        // item exactly.
        let fitting = "x".repeat(MAX_ITEM_SIZE - 7);
        let mut target = item([("pk", s("a")), ("blob", s("x"))]);
        let values = crate::values([(":v", s(&fitting))]);
        apply("SET blob = :v", &mut target, &values).unwrap();
        assert_eq!(item_size(&target), MAX_ITEM_SIZE);

        // One byte more is past the limit, and the item keeps the value it had.
        let too_large = "x".repeat(MAX_ITEM_SIZE - 6);
        let mut target = item([("pk", s("a")), ("blob", s("kept"))]);
        let values = crate::values([(":v", s(&too_large))]);
        let error = apply("SET blob = :v", &mut target, &values).unwrap_err();
        assert_eq!(error.code(), "ValidationException");
        assert!(
            error.to_string().contains("maximum allowed size"),
            "unexpected error: {error}"
        );
        assert_eq!(target.get("blob"), Some(&s("kept")));
    }

    #[test]
    fn list_append_refuses_to_double_a_list_past_the_limit() {
        // Two elements of MAX/2 - 16 bytes each: the list itself is inside the
        // limit, the concatenation with itself is not.
        let element = "x".repeat(MAX_ITEM_SIZE / 2 - 16);
        let list = AttributeValue::L(vec![s(&element), s(&element)]);
        let mut target = item([("pk", s("a")), ("l", list.clone())]);
        let error = apply("SET l = list_append(l, l)", &mut target, &ValueMap::new()).unwrap_err();
        assert_eq!(error.code(), "ValidationException");
        assert!(
            error.to_string().contains("maximum allowed size"),
            "unexpected error: {error}"
        );
        // The refusal happens before the copy: the caller's list is untouched.
        assert_eq!(target.get("l"), Some(&list));
    }

    #[test]
    fn nested_list_appends_are_refused_where_they_would_cross_the_limit() {
        // Base 163 KiB: the doubling fits, the quadrupling does not.
        let element = "x".repeat(80 * 1024);
        let list = AttributeValue::L(vec![s(&element), s(&element)]);

        let mut doubling = item([("pk", s("a")), ("l", list.clone())]);
        apply("SET l = list_append(l, l)", &mut doubling, &ValueMap::new()).unwrap();
        assert!(
            item_size(&doubling) < MAX_ITEM_SIZE,
            "the doubling itself has to fit for this test to say anything"
        );

        let mut quad = item([("pk", s("a")), ("l", list)]);
        let before = quad.get("l").cloned();
        let error = apply(
            "SET l = list_append(list_append(l, l), list_append(l, l))",
            &mut quad,
            &ValueMap::new(),
        )
        .unwrap_err();
        assert_eq!(error.code(), "ValidationException");
        assert_eq!(quad.get("l").cloned(), before);
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
    fn add_rejects_an_unparseable_number_on_both_sides_of_the_boundary() {
        let values = crate::values([(":bad", n("not-a-number")), (":one", n("1"))]);

        // Present attribute: the delta is parsed before it is added, and the
        // failure names the attribute it was meant for.
        let mut target = item([("pk", s("a")), ("count", n("1"))]);
        let error = apply("ADD count :bad", &mut target, &values).unwrap_err();
        assert_eq!(error.code(), "ValidationException");
        assert!(
            error.to_string().contains(
                "The parameter cannot be converted to a numeric value: not-a-number (attribute: count)"
            ),
            "unexpected error: {error}"
        );
        assert_eq!(target.get("count"), Some(&n("1")));

        // Absent attribute: the delta used to be stored verbatim, which put an
        // unparseable number into the item.
        let mut target = item([("pk", s("a"))]);
        let error = apply("ADD absent :bad", &mut target, &values).unwrap_err();
        assert_eq!(error.code(), "ValidationException");
        assert!(
            error.to_string().contains(
                "The parameter cannot be converted to a numeric value: not-a-number (attribute: absent)"
            ),
            "unexpected error: {error}"
        );
        assert!(!target.contains_key("absent"));

        // Just inside: the same shape with a parseable operand lands.
        let mut target = item([("pk", s("a"))]);
        apply("ADD absent :one", &mut target, &values).unwrap();
        assert_eq!(target.get("absent"), Some(&n("1")));
    }

    #[test]
    fn add_stores_a_missing_number_in_the_rendering_the_sum_path_produces() {
        let values = crate::values([(":n", n("1e3")), (":far", n("1e500"))]);

        let mut target = item([("pk", s("a"))]);
        apply("ADD absent :n", &mut target, &values).unwrap();
        assert_eq!(target.get("absent"), Some(&n("1000")));

        // Just outside: a number that cannot be rendered is refused instead of
        // being stored in a spelling no read could use.
        let mut target = item([("pk", s("a"))]);
        let error = apply("ADD absent :far", &mut target, &values).unwrap_err();
        assert_eq!(error.code(), "ValidationException");
        assert!(
            error.to_string().contains(
                "Number overflow. Attempting to store a number with magnitude larger than supported range (attribute: absent, 1e500)"
            ),
            "unexpected error: {error}"
        );
        assert!(!target.contains_key("absent"));
    }

    #[test]
    fn set_arithmetic_names_the_attribute_and_the_operands() {
        let values = crate::values([
            (":bad", n("not-a-number")),
            (":one", n("1")),
            (":huge", n("1e500")),
            (":zero", n("0")),
        ]);

        // A number the request carried that does not parse: the caller's
        // mistake, still a rejected request, but now named with the attribute
        // the assignment would have written.
        let mut target = item([("pk", s("a")), ("count", n("1"))]);
        let error = apply("SET count = count + :bad", &mut target, &values).unwrap_err();
        assert_eq!(error.code(), "ValidationException");
        assert!(
            error
                .to_string()
                .contains("not-a-number (attribute: count)"),
            "unexpected error: {error}"
        );
        assert_eq!(target.get("count"), Some(&n("1")));

        // Just inside: the same expression with a readable operand lands.
        apply("SET count = count + :one", &mut target, &values).unwrap();
        assert_eq!(target.get("count"), Some(&n("2")));

        // An operand too far from the stored one to align: the sum leaves the
        // range DynamoDB can store, and the failure names the sum.
        let mut target = item([("pk", s("a")), ("total", n("1"))]);
        let error = apply("SET total = total + :huge", &mut target, &values).unwrap_err();
        assert_eq!(error.code(), "ValidationException");
        assert!(
            error.to_string().contains(
                "Number overflow. Attempting to store a number with magnitude larger than supported range (attribute: total, 1 + 1e500)"
            ),
            "unexpected error: {error}"
        );
        assert_eq!(target.get("total"), Some(&n("1")));

        // Just inside the same boundary: an operand that aligns still sums.
        apply("SET total = total + :zero", &mut target, &values).unwrap();
        assert_eq!(target.get("total"), Some(&n("1")));
    }

    #[test]
    fn a_number_read_out_of_the_item_is_not_reported_as_the_callers_mistake() {
        // No write can leave an unreadable number behind, so this is a broken
        // invariant of the store, not a request to reject: it names the
        // attribute and the value and carries the internal class.
        let error = add_values(&n("not a number"), &n("1"), "count").unwrap_err();
        assert_eq!(error.code(), "InternalServerError");
        assert!(
            error
                .to_string()
                .contains("the item holds `not a number` at `count`, which is not a number"),
            "unexpected error: {error}"
        );

        // The same failure on the arithmetic path keeps the origin of the side
        // that carried it.
        let stored = Evaluated {
            value: n("not a number"),
            origin: Origin::Item,
        };
        let request = Evaluated {
            value: n("1"),
            origin: Origin::Request,
        };
        let error = arithmetic(&stored, &request, ArithOp::Add, "count").unwrap_err();
        assert_eq!(error.code(), "InternalServerError");
        assert!(
            error.to_string().contains("at `count`"),
            "unexpected error: {error}"
        );

        // Just outside: the identical text on the request side is the caller's
        // mistake and stays a `ValidationException`.
        let managed = Evaluated {
            value: n("1"),
            origin: Origin::Item,
        };
        let carried = Evaluated {
            value: n("not a number"),
            origin: Origin::Request,
        };
        let error = arithmetic(&managed, &carried, ArithOp::Add, "count").unwrap_err();
        assert_eq!(error.code(), "ValidationException");
        assert!(
            error
                .to_string()
                .contains("not a number (attribute: count)"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn add_refuses_an_empty_set_wherever_the_attribute_is() {
        use crate::Blob;

        let values = crate::values([
            (":empty_s", AttributeValue::Ss(Vec::new())),
            (":empty_n", AttributeValue::Ns(Vec::new())),
            (":empty_b", AttributeValue::Bs(Vec::new())),
        ]);

        // Just outside: the empty set would become the stored value (absent) or
        // leave one behind (present, where the union keeps it).
        let cases = [
            (
                AttributeValue::Ss(vec!["seed".into()]),
                ":empty_s",
                "string",
            ),
            (AttributeValue::Ns(vec!["1".into()]), ":empty_n", "number"),
            (
                AttributeValue::Bs(vec![Blob::new(vec![1u8])]),
                ":empty_b",
                "binary",
            ),
        ];
        for (present, operand, kind) in cases {
            let mut target = item([("pk", s("a"))]);
            let error = apply(&format!("ADD absent {operand}"), &mut target, &values).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains(&format!("{kind} set may not be empty")),
                "unexpected error: {error}"
            );
            assert!(!target.contains_key("absent"));

            let mut target = item([("pk", s("a")), ("existing", present.clone())]);
            let error =
                apply(&format!("ADD existing {operand}"), &mut target, &values).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains(&format!("{kind} set may not be empty")),
                "unexpected error: {error}"
            );
            assert_eq!(target.get("existing"), Some(&present));
        }

        // Just inside: a set with one member is accepted on both sides.
        let values = crate::values([
            (":one_s", AttributeValue::Ss(vec!["a".into()])),
            (":bad_n", AttributeValue::Ns(vec!["nope".into()])),
            (":one_n", AttributeValue::Ns(vec!["1".into()])),
        ]);

        let mut target = item([("pk", s("a"))]);
        apply("ADD absent :one_s", &mut target, &values).unwrap();
        assert_eq!(
            target.get("absent"),
            Some(&AttributeValue::Ss(vec!["a".into()]))
        );

        // A number-set member has to parse, too, and the failure names it.
        let mut target = item([("pk", s("a"))]);
        let error = apply("ADD absent :bad_n", &mut target, &values).unwrap_err();
        assert_eq!(error.code(), "ValidationException");
        assert!(
            error.to_string().contains(
                "The parameter cannot be converted to a numeric value: nope (attribute: absent)"
            ),
            "unexpected error: {error}"
        );

        let mut target = item([("pk", s("a"))]);
        apply("ADD absent :one_n", &mut target, &values).unwrap();
        assert_eq!(
            target.get("absent"),
            Some(&AttributeValue::Ns(vec!["1".into()]))
        );
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
