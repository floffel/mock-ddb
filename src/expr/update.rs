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
use super::parse::{
    reject_overlapping, ArithOp, Operand, Path, Segment, SetValue, Update, UpdateClause,
};
use super::path::{get_path, invalid_nested_path, remove_path, render, root, set_path};
use crate::number::Number;
use crate::schema::KeySchema;
use crate::value::{
    item_size, item_too_large, list_size, set_difference, set_union, type_name,
    validate_set_members, SetMemberError, MAX_ITEM_SIZE,
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
                    let updated = apply_add(item, ctx, &resolved, operand, &render(&resolved))?;
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
                    let rendered = render(&resolved);
                    // The operand is checked before the item is consulted, so
                    // a wrong operand is refused whether or not the attribute
                    // happens to exist. Skipping the gate for a missing path
                    // would let a client bug hide behind the stored state.
                    validate_delete_delta(&delta, &rendered)?;
                    validate_set_members(&delta)
                        .map_err(|error| set_member_error(error, &rendered, Origin::Request))?;
                    // A path that is not there is the no-op the service
                    // reports: nothing to remove, nothing written.
                    let Some(current) = get_path(item, &resolved).cloned() else {
                        continue;
                    };
                    // Only the stored side can be wrong now, so it is the one
                    // the error names: a set of another member type cannot be
                    // subtracted from this value, and a malformed stored set is
                    // an invariant failure rather than the caller's request.
                    let remaining = set_difference(&current, &delta)
                        .map_err(|error| set_member_error(error, &rendered, Origin::Item))?
                        .ok_or_else(|| delete_type_error(&current))?;
                    // A value the mock cannot classify is never "empty": that
                    // answer removes the attribute outright, and it is the one
                    // outcome that loses members nobody counted. Today
                    // [`set_difference`] builds only the three known set types,
                    // so the refusal answers for its extension rather than for
                    // a state this code can reach.
                    let drained = empty_set(&remaining)
                        .ok_or_else(|| unclassified_set(&remaining, &rendered))?;
                    touched.insert(name);
                    if drained {
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

/// Apply one `ADD` pair and return the value to store.
///
/// A numeric delta is an operand, not necessarily a storable value by itself.
/// `0.0001e-129` has a scale beyond the plain-form guard, yet adding it to an
/// in-range value can still produce an in-range sum. Parse it in [`add_values`],
/// where the sum is checked, instead of rendering it first. Other delta types
/// keep the up-front checks that refuse malformed set members.
fn apply_add(
    item: &Item,
    ctx: &Ctx<'_>,
    resolved: &[Segment],
    operand: &Operand,
    rendered: &str,
) -> Result<AttributeValue> {
    let delta = ctx
        .operand_value(operand, Some(item))?
        .map(Cow::into_owned)
        .ok_or_else(|| undefined_operand(operand))?;
    let Some(current) = get_path(item, resolved).cloned() else {
        return validate_add_delta(&delta, rendered);
    };
    if matches!(
        (&current, &delta),
        (AttributeValue::N(_), AttributeValue::N(_))
    ) {
        add_values(&current, &delta, rendered)
    } else {
        let delta = validate_add_delta(&delta, rendered)?;
        add_values(&current, &delta, rendered)
    }
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
/// `ADD` on an absent attribute stores its delta as it stands, so an
/// unparseable number or an empty set would otherwise land in the item, where
/// `PutItem` would have refused the same value and every later arithmetic read
/// of it fails. A present numeric attribute is different: its delta is only an
/// operand, so [`add_values`] parses it and checks the sum instead.
///
/// A number comes back rendered plain (`1e3` → `1000`), the form the sum path
/// produces. Set members keep their spelling, as they do on the union path.
fn validate_add_delta(delta: &AttributeValue, path: &str) -> Result<AttributeValue> {
    match delta {
        AttributeValue::N(text) => Ok(AttributeValue::N(render_number(text, path)?)),
        AttributeValue::Ns(members) => {
            refuse_empty_set(members.is_empty(), "number", path)?;
            validate_set_members(delta)
                .map_err(|error| set_member_error(error, path, Origin::Request))?;
            Ok(delta.clone())
        }
        AttributeValue::Ss(members) => {
            refuse_empty_set(members.is_empty(), "string", path)?;
            validate_set_members(delta)
                .map_err(|error| set_member_error(error, path, Origin::Request))?;
            Ok(delta.clone())
        }
        AttributeValue::Bs(members) => {
            refuse_empty_set(members.is_empty(), "binary", path)?;
            validate_set_members(delta)
                .map_err(|error| set_member_error(error, path, Origin::Request))?;
            Ok(delta.clone())
        }
        _ => Err(add_type_error(delta)),
    }
}

/// Validate a `DELETE` operand.
///
/// `DELETE` subtracts members from a set, so the operand has to be a set of
/// its own: a scalar can never name a member. The check belongs here, before
/// the item is read, because the service validates the operand independently
/// of the item — `DELETE tags :one` is the same rejected request whether or not
/// `tags` is stored, and a mock that only checked where the path resolved
/// accepted it on one item and refused it on the next.
///
/// Empty sets are refused with the wording `PutItem` uses, as `ADD` does: an
/// empty operand can neither add nor remove anything, and storing it is what
/// the service rejects. The member type of the *stored* set is not checked
/// here — [`set_difference`] does that once the current value is known, and
/// reports it against the value that is wrong.
fn validate_delete_delta(delta: &AttributeValue, path: &str) -> Result<()> {
    match delta {
        AttributeValue::Ss(members) => refuse_empty_set(members.is_empty(), "string", path),
        AttributeValue::Ns(members) => refuse_empty_set(members.is_empty(), "number", path),
        AttributeValue::Bs(members) => refuse_empty_set(members.is_empty(), "binary", path),
        _ => Err(delete_type_error(delta)),
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
    let shown = crate::number::excerpt(text);
    match origin {
        Origin::Request => Error::validation(format!(
            "The parameter cannot be converted to a numeric value: {shown} (attribute: {path})"
        )),
        Origin::Item => Error::internal(format!(
            "the item holds `{shown}` at `{path}`, which is not a number"
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

/// `ADD` semantics: numbers add, sets of one member type union.
///
/// The stored side is read here, so it is the one place a corrupt number in the
/// item can surface: it is reported as the broken invariant it is rather than as
/// the caller's mistake. The delta was parsed by [`validate_add_delta`], so its
/// arm cannot fire; it is spelled out so a future caller cannot lose the class.
///
/// The set pairs are named here instead of being inferred from
/// [`set_union`]'s `None`, because that `None` means "not a pair I handle" and
/// what it handles grows with the SDK: the day it learns another set type, a
/// value the update cannot read would start unioning instead of being refused.
/// Naming the pairs keeps `ADD` closed to everything else.
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
        (AttributeValue::Ss(_), AttributeValue::Ss(_))
        | (AttributeValue::Ns(_), AttributeValue::Ns(_))
        | (AttributeValue::Bs(_), AttributeValue::Bs(_)) => set_union(current, delta)
            .map_err(|error| set_member_error(error, path, Origin::Item))?
            .ok_or_else(|| add_type_error(delta)),
        _ => Err(add_type_error(delta)),
    }
}

/// Turn a malformed-set verdict into the error class appropriate to its source.
fn set_member_error(error: SetMemberError, path: &str, origin: Origin) -> Error {
    match error {
        SetMemberError::InvalidNumber(member) => unusable_number(&member, origin, path),
        SetMemberError::EmptyString | SetMemberError::EmptyBinary | SetMemberError::Duplicate => {
            match origin {
                Origin::Request => Error::validation(set_member_request_message(&error, path)),
                Origin::Item => Error::internal(format!(
                    "the item holds an invalid set at `{path}`: {}",
                    set_member_request_message(&error, path)
                )),
            }
        }
        SetMemberError::NotASet => match origin {
            Origin::Request => Error::validation(format!(
                "One or more parameter values were invalid: The value at {path} is not a set"
            )),
            Origin::Item => Error::internal(format!("the item holds a non-set value at `{path}`")),
        },
    }
}

fn set_member_request_message(error: &SetMemberError, path: &str) -> String {
    match error {
        SetMemberError::EmptyString => format!(
            "One or more parameter values were invalid: An string set may not have an empty string as a member ({path})"
        ),
        SetMemberError::EmptyBinary => format!(
            "One or more parameter values were invalid: An binary set may not have an empty binary as a member ({path})"
        ),
        SetMemberError::Duplicate => format!(
            "One or more parameter values were invalid: Input collection contains duplicates ({path})"
        ),
        SetMemberError::InvalidNumber(member) => format!(
            "The parameter cannot be converted to a numeric value: {} (attribute: {path})",
            crate::number::excerpt(member)
        ),
        SetMemberError::NotASet => format!(
            "One or more parameter values were invalid: The value at {path} is not a set"
        ),
    }
}

fn add_type_error(delta: &AttributeValue) -> Error {
    Error::validation(format!(
        "Invalid UpdateExpression: Incorrect operand type for operator or function; operator: ADD, operand type: {}",
        type_name(delta)
    ))
}

/// The error for a `DELETE` value that is not a set of the stored member type.
///
/// The wording is the service's: one message for both directions, naming the
/// value that cannot take part in the subtraction — the operand when it is not
/// a set at all, the stored attribute when its member type is another one.
fn delete_type_error(offending: &AttributeValue) -> Error {
    Error::validation(format!(
        "Invalid UpdateExpression: Incorrect operand type for operator or function; operator: DELETE, operand type: {}",
        type_name(offending)
    ))
}

/// The error for a stored value the update cannot subtract from because it is
/// not one of the three set types — reached only if [`set_difference`] starts
/// returning a shape [`empty_set`] does not know.
///
/// It is an internal error, not a `ValidationException`: the request was fine,
/// the store handed back something the mock cannot read, and telling the caller
/// to change its expression would hide that.
fn unclassified_set(value: &AttributeValue, path: &str) -> Error {
    Error::internal(format!(
        "the item holds a {} at `{path}`, which is not a set the update can classify",
        type_name(value)
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

/// Whether a stored value is an empty set, or `None` if it is not a set this
/// update can read.
///
/// The two answers used to be one: everything that is not `Ss`/`Ns`/`Bs` was
/// reported as an *empty* set, which is the answer that removes the attribute.
/// A variant a later SDK release adds — `AttributeValue` is `#[non_exhaustive]`
/// — would therefore have been deleted wholesale the moment
/// [`set_difference`] learned to subtract it. Keeping "not a set" apart from
/// "an empty set" is what makes that fail closed.
fn empty_set(value: &AttributeValue) -> Option<bool> {
    match value {
        // The three arms cannot merge: each set type has its own member type.
        #[allow(clippy::match_same_arms)]
        AttributeValue::Ss(members) => Some(members.is_empty()),
        AttributeValue::Ns(members) => Some(members.is_empty()),
        AttributeValue::Bs(members) => Some(members.is_empty()),
        // A scalar, a list, a map, a boolean, a null — or a variant that did
        // not exist when this match was written.
        _ => None,
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
    fn delete_validates_the_operand_wherever_the_attribute_is() {
        let values = crate::values([
            (":one", s("one")),
            (":x", AttributeValue::Ss(vec!["x".into()])),
            (":numbers", AttributeValue::Ns(vec!["1".into()])),
            (":empty", AttributeValue::Ss(Vec::new())),
        ]);
        let stored = AttributeValue::Ss(vec!["x".into(), "y".into()]);

        // Just outside, on both sides of the boundary that used to decide the
        // outcome: a scalar can never name a member, so the absent attribute is
        // refused just like the stored one instead of hiding it as a no-op.
        for present in [false, true] {
            let mut target = item([("pk", s("a"))]);
            if present {
                target.insert("tags".into(), stored.clone());
            }
            let error = apply("DELETE tags :one", &mut target, &values).unwrap_err();
            assert_eq!(error.code(), "ValidationException");
            assert!(
                error
                    .to_string()
                    .contains("operator: DELETE, operand type: S"),
                "unexpected error: {error}"
            );
            assert_eq!(target.get("tags"), present.then_some(&stored));

            // An empty operand is refused the same way, wherever the attribute
            // is: it can neither remove a member nor be stored.
            let error = apply("DELETE tags :empty", &mut target, &values).unwrap_err();
            assert!(
                error.to_string().contains("string set may not be empty"),
                "unexpected error: {error}"
            );
            assert_eq!(target.get("tags"), present.then_some(&stored));
        }

        // A set of another member type is a valid operand, so only the stored
        // side can refuse it: absent it is the no-op, present the mismatch.
        let mut target = item([("pk", s("a"))]);
        apply("DELETE tags :numbers", &mut target, &values).unwrap();
        assert!(!target.contains_key("tags"));

        let mut target = item([("pk", s("a")), ("tags", stored.clone())]);
        let error = apply("DELETE tags :numbers", &mut target, &values).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("operator: DELETE, operand type: SS"),
            "unexpected error: {error}"
        );
        assert_eq!(target.get("tags"), Some(&stored));

        // Just inside: with a set operand the missing path is a plain no-op —
        // it writes nothing and reports nothing as touched.
        let mut target = item([("pk", s("a"))]);
        let touched = apply("DELETE tags :x", &mut target, &values).unwrap();
        assert!(touched.is_empty());
        assert!(!target.contains_key("tags"));

        // And on a stored set it removes the member, which is the write the
        // no-op above is the absence of.
        let mut target = item([("pk", s("a")), ("tags", stored)]);
        let touched = apply("DELETE tags :x", &mut target, &values).unwrap();
        assert_eq!(touched.iter().collect::<Vec<_>>(), vec!["tags"]);
        assert_eq!(
            target.get("tags"),
            Some(&AttributeValue::Ss(vec!["y".into()]))
        );
    }

    #[test]
    fn delete_rejects_a_malformed_number_operand_before_lookup() {
        let values = crate::values([(":bad", AttributeValue::Ns(vec!["nope".into()]))]);
        for present in [false, true] {
            let mut target = item([("pk", s("a"))]);
            if present {
                target.insert("members".into(), AttributeValue::Ns(vec!["1".into()]));
            }
            let error = apply("DELETE members :bad", &mut target, &values).unwrap_err();
            assert_eq!(error.code(), "ValidationException");
            assert!(
                error.to_string().contains(
                    "The parameter cannot be converted to a numeric value: nope (attribute: members)"
                ),
                "unexpected error: {error}"
            );
            assert_eq!(
                target.get("members"),
                present.then_some(&AttributeValue::Ns(vec!["1".into()]))
            );
        }
    }

    #[test]
    fn add_rejects_duplicate_set_members_after_normalization() {
        let values = crate::values([
            (":dup_n", AttributeValue::Ns(vec!["1".into(), "1.0".into()])),
            (":dup_s", AttributeValue::Ss(vec!["x".into(), "x".into()])),
            (":empty_s", AttributeValue::Ss(vec![String::new()])),
        ]);
        for (operand, expected) in [
            (":dup_n", "Input collection contains duplicates"),
            (":dup_s", "Input collection contains duplicates"),
            (":empty_s", "empty string as a member"),
        ] {
            let mut target = item([("pk", s("a"))]);
            let error = apply(&format!("ADD members {operand}"), &mut target, &values).unwrap_err();
            assert_eq!(error.code(), "ValidationException");
            assert!(
                error.to_string().contains(expected),
                "expected `{expected}` in `{error}`"
            );
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
    fn add_parses_a_present_numeric_delta_before_rendering_it() {
        // `0.0001e-129` normalises to a scale of 133, so it cannot render on
        // its own even though adding it can produce a storable value. Rendering
        // the operand before the sum used to report a false overflow (#151).
        let values = crate::values([(":tiny", n("0.0001e-129"))]);
        let mut target = item([("pk", s("a")), ("v", n("1.2345e-129"))]);

        apply("ADD v :tiny", &mut target, &values).unwrap();

        assert_eq!(
            target.get("v"),
            Some(&n(format!("0.{}12346", "0".repeat(128))))
        );
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
    fn a_value_that_is_not_a_set_is_never_reported_as_an_empty_set() {
        // Just inside: each of the three set types answers for its own members.
        assert_eq!(empty_set(&AttributeValue::Ss(Vec::new())), Some(true));
        assert_eq!(
            empty_set(&AttributeValue::Ss(vec!["a".into()])),
            Some(false)
        );
        assert_eq!(
            empty_set(&AttributeValue::Ns(vec!["1".into()])),
            Some(false)
        );
        assert_eq!(
            empty_set(&AttributeValue::Bs(vec![crate::Blob::new(vec![1])])),
            Some(false)
        );

        // Just outside: every other shape is not a set, and "not a set" must
        // not reach the caller as "empty" — that answer removes the attribute.
        // A variant a later SDK release adds lands in this branch too.
        for value in [
            AttributeValue::S("x".into()),
            AttributeValue::N("1".into()),
            AttributeValue::B(crate::Blob::new(vec![1])),
            AttributeValue::Bool(true),
            AttributeValue::Null(true),
            AttributeValue::L(Vec::new()),
            AttributeValue::M(HashMap::new()),
        ] {
            assert_eq!(
                empty_set(&value),
                None,
                "{} is not a set and must not be classified as one",
                type_name(&value)
            );
        }
    }

    #[test]
    fn a_stored_value_of_another_type_is_refused_instead_of_unioned() {
        let strings = AttributeValue::Ss(vec!["a".into()]);

        // Just inside: a stored set of the delta's own type unions.
        let united = add_values(&AttributeValue::Ss(vec!["x".into()]), &strings, "tags").unwrap();
        assert_eq!(united, AttributeValue::Ss(vec!["x".into(), "a".into()]));

        // Just outside: a stored scalar, and a stored set of another member
        // type, both name the operand the service holds responsible.
        let error = add_values(&AttributeValue::S("x".into()), &strings, "tags").unwrap_err();
        assert_eq!(error.code(), "ValidationException");
        assert!(
            error
                .to_string()
                .contains("operator: ADD, operand type: SS"),
            "unexpected error: {error}"
        );

        let numbers = AttributeValue::Ns(vec!["1".into()]);
        let error = add_values(&strings, &numbers, "tags").unwrap_err();
        assert_eq!(error.code(), "ValidationException");
        assert!(
            error
                .to_string()
                .contains("operator: ADD, operand type: NS"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn an_unclassifiable_stored_value_is_an_internal_error() {
        // The guard that keeps a DELETE from removing an attribute whose
        // remaining members nobody counted. It is unreachable while
        // `set_difference` builds only the three known set types, so it is
        // exercised here rather than through an expression.
        let error = unclassified_set(&AttributeValue::Bool(true), "tags");
        assert_eq!(error.code(), "InternalServerError");
        assert!(
            error.to_string().contains(
                "the item holds a BOOL at `tags`, which is not a set the update can classify"
            ),
            "unexpected error: {error}"
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

    // The two invariants the module documents hold for *every* input, not for
    // the examples above: each right-hand side reads the snapshot taken before
    // the first clause, and the source order of the writes cannot change the
    // result. Asserting them over generated input is the point of the block
    // below — a hand-written example can only ever show one path through them.

    use proptest::prelude::*;

    /// A value the update can read, small enough to stay far from every limit.
    fn stored_value() -> impl Strategy<Value = AttributeValue> {
        prop_oneof![
            "[a-z]{0,8}".prop_map(s),
            small_number().prop_map(n),
            prop::collection::vec(any::<u8>(), 0..4)
                .prop_map(|bytes| AttributeValue::B(crate::Blob::new(bytes))),
            prop::collection::vec("[a-z]{1,4}", 0..3)
                .prop_map(|members| { AttributeValue::L(members.iter().map(s).collect()) }),
            member_set(),
        ]
    }

    /// A number the mock can read and render, far enough inside the service's
    /// range that adding two of them cannot leave it.
    fn small_number() -> impl Strategy<Value = String> {
        (-1_000_000_i64..1_000_000).prop_map(|value| value.to_string())
    }

    /// A set of distinct members of one type: a set never holds a member twice,
    /// and one with no members is refused on write.
    fn member_set() -> impl Strategy<Value = AttributeValue> {
        let strings = prop::collection::hash_set("[a-z]{1,6}", 1..5)
            .prop_map(|members| AttributeValue::Ss(members.into_iter().collect()));
        let numbers = prop::collection::hash_set(-1_000_i64..1_000, 1..5)
            .prop_map(|members| AttributeValue::Ns(members.iter().map(i64::to_string).collect()));
        let binaries = prop::collection::hash_set(prop::collection::vec(any::<u8>(), 1..4), 1..5)
            .prop_map(|members| {
                AttributeValue::Bs(members.into_iter().map(crate::Blob::new).collect())
            });
        prop_oneof![strings, numbers, binaries]
    }

    /// Two non-empty sets of one member type: the stored one and the delta an
    /// `ADD` or `DELETE` would carry.
    fn same_type_sets() -> impl Strategy<Value = (AttributeValue, AttributeValue)> {
        (
            0_usize..3,
            prop::collection::hash_set("[a-z]{1,6}", 1..4),
            prop::collection::hash_set("[a-z]{1,3}", 1..3),
            prop::collection::hash_set(1_i64..1_000, 1..4),
            prop::collection::hash_set(1_i64..1_000, 1..3),
            prop::collection::hash_set(prop::collection::vec(any::<u8>(), 1..4), 1..4),
            prop::collection::hash_set(prop::collection::vec(any::<u8>(), 1..4), 1..3),
        )
            .prop_map(
                |(
                    kind,
                    stored_text,
                    delta_text,
                    stored_number,
                    delta_number,
                    stored_binary,
                    delta_binary,
                )| match kind {
                    0 => (
                        AttributeValue::Ss(stored_text.into_iter().collect()),
                        AttributeValue::Ss(delta_text.into_iter().collect()),
                    ),
                    1 => (
                        AttributeValue::Ns(stored_number.iter().map(i64::to_string).collect()),
                        AttributeValue::Ns(delta_number.iter().map(i64::to_string).collect()),
                    ),
                    _ => (
                        AttributeValue::Bs(
                            stored_binary.into_iter().map(crate::Blob::new).collect(),
                        ),
                        AttributeValue::Bs(
                            delta_binary.into_iter().map(crate::Blob::new).collect(),
                        ),
                    ),
                },
            )
    }

    /// Whether one set holds every member of the other.
    fn covers(left: &AttributeValue, right: &AttributeValue) -> bool {
        set_difference(right, left)
            .ok()
            .flatten()
            .and_then(|remaining| empty_set(&remaining))
            == Some(true)
    }

    /// The clauses [`clause_order_cannot_change_the_result`] permutes: three
    /// writes and one read of a value written by another clause, so a snapshot
    /// taken after the first write instead of before it would show.
    fn order_clauses() -> Vec<UpdateClause> {
        let path = |name: &str| vec![Segment::Name(name.to_string())];
        let assigned = |name: &str, value: &str| {
            UpdateClause::Set(vec![(
                path(name),
                SetValue::Operand(Operand::Value(value.to_string())),
            )])
        };
        vec![
            assigned("a", "x"),
            assigned("b", "y"),
            UpdateClause::Set(vec![(
                path("snapshot"),
                SetValue::Operand(Operand::Path(path("a"))),
            )]),
            UpdateClause::Remove(vec![path("d")]),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Two assignments that name each other swap, because both right-hand
        /// sides read the item as it was before the first clause.
        #[test]
        fn a_snapshot_swap_exchanges_both_attributes(
            first in stored_value(),
            second in stored_value(),
        ) {
            let update = parse_update("SET a = b, b = a").unwrap();
            let values = ValueMap::new();
            let context = Ctx {
                names: &NameMap::new(),
                values: &values,
            };
            let mut target = item([("pk", s("a")), ("a", first.clone()), ("b", second.clone())]);
            super::apply(&update, &mut target, &context, &key_schema()).unwrap();
            prop_assert_eq!(target.get("a"), Some(&second));
            prop_assert_eq!(target.get("b"), Some(&first));

            // Just outside: with the second attribute absent there is nothing
            // to swap, so the reference is refused and the item is untouched.
            let mut target = item([("pk", s("a")), ("a", first.clone())]);
            let original = target.clone();
            let error = super::apply(&update, &mut target, &context, &key_schema()).unwrap_err();
            prop_assert_eq!(error.code(), "ValidationException");
            prop_assert_eq!(target, original);
        }

        /// Source order cannot change the result: overlapping paths were
        /// rejected, so the writes are independent and any permutation of the
        /// clauses lands the same item — including the clause that reads `a`
        /// after another clause has written it.
        #[test]
        fn clause_order_cannot_change_the_result(
            first in stored_value(),
            second in stored_value(),
            keys in prop::collection::vec(0_usize..4, 4),
        ) {
            // Sort the positions by their key: the result is a permutation of
            // the four clauses, not a selection that may repeat one of them.
            let mut ordered: Vec<(usize, usize)> = keys.into_iter().zip(0..4).collect();
            ordered.sort_unstable();
            let base = order_clauses();
            let clauses: Vec<UpdateClause> = ordered
                .iter()
                .map(|(_, index)| base[*index].clone())
                .collect();
            let values = crate::values([(":x", s("written")), (":y", second.clone())]);
            let context = Ctx {
                names: &NameMap::new(),
                values: &values,
            };
            let mut target = item([
                ("pk", s("a")),
                ("a", first.clone()),
                ("b", second.clone()),
                ("d", s("gone")),
            ]);
            super::apply(&Update { clauses }, &mut target, &context, &key_schema()).unwrap();
            prop_assert_eq!(target.get("a"), Some(&s("written")));
            prop_assert_eq!(target.get("b"), Some(&second));
            // Out of the snapshot, not out of the write that ran before it.
            prop_assert_eq!(target.get("snapshot"), Some(&first));
            prop_assert!(!target.contains_key("d"));
        }

        /// Addition of two numbers commutes, in the result as well as in the
        /// rendering: both sides go through the same exact decimal sum.
        #[test]
        fn number_addition_is_commutative(
            first in small_number(),
            second in small_number(),
        ) {
            prop_assert_eq!(
                add_values(&n(&first), &n(&second), "count"),
                add_values(&n(&second), &n(&first), "count"),
            );
        }

        /// `SET total = total + :a` followed by `- :a` is the value it started
        /// with: the two steps run on exact decimals, so nothing is rounded
        /// away in between.
        #[test]
        fn subtraction_inverts_addition(
            first in small_number(),
            second in small_number(),
        ) {
            let left = Evaluated {
                value: n(&first),
                origin: Origin::Request,
            };
            let right = Evaluated {
                value: n(&second),
                origin: Origin::Request,
            };
            let sum = arithmetic(&left, &right, ArithOp::Add, "total").unwrap();
            let back = arithmetic(&sum, &right, ArithOp::Sub, "total").unwrap();
            let restored = match &back.value {
                AttributeValue::N(text) => Number::parse(text),
                other => {
                    prop_assert!(false, "arithmetic produced {:?}", other);
                    return Ok(());
                }
            };
            prop_assert_eq!(restored, Number::parse(&first));
        }

        /// A set that gains members and loses them again is the set it was:
        /// the delta is exactly what the `DELETE` takes back. The pair has to
        /// be disjoint for that to hold — over a member both sets carry, `ADD`
        /// changes nothing and `DELETE` then removes it, which
        /// [`delete_after_add_over_an_overlap_loses_the_member`] pins down.
        #[test]
        fn delete_after_add_restores_the_set((stored, delta) in same_type_sets()) {
            let delta = set_difference(&delta, &stored).unwrap().unwrap();
            let united = add_values(&stored, &delta, "members").unwrap();
            let remaining = set_difference(&united, &delta).unwrap().unwrap();
            prop_assert_eq!(remaining, stored);
        }

        /// A union holds the same members whichever side comes first; only the
        /// order they were appended in can differ.
        #[test]
        fn set_union_is_commutative((left, right) in same_type_sets()) {
            let forward = add_values(&left, &right, "members").unwrap();
            let backward = add_values(&right, &left, "members").unwrap();
            prop_assert!(
                covers(&forward, &backward) && covers(&backward, &forward),
                "{:?} and {:?} hold different members",
                forward,
                backward
            );
        }

        /// "Not a set" and "an empty set" stay apart for every value shape: the
        /// second answer removes the attribute it is applied to.
        #[test]
        fn empty_set_classifies_only_sets(
            value in prop_oneof![
                stored_value(),
                Just(AttributeValue::Ss(Vec::new())),
                Just(AttributeValue::Ns(Vec::new())),
                Just(AttributeValue::Bs(Vec::new())),
            ],
        ) {
            match &value {
                AttributeValue::Ss(members) => {
                    prop_assert_eq!(empty_set(&value), Some(members.is_empty()));
                }
                AttributeValue::Ns(members) => {
                    prop_assert_eq!(empty_set(&value), Some(members.is_empty()));
                }
                AttributeValue::Bs(members) => {
                    prop_assert_eq!(empty_set(&value), Some(members.is_empty()));
                }
                _ => prop_assert_eq!(empty_set(&value), None),
            }
        }

        /// `if_not_exists` prefers the stored value and falls back only where
        /// the attribute is absent — the pair of items the function exists for.
        #[test]
        fn if_not_exists_prefers_the_stored_value(
            existing in stored_value(),
            fallback in stored_value(),
        ) {
            let update = parse_update("SET v = if_not_exists(v, :fallback)").unwrap();
            let values = crate::values([(":fallback", fallback.clone())]);
            let context = Ctx {
                names: &NameMap::new(),
                values: &values,
            };

            let mut present = item([("pk", s("a")), ("v", existing.clone())]);
            super::apply(&update, &mut present, &context, &key_schema()).unwrap();
            prop_assert_eq!(present.get("v"), Some(&existing));

            let mut absent = item([("pk", s("a"))]);
            super::apply(&update, &mut absent, &context, &key_schema()).unwrap();
            prop_assert_eq!(absent.get("v"), Some(&fallback));
        }

        /// `rows[i]` is just inside the list up to its length and past the end
        /// from there; the refusal must leave the item exactly as it was, and
        /// the clause that wrote before it must not stick either.
        #[test]
        fn an_index_at_the_end_is_refused_and_leaves_the_item(
            length in 0_usize..4,
            index in 0_usize..5,
        ) {
            let mut target = item([
                ("pk", s("a")),
                ("rows", AttributeValue::L(vec![s("x"); length])),
            ]);
            let original = target.clone();
            let values = crate::values([(":v", s("new"))]);
            let expression = format!("SET marker = :v, rows[{index}] = :v");
            let outcome = apply(&expression, &mut target, &values);
            if index < length {
                prop_assert!(outcome.is_ok(), "{outcome:?}");
                prop_assert_eq!(target.get("marker"), Some(&s("new")));
                prop_assert_eq!(
                    get_path(
                        &target,
                        &[Segment::Name("rows".into()), Segment::Index(index)],
                    ),
                    Some(&s("new")),
                );
            } else {
                prop_assert_eq!(outcome.unwrap_err().code(), "ValidationException");
                prop_assert_eq!(target, original);
            }
        }
    }

    /// The overlap the property above cannot assert on: `ADD` of a member the
    /// set already holds leaves the set unchanged, and the following `DELETE`
    /// removes that member, so the attribute loses it. Just outside, a member
    /// only the stored set carries survives.
    #[test]
    fn delete_after_add_over_an_overlap_loses_the_member() {
        let shared = AttributeValue::Ns(vec!["831".to_string()]);
        let united = add_values(&shared, &shared, "members").unwrap();
        assert_eq!(united, shared);
        let remaining = set_difference(&united, &shared).unwrap().unwrap();
        assert_eq!(empty_set(&remaining), Some(true));

        let stored = AttributeValue::Ns(vec!["831".to_string()]);
        let delta = AttributeValue::Ns(vec!["832".to_string()]);
        let united = add_values(&stored, &delta, "members").unwrap();
        let remaining = set_difference(&united, &delta).unwrap().unwrap();
        assert_eq!(remaining, stored);
    }
}
