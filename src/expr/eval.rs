//! Evaluation of parsed conditions against an item.
//!
//! A missing attribute never satisfies a comparison: `missing = :v`,
//! `missing < :v`, `contains(missing, :v)` and `size(missing) > :n` are all
//! false. `attribute_not_exists` is the way to test for absence, which is why
//! the write path passes `None` for a missing item.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

use super::parse::{CmpOp, Condition, Operand, Path, Segment};
use super::path::get_path;
use crate::value::{type_name, value_cmp, value_eq};
use crate::{Error, Item, Result};

/// Expression-attribute names (`#alias` → attribute name).
pub(crate) type NameMap = HashMap<String, String>;

/// Expression-attribute values (`:placeholder` → value).
pub(crate) type ValueMap = HashMap<String, AttributeValue>;

/// Resolved aliases and values for one evaluation.
pub(crate) struct Ctx<'a> {
    pub(crate) names: &'a NameMap,
    pub(crate) values: &'a ValueMap,
}

impl Ctx<'_> {
    /// Replace `#alias` segments with the attribute names they stand for.
    pub(crate) fn resolve(&self, path: &Path) -> Result<Path> {
        let mut resolved = Path::with_capacity(path.len());
        for segment in path {
            match segment {
                Segment::Name(name) => match name.strip_prefix('#') {
                    Some(alias) => {
                        let replacement = self.names.get(&format!("#{alias}")).ok_or_else(|| {
                            Error::validation(format!(
                                "Invalid expression: An expression attribute name used in the document path is not defined; attribute name: #{alias}"
                            ))
                        })?;
                        resolved.push(Segment::Name(replacement.clone()));
                    }
                    None => resolved.push(Segment::Name(name.clone())),
                },
                Segment::Index(index) => resolved.push(Segment::Index(*index)),
            }
        }
        Ok(resolved)
    }

    /// Read a path from an item, resolving aliases.
    pub(crate) fn path_value(
        &self,
        path: &Path,
        item: Option<&Item>,
    ) -> Result<Option<AttributeValue>> {
        let resolved = self.resolve(path)?;
        Ok(item.and_then(|item| get_path(item, &resolved)).cloned())
    }

    /// Read an operand: a path, a placeholder or `size(path)`.
    pub(crate) fn operand_value(
        &self,
        operand: &Operand,
        item: Option<&Item>,
    ) -> Result<Option<AttributeValue>> {
        match operand {
            Operand::Path(path) => self.path_value(path, item),
            Operand::Value(placeholder) => {
                self.values
                    .get(&format!(":{placeholder}"))
                    .cloned()
                    .map(Some)
                    .ok_or_else(|| {
                        Error::validation(format!(
                            "Invalid expression: An expression attribute value used in expression is not defined; attribute value: :{placeholder}"
                        ))
                    })
            }
            Operand::Size(path) => {
                let value = self.path_value(path, item)?;
                Ok(value.map(|value| AttributeValue::N(size_of(&value).to_string())))
            }
        }
    }
}

/// The `size()` of a value: bytes for strings and binary, element count for
/// sets, lists and maps. `BOOL` and `NULL` have size 0.
fn size_of(value: &AttributeValue) -> u64 {
    match value {
        AttributeValue::S(text) => u64::try_from(text.len()).unwrap_or(u64::MAX),
        AttributeValue::B(blob) => u64::try_from(blob.as_ref().len()).unwrap_or(u64::MAX),
        AttributeValue::Ss(members) | AttributeValue::Ns(members) => {
            u64::try_from(members.len()).unwrap_or(u64::MAX)
        }
        AttributeValue::Bs(members) => u64::try_from(members.len()).unwrap_or(u64::MAX),
        AttributeValue::L(elements) => u64::try_from(elements.len()).unwrap_or(u64::MAX),
        AttributeValue::M(entries) => u64::try_from(entries.len()).unwrap_or(u64::MAX),
        _ => 0,
    }
}

/// Evaluate a condition against an item.
pub(crate) fn evaluate(condition: &Condition, item: Option<&Item>, ctx: &Ctx<'_>) -> Result<bool> {
    match condition {
        Condition::AttributeExists(path) => Ok(ctx.path_value(path, item)?.is_some()),
        Condition::AttributeNotExists(path) => Ok(ctx.path_value(path, item)?.is_none()),
        Condition::AttributeType(path, expected) => {
            let expected = ctx.operand_value(expected, item)?.ok_or_else(|| {
                Error::validation("Invalid expression: attribute_type requires a type placeholder")
            })?;
            let AttributeValue::S(expected) = expected else {
                return Err(Error::validation(
                    "Invalid ConditionExpression: The attribute type in attribute_type must be a string",
                ));
            };
            Ok(ctx
                .path_value(path, item)?
                .is_some_and(|value| type_name(&value) == expected))
        }
        Condition::BeginsWith(path, prefix) => match (
            ctx.path_value(path, item)?,
            ctx.operand_value(prefix, item)?,
        ) {
            (Some(value), Some(prefix)) => Ok(begins_with(&value, &prefix)),
            _ => Ok(false),
        },
        Condition::Contains(path, needle) => match (
            ctx.path_value(path, item)?,
            ctx.operand_value(needle, item)?,
        ) {
            (Some(value), Some(needle)) => Ok(contains(&value, &needle)),
            _ => Ok(false),
        },
        Condition::Compare(left, operator, right) => {
            match (
                ctx.operand_value(left, item)?,
                ctx.operand_value(right, item)?,
            ) {
                (Some(left), Some(right)) => compare(&left, *operator, &right),
                _ => Ok(false),
            }
        }
        Condition::Between(value, low, high) => {
            let (Some(value), Some(low), Some(high)) = (
                ctx.operand_value(value, item)?,
                ctx.operand_value(low, item)?,
                ctx.operand_value(high, item)?,
            ) else {
                return Ok(false);
            };
            Ok(compare(&value, CmpOp::Ge, &low)? && compare(&value, CmpOp::Le, &high)?)
        }
        Condition::In(value, entries) => {
            let Some(value) = ctx.operand_value(value, item)? else {
                return Ok(false);
            };
            for entry in entries {
                if let Some(candidate) = ctx.operand_value(entry, item)? {
                    if value_eq(&value, &candidate) {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        }
        Condition::And(left, right) => {
            Ok(evaluate(left, item, ctx)? && evaluate(right, item, ctx)?)
        }
        Condition::Or(left, right) => Ok(evaluate(left, item, ctx)? || evaluate(right, item, ctx)?),
        Condition::Not(inner) => Ok(!evaluate(inner, item, ctx)?),
    }
}

/// Apply one light and one heavy operand to a comparison operator.
fn compare(left: &AttributeValue, operator: CmpOp, right: &AttributeValue) -> Result<bool> {
    if matches!(operator, CmpOp::Eq | CmpOp::Ne) {
        let equal = value_eq(left, right);
        return Ok(if operator == CmpOp::Eq { equal } else { !equal });
    }
    let ordering = value_cmp(left, right).ok_or_else(|| {
        Error::validation(format!(
            "Invalid ConditionExpression: The comparison operator is not supported for the types {} and {}",
            type_name(left),
            type_name(right)
        ))
    })?;
    Ok(match operator {
        CmpOp::Lt => ordering.is_lt(),
        CmpOp::Le => ordering.is_le(),
        CmpOp::Gt => ordering.is_gt(),
        CmpOp::Ge => ordering.is_ge(),
        // Equality returned above; keeping the arm total avoids a panic path.
        CmpOp::Eq | CmpOp::Ne => ordering.is_eq(),
    })
}

/// `begins_with` semantics for strings and binary.
fn begins_with(value: &AttributeValue, prefix: &AttributeValue) -> bool {
    match (value, prefix) {
        (AttributeValue::S(value), AttributeValue::S(prefix)) => value.starts_with(prefix),
        (AttributeValue::B(value), AttributeValue::B(prefix)) => {
            value.as_ref().starts_with(prefix.as_ref())
        }
        _ => false,
    }
}

/// `contains` semantics: substring for strings, membership for sets and lists.
fn contains(value: &AttributeValue, needle: &AttributeValue) -> bool {
    match (value, needle) {
        (AttributeValue::S(value), AttributeValue::S(needle)) => value.contains(needle.as_str()),
        (AttributeValue::Ss(members), AttributeValue::S(needle)) => members.contains(needle),
        (AttributeValue::Ns(members), AttributeValue::N(needle)) => members.iter().any(|member| {
            matches!(
                (crate::number::Number::parse(member), crate::number::Number::parse(needle)),
                (Some(left), Some(right)) if left == right
            )
        }),
        (AttributeValue::Bs(members), AttributeValue::B(needle)) => members
            .iter()
            .any(|member| member.as_ref() == needle.as_ref()),
        (AttributeValue::L(elements), needle) => {
            elements.iter().any(|element| value_eq(element, needle))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{n, s};

    fn item() -> Item {
        let mut item = Item::new();
        item.insert("pk".into(), s("a"));
        item.insert("count".into(), n("3"));
        item.insert(
            "tags".into(),
            AttributeValue::Ss(vec!["red".into(), "blue".into()]),
        );
        item.insert(
            "blob".into(),
            AttributeValue::B(crate::Blob::new(vec![1_u8, 2])),
        );
        item.insert(
            "blobs".into(),
            AttributeValue::Bs(vec![crate::Blob::new(vec![1_u8])]),
        );
        item.insert("flag".into(), AttributeValue::Bool(true));
        item.insert(
            "profile".into(),
            AttributeValue::M(HashMap::from([("city".to_string(), s("Berlin"))])),
        );
        item
    }

    fn evaluate_str(expression: &str, values: &[(&str, AttributeValue)]) -> Result<bool> {
        let condition = super::super::parse::parse_condition(expression)?;
        let values: ValueMap = values
            .iter()
            .map(|(key, value)| (format!(":{key}"), value.clone()))
            .collect();
        let item = item();
        let ctx = Ctx {
            names: &NameMap::new(),
            values: &values,
        };
        evaluate(&condition, Some(&item), &ctx)
    }

    #[test]
    fn comparisons_respect_types_and_presence() {
        assert!(evaluate_str("count > :two", &[("two", n("2"))]).unwrap());
        assert!(!evaluate_str("count > :nine", &[("nine", n("9"))]).unwrap());
        assert!(!evaluate_str("missing > :two", &[("two", n("2"))]).unwrap());
        assert!(evaluate_str("missing <> :two", &[("two", n("2"))]).is_ok());
        assert!(!evaluate_str("count = :nope", &[("nope", s("3"))]).unwrap());
    }

    #[test]
    fn set_membership_and_substrings() {
        assert!(evaluate_str("contains(tags, :red)", &[("red", s("red"))]).unwrap());
        assert!(!evaluate_str("contains(tags, :green)", &[("green", s("green"))]).unwrap());
        assert!(evaluate_str("contains(profile.city, :ber)", &[("ber", s("Ber"))]).unwrap());
        assert!(evaluate_str("begins_with(pk, :a)", &[("a", s("a"))]).unwrap());
        assert!(!evaluate_str("begins_with(pk, :b)", &[("b", s("b"))]).unwrap());
    }

    #[test]
    fn size_and_attribute_type() {
        assert!(evaluate_str("size(tags) = :two", &[("two", n("2"))]).unwrap());
        assert!(evaluate_str("attribute_type(tags, :t)", &[("t", s("SS"))]).unwrap());
        assert!(evaluate_str("attribute_type(count, :t)", &[("t", s("N"))]).unwrap());
        assert!(!evaluate_str("attribute_type(count, :t)", &[("t", s("S"))]).unwrap());
    }

    #[test]
    fn size_and_type_functions_cover_binary_and_missing_operands() {
        assert!(evaluate_str("size(blob) = :two", &[("two", n("2"))]).unwrap());
        assert!(evaluate_str("size(blobs) = :one", &[("one", n("1"))]).unwrap());
        assert!(evaluate_str("size(flag) = :zero", &[("zero", n("0"))]).unwrap());
        assert!(evaluate_str("attribute_type(name, absent)", &[]).is_err());
        assert!(!evaluate_str("begins_with(name, absent)", &[]).unwrap());
        assert!(!evaluate_str("count BETWEEN :a AND absent", &[("a", n("1"))]).unwrap());
        assert!(
            !evaluate_str("absent BETWEEN :a AND :b", &[("a", n("1")), ("b", n("2"))]).unwrap()
        );
        assert!(!evaluate_str("absent IN (:a)", &[("a", n("1"))]).unwrap());
        assert!(evaluate_str(
            "begins_with(blob, :prefix)",
            &[("prefix", AttributeValue::B(crate::Blob::new(vec![1])))]
        )
        .unwrap());
        assert!(!evaluate_str(
            "begins_with(blob, :prefix)",
            &[("prefix", AttributeValue::B(crate::Blob::new(vec![9])))]
        )
        .unwrap());
        assert!(!evaluate_str("begins_with(count, :prefix)", &[("prefix", n("1"))]).unwrap());
        assert!(!evaluate_str("contains(flag, :v)", &[("v", AttributeValue::Bool(true))]).unwrap());
    }

    #[test]
    fn boolean_connectives_short_circuit() {
        assert!(evaluate_str(
            "count = :three AND (pk = :a OR pk = :b)",
            &[("three", n("3")), ("a", s("a")), ("b", s("b"))]
        )
        .unwrap());
        assert!(!evaluate_str("NOT count = :three", &[("three", n("3"))]).unwrap());
    }

    #[test]
    fn between_and_in() {
        assert!(evaluate_str(
            "count BETWEEN :one AND :five",
            &[("one", n("1")), ("five", n("5"))]
        )
        .unwrap());
        assert!(!evaluate_str(
            "count BETWEEN :four AND :five",
            &[("four", n("4")), ("five", n("5"))]
        )
        .unwrap());
        assert!(evaluate_str(
            "count IN (:one, :three)",
            &[("one", n("1")), ("three", n("3"))]
        )
        .unwrap());
    }
}
