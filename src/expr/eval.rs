//! Evaluation of parsed conditions against an item.
//!
//! A missing attribute never satisfies a comparison: `missing = :v`,
//! `missing < :v`, `contains(missing, :v)` and `size(missing) > :n` are all
//! false. `attribute_not_exists` is the way to test for absence, which is why
//! the write path passes `None` for a missing item.
//!
//! An operand of the wrong type is not false: `begins_with(count, :s)` and
//! `contains(count, :s)` are rejected with the service's `ValidationException`
//! ("Incorrect operand type for operator or function"), so a test asserting a
//! conditional write is refused fails for the same reason the service would
//! refuse it.
//!
//! Reads borrow: a condition only compares, so the value an operand names stays
//! in the item (or in the value map) and no attribute value is copied. Paths
//! without an `#alias` are used as they are, without being rebuilt either.

use std::borrow::Cow;
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
    ///
    /// A path that names no alias is handed back borrowed, so the common case
    /// allocates nothing; only a path with an alias is rebuilt.
    pub(crate) fn resolve<'p>(&self, path: &'p Path) -> Result<Cow<'p, Path>> {
        let aliased = path
            .iter()
            .any(|segment| matches!(segment, Segment::Name(name) if name.starts_with('#')));
        if !aliased {
            return Ok(Cow::Borrowed(path));
        }
        let mut resolved = Path::with_capacity(path.len());
        for segment in path {
            match segment {
                Segment::Name(name) => match name.strip_prefix('#') {
                    Some(alias) => {
                        // The map is keyed by the whole `#alias` segment.
                        let replacement = self.names.get(name).ok_or_else(|| {
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
        Ok(Cow::Owned(resolved))
    }

    /// Read a path from an item, resolving aliases.
    ///
    /// The value is borrowed from the item, not copied: a membership, prefix or
    /// comparison test only reads it, and an attribute may hold a large payload.
    pub(crate) fn path_value<'i>(
        &'i self,
        path: &Path,
        item: Option<&'i Item>,
    ) -> Result<Option<&'i AttributeValue>> {
        let resolved = self.resolve(path)?;
        Ok(item.and_then(|item| get_path(item, &resolved)))
    }

    /// Read an operand: a path, a placeholder or `size(path)`.
    ///
    /// A path and a placeholder borrow the stored value; `size()` is the only
    /// operand that builds a new one, because it reports a derived number.
    pub(crate) fn operand_value<'i>(
        &'i self,
        operand: &Operand,
        item: Option<&'i Item>,
    ) -> Result<Option<Cow<'i, AttributeValue>>> {
        match operand {
            Operand::Path(path) => Ok(self.path_value(path, item)?.map(Cow::Borrowed)),
            Operand::Value(placeholder) => {
                match self.values.get(&format!(":{placeholder}")) {
                    Some(value) => Ok(Some(Cow::Borrowed(value))),
                    None => Err(Error::validation(format!(
                        "Invalid expression: An expression attribute value used in expression is not defined; attribute value: :{placeholder}"
                    ))),
                }
            }
            Operand::Size(path) => {
                let value = self.path_value(path, item)?;
                Ok(value.map(|value| {
                    Cow::Owned(AttributeValue::N(size_of(value).to_string()))
                }))
            }
        }
    }
}

/// The `size()` of a value: bytes for strings and binary, element count for
/// sets, lists and maps. Scalars without a length — `N`, `BOOL` and `NULL` —
/// have size 0, so the function is total over every [`AttributeValue`] shape.
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
            let AttributeValue::S(expected) = expected.as_ref() else {
                return Err(Error::validation(
                    "Invalid ConditionExpression: The attribute type in attribute_type must be a string",
                ));
            };
            Ok(ctx
                .path_value(path, item)?
                .is_some_and(|value| type_name(value) == expected.as_str()))
        }
        Condition::BeginsWith(path, prefix) => match (
            ctx.path_value(path, item)?,
            ctx.operand_value(prefix, item)?,
        ) {
            (Some(value), Some(prefix)) => begins_with(value, &prefix),
            // A missing attribute satisfies no condition. A missing placeholder
            // is an undefined-value error and was already raised by
            // `operand_value`.
            _ => Ok(false),
        },
        Condition::Contains(path, needle) => match (
            ctx.path_value(path, item)?,
            ctx.operand_value(needle, item)?,
        ) {
            (Some(value), Some(needle)) => contains(value, &needle),
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
        Condition::And(terms) => {
            for term in terms {
                if !evaluate(term, item, ctx)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Condition::Or(terms) => {
            for term in terms {
                if evaluate(term, item, ctx)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
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

/// The service's rejection of an operand whose type the function cannot use.
///
/// The message is the one DynamoDB returns for e.g. `begins_with(count, :s)`,
/// so a test that matches on the text keeps working when it is pointed at the
/// real service.
fn operand_type_error(function: &str, offender: &AttributeValue) -> Error {
    Error::validation(format!(
        "Invalid ConditionExpression: Incorrect operand type for operator or function; \
         operator or function: {function}, operand type: {}",
        type_name(offender)
    ))
}

/// `begins_with` semantics for strings and binary.
///
/// A prefix exists only for `S` and `B`; every other pair — a number, a set, a
/// map, a string prefix on a binary value — is the service's
/// `ValidationException`, not a condition that happens to be false.
fn begins_with(value: &AttributeValue, prefix: &AttributeValue) -> Result<bool> {
    match (value, prefix) {
        (AttributeValue::S(value), AttributeValue::S(prefix)) => Ok(value.starts_with(prefix)),
        (AttributeValue::B(value), AttributeValue::B(prefix)) => {
            Ok(value.as_ref().starts_with(prefix.as_ref()))
        }
        // The prefix is the offending operand when the value could take one.
        (AttributeValue::S(_) | AttributeValue::B(_), prefix) => {
            Err(operand_type_error("begins_with", prefix))
        }
        (value, _) => Err(operand_type_error("begins_with", value)),
    }
}

/// `contains` semantics: substring for strings, membership for sets and lists.
///
/// A set holds members of one type, so its needle has to have that type;
/// `contains` on a number, a map or a NULL is a type error rather than false.
fn contains(value: &AttributeValue, needle: &AttributeValue) -> Result<bool> {
    match (value, needle) {
        (AttributeValue::S(value), AttributeValue::S(needle)) => {
            Ok(value.contains(needle.as_str()))
        }
        (AttributeValue::Ss(members), AttributeValue::S(needle)) => Ok(members.contains(needle)),
        (AttributeValue::Ns(members), AttributeValue::N(needle)) => {
            Ok(members.iter().any(|member| {
                matches!(
                    (crate::number::Number::parse(member), crate::number::Number::parse(needle)),
                    (Some(left), Some(right)) if left == right
                )
            }))
        }
        (AttributeValue::Bs(members), AttributeValue::B(needle)) => Ok(members
            .iter()
            .any(|member| member.as_ref() == needle.as_ref())),
        (AttributeValue::L(elements), needle) => {
            Ok(elements.iter().any(|element| value_eq(element, needle)))
        }
        // A string, set or binary value the function knows, with a needle of
        // the wrong type: the needle is the operand to name.
        (
            AttributeValue::S(_)
            | AttributeValue::Ss(_)
            | AttributeValue::Ns(_)
            | AttributeValue::Bs(_),
            needle,
        ) => Err(operand_type_error("contains", needle)),
        (value, _) => Err(operand_type_error("contains", value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{n, s};
    use proptest::prelude::*;

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
        // A missing attribute satisfies no comparison at all — not even the
        // negation of equality, which reads as "true" if the rule is flipped.
        assert!(!evaluate_str("missing = :two", &[("two", n("2"))]).unwrap());
        assert!(!evaluate_str("missing <> :two", &[("two", n("2"))]).unwrap());
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
        assert!(evaluate_str("begins_with(count, :prefix)", &[("prefix", n("1"))]).is_err());
        assert!(evaluate_str("contains(flag, :v)", &[("v", AttributeValue::Bool(true))]).is_err());
    }

    /// The service rejects a wrongly typed operand of `begins_with`/`contains`
    /// with a `ValidationException`; answering "condition not met" would let a
    /// test that asserts a conditional write is refused pass for the wrong
    /// reason.
    #[test]
    fn a_type_mismatched_operand_is_a_validation_error_not_false() {
        let message = |expression: &str, values: &[(&str, AttributeValue)]| {
            let error = evaluate_str(expression, values).unwrap_err();
            assert_eq!(error.code(), "ValidationException", "{expression}");
            assert!(
                error
                    .to_string()
                    .contains("Incorrect operand type for operator or function"),
                "{expression}: {error}"
            );
            error.to_string()
        };

        // just inside: a string with a string prefix, a binary with a binary one
        assert!(evaluate_str("begins_with(pk, :p)", &[("p", s("a"))]).unwrap());
        assert!(evaluate_str(
            "begins_with(blob, :p)",
            &[("p", AttributeValue::B(crate::Blob::new(vec![1])))]
        )
        .unwrap());
        // just outside: the value, then the prefix, of the wrong type
        assert!(message("begins_with(count, :p)", &[("p", s("1"))])
            .contains("operator or function: begins_with, operand type: N"));
        assert!(message("begins_with(pk, :p)", &[("p", n("1"))])
            .contains("operator or function: begins_with, operand type: N"));
        assert!(message(
            "begins_with(blob, :p)",
            &[("p", AttributeValue::S("a".into()))]
        )
        .contains("operator or function: begins_with, operand type: S"));
        assert!(message("begins_with(tags, :p)", &[("p", s("red"))])
            .contains("operator or function: begins_with, operand type: SS"));
        assert!(message("begins_with(profile, :p)", &[("p", s("Ber"))]).contains("operand type: M"));

        // `contains` keeps accepting every shape it has a meaning for, and
        // rejects a needle whose type the shape cannot hold a member of.
        assert!(evaluate_str("contains(pk, :v)", &[("v", s("a"))]).unwrap());
        assert!(evaluate_str("contains(tags, :v)", &[("v", s("red"))]).unwrap());
        assert!(evaluate_str(
            "contains(blobs, :v)",
            &[("v", AttributeValue::B(crate::Blob::new(vec![1])))]
        )
        .unwrap());
        assert!(message("contains(count, :v)", &[("v", s("3"))])
            .contains("operator or function: contains, operand type: N"));
        assert!(message("contains(tags, :v)", &[("v", n("3"))])
            .contains("operator or function: contains, operand type: N"));
        assert!(
            message("contains(flag, :v)", &[("v", AttributeValue::Bool(true))])
                .contains("operator or function: contains, operand type: BOOL")
        );

        // A missing attribute has no operand to compare, so the wrong-typed
        // needle stays false instead of failing.
        assert!(!evaluate_str("begins_with(absent, :v)", &[("v", n("1"))]).unwrap());
        assert!(!evaluate_str("contains(absent, :v)", &[("v", n("1"))]).unwrap());
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

    #[test]
    fn path_resolution_borrows_until_an_alias_forces_a_copy() {
        let plain: Path = vec![
            Segment::Name("profile".into()),
            Segment::Name("city".into()),
        ];
        let ctx = Ctx {
            names: &NameMap::new(),
            values: &ValueMap::new(),
        };
        // No alias: the parsed path is used as it is, without an allocation.
        let resolved = ctx.resolve(&plain).unwrap();
        assert!(matches!(&resolved, Cow::Borrowed(_)));
        assert!(std::ptr::eq(resolved.as_ref(), &raw const plain));

        // One alias anywhere in the path switches to a rewritten, owned path.
        let names = NameMap::from([("#p".to_string(), "profile".to_string())]);
        let ctx = Ctx {
            names: &names,
            values: &ValueMap::new(),
        };
        let aliased: Path = vec![Segment::Name("#p".into()), Segment::Name("city".into())];
        let expected: Path = vec![
            Segment::Name("profile".into()),
            Segment::Name("city".into()),
        ];
        let resolved = ctx.resolve(&aliased).unwrap();
        assert!(matches!(&resolved, Cow::Owned(_)));
        assert_eq!(resolved.as_ref(), &expected);

        // The last segment counts as much as the first.
        let nested: Path = vec![Segment::Name("a".into()), Segment::Name("#p".into())];
        let expected: Path = vec![Segment::Name("a".into()), Segment::Name("profile".into())];
        assert_eq!(ctx.resolve(&nested).unwrap().as_ref(), &expected);

        // An undefined alias stays an error instead of being read literally.
        let undefined: Path = vec![Segment::Name("#gone".into())];
        assert!(ctx.resolve(&undefined).is_err());
    }

    #[test]
    fn operands_borrow_the_stored_value_instead_of_copying_it() {
        // 400 KB is the attribute size limit: a membership or prefix test on
        // such a value must see the item's own allocation, not a copy of it.
        let payload = "x".repeat(400 * 1024);
        let mut item = Item::new();
        item.insert("payload".into(), s(&payload));
        let ctx = Ctx {
            names: &NameMap::new(),
            values: &ValueMap::new(),
        };
        let stored = item.get("payload").unwrap();

        let path: Path = vec![Segment::Name("payload".into())];
        let read = ctx.path_value(&path, Some(&item)).unwrap().unwrap();
        assert!(std::ptr::eq(read, stored));

        let operand = Operand::Path(path.clone());
        let read = ctx.operand_value(&operand, Some(&item)).unwrap().unwrap();
        assert!(matches!(&read, Cow::Borrowed(_)));
        assert!(std::ptr::eq(read.as_ref(), stored));

        // `size()` is the one operand that has to build a number.
        let operand = Operand::Size(path);
        let read = ctx.operand_value(&operand, Some(&item)).unwrap().unwrap();
        assert!(matches!(&read, Cow::Owned(_)));
        assert_eq!(read.as_ref(), &AttributeValue::N((400 * 1024).to_string()));

        // A path that names nothing reads as absent, still without a copy.
        let absent: Path = vec![Segment::Name("missing".into())];
        assert!(ctx.path_value(&absent, Some(&item)).unwrap().is_none());
        assert!(ctx
            .operand_value(&Operand::Path(absent), None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn empty_needles_scalar_sizes_and_out_of_range_indices() {
        // An empty needle is a substring of every string and a prefix of every
        // string, but a set holds members, not substrings — and a string set
        // cannot begin with a string at all.
        assert!(evaluate_str("contains(pk, :empty)", &[("empty", s(""))]).unwrap());
        assert!(evaluate_str("begins_with(pk, :empty)", &[("empty", s(""))]).unwrap());
        assert!(!evaluate_str("contains(tags, :empty)", &[("empty", s(""))]).unwrap());
        assert!(evaluate_str("begins_with(tags, :empty)", &[("empty", s(""))]).is_err());

        // A number has no length, so `size()` reports 0 for it instead of
        // failing or inventing a count.
        assert!(evaluate_str("size(count) = :zero", &[("zero", n("0"))]).unwrap());
        assert!(!evaluate_str("size(count) = :one", &[("one", n("1"))]).unwrap());

        let mut item = item();
        item.insert("nums".into(), AttributeValue::Ns(vec!["1".into()]));
        item.insert("list".into(), AttributeValue::L(vec![s("a"), n("7")]));

        // Set membership is numeric for `NS`: the member `1` is also named by
        // `1.0`, and by neither `1.1` nor `10`.
        let condition = super::super::parse::parse_condition("contains(nums, :one)").unwrap();
        for (needle, expected) in [("1.0", true), ("1.1", false), ("10", false)] {
            let values = ValueMap::from([(":one".to_string(), n(needle))]);
            let ctx = Ctx {
                names: &NameMap::new(),
                values: &values,
            };
            assert_eq!(
                evaluate(&condition, Some(&item), &ctx).unwrap(),
                expected,
                "needle {needle}"
            );
        }

        // A list index inside the list reads the element; one past the end
        // reads as absent, so the comparison is false rather than an error.
        let inside = super::super::parse::parse_condition("list[1] = :seven").unwrap();
        let outside = super::super::parse::parse_condition("list[2] = :seven").unwrap();
        let values = ValueMap::from([(":seven".to_string(), n("7"))]);
        let ctx = Ctx {
            names: &NameMap::new(),
            values: &values,
        };
        assert!(evaluate(&inside, Some(&item), &ctx).unwrap());
        assert!(!evaluate(&outside, Some(&item), &ctx).unwrap());
    }

    /// A value shape an item may hold: leaves first, then containers built from
    /// them, so nested lists and maps are evaluated too.
    fn attribute() -> impl Strategy<Value = AttributeValue> {
        let leaf = prop_oneof![
            "[a-zA-Z0-9 _#üöä-]{0,12}".prop_map(AttributeValue::S),
            (-1_000_i64..1_000).prop_map(|value| AttributeValue::N(value.to_string())),
            prop::collection::vec(any::<u8>(), 0..8)
                .prop_map(|bytes| AttributeValue::B(crate::Blob::new(bytes))),
            any::<bool>().prop_map(AttributeValue::Bool),
            Just(AttributeValue::Null(true)),
        ];
        // A set is generated with at least one member: DynamoDB rejects an empty
        // set on write, so the evaluator never has to look at one.
        leaf.prop_recursive(2, 4, 4, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..3).prop_map(AttributeValue::L),
                prop::collection::hash_map("[a-z]{1,5}", inner.clone(), 0..3)
                    .prop_map(AttributeValue::M),
                prop::collection::vec("[a-z]{1,4}", 1..3).prop_map(AttributeValue::Ss),
                prop::collection::vec(1_i64..100, 1..3).prop_map(|members| {
                    AttributeValue::Ns(members.iter().map(i64::to_string).collect())
                }),
                prop::collection::vec(prop::collection::vec(any::<u8>(), 0..4), 1..3).prop_map(
                    |members| {
                        AttributeValue::Bs(members.into_iter().map(crate::Blob::new).collect())
                    }
                ),
            ]
        })
    }

    /// A condition tree over the two placeholders and the fixture's attributes,
    /// including a path that names nothing and an index past the list's end.
    fn condition() -> impl Strategy<Value = Condition> {
        let path = prop_oneof![
            Just(vec![Segment::Name("pk".into())]),
            Just(vec![Segment::Name("count".into())]),
            Just(vec![Segment::Name("tags".into())]),
            Just(vec![Segment::Name("blob".into())]),
            Just(vec![
                Segment::Name("profile".into()),
                Segment::Name("city".into()),
            ]),
            Just(vec![Segment::Name("list".into()), Segment::Index(0)]),
            Just(vec![Segment::Name("list".into()), Segment::Index(7)]),
            Just(vec![Segment::Name("absent".into())]),
        ];
        let operand = prop_oneof![
            path.clone().prop_map(Operand::Path),
            Just(Operand::Value("a".to_string())),
            Just(Operand::Value("b".to_string())),
            path.clone().prop_map(Operand::Size),
        ];
        let operator = prop::sample::select(vec![
            CmpOp::Eq,
            CmpOp::Ne,
            CmpOp::Lt,
            CmpOp::Le,
            CmpOp::Gt,
            CmpOp::Ge,
        ]);
        let leaf = prop_oneof![
            path.clone().prop_map(Condition::AttributeExists),
            path.clone().prop_map(Condition::AttributeNotExists),
            (path.clone(), operand.clone())
                .prop_map(|(path, needle)| Condition::Contains(path, needle)),
            (path.clone(), operand.clone())
                .prop_map(|(path, prefix)| Condition::BeginsWith(path, prefix)),
            (path, operand.clone())
                .prop_map(|(path, expected)| Condition::AttributeType(path, expected)),
            (operand.clone(), operator, operand.clone())
                .prop_map(|(left, operator, right)| Condition::Compare(left, operator, right)),
            (operand.clone(), operand.clone()).prop_map(|(value, bound)| Condition::Between(
                value,
                bound.clone(),
                bound
            )),
            (
                operand.clone(),
                prop::collection::vec(operand.clone(), 1..4)
            )
                .prop_map(|(value, entries)| Condition::In(value, entries)),
        ];
        leaf.prop_recursive(2, 8, 4, |inner| {
            prop_oneof![
                inner
                    .clone()
                    .prop_map(|inner| Condition::Not(Box::new(inner))),
                prop::collection::vec(inner.clone(), 1..4).prop_map(Condition::And),
                prop::collection::vec(inner.clone(), 1..4).prop_map(Condition::Or),
            ]
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(96))]

        /// `NOT` is the exact negation of the condition it wraps, error
        /// included: a condition that cannot be evaluated does not become
        /// `true` by being negated.
        #[test]
        fn not_inverts_every_condition(
            condition in condition(),
            first in attribute(),
            second in attribute(),
        ) {
            let values = ValueMap::from([
                (":a".to_string(), first),
                (":b".to_string(), second),
            ]);
            let ctx = Ctx {
                names: &NameMap::new(),
                values: &values,
            };
            let mut item = item();
            item.insert(
                "list".into(),
                AttributeValue::L(vec![s("a"), n("7")]),
            );
            let negated = Condition::Not(Box::new(condition.clone()));
            match (
                evaluate(&condition, Some(&item), &ctx),
                evaluate(&negated, Some(&item), &ctx),
            ) {
                (Ok(plain), Ok(flipped)) => prop_assert_eq!(flipped, !plain),
                (Err(_), Err(_)) => {}
                (plain, flipped) => {
                    prop_assert!(false, "{:?} negated to {:?}", plain, flipped);
                }
            }
        }

        /// One ordering, seen from both sides: `left < right` holds exactly when
        /// `right > left` does, and a pair that is not comparable at all is not
        /// comparable in either direction.
        #[test]
        fn comparison_is_antisymmetric(left in attribute(), right in attribute()) {
            match (
                compare(&left, CmpOp::Lt, &right),
                compare(&right, CmpOp::Gt, &left),
            ) {
                (Ok(forward), Ok(backward)) => prop_assert_eq!(forward, backward),
                (Err(_), Err(_)) => {}
                (forward, backward) => {
                    prop_assert!(false, "one direction only: {:?} vs {:?}", forward, backward);
                }
            }
        }

        /// Equality and inequality are complements, and an equal pair is never
        /// strictly less or greater than itself.
        #[test]
        fn equality_excludes_strict_order(left in attribute(), right in attribute()) {
            let equal = matches!(compare(&left, CmpOp::Eq, &right), Ok(true));
            prop_assert_eq!(
                matches!(compare(&left, CmpOp::Ne, &right), Ok(true)),
                !equal
            );
            if equal {
                prop_assert!(!matches!(compare(&left, CmpOp::Lt, &right), Ok(true)));
                prop_assert!(!matches!(compare(&left, CmpOp::Gt, &right), Ok(true)));
            }
        }

        /// A container contains every member it holds, a string contains itself
        /// and every string starts with itself.
        #[test]
        fn contains_accepts_every_member_it_holds(value in attribute()) {
            match &value {
                AttributeValue::S(text) => {
                    prop_assert!(contains(&value, &AttributeValue::S(text.clone()))?);
                    prop_assert!(begins_with(&value, &AttributeValue::S(text.clone()))?);
                }
                AttributeValue::Ss(members) => {
                    for member in members {
                        prop_assert!(contains(&value, &AttributeValue::S(member.clone()))?);
                    }
                }
                AttributeValue::Ns(members) => {
                    for member in members {
                        prop_assert!(contains(&value, &AttributeValue::N(member.clone()))?);
                    }
                }
                AttributeValue::Bs(members) => {
                    for member in members {
                        prop_assert!(contains(&value, &AttributeValue::B(member.clone()))?);
                    }
                }
                AttributeValue::L(elements) => {
                    for element in elements {
                        prop_assert!(contains(&value, element)?);
                    }
                }
                _ => {}
            }
        }

        /// `size()` has a number for every value shape — bytes for strings and
        /// binary, element count for collections, zero for the rest — and never
        /// panics on one it was not written for.
        #[test]
        fn size_of_counts_bytes_or_elements(value in attribute()) {
            let expected = match &value {
                AttributeValue::S(text) => text.len(),
                AttributeValue::B(blob) => blob.as_ref().len(),
                AttributeValue::Ss(members) | AttributeValue::Ns(members) => members.len(),
                AttributeValue::Bs(members) => members.len(),
                AttributeValue::L(elements) => elements.len(),
                AttributeValue::M(entries) => entries.len(),
                _ => 0,
            };
            prop_assert_eq!(
                size_of(&value),
                u64::try_from(expected).ok().unwrap_or(u64::MAX)
            );
        }

        /// The empty needle is a substring and a prefix of every string and a
        /// prefix of every binary — including of an empty one. Every other
        /// value shape rejects the binary prefix as a type mismatch.
        #[test]
        fn an_empty_needle_matches_every_string_and_binary(value in attribute()) {
            let empty_string = AttributeValue::S(String::new());
            let empty_blob = AttributeValue::B(crate::Blob::new(Vec::new()));
            match &value {
                AttributeValue::S(text) => {
                    prop_assert!(contains(&value, &empty_string)?);
                    prop_assert!(begins_with(&value, &empty_string)?);
                    prop_assert!(text.is_empty() || !begins_with(&empty_string, &value)?);
                }
                AttributeValue::B(blob) => {
                    prop_assert!(begins_with(&value, &empty_blob)?);
                    prop_assert!(blob.as_ref().is_empty() || !begins_with(&empty_blob, &value)?);
                }
                _ => {
                    prop_assert!(begins_with(&value, &empty_blob).is_err());
                }
            }
        }
    }
}
