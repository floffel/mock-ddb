//! Attribute-value semantics: typing, ordering, equality, sizes and set math.
//!
//! Everything a DynamoDB operation needs to know about a value lives here, in
//! one place, so `put_item`, `query` and the expression evaluator cannot
//! disagree about what `"1.0"` equals or how large an item is.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::Hash;

use aws_sdk_dynamodb::types::AttributeValue;

use crate::number::Number;
use crate::{Error, Item, Result};

/// The documented maximum size of an item, in bytes.
pub(crate) const MAX_ITEM_SIZE: usize = 400 * 1024;

/// The error a write gets when the item it would store is too large.
///
/// Shared so the `put` path and the update evaluator reject the same limit
/// with the same message: a caller must not be able to tell which code path
/// noticed.
pub(crate) fn item_too_large(size: usize) -> Error {
    Error::validation(format!(
        "Item size has exceeded the maximum allowed size ({size} bytes > {MAX_ITEM_SIZE})"
    ))
}

/// A value usable as part of a key.
///
/// DynamoDB orders key attributes by raw value: strings byte-wise (which for
/// UTF-8 is code-point order), numbers numerically, binary byte-wise.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Scalar {
    /// A string key value.
    S(String),
    /// A number key value, parsed so equality and ordering share one value.
    N(Number),
    /// A binary key value.
    B(Vec<u8>),
}

impl Scalar {
    /// Extract a scalar from an attribute value, if it is a key-eligible type.
    pub(crate) fn from_value(value: &AttributeValue) -> Option<Self> {
        match value {
            AttributeValue::S(text) => Some(Self::S(text.clone())),
            AttributeValue::N(text) => Number::parse(text).map(Self::N),
            AttributeValue::B(blob) => Some(Self::B(blob.as_ref().to_vec())),
            _ => None,
        }
    }

    /// The DynamoDB type name of this scalar.
    pub(crate) fn type_name(&self) -> &'static str {
        match self {
            Self::S(_) => "S",
            Self::N(_) => "N",
            Self::B(_) => "B",
        }
    }

    /// Whether the value is zero-length, which DynamoDB rejects for keys.
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::S(text) => text.is_empty(),
            Self::N(_) => false,
            Self::B(bytes) => bytes.is_empty(),
        }
    }
}

impl Ord for Scalar {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::S(left), Self::S(right)) => left.as_bytes().cmp(right.as_bytes()),
            (Self::N(left), Self::N(right)) => left.cmp(right),
            (Self::B(left), Self::B(right)) => left.cmp(right),
            _ => discriminant(self).cmp(&discriminant(other)),
        }
    }
}

impl PartialOrd for Scalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Rank of a scalar variant, used only to keep [`Scalar`]'s ordering total when
/// two different key types are compared (which key validation prevents).
fn discriminant(scalar: &Scalar) -> u8 {
    match scalar {
        Scalar::S(_) => 0,
        Scalar::N(_) => 1,
        Scalar::B(_) => 2,
    }
}

/// The DynamoDB type name of a value: `S`, `N`, `B`, `SS`, `NS`, `BS`, `BOOL`,
/// `NULL`, `L`, `M`.
pub(crate) fn type_name(value: &AttributeValue) -> &'static str {
    match value {
        AttributeValue::S(_) => "S",
        AttributeValue::N(_) => "N",
        AttributeValue::B(_) => "B",
        AttributeValue::Ss(_) => "SS",
        AttributeValue::Ns(_) => "NS",
        AttributeValue::Bs(_) => "BS",
        AttributeValue::Bool(_) => "BOOL",
        AttributeValue::Null(_) => "NULL",
        AttributeValue::L(_) => "L",
        AttributeValue::M(_) => "M",
        _ => "UNKNOWN",
    }
}

/// Structural and numeric equality, with DynamoDB's number semantics.
///
/// `"1.0"` equals `"1"`, `NULL` equals `NULL`, and sets compare without regard
/// to member order.
pub(crate) fn value_eq(left: &AttributeValue, right: &AttributeValue) -> bool {
    match (left, right) {
        (AttributeValue::S(left), AttributeValue::S(right)) => left == right,
        (AttributeValue::N(left), AttributeValue::N(right)) => {
            match (Number::parse(left), Number::parse(right)) {
                (Some(left), Some(right)) => left == right,
                _ => left == right,
            }
        }
        (AttributeValue::B(left), AttributeValue::B(right)) => left.as_ref() == right.as_ref(),
        (AttributeValue::Bool(left), AttributeValue::Bool(right)) => left == right,
        (AttributeValue::Null(_), AttributeValue::Null(_)) => true,
        (AttributeValue::Ss(left), AttributeValue::Ss(right)) => unordered_eq(
            left.iter().map(String::as_str),
            right.iter().map(String::as_str),
        ),
        (AttributeValue::Ns(left), AttributeValue::Ns(right)) => numeric_sets_eq(left, right),
        (AttributeValue::Bs(left), AttributeValue::Bs(right)) => unordered_eq(
            left.iter().map(std::convert::AsRef::as_ref),
            right.iter().map(aws_sdk_dynamodb::primitives::Blob::as_ref),
        ),
        (AttributeValue::L(left), AttributeValue::L(right)) => {
            left.len() == right.len() && left.iter().zip(right).all(|(a, b)| value_eq(a, b))
        }
        (AttributeValue::M(left), AttributeValue::M(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .all(|(key, value)| right.get(key).is_some_and(|other| value_eq(value, other)))
        }
        _ => false,
    }
}

/// Order-insensitive comparison of two string-ish sets.
fn unordered_eq<'a, T, I, J>(left: I, right: J) -> bool
where
    T: Eq + Hash + 'a,
    I: Iterator<Item = T> + Clone,
    J: Iterator<Item = T>,
{
    let mut counts: HashMap<T, usize> = HashMap::new();
    for item in left.clone() {
        *counts.entry(item).or_default() += 1;
    }
    let mut right_count = 0;
    for item in right {
        right_count += 1;
        let Some(count) = counts.get_mut(&item) else {
            return false;
        };
        if *count == 0 {
            return false;
        }
        *count -= 1;
    }
    if left.count() != right_count {
        return false;
    }
    counts.values().all(|count| *count == 0)
}

/// Number sets compare numerically, so `["1.0"]` equals `["1"]`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum NumericMember {
    Number(Number),
    Raw(String),
}

impl NumericMember {
    fn parse(text: &str) -> Self {
        Number::parse(text).map_or_else(|| Self::Raw(text.to_string()), Self::Number)
    }
}

fn numeric_sets_eq(left: &[String], right: &[String]) -> bool {
    unordered_eq(
        left.iter().map(|text| NumericMember::parse(text)),
        right.iter().map(|text| NumericMember::parse(text)),
    )
}

/// Ordering for the comparison operators.
///
/// Returns `None` when the two values are not comparable — DynamoDB rejects
/// ordered comparisons across types, and so does the mock.
pub(crate) fn value_cmp(left: &AttributeValue, right: &AttributeValue) -> Option<Ordering> {
    match (left, right) {
        (AttributeValue::S(left), AttributeValue::S(right)) => {
            Some(left.as_bytes().cmp(right.as_bytes()))
        }
        (AttributeValue::N(left), AttributeValue::N(right)) => {
            match (Number::parse(left), Number::parse(right)) {
                (Some(left), Some(right)) => Some(left.cmp(&right)),
                _ => None,
            }
        }
        (AttributeValue::B(left), AttributeValue::B(right)) => {
            Some(left.as_ref().cmp(right.as_ref()))
        }
        _ => None,
    }
}

/// Storage size of one attribute value, as DynamoDB charges it.
pub(crate) fn value_size(value: &AttributeValue) -> usize {
    match value {
        AttributeValue::S(text) | AttributeValue::N(text) => text.len(),
        AttributeValue::B(blob) => blob.as_ref().len(),
        AttributeValue::Ss(members) | AttributeValue::Ns(members) => {
            members.iter().map(String::len).sum::<usize>() + 3 * members.len()
        }
        AttributeValue::Bs(members) => {
            members
                .iter()
                .map(|blob| blob.as_ref().len())
                .sum::<usize>()
                + 3 * members.len()
        }
        AttributeValue::L(elements) => list_size(elements),
        AttributeValue::M(entries) => {
            entries
                .iter()
                .map(|(name, value)| name.len() + value_size(value))
                .sum::<usize>()
                + entries.len()
        }
        _ => 1,
    }
}

/// Storage size of a whole item: attribute names plus values.
pub(crate) fn item_size(item: &Item) -> usize {
    item.iter()
        .map(|(name, value)| name.len() + value_size(value))
        .sum()
}

/// Storage size of a list's elements, as `list_append` would concatenate them.
///
/// Separate from [`value_size`] so the update evaluator can measure the two
/// halves of a `list_append` before it joins them and therefore before it
/// allocates the result.
pub(crate) fn list_size(elements: &[AttributeValue]) -> usize {
    elements.iter().map(value_size).sum::<usize>() + 3 * elements.len()
}

/// Why a set member is not storable.
///
/// Kept apart from [`Error`] so the update evaluator can choose the service
/// wording and attribute path for the caller's operand, while the write path
/// uses the same verdict for a whole item.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SetMemberError {
    /// A string set contains the empty string.
    EmptyString,
    /// A binary set contains a zero-length blob.
    EmptyBinary,
    /// A number set member is not a storable DynamoDB number.
    InvalidNumber(String),
    /// Two members are equal after DynamoDB normalization.
    Duplicate,
    /// The value is not one of the three set types.
    NotASet,
}

/// Validate the members shared by every set type.
///
/// DynamoDB rejects empty members and duplicates after normalization: `"1"`
/// and `"1.0"` are the same NS member. Running this before equality or set
/// algebra means the operations never have to choose between silently ignoring
/// malformed data and reporting it as a type mismatch.
pub(crate) fn validate_set_members(value: &AttributeValue) -> Result<(), SetMemberError> {
    match value {
        AttributeValue::Ss(members) => {
            let mut seen = std::collections::HashSet::with_capacity(members.len());
            for member in members {
                if member.is_empty() {
                    return Err(SetMemberError::EmptyString);
                }
                if !seen.insert(member.as_str()) {
                    return Err(SetMemberError::Duplicate);
                }
            }
            Ok(())
        }
        AttributeValue::Ns(members) => {
            let mut seen = std::collections::HashSet::with_capacity(members.len());
            for member in members {
                let number = Number::parse(member)
                    .ok_or_else(|| SetMemberError::InvalidNumber(member.clone()))?;
                crate::number::validate(member)
                    .map_err(|_| SetMemberError::InvalidNumber(member.clone()))?;
                if !seen.insert(number) {
                    return Err(SetMemberError::Duplicate);
                }
            }
            Ok(())
        }
        AttributeValue::Bs(members) => {
            let mut seen = std::collections::HashSet::with_capacity(members.len());
            for member in members {
                if member.as_ref().is_empty() {
                    return Err(SetMemberError::EmptyBinary);
                }
                if !seen.insert(member.as_ref()) {
                    return Err(SetMemberError::Duplicate);
                }
            }
            Ok(())
        }
        _ => Err(SetMemberError::NotASet),
    }
}

fn set_member_validation_error(error: SetMemberError, path: &str) -> Error {
    match error {
        SetMemberError::EmptyString => Error::validation(format!(
            "One or more parameter values were invalid: An string set may not have an empty string as a member ({path})"
        )),
        SetMemberError::EmptyBinary => Error::validation(format!(
            "One or more parameter values were invalid: An binary set may not have an empty binary as a member ({path})"
        )),
        SetMemberError::InvalidNumber(member) => Error::validation(format!(
            "The parameter cannot be converted to a numeric value: {} (attribute: {path})",
            crate::number::excerpt(&member)
        )),
        SetMemberError::Duplicate => Error::validation(format!(
            "One or more parameter values were invalid: Input collection contains duplicates ({path})"
        )),
        SetMemberError::NotASet => Error::validation(format!(
            "One or more parameter values were invalid: The value at {path} is not a set"
        )),
    }
}

/// Validate one value: numbers must parse within DynamoDB's limits and sets
/// must be non-empty with unique, non-empty members. Recurses into lists and
/// maps.
pub(crate) fn validate_value(value: &AttributeValue, path: &str) -> Result<()> {
    match value {
        AttributeValue::N(text) => crate::number::validate(text),
        AttributeValue::Ns(members) => {
            if members.is_empty() {
                return Err(Error::validation(format!(
                    "One or more parameter values were invalid: An number set may not be empty ({path})"
                )));
            }
            validate_set_members(value).map_err(|error| set_member_validation_error(error, path))
        }
        AttributeValue::Ss(members) => {
            if members.is_empty() {
                return Err(Error::validation(format!(
                    "One or more parameter values were invalid: An string set may not be empty ({path})"
                )));
            }
            validate_set_members(value).map_err(|error| set_member_validation_error(error, path))
        }
        AttributeValue::Bs(members) => {
            if members.is_empty() {
                return Err(Error::validation(format!(
                    "One or more parameter values were invalid: An binary set may not be empty ({path})"
                )));
            }
            validate_set_members(value).map_err(|error| set_member_validation_error(error, path))
        }
        AttributeValue::L(elements) => elements
            .iter()
            .enumerate()
            .try_for_each(|(index, element)| validate_value(element, &format!("{path}[{index}]"))),
        AttributeValue::M(entries) => entries
            .iter()
            .try_for_each(|(name, nested)| validate_value(nested, &format!("{path}.{name}"))),
        _ => Ok(()),
    }
}

/// Union of two sets of the same type. `Ok(None)` when either side is not a set
/// or the types differ; a malformed set member is a typed error.
pub(crate) fn set_union(
    left: &AttributeValue,
    right: &AttributeValue,
) -> std::result::Result<Option<AttributeValue>, SetMemberError> {
    if !matches!(
        (left, right),
        (AttributeValue::Ss(_), AttributeValue::Ss(_))
            | (AttributeValue::Ns(_), AttributeValue::Ns(_))
            | (AttributeValue::Bs(_), AttributeValue::Bs(_))
    ) {
        return Ok(None);
    }
    validate_set_members(left)?;
    validate_set_members(right)?;
    match (left, right) {
        (AttributeValue::Ss(left), AttributeValue::Ss(right)) => {
            let mut values: Vec<String> = left.clone();
            let mut present: std::collections::HashSet<&str> =
                left.iter().map(String::as_str).collect();
            for member in right {
                if present.insert(member.as_str()) {
                    values.push(member.clone());
                }
            }
            Ok(Some(AttributeValue::Ss(values)))
        }
        (AttributeValue::Ns(left), AttributeValue::Ns(right)) => {
            let mut values: Vec<String> = left.clone();
            let mut present: std::collections::HashSet<Number> = left
                .iter()
                .map(|member| {
                    Number::parse(member)
                        .ok_or_else(|| SetMemberError::InvalidNumber(member.clone()))
                })
                .collect::<std::result::Result<_, _>>()?;
            for member in right {
                let member_number = Number::parse(member)
                    .ok_or_else(|| SetMemberError::InvalidNumber(member.clone()))?;
                if present.insert(member_number) {
                    values.push(member.clone());
                }
            }
            Ok(Some(AttributeValue::Ns(values)))
        }
        (AttributeValue::Bs(left), AttributeValue::Bs(right)) => {
            let mut values: Vec<aws_sdk_dynamodb::primitives::Blob> = left.clone();
            let mut present: std::collections::HashSet<&[u8]> =
                left.iter().map(std::convert::AsRef::as_ref).collect();
            for member in right {
                if present.insert(member.as_ref()) {
                    values.push(member.clone());
                }
            }
            Ok(Some(AttributeValue::Bs(values)))
        }
        _ => Ok(None),
    }
}

/// Difference of two sets of the same type. `Ok(None)` when either side is not
/// a set or the types differ. An empty result is reported as `Ok(Some(empty
/// set))`; the caller removes the attribute, as DynamoDB does.
pub(crate) fn set_difference(
    left: &AttributeValue,
    right: &AttributeValue,
) -> std::result::Result<Option<AttributeValue>, SetMemberError> {
    if !matches!(
        (left, right),
        (AttributeValue::Ss(_), AttributeValue::Ss(_))
            | (AttributeValue::Ns(_), AttributeValue::Ns(_))
            | (AttributeValue::Bs(_), AttributeValue::Bs(_))
    ) {
        return Ok(None);
    }
    validate_set_members(left)?;
    validate_set_members(right)?;
    match (left, right) {
        (AttributeValue::Ss(left), AttributeValue::Ss(right)) => {
            let removals: std::collections::HashSet<&str> =
                right.iter().map(String::as_str).collect();
            Ok(Some(AttributeValue::Ss(
                left.iter()
                    .filter(|member| !removals.contains(member.as_str()))
                    .cloned()
                    .collect(),
            )))
        }
        (AttributeValue::Ns(left), AttributeValue::Ns(right)) => {
            let removals: std::collections::HashSet<Number> = right
                .iter()
                .map(|text| {
                    Number::parse(text).ok_or_else(|| SetMemberError::InvalidNumber(text.clone()))
                })
                .collect::<std::result::Result<_, _>>()?;
            let parsed_left = left
                .iter()
                .map(|member| -> std::result::Result<_, SetMemberError> {
                    let number = Number::parse(member)
                        .ok_or_else(|| SetMemberError::InvalidNumber(member.clone()))?;
                    Ok((member, number))
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(Some(AttributeValue::Ns(
                parsed_left
                    .into_iter()
                    .filter(|(_, number)| !removals.contains(number))
                    .map(|(member, _)| member.clone())
                    .collect(),
            )))
        }
        (AttributeValue::Bs(left), AttributeValue::Bs(right)) => {
            let removals: std::collections::HashSet<&[u8]> =
                right.iter().map(std::convert::AsRef::as_ref).collect();
            Ok(Some(AttributeValue::Bs(
                left.iter()
                    .filter(|member| !removals.contains(member.as_ref()))
                    .cloned()
                    .collect(),
            )))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::primitives::Blob;
    use std::collections::HashMap;

    #[test]
    fn numbers_compare_numerically_and_by_value() {
        let one = AttributeValue::N("1".to_string());
        let one_point_zero = AttributeValue::N("1.0".to_string());
        assert!(value_eq(&one, &one_point_zero));
        assert_eq!(value_cmp(&one, &one_point_zero), Some(Ordering::Equal));
        assert_eq!(
            value_cmp(
                &AttributeValue::N("9".into()),
                &AttributeValue::N("10".into())
            ),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn ordering_across_types_is_none() {
        assert!(value_cmp(
            &AttributeValue::S("a".into()),
            &AttributeValue::N("1".into())
        )
        .is_none());
        assert!(!value_eq(
            &AttributeValue::S("1".into()),
            &AttributeValue::N("1".into())
        ));
    }

    #[test]
    fn set_equality_ignores_order_and_number_spelling() {
        let left = AttributeValue::Ns(vec!["1.0".into(), "2".into()]);
        let right = AttributeValue::Ns(vec!["2".into(), "1".into()]);
        assert!(value_eq(&left, &right));
        assert!(value_eq(
            &set_union(&left, &right).unwrap().unwrap(),
            &right
        ));
        assert_eq!(
            set_difference(&right, &left).unwrap(),
            Some(AttributeValue::Ns(Vec::new()))
        );
    }

    #[test]
    fn binary_sets_compare_by_content() {
        let left = AttributeValue::Bs(vec![Blob::new(vec![1_u8, 2])]);
        let right = AttributeValue::Bs(vec![Blob::new(vec![1_u8, 2])]);
        assert!(value_eq(&left, &right));
        assert!(set_union(&left, &right).unwrap().is_some());
    }

    #[test]
    fn item_size_counts_names_and_nesting() {
        let mut item = Item::new();
        item.insert("pk".into(), AttributeValue::S("abcd".into()));
        item.insert("n".into(), AttributeValue::N("12".into()));
        assert_eq!(item_size(&item), 2 + 4 + 1 + 2);
    }

    #[test]
    fn type_names_and_set_detection() {
        let cases = [
            (AttributeValue::S("x".into()), "S"),
            (AttributeValue::N("1".into()), "N"),
            (AttributeValue::B(Blob::new(vec![1])), "B"),
            (AttributeValue::Ss(vec!["a".into()]), "SS"),
            (AttributeValue::Ns(vec!["1".into()]), "NS"),
            (AttributeValue::Bs(vec![Blob::new(vec![1])]), "BS"),
            (AttributeValue::Bool(true), "BOOL"),
            (AttributeValue::Null(true), "NULL"),
            (AttributeValue::L(vec![]), "L"),
            (AttributeValue::M(HashMap::new()), "M"),
        ];
        for (value, expected) in cases {
            assert_eq!(type_name(&value), expected);
        }
    }

    #[test]
    fn value_equality_covers_every_shape() {
        assert!(value_eq(
            &AttributeValue::B(Blob::new(vec![1, 2])),
            &AttributeValue::B(Blob::new(vec![1, 2]))
        ));
        assert!(!value_eq(
            &AttributeValue::B(Blob::new(vec![1])),
            &AttributeValue::B(Blob::new(vec![2]))
        ));
        assert!(value_eq(
            &AttributeValue::Bool(true),
            &AttributeValue::Bool(true)
        ));
        assert!(!value_eq(
            &AttributeValue::Bool(true),
            &AttributeValue::Bool(false)
        ));
        assert!(value_eq(
            &AttributeValue::Null(true),
            &AttributeValue::Null(true)
        ));
        assert!(value_eq(
            &AttributeValue::L(vec![AttributeValue::S("a".into())]),
            &AttributeValue::L(vec![AttributeValue::S("a".into())])
        ));
        assert!(!value_eq(
            &AttributeValue::L(vec![AttributeValue::S("a".into())]),
            &AttributeValue::L(vec![
                AttributeValue::S("a".into()),
                AttributeValue::S("b".into())
            ])
        ));
        let map = |pairs: &[(&str, AttributeValue)]| {
            AttributeValue::M(
                pairs
                    .iter()
                    .map(|(key, value)| ((*key).to_string(), value.clone()))
                    .collect(),
            )
        };
        assert!(value_eq(
            &map(&[("a", AttributeValue::S("x".into()))]),
            &map(&[("a", AttributeValue::S("x".into()))])
        ));
        assert!(!value_eq(
            &map(&[("a", AttributeValue::S("x".into()))]),
            &map(&[("a", AttributeValue::S("y".into()))])
        ));
        assert!(!value_eq(
            &map(&[("a", AttributeValue::S("x".into()))]),
            &map(&[]),
        ));
        // Sets compare without regard to member order, and a mismatch is caught.
        assert!(value_eq(
            &AttributeValue::Ss(vec!["a".into(), "b".into()]),
            &AttributeValue::Ss(vec!["b".into(), "a".into()])
        ));
        assert!(!value_eq(
            &AttributeValue::Ss(vec!["a".into(), "b".into()]),
            &AttributeValue::Ss(vec!["a".into(), "c".into()])
        ));
        assert!(!value_eq(
            &AttributeValue::Ns(vec!["1".into()]),
            &AttributeValue::Ns(vec!["1".into(), "2".into()])
        ));
        // Unparseable numbers fall back to text comparison.
        assert!(value_eq(
            &AttributeValue::N("nope".into()),
            &AttributeValue::N("nope".into())
        ));
        assert!(!value_eq(
            &AttributeValue::N("nope".into()),
            &AttributeValue::N("other".into())
        ));
        assert!(value_eq(
            &AttributeValue::Ns(vec!["nope".into()]),
            &AttributeValue::Ns(vec!["nope".into()])
        ));
    }

    #[test]
    fn ordering_and_set_operations_for_binary_and_string_sets() {
        assert_eq!(
            value_cmp(
                &AttributeValue::B(Blob::new(vec![1])),
                &AttributeValue::B(Blob::new(vec![2]))
            ),
            Some(Ordering::Less)
        );
        assert!(value_cmp(&AttributeValue::Bool(true), &AttributeValue::Bool(false)).is_none());

        let left = AttributeValue::Bs(vec![Blob::new(vec![1])]);
        let right = AttributeValue::Bs(vec![Blob::new(vec![2])]);
        assert_eq!(
            set_union(&left, &right).unwrap(),
            Some(AttributeValue::Bs(vec![
                Blob::new(vec![1]),
                Blob::new(vec![2])
            ]))
        );
        assert_eq!(
            set_difference(&left, &right).unwrap(),
            Some(AttributeValue::Bs(vec![Blob::new(vec![1])]))
        );

        let strings = AttributeValue::Ss(vec!["a".into(), "b".into()]);
        assert_eq!(
            set_difference(&strings, &AttributeValue::Ss(vec!["a".into()])).unwrap(),
            Some(AttributeValue::Ss(vec!["b".into()]))
        );

        let numbers = AttributeValue::Ns(vec!["1".into(), "2".into()]);
        assert_eq!(
            set_difference(&numbers, &numbers).unwrap(),
            Some(AttributeValue::Ns(Vec::new()))
        );

        // Mismatched types are not sets of the same kind.
        assert!(set_union(&left, &strings).unwrap().is_none());
        assert!(set_difference(&strings, &left).unwrap().is_none());
        assert!(set_union(&strings, &AttributeValue::S("x".into()))
            .unwrap()
            .is_none());
    }

    #[test]
    fn scalars_compare_and_report_emptiness() {
        let text = Scalar::S("a".into());
        let number = Scalar::N(Number::parse("1").unwrap());
        let binary = Scalar::B(vec![1]);
        assert!(text < number);
        assert!(number < binary);
        assert_eq!(text.partial_cmp(&number), Some(Ordering::Less));
        assert_eq!(text.type_name(), "S");
        assert_eq!(number.type_name(), "N");
        assert_eq!(binary.type_name(), "B");
        let empty_string = Scalar::S(String::new()).is_empty();
        let empty_binary = Scalar::B(Vec::new()).is_empty();
        let number_is_empty = number.is_empty();
        assert!(empty_string);
        assert!(empty_binary);
        assert!(!number_is_empty);
        assert_eq!(Scalar::from_value(&AttributeValue::M(HashMap::new())), None);
        assert_eq!(
            Scalar::from_value(&AttributeValue::N("1.0".into())),
            Some(Scalar::N(Number::parse("1").unwrap()))
        );
        assert_eq!(Scalar::from_value(&AttributeValue::N("bad".into())), None);
        assert_eq!(
            Scalar::S("a".into()).cmp(&Scalar::S("b".into())),
            Ordering::Less
        );
        assert_eq!(
            Scalar::B(vec![2]).cmp(&Scalar::B(vec![1])),
            Ordering::Greater
        );
    }

    #[test]
    fn scalar_equality_hash_and_order_share_the_parsed_number() {
        use std::hash::{DefaultHasher, Hash, Hasher};

        fn hash(scalar: &Scalar) -> u64 {
            let mut hasher = DefaultHasher::new();
            scalar.hash(&mut hasher);
            hasher.finish()
        }

        let one = Scalar::from_value(&AttributeValue::N("1".into())).unwrap();
        let one_point_zero = Scalar::from_value(&AttributeValue::N("1.0".into())).unwrap();
        let one_exponent = Scalar::from_value(&AttributeValue::N("1e0".into())).unwrap();
        assert_eq!(one, one_point_zero);
        assert_eq!(one, one_exponent);
        assert_eq!(one.cmp(&one_point_zero), Ordering::Equal);
        assert_eq!(hash(&one), hash(&one_point_zero));

        let ten = Scalar::from_value(&AttributeValue::N("10".into())).unwrap();
        assert_eq!(one.cmp(&ten), Ordering::Less);
        assert_ne!(one, ten);
    }

    #[test]
    fn validation_recurses_into_maps() {
        let nested = AttributeValue::M(HashMap::from([(
            "bad".to_string(),
            AttributeValue::Ns(Vec::new()),
        )]));
        assert!(validate_value(&nested, "root").is_err());
    }

    #[test]
    fn validation_rejects_empty_sets_and_bad_numbers() {
        assert!(validate_value(&AttributeValue::Ns(Vec::new()), "x").is_err());
        assert!(validate_value(&AttributeValue::Ss(Vec::new()), "x").is_err());
        assert!(validate_value(&AttributeValue::Bs(Vec::new()), "x").is_err());
        assert!(validate_value(&AttributeValue::N("nope".into()), "x").is_err());
        assert!(validate_value(
            &AttributeValue::L(vec![AttributeValue::N("nope".into())]),
            "x"
        )
        .is_err());
    }
    #[test]
    fn set_validation_rejects_duplicates_and_empty_members() {
        let cases = [
            (
                AttributeValue::Ss(vec![String::new()]),
                "empty string as a member",
            ),
            (
                AttributeValue::Ss(vec!["a".into(), "a".into()]),
                "Input collection contains duplicates",
            ),
            (
                AttributeValue::Ns(vec!["1".into(), "1.0".into()]),
                "Input collection contains duplicates",
            ),
            (
                AttributeValue::Ns(vec!["not-a-number".into()]),
                "cannot be converted to a numeric value",
            ),
            (
                AttributeValue::Bs(vec![Blob::new(Vec::new())]),
                "empty binary as a member",
            ),
            (
                AttributeValue::Bs(vec![Blob::new(vec![1]), Blob::new(vec![1])]),
                "Input collection contains duplicates",
            ),
        ];
        for (value, expected) in cases {
            let error = validate_value(&value, "members").unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "expected `{expected}` in `{error}`"
            );
        }

        for value in [
            AttributeValue::Ss(vec!["a".into()]),
            AttributeValue::Ns(vec!["1".into(), "2.0".into()]),
            AttributeValue::Bs(vec![Blob::new(vec![1]), Blob::new(vec![2])]),
        ] {
            validate_value(&value, "members").unwrap();
        }
    }

    #[test]
    fn set_operations_use_canonical_multiset_membership() {
        let left = AttributeValue::Ns(vec!["1.0".into(), "2".into()]);
        let right = AttributeValue::Ns(vec!["1".into()]);
        assert_eq!(
            set_difference(&left, &right).unwrap(),
            Some(AttributeValue::Ns(vec!["2".into()]))
        );
        assert_eq!(
            set_union(&left, &right).unwrap(),
            Some(AttributeValue::Ns(vec!["1.0".into(), "2".into()]))
        );

        let raw = AttributeValue::Ns(vec!["nope".into()]);
        assert!(value_eq(&raw, &raw));
        assert_eq!(
            set_difference(&raw, &raw),
            Err(SetMemberError::InvalidNumber("nope".into()))
        );
        assert_eq!(
            set_union(&raw, &raw),
            Err(SetMemberError::InvalidNumber("nope".into()))
        );
        assert!(validate_value(&raw, "members").is_err());
    }

    #[test]
    fn large_set_operations_are_linear_enough_for_large_items() {
        let count = 20_000_usize;
        let members: Vec<String> = (0..count).map(|index| index.to_string()).collect();
        let left = AttributeValue::Ns(members.clone());
        let right = AttributeValue::Ns(members.into_iter().rev().collect());
        assert!(value_eq(&left, &right));
        assert_eq!(
            set_difference(&left, &right).unwrap(),
            Some(AttributeValue::Ns(Vec::new()))
        );
    }
}
