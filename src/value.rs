//! Attribute-value semantics: typing, ordering, equality, sizes and set math.
//!
//! Everything a DynamoDB operation needs to know about a value lives here, in
//! one place, so `put_item`, `query` and the expression evaluator cannot
//! disagree about what `"1.0"` equals or how large an item is.

use std::cmp::Ordering;

use aws_sdk_dynamodb::types::AttributeValue;

use crate::number::Number;
use crate::{Error, Item, Result};

/// The documented maximum size of an item, in bytes.
pub(crate) const MAX_ITEM_SIZE: usize = 400 * 1024;

/// A value usable as part of a key.
///
/// DynamoDB orders key attributes by raw value: strings byte-wise (which for
/// UTF-8 is code-point order), numbers numerically, binary byte-wise.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Scalar {
    /// A string key value.
    S(String),
    /// A number key value, stored in canonical form.
    N(String),
    /// A binary key value.
    B(Vec<u8>),
}

impl Scalar {
    /// Extract a scalar from an attribute value, if it is a key-eligible type.
    pub(crate) fn from_value(value: &AttributeValue) -> Option<Self> {
        match value {
            AttributeValue::S(text) => Some(Self::S(text.clone())),
            AttributeValue::N(text) => {
                Number::parse(text).map(|number| Self::N(number.canonical()))
            }
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
            (Self::N(left), Self::N(right)) => match (Number::parse(left), Number::parse(right)) {
                (Some(left), Some(right)) => left.cmp(&right),
                _ => left.cmp(right),
            },
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

/// Whether the value is a set type.
pub(crate) fn is_set(value: &AttributeValue) -> bool {
    matches!(
        value,
        AttributeValue::Ss(_) | AttributeValue::Ns(_) | AttributeValue::Bs(_)
    )
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
    T: PartialEq + 'a,
    I: Iterator<Item = T> + Clone,
    J: Iterator<Item = T>,
{
    let mut right: Vec<T> = right.collect();
    if left.clone().count() != right.len() {
        return false;
    }
    for item in left {
        let Some(position) = right.iter().position(|candidate| *candidate == item) else {
            return false;
        };
        right.swap_remove(position);
    }
    true
}

/// Number sets compare numerically, so `["1.0"]` equals `["1"]`.
fn numeric_sets_eq(left: &[String], right: &[String]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut right: Vec<Option<Number>> = right.iter().map(|text| Number::parse(text)).collect();
    for text in left {
        let Some(number) = Number::parse(text) else {
            return false;
        };
        let Some(position) = right
            .iter()
            .position(|candidate| candidate.as_ref() == Some(&number))
        else {
            return false;
        };
        right.swap_remove(position);
    }
    true
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
        AttributeValue::L(elements) => {
            elements.iter().map(value_size).sum::<usize>() + 3 * elements.len()
        }
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

/// Validate one value: numbers must parse within DynamoDB's limits and sets
/// must not be empty. Recurses into lists and maps.
pub(crate) fn validate_value(value: &AttributeValue, path: &str) -> Result<()> {
    match value {
        AttributeValue::N(text) => crate::number::validate(text),
        AttributeValue::Ns(members) => {
            if members.is_empty() {
                return Err(Error::validation(format!(
                    "One or more parameter values were invalid: An number set may not be empty ({path})"
                )));
            }
            members
                .iter()
                .try_for_each(|text| crate::number::validate(text))
        }
        AttributeValue::Ss(members) => {
            if members.is_empty() {
                return Err(Error::validation(format!(
                    "One or more parameter values were invalid: An string set may not be empty ({path})"
                )));
            }
            Ok(())
        }
        AttributeValue::Bs(members) => {
            if members.is_empty() {
                return Err(Error::validation(format!(
                    "One or more parameter values were invalid: An binary set may not be empty ({path})"
                )));
            }
            Ok(())
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

/// Union of two sets of the same type. `None` when either side is not a set or
/// the types differ.
pub(crate) fn set_union(left: &AttributeValue, right: &AttributeValue) -> Option<AttributeValue> {
    match (left, right) {
        (AttributeValue::Ss(left), AttributeValue::Ss(right)) => {
            let mut values: Vec<String> = left.clone();
            for member in right {
                if !values.contains(member) {
                    values.push(member.clone());
                }
            }
            Some(AttributeValue::Ss(values))
        }
        (AttributeValue::Ns(left), AttributeValue::Ns(right)) => {
            let mut values: Vec<String> = left.clone();
            for member in right {
                let member_number = Number::parse(member)?;
                let present = values
                    .iter()
                    .filter_map(|existing| Number::parse(existing))
                    .any(|existing| existing == member_number);
                if !present {
                    values.push(member.clone());
                }
            }
            Some(AttributeValue::Ns(values))
        }
        (AttributeValue::Bs(left), AttributeValue::Bs(right)) => {
            let mut values: Vec<aws_sdk_dynamodb::primitives::Blob> = left.clone();
            for member in right {
                if !values
                    .iter()
                    .any(|existing| existing.as_ref() == member.as_ref())
                {
                    values.push(member.clone());
                }
            }
            Some(AttributeValue::Bs(values))
        }
        _ => None,
    }
}

/// Difference of two sets of the same type. `None` when either side is not a
/// set or the types differ. An empty result is reported as `Some(empty set)`;
/// the caller removes the attribute, as DynamoDB does.
pub(crate) fn set_difference(
    left: &AttributeValue,
    right: &AttributeValue,
) -> Option<AttributeValue> {
    match (left, right) {
        (AttributeValue::Ss(left), AttributeValue::Ss(right)) => Some(AttributeValue::Ss(
            left.iter()
                .filter(|member| !right.contains(member))
                .cloned()
                .collect(),
        )),
        (AttributeValue::Ns(left), AttributeValue::Ns(right)) => {
            let removals: Vec<Number> = right
                .iter()
                .filter_map(|text| Number::parse(text))
                .collect();
            Some(AttributeValue::Ns(
                left.iter()
                    .filter(|member| {
                        Number::parse(member).is_none_or(|number| !removals.contains(&number))
                    })
                    .cloned()
                    .collect(),
            ))
        }
        (AttributeValue::Bs(left), AttributeValue::Bs(right)) => Some(AttributeValue::Bs(
            left.iter()
                .filter(|member| {
                    !right
                        .iter()
                        .any(|removal| removal.as_ref() == member.as_ref())
                })
                .cloned()
                .collect(),
        )),
        _ => None,
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
        assert!(value_eq(&set_union(&left, &right).unwrap(), &right));
        assert_eq!(
            set_difference(&right, &left),
            Some(AttributeValue::Ns(Vec::new()))
        );
    }

    #[test]
    fn binary_sets_compare_by_content() {
        let left = AttributeValue::Bs(vec![Blob::new(vec![1_u8, 2])]);
        let right = AttributeValue::Bs(vec![Blob::new(vec![1_u8, 2])]);
        assert!(value_eq(&left, &right));
        assert!(set_union(&left, &right).is_some());
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
        assert!(is_set(&AttributeValue::Ss(vec!["a".into()])));
        assert!(!is_set(&AttributeValue::S("x".into())));
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
        assert!(!value_eq(
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
            set_union(&left, &right),
            Some(AttributeValue::Bs(vec![
                Blob::new(vec![1]),
                Blob::new(vec![2])
            ]))
        );
        assert_eq!(
            set_difference(&left, &right),
            Some(AttributeValue::Bs(vec![Blob::new(vec![1])]))
        );

        let strings = AttributeValue::Ss(vec!["a".into(), "b".into()]);
        assert_eq!(
            set_difference(&strings, &AttributeValue::Ss(vec!["a".into()])),
            Some(AttributeValue::Ss(vec!["b".into()]))
        );

        let numbers = AttributeValue::Ns(vec!["1".into(), "2".into()]);
        assert_eq!(
            set_difference(&numbers, &numbers),
            Some(AttributeValue::Ns(Vec::new()))
        );

        // Mismatched types are not sets of the same kind.
        assert!(set_union(&left, &strings).is_none());
        assert!(set_difference(&strings, &left).is_none());
        assert!(set_union(&strings, &AttributeValue::S("x".into())).is_none());
    }

    #[test]
    fn scalars_compare_and_report_emptiness() {
        let text = Scalar::S("a".into());
        let number = Scalar::N("1".into());
        let binary = Scalar::B(vec![1]);
        assert!(text < number);
        assert!(number < binary);
        assert_eq!(text.partial_cmp(&number), Some(Ordering::Less));
        assert_eq!(text.type_name(), "S");
        assert_eq!(number.type_name(), "N");
        assert_eq!(binary.type_name(), "B");
        assert!(Scalar::S(String::new()).is_empty());
        assert!(Scalar::B(Vec::new()).is_empty());
        assert!(!number.is_empty());
        assert_eq!(Scalar::from_value(&AttributeValue::M(HashMap::new())), None);
        assert_eq!(
            Scalar::from_value(&AttributeValue::N("1.0".into())),
            Some(Scalar::N("1E0".into()))
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
}
