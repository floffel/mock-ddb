//! Exact decimal arithmetic for `N` attributes.
//!
//! DynamoDB numbers are 38-digit decimals carried as strings; the service
//! compares them numerically (`"1.0"` equals `"1"`, `"10"` sorts after `"9"`).
//! Parsing into `f64` would silently lose precision at the top of that range,
//! so the mock normalises each number into `sign · digits · 10^exponent` and
//! compares digit strings.

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// A decimal number in normalised form: `(-1)^negative · digits · 10^exponent`.
///
/// `digits` never has a leading or trailing zero; the empty string is zero.
#[derive(Clone, Debug)]
pub(crate) struct Number {
    negative: bool,
    digits: String,
    exponent: i32,
}

/// DynamoDB's documented maximum significant digits.
const MAX_SIGNIFICANT_DIGITS: usize = 38;

impl Number {
    /// Parse a DynamoDB number string. `None` means the string is not a number.
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let text = text.trim();
        if text.is_empty() {
            return None;
        }
        let (negative, rest) = match text.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, text.strip_prefix('+').unwrap_or(text)),
        };
        let (mantissa, exponent_text) = match rest.find(['e', 'E']) {
            Some(index) => (&rest[..index], Some(&rest[index + 1..])),
            None => (rest, None),
        };
        let mut exponent: i64 = match exponent_text {
            Some(raw) => raw.parse().ok()?,
            None => 0,
        };
        let (integer_part, fraction_part) = match mantissa.find('.') {
            Some(index) => (&mantissa[..index], &mantissa[index + 1..]),
            None => (mantissa, ""),
        };
        if integer_part.is_empty() && fraction_part.is_empty() {
            return None;
        }
        if !integer_part.bytes().all(|b| b.is_ascii_digit())
            || !fraction_part.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        exponent -= i64::try_from(fraction_part.len()).ok()?;

        let mut digits = format!("{integer_part}{fraction_part}");
        let first_significant = digits.find(|c: char| c != '0');
        let Some(first_significant) = first_significant else {
            return Some(Self {
                negative: false,
                digits: String::new(),
                exponent: 0,
            });
        };
        digits.drain(..first_significant);
        let significant_len = digits.trim_end_matches('0').len();
        let stripped = digits.len() - significant_len;
        digits.truncate(significant_len);
        exponent += i64::try_from(stripped).ok()?;

        Some(Self {
            negative,
            digits,
            exponent: i32::try_from(exponent).ok()?,
        })
    }

    /// Whether the value is zero.
    pub(crate) fn is_zero(&self) -> bool {
        self.digits.is_empty()
    }

    /// Number of significant digits.
    pub(crate) fn significant_digits(&self) -> usize {
        self.digits.len()
    }

    /// A canonical string identifying the value: `43`, `-15E-1`, `0`.
    ///
    /// Two numbers are equal exactly when their canonical forms match, which is
    /// what key maps use for identity.
    pub(crate) fn canonical(&self) -> String {
        if self.is_zero() {
            return "0".to_string();
        }
        let sign = if self.negative { "-" } else { "" };
        format!("{sign}{}E{}", self.digits, self.exponent)
    }

    /// The number as an `i64`, if it is integral and fits.
    pub(crate) fn as_i64(&self) -> Option<i64> {
        let magnitude = i64::from(self.exponent);
        if magnitude < 0 || self.digits.len() > 19 {
            return None;
        }
        let mut value: i64 = 0;
        for digit in self.digits.bytes() {
            value = value.checked_mul(10)?;
            value = value.checked_add(i64::from(digit - b'0'))?;
        }
        for _ in 0..magnitude {
            value = value.checked_mul(10)?;
        }
        Some(if self.negative { -value } else { value })
    }

    /// Add two numbers exactly.
    ///
    /// `None` when the exponents are so far apart that aligning them would
    /// allocate an absurd string — DynamoDB's own range makes that unreachable
    /// for stored values, and returning `None` keeps a hostile expression from
    /// exhausting memory.
    pub(crate) fn add(&self, other: &Self) -> Option<Self> {
        let exponent = self.exponent.min(other.exponent);
        let left_shift = usize::try_from(self.exponent - exponent).ok()?;
        let right_shift = usize::try_from(other.exponent - exponent).ok()?;
        if left_shift > 400 || right_shift > 400 {
            return None;
        }
        let left = format!("{}{}", self.digits, "0".repeat(left_shift));
        let right = format!("{}{}", other.digits, "0".repeat(right_shift));
        let left = if left.is_empty() {
            "0".to_string()
        } else {
            left
        };
        let right = if right.is_empty() {
            "0".to_string()
        } else {
            right
        };

        let (negative, digits) = if self.negative == other.negative {
            (self.negative, unsigned_add(&left, &right))
        } else {
            match unsigned_cmp(&left, &right) {
                Ordering::Equal => return Some(Self::zero()),
                Ordering::Greater => (self.negative, unsigned_sub(&left, &right)),
                Ordering::Less => (other.negative, unsigned_sub(&right, &left)),
            }
        };
        Some(Self::from_parts(negative, &digits, exponent))
    }

    /// Subtract `other` from `self`.
    pub(crate) fn sub(&self, other: &Self) -> Option<Self> {
        let negated = Self {
            negative: !other.negative && !other.is_zero(),
            digits: other.digits.clone(),
            exponent: other.exponent,
        };
        self.add(&negated)
    }

    /// Zero.
    fn zero() -> Self {
        Self {
            negative: false,
            digits: String::new(),
            exponent: 0,
        }
    }

    /// Build from an unnormalised `digits · 10^exponent`.
    fn from_parts(negative: bool, digits: &str, exponent: i32) -> Self {
        let trimmed = digits.trim_start_matches('0');
        if trimmed.is_empty() {
            return Self::zero();
        }
        let significant = trimmed.trim_end_matches('0');
        let stripped = trimmed.len() - significant.len();
        let exponent = exponent.saturating_add(i32::try_from(stripped).unwrap_or(i32::MAX));
        Self {
            negative,
            digits: significant.to_string(),
            exponent,
        }
    }

    /// Format the number the way DynamoDB stores it (no exponent notation).
    pub(crate) fn to_plain_string(&self) -> String {
        if self.is_zero() {
            return "0".to_string();
        }
        let sign = if self.negative { "-" } else { "" };
        if self.exponent >= 0 {
            let zeros = usize::try_from(self.exponent).unwrap_or_default();
            format!("{sign}{}{}", self.digits, "0".repeat(zeros))
        } else {
            let scale = self.exponent.unsigned_abs() as usize;
            if scale < self.digits.len() {
                let split = self.digits.len() - scale;
                format!("{sign}{}.{}", &self.digits[..split], &self.digits[split..])
            } else {
                format!(
                    "{sign}0.{}{}",
                    "0".repeat(scale - self.digits.len()),
                    self.digits
                )
            }
        }
    }

    /// Compare the magnitudes (absolute values) of two numbers.
    fn magnitude_cmp(&self, other: &Self) -> Ordering {
        let self_order =
            i64::try_from(self.digits.len()).unwrap_or(i64::MAX) + i64::from(self.exponent);
        let other_order =
            i64::try_from(other.digits.len()).unwrap_or(i64::MAX) + i64::from(other.exponent);
        self_order.cmp(&other_order).then_with(|| {
            let width = self.digits.len().max(other.digits.len());
            let mut left = self.digits.bytes();
            let mut right = other.digits.bytes();
            for _ in 0..width {
                let a = left.next().unwrap_or(b'0');
                let b = right.next().unwrap_or(b'0');
                match a.cmp(&b) {
                    Ordering::Equal => {}
                    ordering => return ordering,
                }
            }
            Ordering::Equal
        })
    }
}

impl PartialEq for Number {
    fn eq(&self, other: &Self) -> bool {
        self.canonical() == other.canonical()
    }
}

impl Eq for Number {}

impl Hash for Number {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.canonical().hash(state);
    }
}

impl PartialOrd for Number {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Number {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.is_zero(), other.is_zero()) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if other.negative {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, true) => {
                if self.negative {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, false) => match (self.negative, other.negative) {
                (false, true) => Ordering::Greater,
                (true, false) => Ordering::Less,
                (false, false) => self.magnitude_cmp(other),
                (true, true) => self.magnitude_cmp(other).reverse(),
            },
        }
    }
}

/// Validate a number string against DynamoDB's documented limits.
///
/// The service rejects numbers with more than 38 significant digits, so the
/// mock does too: a test written against an out-of-range number should fail
/// here rather than pass and fail in production.
pub(crate) fn validate(text: &str) -> crate::Result<()> {
    let Some(number) = Number::parse(text) else {
        return Err(crate::Error::validation(format!(
            "The parameter cannot be converted to a numeric value: {text}"
        )));
    };
    if number.significant_digits() > MAX_SIGNIFICANT_DIGITS {
        return Err(crate::Error::validation(format!(
            "Attempting to store more than {MAX_SIGNIFICANT_DIGITS} significant digits in a Number: {text}"
        )));
    }
    Ok(())
}

/// Add two non-negative decimal strings.
fn unsigned_add(left: &str, right: &str) -> String {
    let mut digits: Vec<u8> = Vec::with_capacity(left.len().max(right.len()) + 1);
    let mut carry = 0_u8;
    let mut left = left.bytes().rev();
    let mut right = right.bytes().rev();
    loop {
        let a = left.next().map(|byte| byte - b'0');
        let b = right.next().map(|byte| byte - b'0');
        if a.is_none() && b.is_none() {
            break;
        }
        let sum = a.unwrap_or(0) + b.unwrap_or(0) + carry;
        digits.push(b'0' + sum % 10);
        carry = sum / 10;
    }
    if carry > 0 {
        digits.push(b'0' + carry);
    }
    digits.reverse();
    String::from_utf8(digits).unwrap_or_else(|_| "0".to_string())
}

/// Compare two non-negative decimal strings of any length.
fn unsigned_cmp(left: &str, right: &str) -> Ordering {
    let left = left.trim_start_matches('0');
    let right = right.trim_start_matches('0');
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

/// Subtract `right` from `left`, where `left >= right` and both are
/// non-negative decimal strings.
fn unsigned_sub(left: &str, right: &str) -> String {
    let mut digits: Vec<u8> = Vec::with_capacity(left.len());
    let mut borrow = 0_i16;
    let mut left = left.bytes().rev();
    let mut right = right.bytes().rev();
    loop {
        let a = left.next().map(|byte| i16::from(byte - b'0'));
        let b = right.next().map_or(0, |byte| i16::from(byte - b'0'));
        if a.is_none() {
            break;
        }
        let mut difference = a.unwrap_or(0) - b - borrow;
        if difference < 0 {
            difference += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        digits.push(b'0' + u8::try_from(difference).unwrap_or(0));
    }
    digits.reverse();
    String::from_utf8(digits).unwrap_or_else(|_| "0".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_integers_decimals_and_exponents() {
        for text in [
            "0", "1", "-1", "10", "1.5", "-1.5", "1e3", "1E-3", "+7", "007",
        ] {
            assert!(Number::parse(text).is_some(), "{text}");
        }
        for text in ["", "-", ".", "1.2.3", "abc", "1e", "e5", "--1", "1 2"] {
            assert!(Number::parse(text).is_none(), "{text}");
        }
    }

    #[test]
    fn equal_values_normalise_to_one_canonical_form() {
        for group in [
            vec!["1", "1.0", "01", "1e0", "1.000"],
            vec!["0", "-0", "0.0", "0e10"],
            vec!["100", "1e2", "100.00"],
            vec!["-1.5", "-15e-1", "-0.15e1"],
        ] {
            let canonical = Number::parse(group[0]).unwrap().canonical();
            for text in group {
                assert_eq!(
                    Number::parse(text).unwrap().canonical(),
                    canonical,
                    "{text}"
                );
            }
        }
    }

    #[test]
    fn orders_numerically_not_lexicographically() {
        let parse = |text: &str| Number::parse(text).unwrap();
        assert!(parse("9") < parse("10"));
        assert!(parse("-10") < parse("-9"));
        assert!(parse("-1") < parse("0"));
        assert!(parse("0.001") < parse("0.01"));
        assert!(parse("1e-130") > parse("0"));
        assert!(parse("9.9e125") > parse("9.8e125"));
        assert_eq!(parse("1.0").cmp(&parse("1")), Ordering::Equal);
    }

    #[test]
    fn compares_numbers_with_more_precision_than_f64() {
        let a = Number::parse("10000000000000000000000000000000000001").unwrap();
        let b = Number::parse("10000000000000000000000000000000000002").unwrap();
        assert!(a < b);
    }

    #[test]
    fn as_i64_handles_ttl_values() {
        assert_eq!(
            Number::parse("1700000000").unwrap().as_i64(),
            Some(1_700_000_000)
        );
        assert_eq!(
            Number::parse("1.7e9").unwrap().as_i64(),
            Some(1_700_000_000)
        );
        assert_eq!(Number::parse("-3").unwrap().as_i64(), Some(-3));
        assert_eq!(Number::parse("1.5").unwrap().as_i64(), None);
        assert_eq!(Number::parse("1e30").unwrap().as_i64(), None);
    }

    #[test]
    fn validate_rejects_malformed_and_oversized_numbers() {
        assert!(validate("1.5").is_ok());
        assert!(validate("1.2.3").is_err());
        assert!(validate(&"9".repeat(39)).is_err());
        assert!(validate(&"9".repeat(38)).is_ok());
    }

    #[test]
    fn arithmetic_is_exact() {
        let add = |left: &str, right: &str| {
            Number::parse(left)
                .unwrap()
                .add(&Number::parse(right).unwrap())
                .unwrap()
                .to_plain_string()
        };
        assert_eq!(add("1", "2"), "3");
        assert_eq!(add("1.5", "1.5"), "3");
        assert_eq!(add("-1", "-2"), "-3");
        assert_eq!(add("-1", "1"), "0");
        assert_eq!(add("0.1", "0.2"), "0.3");
        assert_eq!(add("999", "1"), "1000");
        assert_eq!(add("1e3", "1"), "1001");

        let sub = |left: &str, right: &str| {
            Number::parse(left)
                .unwrap()
                .sub(&Number::parse(right).unwrap())
                .unwrap()
                .to_plain_string()
        };
        assert_eq!(sub("3", "2"), "1");
        assert_eq!(sub("2", "3"), "-1");
        assert_eq!(sub("1.0", "1"), "0");
        assert_eq!(sub("-1", "-2"), "1");
    }

    #[test]
    fn arithmetic_rejects_absurd_exponent_gaps() {
        let huge = Number::parse("1e400").unwrap();
        let tiny = Number::parse("1e-400").unwrap();
        assert!(huge.add(&tiny).is_none());
    }

    #[test]
    fn numbers_hash_and_order_by_value() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Number::parse("1.0").unwrap());
        assert!(set.contains(&Number::parse("1").unwrap()));
        assert!(!set.contains(&Number::parse("1.1").unwrap()));

        let negative = Number::parse("-1").unwrap();
        let positive = Number::parse("1").unwrap();
        assert!(negative < positive);
        assert!(negative < Number::parse("-0.5").unwrap());

        // Adding equal magnitudes of opposite sign normalises to zero through
        // the general path.
        let zero = Number::parse("5")
            .unwrap()
            .add(&Number::parse("0").unwrap())
            .unwrap();
        assert_eq!(zero.to_plain_string(), "5");
        let sum = Number::parse("0.0")
            .unwrap()
            .add(&Number::parse("0.00").unwrap())
            .unwrap();
        assert_eq!(sum.to_plain_string(), "0");
    }

    #[test]
    fn plain_string_round_trips() {
        for text in ["0", "1", "-1", "1.5", "-0.25", "1000000", "0.001"] {
            let number = Number::parse(text).unwrap();
            assert_eq!(number.to_plain_string(), text);
        }
    }
}
