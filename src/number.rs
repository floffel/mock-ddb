//! Exact decimal arithmetic for `N` attributes.
//!
//! DynamoDB numbers are 38-digit decimals carried as strings; the service
//! compares them numerically (`"1.0"` equals `"1"`, `"10"` sorts after `"9"`).
//! Parsing into `f64` would silently lose precision at the top of that range,
//! so the value is held as a [`BigDecimal`]: ordering, equality, normalisation
//! and addition are the maintained crate's, not a local implementation.
//!
//! What stays local is DynamoDB's *grammar*. It is narrower than the crate's —
//! the service rejects `1_000` and `1.2.3`, while `BigDecimal` reads digit
//! groups separated by underscores — so the literal is validated here before it
//! reaches the parser, and the DynamoDB range limits
//! (`1E-130` … `9.99…E+125`, 38 significant digits) are checked on the parsed
//! value — and, through the same check, on the result of `add` and `sub`, so
//! arithmetic cannot hand back a number the write path would reject.

use std::hash::{Hash, Hasher};
use std::str::FromStr;

use bigdecimal::{BigDecimal, Zero};

/// A DynamoDB number: an exact decimal value.
///
/// The value is kept normalised (mantissa without leading or trailing zeros),
/// so equality, ordering and hashing all agree on `"1"`, `"1.0"` and `"1e0"`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Number {
    /// The exact value; ordering and arithmetic are delegated to it.
    value: BigDecimal,
    /// `value == mantissa · 10^exponent` for the normalised mantissa.
    ///
    /// Cached because the padding guards below need it without a `BigInt`
    /// round trip, and because normalisation determines it uniquely.
    exponent: i64,
}

/// DynamoDB's documented maximum significant digits.
const MAX_SIGNIFICANT_DIGITS: usize = 38;

/// Smallest magnitude DynamoDB stores, as `digits.len() + exponent`.
///
/// The documented floor is `1E-130`: one significant digit with exponent
/// `-130` gives `-129`. Anything below that is rejected by the service.
const MIN_ORDER: i64 = -129;

/// Largest magnitude DynamoDB stores, as `digits.len() + exponent`.
///
/// The documented ceiling is `9.99…E+125` (38 nines): 38 digits with exponent
/// `88` gives `126`. `127` would be `1E+126`, which the service rejects.
const MAX_ORDER: i64 = 126;

/// Longest run of padding zeros [`Number::to_plain_string`] will produce.
///
/// The documented range covers it exactly: `1E+125` pads 125 zeros and
/// `1E-130` pads 129. A number that needs more is not storable, and the
/// formatter returns `None` instead of allocating it — a 5-byte input such as
/// `1e2147483647` would otherwise ask for gigabytes.
const MAX_PADDING: usize = 130;

/// Why a value cannot be stored, or that it can.
///
/// One verdict, consulted from both ends of the module: [`validate`] turns it
/// into the service's error for a request, and [`Number::add`] / [`Number::sub`]
/// refuse a result that the write path would reject. Keeping the two limits in
/// a single function is the point — a boundary enforced only on the way in lets
/// arithmetic manufacture a number the service would never store, so a mock
/// expression test passes here and the same expression fails against DynamoDB.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Storability {
    /// Within DynamoDB's documented limits.
    Storable,
    /// More than [`MAX_SIGNIFICANT_DIGITS`] significant digits.
    TooManyDigits,
    /// Magnitude outside `1E-130` … `9.99…E+125`.
    OutOfRange,
}

/// Furthest apart two exponents may be before [`Number::add`] gives up.
///
/// Aligning the operands means materialising the gap as zeros; DynamoDB's own
/// exponent range keeps real values well inside this.
const MAX_ALIGNMENT_SHIFT: u64 = 400;

impl Number {
    /// Parse a DynamoDB number string. `None` means the string is not a number.
    ///
    /// The grammar check is DynamoDB's, not the decimal crate's: see the
    /// module documentation for why it cannot be delegated.
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let text = text.trim();
        if text.is_empty() {
            return None;
        }
        let unsigned = match text.strip_prefix('-') {
            Some(rest) => rest,
            None => text.strip_prefix('+').unwrap_or(text),
        };
        let (mantissa, exponent_text) = match unsigned.find(['e', 'E']) {
            Some(index) => (&unsigned[..index], Some(&unsigned[index + 1..])),
            None => (unsigned, None),
        };
        if let Some(raw) = exponent_text {
            // `BigDecimal` takes an `i128` exponent; the service's own limit is
            // far smaller, and an exponent that does not fit an `i64` is not
            // storable whichever way it is read.
            if raw.parse::<i64>().is_err() {
                return None;
            }
        }
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
        // A leading `+` is grammar, and `BigDecimal` only accepts it on the
        // exponent — pass the literal without it.
        let literal = text.strip_prefix('+').unwrap_or(text);
        Self::from_decimal(&BigDecimal::from_str(literal).ok()?)
    }

    /// Wrap an exact value, normalising it and caching its exponent.
    ///
    /// `None` when the value's scale has no negatable `i64` exponent
    /// (`i64::MIN`). No DynamoDB value comes near it, and such a number could
    /// not be rendered, compared or stored, so it is refused rather than
    /// carried around.
    ///
    /// This is the only place the exponent is produced: it comes from
    /// `checked_neg` on the crate's scale, and every later use is checked or
    /// saturating ([`Self::order`], [`Self::alignment_fits`],
    /// [`Self::to_plain_string`]). An exponent therefore never reaches a raw
    /// `i64` operation, however the literal was spelled.
    fn from_decimal(value: &BigDecimal) -> Option<Self> {
        let value = value.normalized();
        let (_, scale) = value.as_bigint_and_exponent();
        let exponent = scale.checked_neg()?;
        Some(Self { value, exponent })
    }

    /// Whether the value is zero.
    pub(crate) fn is_zero(&self) -> bool {
        self.value.is_zero()
    }

    /// The normalised mantissa as `(negative, digits)`, or `None` for zero,
    /// which has no significant digits.
    fn mantissa(&self) -> Option<(bool, String)> {
        if self.is_zero() {
            return None;
        }
        let (mantissa, _) = self.value.as_bigint_and_exponent();
        let text = mantissa.to_str_radix(10);
        match text.strip_prefix('-') {
            Some(digits) => Some((true, digits.to_string())),
            None => Some((false, text)),
        }
    }

    /// Number of significant digits. Zero has none.
    pub(crate) fn significant_digits(&self) -> usize {
        self.mantissa().map_or(0, |(_, digits)| digits.len())
    }

    /// The value's order of magnitude: `digits.len() + exponent`, i.e. the
    /// power of ten just below the magnitude.
    ///
    /// Zero has no order of magnitude; callers check [`Self::is_zero`] first.
    fn order(&self) -> i64 {
        let width = i64::try_from(self.significant_digits()).unwrap_or(i64::MAX);
        width.saturating_add(self.exponent)
    }

    /// Whether the value fits DynamoDB's documented exponent range
    /// (`1E-130` … `9.99…E+125`).
    fn in_documented_range(&self) -> bool {
        self.is_zero() || (MIN_ORDER..=MAX_ORDER).contains(&self.order())
    }

    /// Whether the value is within both documented limits: at most
    /// [`MAX_SIGNIFICANT_DIGITS`] significant digits and a magnitude inside
    /// [`MIN_ORDER`] … [`MAX_ORDER`].
    ///
    /// The digit count is checked first because it is the limit arithmetic can
    /// cross while staying inside the exponent range; the two are otherwise
    /// independent, and the order only decides which wording a request that
    /// breaks both gets.
    fn storability(&self) -> Storability {
        if self.significant_digits() > MAX_SIGNIFICANT_DIGITS {
            Storability::TooManyDigits
        } else if self.in_documented_range() {
            Storability::Storable
        } else {
            Storability::OutOfRange
        }
    }

    /// Wrap an exact value only when DynamoDB would store it.
    ///
    /// This is [`Self::from_decimal`] plus [`Self::storability`], and it is what
    /// [`Self::add`] and [`Self::sub`] return through: a result the service
    /// rejects is not a number the mock hands out. Parsing does *not* go through
    /// here — [`Self::parse`] has to represent an out-of-limit literal so that
    /// [`validate`] can name the limit it broke.
    fn from_storable_decimal(value: &BigDecimal) -> Option<Self> {
        Self::from_decimal(value).filter(|number| number.storability() == Storability::Storable)
    }

    /// A canonical string identifying the value: `43`, `-15E-1`, `0`.
    ///
    /// Two numbers are equal exactly when their canonical forms match, which is
    /// what key maps use for identity.
    pub(crate) fn canonical(&self) -> String {
        self.mantissa().map_or_else(
            || "0".to_string(),
            |(negative, digits)| {
                let sign = if negative { "-" } else { "" };
                format!("{sign}{digits}E{}", self.exponent)
            },
        )
    }

    /// The number as an `i64`, if it is integral and fits.
    pub(crate) fn as_i64(&self) -> Option<i64> {
        if self.is_zero() {
            return Some(0);
        }
        let (negative, digits) = self.mantissa()?;
        if self.exponent < 0 || digits.len() > 19 {
            return None;
        }
        let mut value: i64 = 0;
        for digit in digits.bytes() {
            value = value.checked_mul(10)?;
            value = value.checked_add(i64::from(digit - b'0'))?;
        }
        for _ in 0..self.exponent {
            value = value.checked_mul(10)?;
        }
        Some(if negative { -value } else { value })
    }

    /// Whether aligning the two exponents would materialise an absurd run of
    /// zeros. Two exponents within [`MAX_ALIGNMENT_SHIFT`] of each other are
    /// aligned; anything further apart is refused.
    fn alignment_fits(&self, other: &Self) -> bool {
        self.exponent.saturating_sub(other.exponent).unsigned_abs() <= MAX_ALIGNMENT_SHIFT
    }

    /// Add two numbers exactly.
    ///
    /// `None` in two cases, both of which mean "the service has no answer":
    /// when the exponents are so far apart that aligning them would materialise
    /// an absurd run of digits — returning `None` keeps a hostile expression
    /// from exhausting memory — and when the exact sum is not storable, because
    /// the service rejects it too rather than handing back a number the writer
    /// would refuse. `9e125 + 9e125` is the second case: the sum is `1.8e126`,
    /// whose magnitude is one decade past the documented ceiling. The addition
    /// itself is `BigDecimal`'s, and so is the exactness — nothing is rounded,
    /// so a sum with more than 38 significant digits is refused instead of
    /// silently truncated.
    pub(crate) fn add(&self, other: &Self) -> Option<Self> {
        if !self.alignment_fits(other) {
            return None;
        }
        let sum = &self.value + &other.value;
        Self::from_storable_decimal(&sum)
    }

    /// Subtract `other` from `self`.
    ///
    /// Refused under the same two conditions as [`Self::add`].
    pub(crate) fn sub(&self, other: &Self) -> Option<Self> {
        if !self.alignment_fits(other) {
            return None;
        }
        let difference = &self.value - &other.value;
        Self::from_storable_decimal(&difference)
    }

    /// Format the number the way DynamoDB stores it (no exponent notation).
    ///
    /// `None` when rendering would need more than [`MAX_PADDING`] zeros — the
    /// value is outside DynamoDB's range, so it is not storable and there is
    /// nothing useful to render. Returning `None` keeps a 5-byte input like
    /// `1e2147483647` from asking for gigabytes of zero padding.
    pub(crate) fn to_plain_string(&self) -> Option<String> {
        let Some((negative, digits)) = self.mantissa() else {
            return Some("0".to_string());
        };
        let sign = if negative { "-" } else { "" };
        if self.exponent >= 0 {
            let zeros = usize::try_from(self.exponent).ok()?;
            if zeros > MAX_PADDING {
                return None;
            }
            Some(format!("{sign}{digits}{}", "0".repeat(zeros)))
        } else {
            // `unsigned_abs` keeps `i64::MIN` from overflowing.
            let scale = usize::try_from(self.exponent.unsigned_abs()).ok()?;
            if scale > MAX_PADDING {
                return None;
            }
            if scale < digits.len() {
                let split = digits.len() - scale;
                Some(format!("{sign}{}.{}", &digits[..split], &digits[split..]))
            } else {
                Some(format!(
                    "{sign}0.{}{digits}",
                    "0".repeat(scale - digits.len())
                ))
            }
        }
    }
}

impl Hash for Number {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.canonical().hash(state);
    }
}

/// Validate a number string against DynamoDB's documented limits.
///
/// The service rejects numbers with more than 38 significant digits and
/// numbers outside `1E-130` … `9.99…E+125`, so the mock does too: a test
/// written against an out-of-range number should fail here rather than pass
/// and fail in production. The limits themselves live in
/// [`Number::storability`], which [`Number::add`] and [`Number::sub`] consult
/// for their results as well — the same boundary on the way in and on the way
/// back. The exponent check is also what keeps `1e2147483647` from reaching
/// [`Number::to_plain_string`] with a padding request that no machine can
/// satisfy.
pub(crate) fn validate(text: &str) -> crate::Result<()> {
    let Some(number) = Number::parse(text) else {
        return Err(crate::Error::validation(format!(
            "The parameter cannot be converted to a numeric value: {text}"
        )));
    };
    match number.storability() {
        Storability::Storable => Ok(()),
        Storability::TooManyDigits => Err(crate::Error::validation(format!(
            "Attempting to store more than {MAX_SIGNIFICANT_DIGITS} significant digits in a Number: {text}"
        ))),
        Storability::OutOfRange => Err(crate::Error::validation(format!(
            "Attempting to store a Number outside the supported exponent range (1E-130 to 9.99E+125): {text}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

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
    fn grammar_is_dynamodb_not_bigdecimals() {
        // `BigDecimal` reads these as numbers; the service does not, so the
        // mock must not either. This is the reason the grammar check stays
        // local instead of being handed to the crate.
        for text in ["1_000", "1_._2", "271576662.__E4", "123_", "1.2_3"] {
            assert!(BigDecimal::from_str(text).is_ok(), "{text} parses");
            assert_eq!(Number::parse(text), None, "{text} must be rejected");
        }
        // ... and the forms both accept keep working, sign and all.
        assert_eq!(
            Number::parse("+7").map(|number| number.canonical()),
            Some("7E0".to_string())
        );
        assert_eq!(
            Number::parse("007").map(|number| number.canonical()),
            Some("7E0".to_string())
        );
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
    fn canonical_forms_carry_the_normalised_mantissa() {
        for (text, canonical) in [
            ("0", "0"),
            ("-0.00", "0"),
            ("1.5", "15E-1"),
            ("-1.5", "-15E-1"),
            ("100", "1E2"),
            ("0.001", "1E-3"),
            ("1e125", "1E125"),
            ("-1e-130", "-1E-130"),
        ] {
            assert_eq!(
                Number::parse(text).unwrap().canonical(),
                canonical,
                "{text}"
            );
        }
        // 38 significant digits survive intact - the reason `f64` and the
        // fixed-width decimal crates are not an option here.
        let wide = "12345678901234567890123456789012345678";
        assert_eq!(
            Number::parse(wide).unwrap().canonical(),
            format!("{wide}E0")
        );
        assert_eq!(Number::parse(wide).unwrap().significant_digits(), 38);
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
        assert_eq!(Number::parse("0").unwrap().as_i64(), Some(0));
        assert_eq!(Number::parse("1.5").unwrap().as_i64(), None);
        assert_eq!(Number::parse("1e30").unwrap().as_i64(), None);
        // Just inside and just outside `i64::MAX`.
        assert_eq!(
            Number::parse("9223372036854775807").unwrap().as_i64(),
            Some(i64::MAX)
        );
        assert_eq!(Number::parse("9223372036854775808").unwrap().as_i64(), None);
    }

    #[test]
    fn validate_rejects_malformed_and_oversized_numbers() {
        assert!(validate("1.5").is_ok());
        assert!(validate("1.2.3").is_err());
        assert!(validate(&"9".repeat(39)).is_err());
        assert!(validate(&"9".repeat(38)).is_ok());
    }

    #[test]
    fn validate_bounds_the_exponent_to_the_documented_range() {
        // Inside: 1E-130 (order -129) through 9.99…E+125 (order 126).
        assert!(validate("1e-130").is_ok());
        assert!(validate("1e125").is_ok());
        assert!(validate(&format!("{}e88", "9".repeat(38))).is_ok());

        // One step outside on each end: 1E-131 (order -130) and 1E+126
        // (order 127).
        assert!(validate("1e-131").is_err());
        assert!(validate("1e126").is_err());
        assert!(validate(&format!("{}e89", "9".repeat(38))).is_err());

        // The hostile values from the audit: accepted before, and then the
        // formatter asked for 2^31 zeros.
        let error = validate("1e2147483647").unwrap_err();
        assert!(error.to_string().contains("exponent range"), "{error}");
        assert!(validate("-1e2147483647").is_err());
        assert!(validate("0.5e-9223372036854775808").is_err());
        assert!(validate("10e9223372036854775807").is_err());
    }

    #[test]
    fn exponent_boundary_is_i64_min() {
        // An exponent of `i64::MAX` (or `i64::MIN + 1`) has a negatable
        // counterpart; `i64::MIN` as a scale does not, and such a value
        // cannot be rendered or ordered, so it is not a number.
        let at_limit = Number::from_decimal(&BigDecimal::new(1.into(), -i64::MAX));
        assert!(at_limit.is_some());
        let past_limit = Number::from_decimal(&BigDecimal::new(1.into(), i64::MIN));
        assert!(past_limit.is_none());

        // Through the parser the same boundary sits where the exponent stops
        // fitting an `i64`, on both ends.
        assert!(Number::parse("1e9223372036854775807").is_some());
        assert!(Number::parse("1e9223372036854775808").is_none());
        assert!(Number::parse("1e-9223372036854775807").is_some());
        assert!(Number::parse("1e-9223372036854775808").is_none());

        // The audit's two hostile shapes (issue #58): a fraction length that
        // has to be subtracted from an exponent already at `i64::MIN`, and a
        // mantissa whose normalisation carries `i64::MAX` one step further.
        // Both were raw `i64` additions in the old local parser, so both
        // aborted a debug build instead of returning `None`.
        assert!(Number::parse("0.5e-9223372036854775808").is_none());
        assert!(Number::parse("10e9223372036854775807").is_none());
        // One digit further inside is still a number, so the pair brackets the
        // boundary rather than the whole neighbourhood.
        assert!(Number::parse("0.5e-9223372036854775806").is_some());
        assert!(Number::parse("0.5e-9223372036854775807").is_none());

        // Neither extreme renders, and ordering still holds without touching
        // the padding path.
        let largest = Number::parse("1e9223372036854775807").unwrap();
        let smallest = Number::parse("1e-9223372036854775807").unwrap();
        assert_eq!(largest.to_plain_string(), None);
        assert_eq!(smallest.to_plain_string(), None);
        assert!(largest > smallest);
    }

    #[test]
    fn plain_string_refuses_absurd_padding() {
        // Hostile exponents are refused instead of allocated.
        let huge = Number::parse("1e2147483647").unwrap();
        assert_eq!(huge.to_plain_string(), None);
        let tiny = Number::parse("1e-2147480000").unwrap();
        assert_eq!(tiny.to_plain_string(), None);

        // Boundary pair on the padding limit: 130 zeros render, 131 do not.
        let at_limit = Number::parse("1e130").unwrap();
        assert_eq!(at_limit.to_plain_string().unwrap().len(), 131);
        let past_limit = Number::parse("1e131").unwrap();
        assert_eq!(past_limit.to_plain_string(), None);

        // Same on the fractional side: a scale of 130 pads 129 zeros, 131 is
        // refused.
        let fraction_limit = Number::parse("1e-130").unwrap();
        assert_eq!(
            fraction_limit.to_plain_string().unwrap(),
            format!("0.{}1", "0".repeat(129))
        );
        let fraction_past = Number::parse("1e-131").unwrap();
        assert_eq!(fraction_past.to_plain_string(), None);

        // Everything DynamoDB accepts still renders.
        assert_eq!(
            Number::parse("1e125")
                .unwrap()
                .to_plain_string()
                .unwrap()
                .len(),
            126
        );
    }

    #[test]
    fn arithmetic_survives_extreme_exponents() {
        // `i32::MAX - i32::MIN` does not fit in `i32`: the subtraction used to
        // panic in debug builds before the shift guard could reject the pair.
        let max = Number::parse("1e2147483647").expect("a huge exponent is a number");
        let min = Number::parse("1e-2147483648").expect("a tiny exponent is a number");
        assert!(max.add(&min).is_none());
        assert!(min.add(&max).is_none());
        assert!(max.sub(&min).is_none());
        assert!(min.sub(&max).is_none());
        // Same exponent on both sides needs no alignment at all, so it must
        // simply not panic — and the sum is far outside the storable range, so
        // it is refused for that reason instead (issue #60).
        assert!(max.add(&max).is_none());
        assert!(min.add(&min).is_none());

        // Boundary pair for the alignment guard itself: a gap of 400 is still
        // aligned, 401 is not. `add` cannot show this any more — either sum is
        // outside the storable range and is refused there — so the guard is
        // checked directly.
        let unit = Number::parse("1").unwrap();
        let at_limit = Number::parse("1e400").unwrap();
        assert!(unit.alignment_fits(&at_limit));
        assert!(unit.add(&at_limit).is_none());
        let past_limit = Number::parse("1e401").unwrap();
        assert!(!unit.alignment_fits(&past_limit));
        assert!(unit.add(&past_limit).is_none());
    }

    #[test]
    fn arithmetic_is_exact() {
        let add = |left: &str, right: &str| {
            Number::parse(left)
                .unwrap()
                .add(&Number::parse(right).unwrap())
                .unwrap()
                .to_plain_string()
                .expect("renderable")
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
                .expect("renderable")
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
    fn arithmetic_refuses_results_the_write_path_would_reject() {
        let number = |text: &str| Number::parse(text).expect("a literal");

        // The audit's case (issue #60): both operands are storable, the sum is
        // one decade past the ceiling. It used to come back as `1.8e126`, which
        // `to_plain_string` rendered happily while `validate` rejected the same
        // value.
        assert!(number("9e125").add(&number("9e125")).is_none());
        assert!(number("-9e125").sub(&number("9e125")).is_none());
        assert!(number("9e125").sub(&number("-9e125")).is_none());
        // The boundary pair around it: one step lower the sum is storable …
        assert_eq!(
            number("8e125")
                .add(&number("1e125"))
                .map(|sum| sum.canonical()),
            Some("9E125".to_string())
        );
        // … and exactly at the ceiling too, since `9.99…E+125` is storable.
        assert_eq!(
            number("9e125")
                .sub(&number("1e124"))
                .map(|difference| difference.canonical()),
            Some("89E124".to_string())
        );

        // The other limit arithmetic can cross: 38 significant digits are
        // storable, 39 are not, even though the magnitude is unremarkable.
        let at_limit = number("5e18")
            .add(&number("1e-19"))
            .expect("38 significant digits are storable");
        assert_eq!(at_limit.significant_digits(), 38);
        assert!(number("5e18").add(&number("1e-20")).is_none());

        // Whatever `add` and `sub` do return is a value the write path accepts:
        // that agreement is the point of the shared check.
        for (left, right) in [
            ("1", "2"),
            ("8e125", "1e125"),
            ("5e18", "1e-19"),
            ("-1e-129", "1e-129"),
            ("0", "0"),
        ] {
            for sum in [
                number(left).add(&number(right)),
                number(left).sub(&number(right)),
            ]
            .into_iter()
            .flatten()
            {
                let rendered = sum.to_plain_string().expect("a storable value renders");
                assert!(validate(&rendered).is_ok(), "{left} / {right}: {rendered}");
            }
        }
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
        assert_eq!(zero.to_plain_string().as_deref(), Some("5"));
        let sum = Number::parse("0.0")
            .unwrap()
            .add(&Number::parse("0.00").unwrap())
            .unwrap();
        assert_eq!(sum.to_plain_string().as_deref(), Some("0"));
    }

    #[test]
    fn plain_string_round_trips() {
        for text in ["0", "1", "-1", "1.5", "-0.25", "1000000", "0.001"] {
            let number = Number::parse(text).unwrap();
            assert_eq!(number.to_plain_string().as_deref(), Some(text));
        }
    }

    use proptest::prelude::*;

    /// A literal the service accepts: sign, nine digits with the decimal point
    /// somewhere inside them, and an exponent.
    fn decimal_literal() -> impl Strategy<Value = String> {
        (any::<bool>(), 1_u32..1_000_000_000, 0_u32..9, -30_i32..30).prop_map(
            |(negative, mantissa, scale, exponent)| {
                let sign = if negative { "-" } else { "" };
                let digits = format!("{mantissa:09}");
                let split = digits.len() - scale as usize;
                let (integer, fraction) = digits.split_at(split);
                if fraction.is_empty() {
                    format!("{sign}{integer}e{exponent}")
                } else {
                    format!("{sign}{integer}.{fraction}e{exponent}")
                }
            },
        )
    }

    proptest! {
        /// Equality is equality *of value*: any spelling of a number is the
        /// same number, and the canonical form re-parses to it.
        #[test]
        fn canonical_form_identifies_the_value(literal in decimal_literal()) {
            let number = Number::parse(&literal).expect("generated literals are valid");
            let canonical = number.canonical();
            let again = Number::parse(&canonical).expect("the canonical form is a literal");
            prop_assert_eq!(&again, &number, "{} -> {}", literal.as_str(), canonical.as_str());
            prop_assert_eq!(again.significant_digits(), number.significant_digits());
            prop_assert_eq!(again.to_plain_string(), number.to_plain_string());
        }

        /// Trailing zeros in the fraction, and a matching step of the exponent,
        /// must not change the value.
        #[test]
        fn respelling_keeps_the_value(mantissa in 1_u64..1_000_000, scale in 1_u32..10) {
            let plain = format!("{mantissa}");
            let fraction = format!("{mantissa}.{}", "0".repeat(scale as usize));
            let scaled = format!("{mantissa}{}e-{}", "0".repeat(scale as usize), scale);
            let plain = Number::parse(&plain).expect("plain literal");
            let with_zeros = Number::parse(&fraction).expect("padded literal");
            let with_exponent = Number::parse(&scaled).expect("scaled literal");
            prop_assert_eq!(&with_zeros, &plain);
            prop_assert_eq!(&with_exponent, &plain);
        }

        /// The ordering of integers is the ordering of their values - `f64` is
        /// exact for `i32`, so the float comparison is an independent oracle.
        #[test]
        fn integer_ordering_matches_the_value(left in 0_i32..i32::MAX, right in 0_i32..i32::MAX) {
            let ordering = Number::parse(&left.to_string())
                .expect("left parses")
                .cmp(&Number::parse(&right.to_string()).expect("right parses"));
            let expected = f64::from(left)
                .partial_cmp(&f64::from(right))
                .expect("both are finite");
            prop_assert_eq!(ordering, expected);
        }

        /// Every exponent the wire can carry, with and without a fraction, is
        /// decided without overflow: `parse` either refuses the literal or
        /// returns a value that round-trips through its canonical form, and
        /// `validate` accepts exactly the documented range. The parser this
        /// replaced did the normalisation in raw `i64` arithmetic, so literals
        /// like `0.5e-9223372036854775808` aborted a debug build instead of
        /// being refused (issue #58).
        #[test]
        fn hostile_exponents_are_decided_without_overflow(
            mantissa in 1_u64..1_000_000_000,
            zeros in 0_usize..40,
            exponent in any::<i64>(),
        ) {
            let literal = if zeros == 0 {
                format!("{mantissa}e{exponent}")
            } else {
                format!("{mantissa}.{}e{exponent}", "0".repeat(zeros))
            };
            let parsed = Number::parse(&literal);
            let in_range = parsed.as_ref().is_some_and(Number::in_documented_range);
            prop_assert_eq!(validate(&literal).is_ok(), in_range, "{}", literal.as_str());
            if let Some(number) = parsed {
                let canonical = number.canonical();
                let reparsed = Number::parse(&canonical);
                prop_assert_eq!(reparsed.as_ref(), Some(&number));
            }
        }

        /// An arithmetic result is either refused or storable: whatever `add`
        /// and `sub` return passes the same `validate` the write path runs, so
        /// no expression can build a number the service would reject
        /// (issue #60).
        #[test]
        fn arithmetic_results_are_storable_or_refused(
            left in decimal_literal(),
            right in decimal_literal(),
        ) {
            let left = Number::parse(&left).expect("generated literals are valid");
            let right = Number::parse(&right).expect("generated literals are valid");
            for result in [left.add(&right), left.sub(&right)].into_iter().flatten() {
                prop_assert_eq!(result.storability(), Storability::Storable);
                let rendered = result.to_plain_string().expect("a storable value renders");
                prop_assert!(validate(&rendered).is_ok(), "{}", rendered.as_str());
            }
        }
    }
}
