//! An `Internal` failure that came from a lower layer keeps that failure, so a
//! caller can walk from the rendered message to the root cause (issue #13).

use std::error::Error as _;
use std::fmt;
use std::io;

use mock_ddb::Error;
use proptest::prelude::*;

/// A stand-in for a lower-layer failure that has its own cause.
#[derive(Debug)]
struct Wrapped {
    what: String,
    cause: io::Error,
}

impl fmt::Display for Wrapped {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.what)
    }
}

impl std::error::Error for Wrapped {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

/// A leaf failure whose `Display` is exactly its payload, so the rendered-cause
/// comparison can be driven from a test.
#[derive(Debug)]
struct Leaf(String);

impl fmt::Display for Leaf {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for Leaf {}

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn error_stays_send_and_sync() {
    assert_send_sync::<Error>();
}

#[test]
fn internal_without_a_cause_ends_the_chain() {
    let error = Error::internal("bug");
    assert_eq!(error.code(), "InternalServerError");
    assert_eq!(error.to_string(), "bug");
    assert!(error.source().is_none());
    assert!(!error.is_retryable());
}

#[test]
fn internal_from_keeps_the_cause_out_of_the_message() {
    let error = Error::internal_from("stored count is not an integer", Leaf("bad digits".into()));
    assert_eq!(error.code(), "InternalServerError");
    assert_eq!(error.to_string(), "stored count is not an integer");
    let source = error.source().expect("the cause is kept");
    assert_eq!(source.to_string(), "bad digits");
    assert!(
        source.source().is_none(),
        "the leaf is the end of the chain"
    );
}

#[test]
fn the_chain_is_walked_to_the_deepest_cause() {
    let error = Error::internal_from(
        "stream record could not be rebuilt",
        Wrapped {
            what: "attribute value is not UTF-8".into(),
            cause: io::Error::new(io::ErrorKind::InvalidData, "stream did not contain UTF-8"),
        },
    );

    let middle = error.source().expect("the wrapper is kept");
    assert_eq!(middle.to_string(), "attribute value is not UTF-8");
    let root = middle.source().expect("the io failure is kept");
    assert_eq!(root.to_string(), "stream did not contain UTF-8");
    assert!(root.source().is_none());
}

#[test]
fn cloning_keeps_the_cause() {
    let error = Error::internal_from("bug", Leaf("cause".into()));
    let clone = error.clone();
    assert_eq!(clone.to_string(), error.to_string());
    assert_eq!(
        clone.source().map(ToString::to_string),
        Some("cause".to_string())
    );
}

#[test]
fn equality_covers_class_details_and_cause() {
    let base = Error::internal_from("bug", Leaf("cause".into()));

    // Same class, same message, same rendered cause.
    assert_eq!(base, Error::internal_from("bug", Leaf("cause".into())));
    // Just outside: the cause text differs.
    assert_ne!(
        base,
        Error::internal_from("bug", Leaf("other cause".into()))
    );
    // Just outside: one has a cause, the other does not.
    assert_ne!(base, Error::internal("bug"));
    // The remaining variants keep their old, structural equality.
    assert_eq!(Error::validation("bad"), Error::validation("bad"));
    assert_ne!(Error::validation("bad"), Error::validation("worse"));
    assert_ne!(Error::validation("bad"), Error::internal("bad"));
    assert_eq!(
        Error::table_not_found("users"),
        Error::table_not_found("users")
    );
    assert_ne!(
        Error::table_not_found("users"),
        Error::table_not_found("orders")
    );
}

proptest! {
    /// Equality is reflexive over every message, with and without a cause, and
    /// the cause survives cloning.
    #[test]
    fn equality_is_reflexive(message in ".{0,64}", cause in ".{0,64}") {
        let plain = Error::internal(message.clone());
        prop_assert_eq!(&plain, &plain);
        prop_assert_eq!(plain.clone(), Error::internal(message.clone()));
        prop_assert!(plain.source().is_none());

        let caused = Error::internal_from(message.clone(), Leaf(cause.clone()));
        prop_assert_eq!(&caused, &caused);
        prop_assert_eq!(caused.clone(), Error::internal_from(message, Leaf(cause.clone())));
        prop_assert_eq!(caused.source().map(ToString::to_string), Some(cause.clone()));
        // Just outside: a different cause is a different failure.
        prop_assert_ne!(caused, Error::internal_from("other".to_string(), Leaf(cause)));
    }
}
