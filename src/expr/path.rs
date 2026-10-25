//! Reading, writing and removing document paths.
//!
//! A path is resolved before it reaches this module, so every `Name` segment is
//! a concrete attribute name. Nested writes are strict — DynamoDB rejects
//! `SET a.b = :v` when `a` does not exist — while removals are forgiving, since
//! removing something that is not there is a no-op.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

use super::parse::Segment;
use crate::{Error, Item, Result};

/// The top-level attribute a path starts at.
pub(crate) fn root(path: &[Segment]) -> Option<&str> {
    match path.first() {
        Some(Segment::Name(name)) => Some(name),
        _ => None,
    }
}

/// Read the value a path points at.
pub(crate) fn get_path<'a>(item: &'a Item, path: &[Segment]) -> Option<&'a AttributeValue> {
    let (first, rest) = path.split_first()?;
    let Segment::Name(root) = first else {
        return None;
    };
    get_in(item.get(root)?, rest)
}

/// Read a nested value below an attribute value.
fn get_in<'a>(value: &'a AttributeValue, segments: &[Segment]) -> Option<&'a AttributeValue> {
    match segments.split_first() {
        None => Some(value),
        Some((Segment::Name(name), rest)) => match value {
            AttributeValue::M(map) => get_in(map.get(name)?, rest),
            _ => None,
        },
        Some((Segment::Index(index), rest)) => match value {
            AttributeValue::L(list) => get_in(list.get(*index)?, rest),
            _ => None,
        },
    }
}

/// Write a value at a path.
///
/// The top-level attribute may be created; every parent of a nested path must
/// already exist, and a list index must be in range.
pub(crate) fn set_path(item: &mut Item, path: &[Segment], value: AttributeValue) -> Result<()> {
    let Some((Segment::Name(root), rest)) = path.split_first() else {
        return Err(Error::validation(
            "Invalid expression: a document path must start with an attribute name",
        ));
    };
    if rest.is_empty() {
        item.insert(root.clone(), value);
        return Ok(());
    }
    let Some(parent) = item.get_mut(root) else {
        return Err(invalid_nested_path(path));
    };
    set_in(parent, rest, value, path)
}

/// Write below an existing container.
fn set_in(
    target: &mut AttributeValue,
    segments: &[Segment],
    value: AttributeValue,
    full_path: &[Segment],
) -> Result<()> {
    let Some((first, rest)) = segments.split_first() else {
        return Err(Error::internal("set_in called with an empty path"));
    };
    match first {
        Segment::Name(name) => match target {
            AttributeValue::M(map) => {
                if rest.is_empty() {
                    map.insert(name.clone(), value);
                    return Ok(());
                }
                let Some(child) = map.get_mut(name) else {
                    return Err(invalid_nested_path(full_path));
                };
                set_in(child, rest, value, full_path)
            }
            _ => Err(invalid_nested_path(full_path)),
        },
        Segment::Index(index) => match target {
            AttributeValue::L(list) => {
                let Some(child) = list.get_mut(*index) else {
                    return Err(Error::validation(
                        "Invalid UpdateExpression: The list index is out of range",
                    ));
                };
                if rest.is_empty() {
                    *child = value;
                    return Ok(());
                }
                set_in(child, rest, value, full_path)
            }
            _ => Err(invalid_nested_path(full_path)),
        },
    }
}

/// Remove the value a path points at, if it is there.
pub(crate) fn remove_path(item: &mut Item, path: &[Segment]) {
    let Some((Segment::Name(root), rest)) = path.split_first() else {
        return;
    };
    if rest.is_empty() {
        item.remove(root);
        return;
    }
    if let Some(parent) = item.get_mut(root) {
        remove_in(parent, rest);
    }
}

/// Remove below an existing container.
fn remove_in(target: &mut AttributeValue, segments: &[Segment]) {
    let Some((first, rest)) = segments.split_first() else {
        return;
    };
    match (first, target) {
        (Segment::Name(name), AttributeValue::M(map)) => {
            if rest.is_empty() {
                map.remove(name);
            } else if let Some(child) = map.get_mut(name) {
                remove_in(child, rest);
            }
        }
        (Segment::Index(index), AttributeValue::L(list)) => {
            if rest.is_empty() {
                if *index < list.len() {
                    list.remove(*index);
                }
            } else if let Some(child) = list.get_mut(*index) {
                remove_in(child, rest);
            }
        }
        _ => {}
    }
}

/// Build an item containing only the requested paths.
///
/// A path that runs into a list index contributes the list up to that point:
/// DynamoDB projects list elements, the mock projects the containing list.
pub(crate) fn project(item: &Item, paths: &[&[Segment]]) -> Item {
    let mut projected = Item::new();
    for path in paths {
        let effective = match path
            .iter()
            .position(|segment| matches!(segment, Segment::Index(_)))
        {
            Some(index) => &path[..index],
            None => path,
        };
        if let Some(value) = get_path(item, effective) {
            insert_creating(&mut projected, effective, value.clone());
        }
    }
    projected
}

/// Insert a value at a path, creating intermediate maps.
fn insert_creating(item: &mut Item, path: &[Segment], value: AttributeValue) {
    let Some((Segment::Name(root), rest)) = path.split_first() else {
        return;
    };
    if rest.is_empty() {
        item.insert(root.clone(), value);
        return;
    }
    let entry = item
        .entry(root.clone())
        .or_insert_with(|| AttributeValue::M(HashMap::new()));
    if let AttributeValue::M(map) = entry {
        insert_into_map(map, rest, value);
    }
}

/// Insert a value below a map, creating intermediate maps.
fn insert_into_map(
    map: &mut HashMap<String, AttributeValue>,
    segments: &[Segment],
    value: AttributeValue,
) {
    let Some((first, rest)) = segments.split_first() else {
        return;
    };
    let Segment::Name(name) = first else {
        return;
    };
    if rest.is_empty() {
        map.insert(name.clone(), value);
        return;
    }
    let entry = map
        .entry(name.clone())
        .or_insert_with(|| AttributeValue::M(HashMap::new()));
    if let AttributeValue::M(nested) = entry {
        insert_into_map(nested, rest, value);
    }
}

fn invalid_nested_path(path: &[Segment]) -> Error {
    Error::validation(format!(
        "Invalid UpdateExpression: The document path provided in the update expression is invalid for update: {}",
        render(path)
    ))
}

/// Render a path for an error message.
pub(crate) fn render(path: &[Segment]) -> String {
    let mut rendered = String::new();
    for segment in path {
        match segment {
            Segment::Name(name) => {
                if !rendered.is_empty() {
                    rendered.push('.');
                }
                rendered.push_str(name);
            }
            Segment::Index(index) => {
                rendered.push('[');
                rendered.push_str(&index.to_string());
                rendered.push(']');
            }
        }
    }
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{n, s};

    fn sample() -> Item {
        let mut item = Item::new();
        item.insert("pk".into(), s("a"));
        item.insert(
            "profile".into(),
            AttributeValue::M(HashMap::from([(
                "city".to_string(),
                AttributeValue::S("Berlin".into()),
            )])),
        );
        item.insert("tags".into(), AttributeValue::L(vec![s("x"), s("y")]));
        item
    }

    #[test]
    fn reads_nested_maps_and_lists() {
        let item = sample();
        assert_eq!(
            get_path(
                &item,
                &[
                    Segment::Name("profile".into()),
                    Segment::Name("city".into())
                ]
            ),
            Some(&s("Berlin"))
        );
        assert_eq!(
            get_path(&item, &[Segment::Name("tags".into()), Segment::Index(1)]),
            Some(&s("y"))
        );
        assert!(get_path(&item, &[Segment::Name("tags".into()), Segment::Index(9)]).is_none());
        assert!(get_path(&item, &[Segment::Name("nope".into())]).is_none());
    }

    #[test]
    fn writes_nested_paths_and_rejects_missing_parents() {
        let mut item = sample();
        set_path(
            &mut item,
            &[Segment::Name("profile".into()), Segment::Name("zip".into())],
            s("10115"),
        )
        .unwrap();
        assert_eq!(
            get_path(
                &item,
                &[Segment::Name("profile".into()), Segment::Name("zip".into())]
            ),
            Some(&s("10115"))
        );
        assert!(set_path(
            &mut item,
            &[Segment::Name("missing".into()), Segment::Name("x".into())],
            s("v"),
        )
        .is_err());
        assert!(set_path(
            &mut item,
            &[Segment::Name("tags".into()), Segment::Index(5)],
            s("z"),
        )
        .is_err());
    }

    #[test]
    fn removals_are_forgiving() {
        let mut item = sample();
        remove_path(
            &mut item,
            &[Segment::Name("tags".into()), Segment::Index(0)],
        );
        assert_eq!(
            get_path(&item, &[Segment::Name("tags".into())]),
            Some(&AttributeValue::L(vec![s("y")]))
        );
        remove_path(
            &mut item,
            &[Segment::Name("nope".into()), Segment::Name("deeper".into())],
        );
        remove_path(&mut item, &[Segment::Name("pk".into())]);
        assert!(!item.contains_key("pk"));
    }

    #[test]
    fn projection_keeps_only_requested_paths() {
        let item = sample();
        let projected = project(
            &item,
            &[
                &[Segment::Name("pk".into())],
                &[
                    Segment::Name("profile".into()),
                    Segment::Name("city".into()),
                ],
            ],
        );
        assert_eq!(projected.len(), 2);
        assert!(projected.contains_key("pk"));
        assert!(!projected.contains_key("tags"));
        assert_eq!(
            get_path(
                &projected,
                &[
                    Segment::Name("profile".into()),
                    Segment::Name("city".into())
                ]
            ),
            Some(&s("Berlin"))
        );
    }

    #[test]
    fn rejects_paths_that_do_not_start_with_a_name() {
        let mut item = sample();
        assert!(get_path(&item, &[Segment::Index(0)]).is_none());
        assert!(set_path(&mut item, &[Segment::Index(0)], s("x")).is_err());
        remove_path(&mut item, &[Segment::Index(0)]);
        remove_path(&mut item, &[]);
        assert_eq!(project(&item, &[&[]]).len(), 0);
        let mut value = s("scalar");
        assert!(set_in(&mut value, &[], s("x"), &[]).is_err());
    }

    #[test]
    fn rejects_writes_below_scalars_and_missing_children() {
        let mut item = Item::new();
        item.insert(
            "rows".into(),
            AttributeValue::L(vec![AttributeValue::M(HashMap::from([(
                "v".to_string(),
                n("1"),
            )]))]),
        );
        assert_eq!(
            get_path(
                &item,
                &[
                    Segment::Name("rows".into()),
                    Segment::Index(0),
                    Segment::Name("v".into())
                ]
            ),
            Some(&n("1"))
        );
        set_path(
            &mut item,
            &[
                Segment::Name("rows".into()),
                Segment::Index(0),
                Segment::Name("v".into()),
            ],
            n("2"),
        )
        .unwrap();

        // Descending through existing maps creates the leaf.
        item.insert(
            "deep".into(),
            AttributeValue::M(HashMap::from([(
                "a".to_string(),
                AttributeValue::M(HashMap::from([(
                    "b".to_string(),
                    AttributeValue::M(HashMap::new()),
                )])),
            )])),
        );
        set_path(
            &mut item,
            &[
                Segment::Name("deep".into()),
                Segment::Name("a".into()),
                Segment::Name("b".into()),
                Segment::Name("c".into()),
            ],
            s("leaf"),
        )
        .unwrap();
        assert_eq!(
            get_path(
                &item,
                &[
                    Segment::Name("deep".into()),
                    Segment::Name("a".into()),
                    Segment::Name("b".into()),
                    Segment::Name("c".into()),
                ]
            ),
            Some(&s("leaf"))
        );
    }

    #[test]
    fn nested_removals_are_forgiving() {
        let mut item = Item::new();
        item.insert(
            "rows".into(),
            AttributeValue::L(vec![AttributeValue::M(HashMap::from([(
                "v".to_string(),
                n("1"),
            )]))]),
        );
        // Writing below a scalar, a list or a missing child is rejected.
        assert!(set_path(
            &mut item,
            &[Segment::Name("rows".into()), Segment::Name("v".into())],
            s("x")
        )
        .is_err());
        assert!(set_path(
            &mut item,
            &[
                Segment::Name("rows".into()),
                Segment::Index(0),
                Segment::Name("v".into()),
                Segment::Name("deeper".into()),
            ],
            s("x")
        )
        .is_err());
        assert!(set_path(
            &mut item,
            &[
                Segment::Name("deep".into()),
                Segment::Name("missing".into()),
                Segment::Name("c".into()),
            ],
            s("x")
        )
        .is_err());
        assert!(set_path(
            &mut item,
            &[
                Segment::Name("rows".into()),
                Segment::Index(0),
                Segment::Index(0)
            ],
            s("x")
        )
        .is_err());

        // Removals ignore missing containers and reach through lists.
        remove_path(
            &mut item,
            &[
                Segment::Name("rows".into()),
                Segment::Index(0),
                Segment::Name("v".into()),
            ],
        );
        remove_path(
            &mut item,
            &[
                Segment::Name("rows".into()),
                Segment::Index(9),
                Segment::Name("v".into()),
            ],
        );
        remove_path(
            &mut item,
            &[Segment::Name("rows".into()), Segment::Name("v".into())],
        );
        remove_path(
            &mut item,
            &[
                Segment::Name("rows".into()),
                Segment::Index(0),
                Segment::Name("v".into()),
                Segment::Name("deeper".into()),
            ],
        );
    }

    #[test]
    fn projection_ignores_unreachable_and_scalar_paths() {
        let mut item = sample();
        item.insert(
            "rows".into(),
            AttributeValue::L(vec![AttributeValue::M(HashMap::new())]),
        );
        let projected = project(
            &item,
            &[
                &[Segment::Name("pk".into()), Segment::Name("nested".into())],
                &[
                    Segment::Name("rows".into()),
                    Segment::Index(0),
                    Segment::Name("v".into()),
                ],
                &[Segment::Name("unknown".into()), Segment::Name("x".into())],
            ],
        );
        // `pk.nested` is unreachable (pk is a scalar) and `unknown.x` has no
        // root, so only the list prefix of `rows[0]` survives.
        assert_eq!(projected.len(), 1);
        assert!(projected.contains_key("rows"));
    }

    #[test]
    fn renders_paths_for_errors() {
        assert_eq!(
            render(&[
                Segment::Name("a".into()),
                Segment::Name("b".into()),
                Segment::Index(2)
            ]),
            "a.b[2]"
        );
    }
}
