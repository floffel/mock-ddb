//! Reading, writing and removing document paths.
//!
//! A path is resolved before it reaches this module, so every `Name` segment is
//! a concrete attribute name. Nested writes are strict — DynamoDB rejects
//! `SET a.b = :v` when `a` does not exist — while removals are forgiving, since
//! removing something that is not there is a no-op.

use std::collections::{BTreeMap, HashMap};

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

/// The most document paths one projection may name.
///
/// The service bounds the expression *text* (4 KB); a mock that held no bound
/// of its own would let one request select an unbounded number of branches
/// from a single stored item while the projection is built. A hundred paths is
/// far more than a real projection names and keeps that work small.
pub(crate) const MAX_PROJECTION_PATHS: usize = 100;

/// Reject a projection that names more paths than [`MAX_PROJECTION_PATHS`].
///
/// The count is checked before an item is read or anything is cloned, so an
/// oversized projection costs one comparison per path and no allocation
/// against the stored value.
pub(crate) fn check_path_count(count: usize) -> Result<()> {
    if count > MAX_PROJECTION_PATHS {
        return Err(Error::validation(format!(
            "Invalid ProjectionExpression: The expression names {count} document paths; at most {MAX_PROJECTION_PATHS} are accepted"
        )));
    }
    Ok(())
}

/// Build an item containing only the requested paths.
///
/// Every path is followed to its end: a map keeps only the requested keys, and a
/// list is rebuilt from the requested indices in ascending order and without
/// duplicates — the shape the service returns for `items[0]`, not the containing
/// list. Indices the item does not have, keys it does not carry and paths that
/// run through a scalar contribute nothing.
///
/// The paths are folded into one tree first, so each source attribute is read
/// once however many paths reach into it and only the selected leaves are
/// cloned. Callers reject a longer list than [`MAX_PROJECTION_PATHS`] before
/// calling this (`check_path_count`).
pub(crate) fn project(item: &Item, paths: &[&[Segment]]) -> Item {
    let shape = projection_shape(paths);
    let mut projected = Item::new();
    for (name, branch) in &shape.keys {
        if let Some(value) = item
            .get(name)
            .and_then(|value| project_value(value, branch))
        {
            projected.insert(name.clone(), value);
        }
    }
    projected
}

/// The branches of a document a projection keeps.
#[derive(Debug, Default)]
struct Shape {
    /// Keep the value at this level whole.
    whole: bool,
    /// Keys to keep below a map.
    keys: BTreeMap<String, Shape>,
    /// Indices to keep below a list.
    indices: BTreeMap<usize, Shape>,
}

/// Fold the requested paths into one shape tree.
///
/// Paths that do not start with a name are ignored, and a path that names a
/// value whole wins over a path that reaches into it.
fn projection_shape(paths: &[&[Segment]]) -> Shape {
    fn merge(node: &mut Shape, path: &[Segment]) {
        match path.split_first() {
            None => node.whole = true,
            Some(_) if node.whole => {}
            Some((Segment::Name(name), rest)) => {
                merge(node.keys.entry(name.clone()).or_default(), rest);
            }
            Some((Segment::Index(index), rest)) => {
                merge(node.indices.entry(*index).or_default(), rest);
            }
        }
    }

    let mut root = Shape::default();
    for path in paths {
        if matches!(path.first(), Some(Segment::Name(_))) {
            merge(&mut root, path);
        }
    }
    root
}

/// Keep the requested branches of a value, or drop the value entirely when it
/// carries none of them.
fn project_value(value: &AttributeValue, shape: &Shape) -> Option<AttributeValue> {
    if shape.whole {
        return Some(value.clone());
    }
    match value {
        AttributeValue::M(map) => {
            let mut kept = HashMap::new();
            for (name, branch) in &shape.keys {
                if let Some(child) = map.get(name).and_then(|child| project_value(child, branch)) {
                    kept.insert(name.clone(), child);
                }
            }
            (!kept.is_empty()).then_some(AttributeValue::M(kept))
        }
        AttributeValue::L(list) => {
            let mut kept = Vec::new();
            for (index, branch) in &shape.indices {
                if let Some(child) = list
                    .get(*index)
                    .and_then(|element| project_value(element, branch))
                {
                    kept.push(child);
                }
            }
            (!kept.is_empty()).then_some(AttributeValue::L(kept))
        }
        _ => None,
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

    fn name(value: &str) -> Segment {
        Segment::Name(value.into())
    }

    fn index(value: usize) -> Segment {
        Segment::Index(value)
    }

    fn element(fields: &[(&str, &str)]) -> AttributeValue {
        AttributeValue::M(
            fields
                .iter()
                .map(|(key, value)| ((*key).to_string(), n(*value)))
                .collect(),
        )
    }

    /// `rows` holds two maps: `{v: 1, w: 9}` at index 0 and `{v: 2, w: 8}` at 1.
    fn rows() -> Item {
        let mut item = sample();
        item.insert(
            "rows".into(),
            AttributeValue::L(vec![
                element(&[("v", "1"), ("w", "9")]),
                element(&[("v", "2"), ("w", "8")]),
            ]),
        );
        item
    }

    #[test]
    fn projection_descends_into_list_indices() {
        let projected = project(&rows(), &[&[name("rows"), index(0), name("v")]]);
        assert_eq!(
            projected.get("rows"),
            Some(&AttributeValue::L(vec![element(&[("v", "1")])]))
        );
    }

    #[test]
    fn projection_orders_and_deduplicates_list_indices() {
        // Requested 1 before 0, and 1 twice: ascending, once.
        let projected = project(
            &rows(),
            &[
                &[name("rows"), index(1), name("v")],
                &[name("rows"), index(0), name("v")],
                &[name("rows"), index(1), name("v")],
            ],
        );
        assert_eq!(
            projected.get("rows"),
            Some(&AttributeValue::L(vec![
                element(&[("v", "1")]),
                element(&[("v", "2")]),
            ]))
        );
    }

    #[test]
    fn projection_keeps_a_whole_element_and_merges_shared_prefixes() {
        let whole = project(&rows(), &[&[name("rows"), index(1)]]);
        assert_eq!(
            whole.get("rows"),
            Some(&AttributeValue::L(vec![element(&[("v", "2"), ("w", "8")])]))
        );

        let merged = project(
            &rows(),
            &[
                &[name("rows"), index(0), name("w")],
                &[name("rows"), index(0), name("v")],
            ],
        );
        assert_eq!(
            merged.get("rows"),
            Some(&AttributeValue::L(vec![element(&[("w", "9"), ("v", "1")])]))
        );
    }

    #[test]
    fn projection_keeps_indices_inside_the_list_and_drops_one_past_the_end() {
        // Two elements: index 1 is the last one, index 2 is one past it.
        assert!(project(&rows(), &[&[name("rows"), index(1), name("v")]]).contains_key("rows"));
        assert!(project(&rows(), &[&[name("rows"), index(2), name("v")]]).is_empty());
    }

    #[test]
    fn projection_follows_a_path_through_nested_lists() {
        let mut item = Item::new();
        item.insert(
            "a".into(),
            AttributeValue::M(HashMap::from([(
                "b".to_string(),
                AttributeValue::L(vec![
                    AttributeValue::M(HashMap::new()),
                    AttributeValue::M(HashMap::from([(
                        "c".to_string(),
                        AttributeValue::L(vec![element(&[("keep", "1"), ("drop", "2")])]),
                    )])),
                ]),
            )])),
        );

        let projected = project(
            &item,
            &[
                &[
                    name("a"),
                    name("b"),
                    index(1),
                    name("c"),
                    index(0),
                    name("keep"),
                ],
                // The map at index 0 of `b` has no `c`, so it contributes nothing.
                &[
                    name("a"),
                    name("b"),
                    index(0),
                    name("c"),
                    index(0),
                    name("keep"),
                ],
            ],
        );
        assert_eq!(
            get_path(
                &projected,
                &[
                    name("a"),
                    name("b"),
                    index(0),
                    name("c"),
                    index(0),
                    name("keep")
                ]
            ),
            Some(&n("1"))
        );
        assert_eq!(
            projected
                .get("a")
                .and_then(|value| value.as_m().ok())
                .and_then(|map| map.get("b"))
                .and_then(|value| value.as_l().ok())
                .map(Vec::len),
            Some(1)
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
                &[name("pk"), name("nested")],
                &[name("rows"), index(0), name("v")],
                &[name("unknown"), name("x")],
            ],
        );
        // `pk.nested` is unreachable (pk is a scalar), `unknown.x` has no root
        // and the element at `rows[0]` does not carry `v`.
        assert!(projected.is_empty());
    }

    #[test]
    fn projection_takes_the_branch_the_value_actually_has() {
        // `rows` is a list, so the map branch of the second path is ignored.
        let projected = project(
            &rows(),
            &[
                &[name("rows"), name("v")],
                &[name("rows"), index(1), name("v")],
            ],
        );
        assert_eq!(
            projected.get("rows"),
            Some(&AttributeValue::L(vec![element(&[("v", "2")])]))
        );
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

    #[test]
    fn rejects_a_projection_above_the_path_cap() {
        // Just inside and just outside the cap.
        assert!(check_path_count(MAX_PROJECTION_PATHS).is_ok());
        let error = check_path_count(MAX_PROJECTION_PATHS + 1).unwrap_err();
        assert_eq!(error.code(), "ValidationException");
        assert!(
            error.to_string().contains("at most 100"),
            "unexpected message: {error}"
        );
        // A path list that is short by many still passes: the cap is a
        // maximum, not an exact count.
        assert!(check_path_count(1).is_ok());
        assert!(check_path_count(0).is_ok());
    }
}
