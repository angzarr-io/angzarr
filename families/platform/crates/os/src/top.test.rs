//! The host's tops: unix here; Windows on CI's Windows runner (decision
//! 5); the file-system seam's re-rooting tests cover both.

use std::path::Path;

use super::Top;
use crate::{host, Flavor};

fn take(path: &str) -> Option<Top> {
    Top::take(&mut Path::new(path).components())
}

#[test]
fn test_an_absolute_path_has_a_top_and_a_relative_one_none() {
    let absolute = host().flavor().top();
    let top = take(absolute).expect("the flavor's own top is absolute");
    assert_eq!(top.caller(), Path::new(absolute));
    assert!(take("relative/x").is_none());
    assert!(take("").is_none());
}

#[test]
fn test_taking_the_top_leaves_the_rest() {
    if host().flavor() != Flavor::Unix {
        return;
    }
    let path = Path::new("/a/b");
    let mut components = path.components();
    Top::take(&mut components).unwrap();
    assert_eq!(components.as_path(), Path::new("a/b"));
}

#[test]
fn test_the_top_under_a_root_and_back() {
    if host().flavor() != Flavor::Unix {
        return;
    }
    let root = Path::new("/r/oot");
    assert_eq!(take("/").unwrap().under(root), root);
    let rest = Path::new("a/b");
    let mut components = rest.components();
    let top = Top::from_real(&mut components).unwrap();
    assert_eq!(top.caller(), Path::new("/"));
    assert_eq!(components.as_path(), rest, "nothing taken on unix");
}
