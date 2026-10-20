//! The query module's layering and its seam.
//!
//! Why this matters (decision 14.26): the query service must be
//! constructible and servable without the aggregate pipeline, so it can run
//! standalone for downstream readers. Core has no archlint; this test reads
//! the module's own sources and fails on any import of the pipeline, the
//! component discovery, the services, handlers, the bus or the DLQ.

const FORBIDDEN: &[&str] = &[
    "crate::orchestration",
    "crate::registration",
    "crate::services",
    "crate::handlers",
    "crate::bus",
    "crate::dlq",
];

#[test]
fn test_query_module_does_not_depend_on_the_aggregate_pipeline() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/query");
    let mut checked = 0;
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        let is_this_test = path.file_name().is_some_and(|n| n == "mod.test.rs");
        if path.extension().is_some_and(|e| e == "rs") && !is_this_test {
            let text = std::fs::read_to_string(&path).unwrap();
            for forbidden in FORBIDDEN {
                assert!(
                    !text.contains(forbidden),
                    "{} depends on {forbidden}",
                    path.display()
                );
            }
            checked += 1;
        }
    }
    assert!(checked >= 2, "no query sources found");
}
