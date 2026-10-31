//! The embedded protos are the repository's, byte for byte, and written
//! out they form an include directory protoc can import from.

use std::path::PathBuf;

use angzarr_proto::protos::{write_include_dir, FILES};

fn repo_proto() -> PathBuf {
    std::env::var("ANGZARR_PROJECT_PROTO")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto"))
}

#[test]
fn every_embedded_proto_is_the_repositorys() {
    let paths: Vec<_> = FILES.iter().map(|f| f.path).collect();
    assert!(paths.contains(&"io/angzarr/v1/options.proto"));
    assert!(paths.contains(&"io/angzarr/v1/types.proto"));
    for file in FILES {
        let on_disk = std::fs::read_to_string(repo_proto().join(file.path)).unwrap();
        assert_eq!(file.contents, on_disk, "{}", file.path);
    }
}

#[test]
fn written_out_they_are_an_include_directory() {
    let dir = std::env::temp_dir().join(format!("angzarr-protos-{}", std::process::id()));
    let include = write_include_dir(&dir).unwrap();
    assert_eq!(include, dir);
    for file in FILES {
        assert_eq!(
            std::fs::read_to_string(include.join(file.path)).unwrap(),
            file.contents
        );
    }
    // Every import between them resolves inside the directory
    // (google/protobuf/* aside, which protoc supplies; google/api/* is
    // embedded, for meta.proto).
    for file in FILES {
        for line in file.contents.lines() {
            let Some(rest) = line.trim().strip_prefix("import \"") else {
                continue;
            };
            let import = rest.trim_end_matches("\";");
            if !import.starts_with("google/protobuf/") {
                assert!(
                    include.join(import).exists(),
                    "{} imports {import}",
                    file.path
                );
            }
        }
    }
    std::fs::remove_dir_all(&dir).unwrap();
}
