use tonic::codegen::http;
use tonic::metadata::MetadataMap;

use super::*;
use crate::error_info;

const SECRET: &str = "s3cr3t-token-value";

fn with(values: &[&str]) -> MetadataMap {
    let mut metadata = MetadataMap::new();
    for value in values {
        metadata.append("authorization", value.parse().unwrap());
    }
    metadata
}

#[test]
fn the_bearer_is_read_from_metadata_and_never_logged() {
    let found = bearer(&with(&[&format!("Bearer {SECRET}")]))
        .unwrap()
        .expect("a bearer");
    assert_eq!(found.secret(), SECRET);
    // Nothing that can be printed holds the secret.
    assert!(!format!("{found:?}").contains(SECRET));
    assert!(!format!("{found:#?}").contains(SECRET));
    assert_eq!(format!("{found:?}"), "Bearer(<redacted>)");
    // Nor does a refusal of a header that held it.
    for header in [
        format!("Basic {SECRET}"),
        format!("bearerx {SECRET}"),
        SECRET.to_string(),
    ] {
        let error = bearer(&with(&[&header])).unwrap_err();
        let status = Status::from(error);
        for text in [
            format!("{error:?}"),
            error.to_string(),
            status.message().to_string(),
        ] {
            assert!(!text.contains(SECRET), "{text}");
        }
        assert!(!format!("{status:?}").contains(SECRET));
        assert!(!format!("{:?}", error_info(&status)).contains(SECRET));
    }
}

#[test]
fn no_authorization_header_is_no_bearer() {
    assert_eq!(bearer(&MetadataMap::new()), Ok(None));
    let mut other = MetadataMap::new();
    other.insert("x-run", "r1".parse().unwrap());
    assert_eq!(bearer(&other), Ok(None));
}

/// The scheme is read in any case; the secret is what follows the spaces,
/// kept as it is.
#[test]
fn the_scheme_is_case_insensitive_and_the_secret_is_kept_whole() {
    for header in ["Bearer abc", "bearer abc", "BEARER abc", "Bearer    abc"] {
        let found = bearer(&with(&[header])).unwrap().unwrap();
        assert_eq!(found.secret(), "abc", "{header}");
    }
    let found = bearer(&with(&["Bearer a b=c/d+"])).unwrap().unwrap();
    assert_eq!(found.secret(), "a b=c/d+");
}

#[test]
fn a_header_that_is_not_a_bearer_is_refused_by_its_kind() {
    let table = [
        (vec!["Basic abc"], AuthorizationError::NotBearer),
        (vec!["abc"], AuthorizationError::NotBearer),
        (vec!["Bearerx abc"], AuthorizationError::NotBearer),
        (vec!["Bearer"], AuthorizationError::Empty),
        (vec!["Bearer "], AuthorizationError::Empty),
        (vec!["Bearer   "], AuthorizationError::Empty),
        (vec!["Bearer a", "Bearer b"], AuthorizationError::Repeated),
        (vec!["Bearer a", "Basic b"], AuthorizationError::Repeated),
    ];
    for (headers, error) in table {
        assert_eq!(bearer(&with(&headers)), Err(error), "{headers:?}");
    }
}

#[test]
fn a_header_that_is_not_text_is_refused() {
    let mut headers = http::HeaderMap::new();
    headers.insert(
        "authorization",
        http::HeaderValue::from_bytes(b"Bearer caf\xe9").unwrap(),
    );
    let metadata = MetadataMap::from_headers(headers);
    assert_eq!(bearer(&metadata), Err(AuthorizationError::NotText));
}

/// Every malformed credential is UNAUTHENTICATED under its own code.
#[test]
fn a_malformed_credential_is_grpc_unauthenticated_under_its_code() {
    for error in samples() {
        let status = Status::from(error);
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert_eq!(status.message(), error.code());
        assert_eq!(error_info(&status).unwrap().reason, error.code());
    }
}
