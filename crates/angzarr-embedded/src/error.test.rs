//! The kernel's own classification of a status (decisions 14.37, 14.42).

use super::{class, Class};

#[test]
fn the_kernel_classifies_every_status_itself() {
    // Every gRPC status by its wire number, OK (0) to UNAUTHENTICATED (16).
    let expected = [
        (0, Class::BugRetryable),
        (1, Class::BugRetryable),       // CANCELLED
        (2, Class::BugFatal),           // UNKNOWN
        (3, Class::BusinessFatal),      // INVALID_ARGUMENT
        (4, Class::BugRetryable),       // DEADLINE_EXCEEDED
        (5, Class::BusinessFatal),      // NOT_FOUND
        (6, Class::BusinessFatal),      // ALREADY_EXISTS
        (7, Class::BusinessFatal),      // PERMISSION_DENIED
        (8, Class::BugRetryable),       // RESOURCE_EXHAUSTED
        (9, Class::BusinessFatal),      // FAILED_PRECONDITION
        (10, Class::BusinessImmediate), // ABORTED
        (11, Class::BusinessFatal),     // OUT_OF_RANGE
        (12, Class::BugRetryable),      // UNIMPLEMENTED
        (13, Class::BugFatal),          // INTERNAL
        (14, Class::BugRetryable),      // UNAVAILABLE
        (15, Class::BugFatal),          // DATA_LOSS
        (16, Class::BugRetryable),      // UNAUTHENTICATED
        (99, Class::BugRetryable),      // none at all
    ];
    for (code, class_of) in expected {
        assert_eq!(class(code), class_of, "status {code}");
    }
}

/// The chain's limit and an unreachable handler on the wire, under
/// decision 14.49's keys: the limit INTERNAL (a fatal bug) with its
/// `limit`, the unreachable handler UNAVAILABLE (a retryable bug) with its
/// domain and cause.
#[test]
fn the_limits_and_an_unreachable_handler_are_coded_under_their_keys() {
    use std::collections::BTreeMap;

    use angzarr_router::error::GrpcCode;

    use super::KernelError;
    use angzarr_proto::v1::AngzarrReason;
    let stream = || angzarr_proto::v1::Cover {
        domain: "shelf".to_string(),
        ..Default::default()
    };
    let coded = |error: KernelError| {
        error
            .as_coded()
            .map(|c| (c.code, c.extras, c.grpc))
            .expect("one of the coordinator's answers")
    };
    let extras = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    };
    assert_eq!(
        coded(KernelError::ChainTooDeep { limit: 8 }),
        (
            AngzarrReason::ChainTooDeep.as_str_name().to_string(),
            extras(&[("limit", "8")]),
            GrpcCode::Internal
        )
    );
    assert_eq!(
        coded(KernelError::HandlerUnavailable {
            stream: stream(),
            cause: "timeout".to_string()
        }),
        (
            AngzarrReason::HandlerUnavailable.as_str_name().to_string(),
            extras(&[
                ("stream", "shelf/"),
                ("domain", "shelf"),
                ("cause", "timeout")
            ]),
            GrpcCode::Unavailable
        )
    );
}

/// The retryable failures of the store and the upcaster sidecar on the
/// wire, under decision 14.51's keys, each UNAVAILABLE (a retryable bug):
/// a store that could not answer, its failure the `cause`; a sidecar that
/// could not be reached, its domain and cause. A race the store lost is
/// never answered as itself (it is run again, and given up at the race
/// re-run cap as ANGZARR_REASON_RETRY_CAP_EXHAUSTED; decision 14.84), and a
/// store error that is not retryable has no wire answer here.
#[test]
fn the_retryable_store_and_sidecar_failures_are_coded_under_their_keys() {
    use std::collections::BTreeMap;

    use angzarr_diagnostic::Report;
    use angzarr_router::error::GrpcCode;

    use super::{KernelError, UpcastError};
    use angzarr_proto::v1::AngzarrReason;
    let stream = || angzarr_proto::v1::Cover {
        domain: "shelf".to_string(),
        ..Default::default()
    };
    let coded = |error: KernelError| {
        error
            .as_coded()
            .map(|c| (c.code, c.extras, c.grpc))
            .expect("one of the coordinator's answers")
    };
    let extras = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    };
    assert!(
        KernelError::Store(angzarr_ports::StoreError::conflict(stream(), 3, 4))
            .as_coded()
            .is_none(),
        "a lost race is run again, never answered as itself"
    );
    assert_eq!(
        coded(KernelError::Store(angzarr_ports::StoreError::Unavailable(
            angzarr_ports::Cause::new(
                "x-down",
                [(
                    "ext_err_txt",
                    angzarr_ports::Arg::Str("database is locked".to_string())
                )]
            )
        ))),
        (
            AngzarrReason::StoreUnavailable.as_str_name().to_string(),
            extras(&[
                ("cause", "x-down"),
                ("cause.ext_err_txt", "database is locked")
            ]),
            GrpcCode::Unavailable
        )
    );
    assert_eq!(
        coded(KernelError::Upcast {
            stream: stream(),
            error: UpcastError::Unavailable {
                cause: Report::new("x-sidecar-down").arg("port", 50051u64),
            },
        }),
        (
            AngzarrReason::UpcasterUnavailable.as_str_name().to_string(),
            extras(&[
                ("domain", "shelf"),
                ("cause", "x-sidecar-down"),
                ("cause.port", "50051")
            ]),
            GrpcCode::Unavailable
        )
    );
    assert!(
        KernelError::Store(angzarr_ports::StoreError::discontinuous(stream(), 3, 5))
            .as_coded()
            .is_none()
    );
    assert!(KernelError::Upcast {
        stream: stream(),
        error: UpcastError::Rule {
            source: "a".to_string(),
            error: Report::new("x-rule"),
        },
    }
    .as_coded()
    .is_none());
}

/// A kernel error the spec keys is its key, verbatim, in process and on the
/// wire alike, with exactly the args the key declares and the status its
/// key travels with (decision 14.60); every other has no wire answer of the
/// coordinator's, or answers under the coordinator's key for it.
#[test]
fn a_spec_keyed_kernel_error_is_its_key_with_its_declared_args() {
    use angzarr_diagnostic::Diagnostic;

    use super::KernelError;
    let stream = || angzarr_proto::v1::Cover {
        domain: "shelf".to_string(),
        ..Default::default()
    };
    for (error, status) in spec_keyed(stream) {
        let declared = angzarr_proto::reason_key_args(error.code())
            .unwrap_or_else(|| panic!("{} is no spec key", error.code()));
        let given: Vec<&str> = error.args().iter().map(|(n, _)| *n).collect();
        assert_eq!(given, declared, "{error:?}");
        let coded = error.as_coded().expect("a coordinator's answer");
        assert_eq!(coded.code, error.code());
        assert_eq!(coded.grpc, status, "{error:?}");
        let names: Vec<&str> = coded.extras.keys().map(String::as_str).collect();
        let mut sorted = declared.to_vec();
        sorted.sort_unstable();
        assert_eq!(names, sorted, "{error:?}");
    }
    let unkeyed = KernelError::BeforeSnapshot {
        stream: stream(),
        first: 4,
    };
    assert_eq!(angzarr_proto::reason_key_args(unkeyed.code()), None);
    assert!(unkeyed.as_coded().is_none());
}

/// Every kernel error the spec keys, on `stream`, with the status its key
/// travels with.
fn spec_keyed(
    stream: impl Fn() -> angzarr_proto::v1::Cover,
) -> Vec<(super::KernelError, angzarr_router::error::GrpcCode)> {
    use angzarr_router::error::GrpcCode;

    use super::KernelError;
    vec![
        (
            KernelError::RetryCapExhausted {
                stream: stream(),
                attempts: 3,
                cap: 3,
            },
            GrpcCode::Internal,
        ),
        (
            KernelError::RulesVersionMismatch {
                stream: stream(),
                registered: "1".to_string(),
                reported: "2".to_string(),
            },
            GrpcCode::Internal,
        ),
        (
            KernelError::StateTypeMismatch {
                stream: stream(),
                registered: "a.State".to_string(),
                state_type: "a.Other".to_string(),
            },
            GrpcCode::Internal,
        ),
        (
            KernelError::FactDeadLettered {
                stream: stream(),
                cause: angzarr_diagnostic::Report::new("x-bad"),
            },
            GrpcCode::Internal,
        ),
        (KernelError::ChainTooDeep { limit: 8 }, GrpcCode::Internal),
        (
            KernelError::HandlerUnavailable {
                stream: stream(),
                cause: "not-running".to_string(),
            },
            GrpcCode::Unavailable,
        ),
    ]
}

/// Every store error a step's reads and write, or an open migrating the
/// store, can meet is among the crate's diagnostic samples, forwarded
/// under its angzarr-ports code (`KernelError::Store`), so the catalog
/// check holds each forwarded code to angzarr-ports' messages.
#[test]
fn every_forwarded_store_error_is_sampled() {
    let sampled: Vec<String> = crate::diagnostic_samples()
        .iter()
        .map(|d| d.code().to_string())
        .collect();
    use angzarr_ports::{Cause, StoreError};
    let stream = || Box::new(angzarr_proto::v1::Cover::default());
    let cause = || Cause::new("x-cause", []);
    for error in [
        StoreError::Conflict {
            stream: stream(),
            expected: 1,
            actual: 2,
        },
        StoreError::Discontinuous {
            stream: stream(),
            expected: 1,
            found: 2,
        },
        StoreError::MalformedStream { stream: stream() },
        StoreError::Unavailable(cause()),
        StoreError::Unreadable {
            stream: stream(),
            sequence: 1,
            reason: cause(),
        },
        StoreError::EditionNotForked { stream: stream() },
        StoreError::EditionNotFound {
            name: "what-if".to_string(),
        },
        StoreError::NewerThanBuild {
            applied: 2,
            build: 1,
        },
        StoreError::MigrationFailed { version: 1 },
        StoreError::MigrationChanged { version: 1 },
        StoreError::MigrationMissing { version: 1 },
        StoreError::MigrationUnknown { version: 1 },
        StoreError::MigrationIrreversible { version: 1 },
        StoreError::BackupExists {
            target: "state.db.backup".to_string(),
        },
    ] {
        let code = error.code();
        assert!(sampled.iter().any(|c| c == code), "{code} is not sampled");
    }
}
