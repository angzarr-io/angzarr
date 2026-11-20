use prost_types::Any;

use crate::v1::EventPage;
use angzarr_diagnostic::code;

/// A fold that fails on a persisted event says why as its cause, a key and
/// args (decision 14.37: `FoldFailure.cause`, flattened as `cause` and
/// `cause.<arg>`), never as text: here the kernel's own payload error for
/// bytes that do not decode as the event's type.
#[test]
fn a_fold_that_fails_carries_its_cause_as_a_key_and_args() {
    use crate::toys::{account, command, cover, registry, Ready};
    use angzarr_proto::v1::AngzarrReason;
    use angzarr_router::hpb::HandleRequest;

    let registry = registry();
    let stream = cover("account", 1);
    let mut corrupt = crate::pack(&account::Deposited { cents: 1 });
    corrupt.value = vec![0xff, 0xff, 0xff];
    let type_url = corrupt.type_url.clone();
    let request = HandleRequest {
        history: Some(crate::v1::EventBook {
            cover: Some(stream.clone()),
            pages: vec![EventPage {
                sequence: 0,
                body: Some(crate::v1::event_page::Body::Event(corrupt)),
                meta: None,
            }],
            snapshot: None,
            next_sequence: 1,
        }),
        input: Some(command(
            stream,
            crate::pack(&account::cmd::Deposit { cents: 1 }),
        )),
        ..HandleRequest::default()
    };
    let error = registry
        .handler("account")
        .expect("registered")
        .dispatch(&request)
        .ready()
        .expect_err("the persisted event does not fold");
    assert_eq!(error.code, AngzarrReason::FoldFailed.as_str_name());
    let extra = |key: &str| error.extras.get(key).map(String::as_str);
    assert_eq!(extra("cause"), Some(code!("payload-decode")), "{error:?}");
    assert_eq!(
        extra("cause.type_url"),
        Some(type_url.as_str()),
        "{error:?}"
    );
}

/// A handler's typed refusal goes out under whichever of the six business
/// statuses it gave, keyed ANGZARR_REASON_COMMAND_REFUSED with its object
/// beside, and `Rejected` reads that same status and object back. A
/// handler's own key under a business status is no refusal (decision
/// 14.95): no `Rejected` reads it.
#[test]
fn a_refusal_goes_out_and_reads_back_under_each_business_status() {
    use angzarr_diagnostic::Diagnostic;
    use angzarr_router::error::{BusinessStatus, CodedError, GrpcCode, HandlerError};

    use super::{rejection, Rejected};

    let object = Any {
        type_url: "type.googleapis.com/toy.meter.v1.MeterSealed".to_string(),
        value: Vec::new(),
    };
    for status in [
        BusinessStatus::FailedPrecondition,
        BusinessStatus::InvalidArgument,
        BusinessStatus::NotFound,
        BusinessStatus::AlreadyExists,
        BusinessStatus::OutOfRange,
        BusinessStatus::PermissionDenied,
    ] {
        let HandlerError::Coded(typed) = rejection((), status, object.clone()) else {
            panic!("a rejection is coded");
        };
        assert_eq!(typed.grpc, GrpcCode::from(status), "{status:?}");
        assert_eq!(typed.code, "ANGZARR_REASON_COMMAND_REFUSED", "{status:?}");
        assert_eq!(
            typed.extras.get("refusal_type").map(String::as_str),
            Some("toy.meter.v1.MeterSealed")
        );
        let read = Rejected::of(&typed).expect("a typed refusal");
        assert_eq!(read.status(), status);
        assert_eq!(read.code(), "ANGZARR_REASON_COMMAND_REFUSED");
        assert_eq!(read.refusal_object(), &object);
        assert_eq!(read.as_coded(), typed, "read back as it went out");
        assert_eq!(
            read.as_refusal(),
            typed.as_refusal().expect("Translate's form"),
            "what a refused policy's Translate is handed"
        );

        let untyped = CodedError {
            refusal: None,
            ..typed
        };
        assert_eq!(Rejected::of(&untyped), None, "{status:?}");
    }
    let bug = CodedError::bug("toy-meter-low", []);
    assert_eq!(Rejected::of(&bug), None);
}

/// What this thread holds allocated, net: every allocation counted against
/// the thread that made it, every free against the thread that freed it.
/// The test binary's allocator: the system's, counting.
mod counted {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    thread_local! {
        // Const-initialized and without a destructor: reading it never
        // allocates, so the allocator may.
        static LIVE: Cell<isize> = const { Cell::new(0) };
    }

    pub struct Counting;

    fn count(delta: isize) {
        let _ = LIVE.try_with(|live| live.set(live.get() + delta));
    }

    // SAFETY: every call is the system allocator's, with its arguments
    // unchanged; the count beside it touches no allocation.
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            count(layout.size() as isize);
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            count(-(layout.size() as isize));
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            count(layout.size() as isize);
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            count(new_size as isize - layout.size() as isize);
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    #[global_allocator]
    static COUNTING: Counting = Counting;

    /// The bytes this thread holds allocated, net, so far.
    pub fn live() -> isize {
        LIVE.with(Cell::get)
    }
}

/// A3: the codes and arg names a handler answers are held while its error
/// is, and freed with it. A child answers arbitrary strings, so a stream of
/// distinct codes must not grow what the process holds: each error, its
/// refusal, its diagnostic and its display, made and dropped, leaves
/// nothing behind.
#[test]
fn a_stream_of_distinct_child_codes_leaks_nothing() {
    use angzarr_diagnostic::Diagnostic;
    use angzarr_router::error::{BusinessStatus, CodedError};

    let answered = |n: usize| {
        let refusal = CodedError::refused(
            BusinessStatus::FailedPrecondition,
            Any {
                type_url: format!("type.googleapis.com/child.v1.Refused{n}"),
                value: n.to_string().into_bytes(),
            },
        );
        let rejected = super::Rejected::of(&refusal).expect("a typed refusal");
        let shown = (
            rejected.to_string(),
            rejected.code().len(),
            rejected.args().len(),
        );
        let bug = super::internal(
            &format!("CHILD_BROKE_{n}"),
            [(format!("table_{n}"), n.to_string())],
        );
        let routed = crate::KernelError::Router(bug.clone());
        let routed = (routed.to_string(), routed.code().len(), routed.args().len());
        let handler_bug = CodedError::bug(
            &format!("CHILD_BUG_{n}"),
            [(format!("hand_{n}"), n.to_string())],
        );
        let reported = super::report(&handler_bug).to_string();
        drop((shown, routed, reported));
    };
    // Whatever is built once, on first use, is built before counting.
    answered(0);
    let before = counted::live();
    for n in 1..=2_000 {
        answered(n);
    }
    let held = counted::live() - before;
    assert!(
        held < 1_024,
        "{held} bytes still held after 2000 distinct codes were answered and dropped"
    );
}

/// A kept refusal is the handler's value beside the report: two
/// rejections are equal by their reports, refusal objects and statuses
/// alone, kept or not, and a kept one shows only that it is kept.
#[test]
fn a_rejection_is_compared_and_shown_by_its_report_not_its_kept_value() {
    use angzarr_router::error::{BusinessStatus, CodedError};
    use std::sync::Arc;

    let coded = |code: &str| {
        CodedError::refused(
            BusinessStatus::FailedPrecondition,
            crate::pack(&crate::toys::ToyRefusal {
                code: code.to_string(),
            }),
        )
    };
    let of = |code: &str| super::Rejected::of(&coded(code)).expect("a typed refusal");
    let bare = of("toy-account-overdrawn");
    let kept = of("toy-account-overdrawn")
        .kept(Some(Arc::new(crate::toys::Refused::Overdrawn { short: 8 })));
    assert_eq!(bare, kept, "kept or not, the same refusal");
    assert_ne!(bare, of("toy-account-not-open"));
    assert_eq!(bare.rejection::<crate::toys::Refused>(), None);
    assert_eq!(
        kept.rejection::<crate::toys::Refused>(),
        Some(&crate::toys::Refused::Overdrawn { short: 8 })
    );
    let shown = format!("{kept:?}");
    assert!(shown.contains("kept: Some(..)"), "{shown}");
}

// Decision 14.65: a spec key travels under the status its option
// declares, read off the generated table; a readiness key, never answered
// as a status, is INTERNAL where the kernel would answer it.
#[test]
fn a_spec_key_is_coded_under_its_declared_status() {
    use angzarr_proto::v1::AngzarrReason;
    use angzarr_router::error::GrpcCode;
    let coded = super::spec_coded(
        AngzarrReason::PmNoInstance,
        [("type_url".to_string(), "/a.B".to_string())],
    );
    assert_eq!(coded.code, "ANGZARR_REASON_PM_NO_INSTANCE");
    assert_eq!(coded.grpc, GrpcCode::FailedPrecondition);
    assert_eq!(
        coded.extras.get("type_url").map(String::as_str),
        Some("/a.B")
    );
    assert_eq!(coded.refusal, None);
    for (grpc, expected) in [
        (
            super::spec_coded(AngzarrReason::EditionNotForked, []).grpc,
            GrpcCode::FailedPrecondition,
        ),
        (
            super::spec_coded(AngzarrReason::HandlerUnavailable, []).grpc,
            GrpcCode::Unavailable,
        ),
        (
            super::spec_coded(AngzarrReason::PersistedEventCorrupt, []).grpc,
            GrpcCode::DataLoss,
        ),
        (
            super::spec_coded(AngzarrReason::MissingRulesVersion, []).grpc,
            GrpcCode::Internal,
        ),
    ] {
        assert_eq!(grpc, expected);
    }
    use super::SpecKey;
    assert_eq!(AngzarrReason::MissingRulesVersion.grpc(), None);
    assert_eq!(
        AngzarrReason::EditionNotForked.grpc(),
        Some(GrpcCode::FailedPrecondition)
    );
    assert_eq!(
        AngzarrReason::FactDeadLettered.key(),
        "ANGZARR_REASON_FACT_DEAD_LETTERED"
    );
}
