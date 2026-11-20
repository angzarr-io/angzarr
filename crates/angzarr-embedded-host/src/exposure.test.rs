use super::*;
use angzarr_diagnostic::code;

/// Loopback is admitted by "loopback" and "ip"; the network by "ip"
/// alone; "none" admits nothing.
#[test]
fn an_exposure_admits_the_arrivals_at_or_inside_it() {
    let admitted = |arrival: Arrival| {
        [Expose::None, Expose::Loopback, Expose::Ip]
            .into_iter()
            .filter(|expose| arrival.admitted_by(*expose))
            .collect::<Vec<_>>()
    };
    assert_eq!(admitted(Arrival::Loopback), [Expose::Loopback, Expose::Ip]);
    assert_eq!(admitted(Arrival::Ip), [Expose::Ip]);
}

/// The local log line names the domain, where the request arrived from
/// and the component's exposure, as code and args.
#[test]
fn not_exposed_is_its_code_and_args() {
    let line = NotExposed {
        domain: "table".to_string(),
        arrival: Arrival::Ip,
        expose: Expose::None,
    }
    .to_string();
    assert_eq!(
        line,
        concat!(code!("not-exposed"), " domain=table arrival=ip expose=none")
    );
}

/// The kernel's own failure, unserved, is answered as it is, and has no
/// hidden reason.
#[test]
fn a_kernel_failure_unserved_is_its_own_answer() {
    use angzarr_embedded::{Failure, KernelError};
    let failure = Failure::Kernel(KernelError::ChainTooDeep { limit: 8 });
    let unserved = Unserved::Failed(failure.clone());
    assert_eq!(unserved.answer(), &failure);
    assert_eq!(unserved.hidden(), None);
}
