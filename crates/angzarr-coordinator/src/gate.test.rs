use super::*;
use crate::support::{block_on, cover, uuid};

#[test]
fn a_principal_is_an_identity_with_a_closed_allowlist() {
    let principal = Principal::new("run-1", Allowlist::none());
    assert_eq!(*principal.identity(), "run-1");
    assert_eq!(principal.allowlist(), &Allowlist::none());
    #[cfg(feature = "commands")]
    {
        assert_eq!(principal.caller(), Caller::Client);
        assert_eq!(principal.framework().caller(), Caller::Framework);
    }
}

/// The defaults: stamp is the identity, an identity is still valid.
#[test]
fn the_defaults_stamp_nothing_and_hold_an_identity_good() {
    struct Plain;
    impl Gate for Plain {
        type Identity = u8;
        fn identify<'a>(
            &'a self,
            _arrival: Arrival,
            _credential: Option<&'a Credential>,
        ) -> BoxFuture<'a, Result<Principal<u8>, Unauthenticated>> {
            Box::pin(async { Err(Unauthenticated) })
        }
        fn owns(&self, _identity: &u8, _cover: &Cover) -> bool {
            false
        }
    }
    let book = crate::support::book("d", 1, crate::BodyKind::Fact, "x/Y", 3);
    assert_eq!(Plain.stamp(&1, book.clone()), book);
    assert!(block_on(Plain.still_valid(&1)));
}

/// `Trusted` identifies nobody: whatever is presented, or nothing, the same
/// principal, unrestricted, owning every stream.
#[test]
fn trusted_identifies_nobody_and_owns_everything() {
    let presented = Credential::bearer("anything");
    for credential in [None, Some(&presented)] {
        let principal =
            block_on(Trusted.identify(Arrival::Ip, credential)).expect("trusted identifies");
        assert_eq!(*principal.identity(), ());
        assert_eq!(
            principal.allowlist().permits_read(crate::ReadOp::Follow),
            Ok(())
        );
    }
    assert!(Trusted.owns(&(), &cover("any", 3)));
    assert!(Trusted.owns(&(), &Cover::default()));
    let _ = uuid(1);
}
