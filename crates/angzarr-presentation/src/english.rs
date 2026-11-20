//! Test support, behind the `test-support` feature, for every crate whose
//! tests hold a diagnostic's English to an expected text: a diagnostic's
//! en-US message, rendered from catalogs (each a crate's `EN_US` text) as
//! an edge renders it, its nested causes too. A code no catalog has is its
//! key and args, as an edge shows it. `english_for!(&[...catalogs])` gives
//! every diagnostic an `english()` method over those catalogs.

use angzarr_diagnostic::{Arg, Diagnostic};

/// angzarr-diagnostic, as `english_for!` names it wherever it is invoked.
#[doc(hidden)]
pub use angzarr_diagnostic as diagnostic;
use fluent_bundle::{FluentArgs, FluentBundle, FluentResource, FluentValue};

/// `d`'s en-US message from `catalogs`.
pub fn english_in(catalogs: &[&str], d: &dyn Diagnostic) -> String {
    let mut bundle = FluentBundle::new(vec!["en-US".parse().expect("a locale")]);
    bundle.set_use_isolating(false);
    for ftl in catalogs {
        let resource = FluentResource::try_new(ftl.to_string()).expect("the catalog parses");
        bundle.add_resource(resource).expect("no message twice");
    }
    render(&bundle, d)
}

fn render(bundle: &FluentBundle<FluentResource>, d: &dyn Diagnostic) -> String {
    let Some(pattern) = bundle.get_message(d.code()).and_then(|m| m.value()) else {
        return d.to_string();
    };
    let mut args = FluentArgs::new();
    for (name, arg) in d.args() {
        let value: FluentValue = match arg {
            Arg::Int(n) => (n as f64).into(),
            Arg::Uint(n) => (n as f64).into(),
            Arg::Str(s) => s.into(),
            Arg::Nested(cause) => render(bundle, &*cause).into(),
        };
        args.set(name, value);
    }
    let mut errors = Vec::new();
    let text = bundle.format_pattern(pattern, Some(&args), &mut errors);
    assert!(errors.is_empty(), "{}: {errors:?}", d.code());
    text.into_owned()
}

/// An `English` trait in the invoking module, rendering every diagnostic
/// from `$catalogs` (an expression of type `&[&str]`).
#[macro_export]
macro_rules! english_for {
    ($catalogs:expr) => {
        /// A diagnostic's en-US message, as an edge renders it.
        #[allow(dead_code)]
        trait English {
            fn english(&self) -> String;
        }

        impl<D: $crate::english::diagnostic::Diagnostic + ?Sized> English for D {
            fn english(&self) -> String {
                $crate::english::english_in($catalogs, &Wrapped(self))
            }
        }

        /// `D` as a `dyn Diagnostic`, whatever its size.
        #[allow(dead_code)]
        struct Wrapped<'d, D: ?Sized>(&'d D);

        impl<D: $crate::english::diagnostic::Diagnostic + ?Sized> std::fmt::Debug
            for Wrapped<'_, D>
        {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Debug::fmt(self.0, f)
            }
        }

        impl<D: $crate::english::diagnostic::Diagnostic + ?Sized> std::fmt::Display
            for Wrapped<'_, D>
        {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(self.0, f)
            }
        }

        impl<D: $crate::english::diagnostic::Diagnostic + ?Sized>
            $crate::english::diagnostic::Diagnostic for Wrapped<'_, D>
        {
            fn code(&self) -> &str {
                self.0.code()
            }
            fn args(&self) -> Vec<(&str, $crate::english::diagnostic::Arg)> {
                self.0.args()
            }
        }
    };
}
