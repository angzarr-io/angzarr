//! Why generation refused. Every refusal names the element to change, and
//! is one of four concerns, each a type of its own module:
//!
//! - [`ProtoError`]: the descriptor set as protobuf, which this generator
//!   cannot read, or whose types or names the domain layer cannot map;
//! - [`MarkupError`]: angzarr's declarations on the protos (`(command)`,
//!   `(event)`, `(component)` and the rest) breaking a rule of the
//!   generated domain layer;
//! - [`UseError`]: a use of the protos they cannot serve: a file, wire
//!   path or Python domain asked for, or a user's file read back that does
//!   not match them;
//! - [`AppError`]: an application's host config or manifest
//!   (`angzarr scaffold app`).
//!
//! [`CodegenError`] is one of them, and is each transparently: its code and
//! args are its concern's. Each error is a code and its args
//! ([`Diagnostic`]), and `Display`s as them (decision 14.37): its English
//! is its en-US catalog message ([`crate::EN_US`]), rendered where it is
//! shown, never here.

mod app;
mod markup;
mod proto;
mod usage;

use std::fmt;

use angzarr_diagnostic::{Arg, Diagnostic};

pub use app::AppError;
pub use markup::MarkupError;
pub use proto::ProtoError;
pub use usage::UseError;

/// Each named field as the arg of its own name, its text:
/// `texts![message, component]`. An arg is named as the field it carries.
macro_rules! texts {
    ($($field:ident),+ $(,)?) => {
        vec![$((stringify!($field), ::angzarr_diagnostic::Arg::from($field.as_str()))),+]
    };
}
pub(crate) use texts;

/// A refusal of generation: one of its concerns, whose code and args it
/// carries unchanged.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum CodegenError {
    /// The descriptor set as protobuf.
    Proto(ProtoError),
    /// angzarr's declarations on the protos.
    Markup(MarkupError),
    /// A use of the protos they cannot serve.
    Use(UseError),
    /// An application's host config or manifest.
    App(AppError),
}

impl CodegenError {
    /// The concern's own error, as a [`Diagnostic`].
    fn concern(&self) -> &dyn Diagnostic {
        match self {
            CodegenError::Proto(e) => e,
            CodegenError::Markup(e) => e,
            CodegenError::Use(e) => e,
            CodegenError::App(e) => e,
        }
    }
}

impl From<ProtoError> for CodegenError {
    fn from(e: ProtoError) -> Self {
        CodegenError::Proto(e)
    }
}

impl From<MarkupError> for CodegenError {
    fn from(e: MarkupError) -> Self {
        CodegenError::Markup(e)
    }
}

impl From<UseError> for CodegenError {
    fn from(e: UseError) -> Self {
        CodegenError::Use(e)
    }
}

impl From<AppError> for CodegenError {
    fn from(e: AppError) -> Self {
        CodegenError::App(e)
    }
}

impl fmt::Display for CodegenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for CodegenError {}

impl Diagnostic for CodegenError {
    fn code(&self) -> &str {
        self.concern().code()
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        self.concern().args()
    }
}

/// One value of every error this crate defines, for the catalog test:
/// each refusal as a [`CodegenError`], and each [`crate::AppLint`].
pub fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    let errors = (proto::samples().into_iter().map(CodegenError::from))
        .chain(markup::samples().into_iter().map(CodegenError::from))
        .chain(usage::samples().into_iter().map(CodegenError::from))
        .chain(app::samples().into_iter().map(CodegenError::from));
    errors
        .map(|e| Box::new(e) as Box<dyn Diagnostic>)
        .chain(crate::app::app_lint_samples())
        .chain(
            crate::warnings::samples()
                .into_iter()
                .map(|w| Box::new(w) as Box<dyn Diagnostic>),
        )
        .collect()
}

/// A text sample: the field's own name, so a message filling one arg's
/// place with another's reads wrong.
pub(crate) fn text(field: &str) -> String {
    field.to_string()
}

#[cfg(test)]
mod tests {
    use angzarr_diagnostic::{Arg, Diagnostic, code};

    use super::{CodegenError, MarkupError, diagnostic_samples, text};

    /// Each text arg of a sample is its own name, so a message showing one
    /// arg in another's place reads wrong where it is rendered.
    #[test]
    fn every_samples_text_args_are_their_own_names() {
        for sample in diagnostic_samples() {
            for (name, arg) in sample.args() {
                if let Arg::Str(value) = arg {
                    assert_eq!(value, name, "{}", sample.code());
                }
            }
        }
    }

    /// Each concern's error displays as its code and args on its own, as
    /// it does as a [`CodegenError`].
    #[test]
    fn every_concern_displays_as_its_code_and_args() {
        fn check<E: Diagnostic + Clone + Into<CodegenError>>(samples: Vec<E>) {
            for concern in samples {
                let shown = concern.to_string();
                assert!(shown.starts_with(concern.code()), "{shown}");
                assert_eq!(
                    shown,
                    Into::<CodegenError>::into(concern.clone()).to_string()
                );
            }
        }
        check(super::proto::samples());
        check(super::markup::samples());
        check(super::usage::samples());
        check(super::app::samples());
    }

    /// A refusal displays as its concern's code and args, through the
    /// wrapper as on its own.
    #[test]
    fn a_refusal_is_its_concerns_code_and_args() {
        let concern = MarkupError::UnknownEmit {
            command: text("command"),
            event: text("event"),
        };
        let error = CodegenError::from(concern.clone());
        assert!(matches!(
            error,
            CodegenError::Markup(MarkupError::UnknownEmit { .. })
        ));
        assert_eq!(error.code(), concern.code());
        assert_eq!(error.to_string(), concern.to_string());
        assert_eq!(
            error.to_string(),
            concat!(code!("unknown-emit"), " command=command event=event")
        );
    }
}
