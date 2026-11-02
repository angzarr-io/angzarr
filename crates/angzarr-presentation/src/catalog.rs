//! A diagnostic's message, from the catalogs of the crates it can come
//! from.

use std::fmt::Display;

use angzarr_diagnostic::{Arg, Diagnostic};
use fluent_bundle::concurrent::FluentBundle;
use fluent_bundle::{FluentArgs, FluentResource, FluentValue};
use unic_langid::LanguageIdentifier;

use crate::{CatalogError, Envelope, Unrendered, EN_US};

/// A crate's catalog: its name and its `locales/<locale>/<crate>.ftl`,
/// embedded. Each crate with diagnostics exports its en-US one as `EN_US`.
pub type Source = (&'static str, &'static str);

/// The largest magnitude an `f64` holds exactly: past it, a number is
/// passed to Fluent as its digits, so it is shown as it is.
const MAX_EXACT: u64 = 1 << 53;

/// The messages of every crate a binary's diagnostics come from, in one
/// locale. `Sync`, so a server can share one.
pub struct Catalog {
    bundle: FluentBundle<FluentResource>,
}

impl Catalog {
    /// The en-US catalog: this crate's messages, then `sources`'. A source
    /// that does not parse, or repeats a message, is refused.
    pub fn en_us(sources: &[Source]) -> Result<Catalog, CatalogError> {
        let locale: LanguageIdentifier = "en-US".parse().unwrap_or_default();
        let mut bundle = FluentBundle::new_concurrent(vec![locale]);
        // Messages go to terminals, logs and JSON, never into
        // bidirectional text: no isolation marks around args.
        bundle.set_use_isolating(false);
        for &(name, ftl) in std::iter::once(&EN_US).chain(sources) {
            let invalid = |detail| CatalogError {
                source: name.to_string(),
                detail,
            };
            let resource =
                FluentResource::try_new(ftl.to_string()).map_err(|(_, e)| invalid(joined(&e)))?;
            bundle
                .add_resource(resource)
                .map_err(|e| invalid(joined(&e)))?;
        }
        Ok(Catalog { bundle })
    }

    /// `d`'s message, filled from its args; a nested cause is its own
    /// [`message`](Catalog::message). Strict: for a check that every code
    /// has a message.
    pub fn render(&self, d: &dyn Diagnostic) -> Result<String, Unrendered> {
        let code = d.code();
        let pattern = self
            .bundle
            .get_message(code)
            .and_then(|m| m.value())
            .ok_or_else(|| Unrendered::Unknown {
                code: code.to_string(),
            })?;
        let mut args = FluentArgs::new();
        for (name, arg) in d.args() {
            args.set(name, self.value(arg));
        }
        let mut errors = Vec::new();
        let message = self
            .bundle
            .format_pattern(pattern, Some(&args), &mut errors);
        if errors.is_empty() {
            Ok(message.into_owned())
        } else {
            Err(Unrendered::Unformattable {
                code: code.to_string(),
                detail: joined(&errors),
            })
        }
    }

    /// `d`'s message, or its key and args ([`keyed`]) where no catalog has
    /// one: never the English an error's `Display` may carry.
    pub fn message(&self, d: &dyn Diagnostic) -> String {
        self.render(d).unwrap_or_else(|_| keyed(d))
    }

    /// `d` as JSON: its code, args and message, a nested cause an envelope
    /// in its turn.
    pub fn envelope(&self, d: &dyn Diagnostic) -> Envelope {
        Envelope::of(self, d)
    }

    fn value(&self, arg: Arg) -> FluentValue<'static> {
        match arg {
            Arg::Int(n) => number(n.unsigned_abs() <= MAX_EXACT, n as f64, n),
            Arg::Uint(n) => number(n <= MAX_EXACT, n as f64, n),
            Arg::Str(s) => s.into(),
            Arg::Nested(cause) => self.message(&*cause).into(),
        }
    }
}

/// `d` as its key and args (angzarr decision 14.37, as
/// angzarr-diagnostic's `display` writes it): its code, then
/// ` name=value` for each arg in order, a text value quoted where it would
/// not parse back, a nested cause as its code under the arg's name and each
/// of its own args under `<name>.`.
fn keyed(d: &dyn Diagnostic) -> String {
    let mut out = d.code().to_string();
    write_args(&mut out, d, "");
    out
}

fn write_args(out: &mut String, d: &dyn Diagnostic, prefix: &str) {
    for (name, arg) in d.args() {
        let value = match arg {
            Arg::Int(n) => n.to_string(),
            Arg::Uint(n) => n.to_string(),
            Arg::Str(s) => angzarr_diagnostic::quote(&s).into_owned(),
            Arg::Nested(cause) => {
                out.push_str(&format!(" {prefix}{name}={}", cause.code()));
                write_args(out, &*cause, &format!("{prefix}{name}."));
                continue;
            }
        };
        out.push_str(&format!(" {prefix}{name}={value}"));
    }
}

/// A number Fluent can select a plural on, unless it is too large for an
/// `f64` to hold exactly; then its digits.
fn number(exact: bool, value: f64, digits: impl Display) -> FluentValue<'static> {
    if exact {
        value.into()
    } else {
        digits.to_string().into()
    }
}

fn joined(errors: &[impl Display]) -> String {
    errors
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

#[cfg(test)]
#[path = "catalog.test.rs"]
mod tests;
