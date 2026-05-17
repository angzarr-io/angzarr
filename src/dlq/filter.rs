//! AIP-160 filter-string parser for `ListDeadLetters`.
//!
//! v1 grammar — deliberately narrow so the parser is auditable:
//!
//! ```text
//! filter      := clause { "AND" clause }
//! clause      := field "=" value
//! field       := "domain" | "correlation_id" | "rejection_type"
//!              | "source_component" | "occurred_after" | "occurred_before"
//! value       := quoted-string
//! quoted-string := '"' { any-char-except-quote } '"'
//! ```
//!
//! - Empty / whitespace-only filter → matches everything (`ListFilter::default()`).
//! - Whitespace between tokens is ignored.
//! - `AND` is case-insensitive.
//! - `occurred_after` / `occurred_before` parse the value as RFC-3339.
//! - Repeated assignment of the same field is a parse error (catches
//!   `domain = "a" AND domain = "b"` rather than silently last-wins).
//! - Anything richer (`OR`, `>` / `<`, parens, function calls) is out
//!   of scope for v1 and yields a parse error so future extensions
//!   stay backward-compatible.
//!
//! Plan reference: P1.2 / S1 in `plans/virtual-spinning-flute.md`.

use chrono::{DateTime, Utc};

use super::error::DlqError;
use super::reader::ListFilter;

/// Parse a filter expression into a [`ListFilter`].
///
/// `page_size` and `page_token` are caller-managed (they live on the
/// request, not the filter string); this function leaves them
/// untouched on the returned filter.
pub fn parse_filter(input: &str) -> Result<ListFilter, DlqError> {
    let trimmed = input.trim();
    let mut filter = ListFilter::default();
    if trimmed.is_empty() {
        return Ok(filter);
    }

    let mut cursor = trimmed;
    loop {
        let (rest, clause) = parse_clause(cursor)?;
        apply_clause(&mut filter, clause)?;
        let rest = rest.trim_start();
        if rest.is_empty() {
            break;
        }
        cursor = expect_and(rest)?.trim_start();
        if cursor.is_empty() {
            return Err(DlqError::InvalidArgument(
                "trailing 'AND' with no clause".to_string(),
            ));
        }
    }
    Ok(filter)
}

#[derive(Debug, PartialEq, Eq)]
struct Clause<'a> {
    field: &'a str,
    value: String,
}

fn parse_clause(s: &str) -> Result<(&str, Clause<'_>), DlqError> {
    let s = s.trim_start();
    // field: alphanum + underscore.
    let field_end = s
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(s.len());
    if field_end == 0 {
        return Err(DlqError::InvalidArgument(format!(
            "expected field name at: {}",
            preview(s)
        )));
    }
    let field = &s[..field_end];
    let rest = s[field_end..].trim_start();
    let rest = rest
        .strip_prefix('=')
        .ok_or_else(|| DlqError::InvalidArgument(format!("expected '=' after field '{}'", field)))?
        .trim_start();
    let (rest, value) = parse_quoted(rest)?;
    Ok((rest, Clause { field, value }))
}

fn parse_quoted(s: &str) -> Result<(&str, String), DlqError> {
    let s = s
        .strip_prefix('"')
        .ok_or_else(|| DlqError::InvalidArgument(
            "filter values must be double-quoted strings".to_string(),
        ))?;
    let end = s.find('"').ok_or_else(|| {
        DlqError::InvalidArgument("unterminated quoted string in filter".to_string())
    })?;
    let value = s[..end].to_string();
    Ok((&s[end + 1..], value))
}

fn expect_and(s: &str) -> Result<&str, DlqError> {
    // Case-insensitive "AND" with mandatory surrounding whitespace
    // already trimmed by the caller. Three chars is enough to check.
    if s.len() < 3 || !s[..3].eq_ignore_ascii_case("AND") {
        return Err(DlqError::InvalidArgument(format!(
            "expected 'AND' between clauses, got: {}",
            preview(s)
        )));
    }
    // Must be followed by whitespace OR end-of-string — otherwise
    // "ANDsome_field" wouldn't be a valid separator.
    let after = &s[3..];
    if !after.is_empty() && !after.starts_with(|c: char| c.is_whitespace()) {
        return Err(DlqError::InvalidArgument(format!(
            "expected whitespace after 'AND', got: {}",
            preview(s)
        )));
    }
    Ok(after)
}

fn apply_clause(f: &mut ListFilter, c: Clause<'_>) -> Result<(), DlqError> {
    macro_rules! once {
        ($slot:expr, $name:literal) => {{
            if $slot.is_some() {
                return Err(DlqError::InvalidArgument(format!(
                    "field '{}' specified more than once",
                    $name
                )));
            }
            $slot = Some(c.value);
        }};
    }
    match c.field {
        "domain" => once!(f.domain, "domain"),
        "correlation_id" => once!(f.correlation_id, "correlation_id"),
        "rejection_type" => once!(f.rejection_type, "rejection_type"),
        "source_component" => once!(f.source_component, "source_component"),
        "occurred_after" => {
            if f.occurred_after.is_some() {
                return Err(DlqError::InvalidArgument(
                    "field 'occurred_after' specified more than once".to_string(),
                ));
            }
            f.occurred_after = Some(parse_rfc3339(&c.value, "occurred_after")?);
        }
        "occurred_before" => {
            if f.occurred_before.is_some() {
                return Err(DlqError::InvalidArgument(
                    "field 'occurred_before' specified more than once".to_string(),
                ));
            }
            f.occurred_before = Some(parse_rfc3339(&c.value, "occurred_before")?);
        }
        other => {
            return Err(DlqError::InvalidArgument(format!(
                "unknown filter field: '{}'",
                other
            )))
        }
    }
    Ok(())
}

fn parse_rfc3339(s: &str, field: &str) -> Result<DateTime<Utc>, DlqError> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| {
            DlqError::InvalidArgument(format!(
                "field '{}' must be RFC-3339 timestamp, got '{}': {}",
                field, s, e
            ))
        })
}

/// Truncate a fragment for inclusion in error messages; keeps logs
/// from blowing up if a hostile client sends a multi-MB filter.
fn preview(s: &str) -> String {
    const MAX: usize = 40;
    let mut iter = s.chars();
    let truncated: String = iter.by_ref().take(MAX).collect();
    if iter.next().is_some() {
        format!("{}...", truncated)
    } else {
        truncated
    }
}

#[cfg(test)]
#[path = "filter.test.rs"]
mod tests;
