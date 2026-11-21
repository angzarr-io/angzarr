//! The check of what the store keeps twice. DuckDB checks its own file;
//! what it cannot check is that `streams` (a stream's extent, read by every
//! append) still says what `events` (the pages) hold. They are written in
//! one transaction, so they disagree only if something else wrote the file,
//! and a store that trusts a wrong extent appends at the wrong sequence.

use std::fmt;

use angzarr_ports::angzarr_diagnostic::{self, code, Diagnostic};
use angzarr_ports::{Arg, StoreError};

use crate::queries::Queries;
use crate::DuckStore;

/// What an integrity check found.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Integrity {
    /// The extents say what the pages hold.
    Intact,
    /// They disagree, as listed.
    Damaged(Damage),
}

/// The disagreements found: at least one, each as text that was read from
/// the file's own rows.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Damage {
    pub(crate) problems: Vec<String>,
}

impl Damage {
    /// Every problem, in the order found; never empty.
    pub fn problems(&self) -> &[String] {
        &self.problems
    }

    fn first(&self) -> &str {
        self.problems.first().map_or("", String::as_str)
    }

    fn more(&self) -> usize {
        self.problems.len().saturating_sub(1)
    }

    pub fn code(&self) -> &'static str {
        code!("damaged")
    }

    pub fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![
            ("first", Arg::Str(self.first().to_string())),
            ("more", Arg::Uint(self.more() as u64)),
        ]
    }
}

impl Diagnostic for Damage {
    fn code(&self) -> &str {
        Damage::code(self)
    }

    fn args(&self) -> Vec<(&str, angzarr_diagnostic::Arg)> {
        Damage::args(self)
            .into_iter()
            .map(|(name, arg)| (name, arg.into()))
            .collect()
    }
}

impl fmt::Display for Damage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

/// A damage report, for the catalog test.
pub fn damage_sample() -> Damage {
    Damage {
        problems: vec!["x".to_string(), "y".to_string()],
    }
}

/// Streams whose extent is not what their pages hold, and pages of a stream
/// with no extent.
const CHECK: &str = "\
SELECT 'the extent of ' || s.domain || '/' || s.edition || ' disagrees with its pages' \
FROM streams s LEFT JOIN ( \
    SELECT domain, edition, root, MIN(sequence) AS lo, MAX(sequence) AS hi, COUNT(*) AS n \
    FROM events GROUP BY domain, edition, root) e \
  ON e.domain = s.domain AND e.edition = s.edition AND e.root = s.root \
WHERE e.n IS NULL OR e.lo <> s.first_sequence OR e.hi + 1 <> s.next_sequence \
   OR e.n <> s.next_sequence - s.first_sequence \
UNION ALL \
SELECT 'pages of ' || e.domain || '/' || e.edition || ' have no extent' \
FROM (SELECT DISTINCT domain, edition, root FROM events) e LEFT JOIN streams s \
  ON e.domain = s.domain AND e.edition = s.edition AND e.root = s.root \
WHERE s.domain IS NULL";

impl DuckStore {
    /// Whether every stream's extent says what its pages hold.
    pub fn integrity_check(&self) -> Result<Integrity, StoreError> {
        let problems = self.with(|conn| conn.rows(CHECK, &[], |row| row.get::<_, String>(0)))?;
        Ok(if problems.is_empty() {
            Integrity::Intact
        } else {
            Integrity::Damaged(Damage { problems })
        })
    }
}

#[cfg(test)]
#[path = "integrity.test.rs"]
mod tests;
