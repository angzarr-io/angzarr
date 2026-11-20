//! Which of the two formats an answer is written in.

use std::str::FromStr;

use crate::UnknownFormat;

/// JSON for programs, UTF-8 text for people. YAML, TOML and markdown are
/// left out until something needs them; another variant here, and its arm
/// in [`render`](crate::render), would add one.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Format {
    Text,
    Json,
}

impl Format {
    /// The format asked for: `json` (a `--json` flag) wins, then `asked`
    /// (a `--format`), else text on a terminal and JSON off one.
    pub fn choose(json: bool, asked: Option<Format>, stdout_is_terminal: bool) -> Format {
        match (json, asked) {
            (true, _) => Format::Json,
            (false, Some(format)) => format,
            (false, None) if stdout_is_terminal => Format::Text,
            (false, None) => Format::Json,
        }
    }
}

impl FromStr for Format {
    type Err = UnknownFormat;

    /// `text` or `json`, as a `--format` value.
    fn from_str(value: &str) -> Result<Format, UnknownFormat> {
        match value {
            "text" => Ok(Format::Text),
            "json" => Ok(Format::Json),
            _ => Err(UnknownFormat {
                value: value.to_string(),
            }),
        }
    }
}

#[cfg(test)]
#[path = "format.test.rs"]
mod tests;
