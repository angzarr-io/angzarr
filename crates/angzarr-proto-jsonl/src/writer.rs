//! Messages appended to a stream, a line each.

use std::io::Write;

use prost_reflect::DynamicMessage;

use crate::{encode_line, Error, Problem};

/// Appends each message as `line + "\n"` to the stream it was given.
#[derive(Debug)]
pub struct Writer<W: Write> {
    out: W,
}

impl<W: Write> Writer<W> {
    pub fn new(out: W) -> Self {
        Writer { out }
    }

    /// Append the message and its line feed, in one write. A message that
    /// can't be encoded writes nothing.
    pub fn write(&mut self, message: &DynamicMessage) -> Result<(), Error> {
        let mut line = encode_line(message)?;
        line.push('\n');
        self.out.write_all(line.as_bytes()).map_err(write_failed)
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        self.out.flush().map_err(write_failed)
    }

    /// The stream, given back.
    pub fn into_inner(self) -> W {
        self.out
    }
}

fn write_failed(e: std::io::Error) -> Error {
    Error {
        line: 0,
        problem: Problem::WriteFailed {
            ext_err_txt: e.to_string(),
        },
    }
}

#[cfg(test)]
#[path = "writer.test.rs"]
mod tests;
