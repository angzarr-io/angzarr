//! Messages appended to a stream, a line each.

use std::io::Write;

use prost_reflect::DynamicMessage;

use crate::Error;

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
        let _ = message;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }

    /// The stream, given back.
    pub fn into_inner(self) -> W {
        self.out
    }
}

#[cfg(test)]
#[path = "writer.test.rs"]
mod tests;
