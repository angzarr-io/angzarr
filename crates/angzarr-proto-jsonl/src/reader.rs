//! Messages read from a stream, a line each.

use std::io::BufRead;

use prost_reflect::{DynamicMessage, MessageDescriptor};

use crate::{decode_line, Error, Problem};

/// Reads the messages of type `descriptor` from a stream of lines, one per
/// `next`. An item is a message or the [`Error`] of its line, which names
/// the line's number (1-based).
///
/// After an error the reader can be asked to go on, except where the stream
/// has nothing more to give:
///
/// - a line that does not decode (`MalformedLine`), a blank line and a line
///   that is not UTF-8 have been consumed whole: the next item is the next
///   line;
/// - a `TornTail` is the stream's last line, so the next item is `None`;
/// - a `ReadFailed` leaves the stream in a state nobody knows, so the next
///   item is `None` too.
///
/// A caller that must not skip a bad line stops at the first error.
#[derive(Debug)]
pub struct Reader<R: BufRead> {
    input: R,
    descriptor: MessageDescriptor,
    /// Lines consumed so far.
    line: usize,
    done: bool,
}

impl<R: BufRead> Reader<R> {
    pub fn new(input: R, descriptor: MessageDescriptor) -> Self {
        Reader {
            input,
            descriptor,
            line: 0,
            done: false,
        }
    }
}

impl<R: BufRead> Iterator for Reader<R> {
    type Item = Result<DynamicMessage, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let mut buf = Vec::new();
        let read = match self.input.read_until(b'\n', &mut buf) {
            Ok(0) => {
                self.done = true;
                return None;
            }
            read => read,
        };
        self.line += 1;
        let line = self.line;
        let problem = match read {
            Err(e) => Problem::ReadFailed {
                ext_err_txt: e.to_string(),
            },
            // No line feed: the end of a stream that may have been cut short.
            Ok(bytes) if buf.pop() != Some(b'\n') => Problem::TornTail { bytes },
            Ok(_) => {
                let decoded = match std::str::from_utf8(&buf) {
                    Ok(text) => decode_line(&self.descriptor, text),
                    Err(_) => Err(Error {
                        line,
                        problem: Problem::NotUtf8,
                    }),
                };
                return Some(decoded.map_err(|e| Error { line, ..e }));
            }
        };
        self.done = true;
        Some(Err(Error { line, problem }))
    }
}

#[cfg(test)]
#[path = "reader.test.rs"]
mod tests;
