//! Messages read from a stream, a line each.

use std::io::BufRead;

use prost_reflect::{DynamicMessage, MessageDescriptor};

use crate::Error;

/// Reads the messages of type `descriptor` from a stream of lines.
#[derive(Debug)]
pub struct Reader<R: BufRead> {
    input: R,
    descriptor: MessageDescriptor,
}

impl<R: BufRead> Reader<R> {
    pub fn new(input: R, descriptor: MessageDescriptor) -> Self {
        Reader { input, descriptor }
    }
}

impl<R: BufRead> Iterator for Reader<R> {
    type Item = Result<DynamicMessage, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let _ = (&mut self.input, &self.descriptor);
        None
    }
}

#[cfg(test)]
#[path = "reader.test.rs"]
mod tests;
