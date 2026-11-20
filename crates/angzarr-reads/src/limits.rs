//! [`Limits`]: how many pages and bytes one book may carry.

use angzarr_proto::v1::EventPage;
use prost::Message;

/// The size of one book an answer is cut into: at most `max_pages` pages
/// and `max_bytes` of encoded pages. A book always holds a page, so one
/// larger than `max_bytes` travels alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub max_pages: usize,
    pub max_bytes: usize,
}

impl Limits {
    /// 1000 pages and 3 MiB: well under a transport's 4 MiB message.
    pub const DEFAULT: Limits = Limits {
        max_pages: 1000,
        max_bytes: 3 << 20,
    };

    /// `pages`, in order, cut into books within the limits.
    pub fn chunks(&self, pages: Vec<EventPage>) -> Vec<Vec<EventPage>> {
        let mut books = Vec::new();
        let mut book = Vec::new();
        let mut bytes = 0usize;
        for page in pages {
            let size = page.encoded_len();
            let full = !book.is_empty()
                && (book.len() >= self.max_pages || bytes.saturating_add(size) > self.max_bytes);
            if full {
                books.push(std::mem::take(&mut book));
                bytes = 0;
            }
            bytes += size;
            book.push(page);
        }
        if !book.is_empty() {
            books.push(book);
        }
        books
    }
}

impl Default for Limits {
    fn default() -> Limits {
        Limits::DEFAULT
    }
}

#[cfg(test)]
#[path = "limits.test.rs"]
mod tests;
