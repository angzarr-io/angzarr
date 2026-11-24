//! Test support, shared with the binary's acceptance tests: a SQLite
//! database file damaged as annals-store-sql's own tests damage one, by
//! changing the bytes of one index's root page, so the index is damaged
//! and the tables an open reads are not. It reads the file as SQLite lays
//! it out (https://sqlite.org/fileformat2.html), so it needs no SQLite of
//! its own.

/// A SQLite varint at the start of `bytes`: its value and its length.
fn varint(bytes: &[u8]) -> (u64, usize) {
    let mut value = 0u64;
    for (i, byte) in bytes.iter().enumerate().take(9) {
        if i == 8 {
            return ((value << 8) | u64::from(*byte), 9);
        }
        value = (value << 7) | u64::from(byte & 0x7f);
        if byte & 0x80 == 0 {
            return (value, i + 1);
        }
    }
    panic!("a truncated varint")
}

/// The root page of the schema object `name`, read from the schema table
/// on the file's first page (one leaf page, for a schema this small): each
/// row is (type, name, tbl_name, rootpage, sql).
fn root_page(file: &[u8], name: &str) -> usize {
    let page = &file[100..];
    assert_eq!(page[0], 0x0d, "the schema is one table leaf page");
    let cells = usize::from(u16::from_be_bytes([page[3], page[4]]));
    for cell in 0..cells {
        let at = usize::from(u16::from_be_bytes([page[8 + 2 * cell], page[9 + 2 * cell]]));
        let (_, size) = varint(&file[at..]);
        let (_, rowid) = varint(&file[at + size..]);
        let record = &file[at + size + rowid..];
        let (header, mut i) = varint(record);
        let mut body = usize::try_from(header).unwrap();
        // Each column: its bytes, and the integer they hold if one.
        let mut columns: Vec<(&[u8], usize)> = Vec::new();
        while i < usize::try_from(header).unwrap() {
            let (serial, n) = varint(&record[i..]);
            i += n;
            let len = match serial {
                0 | 8 | 9 => 0,
                1..=4 => usize::try_from(serial).unwrap(),
                5 => 6,
                6 | 7 => 8,
                text_or_blob => usize::try_from((text_or_blob - 12) / 2).unwrap(),
            };
            let bytes = &record[body..body + len];
            let int = bytes.iter().fold(0, |v, b| (v << 8) | usize::from(*b));
            columns.push((bytes, int));
            body += len;
        }
        if columns[1].0 == name.as_bytes() {
            return columns[3].1;
        }
    }
    panic!("no {name} in the schema")
}

/// `file`, a whole database file (its write-ahead log checkpointed into
/// it), with the root page of `index` changed by `tamper`.
pub fn damaged(file: &[u8], index: &str, tamper: impl FnOnce(&mut [u8])) -> Vec<u8> {
    let mut file = file.to_vec();
    let page_size = match u16::from_be_bytes([file[16], file[17]]) {
        1 => 65_536,
        size => usize::from(size),
    };
    let root = root_page(&file, index);
    let start = (root - 1) * page_size;
    tamper(&mut file[start..start + page_size]);
    file
}
