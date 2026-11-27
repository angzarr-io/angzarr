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
/// (page 1) as SQLite lays it out: a table b-tree, one leaf page for a
/// small schema and interior pages above leaves for a larger one. Each row
/// is (type, name, tbl_name, rootpage, sql).
fn root_page(file: &[u8], name: &str) -> usize {
    let page_size = match u16::from_be_bytes([file[16], file[17]]) {
        1 => 65_536,
        size => usize::from(size),
    };
    find_in(file, page_size, 1, name).unwrap_or_else(|| panic!("no {name} in the schema"))
}

/// The root page of `name` in the schema rows under table b-tree page `no`.
fn find_in(file: &[u8], page_size: usize, no: usize, name: &str) -> Option<usize> {
    let base = (no - 1) * page_size;
    // The first page holds the file's 100-byte header before the b-tree's.
    let page = &file[base + if no == 1 { 100 } else { 0 }..base + page_size];
    let cells = usize::from(u16::from_be_bytes([page[3], page[4]]));
    match page[0] {
        0x0d => {
            for cell in 0..cells {
                let pointer = 8 + 2 * cell;
                let at = usize::from(u16::from_be_bytes([page[pointer], page[pointer + 1]]));
                let at = base + at;
                if let Some(root) = row_named(file, at, name) {
                    return Some(root);
                }
            }
            None
        }
        0x05 => {
            let mut children = Vec::new();
            for cell in 0..cells {
                let pointer = 12 + 2 * cell;
                let at = usize::from(u16::from_be_bytes([page[pointer], page[pointer + 1]]));
                let at = base + at;
                children.push(
                    usize::try_from(u32::from_be_bytes([
                        file[at],
                        file[at + 1],
                        file[at + 2],
                        file[at + 3],
                    ]))
                    .unwrap(),
                );
            }
            children.push(
                usize::try_from(u32::from_be_bytes([page[8], page[9], page[10], page[11]]))
                    .unwrap(),
            );
            children
                .into_iter()
                .find_map(|child| find_in(file, page_size, child, name))
        }
        other => panic!("the schema table has a page of type {other:#x}"),
    }
}

/// The schema row whose cell starts at `at`, as its rootpage if it names
/// `name`.
fn row_named(file: &[u8], at: usize, name: &str) -> Option<usize> {
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
    (columns[1].0 == name.as_bytes()).then_some(columns[3].1)
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
