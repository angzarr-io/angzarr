//! Reading a compiled `FileDescriptorSet` off its wire bytes, for what
//! prost does not generate: the custom options (`extend`) whose values a
//! build script reads (`build_reasons.rs`'s `(io.angzarr.v1.reason)` args,
//! angzarr-proto-transport-embedded's `(child_protocol)` constants). Shared
//! by angzarr-core's proto crates' build scripts (`#[path]`), which compile
//! no dependency for it.

/// Each wire field: its number, its bytes if length-delimited, and its
/// value if a varint. A compiled descriptor holds no fixed-width field (an
/// uninterpreted option's double never survives compilation), so one is
/// refused rather than skipped unread.
pub fn fields(mut buf: &[u8]) -> Vec<(u64, Option<&[u8]>, u64)> {
    let mut out = Vec::new();
    while !buf.is_empty() {
        let key = varint(&mut buf);
        match key & 7 {
            0 => {
                let v = varint(&mut buf);
                out.push((key >> 3, None, v));
            }
            2 => {
                let len = usize::try_from(varint(&mut buf)).expect("a length");
                let (body, rest) = buf.split_at(len);
                out.push((key >> 3, Some(body), 0));
                buf = rest;
            }
            wire => panic!("descriptor wire type {wire}: not a varint or length-delimited field"),
        }
    }
    out
}

pub fn varint(buf: &mut &[u8]) -> u64 {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = buf[0];
        *buf = &buf[1..];
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return value;
        }
    }
    panic!("a varint longer than 64 bits in the descriptor set")
}

/// The length-delimited fields of `buf` numbered `number`.
pub fn messages(buf: &[u8], number: u64) -> impl Iterator<Item = &[u8]> {
    fields(buf)
        .into_iter()
        .filter(move |(n, _, _)| *n == number)
        .filter_map(|(_, body, _)| body)
}

pub fn text(buf: &[u8], number: u64) -> String {
    messages(buf, number)
        .last()
        .map(|b| String::from_utf8(b.to_vec()).expect("a UTF-8 name"))
        .unwrap_or_default()
}

/// The varint fields of `buf` numbered `number`.
pub fn varints(buf: &[u8], number: u64) -> impl Iterator<Item = u64> + '_ {
    fields(buf)
        .into_iter()
        .filter(move |(n, body, _)| *n == number && body.is_none())
        .map(|(_, _, v)| v)
}

/// The `FileDescriptorProto` of `path` (field 1 of the set, name 1).
pub fn file<'a>(set: &'a [u8], path: &str) -> &'a [u8] {
    messages(set, 1)
        .find(|file| text(file, 1) == path)
        .unwrap_or_else(|| panic!("{path} is not in the descriptor set"))
}
