//! Raw HTTP/2 frames (RFC 9113), for the tests that must do what a real
//! client never would: stay silent after the preface, or open more streams
//! than the server allows. Only what those tests need: the frame header,
//! the connection preface, a request's header block (HPACK, literal, no
//! Huffman) and the server's SETTINGS.

use std::io;
use std::net::SocketAddr;

use std::path::Path;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};

pub const HEADERS: u8 = 0x1;
pub const RST_STREAM: u8 = 0x3;
pub const SETTINGS: u8 = 0x4;
pub const PING: u8 = 0x6;
pub const GOAWAY: u8 = 0x7;

/// SETTINGS and PING: this frame acknowledges the peer's.
pub const ACK: u8 = 0x1;
/// HEADERS: the header block is whole in this frame.
pub const END_HEADERS: u8 = 0x4;

/// The SETTINGS parameter SETTINGS_MAX_CONCURRENT_STREAMS.
pub const MAX_CONCURRENT_STREAMS: u16 = 0x3;
/// The RST_STREAM code REFUSED_STREAM.
pub const REFUSED_STREAM: u32 = 0x7;

const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// One frame as read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub kind: u8,
    pub flags: u8,
    pub stream: u32,
    pub payload: Vec<u8>,
}

impl Frame {
    /// A SETTINGS frame's value for `id`, if it sets it.
    pub fn setting(&self, id: u16) -> Option<u32> {
        assert_eq!(self.kind, SETTINGS);
        self.payload.chunks_exact(6).find_map(|param| {
            (u16::from_be_bytes([param[0], param[1]]) == id)
                .then(|| u32::from_be_bytes([param[2], param[3], param[4], param[5]]))
        })
    }

    /// A RST_STREAM frame's error code.
    pub fn reset_code(&self) -> u32 {
        assert_eq!(self.kind, RST_STREAM);
        u32::from_be_bytes(self.payload[..4].try_into().unwrap())
    }
}

/// Connects to `addr` and says the client preface and an empty SETTINGS,
/// as every HTTP/2 client must first; nothing more.
pub async fn open(addr: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.expect("connects");
    greet(&mut stream).await;
    stream
}

/// The same over the Unix socket at `path`.
pub async fn open_uds(path: &Path) -> UnixStream {
    let mut stream = UnixStream::connect(path).await.expect("connects");
    greet(&mut stream).await;
    stream
}

async fn greet<S: AsyncWrite + Unpin>(stream: &mut S) {
    stream.write_all(PREFACE).await.unwrap();
    write(stream, SETTINGS, 0, 0, &[]).await.unwrap();
}

/// Writes one frame.
pub async fn write<S: AsyncWrite + Unpin>(
    stream: &mut S,
    kind: u8,
    flags: u8,
    id: u32,
    payload: &[u8],
) -> io::Result<()> {
    let length = u32::try_from(payload.len()).unwrap().to_be_bytes();
    let mut frame = vec![length[1], length[2], length[3], kind, flags];
    frame.extend_from_slice(&id.to_be_bytes());
    frame.extend_from_slice(payload);
    stream.write_all(&frame).await
}

/// The next frame, or `None` once the server has closed the connection
/// (an end of stream, or a reset).
pub async fn read<S: AsyncRead + Unpin>(stream: &mut S) -> Option<Frame> {
    let mut header = [0u8; 9];
    if stream.read_exact(&mut header).await.is_err() {
        return None;
    }
    let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
    let mut payload = vec![0u8; length];
    if stream.read_exact(&mut payload).await.is_err() {
        return None;
    }
    Some(Frame {
        kind: header[3],
        flags: header[4],
        stream: u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff,
        payload,
    })
}

/// A string literal, its length a 7-bit prefix (no Huffman).
fn literal(block: &mut Vec<u8>, text: &str) {
    assert!(text.len() < 127);
    block.push(text.len() as u8);
    block.extend_from_slice(text.as_bytes());
}

/// The header block of a gRPC call to `path` (RFC 7541: indexed names from
/// the static table, values literal and never indexed).
pub fn request(path: &str) -> Vec<u8> {
    let mut block = vec![
        0x83, // :method POST (static 3)
        0x86, // :scheme http (static 6)
        0x04, // :path (static 4), literal value
    ];
    literal(&mut block, path);
    block.push(0x01); // :authority (static 1), literal value
    literal(&mut block, "localhost");
    block.extend_from_slice(&[0x0f, 0x10]); // content-type (static 31)
    literal(&mut block, "application/grpc");
    block.push(0x00); // a literal name: te
    literal(&mut block, "te");
    literal(&mut block, "trailers");
    block
}
