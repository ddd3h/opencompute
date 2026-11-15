//! Length-prefixed postcard framing: `u32 big-endian length` + postcard bytes.
//!
//! The length is validated against `max_len` *before* any allocation, so a hostile peer
//! cannot make us allocate more than the limit.

use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Framing errors.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// Underlying I/O failure or EOF.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Frame larger than the allowed maximum.
    #[error("frame of {0} bytes exceeds limit {1}")]
    TooLarge(usize, usize),
    /// Payload is not a valid message.
    #[error("decode: {0}")]
    Decode(#[from] postcard::Error),
    /// Peer closed the stream between frames.
    #[error("stream closed")]
    Closed,
}

/// Serialise `msg` into one frame (header + body).
pub fn encode<T: Serialize>(msg: &T, max_len: usize) -> Result<Vec<u8>, CodecError> {
    let body = postcard::to_stdvec(msg)?;
    if body.len() > max_len {
        return Err(CodecError::TooLarge(body.len(), max_len));
    }
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

/// Write one frame.
pub async fn write_frame<W, T>(w: &mut W, msg: &T, max_len: usize) -> Result<(), CodecError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let buf = encode(msg, max_len)?;
    w.write_all(&buf).await?;
    w.flush().await?;
    Ok(())
}

/// Read one frame. Returns [`CodecError::Closed`] on clean EOF at a frame boundary.
pub async fn read_frame<R, T>(r: &mut R, max_len: usize) -> Result<T, CodecError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut hdr = [0u8; 4];
    match r.read_exact(&mut hdr).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Err(CodecError::Closed),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_be_bytes(hdr) as usize;
    if len > max_len {
        return Err(CodecError::TooLarge(len, max_len));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await?;
    Ok(postcard::from_bytes(&body)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;

    #[tokio::test]
    async fn roundtrip() {
        let (mut a, mut b) = tokio::io::duplex(1024);
        let msg = ClientMessage::Heartbeat(Heartbeat {
            running: vec![(TaskId(1), 2)],
        });
        write_frame(&mut a, &msg, MAX_FRAME_LEN).await.unwrap();
        let got: ClientMessage = read_frame(&mut b, MAX_FRAME_LEN).await.unwrap();
        match got {
            ClientMessage::Heartbeat(h) => assert_eq!(h.running, vec![(TaskId(1), 2)]),
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn oversize_header_rejected_without_alloc() {
        let (mut a, mut b) = tokio::io::duplex(64);
        a.write_all(&u32::MAX.to_be_bytes()).await.unwrap();
        let r: Result<ClientMessage, _> = read_frame(&mut b, 1024).await;
        assert!(matches!(r, Err(CodecError::TooLarge(_, 1024))));
    }

    #[tokio::test]
    async fn garbage_is_decode_error_and_eof_is_closed() {
        let (mut a, mut b) = tokio::io::duplex(64);
        a.write_all(&3u32.to_be_bytes()).await.unwrap();
        a.write_all(&[0xff, 0xff, 0xff]).await.unwrap();
        let r: Result<ClientMessage, _> = read_frame(&mut b, 1024).await;
        assert!(matches!(r, Err(CodecError::Decode(_))));
        drop(a);
        let r: Result<ClientMessage, _> = read_frame(&mut b, 1024).await;
        assert!(matches!(r, Err(CodecError::Closed)));
    }

    #[test]
    fn encode_rejects_oversize() {
        let m = ServerMessage::Shutdown {
            reason: "x".repeat(100),
        };
        assert!(matches!(encode(&m, 10), Err(CodecError::TooLarge(_, 10))));
    }
}
