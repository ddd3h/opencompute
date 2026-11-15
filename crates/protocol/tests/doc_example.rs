//! Pins the byte-level framed `Heartbeat` example shown in `docs/PROTOCOL.md`.
//!
//! Run with: `cargo test -p opencompute-protocol --test doc_example`.

use opencompute_protocol::codec::{encode, read_frame, write_frame};
use opencompute_protocol::{ClientMessage, Heartbeat, MAX_FRAME_LEN, TaskId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// One complete frame: `u32` big-endian body length, then the postcard body.
///
/// Body breakdown (4 bytes):
/// * `0x02` – `ClientMessage` variant index 2 (`Heartbeat`), postcard varint
/// * `0x01` – `running.len()` == 1, postcard varint
/// * `0x01` – `TaskId(1)`, postcard varint (`u64`)
/// * `0x02` – `attempt` == 2, postcard varint (`u32`)
const HEARTBEAT_FRAME: [u8; 8] = [0x00, 0x00, 0x00, 0x04, 0x02, 0x01, 0x01, 0x02];

fn heartbeat() -> ClientMessage {
    ClientMessage::Heartbeat(Heartbeat {
        running: vec![(TaskId(1), 2)],
    })
}

#[test]
fn encoded_heartbeat_matches_documented_bytes() {
    let frame = encode(&heartbeat(), MAX_FRAME_LEN).expect("heartbeat fits in one frame");
    assert_eq!(frame.as_slice(), &HEARTBEAT_FRAME[..]);
}

#[tokio::test]
async fn documented_bytes_decode_as_heartbeat() {
    let (mut writer, mut reader) = tokio::io::duplex(HEARTBEAT_FRAME.len());
    writer
        .write_all(&HEARTBEAT_FRAME)
        .await
        .expect("documented bytes are writable");

    let got: ClientMessage = read_frame(&mut reader, MAX_FRAME_LEN)
        .await
        .expect("documented bytes form one valid frame");

    match got {
        ClientMessage::Heartbeat(h) => assert_eq!(h.running, vec![(TaskId(1), 2)]),
        other => panic!("expected Heartbeat, decoded {other:?}"),
    }
}

#[tokio::test]
async fn encoder_and_framer_produce_the_same_bytes() {
    let (mut writer, mut reader) = tokio::io::duplex(HEARTBEAT_FRAME.len());
    write_frame(&mut writer, &heartbeat(), MAX_FRAME_LEN)
        .await
        .expect("write_frame succeeds");

    let mut framed = vec![0u8; HEARTBEAT_FRAME.len()];
    reader
        .read_exact(&mut framed)
        .await
        .expect("read back the frame");

    assert_eq!(framed, HEARTBEAT_FRAME);
}
