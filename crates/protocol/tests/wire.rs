//! Wire-level integration tests for `opencompute-protocol`: postcard round-trips of every
//! message variant, id hex parsing edge cases, and the framing size limits.

use std::fmt::Debug;
use std::io::ErrorKind;

use opencompute_protocol::codec::{self, CodecError};
use opencompute_protocol::control::{
    ControlRequest, ControlResponse, JobInfo, NodeInfo, ServerInfo, ServerStats, StatefulInfo,
    SubmitJob, SubmitStateful,
};
use opencompute_protocol::{
    CancelTask, ClientMessage, ExecLimits, Heartbeat, JoinAccepted, JoinRequest, LeaveNetwork,
    MAX_FRAME_LEN, ModuleHash, ModuleRequest, ModuleTransfer, NodeCapabilities, NodeId,
    PROTOCOL_VERSION, PartBoundary, PartFailed, PartInit, PartRelease, PartStep, ServerMessage,
    TaskAssignment, TaskFailed, TaskId, TaskKind, TaskResult, TaskStarted, digest,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

fn node_caps() -> NodeCapabilities {
    NodeCapabilities {
        total_cores: 16,
        allowed_cores: 4,
        max_concurrent_tasks: 4,
        allowed_memory_bytes: 1 << 30,
        total_memory_bytes: 16 << 30,
        os: "linux".into(),
    }
}

fn job_info() -> JobInfo {
    JobInfo {
        job_id: opencompute_protocol::JobId(7),
        total: 3,
        pending: 1,
        active: 1,
        completed: 1,
        failed: 0,
        cancelled: 0,
        retries: 2,
        done: false,
        tasks: vec![(TaskId(10), "active".into()), (TaskId(11), "done".into())],
    }
}

fn postcard_roundtrip<T>(value: &T, label: &str)
where
    T: Serialize + DeserializeOwned + Debug,
{
    let bytes = postcard::to_stdvec(value).expect("postcard encode");
    let back: T = postcard::from_bytes(&bytes).expect("postcard decode");
    // Encoding the decoded value must reproduce the exact same bytes; this pins the wire
    // format (variant tag + fields) without requiring `PartialEq` on the message structs.
    assert_eq!(
        postcard::to_stdvec(&back).expect("postcard re-encode"),
        bytes,
        "re-encoded bytes differ for {label}"
    );
    assert_eq!(
        format!("{value:?}"),
        format!("{back:?}"),
        "decoded value differs for {label}"
    );
}

async fn frame_roundtrip<T>(value: &T, label: &str)
where
    T: Serialize + DeserializeOwned + Debug,
{
    let frame = codec::encode(value, MAX_FRAME_LEN).expect("encode frame");
    assert!(frame.len() > 4, "frame has no body for {label}");
    assert_eq!(
        u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize,
        frame.len() - 4,
        "big-endian header mismatch for {label}"
    );
    let mut reader: &[u8] = &frame;
    let back: T = codec::read_frame(&mut reader, MAX_FRAME_LEN)
        .await
        .expect("read frame");
    assert!(reader.is_empty(), "frame not fully consumed for {label}");
    assert_eq!(
        format!("{value:?}"),
        format!("{back:?}"),
        "framed value differs for {label}"
    );
}

#[test]
fn postcard_roundtrip_client_messages() {
    let hash = ModuleHash::of(b"module");
    let messages = [
        (
            "Join",
            ClientMessage::Join(JoinRequest {
                peer_addr: vec![],
                protocol_version: PROTOCOL_VERSION,
                token: [0xab; 16],
                name: "worker-a".into(),
                capabilities: node_caps(),
            }),
        ),
        ("Capabilities", ClientMessage::Capabilities(node_caps())),
        (
            "Heartbeat",
            ClientMessage::Heartbeat(Heartbeat {
                running: vec![(TaskId(1), 1), (TaskId(2), 3)],
            }),
        ),
        (
            "TaskStarted",
            ClientMessage::TaskStarted(TaskStarted {
                task_id: TaskId(5),
                attempt: 2,
            }),
        ),
        (
            "TaskResult",
            ClientMessage::TaskResult(TaskResult {
                task_id: TaskId(5),
                attempt: 2,
                output: vec![1, 2, 3, 4],
                output_hash: digest(&[1, 2, 3, 4]),
                elapsed_ms: 42,
            }),
        ),
        (
            "TaskFailed",
            ClientMessage::TaskFailed(TaskFailed {
                task_id: TaskId(5),
                attempt: 2,
                error: "boom".into(),
            }),
        ),
        (
            "ModuleRequest",
            ClientMessage::ModuleRequest(ModuleRequest { hash }),
        ),
        (
            "Leave",
            ClientMessage::Leave(LeaveNetwork {
                reason: "bye".into(),
            }),
        ),
        (
            "PartBoundary",
            ClientMessage::PartBoundary(PartBoundary {
                job_id: opencompute_protocol::JobId(3),
                part: 1,
                epoch: 4,
                step: 100,
                lo: vec![9; 32],
                hi: vec![8; 32],
                snapshot: Some(vec![7; 64]),
                hash: digest(b"lo-hi"),
            }),
        ),
        (
            "PartFailed",
            ClientMessage::PartFailed(PartFailed {
                job_id: opencompute_protocol::JobId(3),
                part: 1,
                epoch: 4,
                error: "step failed".into(),
            }),
        ),
    ];
    assert_eq!(messages.len(), 10, "every ClientMessage variant is covered");
    for (label, msg) in &messages {
        postcard_roundtrip(msg, label);
    }
}

#[test]
fn postcard_roundtrip_server_messages() {
    let hash = ModuleHash::of(b"module");
    let messages = [
        (
            "JoinAccepted",
            ServerMessage::JoinAccepted(JoinAccepted {
                protocol_version: PROTOCOL_VERSION,
                node_id: NodeId([0x11; 32]),
                heartbeat_interval_ms: 5_000,
            }),
        ),
        (
            "JoinRejected",
            ServerMessage::JoinRejected {
                reason: "bad token".into(),
            },
        ),
        (
            "Assign",
            ServerMessage::Assign(TaskAssignment {
                job_id: opencompute_protocol::JobId(9),
                task_id: TaskId(12),
                attempt: 1,
                kind: TaskKind::Wasm(hash),
                input: vec![4, 5, 6],
                input_hash: digest(&[4, 5, 6]),
                timeout_ms: 30_000,
                limits: ExecLimits {
                    max_memory_bytes: 64 << 20,
                    max_fuel: 1_000_000,
                },
            }),
        ),
        (
            "ModuleTransfer",
            ServerMessage::ModuleTransfer(ModuleTransfer {
                hash,
                total_len: 3,
                offset: 1,
                data: vec![0xde, 0xad],
            }),
        ),
        ("ModuleNotFound", ServerMessage::ModuleNotFound(hash)),
        (
            "CancelTask",
            ServerMessage::CancelTask(CancelTask {
                task_id: TaskId(12),
                attempt: 1,
            }),
        ),
        (
            "Shutdown",
            ServerMessage::Shutdown {
                reason: "maintenance".into(),
            },
        ),
        (
            "PartInit",
            ServerMessage::PartInit(PartInit {
                direct: false,
                peers: vec![],
                steps: 10,
                checkpoint_every: 5,
                job_id: opencompute_protocol::JobId(3),
                part: 2,
                epoch: 1,
                kernel: "diffusion".into(),
                params: vec![1, 2, 3],
                restore: Some(vec![9; 16]),
                step: 50,
            }),
        ),
        (
            "PartStep",
            ServerMessage::PartStep(PartStep {
                job_id: opencompute_protocol::JobId(3),
                part: 2,
                epoch: 1,
                step: 51,
                halo_lo: Some(vec![1; 8]),
                halo_hi: None,
                snapshot: true,
            }),
        ),
        (
            "PartRelease",
            ServerMessage::PartRelease(PartRelease {
                below_epoch: 1,
                job_id: opencompute_protocol::JobId(3),
            }),
        ),
    ];
    assert_eq!(messages.len(), 10, "every ServerMessage variant is covered");
    for (label, msg) in &messages {
        postcard_roundtrip(msg, label);
    }
}

#[test]
fn postcard_roundtrip_control_requests() {
    let job = opencompute_protocol::JobId(21);
    let requests = [
        ("Info", ControlRequest::Info),
        ("Nodes", ControlRequest::Nodes),
        ("Jobs", ControlRequest::Jobs),
        ("Status", ControlRequest::Status(job)),
        (
            "UploadModule",
            ControlRequest::UploadModule(vec![0, 1, 2, 3]),
        ),
        (
            "Submit",
            ControlRequest::Submit(SubmitJob {
                kind: TaskKind::Builtin("prime_count".into()),
                inputs: vec![vec![1], vec![]],
                timeout_ms: 60_000,
                max_attempts: 3,
                limits: ExecLimits::default(),
            }),
        ),
        (
            "Results",
            ControlRequest::Results {
                job,
                wait_ms: 1_500,
            },
        ),
        ("Cancel", ControlRequest::Cancel(job)),
        (
            "SubmitStateful",
            ControlRequest::SubmitStateful(SubmitStateful {
                direct: false,
                parts_per_node: 0,
                kernel: "lb多的".into(),
                params: vec![vec![1, 2], vec![3, 4, 5]],
                steps: 1_000,
                checkpoint_every: 25,
            }),
        ),
        (
            "StatefulResult",
            ControlRequest::StatefulResult {
                job,
                wait_ms: 2_500,
            },
        ),
        ("Diag", ControlRequest::Diag),
    ];
    assert_eq!(
        requests.len(),
        11,
        "every ControlRequest variant is covered"
    );
    for (label, req) in &requests {
        postcard_roundtrip(req, label);
    }
}

#[test]
fn postcard_roundtrip_control_responses() {
    let hash = ModuleHash::of(b"module");
    let job = opencompute_protocol::JobId(21);
    let responses = [
        ("Error", ControlResponse::Error("nope".into())),
        (
            "Info",
            ControlResponse::Info(ServerInfo {
                node_id: NodeId([0x22; 32]),
                ticket: "oc1abcdef".into(),
                protocol_version: PROTOCOL_VERSION,
            }),
        ),
        (
            "Nodes",
            ControlResponse::Nodes(vec![NodeInfo {
                node_id: NodeId([0x33; 32]),
                name: "node-a".into(),
                capabilities: node_caps(),
                assigned: 2,
                completed: 100,
                last_seen_ms: 250,
                local: true,
            }]),
        ),
        ("Jobs", ControlResponse::Jobs(vec![job_info(), job_info()])),
        ("Status", ControlResponse::Status(job_info())),
        ("ModuleStored", ControlResponse::ModuleStored(hash)),
        ("Submitted", ControlResponse::Submitted(job)),
        (
            "Results",
            ControlResponse::Results {
                job: job_info(),
                results: vec![Some(Ok(vec![1, 2])), Some(Err("failed".into())), None],
            },
        ),
        ("StatefulSubmitted", ControlResponse::StatefulSubmitted(job)),
        (
            "Stateful",
            ControlResponse::Stateful {
                final_params: vec![],
                info: StatefulInfo {
                    direct: false,
                    repartitions: 0,
                    job_id: job,
                    parts: 2,
                    step: 40,
                    steps: 100,
                    checkpoint_step: Some(25),
                    rollbacks: 1,
                    state: "Running".into(),
                    placement: vec![Some(NodeId([0x44; 32])), None],
                },
                final_state: Some(vec![vec![1, 2, 3], vec![]]),
            },
        ),
        (
            "Diag",
            ControlResponse::Diag(ServerStats {
                halos_relayed: 0,
                uptime_s: 3_600,
                nodes: 2,
                jobs: 5,
                nodes_lost: 1,
                rollbacks: 1,
                saves: 4,
                restored_jobs: 2,
            }),
        ),
        ("Ok", ControlResponse::Ok),
    ];
    assert_eq!(
        responses.len(),
        12,
        "every ControlResponse variant is covered"
    );
    for (label, resp) in &responses {
        postcard_roundtrip(resp, label);
    }
}

#[tokio::test]
async fn frames_roundtrip_through_codec() {
    let hash = ModuleHash::of(b"module");
    let clients = [
        ClientMessage::Join(JoinRequest {
            peer_addr: vec![],
            protocol_version: PROTOCOL_VERSION,
            token: [1; 16],
            name: "w".into(),
            capabilities: node_caps(),
        }),
        ClientMessage::Heartbeat(Heartbeat { running: vec![] }),
        ClientMessage::ModuleRequest(ModuleRequest { hash }),
        ClientMessage::Leave(LeaveNetwork {
            reason: "done".into(),
        }),
    ];
    for (i, msg) in clients.iter().enumerate() {
        frame_roundtrip(msg, &format!("ClientMessage #{i}")).await;
    }

    let servers = [
        ServerMessage::JoinRejected {
            reason: "no".into(),
        },
        ServerMessage::ModuleNotFound(hash),
        ServerMessage::Assign(TaskAssignment {
            job_id: opencompute_protocol::JobId(1),
            task_id: TaskId(2),
            attempt: 0,
            kind: TaskKind::Builtin("sleep_square".into()),
            input: vec![1; 10],
            input_hash: digest(&[1; 10]),
            timeout_ms: 1_000,
            limits: ExecLimits::default(),
        }),
    ];
    for (i, msg) in servers.iter().enumerate() {
        frame_roundtrip(msg, &format!("ServerMessage #{i}")).await;
    }

    // Two frames back to back on one stream must both be readable.
    let a = ServerMessage::Shutdown {
        reason: "first".into(),
    };
    let b = ServerMessage::Shutdown {
        reason: "second".into(),
    };
    let mut stream = codec::encode(&a, MAX_FRAME_LEN).unwrap();
    stream.extend_from_slice(&codec::encode(&b, MAX_FRAME_LEN).unwrap());
    let mut reader: &[u8] = &stream;
    let got_a: ServerMessage = codec::read_frame(&mut reader, MAX_FRAME_LEN).await.unwrap();
    let got_b: ServerMessage = codec::read_frame(&mut reader, MAX_FRAME_LEN).await.unwrap();
    assert!(reader.is_empty());
    assert_eq!(format!("{got_a:?}"), format!("{a:?}"));
    assert_eq!(format!("{got_b:?}"), format!("{b:?}"));
}

#[test]
fn hex_parsing_edge_cases() {
    let hash = ModuleHash::of(b"hello world");
    assert_eq!(ModuleHash::from_hex(&hash.to_string()), Some(hash));
    assert_eq!(
        NodeId::from_hex(&NodeId([0x5a; 32]).to_string()),
        Some(NodeId([0x5a; 32]))
    );

    // All-zero and all-ones, plus uppercase hex (accepted by `u8::from_str_radix`).
    assert_eq!(
        ModuleHash::from_hex(&"0".repeat(64)),
        Some(ModuleHash([0; 32]))
    );
    assert_eq!(
        ModuleHash::from_hex(&"f".repeat(64)),
        Some(ModuleHash([0xff; 32]))
    );
    assert_eq!(
        ModuleHash::from_hex(&"AB".repeat(32)),
        Some(ModuleHash([0xab; 32]))
    );

    // Length edge cases: empty, one short, one long.
    assert_eq!(ModuleHash::from_hex(""), None);
    assert_eq!(ModuleHash::from_hex(&"0".repeat(63)), None);
    assert_eq!(ModuleHash::from_hex(&"0".repeat(65)), None);
    assert_eq!(NodeId::from_hex(&"é".repeat(16)), None); // 32 bytes, not 64

    // Non-hex characters, `0x` prefix and whitespace are rejected.
    assert_eq!(ModuleHash::from_hex(&"g".repeat(64)), None);
    assert_eq!(ModuleHash::from_hex(&format!("0x{}", "0".repeat(62))), None);
    assert_eq!(NodeId::from_hex(&format!(" {}", "0".repeat(63))), None);
    assert_eq!(NodeId::from_hex(&format!("{} ", "0".repeat(63))), None);

    // 64 bytes that are not ASCII must not panic while slicing and must be rejected.
    assert_eq!(NodeId::from_hex(&"é".repeat(32)), None);
    assert_eq!(ModuleHash::from_hex(&"é".repeat(32)), None);

    // Display is lowercase, zero padded; short() is the first four bytes.
    assert_eq!(ModuleHash([0x0a; 32]).to_string(), "0a".repeat(32));
    assert_eq!(NodeId([0xff; 32]).to_string(), "ff".repeat(32));
    let short_node = NodeId({
        let mut bytes = [0u8; 32];
        bytes[..4].copy_from_slice(&[0x01, 0x02, 0x03, 0x04]);
        bytes
    });
    assert_eq!(short_node.short(), "01020304");
    assert_eq!(short_node.short().len(), 8);
}

/// Build a `ControlRequest::UploadModule` whose postcard body is exactly `target` bytes.
///
/// postcard encodes the variant tag (1 byte) then the `Vec<u8>` as a length varint plus one
/// byte per element, so the body length is `1 + varint_len(data) + data`.
fn upload_module_exact(target: usize) -> (ControlRequest, Vec<u8>) {
    for varint_len in 1..=5usize {
        let Some(data_len) = target.checked_sub(1 + varint_len) else {
            continue;
        };
        let msg = ControlRequest::UploadModule(vec![0u8; data_len]);
        let body = postcard::to_stdvec(&msg).expect("encode UploadModule");
        if body.len() == target {
            return (msg, body);
        }
    }
    panic!("cannot build an UploadModule body of exactly {target} bytes");
}

async fn assert_read_frame_boundary(target: usize) {
    let (msg, body) = upload_module_exact(target);
    assert_eq!(body.len(), target);

    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);

    // len == max is accepted.
    let mut reader: &[u8] = &frame;
    let decoded: ControlRequest = codec::read_frame(&mut reader, target)
        .await
        .unwrap_or_else(|e| panic!("len == max ({target}) must be accepted, got {e}"));
    assert_eq!(format!("{decoded:?}"), format!("{msg:?}"));
    assert!(reader.is_empty());

    // len == max + 1 is rejected before any body is read.
    let mut oversize = Vec::from((target as u32 + 1).to_be_bytes());
    oversize.extend_from_slice(&body);
    let mut reader: &[u8] = &oversize;
    match codec::read_frame::<_, ControlRequest>(&mut reader, target).await {
        Err(CodecError::TooLarge(len, max)) => {
            assert_eq!(len, target + 1);
            assert_eq!(max, target);
        }
        other => panic!("expected TooLarge for len == max + 1, got {other:?}"),
    }

    // A frame body of exactly `max` must also be rejected at `max - 1`.
    let mut reader: &[u8] = &frame;
    match codec::read_frame::<_, ControlRequest>(&mut reader, target - 1).await {
        Err(CodecError::TooLarge(len, max)) => {
            assert_eq!(len, target);
            assert_eq!(max, target - 1);
        }
        other => panic!("expected TooLarge for len == max at max - 1, got {other:?}"),
    }
}

#[tokio::test]
async fn read_frame_max_len_boundary() {
    assert_read_frame_boundary(1_024).await;
    // The real protocol limit: a full-size frame is accepted, one byte more is not.
    assert_read_frame_boundary(MAX_FRAME_LEN).await;
}

#[test]
fn encode_max_len_boundary() {
    let target = 2_048;
    let (msg, _) = upload_module_exact(target);

    let encoded = codec::encode(&msg, target).expect("body of exactly max must be accepted");
    assert_eq!(encoded.len(), 4 + target);
    assert_eq!(
        u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]) as usize,
        target
    );

    match codec::encode(&msg, target - 1) {
        Err(CodecError::TooLarge(len, max)) => {
            assert_eq!(len, target);
            assert_eq!(max, target - 1);
        }
        other => panic!("expected TooLarge at max - 1, got {other:?}"),
    }
}

#[tokio::test]
async fn read_frame_truncated_body_is_io_error() {
    let body = postcard::to_stdvec(&ClientMessage::Leave(LeaveNetwork {
        reason: "bye".into(),
    }))
    .unwrap();

    // Header promises the full body, but two bytes are missing.
    let mut truncated = Vec::new();
    truncated.extend_from_slice(&(body.len() as u32).to_be_bytes());
    truncated.extend_from_slice(&body[..body.len() - 2]);
    let mut reader: &[u8] = &truncated;
    match codec::read_frame::<_, ClientMessage>(&mut reader, MAX_FRAME_LEN).await {
        Err(CodecError::Io(e)) => assert_eq!(e.kind(), ErrorKind::UnexpectedEof),
        other => panic!("expected Io(UnexpectedEof) for a truncated body, got {other:?}"),
    }

    // Header only, no body bytes at all.
    let header_only = Vec::from((body.len() as u32).to_be_bytes());
    let mut reader: &[u8] = &header_only;
    match codec::read_frame::<_, ClientMessage>(&mut reader, MAX_FRAME_LEN).await {
        Err(CodecError::Io(e)) => assert_eq!(e.kind(), ErrorKind::UnexpectedEof),
        other => panic!("expected Io(UnexpectedEof) for a missing body, got {other:?}"),
    }
}
