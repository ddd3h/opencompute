//! Tests for [`opencompute_client::run_session`] using an in-memory `tokio::io::duplex` peer
//! that speaks the wire protocol via `opencompute_protocol::codec`.
//!
//! Each test drives one session to its end and asserts both the messages the worker sent and the
//! returned [`SessionEnd`]. Heartbeats are skipped by the helpers because the session ticker fires
//! immediately on start.

use opencompute_client::{ClientConfig, SessionEnd, Worker, run_session};
use opencompute_protocol::codec::{read_frame, write_frame};
use opencompute_protocol::{
    ClientMessage, ExecLimits, JobId, JoinAccepted, MAX_FRAME_LEN, MAX_STATE_FRAME_LEN, ModuleHash,
    NodeId, PROTOCOL_VERSION, ServerMessage, TaskAssignment, TaskId, TaskKind, digest,
};
use tokio::io::DuplexStream;
use tokio::sync::watch;
use tokio::task::JoinHandle;

const TOKEN: [u8; 16] = [0x5A; 16];

/// A running session plus the server end of the duplex stream.
struct Session {
    server: DuplexStream,
    task: JoinHandle<SessionEnd>,
    stop: watch::Sender<bool>,
}

/// Build a real worker through `ClientConfig::detect` + `Worker::new` and spawn `run_session`.
fn start(name: &str) -> Session {
    // 64 KiB is plenty: every message in these tests is small.
    let (client_side, server) = tokio::io::duplex(1 << 16);
    let (rx, tx) = tokio::io::split(client_side);
    let mut cfg = ClientConfig::detect(name);
    cfg.allowed_cores = 1;
    cfg.max_concurrent_tasks = 1;
    let worker = Worker::new(&cfg).expect("build worker");
    let (stop, stop_rx) = watch::channel(false);
    let task = tokio::spawn(run_session(rx, tx, TOKEN, worker, stop_rx));
    Session { server, task, stop }
}

/// Read the worker's `Join`, check it, and accept it.
async fn handshake(server: &mut DuplexStream) {
    let msg = read_frame::<_, ClientMessage>(server, MAX_FRAME_LEN)
        .await
        .expect("join frame");
    match msg {
        ClientMessage::Join(j) => {
            assert_eq!(j.protocol_version, PROTOCOL_VERSION);
            assert_eq!(j.token, TOKEN);
            assert!(!j.name.is_empty());
            j.capabilities.validate().expect("valid capabilities");
        }
        other => panic!("expected Join, got {other:?}"),
    }
    send(
        server,
        &ServerMessage::JoinAccepted(JoinAccepted {
            protocol_version: PROTOCOL_VERSION,
            node_id: NodeId([0x42; 32]),
            // Large, so no extra heartbeats arrive while a test is asserting.
            heartbeat_interval_ms: 60_000,
        }),
    )
    .await;
}

async fn send(server: &mut DuplexStream, msg: &ServerMessage) {
    write_frame(server, msg, MAX_FRAME_LEN)
        .await
        .expect("write server frame");
}

/// Next client message, ignoring the (immediate) heartbeat ticks.
async fn next(server: &mut DuplexStream) -> ClientMessage {
    loop {
        let msg = read_frame::<_, ClientMessage>(server, MAX_STATE_FRAME_LEN)
            .await
            .expect("decode client message");
        if !matches!(msg, ClientMessage::Heartbeat(_)) {
            return msg;
        }
    }
}

fn assignment(kind: TaskKind, input: &[u8], input_hash: [u8; 32], task: u64) -> TaskAssignment {
    TaskAssignment {
        job_id: JobId(1),
        task_id: TaskId(task),
        attempt: 0,
        kind,
        input: input.to_vec(),
        shared: None,
        input_hash,
        timeout_ms: 5_000,
        limits: ExecLimits::default(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_rejected_ends_session() {
    let mut s = start("reject");
    let msg = read_frame::<_, ClientMessage>(&mut s.server, MAX_FRAME_LEN)
        .await
        .expect("join frame");
    assert!(matches!(msg, ClientMessage::Join(_)), "{msg:?}");

    send(
        &mut s.server,
        &ServerMessage::JoinRejected {
            reason: "bad join token".into(),
        },
    )
    .await;

    let end = s.task.await.expect("session task");
    match end {
        SessionEnd::Rejected(r) => assert_eq!(r, "bad join token"),
        other => panic!("expected Rejected, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupted_input_hash_is_reported_as_task_failed() {
    let mut s = start("corrupt");
    handshake(&mut s.server).await;

    let input = b"return me unchanged".to_vec();
    let mut bad_hash = digest(&input);
    bad_hash[0] ^= 0xFF;
    let a = assignment(TaskKind::Builtin("echo".into()), &input, bad_hash, 7);
    send(&mut s.server, &ServerMessage::Assign(a)).await;

    match next(&mut s.server).await {
        ClientMessage::TaskFailed(f) => {
            assert_eq!(f.task_id, TaskId(7));
            assert_eq!(f.attempt, 0);
            assert!(
                f.error.contains("corrupted"),
                "unexpected error text: {}",
                f.error
            );
        }
        other => panic!("expected TaskFailed, got {other:?}"),
    }
    s.task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn echo_builtin_runs_and_result_hash_matches() {
    let mut s = start("echo");
    handshake(&mut s.server).await;

    let input = b"hello from the scheduler".to_vec();
    let a = assignment(TaskKind::Builtin("echo".into()), &input, digest(&input), 11);
    send(&mut s.server, &ServerMessage::Assign(a)).await;

    match next(&mut s.server).await {
        ClientMessage::TaskStarted(st) => {
            assert_eq!(st.task_id, TaskId(11));
            assert_eq!(st.attempt, 0);
        }
        other => panic!("expected TaskStarted, got {other:?}"),
    }
    match next(&mut s.server).await {
        ClientMessage::TaskResult(r) => {
            assert_eq!(r.task_id, TaskId(11));
            assert_eq!(r.attempt, 0);
            assert_eq!(r.output, input, "echo must return its input");
            assert_eq!(r.output_hash, digest(&input), "output hash must match");
        }
        other => panic!("expected TaskResult, got {other:?}"),
    }
    s.task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_wasm_module_is_requested_then_fails() {
    let mut s = start("wasm-missing");
    handshake(&mut s.server).await;

    let hash = ModuleHash([0xAB; 32]);
    let input = b"payload".to_vec();
    let a = assignment(TaskKind::Wasm(hash), &input, digest(&input), 13);
    send(&mut s.server, &ServerMessage::Assign(a)).await;

    match next(&mut s.server).await {
        ClientMessage::ModuleRequest(req) => assert_eq!(req.hash, hash),
        other => panic!("expected ModuleRequest, got {other:?}"),
    }

    send(&mut s.server, &ServerMessage::ModuleNotFound(hash)).await;
    match next(&mut s.server).await {
        ClientMessage::TaskFailed(f) => {
            assert_eq!(f.task_id, TaskId(13));
            assert_eq!(f.attempt, 0);
            assert!(
                f.error.contains("not found"),
                "unexpected error text: {}",
                f.error
            );
        }
        other => panic!("expected TaskFailed, got {other:?}"),
    }
    s.task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_signal_sends_leave_and_ends_stopped() {
    let mut s = start("stopper");
    handshake(&mut s.server).await;

    s.stop.send(true).expect("signal stop");
    match next(&mut s.server).await {
        ClientMessage::Leave(l) => assert!(!l.reason.is_empty()),
        other => panic!("expected Leave, got {other:?}"),
    }

    let end = s.task.await.expect("session task");
    assert!(matches!(end, SessionEnd::Stopped), "got {end:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_shutdown_ends_session() {
    let mut s = start("shutdown");
    handshake(&mut s.server).await;

    send(
        &mut s.server,
        &ServerMessage::Shutdown {
            reason: "server maintenance".into(),
        },
    )
    .await;

    let end = s.task.await.expect("session task");
    match end {
        SessionEnd::ServerShutdown(r) => assert_eq!(r, "server maintenance"),
        other => panic!("expected ServerShutdown, got {other:?}"),
    }
}
