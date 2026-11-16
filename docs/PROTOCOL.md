# OpenCompute wire protocol

This document describes protocol version **3** as implemented by `crates/protocol`, the
server (`crates/server`) and the worker (`crates/client`). It covers the transport handshake,
framing, every message, size limits, direct client↔client halo exchange and the local control
socket. The authoritative definitions live in:

| File | Contents |
|---|---|
| `crates/protocol/src/lib.rs` | version, ALPNs, size limits |
| `crates/protocol/src/ids.rs` | `JobId`, `TaskId`, `NodeId`, `ModuleHash` |
| `crates/protocol/src/message.rs` | worker and peer messages (`ClientMessage`, `ServerMessage`, `PeerMessage`) |
| `crates/protocol/src/control.rs` | control-socket messages |
| `crates/protocol/src/codec.rs` | `u32` big-endian length + postcard framing |
| `crates/client/src/peer.rs` | client↔client transport (ALPN `opencompute-peer/1`) |
| `crates/client/src/part.rs` | direct-mode part actor |
| `crates/server/src/stateful.rs` | stateful coordinator, watchdog and re-partitioning |

---

## 1. Versions and transport

* **Protocol version:** `PROTOCOL_VERSION = 4` (`u16`).
* **Worker ALPN:** `opencompute/5` (`b"opencompute/5"`), negotiated by QUIC between a client and
  the server.
* **Peer ALPN:** `opencompute-peer/1` (`b"opencompute-peer/1"`), negotiated by QUIC between two
  clients for direct halo exchange. It has no version field on the wire; its version is the ALPN
  string.
* **Transport:** [iroh](https://docs.rs/iroh) over QUIC. Each endpoint has an Ed25519 identity;
  QUIC/TLS authenticates both sides by key.
* **Worker streams:** exactly **one bidirectional stream per client↔server connection**. All worker
  messages travel on that stream in both directions. The protocol is independent of the stream
  type: the in-process worker uses `tokio::io::duplex`, tests use the same
  `AsyncRead + AsyncWrite` interface.
* **Peer streams:** a client dialling another client opens **one unidirectional stream per peer
  link** and writes `PeerMessage` frames on it. The receiver accepts any number of such streams
  on its endpoint (which is bound with `PEER_ALPN`).

The server's identity is pinned by the ticket, so a client dials the key it was given and cannot be
redirected to an impostor. The server learns the client's `NodeId` from the authenticated remote
key; `JoinRequest` carries no node id and cannot spoof one. Likewise, a peer halo is only
*consumed* when its authenticated sender is the node the server named as the neighbour for that
part (see §8.4); a stranger can connect and send, but its data is not used.

### 1.1 Ticket

The user-facing ticket is a string:

```
oc1<lowercase hex of postcard(Ticket { addr: EndpointAddr, token: [u8; 16] })>
```

* Prefix `oc1`.
* Hex part must have even length, be ASCII, and be at most 8192 characters.
* `addr.id` is the server's pinned EndpointId (converted to `NodeId`).
* `token` is the 16-byte join secret. It is persisted in `<data-dir>/token` (mode `0600`) and
  compared in constant time by the server.

The ticket is handed to clients out of band; it is not part of the framed protocol.

---

## 2. Framing

Every application message is one frame:

```
+----------------+--------------------------------+
| u32 BE length  | postcard body (length bytes)   |
+----------------+--------------------------------+
        4 bytes            N bytes
```

* The length prefix counts **only the postcard body**, not the 4-byte header.
* The body is encoded with [postcard](https://docs.rs/postcard) 1.1.
* The receiver validates `N` against its maximum **before allocating** the body buffer.
* The sender checks the encoded body length against its configured maximum before writing.
* Each frame is written with `write_all` and flushed. There are no checksums, compression,
  retransmissions or application-level ACKs at this layer; QUIC provides reliability, ordering and
  encryption for the lifetime of a connection.
* The same framing is used on client↔server streams, client↔client peer streams and the control
  socket, with different limits.

### 2.1 Postcard encoding rules relevant to this protocol

Postcard is a positional, compact binary format:

| Rust type | Wire encoding |
|---|---|
| `u16`, `u32`, `u64` | unsigned LEB128 varint |
| enum | variant index as a `u32` varint, followed by the variant payload |
| `bool` | one byte, `0x00` or `0x01` |
| `String` | varint byte length, then UTF-8 bytes |
| `Vec<T>` | varint element count, then each element |
| `Option<T>` | `0x00` for `None`, `0x01` followed by the value for `Some` |
| `[u8; N]` | exactly `N` raw bytes, no length prefix |
| struct | fields in declaration order, with no field names or tags |
| tuple / tuple struct | fields in order |

Consequences:

* Field **order**, enum variant **order** and struct layout are wire ABI. Reordering fields,
  inserting a variant in the middle, or changing integer width is an incompatible change.
* `[u8; 16]` (join token) and `[u8; 32]` (`NodeId`, `ModuleHash`, hashes) are raw fixed-size
  blobs.
* All numeric IDs on the wire are plain varints. The display forms (`job-7`, `task-42`, 64-character
  hex for keys/hashes) exist only for humans and logs.

### 2.2 Size limits

The limits are compiled into both ends; they are **not** negotiated on the wire. Changing them is
therefore a protocol change.

| Constant | Value | Meaning |
|---|---:|---|
| `MAX_FRAME_LEN` | 4 MiB (4 194 304 B) | Maximum pre-authentication frame: the client's `Join` and the server's `JoinRejected`. |
| `MAX_STATE_FRAME_LEN` | 256 MiB (268 435 456 B) | Maximum worker-protocol frame after a successful join (stateful snapshots can be large). |
| `MAX_MODULE_LEN` | 32 MiB (33 554 432 B) | Maximum WASM module accepted by server or client. |
| `MAX_PAYLOAD_LEN` | 2 MiB (2 097 152 B) | Maximum single task input or output, maximum each of `PartBoundary::lo` / `hi`, and maximum each stateful part parameter. |
| `MAX_PEER_ADDR_LEN` | 4096 B | Maximum serialised peer address in `JoinRequest::peer_addr` and `PeerInfo::addr` (declared in `message.rs`); also enforced when parsing a peer address. |
| `MODULE_CHUNK_LEN` | 1 MiB (1 048 576 B) | Chunk size used by `ModuleTransfer` (the last chunk may be shorter). |
| `CONTROL_MAX_FRAME` | 256 MiB | Maximum control-socket frame; equal to `MAX_STATE_FRAME_LEN`. |
| Peer frame limit | 16 MiB (16 777 216 B) | Limit used by the client for `PeerMessage` frames (`MAX_PEER_FRAME` in `crates/client/src/peer.rs`). |
| Peer mailbox | 4096 entries | Received halos buffered per worker (`MAX_MAILBOX_ENTRIES`); new halos are dropped when it is full. |
| `NodeCapabilities::os` | 128 bytes | Maximum accepted OS string (checked by `validate`). |
| Join node name | 64 characters | Truncated server-side after removing control characters. |

Practical notes:

* The client's generic send path allows up to `MAX_STATE_FRAME_LEN`, but the server reads the first
  client frame with `MAX_FRAME_LEN`. A `Join` larger than 4 MiB is therefore rejected by the
  receiver's frame check before decoding.
* `JoinRejected` is also written with `MAX_FRAME_LEN`.
* After `JoinAccepted`, both directions use `MAX_STATE_FRAME_LEN`.
* Peer snapshots are not sent as `PeerMessage`; they travel to the server in `PartProgress` and
  are bounded by `MAX_STATE_FRAME_LEN`. The 16 MiB peer limit only bounds halo blobs.

If a body exceeds the receiver's limit, the receiver returns `TooLarge` **without allocating** and
the connection/session is ended. See §7.

---

## 3. Handshake sequence

```
client                                              server
  |                                                    |
  |-- QUIC connect, ALPN "opencompute/5" ------------->|  (mutual TLS; server records remote NodeId)
  |-- open bidirectional stream ---------------------->|
  |-- ClientMessage::Join(JoinRequest) -------------->|  (server waits at most 10 s, max 4 MiB)
  |                                                    |  validate version, token, capabilities, peer_addr
  |<-- ServerMessage::JoinAccepted(JoinAccepted) ------|  (success)
  |    or ServerMessage::JoinRejected { reason } -----|  (failure, then close)
  |                                                    |
  |-- Heartbeat / TaskStarted / TaskResult / ... ----->|
  |<-- Assign / CancelTask / ModuleTransfer / ... -----|
```

1. **Connect.** The client dials the EndpointId from the ticket with ALPN `opencompute/5` and opens
   one bidirectional stream. The server accepts the stream and derives the peer `NodeId` from the
   authenticated QUIC identity.
2. **Join.** The client must send `ClientMessage::Join` as the **first frame**. The server reads it
   with a 10-second timeout and a 4 MiB frame limit.
3. **Validate.** The server checks, in order:
   * first message is `Join` (otherwise `JoinRejected`, reason `first message must be Join`);
   * `protocol_version == 4` (otherwise a version-mismatch reason);
   * `token` equals the server token in constant time (otherwise `invalid join token`);
   * `capabilities.validate()`:
     * `allowed_cores` in `1..=4096`;
     * `max_concurrent_tasks` in `1..=4096`;
     * `os.len() <= 128` bytes.
   * `peer_addr.len() <= MAX_PEER_ADDR_LEN` (otherwise `peer_addr too long`);
   * The name is sanitised: control characters are removed and at most 64 characters are kept.
     `allowed_memory_bytes`, `total_memory_bytes` and `total_cores` are not range-checked here.
4. **Register.** On success the server:
   * registers or re-registers the node keyed by the authenticated `NodeId` (a re-registration
     immediately re-queues any unfinished tasks of the previous session);
   * records `peer_addr` in its peer-address table (replacing a previous value) and removes the
     entry when that session ends;
   * replaces the previous connection for that node, if any, and best-effort sends it
     `Shutdown { reason: "replaced by a newer session" }`;
   * sends `JoinAccepted` with the server's protocol version, the authenticated `node_id`, and
     `heartbeat_interval_ms` (default **2000 ms**; server config may differ).
5. **Client accepts.** The client requires `JoinAccepted.protocol_version == 4`; a mismatch is
   treated as a rejection. It clamps `heartbeat_interval_ms` to `50..=60_000` ms, starts its
   heartbeat ticker and its reader loop.
6. **Reject.** On failure the server sends `JoinRejected { reason }`, waits up to 3 seconds for the
   peer to close (so the frame is not discarded), then closes the connection.
   * The client treats a rejection as fatal and does **not** retry.
   * A transport failure or an unexpected end after a successful join is retried by
     `run_client` with exponential back-off from 1 s to 30 s.
7. **Duplicate Join.** A second `Join` on an established session ends that session
   (`duplicate join`).

Liveness after the handshake:

* The client sends `Heartbeat` every negotiated interval. It also refreshes the server's
  `last_seen` implicitly through `TaskStarted`, `TaskResult`, `TaskFailed`, `PartBoundary` and
  `PartProgress`. `Capabilities`, `ModuleRequest`, `PartFailed` and `Leave` do not refresh it.
* The server polls every 100 ms. A node with `now - last_seen > heartbeat_timeout` (default 6 s) is
  declared dead: its connection entry and peer address are removed, a best-effort
  `Shutdown { reason: "heartbeat timeout" }` is sent, its unfinished tasks are re-queued, and any
  stateful job placed on it is rolled back. The CLI's `--heartbeat-timeout-secs` overrides the
  timeout and sets the requested interval to `timeout / 3` (minimum 200 ms).

---

## 4. Common types

### 4.1 Identifiers

| Type | Wire form | Meaning |
|---|---|---|
| `JobId(u64)` | varint | Job/batch identity. Display: `job-<n>`. |
| `TaskId(u64)` | varint | Task identity, unique across all jobs. Display: `task-<n>`. |
| `NodeId([u8; 32])` | raw 32 bytes | Authenticated iroh EndpointId (Ed25519 public key). Display: 64 lowercase hex chars. |
| `ModuleHash([u8; 32])` | raw 32 bytes | BLAKE3-256 of a WASM module, also used for integrity checks. Display: 64 lowercase hex chars. |

`digest(bytes)` is BLAKE3-256 and returns `[u8; 32]`.

### 4.2 Task kinds and limits

```rust
enum TaskKind {
    Builtin(String),   // variant 0
    Wasm(ModuleHash),  // variant 1
}
```

* `Builtin` names a function compiled into the client (`prime_count`, `sleep_square`, `echo`, `nbody_force` in the current
  runtime). An unknown name becomes a task failure.
* `Wasm` names a module by hash. If the client does not have it, it requests it from the server.

```rust
struct ExecLimits {
    max_memory_bytes: u64,
    max_fuel: u64,
}
```

`ExecLimits::default()` is 256 MiB of memory and 20 000 000 000 fuel. The client additionally caps
memory by the user's per-task share (`allowed_memory_bytes / max_concurrent_tasks`, at least 1 MiB)
and clamps the requested wall-clock timeout to at most 1 hour.

### 4.3 Capabilities

```rust
struct NodeCapabilities {
    total_cores: u32,
    allowed_cores: u32,
    max_concurrent_tasks: u32,
    allowed_memory_bytes: u64,
    total_memory_bytes: u64,
    os: String,
}
```

Offered resources. Sent in `JoinRequest` and may be updated later. The scheduler uses
`min(max_concurrent_tasks, allowed_cores)` (at least 1) as the node's task slots.

### 4.4 Enum variant indices

These indices are part of the wire format.

`ClientMessage`:

| Index | Variant |
|---:|---|
| 0 | `Join(JoinRequest)` |
| 1 | `Capabilities(NodeCapabilities)` |
| 2 | `Heartbeat(Heartbeat)` |
| 3 | `TaskStarted(TaskStarted)` |
| 4 | `TaskResult(TaskResult)` |
| 5 | `TaskFailed(TaskFailed)` |
| 6 | `ModuleRequest(ModuleRequest)` |
| 7 | `Leave(LeaveNetwork)` |
| 8 | `PartBoundary(PartBoundary)` |
| 9 | `PartFailed(PartFailed)` |
| 10 | `PartProgress(PartProgress)` |
| 11 | `BlobRequest(ModuleRequest)` |

`ServerMessage`:

| Index | Variant |
|---:|---|
| 0 | `JoinAccepted(JoinAccepted)` |
| 1 | `JoinRejected { reason }` |
| 2 | `Assign(TaskAssignment)` |
| 3 | `ModuleTransfer(ModuleTransfer)` |
| 4 | `ModuleNotFound(ModuleHash)` |
| 5 | `CancelTask(CancelTask)` |
| 6 | `Shutdown { reason }` |
| 7 | `PartInit(PartInit)` |
| 8 | `PartStep(PartStep)` |
| 9 | `PartRelease(PartRelease)` |
| 10 | `BlobTransfer(ModuleTransfer)` |
| 11 | `BlobNotFound(ModuleHash)` |

`PeerMessage` (client↔client, ALPN `opencompute-peer/1`):

| Index | Variant |
|---:|---|
| 0 | `Halo { job_id, dst_part, epoch, step, side, data }` |

### 4.5 Peer placement info

```rust
struct PeerInfo {
    node: NodeId,
    addr: Vec<u8>,
}
```

`node` is the node holding a part; `addr` is that node's serialised iroh `EndpointAddr`, or empty
if unknown. Addresses are produced by `opencompute_network::encode_addr` (postcard of
`EndpointAddr`) and parsed with `decode_addr`, which rejects empty input and input longer than
`MAX_PEER_ADDR_LEN`. `PeerInfo` appears only inside `PartInit::peers` (direct mode) and is not
otherwise exchanged.

---

## 5. Worker messages

All payload fields below are in declaration order; that order is the wire layout.

### 5.1 Client → server

#### `Join` / `JoinRequest` (variant 0)

| Field | Type | Semantics |
|---|---|---|
| `protocol_version` | `u16` | Must be 3. |
| `token` | `[u8; 16]` | Join secret from the ticket; compared in constant time. |
| `name` | `String` | Human-readable node name; control characters removed and truncated to 64 chars by the server. Informational only. |
| `capabilities` | `NodeCapabilities` | Offered resources; validated as in §3. |
| `peer_addr` | `Vec<u8>` | Serialised iroh `EndpointAddr` on which this client accepts peer connections for direct halo exchange. Empty = not reachable by peers. At most `MAX_PEER_ADDR_LEN`; longer is rejected (`peer_addr too long`). |

The address is stored by the server for the lifetime of the session and is what direct-mode
`PartInit::peers` is built from. It is **not** trusted for identity: the `NodeId` in `PeerInfo` is
the authenticated key.

Must be the first and only `Join` on a connection. A duplicate ends the session.

#### `Capabilities` (variant 1)

Carries a complete `NodeCapabilities` replacement. The server validates it with the same rules as
at join; invalid values are ignored. There is no acknowledgement. This does not refresh liveness.
It does not update `peer_addr` (that is only sent in `Join`).

#### `Heartbeat` (variant 2)

| Field | Type | Semantics |
|---|---|---|
| `running` | `Vec<(TaskId, u32)>` | Pairs of task and attempt currently executing. Sorted by the client. |

Sent every negotiated interval. It refreshes the server's liveness timestamp. The current server
does not otherwise use the `running` list; it is diagnostic/future use.

#### `TaskStarted` (variant 3)

| Field | Type | Semantics |
|---|---|---|
| `task_id` | `TaskId` | Task being executed. |
| `attempt` | `u32` | Attempt being executed; must match the current assignment. |

Marks the attempt as running and refreshes liveness. It is best-effort; a stale attempt is ignored
(the client does not retry it).

#### `TaskResult` (variant 4)

| Field | Type | Semantics |
|---|---|---|
| `task_id` | `TaskId` | Task identity. |
| `attempt` | `u32` | Attempt that produced the result. |
| `output` | `Vec<u8>` | Result bytes; at most `MAX_PAYLOAD_LEN`. |
| `output_hash` | `[u8; 32]` | BLAKE3-256 of `output`. |
| `elapsed_ms` | `u64` | Worker-side execution time, informational. |

The server checks `digest(output) == output_hash` and `output.len() <= MAX_PAYLOAD_LEN`. A failure of
either check is treated as a **failed attempt**, not a protocol error. Otherwise the result is
committed at most once; duplicates, results for an old attempt, results from a different node and
results after cancellation are dropped as stale.

#### `TaskFailed` (variant 5)

| Field | Type | Semantics |
|---|---|---|
| `task_id` | `TaskId` | Task identity. |
| `attempt` | `u32` | Attempt that failed. |
| `error` | `String` | Human-readable reason. |

Counts one failure against the task's `max_attempts`; if attempts remain the task is queued at the
front, otherwise the task becomes failed. Stale reports are ignored (they must not double-count).
Failures are produced by execution errors, panics (`task panicked`), oversized output, corrupt or
oversized input, module-not-found/bad-transfer, and unknown built-ins.

#### `ModuleRequest` (variant 6)

| Field | Type | Semantics |
|---|---|---|
| `hash` | `ModuleHash` | Wanted module. |

Sent when an assignment needs a WASM module the client does not have. While waiting, assignments
for that module are queued. The client sends at most one request per module for the currently queued
batch.

#### `Leave` (variant 7)

| Field | Type | Semantics |
|---|---|---|
| `reason` | `String` | Informational. |

Graceful departure. The server ends the session and re-queues the node's unfinished tasks. The
client may close the connection after flushing. The standard reason for user shutdown is
`user requested`.

#### `PartBoundary` (variant 8) — stateful, relayed mode

See §8.3.

#### `PartFailed` (variant 9) — stateful

See §8.1.

#### `PartProgress` (variant 10) — stateful, direct mode

| Field | Type | Semantics |
|---|---|---|
| `job_id` | `JobId` | Job. |
| `part` | `u32` | Part index. |
| `epoch` | `u32` | Epoch from the matching `PartInit`. |
| `step` | `u64` | Step the part has reached. |
| `snapshot` | `Option<Vec<u8>>` | Full state if this step is a checkpoint step or the final step. |

Sent by direct-mode part actors instead of per-step `PartBoundary`. It refreshes liveness. A part
sends a progress frame at least every 20 steps even without a snapshot, and always when it takes a
snapshot or reaches the final step. See §8.4 for the server's acceptance rules.

### 5.2 Server → client

#### `JoinAccepted` (variant 0)

| Field | Type | Semantics |
|---|---|---|
| `protocol_version` | `u16` | Server version (3). |
| `node_id` | `NodeId` | The authenticated node id the server recorded. |
| `heartbeat_interval_ms` | `u64` | Requested heartbeat period. Client clamps to 50..=60 000 ms. |

#### `JoinRejected` (variant 1)

Struct variant with one field:

| Field | Type | Semantics |
|---|---|---|
| `reason` | `String` | Human-readable rejection reason. |

Sent instead of `JoinAccepted`; the connection closes afterwards. The client does not retry a
rejection. Reason strings are informational and may change.

#### `Assign` / `TaskAssignment` (variant 2)

| Field | Type | Semantics |
|---|---|---|
| `job_id` | `JobId` | Owning job. |
| `task_id` | `TaskId` | Task identity. |
| `attempt` | `u32` | Attempt counter, starting at 1 and incremented on every dispatch. |
| `kind` | `TaskKind` | Built-in or WASM module. |
| `shared` | `Option<ModuleHash>` | Shared input blob (v5). |
| `input` | `Vec<u8>` | Task input; at most `MAX_PAYLOAD_LEN`. |
| `input_hash` | `[u8; 32]` | BLAKE3-256 of `input`. |
| `timeout_ms` | `u64` | Wall-clock budget. |
| `limits` | `ExecLimits` | Memory/fuel limits. |

The worker verifies the input size and hash before starting; a mismatch produces `TaskFailed`
(`input corrupted or oversized`). If the WASM module is missing, the assignment waits for a module
transfer, then for its shared blob (`BlobRequest` → `BlobTransfer` chunks, verified against the hash, cached per worker). For WASM, the guest input is then `u64` little-endian shared length, shared bytes, task input. Builtins receive shared bytes and task input separately. `nbody_force` takes raw little-endian f64 xyz positions in the shared blob, postcard `(u32 start, u32 end, f64 eps2)` as task input, and returns a postcard `Vec<f64>` of xyz accelerations. `Assign` and `CancelTask` share one ordered stream, so their relative order is preserved.

#### `ModuleTransfer` (variant 3)

| Field | Type | Semantics |
|---|---|---|
| `hash` | `ModuleHash` | Hash of the whole module. |
| `total_len` | `u64` | Total module length in bytes. |
| `offset` | `u64` | Offset of this chunk. |
| `data` | `Vec<u8>` | Chunk bytes; at most `MODULE_CHUNK_LEN`. |

The server splits the module into 1 MiB chunks in ascending offset order. Other messages may be
interleaved between chunks. The client accepts a chunk only if:

* it is waiting for this hash;
* `total_len` is consistent with the first chunk;
* `total_len <= MAX_MODULE_LEN`;
* `offset == bytes already assembled` (strictly sequential, no gaps or overlaps);
* `data.len() <= MODULE_CHUNK_LEN`;
* the assembled size does not exceed `total_len`.

Any violation discards the partial assembly and fails all assignments waiting for that module with
`TaskFailed`. Completion is implicit: when assembled length equals `total_len`, the client verifies
the BLAKE3 hash and loads the module. There is no chunk ACK or end marker.

#### `ModuleNotFound` (variant 4)

Payload is the requested `ModuleHash`. All assignments waiting for that module fail with
`module not found on server`.

#### `CancelTask` (variant 5)

| Field | Type | Semantics |
|---|---|---|
| `task_id` | `TaskId` | Task to cancel. |
| `attempt` | `u32` | Exact attempt to cancel. |

Sent when a job is cancelled or an attempt times out. The client records the exact
`(task_id, attempt)`, suppresses a later result/failure for it and removes it from the
module-wait queue. A task that is already running is **not pre-empted**: the runtime keeps going
until completion or its own timeout/fuel limit, but its result is discarded. A `CancelTask` for a
different (newer) attempt has no effect.

#### `Shutdown` (variant 6)

Struct variant with one field:

| Field | Type | Semantics |
|---|---|---|
| `reason` | `String` | Why the server is ending the session. |

Sent when the server shuts down, when a node is replaced by a newer session, when its outbound
queue is stalled, or after a heartbeat timeout. The server stops the writer after this message. The
client ends the session; `run_client` may reconnect with back-off unless the local user stopped it.

#### `PartInit`, `PartStep`, `PartRelease` — stateful

See §8.

---

## 6. Idempotency and task lifecycle

The unit of idempotency for ordinary tasks is **`(task_id, attempt)`**, together with the node
identity.

* Each task has one current `attempt`. It starts at 0 and is incremented for every dispatch; the
  first assignment is attempt 1.
* The server accepts a result/failure only if the task is not terminal, the attempt equals the
  current attempt, and the sending node is the one currently assigned. Everything else returns
  `Report::Stale` and is ignored.
* A successful result is committed **at most once**. Once the task is completed, further results
  are stale even if they carry the same attempt.
* A failure is counted **at most once**: after the task is re-queued or marked failed, a duplicate
  `TaskFailed` for the same attempt is stale.
* Node loss re-queues unfinished tasks **without** charging a failure. The next dispatch increments
  the attempt, so a late result from the previous node/attempt is ignored.
* A timeout charges one failure (reason `task timed out`), re-queues or fails the task according to
  `max_attempts`, and sends `CancelTask` for the expired attempt.
* `max_attempts` counts failures, not assignments. Node-loss re-queues do not consume attempts.
* Cancellation makes all non-terminal tasks of the job terminal (`Cancelled`) and sends
  `CancelTask` for in-flight attempts. Late reports are stale.
* The client also suppresses a result/failure for an exact cancelled `(task, attempt)`, so a race
  between completion and `CancelTask` does not resurrect the task.

Task states on the server: `Pending` → `Assigned` → `Running` → `Completed` / `Failed` /
`Cancelled`, with failures returning to `Pending` while attempts remain.

Because frames travel on one reliable ordered stream, there is no application-level retransmission.
The attempt matching exists for re-assignment, duplicate delivery and late messages, not for frame
loss.

---

## 7. Error handling

### 7.1 Framing and session errors

`CodecError` has four cases:

| Variant | Cause | Effect |
|---|---|---|
| `Io` | socket/stream I/O error, including EOF in the middle of a body | Session ends. |
| `Closed` | EOF while reading the 4-byte header (including a truncated header) | Session ends normally. |
| `TooLarge(n, max)` | header length exceeds the receiver's limit | Session ends **before allocation**. |
| `Decode(e)` | body is not a valid postcard encoding of the expected type | Session ends. |

A malformed or oversized frame is never answered with an application error message; the connection
is simply closed. On the server, session cleanup re-queues unfinished tasks **only if this session is
still the registered one**; a session that was replaced by a newer join does not disturb the new
session's tasks.

On a peer link, a framing or I/O error ends that link only. Halos already queued on it are lost;
the sender drops a halo when its per-peer queue (capacity 256) is full or the link is closed. There
is no retry at the peer layer: a stalled direct-mode job is recovered by the server's stall
watchdog (§8.4).

### 7.2 Rejections and timeouts

| Situation | Behaviour |
|---|---|
| Join frame not received within 10 s | Server gives up silently (`join timeout`). |
| `JoinAccepted` not received within 15 s (client) | Client treats it as a disconnect and may reconnect. |
| Wrong first message | `JoinRejected`: `first message must be Join`. |
| Unsupported version | `JoinRejected` with the client's and server's versions. |
| Bad token | `JoinRejected`: `invalid join token`. |
| Invalid capabilities | `JoinRejected` with the validation message. |
| `peer_addr` longer than 4096 B | `JoinRejected`: `peer_addr too long`. |
| Duplicate `Join` | Session ends (`duplicate join`). |
| Heartbeat timeout | Node removed (also its peer address), tasks re-queued, stateful jobs rolled back, best-effort `Shutdown`. |
| Outbound queue full/gone | Node removed, tasks re-queued, best-effort `Shutdown` (`outbound queue stalled`). |
| Attempt wall-clock deadline exceeded | One failure charged, `CancelTask { task_id, attempt }` sent, task re-queued or failed. |
| Direct-mode job makes no progress for `stall_timeout` (default 20 s) | Job rolled back to its checkpoint with a new epoch; after 2 such stalls the job falls back to server-relayed halos (§8.4). |
| Peer link cannot be dialled or breaks | Halos are dropped; the part stalls; the stall watchdog handles recovery. |

### 7.3 Integrity checks

| Check | Where | Failure handling |
|---|---|---|
| `digest(input) == input_hash`, `input.len() <= MAX_PAYLOAD_LEN` | client on `Assign` | `TaskFailed` (`input corrupted or oversized`). |
| `digest(output) == output_hash`, `output.len() <= MAX_PAYLOAD_LEN` | server on `TaskResult` | Counted as failed attempt (`result integrity check failed`). |
| Module hash (`ModuleHash::of(bytes)`) | client on assembly/load and disk-cache re-load | Module load error; waiting tasks fail. |
| `digest(lo \|\| hi) == hash`, each blob `<= MAX_PAYLOAD_LEN` | server on `PartBoundary` (relayed mode) | Stateful job becomes `Failed`. |
| Chunk bounds and offsets | client on `ModuleTransfer` | Partial module discarded; waiting tasks fail. |

Note that an integrity failure on a result is a task-level failure and does not close the
connection; framing/decoding failures do close it.

`PartProgress` (direct mode) carries no hash field: snapshots and progress reports are accepted on
the basis of authenticated node identity, epoch, placement and monotonic step, but the snapshot
bytes themselves are not checksummed.

---

## 8. Stateful jobs

Stateful jobs are an **iterated chain** of parts `0..n-1`, each holding a kernel state. Part `i`
needs part `i-1`'s upper boundary as its lower halo and part `i+1`'s lower boundary as its upper
halo. Two halo paths exist:

* **Relayed (default, `direct = false`):** the server is the hub. It sends `PartStep`, receives
  `PartBoundary` and forwards halos. Topology is a star; there are no client↔client streams.
* **Direct (`direct = true`):** parts exchange `PeerMessage::Halo` frames over client↔client QUIC
  streams (ALPN `opencompute-peer/1`) and report only `PartProgress`/snapshots to the server. The
  server still owns placement, checkpoints, rollback and completion. If peers are unreachable, the
  server's watchdog rolls the job back and eventually switches it to relayed mode.

Both modes support re-partitioning when `parts_per_node > 0` (§8.6).

### 8.1 Stateful message semantics

#### `PartInit` (server → client, variant 7)

| Field | Type | Semantics |
|---|---|---|
| `job_id` | `JobId` | Job. |
| `part` | `u32` | Part index within the job. |
| `epoch` | `u32` | Rollback generation; echoed in replies so stale replies can be dropped. |
| `kernel` | `String` | Kernel name resolved by the client's stateful runtime. |
| `params` | `Vec<u8>` | Kernel parameters for this part. |
| `restore` | `Option<Vec<u8>>` | Checkpoint snapshot to restore from, if any. |
| `direct` | `bool` | `true`: this part runs the direct-mode actor; `false`: relayed mode. |
| `peers` | `Vec<PeerInfo>` | Direct mode: placement of **all** parts, indexed by part. Empty in relayed mode. |
| `steps` | `u64` | Direct mode: total steps. |
| `checkpoint_every` | `u64` | Direct mode: take a snapshot every this many steps (`0` = only at the end). |
| `step` | `u64` | Step the initial/restored state is at. Direct-mode halos for this step are published immediately after init; in relayed mode the first `PartBoundary` carries it. |

The server sends all fields in both modes; `peers` is empty and `steps`/`checkpoint_every` are not
used by the client when `direct = false`.

Relayed mode (`direct = false`): the client first drops any existing state for `(job_id, part)`,
then creates the kernel with `params` and optional `restore`. Success produces a `PartBoundary` at
the given `step` with `snapshot: None` and the kernel's canonical boundary data. A creation error or
panic produces `PartFailed` (`kernel init panicked` for a panic).

Direct mode (`direct = true`): the client aborts any previous actor for `(job_id, part)`, spawns a
part actor (§8.4) and does not reply with `PartBoundary`. If the worker has no peer network (for
example the server's in-process worker), the actor answers `PartFailed`
(`this worker has no peer network`).

#### `PartStep` (server → client, variant 8, relayed mode only)

| Field | Type | Semantics |
|---|---|---|
| `job_id` | `JobId` | Job. |
| `part` | `u32` | Part. |
| `epoch` | `u32` | Must match the epoch from `PartInit`. |
| `step` | `u64` | Step to compute; the previous state is `step - 1`. |
| `halo_lo` | `Option<Vec<u8>>` | Lower neighbour's upper boundary from the previous step. |
| `halo_hi` | `Option<Vec<u8>>` | Upper neighbour's lower boundary from the previous step. |
| `snapshot` | `bool` | Include a full snapshot in the reply. |

The client applies the step only if it has state for `(job_id, part)` with exactly this `epoch`.
Otherwise the message is silently dropped. A successful step replies with `PartBoundary` carrying
the requested `step` and, if `snapshot` was true, a full snapshot. A kernel error or panic replies
with `PartFailed`.

`halo_lo` is `None` for part 0; `halo_hi` is `None` for the last part. The server never sends
`PartStep` for a direct-mode job.

#### `PartBoundary` (client → server, variant 8, relayed mode only)

| Field | Type | Semantics |
|---|---|---|
| `job_id` | `JobId` | Job. |
| `part` | `u32` | Part. |
| `epoch` | `u32` | Epoch of the matching `PartInit`/`PartStep`. |
| `step` | `u64` | Step the data belongs to. |
| `lo` | `Vec<u8>` | Lower boundary blob. |
| `hi` | `Vec<u8>` | Upper boundary blob. |
| `snapshot` | `Option<Vec<u8>>` | Full state if requested, otherwise `None`. |
| `hash` | `[u8; 32]` | BLAKE3-256 over `lo || hi`. |

The server accepts a boundary only if all of the following hold:

* the job exists and is `Running`;
* the job is **not** in direct mode;
* `epoch` equals the job's current epoch;
* `part` is in range and currently placed on the sending node;
* `step` equals the job's current barrier step;
* this part has not already reported for this step;
* `hash == digest(lo || hi)` and both blobs are at most `MAX_PAYLOAD_LEN`.

Anything else is ignored as stale/bogus. An integrity failure fails the whole job.

See §8.3 for what happens when all parts have reported.

#### `PartProgress` (client → server, variant 10, direct mode only)

See §5.1 for the fields. The server accepts a report only if:

* the job exists, is `Running`, and has `direct == true`;
* `epoch` equals the job's current epoch;
* `part` is in range and currently placed on the sending node;
* `step > progress[part]` and `step <= steps` (strictly monotonic, never past the end).

Accepted reports update `progress[part]`. The server resets its stall timer when the **minimum**
progress across all parts increases. If `snapshot` is present, it is stored in a per-step slot; when
every part has supplied a snapshot for that step, all older pending slots are discarded and the
checkpoint is replaced if the step is newer than the stored checkpoint. If that step is also
`>= steps`, the job becomes `Done` with those snapshots as the final state. Duplicate or
out-of-order reports, reports from a node that does not host the part, and reports from an old
epoch are ignored.

#### `PartFailed` (client → server, variant 9)

| Field | Type | Semantics |
|---|---|---|
| `job_id` | `JobId` | Job. |
| `part` | `u32` | Part. |
| `epoch` | `u32` | Epoch. |
| `error` | `String` | Human-readable reason. |

Accepted only if the epoch matches, the part is placed on the sending node, and the job is
`Running`; it fails the **whole** stateful job with `part <n>: <error>`. A stale epoch, a wrong node
or a non-running job is ignored. There is no step-level retry: the recovery mechanism is node-loss
rollback, not per-part retry. This applies to both modes.

#### `PartRelease` (server → client, variant 9)

| Field | Type | Semantics |
|---|---|---|
| `job_id` | `JobId` | Job whose state should be dropped. |
| `below_epoch` | `u32` | Drop buffered halos with an epoch strictly below this value. |

The client drops **all** in-memory parts and direct-mode actors of that job, and purges buffered
peer halos whose epoch is below `below_epoch`. Halos of `below_epoch` and newer are kept, so data
that a fast neighbour already sent for the epoch about to start survives. `u32::MAX` would drop
everything; the server currently sends the new epoch of the (re)start.

The server sends `PartRelease` to every connected node before a (re)start, including nodes that
will not host a part, so leftover state cannot be reused by mistake. Dropping a session also
discards its in-memory part state.

### 8.2 Epochs

* Every (re)start of a stateful job increments `epoch` (starting from 0, so the first start is 1).
* `PartInit`, `PartStep`, `PartBoundary`, `PartFailed`, `PartProgress` and `PartRelease` all carry
  the epoch or an epoch bound. Replies with an old epoch are ignored; this makes a late message
  from before a rollback harmless.
* A `PartRelease` before a restart tells every worker to drop the previous epoch's kernels and
  buffered halos.

### 8.3 Relayed mode (server barrier)

The server drives a strict barrier: it sends `PartStep { step: s }` to every part, waits for one
`PartBoundary { step: s }` from each, and only then sends step `s+1` with the new halos.

When **all** parts have reported:

* if a snapshot is due, every part's `snapshot` must be present; a missing snapshot fails the job;
* the checkpoint is updated only when the completed step is newer than the stored checkpoint;
* if `step >= steps`, the job is `Done` and the final state is the checkpoint's snapshot list;
* otherwise the server increments the step and sends `PartStep` for the next step (`next`), with
  `halo_lo =` the previous part's `hi`, `halo_hi =` the next part's `lo`, and
  `snapshot =` `next >= steps || (checkpoint_every > 0 && next % checkpoint_every == 0)`.

A snapshot is therefore requested at every `checkpoint_every` steps and always at the final step.
With `checkpoint_every == 0`, only the final step snapshots.

Each barrier pass adds `2·n − 1` to the server's `halos_relayed` counter, where `n` is the current
part count. The counter is an activity metric for relayed halos; it is not an exact blob count.

### 8.4 Direct mode (client↔client halos)

In direct mode the server still places parts, stores checkpoints and decides completion, but it
never sends `PartStep`. Each part runs autonomously as a task actor on the client:

1. The actor creates/restores the kernel, validates the peer table (`part < peers.len()` and no
   `NodeId([0; 32])` entries; otherwise `PartFailed: bad peer table`), and publishes its initial
   boundary at `PartInit::step` to its neighbours.
2. For each step `s` it waits until it has both neighbour halos for `(job, part, epoch, s)`:
   * side `0` is its **lower** halo, sent by part `p-1` (none for part 0);
   * side `1` is its **upper** halo, sent by part `p+1` (none for the last part).
3. It runs the kernel step on the worker's compute pool, publishes its new boundaries, increments
   `s`, and repeats until `s == steps`.
4. It reports `PartProgress` when it takes a snapshot, every `PROGRESS_EVERY = 20` steps, and at the
   final step. A snapshot is taken for the **new** step when that step is the final step
   (`new_step >= steps`) or a checkpoint step
   (`checkpoint_every > 0 && new_step % checkpoint_every == 0`).

Halos are published as `PeerMessage::Halo`:

| Field | Type | Semantics |
|---|---|---|
| `job_id` | `JobId` | Job. |
| `dst_part` | `u32` | Receiving part. |
| `epoch` | `u32` | Epoch of the sender's `PartInit`. |
| `step` | `u64` | Step the boundary data belongs to. |
| `side` | `u8` | `0`: receiver's lower halo; `1`: receiver's upper halo. |
| `data` | `Vec<u8>` | Boundary blob. |

The sender sends its own lower boundary to part `p-1` as that part's **upper** halo
(`side = 1`) and its upper boundary to part `p+1` as that part's **lower** halo (`side = 0`).

Transport and delivery rules:

* A worker binds its iroh endpoint with `PEER_ALPN` and advertises its address as
  `JoinRequest::peer_addr`. `PartInit::peers` gives every part the `NodeId` and address of all
  parts, so each part knows its two neighbours.
* A link to a neighbour is opened lazily on first use: the sender dials the neighbour's address,
  opens one unidirectional QUIC stream and writes frames with the 16 MiB peer limit. One
  `mpsc` queue (capacity 256) serialises sends per destination; if it is full or closed, the halo
  is dropped.
* Parts placed on the same node bypass the network and use the worker's in-process mailbox.
* The receiver stores halos in a mailbox keyed by `(job, part, epoch, step, side, sender)`, with at
  most 4096 entries; new halos are dropped when full. A part only **consumes** a halo whose sender
  is the `NodeId` the server named as that neighbour in `PartInit::peers`. Data from any other node
  can occupy mailbox space but is never applied.
* Delivery is best effort. There is no ACK, retransmission or ordering guarantee beyond QUIC.
  Partial halo sets are put back and the actor waits for a notification. If a neighbour is lost,
  its halos never arrive; the part stalls.

**Stall watchdog and relay fallback.** The server ticks every 100 ms. For a running direct job it
compares `last_progress` against `stall_timeout` (default **20 s**, configurable in
`ServerConfig`; not exposed as a CLI flag in the current server binary). If the job has made no
progress:

* `direct_failures` and `rollbacks` are incremented;
* the job is restarted from its checkpoint via `s_start`, with a new epoch, `PartRelease` and
  `PartInit`;
* if `direct_failures >= 2`, `direct` is cleared before the restart, so the job continues in
  relayed mode from the same checkpoint.

The stall timer (`last_progress`) is reset when the **minimum** step over all parts increases, and
at every (re)start. Storing a checkpoint newer than the existing one resets `direct_failures`.
A single part advancing alone therefore does not reset the stall timer, while a checkpoint does
clear the failure count.

A direct job is only placed on nodes that advertised a non-empty `peer_addr`; nodes without a peer
address are ineligible for direct parts (the server-local in-process worker always is). If no
eligible node is connected, the job stays `Waiting`.

### 8.5 Node loss and rollback

* If a node holding any part disappears, the server increments the job's rollback counter, removes
  the node, and restarts the **entire** job from the last complete checkpoint: it re-places all
  parts round-robin over the remaining nodes (sorted by `NodeId`), sends `PartRelease`, then
  `PartInit` to every part with `restore` = the checkpoint snapshot and `step` = checkpoint step.
* If no workers remain, the job returns to `Waiting` with no placement. It starts when a worker is
  available.
* A node loss on a direct job restarts it in direct mode if the survivors are peer-capable; the
  stall watchdog is the mechanism that eventually switches it to relayed mode.
* There is no per-step timeout in relayed mode. A part that never replies (for example because its
  `PartStep` was dropped due to missing state) stalls the barrier until the node is lost or the job
  is restarted.

Placement is round-robin, so several parts may share one node.

### 8.6 Re-partitioning (`parts_per_node`)

`SubmitStateful::parts_per_node` controls the target part count:

* `0`: fixed number of parts (the submitted `params.len()`). Node count changes do not re-split the
  job; after a loss, the remaining parts are simply re-placed and the job rolls back to its
  checkpoint.
* `> 0`: the job keeps `parts_per_node × eligible_connected_nodes` parts. On every (re)start the
  server computes the target and, if it differs from the current part count, calls
  `runtime::stateful::repartition(kernel, params, checkpoint_snapshots, target)`, which returns new
  per-part parameters **and** matching snapshots (the LBM kernel redistributes rows). Only the LBM
  kernel supports this today.
* While a job is `Running`, the server checks every tick (after the job has run for at least 2 s)
  whether the target changed because nodes joined or left; if so it restarts the job from its last
  checkpoint with the new part count. `last_target` prevents restart loops when the kernel clamps
  the requested count to a different value.
* Every re-split increments `repartitions` and the epoch, and is preceded by `PartRelease` +
  `PartInit`, so all workers discard the old strips.
* Because `params` are replaced, `StatefulResult.final_params` may differ from the params that were
  submitted (§9.2).

---

## 9. Control socket protocol

The control socket is a **local, trusted** administrative interface. It is not the P2P worker
protocol and has no authentication beyond file-system permissions.

* **Path:** `<data-dir>/control.sock` (`default_control_path`).
* **Platform:** Unix domain socket only.
* **Permissions:** mode `0600`, set right after bind. The server removes a stale socket before
  binding and removes it on shutdown.
* **Trust:** anyone who can open the socket can submit jobs and inspect the cluster. Do not expose
  the data directory to untrusted users.
* **Framing:** the same `u32` big-endian length + postcard framing, with
  `CONTROL_MAX_FRAME = MAX_STATE_FRAME_LEN` (256 MiB).
* **Flow:** the client connects and sends one `ControlRequest` frame; the server handles it and
  writes exactly one `ControlResponse` frame. The connection may then be reused for another
  sequential request/response pair. There are no request IDs and no handshake or version field;
  requests are processed and answered strictly in order, so a response is correlated with its
  request by position. A framing/decoding error ends that connection; the server does not send an
  `Error` response for it.
* Multiple connections may be open concurrently.

### 9.1 Requests

`ControlRequest` variant indices:

| Index | Variant | Fields / semantics |
|---:|---|---|
| 0 | `Info` | Returns server identity and ticket. |
| 1 | `Nodes` | Lists connected nodes. |
| 2 | `Jobs` | Lists **batch** jobs known to the scheduler. |
| 3 | `Status(JobId)` | One batch job with per-task states; unknown → `Error`. |
| 4 | `UploadModule(Vec<u8>)` | Stores a WASM module in memory and on disk. Empty or `> MAX_MODULE_LEN` → `Error`. Returns `ModuleStored(hash)`. |
| 5 | `Submit(SubmitJob)` | Submits a batch job. See validation below. |
| 6 | `Results { job, wait_ms }` | Waits up to `wait_ms` for a batch job, then returns its summary and per-task results. |
| 7 | `Cancel(JobId)` | Cancels all non-terminal tasks of a batch job and sends `CancelTask` for in-flight attempts. Returns `Ok` even if the job is unknown. |
| 8 | `SubmitStateful(SubmitStateful)` | Submits a stateful chain job. |
| 9 | `StatefulResult { job, wait_ms }` | Waits up to `wait_ms` for a stateful job. |
| 10 | `Diag` | Returns server counters. |
| 11 | `UploadBlob(Vec<u8>)` | Stores a shared input blob in memory and under `blobs/`. Empty or `> MAX_BLOB_LEN` (128 MiB) → `Error`. Returns `BlobStored(hash)`. (v5) |
| 12 | `ReleaseBlob(ModuleHash)` | Forgets a blob; tasks still waiting for it fail. Returns `Ok`. (v5) |

`SubmitJob`:

| Field | Type | Semantics |
|---|---|---|
| `kind` | `TaskKind` | Shared by all tasks. |
| `shared` | `Option<ModuleHash>` | Blob uploaded earlier with `UploadBlob`, given to every task (v5). |
| `inputs` | `Vec<Vec<u8>>` | One input per task, in submission order. |
| `timeout_ms` | `u64` | Per-task wall-clock budget. |
| `max_attempts` | `u32` | Maximum counted failures per task. |
| `limits` | `ExecLimits` | Memory/fuel limits. |

`Submit` validation:

* `1 <= inputs.len() <= 1_000_000` and every input `<= MAX_PAYLOAD_LEN`;
* `timeout_ms > 0` and `max_attempts > 0`;
* a `Wasm(hash)` module must already have been uploaded to this server;
* a `shared` blob must have been uploaded;
* a `Builtin(name)` name must be non-empty and at most 64 bytes.

Submission creates a new job every time; there is **no idempotency key**, so retrying a `Submit`
creates a second job.

`SubmitStateful`:

| Field | Type | Semantics |
|---|---|---|
| `kernel` | `String` | Kernel name; must be known to the server's runtime. |
| `params` | `Vec<Vec<u8>>` | One parameter blob per part, in chain order; 1..=4096 parts. |
| `steps` | `u64` | Total steps; must be `> 0`. |
| `checkpoint_every` | `u64` | Checkpoint period; `0` means only at the end. |
| `direct` | `bool` | Exchange halos directly between clients; falls back to server relay if peers are unreachable. |
| `parts_per_node` | `u32` | If `> 0`, target `parts_per_node × eligible nodes` parts and re-partition from the last checkpoint when nodes join or leave. `0` = fixed part count. |

Each `params` blob must be `<= MAX_PAYLOAD_LEN`. Validation only checks the kernel name, the
1..=4096 part range, `steps > 0` and the parameter sizes; `direct` and `parts_per_node` are accepted
as given (`parts_per_node` is not range-checked, and a positive value only matters for kernels that
support re-partitioning).

### 9.2 Responses

`ControlResponse` variant indices:

| Index | Variant | Fields / semantics |
|---:|---|---|
| 0 | `Error(String)` | Request-level error; the connection may continue. |
| 1 | `Info(ServerInfo)` | `node_id`, `ticket`, `protocol_version`. |
| 2 | `Nodes(Vec<NodeInfo>)` | `node_id`, `name`, `capabilities`, `assigned`, `completed`, `last_seen_ms`, `local`. |
| 3 | `Jobs(Vec<JobInfo>)` | Batch job summaries. |
| 4 | `Status(JobInfo)` | One batch job, including per-task states. |
| 5 | `ModuleStored(ModuleHash)` | Stored module hash. |
| 6 | `Submitted(JobId)` | Accepted batch job. |
| 7 | `Results { job: JobInfo, results }` | `results` is per task in submission order: `None` until finished, `Some(Ok(bytes))` on success, `Some(Err(message))` on failure/cancel. |
| 8 | `StatefulSubmitted(JobId)` | Accepted stateful job. |
| 9 | `Stateful { info, final_state, final_params }` | See below. |
| 10 | `Diag(ServerStats)` | `uptime_s`, `nodes`, `jobs`, `nodes_lost`, `rollbacks`, `saves`, `restored_jobs`, `halos_relayed`, `blobs_sent`. |
| 11 | `Ok` | Generic success (used by `Cancel`, `ReleaseBlob`). |
| 12 | `BlobStored(ModuleHash)` | Hash of the stored blob. (v5) |

`StatefulInfo`:

| Field | Type | Semantics |
|---|---|---|
| `job_id` | `JobId` | Job. |
| `parts` | `u32` | Current part count (can change after re-partitioning). |
| `step` | `u64` | Last fully completed (barrier-passed) step. It is derived from the coordinator's step counter: for a `Running` relayed job that is the step being computed minus one. In direct mode the coordinator's step counter stays at the restart step, so this field does not advance with `PartProgress` reports. |
| `steps` | `u64` | Target steps. |
| `checkpoint_step` | `Option<u64>` | Latest checkpoint step. |
| `rollbacks` | `u32` | Times the job was rolled back to a checkpoint (node loss or direct-mode stall). |
| `state` | `String` | `Waiting`, `Running`, `Done`, or `Failed: <reason>`. |
| `placement` | `Vec<Option<NodeId>>` | Node of each part (`None` while unplaced). |
| `direct` | `bool` | Whether halos currently go directly between clients. |
| `repartitions` | `u32` | How often the job was re-split into a different number of parts. |

`Stateful` response fields:

| Field | Type | Semantics |
|---|---|---|
| `info` | `StatefulInfo` | Status. |
| `final_state` | `Option<Vec<Vec<u8>>>` | `Some` when the job is `Done`: one snapshot per part. `None` on failure or timeout. |
| `final_params` | `Vec<Vec<u8>>` | Per-part parameters matching `final_state`; they differ from the submitted ones if the job was re-partitioned. Empty when the request timed out before the job finished. |

`JobInfo` contains `job_id`, `total`, `pending`, `active`, `completed`, `failed`, `cancelled`,
`retries`, `done`, and `tasks: Vec<(TaskId, String)>` (per-task state strings when a single job is
requested).

`Results` and `StatefulResult` poll every 25 ms and return when the job is finished or the deadline
passes. A `Results` response may therefore be **partial**: `job.done == false` and some entries are
`None`. An unknown job returns `Error`.

Batch-job listings (`Jobs`, `Status`) come from the scheduler and do not include stateful jobs;
`Diag.jobs` counts both. Stateful jobs are queried through `StatefulResult`.

---

## 10. Worked byte-level example: framed `Heartbeat`

Take a heartbeat from a worker that is running exactly one task, `task-1`, on attempt 2:

```rust
ClientMessage::Heartbeat(Heartbeat {
    running: vec![(TaskId(1), 2)],
})
```

Postcard body, in order:

1. `ClientMessage::Heartbeat` is variant index **2** → varint `0x02`.
2. `running` has one element → `u32` varint `0x01`.
3. The pair's first field is `TaskId(1)` → `u64` varint `0x01`.
4. The pair's second field is attempt `2` → `u32` varint `0x02`.

The body is 4 bytes: `02 01 01 02`. The framing header is the body length as `u32` big-endian:
`00 00 00 04`.

Complete frame (8 bytes):

```
00 00 00 04 02 01 01 02
```

Byte-by-byte:

| Offset | Bytes | Meaning |
|---:|---|---|
| 0 | `00 00 00 04` | body length = 4 (`u32` big-endian) |
| 4 | `02` | `ClientMessage` variant 2 = `Heartbeat` |
| 5 | `01` | `running.len()` = 1 |
| 6 | `01` | `TaskId(1)` = 1 |
| 7 | `02` | `attempt` = 2 |

All four integer values are below 128, so each varint is a single byte. A larger value, for example
`TaskId(300)`, would use a multi-byte LEB128 encoding (`0xAC 0x02`) and lengthen the body
accordingly; the header would change but still count only body bytes.

This frame does not depend on the protocol version: `Heartbeat` has been variant 2 since v2, and
v3 only appended `PartProgress` as variant 10. The exact byte sequence is asserted by
`crates/protocol/tests/doc_example.rs`:

```bash
cargo test -p opencompute-protocol --test doc_example
```

The test checks that `encode()` produces these bytes, that `read_frame()` decodes them back to the
same heartbeat, and that `write_frame()` emits the identical byte sequence. If this document and the
codec ever disagree, that test fails.

---

## 11. Changing the protocol

* `PROTOCOL_VERSION` and `ALPN` are both `5` / `opencompute/5`. An incompatible change must bump
  both (and the ALPN string), because peers with different ALPNs never reach the application
  handshake. The worker protocol and the peer protocol are versioned separately: peer streams use
  `opencompute-peer/1`, and `PeerMessage` has no version field of its own.
* At the application layer the server requires exact version equality and the client requires the
  server's `JoinAccepted.protocol_version` to equal 3. A mismatch is a clean `JoinRejected`, not a
  best-effort downgrade.
* Postcard enum variant order and struct field order are wire ABI. Append new enum variants at the
  end; do not reorder or remove existing ones. Adding, removing, reordering or resizing struct
  fields is incompatible. This applies to `PeerMessage` as well.
* Size limits are compiled into both ends and are not negotiated. Changing one requires a version
  bump. The peer frame limit is a client constant, not a protocol constant.
* There is no extension/TLV mechanism, no per-message length or type tag beyond the enum variant,
  and no protocol negotiation beyond ALPN and the version fields in `Join`/`JoinAccepted`.

## 11. Changes in protocol version 4 (halo depth)

Version 4 adds temporal blocking to direct-mode stateful jobs. Fields were appended to existing
structs, so every peer must run version 4 (the version and ALPN were bumped).

| Struct | New field | Meaning |
|---|---|---|
| `PartInit` | `edge_depths: Vec<u32>` | Halo depth of each edge (`peers.len() - 1` entries; edge `e` joins parts `e` and `e + 1`). Empty means depth 1. Edge `e` exchanges halos at the steps where `(step - init.step) % edge_depths[e] == 0`, and the halo carries `edge_depths[e]` rows. Both ends of an edge receive the same value. |
| `PartProgress` | `steps_done: u32`, `compute_us: u64`, `wait_us: u64` | Steps covered since the previous report, microseconds spent computing them, and microseconds spent waiting for neighbours' halos. The server uses them to tune the depth. |
| `SubmitStateful` | `halo_depth: u32` | Requested depth. `0` = automatic, `1` = exchange every step. |
| `StatefulInfo` | `halo_depth: u32`, `retunes: u32` | Largest depth in use, and how often the automatic tuner changed it. |

`state.bin` magic changed to `0x4f43_5303` (adds `halo_req`, `halo_depth`, `retunes` per stateful
job). Files with the old magic are ignored.

The server clamps the depth of every edge to the smaller row count of the two parts it joins.
In relay mode (`direct = false`) the depth is always 1 and `edge_depths` is empty.

## 12. Changes in protocol version 5 (shared input blobs)

Version 5 lets all tasks of a job read the same large input without sending it once per task.
New enum variants were appended (existing indices are unchanged); `TaskAssignment` and `SubmitJob`
gained a field, so the version and ALPN were bumped.

* `UploadBlob` / `ReleaseBlob` / `BlobStored` (control), `BlobRequest` / `BlobTransfer` /
  `BlobNotFound` (worker). Blobs are identified by BLAKE3 and limited to `MAX_BLOB_LEN` (128 MiB);
  chunks are `MODULE_CHUNK_LEN`.
* The server serves a blob from a separate task, so a slow link does not delay that node's
  heartbeats and results. A worker fetches each blob once and keeps it in a cache of at most a
  quarter of its memory budget (least recently used blobs are dropped first).
* `ServerStats.blobs_sent` counts blobs served.
* `state.bin` magic changed to `0x4f43_5304` (`TaskSnapshot.shared`). Blob bytes are not in
  `state.bin`; they are files `blobs/<hash>.bin`.

## 13. Reaching a server only over TCP (ssh tunnel)

A client that cannot send UDP to the server (no IPv6, no VPN, only ssh) can still join:
run `scripts/udp_tcp_bridge.py serve 17777 <server-ip> <server-port>` next to the server,
forward it with `ssh -R 127.0.0.1:17777:127.0.0.1:17777 <client-host>`, run
`scripts/udp_tcp_bridge.py client 18777 17777` on the client host, and start the client with
`join --connect-addr 127.0.0.1:18777 ...`. `--connect-addr` replaces the addresses in the ticket
(identity and token stay pinned) and stops the client advertising a peer address, so the server
sends it everything itself. Measured N=65536: this node alone 2.4 s/step; the TCP head-of-line
blocking makes it slower than leaving it out when the other nodes are fast.

## 14. Automatic tuning (added with v5)

- `NodeCapabilities.avx2` (last field): the worker can run the native SIMD built-in kernels.
  `Cluster::prefers_native()` is true when at least 80% of the slots are on such workers.
- `NodeInfo.mean_task_ms` (last field): measured time per task = compute (EWMA of `elapsed_ms`)
  plus round trip. The round trip is `result arrival - hand-out - elapsed_ms`, sampled only for
  tasks that found an idle slot after the node had finished one task of the same job (so the
  job's shared data was already there).
- Scheduler tail rules (`Tuning::tail_holdback`): a node is skipped for the last tasks of a batch
  if it is slower than `holdback_ratio` x the best node while faster slots can absorb the queue,
  or if its task would finish after the other nodes drain the queue. A skipped node still gets a
  task after 5 s without one when the queue is long, so its estimate cannot go stale.
- `ControlRequest::ForgetJob(JobId)` (last variant): drop a finished job and its results. The
  `map*` helpers send it after collecting the results, so the state file no longer grows with
  every step (it reached 288 MB after 180 N-body steps and slowed everything down).
- `BlobExpect.wait_ms` is derived from the seed's measured download rate (1.5x its transfer
  time + 400 ms, clamped to 1-6 s) instead of a constant.
- `BlockTuner` (compute-api): picks the task count of a repeated step among 0.5x-2.25x of
  `8 x slots` by timing them, re-measuring each every 24 steps; `nbody_batch N steps 0` uses it.
