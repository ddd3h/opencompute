//! Wire protocol shared by the OpenCompute server, client and control tools.
//!
//! * [`ids`]      – strongly typed `JobId`, `TaskId`, `NodeId`, `ModuleHash`.
//! * [`message`]  – worker protocol (`ClientMessage` / `ServerMessage`).
//! * [`control`]  – admin / submission protocol spoken over the local control socket.
//! * [`codec`]    – length-prefixed postcard framing with strict size limits.

pub mod codec;
pub mod control;
pub mod ids;
pub mod message;

pub use ids::{JobId, ModuleHash, NodeId, TaskId, digest};
pub use message::*;

/// Protocol version. Bumped on any incompatible wire change.
pub const PROTOCOL_VERSION: u16 = 5;

/// ALPN identifier negotiated during the QUIC handshake.
pub const ALPN: &[u8] = b"opencompute/5";

/// Maximum size of a single framed message (header excluded).
pub const MAX_FRAME_LEN: usize = 4 * 1024 * 1024;

/// ALPN of the client↔client (peer) protocol.
pub const PEER_ALPN: &[u8] = b"opencompute-peer/1";

/// Maximum frame after the join handshake (stateful-job snapshots can be large).
/// The pre-authentication join frame is still limited to [`MAX_FRAME_LEN`].
pub const MAX_STATE_FRAME_LEN: usize = 256 * 1024 * 1024;

/// Maximum size of a WASM module accepted by server and client.
pub const MAX_MODULE_LEN: usize = 32 * 1024 * 1024;

/// Maximum size of a single task input or output.
pub const MAX_PAYLOAD_LEN: usize = 2 * 1024 * 1024;

/// Maximum size of a shared input blob (see `ControlRequest::UploadBlob`).
pub const MAX_BLOB_LEN: usize = 128 * 1024 * 1024;

/// Size of one `ModuleTransfer` chunk (also used for blobs).
pub const MODULE_CHUNK_LEN: usize = 1024 * 1024;
