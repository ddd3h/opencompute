//! P2P transport for OpenCompute, built on [iroh](https://docs.rs/iroh).
//!
//! * Every node has an Ed25519 identity (its `EndpointId`). QUIC/TLS authenticates both
//!   sides by that key, so dialling the id from the [`Ticket`] pins the server's identity.
//! * Direct paths, hole punching and (optionally) n0's public relay are handled by iroh.
//! * The worker protocol runs on a single bidirectional stream per connection.
//!
//! The transport is deliberately thin: the rest of the system only sees
//! `AsyncRead + AsyncWrite`, so the in-process worker and the tests use `tokio::io::duplex`.

use iroh::endpoint::{Connection, RecvStream, SendStream, presets};
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey};
use opencompute_protocol::{ALPN, NodeId, PEER_ALPN};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

pub use iroh;

/// Network layer errors.
#[derive(Debug, thiserror::Error)]
pub enum NetError {
    /// Could not bind the endpoint.
    #[error("bind failed: {0}")]
    Bind(String),
    /// Could not connect to the peer.
    #[error("connect failed: {0}")]
    Connect(String),
    /// Stream could not be opened/accepted.
    #[error("stream: {0}")]
    Stream(String),
    /// Ticket string is malformed.
    #[error("invalid ticket: {0}")]
    Ticket(String),
    /// Key file I/O problem.
    #[error("key file: {0}")]
    Key(String),
}

/// How the endpoint reaches the network.
#[derive(Clone, Debug, Default)]
pub struct NetworkConfig {
    /// Use n0's default relay servers and DNS address publishing (NAT traversal across the
    /// Internet). **This contacts third-party (n0) servers.** When `false` only direct
    /// IP connectivity (LAN, public IPs, port-forwarding) works.
    pub relay: bool,
    /// Bind to this local UDP address instead of any port (used by tests: `127.0.0.1:0`).
    pub bind: Option<SocketAddr>,
}

/// Convert an iroh endpoint id into our [`NodeId`].
pub fn node_id(id: iroh::EndpointId) -> NodeId {
    NodeId(*id.as_bytes())
}

/// Load a secret key from `path`, creating it (mode 0600) if absent.
pub fn load_or_create_secret(path: &Path) -> Result<SecretKey, NetError> {
    let key_err = |e: std::io::Error| NetError::Key(format!("{}: {e}", path.display()));
    match std::fs::read(path) {
        Ok(bytes) => {
            let arr: [u8; 32] = bytes
                .try_into()
                .map_err(|_| NetError::Key(format!("{}: expected 32 bytes", path.display())))?;
            Ok(SecretKey::from_bytes(&arr))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir).map_err(key_err)?;
            }
            let key = generate_secret();
            write_private(path, &key.to_bytes()).map_err(key_err)?;
            Ok(key)
        }
        Err(e) => Err(key_err(e)),
    }
}

/// Generate a fresh random secret key.
pub fn generate_secret() -> SecretKey {
    let mut b = [0u8; 32];
    rand::fill(&mut b);
    SecretKey::from_bytes(&b)
}

fn write_private(path: &Path, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create_new(true);
    #[cfg(unix)]
    o.mode(0o600);
    o.open(path)?.write_all(data)
}

/// QUIC settings tuned for bulk shipping (per-step blobs, results) over 10-100 ms links.
///
/// The defaults assume ~100 ms RTT and a 100 Mbit/s stream. Larger flow-control windows keep a
/// single stream from stalling on a fast long path, and a smaller initial RTT lets loss
/// recovery start sooner on the paths this system actually uses. Memory is only used when data
/// is in flight.
fn transport_config() -> iroh::endpoint::QuicTransportConfig {
    use iroh::endpoint::VarInt;
    iroh::endpoint::QuicTransportConfig::builder()
        .stream_receive_window(VarInt::from_u32(16 << 20))
        .receive_window(VarInt::from_u32(64 << 20))
        .send_window(64 << 20)
        .initial_rtt(Duration::from_millis(50))
        .build()
}

/// Bind an iroh endpoint. `accept` registers our ALPN so the endpoint can be dialled.
pub async fn bind(
    secret: SecretKey,
    cfg: &NetworkConfig,
    accept: bool,
) -> Result<Endpoint, NetError> {
    bind_alpns(
        secret,
        cfg,
        if accept { vec![ALPN.to_vec()] } else { vec![] },
    )
    .await
}

/// Bind an endpoint accepting exactly the given ALPNs (e.g. `[PEER_ALPN]` for workers).
pub async fn bind_alpns(
    secret: SecretKey,
    cfg: &NetworkConfig,
    alpns: Vec<Vec<u8>>,
) -> Result<Endpoint, NetError> {
    let mut b = if cfg.relay {
        Endpoint::builder(presets::N0)
    } else {
        Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled)
    };
    b = b.secret_key(secret).transport_config(transport_config());
    if !alpns.is_empty() {
        b = b.alpns(alpns);
    }
    if let Some(addr) = cfg.bind {
        b = b
            .bind_addr(addr)
            .map_err(|e| NetError::Bind(e.to_string()))?;
    }
    b.bind().await.map_err(|e| NetError::Bind(e.to_string()))
}

/// Wait (bounded) until the endpoint has contacted its relay, so its address is dialable from outside.
pub async fn wait_online(ep: &Endpoint, cfg: &NetworkConfig, max: Duration) {
    if cfg.relay {
        let _ = tokio::time::timeout(max, ep.online()).await;
    }
}

/// Dial `addr` and open the worker stream.
pub async fn dial(
    ep: &Endpoint,
    addr: EndpointAddr,
) -> Result<(Connection, SendStream, RecvStream), NetError> {
    let conn = tokio::time::timeout(Duration::from_secs(10), ep.connect(addr, ALPN))
        .await
        .map_err(|_| NetError::Connect("connect timed out after 10s".into()))?
        .map_err(|e| NetError::Connect(e.to_string()))?;
    let (tx, rx) = conn
        .open_bi()
        .await
        .map_err(|e| NetError::Stream(e.to_string()))?;
    Ok((conn, tx, rx))
}

/// Wait for the next inbound connection attempt (cheap; does not complete the handshake).
pub async fn accept_incoming(ep: &Endpoint) -> Option<iroh::endpoint::Incoming> {
    ep.accept().await
}

/// Complete the handshake of `incoming` and accept its worker stream, within `limit`.
///
/// Run this in its own task per connection: a peer that connects but never writes would otherwise
/// stall the accept loop (QUIC only surfaces a stream once data arrives on it).
pub async fn open_worker(
    incoming: iroh::endpoint::Incoming,
    limit: Duration,
) -> Result<(Connection, NodeId, SendStream, RecvStream), NetError> {
    let work = async {
        let conn = incoming
            .await
            .map_err(|e| NetError::Connect(e.to_string()))?;
        let id = node_id(conn.remote_id());
        let (tx, rx) = conn
            .accept_bi()
            .await
            .map_err(|e| NetError::Stream(e.to_string()))?;
        Ok((conn, id, tx, rx))
    };
    tokio::time::timeout(limit, work)
        .await
        .map_err(|_| NetError::Stream("peer did not open a stream in time".into()))?
}

/// Accept the next inbound connection and its worker stream (sequential convenience wrapper).
pub async fn accept_worker(
    ep: &Endpoint,
) -> Option<Result<(Connection, NodeId, SendStream, RecvStream), NetError>> {
    let incoming = accept_incoming(ep).await?;
    Some(open_worker(incoming, Duration::from_secs(10)).await)
}

/// Serialise an endpoint address for transport inside protocol messages.
pub fn encode_addr(addr: &EndpointAddr) -> Vec<u8> {
    postcard::to_stdvec(addr).expect("addr serialises")
}

/// Parse an address received from a peer (untrusted).
pub fn decode_addr(bytes: &[u8]) -> Option<EndpointAddr> {
    if bytes.is_empty() || bytes.len() > opencompute_protocol::MAX_PEER_ADDR_LEN {
        return None;
    }
    postcard::from_bytes(bytes).ok()
}

/// Dial another worker and open the one-directional halo stream.
pub async fn dial_peer(
    ep: &Endpoint,
    addr: EndpointAddr,
) -> Result<(Connection, SendStream), NetError> {
    let conn = tokio::time::timeout(Duration::from_secs(10), ep.connect(addr, PEER_ALPN))
        .await
        .map_err(|_| NetError::Connect("peer connect timed out".into()))?
        .map_err(|e| NetError::Connect(e.to_string()))?;
    let tx = conn
        .open_uni()
        .await
        .map_err(|e| NetError::Stream(e.to_string()))?;
    Ok((conn, tx))
}

/// What a client needs to join: the server's address (with pinned identity) plus a join token.
///
/// String form: `oc1` + lowercase hex of the postcard encoding.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Ticket {
    /// Server address; `addr.id` is the identity the client verifies.
    pub addr: EndpointAddr,
    /// Shared join secret.
    pub token: [u8; 16],
}

impl Ticket {
    /// Fresh random token.
    pub fn random_token() -> [u8; 16] {
        let mut t = [0u8; 16];
        rand::fill(&mut t);
        t
    }

    /// Encode to the string handed to users.
    pub fn encode(&self) -> String {
        let bytes = postcard::to_stdvec(self).expect("ticket serialises");
        let mut s = String::from("oc1");
        for b in bytes {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }

    /// Decode a ticket string.
    pub fn decode(s: &str) -> Result<Self, NetError> {
        let h = s
            .trim()
            .strip_prefix("oc1")
            .ok_or_else(|| NetError::Ticket("missing oc1 prefix".into()))?;
        if h.len() % 2 != 0 || !h.is_ascii() || h.len() > 8192 {
            return Err(NetError::Ticket("bad length".into()));
        }
        let bytes: Result<Vec<u8>, _> = (0..h.len() / 2)
            .map(|i| u8::from_str_radix(&h[2 * i..2 * i + 2], 16))
            .collect();
        let bytes = bytes.map_err(|_| NetError::Ticket("not hex".into()))?;
        postcard::from_bytes(&bytes).map_err(|e| NetError::Ticket(e.to_string()))
    }

    /// The server's node id.
    pub fn server_id(&self) -> NodeId {
        node_id(self.addr.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opencompute_protocol::codec::{read_frame, write_frame};

    #[test]
    fn ticket_roundtrip_and_rejects_garbage() {
        let sk = generate_secret();
        let t = Ticket {
            addr: EndpointAddr::new(sk.public()),
            token: [7; 16],
        };
        let s = t.encode();
        assert_eq!(Ticket::decode(&s).unwrap(), t);
        assert!(Ticket::decode("oc1zz").is_err());
        assert!(Ticket::decode("xyz").is_err());
        assert!(Ticket::decode("oc1abc").is_err());
        assert!(Ticket::decode(&format!("oc1{}", "é".repeat(4))).is_err());
    }

    #[test]
    fn key_persisted() {
        let dir = std::env::temp_dir().join(format!("oc-key-{}", std::process::id()));
        let p = dir.join("k");
        let a = load_or_create_secret(&p).unwrap();
        let b = load_or_create_secret(&p).unwrap();
        assert_eq!(a.to_bytes(), b.to_bytes());
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn two_endpoints_exchange_frames_over_quic() {
        let cfg = NetworkConfig {
            relay: false,
            bind: Some("127.0.0.1:0".parse().unwrap()),
        };
        let server = bind(generate_secret(), &cfg, true).await.unwrap();
        let client_key = generate_secret();
        let client_id = node_id(client_key.public());
        let client = bind(client_key, &cfg, false).await.unwrap();
        let addr = server.addr();
        let s2 = server.clone();
        let h = tokio::spawn(async move {
            let (conn, id, mut tx, mut rx) = accept_worker(&s2).await.unwrap().unwrap();
            let m: String = read_frame(&mut rx, 1024).await.unwrap();
            write_frame(&mut tx, &format!("echo {m}"), 1024)
                .await
                .unwrap();
            tx.finish().ok();
            conn.closed().await;
            id
        });
        let (conn, mut tx, mut rx) = dial(&client, addr).await.unwrap();
        write_frame(&mut tx, &"hi".to_string(), 1024).await.unwrap();
        let r: String = read_frame(&mut rx, 1024).await.unwrap();
        assert_eq!(r, "echo hi");
        conn.close(0u32.into(), b"bye");
        client.close().await;
        assert_eq!(
            h.await.unwrap(),
            client_id,
            "server sees the authenticated client key"
        );
    }
}
