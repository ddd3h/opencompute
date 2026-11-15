//! Transport-level tests: identity pinning, ticket encoding and key files.
//!
//! Everything runs on loopback with `relay: false`, so no third-party servers are contacted.

use opencompute_network::iroh::{EndpointAddr, TransportAddr};
use opencompute_network::{
    NetError, NetworkConfig, Ticket, bind, dial, generate_secret, load_or_create_secret,
};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

static NEXT_DIR: AtomicUsize = AtomicUsize::new(0);

fn tmp_dir(name: &str) -> PathBuf {
    let n = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("oc-net-{name}-{}-{n}", std::process::id()))
}

fn local_net() -> NetworkConfig {
    NetworkConfig {
        relay: false,
        bind: Some("127.0.0.1:0".parse().unwrap()),
    }
}

/// Dialing the server's socket under a *different* endpoint id must fail: the QUIC/TLS
/// handshake pins the peer key, so an impostor (or a mis-copied ticket) cannot connect.
///
/// `dial` errors, but only after iroh's ~30 s connection/idle timeout, because neither
/// `dial` nor `Endpoint::connect` has a connect timeout of its own. The assertion below
/// encodes the desired 10 s bound and currently fails (see the `ignore` reason).
#[tokio::test]
async fn dial_rejects_mismatched_endpoint_id() {
    let cfg = local_net();
    let server = bind(generate_secret(), &cfg, true).await.unwrap();

    let sockets = server.bound_sockets();
    assert!(!sockets.is_empty(), "server has no bound socket");

    // Same reachable addresses as the real server, but the wrong identity.
    let impostor_addr = EndpointAddr::from_parts(
        generate_secret().public(),
        sockets.into_iter().map(TransportAddr::Ip),
    );
    assert_ne!(impostor_addr.id, server.id(), "ids must differ");

    let client = bind(generate_secret(), &cfg, false).await.unwrap();
    let res = tokio::time::timeout(Duration::from_secs(10), dial(&client, impostor_addr)).await;

    match res {
        Ok(Err(NetError::Connect(_) | NetError::Stream(_))) => {}
        Ok(Err(e)) => panic!("dial failed with an unexpected error: {e}"),
        Ok(Ok(_)) => panic!("dial succeeded although the endpoint id did not match the server key"),
        Err(_) => panic!("dial did not fail within 10s"),
    }

    client.close().await;
    server.close().await;
}

#[test]
fn ticket_decode_rejects_oversize_and_odd_lengths() {
    // 8192 hex chars is the limit; anything longer is rejected before hex decoding.
    let oversize = format!("oc1{}", "0".repeat(8194));
    assert!(Ticket::decode(&oversize).is_err());

    let way_oversize = format!("oc1{}", "ab".repeat(8192));
    assert!(Ticket::decode(&way_oversize).is_err());

    // Odd number of hex digits can never be bytes.
    assert!(Ticket::decode(&format!("oc1{}", "a".repeat(101))).is_err());
    assert!(Ticket::decode("oc1abc").is_err());
}

#[test]
fn ticket_roundtrips_with_several_direct_addresses() {
    let id = generate_secret().public();
    let addr = EndpointAddr::new(id)
        .with_ip_addr("127.0.0.1:45678".parse().unwrap())
        .with_ip_addr("192.0.2.10:9000".parse().unwrap())
        .with_ip_addr("[2001:db8::1]:4433".parse().unwrap());
    assert_eq!(addr.ip_addrs().count(), 3);

    let ticket = Ticket {
        addr,
        token: [0x5a; 16],
    };
    let encoded = ticket.encode();
    assert!(encoded.starts_with("oc1"));

    let decoded = Ticket::decode(&encoded).unwrap();
    assert_eq!(decoded, ticket);
    assert_eq!(decoded.addr.ip_addrs().count(), 3);
    assert_eq!(decoded.token, [0x5a; 16]);
    assert_eq!(decoded.server_id(), ticket.server_id());
}

#[test]
fn load_or_create_secret_rejects_wrong_key_length() {
    let dir = tmp_dir("bad-key");

    let short = dir.join("short");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(&short, [0u8; 31]).unwrap();
    let err = load_or_create_secret(&short).unwrap_err();
    assert!(matches!(err, NetError::Key(_)), "got {err}");

    let long = dir.join("long");
    std::fs::write(&long, [7u8; 33]).unwrap();
    let err = load_or_create_secret(&long).unwrap_err();
    assert!(matches!(err, NetError::Key(_)), "got {err}");

    std::fs::remove_dir_all(dir).ok();
}

#[cfg(unix)]
#[test]
fn key_file_mode_is_0600() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tmp_dir("key-mode");
    let path = dir.join("node.key");

    let key = load_or_create_secret(&path).unwrap();
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "key file must not be readable by others");

    // The key that was written is the one returned and reloaded.
    let reloaded = load_or_create_secret(&path).unwrap();
    assert_eq!(key.to_bytes(), reloaded.to_bytes());

    std::fs::remove_dir_all(dir).ok();
}
