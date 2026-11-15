//! Strongly typed identifiers. They are distinct types so they cannot be mixed up.

use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! numeric_id {
    ($(#[$m:meta])* $name:ident, $prefix:literal) => {
        $(#[$m])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub struct $name(pub u64);
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!($prefix, "-{}"), self.0)
            }
        }
    };
}

numeric_id!(/// Identifies a job (a batch of independent tasks).
    JobId, "job");
numeric_id!(/// Identifies one task inside the server; unique across jobs.
    TaskId, "task");

fn hex(bytes: &[u8]) -> String {
    use fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn unhex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 || !s.is_ascii() {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

/// Identifies a cluster node. This is the node's authenticated public key (iroh endpoint id).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub [u8; 32]);

impl NodeId {
    /// Short form for logs.
    pub fn short(&self) -> String {
        hex(&self.0[..4])
    }
    /// Parse the 64-char hex form.
    pub fn from_hex(s: &str) -> Option<Self> {
        unhex32(s).map(Self)
    }
}
impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex(&self.0))
    }
}
impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self.short())
    }
}

/// BLAKE3 hash identifying a WASM module (and used for integrity checks).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ModuleHash(pub [u8; 32]);

impl ModuleHash {
    /// Hash `bytes`.
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }
    /// Parse the 64-char hex form.
    pub fn from_hex(s: &str) -> Option<Self> {
        unhex32(s).map(Self)
    }
}
impl fmt::Display for ModuleHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex(&self.0))
    }
}
impl fmt::Debug for ModuleHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ModuleHash({})", hex(&self.0[..4]))
    }
}

/// BLAKE3 digest of arbitrary payload bytes.
pub fn digest(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hex_roundtrip() {
        let h = ModuleHash::of(b"abc");
        assert_eq!(ModuleHash::from_hex(&h.to_string()), Some(h));
        assert_eq!(ModuleHash::from_hex("zz"), None);
        assert_eq!(NodeId::from_hex(&"é".repeat(32)), None);
    }
    #[test]
    fn ids_display_distinct() {
        assert_eq!(JobId(3).to_string(), "job-3");
        assert_eq!(TaskId(3).to_string(), "task-3");
    }
}
