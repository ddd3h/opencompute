//! N-body force task. Shared blob: all positions (raw f64 LE). Input (postcard): [`Range`].
//! Output: accelerations of the particles in the range.
mod force;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Range {
    start: u32,
    end: u32,
    eps2: f64,
}

/// Shared blob: positions as raw little-endian f64. Input: postcard [`Range`].
fn run_shared(shared: &[u8], input: &[u8]) -> Vec<u8> {
    let r: Range = postcard::from_bytes(input).expect("input must be a postcard Range");
    let pos: Vec<f64> = shared
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes(c.try_into().expect("8 bytes")))
        .collect();
    let acc = force::accelerations(&pos, r.start as usize, r.end as usize, r.eps2);
    postcard::to_stdvec(&acc).expect("encode")
}
opencompute_guest::export_task_shared!(run_shared);
