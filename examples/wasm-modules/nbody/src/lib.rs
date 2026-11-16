//! N-body force task. Input (postcard): [`Block`]. Output: accelerations of the block's particles.
mod force;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Block {
    /// Positions of all particles, `3 * N` values.
    pos: Vec<f64>,
    start: u32,
    end: u32,
    eps2: f64,
}

fn run(input: &[u8]) -> Vec<u8> {
    let b: Block = postcard::from_bytes(input).expect("input must be a postcard Block");
    let acc = force::accelerations(&b.pos, b.start as usize, b.end as usize, b.eps2);
    postcard::to_stdvec(&acc).expect("encode")
}
opencompute_guest::export_task!(run);
