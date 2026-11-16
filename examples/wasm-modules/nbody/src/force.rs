//! Softened gravitational acceleration for equal-mass particles (`m = 1/N`, `G = 1`).
//!
//! The sum over `j` always runs in index order, so the acceleration of particle `i` does not
//! depend on how the particles are split into blocks: any partition gives bit-identical results.
//! This file is compiled both into the WASM module and into the host example that verifies it.

/// Acceleration of particles `start..end`, as `3 * (end - start)` values (x, y, z per particle).
/// `pos` holds `3 * N` coordinates.
pub fn accelerations(pos: &[f64], start: usize, end: usize, eps2: f64) -> Vec<f64> {
    let n = pos.len() / 3;
    let m = 1.0 / n as f64;
    let mut out = Vec::with_capacity(3 * (end - start));
    for i in start..end {
        let (xi, yi, zi) = (pos[3 * i], pos[3 * i + 1], pos[3 * i + 2]);
        let (mut ax, mut ay, mut az) = (0.0f64, 0.0f64, 0.0f64);
        for j in 0..n {
            let dx = pos[3 * j] - xi;
            let dy = pos[3 * j + 1] - yi;
            let dz = pos[3 * j + 2] - zi;
            let r2 = dx * dx + dy * dy + dz * dz + eps2;
            let inv = m / (r2 * r2.sqrt());
            ax += dx * inv;
            ay += dy * inv;
            az += dz * inv;
        }
        out.extend_from_slice(&[ax, ay, az]);
    }
    out
}
