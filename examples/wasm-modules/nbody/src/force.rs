//! Softened gravitational acceleration for equal-mass particles (`m = 1/N`, `G = 1`).
//!
//! The sum over `j` always runs in index order, so the acceleration of particle `i` does not
//! depend on how the particles are split into blocks: any partition gives bit-identical results.
//! This file is compiled both into the WASM module and into the host example that verifies it.

/// Fast kernel: 8 particles per pass over `j` (wasm simd128 when built with it), bit-identical to
/// [`accelerations_scalar`] because every particle keeps its own j-ordered sums.
pub fn accelerations(pos: &[f64], start: usize, end: usize, eps2: f64) -> Vec<f64> {
    #[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
    return accelerations_simd::<4>(pos, start, end, eps2);
    #[allow(unreachable_code)]
    accelerations_blocked::<8>(pos, start, end, eps2)
}

/// Reference (unblocked) acceleration of particles `start..end`, as `3 * (end - start)` values (x, y, z per particle).
/// `pos` holds `3 * N` coordinates.
#[allow(dead_code)]
pub fn accelerations_scalar(pos: &[f64], start: usize, end: usize, eps2: f64) -> Vec<f64> {
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

/// Split `pos` (AoS) into x/y/z arrays.
fn soa(pos: &[f64]) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let n = pos.len() / 3;
    let (mut x, mut y, mut z) = (
        Vec::with_capacity(n),
        Vec::with_capacity(n),
        Vec::with_capacity(n),
    );
    for p in pos.as_chunks::<3>().0 {
        x.push(p[0]);
        y.push(p[1]);
        z.push(p[2]);
    }
    (x, y, z)
}

/// Scalar register-blocked: `B` particles per pass over `j`, each with its own j-ordered sums.
pub fn accelerations_blocked<const B: usize>(
    pos: &[f64],
    start: usize,
    end: usize,
    eps2: f64,
) -> Vec<f64> {
    let n = pos.len() / 3;
    let m = 1.0 / n as f64;
    let (x, y, z) = soa(pos);
    let mut out = Vec::with_capacity(3 * (end - start));
    let mut i0 = start;
    while i0 < end {
        // Lanes past `end` repeat the last real particle; their results are dropped.
        let idx: [usize; B] = core::array::from_fn(|k| (i0 + k).min(end - 1));
        let xi: [f64; B] = core::array::from_fn(|k| x[idx[k]]);
        let yi: [f64; B] = core::array::from_fn(|k| y[idx[k]]);
        let zi: [f64; B] = core::array::from_fn(|k| z[idx[k]]);
        let mut ax = [0.0f64; B];
        let mut ay = [0.0f64; B];
        let mut az = [0.0f64; B];
        for ((&xj, &yj), &zj) in x.iter().zip(&y).zip(&z) {
            for k in 0..B {
                let dx = xj - xi[k];
                let dy = yj - yi[k];
                let dz = zj - zi[k];
                let r2 = dx * dx + dy * dy + dz * dz + eps2;
                let inv = m / (r2 * r2.sqrt());
                ax[k] += dx * inv;
                ay[k] += dy * inv;
                az[k] += dz * inv;
            }
        }
        for k in 0..B.min(end - i0) {
            out.extend_from_slice(&[ax[k], ay[k], az[k]]);
        }
        i0 += B;
    }
    out
}

/// wasm simd128: `V` vectors of 2 particles (lanes = different i), same op order as the scalar
/// kernel, so every lane is bit-identical to it (no FMA exists in simd128).
#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
pub fn accelerations_simd<const V: usize>(
    pos: &[f64],
    start: usize,
    end: usize,
    eps2: f64,
) -> Vec<f64> {
    use core::arch::wasm32::*;
    let n = pos.len() / 3;
    let m = 1.0 / n as f64;
    let (x, y, z) = soa(pos);
    let w = 2 * V;
    let mut out = Vec::with_capacity(3 * (end - start));
    let (vm, ve) = (f64x2_splat(m), f64x2_splat(eps2));
    let mut i0 = start;
    while i0 < end {
        let idx: [usize; 16] = core::array::from_fn(|k| (i0 + k).min(end - 1));
        let ld = |a: &[f64], v: usize| f64x2(a[idx[2 * v]], a[idx[2 * v + 1]]);
        let xi: [v128; V] = core::array::from_fn(|v| ld(&x, v));
        let yi: [v128; V] = core::array::from_fn(|v| ld(&y, v));
        let zi: [v128; V] = core::array::from_fn(|v| ld(&z, v));
        let mut ax = [f64x2_splat(0.0); V];
        let mut ay = [f64x2_splat(0.0); V];
        let mut az = [f64x2_splat(0.0); V];
        for ((&xj, &yj), &zj) in x.iter().zip(&y).zip(&z) {
            let (xs, ys, zs) = (f64x2_splat(xj), f64x2_splat(yj), f64x2_splat(zj));
            for v in 0..V {
                let dx = f64x2_sub(xs, xi[v]);
                let dy = f64x2_sub(ys, yi[v]);
                let dz = f64x2_sub(zs, zi[v]);
                let r2 = f64x2_add(
                    f64x2_add(
                        f64x2_add(f64x2_mul(dx, dx), f64x2_mul(dy, dy)),
                        f64x2_mul(dz, dz),
                    ),
                    ve,
                );
                let inv = f64x2_div(vm, f64x2_mul(r2, f64x2_sqrt(r2)));
                ax[v] = f64x2_add(ax[v], f64x2_mul(dx, inv));
                ay[v] = f64x2_add(ay[v], f64x2_mul(dy, inv));
                az[v] = f64x2_add(az[v], f64x2_mul(dz, inv));
            }
        }
        for k in 0..w.min(end - i0) {
            let (v, l) = (k / 2, k % 2);
            let g = |a: v128| {
                if l == 0 {
                    f64x2_extract_lane::<0>(a)
                } else {
                    f64x2_extract_lane::<1>(a)
                }
            };
            out.extend_from_slice(&[g(ax[v]), g(ay[v]), g(az[v])]);
        }
        i0 += w;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fast_kernel_is_bit_identical() {
        let mut s = 7u64;
        let pos: Vec<f64> = (0..3 * 61)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                (s >> 11) as f64 / (1u64 << 53) as f64
            })
            .collect();
        let r = accelerations_scalar(&pos, 3, 50, 1e-4);
        let bits = |v: &[f64]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
        assert_eq!(bits(&r), bits(&accelerations(&pos, 3, 50, 1e-4)));
        assert_eq!(bits(&r), bits(&accelerations_blocked::<8>(&pos, 3, 50, 1e-4)));
        assert_eq!(bits(&r), bits(&accelerations_blocked::<3>(&pos, 3, 50, 1e-4)));
    }
}
