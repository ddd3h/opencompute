//! D2Q9 lattice Boltzmann (BGK, body force) on a channel: periodic in x, half-way bounce-back
//! walls at the global top/bottom, split into horizontal *strips* ("parts") that exchange one
//! halo row per step.
//!
//! # State invariant
//! A [`Strip`] always holds the **post-collision** distributions after step `s`.
//! [`Strip::boundary`] returns its lowest and highest interior rows (all 9 populations).
//! [`Strip::step`] takes the neighbours' boundary rows of the *same* step `s`, streams, then
//! collides → state at step `s+1`.
//!
//! Because every node performs the same arithmetic in the same order regardless of the
//! partitioning, a decomposed run is **bit-identical** to a single-strip run (tested).

use serde::{Deserialize, Serialize};

const C: [(i32, i32); 9] = [
    (0, 0),
    (1, 0),
    (0, 1),
    (-1, 0),
    (0, -1),
    (1, 1),
    (-1, 1),
    (-1, -1),
    (1, -1),
];
const W: [f64; 9] = [
    4.0 / 9.0,
    1.0 / 9.0,
    1.0 / 9.0,
    1.0 / 9.0,
    1.0 / 9.0,
    1.0 / 36.0,
    1.0 / 36.0,
    1.0 / 36.0,
    1.0 / 36.0,
];
const OPP: [usize; 9] = [0, 3, 4, 1, 2, 7, 8, 5, 6];

/// Kernel name used in the protocol.
pub const KERNEL_NAME: &str = "lbm_d2q9";

/// Parameters of one strip.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LbmParams {
    /// Lattice width (periodic).
    pub nx: usize,
    /// Global height (rows between the two walls).
    pub ny_global: usize,
    /// First global row of this strip.
    pub y0: usize,
    /// Number of rows in this strip.
    pub rows: usize,
    /// BGK relaxation time (> 0.5). Viscosity ν = (τ − 0.5)/3.
    pub tau: f64,
    /// Body force per unit mass along x.
    pub force_x: f64,
}

impl LbmParams {
    fn has_lo(&self) -> bool {
        self.y0 > 0
    }
    fn has_hi(&self) -> bool {
        self.y0 + self.rows < self.ny_global
    }
    fn validate(&self) -> Result<(), String> {
        if self.nx < 2
            || self.rows == 0
            || self.y0 + self.rows > self.ny_global
            || self.nx > 1 << 16
            || self.ny_global > 1 << 16
        {
            return Err("bad lbm geometry".into());
        }
        if !(self.tau > 0.5 && self.tau < 10.0) || !self.force_x.is_finite() {
            return Err("bad tau/force".into());
        }
        if self.nx * self.rows > 3_000_000 {
            return Err("strip too large".into());
        }
        Ok(())
    }
}

/// Split a `nx × ny` channel into `parts` strips (as even as possible).
pub fn decompose(nx: usize, ny: usize, parts: usize, tau: f64, force_x: f64) -> Vec<LbmParams> {
    let parts = parts.clamp(1, ny);
    let (base, extra) = (ny / parts, ny % parts);
    let mut y0 = 0;
    (0..parts)
        .map(|i| {
            let rows = base + usize::from(i < extra);
            let p = LbmParams {
                nx,
                ny_global: ny,
                y0,
                rows,
                tau,
                force_x,
            };
            y0 += rows;
            p
        })
        .collect()
}

/// One strip's state.
pub struct Strip {
    p: LbmParams,
    /// `f[i][row*nx + x]`, post-collision.
    f: Vec<Vec<f64>>,
    scratch: Vec<Vec<f64>>,
}

fn feq(i: usize, rho: f64, ux: f64, uy: f64) -> f64 {
    let cu = C[i].0 as f64 * ux + C[i].1 as f64 * uy;
    W[i] * rho * (1.0 + 3.0 * cu + 4.5 * cu * cu - 1.5 * (ux * ux + uy * uy))
}

impl Strip {
    /// Fresh strip at rest (ρ=1, u=0) at step 0, or restored from a [`snapshot`](Self::snapshot).
    pub fn new(p: LbmParams, restore: Option<&[u8]>) -> Result<Self, String> {
        p.validate()?;
        let n = p.nx * p.rows;
        let f = match restore {
            Some(b) => {
                let f: Vec<Vec<f64>> =
                    postcard::from_bytes(b).map_err(|e| format!("bad snapshot: {e}"))?;
                if f.len() != 9 || f.iter().any(|v| v.len() != n) {
                    return Err("snapshot does not match geometry".into());
                }
                f
            }
            None => {
                let mut s = Self {
                    f: (0..9).map(|i| vec![W[i]; n]).collect(),
                    scratch: vec![],
                    p: p.clone(),
                };
                s.collide();
                return Ok(Self {
                    scratch: vec![vec![0.0; n]; 9],
                    ..s
                });
            }
        };
        Ok(Self {
            p,
            f,
            scratch: vec![vec![0.0; n]; 9],
        })
    }

    /// BGK collision with velocity-shift forcing, in place.
    fn collide(&mut self) {
        let (tau, fx) = (self.p.tau, self.p.force_x);
        for k in 0..self.p.nx * self.p.rows {
            let mut rho = 0.0;
            let (mut mx, mut my) = (0.0, 0.0);
            #[allow(clippy::needless_range_loop)]
            for i in 0..9 {
                let v = self.f[i][k];
                rho += v;
                mx += C[i].0 as f64 * v;
                my += C[i].1 as f64 * v;
            }
            let ux = mx / rho + tau * fx;
            let uy = my / rho;
            for i in 0..9 {
                let v = self.f[i][k];
                self.f[i][k] = v - (v - feq(i, rho, ux, uy)) / tau;
            }
        }
    }

    /// `(lowest row, highest row)` of the interior, all 9 populations each, serialised.
    pub fn boundary(&self) -> (Vec<u8>, Vec<u8>) {
        let row = |y: usize| -> Vec<u8> {
            let r: Vec<f64> = (0..9)
                .flat_map(|i| self.f[i][y * self.p.nx..(y + 1) * self.p.nx].to_vec())
                .collect();
            postcard::to_stdvec(&r).expect("encode")
        };
        (row(0), row(self.p.rows - 1))
    }

    fn decode_halo(&self, b: &[u8]) -> Result<Vec<f64>, String> {
        let v: Vec<f64> = postcard::from_bytes(b).map_err(|e| format!("bad halo: {e}"))?;
        if v.len() != 9 * self.p.nx {
            return Err("halo has wrong size".into());
        }
        Ok(v)
    }

    /// Advance one step. `lo`/`hi` are required iff the strip has a lower/upper neighbour.
    pub fn step(&mut self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<(), String> {
        let (nx, h) = (self.p.nx, self.p.rows);
        let lo_g = match (self.p.has_lo(), lo) {
            (true, Some(b)) => Some(self.decode_halo(b)?),
            (false, None) => None,
            _ => return Err("lower halo presence mismatch".into()),
        };
        let hi_g = match (self.p.has_hi(), hi) {
            (true, Some(b)) => Some(self.decode_halo(b)?),
            (false, None) => None,
            _ => return Err("upper halo presence mismatch".into()),
        };
        for i in 0..9 {
            let (cx, cy) = C[i];
            for y in 0..h {
                let sy = y as i32 - cy;
                for x in 0..nx {
                    let sx = (x as i32 - cx).rem_euclid(nx as i32) as usize;
                    self.scratch[i][y * nx + x] = if sy < 0 {
                        match &lo_g {
                            Some(g) => g[i * nx + sx],
                            None => self.f[OPP[i]][y * nx + x], // wall: bounce back
                        }
                    } else if sy >= h as i32 {
                        match &hi_g {
                            Some(g) => g[i * nx + sx],
                            None => self.f[OPP[i]][y * nx + x],
                        }
                    } else {
                        self.f[i][sy as usize * nx + sx]
                    };
                }
            }
        }
        std::mem::swap(&mut self.f, &mut self.scratch);
        self.collide();
        Ok(())
    }

    /// Serialise the full state (for checkpoints and final results).
    pub fn snapshot(&self) -> Vec<u8> {
        postcard::to_stdvec(&self.f).expect("encode")
    }
}

/// Re-split a decomposed domain into `new_parts` strips.
///
/// `params`/`snapshots` describe the current strips (snapshots may be `None` for a fresh start).
/// Returns the new parameters and, if snapshots were given, the matching new snapshots.
/// The global state is unchanged, so continuing from the result is bit-identical to continuing
/// from the old decomposition.
#[allow(clippy::type_complexity)]
pub fn repartition(
    params: &[LbmParams],
    snapshots: Option<&[Vec<u8>]>,
    new_parts: usize,
) -> Result<(Vec<LbmParams>, Option<Vec<Vec<u8>>>), String> {
    let first = params.first().ok_or("no parts")?;
    let newp = decompose(
        first.nx,
        first.ny_global,
        new_parts,
        first.tau,
        first.force_x,
    );
    let Some(snaps) = snapshots else {
        return Ok((newp, None));
    };
    if snaps.len() != params.len() {
        return Err("snapshot count mismatch".into());
    }
    let nx = first.nx;
    // Global rows per direction.
    let mut global: Vec<Vec<f64>> = (0..9)
        .map(|_| Vec::with_capacity(nx * first.ny_global))
        .collect();
    for (p, s) in params.iter().zip(snaps) {
        let f: Vec<Vec<f64>> = postcard::from_bytes(s).map_err(|e| e.to_string())?;
        if f.len() != 9
            || f.iter().any(|v| v.len() != p.nx * p.rows)
            || p.y0 != global[0].len() / nx
        {
            return Err("snapshot does not match geometry".into());
        }
        for i in 0..9 {
            global[i].extend_from_slice(&f[i]);
        }
    }
    if global[0].len() != nx * first.ny_global {
        return Err("strips do not cover the domain".into());
    }
    let out = newp
        .iter()
        .map(|p| {
            let f: Vec<Vec<f64>> = global
                .iter()
                .map(|g| g[p.y0 * nx..(p.y0 + p.rows) * nx].to_vec())
                .collect();
            postcard::to_stdvec(&f).expect("encode")
        })
        .collect();
    Ok((newp, Some(out)))
}

/// Streamwise velocity per row (averaged over x), including the half-force correction,
/// from the snapshots of all strips in order. Index = global row.
pub fn velocity_profile(params: &[LbmParams], snapshots: &[Vec<u8>]) -> Result<Vec<f64>, String> {
    let mut out = vec![];
    for (p, s) in params.iter().zip(snapshots) {
        let f: Vec<Vec<f64>> = postcard::from_bytes(s).map_err(|e| e.to_string())?;
        if f.len() != 9 || f.iter().any(|v| v.len() != p.nx * p.rows) {
            return Err("snapshot does not match geometry".into());
        }
        for y in 0..p.rows {
            let mut acc = 0.0;
            for x in 0..p.nx {
                let k = y * p.nx + x;
                let (mut rho, mut mx) = (0.0, 0.0);
                for i in 0..9 {
                    rho += f[i][k];
                    mx += C[i].0 as f64 * f[i][k];
                }
                acc += mx / rho + 0.5 * p.force_x;
            }
            out.push(acc / p.nx as f64);
        }
    }
    Ok(out)
}

/// Run the decomposition entirely in-process (reference driver for tests and benchmarks).
pub fn run_local(params: &[LbmParams], steps: u64) -> Result<Vec<Vec<u8>>, String> {
    let mut strips: Vec<Strip> = params
        .iter()
        .map(|p| Strip::new(p.clone(), None))
        .collect::<Result<_, _>>()?;
    for _ in 0..steps {
        let bs: Vec<_> = strips.iter().map(Strip::boundary).collect();
        for (i, s) in strips.iter_mut().enumerate() {
            let lo = (i > 0).then(|| bs[i - 1].1.as_slice());
            let hi = (i + 1 < bs.len()).then(|| bs[i + 1].0.as_slice());
            s.step(lo, hi)?;
        }
    }
    Ok(strips.iter().map(Strip::snapshot).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decomposition_covers_domain() {
        let d = decompose(8, 17, 4, 0.8, 1e-5);
        assert_eq!(d.iter().map(|p| p.rows).sum::<usize>(), 17);
        assert_eq!(d[0].y0, 0);
        assert!(d.windows(2).all(|w| w[0].y0 + w[0].rows == w[1].y0));
    }

    #[test]
    fn distributed_equals_single_bitwise() {
        let one = run_local(&decompose(12, 16, 1, 0.9, 2e-5), 200).unwrap();
        let four = run_local(&decompose(12, 16, 4, 0.9, 2e-5), 200).unwrap();
        let a = velocity_profile(&decompose(12, 16, 1, 0.9, 2e-5), &one).unwrap();
        let b = velocity_profile(&decompose(12, 16, 4, 0.9, 2e-5), &four).unwrap();
        assert_eq!(a, b, "halo exchange must be exact");
    }

    #[test]
    fn poiseuille_matches_analytic_profile() {
        let (nx, ny, tau, f) = (4, 16, 0.8, 1e-5);
        let ps = decompose(nx, ny, 4, tau, f);
        let snaps = run_local(&ps, 4000).unwrap();
        let u = velocity_profile(&ps, &snaps).unwrap();
        let nu = (tau - 0.5) / 3.0;
        let h = ny as f64;
        for (y, got) in u.iter().enumerate() {
            let yy = y as f64 + 0.5;
            let want = f / (2.0 * nu) * yy * (h - yy);
            assert!(
                (got - want).abs() < 0.03 * f / (2.0 * nu) * h * h / 4.0,
                "row {y}: {got} vs {want}"
            );
        }
    }

    #[test]
    fn snapshot_restore_continues_identically() {
        let ps = decompose(8, 8, 2, 0.7, 1e-5);
        let mid = run_local(&ps, 50).unwrap();
        let mut strips: Vec<Strip> = ps
            .iter()
            .zip(&mid)
            .map(|(p, s)| Strip::new(p.clone(), Some(s)).unwrap())
            .collect();
        for _ in 0..50 {
            let bs: Vec<_> = strips.iter().map(Strip::boundary).collect();
            for (i, s) in strips.iter_mut().enumerate() {
                s.step(
                    (i > 0).then(|| bs[i - 1].1.as_slice()),
                    (i + 1 < bs.len()).then(|| bs[i + 1].0.as_slice()),
                )
                .unwrap();
            }
        }
        let resumed: Vec<_> = strips.iter().map(Strip::snapshot).collect();
        assert_eq!(resumed, run_local(&ps, 100).unwrap());
    }

    #[test]
    fn repartition_is_lossless_and_continuation_is_identical() {
        let ps = decompose(8, 12, 3, 0.7, 1e-5);
        let mid = run_local(&ps, 40).unwrap();
        for n in [1, 2, 5, 12] {
            let (np, ns) = repartition(&ps, Some(&mid), n).unwrap();
            assert_eq!(np.len(), n);
            // Continue 60 steps from the re-split state.
            let mut strips: Vec<Strip> = np
                .iter()
                .zip(ns.as_ref().unwrap())
                .map(|(p, s)| Strip::new(p.clone(), Some(s)).unwrap())
                .collect();
            for _ in 0..60 {
                let bs: Vec<_> = strips.iter().map(Strip::boundary).collect();
                for (i, s) in strips.iter_mut().enumerate() {
                    s.step(
                        (i > 0).then(|| bs[i - 1].1.as_slice()),
                        (i + 1 < bs.len()).then(|| bs[i + 1].0.as_slice()),
                    )
                    .unwrap();
                }
            }
            let got =
                velocity_profile(&np, &strips.iter().map(Strip::snapshot).collect::<Vec<_>>())
                    .unwrap();
            let want = run_local(&decompose(8, 12, 1, 0.7, 1e-5), 100).unwrap();
            assert_eq!(
                got,
                velocity_profile(&decompose(8, 12, 1, 0.7, 1e-5), &want).unwrap(),
                "parts={n}"
            );
        }
        assert!(repartition(&ps, Some(&mid[..2]), 2).is_err());
    }

    #[test]
    fn rejects_bad_input() {
        let p = decompose(8, 8, 2, 0.7, 0.0);
        assert!(
            Strip::new(
                LbmParams {
                    tau: 0.4,
                    ..p[0].clone()
                },
                None
            )
            .is_err()
        );
        assert!(Strip::new(p[0].clone(), Some(&[1, 2])).is_err());
        let mut s = Strip::new(p[0].clone(), None).unwrap();
        assert!(s.step(None, Some(&[0])).is_err());
        assert!(s.step(None, None).is_err(), "upper neighbour required");
    }
}
