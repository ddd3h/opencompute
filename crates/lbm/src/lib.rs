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
pub mod codec;

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
///
/// Rows are stored with `g_lo` ghost rows below and `g_hi` above (only on sides that have a
/// neighbour). The ghost rows hold a copy of the neighbour's boundary rows. A ghost of depth `k`
/// is refreshed every `k` steps ("temporal blocking"): after an exchange the window of valid
/// rows is the whole local array, and each step it shrinks by one row on every side that has a
/// neighbour, because the outermost valid row lacks one of its streaming sources. The strip
/// recomputes the shrinking ghost rows itself instead of waiting for a message. Every node
/// still performs exactly the same arithmetic as in a single-strip run, so results stay
/// bit-identical for any depth.
pub struct Strip {
    p: LbmParams,
    g_lo: usize,
    g_hi: usize,
    /// `f[i][ly*nx + x]` with `ly` in local rows (ghosts included), post-collision.
    f: Vec<Vec<f64>>,
    scratch: Vec<Vec<f64>>,
    /// Valid local rows are `[vlo, vhi)`.
    vlo: usize,
    vhi: usize,
}

fn feq(i: usize, rho: f64, ux: f64, uy: f64) -> f64 {
    let cu = C[i].0 as f64 * ux + C[i].1 as f64 * uy;
    W[i] * rho * (1.0 + 3.0 * cu + 4.5 * cu * cu - 1.5 * (ux * ux + uy * uy))
}

impl Strip {
    fn build(p: LbmParams, g_lo: usize, g_hi: usize, interior: Option<Vec<Vec<f64>>>) -> Self {
        let nrows = g_lo + p.rows + g_hi;
        let n = p.nx * nrows;
        let mut s = Self {
            g_lo,
            g_hi,
            f: (0..9).map(|i| vec![W[i]; n]).collect(),
            scratch: vec![vec![0.0; n]; 9],
            vlo: g_lo,
            vhi: g_lo + p.rows,
            p,
        };
        match interior {
            Some(int) => {
                let off = g_lo * s.p.nx;
                for (dst, src) in s.f.iter_mut().zip(&int) {
                    dst[off..off + src.len()].copy_from_slice(src);
                }
            }
            None => s.collide(g_lo, g_lo + s.p.rows),
        }
        s
    }

    /// Fresh strip at rest (ρ=1, u=0) at step 0, or restored from a [`snapshot`](Self::snapshot).
    /// Ghost depth is 1 on every side that has a neighbour until [`set_depth`](Self::set_depth).
    pub fn new(p: LbmParams, restore: Option<&[u8]>) -> Result<Self, String> {
        p.validate()?;
        let n = p.nx * p.rows;
        let interior = match restore {
            Some(b) => {
                let f = codec::decode(b)?;
                if f.len() != 9 || f.iter().any(|v| v.len() != n) {
                    return Err("snapshot does not match geometry".into());
                }
                Some(f)
            }
            None => None,
        };
        let (g_lo, g_hi) = (usize::from(p.has_lo()), usize::from(p.has_hi()));
        Ok(Self::build(p, g_lo, g_hi, interior))
    }

    /// Largest ghost depth this strip can serve (its own row count).
    pub fn max_depth(&self) -> usize {
        self.p.rows
    }

    /// Set the ghost depth of the lower/upper edge (ignored on sides without a neighbour).
    /// Both strips sharing an edge must use the same depth. Ghost rows become invalid: the
    /// next [`step`](Self::step) needs fresh halos on both sides.
    pub fn set_depth(&mut self, lo: usize, hi: usize) -> Result<(), String> {
        let g_lo = if self.p.has_lo() { lo } else { 0 };
        let g_hi = if self.p.has_hi() { hi } else { 0 };
        if (self.p.has_lo() && !(1..=self.p.rows).contains(&lo))
            || (self.p.has_hi() && !(1..=self.p.rows).contains(&hi))
        {
            return Err("halo depth must be between 1 and the number of rows".into());
        }
        let n = self.p.nx * self.p.rows;
        let off = self.g_lo * self.p.nx;
        let interior: Vec<Vec<f64>> = self.f.iter().map(|v| v[off..off + n].to_vec()).collect();
        let p = self.p.clone();
        *self = Self::build(p, g_lo, g_hi, Some(interior));
        Ok(())
    }

    /// BGK collision with velocity-shift forcing on local rows `[a, b)`, in place.
    fn collide(&mut self, a: usize, b: usize) {
        let (tau, fx) = (self.p.tau, self.p.force_x);
        for k in a * self.p.nx..b * self.p.nx {
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

    /// The boundary rows neighbours need: `(lowest g_lo interior rows, highest g_hi interior
    /// rows)`, 9 populations × rows × nx values each, serialised. A side without a neighbour is
    /// empty.
    pub fn boundary(&self) -> (Vec<u8>, Vec<u8>) {
        self.boundary_for(true, true)
    }

    /// Like [`boundary`](Self::boundary) but only serialises the requested sides.
    pub fn boundary_for(&self, want_lo: bool, want_hi: bool) -> (Vec<u8>, Vec<u8>) {
        let nx = self.p.nx;
        let rows = |first: usize, count: usize| -> Vec<u8> {
            if count == 0 {
                return Vec::new();
            }
            let r: Vec<f64> = (0..9)
                .flat_map(|i| self.f[i][first * nx..(first + count) * nx].iter().copied())
                .collect();
            postcard::to_stdvec(&r).expect("encode")
        };
        let lo = if want_lo {
            rows(self.g_lo, self.g_lo)
        } else {
            Vec::new()
        };
        let hi = if want_hi {
            rows(self.g_lo + self.p.rows - self.g_hi, self.g_hi)
        } else {
            Vec::new()
        };
        (lo, hi)
    }

    fn decode_halo(&self, b: &[u8], depth: usize) -> Result<Vec<f64>, String> {
        let v: Vec<f64> = postcard::from_bytes(b).map_err(|e| format!("bad halo: {e}"))?;
        if v.len() != 9 * depth * self.p.nx {
            return Err("halo has wrong size".into());
        }
        Ok(v)
    }

    fn install_ghost(&mut self, first_row: usize, depth: usize, v: &[f64]) {
        let nx = self.p.nx;
        for i in 0..9 {
            let src = &v[i * depth * nx..(i + 1) * depth * nx];
            self.f[i][first_row * nx..(first_row + depth) * nx].copy_from_slice(src);
        }
    }

    /// Advance one step.
    ///
    /// `lo`/`hi` carry the neighbour's boundary rows of the *current* step when an exchange
    /// happens (always, with depth 1). They may be `None` while the ghost rows of that side
    /// are still valid (depth > 1). A side without a neighbour must be `None`.
    pub fn step(&mut self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<(), String> {
        let (nx, nrows) = (self.p.nx, self.g_lo + self.p.rows + self.g_hi);
        if (lo.is_some() && !self.p.has_lo()) || (hi.is_some() && !self.p.has_hi()) {
            return Err("halo given for a side without a neighbour".into());
        }
        if let Some(b) = lo {
            let v = self.decode_halo(b, self.g_lo)?;
            self.install_ghost(0, self.g_lo, &v);
            self.vlo = 0;
        }
        if let Some(b) = hi {
            let v = self.decode_halo(b, self.g_hi)?;
            self.install_ghost(self.g_lo + self.p.rows, self.g_hi, &v);
            self.vhi = nrows;
        }
        if (self.p.has_lo() && self.vlo >= self.g_lo)
            || (self.p.has_hi() && self.vhi <= self.g_lo + self.p.rows)
        {
            return Err("ghost rows exhausted: halo missing".into());
        }
        let a = if self.p.has_lo() { self.vlo + 1 } else { 0 };
        let b = if self.p.has_hi() { self.vhi - 1 } else { nrows };
        for i in 0..9 {
            let (cx, cy) = C[i];
            for y in a..b {
                let sy = y as i32 - cy;
                for x in 0..nx {
                    let sx = (x as i32 - cx).rem_euclid(nx as i32) as usize;
                    self.scratch[i][y * nx + x] = if sy < 0 || sy >= nrows as i32 {
                        self.f[OPP[i]][y * nx + x] // wall: bounce back
                    } else {
                        self.f[i][sy as usize * nx + sx]
                    };
                }
            }
        }
        std::mem::swap(&mut self.f, &mut self.scratch);
        self.vlo = a;
        self.vhi = b;
        self.collide(a, b);
        Ok(())
    }

    /// Serialise the full state (for checkpoints and final results). Interior rows only; they
    /// are valid after every step regardless of the ghost depth.
    pub fn snapshot(&self) -> Vec<u8> {
        let n = self.p.nx * self.p.rows;
        let off = self.g_lo * self.p.nx;
        let interior: Vec<Vec<f64>> = self.f.iter().map(|v| v[off..off + n].to_vec()).collect();
        codec::encode(&interior)
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
        let f = codec::decode(s)?;
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
            codec::encode(&f)
        })
        .collect();
    Ok((newp, Some(out)))
}

/// Streamwise velocity per row (averaged over x), including the half-force correction,
/// from the snapshots of all strips in order. Index = global row.
pub fn velocity_profile(params: &[LbmParams], snapshots: &[Vec<u8>]) -> Result<Vec<f64>, String> {
    let mut out = vec![];
    for (p, s) in params.iter().zip(snapshots) {
        let f = codec::decode(s)?;
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

/// Like [`run_local`] but continues from optional `snapshots` for `steps` steps with a ghost
/// depth per edge (`depths[e]` for the edge between strip `e` and `e + 1`). Edge `e` exchanges
/// at the steps where `(t - start) % depths[e] == 0`, counting steps from this call's start,
/// exactly as the distributed workers do. Reference driver for tests.
pub fn run_local_depth(
    params: &[LbmParams],
    snapshots: Option<&[Vec<u8>]>,
    steps: u64,
    depths: &[usize],
) -> Result<Vec<Vec<u8>>, String> {
    if depths.len() + 1 != params.len() {
        return Err("need one depth per edge".into());
    }
    let mut strips: Vec<Strip> = params
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let mut s = Strip::new(p.clone(), snapshots.map(|v| v[i].as_slice()))?;
            let lo = i.checked_sub(1).map_or(1, |e| depths[e]);
            let hi = depths.get(i).copied().unwrap_or(1);
            s.set_depth(lo, hi)?;
            Ok(s)
        })
        .collect::<Result<_, String>>()?;
    for t in 0..steps {
        let bs: Vec<_> = strips.iter().map(Strip::boundary).collect();
        for (i, s) in strips.iter_mut().enumerate() {
            let lo = i
                .checked_sub(1)
                .filter(|e| t % depths[*e] as u64 == 0)
                .map(|e| bs[e].1.as_slice());
            let hi = depths
                .get(i)
                .filter(|d| t % **d as u64 == 0)
                .map(|_| bs[i + 1].0.as_slice());
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
