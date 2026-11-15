//! Stateful kernels: workers keep state between steps and exchange boundary ("halo") data
//! with their chain neighbours through the server each step.
//!
//! Contract (see [`Kernel`]): state is *at step s*; `boundary()` returns the data neighbours need
//! for step `s+1`; `step()` consumes the neighbours' boundaries of step `s` and moves to `s+1`.
//! A snapshot taken at step `s` must restore to exactly that state.

use crate::ExecError;

/// A stateful, steppable computation (one part of a decomposed domain).
pub trait Kernel: Send {
    /// `(lower, upper)` boundary blobs of the current step.
    fn boundary(&self) -> (Vec<u8>, Vec<u8>);
    /// Advance one step with the neighbours' boundaries (`None` where there is no neighbour).
    fn step(&mut self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<(), String>;
    /// Full serialisable state.
    fn snapshot(&self) -> Vec<u8>;
    /// Like [`boundary`](Kernel::boundary) but only the requested sides need to be filled
    /// (the other may be empty). Lets a kernel skip serialising data that is not sent this step.
    fn boundary_for(&self, _want_lo: bool, _want_hi: bool) -> (Vec<u8>, Vec<u8>) {
        self.boundary()
    }
    /// Use halos that are `lo` / `hi` rows deep on the lower / upper edge, exchanged once every
    /// that many steps (temporal blocking). `boundary()` then returns that many rows and
    /// `step()` receives `None` for a side whose ghost rows are still valid. Kernels that do not
    /// support this accept only depth 1.
    fn set_depth(&mut self, lo: u32, hi: u32) -> Result<(), String> {
        if lo == 1 && hi == 1 {
            Ok(())
        } else {
            Err("this kernel does not support deep halos".into())
        }
    }
}

struct Lbm(opencompute_lbm::Strip);

impl Kernel for Lbm {
    fn boundary(&self) -> (Vec<u8>, Vec<u8>) {
        self.0.boundary()
    }
    fn step(&mut self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<(), String> {
        self.0.step(lo, hi)
    }
    fn snapshot(&self) -> Vec<u8> {
        self.0.snapshot()
    }
    fn boundary_for(&self, want_lo: bool, want_hi: bool) -> (Vec<u8>, Vec<u8>) {
        self.0.boundary_for(want_lo, want_hi)
    }
    fn set_depth(&mut self, lo: u32, hi: u32) -> Result<(), String> {
        self.0.set_depth(lo as usize, hi as usize)
    }
}

/// Create a kernel by name. `restore` is a snapshot produced by the same kernel.
pub fn create(
    name: &str,
    params: &[u8],
    restore: Option<&[u8]>,
) -> Result<Box<dyn Kernel>, ExecError> {
    match name {
        opencompute_lbm::KERNEL_NAME => {
            let p: opencompute_lbm::LbmParams =
                postcard::from_bytes(params).map_err(|e| ExecError::BadInput(e.to_string()))?;
            let strip = opencompute_lbm::Strip::new(p, restore).map_err(ExecError::BadInput)?;
            Ok(Box::new(Lbm(strip)))
        }
        other => Err(ExecError::UnknownBuiltin(format!(
            "stateful kernel '{other}'"
        ))),
    }
}

/// Is `name` a known stateful kernel?
pub fn known(name: &str) -> bool {
    name == opencompute_lbm::KERNEL_NAME
}

/// Re-split a decomposed job into `n` parts (see `opencompute_lbm::repartition`).
/// Returns new per-part parameters and, if `snapshots` was given, the matching snapshots.
#[allow(clippy::type_complexity)]
pub fn repartition(
    kernel: &str,
    params: &[Vec<u8>],
    snapshots: Option<&[Vec<u8>]>,
    n: usize,
) -> Result<(Vec<Vec<u8>>, Option<Vec<Vec<u8>>>), ExecError> {
    if kernel != opencompute_lbm::KERNEL_NAME {
        return Err(ExecError::UnknownBuiltin(format!(
            "stateful kernel '{kernel}'"
        )));
    }
    let ps: Vec<opencompute_lbm::LbmParams> = params
        .iter()
        .map(|p| postcard::from_bytes(p).map_err(|e| ExecError::BadInput(e.to_string())))
        .collect::<Result<_, _>>()?;
    let (np, ns) = opencompute_lbm::repartition(&ps, snapshots, n).map_err(ExecError::BadInput)?;
    Ok((
        np.iter()
            .map(|p| postcard::to_stdvec(p).expect("encode"))
            .collect(),
        ns,
    ))
}

/// Largest halo depth each edge between consecutive parts can use (`params.len() - 1` entries).
/// Kernels without deep-halo support report 1 everywhere.
pub fn edge_limits(kernel: &str, params: &[Vec<u8>]) -> Vec<u32> {
    let one = vec![1; params.len().saturating_sub(1)];
    if kernel != opencompute_lbm::KERNEL_NAME {
        return one;
    }
    let rows: Option<Vec<u32>> = params
        .iter()
        .map(|p| {
            postcard::from_bytes::<opencompute_lbm::LbmParams>(p)
                .ok()
                .map(|p| p.rows as u32)
        })
        .collect();
    match rows {
        Some(r) => r.windows(2).map(|w| w[0].min(w[1]).max(1)).collect(),
        None => one,
    }
}
