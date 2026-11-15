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
