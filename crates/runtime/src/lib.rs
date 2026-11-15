//! Task execution layer. Independent of networking.
//!
//! * [`builtin`] – a few functions compiled into the binary (used for Phase-1 style jobs and tests).
//! * [`wasm`]    – sandboxed WebAssembly execution with memory / fuel / wall-clock limits,
//!   no WASI and no host imports (so no file or network access).

pub mod builtin;
pub mod stateful;
pub mod wasm;

use opencompute_protocol::{ExecLimits, ModuleHash, TaskKind};
use std::time::Duration;

/// Execution errors. The `Display` text is sent back to the server in `TaskFailed`.
#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    /// Unknown built-in name.
    #[error("unknown builtin '{0}'")]
    UnknownBuiltin(String),
    /// Input did not decode.
    #[error("bad input: {0}")]
    BadInput(String),
    /// Module bytes do not match their hash.
    #[error("module hash mismatch")]
    HashMismatch,
    /// Module not available locally.
    #[error("module {0} not loaded")]
    ModuleMissing(ModuleHash),
    /// Module failed validation/compilation or imports something forbidden.
    #[error("invalid module: {0}")]
    InvalidModule(String),
    /// Guest trapped (panic, unreachable, out of bounds ...).
    #[error("guest trapped: {0}")]
    Trap(String),
    /// Guest used more fuel than allowed.
    #[error("fuel exhausted (cpu budget exceeded)")]
    OutOfFuel,
    /// Wall-clock timeout.
    #[error("wall-clock timeout")]
    Timeout,
    /// Memory limit exceeded.
    #[error("memory limit exceeded")]
    OutOfMemory,
    /// Output too large or ABI violation.
    #[error("abi violation: {0}")]
    Abi(String),
}

/// Runs tasks. Cheap to clone-share via `Arc`; execution functions are blocking and must be
/// called from a compute thread (rayon pool / `spawn_blocking`), never from async tasks.
pub struct Executor {
    /// WASM runtime and module cache.
    pub wasm: wasm::WasmRuntime,
}

impl Executor {
    /// New executor. `cache_dir` enables an on-disk module cache.
    pub fn new(cache_dir: Option<std::path::PathBuf>) -> Result<Self, ExecError> {
        Ok(Self {
            wasm: wasm::WasmRuntime::new(cache_dir)?,
        })
    }

    /// Module required by `kind` but not yet loaded.
    pub fn missing_module(&self, kind: &TaskKind) -> Option<ModuleHash> {
        match kind {
            TaskKind::Wasm(h) if !self.wasm.has(h) => Some(*h),
            _ => None,
        }
    }

    /// Run one task synchronously.
    pub fn execute(
        &self,
        kind: &TaskKind,
        input: &[u8],
        limits: &ExecLimits,
        timeout: Duration,
    ) -> Result<Vec<u8>, ExecError> {
        match kind {
            TaskKind::Builtin(name) => builtin::run(name, input),
            TaskKind::Wasm(h) => self.wasm.run(h, input, limits, timeout),
        }
    }
}
