//! Sandboxed WASM execution.
//!
//! # Guest ABI (`wasm32-unknown-unknown`, no WASI)
//! The module must export:
//! * `memory`                      – linear memory
//! * `oc_alloc(len: i32) -> i32`   – returns a pointer to `len` writable bytes
//! * `oc_run(ptr: i32, len: i32) -> i64` – processes the input at `ptr..ptr+len`;
//!   returns `(out_ptr << 32) | out_len`
//!
//! The module must import **nothing**. Because no host functions or WASI are linked,
//! guests cannot touch files, sockets, clocks or the environment.
//!
//! A fresh instance is created per task, so guests cannot keep state between tasks.
//!
//! # Limits
//! * memory: `ExecLimits::max_memory_bytes` (via `StoreLimits`)
//! * cpu: `ExecLimits::max_fuel` (deterministic instruction-ish budget)
//! * wall clock: epoch interruption, ticked every [`EPOCH_TICK_MS`] ms

use crate::ExecError;
use opencompute_protocol::{ExecLimits, MAX_MODULE_LEN, MAX_PAYLOAD_LEN, ModuleHash};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use wasmtime::{Config, Engine, Linker, Module, Store, StoreLimits, StoreLimitsBuilder, Trap};

/// Granularity of the wall-clock timeout.
pub const EPOCH_TICK_MS: u64 = 10;

/// Compiled-module cache plus engine.
pub struct WasmRuntime {
    engine: Engine,
    modules: Mutex<HashMap<ModuleHash, Module>>,
    cache_dir: Option<PathBuf>,
    stop: Arc<AtomicBool>,
}

impl Drop for WasmRuntime {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

struct Ctx {
    limits: StoreLimits,
}

impl WasmRuntime {
    /// Create a runtime. With `cache_dir`, verified module bytes are stored as `<hash>.wasm`.
    pub fn new(cache_dir: Option<PathBuf>) -> Result<Self, ExecError> {
        let mut cfg = Config::new();
        cfg.consume_fuel(true);
        cfg.epoch_interruption(true);
        let engine = Engine::new(&cfg).map_err(|e| ExecError::InvalidModule(e.to_string()))?;
        let stop = Arc::new(AtomicBool::new(false));
        {
            let (engine, stop) = (engine.clone(), stop.clone());
            std::thread::Builder::new()
                .name("oc-epoch".into())
                .spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(Duration::from_millis(EPOCH_TICK_MS));
                        engine.increment_epoch();
                    }
                })
                .expect("spawn epoch thread");
        }
        if let Some(d) = &cache_dir {
            let _ = std::fs::create_dir_all(d);
        }
        Ok(Self {
            engine,
            modules: Mutex::new(HashMap::new()),
            cache_dir,
            stop,
        })
    }

    /// Is the module compiled in memory (or loadable from the disk cache)?
    pub fn has(&self, hash: &ModuleHash) -> bool {
        if self.modules.lock().expect("lock").contains_key(hash) {
            return true;
        }
        self.load_from_disk(hash)
    }

    fn load_from_disk(&self, hash: &ModuleHash) -> bool {
        let Some(dir) = &self.cache_dir else {
            return false;
        };
        let Ok(bytes) = std::fs::read(dir.join(format!("{hash}.wasm"))) else {
            return false;
        };
        // Re-verify: never trust the disk cache blindly.
        self.load(hash, &bytes, false).is_ok()
    }

    /// Verify `bytes` against `hash`, compile and cache them.
    pub fn load(&self, hash: &ModuleHash, bytes: &[u8], persist: bool) -> Result<(), ExecError> {
        if bytes.len() > MAX_MODULE_LEN {
            return Err(ExecError::InvalidModule("module too large".into()));
        }
        if ModuleHash::of(bytes) != *hash {
            return Err(ExecError::HashMismatch);
        }
        let module = Module::new(&self.engine, bytes)
            .map_err(|e| ExecError::InvalidModule(e.to_string()))?;
        if let Some(i) = module.imports().next() {
            return Err(ExecError::InvalidModule(format!(
                "module imports '{}::{}' but no host functions are provided",
                i.module(),
                i.name()
            )));
        }
        if persist && let Some(dir) = &self.cache_dir {
            let tmp = dir.join(format!("{hash}.tmp"));
            if std::fs::write(&tmp, bytes).is_ok() {
                let _ = std::fs::rename(&tmp, dir.join(format!("{hash}.wasm")));
            }
        }
        self.modules.lock().expect("lock").insert(*hash, module);
        Ok(())
    }

    /// Run `oc_run` of a loaded module on `input`.
    pub fn run(
        &self,
        hash: &ModuleHash,
        input: &[u8],
        limits: &ExecLimits,
        timeout: Duration,
    ) -> Result<Vec<u8>, ExecError> {
        let module = {
            let guard = self.modules.lock().expect("lock");
            guard.get(hash).cloned()
        }
        .ok_or(ExecError::ModuleMissing(*hash))?;

        let mut store = Store::new(
            &self.engine,
            Ctx {
                limits: StoreLimitsBuilder::new()
                    .memory_size(limits.max_memory_bytes as usize)
                    .instances(1)
                    .memories(1)
                    .tables(4)
                    .table_elements(100_000)
                    .trap_on_grow_failure(true)
                    .build(),
            },
        );
        store.limiter(|c| &mut c.limits);
        store
            .set_fuel(limits.max_fuel)
            .map_err(|e| ExecError::Abi(e.to_string()))?;
        let ticks = (timeout.as_millis() as u64 / EPOCH_TICK_MS).max(1) + 1;
        store.set_epoch_deadline(ticks);
        store.epoch_deadline_trap();

        let linker: Linker<Ctx> = Linker::new(&self.engine);
        let map_trap = |e: wasmtime::Error| classify(e);
        let instance = linker.instantiate(&mut store, &module).map_err(map_trap)?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| ExecError::Abi("missing export 'memory'".into()))?;
        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "oc_alloc")
            .map_err(|e| ExecError::Abi(format!("oc_alloc: {e}")))?;
        let run = instance
            .get_typed_func::<(i32, i32), i64>(&mut store, "oc_run")
            .map_err(|e| ExecError::Abi(format!("oc_run: {e}")))?;

        let len =
            i32::try_from(input.len()).map_err(|_| ExecError::Abi("input too large".into()))?;
        let ptr = alloc.call(&mut store, len).map_err(map_trap)?;
        memory
            .write(&mut store, ptr as u32 as usize, input)
            .map_err(|_| ExecError::Abi("oc_alloc returned an invalid pointer".into()))?;
        let packed = run.call(&mut store, (ptr, len)).map_err(map_trap)? as u64;
        let (out_ptr, out_len) = ((packed >> 32) as usize, (packed & 0xffff_ffff) as usize);
        if out_len > MAX_PAYLOAD_LEN {
            return Err(ExecError::Abi(format!(
                "output of {out_len} bytes exceeds limit"
            )));
        }
        let mut out = vec![0u8; out_len];
        memory
            .read(&store, out_ptr, &mut out)
            .map_err(|_| ExecError::Abi("oc_run returned an out-of-bounds slice".into()))?;
        Ok(out)
    }
}

fn classify(e: wasmtime::Error) -> ExecError {
    if let Some(t) = e.downcast_ref::<Trap>() {
        return match t {
            Trap::OutOfFuel => ExecError::OutOfFuel,
            Trap::Interrupt => ExecError::Timeout,
            _ => ExecError::Trap(t.to_string()),
        };
    }
    let msg = format!("{e:#}");
    if msg.contains("memory")
        && (msg.contains("limit") || msg.contains("grow") || msg.contains("minimum"))
    {
        ExecError::OutOfMemory
    } else {
        ExecError::Trap(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Echo module: returns input unchanged.
    const ECHO: &str = r#"(module
        (memory (export "memory") 1)
        (global $bump (mut i32) (i32.const 1024))
        (func (export "oc_alloc") (param $n i32) (result i32)
            (local $p i32)
            (local.set $p (global.get $bump))
            (global.set $bump (i32.add (global.get $bump) (local.get $n)))
            (local.get $p))
        (func (export "oc_run") (param $p i32) (param $n i32) (result i64)
            (i64.or (i64.shl (i64.extend_i32_u (local.get $p)) (i64.const 32))
                    (i64.extend_i32_u (local.get $n)))))"#;

    fn rt() -> WasmRuntime {
        WasmRuntime::new(None).unwrap()
    }
    fn load(rt: &WasmRuntime, wat: &str) -> ModuleHash {
        let bytes = wat::parse_str(wat).unwrap();
        let h = ModuleHash::of(&bytes);
        rt.load(&h, &bytes, false).unwrap();
        h
    }
    fn lim() -> ExecLimits {
        ExecLimits {
            max_memory_bytes: 16 << 20,
            max_fuel: 10_000_000,
        }
    }

    #[test]
    fn echo_roundtrip() {
        let rt = rt();
        let h = load(&rt, ECHO);
        let out = rt
            .run(&h, b"hello", &lim(), Duration::from_secs(5))
            .unwrap();
        assert_eq!(out, b"hello");
    }

    #[test]
    fn hash_mismatch_rejected() {
        let rt = rt();
        let bytes = wat::parse_str(ECHO).unwrap();
        let wrong = ModuleHash::of(b"other");
        assert!(matches!(
            rt.load(&wrong, &bytes, false),
            Err(ExecError::HashMismatch)
        ));
    }

    #[test]
    fn imports_are_forbidden() {
        let rt = rt();
        let bytes = wat::parse_str(
            r#"(module (import "env" "read_file" (func)) (memory (export "memory") 1))"#,
        )
        .unwrap();
        let h = ModuleHash::of(&bytes);
        assert!(matches!(
            rt.load(&h, &bytes, false),
            Err(ExecError::InvalidModule(_))
        ));
    }

    #[test]
    fn infinite_loop_runs_out_of_fuel() {
        let rt = rt();
        let h = load(
            &rt,
            r#"(module (memory (export "memory") 1)
                (func (export "oc_alloc") (param i32) (result i32) (i32.const 0))
                (func (export "oc_run") (param i32 i32) (result i64) (loop $l (br $l)) (i64.const 0)))"#,
        );
        let r = rt.run(&h, b"", &lim(), Duration::from_secs(5));
        assert!(matches!(r, Err(ExecError::OutOfFuel)), "{r:?}");
    }

    #[test]
    fn wall_clock_timeout() {
        let rt = rt();
        let h = load(
            &rt,
            r#"(module (memory (export "memory") 1)
                (func (export "oc_alloc") (param i32) (result i32) (i32.const 0))
                (func (export "oc_run") (param i32 i32) (result i64) (loop $l (br $l)) (i64.const 0)))"#,
        );
        let l = ExecLimits {
            max_memory_bytes: 1 << 20,
            max_fuel: u64::MAX,
        };
        let t = std::time::Instant::now();
        let r = rt.run(&h, b"", &l, Duration::from_millis(100));
        assert!(matches!(r, Err(ExecError::Timeout)), "{r:?}");
        assert!(t.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn memory_limit_enforced() {
        let rt = rt();
        let h = load(
            &rt,
            r#"(module (memory (export "memory") 1)
                (func (export "oc_alloc") (param i32) (result i32) (i32.const 0))
                (func (export "oc_run") (param i32 i32) (result i64)
                    (drop (memory.grow (i32.const 1000))) (i64.const 0)))"#,
        );
        let l = ExecLimits {
            max_memory_bytes: 1 << 20,
            max_fuel: 1_000_000,
        };
        let r = rt.run(&h, b"", &l, Duration::from_secs(5));
        assert!(r.is_err(), "growing past the limit must fail: {r:?}");
    }

    #[test]
    fn out_of_bounds_output_is_abi_error() {
        let rt = rt();
        let h = load(
            &rt,
            r#"(module (memory (export "memory") 1)
                (func (export "oc_alloc") (param i32) (result i32) (i32.const 0))
                (func (export "oc_run") (param i32 i32) (result i64)
                    (i64.or (i64.shl (i64.const 70000) (i64.const 32)) (i64.const 10))))"#,
        );
        let r = rt.run(&h, b"", &lim(), Duration::from_secs(5));
        assert!(matches!(r, Err(ExecError::Abi(_))), "{r:?}");
    }

    #[test]
    fn disk_cache_reloads_verified_module() {
        let dir = std::env::temp_dir().join(format!("oc-mod-{}", std::process::id()));
        let bytes = wat::parse_str(ECHO).unwrap();
        let h = ModuleHash::of(&bytes);
        {
            let rt = WasmRuntime::new(Some(dir.clone())).unwrap();
            rt.load(&h, &bytes, true).unwrap();
        }
        let rt2 = WasmRuntime::new(Some(dir.clone())).unwrap();
        assert!(rt2.has(&h));
        // Corrupt the cache → must be rejected.
        let bad = ModuleHash::of(b"x");
        std::fs::write(dir.join(format!("{bad}.wasm")), &bytes).unwrap();
        assert!(!rt2.has(&bad));
        std::fs::remove_dir_all(dir).ok();
    }
}
