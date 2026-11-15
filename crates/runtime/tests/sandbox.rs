//! Sandbox behaviour of `opencompute_runtime::wasm::WasmRuntime`: traps must become `Err`,
//! resource limits must hold, the ABI must be validated, and one runtime must be safely
//! shareable across threads.
//!
//! These tests use `wat` only; no pre-built guest modules are needed.

use opencompute_protocol::{ExecLimits, MAX_PAYLOAD_LEN, ModuleHash};
use opencompute_runtime::ExecError;
use opencompute_runtime::wasm::WasmRuntime;
use std::time::Duration;
use wat::parse_str;

/// Echo guest: copies the input back to the host. Valid ABI, no imports.
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

/// Compile `wat_bytes` and load it; panics if loading fails.
fn load(rt: &WasmRuntime, wat: &str) -> ModuleHash {
    let bytes = parse_str(wat).unwrap();
    let hash = ModuleHash::of(&bytes);
    rt.load(&hash, &bytes, false).unwrap();
    hash
}

/// Limits matching the README's per-task defaults for these tests: 16 MiB, plenty of fuel.
fn lim() -> ExecLimits {
    ExecLimits {
        max_memory_bytes: 16 << 20,
        max_fuel: 1_000_000_000,
    }
}

/// A guest that recurses forever. The host must see a trap (`Err`), not a process abort.
#[test]
fn infinite_recursion_traps_instead_of_crashing() {
    let rt = rt();
    let h = load(
        &rt,
        r#"(module
            (memory (export "memory") 1)
            (func (export "oc_alloc") (param i32) (result i32) (i32.const 0))
            (func $rec (export "oc_run") (param i32 i32) (result i64)
                (call $rec (local.get 0) (local.get 1))))"#,
    );
    // Generous fuel so the call stack, not the fuel budget, is the limiting resource.
    let limits = ExecLimits {
        max_memory_bytes: 16 << 20,
        max_fuel: u64::MAX - 1,
    };
    let r = rt.run(&h, b"", &limits, Duration::from_secs(5));
    match r {
        Err(ExecError::Trap(msg)) => {
            assert!(
                msg.contains("stack"),
                "expected a call-stack exhaustion trap, got: {msg}"
            );
        }
        other => panic!("expected a stack-overflow trap, got {other:?}"),
    }
}

/// A module whose linear memory starts at 4 GiB cannot fit a 16 MiB sandbox.
/// Wasmtime may reject it at compile time or at instantiation; both are acceptable, but it
/// must never be executed with a smaller memory than it asked for.
#[test]
fn huge_memory_minimum_is_rejected_or_fails() {
    let rt = rt();
    let bytes = parse_str(
        r#"(module
            (memory (export "memory") 65536)
            (func (export "oc_alloc") (param i32) (result i32) (i32.const 0))
            (func (export "oc_run") (param i32 i32) (result i64) (i64.const 0)))"#,
    )
    .unwrap();
    let h = ModuleHash::of(&bytes);

    if rt.load(&h, &bytes, false).is_err() {
        // Rejected before it can ever run: fine.
        return;
    }
    let limits = ExecLimits {
        max_memory_bytes: 16 << 20,
        max_fuel: 1_000_000,
    };
    let r = rt.run(&h, b"", &limits, Duration::from_secs(5));
    assert!(
        matches!(
            r,
            Err(ExecError::OutOfMemory) | Err(ExecError::Trap(_)) | Err(ExecError::Abi(_))
        ),
        "a 4 GiB memory minimum must not run under a 16 MiB limit: {r:?}"
    );
}

/// Outputs larger than `MAX_PAYLOAD_LEN` are an ABI violation, checked before any copy.
#[test]
fn oversized_output_length_is_abi_error() {
    let rt = rt();
    let out_len = MAX_PAYLOAD_LEN + 1;
    let h = load(
        &rt,
        &format!(
            r#"(module
                (memory (export "memory") 1)
                (func (export "oc_alloc") (param i32) (result i32) (i32.const 0))
                (func (export "oc_run") (param i32 i32) (result i64)
                    (i64.const {out_len})))"#
        ),
    );
    let r = rt.run(&h, b"", &lim(), Duration::from_secs(5));
    match r {
        Err(ExecError::Abi(msg)) => assert!(
            msg.contains("exceeds"),
            "expected an 'exceeds limit' ABI error, got: {msg}"
        ),
        other => panic!("expected an ABI error for an oversized output, got {other:?}"),
    }
}

/// A module can load without `oc_run`; the missing export must surface as an ABI error at run.
#[test]
fn missing_oc_run_is_abi_error_at_run() {
    let rt = rt();
    let h = load(
        &rt,
        r#"(module
            (memory (export "memory") 1)
            (func (export "oc_alloc") (param i32) (result i32) (i32.const 0)))"#,
    );
    let r = rt.run(&h, b"", &lim(), Duration::from_secs(5));
    assert!(
        matches!(&r, Err(ExecError::Abi(msg)) if msg.contains("oc_run")),
        "expected a missing-`oc_run` ABI error, got {r:?}"
    );
}

/// A start function that never returns is stopped by the fuel budget, not by the wall clock.
#[test]
fn looping_start_function_hits_fuel_limit() {
    let rt = rt();
    let h = load(
        &rt,
        r#"(module
            (memory (export "memory") 1)
            (func $spin (loop $l (br $l)))
            (start $spin))"#,
    );
    let limits = ExecLimits {
        max_memory_bytes: 1 << 20,
        max_fuel: 100_000,
    };
    let started = std::time::Instant::now();
    let r = rt.run(&h, b"", &limits, Duration::from_secs(30));
    assert!(
        matches!(r, Err(ExecError::OutOfFuel)),
        "expected OutOfFuel from a looping start function, got {r:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "fuel limit should stop the loop quickly, took {:?}",
        started.elapsed()
    );
}

/// Imported linear memory would let a guest name host-owned state, so load() must refuse it.
#[test]
fn imported_memory_is_rejected_by_load() {
    let rt = rt();
    let bytes = parse_str(
        r#"(module
            (import "env" "memory" (memory 1))
            (export "memory" (memory 0)))"#,
    )
    .unwrap();
    let h = ModuleHash::of(&bytes);
    let r = rt.load(&h, &bytes, false);
    assert!(
        matches!(r, Err(ExecError::InvalidModule(_))),
        "imported memory must be rejected, got {r:?}"
    );
}

/// Same for imported tables: no host tables are provided, so load() must refuse them.
#[test]
fn imported_table_is_rejected_by_load() {
    let rt = rt();
    let bytes = parse_str(
        r#"(module
            (import "env" "table" (table 1 funcref))
            (export "table" (table 0))
            (memory (export "memory") 1))"#,
    )
    .unwrap();
    let h = ModuleHash::of(&bytes);
    let r = rt.load(&h, &bytes, false);
    assert!(
        matches!(r, Err(ExecError::InvalidModule(_))),
        "imported table must be rejected, got {r:?}"
    );
}

/// `WasmRuntime` is documented as cheap to share (`Arc`); `&rt` must be usable from many
/// threads at once, each running the same compiled module.
#[test]
fn same_module_runs_concurrently_from_eight_threads() {
    fn assert_sync<T: Sync>() {}
    assert_sync::<WasmRuntime>();

    let rt = rt();
    let h = load(&rt, ECHO);
    assert!(rt.has(&h));

    let results: Vec<Vec<Vec<u8>>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..8)
            .map(|t| {
                let rt = &rt;
                s.spawn(move || {
                    (0..4)
                        .map(|i| {
                            let input = format!("thread-{t}-round-{i}");
                            rt.run(&h, input.as_bytes(), &lim(), Duration::from_secs(10))
                                .unwrap_or_else(|e| panic!("thread {t} run {i} failed: {e}"))
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("worker thread panicked"))
            .collect()
    });

    for (t, per_thread) in results.iter().enumerate() {
        for (i, out) in per_thread.iter().enumerate() {
            assert_eq!(out, format!("thread-{t}-round-{i}").as_bytes());
        }
    }
}
