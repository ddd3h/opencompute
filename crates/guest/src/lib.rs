//! Guest-side helper for writing OpenCompute WASM modules.
//!
//! This crate has **no dependencies** so it compiles for `wasm32-unknown-unknown` instantly and
//! never drags the network stack into a guest.
//!
//! ```ignore
//! // Cargo.toml: crate-type = ["cdylib"], deps: opencompute-guest, postcard (serde)
//! fn run(input: &[u8]) -> Vec<u8> {
//!     let n: u64 = postcard::from_bytes(input).unwrap();
//!     postcard::to_stdvec(&(n * 2)).unwrap()
//! }
//! opencompute_guest::export_task!(run);
//! ```
//!
//! Build: `cargo build --release --target wasm32-unknown-unknown`.
//!
//! # ABI
//! The macro exports `oc_alloc(len) -> ptr` and `oc_run(ptr, len) -> (out_ptr << 32 | out_len)`
//! (see `opencompute-runtime::wasm`). The module must import nothing: no WASI, no host calls.
//! A panic aborts the task (the host reports a trap and may retry elsewhere).
//!
//! The only `unsafe` in the project lives in this macro: it is the FFI boundary that hands
//! raw linear-memory pointers between host and guest.

/// Export `$handler: fn(&[u8]) -> Vec<u8>` as the module's task entry point.
#[macro_export]
macro_rules! export_task {
    ($handler:path) => {
        /// Host calls this to reserve `len` bytes for the task input.
        #[unsafe(no_mangle)]
        pub extern "C" fn oc_alloc(len: i32) -> i32 {
            let mut buf = ::std::vec::Vec::<u8>::with_capacity(len.max(0) as usize);
            let ptr = buf.as_mut_ptr();
            ::std::mem::forget(buf);
            ptr as usize as i32
        }

        /// Host calls this with the input location; returns the packed output location.
        #[unsafe(no_mangle)]
        pub extern "C" fn oc_run(ptr: i32, len: i32) -> i64 {
            // SAFETY: the host wrote exactly `len` bytes at `ptr`, which `oc_alloc` reserved.
            let input = unsafe {
                ::std::slice::from_raw_parts(ptr as usize as *const u8, len.max(0) as usize)
            };
            let out: ::std::vec::Vec<u8> = $handler(input);
            let out = out.leak();
            (((out.as_ptr() as usize as u64) << 32) | out.len() as u64) as i64
        }
    };
}
