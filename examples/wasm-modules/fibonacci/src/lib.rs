//! Input: `u64` n. Output: `u64` fib(n) mod 2^64 (wrapping), computed iteratively.
fn run(input: &[u8]) -> Vec<u8> {
    let n: u64 = postcard::from_bytes(input).expect("input must be a postcard u64");
    let (mut a, mut b) = (0u64, 1u64);
    for _ in 0..n {
        let next = a.wrapping_add(b);
        a = b;
        b = next;
    }
    postcard::to_stdvec(&a).expect("encode")
}
opencompute_guest::export_task!(run);
