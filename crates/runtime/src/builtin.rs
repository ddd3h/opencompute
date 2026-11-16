//! Built-in task functions. Inputs/outputs are postcard-encoded.
//!
//! | name           | input            | output | meaning                                  |
//! |----------------|------------------|--------|------------------------------------------|
//! | `prime_count`  | `(u64 lo, u64 hi)` | `u64`  | number of primes in `[lo, hi)`           |
//! | `sleep_square` | `(u64 ms, u64 n)`  | `u64`  | sleep `ms` then return `n*n` (testing)   |
//! | `nbody_force`  | shared xyz f64 LE + `(u32 start, u32 end, f64 eps2)` | `Vec<f64>` | acceleration |
//! | `echo`         | any bytes        | same   | identity                                 |

use crate::ExecError;

/// Count primes in `[lo, hi)` by trial division. Deterministic CPU work.
pub fn prime_count(lo: u64, hi: u64) -> u64 {
    (lo.max(2)..hi).filter(|&n| is_prime(n)).count() as u64
}

fn is_prime(n: u64) -> bool {
    if n < 2 {
        return false;
    }
    if n.is_multiple_of(2) {
        return n == 2;
    }
    let mut d = 3;
    while d * d <= n {
        if n.is_multiple_of(d) {
            return false;
        }
        d += 2;
    }
    true
}

fn dec<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T, ExecError> {
    postcard::from_bytes(b).map_err(|e| ExecError::BadInput(e.to_string()))
}
fn enc<T: serde::Serialize>(v: &T) -> Vec<u8> {
    postcard::to_stdvec(v).expect("serialisable")
}

/// Run a built-in by name.
pub fn run(name: &str, input: &[u8]) -> Result<Vec<u8>, ExecError> {
    run_shared(name, &[], input)
}

/// Run a builtin with raw shared bytes and postcard task input.
pub fn run_shared(name: &str, shared: &[u8], input: &[u8]) -> Result<Vec<u8>, ExecError> {
    match name {
        "nbody_force" => nbody_force(shared, input),
        "prime_count" => {
            let (lo, hi): (u64, u64) = dec(input)?;
            Ok(enc(&prime_count(lo, hi)))
        }
        "sleep_square" => {
            let (ms, n): (u64, u64) = dec(input)?;
            std::thread::sleep(std::time::Duration::from_millis(ms.min(60_000)));
            Ok(enc(&n.wrapping_mul(n)))
        }
        "echo" => Ok(input.to_vec()),
        other => Err(ExecError::UnknownBuiltin(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn primes() {
        assert_eq!(prime_count(0, 10), 4);
        assert_eq!(prime_count(0, 100), 25);
        let out = run("prime_count", &enc(&(0u64, 1000u64))).unwrap();
        assert_eq!(dec::<u64>(&out).unwrap(), 168);
    }
    #[test]
    fn bad_input_and_unknown() {
        assert!(matches!(
            run("prime_count", &[1]),
            Err(ExecError::BadInput(_))
        ));
        assert!(matches!(
            run("nope", &[]),
            Err(ExecError::UnknownBuiltin(_))
        ));
    }
}

// Native force kernel: lanes represent particles, never terms of a sum.

#[path = "../../../examples/wasm-modules/nbody/src/force.rs"]
mod reference;

fn nbody_force(shared: &[u8], input: &[u8]) -> Result<Vec<u8>, ExecError> {
    let (start, end, eps2): (u32, u32, f64) =
        postcard::from_bytes(input).map_err(|e| ExecError::BadInput(e.to_string()))?;
    if !shared.len().is_multiple_of(24) || start > end || end as usize > shared.len() / 24 {
        return Err(ExecError::BadInput(
            "invalid positions or particle range".into(),
        ));
    }
    let pos: Vec<f64> = shared
        .as_chunks::<8>()
        .0
        .iter()
        .map(|b| f64::from_le_bytes(*b))
        .collect();
    postcard::to_stdvec(&accelerations(&pos, start as usize, end as usize, eps2))
        .map_err(|e| ExecError::BadInput(e.to_string()))
}

fn accelerations(pos: &[f64], start: usize, end: usize, eps2: f64) -> Vec<f64> {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: runtime feature detection; all indices are bounded by the range.
        return unsafe { avx2(pos, start, end, eps2) };
    }
    reference::accelerations(pos, start, end, eps2)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2(pos: &[f64], start: usize, end: usize, eps2: f64) -> Vec<f64> {
    use std::arch::x86_64::*;
    let n = pos.len() / 3;
    let m = _mm256_set1_pd(1.0 / n as f64);
    let eps = _mm256_set1_pd(eps2);
    let mut out = Vec::with_capacity(3 * (end - start));
    let mut i = start;
    // Two independent vectors (eight particles) hide sqrt/div latency.
    while end - i >= 8 {
        let x = std::array::from_fn::<_, 2, _>(|b| {
            _mm256_setr_pd(
                pos[3 * (i + 4 * b)],
                pos[3 * (i + 4 * b + 1)],
                pos[3 * (i + 4 * b + 2)],
                pos[3 * (i + 4 * b + 3)],
            )
        });
        let y = std::array::from_fn::<_, 2, _>(|b| {
            _mm256_setr_pd(
                pos[3 * (i + 4 * b) + 1],
                pos[3 * (i + 4 * b + 1) + 1],
                pos[3 * (i + 4 * b + 2) + 1],
                pos[3 * (i + 4 * b + 3) + 1],
            )
        });
        let z = std::array::from_fn::<_, 2, _>(|b| {
            _mm256_setr_pd(
                pos[3 * (i + 4 * b) + 2],
                pos[3 * (i + 4 * b + 1) + 2],
                pos[3 * (i + 4 * b + 2) + 2],
                pos[3 * (i + 4 * b + 3) + 2],
            )
        });
        let mut ax = [_mm256_setzero_pd(); 2];
        let mut ay = ax;
        let mut az = ax;
        for j in 0..n {
            let px = _mm256_set1_pd(pos[3 * j]);
            let py = _mm256_set1_pd(pos[3 * j + 1]);
            let pz = _mm256_set1_pd(pos[3 * j + 2]);
            for b in 0..2 {
                let dx = _mm256_sub_pd(px, x[b]);
                let dy = _mm256_sub_pd(py, y[b]);
                let dz = _mm256_sub_pd(pz, z[b]);
                let r2 = _mm256_add_pd(
                    _mm256_add_pd(
                        _mm256_add_pd(_mm256_mul_pd(dx, dx), _mm256_mul_pd(dy, dy)),
                        _mm256_mul_pd(dz, dz),
                    ),
                    eps,
                );
                let inv = _mm256_div_pd(m, _mm256_mul_pd(r2, _mm256_sqrt_pd(r2)));
                ax[b] = _mm256_add_pd(ax[b], _mm256_mul_pd(dx, inv));
                ay[b] = _mm256_add_pd(ay[b], _mm256_mul_pd(dy, inv));
                az[b] = _mm256_add_pd(az[b], _mm256_mul_pd(dz, inv));
            }
        }
        for b in 0..2 {
            let mut a = [[0.0; 4]; 3];
            // SAFETY: each destination holds four doubles; unaligned stores are permitted.
            unsafe {
                _mm256_storeu_pd(a[0].as_mut_ptr(), ax[b]);
                _mm256_storeu_pd(a[1].as_mut_ptr(), ay[b]);
                _mm256_storeu_pd(a[2].as_mut_ptr(), az[b]);
            }
            for ((x, y), z) in a[0].iter().zip(&a[1]).zip(&a[2]) {
                out.extend_from_slice(&[*x, *y, *z]);
            }
        }
        i += 8;
    }
    out.extend(reference::accelerations(pos, i, end, eps2));
    out
}

#[cfg(test)]
mod nbody_tests {
    use super::*;
    fn positions(n: usize) -> Vec<f64> {
        let mut s = 123456789u64;
        (0..3 * n)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                (s >> 11) as f64 / (1u64 << 53) as f64 * 2.0 - 1.0
            })
            .collect()
    }
    #[test]
    fn bit_identity() {
        for n in [0, 1, 7, 8, 9, 17, 65, 257] {
            let p = positions(n);
            for start in 0..=n.min(9) {
                for eps in [0.0025, 1e-12, 1.0] {
                    let expected = reference::accelerations(&p, start, n, eps);
                    let actual = accelerations(&p, start, n, eps);
                    assert_eq!(
                        actual.iter().map(|x| x.to_bits()).collect::<Vec<_>>(),
                        expected.iter().map(|x| x.to_bits()).collect::<Vec<_>>()
                    );
                }
            }
        }
    }
    #[test]
    fn shared_wire_and_validation() {
        let p = positions(19);
        let blob: Vec<u8> = p.iter().flat_map(|x| x.to_le_bytes()).collect();
        let input = postcard::to_stdvec(&(1u32, 18u32, 0.0025f64)).unwrap();
        let out = crate::builtin::run_shared("nbody_force", &blob, &input).unwrap();
        assert_eq!(
            out,
            postcard::to_stdvec(&reference::accelerations(&p, 1, 18, 0.0025)).unwrap()
        );
        assert!(nbody_force(&blob[..blob.len() - 1], &input).is_err());
        assert!(nbody_force(&[], &input).is_err());
        assert!(nbody_force(&blob, &[255]).is_err());
        for range in [(18u32, 1u32, 0.0025f64), (0, 20, 0.0025)] {
            assert!(nbody_force(&blob, &postcard::to_stdvec(&range).unwrap()).is_err());
        }
    }
    #[test]
    #[ignore = "single-thread release benchmark; run with --release --ignored --nocapture"]
    fn benchmark() {
        let n = 8192;
        let p = positions(n);
        for (name, kernel) in [
            (
                "scalar",
                reference::accelerations as fn(&[f64], usize, usize, f64) -> Vec<f64>,
            ),
            ("native", accelerations),
        ] {
            let t = std::time::Instant::now();
            let result = kernel(std::hint::black_box(&p), 0, n, 0.0025);
            std::hint::black_box(result);
            let elapsed = t.elapsed();
            eprintln!(
                "{name}: {elapsed:?}, {:.3} Ginteractions/s",
                (n * n) as f64 / elapsed.as_secs_f64() / 1e9
            );
        }
    }
}
