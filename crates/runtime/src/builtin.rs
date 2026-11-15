//! Built-in task functions. Inputs/outputs are postcard-encoded.
//!
//! | name           | input            | output | meaning                                  |
//! |----------------|------------------|--------|------------------------------------------|
//! | `prime_count`  | `(u64 lo, u64 hi)` | `u64`  | number of primes in `[lo, hi)`           |
//! | `sleep_square` | `(u64 ms, u64 n)`  | `u64`  | sleep `ms` then return `n*n` (testing)   |
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
    match name {
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
