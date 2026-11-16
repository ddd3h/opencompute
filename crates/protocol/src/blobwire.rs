//! Lossless wire encoding of shared input blobs.
//!
//! A blob of `f64` data (e.g. positions) is split into byte lanes (all byte 0, then all byte 1,
//! ...) so the slowly varying sign/exponent bytes sit together, then compressed with zstd. The
//! transform is exactly reversible, so every bit survives. Blobs that do not shrink are sent raw.
//!
//! Layout: `tag: u8` then, for tag 0 the raw bytes; for tag 1 `raw_len: u64 LE | zstd(lanes)`.
//! The blob hash always covers the *decoded* bytes.

const RAW: u8 = 0;
const LANES_ZSTD: u8 = 1;
const ZSTD_LEVEL: i32 = 1;
/// Below this size compression is not worth the CPU.
const MIN_COMPRESS_LEN: usize = 4096;

/// Encode `blob` for transfer.
pub fn encode(blob: &[u8]) -> Vec<u8> {
    if blob.len() >= MIN_COMPRESS_LEN && blob.len().is_multiple_of(8) {
        let n = blob.len() / 8;
        let mut lanes = vec![0u8; blob.len()];
        for (i, w) in blob.as_chunks::<8>().0.iter().enumerate() {
            for (l, b) in w.iter().enumerate() {
                lanes[l * n + i] = *b;
            }
        }
        if let Ok(z) = zstd::bulk::compress(&lanes, ZSTD_LEVEL)
            && z.len() + 9 < blob.len()
        {
            let mut out = Vec::with_capacity(9 + z.len());
            out.push(LANES_ZSTD);
            out.extend_from_slice(&(blob.len() as u64).to_le_bytes());
            out.extend_from_slice(&z);
            return out;
        }
    }
    let mut out = Vec::with_capacity(1 + blob.len());
    out.push(RAW);
    out.extend_from_slice(blob);
    out
}

/// Decode a transfer produced by [`encode`]; `max_len` bounds the decoded size.
pub fn decode(wire: &[u8], max_len: usize) -> Result<Vec<u8>, String> {
    let (&tag, rest) = wire.split_first().ok_or("empty blob transfer")?;
    match tag {
        RAW if rest.len() <= max_len => Ok(rest.to_vec()),
        RAW => Err("blob too large".into()),
        LANES_ZSTD => {
            let (len, z) = rest.split_first_chunk::<8>().ok_or("truncated blob")?;
            let len = usize::try_from(u64::from_le_bytes(*len)).map_err(|_| "blob too large")?;
            if len > max_len || !len.is_multiple_of(8) {
                return Err("bad blob length".into());
            }
            let lanes = zstd::bulk::decompress(z, len).map_err(|e| format!("bad blob: {e}"))?;
            if lanes.len() != len {
                return Err("blob size mismatch".into());
            }
            let n = len / 8;
            let mut out = vec![0u8; len];
            for (i, w) in out.as_chunks_mut::<8>().0.iter_mut().enumerate() {
                for (l, b) in w.iter_mut().enumerate() {
                    *b = lanes[l * n + i];
                }
            }
            Ok(out)
        }
        t => Err(format!("unknown blob encoding {t}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_floats_and_odd_sizes() {
        let f: Vec<u8> = (0..4000)
            .flat_map(|i| (((i * 7919) % 2001) as f64 / 1000.0 - 1.0).to_le_bytes())
            .collect();
        let w = encode(&f);
        assert_eq!(w[0], LANES_ZSTD);
        assert!(w.len() < f.len());
        assert_eq!(decode(&w, f.len()).unwrap(), f);
        for len in [1usize, 7, 8, 4095, 5001] {
            let b: Vec<u8> = (0..len).map(|i| (i * 31 % 251) as u8).collect();
            assert_eq!(decode(&encode(&b), len).unwrap(), b);
        }
        assert_eq!(decode(&encode(&[]), 0).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn rejects_bombs_and_garbage() {
        let f = vec![0u8; 1 << 20];
        let w = encode(&f);
        assert!(w.len() < 1000);
        assert!(decode(&w, 1000).is_err());
        assert!(decode(&[], 10).is_err());
        assert!(decode(&[9, 1, 2], 10).is_err());
        assert!(decode(&w[..20], 1 << 20).is_err());
    }
}
