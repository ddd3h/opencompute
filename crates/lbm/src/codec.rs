//! Compact, lossless snapshot encoding.
//!
//! A snapshot holds `Vec<Vec<f64>>` (one vector per lattice direction). The 8-byte values are
//! split into byte lanes (all byte 0, then all byte 1, ...) so that the slowly varying sign and
//! exponent bytes sit next to each other, then compressed with zstd. The transform is exactly
//! reversible, so restoring a snapshot reproduces every bit and results stay bit-identical.
//!
//! Layout: `MAGIC | n_vectors: u8 | n_vectors * len: u32 LE | zstd(lane-shuffled bytes)`.
//! Snapshots written before this encoding (plain postcard) are still readable.

const MAGIC: &[u8; 4] = b"OCZ1";
/// Upper bound for the decoded size of one snapshot; guards against decompression bombs.
const MAX_DECODED_BYTES: usize = 1 << 30;
const ZSTD_LEVEL: i32 = 1;

/// Encode a snapshot.
pub fn encode(f: &[Vec<f64>]) -> Vec<u8> {
    let total: usize = f.iter().map(Vec::len).sum();
    let mut shuffled = vec![0u8; total * 8];
    let mut k = 0;
    for v in f {
        for x in v {
            for (lane, b) in x.to_le_bytes().iter().enumerate() {
                shuffled[lane * total + k] = *b;
            }
            k += 1;
        }
    }
    let packed = zstd::bulk::compress(&shuffled, ZSTD_LEVEL).expect("zstd compress");
    let mut out = Vec::with_capacity(5 + 4 * f.len() + packed.len());
    out.extend_from_slice(MAGIC);
    out.push(u8::try_from(f.len()).expect("at most 255 vectors"));
    for v in f {
        out.extend_from_slice(
            &u32::try_from(v.len())
                .expect("vector too long")
                .to_le_bytes(),
        );
    }
    out.extend_from_slice(&packed);
    out
}

/// Decode a snapshot produced by [`encode`], or by the earlier plain-postcard format.
pub fn decode(b: &[u8]) -> Result<Vec<Vec<f64>>, String> {
    let Some(rest) = b.strip_prefix(MAGIC.as_slice()) else {
        return postcard::from_bytes(b).map_err(|e| format!("bad snapshot: {e}"));
    };
    let (&n, rest) = rest.split_first().ok_or("truncated snapshot")?;
    let n = n as usize;
    if rest.len() < 4 * n {
        return Err("truncated snapshot".into());
    }
    let (lens_raw, packed) = rest.split_at(4 * n);
    let lens: Vec<usize> = lens_raw
        .as_chunks::<4>()
        .0
        .iter()
        .map(|c| u32::from_le_bytes(*c) as usize)
        .collect();
    let total = lens
        .iter()
        .try_fold(0usize, |a, l| a.checked_add(*l))
        .ok_or("snapshot too large")?;
    let bytes = total.checked_mul(8).ok_or("snapshot too large")?;
    if bytes > MAX_DECODED_BYTES {
        return Err("snapshot too large".into());
    }
    let shuffled =
        zstd::bulk::decompress(packed, bytes).map_err(|e| format!("bad snapshot: {e}"))?;
    if shuffled.len() != bytes {
        return Err("snapshot size mismatch".into());
    }
    let mut out = Vec::with_capacity(n);
    let mut k = 0;
    for l in lens {
        let mut v = Vec::with_capacity(l);
        for _ in 0..l {
            let mut a = [0u8; 8];
            for (lane, slot) in a.iter_mut().enumerate() {
                *slot = shuffled[lane * total + k];
            }
            v.push(f64::from_le_bytes(a));
            k += 1;
        }
        out.push(v);
    }
    Ok(out)
}
