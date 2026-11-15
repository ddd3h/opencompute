//! Snapshot codec: lossless round trip, legacy format, malformed input, and compression ratio.

use opencompute_lbm::{codec, decompose, run_local};

#[test]
fn round_trip_is_bit_exact() {
    let f = vec![
        vec![0.0, -0.0, 1.0, f64::MIN_POSITIVE, f64::MAX, f64::INFINITY],
        vec![
            f64::NAN,
            f64::from_bits(0x7ff8_dead_beef_0001),
            1e-310,
            -2.5,
            3.0,
            4.0,
        ],
        vec![],
    ];
    let back = codec::decode(&codec::encode(&f)).unwrap();
    assert_eq!(f.len(), back.len());
    for (a, b) in f.iter().zip(&back) {
        let a: Vec<u64> = a.iter().map(|x| x.to_bits()).collect();
        let b: Vec<u64> = b.iter().map(|x| x.to_bits()).collect();
        assert_eq!(a, b);
    }
}

#[test]
fn reads_legacy_postcard_snapshots() {
    let f = vec![vec![1.0f64, 2.0, 3.0]; 9];
    let legacy = postcard::to_stdvec(&f).unwrap();
    assert_eq!(codec::decode(&legacy).unwrap(), f);
}

#[test]
fn rejects_malformed_input() {
    let good = codec::encode(&[vec![1.0f64; 64]]);
    assert!(codec::decode(&good[..good.len() - 3]).is_err());
    assert!(codec::decode(b"OCZ1").is_err());
    // Header claims 4 GiB of values: must be refused before allocating.
    let mut bomb = b"OCZ1".to_vec();
    bomb.push(1);
    bomb.extend_from_slice(&u32::MAX.to_le_bytes());
    bomb.extend_from_slice(&[0; 8]);
    assert!(codec::decode(&bomb).is_err());
}

/// Prints compression ratios at several times. Run with `--ignored --nocapture`.
#[test]
#[ignore]
fn measure_ratio() {
    let p = decompose(128, 128, 1, 0.8, 1e-5);
    for steps in [0u64, 100, 1000, 5000, 20000] {
        let snap = run_local(&p, steps).unwrap().remove(0);
        let f = codec::decode(&snap).unwrap();
        let plain = postcard::to_stdvec(&f).unwrap().len();
        println!(
            "step {steps:>6}: plain {plain} B, encoded {} B, ratio {:.2}x",
            snap.len(),
            plain as f64 / snap.len() as f64
        );
    }
}
