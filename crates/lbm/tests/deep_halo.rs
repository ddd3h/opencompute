//! Temporal blocking: deep ghost zones must reproduce the single-strip result bit for bit.

use opencompute_lbm::{LbmParams, codec, decompose, run_local, run_local_depth};

fn bits(snaps: &[Vec<u8>]) -> Vec<Vec<u64>> {
    // Concatenate the strips in order; compare per population.
    let mut out = vec![vec![]; 9];
    for s in snaps {
        for (i, v) in codec::decode(s).unwrap().iter().enumerate() {
            out[i].extend(v.iter().map(|x| x.to_bits()));
        }
    }
    out
}

fn uneven(nx: usize, rows: &[usize]) -> Vec<LbmParams> {
    let mut y0 = 0;
    rows.iter()
        .map(|&r| {
            let p = LbmParams {
                nx,
                ny_global: rows.iter().sum(),
                y0,
                rows: r,
                tau: 0.9,
                force_x: 2e-5,
            };
            y0 += r;
            p
        })
        .collect()
}

#[test]
fn any_depth_is_bit_identical() {
    let steps = 37;
    let reference = bits(&run_local(&decompose(10, 40, 1, 0.9, 2e-5), steps).unwrap());
    for (parts, depths) in [
        (4, vec![1, 1, 1]),
        (4, vec![2, 2, 2]),
        (4, vec![4, 4, 4]),
        (4, vec![8, 8, 8]),
        (4, vec![10, 10, 10]),
        (4, vec![1, 8, 2]),
        (4, vec![8, 1, 8]),
        (2, vec![7]),
        (8, vec![3; 7]),
    ] {
        let p = decompose(10, 40, parts, 0.9, 2e-5);
        let got = bits(&run_local_depth(&p, None, steps, &depths).unwrap());
        assert_eq!(got, reference, "parts {parts} depths {depths:?}");
    }
}

#[test]
fn uneven_strips_and_clamped_depth() {
    let p = uneven(9, &[3, 17, 5, 15]);
    let reference = bits(&run_local(&decompose(9, 40, 1, 0.9, 2e-5), 41).unwrap());
    let got = bits(&run_local_depth(&p, None, 41, &[3, 5, 5]).unwrap());
    assert_eq!(got, reference);
}

#[test]
fn restart_in_the_middle_of_a_block() {
    // Checkpoint at step 19 (mid-block for k = 8), re-split to uneven strips with other depths.
    let p4 = decompose(12, 42, 4, 0.9, 2e-5);
    let mid = run_local_depth(&p4, None, 19, &[8, 8, 8]).unwrap();
    let reference = bits(&run_local(&decompose(12, 42, 1, 0.9, 2e-5), 37).unwrap());
    let same = bits(&run_local_depth(&p4, Some(&mid), 18, &[8, 8, 8]).unwrap());
    assert_eq!(same, reference);
    let (np, ns) = opencompute_lbm::repartition(&p4, Some(&mid), 3).unwrap();
    let again = bits(&run_local_depth(&np, ns.as_deref(), 18, &[4, 6]).unwrap());
    assert_eq!(again, reference);
}

#[test]
fn missing_halo_is_reported() {
    let p = decompose(8, 16, 2, 0.8, 1e-5);
    let mut s = opencompute_lbm::Strip::new(p[0].clone(), None).unwrap();
    s.set_depth(1, 4).unwrap();
    assert!(s.step(None, None).is_err());
    assert!(s.set_depth(1, 99).is_err());
}
