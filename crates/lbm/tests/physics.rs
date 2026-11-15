//! Integration tests for the conservation and reproducibility properties of the D2Q9 kernel.

use opencompute_lbm::{Strip, decompose, run_local, velocity_profile};

/// Sum of every population of every node of every strip.
fn total_mass(snapshots: &[Vec<u8>]) -> f64 {
    let mut total = 0.0;
    for snap in snapshots {
        let f = opencompute_lbm::codec::decode(snap).expect("decode snapshot");
        assert_eq!(f.len(), 9, "snapshot must hold 9 populations");
        for pop in &f {
            total += pop.iter().sum::<f64>();
        }
    }
    total
}

#[test]
fn mass_conserved_with_force_over_500_steps() {
    let (nx, ny, parts, tau, force) = (10, 24, 3, 0.8, 1e-5);
    let ps = decompose(nx, ny, parts, tau, force);
    let start = run_local(&ps, 0).expect("initial state");
    let end = run_local(&ps, 500).expect("500 steps");

    let m0 = total_mass(&start);
    let m1 = total_mass(&end);
    let expected = (nx * ny) as f64;
    assert!(
        (m0 - expected).abs() < 1e-12,
        "initial mass {m0} should be {expected}"
    );
    let rel = ((m1 - m0) / m0).abs();
    assert!(
        rel < 1e-9,
        "mass drifted from {m0} to {m1} (relative {rel:e})"
    );
}

#[test]
fn decomposition_is_bit_identical_for_1_2_3_5_8_parts() {
    let (nx, ny, tau, force) = (10, 24, 0.9, 2e-5);
    let reference = {
        let ps = decompose(nx, ny, 1, tau, force);
        let snaps = run_local(&ps, 200).expect("single-strip run");
        velocity_profile(&ps, &snaps).expect("profile")
    };
    assert_eq!(reference.len(), ny);

    for parts in [2, 3, 5, 8] {
        let ps = decompose(nx, ny, parts, tau, force);
        let snaps = run_local(&ps, 200).expect("decomposed run");
        let profile = velocity_profile(&ps, &snaps).expect("profile");
        assert_eq!(profile.len(), ny, "{parts} parts: profile height");
        assert_eq!(
            profile, reference,
            "{parts} parts must reproduce the single-strip profile bit for bit"
        );
    }
}

#[test]
fn zero_force_keeps_fluid_at_rest() {
    let ps = decompose(10, 24, 4, 0.8, 0.0);
    let snaps = run_local(&ps, 200).expect("run");
    let profile = velocity_profile(&ps, &snaps).expect("profile");
    assert_eq!(profile.len(), 24);
    for (y, u) in profile.iter().enumerate() {
        assert!(
            u.abs() < 1e-12,
            "row {y} is not at rest: u = {u:e} \
             (the forcing term must not inject momentum when force_x = 0)"
        );
    }
}

#[test]
fn strip_new_rejects_mismatched_snapshot_sizes() {
    let wide = &decompose(8, 8, 1, 0.7, 0.0)[0];
    let snapshot = Strip::new(wide.clone(), None)
        .expect("build strip")
        .snapshot();

    // Same width, different number of rows.
    let shorter = &decompose(8, 4, 1, 0.7, 0.0)[0];
    assert!(
        Strip::new(shorter.clone(), Some(&snapshot)).is_err(),
        "snapshot of an 8x8 strip must be rejected for an 8x4 strip"
    );

    // Same height, different width.
    let narrower = &decompose(6, 8, 1, 0.7, 0.0)[0];
    assert!(
        Strip::new(narrower.clone(), Some(&snapshot)).is_err(),
        "snapshot of an 8x8 strip must be rejected for a 6x8 strip"
    );

    // Not a valid snapshot at all.
    assert!(
        Strip::new(wide.clone(), Some(&[0xde, 0xad, 0xbe, 0xef])).is_err(),
        "garbage bytes must be rejected"
    );
}

#[test]
fn velocity_profile_rejects_wrong_geometry() {
    let source = decompose(8, 8, 2, 0.7, 1e-5);
    let snaps = run_local(&source, 3).expect("run");

    // Wider strips.
    let wider = decompose(10, 8, 2, 0.7, 1e-5);
    assert!(
        velocity_profile(&wider, &snaps).is_err(),
        "10-wide params must reject snapshots of 8-wide strips"
    );

    // Different row count.
    let taller = decompose(8, 12, 2, 0.7, 1e-5);
    assert!(
        velocity_profile(&taller, &snaps).is_err(),
        "12-row params must reject snapshots of 8-row strips"
    );

    // Corrupt payload.
    let bad = vec![vec![1u8, 2, 3]];
    assert!(
        velocity_profile(&source[..1], &bad).is_err(),
        "undecodable snapshot must be rejected"
    );
}

#[test]
fn decompose_clamps_parts_above_ny() {
    let (nx, ny) = (4usize, 8usize);
    let d = decompose(nx, ny, 100, 0.7, 0.0);
    assert_eq!(d.len(), ny, "parts must be clamped to ny");
    assert!(
        d.iter()
            .all(|p| p.rows == 1 && p.nx == nx && p.ny_global == ny)
    );
    assert_eq!(d.iter().map(|p| p.rows).sum::<usize>(), ny);
    for (i, p) in d.iter().enumerate() {
        assert_eq!(p.y0, i, "strip {i} must start at global row {i}");
    }

    // Zero parts is clamped up to one strip covering the whole channel.
    let one = decompose(nx, ny, 0, 0.7, 0.0);
    assert_eq!(one.len(), 1);
    assert_eq!((one[0].y0, one[0].rows), (0, ny));

    // A non-divisor split still covers the domain exactly.
    let d3 = decompose(nx, ny, 3, 0.7, 0.0);
    assert_eq!(d3.iter().map(|p| p.rows).collect::<Vec<_>>(), vec![3, 3, 2]);
    assert_eq!(d3.iter().map(|p| p.rows).sum::<usize>(), ny);
}
