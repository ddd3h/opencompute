//! Integration tests for `opencompute_lbm::repartition`.

use opencompute_lbm::{LbmParams, Strip, decompose, repartition, run_local, velocity_profile};

const NX: usize = 8;
const NY: usize = 24;
const TAU: f64 = 0.8;
const FORCE: f64 = 1e-5;

/// Continue a decomposed run from its snapshots for `steps` further steps.
fn advance(params: &[LbmParams], snapshots: &[Vec<u8>], steps: u64) -> Vec<Vec<u8>> {
    let mut strips: Vec<Strip> = params
        .iter()
        .zip(snapshots)
        .map(|(p, s)| Strip::new(p.clone(), Some(s)).expect("restore strip"))
        .collect();
    for _ in 0..steps {
        let boundaries: Vec<_> = strips.iter().map(Strip::boundary).collect();
        for (i, strip) in strips.iter_mut().enumerate() {
            let lo = i.checked_sub(1).map(|j| boundaries[j].1.as_slice());
            let hi = boundaries.get(i + 1).map(|b| b.0.as_slice());
            strip.step(lo, hi).expect("step");
        }
    }
    strips.iter().map(Strip::snapshot).collect()
}

#[test]
fn repartition_preserves_velocity_profile_exactly() {
    for (old_parts, new_parts) in [(1, 7), (7, 3), (3, 24)] {
        let old_params = decompose(NX, NY, old_parts, TAU, FORCE);
        let old_snapshots = run_local(&old_params, 60).expect("old decomposition runs");
        let before = velocity_profile(&old_params, &old_snapshots).expect("old profile");

        let (new_params, new_snapshots) =
            repartition(&old_params, Some(&old_snapshots), new_parts).expect("repartition");
        assert_eq!(
            new_params,
            decompose(NX, NY, new_parts, TAU, FORCE),
            "{old_parts} -> {new_parts}: parameters"
        );

        let new_snapshots = new_snapshots.expect("snapshots were supplied");
        assert_eq!(new_snapshots.len(), new_parts);
        assert_eq!(new_params.iter().map(|p| p.rows).sum::<usize>(), NY);
        let after = velocity_profile(&new_params, &new_snapshots).expect("new profile");
        assert_eq!(
            after, before,
            "{old_parts} -> {new_parts}: profile must be unchanged"
        );
    }
}

#[test]
fn continuation_after_repartition_matches_unrepartitioned_run() {
    const BEFORE: u64 = 120;
    const AFTER: u64 = 80;
    const TOTAL: u64 = BEFORE + AFTER;

    for (old_parts, new_parts) in [(1, 7), (7, 3), (3, 24)] {
        let old_params = decompose(NX, NY, old_parts, TAU, FORCE);
        let old_snapshots = run_local(&old_params, BEFORE).expect("first 120 steps");

        let (new_params, new_snapshots) =
            repartition(&old_params, Some(&old_snapshots), new_parts).expect("repartition");
        let continued = advance(
            &new_params,
            new_snapshots.as_ref().expect("snapshots"),
            AFTER,
        );

        let reference_snapshots = run_local(&old_params, TOTAL).expect("200 reference steps");
        let reference =
            velocity_profile(&old_params, &reference_snapshots).expect("reference profile");
        let got = velocity_profile(&new_params, &continued).expect("continued profile");
        assert_eq!(
            got, reference,
            "{old_parts} -> {new_parts}: 80 steps after repartition must match 200 steps \
             on the original decomposition bit for bit"
        );
    }
}

#[test]
fn more_parts_than_rows_clamps_to_ny() {
    let params = decompose(NX, NY, 2, TAU, FORCE);
    let snapshots = run_local(&params, 20).expect("run");
    let before = velocity_profile(&params, &snapshots).expect("profile");

    let (new_params, new_snapshots) =
        repartition(&params, Some(&snapshots), NY * 4).expect("clamped repartition");
    assert_eq!(new_params.len(), NY, "parts are clamped to ny");
    assert_eq!(new_params, decompose(NX, NY, NY, TAU, FORCE));
    assert!(new_params.iter().all(|p| p.rows == 1));
    for (i, p) in new_params.iter().enumerate() {
        assert_eq!(p.y0, i, "strip {i} starts at row {i}");
        assert_eq!((p.nx, p.ny_global), (NX, NY));
        assert_eq!((p.tau, p.force_x), (TAU, FORCE));
    }

    let new_snapshots = new_snapshots.expect("snapshots were supplied");
    assert_eq!(new_snapshots.len(), NY);
    let after = velocity_profile(&new_params, &new_snapshots).expect("profile");
    assert_eq!(after, before);
}

#[test]
fn no_snapshots_returns_none_with_decomposed_params() {
    let params = decompose(NX, NY, 3, TAU, FORCE);
    let (new_params, new_snapshots) =
        repartition(&params, None, 7).expect("repartition without snapshots");
    assert_eq!(new_params, decompose(NX, NY, 7, TAU, FORCE));
    assert!(new_snapshots.is_none());
}

#[test]
fn rejects_mismatched_snapshot_count_and_corrupt_bytes() {
    let params = decompose(NX, NY, 3, TAU, FORCE);
    let snapshots = run_local(&params, 10).expect("run");

    assert!(
        repartition(&params, Some(&snapshots[..2]), 7).is_err(),
        "fewer snapshots than parts must be rejected"
    );

    let mut too_many = snapshots.clone();
    too_many.push(snapshots[0].clone());
    assert!(
        repartition(&params, Some(&too_many), 7).is_err(),
        "more snapshots than parts must be rejected"
    );

    let mut garbage = snapshots.clone();
    garbage[1] = vec![0xde, 0xad, 0xbe, 0xef];
    assert!(
        repartition(&params, Some(&garbage), 7).is_err(),
        "undecodable snapshot bytes must be rejected"
    );

    let mut truncated = snapshots.clone();
    let half = truncated[2].len() / 2;
    truncated[2].truncate(half);
    assert!(
        repartition(&params, Some(&truncated), 7).is_err(),
        "truncated snapshot bytes must be rejected"
    );
}

#[test]
fn chained_repartitions_are_lossless() {
    const SEGMENT: u64 = 50;

    let mut params = decompose(NX, NY, 2, TAU, FORCE);
    let mut snapshots = run_local(&params, SEGMENT).expect("2 parts, first segment");
    let mut total = SEGMENT;

    for new_parts in [5, 3, 1] {
        let (next_params, next_snapshots) =
            repartition(&params, Some(&snapshots), new_parts).expect("repartition");
        let next_snapshots = next_snapshots.expect("snapshots were supplied");
        snapshots = advance(&next_params, &next_snapshots, SEGMENT);
        params = next_params;
        total += SEGMENT;
    }

    // The chain ends with one strip, so its snapshot can be compared byte for byte with a
    // single-strip run of the same number of steps.
    assert_eq!(params.len(), 1);
    assert_eq!(params, decompose(NX, NY, 1, TAU, FORCE));
    let reference = run_local(&decompose(NX, NY, 1, TAU, FORCE), total).expect("reference");
    assert_eq!(
        snapshots, reference,
        "2 -> 5 -> 3 -> 1 repartitions must keep the global state lossless"
    );

    let one = decompose(NX, NY, 1, TAU, FORCE);
    assert_eq!(
        velocity_profile(&params, &snapshots).expect("profile"),
        velocity_profile(&one, &reference).expect("reference profile")
    );
}
