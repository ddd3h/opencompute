//! All-pairs N-body simulation using WASM tasks (or native tasks with `NBODY_NATIVE=1`).
//!
//! ```text
//! (cd examples/wasm-modules && cargo build --release --target wasm32-unknown-unknown)
//! cargo run --release -p opencompute-compute-api --example nbody_batch -- [N STEPS BLOCKS]
//! ```
//! Each step uploads all positions once as a shared blob, so every worker fetches them once, and
//! sends one small task per block of particles, which returns that block's accelerations. Leapfrog
//! (kick-drift-kick) integration runs here. The result is compared bit for bit with the same
//! integration done in this process, and the wall times are printed.
use opencompute::{Cluster, TaskKind};
use serde::Serialize;
use std::time::Instant;

struct Timing<'a>(&'a std::cell::RefCell<Vec<f64>>, Instant);
impl Drop for Timing<'_> {
    fn drop(&mut self) {
        self.0
            .borrow_mut()
            .push(self.1.elapsed().as_secs_f64() * 1e3);
    }
}
fn scopeguard(v: &std::cell::RefCell<Vec<f64>>, t: Instant) -> Timing<'_> {
    Timing(v, t)
}

#[path = "../../../examples/wasm-modules/nbody/src/force.rs"]
mod force;

#[derive(Serialize)]
struct Range {
    start: u32,
    end: u32,
    eps2: f64,
}

/// Deterministic initial condition: uniform cube, small random velocities.
fn initial(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut s = 0x2545_f491_4f6c_dd1du64;
    let mut rnd = move || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        (s >> 11) as f64 / (1u64 << 53) as f64 * 2.0 - 1.0
    };
    let pos = (0..3 * n).map(|_| rnd()).collect();
    let vel = (0..3 * n).map(|_| 0.1 * rnd()).collect();
    (pos, vel)
}

fn energy(pos: &[f64], vel: &[f64], eps2: f64) -> f64 {
    let n = pos.len() / 3;
    let m = 1.0 / n as f64;
    let kin: f64 = vel.iter().map(|v| 0.5 * m * v * v).sum();
    let mut pot = 0.0;
    for i in 0..n {
        for j in i + 1..n {
            let d: f64 = (0..3)
                .map(|k| (pos[3 * i + k] - pos[3 * j + k]).powi(2))
                .sum();
            pot -= m * m / (d + eps2).sqrt();
        }
    }
    kin + pot
}

fn kick(vel: &mut [f64], acc: &[f64], dt: f64) {
    for (v, a) in vel.iter_mut().zip(acc) {
        *v += 0.5 * dt * a;
    }
}

fn drift(pos: &mut [f64], vel: &[f64], dt: f64) {
    for (x, v) in pos.iter_mut().zip(vel) {
        *x += dt * v;
    }
}

#[tokio::main]
async fn main() -> Result<(), opencompute::Error> {
    let arg = |i: usize, d: usize| {
        std::env::args()
            .nth(i)
            .and_then(|s| s.parse().ok())
            .unwrap_or(d)
    };
    let (n, steps, blocks) = (arg(1, 4096), arg(2, 10), arg(3, 16));
    let (eps2, dt) = (0.05f64 * 0.05, 0.01f64);
    let cluster = Cluster::connect_default().await?;
    let kind = if std::env::var("NBODY_NATIVE").as_deref() == Ok("1") {
        TaskKind::Builtin("nbody_force".into())
    } else {
        TaskKind::Wasm(
            cluster
                .load_module(
                    "examples/wasm-modules/target/wasm32-unknown-unknown/release/nbody.wasm",
                )
                .await?
                .hash,
        )
    };
    let ranges: Vec<(usize, usize)> = (0..blocks)
        .map(|b| (b * n / blocks, (b + 1) * n / blocks))
        .filter(|(a, b)| b > a)
        .collect();

    let (mut pos, mut vel) = initial(n);
    let e0 = energy(&pos, &vel, eps2);
    let t0 = Instant::now();
    let mut acc = vec![0.0; 3 * n];
    let times = std::cell::RefCell::new(Vec::<f64>::new());
    let force_eval = async |pos: &[f64], acc: &mut Vec<f64>| -> Result<(), opencompute::Error> {
        let t = Instant::now();
        let _g = scopeguard(&times, t);
        let blob: Vec<u8> = pos.iter().flat_map(|x| x.to_le_bytes()).collect();
        let inputs: Vec<Range> = ranges
            .iter()
            .map(|&(a, b)| Range {
                start: a as u32,
                end: b as u32,
                eps2,
            })
            .collect();
        let parts: Vec<Vec<f64>> = cluster.map_shared_with(kind.clone(), blob, inputs).await?;
        acc.clear();
        acc.extend(parts.into_iter().flatten());
        Ok(())
    };
    force_eval(&pos, &mut acc).await?;
    for _ in 0..steps {
        kick(&mut vel, &acc, dt);
        drift(&mut pos, &vel, dt);
        force_eval(&pos, &mut acc).await?;
        kick(&mut vel, &acc, dt);
    }
    let dist = t0.elapsed();
    {
        let mut v = times.borrow().clone();
        let all = v
            .iter()
            .map(|x| format!("{x:.0}"))
            .collect::<Vec<_>>()
            .join(" ");
        v.sort_by(f64::total_cmp);
        println!("per-step ms: {all}  (median {:.0})", v[v.len() / 2]);
    }
    let e1 = energy(&pos, &vel, eps2);

    // Same integration in this process, one block.
    let t1 = Instant::now();
    let (mut rp, mut rv) = initial(n);
    let mut ra = force::accelerations(&rp, 0, n, eps2);
    for _ in 0..steps {
        kick(&mut rv, &ra, dt);
        drift(&mut rp, &rv, dt);
        ra = force::accelerations(&rp, 0, n, eps2);
        kick(&mut rv, &ra, dt);
    }
    let local = t1.elapsed();

    let pairs = (n as f64) * (n as f64) * (steps + 1) as f64;
    println!("N={n} steps={steps} blocks={}", ranges.len());
    println!(
        "cluster: {dist:.2?} ({:.1} ms/step, {:.2} Ginteractions/s)",
        dist.as_secs_f64() * 1e3 / steps as f64,
        pairs / dist.as_secs_f64() / 1e9
    );
    println!(
        "local  : {local:.2?} (1 thread, native; {:.2} Ginteractions/s)",
        pairs / local.as_secs_f64() / 1e9
    );
    println!(
        "speed-up vs local: {:.2}x",
        local.as_secs_f64() / dist.as_secs_f64()
    );
    println!("energy drift: {:.3e}", (e1 - e0) / e0.abs());
    let same = pos.iter().zip(&rp).all(|(a, b)| a.to_bits() == b.to_bits());
    println!("positions bit-identical to local run: {same}");
    if !same {
        std::process::exit(1);
    }
    Ok(())
}
