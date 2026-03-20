// SPDX-License-Identifier: LGPL-3.0-or-later
//! Benchmark matrix for Rust `LatticeStatistics` execution policies.
//!
//! This example is the primary timing surface for comparing:
//!
//! - serial traversal/reduction
//! - overlapped I/O with serial compute (`Pipelined`)
//! - overlapped I/O plus threaded compute (`Parallel`)
//!
//! The default shape is the large steady-state case used elsewhere in the
//! performance work:
//!
//! ```sh
//! cargo run --example profile_lattice_statistics -p casacore-lattices --release
//! ```
//!
//! Useful environment variables:
//!
//! - `CASA_RS_LATTICE_STATS_PROFILE_SHAPE=1024,1024,256`
//! - `CASA_RS_LATTICE_STATS_PROFILE_REPEATS=5`
//! - `CASA_RS_LATTICE_STATS_PROFILE_MODE=matrix|array_basic|paged_basic|paged_order`

use std::time::Instant;

use casacore_lattices::{
    ArrayLattice, ExecutionPolicy, LatticeMut, LatticeStatistics, Statistic, TempLattice,
};
use ndarray::{ArrayD, IxDyn};

fn main() {
    let shape = parse_shape_env("CASA_RS_LATTICE_STATS_PROFILE_SHAPE", &[1024, 1024, 256]);
    let repeats = std::env::var("CASA_RS_LATTICE_STATS_PROFILE_REPEATS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(3);
    let mode = std::env::var("CASA_RS_LATTICE_STATS_PROFILE_MODE")
        .unwrap_or_else(|_| "matrix".to_string());
    let axes = vec![0, 1];
    let workers = std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1)
        .min(shape[2].max(1));

    println!("mode={mode} shape={shape:?} axes={axes:?} repeats={repeats} workers={workers}");

    let data = ArrayD::from_shape_fn(IxDyn(&shape), |idx| {
        let x = idx[0];
        let y = idx[1];
        let z = idx[2];
        (x + y * shape[0] + z * shape[0] * shape[1]) as f32
    });

    match mode.as_str() {
        "matrix" => run_matrix(data, &axes, repeats, workers),
        "array_basic" => {
            let serial = bench_array_basic(&data, &axes, ExecutionPolicy::Serial, repeats);
            let parallel = bench_array_basic(
                &data,
                &axes,
                ExecutionPolicy::Parallel {
                    workers: workers.max(1),
                    prefetch_depth: workers.max(1) * 2,
                },
                repeats,
            );
            print_result_block(
                "Array basic",
                &[("serial", serial), ("parallel", parallel)],
                Some("compute-only threaded speedup"),
            );
        }
        "paged_basic" => {
            let serial = bench_paged_basic(&data, &axes, ExecutionPolicy::Serial, repeats);
            let pipelined = bench_paged_basic(
                &data,
                &axes,
                ExecutionPolicy::Pipelined { prefetch_depth: 4 },
                repeats,
            );
            let parallel = bench_paged_basic(
                &data,
                &axes,
                ExecutionPolicy::Parallel {
                    workers: workers.max(1),
                    prefetch_depth: workers.max(1) * 2,
                },
                repeats,
            );
            print_result_block(
                "Paged basic",
                &[
                    ("serial", serial),
                    ("pipelined", pipelined),
                    ("parallel", parallel),
                ],
                Some("overlap-only versus overlap+threaded compute"),
            );
        }
        "paged_order" => {
            let serial = bench_paged_order(&data, &axes, ExecutionPolicy::Serial, repeats);
            let pipelined = bench_paged_order(
                &data,
                &axes,
                ExecutionPolicy::Pipelined { prefetch_depth: 4 },
                repeats,
            );
            let parallel = bench_paged_order(
                &data,
                &axes,
                ExecutionPolicy::Parallel {
                    workers: workers.max(1),
                    prefetch_depth: workers.max(1) * 2,
                },
                repeats,
            );
            print_result_block(
                "Paged order",
                &[
                    ("serial", serial),
                    ("pipelined", pipelined),
                    ("parallel", parallel),
                ],
                Some("same I/O pattern with heavier bucket/quantile work"),
            );
        }
        other => panic!(
            "unknown CASA_RS_LATTICE_STATS_PROFILE_MODE={other}; expected one of \
             matrix,array_basic,paged_basic,paged_order"
        ),
    }
}

fn run_matrix(data: ArrayD<f32>, axes: &[usize], repeats: usize, workers: usize) {
    let parallel_policy = ExecutionPolicy::Parallel {
        workers: workers.max(1),
        prefetch_depth: workers.max(1) * 2,
    };
    let paged_basic_serial = bench_paged_basic(&data, axes, ExecutionPolicy::Serial, repeats);
    let paged_basic_pipelined = bench_paged_basic(
        &data,
        axes,
        ExecutionPolicy::Pipelined { prefetch_depth: 4 },
        repeats,
    );
    let paged_basic_parallel = bench_paged_basic(&data, axes, parallel_policy, repeats);
    print_result_block(
        "Paged basic",
        &[
            ("serial", paged_basic_serial),
            ("pipelined", paged_basic_pipelined),
            ("parallel", paged_basic_parallel),
        ],
        Some("overlap-only versus overlap+threaded compute"),
    );

    let paged_order_serial = bench_paged_order(&data, axes, ExecutionPolicy::Serial, repeats);
    let paged_order_pipelined = bench_paged_order(
        &data,
        axes,
        ExecutionPolicy::Pipelined { prefetch_depth: 4 },
        repeats,
    );
    let paged_order_parallel = bench_paged_order(&data, axes, parallel_policy, repeats);
    print_result_block(
        "Paged order",
        &[
            ("serial", paged_order_serial),
            ("pipelined", paged_order_pipelined),
            ("parallel", paged_order_parallel),
        ],
        Some("same I/O pattern with heavier bucket/quantile work"),
    );
}

fn bench_array_basic(
    data: &ArrayD<f32>,
    axes: &[usize],
    policy: ExecutionPolicy,
    repeats: usize,
) -> f64 {
    benchmark_ms(repeats, || {
        let lattice = ArrayLattice::new(data.clone());
        let mut stats = LatticeStatistics::new(&lattice);
        stats.set_axes(axes.to_vec());
        stats.set_execution_policy(policy);
        pause_before_timed_phase();
        let start = Instant::now();
        let _npts = stats.get_statistic(Statistic::Npts).unwrap();
        let _mean = stats.get_statistic(Statistic::Mean).unwrap();
        let _sigma = stats.get_statistic(Statistic::Sigma).unwrap();
        start.elapsed().as_secs_f64() * 1000.0
    })
}

fn bench_paged_basic(
    data: &ArrayD<f32>,
    axes: &[usize],
    policy: ExecutionPolicy,
    repeats: usize,
) -> f64 {
    benchmark_ms(repeats, || {
        let mut lattice = make_paged_lattice(data);
        lattice.temp_close().unwrap();
        let mut stats = LatticeStatistics::new(&lattice);
        stats.set_axes(axes.to_vec());
        stats.set_execution_policy(policy);
        pause_before_timed_phase();
        let start = Instant::now();
        let _npts = stats.get_statistic(Statistic::Npts).unwrap();
        let _mean = stats.get_statistic(Statistic::Mean).unwrap();
        let _sigma = stats.get_statistic(Statistic::Sigma).unwrap();
        start.elapsed().as_secs_f64() * 1000.0
    })
}

fn bench_paged_order(
    data: &ArrayD<f32>,
    axes: &[usize],
    policy: ExecutionPolicy,
    repeats: usize,
) -> f64 {
    benchmark_ms(repeats, || {
        let mut lattice = make_paged_lattice(data);
        lattice.temp_close().unwrap();
        let mut stats = LatticeStatistics::new(&lattice);
        stats.set_axes(axes.to_vec());
        stats.set_execution_policy(policy);
        pause_before_timed_phase();
        let start = Instant::now();
        let _median = stats.get_statistic(Statistic::Median).unwrap();
        let _q1 = stats.get_statistic(Statistic::Q1).unwrap();
        let _q3 = stats.get_statistic(Statistic::Q3).unwrap();
        start.elapsed().as_secs_f64() * 1000.0
    })
}

fn make_paged_lattice(data: &ArrayD<f32>) -> TempLattice<f32> {
    let shape = data.shape().to_vec();
    let mut lattice = TempLattice::<f32>::new(shape, Some(1)).unwrap();
    lattice.put_slice(data, &vec![0; data.ndim()]).unwrap();
    lattice
}

fn benchmark_ms(repeats: usize, mut run_once: impl FnMut() -> f64) -> f64 {
    let mut times = Vec::with_capacity(repeats.max(1));
    for _ in 0..repeats.max(1) {
        times.push(run_once());
    }
    times.sort_by(f64::total_cmp);
    times[times.len() / 2]
}

fn print_result_block(title: &str, rows: &[(&str, f64)], note: Option<&str>) {
    println!("\n{title}:");
    if let Some(note) = note {
        println!("  note: {note}");
    }
    let baseline = rows.first().map(|(_, ms)| *ms).unwrap_or(1.0).max(0.001);
    for (label, ms) in rows {
        println!("  {label:10} {ms:10.2} ms   ratio {:6.2}x", ms / baseline);
    }
}

fn parse_shape_env(var: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(var)
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| part.trim().parse::<usize>().ok())
                .collect::<Vec<_>>()
        })
        .filter(|shape| shape.len() == default.len())
        .unwrap_or_else(|| default.to_vec())
}

fn pause_before_timed_phase() {
    if let Ok(path) = std::env::var("CASA_RS_LATTICE_STATS_PROFILE_WAIT_FILE") {
        let wait_path = std::path::PathBuf::from(path);
        println!("waiting for {:?}", wait_path);
        while !wait_path.exists() {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }
    let pause_secs = std::env::var("CASA_RS_LATTICE_STATS_PROFILE_PAUSE_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    if pause_secs > 0 {
        println!("pausing {pause_secs}s before timed phase");
        std::thread::sleep(std::time::Duration::from_secs(pause_secs));
    }
}
