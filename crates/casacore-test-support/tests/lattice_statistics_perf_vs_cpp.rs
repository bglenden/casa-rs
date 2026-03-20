// SPDX-License-Identifier: LGPL-3.0-or-later
//! Rust-vs-C++ forced-I/O lattice statistics benchmark and correctness check.
//!
//! The Rust and C++ sides both run the same workload:
//! - 3-D paged lattice with explicit `16×16×16` tiles
//! - deterministic ramp values `x + y*nx + z*(nx*ny)`
//! - tile cache limited to one tile
//! - axes collapsed over `[0, 1]` so the output is one value per plane
//!
//! Use `cargo test --release -p casacore-test-support --test lattice_statistics_perf_vs_cpp -- --nocapture`
//! for meaningful performance ratios.

use std::time::Instant;

use casacore_lattices::{
    ExecutionPolicy, LatticeMut, LatticeStatistics, PagedArray, Statistic, TiledShape,
};
use casacore_test_support::{cpp_backend_available, cpp_lattice_statistics_forced_io_bench};
use ndarray::{ArrayD, IxDyn};

const MEDIUM_SHAPE: [usize; 3] = [512, 512, 64];
const LARGE_SHAPE: [usize; 3] = [1024, 1024, 256];
const TILE_SHAPE: [usize; 3] = [16, 16, 16];
const CACHE_TILES: u64 = 1;

struct RustBenchResult {
    basic_ns: u64,
    order_ns: u64,
    mean: Vec<f64>,
    sigma: Vec<f64>,
    median: Vec<f64>,
    q1: Vec<f64>,
    q3: Vec<f64>,
}

fn ramp_array(shape: &[usize]) -> ArrayD<f32> {
    let nx = shape[0];
    let ny = shape[1];
    let plane = nx * ny;
    ArrayD::from_shape_fn(IxDyn(shape), |idx| {
        (idx[0] + idx[1] * nx + idx[2] * plane) as f32
    })
}

fn collect_stat(array: &ArrayD<f64>) -> Vec<f64> {
    array.iter().copied().collect()
}

fn assert_close(label: &str, actual: &[f64], expected: &[f64], tol: f64) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "{label}: output length mismatch"
    );
    for (i, (&got, &want)) in actual.iter().zip(expected.iter()).enumerate() {
        assert!(
            (got - want).abs() <= tol,
            "{label}: plane {i}: got={got}, expected={want}, diff={}",
            (got - want).abs()
        );
    }
}

fn assert_close_with_relative(
    label: &str,
    actual: &[f64],
    expected: &[f64],
    abs_tol: f64,
    rel_tol: f64,
) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "{label}: output length mismatch"
    );
    for (i, (&got, &want)) in actual.iter().zip(expected.iter()).enumerate() {
        let diff = (got - want).abs();
        let scale = want.abs().max(1.0);
        assert!(
            diff <= abs_tol.max(rel_tol * scale),
            "{label}: plane {i}: got={got}, expected={want}, diff={diff}"
        );
    }
}

fn rust_lattice_statistics_forced_io_bench(shape: &[usize; 3]) -> RustBenchResult {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rust_lattice_stats.table");
    let tiled_shape = TiledShape::with_tile_shape(shape.to_vec(), TILE_SHAPE.to_vec()).unwrap();
    let mut lattice = PagedArray::<f32>::create(tiled_shape, &path).unwrap();
    lattice.put_slice(&ramp_array(shape), &[0, 0, 0]).unwrap();
    lattice
        .set_cache_size_in_tiles(CACHE_TILES as usize)
        .unwrap();

    let (basic_ns, mean, sigma) = {
        lattice.temp_close().unwrap();
        let mut stats = LatticeStatistics::new(&lattice);
        stats.set_axes(vec![0, 1]);
        stats.set_execution_policy(best_rust_policy(shape));

        let t0 = Instant::now();
        let npts = stats.get_statistic(Statistic::Npts).unwrap();
        let mean = stats.get_statistic(Statistic::Mean).unwrap();
        let sigma = stats.get_statistic(Statistic::Sigma).unwrap();
        let elapsed = t0.elapsed().as_nanos() as u64;

        let expected_npts = (shape[0] * shape[1]) as f64;
        assert!(
            npts.iter().all(|&value| value == expected_npts),
            "Rust NPTS output mismatch"
        );

        (elapsed, collect_stat(&mean), collect_stat(&sigma))
    };

    let (order_ns, median, q1, q3) = {
        lattice.temp_close().unwrap();
        let mut stats = LatticeStatistics::new(&lattice);
        stats.set_axes(vec![0, 1]);
        stats.set_execution_policy(best_rust_policy(shape));

        let t0 = Instant::now();
        let median = stats.get_statistic(Statistic::Median).unwrap();
        let q1 = stats.get_statistic(Statistic::Q1).unwrap();
        let q3 = stats.get_statistic(Statistic::Q3).unwrap();
        let elapsed = t0.elapsed().as_nanos() as u64;

        (
            elapsed,
            collect_stat(&median),
            collect_stat(&q1),
            collect_stat(&q3),
        )
    };

    RustBenchResult {
        basic_ns,
        order_ns,
        mean,
        sigma,
        median,
        q1,
        q3,
    }
}

fn best_rust_policy(shape: &[usize; 3]) -> ExecutionPolicy {
    let workers = std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1)
        .min(shape[2].max(1));
    if workers < 2 {
        ExecutionPolicy::Serial
    } else {
        ExecutionPolicy::Parallel {
            workers,
            prefetch_depth: workers * 2,
        }
    }
}

fn run_lattice_statistics_forced_io_case(case_name: &str, shape: &[usize; 3]) {
    if !cpp_backend_available() {
        eprintln!("skipping {case_name}: C++ casacore not available");
        return;
    }

    let shape_i32: Vec<i32> = shape.iter().map(|&dim| dim as i32).collect();
    let tile_i32: Vec<i32> = TILE_SHAPE.iter().map(|&dim| dim as i32).collect();

    let cpp_dir = tempfile::tempdir().unwrap();
    let cpp_path = cpp_dir.path().join("cpp_lattice_stats.table");
    let cpp = cpp_lattice_statistics_forced_io_bench(&cpp_path, &shape_i32, &tile_i32, CACHE_TILES)
        .expect("C++ lattice statistics benchmark should succeed");

    let rust = rust_lattice_statistics_forced_io_bench(shape);

    assert_close("mean", &rust.mean, &cpp.mean, 2e-5);
    assert_close_with_relative("sigma", &rust.sigma, &cpp.sigma, 2e-4, 1e-7);
    assert_close("median", &rust.median, &cpp.median, 2e-5);
    assert_close("q1", &rust.q1, &cpp.q1, 2e-5);
    assert_close("q3", &rust.q3, &cpp.q3, 2e-5);

    let basic_ratio = rust.basic_ns as f64 / cpp.basic_ns.max(1) as f64;
    let order_ratio = rust.order_ns as f64 / cpp.order_ns.max(1) as f64;

    eprintln!(
        "LatticeStatistics forced-I/O perf ({:?}, tile {:?}, cache_tiles={}):\n  \
         basic (NPTS/MEAN/SIGMA): C++ {:.1} ms, Rust {:.1} ms, ratio {basic_ratio:.2}×\n  \
         order (MEDIAN/Q1/Q3):    C++ {:.1} ms, Rust {:.1} ms, ratio {order_ratio:.2}×",
        shape,
        TILE_SHAPE,
        CACHE_TILES,
        cpp.basic_ns as f64 / 1e6,
        rust.basic_ns as f64 / 1e6,
        cpp.order_ns as f64 / 1e6,
        rust.order_ns as f64 / 1e6,
    );

    let max_ratio = basic_ratio.max(order_ratio);
    if max_ratio > 2.0 {
        eprintln!(
            "  WARNING: Rust lattice statistics forced-I/O path is {max_ratio:.1}× slower than C++ (threshold: 2.0×)"
        );
    }
}

#[test]
fn lattice_statistics_forced_io_medium_vs_cpp() {
    run_lattice_statistics_forced_io_case(
        "lattice_statistics_forced_io_medium_vs_cpp",
        &MEDIUM_SHAPE,
    );
}

#[test]
#[ignore = "large perf comparison; run explicitly when evaluating steady-state performance"]
fn lattice_statistics_forced_io_large_vs_cpp() {
    run_lattice_statistics_forced_io_case(
        "lattice_statistics_forced_io_large_vs_cpp",
        &LARGE_SHAPE,
    );
}
