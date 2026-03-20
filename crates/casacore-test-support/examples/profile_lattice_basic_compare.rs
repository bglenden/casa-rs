// SPDX-License-Identifier: LGPL-3.0-or-later
//! Repeated basic-family lattice-statistics runner for profiling Rust vs C++.
//!
//! Usage:
//! `cargo run --release -p casacore-test-support --example profile_lattice_basic_compare -- rust`
//! `cargo run --release -p casacore-test-support --example profile_lattice_basic_compare -- cpp`
//!
//! Environment:
//! - `CASA_RS_PROFILE_ITERATIONS` default `50`
//! - `CASA_RS_PROFILE_SLEEP_SECS` default `5`

use std::env;
use std::process;
use std::thread;
use std::time::{Duration, Instant};

use casacore_lattices::{LatticeMut, LatticeStatistics, PagedArray, Statistic, TiledShape};
use casacore_test_support::{
    cpp_backend_available, cpp_lattice_statistics_forced_io_repeated_basic,
};
use ndarray::{ArrayD, IxDyn};

const SHAPE: [usize; 3] = [512, 512, 64];
const TILE_SHAPE: [usize; 3] = [16, 16, 16];
const CACHE_TILES: usize = 1;

fn ramp_array(shape: &[usize]) -> ArrayD<f32> {
    let nx = shape[0];
    let ny = shape[1];
    let plane = nx * ny;
    ArrayD::from_shape_fn(IxDyn(shape), |idx| {
        (idx[0] + idx[1] * nx + idx[2] * plane) as f32
    })
}

fn rust_repeated_basic(iterations: u32) -> (u64, f64) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rust_profile_lattice_stats.table");
    let tiled_shape = TiledShape::with_tile_shape(SHAPE.to_vec(), TILE_SHAPE.to_vec()).unwrap();
    let mut lattice = PagedArray::<f32>::create(tiled_shape, &path).unwrap();
    lattice.put_slice(&ramp_array(&SHAPE), &[0, 0, 0]).unwrap();
    lattice.set_cache_size_in_tiles(CACHE_TILES).unwrap();

    let mut checksum = 0.0f64;
    let t0 = Instant::now();
    for _ in 0..iterations {
        lattice.temp_close().unwrap();
        let mut stats = LatticeStatistics::new(&lattice);
        stats.set_axes(vec![0, 1]);
        let npts = stats.get_statistic(Statistic::Npts).unwrap();
        let mean = stats.get_statistic(Statistic::Mean).unwrap();
        let sigma = stats.get_statistic(Statistic::Sigma).unwrap();
        checksum += npts[[0]] + mean[[0]] + sigma[[0]];
    }
    (t0.elapsed().as_nanos() as u64, checksum)
}

fn cpp_repeated_basic(iterations: u32) -> (u64, f64) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cpp_profile_lattice_stats.table");
    let shape_i32: Vec<i32> = SHAPE.iter().map(|&dim| dim as i32).collect();
    let tile_i32: Vec<i32> = TILE_SHAPE.iter().map(|&dim| dim as i32).collect();
    cpp_lattice_statistics_forced_io_repeated_basic(
        &path,
        &shape_i32,
        &tile_i32,
        CACHE_TILES as u64,
        iterations,
    )
    .expect("C++ repeated basic profile run should succeed")
}

fn main() {
    let mode = env::args().nth(1).unwrap_or_else(|| "rust".to_string());
    let iterations = env::var("CASA_RS_PROFILE_ITERATIONS")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(50);
    let sleep_secs = env::var("CASA_RS_PROFILE_SLEEP_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(5);

    if mode == "cpp" && !cpp_backend_available() {
        eprintln!("C++ casacore backend unavailable");
        process::exit(2);
    }

    println!(
        "mode={mode} pid={} shape={:?} tile={:?} cache_tiles={} iterations={iterations}",
        process::id(),
        SHAPE,
        TILE_SHAPE,
        CACHE_TILES
    );
    println!("sleeping {sleep_secs}s before benchmark");
    thread::sleep(Duration::from_secs(sleep_secs));

    let (total_ns, checksum) = match mode.as_str() {
        "rust" => rust_repeated_basic(iterations),
        "cpp" => cpp_repeated_basic(iterations),
        other => {
            eprintln!("unknown mode '{other}', expected 'rust' or 'cpp'");
            process::exit(2);
        }
    };

    println!(
        "done mode={mode} total_ms={:.2} per_iter_ms={:.2} checksum={checksum:.6}",
        total_ns as f64 / 1e6,
        total_ns as f64 / 1e6 / iterations as f64,
    );
}
