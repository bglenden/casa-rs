// SPDX-License-Identifier: LGPL-3.0-or-later
//! Benchmark compiled `ImageExpr` evaluation and materialization policies.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example profile_image_expr_save_as -p casacore-images
//! ```

use std::time::Instant;

use casacore_coordinates::CoordinateSystem;
use casacore_images::{ImageExpr, MaskExpr, PagedImage, TempImage};
use casacore_lattices::ExecutionPolicy;
use ndarray::{ArrayD, IxDyn};

const DEFAULT_SHAPE: [usize; 3] = [256, 256, 96];
const DEFAULT_TILE_SHAPE: [usize; 3] = [16, 16, 16];

fn main() {
    let shape = parse_shape_env("CASA_RS_IMAGE_EXPR_SHAPE", &DEFAULT_SHAPE);
    let repeats = std::env::var("CASA_RS_IMAGE_EXPR_REPEATS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(3);
    let include_diagnostics = std::env::var("CASA_RS_IMAGE_EXPR_DIAGNOSTICS")
        .ok()
        .map(|value| value != "0")
        .unwrap_or(true);
    let include_snapshot_diagnostics = std::env::var("CASA_RS_IMAGE_EXPR_INCLUDE_SNAPSHOT")
        .ok()
        .map(|value| value != "0")
        .unwrap_or(true);
    let workers = std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(2)
        .max(2);

    println!(
        "shape={shape:?} repeats={repeats} workers={workers} diagnostics={include_diagnostics} snapshot_diagnostics={include_snapshot_diagnostics}"
    );

    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.image");
    let source = build_paged_source(&source_path, &shape, &DEFAULT_TILE_SHAPE);

    let base = ImageExpr::from_image(&source).unwrap();
    let condition = base
        .clone()
        .gt_scalar((shape.iter().product::<usize>() / 3) as f32);
    let heavy = base
        .clone()
        .multiply_scalar(2.0)
        .add_scalar(1.0)
        .sin()
        .exp();
    let negated = base.multiply_scalar(-1.0);
    let masked = ImageExpr::iif(condition.clone(), heavy.clone(), negated.clone()).unwrap();

    profile_expr("compiled get (unmasked)", &heavy, repeats, workers, false);
    profile_expr("compiled get (masked)", &masked, repeats, workers, false);
    profile_expr(
        "compiled save_as (unmasked)",
        &heavy,
        repeats,
        workers,
        true,
    );
    profile_expr("compiled save_as (masked)", &masked, repeats, workers, true);
    if !include_diagnostics {
        return;
    }

    println!("masked decomposition (get only):");
    profile_mask("  condition", &condition, repeats, workers);
    profile_expr("  true branch", &heavy, repeats, workers, false);
    profile_expr("  false branch", &negated, repeats, workers, false);
    profile_expr("  combined", &masked, repeats, workers, false);
    if include_snapshot_diagnostics {
        let snapshot = build_snapshot_source(&shape);
        let snapshot_base = ImageExpr::from_image(&snapshot).unwrap();
        println!("masked source comparison (get only):");
        let snapshot_heavy = snapshot_base
            .clone()
            .multiply_scalar(2.0)
            .add_scalar(1.0)
            .sin()
            .exp();
        let snapshot_masked = ImageExpr::iif(
            snapshot_base
                .clone()
                .gt_scalar((shape.iter().product::<usize>() / 3) as f32),
            snapshot_heavy.clone(),
            snapshot_base.multiply_scalar(-1.0),
        )
        .unwrap();
        profile_expr("  paged masked", &masked, repeats, workers, false);
        profile_expr(
            "  snapshotted masked",
            &snapshot_masked,
            repeats,
            workers,
            false,
        );
    }
}

fn build_paged_source(
    path: &std::path::Path,
    shape: &[usize],
    tile_shape: &[usize],
) -> PagedImage<f32> {
    let mut image = PagedImage::<f32>::create_with_tile_shape_and_cache(
        shape.to_vec(),
        tile_shape.to_vec(),
        CoordinateSystem::new(),
        path,
        tile_bytes(tile_shape, std::mem::size_of::<f32>()),
    )
    .unwrap();
    let data = ArrayD::from_shape_fn(IxDyn(shape), |idx| {
        (idx[0] + idx[1] * shape[0] + idx[2] * shape[0] * shape[1]) as f32
    });
    image.put_slice(&data, &vec![0; shape.len()]).unwrap();
    image.save().unwrap();
    PagedImage::<f32>::open_with_cache(path, tile_bytes(tile_shape, std::mem::size_of::<f32>()))
        .unwrap()
}

fn build_snapshot_source(shape: &[usize]) -> TempImage<f32> {
    let mut image = TempImage::<f32>::new(shape.to_vec(), CoordinateSystem::new()).unwrap();
    let data = ArrayD::from_shape_fn(IxDyn(shape), |idx| {
        (idx[0] + idx[1] * shape[0] + idx[2] * shape[0] * shape[1]) as f32
    });
    image.put_slice(&data, &vec![0; shape.len()]).unwrap();
    image
}

fn profile_expr(
    label: &str,
    expr: &ImageExpr<'_, f32>,
    repeats: usize,
    workers: usize,
    save_as: bool,
) {
    let serial = benchmark_ms(repeats, || {
        run_expr(expr, ExecutionPolicy::Serial, workers, save_as)
    });
    let auto = benchmark_ms(repeats, || {
        run_expr(expr, ExecutionPolicy::Auto, workers, save_as)
    });
    let pipelined = benchmark_ms(repeats, || {
        run_expr(
            expr,
            ExecutionPolicy::Pipelined {
                prefetch_depth: workers * 2,
            },
            workers,
            save_as,
        )
    });
    let parallel = benchmark_ms(repeats, || {
        run_expr(
            expr,
            ExecutionPolicy::Parallel {
                workers,
                prefetch_depth: workers * 2,
            },
            workers,
            save_as,
        )
    });

    println!("{label}:");
    println!("  serial      {:10.2} ms   ratio {:6.2}x", serial, 1.0);
    println!(
        "  auto        {:10.2} ms   ratio {:6.2}x",
        auto,
        auto / serial.max(0.001)
    );
    println!(
        "  pipelined   {:10.2} ms   ratio {:6.2}x",
        pipelined,
        pipelined / serial.max(0.001)
    );
    println!(
        "  parallel    {:10.2} ms   ratio {:6.2}x",
        parallel,
        parallel / serial.max(0.001)
    );
}

fn profile_mask(label: &str, expr: &MaskExpr<'_, f32>, repeats: usize, workers: usize) {
    let serial = benchmark_ms(repeats, || run_mask(expr, ExecutionPolicy::Serial));
    let auto = benchmark_ms(repeats, || run_mask(expr, ExecutionPolicy::Auto));
    let pipelined = benchmark_ms(repeats, || {
        run_mask(
            expr,
            ExecutionPolicy::Pipelined {
                prefetch_depth: workers * 2,
            },
        )
    });
    let parallel = benchmark_ms(repeats, || {
        run_mask(
            expr,
            ExecutionPolicy::Parallel {
                workers,
                prefetch_depth: workers * 2,
            },
        )
    });

    println!("{label}:");
    println!("    serial      {:10.2} ms   ratio {:6.2}x", serial, 1.0);
    println!(
        "    auto        {:10.2} ms   ratio {:6.2}x",
        auto,
        auto / serial.max(0.001)
    );
    println!(
        "    pipelined   {:10.2} ms   ratio {:6.2}x",
        pipelined,
        pipelined / serial.max(0.001)
    );
    println!(
        "    parallel    {:10.2} ms   ratio {:6.2}x",
        parallel,
        parallel / serial.max(0.001)
    );
}

fn run_expr(
    expr: &ImageExpr<'_, f32>,
    policy: ExecutionPolicy,
    workers: usize,
    save_as: bool,
) -> f64 {
    let mut compiled = expr.compile().unwrap();
    compiled.set_execution_policy(policy);
    let start = Instant::now();
    if save_as {
        let out_dir = tempfile::tempdir().unwrap();
        let out_path = out_dir.path().join("compiled.image");
        let saved = compiled.save_as(&out_path).unwrap();
        let _ = saved.get_at(&vec![0; saved.ndim()]).unwrap_or_default();
    } else {
        let _ = compiled.get().unwrap();
    }
    let _ = workers;
    start.elapsed().as_secs_f64() * 1000.0
}

fn run_mask(expr: &MaskExpr<'_, f32>, policy: ExecutionPolicy) -> f64 {
    let mut compiled = expr.compile().unwrap();
    compiled.set_execution_policy(policy);
    let start = Instant::now();
    let _ = compiled.get().unwrap();
    start.elapsed().as_secs_f64() * 1000.0
}

fn benchmark_ms(repeats: usize, mut run_once: impl FnMut() -> f64) -> f64 {
    let mut times = Vec::with_capacity(repeats.max(1));
    for _ in 0..repeats.max(1) {
        times.push(run_once());
    }
    times.sort_by(f64::total_cmp);
    times[times.len() / 2]
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

fn tile_bytes(tile_shape: &[usize], bytes_per_pixel: usize) -> usize {
    tile_shape
        .iter()
        .product::<usize>()
        .saturating_mul(bytes_per_pixel)
}
