// SPDX-License-Identifier: LGPL-3.0-or-later
//! Demonstration of lattice statistics, mirroring C++ `tLatticeStatistics.cc`.
//!
//! Covers the main patterns from
//! `casacore/lattices/LatticeMath/test/tLatticeStatistics.cc`:
//!
//! 1. **1-D statistics** — 64-element `ArrayLattice<f32>` filled with
//!    `indgen` (0, 1, …, 63).  All 14 statistics are computed and verified
//!    against closed-form expected values.
//! 2. **Include-range filtering** — only pixels in `[10, 50]` included.
//! 3. **Exclude-range filtering** — pixels in `[10, 50]` excluded.
//! 4. **Pixel masking** — every 3rd element unmasked (`i % 3 == 0`).
//! 5. **2-D axis-based statistics** — 64 × 20 lattice with replicated rows;
//!    `set_axes([0])` collapses the row axis, yielding 20 identical output
//!    positions each matching the 1-D statistics.
//! 6. **Large-scale timing** — 3-D array with axis-based statistics timed
//!    end-to-end.  Scale `PERF_SHAPE` to hit your desired wall-clock budget.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example t_lattice_statistics -p casacore-lattices --release
//! ```
//!
//! ## Missing C++ functions added for this demo
//!
//! The following utilities mirror C++ `ArrayMath.h` functions absent from
//! ndarray and are now available in `casacore_lattices::array_math`:
//!
//! | Rust                   | C++ equivalent               |
//! |------------------------|------------------------------|
//! | `array_median`         | `casacore::median(arr)`      |
//! | `array_fractile`       | `casacore::fractile(arr, p)` |
//! | `array_madfm`          | `casacore::madfm(arr)`       |
//! | `near` / `near_tol`    | `casacore::near(a, b)`       |
//! | `near_abs`             | `casacore::nearAbs(a, b)`    |

use std::time::Instant;

use casacore_lattices::{
    ArrayLattice, Lattice, LatticeMut, LatticeStatistics, Statistic, TempLattice, array_fractile,
    array_madfm, array_median, near_f32, near_tol,
};
use ndarray::{ArrayD, ArrayViewD, IxDyn};

fn main() {
    demo_1d_statistics();
    demo_include_range();
    demo_exclude_range();
    demo_pixel_mask();
    demo_2d_axis_stats();
    demo_performance();

    println!("\nAll t_lattice_statistics demos completed successfully.");
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. 1-D statistics over indgen 0..63
// ─────────────────────────────────────────────────────────────────────────────

fn demo_1d_statistics() {
    println!("=== 1-D statistics (indgen 0..63) ===");

    // C++: Array<Float> inArr(IPosition(1,64)); indgen(inArr);
    let data: ArrayD<f32> = ArrayD::from_shape_fn(IxDyn(&[64]), |idx| idx[0] as f32);
    let lat = ArrayLattice::new(data.clone());
    let stats = LatticeStatistics::new(&lat);

    // ── Closed-form expected values (verified against C++ tLatticeStatistics) ──
    //
    // npts  = 64
    // sum   = 0+1+…+63 = 64·63/2 = 2016
    // sumsq = Σ i² (i=0..63) = 63·64·127/6 = 85 344
    // min   = 0,  max = 63
    // mean  = 2016/64 = 31.5
    // rms   = sqrt(85344/64) = sqrt(1333.5) ≈ 36.5170
    // var   = (85344 − 64·31.5²)/63 = 21840/63 ≈ 346.6667
    // sigma = sqrt(346.6667) ≈ 18.6190
    // median (takeEvenMean=true): (31+32)/2 = 31.5
    // Q1 = sorted[floor(63·0.25+0.01)] = sorted[15] = 15
    // Q3 = sorted[floor(63·0.75+0.01)] = sorted[47] = 47
    // IQR = 47 − 15 = 32
    // madfm = median(|i−31.5|) = (15.5+16.5)/2 = 16.0

    let npts = scalar(&stats, Statistic::Npts);
    let sum = scalar(&stats, Statistic::Sum);
    let sumsq = scalar(&stats, Statistic::SumSq);
    let min = scalar(&stats, Statistic::Min);
    let max = scalar(&stats, Statistic::Max);
    let mean = scalar(&stats, Statistic::Mean);
    let rms = scalar(&stats, Statistic::Rms);
    let var = scalar(&stats, Statistic::Variance);
    let sigma = scalar(&stats, Statistic::Sigma);
    let median = scalar(&stats, Statistic::Median);
    let q1 = scalar(&stats, Statistic::Q1);
    let q3 = scalar(&stats, Statistic::Q3);
    let iqr = scalar(&stats, Statistic::Quartile);
    let madfm = scalar(&stats, Statistic::MedAbsDevMed);

    println!("  npts={npts}, sum={sum}, sumsq={sumsq}, min={min}, max={max}");
    println!("  mean={mean:.4}, rms={rms:.4}, var={var:.4}, sigma={sigma:.4}");
    println!("  median={median}, Q1={q1}, Q3={q3}, IQR={iqr}, madfm={madfm}");

    assert_eq!(npts, 64.0, "npts");
    assert_eq!(sum, 2016.0, "sum");
    assert_eq!(sumsq, 85344.0, "sumsq");
    assert_eq!(min, 0.0, "min");
    assert_eq!(max, 63.0, "max");
    assert_near(mean, 31.5, 1e-10, "mean");
    assert_near(rms, 36.517_028_824_647_3, 1e-6, "rms");
    assert_near(var, 346.666_666_666_7, 1e-6, "variance");
    assert_near(sigma, 18.619_045_764_45, 1e-6, "sigma");
    assert_eq!(median, 31.5, "median");
    assert_eq!(q1, 15.0, "Q1");
    assert_eq!(q3, 47.0, "Q3");
    assert_eq!(iqr, 32.0, "IQR");
    assert_eq!(madfm, 16.0, "madfm");

    // Verify array_math utilities against the same data.
    // These mirror C++ casacore::median / fractile / madfm on Array<Float>.
    assert_eq!(array_median(&data), 31.5, "array_median");
    assert_eq!(array_fractile(&data, 0.25), 15.0, "array_fractile Q1");
    assert_eq!(array_fractile(&data, 0.75), 47.0, "array_fractile Q3");
    assert_eq!(array_madfm(&data), 16.0, "array_madfm");

    // Verify min/max positions.
    let (min_pos, max_pos) = stats.get_min_max_pos().unwrap();
    assert_eq!(min_pos.as_deref(), Some(&[0usize][..]), "min_pos");
    assert_eq!(max_pos.as_deref(), Some(&[63usize][..]), "max_pos");
    println!("  min_pos={min_pos:?}, max_pos={max_pos:?}");

    println!("  All 1-D assertions passed.");
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Include-range filtering  [10, 50]
// ─────────────────────────────────────────────────────────────────────────────

fn demo_include_range() {
    println!("\n=== Include-range [10, 50] ===");

    // C++: stats.setInExCludeRange(range, Vector<Float>(), False);
    let data: ArrayD<f32> = ArrayD::from_shape_fn(IxDyn(&[64]), |idx| idx[0] as f32);
    let lat = ArrayLattice::new(data.clone());
    let mut stats = LatticeStatistics::new(&lat);
    stats.set_include_range(10.0, 50.0);

    // Valid pixels: 10..=50  →  41 values
    // sum = Σ i (i=10..50) = 41·(10+50)/2 = 1230
    // mean = 1230/41 = 30.0
    // median: 41 values, sorted[20] = 30.0
    // Q1: sorted[floor(40·0.25+0.01)] = sorted[10] = 20
    // Q3: sorted[floor(40·0.75+0.01)] = sorted[30] = 40

    let npts = scalar(&stats, Statistic::Npts);
    let sum = scalar(&stats, Statistic::Sum);
    let mean = scalar(&stats, Statistic::Mean);
    let min = scalar(&stats, Statistic::Min);
    let max = scalar(&stats, Statistic::Max);
    let median = scalar(&stats, Statistic::Median);
    let q1 = scalar(&stats, Statistic::Q1);
    let q3 = scalar(&stats, Statistic::Q3);

    println!("  npts={npts}, sum={sum}, mean={mean}, min={min}, max={max}");
    println!("  median={median}, Q1={q1}, Q3={q3}");

    assert_eq!(npts, 41.0, "npts include");
    assert_eq!(sum, 1230.0, "sum include");
    assert_near(mean, 30.0, 1e-10, "mean include");
    assert_eq!(min, 10.0, "min include");
    assert_eq!(max, 50.0, "max include");
    assert_eq!(median, 30.0, "median include");
    assert_eq!(q1, 20.0, "Q1 include");
    assert_eq!(q3, 40.0, "Q3 include");

    println!("  All include-range assertions passed.");
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Exclude-range filtering  [10, 50]
// ─────────────────────────────────────────────────────────────────────────────

fn demo_exclude_range() {
    println!("\n=== Exclude-range [10, 50] ===");

    // C++: stats.setInExCludeRange(Vector<Float>(), range, False);
    let data: ArrayD<f32> = ArrayD::from_shape_fn(IxDyn(&[64]), |idx| idx[0] as f32);
    let lat = ArrayLattice::new(data.clone());
    let mut stats = LatticeStatistics::new(&lat);
    stats.set_exclude_range(10.0, 50.0);

    // Valid pixels: 0..=9 (10) + 51..=63 (13) = 23 values
    // sum = Σ(0..9) + Σ(51..63) = 45 + 741 = 786
    // min = 0, max = 63

    let npts = scalar(&stats, Statistic::Npts);
    let sum = scalar(&stats, Statistic::Sum);
    let min = scalar(&stats, Statistic::Min);
    let max = scalar(&stats, Statistic::Max);

    println!("  npts={npts}, sum={sum}, min={min}, max={max}");

    assert_eq!(npts, 23.0, "npts exclude");
    assert_eq!(
        sum,
        (0..=9i32).sum::<i32>() as f64 + (51..=63i32).sum::<i32>() as f64,
        "sum exclude"
    );
    assert_eq!(min, 0.0, "min exclude");
    assert_eq!(max, 63.0, "max exclude");

    println!("  All exclude-range assertions passed.");
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Pixel masking (every 3rd element valid: i % 3 == 0)
// ─────────────────────────────────────────────────────────────────────────────

fn demo_pixel_mask() {
    println!("\n=== Pixel mask (i % 3 == 0) ===");

    // C++:
    //   Vector<Bool> mask(1000); count=0;
    //   while (miter!=mend) { *miter = count%3==0; … }
    //   subLatt.setPixelMask(ArrayLattice<Bool>(mask), True);
    let data: ArrayD<f32> = ArrayD::from_shape_fn(IxDyn(&[64]), |idx| idx[0] as f32);
    let lat = ArrayLattice::new(data.clone());
    let mask: ArrayD<bool> = ArrayD::from_shape_fn(IxDyn(&[64]), |idx| idx[0] % 3 == 0);

    let mut stats = LatticeStatistics::new(&lat);
    stats.set_pixel_mask(mask);

    // Valid pixels: 0, 3, 6, …, 63  →  22 values (floor(63/3)+1 = 22)
    // sum = 3·(0+1+…+21) = 3·231 = 693
    // min = 0, max = 63
    // median: 22 values, n2=(22-1)/2=10, sorted[10]=30, sorted[11]=33
    //   median = (30+33)/2 = 31.5

    let npts = scalar(&stats, Statistic::Npts);
    let sum = scalar(&stats, Statistic::Sum);
    let min = scalar(&stats, Statistic::Min);
    let max = scalar(&stats, Statistic::Max);
    let median = scalar(&stats, Statistic::Median);

    println!("  npts={npts}, sum={sum}, min={min}, max={max}, median={median}");

    assert_eq!(npts, 22.0, "npts masked");
    assert_eq!(sum, 693.0, "sum masked");
    assert_eq!(min, 0.0, "min masked");
    assert_eq!(max, 63.0, "max masked");
    assert_eq!(median, 31.5, "median masked");

    // Completely masked → empty result (matches C++ size==0 check).
    let all_false: ArrayD<bool> = ArrayD::from_elem(IxDyn(&[64]), false);
    let lat2 = ArrayLattice::new(ArrayD::from_shape_fn(IxDyn(&[64]), |idx| idx[0] as f32));
    let mut stats2 = LatticeStatistics::new(&lat2);
    stats2.set_pixel_mask(all_false);
    let empty = stats2.get_statistic(Statistic::Npts).unwrap();
    assert_eq!(empty.len(), 0, "completely masked → empty result");
    println!("  Completely masked → empty array (len={})", empty.len());

    println!("  All mask assertions passed.");
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. 2-D axis-based statistics (64 × 20, axes=[0])
// ─────────────────────────────────────────────────────────────────────────────

fn demo_2d_axis_stats() {
    println!("\n=== 2-D axis statistics (64×20, axes=[0]) ===");

    // C++ do2DFloat: replicate 1-D arr across nY=20 columns, setAxes([0]).
    // lat[i, j] = i  (the row value, same for every column).
    // With axes=[0] we collapse rows → output shape [20].
    // Each output position j=0..19 holds statistics over {0,1,…,63},
    // so every column has the same statistics as the 1-D case.
    let n_x: usize = 64;
    let n_y: usize = 20;
    let data: ArrayD<f32> = ArrayD::from_shape_fn(IxDyn(&[n_x, n_y]), |idx| idx[0] as f32);
    let lat = ArrayLattice::new(data.clone());
    let mut stats = LatticeStatistics::new(&lat);
    stats.set_axes(vec![0]); // collapse rows, display over columns

    let npts_arr = stats.get_statistic(Statistic::Npts).unwrap();
    let mean_arr = stats.get_statistic(Statistic::Mean).unwrap();
    let median_arr = stats.get_statistic(Statistic::Median).unwrap();
    let q1_arr = stats.get_statistic(Statistic::Q1).unwrap();
    let q3_arr = stats.get_statistic(Statistic::Q3).unwrap();

    assert_eq!(npts_arr.shape(), &[n_y], "output shape [n_y]");

    // Every column contains the same 64 values 0..63.
    for j in 0..n_y {
        let pos = IxDyn(&[j]);
        assert_eq!(npts_arr[pos.clone()], 64.0, "npts[{j}]");
        assert_near(mean_arr[pos.clone()], 31.5, 1e-4, &format!("mean[{j}]"));
        assert_eq!(median_arr[pos.clone()], 31.5, "median[{j}]");
        assert_eq!(q1_arr[pos.clone()], 15.0, "Q1[{j}]");
        assert_eq!(q3_arr[pos.clone()], 47.0, "Q3[{j}]");
    }

    println!(
        "  Output shape: {:?}  (one result per column)",
        npts_arr.shape()
    );
    println!(
        "  Column 0:  npts={}, mean={:.1}, median={}, Q1={}, Q3={}",
        npts_arr[IxDyn(&[0])],
        mean_arr[IxDyn(&[0])],
        median_arr[IxDyn(&[0])],
        q1_arr[IxDyn(&[0])],
        q3_arr[IxDyn(&[0])],
    );
    println!("  All 2-D axis assertions passed ({n_y} columns).");
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. Large-scale timing
// ─────────────────────────────────────────────────────────────────────────────
//
// Array size for benchmark.  Adjust to hit your target wall-clock budget.
//
//   Shape          Elements    f32 RAM    Approx. time (Apple M-series)
//   ─────────────────────────────────────────────────────────────────────
//   [512, 512, 64]   16.8 M     64 MB      ~10 s  (quick check)
//   [1024,1024, 64]  67.1 M    256 MB      ~60 s
//   [1024,1024,256] 268.4 M    1.0 GB      ~5 min (reference target)
//   [2048,2048,256] 1073.7 M   4.0 GB      very long
//
// With axes=[0,1] (collapse spatial axes) each of the 64/256 "channel"
// positions sorts a 512² or 1024² vector — the sort is the bottleneck.

fn demo_performance() {
    let perf_shape = perf_shape();
    println!("\n=== Performance timing: shape={perf_shape:?}, axes=[0,1] ===");

    let n_elem: usize = perf_shape.iter().product();
    println!(
        "  Creating {n_elem} f32 elements ({:.0} MB) …",
        n_elem as f64 * 4.0 / 1024.0 / 1024.0
    );

    // Ramp data: value at (x, y, c) = x + y*PERF_SHAPE[0] + c*spatial
    // Each channel c contains a distinct linear ramp over the spatial plane.
    let spatial = perf_shape[0] * perf_shape[1];
    let data: ArrayD<f32> = ArrayD::from_shape_fn(IxDyn(&perf_shape), |idx| {
        let x = idx[0];
        let y = idx[1];
        let c = idx[2];
        (x + y * perf_shape[0] + c * spatial) as f32
    });

    let n_chan = perf_shape[2];
    let axes = [0usize, 1usize];

    let t_baseline_basic = Instant::now();
    let baseline_npts = baseline_axis_stat(data.view(), &axes, Statistic::Npts);
    let baseline_mean = baseline_axis_stat(data.view(), &axes, Statistic::Mean);
    let baseline_sigma = baseline_axis_stat(data.view(), &axes, Statistic::Sigma);
    let baseline_basic = t_baseline_basic.elapsed();

    let t_baseline_order = Instant::now();
    let baseline_median = baseline_axis_stat(data.view(), &axes, Statistic::Median);
    let baseline_q1 = baseline_axis_stat(data.view(), &axes, Statistic::Q1);
    let baseline_q3 = baseline_axis_stat(data.view(), &axes, Statistic::Q3);
    let baseline_order = t_baseline_order.elapsed();

    let lat = ArrayLattice::new(data.clone());
    let mut stats = LatticeStatistics::new(&lat);
    stats.set_axes(axes.to_vec()); // collapse spatial axes

    // ── basic pass-1 statistics ───────────────────────────────────────────
    let t0 = Instant::now();
    let npts_arr = stats.get_statistic(Statistic::Npts).unwrap();
    let mean_arr = stats.get_statistic(Statistic::Mean).unwrap();
    let sigma_arr = stats.get_statistic(Statistic::Sigma).unwrap();
    let t_basic = t0.elapsed();

    assert_eq!(npts_arr.shape(), &[n_chan]);
    assert_eq!(npts_arr, baseline_npts);
    for channel in 0..n_chan {
        assert_near(
            mean_arr[IxDyn(&[channel])],
            baseline_mean[IxDyn(&[channel])],
            1e-8,
            &format!("baseline mean[{channel}]"),
        );
        assert_near(
            sigma_arr[IxDyn(&[channel])],
            baseline_sigma[IxDyn(&[channel])],
            1e-8,
            &format!("baseline sigma[{channel}]"),
        );
    }
    // Each channel has exactly `spatial` valid pixels.
    assert_near(npts_arr[IxDyn(&[0])], spatial as f64, 1e-10, "perf npts[0]");
    // Channel c: pixels = c*spatial, c*spatial+1, …, c*spatial+(spatial-1)
    // mean = c*spatial + (spatial-1)/2
    let c0_mean = (perf_shape[2] - 1) as f64 / 2.0 * spatial as f64 + (spatial - 1) as f64 / 2.0;
    // Actually: channel 0 pixels = 0..spatial-1, mean=(spatial-1)/2
    let chan0_mean = (spatial - 1) as f64 / 2.0;
    assert!(
        near_tol(mean_arr[IxDyn(&[0])], chan0_mean, 1e-4),
        "perf mean[0]: got {}, expected {chan0_mean}",
        mean_arr[IxDyn(&[0])]
    );

    println!(
        "  ArrayLattice basic stats: traversal={t_basic:.2?}, baseline={baseline_basic:.2?}, ratio={:.2}x  |  \
         chan[0] mean={:.1}, sigma={:.1}",
        duration_ratio(t_basic, baseline_basic),
        mean_arr[IxDyn(&[0])],
        sigma_arr[IxDyn(&[0])],
    );
    let _ = c0_mean; // silence unused variable

    // ── order statistics (sort-intensive) ────────────────────────────────
    let t1 = Instant::now();
    let median_arr = stats.get_statistic(Statistic::Median).unwrap();
    let q1_arr = stats.get_statistic(Statistic::Q1).unwrap();
    let q3_arr = stats.get_statistic(Statistic::Q3).unwrap();
    let t_order = t1.elapsed();

    assert_eq!(median_arr.shape(), &[n_chan]);
    assert_eq!(median_arr, baseline_median);
    assert_eq!(q1_arr, baseline_q1);
    assert_eq!(q3_arr, baseline_q3);
    // Channel 0: sort 0..spatial-1, median = floor((spatial-1)·0.5+0.01)
    //   For spatial=1024*1024=1048576:
    //   C++ median n2 = (1048576-1)/2 = 524287 (odd n, so single value)
    let c0_median_n2 = (spatial - 1) / 2;
    assert_near(
        median_arr[IxDyn(&[0])],
        c0_median_n2 as f64,
        1e-3,
        "perf median[0]",
    );

    println!(
        "  ArrayLattice order stats: traversal={t_order:.2?}, baseline={baseline_order:.2?}, ratio={:.2}x  |  \
         chan[0] median={:.1}, Q1={:.1}, Q3={:.1}",
        duration_ratio(t_order, baseline_order),
        median_arr[IxDyn(&[0])],
        q1_arr[IxDyn(&[0])],
        q3_arr[IxDyn(&[0])],
    );

    let mut paged = TempLattice::<f32>::new(perf_shape.clone(), Some(1)).unwrap();
    assert!(paged.is_paged(), "performance temp lattice should be paged");
    paged.put_slice(&data, &vec![0; perf_shape.len()]).unwrap();
    let paged_tile_shape = match &paged {
        TempLattice::Paged { array, .. } => array.tile_shape().to_vec(),
        TempLattice::Memory(_) => unreachable!("forced paged temp lattice stayed in memory"),
    };
    let paged_cursor_shape = paged.nice_cursor_shape();
    println!(
        "  TempLattice paged mode: tile_shape={paged_tile_shape:?}, cursor_shape={paged_cursor_shape:?}"
    );

    let mut paged_stats = LatticeStatistics::new(&paged);
    paged_stats.set_axes(axes.to_vec());

    let t2 = Instant::now();
    let paged_npts = paged_stats.get_statistic(Statistic::Npts).unwrap();
    let paged_mean = paged_stats.get_statistic(Statistic::Mean).unwrap();
    let paged_sigma = paged_stats.get_statistic(Statistic::Sigma).unwrap();
    let t_paged_basic = t2.elapsed();

    assert_eq!(paged_npts, baseline_npts);
    for channel in 0..n_chan {
        assert_near(
            paged_mean[IxDyn(&[channel])],
            baseline_mean[IxDyn(&[channel])],
            1e-8,
            &format!("paged mean[{channel}]"),
        );
        assert_near(
            paged_sigma[IxDyn(&[channel])],
            baseline_sigma[IxDyn(&[channel])],
            1e-8,
            &format!("paged sigma[{channel}]"),
        );
    }

    println!(
        "  TempLattice basic stats: traversal={t_paged_basic:.2?}, baseline={baseline_basic:.2?}, ratio={:.2}x  |  \
         chan[0] mean={:.1}, sigma={:.1}",
        duration_ratio(t_paged_basic, baseline_basic),
        paged_mean[IxDyn(&[0])],
        paged_sigma[IxDyn(&[0])],
    );

    let t3 = Instant::now();
    let paged_median = paged_stats.get_statistic(Statistic::Median).unwrap();
    let paged_q1 = paged_stats.get_statistic(Statistic::Q1).unwrap();
    let paged_q3 = paged_stats.get_statistic(Statistic::Q3).unwrap();
    let t_paged_order = t3.elapsed();

    assert_eq!(paged_median, baseline_median);
    assert_eq!(paged_q1, baseline_q1);
    assert_eq!(paged_q3, baseline_q3);

    println!(
        "  TempLattice order stats: traversal={t_paged_order:.2?}, baseline={baseline_order:.2?}, ratio={:.2}x  |  \
         chan[0] median={:.1}, Q1={:.1}, Q3={:.1}",
        duration_ratio(t_paged_order, baseline_order),
        paged_median[IxDyn(&[0])],
        paged_q1[IxDyn(&[0])],
        paged_q3[IxDyn(&[0])],
    );

    if let Some(cache_tiles) = paged_cache_tiles() {
        let total_tiles = perf_shape
            .iter()
            .zip(paged_tile_shape.iter())
            .map(|(&dim, &tile)| dim.div_ceil(tile))
            .product::<usize>();
        paged.set_cache_size_in_tiles(cache_tiles).unwrap();
        paged.temp_close().unwrap();

        println!(
            "  TempLattice forced-I/O mode: cache_tiles={cache_tiles}, total_tiles={total_tiles}, max_cache_pixels={}",
            paged.maximum_cache_size_pixels()
        );

        let mut forced_stats = LatticeStatistics::new(&paged);
        forced_stats.set_axes(axes.to_vec());

        let t4 = Instant::now();
        let forced_mean = forced_stats.get_statistic(Statistic::Mean).unwrap();
        let forced_sigma = forced_stats.get_statistic(Statistic::Sigma).unwrap();
        let t_forced_basic = t4.elapsed();

        for channel in 0..n_chan {
            assert_near(
                forced_mean[IxDyn(&[channel])],
                baseline_mean[IxDyn(&[channel])],
                1e-8,
                &format!("forced mean[{channel}]"),
            );
            assert_near(
                forced_sigma[IxDyn(&[channel])],
                baseline_sigma[IxDyn(&[channel])],
                1e-8,
                &format!("forced sigma[{channel}]"),
            );
        }

        println!(
            "  TempLattice forced-I/O basic stats: traversal={t_forced_basic:.2?}, baseline={baseline_basic:.2?}, ratio={:.2}x  |  \
             chan[0] mean={:.1}, sigma={:.1}",
            duration_ratio(t_forced_basic, baseline_basic),
            forced_mean[IxDyn(&[0])],
            forced_sigma[IxDyn(&[0])],
        );

        let t5 = Instant::now();
        let forced_median = forced_stats.get_statistic(Statistic::Median).unwrap();
        let forced_q1 = forced_stats.get_statistic(Statistic::Q1).unwrap();
        let forced_q3 = forced_stats.get_statistic(Statistic::Q3).unwrap();
        let t_forced_order = t5.elapsed();

        assert_eq!(forced_median, baseline_median);
        assert_eq!(forced_q1, baseline_q1);
        assert_eq!(forced_q3, baseline_q3);

        println!(
            "  TempLattice forced-I/O order stats: traversal={t_forced_order:.2?}, baseline={baseline_order:.2?}, ratio={:.2}x  |  \
             chan[0] median={:.1}, Q1={:.1}, Q3={:.1}",
            duration_ratio(t_forced_order, baseline_order),
            forced_median[IxDyn(&[0])],
            forced_q1[IxDyn(&[0])],
            forced_q3[IxDyn(&[0])],
        );
    }

    let t_total = t0.elapsed();
    println!("  Total elapsed including paged benchmark: {t_total:.2?}");
    println!("  Set CASA_RS_LATTICE_STATS_PERF_SHAPE=x,y,z to scale the benchmark.");
    println!(
        "  Set CASA_RS_LATTICE_STATS_PAGED_CACHE_TILES=n to rerun the paged benchmark with a bounded tile cache."
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Extract the single scalar value from a global-stats result array.
fn scalar<T: casacore_lattices::StatsElement>(
    stats: &LatticeStatistics<T>,
    stat: Statistic,
) -> f64 {
    let arr = stats.get_statistic(stat).unwrap();
    assert_eq!(arr.len(), 1, "expected scalar result for {stat:?}");
    arr[IxDyn(&[0])]
}

/// Assert two f64 values are within relative tolerance.
/// Mirrors C++ `AlwaysAssert(near(a, b, tol), AipsError)`.
fn assert_near(actual: f64, expected: f64, tol: f64, label: &str) {
    assert!(
        near_f32(actual, expected) || near_tol(actual, expected, tol),
        "{label}: {actual} != {expected} (tol={tol})"
    );
}

fn perf_shape() -> Vec<usize> {
    std::env::var("CASA_RS_LATTICE_STATS_PERF_SHAPE")
        .ok()
        .and_then(|raw| {
            let dims: Option<Vec<usize>> = raw
                .split(',')
                .map(|part| part.trim().parse::<usize>().ok())
                .collect();
            dims.filter(|dims| dims.len() == 3 && dims.iter().all(|&dim| dim > 0))
        })
        .unwrap_or_else(|| vec![512, 512, 64])
}

fn paged_cache_tiles() -> Option<usize> {
    std::env::var("CASA_RS_LATTICE_STATS_PAGED_CACHE_TILES")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
}

fn duration_ratio(new_time: std::time::Duration, baseline: std::time::Duration) -> f64 {
    if baseline.is_zero() {
        f64::INFINITY
    } else {
        new_time.as_secs_f64() / baseline.as_secs_f64()
    }
}

fn baseline_axis_stat(data: ArrayViewD<'_, f32>, axes: &[usize], stat: Statistic) -> ArrayD<f64> {
    let shape = data.shape();
    let ndim = shape.len();
    let out_axes: Vec<usize> = (0..ndim).filter(|d| !axes.contains(d)).collect();
    let out_shape: Vec<usize> = out_axes.iter().map(|&d| shape[d]).collect();

    if out_shape.is_empty() {
        let values: Vec<f64> = data.iter().map(|value| f64::from(*value)).collect();
        return ArrayD::from_elem(IxDyn(&[1]), baseline_compute_statistic(&values, stat));
    }

    let collapsed_shape: Vec<usize> = axes.iter().map(|&d| shape[d]).collect();
    let n_out: usize = out_shape.iter().product();
    let n_collapsed: usize = collapsed_shape.iter().product();
    let mut full_idx = vec![0usize; ndim];
    let mut result = vec![0.0; n_out];

    for (out_flat, slot) in result.iter_mut().enumerate() {
        let out_idx = baseline_flat_to_multiidx(out_flat, &out_shape);
        for (i, &axis) in out_axes.iter().enumerate() {
            full_idx[axis] = out_idx[i];
        }

        let mut values = Vec::with_capacity(n_collapsed);
        for collapsed_flat in 0..n_collapsed {
            let collapsed_idx = baseline_flat_to_multiidx(collapsed_flat, &collapsed_shape);
            for (&axis, &coord) in axes.iter().zip(collapsed_idx.iter()) {
                full_idx[axis] = coord;
            }
            values.push(f64::from(data[IxDyn(&full_idx)]));
        }

        *slot = baseline_compute_statistic(&values, stat);
    }

    ArrayD::from_shape_vec(IxDyn(&out_shape), result).expect("shape/data size match")
}

fn baseline_flat_to_multiidx(mut flat: usize, shape: &[usize]) -> Vec<usize> {
    let mut idx = vec![0usize; shape.len()];
    for axis in (0..shape.len()).rev() {
        idx[axis] = flat % shape[axis];
        flat /= shape[axis];
    }
    idx
}

fn baseline_compute_statistic(values: &[f64], stat: Statistic) -> f64 {
    match stat {
        Statistic::Npts => values.len() as f64,
        Statistic::Mean => values.iter().sum::<f64>() / values.len() as f64,
        Statistic::Sigma => {
            if values.len() < 2 {
                return f64::NAN;
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance = values
                .iter()
                .map(|value| (value - mean).powi(2))
                .sum::<f64>()
                / (values.len() - 1) as f64;
            variance.sqrt()
        }
        Statistic::Median => baseline_median(values),
        Statistic::Q1 => baseline_fractile(values, 0.25),
        Statistic::Q3 => baseline_fractile(values, 0.75),
        _ => panic!("baseline stat {stat:?} is not implemented in the demo"),
    }
}

fn baseline_median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).expect("no NaNs in benchmark data"));
    let n = sorted.len();
    let n2 = (n - 1) / 2;
    if n % 2 == 0 {
        (sorted[n2] + sorted[n2 + 1]) / 2.0
    } else {
        sorted[n2]
    }
}

fn baseline_fractile(values: &[f64], frac: f64) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).expect("no NaNs in benchmark data"));
    let idx = ((sorted.len() - 1) as f64 * frac + 0.01) as usize;
    sorted[idx.min(sorted.len() - 1)]
}
