// SPDX-License-Identifier: LGPL-3.0-or-later
//! Demonstration of [`TempLattice`], mirroring C++ `tTempLattice.cc`.
//!
//! Creates a `TempLattice<f32>`, fills it with a constant, reads back
//! individual elements and slices, writes a sub-region, and verifies
//! the changes.
//!
//! ```sh
//! cargo run --example t_temp_lattice -p casacore-lattices
//! ```

use casacore_lattices::{Lattice, LatticeMut, TempLattice};
use ndarray::{ArrayD, IxDyn};

fn main() {
    println!("=== TempLattice<f32> Demo ===\n");

    // 1. Create a 32x32 in-memory TempLattice<f32>.
    let mut lat = TempLattice::<f32>::new(vec![32, 32], None).expect("create TempLattice");
    println!(
        "Created TempLattice: shape={:?}, ndim={}, nelements={}, in_memory={}",
        lat.shape(),
        lat.ndim(),
        lat.nelements(),
        lat.is_in_memory()
    );
    assert_eq!(lat.shape(), &[32, 32]);
    assert_eq!(lat.ndim(), 2);
    assert_eq!(lat.nelements(), 1024);
    assert!(lat.is_in_memory());

    // 2. Fill with a constant value via set().
    lat.set(7.5).expect("set constant");
    println!("Filled all elements with 7.5");

    // 3. Read back individual elements with get_at().
    let v00 = lat.get_at(&[0, 0]).expect("get_at [0,0]");
    let v15_15 = lat.get_at(&[15, 15]).expect("get_at [15,15]");
    let v31_31 = lat.get_at(&[31, 31]).expect("get_at [31,31]");
    println!("  get_at([0,0])   = {v00}");
    println!("  get_at([15,15]) = {v15_15}");
    println!("  get_at([31,31]) = {v31_31}");
    assert!((v00 - 7.5).abs() < 1e-6);
    assert!((v15_15 - 7.5).abs() < 1e-6);
    assert!((v31_31 - 7.5).abs() < 1e-6);

    // 4. Read a slice with get_slice().
    //    Extract a 4x4 sub-region starting at [10, 10] with unit stride.
    let slice = lat
        .get_slice(&[10, 10], &[4, 4], &[1, 1])
        .expect("get_slice");
    println!(
        "  get_slice([10,10], [4,4]) -> shape={:?}, all values = 7.5: {}",
        slice.shape(),
        slice.iter().all(|&v| (v - 7.5).abs() < 1e-6)
    );
    assert_eq!(slice.shape(), &[4, 4]);
    assert!(slice.iter().all(|&v| (v - 7.5).abs() < 1e-6));

    // 5. Write a sub-region with put_slice().
    //    Place a 4x4 patch of 99.0 at position [5, 5].
    let patch = ArrayD::from_elem(IxDyn(&[4, 4]), 99.0f32);
    lat.put_slice(&patch, &[5, 5]).expect("put_slice");
    println!("  put_slice: wrote 4x4 patch of 99.0 at [5,5]");

    // 6. Verify the changes.
    //    Inside the patch: [5,5] through [8,8] should be 99.0.
    let inside = lat.get_at(&[6, 6]).expect("get_at [6,6]");
    assert!((inside - 99.0).abs() < 1e-6);
    println!("  get_at([6,6])  = {inside} (inside patch)");

    //    Outside the patch: [0,0] should still be 7.5.
    let outside = lat.get_at(&[0, 0]).expect("get_at [0,0]");
    assert!((outside - 7.5).abs() < 1e-6);
    println!("  get_at([0,0])  = {outside} (outside patch, unchanged)");

    //    Edge of patch: [5,5] and [8,8] should be 99.0.
    let edge_lo = lat.get_at(&[5, 5]).expect("get_at [5,5]");
    let edge_hi = lat.get_at(&[8, 8]).expect("get_at [8,8]");
    assert!((edge_lo - 99.0).abs() < 1e-6);
    assert!((edge_hi - 99.0).abs() < 1e-6);
    println!("  get_at([5,5])  = {edge_lo} (patch corner)");
    println!("  get_at([8,8])  = {edge_hi} (patch corner)");

    //    Just outside the patch: [4,4] and [9,9] should be 7.5.
    let near_lo = lat.get_at(&[4, 4]).expect("get_at [4,4]");
    let near_hi = lat.get_at(&[9, 9]).expect("get_at [9,9]");
    assert!((near_lo - 7.5).abs() < 1e-6);
    assert!((near_hi - 7.5).abs() < 1e-6);
    println!("  get_at([4,4])  = {near_lo} (just outside patch)");
    println!("  get_at([9,9])  = {near_hi} (just outside patch)");

    // 7. Read the full lattice and compute a summary.
    let all = lat.get().expect("get all");
    let sum: f64 = all.iter().map(|&v| v as f64).sum();
    let n_patched = all.iter().filter(|&&v| (v - 99.0).abs() < 1e-6).count();
    let n_original = all.iter().filter(|&&v| (v - 7.5).abs() < 1e-6).count();
    println!("\nSummary:");
    println!("  Total elements: {}", all.len());
    println!("  Elements at 99.0:  {n_patched} (expected 16)");
    println!("  Elements at 7.5:  {n_original} (expected {})", 1024 - 16);
    println!("  Sum: {sum:.2}");
    assert_eq!(n_patched, 16);
    assert_eq!(n_original, 1024 - 16);

    println!("\nTempLattice demo completed successfully.");
}
