// SPDX-License-Identifier: LGPL-3.0-or-later
//! Demonstration of [`SubLattice`] region extraction, mirroring C++
//! `tSubLattice.cc`.
//!
//! Creates an `ArrayLattice<f64>` filled with a ramp, defines an
//! `LCBox` region to select a sub-region, wraps it in a `SubLattice`,
//! and verifies the shape and values of the sub-view.
//!
//! ```sh
//! cargo run --example t_sub_lattice -p casacore-lattices
//! ```

use casacore_lattices::{ArrayLattice, LCBox, LCRegion, Lattice, SubLattice};
use ndarray::{ArrayD, IxDyn};

fn main() {
    println!("=== SubLattice Demo ===\n");

    // 1. Create a 10x10 ArrayLattice<f64> filled with a ramp.
    //    Value at [i, j] = i*10 + j.
    let data = ArrayD::from_shape_fn(IxDyn(&[10, 10]), |idx| (idx[0] * 10 + idx[1]) as f64);
    let lat = ArrayLattice::new(data);
    println!(
        "Created ArrayLattice: shape={:?}, ndim={}, nelements={}",
        lat.shape(),
        lat.ndim(),
        lat.nelements()
    );
    println!("  Ramp values: [0,0]={}, [9,9]={}", 0, 99);

    // 2. Create an LCBox region selecting [2,3] through [5,7].
    let blc = vec![2, 3]; // bottom-left corner
    let trc = vec![5, 7]; // top-right corner
    let lattice_shape = vec![10, 10];
    let region = LCBox::new(blc.clone(), trc.clone(), lattice_shape);
    println!("\nLCBox region:");
    println!("  BLC (bottom-left corner):  {blc:?}");
    println!("  TRC (top-right corner):    {trc:?}");
    println!(
        "  Bounding box shape:        {:?}",
        region.bounding_box_shape()
    );
    println!("  contains([3,5]):           {}", region.contains(&[3, 5]));
    println!("  contains([0,0]):           {}", region.contains(&[0, 0]));
    assert_eq!(region.bounding_box_shape(), vec![4, 5]);
    assert!(region.contains(&[3, 5]));
    assert!(!region.contains(&[0, 0]));

    // 3. Create a SubLattice from the lattice + region.
    let sub = SubLattice::new(&lat, Box::new(region));
    println!("\nSubLattice:");
    println!("  shape:       {:?}", sub.shape());
    println!("  ndim:        {}", sub.ndim());
    println!("  nelements:   {}", sub.nelements());
    println!("  is_writable: {}", sub.is_writable());
    assert_eq!(sub.shape(), &[4, 5]);
    assert_eq!(sub.ndim(), 2);
    assert_eq!(sub.nelements(), 20);
    assert!(!sub.is_writable()); // SubLattice is read-only

    // 4. Read individual elements and verify values.
    //    sub[0,0] corresponds to lat[2,3] = 2*10+3 = 23
    //    sub[3,4] corresponds to lat[5,7] = 5*10+7 = 57
    let v00 = sub.get_at(&[0, 0]).expect("get_at [0,0]");
    let v34 = sub.get_at(&[3, 4]).expect("get_at [3,4]");
    let v10 = sub.get_at(&[1, 0]).expect("get_at [1,0]");
    let v02 = sub.get_at(&[0, 2]).expect("get_at [0,2]");
    println!("\nElement access:");
    println!("  sub[0,0] = {v00}  (lat[2,3] = 23)");
    println!("  sub[3,4] = {v34}  (lat[5,7] = 57)");
    println!("  sub[1,0] = {v10}  (lat[3,3] = 33)");
    println!("  sub[0,2] = {v02}  (lat[2,5] = 25)");
    assert_eq!(v00, 23.0);
    assert_eq!(v34, 57.0);
    assert_eq!(v10, 33.0);
    assert_eq!(v02, 25.0);

    // 5. Read the full sub-lattice data via get_slice and verify shape/values.
    let full = sub.get_slice(&[0, 0], &[4, 5], &[1, 1]).expect("get_slice");
    println!("\nFull sub-lattice slice (4x5):");
    assert_eq!(full.shape(), &[4, 5]);
    for i in 0..4 {
        let row: Vec<f64> = (0..5).map(|j| full[IxDyn(&[i, j])]).collect();
        let expected_row: Vec<f64> = (0..5).map(|j| ((i + 2) * 10 + (j + 3)) as f64).collect();
        println!("  row {i}: {row:?}");
        assert_eq!(row, expected_row, "row {i} mismatch");
    }

    // 6. Read a smaller sub-slice within the sub-lattice.
    let inner = sub
        .get_slice(&[1, 1], &[2, 2], &[1, 1])
        .expect("inner slice");
    println!("\nInner slice (offset [1,1], shape [2,2]):");
    println!(
        "  [0,0]={}, [0,1]={}, [1,0]={}, [1,1]={}",
        inner[IxDyn(&[0, 0])],
        inner[IxDyn(&[0, 1])],
        inner[IxDyn(&[1, 0])],
        inner[IxDyn(&[1, 1])]
    );
    // sub[1,1] = lat[3,4] = 34, sub[1,2] = lat[3,5] = 35, etc.
    assert_eq!(inner[IxDyn(&[0, 0])], 34.0);
    assert_eq!(inner[IxDyn(&[0, 1])], 35.0);
    assert_eq!(inner[IxDyn(&[1, 0])], 44.0);
    assert_eq!(inner[IxDyn(&[1, 1])], 45.0);

    // 7. Verify the sum of all elements in the sub-lattice.
    let total: f64 = full.iter().sum();
    let expected_total: f64 = (0..4)
        .flat_map(|i| (0..5).map(move |j| ((i + 2) * 10 + (j + 3)) as f64))
        .sum();
    println!("\nSum of sub-lattice elements: {total} (expected {expected_total})");
    assert!((total - expected_total).abs() < 1e-10);

    println!("\nSubLattice demo completed successfully.");
}
