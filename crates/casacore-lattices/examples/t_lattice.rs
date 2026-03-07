// SPDX-License-Identifier: LGPL-3.0-or-later
//! Demonstration of the lattice API, mirroring C++ `tArrayLattice`,
//! `tPagedArray`, `tLatticeIterator`, `tTempLattice`, and `tSubLattice`.
//!
//! Each section creates, populates, reads, and verifies a different lattice
//! type or feature. Run with:
//!
//! ```sh
//! cargo run --example t_lattice -p casacore-lattices
//! ```

use casacore_lattices::{
    ArrayLattice, LCBox, LCComplement, LCDifference, LCEllipsoid, LCIntersection, LCRegion,
    LCUnion, Lattice, LatticeIter, LatticeIterExt, LatticeIterMut, LatticeMut, LatticeStepper,
    PagedArray, SubLattice, SubLatticeMut, TempLattice, TileStepper, TiledLineStepper, TiledShape,
};
use ndarray::{ArrayD, IxDyn};

fn main() {
    array_lattice_demo();
    paged_array_demo();
    iteration_demo();
    temp_lattice_demo();
    regions_and_sublattice_demo();

    println!("\nAll lattice demos completed successfully.");
}

// -----------------------------------------------------------------------
// 1. ArrayLattice
// -----------------------------------------------------------------------

fn array_lattice_demo() {
    println!("=== ArrayLattice ===");

    // Create a 4×6×3 lattice filled with a ramp.
    let data = ArrayD::from_shape_fn(IxDyn(&[4, 6, 3]), |idx| {
        (idx[0] * 18 + idx[1] * 3 + idx[2]) as f64
    });
    let lat = ArrayLattice::new(data);
    println!(
        "Created 3D ArrayLattice: shape={:?}, ndim={}, nelements={}",
        lat.shape(),
        lat.ndim(),
        lat.nelements()
    );
    assert!(!lat.is_persistent());
    assert!(!lat.is_paged());
    assert!(lat.is_writable());

    // Read a single element.
    let val = lat.get_at(&[1, 2, 1]).unwrap();
    println!("  get_at([1,2,1]) = {val}");
    assert_eq!(val, (18 + 2 * 3 + 1) as f64);

    // Read a slice.
    let slice = lat.get_slice(&[0, 0, 0], &[2, 3, 2], &[1, 1, 1]).unwrap();
    assert_eq!(slice.shape(), &[2, 3, 2]);
    println!("  get_slice([0,0,0], [2,3,2]) -> shape={:?}", slice.shape());

    // Write and verify with a mutable lattice.
    let mut lat = ArrayLattice::<f64>::zeros(vec![4, 4]);
    lat.set(99.0).unwrap();
    assert_eq!(lat.get_at(&[3, 3]).unwrap(), 99.0);

    let patch = ArrayD::from_elem(IxDyn(&[2, 2]), 1.0);
    lat.put_slice(&patch, &[1, 1]).unwrap();
    assert_eq!(lat.get_at(&[1, 1]).unwrap(), 1.0);
    assert_eq!(lat.get_at(&[0, 0]).unwrap(), 99.0);
    println!("  put_slice + set verified");

    // Apply transform.
    lat.apply(|&v| v * 2.0).unwrap();
    assert_eq!(lat.get_at(&[1, 1]).unwrap(), 2.0);
    println!("  apply(x * 2) verified");
}

// -----------------------------------------------------------------------
// 2. PagedArray
// -----------------------------------------------------------------------

fn paged_array_demo() {
    println!("\n=== PagedArray ===");

    let dir = tempfile::tempdir().expect("create temp dir");
    let path = dir.path().join("demo_paged.table");

    // Create on disk.
    let ts = TiledShape::new(vec![16, 16]);
    let mut pa = PagedArray::<f64>::create(ts, &path).unwrap();
    println!(
        "Created PagedArray: shape={:?}, tile_shape={:?}",
        pa.shape(),
        pa.tile_shape()
    );
    assert!(pa.is_persistent());
    assert!(pa.is_paged());

    // Fill with ramp.
    let data = ArrayD::from_shape_fn(IxDyn(&[16, 16]), |idx| (idx[0] * 16 + idx[1]) as f64);
    pa.put_slice(&data, &[0, 0]).unwrap();
    pa.flush().unwrap();
    println!("  Wrote ramp data and flushed");

    // Reopen and verify.
    let pa2 = PagedArray::<f64>::open(&path).unwrap();
    assert_eq!(pa2.shape(), &[16, 16]);
    assert_eq!(pa2.get_at(&[0, 0]).unwrap(), 0.0);
    assert_eq!(pa2.get_at(&[15, 15]).unwrap(), 255.0);
    println!("  Reopened and verified: [0,0]=0, [15,15]=255");

    // Scratch (non-persistent) array.
    let ts = TiledShape::new(vec![8, 8]);
    let mut scratch = PagedArray::<i32>::new_scratch(ts).unwrap();
    scratch.set(42).unwrap();
    assert!(!scratch.is_persistent());
    assert_eq!(scratch.get_at(&[0, 0]).unwrap(), 42);
    println!("  Scratch PagedArray verified (non-persistent)");
}

// -----------------------------------------------------------------------
// 3. Iteration
// -----------------------------------------------------------------------

fn iteration_demo() {
    println!("\n=== Iteration ===");

    let data = ArrayD::from_shape_fn(IxDyn(&[12, 8]), |idx| (idx[0] * 8 + idx[1]) as f64);
    let lat = ArrayLattice::new(data.clone());
    let total: f64 = data.iter().sum();

    // LatticeStepper + LatticeIter
    let stepper = LatticeStepper::new(vec![12, 8], vec![4, 4], None);
    let chunks: Vec<_> = LatticeIter::new(&lat, stepper).collect();
    let chunk_sum: f64 = chunks.iter().flat_map(|c| c.iter()).sum();
    assert_eq!(chunks.len(), 6); // 3×2 chunks
    assert_eq!(chunk_sum, total);
    println!(
        "  LatticeStepper: {} chunks (4x4), sum={:.0} (matches total)",
        chunks.len(),
        chunk_sum
    );

    // TiledLineStepper + iter_lines (convenience)
    let line_sum: f64 = lat.iter_lines(0).flat_map(|l| l.into_iter()).sum();
    assert_eq!(line_sum, total);
    println!("  iter_lines(axis=0): sum matches total");

    // TileStepper + iter_tiles (convenience)
    let tile_sum: f64 = lat.iter_tiles().flat_map(|t| t.into_iter()).sum();
    assert_eq!(tile_sum, total);
    println!("  iter_tiles: sum matches total");

    // iter_chunks (convenience)
    let chunk_sum: f64 = lat
        .iter_chunks(vec![6, 4])
        .flat_map(|c| c.into_iter())
        .sum();
    assert_eq!(chunk_sum, total);
    println!("  iter_chunks([6,4]): sum matches total");

    // Explicit TiledLineStepper
    let tls = TiledLineStepper::new(vec![12, 8], vec![4, 4], 1);
    let line_count = LatticeIter::new(&lat, tls).count();
    println!("  TiledLineStepper(axis=1): {line_count} lines");

    // Explicit TileStepper
    let ts = TileStepper::new(vec![12, 8], vec![4, 4]);
    let tile_count = LatticeIter::new(&lat, ts).count();
    println!("  TileStepper(4x4): {tile_count} tiles");

    // Mutable iteration: scale all values by 0.5
    let mut lat_mut = ArrayLattice::new(data);
    let stepper = LatticeStepper::new(vec![12, 8], vec![6, 4], None);
    let mut iter = LatticeIterMut::new(&mut lat_mut, stepper);
    while let Some(mut chunk) = iter.next_chunk() {
        chunk.data.mapv_inplace(|v| v * 0.5);
        chunk.write_back(&mut iter).unwrap();
    }
    let result = iter.lattice().get().unwrap();
    assert_eq!(result[IxDyn(&[0, 0])], 0.0);
    assert!((result[IxDyn(&[11, 7])] - 47.5).abs() < 0.01);
    println!("  LatticeIterMut: scaled all values by 0.5");
}

// -----------------------------------------------------------------------
// 4. TempLattice
// -----------------------------------------------------------------------

fn temp_lattice_demo() {
    println!("\n=== TempLattice ===");

    // Small: in-memory.
    let mut small = TempLattice::<f64>::new(vec![4, 4], None).unwrap();
    assert!(small.is_in_memory());
    assert!(!small.is_paged());
    small.set(3.125).unwrap();
    assert_eq!(small.get_at(&[2, 2]).unwrap(), 3.125);
    println!(
        "  Small (in-memory): shape={:?}, value at [2,2]={:.3}",
        small.shape(),
        small.get_at(&[2, 2]).unwrap()
    );

    // Large: paged (force with low threshold).
    let mut large = TempLattice::<f64>::new(vec![10, 10], Some(10)).unwrap();
    assert!(!large.is_in_memory());
    assert!(large.is_paged());
    large.set(2.5).unwrap();
    assert_eq!(large.get_at(&[5, 5]).unwrap(), 2.5);
    println!(
        "  Large (paged): shape={:?}, value at [5,5]={:.1}",
        large.shape(),
        large.get_at(&[5, 5]).unwrap()
    );

    // temp_close / reopen cycle.
    large.temp_close().unwrap();
    assert!(large.is_temp_closed());
    println!("  temp_close(): is_temp_closed={}", large.is_temp_closed());

    large.reopen().unwrap();
    assert!(!large.is_temp_closed());
    assert_eq!(large.get_at(&[0, 0]).unwrap(), 2.5);
    println!("  reopen(): data preserved, value at [0,0]={:.1}", 2.5);

    // Auto-reopen on read.
    large.temp_close().unwrap();
    let val = large.get_at(&[9, 9]).unwrap(); // triggers auto-reopen
    assert_eq!(val, 2.5);
    assert!(!large.is_temp_closed());
    println!("  Auto-reopen on read: value at [9,9]={val:.1}");
}

// -----------------------------------------------------------------------
// 5. Regions & SubLattice
// -----------------------------------------------------------------------

fn regions_and_sublattice_demo() {
    println!("\n=== Regions & SubLattice ===");

    let lattice_shape = vec![10, 10];

    // LCBox
    let box_region = LCBox::new(vec![2, 2], vec![5, 5], lattice_shape.clone());
    assert!(box_region.contains(&[3, 3]));
    assert!(!box_region.contains(&[0, 0]));
    assert_eq!(box_region.bounding_box_shape(), vec![4, 4]);
    println!(
        "  LCBox([2,2]-[5,5]): bb_shape={:?}, contains([3,3])={}, contains([0,0])={}",
        box_region.bounding_box_shape(),
        box_region.contains(&[3, 3]),
        box_region.contains(&[0, 0])
    );

    // LCEllipsoid
    let ellipse = LCEllipsoid::new(vec![5.0, 5.0], vec![3.0, 3.0], lattice_shape.clone());
    assert!(ellipse.contains(&[5, 5])); // center
    assert!(ellipse.contains(&[3, 5])); // within semi-axis
    assert!(!ellipse.contains(&[0, 0])); // outside
    println!(
        "  LCEllipsoid(center=[5,5], r=[3,3]): contains([5,5])={}, contains([0,0])={}",
        ellipse.contains(&[5, 5]),
        ellipse.contains(&[0, 0])
    );

    // Set algebra: complement
    let complement = LCComplement::new(Box::new(box_region.clone()));
    assert!(!complement.contains(&[3, 3]));
    assert!(complement.contains(&[0, 0]));
    println!("  LCComplement: !box -> contains([0,0])=true");

    // Set algebra: union
    let box2 = LCBox::new(vec![7, 7], vec![9, 9], lattice_shape.clone());
    let union = LCUnion::new(Box::new(box_region.clone()), Box::new(box2.clone()));
    assert!(union.contains(&[3, 3])); // in box1
    assert!(union.contains(&[8, 8])); // in box2
    assert!(!union.contains(&[6, 6])); // in neither
    println!("  LCUnion(box1, box2): covers both regions");

    // Set algebra: intersection
    let big_box = LCBox::new(vec![3, 3], vec![8, 8], lattice_shape.clone());
    let intersection = LCIntersection::new(Box::new(box_region.clone()), Box::new(big_box.clone()));
    assert!(intersection.contains(&[4, 4])); // in both
    assert!(!intersection.contains(&[2, 2])); // in box_region but not big_box
    println!("  LCIntersection: overlap region verified");

    // Set algebra: difference
    let difference = LCDifference::new(Box::new(big_box), Box::new(box_region.clone()));
    assert!(difference.contains(&[6, 6])); // in big_box but not box_region
    assert!(!difference.contains(&[4, 4])); // in both (excluded)
    println!("  LCDifference: A - B verified");

    // SubLattice (read-only view)
    let data = ArrayD::from_shape_fn(IxDyn(&[10, 10]), |idx| (idx[0] * 10 + idx[1]) as f64);
    let lat = ArrayLattice::new(data);
    let sub = SubLattice::new(&lat, Box::new(box_region.clone()));
    assert_eq!(sub.shape(), &[4, 4]);
    assert_eq!(sub.get_at(&[0, 0]).unwrap(), 22.0); // lat[2,2]
    assert_eq!(sub.get_at(&[3, 3]).unwrap(), 55.0); // lat[5,5]
    assert!(!sub.is_writable());
    println!(
        "  SubLattice: shape={:?}, [0,0]={} (=lat[2,2]), [3,3]={} (=lat[5,5])",
        sub.shape(),
        sub.get_at(&[0, 0]).unwrap(),
        sub.get_at(&[3, 3]).unwrap()
    );

    // SubLatticeMut (mutable view)
    let mut lat_mut = ArrayLattice::<f64>::zeros(vec![10, 10]);
    lat_mut.set(1.0).unwrap();
    {
        let mut sub_mut = SubLatticeMut::new(&mut lat_mut, Box::new(box_region));
        sub_mut.set(0.0).unwrap();
    }
    // Pixels inside the box should be 0, outside should be 1.
    assert_eq!(lat_mut.get_at(&[3, 3]).unwrap(), 0.0);
    assert_eq!(lat_mut.get_at(&[0, 0]).unwrap(), 1.0);
    println!("  SubLatticeMut: set region to 0, outside unchanged");
}
