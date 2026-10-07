// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA cell rules of the density grid on a hand-built grid, and the
//! uniform, Briggs and taper weights derived from it.

use casa_imaging_operator::{
    CfKey, DensityCellRule, DensityGridShape, Placement, SampleBuffer, Taper, WeightingGeneration,
    build_density_grid,
};
use num_complex::Complex32;

/// 8×8 cells, `Δx = −1e-3`, `Δy = 1e-3`: `x = 4 − 8e-3·u`, `y = 4 + 8e-3·v`
/// before truncation, so `u = −250` lands at `x = 6` and `v = 125` at `y = 5`.
fn shape(rule: DensityCellRule) -> DensityGridShape {
    DensityGridShape {
        width: 8,
        height: 8,
        planes: 1,
        increment_rad: [-1.0e-3, 1.0e-3],
        rule,
    }
}

fn placement(u: f64, v: f64) -> Placement {
    Placement {
        u,
        v,
        w: 0.0,
        phase: 0.0,
        plane: 0,
        spectral: 0.0,
        cf: CfKey::default(),
        gradient: [0.0, 0.0],
    }
}

fn density_buffer(samples: &[(f64, f64, f32)]) -> SampleBuffer {
    let mut buffer = SampleBuffer::new(1);
    for (u, v, weight) in samples {
        buffer.push(placement(*u, *v), &[Complex32::default()], &[*weight]);
    }
    buffer
}

#[test]
fn standard_cells_truncate_toward_zero_and_add_the_conjugate() {
    // (−250, 125) → (6, 5); (−212.5, 125) → (5.7, 5) → (5, 5); the
    // conjugates land at (2, 3) and (2.3, 3) → (2, 3). (0, 0) → (4, 4) twice.
    // (−500, 0) → x = 8 is outside and so is its conjugate at 0.
    // (−437.5, 0) → x = 7.5 → 7 is inside; its conjugate 0.5 → 0 is not.
    let buffer = density_buffer(&[
        (-250.0, 125.0, 1.0),
        (-212.5, 125.0, 2.0),
        (0.0, 0.0, 1.0),
        (-500.0, 0.0, 1.0),
        (-437.5, 0.0, 1.0),
    ]);
    let grid = build_density_grid(
        std::iter::once(buffer.block()),
        shape(DensityCellRule::Standard),
    );
    let cells = grid.plane(0);
    let at = |x: usize, y: usize| cells[y * 8 + x];
    assert_eq!(at(6, 5), 1.0);
    assert_eq!(at(5, 5), 2.0);
    assert_eq!(at(2, 3), 3.0);
    assert_eq!(at(4, 4), 2.0);
    assert_eq!(at(7, 4), 1.0);
    assert_eq!(at(1, 4), 0.0);
    assert_eq!(cells.iter().sum::<f64>(), 9.0);
    assert_eq!(grid.sum_weights(), &[5.0]);
    assert_eq!(grid.lookup(0, -500.0, 0.0), None);
    assert_eq!(grid.lookup(0, 0.0, 0.0), Some(2.0));
}

#[test]
fn uniform_and_briggs_weights_follow_the_casa_formulae() {
    let buffer = density_buffer(&[
        (-250.0, 125.0, 1.0),
        (-250.0, 125.0, 2.0),
        (-437.5, 0.0, 1.0),
    ]);
    let grid = build_density_grid(
        std::iter::once(buffer.block()),
        shape(DensityCellRule::Standard),
    );
    // Cells: (6,5) = 3, (2,3) = 3, (7,4) = 1 (its conjugate falls on
    // column 0): Σd = 7, Σd² = 19.
    let uniform = WeightingGeneration::density(grid.clone(), None, None).expect("uniform");
    assert!((uniform.imaging_weight(&placement(-250.0, 125.0), 1.5) - 0.5).abs() < 1e-7);
    assert_eq!(uniform.imaging_weight(&placement(-437.5, 0.0), 1.0), 1.0);
    assert_eq!(uniform.imaging_weight(&placement(-500.0, 0.0), 1.0), 0.0);
    assert_eq!(uniform.imaging_weight(&placement(-250.0, 125.0), 0.0), 0.0);
    // Empty cell: uniform gives zero.
    assert_eq!(uniform.imaging_weight(&placement(100.0, 100.0), 1.0), 0.0);

    let briggs = WeightingGeneration::density(grid, Some(0.0), None).expect("briggs");
    let WeightingGeneration::Density { robust, .. } = &briggs else {
        panic!("density weighting");
    };
    let f2 = robust.as_ref().expect("factors").factor(0);
    assert!((f2 - 25.0 / (19.0 / 7.0)).abs() < 1e-12, "f2 = {f2}");
    let expected = 1.0 / (3.0 * f2 + 1.0);
    assert!(
        (f64::from(briggs.imaging_weight(&placement(-250.0, 125.0), 1.0)) - expected).abs() < 1e-7
    );
    // An empty cell under standard Briggs keeps the input weight.
    assert_eq!(briggs.imaging_weight(&placement(100.0, 100.0), 1.0), 1.0);
}

#[test]
fn cube_cells_round_and_mirror_v() {
    // Build: x = round(4 − 8e-3·u + 1) − 1, y = round(4 − 8e-3·v + 1) − 1,
    // away from rounding ties. (−250, 125) → (6, 3), conjugate (2, 5);
    // (−200, 50) → (6.6 → 6, 4.6 → 4), conjugate (3.4 → 2, 5.4 → 4).
    let buffer = density_buffer(&[(-250.0, 125.0, 1.0), (-200.0, 50.0, 1.0)]);
    let grid = build_density_grid(
        std::iter::once(buffer.block()),
        shape(DensityCellRule::Cube),
    );
    let cells = grid.plane(0);
    let at = |x: usize, y: usize| cells[y * 8 + x];
    assert_eq!(at(6, 3), 1.0);
    assert_eq!(at(2, 5), 1.0);
    assert_eq!(at(6, 4), 1.0);
    assert_eq!(at(2, 4), 1.0);
    assert_eq!(cells.iter().sum::<f64>(), 4.0);
    assert_eq!(grid.sum_weights(), &[2.0]);
    // Lookup: x = round(4 − 8e-3·u), y = round(4 − 8e-3·v) in single precision.
    assert_eq!(grid.lookup(0, -250.0, 125.0), Some(1.0));
    assert_eq!(grid.lookup(0, -200.0, 50.0), Some(1.0));
    assert_eq!(grid.lookup(0, 100.0, 100.0), Some(0.0));
    let briggs = WeightingGeneration::density(grid, Some(0.0), None).expect("briggs");
    // Cube Briggs uses 2·Σw for the density sum and zeroes empty cells.
    let WeightingGeneration::Density { robust, .. } = &briggs else {
        panic!("density weighting");
    };
    let f2 = robust.as_ref().expect("factors").factor(0);
    assert!((f2 - 25.0 / (4.0 / 4.0)).abs() < 1e-12, "f2 = {f2}");
    let expected = 1.0 / (f2 + 1.0);
    assert!(
        (f64::from(briggs.imaging_weight(&placement(-250.0, 125.0), 1.0)) - expected).abs() < 1e-7
    );
    assert_eq!(briggs.imaging_weight(&placement(100.0, 100.0), 1.0), 0.0);
}

#[test]
fn per_channel_grids_select_the_placement_plane() {
    let mut buffer = SampleBuffer::new(1);
    let mut on_plane = placement(-250.0, 125.0);
    on_plane.plane = 1;
    buffer.push(on_plane, &[Complex32::default()], &[1.0]);
    let mut off_grid = placement(-250.0, 125.0);
    off_grid.plane = 2;
    buffer.push(off_grid, &[Complex32::default()], &[1.0]);
    let mut shape = shape(DensityCellRule::Standard);
    shape.planes = 2;
    let grid = build_density_grid(std::iter::once(buffer.block()), shape);
    assert_eq!(grid.plane(0).iter().sum::<f64>(), 0.0);
    assert_eq!(grid.plane(1).iter().sum::<f64>(), 2.0);
    let uniform = WeightingGeneration::density(grid, None, None).expect("uniform");
    assert_eq!(uniform.imaging_weight(&on_plane, 1.0), 1.0);
    assert_eq!(uniform.imaging_weight(&off_grid, 1.0), 0.0);
}

#[test]
fn natural_weighting_applies_the_gaussian_taper() {
    let natural = WeightingGeneration::Natural { taper: None };
    assert_eq!(natural.imaging_weight(&placement(10.0, 20.0), 3.0), 3.0);
    let tapered = WeightingGeneration::Natural {
        taper: Some(Taper::new(100.0, 50.0, 0.0)),
    };
    // Position angle 0: rotated_u = v, rotated_v = −u.
    let weight = f64::from(tapered.imaging_weight(&placement(50.0, 100.0), 1.0));
    let expected = (-std::f64::consts::LN_2 * (100.0_f64 / 100.0).powi(2)
        - std::f64::consts::LN_2 * (50.0_f64 / 50.0).powi(2))
    .exp();
    assert!((weight - expected).abs() < 1e-6, "{weight} vs {expected}");
}
