// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA cell rules of the density grid on a hand-built grid, and the
//! uniform, Briggs and taper weights derived from it.

use casa_imaging_operator::{
    BandwidthTaper, CfKey, DensityCellRule, DensityGridShape, DensityUv, OperatorError, Placement,
    SampleBuffer, Taper, WeightingGeneration, build_density_grid,
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

/// Density coordinates equal to the placement's (exact in single precision).
fn uv(u: f64, v: f64) -> DensityUv {
    DensityUv {
        u: u as f32,
        v: v as f32,
    }
}

/// The imaging weight of a placement at `(u, v)` looked up at the same point.
fn weight(generation: &WeightingGeneration, u: f64, v: f64, input: f32) -> f32 {
    generation.imaging_weight(&placement(u, v), uv(u, v), input)
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
    assert_eq!(grid.lookup(0, uv(-500.0, 0.0)), None);
    assert_eq!(grid.lookup(0, uv(0.0, 0.0)), Some(2.0));
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
    let uniform = WeightingGeneration::density(grid.clone(), None, None, None).expect("uniform");
    assert!((weight(&uniform, -250.0, 125.0, 1.5) - 0.5).abs() < 1e-7);
    assert_eq!(weight(&uniform, -437.5, 0.0, 1.0), 1.0);
    assert_eq!(weight(&uniform, -500.0, 0.0, 1.0), 0.0);
    assert_eq!(weight(&uniform, -250.0, 125.0, 0.0), 0.0);
    // Empty cell: uniform gives zero.
    assert_eq!(weight(&uniform, 100.0, 100.0, 1.0), 0.0);

    let briggs = WeightingGeneration::density(grid, Some(0.0), None, None).expect("briggs");
    let WeightingGeneration::Density { robust, .. } = &briggs else {
        panic!("density weighting");
    };
    let f2 = robust.as_ref().expect("factors").factor(0);
    assert!((f2 - 25.0 / (19.0 / 7.0)).abs() < 1e-12, "f2 = {f2}");
    let expected = 1.0 / (3.0 * f2 + 1.0);
    assert!((f64::from(weight(&briggs, -250.0, 125.0, 1.0)) - expected).abs() < 1e-7);
    // An empty cell under standard Briggs keeps the input weight.
    assert_eq!(weight(&briggs, 100.0, 100.0, 1.0), 1.0);
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
    assert_eq!(grid.lookup(0, uv(-250.0, 125.0)), Some(1.0));
    assert_eq!(grid.lookup(0, uv(-200.0, 50.0)), Some(1.0));
    assert_eq!(grid.lookup(0, uv(100.0, 100.0)), Some(0.0));
    let briggs = WeightingGeneration::density(grid, Some(0.0), None, None).expect("briggs");
    // Cube Briggs uses 2·Σw for the density sum and zeroes empty cells.
    let WeightingGeneration::Density { robust, .. } = &briggs else {
        panic!("density weighting");
    };
    let f2 = robust.as_ref().expect("factors").factor(0);
    assert!((f2 - 25.0 / (4.0 / 4.0)).abs() < 1e-12, "f2 = {f2}");
    let expected = 1.0 / (f2 + 1.0);
    assert!((f64::from(weight(&briggs, -250.0, 125.0, 1.0)) - expected).abs() < 1e-7);
    assert_eq!(weight(&briggs, 100.0, 100.0, 1.0), 0.0);
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
    let uniform = WeightingGeneration::density(grid, None, None, None).expect("uniform");
    assert_eq!(
        uniform.imaging_weight(&on_plane, uv(-250.0, 125.0), 1.0),
        1.0
    );
    assert_eq!(
        uniform.imaging_weight(&off_grid, uv(-250.0, 125.0), 1.0),
        0.0
    );
}

#[test]
fn natural_weighting_applies_the_gaussian_taper() {
    let natural = WeightingGeneration::Natural { taper: None };
    assert_eq!(weight(&natural, 10.0, 20.0, 3.0), 3.0);
    let tapered = WeightingGeneration::Natural {
        taper: Some(Taper::new(100.0, 50.0, 0.0)),
    };
    // Position angle 0: rotated_u = v, rotated_v = −u.
    let tapered = f64::from(weight(&tapered, 50.0, 100.0, 1.0));
    let expected = (-std::f64::consts::LN_2 * (100.0_f64 / 100.0).powi(2)
        - std::f64::consts::LN_2 * (50.0_f64 / 50.0).powi(2))
    .exp();
    assert!((tapered - expected).abs() < 1e-6, "{tapered} vs {expected}");
}

#[test]
fn briggs_bandwidth_taper_divides_the_density_term_by_the_uv_distance_factor() {
    let buffer = density_buffer(&[
        (-250.0, 125.0, 1.0),
        (-250.0, 125.0, 2.0),
        (-437.5, 0.0, 1.0),
    ]);
    let grid = build_density_grid(
        std::iter::once(buffer.block()),
        shape(DensityCellRule::Standard),
    );
    // CASA: fracBW = 2(ν_last − ν_first)/(ν_last + ν_first) from the image axis.
    let bandwidth = BandwidthTaper::from_frequency_range(1.0e9, 1.1e9).expect("taper");
    let fractional = 0.2e9 / 2.1e9;
    assert!((bandwidth.fractional_bandwidth() - fractional).abs() < 1e-15);
    let f2 = 25.0 / (19.0 / 7.0);
    let tapered = WeightingGeneration::density(grid.clone(), Some(0.0), Some(bandwidth), None)
        .expect("briggsbwtaper");
    // (−250, 125) lies √5 cells from the origin: n = fracBW·√5 < 1, so the
    // factor takes CASA's small-distance branch (4 − n)/(4 − 2n).
    let cells = fractional * 5.0_f64.sqrt();
    let factor = (4.0 - cells) / (4.0 - 2.0 * cells);
    assert!((factor - 1.059_585).abs() < 1e-6, "factor {factor}");
    let expected = 1.0 / (3.0 * f2 / factor + 1.0);
    let actual = f64::from(weight(&tapered, -250.0, 125.0, 1.0));
    assert!((actual - expected).abs() < 1e-7, "{actual} vs {expected}");
    // Once n reaches 1 the factor is n + 0.5: with unit fractional bandwidth
    // (1 to 3 GHz) the sample at (−437.5, 0), 3.5 cells out, gets 4.0.
    let wide = BandwidthTaper::from_frequency_range(1.0e9, 3.0e9).expect("taper");
    assert_eq!(wide.fractional_bandwidth(), 1.0);
    let tapered = WeightingGeneration::density(grid.clone(), Some(0.0), Some(wide), None)
        .expect("briggsbwtaper");
    let expected = 1.0 / (f2 / 4.0 + 1.0);
    let actual = f64::from(weight(&tapered, -437.5, 0.0, 1.0));
    assert!((actual - expected).abs() < 1e-7, "{actual} vs {expected}");
    assert!(matches!(
        WeightingGeneration::density(grid, None, Some(bandwidth), None),
        Err(OperatorError::Weighting { .. })
    ));
    assert!(matches!(
        BandwidthTaper::from_frequency_range(1.0e9, 1.0e9),
        Err(OperatorError::Weighting { .. })
    ));
}
