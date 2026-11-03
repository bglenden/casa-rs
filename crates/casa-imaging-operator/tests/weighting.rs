// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA cell rules of the density grid on a hand-built grid, and the
//! uniform, superuniform, Briggs (normalized and absolute), radial and taper
//! weights derived from it.

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
        padding: 0,
        box_half_width: 0,
        increment_rad: [-1.0e-3, 1.0e-3],
        rule,
        fields: None,
    }
}

fn placement(u: f64, v: f64) -> Placement {
    Placement {
        u,
        v,
        w: 0.0,
        prediction_w_positive: false,
        phase: 0.0,
        plane: 0,
        spectral: 0.0,
        cf: CfKey::default(),
        gradient: [0.0, 0.0],
    }
}

/// Density coordinates equal to the placement's (exact in single precision),
/// as metres at one wavelength per metre.
fn uv(u: f64, v: f64) -> DensityUv {
    DensityUv {
        u: u as f32,
        v: v as f32,
        wavelengths_per_m: 1.0,
        uv_m: [u, v],
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
    // An empty cell weighs nothing under Briggs too
    // (`VisImagingWeight::weightUniform` divides only when `a_gwt > 0`).
    assert_eq!(weight(&briggs, 100.0, 100.0, 1.0), 0.0);
}

/// CASA's cell origin is the integer `n/2` (`uorigin_p = nx/2`, `GridFT`'s
/// `offset_p` likewise): on a 7 × 7 grid it is cell 3, not 3.5. With
/// `Δx = −1e-3` the scale is `−7e-3`, so the sample at `u = 100` sits at
/// `x = 3 ∓ 0.7` with its conjugate:
/// - standard (truncate): 2.3 → 2 and 3.7 → 3, where a half-cell origin
///   gives 2.8 → 2 and 4.2 → 4;
/// - cube build (`nint(x + 1) − 1`, `u` negated): 4.7 → 4 and 3.3 → 2, where
///   a half-cell origin gives 3.8 → 3; the cube lookup rounds 3.7 → 4.
#[test]
fn odd_sizes_put_the_density_origin_on_the_lower_middle_cell() {
    let odd = |rule| DensityGridShape {
        width: 7,
        height: 7,
        planes: 1,
        padding: 0,
        box_half_width: 0,
        increment_rad: [-1.0e-3, 1.0e-3],
        rule,
        fields: None,
    };
    // (rule, density at columns 2, 3 and 4 of row 3)
    for (rule, expected) in [
        (DensityCellRule::Standard, [2.0, 1.0 + 1.0 + 2.0, 0.0]),
        (DensityCellRule::Cube, [2.0, 1.0 + 1.0, 2.0]),
    ] {
        let buffer = density_buffer(&[(0.0, 0.0, 1.0), (100.0, 0.0, 2.0)]);
        let grid = build_density_grid(std::iter::once(buffer.block()), odd(rule));
        let row = &grid.plane(0)[3 * 7..4 * 7];
        assert_eq!(row[2..5], expected, "{rule:?}");
        assert_eq!(grid.lookup(0, uv(100.0, 0.0)), Some(2.0), "{rule:?}");
        assert_eq!(grid.lookup(0, uv(0.0, 0.0)), Some(expected[1]), "{rule:?}");
    }
}

#[test]
fn cube_cells_round_the_negated_uv() {
    // CASA's cube weightor negates u and v (`FTMachine::negateUV` for the
    // build, `u = −uvw·f` for the lookup). Build: x = round(4 + 8e-3·u + 1) − 1,
    // y = round(4 − 8e-3·v + 1) − 1, away from rounding ties. (−250, 125) →
    // (2, 3), conjugate (6, 5); (−200, 50) → (3.4 → 2, 4.6 → 4), conjugate
    // (6.6 → 6, 5.4 → 4).
    let buffer = density_buffer(&[(-250.0, 125.0, 1.0), (-200.0, 50.0, 1.0)]);
    let grid = build_density_grid(
        std::iter::once(buffer.block()),
        shape(DensityCellRule::Cube),
    );
    let cells = grid.plane(0);
    let at = |x: usize, y: usize| cells[y * 8 + x];
    assert_eq!(at(2, 3), 1.0);
    assert_eq!(at(6, 5), 1.0);
    assert_eq!(at(2, 4), 1.0);
    assert_eq!(at(6, 4), 1.0);
    assert_eq!(cells.iter().sum::<f64>(), 4.0);
    assert_eq!(grid.sum_weights(), &[2.0]);
    // Lookup: x = round(4 + 8e-3·u), y = round(4 − 8e-3·v) in single precision.
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

/// A sample of `refim_point.ms` (row 32452, channel 4) whose `Float` cell
/// coordinate is exactly 51.5 in CASA's orientation: CASA 6.7.6.14's cube
/// weightor rounds it half away from zero to cell 52 and weighs it from
/// that cell's density. A lookup in the mirrored orientation computes
/// 48.5, rounds to 49 and reads the cell CASA calls 51 (#980).
#[test]
fn a_float_tie_rounds_in_casas_orientation() {
    let cell_rad = 8.0 / 206_264.806_247_096_36;
    let shape = DensityGridShape {
        width: 100,
        height: 100,
        planes: 1,
        padding: 0,
        box_half_width: 0,
        increment_rad: [-cell_rad, cell_rad],
        rule: DensityCellRule::Cube,
        fields: None,
    };
    let tie = DensityUv::casa(
        [96.620_773_734_500_32, -870.029_305_110_418_4, 0.0],
        1_199_986_794.390_933,
    );
    let scale_x = (100.0 * -cell_rad) as f32;
    assert_eq!(
        scale_x * -tie.u + 50.0,
        51.5,
        "the sample is an exact Float tie"
    );
    // Cells 52 and 51 of row 64, built from double coordinates far from
    // their own rounding ties: x = round(u·100·Δ + 51) − 1, y = round(14 + 51) − 1.
    let per_cell = 1.0 / (100.0 * cell_rad);
    let row_v = -14.0 * per_cell;
    let buffer = density_buffer(&[(2.0 * per_cell, row_v, 1.0), (per_cell, row_v, 2.0)]);
    let grid = build_density_grid(std::iter::once(buffer.block()), shape);
    assert_eq!(grid.plane(0)[64 * 100 + 52], 1.0);
    assert_eq!(grid.plane(0)[64 * 100 + 51], 2.0);
    assert_eq!(grid.lookup(0, tie), Some(1.0), "CASA reads cell 52");
}

/// On an odd width CASA accepts cells `0 < x < 7` about origin 3, which is
/// not mirror-symmetric: a sample at `u = 430` lands at `x = round(3 +
/// 7e-3·430) = 6` in CASA's orientation, an accepted cell, where the mirrored
/// orientation computes `x = 0` and drops it (Astra's review of #1043).
#[test]
fn an_odd_width_keeps_casas_outermost_column() {
    let odd = DensityGridShape {
        width: 7,
        height: 7,
        planes: 1,
        padding: 0,
        box_half_width: 0,
        increment_rad: [-1.0e-3, 1.0e-3],
        rule: DensityCellRule::Cube,
        fields: None,
    };
    let buffer = density_buffer(&[(430.0, 0.0, 1.0)]);
    let grid = build_density_grid(std::iter::once(buffer.block()), odd);
    assert_eq!(grid.plane(0)[3 * 7 + 6], 1.0, "the sample's own cell");
    assert_eq!(grid.lookup(0, uv(430.0, 0.0)), Some(1.0));
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

/// CASA `superuniform` (`VisImagingWeight`'s constructor with
/// `uBox = vBox = b`): each sample adds its whole weight to the
/// `(2b + 1)²` cells about its cell, and about its conjugate cell, only when
/// that whole box is inside the grid, `(ucell − uBox) > 0 && (ucell + uBox)
/// < nx` and likewise in v; `weightUniform` then looks up the sample's own
/// cell without a box.
#[test]
fn a_superuniform_box_adds_the_whole_weight_to_every_cell_of_a_box_inside_the_grid() {
    let mut shape = shape(DensityCellRule::Standard);
    shape.box_half_width = 1;
    // A = (−250, 125) → (6, 5), conjugate (2, 3); B = (−125, 0) → (5, 4),
    // conjugate (3, 4); all four boxes lie in 1..=7. C = (−375, 0) →
    // (7, 4): its box reaches column 8, and its conjugate's, about (1, 4),
    // column 0, so C adds nothing (one cell each would have been accepted).
    let buffer = density_buffer(&[(-250.0, 125.0, 1.0), (-125.0, 0.0, 2.0), (-375.0, 0.0, 4.0)]);
    let grid = build_density_grid(std::iter::once(buffer.block()), shape);
    let cells = grid.plane(0);
    let at = |x: usize, y: usize| cells[y * 8 + x];
    // Boxes: A x 5..=7, y 4..=6; A* x 1..=3, y 2..=4; B x 4..=6, y 3..=5;
    // B* x 2..=4, y 3..=5.
    assert_eq!(at(6, 5), 1.0 + 2.0, "A's cell, inside B's box");
    assert_eq!(at(5, 4), 1.0 + 2.0);
    assert_eq!(at(7, 6), 1.0, "A's box alone");
    assert_eq!(at(3, 4), 1.0 + 2.0, "A* and B*");
    assert_eq!(at(1, 2), 1.0, "A*'s box alone");
    assert_eq!(at(4, 3), 2.0 + 2.0, "B's and B*'s boxes meet");
    assert_eq!(at(7, 4), 1.0, "C's own cell holds A's box only");
    assert_eq!(cells.iter().sum::<f64>(), 9.0 * 2.0 * (1.0 + 2.0));
    assert_eq!(grid.sum_weights(), &[3.0]);
    let superuniform = WeightingGeneration::density(grid, None, None, None).expect("uniform");
    assert!((weight(&superuniform, -250.0, 125.0, 1.5) - 0.5).abs() < 1e-7);
    // C weighs by the density at its own cell, though its box added none.
    assert_eq!(weight(&superuniform, -375.0, 0.0, 4.0), 4.0);
}

/// A cube's per-channel density with `npixels = b` (`BriggsCubeWeightor`'s
/// `superUniformBox`): `GridFT` grids each sample through a convolution
/// function of ones over the box, which `sectggridd` normalizes to unit sum,
/// so each of the `(2b + 1)²` cells takes `w/(2b + 1)²`, and the weight sum
/// (`sumWgts`) takes `w` once.
#[test]
fn a_cube_density_box_shares_the_weight_among_its_cells() {
    let mut shape = shape(DensityCellRule::Cube);
    shape.box_half_width = 1;
    // (−250, 125) → (2, 3), conjugate (6, 5) (the cube rule negates u and
    // v); both boxes lie in 1..=7. (375, 0) → (7, 4) leaves the grid, and
    // CASA folds no conjugate from a sample it did not grid.
    let buffer = density_buffer(&[(-250.0, 125.0, 9.0), (375.0, 0.0, 9.0)]);
    let grid = build_density_grid(std::iter::once(buffer.block()), shape);
    let cells = grid.plane(0);
    let at = |x: usize, y: usize| cells[y * 8 + x];
    for (x, y) in [(1, 2), (2, 3), (3, 4), (5, 4), (6, 5), (7, 6)] {
        assert_eq!(at(x, y), 1.0, "({x}, {y})");
    }
    assert_eq!(at(4, 4), 0.0);
    assert_eq!(cells.iter().sum::<f64>(), 18.0);
    assert_eq!(grid.sum_weights(), &[9.0]);
    // f² = 25 / (ΣD² / 2Σw) = 25 / (18 / 18).
    let briggs = WeightingGeneration::density(grid, Some(0.0), None, None).expect("briggs");
    assert!((f64::from(weight(&briggs, -250.0, 125.0, 1.0)) - 1.0 / 26.0).abs() < 1e-7);
}

/// CASA's continuum uniform and Briggs density with `npixels = n`
/// (`SynthesisImagerVi2::weight`): an `n × n` grid whose cell increment is
/// the field of view `n·Δ` divided by `n` again, so its uv cells are
/// `N / n` times the image's; on 4 and 5 cells two samples in different
/// image cells share one.
#[test]
fn a_square_npixels_grid_spans_the_image_uv_extent_with_coarser_cells() {
    // In double, (3·0.1)/3 is 0.10000000000000002, not 0.1.
    assert_eq!(
        DensityGridShape::visimagingweight_increment(3, 0.1),
        0.100_000_000_000_000_02
    );
    let samples = [(-150.0, 150.0, 1.0), (-100.0, 50.0, 1.0)];
    let image = build_density_grid(
        std::iter::once(density_buffer(&samples).block()),
        shape(DensityCellRule::Standard),
    );
    // The 8 × 8 image grid: (−150, 150) → (5.2, 5.2) → (5, 5) and
    // (−100, 50) → (4.8, 4.4) → (4, 4).
    assert_eq!(image.lookup(0, uv(-150.0, 150.0)), Some(1.0));
    assert_eq!(image.lookup(0, uv(-100.0, 50.0)), Some(1.0));
    // n = 4: origin 2, scale ∓4e-3: (2.6, 2.6) and (2.4, 2.2) → (2, 2);
    // conjugates (1.4, 1.4) and (1.6, 1.8) → (1, 1). n = 5, odd: origin
    // 5/2 = 2, scale ∓5e-3: (2.75, 2.75) and (2.5, 2.25) → (2, 2);
    // conjugates (1.25, 1.25) and (1.5, 1.75) → (1, 1).
    for cells in [4, 5] {
        let square = DensityGridShape {
            width: cells,
            height: cells,
            increment_rad: [-1.0e-3, 1.0e-3]
                .map(|increment| DensityGridShape::visimagingweight_increment(cells, increment)),
            ..shape(DensityCellRule::Standard)
        };
        assert_eq!(square.bytes(), ((cells * cells + 1) * 8) as u64);
        let grid = build_density_grid(std::iter::once(density_buffer(&samples).block()), square);
        let plane = grid.plane(0);
        assert_eq!(plane[2 * cells + 2], 2.0, "{cells} cells");
        assert_eq!(plane[cells + 1], 2.0, "{cells} cells");
        assert_eq!(plane.iter().sum::<f64>(), 4.0, "{cells} cells");
        let uniform = WeightingGeneration::density(grid, None, None, None).expect("uniform");
        assert_eq!(weight(&uniform, -150.0, 150.0, 1.0), 0.5, "{cells} cells");
    }
}

/// CASA `briggsabs` (`rmode = "abs"`): `f² = robust²` and `d² = 2·noise²`,
/// each a `Float` (`VisImagingWeight`'s `f2_p`, `d2_p`), on every plane;
/// `weightUniform` divides by `D·f² + d²` where the cell is non-empty.
#[test]
fn absolute_briggs_weights_divide_by_robust_squared_density_plus_twice_the_noise_squared() {
    let buffer = density_buffer(&[
        (-250.0, 125.0, 1.0),
        (-250.0, 125.0, 2.0),
        (-437.5, 0.0, 1.0),
    ]);
    // Cells (6, 5) = 3, (2, 3) = 3, (7, 4) = 1.
    let grid = build_density_grid(
        std::iter::once(buffer.block()),
        shape(DensityCellRule::Standard),
    );
    let factors = grid.absolute_factors(0.3, 0.1);
    assert_eq!(factors.factor(0), f64::from(0.09_f32));
    assert_eq!(factors.factor(0), 0.090_000_003_576_278_69);
    assert_eq!(factors.offset(), 0.019_999_999_552_965_164);
    let absolute =
        WeightingGeneration::absolute_density(grid.clone(), 0.5, 0.1, None).expect("briggsabs");
    // 1 / (3·0.25 + Float(0.02)).
    assert_eq!(weight(&absolute, -250.0, 125.0, 1.0), 1.298_701_3);
    assert_eq!(weight(&absolute, 100.0, 100.0, 1.0), 0.0, "an empty cell");
    // robust = 0 is natural weighting scaled by 1/(2·noise²), whatever the
    // density; noise = 0 is uniform weighting scaled by 1/robust².
    let flat = WeightingGeneration::absolute_density(grid.clone(), 0.0, 0.5, None).expect("abs");
    assert_eq!(weight(&flat, -250.0, 125.0, 1.0), 2.0);
    assert_eq!(weight(&flat, -437.5, 0.0, 1.0), 2.0);
    let uniform = WeightingGeneration::absolute_density(grid.clone(), 2.0, 0.0, None).expect("abs");
    assert_eq!(weight(&uniform, -250.0, 125.0, 3.0), 0.25);
    assert_eq!(weight(&uniform, -437.5, 0.0, 1.0), 0.25);
    // SynthesisImagerVi2::weight refuses robust = 0 with noise = 0.
    for (robust, noise) in [(0.0, 0.0), (f64::NAN, 1.0), (0.5, f64::INFINITY)] {
        assert!(matches!(
            WeightingGeneration::absolute_density(grid.clone(), robust, noise, None),
            Err(OperatorError::Weighting { .. })
        ));
    }
}

/// CASA `radial` (`VisImagingWeight::weightRadial`): the input weight times
/// `f·sqrt(square(u) + square(v))`, `f = Float(ν/c)` and the metre
/// coordinates in double; weightless samples weigh nothing, and the uv
/// taper multiplies the result.
#[test]
fn radial_weights_scale_the_input_weight_by_casas_uv_distance() {
    // At ν = c, f = 1 exactly: a 300 m by 400 m baseline is 500 wavelengths.
    let unit = DensityUv::casa([300.0, 400.0, 50.0], 299_792_458.0);
    assert_eq!(unit.radial_distance(), 500.0);
    let radial = WeightingGeneration::Radial { taper: None };
    assert_eq!(
        radial.imaging_weight(&placement(0.0, 0.0), unit, 2.0),
        1000.0
    );
    assert_eq!(radial.imaging_weight(&placement(0.0, 0.0), unit, 0.0), 0.0);
    // At 1.4 GHz, f is ν/c = 4.669897332774129 rounded to the Float
    // 4.669897556304932.
    let band = DensityUv::casa([300.0, 400.0, 0.0], 1.4e9);
    assert_eq!(f64::from(band.wavelengths_per_m), 4.669_897_556_304_932);
    assert_ne!(f64::from(band.wavelengths_per_m), 1.4e9 / 299_792_458.0);
    assert_eq!(band.radial_distance(), 4.669_897_556_304_932 * 500.0);
    let tapered = WeightingGeneration::Radial {
        taper: Some(Taper::new(100.0, 50.0, 0.0)),
    };
    // The taper at the placement (50, 100) is exp(−2 ln 2) = 1/4.
    let weight = tapered.imaging_weight(&placement(50.0, 100.0), unit, 1.0);
    assert!((weight - 125.0).abs() < 1e-4, "{weight}");
}
