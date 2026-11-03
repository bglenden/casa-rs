// SPDX-License-Identifier: LGPL-3.0-or-later
//! tclean's `mosweight` on a cube's per-channel density
//! (`BriggsCubeWeightor`'s `multiField`): CASA grids each field's padded
//! density cube from that field's rows alone, takes its Briggs factors per
//! channel from that grid, and weighs that field's rows by it
//! (`BriggsCubeWeightor::fillImgWeightCol`). A per-field generation holds,
//! for each field, exactly the density, factors and imaging weights that
//! field's samples give on their own.

mod common;

use std::num::NonZeroU32;

use casa_imaging_model::PolarizationCoordinate;
use casa_imaging_operator::{
    BandwidthTaper, DensityCellRule, DensityFields, DensityGrid, DensityGridShape, GridPrecision,
    MeasurementOperator, NativeRow, RowContext, SampleBuffer, SpectralAxis, SpectralKernel,
    SpectralResampler, WeightingGeneration, build_density_grid,
};
use casa_types::Correlation;
use common::{Rng, operator};
use num_complex::Complex32;

const XX_YY: [Correlation; 2] = [Correlation::XX, Correlation::YY];
const STOKES_I: [PolarizationCoordinate; 1] = [PolarizationCoordinate::StokesI];
/// Four 50 MHz output channels from 1 GHz, padded by CASA's `multiField`
/// swing pad of four planes, two on each side.
const OUTPUT_CHANNELS: u32 = 4;
const PADDING: u32 = 2;
/// Native channels 40 MHz apart from 0.9 GHz: the first ones fall on the
/// lower padding planes and the last ones beyond the padded axis.
const NATIVE_HZ: [f64; 9] = [
    0.90e9, 0.94e9, 0.98e9, 1.02e9, 1.06e9, 1.10e9, 1.14e9, 1.18e9, 1.22e9,
];
/// The span's field keys: 4, 5 and 7 have rows, 6 none; 9 is outside it.
const SPAN: DensityFields = DensityFields {
    first: 4,
    count: NonZeroU32::new(4).expect("four fields"),
};

/// One row's owned samples.
struct Row {
    field: u32,
    uvw_m: [f64; 3],
    values: Vec<Complex32>,
    weights: Vec<f32>,
    flags: Vec<bool>,
}

impl Row {
    fn native(&self) -> NativeRow<'_> {
        NativeRow {
            uvw_m: self.uvw_m,
            phase_shift_m: 0.0,
            pointing_offset_rad: [0.0; 2],
            frequencies_hz: &NATIVE_HZ,
            values: &self.values,
            weights: &self.weights,
            flags: &self.flags,
            row_flag: false,
            context: RowContext {
                original_uvw_m: None,
                time_s: 0.0,
                antennas: [0, 1],
                antenna_types: [0, 0],
                parallactic_angle_rad: [0.0, 0.0],
                field: self.field,
                spectral_window: 0,
            },
        }
    }
}

/// Rows of every field on short baselines, so samples of one field and of
/// different fields share cells; random weights per hand and a few flags.
fn rows() -> Vec<Row> {
    let mut rng = Rng::new(818);
    let mut rows = Vec::new();
    for field in [4, 5, 7, 9] {
        for _ in 0..24 {
            let samples = NATIVE_HZ.len() * XX_YY.len();
            rows.push(Row {
                field,
                uvw_m: [
                    800.0 * rng.signed(),
                    800.0 * rng.signed(),
                    50.0 * rng.signed(),
                ],
                values: vec![Complex32::new(1.0, 0.0); samples],
                weights: (0..samples)
                    .map(|_| (0.5 + 1.5 * rng.unit()) as f32)
                    .collect(),
                flags: (0..samples).map(|_| rng.unit() < 0.05).collect(),
            });
        }
    }
    rows
}

fn shape(fields: Option<DensityFields>) -> DensityGridShape {
    DensityGridShape {
        width: 64,
        height: 64,
        planes: (OUTPUT_CHANNELS + 2 * PADDING) as usize,
        padding: PADDING,
        box_half_width: 0,
        increment_rad: [-2.0e-5, 2.0e-5],
        rule: DensityCellRule::Cube,
        fields,
    }
}

struct Fixture {
    resampler: SpectralResampler,
    operator: MeasurementOperator,
}

impl Fixture {
    fn new() -> Self {
        let output = SpectralAxis::new(1.0e9, 0.05e9, OUTPUT_CHANNELS).expect("axis");
        let resampler = SpectralResampler::channel_local(output, SpectralKernel::Linear);
        let operator = operator(GridPrecision::F64, resampler.basis(), &XX_YY, &STOKES_I);
        Self {
            resampler,
            operator,
        }
    }

    /// The density of `rows` on a grid of `shape`.
    fn density<'r>(
        &self,
        shape: DensityGridShape,
        rows: impl Iterator<Item = &'r Row>,
    ) -> DensityGrid {
        let mut density = SampleBuffer::new(1);
        for row in rows {
            self.resampler
                .place_density(&self.operator, &row.native(), &shape, &mut density)
                .expect("density");
        }
        build_density_grid(std::iter::once(density.block()), shape)
    }

    /// Each placed sample's output plane and imaging weight under
    /// `weighting`.
    fn weights(&self, weighting: &WeightingGeneration, row: &Row) -> Vec<(u32, f32)> {
        let mut out = SampleBuffer::new(XX_YY.len());
        self.resampler
            .place(&self.operator, weighting, &row.native(), &mut out)
            .expect("place");
        let block = out.block();
        (0..out.len())
            .map(|sample| (out.placements()[sample].plane, block.weights_of(sample)[0]))
            .collect()
    }
}

/// Uniform, Briggs and bandwidth-tapered Briggs weighting over `grid`.
fn weightings(grid: &DensityGrid) -> [WeightingGeneration; 3] {
    let bandwidth = BandwidthTaper::from_frequency_range(1.0e9, 1.15e9).expect("bandwidth");
    [
        WeightingGeneration::density(grid.clone(), None, None, None).expect("uniform"),
        WeightingGeneration::density(grid.clone(), Some(0.5), None, None).expect("briggs"),
        WeightingGeneration::density(grid.clone(), Some(-0.5), Some(bandwidth), None)
            .expect("briggsbwtaper"),
    ]
}

/// Field `index` of the span's planes in `grid` equal `alone`'s, cell for
/// cell, with its weight sums.
fn assert_field_planes(grid: &DensityGrid, index: usize, alone: &DensityGrid, field: u32) {
    let planes = alone.shape().planes;
    for plane in 0..planes {
        assert_eq!(
            grid.plane(index * planes + plane),
            alone.plane(plane),
            "field {field}, density plane {plane}"
        );
    }
    assert_eq!(
        &grid.sum_weights()[index * planes..(index + 1) * planes],
        alone.sum_weights(),
        "field {field}"
    );
}

/// Each field's planes of the per-field cube density are its own density
/// cube: its cells and weight sums, its Briggs factors per channel, and the
/// imaging weight of every sample of its rows, under uniform, Briggs and
/// bandwidth-tapered Briggs weighting, equal those of a per-channel density
/// gridded from that field's rows alone. A field in the span without rows
/// keeps empty planes, and a field outside the span grids and weighs
/// nothing. One density over every field weighs the same rows otherwise.
#[test]
fn a_per_field_cube_density_is_each_fields_density_alone() {
    let fixture = Fixture::new();
    let rows = rows();
    let per_field = fixture.density(shape(Some(SPAN)), rows.iter());
    let by_field = weightings(&per_field);
    let joint = weightings(&fixture.density(shape(None), rows.iter()));
    // The grid holds the planes of every field the span names: it is charged
    // as that many single-field grids.
    assert_eq!(shape(Some(SPAN)).total_planes(), 4 * shape(None).planes);
    assert_eq!(shape(Some(SPAN)).bytes(), 4 * shape(None).bytes());
    assert_eq!(per_field.cells().len(), 4 * shape(None).cells());
    let mut joint_differs = false;
    for (index, field) in (SPAN.first..SPAN.first + SPAN.count.get()).enumerate() {
        let own = rows.iter().filter(|row| row.field == field);
        let alone = fixture.density(shape(None), own.clone());
        assert_field_planes(&per_field, index, &alone, field);
        if field == 6 {
            assert!(alone.cells().iter().all(|cell| *cell == 0.0));
            continue;
        }
        assert!(
            alone.cells().iter().any(|cell| *cell > 0.0),
            "field {field}"
        );
        let planes = alone.shape().planes;
        let briggs = |grid: &DensityGrid, plane: usize| grid.robust_factors(0.5).factor(plane);
        for plane in 0..planes {
            assert_eq!(
                briggs(&per_field, index * planes + plane).to_bits(),
                briggs(&alone, plane).to_bits(),
                "field {field}, plane {plane}"
            );
        }
        for ((by_field, alone), joint) in by_field.iter().zip(weightings(&alone)).zip(&joint) {
            for row in own.clone() {
                let weights = fixture.weights(by_field, row);
                assert!(!weights.is_empty(), "field {field}");
                assert_eq!(weights, fixture.weights(&alone, row), "field {field}");
                joint_differs |= weights != fixture.weights(joint, row);
            }
        }
    }
    assert!(joint_differs, "one density over every field weighs alike");
    for row in rows.iter().filter(|row| row.field == 9) {
        for weighting in &by_field {
            assert!(fixture.weights(weighting, row).is_empty());
        }
    }
}
