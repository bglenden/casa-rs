// SPDX-License-Identifier: LGPL-3.0-or-later
//! The gridder against `ggridsd` written out pixel by pixel. Its agreement
//! with CASA's `tsdimaging` is in `tests/tsdimaging_casa.rs`.

use ndarray::{Array2, Array3};

use super::*;
use crate::GridFunction;

/// Spectra to grid, owning what a [`SpectrumBlock`] borrows.
struct Spectra {
    pixels: Vec<Option<[f64; 2]>>,
    values: Array3<f32>,
    flags: Array3<bool>,
    row_flags: Vec<bool>,
    weights: Array2<f32>,
    channel_map: Vec<Option<usize>>,
    correlation_map: Vec<Option<usize>>,
}

impl Spectra {
    /// `pixels.len()` spectra of `channels` channels and two hands, each
    /// value distinct, weights 1 + spectrum/10, every data channel gridded
    /// into its own image channel and both hands into plane 0.
    fn new(pixels: Vec<Option<[f64; 2]>>, channels: usize) -> Self {
        let spectra = pixels.len();
        Self {
            row_flags: vec![false; spectra],
            values: Array3::from_shape_fn((spectra, channels, 2), |(s, c, p)| {
                1.0 + s as f32 * 0.5 + c as f32 * 0.25 - p as f32 * 0.125
            }),
            flags: Array3::from_elem((spectra, channels, 2), false),
            weights: Array2::from_shape_fn((spectra, channels), |(s, _)| 1.0 + s as f32 / 10.0),
            channel_map: (0..channels).map(Some).collect(),
            correlation_map: vec![Some(0), Some(0)],
            pixels,
        }
    }

    fn block(&self) -> SpectrumBlock<'_> {
        SpectrumBlock {
            pixels: &self.pixels,
            values: self.values.view(),
            flags: self.flags.view(),
            row_flags: &self.row_flags,
            weights: self.weights.view(),
            channel_map: &self.channel_map,
            correlation_map: &self.correlation_map,
        }
    }
}

fn kernel() -> KernelTable {
    KernelTable::new(GridFunction::Spheroidal { support: 3 }).expect("kernel")
}

fn grid(
    shape: GridShape,
    spectra: &Spectra,
    (workers, flags): (usize, FlagMerge),
) -> TotalPowerImage {
    let mut grid = TotalPowerGrid::new(shape, kernel(), flags, Reservation::none()).expect("grid");
    let team = WorkerTeam::new(workers).expect("team");
    grid.grid(&spectra.block(), &team).expect("gridded");
    grid.finish().value
}

/// `ggridsd` written out, after `setSpectralFlag` merges the flags as
/// `flags` says: the data and weight grids, `[channel][plane][y][x]`.
fn reference(shape: GridShape, spectra: &Spectra, flags: FlagMerge) -> (Vec<f32>, Vec<f32>) {
    let (mut data, mut weight) = (vec![0.0_f32; shape.pixels()], vec![0.0_f32; shape.pixels()]);
    let kernel = kernel();
    let (count, channels, correlations) = spectra.values.dim();
    for spectrum in 0..count {
        let Some(pixel) = spectra.pixels[spectrum] else {
            continue;
        };
        if spectra.row_flags[spectrum] {
            continue;
        }
        let mut taps = Vec::new();
        kernel.taps(pixel, (shape.nx, shape.ny), &mut taps);
        for channel in 0..channels {
            let Some(target) = spectra.channel_map[channel] else {
                continue;
            };
            let w = spectra.weights[[spectrum, channel]];
            // `weight > 0`, which a NaN is not.
            if w.is_nan() || w <= 0.0 {
                continue;
            }
            let any = (0..correlations).any(|c| spectra.flags[[spectrum, channel, c]]);
            if flags == FlagMerge::AnyCorrelation && any {
                continue;
            }
            for correlation in 0..correlations {
                let Some(plane) = spectra.correlation_map[correlation] else {
                    continue;
                };
                if spectra.flags[[spectrum, channel, correlation]] {
                    continue;
                }
                let value = w * spectra.values[[spectrum, channel, correlation]];
                for tap in &taps {
                    let index =
                        ((target * shape.planes + plane) * shape.ny + tap.y) * shape.nx + tap.x;
                    data[index] += value * tap.weight;
                    weight[index] += w * tap.weight;
                }
            }
        }
    }
    (data, weight)
}

fn shape(channels: usize) -> GridShape {
    GridShape {
        nx: 24,
        ny: 20,
        planes: 1,
        channels,
    }
}

#[test]
fn a_spectrum_grids_its_hands_mean_with_twice_its_weight() {
    let spectra = Spectra::new(vec![Some([10.3, 9.6])], 1);
    let gridded = grid(shape(1), &spectra, (1, FlagMerge::AnyCorrelation));
    let kernel = kernel();
    let mut taps = Vec::new();
    kernel.taps([10.3, 9.6], (24, 20), &mut taps);
    let mean = 0.5 * (spectra.values[[0, 0, 0]] + spectra.values[[0, 0, 1]]);
    for tap in taps.iter().filter(|tap| tap.weight > 0.0) {
        let at = [tap.x, tap.y, 0, 0];
        assert!((gridded.image[at.as_slice()] - mean).abs() < 1.0e-6);
        assert_eq!(gridded.weight[at.as_slice()], 2.0 * tap.weight);
    }
    assert_eq!(gridded.image[[0, 0, 0, 0].as_slice()], 0.0);
}

#[test]
fn the_grids_are_ggridsds_in_its_order() {
    let pixels: Vec<Option<[f64; 2]>> = (0..40)
        .map(|i| {
            let i = f64::from(i);
            (i != 7.0).then_some([2.0 + 0.47 * i, 18.0 - 0.41 * i])
        })
        .collect();
    let mut spectra = Spectra::new(pixels, 3);
    spectra.flags[[3, 1, 0]] = true;
    // The hand not imaged, which flags the other's only when they merge.
    spectra.flags[[4, 0, 1]] = true;
    spectra.row_flags[5] = true;
    spectra.weights[[9, 2]] = 0.0;
    spectra.weights[[10, 0]] = -1.0;
    spectra.weights[[11, 1]] = f32::NAN;
    // Data channel 2 grids into image channel 0 too; image channel 2 takes
    // none; the second hand of each spectrum is not imaged.
    spectra.channel_map = vec![Some(0), Some(1), Some(0)];
    spectra.correlation_map = vec![Some(0), None];
    let shape = shape(3);
    let mut weights = Vec::new();
    for flags in [FlagMerge::AnyCorrelation, FlagMerge::PerCorrelation] {
        let (data, weight) = reference(shape, &spectra, flags);
        for workers in [1, 3] {
            let gridded = grid(shape, &spectra, (workers, flags));
            let expected: Vec<f32> = data
                .iter()
                .zip(&weight)
                .map(|(d, w)| if *w > 0.0 { d / w } else { 0.0 })
                .collect();
            assert_eq!(
                gridded.weight.as_slice_memory_order(),
                Some(weight.as_slice())
            );
            assert_eq!(
                gridded.image.as_slice_memory_order(),
                Some(expected.as_slice())
            );
        }
        weights.push(weight);
    }
    assert_ne!(weights[0], weights[1]);
}

/// Two spectra at one pointing, both of value 2: the first of `weights[0]`,
/// the second of `weights[1]`.
fn collocated(weights: [f32; 2]) -> Spectra {
    let mut spectra = Spectra::new(vec![Some([10.3, 9.6]); 2], 1);
    spectra.values.fill(2.0);
    for (spectrum, weight) in weights.into_iter().enumerate() {
        spectra.weights[[spectrum, 0]] = weight;
    }
    spectra
}

#[test]
fn a_nan_weight_is_skipped_as_ggridsd_skips_it() {
    // CASA keeps the first spectrum's value and weight; summing the NaN
    // would leave no positive weight and an empty image.
    let gridded = grid(
        shape(1),
        &collocated([1.0, f32::NAN]),
        (1, FlagMerge::AnyCorrelation),
    );
    let mut alone = Spectra::new(vec![Some([10.3, 9.6])], 1);
    alone.values.fill(2.0);
    alone.weights.fill(1.0);
    assert_eq!(
        gridded,
        grid(shape(1), &alone, (1, FlagMerge::AnyCorrelation))
    );
    assert!(gridded.weight.iter().all(|w| w.is_finite()));
    assert!(gridded.image.iter().any(|&v| (v - 2.0).abs() < 1.0e-6));
}

#[test]
fn a_nan_hand_weight_leaves_its_spectrum_ungridded() {
    // `pickWeights` combines the hands' weights; a NaN hand makes a NaN.
    let mut hands = Array3::from_elem((2, 1, 2), 1.0_f32);
    hands[[1, 0, 0]] = f32::NAN;
    let weights = crate::weights_from_spectra(hands.view()).expect("weights");
    assert_eq!(weights[[0, 0]], 2.0);
    assert!(weights[[1, 0]].is_nan());
    let gridded = grid(
        shape(1),
        &collocated([weights[[0, 0]], weights[[1, 0]]]),
        (1, FlagMerge::AnyCorrelation),
    );
    let mut alone = Spectra::new(vec![Some([10.3, 9.6])], 1);
    alone.values.fill(2.0);
    alone.weights.fill(2.0);
    assert_eq!(
        gridded,
        grid(shape(1), &alone, (1, FlagMerge::AnyCorrelation))
    );
}

#[test]
fn spectra_beyond_one_pass_of_taps_grid_as_one_block() {
    let count = tap_scratch_rows(&kernel()) + 17;
    let pixels = (0..count)
        .map(|i| Some([(i % 23) as f64 * 1.01, (i % 19) as f64 * 0.97]))
        .collect();
    let spectra = Spectra::new(pixels, 2);
    let shape = shape(2);
    let (_, weight) = reference(shape, &spectra, FlagMerge::AnyCorrelation);
    let gridded = grid(shape, &spectra, (2, FlagMerge::AnyCorrelation));
    assert_eq!(
        gridded.weight.as_slice_memory_order(),
        Some(weight.as_slice())
    );
}

#[test]
fn pixels_without_weight_are_zero_and_the_others_normalised() {
    let spectra = Spectra::new(vec![Some([10.0, 10.0])], 1);
    let gridded = grid(shape(1), &spectra, (1, FlagMerge::AnyCorrelation));
    let mean = 0.5 * (spectra.values[[0, 0, 0]] + spectra.values[[0, 0, 1]]);
    for (value, weight) in gridded.image.iter().zip(&gridded.weight) {
        if *weight > 0.0 {
            assert!((value - mean).abs() < 1.0e-6, "{value} at weight {weight}");
        } else {
            assert_eq!(*value, 0.0);
        }
    }
}

#[test]
fn blocks_that_disagree_with_themselves_or_the_image_are_refused() {
    let mut spectra = Spectra::new(vec![Some([1.0, 1.0]), None], 2);
    let mut grid = TotalPowerGrid::new(
        shape(2),
        kernel(),
        FlagMerge::AnyCorrelation,
        Reservation::none(),
    )
    .expect("grid");
    let team = WorkerTeam::new(1).expect("team");
    spectra.row_flags.pop();
    assert!(grid.grid(&spectra.block(), &team).is_err());
    spectra.row_flags.push(false);
    spectra.channel_map = vec![Some(0), Some(2)];
    assert!(grid.grid(&spectra.block(), &team).is_err());
    spectra.channel_map = vec![Some(0), Some(1)];
    spectra.correlation_map = vec![Some(0), Some(1)];
    assert!(grid.grid(&spectra.block(), &team).is_err());
}
