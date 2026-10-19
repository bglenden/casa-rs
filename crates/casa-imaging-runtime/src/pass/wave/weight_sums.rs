// SPDX-License-Identifier: LGPL-3.0-or-later
//! An owner's weight image, summed by contribution before it is gridded
//! (#787).
//!
//! `Mode::Weight` places every sample's weight taps at the uv origin
//! (`CpuBackend`'s gridding, the Metal records), so a sample's contribution
//! is fixed by its kernel cell, plane, spectral factor, pointing gradient
//! and the sign of its w, and scales with its weights alone. Samples that
//! agree on those five add the same taps scaled by their weights: summing
//! their weights in f64 first and gridding each group once gives the same
//! image, to rounding, for a lookup and an add per sample rather than a
//! kernel stamp.
//!
//! What it saves, in work and in precision, depends on how many groups the
//! samples share:
//! - **Fixed pointing.** A mosaic with fixed pointings shares a few groups
//!   per pointing and plane: B10, the 1024² ALMA mosaic, sums 4.19 M
//!   placements into 14 (#787). Its weight image then costs a lookup per
//!   sample. On f32 grids (Metal, plan decision D2) it also keeps the grid's
//!   own precision: millions of adds into the origin's cells had lost about
//!   1e-3, and a few adds per group do not.
//! - **A pointing that drifts row by row.** AW projection's pointing does,
//!   which leaves about two samples a group: B3, the 63-field VLASS clean,
//!   has 3.36 M groups for 6.42 M placements. The saving is then about
//!   half, and f32 grids keep most of their per-sample drift.
//!
//! The row-work pass counts the groups the same way ([`GroupCount`]), so
//! the strips charge the weight owner for the stamps it will make.

use std::collections::{HashMap, HashSet};

use casa_imaging_operator::{CfKey, Placement, SampleBlock, SampleBuffer};
use num_complex::Complex32;

/// Groups an owner's table holds before it grids them and starts again: a
/// mosaic's first pass has a few per pointing and plane (14 on B10), so the
/// table grids once a wave unless the pointing drifts row by row.
pub(in crate::pass) const WEIGHT_GROUPS: usize = 1 << 14;

/// Correlations a placement carries at most.
const MAX_POL: usize = 4;

/// What fixes one sample's weight-image contribution apart from its
/// weights: every input `Mode::Weight` reads but the weights.
///
/// Both backends locate the weight taps at the uv origin, so the sample's
/// `u` and `v` are not read. Only the sign of `w` is read (the Mueller table
/// and the tap conjugation), and `phase` and `prediction_w_positive` are
/// read only by prediction. Floats are kept as their bits, so two samples
/// share a group only when their contributions are the same bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Contribution {
    cf: CfKey,
    plane: u32,
    spectral: u32,
    gradient: [u32; 2],
    w_positive: bool,
}

impl Contribution {
    fn of(placement: &Placement) -> Self {
        Self {
            cf: placement.cf,
            plane: placement.plane,
            spectral: placement.spectral.to_bits(),
            gradient: placement.gradient.map(f32::to_bits),
            w_positive: placement.w > 0.0,
        }
    }

    /// A placement with this contribution: at the uv origin, as
    /// `Mode::Weight` grids every sample, with a w of the group's sign.
    fn placement(self) -> Placement {
        Placement {
            u: 0.0,
            v: 0.0,
            w: if self.w_positive { 1.0 } else { -1.0 },
            prediction_w_positive: self.w_positive,
            phase: 0.0,
            plane: self.plane,
            spectral: f32::from_bits(self.spectral),
            cf: self.cf,
            gradient: self.gradient.map(f32::from_bits),
        }
    }
}

/// The groups a [`WeightSums`] of `capacity` groups grids for the
/// placements it is shown, counted without summing: the row-work pass
/// charges the weight owner a kernel stamp for each
/// ([`ModeWork`](crate::pass::ModeWork)). Its set is allocated whole on the
/// first placement, so a pass without a weight image holds nothing.
pub(in crate::pass) struct GroupCount {
    capacity: usize,
    seen: HashSet<Contribution>,
    last: Option<Contribution>,
}

impl GroupCount {
    /// A counter that forgets every group when `capacity` are held, as a
    /// full [`WeightSums`] grids and starts again.
    pub(in crate::pass) fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "a group count holds at least one group");
        Self {
            capacity,
            seen: HashSet::new(),
            last: None,
        }
    }

    /// Bytes a counter of `capacity` groups holds once it has seen a
    /// placement: its hash set's buckets, a key and a control byte each.
    #[must_use]
    pub(in crate::pass) fn bytes(capacity: usize) -> u64 {
        let buckets = (capacity * 8).div_ceil(7).next_power_of_two().max(4);
        (buckets * (size_of::<Contribution>() + 1) + 16) as u64
    }

    /// Whether `placement` starts a group that [`WeightSums::add`] would
    /// grid: its contribution is not among those held since the last time
    /// the table filled.
    pub(in crate::pass) fn starts(&mut self, placement: &Placement) -> bool {
        let key = Contribution::of(placement);
        if self.last == Some(key) {
            return false;
        }
        self.last = Some(key);
        if self.seen.capacity() == 0 {
            self.seen.reserve(self.capacity);
        }
        if self.seen.contains(&key) {
            return false;
        }
        if self.seen.len() == self.capacity {
            self.seen.clear();
        }
        self.seen.insert(key);
        true
    }
}

/// One owner's weight-image contributions, summed in f64 by
/// [`Contribution`] in the order they first appear, and gridded as one
/// placement per group ([`Self::flush`]) when the table fills and when the
/// wave ends.
pub(in crate::pass) struct WeightSums {
    npol: usize,
    capacity: usize,
    index: HashMap<Contribution, usize>,
    groups: Vec<Contribution>,
    /// `npol` weights per group.
    sums: Vec<f64>,
    /// The groups as placements, for the backend.
    buffer: SampleBuffer,
    /// Placements summed, groups gridded and grids made since the table
    /// was made, for the wave's log ([`Self::totals`]).
    totals: [u64; 3],
}

impl WeightSums {
    /// An empty table of `capacity` groups of `npol` weights, allocated
    /// whole ([`Self::bytes`]).
    pub(in crate::pass) fn new(npol: usize, capacity: usize) -> Self {
        assert!(
            (1..=MAX_POL).contains(&npol),
            "a placement carries one to four correlations"
        );
        assert!(capacity > 0, "a weight table holds at least one group");
        Self {
            npol,
            capacity,
            index: HashMap::with_capacity(capacity),
            groups: Vec::with_capacity(capacity),
            sums: Vec::with_capacity(capacity * npol),
            buffer: SampleBuffer::with_capacity(npol, capacity),
            totals: [0; 3],
        }
    }

    /// Placements summed, groups gridded and grids made so far.
    pub(in crate::pass) const fn totals(&self) -> [u64; 3] {
        self.totals
    }

    /// Bytes a table of `capacity` groups of `npol` weights holds: the
    /// hash index (its buckets, each a key, an index and a control byte,
    /// at the standard library's 7/8 load), the groups, their sums and
    /// the buffer they are gridded from.
    #[must_use]
    pub(in crate::pass) fn bytes(npol: usize, capacity: usize) -> u64 {
        let buckets = (capacity * 8).div_ceil(7).next_power_of_two().max(4);
        let index = buckets * (size_of::<(Contribution, usize)>() + 1) + 16;
        (index + capacity * size_of::<Contribution>() + capacity * npol * size_of::<f64>()) as u64
            + SampleBuffer::bytes(npol, capacity)
    }

    /// Bytes the table holds now, from its parts' capacities.
    #[cfg(test)]
    fn capacity_bytes(&self) -> u64 {
        let index = self.index.capacity();
        let buckets = (index * 8).div_ceil(7).next_power_of_two().max(4);
        (buckets * (size_of::<(Contribution, usize)>() + 1)
            + 16
            + self.groups.capacity() * size_of::<Contribution>()
            + self.sums.capacity() * size_of::<f64>()) as u64
            + self.buffer.capacity_bytes()
    }

    /// Add the weights of every placement of `block` to its group, gridding
    /// the groups through `grid` first whenever a new group finds the
    /// table full.
    pub(in crate::pass) fn add<E>(
        &mut self,
        block: &SampleBlock<'_>,
        grid: &mut dyn FnMut(&SampleBlock<'_>) -> Result<(), E>,
    ) -> Result<(), E> {
        debug_assert_eq!(block.npol, self.npol, "the table's correlations");
        self.totals[0] += block.len() as u64;
        // Rows place their channels together, so successive samples share
        // a group almost always: look the hash up only when it changes.
        let mut last: Option<(Contribution, usize)> = None;
        for (index, placement) in block.placements.iter().enumerate() {
            let key = Contribution::of(placement);
            let group = match last {
                Some((held, group)) if held == key => group,
                _ => {
                    let group = match self.index.get(&key) {
                        Some(&group) => group,
                        None => {
                            if self.groups.len() == self.capacity {
                                self.flush(grid)?;
                            }
                            let group = self.groups.len();
                            self.index.insert(key, group);
                            self.groups.push(key);
                            self.sums.resize(self.sums.len() + self.npol, 0.0);
                            group
                        }
                    };
                    last = Some((key, group));
                    group
                }
            };
            let sums = &mut self.sums[group * self.npol..(group + 1) * self.npol];
            for (sum, weight) in sums.iter_mut().zip(block.weights_of(index)) {
                *sum += f64::from(*weight);
            }
        }
        Ok(())
    }

    /// Grid every group through `grid`, as one placement of its summed
    /// weights each, and empty the table. Nothing when it is empty.
    pub(in crate::pass) fn flush<E>(
        &mut self,
        grid: &mut dyn FnMut(&SampleBlock<'_>) -> Result<(), E>,
    ) -> Result<(), E> {
        if self.groups.is_empty() {
            return Ok(());
        }
        self.buffer.clear();
        let values = [Complex32::default(); MAX_POL];
        let mut weights = [0.0_f32; MAX_POL];
        for (group, key) in self.groups.iter().enumerate() {
            let sums = &self.sums[group * self.npol..(group + 1) * self.npol];
            for (weight, sum) in weights.iter_mut().zip(sums) {
                *weight = *sum as f32;
            }
            self.buffer
                .push(key.placement(), &values[..self.npol], &weights[..self.npol]);
        }
        self.totals[1] += self.groups.len() as u64;
        self.totals[2] += 1;
        self.index.clear();
        self.groups.clear();
        self.sums.clear();
        grid(&self.buffer.block())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn placement(cf: u16, plane: u32, spectral: f32, gradient: [f32; 2], w: f64) -> Placement {
        Placement {
            u: 123.0,
            v: -45.0,
            w,
            prediction_w_positive: w > 0.0,
            phase: 0.7,
            plane,
            spectral,
            cf: CfKey { group: cf, cube: 0 },
            gradient,
        }
    }

    /// The groups a table hands to `grid`, with their summed weights.
    fn flushed(sums: &mut WeightSums) -> Vec<(Placement, Vec<f32>)> {
        let mut out = Vec::new();
        sums.flush(&mut |block: &SampleBlock<'_>| {
            for (index, placement) in block.placements.iter().enumerate() {
                out.push((*placement, block.weights_of(index).to_vec()));
            }
            Ok::<_, ()>(())
        })
        .expect("flush");
        out
    }

    /// Samples that differ in any input of `Mode::Weight` but their weights
    /// keep apart, and the rest merge with their weights summed (#787).
    #[test]
    fn only_samples_with_one_contribution_share_a_group() {
        let base = placement(1, 0, 0.25, [0.5, -0.5], 10.0);
        let variants = [
            base,
            placement(2, 0, 0.25, [0.5, -0.5], 10.0),
            placement(1, 1, 0.25, [0.5, -0.5], 10.0),
            placement(1, 0, 0.5, [0.5, -0.5], 10.0),
            placement(1, 0, 0.25, [0.25, -0.5], 10.0),
            placement(1, 0, 0.25, [0.5, -0.25], 10.0),
            placement(1, 0, 0.25, [0.5, -0.5], -10.0),
        ];
        // The base again at another uv, w magnitude and phase, which
        // `Mode::Weight` does not read.
        let same = Placement {
            u: -9.0,
            v: 8.0,
            w: 3.0,
            phase: -1.0,
            ..base
        };
        let mut buffer = SampleBuffer::with_capacity(2, 16);
        for placement in variants.iter().chain([&same]) {
            buffer.push(*placement, &[Complex32::default(); 2], &[1.5, 2.5]);
        }
        let mut sums = WeightSums::new(2, 16);
        sums.add(&buffer.block(), &mut |_: &SampleBlock<'_>| Ok::<_, ()>(()))
            .expect("add");
        let groups = flushed(&mut sums);
        assert_eq!(groups.len(), variants.len());
        assert_eq!(groups[0].1, [3.0, 5.0], "the base merged with its twin");
        for (group, variant) in groups.iter().zip(&variants).skip(1) {
            assert_eq!(group.1, [1.5, 2.5]);
            assert_eq!(Contribution::of(&group.0), Contribution::of(variant));
        }
        assert!(flushed(&mut sums).is_empty(), "a flush empties the table");
    }

    /// A full table grids its groups before it takes a new one, and every
    /// weight reaches exactly one flushed group.
    #[test]
    fn a_full_table_grids_before_it_takes_a_new_group() {
        let mut buffer = SampleBuffer::with_capacity(1, 64);
        for sample in 0..64_u32 {
            let cf = u16::try_from(sample % 5).expect("small");
            buffer.push(
                placement(cf, 0, 0.0, [0.0, 0.0], 1.0),
                &[Complex32::default()],
                &[1.0],
            );
        }
        let mut sums = WeightSums::new(1, 3);
        let mut grids = 0;
        let mut total = 0.0;
        let mut grid = |block: &SampleBlock<'_>| {
            assert!(block.len() <= 3, "never more groups than the table holds");
            grids += 1;
            total += block.weights.iter().map(|w| f64::from(*w)).sum::<f64>();
            Ok::<_, ()>(())
        };
        sums.add(&buffer.block(), &mut grid).expect("add");
        sums.flush(&mut grid).expect("flush");
        assert!(grids > 1, "the table filled and gridded early");
        assert_eq!(total, 64.0);
    }

    /// The ALMA 12 m mosaic set on a 64² grid, on a two-term Taylor basis,
    /// gridding at `precision`.
    fn mosaic_operator(
        precision: casa_imaging_operator::GridPrecision,
    ) -> casa_imaging_operator::MeasurementOperator {
        use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
        use casa_imaging_operator::{
            AiryDish, Basis, GridGeometry, GridPadding, ImageExtent, MeasurementOperator, MosaicPb,
            MosaicWindow, PolarizationRouting,
        };
        let geometry = GridGeometry::new(
            ImageExtent {
                shape: [64, 64],
                increment_rad: [-2.0e-5, 2.0e-5],
                reference_pixel: [32, 32],
            },
            GridPadding::None,
        )
        .expect("geometry");
        let routing = PolarizationRouting::compile(
            &[CorrelationType::LinearXx, CorrelationType::LinearYy],
            &[PolarizationCoordinate::StokesI],
        )
        .expect("routing");
        let window = MosaicWindow {
            spectral_window: 0,
            window_frequencies_hz: vec![0.98e11, 1.0e11],
            channel_width_hz: 1.0e6,
            selected_frequencies_hz: vec![1.0e11],
        };
        let dishes = [AiryDish::casa_alma(12.0, &geometry)];
        let cf =
            MosaicPb::new(&geometry, &routing, 1.0e11, &dishes, &[window]).expect("mosaic set");
        MeasurementOperator::new(
            geometry,
            Basis::Taylor {
                terms: 2,
                reference_hz: 1.0e11,
            },
            routing,
            Box::new(cf),
            precision,
        )
    }

    /// `samples` weight placements over three pointings, three spectral
    /// factors and both signs of w, with random weights, in random order.
    fn weight_samples(
        operator: &casa_imaging_operator::MeasurementOperator,
        samples: usize,
    ) -> SampleBuffer {
        let gradients = [[0.0, 0.0], [0.6, -0.2], [-0.3, 0.9]].map(|offset: [f64; 2]| {
            operator
                .geometry()
                .pointing_gradient([offset[0] * 1.0e-4, offset[1] * 1.0e-4])
        });
        let spectral = [-0.01_f32, 0.0, 0.02];
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut buffer = SampleBuffer::with_capacity(2, samples);
        for _ in 0..samples {
            let draw = next();
            let weight = |shift: u32| 0.5 + ((draw >> shift) & 0xffff) as f32 / 65536.0;
            buffer.push(
                Placement {
                    u: 0.0,
                    v: 0.0,
                    w: if draw & 1 == 0 { 25.0 } else { -25.0 },
                    prediction_w_positive: true,
                    phase: 0.0,
                    plane: 0,
                    spectral: spectral[(draw >> 1) as usize % 3],
                    cf: CfKey::default(),
                    gradient: gradients[(draw >> 3) as usize % 3],
                },
                &[Complex32::default(); 2],
                &[weight(16), weight(32)],
            );
        }
        buffer
    }

    /// The weight image `samples` grid on `operator`, one sample at a time
    /// (`per_sample`) or summed by contribution, as its finished images.
    fn weight_image(
        operator: &casa_imaging_operator::MeasurementOperator,
        samples: &SampleBuffer,
        per_sample: bool,
    ) -> Vec<f32> {
        weight_image_and_sums(operator, samples, per_sample).0
    }

    /// [`weight_image`] and the finished images' `sumwt`.
    fn weight_image_and_sums(
        operator: &casa_imaging_operator::MeasurementOperator,
        samples: &SampleBuffer,
        per_sample: bool,
    ) -> (Vec<f32>, Vec<f64>) {
        use casa_imaging_operator::{CpuBackend, GridBackend, Mode, ModeSet, PlaneRange, Work};
        let mut acc = operator.accumulator(PlaneRange::single(0), None, ModeSet::ALL);
        let mut backend = CpuBackend::new();
        let cf = operator.cf();
        let mut grid = |block: &SampleBlock<'_>| {
            backend.apply(
                block,
                cf,
                Work::Grid {
                    mode: Mode::Weight,
                    acc: &mut acc,
                },
            )
        };
        if per_sample {
            grid(&samples.block()).expect("grid");
        } else {
            let mut sums = WeightSums::new(2, WEIGHT_GROUPS);
            sums.add(&samples.block(), &mut grid).expect("add");
            sums.flush(&mut grid).expect("flush");
        }
        let images = operator.finish(acc).expect("finish");
        let plane = &images.planes[0];
        (
            plane
                .weight
                .iter()
                .flat_map(|image| image.iter().copied())
                .collect(),
            plane.sumwt.clone(),
        )
    }

    /// The largest difference between `a` and `b`, relative to `b`'s peak.
    fn relative(a: &[f32], b: &[f32]) -> f64 {
        let peak = b
            .iter()
            .fold(0.0_f64, |peak, v| peak.max(f64::from(v.abs())));
        a.iter()
            .zip(b)
            .map(|(a, b)| f64::from((a - b).abs()))
            .fold(0.0, f64::max)
            / peak
    }

    /// Summing the weight image by contribution grids, on the real mosaic
    /// set with pointing ramps and Taylor terms, what gridding each sample
    /// grids, to rounding in f64 grids (#787). On f32 grids it stays at the
    /// f32 image's precision while the per-sample f32 weight image drifts
    /// with the sample count.
    #[test]
    fn summed_weights_grid_what_each_sample_grids() {
        use casa_imaging_operator::GridPrecision;
        let f64_operator = mosaic_operator(GridPrecision::F64);
        // Enough samples for the per-sample f32 drift, which grows with
        // their count, to show: B10 grids 4.19 M (#787).
        let samples = weight_samples(&f64_operator, 400_000);
        let (reference, reference_sumwt) = weight_image_and_sums(&f64_operator, &samples, true);
        let (summed, summed_sumwt) = weight_image_and_sums(&f64_operator, &samples, false);
        let f64_error = relative(&summed, &reference);
        assert!(f64_error <= 1.0e-6, "f64 grids: {f64_error}");
        // The weight terms' sumwt takes each group's summed weight once, as
        // the f32 weight its f64 sum rounds to (#792 review, Astra).
        assert!(
            reference_sumwt.iter().any(|sum| *sum > 0.0),
            "a weight sumwt"
        );
        for (summed, each) in summed_sumwt.iter().zip(&reference_sumwt) {
            assert!(
                (summed - each).abs() <= 1.0e-7 * each.abs().max(1.0),
                "sumwt: {summed} against {each}"
            );
        }
        let f32_operator = mosaic_operator(GridPrecision::F32);
        let summed_f32 = relative(&weight_image(&f32_operator, &samples, false), &reference);
        let each_f32 = relative(&weight_image(&f32_operator, &samples, true), &reference);
        assert!(summed_f32 <= 1.0e-6, "summed on f32 grids: {summed_f32}");
        assert!(
            each_f32 > 10.0 * summed_f32,
            "per sample on f32 grids drifts: {each_f32} against {summed_f32}"
        );
    }

    /// With no group shared, every pointing gradient distinct (AW's case
    /// at its extreme, #792 review, Astra 2), the summed table grids on f32
    /// grids exactly what each sample grids, bit for bit: groups of one
    /// sample keep its f32 weights and their order, across the table's
    /// flushes. It saves nothing then, but costs nothing in precision.
    #[test]
    fn with_no_group_shared_the_table_grids_each_sample_exactly() {
        use casa_imaging_operator::GridPrecision;
        let operator = mosaic_operator(GridPrecision::F32);
        let mut samples = weight_samples(&operator, WEIGHT_GROUPS + 3000);
        let block = samples.block();
        let mut distinct = SampleBuffer::with_capacity(2, block.len());
        for (index, placement) in block.placements.iter().enumerate() {
            let step = index as f32 * 1.0e-7;
            distinct.push(
                Placement {
                    gradient: [placement.gradient[0] + step, placement.gradient[1]],
                    ..*placement
                },
                block.values_of(index),
                block.weights_of(index),
            );
        }
        samples = distinct;
        let summed = weight_image(&operator, &samples, false);
        let each = weight_image(&operator, &samples, true);
        assert!(
            summed == each,
            "summed and per-sample f32 weight images differ"
        );
    }

    /// A table holds at most [`WeightSums::bytes`].
    #[test]
    fn a_table_holds_at_most_its_bytes() {
        for (npol, capacity) in [(1, 1), (2, 7), (2, WEIGHT_GROUPS), (4, 1000)] {
            let sums = WeightSums::new(npol, capacity);
            assert!(
                sums.capacity_bytes() <= WeightSums::bytes(npol, capacity),
                "{npol} correlations, {capacity} groups: {} > {}",
                sums.capacity_bytes(),
                WeightSums::bytes(npol, capacity)
            );
        }
    }
}
