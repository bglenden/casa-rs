// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::{ArrayD, ArrayValue, RecordValue, Value};
use ndarray::{Dimension, IxDyn, Zip};

use crate::analysis::axes::StridedSection;
use crate::analysis::lel_mask::PreparedMask;
use crate::image_expr::bounded_read_cursor;
use crate::{ImageError, ImageInterface, ImagePixel};

use super::SubimageRegion;
use super::read_window::PixelSections;
use super::region::SubimageSelection;

/// Selection and binning controls for the public image histogram.
pub struct HistogramOptions<'a> {
    /// Sorted, zero-based axes reduced into each histogram, or `[-1]` for all.
    pub axes: &'a [i64],
    /// Image region or whole image if absent.
    pub region: Option<SubimageRegion<'a>>,
    /// Additional LEL mask applied before measuring a range or counting pixels.
    pub mask: Option<&'a str>,
    /// Expand a compatible singleton-axis mask when necessary.
    pub stretch: bool,
    /// Positive number of bins per histogram.
    pub bins: i64,
    /// Accepted pixel interval, a symmetric bound, or `[-1]`/empty for all samples.
    pub include: &'a [f64],
    /// Report running counts over successive bins.
    pub cumulative: bool,
    /// Report base-ten logarithms of nonzero counts.
    pub logarithmic: bool,
}

/// The two real image precisions supported by CASA histogram records.
pub trait HistogramPixel: ImagePixel + Into<f64> {
    /// Retain the source precision in a value reported after arithmetic.
    fn rounded(value: f64) -> f64;
}

impl HistogramPixel for f32 {
    fn rounded(value: f64) -> f64 {
        f64::from(value as f32)
    }
}

impl HistogramPixel for f64 {
    fn rounded(value: f64) -> f64 {
        value
    }
}

/// Measure and bin the accepted pixels in bounded image windows.
///
/// The result contains values and counts with a leading bin axis, and the
/// corresponding per-slice sample mean and standard deviation.
///
/// # Errors
/// Invalid axes, range, output dimensions, or a slice with no accepted pixels.
pub fn image_histogram<T: HistogramPixel>(
    image: &dyn ImageInterface<T>,
    options: HistogramOptions<'_>,
) -> Result<RecordValue, ImageError> {
    let bins = u32::try_from(options.bins)
        .ok()
        .filter(|&bins| bins > 0)
        .ok_or_else(|| invalid("Invalid number of bins"))? as usize;
    let kept = retained_axes(image.ndim(), options.axes)?;
    let include = inclusion_range::<T>(options.include)?;
    let selection = SubimageSelection::resolve_section(image, None, options.region, None, None)?;
    let windows = PixelSections {
        image,
        selection: &selection,
        mask: PreparedMask::new(
            options.mask,
            image.shape(),
            image.coordinates(),
            options.stretch,
        )?,
    };
    let shape = selection.packed_shape();
    let mut output_shape: Vec<_> = kept.iter().map(|&axis| shape[axis]).collect();
    let groups = product(&output_shape)?;
    let count = groups
        .checked_mul(bins)
        .ok_or_else(|| invalid("Histogram output shape is too large"))?;
    let mut stats: Vec<HistogramStatistics> = allocate(groups)?;
    let ones = vec![1; image.ndim()];
    let section = StridedSection {
        start: &selection.start,
        shape: &shape,
        stride: selection.stride.as_deref().unwrap_or(&ones),
    };
    let scan = HistogramScan {
        kept: &kept,
        section,
        windows,
        include,
    };
    scan.visit(|group, value| stats[group].add(value))?;
    if stats.iter().any(|stats| stats.count == 0) {
        return Err(invalid(
            "Failed to compute statistics, if you set a range you have likely excluded all valid pixels",
        ));
    }
    let limits: Vec<_> = stats
        .iter()
        .map(|stats| BinRange::new(include.unwrap_or([stats.minimum, stats.maximum]), bins))
        .collect();
    let mut counts: Vec<u64> = allocate(count)?;
    scan.visit(|group, value| {
        let bin = limits[group].index(value, bins);
        counts[bin * groups + group] += 1;
    })?;
    let mut bin_shape = vec![bins];
    bin_shape.extend_from_slice(&output_shape);
    if output_shape.is_empty() {
        output_shape.push(1);
    }
    let mut coords: Vec<f64> = allocate(count)?;
    let mut displayed: Vec<f64> = allocate(count)?;
    for (group, range) in limits.into_iter().enumerate() {
        let mut center = T::rounded(range.minimum + range.width * 0.5);
        let mut total = 0;
        for bin in 0..bins {
            let at = bin * groups + group;
            let count = counts[at];
            total += count;
            let tally = if options.cumulative { total } else { count };
            let value = T::rounded(tally as f64);
            coords[at] = center;
            displayed[at] = if options.logarithmic && tally > 0 {
                T::rounded(value.log10())
            } else {
                value
            };
            center = T::rounded(center + range.width);
        }
    }
    let mean = stats.iter().map(|stats| T::rounded(stats.mean)).collect();
    let sigma = stats
        .iter()
        .map(|stats| T::rounded(stats.sigma()))
        .collect();
    let mut record = RecordValue::default();
    for (field, shape, values) in [
        ("values", bin_shape.as_slice(), coords),
        ("counts", bin_shape.as_slice(), displayed),
        ("mean", output_shape.as_slice(), mean),
        ("sigma", output_shape.as_slice(), sigma),
    ] {
        let data = ArrayD::from_shape_vec(IxDyn(shape), values)
            .map_err(|error| invalid(error.to_string()))?;
        record.upsert(field, Value::Array(ArrayValue::Float64(data)));
    }
    Ok(record)
}

struct HistogramScan<'a, T: HistogramPixel> {
    kept: &'a [usize],
    section: StridedSection<'a>,
    windows: PixelSections<'a, T>,
    include: Option<[f64; 2]>,
}

impl<T: HistogramPixel> HistogramScan<'_, T> {
    fn visit(&self, mut sample: impl FnMut(usize, f64)) -> Result<(), ImageError> {
        let shape = self.section.shape;
        let cursor = bounded_read_cursor(shape.to_vec())?;
        for block in self.section.cursors(cursor) {
            let block = block?;
            let start = self.section.source_start(&block.position);
            if !self
                .windows
                .selection
                .overlaps_window(&start, &self.section.dense_shape(&block.shape))
            {
                continue;
            }
            let (values, mask) = self
                .windows
                .read(&start, &block.shape, self.section.stride)?;
            if self.kept.is_empty() {
                let mut keep = |value: T| {
                    let value = value.into();
                    if self.accepts(value) {
                        sample(0, value);
                    }
                };
                if let Some(mask) = &mask {
                    Zip::from(&values).and(mask).for_each(|&value, &valid| {
                        if valid {
                            keep(value);
                        }
                    });
                } else {
                    values.iter().copied().for_each(keep);
                }
                continue;
            }
            for (index, &value) in values.indexed_iter() {
                if mask.as_ref().is_some_and(|mask| !mask[&index]) {
                    continue;
                }
                let value = value.into();
                if !self.accepts(value) {
                    continue;
                }
                let at = index.slice();
                let group = self.kept.iter().fold(0_usize, |group, &axis| {
                    group * shape[axis] + block.position[axis] + at[axis]
                });
                sample(group, value);
            }
        }
        Ok(())
    }

    fn accepts(&self, value: f64) -> bool {
        value.is_finite()
            && self
                .include
                .is_none_or(|[min, max]| min <= value && value <= max)
    }
}

#[derive(Clone, Copy)]
struct HistogramStatistics {
    count: u64,
    mean: f64,
    squared_error: f64,
    minimum: f64,
    maximum: f64,
}

impl Default for HistogramStatistics {
    fn default() -> Self {
        Self {
            count: 0,
            mean: 0.,
            squared_error: 0.,
            minimum: f64::INFINITY,
            maximum: f64::NEG_INFINITY,
        }
    }
}

impl HistogramStatistics {
    fn add(&mut self, value: f64) {
        self.count += 1;
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        self.squared_error += delta * (value - self.mean);
        self.minimum = self.minimum.min(value);
        self.maximum = self.maximum.max(value);
    }

    fn sigma(self) -> f64 {
        if self.count <= 1 {
            0.
        } else {
            (self.squared_error / (self.count - 1) as f64)
                .max(0.)
                .sqrt()
        }
    }
}

struct BinRange {
    minimum: f64,
    width: f64,
}

impl BinRange {
    fn new([minimum, maximum]: [f64; 2], bins: usize) -> Self {
        // Casacore uses the Float bin-width calculation for both image types.
        let width = (maximum as f32 - minimum as f32) / bins as f32;
        let width = if width == 0. { 0.001_f32 } else { width };
        Self {
            minimum,
            width: f64::from(width),
        }
    }

    fn index(&self, value: f64, bins: usize) -> usize {
        let distance = value as f32 - self.minimum as f32;
        ((distance / self.width as f32) as usize).min(bins - 1)
    }
}

fn retained_axes(rank: usize, axes: &[i64]) -> Result<Vec<usize>, ImageError> {
    if axes == [-1] {
        return Ok(Vec::new());
    }
    if axes.is_empty() || axes.iter().any(|&axis| axis < 0) {
        return Err(invalid("All axes must be nonnegative"));
    }
    if axes.iter().any(|&axis| axis as usize >= rank) {
        return Err(invalid("Invalid cursor axes"));
    }
    if axes.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid("Histogram axes must be distinct and increasing"));
    }
    Ok((0..rank)
        .filter(|&axis| !axes.contains(&(axis as i64)))
        .collect())
}

fn inclusion_range<T: HistogramPixel>(values: &[f64]) -> Result<Option<[f64; 2]>, ImageError> {
    let interval = match values {
        [] | [-1.] => return Ok(None),
        [limit] => [-limit.abs(), limit.abs()],
        [first, last] => [first.min(*last), first.max(*last)],
        _ => return Err(invalid("Invalid pixel inclusion range")),
    };
    Ok(Some(interval.map(T::rounded)))
}

fn product(shape: &[usize]) -> Result<usize, ImageError> {
    shape
        .iter()
        .try_fold(1_usize, |size, &length| size.checked_mul(length))
        .ok_or_else(|| invalid("Histogram output shape is too large"))
}

fn allocate<T: Default + Clone>(count: usize) -> Result<Vec<T>, ImageError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|error| invalid(error.to_string()))?;
    values.resize(count, T::default());
    Ok(values)
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(message.into())
}
