// SPDX-License-Identifier: LGPL-3.0-or-later

//! Image statistics computation (`ia.statistics`) for attached images.

pub mod flux;
pub mod region_planes;
pub mod report;
pub mod sampled;

use std::path::Path;

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType, format_world_display};
use casa_core::measures::MeasFrame;
use casa_core::measures::doppler::{DopplerRef, MDoppler};
use casa_core::quanta::Unit;
use casa_core::{ArrayValue, RecordField, RecordValue, Value};
use casa_images::{ImageInterface, ImagePixel};
use casa_lattices::{
    ExecutionResources, FitHalfCenter, Lattice, LatticeStatistics, ScratchSpace, Statistic,
    StatisticsAlgorithm, StatsElement, TempStoragePolicy,
};
use ndarray::{ArrayD, IxDyn, ShapeBuilder, Zip};

use crate::analysis::evaluate_image_tool_mask_section;
use crate::analysis::imsubimage::{ImageRegionSection, SubimageRegion};

use super::attachment::AttachedImage;
use super::coords::{SPEED_OF_LIGHT_M_S, stored_velocity_settings};
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, with_storage};
use super::pixels::{and_mask, collapse_axes};
use region_planes::RegionPlanes;
use report::{ImageStatisticsResult, StatisticsReport};

pub(super) const BASIC: [(&str, Statistic); 8] = [
    ("npts", Statistic::Npts),
    ("sum", Statistic::Sum),
    ("sumsq", Statistic::SumSq),
    ("min", Statistic::Min),
    ("max", Statistic::Max),
    ("mean", Statistic::Mean),
    ("rms", Statistic::Rms),
    ("sigma", Statistic::Sigma),
];

pub(super) const QUANTILES: [(&str, Statistic); 5] = [
    ("median", Statistic::Median),
    ("medabsdevmed", Statistic::MedAbsDevMed),
    ("q1", Statistic::Q1),
    ("q3", Statistic::Q3),
    ("quartile", Statistic::Quartile),
];

/// Full request specification for `ia.statistics`.
pub struct StatisticsRequest<'a> {
    /// Reduction axes (default: all axes collapsed into global statistics).
    pub axes: Option<Vec<usize>>,
    /// Optional region text or record.
    pub region: Option<SubimageRegion<'a>>,
    /// Optional LEL mask expression.
    pub mask: Option<&'a str>,
    /// Stretch lower-dimensional masks across remaining axes.
    pub stretch: bool,
    /// Optional inclusive pixel value filter `[min, max]`.
    pub includepix: Option<[f64; 2]>,
    /// Optional exclusive pixel value filter `[min, max]`.
    pub excludepix: Option<[f64; 2]>,
    /// Log formatted summary lines.
    pub list: bool,
    /// Compute robust/order statistics.
    pub robust: bool,
    /// Output text log path, if writing report to file.
    pub logfile: Option<&'a Path>,
    /// Append to existing `logfile` instead of overwriting.
    pub append: bool,
    /// Statistics algorithm name (`"classic"`, `"hinges-fences"`, `"chauvenet"`, `"fit-half"`, `"biweight"`).
    pub algorithm: &'a str,
    /// Fence factor for `"hinges-fences"`.
    pub fence: f64,
    /// Center mode for `"fit-half"` (`"mean"`, `"median"`, `"zero"`).
    pub center: &'a str,
    /// Side flag for `"fit-half"`.
    pub lside: bool,
    /// Z-score threshold for `"chauvenet"`.
    pub zscore: f64,
    /// Maximum iterations for `"chauvenet"`.
    pub maxiter: i32,
    /// Classical method (`"auto"`, `"tiled"`, `"framework"`).
    pub clmethod: &'a str,
    /// Number of iterations for `"biweight"`.
    pub niter: i32,
    /// Spill per-plane row buffers to temporary paged storage when `true`.
    pub disk: bool,
    /// Include per-plane details table in logs when `true`.
    pub verbose: bool,
}

/// Validates and parses CASA `includepix`/`excludepix` range vectors.
pub fn parse_pixel_range(values: Option<&[f64]>, name: &str) -> ImageToolResult<Option<[f64; 2]>> {
    let Some(values) = values else {
        return Ok(None);
    };
    match values {
        [] | [-1.0] => Ok(None),
        [value] => Ok(Some([-value.abs(), value.abs()])),
        [first, second] => Ok(Some([first.min(*second), first.max(*second)])),
        _ => Err(ImageToolError::new(format!(
            "Too many elements for argument {name}."
        ))),
    }
}

/// Parses CASA cursor/reduction axis arguments into validated 0-based pixel axis indices.
pub fn parse_cursor_axes(axes: &[i64], rank: usize) -> ImageToolResult<Vec<usize>> {
    if axes != [-1] && axes.iter().any(|&axis| axis < 0 || axis as usize >= rank) {
        return Err(ImageToolError::new(format!(
            "Invalid cursor axes: {axes:?}."
        )));
    }
    collapse_axes(axes, rank)
}

/// Parses CASA statistics algorithm options into [`StatisticsAlgorithm`].
pub fn parse_statistics_algorithm(
    request: &StatisticsRequest<'_>,
) -> ImageToolResult<StatisticsAlgorithm> {
    let name = request.algorithm.to_ascii_lowercase();
    if name.starts_with("cl") {
        if !request
            .clmethod
            .to_ascii_lowercase()
            .starts_with(['a', 't', 'f'])
        {
            return Err(ImageToolError::new(format!(
                "Unsupported classical method {}",
                request.clmethod
            )));
        }
        Ok(StatisticsAlgorithm::Classic)
    } else if name.starts_with("ch") {
        Ok(StatisticsAlgorithm::Chauvenet {
            zscore: request.zscore,
            maxiter: i64::from(request.maxiter),
        })
    } else if name.starts_with('b') {
        Ok(StatisticsAlgorithm::Biweight {
            niter: i64::from(request.niter),
        })
    } else if name.starts_with('h') {
        Ok(StatisticsAlgorithm::HingesFences {
            fence: request.fence,
        })
    } else if name.starts_with('f') {
        let lower = request.center.to_ascii_lowercase();
        let center = if lower.starts_with("mea") {
            FitHalfCenter::Mean
        } else if lower.starts_with("med") {
            FitHalfCenter::Median
        } else if lower.starts_with('z') {
            FitHalfCenter::Zero
        } else {
            return Err(ImageToolError::new(format!(
                "Unsupported center value {}",
                request.center
            )));
        };
        Ok(StatisticsAlgorithm::FitToHalf {
            center,
            upper: !request.lside,
        })
    } else {
        Err(ImageToolError::new(format!(
            "Unsupported algorithm {}",
            request.algorithm
        )))
    }
}

pub(super) struct SpectralVelocity {
    pub(super) unit: Unit,
    pub(super) convention: DopplerRef,
}

impl SpectralVelocity {
    fn from_image(image: &ImageHandle) -> Option<Self> {
        let coordinates = image.coordinates();
        let index = coordinates.find_coordinate(CoordinateType::Spectral)?;
        let CoordinateModel::Spectral(spectral) = coordinates.coordinate(index) else {
            return None;
        };
        if spectral.rest_frequency() <= 0.0 {
            return None;
        }
        let record = image.coordinate_record();
        let placements = CoordinateSystem::record_coordinates(&record).ok()?;
        let native = match record.get(&placements.get(index)?.field) {
            Some(Value::Record(record)) => Some(record),
            _ => None,
        };
        let (unit, convention) = stored_velocity_settings(native, spectral).ok()?;
        Some(Self { unit, convention })
    }

    pub(super) fn velocity_for_ratio(&self, ratio: f64) -> Option<f64> {
        let frame = MeasFrame::new();
        let shift = MDoppler::new(ratio, DopplerRef::RATIO)
            .convert_to(DopplerRef::BETA, &frame)
            .ok()?
            .convert_to(self.convention, &frame)
            .ok()?;
        Some(shift.value() * SPEED_OF_LIGHT_M_S / self.unit.val().factor)
    }
}

struct StatsOptions<'a> {
    axes: Vec<usize>,
    include: Option<[f64; 2]>,
    exclude: Option<[f64; 2]>,
    robust: bool,
    disk: bool,
    expression: Option<ArrayD<bool>>,
    algorithm: StatisticsAlgorithm,
    single_precision: bool,
    fits_backed: bool,
    spectral_velocity: Option<&'a SpectralVelocity>,
}

fn floating_array(array: ArrayD<f64>) -> Value {
    let mut shape: Vec<_> = array.shape().iter().copied().filter(|&n| n != 1).collect();
    if shape.is_empty() {
        shape.push(1);
    }
    let values: Vec<f64> = array.reversed_axes().iter().copied().collect();
    let reshaped = ArrayD::from_shape_vec(IxDyn(&shape).f(), values)
        .expect("collapsed floating array shape matches element count");
    Value::Array(ArrayValue::Float64(reshaped))
}

fn position_fields(
    coordinates: &CoordinateSystem,
    pixel: &[usize],
) -> ImageToolResult<(Value, Value)> {
    let world_pixel: Vec<_> = pixel.iter().map(|&value| value as f64).collect();
    let coords_i64: Vec<_> = pixel.iter().map(|&value| value as i64).collect();
    let world = format_world_display(coordinates, &world_pixel)?;
    Ok((
        Value::Array(ArrayValue::from_i64_vec(coords_i64)),
        Value::from(world),
    ))
}

fn region_fields(
    coordinates: &CoordinateSystem,
    chunk: &ImageRegionSection,
) -> ImageToolResult<Vec<RecordField>> {
    let trc = chunk
        .start
        .iter()
        .zip(&chunk.count)
        .zip(&chunk.stride)
        .map(|((&first, &n), &step)| first + (n - 1) * step)
        .collect::<Vec<_>>();
    let (blc, blcf) = position_fields(coordinates, &chunk.start)?;
    let (trc, trcf) = position_fields(coordinates, &trc)?;
    Ok(vec![
        RecordField::new("blc", blc),
        RecordField::new("blcf", blcf),
        RecordField::new("trc", trc),
        RecordField::new("trcf", trcf),
    ])
}

pub(super) fn add_extrema(
    fields: &mut Vec<RecordField>,
    coordinates: &CoordinateSystem,
    chunk: &ImageRegionSection,
    extrema: [Option<Vec<usize>>; 2],
) -> ImageToolResult<()> {
    for (position, (raw_name, world_name)) in extrema
        .into_iter()
        .zip([("minpos", "minposf"), ("maxpos", "maxposf")])
    {
        if let Some(position) = position {
            let absolute = position
                .iter()
                .zip(&chunk.start)
                .zip(&chunk.stride)
                .map(|((&local, &first), &step)| first + local * step)
                .collect::<Vec<_>>();
            let (raw, world) = position_fields(coordinates, &absolute)?;
            fields.push(RecordField::new(raw_name, raw));
            fields.push(RecordField::new(world_name, world));
        }
    }
    Ok(())
}

fn calculate<T: ImagePixel + StatsElement>(
    image: &impl ImageInterface<T>,
    chunk: &ImageRegionSection,
    mut options: StatsOptions<'_>,
) -> ImageToolResult<(RecordValue, Vec<usize>)> {
    let selection = image.sub_image_with_stride(
        chunk.start.clone(),
        chunk.count.clone(),
        chunk.stride.clone(),
    )?;
    let global = options.axes.is_empty() || options.axes.len() == chunk.count.len();
    let lattice_mask = image.get_mask_slice(&chunk.start, &chunk.count, &chunk.stride)?;
    let mask = and_mask(lattice_mask, options.expression.take());
    let mut fields = region_fields(image.coordinates(), chunk)?;
    let report_flux = flux::allowed(image, &options.axes);
    let flux_plan = report_flux
        .then(|| {
            flux::plan(
                image,
                chunk,
                &options.axes,
                global,
                options.spectral_velocity,
            )
        })
        .flatten();
    if !matches!(options.algorithm, StatisticsAlgorithm::Classic)
        && !matches!(options.algorithm, StatisticsAlgorithm::HingesFences { fence } if fence < 0.0)
    {
        let iterations = sampled::collect(
            &selection,
            chunk,
            &options,
            mask.as_ref(),
            flux_plan
                .as_ref()
                .filter(|plan| plan.integral.is_none())
                .map(|plan| &plan.scale),
            &mut fields,
        )?;
        if !report_flux {
            fields.retain(|field| field.name != "flux");
        }
        if let Some(plan) = flux_plan
            && let Some(integral) = &plan.integral
            && !matches!(options.algorithm, StatisticsAlgorithm::Biweight { .. })
            && !fields.iter().any(|field| field.name == "flux")
        {
            let density_options = StatsOptions {
                axes: integral.density_axes.clone(),
                robust: false,
                ..options
            };
            let mut densities = Vec::new();
            sampled::collect(
                &selection,
                chunk,
                &density_options,
                mask.as_ref(),
                Some(&plan.scale),
                &mut densities,
            )?;
            if let Some(flux_field) = densities.into_iter().find(|field| field.name == "flux")
                && let Value::Array(ArrayValue::Float64(arr)) = flux_field.value
            {
                let total = arr.iter().copied().sum::<f64>() * integral.channel_width;
                fields.push(RecordField::new(
                    "flux",
                    Value::Array(ArrayValue::from_f64_vec(vec![total])),
                ));
            }
        }
        return Ok((RecordValue::new(fields), iterations));
    }
    let lattice: &dyn Lattice<T> =
        if chunk.count == image.shape() && chunk.stride.iter().all(|&step| step == 1) {
            image
        } else {
            &selection
        };
    let mut stats = LatticeStatistics::new(lattice);
    if options.disk {
        stats.set_cache_storage(TempStoragePolicy::Paged {
            scratch: ScratchSpace::SystemTemp,
        });
    }
    if options.fits_backed {
        // Keep queued FITS sections bounded while workers reduce their tiles.
        stats.set_execution_resources(ExecutionResources {
            worker_limit: 4,
            prefetch_cap: 2,
            ..ExecutionResources::default()
        });
    }
    if !global {
        stats.set_axes(options.axes.clone());
    }
    if let Some([low, high]) = options.include {
        stats.set_include_range(low, high);
    }
    if let Some([low, high]) = options.exclude {
        stats.set_exclude_range(low, high);
    }
    if let Some(mask) = mask {
        stats.set_pixel_mask(mask);
    }
    let npts = stats.get_statistic(Statistic::Npts)?;
    let empty = !npts.iter().any(|&n| n > 0.0);
    let stats_fields = BASIC
        .iter()
        .chain(QUANTILES.iter().filter(|_| options.robust));
    for &(name, statistic) in stats_fields {
        let value = if empty {
            Value::Array(ArrayValue::from_f64_vec(Vec::new()))
        } else {
            let mut values = stats.get_statistic(statistic)?;
            Zip::from(&mut values).and(&npts).for_each(|value, &count| {
                if count == 0.0 || count.is_nan() {
                    *value = 0.0;
                }
            });
            floating_array(values)
        };
        fields.push(RecordField::new(name, value));
    }
    if empty {
        if report_flux {
            fields.push(RecordField::new(
                "flux",
                Value::Array(ArrayValue::from_f64_vec(Vec::new())),
            ));
        }
    } else {
        if global {
            let (min, max) = stats.get_min_max_pos()?;
            add_extrema(&mut fields, image.coordinates(), chunk, [min, max])?;
        }
        if let Some(plan) = flux_plan {
            let counts = if let Some(integral) = &plan.integral {
                stats.set_axes(integral.density_axes.clone());
                stats.get_statistic(Statistic::Npts)?
            } else {
                npts
            };
            let mut sums = stats.get_statistic(Statistic::Sum)?;
            for (offset, (sum, &count)) in sums
                .view_mut()
                .reversed_axes()
                .iter_mut()
                .zip(counts.reversed_axes().iter())
                .enumerate()
            {
                *sum = if count > 0.0 {
                    *sum * plan.scale.for_plane(offset)
                } else {
                    0.0
                };
            }
            let output = if let Some(integral) = plan.integral {
                Value::Array(ArrayValue::from_f64_vec(vec![
                    sums.sum() * integral.channel_width,
                ]))
            } else {
                floating_array(sums)
            };
            fields.push(RecordField::new("flux", output));
        }
    }
    Ok((RecordValue::new(fields), Vec::new()))
}

/// Computes image statistics on `attached` according to `request`.
pub fn image_statistics(
    attached: &AttachedImage,
    request: &StatisticsRequest<'_>,
) -> ImageToolResult<ImageStatisticsResult> {
    let mut image = attached.lock();
    if matches!(&*image, ImageHandle::Complex(_) | ImageHandle::DComplex(_)) {
        return Err(ImageToolError::new(
            "This method only supports real valued images",
        ));
    }
    let algorithm = parse_statistics_algorithm(request)?;
    // casatools stores its public thresholds as Float even when the
    // underlying image and the reported statistics use Double.
    let single = |limits: [f64; 2]| limits.map(|bound| f64::from(bound as f32));
    let include = request.includepix.map(single);
    let exclude = request.excludepix.map(single);
    if include.is_some() && exclude.is_some() {
        return Err(ImageToolError::new(
            "You can only give one of arguments include or exclude.",
        ));
    }
    let single_precision = matches!(&*image, ImageHandle::Float(_));
    attached.require_pixel_read_lock(&mut image)?;
    let spectral_velocity = SpectralVelocity::from_image(&image);
    let axes = request.axes.clone().unwrap_or_default();
    if axes.iter().any(|&axis| axis >= image.shape().len()) {
        return Err(ImageToolError::new(format!(
            "Invalid cursor axes: {axes:?}."
        )));
    }
    let fits_backed = image.fits().is_some();

    let computed = (|| {
        let shape = image.shape().to_vec();
        let (chunk, membership, (value, iterations), units) = match &*image {
            ImageHandle::Float(storage) => {
                if let Some(fits) = image.fits() {
                    let metadata = match storage {
                        super::handle::Storage::Temp { image, .. } => image.as_ref(),
                        super::handle::Storage::Paged(_) => unreachable!(),
                    };
                    let view = fits.image_view(metadata);
                    run_typed(
                        &view,
                        &shape,
                        request.region,
                        request.mask,
                        request.stretch,
                        axes.clone(),
                        include,
                        exclude,
                        request.robust,
                        request.disk,
                        algorithm,
                        single_precision,
                        fits_backed,
                        spectral_velocity.as_ref(),
                    )?
                } else {
                    with_storage!(storage, img => run_typed(
                        img.as_ref(),
                        &shape,
                        request.region,
                        request.mask,
                        request.stretch,
                        axes.clone(),
                        include,
                        exclude,
                        request.robust,
                        request.disk,
                        algorithm,
                        single_precision,
                        fits_backed,
                        spectral_velocity.as_ref(),
                    )?)
                }
            }
            ImageHandle::Double(storage) => with_storage!(storage, img => run_typed(
                img.as_ref(),
                &shape,
                request.region,
                request.mask,
                request.stretch,
                axes.clone(),
                include,
                exclude,
                request.robust,
                request.disk,
                algorithm,
                single_precision,
                fits_backed,
                spectral_velocity.as_ref(),
            )?),
            _ => unreachable!("complex statistics were rejected"),
        };
        let planes = membership
            .as_ref()
            .and_then(|mask| RegionPlanes::from_membership(mask, &axes));
        Ok::<_, ImageToolError>((value, iterations, chunk, units, planes))
    })();
    let released = image.release_requested_lock();
    let (value, iterations, chunk, units, planes) = computed?;
    released?;

    let mut logs = Vec::new();
    if request.verbose {
        let name = image.path().map_or_else(
            || "Temporary Image".into(),
            |path| path.display().to_string(),
        );
        logs.push(ToolLogLine::info(format!(
            "Determining stats for image {name}"
        )));
    }
    let logfile = request
        .logfile
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_default();
    if request.list || request.verbose || !logfile.is_empty() {
        StatisticsReport {
            coordinates: image.coordinates(),
            spectral_velocity: spectral_velocity.as_ref(),
            chunk: &chunk,
            collapsed_axes: &axes,
            values: &value,
            iterations: &iterations,
            units: &units,
            algorithm,
        }
        .publish(
            &logfile,
            request.append,
            request.list,
            request.verbose,
            &mut logs,
        );
    }
    let mut value = value;
    if let Some(planes) = planes {
        planes.compact(&mut value);
    }
    if request.robust && matches!(algorithm, StatisticsAlgorithm::Biweight { .. }) {
        logs.push(ToolLogLine::warning(
            "The biweight algorithm does not support quantile-related statistics.",
        ));
    }
    Ok(ImageStatisticsResult::from_record(value, logs))
}

type TypedStatsResult = (
    ImageRegionSection,
    Option<ArrayD<bool>>,
    (RecordValue, Vec<usize>),
    String,
);

#[allow(clippy::too_many_arguments)]
fn run_typed<T: ImagePixel + StatsElement>(
    view: &impl ImageInterface<T>,
    shape: &[usize],
    region: Option<SubimageRegion<'_>>,
    mask: Option<&str>,
    stretch: bool,
    axes: Vec<usize>,
    include: Option<[f64; 2]>,
    exclude: Option<[f64; 2]>,
    robust: bool,
    disk: bool,
    algorithm: StatisticsAlgorithm,
    single_precision: bool,
    fits_backed: bool,
    spectral_velocity: Option<&SpectralVelocity>,
) -> ImageToolResult<TypedStatsResult> {
    let chunk = ImageRegionSection::resolve(view, region, true)?;
    let membership = chunk.membership.clone();
    let expression = match mask {
        Some(expr) if !expr.is_empty() => Some(evaluate_image_tool_mask_section(
            expr,
            shape,
            view.coordinates(),
            (&chunk.start, &chunk.count, &chunk.stride),
            stretch,
        )?),
        _ => None,
    };
    let options = StatsOptions {
        axes,
        include,
        exclude,
        robust,
        disk,
        expression: and_mask(membership.clone(), expression),
        algorithm,
        single_precision,
        fits_backed,
        spectral_velocity,
    };
    let res = calculate(view, &chunk, options)?;
    let units = view.units().to_owned();
    Ok((chunk, membership, res, units))
}

/// Convenience helper for extracting pure log lines from a statistics result.
pub fn stats_logs(result: &ImageStatisticsResult) -> &[ToolLogLine] {
    &result.logs
}
