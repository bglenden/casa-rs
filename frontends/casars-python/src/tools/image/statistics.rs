// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType, format_world_display};
use casa_core::Value;
use casa_core::measures::MeasFrame;
use casa_core::measures::doppler::{DopplerRef, MDoppler};
use casa_core::quanta::Unit;
use casa_images::analysis::evaluate_image_tool_mask_section;
use casa_images::{ImageInterface, ImagePixel};
use casa_lattices::{
    ExecutionResources, FitHalfCenter, Lattice, LatticeStatistics, ScratchSpace, Statistic,
    StatisticsAlgorithm, StatsElement, TempStoragePolicy,
};
use ndarray::{ArrayD, Zip};
use pyo3::prelude::*;

use crate::convert::holder::C_MPS;
use crate::convert::{DoubleList, IntList, Variant, value::CasaValue};
use crate::tools::coordsys::stored_velocity_settings;
use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::pixels::{Chunk, and_mask, collapse_axes};
use super::region_read::{read_region_argument, read_region_section};
use super::subimage::Region;

mod flux;
mod region_planes;
mod report;
mod sampled;

use self::region_planes::RegionPlanes;

const BASIC: [(&str, Statistic); 8] = [
    ("npts", Statistic::Npts),
    ("sum", Statistic::Sum),
    ("sumsq", Statistic::SumSq),
    ("min", Statistic::Min),
    ("max", Statistic::Max),
    ("mean", Statistic::Mean),
    ("rms", Statistic::Rms),
    ("sigma", Statistic::Sigma),
];
const QUANTILES: [(&str, Statistic); 5] = [
    ("median", Statistic::Median),
    ("medabsdevmed", Statistic::MedAbsDevMed),
    ("q1", Statistic::Q1),
    ("q3", Statistic::Q3),
    ("quartile", Statistic::Quartile),
];

fn range(values: Option<DoubleList>, name: &str) -> PyResult<Option<[f64; 2]>> {
    let Some(DoubleList(values)) = values else {
        return Ok(None);
    };
    match values.as_slice() {
        [] | [-1.0] => Ok(None),
        [value] => Ok(Some([-value.abs(), value.abs()])),
        [first, second] => Ok(Some([first.min(*second), first.max(*second)])),
        _ => Err(raised(format!("Too many elements for argument {name}."))),
    }
}

fn stats_mask(value: Option<&Bound<'_, PyAny>>) -> PyResult<String> {
    match value.map(|value| value.extract::<Variant>()).transpose()? {
        None | Some(Variant::Empty | Variant::Bool(false)) => Ok(String::new()),
        Some(Variant::Text(text)) if matches!(text.as_str(), "false" | "[]") => Ok(String::new()),
        Some(Variant::Text(text)) => Ok(text),
        Some(Variant::Bool(true)) => Ok("true".into()),
        _ => Err(raised("Mask is not understood, try a valid LEL string")),
    }
}

fn floating_array(array: ArrayD<f64>) -> CasaValue {
    let mut shape: Vec<_> = array.shape().iter().copied().filter(|&n| n != 1).collect();
    if shape.is_empty() {
        shape.push(1);
    }
    CasaValue::Floats {
        shape,
        values: array.reversed_axes().iter().copied().collect(),
    }
}

fn position_fields(
    coordinates: &CoordinateSystem,
    pixel: &[usize],
) -> PyResult<(CasaValue, CasaValue)> {
    let world_pixel: Vec<_> = pixel.iter().map(|&value| value as f64).collect();
    Ok((
        CasaValue::Ints {
            shape: vec![pixel.len()],
            values: pixel.iter().map(|&value| value as i64).collect(),
        },
        CasaValue::Text(format_world_display(coordinates, &world_pixel).map_err(raised)?),
    ))
}

struct SpectralVelocity {
    unit: Unit,
    convention: DopplerRef,
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

    fn velocity_for_ratio(&self, ratio: f64) -> Option<f64> {
        let frame = MeasFrame::new();
        let shift = MDoppler::new(ratio, DopplerRef::RATIO)
            .convert_to(DopplerRef::BETA, &frame)
            .ok()?
            .convert_to(self.convention, &frame)
            .ok()?;
        Some(shift.value() * C_MPS / self.unit.val().factor)
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

fn calculate<T: ImagePixel + StatsElement>(
    image: &impl ImageInterface<T>,
    chunk: &Chunk,
    mut options: StatsOptions<'_>,
) -> PyResult<(CasaValue, Vec<usize>)> {
    let selection = image
        .sub_image_with_stride(
            chunk.start.clone(),
            chunk.size.clone(),
            chunk.stride.clone(),
        )
        .map_err(raised)?;
    let global = options.axes.is_empty() || options.axes.len() == chunk.size.len();
    let mask = and_mask(chunk.mask(image)?, options.expression.take());
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
            fields.retain(|(name, _)| *name != "flux");
        }
        if let Some(plan) = flux_plan
            && let Some(integral) = &plan.integral
            && !matches!(options.algorithm, StatisticsAlgorithm::Biweight { .. })
            && !fields.iter().any(|(name, _)| *name == "flux")
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
            if let Some((_, CasaValue::Floats { values, .. })) =
                densities.into_iter().find(|(name, _)| *name == "flux")
            {
                fields.push((
                    "flux",
                    CasaValue::floats(vec![
                        values.into_iter().sum::<f64>() * integral.channel_width,
                    ]),
                ));
            }
        }
        return Ok((CasaValue::record(fields), iterations));
    }
    let lattice: &dyn Lattice<T> =
        if chunk.size == image.shape() && chunk.stride.iter().all(|&step| step == 1) {
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
    let npts = stats.get_statistic(Statistic::Npts).map_err(raised)?;
    let empty = !npts.iter().any(|&n| n > 0.0);
    let stats_fields = BASIC
        .iter()
        .chain(QUANTILES.iter().filter(|_| options.robust));
    for &(name, statistic) in stats_fields {
        let value = if empty {
            CasaValue::floats(Vec::new())
        } else {
            let mut values = stats.get_statistic(statistic).map_err(raised)?;
            Zip::from(&mut values).and(&npts).for_each(|value, &count| {
                if count == 0.0 || count.is_nan() {
                    *value = 0.0;
                }
            });
            floating_array(values)
        };
        fields.push((name, value));
    }
    if empty {
        if report_flux {
            fields.push(("flux", CasaValue::floats(Vec::new())));
        }
    } else {
        if global {
            let (min, max) = stats.get_min_max_pos().map_err(raised)?;
            add_extrema(&mut fields, image.coordinates(), chunk, [min, max])?;
        }
        if let Some(plan) = flux_plan {
            let counts = if let Some(integral) = &plan.integral {
                stats.set_axes(integral.density_axes.clone());
                stats.get_statistic(Statistic::Npts).map_err(raised)?
            } else {
                npts
            };
            let mut sums = stats.get_statistic(Statistic::Sum).map_err(raised)?;
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
                CasaValue::floats(vec![sums.sum() * integral.channel_width])
            } else {
                floating_array(sums)
            };
            fields.push(("flux", output));
        }
    }
    Ok((CasaValue::record(fields), Vec::new()))
}

fn region_fields(
    coordinates: &CoordinateSystem,
    chunk: &Chunk,
) -> PyResult<Vec<(&'static str, CasaValue)>> {
    let trc = chunk
        .start
        .iter()
        .zip(&chunk.size)
        .zip(&chunk.stride)
        .map(|((&first, &n), &step)| first + (n - 1) * step)
        .collect::<Vec<_>>();
    let (blc, blcf) = position_fields(coordinates, &chunk.start)?;
    let (trc, trcf) = position_fields(coordinates, &trc)?;
    Ok(vec![
        ("blc", blc),
        ("blcf", blcf),
        ("trc", trc),
        ("trcf", trcf),
    ])
}

fn add_extrema(
    fields: &mut Vec<(&'static str, CasaValue)>,
    coordinates: &CoordinateSystem,
    chunk: &Chunk,
    extrema: [Option<Vec<usize>>; 2],
) -> PyResult<()> {
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
            fields.extend([(raw_name, raw), (world_name, world)]);
        }
    }
    Ok(())
}

fn algorithm(
    name: &str,
    clmethod: &str,
    fence: f64,
    center: &str,
    lside: bool,
    clipping: (f64, i64),
    niter: i64,
) -> PyResult<StatisticsAlgorithm> {
    let name = name.to_ascii_lowercase();
    let (zscore, maxiter) = clipping;
    if name.starts_with("cl") {
        if !clmethod.to_ascii_lowercase().starts_with(['a', 't', 'f']) {
            return Err(raised(format!("Unsupported classical method {clmethod}")));
        }
        Ok(StatisticsAlgorithm::Classic)
    } else if name.starts_with("ch") {
        Ok(StatisticsAlgorithm::Chauvenet { zscore, maxiter })
    } else if name.starts_with('b') {
        Ok(StatisticsAlgorithm::Biweight { niter })
    } else if name.starts_with('h') {
        Ok(StatisticsAlgorithm::HingesFences { fence })
    } else if name.starts_with('f') {
        let lower = center.to_ascii_lowercase();
        let center = if lower.starts_with("mea") {
            FitHalfCenter::Mean
        } else if lower.starts_with("med") {
            FitHalfCenter::Median
        } else if lower.starts_with('z') {
            FitHalfCenter::Zero
        } else {
            return Err(raised(format!("Unsupported center value {center}")));
        };
        Ok(StatisticsAlgorithm::FitToHalf {
            center,
            upper: !lside,
        })
    } else {
        Err(raised(format!("Unsupported algorithm {name}")))
    }
}

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (axes=None, region=None, mask=None, includepix=None, excludepix=None, list=false, force=false, disk=false, robust=false, verbose=false, stretch=false, logfile="", append=true, algorithm="classic", fence=-1.0, center="mean", lside=true, zscore=-1.0, maxiter=-1, clmethod="auto", niter=3), text_signature = "($self, axes=[-1], region='', mask='', includepix=[-1.0], excludepix=[-1.0], list=False, force=False, disk=False, robust=False, verbose=False, stretch=False, logfile='', append=True, algorithm='classic', fence=-1.0, center='mean', lside=True, zscore=-1.0, maxiter=-1, clmethod='auto', niter=3)")]
    fn statistics(
        &self,
        py: Python<'_>,
        axes: Option<IntList>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        includepix: Option<DoubleList>,
        excludepix: Option<DoubleList>,
        list: bool,
        force: bool,
        disk: bool,
        robust: bool,
        verbose: bool,
        stretch: bool,
        logfile: &str,
        append: bool,
        algorithm: &str,
        fence: f64,
        center: &str,
        lside: bool,
        zscore: f64,
        maxiter: i64,
        clmethod: &str,
        niter: i64,
    ) -> PyResult<Py<PyAny>> {
        let _ = force;
        let region = read_region_argument(py, region, "statistics");
        let expression = stats_mask(mask);
        let (value, log) = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok((CasaValue::empty_record(), Vec::new()));
            };
            let mut image = attachment.lock();
            if matches!(&*image, ImageHandle::Complex(_) | ImageHandle::DComplex(_)) {
                return Err(raised("This method only supports real valued images"));
            }
            let algorithm = self::algorithm(
                algorithm,
                clmethod,
                fence,
                center,
                lside,
                (zscore, maxiter),
                niter,
            )?;
            // casatools stores its public thresholds as Float even when the
            // underlying image and the reported statistics use Double.
            let single = |limits: [f64; 2]| limits.map(|bound| f64::from(bound as f32));
            let include = range(includepix, "include")?.map(single);
            let exclude = range(excludepix, "exclude")?.map(single);
            if include.is_some() && exclude.is_some() {
                return Err(raised(
                    "You can only give one of arguments include or exclude.",
                ));
            }
            let single_precision = matches!(&*image, ImageHandle::Float(_));
            let region = region?;
            let mask = expression?;
            attachment.require_pixel_read_lock(&mut image)?;
            let spectral_velocity = SpectralVelocity::from_image(&image);
            let result = (|| {
                let shape = image.shape();
                let axes = axes.map_or_else(Vec::new, |arg| arg.0);
                if axes != [-1]
                    && axes
                        .iter()
                        .any(|&axis| axis < 0 || axis as usize >= shape.len())
                {
                    return Err(raised(format!("Invalid cursor axes: {axes:?}.")));
                }
                let axes = collapse_axes(&axes, shape.len())?;
                let (chunk, membership) = read_region_section(&image, region.as_ref().map(Region::as_region), true)?;
                let planes = membership.as_ref().and_then(|mask| RegionPlanes::from_membership(mask, &axes));
                let expression = if mask.is_empty() {
                    None
                } else {
                    Some(
                        evaluate_image_tool_mask_section(
                            &mask,
                            shape,
                            image.coordinates(),
                            (&chunk.start, &chunk.size, &chunk.stride),
                            stretch,
                        )
                        .map_err(raised)?,
                    )
                };
                let options = StatsOptions {
                    axes: axes.clone(),
                    include,
                    exclude,
                    robust,
                    disk,
                    expression: and_mask(membership, expression),
                    algorithm,
                    single_precision,
                    fits_backed: image.fits().is_some(),
                    spectral_velocity: spectral_velocity.as_ref(),
                };
                let (value, units) = match &*image {
                    ImageHandle::Float(Storage::Temp {
                        image,
                        fits: Some(fits),
                        ..
                    }) => {
                        let view = fits.image_view(image);
                        (calculate(&view, &chunk, options), view.units().to_owned())
                    }
                    ImageHandle::Float(storage) => {
                        with_storage!(storage, image => (calculate(image.as_ref(), &chunk, options), image.units().to_owned()))
                    }
                    ImageHandle::Double(storage) => {
                        with_storage!(storage, image => (calculate(image.as_ref(), &chunk, options), image.units().to_owned()))
                    }
                    _ => unreachable!("complex statistics were rejected"),
                };
                Ok::<_, PyErr>((value?, chunk, axes, units, planes))
            })();
            let released = image.release_requested_lock();
            let ((mut value, iterations), chunk, axes, units, planes) = result?;
            released?;
            let mut log = Vec::new();
            if verbose {
                let name = image.path().map_or_else(
                    || "Temporary Image".into(),
                    |path| path.display().to_string(),
                );
                log.push(LogLine::info(format!("Determining stats for image {name}")));
            }
            if list || verbose || !logfile.is_empty() {
                report::StatisticsReport {
                    coordinates: image.coordinates(),
                    spectral_velocity: spectral_velocity.as_ref(),
                    chunk: &chunk,
                    collapsed_axes: &axes,
                    values: &value,
                    iterations: &iterations,
                    units: &units,
                    algorithm,
                }
                .publish(logfile, append, list, verbose, &mut log);
            }
            if let Some(planes) = planes {
                planes.compact(&mut value);
            }
            if robust && matches!(algorithm, StatisticsAlgorithm::Biweight { .. }) {
                log.push(LogLine::warning(
                    "The biweight algorithm does not support quantile-related statistics.",
                ));
            }
            Ok::<_, PyErr>((value, log))
        })?;
        emit(py, "casars.tools.image", &log)?;
        value.into_python(py)
    }
}
