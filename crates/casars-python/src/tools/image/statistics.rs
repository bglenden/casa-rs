// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType, format_world_display};
use casa_images::{ImageInterface, ImagePixel, analysis::statistics_flux_scale};
use casa_lattices::{
    FitHalfCenter, LatticeStatistics, Statistic, StatisticsAlgorithm, StatsElement,
};
use casa_tables::LockType;
use ndarray::{ArrayD, Zip};
use pyo3::prelude::*;

use crate::convert::{DoubleList, IntList, Variant, value::CasaValue};
use crate::tools::{LogLine, emit};

use super::copy_region::Selection;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::mask_expression::{MaskTarget, evaluate_mask};
use super::pixels::{Chunk, and_mask, collapse_axes};
use super::region_read::region_argument;
use super::{PyImageTool, stub};

mod sampled;

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

struct StatsOptions {
    axes: Vec<usize>,
    include: Option<[f64; 2]>,
    exclude: Option<[f64; 2]>,
    robust: bool,
    expression: Option<ArrayD<bool>>,
    algorithm: StatisticsAlgorithm,
    single_precision: bool,
}

fn calculate<T: ImagePixel + StatsElement>(
    image: &impl ImageInterface<T>,
    chunk: &Chunk,
    mut options: StatsOptions,
) -> PyResult<CasaValue> {
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
    if options.algorithm != StatisticsAlgorithm::Classic {
        let scale = flux_scale(image, chunk, &options.axes, global);
        sampled::collect(
            &selection,
            chunk,
            &options,
            mask.as_ref(),
            scale,
            &mut fields,
        )?;
        return Ok(CasaValue::record(fields));
    }
    let mut stats = LatticeStatistics::new(&selection);
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
        fields.push(("flux", CasaValue::floats(Vec::new())));
    } else {
        if let Some(scale) = flux_scale(image, chunk, &options.axes, global) {
            let mut sums = stats.get_statistic(Statistic::Sum).map_err(raised)?;
            Zip::from(&mut sums).and(&npts).for_each(|sum, &count| {
                *sum = if count > 0.0 { *sum * scale } else { 0.0 };
            });
            fields.push(("flux", floating_array(sums)));
        }
        if global {
            let (min, max) = stats.get_min_max_pos().map_err(raised)?;
            add_extrema(&mut fields, image.coordinates(), chunk, [min, max])?;
        }
    }
    Ok(CasaValue::record(fields))
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

fn flux_scale<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    chunk: &Chunk,
    axes: &[usize],
    global: bool,
) -> Option<f64> {
    let collapsed = |axis| global || axes.contains(&axis);
    let axis_types = image.axis_types();
    if !axis_types
        .iter()
        .enumerate()
        .any(|(axis, kind)| collapsed(axis) && *kind == CoordinateType::Direction)
        || axis_types.iter().enumerate().any(|(axis, kind)| {
            collapsed(axis)
                && chunk.size[axis] > 1
                && !matches!(kind, CoordinateType::Direction | CoordinateType::Spectral)
        })
    {
        return None;
    }
    let coordinates = image.coordinates();
    let spectral_axis = image
        .find_axis(CoordinateType::Spectral)
        .filter(|&axis| collapsed(axis) && chunk.size[axis] > 1);
    if spectral_axis.is_some()
        && let Some(index) = coordinates.find_coordinate(CoordinateType::Spectral)
        && let CoordinateModel::Spectral(spectral) = coordinates.coordinate(index)
        && !spectral.has_linear_mapping()
    {
        return None;
    }
    let channels =
        spectral_axis.map(|axis| [chunk.start[axis], chunk.start[axis] + chunk.stride[axis]]);
    let beam = image
        .units()
        .contains("/beam")
        .then(|| image.image_info().ok()?.beam_set.single_beam())
        .flatten();
    statistics_flux_scale(coordinates, image.units(), beam, channels)
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
        let _ = (force, append);
        let region = region_argument(py, region, "statistics");
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
            if disk || !logfile.is_empty() || image.fits().is_some() {
                return Err(stub("statistics").unwrap_err());
            }
            let mut include = range(includepix, "include")?;
            let mut exclude = range(excludepix, "exclude")?;
            if include.is_some() && exclude.is_some() {
                return Err(raised(
                    "You can only give one of arguments include or exclude.",
                ));
            }
            let single_precision = matches!(&*image, ImageHandle::Float(_));
            if single_precision {
                let single = |limits: [f64; 2]| limits.map(|bound| f64::from(bound as f32));
                include = include.map(single);
                exclude = exclude.map(single);
            }
            let region = region?;
            let mask = expression?;
            attachment.require_lock(&mut image, LockType::Read)?;
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
                let chunk = Chunk::from(Selection::new(shape, &region, "statistics")?);
                let expression = if mask.is_empty() {
                    None
                } else {
                    Some(evaluate_mask(
                        &mask,
                        MaskTarget {
                            shape,
                            section: Some((&chunk, image.coordinates())),
                            stretch,
                            method: "statistics",
                        },
                    )?)
                };
                let options = StatsOptions {
                    axes,
                    include,
                    exclude,
                    robust,
                    expression,
                    algorithm,
                    single_precision,
                };
                match &*image {
                    ImageHandle::Float(storage) => {
                        with_storage!(storage, image => calculate(image.as_ref(), &chunk, options))
                    }
                    ImageHandle::Double(storage) => {
                        with_storage!(storage, image => calculate(image.as_ref(), &chunk, options))
                    }
                    _ => unreachable!("complex statistics were rejected"),
                }
            })();
            let released = image.release_requested_lock();
            let value = result?;
            released?;
            let mut log = Vec::new();
            if verbose {
                let name = image.path().map_or_else(
                    || "Temporary Image".into(),
                    |path| path.display().to_string(),
                );
                log.push(LogLine::info(format!("Determining stats for image {name}")));
            }
            if list {
                log.push(LogLine::info(format!(
                    "Statistics calculated using {:?} algorithm",
                    algorithm.kind()
                )));
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
