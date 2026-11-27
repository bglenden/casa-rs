// SPDX-License-Identifier: LGPL-3.0-or-later

mod deconvolution;
mod estimate_seed;
mod estimates;
mod model;
mod planes;
mod products;
mod record;
mod samples;
mod summary;
mod uncertainty;

use std::path::Path;

use casa_components::ComponentList;
use casa_coordinates::{Coordinate, CoordinateModel, CoordinateType};
use casa_core::measures::frequency::{FrequencyRef, MFrequency};
use casa_core::quanta::{Quantity, Unit, UnitVal};
use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};

use crate::analysis::lel_mask::PreparedMask;
use casa_images::{ImageBeamSet, ImageError, ImageInterface, ImagePixel};

use self::estimates::Estimates;
use self::model::{FitComponent, FitGeometry};
use self::planes::{FitPlane, FitSelection};
use self::record::quantity;
use self::samples::{FitStatistics, IntensityRange};
use self::summary::{SummaryRow, format_summary};
use self::uncertainty::Noise;
use super::SubimageRegion;
use super::decompose::{SkyFit, evaluate_sky, fit_sky};
use super::findsources::components::SkyPlane;
use super::read_window::PixelSections;

/// A Float or Double input that CASA's component fitter reads as Float.
pub trait ComponentFitPixel: ImagePixel {
    /// Convert a stored sample to the Float fitting grid.
    fn fitting_value(self) -> f32;
}

impl ComponentFitPixel for f32 {
    fn fitting_value(self) -> f32 {
        self
    }
}

impl ComponentFitPixel for f64 {
    fn fitting_value(self) -> f32 {
        self as f32
    }
}

/// A noise or RMS parameter expressed in the image's units or as a quantity.
pub enum ComponentFitQuantity<'a> {
    /// RMS in input brightness units, or noise correlation width in pixels.
    Numeric(f64),
    /// RMS in a brightness unit, or the noise correlation angle.
    Quantity(&'a Quantity),
}

/// Selection, initial estimates and noise settings for Gaussian component fitting.
pub struct FitComponentsOptions<'a> {
    /// Optional pixel bounding box.
    pub pixel_box: Option<&'a str>,
    /// Optional image region.
    pub region: Option<SubimageRegion<'a>>,
    /// Optional contiguous channel range.
    pub channels: Option<&'a str>,
    /// Optional Stokes plane.
    pub stokes: Option<&'a str>,
    /// Optional Boolean image expression.
    pub mask: Option<&'a str>,
    /// Output artifact paths protected against aliasing the fitted mask.
    pub destinations: &'a [&'a Path],
    /// Broadcast a compatible singleton-axis expression mask.
    pub stretch: bool,
    /// Pixel values admitted to the fit.
    pub include_pixels: &'a [f64],
    /// Pixel values excluded from the fit.
    pub exclude_pixels: &'a [f64],
    /// Optional initial elliptical component estimates and fixed parameters.
    pub estimates: Option<&'a Path>,
    /// Initial constant level and whether to hold it fixed.
    pub zero_level: Option<(f64, bool)>,
    /// Restoring beams visible to the caller, if different from storage metadata.
    pub beams: Option<&'a ImageBeamSet>,
    /// Override the residual RMS in the uncertainty calculation.
    pub rms: Option<ComponentFitQuantity<'a>>,
    /// Collect per-plane statistics for the optional text report.
    pub report_log: bool,
    /// Correlation width for the uncertainty calculation.
    pub noise_fwhm: Option<ComponentFitQuantity<'a>>,
}

struct ModelPlane {
    channel: usize,
    model: Option<SkyFit>,
    statistics: Option<FitStatistics>,
}

/// Fitted Gaussian components, their public records and model planes.
pub struct ImageComponentFit {
    /// Convolved components in channel order, suitable for native persistence.
    pub components: ComponentList,
    /// CASA result record including convergence and optional zero-level estimates.
    pub record: RecordValue,
    selection: FitSelection,
    planes: Vec<ModelPlane>,
    summary: Vec<SummaryRow>,
    direction_frame: String,
    brightness: String,
    spectral: bool,
    correlated: bool,
    input_estimates: String,
}

impl ImageComponentFit {
    /// Fitted component estimates from the last converged plane, in the
    /// original input pixel grid.
    pub fn estimates_text(&self) -> String {
        use std::fmt::Write;

        let Some(last) = self.summary.last() else {
            return String::new();
        };
        let mut output = String::new();
        let Value::Record(components) = self.record.get("results").expect("fit results") else {
            unreachable!("fit result component list")
        };
        for (index, row) in self
            .summary
            .iter()
            .enumerate()
            .filter(|(_, row)| row.channel == last.channel)
        {
            let name = format!("component{index}");
            let Some(Value::Record(record)) = components.get(&name) else {
                continue;
            };
            let Some(Value::Array(ArrayValue::Float64(pixel))) = record.get("pixelcoords") else {
                continue;
            };
            let [x, y] = [pixel[ndarray::IxDyn(&[0])], pixel[ndarray::IxDyn(&[1])]];
            let [major, minor, angle, ..] = row.convolved;
            let _ = writeln!(
                output,
                "{}, {x}, {y}, {} arcsec, {} arcsec, {} deg",
                row.peak[0],
                major.to_degrees() * 3600.,
                minor.to_degrees() * 3600.,
                angle.to_degrees()
            );
        }
        output
    }

    /// A fit report with the estimates, fitted measurements and convergence.
    pub fn log_text(&self) -> String {
        use std::fmt::Write;

        let mut text = String::new();
        if !self.input_estimates.is_empty() {
            text.push_str("Estimates file contents\n");
            text.push_str(&self.input_estimates);
            text.push('\n');
        }
        let zero_level = self.record.get("zerooff").is_some();
        for plane in &self.planes {
            let _ = writeln!(
                text,
                "*** Details of fit for channel number {}",
                plane.channel
            );
            if let Some(statistics) = &plane.statistics {
                let _ = writeln!(text, "Number of pixels used in fit: {}", statistics.used);
                let _ = writeln!(
                    text,
                    "Input and residual image statistics (to be used as a rough guide only as to goodness of fit)"
                );
                for (name, stats) in [
                    ("input", &statistics.input),
                    ("residual", &statistics.residual),
                ] {
                    let _ = writeln!(
                        text,
                        "       --- Standard deviation of {name} image: {} {}",
                        stats.sigma(),
                        self.brightness
                    );
                    let _ = writeln!(
                        text,
                        "       --- RMS of {name} image: {} {}",
                        stats.rms(),
                        self.brightness
                    );
                }
            }
            if let Some(model) = &plane.model {
                if zero_level {
                    let _ = writeln!(
                        text,
                        "Zero level offset fit: {} +/- {} {}",
                        model.offset, model.offset_error, self.brightness
                    );
                }
            } else {
                let _ = writeln!(text, "Fit did not converge");
            }
        }
        text.push_str(&self.summary_text());
        text
    }

    /// CASA's text summary for the fitted components.
    pub fn summary_text(&self) -> String {
        let Some(first) = self.components.get(0) else {
            return String::new();
        };
        let stokes = if self.selection.region.stokes_axis.is_some() {
            self.selection.stokes.name()
        } else {
            ""
        };
        format_summary(
            self.summary.iter().copied(),
            &self.direction_frame,
            stokes,
            first.flux().unit().name(),
            &self.brightness,
            self.correlated,
            self.spectral,
        )
    }
}

/// Fit Gaussian components on one polarization and a contiguous channel range.
///
/// Fits use all valid pixels admitted by the pixel selection. If no estimate
/// file is used, CASA estimates one elliptical component independently for
/// each channel. An optional constant offset follows the fit between planes.
///
/// # Errors
/// Returns an error for invalid selectors, estimates, units or failed image reads.
pub fn fit_image_components<T: ComponentFitPixel>(
    image: &dyn ImageInterface<T>,
    options: FitComponentsOptions<'_>,
) -> Result<ImageComponentFit, ImageError> {
    let selection = FitSelection::resolve(
        image,
        options.pixel_box,
        options.region,
        options.channels,
        options.stokes,
    )?;
    let range = IntensityRange::new(options.include_pixels, options.exclude_pixels)?;
    let image_info = image.image_info()?;
    let beams = options.beams.unwrap_or(&image_info.beam_set);
    let sky_coordinate = image
        .coordinates()
        .find_coordinate(CoordinateType::Direction)
        .expect("direction plane resolved");
    let CoordinateModel::Direction(direction) = image.coordinates().coordinate(sky_coordinate)
    else {
        unreachable!("direction coordinate selected by type")
    };
    let spectral = image
        .coordinates()
        .find_coordinate(CoordinateType::Spectral)
        .and_then(|index| match image.coordinates().coordinate(index) {
            CoordinateModel::Spectral(spectral) => Some(spectral),
            _ => None,
        });
    let brightness = fit_brightness(image.units());
    let increments = direction.increment();
    let pixel_width = increments[0].abs();
    let pixel_per_arcsec: Vec<_> = increments
        .iter()
        .map(|increment| (std::f64::consts::PI / (180. * 3600.) / increment).abs())
        .collect();
    let rms = noise_value(options.rms, brightness, 1.)?.filter(|value| *value > 0.);
    let noise_width = noise_value(options.noise_fwhm, "rad", pixel_width)?;
    let input_estimates = options.estimates.map(Estimates::read).transpose()?;
    let seeds = input_estimates
        .as_ref()
        .map(|estimates| {
            let lower = selection
                .sky_axes
                .map(|axis| selection.region.start[axis] as f64);
            let stride = selection.sky_stride.map(|step| step as f64);
            estimates
                .components
                .iter()
                .map(|estimate| estimate.parameters(direction, lower, stride))
                .collect::<Result<Vec<_>, _>>()
                .map(|parameters| {
                    let fixed = estimates
                        .components
                        .iter()
                        .map(|estimate| estimate.fixed)
                        .collect::<Vec<_>>();
                    (parameters, fixed)
                })
        })
        .transpose()?;
    let reader = PixelSections {
        image,
        selection: &selection.region,
        mask: PreparedMask::new_for_outputs(
            options.mask,
            image.shape(),
            image.coordinates(),
            options.stretch,
            options.destinations.iter().copied(),
        )?,
    };
    let mut progress = FitProgress::default();
    let mut zero_level = options.zero_level;
    for &channel in &selection.channels {
        if !selection.can_fit_plane {
            progress.planes.push(ModelPlane {
                channel,
                model: None,
                statistics: None,
            });
            continue;
        }
        let data = selection.read(&reader, channel, T::fitting_value)?;
        let mut fit = fit_sky(
            data.selected_samples(&range),
            data.shape,
            seeds
                .as_ref()
                .map(|(params, fixed)| (params.as_slice(), fixed.as_slice())),
            zero_level,
        );
        if let Some(model) = &fit {
            let beam = (!beams.is_empty()).then(|| *beams.beam(channel, selection.stokes_pixel));
            let frequency = spectral
                .map(|spectral| {
                    spectral
                        .world_frequency_hz(channel as f64)
                        .map(|hz| MFrequency::new(hz, spectral.world_frequency_ref()))
                })
                .transpose()?
                .unwrap_or_else(|| MFrequency::new(1e9, FrequencyRef::LSRK));
            let geometry = FitGeometry {
                sky: SkyPlane {
                    direction,
                    sky_order: [0, 1],
                    brightness,
                    beam,
                },
                selection: &selection,
                channel,
                frequency,
            };
            let correlation = match noise_width {
                Some(width) => (width >= pixel_width).then_some(width),
                None => beam.map(|beam| (beam.major * beam.minor).sqrt()),
            };
            let noise = Noise {
                rms: rms
                    .unwrap_or_else(|| data.selected_rms(|position| fit_value(model, position))),
                pixel_width,
                correlation,
            };
            let records = export_plane(
                &geometry,
                &data,
                model,
                &noise,
                seeds.as_ref().map(|(_, fixed)| fixed.as_slice()),
            );
            if let Ok(records) = records {
                progress.extend(records)?;
                if let Some(level) = &mut zero_level {
                    level.0 = model.offset;
                }
            } else {
                fit = None;
            }
        }
        let statistics = if options.report_log {
            fit.as_ref()
                .map(|model| data.statistics(&range, |position| fit_value(model, position)))
        } else {
            None
        };
        progress.planes.push(ModelPlane {
            channel,
            model: fit,
            statistics,
        });
    }
    Ok(progress.finish(
        selection,
        pixel_per_arcsec,
        options.zero_level.is_some(),
        direction.conversion_direction_ref().as_str(),
        brightness,
        spectral.is_some(),
        !beams.is_empty(),
        input_estimates.map_or_else(String::new, |estimates| estimates.text),
    ))
}

fn export_plane(
    geometry: &FitGeometry<'_>,
    data: &FitPlane,
    model: &SkyFit,
    noise: &Noise,
    fixed: Option<&[[bool; 6]]>,
) -> Result<Vec<FitComponent>, ImageError> {
    model
        .gaussians
        .iter()
        .enumerate()
        .map(|(i, parameters)| {
            geometry.component(
                data,
                parameters,
                fixed.map_or([false; 6], |fixed| fixed[i]),
                noise,
            )
        })
        .collect()
}

fn fit_value(fit: &SkyFit, position: [f64; 2]) -> f64 {
    fit.gaussians
        .iter()
        .map(|gaussian| evaluate_sky(gaussian, position))
        .sum::<f64>()
        + fit.offset
}

#[derive(Default)]
struct FitProgress {
    convolved: RecordValue,
    deconvolved: RecordValue,
    components: ComponentList,
    planes: Vec<ModelPlane>,
    summary: Vec<SummaryRow>,
}

impl FitProgress {
    fn extend(&mut self, components: Vec<FitComponent>) -> Result<(), ImageError> {
        for component in components {
            let name = format!("component{}", self.components.len());
            self.convolved.upsert(
                &name,
                Value::Record(component.measurement.record(&component.shape, false)?),
            );
            if let Some(shape) = &component.deconvolved {
                self.deconvolved.upsert(
                    &name,
                    Value::Record(component.measurement.record(shape, true)?),
                );
            }
            self.components.push(component.shape);
            self.summary.push(component.summary);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn finish(
        mut self,
        selection: FitSelection,
        pixel_per_arcsec: Vec<f64>,
        use_zero_level: bool,
        frame: &str,
        brightness: &str,
        spectral: bool,
        correlated: bool,
        input_estimates: String,
    ) -> ImageComponentFit {
        let mut record = RecordValue::default();
        let count = Value::Scalar(ScalarValue::Int32(self.components.len() as i32));
        self.convolved.upsert("nelements", count.clone());
        record.upsert("results", Value::Record(self.convolved));
        if !self.deconvolved.is_empty() {
            self.deconvolved.upsert("nelements", count);
            record.upsert("deconvolved", Value::Record(self.deconvolved));
        }
        let converged: Vec<_> = self
            .planes
            .iter()
            .map(|plane| plane.model.is_some())
            .collect();
        record.upsert(
            "converged",
            Value::Array(ArrayValue::from_bool_vec(converged)),
        );
        record.upsert(
            "pixelsperarcsec",
            Value::Array(ArrayValue::from_f64_vec(pixel_per_arcsec)),
        );
        if use_zero_level {
            for (name, values) in [
                (
                    "zerooff",
                    self.planes
                        .iter()
                        .map(|plane| plane.model.as_ref().map_or(f64::NAN, |model| model.offset))
                        .collect(),
                ),
                (
                    "zeroofferr",
                    self.planes
                        .iter()
                        .map(|plane| {
                            plane
                                .model
                                .as_ref()
                                .map_or(f64::NAN, |model| model.offset_error)
                        })
                        .collect(),
                ),
            ] {
                let mut measurement = quantity(0., brightness);
                measurement.upsert("value", Value::Array(ArrayValue::from_f64_vec(values)));
                record.upsert(name, Value::Record(measurement));
            }
        }
        ImageComponentFit {
            components: self.components,
            record,
            selection,
            planes: self.planes,
            summary: self.summary,
            direction_frame: frame.to_owned(),
            brightness: brightness.to_owned(),
            spectral,
            correlated,
            input_estimates,
        }
    }
}

fn fit_brightness(brightness: &str) -> &str {
    let rad2 = Unit::new("rad2").expect("standard unit");
    let local = [("pixel", *rad2.val()), ("beam", *rad2.val())];
    let conformant = Unit::with_local_definitions(brightness, &local).is_ok_and(|unit| {
        ["Jy/rad2", "Jy.m/s/rad2", "K"]
            .iter()
            .any(|name| unit.conformant(&Unit::new(name).expect("standard brightness unit")))
    });
    if conformant { brightness } else { "Jy/pixel" }
}

fn noise_value(
    input: Option<ComponentFitQuantity<'_>>,
    unit: &str,
    numeric_scale: f64,
) -> Result<Option<f64>, ImageError> {
    let Some(input) = input else { return Ok(None) };
    match input {
        ComponentFitQuantity::Numeric(value) => Ok(Some(value * numeric_scale)),
        ComponentFitQuantity::Quantity(quantity) if unit != "rad" && quantity.value() <= 0. => {
            Ok(None)
        }
        ComponentFitQuantity::Quantity(quantity)
            if unit != "rad" && quantity.unit().name().is_empty() =>
        {
            Ok(Some(quantity.value()))
        }
        ComponentFitQuantity::Quantity(quantity) => {
            let local = [("pixel", UnitVal::NODIM), ("beam", UnitVal::NODIM)];
            let unit = Unit::with_local_definitions(unit, &local)
                .map_err(|error| ImageError::CasaSelection(error.to_string()))?;
            let source = Unit::with_local_definitions(quantity.unit().name(), &local)
                .map_err(|error| ImageError::CasaSelection(error.to_string()))?;
            Quantity::with_unit(quantity.value(), source)
                .get_value_in(&unit)
                .map(Some)
                .map_err(|error| ImageError::CasaSelection(error.to_string()))
        }
    }
}
