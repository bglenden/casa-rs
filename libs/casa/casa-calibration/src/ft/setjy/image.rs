// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare CASA calibrator images for the shared Fourier predictor.

use std::path::Path;

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateType, SpectralCoordinate, StokesType,
};
use casa_core::measures::frame::MeasFrame;
use casa_core::measures::frequency::{FrequencyRef, MFrequency};
use casa_core::measures::{direction::MDirection, position::MPosition};
use casa_core::quanta::{Quantity, Unit};
use casa_core::{ArrayValue, ScalarValue, Value};
use casa_images::PagedImage;
use casa_ms::{MeasurementSet, MsResult, ResolvedMsSelection};

use crate::ft::image::{ImageModel, prepare_image};
use crate::ft::{invalid, spectral_with_runtime};

/// Frequency grid used to scale a setjy calibrator image.
pub struct ImageFrequencies {
    /// Model sampling frequencies in hertz.
    pub samples: Vec<f64>,
    span: f64,
    /// Frequency reference of the first selected spectral window, used when converting native image
    /// frequencies.
    pub first_window_reference: FrequencyRef,
}

impl ImageFrequencies {
    /// Builds the model grid from a positive lower frequency, upper frequency, and step in hertz.
    /// Invalid ranges and nonfinite or oversized grids return an error.
    pub fn new(
        min: f64,
        max: f64,
        step: f64,
        first_window_reference: FrequencyRef,
    ) -> MsResult<Self> {
        let span = max - min;
        let intervals = (span / step).ceil();
        if !min.is_finite()
            || min <= 0.
            || !intervals.is_finite()
            || !(0. ..i32::MAX as f64).contains(&intervals)
        {
            return Err(invalid(
                "invalid setjy image frequency range or channel width",
            ));
        }
        let samples = match intervals as usize {
            0 => vec![min],
            1 => vec![min, max],
            2 => vec![min - step, min, min + step],
            intervals => (0..=intervals)
                .map(|index| min + index as f64 * step)
                .collect(),
        };
        Ok(Self {
            samples,
            span,
            first_window_reference,
        })
    }

    /// Returns the CASA image scaling reference frequency in hertz, using the middle samples of the
    /// grid.
    pub fn center(&self) -> f64 {
        let count = self.samples.len();
        let lower = self.samples[(count - 1) / 2];
        if count.is_multiple_of(2) && count <= 100 {
            return 0.5 * (lower + self.samples[count / 2]);
        }
        lower
    }

    fn record_scale(&self, model: &mut ImageModel, scales: &[f64]) {
        let record = &mut model.entry.container;
        record.upsert(
            "name",
            Value::Scalar(ScalarValue::String("SetJyGridFT".into())),
        );
        record.upsert(
            "freqscale",
            Value::Array(ArrayValue::from_f64_vec(self.samples.clone())),
        );
        record.upsert(
            "scaleamp",
            Value::Array(ArrayValue::from_f64_vec(scales.to_vec())),
        );
    }
}

/// Requested Stokes I normalization of an image model, in janskys.
pub enum ImageFlux {
    /// One normalization across the frequency grid, or None to retain native scaling.
    Constant(Option<f64>),
    /// One normalization per sampled frequency, or None to retain native scaling.
    PerChannel(Option<Vec<f64>>),
}

/// Prepares image models for the selected MeasurementSet rows, with flux normalization in janskys.
/// The input must have two leading direction axes plus Stokes and spectral axes.
pub fn calibrator_images(
    ms: &MeasurementSet,
    selections: &[ResolvedMsSelection],
    path: &Path,
    frequencies: &ImageFrequencies,
    flux: ImageFlux,
    frame: &MeasFrame,
    scratch: bool,
) -> MsResult<(Vec<ImageModel>, Option<f64>)> {
    let input = PagedImage::<f32>::open(path).map_err(invalid)?;
    let mut coordinates = input.coordinates().clone();
    let spectral_index = coordinates.find_coordinate(CoordinateType::Spectral);
    let stokes_index = coordinates.find_coordinate(CoordinateType::Stokes);
    let (Some(spectral_index), Some(stokes_index), [nx, ny, _, _]) =
        (spectral_index, stokes_index, input.shape())
    else {
        return Err(invalid(
            "setjy requires a four-axis direction, Stokes and spectral image",
        ));
    };
    let (nx, ny) = (*nx, *ny);
    let spectral_axis = input
        .find_axis(CoordinateType::Spectral)
        .unwrap_or_default();
    let stokes_axis = input.find_axis(CoordinateType::Stokes).unwrap_or_default();
    if !matches!((spectral_axis, stokes_axis), (2, 3) | (3, 2)) {
        return Err(invalid("the setjy image must have direction axes first"));
    }
    let CoordinateModel::Spectral(spectral) = coordinates.coordinate(spectral_index) else {
        unreachable!("spectral coordinate");
    };
    let rest = spectral.rest_frequency_hz().map_err(invalid)?;
    let planes = input.shape()[stokes_axis];
    let channels = input.shape()[spectral_axis];
    let cube = channels > 1;
    let direction = coordinates
        .find_coordinate(CoordinateType::Direction)
        .ok_or_else(|| invalid("the setjy image has no direction coordinate"))?;
    let CoordinateModel::Direction(direction) = coordinates.coordinate(direction) else {
        unreachable!("direction coordinate");
    };
    let center = direction.reference_pixel();
    let radius = if cube {
        None
    } else {
        calibrator_radius(path, direction)?
    };
    let (output_spectral, input_pixels) = if cube {
        if spectral_axis != 3 {
            return Err(invalid("the spectral axis of a setjy cube must be last"));
        }
        let target = SpectralCoordinate::from_tabular(
            frequencies.first_window_reference,
            (0..frequencies.samples.len())
                .map(|pixel| pixel as f64)
                .collect(),
            frequencies.samples.clone(),
            frequencies.samples[0],
            frequencies
                .samples
                .get(1)
                .map_or(1., |hz| hz - frequencies.samples[0]),
            0.,
            rest,
        )
        .map_err(invalid)?;
        let spectral = spectral_with_runtime(spectral)?;
        let ratio = cube_frame_ratio(
            &coordinates,
            direction,
            frequencies.first_window_reference,
            spectral.frequency_ref(),
            frame,
        )?;
        let input_pixels = frequencies
            .samples
            .iter()
            .map(|&hz| spectral.pixel_at_frequency_hz(hz * ratio).map_err(invalid))
            .collect::<MsResult<Vec<_>>>()?;
        (target, input_pixels)
    } else {
        let target = SpectralCoordinate::new(
            FrequencyRef::LSRK,
            frequencies.center(),
            (2. * frequencies.span).max(1.),
            0.,
            rest,
        );
        (target, vec![0.])
    };
    coordinates.replace_coordinate(spectral_index, output_spectral);
    if spectral_axis == 2 {
        let order = [0, 1, 3, 2];
        let world_order =
            if coordinates.coordinate_world_axes(spectral_index).as_deref() == Some(&[Some(2)]) {
                order
            } else {
                [0, 1, 2, 3]
            };
        coordinates
            .transpose_axes(&world_order, &order)
            .map_err(invalid)?;
    }
    let CoordinateModel::Stokes(stokes) = coordinates.coordinate(stokes_index) else {
        unreachable!("Stokes coordinate");
    };
    let intensity = if planes == 1 {
        Some(0)
    } else {
        stokes
            .stokes()
            .iter()
            .position(|&label| label == StokesType::I)
    };

    let stage = tempfile::tempdir().map_err(invalid)?;
    let staged_path = stage.path().join("model.im");
    let tile = [nx.min(128), ny.min(128)];
    let mut staged = PagedImage::<f32>::create_with_tile_shape(
        vec![nx, ny, planes, input_pixels.len()],
        vec![tile[0], tile[1], 1, 1],
        coordinates,
        &staged_path,
    )
    .map_err(invalid)?;
    let mut sum = 0.;
    for (channel, pixel) in input_pixels.into_iter().enumerate() {
        // ImageRegrid keeps endpoint pixels in the unmasked output cube.
        let pixel = (pixel as f32).clamp(0., (channels - 1) as f32);
        let lower = pixel.floor() as usize;
        let upper = pixel.ceil() as usize;
        let weight = pixel - lower as f32;
        for pol in 0..planes {
            for y in (0..ny).step_by(tile[1]) {
                for x in (0..nx).step_by(tile[0]) {
                    let mut start = [x, y, 0, 0];
                    let shape = [tile[0].min(nx - x), tile[1].min(ny - y), 1, 1];
                    start[stokes_axis] = pol;
                    start[spectral_axis] = lower;
                    let mut pixels = input.get_slice(&start, &shape).map_err(invalid)?;
                    if lower != upper {
                        start[spectral_axis] = upper;
                        let other = input.get_slice(&start, &shape).map_err(invalid)?;
                        pixels.zip_mut_with(&other, |first, second| {
                            *first += (*second - *first) * weight;
                        });
                    }
                    if let Some(radius) = radius {
                        for (index, value) in pixels.indexed_iter_mut() {
                            let dx = (x + index[0]) as f32 - center[0] as f32;
                            let dy = (y + index[1]) as f32 - center[1] as f32;
                            if dx * dx + dy * dy > radius * radius {
                                *value = 0.;
                            }
                        }
                    }
                    if !cube && intensity == Some(pol) {
                        sum += pixels.iter().copied().map(f64::from).sum::<f64>();
                    }
                    staged
                        .put_slice(&pixels, &[x, y, pol, channel])
                        .map_err(invalid)?;
                }
            }
        }
    }
    let sum = f64::from(sum as f32);
    let mut scales = None;
    let mut scalar_flux = None;
    if !cube {
        match flux {
            ImageFlux::Constant(Some(jy)) => {
                scale_image(
                    &mut staged,
                    [nx, ny, planes],
                    tile,
                    normalization(jy, sum)? as f32,
                )?;
                scalar_flux = Some(jy);
            }
            ImageFlux::Constant(None) => {}
            ImageFlux::PerChannel(fluxes) => {
                scales = Some(match fluxes {
                    Some(fluxes) => fluxes
                        .into_iter()
                        .map(|jy| normalization(jy, sum))
                        .collect::<MsResult<_>>()?,
                    None => vec![1.; frequencies.samples.len()],
                });
            }
        }
    }
    staged.save().map_err(invalid)?;
    drop(staged);
    let mut models = Vec::new();
    for selected in selections {
        // CASA grids each window with its own feed and correlation basis.
        if let Some(mut model) = prepare_image(ms, selected, &staged_path, None, scratch, scratch)?
        {
            if let Some(scales) = &scales {
                frequencies.record_scale(&mut model, scales);
            }
            models.push(model);
        }
    }
    Ok((models, scalar_flux))
}

fn cube_frame_ratio(
    coordinates: &casa_coordinates::CoordinateSystem,
    direction: &casa_coordinates::DirectionCoordinate,
    from: FrequencyRef,
    to: FrequencyRef,
    runtime: &MeasFrame,
) -> MsResult<f64> {
    if from == to {
        return Ok(1.);
    }
    let observation = coordinates.obs_info();
    let position = runtime
        .measures()
        .and_then(|provider| MPosition::casa_observatory(&observation.telescope, provider))
        .or_else(|| observation.telescope_position.clone())
        .ok_or_else(|| {
            invalid(format!(
                "the image observatory {:?} is unavailable",
                observation.telescope
            ))
        })?;
    let epoch = observation
        .date
        .clone()
        .ok_or_else(|| invalid("the setjy cube has no observation epoch"))?;
    let [longitude, latitude] = direction.projection_reference();
    let direction = MDirection::from_angles(longitude, latitude, direction.direction_ref());
    let frame = runtime
        .clone()
        .with_epoch(epoch)
        .with_position(position)
        .with_direction(direction);
    // casacore retains the numerical world grid if no frequency machine can be built.
    Ok(MFrequency::new(1., from)
        .convert_to(to, &frame)
        .map_or(1., |frequency| frequency.hz()))
}

fn scale_image(
    image: &mut PagedImage<f32>,
    [nx, ny, planes]: [usize; 3],
    tile: [usize; 2],
    scale: f32,
) -> MsResult<()> {
    for pol in 0..planes {
        for y in (0..ny).step_by(tile[1]) {
            for x in (0..nx).step_by(tile[0]) {
                let start = [x, y, pol, 0];
                let shape = [tile[0].min(nx - x), tile[1].min(ny - y), 1, 1];
                let mut pixels = image.get_slice(&start, &shape).map_err(invalid)?;
                pixels.mapv_inplace(|value| value * scale);
                image.put_slice(&pixels, &start).map_err(invalid)?;
            }
        }
    }
    Ok(())
}

fn normalization(jy: f64, sum: f64) -> MsResult<f64> {
    if sum == 0. || !sum.is_finite() || !jy.is_finite() {
        return Err(invalid(
            "the setjy image needs finite nonzero Stokes I to normalize its flux",
        ));
    }
    Ok(jy / sum)
}

fn calibrator_radius(
    path: &Path,
    direction: &casa_coordinates::DirectionCoordinate,
) -> MsResult<Option<f32>> {
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    let radius = [
        ("3C286", 3.),
        ("3C48", 0.95),
        ("3C147", 0.85),
        ("3C138", 0.75),
    ]
    .into_iter()
    .find(|(prefix, _)| name.starts_with(prefix));
    let Some((_, arcseconds)) = radius else {
        return Ok(None);
    };
    let radians = Unit::new("rad").map_err(invalid)?;
    let unit = Unit::new(&direction.axis_units()[0]).map_err(invalid)?;
    let increment = Quantity::with_unit(direction.increment()[0], unit)
        .get_value_in(&radians)
        .map_err(invalid)?;
    Ok(Some(
        ((arcseconds * std::f64::consts::PI / (180. * 3600.) / increment.abs()) as f32).max(0.5),
    ))
}
