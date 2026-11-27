// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sample a vertical TEC image at each antenna's ionospheric pierce point.

use std::path::Path;

use casa_core::measures::epoch::{EpochRef, MEpoch};
use casa_core::measures::ionospheric_pierce_point;
use casa_images::PagedImage;
use casa_ms::MeasurementSet;
use casa_ms::derived::engine::MsCalEngine;
use casa_tables::RequiredScalarColumnValues::{Float64, Int32};
use ndarray::ArrayD;

use super::{
    GencalError, GencalReport, GencalRequest, PriorCalRow, f32_column, open_measurement_set,
    refuse_prior_input_overlap, write_prior_caltable,
};
use crate::output::{CalibrationOutput, CalibrationOutputMode};

pub(crate) const HEIGHT_KEYWORD: &str = "IonosphereHeight(km)";
pub(crate) const DEFAULT_HEIGHT_KM: f64 = 450.;

pub(super) fn height_km(parameters: &[f64]) -> f64 {
    match parameters {
        [height] => *height,
        _ => DEFAULT_HEIGHT_KM,
    }
}

pub(super) fn generate(
    request: &GencalRequest,
    image: Option<&Path>,
    mode: CalibrationOutputMode,
) -> Result<GencalReport, GencalError> {
    let image = image.ok_or_else(|| invalid("An existing tec map must be specified in infile"))?;
    refuse_prior_input_overlap(request, image)?;
    let ms = open_measurement_set(&request.measurement_set)?;
    // specifycal validates these expressions when assembling its input, then
    // the F Jones algorithm derives the entire time and antenna grid.
    if !matches!(request.spw.as_str(), "" | "*") {
        ms.resolve_spw_selector(&request.spw).map_err(invalid)?;
    }
    if !matches!(request.antenna.as_str(), "" | "*") {
        ms.resolve_specified_antennas(&request.antenna)
            .map_err(invalid)?;
    }
    let height = height_km(&request.parameter);
    if !height.is_finite() {
        return Err(invalid("Ionosphere height in kilometres must be finite"));
    }
    let mut source = TecImage::open(image)?;
    let engine = MsCalEngine::for_calibration(&ms).map_err(invalid)?;
    let samples = samples(&ms)?;
    let mode = if mode == CalibrationOutputMode::Update {
        CalibrationOutputMode::Replace
    } else {
        mode
    };
    let output = CalibrationOutput::prepare(&request.output_table, mode)?;
    let rows = solution_rows(&engine, &mut source, &samples, height * 1000.)?;
    write_prior_caltable(&ms, request, "F Jones", &rows, output, mode, true)
}

fn solution_rows(
    engine: &MsCalEngine,
    source: &mut TecImage,
    samples: &[Sample],
    height_m: f64,
) -> Result<Vec<PriorCalRow>, GencalError> {
    let first = samples
        .first()
        .ok_or_else(|| invalid("MeasurementSet has no TEC solution times"))?;
    let seconds_in_day = 86_400.;
    let origin = if source.absolute_time {
        0.
    } else {
        (first.time / seconds_in_day).floor() * seconds_in_day
    };
    let mut rows = Vec::with_capacity(samples.len() * engine.num_antennas());
    for sample in samples {
        let field = sample.field as usize;
        let direction = engine
            .observation_direction_j2000(sample.time, field)
            .map_err(invalid)?;
        for antenna in 0..engine.num_antennas() {
            let frame = engine
                .antenna_frame(sample.time, antenna)
                .map_err(invalid)?
                .with_epoch(MEpoch::from_mjd_seconds(sample.time, EpochRef::UTC));
            let point = ionospheric_pierce_point(height_m, &direction, &frame).map_err(invalid)?;
            let [x, y, z] = point.as_itrf();
            let longitude = y.atan2(x).to_degrees();
            let latitude = z.atan2(x.hypot(y)).to_degrees();
            let vertical = source.sample([longitude, latitude, sample.time - origin])?;
            let (_, elevation) = engine.azel(sample.time, field, antenna).map_err(invalid)?;
            let zenith = std::f64::consts::FRAC_PI_2 - elevation;
            let line_of_sight = (f64::from(vertical) / zenith.cos()) as f32;
            let electrons = (f64::from(line_of_sight) * 1.0e16) as f32;
            if !electrons.is_finite() {
                return Err(invalid(
                    "TEC image and source elevation produced a nonfinite correction",
                ));
            }
            rows.push(PriorCalRow {
                time_seconds: sample.time,
                interval_seconds: 0.,
                field_id: sample.field,
                spw_id: 0,
                antenna_id: antenna as i32,
                observation_id: sample.observation,
                scan_number: sample.scan,
                parameters: f32_column(&[electrons]),
                snr_value: 100.,
                preserved: None,
            });
        }
    }
    Ok(rows)
}

struct Sample {
    array: i32,
    scan: i32,
    time: f64,
    field: i32,
    description: i32,
    observation: i32,
}

fn samples(ms: &MeasurementSet) -> Result<Vec<Sample>, GencalError> {
    let columns = ms
        .main_table()
        .required_scalar_columns_owned(&[
            "ARRAY_ID",
            "SCAN_NUMBER",
            "TIME",
            "FIELD_ID",
            "DATA_DESC_ID",
            "OBSERVATION_ID",
        ])
        .map_err(invalid)?;
    let (
        Int32(array),
        Int32(scan),
        Float64(time),
        Int32(field),
        Int32(description),
        Int32(observation),
    ) = (
        &columns["ARRAY_ID"],
        &columns["SCAN_NUMBER"],
        &columns["TIME"],
        &columns["FIELD_ID"],
        &columns["DATA_DESC_ID"],
        &columns["OBSERVATION_ID"],
    )
    else {
        return Err(invalid("Invalid MAIN column types for TEC sampling"));
    };
    let mut samples = (0..time.len())
        .map(|row| Sample {
            array: array[row],
            scan: scan[row],
            time: time[row],
            field: field[row],
            description: description[row],
            observation: observation[row],
        })
        .collect::<Vec<_>>();
    // Match the visibility iterator's sorting and first field at a timestamp.
    samples.sort_by(|left, right| {
        (left.array, left.scan)
            .cmp(&(right.array, right.scan))
            .then_with(|| left.time.total_cmp(&right.time))
            .then_with(|| (left.field, left.description).cmp(&(right.field, right.description)))
    });
    samples.dedup_by(|left, right| left.time == right.time);
    Ok(samples)
}

struct TecImage {
    image: PagedImage<f32>,
    absolute_time: bool,
    increment: [f64; 3],
    block: Option<TecBlock>,
}

struct TecBlock {
    start: [usize; 3],
    world: Vec<f64>,
    pixels: ArrayD<f32>,
}

impl TecImage {
    fn open(path: &Path) -> Result<Self, GencalError> {
        let image = PagedImage::<f32>::open(path).map_err(invalid)?;
        let coordinates = image.coordinates().reference_value();
        if coordinates.len() != 3
            || image.shape().len() != 3
            || image.shape().iter().any(|&n| n < 2)
        {
            return Err(invalid(
                "TEC image must have longitude, latitude and time axes with at least two pixels each",
            ));
        }
        let coordinates_system = image.coordinates();
        let mut increment = [0.; 3];
        for (axis, step) in increment.iter_mut().enumerate() {
            let (owner, local) = coordinates_system
                .world_axis_owner(axis)
                .ok_or_else(|| invalid("TEC image is missing a world axis"))?;
            *step = coordinates_system.coordinate(owner).increment()[local];
            if !step.is_finite() || *step == 0. {
                return Err(invalid(
                    "TEC image axes must have finite nonzero increments",
                ));
            }
        }
        Ok(Self {
            image,
            absolute_time: coordinates[2] > 0.,
            increment,
            block: None,
        })
    }

    fn sample(&mut self, world: [f64; 3]) -> Result<f32, GencalError> {
        let pixel = self.image.coordinates().to_pixel(&world).map_err(invalid)?;
        let mut start = [0; 3];
        for axis in 0..3 {
            let corner = pixel[axis].trunc();
            if !corner.is_finite()
                || corner < 0.0
                || corner as usize >= self.image.shape()[axis] - 1
            {
                return Err(invalid(format!(
                    "TEC image does not cover longitude, latitude and epoch {world:?}"
                )));
            }
            start[axis] = corner as usize;
        }
        if self.block.as_ref().is_none_or(|block| block.start != start) {
            let pixels = self.image.get_slice(&start, &[2; 3]).map_err(invalid)?;
            let point = start.map(|index| index as f64);
            let world = self.image.coordinates().to_world(&point).map_err(invalid)?;
            self.block = Some(TecBlock {
                start,
                world,
                pixels,
            });
        }
        let block = self.block.as_ref().expect("TEC interpolation block");
        let values = &block.pixels;
        let [x, y, t] = std::array::from_fn(|axis| {
            f64::from(((world[axis] - block.world[axis]) / self.increment[axis]) as f32)
        });
        let plane = |time| {
            ((1. - x) * (1. - y) * f64::from(values[[0, 0, time]])
                + x * (1. - y) * f64::from(values[[1, 0, time]])
                + x * y * f64::from(values[[1, 1, time]])
                + (1. - x) * y * f64::from(values[[0, 1, time]])) as f32
        };
        Ok(((1. - t) * f64::from(plane(0)) + t * f64::from(plane(1))) as f32)
    }
}

fn invalid(error: impl std::fmt::Display) -> GencalError {
    GencalError::InvalidRequest(format!("Error in Calibrater::specifycal. {error}"))
}
