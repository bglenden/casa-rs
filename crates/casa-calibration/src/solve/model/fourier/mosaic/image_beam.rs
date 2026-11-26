// SPDX-License-Identifier: LGPL-3.0-or-later
//! The scalar voltage screen that HetArrayConvFunc takes from a beam image.

use std::path::Path;

use casa_coordinates::{Coordinate, CoordinateModel, CoordinateType};
use casa_core::{ArrayValue, Complex32, RecordValue, Value};
use casa_images::{ImagePixel, PagedImage};
use casa_ms::voltage_pattern::VoltagePatternTable;
use ndarray::{Array2, Axis, Ix2};

use super::{FourierRow, GainSolveError, model_error, record_text};

pub(super) struct ImageBeams {
    records: Vec<RecordValue>,
    pub(super) classes: Vec<u8>,
}

impl ImageBeams {
    pub(super) fn load(path: &Path, row: &FourierRow<'_>) -> Result<Self, GainSolveError> {
        let records = VoltagePatternTable::open(path)
            .and_then(|table| table.definitions())
            .map_err(model_error)?;
        let patterns = records
            .iter()
            .filter(|record| matches!(record_text(record, "name"), Ok("IMAGE")))
            .map(|record| Ok((record, image_key(record)?, antenna_names(record)?)))
            .collect::<Result<Vec<_>, GainSolveError>>()?;
        let antenna = row.ms.antenna().map_err(model_error)?;
        let mut keys = Vec::new();
        let mut selected = Vec::new();
        let mut classes = Vec::with_capacity(antenna.row_count());
        for index in 0..antenna.row_count() {
            let name = antenna.name(index).map_err(model_error)?;
            let mut last = None;
            let mut active = None;
            for &(record, key, names) in &patterns {
                if names.first().is_some_and(|name| name == "*") || names.iter().any(|v| v == &name)
                {
                    last = Some((record, key));
                    if let Some(index) = keys.iter().position(|known| *known == key) {
                        active = Some(index);
                    }
                }
            }
            let (record, key) = last.ok_or_else(|| {
                model_error(format!(
                    "virtual mosaic antenna {name} has no image voltage pattern"
                ))
            })?;
            let class = active.unwrap_or_else(|| {
                keys.push(key);
                selected.push(record.clone());
                keys.len() - 1
            });
            classes.push(u8::try_from(class).map_err(model_error)?);
        }
        Ok(Self {
            records: selected,
            classes,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.records.len()
    }

    pub(super) fn screens(&self, legacy: bool) -> Result<Vec<ImageBeam>, GainSolveError> {
        self.records
            .iter()
            .map(|record| ImageBeam::load(record, legacy))
            .collect()
    }
}

fn image_key(record: &RecordValue) -> Result<&str, GainSolveError> {
    match record.get("realimage") {
        Some(_) => record_text(record, "realimage"),
        None => record_text(record, "compleximage"),
    }
}

fn antenna_names(record: &RecordValue) -> Result<&ndarray::ArrayD<String>, GainSolveError> {
    match record.get("antennanames") {
        Some(Value::Array(ArrayValue::String(names))) => Ok(names),
        _ => Err(model_error("mosaic voltage pattern has no antenna names")),
    }
}

struct ImagePlane {
    voltage: Array2<Complex32>,
    channel_count: usize,
    diagonal: usize,
    increment: [f64; 2],
    reference_pixel: [f64; 2],
    frequency_hz: f64,
}

impl ImagePlane {
    fn load<T: ImagePixel + Copy>(
        path: &str,
        real_diagonal: Option<usize>,
        convert: impl Fn(T) -> Complex32,
    ) -> Result<Self, GainSolveError> {
        let image = PagedImage::<T>::open(path).map_err(model_error)?;
        let &[nx, ny, 4, channels] = image.shape() else {
            return Err(model_error(
                "mosaic voltage image requires four correlation planes",
            ));
        };
        if nx < 2 || ny < 2 || channels == 0 {
            return Err(model_error("empty mosaic voltage image"));
        }
        let coords = image.coordinates();
        if coords.find_coordinate(CoordinateType::Direction) != Some(0)
            || coords.find_coordinate(CoordinateType::Stokes) != Some(1)
            || coords.find_coordinate(CoordinateType::Spectral) != Some(2)
        {
            return Err(model_error("invalid mosaic voltage image axes"));
        }
        let CoordinateModel::Direction(direction) = coords.coordinate(0) else {
            unreachable!()
        };
        let diagonal = if let Some(diagonal) = real_diagonal {
            diagonal
        } else {
            let CoordinateModel::Stokes(stokes) = coords.coordinate(1) else {
                unreachable!()
            };
            diagonal_index(stokes.stokes())?
        };
        let CoordinateModel::Spectral(spectral) = coords.coordinate(2) else {
            unreachable!()
        };
        let values = image
            .get_slice(&[0, 0, diagonal, 0], &[nx, ny, 1, 1])
            .map_err(model_error)?;
        let voltage = values
            .index_axis_move(Axis(3), 0)
            .index_axis_move(Axis(2), 0)
            .into_dimensionality::<Ix2>()
            .map_err(model_error)?
            .mapv(convert);
        Ok(Self {
            voltage,
            channel_count: channels,
            diagonal,
            increment: direction.increment().try_into().expect("direction axes"),
            reference_pixel: direction
                .reference_pixel()
                .try_into()
                .expect("direction axes"),
            frequency_hz: spectral
                .world_frequency_hz(spectral.reference_pixel()[0])
                .map_err(model_error)?,
        })
    }

    fn value(&self, offset: [f64; 2], frequency_hz: f64) -> Complex32 {
        // PBMath2DImage::updateJones replaces the coordinate centre and PC
        // matrix, reverses the longitude increment and scales both axes.
        let [x, y] = [
            self.reference_pixel[0]
                - offset[0] * frequency_hz / (self.increment[0] * self.frequency_hz),
            self.reference_pixel[1]
                + offset[1] * frequency_hz / (self.increment[1] * self.frequency_hz),
        ];
        let [nx, ny] = [self.voltage.nrows(), self.voltage.ncols()];
        // Interpolate2D tests the truncated indices and uses the last
        // interval at the upper edge. A fractional edge pixel can extrapolate.
        if !(x > -1. && x < nx as f64 && y > -1. && y < ny as f64) {
            return Complex32::default();
        }
        let [ix, iy] = [(x as usize).min(nx - 2), (y as usize).min(ny - 2)];
        let [fx, fy] = [(x - ix as f64) as f32, (y - iy as f64) as f32];
        let low = self.voltage[[ix, iy]] * (1. - fx) + self.voltage[[ix + 1, iy]] * fx;
        let high = self.voltage[[ix, iy + 1]] * (1. - fx) + self.voltage[[ix + 1, iy + 1]] * fx;
        low * (1. - fy) + high * fy
    }
}

fn diagonal_index(stokes: &[casa_coordinates::StokesType]) -> Result<usize, GainSolveError> {
    let first = stokes.iter().map(|kind| kind.code()).min();
    let last = stokes.get(3).map(|kind| kind.code());
    match (first, last) {
        (Some(first @ (5 | 9)), Some(last)) if (first..first + 4).contains(&last) => {
            Ok((last - first) as usize)
        }
        _ => Err(model_error("invalid mosaic voltage correlations")),
    }
}

pub(super) struct ImageBeam {
    real: ImagePlane,
    imaginary: Option<ImagePlane>,
}

impl ImageBeam {
    fn load(record: &RecordValue, legacy: bool) -> Result<Self, GainSolveError> {
        let (real, imaginary) = match (record.get("realimage"), record.get("imagimage"), legacy) {
            (Some(_), Some(_), _) | (Some(_), _, false) => {
                let real = ImagePlane::load::<f32>(record_text(record, "realimage")?, None, |v| {
                    Complex32::new(v, 0.)
                })?;
                // Use the standard record field when a legacy table lacks
                // the compatibility alias that CASA's reader expects.
                let key = if legacy && record.get("imagim").is_some() {
                    "imagim"
                } else {
                    "imagimage"
                };
                let path = record_text(record, key)?;
                let imaginary = if path.is_empty() {
                    None
                } else {
                    // PBMath2DImage uses the real-image polarization map
                    // for both arrays, including independently labeled axes.
                    let plane = ImagePlane::load::<f32>(path, Some(real.diagonal), |v| {
                        Complex32::new(0., v)
                    })?;
                    if plane.voltage.shape() != real.voltage.shape()
                        || plane.channel_count != real.channel_count
                    {
                        return Err(model_error(
                            "real and imaginary mosaic voltage images have different shapes",
                        ));
                    }
                    Some(plane)
                };
                (real, imaginary)
            }
            _ => (
                ImagePlane::load::<Complex32>(record_text(record, "compleximage")?, None, |v| v)?,
                None,
            ),
        };
        Ok(Self { real, imaginary })
    }

    pub(super) fn support_pixels(&self, increment: f64, reference_hz: f64) -> usize {
        (self.real.voltage.nrows() as f64 * self.real.frequency_hz / reference_hz
            * (self.real.increment[0] / increment).abs()) as usize
    }

    pub(super) fn value(&self, offset: [f64; 2], frequency_hz: f64) -> Complex32 {
        let real = self.real.value(offset, frequency_hz);
        real + self
            .imaginary
            .as_ref()
            .map_or(Complex32::default(), |plane| {
                plane.value(offset, frequency_hz)
            })
    }
}
