// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::quanta::{Quantity, Unit};
use ndarray::Array2;

use crate::coordinate::CoordinateModel;
use crate::error::CoordinateError;
use crate::linear::{LinearCoordinate, invert_matrix};

use super::CoordinateSystem;

impl CoordinateSystem {
    /// Replace the selected pixel axes with Fourier coordinates for this shape.
    /// The transform keeps the pixel and world axis maps of the original system.
    ///
    /// # Errors
    /// An axis is absent, only one direction axis is selected, or the source
    /// cannot be represented by a linear transform in the Fourier domain.
    pub fn fourier_axes(&self, shape: &[usize], axes: &[usize]) -> Result<Self, CoordinateError> {
        let ndim = self.n_pixel_axes();
        if shape.len() != ndim {
            return Err(CoordinateError::DimensionMismatch {
                expected: ndim,
                got: shape.len(),
            });
        }
        if axes.is_empty() || shape.contains(&0) {
            return Err(failed("You have not specified any axes to transform"));
        }
        let mut active = vec![false; ndim];
        for &axis in axes {
            *active
                .get_mut(axis)
                .ok_or(CoordinateError::AxisOutOfRange {
                    index: axis,
                    naxes: ndim,
                })? = true;
        }
        let mut output = self.clone();
        for index in 0..self.n_coordinates() {
            let pixel_axes = self
                .coordinate_pixel_axes(index)
                .expect("existing coordinate");
            let lengths = pixel_axes
                .iter()
                .map(|axis| axis.filter(|&axis| active[axis]).map(|axis| shape[axis]))
                .collect::<Vec<_>>();
            if lengths.iter().all(Option::is_none) {
                continue;
            }
            let source = self.coordinate(index);
            let converted = fourier_coordinate(source, &lengths)?;
            output.replace_coordinate(index, converted);
        }
        Ok(output)
    }
}

fn failed(message: &str) -> CoordinateError {
    CoordinateError::ConversionFailed(message.into())
}

fn fourier_coordinate(
    source: &CoordinateModel,
    lengths: &[Option<usize>],
) -> Result<CoordinateModel, CoordinateError> {
    let n = source.n_pixel_axes();
    if lengths.len() != n || source.n_world_axes() != n {
        return Err(failed(
            "A selected coordinate has incompatible pixel and world axes",
        ));
    }
    let mut names = source.axis_names();
    let mut units = source.axis_units();
    let mut center = source.reference_value();
    let mut pixel = source.reference_pixel();
    let mut increment = source.increment();
    let transform = match source {
        CoordinateModel::System(system) => {
            let shape = lengths
                .iter()
                .map(|length| length.unwrap_or(1))
                .collect::<Vec<_>>();
            let axes = lengths
                .iter()
                .enumerate()
                .filter_map(|(axis, length)| length.map(|_| axis))
                .collect::<Vec<_>>();
            return system.fourier_axes(&shape, &axes).map(Into::into);
        }
        CoordinateModel::Direction(direction) => {
            if n != 2 || lengths.iter().any(Option::is_none) {
                return Err(failed(
                    "You must specify both the DirectionCoordinate (sky) axes to FFT",
                ));
            }
            names = vec!["UU".into(), "VV".into()];
            units = vec!["lambda".into(); 2];
            invert_pc(direction.pc_matrix(), lengths)?
        }
        CoordinateModel::Linear(linear) => {
            for (i, length) in lengths.iter().enumerate() {
                if length.is_some() {
                    let original = units[i].clone();
                    let canonical = inverse_axis_units(&mut names[i], &mut units[i])?;
                    increment[i] = convert_increment(increment[i], &original, &canonical)?;
                }
            }
            invert_pc(linear.pc_matrix(), lengths)?
        }
        CoordinateModel::Spectral(spectral) if spectral.has_linear_mapping() => {
            let source_unit = units[0].clone();
            let canonical = inverse_axis_units(&mut names[0], &mut units[0])?;
            increment[0] = convert_increment(increment[0], &source_unit, &canonical)?;
            Array2::eye(n)
        }
        CoordinateModel::Tabular(table) if table.pixel_values().is_empty() => {
            let source_unit = units[0].clone();
            let canonical = inverse_axis_units(&mut names[0], &mut units[0])?;
            increment[0] = convert_increment(increment[0], &source_unit, &canonical)?;
            Array2::eye(n)
        }
        CoordinateModel::Spectral(_) => {
            return Err(failed(
                "Cannot Fourier Transform a non-linear SpectralCoordinate",
            ));
        }
        CoordinateModel::Tabular(_) => {
            return Err(failed(
                "Cannot Fourier Transform a non-linear TabularCoordinate",
            ));
        }
        CoordinateModel::Stokes(_) => {
            return Err(failed(
                "Coordinates of type Stokes cannot be Fourier Transformed",
            ));
        }
        CoordinateModel::Quality(_) => {
            return Err(failed(
                "Coordinates of type Quality cannot be Fourier Transformed",
            ));
        }
    };
    for (i, &length) in lengths.iter().enumerate() {
        if let Some(length) = length {
            if increment[i] == 0. || !increment[i].is_finite() {
                return Err(failed(
                    "A Fourier transform requires a nonzero finite coordinate increment",
                ));
            }
            center[i] = 0.;
            increment[i] = 1. / (length as f64 * increment[i]);
            pixel[i] = (length / 2) as f64;
        }
    }
    Ok(LinearCoordinate::new(n, names, units)
        .with_reference_value(center)
        .with_reference_pixel(pixel)
        .with_increment(increment)
        .with_pc_matrix(transform)
        .into())
}

fn invert_pc(
    matrix: &Array2<f64>,
    lengths: &[Option<usize>],
) -> Result<Array2<f64>, CoordinateError> {
    if matrix
        .indexed_iter()
        .all(|((i, j), &value)| i == j || value.abs() <= 1e-12)
    {
        let mut inverse = matrix.clone();
        for (i, length) in lengths.iter().enumerate() {
            if length.is_some() {
                let reciprocal = inverse[[i, i]].recip();
                if !reciprocal.is_finite() {
                    return Err(CoordinateError::SingularTransform("singular PC matrix"));
                }
                inverse[[i, i]] = reciprocal;
            }
        }
        Ok(inverse)
    } else if lengths.iter().all(Option::is_some) {
        invert_matrix(matrix)
    } else {
        Err(failed(
            "Cannot invert non-diagonal PC matrix when some axes are not being transformed",
        ))
    }
}

fn inverse_axis_units(name: &mut String, unit: &mut String) -> Result<String, CoordinateError> {
    let input = Unit::new(unit.as_str()).map_err(|error| failed(&error.to_string()))?;
    let hz = Unit::new("Hz").expect("hertz unit");
    let seconds = Unit::new("s").expect("second unit");
    let canonical = if input.conformant(&hz) {
        *name = "Time".into();
        *unit = "s".into();
        "Hz".into()
    } else if input.conformant(&seconds) {
        *name = "Frequency".into();
        *unit = "Hz".into();
        "s".into()
    } else {
        *name = format!("Inverse({name})");
        let original = std::mem::take(unit);
        *unit = format!("1/{original}");
        original
    };
    Ok(canonical)
}

fn convert_increment(value: f64, unit: &str, canonical: &str) -> Result<f64, CoordinateError> {
    let target = Unit::new(canonical).map_err(|error| failed(&error.to_string()))?;
    Quantity::new(value, unit)
        .and_then(|increment| increment.get_value_in(&target))
        .map_err(|error| failed(&error.to_string()))
}
