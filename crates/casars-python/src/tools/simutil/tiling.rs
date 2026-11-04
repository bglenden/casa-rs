// SPDX-License-Identifier: LGPL-3.0-or-later
//! Direct bindings for CASA's pointing tilings.

use casa_simulation::simutil::pointings::{triangle_points, triangular_tiling};
use pyo3::exceptions::PyZeroDivisionError;
use pyo3::prelude::*;

use super::{PySimutil, ops};

/// CASA returns a pair of lists, one coordinate per list.
fn coordinates(points: Vec<(f64, f64)>) -> (Vec<f64>, Vec<f64>) {
    points.into_iter().unzip()
}

#[pymethods]
#[allow(non_snake_case)]
impl PySimutil {
    /// The Cycle 0 triangular pointing pattern.
    fn getTrianglePoints(
        &self,
        width: f64,
        height: f64,
        angle: f64,
        spacing: f64,
    ) -> PyResult<(Vec<f64>, Vec<f64>)> {
        if spacing == 0.0 {
            return Err(PyZeroDivisionError::new_err("float division by zero"));
        }
        triangle_points(width, height, angle, spacing)
            .map(coordinates)
            .map_err(ops::raised)
    }

    /// The ALMA Observing Tool's base or shifted triangular pattern.
    fn getTriangularTiling(
        &self,
        longlength: f64,
        latlength: f64,
        angle: f64,
        spacing: f64,
        pb: f64,
    ) -> PyResult<(Vec<f64>, Vec<f64>)> {
        if spacing == 0.0 {
            return Err(PyZeroDivisionError::new_err("float division by zero"));
        }
        triangular_tiling(longlength, latlength, angle, spacing, pb)
            .map(coordinates)
            .map_err(ops::raised)
    }
}
