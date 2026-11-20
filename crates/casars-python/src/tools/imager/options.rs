// SPDX-License-Identifier: LGPL-3.0-or-later
//! Convert the legacy imager's Python Fourier settings.

use casa_calibration::{
    ImagerGridder, ImagerSingleDish, ImagerSingleDishKernel, ImagerTransformOptions,
};
use casa_types::measures::position::PositionRef;
use pyo3::{
    Borrowed,
    exceptions::{PySystemError, PyTypeError},
    prelude::*,
    types::{PyBytes, PyDict, PyFloat, PyList, PyString},
};

use crate::convert::{Variant, measures::position};

use super::runtime_error;

#[derive(Default)]
pub(super) struct ImageTileVolume(pub(super) i32);

impl<'a, 'py> FromPyObject<'a, 'py> for ImageTileVolume {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if value.is_instance_of::<PyFloat>()
            || value.is_instance_of::<PyString>()
            || value.is_instance_of::<PyBytes>()
            || value.is_instance_of::<PyList>()
            || value.is_instance_of::<PyDict>()
        {
            return Err(PyTypeError::new_err(
                "argument _imagetilevol must be an integer",
            ));
        }
        // The legacy Python wrapper reads a signed long and narrows it
        // to the imager's signed Int before it computes a tile shape.
        value
            .extract::<i64>()
            .map(|volume| Self(volume as i32))
            .map_err(|_| {
                PySystemError::new_err(
                    "<built-in function imager_setoptions> returned a result with an exception set",
                )
            })
    }
}

pub(super) struct TransformArguments<'a> {
    pub(super) machine: &'a str,
    pub(super) cache: i64,
    pub(super) tile: i32,
    pub(super) convolution: &'a str,
    pub(super) location: &'a Variant,
    pub(super) padding: f64,
    pub(super) interpolation: &'a str,
    pub(super) planes: i32,
    pub(super) single_precision: bool,
    pub(super) threads: i32,
}

impl TransformArguments<'_> {
    pub(super) fn options(
        self,
        mut previous: ImagerTransformOptions,
    ) -> PyResult<ImagerTransformOptions> {
        let machine = self.machine.to_ascii_lowercase();
        let convolution = self.convolution.to_ascii_lowercase();
        previous.single_dish = if matches!(machine.as_str(), "sd" | "both") {
            let kernel = ImagerSingleDishKernel::from_name(&convolution).ok_or_else(|| {
                runtime_error(format!(
                    "Unknown single-dish grid function {}",
                    self.convolution
                ))
            })?;
            Some(ImagerSingleDish {
                kernel,
                combined: machine == "both",
            })
        } else {
            None
        };
        if machine != "sd" && matches!(convolution.as_str(), "gauss" | "gjinc") {
            return Err(runtime_error(format!(
                "Grid function {} is available only for single dish imaging",
                self.convolution,
            )));
        }
        previous.gridder = match machine.as_str() {
            "wproject" => ImagerGridder::w_project(self.planes),
            "mosaic" => ImagerGridder::Mosaic,
            "sd" | "both" | "pbwproject" | "pbmosaic" | "awproject" | "mawproject" => {
                ImagerGridder::Spheroidal
            }
            // CASA's rGridFT (nift) and GridFT give the same predictions.
            _ => match convolution.as_str() {
                "box" => ImagerGridder::Boxcar,
                "sf" => ImagerGridder::Spheroidal,
                _ => {
                    return Err(runtime_error(format!(
                        "Unknown interferometric grid function {}",
                        self.convolution
                    )));
                }
            },
        };
        if self.cache > 0 {
            previous.cache_size = self.cache;
        }
        if self.tile > 0 {
            previous.tile_size = self.tile;
        }
        if self.padding >= 1.0 {
            previous.padding = self.padding as f32;
        }
        let interpolation = self.interpolation.to_ascii_lowercase();
        previous.frequency_interpolation = if interpolation.contains("linear") {
            1
        } else if interpolation.contains("splin") {
            3
        } else if interpolation.contains("cub") {
            2
        } else {
            0
        };
        previous.observatory = if self.location.is_empty_sequence()
            || matches!(self.location, Variant::Text(value) if value.is_empty())
        {
            None
        } else {
            position(
                self.location,
                &casa_ms::open_measures_runtime().map_err(runtime_error)?,
            )?
            .map(|position| {
                let itrf = position
                    .convert_to(PositionRef::ITRF)
                    .map_err(runtime_error)?;
                let (longitude, latitude, radius) = itrf.as_spherical();
                Ok::<_, PyErr>([longitude, latitude, radius])
            })
            .transpose()?
        };
        previous.threads = self.threads;
        previous.use_single_precision(self.single_precision);
        Ok(previous)
    }
}
