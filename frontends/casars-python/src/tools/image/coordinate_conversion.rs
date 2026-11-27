// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `coordmeasures` / `topixel` / `toworld` wrapper delegating to `casa_imageanalysis::tool::coordinate_conversion`.

use casa_imageanalysis::tool::coordinate_conversion::coord_measures_intensity;
use casa_imageanalysis::tool::point::PixelScalar;
use num_complex::Complex64;
use pyo3::prelude::*;

use crate::convert::DoubleList;
use crate::convert::value::CasaValue;
use crate::tools::coordsys::{PyCoordsys, WorldArgument, image_pixel_argument};

use super::PyImageTool;
use super::attachment::coordsys_for;
use super::handle::raised;

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (pixel=None, dframe="cl", sframe="cl"), text_signature = "($self, pixel=[-1.0], dframe='cl', sframe='cl')")]
    fn coordmeasures(
        &self,
        py: Python<'_>,
        pixel: Option<DoubleList>,
        dframe: &str,
        sframe: &str,
    ) -> PyResult<Py<PyAny>> {
        let mut pixel = pixel.map_or_else(Vec::new, |value| value.0);
        if pixel == [-1.0] {
            pixel.clear();
        }
        py.detach(|| -> PyResult<CasaValue> {
            let state = self.state();
            let Some(attachment) = &state.image else {
                return Ok(CasaValue::empty_record());
            };
            let intensity = coord_measures_intensity(attachment, &pixel).map_err(raised)?;
            let coords = coordsys_for(attachment, &attachment.lock())?;
            let CasaValue::Record(mut fields) = coords.image_measures(pixel, dframe, sframe)?
            else {
                unreachable!("the world measure is returned in a record");
            };
            if let Some(mask) = intensity.mask {
                fields.push(("mask".into(), CasaValue::Bool(mask)));
            }
            let scalar = match intensity.value {
                PixelScalar::Float(v) => CasaValue::Float(f64::from(v)),
                PixelScalar::Double(v) => CasaValue::Float(v),
                PixelScalar::Complex(v) => {
                    CasaValue::Complex(Complex64::new(f64::from(v.re), f64::from(v.im)))
                }
                PixelScalar::DComplex(v) => CasaValue::Complex(v),
            };
            fields.push((
                "intensity".into(),
                CasaValue::record([("unit", CasaValue::Text(intensity.unit)), ("value", scalar)]),
            ));
            Ok(CasaValue::record(fields))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (value=None), text_signature = "($self, value=[])")]
    fn topixel(&self, py: Python<'_>, value: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        if py.detach(|| self.state().image.is_none()) {
            return CasaValue::empty_record().into_python(py);
        }
        let value = WorldArgument::capture(py, value)?;
        py.detach(|| match self.coordinate_state()? {
            Some(coordinates) => coordinates.image_pixel(value),
            None => Ok(CasaValue::empty_record()),
        })?
        .into_python(py)
    }

    #[pyo3(signature = (value=None, format="n", dovelocity=true), text_signature = "($self, value=[], format='n', dovelocity=True)")]
    fn toworld(
        &self,
        py: Python<'_>,
        value: Option<&Bound<'_, PyAny>>,
        format: &str,
        dovelocity: bool,
    ) -> PyResult<Py<PyAny>> {
        if py.detach(|| self.state().image.is_none()) {
            return CasaValue::empty_record().into_python(py);
        }
        let pixel = image_pixel_argument(py, value)?;
        py.detach(|| match self.coordinate_state()? {
            Some(coordinates) => coordinates.image_world(pixel, format, dovelocity),
            None => Ok(CasaValue::empty_record()),
        })?
        .into_python(py)
    }
}

impl PyImageTool {
    fn coordinate_state(&self) -> PyResult<Option<PyCoordsys>> {
        self.state()
            .image
            .as_ref()
            .map(|attachment| coordsys_for(attachment, &attachment.lock()))
            .transpose()
    }
}
