// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_images::{ImageInterface, ImagePixel};
use num_complex::Complex64;
use pyo3::prelude::*;

use crate::convert::DoubleList;
use crate::convert::value::CasaValue;
use crate::tools::coordsys::{PyCoordsys, WorldArgument, image_pixel_argument};

use super::PyImageTool;
use super::attachment::coordsys_for;
use super::handle::{ImageHandle, raised, with_storage};
use super::point::{pixel_position, pixel_sample};

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
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = &state.image else {
                return Ok(CasaValue::empty_record());
            };
            let image = attachment.lock();
            image
                .check_single_precision("coordmeasures")
                .map_err(raised)?;
            let coords = coordsys_for(attachment, &image)?;
            let requested = pixel.iter().map(|&axis| axis as i64).collect::<Vec<_>>();
            let result = coords.image_measures(pixel, dframe, sframe)?;
            match &*image {
                ImageHandle::Float(storage) => storage.with_float_view(|image| {
                    add_intensity(result, image, requested, |value| {
                        CasaValue::Float(f64::from(value))
                    })
                }),
                ImageHandle::Complex(storage) => with_storage!(storage, image => {
                    add_intensity(result, image.as_ref(), requested, |value| {
                        CasaValue::Complex(Complex64::new(f64::from(value.re), f64::from(value.im)))
                    })
                }),
                _ => unreachable!("double precision was rejected"),
            }
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

fn add_intensity<T: ImagePixel>(
    result: CasaValue,
    image: &dyn ImageInterface<T>,
    mut requested: Vec<i64>,
    value: impl Fn(T) -> CasaValue,
) -> PyResult<CasaValue> {
    if requested.is_empty() {
        let coords = image.coordinates();
        requested = coords
            .reference_pixel()
            .into_iter()
            .map(|pixel| pixel as i64)
            .collect();
    }
    let CasaValue::Record(mut fields) = result else {
        unreachable!("the world measure is returned in a record");
    };
    let (intensity, units) = match pixel_position(image.coordinates(), image.shape(), &requested) {
        Some(position) => {
            let (sample, mask) = pixel_sample(image, &position)?;
            fields.push(("mask".into(), CasaValue::Bool(mask)));
            (sample, image.units())
        }
        None => (T::default_value(), ""),
    };
    fields.push((
        "intensity".into(),
        CasaValue::record([
            ("unit", CasaValue::Text(units.into())),
            ("value", value(intensity)),
        ]),
    ));
    Ok(CasaValue::record(fields))
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
