// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::{CoordinateError, CoordinateSystem, ObsInfo};
use casa_core::measures::{EpochRef, MEpoch};
use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
use casa_images::{ImageInfo, ImageType};
use ndarray::{ArrayD, Axis};
use num_complex::Complex32;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::value::CasaValue;
use crate::tools::table::{casa_record, values};

use super::PyImageTool;
use super::beams::{image_info, image_info_record, subrecord};
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};

fn serialize_image(image: &ImageHandle) -> PyResult<CasaValue> {
    let shape = image.shape();
    let pixels = if let Some(fits) = image.fits() {
        let start = vec![0; shape.len()];
        let stride = vec![1; shape.len()];
        ArrayValue::Float32(fits.read(&start, shape, &stride)?)
    } else {
        match image {
            ImageHandle::Float(storage) => {
                with_storage!(storage, image => image.get().map(ArrayValue::Float32))
            }
            ImageHandle::Double(storage) => {
                with_storage!(storage, image => image.get().map(ArrayValue::Float64))
            }
            ImageHandle::Complex(storage) => {
                with_storage!(storage, image => image.get().map(ArrayValue::Complex32))
            }
            ImageHandle::DComplex(storage) => {
                with_storage!(storage, image => image.get().map(ArrayValue::Complex64))
            }
        }
        .map_err(raised)?
    };
    let owner = Path::new(".");
    let info = record_info(image_info(image).map_err(raised)?, image_info_record(image));
    Ok(CasaValue::record([
        (
            "coordsys",
            CasaValue::record([(
                "coordsys",
                values::record(&image.coordinate_record(), owner),
            )]),
        ),
        ("imagearray", values::value(&Value::Array(pixels), owner)),
        ("imageinfo", values::record(&info, owner)),
        (
            "shape",
            CasaValue::Ints {
                shape: vec![shape.len()],
                values: shape.iter().map(|&size| size as i64).collect(),
            },
        ),
    ]))
}

fn record_info(mut info: ImageInfo, original: Option<&RecordValue>) -> RecordValue {
    if info.image_type == ImageType::Undefined {
        info.image_type = ImageType::Intensity;
    }
    let mut record = info.to_record();
    for beam_field in ["restoringbeam", "perplanebeams"] {
        if record.get(beam_field).is_some()
            && let Some(value) = original.and_then(|original| original.get(beam_field))
        {
            record.upsert(beam_field, value.clone());
        }
    }
    record
}

fn field<'a>(record: &'a RecordValue, key: &str) -> PyResult<&'a Value> {
    record
        .get(key)
        .ok_or_else(|| raised(format!("RecordInterface: field {key} is unknown")))
}

fn expect_record<'a>(record: &'a RecordValue, key: &str) -> PyResult<&'a RecordValue> {
    if let Value::Record(value) = field(record, key)? {
        return Ok(value);
    }
    Err(raised(format!(
        "RecordInterface: field {key} is not a record"
    )))
}

fn incorrect_type(key: &str) -> PyErr {
    raised(format!(
        "RecordRep::get_pointer - incorrect data type used for field {key}"
    ))
}

fn record_shape(value: &Value) -> PyResult<Vec<usize>> {
    let extents = match value {
        Value::Array(ArrayValue::Int32(shape)) if shape.ndim() == 1 => {
            shape.iter().map(|&extent| i64::from(extent)).collect()
        }
        Value::Array(ArrayValue::Int64(shape)) if shape.ndim() == 1 => {
            shape.iter().copied().collect()
        }
        Value::Scalar(ScalarValue::Int32(extent)) => vec![i64::from(*extent)],
        Value::Scalar(ScalarValue::Int64(extent)) => vec![*extent],
        _ => return Err(incorrect_type("shape")),
    };
    if extents.is_empty()
        || extents
            .iter()
            .any(|&extent| extent <= 0 || extent > i64::from(i32::MAX))
    {
        return Err(raised(
            "TiledShape: shape has to be > 0 and fit in a signed 32-bit integer",
        ));
    }
    Ok(extents.into_iter().map(|extent| extent as usize).collect())
}

fn fit_into_image<T>(shape: &[usize], mut input: ArrayD<T>) -> PyResult<ArrayD<T>> {
    if input.ndim() > shape.len() {
        return Err(raised("ArrayLattice: imagearray has more axes than shape"));
    }
    if !input
        .shape()
        .iter()
        .zip(shape)
        .all(|(&input, &output)| input <= output)
    {
        return Err(raised(
            "ArrayLattice: imagearray does not fit the requested shape",
        ));
    }
    for axis in input.ndim()..shape.len() {
        input = input.insert_axis(Axis(axis));
    }
    Ok(input)
}

fn record_pixels(value: &Value) -> PyResult<ArrayValue> {
    let convert_complex =
        |number: num_complex::Complex64| Complex32::new(number.re as f32, number.im as f32);
    let scalar = |value| ndarray::arr1(&[value]).into_dyn();
    let array = match value {
        Value::Array(ArrayValue::Int16(values)) => ArrayValue::Float32(values.mapv(f32::from)),
        Value::Array(ArrayValue::Int32(values)) => {
            ArrayValue::Float32(values.mapv(|value| value as f32))
        }
        Value::Array(ArrayValue::Int64(values)) => {
            ArrayValue::Float32(values.mapv(|value| value as f32))
        }
        Value::Array(ArrayValue::UInt8(values)) => ArrayValue::Float32(values.mapv(f32::from)),
        Value::Array(ArrayValue::UInt16(values)) => ArrayValue::Float32(values.mapv(f32::from)),
        Value::Array(ArrayValue::UInt32(values)) => {
            ArrayValue::Float32(values.mapv(|value| value as f32))
        }
        Value::Array(ArrayValue::Float32(values)) => ArrayValue::Float32(values.clone()),
        Value::Array(ArrayValue::Float64(values)) => {
            ArrayValue::Float32(values.mapv(|value| value as f32))
        }
        Value::Array(ArrayValue::Complex32(values)) => ArrayValue::Complex32(values.clone()),
        Value::Array(ArrayValue::Complex64(values)) => {
            ArrayValue::Complex32(values.mapv(convert_complex))
        }
        Value::Scalar(ScalarValue::Int32(value)) => ArrayValue::Float32(scalar(*value as f32)),
        Value::Scalar(ScalarValue::Int64(value)) => ArrayValue::Float32(scalar(*value as f32)),
        Value::Scalar(ScalarValue::Float32(value)) => ArrayValue::Float32(scalar(*value)),
        Value::Scalar(ScalarValue::Float64(value)) => ArrayValue::Float32(scalar(*value as f32)),
        Value::Scalar(ScalarValue::Complex32(value)) => {
            ArrayValue::Complex32(ndarray::arr1(&[*value]).into_dyn())
        }
        Value::Scalar(ScalarValue::Complex64(value)) => {
            ArrayValue::Complex32(ndarray::arr1(&[convert_complex(*value)]).into_dyn())
        }
        _ => return Err(incorrect_type("imagearray")),
    };
    Ok(array)
}

#[pymethods]
impl PyImageTool {
    fn torecord(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let record = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(CasaValue::empty_record());
            };
            let mut image = attachment.lock();
            attachment.require_pixel_read_lock(&mut image)?;
            let record = serialize_image(&image)?;
            image.release_requested_lock()?;
            Ok::<_, PyErr>(record)
        })?;
        record.into_python(py)
    }

    #[pyo3(signature = (record, outfile=""), text_signature = "($self, record, outfile='')")]
    fn fromrecord(
        &self,
        py: Python<'_>,
        record: &Bound<'_, PyDict>,
        outfile: &str,
    ) -> PyResult<bool> {
        let record = casa_record(py, Some(record))?;
        py.detach(|| {
            let mut state = self.state();
            state.image = None;
            let pixel_value = field(&record, "imagearray")?;
            let shape = record_shape(field(&record, "shape")?)?;
            let coordinate_record = subrecord(expect_record(&record, "coordsys")?, "coordsys")
                .ok_or_else(|| raised("Error converting image from record: invalid coordinate system"))?;
            let coordinates = CoordinateSystem::from_record(coordinate_record).map_err(|error| {
                match error {
                    CoordinateError::UnsupportedAxisLayout(_) => raised(format!("{error} (casa-rs issue #1002)")),
                    CoordinateError::UnsupportedProjection(_) => raised(format!("{error} (casa-rs issue #1626)")),
                    _ => raised(error),
                }
            })?;
            let (coordinates, coordinate_record) = if coordinates.n_pixel_axes() == shape.len() {
                (coordinates, coordinate_record.clone())
            } else {
                if !outfile.is_empty() {
                    return Err(raised("PagedImage: Failed AlwaysAssert setCoordinateInfo(coordinateInfo)"));
                }
                // casacore's temporary restore keeps its default coordinate
                // system when setting the incompatible record fails.
                let obs = ObsInfo::new("UNKNOWN")
                    .with_observer("UNKNOWN")
                    .with_date(MEpoch::from_mjd(0.0, EpochRef::LAST));
                let coordinates = CoordinateSystem::new().with_obs_info(obs);
                let record = coordinates.to_record();
                (coordinates, record)
            };
            let info = expect_record(&record, "imageinfo")?;
            let info = record_info(ImageInfo::from_record(info).map_err(raised)?, Some(info));
            let pixels = record_pixels(pixel_value)?;
            let attached = publish_image(outfile, true, |path| {
                let mut image = match pixels {
                    ArrayValue::Float32(data) => {
                        let data = fit_into_image(&shape, data)?;
                        ImageHandle::Float(Storage::create(shape, coordinates, path, Some(&data)).map_err(raised)?)
                    }
                    ArrayValue::Complex32(data) => {
                        let data = fit_into_image(&shape, data)?;
                        ImageHandle::Complex(Storage::create(shape, coordinates, path, Some(&data)).map_err(raised)?)
                    }
                    _ => unreachable!("record allocated single precision pixels"),
                };
                image.set_coordinate_record(coordinate_record)?;
                with_pixels!(&mut image, storage => with_storage!(storage, image => image.set_image_info_record(info))).map_err(raised)?;
                if state.history {
                    image.record_history("fromrecord", format!("ia.fromrecord(record={{...}}, outfile={outfile:?})"))?;
                }
                image.save().map_err(raised)?;
                Ok(image)
            })?;
            state.image = Some(attached);
            Ok(true)
        })
    }
}
