// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.getprofile` wrapper delegating to [`casa_imageanalysis::tool::ops_fit::get_profile`].

use std::path::Path;

use casa_coordinates::CoordinateType;
use casa_core::{ArrayD, ArrayValue, RecordValue, ScalarValue, Value};
use casa_imageanalysis::analysis::{
    CollapseFunction, CollapsePixel, ImageReadProfile, ProfileFrequencyFrame, ReadProfileOptions,
};
use casa_imageanalysis::tool::ops_fit::{ProfileData, get_profile};
use ndarray::IxDyn;
use num_complex::{Complex32, Complex64};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::{Variant, quantity};
use crate::tools::coordsys::profile_frequency_frame;

use super::PyImageTool;
use super::data_compat::record_to_py;
use super::handle::{raised, with_pixels, with_storage};
use super::subimage::{RegionArgument, require_bool};

#[pymethods]
impl PyImageTool {
    /// Reduce the selected image perpendicular to an axis and return the profile.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (axis=-1, function="mean", region=None, mask="", unit="", stretch=false, spectype="default", restfreq=None, frame="", logfile=""), text_signature = "($self, axis=-1, function='mean', region={}, mask='', unit='', stretch=False, spectype='default', restfreq='', frame='', logfile='')")]
    fn getprofile<'py>(
        &self,
        py: Python<'py>,
        axis: i64,
        function: &str,
        region: Option<&Bound<'py, PyAny>>,
        mask: &str,
        unit: &str,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
        spectype: &str,
        restfreq: Option<Variant>,
        frame: &str,
        logfile: &str,
    ) -> PyResult<Py<PyAny>> {
        let axis_u = usize::try_from(axis).map_err(|_| raised("Axis must be greater than 0"))?;
        let rest_frequency = match restfreq {
            None | Some(Variant::Empty) => None,
            Some(Variant::Text(text)) if text.trim().is_empty() => None,
            Some(value) => Some(quantity(&value).map_err(raised)?),
        };
        let function = CollapseFunction::parse(function).map_err(raised)?;
        let region: RegionArgument =
            region.map_or_else(|| Ok(RegionArgument::default()), |value| value.extract())?;
        let frame = frame.trim();
        let path = (!logfile.is_empty()).then(|| Path::new(logfile));
        let profile = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let attachment = state
                .image
                .as_ref()
                .ok_or_else(|| raised("No image attached to tool"))?;
            let spectral_frame = {
                let handle = attachment.lock();
                let spectral_axis = with_pixels!(
                    &*handle,
                    s => with_storage!(s, img => img.find_axis(CoordinateType::Spectral))
                );
                if !frame.is_empty()
                    && !unit.to_ascii_lowercase().starts_with("pix")
                    && spectral_axis == Some(axis_u)
                {
                    profile_frequency_frame(handle.coordinates(), frame)?
                        .map(|(reference, context)| ProfileFrequencyFrame { reference, context })
                } else {
                    None
                }
            };
            let options = ReadProfileOptions {
                axis: axis_u,
                function,
                region: region.selection(),
                mask: (!mask.is_empty()).then_some(mask),
                stretch,
                unit,
                spectral_type: spectype,
                rest_frequency: rest_frequency.as_ref(),
                spectral_frame,
                logfile: path,
            };
            get_profile(attachment, options).map_err(raised)
        })?;
        let record = match profile {
            ProfileData::Float(p) => ProfileRecord::new(p),
            ProfileData::Complex(p) => ProfileRecord::new(p),
        };
        record.into_object(py)
    }
}

struct ProfileRecord {
    record: RecordValue,
    counts: Vec<usize>,
}

impl ProfileRecord {
    fn new<T: ProfilePixel>(profile: ImageReadProfile<T>) -> Self {
        let mut record = RecordValue::default();
        for (field, values) in [
            ("coords", ArrayValue::Float64(vector(profile.coordinates))),
            ("mask", ArrayValue::Bool(vector(profile.mask))),
            ("planes", ArrayValue::Float64(vector(profile.planes))),
            ("values", T::record_values(profile.values)),
        ] {
            record.upsert(field, Value::Array(values));
        }
        record.upsert("xUnit", Value::Scalar(ScalarValue::String(profile.x_unit)));
        record.upsert("yUnit", Value::Scalar(ScalarValue::String(profile.y_unit)));
        Self {
            record,
            counts: profile.counts,
        }
    }

    fn into_object(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let result = record_to_py(py, self.record)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("dtype", "uint64")?;
        let counts = py
            .import("numpy")?
            .call_method("asarray", (self.counts,), Some(&kwargs))?;
        result.bind(py).cast::<PyDict>()?.set_item("npix", counts)?;
        Ok(result)
    }
}

trait ProfilePixel: CollapsePixel {
    fn record_values(values: Vec<Self>) -> ArrayValue;
}

impl ProfilePixel for f32 {
    fn record_values(values: Vec<Self>) -> ArrayValue {
        ArrayValue::Float64(vector(values.into_iter().map(f64::from).collect()))
    }
}

impl ProfilePixel for Complex32 {
    fn record_values(values: Vec<Self>) -> ArrayValue {
        ArrayValue::Complex64(vector(
            values
                .into_iter()
                .map(|value| Complex64::new(value.re.into(), value.im.into()))
                .collect(),
        ))
    }
}

fn vector<T>(values: Vec<T>) -> ArrayD<T> {
    ArrayD::from_shape_vec(IxDyn(&[values.len()]), values).expect("one-dimensional profile")
}
