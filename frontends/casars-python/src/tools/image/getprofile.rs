// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fmt::Write as _;
use std::path::Path;

use casa_coordinates::CoordinateType;
use casa_core::{ArrayD, ArrayValue, RecordValue, ScalarValue, Value};
use casa_images::ImageInterface;
use casa_images::analysis::{
    CollapseFunction, CollapsePixel, ImageReadProfile, ProfileFrequencyFrame, ReadProfileOptions,
    SubimageRegion, ensure_artifacts_do_not_overlap, read_image_profile,
};
use ndarray::IxDyn;
use num_complex::{Complex32, Complex64};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::{Variant, quantity};
use crate::tools::coordsys::profile_frequency_frame;

use super::PyImageTool;
use super::attachment::{AttachedImage, read_locked};
use super::data_compat::record_to_py;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::subimage::{RegionArgument, require_bool};
use super::text_output::StagedTextOutput;

struct ReadRequest<'a> {
    axis: usize,
    function: CollapseFunction,
    region: RegionArgument,
    mask: &'a str,
    unit: &'a str,
    stretch: bool,
    spectral_type: &'a str,
    rest_frequency: Option<casa_core::quanta::Quantity>,
    frame: &'a str,
    logfile: &'a str,
}

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
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = &state.image else {
                return Err(raised("No image attached to tool"));
            };
            match *attachment.lock() {
                ImageHandle::Float(_) | ImageHandle::Complex(_) => Ok(()),
                _ => Err(raised(
                    "getprofile does not support images with double precision pixel values",
                )),
            }
        })?;
        let axis = usize::try_from(axis).map_err(|_| raised("Axis must be greater than 0"))?;
        let rest_frequency = match restfreq {
            None | Some(Variant::Empty) => None,
            Some(Variant::Text(text)) if text.trim().is_empty() => None,
            Some(value) => Some(quantity(&value).map_err(raised)?),
        };
        let request = ReadRequest {
            axis,
            function: CollapseFunction::parse(function).map_err(raised)?,
            region: region
                .map_or_else(|| Ok(RegionArgument::default()), |value| value.extract())?,
            mask,
            unit,
            stretch,
            spectral_type: spectype,
            rest_frequency,
            frame: frame.trim(),
            logfile,
        };
        let result = py.detach(|| {
            let state = self.state();
            let attachment = state
                .image
                .as_ref()
                .ok_or_else(|| raised("No image attached to tool"))?;
            request.run(attachment)
        })?;
        result.into_object(py)
    }
}

impl ReadRequest<'_> {
    fn run(&self, attachment: &AttachedImage) -> PyResult<ProfileRecord> {
        read_locked(attachment, |source| match source {
            ImageHandle::Float(storage) => {
                storage.with_float_view(|image| self.compute(attachment, source, image))
            }
            ImageHandle::Complex(storage) => with_storage!(
                storage, image => self.compute(attachment, source, image.as_ref())
            ),
            _ => Err(raised(
                "getprofile does not support images with double precision pixel values",
            )),
        })
    }

    fn compute<T: ProfilePixel>(
        &self,
        attachment: &AttachedImage,
        source: &ImageHandle,
        image: &dyn ImageInterface<T>,
    ) -> PyResult<ProfileRecord> {
        let path = nonempty(self.logfile).map(Path::new);
        let region = self.region.selection();
        if let Some(path) = path {
            let region_file = region.and_then(SubimageRegion::external_path);
            for input in source
                .path()
                .into_iter()
                .chain(attachment.operand_paths().iter().map(|path| path.as_path()))
                .chain(region_file)
            {
                ensure_artifacts_do_not_overlap(path, input).map_err(raised)?;
            }
        }
        let frame = if !self.frame.is_empty()
            && !self.unit.to_ascii_lowercase().starts_with("pix")
            && image.find_axis(CoordinateType::Spectral) == Some(self.axis)
        {
            profile_frequency_frame(image.coordinates(), self.frame)?
                .map(|(reference, context)| ProfileFrequencyFrame { reference, context })
        } else {
            None
        };
        let region_path = match &region {
            Some(SubimageRegion::Text(text)) => Some(*text),
            _ => None,
        };
        let mut beams = image.image_info().map_err(raised)?.to_record();
        attachment.beam_view().apply_to(&mut beams);
        let profile = read_image_profile(
            image,
            source.coordinate_record(),
            &beams,
            ReadProfileOptions {
                axis: self.axis,
                function: self.function,
                region,
                mask: nonempty(self.mask),
                stretch: self.stretch,
                unit: self.unit,
                spectral_type: self.spectral_type,
                rest_frequency: self.rest_frequency.as_ref(),
                spectral_frame: frame,
                logfile: path,
            },
        )
        .map_err(raised)?;
        if let Some(path) = path {
            let name = source
                .path()
                .map(|path| path.to_string_lossy())
                .unwrap_or_else(|| "Temporary_Image".into());
            StagedTextOutput::new(path, &report(&profile, &name, region_path), false)?.install()?;
        }
        Ok(ProfileRecord::new(profile))
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
    fn report_value(self) -> String;
}

impl ProfilePixel for f32 {
    fn record_values(values: Vec<Self>) -> ArrayValue {
        ArrayValue::Float64(vector(values.into_iter().map(f64::from).collect()))
    }

    fn report_value(self) -> String {
        format!("{self:10.7}")
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

    fn report_value(self) -> String {
        format!("({:.7},{:.7})", self.re, self.im)
    }
}

fn vector<T>(values: Vec<T>) -> ArrayD<T> {
    ArrayD::from_shape_vec(IxDyn(&[values.len()]), values).expect("one-dimensional profile")
}

fn report<T: ProfilePixel>(
    profile: &ImageReadProfile<T>,
    image: &str,
    region: Option<&str>,
) -> String {
    let mut text = format!("#title: {} profile - {image}\n", profile.axis_name);
    if let Some(region) = region {
        let _ = writeln!(text, "#region : {region}");
    }
    let _ = writeln!(
        text,
        "#xUnit {}\n#yUnit {}\n# {image}\n",
        profile.x_unit, profile.y_unit
    );
    for ((&coordinate, &value), &valid) in profile
        .coordinates
        .iter()
        .zip(&profile.values)
        .zip(&profile.mask)
    {
        if valid {
            let _ = writeln!(text, "{coordinate:.7} {}", value.report_value());
        }
    }
    text
}

fn nonempty(text: &str) -> Option<&str> {
    (!text.is_empty()).then_some(text)
}
