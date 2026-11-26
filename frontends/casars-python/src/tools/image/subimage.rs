// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_core::{RecordValue, ScalarValue, Value};
use casa_images::analysis::{
    SubimageOptions, SubimagePlan, SubimageRegion, ensure_outfile_does_not_alias_input,
};
use casa_images::image::image_history_rows;
use casa_images::{ImageInterface, ImagePixel};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyList, PyString, PyTuple};

use crate::convert::{IntList, Variant};
use crate::tools::{LogLine, emit, table::casa_record};

use super::PyImageTool;
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_storage};

pub(super) enum Region {
    Text(String),
    Record(RecordValue),
}

impl Region {
    pub(super) fn argument(
        py: Python<'_>,
        value: Option<&Bound<'_, PyAny>>,
        method: &str,
    ) -> PyResult<Option<Self>> {
        let region = Self::capture(py, value)?;
        if let Some(Self::Record(record)) = &region {
            let Some(Value::Scalar(ScalarValue::String(name))) = record.get("name") else {
                return Err(super::stub(method).unwrap_err());
            };
            match name.as_str() {
                "LCSlicer" | "LCBox" | "WCBox" | "WCPolygon" | "WCEllipsoid" => {}
                "WCUnion" | "WCDifference" | "WCIntersection" | "WCComplement" | "WCExtension"
                | "WCConcatenation" => {
                    if record.get("regions").is_none() {
                        return Err(raised("RecordInterface: field regions is unknown"));
                    }
                }
                _ => return Err(super::stub(method).unwrap_err()),
            }
        }
        Ok(region)
    }

    pub(super) fn reject_compound_record(region: Option<&Self>) -> PyResult<()> {
        if matches!(region, Some(Self::Record(record)) if record.get("regions").is_some()) {
            return Err(raised(
                "Exception: Only a single n-dimensional rectangular region is supported.",
            ));
        }
        Ok(())
    }

    pub(super) fn capture(
        py: Python<'_>,
        value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Option<Self>> {
        let Some(value) = value else {
            return Ok(None);
        };
        if let Ok(record) = value.cast::<PyDict>() {
            let record = casa_record(py, Some(record))?;
            return Ok((!record.is_empty()).then_some(Self::Record(record)));
        }
        if let Ok(text) = value.cast::<PyString>() {
            let text = text.to_str()?;
            return Ok((!text.is_empty()).then(|| Self::Text(text.to_owned())));
        }
        if empty_variant(value)? {
            return Ok(None);
        }
        Err(raised("Unsupported type for region"))
    }

    pub(super) fn as_region(&self) -> SubimageRegion<'_> {
        match self {
            Self::Text(text) => SubimageRegion::Text(text),
            Self::Record(record) => SubimageRegion::Record(record),
        }
    }

    pub(super) fn history(&self) -> String {
        match self {
            Self::Text(text) => format!("{text:?}"),
            Self::Record(_) => "{...}".to_owned(),
        }
    }
}

/// Draw the same distinction CASA makes between its default record and an
/// explicitly supplied empty text or array.
pub(super) struct RegionArgument {
    selection: Option<Region>,
    record: bool,
    empty_history: &'static str,
}

impl Default for RegionArgument {
    fn default() -> Self {
        Self {
            selection: None,
            record: true,
            empty_history: "{...}",
        }
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for RegionArgument {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let record = value.is_instance_of::<PyDict>();
        let empty_history = if record {
            "{...}"
        } else if value.is_instance_of::<PyString>() {
            r#""""#
        } else {
            "[]"
        };
        Ok(Self {
            selection: Region::capture(value.py(), (!value.is_none()).then_some(&value))?,
            record,
            empty_history,
        })
    }
}

impl RegionArgument {
    pub(super) fn empty_text() -> Self {
        Self {
            selection: None,
            record: false,
            empty_history: r#""""#,
        }
    }

    pub(super) fn selection(&self) -> Option<SubimageRegion<'_>> {
        self.selection.as_ref().map(Region::as_region)
    }

    pub(super) fn validate_selectors(
        &self,
        spatial: &str,
        chans: &str,
        stokes: &str,
    ) -> PyResult<()> {
        if self.record || self.selection.is_some() {
            if !spatial.is_empty() {
                return Err(raised(
                    "box and regionPtr cannot be simultaneously specified",
                ));
            }
            if !chans.is_empty() || !stokes.is_empty() {
                return Err(raised(
                    "regionPtr and regionName, chans, and/or stokes cannot be simultaneously specified",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn history(&self) -> String {
        self.selection
            .as_ref()
            .map_or_else(|| self.empty_history.into(), Region::history)
    }
}

pub(super) fn require_bool(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    value
        .cast::<PyBool>()
        .map(|value| value.is_true())
        .map_err(|_| PyTypeError::new_err("expected bool"))
}

fn empty_variant(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
        return Ok(value
            .try_iter()?
            .all(|item| item.is_ok_and(|item| item.is_instance_of::<PyBool>())));
    }
    Ok(value.extract::<Variant>()?.is_empty_sequence())
}

pub(super) fn mask_argument(value: Option<&Bound<'_, PyAny>>) -> PyResult<String> {
    let Some(value) = value else {
        return Ok(String::new());
    };
    if let Ok(text) = value.cast::<PyString>() {
        return Ok(text.to_str()?.to_owned());
    }
    if empty_variant(value)? {
        return Ok(String::new());
    }
    Err(raised("Mask is not understood, try a valid LEL string"))
}

struct Request<'a> {
    outfile: &'a str,
    region: Option<Region>,
    mask: String,
    dropdeg: bool,
    overwrite: bool,
    list: bool,
    stretch: bool,
    wantreturn: bool,
    keepaxes: Vec<i64>,
}

impl Request<'_> {
    fn copy_selection<T: ImagePixel + Copy>(
        &self,
        source: &dyn ImageInterface<T>,
        coordinates: Option<RecordValue>,
        beams: &RecordValue,
        path: Option<&Path>,
        logs: &mut Vec<LogLine>,
    ) -> PyResult<Storage<T>> {
        let options = SubimageOptions {
            region: self.region.as_ref().map(Region::as_region),
            dropdeg: self.dropdeg,
            keepaxes: &self.keepaxes,
        };
        let plan = SubimagePlan::new_preserving_beams(source, coordinates, options, beams)
            .map_err(raised)?;
        if self.list {
            let (blc, trc) = plan.bounds();
            logs.push(LogLine::info(format!(
                "Selected bounding box {blc:?} to {trc:?}"
            )));
        }
        let mut result = Storage::create(plan.shape.clone(), plan.coordinates.clone(), path, None)
            .map_err(raised)?;
        let expression = (!self.mask.is_empty()).then_some(self.mask.as_str());
        with_storage!(&mut result, copy =>
            plan.copy_to(source, copy.as_mut(), expression, self.stretch)
        )
        .map_err(raised)?;
        if let Some(record) = plan.coordinate_record {
            match &mut result {
                Storage::Paged(image) => image.set_coordinate_record(record).map_err(raised)?,
                Storage::Temp {
                    record: coordinates,
                    ..
                } => *coordinates = record,
            }
        }
        Ok(result)
    }

    fn build<T: ImagePixel + Copy>(
        &self,
        source: &Storage<T>,
        beams: &RecordValue,
        path: Option<&Path>,
        logs: &mut Vec<LogLine>,
    ) -> PyResult<Storage<T>> {
        let coordinates = match source {
            Storage::Paged(image) => match image.table().keywords().get("coords") {
                Some(Value::Record(record)) => Some(record.clone()),
                _ => None,
            },
            Storage::Temp { record, .. } => Some(record.clone()),
        };
        let mut result = with_storage!(
            source, image => self.copy_selection(image.as_ref(), coordinates, beams, path, logs)
        )?;
        let rows = match source {
            Storage::Paged(image) => {
                image_history_rows(image.name().expect("persistent image")).map_err(raised)?
            }
            Storage::Temp { image, .. } => image.history_rows().map_err(raised)?.into_owned(),
        };
        with_storage!(&mut result, image => image.append_history_entries(&rows)).map_err(raised)?;
        Ok(result)
    }

    fn history(&self) -> String {
        let region = self
            .region
            .as_ref()
            .map_or_else(|| "\"\"".to_owned(), Region::history);
        format!(
            "ia.subimage(outfile={:?}, region={region}, mask={:?}, dropdeg={}, overwrite={}, list={}, stretch={}, wantreturn={}, keepaxes={:?})",
            self.outfile,
            self.mask,
            self.dropdeg,
            self.overwrite,
            self.list,
            self.stretch,
            self.wantreturn,
            self.keepaxes
        )
    }
}

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", region=None, mask=None, dropdeg=false, overwrite=false, list=true, stretch=false, wantreturn=true, keepaxes=None), text_signature = "($self, outfile='', region='', mask='', dropdeg=False, overwrite=False, list=True, stretch=False, wantreturn=True, keepaxes=[])")]
    fn subimage(
        &self,
        py: Python<'_>,
        outfile: &str,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        dropdeg: bool,
        overwrite: bool,
        list: bool,
        stretch: bool,
        wantreturn: bool,
        keepaxes: Option<IntList>,
    ) -> PyResult<Py<Self>> {
        let request = Request {
            outfile,
            region: Region::argument(py, region, "subimage")?,
            mask: mask_argument(mask)?,
            dropdeg,
            overwrite,
            list,
            stretch,
            wantreturn,
            keepaxes: keepaxes.map_or_else(Vec::new, |axes| axes.0),
        };
        let mut logs = Vec::new();
        let result = py.detach(|| {
            let state = self.state();
            let attached = state.image.as_ref().ok_or_else(|| raised("Unable to create image"))?;
            if outfile.is_empty() && !wantreturn {
                logs.push(LogLine::warning("outfile was not specified and wantreturn is false. The resulting image will be inaccessible"));
            }
            publish_image(outfile, overwrite, |path| {
                // Release the source handle before publication takes the image registry lock.
                let mut original = attached.lock();
                attached.require_pixel_read_lock(&mut original)?;
                let beams = attached.beam_view().clone();
                if !outfile.is_empty() && let Some(source) = original.path() {
                    ensure_outfile_does_not_alias_input(Path::new(outfile), source).map_err(raised)?;
                }
                let mut copy = match &*original {
                    ImageHandle::Float(Storage::Temp { image, record, fits: Some(fits) }) => {
                        let source = fits.image_view(image);
                        let mut storage = request.copy_selection(&source, Some(record.clone()), beams.record(), path, &mut logs)?;
                        with_storage!(&mut storage, product =>
                            product.append_history_entries(&image.history_rows().map_err(raised)?)
                        ).map_err(raised)?;
                        ImageHandle::Float(storage)
                    }
                    ImageHandle::Float(source) => ImageHandle::Float(request.build(source, beams.record(), path, &mut logs)?),
                    ImageHandle::Double(source) => ImageHandle::Double(request.build(source, beams.record(), path, &mut logs)?),
                    ImageHandle::Complex(source) => ImageHandle::Complex(request.build(source, beams.record(), path, &mut logs)?),
                    ImageHandle::DComplex(source) => ImageHandle::DComplex(request.build(source, beams.record(), path, &mut logs)?),
                };
                if state.history {
                    copy.record_history("subimage", request.history())?;
                }
                copy.save().map_err(raised)?;
                Ok(copy)
            })
        });
        emit(py, "casars.tools.image", &logs)?;
        let result = result?;
        if wantreturn {
            Py::new(py, Self::attached(result))
        } else {
            py.detach(|| drop(result));
            Py::new(py, Self::new())
        }
    }
}
