// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_images::ImagePixel;
use casa_images::analysis::{
    SubimageOptions, SubimagePlan, SubimageRegion, ensure_outfile_does_not_alias_input,
};
use casa_images::image::image_history_rows;
use casa_types::{RecordValue, ScalarValue, Value};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyList, PyString, PyTuple};

use crate::convert::{IntList, Variant};
use crate::tools::{LogLine, emit, not_yet_implemented_in, table::casa_record};

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
        if let Some(Self::Record(record)) = &region
            && !matches!(record.get("name"), Some(Value::Scalar(ScalarValue::String(name)))
                if matches!(name.as_str(), "LCSlicer" | "LCBox" | "WCBox" | "WCPolygon" | "WCEllipsoid" | "WCUnion"))
        {
            return Err(super::stub(method).unwrap_err());
        }
        Ok(region)
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
    fn build<T: ImagePixel + Copy>(
        &self,
        source: &Storage<T>,
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
        let options = SubimageOptions {
            region: self.region.as_ref().map(Region::as_region),
            dropdeg: self.dropdeg,
            keepaxes: &self.keepaxes,
        };
        let plan = with_storage!(source, image =>
            SubimagePlan::new(image.as_ref(), coordinates, options)
        )
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
        with_storage!(source, original => with_storage!(&mut result, copy =>
            plan.copy_to(original.as_ref(), copy.as_mut(), expression, self.stretch)
        ))
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
        let rows = match source {
            Storage::Paged(image) => {
                image_history_rows(image.name().expect("persistent image")).map_err(raised)?
            }
            Storage::Temp { image, .. } => image.history_rows().to_vec(),
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
    ) -> PyResult<Option<Py<Self>>> {
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
                if original.fits().is_some() {
                    return not_yet_implemented_in("image", "subimage", 1627);
                }
                attached.require_pixel_read_lock(&mut original)?;
                if !outfile.is_empty() && let Some(source) = original.path() {
                    ensure_outfile_does_not_alias_input(Path::new(outfile), source).map_err(raised)?;
                }
                let mut copy = match &*original {
                    ImageHandle::Float(source) => ImageHandle::Float(request.build(source, path, &mut logs)?),
                    ImageHandle::Double(source) => ImageHandle::Double(request.build(source, path, &mut logs)?),
                    ImageHandle::Complex(source) => ImageHandle::Complex(request.build(source, path, &mut logs)?),
                    ImageHandle::DComplex(source) => ImageHandle::DComplex(request.build(source, path, &mut logs)?),
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
            Py::new(py, Self::attached(result)).map(Some)
        } else {
            py.detach(|| drop(result));
            Ok(None)
        }
    }
}
