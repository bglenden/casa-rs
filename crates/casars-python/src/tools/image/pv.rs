// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_images::{
    ImpvRequest, PvAmount, PvPosition,
    analysis::{
        SubimageRegion, ensure_artifacts_do_not_overlap, ensure_outfile_does_not_alias_input,
        generate_image_pv,
    },
    image::image_history_rows,
};
use casa_provider_contracts::ParameterValue;
use pyo3::prelude::*;

use crate::convert::{IntList, Variant};
use crate::parameters::parameter_value;
use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::attachment::{AttachedImage, read_locked};
use super::beams::subset_beam_record;
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised};
use super::subimage::{RegionArgument, require_bool};

#[pymethods]
impl PyImageTool {
    /// Extract a position–velocity strip from a Float sky cube.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", start=None, end=None, center=None, length=None, pa=None, width=None, unit="arcsec", overwrite=false, region=RegionArgument::default(), chans="", stokes="", mask="", stretch=false, wantreturn=true), text_signature = "($self, outfile='', start=[], end=[], center=[], length=[], pa=[], width=1, unit='arcsec', overwrite=False, region={}, chans='', stokes='', mask='', stretch=False, wantreturn=True)")]
    fn pv(
        &self,
        py: Python<'_>,
        outfile: &str,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        center: Option<&Bound<'_, PyAny>>,
        length: Option<&Bound<'_, PyAny>>,
        pa: Option<&Bound<'_, PyAny>>,
        width: Option<&Bound<'_, PyAny>>,
        unit: &str,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        region: RegionArgument,
        chans: &str,
        stokes: &str,
        mask: &str,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
        #[pyo3(from_py_with = require_bool)] wantreturn: bool,
    ) -> PyResult<Py<Self>> {
        let attached = py.detach(|| self.state().image.is_some());
        if !attached {
            return Py::new(py, Self::new());
        }
        region.validate_selectors("", chans, stokes)?;
        let [start_text, end_text, center_text, length_text, pa_text] =
            [start, end, center, length, pa].map(representation);
        let width_text = if width.is_some() {
            representation(width)?
        } else {
            "1".into()
        };
        let start = position(start, "start")?;
        let end = position(end, "end")?;
        let center = position(center, "center")?;
        let length = scalar(length, "length")?;
        let pa = scalar(pa, "pa")?;
        let mode = match (&start, &end, &center, &length, &pa) {
            (Some(_), Some(_), None, None, None) => "coords",
            (None, None, Some(_), Some(_), Some(_)) => "length",
            _ => {
                return Err(raised(
                    "Specify both start and end, or all of center, length and pa",
                ));
            }
        };
        let width = scalar(width, "width")?.unwrap_or(PvAmount::Whole(1));
        if matches!(width, PvAmount::Fractional(_)) {
            return Err(raised("Unsupported data type for width double"));
        }
        let request = ImpvRequest {
            mode: mode.into(),
            start,
            end,
            center,
            length,
            pa,
            width,
            unit: unit.into(),
            outfile: Path::new(outfile.trim()).into(),
            overwrite,
            chans: Some(chans.into()),
            stokes: Some(stokes.into()),
            mask: Some(mask.into()),
            stretch,
            ..ImpvRequest::default()
        };
        let history = format!(
            "ia.pv(outfile={outfile:?}, start={start_text}, end={end_text}, center={center_text}, length={length_text}, pa={pa_text}, width={width_text}, unit={unit:?}, overwrite={}, region={}, chans={chans:?}, stokes={stokes:?}, mask={mask:?}, stretch={}, wantreturn={})",
            python_bool(overwrite),
            region.history(),
            python_bool(stretch),
            python_bool(wantreturn),
            start_text = start_text?,
            end_text = end_text?,
            center_text = center_text?,
            length_text = length_text?,
            pa_text = pa_text?,
        );
        let region = region.selection();
        let output = py.detach(|| {
            let state = self.state();
            let Some(input) = &state.image else {
                return Ok(None);
            };
            let result = publish_image(outfile.trim(), overwrite, |path| {
                build(
                    input,
                    path,
                    region,
                    &request,
                    state.history.then_some(history),
                )
            })?;
            Ok::<_, PyErr>(wantreturn.then_some(result))
        })?;
        if !wantreturn && outfile.trim().is_empty() {
            emit(
                py,
                "casars.tools.image",
                &[LogLine::warning(
                    "outfile was not specified and wantreturn is false. The resulting image will be inaccessible",
                )],
            )?;
        }
        Py::new(py, output.map_or_else(Self::new, Self::attached))
    }
}

fn build(
    input: &AttachedImage,
    stage: Option<&Path>,
    region: Option<SubimageRegion<'_>>,
    request: &ImpvRequest,
    history: Option<String>,
) -> PyResult<ImageHandle> {
    read_locked(input, |handle| {
        let ImageHandle::Float(storage) = handle else {
            return Err(raised("This method only supports Float valued images"));
        };
        if stage.is_some() {
            for source in handle
                .path()
                .into_iter()
                .chain(input.operand_paths().iter().map(|path| path.as_path()))
            {
                ensure_outfile_does_not_alias_input(&request.outfile, source).map_err(raised)?;
            }
            if let Some(region) = region.and_then(SubimageRegion::external_path) {
                ensure_artifacts_do_not_overlap(&request.outfile, region).map_err(raised)?;
            }
        }
        let view = input.beam_view();
        let mut result = storage.with_float_view(|image| {
            let mut beams = image.image_info().map_err(raised)?.to_record();
            view.apply_to(&mut beams);
            generate_image_pv(image, handle.coordinate_record(), &beams, request, region)
                .map_err(raised)
        })?;
        let source_beams = view.beam_set().map_err(raised)?;
        let mut info = result.image.image_info().map_err(raised)?.to_record();
        if source_beams.is_multi() {
            let [channels, polarizations] = &result.beam_planes;
            subset_beam_record(
                &mut info,
                Some(view.record()),
                &source_beams,
                channels,
                polarizations,
            )
            .map_err(raised)?;
        } else {
            view.apply_to(&mut info);
        }
        result.image.set_image_info_record(info).map_err(raised)?;
        let entries = match storage {
            Storage::Paged(image) => {
                image_history_rows(image.name().expect("persistent image")).map_err(raised)?
            }
            Storage::Temp { image, .. } => image.history_rows().map_err(raised)?.into_owned(),
        };
        result
            .image
            .append_history_entries(&entries)
            .map_err(raised)?;
        if let Some(line) = history {
            result
                .image
                .append_history_at(
                    "image::pv",
                    &[
                        concat!("version: casa-rs ", env!("CARGO_PKG_VERSION")).into(),
                        line,
                    ],
                )
                .map_err(raised)?;
        }
        let record = result.coordinate_record;
        let output = if let Some(path) = stage {
            let mut image = result.image.save_as(path).map_err(raised)?;
            image.set_coordinate_record(record).map_err(raised)?;
            image.save().map_err(raised)?;
            Storage::Paged(Box::new(image))
        } else {
            Storage::Temp {
                image: Box::new(result.image),
                fits: None,
                record,
            }
        };
        Ok(ImageHandle::Float(output))
    })
}

fn position(value: Option<&Bound<'_, PyAny>>, name: &str) -> PyResult<Option<PvPosition>> {
    let Some(value) = value else { return Ok(None) };
    let variant: Variant = value.extract()?;
    let invalid = || {
        raised(format!(
            "{name} must contain two pixel values or a sky direction"
        ))
    };
    let position = match variant {
        Variant::Empty => return Ok(None),
        Variant::Text(text) if text.is_empty() => return Ok(None),
        Variant::Text(text) => PvPosition::Text(text),
        Variant::Texts(items) => PvPosition::World(items.try_into().map_err(|_| invalid())?),
        Variant::Numbers(items) => {
            if let Ok(IntList(integers)) = value.extract::<IntList>() {
                PvPosition::Integral(integers.try_into().map_err(|_| invalid())?)
            } else {
                PvPosition::Pixel(items.try_into().map_err(|_| invalid())?)
            }
        }
        _ => return Err(invalid()),
    };
    Ok(Some(position))
}

fn scalar(value: Option<&Bound<'_, PyAny>>, name: &str) -> PyResult<Option<PvAmount>> {
    let Some(value) = value else { return Ok(None) };
    let variant: Variant = value.extract()?;
    let invalid = || raised(format!("Unsupported data type for {name}"));
    let measure = match variant {
        Variant::Empty => return Ok(None),
        Variant::Text(text) if text.is_empty() => return Ok(None),
        Variant::Text(text) => PvAmount::Angular(text),
        Variant::Number(number) => value
            .extract::<i64>()
            .map_or(PvAmount::Fractional(number), PvAmount::Whole),
        Variant::Record(_) => match parameter_value(value)? {
            ParameterValue::Table(fields) => PvAmount::Quantity(fields),
            _ => return Err(invalid()),
        },
        _ => return Err(invalid()),
    };
    Ok(Some(measure))
}

fn representation(value: Option<&Bound<'_, PyAny>>) -> PyResult<String> {
    value.map_or_else(
        || Ok("[]".into()),
        |value| Ok(value.repr()?.to_str()?.to_owned()),
    )
}

fn python_bool(value: bool) -> &'static str {
    if value { "True" } else { "False" }
}
