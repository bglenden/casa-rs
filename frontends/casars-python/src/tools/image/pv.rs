// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.pv` wrapper and `casa_contracts` ParameterValue shim delegating to [`casa_imageanalysis::tool::ops_spatial::pv_image`].

use std::path::Path;

use casa_contracts::ParameterValue;
use casa_imageanalysis::tool::ops_spatial::pv_image;
use casa_imageanalysis::{ImpvRequest, PvAmount, PvPosition};
use pyo3::prelude::*;

use crate::convert::{IntList, Variant};
use crate::parameters::parameter_value;

use super::PyImageTool;
use super::handle::{emit_tool_logs, external_open_check, raised};
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
        let region_sel = region.selection();
        let (output, logs) = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(input) = &state.image else {
                return Ok((None, Vec::new()));
            };
            pv_image(
                input,
                &request,
                region_sel,
                outfile,
                overwrite,
                wantreturn,
                state.history.then_some(history),
                external_open_check(),
            )
            .map_err(raised)
        })?;
        emit_tool_logs(py, &logs)?;
        Py::new(py, output.map_or_else(Self::new, Self::attached))
    }
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
            param @ ParameterValue::Table(_) => {
                match casa_contracts::parameter_value_to_casacore(&param) {
                    Ok(casa_core::Value::Record(record)) => PvAmount::Record(record),
                    Ok(_) => return Err(invalid()),
                    Err(error) => return Err(raised(error)),
                }
            }
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
