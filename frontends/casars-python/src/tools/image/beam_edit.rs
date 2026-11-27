// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.setrestoringbeam` wrapper delegating to [`casa_imageanalysis::tool::beam_ops`].

use std::path::Path;

use casa_core::quanta::{Quantity, Unit};
use casa_imageanalysis::tool::beam_ops::{
    FullSetBeamRequest, format_beam_record_history, set_restoring_beam_full,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::{Variant, quantity};
use crate::tools::table::casa_record;
use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::handle::raised;

const UNSPECIFIED_PA: &str = "Not specified";

fn beam_quantity(value: &Variant, label: &str) -> PyResult<Quantity> {
    if !matches!(
        value,
        Variant::Text(_) | Variant::Record(_) | Variant::Number(_)
    ) || matches!(value, Variant::Text(text) if text == UNSPECIFIED_PA)
    {
        return Err(raised(
            "If beam record not specified, all of major, minor, and positionangle must be specified and be either valid quantity records or strings",
        ));
    }
    let value = quantity(value)?;
    if !value.unit().conformant(&Unit::dimensionless())
        && !value
            .unit()
            .conformant(&Unit::new("rad").expect("built-in angular unit"))
    {
        return Err(raised(format!(
            "Exception: {label} must have angular units ({} is not)..",
            value.unit().name()
        )));
    }
    Ok(value)
}

pub(super) fn input_history(value: &Variant) -> String {
    match value {
        Variant::Text(text) => format!("\"{text}\""),
        _ => value.describe(),
    }
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (major=Variant::Empty, minor=Variant::Empty, pa=Variant::Text(UNSPECIFIED_PA.into()), beam=None, remove=false, log=true, channel=-1, polarization=-1, imagename=""), text_signature = "($self, major=[], minor=[], pa='Not specified', beam={}, remove=False, log=True, channel=-1, polarization=-1, imagename='')")]
    #[allow(clippy::too_many_arguments)]
    fn setrestoringbeam(
        &self,
        py: Python<'_>,
        major: Variant,
        minor: Variant,
        pa: Variant,
        beam: Option<&Bound<'_, PyDict>>,
        remove: bool,
        log: bool,
        channel: i64,
        polarization: i64,
        imagename: &str,
    ) -> PyResult<bool> {
        let beam = casa_record(py, beam)?;
        let beam_history = format_beam_record_history(&beam).map_err(raised)?;
        let message = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(None);
            };
            let has_axis_args = !major.is_empty_sequence()
                || !minor.is_empty_sequence()
                || !matches!(&pa, Variant::Text(text) if text == UNSPECIFIED_PA);
            let quantities = if imagename.is_empty() && !remove && beam.is_empty() {
                // CASA checks for missing arguments before converting any of them.
                for (name, value) in [("major", &major), ("minor", &minor), ("positionangle", &pa)]
                {
                    if value.is_empty_sequence()
                        || matches!(value, Variant::Text(text) if text.is_empty())
                    {
                        return Err(raised(format!(
                            "beam record is empty, {name} must be specified"
                        )));
                    }
                }
                Some([
                    beam_quantity(&major, "Major axis")?,
                    beam_quantity(&minor, "Minor axis")?,
                    beam_quantity(&pa, "Position angle")?,
                ])
            } else {
                None
            };
            let major_h = input_history(&major);
            let minor_h = input_history(&minor);
            let pa_h = input_history(&pa);
            let request = FullSetBeamRequest {
                quantities,
                beam: &beam,
                beam_history: &beam_history,
                input_histories: [&major_h, &minor_h, &pa_h],
                remove,
                log,
                channel,
                polarization,
                imagename,
                imagename_path: (!imagename.is_empty()).then(|| Path::new(imagename)),
                has_axis_args,
            };
            set_restoring_beam_full(attachment, &request, state.history).map_err(raised)
        })?;
        if let Some(message) = message {
            if log {
                emit(py, "casars.tools.image", &[LogLine::info(message)])?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
