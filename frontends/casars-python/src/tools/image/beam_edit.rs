// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_core::quanta::{Quantity, Unit, casacore::stream_double};
use casa_core::{RecordValue, ScalarValue, Value};
use casa_images::{BeamComponent, ImagePixelType, StoredGaussianBeam};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::{Variant, quantity, value::CasaValue};
use crate::tools::{LogLine, emit, table::casa_record};

use super::PyImageTool;
use super::beam_view::BeamView;
use super::beams::{
    axis_length, image_info, image_info_record, plane_axes, stored_beam_record, subrecord,
};
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};

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

pub(super) fn beam_for_units(
    current: &BeamView,
    channel: i64,
    polarization: i64,
) -> PyResult<Option<StoredGaussianBeam>> {
    let beams = current.beam_set().map_err(raised)?;
    if beams.is_empty() {
        return Ok(None);
    }
    let index = |index: i64, length: usize| {
        if length == 1 {
            Ok(0)
        } else if index >= 0 && (index as usize) < length {
            Ok(index as usize)
        } else {
            Err(raised(
                "Failed AlwaysAssert chan >=0 && chan < Int(nchan()) && stokes >= 0 && stokes < Int(nstokes())",
            ))
        }
    };
    let record = stored_beam_record(
        Some(current.record()),
        &beams,
        index(channel, beams.n_channels())?,
        index(polarization, beams.n_stokes())?,
    );
    StoredGaussianBeam::from_record(&record)
        .map(Some)
        .map_err(raised)
}

fn beam_from_arguments(
    current: &BeamView,
    major: &Variant,
    minor: &Variant,
    pa: &Variant,
    channel: i64,
    polarization: i64,
) -> PyResult<RecordValue> {
    // CASA checks for missing arguments before it tries to convert any of them.
    for (name, value) in [("major", major), ("minor", minor), ("positionangle", pa)] {
        if value.is_empty_sequence() || matches!(value, Variant::Text(text) if text.is_empty()) {
            return Err(raised(format!(
                "beam record is empty, {name} must be specified"
            )));
        }
    }
    let major = beam_quantity(major, "Major axis")?;
    let minor = beam_quantity(minor, "Minor axis")?;
    let pa = beam_quantity(pa, "Position angle")?;
    let dimensionless = Unit::dimensionless();
    let existing = if [&major, &minor, &pa]
        .iter()
        .any(|value| value.unit().conformant(&dimensionless))
    {
        beam_for_units(current, channel, polarization)?
    } else {
        None
    };
    let with_units = |value: Quantity, component, default| {
        if !value.unit().conformant(&dimensionless) {
            return value;
        }
        let unit = existing.as_ref().map_or_else(
            || Unit::new(default).expect("built-in beam unit"),
            |beam| beam.component(component).unit().clone(),
        );
        Quantity::with_unit(value.value(), unit)
    };
    let major = with_units(major, BeamComponent::Major, "arcsec");
    let minor = with_units(minor, BeamComponent::Minor, "arcsec");
    let pa = with_units(pa, BeamComponent::PositionAngle, "deg");
    if major.value() <= 0.0 || minor.value() <= 0.0 {
        return Err(raised(
            "Exception: Both the major and minor axes must be non-negative.",
        ));
    }
    let beam = StoredGaussianBeam::new(major, minor, pa).map_err(|error| {
        if error.to_string().contains("greater or equal") {
            raised("Exception: Major axis must be greater or equal to minor axis.")
        } else {
            raised(error)
        }
    })?;
    Ok(beam.to_record())
}

fn beam_from_record(record: &RecordValue) -> PyResult<RecordValue> {
    if ["beams", "nChannels", "nStokes"]
        .iter()
        .all(|key| record.get(key).is_some())
    {
        return Err(raised(
            "Exception: Both the major and minor axes must be non-negative.",
        ));
    }
    if record.len() != 3 {
        return Err(raised("Beam record does not contain 3 fields"));
    }
    for name in ["major", "minor", "positionangle"] {
        if record.get(name).is_none() {
            return Err(raised(format!(
                "Field {name} missing from restoring beam record"
            )));
        }
    }
    let beam = StoredGaussianBeam::from_record(record).map_err(|error| {
        if error.to_string().contains("greater or equal") {
            raised("Exception: Major axis must be greater or equal to minor axis.")
        } else {
            raised(error)
        }
    })?;
    if beam.is_null() {
        return Err(raised(
            "Exception: Both the major and minor axes must be non-negative.",
        ));
    }
    let angle = beam.wrapped_position_angle().map_err(raised)?;
    Ok(beam
        .with_component(BeamComponent::PositionAngle, angle)
        .map_err(raised)?
        .to_record())
}

fn grid_record(channels: usize, stokes: usize) -> RecordValue {
    let mut grid = RecordValue::default();
    grid.upsert(
        "nChannels",
        Value::Scalar(ScalarValue::Int32(channels as i32)),
    );
    grid.upsert("nStokes", Value::Scalar(ScalarValue::Int32(stokes as i32)));
    grid
}

fn replace_all(info: &mut RecordValue, beam: RecordValue, channels: usize, stokes: usize) {
    if channels == 1 && stokes == 1 {
        info.upsert("restoringbeam", Value::Record(beam));
    } else {
        let mut grid = grid_record(channels, stokes);
        for index in 0..channels * stokes {
            grid.upsert(format!("*{index}"), Value::Record(beam.clone()));
        }
        info.upsert("perplanebeams", Value::Record(grid));
    }
}

fn copy_beams(
    destination: &mut RecordValue,
    source: &str,
    channels: usize,
    stokes: usize,
) -> PyResult<()> {
    let path = Path::new(source);
    if !path.exists() {
        return Err(raised(format!(
            "Exception: File '{source}' does not exist."
        )));
    }
    let image = ImageHandle::open(path).map_err(raised)?;
    if image.kind() != ImagePixelType::Float32 {
        return Err(raised(format!(
            "Logic Error: {source} has a different data type than the data type of the requested object"
        )));
    }
    let beams = image_info(&image).map_err(raised)?.beam_set;
    if beams.is_empty() {
        return Err(raised(format!("Image {source} has no beam")));
    }
    if beams.n_channels() != 1 && beams.n_channels() != channels {
        return Err(raised("Exception: Number of channels is not consistent."));
    }
    if beams.n_stokes() != 1 && beams.n_stokes() != stokes {
        return Err(raised(
            "Exception: Number of polarizations is not consistent.",
        ));
    }
    let source = image_info_record(&image);
    if beams.is_single() {
        destination.upsert(
            "restoringbeam",
            Value::Record(stored_beam_record(source, &beams, 0, 0)),
        );
    } else {
        let grid = source
            .and_then(|record| subrecord(record, "perplanebeams"))
            .cloned()
            .unwrap_or_else(|| beams.to_record());
        destination.upsert("perplanebeams", Value::Record(grid));
    }
    Ok(())
}

fn edit_beam(
    image: &ImageHandle,
    view: &BeamView,
    beam: RecordValue,
    channel: i64,
    polarization: i64,
) -> PyResult<RecordValue> {
    let beams = view.beam_set().map_err(raised)?;
    let mut result = image_info_record(image)
        .cloned()
        .unwrap_or(image_info(image).map_err(raised)?.to_record());
    result.remove("restoringbeam");
    result.remove("perplanebeams");
    let indexed = channel >= 0 || polarization >= 0;
    if beams.is_single() && indexed {
        return Err(raised(
            "Channel and/or polarization has been specified, but this image has a single (global restoring beam. This beam will not be altered. If you really want to modify the global beam, rerun setting both channel and polarization less than zero",
        ));
    }
    if !indexed || beams.is_empty() {
        let (channels, stokes) = if indexed || beams.is_multi() {
            let (spectral, stokes_axis) = plane_axes(&image.coordinate_record())?;
            (
                axis_length(image, spectral)?,
                axis_length(image, stokes_axis)?,
            )
        } else {
            (1, 1)
        };
        replace_all(&mut result, beam, channels, stokes);
        return Ok(result);
    }
    let channels = beams.n_channels();
    let stokes = beams.n_stokes();
    if channel >= channels as i64 || polarization >= stokes as i64 {
        return Err(raised(
            "Failed AlwaysAssert Int(chan) < _beams.shape()[0] && Int(stokes) < _beams.shape()[1]",
        ));
    }
    let mut grid = subrecord(view.record(), "perplanebeams")
        .cloned()
        .unwrap_or_else(|| beams.to_record());
    for pol in 0..stokes {
        for chan in 0..channels {
            if (polarization < 0 || polarization as usize == pol)
                && (channel < 0 || channel as usize == chan)
            {
                grid.upsert(
                    format!("*{}", chan + pol * channels),
                    Value::Record(beam.clone()),
                );
            }
        }
    }
    result.upsert("perplanebeams", Value::Record(grid));
    Ok(result)
}

pub(super) fn input_history(value: &Variant) -> String {
    match value {
        Variant::Text(text) => format!("\"{text}\""),
        _ => value.describe(),
    }
}

fn record_history_value(record: &RecordValue) -> PyResult<String> {
    if record.is_empty() {
        return Ok("{...}".into());
    }
    let field = |name: &str| -> PyResult<String> {
        let field = subrecord(record, name)
            .ok_or_else(|| raised(format!("RecordInterface: field {name} is unknown")))?;
        let Some(Value::Scalar(ScalarValue::String(unit))) = field.get("unit") else {
            return Err(raised("RecordInterface: invalid unit field"));
        };
        let value = match field.get("value").map(CasaValue::from_value) {
            Some(CasaValue::Int(value)) => stream_double(value as f64),
            Some(CasaValue::Float(value)) => stream_double(value),
            _ => return Err(raised("RecordInterface: invalid value field")),
        };
        Ok(format!("{{'value': {value}, 'unit': '{unit}'}}"))
    };
    let angle = if record.get("pa").is_some() {
        "pa"
    } else {
        "positionangle"
    };
    Ok(format!(
        "{{'major': {}, 'minor': {}, 'positionangle': {}}}",
        field("major")?,
        field("minor")?,
        field(angle)?,
    ))
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
        let message = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else { return Ok(None) };
            let mut image = attachment.lock();
            attachment.require_lock(&mut image, casa_tables::LockType::Write)?;
            let mut view = attachment.beam_view();
            let (info, message) = if !imagename.is_empty() {
                if !major.is_empty_sequence() || !minor.is_empty_sequence()
                    || !matches!(&pa, Variant::Text(text) if text == UNSPECIFIED_PA)
                {
                    return Err(raised("Cannot specify both imagename and major, minor, and/or pa"));
                }
                if remove {
                    return Err(raised("remove cannot be true if imagename is specified"));
                }
                if !beam.is_empty() {
                    return Err(raised("beam must be empty if imagename specified"));
                }
                if channel >= 0 || polarization >= 0 {
                    return Err(raised("Neither channel nor polarization can be non-negative if imagename is specified"));
                }
                let mut info = image_info_record(&image).cloned()
                    .unwrap_or(image_info(&image).map_err(raised)?.to_record());
                info.remove("restoringbeam");
                info.remove("perplanebeams");
                let (spectral, stokes) = plane_axes(&image.coordinate_record())?;
                copy_beams(&mut info, imagename, axis_length(&image, spectral)?, axis_length(&image, stokes)?)?;
                (info, "Set image beam set".to_owned())
            } else if remove {
                let mut info = image_info_record(&image).cloned()
                    .unwrap_or(image_info(&image).map_err(raised)?.to_record());
                info.remove("restoringbeam");
                info.remove("perplanebeams");
                (info, "Removed restoring beam(s)".to_owned())
            } else {
                let updated = if beam.is_empty() {
                    beam_from_arguments(&view, &major, &minor, &pa, channel, polarization)?
                } else {
                    beam_from_record(&beam)?
                };
                let message = if log {
                    format!("Set restoring beam {}", record_history_value(&updated)?)
                } else {
                    String::new()
                };
                (edit_beam(&image, &view, updated, channel, polarization)?, message)
            };
            let beam = record_history_value(&beam)?;
            let command = state.history.then(|| {
                format!(
                    "ia.setrestoringbeam(major={}, minor={}, pa={}, beam={beam}, remove={remove}, log={log}, channel={channel}, polarization={polarization}, imagename={imagename:?})",
                    input_history(&major), input_history(&minor), input_history(&pa),
                )
            });
            with_pixels!(&mut *image, storage => with_storage!(storage, image => image.set_image_info_record(info.clone()))).map_err(raised)?;
            view.capture(&info);
            if let Some(command) = command {
                image.record_history("setrestoringbeam", command)?;
            }
            image.save().map_err(raised)?;
            Ok::<_, PyErr>(Some(message))
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
