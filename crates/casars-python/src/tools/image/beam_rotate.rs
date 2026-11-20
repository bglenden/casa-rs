// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_images::{BeamComponent, ImageBeamSet, StoredGaussianBeam, beam::wrap_position_angle};
use casa_tables::LockType;
use casa_types::quanta::{Quantity, Unit, casacore};
use casa_types::{RecordValue, Value};
use pyo3::prelude::*;

use crate::convert::{Variant, quantity};
use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::beam_edit::input_history;
use super::beams::{image_info, image_info_record, stored_beam_record, subrecord};
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};

#[derive(Clone)]
struct BeamChange {
    original: StoredGaussianBeam,
    rotated: StoredGaussianBeam,
}

impl BeamChange {
    fn new(original: StoredGaussianBeam, angle: &Quantity) -> PyResult<Self> {
        let position = original.wrapped_position_angle().map_err(raised)?;
        let increment = casacore::get_conformant_value(angle, position.unit()).map_err(raised)?;
        let new_position =
            Quantity::with_unit(position.value() + increment, position.unit().clone());
        let new_position = wrap_position_angle(&new_position).map_err(raised)?;
        let rotated = original
            .with_component(BeamComponent::PositionAngle, new_position)
            .map_err(raised)?;
        Ok(Self { original, rotated })
    }

    fn messages(&self, name: &str, kind: &str) -> PyResult<[String; 2]> {
        let describe = |beam: &StoredGaussianBeam| {
            let format = |value: &Quantity| {
                format!(
                    "{} {}",
                    casacore::stream_double(value.value()),
                    value.unit().name()
                )
            };
            Ok::<_, PyErr>(format!(
                "major: {}, minor: {}, pa: {}",
                format(beam.component(BeamComponent::Major)),
                format(beam.component(BeamComponent::Minor)),
                format(&beam.wrapped_position_angle().map_err(raised)?)
            ))
        };
        Ok([
            format!(
                "Original {name} {kind}restoring beam: {}",
                describe(&self.original)?
            ),
            format!(
                "New {name} {kind}restoring beam: {}",
                describe(&self.rotated)?
            ),
        ])
    }
}

fn multiple_beam_messages(name: &str, min: &BeamChange, max: &BeamChange) -> PyResult<Vec<String>> {
    let mut messages = vec![
        "This image has multiple beams. Reporting the rotation of the minimum and maximum area beams:"
            .into(),
    ];
    messages.extend(min.messages(name, "minimum area ")?);
    messages.extend(max.messages(name, "maximum area ")?);
    Ok(messages)
}

fn rotate_beam_record(
    beams: &ImageBeamSet,
    info: Option<&RecordValue>,
    updated: &mut RecordValue,
    angle: &Quantity,
    name: &str,
) -> PyResult<Vec<String>> {
    let change = |channel, stokes| {
        let beam = stored_beam_record(info, beams, channel, stokes);
        BeamChange::new(
            StoredGaussianBeam::from_record(&beam).map_err(raised)?,
            angle,
        )
    };
    if beams.is_single() {
        let single = change(0, 0)?;
        updated.upsert("restoringbeam", Value::Record(single.rotated.to_record()));
        return single.messages(name, "").map(Vec::from);
    }
    let mut grid = info
        .and_then(|info| subrecord(info, "perplanebeams"))
        .cloned()
        .unwrap_or_else(|| beams.to_record());
    let mut minimum: Option<(f64, BeamChange)> = None;
    let mut maximum: Option<(f64, BeamChange)> = None;
    for stokes in 0..beams.n_stokes() {
        for channel in 0..beams.n_channels() {
            let plane = change(channel, stokes)?;
            let index = stokes * beams.n_channels() + channel;
            grid.upsert(
                format!("*{index}"),
                Value::Record(plane.rotated.to_record()),
            );
            let area = beams.beam(channel, stokes).area();
            if minimum.as_ref().is_none_or(|(min, _)| area < *min) {
                minimum = Some((area, plane.clone()));
            }
            if maximum.as_ref().is_none_or(|(max, _)| area > *max) {
                maximum = Some((area, plane));
            }
        }
    }
    updated.upsert("perplanebeams", Value::Record(grid));
    multiple_beam_messages(
        name,
        &minimum.expect("nonempty beam set").1,
        &maximum.expect("nonempty beam set").1,
    )
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (angle=Variant::Empty), text_signature = "($self, angle=[])")]
    fn rotatebeam(&self, py: Python<'_>, angle: Variant) -> PyResult<bool> {
        let result = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(None);
            };
            let mut image = attachment.lock();
            image.check_single_precision("rotatebeam")?;
            let rotation = quantity(&angle)?;
            let degrees = Unit::new("deg").expect("built-in angular unit");
            if !rotation.unit().conformant(&degrees) {
                return Err(raised("Exception: Quantity is not an angle."));
            }
            attachment.require_lock(&mut image, LockType::Write)?;
            let info = image_info(&image).map_err(raised)?;
            let name = image
                .path()
                .map_or_else(|| "Temporary_Image".into(), |path| path.display().to_string());
            let messages = if info.beam_set.is_empty() {
                let zero = Quantity::new(0.0, "arcsec").expect("built-in angular unit");
                let previous = Quantity::with_unit(attachment.empty_beam_angle(), degrees);
                let beam = StoredGaussianBeam::new(zero.clone(), zero, previous).map_err(raised)?;
                let change = BeamChange::new(beam, &rotation)?;
                let messages = multiple_beam_messages(&name, &change, &change)?;
                attachment.set_empty_beam_angle(
                    change.rotated.component(BeamComponent::PositionAngle).value(),
                );
                messages
            } else {
                let record = image_info_record(&image);
                let mut updated = record.cloned().unwrap_or_else(|| info.to_record());
                let messages = rotate_beam_record(&info.beam_set, record, &mut updated, &rotation, &name)?;
                with_pixels!(&mut *image, storage => with_storage!(storage, target => target.set_image_info_record(updated)))
                    .map_err(raised)?;
                messages
            };
            let mut history = Vec::with_capacity(messages.len() + 2);
            if state.history {
                history.push("Ran ia.rotatebeam".into());
                history.push(format!("ia.rotatebeam(angle={})", input_history(&angle)));
            }
            history.extend(messages.iter().cloned());
            image.append_history_at("::BeamManipulator::rotate", &history)?;
            image.save().map_err(raised)?;
            Ok::<_, PyErr>(Some(messages))
        })?;
        let Some(messages) = result else {
            return Ok(false);
        };
        let logs = messages.into_iter().map(LogLine::info).collect::<Vec<_>>();
        emit(py, "casars.tools.image", &logs)?;
        Ok(true)
    }
}
