// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::quanta::{Quantity, Unit, casacore};
use casa_core::{RecordValue, Value};
use casa_images::{BeamComponent, ImageBeamSet, StoredGaussianBeam, wrap_position_angle};
use casa_tables::LockType;
use pyo3::prelude::*;

use crate::convert::{Variant, quantity};
use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::beam_edit::input_history;
use super::beam_view::BeamView;
use super::beams::{image_info, image_info_record, stored_beam_record, subrecord};
use super::handle::{ImageHandle, Storage, raised, with_pixels};

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
    view: &BeamView,
    updated: &mut RecordValue,
    angle: &Quantity,
    name: &str,
) -> PyResult<Vec<String>> {
    let change = |channel, stokes| {
        let beam = stored_beam_record(Some(view.record()), beams, channel, stokes);
        BeamChange::new(
            StoredGaussianBeam::from_record(&beam).map_err(raised)?,
            angle,
        )
    };
    if beams.is_single() {
        let single = change(0, 0)?;
        updated.remove("perplanebeams");
        updated.upsert("restoringbeam", Value::Record(single.rotated.to_record()));
        return single.messages(name, "").map(Vec::from);
    }
    let mut grid = subrecord(view.record(), "perplanebeams")
        .cloned()
        .unwrap_or_else(|| beams.to_record());
    let [minimum, maximum] = view.extrema();
    let indices = |index| (index % beams.n_channels(), index / beams.n_channels());
    for stokes in 0..beams.n_stokes() {
        for channel in 0..beams.n_channels() {
            let plane = change(channel, stokes)?;
            let index = stokes * beams.n_channels() + channel;
            grid.upsert(
                format!("*{index}"),
                Value::Record(plane.rotated.to_record()),
            );
        }
    }
    updated.remove("restoringbeam");
    updated.upsert("perplanebeams", Value::Record(grid));
    let (min_chan, min_stokes) = indices(minimum);
    let (max_chan, max_stokes) = indices(maximum);
    multiple_beam_messages(
        name,
        &change(min_chan, min_stokes)?,
        &change(max_chan, max_stokes)?,
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
            let mut view = attachment.beam_view();
            let beams = view.beam_set().map_err(raised)?;
            let name = image
                .path()
                .map_or_else(|| "Temporary_Image".into(), |path| path.display().to_string());
            let (messages, updated_info, empty_angle) = if beams.is_empty() {
                let zero = Quantity::new(0.0, "arcsec").expect("built-in angular unit");
                let previous = Quantity::with_unit(view.empty_angle_degrees, degrees);
                let beam = StoredGaussianBeam::new(zero.clone(), zero, previous).map_err(raised)?;
                let change = BeamChange::new(beam, &rotation)?;
                let messages = multiple_beam_messages(&name, &change, &change)?;
                let angle = change.rotated.component(BeamComponent::PositionAngle).value();
                (messages, None, angle)
            } else {
                let mut updated = match image_info_record(&image) {
                    Some(info) => info.clone(),
                    None => image_info(&image).map_err(raised)?.to_record(),
                };
                let messages = rotate_beam_record(&beams, &view, &mut updated, &rotation, &name)?;
                casa_images::ImageInfo::from_record(&updated).map_err(raised)?;
                (messages, Some(updated), 0.0)
            };
            let mut history = Vec::with_capacity(messages.len() + 2);
            if state.history {
                history.push("Ran ia.rotatebeam".into());
                history.push(format!("ia.rotatebeam(angle={})", input_history(&angle)));
            }
            history.extend(messages.iter().cloned());
            let location = "::BeamManipulator::rotate";
            with_pixels!(&mut *image, storage => match storage {
                Storage::Paged(image) => image.save_image_info_with_history(updated_info.as_ref(), location, &history),
                Storage::Temp { image, .. } => {
                    if let Some(updated) = &updated_info {
                        image.set_image_info_record(updated.clone()).map_err(raised)?;
                    }
                    image.append_history_at(location, &history)
                },
            }).map_err(raised)?;
            if let Some(updated) = updated_info {
                view.capture(&updated);
            } else {
                view.empty_angle_degrees = empty_angle;
            }
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
