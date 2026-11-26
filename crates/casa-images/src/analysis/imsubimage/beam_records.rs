// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::HashMap;

use casa_coordinates::CoordinateSystem;
use casa_core::{RecordField, RecordValue, ScalarValue, Value};

use crate::{ImageBeamSet, ImageError, ImageInfo};

use super::coords::beam_section_indices_for_shape;
use super::region::SubimageSelection;

pub(super) fn select_image_info(
    coordinates: &CoordinateSystem,
    selection: &SubimageSelection,
    output_info: &ImageInfo,
    source: &RecordValue,
) -> Result<Option<RecordValue>, ImageError> {
    if output_info.beam_set.is_empty()
        || output_info
            .beam_set
            .single_beam()
            .is_some_and(|beam| beam.is_null())
    {
        return Ok(None);
    }
    let metadata = || {
        ImageInfo {
            beam_set: ImageBeamSet::default(),
            image_type: output_info.image_type,
            object_name: output_info.object_name.clone(),
        }
        .to_record()
    };
    if let Some(Value::Record(grid)) = source.get("perplanebeams") {
        let counts = ImageBeamSet::record_shape(grid)?;
        let [channels, stokes] = beam_section_indices_for_shape(
            counts,
            coordinates,
            selection.start.len(),
            &selection.start,
            &selection.packed_shape(),
            selection.stride.as_deref(),
        );
        let mut indexed = HashMap::with_capacity(grid.len());
        for field in grid.fields() {
            indexed.entry(field.name.as_str()).or_insert(&field.value);
        }
        let beam = |channel, polarization| {
            let index = polarization * counts[0] + channel;
            let lookup = |name: &str| match indexed.get(name) {
                Some(Value::Record(value)) => Some(value),
                _ => None,
            };
            lookup(&format!("*{index}"))
                .or_else(|| lookup(&format!("*{channel}_{polarization}")))
                .cloned()
                .map(Value::Record)
                .ok_or_else(|| ImageError::InvalidMetadata(format!("missing beam *{index}")))
        };
        let mut output = metadata();
        if output_info.beam_set.is_multi() {
            let mut selected = RecordValue::new(vec![
                RecordField::new(
                    "nChannels",
                    Value::Scalar(ScalarValue::Int32(channels.len() as i32)),
                ),
                RecordField::new(
                    "nStokes",
                    Value::Scalar(ScalarValue::Int32(stokes.len() as i32)),
                ),
            ]);
            for (plane, &polarization) in stokes.iter().enumerate() {
                for (slot, &channel) in channels.iter().enumerate() {
                    selected.push(RecordField::new(
                        format!("*{}", plane * channels.len() + slot),
                        beam(channel, polarization)?,
                    ));
                }
            }
            output.upsert("perplanebeams", Value::Record(selected));
        } else {
            output.upsert("restoringbeam", beam(channels[0], stokes[0])?);
        }
        Ok(Some(output))
    } else if let Some(beam) = source.get("restoringbeam") {
        let mut output = metadata();
        output.upsert("restoringbeam", beam.clone());
        Ok(Some(output))
    } else {
        Ok(None)
    }
}
