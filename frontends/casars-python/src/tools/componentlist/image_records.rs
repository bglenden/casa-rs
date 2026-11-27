// SPDX-License-Identifier: LGPL-3.0-or-later
//! Component-list records used by the public image tool.

use std::collections::BTreeMap;

use casa_components::{ComponentList, ShapeType};
use casa_core::RecordValue;
use casa_core::quanta::Quantity;

use crate::convert::value::CasaValue;
use crate::convert::{ConvertError, ConvertResult};
use crate::tools::LogLine;

use super::{ops::State, records};

pub(in crate::tools) struct ImageComponentRecord {
    pub(in crate::tools) shape: ShapeType,
    pub(in crate::tools) gaussian_axes: Option<[Quantity; 3]>,
    pub(in crate::tools) value: CasaValue,
}

/// Read a CASA list for the image tool, retaining its record units and offsets.
pub(in crate::tools) fn image_component_records<'a>(
    record: &'a RecordValue,
    logs: &mut Vec<LogLine>,
) -> ConvertResult<impl ExactSizeIterator<Item = ConvertResult<ImageComponentRecord>> + use<'a>> {
    let components = State::image_components(record, logs, |detail| {
        ConvertError::runtime(format!(
            "Exception: Input dictionary is not a valid component list: {detail}."
        ))
    })?;
    Ok((0..components.length() as usize).map(move |index| {
        let shape = components.checked(index as i64)?.shape().shape_type();
        let gaussian_axes = if shape == ShapeType::Gaussian {
            let Some(casa_core::Value::Record(raw_shape)) =
                records::item(record, index)?.get("shape")
            else {
                unreachable!("validated Gaussian record");
            };
            let [major, minor, pa] = ["majoraxis", "minoraxis", "positionangle"]
                .map(|key| records::angular(raw_shape, key).map_err(ConvertError::runtime));
            Some([major?, minor?, pa?])
        } else {
            None
        };
        Ok(ImageComponentRecord {
            shape,
            gaussian_axes,
            value: components.getcomponent(index as i64)?,
        })
    }))
}

/// Decode an image component model with its direction and frequency offsets.
pub(in crate::tools) fn image_component_model(
    record: &RecordValue,
    logs: &mut Vec<LogLine>,
) -> ConvertResult<ComponentList> {
    State::image_components(record, logs, |_| {
        ConvertError::runtime("Exception: model is an invalid componentlist record.")
    })?
    .into_image_model()
}

pub(in crate::tools) fn component_image_creation_model(
    record: &RecordValue,
    logs: &mut Vec<LogLine>,
    persistent: bool,
) -> ConvertResult<(ComponentList, BTreeMap<usize, RecordValue>)> {
    let components = State::image_components(record, logs, ConvertError::runtime)?;
    let spectral_records = if persistent {
        components
            .spectral_records()
            .map_err(ConvertError::runtime)?
    } else {
        BTreeMap::new()
    };
    Ok((components.into_image_model()?, spectral_records))
}
