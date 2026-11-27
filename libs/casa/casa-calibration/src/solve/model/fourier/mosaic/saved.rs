// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the completed convolution buffers retained in HetArrayConvFunc's record.

use std::borrow::Cow;

use casa_core::{ArrayValue, Complex32, RecordValue, ScalarValue, Value};
use casa_imaging_operator::{MosaicRules, MosaicWindow, SavedMosaicPlanes};

use super::super::{GainSolveError, model_error, record_text};

pub(super) fn rules(machine: &RecordValue, beam: &RecordValue) -> MosaicRules {
    // The older HetArrayConvFunc keys its cache by MS and SPW. The newer
    // Fourier machines include the selected native channel count as well.
    // Some tclean records carry the older machine name as an alias.
    if record_text(machine, "name").is_ok_and(|name| name == "MosaicFT")
        && record_text(beam, "key0").map_or_else(
            |_| machine.get("usepointingtable").is_none(),
            |key| key.split('_').count() == 2,
        )
    {
        MosaicRules::Simulator
    } else {
        MosaicRules::Tclean
    }
}

pub(super) fn planes<'a>(
    machine: &RecordValue,
    beam: &'a RecordValue,
    window: &MosaicWindow,
) -> Result<Option<SavedMosaicPlanes<'a>>, GainSolveError> {
    let key = format!(
        "0_{}_{}",
        window.spectral_window,
        window.selected_frequencies_hz.len()
    );
    let count = integer(beam, "ndefined")?;
    if count < 0 || count as usize > beam.fields().len() {
        return Err(model_error("invalid virtual mosaic cache count"));
    }
    for entry in 0..count {
        if record_text(beam, &format!("key{entry}"))? == key {
            let index =
                usize::try_from(integer(beam, &format!("val{entry}"))?).map_err(model_error)?;
            return cached_planes(machine, beam, index);
        }
    }
    Ok(None)
}

fn cached_planes<'a>(
    machine: &RecordValue,
    beam: &'a RecordValue,
    index: usize,
) -> Result<Option<SavedMosaicPlanes<'a>>, GainSolveError> {
    let invalid = || model_error("invalid saved virtual mosaic convolution");
    let Some(Value::Array(ArrayValue::Bool(done))) = beam.get("donemainconv") else {
        return Err(invalid());
    };
    if !done
        .get(ndarray::IxDyn(&[index]))
        .copied()
        .ok_or_else(invalid)?
    {
        return Ok(None);
    }
    let (shape, strides, values) = match beam.get(&format!("convfunctions{index}")) {
        Some(Value::Array(ArrayValue::Complex32(grid))) => (
            grid.shape(),
            grid.strides(),
            Cow::Borrowed(grid.as_slice_memory_order().ok_or_else(invalid)?),
        ),
        Some(Value::Array(ArrayValue::Complex64(grid))) => (
            grid.shape(),
            grid.strides(),
            Cow::Owned(
                grid.as_slice_memory_order()
                    .ok_or_else(invalid)?
                    .iter()
                    .map(|value| Complex32::new(value.re as f32, value.im as f32))
                    .collect(),
            ),
        ),
        _ => return Err(invalid()),
    };
    let &[side, height, 1, cells, pairs] = shape else {
        return Err(invalid());
    };
    let dishes = (1..=256)
        .find(|dishes| dishes * (dishes + 1) / 2 == pairs)
        .ok_or_else(invalid)?;
    let support = match beam.get(&format!("convsupportblock{index}")) {
        Some(Value::Array(ArrayValue::Int32(values))) => {
            values.iter().copied().max().map(i64::from)
        }
        Some(Value::Array(ArrayValue::Int64(values))) => values.iter().copied().max(),
        _ => None,
    };
    if side != height || strides[0] != 1 {
        return Err(invalid());
    }
    let half_support = support
        .and_then(|max| u16::try_from(max).ok())
        .ok_or_else(invalid)?;
    let sampling = u16::try_from(integer(machine, "convsampling")?).map_err(model_error)?;
    Ok(Some(SavedMosaicPlanes {
        values,
        side,
        cells,
        dishes,
        half_support,
        sampling,
    }))
}

pub(super) fn integer(record: &RecordValue, field: &str) -> Result<i32, GainSolveError> {
    match record.get(field) {
        Some(Value::Scalar(ScalarValue::Int32(value))) => Ok(*value),
        Some(Value::Scalar(ScalarValue::Int64(value))) => {
            i32::try_from(*value).map_err(model_error)
        }
        _ => Err(model_error(format!(
            "virtual mosaic has no {field} integer"
        ))),
    }
}
