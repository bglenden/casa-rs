// SPDX-License-Identifier: LGPL-3.0-or-later
//! Admit explicit channel axes before decoding their uncached numerical cells.
use casa_imaging_runtime::{Demand, Reservation, admit};
use casa_tables::Table;
use casa_types::ArrayValue;

use super::super::SessionResources;
use super::{SimulatorError, SimulatorResult};

fn error(value: impl std::fmt::Display) -> SimulatorError {
    SimulatorError::raised(format!("openfromms channel axis: {value}"))
}

pub(super) fn read(
    table: &Table,
    row: usize,
    resources: &SessionResources,
) -> SimulatorResult<(Vec<f64>, Vec<f64>, Reservation)> {
    let mut elements = 0usize;
    for name in ["CHAN_FREQ", "CHAN_WIDTH"] {
        let shapes = table
            .column_accessor(name)
            .map_err(error)?
            .array_shapes_uncached(&[row])
            .map_err(error)?;
        let shape = shapes
            .first()
            .and_then(Option::as_ref)
            .ok_or_else(|| error(format!("undefined {name}")))?;
        if shape.len() != 1 {
            return Err(error(format!("{name} is not a vector")));
        }
        elements = elements
            .checked_add(shape[0])
            .ok_or_else(|| error("channel count overflows usize"))?;
    }
    let bytes = elements
        .checked_mul(32)
        .and_then(|v| v.checked_add(256))
        .ok_or_else(|| error("channel axis memory overflows usize"))?;
    let reservation = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "reading stored simulation channel frequencies",
            memory: bytes as u64,
        },
    )
    .map_err(SimulatorError::admission)?;
    let read = |name: &str| -> SimulatorResult<Vec<f64>> {
        let mut cells = table
            .column_accessor(name)
            .map_err(error)?
            .array_cells_owned_uncached(&[row])
            .map_err(error)?;
        let Some(ArrayValue::Float64(values)) = cells.pop().flatten() else {
            return Err(error(format!("{name} is not a Float64 array")));
        };
        Ok(values.into_raw_vec_and_offset().0)
    };
    let frequencies = read("CHAN_FREQ")?;
    let widths = read("CHAN_WIDTH")?;
    if frequencies.is_empty()
        || frequencies.len() != widths.len()
        || frequencies.iter().any(|v| !v.is_finite() || *v <= 0.)
        || widths.iter().any(|v| !v.is_finite())
    {
        return Err(error(
            "channel frequencies/widths are empty, inconsistent or invalid",
        ));
    }
    Ok((frequencies, widths, reservation))
}
