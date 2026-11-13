// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve stored DD/POL mappings, admitting their buffers before decoding.
use std::sync::Arc;

use casa_imaging_runtime::{Demand, Reservation, admit};
use casa_ms::{MeasurementSet, SubtableId};
use casa_tables::{RequiredScalarColumnDestination, RequiredScalarColumnValuesMut};
use casa_types::ArrayValue;

use super::super::{SessionResources, spectral::DataDescription};
use super::{SimulatorError, SimulatorResult, index};

pub(super) struct Mappings {
    pub(super) polarizations: Arc<Vec<Vec<i32>>>,
    pub(super) descriptions: Arc<Vec<DataDescription>>,
    pub(super) memory: Arc<Reservation>,
}

fn error(value: impl std::fmt::Display) -> SimulatorError {
    SimulatorError::raised(format!("openfromms data descriptions: {value}"))
}

pub(super) fn read(ms: &MeasurementSet, resources: &SessionResources) -> SimulatorResult<Mappings> {
    let dd = ms
        .subtable(SubtableId::DataDescription)
        .ok_or_else(|| error("missing DATA_DESCRIPTION"))?;
    let pol = ms
        .subtable(SubtableId::Polarization)
        .ok_or_else(|| error("missing POLARIZATION"))?;
    let windows = ms.spectral_window().map_err(error)?.row_count();
    let codes = pol.column_accessor("CORR_TYPE").map_err(error)?;
    // One cell header at a time: the pass holds no row-count-sized scratch.
    let mut elements = 0usize;
    for row in 0..pol.row_count() {
        let shapes = codes.array_shapes_uncached(&[row]).map_err(error)?;
        let shape = shapes
            .first()
            .and_then(Option::as_ref)
            .ok_or_else(|| error("undefined CORR_TYPE"))?;
        if shape.len() != 1 {
            return Err(error("CORR_TYPE is not a vector"));
        }
        elements = elements
            .checked_add(shape[0])
            .ok_or_else(|| error("correlation count overflows usize"))?;
    }
    let bytes = dd
        .row_count()
        .checked_mul(size_of::<DataDescription>())
        .and_then(|v| v.checked_add(pol.row_count().checked_mul(size_of::<Vec<i32>>())?))
        .and_then(|v| v.checked_add(elements.checked_mul(8)?))
        .and_then(|v| v.checked_add(128 << 10))
        .ok_or_else(|| error("mapping memory overflows usize"))?;
    let mut reservation = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "reading stored simulation data descriptions",
            memory: bytes as u64,
        },
    )
    .map_err(SimulatorError::admission)?;
    let mut polarizations = Vec::with_capacity(pol.row_count());
    for row in 0..pol.row_count() {
        let mut cells = codes.array_cells_owned_uncached(&[row]).map_err(error)?;
        let Some(ArrayValue::Int32(values)) = cells.pop().flatten() else {
            return Err(error("CORR_TYPE is not Int32"));
        };
        polarizations.push(values.into_raw_vec_and_offset().0);
    }
    let mut descriptions = Vec::with_capacity(dd.row_count());
    {
        let mut ids = Vec::with_capacity(1024);
        let mut window_ids = Vec::with_capacity(1024);
        let mut polarization_ids = Vec::with_capacity(1024);
        for first in (0..dd.row_count()).step_by(1024) {
            ids.clear();
            ids.extend(first..dd.row_count().min(first + 1024));
            window_ids.clear();
            polarization_ids.clear();
            dd.required_scalar_columns_for_rows_into(
                &ids,
                &mut [
                    RequiredScalarColumnDestination::new(
                        "SPECTRAL_WINDOW_ID",
                        RequiredScalarColumnValuesMut::Int32(&mut window_ids),
                    ),
                    RequiredScalarColumnDestination::new(
                        "POLARIZATION_ID",
                        RequiredScalarColumnValuesMut::Int32(&mut polarization_ids),
                    ),
                ],
            )
            .map_err(error)?;
            for (offset, (&window, &polarization)) in
                window_ids.iter().zip(&polarization_ids).enumerate()
            {
                descriptions.push(DataDescription {
                    window: index(window, windows, first + offset, "SPECTRAL_WINDOW_ID")?,
                    polarization: index(
                        polarization,
                        polarizations.len(),
                        first + offset,
                        "POLARIZATION_ID",
                    )?,
                });
            }
        }
    }
    let retained = descriptions.capacity() * size_of::<DataDescription>()
        + polarizations.capacity() * size_of::<Vec<i32>>()
        + polarizations
            .iter()
            .map(|v| v.capacity() * size_of::<i32>())
            .sum::<usize>()
        + 128;
    reservation.retain(retained as u64);
    Ok(Mappings {
        polarizations: Arc::new(polarizations),
        descriptions: Arc::new(descriptions),
        memory: Arc::new(reservation),
    })
}
