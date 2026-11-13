// SPDX-License-Identifier: LGPL-3.0-or-later
//! FIELD polynomial payload admission and MSIter's chunk-epoch phase centre.
use casa_imaging_runtime::{Demand, Reservation, admit};
use casa_ms::{MeasurementSet, SubtableId};
use casa_types::{ArrayValue, measures::direction::MDirection};

use super::super::SessionResources;
use super::super::observe::{Scan, ScanRows};
use super::{Simulator, SimulatorError, SimulatorResult, raw_field_phase_direction, read_error};
use std::{collections::HashMap, sync::Arc};

pub(in crate::session) type PhaseCentres =
    Arc<casa_imaging_runtime::Admitted<Vec<((i32, usize, usize), [f64; 2])>>>;

pub(in crate::session) fn centre(centres: Option<&PhaseCentres>, scan: &Scan) -> [f64; 2] {
    centres
        .and_then(|p| {
            p.value
                .binary_search_by_key(&key(scan), |v| v.0)
                .ok()
                .map(|i| p.value[i].1)
        })
        .unwrap_or(scan.phase_center_rad)
}

fn key(scan: &Scan) -> (i32, usize, usize) {
    (scan.array_id, scan.field_id, scan.data_description_id)
}

pub(super) fn set_scan_centres(
    session: &Simulator,
    ms: &MeasurementSet,
    scans: &mut [Scan],
    times: &[[f64; 2]],
) -> SimulatorResult<()> {
    if !session.fields.iter().any(|field| field.polynomial) {
        return Ok(());
    }
    let resources = &session.options.resources;
    let _memory = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "grouping stored simulation FIELD epochs",
            memory: (scans.len() as u64).saturating_mul(128).saturating_add(256),
        },
    )
    .map_err(SimulatorError::admission)?;
    let mut first = HashMap::<_, f64>::new();
    for (scan, time) in scans.iter().zip(times) {
        if session.fields[scan.field_id].polynomial {
            first
                .entry(key(scan))
                .and_modify(|v| *v = v.min(time[0]))
                .or_insert(time[0]);
        }
    }
    for scan in scans {
        if let Some(&time) = first.get(&key(scan)) {
            scan.phase_center_rad = at(session, ms, scan.field_id, time)?;
        }
    }
    Ok(())
}

/// Freeze this selection's first timestamp per CASA ARRAY/FIELD/DD chunk.
pub(in crate::session) fn selected_centres(
    session: &Simulator,
    image_frame: bool,
) -> SimulatorResult<Option<PhaseCentres>> {
    let selection = session.data_plan.selection();
    if session.opened.is_none()
        || (!selection.has_query() && !image_frame)
        || !session.fields.iter().any(|f| f.polynomial)
    {
        return Ok(None);
    }
    let resources = &session.options.resources;
    let mut reservation = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "the simulator's selected polynomial FIELD epochs",
            memory: (session.scans.len() as u64)
                .saturating_mul(160)
                .saturating_add(256 << 10)
                .saturating_add(selection.evaluation_bytes()),
        },
    )
    .map_err(SimulatorError::admission)?;
    let phase_memory;
    let ms = MeasurementSet::open(session.path()).map_err(error)?;
    phase_memory = admit_fields(&ms, resources)?;
    let value = {
        let mut first = HashMap::<_, f64>::new();
        if selection.has_query() {
            session.visit_selected_headers(selection, |scan, header| {
                first
                    .entry(key(scan))
                    .and_modify(|v| *v = v.min(header.time_s))
                    .or_insert(header.time_s);
                Ok(())
            })?;
        } else {
            let opened = session.opened.as_ref().expect("opened");
            for (index, scan) in session.scans.iter().enumerate() {
                if !selection.contains_scan(scan) || scan.row_count() == 0 {
                    continue;
                }
                let time = match scan.rows {
                    ScanRows::OnDisk(_) => opened.scan_times_s[index][0],
                    ScanRows::Planned(ref rows) => rows.integration_times_s().next().unwrap_or(0.),
                };
                first
                    .entry(key(scan))
                    .and_modify(|v| *v = v.min(time))
                    .or_insert(time);
            }
        }
        let mut chunks: Vec<_> = first.into_iter().collect();
        chunks.sort_unstable_by_key(|v| v.0);
        let mut previous_field = None;
        let mut frame_direction = [0., 0.];
        let mut centres = Vec::with_capacity(chunks.len());
        for (chunk, time) in chunks {
            // ComponentFT uses MSIter's current chunk. FTMachine::rotateUVW
            // retains its frame while FIELD_ID/MS_ID stay unchanged, across
            // ARRAY_ID and DD boundaries, in iterator chunk order.
            if !image_frame || previous_field != Some(chunk.1) {
                frame_direction = at(session, &ms, chunk.1, time)?;
                previous_field = Some(chunk.1);
            }
            centres.push((chunk, frame_direction));
        }
        centres.sort_unstable_by_key(|v| v.0);
        centres
    };
    drop(ms);
    drop(phase_memory);
    reservation
        .retain((value.capacity() * size_of::<((i32, usize, usize), [f64; 2])>() + 64) as u64);
    Ok(Some(Arc::new(casa_imaging_runtime::Admitted {
        value,
        reservation,
    })))
}

fn error(value: impl std::fmt::Display) -> SimulatorError {
    SimulatorError::raised(format!("openfromms polynomial FIELD: {value}"))
}

pub(super) fn admit_fields(
    ms: &MeasurementSet,
    resources: &SessionResources,
) -> SimulatorResult<Option<Reservation>> {
    let fields = ms.field().map_err(read_error("FIELD"))?;
    let mut polynomial = false;
    for row in 0..fields.row_count() {
        let degree = fields.num_poly(row).map_err(read_error("FIELD"))?;
        if degree < 0 {
            return Err(error("negative NUM_POLY"));
        }
        polynomial |= degree > 0;
    }
    if !polynomial {
        return Ok(None);
    }
    let table = ms.subtable(SubtableId::Field).expect("FIELD");
    let mut elements = 0usize;
    // Typed FIELD access may cache other direction cells of the row. Admit
    // all three columns, including a transient decoded copy, before access.
    for row in 0..fields.row_count() {
        let terms = fields.num_poly(row).map_err(read_error("FIELD"))? as usize + 1;
        for name in ["PHASE_DIR", "DELAY_DIR", "REFERENCE_DIR"] {
            let shapes = table
                .column_accessor(name)
                .map_err(error)?
                .array_shapes_uncached(&[row])
                .map_err(error)?;
            let shape = shapes
                .first()
                .and_then(Option::as_ref)
                .ok_or_else(|| error(format!("undefined {name}")))?;
            if shape.as_slice() != [2, terms] {
                return Err(error(format!("{name} shape does not match NUM_POLY")));
            }
            elements = elements
                .checked_add(
                    terms
                        .checked_mul(2)
                        .ok_or_else(|| error("polynomial size overflow"))?,
                )
                .ok_or_else(|| error("polynomial size overflow"))?;
        }
    }
    let bytes = elements
        .checked_mul(16)
        .and_then(|n| n.checked_add(fields.row_count().checked_mul(1024)?))
        .and_then(|n| n.checked_add(256 << 10))
        .ok_or_else(|| error("polynomial memory overflow"))?;
    admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "reading stored simulation FIELD polynomials",
            memory: bytes as u64,
        },
    )
    .map(Some)
    .map_err(SimulatorError::admission)
}

pub(super) fn at(
    session: &Simulator,
    ms: &MeasurementSet,
    field_id: usize,
    time_s: f64,
) -> SimulatorResult<[f64; 2]> {
    let field = &session.fields[field_id];
    if !field.polynomial {
        return Ok(field.direction_rad);
    }
    let raw = raw_field_phase_direction(ms, field_id).map_err(read_error("FIELD"))?;
    let fields = ms.field().map_err(read_error("FIELD"))?;
    let ArrayValue::Float64(coefficients) =
        fields.phase_dir(field_id).map_err(read_error("FIELD"))?
    else {
        return Err(error("PHASE_DIR is not Float64"));
    };
    let (longitude, latitude) = raw.as_angles();
    let mut value = [longitude, latitude];
    // MSFieldColumns::interpolateDirMeas returns coefficient zero when no
    // positive interpolation time was requested.
    if time_s >= 1. {
        let dt = time_s - field.source_time_s;
        let mut power = 1.;
        for term in 1..coefficients.shape()[1] {
            power *= dt;
            let coefficient = MDirection::from_angles(
                coefficients[[0, term]],
                coefficients[[1, term]],
                raw.refer(),
            );
            let (lon, lat) = coefficient.as_angles();
            value[0] += power * lon;
            value[1] += power * lat;
        }
    }
    if value.iter().any(|v| !v.is_finite()) {
        return Err(error("PHASE_DIR evaluates to a non-finite direction"));
    }
    session.j2000_rad(
        &MDirection::from_angles(value[0], value[1], raw.refer()),
        "the polynomial FIELD phase direction",
    )
}
