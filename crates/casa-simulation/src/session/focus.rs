// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selected-buffer antenna positions for FTMachine's finite-distance focus.
use super::{
    Simulator,
    error::{SimulatorError, SimulatorResult},
    observe::Scan,
};
use crate::write::geometry::{ArrayGeometry, RowGeometry};
use casa_imaging_model::GridFtProjection;
use casa_imaging_runtime::{Admitted, Demand, admit};
use casa_ms::MeasurementSet;
use casa_types::ArrayValue;
use std::{collections::HashMap, sync::Arc};

type Chunk = (i32, usize, usize);
type Antenna = (usize, usize);
pub(crate) type Positions = Arc<Admitted<Geometry>>;

pub(crate) struct Geometry {
    antennas: Vec<(Antenna, [f64; 3])>,
    rows: Vec<(u64, usize)>,
}

struct SelectedRow {
    chunk: Chunk,
    row: u64,
    time_s: f64,
    antennas: [usize; 2],
    uvw: [f64; 3],
}

fn buffer(scan: &Scan, time_s: f64) -> (Chunk, u64) {
    (
        (scan.array_id, scan.field_id, scan.data_description_id),
        time_s.to_bits(),
    )
}

/// Freeze the same selected rows that CASA's VisBuffer uses, including
/// flagged rows and autocorrelations. The buffer normalizes sums by the
/// number of appearances of each antenna, then subtracts min(ANTENNA1)'s
/// location; replacing these with physical ANTENNA positions changes CASA's
/// result for incomplete baselines and for buffers containing autocorrelations.
pub(super) fn capture(session: &Simulator) -> SimulatorResult<Positions> {
    let count = session.scans.iter().map(Scan::row_count).sum::<usize>();
    let largest = session
        .scans
        .iter()
        .filter_map(Scan::planned)
        .map(|rows| rows.baseline_count())
        .max()
        .unwrap_or(0);
    let resources = &session.options.resources;
    let mut reservation = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "the simulator's finite-distance antenna positions",
            // Two hash entries per row, reference keys, sorted output, and one
            // planned integration's reusable geometry during the capture.
            memory: (count as u64)
                .saturating_mul(512)
                .saturating_add((largest * size_of::<RowGeometry>()) as u64)
                .saturating_add((session.antennas.len() * 128) as u64)
                .saturating_add(256 << 10),
        },
    )
    .map_err(SimulatorError::admission)?;
    let ms = session
        .opened
        .as_ref()
        .map(|_| MeasurementSet::open(session.path()))
        .transpose()
        .map_err(error)?;
    let array = if largest > 0 {
        Some(ArrayGeometry::new(
            &session.telescope,
            &session.antennas,
            session.options.measures.clone(),
        )?)
    } else {
        None
    };
    let mut geometry = Vec::with_capacity(largest);
    let mut cached = None;
    let mut selected = Vec::new();
    session.visit_selected_headers(session.data_plan.selection(), |scan, row| {
        let key = buffer(scan, row.time_s);
        let uvw = if let Some(planned) = scan.planned() {
            if cached != Some(key) {
                geometry.resize(planned.baseline_count(), RowGeometry::default());
                array.as_ref().expect("planned geometry").integration_rows(
                    super::observe::PlannedScan {
                        scan,
                        rows: planned,
                    },
                    row.time_s,
                    &mut geometry,
                )?;
                cached = Some(key);
            }
            let [a, b] = row.antennas;
            let skip_auto = usize::from(planned.autocorrelation_weight <= 0.);
            let index = a * (planned.antenna_count - skip_auto) - a * a.saturating_sub(1) / 2 + b
                - a
                - skip_auto;
            geometry[index].uvw_m
        } else {
            let value = ms
                .as_ref()
                .expect("stored geometry")
                .main_table()
                .cell_accessor(row.row as usize, "UVW")
                .and_then(|cell| cell.array())
                .map_err(error)?;
            let ArrayValue::Float64(uvw) = value else {
                return Err(error("UVW must be double"));
            };
            if uvw.len() != 3 {
                return Err(error("UVW must have three elements"));
            }
            let mut values = uvw.iter().copied();
            [
                values.next().unwrap(),
                values.next().unwrap(),
                values.next().unwrap(),
            ]
        };
        selected.push(SelectedRow {
            chunk: key.0,
            row: row.row,
            time_s: row.time_s,
            antennas: row.antennas,
            uvw,
        });
        Ok(())
    })?;
    drop(ms);
    drop(array);
    drop(geometry);
    selected.sort_unstable_by(|a, b| {
        a.chunk
            .cmp(&b.chunk)
            .then(a.time_s.total_cmp(&b.time_s))
            .then(a.row.cmp(&b.row))
    });
    // Simulator::createSkyEquation requests rowBlocking=100 for mosaic;
    // the iterator's inclusive endpoint makes 101 rows. SkyEquation enlarges
    // a smaller first buffer to nAnt*(nAnt-1)/2+nAnt, again inclusive.
    let n = session.antennas.len();
    let mut blocking = if session.mosaic { 101 } else { 0 };
    if let Some(first) = selected.first() {
        let first_count = selected
            .iter()
            .take_while(|row| {
                row.chunk == first.chunk && (blocking > 0 || row.time_s == first.time_s)
            })
            .count();
        let first_count = if blocking > 0 {
            first_count.min(blocking)
        } else {
            first_count
        };
        if n > 1 && first_count < n * (n - 1) / 4 {
            blocking = n * (n - 1) / 2 + n + 1;
        }
    }
    let value = accumulate(selected, blocking);
    reservation.retain(
        (value.antennas.capacity() * size_of::<(Antenna, [f64; 3])>()
            + value.rows.capacity() * size_of::<(u64, usize)>()
            + 128) as u64,
    );
    Ok(Arc::new(Admitted { value, reservation }))
}

fn accumulate(selected: Vec<SelectedRow>, blocking: usize) -> Geometry {
    let mut sums = HashMap::<Antenna, ([f64; 3], usize)>::new();
    let mut references = Vec::<usize>::new();
    let mut rows = Vec::with_capacity(selected.len());
    let mut previous = None;
    let mut in_buffer = 0;
    for row in selected {
        let key = (row.chunk, row.time_s.to_bits());
        if previous
            .is_none_or(|old: (Chunk, u64)| old.0 != key.0 || (blocking == 0 && old.1 != key.1))
            || (blocking > 0 && in_buffer == blocking)
        {
            references.push(row.antennas[0]);
            in_buffer = 0;
        }
        let buffer = references.len() - 1;
        references[buffer] = references[buffer].min(row.antennas[0]);
        rows.push((row.row, buffer));
        for (antenna, sign) in [(row.antennas[0], 1.), (row.antennas[1], -1.)] {
            let (position, count) = sums.entry((buffer, antenna)).or_insert(([0.; 3], 0));
            for (position, uvw) in position.iter_mut().zip(row.uvw) {
                *position += sign * uvw;
            }
            *count += 1;
        }
        in_buffer += 1;
        previous = Some(key);
    }
    let mut antennas = Vec::with_capacity(sums.len());
    for (&(buffer, antenna), &(sum, count)) in &sums {
        let (reference, n) = sums[&(buffer, references[buffer])];
        antennas.push((
            (buffer, antenna),
            std::array::from_fn(|dim| sum[dim] / count as f64 - reference[dim] / n as f64),
        ));
    }
    antennas.sort_unstable_by_key(|entry| entry.0);
    rows.sort_unstable_by_key(|entry| entry.0);
    Geometry { antennas, rows }
}

/// FTMachine::refocus, after rotateUVW. Input and output use the native
/// operator's UVW and phase signs; the equations run in CASA's convention.
pub(crate) fn apply(
    positions: &Positions,
    number: u64,
    row: &RowGeometry,
    projection: &GridFtProjection,
    distance: f64,
    uvw: [f64; 3],
    phase: f64,
) -> ([f64; 3], f64) {
    let value = &positions.value;
    let Ok(index) = value.rows.binary_search_by_key(&number, |entry| entry.0) else {
        return (uvw, phase);
    };
    let key = value.rows[index].1;
    let at = |antenna| {
        value
            .antennas
            .binary_search_by_key(&(key, antenna), |entry| entry.0)
            .ok()
            .map(|index| {
                let (p, _) = projection.apply(value.antennas[index].1);
                [-p[0], -p[1], p[2]]
            })
    };
    let (Some(a), Some(b)) = (at(row.antenna1), at(row.antenna2)) else {
        // This time or row was excluded from the operation; the final
        // selection mask preserves its stored science exactly.
        return (uvw, phase);
    };
    let length = |p: [f64; 3]| {
        let mut squared = distance * distance - 2. * distance * p[2];
        for x in p {
            squared += x * x;
        }
        squared.sqrt()
    };
    let (d1, d2) = (length(a), length(b));
    let mut dphase = -phase;
    for dim in 0..2 {
        dphase -= (a[dim] * a[dim] - b[dim] * b[dim]) / (2. * distance);
    }
    (
        [
            -distance * (a[0] / d1 - b[0] / d2),
            -distance * (a[1] / d1 - b[1] / d2),
            distance * (a[2] / d1 - b[2] / d2) + dphase,
        ],
        -dphase,
    )
}

fn error(value: impl std::fmt::Display) -> SimulatorError {
    SimulatorError::raised(format!("simulator finite-distance geometry: {value}"))
}
