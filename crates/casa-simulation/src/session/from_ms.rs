// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.openfromms`: a session on a MeasurementSet that already exists.
//!
//! CASA's `Simulator(MeasurementSet&)` reads the array, the spectral
//! windows, the fields and the feed mode back from the MeasurementSet
//! (`NewMSSimulator::getAnt`, `getSpWindows`, `getFields`, `getFeedMode`);
//! its `predict` and `corrupt` then rewrite the rows already there. The
//! session reads what its steps take:
//! - ANTENNA, whose dishes the beams and the mosaic machine take;
//! - OBSERVATION, whose first telescope picks the beams (`VPSkyJones`). The
//!   session's own telescope stays `Unknown`, as `NewMSSimulator`'s does:
//!   only `setconfig` sets it;
//! - each data description's actual spectral-window and polarization IDs;
//! - each field's name, code and PHASE_DIR, the phase centre CASA's
//!   `VisBuffer` predicts at;
//! - MAIN's rows as scans: each run of consecutive rows with one
//!   SCAN_NUMBER, FIELD_ID and DATA_DESC_ID is a scan whose rows are on disk
//!   ([`ScanRows::OnDisk`]). They were observed before any step, so every
//!   later `predict` and `corrupt` covers them.
//!
//! `close` patches DATA in place (`crate::write::patch`). Observations added
//! after opening are rendered through the same geometry and DATA operators,
//! then published atomically with the existing rows. Publishing currently
//! copies the existing MS, so its I/O cost grows with the existing data.
//!
//! What casa-rs does not read yet is refused, here or by the step needing
//! it, rather than predicted wrongly: a primary beam, the mosaic
//! machine or total-power sampling when POINTING does not point at the
//! scans' phase centres. Adding to the MeasurementSet after `openfromms`
//! (`setconfig`, `setfeed`, `setspwindow`, `setfield`, `setmosaicfield`)
//! is not yet implemented; `observe` and `observemany` extend the schedule.

use std::collections::HashMap;
use std::path::Path;

use casa_ms::MeasurementSet;
use casa_ms::derived::engine::raw_field_phase_direction;
use casa_tables::{
    RequiredScalarColumnDestination, RequiredScalarColumnValuesMut, Table, TableOptions,
};
use casa_types::ArrayValue;

use super::array::Antenna;
use super::error::{SimulatorError, SimulatorResult};
use super::fields::Field;
use super::observe::{Observation, Scan, ScanRows};
use super::spectral::{DataDescription, SpectralWindow};
use super::{Simulator, SimulatorOptions};

#[path = "from_ms_append.rs"]
mod append;
#[path = "from_ms_channels.rs"]
mod channels;
#[path = "from_ms_descriptions.rs"]
mod descriptions;
#[path = "from_ms_polynomial.rs"]
pub(super) mod polynomial;
#[path = "from_ms_scans.rs"]
mod scan_groups;

/// MAIN rows read at a time while finding the scans.
const SCAN_READ_ROWS: usize = 1024;

/// Two directions closer than this, radians, are one: a POINTING row at a
/// scan's phase centre (about 0.2 mas).
const SAME_DIRECTION_RAD: f64 = 1e-9;

/// The MeasurementSet a session opened with `openfromms`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OpenedMs {
    /// Whether MAIN has MODEL_DATA, which CASA's `predict` sets to 1.
    pub(crate) model_data: bool,
    /// Whether MAIN has CORRECTED_DATA, which CASA's `predict` and
    /// `corrupt` set to DATA. casa-rs keeps the scratch columns there are
    /// in step with DATA and creates none (plan decision 6).
    pub(crate) corrected_data: bool,
    /// Each scan's first and last TIME, seconds: a POINTING row valid
    /// during that span points the scan.
    pub(crate) scan_times_s: Vec<[f64; 2]>,
    /// Each scan's distinct TIMEs in order, seconds: its integrations,
    /// which `setgain` solves one by one.
    pub(crate) integration_times_s: Vec<Vec<f64>>,
}

fn read_error(what: &'static str) -> impl Fn(casa_ms::MsError) -> SimulatorError {
    move |error| SimulatorError::raised(format!("openfromms: {what}: {error}"))
}

impl Simulator {
    /// `sm.openfromms(ms)`: a session on the MeasurementSet at `path`, whose
    /// rows the following `predict` and `corrupt` rewrite at
    /// [`close`](Self::close).
    ///
    /// # Errors
    ///
    /// The MeasurementSet cannot be read (raised, as CASA's table open
    /// raises), or it holds what casa-rs does not read yet (see the
    /// module).
    pub fn open_from_ms(
        path: impl AsRef<Path>,
        options: SimulatorOptions,
    ) -> SimulatorResult<Self> {
        let raised = |error: &dyn std::fmt::Display| {
            SimulatorError::raised(format!(
                "Exception Reported: {}: {error}",
                path.as_ref().display()
            ))
        };
        // The session holds the MeasurementSet it read: `close` and the
        // POINTING reads find it whatever the working directory is by then.
        let path = std::path::absolute(path.as_ref()).map_err(|error| raised(&error))?;
        // Declare admission first so cached FIELD cells drop before its
        // reservation on errors as well as on the successful return.
        let phase_memory;
        let ms = MeasurementSet::open(&path).map_err(|error| raised(&error))?;
        let mut session = Self::open(&path, options);
        session.antennas = read_antennas(&ms)?;
        session.observations = read_observations(&ms)?;
        let mappings = descriptions::read(&ms, &session.options.resources)?;
        session.polarizations = mappings.polarizations;
        session.data_descriptions = mappings.descriptions;
        session.description_memory = Some(mappings.memory);
        session.spectral_windows = read_windows(
            &ms,
            &session.data_descriptions,
            &session.polarizations,
            &session.options.resources,
        )?;
        // Append reads the feed rows when observe first needs them.
        session.feeds_set = true;
        phase_memory = polynomial::admit_fields(&ms, &session.options.resources)?;
        session.fields = read_fields(&session, &ms)?;
        let ReadScans {
            scans,
            times_s: scan_times_s,
            integrations,
            memory,
        } = read_scans(&ms, &session)?;
        session.scans = scans;
        session.scan_memory = Some(memory);
        let main = ms.main_table();
        let has = |column: &str| {
            main.schema()
                .is_some_and(|schema| schema.contains_column(column))
        };
        session.opened = Some(OpenedMs {
            model_data: has("MODEL_DATA"),
            corrected_data: has("CORRECTED_DATA"),
            scan_times_s,
            integration_times_s: integrations,
        });
        tracing::info!(
            rows = ms.row_count(),
            scans = session.scans.len(),
            antennas = session.antennas.len(),
            spectral_windows = session.spectral_windows.len(),
            fields = session.fields.len(),
            "Opened MeasurementSet {}",
            path.display()
        );
        // Borrowed FIELD cells may remain cached in ms until it drops.
        drop(ms);
        drop(phase_memory);
        Ok(session)
    }

    /// Refuse `call`, which adds to the MeasurementSet, after `openfromms`.
    pub(super) fn refuse_after_openfromms(&self, call: &str) -> SimulatorResult<()> {
        if self.opened.is_some() {
            return Err(SimulatorError::not_yet_implemented(format!(
                "{call} after openfromms is not yet implemented in casa-rs (#1053)"
            )));
        }
        Ok(())
    }

    /// Read POINTING only when a prediction consumes it, leaving visibility
    /// phase centres independent of the antennas' beam/sample directions.
    pub(super) fn load_pointings(&mut self) -> SimulatorResult<()> {
        let Some(opened) = &self.opened else {
            return Ok(());
        };
        let directions = read_pointings(
            self.path(),
            &self.scans,
            &opened.scan_times_s,
            &self.options.resources,
            self.antennas.len(),
        )?;
        for (scan, direction) in self.scans.iter_mut().zip(directions) {
            if matches!(scan.rows, ScanRows::OnDisk(_)) {
                scan.pointing_rad = direction.unwrap_or(scan.phase_center_rad);
            }
        }
        Ok(())
    }

    /// The MeasurementSet `openfromms` opened, `None` for a session that
    /// creates one.
    pub(crate) fn opened(&self) -> Option<&OpenedMs> {
        self.opened.as_ref()
    }
}

fn read_antennas(ms: &MeasurementSet) -> SimulatorResult<Vec<Antenna>> {
    let error = read_error("ANTENNA");
    let antenna = ms.antenna().map_err(&error)?;
    (0..antenna.row_count())
        .map(|row| {
            Ok(Antenna {
                name: antenna.name(row).map_err(&error)?,
                station: antenna.station(row).map_err(&error)?,
                mount: antenna.mount(row).map_err(&error)?,
                diameter_m: antenna.dish_diameter(row).map_err(&error)?,
                position_itrf_m: antenna.position(row).map_err(&error)?,
            })
        })
        .collect()
}

fn read_observations(ms: &MeasurementSet) -> SimulatorResult<Vec<Observation>> {
    let error = read_error("OBSERVATION");
    let observation = ms.observation().map_err(&error)?;
    (0..observation.row_count())
        .map(|row| {
            let time_range = match observation.array(row, "TIME_RANGE").map_err(&error)? {
                ArrayValue::Float64(range) if range.len() == 2 => [range[[0]], range[[1]]],
                _ => {
                    return Err(SimulatorError::raised(
                        "openfromms: OBSERVATION TIME_RANGE is not two Float64 values",
                    ));
                }
            };
            Ok(Observation {
                telescope: observation.string(row, "TELESCOPE_NAME").map_err(&error)?,
                time_range_s: time_range,
                observer: observation.string(row, "OBSERVER").map_err(&error)?,
                project: observation.string(row, "PROJECT").map_err(&error)?,
            })
        })
        .collect()
}

/// Physical spectral windows; each MAIN data description supplies its own
/// polarization setup independently of this physical frequency index.
fn read_windows(
    ms: &MeasurementSet,
    descriptions: &[DataDescription],
    polarizations: &[Vec<i32>],
    resources: &super::SessionResources,
) -> SimulatorResult<Vec<SpectralWindow>> {
    let windows = ms
        .spectral_window()
        .map_err(read_error("SPECTRAL_WINDOW"))?;
    (0..windows.row_count())
        .map(|row| {
            let correlations = descriptions
                .iter()
                .find(|d| d.window == row)
                .map(|d| polarizations[d.polarization].clone())
                .unwrap_or_default();
            read_window(
                &windows,
                ms.subtable(casa_ms::SubtableId::SpectralWindow)
                    .expect("spectral window"),
                row,
                correlations,
                resources,
            )
        })
        .collect()
}

fn read_window(
    windows: &casa_ms::subtables::spectral_window::MsSpectralWindow<'_>,
    raw: &Table,
    row: usize,
    correlations: Vec<i32>,
    resources: &super::SessionResources,
) -> SimulatorResult<SpectralWindow> {
    let error = read_error("SPECTRAL_WINDOW");
    let (frequencies, widths, mut reservation) = channels::read(raw, row, resources)?;
    let (Some(&start_frequency_hz), Some(&channel_width_hz)) =
        (frequencies.first(), widths.first())
    else {
        return Err(SimulatorError::raised(format!(
            "openfromms: spectral window {row} has no channels"
        )));
    };
    let frame = windows.frequency_reference(row).map_err(&error)?;
    // Validate each column's own reference descriptor. CASA's VPSkyJones
    // selects the beam model from REF_FREQUENCY's raw Hz value; its frame
    // need not equal the channel frequencies' frame.
    windows.reference_frequency_reference(row).map_err(&error)?;
    let mut window = SpectralWindow {
        stored_frequencies: None,
        name: windows.name(row).map_err(&error)?,
        channel_count: frequencies.len(),
        start_frequency_hz,
        channel_width_hz,
        reference_frequency_hz: windows.ref_frequency(row).map_err(&error)?,
        total_bandwidth_hz: windows.total_bandwidth(row).map_err(&error)?,
        frame,
        correlations,
    };
    // Generated windows retain the compact linear representation. Other
    // observed axes keep their stored frequencies and their memory charge.
    let linear = widths.iter().all(|&width| width == channel_width_hz)
        && window
            .channel_frequencies_hz()
            .iter()
            .zip(&frequencies)
            .all(|(axis, stored)| (axis - stored).abs() <= 4.0 * f64::EPSILON * stored.abs());
    drop(widths);
    if !linear {
        reservation.retain((frequencies.capacity() * size_of::<f64>() + 64) as u64);
        window.stored_frequencies = Some(std::sync::Arc::new(casa_imaging_runtime::Admitted {
            value: frequencies,
            reservation,
        }));
    }
    Ok(window)
}

fn read_fields(session: &Simulator, ms: &MeasurementSet) -> SimulatorResult<Vec<Field>> {
    let error = read_error("FIELD");
    let field = ms.field().map_err(&error)?;
    (0..field.row_count())
        .map(|row| {
            let direction = raw_field_phase_direction(ms, row).map_err(&error)?;
            Ok(Field {
                polynomial: field.num_poly(row).map_err(&error)? > 0,
                name: field.name(row).map_err(&error)?,
                code: field.code(row).map_err(&error)?,
                direction_rad: session.j2000_rad(&direction, "the field's PHASE_DIR")?,
                distance_m: 0.0,
                source_time_s: field.time(row).map_err(&error)?,
                rest_frequency_hz: None,
            })
        })
        .collect()
}

/// MAIN's rows as scans: each run of consecutive rows with one SCAN_NUMBER,
/// FIELD_ID and DATA_DESC_ID, phased on its field's PHASE_DIR, as CASA's
/// `VisBuffer` phases it; each scan's first and last TIME; and each scan's
/// integrations, its distinct TIMEs in order.
///
/// A pointing `observemany` moved off its field keeps its field's FIELD
/// row, so it is phased on the field: CASA predicts a source at the field's
/// direction with zero phase there although the pointing's UVW were
/// computed at the moved direction (measured on CASA 6.7.6;
/// `docs/CASA (C++) bugs.md`).
fn read_scans(ms: &MeasurementSet, session: &Simulator) -> SimulatorResult<ReadScans> {
    let fields = &session.fields;
    let mappings = &session.data_descriptions;
    let main = ms.main_table();
    let rows = main.row_count();
    let count = scan_groups::count(ms, &session.options.resources)?;
    let mut memory = casa_imaging_runtime::admit(
        session.options.resources.host(),
        session.options.resources.policy(),
        &casa_imaging_runtime::Demand {
            phase: "reading stored simulation integrations",
            memory: (count as u64)
                .saturating_mul(
                    (size_of::<Scan>() + size_of::<[f64; 2]>() + size_of::<Vec<f64>>() + 8) as u64,
                )
                .saturating_add(512 << 10),
        },
    )
    .map_err(SimulatorError::admission)?;
    let mut scans: Vec<Scan> = Vec::with_capacity(count);
    let mut times_s: Vec<[f64; 2]> = Vec::with_capacity(count);
    let mut integrations: Vec<Vec<f64>> = Vec::with_capacity(count);
    let (mut numbers, mut field_ids, mut descriptions) = (Vec::new(), Vec::new(), Vec::new());
    let mut observation_ids = Vec::new();
    let mut array_ids = Vec::new();
    let mut row_times = Vec::new();
    let mut indices = Vec::with_capacity(SCAN_READ_ROWS.min(rows));
    for first in (0..rows).step_by(SCAN_READ_ROWS) {
        indices.clear();
        indices.extend(first..rows.min(first + SCAN_READ_ROWS));
        for column in [&mut numbers, &mut field_ids, &mut descriptions] {
            column.clear();
        }
        row_times.clear();
        observation_ids.clear();
        array_ids.clear();
        main.required_scalar_columns_for_rows_into(
            &indices,
            &mut [
                RequiredScalarColumnDestination::new(
                    "ARRAY_ID",
                    RequiredScalarColumnValuesMut::Int32(&mut array_ids),
                ),
                RequiredScalarColumnDestination::new(
                    "OBSERVATION_ID",
                    RequiredScalarColumnValuesMut::Int32(&mut observation_ids),
                ),
                RequiredScalarColumnDestination::new(
                    "SCAN_NUMBER",
                    RequiredScalarColumnValuesMut::Int32(&mut numbers),
                ),
                RequiredScalarColumnDestination::new(
                    "FIELD_ID",
                    RequiredScalarColumnValuesMut::Int32(&mut field_ids),
                ),
                RequiredScalarColumnDestination::new(
                    "DATA_DESC_ID",
                    RequiredScalarColumnValuesMut::Int32(&mut descriptions),
                ),
                RequiredScalarColumnDestination::new(
                    "TIME",
                    RequiredScalarColumnValuesMut::Float64(&mut row_times),
                ),
            ],
        )
        .map_err(|error| SimulatorError::raised(format!("openfromms: MAIN: {error}")))?;
        for (offset, (((&number, &field), &description), &time_s)) in numbers
            .iter()
            .zip(&field_ids)
            .zip(&descriptions)
            .zip(&row_times)
            .enumerate()
        {
            let row = first + offset;
            let field_id = index(field, fields.len(), row, "FIELD_ID")?;
            let data_description_id = index(description, mappings.len(), row, "DATA_DESC_ID")?;
            let window = mappings[data_description_id].window;
            if let Some(scan) = scans.last_mut()
                && (
                    scan.scan_number,
                    scan.array_id,
                    scan.observation_id,
                    scan.field_id,
                    scan.data_description_id,
                ) == (
                    number,
                    array_ids[offset],
                    observation_ids[offset],
                    field_id,
                    data_description_id,
                )
                && let ScanRows::OnDisk(scan_rows) = &mut scan.rows
                && let Some(span) = times_s.last_mut()
                && let Some(times) = integrations.last_mut()
                && span[0] == time_s
            {
                scan_rows.end = row + 1;
                *span = [span[0].min(time_s), span[1].max(time_s)];
                if times.last() != Some(&time_s) {
                    times.push(time_s);
                }
                continue;
            }
            scans.push(Scan {
                array_id: array_ids[offset],
                scan_number: number,
                observation_id: observation_ids[offset],
                field_id,
                spectral_window_id: window,
                data_description_id,
                phase_center_rad: fields[field_id].direction_rad,
                // A beam or total-power step reads stored POINTING lazily.
                pointing_rad: fields[field_id].direction_rad,
                rows: ScanRows::OnDisk(row..row + 1),
            });
            times_s.push([time_s, time_s]);
            integrations.push(vec![time_s]);
        }
    }
    for times in &mut integrations {
        times.sort_by(f64::total_cmp);
        times.dedup();
    }
    polynomial::set_scan_centres(session, ms, &mut scans, &times_s)?;
    memory.retain(
        (scans.capacity() * size_of::<Scan>()
            + times_s.capacity() * size_of::<[f64; 2]>()
            + integrations.capacity() * size_of::<Vec<f64>>()
            + integrations
                .iter()
                .map(|times| times.capacity() * 8)
                .sum::<usize>()) as u64,
    );
    Ok(ReadScans {
        scans,
        times_s,
        integrations,
        memory,
    })
}

/// MAIN's scans as `read_scans` reads them.
struct ReadScans {
    scans: Vec<Scan>,
    /// Each scan's first and last TIME.
    times_s: Vec<[f64; 2]>,
    /// Each scan's distinct TIMEs in order.
    integrations: Vec<Vec<f64>>,
    memory: casa_imaging_runtime::Reservation,
}

/// `value`, MAIN row `row`'s `column`, as an index below `count`.
fn index(value: i32, count: usize, row: usize, column: &str) -> SimulatorResult<usize> {
    usize::try_from(value)
        .ok()
        .filter(|&index| index < count)
        .ok_or_else(|| {
            SimulatorError::raised(format!(
                "openfromms: MAIN row {row} has {column} {value}, which does not exist"
            ))
        })
}

/// Resolve valid stored pointings for each integration, retaining FIELD as
/// the fallback when no POINTING row covers it.
///
/// A row is valid for the times casacore's `MSPointingColumns::
/// pointingIndex` finds it at, which CASA's `VisBuffer` reads each row's
/// pointing with ([`Validity`]). A scan's span is from its first to its
/// last TIME, so a row valid between two of its integrations counts too:
/// that refuses what CASA might not use, rather than predicting a scan at
/// a pointing it does not have.
fn read_pointings(
    path: &Path,
    scans: &[Scan],
    times_s: &[[f64; 2]],
    resources: &super::SessionResources,
    antenna_count: usize,
) -> SimulatorResult<Vec<Option<[f64; 2]>>> {
    let error =
        |error: casa_tables::TableError| SimulatorError::raised(format!("read POINTING: {error}"));
    let pointing_path = path.join("POINTING");
    if !pointing_path.exists() {
        return Ok(vec![None; scans.len()]);
    }
    let words = antenna_count.div_ceil(64);
    let memory;
    let pointing = Table::open(TableOptions::new(&pointing_path)).map_err(error)?;
    memory = casa_imaging_runtime::admit(
        resources.host(),
        resources.policy(),
        &casa_imaging_runtime::Demand {
            phase: "reading stored simulation POINTING",
            memory: (scans.len() as u64)
                .saturating_mul(128 + words as u64 * 8)
                .saturating_add((pointing.row_count() as u64).saturating_mul(256))
                .saturating_add(256 << 10),
        },
    )
    .map_err(SimulatorError::admission)?;
    let _ = &memory;
    let frame = casa_tables::table_measures::TableMeasDesc::reconstruct(&pointing, "DIRECTION")
        .ok_or_else(|| SimulatorError::raised("POINTING.DIRECTION has no measure descriptor"))?;
    if !matches!(frame.ref_desc(), casa_tables::table_measures::MeasRefDesc::Fixed { refer } if refer == "J2000")
        || frame.offset_desc().is_some()
    {
        return Err(SimulatorError::not_yet_implemented(
            "POINTING frame/measure offsets are not yet implemented (#1294)",
        ));
    }
    let by_time = ScansByTime::new(times_s);
    let mut centres = vec![None; scans.len()];
    let mut coverage = vec![0u64; scans.len() * words];
    let mut validity = Validity::default();
    let (mut antennas, mut times, mut intervals, mut rows) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut valid = Vec::new();
    let count = pointing.row_count();
    for first in (0..count).step_by(256) {
        rows.clear();
        rows.extend(first..count.min(first + 256));
        antennas.clear();
        times.clear();
        intervals.clear();
        pointing
            .required_scalar_columns_for_rows_into(
                &rows,
                &mut [
                    RequiredScalarColumnDestination::new(
                        "ANTENNA_ID",
                        RequiredScalarColumnValuesMut::Int32(&mut antennas),
                    ),
                    RequiredScalarColumnDestination::new(
                        "TIME",
                        RequiredScalarColumnValuesMut::Float64(&mut times),
                    ),
                    RequiredScalarColumnDestination::new(
                        "INTERVAL",
                        RequiredScalarColumnValuesMut::Float64(&mut intervals),
                    ),
                ],
            )
            .map_err(error)?;
        // Cell by cell, which every data manager serves: CASA keeps
        // DIRECTION in a StandardStMan.
        let column = pointing.column_accessor("DIRECTION").map_err(error)?;
        for shape in column.array_shapes_uncached(&rows).map_err(error)? {
            if shape.as_deref() != Some(&[2, 1]) {
                return Err(SimulatorError::not_yet_implemented(
                    "POINTING polynomials are not yet implemented (#1294)",
                ));
            }
        }
        let directions = column.array_cells_owned_uncached(&rows).map_err(error)?;
        for (((&antenna, &time_s), &interval_s), direction) in
            antennas.iter().zip(&times).zip(&intervals).zip(&directions)
        {
            let direction = fixed_direction(direction.as_ref());
            validity.push((antenna, time_s, interval_s), direction, &mut valid);
            by_time.assign(&mut valid, &mut centres, &mut coverage, antenna_count)?;
        }
    }
    validity.finish(&mut valid);
    by_time.assign(&mut valid, &mut centres, &mut coverage, antenna_count)?;
    for (index, direction) in centres.iter().enumerate() {
        if direction.is_some_and(|direction| {
            separation_rad(direction, scans[index].phase_center_rad) >= SAME_DIRECTION_RAD
        }) && coverage[index * words..(index + 1) * words]
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum::<usize>()
            != antenna_count
        {
            return Err(SimulatorError::not_yet_implemented(
                "offset POINTING without coverage for every antenna is not yet implemented (#1294)",
            ));
        }
    }
    Ok(centres)
}

/// A POINTING row's DIRECTION, as (longitude, latitude), radians: its
/// first polynomial term, `None` when a later term moves it or the cell is
/// not a direction, which no scan's phase centre matches.
fn fixed_direction(cell: Option<&ArrayValue>) -> Option<[f64; 2]> {
    match cell {
        Some(ArrayValue::Float64(direction))
            if direction.ndim() == 2 && direction.shape()[0] == 2 && direction.shape()[1] > 0 =>
        {
            let moves = direction
                .indexed_iter()
                .any(|(index, &term)| index[1] > 0 && term != 0.0);
            (!moves).then(|| [direction[[0, 0]], direction[[1, 0]]])
        }
        _ => None,
    }
}

/// A POINTING row's direction ([`fixed_direction`]) and the times it is
/// valid for, seconds.
type ValidPointing = (i32, Option<[f64; 2]>, [f64; 2]);

/// The times POINTING rows are valid for, found in table order as
/// casacore's `MSPointingColumns::pointingIndex` finds them. A row is
/// valid within half its INTERVAL of its TIME; with INTERVAL 0, within half
/// the gap to its antenna's next row at another TIME, else to its previous
/// one, else at every time; and with a negative INTERVAL at every time.
#[derive(Debug, Default)]
struct Validity {
    antennas: HashMap<i32, AntennaPointings>,
}

/// One antenna's rows so far.
#[derive(Debug)]
struct AntennaPointings {
    /// The TIME of its last row.
    last_s: f64,
    /// The TIME of its last row at another TIME before that.
    earlier_s: Option<f64>,
    /// The directions of its rows of INTERVAL 0 at `last_s`, which the next
    /// row at another TIME bounds.
    waiting: Vec<Option<[f64; 2]>>,
}

impl Validity {
    /// Take the next row, of `antenna` at `time_s` with `interval_s`,
    /// pointing at `direction`: onto `valid` go the rows whose times are
    /// now known.
    fn push(
        &mut self,
        (antenna, time_s, interval_s): (i32, f64, f64),
        direction: Option<[f64; 2]>,
        valid: &mut Vec<ValidPointing>,
    ) {
        let rows = self
            .antennas
            .entry(antenna)
            .or_insert_with(|| AntennaPointings {
                last_s: time_s,
                earlier_s: None,
                waiting: Vec::new(),
            });
        if rows.last_s != time_s {
            let half = (time_s - rows.last_s).abs() / 2.0;
            let span = [rows.last_s - half, rows.last_s + half];
            valid.extend(
                rows.waiting
                    .drain(..)
                    .map(|direction| (antenna, direction, span)),
            );
            rows.earlier_s = Some(rows.last_s);
            rows.last_s = time_s;
        }
        if interval_s == 0.0 {
            rows.waiting.push(direction);
        } else if interval_s > 0.0 {
            let half = interval_s / 2.0;
            valid.push((antenna, direction, [time_s - half, time_s + half]));
        } else {
            valid.push((antenna, direction, [f64::NEG_INFINITY, f64::INFINITY]));
        }
    }

    /// Onto `valid` go the rows of INTERVAL 0 no later row bounds: by the
    /// antenna's earlier TIME, else at every time.
    fn finish(&mut self, valid: &mut Vec<ValidPointing>) {
        for (&antenna, rows) in &mut self.antennas {
            let span = rows
                .earlier_s
                .map_or([f64::NEG_INFINITY, f64::INFINITY], |earlier| {
                    let half = (rows.last_s - earlier).abs() / 2.0;
                    [rows.last_s - half, rows.last_s + half]
                });
            valid.extend(
                rows.waiting
                    .drain(..)
                    .map(|direction| (antenna, direction, span)),
            );
        }
    }
}

/// The scans by their first TIME, with the latest last TIME so far, to
/// find those a span of time overlaps.
struct ScansByTime<'t> {
    times_s: &'t [[f64; 2]],
    by_start: Vec<usize>,
    latest_end: Vec<f64>,
}

impl<'t> ScansByTime<'t> {
    fn new(times_s: &'t [[f64; 2]]) -> Self {
        let mut by_start: Vec<usize> = (0..times_s.len()).collect();
        by_start.sort_by(|&a, &b| times_s[a][0].total_cmp(&times_s[b][0]));
        let mut latest_end = Vec::with_capacity(by_start.len());
        for &scan in &by_start {
            let end = latest_end
                .last()
                .map_or(f64::NEG_INFINITY, |&end: &f64| end);
            latest_end.push(end.max(times_s[scan][1]));
        }
        Self {
            times_s,
            by_start,
            latest_end,
        }
    }

    /// Assign valid directions and preserve their per-antenna coverage.
    fn assign(
        &self,
        valid: &mut Vec<ValidPointing>,
        centres: &mut [Option<[f64; 2]>],
        coverage: &mut [u64],
        antenna_count: usize,
    ) -> SimulatorResult<()> {
        let words = antenna_count.div_ceil(64);
        for (antenna, direction, [from_s, to_s]) in valid.drain(..) {
            if antenna < 0 || antenna as usize >= antenna_count {
                continue;
            }
            let antenna = antenna as usize;
            let started = self
                .by_start
                .partition_point(|&scan| self.times_s[scan][0] <= to_s);
            for position in (0..started).rev() {
                if self.latest_end[position] < from_s {
                    break;
                }
                let scan = self.by_start[position];
                if self.times_s[scan][1] < from_s {
                    continue;
                }
                let Some(direction) = direction else {
                    return Err(SimulatorError::not_yet_implemented(
                        "POINTING direction is not constant (#1294)",
                    ));
                };
                if centres[scan]
                    .is_some_and(|known| separation_rad(known, direction) >= SAME_DIRECTION_RAD)
                {
                    return Err(SimulatorError::not_yet_implemented(
                        "POINTING changes within a scan or between antennas (#1294)",
                    ));
                }
                centres[scan] = Some(direction);
                coverage[scan * words + antenna / 64] |= 1 << (antenna % 64);
            }
        }
        Ok(())
    }
}

/// The angle between two directions given as (longitude, latitude),
/// radians.
fn separation_rad([lon1, lat1]: [f64; 2], [lon2, lat2]: [f64; 2]) -> f64 {
    let haversine = ((lat2 - lat1) / 2.0).sin().powi(2)
        + lat1.cos() * lat2.cos() * ((lon2 - lon1) / 2.0).sin().powi(2);
    2.0 * haversine.sqrt().min(1.0).asin()
}
