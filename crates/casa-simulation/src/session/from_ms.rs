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
//! - each data description's spectral window and correlations. casa-rs's
//!   window `w` is data description `w`, so the data descriptions must name
//!   the windows in order, as both simulators write them;
//! - each field's name, code and PHASE_DIR, the phase centre CASA's
//!   `VisBuffer` predicts at;
//! - MAIN's rows as scans: each run of consecutive rows with one
//!   SCAN_NUMBER, FIELD_ID and DATA_DESC_ID is a scan whose rows are on disk
//!   ([`ScanRows::OnDisk`]). They were observed before any step, so every
//!   later `predict` and `corrupt` covers them.
//!
//! `close` then patches DATA in place (`crate::write::patch`).
//!
//! What casa-rs does not read yet is refused, here or by the step needing
//! it, rather than predicted wrongly: a PHASE_DIR polynomial, channel
//! frequencies off a linear axis, several feeds per antenna, data
//! descriptions out of the windows' order, and a primary beam, the mosaic
//! machine or total-power sampling when POINTING does not point at the
//! scans' phase centres. Adding to the MeasurementSet after `openfromms`
//! (`setconfig`, `setfeed`, `setspwindow`, `setfield`, `setmosaicfield`,
//! `observe`, `observemany`) is not yet implemented.

use std::collections::HashMap;
use std::path::Path;

use casa_ms::MeasurementSet;
use casa_ms::derived::engine::raw_field_phase_direction;
use casa_tables::{
    RequiredScalarColumnDestination, RequiredScalarColumnValuesMut, Table, TableOptions,
};
use casa_types::ArrayValue;
use casa_types::measures::frequency::FrequencyRef;

use super::array::Antenna;
use super::error::{SimulatorError, SimulatorResult};
use super::fields::Field;
use super::observe::{Observation, Scan, ScanRows};
use super::spectral::SpectralWindow;
use super::{Simulator, SimulatorOptions};

/// MAIN rows read at a time while finding the scans.
const SCAN_READ_ROWS: usize = 1 << 16;

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
        let ms = MeasurementSet::open(&path).map_err(|error| raised(&error))?;
        let mut session = Self::open(&path, options);
        session.antennas = read_antennas(&ms)?;
        session.observations = read_observations(&ms)?;
        session.spectral_windows = read_windows(&ms)?;
        refuse_several_feeds(&ms)?;
        // CASA's `getFeedMode` reads the feeds back; `observe`, which takes
        // them, is refused after `openfromms`.
        session.feeds_set = true;
        session.fields = read_fields(&session, &ms)?;
        let (scans, scan_times_s) =
            read_scans(&ms, &session.fields, session.spectral_windows.len())?;
        session.scans = scans;
        let main = ms.main_table();
        let has = |column: &str| {
            main.schema()
                .is_some_and(|schema| schema.contains_column(column))
        };
        session.opened = Some(OpenedMs {
            model_data: has("MODEL_DATA"),
            corrected_data: has("CORRECTED_DATA"),
            scan_times_s,
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
        Ok(session)
    }

    /// Refuse `call`, which adds to the MeasurementSet, after `openfromms`.
    pub(super) fn refuse_after_openfromms(&self, call: &str) -> SimulatorResult<()> {
        if self.opened.is_some() {
            return Err(SimulatorError::not_yet_implemented(format!(
                "{call} after openfromms is not yet implemented in casa-rs (#841)"
            )));
        }
        Ok(())
    }

    /// Refuse `what` (a primary beam, the mosaic machine, total-power
    /// sampling), which points each scan at its phase centre, on an opened
    /// MeasurementSet whose POINTING points a scan elsewhere: a pointing
    /// `observemany` moved off its field, or an offset or tracking error.
    /// POINTING is read here, when a step needs it.
    pub(super) fn require_pointings_at_phase_centres(&self, what: &str) -> SimulatorResult<()> {
        let Some(opened) = &self.opened else {
            return Ok(());
        };
        match pointing_away_from_phase_centre(self.path(), &self.scans, &opened.scan_times_s)? {
            Some(scan) => Err(SimulatorError::not_yet_implemented(format!(
                "{what} on a MeasurementSet whose POINTING points scan {} away from its \
                 field's PHASE_DIR is not yet implemented in casa-rs (#841)",
                self.scans[scan].scan_number
            ))),
            None => Ok(()),
        }
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

/// The windows, one per data description: casa-rs's window `w` is data
/// description `w` and spectral window `w`, with data description `w`'s
/// correlations.
fn read_windows(ms: &MeasurementSet) -> SimulatorResult<Vec<SpectralWindow>> {
    let error = read_error("SPECTRAL_WINDOW");
    let windows = ms.spectral_window().map_err(&error)?;
    let descriptions = ms
        .data_description()
        .map_err(read_error("DATA_DESCRIPTION"))?;
    let polarizations = ms.polarization().map_err(read_error("POLARIZATION"))?;
    let in_order = descriptions.row_count() == windows.row_count()
        && (0..descriptions.row_count())
            .all(|row| descriptions.spectral_window_id(row).ok() == Some(row as i32));
    if !in_order {
        return Err(SimulatorError::not_yet_implemented(
            "openfromms on a MeasurementSet whose data descriptions are not its spectral \
             windows in order is not yet implemented in casa-rs (#841)",
        ));
    }
    (0..windows.row_count())
        .map(|row| {
            let polarization = descriptions
                .polarization_id(row)
                .map_err(read_error("DATA_DESCRIPTION"))?;
            let correlations = usize::try_from(polarization)
                .ok()
                .filter(|&id| id < polarizations.row_count())
                .ok_or_else(|| {
                    SimulatorError::raised(format!(
                        "openfromms: data description {row} names POLARIZATION row \
                         {polarization}, which does not exist"
                    ))
                })
                .and_then(|id| {
                    polarizations
                        .corr_type(id)
                        .map_err(read_error("POLARIZATION"))
                })?;
            read_window(&windows, row, correlations)
        })
        .collect()
}

fn read_window(
    windows: &casa_ms::subtables::spectral_window::MsSpectralWindow<'_>,
    row: usize,
    correlations: Vec<i32>,
) -> SimulatorResult<SpectralWindow> {
    let error = read_error("SPECTRAL_WINDOW");
    let frequencies = windows.chan_freq(row).map_err(&error)?;
    let widths = windows.chan_width(row).map_err(&error)?;
    let (Some(&start_frequency_hz), Some(&channel_width_hz)) =
        (frequencies.first(), widths.first())
    else {
        return Err(SimulatorError::raised(format!(
            "openfromms: spectral window {row} has no channels"
        )));
    };
    let code = windows.meas_freq_ref(row).map_err(&error)?;
    let frame = FrequencyRef::from_casacore_code(code).ok_or_else(|| {
        SimulatorError::raised(format!(
            "openfromms: spectral window {row} has an unknown MEAS_FREQ_REF {code}"
        ))
    })?;
    let window = SpectralWindow {
        name: windows.name(row).map_err(&error)?,
        channel_count: frequencies.len(),
        start_frequency_hz,
        channel_width_hz,
        reference_frequency_hz: windows.ref_frequency(row).map_err(&error)?,
        frame,
        correlations,
    };
    // The prediction takes the channels as the start plus whole channel
    // widths, as both simulators write them.
    let linear = widths.iter().all(|&width| width == channel_width_hz)
        && window
            .channel_frequencies_hz()
            .iter()
            .zip(&frequencies)
            .all(|(axis, stored)| (axis - stored).abs() <= 4.0 * f64::EPSILON * stored.abs());
    if !linear {
        return Err(SimulatorError::not_yet_implemented(format!(
            "openfromms on spectral window {row}, whose channels are not evenly spaced by their \
             width, is not yet implemented in casa-rs (#841)"
        )));
    }
    Ok(window)
}

/// Refuse a FEED table with several feeds per antenna, which `setfeed`
/// cannot write either.
fn refuse_several_feeds(ms: &MeasurementSet) -> SimulatorResult<()> {
    let error = read_error("FEED");
    let feed = ms.feed().map_err(&error)?;
    for row in 0..feed.row_count() {
        if feed.i32(row, "FEED_ID").map_err(&error)? != 0 {
            return Err(SimulatorError::not_yet_implemented(
                "openfromms on a MeasurementSet with several feeds per antenna is not yet \
                 implemented in casa-rs (#841)",
            ));
        }
    }
    Ok(())
}

fn read_fields(session: &Simulator, ms: &MeasurementSet) -> SimulatorResult<Vec<Field>> {
    let error = read_error("FIELD");
    let field = ms.field().map_err(&error)?;
    (0..field.row_count())
        .map(|row| {
            if field.num_poly(row).map_err(&error)? != 0 {
                return Err(SimulatorError::not_yet_implemented(format!(
                    "openfromms on field {row}, whose PHASE_DIR is a polynomial, is not yet \
                     implemented in casa-rs (#841)"
                )));
            }
            let direction = raw_field_phase_direction(ms, row).map_err(&error)?;
            Ok(Field {
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
/// `VisBuffer` phases it; and each scan's first and last TIME.
///
/// A pointing `observemany` moved off its field keeps its field's FIELD
/// row, so it is phased on the field: CASA predicts a source at the field's
/// direction with zero phase there although the pointing's UVW were
/// computed at the moved direction (measured on CASA 6.7.6;
/// `docs/CASA (C++) bugs.md`).
fn read_scans(
    ms: &MeasurementSet,
    fields: &[Field],
    windows: usize,
) -> SimulatorResult<(Vec<Scan>, Vec<[f64; 2]>)> {
    let main = ms.main_table();
    let rows = main.row_count();
    let mut scans: Vec<Scan> = Vec::new();
    let mut times_s: Vec<[f64; 2]> = Vec::new();
    let (mut numbers, mut field_ids, mut descriptions) = (Vec::new(), Vec::new(), Vec::new());
    let mut row_times = Vec::new();
    let mut indices = Vec::with_capacity(SCAN_READ_ROWS.min(rows));
    for first in (0..rows).step_by(SCAN_READ_ROWS) {
        indices.clear();
        indices.extend(first..rows.min(first + SCAN_READ_ROWS));
        for column in [&mut numbers, &mut field_ids, &mut descriptions] {
            column.clear();
        }
        row_times.clear();
        main.required_scalar_columns_for_rows_into(
            &indices,
            &mut [
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
            let window = index(description, windows, row, "DATA_DESC_ID")?;
            if let Some(scan) = scans.last_mut()
                && (scan.scan_number, scan.field_id, scan.spectral_window_id)
                    == (number, field_id, window)
                && let ScanRows::OnDisk(scan_rows) = &mut scan.rows
                && let Some(span) = times_s.last_mut()
            {
                scan_rows.end = row + 1;
                *span = [span[0].min(time_s), span[1].max(time_s)];
                continue;
            }
            scans.push(Scan {
                scan_number: number,
                field_id,
                spectral_window_id: window,
                phase_center_rad: fields[field_id].direction_rad,
                // POINTING, read when a step needs it, must point there
                // (`require_pointings_at_phase_centres`).
                pointing_rad: fields[field_id].direction_rad,
                rows: ScanRows::OnDisk(row..row + 1),
            });
            times_s.push([time_s, time_s]);
        }
    }
    Ok((scans, times_s))
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

/// The first scan a POINTING row valid within its time span points away
/// from its phase centre, `None` when every row points at the scans it is
/// valid for; without POINTING rows CASA points at the fields.
///
/// A row is valid for the times casacore's `MSPointingColumns::
/// pointingIndex` finds it at, which CASA's `VisBuffer` reads each row's
/// pointing with ([`Validity`]). A scan's span is from its first to its
/// last TIME, so a row valid between two of its integrations counts too:
/// that refuses what CASA might not use, rather than predicting a scan at
/// a pointing it does not have.
fn pointing_away_from_phase_centre(
    path: &Path,
    scans: &[Scan],
    times_s: &[[f64; 2]],
) -> SimulatorResult<Option<usize>> {
    let error =
        |error: casa_tables::TableError| SimulatorError::raised(format!("read POINTING: {error}"));
    let pointing_path = path.join("POINTING");
    if !pointing_path.exists() {
        return Ok(None);
    }
    let pointing = Table::open(TableOptions::new(&pointing_path)).map_err(error)?;
    let by_time = ScansByTime::new(times_s);
    let mut validity = Validity::default();
    let (mut antennas, mut times, mut intervals, mut rows) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut valid = Vec::new();
    let count = pointing.row_count();
    for first in (0..count).step_by(SCAN_READ_ROWS) {
        rows.clear();
        rows.extend(first..count.min(first + SCAN_READ_ROWS));
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
        let directions = pointing
            .column_accessor("DIRECTION")
            .and_then(|column| column.array_cells_owned_uncached(&rows))
            .map_err(error)?;
        for (((&antenna, &time_s), &interval_s), direction) in
            antennas.iter().zip(&times).zip(&intervals).zip(&directions)
        {
            let direction = fixed_direction(direction.as_ref());
            validity.push((antenna, time_s, interval_s), direction, &mut valid);
            if let Some(scan) = by_time.away(&mut valid, scans) {
                return Ok(Some(scan));
            }
        }
    }
    validity.finish(&mut valid);
    Ok(by_time.away(&mut valid, scans))
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
type ValidPointing = (Option<[f64; 2]>, [f64; 2]);

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
            valid.extend(rows.waiting.drain(..).map(|direction| (direction, span)));
            rows.earlier_s = Some(rows.last_s);
            rows.last_s = time_s;
        }
        if interval_s == 0.0 {
            rows.waiting.push(direction);
        } else if interval_s > 0.0 {
            let half = interval_s / 2.0;
            valid.push((direction, [time_s - half, time_s + half]));
        } else {
            valid.push((direction, [f64::NEG_INFINITY, f64::INFINITY]));
        }
    }

    /// Onto `valid` go the rows of INTERVAL 0 no later row bounds: by the
    /// antenna's earlier TIME, else at every time.
    fn finish(&mut self, valid: &mut Vec<ValidPointing>) {
        for rows in self.antennas.values_mut() {
            let span = rows
                .earlier_s
                .map_or([f64::NEG_INFINITY, f64::INFINITY], |earlier| {
                    let half = (rows.last_s - earlier).abs() / 2.0;
                    [rows.last_s - half, rows.last_s + half]
                });
            valid.extend(rows.waiting.drain(..).map(|direction| (direction, span)));
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

    /// The first scan of `scans` a row of `valid`, which this empties, is
    /// valid during and points away from.
    fn away(&self, valid: &mut Vec<ValidPointing>, scans: &[Scan]) -> Option<usize> {
        for (direction, [from_s, to_s]) in valid.drain(..) {
            let started = self
                .by_start
                .partition_point(|&scan| self.times_s[scan][0] <= to_s);
            for position in (0..started).rev() {
                if self.latest_end[position] < from_s {
                    break;
                }
                let scan = self.by_start[position];
                let away = direction.is_none_or(|direction| {
                    separation_rad(scans[scan].phase_center_rad, direction) >= SAME_DIRECTION_RAD
                });
                if self.times_s[scan][1] >= from_s && away {
                    return Some(scan);
                }
            }
        }
        None
    }
}

/// The angle between two directions given as (longitude, latitude),
/// radians.
fn separation_rad([lon1, lat1]: [f64; 2], [lon2, lat2]: [f64; 2]) -> f64 {
    let haversine = ((lat2 - lat1) / 2.0).sin().powi(2)
        + lat1.cos() * lat2.cos() * ((lon2 - lon1) / 2.0).sin().powi(2);
    2.0 * haversine.sqrt().min(1.0).asin()
}
