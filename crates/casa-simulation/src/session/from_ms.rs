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
    /// Each scan's first and last TIME, seconds: a POINTING row in that
    /// span points the scan.
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
        let path = path.as_ref();
        let ms = MeasurementSet::open(path).map_err(|error| {
            SimulatorError::raised(format!("Exception Reported: {}: {error}", path.display()))
        })?;
        let mut session = Self::open(path, options);
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
        match pointing_away_from_phase_centre(&self.path, &self.scans, &opened.scan_times_s)? {
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

/// The first scan a POINTING row within its time span points away from
/// its phase centre, `None` when every row points at the scans covering
/// its TIME; without POINTING rows CASA points at the fields.
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
    // The scans by start, with the latest end so far, to find those covering
    // a time.
    let mut by_start: Vec<usize> = (0..scans.len()).collect();
    by_start.sort_by(|&a, &b| times_s[a][0].total_cmp(&times_s[b][0]));
    let mut latest_end = Vec::with_capacity(by_start.len());
    for &scan in &by_start {
        let end = latest_end
            .last()
            .map_or(f64::NEG_INFINITY, |&end: &f64| end);
        latest_end.push(end.max(times_s[scan][1]));
    }
    let (mut times, mut rows) = (Vec::new(), Vec::new());
    let count = pointing.row_count();
    for first in (0..count).step_by(SCAN_READ_ROWS) {
        rows.clear();
        rows.extend(first..count.min(first + SCAN_READ_ROWS));
        times.clear();
        pointing
            .required_scalar_columns_for_rows_into(
                &rows,
                &mut [RequiredScalarColumnDestination::new(
                    "TIME",
                    RequiredScalarColumnValuesMut::Float64(&mut times),
                )],
            )
            .map_err(error)?;
        // Cell by cell, which every data manager serves: CASA keeps
        // DIRECTION in a StandardStMan.
        let directions = pointing
            .column_accessor("DIRECTION")
            .and_then(|column| column.array_cells_owned_uncached(&rows))
            .map_err(error)?;
        for (&time_s, direction) in times.iter().zip(&directions) {
            // DIRECTION's first polynomial term.
            let direction = match direction {
                Some(ArrayValue::Float64(direction))
                    if direction.ndim() == 2
                        && direction.shape()[0] == 2
                        && direction.shape()[1] > 0 =>
                {
                    [direction[[0, 0]], direction[[1, 0]]]
                }
                _ => continue,
            };
            let started = by_start.partition_point(|&scan| times_s[scan][0] <= time_s);
            for position in (0..started).rev() {
                if latest_end[position] < time_s {
                    break;
                }
                let scan = by_start[position];
                if times_s[scan][1] >= time_s
                    && separation_rad(scans[scan].phase_center_rad, direction) >= SAME_DIRECTION_RAD
                {
                    return Ok(Some(scan));
                }
            }
        }
    }
    Ok(None)
}

/// The angle between two directions given as (longitude, latitude),
/// radians.
fn separation_rad([lon1, lat1]: [f64; 2], [lon2, lat2]: [f64; 2]) -> f64 {
    let haversine = ((lat2 - lat1) / 2.0).sin().powi(2)
        + lat1.cos() * lat2.cos() * ((lon2 - lon1) / 2.0).sin().powi(2);
    2.0 * haversine.sqrt().min(1.0).asin()
}
