// SPDX-License-Identifier: LGPL-3.0-or-later
//! The legacy MS spectral writers and their in-place publication.

use std::fmt::Write;
use std::path::Path;

use casa_core::measures::MeasFrame;
use casa_core::measures::frequency::FrequencyRef;
use casa_core::quanta::{Quantity, Unit, casacore};
use casa_ms::LegacyRegridQuantity;
use casa_ms::SubtableId;
use casa_ms::append_tool_history;
use casa_ms::selection::MsSelectionIoBudget;
use casa_ms::subtable_path_from_main;
use casa_ms::subtables::{MsAntenna, MsSpectralWindow};
use casa_mstransform::{
    HanningDataColumn, LegacyMsTool, LegacySpectralGrid, MsRegridOptions, MsTransformError,
    MsTransformRequest, MsTransformTaskOptions, ToolSpectralWindow, TransformDataColumn,
    mstransform_task, reframe_staged_ms, stage_frame_only_ms, tool_channel_frequencies,
};
use casa_tables::publication::{
    check_write_access, exchange_directories, lock_table_tree_for_replacement,
    lock_table_tree_for_replacement_with_held, preserve_replacement_tree_entries,
};
use casa_tables::{Table, TableOptions, TableWriteLock};
use pyo3::prelude::*;

use super::sort::SortedDestination;
use super::spectral_grid::{axis_argument, grid_direction};
use super::{AttachedMs, PyMsTool, runtime_error, strict_bool};
use crate::convert::Variant;

const UNSET_CENTRE: f64 = -1e30;

struct SpectralRewrite {
    options: MsRegridOptions,
    grid: LegacySpectralGrid,
    phase: Option<Variant>,
    hanning: bool,
}

enum RewriteOutcome<T> {
    Complete(T),
    Unchanged,
    Refused,
}

impl SpectralRewrite {
    fn method(&self) -> &'static str {
        match self.grid {
            LegacySpectralGrid::Cvel => "cvel",
            _ => "regridspw",
        }
    }

    fn unchanged(&mut self, ms: &AttachedMs) -> PyResult<bool> {
        let cvel = matches!(self.grid, LegacySpectralGrid::Cvel);
        let link = ms.working_metadata_link();
        link.verify_directory().map_err(runtime_error)?;
        let target = if self.options.outframe.is_empty() {
            None
        } else if self.options.outframe.eq_ignore_ascii_case("SOURCE") {
            return Ok(self.options.interpolation.is_empty());
        } else {
            match self.options.outframe.parse::<FrequencyRef>() {
                Ok(FrequencyRef::Undefined) | Err(_) => return Ok(true),
                Ok(target) => Some(target),
            }
        };
        if self.options.interpolation.is_empty() && target != Some(FrequencyRef::REST) {
            // SubMS declines an unknown interpolation without editing the grid.
            return Ok(true);
        }
        if matches!(self.grid, LegacySpectralGrid::FrameOnly) && target.is_none() {
            return Ok(true);
        }

        let tables = ms
            .subtables
            .spectral_grid_tables(&ms.main)
            .map_err(runtime_error)?;
        let mut unchanged =
            !cvel || tables.windows.iter().map(Table::row_count).sum::<usize>() == 1;
        for table in tables.windows.iter() {
            let window = MsSpectralWindow::new(table);
            for row in 0..table.row_count() {
                let code = window.meas_freq_ref(row).map_err(runtime_error)?;
                let reference = FrequencyRef::from_casacore_code(code)
                    .ok_or_else(|| runtime_error(format!("Invalid MEAS_FREQ_REF {code}")))?;
                if target.is_some_and(|target| target != reference) {
                    if target == Some(FrequencyRef::REST) {
                        return Err(runtime_error(
                            "No MeasFrame specified for conversion of Frequency",
                        ));
                    }
                    return Ok(false);
                }
                if unchanged && !self.options.interpolation.is_empty() {
                    unchanged = if cvel {
                        self.same_channels(&window, row, reference)?
                    } else if matches!(self.grid, LegacySpectralGrid::FrameOnly) {
                        true
                    } else {
                        self.grid.leaves_band_unchanged(
                            &window.chan_freq(row).map_err(runtime_error)?,
                            &window.chan_width(row).map_err(runtime_error)?,
                        )
                    };
                }
            }
        }
        link.verify_directory().map_err(runtime_error)?;
        if target == Some(FrequencyRef::REST) {
            self.options.outframe.clear();
        }
        Ok(unchanged || self.options.interpolation.is_empty())
    }

    fn same_channels(
        &self,
        window: &MsSpectralWindow<'_>,
        row: usize,
        reference: FrequencyRef,
    ) -> PyResult<bool> {
        let input = window.chan_freq(row).map_err(runtime_error)?;
        let spectrum = ToolSpectralWindow {
            centres_hz: input.clone(),
            widths_hz: window.chan_width(row).map_err(runtime_error)?,
            reference,
        };
        let budget = MsSelectionIoBudget::from_system_memory(1, size_of::<f64>(), None)
            .map_err(runtime_error)?
            .available_bytes;
        let candidate =
            tool_channel_frequencies(vec![spectrum], &self.options, budget, false, || {
                unreachable!("checked the frequency reference before testing the grid")
            });
        Ok(candidate.is_ok_and(|centres| centres == input))
    }

    fn prepare_phase(&mut self, ms: &AttachedMs) -> PyResult<bool> {
        if !matches!(self.grid, LegacySpectralGrid::Cvel) {
            return Ok(true);
        }
        let phase = self.phase.take().unwrap_or(Variant::Empty);
        let field = match &phase {
            phase if phase.is_empty_sequence() => Some(0),
            Variant::Text(text) if text.is_empty() => Some(0),
            Variant::Number(field) => Some(*field as i32),
            Variant::Numbers(fields) => fields.first().map(|&field| field as i32),
            Variant::Array { values, .. } => values.first().map(|&field| field as i32),
            _ => None,
        };
        if let Some(field) = field {
            let tables = ms
                .subtables
                .spectral_grid_tables(&ms.main)
                .map_err(runtime_error)?;
            let rows = tables.fields.iter().map(Table::row_count).sum::<usize>();
            if !(0..rows).contains(&(field as usize)) {
                return Ok(false);
            }
            self.options.phasecenter = field.to_string();
        } else {
            let measures = ms.spectral_query_measures().map_err(runtime_error)?;
            let Some(direction) = grid_direction(&phase, &measures) else {
                return Ok(false);
            };
            let frame = MeasFrame::new().with_measures(measures);
            let direction = direction.resolve_offset(&frame).map_err(runtime_error)?;
            self.options.phasecenter = format!(
                "{} {}rad {}rad",
                direction.refer(),
                direction.longitude_rad(),
                direction.latitude_rad()
            );
        }
        Ok(true)
    }
}

#[pymethods]
impl PyMsTool {
    /// Regrid the attached MeasurementSet in place.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outframe = "LSRK".into(), mode = "chan".into(), restfreq = -3e30, interpolation = "LINEAR".into(), start = -3e30, center = -3e30, bandwidth = -1., chanwidth = -1., hanning = true), text_signature = "($self, outframe='LSRK', mode='chan', restfreq=-3e+30, interpolation='LINEAR', start=-3e+30, center=-3e+30, bandwidth=-1.0, chanwidth=-1.0, hanning=True)")]
    fn regridspw(
        &self,
        py: Python<'_>,
        outframe: String,
        mode: String,
        restfreq: f64,
        interpolation: String,
        start: f64,
        center: f64,
        bandwidth: f64,
        chanwidth: f64,
        #[pyo3(from_py_with = strict_bool)] hanning: bool,
    ) -> PyResult<bool> {
        let centre = if start > UNSET_CENTRE {
            let derived = start + bandwidth / 2.;
            let derived = if mode == "chan" {
                derived.floor()
            } else {
                derived
            };
            if center > UNSET_CENTRE && derived != center {
                return Ok(false);
            }
            derived
        } else {
            center
        };
        // Empty mode and the default channel band only transform the frame.
        let grid = match mode.as_str() {
            "" => LegacySpectralGrid::FrameOnly,
            "chan" if centre < UNSET_CENTRE && bandwidth <= 0. && chanwidth <= 1. => {
                LegacySpectralGrid::FrameOnly
            }
            "chan" => LegacySpectralGrid::CenteredChannels {
                centre,
                bandwidth,
                channel_width: chanwidth,
            },
            _ => LegacySpectralGrid::CenteredNumeric {
                quantity: match mode.as_str() {
                    "freq" => LegacyRegridQuantity::Frequency,
                    "vrad" => LegacyRegridQuantity::RadioVelocity,
                    "vopt" => LegacyRegridQuantity::OpticalVelocity,
                    "wave" => LegacyRegridQuantity::Wavelength,
                    _ => return self.invalid_regridspw(),
                },
                centre,
                bandwidth,
                channel_width: chanwidth,
                rest_hz: restfreq,
            },
        };
        let interpolation = if matches!(grid, LegacySpectralGrid::FrameOnly) {
            "linear"
        } else {
            let fft_allowed = !matches!(
                grid,
                LegacySpectralGrid::CenteredNumeric {
                    quantity: LegacyRegridQuantity::OpticalVelocity
                        | LegacyRegridQuantity::Wavelength,
                    ..
                }
            );
            rewrite_interpolation(&interpolation, fft_allowed)
        };
        self.rewrite_spectra(
            py,
            SpectralRewrite {
                options: MsRegridOptions {
                    mode: "channel".into(),
                    nchan: -1,
                    start: String::new(),
                    width: String::new(),
                    nspw: 1,
                    interpolation: interpolation.into(),
                    phasecenter: String::new(),
                    restfreq: String::new(),
                    outframe,
                    veltype: "radio".into(),
                    preaverage: false,
                },
                grid,
                phase: None,
                hanning,
            },
        )
    }

    /// Combine the input windows and write a new spectral grid in place.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (mode = "channel".into(), nchan = -1, start = None, width = None, interp = "linear".into(), phasec = None, restfreq = None, outframe = String::new(), veltype = "radio".into(), hanning = true), text_signature = "($self, mode='channel', nchan=-1, start=[], width=[], interp='linear', phasec=[], restfreq=[], outframe='', veltype='radio', hanning=True)")]
    fn cvel(
        &self,
        py: Python<'_>,
        mode: String,
        nchan: i64,
        start: Option<Variant>,
        width: Option<Variant>,
        interp: String,
        phasec: Option<Variant>,
        restfreq: Option<Variant>,
        outframe: String,
        veltype: String,
        #[pyo3(from_py_with = strict_bool)] hanning: bool,
    ) -> PyResult<bool> {
        let unit = match mode.as_str() {
            "channel" | "channel_b" => None,
            "frequency" => Some("Hz"),
            "velocity" => Some("m/s"),
            _ => return Ok(false),
        };
        let Some(restfreq) = cvel_quantity(axis_argument("", restfreq), "Hz") else {
            return Ok(false);
        };
        if mode == "velocity" && matches!(restfreq.as_str(), "" | "0Hz" | "-0Hz") {
            return Ok(false);
        }
        let start = axis_argument(&mode, start);
        let width = axis_argument(&mode, width);
        let (start, width) = if let Some(unit) = unit {
            let (Some(start), Some(width)) =
                (cvel_quantity(start, unit), cvel_quantity(width, unit))
            else {
                return Ok(false);
            };
            (start, width)
        } else {
            (start, width)
        };
        let restfreq = if mode == "velocity" {
            restfreq
        } else {
            String::new()
        };
        let interpolation =
            rewrite_interpolation(&interp, mode != "velocity" || veltype != "optical");
        let veltype = if veltype == "optical" {
            veltype
        } else {
            "radio".into()
        };
        self.rewrite_spectra(
            py,
            SpectralRewrite {
                options: MsRegridOptions {
                    start,
                    width,
                    mode,
                    nchan,
                    nspw: 1,
                    interpolation: interpolation.into(),
                    phasecenter: String::new(),
                    restfreq,
                    outframe,
                    veltype,
                    preaverage: false,
                },
                grid: LegacySpectralGrid::Cvel,
                phase: phasec,
                hanning,
            },
        )
    }
}

fn cvel_quantity(text: String, unit: &str) -> Option<String> {
    if matches!(text.as_str(), "" | "[]") {
        return Some(String::new());
    }
    let quantity = Quantity::read(&text).ok()?;
    let target = Unit::new(unit).expect("cvel uses built-in units");
    // SubMS reads a QuantumHolder, then uses the non-conformant getValue overload.
    let value = casacore::get_value(&quantity, &target);
    value.is_finite().then(|| format!("{value}{unit}"))
}

fn rewrite_interpolation(interpolation: &str, fft_allowed: bool) -> &'static str {
    let interpolation = interpolation.to_ascii_lowercase();
    if interpolation.contains("nearest") {
        "nearest"
    } else if interpolation.contains("splin") {
        "spline"
    } else if interpolation.contains("cub") {
        "cubic"
    } else if interpolation.contains("fft") {
        if fft_allowed { "fftshift" } else { "" }
    } else if interpolation.is_empty() || interpolation.contains("linear") {
        "linear"
    } else {
        ""
    }
}

impl PyMsTool {
    fn invalid_regridspw(&self) -> PyResult<bool> {
        let attached = self.attached.lock().map_err(runtime_error)?;
        if attached.as_ref().is_some_and(|ms| ms.writable) {
            Err(runtime_error("Regridding failed."))
        } else {
            Ok(false)
        }
    }

    fn rewrite_spectra(&self, py: Python<'_>, mut rewrite: SpectralRewrite) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let cvel = rewrite.method() == "cvel";
            let Some(ms) = attached.as_mut() else {
                return if cvel {
                    Err(runtime_error("Tool is not attached to an MS"))
                } else {
                    Ok(false)
                };
            };
            if !ms.writable {
                return Ok(false);
            }
            if cvel && ms.selected_rows.is_some() {
                return Err(runtime_error(
                    "In-place cvel requires the complete attached MeasurementSet",
                ));
            }
            if !rewrite.prepare_phase(ms)? {
                return Ok(false);
            }
            if cvel {
                validate_cvel_windows(ms)?;
            }
            if ms.selected_rows.is_none()
                && ms
                    .working_metadata_link()
                    .row_count()
                    .map_err(runtime_error)?
                    == 0
            {
                return Ok(true);
            }
            if !has_unflagged_antenna(ms)? || rewrite.unchanged(ms)? {
                if cvel && rewrite.hanning {
                    return ms.hanning_rows(HanningDataColumn::Both);
                }
                return Ok(true);
            }
            if ms.selected_rows.is_some() {
                return Ok(false);
            }
            let read_locks = std::mem::take(&mut ms.read_locks).into_keys().collect();
            let hanning = rewrite.hanning;
            match rewrite_attached(ms, rewrite) {
                Ok(RewriteOutcome::Complete(mut replacement)) => {
                    if !replacement.write_locks.is_empty() {
                        replacement.write_locks.extend(
                            std::mem::take(&mut ms.write_locks)
                                .into_iter()
                                .filter(|(path, _)| !path.starts_with(&ms.path)),
                        );
                    }
                    *attached = Some(replacement);
                    Ok(true)
                }
                Ok(RewriteOutcome::Unchanged) => {
                    ms.retain_tables(read_locks)?;
                    if cvel && hanning {
                        return ms.hanning_rows(HanningDataColumn::Both);
                    }
                    Ok(true)
                }
                Ok(RewriteOutcome::Refused) => {
                    ms.retain_tables(read_locks)?;
                    Ok(false)
                }
                Err(error) => {
                    ms.retain_tables(read_locks)?;
                    Err(error)
                }
            }
        })
    }
}

fn validate_cvel_windows(ms: &AttachedMs) -> PyResult<()> {
    let link = ms.working_metadata_link();
    link.verify_directory().map_err(runtime_error)?;
    let tables = ms
        .subtables
        .spectral_grid_tables(&ms.main)
        .map_err(runtime_error)?;
    for table in tables.windows.iter() {
        let windows = MsSpectralWindow::new(table);
        for row in 0..table.row_count() {
            let centres = windows.chan_freq(row).map_err(runtime_error)?;
            let widths = windows.chan_width(row).map_err(runtime_error)?;
            let sign = if centres.first() > centres.last() {
                -1.
            } else {
                1.
            };
            // SubMS checks adjacent channel edges before combining windows.
            if centres.len() != widths.len()
                || centres
                    .windows(2)
                    .zip(widths.windows(2))
                    .take(centres.len().saturating_sub(2))
                    .any(|(centre, width)| {
                        let upper = centre[0] + sign * width[0].abs() / 2.;
                        let lower = centre[1] - sign * width[1].abs() / 2.;
                        (upper - lower).abs() > 1.
                    })
            {
                return Err(runtime_error(
                    "Inconsistent SPECTRAL_WINDOW table in input MS.",
                ));
            }
        }
    }
    link.verify_directory().map_err(runtime_error)
}

fn has_unflagged_antenna(ms: &AttachedMs) -> PyResult<bool> {
    let id = SubtableId::Antenna;
    let tables = ms
        .subtables
        .spectral_grid_geometry(&ms.main, &[id])
        .map_err(runtime_error)?;
    for table in tables[&id].iter() {
        let antenna = MsAntenna::new(table);
        for row in 0..table.row_count() {
            if !antenna.flag_row(row).map_err(runtime_error)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn rewrite_attached(
    ms: &AttachedMs,
    rewrite: SpectralRewrite,
) -> PyResult<RewriteOutcome<AttachedMs>> {
    let operation = rewrite.method();
    let interpolation = rewrite.options.interpolation.clone();
    let link = ms.working_metadata_link();
    link.verify_directory().map_err(runtime_error)?;
    check_write_access(&ms.path).map_err(runtime_error)?;
    let destination = SortedDestination::Table(ms.directory.clone());
    let locks = lock_table_tree_for_replacement_with_held(&ms.path, &ms.write_locks)
        .map_err(runtime_error)?;
    let workspace = tempfile::tempdir_in(ms.path.parent().unwrap_or_else(|| Path::new(".")))
        .map_err(runtime_error)?;
    let staged = workspace.path().join("spectral.ms");
    let frame_only = matches!(rewrite.grid, LegacySpectralGrid::FrameOnly);
    if frame_only {
        stage_frame_only_ms(&ms.path, &staged).map_err(runtime_error)?;
        match reframe_staged_ms(&staged, &rewrite.options) {
            Ok(true) => {}
            Ok(false) | Err(MsTransformError::RegridParameter { .. }) => {
                return Ok(RewriteOutcome::Refused);
            }
            Err(error) => return Err(runtime_error(error)),
        }
    } else {
        match regrid_to_stage(ms, rewrite, &staged)? {
            RewriteOutcome::Complete(()) => {}
            RewriteOutcome::Unchanged => return Ok(RewriteOutcome::Unchanged),
            RewriteOutcome::Refused => return Ok(RewriteOutcome::Refused),
        }
    }
    let table = Table::open(TableOptions::new(&staged)).map_err(runtime_error)?;
    let history = spectral_history(&table, &staged, &interpolation)?;
    append_tool_history(
        &table,
        true,
        &[format!("Transformed/regridded with {operation}")],
        &history,
        &format!("ms::{operation}()"),
        "ms",
        &mut Default::default(),
    )
    .map_err(runtime_error)?;
    drop(table);
    if !frame_only {
        preserve_replacement_tree_entries(&ms.path, &staged).map_err(runtime_error)?;
    }
    link.verify_directory().map_err(runtime_error)?;
    if !locks
        .iter()
        .all(|lock| lock.matches_table_path(lock.path()))
    {
        return Err(runtime_error("The input changed while regridding"));
    }
    let keep_lock = operation == "regridspw" && !ms.write_locks.is_empty();
    let original_main = ms
        .write_locks
        .get(&ms.path)
        .or_else(|| locks.iter().find(|lock| lock.path() == ms.path))
        .ok_or_else(|| runtime_error("The original MeasurementSet has no write lock"))?;
    let mut published_locks = lock_table_tree_for_replacement(&staged)
        .map_err(runtime_error)?
        .into_iter()
        .map(|lock| {
            let relative = lock
                .path()
                .strip_prefix(&staged)
                .expect("lock under the stage");
            (ms.path.join(relative), lock)
        })
        .collect::<Vec<_>>();
    destination.publish(&staged, &ms.path, operation)?;
    match reopen_rewritten_ms(ms, &mut published_locks, keep_lock) {
        Ok(replacement) => Ok(RewriteOutcome::Complete(replacement)),
        Err(error) => {
            let rollback = if original_main.matches_table_path(&staged)
                && published_locks
                    .iter()
                    .all(|(path, lock)| lock.matches_table_path(path))
            {
                exchange_directories(&staged, &ms.path)
            } else {
                Err(std::io::Error::other(
                    "a locked directory changed at publication",
                ))
            };
            if let Err(rollback) = rollback {
                let backup = workspace.keep();
                return Err(runtime_error(format!(
                    "Could not reopen the regridded MS: {error}; rollback failed: {rollback}; original at {}",
                    backup.join("spectral.ms").display()
                )));
            }
            Err(error)
        }
    }
}

fn reopen_rewritten_ms(
    ms: &AttachedMs,
    published_locks: &mut Vec<(std::path::PathBuf, TableWriteLock)>,
    keep_lock: bool,
) -> PyResult<AttachedMs> {
    for (path, lock) in published_locks.iter_mut() {
        lock.follow_directory_rename(path).map_err(runtime_error)?;
    }
    let mut replacement = AttachedMs::open(ms.path.clone(), true, false, false)?;
    replacement.name = ms.name.clone();
    if keep_lock {
        replacement.write_locks.extend(published_locks.drain(..));
    }
    Ok(replacement)
}

fn spectral_history(main: &Table, staged: &Path, interpolation: &str) -> PyResult<String> {
    let path = subtable_path_from_main(main, staged, SubtableId::SpectralWindow)
        .ok_or_else(|| runtime_error("Missing SPECTRAL_WINDOW."))?;
    let table = Table::open(TableOptions::new(path)).map_err(runtime_error)?;
    let windows = MsSpectralWindow::new(&table);
    let mut history = String::new();
    for row in 0..table.row_count() {
        let code = windows.meas_freq_ref(row).map_err(runtime_error)?;
        let reference = FrequencyRef::from_casacore_code(code)
            .ok_or_else(|| runtime_error(format!("Invalid MEAS_FREQ_REF {code}")))?;
        let channels = windows.num_chan(row).map_err(runtime_error)?;
        let bandwidth = windows.total_bandwidth(row).map_err(runtime_error)?;
        writeln!(
            history,
            "Regridded spectral window {row}\n output frame = {reference}\n Number of channels = {channels}\n Total width of SPW (in output frame) = {bandwidth} Hz\n Interpolation Method = {interpolation}"
        )
        .expect("writing to a String cannot fail");
    }
    Ok(history)
}

fn regrid_to_stage(
    ms: &AttachedMs,
    rewrite: SpectralRewrite,
    staged: &Path,
) -> PyResult<RewriteOutcome<()>> {
    let operation = rewrite.method();
    let link = ms.working_metadata_link();
    let combine_spws =
        operation == "cvel" && link.spectral_window_count().map_err(runtime_error)? > 1;
    let request = MsTransformRequest {
        input_ms: ms.path.clone(),
        output_ms: staged.to_owned(),
        spw: "*".into(),
        width: 1,
        data_column: TransformDataColumn::All,
        selection: Default::default(),
        keep_flags: true,
    };
    let options = MsTransformTaskOptions {
        combine_spws,
        preserve_ids: !combine_spws,
        hanning: rewrite.hanning,
        regrid: Some(rewrite.options),
        legacy_tool: Some(LegacyMsTool::Regrid(rewrite.grid)),
        ..Default::default()
    };
    match mstransform_task(&request, &options) {
        Err(MsTransformError::LegacyRegridUnchanged) => return Ok(RewriteOutcome::Unchanged),
        Err(MsTransformError::Spectrum(_)) if operation == "regridspw" => {
            return Err(runtime_error("Regridding failed."));
        }
        Err(MsTransformError::RegridParameter { .. } | MsTransformError::Spectrum(_)) => {
            return Ok(RewriteOutcome::Refused);
        }
        result => result.map_err(runtime_error)?,
    };
    Ok(RewriteOutcome::Complete(()))
}
