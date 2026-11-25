// SPDX-License-Identifier: LGPL-3.0-or-later
//! The legacy MS spectral writers and their in-place publication.

use std::path::Path;

use casa_ms::selection::MsSelectionIoBudget;
use casa_ms::spectral_selection::LegacyRegridQuantity;
use casa_ms::subtables::MsSpectralWindow;
use casa_ms::tool_history::append_tool_history;
use casa_ms::transform::{
    HanningDataColumn, LegacyMsTool, LegacySpectralGrid, MsRegridOptions, MsTransformError,
    MsTransformRequest, MsTransformTaskOptions, ToolSpectralWindow, TransformDataColumn,
    mstransform_task, reframe_staged_ms, tool_channel_frequencies,
};
use casa_tables::publication::{
    check_write_access, copy_locked_table_tree, exchange_directories,
    lock_table_tree_for_replacement_with_held,
};
use casa_tables::{Table, TableOptions};
use casa_types::measures::MeasFrame;
use casa_types::measures::frequency::FrequencyRef;
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

impl SpectralRewrite {
    fn method(&self) -> &'static str {
        match self.grid {
            LegacySpectralGrid::Cvel => "cvel",
            _ => "regridspw",
        }
    }

    fn unchanged(&self, ms: &AttachedMs) -> PyResult<bool> {
        let cvel = matches!(self.grid, LegacySpectralGrid::Cvel);
        if !cvel
            && !matches!(self.grid, LegacySpectralGrid::FrameOnly)
            && !self.options.interpolation.is_empty()
        {
            return Ok(false);
        }
        let link = ms.working_metadata_link();
        link.verify_directory().map_err(runtime_error)?;
        if self.options.interpolation.is_empty() {
            // SubMS declines an unknown interpolation without editing the grid.
            return Ok(true);
        }
        let target = if self.options.outframe.is_empty() {
            if !cvel {
                return Ok(true);
            }
            None
        } else {
            let Ok(target) = self.options.outframe.parse::<FrequencyRef>() else {
                return Ok(cvel && !self.options.outframe.eq_ignore_ascii_case("SOURCE"));
            };
            if matches!(target, FrequencyRef::REST | FrequencyRef::Undefined) {
                return Ok(cvel);
            }
            if target == FrequencyRef::GEO {
                return Ok(false);
            }
            Some(target)
        };

        let tables = ms
            .subtables
            .spectral_grid_tables(&ms.main)
            .map_err(runtime_error)?;
        if cvel && tables.windows.iter().map(Table::row_count).sum::<usize>() != 1 {
            return Ok(false);
        }
        for table in tables.windows.iter() {
            let window = MsSpectralWindow::new(table);
            for row in 0..table.row_count() {
                let code = window.meas_freq_ref(row).map_err(runtime_error)?;
                let reference = FrequencyRef::from_casacore_code(code)
                    .ok_or_else(|| runtime_error(format!("Invalid MEAS_FREQ_REF {code}")))?;
                if target.is_some_and(|target| target != reference) {
                    return Ok(false);
                }
                if cvel && !self.same_channels(&window, row, reference)? {
                    return Ok(false);
                }
            }
        }
        link.verify_directory().map_err(runtime_error)?;
        Ok(true)
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
        if !matches!(
            mode.as_str(),
            "channel" | "channel_b" | "frequency" | "velocity"
        ) {
            return Ok(false);
        }
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
                    start: axis_argument(&mode, start),
                    width: axis_argument(&mode, width),
                    mode,
                    nchan,
                    nspw: 1,
                    interpolation: interpolation.into(),
                    phasecenter: String::new(),
                    restfreq: axis_argument("", restfreq),
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
            let Some(ms) = attached.as_mut().filter(|ms| ms.writable) else {
                return Ok(false);
            };
            let cvel = rewrite.method() == "cvel";
            if cvel && ms.selected_rows.is_some() {
                return Err(runtime_error(
                    "In-place cvel requires the complete attached MeasurementSet",
                ));
            }
            if !rewrite.prepare_phase(ms)? {
                return Ok(false);
            }
            if rewrite.unchanged(ms)? {
                if cvel && rewrite.hanning {
                    return ms.hanning_rows(HanningDataColumn::Both);
                }
                return Ok(true);
            }
            if ms.selected_rows.is_some() {
                return Ok(false);
            }
            let read_locks = std::mem::take(&mut ms.read_locks).into_keys().collect();
            match rewrite_attached(ms, rewrite) {
                Ok(Some(replacement)) => {
                    *attached = Some(replacement);
                    Ok(true)
                }
                Ok(None) => {
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

fn rewrite_attached(ms: &AttachedMs, rewrite: SpectralRewrite) -> PyResult<Option<AttachedMs>> {
    let operation = rewrite.method();
    let link = ms.working_metadata_link();
    link.verify_directory().map_err(runtime_error)?;
    check_write_access(&ms.path).map_err(runtime_error)?;
    let destination = SortedDestination::inspect(&ms.path, operation)?;
    let locks = lock_table_tree_for_replacement_with_held(&ms.path, &ms.write_locks)
        .map_err(runtime_error)?;
    let workspace = tempfile::tempdir_in(ms.path.parent().unwrap_or_else(|| Path::new(".")))
        .map_err(runtime_error)?;
    let staged = workspace.path().join("spectral.ms");
    if matches!(rewrite.grid, LegacySpectralGrid::FrameOnly) {
        copy_locked_table_tree(&ms.path, &staged).map_err(runtime_error)?;
        match reframe_staged_ms(&staged, &rewrite.options) {
            Ok(true) => {}
            Ok(false) | Err(MsTransformError::RegridParameter { .. }) => return Ok(None),
            Err(error) => return Err(runtime_error(error)),
        }
    } else if !regrid_to_stage(ms, rewrite, &staged)? {
        return Ok(None);
    }
    let table = Table::open(TableOptions::new(&staged)).map_err(runtime_error)?;
    append_tool_history(
        &table,
        true,
        &[format!("Transformed/regridded with {operation}")],
        "",
        &format!("ms::{operation}()"),
        "ms",
        &mut Default::default(),
    )
    .map_err(runtime_error)?;
    drop(table);
    link.verify_directory().map_err(runtime_error)?;
    if !locks
        .iter()
        .all(|lock| lock.matches_table_path(lock.path()))
    {
        return Err(runtime_error("The input changed while regridding"));
    }
    destination.publish(&staged, &ms.path, operation)?;
    match AttachedMs::open(ms.path.clone(), true, false, false) {
        Ok(mut replacement) => {
            replacement.name = ms.name.clone();
            Ok(Some(replacement))
        }
        Err(error) => {
            if let Err(rollback) = exchange_directories(&staged, &ms.path) {
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

fn regrid_to_stage(ms: &AttachedMs, rewrite: SpectralRewrite, staged: &Path) -> PyResult<bool> {
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
        Err(MsTransformError::Spectrum(_)) if operation == "regridspw" => {
            return Err(runtime_error("Regridding failed."));
        }
        Err(MsTransformError::RegridParameter { .. } | MsTransformError::Spectrum(_)) => {
            return Ok(false);
        }
        result => result.map_err(runtime_error)?,
    };
    Ok(true)
}
