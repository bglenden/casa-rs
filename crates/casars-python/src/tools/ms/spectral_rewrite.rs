// SPDX-License-Identifier: LGPL-3.0-or-later
//! The legacy MS spectral writers and their in-place publication.

use std::path::Path;

use casa_ms::spectral_selection::LegacyRegridQuantity;
use casa_ms::subtables::MsSpectralWindow;
use casa_ms::tool_history::append_tool_history;
use casa_ms::transform::{
    LegacyMsTool, LegacySpectralGrid, MsRegridOptions, MsTransformError, MsTransformRequest,
    MsTransformTaskOptions, TransformDataColumn, mstransform_task,
};
use casa_tables::publication::{
    check_write_access, exchange_directories, lock_table_tree_for_replacement_with_held,
};
use casa_tables::{Table, TableOptions};
use casa_types::measures::frequency::FrequencyRef;
use pyo3::prelude::*;

use super::sort::SortedDestination;
use super::spectral_grid::axis_argument;
use super::{AttachedMs, PyMsTool, runtime_error, strict_bool};
use crate::convert::Variant;

const UNSET_CENTRE: f64 = -1e30;

struct SpectralRewrite {
    options: MsRegridOptions,
    grid: LegacySpectralGrid,
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
        if !matches!(self.grid, LegacySpectralGrid::FrameOnly)
            && !self.options.interpolation.is_empty()
        {
            return Ok(false);
        }
        let link = ms.working_metadata_link();
        link.verify_directory().map_err(runtime_error)?;
        if self.options.interpolation.is_empty() {
            // SubMS declines an unknown regridspw interpolation without editing.
            return Ok(self.method() == "regridspw");
        }
        if self.options.outframe.is_empty() {
            return Ok(true);
        }
        let Ok(reference) = self.options.outframe.parse::<FrequencyRef>() else {
            return Ok(false);
        };
        if reference == FrequencyRef::GEO {
            return Ok(false);
        }
        let tables = ms
            .subtables
            .spectral_grid_tables(&ms.main)
            .map_err(runtime_error)?;
        for table in tables.windows.iter() {
            let window = MsSpectralWindow::new(table);
            for row in 0..table.row_count() {
                if window.meas_freq_ref(row).map_err(runtime_error)? != reference.casacore_code() {
                    return Ok(false);
                }
            }
        }
        link.verify_directory().map_err(runtime_error)?;
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
            regridspw_interpolation(&interpolation, &grid)
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
                    interpolation: interp.to_ascii_lowercase(),
                    phasecenter: axis_argument("", phasec),
                    restfreq: axis_argument("", restfreq),
                    outframe,
                    veltype,
                    preaverage: false,
                },
                grid: LegacySpectralGrid::Cvel,
                hanning,
            },
        )
    }
}

fn regridspw_interpolation(interpolation: &str, grid: &LegacySpectralGrid) -> &'static str {
    let interpolation = interpolation.to_ascii_lowercase();
    if interpolation.contains("nearest") {
        "nearest"
    } else if interpolation.contains("splin") {
        "spline"
    } else if interpolation.contains("cub") {
        "cubic"
    } else if interpolation.contains("fft") {
        if matches!(
            grid,
            LegacySpectralGrid::CenteredNumeric {
                quantity: LegacyRegridQuantity::OpticalVelocity | LegacyRegridQuantity::Wavelength,
                ..
            }
        ) {
            ""
        } else {
            "fftshift"
        }
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

    fn rewrite_spectra(&self, py: Python<'_>, rewrite: SpectralRewrite) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_mut().filter(|ms| ms.writable) else {
                return Ok(false);
            };
            if rewrite.unchanged(ms)? {
                return Ok(true);
            }
            if ms.selected_rows.is_some() {
                return if rewrite.method() == "cvel" {
                    Err(runtime_error(
                        "In-place cvel requires the complete attached MeasurementSet",
                    ))
                } else {
                    Ok(false)
                };
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
    let combine_spws =
        operation == "cvel" && link.spectral_window_count().map_err(runtime_error)? > 1;
    let request = MsTransformRequest {
        input_ms: ms.path.clone(),
        output_ms: staged.clone(),
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
            return Ok(None);
        }
        result => result.map_err(runtime_error)?,
    };
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
