// SPDX-License-Identifier: LGPL-3.0-or-later
//! The legacy MS spectral writers and their in-place publication.

use std::path::Path;

use casa_ms::tool_history::append_tool_history;
use casa_ms::transform::{
    LegacyMsTool, LegacySpectralGrid, MsRegridOptions, MsTransformError, MsTransformRequest,
    MsTransformTaskOptions, TransformDataColumn, mstransform_task,
};
use casa_tables::publication::{
    check_write_access, exchange_directories, lock_table_tree_for_replacement_with_held,
};
use casa_tables::{Table, TableOptions};
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
            LegacySpectralGrid::CenteredChannels { .. } => "regridspw",
        }
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
            let derived = (start + bandwidth / 2.).floor();
            if center > UNSET_CENTRE && derived != center {
                return Ok(false);
            }
            derived
        } else {
            center
        };
        if mode != "chan" {
            return Err(runtime_error(format!(
                "The legacy regridspw mode {mode:?} is not yet implemented (#1030)"
            )));
        }
        self.rewrite_spectra(
            py,
            SpectralRewrite {
                options: MsRegridOptions {
                    mode: "channel".into(),
                    nchan: -1,
                    start: String::new(),
                    width: String::new(),
                    nspw: 1,
                    interpolation: interpolation.to_ascii_lowercase(),
                    phasecenter: String::new(),
                    restfreq: if restfreq > 0. {
                        format!("{restfreq}Hz")
                    } else {
                        String::new()
                    },
                    outframe,
                    veltype: "radio".into(),
                    preaverage: false,
                },
                grid: LegacySpectralGrid::CenteredChannels {
                    centre,
                    bandwidth,
                    channel_width: chanwidth,
                },
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

impl PyMsTool {
    fn rewrite_spectra(&self, py: Python<'_>, rewrite: SpectralRewrite) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_mut().filter(|ms| ms.writable) else {
                return Ok(false);
            };
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
