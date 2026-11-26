// SPDX-License-Identifier: LGPL-3.0-or-later
//! UVFITS and FITS-IDI visibility exchange through the Python MS tool.

use std::path::{Path, PathBuf};

use casa_ms::MsSelection;
use casa_ms::fits_visibility::{
    UvFitsExportOptions, UvFitsImportOptions, export_uvfits, import_uvfits,
};
use casa_mstransform::fits_idi::{FitsIdiOptions, import_fits_idi};
use pyo3::prelude::*;

use super::partition::{all_if_minus_one, optional_expression};
use super::{AttachedMs, PyMsTool, runtime_error, strict_bool, table_cache_name};

#[pymethods]
impl PyMsTool {
    /// Import a UVFITS data set and attach the created MeasurementSet.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (msfile = "", fitsfile = "", nomodify = true, lock = false, obstype = 0, host = "", forcenewserver = false, antnamescheme = "old"), text_signature = "($self, msfile='', fitsfile='', nomodify=True, lock=False, obstype=0, host='', forcenewserver=False, antnamescheme='old')")]
    fn fromfits(
        &self,
        py: Python<'_>,
        msfile: &str,
        fitsfile: &str,
        #[pyo3(from_py_with = strict_bool)] nomodify: bool,
        #[pyo3(from_py_with = strict_bool)] lock: bool,
        obstype: i64,
        host: &str,
        #[pyo3(from_py_with = strict_bool)] forcenewserver: bool,
        antnamescheme: &str,
    ) -> PyResult<bool> {
        let _ = (host, forcenewserver);
        let options = UvFitsImportOptions {
            observation_type: obstype as i32,
            new_antenna_names: antnamescheme.eq_ignore_ascii_case("new"),
        };
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let output = Path::new(msfile);
            import_uvfits(Path::new(fitsfile), output, options).map_err(runtime_error)?;
            let output = table_cache_name(output).map_err(runtime_error)?;
            *attached = Some(AttachedMs::open(output, !nomodify, lock, false)?);
            Ok(true)
        })
    }

    /// Import a FITS-IDI data set and attach the created MeasurementSet.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (msfile = "", fitsfile = "", nomodify = true, lock = false, obstype = 0), text_signature = "($self, msfile='', fitsfile='', nomodify=True, lock=False, obstype=0)")]
    fn fromfitsidi(
        &self,
        py: Python<'_>,
        msfile: &str,
        fitsfile: &str,
        #[pyo3(from_py_with = strict_bool)] nomodify: bool,
        #[pyo3(from_py_with = strict_bool)] lock: bool,
        obstype: i64,
    ) -> PyResult<bool> {
        let options = FitsIdiOptions {
            observation_type: obstype as i32,
            overwrite: !nomodify,
        };
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let output = Path::new(msfile);
            import_fits_idi(Path::new(fitsfile), output, options).map_err(runtime_error)?;
            *attached = Some(AttachedMs::open(
                table_cache_name(output).map_err(runtime_error)?,
                !nomodify,
                lock,
                false,
            )?);
            Ok(true)
        })
    }

    /// Export the current MeasurementSet view as a UVFITS file.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (fitsfile = "", column = "corrected", field = None, spw = None, baseline = None, time = "", scan = None, uvrange = None, taql = "", writesyscal = false, multisource = false, combinespw = false, writestation = false, padwithflags = false, overwrite = false), text_signature = "($self, fitsfile='', column='corrected', field=[], spw=[], baseline=[], time='', scan=[], uvrange=[], taql='', writesyscal=False, multisource=False, combinespw=False, writestation=False, padwithflags=False, overwrite=False)")]
    fn tofits(
        &self,
        py: Python<'_>,
        fitsfile: &str,
        column: &str,
        field: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        baseline: Option<&Bound<'_, PyAny>>,
        time: &str,
        scan: Option<&Bound<'_, PyAny>>,
        uvrange: Option<&Bound<'_, PyAny>>,
        taql: &str,
        #[pyo3(from_py_with = strict_bool)] writesyscal: bool,
        #[pyo3(from_py_with = strict_bool)] multisource: bool,
        #[pyo3(from_py_with = strict_bool)] combinespw: bool,
        #[pyo3(from_py_with = strict_bool)] writestation: bool,
        #[pyo3(from_py_with = strict_bool)] padwithflags: bool,
        #[pyo3(from_py_with = strict_bool)] overwrite: bool,
    ) -> PyResult<bool> {
        let options = UvFitsExportOptions {
            column,
            selection: MsSelection {
                field: all_if_minus_one(field)?,
                spw: all_if_minus_one(spw)?,
                antenna: optional_expression(baseline)?,
                timerange: (!time.is_empty()).then(|| time.to_owned()),
                scan: optional_expression(scan)?,
                uvrange: optional_expression(uvrange)?,
                msselect: (!taql.is_empty()).then(|| taql.to_owned()),
                ..MsSelection::default()
            },
            writesyscal,
            multisource,
            combinespw,
            writestation,
            padwithflags,
            overwrite,
        };
        py.detach(|| {
            let attached = self.attached.lock().map_err(runtime_error)?;
            let Some(attached) = attached.as_ref() else {
                return Ok(true);
            };
            let output = if fitsfile.is_empty() {
                default_fits_path(&attached.path)
            } else {
                PathBuf::from(fitsfile)
            };
            let snapshot = attached.selection_snapshot()?;
            export_uvfits(&snapshot.ms, &output, &options).map_err(runtime_error)?;
            Ok(true)
        })
    }
}

fn default_fits_path(source: &Path) -> PathBuf {
    if source
        .extension()
        .is_some_and(|extension| extension == "ms")
    {
        source.with_extension("fits")
    } else {
        let mut path = source.as_os_str().to_os_string();
        path.push(".fits");
        path.into()
    }
}
