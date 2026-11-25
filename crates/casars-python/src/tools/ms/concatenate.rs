// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_ms::tool_concatenate::{
    PhysicalConcatenation, TestConcatenation, VirtualConcatenation, concatenate, test_concatenate,
    virtual_concatenate,
};
use casa_tables::table_cache_name;
use casa_types::quanta::{
    Unit,
    casacore::{get_value, stream_double},
};
use pyo3::prelude::*;

use crate::convert::{Variant, quantity};

use super::selection_indices::selection_text;
use super::{PyMsTool, runtime_error, strict_bool};

#[pymethods]
impl PyMsTool {
    /// Reconcile the subtables of another MeasurementSet with this one.
    #[pyo3(signature = (msfile = String::new(), freqtol = None, dirtol = None, respectname = false), text_signature = "($self, msfile='', freqtol=[], dirtol=[], respectname=False)")]
    fn testconcatenate(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = donor_argument)] msfile: String,
        freqtol: Option<Variant>,
        dirtol: Option<Variant>,
        #[pyo3(from_py_with = strict_bool)] respectname: bool,
    ) -> PyResult<bool> {
        self.run_concatenation(
            py,
            ConcatArgs {
                msfile,
                freqtol,
                dirtol,
                respectname,
                mode: ConcatMode::Metadata,
            },
        )
    }

    /// Append another MeasurementSet or write its remapped rows to a separate table.
    #[pyo3(signature = (msfile = String::new(), freqtol = None, dirtol = None, weightscale = 1.0, handling = 0, destmsfile = String::new(), respectname = false), text_signature = "($self, msfile='', freqtol=[], dirtol=[], weightscale=1.0, handling=0, destmsfile='', respectname=False)")]
    #[allow(clippy::too_many_arguments)]
    fn concatenate(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = donor_argument)] msfile: String,
        freqtol: Option<Variant>,
        dirtol: Option<Variant>,
        weightscale: f32,
        handling: i32,
        #[pyo3(from_py_with = destination_argument)] destmsfile: String,
        #[pyo3(from_py_with = strict_bool)] respectname: bool,
    ) -> PyResult<bool> {
        self.run_concatenation(
            py,
            ConcatArgs {
                msfile,
                freqtol,
                dirtol,
                respectname,
                mode: ConcatMode::Physical(weightscale, handling, destmsfile),
            },
        )
    }

    /// Reconcile attached metadata and prepare another MS for virtual concatenation.
    #[pyo3(signature = (msfile = String::new(), auxfilename = String::new(), freqtol = None, dirtol = None, weightscale = 1.0, respectname = true), text_signature = "($self, msfile='', auxfilename='', freqtol=[], dirtol=[], weightscale=1.0, respectname=True)")]
    #[allow(clippy::too_many_arguments)]
    fn virtconcatenate(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = donor_argument)] msfile: String,
        #[pyo3(from_py_with = auxiliary_argument)] auxfilename: String,
        freqtol: Option<Variant>,
        dirtol: Option<Variant>,
        weightscale: f32,
        #[pyo3(from_py_with = strict_bool)] respectname: bool,
    ) -> PyResult<bool> {
        self.run_concatenation(
            py,
            ConcatArgs {
                msfile,
                freqtol,
                dirtol,
                respectname,
                mode: ConcatMode::Virtual(weightscale, auxfilename),
            },
        )
    }
}

struct ConcatArgs {
    msfile: String,
    freqtol: Option<Variant>,
    dirtol: Option<Variant>,
    respectname: bool,
    mode: ConcatMode,
}

enum ConcatMode {
    Metadata,
    Physical(f32, i32, String),
    Virtual(f32, String),
}

impl PyMsTool {
    fn run_concatenation(&self, py: Python<'_>, args: ConcatArgs) -> PyResult<bool> {
        let ConcatArgs {
            msfile,
            freqtol,
            dirtol,
            respectname,
            mode,
        } = args;
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_mut() else {
                return Ok(true);
            };
            if !ms.writable {
                return Err(runtime_error("the attached MeasurementSet is not writable"));
            }
            if matches!(mode, ConcatMode::Physical(..)) && ms.selected_rows.is_some() {
                return Err(runtime_error(
                    "cannot concatenate a selected MeasurementSet view",
                ));
            }
            let metadata = ms.working_metadata_link();
            metadata.verify_directory().map_err(runtime_error)?;
            let donor = table_cache_name(Path::new(&msfile)).map_err(runtime_error)?;
            let (frequency_hz, frequency_text) = tolerance(freqtol.as_ref(), "Hz", 1.0)?;
            let mas = std::f64::consts::PI / 648_000_000.0;
            let (direction_rad, direction_text) = tolerance(dirtol.as_ref(), "rad", mas)?;
            let request = TestConcatenation {
                frequency_hz,
                direction_rad,
                respect_name: respectname,
                frequency_text: &frequency_text,
                direction_text: &direction_text,
                donor_name: &msfile,
                attached_name: &ms.name,
            };
            let result = match &mode {
                ConcatMode::Metadata => {
                    test_concatenate(&ms.path, &donor, &request, &mut ms.write_locks)
                }
                ConcatMode::Physical(weight_scale, handling, dest) => concatenate(
                    &ms.path,
                    &donor,
                    &PhysicalConcatenation {
                        metadata: request,
                        weight_scale: *weight_scale,
                        handling: *handling,
                        output: (!dest.is_empty()).then_some(Path::new(dest)),
                    },
                    &mut ms.write_locks,
                ),
                ConcatMode::Virtual(weight_scale, auxiliary) => virtual_concatenate(
                    &ms.path,
                    &donor,
                    &VirtualConcatenation {
                        metadata: request,
                        weight_scale: *weight_scale,
                        scan_file: (!auxiliary.is_empty()).then_some(Path::new(auxiliary)),
                        attached_rows: ms.selected_rows.as_deref(),
                    },
                    &mut ms.write_locks,
                ),
            };
            result.map_err(runtime_error)?;
            ms.subtables = Default::default();
            metadata.invalidate_view().map_err(runtime_error)?;
            if let Some(view) = &ms.exact_time_metadata {
                view.invalidate_view().map_err(runtime_error)?;
            }
            Ok(true)
        })
    }
}

fn donor_argument(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "msfile")
}

fn destination_argument(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "destmsfile")
}

fn auxiliary_argument(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "auxfilename")
}

fn tolerance(value: Option<&Variant>, unit: &str, default: f64) -> PyResult<(f64, String)> {
    let Some(value) = value.filter(|value| !value.is_empty_sequence()) else {
        return Ok((0.0, "0 ".to_owned()));
    };
    if matches!(value, Variant::Text(text) if text.is_empty()) {
        return Ok((default, "0 ".to_owned()));
    }
    let quantity = quantity(value).map_err(runtime_error)?;
    let rendered = format!(
        "{} {}",
        stream_double(quantity.value()),
        quantity.unit().name()
    );
    let scalar = get_value(&quantity, &Unit::new(unit).map_err(runtime_error)?);
    if !scalar.is_finite() {
        return Err(runtime_error("non-finite concatenation tolerance"));
    }
    Ok((scalar, rendered))
}
