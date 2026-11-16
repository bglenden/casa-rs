// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's Stokes-spectrum arguments and validation.

use casa_components::{SpectralIndex, SpectralKind, TabularSpectrum};
use casa_types::measures::frequency::FrequencyRef;
use pyo3::exceptions::{PyOverflowError, PySystemError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyInt;

use super::{PyComponentList, ops::frequency_frame, setflux, setspectrum, supplied};
use crate::convert::value::CasaValue;
use crate::convert::{ConvertError, ConvertResult, Variant, casacore_unit, quantity};

pub(super) struct Request {
    name: String,
    index: setspectrum::VectorArg,
    vectors: [setspectrum::VectorArg; 5],
    reference: Variant,
    frame: String,
}

pub(super) struct Plan {
    pub(super) kind: SpectralKind,
    pub(super) reference: Option<(f64, FrequencyRef)>,
    pub(super) refused: bool,
}

fn frame(name: &str) -> ConvertResult<FrequencyRef> {
    frequency_frame(name).ok_or_else(|| {
        ConvertError::runtime(format!(
            "{name} is not a frequency frame that is understood"
        ))
    })
}

/// Construct all spectral values before mutating the selected component.
pub(super) fn prepare(request: &Request) -> ConvertResult<Option<Plan>> {
    let name = request.name.to_ascii_uppercase();
    if name.contains("TABU") {
        return tabular(request).map(Some);
    }
    let spectral = name.contains("SPECTRAL");
    if !spectral && name != "CONSTANT" && name != "POWER LOGARITHMIC POLYNOMIAL" {
        return Ok(None);
    }
    let frame = frame(&request.frame)?;
    let hz = if request.reference.is_empty_sequence()
        || matches!(&request.reference, Variant::Numbers(values) if values.is_empty())
    {
        1.4e9
    } else {
        let value = quantity(&request.reference)?;
        let hz = casacore_unit("Hz").map_err(ConvertError::runtime)?;
        if !value.unit().conformant(&hz) || !value.get_si_value().is_finite() {
            return Err(ConvertError::runtime("invalid frequency refused (#1053)"));
        }
        value.get_si_value()
    };
    if request.index.invalid || request.index.values.is_empty() {
        return Err(ConvertError::runtime(
            "invalid or empty spectrum index refused (#1053)",
        ));
    }
    if name == "POWER LOGARITHMIC POLYNOMIAL" {
        // CASA returns false after changing the reference frequency.
        return Ok(Some(Plan {
            kind: SpectralKind::Constant,
            reference: None,
            refused: true,
        }));
    }
    let kind = if spectral {
        let indices = &request.index.values;
        if indices.iter().any(|value| !value.is_finite()) {
            return Err(ConvertError::runtime(
                "non-finite spectrum index refused (#1053)",
            ));
        }
        let index = SpectralIndex::new(indices[0]);
        let index = if let [i, q, u, v] = indices.as_slice() {
            index.with_stokes_indices([*i, *q, *u, *v])
        } else {
            index
        };
        SpectralKind::SpectralIndex(index)
    } else {
        SpectralKind::Constant
    };
    Ok(Some(Plan {
        kind,
        reference: Some((hz, frame)),
        refused: false,
    }))
}

fn tabular(request: &Request) -> ConvertResult<Plan> {
    let [freq, i, q, u, v] = &request.vectors;
    if freq.values.len() < 2 {
        return Err(ConvertError::runtime(
            "There need to be at least 2 points in a tabular spectrum to interpolate in between",
        ));
    }
    for (name, values) in [("I", i), ("Q", q), ("U", u), ("V", v)] {
        if freq.values.len() != values.values.len() {
            return Err(ConvertError::runtime(format!(
                "lengths of tabular frequencies and {name} values have to be the same"
            )));
        }
    }
    let frame = frame(&request.frame)?;
    if request.vectors.iter().any(|v| v.invalid) {
        return Err(ConvertError::runtime(
            "unrepresentable tabular value refused (#1053)",
        ));
    }
    if request
        .vectors
        .iter()
        .flat_map(|v| &v.values)
        .any(|value| !value.is_finite())
    {
        return Err(ConvertError::runtime(
            "non-finite tabular spectrum refused (#1053)",
        ));
    }
    let values = (0..freq.values.len())
        .map(|n| [i.values[n], q.values[n], u.values[n], v.values[n]])
        .collect();
    let table = TabularSpectrum::new(frame, freq.values.clone(), values)
        .map_err(|e| ConvertError::runtime(e.to_string()))?;
    Ok(Plan {
        kind: SpectralKind::Tabular(table),
        reference: None,
        refused: false,
    })
}

#[pymethods]
impl PyComponentList {
    /// Set one component's Stokes-dependent spectrum.
    #[pyo3(signature = (which, r#type = None, index = None, tabularfreqs = None, tabulari = None, tabularq = None, tabularu = None, tabularv = None, reffreq = None, frame = None), text_signature = "($self, which, type='spectral index', index=[0.0], tabularfreqs=[100000000000.0], tabulari=[1.0], tabularq=[0.0], tabularu=[0.0], tabularv=[0.0], reffreq=[], frame='LSRK')")]
    #[allow(clippy::too_many_arguments)]
    fn setstokesspectrum(
        &self,
        py: Python<'_>,
        which: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = supplied)] r#type: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] index: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] tabularfreqs: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] tabulari: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] tabularq: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] tabularu: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] tabularv: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] reffreq: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] frame: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (which, index_pending) = if !which.is_instance_of::<PyInt>()
            && which.is_instance(&py.import("numpy")?.getattr("bool_")?)?
        {
            (-1, true)
        } else {
            match which.extract::<i64>() {
                Ok(index) => (i64::from(index as i32), false),
                Err(error) if which.is_none() || error.is_instance_of::<PyOverflowError>(py) => {
                    (-1, true)
                }
                Err(_) => return Err(PyTypeError::new_err("argument _which must be an integer")),
            }
        };
        let name = setspectrum::text(r#type, "spectral index", "type")?;
        let (index, mut pending) = setspectrum::vector(index, 0.)?;
        let mut vectors = Vec::new();
        for (value, default) in [
            (tabularfreqs, 1e11),
            (tabulari, 1.),
            (tabularq, 0.),
            (tabularu, 0.),
            (tabularv, 0.),
        ] {
            let (value, item_pending) = setspectrum::vector(value, default)?;
            pending |= item_pending;
            vectors.push(value);
        }
        let (reference, reference_pending) = setflux::argument(reffreq)?;
        pending |= reference_pending || index_pending;
        let frame = setspectrum::text(frame, "LSRK", "frame")?;
        let request = Request {
            name,
            index,
            vectors: vectors
                .try_into()
                .map_err(|_| PyTypeError::new_err("invalid tabular vectors"))?,
            reference,
            frame,
        };
        let result = self.call(py, move |state, lines| {
            state
                .setstokesspectrum(which, &request, pending, lines)
                .map(CasaValue::Bool)
        })?;
        if pending {
            return Err(PySystemError::new_err(
                "<built-in function componentlist_setstokesspectrum> returned a result with an exception set",
            ));
        }
        Ok(result)
    }
}
