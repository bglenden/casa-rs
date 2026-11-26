// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA spectral-velocity constructors and direct algebraic conversions.
use super::{PyMeasures, record::Measures};
use crate::convert::holder::{
    C_MPS, DOPPLER_CODES, FREQUENCY_CODES, Measure, MeasureKind, MeasureValue,
    RADIAL_VELOCITY_CODES, doppler_ref,
};
use crate::convert::value::CasaValue;
use crate::convert::{RecordArg, Variant, quantity};
use crate::tools::LogLine;
use casa_core::measures::MeasFrame;
use casa_core::measures::doppler::{DopplerRef, MDoppler};
use casa_core::quanta::Quantity;
use pyo3::prelude::*;

fn failed(lines: &mut Vec<LogLine>, message: impl Into<String>) -> CasaValue {
    lines.push(LogLine::error(message));
    CasaValue::empty_record()
}
fn read(record: &RecordArg, lines: &mut Vec<LogLine>, context: &str) -> Option<Measures> {
    match Measures::read(record) {
        Ok(values) => Some(values),
        Err(error) => {
            failed(lines, format!("{}{context}", error.message()));
            None
        }
    }
}
fn define(
    kind: MeasureKind,
    rf: &str,
    value: &Variant,
    off: &RecordArg,
    lines: &mut Vec<LogLine>,
) -> CasaValue {
    let Some(mut value) = super::define::quantity_of(value, lines) else {
        return CasaValue::empty_record();
    };
    if value.value() == 0.0 && value.unit().name().is_empty() {
        value = Quantity::new(0.0, "m/s").expect("m/s");
    }
    let refer = kind.codes().resolve_or_default(rf);
    if kind.codes().resolve(rf).is_none() {
        lines.push(LogLine::warning("Illegal reference frame string."));
    }
    let base = match Measure::from_values(kind, refer, &[value]) {
        Ok(value) => value,
        Err(error) => return failed(lines, format!("Exception Reports: {}", error.message())),
    };
    let mut result = Measures::scalar(base);
    if !off.0.is_empty() {
        match Measures::read(off) {
            Ok(offset) if offset.values[0].kind() == kind => {
                result.offset = Some(Box::new(offset.into_reference_origin()))
            }
            Ok(_) if kind == MeasureKind::Doppler => {
                return failed(lines, "Illegal offset type specified, not used\n");
            }
            Ok(_) => lines.push(LogLine::warning(
                "Illegal offset type specified, not used\n",
            )),
            Err(error) => lines.push(LogLine::warning(format!(
                "{}Non-measure type offset in measure conversion\n",
                error.message()
            ))),
        }
    }
    result.to_value()
}
// todoppler reads rfq with casaMFrequency first, including period, wavelength,
// angular-rate and energy quantities; a parsed non-frequency quantity is only
// ignored when the input is already a radial velocity.
fn doppler_rest_hz(value: &Variant) -> Result<Option<f64>, String> {
    if let Ok((Some(frequency), _)) = crate::convert::measures::frequency(value) {
        return Ok(Some(frequency.hz()));
    }
    if let Variant::Record(record) = value
        && let Ok(values) = Measures::read(&RecordArg((**record).clone()))
        && let MeasureValue::Frequency(hz) = values.values[0].value
    {
        return Ok(Some(hz));
    }
    let q = quantity(value).map_err(|error| error.to_string())?;
    let hz = casa_core::quanta::Unit::new("Hz").expect("Hz");
    Ok(q.get_value_in(&hz).ok())
}

// tofrequency instead reads a QuantumHolder, requiring frequency dimensions
// even when rfq is a frequency measure with a frequency-equivalent m0 unit.
fn quantity_rest_hz(value: &RecordArg) -> Option<f64> {
    let outer = Variant::Record(value.0.clone().into());
    let q = quantity(&outer).ok().or_else(|| {
        let measure = Measures::read(value).ok()?;
        if measure.values[0].kind() != MeasureKind::Frequency {
            return None;
        }
        quantity(value.0.get("m0")?).ok()
    })?;
    let hz = casa_core::quanta::Unit::new("Hz").ok()?;
    if q.unit().val().dim != hz.val().dim {
        return None;
    }
    crate::convert::holder::frequency_hz(&q)
}
fn doppler(value: Measure) -> Result<MDoppler, String> {
    let MeasureValue::Doppler(v) = value.value else {
        return Err("Illegal Doppler specified".into());
    };
    Ok(MDoppler::new(v, doppler_ref(value.refer)?))
}
fn converted(result: Result<Measures, String>, lines: &mut Vec<LogLine>) -> CasaValue {
    match result {
        Ok(values) => values.to_value(),
        Err(error) => failed(lines, error),
    }
}

#[pymethods]
impl PyMeasures {
    /// Define a Doppler measure in a CASA convention, retaining its reference offset.
    #[pyo3(signature=(rf=None,v0=Variant::Empty,off=None),text_signature="($self, rf='RADIO', v0=[], off={})")]
    fn doppler(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with=super::arguments::supplied)] rf: Option<Bound<'_, PyAny>>,
        v0: Variant,
        #[pyo3(from_py_with=super::arguments::supplied)] off: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let rf = super::arguments::reference(rf.as_ref(), "RADIO")?;
        let off = super::arguments::record(off.as_ref(), "off")?;
        self.call(py, |_, lines| {
            Ok(define(MeasureKind::Doppler, &rf, &v0, &off, lines))
        })
    }
    /// Define a radial-velocity measure in metres per second.
    #[pyo3(signature=(rf=None,v0=Variant::Empty,off=None),text_signature="($self, rf='LSRK', v0=[], off={})")]
    fn radialvelocity(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with=super::arguments::supplied)] rf: Option<Bound<'_, PyAny>>,
        v0: Variant,
        #[pyo3(from_py_with=super::arguments::supplied)] off: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let rf = super::arguments::reference(rf.as_ref(), "LSRK")?;
        let off = super::arguments::record(off.as_ref(), "off")?;
        self.call(py, |_, lines| {
            Ok(define(MeasureKind::RadialVelocity, &rf, &v0, &off, lines))
        })
    }
    /// Convert a frequency or radial velocity to a Doppler convention.
    #[pyo3(signature=(rf,v0,rfq=Variant::Empty),text_signature="($self, rf, v0, rfq=[])")]
    fn todoppler(
        &self,
        py: Python<'_>,
        rf: &Bound<'_, PyAny>,
        v0: &Bound<'_, PyAny>,
        rfq: Variant,
    ) -> PyResult<Py<PyAny>> {
        let rf = super::arguments::reference(Some(rf), "")?;
        let v0 = super::arguments::record(Some(v0), "v0")?;
        self.call(py,|_,lines| {
            let Some(values)=read(&v0,lines,"Non-measure type radvel/freq in measure conversion\n") else {return Ok(CasaValue::empty_record())};
            let target=DOPPLER_CODES.resolve_or_default(&rf);
            let rest=match doppler_rest_hz(&rfq) { Ok(rest)=>rest, Err(error)=>return Ok(failed(lines,error)) };
            let result=values.map(|v| {
                // CASA's direct helpers use getValue(); input reference offsets
                // are not materialized here as they are in me.measure().
                let dop=match v.value {
                    MeasureValue::RadialVelocity(ms)=>MDoppler::new(ms/C_MPS,DopplerRef::BETA),
                    MeasureValue::Frequency(hz)=> {
                        let ratio = hz/rest.ok_or("Illegal Doppler or rest frequency specified.  todoppler can only convert MFrequency or MRadialVelocity")?;
                        let squared = ratio * ratio;
                        MDoppler::new((1.0 - squared)/(1.0 + squared), DopplerRef::BETA)
                    },
                    _=>return Err("Illegal Doppler or rest frequency specified.  todoppler can only convert MFrequency or MRadialVelocity".into()),
                };
                let dop=dop.convert_to(doppler_ref(target)?,&MeasFrame::new()).map_err(|e|e.to_string())?;
                Ok(Measure { refer:target,value:MeasureValue::Doppler(dop.value()) })
            });
            Ok(converted(result,lines))
        })
    }
    /// Convert a Doppler measure to a frequency using the supplied rest frequency.
    #[pyo3(signature=(rf,v0,rfq),text_signature="($self, rf, v0, rfq)")]
    fn tofrequency(
        &self,
        py: Python<'_>,
        rf: &Bound<'_, PyAny>,
        v0: &Bound<'_, PyAny>,
        rfq: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let rf = super::arguments::reference(Some(rf), "")?;
        let v0 = super::arguments::record(Some(v0), "v0")?;
        let rfq = super::arguments::record(Some(rfq), "rfq")?;
        self.call(py, |state, lines| {
            let Some(rest) = quantity_rest_hz(&rfq) else {
                return Ok(failed(lines, "Illegal Doppler or rest frequency specified"));
            };
            let Some(values) = read(&v0, lines, "Non-measure type v0 in measure conversion\n")
            else {
                return Ok(CasaValue::empty_record());
            };
            let values = match values.resolve(state, values.values[0].refer)? {
                Ok(values) => values,
                Err(error) => return Ok(failed(lines, error)),
            };
            let refer = FREQUENCY_CODES.resolve(&rf).unwrap_or("REST");
            Ok(converted(
                values.map(|v| {
                    Ok(Measure {
                        refer,
                        value: MeasureValue::Frequency(
                            doppler(v)?
                                .convert_to(DopplerRef::BETA, &MeasFrame::new())
                                .map_err(|e| e.to_string())?
                                .shift_frequency_hz(rest),
                        ),
                    })
                }),
                lines,
            ))
        })
    }
    /// Convert a Doppler convention to relativistic radial velocity.
    #[pyo3(signature=(rf,v0),text_signature="($self, rf, v0)")]
    fn toradialvelocity(
        &self,
        py: Python<'_>,
        rf: &Bound<'_, PyAny>,
        v0: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let rf = super::arguments::reference(Some(rf), "")?;
        let v0 = super::arguments::record(Some(v0), "v0")?;
        self.call(py, |state, lines| {
            let Some(values) = read(&v0, lines, "Non-measure type v0 in measure conversion\n")
            else {
                return Ok(CasaValue::empty_record());
            };
            let values = match values.resolve(state, values.values[0].refer)? {
                Ok(values) => values,
                Err(error) => return Ok(failed(lines, error)),
            };
            let refer = RADIAL_VELOCITY_CODES.resolve_or_default(&rf);
            Ok(converted(
                values.map(|v| {
                    let beta = doppler(v)?
                        .convert_to(DopplerRef::BETA, &MeasFrame::new())
                        .map_err(|e| e.to_string())?;
                    Ok(Measure {
                        refer,
                        value: MeasureValue::RadialVelocity(beta.value() * C_MPS),
                    })
                }),
                lines,
            ))
        })
    }
    /// Recover a rest frequency from an observed frequency and a Doppler measure.
    #[pyo3(signature=(v0,d0),text_signature="($self, v0, d0)")]
    fn torestfrequency(
        &self,
        py: Python<'_>,
        v0: &Bound<'_, PyAny>,
        d0: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let v0 = super::arguments::record(Some(v0), "v0")?;
        let d0 = super::arguments::record(Some(d0), "d0")?;
        self.call(py, |state, lines| {
            let Some(mut freq) = read(&v0, lines, "Non-frequency measure in conversion\n") else {
                return Ok(CasaValue::empty_record());
            };
            let Some(dop) = read(&d0, lines, "Non-Doppler measure in conversion\n") else {
                return Ok(CasaValue::empty_record());
            };
            let dop = match dop.resolve(state, dop.values[0].refer)? {
                Ok(values) => values,
                Err(error) => return Ok(failed(lines, error)),
            };
            if freq.values.len() == 1 && dop.values.len() > 1 {
                freq.values.resize(dop.values.len(), freq.values[0]);
                freq.vector = true;
            }
            if dop.values.len() != 1 && dop.values.len() != freq.values.len() {
                return Ok(failed(
                    lines,
                    "frequency and Doppler array lengths differ (#1252)",
                ));
            }
            let mut i = 0;
            let result = freq.map(|v| {
                let MeasureValue::Frequency(hz) = v.value else {
                    return Err("Illegal frequency or Doppler specified".into());
                };
                let dop = doppler(dop.values[i % dop.values.len()])?
                    .convert_to(DopplerRef::BETA, &MeasFrame::new())
                    .map_err(|e| e.to_string())?;
                i += 1;
                Ok(Measure {
                    refer: "REST",
                    value: MeasureValue::Frequency(dop.rest_frequency_hz(hz)),
                })
            });
            Ok(converted(result, lines))
        })
    }
}
