// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `Spectral_Record` column: the record CASA's spectral models write
//! with `toRecord` and read back with `fromRecord`.
//!
//! The record repeats what the other columns hold (the type, the reference
//! frequency, the parameters) and adds what they cannot: a spectral index's
//! four Stokes indices and a tabular spectrum's tabulation. Its layout, in
//! CASA's field order:
//!
//! | model | fields |
//! |---|---|
//! | every model | `type` (the model's name), `frequency` (a `MeasureHolder` frequency record whose `m0` is in GHz) |
//! | spectral index | `index`, `stokesindex` (1 or 4 values), `error` |
//! | tabular | `freqRef` (a frequency record of 0 Hz in the tabulation frame), `tabFreqVal` (Hz), `ival`, `qval`, `uval`, `vval`, `referenceFreq` (Hz, in the tabulation frame), `maxFreq`, `minFreq` |
//! | power-log-polynomial | `coeffs`, `error` |
//!
//! `readTable` applies the record first and then sets the reference
//! frequency, its error, the parameters and their errors from their own
//! columns, so only the record-only values survive from it. It ignores
//! whether `fromRecord` succeeded, so a record missing a field it needs leaves
//! the model's defaults in place; casa-rs does the same.

use casa_types::measures::frequency::FrequencyRef;
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};

use crate::error::ComponentError;
use crate::spectrum::{SpectralKind, SpectralModel, TabularSpectrum};

/// The record CASA's `toRecord` writes for `model`.
///
/// # Errors
///
/// [`ComponentError::Spectrum`] for a tabular spectrum whose reference
/// frequency CASA could not express in its tabulation frame.
pub(crate) fn spectral_record(model: &SpectralModel) -> Result<RecordValue, ComponentError> {
    let reference = model.reference_frequency();
    let mut fields = vec![
        field("type", string(model.spectral_type().casa_name())),
        field(
            "frequency",
            Value::Record(frequency_record(
                reference.refer(),
                // casacore's `Quantum::convert` from Hz to GHz.
                RecordField::new("unit", string("GHz")),
                RecordField::new("value", real(reference.hz() * (1.0 / 1.0e9))),
            )),
        ),
    ];
    match model.kind() {
        SpectralKind::Constant => {}
        SpectralKind::SpectralIndex(index) => {
            fields.push(field("index", real(index.index())));
            fields.push(field("stokesindex", reals(index.casa_stokes_index())));
            fields.push(field("error", real(index.error())));
        }
        SpectralKind::Tabular(tabular) => {
            let reference_in_frame = tabular.reference_frequency_in_frame(reference)?;
            let (max, min) = tabular.frequency_range_hz();
            let column = |stokes: usize| {
                reals(
                    tabular
                        .stokes_flux()
                        .iter()
                        .map(|flux| flux[stokes])
                        .collect(),
                )
            };
            fields.push(field(
                "freqRef",
                Value::Record(frequency_record(
                    tabular.frame(),
                    RecordField::new("value", real(0.0)),
                    RecordField::new("unit", string("Hz")),
                )),
            ));
            fields.push(field(
                "tabFreqVal",
                reals(tabular.frequencies_hz().to_vec()),
            ));
            fields.push(field("ival", column(0)));
            fields.push(field("qval", column(1)));
            fields.push(field("uval", column(2)));
            fields.push(field("vval", column(3)));
            fields.push(field("referenceFreq", real(reference_in_frame)));
            fields.push(field("maxFreq", real(max)));
            fields.push(field("minFreq", real(min)));
        }
        SpectralKind::PowerLogPolynomial(polynomial) => {
            fields.push(field("coeffs", reals(polynomial.coefficients().to_vec())));
            fields.push(field("error", reals(polynomial.errors().to_vec())));
        }
    }
    Ok(RecordValue::new(fields))
}

/// Apply a `Spectral_Record` cell to `kind` as the model's `fromRecord` does.
///
/// Nothing is applied when the record lacks a usable `frequency` field
/// (`SpectralModel::fromRecord` fails first), and a model-specific field that
/// is missing or malformed leaves that value at its default.
///
/// # Errors
///
/// [`ComponentError::Spectrum`] for a negative error, which CASA's setters
/// throw on, and for a tabulation whose columns differ in length.
pub(super) fn apply_spectral_record(
    kind: &mut SpectralKind,
    record: &RecordValue,
) -> Result<(), ComponentError> {
    if !has_frequency(record) {
        return Ok(());
    }
    match kind {
        SpectralKind::Constant => {}
        SpectralKind::SpectralIndex(index) => {
            let Some(value) = record.get("index").and_then(scalar_real) else {
                return Ok(());
            };
            // `setIndex`, then the record's Stokes index without touching α.
            let mut updated = crate::spectrum::SpectralIndex::new(value);
            if let Some(stokes) = record.get("stokesindex").and_then(real_array) {
                updated = updated.with_casa_stokes_index(&stokes);
            }
            let error = match record.get("error") {
                Some(value) => scalar_real(value).unwrap_or(0.0),
                None => 0.0,
            };
            *index = updated.with_error(error)?;
        }
        SpectralKind::Tabular(tabular) => {
            let frame = record
                .get("freqRef")
                .and_then(|value| match value {
                    Value::Record(frame) => frame_of(frame),
                    _ => None,
                })
                .unwrap_or(tabular.frame());
            let frequencies = record.get("tabFreqVal").and_then(real_array);
            let ival = record.get("ival").and_then(real_array);
            *tabular = match (frequencies, ival) {
                (Some(frequencies), Some(ival)) => {
                    let other = |name: &str| {
                        record
                            .get(name)
                            .and_then(real_array)
                            .unwrap_or_else(|| vec![0.0; ival.len()])
                    };
                    let (qval, uval, vval) = (other("qval"), other("uval"), other("vval"));
                    if [qval.len(), uval.len(), vval.len()]
                        .iter()
                        .any(|length| *length != ival.len())
                    {
                        return Err(ComponentError::Spectrum {
                            spectrum: "tabular",
                            reason: "the record's Stokes columns differ in length".to_string(),
                        });
                    }
                    let flux = (0..ival.len())
                        .map(|k| [ival[k], qval[k], uval[k], vval[k]])
                        .collect();
                    TabularSpectrum::new(frame, frequencies, flux)?
                }
                _ => TabularSpectrum::new(
                    frame,
                    tabular.frequencies_hz().to_vec(),
                    tabular.stokes_flux().to_vec(),
                )?,
            };
        }
        SpectralKind::PowerLogPolynomial(polynomial) => {
            let Some(coefficients) = record.get("coeffs").and_then(real_array) else {
                return Ok(());
            };
            let mut updated = crate::spectrum::PowerLogPolynomial::new(coefficients);
            if let Some(errors) = record.get("error").and_then(real_array)
                && errors.len() == updated.coefficients().len()
            {
                updated = updated.with_errors(errors)?;
            }
            *polynomial = updated;
        }
    }
    Ok(())
}

/// A `MeasureHolder` frequency record: `type`, `refer`, then `m0` with its
/// two fields in the order given. A holder written directly puts the value
/// first; `SpectralModel::toRecord` rewrites `m0` in GHz, which removes and
/// re-adds the value after the unit (`QuantumHolder::toRecord`).
fn frequency_record(frame: FrequencyRef, first: RecordField, second: RecordField) -> RecordValue {
    RecordValue::new(vec![
        field("type", string("frequency")),
        field("refer", string(frame.as_str())),
        field("m0", Value::Record(RecordValue::new(vec![first, second]))),
    ])
}

/// `SpectralModel::fromRecord` needs a `frequency` field holding a frequency
/// record, or the string `current`.
fn has_frequency(record: &RecordValue) -> bool {
    match record.get("frequency") {
        Some(Value::Record(frequency)) => {
            matches!(frequency.get("type"), Some(Value::Scalar(ScalarValue::String(kind))) if kind.eq_ignore_ascii_case("frequency"))
        }
        Some(Value::Scalar(ScalarValue::String(text))) => text == "current",
        _ => false,
    }
}

/// The frame of a `MeasureHolder` frequency record.
fn frame_of(record: &RecordValue) -> Option<FrequencyRef> {
    match record.get("refer") {
        Some(Value::Scalar(ScalarValue::String(refer))) => refer.parse().ok(),
        _ => None,
    }
}

fn field(name: &str, value: Value) -> RecordField {
    RecordField::new(name, value)
}

fn string(text: &str) -> Value {
    Value::Scalar(ScalarValue::String(text.to_string()))
}

fn real(value: f64) -> Value {
    Value::Scalar(ScalarValue::Float64(value))
}

fn reals(values: Vec<f64>) -> Value {
    Value::Array(ArrayValue::from_f64_vec(values))
}

fn scalar_real(value: &Value) -> Option<f64> {
    match value {
        Value::Scalar(ScalarValue::Float64(value)) => Some(*value),
        Value::Scalar(ScalarValue::Float32(value)) => Some(f64::from(*value)),
        Value::Scalar(ScalarValue::Int32(value)) => Some(f64::from(*value)),
        _ => None,
    }
}

fn real_array(value: &Value) -> Option<Vec<f64>> {
    match value {
        Value::Array(array) => super::measures::array_f64(array),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use casa_types::measures::frequency::MFrequency;

    use super::*;
    use crate::spectrum::{PowerLogPolynomial, SpectralIndex};

    fn applied(model: &SpectralModel) -> SpectralKind {
        let record = spectral_record(model).expect("record");
        let mut kind = SpectralKind::casa_default(model.spectral_type());
        apply_spectral_record(&mut kind, &record).expect("apply");
        kind
    }

    #[test]
    fn records_carry_what_the_columns_cannot() {
        let reference = MFrequency::new(1.4e9, FrequencyRef::LSRK);
        let index = SpectralModel::new(
            reference.clone(),
            SpectralKind::SpectralIndex(
                SpectralIndex::new(-0.7).with_stokes_indices([-0.8, 0.1, 50.0, 0.0]),
            ),
        );
        assert_eq!(&applied(&index), index.kind());

        let tabular = SpectralModel::new(
            reference.clone(),
            SpectralKind::Tabular(
                TabularSpectrum::new(
                    FrequencyRef::LSRK,
                    vec![1.0e9, 1.5e9, 2.0e9],
                    vec![
                        [1.0, 0.1, 0.0, 0.0],
                        [0.8, 0.0, 0.1, 0.0],
                        [0.5, 0.0, 0.0, 0.05],
                    ],
                )
                .expect("tabulation"),
            ),
        );
        assert_eq!(&applied(&tabular), tabular.kind());

        let polynomial = SpectralModel::new(
            reference,
            SpectralKind::PowerLogPolynomial(
                PowerLogPolynomial::new(vec![0.1, -0.5, 0.02])
                    .with_errors(vec![0.01, 0.02, 0.003])
                    .expect("errors"),
            ),
        );
        assert_eq!(&applied(&polynomial), polynomial.kind());
    }

    #[test]
    fn the_frequency_is_written_in_ghz_as_casa_writes_it() {
        let model = SpectralModel::constant(MFrequency::new(1.4e9, FrequencyRef::TOPO));
        let record = spectral_record(&model).expect("record");
        let Some(Value::Record(frequency)) = record.get("frequency") else {
            panic!("frequency record");
        };
        let Some(Value::Record(m0)) = frequency.get("m0") else {
            panic!("m0");
        };
        assert_eq!(m0.fields()[0].name, "unit");
        // CASA writes 1.4000000000000001 GHz for 1.4 GHz.
        assert_eq!(m0.get("value"), Some(&real(1.400_000_000_000_000_1)));
    }

    #[test]
    fn a_record_without_a_frequency_is_ignored() {
        let mut kind =
            SpectralKind::casa_default(crate::component_type::SpectralType::SpectralIndex);
        let record = RecordValue::new(vec![
            field("index", real(2.0)),
            field("stokesindex", reals(vec![2.0, 0.0, 1.0, 0.0])),
        ]);
        apply_spectral_record(&mut kind, &record).expect("apply");
        assert_eq!(kind, SpectralKind::SpectralIndex(SpectralIndex::new(0.0)));
    }

    #[test]
    fn mismatched_frames_cannot_be_written() {
        let model = SpectralModel::new(
            MFrequency::new(1.4e9, FrequencyRef::LSRK),
            SpectralKind::Tabular(
                TabularSpectrum::new(
                    FrequencyRef::BARY,
                    vec![1.0e9, 2.0e9],
                    vec![[1.0, 0.0, 0.0, 0.0]; 2],
                )
                .expect("tabulation"),
            ),
        );
        assert!(spectral_record(&model).is_err());
    }
}
