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
//! the model's defaults in place; casa-rs does the same. `fromRecord` applies
//! nothing unless `MeasureHolder::fromRecord` reads the record's `frequency`
//! as a frequency, and a tabulation nothing unless it also reads `freqRef`.

use casa_types::measures::frequency::FrequencyRef;
use casa_types::quanta::{Dimension, Unit, UnitDim};
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
/// Nothing is applied unless the record's `frequency` is a frequency measure
/// `frequency_measure` accepts or the string `current`
/// (`SpectralModel::fromRecord` fails first), and nothing to a tabulation
/// unless its `freqRef` is one too (`TabularSpectrum::fromRecord` reads it
/// before the tabulation). A model-specific field that is missing or
/// malformed leaves that value at its default.
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
            let Some(frame) = record.get("freqRef").and_then(frequency_measure) else {
                return Ok(());
            };
            let frame = frame_of(frame).unwrap_or(tabular.frame());
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
/// measure, or the string `current`.
fn has_frequency(record: &RecordValue) -> bool {
    match record.get("frequency") {
        Some(Value::Scalar(ScalarValue::String(text))) => text == "current",
        Some(value) => frequency_measure(value).is_some(),
        None => false,
    }
}

/// The measure record in `value`, when `MeasureHolder::fromRecord` reads it
/// and it is an `MFrequency` (`isMFrequency`).
///
/// casacore needs string `type` and `refer` fields. It takes any `refer` (an
/// unknown one is the default frame, with a warning), reads an `offset`
/// record as a measure of the same kind (`MFrequency::setOffset`), and hands
/// the quantity records `m0`, then `m1` and `m2`, to `MVFrequency::putValue`,
/// which takes none (0 Hz) or one. casacore throws, rather than refuses, on a
/// quantity it cannot convert (an unknown unit, a complex value, a
/// non-`Double` array); casa-rs refuses those records too.
fn frequency_measure(value: &Value) -> Option<&RecordValue> {
    let Value::Record(measure) = value else {
        return None;
    };
    let (
        Some(Value::Scalar(ScalarValue::String(kind))),
        Some(Value::Scalar(ScalarValue::String(_))),
    ) = (measure.get("type"), measure.get("refer"))
    else {
        return None;
    };
    if !kind.eq_ignore_ascii_case("frequency") {
        return None;
    }
    if let Some(offset @ Value::Record(_)) = measure.get("offset") {
        frequency_measure(offset)?;
    }
    let quantities: Vec<&RecordValue> = ["m0", "m1", "m2"]
        .into_iter()
        .map_while(|name| match measure.get(name) {
            Some(Value::Record(quantity)) => Some(quantity),
            _ => None,
        })
        .collect();
    match quantities.as_slice() {
        [] => Some(measure),
        [quantity] => is_frequency_quantity(quantity).then_some(measure),
        _ => None,
    }
}

/// Whether `quantity` is a quantity record (`QuantumHolder::fromRecord`) with
/// one real value, or a non-empty `Double` array, in a unit
/// `MVFrequency::putValue` takes: a frequency, period, angular rate,
/// wavelength, wavenumber, energy, or casacore's `Impuls` (mass × length).
fn is_frequency_quantity(quantity: &RecordValue) -> bool {
    let value = match quantity.get("value") {
        Some(Value::Scalar(
            ScalarValue::Float64(_) | ScalarValue::Float32(_) | ScalarValue::Int32(_),
        )) => true,
        Some(Value::Array(ArrayValue::Float64(values))) => !values.is_empty(),
        _ => false,
    };
    let Some(Value::Scalar(ScalarValue::String(unit))) = quantity.get("unit") else {
        return false;
    };
    let basis = UnitDim::basis;
    let (time, length, mass) = (
        basis(Dimension::Time),
        basis(Dimension::Length),
        basis(Dimension::Mass),
    );
    let frequency_like = [
        time,
        UnitDim::NODIM / time,
        basis(Dimension::Angle) / time,
        length,
        UnitDim::NODIM / length,
        mass * length * length / time / time,
        mass * length,
    ];
    value && Unit::new(unit).is_ok_and(|unit| frequency_like.contains(&unit.val().dim))
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
    use crate::component_type::SpectralType;
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

    fn record(fields: Vec<RecordField>) -> Value {
        Value::Record(RecordValue::new(fields))
    }

    /// The quantity record `name` holding 1.4 `unit`.
    fn quantity(name: &str, unit: &str) -> RecordField {
        field(
            name,
            record(vec![field("unit", string(unit)), field("value", real(1.4))]),
        )
    }

    /// Frequency fields `MeasureHolder::fromRecord` refuses or reads as
    /// another measure.
    fn unreadable_frequencies() -> Vec<Value> {
        let typed = |kind: &str| field("type", string(kind));
        let lsrk = || field("refer", string("LSRK"));
        vec![
            // No `refer`: the record CASA 6.7.6 ignored.
            record(vec![typed("frequency")]),
            record(vec![typed("frequency"), field("refer", real(1.0))]),
            record(vec![typed("direction"), field("refer", string("J2000"))]),
            record(vec![typed("frequencies"), lsrk()]),
            record(vec![typed("frequency"), lsrk(), quantity("m0", "Jy")]),
            record(vec![
                typed("frequency"),
                lsrk(),
                quantity("m0", "GHz"),
                quantity("m1", "GHz"),
            ]),
            record(vec![
                typed("frequency"),
                lsrk(),
                field("m0", record(vec![field("value", real(1.4))])),
            ]),
            record(vec![
                typed("frequency"),
                lsrk(),
                field("offset", record(vec![typed("frequency")])),
            ]),
            string("now"),
        ]
    }

    fn index_record() -> RecordValue {
        let model = SpectralModel::new(
            MFrequency::new(1.4e9, FrequencyRef::LSRK),
            SpectralKind::SpectralIndex(
                SpectralIndex::new(-0.7).with_stokes_indices([2.0, 3.0, 4.0, 5.0]),
            ),
        );
        spectral_record(&model).expect("record")
    }

    fn tabular_record() -> RecordValue {
        let model = SpectralModel::new(
            MFrequency::new(1.4e9, FrequencyRef::BARY),
            SpectralKind::Tabular(
                TabularSpectrum::new(
                    FrequencyRef::BARY,
                    vec![1.0e9, 2.0e9],
                    vec![[1.0, 0.0, 0.0, 0.0], [0.5, 0.0, 0.0, 0.0]],
                )
                .expect("tabulation"),
            ),
        );
        spectral_record(&model).expect("record")
    }

    fn applied_to_default(record: &RecordValue, kind: SpectralType) -> SpectralKind {
        let mut applied = SpectralKind::casa_default(kind);
        apply_spectral_record(&mut applied, record).expect("apply");
        applied
    }

    #[test]
    fn a_frequency_casacore_cannot_read_leaves_the_model_alone() {
        // `SpectralModel::fromRecord` needs `MeasureHolder::fromRecord` to
        // read a frequency before a model applies anything else.
        for unreadable in unreadable_frequencies() {
            let mut record = index_record();
            record.upsert("frequency", unreadable.clone());
            assert_eq!(
                applied_to_default(&record, SpectralType::SpectralIndex),
                SpectralKind::casa_default(SpectralType::SpectralIndex),
                "{unreadable:?}"
            );
            let mut record = tabular_record();
            record.upsert("frequency", unreadable.clone());
            assert_eq!(
                applied_to_default(&record, SpectralType::Tabular),
                SpectralKind::casa_default(SpectralType::Tabular),
                "{unreadable:?}"
            );
        }
    }

    #[test]
    fn a_tabulation_needs_a_readable_frame() {
        // `TabularSpectrum::fromRecord` reads `freqRef` with `MeasureHolder`
        // too, and stops before the tabulation when it cannot.
        let mut missing = tabular_record();
        missing.remove("freqRef");
        let mut cases = vec![missing];
        for unreadable in unreadable_frequencies() {
            let mut record = tabular_record();
            record.upsert("freqRef", unreadable);
            cases.push(record);
        }
        for record in cases {
            assert_eq!(
                applied_to_default(&record, SpectralType::Tabular),
                SpectralKind::casa_default(SpectralType::Tabular),
                "{:?}",
                record.get("freqRef")
            );
        }
    }

    #[test]
    fn frequencies_casacore_reads_are_applied() {
        // A record without a quantity record `m0` is 0 Hz
        // (`MVFrequency::putValue`), an unknown frame is casacore's default
        // with a warning, a frequency may be a wavelength or a period, and
        // `current` keeps the model's frequency.
        let typed = field("type", string("FREQUENCY"));
        let lsrk = || field("refer", string("LSRK"));
        let expected = applied_to_default(&index_record(), SpectralType::SpectralIndex);
        assert_ne!(
            expected,
            SpectralKind::casa_default(SpectralType::SpectralIndex)
        );
        for readable in [
            record(vec![typed.clone(), lsrk()]),
            record(vec![typed.clone(), field("refer", string("NOSUCH"))]),
            record(vec![typed.clone(), lsrk(), quantity("m0", "cm")]),
            record(vec![typed.clone(), lsrk(), quantity("m0", "ns")]),
            record(vec![typed.clone(), lsrk(), field("m0", real(1.0))]),
            record(vec![
                typed.clone(),
                lsrk(),
                field("offset", record(vec![typed.clone(), lsrk()])),
            ]),
            string("current"),
        ] {
            let mut record = index_record();
            record.upsert("frequency", readable.clone());
            assert_eq!(
                applied_to_default(&record, SpectralType::SpectralIndex),
                expected,
                "{readable:?}"
            );
        }
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
