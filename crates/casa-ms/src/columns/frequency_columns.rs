// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed accessor for frequency columns in the SPECTRAL_WINDOW subtable.
//!
//! Returns channel frequencies as [`MFrequency`] values with the correct
//! reference frame, determined by the MEAS_FREQ_REF column.
//!
//! Cf. C++ `MSSpWindowColumns::chanFreqMeas()`.

use casa_tables::Table;
use casa_tables::table_measures::{
    MeasRefDesc, MeasureType, TableMeasDesc, frequency_ref_for_table_code,
};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::{ArrayValue, ScalarValue, Value};

use crate::error::{MsError, MsResult};
use crate::subtables::{get_array, get_f64, get_i32, get_string};

/// Typed accessor for channel frequencies.
pub struct ChanFreqColumn<'a> {
    table: &'a Table,
    channels_reference: FrequencyReference,
    centre_reference: FrequencyReference,
}

enum FrequencyReference {
    Missing,
    Invalid,
    Incomplete,
    Present(TableMeasDesc),
}

impl FrequencyReference {
    fn read(table: &Table, column: &str) -> Self {
        if let Some(desc) = TableMeasDesc::reconstruct(table, column) {
            Self::Present(desc)
        } else if let Some(Value::Record(info)) = table
            .column_keywords(column)
            .and_then(|keywords| keywords.get("MEASINFO"))
            && matches!(info.get("type"), Some(Value::Scalar(ScalarValue::String(t))) if t.eq_ignore_ascii_case("frequency"))
            && info.get("Ref").is_none()
            && info.get("VarRefCol").is_none()
        {
            // casacore gives a frequency measure with no Ref its code-zero
            // default (REST); it does not consult MEAS_FREQ_REF in this case.
            Self::Incomplete
        } else if table
            .column_keywords(column)
            .is_some_and(|keywords| keywords.get("MEASINFO").is_some())
        {
            Self::Invalid
        } else {
            Self::Missing
        }
    }

    fn or<'a>(&'a self, other: &'a Self) -> &'a Self {
        if matches!(self, Self::Missing) {
            other
        } else {
            self
        }
    }
}

impl<'a> ChanFreqColumn<'a> {
    /// Create a channel frequency column accessor over the SPECTRAL_WINDOW subtable.
    pub fn new(table: &'a Table) -> Self {
        let channels_reference = FrequencyReference::read(table, "CHAN_FREQ");
        let centre_reference = FrequencyReference::read(table, "REF_FREQUENCY");
        Self {
            table,
            channels_reference,
            centre_reference,
        }
    }

    /// Read channel frequencies as [`MFrequency`] values for a spectral window row.
    ///
    /// Uses the MEAS_FREQ_REF column to determine the reference frame.
    pub fn get_frequencies(&self, row: usize) -> MsResult<Vec<MFrequency>> {
        let refer = self.frequency_reference(row)?;
        let arr = get_array(self.table, row, "CHAN_FREQ")?;

        match arr {
            ArrayValue::Float64(a) => Ok(a.iter().map(|&hz| MFrequency::new(hz, refer)).collect()),
            other => Err(MsError::ColumnTypeMismatch {
                column: "CHAN_FREQ".to_string(),
                table: "SPECTRAL_WINDOW".to_string(),
                expected: "Float64 array".to_string(),
                found: format!("{:?}", other.primitive_type()),
            }),
        }
    }

    /// Read the reference frequency as an [`MFrequency`].
    pub fn get_ref_frequency(&self, row: usize) -> MsResult<MFrequency> {
        let refer = self.reference_frequency_reference(row)?;
        let hz = get_f64(self.table, row, "REF_FREQUENCY")?;
        Ok(MFrequency::new(hz, refer))
    }

    /// Resolve the row's frequency frame using its table-specific measure
    /// reference mapping, including codes inferred by casacore for old tables.
    /// Without measure metadata, use the standard `MEAS_FREQ_REF` codes.
    ///
    /// # Errors
    ///
    /// The reference column or its measure mapping is invalid or unreadable.
    pub fn frequency_reference(&self, row: usize) -> MsResult<FrequencyRef> {
        self.resolve_reference(
            row,
            self.channels_reference.or(&self.centre_reference),
            "CHAN_FREQ",
        )
    }

    /// The visibility frame for a channel column whose frequency measure
    /// descriptor lacks a reference. CASA's visibility iterator reads the
    /// measurement-frame column in that case, while the measure accessor's
    /// default remains REST.
    pub(crate) fn visibility_frequency_reference(&self, row: usize) -> MsResult<FrequencyRef> {
        if matches!(
            self.channels_reference.or(&self.centre_reference),
            FrequencyReference::Incomplete
        ) {
            let code = get_i32(self.table, row, "MEAS_FREQ_REF")?;
            FrequencyRef::from_casacore_code(code)
                .ok_or_else(|| invalid_code("MEAS_FREQ_REF", code))
        } else {
            self.frequency_reference(row)
        }
    }

    /// Resolve the reference-frequency column's frame, which can be distinct
    /// from the channel-frequency column's frame.
    ///
    /// # Errors
    ///
    /// The reference column or its measure mapping is invalid or unreadable.
    pub fn reference_frequency_reference(&self, row: usize) -> MsResult<FrequencyRef> {
        self.resolve_reference(
            row,
            self.centre_reference.or(&self.channels_reference),
            "REF_FREQUENCY",
        )
    }

    fn resolve_reference(
        &self,
        row: usize,
        reference: &FrequencyReference,
        column: &str,
    ) -> MsResult<FrequencyRef> {
        if matches!(reference, FrequencyReference::Invalid) {
            return Err(MsError::Table(casa_tables::TableError::Storage(format!(
                "invalid MEASINFO for SPECTRAL_WINDOW.{column}"
            ))));
        }
        if matches!(reference, FrequencyReference::Incomplete) {
            return Ok(FrequencyRef::REST);
        }
        if let FrequencyReference::Present(desc) = reference {
            if desc.measure_type() != MeasureType::Frequency {
                return Err(MsError::ColumnTypeMismatch {
                    column: column.to_string(),
                    table: "SPECTRAL_WINDOW".to_string(),
                    expected: "frequency measure".to_string(),
                    found: desc.measure_type().as_str().to_string(),
                });
            }
            match desc.ref_desc() {
                MeasRefDesc::Fixed { refer } => return Ok(refer.parse()?),
                MeasRefDesc::VariableString { ref_column } => {
                    return Ok(get_string(self.table, row, ref_column)?.parse()?);
                }
                MeasRefDesc::VariableInt {
                    ref_column,
                    tab_ref_types,
                    tab_ref_codes,
                } => {
                    let code = get_i32(self.table, row, ref_column)?;
                    return frequency_ref_for_table_code(tab_ref_types, tab_ref_codes, code)
                        .ok_or_else(|| invalid_code(ref_column, code));
                }
            }
        }
        let code = get_i32(self.table, row, "MEAS_FREQ_REF")?;
        FrequencyRef::from_casacore_code(code).ok_or_else(|| invalid_code("MEAS_FREQ_REF", code))
    }
}

fn invalid_code(column: &str, code: i32) -> MsError {
    MsError::InvalidMeasureCode {
        table: "SPECTRAL_WINDOW".to_string(),
        column: column.to_string(),
        code,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;
    use crate::schema;
    use casa_types::{RecordField, RecordValue, ScalarValue, Value};
    use ndarray::ArrayD;

    fn make_spw_table(freq_ref: i32, ref_freq: f64, chan_freqs: &[f64]) -> Table {
        let schema =
            build_table_schema(schema::spectral_window::REQUIRED_COLUMNS).expect("valid schema");
        let mut table = Table::with_schema(schema);

        let widths = vec![1.0e6; chan_freqs.len()];
        let n = chan_freqs.len();

        let make_arr = |v: &[f64]| -> ArrayValue {
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![v.len()], v.to_vec()).unwrap())
        };

        let row = RecordValue::new(vec![
            RecordField::new("CHAN_FREQ", Value::Array(make_arr(chan_freqs))),
            RecordField::new("CHAN_WIDTH", Value::Array(make_arr(&widths))),
            RecordField::new("EFFECTIVE_BW", Value::Array(make_arr(&widths))),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new(
                "FREQ_GROUP_NAME",
                Value::Scalar(ScalarValue::String(String::new())),
            ),
            RecordField::new("IF_CONV_CHAIN", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(freq_ref))),
            RecordField::new("NAME", Value::Scalar(ScalarValue::String(String::new()))),
            RecordField::new("NET_SIDEBAND", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("NUM_CHAN", Value::Scalar(ScalarValue::Int32(n as i32))),
            RecordField::new(
                "REF_FREQUENCY",
                Value::Scalar(ScalarValue::Float64(ref_freq)),
            ),
            RecordField::new("RESOLUTION", Value::Array(make_arr(&widths))),
            RecordField::new(
                "TOTAL_BANDWIDTH",
                Value::Scalar(ScalarValue::Float64(n as f64 * 1.0e6)),
            ),
        ]);
        table.add_row(row).unwrap();
        table
    }

    #[test]
    fn chan_freq_with_topo_ref() {
        let freqs = [1.0e9, 1.001e9, 1.002e9];
        let table = make_spw_table(5, 1.0e9, &freqs); // 5 = TOPO

        let col = ChanFreqColumn::new(&table);
        let mfreqs = col.get_frequencies(0).unwrap();
        assert_eq!(mfreqs.len(), 3);
        assert_eq!(mfreqs[0].refer(), FrequencyRef::TOPO);
        assert!((mfreqs[0].hz() - 1.0e9).abs() < 1.0);
        assert!((mfreqs[2].hz() - 1.002e9).abs() < 1.0);
    }

    #[test]
    fn ref_frequency_round_trip() {
        let table = make_spw_table(1, 1.42e9, &[1.42e9]); // 1 = LSRK
        let col = ChanFreqColumn::new(&table);
        let rf = col.get_ref_frequency(0).unwrap();
        assert_eq!(rf.refer(), FrequencyRef::LSRK);
        assert!((rf.hz() - 1.42e9).abs() < 1.0);
    }

    #[test]
    fn table_reference_mapping_takes_precedence_over_canonical_integer_codes() {
        for (code, names, codes, expected) in [
            (
                64,
                vec!["LSRK", "Undefined"],
                vec![64, 9],
                FrequencyRef::LSRK,
            ),
            (
                9,
                vec!["LSRK", "Undefined"],
                vec![64, 9],
                FrequencyRef::Undefined,
            ),
            (36, vec!["LSRK"], vec![27], FrequencyRef::Undefined),
        ] {
            let mut table = make_spw_table(code, 1.4e9, &[1.4e9]);
            TableMeasDesc::new_variable_int(
                "CHAN_FREQ",
                MeasureType::Frequency,
                "MEAS_FREQ_REF",
                names.into_iter().map(str::to_string).collect(),
                codes,
            )
            .unwrap()
            .write(&mut table)
            .unwrap();
            let column = ChanFreqColumn::new(&table);
            assert_eq!(column.frequency_reference(0).unwrap(), expected);
            assert_eq!(column.get_frequencies(0).unwrap()[0].refer(), expected);
            assert_eq!(column.get_ref_frequency(0).unwrap().refer(), expected);
        }
    }

    #[test]
    fn channel_and_reference_frequency_use_their_own_measure_metadata() {
        let mut table = make_spw_table(64, 1.4e9, &[1.4e9]);
        TableMeasDesc::new_fixed("CHAN_FREQ", MeasureType::Frequency, "BARY")
            .write(&mut table)
            .unwrap();
        TableMeasDesc::new_fixed("REF_FREQUENCY", MeasureType::Frequency, "LSRK")
            .write(&mut table)
            .unwrap();
        let column = ChanFreqColumn::new(&table);
        assert_eq!(
            column.get_frequencies(0).unwrap()[0].refer(),
            FrequencyRef::BARY
        );
        assert_eq!(
            column.get_ref_frequency(0).unwrap().refer(),
            FrequencyRef::LSRK
        );
    }

    #[test]
    fn malformed_frequency_metadata_is_not_treated_as_canonical_codes() {
        let mut table = make_spw_table(64, 1.4e9, &[1.4e9]);
        table.set_column_keywords(
            "CHAN_FREQ",
            RecordValue::new(vec![RecordField::new(
                "MEASINFO",
                Value::Scalar(ScalarValue::Int32(1)),
            )]),
        );
        let error = ChanFreqColumn::new(&table)
            .frequency_reference(0)
            .unwrap_err();
        assert!(error.to_string().contains("invalid MEASINFO"), "{error}");
    }

    #[test]
    fn type_only_frequency_metadata_uses_casacores_rest_default() {
        let mut table = make_spw_table(3, 1.4e9, &[1.4e9]);
        for name in ["CHAN_FREQ", "REF_FREQUENCY"] {
            table.set_column_keywords(
                name,
                RecordValue::new(vec![RecordField::new(
                    "MEASINFO",
                    Value::Record(RecordValue::new(vec![RecordField::new(
                        "type",
                        Value::Scalar(ScalarValue::String("frequency".into())),
                    )])),
                )]),
            );
        }
        let column = ChanFreqColumn::new(&table);
        assert_eq!(column.frequency_reference(0).unwrap(), FrequencyRef::REST);
        assert_eq!(
            column.reference_frequency_reference(0).unwrap(),
            FrequencyRef::REST
        );
    }

    #[test]
    fn integer_reference_without_mapping_keywords_uses_standard_codes() {
        for (code, expected) in [(3, FrequencyRef::BARY), (64, FrequencyRef::Undefined)] {
            let mut table = make_spw_table(code, 1.4e9, &[1.4e9]);
            for name in ["CHAN_FREQ", "REF_FREQUENCY"] {
                TableMeasDesc::new_variable_string(name, MeasureType::Frequency, "MEAS_FREQ_REF")
                    .write(&mut table)
                    .unwrap();
            }
            let column = ChanFreqColumn::new(&table);
            assert_eq!(column.frequency_reference(0).unwrap(), expected);
            assert_eq!(column.reference_frequency_reference(0).unwrap(), expected);
        }
    }

    #[test]
    fn new_measurement_sets_have_explicit_variable_frequency_references() {
        let ms = crate::MeasurementSet::create_memory(crate::MeasurementSetBuilder::new()).unwrap();
        let spw = ms
            .subtable(crate::schema::SubtableId::SpectralWindow)
            .unwrap();
        for column in ["CHAN_FREQ", "REF_FREQUENCY"] {
            let desc = TableMeasDesc::reconstruct(spw, column).unwrap();
            let MeasRefDesc::VariableInt {
                ref_column,
                tab_ref_types,
                tab_ref_codes,
            } = desc.ref_desc()
            else {
                panic!("{column} must use the row's frequency reference");
            };
            assert_eq!(ref_column, "MEAS_FREQ_REF");
            assert_eq!(
                frequency_ref_for_table_code(tab_ref_types, tab_ref_codes, 3),
                Some(FrequencyRef::BARY)
            );
            assert_eq!(
                frequency_ref_for_table_code(tab_ref_types, tab_ref_codes, 64),
                Some(FrequencyRef::Undefined)
            );
        }
    }

    #[test]
    fn unknown_freq_ref_code_errors() {
        let table = make_spw_table(99, 1.42e9, &[1.42e9]);
        let col = ChanFreqColumn::new(&table);
        assert!(matches!(
            col.get_ref_frequency(0),
            Err(MsError::InvalidMeasureCode {
                table,
                column,
                code: 99
            }) if table == "SPECTRAL_WINDOW" && column == "MEAS_FREQ_REF"
        ));
    }
}
