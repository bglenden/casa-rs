// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed accessor for frequency columns in the SPECTRAL_WINDOW subtable.
//!
//! Returns channel frequencies as [`MFrequency`] values with the correct
//! reference frame, determined by the MEAS_FREQ_REF column.
//!
//! Cf. C++ `MSSpWindowColumns::chanFreqMeas()`.

use casacore_tables::Table;
use casacore_types::ArrayValue;
use casacore_types::measures::frequency::{FrequencyRef, MFrequency};

use crate::error::{MsError, MsResult};
use crate::subtables::{get_array, get_f64, get_i32};

/// Typed accessor for channel frequencies.
pub struct ChanFreqColumn<'a> {
    table: &'a Table,
}

impl<'a> ChanFreqColumn<'a> {
    /// Create a channel frequency column accessor over the SPECTRAL_WINDOW subtable.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Read channel frequencies as [`MFrequency`] values for a spectral window row.
    ///
    /// Uses the MEAS_FREQ_REF column to determine the reference frame.
    pub fn get_frequencies(&self, row: usize) -> MsResult<Vec<MFrequency>> {
        let refer = self.get_freq_ref(row)?;
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
        let refer = self.get_freq_ref(row)?;
        let hz = get_f64(self.table, row, "REF_FREQUENCY")?;
        Ok(MFrequency::new(hz, refer))
    }

    /// Read the MEAS_FREQ_REF integer code and convert to [`FrequencyRef`].
    fn get_freq_ref(&self, row: usize) -> MsResult<FrequencyRef> {
        let code = get_i32(self.table, row, "MEAS_FREQ_REF")?;
        FrequencyRef::from_casacore_code(code).ok_or_else(|| MsError::InvalidMeasureCode {
            table: "SPECTRAL_WINDOW".to_string(),
            column: "MEAS_FREQ_REF".to_string(),
            code,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;
    use crate::schema;
    use casacore_types::{RecordField, RecordValue, ScalarValue, Value};
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
