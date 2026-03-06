// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrapper for the SPECTRAL_WINDOW subtable.
//!
//! Provides read access to channel frequencies, widths, reference frequency,
//! and other spectral window parameters.
//!
//! Cf. C++ `MSSpWindowColumns`.

use casacore_tables::Table;
use casacore_types::{ArrayValue, ScalarValue};

use crate::column_def::ColumnDef;
use crate::error::{MsError, MsResult};
use crate::schema::{self, SubtableId};
use crate::subtables::{
    SubTable, get_array, get_bool, get_f64, get_i32, get_string, set_array, set_scalar,
};

/// Read-only typed wrapper for the SPECTRAL_WINDOW subtable.
pub struct MsSpectralWindow<'a> {
    table: &'a Table,
}

impl<'a> MsSpectralWindow<'a> {
    /// Wrap an existing table as a SPECTRAL_WINDOW subtable.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Number of spectral window rows.
    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }

    /// Number of channels.
    pub fn num_chan(&self, row: usize) -> MsResult<i32> {
        get_i32(self.table, row, "NUM_CHAN")
    }

    /// Channel center frequencies in Hz.
    pub fn chan_freq(&self, row: usize) -> MsResult<Vec<f64>> {
        extract_f64_vec(get_array(self.table, row, "CHAN_FREQ")?)
    }

    /// Channel widths in Hz.
    pub fn chan_width(&self, row: usize) -> MsResult<Vec<f64>> {
        extract_f64_vec(get_array(self.table, row, "CHAN_WIDTH")?)
    }

    /// Effective noise bandwidth per channel in Hz.
    pub fn effective_bw(&self, row: usize) -> MsResult<Vec<f64>> {
        extract_f64_vec(get_array(self.table, row, "EFFECTIVE_BW")?)
    }

    /// Resolution per channel in Hz.
    pub fn resolution(&self, row: usize) -> MsResult<Vec<f64>> {
        extract_f64_vec(get_array(self.table, row, "RESOLUTION")?)
    }

    /// Reference frequency in Hz.
    pub fn ref_frequency(&self, row: usize) -> MsResult<f64> {
        get_f64(self.table, row, "REF_FREQUENCY")
    }

    /// Total bandwidth in Hz.
    pub fn total_bandwidth(&self, row: usize) -> MsResult<f64> {
        get_f64(self.table, row, "TOTAL_BANDWIDTH")
    }

    /// Frequency measure reference code (index into `FrequencyRef` enum).
    pub fn meas_freq_ref(&self, row: usize) -> MsResult<i32> {
        get_i32(self.table, row, "MEAS_FREQ_REF")
    }

    /// Spectral window name.
    pub fn name(&self, row: usize) -> MsResult<String> {
        get_string(self.table, row, "NAME")
    }

    /// Net sideband (+/- 1).
    pub fn net_sideband(&self, row: usize) -> MsResult<i32> {
        get_i32(self.table, row, "NET_SIDEBAND")
    }

    /// Frequency group.
    pub fn freq_group(&self, row: usize) -> MsResult<i32> {
        get_i32(self.table, row, "FREQ_GROUP")
    }

    /// Frequency group name.
    pub fn freq_group_name(&self, row: usize) -> MsResult<String> {
        get_string(self.table, row, "FREQ_GROUP_NAME")
    }

    /// IF conversion chain number.
    pub fn if_conv_chain(&self, row: usize) -> MsResult<i32> {
        get_i32(self.table, row, "IF_CONV_CHAIN")
    }

    /// Row flag.
    pub fn flag_row(&self, row: usize) -> MsResult<bool> {
        get_bool(self.table, row, "FLAG_ROW")
    }
}

impl SubTable for MsSpectralWindow<'_> {
    fn id() -> SubtableId {
        SubtableId::SpectralWindow
    }
    fn required_columns() -> &'static [ColumnDef] {
        schema::spectral_window::REQUIRED_COLUMNS
    }
    fn optional_columns() -> &'static [ColumnDef] {
        schema::spectral_window::OPTIONAL_COLUMNS
    }
    fn table(&self) -> &Table {
        self.table
    }
}

/// Mutable typed wrapper for the SPECTRAL_WINDOW subtable.
pub struct MsSpectralWindowMut<'a> {
    table: &'a mut Table,
}

impl<'a> MsSpectralWindowMut<'a> {
    /// Wrap an existing mutable table as a SPECTRAL_WINDOW subtable.
    pub fn new(table: &'a mut Table) -> Self {
        Self { table }
    }

    /// Number of spectral window rows.
    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }

    /// View the table through the read-only wrapper.
    pub fn as_ref(&self) -> MsSpectralWindow<'_> {
        MsSpectralWindow { table: self.table }
    }

    /// Access the underlying mutable table.
    pub fn table_mut(&mut self) -> &mut Table {
        self.table
    }

    /// Set a scalar `i32` column by name.
    pub fn set_i32(&mut self, row: usize, col: &str, value: i32) -> MsResult<()> {
        set_scalar(self.table, row, col, ScalarValue::Int32(value))
    }

    /// Set a scalar `f64` column by name.
    pub fn set_f64(&mut self, row: usize, col: &str, value: f64) -> MsResult<()> {
        set_scalar(self.table, row, col, ScalarValue::Float64(value))
    }

    /// Set a scalar `bool` column by name.
    pub fn set_bool(&mut self, row: usize, col: &str, value: bool) -> MsResult<()> {
        set_scalar(self.table, row, col, ScalarValue::Bool(value))
    }

    /// Set a scalar `String` column by name.
    pub fn set_string(&mut self, row: usize, col: &str, value: impl Into<String>) -> MsResult<()> {
        set_scalar(self.table, row, col, ScalarValue::String(value.into()))
    }

    /// Set an array column by name.
    pub fn set_array(&mut self, row: usize, col: &str, value: ArrayValue) -> MsResult<()> {
        set_array(self.table, row, col, value)
    }
}

impl SubTable for MsSpectralWindowMut<'_> {
    fn id() -> SubtableId {
        SubtableId::SpectralWindow
    }
    fn required_columns() -> &'static [ColumnDef] {
        schema::spectral_window::REQUIRED_COLUMNS
    }
    fn optional_columns() -> &'static [ColumnDef] {
        schema::spectral_window::OPTIONAL_COLUMNS
    }
    fn table(&self) -> &Table {
        self.table
    }
}

fn extract_f64_vec(arr: &ArrayValue) -> MsResult<Vec<f64>> {
    match arr {
        ArrayValue::Float64(a) => Ok(a.iter().copied().collect()),
        other => Err(MsError::ColumnTypeMismatch {
            column: "array".to_string(),
            table: "SPECTRAL_WINDOW".to_string(),
            expected: "Float64 array".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;
    use casacore_types::{RecordField, RecordValue, ScalarValue, Value};
    use ndarray::ArrayD;

    fn make_spw_table() -> Table {
        let schema =
            build_table_schema(schema::spectral_window::REQUIRED_COLUMNS).expect("valid schema");
        Table::with_schema(schema)
    }

    fn make_f64_array(vals: &[f64]) -> ArrayValue {
        ArrayValue::Float64(ArrayD::from_shape_vec(vec![vals.len()], vals.to_vec()).unwrap())
    }

    #[test]
    fn read_spectral_window() {
        let mut table = make_spw_table();
        let freqs = [1.0e9, 1.001e9, 1.002e9, 1.003e9];
        let widths = [1.0e6; 4];

        let row = RecordValue::new(vec![
            RecordField::new("CHAN_FREQ", Value::Array(make_f64_array(&freqs))),
            RecordField::new("CHAN_WIDTH", Value::Array(make_f64_array(&widths))),
            RecordField::new("EFFECTIVE_BW", Value::Array(make_f64_array(&widths))),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new(
                "FREQ_GROUP_NAME",
                Value::Scalar(ScalarValue::String("Group0".to_string())),
            ),
            RecordField::new("IF_CONV_CHAIN", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(5))),
            RecordField::new(
                "NAME",
                Value::Scalar(ScalarValue::String("SPW0".to_string())),
            ),
            RecordField::new("NET_SIDEBAND", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("NUM_CHAN", Value::Scalar(ScalarValue::Int32(4))),
            RecordField::new("REF_FREQUENCY", Value::Scalar(ScalarValue::Float64(1.0e9))),
            RecordField::new("RESOLUTION", Value::Array(make_f64_array(&widths))),
            RecordField::new(
                "TOTAL_BANDWIDTH",
                Value::Scalar(ScalarValue::Float64(4.0e6)),
            ),
        ]);
        table.add_row(row).unwrap();

        let spw = MsSpectralWindow::new(&table);
        assert_eq!(spw.row_count(), 1);
        assert_eq!(spw.num_chan(0).unwrap(), 4);
        assert_eq!(spw.chan_freq(0).unwrap().len(), 4);
        assert_eq!(spw.ref_frequency(0).unwrap(), 1.0e9);
        assert_eq!(spw.total_bandwidth(0).unwrap(), 4.0e6);
        assert_eq!(spw.meas_freq_ref(0).unwrap(), 5);
        assert_eq!(spw.name(0).unwrap(), "SPW0");
    }
}
