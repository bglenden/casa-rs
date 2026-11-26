// SPDX-License-Identifier: LGPL-3.0-or-later
//! Optional computed input arrays for a MeasurementSet transformation.

use std::collections::BTreeMap;

use casa_core::ArrayValue;
use casa_tables::{Table, TableError};

use crate::{MeasurementSet, ResolvedMsSelectionRow};

/// A bounded, read-only source for columns computed from the input
/// MeasurementSet, for example calibrated visibilities.
///
/// Array shapes are the physical input shapes. The transform applies
/// correlation and channel selection after reading them. An implementation
/// returns one cell per requested physical row in request order and may
/// leave untouched column names to the ordinary table reader.
pub trait TransformInputView: std::fmt::Debug + Send + Sync {
    /// Arrange the selected rows in the input operator's publication order.
    /// The default keeps the order provided by the transform selection.
    fn order_output_rows(
        &self,
        _input: &MeasurementSet,
        _rows: &mut [ResolvedMsSelectionRow],
    ) -> Result<(), TableError> {
        Ok(())
    }

    /// Prepare the input view for the exact physical rows and channels used
    /// by one transform product. Called again for each Multi-MS partition.
    fn prepare<'a>(
        &self,
        input: &'a MeasurementSet,
        rows: &[ResolvedMsSelectionRow],
        channels: &BTreeMap<i32, &[usize]>,
    ) -> Result<Box<dyn PreparedTransformInputView + 'a>, TableError>;

    /// Additional transient bytes per input visibility sample while reading.
    fn additional_bytes_per_sample(&self) -> usize;

    /// Whether the view provides this column for the supplied physical
    /// input table even when the column does not exist on disk.
    fn provides(&self, input: &MeasurementSet, column: &str) -> bool;
}

/// Computed visibility arrays bound to one physical transform selection.
pub trait PreparedTransformInputView: Send + Sync {
    /// Materialize requested computed columns for a bounded block.
    fn read(
        &self,
        input: &Table,
        rows: &[usize],
        columns: &[&str],
    ) -> Result<TransformInputColumns, TableError>;
}

/// Optional input columns read for one transform block.
pub type TransformInputColumns = BTreeMap<&'static str, Vec<Option<ArrayValue>>>;
