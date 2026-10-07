// SPDX-License-Identifier: LGPL-3.0-or-later
//! Owned native rows of one bounded source block.

use casa_imaging_operator::{NativeRow, RowContext};
use num_complex::Complex32;

/// Where a row lives in its MeasurementSet, for the model-column writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowAddress {
    /// Physical MAIN row.
    pub physical_row: u64,
    /// MAIN `DATA_DESC_ID`.
    pub data_description: i32,
}

/// Row-shared values of one native row.
#[derive(Clone, Copy, Debug)]
pub struct NativeRowHeader {
    /// Baseline `[u, v, w]` in metres in the image phase-centre frame.
    pub uvw_m: [f64; 3],
    /// Path-length shift to the image phase centre in metres.
    pub phase_shift_m: f64,
    /// Row flag (MAIN `FLAG_ROW`, or a row CASA excludes from imaging).
    pub row_flag: bool,
    /// Per-row context for kernel-set cell selection.
    pub context: RowContext,
    /// MeasurementSet address.
    pub address: RowAddress,
}

/// One block of native rows sharing a channel and correlation layout.
///
/// Arrays are `[row][channel][correlation]`, correlation fastest, over the
/// selected channels and correlations in the operator's routing order.
/// Frequencies are per row and channel because Doppler tracking moves them.
/// Capacity survives [`NativeBlock::clear`], so a source allocates once.
#[derive(Clone, Debug, Default)]
pub struct NativeBlock {
    rows: Vec<NativeRowHeader>,
    channel_indices: Vec<u32>,
    correlation_indices: Vec<u32>,
    frequencies_hz: Vec<f64>,
    values: Vec<Complex32>,
    weights: Vec<f32>,
    flags: Vec<bool>,
}

impl NativeBlock {
    /// Empty the block and set the layout of the rows that follow:
    /// the native channel and correlation indices every row selects.
    pub fn reset(&mut self, channel_indices: &[u32], correlation_indices: &[u32]) {
        assert!(
            !channel_indices.is_empty() && !correlation_indices.is_empty(),
            "a native block selects at least one channel and correlation"
        );
        self.rows.clear();
        self.frequencies_hz.clear();
        self.values.clear();
        self.weights.clear();
        self.flags.clear();
        self.channel_indices.clear();
        self.channel_indices.extend_from_slice(channel_indices);
        self.correlation_indices.clear();
        self.correlation_indices
            .extend_from_slice(correlation_indices);
    }

    /// Append one row: `frequencies_hz` per channel, `values`, `weights` and
    /// `flags` per channel and correlation.
    pub fn push_row(
        &mut self,
        header: NativeRowHeader,
        frequencies_hz: &[f64],
        values: &[Complex32],
        weights: &[f32],
        flags: &[bool],
    ) {
        let cells = self.channels() * self.correlations();
        assert_eq!(frequencies_hz.len(), self.channels(), "frequencies per row");
        assert!(
            values.len() == cells && weights.len() == cells && flags.len() == cells,
            "values, weights and flags per row must be channels × correlations"
        );
        self.rows.push(header);
        self.frequencies_hz.extend_from_slice(frequencies_hz);
        self.values.extend_from_slice(values);
        self.weights.extend_from_slice(weights);
        self.flags.extend_from_slice(flags);
    }

    /// Number of rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the block holds no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Selected channels per row.
    #[must_use]
    pub fn channels(&self) -> usize {
        self.channel_indices.len()
    }

    /// Selected correlations per row.
    #[must_use]
    pub fn correlations(&self) -> usize {
        self.correlation_indices.len()
    }

    /// Native channel index of each selected channel.
    #[must_use]
    pub fn channel_indices(&self) -> &[u32] {
        &self.channel_indices
    }

    /// Correlation index of each selected correlation.
    #[must_use]
    pub fn correlation_indices(&self) -> &[u32] {
        &self.correlation_indices
    }

    /// Header of row `index`.
    #[must_use]
    pub fn header(&self, index: usize) -> &NativeRowHeader {
        &self.rows[index]
    }

    /// Row `index` as the operator reads it.
    #[must_use]
    pub fn row(&self, index: usize) -> NativeRow<'_> {
        let header = &self.rows[index];
        let channels = self.channels();
        let cells = channels * self.correlations();
        NativeRow {
            uvw_m: header.uvw_m,
            phase_shift_m: header.phase_shift_m,
            frequencies_hz: &self.frequencies_hz[index * channels..(index + 1) * channels],
            values: &self.values[index * cells..(index + 1) * cells],
            weights: &self.weights[index * cells..(index + 1) * cells],
            flags: &self.flags[index * cells..(index + 1) * cells],
            row_flag: header.row_flag,
            context: header.context,
        }
    }
}
