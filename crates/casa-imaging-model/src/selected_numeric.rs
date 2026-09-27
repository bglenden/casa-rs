// SPDX-License-Identifier: LGPL-3.0-or-later

//! Borrowed source columns. Shape and selected axes belong to the block owner;
//! values have no execution, publication or per-sample transport state.

use num_complex::Complex32;

use crate::{CorrelationProduct, SelectedObservationRunChannel, SelectedObservationRunRow};

/// Stored visibility precision, selected once for a block.
#[derive(Clone, Copy)]
pub enum SelectedNumericVisibility<'a> {
    /// `FLOAT_DATA` values.
    Float32(&'a [f32]),
    /// `DATA` or `CORRECTED_DATA` values.
    Complex32(&'a [Complex32]),
}

/// Input weights, retaining the original broadcast representation.
#[derive(Clone, Copy)]
pub enum SelectedNumericWeights<'a> {
    /// One value per correlation for this row.
    PerRow(&'a [f32]),
    /// Channel/correlation values.
    PerChannel(&'a [f32]),
}

/// A complete selected row in channel/correlation order. The source owner
/// supplies exact selected axes; no scalar sample envelopes are constructed.
#[derive(Clone, Copy)]
pub struct SelectedNumericRow<'a> {
    /// Row-invariant coordinates and provenance.
    pub row: &'a SelectedObservationRunRow,
    /// Selected native channels, in canonical order.
    pub channels: &'a [SelectedObservationRunChannel],
    /// Selected correlation coordinates, in canonical order.
    pub correlations: &'a [CorrelationProduct],
    /// Physical first channel and stored correlation stride of the source row.
    pub first_stored_channel: u32,
    /// Number of contiguous stored channels, including any selection gaps.
    pub stored_channels: usize,
    /// Stored correlations per channel, including unselected correlations.
    pub stored_correlations: usize,
    /// Channel/correlation payload.
    pub visibility: SelectedNumericVisibility<'a>,
    /// Channel/correlation flags.
    pub flags: &'a [bool],
    /// Row-broadcast or channelized weights.
    pub weights: SelectedNumericWeights<'a>,
}

impl SelectedNumericRow<'_> {
    /// Check array lengths once at the row boundary, using overflow-safe shape arithmetic.
    pub fn has_exact_shape(self) -> bool {
        let Some(samples) = self.stored_channels.checked_mul(self.stored_correlations) else {
            return false;
        };
        samples != 0
            && !self.channels.is_empty()
            && !self.correlations.is_empty()
            && self.channels.iter().all(|channel| {
                channel
                    .channel_index
                    .checked_sub(self.first_stored_channel)
                    .is_some_and(|offset| (offset as usize) < self.stored_channels)
            })
            && self
                .correlations
                .iter()
                .all(|product| (product.correlation_index() as usize) < self.stored_correlations)
            && self.flags.len() == samples
            && match self.visibility {
                SelectedNumericVisibility::Float32(values) => values.len() == samples,
                SelectedNumericVisibility::Complex32(values) => values.len() == samples,
            }
            && match self.weights {
                SelectedNumericWeights::PerRow(values) => values.len() == self.stored_correlations,
                SelectedNumericWeights::PerChannel(values) => values.len() == samples,
            }
    }
}
