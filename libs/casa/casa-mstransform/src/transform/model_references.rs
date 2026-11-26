// SPDX-License-Identifier: LGPL-3.0-or-later
//! Model-selection references passed from PlotMS callers to calibration prediction.

use std::collections::BTreeMap;

use casa_ms::selection::ChannelRange;

/// Iterator references for averaging virtual model cells. Both slot
/// slices are indexed by input selection slot and contain slots in that same
/// selection.
#[derive(Debug, Clone, Copy)]
pub struct PlotMsModelReferences<'a> {
    /// The first row of the iterator chunk supplies FIELD direction and epoch.
    pub phase_slots: &'a [usize],
    /// The first row of each input visibility buffer supplies the model recipe.
    pub recipe_slots: &'a [usize],
    /// Selected cells for aligning predictions with the first input buffer
    /// window. Only supplied for spectral-window averaging.
    pub spectral_windows: Option<PlotMsModelSpwSelection<'a>>,
}

/// Physical selection to align with the prediction buffer by cell position.
#[derive(Debug, Clone, Copy)]
pub struct PlotMsModelSpwSelection<'a> {
    /// Selected channel ranges in each physical spectral window.
    pub channels: &'a BTreeMap<i32, Vec<ChannelRange>>,
    /// Requested correlation types, or empty to select all physical slots.
    pub correlations: &'a [i32],
}
