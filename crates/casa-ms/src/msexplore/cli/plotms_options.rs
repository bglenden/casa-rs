// SPDX-License-Identifier: LGPL-3.0-or-later
//! Effective MeasurementSet PlotMS columns and averaging controls.

use super::{CliOptions, MsAxis, MsDataColumn};

impl CliOptions {
    pub(super) fn plotms_axis_columns(&self) -> Option<[MsDataColumn; 3]> {
        (self.x_axis.is_some_and(MsAxis::is_visibility_math)
            || self.y_axis.is_some_and(MsAxis::is_visibility_math)
            || self.y_axis2.is_some())
        .then_some([
            self.x_data_column.unwrap_or(self.data_column),
            self.data_column,
            self.secondary_y_data_column.unwrap_or(self.data_column),
        ])
    }

    pub(super) fn plotms_averaging_active(&self) -> bool {
        self.avgchannel.is_some_and(|width| width > 1) || self.plotms_extended_averaging()
    }

    pub(super) fn plotms_extended_averaging(&self) -> bool {
        self.avgtime.is_some()
            || self.avgscan
            || self.avgfield
            || self.avgbaseline
            || self.avgantenna
            || self.avgspw
    }
}
