// SPDX-License-Identifier: LGPL-3.0-or-later
//! Effective MeasurementSet PlotMS columns and averaging controls.

use std::path::PathBuf;

use casa_provider_contracts::ParameterValue;
use casa_task_runtime::{TaskFailure, TaskInvocation};
use casa_types::measures::direction::MDirection;

use super::super::{
    RawSymbolVisibility, calibration, plotms_columns, plotms_figure::VisibilityAxisColumns,
    plotms_phasecenter, plotms_pointing::PointingAxes, plotms_row_axes,
};
use super::{
    CliOptions, MeasurementSet, MeasurementSetSummaryOutputFormat, MsAxis, MsColorAxis,
    MsDataColumn, MsExportFormat, MsIterationAxis, MsLegendPosition, MsSelection, count,
    legend_position, number,
};

impl CliOptions {
    pub(super) fn has_independent_ordinates(&self) -> bool {
        self.secondary_y_data_column.is_some() || !self.additional_ordinates.is_empty()
    }

    pub(super) fn has_mixed_ordinate_dimensions(&self) -> bool {
        self.x_axis.is_some_and(MsAxis::uses_derived_geometry)
            || self
                .y_axis
                .into_iter()
                .chain(self.y_axis2)
                .chain(self.additional_ordinates.iter().map(|&(axis, _)| axis))
                .any(|axis| !axis.is_visibility_math())
    }

    pub(super) fn uses_channel_ordinate_masks(&self) -> bool {
        self.x_axis.is_some_and(|x_axis| {
            plotms_columns::uses_channel_ordinate_masks(
                x_axis,
                self.y_axis
                    .into_iter()
                    .chain(self.y_axis2)
                    .chain(self.additional_ordinates.iter().map(|&(axis, _)| axis)),
            )
        })
    }

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

    pub(super) fn prepare_pointing_axes(
        &mut self,
        task: &TaskInvocation<'_>,
        pointing: &PointingAxes,
        has_calibration_or_overlays: bool,
    ) -> Result<(), TaskFailure> {
        let x = self.x_axis.expect("PlotMS resolves the primary X axis");
        let y = self.y_axis.expect("PlotMS resolves the primary Y axis");
        let metadata_axis =
            |axis: MsAxis| axis.is_pointing() || matches!(axis, MsAxis::Time | MsAxis::Channel);
        let metadata_pair = metadata_axis(x) && metadata_axis(y);
        let real_pair = x.is_pointing() && y == MsAxis::Real;
        let single_metadata_pair = metadata_pair && pointing.len() == 1;
        let averaging = self.plotms_averaging_active();
        if !(metadata_pair || real_pair)
            || self.y_axis2.is_some()
            || has_calibration_or_overlays
            || raw_symbol_visibility(task).flagged
            || (self.iteraxis.is_some()
                && !(self.iteraxis == Some(MsIterationAxis::Scan) && single_metadata_pair))
            || (averaging
                && (!single_metadata_pair
                    || self.avgscan
                    || self.avgfield
                    || self.avgbaseline
                    || self.avgantenna
                    || self.avgspw
                    || self.scalar))
        {
            return Err(TaskFailure::validation(
                "This PlotMS pointing axis, averaging, iteration, callibrary, overlay or flag-symbol combination is pending (#1032)",
            ));
        }
        if averaging {
            calibration::averaging::options(task)?;
            // CASA's metadata-only iterator requests no visibility column. It reports
            // the requested averaging in the export but retains the selected raw grid.
            self.avgtime = None;
            self.avgchannel = None;
        }
        if pointing.len() > 1 {
            self.y_axis2 = self.y_axis;
            self.secondary_y_data_column = Some(self.data_column);
            self.additional_ordinates.extend(
                (2..pointing.len())
                    .map(|_| (self.y_axis.expect("pointing ordinate"), self.data_column)),
            );
        }
        Ok(())
    }

    pub(super) fn figure_axis_columns<'a>(
        &'a self,
        columns: Option<[MsDataColumn; 3]>,
        calibrated: bool,
        pointing: Option<&'a PointingAxes>,
    ) -> Option<VisibilityAxisColumns<'a>> {
        columns
            .or_else(|| {
                let (x, y) = self.x_axis.zip(self.y_axis)?;
                (pointing.is_some() || plotms_row_axes::metadata_coordinate_pair(x, y).is_some())
                    .then_some([self.data_column; 3])
            })
            .filter(|_| !calibrated || self.secondary_y_data_column.is_some())
            .map(|base| VisibilityAxisColumns {
                base,
                secondary_axis: self.y_axis2,
                additional: &self.additional_ordinates,
                right_ordinates: pointing
                    .map_or(&self.right_ordinates, PointingAxes::right_ordinates),
                pointing,
            })
    }

    pub(super) fn plotms_averaging_active(&self) -> bool {
        self.avgchannel.is_some_and(|width| width > 1) || self.plotms_extended_averaging()
    }

    fn supports_effective_float_averaging(&self) -> bool {
        let channel = self.avgchannel.is_some_and(|width| width > 1);
        let time = self.avgtime.is_some();
        (channel || time)
            && (!self.avgspw || (channel && !time))
            && !(self.avgbaseline || self.avgantenna || self.avgscan || self.avgfield)
            && self.iteraxis.is_none()
            && self.freqframe.is_none()
            && self.y_axis2.is_none()
            && self.additional_ordinates.is_empty()
            && self
                .y_axis
                .is_some_and(|axis| matches!(axis, MsAxis::Amplitude | MsAxis::Real))
            && self.x_axis.is_some_and(|axis| {
                matches!(axis, MsAxis::Time | MsAxis::Channel | MsAxis::Frequency)
            })
    }

    pub(super) fn resolve_visibility_storage(
        &mut self,
        ms: &MeasurementSet,
        columns: &mut [MsDataColumn; 3],
        implicit_derived_model: bool,
        phasecenter: bool,
    ) -> Result<bool, TaskFailure> {
        let axes = [self.x_axis, self.y_axis, self.y_axis2];
        let (adjusted, use_float) = plotms_columns::resolve_storage_columns(
            ms,
            *columns,
            axes,
            self.plotms_averaging_active(),
            self.supports_effective_float_averaging(),
        )?;
        if let Some((storage, axis)) = self.derived_average_storage(*columns, adjusted) {
            plotms_columns::check_averaged_derived_storage(
                ms,
                storage,
                axis,
                implicit_derived_model,
            )?;
        }
        if phasecenter
            && use_float
            && axes.into_iter().zip(adjusted).any(|(axis, column)| {
                axis.is_some_and(MsAxis::is_visibility_math)
                    && matches!(column, MsDataColumn::Data | MsDataColumn::Float)
            })
        {
            return Err(TaskFailure::validation(
                "PlotMS phasecenter with MeasurementSet FLOAT_DATA is pending (#1032)",
            )
            .at("phasecenter"));
        }
        if adjusted != *columns {
            let [x, y, secondary_y] = adjusted;
            self.data_column = y;
            self.x_data_column = (x != y).then_some(x);
            if self.secondary_y_data_column.is_some() {
                self.secondary_y_data_column = Some(secondary_y);
            }
            *columns = adjusted;
        }
        Ok(use_float)
    }

    pub(super) fn supports_averaged_ordinate_iteration(&self) -> bool {
        self.iteraxis
            .is_some_and(MsIterationAxis::supports_averaged_ordinates)
            && self.freqframe.is_none()
            && self.x_axis.is_some_and(|axis| {
                axis.is_visibility_math()
                    || matches!(axis, MsAxis::Time | MsAxis::Channel | MsAxis::Frequency)
            })
            && (self.supports_visibility_ordinate_averaging(true)
                || (self.avgspw
                    && !self.avgscan
                    && !self.avgfield
                    && self.supports_visibility_ordinate_averaging(false)))
    }

    pub(super) fn supports_calibrated_ordinates(&self) -> bool {
        self.secondary_y_data_column.is_some()
            && self.additional_ordinates.is_empty()
            && self.x_axis == Some(MsAxis::Channel)
            && !self.has_mixed_ordinate_dimensions()
            && !self.plotms_extended_averaging()
            && !self.scalar
            && self.freqframe.is_none()
            && matches!(self.iteraxis, None | Some(MsIterationAxis::Scan))
            && [Some(self.data_column), self.secondary_y_data_column]
                .into_iter()
                .flatten()
                .all(|column| matches!(column, MsDataColumn::Data | MsDataColumn::Corrected))
    }

    pub(super) fn validate_plotms_columns(
        &self,
        task: &TaskInvocation<'_>,
        has_callibrary: bool,
        calibrated_ordinates: bool,
        has_atmosphere: bool,
        iterated_averaging: bool,
    ) -> Result<(), TaskFailure> {
        let columns = self.plotms_axis_columns();
        let row_identifier_axes = [self.x_axis, self.y_axis]
            .into_iter()
            .flatten()
            .any(MsAxis::is_row_identifier);
        let uncalibrated_columns = !has_callibrary && (columns.is_some() || row_identifier_axes);
        if uncalibrated_columns && let Some(columns) = columns {
            plotms_columns::check_averaging(
                self.plotms_averaging_active(),
                self.plotms_extended_averaging()
                    && self.secondary_y_data_column.is_some()
                    && !iterated_averaging
                    && !self.plotms_independent_average(
                        !has_atmosphere && (self.avgspw || !self.additional_ordinates.is_empty()),
                    ),
                self.supports_channel_derived(columns) || self.supports_extended_derived(columns),
                columns,
            )?;
        }
        if uncalibrated_columns || calibrated_ordinates {
            let symbols = task.value("customflaggedsymbol");
            let ordinate_count =
                1 + usize::from(self.y_axis2.is_some()) + self.additional_ordinates.len();
            if (0..ordinate_count).any(|index| calibration::figure::shows_flagged(symbols, index))
                && (calibrated_ordinates
                    || !columns.is_some_and(|columns| self.supports_visibility_symbols(columns))
                        && !self.supports_metadata_flag_symbols())
            {
                return Err(TaskFailure::validation(
                    "displaying flagged points for this MeasurementSet column plot is pending #1032",
                )
                .at("customflaggedsymbol"));
            }
        }
        Ok(())
    }

    pub(super) fn supports_visibility_ordinate_averaging(&self, ordinary_only: bool) -> bool {
        let spans = self.avgscan || self.avgfield;
        let time_average = self.avgtime.is_some()
            && (!ordinary_only || !spans)
            && !self.avgbaseline
            && !self.avgantenna;
        let other_reduction = !ordinary_only
            && ((self.avgantenna && (!spans || self.avgtime.is_some()))
                || ((self.avgbaseline || self.avgspw) && !spans));
        let supported_reduction = (time_average || other_reduction)
            && self.freqframe.is_none()
            && self.plotms_independent_average(!ordinary_only);
        let colors_spanned_antenna = self.avgantenna
            && !self.scalar
            && ((self.avgscan && self.color_by == MsColorAxis::Scan)
                || (self.avgfield && self.color_by == MsColorAxis::Field));
        (!self.plotms_extended_averaging() || supported_reduction)
            && !colors_spanned_antenna
            && !self.has_mixed_ordinate_dimensions()
            && self.plotms_axis_columns().is_some_and(|columns| {
                columns
                    .into_iter()
                    .chain(self.additional_ordinates.iter().map(|&(_, column)| column))
                    .all(|column| {
                        matches!(
                            column,
                            MsDataColumn::Data | MsDataColumn::Corrected | MsDataColumn::Model
                        )
                    })
            })
    }

    pub(super) fn supports_channel_derived(&self, [x, y, _]: [MsDataColumn; 3]) -> bool {
        let supported_pair = |derived: MsDataColumn, stored| {
            derived.is_derived() && matches!(stored, MsDataColumn::Data | MsDataColumn::Corrected)
        };
        let supported_abscissa = self.x_axis.is_some_and(|axis| {
            if axis.is_visibility_math() {
                supported_pair(x, y) || supported_pair(y, x)
            } else {
                matches!(axis, MsAxis::Channel | MsAxis::Time) && y.is_derived()
            }
        });
        self.avgchannel.is_some_and(|width| width > 1)
            && !self.plotms_extended_averaging()
            && supported_abscissa
            && self.y_axis.is_some_and(MsAxis::is_visibility_math)
            && self.y_axis2.is_none()
            && self.iteraxis.is_none()
    }

    pub(super) fn supports_extended_derived(&self, [x, y, _]: [MsDataColumn; 3]) -> bool {
        let channel_average = self.avgchannel.is_some_and(|width| width > 1);
        let supported_average = if self.avgantenna || self.avgspw {
            self.avgtime.is_some()
                && !self.avgbaseline
                && !(self.avgantenna && self.avgspw)
                && !self.avgscan
                && !self.avgfield
                && self.freqframe.is_none()
        } else if self.avgbaseline {
            self.scalar
        } else {
            self.avgtime.is_some()
        };
        let visibility_column = |column: MsDataColumn| {
            column.is_derived()
                || matches!(
                    column,
                    MsDataColumn::Data | MsDataColumn::Corrected | MsDataColumn::Model
                )
        };
        let supported_abscissa = self.x_axis.is_some_and(|axis| {
            if axis == MsAxis::Time || ((channel_average || self.scalar) && axis == MsAxis::Channel)
            {
                y.is_derived()
            } else {
                axis.is_visibility_math()
                    && visibility_column(x)
                    && visibility_column(y)
                    && (x.is_derived() || y.is_derived())
            }
        });
        supported_abscissa
            && self.y_axis.is_some_and(MsAxis::is_visibility_math)
            && self.y_axis2.is_none()
            && self.iteraxis.is_none()
            && supported_average
    }

    pub(super) fn derived_average_storage(
        &self,
        requested: [MsDataColumn; 3],
        adjusted: [MsDataColumn; 3],
    ) -> Option<([MsDataColumn; 3], &'static str)> {
        if self.supports_channel_derived(requested) {
            Some((adjusted, "channel"))
        } else if self.supports_extended_derived(requested) {
            let axis = if self.scalar && self.avgbaseline {
                "scalar baseline"
            } else if self.scalar {
                "scalar time"
            } else if self.avgchannel.is_some_and(|width| width > 1) {
                "time- and channel"
            } else {
                "time"
            };
            Some((adjusted, axis))
        } else {
            None
        }
    }

    fn supports_visibility_symbols(&self, columns: [MsDataColumn; 3]) -> bool {
        let reductions = [self.avgtime.is_some(), self.avgantenna, self.avgspw];
        let unbinned_reduction = reductions.into_iter().filter(|&enabled| enabled).count() == 1
            && !(self.avgscan || self.avgfield || self.avgbaseline)
            && self.avgchannel.is_none_or(|width| width <= 1)
            && self.freqframe.is_none();
        self.x_axis.is_some_and(MsAxis::is_visibility_math)
            && self.y_axis.is_some_and(MsAxis::is_visibility_math)
            && self.y_axis2.is_none()
            && (!self.plotms_extended_averaging() || unbinned_reduction)
            && (!self.scalar || self.avgchannel.is_none_or(|width| width <= 1))
            && self.iteraxis.is_none()
            && self.color_by == MsColorAxis::None
            && columns[..2]
                .iter()
                .all(|column| matches!(column, MsDataColumn::Data | MsDataColumn::Corrected))
    }

    fn supports_metadata_flag_symbols(&self) -> bool {
        self.x_axis == Some(MsAxis::Channel)
            && matches!(self.y_axis, Some(MsAxis::Row | MsAxis::Elevation))
            && self.y_axis2.is_none()
            && !self.plotms_averaging_active()
            && self.iteraxis.is_none()
            && self.color_by == MsColorAxis::None
    }

    pub(super) fn plotms_symbol_visibility(
        &self,
        task: &TaskInvocation<'_>,
        columns: Option<[MsDataColumn; 3]>,
        calibrated: bool,
        pointing: bool,
    ) -> Option<RawSymbolVisibility> {
        let visibility = pointing
            || columns.is_some_and(|columns| self.supports_visibility_symbols(columns))
                && (!calibrated || !self.plotms_averaging_active());
        let metadata = !calibrated && self.supports_metadata_flag_symbols();
        (visibility || metadata).then(|| raw_symbol_visibility(task))
    }

    pub(super) fn plotms_phasecenter(
        &mut self,
        value: &str,
        without_calibration_or_overlays: bool,
    ) -> Result<MDirection, TaskFailure> {
        let complex_visibility = |axis: MsAxis, column: MsDataColumn| {
            axis.is_visibility_math()
                && matches!(
                    column,
                    MsDataColumn::Data | MsDataColumn::Corrected | MsDataColumn::Model
                )
        };
        let x = self.x_axis.is_some_and(|axis| {
            axis.uses_uvw_metres()
                || complex_visibility(axis, self.x_data_column.unwrap_or(self.data_column))
        });
        let channels = self.avgchannel.is_some_and(|width| width > 1);
        let time = self.avgtime.is_some();
        if channels && time {
            return Err(TaskFailure::validation(
                "PlotMS phasecenter with simultaneous time and channel averaging is refused by CASA 6.7.6.14",
            )
            .at("phasecenter"));
        }
        if !x
            || !self
                .y_axis
                .is_some_and(|axis| complex_visibility(axis, self.data_column))
            || !without_calibration_or_overlays
            || self.y_axis2.is_some()
            || !self.additional_ordinates.is_empty()
            || self.avgscan
            || self.avgfield
            || self.avgspw
            || self.avgbaseline
            || self.avgantenna
            || (self.scalar && time)
            || !matches!(self.iteraxis, None | Some(MsIterationAxis::Scan))
            || ((channels || time) && (self.iteraxis.is_some() || self.freqframe.is_some()))
        {
            return Err(TaskFailure::validation(
                "This combination of PlotMS phasecenter with axes, averaging, iteration, callibrary or overlays is pending (#1032)",
            )
            .at("phasecenter"));
        }
        // PlotMS ignores scalar for both raw and channel-only requests.
        self.scalar = false;
        plotms_phasecenter::parse(value)
    }

    pub(super) fn plotms_independent_average(&self, allow_extended_list: bool) -> bool {
        let transforms_frequency =
            self.x_axis == Some(MsAxis::Frequency) && self.freqframe.is_some();
        let crosses_boundary = self.avgscan || self.avgfield;
        let time_average = self.avgtime.is_some()
            && !self.avgbaseline
            && !self.avgantenna
            && !(transforms_frequency && crosses_boundary);
        let baseline_average = self.avgbaseline
            && !self.avgantenna
            && (!crosses_boundary || self.scalar)
            && !transforms_frequency;
        let list_reduction = allow_extended_list
            && !self.avgbaseline
            && (!crosses_boundary || self.avgtime.is_some())
            && self.freqframe.is_none();
        let antenna_average = list_reduction && self.avgantenna;
        let spectral_window_average = list_reduction && self.avgspw && !self.avgantenna;
        let channel_average = self.avgchannel.is_some_and(|width| width > 1);
        let ordinary_time = time_average
            && !self.avgspw
            && !transforms_frequency
            && !crosses_boundary
            && !channel_average;
        let spw_time_u = spectral_window_average
            && self.avgtime.is_some()
            && !crosses_boundary
            && !channel_average
            && !self.scalar
            && self.iteraxis.is_none();
        let scalar_time = time_average && !transforms_frequency;
        (spectral_window_average
            || (!self.avgspw
                && (time_average || baseline_average || antenna_average)
                && (!self.scalar || scalar_time || baseline_average || antenna_average)))
            && !self.has_mixed_ordinate_dimensions()
            && self.x_axis.is_some_and(|axis| {
                axis.is_visibility_math()
                    || matches!(axis, MsAxis::Time | MsAxis::Channel | MsAxis::Frequency)
                    || (ordinary_time && !self.scalar && axis.uses_uvw_metres())
                    || (spw_time_u && axis == MsAxis::U)
            })
    }

    pub(super) fn plotms_extended_averaging(&self) -> bool {
        self.avgtime.is_some()
            || self.avgscan
            || self.avgfield
            || self.avgbaseline
            || self.avgantenna
            || self.avgspw
    }

    fn validate_plotms_weight_averaging(&self) -> Result<(), TaskFailure> {
        if (self.avgbaseline || self.avgantenna || self.avgspw || self.scalar)
            && [self.x_axis, self.y_axis, self.y_axis2]
                .into_iter()
                .flatten()
                .chain(self.additional_ordinates.iter().map(|&(axis, _)| axis))
                .any(MsAxis::is_weight_axis)
        {
            return Err(TaskFailure::validation(
                "Selected averaging does not yet support Weight and Sigma axes.",
            ));
        }
        Ok(())
    }

    fn validate_geometry_controls(&self, task: &TaskInvocation<'_>) -> Result<(), TaskFailure> {
        if self.y_axis.is_some_and(MsAxis::is_ephemeris) {
            let symbols = raw_symbol_visibility(task);
            if symbols.flagged || !symbols.unflagged {
                return Err(TaskFailure::validation(
                    "PlotMS flag-symbol filtering with ephemeris axes is pending (#1032)",
                ));
            }
        }
        let antenna_horizontal = [self.x_axis, self.y_axis, self.y_axis2]
            .into_iter()
            .flatten()
            .chain(self.additional_ordinates.iter().map(|(axis, _)| *axis))
            .any(MsAxis::is_antenna_horizontal);
        if antenna_horizontal {
            let symbols = raw_symbol_visibility(task);
            if self.x_axis != Some(MsAxis::Channel)
                || !self.y_axis.is_some_and(MsAxis::is_antenna_horizontal)
                || self.y_axis2.is_some()
                || self.plotms_averaging_active()
                || self.iteraxis.is_some()
                || symbols.flagged
                || !symbols.unflagged
            {
                return Err(TaskFailure::validation(
                    "This PlotMS antenna horizontal axis combination is pending (#1032)",
                ));
            }
            return Ok(());
        }
        let Some((x, y)) = self
            .x_axis
            .zip(self.y_axis)
            .filter(|_| self.y_axis2.is_none())
        else {
            return Ok(());
        };
        let array_horizontal = plotms_row_axes::array_horizontal_pair(x, y);
        let static_coordinates = plotms_row_axes::metadata_coordinate_pair(x, y).is_some();
        if !array_horizontal && !static_coordinates {
            return Ok(());
        }
        let symbols = raw_symbol_visibility(task);
        if (symbols.flagged || !symbols.unflagged)
            && (!self.supports_metadata_flag_symbols() || !task.texts("callib")?.is_empty())
        {
            let label = if array_horizontal {
                "array horizontal axes"
            } else {
                "metadata coordinate axes"
            };
            return Err(TaskFailure::validation(format!(
                "PlotMS flag-symbol filtering with {label} is pending (#1032)"
            )));
        }
        Ok(())
    }
}

pub(super) fn raw_symbol_visibility(task: &TaskInvocation<'_>) -> RawSymbolVisibility {
    let is_custom = |name| calibration::figure::shows_flagged(task.value(name), 0);
    let has_symbol = |name| {
        let shape = match task.value(name) {
            Some(ParameterValue::Array(values)) => values.first(),
            other => other,
        };
        !matches!(shape, Some(ParameterValue::String(value)) if value == "nosymbol")
    };
    RawSymbolVisibility {
        unflagged: !is_custom("customsymbol") || has_symbol("symbolshape"),
        flagged: is_custom("customflaggedsymbol") && has_symbol("flaggedsymbolshape"),
    }
}

fn right_ordinates(task: &TaskInvocation<'_>) -> Result<Vec<bool>, TaskFailure> {
    let supplied = task.value("yaxislocation");
    let names = calibration::axis_names(supplied).map_err(TaskFailure::validation)?;
    let sides = names
        .into_iter()
        .map(|name| match name {
            "left" => Ok(false),
            "right" => Ok(true),
            _ => Err(
                TaskFailure::validation(format!("invalid yaxislocation value '{name}'"))
                    .at("yaxislocation"),
            ),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let is_list = |value: Option<&ParameterValue>| matches!(value, Some(ParameterValue::Array(_)));
    if is_list(supplied) == is_list(task.value("yaxis")) {
        Ok(sides)
    } else {
        Ok(Vec::new())
    }
}

fn requested_ordinates(
    task: &TaskInvocation<'_>,
    x_axis: MsAxis,
) -> Result<Vec<MsAxis>, TaskFailure> {
    let y_value = task.value("yaxis");
    let mut ordinates = calibration::axis_names(y_value)
        .map_err(TaskFailure::validation)?
        .into_iter()
        .map(|axis| MsAxis::parse(&axis.to_ascii_lowercase()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(TaskFailure::validation)?;
    if matches!(y_value, Some(ParameterValue::Array(_)))
        && !ordinates.is_empty()
        && (x_axis.is_pointing() || ordinates.iter().any(|axis| axis.is_pointing()))
    {
        return Err(TaskFailure::validation(
            "currently not supported: multiple y-axes involving ant-ra or ant-dec",
        )
        .at("yaxis"));
    }
    if ordinates.is_empty() {
        ordinates.push(MsAxis::Amplitude);
    }
    Ok(ordinates)
}

/// Translate the CASA PlotMS spellings to the existing MS exploration request.
pub(super) fn options_from_plotms(
    task: &TaskInvocation<'_>,
    visibility_document: bool,
) -> Result<(CliOptions, bool), TaskFailure> {
    let text = |name| {
        task.text(name)
            .map(|text| text.filter(|value| !value.is_empty()))
    };
    let plot_output = text("plotfile")?.map(PathBuf::from);
    let export_format = text("expformat")?.or_else(|| {
        plot_output
            .as_deref()
            .and_then(std::path::Path::extension)
            .and_then(std::ffi::OsStr::to_str)
            .map(str::to_owned)
    });
    let x_axis = text("xaxis")?
        .map_or(Ok(MsAxis::Time), |axis| {
            MsAxis::parse(&axis.to_ascii_lowercase())
        })
        .map_err(TaskFailure::validation)?;
    let ordinates = requested_ordinates(task, x_axis)?;
    let y_axis = ordinates[0];
    let y_axis2 = ordinates.get(1).copied();
    let averaging = task.flag("averagedata")?;
    let average = |name| Ok::<_, TaskFailure>(averaging && task.flag(name)?);
    let average_text = |name| if averaging { text(name) } else { Ok(None) };
    let data_column = |column: Option<String>| {
        column
            .map(|column| MsDataColumn::parse(&column))
            .transpose()
            .map_err(TaskFailure::validation)
    };
    let x_column = data_column(text("xdatacolumn")?)?.unwrap_or(MsDataColumn::Data);
    let y_columns = plotms_columns::ordinate_columns(task, &ordinates)?;
    let y_column = y_columns[0];
    let secondary_y = y_columns.get(1).copied().unwrap_or(MsDataColumn::Data);
    let additional_ordinates = ordinates.into_iter().zip(y_columns).skip(2).collect();
    let weight_axis_columns = plotms_columns::weight_axis_columns(
        [Some(x_axis), Some(y_axis), y_axis2],
        [x_column, y_column, secondary_y],
    );
    let secondary_uses_data = y_axis2.is_some_and(MsAxis::is_visibility_math);
    let data_column = if y_axis.is_visibility_math() {
        y_column
    } else if x_axis.is_visibility_math() {
        x_column
    } else if secondary_uses_data {
        secondary_y
    } else {
        MsDataColumn::Data
    };
    let x_data_column =
        (x_axis.is_visibility_math() && x_column != data_column).then_some(x_column);
    let secondary_y = if secondary_uses_data {
        secondary_y
    } else {
        data_column
    };
    let secondary_y_data_column =
        (y_axis2 == Some(y_axis) || secondary_y != data_column).then_some(secondary_y);
    let avgtime = average_text("avgtime")?
        .map(|value| number::<f64>(&value))
        .transpose()
        .map_err(TaskFailure::validation)?;
    let document_axes = x_axis.is_visibility_math()
        || y_axis.is_visibility_math()
        || (avgtime.is_some_and(|interval| interval != 0.0)
            && [x_axis, y_axis].contains(&MsAxis::WeightSpectrum))
        || plotms_row_axes::array_horizontal_pair(x_axis, y_axis)
        || plotms_row_axes::metadata_coordinate_pair(x_axis, y_axis).is_some()
        || y_axis.is_antenna_horizontal()
        || x_axis.is_ephemeris()
        || y_axis.is_ephemeris()
        // PlotMS falls back to scalar SIGMA for undefined spectral cells;
        // the raw explorer deliberately requires the requested stored cell.
        || x_axis == MsAxis::SigmaSpectrum
        || y_axis == MsAxis::SigmaSpectrum
        || y_axis2.is_some()
        || x_axis.is_row_identifier()
        || y_axis.is_row_identifier()
        || x_axis.is_pointing()
        || y_axis.is_pointing();
    let visibility_document = visibility_document || document_axes;
    let plot_format = if visibility_document {
        MsExportFormat::Txt
    } else {
        export_format
            .as_deref()
            .map(MsExportFormat::parse)
            .transpose()
            .map_err(|_| TaskFailure::validation("MeasurementSet figure export is pending #1032"))?
            .unwrap_or(MsExportFormat::Txt)
    };
    let color_by = text("coloraxis")?
        .map_or(Ok(MsColorAxis::None), |axis| {
            MsColorAxis::parse(&axis.to_ascii_lowercase())
        })
        .map_err(TaskFailure::validation)?;
    let correlation = text("correlation")?.or(text("polarization")?);
    let transform = task.flag("transform")?;
    let options = CliOptions {
        ms_path: task.path("vis")?,
        summary_format: MeasurementSetSummaryOutputFormat::Text,
        summary_output: None,
        overwrite: task.flag("overwrite")?,
        selection: MsSelection {
            selectdata: task.flag("selectdata")?,
            field: text("field")?,
            spw: text("spw")?,
            timerange: text("timerange")?,
            uvrange: text("uvrange")?,
            antenna: text("antenna")?,
            scan: text("scan")?,
            correlation,
            observation: text("observation")?,
            array: text("array")?,
            intent: text("intent")?,
            feed: text("feed")?,
            msselect: text("msselect")?,
            ..MsSelection::default()
        },
        page_spec: None,
        preset: None,
        x_axis: Some(x_axis),
        y_axis: Some(y_axis),
        y_axis2,
        data_column,
        x_data_column,
        secondary_y_data_column,
        additional_ordinates,
        right_ordinates: right_ordinates(task)?,
        weight_axis_columns,
        color_by,
        avgchannel: average_text("avgchannel")?
            .map(|value| number::<usize>(&value))
            .transpose()
            .map_err(TaskFailure::validation)?
            .filter(|&width| !document_axes || width != 0),
        avgtime: avgtime.filter(|&interval| !document_axes || interval != 0.0),
        avgscan: average("avgscan")?,
        avgfield: average("avgfield")?,
        avgbaseline: average("avgbaseline")?,
        avgantenna: average("avgantenna")?,
        avgspw: average("avgspw")?,
        scalar: average("scalar")?,
        freqframe: if transform { text("freqframe")? } else { None },
        restfreq: if transform { text("restfreq")? } else { None },
        veldef: if transform {
            task.required_text("veldef")?
        } else {
            "RADIO".into()
        },
        iteraxis: text("iteraxis")?
            .map(|value| MsIterationAxis::parse(&value.to_ascii_lowercase()))
            .transpose()
            .map_err(TaskFailure::validation)?,
        gridrows: count(task, "gridrows")?.max(1),
        gridcols: count(task, "gridcols")?.max(1),
        xselfscale: task.flag("xselfscale")?,
        yselfscale: task.flag("yselfscale")?,
        xsharedaxis: task.flag("xsharedaxis")?,
        ysharedaxis: task.flag("ysharedaxis")?,
        title: text("title")?,
        xlabel: text("xlabel")?,
        ylabel: text("ylabel")?,
        symbol_size: task
            .optional_number("symbolsize")?
            .map(|value| value as u32),
        showlegend: task.flag("showlegend")?,
        legendposition: text("legendposition")?
            .map_or(Ok(MsLegendPosition::UpperRight), |value| {
                legend_position(&value)
            })
            .map_err(TaskFailure::validation)?,
        showmajorgrid: task.flag("showmajorgrid")?,
        showminorgrid: task.flag("showminorgrid")?,
        headeritems: text("headeritems")?,
        max_points: usize::MAX,
        flag_action: None,
        flag_xmin: None,
        flag_xmax: None,
        flag_ymin: None,
        flag_ymax: None,
        flag_plotindex: None,
        flag_panel: None,
        flag_extcorr: false,
        flag_extchannel: false,
        flag_selected: false,
        flag_apply: false,
        flag_output: None,
        plot_output,
        plot_format,
    };
    options.validate_plotms_weight_averaging()?;
    options.validate_geometry_controls(task)?;
    Ok((options, visibility_document))
}
