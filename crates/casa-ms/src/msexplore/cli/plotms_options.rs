// SPDX-License-Identifier: LGPL-3.0-or-later
//! Effective MeasurementSet PlotMS columns and averaging controls.

use std::path::PathBuf;

use casa_provider_contracts::ParameterValue;
use casa_task_runtime::{TaskFailure, TaskInvocation};
use casa_types::measures::direction::MDirection;

use super::super::{RawSymbolVisibility, calibration, plotms_columns, plotms_phasecenter};
use super::{
    CliOptions, MeasurementSetSummaryOutputFormat, MsAxis, MsColorAxis, MsDataColumn,
    MsExportFormat, MsIterationAxis, MsLegendPosition, MsSelection, count, legend_position, number,
};

impl CliOptions {
    pub(super) fn has_independent_ordinates(&self) -> bool {
        self.secondary_y_data_column.is_some() || !self.additional_ordinates.is_empty()
    }

    pub(super) fn supports_offset_scan_grid(&self) -> bool {
        self.iteraxis == Some(MsIterationAxis::Scan)
            && self.gridrows == 1
            && self.gridcols == 2
            && self.x_axis == Some(MsAxis::Time)
            && self.y_axis == Some(MsAxis::Amplitude)
            && self.y_axis2.is_none()
            && self.data_column == MsDataColumn::Data
            && !self.plotms_averaging_active()
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

    pub(super) fn plotms_averaging_active(&self) -> bool {
        self.avgchannel.is_some_and(|width| width > 1) || self.plotms_extended_averaging()
    }

    pub(super) fn supports_averaged_ordinate_iteration(&self) -> bool {
        self.iteraxis
            .is_some_and(MsIterationAxis::supports_averaged_ordinates)
            && self.freqframe.is_none()
            && self.x_axis.is_some_and(|axis| {
                axis.is_visibility_math()
                    || matches!(axis, MsAxis::Time | MsAxis::Channel | MsAxis::Frequency)
            })
            && self.supports_visibility_ordinate_averaging(false)
    }

    pub(super) fn supports_visibility_ordinate_averaging(&self, allow_spans: bool) -> bool {
        let time_average = self.avgtime.is_some()
            && (allow_spans || !(self.avgscan || self.avgfield))
            && !self.avgbaseline
            && !self.avgantenna
            && !self.avgspw
            && self.freqframe.is_none()
            && self.plotms_independent_average();
        (!self.plotms_extended_averaging() || time_average)
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
        let supported_average = if self.avgbaseline {
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
            && !self.avgantenna
            && !self.avgspw
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

    pub(super) fn supports_raw_visibility_symbols(&self, columns: [MsDataColumn; 3]) -> bool {
        self.x_axis.is_some_and(MsAxis::is_visibility_math)
            && self.y_axis.is_some_and(MsAxis::is_visibility_math)
            && self.y_axis2.is_none()
            && !self.plotms_averaging_active()
            && self.iteraxis.is_none()
            && self.color_by == MsColorAxis::None
            && columns[..2]
                .iter()
                .all(|column| matches!(column, MsDataColumn::Data | MsDataColumn::Corrected))
    }

    pub(super) fn plotms_phasecenter(
        &self,
        value: &str,
        without_calibration_or_overlays: bool,
    ) -> Result<MDirection, TaskFailure> {
        let raw_visibility = |axis: MsAxis, column: MsDataColumn| {
            axis.is_visibility_math()
                && matches!(
                    column,
                    MsDataColumn::Data | MsDataColumn::Corrected | MsDataColumn::Model
                )
        };
        let x = self.x_axis.is_some_and(|axis| {
            axis.uses_uvw_metres()
                || raw_visibility(axis, self.x_data_column.unwrap_or(self.data_column))
        });
        if !x
            || !self
                .y_axis
                .is_some_and(|axis| raw_visibility(axis, self.data_column))
            || !without_calibration_or_overlays
            || self.y_axis2.is_some()
            || self.plotms_averaging_active()
            || self.iteraxis.is_some()
            || self.freqframe.is_some()
        {
            return Err(TaskFailure::validation(
                "PlotMS phasecenter currently supports one unaveraged complex visibility ordinate and a complex visibility or UVW abscissa, with no iteration, reporting frame, callibrary or overlays (#1032)",
            )
            .at("phasecenter"));
        }
        plotms_phasecenter::parse(value)
    }

    pub(super) fn plotms_independent_average(&self) -> bool {
        let transforms_frequency =
            self.x_axis == Some(MsAxis::Frequency) && self.freqframe.is_some();
        let crosses_boundary = self.avgscan || self.avgfield;
        let time_average = self.avgtime.is_some()
            && !self.avgbaseline
            && !(transforms_frequency && crosses_boundary);
        let baseline_average =
            self.avgbaseline && (!crosses_boundary || self.scalar) && !transforms_frequency;
        let channel_average = self.avgchannel.is_some_and(|width| width > 1);
        let ordinary_time =
            time_average && !transforms_frequency && !crosses_boundary && !channel_average;
        let scalar_time = time_average && !transforms_frequency;
        (time_average || baseline_average)
            && !self.avgantenna
            && !self.avgspw
            && (!self.scalar || scalar_time || baseline_average)
            && !self.has_mixed_ordinate_dimensions()
            && self.x_axis.is_some_and(|axis| {
                axis.is_visibility_math()
                    || matches!(axis, MsAxis::Time | MsAxis::Channel | MsAxis::Frequency)
                    || (ordinary_time && !self.scalar && axis.uses_uvw_metres())
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
    let mut ordinates = calibration::axis_names(task.value("yaxis"))
        .map_err(TaskFailure::validation)?
        .into_iter()
        .map(|axis| MsAxis::parse(&axis.to_ascii_lowercase()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(TaskFailure::validation)?;
    if ordinates.is_empty() {
        ordinates.push(MsAxis::Amplitude);
    }
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
    let document_axes = x_axis.is_visibility_math()
        || y_axis.is_visibility_math()
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
        avgtime: average_text("avgtime")?
            .map(|value| number::<f64>(&value))
            .transpose()
            .map_err(TaskFailure::validation)?
            .filter(|&interval| !document_axes || interval != 0.0),
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
    Ok((options, visibility_document))
}
