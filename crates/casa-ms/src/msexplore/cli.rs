// SPDX-License-Identifier: LGPL-3.0-or-later
//! Schema-backed CLI support for `msexplore`.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use casa_provider_contracts::ParameterValue;
use casa_task_runtime::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use casa_types::{ArrayValue, ScalarValue, Value};
use serde::Deserialize;

use super::task_contract::{
    MsExplorePlotArtifact, MsExploreRunTaskResult, msexplore_task_schema_bundle,
};
use super::{
    MsAxis, MsColorAxis, MsDataColumn, MsExploreSpec, MsExportFormat, MsFlagAction,
    MsFlagEditPreview, MsFlagEditSpec, MsFlagRegion, MsFlagRowEdit, MsFlagSampleEdit,
    MsIterationAxis, MsLegendPosition, MsPageExportRange, MsPageHeaderItem, MsPlotPreset,
    MsPlotSpec, MsPlotStyleSpec, apply_msexplore_flag_edit_for_request, build_msexplore_payload,
    export_msexplore_plot, preview_msexplore_flag_edit_for_request,
};
pub use crate::presentation::{UiActionKind, UiArgumentParser, UiCommandSchema, UiValueKind};
use crate::{MeasurementSet, MsSelection};
use crate::{MeasurementSetSummary, MeasurementSetSummaryOutputFormat};

#[derive(Debug)]
struct CliOptions {
    ms_path: PathBuf,
    summary_format: MeasurementSetSummaryOutputFormat,
    summary_output: Option<PathBuf>,
    overwrite: bool,
    selection: MsSelection,
    page_spec: Option<PathBuf>,
    preset: Option<MsPlotPreset>,
    x_axis: Option<MsAxis>,
    y_axis: Option<MsAxis>,
    y_axis2: Option<MsAxis>,
    data_column: MsDataColumn,
    color_by: MsColorAxis,
    avgchannel: Option<usize>,
    avgtime: Option<f64>,
    avgscan: bool,
    avgfield: bool,
    avgbaseline: bool,
    avgantenna: bool,
    avgspw: bool,
    scalar: bool,
    freqframe: Option<String>,
    restfreq: Option<String>,
    veldef: String,
    iteraxis: Option<MsIterationAxis>,
    gridrows: usize,
    gridcols: usize,
    xselfscale: bool,
    yselfscale: bool,
    xsharedaxis: bool,
    ysharedaxis: bool,
    title: Option<String>,
    xlabel: Option<String>,
    ylabel: Option<String>,
    symbol_size: Option<u32>,
    showlegend: bool,
    legendposition: MsLegendPosition,
    showmajorgrid: bool,
    showminorgrid: bool,
    headeritems: Option<String>,
    max_points: usize,
    flag_action: Option<MsFlagAction>,
    flag_xmin: Option<f64>,
    flag_xmax: Option<f64>,
    flag_ymin: Option<f64>,
    flag_ymax: Option<f64>,
    flag_plotindex: Option<usize>,
    flag_panel: Option<String>,
    flag_extcorr: bool,
    flag_extchannel: bool,
    flag_selected: bool,
    flag_apply: bool,
    flag_output: Option<PathBuf>,
    plot_output: Option<PathBuf>,
    plot_format: MsExportFormat,
}

#[derive(Debug, Deserialize)]
struct CliPageSpecFile {
    #[serde(default = "one")]
    gridrows: usize,
    #[serde(default = "one")]
    gridcols: usize,
    #[serde(default)]
    page_title: Option<String>,
    #[serde(default)]
    exprange: MsPageExportRange,
    #[serde(default)]
    headeritems: Option<String>,
    plots: Vec<CliPagePlotSpec>,
}

#[derive(Debug, Deserialize)]
struct CliPagePlotSpec {
    #[serde(default)]
    preset: Option<MsPlotPreset>,
    #[serde(default)]
    x_axis: Option<MsAxis>,
    #[serde(default)]
    y_axis: Option<MsAxis>,
    #[serde(default)]
    y_axis2: Option<MsAxis>,
    #[serde(default)]
    data_column: Option<MsDataColumn>,
    #[serde(default)]
    color_by: Option<MsColorAxis>,
    #[serde(default)]
    avgchannel: Option<usize>,
    #[serde(default)]
    avgtime: Option<f64>,
    #[serde(default)]
    avgscan: bool,
    #[serde(default)]
    avgfield: bool,
    #[serde(default)]
    avgbaseline: bool,
    #[serde(default)]
    avgantenna: bool,
    #[serde(default)]
    avgspw: bool,
    #[serde(default)]
    scalar: bool,
    #[serde(default)]
    freqframe: Option<String>,
    #[serde(default)]
    restfreq: Option<String>,
    #[serde(default)]
    veldef: Option<String>,
    #[serde(default)]
    rowindex: usize,
    #[serde(default)]
    colindex: usize,
    #[serde(default)]
    plotindex: usize,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    xlabel: Option<String>,
    #[serde(default)]
    ylabel: Option<String>,
    #[serde(default)]
    symbol_size: Option<u32>,
    #[serde(default)]
    showlegend: bool,
    #[serde(default)]
    legendposition: Option<MsLegendPosition>,
    #[serde(default)]
    showmajorgrid: bool,
    #[serde(default)]
    showminorgrid: bool,
}

const fn one() -> usize {
    1
}

/// The front end of the `msexplore` binary, which hosts `msexplore` (the
/// default) and `plotms`.
pub fn task_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    TaskFrontEnd::new(
        "msexplore",
        msexplore_task_schema_bundle(),
        &["msexplore", "plotms"],
        run_task,
    )
    .expect("the msexplore contract carries msexplore and plotms")
}

/// Run one resolved `msexplore` or `plotms` request. `msexplore` returns its
/// summary, as CASA's `listobs` returns its record; `plotms` returns None.
fn run_task(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let outcome = run(options_from(task)?)?;
    let success = if task.surface() == "plotms" {
        TaskSuccess::new(&(), &outcome.result)?
    } else {
        TaskSuccess::new(&outcome.result.summary, &outcome.result)?
    };
    let success = success.with_text(outcome.text);
    Ok(match outcome.note {
        Some(note) => success.with_note(note),
        None => success,
    })
}

/// Build the machine-readable command schema for `msexplore`.
pub fn command_schema(program_name: &str) -> UiCommandSchema {
    let bundle = casa_provider_contracts::builtin_surface_bundle("msexplore")
        .expect("built-in msexplore parameter surface must remain valid");
    let mut schema: UiCommandSchema =
        serde_json::from_value(casa_provider_contracts::project_ui_form(&bundle))
            .expect("canonical msexplore UI projection must match UiCommandSchema");
    schema.invocation_name = program_name.to_string();
    schema.usage = format!("{program_name} [parameters]");
    schema
}
/// Run one command line. Everything a refusal can find is checked before the
/// first write: the outputs, the flag arguments and the plot (#931). The flag
/// edit is then made in memory, so a refusal that needs the MeasurementSet
/// also comes first, and is saved after the summary is written.
fn run(options: CliOptions) -> Result<RunOutcome, TaskFailure> {
    let refused = TaskFailure::validation;
    let failed = TaskFailure::execution;
    super::task_contract::refuse_unwritable_outputs(
        &[
            ("summary output", options.summary_output.as_deref()),
            ("flag output", options.flag_output.as_deref()),
            ("plot output", options.plot_output.as_deref()),
        ],
        options.overwrite,
        "pass --overwrite to replace it",
    )
    .map_err(refused)?;
    let flag_edit = build_flag_edit_spec(&options).map_err(refused)?;
    let explore_spec = if options.plot_output.is_some() || flag_edit.is_some() {
        Some(build_explore_spec(&options).map_err(refused)?)
    } else {
        None
    };
    let mut ms = MeasurementSet::open(&options.ms_path).map_err(|error| {
        failed(if options.ms_path.is_dir() {
            format!(
                "msexplore currently supports MeasurementSets only; failed to open {} as an MS: {error}",
                options.ms_path.display()
            )
        } else {
            format!("open MeasurementSet {}: {error}", options.ms_path.display())
        })
    })?;

    let summary =
        MeasurementSetSummary::from_ms_with_options(&ms, &options.selection.to_summary_options())
            .map_err(|error| failed(error.to_string()))?;
    let summary_text = summary
        .render(options.summary_format)
        .map_err(|error| failed(error.to_string()))?;
    // Refused here, before anything is written: the edit is in memory.
    let flag_preview =
        edit_flags_in_memory(&mut ms, &options, flag_edit.as_ref(), explore_spec.as_ref())
            .map_err(refused)?;
    let mut text = String::new();
    match options.summary_output.as_deref() {
        Some(path) => write_output(path, options.overwrite, &summary_text).map_err(failed)?,
        None => text = summary_text,
    }
    let mut note = None;
    if let Some(preview) = &flag_preview {
        if options.flag_apply {
            ms.save_main_table_only()
                .map_err(|error| failed(error.to_string()))?;
        }
        note = write_or_describe_flag_preview(&options, preview).map_err(failed)?;
    }

    let plot_export = match &options.plot_output {
        Some(plot_output) => {
            let explore_spec = explore_spec.as_ref().ok_or_else(|| {
                failed("msexplore plot export lost its prepared explore spec".to_string())
            })?;
            let payload = build_msexplore_payload(&ms, explore_spec).map_err(failed)?;
            match options.plot_format {
                MsExportFormat::Txt => {
                    export_msexplore_plot(&payload, plot_output).map_err(failed)?;
                }
            }
            Some(MsExplorePlotArtifact {
                output_path: plot_output.clone(),
                format: options.plot_format,
            })
        }
        None => None,
    };

    Ok(RunOutcome {
        result: MsExploreRunTaskResult {
            summary,
            summary_output_path: options.summary_output,
            flag_edit_preview: flag_preview,
            flag_edit_output_path: options.flag_output,
            plot_export,
        },
        text,
        note,
    })
}

/// What one run produced: the provider's result, the summary for stdout
/// when no `output` file was named, and the flag-edit line for stderr when
/// no `flag_output` file was.
struct RunOutcome {
    result: MsExploreRunTaskResult,
    text: String,
    note: Option<String>,
}

fn build_explore_spec(options: &CliOptions) -> Result<MsExploreSpec, String> {
    if let Some(page_spec_path) = &options.page_spec {
        let page_spec = load_page_spec_file(page_spec_path)?;
        let mut header_items = parse_header_items(page_spec.headeritems.as_deref())?;
        merge_header_items(
            &mut header_items,
            parse_header_items(options.headeritems.as_deref())?,
        );
        let plots = page_spec
            .plots
            .into_iter()
            .map(|plot| {
                build_plot_spec_from_values(
                    plot.preset,
                    plot.x_axis,
                    plot.y_axis,
                    plot.y_axis2,
                    plot.data_column.unwrap_or(MsDataColumn::Data),
                    plot.color_by.unwrap_or(MsColorAxis::Field),
                    plot.avgchannel,
                    plot.avgtime,
                    plot.avgscan,
                    plot.avgfield,
                    plot.avgbaseline,
                    plot.avgantenna,
                    plot.avgspw,
                    plot.scalar,
                    plot.freqframe,
                    plot.restfreq,
                    plot.veldef.unwrap_or_else(|| "RADIO".to_string()),
                    None,
                    page_spec.gridrows.max(1),
                    page_spec.gridcols.max(1),
                    plot.rowindex,
                    plot.colindex,
                    plot.plotindex,
                    false,
                    false,
                    false,
                    false,
                    plot.title,
                    plot.xlabel,
                    plot.ylabel,
                    plot.symbol_size,
                    plot.showlegend,
                    plot.legendposition.unwrap_or(MsLegendPosition::UpperRight),
                    plot.showmajorgrid,
                    plot.showminorgrid,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let spec = MsExploreSpec {
            ms_path: options.ms_path.clone(),
            summary_format: options.summary_format,
            selection: options.selection.clone(),
            header_items,
            page_title: page_spec.page_title,
            exprange: page_spec.exprange,
            max_plot_points: options.max_points,
            plots,
        };
        spec.validate()?;
        return Ok(spec);
    }

    let plot_spec = build_plot_spec(options)?;
    let spec = MsExploreSpec {
        ms_path: options.ms_path.clone(),
        summary_format: options.summary_format,
        selection: options.selection.clone(),
        header_items: parse_header_items(options.headeritems.as_deref())?,
        page_title: None,
        exprange: MsPageExportRange::Current,
        max_plot_points: options.max_points,
        plots: vec![plot_spec],
    };
    spec.validate()?;
    Ok(spec)
}

fn build_plot_spec(options: &CliOptions) -> Result<MsPlotSpec, String> {
    let spec = build_plot_spec_from_values(
        options.preset,
        options.x_axis,
        options.y_axis,
        options.y_axis2,
        options.data_column,
        options.color_by,
        options.avgchannel,
        options.avgtime,
        options.avgscan,
        options.avgfield,
        options.avgbaseline,
        options.avgantenna,
        options.avgspw,
        options.scalar,
        options.freqframe.clone(),
        options.restfreq.clone(),
        options.veldef.clone(),
        options.iteraxis,
        options.gridrows,
        options.gridcols,
        0,
        0,
        0,
        options.xselfscale,
        options.yselfscale,
        options.xsharedaxis,
        options.ysharedaxis,
        options.title.clone(),
        options.xlabel.clone(),
        options.ylabel.clone(),
        options.symbol_size,
        options.showlegend,
        options.legendposition,
        options.showmajorgrid,
        options.showminorgrid,
    )?;
    spec.validate()?;
    Ok(spec)
}

fn build_flag_edit_spec(options: &CliOptions) -> Result<Option<MsFlagEditSpec>, String> {
    let Some(action) = options.flag_action else {
        return Ok(None);
    };
    if options.flag_selected {
        return Ok(None);
    }
    let x_min = options
        .flag_xmin
        .ok_or_else(|| "--flag-action requires --flag-xmin".to_string())?;
    let x_max = options
        .flag_xmax
        .ok_or_else(|| "--flag-action requires --flag-xmax".to_string())?;
    let y_min = options
        .flag_ymin
        .ok_or_else(|| "--flag-action requires --flag-ymin".to_string())?;
    let y_max = options
        .flag_ymax
        .ok_or_else(|| "--flag-action requires --flag-ymax".to_string())?;
    Ok(Some(MsFlagEditSpec {
        action,
        region: MsFlagRegion {
            x_min,
            x_max,
            y_min,
            y_max,
        },
        plot_index: options.flag_plotindex,
        panel_key: options.flag_panel.clone(),
        extcorr: options.flag_extcorr,
        extchannel: options.flag_extchannel,
    }))
}

/// Make the requested flag edit in memory, with `--flag-apply`, and return
/// its preview; the caller saves it.
fn edit_flags_in_memory(
    ms: &mut MeasurementSet,
    options: &CliOptions,
    flag_edit: Option<&MsFlagEditSpec>,
    explore_spec: Option<&MsExploreSpec>,
) -> Result<Option<MsFlagEditPreview>, String> {
    let Some(action) = options.flag_action else {
        return Ok(None);
    };
    if options.flag_selected {
        if options.flag_apply && options.flag_output.is_none() {
            return apply_selection_flag_edit_direct(ms, &options.selection, action).map(Some);
        }
        let preview = preview_selection_flag_edit(ms, &options.selection, action)?;
        if options.flag_apply {
            apply_selection_flag_edit_preview(ms, &preview)?;
        }
        return Ok(Some(preview));
    }
    let (Some(explore_spec), Some(flag_edit)) = (explore_spec, flag_edit) else {
        return Err("msexplore lost its prepared flag edit".to_string());
    };
    if options.flag_apply {
        apply_msexplore_flag_edit_for_request(ms, explore_spec, flag_edit).map(Some)
    } else {
        preview_msexplore_flag_edit_for_request(ms, explore_spec, flag_edit).map(Some)
    }
}

/// Write the preview to `flag_output`, or describe it in one line.
fn write_or_describe_flag_preview(
    options: &CliOptions,
    preview: &MsFlagEditPreview,
) -> Result<Option<String>, String> {
    if let Some(path) = options.flag_output.as_deref() {
        let json = serde_json::to_string_pretty(preview)
            .map_err(|error| format!("serialize flag preview: {error}"))?;
        write_output(path, options.overwrite, &json)?;
        return Ok(None);
    }
    Ok(Some(format!(
        "Flag edit preview: matched_points={} affected_rows={} affected_samples={} apply={}",
        preview.matched_points, preview.affected_rows, preview.affected_samples, options.flag_apply
    )))
}

fn preview_selection_flag_edit(
    ms: &MeasurementSet,
    selection: &MsSelection,
    action: MsFlagAction,
) -> Result<MsFlagEditPreview, String> {
    let listobs_options = selection.to_summary_options();
    let row_numbers = super::resolve_selected_rows_with_msselect(ms, selection, &listobs_options)?;
    let mut sample_edits = Vec::new();
    let mut row_edits = Vec::new();

    for row in row_numbers {
        let old_flag_row = ms
            .flag_row_column()
            .get(row)
            .map_err(|error| error.to_string())?;
        let old_matrix = super::clone_flag_matrix(ms, row)?;
        let mut new_matrix = old_matrix.clone();
        let (corr_count, chan_count) = old_matrix.dim();
        let mut changed_samples = 0usize;
        for corr in 0..corr_count {
            for chan in 0..chan_count {
                let old_flag = old_matrix[(corr, chan)];
                let new_flag = match action {
                    MsFlagAction::Flag => true,
                    MsFlagAction::Unflag => false,
                };
                if old_flag != new_flag {
                    new_matrix[(corr, chan)] = new_flag;
                    sample_edits.push(MsFlagSampleEdit {
                        row,
                        corr,
                        chan,
                        old_flag,
                        new_flag,
                    });
                    changed_samples += 1;
                }
            }
        }
        let new_flag_row = new_matrix.iter().all(|value| *value);
        if changed_samples > 0 || old_flag_row != new_flag_row {
            row_edits.push(MsFlagRowEdit {
                row,
                old_flag_row,
                new_flag_row,
                changed_samples,
            });
        }
    }

    Ok(MsFlagEditPreview {
        plot_title: "selection flag edit".to_string(),
        plot_index: None,
        panel_key: None,
        panel_label: None,
        x_axis: MsAxis::Time,
        y_axis: MsAxis::Flag,
        region: MsFlagRegion {
            x_min: 0.0,
            x_max: 0.0,
            y_min: 0.0,
            y_max: 0.0,
        },
        action,
        extcorr: true,
        extchannel: true,
        matched_points: sample_edits.len(),
        affected_rows: row_edits.len(),
        affected_samples: sample_edits.len(),
        sample_edits,
        row_edits,
    })
}

fn apply_selection_flag_edit_preview(
    ms: &mut MeasurementSet,
    preview: &MsFlagEditPreview,
) -> Result<(), String> {
    let mut row_updates = BTreeMap::<usize, (ndarray::Array2<bool>, bool)>::new();
    for row_edit in &preview.row_edits {
        row_updates.insert(
            row_edit.row,
            (
                super::clone_flag_matrix(ms, row_edit.row)?,
                row_edit.new_flag_row,
            ),
        );
    }
    for sample in &preview.sample_edits {
        let (matrix, _) = row_updates
            .get_mut(&sample.row)
            .ok_or_else(|| format!("flag edit lost planned row {}", sample.row))?;
        matrix[(sample.corr, sample.chan)] = sample.new_flag;
    }
    for (row, (matrix, flag_row)) in row_updates {
        ms.main_table_mut()
            .cell_accessor_mut(row, "FLAG")
            .map_err(|error| error.to_string())?
            .set(Value::Array(ArrayValue::Bool(matrix.into_dyn())))
            .map_err(|error| error.to_string())?;
        ms.main_table_mut()
            .cell_accessor_mut(row, "FLAG_ROW")
            .map_err(|error| error.to_string())?
            .set(Value::Scalar(ScalarValue::Bool(flag_row)))
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn apply_selection_flag_edit_direct(
    ms: &mut MeasurementSet,
    selection: &MsSelection,
    action: MsFlagAction,
) -> Result<MsFlagEditPreview, String> {
    let listobs_options = selection.to_summary_options();
    let row_numbers = super::resolve_selected_rows_with_msselect(ms, selection, &listobs_options)?;
    let mut affected_rows = 0usize;
    let mut affected_samples = 0usize;

    for row in row_numbers {
        let mut matrix = super::clone_flag_matrix(ms, row)?;
        let mut changed_samples = 0usize;
        let new_flag = match action {
            MsFlagAction::Flag => true,
            MsFlagAction::Unflag => false,
        };
        for value in &mut matrix {
            if *value != new_flag {
                *value = new_flag;
                changed_samples += 1;
            }
        }
        if changed_samples == 0 {
            continue;
        }
        let new_flag_row = matrix.iter().all(|value| *value);
        ms.main_table_mut()
            .cell_accessor_mut(row, "FLAG")
            .map_err(|error| error.to_string())?
            .set(Value::Array(ArrayValue::Bool(matrix.into_dyn())))
            .map_err(|error| error.to_string())?;
        ms.main_table_mut()
            .cell_accessor_mut(row, "FLAG_ROW")
            .map_err(|error| error.to_string())?
            .set(Value::Scalar(ScalarValue::Bool(new_flag_row)))
            .map_err(|error| error.to_string())?;
        affected_rows += 1;
        affected_samples += changed_samples;
    }

    Ok(MsFlagEditPreview {
        plot_title: "selection flag edit".to_string(),
        plot_index: None,
        panel_key: None,
        panel_label: None,
        x_axis: MsAxis::Time,
        y_axis: MsAxis::Flag,
        region: MsFlagRegion {
            x_min: 0.0,
            x_max: 0.0,
            y_min: 0.0,
            y_max: 0.0,
        },
        action,
        extcorr: true,
        extchannel: true,
        matched_points: affected_samples,
        affected_rows,
        affected_samples,
        sample_edits: Vec::new(),
        row_edits: Vec::new(),
    })
}

#[allow(clippy::too_many_arguments)]
fn build_plot_spec_from_values(
    preset: Option<MsPlotPreset>,
    x_axis: Option<MsAxis>,
    y_axis: Option<MsAxis>,
    y_axis2: Option<MsAxis>,
    data_column: MsDataColumn,
    color_by: MsColorAxis,
    avgchannel: Option<usize>,
    avgtime: Option<f64>,
    avgscan: bool,
    avgfield: bool,
    avgbaseline: bool,
    avgantenna: bool,
    avgspw: bool,
    scalar: bool,
    freqframe: Option<String>,
    restfreq: Option<String>,
    veldef: String,
    iteraxis: Option<MsIterationAxis>,
    gridrows: usize,
    gridcols: usize,
    rowindex: usize,
    colindex: usize,
    plotindex: usize,
    xselfscale: bool,
    yselfscale: bool,
    xsharedaxis: bool,
    ysharedaxis: bool,
    title: Option<String>,
    xlabel: Option<String>,
    ylabel: Option<String>,
    symbol_size: Option<u32>,
    showlegend: bool,
    legendposition: MsLegendPosition,
    showmajorgrid: bool,
    showminorgrid: bool,
) -> Result<MsPlotSpec, String> {
    let mut spec = if let Some(preset) = preset {
        MsPlotSpec::from_preset(preset)
    } else {
        let x_axis = x_axis.ok_or_else(|| {
            "msexplore plot specs require either a preset or both x_axis and y_axis".to_string()
        })?;
        let y_axis = y_axis.ok_or_else(|| {
            "msexplore plot specs require either a preset or both x_axis and y_axis".to_string()
        })?;
        let mut y_axes = vec![y_axis];
        if let Some(y_axis2) = y_axis2 {
            y_axes.push(y_axis2);
        }
        MsPlotSpec {
            preset: None,
            x_axis,
            y_axes,
            data_column,
            color_by,
            averaging: Default::default(),
            transforms: Default::default(),
            layout: Default::default(),
            iteration: Default::default(),
            style: Default::default(),
            flag_edit: None,
        }
    };
    spec.data_column = data_column;
    spec.color_by = color_by;
    if spec.preset == Some(MsPlotPreset::AmplitudePhaseVsTimeStacked) && y_axis2.is_some() {
        return Err(
            "--yaxis2 is not supported with stacked paired presets; the preset already defines both panels"
                .to_string(),
        );
    }
    if let Some(y_axis2) = y_axis2 {
        spec.y_axes.truncate(1);
        spec.y_axes.push(y_axis2);
    } else {
        spec.y_axes.truncate(1);
    }
    spec.averaging.avgchannel = avgchannel;
    spec.averaging.avgtime = avgtime;
    spec.averaging.avgscan = avgscan;
    spec.averaging.avgfield = avgfield;
    spec.averaging.avgbaseline = avgbaseline;
    spec.averaging.avgantenna = avgantenna;
    spec.averaging.avgspw = avgspw;
    spec.averaging.scalar = scalar;
    spec.transforms.freqframe = freqframe;
    spec.transforms.restfreq = restfreq;
    spec.transforms.veldef = veldef;
    spec.layout.gridrows = gridrows.max(1);
    spec.layout.gridcols = gridcols.max(1);
    spec.layout.rowindex = rowindex;
    spec.layout.colindex = colindex;
    spec.layout.plotindex = plotindex;
    spec.iteration.iteraxis = iteraxis;
    spec.iteration.xselfscale = xselfscale;
    spec.iteration.yselfscale = yselfscale;
    spec.iteration.xsharedaxis = xsharedaxis;
    spec.iteration.ysharedaxis = ysharedaxis;
    spec.style = MsPlotStyleSpec {
        title,
        xlabel,
        ylabel,
        symbol_size,
        showlegend,
        legendposition,
        showmajorgrid,
        showminorgrid,
    };
    Ok(spec)
}

fn load_page_spec_file(path: &std::path::Path) -> Result<CliPageSpecFile, String> {
    let text = fs::read_to_string(path)
        .map_err(|error| format!("read --page-spec {}: {error}", path.display()))?;
    serde_json::from_str(&text)
        .map_err(|error| format!("parse --page-spec {}: {error}", path.display()))
}

fn parse_header_items(value: Option<&str>) -> Result<Vec<MsPageHeaderItem>, String> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let mut items = Vec::new();
    for raw in value.split(',') {
        let token = raw.trim();
        if token.is_empty() {
            continue;
        }
        let item = match token {
            "filename" => MsPageHeaderItem::Filename,
            "ycolumn" => MsPageHeaderItem::YColumn,
            "obsdate" => MsPageHeaderItem::ObsDate,
            "obstime" => MsPageHeaderItem::ObsTime,
            "observer" => MsPageHeaderItem::Observer,
            "projid" => MsPageHeaderItem::ProjId,
            "telescope" => MsPageHeaderItem::Telescope,
            "targname" => MsPageHeaderItem::TargName,
            "targdir" => MsPageHeaderItem::TargDir,
            other => {
                return Err(format!(
                    "unsupported --headeritems value {other:?}; expected a comma-separated subset of: filename, ycolumn, obsdate, obstime, observer, projid, telescope, targname, targdir"
                ));
            }
        };
        if !items.contains(&item) {
            items.push(item);
        }
    }
    Ok(items)
}

fn merge_header_items(target: &mut Vec<MsPageHeaderItem>, extra: Vec<MsPageHeaderItem>) {
    for item in extra {
        if !target.contains(&item) {
            target.push(item);
        }
    }
}

/// The single-plot controls, which a run that names a page spec cannot set.
const PLOT_CONTROLS: &[&str] = &[
    "preset",
    "x_axis",
    "y_axis",
    "y_axis2",
    "datacolumn",
    "color_by",
    "avgchannel",
    "avgtime",
    "avgscan",
    "avgfield",
    "avgbaseline",
    "avgantenna",
    "avgspw",
    "scalar",
    "freqframe",
    "restfreq",
    "veldef",
    "iteraxis",
    "gridrows",
    "gridcols",
    "xselfscale",
    "yselfscale",
    "xsharedaxis",
    "ysharedaxis",
    "title",
    "xlabel",
    "ylabel",
    "symbol_size",
    "showlegend",
    "legendposition",
    "showmajorgrid",
    "showminorgrid",
    "max_points",
];

/// The resolved run as [`CliOptions`], refused where its parameters
/// contradict one another.
fn options_from(task: &TaskInvocation<'_>) -> Result<CliOptions, TaskFailure> {
    let text = |name: &str| task.text(name);
    let path = |name: &str| Ok::<_, TaskFailure>(task.text(name)?.map(PathBuf::from));
    let flag = |name: &str| task.flag(name);
    let options = CliOptions {
        ms_path: task.path("vis")?,
        summary_format: required(task, "format", |format| match format {
            "text" => Ok(MeasurementSetSummaryOutputFormat::Text),
            "json" => Ok(MeasurementSetSummaryOutputFormat::Json),
            other => Err(format!(
                "unsupported format {other:?}; expected text or json"
            )),
        })?,
        summary_output: path("output")?,
        overwrite: flag("overwrite")?,
        selection: MsSelection {
            selectdata: flag("selectdata")?,
            field: text("field")?,
            spw: text("spw")?,
            timerange: text("timerange")?,
            uvrange: text("uvrange")?,
            antenna: text("antenna")?,
            scan: text("scan")?,
            correlation: text("correlation")?,
            observation: text("observation")?,
            array: text("array")?,
            intent: text("intent")?,
            feed: text("feed")?,
            msselect: text("msselect")?,
            ..MsSelection::default()
        },
        page_spec: path("page_spec")?,
        preset: parsed(task, "preset", MsPlotPreset::parse)?,
        x_axis: parsed(task, "x_axis", MsAxis::parse)?,
        y_axis: parsed(task, "y_axis", MsAxis::parse)?,
        y_axis2: parsed(task, "y_axis2", MsAxis::parse)?,
        data_column: required(task, "datacolumn", MsDataColumn::parse)?,
        color_by: required(task, "color_by", MsColorAxis::parse)?,
        avgchannel: parsed(task, "avgchannel", number::<usize>)?,
        avgtime: parsed(task, "avgtime", number::<f64>)?,
        avgscan: flag("avgscan")?,
        avgfield: flag("avgfield")?,
        avgbaseline: flag("avgbaseline")?,
        avgantenna: flag("avgantenna")?,
        avgspw: flag("avgspw")?,
        scalar: flag("scalar")?,
        freqframe: text("freqframe")?,
        restfreq: text("restfreq")?,
        veldef: task.required_text("veldef")?,
        iteraxis: parsed(task, "iteraxis", MsIterationAxis::parse)?,
        gridrows: count(task, "gridrows")?.max(1),
        gridcols: count(task, "gridcols")?.max(1),
        xselfscale: flag("xselfscale")?,
        yselfscale: flag("yselfscale")?,
        xsharedaxis: flag("xsharedaxis")?,
        ysharedaxis: flag("ysharedaxis")?,
        title: text("title")?,
        xlabel: text("xlabel")?,
        ylabel: text("ylabel")?,
        symbol_size: parsed(task, "symbol_size", number::<u32>)?,
        showlegend: flag("showlegend")?,
        legendposition: required(task, "legendposition", legend_position)?,
        showmajorgrid: flag("showmajorgrid")?,
        showminorgrid: flag("showminorgrid")?,
        headeritems: text("headeritems")?,
        max_points: count(task, "max_points")?,
        flag_action: parsed(task, "flag_action", flag_action)?,
        flag_xmin: parsed(task, "flag_xmin", number::<f64>)?,
        flag_xmax: parsed(task, "flag_xmax", number::<f64>)?,
        flag_ymin: parsed(task, "flag_ymin", number::<f64>)?,
        flag_ymax: parsed(task, "flag_ymax", number::<f64>)?,
        flag_plotindex: parsed(task, "flag_plotindex", number::<usize>)?,
        flag_panel: text("flag_panel")?,
        flag_extcorr: flag("flag_extcorr")?,
        flag_extchannel: flag("flag_extchannel")?,
        flag_selected: flag("flag_selected")?,
        flag_apply: flag("flag_apply")?,
        flag_output: path("flag_output")?,
        plot_output: path("plot_output")?,
        plot_format: required(task, "plot_format", MsExportFormat::parse)?,
    };
    let plot_control_used = PLOT_CONTROLS.iter().any(|name| task.is_set(name));
    check_options(&options, plot_control_used).map_err(TaskFailure::validation)?;
    Ok(options)
}

/// Refuse the combinations a plot or flag edit cannot take.
fn check_options(options: &CliOptions, plot_control_used: bool) -> Result<(), String> {
    let single_plot =
        options.preset.is_some() || (options.x_axis.is_some() && options.y_axis.is_some());
    if options.page_spec.is_some() && plot_control_used {
        return Err(
            "--page-spec cannot be combined with the single-plot preset/axis/layout flags; put those settings in the JSON page spec instead".to_string(),
        );
    }
    if options.plot_output.is_some() && options.page_spec.is_none() && !single_plot {
        return Err(
            "--plot-output requires either --preset or both --xaxis and --yaxis".to_string(),
        );
    }
    if options.flag_action.is_some()
        && !options.flag_selected
        && options.page_spec.is_none()
        && !single_plot
    {
        return Err(
            "--flag-action requires either --preset or both --xaxis and --yaxis".to_string(),
        );
    }
    if options.flag_plotindex.is_some() && options.flag_panel.is_some() {
        return Err(
            "msexplore staged flag editing accepts either --flag-plotindex or --flag-panel, not both".to_string(),
        );
    }
    let region_controls = options.flag_xmin.is_some()
        || options.flag_xmax.is_some()
        || options.flag_ymin.is_some()
        || options.flag_ymax.is_some()
        || options.flag_plotindex.is_some()
        || options.flag_panel.is_some()
        || options.flag_extcorr
        || options.flag_extchannel;
    if options.flag_selected && region_controls {
        return Err(
            "msexplore --flag-selected cannot be combined with plot-region flag-edit controls"
                .to_string(),
        );
    }
    if options.flag_action.is_none()
        && (region_controls
            || options.flag_selected
            || options.flag_apply
            || options.flag_output.is_some())
    {
        return Err(
            "msexplore flag-edit region, extension, apply, and output flags require --flag-action"
                .to_string(),
        );
    }
    Ok(())
}

/// `name` parsed by `parse`, or `None` when unset; a refusal names it. A
/// number or `none` resolves as a number when it is one.
fn parsed<T>(
    task: &TaskInvocation<'_>,
    name: &str,
    parse: impl Fn(&str) -> Result<T, String>,
) -> Result<Option<T>, TaskFailure> {
    let text = match task.value(name) {
        Some(ParameterValue::Integer(value)) => Some(value.to_string()),
        Some(ParameterValue::Float(value)) => Some(value.to_string()),
        _ => task.text(name)?,
    };
    text.map(|text| parse(&text).map_err(|message| TaskFailure::validation(message).at(name)))
        .transpose()
}

/// `name` parsed by `parse`; it must be set.
fn required<T>(
    task: &TaskInvocation<'_>,
    name: &str,
    parse: impl Fn(&str) -> Result<T, String>,
) -> Result<T, TaskFailure> {
    parsed(task, name, parse)?
        .ok_or_else(|| TaskFailure::validation(format!("parameter {name:?} is required")).at(name))
}

fn number<T: std::str::FromStr>(text: &str) -> Result<T, String> {
    text.trim()
        .parse()
        .map_err(|_| format!("invalid numeric value {text:?}"))
}

/// `name`, a count, which cannot be negative.
fn count(task: &TaskInvocation<'_>, name: &str) -> Result<usize, TaskFailure> {
    let value = task.integer(name)?;
    usize::try_from(value).map_err(|_| {
        TaskFailure::validation(format!("{name} cannot be negative, found {value}")).at(name)
    })
}

fn legend_position(text: &str) -> Result<MsLegendPosition, String> {
    serde_json::from_value(serde_json::Value::String(text.to_string())).map_err(|_| {
        "unsupported value for --legendposition; expected one of: upperRight, upperLeft, lowerRight, lowerLeft, exteriorRight, exteriorLeft, exteriorTop, exteriorBottom".to_string()
    })
}

fn flag_action(text: &str) -> Result<MsFlagAction, String> {
    match text.to_ascii_lowercase().as_str() {
        "flag" => Ok(MsFlagAction::Flag),
        "unflag" => Ok(MsFlagAction::Unflag),
        other => Err(format!(
            "unsupported value for --flag-action {other:?}; expected flag or unflag"
        )),
    }
}

fn write_output(path: &std::path::Path, overwrite: bool, text: &str) -> Result<(), String> {
    if path.exists() && !overwrite {
        return Err(format!(
            "refusing to overwrite existing output {}; pass --overwrite to replace it",
            path.display()
        ));
    }
    fs::write(path, text).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests;
