// SPDX-License-Identifier: LGPL-3.0-or-later
//! Schema-backed CLI support for `msexplore`.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;

use casa_types::{ArrayValue, ScalarValue, Value};
use serde::Deserialize;

use super::task_contract::{MsExploreTaskRequest, msexplore_task_schema_bundle};
use super::{
    DEFAULT_MAX_PLOT_POINTS, MsAxis, MsColorAxis, MsDataColumn, MsExploreSpec, MsExportFormat,
    MsFlagAction, MsFlagEditPreview, MsFlagEditSpec, MsFlagRegion, MsFlagRowEdit, MsFlagSampleEdit,
    MsIterationAxis, MsLegendPosition, MsPageExportRange, MsPageHeaderItem, MsPlotPreset,
    MsPlotSpec, MsPlotStyleSpec, apply_msexplore_flag_edit_for_request, build_msexplore_payload,
    export_msexplore_plot, preview_msexplore_flag_edit_for_request,
};
pub use crate::presentation::{UiActionKind, UiArgumentParser, UiCommandSchema, UiValueKind};
use crate::{MeasurementSet, MsSelection};
use crate::{MeasurementSetSummary, MeasurementSetSummaryOutputFormat};

#[derive(Debug)]
enum CliAction {
    Help,
    Run(Box<CliOptions>),
}

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
    plot_width: u32,
    plot_height: u32,
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

/// Parse environment arguments, run `msexplore`, and return a process exit code.
pub fn run_env(program_name: &str) -> i32 {
    let (logging_guard, args) =
        match casa_logging::init_global_from_env_and_args(std::env::args_os().skip(1)) {
            Ok((guard, args)) => (guard, args),
            Err(error) => {
                eprintln!("Error: failed to initialize logging: {error}");
                return 1;
            }
        };
    tracing::info!("msexplore started");
    let code = run_with_cli_args(program_name, args);
    if code == 0 {
        tracing::info!("msexplore completed");
    } else {
        tracing::error!(
            casa.priority = "SEVERE",
            exit_code = code,
            "msexplore failed"
        );
    }
    if let Err(error) = logging_guard.flush() {
        eprintln!("Error: failed to flush logging: {error}");
        return 1;
    }
    code
}

/// Run `msexplore` with already-filtered CLI arguments.
pub fn run_with_cli_args(program_name: &str, args: impl IntoIterator<Item = OsString>) -> i32 {
    let args = args.into_iter().collect::<Vec<_>>();
    let host = casa_task_runtime::TaskCliHost::new(
        msexplore_task_schema_bundle(),
        |request: MsExploreTaskRequest| request.execute(),
    );
    match host.dispatch(&args) {
        Ok(Some(output)) => {
            println!("{output}");
            return 0;
        }
        Ok(None) => {}
        Err(error) => {
            eprintln!("Error: {error}");
            return error.exit_code();
        }
    }
    let schema = command_schema(program_name);
    match parse_args(args) {
        Ok(CliAction::Help) => {
            print!("{}", render_help(&schema));
            0
        }
        Ok(CliAction::Run(options)) => match run(*options) {
            Ok(()) => 0,
            Err(error) => {
                eprintln!("Error: {error}");
                1
            }
        },
        Err(error) => {
            eprintln!("Error: {error}\n");
            eprintln!("{}", render_help(&schema));
            1
        }
    }
}

/// Parse CLI-style arguments and build a validated [`MsExploreSpec`].
///
/// This is intended for schema-driven callers such as `casars` that need to
/// preview the current form state without spawning the `msexplore` binary.
pub fn build_explore_spec_from_args(
    args: impl IntoIterator<Item = OsString>,
) -> Result<MsExploreSpec, String> {
    let args = casa_logging::strip_cli_logging_args(args)
        .map_err(|error| format!("invalid logging argument: {error}"))?;
    match parse_args(args)? {
        CliAction::Run(options) => build_explore_spec(&options),
        CliAction::Help => Err("help actions do not produce an msexplore spec".to_string()),
    }
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
fn run(options: CliOptions) -> Result<(), String> {
    let mut ms = MeasurementSet::open(&options.ms_path).map_err(|error| {
        if options.ms_path.is_dir() {
            format!(
                "msexplore currently supports MeasurementSets only; failed to open {} as an MS: {error}",
                options.ms_path.display()
            )
        } else {
            format!("open MeasurementSet {}: {error}", options.ms_path.display())
        }
    })?;

    let summary =
        MeasurementSetSummary::from_ms_with_options(&ms, &options.selection.to_summary_options())
            .map_err(|error| error.to_string())?;
    let summary_text = summary
        .render(options.summary_format)
        .map_err(|error| error.to_string())?;
    write_output(
        options.summary_output.as_deref(),
        options.overwrite,
        &summary_text,
    )?;

    let flag_edit = build_flag_edit_spec(&options)?;
    let explore_spec = if options.plot_output.is_some() || flag_edit.is_some() {
        Some(build_explore_spec(&options)?)
    } else {
        None
    };

    if let Some(action) = options.flag_action {
        if options.flag_selected {
            let preview = if options.flag_apply && options.flag_output.is_none() {
                apply_selection_flag_edit_direct(&mut ms, &options.selection, action)?
            } else {
                let preview = preview_selection_flag_edit(&ms, &options.selection, action)?;
                if options.flag_apply {
                    apply_selection_flag_edit_preview(&mut ms, &preview)?;
                }
                preview
            };
            if options.flag_apply {
                ms.save_main_table_only()
                    .map_err(|error| error.to_string())?;
            }
            write_or_log_flag_preview(&options, &preview)?;
        } else if let Some(explore_spec) = &explore_spec {
            let flag_edit = flag_edit
                .as_ref()
                .ok_or_else(|| "msexplore lost its prepared flag edit".to_string())?;
            let preview = if options.flag_apply {
                let preview =
                    apply_msexplore_flag_edit_for_request(&mut ms, explore_spec, flag_edit)?;
                ms.save_main_table_only()
                    .map_err(|error| error.to_string())?;
                preview
            } else {
                preview_msexplore_flag_edit_for_request(&ms, explore_spec, flag_edit)?
            };
            write_or_log_flag_preview(&options, &preview)?;
        }
    }

    if let Some(plot_output) = &options.plot_output {
        let explore_spec = explore_spec
            .as_ref()
            .ok_or_else(|| "msexplore plot export lost its prepared explore spec".to_string())?;
        let payload = build_msexplore_payload(&ms, explore_spec)?;
        export_msexplore_plot(
            &payload,
            crate::MeasurementSetPlotTheme::light(),
            plot_output,
            options.plot_format,
            options.plot_width,
            options.plot_height,
        )?;
    }

    Ok(())
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

fn write_or_log_flag_preview(
    options: &CliOptions,
    preview: &MsFlagEditPreview,
) -> Result<(), String> {
    if let Some(path) = options.flag_output.as_deref() {
        let json = serde_json::to_string_pretty(preview)
            .map_err(|error| format!("serialize flag preview: {error}"))?;
        write_output(Some(path), options.overwrite, &json)?;
    } else {
        eprintln!(
            "Flag edit preview: matched_points={} affected_rows={} affected_samples={} apply={}",
            preview.matched_points,
            preview.affected_rows,
            preview.affected_samples,
            options.flag_apply
        );
    }
    Ok(())
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

fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<CliAction, String> {
    let args = args.into_iter().collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "-h" || arg == "--help") {
        return Ok(CliAction::Help);
    }

    let mut index = 0usize;
    let mut ms_path = None;
    let mut summary_format = "text".to_string();
    let mut summary_output = None;
    let mut overwrite = false;
    let mut selection = MsSelection::default();
    let mut page_spec = None;
    let mut preset = None;
    let mut x_axis = None;
    let mut y_axis = None;
    let mut y_axis2 = None;
    let mut data_column = MsDataColumn::Data;
    let mut color_by = MsColorAxis::Field;
    let mut avgchannel = None;
    let mut avgtime = None;
    let mut avgscan = false;
    let mut avgfield = false;
    let mut avgbaseline = false;
    let mut avgantenna = false;
    let mut avgspw = false;
    let mut scalar = false;
    let mut freqframe = None;
    let mut restfreq = None;
    let mut veldef = "RADIO".to_string();
    let mut iteraxis = None;
    let mut gridrows = 1usize;
    let mut gridcols = 1usize;
    let mut xselfscale = false;
    let mut yselfscale = false;
    let mut xsharedaxis = false;
    let mut ysharedaxis = false;
    let mut title = None;
    let mut xlabel = None;
    let mut ylabel = None;
    let mut symbol_size = None;
    let mut showlegend = false;
    let mut legendposition = MsLegendPosition::UpperRight;
    let mut showmajorgrid = false;
    let mut showminorgrid = false;
    let mut headeritems = None;
    let mut max_points = DEFAULT_MAX_PLOT_POINTS;
    let mut flag_action = None;
    let mut flag_xmin = None;
    let mut flag_xmax = None;
    let mut flag_ymin = None;
    let mut flag_ymax = None;
    let mut flag_plotindex = None;
    let mut flag_panel = None;
    let mut flag_extcorr = false;
    let mut flag_extchannel = false;
    let mut flag_selected = false;
    let mut flag_apply = false;
    let mut flag_output = None;
    let mut plot_output = None;
    let mut plot_format = MsExportFormat::Png;
    let mut plot_width = 1600u32;
    let mut plot_height = 900u32;
    let mut plot_control_used = false;

    while index < args.len() {
        let raw = args[index].to_string_lossy().to_string();
        let take_value =
            |index: &mut usize, args: &[OsString], flag: &str| -> Result<String, String> {
                *index += 1;
                args.get(*index)
                    .map(|value| value.to_string_lossy().to_string())
                    .ok_or_else(|| format!("missing value for {flag}"))
            };
        match raw.as_str() {
            "--format" => summary_format = take_value(&mut index, &args, "--format")?,
            "-o" | "--output" => {
                summary_output = Some(PathBuf::from(take_value(&mut index, &args, raw.as_str())?))
            }
            "--overwrite" => overwrite = true,
            "--selectdata" => selection.selectdata = true,
            "--no-selectdata" => selection.selectdata = false,
            "--field" => selection.field = Some(take_value(&mut index, &args, "--field")?),
            "--spw" => selection.spw = Some(take_value(&mut index, &args, "--spw")?),
            "--timerange" => {
                selection.timerange = Some(take_value(&mut index, &args, "--timerange")?)
            }
            "--uvrange" => selection.uvrange = Some(take_value(&mut index, &args, "--uvrange")?),
            "--antenna" => selection.antenna = Some(take_value(&mut index, &args, "--antenna")?),
            "--scan" => selection.scan = Some(take_value(&mut index, &args, "--scan")?),
            "--correlation" => {
                selection.correlation = Some(take_value(&mut index, &args, "--correlation")?)
            }
            "--observation" => {
                selection.observation = Some(take_value(&mut index, &args, "--observation")?)
            }
            "--array" => selection.array = Some(take_value(&mut index, &args, "--array")?),
            "--intent" => selection.intent = Some(take_value(&mut index, &args, "--intent")?),
            "--feed" => selection.feed = Some(take_value(&mut index, &args, "--feed")?),
            "--msselect" => selection.msselect = Some(take_value(&mut index, &args, "--msselect")?),
            "--page-spec" => {
                page_spec = Some(PathBuf::from(take_value(&mut index, &args, "--page-spec")?))
            }
            "--preset" => {
                plot_control_used = true;
                preset = Some(MsPlotPreset::parse(&take_value(
                    &mut index, &args, "--preset",
                )?)?)
            }
            "--xaxis" => {
                plot_control_used = true;
                x_axis = Some(MsAxis::parse(&take_value(&mut index, &args, "--xaxis")?)?)
            }
            "--yaxis" => {
                plot_control_used = true;
                y_axis = Some(MsAxis::parse(&take_value(&mut index, &args, "--yaxis")?)?)
            }
            "--yaxis2" => {
                plot_control_used = true;
                y_axis2 = Some(MsAxis::parse(&take_value(&mut index, &args, "--yaxis2")?)?)
            }
            "--data-column" => {
                plot_control_used = true;
                data_column = MsDataColumn::parse(&take_value(&mut index, &args, "--data-column")?)?
            }
            "--color-by" => {
                plot_control_used = true;
                color_by = MsColorAxis::parse(&take_value(&mut index, &args, "--color-by")?)?
            }
            "--avgchannel" => {
                plot_control_used = true;
                avgchannel = Some(
                    take_value(&mut index, &args, "--avgchannel")?
                        .parse::<usize>()
                        .map_err(|_| "invalid integer value for --avgchannel".to_string())?,
                )
            }
            "--avgtime" => {
                plot_control_used = true;
                avgtime = Some(
                    take_value(&mut index, &args, "--avgtime")?
                        .parse::<f64>()
                        .map_err(|_| "invalid floating-point value for --avgtime".to_string())?,
                )
            }
            "--avgscan" => {
                plot_control_used = true;
                avgscan = true
            }
            "--avgfield" => {
                plot_control_used = true;
                avgfield = true
            }
            "--avgbaseline" => {
                plot_control_used = true;
                avgbaseline = true
            }
            "--avgantenna" => {
                plot_control_used = true;
                avgantenna = true
            }
            "--avgspw" => {
                plot_control_used = true;
                avgspw = true
            }
            "--scalar" => {
                plot_control_used = true;
                scalar = true
            }
            "--freqframe" => {
                plot_control_used = true;
                freqframe = Some(take_value(&mut index, &args, "--freqframe")?)
            }
            "--restfreq" => {
                plot_control_used = true;
                restfreq = Some(take_value(&mut index, &args, "--restfreq")?)
            }
            "--veldef" => {
                plot_control_used = true;
                veldef = take_value(&mut index, &args, "--veldef")?
            }
            "--iteraxis" => {
                plot_control_used = true;
                iteraxis = Some(MsIterationAxis::parse(&take_value(
                    &mut index,
                    &args,
                    "--iteraxis",
                )?)?)
            }
            "--gridrows" => {
                plot_control_used = true;
                gridrows = take_value(&mut index, &args, "--gridrows")?
                    .parse::<usize>()
                    .map_err(|_| "invalid integer value for --gridrows".to_string())?
            }
            "--gridcols" => {
                plot_control_used = true;
                gridcols = take_value(&mut index, &args, "--gridcols")?
                    .parse::<usize>()
                    .map_err(|_| "invalid integer value for --gridcols".to_string())?
            }
            "--xselfscale" => {
                plot_control_used = true;
                xselfscale = true
            }
            "--yselfscale" => {
                plot_control_used = true;
                yselfscale = true
            }
            "--xsharedaxis" => {
                plot_control_used = true;
                xsharedaxis = true
            }
            "--ysharedaxis" => {
                plot_control_used = true;
                ysharedaxis = true
            }
            "--title" => {
                plot_control_used = true;
                title = Some(take_value(&mut index, &args, "--title")?)
            }
            "--xlabel" => {
                plot_control_used = true;
                xlabel = Some(take_value(&mut index, &args, "--xlabel")?)
            }
            "--ylabel" => {
                plot_control_used = true;
                ylabel = Some(take_value(&mut index, &args, "--ylabel")?)
            }
            "--symbolsize" | "--marker-size" => {
                plot_control_used = true;
                symbol_size = Some(
                    take_value(&mut index, &args, raw.as_str())?
                        .parse::<u32>()
                        .map_err(|_| format!("invalid integer value for {raw}"))?,
                )
            }
            "--showlegend" => {
                plot_control_used = true;
                showlegend = true
            }
            "--legendposition" => {
                plot_control_used = true;
                legendposition =
                    serde_json::from_str::<MsLegendPosition>(&format!(
                        "\"{}\"",
                        take_value(&mut index, &args, "--legendposition")?
                    ))
                    .map_err(|_| {
                        "unsupported value for --legendposition; expected one of: upperRight, upperLeft, lowerRight, lowerLeft, exteriorRight, exteriorLeft, exteriorTop, exteriorBottom".to_string()
                    })?
            }
            "--showmajorgrid" => {
                plot_control_used = true;
                showmajorgrid = true
            }
            "--showminorgrid" => {
                plot_control_used = true;
                showminorgrid = true
            }
            "--headeritems" => headeritems = Some(take_value(&mut index, &args, "--headeritems")?),
            "--max-points" => {
                plot_control_used = true;
                max_points = take_value(&mut index, &args, "--max-points")?
                    .parse::<usize>()
                    .map_err(|_| "invalid integer value for --max-points".to_string())?
            }
            "--flag-action" => {
                flag_action = Some(
                    match take_value(&mut index, &args, "--flag-action")?
                        .to_ascii_lowercase()
                        .as_str()
                    {
                        "flag" => MsFlagAction::Flag,
                        "unflag" => MsFlagAction::Unflag,
                        other => {
                            return Err(format!(
                                "unsupported value for --flag-action {other:?}; expected flag or unflag"
                            ));
                        }
                    },
                )
            }
            "--flag-xmin" => {
                flag_xmin = Some(
                    take_value(&mut index, &args, "--flag-xmin")?
                        .parse::<f64>()
                        .map_err(|_| "invalid floating-point value for --flag-xmin".to_string())?,
                )
            }
            "--flag-xmax" => {
                flag_xmax = Some(
                    take_value(&mut index, &args, "--flag-xmax")?
                        .parse::<f64>()
                        .map_err(|_| "invalid floating-point value for --flag-xmax".to_string())?,
                )
            }
            "--flag-ymin" => {
                flag_ymin = Some(
                    take_value(&mut index, &args, "--flag-ymin")?
                        .parse::<f64>()
                        .map_err(|_| "invalid floating-point value for --flag-ymin".to_string())?,
                )
            }
            "--flag-ymax" => {
                flag_ymax = Some(
                    take_value(&mut index, &args, "--flag-ymax")?
                        .parse::<f64>()
                        .map_err(|_| "invalid floating-point value for --flag-ymax".to_string())?,
                )
            }
            "--flag-plotindex" => {
                flag_plotindex = Some(
                    take_value(&mut index, &args, "--flag-plotindex")?
                        .parse::<usize>()
                        .map_err(|_| "invalid integer value for --flag-plotindex".to_string())?,
                )
            }
            "--flag-panel" => flag_panel = Some(take_value(&mut index, &args, "--flag-panel")?),
            "--flag-extcorr" => flag_extcorr = true,
            "--flag-extchannel" => flag_extchannel = true,
            "--flag-selected" => flag_selected = true,
            "--flag-apply" => flag_apply = true,
            "--flag-output" => {
                flag_output = Some(PathBuf::from(take_value(
                    &mut index,
                    &args,
                    "--flag-output",
                )?))
            }
            "--plot-output" => {
                plot_output = Some(PathBuf::from(take_value(
                    &mut index,
                    &args,
                    "--plot-output",
                )?))
            }
            "--plot-format" => {
                plot_format =
                    MsExportFormat::parse(&take_value(&mut index, &args, "--plot-format")?)?
            }
            "--plot-width" => {
                plot_width = take_value(&mut index, &args, "--plot-width")?
                    .parse::<u32>()
                    .map_err(|_| "invalid integer value for --plot-width".to_string())?
            }
            "--plot-height" => {
                plot_height = take_value(&mut index, &args, "--plot-height")?
                    .parse::<u32>()
                    .map_err(|_| "invalid integer value for --plot-height".to_string())?
            }
            value if value.starts_with('-') => return Err(format!("unknown option {value:?}")),
            value => {
                if ms_path.is_some() {
                    return Err("expected exactly one MeasurementSet path".to_string());
                }
                ms_path = Some(PathBuf::from(value));
            }
        }
        index += 1;
    }

    let ms_path = ms_path.ok_or_else(|| "missing required MeasurementSet path".to_string())?;
    let summary_format = match summary_format.as_str() {
        "text" => MeasurementSetSummaryOutputFormat::Text,
        "json" => MeasurementSetSummaryOutputFormat::Json,
        other => {
            return Err(format!(
                "unsupported format {other:?}; expected text or json"
            ));
        }
    };
    if page_spec.is_some() && plot_control_used {
        return Err(
            "--page-spec cannot be combined with the single-plot preset/axis/layout flags; put those settings in the JSON page spec instead".to_string(),
        );
    }
    if plot_output.is_some()
        && page_spec.is_none()
        && preset.is_none()
        && (x_axis.is_none() || y_axis.is_none())
    {
        return Err(
            "--plot-output requires either --preset or both --xaxis and --yaxis".to_string(),
        );
    }
    if flag_action.is_some()
        && !flag_selected
        && page_spec.is_none()
        && preset.is_none()
        && (x_axis.is_none() || y_axis.is_none())
    {
        return Err(
            "--flag-action requires either --preset or both --xaxis and --yaxis".to_string(),
        );
    }
    if flag_plotindex.is_some() && flag_panel.is_some() {
        return Err(
            "msexplore staged flag editing accepts either --flag-plotindex or --flag-panel, not both".to_string(),
        );
    }
    if flag_selected
        && (flag_xmin.is_some()
            || flag_xmax.is_some()
            || flag_ymin.is_some()
            || flag_ymax.is_some()
            || flag_plotindex.is_some()
            || flag_panel.is_some()
            || flag_extcorr
            || flag_extchannel)
    {
        return Err(
            "msexplore --flag-selected cannot be combined with plot-region flag-edit controls"
                .to_string(),
        );
    }
    if flag_action.is_none()
        && (flag_xmin.is_some()
            || flag_xmax.is_some()
            || flag_ymin.is_some()
            || flag_ymax.is_some()
            || flag_plotindex.is_some()
            || flag_panel.is_some()
            || flag_extcorr
            || flag_extchannel
            || flag_selected
            || flag_apply
            || flag_output.is_some())
    {
        return Err(
            "msexplore flag-edit region, extension, apply, and output flags require --flag-action"
                .to_string(),
        );
    }
    Ok(CliAction::Run(Box::new(CliOptions {
        ms_path,
        summary_format,
        summary_output,
        overwrite,
        selection,
        page_spec,
        preset,
        x_axis,
        y_axis,
        y_axis2,
        data_column,
        color_by,
        avgchannel,
        avgtime,
        avgscan,
        avgfield,
        avgbaseline,
        avgantenna,
        avgspw,
        scalar,
        freqframe,
        restfreq,
        veldef,
        iteraxis,
        gridrows: gridrows.max(1),
        gridcols: gridcols.max(1),
        xselfscale,
        yselfscale,
        xsharedaxis,
        ysharedaxis,
        title,
        xlabel,
        ylabel,
        symbol_size,
        showlegend,
        legendposition,
        showmajorgrid,
        showminorgrid,
        headeritems,
        max_points,
        flag_action,
        flag_xmin,
        flag_xmax,
        flag_ymin,
        flag_ymax,
        flag_plotindex,
        flag_panel,
        flag_extcorr,
        flag_extchannel,
        flag_selected,
        flag_apply,
        flag_output,
        plot_output,
        plot_format,
        plot_width: plot_width.max(1),
        plot_height: plot_height.max(1),
    })))
}

fn render_help(schema: &UiCommandSchema) -> String {
    format!(
        "{}\n\n{}\n",
        schema.render_help(),
        casa_task_runtime::task_cli_machine_help("MsExploreTaskRequest")
    )
}

fn write_output(path: Option<&std::path::Path>, overwrite: bool, text: &str) -> Result<(), String> {
    match path {
        Some(path) => {
            if path.exists() && !overwrite {
                return Err(format!(
                    "refusing to overwrite existing output {}; pass --overwrite to replace it",
                    path.display()
                ));
            }
            fs::write(path, text).map_err(|error| error.to_string())
        }
        None => {
            print!("{text}");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use tempfile::tempdir;

    use super::*;

    fn cli_args(args: &[&str]) -> Vec<OsString> {
        args.iter().map(OsString::from).collect()
    }

    #[test]
    fn command_schema_describes_core_arguments_and_actions() {
        let schema = command_schema("msexplore-test");

        assert_eq!(schema.command_id, "msexplore");
        assert_eq!(schema.invocation_name, "msexplore-test");
        assert_eq!(schema.display_name, "MSExplore");
        assert_eq!(schema.category, "MeasurementSet");
        assert_eq!(schema.usage, "msexplore-test [parameters]");
        assert!(schema.render_help().contains("<ms-path>"));
        assert!(schema.render_help().contains("--plot-output"));

        let ms_path = schema.argument("vis").expect("canonical vis argument");
        assert!(matches!(
            ms_path.parser,
            UiArgumentParser::Positional { .. }
        ));
        assert_eq!(ms_path.group, "Input");
        assert!(ms_path.required);

        let format = schema.argument("format").expect("format argument");
        assert_eq!(format.default.as_deref(), Some("text"));
        assert_eq!(format.group, "Output");
        assert!(matches!(format.value_kind, UiValueKind::Choice));

        let selectdata = schema.argument("selectdata").expect("selectdata argument");
        assert_eq!(selectdata.default_bool(), Some(true));
        assert_eq!(selectdata.group, "Selection");
        assert!(matches!(selectdata.parser, UiArgumentParser::Toggle { .. }));

        let help = schema.argument("help").expect("help action");
        assert!(matches!(
            help.parser,
            UiArgumentParser::Action {
                action: UiActionKind::Help,
                ..
            }
        ));

        let managed_output = schema.managed_output.expect("managed output");
        assert_eq!(managed_output.renderer, "measurementset-summary-v1");
        assert_eq!(managed_output.stdout_format, "json");
        assert!(managed_output.raw_stdout_available);
        assert!(managed_output.raw_stderr_available);
        assert!(
            managed_output
                .inject_arguments
                .iter()
                .any(|argument| argument.flag == "--format" && argument.value == "json")
        );
    }

    #[test]
    fn parse_args_captures_dense_single_plot_configuration() {
        let action = parse_args(cli_args(&[
            "--format",
            "json",
            "-o",
            "summary.json",
            "--overwrite",
            "--field",
            "0",
            "--spw",
            "0:3~7",
            "--timerange",
            "09:00:00~10:00:00",
            "--uvrange",
            ">100m",
            "--antenna",
            "DV01&DV02",
            "--scan",
            "5",
            "--correlation",
            "RR,LL",
            "--observation",
            "0",
            "--array",
            "1",
            "--intent",
            "CALIBRATE_BANDPASS",
            "--feed",
            "0",
            "--msselect",
            "DATA_DESC_ID==0",
            "--preset",
            "amp_frequency",
            "--data-column",
            "corrected_div_model_scalar",
            "--color-by",
            "corr",
            "--avgchannel",
            "8",
            "--avgtime",
            "30.5",
            "--avgscan",
            "--avgfield",
            "--avgbaseline",
            "--scalar",
            "--freqframe",
            "BARY",
            "--restfreq",
            "1.420GHz",
            "--veldef",
            "OPTICAL",
            "--iteraxis",
            "spw",
            "--gridrows",
            "2",
            "--gridcols",
            "3",
            "--xselfscale",
            "--ysharedaxis",
            "--title",
            "Amplitude",
            "--xlabel",
            "Time",
            "--ylabel",
            "Amp",
            "--symbolsize",
            "2",
            "--showlegend",
            "--legendposition",
            "exteriorRight",
            "--showmajorgrid",
            "--showminorgrid",
            "--headeritems",
            "filename,observer",
            "--max-points",
            "12345",
            "--plot-output",
            "plot.pdf",
            "--plot-format",
            "pdf",
            "--plot-width",
            "1024",
            "--plot-height",
            "768",
            "example.ms",
        ]))
        .expect("parse args");

        let CliAction::Run(options) = action else {
            panic!("expected run action");
        };
        assert_eq!(options.ms_path, PathBuf::from("example.ms"));
        assert_eq!(
            options.summary_format,
            MeasurementSetSummaryOutputFormat::Json
        );
        assert_eq!(
            options.summary_output.as_deref(),
            Some(std::path::Path::new("summary.json"))
        );
        assert!(options.overwrite);
        assert_eq!(options.selection.field.as_deref(), Some("0"));
        assert_eq!(options.selection.spw.as_deref(), Some("0:3~7"));
        assert_eq!(
            options.selection.timerange.as_deref(),
            Some("09:00:00~10:00:00")
        );
        assert_eq!(options.selection.uvrange.as_deref(), Some(">100m"));
        assert_eq!(options.selection.antenna.as_deref(), Some("DV01&DV02"));
        assert_eq!(options.selection.scan.as_deref(), Some("5"));
        assert_eq!(options.selection.correlation.as_deref(), Some("RR,LL"));
        assert_eq!(options.selection.observation.as_deref(), Some("0"));
        assert_eq!(options.selection.array.as_deref(), Some("1"));
        assert_eq!(
            options.selection.intent.as_deref(),
            Some("CALIBRATE_BANDPASS")
        );
        assert_eq!(options.selection.feed.as_deref(), Some("0"));
        assert_eq!(
            options.selection.msselect.as_deref(),
            Some("DATA_DESC_ID==0")
        );
        assert_eq!(options.preset, Some(MsPlotPreset::AmplitudeVsFrequency));
        assert_eq!(options.data_column, MsDataColumn::CorrectedDivModelScalar);
        assert_eq!(options.color_by, MsColorAxis::Correlation);
        assert_eq!(options.avgchannel, Some(8));
        assert_eq!(options.avgtime, Some(30.5));
        assert!(options.avgscan);
        assert!(options.avgfield);
        assert!(options.avgbaseline);
        assert!(!options.avgantenna);
        assert!(!options.avgspw);
        assert!(options.scalar);
        assert_eq!(options.freqframe.as_deref(), Some("BARY"));
        assert_eq!(options.restfreq.as_deref(), Some("1.420GHz"));
        assert_eq!(options.veldef, "OPTICAL");
        assert_eq!(options.iteraxis, Some(MsIterationAxis::SpectralWindow));
        assert_eq!(options.gridrows, 2);
        assert_eq!(options.gridcols, 3);
        assert!(options.xselfscale);
        assert!(!options.yselfscale);
        assert!(!options.xsharedaxis);
        assert!(options.ysharedaxis);
        assert_eq!(options.title.as_deref(), Some("Amplitude"));
        assert_eq!(options.xlabel.as_deref(), Some("Time"));
        assert_eq!(options.ylabel.as_deref(), Some("Amp"));
        assert_eq!(options.symbol_size, Some(2));
        assert!(options.showlegend);
        assert_eq!(options.legendposition, MsLegendPosition::ExteriorRight);
        assert!(options.showmajorgrid);
        assert!(options.showminorgrid);
        assert_eq!(options.headeritems.as_deref(), Some("filename,observer"));
        assert_eq!(options.max_points, 12345);
        assert_eq!(
            options.plot_output.as_deref(),
            Some(std::path::Path::new("plot.pdf"))
        );
        assert_eq!(options.plot_format, MsExportFormat::Pdf);
        assert_eq!(options.plot_width, 1024);
        assert_eq!(options.plot_height, 768);

        let spec = build_explore_spec(&options).expect("build spec");
        assert_eq!(spec.ms_path, PathBuf::from("example.ms"));
        assert_eq!(spec.summary_format, MeasurementSetSummaryOutputFormat::Json);
        assert_eq!(
            spec.header_items,
            vec![MsPageHeaderItem::Filename, MsPageHeaderItem::Observer]
        );
        assert_eq!(spec.max_plot_points, 12345);
        assert_eq!(spec.plots.len(), 1);
        let plot = &spec.plots[0];
        assert_eq!(plot.preset, Some(MsPlotPreset::AmplitudeVsFrequency));
        assert_eq!(plot.data_column, MsDataColumn::CorrectedDivModelScalar);
        assert_eq!(plot.color_by, MsColorAxis::Correlation);
        assert_eq!(plot.averaging.avgchannel, Some(8));
        assert_eq!(plot.averaging.avgtime, Some(30.5));
        assert!(plot.averaging.avgscan);
        assert!(plot.averaging.avgfield);
        assert!(plot.averaging.avgbaseline);
        assert!(!plot.averaging.avgantenna);
        assert!(!plot.averaging.avgspw);
        assert!(plot.averaging.scalar);
        assert_eq!(plot.transforms.freqframe.as_deref(), Some("BARY"));
        assert_eq!(plot.transforms.restfreq.as_deref(), Some("1.420GHz"));
        assert_eq!(plot.transforms.veldef, "OPTICAL");
        assert_eq!(
            plot.iteration.iteraxis,
            Some(MsIterationAxis::SpectralWindow)
        );
        assert!(plot.iteration.xselfscale);
        assert!(!plot.iteration.yselfscale);
        assert!(!plot.iteration.xsharedaxis);
        assert!(plot.iteration.ysharedaxis);
        assert_eq!(plot.style.title.as_deref(), Some("Amplitude"));
        assert_eq!(plot.style.xlabel.as_deref(), Some("Time"));
        assert_eq!(plot.style.ylabel.as_deref(), Some("Amp"));
        assert_eq!(plot.style.symbol_size, Some(2));
        assert!(plot.style.showlegend);
        assert_eq!(plot.style.legendposition, MsLegendPosition::ExteriorRight);
        assert!(plot.style.showmajorgrid);
        assert!(plot.style.showminorgrid);
    }

    #[test]
    fn page_spec_build_merges_header_items_without_duplicates() {
        let temp = tempdir().expect("tempdir");
        let page_spec_path = temp.path().join("page.json");
        fs::write(
            &page_spec_path,
            r#"{
              "gridrows": 2,
              "gridcols": 2,
              "page_title": "Page Title",
              "exprange": "all",
              "headeritems": "filename,ycolumn",
              "plots": [
                {
                  "preset": "amplitude_vs_frequency",
                  "iteraxis": "field",
                  "rowindex": 0,
                  "colindex": 0,
                  "plotindex": 0,
                  "showlegend": true,
                  "legendposition": "upperLeft",
                  "showmajorgrid": true,
                  "showminorgrid": true,
                  "symbol_size": 1
                },
                {
                  "preset": "amplitude_vs_frequency",
                  "iteraxis": "field",
                  "rowindex": 0,
                  "colindex": 1,
                  "plotindex": 1
                }
              ]
            }"#,
        )
        .expect("write page spec");

        let action = parse_args(vec![
            OsString::from("--format"),
            OsString::from("json"),
            OsString::from("--page-spec"),
            page_spec_path.as_os_str().to_os_string(),
            OsString::from("--headeritems"),
            OsString::from("telescope,filename"),
            OsString::from("example.ms"),
        ])
        .expect("parse args");

        let CliAction::Run(options) = action else {
            panic!("expected run action");
        };
        let spec = build_explore_spec(&options).expect("build spec from page spec");

        assert_eq!(spec.ms_path, PathBuf::from("example.ms"));
        assert_eq!(spec.summary_format, MeasurementSetSummaryOutputFormat::Json);
        assert_eq!(spec.page_title.as_deref(), Some("Page Title"));
        assert_eq!(spec.exprange, MsPageExportRange::All);
        assert_eq!(
            spec.header_items,
            vec![
                MsPageHeaderItem::Filename,
                MsPageHeaderItem::YColumn,
                MsPageHeaderItem::Telescope,
            ]
        );
        assert_eq!(spec.plots.len(), 2);
        let plot = &spec.plots[0];
        assert_eq!(plot.preset, Some(MsPlotPreset::AmplitudeVsFrequency));
        assert_eq!(plot.layout.gridrows, 2);
        assert_eq!(plot.layout.gridcols, 2);
        assert_eq!(plot.layout.rowindex, 0);
        assert_eq!(plot.layout.colindex, 0);
        assert_eq!(plot.layout.plotindex, 0);
        assert!(plot.style.showlegend);
        assert_eq!(plot.style.legendposition, MsLegendPosition::UpperLeft);
        assert!(plot.style.showmajorgrid);
        assert!(plot.style.showminorgrid);
        assert_eq!(plot.style.symbol_size, Some(1));
        let second_plot = &spec.plots[1];
        assert_eq!(second_plot.preset, Some(MsPlotPreset::AmplitudeVsFrequency));
        assert_eq!(second_plot.layout.colindex, 1);
        assert_eq!(second_plot.layout.plotindex, 1);
    }

    #[test]
    fn parse_and_build_helpers_reject_invalid_combinations() {
        assert!(
            parse_args(cli_args(&["--flag-xmin", "0.0", "example.ms"]))
                .unwrap_err()
                .contains("require --flag-action")
        );
        assert!(
            parse_args(cli_args(&[
                "--page-spec",
                "page.json",
                "--preset",
                "uv",
                "example.ms"
            ]))
            .unwrap_err()
            .contains("--page-spec cannot be combined")
        );
        assert!(
            parse_args(cli_args(&["--plot-output", "plot.png", "example.ms"]))
                .unwrap_err()
                .contains("--plot-output requires either --preset or both --xaxis and --yaxis")
        );
        assert!(
            parse_args(cli_args(&[
                "--flag-action",
                "flag",
                "--flag-plotindex",
                "0",
                "--flag-panel",
                "panel",
                "--preset",
                "uv",
                "example.ms"
            ]))
            .unwrap_err()
            .contains("either --flag-plotindex or --flag-panel")
        );
        assert!(
            build_plot_spec_from_values(
                Some(MsPlotPreset::AmplitudePhaseVsTimeStacked),
                None,
                None,
                Some(MsAxis::Phase),
                MsDataColumn::Data,
                MsColorAxis::Field,
                None,
                None,
                false,
                false,
                false,
                false,
                false,
                false,
                None,
                None,
                "RADIO".to_string(),
                None,
                1,
                1,
                0,
                0,
                0,
                false,
                false,
                false,
                false,
                None,
                None,
                None,
                None,
                false,
                MsLegendPosition::UpperRight,
                false,
                false,
            )
            .unwrap_err()
            .contains("--yaxis2 is not supported")
        );
        let action = parse_args(cli_args(&[
            "--preset",
            "amp_time",
            "--xselfscale",
            "--xsharedaxis",
            "example.ms",
        ]))
        .expect("parse args");
        let CliAction::Run(options) = action else {
            panic!("expected run action");
        };
        assert!(
            build_explore_spec(&options)
                .unwrap_err()
                .contains("cannot request both self-scaled and shared axes")
        );
        let action = parse_args(cli_args(&[
            "--preset",
            "amp_time",
            "--avgantenna",
            "--avgbaseline",
            "example.ms",
        ]))
        .expect("parse args");
        let CliAction::Run(options) = action else {
            panic!("expected run action");
        };
        assert!(
            build_explore_spec(&options)
                .unwrap_err()
                .contains("mutually exclusive")
        );
        let action = parse_args(cli_args(&[
            "--preset",
            "amp_time",
            "--iteraxis",
            "spw",
            "--avgspw",
            "example.ms",
        ]))
        .expect("parse args");
        let CliAction::Run(options) = action else {
            panic!("expected run action");
        };
        assert!(
            build_explore_spec(&options).unwrap_err().contains(
                "cannot iterate by spectral window while averaging across spectral windows"
            )
        );
    }

    #[test]
    fn build_explore_spec_strips_shared_logging_controls() {
        let spec = build_explore_spec_from_args(cli_args(&[
            "--log-table",
            "msexplore.log",
            "--log-table-priority",
            "INFO",
            "--log-stderr-priority=off",
            "--preset",
            "amp_time",
            "example.ms",
        ]))
        .expect("build spec with logging controls");

        assert_eq!(spec.ms_path, PathBuf::from("example.ms"));
        assert_eq!(
            spec.plots.first().expect("plot spec").preset,
            Some(MsPlotPreset::AmplitudeVsTime)
        );
    }

    #[test]
    fn build_flag_edit_spec_and_write_output_handle_expected_edges() {
        let action = parse_args(cli_args(&[
            "--preset",
            "amp_time",
            "--flag-action",
            "flag",
            "--flag-xmin",
            "1.0",
            "--flag-xmax",
            "2.0",
            "--flag-ymin",
            "3.0",
            "--flag-ymax",
            "4.0",
            "--flag-plotindex",
            "7",
            "--flag-extcorr",
            "--flag-extchannel",
            "example.ms",
        ]))
        .expect("parse args");
        let CliAction::Run(options) = action else {
            panic!("expected run action");
        };
        let flag_edit = build_flag_edit_spec(&options)
            .expect("build flag edit spec")
            .expect("flag edit");
        assert_eq!(flag_edit.action, MsFlagAction::Flag);
        assert_eq!(flag_edit.region.x_min, 1.0);
        assert_eq!(flag_edit.region.x_max, 2.0);
        assert_eq!(flag_edit.region.y_min, 3.0);
        assert_eq!(flag_edit.region.y_max, 4.0);
        assert_eq!(flag_edit.plot_index, Some(7));
        assert!(flag_edit.extcorr);
        assert!(flag_edit.extchannel);

        let temp = tempdir().expect("tempdir");
        let output_path = temp.path().join("out.txt");
        write_output(Some(&output_path), false, "first").expect("write first");
        assert_eq!(fs::read_to_string(&output_path).unwrap(), "first");
        assert!(
            write_output(Some(&output_path), false, "second")
                .unwrap_err()
                .contains("refusing to overwrite")
        );
        write_output(Some(&output_path), true, "second").expect("overwrite");
        assert_eq!(fs::read_to_string(&output_path).unwrap(), "second");
    }
}
