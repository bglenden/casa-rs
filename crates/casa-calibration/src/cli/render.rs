// SPDX-License-Identifier: LGPL-3.0-or-later
//! A calibration run's report as a command line prints it: the
//! human-readable text, or the report itself as JSON.

use std::fmt::Write;
use std::path::Path;

use super::read::OutputFormat;
use crate::{
    ApplyExecutionReport, ApplyPlan, BandpassSolveReport, CalibrationStatsReport,
    CalibrationTableSummary, CalibrationTaskResult, ExportCorrectedDataReport, FluxScaleReport,
    GainSolveReport, GencalReport,
};

/// `result` in `format`. JSON is the workflow's own report; the task reply
/// carries the tagged [`CalibrationTaskResult`].
pub(super) fn render(
    format: OutputFormat,
    result: &CalibrationTaskResult,
) -> Result<String, String> {
    match format {
        OutputFormat::Text => Ok(render_text(result)),
        OutputFormat::Json => render_json(result),
    }
}

fn render_json(result: &CalibrationTaskResult) -> Result<String, String> {
    let report = match result {
        CalibrationTaskResult::Apply(report) => serde_json::to_string_pretty(report),
        CalibrationTaskResult::ExportCorrectedData(report) => serde_json::to_string_pretty(report),
        CalibrationTaskResult::Summary(report) => serde_json::to_string_pretty(report),
        CalibrationTaskResult::PlanApply(report) => serde_json::to_string_pretty(report),
        CalibrationTaskResult::Stats(report) => serde_json::to_string_pretty(report),
        CalibrationTaskResult::SolveGain(report) => serde_json::to_string_pretty(report),
        CalibrationTaskResult::SolveBandpass(report) => serde_json::to_string_pretty(report),
        CalibrationTaskResult::FluxScale(report) => serde_json::to_string_pretty(report),
        CalibrationTaskResult::Gencal(report) => serde_json::to_string_pretty(report),
    };
    report
        .map(|json| json + "\n")
        .map_err(|error| format!("serialize the calibration report: {error}"))
}

fn render_text(result: &CalibrationTaskResult) -> String {
    match result {
        CalibrationTaskResult::Apply(report) => render_apply_report_text(report),
        CalibrationTaskResult::ExportCorrectedData(report) => {
            render_export_corrected_data_report_text(report)
        }
        CalibrationTaskResult::Summary(report) => render_summary_text(report),
        CalibrationTaskResult::PlanApply(report) => render_apply_plan_text(report),
        CalibrationTaskResult::Stats(report) => render_stats_text(report),
        CalibrationTaskResult::SolveGain(report) => render_gain_solve_report_text(report),
        CalibrationTaskResult::SolveBandpass(report) => render_bandpass_solve_report_text(report),
        CalibrationTaskResult::FluxScale(report) => render_fluxscale_report_text(report),
        CalibrationTaskResult::Gencal(report) => render_gencal_report_text(report),
    }
}

pub(super) fn render_summary_text(summaries: &[CalibrationTableSummary]) -> String {
    let mut out = String::new();
    for (index, summary) in summaries.iter().enumerate() {
        if index > 0 {
            out.push('\n');
        }
        let _ = writeln!(out, "Calibration Table: {}", summary.path.display());
        let _ = writeln!(
            out,
            "  type={} subtype={}",
            summary.table_type, summary.table_subtype
        );
        let _ = writeln!(out, "  rows={}", summary.row_count);
        let _ = writeln!(out, "  par_type={:?}", summary.keywords.par_type);
        let _ = writeln!(out, "  vis_cal={:?}", summary.keywords.vis_cal);
        let _ = writeln!(out, "  parameter_family={:?}", summary.parameter_family);
        let _ = writeln!(
            out,
            "  supported_for_v1_apply={}",
            summary.supported_for_v1_apply()
        );
        let _ = writeln!(out, "  columns={}", summary.columns.join(", "));
        let _ = writeln!(out, "  field_ids={:?}", summary.field_ids);
        let _ = writeln!(
            out,
            "  spectral_window_ids={:?}",
            summary.spectral_window_ids
        );
        let _ = writeln!(out, "  antenna1_ids={:?}", summary.antenna1_ids);
        let _ = writeln!(out, "  antenna2_ids={:?}", summary.antenna2_ids);
        let _ = writeln!(out, "  observation_ids={:?}", summary.observation_ids);
        if let Some(time) = &summary.time_coverage {
            let _ = writeln!(
                out,
                "  time_coverage=[{}, {}] interval={:?}..{:?}",
                time.min_time, time.max_time, time.min_interval, time.max_interval
            );
        }
        for subtable in &summary.subtables {
            let _ = writeln!(
                out,
                "  subtable {} exists={} rows={:?} path={}",
                subtable.name,
                subtable.exists,
                subtable.row_count,
                subtable
                    .resolved_path
                    .as_deref()
                    .unwrap_or_else(|| Path::new("<missing>"))
                    .display()
            );
        }
        if summary.issues.is_empty() {
            let _ = writeln!(out, "  issues=none");
        } else {
            let _ = writeln!(out, "  issues:");
            for issue in &summary.issues {
                let _ = writeln!(
                    out,
                    "    - {:?} {}: {}",
                    issue.severity, issue.code, issue.message
                );
            }
        }
    }
    out
}

pub(super) fn render_apply_report_text(report: &ApplyExecutionReport) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "Calibration Apply Report: {}",
        report
            .plan
            .measurement_set_path
            .as_deref()
            .unwrap_or_else(|| Path::new("<in-memory>"))
            .display()
    );
    let _ = writeln!(out, "  apply_mode={:?}", report.plan.apply_mode);
    let _ = writeln!(out, "  parang={}", report.plan.parang);
    let _ = writeln!(out, "  selected_rows={}", report.plan.selected_row_count);
    let _ = writeln!(
        out,
        "  created_corrected_data_column={}",
        report.created_corrected_data_column
    );
    let _ = writeln!(
        out,
        "  wrote_measurement_set={}",
        report.wrote_measurement_set
    );
    let _ = writeln!(out, "  updated_rows={}", report.updated_row_count);
    let _ = writeln!(out, "  flagged_rows={}", report.flagged_row_count);
    let _ = writeln!(out, "  flagged_samples={}", report.flagged_sample_count);
    if let Some(version) = &report.flag_backup_version {
        let _ = writeln!(out, "  flag_backup={version}");
    }
    let _ = writeln!(out, "  timings:");
    let _ = writeln!(
        out,
        "    total={} planning={} open_ms={}",
        format_duration_ns(report.timings.total_ns),
        format_duration_ns(report.timings.planning_ns),
        format_duration_ns(report.timings.open_measurement_set_ns)
    );
    let _ = writeln!(
        out,
        "    planning.selection={} planning.selected_rows={} planning.ms_spws={} planning.table_plans={}",
        format_duration_ns(report.timings.planning_selection_ns),
        format_duration_ns(report.timings.planning_selected_rows_ns),
        format_duration_ns(report.timings.planning_measurement_set_spectral_windows_ns),
        format_duration_ns(report.timings.planning_calibration_table_plans_ns)
    );
    let _ = writeln!(
        out,
        "    ensure_corrected_data={} correlation_lookup={} calibration_load={}",
        format_duration_ns(report.timings.ensure_corrected_data_ns),
        format_duration_ns(report.timings.correlation_lookup_ns),
        format_duration_ns(report.timings.calibration_load_ns)
    );
    let _ = writeln!(
        out,
        "    row_compute={} row_writeback={} save={}",
        format_duration_ns(report.timings.row_compute_ns),
        format_duration_ns(report.timings.row_writeback_ns),
        format_duration_ns(report.timings.save_ns)
    );
    for table in &report.plan.calibration_tables {
        let _ = writeln!(out, "  table {}", table.spec.path.display());
        let _ = writeln!(
            out,
            "    applicable_selected_rows={}",
            table.applicable_selected_row_count
        );
        let _ = writeln!(out, "    interp={:?}", table.interp);
        let _ = writeln!(out, "    calwt={}", table.calwt);
        if !table.spec.apply_to.is_empty() {
            let _ = writeln!(
                out,
                "    apply_to={}",
                format_apply_table_selection(&table.spec.apply_to)
            );
        }
        if let Some(gainfield) = &table.resolved_gainfield {
            let _ = writeln!(
                out,
                "    gainfield={} {:?}",
                gainfield.field_id, gainfield.field_name
            );
        }
        for nearest in &table.resolved_nearest_gainfields {
            let _ = writeln!(
                out,
                "    gainfield[ms_field={}] -> {} {:?} sep={:.6}rad",
                nearest.measurement_set_field_id,
                nearest.calibration_field_id,
                nearest.calibration_field_name,
                nearest.angular_separation_rad
            );
        }
    }
    out
}

pub(super) fn format_duration_ns(duration_ns: u64) -> String {
    if duration_ns >= 1_000_000_000 {
        format!("{:.3}s", duration_ns as f64 / 1_000_000_000.0)
    } else if duration_ns >= 1_000_000 {
        format!("{:.3}ms", duration_ns as f64 / 1_000_000.0)
    } else if duration_ns >= 1_000 {
        format!("{:.3}us", duration_ns as f64 / 1_000.0)
    } else {
        format!("{duration_ns}ns")
    }
}

pub(super) fn render_apply_plan_text(plan: &ApplyPlan) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "Apply Plan: {}",
        plan.measurement_set_path
            .as_deref()
            .unwrap_or_else(|| Path::new("<in-memory>"))
            .display()
    );
    let _ = writeln!(out, "  apply_mode={:?}", plan.apply_mode);
    let _ = writeln!(out, "  parang={}", plan.parang);
    let _ = writeln!(
        out,
        "  requires_corrected_data_column={}",
        plan.requires_corrected_data_column
    );
    let _ = writeln!(out, "  selected_rows={}", plan.selected_row_count);
    let _ = writeln!(out, "  selected_fields={:?}", plan.selected_field_ids);
    let _ = writeln!(
        out,
        "  selected_data_desc_ids={:?}",
        plan.selected_data_desc_ids
    );
    let _ = writeln!(out, "  selected_data_spws={:?}", plan.selected_data_spw_ids);
    for table in &plan.calibration_tables {
        let _ = writeln!(out, "  table {}", table.spec.path.display());
        let _ = writeln!(out, "    vis_cal={:?}", table.summary.keywords.vis_cal);
        let _ = writeln!(
            out,
            "    applicable_selected_rows={}",
            table.applicable_selected_row_count
        );
        let _ = writeln!(out, "    interp={:?}", table.interp);
        let _ = writeln!(out, "    calwt={}", table.calwt);
        if !table.spec.apply_to.is_empty() {
            let _ = writeln!(
                out,
                "    apply_to={}",
                format_apply_table_selection(&table.spec.apply_to)
            );
        }
        if let Some(gainfield) = &table.resolved_gainfield {
            let _ = writeln!(
                out,
                "    gainfield={} {:?}",
                gainfield.field_id, gainfield.field_name
            );
        }
        for nearest in &table.resolved_nearest_gainfields {
            let _ = writeln!(
                out,
                "    gainfield[ms_field={}] -> {} {:?} sep={:.6}rad",
                nearest.measurement_set_field_id,
                nearest.calibration_field_id,
                nearest.calibration_field_name,
                nearest.angular_separation_rad
            );
        }
        for mapping in &table.spw_mapping {
            let _ = writeln!(
                out,
                "    spw {} -> {}",
                mapping.data_spw_id, mapping.calibration_spw_id
            );
        }
    }
    out
}

fn format_apply_table_selection(selection: &crate::ApplyTableSelection) -> String {
    let mut parts = Vec::new();
    if !selection.field_ids.is_empty() {
        parts.push(format!("field={:?}", selection.field_ids));
    }
    if !selection.spectral_window_ids.is_empty() {
        parts.push(format!("spw={:?}", selection.spectral_window_ids));
    }
    if !selection.observation_ids.is_empty() {
        parts.push(format!("obs={:?}", selection.observation_ids));
    }
    parts.join(" ")
}

pub(super) fn render_stats_text(report: &CalibrationStatsReport) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "Calibration Stats: {}", report.path.display());
    let _ = writeln!(out, "  axis={}", report.axis.display_name());
    let _ = writeln!(out, "  datacolumn={:?}", report.datacolumn);
    let _ = writeln!(out, "  rows={}", report.row_count);
    let _ = writeln!(out, "  global:");
    render_value_stats_block(&mut out, &report.global, 4);

    for (label, groups) in [
        ("field_id", &report.by_field_id),
        ("spectral_window_id", &report.by_spectral_window_id),
        ("antenna1_id", &report.by_antenna1_id),
        ("observation_id", &report.by_observation_id),
    ] {
        let _ = writeln!(out, "  grouped_by_{label}:");
        for group in groups {
            let _ = writeln!(out, "    {}={}", label, group.key);
            render_value_stats_block(&mut out, &group.stats, 6);
        }
    }

    out
}

pub(super) fn render_gain_solve_report_text(report: &GainSolveReport) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "Gain Solve Report: {}", report.output_table.display());
    let _ = writeln!(out, "  gain_type={:?}", report.gain_type);
    let _ = writeln!(out, "  refant_antenna_id={}", report.refant_antenna_id);
    let _ = writeln!(out, "  field_ids={:?}", report.field_ids);
    let _ = writeln!(
        out,
        "  spectral_window_ids={:?}",
        report.spectral_window_ids
    );
    let _ = writeln!(out, "  solution_rows={}", report.solution_row_count);
    out
}

fn render_export_corrected_data_report_text(report: &ExportCorrectedDataReport) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "Export Corrected Data Report: {}",
        report.output_ms.display()
    );
    let _ = writeln!(out, "  input_ms={}", report.input_ms.display());
    let _ = writeln!(out, "  rows={}", report.row_count);
    let _ = writeln!(
        out,
        "  copied {} -> {}",
        report.source_column, report.output_column
    );
    out
}

pub(super) fn render_bandpass_solve_report_text(report: &BandpassSolveReport) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "Bandpass Solve Report: {}",
        report.output_table.display()
    );
    let _ = writeln!(out, "  refant_antenna_id={}", report.refant_antenna_id);
    let _ = writeln!(out, "  field_ids={:?}", report.field_ids);
    let _ = writeln!(
        out,
        "  spectral_window_ids={:?}",
        report.spectral_window_ids
    );
    let _ = writeln!(out, "  solution_rows={}", report.solution_row_count);
    let _ = writeln!(out, "  channel_count={}", report.channel_count);
    out
}

pub(super) fn render_fluxscale_report_text(report: &FluxScaleReport) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "FluxScale Report: {}", report.output_table.display());
    let _ = writeln!(out, "  spw_ids={:?}", report.spw_ids);
    let _ = writeln!(out, "  frequencies_hz={:?}", report.frequencies_hz);
    for (field_id, field) in &report.fields {
        let _ = writeln!(out, "  field {field_id} ({})", field.field_name);
        for (spw_id, spw) in &field.spw_results {
            let _ = writeln!(
                out,
                "    spw {spw_id}: fluxd={:?} fluxd_err={:?} num_sol={:?}",
                spw.fluxd, spw.fluxd_err, spw.num_sol
            );
        }
    }
    out
}

fn render_gencal_report_text(report: &GencalReport) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "Gencal Report: {}", report.output_table.display());
    let _ = writeln!(out, "  caltype={:?}", report.caltype);
    let _ = writeln!(out, "  table_subtype={}", report.table_subtype);
    let _ = writeln!(out, "  rows={}", report.row_count);
    let _ = writeln!(
        out,
        "  spectral_window_ids={:?}",
        report.spectral_window_ids
    );
    let _ = writeln!(out, "  antenna_ids={:?}", report.antenna_ids);
    out
}

fn render_value_stats_block(out: &mut String, stats: &crate::CalibrationValueStats, indent: usize) {
    let prefix = " ".repeat(indent);
    let _ = writeln!(
        out,
        "{prefix}npts={} flagged_npts={} total_npts={}",
        stats.npts, stats.flagged_npts, stats.total_npts
    );
    let _ = writeln!(
        out,
        "{prefix}min={:.6} max={:.6} mean={:.6} median={:.6}",
        stats.min, stats.max, stats.mean, stats.median
    );
    let _ = writeln!(
        out,
        "{prefix}sum={:.6} sumsq={:.6} rms={:.6}",
        stats.sum, stats.sumsq, stats.rms
    );
    let _ = writeln!(
        out,
        "{prefix}stddev={:.6} var={:.6} medabsdevmed={:.6} quartile={:.6}",
        stats.stddev, stats.var, stats.medabsdevmed, stats.quartile
    );
}
