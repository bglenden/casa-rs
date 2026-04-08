// SPDX-License-Identifier: LGPL-3.0-or-later
//! Framework-owned workflow graph helpers for `WorkflowShell` apps.
//!
//! The shell uses these types to reason about ordered but revisitable stages,
//! versioned products, and iteration history without hard-coding a particular
//! domain such as calibration or imaging.

use std::collections::{BTreeMap, BTreeSet};

use casa_calibration::CalibrationPlotPreset;

pub(crate) const WORKFLOW_VALUE_LABEL_WIDTH: usize = 18;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkflowValueDisplay {
    pub label: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkflowArtifactDisplay {
    pub heading: String,
    pub detail_lines: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkflowArtifactGroupDisplay {
    pub title: String,
    pub items: Vec<WorkflowArtifactDisplay>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkflowDetailDisplay {
    pub label: String,
    pub value: String,
    pub indent: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkflowProductRowDisplay {
    pub family: String,
    pub revision: usize,
    pub display_name: String,
    pub stage_label: String,
    pub status_label: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkflowStageDisplay {
    pub label: String,
    pub status: WorkflowStageStatus,
    pub latest_revision: Option<usize>,
    pub stale_revisions: usize,
    pub run_count: usize,
    pub recommended: bool,
    pub selected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkflowOverviewDisplay {
    pub dataset_path: Option<String>,
    pub recommended_stage: Option<String>,
    pub selected_stage: String,
    pub active_products: usize,
    pub stale_products: usize,
    pub total_products: usize,
    pub guidance: String,
    pub latest_run_lines: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkflowDiagnosticSummaryDisplay {
    pub description: String,
    pub source_path: Option<String>,
    pub missing_source_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkflowCatalogEntryDisplay<T> {
    pub target: T,
    pub label: String,
    pub selected: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkflowCalibrationArtifactKind {
    GainLike,
    BandpassLike,
    CorrectedData,
}

pub(crate) fn render_workflow_value_display(entry: &WorkflowValueDisplay) -> String {
    format!(
        "{:<width$} {}",
        entry.label,
        entry.value,
        width = WORKFLOW_VALUE_LABEL_WIDTH
    )
}

pub(crate) fn render_workflow_artifact_groups(
    groups: &[WorkflowArtifactGroupDisplay],
    empty_message: &str,
) -> Vec<String> {
    let mut lines = vec!["Products".to_string()];
    let has_items = groups.iter().any(|group| !group.items.is_empty());
    if !has_items {
        lines.push(empty_message.to_string());
        return lines;
    }
    for group in groups.iter().filter(|group| !group.items.is_empty()) {
        lines.push(group.title.clone());
        for item in &group.items {
            lines.push(format!("  {}", item.heading));
            lines.extend(item.detail_lines.iter().map(|line| format!("     {line}")));
        }
    }
    lines
}

pub(crate) fn render_workflow_detail_display(entry: &WorkflowDetailDisplay) -> String {
    format!(
        "{space:>indent$}{:<width$} {}",
        entry.label,
        entry.value,
        space = "",
        indent = entry.indent,
        width = WORKFLOW_VALUE_LABEL_WIDTH.saturating_sub(entry.indent),
    )
}

pub(crate) fn render_workflow_product_row_display(entry: &WorkflowProductRowDisplay) -> String {
    format!(
        "{:<18} r{} {} [{} | {}]",
        entry.family, entry.revision, entry.display_name, entry.stage_label, entry.status_label
    )
}

pub(crate) fn render_workflow_stage_display(entry: &WorkflowStageDisplay) -> String {
    let mut parts = vec![entry.status.label().to_string()];
    if let Some(revision) = entry.latest_revision {
        parts.push(format!("r{revision}"));
    }
    if entry.stale_revisions > 0 {
        parts.push(format!("stale:{}", entry.stale_revisions));
    }
    if entry.run_count > 0 {
        parts.push(format!("runs:{}", entry.run_count));
    }
    if entry.recommended {
        parts.push("next".to_string());
    }
    if entry.selected {
        parts.push("selected".to_string());
    }
    let suffix = if parts.is_empty() {
        String::new()
    } else {
        format!("  [{}]", parts.join(" | "))
    };
    format!(
        "{:<width$} {}{}",
        "Stage",
        entry.label,
        suffix,
        width = WORKFLOW_VALUE_LABEL_WIDTH
    )
}

pub(crate) fn render_workflow_overview_lines(entry: &WorkflowOverviewDisplay) -> Vec<String> {
    let mut lines = vec!["Workflow Session".to_string()];
    if let Some(path) = &entry.dataset_path {
        lines.push(format!("Dataset: {path}"));
    }
    if let Some(stage) = &entry.recommended_stage {
        lines.push(format!("Recommended next stage: {stage}"));
    }
    lines.push(format!("Selected stage: {}", entry.selected_stage));
    lines.push(format!(
        "Products: active={} stale={} total={}",
        entry.active_products, entry.stale_products, entry.total_products
    ));
    lines.push(String::new());
    lines.push("Workflow shell".to_string());
    lines.push(entry.guidance.clone());
    if !entry.latest_run_lines.is_empty() {
        lines.push(String::new());
        lines.push("Latest run".to_string());
        lines.extend(
            entry
                .latest_run_lines
                .iter()
                .map(|line| format!("  {line}")),
        );
    }
    lines
}

pub(crate) fn render_workflow_diagnostic_summary(
    entry: &WorkflowDiagnosticSummaryDisplay,
) -> String {
    match (&entry.source_path, &entry.missing_source_message) {
        (Some(source_path), _) => format!("{} Source: {source_path}", entry.description),
        (None, Some(message)) => message.clone(),
        (None, None) => entry.description.clone(),
    }
}

pub(crate) fn workflow_calibration_catalog_entries(
    selected: Option<CalibrationPlotPreset>,
) -> Vec<WorkflowCatalogEntryDisplay<CalibrationPlotPreset>> {
    CalibrationPlotPreset::ALL
        .into_iter()
        .map(|preset| WorkflowCatalogEntryDisplay {
            target: preset,
            label: preset.display_name().to_string(),
            selected: selected == Some(preset),
        })
        .collect()
}

pub(crate) fn preferred_workflow_calibration_preset(
    kind: WorkflowCalibrationArtifactKind,
) -> CalibrationPlotPreset {
    match kind {
        WorkflowCalibrationArtifactKind::GainLike => CalibrationPlotPreset::GainPhaseVsTime,
        WorkflowCalibrationArtifactKind::BandpassLike => {
            CalibrationPlotPreset::BandpassAmplitudeVsFrequency
        }
        WorkflowCalibrationArtifactKind::CorrectedData => {
            CalibrationPlotPreset::CorrectedAmplitudeVsTime
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkflowProductStatus {
    Active,
    Stale,
    Superseded,
}

impl WorkflowProductStatus {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Stale => "stale",
            Self::Superseded => "superseded",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkflowStageStatus {
    Ready,
    Completed,
    Stale,
    Blocked,
}

impl WorkflowStageStatus {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Completed => "completed",
            Self::Stale => "stale",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkflowStageSpec {
    pub id: &'static str,
    pub label: &'static str,
    pub depends_on: &'static [&'static str],
    pub produces_product: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkflowRunSnapshot {
    pub stage_id: &'static str,
    pub sequence: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkflowProductSnapshot {
    pub stage_id: &'static str,
    pub revision: usize,
    pub status: WorkflowProductStatus,
    pub dependency_revisions: BTreeMap<&'static str, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkflowStageState {
    pub id: &'static str,
    pub label: &'static str,
    pub status: WorkflowStageStatus,
    pub recommended: bool,
    pub run_count: usize,
    pub latest_revision: Option<usize>,
    pub stale_revisions: usize,
}

pub(crate) fn derive_stage_states(
    specs: &[WorkflowStageSpec],
    runs: &[WorkflowRunSnapshot],
    products: &[WorkflowProductSnapshot],
) -> Vec<WorkflowStageState> {
    let latest_active_revision = latest_active_revisions(products);
    let latest_run = latest_run_sequence(runs);
    let mut states = specs
        .iter()
        .map(|spec| {
            let run_count = runs.iter().filter(|run| run.stage_id == spec.id).count();
            let latest_product = latest_product_for_stage(products, spec.id);
            let latest_revision = latest_product.map(|product| product.revision);
            let stale_revisions = products
                .iter()
                .filter(|product| {
                    product.stage_id == spec.id && product.status == WorkflowProductStatus::Stale
                })
                .count();
            let status = if latest_product
                .is_some_and(|product| product.status == WorkflowProductStatus::Stale)
            {
                WorkflowStageStatus::Stale
            } else if spec.produces_product {
                if latest_product
                    .is_some_and(|product| product.status == WorkflowProductStatus::Active)
                {
                    WorkflowStageStatus::Completed
                } else if dependencies_satisfied(
                    spec.depends_on,
                    &latest_active_revision,
                    &latest_run,
                ) {
                    WorkflowStageStatus::Ready
                } else {
                    WorkflowStageStatus::Blocked
                }
            } else if run_count > 0
                && dependencies_satisfied(spec.depends_on, &latest_active_revision, &latest_run)
            {
                WorkflowStageStatus::Completed
            } else if dependencies_satisfied(spec.depends_on, &latest_active_revision, &latest_run)
            {
                WorkflowStageStatus::Ready
            } else {
                WorkflowStageStatus::Blocked
            };

            WorkflowStageState {
                id: spec.id,
                label: spec.label,
                status,
                recommended: false,
                run_count,
                latest_revision,
                stale_revisions,
            }
        })
        .collect::<Vec<_>>();

    if let Some(index) = states
        .iter()
        .position(|state| state.status == WorkflowStageStatus::Stale)
        .or_else(|| {
            states
                .iter()
                .position(|state| state.status == WorkflowStageStatus::Ready)
        })
        && let Some(state) = states.get_mut(index)
    {
        state.recommended = true;
    }

    states
}

pub(crate) fn stale_descendant_product_indices(
    specs: &[WorkflowStageSpec],
    products: &[WorkflowProductSnapshot],
    changed_stage_id: &'static str,
    changed_revision: usize,
) -> Vec<usize> {
    let mut descendants = BTreeSet::new();
    let mut frontier = vec![changed_stage_id];
    while let Some(stage_id) = frontier.pop() {
        for spec in specs {
            if spec.depends_on.contains(&stage_id) && descendants.insert(spec.id) {
                frontier.push(spec.id);
            }
        }
    }

    products
        .iter()
        .enumerate()
        .filter_map(|(index, product)| {
            if product.status != WorkflowProductStatus::Active {
                return None;
            }
            if !descendants.contains(product.stage_id) {
                return None;
            }
            let dependency_revision = product
                .dependency_revisions
                .get(changed_stage_id)
                .copied()
                .unwrap_or(0);
            (dependency_revision < changed_revision).then_some(index)
        })
        .collect()
}

fn latest_active_revisions(products: &[WorkflowProductSnapshot]) -> BTreeMap<&'static str, usize> {
    let mut revisions = BTreeMap::new();
    for product in products
        .iter()
        .filter(|product| product.status == WorkflowProductStatus::Active)
    {
        revisions
            .entry(product.stage_id)
            .and_modify(|current: &mut usize| *current = (*current).max(product.revision))
            .or_insert(product.revision);
    }
    revisions
}

fn latest_run_sequence(runs: &[WorkflowRunSnapshot]) -> BTreeMap<&'static str, usize> {
    let mut sequences = BTreeMap::new();
    for run in runs {
        sequences
            .entry(run.stage_id)
            .and_modify(|current: &mut usize| *current = (*current).max(run.sequence))
            .or_insert(run.sequence);
    }
    sequences
}

fn latest_product_for_stage<'a>(
    products: &'a [WorkflowProductSnapshot],
    stage_id: &'static str,
) -> Option<&'a WorkflowProductSnapshot> {
    products
        .iter()
        .filter(|product| product.stage_id == stage_id)
        .max_by_key(|product| product.revision)
}

fn dependencies_satisfied(
    dependencies: &[&'static str],
    latest_active_revision: &BTreeMap<&'static str, usize>,
    latest_run: &BTreeMap<&'static str, usize>,
) -> bool {
    dependencies.iter().all(|dependency| {
        latest_active_revision.contains_key(dependency) || latest_run.contains_key(dependency)
    })
}

#[cfg(test)]
mod tests {
    use casa_calibration::CalibrationPlotPreset;

    use super::{
        WORKFLOW_VALUE_LABEL_WIDTH, WorkflowArtifactDisplay, WorkflowArtifactGroupDisplay,
        WorkflowCalibrationArtifactKind, WorkflowDetailDisplay, WorkflowDiagnosticSummaryDisplay,
        WorkflowOverviewDisplay, WorkflowProductRowDisplay, WorkflowProductSnapshot,
        WorkflowProductStatus, WorkflowRunSnapshot, WorkflowStageDisplay, WorkflowStageSpec,
        WorkflowStageStatus, WorkflowValueDisplay, derive_stage_states,
        preferred_workflow_calibration_preset, render_workflow_artifact_groups,
        render_workflow_detail_display, render_workflow_diagnostic_summary,
        render_workflow_overview_lines, render_workflow_product_row_display,
        render_workflow_stage_display, render_workflow_value_display,
        stale_descendant_product_indices, workflow_calibration_catalog_entries,
    };
    use std::collections::BTreeMap;

    #[test]
    fn render_workflow_value_display_aligns_labels() {
        let rendered = render_workflow_value_display(&WorkflowValueDisplay {
            label: "Refant".to_string(),
            value: "VA15".to_string(),
        });
        assert_eq!(
            rendered,
            format!(
                "{:<width$} {}",
                "Refant",
                "VA15",
                width = WORKFLOW_VALUE_LABEL_WIDTH
            )
        );
    }

    #[test]
    fn render_workflow_artifact_groups_formats_grouped_products() {
        let lines = render_workflow_artifact_groups(
            &[WorkflowArtifactGroupDisplay {
                title: "Derived products".to_string(),
                items: vec![WorkflowArtifactDisplay {
                    heading: "r2 phase.gcal [G Jones | active]".to_string(),
                    detail_lines: vec![
                        "stage=Solve Gain  provenance=Solve Gain  run=2".to_string(),
                        "dependencies=inspect_dataset@r1".to_string(),
                    ],
                }],
            }],
            "No workflow products yet.",
        );

        assert_eq!(lines[0], "Products");
        assert!(lines.iter().any(|line| line == "Derived products"));
        assert!(
            lines
                .iter()
                .any(|line| line.contains("r2 phase.gcal [G Jones | active]"))
        );
        assert!(
            lines
                .iter()
                .any(|line| line.contains("stage=Solve Gain  provenance=Solve Gain  run=2"))
        );
    }

    #[test]
    fn render_workflow_stage_display_formats_status_metadata() {
        let rendered = render_workflow_stage_display(&WorkflowStageDisplay {
            label: "Solve Gain".to_string(),
            status: WorkflowStageStatus::Stale,
            latest_revision: Some(2),
            stale_revisions: 1,
            run_count: 3,
            recommended: true,
            selected: true,
        });

        assert!(rendered.contains("Stage"));
        assert!(rendered.contains("Solve Gain"));
        assert!(rendered.contains("stale"));
        assert!(rendered.contains("r2"));
        assert!(rendered.contains("stale:1"));
        assert!(rendered.contains("runs:3"));
        assert!(rendered.contains("next"));
        assert!(rendered.contains("selected"));
    }

    #[test]
    fn render_workflow_detail_display_aligns_indented_key_values() {
        let rendered = render_workflow_detail_display(&WorkflowDetailDisplay {
            label: "interp".to_string(),
            value: "nearest".to_string(),
            indent: 2,
        });

        assert_eq!(rendered, "  interp           nearest");
    }

    #[test]
    fn render_workflow_product_row_display_formats_revision_status_rows() {
        let rendered = render_workflow_product_row_display(&WorkflowProductRowDisplay {
            family: "G Jones".to_string(),
            revision: 2,
            display_name: "phase.gcal".to_string(),
            stage_label: "Solve Gain".to_string(),
            status_label: "active".to_string(),
        });

        assert!(rendered.contains("G Jones"));
        assert!(rendered.contains("r2 phase.gcal"));
        assert!(rendered.contains("[Solve Gain | active]"));
    }

    #[test]
    fn render_workflow_overview_lines_formats_session_summary() {
        let lines = render_workflow_overview_lines(&WorkflowOverviewDisplay {
            dataset_path: Some("/tmp/example.ms".to_string()),
            recommended_stage: Some("Solve Gain".to_string()),
            selected_stage: "Apply".to_string(),
            active_products: 2,
            stale_products: 1,
            total_products: 4,
            guidance: "Use Context, Products, Stages, and Diagnostics.".to_string(),
            latest_run_lines: vec![
                "Gain Solve".to_string(),
                "Output: /tmp/phase.gcal".to_string(),
            ],
        });

        assert_eq!(lines[0], "Workflow Session");
        assert!(lines.iter().any(|line| line == "Dataset: /tmp/example.ms"));
        assert!(
            lines
                .iter()
                .any(|line| line == "Recommended next stage: Solve Gain")
        );
        assert!(lines.iter().any(|line| line == "Selected stage: Apply"));
        assert!(
            lines
                .iter()
                .any(|line| line == "Products: active=2 stale=1 total=4")
        );
        assert!(lines.iter().any(|line| line == "Latest run"));
        assert!(lines.iter().any(|line| line == "  Gain Solve"));
    }

    #[test]
    fn render_workflow_diagnostic_summary_prefers_source_path() {
        let rendered = render_workflow_diagnostic_summary(&WorkflowDiagnosticSummaryDisplay {
            description: "Inspect: Gain Phase vs Time".to_string(),
            source_path: Some("/tmp/phase.gcal".to_string()),
            missing_source_message: Some("Choose a calibration table.".to_string()),
        });

        assert_eq!(
            rendered,
            "Inspect: Gain Phase vs Time Source: /tmp/phase.gcal"
        );
    }

    #[test]
    fn workflow_calibration_catalog_entries_marks_selected_preset() {
        let rows =
            workflow_calibration_catalog_entries(Some(CalibrationPlotPreset::GainPhaseVsTime));
        let selected = rows
            .into_iter()
            .find(|row| row.target == CalibrationPlotPreset::GainPhaseVsTime)
            .expect("selected row");
        assert!(selected.selected);
        assert_eq!(selected.label, "Inspect: Gain Phase vs Time");
    }

    #[test]
    fn preferred_workflow_calibration_preset_matches_artifact_kind() {
        assert_eq!(
            preferred_workflow_calibration_preset(WorkflowCalibrationArtifactKind::GainLike),
            CalibrationPlotPreset::GainPhaseVsTime
        );
        assert_eq!(
            preferred_workflow_calibration_preset(WorkflowCalibrationArtifactKind::BandpassLike),
            CalibrationPlotPreset::BandpassAmplitudeVsFrequency
        );
        assert_eq!(
            preferred_workflow_calibration_preset(WorkflowCalibrationArtifactKind::CorrectedData),
            CalibrationPlotPreset::CorrectedAmplitudeVsTime
        );
    }

    #[test]
    fn linear_workflow_recommends_first_ready_stage() {
        let specs = [
            WorkflowStageSpec {
                id: "inspect",
                label: "Inspect Dataset",
                depends_on: &[],
                produces_product: false,
            },
            WorkflowStageSpec {
                id: "solve_gain",
                label: "Solve Gain",
                depends_on: &["inspect"],
                produces_product: true,
            },
            WorkflowStageSpec {
                id: "solve_bandpass",
                label: "Solve Bandpass",
                depends_on: &["solve_gain"],
                produces_product: true,
            },
            WorkflowStageSpec {
                id: "apply",
                label: "Apply",
                depends_on: &["solve_gain"],
                produces_product: true,
            },
        ];
        let runs = [WorkflowRunSnapshot {
            stage_id: "inspect",
            sequence: 1,
        }];
        let products = [WorkflowProductSnapshot {
            stage_id: "solve_gain",
            revision: 1,
            status: WorkflowProductStatus::Active,
            dependency_revisions: BTreeMap::from([("inspect", 1)]),
        }];

        let states = derive_stage_states(&specs, &runs, &products);
        assert_eq!(states[0].status, WorkflowStageStatus::Completed);
        assert_eq!(states[1].status, WorkflowStageStatus::Completed);
        assert_eq!(states[2].status, WorkflowStageStatus::Ready);
        assert!(states[2].recommended);
        assert_eq!(states[3].status, WorkflowStageStatus::Ready);
        assert!(!states[3].recommended);
    }

    #[test]
    fn rerun_upstream_stage_marks_downstream_products_stale() {
        let specs = [
            WorkflowStageSpec {
                id: "solve_gain",
                label: "Solve Gain",
                depends_on: &[],
                produces_product: true,
            },
            WorkflowStageSpec {
                id: "apply",
                label: "Apply",
                depends_on: &["solve_gain"],
                produces_product: true,
            },
        ];
        let products = [
            WorkflowProductSnapshot {
                stage_id: "solve_gain",
                revision: 2,
                status: WorkflowProductStatus::Active,
                dependency_revisions: BTreeMap::new(),
            },
            WorkflowProductSnapshot {
                stage_id: "apply",
                revision: 1,
                status: WorkflowProductStatus::Active,
                dependency_revisions: BTreeMap::from([("solve_gain", 1)]),
            },
        ];

        assert_eq!(
            stale_descendant_product_indices(&specs, &products, "solve_gain", 2),
            vec![1]
        );
    }

    #[test]
    fn self_cal_iteration_graph_supports_revisitable_loops() {
        let specs = [
            WorkflowStageSpec {
                id: "image",
                label: "Image",
                depends_on: &[],
                produces_product: true,
            },
            WorkflowStageSpec {
                id: "solve_selfcal",
                label: "Solve Self-Cal",
                depends_on: &["image"],
                produces_product: true,
            },
            WorkflowStageSpec {
                id: "apply",
                label: "Apply Self-Cal",
                depends_on: &["solve_selfcal"],
                produces_product: true,
            },
            WorkflowStageSpec {
                id: "reimage",
                label: "Reimage",
                depends_on: &["apply"],
                produces_product: true,
            },
        ];
        let products = [
            WorkflowProductSnapshot {
                stage_id: "image",
                revision: 2,
                status: WorkflowProductStatus::Active,
                dependency_revisions: BTreeMap::new(),
            },
            WorkflowProductSnapshot {
                stage_id: "solve_selfcal",
                revision: 2,
                status: WorkflowProductStatus::Active,
                dependency_revisions: BTreeMap::from([("image", 2)]),
            },
            WorkflowProductSnapshot {
                stage_id: "apply",
                revision: 1,
                status: WorkflowProductStatus::Active,
                dependency_revisions: BTreeMap::from([("solve_selfcal", 1)]),
            },
            WorkflowProductSnapshot {
                stage_id: "reimage",
                revision: 1,
                status: WorkflowProductStatus::Active,
                dependency_revisions: BTreeMap::from([("apply", 1)]),
            },
        ];

        assert_eq!(
            stale_descendant_product_indices(&specs, &products, "solve_selfcal", 2),
            vec![2, 3]
        );
    }

    #[test]
    fn imaging_stage_graph_tracks_product_families_as_first_class_artifacts() {
        let specs = [
            WorkflowStageSpec {
                id: "dirty",
                label: "Dirty Image",
                depends_on: &[],
                produces_product: true,
            },
            WorkflowStageSpec {
                id: "deconvolve",
                label: "Deconvolve",
                depends_on: &["dirty"],
                produces_product: true,
            },
            WorkflowStageSpec {
                id: "inspect",
                label: "Inspect Image",
                depends_on: &["deconvolve"],
                produces_product: false,
            },
        ];
        let products = [WorkflowProductSnapshot {
            stage_id: "dirty",
            revision: 1,
            status: WorkflowProductStatus::Active,
            dependency_revisions: BTreeMap::new(),
        }];

        let states = derive_stage_states(&specs, &[], &products);
        assert_eq!(states[0].status, WorkflowStageStatus::Completed);
        assert_eq!(states[1].status, WorkflowStageStatus::Ready);
        assert!(states[1].recommended);
        assert_eq!(states[2].status, WorkflowStageStatus::Blocked);
    }

    #[test]
    fn vlbi_style_prior_calibration_chain_fits_stage_dependencies() {
        let specs = [
            WorkflowStageSpec {
                id: "import_prior",
                label: "Import Prior Cal",
                depends_on: &[],
                produces_product: true,
            },
            WorkflowStageSpec {
                id: "fringefit",
                label: "Fringe Fit",
                depends_on: &["import_prior"],
                produces_product: true,
            },
            WorkflowStageSpec {
                id: "apply",
                label: "Apply",
                depends_on: &["fringefit"],
                produces_product: true,
            },
        ];
        let products = [
            WorkflowProductSnapshot {
                stage_id: "import_prior",
                revision: 1,
                status: WorkflowProductStatus::Active,
                dependency_revisions: BTreeMap::new(),
            },
            WorkflowProductSnapshot {
                stage_id: "fringefit",
                revision: 1,
                status: WorkflowProductStatus::Active,
                dependency_revisions: BTreeMap::from([("import_prior", 1)]),
            },
        ];

        let states = derive_stage_states(&specs, &[], &products);
        assert_eq!(states[2].status, WorkflowStageStatus::Ready);
        assert!(states[2].recommended);
    }
}
