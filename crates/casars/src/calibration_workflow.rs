// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration-specific adapter types and helpers for the generic `WorkflowShell`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_calibration::{
    ApplyCalibrationTableSpec, ApplyInterpolationMode, CalibrationPlotPreset, GainFieldSelector,
    ManagedCalibrationOutput, load_apply_specs_from_callib,
};

use crate::workflow::{
    WorkflowArtifactDisplay, WorkflowArtifactGroupDisplay, WorkflowCatalogEntryDisplay,
    WorkflowProductSnapshot, WorkflowRunSnapshot, WorkflowStageSpec, WorkflowStageState,
    WorkflowStageStatus, derive_stage_states,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum WorkflowStageId {
    InspectDataset,
    SolveGain,
    SolveBandpass,
    FluxScale,
    Apply,
    InspectResults,
}

impl WorkflowStageId {
    pub(crate) const ALL: [Self; 6] = [
        Self::InspectDataset,
        Self::SolveGain,
        Self::SolveBandpass,
        Self::FluxScale,
        Self::Apply,
        Self::InspectResults,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::InspectDataset => "Inspect Dataset",
            Self::SolveGain => "Solve Gain",
            Self::SolveBandpass => "Solve Bandpass",
            Self::FluxScale => "Fluxscale",
            Self::Apply => "Apply",
            Self::InspectResults => "Inspect Results",
        }
    }

    pub(crate) fn cli_mode(self) -> &'static str {
        match self {
            Self::InspectDataset => "summary",
            Self::SolveGain => "solve_gain",
            Self::SolveBandpass => "solve_bandpass",
            Self::FluxScale => "fluxscale",
            Self::Apply => "apply",
            Self::InspectResults => "stats",
        }
    }

    pub(crate) fn from_mode(mode: &str) -> Self {
        match mode {
            "summary" => Self::InspectDataset,
            "solve_gain" => Self::SolveGain,
            "solve_bandpass" => Self::SolveBandpass,
            "fluxscale" => Self::FluxScale,
            "stats" => Self::InspectResults,
            "apply" => Self::Apply,
            _ => Self::Apply,
        }
    }

    pub(crate) fn from_key(key: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|stage| stage.key() == key)
    }

    pub(crate) fn key(self) -> &'static str {
        match self {
            Self::InspectDataset => "inspect_dataset",
            Self::SolveGain => "solve_gain",
            Self::SolveBandpass => "solve_bandpass",
            Self::FluxScale => "fluxscale",
            Self::Apply => "apply",
            Self::InspectResults => "inspect_results",
        }
    }

    pub(crate) fn cycle(self, forward: bool) -> Self {
        let position = Self::ALL
            .iter()
            .position(|candidate| *candidate == self)
            .unwrap_or(0);
        if forward {
            Self::ALL[(position + 1) % Self::ALL.len()]
        } else if position == 0 {
            Self::ALL[Self::ALL.len() - 1]
        } else {
            Self::ALL[position - 1]
        }
    }

    fn depends_on_keys(self) -> &'static [&'static str] {
        match self {
            Self::InspectDataset => &[],
            Self::SolveGain => &["inspect_dataset"],
            Self::SolveBandpass => &["solve_gain"],
            Self::FluxScale => &["solve_gain"],
            Self::Apply => &["solve_gain"],
            Self::InspectResults => &["apply"],
        }
    }

    fn produces_product(self) -> bool {
        !matches!(self, Self::InspectDataset | Self::InspectResults)
    }

    pub(crate) fn spec(self) -> WorkflowStageSpec {
        WorkflowStageSpec {
            id: self.key(),
            label: self.label(),
            depends_on: self.depends_on_keys(),
            produces_product: self.produces_product(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum WorkflowContextSettingKind {
    ActiveFields,
    RefAnt,
    FluxReferenceFields,
    FluxTransferFields,
}

impl WorkflowContextSettingKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::ActiveFields => "Selected Fields",
            Self::RefAnt => "Refant",
            Self::FluxReferenceFields => "Flux Reference",
            Self::FluxTransferFields => "Flux Transfer",
        }
    }

    pub(crate) fn field_id(self) -> &'static str {
        match self {
            Self::ActiveFields => "field",
            Self::RefAnt => "refant",
            Self::FluxReferenceFields => "reference_fields",
            Self::FluxTransferFields => "transfer_fields",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum WorkflowStageGuideKind {
    Goal,
    Produces,
    Hint,
}

impl WorkflowStageGuideKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Goal => "Goal",
            Self::Produces => "Produces",
            Self::Hint => "Hint",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum WorkflowProductActionKind {
    AddSolvedProduct,
    ImportChainTable,
    ChooseCallibrary,
}

impl WorkflowProductActionKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::AddSolvedProduct => "+ Add solved product to chain",
            Self::ImportChainTable => "+ Import calibration table into chain",
            Self::ChooseCallibrary => "+ Choose callibrary file",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum WorkflowChainSettingKind {
    Gainfield,
    Interp,
    Spwmap,
    Calwt,
}

impl WorkflowChainSettingKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Gainfield => "gainfield",
            Self::Interp => "interp",
            Self::Spwmap => "spwmap",
            Self::Calwt => "calwt",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct WorkflowProductRecord {
    pub(crate) path: PathBuf,
    pub(crate) stage: WorkflowStageId,
    pub(crate) family: String,
    pub(crate) revision: usize,
    pub(crate) provenance: String,
    pub(crate) status: crate::workflow::WorkflowProductStatus,
    pub(crate) dependency_revisions: BTreeMap<&'static str, usize>,
    pub(crate) run_sequence: usize,
}

#[derive(Debug, Clone)]
pub(crate) enum WorkflowChainEntrySource {
    DirectTable,
    CallibraryFile {
        path: PathBuf,
    },
    CallibrarySpec {
        callib_path: PathBuf,
        spec_index: usize,
        spec: ApplyCalibrationTableSpec,
    },
    CallibraryError {
        path: PathBuf,
        message: String,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct WorkflowChainEntryRecord {
    pub(crate) label: String,
    pub(crate) inspect_path: Option<PathBuf>,
    pub(crate) source: WorkflowChainEntrySource,
}

#[derive(Debug, Clone)]
pub(crate) struct WorkflowChainSettingRecord {
    pub(crate) entry: usize,
    pub(crate) kind: WorkflowChainSettingKind,
    pub(crate) text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkflowCalibrationArtifactKind {
    GainLike,
    BandpassLike,
    CorrectedData,
}

pub(crate) fn calibration_stage_specs() -> Vec<WorkflowStageSpec> {
    WorkflowStageId::ALL
        .into_iter()
        .map(WorkflowStageId::spec)
        .collect()
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

pub(crate) fn workflow_preferred_diagnostic_preset_for_stage(
    stage: WorkflowStageId,
) -> Option<CalibrationPlotPreset> {
    let kind = match stage {
        WorkflowStageId::SolveGain | WorkflowStageId::FluxScale => {
            WorkflowCalibrationArtifactKind::GainLike
        }
        WorkflowStageId::SolveBandpass => WorkflowCalibrationArtifactKind::BandpassLike,
        WorkflowStageId::Apply => WorkflowCalibrationArtifactKind::CorrectedData,
        WorkflowStageId::InspectDataset | WorkflowStageId::InspectResults => return None,
    };
    Some(preferred_workflow_calibration_preset(kind))
}

pub(crate) fn workflow_stage_from_report(
    report: &ManagedCalibrationOutput,
) -> Option<WorkflowStageId> {
    match report {
        ManagedCalibrationOutput::Apply(_) => Some(WorkflowStageId::Apply),
        ManagedCalibrationOutput::Summary(_) => Some(WorkflowStageId::InspectDataset),
        ManagedCalibrationOutput::Stats(_) => Some(WorkflowStageId::InspectResults),
        ManagedCalibrationOutput::SolveGain(_) => Some(WorkflowStageId::SolveGain),
        ManagedCalibrationOutput::SolveBandpass(_) => Some(WorkflowStageId::SolveBandpass),
        ManagedCalibrationOutput::FluxScale(_) => Some(WorkflowStageId::FluxScale),
        ManagedCalibrationOutput::PlanApply(_) => None,
    }
}

pub(crate) fn workflow_product_metadata_from_report(
    report: &ManagedCalibrationOutput,
) -> Option<(PathBuf, WorkflowStageId, String, String)> {
    match report {
        ManagedCalibrationOutput::SolveGain(report) => Some((
            report.output_table.clone(),
            WorkflowStageId::SolveGain,
            format!("{:?}", report.gain_type),
            "solved gain table".to_string(),
        )),
        ManagedCalibrationOutput::SolveBandpass(report) => Some((
            report.output_table.clone(),
            WorkflowStageId::SolveBandpass,
            report.table_subtype.clone(),
            "solved bandpass table".to_string(),
        )),
        ManagedCalibrationOutput::FluxScale(report) => Some((
            report.output_table.clone(),
            WorkflowStageId::FluxScale,
            "Fluxscale".to_string(),
            "fluxscale output".to_string(),
        )),
        ManagedCalibrationOutput::Apply(report) => {
            report.plan.measurement_set_path.clone().map(|path| {
                (
                    path,
                    WorkflowStageId::Apply,
                    "CorrectedData".to_string(),
                    "updated measurement set".to_string(),
                )
            })
        }
        ManagedCalibrationOutput::Summary(_)
        | ManagedCalibrationOutput::PlanApply(_)
        | ManagedCalibrationOutput::Stats(_) => None,
    }
}

pub(crate) fn workflow_stage_states(
    runs: &[WorkflowRunSnapshot],
    products: &[WorkflowProductSnapshot],
    has_measurement_set: bool,
) -> Vec<WorkflowStageState> {
    let mut states = derive_stage_states(&calibration_stage_specs(), runs, products);
    if has_measurement_set {
        if let Some(inspect_stage) = states
            .iter_mut()
            .find(|state| state.id == WorkflowStageId::InspectDataset.key())
        {
            inspect_stage.status = WorkflowStageStatus::Completed;
            inspect_stage.recommended = false;
        }
        if !states.iter().any(|state| state.recommended)
            && let Some(next_index) = states.iter().position(|state| {
                matches!(
                    state.status,
                    WorkflowStageStatus::Stale | WorkflowStageStatus::Ready
                )
            })
            && let Some(state) = states.get_mut(next_index)
        {
            state.recommended = true;
        }
    }
    states
}

pub(crate) fn workflow_products_display_groups(
    gaintables: &[String],
    callib: Option<&str>,
    workflow_products: &[WorkflowProductRecord],
) -> Vec<WorkflowArtifactGroupDisplay> {
    let mut configured = gaintables
        .iter()
        .map(|path| WorkflowArtifactDisplay {
            heading: format!("Chain: {path}"),
            detail_lines: Vec::new(),
        })
        .collect::<Vec<_>>();
    if let Some(callib) = callib {
        configured.push(WorkflowArtifactDisplay {
            heading: format!("Callibrary: {callib}"),
            detail_lines: Vec::new(),
        });
    }

    let derived = workflow_products
        .iter()
        .map(|product| {
            let dependencies = if product.dependency_revisions.is_empty() {
                "none".to_string()
            } else {
                product
                    .dependency_revisions
                    .iter()
                    .map(|(stage, revision)| format!("{stage}@r{revision}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            WorkflowArtifactDisplay {
                heading: format!(
                    "r{} {} [{} | {}]",
                    product.revision,
                    product.path.display(),
                    product.family,
                    product.status.label()
                ),
                detail_lines: vec![
                    format!(
                        "stage={}  provenance={}  run={}",
                        product.stage.label(),
                        product.provenance,
                        product.run_sequence
                    ),
                    format!("depends_on={dependencies}"),
                ],
            }
        })
        .collect::<Vec<_>>();

    vec![
        WorkflowArtifactGroupDisplay {
            title: "Configured chain".to_string(),
            items: configured,
        },
        WorkflowArtifactGroupDisplay {
            title: "Derived products".to_string(),
            items: derived,
        },
    ]
}

pub(crate) fn workflow_chain_entries(
    gaintables: &[String],
    callib: Option<&str>,
) -> Vec<WorkflowChainEntryRecord> {
    let mut entries = gaintables
        .iter()
        .enumerate()
        .map(|(index, path)| WorkflowChainEntryRecord {
            label: format!("Chain {:<12} {}", index + 1, path),
            inspect_path: Some(PathBuf::from(path)),
            source: WorkflowChainEntrySource::DirectTable,
        })
        .collect::<Vec<_>>();
    if let Some(callib) = callib {
        let callib_path = PathBuf::from(callib);
        entries.push(WorkflowChainEntryRecord {
            label: format!("Callibrary      {callib}"),
            inspect_path: None,
            source: WorkflowChainEntrySource::CallibraryFile {
                path: callib_path.clone(),
            },
        });
        match load_apply_specs_from_callib(&callib_path) {
            Ok(specs) => {
                entries.extend(specs.into_iter().enumerate().map(|(index, spec)| {
                    let table_path = spec.path.clone();
                    WorkflowChainEntryRecord {
                        label: format!("Callib {:<13} {}", index + 1, table_path.display()),
                        inspect_path: Some(table_path),
                        source: WorkflowChainEntrySource::CallibrarySpec {
                            callib_path: callib_path.clone(),
                            spec_index: index,
                            spec,
                        },
                    }
                }));
            }
            Err(error) => {
                entries.push(WorkflowChainEntryRecord {
                    label: "Callib parse error".to_string(),
                    inspect_path: None,
                    source: WorkflowChainEntrySource::CallibraryError {
                        path: callib_path,
                        message: error.to_string(),
                    },
                });
            }
        }
    }
    entries
}

pub(crate) fn workflow_stage_goal(stage: WorkflowStageId) -> &'static str {
    match stage {
        WorkflowStageId::InspectDataset => {
            "Summarize the MeasurementSet and verify the calibrator/target selections."
        }
        WorkflowStageId::SolveGain => {
            "Solve per-antenna gain corrections on the selected calibrator data."
        }
        WorkflowStageId::SolveBandpass => {
            "Solve frequency-dependent bandpass corrections using prior gain calibration."
        }
        WorkflowStageId::FluxScale => {
            "Transfer the absolute flux density scale from the reference calibrator."
        }
        WorkflowStageId::Apply => {
            "Apply the configured calibration chain to the selected MeasurementSet rows."
        }
        WorkflowStageId::InspectResults => {
            "Inspect solved tables or computed calibration statistics before the next step."
        }
    }
}

pub(crate) fn workflow_stage_output(stage: WorkflowStageId) -> &'static str {
    match stage {
        WorkflowStageId::InspectDataset => "MeasurementSet summary and selection context",
        WorkflowStageId::SolveGain => "Gain calibration table (G/T Jones)",
        WorkflowStageId::SolveBandpass => "Bandpass calibration table (B Jones)",
        WorkflowStageId::FluxScale => "Flux-scaled gain table",
        WorkflowStageId::Apply => "Updated CORRECTED_DATA and apply report",
        WorkflowStageId::InspectResults => "Calibration stats report and inspection plots",
    }
}

pub(crate) fn workflow_stage_hint(stage: WorkflowStageId) -> &'static str {
    match stage {
        WorkflowStageId::InspectDataset => {
            "Start here: choose the dataset and confirm fields, SPWs, and refant."
        }
        WorkflowStageId::SolveGain => {
            "Typical first solve: primary calibrator field, spw 0, refant set, solint inf or int."
        }
        WorkflowStageId::SolveBandpass => {
            "Usually run after gain solve and include the gain table in the chain or callibrary."
        }
        WorkflowStageId::FluxScale => {
            "Use after gain/bandpass when you want to transfer absolute flux from the flux calibrator."
        }
        WorkflowStageId::Apply => {
            "Make sure the Products chain is correct, then run apply and inspect corrected-data diagnostics."
        }
        WorkflowStageId::InspectResults => {
            "Point Table Path at the table you want to inspect; use Diagnostics for the recommended plots."
        }
    }
}

pub(crate) fn workflow_stage_action_label(stage: WorkflowStageId) -> &'static str {
    match stage {
        WorkflowStageId::InspectDataset => "Run dataset summary now [Enter/r]",
        WorkflowStageId::SolveGain => "Run gain solve now [Enter/r]",
        WorkflowStageId::SolveBandpass => "Run bandpass solve now [Enter/r]",
        WorkflowStageId::FluxScale => "Run fluxscale now [Enter/r]",
        WorkflowStageId::Apply => "Run apply now [Enter/r]",
        WorkflowStageId::InspectResults => "Run stats now [Enter/r]",
    }
}

pub(crate) fn suggested_output_table_path(
    stage: WorkflowStageId,
    base_path: &Path,
) -> Option<String> {
    let parent = base_path.parent().unwrap_or_else(|| Path::new("."));
    let name = base_path.file_name()?.to_string_lossy();
    let stem = if let Some(stem) = name.strip_suffix(".ms") {
        stem.to_string()
    } else if let Some(stem) = base_path.file_stem() {
        stem.to_string_lossy().into_owned()
    } else {
        name.into_owned()
    };
    let suffix = match stage {
        WorkflowStageId::SolveGain => "gain.gcal",
        WorkflowStageId::SolveBandpass => "bandpass.bcal",
        WorkflowStageId::FluxScale => "flux.gcal",
        WorkflowStageId::InspectDataset
        | WorkflowStageId::Apply
        | WorkflowStageId::InspectResults => return None,
    };
    Some(
        parent
            .join(format!("{stem}.{suffix}"))
            .display()
            .to_string(),
    )
}

pub(crate) fn workflow_callib_setting_display_value(
    spec: &ApplyCalibrationTableSpec,
    kind: WorkflowChainSettingKind,
) -> String {
    match kind {
        WorkflowChainSettingKind::Gainfield => format_gainfield_selector(spec.gainfield.as_ref()),
        WorkflowChainSettingKind::Interp => format_apply_interp(spec.interp).to_string(),
        WorkflowChainSettingKind::Spwmap => format_spwmap_value(&spec.spwmap),
        WorkflowChainSettingKind::Calwt => yes_no(spec.calwt).to_string(),
    }
}

pub(crate) fn workflow_callib_setting_raw_value(
    spec: &ApplyCalibrationTableSpec,
    kind: WorkflowChainSettingKind,
) -> String {
    match kind {
        WorkflowChainSettingKind::Gainfield => match spec.gainfield.as_ref() {
            None => String::new(),
            Some(GainFieldSelector::Nearest) => "nearest".to_string(),
            Some(GainFieldSelector::FieldId(field_id)) => field_id.to_string(),
            Some(GainFieldSelector::FieldName(name)) => name.clone(),
        },
        WorkflowChainSettingKind::Interp => format_apply_interp(spec.interp).to_string(),
        WorkflowChainSettingKind::Spwmap => {
            if spec.spwmap.is_empty() {
                String::new()
            } else {
                spec.spwmap
                    .iter()
                    .map(i32::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            }
        }
        WorkflowChainSettingKind::Calwt => spec.calwt.to_string(),
    }
}

pub(crate) fn workflow_callib_apply_to_row(spec: &ApplyCalibrationTableSpec) -> Option<String> {
    (!spec.apply_to.is_empty())
        .then(|| format!("  apply_to         {}", format_apply_table_selection(spec)))
}

pub(crate) fn parse_workflow_gainfield_value(
    value: &str,
) -> Result<Option<GainFieldSelector>, String> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    if value.eq_ignore_ascii_case("nearest") {
        return Ok(Some(GainFieldSelector::Nearest));
    }
    if let Ok(field_id) = value.parse::<i32>() {
        return Ok(Some(GainFieldSelector::FieldId(field_id)));
    }
    Ok(Some(GainFieldSelector::FieldName(value.to_string())))
}

pub(crate) fn parse_workflow_interp_value(value: &str) -> Result<ApplyInterpolationMode, String> {
    match value.trim() {
        "" | "nearest" => Ok(ApplyInterpolationMode::Nearest),
        "linear" => Ok(ApplyInterpolationMode::Linear),
        "nearest,linear" => Ok(ApplyInterpolationMode::NearestLinear),
        other => Err(format!("unsupported apply interpolation {other:?}")),
    }
}

pub(crate) fn parse_workflow_spwmap_value(value: &str) -> Result<Vec<i32>, String> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| {
            item.parse::<i32>()
                .map_err(|error| format!("invalid spwmap value {item:?}: {error}"))
        })
        .collect()
}

pub(crate) fn parse_workflow_calwt_value(value: &str) -> Result<bool, String> {
    match value.trim() {
        "true" => Ok(true),
        "false" | "" => Ok(false),
        other => Err(format!("unsupported calwt value {other:?}")),
    }
}

fn format_gainfield_selector(selector: Option<&GainFieldSelector>) -> String {
    match selector {
        None => "<default>".to_string(),
        Some(GainFieldSelector::Nearest) => "nearest".to_string(),
        Some(GainFieldSelector::FieldId(field_id)) => field_id.to_string(),
        Some(GainFieldSelector::FieldName(name)) => name.clone(),
    }
}

fn format_apply_interp(interp: ApplyInterpolationMode) -> &'static str {
    match interp {
        ApplyInterpolationMode::Nearest => "nearest",
        ApplyInterpolationMode::Linear => "linear",
        ApplyInterpolationMode::NearestLinear => "nearest,linear",
    }
}

fn format_spwmap_value(spwmap: &[i32]) -> String {
    if spwmap.is_empty() {
        "<identity>".to_string()
    } else {
        spwmap
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn format_apply_table_selection(spec: &ApplyCalibrationTableSpec) -> String {
    let mut parts = Vec::new();
    if !spec.apply_to.field_ids.is_empty() {
        parts.push(format!(
            "field={}",
            spec.apply_to
                .field_ids
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if !spec.apply_to.spectral_window_ids.is_empty() {
        parts.push(format!(
            "spw={}",
            spec.apply_to
                .spectral_window_ids
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if !spec.apply_to.observation_ids.is_empty() {
        parts.push(format!(
            "obs={}",
            spec.apply_to
                .observation_ids
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    parts.join("  ")
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

#[cfg(test)]
mod tests {
    use casa_calibration::{
        ApplyCalibrationTableSpec, ApplyExecutionReport, ApplyExecutionTimings, ApplyMode,
        ApplyPlan, CalibrationPlotPreset, GainFieldSelector, GainSolveReport,
    };
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use super::{
        WorkflowCalibrationArtifactKind, WorkflowChainSettingKind, WorkflowContextSettingKind,
        WorkflowProductActionKind, WorkflowProductRecord, WorkflowStageGuideKind,
        WorkflowStageId,
        parse_workflow_gainfield_value, preferred_workflow_calibration_preset,
        workflow_calibration_catalog_entries, workflow_callib_setting_display_value,
        workflow_preferred_diagnostic_preset_for_stage, workflow_product_metadata_from_report,
        workflow_products_display_groups, workflow_stage_from_report, workflow_stage_goal,
        workflow_stage_states,
    };
    use crate::workflow::{WorkflowProductStatus, WorkflowStageStatus};
    use casa_calibration::ManagedCalibrationOutput;

    #[test]
    fn workflow_context_setting_labels_match_public_ui() {
        assert_eq!(
            WorkflowContextSettingKind::ActiveFields.label(),
            "Selected Fields"
        );
        assert_eq!(WorkflowContextSettingKind::RefAnt.field_id(), "refant");
        assert_eq!(WorkflowStageGuideKind::Goal.label(), "Goal");
        assert_eq!(
            WorkflowProductActionKind::ChooseCallibrary.label(),
            "+ Choose callibrary file"
        );
        assert_eq!(WorkflowChainSettingKind::Interp.label(), "interp");
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
    fn workflow_stage_helpers_are_calibration_specific() {
        assert_eq!(
            workflow_preferred_diagnostic_preset_for_stage(WorkflowStageId::Apply),
            Some(CalibrationPlotPreset::CorrectedAmplitudeVsTime)
        );
        assert!(workflow_stage_goal(WorkflowStageId::SolveGain).contains("per-antenna"));
    }

    #[test]
    fn parse_workflow_gainfield_value_accepts_ids_and_nearest() {
        assert_eq!(
            parse_workflow_gainfield_value("nearest").expect("nearest"),
            Some(GainFieldSelector::Nearest)
        );
        assert_eq!(
            parse_workflow_gainfield_value("4").expect("id"),
            Some(GainFieldSelector::FieldId(4))
        );
    }

    #[test]
    fn workflow_callib_display_value_formats_defaults() {
        let spec = ApplyCalibrationTableSpec::new(PathBuf::from("/tmp/gain.gcal"));
        assert_eq!(
            workflow_callib_setting_display_value(&spec, WorkflowChainSettingKind::Gainfield),
            "<default>"
        );
    }

    #[test]
    fn workflow_stage_states_mark_dataset_stage_complete_once_ms_exists() {
        let states = workflow_stage_states(&[], &[], true);
        let inspect = states
            .iter()
            .find(|state| state.id == WorkflowStageId::InspectDataset.key())
            .expect("inspect stage");
        assert_eq!(inspect.status, WorkflowStageStatus::Completed);
    }

    #[test]
    fn workflow_products_display_groups_collect_chain_and_products() {
        let groups = workflow_products_display_groups(
            &[String::from("/tmp/phase.gcal")],
            Some("/tmp/apply.callib"),
            &[WorkflowProductRecord {
                path: PathBuf::from("/tmp/bandpass.bcal"),
                stage: WorkflowStageId::SolveBandpass,
                family: "B".to_string(),
                revision: 2,
                provenance: "solved".to_string(),
                status: WorkflowProductStatus::Active,
                dependency_revisions: BTreeMap::from([(WorkflowStageId::SolveGain.key(), 1usize)]),
                run_sequence: 3,
            }],
        );
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].items.len(), 2);
        assert_eq!(groups[1].items.len(), 1);
    }

    #[test]
    fn workflow_stage_from_report_and_product_metadata_match_apply_and_gain() {
        let gain = ManagedCalibrationOutput::SolveGain(GainSolveReport {
            output_table: PathBuf::from("/tmp/phase.gcal"),
            gain_type: casa_calibration::GainType::G,
            refant_antenna_id: 3,
            field_ids: vec![0],
            spectral_window_ids: vec![0],
            solution_row_count: 3,
        });
        assert_eq!(
            workflow_stage_from_report(&gain),
            Some(WorkflowStageId::SolveGain)
        );
        let (path, stage, _, _) = workflow_product_metadata_from_report(&gain).expect("product");
        assert_eq!(path, PathBuf::from("/tmp/phase.gcal"));
        assert_eq!(stage, WorkflowStageId::SolveGain);

        let apply = ManagedCalibrationOutput::Apply(ApplyExecutionReport {
            plan: ApplyPlan {
                measurement_set_path: Some(PathBuf::from("/tmp/example.ms")),
                apply_mode: ApplyMode::Trial,
                requires_corrected_data_column: false,
                selected_rows: vec![],
                selected_row_count: 10,
                parang: false,
                selected_field_ids: vec![0],
                selected_data_desc_ids: vec![0],
                selected_data_spw_ids: vec![0],
                measurement_set_spectral_windows: vec![],
                calibration_tables: vec![],
            },
            created_corrected_data_column: false,
            wrote_measurement_set: false,
            updated_row_count: 10,
            flagged_row_count: 0,
            flagged_sample_count: 0,
            timings: ApplyExecutionTimings::default(),
        });
        assert_eq!(
            workflow_stage_from_report(&apply),
            Some(WorkflowStageId::Apply)
        );
        let (path, stage, _, _) =
            workflow_product_metadata_from_report(&apply).expect("apply product");
        assert_eq!(path, PathBuf::from("/tmp/example.ms"));
        assert_eq!(stage, WorkflowStageId::Apply);
    }
}
