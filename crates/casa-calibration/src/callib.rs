// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA task callibrary parsing and apply specifications.
//! File syntax follows CalLibraryGram; data slices and mappings follow CLPatchPanel.

#[path = "callib_parser.rs"]
mod parser;
#[path = "callib_record.rs"]
mod record;
pub use record::{load_callibrary_record, validate_callibrary_record};
pub(crate) use record::{tool_library_specs, tool_table_record};

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::{
    ApplyCalibrationTableSpec, ApplyInterpolationMode, ApplyTableSelection, GainFieldSelector,
};

/// Errors returned while loading a narrow callibrary file.
#[derive(Debug, Error)]
pub enum CallibError {
    /// Reading the callibrary file failed.
    #[error("failed to read callibrary file {path}: {source}")]
    ReadFile {
        /// Path that was being loaded.
        path: String,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// Writing the callibrary file failed.
    #[error("failed to write callibrary file {path}: {source}")]
    WriteFile {
        /// Path that was being written.
        path: String,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// One callibrary line could not be parsed inside the supported surface.
    #[error("failed to parse callibrary line {line_number} in {path}: {reason}: {line}")]
    ParseLine {
        /// Callibrary file path.
        path: String,
        /// One-based line number.
        line_number: usize,
        /// Original source line.
        line: String,
        /// Human-readable reason.
        reason: String,
    },
}

/// Observation or scan mapping used by a callibrary slice.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub enum CallibraryContextMap {
    /// Merge all available calibration contexts in time.
    #[default]
    All,
    /// Use the data context's own calibration ID.
    SelfMap,
    /// Map each listed data ID; unlisted IDs are outside the slice.
    Indices(Vec<i32>),
}

impl CallibraryContextMap {
    pub(crate) fn active(&self) -> bool {
        !matches!(self, Self::All)
    }
    pub(crate) fn covers(&self, observation: i32) -> bool {
        match self {
            Self::Indices(ids) => usize::try_from(observation).is_ok_and(|id| id < ids.len()),
            _ => true,
        }
    }
    pub(crate) fn map(&self, observation: i32) -> i32 {
        match self {
            Self::All => -1,
            Self::SelfMap => observation,
            Self::Indices(ids) => usize::try_from(observation)
                .ok()
                .and_then(|id| ids.get(id))
                .copied()
                .unwrap_or(-1),
        }
    }
}

/// Callibrary-specific data selection and retained upstream configuration.
/// Mapping and interpolation follow CLPatchPanel rather than traditional setapply.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub struct CallibraryOptions {
    /// Literal caltable record key, before process-directory path resolution.
    pub table_name: String,
    /// Raw time/frequency strings retained for file output; validation follows slice selection.
    pub interpolation: Option<(String, String)>,
    /// MS slice to which this library instance contributes.
    pub selection: casa_ms::MsSelection,
    /// Explicit data FIELD_ID to calibration FIELD_ID map.
    pub field_map: Vec<i32>,
    /// Explicit observation grouping, independent of traditional perobs admission.
    pub observation_map: CallibraryContextMap,
    /// Explicit scan grouping, independent of traditional perscan admission.
    pub scan_map: CallibraryContextMap,
    /// Antenna map; hardware IDs beyond its length repeat the last value.
    pub antenna_map: Vec<i32>,
    /// Recorded by CASA's parser; CLPatchPanel 6.7.6 does not consume reach.
    pub reach: String,
}

/// Load supported apply specs from a CASA callibrary file.
pub fn load_apply_specs_from_callib(
    path: impl AsRef<Path>,
) -> Result<Vec<ApplyCalibrationTableSpec>, CallibError> {
    let path = path.as_ref();
    let contents = read_contents(path)?;
    parse_apply_specs(&path.display().to_string(), &contents)
}

/// Load a callibrary, resolving relative table names against the caller's
/// workspace, as CASA does when it runs in that directory.
pub fn load_apply_specs_from_callib_at(
    path: &Path,
    workspace: &Path,
) -> Result<Vec<ApplyCalibrationTableSpec>, CallibError> {
    let contents = read_contents(path)?;
    parse_apply_specs_at(&path.display().to_string(), &contents, Some(workspace))
}

fn read_contents(path: &Path) -> Result<String, CallibError> {
    fs::read_to_string(path).map_err(|source| CallibError::ReadFile {
        path: path.display().to_string(),
        source,
    })
}

fn parse_entries(
    source_name: &str,
    contents: &str,
    allow_empty_maps: bool,
) -> Result<(Vec<parser::Entry>, bool), CallibError> {
    parser::parse(contents, allow_empty_maps)
        .map_err(|(line_number, reason)| line_error(source_name, contents, line_number, reason))
}

fn line_error(
    source_name: &str,
    contents: &str,
    line_number: usize,
    reason: String,
) -> CallibError {
    CallibError::ParseLine {
        path: source_name.to_owned(),
        line_number,
        line: contents
            .lines()
            .nth(line_number.saturating_sub(1))
            .unwrap_or_default()
            .to_string(),
        reason,
    }
}

/// Parse an inline calibration-library string into `ApplyCalibrationTableSpec` entries.
pub fn parse_apply_specs(
    source_name: &str,
    contents: &str,
) -> Result<Vec<ApplyCalibrationTableSpec>, CallibError> {
    parse_apply_specs_at(source_name, contents, None)
}

/// Parse inline callibrary entries against an explicit workspace, defaulting
/// to the process working directory when the caller supplies none.
pub fn parse_apply_specs_at(
    source_name: &str,
    contents: &str,
    workspace: Option<&Path>,
) -> Result<Vec<ApplyCalibrationTableSpec>, CallibError> {
    let specs = collect_apply_specs(source_name, contents, false, workspace)?;
    if specs.is_empty() {
        return Err(CallibError::ParseLine {
            path: source_name.to_owned(),
            line_number: 0,
            line: String::new(),
            reason: "callibrary file did not contain any supported entries".to_string(),
        });
    }
    Ok(specs)
}

pub(crate) fn load_transform_apply_specs(
    path: &Path,
    workspace: &Path,
) -> Result<Vec<ApplyCalibrationTableSpec>, CallibError> {
    let contents = read_contents(path)?;
    // mstransform accepts empty mapping arrays and leaves calibration inactive
    // for an empty callibrary.
    collect_apply_specs(
        &path.display().to_string(),
        &contents,
        true,
        Some(workspace),
    )
}

fn collect_apply_specs(
    source_name: &str,
    contents: &str,
    allow_empty_maps: bool,
    workspace: Option<&Path>,
) -> Result<Vec<ApplyCalibrationTableSpec>, CallibError> {
    // CASA resolves relative table names against the process working
    // directory for both text and file callibraries.
    let base_dir = match workspace {
        Some(workspace) => std::path::absolute(workspace),
        None => std::env::current_dir(),
    }
    .map_err(|source| CallibError::ReadFile {
        path: source_name.to_owned(),
        source,
    })?;
    let mut specs = Vec::new();
    let mut calwt_by_table = BTreeMap::<String, bool>::new();

    let (entries, _) = parse_entries(source_name, contents, allow_empty_maps)?;
    for entry in entries {
        let mut spec = parse_callib_entry(entry.values, &base_dir)
            .map_err(|reason| line_error(source_name, contents, entry.line, reason))?;
        let literal = spec
            .callibrary
            .as_ref()
            .expect("file options")
            .table_name
            .clone();
        if let Some(existing) = calwt_by_table.get(&literal) {
            // CalLibraryParse retains the first table-level calwt when
            // subsequent lines add slices of the same physical table.
            spec.calwt = *existing;
        } else {
            calwt_by_table.insert(literal, spec.calwt);
        }
        specs.push(spec);
    }

    Ok(specs)
}

/// Save supported apply specs to a CASA callibrary file.
pub fn save_apply_specs_to_callib(
    path: impl AsRef<Path>,
    specs: &[ApplyCalibrationTableSpec],
) -> Result<(), CallibError> {
    let path = path.as_ref();
    let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
    let contents = specs
        .iter()
        .map(|spec| format_callib_line(base_dir, spec))
        .collect::<Vec<_>>()
        .join("\n");
    let mut output = contents;
    if !output.is_empty() {
        output.push('\n');
    }
    fs::write(path, output).map_err(|source| CallibError::WriteFile {
        path: path.display().to_string(),
        source,
    })
}

fn parse_callib_entry(
    mut values: BTreeMap<String, String>,
    base_dir: &Path,
) -> Result<ApplyCalibrationTableSpec, String> {
    let caltable = values
        .remove("caltable")
        .ok_or_else(|| "missing required caltable= directive".to_string())?;
    let table_name = strip_quotes(&caltable);
    let caltable_path = resolve_callib_path(base_dir, &table_name);
    let calwt = values
        .remove("calwt")
        .map(|value| parse_bool(&value))
        .transpose()?
        .unwrap_or(true);
    let mut selection = casa_ms::MsSelection::new();
    selection.field = values
        .remove("field")
        .map(|v| strip_quotes(&v))
        .filter(|v| !v.is_empty());
    selection.spw = values
        .remove("spw")
        .map(|v| strip_quotes(&v))
        .filter(|v| !v.is_empty());
    selection.observation = values
        .remove("obs")
        .map(|v| strip_quotes(&v))
        .filter(|v| !v.is_empty());
    selection.scan = values
        .remove("scan")
        .map(|v| strip_quotes(&v))
        .filter(|v| !v.is_empty());
    selection.intent = values
        .remove("intent")
        .map(|v| strip_quotes(&v))
        .filter(|v| !v.is_empty());
    let reach = values
        .remove("reach")
        .map(|v| strip_quotes(&v))
        .unwrap_or_default();
    let observation_map = parse_context_map(values.remove("obsmap"), "observation")?;
    let scan_map = parse_context_map(values.remove("scanmap"), "scan")?;
    let antenna_map = match values.remove("antmap") {
        Some(value) if value.trim().starts_with('[') => parse_int_array(&value, "antmap")?,
        // CLPatchPanel only consumes an integer array for antmap.
        _ => Vec::new(),
    };
    let field_value = values.remove("fldmap");
    let (gainfield, field_map) = match field_value {
        Some(value) if value.trim().starts_with('[') => (None, parse_int_array(&value, "fldmap")?),
        Some(value) => (parse_gainfield_selector(&value)?, Vec::new()),
        None => (None, Vec::new()),
    };
    let time = values
        .remove("tinterp")
        .map(|v| strip_quotes(&v))
        .unwrap_or_default();
    let frequency = values
        .remove("finterp")
        .map(|v| strip_quotes(&v))
        .unwrap_or_default();
    let interp = parse_interp(&time, &frequency);
    let callibrary = Some(CallibraryOptions {
        table_name,
        interpolation: Some((time, frequency)),
        selection,
        reach,
        field_map,
        observation_map,
        scan_map,
        antenna_map,
    });
    let apply_to = ApplyTableSelection::default();

    let spwmap = values
        .remove("spwmap")
        .filter(|value| value.starts_with('['))
        .map(|value| parse_int_array(&value, "spwmap"))
        .transpose()?
        .unwrap_or_default();

    for (key, _) in values {
        tracing::warn!(
            keyword = key,
            "Cal Library: undefined keyword will be ignored"
        );
    }

    Ok(ApplyCalibrationTableSpec {
        callibrary,
        path: caltable_path,
        apply_to,
        gainfield,
        spwmap,
        interp,
        calwt,
    })
}

fn parse_context_map(value: Option<String>, kind: &str) -> Result<CallibraryContextMap, String> {
    match value {
        None => Ok(CallibraryContextMap::All),
        Some(value) if value.trim().starts_with('[') => Ok(CallibraryContextMap::Indices(
            parse_int_array(&value, kind)?,
        )),
        Some(value) if strip_quotes(&value) == "self" => Ok(CallibraryContextMap::SelfMap),
        Some(value) => Err(format!("unrecognized {kind} mapping {value}")),
    }
}

fn format_callib_line(base_dir: &Path, spec: &ApplyCalibrationTableSpec) -> String {
    let literal = spec
        .callibrary
        .as_ref()
        .map(|library| library.table_name.as_str())
        .filter(|name| !name.is_empty());
    let name = literal
        .map(str::to_string)
        .unwrap_or_else(|| format_callib_path(base_dir, &spec.path));
    let quoted = if name.contains('\'') {
        format!("\"{name}\"")
    } else {
        format!("'{name}'")
    };
    let mut parts = vec![
        format!("caltable={quoted}"),
        format!("calwt={}", if spec.calwt { "T" } else { "F" }),
    ];

    if let Some(library) = &spec.callibrary {
        for (key, value) in [
            ("field", &library.selection.field),
            ("spw", &library.selection.spw),
            ("obs", &library.selection.observation),
            ("scan", &library.selection.scan),
            ("intent", &library.selection.intent),
        ] {
            if let Some(value) = value {
                parts.push(format!("{key}='{}'", escape_callib_string(value)));
            }
        }
        for (name, map) in [
            ("fldmap", &library.field_map),
            ("antmap", &library.antenna_map),
        ] {
            if !map.is_empty() {
                parts.push(format!(
                    "{name}=[{}]",
                    map.iter().map(i32::to_string).collect::<Vec<_>>().join(",")
                ));
            }
        }
        for (name, map) in [
            ("obsmap", &library.observation_map),
            ("scanmap", &library.scan_map),
        ] {
            match map {
                CallibraryContextMap::All => {}
                CallibraryContextMap::SelfMap => parts.push(format!("{name}='self'")),
                CallibraryContextMap::Indices(ids) => parts.push(format!(
                    "{name}=[{}]",
                    ids.iter().map(i32::to_string).collect::<Vec<_>>().join(",")
                )),
            }
        }
        if !library.reach.is_empty() {
            parts.push(format!("reach='{}'", escape_callib_string(&library.reach)));
        }
    }
    if !spec.apply_to.field_ids.is_empty() {
        parts.push(format!(
            "field='{}'",
            join_numeric_selector(&spec.apply_to.field_ids)
        ));
    }
    if !spec.apply_to.spectral_window_ids.is_empty() {
        parts.push(format!(
            "spw='{}'",
            join_numeric_selector(&spec.apply_to.spectral_window_ids)
        ));
    }
    if !spec.apply_to.observation_ids.is_empty() {
        parts.push(format!(
            "obs='{}'",
            join_numeric_selector(&spec.apply_to.observation_ids)
        ));
    }
    if let Some(gainfield) = spec.gainfield.as_ref() {
        parts.push(format!(
            "fldmap='{}'",
            escape_callib_string(&format_gainfield_selector(gainfield))
        ));
    }
    if !spec.spwmap.is_empty() {
        parts.push(format!(
            "spwmap=[{}]",
            spec.spwmap
                .iter()
                .map(i32::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ));
    }

    if let Some((time, frequency)) = spec
        .callibrary
        .as_ref()
        .and_then(|library| library.interpolation.as_ref())
    {
        if !time.is_empty() {
            parts.push(format!("tinterp='{time}'"));
        }
        if !frequency.is_empty() {
            parts.push(format!("finterp='{frequency}'"));
        }
        return parts.join(" ");
    }
    match spec.interp {
        ApplyInterpolationMode::Nearest => parts.push("tinterp='nearest'".to_string()),
        ApplyInterpolationMode::Linear => parts.push("tinterp='linear'".to_string()),
        ApplyInterpolationMode::NearestLinear => {
            parts.push("tinterp='nearest'".to_string());
            parts.push("finterp='linear'".to_string());
        }
        ApplyInterpolationMode::Casa {
            time,
            frequency,
            per_observation,
            per_scan,
            phase_delay,
            relative_frequency,
        } => {
            let mut time = format!("{time:?}").to_ascii_lowercase();
            if per_observation {
                time.push_str("perobs");
            }
            if per_scan {
                time.push_str("perscan");
            }
            if phase_delay {
                time.push_str("PD");
            }
            parts.push(format!("tinterp='{time}'"));
            if frequency != crate::FrequencyInterpolation::Default {
                let mut frequency = format!("{frequency:?}").to_ascii_lowercase();
                if relative_frequency {
                    frequency.push_str("rel");
                }
                parts.push(format!("finterp='{frequency}'"));
            }
        }
        ApplyInterpolationMode::Invalid => parts.push("tinterp='invalid'".to_string()),
    }

    parts.join(" ")
}

fn format_callib_path(_base_dir: &Path, path: &Path) -> String {
    // Keep absolute inputs absolute so CASA can read the output from any cwd.
    path.display().to_string()
}

fn join_numeric_selector(values: &[i32]) -> String {
    values
        .iter()
        .map(i32::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn format_gainfield_selector(selector: &GainFieldSelector) -> String {
    match selector {
        GainFieldSelector::Nearest => "nearest".to_string(),
        GainFieldSelector::FieldId(field_id) => field_id.to_string(),
        GainFieldSelector::Expression(name) => name.clone(),
    }
}

fn escape_callib_string(value: &str) -> String {
    value.replace('\'', "\\'")
}

fn resolve_callib_path(base_dir: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        base_dir.join(path)
    }
}

fn strip_quotes(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2 {
        let first = trimmed.as_bytes()[0] as char;
        let last = trimmed.as_bytes()[trimmed.len() - 1] as char;
        if (first == '\'' || first == '"') && first == last {
            return trimmed[1..trimmed.len() - 1].to_string();
        }
    }
    trimmed.to_string()
}

fn parse_bool(value: &str) -> Result<bool, String> {
    match strip_quotes(value).to_ascii_lowercase().as_str() {
        "t" | "true" | "1" | "yes" | "y" => Ok(true),
        "f" | "false" | "0" | "no" | "n" => Ok(false),
        other => Err(format!("unsupported boolean literal {other:?}")),
    }
}

fn parse_gainfield_selector(value: &str) -> Result<Option<GainFieldSelector>, String> {
    let value = strip_quotes(value);
    if value.is_empty() {
        return Err("Field mapping by selection failure: empty selection matches no fields".into());
    }
    if value == "nearest" {
        return Ok(Some(GainFieldSelector::Nearest));
    }
    Ok(Some(GainFieldSelector::Expression(value)))
}

fn parse_int_array(value: &str, label: &str) -> Result<Vec<i32>, String> {
    let value = strip_quotes(value);
    if value == "[]" {
        return Ok(Vec::new());
    }
    let inner = value
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(value.as_str());
    if inner.trim().is_empty() {
        return Err(format!(
            "empty {label} list is not accepted by CalLibraryGram"
        ));
    }
    inner
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|item| {
            item.parse::<i32>()
                .map_err(|error| format!("parse {label} value {item:?}: {error}"))
        })
        .collect()
}

fn parse_interp(time: &str, frequency: &str) -> ApplyInterpolationMode {
    // CTTimeInterp1 selects a recognized substring. CLPatchPanel frequency
    // interpolation falls back to linear, including the old aipslin spelling.
    let time_curve = if time.contains("nearest") {
        "nearest"
    } else if time.is_empty() || time.contains("linear") {
        "linear"
    } else if time.contains("cubic") {
        "cubic"
    } else if time.contains("spline") {
        "spline"
    } else {
        return ApplyInterpolationMode::Invalid;
    };
    let frequency_curve = if frequency.contains("nearest") {
        "nearest"
    } else if frequency.contains("linear") {
        "linear"
    } else if frequency.contains("cubic") {
        "cubic"
    } else if frequency.contains("spline") {
        "spline"
    } else {
        "linear"
    };
    ApplyInterpolationMode::parse_casa(&format!(
        "{time_curve}{},{frequency_curve}{}",
        if time.contains("PD") { "PD" } else { "" },
        if frequency.contains("flag") {
            "flag"
        } else {
            ""
        }
    ))
    .expect("canonical library interpolation")
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{load_apply_specs_from_callib, save_apply_specs_to_callib};
    use crate::{
        ApplyCalibrationTableSpec, ApplyInterpolationMode, ApplyTableSelection, GainFieldSelector,
    };

    #[test]
    fn interpolation_file_round_trip_keeps_raw_controls_and_effective_library_mode() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("interpolation.callib");
        for value in [
            "nearest,unknown",
            "linear,cubicflag",
            "linearperobsperscanPD,splinerel",
        ] {
            let mut spec = ApplyCalibrationTableSpec::new(dir.path().join("g.gcal"));
            spec.interp = ApplyInterpolationMode::parse_casa(value).unwrap();
            spec.callibrary = Some(crate::CallibraryOptions::default());
            save_apply_specs_to_callib(&path, &[spec.clone()]).unwrap();
            let text = std::fs::read_to_string(&path).unwrap();
            let loaded = load_apply_specs_from_callib(&path).unwrap();
            // Parsing may canonicalize Casa{nearest, linear, ...} to
            // NearestLinear; compare the actual interpolation controls.
            let expected = spec.interp.for_callibrary();
            assert_eq!(loaded[0].interp.time_options(2), expected.time_options(2));
            assert_eq!(loaded[0].interp.frequency_mode(), expected.frequency_mode());
            assert_eq!(
                loaded[0].interp.uses_frequency_axis(),
                expected.uses_frequency_axis()
            );
            if let ApplyInterpolationMode::Casa {
                relative_frequency, ..
            } = loaded[0].interp
            {
                assert!(!relative_frequency);
            }
            save_apply_specs_to_callib(&path, &loaded).unwrap();
            assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
        }
    }

    #[test]
    fn load_callib_parses_supported_apply_entries() {
        let dir = TempDir::new().expect("tempdir");
        let callib = dir.path().join("apply.callib");
        std::fs::write(
            &callib,
            "\
caltable='phase.gcal' calwt=F field='0~1' spw='0,2' obs='3' fldmap='nearest' tinterp='nearest'\n\
caltable='bandpass.bcal' calwt=F spwmap=[0,0] tinterp='nearest' finterp='linear'\n",
        )
        .expect("write callib");

        let specs = load_apply_specs_from_callib(&callib).expect("load callib");
        assert_eq!(specs.len(), 2);
        assert_eq!(
            specs[0].path,
            std::env::current_dir().unwrap().join("phase.gcal")
        );
        // File selectors are FIELD/SPW/observation expressions resolved on the MS,
        // rather than pre-parsed numeric native apply_to vectors.
        assert_eq!(specs[0].apply_to, ApplyTableSelection::default());
        let selection = &specs[0].callibrary.as_ref().unwrap().selection;
        assert_eq!(selection.field.as_deref(), Some("0~1"));
        assert_eq!(selection.spw.as_deref(), Some("0,2"));
        assert_eq!(selection.observation.as_deref(), Some("3"));
        assert_eq!(specs[0].gainfield, Some(GainFieldSelector::Nearest));
        // An omitted CLPatchPanel frequency mode is linear.
        assert_eq!(specs[0].interp, ApplyInterpolationMode::NearestLinear);
        assert_eq!(specs[1].spwmap, vec![0, 0]);
        assert_eq!(specs[1].interp, ApplyInterpolationMode::NearestLinear);
    }

    #[test]
    fn load_callib_retains_supported_intent() {
        let dir = TempDir::new().expect("tempdir");
        let callib = dir.path().join("apply.callib");
        std::fs::write(&callib, "caltable='phase.gcal' intent='CALIBRATE_PHASE'\n")
            .expect("write callib");

        let specs = load_apply_specs_from_callib(&callib).expect("intent is supported");
        assert_eq!(
            specs[0]
                .callibrary
                .as_ref()
                .unwrap()
                .selection
                .intent
                .as_deref(),
            Some("CALIBRATE_PHASE")
        );
    }

    #[test]
    fn save_callib_round_trips_supported_specs() {
        let dir = TempDir::new().expect("tempdir");
        let callib = dir.path().join("apply.callib");
        let specs = vec![
            ApplyCalibrationTableSpec {
                callibrary: None,
                path: dir.path().join("phase.gcal"),
                apply_to: ApplyTableSelection {
                    field_ids: vec![0, 1],
                    spectral_window_ids: vec![0, 2],
                    observation_ids: vec![3],
                },
                gainfield: Some(GainFieldSelector::Nearest),
                spwmap: Vec::new(),
                interp: ApplyInterpolationMode::Nearest,
                calwt: false,
            },
            ApplyCalibrationTableSpec {
                callibrary: None,
                path: dir.path().join("bandpass.bcal"),
                apply_to: ApplyTableSelection::default(),
                gainfield: Some(GainFieldSelector::FieldId(2)),
                spwmap: vec![0, 0],
                interp: ApplyInterpolationMode::NearestLinear,
                calwt: true,
            },
        ];

        save_apply_specs_to_callib(&callib, &specs).expect("save callib");
        let loaded = load_apply_specs_from_callib(&callib).expect("reload callib");
        // Loading a file changes typed native apply_to into its raw MS selector
        // representation. The canonical file and effective controls must persist.
        let text = std::fs::read_to_string(&callib).unwrap();
        save_apply_specs_to_callib(&callib, &loaded).unwrap();
        assert_eq!(std::fs::read_to_string(&callib).unwrap(), text);
        assert_eq!(loaded[0].path, specs[0].path);
        assert_eq!(loaded[0].calwt, specs[0].calwt);
        assert_eq!(loaded[0].gainfield, specs[0].gainfield);
        assert_eq!(
            loaded[0]
                .callibrary
                .as_ref()
                .unwrap()
                .selection
                .field
                .as_deref(),
            Some("0,1")
        );
        assert_eq!(loaded[1].spwmap, specs[1].spwmap);
        assert_eq!(
            loaded[1].gainfield,
            Some(GainFieldSelector::Expression("2".into()))
        );
        assert_eq!(loaded[1].interp, specs[1].interp);
    }
}
