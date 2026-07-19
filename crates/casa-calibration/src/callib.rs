// SPDX-License-Identifier: LGPL-3.0-or-later
//! Narrow callibrary parsing for `applycal`-class workflows.
//!
//! The first slice intentionally parses only the subset that maps onto the
//! existing apply planner/executor surface:
//!
//! - `caltable`
//! - `calwt`
//! - `field`, `spw`, `obs`
//! - `fldmap`
//! - `spwmap`
//! - `tinterp`, `finterp`
//!
//! Unsupported callibrary directives stay explicit errors so the compatibility
//! boundary remains honest.

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

/// Load supported apply specs from a CASA callibrary file.
pub fn load_apply_specs_from_callib(
    path: impl AsRef<Path>,
) -> Result<Vec<ApplyCalibrationTableSpec>, CallibError> {
    let path = path.as_ref();
    let contents = fs::read_to_string(path).map_err(|source| CallibError::ReadFile {
        path: path.display().to_string(),
        source,
    })?;
    let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
    let mut specs = Vec::new();
    let mut calwt_by_table = BTreeMap::<PathBuf, bool>::new();

    for (line_number, raw_line) in contents.lines().enumerate() {
        let line_number = line_number + 1;
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let spec = parse_callib_line(line, base_dir).map_err(|reason| CallibError::ParseLine {
            path: path.display().to_string(),
            line_number,
            line: raw_line.to_string(),
            reason,
        })?;
        if let Some(existing) = calwt_by_table.get(&spec.path) {
            if *existing != spec.calwt {
                return Err(CallibError::ParseLine {
                    path: path.display().to_string(),
                    line_number,
                    line: raw_line.to_string(),
                    reason: format!(
                        "conflicting calwt values for repeated caltable {}",
                        spec.path.display()
                    ),
                });
            }
        } else {
            calwt_by_table.insert(spec.path.clone(), spec.calwt);
        }
        specs.push(spec);
    }

    if specs.is_empty() {
        return Err(CallibError::ParseLine {
            path: path.display().to_string(),
            line_number: 0,
            line: String::new(),
            reason: "callibrary file did not contain any supported entries".to_string(),
        });
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

fn parse_callib_line(line: &str, base_dir: &Path) -> Result<ApplyCalibrationTableSpec, String> {
    let tokens = tokenize_callib_line(line)?;
    let mut values = BTreeMap::<String, String>::new();
    for token in tokens {
        let (key, value) = token
            .split_once('=')
            .ok_or_else(|| format!("expected key=value token, found {token:?}"))?;
        values.insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    let caltable = values
        .remove("caltable")
        .ok_or_else(|| "missing required caltable= directive".to_string())?;
    let caltable_path = resolve_callib_path(base_dir, &strip_quotes(&caltable));
    let calwt = values
        .remove("calwt")
        .map(|value| parse_bool(&value))
        .transpose()?
        .unwrap_or(false);
    let apply_to = ApplyTableSelection {
        field_ids: values
            .remove("field")
            .map(|value| parse_numeric_selector(&value, "field"))
            .transpose()?
            .unwrap_or_default(),
        spectral_window_ids: values
            .remove("spw")
            .map(|value| parse_numeric_selector(&value, "spw"))
            .transpose()?
            .unwrap_or_default(),
        observation_ids: values
            .remove("obs")
            .map(|value| parse_numeric_selector(&value, "obs"))
            .transpose()?
            .unwrap_or_default(),
    };
    if let Some(intent) = values.remove("intent") {
        if !strip_quotes(&intent).is_empty() {
            return Err("intent= is not supported in this callibrary slice".to_string());
        }
    }
    if let Some(reach) = values.remove("reach") {
        if !strip_quotes(&reach).is_empty() {
            return Err("reach= is not supported in this callibrary slice".to_string());
        }
    }
    if let Some(obsmap) = values.remove("obsmap") {
        if !strip_quotes(&obsmap).is_empty() && strip_quotes(&obsmap) != "[]" {
            return Err("obsmap= is not supported in this callibrary slice".to_string());
        }
    }
    if let Some(antmap) = values.remove("antmap") {
        if !strip_quotes(&antmap).is_empty() && strip_quotes(&antmap) != "[]" {
            return Err("antmap= is not supported in this callibrary slice".to_string());
        }
    }

    let gainfield = values
        .remove("fldmap")
        .or_else(|| values.remove("gainfield"))
        .map(|value| parse_gainfield_selector(&value))
        .transpose()?
        .flatten();
    let spwmap = values
        .remove("spwmap")
        .map(|value| parse_int_array(&value, "spwmap"))
        .transpose()?
        .unwrap_or_default();
    let interp = parse_interp(
        values.remove("tinterp").as_deref(),
        values.remove("finterp").as_deref(),
    )?;

    if let Some((key, _)) = values.into_iter().next() {
        return Err(format!("unsupported callibrary directive {key}="));
    }

    Ok(ApplyCalibrationTableSpec {
        path: caltable_path,
        apply_to,
        gainfield,
        spwmap,
        interp,
        calwt,
    })
}

fn format_callib_line(base_dir: &Path, spec: &ApplyCalibrationTableSpec) -> String {
    let mut parts = vec![
        format!(
            "caltable='{}'",
            escape_callib_string(&format_callib_path(base_dir, &spec.path))
        ),
        format!("calwt={}", if spec.calwt { "T" } else { "F" }),
    ];

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

    match spec.interp {
        ApplyInterpolationMode::Nearest => parts.push("tinterp='nearest'".to_string()),
        ApplyInterpolationMode::Linear => parts.push("tinterp='linear'".to_string()),
        ApplyInterpolationMode::NearestLinear => {
            parts.push("tinterp='nearest'".to_string());
            parts.push("finterp='linear'".to_string());
        }
    }

    parts.join(" ")
}

fn format_callib_path(base_dir: &Path, path: &Path) -> String {
    path.strip_prefix(base_dir)
        .unwrap_or(path)
        .display()
        .to_string()
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
        GainFieldSelector::FieldName(name) => name.clone(),
    }
}

fn escape_callib_string(value: &str) -> String {
    value.replace('\'', "\\'")
}

fn tokenize_callib_line(line: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut bracket_depth = 0usize;

    for ch in line.chars() {
        match ch {
            '\'' | '"' => {
                if let Some(active) = quote {
                    if active == ch {
                        quote = None;
                    }
                } else {
                    quote = Some(ch);
                }
                current.push(ch);
            }
            '[' if quote.is_none() => {
                bracket_depth += 1;
                current.push(ch);
            }
            ']' if quote.is_none() => {
                if bracket_depth == 0 {
                    return Err("unmatched closing ']'".to_string());
                }
                bracket_depth -= 1;
                current.push(ch);
            }
            ch if ch.is_whitespace() && quote.is_none() && bracket_depth == 0 => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(ch),
        }
    }

    if quote.is_some() {
        return Err("unterminated quoted string".to_string());
    }
    if bracket_depth != 0 {
        return Err("unterminated list literal".to_string());
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    Ok(tokens)
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
            return trimmed[1..trimmed.len() - 1].trim().to_string();
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

fn parse_numeric_selector(value: &str, label: &str) -> Result<Vec<i32>, String> {
    let value = strip_quotes(value);
    casa_ms::parse_numeric_id_selector(&value, label).map_err(|error| error.to_string())
}

fn parse_gainfield_selector(value: &str) -> Result<Option<GainFieldSelector>, String> {
    let value = strip_quotes(value);
    if value.is_empty() {
        return Ok(None);
    }
    if value.eq_ignore_ascii_case("nearest") {
        return Ok(Some(GainFieldSelector::Nearest));
    }
    if let Ok(ids) = parse_numeric_selector(&value, "fldmap") {
        return match ids.as_slice() {
            [] => Ok(None),
            [field_id] => Ok(Some(GainFieldSelector::FieldId(*field_id))),
            _ => Err("fldmap supports only one numeric FIELD_ID in this wave".to_string()),
        };
    }
    Ok(Some(GainFieldSelector::FieldName(value)))
}

fn parse_int_array(value: &str, label: &str) -> Result<Vec<i32>, String> {
    let value = strip_quotes(value);
    let inner = value
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(value.as_str());
    if inner.trim().is_empty() {
        return Ok(Vec::new());
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

fn parse_interp(
    tinterp: Option<&str>,
    finterp: Option<&str>,
) -> Result<ApplyInterpolationMode, String> {
    let tinterp = tinterp
        .map(strip_quotes)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let finterp = finterp
        .map(strip_quotes)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let effective_tinterp = if tinterp.is_empty() {
        "linear"
    } else {
        &tinterp
    };

    match (effective_tinterp, finterp.as_str()) {
        ("nearest", "") => Ok(ApplyInterpolationMode::Nearest),
        ("linear", "") => Ok(ApplyInterpolationMode::Linear),
        ("nearest", "linear") => Ok(ApplyInterpolationMode::NearestLinear),
        _ => Err(format!(
            "unsupported interpolation combination tinterp={effective_tinterp:?}, finterp={:?}",
            finterp
        )),
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{load_apply_specs_from_callib, save_apply_specs_to_callib};
    use crate::{
        ApplyCalibrationTableSpec, ApplyInterpolationMode, ApplyTableSelection, GainFieldSelector,
    };

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
        assert_eq!(specs[0].path, dir.path().join("phase.gcal"));
        assert_eq!(
            specs[0].apply_to,
            ApplyTableSelection {
                field_ids: vec![0, 1],
                spectral_window_ids: vec![0, 2],
                observation_ids: vec![3],
            }
        );
        assert_eq!(specs[0].gainfield, Some(GainFieldSelector::Nearest));
        assert_eq!(specs[0].interp, ApplyInterpolationMode::Nearest);
        assert_eq!(specs[1].spwmap, vec![0, 0]);
        assert_eq!(specs[1].interp, ApplyInterpolationMode::NearestLinear);
    }

    #[test]
    fn load_callib_rejects_unsupported_directives() {
        let dir = TempDir::new().expect("tempdir");
        let callib = dir.path().join("apply.callib");
        std::fs::write(&callib, "caltable='phase.gcal' intent='CALIBRATE_PHASE'\n")
            .expect("write callib");

        let error = load_apply_specs_from_callib(&callib).expect_err("reject unsupported intent");
        assert!(
            error.to_string().contains("intent="),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn save_callib_round_trips_supported_specs() {
        let dir = TempDir::new().expect("tempdir");
        let callib = dir.path().join("apply.callib");
        let specs = vec![
            ApplyCalibrationTableSpec {
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
        assert_eq!(loaded, specs);
    }
}
