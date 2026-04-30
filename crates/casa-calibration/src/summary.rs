// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration-table opening and summary generation.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use casa_tables::{Table, TableError, TableOptions};
use casa_types::{ScalarValue, Value};
use thiserror::Error;

use crate::constants::{
    COL_ANTENNA1, COL_ANTENNA2, COL_CPARAM, COL_FIELD_ID, COL_FPARAM, COL_INTERVAL,
    COL_OBSERVATION_ID, COL_POLY_COEFF_AMP, COL_SPECTRAL_WINDOW_ID, COL_TIME, KEY_CASA_VERSION,
    KEY_MS_NAME, KEY_PAR_TYPE, KEY_POL_BASIS, KEY_VIS_CAL, LEGACY_CAL_DESC_KEYWORD,
    REQUIRED_BPOLY_COLUMNS, REQUIRED_COMPLEX_COLUMNS, REQUIRED_FLOAT_COLUMNS,
    STANDARD_SUBTABLE_KEYWORDS, TABLE_INFO_TYPE, TOLERATED_OPTIONAL_COLUMNS,
};
use crate::model::{
    CalibrationColumnSummary, CalibrationIssueSeverity, CalibrationKeywordSummary,
    CalibrationParameterFamily, CalibrationSubtableSummary, CalibrationTableSummary,
    CalibrationValidationIssue, TimeCoverageSummary,
};

/// Errors returned while opening or summarizing a calibration table.
#[derive(Debug, Error)]
pub enum CalibrationTableError {
    /// The table could not be opened through the shared `casa-tables`
    /// substrate.
    #[error("failed to open calibration table {path}: {source}")]
    Open {
        /// Path that was being opened.
        path: String,
        /// Underlying table error.
        #[source]
        source: TableError,
    },
}

/// Open one calibration table and return a normalized summary.
pub fn summarize_table(
    path: impl AsRef<Path>,
) -> Result<CalibrationTableSummary, CalibrationTableError> {
    let path = path.as_ref().to_path_buf();
    let table =
        Table::open(TableOptions::new(&path)).map_err(|source| CalibrationTableError::Open {
            path: path.display().to_string(),
            source,
        })?;
    Ok(build_summary(&path, &table))
}

/// Open several calibration tables in order and return their summaries.
pub fn summarize_tables<'a>(
    paths: impl IntoIterator<Item = &'a Path>,
) -> Result<Vec<CalibrationTableSummary>, CalibrationTableError> {
    paths.into_iter().map(summarize_table).collect()
}

fn build_summary(path: &Path, table: &Table) -> CalibrationTableSummary {
    let columns: Vec<String> = table
        .schema()
        .map(|schema| {
            schema
                .columns()
                .iter()
                .map(|column| column.name().to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let info = table.info();
    let keywords = summarize_keywords(table);
    let parameter_family = infer_parameter_family(info.sub_type.as_str(), &columns, &keywords);
    let subtables = summarize_subtables(path, table);
    let parameter_column =
        summarize_parameter_column(table, &columns, parameter_family, info.sub_type.as_str());
    let mut issues = validate_shape(
        path,
        info.table_type.as_str(),
        info.sub_type.as_str(),
        &columns,
        &keywords,
        &subtables,
        parameter_family,
    );
    let coverage = scan_coverage(table, &columns, &mut issues);
    let spectral_window_ids = if info.sub_type == "BPOLY" {
        scan_bpoly_spectral_window_ids(path, &mut issues)
    } else {
        coverage.spw_ids.into_iter().collect()
    };

    CalibrationTableSummary {
        path: path.to_path_buf(),
        table_type: info.table_type.clone(),
        table_subtype: info.sub_type.clone(),
        row_count: table.row_count(),
        columns,
        keywords,
        subtables,
        parameter_family,
        parameter_column,
        field_ids: coverage.field_ids.into_iter().collect(),
        spectral_window_ids,
        antenna1_ids: coverage.antenna1_ids.into_iter().collect(),
        antenna2_ids: coverage.antenna2_ids.into_iter().collect(),
        observation_ids: coverage.observation_ids.into_iter().collect(),
        time_coverage: coverage.time_coverage,
        issues,
    }
}

fn summarize_keywords(table: &Table) -> CalibrationKeywordSummary {
    let keywords = table.keywords();
    CalibrationKeywordSummary {
        par_type: keyword_string(keywords, KEY_PAR_TYPE),
        vis_cal: keyword_string(keywords, KEY_VIS_CAL),
        ms_name: keyword_string(keywords, KEY_MS_NAME),
        pol_basis: keyword_string(keywords, KEY_POL_BASIS),
        casa_version: keyword_string(keywords, KEY_CASA_VERSION),
    }
}

fn infer_parameter_family(
    table_subtype: &str,
    columns: &[String],
    keywords: &CalibrationKeywordSummary,
) -> CalibrationParameterFamily {
    if table_subtype == "BPOLY" {
        return CalibrationParameterFamily::Unknown;
    }
    match keywords.par_type.as_deref() {
        Some("Complex") => CalibrationParameterFamily::Complex,
        Some("Float") => CalibrationParameterFamily::Float,
        _ if columns.iter().any(|column| column == COL_CPARAM) => {
            CalibrationParameterFamily::Complex
        }
        _ if columns.iter().any(|column| column == COL_FPARAM) => CalibrationParameterFamily::Float,
        _ => CalibrationParameterFamily::Unknown,
    }
}

fn summarize_subtables(path: &Path, table: &Table) -> Vec<CalibrationSubtableSummary> {
    let keywords = table.keywords();
    STANDARD_SUBTABLE_KEYWORDS
        .iter()
        .map(|name| {
            let stored_reference = keywords
                .get(name)
                .and_then(Value::as_table_ref)
                .map(ToOwned::to_owned);
            let resolved_path = stored_reference
                .as_deref()
                .map(|stored| resolve_table_ref(path, stored));

            match resolved_path.as_ref() {
                Some(resolved) if resolved.exists() => {
                    match Table::open(TableOptions::new(resolved)) {
                        Ok(subtable) => CalibrationSubtableSummary {
                            name: (*name).to_string(),
                            stored_reference,
                            resolved_path,
                            exists: true,
                            row_count: Some(subtable.row_count()),
                            open_error: None,
                        },
                        Err(error) => CalibrationSubtableSummary {
                            name: (*name).to_string(),
                            stored_reference,
                            resolved_path,
                            exists: true,
                            row_count: None,
                            open_error: Some(error.to_string()),
                        },
                    }
                }
                Some(_) => CalibrationSubtableSummary {
                    name: (*name).to_string(),
                    stored_reference,
                    resolved_path,
                    exists: false,
                    row_count: None,
                    open_error: None,
                },
                None => CalibrationSubtableSummary {
                    name: (*name).to_string(),
                    stored_reference: None,
                    resolved_path: None,
                    exists: false,
                    row_count: None,
                    open_error: None,
                },
            }
        })
        .collect()
}

fn summarize_parameter_column(
    table: &Table,
    columns: &[String],
    family: CalibrationParameterFamily,
    table_subtype: &str,
) -> CalibrationColumnSummary {
    let parameter_column =
        if table_subtype == "BPOLY" && columns.iter().any(|column| column == COL_POLY_COEFF_AMP) {
            Some(COL_POLY_COEFF_AMP.to_string())
        } else {
            match family {
                CalibrationParameterFamily::Complex
                    if columns.iter().any(|column| column == COL_CPARAM) =>
                {
                    Some(COL_CPARAM.to_string())
                }
                CalibrationParameterFamily::Float
                    if columns.iter().any(|column| column == COL_FPARAM) =>
                {
                    Some(COL_FPARAM.to_string())
                }
                _ => None,
            }
        };
    let parameter_primitive_type = parameter_column
        .as_deref()
        .and_then(|column| table.schema()?.column(column))
        .and_then(|column| column.data_type())
        .map(|data_type| format!("{data_type:?}"));
    let first_cell_shape = parameter_column.as_deref().and_then(|column| {
        for row in 0..table.row_count() {
            if let Ok(array) = table
                .cell_accessor(row, column)
                .and_then(|cell| cell.array())
            {
                return Some(array.shape().to_vec());
            }
        }
        None
    });
    CalibrationColumnSummary {
        parameter_column,
        parameter_primitive_type,
        first_cell_shape,
    }
}

fn validate_shape(
    path: &Path,
    table_type: &str,
    table_subtype: &str,
    columns: &[String],
    keywords: &CalibrationKeywordSummary,
    subtables: &[CalibrationSubtableSummary],
    parameter_family: CalibrationParameterFamily,
) -> Vec<CalibrationValidationIssue> {
    let mut issues = Vec::new();

    if table_subtype == "BPOLY" {
        return validate_bpoly_shape(path, table_type, columns);
    }

    if table_type != TABLE_INFO_TYPE {
        issues.push(issue(
            "table-info-type",
            CalibrationIssueSeverity::Error,
            format!("expected table.info Type={TABLE_INFO_TYPE:?}, found {table_type:?}"),
        ));
    }
    if keywords.par_type.is_none() {
        issues.push(issue(
            "missing-par-type",
            CalibrationIssueSeverity::Error,
            "missing required ParType keyword".to_string(),
        ));
    }
    if keywords.vis_cal.is_none() {
        issues.push(issue(
            "missing-vis-cal",
            CalibrationIssueSeverity::Error,
            "missing required VisCal keyword".to_string(),
        ));
    }
    if keywords.ms_name.is_none() {
        issues.push(issue(
            "missing-ms-name",
            CalibrationIssueSeverity::Warning,
            "missing MSName keyword; tolerate on read but write the canonical form later"
                .to_string(),
        ));
    }
    if keywords.pol_basis.is_none() {
        issues.push(issue(
            "missing-pol-basis",
            CalibrationIssueSeverity::Warning,
            "missing PolBasis keyword; tolerate on read and normalize in memory".to_string(),
        ));
    }
    if keywords.casa_version.is_none() {
        issues.push(issue(
            "missing-casa-version",
            CalibrationIssueSeverity::Warning,
            "missing CASA_Version keyword; tolerate on read".to_string(),
        ));
    }

    let required_columns = match parameter_family {
        CalibrationParameterFamily::Complex => REQUIRED_COMPLEX_COLUMNS,
        CalibrationParameterFamily::Float => REQUIRED_FLOAT_COLUMNS,
        CalibrationParameterFamily::Unknown => REQUIRED_COMPLEX_COLUMNS,
    };
    for column in required_columns {
        if !columns.iter().any(|present| present == column) {
            issues.push(issue(
                format!("missing-column-{column}"),
                CalibrationIssueSeverity::Error,
                format!("missing required calibration MAIN column {column}"),
            ));
        }
    }
    for column in TOLERATED_OPTIONAL_COLUMNS {
        if !columns.iter().any(|present| present == column) {
            issues.push(issue(
                format!("missing-optional-column-{column}"),
                CalibrationIssueSeverity::Warning,
                format!("missing tolerated optional calibration column {column}"),
            ));
        }
    }

    for subtable in subtables {
        if subtable.name == "OBSERVATION" && !subtable.exists {
            issues.push(issue(
                "missing-observation-subtable",
                CalibrationIssueSeverity::Warning,
                "missing OBSERVATION keyword subtable; tolerate legacy tables on read".to_string(),
            ));
        }
        if subtable.name != "OBSERVATION" && subtable.stored_reference.is_none() {
            issues.push(issue(
                format!("missing-subtable-{}", subtable.name.to_ascii_lowercase()),
                CalibrationIssueSeverity::Error,
                format!("missing required {} subtable link", subtable.name),
            ));
        }
        if subtable.stored_reference.is_some() && !subtable.exists {
            issues.push(issue(
                format!("dangling-subtable-{}", subtable.name.to_ascii_lowercase()),
                CalibrationIssueSeverity::Error,
                format!("{} subtable link does not resolve on disk", subtable.name),
            ));
        }
        if let Some(open_error) = &subtable.open_error {
            issues.push(issue(
                format!(
                    "failed-open-subtable-{}",
                    subtable.name.to_ascii_lowercase()
                ),
                CalibrationIssueSeverity::Error,
                format!("failed to open {} subtable: {open_error}", subtable.name),
            ));
        }
    }

    match parameter_family {
        CalibrationParameterFamily::Complex => {}
        CalibrationParameterFamily::Float
            if matches!(
                keywords.vis_cal.as_deref(),
                Some("K Jones" | "KAntPos Jones" | "EGainCurve" | "TOpac")
            ) => {}
        CalibrationParameterFamily::Float => issues.push(issue(
            "unsupported-float-family",
            CalibrationIssueSeverity::Error,
            "float-parameter calibration tables are only supported for K Jones and VLA prior tables in the current apply surface"
                .to_string(),
        )),
        CalibrationParameterFamily::Unknown => issues.push(issue(
            "unknown-parameter-family",
            CalibrationIssueSeverity::Error,
            "unable to infer calibration parameter family from ParType/CPARAM/FPARAM".to_string(),
        )),
    }

    issues
}

fn validate_bpoly_shape(
    path: &Path,
    table_type: &str,
    columns: &[String],
) -> Vec<CalibrationValidationIssue> {
    let mut issues = Vec::new();
    if table_type != TABLE_INFO_TYPE {
        issues.push(issue(
            "table-info-type",
            CalibrationIssueSeverity::Error,
            format!("expected table.info Type={TABLE_INFO_TYPE:?}, found {table_type:?}"),
        ));
    }

    for column in REQUIRED_BPOLY_COLUMNS {
        if !columns.iter().any(|present| present == column) {
            issues.push(issue(
                format!("missing-column-{column}"),
                CalibrationIssueSeverity::Error,
                format!("missing required BPOLY MAIN column {column}"),
            ));
        }
    }

    let cal_desc_path = path.join(LEGACY_CAL_DESC_KEYWORD);
    if !cal_desc_path.exists() {
        issues.push(issue(
            "missing-cal-desc",
            CalibrationIssueSeverity::Error,
            "missing required CAL_DESC keyword subtable for BPOLY".to_string(),
        ));
    } else if let Err(error) = Table::open(TableOptions::new(&cal_desc_path)) {
        issues.push(issue(
            "failed-open-cal-desc",
            CalibrationIssueSeverity::Error,
            format!("failed to open CAL_DESC subtable for BPOLY: {error}"),
        ));
    }

    issues
}

#[derive(Default)]
struct CoverageAccumulator {
    field_ids: BTreeSet<i32>,
    spw_ids: BTreeSet<i32>,
    antenna1_ids: BTreeSet<i32>,
    antenna2_ids: BTreeSet<i32>,
    observation_ids: BTreeSet<i32>,
    time_coverage: Option<TimeCoverageSummary>,
}

fn scan_coverage(
    table: &Table,
    columns: &[String],
    issues: &mut Vec<CalibrationValidationIssue>,
) -> CoverageAccumulator {
    let mut coverage = CoverageAccumulator::default();

    let has_field = columns.iter().any(|column| column == COL_FIELD_ID);
    let has_spw = columns
        .iter()
        .any(|column| column == COL_SPECTRAL_WINDOW_ID);
    let has_ant1 = columns.iter().any(|column| column == COL_ANTENNA1);
    let has_ant2 = columns.iter().any(|column| column == COL_ANTENNA2);
    let has_obs = columns.iter().any(|column| column == COL_OBSERVATION_ID);
    let has_time = columns.iter().any(|column| column == COL_TIME);
    let has_interval = columns.iter().any(|column| column == COL_INTERVAL);

    let mut min_time = None::<f64>;
    let mut max_time = None::<f64>;
    let mut min_interval = None::<f64>;
    let mut max_interval = None::<f64>;

    for row in 0..table.row_count() {
        if has_field {
            collect_i32_cell(table, row, COL_FIELD_ID, &mut coverage.field_ids, issues);
        }
        if has_spw {
            collect_i32_cell(
                table,
                row,
                COL_SPECTRAL_WINDOW_ID,
                &mut coverage.spw_ids,
                issues,
            );
        }
        if has_ant1 {
            collect_i32_cell(table, row, COL_ANTENNA1, &mut coverage.antenna1_ids, issues);
        }
        if has_ant2 {
            collect_i32_cell(table, row, COL_ANTENNA2, &mut coverage.antenna2_ids, issues);
        }
        if has_obs {
            collect_i32_cell(
                table,
                row,
                COL_OBSERVATION_ID,
                &mut coverage.observation_ids,
                issues,
            );
        }
        if has_time {
            match table
                .cell_accessor(row, COL_TIME)
                .and_then(|cell| cell.scalar())
            {
                Ok(&ScalarValue::Float64(value)) => {
                    min_time = Some(min_time.map_or(value, |current| current.min(value)));
                    max_time = Some(max_time.map_or(value, |current| current.max(value)));
                }
                Ok(other) => issues.push(issue(
                    "time-column-type",
                    CalibrationIssueSeverity::Warning,
                    format!("TIME row {row} used unexpected scalar type {other:?}"),
                )),
                Err(error) => issues.push(issue(
                    "time-column-read",
                    CalibrationIssueSeverity::Warning,
                    format!("failed to read TIME row {row}: {error}"),
                )),
            }
        }
        if has_interval {
            match table
                .cell_accessor(row, COL_INTERVAL)
                .and_then(|cell| cell.scalar())
            {
                Ok(&ScalarValue::Float64(value)) => {
                    min_interval = Some(min_interval.map_or(value, |current| current.min(value)));
                    max_interval = Some(max_interval.map_or(value, |current| current.max(value)));
                }
                Ok(other) => issues.push(issue(
                    "interval-column-type",
                    CalibrationIssueSeverity::Warning,
                    format!("INTERVAL row {row} used unexpected scalar type {other:?}"),
                )),
                Err(error) => issues.push(issue(
                    "interval-column-read",
                    CalibrationIssueSeverity::Warning,
                    format!("failed to read INTERVAL row {row}: {error}"),
                )),
            }
        }
    }

    if let (Some(min_time), Some(max_time)) = (min_time, max_time) {
        coverage.time_coverage = Some(TimeCoverageSummary {
            min_time,
            max_time,
            min_interval,
            max_interval,
        });
    }

    coverage
}

fn scan_bpoly_spectral_window_ids(
    path: &Path,
    issues: &mut Vec<CalibrationValidationIssue>,
) -> Vec<i32> {
    let cal_desc_path = path.join(LEGACY_CAL_DESC_KEYWORD);
    let table = match Table::open(TableOptions::new(&cal_desc_path)) {
        Ok(table) => table,
        Err(error) => {
            issues.push(issue(
                "failed-open-cal-desc",
                CalibrationIssueSeverity::Warning,
                format!("failed to read BPOLY CAL_DESC spectral-window coverage: {error}"),
            ));
            return Vec::new();
        }
    };
    let mut spw_ids = BTreeSet::new();
    for row in 0..table.row_count() {
        match table
            .cell_accessor(row, COL_SPECTRAL_WINDOW_ID)
            .and_then(|cell| cell.array())
        {
            Ok(casa_types::ArrayValue::Int32(values)) => {
                for value in values.iter().copied() {
                    spw_ids.insert(value);
                }
            }
            Ok(other) => issues.push(issue(
                "bpoly-spw-array-type",
                CalibrationIssueSeverity::Warning,
                format!(
                    "BPOLY CAL_DESC row {row} used unexpected SPECTRAL_WINDOW_ID type {:?}",
                    other.primitive_type()
                ),
            )),
            Err(error) => issues.push(issue(
                "bpoly-spw-array-read",
                CalibrationIssueSeverity::Warning,
                format!("failed to read BPOLY CAL_DESC SPECTRAL_WINDOW_ID row {row}: {error}"),
            )),
        }
    }
    spw_ids.into_iter().collect()
}

fn collect_i32_cell(
    table: &Table,
    row: usize,
    column: &str,
    values: &mut BTreeSet<i32>,
    issues: &mut Vec<CalibrationValidationIssue>,
) {
    match table
        .cell_accessor(row, column)
        .and_then(|cell| cell.scalar())
    {
        Ok(&ScalarValue::Int32(value)) => {
            values.insert(value);
        }
        Ok(other) => issues.push(issue(
            format!("unexpected-{column}-type"),
            CalibrationIssueSeverity::Warning,
            format!("{column} row {row} used unexpected scalar type {other:?}"),
        )),
        Err(error) => issues.push(issue(
            format!("failed-read-{column}"),
            CalibrationIssueSeverity::Warning,
            format!("failed to read {column} row {row}: {error}"),
        )),
    }
}

fn keyword_string(record: &casa_types::RecordValue, name: &str) -> Option<String> {
    match record.get(name) {
        Some(Value::Scalar(ScalarValue::String(value))) => Some(value.clone()),
        _ => None,
    }
}

fn resolve_table_ref(table_path: &Path, stored: &str) -> PathBuf {
    let stored_path = Path::new(stored);
    if stored_path.is_absolute() {
        stored_path.to_path_buf()
    } else {
        table_path.join(stored_path)
    }
}

fn issue(
    code: impl Into<String>,
    severity: CalibrationIssueSeverity,
    message: impl Into<String>,
) -> CalibrationValidationIssue {
    CalibrationValidationIssue {
        code: code.into(),
        severity,
        message: message.into(),
    }
}
