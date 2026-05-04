// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared frontend services exposed to Swift and Python through UniFFI.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use casa_images::{AnyPagedImage, ImagePixelType};
use casa_ms::MeasurementSet;
use casa_tables::{Table, TableOptions};
use thiserror::Error;

const MAX_PROJECT_SCAN_ENTRIES: usize = 512;
const MAX_PROJECT_SCAN_DEPTH: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum DatasetKind {
    MeasurementSet,
    Image,
    Table,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct DatasetProbe {
    pub id: String,
    pub name: String,
    pub path: String,
    pub kind: DatasetKind,
    pub size_bytes: u64,
    pub modified_unix_seconds: Option<u64>,
    pub probed_unix_seconds: u64,
    pub logical_size: String,
    pub units: String,
    pub fields: Vec<String>,
    pub spectral_windows: Vec<String>,
    pub scans: Vec<String>,
    pub antennas: Vec<String>,
    pub correlations: Vec<String>,
    pub columns: Vec<String>,
    pub shape: Vec<u64>,
    pub notes: String,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct ProjectProbe {
    pub name: String,
    pub root_path: String,
    pub datasets: Vec<DatasetProbe>,
    pub diagnostics: Vec<String>,
    pub scanned_entry_count: u64,
    pub truncated: bool,
}

#[derive(Debug, Error, uniffi::Error)]
pub enum FrontendServiceError {
    #[error("invalid path: {reason}")]
    InvalidPath { reason: String },
    #[error("I/O error: {reason}")]
    Io { reason: String },
    #[error("probe failed: {reason}")]
    Probe { reason: String },
}

type FrontendResult<T> = Result<T, FrontendServiceError>;

#[uniffi::export]
pub fn probe_path(path: String) -> FrontendResult<Option<DatasetProbe>> {
    let path = PathBuf::from(path);
    probe_dataset_path(&path).map_err(|error| FrontendServiceError::Probe {
        reason: format!("{}: {error}", path.display()),
    })
}

#[uniffi::export]
pub fn probe_project(path: String) -> FrontendResult<ProjectProbe> {
    let root = PathBuf::from(path);
    let metadata = fs::metadata(&root).map_err(|error| FrontendServiceError::Io {
        reason: format!("metadata {}: {error}", root.display()),
    })?;
    if !metadata.is_dir() {
        return Err(FrontendServiceError::InvalidPath {
            reason: format!("{} is not a directory", root.display()),
        });
    }

    let mut scan = ProjectScan::default();
    scan_path(&root, 0, &mut scan);
    let name = root
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("Project")
        .to_string();

    Ok(ProjectProbe {
        name,
        root_path: root.display().to_string(),
        datasets: scan.datasets,
        diagnostics: scan.diagnostics,
        scanned_entry_count: scan.scanned_entry_count as u64,
        truncated: scan.truncated,
    })
}

#[derive(Default)]
struct ProjectScan {
    datasets: Vec<DatasetProbe>,
    diagnostics: Vec<String>,
    scanned_entry_count: usize,
    truncated: bool,
}

fn scan_path(path: &Path, depth: usize, scan: &mut ProjectScan) {
    if scan.scanned_entry_count >= MAX_PROJECT_SCAN_ENTRIES {
        scan.truncated = true;
        return;
    }
    scan.scanned_entry_count += 1;

    match probe_dataset_path(path) {
        Ok(Some(dataset)) => {
            scan.datasets.push(dataset);
            return;
        }
        Ok(None) => {}
        Err(error) => scan
            .diagnostics
            .push(format!("{}: {error}", path.display())),
    }

    if depth >= MAX_PROJECT_SCAN_DEPTH {
        return;
    }
    let Ok(metadata) = fs::metadata(path) else {
        return;
    };
    if !metadata.is_dir() {
        return;
    }

    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) => {
            scan.diagnostics
                .push(format!("read {}: {error}", path.display()));
            return;
        }
    };
    for entry in entries {
        if scan.scanned_entry_count >= MAX_PROJECT_SCAN_ENTRIES {
            scan.truncated = true;
            break;
        }
        match entry {
            Ok(entry) => scan_path(&entry.path(), depth + 1, scan),
            Err(error) => scan.diagnostics.push(format!("directory entry: {error}")),
        }
    }
}

fn probe_dataset_path(path: &Path) -> Result<Option<DatasetProbe>, String> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) => return Err(format!("metadata failed: {error}")),
    };
    if !(metadata.is_dir() || metadata.is_file()) {
        return Ok(None);
    }

    if metadata.is_dir() {
        if let Some(probe) = probe_measurement_set(path, &metadata)? {
            return Ok(Some(probe));
        }
        if let Some(probe) = probe_image(path, &metadata)? {
            return Ok(Some(probe));
        }
        if let Some(probe) = probe_table(path, &metadata)? {
            return Ok(Some(probe));
        }
    }

    Ok(None)
}

fn probe_measurement_set(
    path: &Path,
    metadata: &fs::Metadata,
) -> Result<Option<DatasetProbe>, String> {
    let ms = match MeasurementSet::open(path) {
        Ok(ms) => ms,
        Err(_) => return Ok(None),
    };
    let summary = match ms.summary() {
        Ok(summary) => summary,
        Err(_) => return Ok(None),
    };
    let columns = table_columns(ms.main_table());
    let fields = summary
        .fields
        .iter()
        .map(|field| format!("{}: {}", field.field_id, field.name))
        .collect();
    let spectral_windows = summary
        .spectral_windows
        .iter()
        .map(|spw| {
            format!(
                "spw {}: {} chan, {:.6} GHz center",
                spw.spectral_window_id,
                spw.num_channels,
                spw.center_frequency_hz / 1.0e9
            )
        })
        .collect();
    let scans = summary
        .scans
        .iter()
        .map(|scan| {
            format!(
                "scan {}: {} rows, {}",
                scan.scan_number, scan.row_count, scan.field_name
            )
        })
        .collect();
    let antennas = summary
        .antennas
        .iter()
        .map(|antenna| antenna.name.clone())
        .collect();
    let correlations = unique_sorted(
        summary
            .polarization_setups
            .iter()
            .flat_map(|polarization| polarization.correlation_types.iter().cloned()),
    );

    Ok(Some(DatasetProbe {
        id: stable_id(path),
        name: path_name(path),
        path: path.display().to_string(),
        kind: DatasetKind::MeasurementSet,
        size_bytes: metadata.len(),
        modified_unix_seconds: modified_unix_seconds(metadata),
        probed_unix_seconds: now_unix_seconds(),
        logical_size: format!(
            "{} rows, {} fields, {} spw, {} antennas",
            summary.measurement_set.row_count,
            summary.measurement_set.field_count,
            summary.measurement_set.spectral_window_count,
            summary.measurement_set.antenna_count
        ),
        units: "Jy, Hz, seconds".to_string(),
        fields,
        spectral_windows,
        scans,
        antennas,
        correlations,
        columns,
        shape: vec![summary.measurement_set.row_count as u64],
        notes: "Recognized by opening the path as a MeasurementSet and reading MS metadata."
            .to_string(),
        diagnostics: vec![],
    }))
}

fn probe_image(path: &Path, metadata: &fs::Metadata) -> Result<Option<DatasetProbe>, String> {
    let image = match AnyPagedImage::open(path) {
        Ok(image) => image,
        Err(_) => return Ok(None),
    };
    let pixel_type = match image.pixel_type() {
        ImagePixelType::Float32 => "float32",
        ImagePixelType::Float64 => "float64",
        ImagePixelType::Complex32 => "complex64",
        ImagePixelType::Complex64 => "complex128",
    };
    let shape: Vec<u64> = image.shape().iter().map(|value| *value as u64).collect();
    let mask_names = image.mask_names();
    let region_names = image.region_names();
    let mut diagnostics = Vec::new();
    if let Some(default_mask) = image.default_mask_name() {
        diagnostics.push(format!("default mask: {default_mask}"));
    }

    Ok(Some(DatasetProbe {
        id: stable_id(path),
        name: path_name(path),
        path: path.display().to_string(),
        kind: DatasetKind::Image,
        size_bytes: metadata.len(),
        modified_unix_seconds: modified_unix_seconds(metadata),
        probed_unix_seconds: now_unix_seconds(),
        logical_size: format_shape(&shape),
        units: pixel_type.to_string(),
        fields: vec![],
        spectral_windows: vec![],
        scans: vec![],
        antennas: vec![],
        correlations: vec![],
        columns: vec!["map".to_string()],
        shape,
        notes: format!(
            "Recognized by opening the path as a casa-rs image; {} masks, {} regions.",
            mask_names.len(),
            region_names.len()
        ),
        diagnostics,
    }))
}

fn probe_table(path: &Path, metadata: &fs::Metadata) -> Result<Option<DatasetProbe>, String> {
    let table = match Table::open(TableOptions::new(path)) {
        Ok(table) => table,
        Err(_) => return Ok(None),
    };
    let columns = table_columns(&table);
    if columns.is_empty() && table.row_count() == 0 && table.info().table_type.is_empty() {
        return Ok(None);
    }
    let table_type = if table.info().table_type.is_empty() {
        "casacore table".to_string()
    } else {
        table.info().table_type.clone()
    };

    Ok(Some(DatasetProbe {
        id: stable_id(path),
        name: path_name(path),
        path: path.display().to_string(),
        kind: DatasetKind::Table,
        size_bytes: metadata.len(),
        modified_unix_seconds: modified_unix_seconds(metadata),
        probed_unix_seconds: now_unix_seconds(),
        logical_size: format!("{} rows, {} columns", table.row_count(), columns.len()),
        units: table_type.clone(),
        fields: vec![],
        spectral_windows: vec![],
        scans: vec![],
        antennas: vec![],
        correlations: vec![],
        columns,
        shape: vec![table.row_count() as u64],
        notes: format!("Recognized by opening the path as a {table_type}."),
        diagnostics: vec![],
    }))
}

fn table_columns(table: &Table) -> Vec<String> {
    table
        .schema()
        .map(|schema| {
            schema
                .columns()
                .iter()
                .map(|column| column.name().to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn stable_id(path: &Path) -> String {
    path.canonicalize()
        .unwrap_or_else(|_| path.to_path_buf())
        .display()
        .to_string()
}

fn path_name(path: &Path) -> String {
    if let Some(name) = path
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
    {
        return name.to_string();
    }
    path.to_string_lossy().into_owned()
}

fn modified_unix_seconds(metadata: &fs::Metadata) -> Option<u64> {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn unique_sorted(values: impl IntoIterator<Item = String>) -> Vec<String> {
    values
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn format_shape(shape: &[u64]) -> String {
    if shape.is_empty() {
        return "scalar".to_string();
    }
    shape
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(" x ")
}

uniffi::setup_scaffolding!();

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;

    use casa_tables::{ColumnSchema, TableInfo, TableSchema};
    use casa_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
    use flate2::read::GzDecoder;
    use tempfile::TempDir;

    fn unpack_small_ms() -> (TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let fixture = File::open("../casa-ms/tests/fixtures/mssel_test_small.ms.tgz")
            .expect("small MS fixture");
        let decoder = GzDecoder::new(fixture);
        let mut archive = tar::Archive::new(decoder);
        archive.unpack(dir.path()).expect("unpack MS fixture");
        let ms_path = dir.path().join("mssel_test_small.ms");
        (dir, ms_path)
    }

    fn make_table(path: &Path) {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("id", PrimitiveType::Int32),
            ColumnSchema::scalar("name", PrimitiveType::String),
        ])
        .expect("schema");
        let mut table = Table::with_schema(schema);
        table.set_info(TableInfo {
            table_type: "Calibration Table".to_string(),
            sub_type: "G Jones".to_string(),
        });
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new(
                    "name",
                    Value::Scalar(ScalarValue::String("gain".to_string())),
                ),
            ]))
            .expect("row");
        table.save(TableOptions::new(path)).expect("save table");
    }

    #[test]
    fn probe_measurement_set_reads_real_metadata() {
        let (_dir, ms_path) = unpack_small_ms();

        let probe = probe_path(ms_path.display().to_string())
            .expect("probe")
            .expect("recognized");

        assert_eq!(probe.kind, DatasetKind::MeasurementSet);
        assert!(probe.logical_size.contains("rows"));
        assert!(!probe.fields.is_empty());
        assert!(!probe.spectral_windows.is_empty());
        assert!(!probe.antennas.is_empty());
        assert!(probe.columns.iter().any(|column| column == "DATA"));
    }

    #[test]
    fn probe_project_discovers_ms_and_table_without_suffix_guessing() {
        let (dir, ms_path) = unpack_small_ms();
        let table_path = dir.path().join("derived_gain");
        make_table(&table_path);
        fs::write(dir.path().join("notes.txt"), "not a dataset").expect("notes");

        let project = probe_project(dir.path().display().to_string()).expect("project");

        assert!(
            project
                .datasets
                .iter()
                .any(|dataset| dataset.path == ms_path.display().to_string()
                    && dataset.kind == DatasetKind::MeasurementSet)
        );
        assert!(
            project
                .datasets
                .iter()
                .any(|dataset| dataset.path == table_path.display().to_string()
                    && dataset.kind == DatasetKind::Table)
        );
        assert!(
            !project
                .datasets
                .iter()
                .any(|dataset| dataset.name == "notes.txt")
        );
    }

    #[test]
    fn unrecognized_path_returns_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("plain.dat");
        fs::write(&file, "plain").expect("write");

        assert!(
            probe_path(file.display().to_string())
                .expect("probe")
                .is_none()
        );
    }
}
