// SPDX-License-Identifier: LGPL-3.0-or-later
//! Internal maintenance helpers for packaging a CASA-table measures snapshot.

use std::fs;
use std::path::{Path, PathBuf};

use flate2::Compression;
use flate2::write::GzEncoder;
use tar::Builder;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// Relative paths included in the packaged fallback snapshot.
pub const PACKAGED_SNAPSHOT_PATHS: &[&str] = &[
    "readme.txt",
    "geodetic",
    "ephemerides/DE200",
    "ephemerides/DE405",
    "ephemerides/VGEO",
    "ephemerides/VTOP",
    "ephemerides/JPL-Horizons",
    "ephemerides/Sources",
    "ephemerides/Lines",
];

const REQUIRED_RELATIVE_PATHS: &[&str] = &[
    "geodetic/IERSeop2000/table.dat",
    "geodetic/IERSpredict2000/table.dat",
    "geodetic/TAI_UTC/table.dat",
    "geodetic/Observatories/table.dat",
    "geodetic/IGRF/table.dat",
    "ephemerides/DE200/table.dat",
    "ephemerides/DE405/table.dat",
    "ephemerides/VGEO/table.dat",
    "ephemerides/VTOP/table.dat",
    "ephemerides/Sources/table.dat",
    "ephemerides/Lines/table.dat",
];

const REQUIRED_NONEMPTY_DIRS: &[&str] = &["ephemerides/JPL-Horizons"];

/// Candidate runtime roots suitable for packaging.
pub fn runtime_root_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Ok(path) = std::env::var("CASA_RS_MEASURESPATH") {
        candidates.push(PathBuf::from(path));
    }
    if let Ok(path) = std::env::var("CASA_RS_DATA") {
        candidates.push(PathBuf::from(path));
    }
    if let Ok(home) = std::env::var("HOME") {
        candidates.push(PathBuf::from(home).join(".casa").join("data"));
    }

    candidates
}

/// Validate that `root` contains the required CASA-table runtime subset.
pub fn validate_runtime_root(root: &Path) -> Result<(), String> {
    for relative in REQUIRED_RELATIVE_PATHS {
        let path = root.join(relative);
        if !path.is_file() {
            return Err(format!("missing required file {}", path.display()));
        }
    }

    for relative in REQUIRED_NONEMPTY_DIRS {
        let path = root.join(relative);
        let nonempty = path
            .read_dir()
            .map(|mut entries| entries.next().is_some())
            .unwrap_or(false);
        if !path.is_dir() || !nonempty {
            return Err(format!(
                "required directory {} is missing or empty",
                path.display()
            ));
        }
    }

    Ok(())
}

/// Package the configured fallback snapshot archive and provenance JSON.
pub fn create_packaged_snapshot(
    runtime_root: &Path,
    archive_path: &Path,
    provenance_path: &Path,
) -> Result<(), String> {
    validate_runtime_root(runtime_root)?;

    if let Some(parent) = archive_path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    if let Some(parent) = provenance_path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }

    let archive_file = fs::File::create(archive_path).map_err(|error| error.to_string())?;
    let encoder = GzEncoder::new(archive_file, Compression::default());
    let mut builder = Builder::new(encoder);
    for relative in PACKAGED_SNAPSHOT_PATHS {
        let path = runtime_root.join(relative);
        append_snapshot_path(&mut builder, runtime_root, &path)?;
    }
    builder.finish().map_err(|error| error.to_string())?;

    let provenance = casa_measures_data::SnapshotProvenance {
        generated_at_utc: OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .map_err(|error| error.to_string())?,
        casarundata_version: read_manifest_value(&runtime_root.join("readme.txt"), "version")
            .unwrap_or_else(|| "unknown".to_string()),
        measures_version: read_manifest_value(&runtime_root.join("geodetic/readme.txt"), "version")
            .unwrap_or_else(|| "unknown".to_string()),
        measures_site: read_manifest_value(&runtime_root.join("geodetic/readme.txt"), "site")
            .unwrap_or_else(|| "unknown".to_string()),
        included_paths: PACKAGED_SNAPSHOT_PATHS
            .iter()
            .map(|path| (*path).to_string())
            .collect(),
    };
    let provenance_json =
        serde_json::to_string_pretty(&provenance).map_err(|error| error.to_string())?;
    fs::write(provenance_path, provenance_json).map_err(|error| error.to_string())
}

fn append_snapshot_path(
    builder: &mut Builder<GzEncoder<fs::File>>,
    runtime_root: &Path,
    path: &Path,
) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|error| error.to_string())? {
            let entry = entry.map_err(|error| error.to_string())?;
            append_snapshot_path(builder, runtime_root, &entry.path())?;
        }
        return Ok(());
    }

    let relative = path
        .strip_prefix(runtime_root)
        .map_err(|error| error.to_string())?;
    if relative
        .file_name()
        .is_some_and(|name| matches!(name.to_str(), Some("table.lock" | "data_update.lock")))
    {
        return Ok(());
    }

    builder
        .append_path_with_name(path, relative)
        .map_err(|error| error.to_string())
}

fn read_manifest_value(path: &Path, key: &str) -> Option<String> {
    let content = fs::read_to_string(path).ok()?;
    content.lines().find_map(|line| {
        let (found_key, value) = line.split_once(':')?;
        (found_key.trim() == key).then(|| value.trim().to_string())
    })
}
