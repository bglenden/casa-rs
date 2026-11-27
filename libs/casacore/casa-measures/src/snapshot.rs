// SPDX-License-Identifier: LGPL-3.0-or-later
//! Create and verify the independently distributed CASA measures snapshot.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use crate::{
    INSTALLED_PROVENANCE_FILE, MeasuresDataError, PACKAGED_SNAPSHOT_PATHS, REQUIRED_NONEMPTY_DIRS,
    REQUIRED_RELATIVE_PATHS, SnapshotProvenance, configured_measures_path, default_measures_path,
    measures_tree_complete,
};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tar::{Archive, Builder};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const SNAPSHOT_MANIFEST_JSON: &str = include_str!("../data/casa-measures-runtime.provenance.json");
static SNAPSHOT_MANIFEST: OnceLock<Result<SnapshotManifest, String>> = OnceLock::new();

#[derive(Serialize, Deserialize)]
struct SnapshotManifest {
    #[serde(flatten)]
    provenance: SnapshotProvenance,
    archive_size_bytes: u64,
    archive_sha256: String,
}

fn packaged_manifest() -> Result<&'static SnapshotManifest, MeasuresDataError> {
    match SNAPSHOT_MANIFEST.get_or_init(|| {
        serde_json::from_str(SNAPSHOT_MANIFEST_JSON).map_err(|error| error.to_string())
    }) {
        Ok(manifest) => Ok(manifest),
        Err(error) => Err(MeasuresDataError::Provenance(error.clone())),
    }
}

/// Return the release snapshot's provenance without reading its archive.
pub fn packaged_snapshot_provenance() -> Result<&'static SnapshotProvenance, MeasuresDataError> {
    Ok(&packaged_manifest()?.provenance)
}

/// Verify and install the release snapshot archive at a caller-selected destination.
///
/// Supply the gzip-compressed archive from the corresponding source release.
/// The library checks its size and SHA-256 against the packaged provenance
/// before writing any files, then validates the extracted CASA table tree.
/// Existing files at `root` may be replaced. This operation performs no
/// network I/O and does not change runtime discovery settings.
///
/// # Errors
///
/// Returns an error for unreadable or mismatched archive bytes, an invalid
/// packaged manifest, or any failure to extract and validate the tables or
/// write their provenance in `root`.
pub fn install_packaged_snapshot(
    archive_path: &Path,
    root: &Path,
) -> Result<SnapshotProvenance, MeasuresDataError> {
    let manifest = packaged_manifest()?;
    let mut archive_bytes = Vec::new();
    fs::File::open(archive_path)
        .and_then(|file| {
            file.take(manifest.archive_size_bytes.saturating_add(1))
                .read_to_end(&mut archive_bytes)
        })
        .map_err(|error| {
            MeasuresDataError::Bootstrap(format!("reading {}: {error}", archive_path.display()))
        })?;
    if archive_bytes.len() as u64 != manifest.archive_size_bytes
        || archive_sha256(&archive_bytes) != manifest.archive_sha256
    {
        return Err(MeasuresDataError::Bootstrap(format!(
            "{} does not match the release snapshot size and SHA-256",
            archive_path.display()
        )));
    }

    fs::create_dir_all(root).map_err(|error| {
        MeasuresDataError::Bootstrap(format!("creating {}: {error}", root.display()))
    })?;

    let decoder = GzDecoder::new(archive_bytes.as_slice());
    let mut archive = Archive::new(decoder);
    archive.unpack(root).map_err(|error| {
        MeasuresDataError::Bootstrap(format!(
            "extracting packaged snapshot into {}: {error}",
            root.display()
        ))
    })?;

    if !measures_tree_complete(root) {
        return Err(MeasuresDataError::Bootstrap(format!(
            "packaged snapshot extracted into {} but required tables are still missing",
            root.display()
        )));
    }
    let provenance = manifest.provenance.clone();
    let provenance_bytes = serde_json::to_vec_pretty(&provenance)
        .map_err(|error| MeasuresDataError::Provenance(error.to_string()))?;
    fs::write(root.join(INSTALLED_PROVENANCE_FILE), provenance_bytes).map_err(|error| {
        MeasuresDataError::Bootstrap(format!("writing installed provenance: {error}"))
    })?;
    Ok(provenance)
}

fn archive_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Candidate runtime roots suitable for packaging.
pub fn runtime_root_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Some(path) = configured_measures_path() {
        candidates.push(path);
    }
    if let Ok(path) = default_measures_path() {
        candidates.push(path);
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
    builder
        .into_inner()
        .and_then(GzEncoder::finish)
        .map_err(|error| error.to_string())?;
    let archive_bytes = fs::read(archive_path).map_err(|error| error.to_string())?;

    let provenance = crate::SnapshotProvenance {
        generated_at_utc: OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .map_err(|error| error.to_string())?,
        casarundata_version: required_manifest_value(&runtime_root.join("readme.txt"), "version")?,
        measures_version: required_manifest_value(
            &runtime_root.join("geodetic/readme.txt"),
            "version",
        )?,
        measures_site: required_manifest_value(&runtime_root.join("geodetic/readme.txt"), "site")?,
        included_paths: PACKAGED_SNAPSHOT_PATHS
            .iter()
            .map(|path| (*path).to_string())
            .collect(),
    };
    let manifest = SnapshotManifest {
        provenance,
        archive_size_bytes: archive_bytes.len() as u64,
        archive_sha256: archive_sha256(&archive_bytes),
    };
    let provenance_json =
        serde_json::to_string_pretty(&manifest).map_err(|error| error.to_string())?;
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

fn required_manifest_value(path: &Path, key: &str) -> Result<String, String> {
    read_manifest_value(path, key)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing required provenance {key:?} in {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::ENV_LOCK;
    use flate2::read::GzDecoder;
    use std::fs;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tar::Archive;

    fn write_file(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, contents).unwrap();
    }

    fn temp_root(name: &str) -> std::path::PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{name}-{}-{stamp}", std::process::id()))
    }

    fn write_runtime_root(
        root: &Path,
        include_all_required_files: bool,
        include_horizon_file: bool,
    ) {
        write_file(&root.join("readme.txt"), "version: 1.2.3\n");
        write_file(
            &root.join("geodetic/readme.txt"),
            "version: 4.5.6\nsite: TEST_SITE\n",
        );

        for relative in REQUIRED_RELATIVE_PATHS {
            if include_all_required_files || *relative != "ephemerides/Sources/table.dat" {
                write_file(&root.join(relative), "table");
            }
        }

        if include_horizon_file {
            write_file(
                &root.join("ephemerides/JPL-Horizons/nested/keep.txt"),
                "kept",
            );
            write_file(&root.join("geodetic/table.lock"), "skip");
            write_file(&root.join("ephemerides/DE405/data_update.lock"), "skip");
        }
    }

    #[test]
    fn runtime_root_candidates_include_configured_and_home_paths() {
        let _guard = ENV_LOCK.lock().unwrap();

        let tmp = temp_root("casa-measures-tools-root-candidates");
        fs::create_dir_all(&tmp).unwrap();
        let measures = tmp.join("measures");
        let home = tmp.join("home");

        let old_measures = std::env::var_os("CASA_RS_MEASURESPATH");
        let old_home = std::env::var_os("HOME");

        unsafe {
            std::env::set_var("CASA_RS_MEASURESPATH", &measures);
            std::env::set_var("HOME", &home);
        }

        let candidates = runtime_root_candidates();
        assert_eq!(candidates, vec![measures.clone(), home.join(".casa/data")]);

        unsafe {
            match old_measures {
                Some(value) => std::env::set_var("CASA_RS_MEASURESPATH", value),
                None => std::env::remove_var("CASA_RS_MEASURESPATH"),
            }
            match old_home {
                Some(value) => std::env::set_var("HOME", value),
                None => std::env::remove_var("HOME"),
            }
        }
    }

    #[test]
    fn validate_runtime_root_reports_missing_required_files() {
        let root = temp_root("casa-measures-tools-missing-file");
        fs::create_dir_all(&root).unwrap();
        write_runtime_root(&root, false, false);

        let error = validate_runtime_root(&root).unwrap_err();
        assert!(
            error.contains("missing required file"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validate_runtime_root_reports_empty_required_directories() {
        let root = temp_root("casa-measures-tools-empty-dir");
        fs::create_dir_all(&root).unwrap();
        write_runtime_root(&root, true, false);

        let error = validate_runtime_root(&root).unwrap_err();
        assert!(
            error.contains("missing or empty"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn create_packaged_snapshot_writes_archive_and_provenance() {
        let root = temp_root("casa-measures-tools-snapshot");
        fs::create_dir_all(&root).unwrap();
        write_runtime_root(&root, true, true);

        let archive_path = root.join("snapshot.tar.gz");
        let provenance_path = root.join("snapshot.provenance.json");
        create_packaged_snapshot(&root, &archive_path, &provenance_path).unwrap();

        let archive_file = fs::File::open(&archive_path).unwrap();
        let mut archive = Archive::new(GzDecoder::new(archive_file));
        let mut names = Vec::new();
        for entry in archive.entries().unwrap() {
            let entry = entry.unwrap();
            names.push(entry.path().unwrap().to_string_lossy().to_string());
        }

        assert!(names.contains(&"readme.txt".to_string()));
        assert!(names.contains(&"geodetic/readme.txt".to_string()));
        assert!(names.contains(&"ephemerides/JPL-Horizons/nested/keep.txt".to_string()));
        assert!(!names.iter().any(|name| name.ends_with("table.lock")));
        assert!(!names.iter().any(|name| name.ends_with("data_update.lock")));

        let provenance: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&provenance_path).unwrap()).unwrap();
        assert_eq!(provenance["casarundata_version"], "1.2.3");
        assert_eq!(provenance["measures_version"], "4.5.6");
        assert_eq!(provenance["measures_site"], "TEST_SITE");
        let archive_bytes = fs::read(&archive_path).unwrap();
        assert_eq!(provenance["archive_size_bytes"], archive_bytes.len());
        assert_eq!(provenance["archive_sha256"], archive_sha256(&archive_bytes));
        assert_eq!(
            provenance["included_paths"].as_array().unwrap().len(),
            PACKAGED_SNAPSHOT_PATHS.len()
        );
    }
}
