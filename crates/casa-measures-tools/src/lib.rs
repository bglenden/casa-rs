// SPDX-License-Identifier: LGPL-3.0-or-later
//! Internal maintenance helpers for packaging a CASA-table measures snapshot.

use std::fs;
use std::path::{Path, PathBuf};

use casa_measures_data::{
    PACKAGED_SNAPSHOT_PATHS, REQUIRED_NONEMPTY_DIRS, REQUIRED_RELATIVE_PATHS,
};
use flate2::Compression;
use flate2::write::GzEncoder;
use tar::Builder;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// Candidate runtime roots suitable for packaging.
pub fn runtime_root_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Ok(path) = std::env::var("CASA_RS_MEASURESPATH") {
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

fn required_manifest_value(path: &Path, key: &str) -> Result<String, String> {
    read_manifest_value(path, key)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing required provenance {key:?} in {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::fs;
    use std::path::Path;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tar::Archive;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

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
        assert_eq!(
            provenance["included_paths"].as_array().unwrap().len(),
            PACKAGED_SNAPSHOT_PATHS.len()
        );
    }
}
