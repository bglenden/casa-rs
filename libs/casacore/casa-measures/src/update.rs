// SPDX-License-Identifier: LGPL-3.0-or-later
//! Download and install CASA-compatible measures runtime bundles.
//!
//! This module is gated behind the `"update"` cargo feature and mirrors the
//! current CASA/casaconfig model:
//!
//! - fetch a `casarundata` tarball from the NRAO data site
//! - fetch the latest `WSRT_Measures_*.ztar` from an ASTRON/NRAO measures site
//! - overlay the measures update on top of the base tree
//! - preserve the base `geodetic/Observatories` table, matching CASA's default
//!   `measures_update` behavior

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use tar::Archive;

use super::{MeasuresDataError, SnapshotProvenance};

/// Default `casarundata` site used by CASA.
pub const DEFAULT_CASARUNDATA_SITE: &str = "https://go.nrao.edu/casarundata/";

/// Default measures sites tried in order when looking for `WSRT_Measures_*`.
pub const DEFAULT_MEASURES_SITES: &[&str] =
    &["https://www.astron.nl/iers/", "https://go.nrao.edu/iers/"];

/// Provenance filename written into refreshed runtime trees.
pub const PROVENANCE_FILENAME: &str = "casa-rs-measures-provenance.json";

/// Result of a measures refresh operation.
#[derive(Debug, Clone)]
pub struct RefreshResult {
    /// Installed runtime root.
    pub path: PathBuf,
    /// `casarundata` archive used for the refresh.
    pub casarundata_version: String,
    /// Measures overlay archive used for the refresh.
    pub measures_version: String,
    /// Measures site that supplied the overlay archive.
    pub measures_site: String,
}

/// Download the latest CASA-compatible runtime bundles and install them at `dest_root`.
pub fn refresh_measures_path(dest_root: &Path) -> Result<RefreshResult, MeasuresDataError> {
    refresh_measures_path_with_sites(dest_root, DEFAULT_CASARUNDATA_SITE, DEFAULT_MEASURES_SITES)
}

fn refresh_measures_path_with_sites(
    dest_root: &Path,
    casarundata_site: &str,
    measures_sites: &[&str],
) -> Result<RefreshResult, MeasuresDataError> {
    let casarundata_version = latest_archive_name(casarundata_site, "casarundata-", ".tar.gz")?;
    let casarundata_url = join_url(casarundata_site, &casarundata_version);

    let (measures_site, measures_version) = measures_sites
        .iter()
        .find_map(|site| {
            latest_archive_name(site, "WSRT_Measures_", ".ztar")
                .ok()
                .map(|version| ((*site).to_string(), version))
        })
        .ok_or_else(|| {
            MeasuresDataError::Bootstrap(format!(
                "unable to find a WSRT_Measures archive in configured sites: {}",
                measures_sites.join(", ")
            ))
        })?;
    let measures_url = join_url(&measures_site, &measures_version);

    let parent = dest_root.parent().ok_or_else(|| {
        MeasuresDataError::Bootstrap(format!(
            "destination {} has no parent directory",
            dest_root.display()
        ))
    })?;
    fs::create_dir_all(parent).map_err(|error| {
        MeasuresDataError::Bootstrap(format!("creating {}: {error}", parent.display()))
    })?;

    let staging_root = unique_sibling_path(dest_root, ".casa-rs-measures-staging");
    let backup_root = unique_sibling_path(dest_root, ".casa-rs-measures-backup");
    if staging_root.exists() {
        let _ = fs::remove_dir_all(&staging_root);
    }
    fs::create_dir_all(&staging_root).map_err(|error| {
        MeasuresDataError::Bootstrap(format!("creating {}: {error}", staging_root.display()))
    })?;

    let casarundata = download_bytes(&casarundata_url)?;
    extract_gzip_tar(&casarundata, &staging_root)?;

    let observatories_backup = unique_sibling_path(dest_root, ".casa-rs-observatories-backup");
    let observatories = staging_root.join("geodetic/Observatories");
    if observatories.is_dir() {
        copy_tree(&observatories, &observatories_backup)?;
    }

    let measures = download_bytes(&measures_url)?;
    extract_gzip_tar(&measures, &staging_root)?;

    if observatories_backup.is_dir() {
        if observatories.exists() {
            fs::remove_dir_all(&observatories).map_err(|error| {
                MeasuresDataError::Bootstrap(format!(
                    "removing {} before restore: {error}",
                    observatories.display()
                ))
            })?;
        }
        copy_tree(&observatories_backup, &observatories)?;
        fs::remove_dir_all(&observatories_backup).map_err(|error| {
            MeasuresDataError::Bootstrap(format!(
                "removing {}: {error}",
                observatories_backup.display()
            ))
        })?;
    }

    if !super::measures_tree_complete(&staging_root) {
        return Err(MeasuresDataError::Bootstrap(format!(
            "downloaded runtime tree at {} is missing required measures tables",
            staging_root.display()
        )));
    }

    let provenance = SnapshotProvenance {
        generated_at_utc: format!("{:?}", std::time::SystemTime::now()),
        casarundata_version: casarundata_version.clone(),
        measures_version: measures_version.clone(),
        measures_site: measures_site.clone(),
        included_paths: vec![
            "readme.txt".to_string(),
            "geodetic".to_string(),
            "ephemerides".to_string(),
        ],
    };
    let provenance_json = serde_json::to_string_pretty(&provenance).map_err(|error| {
        MeasuresDataError::Bootstrap(format!("serializing provenance: {error}"))
    })?;
    fs::write(staging_root.join(PROVENANCE_FILENAME), provenance_json).map_err(|error| {
        MeasuresDataError::Bootstrap(format!(
            "writing {}: {error}",
            staging_root.join(PROVENANCE_FILENAME).display()
        ))
    })?;

    if backup_root.exists() {
        let _ = fs::remove_dir_all(&backup_root);
    }
    if dest_root.exists() {
        fs::rename(dest_root, &backup_root).map_err(|error| {
            MeasuresDataError::Bootstrap(format!(
                "moving existing {} aside: {error}",
                dest_root.display()
            ))
        })?;
    }
    fs::rename(&staging_root, dest_root).map_err(|error| {
        MeasuresDataError::Bootstrap(format!(
            "installing refreshed runtime at {}: {error}",
            dest_root.display()
        ))
    })?;
    if backup_root.exists() {
        fs::remove_dir_all(&backup_root).map_err(|error| {
            MeasuresDataError::Bootstrap(format!(
                "removing backup {}: {error}",
                backup_root.display()
            ))
        })?;
    }

    Ok(RefreshResult {
        path: dest_root.to_path_buf(),
        casarundata_version,
        measures_version,
        measures_site,
    })
}

fn latest_archive_name(
    site_url: &str,
    prefix: &str,
    suffix: &str,
) -> Result<String, MeasuresDataError> {
    let index = download_text(site_url)?;
    let mut candidates = Vec::new();
    let mut search_start = 0usize;

    while let Some(found) = index[search_start..].find(prefix) {
        let start = search_start + found;
        let remainder = &index[start..];
        let Some(end_rel) = remainder.find(suffix) else {
            break;
        };
        let end = start + end_rel + suffix.len();
        let candidate = &index[start..end];
        if candidate
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
        {
            candidates.push(candidate.to_string());
        }
        search_start = end;
    }

    candidates.sort();
    candidates.dedup();
    candidates.pop().ok_or_else(|| {
        MeasuresDataError::Bootstrap(format!(
            "no archive matching {prefix}*{suffix} found at {site_url}"
        ))
    })
}

fn download_text(url: &str) -> Result<String, MeasuresDataError> {
    ureq::get(url)
        .call()
        .map_err(|error| MeasuresDataError::Bootstrap(format!("downloading {url}: {error}")))?
        .into_body()
        .read_to_string()
        .map_err(|error| MeasuresDataError::Bootstrap(format!("reading {url}: {error}")))
}

fn download_bytes(url: &str) -> Result<Vec<u8>, MeasuresDataError> {
    ureq::get(url)
        .call()
        .map_err(|error| MeasuresDataError::Bootstrap(format!("downloading {url}: {error}")))?
        .into_body()
        .read_to_vec()
        .map_err(|error| MeasuresDataError::Bootstrap(format!("reading {url}: {error}")))
}

fn extract_gzip_tar(bytes: &[u8], dest_root: &Path) -> Result<(), MeasuresDataError> {
    let decoder = GzDecoder::new(bytes);
    let mut archive = Archive::new(decoder);
    archive.unpack(dest_root).map_err(|error| {
        MeasuresDataError::Bootstrap(format!(
            "extracting archive into {}: {error}",
            dest_root.display()
        ))
    })
}

fn join_url(base: &str, name: &str) -> String {
    if base.ends_with('/') {
        format!("{base}{name}")
    } else {
        format!("{base}/{name}")
    }
}

fn unique_sibling_path(dest_root: &Path, prefix: &str) -> PathBuf {
    let parent = dest_root.parent().unwrap_or_else(|| Path::new("."));
    let mut name = OsString::from(prefix);
    name.push(format!(
        "-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0)
    ));
    parent.join(name)
}

fn copy_tree(src: &Path, dest: &Path) -> Result<(), MeasuresDataError> {
    if src.is_dir() {
        fs::create_dir_all(dest).map_err(|error| {
            MeasuresDataError::Bootstrap(format!("creating {}: {error}", dest.display()))
        })?;
        for entry in fs::read_dir(src).map_err(|error| {
            MeasuresDataError::Bootstrap(format!("reading {}: {error}", src.display()))
        })? {
            let entry = entry.map_err(|error| {
                MeasuresDataError::Bootstrap(format!("reading {}: {error}", src.display()))
            })?;
            copy_tree(&entry.path(), &dest.join(entry.file_name()))?;
        }
    } else {
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                MeasuresDataError::Bootstrap(format!("creating {}: {error}", parent.display()))
            })?;
        }
        fs::copy(src, dest).map_err(|error| {
            MeasuresDataError::Bootstrap(format!(
                "copying {} -> {}: {error}",
                src.display(),
                dest.display()
            ))
        })?;
    }
    Ok(())
}
