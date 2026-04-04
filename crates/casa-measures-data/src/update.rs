// SPDX-License-Identifier: LGPL-3.0-or-later
//! Download and safe-update mechanism for IERS EOP data.
//!
//! This module is gated behind the `"update"` cargo feature and provides
//! functions to download the latest finals2000A.data from IERS/USNO,
//! validate it, and safely swap it into place.

use std::fs;
use std::path::{Path, PathBuf};

use super::{EopError, EopSummary, EopTable};

/// Default download URL for finals2000A.data.
///
/// The USNO mirror is used as the primary source since it provides the
/// standard fixed-column ASCII format directly.
pub const DEFAULT_URL: &str = "https://maia.usno.navy.mil/ser7/finals2000A.data";

/// Alternative download URL (IERS Data Center).
pub const IERS_URL: &str = "https://datacenter.iers.org/data/latestVersion/finals2000A.data";

/// Result of a download-and-install operation.
#[derive(Debug)]
pub enum UpdateResult {
    /// New data was installed (path, summary).
    Updated(PathBuf, EopSummary),
    /// Downloaded data is the same as the existing file; no update needed.
    AlreadyCurrent(EopSummary),
}

/// Download the latest finals2000A.data to a destination directory.
///
/// The file is first written to `finals2000A.data.new` in `dest_dir`,
/// then validated. If the downloaded data's last MJD matches the existing
/// file's last MJD, the download is discarded and [`UpdateResult::AlreadyCurrent`]
/// is returned. Otherwise, the existing file (if any) is renamed to
/// `finals2000A.data.bak` and the new file takes its place.
///
/// # Errors
///
/// Returns an error if the download fails, the data is invalid, or
/// filesystem operations fail.
pub fn download_and_install(dest_dir: &Path) -> Result<UpdateResult, EopError> {
    fs::create_dir_all(dest_dir)
        .map_err(|e| EopError::IoError(format!("creating {}: {e}", dest_dir.display())))?;

    let final_path = dest_dir.join("finals2000A.data");
    let new_path = dest_dir.join("finals2000A.data.new");
    let bak_path = dest_dir.join("finals2000A.data.bak");

    // Download
    let content = download_from_url(DEFAULT_URL).or_else(|_| download_from_url(IERS_URL))?;

    // Validate the downloaded content
    let table = match EopTable::from_finals2000a(&content) {
        Ok(t) => t,
        Err(e) => {
            return Err(EopError::ParseError(format!(
                "downloaded data failed validation: {e}"
            )));
        }
    };

    let summary = table.summary();

    // Require at least 1000 entries
    if summary.num_entries < 1000 {
        return Err(EopError::ParseError(format!(
            "downloaded data has only {} entries (expected >1000)",
            summary.num_entries
        )));
    }

    // Check if existing file already has the same data
    if final_path.exists() {
        if let Ok(existing) = EopTable::from_file(&final_path) {
            let (_, existing_end) = existing.mjd_range();
            if (existing_end - summary.mjd_end).abs() < 0.5 {
                return Ok(UpdateResult::AlreadyCurrent(summary));
            }
        }
    }

    // Write to .new
    fs::write(&new_path, &content)
        .map_err(|e| EopError::IoError(format!("writing {}: {e}", new_path.display())))?;

    // Safe swap
    if final_path.exists() {
        let _ = fs::remove_file(&bak_path); // remove old backup if present
        fs::rename(&final_path, &bak_path)
            .map_err(|e| EopError::IoError(format!("backing up old data: {e}")))?;
    }
    fs::rename(&new_path, &final_path)
        .map_err(|e| EopError::IoError(format!("installing new data: {e}")))?;

    Ok(UpdateResult::Updated(final_path, summary))
}

/// Download content from a URL as a string.
fn download_from_url(url: &str) -> Result<String, EopError> {
    let body = ureq::get(url)
        .call()
        .map_err(|e| EopError::IoError(format!("downloading {url}: {e}")))?
        .into_body()
        .read_to_string()
        .map_err(|e| EopError::IoError(format!("reading response from {url}: {e}")))?;
    Ok(body)
}

/// Validate an existing finals2000A.data file.
///
/// Parses the file and returns a summary if valid.
pub fn validate(path: &Path) -> Result<EopSummary, EopError> {
    let table = EopTable::from_file(path)?;
    Ok(table.summary())
}
