// SPDX-License-Identifier: LGPL-3.0-or-later
//! Archive scan summaries built from disk logical records.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{ImportVlaOptions, VlaDiskReader, VlaError};

/// Summary for a single archive file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ArchiveFileSummary {
    /// Source file.
    pub path: PathBuf,
    /// Number of logical records in the file.
    pub logical_records: u64,
    /// Total bytes across all logical records.
    pub logical_bytes: u64,
    /// Range of archive revision values observed.
    pub revision_range: Option<(u16, u16)>,
    /// Range of archive observation-day values observed.
    pub obs_day_range: Option<(u32, u32)>,
    /// Maximum number of antennas encountered in a record.
    pub max_antennas: u16,
    /// Histogram of how many CDAs were present per record.
    pub used_cda_histogram: BTreeMap<usize, u64>,
}

/// Multi-file archive scan summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ArchiveSummary {
    /// Requested output MeasurementSet path, when supplied.
    pub vis: Option<PathBuf>,
    /// Per-file breakdown.
    pub files: Vec<ArchiveFileSummary>,
    /// Aggregate logical-record count.
    pub logical_records: u64,
    /// Aggregate logical bytes.
    pub logical_bytes: u64,
}

/// Scan a set of disk archive files.
pub fn scan_disk_archive_files(paths: &[PathBuf]) -> Result<ArchiveSummary, VlaError> {
    if paths.is_empty() {
        return Err(VlaError::NoArchiveFiles);
    }
    let mut summaries = Vec::with_capacity(paths.len());
    let mut logical_records = 0_u64;
    let mut logical_bytes = 0_u64;
    for path in paths {
        let file_summary = scan_one(path)?;
        logical_records += file_summary.logical_records;
        logical_bytes += file_summary.logical_bytes;
        summaries.push(file_summary);
    }
    Ok(ArchiveSummary {
        vis: None,
        files: summaries,
        logical_records,
        logical_bytes,
    })
}

/// Scan the disk archive files referenced by task-style options.
pub fn scan_disk_archive_files_from_options(
    options: &ImportVlaOptions,
) -> Result<ArchiveSummary, VlaError> {
    let mut summary = scan_disk_archive_files(options.require_archivefiles()?)?;
    summary.vis = options.vis.clone();
    Ok(summary)
}

fn scan_one(path: &Path) -> Result<ArchiveFileSummary, VlaError> {
    let mut reader = VlaDiskReader::open(path)?;
    let mut logical_records = 0_u64;
    let mut logical_bytes = 0_u64;
    let mut revision_min = None::<u16>;
    let mut revision_max = None::<u16>;
    let mut day_min = None::<u32>;
    let mut day_max = None::<u32>;
    let mut max_antennas = 0_u16;
    let mut used_cda_histogram = BTreeMap::<usize, u64>::new();

    while let Some(record) = reader.next_record()? {
        logical_records += 1;
        logical_bytes += record.bytes().len() as u64;
        let rca = record.rca();
        let revision = rca
            .revision()
            .map_err(|message| VlaError::invalid_archive(path, message))?;
        let obs_day = rca
            .obs_day()
            .map_err(|message| VlaError::invalid_archive(path, message))?;
        let n_antennas = rca
            .n_antennas()
            .map_err(|message| VlaError::invalid_archive(path, message))?;
        let used_cdas = rca
            .used_cda_count()
            .map_err(|message| VlaError::invalid_archive(path, message))?;

        revision_min = Some(revision_min.map_or(revision, |min| min.min(revision)));
        revision_max = Some(revision_max.map_or(revision, |max| max.max(revision)));
        day_min = Some(day_min.map_or(obs_day, |min| min.min(obs_day)));
        day_max = Some(day_max.map_or(obs_day, |max| max.max(obs_day)));
        max_antennas = max_antennas.max(n_antennas);
        *used_cda_histogram.entry(used_cdas).or_insert(0) += 1;
    }

    Ok(ArchiveFileSummary {
        path: path.to_path_buf(),
        logical_records,
        logical_bytes,
        revision_range: revision_min.zip(revision_max),
        obs_day_range: day_min.zip(day_max),
        max_antennas,
        used_cda_histogram,
    })
}
