// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use casa_vla::{CdaId, CorrelatorMode, DirectionEpoch, FrequencyFrame, VlaDiskReader};

#[test]
#[ignore = "real-data inventory helper; run explicitly with --ignored --nocapture"]
fn inventory_real_archives() {
    let archives = configured_archives();
    if archives.is_empty() {
        eprintln!("skipping: no candidate VLA export archives found");
        return;
    }

    for archive in archives {
        let summary = summarize_archive(&archive).expect("summarize archive");
        println!("archive={}", summary.path.display());
        println!("  logical_records={}", summary.logical_records);
        println!(
            "  revision_range={}..{}",
            summary.revision_min, summary.revision_max
        );
        println!(
            "  obs_day_range={}..{}",
            summary.obs_day_min, summary.obs_day_max
        );
        println!("  observation_ids={}", summary.observation_ids.join(", "));
        println!("  observing_modes={}", summary.observing_modes.join(", "));
        println!("  direction_epochs={}", summary.direction_epochs.join(", "));
        println!("  correlator_modes={}", summary.correlator_modes.join(", "));
        println!("  unique_sources={}", summary.sources.len());
        println!("  unique_source_directions={}", summary.direction_count);
        println!("  sources={}", summary.sources.join(", "));
        println!(
            "  doppler_tracking={} rest_frames={} spectral_setups={}",
            summary.has_doppler_tracking,
            summary.rest_frames.join(", "),
            summary.spectral_setups.join(" | ")
        );
    }
}

#[derive(Debug)]
struct ArchiveInventory {
    path: PathBuf,
    logical_records: usize,
    revision_min: u16,
    revision_max: u16,
    obs_day_min: u32,
    obs_day_max: u32,
    observation_ids: Vec<String>,
    observing_modes: Vec<String>,
    direction_epochs: Vec<String>,
    correlator_modes: Vec<String>,
    sources: Vec<String>,
    direction_count: usize,
    has_doppler_tracking: bool,
    rest_frames: Vec<String>,
    spectral_setups: Vec<String>,
}

fn summarize_archive(path: &Path) -> Result<ArchiveInventory, Box<dyn std::error::Error>> {
    let mut reader = VlaDiskReader::open(path)?;
    let max_records = std::env::var("CASA_RS_IMPORTVLA_MAX_RECORDS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok());
    let mut logical_records = 0usize;
    let mut revision_min = u16::MAX;
    let mut revision_max = 0u16;
    let mut obs_day_min = u32::MAX;
    let mut obs_day_max = 0u32;
    let mut observation_ids = BTreeSet::new();
    let mut observing_modes = BTreeSet::new();
    let mut direction_epochs = BTreeSet::new();
    let mut correlator_modes = BTreeSet::new();
    let mut sources = BTreeSet::new();
    let mut directions = BTreeSet::new();
    let mut rest_frames = BTreeSet::new();
    let mut spectral_setups = BTreeSet::new();
    let mut has_doppler_tracking = false;

    while let Some(record) = reader.next_record()? {
        if max_records.is_some_and(|limit| logical_records >= limit) {
            break;
        }
        logical_records += 1;
        let rca = record.rca();
        revision_min = revision_min.min(rca.revision()?);
        revision_max = revision_max.max(rca.revision()?);
        obs_day_min = obs_day_min.min(rca.obs_day()?);
        obs_day_max = obs_day_max.max(rca.obs_day()?);

        let sda = record.sda()?;
        observation_ids.insert(normalize_blank(sda.observation_id()?));
        observing_modes.insert(normalize_blank(sda.observation_mode_description()?));
        direction_epochs.insert(format!("{:?}", sda.direction_epoch()?));

        let source_name = normalize_blank(sda.source_name()?);
        if !source_name.is_empty() {
            sources.insert(source_name);
        }
        let direction = sda.source_direction_radians()?;
        directions.insert(format!("{:.9}:{:.9}", direction[0], direction[1]));

        let corr_mode = sda.correlator_mode()?;
        correlator_modes.insert(format!("{corr_mode:?}"));

        for cda_id in [CdaId::Cda0, CdaId::Cda1, CdaId::Cda2, CdaId::Cda3] {
            if rca.cda_offset_bytes(cda_id.index())? == 0 {
                continue;
            }
            let cda = record.cda(cda_id)?;
            if !cda.is_valid() {
                continue;
            }
            if sda.n_polarizations(cda_id)? == 0 {
                continue;
            }

            let rest_frame = sda.rest_frame(cda_id)?;
            if sda.doppler_tracking(cda_id)? {
                has_doppler_tracking = true;
                rest_frames.insert(format!("{rest_frame:?}"));
            }

            let observed_frequency_ghz = sda.observed_frequency_hz(cda_id)? / 1.0e9;
            let channel_width_khz = sda.channel_width_hz(cda_id)? / 1.0e3;
            let total_bandwidth_mhz = sda.correlated_bandwidth_hz(cda_id)? / 1.0e6;
            spectral_setups.insert(format!(
                "{corr_mode:?}:{cda_id:?}:nchan={}:df={channel_width_khz:.3}kHz:bw={total_bandwidth_mhz:.6}MHz:f0={observed_frequency_ghz:.6}GHz:frame={rest_frame:?}:doppler={}",
                sda.n_channels(cda_id)?,
                sda.doppler_tracking(cda_id)?
            ));
        }
    }

    if logical_records == 0 {
        return Err("archive contains no logical records".into());
    }

    Ok(ArchiveInventory {
        path: path.to_path_buf(),
        logical_records,
        revision_min,
        revision_max,
        obs_day_min,
        obs_day_max,
        observation_ids: observation_ids.into_iter().collect(),
        observing_modes: observing_modes.into_iter().collect(),
        direction_epochs: direction_epochs.into_iter().collect(),
        correlator_modes: correlator_modes.into_iter().collect(),
        sources: sources.into_iter().collect(),
        direction_count: directions.len(),
        has_doppler_tracking,
        rest_frames: rest_frames.into_iter().collect(),
        spectral_setups: spectral_setups.into_iter().collect(),
    })
}

fn configured_archives() -> Vec<PathBuf> {
    let from_env = std::env::var("CASA_RS_IMPORTVLA_ARCHIVES")
        .ok()
        .map(|value| {
            value
                .split([',', ':'])
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(PathBuf::from)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if !from_env.is_empty() {
        return existing_paths(from_env);
    }

    let mut candidates = Vec::new();
    for root in [
        Path::new("/Volumes/home/casatestdata/unittest/importvla"),
        Path::new("/Volumes/home/casatestdata/other"),
        Path::new("/Users/brianglendenning/SoftwareProjects/casatestdata/unittest/importvla"),
        Path::new("/Users/brianglendenning/SoftwareProjects/casatestdata/other"),
    ] {
        candidates.extend(archives_in_dir(root));
    }
    candidates.sort();
    candidates.dedup();
    candidates
}

fn existing_paths(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut result = paths
        .into_iter()
        .filter(|path| path.exists() && path.is_file())
        .collect::<Vec<_>>();
    result.sort();
    result.dedup();
    result
}

fn archives_in_dir(root: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(root) else {
        return Vec::new();
    };
    let mut archives = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_file() && is_export_archive_name(path.file_name()))
        .collect::<Vec<_>>();
    archives.sort();
    archives
}

fn is_export_archive_name(name: Option<&OsStr>) -> bool {
    let Some(name) = name.and_then(OsStr::to_str) else {
        return false;
    };
    name.starts_with("AT166_")
        || name.ends_with(".exp")
        || name.ends_with(".xp1")
        || name.ends_with(".xp2")
        || name.ends_with(".xp3")
        || name.ends_with(".xp4")
        || name.ends_with(".xp5")
}

fn normalize_blank(value: String) -> String {
    value.trim().to_string()
}

#[allow(dead_code)]
fn _mode_label(mode: CorrelatorMode) -> &'static str {
    match mode {
        CorrelatorMode::Continuum => "continuum",
        CorrelatorMode::A
        | CorrelatorMode::B
        | CorrelatorMode::C
        | CorrelatorMode::D
        | CorrelatorMode::Ab
        | CorrelatorMode::Ac
        | CorrelatorMode::Ad
        | CorrelatorMode::Bc
        | CorrelatorMode::Bd
        | CorrelatorMode::Cd
        | CorrelatorMode::Abcd
        | CorrelatorMode::Pa
        | CorrelatorMode::Pb => "spectral-line",
        CorrelatorMode::Unknown => "unknown",
    }
}

#[allow(dead_code)]
fn _epoch_label(epoch: DirectionEpoch) -> &'static str {
    match epoch {
        DirectionEpoch::J2000 => "J2000",
        DirectionEpoch::B1950Vla => "B1950_VLA",
        DirectionEpoch::Apparent => "APP",
        DirectionEpoch::Unknown(_) => "UNKNOWN",
    }
}

#[allow(dead_code)]
fn _frame_label(frame: FrequencyFrame) -> &'static str {
    match frame {
        FrequencyFrame::Topocentric => "TOPO",
        FrequencyFrame::Geocentric => "GEO",
        FrequencyFrame::Barycentric => "BARY",
        FrequencyFrame::Lsrk => "LSRK",
    }
}
