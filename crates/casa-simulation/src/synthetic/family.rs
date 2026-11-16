// SPDX-License-Identifier: LGPL-3.0-or-later
//! Developer request planning; execution belongs to the native Simulator.
use super::spec::*;
use super::task::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
};
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FamilyResult {
    /// Active source-model kind.
    pub source_model_kind: String,
    /// Requested target size in bytes.
    pub target_bytes: u64,
    /// Actual generated size in bytes from `actual_ms_bytes_source`.
    pub actual_ms_bytes: u64,
    /// Source used for `actual_ms_bytes`.
    pub actual_ms_bytes_source: String,
    /// Estimated MAIN row count needed to approach the target.
    pub estimated_main_rows: u64,
    /// Estimated DATA+FLAG payload bytes represented by the row budget.
    pub estimated_visibility_payload_bytes: u64,
    /// Requested MS channel count.
    pub requested_ms_channels: usize,
    /// Requested image channel count.
    pub requested_image_channels: usize,
    /// Requested pointing count.
    pub requested_pointing_count: usize,
    /// Requested exact field phase centers, when supplied by an oracle harness.
    #[serde(default)]
    pub requested_field_phase_centers_rad: Option<Vec<[f64; 2]>>,
    /// MAIN rows actually generated.
    pub generated_main_rows: u64,
    /// Generated MeasurementSet path.
    pub output_ms: PathBuf,
    /// Persisted manifest path.
    pub manifest_path: PathBuf,
    /// Concrete run result for the generated family member.
    pub run_result: Box<RunResult>,
    /// Worker selection policy for generated runs.
    pub worker_policy: WorkerPolicy,
    /// Observation row topology for generated runs.
    #[serde(default)]
    pub observation_mode: ObservationMode,
    /// Source antenna index for total-power family runs.
    #[serde(default)]
    pub total_power_antenna_index: Option<usize>,
    /// Optional row worker count.
    #[serde(default)]
    pub row_workers: Option<usize>,
    /// Optional channel worker count.
    #[serde(default)]
    pub channel_workers: Option<usize>,
}

#[derive(Debug, Clone)]
pub(super) struct FamilyPreset {
    pub(super) telescope_name: String,
    pub(super) field_name: String,
    pub(super) antennas: Vec<Antenna>,
    pub(super) array_config_source: String,
    pub(super) phase_center_rad: [f64; 2],
    pub(super) start_time_mjd_seconds: f64,
    pub(super) start_frequency_hz: f64,
    pub(super) channel_width_hz: f64,
    pub(super) dish_diameter_m: f64,
    pub(super) polarization_basis: PolarizationBasis,
}

pub(super) fn resolve_family_preset(request: &FamilyRequest) -> Result<FamilyPreset, String> {
    let telescope = request.telescope.trim().to_ascii_lowercase();
    let array_config = request.array_config.trim().to_ascii_lowercase();
    let start_frequency_hz = resolve_band_frequency_hz(&telescope, &request.band)?;
    let phase_center_rad = [4.712_391_234_768_306, -0.401_423_788_703_971_4];
    let start_time_mjd_seconds = 4_895_229_577.784_943;

    if telescope == "vla" {
        let (antennas, array_config_source) = match array_config.as_str() {
            "a" | "vla.a" | "vla.a.cfg" => {
                load_real_family_config("VLA", "vla.a.cfg", &request.data_roots)?.unwrap_or_else(
                    || {
                        (
                            tutorial_vla_a_antennas(),
                            "embedded CASA vla.a.cfg coordinates".to_string(),
                        )
                    },
                )
            }
            "b" | "vla.b" | "vla.b.cfg" => {
                require_real_family_config("VLA", "vla.b.cfg", &request.data_roots)?
            }
            "c" | "vla.c" | "vla.c.cfg" => {
                require_real_family_config("VLA", "vla.c.cfg", &request.data_roots)?
            }
            "d" | "vla.d" | "vla.d.cfg" => {
                require_real_family_config("VLA", "vla.d.cfg", &request.data_roots)?
            }
            "synthetic-vla-b" | "synthetic-b" => (
                scaled_vla_antennas(0.35),
                "synthetic scaled VLA A".to_string(),
            ),
            "synthetic-vla-c" | "synthetic-c" => (
                scaled_vla_antennas(0.11),
                "synthetic scaled VLA A".to_string(),
            ),
            "synthetic-vla-d" | "synthetic-d" => (
                scaled_vla_antennas(0.035),
                "synthetic scaled VLA A".to_string(),
            ),
            _ => match load_real_family_config(
                "VLA",
                request.array_config.trim(),
                &request.data_roots,
            )? {
                Some(loaded) => loaded,
                None => {
                    return Err(format!(
                        "unsupported VLA array_config {:?}; expected A, B, C, D, a CASA .cfg path/name, or synthetic-vla-b/c/d",
                        request.array_config
                    ));
                }
            },
        };
        return Ok(FamilyPreset {
            telescope_name: "VLA".to_string(),
            field_name: "synthetic-vla".to_string(),
            antennas,
            array_config_source,
            phase_center_rad,
            start_time_mjd_seconds,
            start_frequency_hz,
            channel_width_hz: 1.0e6,
            dish_diameter_m: 25.0,
            polarization_basis: PolarizationBasis::Circular,
        });
    }

    if telescope == "alma" || telescope == "aca" {
        let (antennas, array_config_source, aca_only, simalma) = match array_config.as_str() {
            "aca" | "aca.cycle10" | "aca.cycle10.cfg" | "aca.cycle10.named.cfg"
                if telescope == "aca" =>
            {
                let loaded =
                    load_real_family_config("ACA", "aca.cycle10.cfg", &request.data_roots)?.or(
                        load_real_family_config(
                            "ACA",
                            "aca.cycle10.named.cfg",
                            &request.data_roots,
                        )?,
                    );
                match loaded {
                    Some((antennas, source)) => (antennas, source, true, false),
                    None => {
                        return Err(real_config_not_found_message("ACA", "aca.cycle10.cfg"));
                    }
                }
            }
            "alma.cycle8.5.cfg" | "alma.cycle10.5" | "alma.cycle10.5.cfg" => {
                let config_name = if array_config == "alma.cycle8.5.cfg" {
                    "alma.cycle8.5.cfg"
                } else {
                    "alma.cycle10.5.cfg"
                };
                let (antennas, source) =
                    require_real_family_config("ALMA", config_name, &request.data_roots)?;
                (antennas, source, false, false)
            }
            "synthetic-alma-compact" | "synthetic-12m" | "synthetic-compact" => (
                synthetic_alma_antennas("DA", 16, 12.0, 450.0),
                "synthetic ALMA 12m compact".to_string(),
                false,
                false,
            ),
            "synthetic-aca" => (
                synthetic_alma_antennas("CM", 10, 7.0, 120.0),
                "synthetic ACA 7m compact".to_string(),
                true,
                false,
            ),
            "synthetic-simalma" => {
                let mut antennas = synthetic_alma_antennas("DA", 12, 12.0, 450.0);
                antennas.extend(synthetic_alma_antennas("CM", 10, 7.0, 120.0));
                (
                    antennas,
                    "synthetic ALMA 12m plus ACA 7m".to_string(),
                    false,
                    true,
                )
            }
            _ => match load_real_family_config(
                &request.telescope,
                request.array_config.trim(),
                &request.data_roots,
            )? {
                Some((antennas, source)) => (
                    antennas,
                    source,
                    telescope == "aca" || array_config.contains("aca"),
                    false,
                ),
                None => {
                    return Err(format!(
                        "unsupported ALMA/ACA array_config {:?}; expected a CASA .cfg path/name such as alma.cycle10.5.cfg or aca.cycle10.cfg, or synthetic-alma-compact/synthetic-aca/synthetic-simalma",
                        request.array_config
                    ));
                }
            },
        };
        let dish_diameter_m = if aca_only { 7.0 } else { 12.0 };
        return Ok(FamilyPreset {
            telescope_name: if aca_only {
                "ACA".to_string()
            } else {
                "ALMA".to_string()
            },
            field_name: if simalma {
                "synthetic-simalma".to_string()
            } else if aca_only {
                "synthetic-aca".to_string()
            } else {
                "synthetic-alma".to_string()
            },
            antennas,
            array_config_source,
            phase_center_rad,
            start_time_mjd_seconds,
            start_frequency_hz,
            channel_width_hz: 1.0e6,
            dish_diameter_m,
            polarization_basis: PolarizationBasis::Linear,
        });
    }

    Err(format!(
        "unsupported telescope {:?}; expected VLA, ALMA, or ACA",
        request.telescope
    ))
}

pub(super) fn require_real_family_config(
    telescope: &str,
    config_name: &str,
    roots: &[PathBuf],
) -> Result<(Vec<Antenna>, String), String> {
    load_real_family_config(telescope, config_name, roots)?
        .ok_or_else(|| real_config_not_found_message(telescope, config_name))
}

pub(super) fn real_config_not_found_message(telescope: &str, config_name: &str) -> String {
    format!(
        "real {telescope} array_config {config_name:?} was not found; provide a data_roots directory containing CASA simmos .cfg files, pass an explicit .cfg path, or use an explicit synthetic-* array_config"
    )
}

pub(super) fn load_real_family_config(
    telescope: &str,
    config_label: &str,
    roots: &[PathBuf],
) -> Result<Option<(Vec<Antenna>, String)>, String> {
    let label = config_label.trim();
    if label.is_empty() {
        return Ok(None);
    }
    let looks_like_cfg = label.ends_with(".cfg") || Path::new(label).is_file();
    if !looks_like_cfg {
        return Ok(None);
    }
    for candidate in family_config_candidates(label, roots) {
        if candidate.is_file() {
            let antennas = parse_casa_array_config(telescope, &candidate)?;
            return Ok(Some((antennas, candidate.display().to_string())));
        }
    }
    Ok(None)
}

pub(super) fn family_config_candidates(label: &str, roots: &[PathBuf]) -> Vec<PathBuf> {
    let path = Path::new(label);
    if path.is_absolute() || path.components().count() > 1 {
        return vec![path.to_path_buf()];
    }

    let mut candidates = vec![path.to_path_buf()];
    for root in roots {
        push_family_config_candidates(&mut candidates, root, label);
    }
    candidates
}

pub(super) fn push_family_config_candidates(
    candidates: &mut Vec<PathBuf>,
    root: &Path,
    label: &str,
) {
    candidates.push(root.join(label));
    candidates.push(root.join("alma").join("simmos").join(label));
    candidates.push(
        root.join("__data__")
            .join("alma")
            .join("simmos")
            .join(label),
    );
}

pub(super) fn parse_casa_array_config(
    _telescope: &str,
    path: &Path,
) -> Result<Vec<Antenna>, String> {
    let measures = casa_ms::open_measures_runtime().map_err(|e| e.to_string())?;
    let config = crate::readantenna::read_antenna_config(path, measures.as_ref())
        .map_err(|e| e.to_string())?;
    Ok((0..config.x.len())
        .map(|i| Antenna {
            name: config.antenna_names[i].clone(),
            station: config.pad_names[i].clone(),
            position_m: [config.x[i], config.y[i], config.z[i]],
            dish_diameter_m: config.dish_diameter[i],
        })
        .collect())
}

pub(super) fn resolve_band_frequency_hz(telescope: &str, band: &str) -> Result<f64, String> {
    let band = band.trim().to_ascii_lowercase().replace(' ', "");
    match (telescope, band.as_str()) {
        ("vla", "l") => Ok(1.5e9),
        ("vla", "s") => Ok(3.0e9),
        ("vla", "c") => Ok(6.0e9),
        ("vla", "x") => Ok(10.0e9),
        ("vla", "ku") => Ok(15.0e9),
        ("vla", "k") => Ok(22.0e9),
        ("vla", "ka") => Ok(33.0e9),
        ("vla", "q") => Ok(44.0e9),
        ("alma" | "aca", "3" | "band3" | "band_3") => Ok(100.0e9),
        ("alma" | "aca", "6" | "band6" | "band_6") => Ok(230.0e9),
        ("alma" | "aca", "7" | "band7" | "band_7") => Ok(345.0e9),
        ("alma" | "aca", "9" | "band9" | "band_9") => Ok(690.0e9),
        ("vla", other) => Err(format!(
            "unsupported VLA band {other:?}; expected L, S, C, X, Ku, K, Ka, or Q"
        )),
        (_, other) => Err(format!(
            "unsupported ALMA/ACA band {other:?}; expected Band 3, 6, 7, or 9"
        )),
    }
}

pub(super) fn family_channel_width_hz(imaging_mode: &str, preset: &FamilyPreset) -> f64 {
    let mode = imaging_mode.trim().to_ascii_lowercase();
    if matches!(
        mode.as_str(),
        "mfs" | "continuum" | "continuum_mfs" | "single_field" | "mt_mfs" | "mtmfs"
    ) {
        preset.channel_width_hz.max(32.0e6)
    } else {
        preset.channel_width_hz
    }
}

pub(super) fn family_row_pair_count(
    antenna_count: usize,
    observation_mode: ObservationMode,
) -> usize {
    match observation_mode {
        ObservationMode::Interferometric => antenna_count * antenna_count.saturating_sub(1) / 2,
        ObservationMode::TotalPower => antenna_count,
    }
    .max(1)
}

pub(super) fn family_antennas_for_mode(
    antennas: Vec<Antenna>,
    observation_mode: ObservationMode,
    total_power_antenna_index: Option<usize>,
) -> Result<Vec<Antenna>, String> {
    match observation_mode {
        ObservationMode::Interferometric => Ok(antennas),
        ObservationMode::TotalPower => {
            let selected_index = total_power_antenna_index.unwrap_or(0);
            antennas
                .into_iter()
                .nth(selected_index)
                .map(|antenna| vec![antenna])
                .ok_or_else(|| {
                    format!(
                        "total_power_antenna_index {selected_index} is out of range for array_config"
                    )
                })
        }
    }
}

pub(super) fn validate_family_imaging_mode(imaging_mode: &str) -> Result<(), String> {
    let mode = imaging_mode.trim().to_ascii_lowercase();
    if matches!(
        mode.as_str(),
        "single_field"
            | "mfs"
            | "continuum"
            | "continuum_mfs"
            | "mosaic"
            | "mosaic_mfs"
            | "spectral_cube"
            | "cube"
            | "cubedata"
            | "mt_mfs"
            | "mtmfs"
            | "simalma"
            | "aca"
    ) {
        return Ok(());
    }
    Err(format!(
        "unsupported imaging_mode {imaging_mode:?}; expected single_field, mfs, mosaic, spectral_cube, cubedata, mt_mfs, simalma, or aca"
    ))
}

pub(super) fn scaled_vla_antennas(scale: f64) -> Vec<Antenna> {
    let antennas = tutorial_vla_a_antennas();
    if (scale - 1.0).abs() < f64::EPSILON {
        return antennas;
    }
    let center = antenna_centroid(&antennas);
    antennas
        .into_iter()
        .map(|antenna| Antenna {
            position_m: [
                center[0] + (antenna.position_m[0] - center[0]) * scale,
                center[1] + (antenna.position_m[1] - center[1]) * scale,
                center[2] + (antenna.position_m[2] - center[2]) * scale,
            ],
            ..antenna
        })
        .collect()
}

pub(super) fn antenna_centroid(antennas: &[Antenna]) -> [f64; 3] {
    let mut sum = [0.0; 3];
    for antenna in antennas {
        sum[0] += antenna.position_m[0];
        sum[1] += antenna.position_m[1];
        sum[2] += antenna.position_m[2];
    }
    let count = antennas.len().max(1) as f64;
    [sum[0] / count, sum[1] / count, sum[2] / count]
}

pub(super) fn synthetic_alma_antennas(
    name_prefix: &str,
    count: usize,
    dish_diameter_m: f64,
    max_radius_m: f64,
) -> Vec<Antenna> {
    let center = [-2_223_990.194, -5_440_045.461, -2_481_682.086];
    (0..count)
        .map(|index| {
            let radius = if count <= 1 {
                0.0
            } else {
                max_radius_m * ((index + 1) as f64 / count as f64).sqrt()
            };
            let angle = index as f64 * 2.399_963_229_728_653;
            let east_m = radius * angle.cos();
            let north_m = radius * angle.sin();
            let name = format!("{name_prefix}{:02}", index + 1);
            Antenna {
                name: name.clone(),
                station: name,
                position_m: offset_itrf_position(center, east_m, north_m),
                dish_diameter_m,
            }
        })
        .collect()
}

pub(super) fn offset_itrf_position(center: [f64; 3], east_m: f64, north_m: f64) -> [f64; 3] {
    offset_itrf_position_enu(center, east_m, north_m, 0.0)
}

pub(super) fn offset_itrf_position_enu(
    center: [f64; 3],
    east_m: f64,
    north_m: f64,
    up_m: f64,
) -> [f64; 3] {
    let up = normalized(center);
    let east = normalized([-center[1], center[0], 0.0]);
    let north = [
        up[1] * east[2] - up[2] * east[1],
        up[2] * east[0] - up[0] * east[2],
        up[0] * east[1] - up[1] * east[0],
    ];
    [
        center[0] + east[0] * east_m + north[0] * north_m + up[0] * up_m,
        center[1] + east[1] * east_m + north[1] * north_m + up[1] * up_m,
        center[2] + east[2] * east_m + north[2] * north_m + up[2] * up_m,
    ]
}

pub(super) fn normalized(vector: [f64; 3]) -> [f64; 3] {
    let norm = (vector[0] * vector[0] + vector[1] * vector[1] + vector[2] * vector[2]).sqrt();
    if norm == 0.0 {
        return [0.0, 0.0, 0.0];
    }
    [vector[0] / norm, vector[1] / norm, vector[2] / norm]
}

pub(super) fn family_fields(
    pointing_count: usize,
    phase_center_rad: [f64; 2],
    field_phase_centers_rad: Option<&[[f64; 2]]>,
    imaging_mode: &str,
    reference_frequency_hz: f64,
    dish_diameter_m: f64,
) -> Vec<Field> {
    if let Some(field_phase_centers_rad) = field_phase_centers_rad {
        if field_phase_centers_rad.len() == 1 {
            return Vec::new();
        }
        return field_phase_centers_rad
            .iter()
            .enumerate()
            .map(|(index, phase_center_rad)| Field {
                name: format!("P{:03}", index),
                phase_center_rad: *phase_center_rad,
            })
            .collect();
    }
    if pointing_count <= 1 || !mode_uses_mosaic(imaging_mode) {
        return Vec::new();
    }
    let wavelength_m = 299_792_458.0 / reference_frequency_hz;
    let primary_beam_fwhm_rad = 1.13 * wavelength_m / dish_diameter_m;
    let spacing_rad = 0.45 * primary_beam_fwhm_rad;
    mosaic_offsets(pointing_count, spacing_rad)
        .into_iter()
        .enumerate()
        .map(|(index, [l_rad, m_rad])| Field {
            name: format!("P{:03}", index),
            phase_center_rad: [
                phase_center_rad[0] + l_rad / phase_center_rad[1].cos().max(1.0e-6),
                phase_center_rad[1] + m_rad,
            ],
        })
        .collect()
}

pub(super) fn mode_uses_mosaic(imaging_mode: &str) -> bool {
    let mode = imaging_mode.trim().to_ascii_lowercase();
    matches!(
        mode.as_str(),
        "mosaic" | "mosaic_mfs" | "mosaic_cube" | "simalma" | "aca"
    )
}

pub(super) fn mosaic_offsets(pointing_count: usize, spacing_rad: f64) -> Vec<[f64; 2]> {
    let mut offsets = vec![[0.0, 0.0]];
    let mut ring = 1usize;
    while offsets.len() < pointing_count {
        let samples = ring * 6;
        for sample in 0..samples {
            if offsets.len() == pointing_count {
                break;
            }
            let angle = std::f64::consts::TAU * sample as f64 / samples as f64;
            offsets.push([
                ring as f64 * spacing_rad * angle.cos(),
                ring as f64 * spacing_rad * angle.sin(),
            ]);
        }
        ring += 1;
    }
    offsets
}

pub(super) fn family_manifest_path(output_ms: &Path) -> PathBuf {
    let mut path = output_ms.to_path_buf();
    path.set_extension("synthetic-family.json");
    path
}

pub(super) fn directory_size_bytes(path: &Path) -> std::io::Result<u64> {
    let metadata = fs::metadata(path)?;
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    let mut total = 0;
    for entry in fs::read_dir(path)? {
        total += directory_size_bytes(&entry?.path())?;
    }
    Ok(total)
}
