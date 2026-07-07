// SPDX-License-Identifier: LGPL-3.0-or-later
//! Frozen-oracle manifest and seam-trace types for imaging parity work.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use flate2::Compression;
use flate2::GzBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Schema version for JSON trace bundles emitted by the imaging frontend.
pub const ORACLE_SCHEMA_VERSION: u32 = 2;

/// Source-of-truth domain used to validate a bundle or artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TruthDomain {
    /// Numeric parity against CASA/casacore imaging behavior.
    CasaImaging,
    /// Metadata, serialization, and `image.open()` parity against CASA tables.
    CasaImageTables,
}

/// Dataset tier for frozen-oracle scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatasetTier {
    /// Fast default path.
    TierA,
    /// Slow gated path.
    TierB,
    /// Manual or stress-only path.
    TierC,
}

/// Tolerance class applied to one persisted artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToleranceClass {
    /// Discrete artifacts that must match exactly.
    Exact,
    /// Extremely tight geometric quantities such as UVW and phasors.
    Geometry,
    /// Intermediate floating-point arrays.
    IntermediateFloat,
    /// Final image-domain products.
    FinalImage,
    /// `sumwt`-style normalization artifacts.
    Sumwt,
}

/// Wire format of one persisted artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactFormat {
    /// Pretty-printed JSON.
    Json,
    /// Gzip-compressed JSON with deterministic header fields.
    JsonGzip,
    /// Stable array payload such as `.npy` or `.npz`.
    Array,
}

/// Manifest entry for one persisted oracle artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OracleArtifactManifest {
    /// Human-readable artifact label.
    pub name: String,
    /// Truth domain that governs this artifact.
    pub truth_domain: TruthDomain,
    /// Tolerance class used when comparing this artifact.
    pub tolerance: ToleranceClass,
    /// Relative path to the persisted artifact.
    pub relative_path: String,
    /// Wire format of the artifact.
    pub format: ArtifactFormat,
    /// Optional SHA256 of the artifact contents when a frozen bundle is pinned.
    pub sha256: Option<String>,
    /// Optional free-form notes for reviewers.
    pub notes: Option<String>,
}

/// Top-level manifest for one frozen-oracle bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OracleBundleManifest {
    /// Bundle schema version.
    pub schema_version: u32,
    /// Dataset path used to generate the bundle.
    pub dataset_path: String,
    /// Stable identity for the staged dataset copy used to generate the bundle.
    pub dataset_identity: Option<String>,
    /// Optional SHA256 of a canonical dataset manifest or tree digest.
    pub dataset_sha256: Option<String>,
    /// Dataset tier used for scheduling this bundle.
    pub dataset_tier: DatasetTier,
    /// Optional CASA version string recorded by the generator.
    pub casa_version: Option<String>,
    /// Optional casacore version string recorded by the generator.
    pub casacore_version: Option<String>,
    /// Fixed run parameters used to generate the bundle.
    pub parameter_manifest: BTreeMap<String, String>,
    /// Persisted artifacts that belong to this bundle.
    pub artifacts: Vec<OracleArtifactManifest>,
}

/// Optional manifest overrides supplied by a higher-level freezing workflow.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OracleBundleOverrides {
    /// Override the dataset path recorded in the bundle manifest.
    pub dataset_path: Option<String>,
    /// Override the stable dataset identity recorded in the bundle manifest.
    pub dataset_identity: Option<String>,
    /// Override the dataset SHA256 recorded in the bundle manifest.
    pub dataset_sha256: Option<String>,
    /// CASA runtime version string for the generator, when available.
    pub casa_version: Option<String>,
    /// casacore runtime version string for the generator, when available.
    pub casacore_version: Option<String>,
}

/// Resolved phase-center metadata for one prepared trace.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhaseCenterTrace {
    /// Field id used as the imaging phase center when the target center is an
    /// existing FIELD row.
    pub field_id: Option<usize>,
    /// Direction reference label, for example `J2000`.
    pub reference: String,
    /// `[longitude, latitude]` phase-center angles in radians.
    pub angles_rad: [f64; 2],
}

/// One selected MAIN row in stable oracle order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectedRowTrace {
    /// Zero-based MAIN row index.
    pub row_index: usize,
    /// FIELD_ID selected for this row.
    pub field_id: usize,
    /// DATA_DESC_ID selected for this row.
    pub ddid: usize,
    /// SPECTRAL_WINDOW row resolved from the DDID.
    pub spw_id: usize,
    /// POLARIZATION row resolved from the DDID.
    pub polarization_id: usize,
    /// Row timestamp in MJD seconds when required by the preparation path.
    pub time_mjd_seconds: Option<f64>,
}

/// One selected source channel retained by the spectral preparation seam.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreparedSourceChannelTrace {
    /// Slot within the selected source-channel arrays.
    pub source_channel_slot: usize,
    /// Original SPW channel index used for this slot.
    pub source_channel_index: usize,
    /// Source-channel center frequency in Hz.
    pub frequency_hz: f64,
    /// Source-channel width in Hz.
    pub width_hz: f64,
}

/// One resolved output cube channel emitted by the spectral preparation seam.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreparedOutputChannelTrace {
    /// Zero-based output-channel index.
    pub output_channel_index: usize,
    /// Output-channel center frequency in Hz.
    pub frequency_hz: f64,
}

/// Frozen-oracle trace for the spectral-axis portion of prepared visibility
/// generation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreparedSpectralAxisTrace {
    /// Requested spectral mode.
    pub spectral_mode: String,
    /// Selected source channels after SPW/channel filtering.
    pub source_channels: Vec<PreparedSourceChannelTrace>,
    /// Output cube-channel centers in the final imaging frame.
    pub output_channels: Vec<PreparedOutputChannelTrace>,
}

/// One row-level geometric preparation result prior to spectral interpolation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreparedGeometryRowTrace {
    /// Zero-based MAIN row index.
    pub row_index: usize,
    /// FIELD_ID native to the source row.
    pub input_field_id: usize,
    /// FIELD_ID used as the imaging phase center when the target center is an
    /// existing FIELD row.
    pub phase_center_field_id: Option<usize>,
    /// DATA_DESC_ID selected for this row.
    pub ddid: usize,
    /// SPECTRAL_WINDOW row resolved from the DDID.
    pub spw_id: usize,
    /// POLARIZATION row resolved from the DDID.
    pub polarization_id: usize,
    /// Optional POINTING_ID value when available in the MAIN table.
    pub pointing_id: Option<i32>,
    /// POINTING row used for ANTENNA1 when a concrete row matched the row time.
    pub antenna1_pointing_row: Option<usize>,
    /// J2000 pointing direction used for ANTENNA1 in radians.
    pub antenna1_pointing_direction_rad: [f64; 2],
    /// Whether ANTENNA1 fell back to the row FIELD phase center instead of a
    /// concrete POINTING row.
    pub antenna1_pointing_used_fallback: bool,
    /// POINTING row used for ANTENNA2 when a concrete row matched the row time.
    pub antenna2_pointing_row: Option<usize>,
    /// J2000 pointing direction used for ANTENNA2 in radians.
    pub antenna2_pointing_direction_rad: [f64; 2],
    /// Whether ANTENNA2 fell back to the row FIELD phase center instead of a
    /// concrete POINTING row.
    pub antenna2_pointing_used_fallback: bool,
    /// ANTENNA1 id for the source row.
    pub antenna1_id: i32,
    /// ANTENNA2 id for the source row.
    pub antenna2_id: i32,
    /// Whether the row is cross-correlation data.
    pub is_cross: bool,
    /// Raw row UVW in meters from the source MeasurementSet.
    pub raw_uvw_m: [f64; 3],
    /// GridFT/CASA density-grid UVW in meters for the selected imaging phase
    /// center.
    pub gridft_density_uvw_m: [f64; 3],
    /// Reprojected imaging UVW in meters for the chosen phase center.
    pub imaging_uvw_m: [f64; 3],
    /// Frequency-scaled visibility phase-shift distance in meters.
    pub phase_shift_m: f64,
    /// FIELD.PHASE_DIR for the source row resolved into J2000 radians.
    pub field_phase_center_direction_rad: [f64; 2],
}

/// Weight source chosen for one prepared sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WeightSourceKind {
    /// Weight came from the row-wise `WEIGHT` column.
    Weight,
    /// Weight came from the per-channel `WEIGHT_SPECTRUM` column.
    WeightSpectrum,
    /// Final weight was derived from a mixture of sources.
    Mixed,
}

/// One source-channel contribution to a prepared output sample.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChannelContributionTrace {
    /// Slot within the selected source-channel arrays.
    pub source_channel_slot: usize,
    /// Original SPW channel index used for this contribution.
    pub source_channel_index: usize,
    /// Source-channel frequency in Hz.
    pub source_frequency_hz: f64,
    /// Contribution factor applied to this source channel.
    pub factor: f32,
}

/// One final prepared scalar visibility sample emitted by the frontend.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreparedVisibilitySampleTrace {
    /// MAIN row that produced this prepared sample.
    pub row_index: usize,
    /// FIELD_ID native to the source row.
    pub input_field_id: usize,
    /// FIELD_ID used as the imaging phase center when the target center is an
    /// existing FIELD row.
    pub phase_center_field_id: Option<usize>,
    /// DDID resolved for the source row.
    pub ddid: usize,
    /// SPW id resolved from the DDID.
    pub spw_id: usize,
    /// POLARIZATION id resolved from the DDID.
    pub polarization_id: usize,
    /// ANTENNA1 id for the source row.
    pub antenna1_id: i32,
    /// ANTENNA2 id for the source row.
    pub antenna2_id: i32,
    /// Whether the sample is cross-correlation data.
    pub is_cross: bool,
    /// Raw row UVW in meters.
    pub raw_uvw_m: [f64; 3],
    /// Imaging UVW after reprojection in meters.
    pub imaging_uvw_m: [f64; 3],
    /// Visibility-domain phase shift in meters.
    pub phase_shift_m: f64,
    /// Correlation indices used to derive this sample.
    pub correlation_indices: Vec<usize>,
    /// Output cube channel index when a cube mode is active.
    pub output_channel_index: Option<usize>,
    /// Output sample frequency in Hz.
    pub output_frequency_hz: f64,
    /// FIELD.PHASE_DIR for the source row resolved into J2000 radians.
    pub field_phase_center_direction_rad: [f64; 2],
    /// Resolved antenna-1 pointing direction `[ra, dec]` in radians.
    pub pointing_direction_rad: [f64; 2],
    /// Prepared scalar visibility real part.
    pub visibility_re: f32,
    /// Prepared scalar visibility imaginary part.
    pub visibility_im: f32,
    /// Final prepared sample weight.
    pub weight: f32,
    /// Source of the final weight value.
    pub weight_source: WeightSourceKind,
    /// Final `sumwt` factor attached to this sample.
    pub sumwt_factor: f32,
    /// Whether the sample is eligible for final gridding.
    pub gridable: bool,
    /// Source-channel contributions that formed this sample.
    pub source_contributions: Vec<ChannelContributionTrace>,
}

/// Reason a paired-hand trace entry did not collapse into a final scalar sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparedSampleRejectionReason {
    /// One or more contributing correlations were flagged.
    FlaggedCorrelation,
    /// One or more contributing weights were non-finite or non-positive.
    NonPositiveWeight,
    /// One or more contributing visibilities were non-finite.
    NonFiniteVisibility,
}

/// One paired-hand sample that was rejected during scalar Stokes-I collapse.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RejectedPreparedVisibilitySampleTrace {
    /// MAIN row that produced this rejected sample.
    pub row_index: usize,
    /// FIELD_ID native to the source row.
    pub input_field_id: usize,
    /// FIELD_ID used as the imaging phase center when the target center is an
    /// existing FIELD row.
    pub phase_center_field_id: Option<usize>,
    /// DDID resolved for the source row.
    pub ddid: usize,
    /// SPW id resolved from the DDID.
    pub spw_id: usize,
    /// POLARIZATION id resolved from the DDID.
    pub polarization_id: usize,
    /// ANTENNA1 id for the source row.
    pub antenna1_id: i32,
    /// ANTENNA2 id for the source row.
    pub antenna2_id: i32,
    /// Whether the sample is cross-correlation data.
    pub is_cross: bool,
    /// Raw row UVW in meters.
    pub raw_uvw_m: [f64; 3],
    /// Imaging UVW after reprojection in meters.
    pub imaging_uvw_m: [f64; 3],
    /// Visibility-domain phase shift in meters.
    pub phase_shift_m: f64,
    /// Correlation indices that were collapsed.
    pub correlation_indices: Vec<usize>,
    /// Output cube channel index when a cube mode is active.
    pub output_channel_index: Option<usize>,
    /// Output sample frequency in Hz.
    pub output_frequency_hz: f64,
    /// FIELD.PHASE_DIR for the source row resolved into J2000 radians.
    pub field_phase_center_direction_rad: [f64; 2],
    /// Resolved antenna-1 pointing direction `[ra, dec]` in radians.
    pub pointing_direction_rad: [f64; 2],
    /// First correlation weight.
    pub first_weight: f32,
    /// Second correlation weight.
    pub second_weight: f32,
    /// Source of the first weight.
    pub first_weight_source: WeightSourceKind,
    /// Source of the second weight.
    pub second_weight_source: WeightSourceKind,
    /// Whether the first correlation was flagged.
    pub first_flagged: bool,
    /// Whether the second correlation was flagged.
    pub second_flagged: bool,
    /// Source-channel contributions that formed this rejected sample.
    pub source_contributions: Vec<ChannelContributionTrace>,
    /// Canonical rejection reason.
    pub rejection_reason: PreparedSampleRejectionReason,
}

/// Frozen-oracle trace for the current `prepare_plane_input()` seam.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreparedVisibilityTraceBundle {
    /// Bundle schema version.
    pub schema_version: u32,
    /// MeasurementSet path used to create this trace.
    pub ms_path: String,
    /// Selected data column.
    pub data_column: String,
    /// Requested spectral mode.
    pub spectral_mode: String,
    /// Resolved phase-center metadata.
    pub phase_center: PhaseCenterTrace,
    /// Selected source-channel indices.
    pub source_channel_indices: Vec<usize>,
    /// Selected source-channel frequencies in Hz.
    pub source_channel_frequencies_hz: Vec<f64>,
    /// Selected source-channel widths in Hz.
    pub source_channel_widths_hz: Vec<f64>,
    /// Output channel frequencies in Hz.
    pub output_channel_frequencies_hz: Vec<f64>,
    /// Selected MAIN rows in stable order.
    pub selected_rows: Vec<SelectedRowTrace>,
    /// Final prepared scalar visibility samples.
    pub samples: Vec<PreparedVisibilitySampleTrace>,
    /// Paired-hand samples that were rejected during scalar collapse.
    pub rejected_samples: Vec<RejectedPreparedVisibilitySampleTrace>,
}

/// Frozen-oracle trace for the row-level geometric preparation seam.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreparedGeometryTraceBundle {
    /// Bundle schema version.
    pub schema_version: u32,
    /// MeasurementSet path used to create this trace.
    pub ms_path: String,
    /// Resolved phase-center metadata.
    pub phase_center: PhaseCenterTrace,
    /// Selected MAIN rows in stable order.
    pub selected_rows: Vec<SelectedRowTrace>,
    /// Row-level geometric preparation results in the same stable order.
    pub rows: Vec<PreparedGeometryRowTrace>,
}

/// Canonical serialized reason for a rejected `wproject` sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WProjectSkipReasonTrace {
    /// Sample was marked non-gridable before `wproject` planning.
    NotGridable,
    /// Sample coordinates, visibility, weight, or `sumwt_factor` were invalid.
    InvalidInput,
    /// The planned support footprint would run outside the padded grid.
    OutsideGrid,
}

/// One kernel plane in a serialized `wproject` trace bundle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WProjectKernelTrace {
    /// Zero-based kernel plane index.
    pub plane_index: usize,
    /// Representative positive-`w` value for this kernel plane in wavelengths.
    pub w_lambda: f64,
    /// Kernel support radius in grid cells.
    pub support: usize,
    /// Integral of the normalized kernel over its sampled support.
    pub kernel_integral: f32,
}

/// One planned `wproject` sample in stable input order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WProjectSamplePlanTrace {
    /// Zero-based input batch index.
    pub batch_index: usize,
    /// Zero-based sample index within the input batch.
    pub sample_index: usize,
    /// Baseline `u` coordinate in wavelengths.
    pub u_lambda: f64,
    /// Baseline `v` coordinate in wavelengths.
    pub v_lambda: f64,
    /// Baseline `w` coordinate in wavelengths.
    pub w_lambda: f64,
    /// Final imaging weight attached to the sample.
    pub weight: f32,
    /// CASA-style logical multiplicity factor used for reported `sumwt`.
    pub sumwt_factor: f32,
    /// Selected convolution-function plane index.
    pub plane_index: usize,
    /// Grid-center x location for the sample plan.
    pub loc_x: isize,
    /// Grid-center y location for the sample plan.
    pub loc_y: isize,
    /// Sub-grid x offset in oversampled kernel coordinates.
    pub off_x: isize,
    /// Sub-grid y offset in oversampled kernel coordinates.
    pub off_y: isize,
    /// Whether the kernel is conjugated for positive-`w` samples.
    pub conjugate_kernel: bool,
    /// Sample-local normalization gathered from the chosen kernel support.
    pub normalization: f32,
    /// Kernel support radius in grid cells for the selected plane.
    pub support: usize,
}

/// One rejected `wproject` sample in stable input order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WProjectSkippedSampleTrace {
    /// Zero-based input batch index.
    pub batch_index: usize,
    /// Zero-based sample index within the input batch.
    pub sample_index: usize,
    /// Baseline `u` coordinate in wavelengths.
    pub u_lambda: f64,
    /// Baseline `v` coordinate in wavelengths.
    pub v_lambda: f64,
    /// Baseline `w` coordinate in wavelengths.
    pub w_lambda: f64,
    /// Final imaging weight attached to the sample before rejection.
    pub weight: f32,
    /// CASA-style logical multiplicity factor attached before rejection.
    pub sumwt_factor: f32,
    /// Canonical rejection reason.
    pub reason: WProjectSkipReasonTrace,
}

/// Frozen-oracle trace for the `wproject` CF/grid-planning seam.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WProjectTraceBundle {
    /// Bundle schema version.
    pub schema_version: u32,
    /// MeasurementSet path used to create this trace.
    pub ms_path: String,
    /// Requested spectral mode.
    pub spectral_mode: String,
    /// Zero-based output cube channel index for cube traces.
    pub channel_index: Option<usize>,
    /// Output channel center frequency in Hz for cube traces.
    pub channel_frequency_hz: Option<f64>,
    /// Optional explicit `wprojplanes` request from the caller.
    pub requested_plane_count: Option<usize>,
    /// Actual number of kernel planes in the current CF plan.
    pub plane_count: usize,
    /// Kernel oversampling factor.
    pub sampling: usize,
    /// CASA-style `w`-axis scale used to map samples to planes.
    pub w_scale: f64,
    /// Maximum absolute input `w` value seen during planning.
    pub max_abs_w_lambda: f64,
    /// One summary per kernel plane.
    pub kernels: Vec<WProjectKernelTrace>,
    /// One planned sample in stable input order.
    pub samples: Vec<WProjectSamplePlanTrace>,
    /// Samples rejected before gridding, with explicit reasons.
    pub skipped_samples: Vec<WProjectSkippedSampleTrace>,
    /// Sum of weighted-sample contributions used for normalization.
    pub normalization_sumwt: f32,
    /// CASA-style reported `sumwt` used for the persisted `.sumwt` product.
    pub reported_sumwt: f32,
    /// Number of samples that contributed to the final `wproject` grid plan.
    pub gridded_samples: usize,
}

/// Persist one serializable oracle value as pretty-printed JSON.
pub fn write_json_pretty<T: Serialize>(value: &T, path: &Path) -> Result<(), String> {
    let bytes = render_json_pretty(value, path)?;
    fs::write(path, bytes).map_err(|error| format!("write JSON {}: {error}", path.display()))
}

/// Persist one serializable oracle value as pretty-printed JSON and return its SHA256.
pub fn write_json_pretty_hashed<T: Serialize>(value: &T, path: &Path) -> Result<String, String> {
    let bytes = render_json_pretty(value, path)?;
    fs::write(path, &bytes).map_err(|error| format!("write JSON {}: {error}", path.display()))?;
    Ok(sha256_hex_bytes(&bytes))
}

/// Persist one serializable oracle value as deterministic gzip-compressed JSON
/// and return the SHA256 of the compressed bytes.
pub fn write_json_gzip_hashed<T: Serialize>(value: &T, path: &Path) -> Result<String, String> {
    let bytes = render_json_compact(value, path)?;
    let mut encoder = GzBuilder::new()
        .mtime(0)
        .write(Vec::new(), Compression::default());
    encoder
        .write_all(&bytes)
        .map_err(|error| format!("gzip JSON {}: {error}", path.display()))?;
    let compressed = encoder
        .finish()
        .map_err(|error| format!("finish gzip JSON {}: {error}", path.display()))?;
    fs::write(path, &compressed)
        .map_err(|error| format!("write gzip JSON {}: {error}", path.display()))?;
    Ok(sha256_hex_bytes(&compressed))
}

/// Return the SHA256 for arbitrary bytes as lowercase hexadecimal.
pub fn sha256_hex_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_lower(&hasher.finalize())
}

/// Return a stable SHA256 for a file or directory tree.
pub fn sha256_hex_path(path: &Path) -> Result<String, String> {
    let metadata =
        fs::metadata(path).map_err(|error| format!("stat path {}: {error}", path.display()))?;
    if metadata.is_file() {
        let bytes =
            fs::read(path).map_err(|error| format!("read file {}: {error}", path.display()))?;
        return Ok(sha256_hex_bytes(&bytes));
    }
    if !metadata.is_dir() {
        return Err(format!(
            "path {} must be a file or directory to compute SHA256",
            path.display()
        ));
    }
    let mut files = Vec::<PathBuf>::new();
    collect_files(path, path, &mut files)?;
    files.sort();
    let mut hasher = Sha256::new();
    hasher.update(b"casa-rs-oracle-tree-v1");
    for relative in files {
        let file_path = path.join(&relative);
        let relative_text = relative.to_string_lossy();
        hasher.update(relative_text.as_bytes());
        hasher.update([0]);
        let bytes = fs::read(&file_path)
            .map_err(|error| format!("read file {}: {error}", file_path.display()))?;
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(&bytes);
    }
    Ok(hex_lower(&hasher.finalize()))
}

fn render_json_pretty<T: Serialize>(value: &T, path: &Path) -> Result<Vec<u8>, String> {
    serde_json::to_vec_pretty(value)
        .map_err(|error| format!("serialize JSON {}: {error}", path.display()))
}

fn render_json_compact<T: Serialize>(value: &T, path: &Path) -> Result<Vec<u8>, String> {
    serde_json::to_vec(value).map_err(|error| format!("serialize JSON {}: {error}", path.display()))
}

fn collect_files(root: &Path, current: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries = fs::read_dir(current)
        .map_err(|error| format!("read directory {}: {error}", current.display()))?;
    for entry in entries {
        let entry = entry
            .map_err(|error| format!("read directory entry {}: {error}", current.display()))?;
        let path = entry.path();
        let metadata = entry
            .metadata()
            .map_err(|error| format!("stat path {}: {error}", path.display()))?;
        if metadata.is_dir() {
            collect_files(root, &path, files)?;
        } else if metadata.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|error| {
                    format!(
                        "strip root {} from {}: {error}",
                        root.display(),
                        path.display()
                    )
                })?
                .to_path_buf();
            files.push(relative);
        }
    }
    Ok(())
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut text, "{byte:02x}");
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

    #[test]
    fn directory_sha256_changes_when_contents_change() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("tree");
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join("a.txt"), b"alpha").unwrap();
        let first = sha256_hex_path(&path).unwrap();
        fs::write(path.join("a.txt"), b"beta").unwrap();
        let second = sha256_hex_path(&path).unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn gzip_json_hash_is_stable_across_writes() {
        let tmp = tempfile::tempdir().unwrap();
        let payload = vec!["alpha", "beta", "gamma"];
        let first_path = tmp.path().join("first.json.gz");
        let second_path = tmp.path().join("second.json.gz");
        let first = write_json_gzip_hashed(&payload, &first_path).unwrap();
        let second = write_json_gzip_hashed(&payload, &second_path).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            fs::read(first_path).unwrap(),
            fs::read(second_path).unwrap()
        );
    }

    #[test]
    fn pretty_json_and_file_hash_helpers_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let payload = OracleBundleManifest {
            schema_version: ORACLE_SCHEMA_VERSION,
            dataset_path: "demo.ms".to_string(),
            dataset_identity: Some("identity".to_string()),
            dataset_sha256: Some("abc123".to_string()),
            dataset_tier: DatasetTier::TierB,
            casa_version: Some("6.6".to_string()),
            casacore_version: Some("3.6".to_string()),
            parameter_manifest: BTreeMap::from([("specmode".to_string(), "cube".to_string())]),
            artifacts: vec![OracleArtifactManifest {
                name: "trace".to_string(),
                truth_domain: TruthDomain::CasaImaging,
                tolerance: ToleranceClass::IntermediateFloat,
                relative_path: "trace.json".to_string(),
                format: ArtifactFormat::Json,
                sha256: None,
                notes: Some("note".to_string()),
            }],
        };
        let pretty_path = tmp.path().join("bundle.json");
        write_json_pretty(&payload, &pretty_path).unwrap();
        let pretty_text = fs::read_to_string(&pretty_path).unwrap();
        assert!(pretty_text.contains('\n'));

        let hashed_path = tmp.path().join("bundle-hashed.json");
        let expected_hash = write_json_pretty_hashed(&payload, &hashed_path).unwrap();
        let hashed_bytes = fs::read(&hashed_path).unwrap();
        assert_eq!(sha256_hex_bytes(&hashed_bytes), expected_hash);
        assert_eq!(sha256_hex_path(&hashed_path).unwrap(), expected_hash);
        let bytes_path = tmp.path().join("bytes.txt");
        fs::write(&bytes_path, b"abc").unwrap();
        assert_eq!(
            sha256_hex_bytes(b"abc"),
            sha256_hex_path(&bytes_path).unwrap()
        );
    }

    #[test]
    fn directory_hash_recurses_and_special_files_error_cleanly() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("tree");
        let nested = root.join("nested");
        fs::create_dir_all(&nested).unwrap();
        fs::write(root.join("b.txt"), b"beta").unwrap();
        fs::write(nested.join("a.txt"), b"alpha").unwrap();

        let first = sha256_hex_path(&root).unwrap();
        fs::rename(root.join("b.txt"), root.join("c.txt")).unwrap();
        let second = sha256_hex_path(&root).unwrap();
        assert_ne!(first, second);

        let socket_path = tmp.path().join("oracle.sock");
        if let Ok(_listener) = UnixListener::bind(&socket_path) {
            assert!(
                sha256_hex_path(&socket_path)
                    .unwrap_err()
                    .contains("must be a file or directory")
            );
        }
    }

    #[test]
    fn private_serialization_and_file_collection_helpers_are_exercised_directly() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("tree");
        let nested = root.join("nested");
        fs::create_dir_all(&nested).unwrap();
        fs::write(root.join("z.json"), b"{}").unwrap();
        fs::write(nested.join("a.json"), b"[]").unwrap();

        let compact =
            render_json_compact(&vec!["alpha", "beta"], &root.join("payload.json")).unwrap();
        assert_eq!(String::from_utf8(compact).unwrap(), "[\"alpha\",\"beta\"]");

        let mut files = Vec::new();
        collect_files(&root, &root, &mut files).unwrap();
        files.sort();
        assert_eq!(
            files,
            vec![PathBuf::from("nested/a.json"), PathBuf::from("z.json")]
        );

        assert_eq!(hex_lower(&[0x00, 0xab, 0xff]), "00abff");
    }

    #[test]
    fn collect_files_reports_paths_outside_root() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let outside = tmp.path().join("outside");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("payload.json"), b"{}").unwrap();

        let err = collect_files(&root, &outside, &mut Vec::new()).unwrap_err();
        assert!(err.contains("strip root"));
        assert!(err.contains(root.to_string_lossy().as_ref()));
        assert!(err.contains(outside.join("payload.json").to_string_lossy().as_ref()));
    }
}
