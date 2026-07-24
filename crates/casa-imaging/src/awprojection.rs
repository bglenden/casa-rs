// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA AWProject convolution-function cache interoperability.
//!
//! CASA persists one complex `PagedImage` for every imaging convolution
//! function (`CFS_*.im`) and one for the corresponding weight convolution
//! function (`WTCFS_*.im`). This module indexes those image tables from
//! metadata without retaining their pixels, validates the paired scientific
//! key, and loads a requested pair on demand.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use casa_coordinates::{Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, StokesType};
use casa_images::PagedImage;
use casa_types::{RecordValue, ScalarValue, Value};
use ndarray::{Array2, ArrayD, Axis, Ix4};
use num_complex::Complex32;

use crate::ImagingError;

const IMAGING_PREFIX: &str = "CFS_";
const WEIGHT_PREFIX: &str = "WTCFS_";

/// Scientific lookup key for one CASA AWProject convolution-function cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AwConvolutionFunctionKey {
    /// Spectral-coordinate reference frequency in Hz.
    pub frequency_hz: f64,
    /// W-coordinate value represented by this cell, in wavelengths.
    pub w_value_lambda: f64,
    /// CASA Mueller-matrix element number.
    pub mueller_element: i32,
    /// Parallactic-angle bin in degrees.
    pub parallactic_angle_deg: f64,
}

impl AwConvolutionFunctionKey {
    fn validate(self, path: &Path) -> Result<Self, ImagingError> {
        if !(self.frequency_hz.is_finite() && self.frequency_hz > 0.0) {
            return Err(cache_error(path, "frequency must be finite and positive"));
        }
        if !self.w_value_lambda.is_finite() {
            return Err(cache_error(path, "W value must be finite"));
        }
        if !self.parallactic_angle_deg.is_finite() {
            return Err(cache_error(path, "parallactic angle must be finite"));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct StableKey {
    frequency_hz_bits: u64,
    w_value_lambda_bits: u64,
    mueller_element: i32,
    parallactic_angle_deg_bits: u64,
}

impl From<AwConvolutionFunctionKey> for StableKey {
    fn from(value: AwConvolutionFunctionKey) -> Self {
        Self {
            frequency_hz_bits: value.frequency_hz.to_bits(),
            w_value_lambda_bits: value.w_value_lambda.to_bits(),
            mueller_element: value.mueller_element,
            parallactic_angle_deg_bits: value.parallactic_angle_deg.to_bits(),
        }
    }
}

impl From<StableKey> for AwConvolutionFunctionKey {
    fn from(value: StableKey) -> Self {
        Self {
            frequency_hz: f64::from_bits(value.frequency_hz_bits),
            w_value_lambda: f64::from_bits(value.w_value_lambda_bits),
            mueller_element: value.mueller_element,
            parallactic_angle_deg: f64::from_bits(value.parallactic_angle_deg_bits),
        }
    }
}

/// Metadata required to interpret one oversampled complex CF image.
#[derive(Debug, Clone, PartialEq)]
pub struct AwConvolutionFunctionKernelMetadata {
    /// Filesystem path of the standard casacore image table.
    pub path: PathBuf,
    /// Two-dimensional stored pixel shape.
    pub shape: [usize; 2],
    /// Oversampling factor used by the visibility resampler.
    pub sampling: usize,
    /// X-axis convolution support in grid pixels.
    pub x_support: usize,
    /// Y-axis convolution support in grid pixels.
    pub y_support: usize,
    /// Exact affine UU/VV pixel-to-wavelength coordinate definition.
    pub uv_coordinate: AwConvolutionFunctionUvCoordinateIdentity,
    /// Telescope name recorded by CASA.
    pub telescope_name: String,
    /// CASA beam/band identifier.
    pub band_name: String,
    /// Antenna diameter in meters.
    pub diameter_m: f64,
    /// Wideband conjugate-beam frequency in Hz.
    pub conjugate_frequency_hz: f64,
    /// Conjugate polarization code.
    pub conjugate_polarization: i32,
    /// Correlation plane carried by the degenerate Stokes coordinate.
    pub polarization: StokesType,
    /// W-coordinate increment recorded by CASA.
    pub w_increment: f64,
    /// Whether CASA marked this CF as rotationally symmetric.
    pub rotationally_symmetric: bool,
}

/// Bit-exact affine coordinate identity for a CASA CF's UU/VV pixel axes.
///
/// Axis names and units are not repeated here because cache opening requires
/// exactly `UU`, `VV` in `lambda`. The remaining vectors and PC matrix fully
/// determine `world = crval + cdelt * PC * (pixel - crpix)`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AwConvolutionFunctionUvCoordinateIdentity {
    /// UU/VV reference values as IEEE-754 bit patterns.
    pub reference_value_bits: [u64; 2],
    /// UU/VV reference pixels as IEEE-754 bit patterns.
    pub reference_pixel_bits: [u64; 2],
    /// UU/VV increments as IEEE-754 bit patterns.
    pub increment_bits: [u64; 2],
    /// Row-major 2x2 PC matrix as IEEE-754 bit patterns.
    pub pc_matrix_bits: [[u64; 2]; 2],
}

/// Paired imaging and weight metadata for one scientific CF key.
#[derive(Debug, Clone, PartialEq)]
pub struct AwConvolutionFunctionEntryMetadata {
    /// Shared scientific lookup key.
    pub key: AwConvolutionFunctionKey,
    /// Imaging convolution-function metadata.
    pub imaging: AwConvolutionFunctionKernelMetadata,
    /// Weight convolution-function metadata.
    pub weight: AwConvolutionFunctionKernelMetadata,
}

/// On-demand paired imaging and weight CF pixels.
#[derive(Debug, Clone)]
pub struct AwConvolutionFunctionCell {
    /// Validated metadata for the pair.
    pub metadata: AwConvolutionFunctionEntryMetadata,
    /// Imaging convolution-function pixels indexed as `(x, y)`.
    pub imaging: Array2<Complex32>,
    /// Weight convolution-function pixels indexed as `(x, y)`.
    pub weight: Array2<Complex32>,
}

/// Summary of one validated CASA AWProject CF cache.
#[derive(Debug, Clone, PartialEq)]
pub struct AwConvolutionFunctionInventory {
    /// Number of paired imaging/weight cells.
    pub paired_cells: usize,
    /// Sorted unique frequencies in Hz.
    pub frequencies_hz: Vec<f64>,
    /// Sorted unique W values in wavelengths.
    pub w_values_lambda: Vec<f64>,
    /// Sorted unique Mueller elements.
    pub mueller_elements: Vec<i32>,
    /// Sorted unique parallactic-angle bins in degrees.
    pub parallactic_angles_deg: Vec<f64>,
}

/// Immutable identity of one validated read-only CASA CF cache.
///
/// The fingerprint covers every authoritative cell key and all paired kernel
/// metadata used by the resampler. Pixel arrays remain lazily loaded; malformed
/// or non-finite pixels fail when their cell first becomes resident.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwConvolutionFunctionCacheIdentity {
    /// Interoperability format understood by this reader.
    pub format: &'static str,
    /// Canonical cache root, preventing two path aliases from appearing distinct.
    pub source_root: PathBuf,
    /// Stable FNV-1a digest of authoritative paired-cell keys and metadata.
    pub metadata_fingerprint: u64,
    /// Number of validated paired imaging/weight cells.
    pub paired_cells: usize,
    /// Exact represented frequency coordinates as IEEE-754 bit patterns.
    pub frequency_hz_bits: Vec<u64>,
    /// Exact represented W coordinates as IEEE-754 bit patterns.
    pub w_value_lambda_bits: Vec<u64>,
    /// Exact Mueller elements represented by the cache.
    pub mueller_elements: Vec<i32>,
    /// Exact represented parallactic angles as IEEE-754 bit patterns.
    pub parallactic_angle_deg_bits: Vec<u64>,
    /// Telescope identities found in kernel metadata.
    pub telescope_names: Vec<String>,
    /// Receiver/band identities found in kernel metadata.
    pub band_names: Vec<String>,
    /// Antenna diameters as exact IEEE-754 bit patterns.
    pub diameter_m_bits: Vec<u64>,
    /// Conjugate frequencies as exact IEEE-754 bit patterns.
    pub conjugate_frequency_hz_bits: Vec<u64>,
    /// Conjugate-polarization codes.
    pub conjugate_polarizations: Vec<i32>,
    /// Correlation-plane FITS codes carried by the kernels.
    pub polarization_codes: Vec<i32>,
    /// W increments as exact IEEE-754 bit patterns.
    pub w_increment_bits: Vec<u64>,
    /// Distinct bit-exact UU/VV affine coordinate definitions.
    pub uv_coordinates: Vec<AwConvolutionFunctionUvCoordinateIdentity>,
    /// Distinct imaging-kernel shapes.
    pub imaging_shapes: Vec<[usize; 2]>,
    /// Distinct weight-kernel shapes.
    pub weight_shapes: Vec<[usize; 2]>,
    /// Distinct oversampling factors.
    pub sampling: Vec<usize>,
    /// Distinct imaging-kernel supports.
    pub imaging_supports: Vec<[usize; 2]>,
    /// Distinct weight-kernel supports.
    pub weight_supports: Vec<[usize; 2]>,
    /// Whether any cells are rotationally symmetric or asymmetric.
    pub rotational_symmetry: Vec<bool>,
    /// Stored pixel representation.
    pub pixel_type: &'static str,
}

impl AwConvolutionFunctionCacheIdentity {
    /// Stable concise identity for logs and receipts.
    pub fn log_line(&self) -> String {
        format!(
            "cf_format={} cf_source={} cf_metadata_key={:016x} cf_pairs={} cf_freqs={} cf_wplanes={} cf_mueller={:?} cf_pa_bins={} cf_telescopes={:?} cf_bands={:?} cf_uv_coordinate_definitions={} cf_imaging_shapes={:?} cf_weight_shapes={:?} cf_sampling={:?} cf_imaging_supports={:?} cf_weight_supports={:?} cf_pixel_type={}",
            self.format,
            self.source_root.display(),
            self.metadata_fingerprint,
            self.paired_cells,
            self.frequency_hz_bits.len(),
            self.w_value_lambda_bits.len(),
            self.mueller_elements,
            self.parallactic_angle_deg_bits.len(),
            self.telescope_names,
            self.band_names,
            self.uv_coordinates.len(),
            self.imaging_shapes,
            self.weight_shapes,
            self.sampling,
            self.imaging_supports,
            self.weight_supports,
            self.pixel_type,
        )
    }
}

/// Complete immutable science key for one AWProject execution plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwProjectPlanKey {
    /// Versioned casa-rs implementation semantics.
    pub implementation: &'static str,
    /// Validated source-cache identity.
    pub cache: AwConvolutionFunctionCacheIdentity,
    /// Output image shape.
    pub image_shape: [usize; 2],
    /// Output cell increments as exact IEEE-754 bit patterns.
    pub cell_size_rad_bits: [u64; 2],
    /// Direction projection name.
    pub projection: &'static str,
    /// Image phase center as exact IEEE-754 bit patterns.
    pub phase_center_direction_rad_bits: [u64; 2],
    /// Requested output Stokes/correlation plane.
    pub plane_stokes: &'static str,
    /// Selected data-frequency range as exact IEEE-754 bit patterns.
    pub selected_frequency_range_hz_bits: [u64; 2],
    /// MT-MFS reference frequency as an exact IEEE-754 bit pattern.
    pub reference_frequency_hz_bits: u64,
    /// Primary-beam/voltage-pattern model identity.
    pub primary_beam_model: String,
    /// PB cutoff as an exact IEEE-754 bit pattern.
    pub pb_limit_bits: u32,
    /// Requested W-plane count after cache resolution.
    pub w_plane_count: usize,
    /// Requested facet count.
    pub facets: usize,
    /// Optional distinct PSF phase center as exact IEEE-754 bit patterns.
    pub psf_phase_center_direction_rad_bits: Option<[u64; 2]>,
    /// Canonical voltage-pattern table identity when supplied.
    pub vp_table: Option<PathBuf>,
    /// A-term toggle.
    pub a_term: bool,
    /// Prolate-spheroidal-term toggle.
    pub ps_term: bool,
    /// Wideband A-projection toggle.
    pub wb_awp: bool,
    /// Conjugate-beam lookup toggle.
    pub conjugate_beams: bool,
    /// PA computation step as an exact IEEE-754 bit pattern.
    pub compute_pa_step_deg_bits: u64,
    /// PA rotation step as an exact IEEE-754 bit pattern.
    pub rotate_pa_step_deg_bits: u64,
    /// Pointing-offset sigmas as exact IEEE-754 bit patterns.
    pub pointing_offset_sigdev_bits: Vec<u64>,
    /// Whether POINTING-table directions are part of the plan.
    pub use_pointing: bool,
    /// Mosaic weight-density policy.
    pub mosaic_weighting: bool,
    /// Image-domain normalization policy.
    pub normalization: &'static str,
    /// Numeric representation used by CFs, accumulators, and output products.
    pub precision: &'static str,
}

impl AwProjectPlanKey {
    /// Stable detailed line for run logs and evidence receipts.
    pub fn log_line(&self) -> String {
        format!(
            "awproject_plan implementation={} projection={} image_shape={}x{} cell_bits={:016x},{:016x} phase_center_bits={:016x},{:016x} stokes={} selected_frequency_bits={:016x},{:016x} reffreq_bits={:016x} primary_beam={} pblimit_bits={:08x} wplanes={} facets={} psf_phase_center={:?} vptable={} aterm={} psterm={} wbawp={} conjbeams={} computepastep_bits={:016x} rotatepastep_bits={:016x} pointing_sigma_bits={:?} usepointing={} mosweight={} normtype={} precision={} {}",
            self.implementation,
            self.projection,
            self.image_shape[0],
            self.image_shape[1],
            self.cell_size_rad_bits[0],
            self.cell_size_rad_bits[1],
            self.phase_center_direction_rad_bits[0],
            self.phase_center_direction_rad_bits[1],
            self.plane_stokes,
            self.selected_frequency_range_hz_bits[0],
            self.selected_frequency_range_hz_bits[1],
            self.reference_frequency_hz_bits,
            self.primary_beam_model,
            self.pb_limit_bits,
            self.w_plane_count,
            self.facets,
            self.psf_phase_center_direction_rad_bits,
            self.vp_table
                .as_ref()
                .map_or_else(|| "default".to_string(), |path| path.display().to_string()),
            self.a_term,
            self.ps_term,
            self.wb_awp,
            self.conjugate_beams,
            self.compute_pa_step_deg_bits,
            self.rotate_pa_step_deg_bits,
            self.pointing_offset_sigdev_bits,
            self.use_pointing,
            self.mosaic_weighting,
            self.normalization,
            self.precision,
            self.cache.log_line(),
        )
    }
}

/// Measured AWProject sample acceptance and rejection counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AwProjectSampleStats {
    /// Valid, gridable samples presented to the AWProject path plus samples
    /// rejected before convolution-function placement.
    pub attempted_samples: usize,
    /// Samples that contributed to PSF, residual, and weight grids.
    pub accepted_samples: usize,
    /// Samples rejected by the shared selection/weighting preparation.
    pub rejected_not_gridable: usize,
    /// Samples rejected for non-finite frequency, weight, or visibility data.
    pub rejected_invalid_input: usize,
    /// Samples whose RR imaging CF could not be placed.
    pub rejected_rr_imaging_plan: usize,
    /// Samples whose LL imaging CF could not be placed.
    pub rejected_ll_imaging_plan: usize,
    /// Samples whose RR PSF/WTCF could not be placed.
    pub rejected_rr_psf_plan: usize,
    /// Samples whose LL PSF/WTCF could not be placed.
    pub rejected_ll_psf_plan: usize,
    /// Placement rejections caused by non-finite UVW coordinates.
    pub rejected_nonfinite_coordinate: usize,
    /// Placement rejections caused by convolution support crossing the grid.
    pub rejected_outside_grid: usize,
    /// Placement rejections caused by a CF pixel index outside its cell.
    pub rejected_kernel_index: usize,
    /// Placement rejections caused by non-finite or zero CF normalization.
    pub rejected_invalid_normalization: usize,
}

/// Cache identity, sample census, and measured residency counters returned by
/// an AWProject run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwProjectRunDiagnostics {
    /// Immutable execution key.
    pub plan_key: AwProjectPlanKey,
    /// Maximum paired-CF pixel bytes permitted to remain resident.
    pub resident_budget_bytes: usize,
    /// On-demand load, hit, eviction, and resident-byte counters.
    pub resident: AwConvolutionFunctionResidentStats,
    /// Sample acceptance and exact rejection-reason counters.
    pub samples: AwProjectSampleStats,
}

#[derive(Debug, Clone)]
struct PartialEntry {
    key: AwConvolutionFunctionKey,
    imaging: Option<AwConvolutionFunctionKernelMetadata>,
    weight: Option<AwConvolutionFunctionKernelMetadata>,
}

/// Read-only index of a CASA AWProject convolution-function cache.
///
/// Opening the cache reads image-table metadata only. [`Self::load`] reads the
/// two selected pixel arrays and does not retain them in the index, giving the
/// caller explicit control of bounded CF residency.
#[derive(Debug, Clone)]
pub struct AwConvolutionFunctionCache {
    root: Arc<PathBuf>,
    entries: Arc<BTreeMap<StableKey, AwConvolutionFunctionEntryMetadata>>,
    inventory: AwConvolutionFunctionInventory,
    identity: AwConvolutionFunctionCacheIdentity,
}

#[derive(Debug)]
struct ResidentCell {
    cell: Arc<AwConvolutionFunctionCell>,
    bytes: usize,
    last_used: u64,
}

#[derive(Debug, Default)]
struct ResidentState {
    cells: BTreeMap<StableKey, ResidentCell>,
    resident_bytes: usize,
    clock: u64,
    loads: u64,
    hits: u64,
    evictions: u64,
}

/// Observable state of a bounded AWProject CF resident cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AwConvolutionFunctionResidentStats {
    /// Number of paired imaging/weight cells currently resident.
    pub resident_cells: usize,
    /// Pixel bytes currently retained by the cache.
    pub resident_bytes: usize,
    /// Number of on-disk paired-cell loads completed by this cache.
    pub loads: u64,
    /// Number of requests satisfied from resident memory.
    pub hits: u64,
    /// Number of resident cells evicted to remain under the byte budget.
    pub evictions: u64,
}

/// Deterministic, byte-bounded LRU over a read-only CASA AWProject CF cache.
///
/// A cell larger than the configured budget is returned to the caller but is
/// not retained. Concurrent misses may perform redundant reads, but insertion
/// remains deterministic and the resident set never exceeds `byte_budget`.
#[derive(Debug)]
pub struct AwConvolutionFunctionResidentCache {
    cache: AwConvolutionFunctionCache,
    byte_budget: usize,
    state: Mutex<ResidentState>,
}

impl AwConvolutionFunctionResidentCache {
    /// Create bounded residency over an already validated metadata index.
    pub fn new(cache: AwConvolutionFunctionCache, byte_budget: usize) -> Self {
        Self {
            cache,
            byte_budget,
            state: Mutex::new(ResidentState::default()),
        }
    }

    /// Underlying validated, read-only cache index.
    pub fn cache(&self) -> &AwConvolutionFunctionCache {
        &self.cache
    }

    /// Maximum number of paired-CF pixel bytes retained by this cache.
    pub fn byte_budget(&self) -> usize {
        self.byte_budget
    }

    /// Load or reuse one exact paired CF cell.
    pub fn get(
        &self,
        key: AwConvolutionFunctionKey,
    ) -> Result<Arc<AwConvolutionFunctionCell>, ImagingError> {
        let stable = StableKey::from(key);
        {
            let mut state = self.lock_state()?;
            state.clock = state.clock.wrapping_add(1);
            let last_used = state.clock;
            if let Some(resident) = state.cells.get_mut(&stable) {
                resident.last_used = last_used;
                let cell = Arc::clone(&resident.cell);
                state.hits = state.hits.saturating_add(1);
                return Ok(cell);
            }
        }

        let loaded = Arc::new(self.cache.load(key)?);
        let bytes = paired_cell_pixel_bytes(&loaded);
        let mut state = self.lock_state()?;
        state.loads = state.loads.saturating_add(1);
        state.clock = state.clock.wrapping_add(1);
        let last_used = state.clock;

        if let Some(resident) = state.cells.get_mut(&stable) {
            resident.last_used = last_used;
            let cell = Arc::clone(&resident.cell);
            state.hits = state.hits.saturating_add(1);
            return Ok(cell);
        }
        if bytes > self.byte_budget {
            return Ok(loaded);
        }

        while state.resident_bytes.saturating_add(bytes) > self.byte_budget {
            let Some(eviction_key) = state
                .cells
                .iter()
                .min_by_key(|(key, resident)| (resident.last_used, **key))
                .map(|(key, _)| *key)
            else {
                break;
            };
            if let Some(evicted) = state.cells.remove(&eviction_key) {
                state.resident_bytes = state.resident_bytes.saturating_sub(evicted.bytes);
                state.evictions = state.evictions.saturating_add(1);
            }
        }
        state.resident_bytes = state.resident_bytes.saturating_add(bytes);
        state.cells.insert(
            stable,
            ResidentCell {
                cell: Arc::clone(&loaded),
                bytes,
                last_used,
            },
        );
        Ok(loaded)
    }

    /// Current residency and I/O counters.
    pub fn stats(&self) -> Result<AwConvolutionFunctionResidentStats, ImagingError> {
        let state = self.lock_state()?;
        Ok(AwConvolutionFunctionResidentStats {
            resident_cells: state.cells.len(),
            resident_bytes: state.resident_bytes,
            loads: state.loads,
            hits: state.hits,
            evictions: state.evictions,
        })
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, ResidentState>, ImagingError> {
        self.state.lock().map_err(|_| {
            cache_error(
                self.cache.root(),
                "resident CF cache lock was poisoned by an earlier panic",
            )
        })
    }
}

impl AwConvolutionFunctionCache {
    /// Open and fully validate a CASA `CFS`/`WTCFS` cache index.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ImagingError> {
        let root = path.as_ref();
        let directory = fs::read_dir(root)
            .map_err(|error| cache_error(root, format!("cannot read cache directory: {error}")))?;
        let mut partial = BTreeMap::<StableKey, PartialEntry>::new();
        let mut recognized = 0usize;
        for entry in directory {
            let entry = entry.map_err(|error| {
                cache_error(root, format!("cannot enumerate cache directory: {error}"))
            })?;
            let file_type = entry.file_type().map_err(|error| {
                cache_error(
                    &entry.path(),
                    format!("cannot inspect cache entry: {error}"),
                )
            })?;
            if !file_type.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let kind = if name.starts_with(WEIGHT_PREFIX) && name.ends_with(".im") {
                CacheCellKind::Weight
            } else if name.starts_with(IMAGING_PREFIX) && name.ends_with(".im") {
                CacheCellKind::Imaging
            } else {
                continue;
            };
            recognized += 1;
            let (key, metadata) = read_kernel_metadata(&entry.path())?;
            let stable = StableKey::from(key);
            let slot = partial.entry(stable).or_insert_with(|| PartialEntry {
                key,
                imaging: None,
                weight: None,
            });
            if StableKey::from(slot.key) != stable {
                return Err(cache_error(
                    &entry.path(),
                    "two non-identical scientific keys produced the same stable key",
                ));
            }
            let target = match kind {
                CacheCellKind::Imaging => &mut slot.imaging,
                CacheCellKind::Weight => &mut slot.weight,
            };
            if let Some(previous) = target.as_ref() {
                return Err(cache_error(
                    &entry.path(),
                    format!(
                        "duplicate {} cell for key {:?}; first path is {}",
                        kind.label(),
                        key,
                        previous.path.display()
                    ),
                ));
            }
            *target = Some(metadata);
        }
        if recognized == 0 {
            return Err(cache_error(root, "no CFS_*.im or WTCFS_*.im entries found"));
        }

        let mut entries = BTreeMap::new();
        for (stable, pair) in partial {
            let imaging = pair.imaging.ok_or_else(|| {
                cache_error(
                    root,
                    format!("missing imaging CFS cell for key {:?}", pair.key),
                )
            })?;
            let weight = pair.weight.ok_or_else(|| {
                cache_error(
                    root,
                    format!("missing weight WTCFS cell for key {:?}", pair.key),
                )
            })?;
            validate_pair(&pair.key, &imaging, &weight)?;
            entries.insert(
                stable,
                AwConvolutionFunctionEntryMetadata {
                    key: pair.key,
                    imaging,
                    weight,
                },
            );
        }
        validate_cache_axes(root, &entries)?;
        let inventory = inventory_from_entries(&entries);
        let identity = identity_from_entries(root, &entries, &inventory);
        Ok(Self {
            root: Arc::new(root.to_path_buf()),
            entries: Arc::new(entries),
            inventory,
            identity,
        })
    }

    /// Root directory of this read-only cache.
    pub fn root(&self) -> &Path {
        self.root.as_ref()
    }

    /// Validated cache inventory.
    pub fn inventory(&self) -> &AwConvolutionFunctionInventory {
        &self.inventory
    }

    /// Immutable cache source and scientific metadata identity.
    pub fn identity(&self) -> &AwConvolutionFunctionCacheIdentity {
        &self.identity
    }

    /// Return all paired scientific keys in deterministic order.
    pub fn keys(&self) -> Vec<AwConvolutionFunctionKey> {
        self.entries.keys().copied().map(Into::into).collect()
    }

    /// Return metadata for an exact scientific key.
    pub fn metadata(
        &self,
        key: AwConvolutionFunctionKey,
    ) -> Option<&AwConvolutionFunctionEntryMetadata> {
        self.entries.get(&StableKey::from(key))
    }

    /// Select the nearest cache cell for a requested frequency, W value,
    /// Mueller element, and parallactic angle.
    ///
    /// Mueller is exact. Frequency, W, and PA follow CASA's cache lookup model
    /// and choose the closest represented coordinate using deterministic key
    /// order to break ties.
    pub fn nearest_key(
        &self,
        frequency_hz: f64,
        w_value_lambda: f64,
        mueller_element: i32,
        parallactic_angle_deg: f64,
    ) -> Option<AwConvolutionFunctionKey> {
        if !(frequency_hz.is_finite()
            && frequency_hz > 0.0
            && w_value_lambda.is_finite()
            && parallactic_angle_deg.is_finite())
        {
            return None;
        }
        self.entries
            .values()
            .filter(|entry| entry.key.mueller_element == mueller_element)
            .min_by(|left, right| {
                nearest_distance(
                    left.key,
                    frequency_hz,
                    w_value_lambda,
                    parallactic_angle_deg,
                )
                .total_cmp(&nearest_distance(
                    right.key,
                    frequency_hz,
                    w_value_lambda,
                    parallactic_angle_deg,
                ))
                .then_with(|| StableKey::from(left.key).cmp(&StableKey::from(right.key)))
            })
            .map(|entry| entry.key)
    }

    /// Select a cache cell with CASA `CFBuffer` axis semantics.
    ///
    /// Frequency and parallactic angle are selected independently. W uses
    /// CASA's quadratic plane index, `round(sqrt(WIncr * abs(w)))`, rather than
    /// a generic multidimensional distance. CASA also uses the conjugate
    /// Mueller row for non-positive W; in its 4-by-4 flattened Mueller
    /// convention that maps element `m` to `15 - m`. When `conjugate_beams`
    /// is true, CASA first maps the data frequency to
    /// `sqrt(2 * image_reference_frequency_hz^2 - sample_frequency_hz^2)` and
    /// then selects the nearest primary CF frequency. The persisted `ConjFreq`
    /// values describe individual cells, but are quantized and cannot be used
    /// as an inverse lookup for this operation.
    pub fn select_key_for_sample(
        &self,
        sample_frequency_hz: f64,
        image_reference_frequency_hz: f64,
        w_lambda: f64,
        mueller_element: i32,
        parallactic_angle_deg: f64,
        conjugate_beams: bool,
    ) -> Option<AwConvolutionFunctionKey> {
        if !(sample_frequency_hz.is_finite()
            && sample_frequency_hz > 0.0
            && image_reference_frequency_hz.is_finite()
            && image_reference_frequency_hz > 0.0
            && w_lambda.is_finite()
            && parallactic_angle_deg.is_finite())
        {
            return None;
        }
        let requested_cf_frequency_hz = if conjugate_beams {
            let radicand = 2.0 * image_reference_frequency_hz * image_reference_frequency_hz
                - sample_frequency_hz * sample_frequency_hz;
            if !(radicand.is_finite() && radicand > 0.0) {
                return None;
            }
            radicand.sqrt()
        } else {
            sample_frequency_hz
        };
        let selected_mueller_element = if w_lambda > 0.0 {
            mueller_element
        } else {
            15_i32.checked_sub(mueller_element)?
        };
        let candidates = self
            .entries
            .values()
            .filter(|entry| entry.key.mueller_element == selected_mueller_element)
            .collect::<Vec<_>>();
        let first = candidates.first()?;
        let w_increment = first.imaging.w_increment;
        let w_values = sorted_f64_bits(
            candidates
                .iter()
                .map(|entry| entry.key.w_value_lambda.to_bits())
                .collect(),
        );
        let w_index = if w_values.len() <= 1 || w_increment == 0.0 {
            0
        } else {
            (w_increment * w_lambda.abs())
                .sqrt()
                .round()
                .clamp(0.0, (w_values.len() - 1) as f64) as usize
        };
        let selected_w = w_values[w_index];
        candidates
            .into_iter()
            .filter(|entry| entry.key.w_value_lambda.to_bits() == selected_w.to_bits())
            .min_by(|left, right| {
                (left.key.frequency_hz - requested_cf_frequency_hz)
                    .abs()
                    .total_cmp(&(right.key.frequency_hz - requested_cf_frequency_hz).abs())
                    .then_with(|| {
                        circular_degrees(left.key.parallactic_angle_deg - parallactic_angle_deg)
                            .abs()
                            .total_cmp(
                                &circular_degrees(
                                    right.key.parallactic_angle_deg - parallactic_angle_deg,
                                )
                                .abs(),
                            )
                    })
                    .then_with(|| StableKey::from(left.key).cmp(&StableKey::from(right.key)))
            })
            .map(|entry| entry.key)
    }

    /// Load one exact paired CF cell without retaining its pixels in the cache.
    pub fn load(
        &self,
        key: AwConvolutionFunctionKey,
    ) -> Result<AwConvolutionFunctionCell, ImagingError> {
        let metadata = self
            .metadata(key)
            .cloned()
            .ok_or_else(|| cache_error(self.root(), format!("no exact CF cell for key {key:?}")))?;
        let imaging = read_kernel_pixels(&metadata.imaging)?;
        let weight = read_kernel_pixels(&metadata.weight)?;
        Ok(AwConvolutionFunctionCell {
            metadata,
            imaging,
            weight,
        })
    }
}

fn paired_cell_pixel_bytes(cell: &AwConvolutionFunctionCell) -> usize {
    cell.imaging
        .len()
        .saturating_add(cell.weight.len())
        .saturating_mul(std::mem::size_of::<Complex32>())
}

#[derive(Debug, Clone, Copy)]
enum CacheCellKind {
    Imaging,
    Weight,
}

impl CacheCellKind {
    fn label(self) -> &'static str {
        match self {
            Self::Imaging => "imaging",
            Self::Weight => "weight",
        }
    }
}

fn read_kernel_metadata(
    path: &Path,
) -> Result<
    (
        AwConvolutionFunctionKey,
        AwConvolutionFunctionKernelMetadata,
    ),
    ImagingError,
> {
    let image = PagedImage::<Complex32>::open(path)
        .map_err(|error| cache_error(path, format!("cannot open Complex32 image: {error}")))?;
    let shape = image.shape();
    if shape.len() != 4 || shape[2] != 1 || shape[3] != 1 || shape[0] == 0 || shape[1] == 0 {
        return Err(cache_error(
            path,
            format!("expected non-empty [nx, ny, 1, 1] shape, got {shape:?}"),
        ));
    }
    let coordinates = validate_coordinates(path, image.coordinates())?;
    let frequency_hz = spectral_reference_frequency(path, image.coordinates())?;
    let misc = image.misc_info();
    let key = AwConvolutionFunctionKey {
        frequency_hz,
        w_value_lambda: required_f64(&misc, "WValue", path)?,
        mueller_element: required_i32(&misc, "MuellerElement", path)?,
        parallactic_angle_deg: required_f64(&misc, "ParallacticAngle", path)?,
    }
    .validate(path)?;
    let sampling =
        positive_integral_usize(required_f64(&misc, "Sampling", path)?, "Sampling", path)?;
    let x_support = nonnegative_usize(required_i32(&misc, "Xsupport", path)?, "Xsupport", path)?;
    let y_support = nonnegative_usize(required_i32(&misc, "Ysupport", path)?, "Ysupport", path)?;
    if x_support == 0 || y_support == 0 {
        return Err(cache_error(path, "Xsupport and Ysupport must be positive"));
    }
    let metadata = AwConvolutionFunctionKernelMetadata {
        path: path.to_path_buf(),
        shape: [shape[0], shape[1]],
        sampling,
        x_support,
        y_support,
        uv_coordinate: coordinates.uv,
        telescope_name: required_string(&misc, "TelescopeName", path)?,
        band_name: required_string(&misc, "BandName", path)?,
        diameter_m: required_f64(&misc, "Diameter", path)?,
        conjugate_frequency_hz: required_f64(&misc, "ConjFreq", path)?,
        conjugate_polarization: required_i32(&misc, "ConjPoln", path)?,
        polarization: coordinates.polarization,
        w_increment: required_f64(&misc, "WIncr", path)?,
        rotationally_symmetric: required_bool(&misc, "OpCode", path)?,
    };
    validate_kernel_metadata(&metadata)?;
    Ok((key, metadata))
}

struct ValidatedCfCoordinates {
    uv: AwConvolutionFunctionUvCoordinateIdentity,
    polarization: StokesType,
}

fn validate_coordinates(
    path: &Path,
    coords: &CoordinateSystem,
) -> Result<ValidatedCfCoordinates, ImagingError> {
    if coords.n_pixel_axes() != 4 {
        return Err(cache_error(
            path,
            format!(
                "expected four CF coordinate axes, got {}",
                coords.n_pixel_axes()
            ),
        ));
    }
    let Some(linear_index) = coords.find_coordinate(CoordinateType::Linear) else {
        return Err(cache_error(path, "missing UV linear coordinate"));
    };
    let linear_model = coords.coordinate(linear_index);
    if linear_model.axis_names() != ["UU", "VV"]
        || linear_model.axis_units() != ["lambda", "lambda"]
    {
        return Err(cache_error(
            path,
            format!(
                "expected UU/VV axes in lambda, got {:?} in {:?}",
                linear_model.axis_names(),
                linear_model.axis_units()
            ),
        ));
    }
    let CoordinateModel::Linear(linear) = linear_model else {
        return Err(cache_error(
            path,
            "UV linear coordinate has the wrong model type",
        ));
    };
    if linear.n_pixel_axes() != 2 || linear.n_world_axes() != 2 {
        return Err(cache_error(
            path,
            format!(
                "expected a two-axis UU/VV coordinate, got {} pixel and {} world axes",
                linear.n_pixel_axes(),
                linear.n_world_axes()
            ),
        ));
    }
    let reference_value = two_f64(path, "UV reference value", linear.reference_value())?;
    let reference_pixel = two_f64(path, "UV reference pixel", linear.reference_pixel())?;
    let increment = two_f64(path, "UV increment", linear.increment())?;
    let pc = linear.pc_matrix();
    let uv_values = [
        reference_value[0],
        reference_value[1],
        reference_pixel[0],
        reference_pixel[1],
        increment[0],
        increment[1],
        pc[[0, 0]],
        pc[[0, 1]],
        pc[[1, 0]],
        pc[[1, 1]],
    ];
    if uv_values.iter().any(|value| !value.is_finite()) {
        return Err(cache_error(
            path,
            "UV affine coordinate contains a non-finite value",
        ));
    }
    if increment[0] == 0.0 || increment[1] == 0.0 {
        return Err(cache_error(
            path,
            "UV coordinate increments must be non-zero",
        ));
    }
    let determinant = pc[[0, 0]] * pc[[1, 1]] - pc[[0, 1]] * pc[[1, 0]];
    if !determinant.is_finite() || determinant == 0.0 {
        return Err(cache_error(
            path,
            "UV coordinate PC matrix must be invertible",
        ));
    }
    let uv = AwConvolutionFunctionUvCoordinateIdentity {
        reference_value_bits: reference_value.map(f64::to_bits),
        reference_pixel_bits: reference_pixel.map(f64::to_bits),
        increment_bits: increment.map(f64::to_bits),
        pc_matrix_bits: [
            [pc[[0, 0]].to_bits(), pc[[0, 1]].to_bits()],
            [pc[[1, 0]].to_bits(), pc[[1, 1]].to_bits()],
        ],
    };
    let stokes_index = coords
        .find_coordinate(CoordinateType::Stokes)
        .ok_or_else(|| cache_error(path, "missing Stokes coordinate"))?;
    let CoordinateModel::Stokes(stokes) = coords.coordinate(stokes_index) else {
        return Err(cache_error(
            path,
            "Stokes coordinate has the wrong model type",
        ));
    };
    let [polarization] = stokes.stokes() else {
        return Err(cache_error(
            path,
            format!(
                "expected one degenerate correlation coordinate, got {:?}",
                stokes.stokes()
            ),
        ));
    };
    if coords.find_coordinate(CoordinateType::Spectral).is_none() {
        return Err(cache_error(path, "missing spectral coordinate"));
    }
    Ok(ValidatedCfCoordinates {
        uv,
        polarization: *polarization,
    })
}

fn two_f64(path: &Path, label: &str, values: Vec<f64>) -> Result<[f64; 2], ImagingError> {
    values.try_into().map_err(|values: Vec<f64>| {
        cache_error(
            path,
            format!("{label} must have two values, got {}", values.len()),
        )
    })
}

fn spectral_reference_frequency(
    path: &Path,
    coords: &CoordinateSystem,
) -> Result<f64, ImagingError> {
    let index = coords
        .find_coordinate(CoordinateType::Spectral)
        .ok_or_else(|| cache_error(path, "missing spectral coordinate"))?;
    let values = coords.coordinate(index).reference_value();
    let frequency = values
        .first()
        .copied()
        .ok_or_else(|| cache_error(path, "spectral coordinate has no reference frequency"))?;
    if !(frequency.is_finite() && frequency > 0.0) {
        return Err(cache_error(
            path,
            format!("invalid spectral reference frequency {frequency}"),
        ));
    }
    Ok(frequency)
}

fn validate_kernel_metadata(
    metadata: &AwConvolutionFunctionKernelMetadata,
) -> Result<(), ImagingError> {
    let path = &metadata.path;
    if metadata.telescope_name.trim().is_empty() || metadata.band_name.trim().is_empty() {
        return Err(cache_error(
            path,
            "telescope and band names must be non-empty",
        ));
    }
    if !(metadata.diameter_m.is_finite() && metadata.diameter_m > 0.0) {
        return Err(cache_error(path, "diameter must be finite and positive"));
    }
    if !(metadata.conjugate_frequency_hz.is_finite() && metadata.conjugate_frequency_hz > 0.0) {
        return Err(cache_error(
            path,
            "conjugate frequency must be finite and positive",
        ));
    }
    if !(metadata.w_increment.is_finite() && metadata.w_increment >= 0.0) {
        return Err(cache_error(
            path,
            "W increment must be finite and non-negative",
        ));
    }
    let required_x = metadata
        .x_support
        .saturating_mul(metadata.sampling)
        .saturating_mul(2);
    let required_y = metadata
        .y_support
        .saturating_mul(metadata.sampling)
        .saturating_mul(2);
    if metadata.shape[0] < required_x || metadata.shape[1] < required_y {
        return Err(cache_error(
            path,
            format!(
                "shape {:?} cannot hold support {}x{} at sampling {}",
                metadata.shape, metadata.x_support, metadata.y_support, metadata.sampling
            ),
        ));
    }
    Ok(())
}

fn validate_pair(
    key: &AwConvolutionFunctionKey,
    imaging: &AwConvolutionFunctionKernelMetadata,
    weight: &AwConvolutionFunctionKernelMetadata,
) -> Result<(), ImagingError> {
    for (label, same) in [
        ("sampling", imaging.sampling == weight.sampling),
        ("telescope", imaging.telescope_name == weight.telescope_name),
        ("band", imaging.band_name == weight.band_name),
        (
            "diameter",
            imaging.diameter_m.to_bits() == weight.diameter_m.to_bits(),
        ),
        (
            "conjugate frequency",
            imaging.conjugate_frequency_hz.to_bits() == weight.conjugate_frequency_hz.to_bits(),
        ),
        (
            "conjugate polarization",
            imaging.conjugate_polarization == weight.conjugate_polarization,
        ),
        ("polarization", imaging.polarization == weight.polarization),
        (
            "W increment",
            imaging.w_increment.to_bits() == weight.w_increment.to_bits(),
        ),
        (
            "rotational symmetry",
            imaging.rotationally_symmetric == weight.rotationally_symmetric,
        ),
    ] {
        if !same {
            return Err(cache_error(
                &weight.path,
                format!(
                    "{label} does not match paired imaging cell {} for key {key:?}",
                    imaging.path.display()
                ),
            ));
        }
    }
    if imaging.uv_coordinate.reference_value_bits != weight.uv_coordinate.reference_value_bits
        || imaging.uv_coordinate.pc_matrix_bits != weight.uv_coordinate.pc_matrix_bits
        || !same_uv_world_window(imaging, weight)
    {
        return Err(cache_error(
            &weight.path,
            format!(
                "weight CF coordinate does not cover the same UV world window as paired imaging cell {} for key {key:?}: imaging_shape={:?} weight_shape={:?} imaging_support={}x{} weight_support={}x{} imaging_uv={:?} weight_uv={:?}",
                imaging.path.display(),
                imaging.shape,
                weight.shape,
                imaging.x_support,
                imaging.y_support,
                weight.x_support,
                weight.y_support,
                imaging.uv_coordinate,
                weight.uv_coordinate
            ),
        ));
    }
    Ok(())
}

fn same_uv_world_window(
    imaging: &AwConvolutionFunctionKernelMetadata,
    weight: &AwConvolutionFunctionKernelMetadata,
) -> bool {
    (0..2).all(|axis| {
        let imaging_size = imaging.shape[axis] as f64;
        let weight_size = weight.shape[axis] as f64;
        let imaging_reference_pixel =
            f64::from_bits(imaging.uv_coordinate.reference_pixel_bits[axis]);
        let weight_reference_pixel =
            f64::from_bits(weight.uv_coordinate.reference_pixel_bits[axis]);
        let imaging_increment = f64::from_bits(imaging.uv_coordinate.increment_bits[axis]);
        let weight_increment = f64::from_bits(weight.uv_coordinate.increment_bits[axis]);
        nearly_equal(
            imaging_reference_pixel / imaging_size,
            weight_reference_pixel / weight_size,
        ) && nearly_equal(
            imaging_increment * imaging_size,
            weight_increment * weight_size,
        )
    })
}

fn nearly_equal(left: f64, right: f64) -> bool {
    let scale = left.abs().max(right.abs()).max(1.0);
    (left - right).abs() <= scale * 32.0 * f64::EPSILON
}

fn validate_cache_axes(
    root: &Path,
    entries: &BTreeMap<StableKey, AwConvolutionFunctionEntryMetadata>,
) -> Result<(), ImagingError> {
    let first = entries
        .values()
        .next()
        .ok_or_else(|| cache_error(root, "validated cache has no paired cells"))?;
    let w_increment_bits = first.imaging.w_increment.to_bits();
    if entries
        .values()
        .any(|entry| entry.imaging.w_increment.to_bits() != w_increment_bits)
    {
        return Err(cache_error(
            root,
            "WIncr must be identical across one CF cache",
        ));
    }

    let inventory = inventory_from_entries(entries);
    let expected_cells = inventory
        .frequencies_hz
        .len()
        .checked_mul(inventory.w_values_lambda.len())
        .and_then(|count| count.checked_mul(inventory.mueller_elements.len()))
        .and_then(|count| count.checked_mul(inventory.parallactic_angles_deg.len()))
        .ok_or_else(|| cache_error(root, "CF-cache axis cardinality overflow"))?;
    if entries.len() != expected_cells {
        return Err(cache_error(
            root,
            format!(
                "CF cache is not a complete frequency x W x Mueller x PA product: found {} paired cells, expected {expected_cells}",
                entries.len()
            ),
        ));
    }

    let w_increment = first.imaging.w_increment;
    if inventory.w_values_lambda.len() > 1 && w_increment == 0.0 {
        return Err(cache_error(
            root,
            "multiple W planes require a positive WIncr",
        ));
    }
    if w_increment > 0.0 {
        for (index, &actual) in inventory.w_values_lambda.iter().enumerate() {
            let expected = (index * index) as f64 / w_increment;
            let tolerance = expected.abs().max(1.0) * 1.0e-10;
            if (actual - expected).abs() > tolerance {
                return Err(cache_error(
                    root,
                    format!(
                        "W plane {index} has value {actual}, expected CASA quadratic value {expected} from WIncr {w_increment}"
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn inventory_from_entries(
    entries: &BTreeMap<StableKey, AwConvolutionFunctionEntryMetadata>,
) -> AwConvolutionFunctionInventory {
    let frequencies = entries
        .values()
        .map(|entry| entry.key.frequency_hz.to_bits())
        .collect::<BTreeSet<_>>();
    let w_values = entries
        .values()
        .map(|entry| entry.key.w_value_lambda.to_bits())
        .collect::<BTreeSet<_>>();
    let parallactic_angles = entries
        .values()
        .map(|entry| entry.key.parallactic_angle_deg.to_bits())
        .collect::<BTreeSet<_>>();
    let mueller_elements = entries
        .values()
        .map(|entry| entry.key.mueller_element)
        .collect::<BTreeSet<_>>();
    AwConvolutionFunctionInventory {
        paired_cells: entries.len(),
        frequencies_hz: sorted_f64_bits(frequencies),
        w_values_lambda: sorted_f64_bits(w_values),
        mueller_elements: mueller_elements.into_iter().collect(),
        parallactic_angles_deg: sorted_f64_bits(parallactic_angles),
    }
}

fn identity_from_entries(
    root: &Path,
    entries: &BTreeMap<StableKey, AwConvolutionFunctionEntryMetadata>,
    inventory: &AwConvolutionFunctionInventory,
) -> AwConvolutionFunctionCacheIdentity {
    let mut fingerprint = StableFnv1a64::default();
    let mut telescope_names = BTreeSet::new();
    let mut band_names = BTreeSet::new();
    let mut diameter_m_bits = BTreeSet::new();
    let mut conjugate_frequency_hz_bits = BTreeSet::new();
    let mut conjugate_polarizations = BTreeSet::new();
    let mut polarization_codes = BTreeSet::new();
    let mut w_increment_bits = BTreeSet::new();
    let mut uv_coordinates = BTreeSet::new();
    let mut imaging_shapes = BTreeSet::new();
    let mut weight_shapes = BTreeSet::new();
    let mut sampling = BTreeSet::new();
    let mut imaging_supports = BTreeSet::new();
    let mut weight_supports = BTreeSet::new();
    let mut rotational_symmetry = BTreeSet::new();

    fingerprint.text("casa-cf-cache-pagedimage-v1");
    for (stable, entry) in entries {
        fingerprint.u64(stable.frequency_hz_bits);
        fingerprint.u64(stable.w_value_lambda_bits);
        fingerprint.i32(stable.mueller_element);
        fingerprint.u64(stable.parallactic_angle_deg_bits);
        fingerprint.kernel_metadata(root, &entry.imaging);
        fingerprint.kernel_metadata(root, &entry.weight);

        for metadata in [&entry.imaging, &entry.weight] {
            telescope_names.insert(metadata.telescope_name.clone());
            band_names.insert(metadata.band_name.clone());
            diameter_m_bits.insert(metadata.diameter_m.to_bits());
            conjugate_frequency_hz_bits.insert(metadata.conjugate_frequency_hz.to_bits());
            conjugate_polarizations.insert(metadata.conjugate_polarization);
            polarization_codes.insert(metadata.polarization.code());
            w_increment_bits.insert(metadata.w_increment.to_bits());
            uv_coordinates.insert(metadata.uv_coordinate.clone());
            sampling.insert(metadata.sampling);
            rotational_symmetry.insert(metadata.rotationally_symmetric);
        }
        imaging_shapes.insert(entry.imaging.shape);
        weight_shapes.insert(entry.weight.shape);
        imaging_supports.insert([entry.imaging.x_support, entry.imaging.y_support]);
        weight_supports.insert([entry.weight.x_support, entry.weight.y_support]);
    }

    AwConvolutionFunctionCacheIdentity {
        format: "casa-cf-cache-pagedimage-v1",
        source_root: fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf()),
        metadata_fingerprint: fingerprint.finish(),
        paired_cells: inventory.paired_cells,
        frequency_hz_bits: inventory
            .frequencies_hz
            .iter()
            .map(|value| value.to_bits())
            .collect(),
        w_value_lambda_bits: inventory
            .w_values_lambda
            .iter()
            .map(|value| value.to_bits())
            .collect(),
        mueller_elements: inventory.mueller_elements.clone(),
        parallactic_angle_deg_bits: inventory
            .parallactic_angles_deg
            .iter()
            .map(|value| value.to_bits())
            .collect(),
        telescope_names: telescope_names.into_iter().collect(),
        band_names: band_names.into_iter().collect(),
        diameter_m_bits: diameter_m_bits.into_iter().collect(),
        conjugate_frequency_hz_bits: conjugate_frequency_hz_bits.into_iter().collect(),
        conjugate_polarizations: conjugate_polarizations.into_iter().collect(),
        polarization_codes: polarization_codes.into_iter().collect(),
        w_increment_bits: w_increment_bits.into_iter().collect(),
        uv_coordinates: uv_coordinates.into_iter().collect(),
        imaging_shapes: imaging_shapes.into_iter().collect(),
        weight_shapes: weight_shapes.into_iter().collect(),
        sampling: sampling.into_iter().collect(),
        imaging_supports: imaging_supports.into_iter().collect(),
        weight_supports: weight_supports.into_iter().collect(),
        rotational_symmetry: rotational_symmetry.into_iter().collect(),
        pixel_type: "complex32",
    }
}

struct StableFnv1a64(u64);

impl Default for StableFnv1a64 {
    fn default() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
}

impl StableFnv1a64 {
    fn bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn text(&mut self, value: &str) {
        self.u64(value.len() as u64);
        self.bytes(value.as_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes(&value.to_le_bytes());
    }

    fn i32(&mut self, value: i32) {
        self.bytes(&value.to_le_bytes());
    }

    fn usize(&mut self, value: usize) {
        self.u64(value as u64);
    }

    fn kernel_metadata(&mut self, root: &Path, metadata: &AwConvolutionFunctionKernelMetadata) {
        let relative = metadata.path.strip_prefix(root).unwrap_or(&metadata.path);
        self.text(&relative.to_string_lossy());
        self.usize(metadata.shape[0]);
        self.usize(metadata.shape[1]);
        self.usize(metadata.sampling);
        self.usize(metadata.x_support);
        self.usize(metadata.y_support);
        for value in metadata
            .uv_coordinate
            .reference_value_bits
            .into_iter()
            .chain(metadata.uv_coordinate.reference_pixel_bits)
            .chain(metadata.uv_coordinate.increment_bits)
            .chain(metadata.uv_coordinate.pc_matrix_bits.into_iter().flatten())
        {
            self.u64(value);
        }
        self.text(&metadata.telescope_name);
        self.text(&metadata.band_name);
        self.u64(metadata.diameter_m.to_bits());
        self.u64(metadata.conjugate_frequency_hz.to_bits());
        self.i32(metadata.conjugate_polarization);
        self.i32(metadata.polarization.code());
        self.u64(metadata.w_increment.to_bits());
        self.bytes(&[u8::from(metadata.rotationally_symmetric)]);
    }

    fn finish(self) -> u64 {
        self.0
    }
}

fn sorted_f64_bits(values: BTreeSet<u64>) -> Vec<f64> {
    let mut values = values.into_iter().map(f64::from_bits).collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    values
}

fn read_kernel_pixels(
    metadata: &AwConvolutionFunctionKernelMetadata,
) -> Result<Array2<Complex32>, ImagingError> {
    let image = PagedImage::<Complex32>::open(&metadata.path).map_err(|error| {
        cache_error(
            &metadata.path,
            format!("cannot reopen Complex32 image pixels: {error}"),
        )
    })?;
    let pixels = image.get().map_err(|error| {
        cache_error(&metadata.path, format!("cannot read image pixels: {error}"))
    })?;
    let plane = first_complex_plane(&metadata.path, pixels, metadata.shape)?;
    if plane
        .iter()
        .any(|value| !(value.re.is_finite() && value.im.is_finite()))
    {
        return Err(cache_error(
            &metadata.path,
            "pixel array contains non-finite complex values",
        ));
    }
    Ok(plane)
}

fn first_complex_plane(
    path: &Path,
    pixels: ArrayD<Complex32>,
    shape: [usize; 2],
) -> Result<Array2<Complex32>, ImagingError> {
    if pixels.shape() != [shape[0], shape[1], 1, 1] {
        return Err(cache_error(
            path,
            format!(
                "pixel shape {:?} changed after metadata indexing; expected [{}, {}, 1, 1]",
                pixels.shape(),
                shape[0],
                shape[1]
            ),
        ));
    }
    let pixels = pixels.into_dimensionality::<Ix4>().map_err(|error| {
        cache_error(
            path,
            format!("cannot view validated four-dimensional pixel shape: {error}"),
        )
    })?;
    Ok(pixels
        .index_axis_move(Axis(3), 0)
        .index_axis_move(Axis(2), 0))
}

fn nearest_distance(
    key: AwConvolutionFunctionKey,
    frequency_hz: f64,
    w_value_lambda: f64,
    parallactic_angle_deg: f64,
) -> f64 {
    let frequency_scale = frequency_hz.abs().max(1.0);
    let w_scale = w_value_lambda.abs().max(1.0);
    let pa_delta = circular_degrees(key.parallactic_angle_deg - parallactic_angle_deg);
    ((key.frequency_hz - frequency_hz) / frequency_scale).abs()
        + ((key.w_value_lambda - w_value_lambda) / w_scale).abs()
        + pa_delta.abs() / 360.0
}

fn circular_degrees(value: f64) -> f64 {
    (value + 180.0).rem_euclid(360.0) - 180.0
}

fn required_f64(record: &RecordValue, name: &str, path: &Path) -> Result<f64, ImagingError> {
    match record.get(name) {
        Some(Value::Scalar(ScalarValue::Float64(value))) => Ok(*value),
        Some(Value::Scalar(ScalarValue::Float32(value))) => Ok(f64::from(*value)),
        Some(other) => Err(cache_error(
            path,
            format!("miscinfo field {name} must be floating point, got {other:?}"),
        )),
        None => Err(cache_error(path, format!("missing miscinfo field {name}"))),
    }
}

fn required_i32(record: &RecordValue, name: &str, path: &Path) -> Result<i32, ImagingError> {
    match record.get(name) {
        Some(Value::Scalar(ScalarValue::Int32(value))) => Ok(*value),
        Some(Value::Scalar(ScalarValue::Int64(value))) => i32::try_from(*value)
            .map_err(|_| cache_error(path, format!("miscinfo field {name} is outside i32 range"))),
        Some(other) => Err(cache_error(
            path,
            format!("miscinfo field {name} must be an integer, got {other:?}"),
        )),
        None => Err(cache_error(path, format!("missing miscinfo field {name}"))),
    }
}

fn required_bool(record: &RecordValue, name: &str, path: &Path) -> Result<bool, ImagingError> {
    match record.get(name) {
        Some(Value::Scalar(ScalarValue::Bool(value))) => Ok(*value),
        Some(other) => Err(cache_error(
            path,
            format!("miscinfo field {name} must be Boolean, got {other:?}"),
        )),
        None => Err(cache_error(path, format!("missing miscinfo field {name}"))),
    }
}

fn required_string(record: &RecordValue, name: &str, path: &Path) -> Result<String, ImagingError> {
    match record.get(name) {
        Some(Value::Scalar(ScalarValue::String(value))) => Ok(value.clone()),
        Some(other) => Err(cache_error(
            path,
            format!("miscinfo field {name} must be a string, got {other:?}"),
        )),
        None => Err(cache_error(path, format!("missing miscinfo field {name}"))),
    }
}

fn positive_integral_usize(value: f64, name: &str, path: &Path) -> Result<usize, ImagingError> {
    if !(value.is_finite() && value >= 1.0 && value.fract() == 0.0 && value <= usize::MAX as f64) {
        return Err(cache_error(
            path,
            format!("miscinfo field {name} must be a positive integral value, got {value}"),
        ));
    }
    Ok(value as usize)
}

fn nonnegative_usize(value: i32, name: &str, path: &Path) -> Result<usize, ImagingError> {
    usize::try_from(value).map_err(|_| {
        cache_error(
            path,
            format!("miscinfo field {name} must be non-negative, got {value}"),
        )
    })
}

fn cache_error(path: &Path, message: impl Into<String>) -> ImagingError {
    ImagingError::ConvolutionFunctionCache(format!("{}: {}", path.display(), message.into()))
}

#[cfg(test)]
mod tests {
    use casa_coordinates::{
        CoordinateSystem, LinearCoordinate, SpectralCoordinate, StokesCoordinate, StokesType,
    };
    use casa_types::{RecordField, ScalarValue, Value, measures::frequency::FrequencyRef};
    use ndarray::Array2;
    use num_complex::Complex32;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        AwParallelHandVisibilityBatch, AwProjectControls, AwProjectGridderConfig, CleanConfig,
        CompatibilityMode, DirtyProductFftPolicy, GridderMode, GroupedVisibilityMetadata,
        GroupedVisibilityMetadataBatch, ImageGeometry, ImagingExecutionPlan, ImagingResolvedPlan,
        MosaicGridderConfig, MosaicMtmfsVisibilityBlock, MtmfsRequest, PlaneStokes,
        PrimaryBeamModel, VisibilityBatch, VisibilitySampleRange, WTermMode, WeightDensityMode,
        WeightingMode, run_mosaic_mtmfs_from_single_plane_stream,
    };

    #[test]
    fn first_complex_plane_consumes_singleton_axes_without_copying_pixels() {
        let pixels = ArrayD::from_shape_vec(
            ndarray::IxDyn(&[2, 3, 1, 1]),
            (0..6)
                .map(|value| Complex32::new(value as f32, -(value as f32)))
                .collect(),
        )
        .unwrap();
        let allocation = pixels.as_ptr();

        let plane = first_complex_plane(Path::new("test.im"), pixels, [2, 3]).unwrap();

        assert_eq!(plane.as_ptr(), allocation);
        assert_eq!(plane.shape(), [2, 3]);
        assert_eq!(plane[[1, 2]], Complex32::new(5.0, -5.0));
    }

    #[test]
    fn paired_cache_indexes_and_loads_pixels_on_demand() {
        let temp = TempDir::new().unwrap();
        for (frequency_index, frequency_hz) in [1.1e9, 1.2e9].into_iter().enumerate() {
            for (w_index, w_value_lambda) in [0.0, 2.0].into_iter().enumerate() {
                let stem = format!("0_0_CF_{frequency_index}_{w_index}_0.im");
                write_test_cell(
                    temp.path(),
                    &format!("CFS_{stem}"),
                    frequency_hz,
                    w_value_lambda,
                    false,
                );
                write_test_cell(
                    temp.path(),
                    &format!("WTCFS_{stem}"),
                    frequency_hz,
                    w_value_lambda,
                    true,
                );
            }
        }

        let cache = AwConvolutionFunctionCache::open(temp.path()).unwrap();
        assert_eq!(cache.inventory().paired_cells, 4);
        assert_eq!(cache.inventory().frequencies_hz, vec![1.1e9, 1.2e9]);
        assert_eq!(cache.inventory().w_values_lambda, vec![0.0, 2.0]);
        assert_eq!(cache.inventory().mueller_elements, vec![0]);
        assert_eq!(cache.inventory().parallactic_angles_deg, vec![30.0]);
        let identity = cache.identity();
        assert_eq!(identity.format, "casa-cf-cache-pagedimage-v1");
        assert_eq!(identity.paired_cells, 4);
        assert_eq!(identity.frequency_hz_bits.len(), 2);
        assert_eq!(identity.w_value_lambda_bits.len(), 2);
        assert_eq!(identity.imaging_shapes, [[16, 16]]);
        assert_eq!(identity.weight_shapes, [[32, 32]]);
        assert_eq!(identity.sampling, [2]);
        assert_eq!(identity.uv_coordinates.len(), 2);
        assert!(identity.uv_coordinates.iter().any(|coordinate| {
            coordinate.increment_bits == [(-2.0f64).to_bits(), 2.0f64.to_bits()]
        }));
        assert!(identity.uv_coordinates.iter().any(|coordinate| {
            coordinate.increment_bits == [(-1.0f64).to_bits(), 1.0f64.to_bits()]
        }));
        assert_eq!(identity.imaging_supports, [[2, 2]]);
        assert_eq!(identity.weight_supports, [[4, 4]]);
        assert_eq!(identity.pixel_type, "complex32");
        assert_eq!(
            identity.metadata_fingerprint,
            AwConvolutionFunctionCache::open(temp.path())
                .unwrap()
                .identity()
                .metadata_fingerprint
        );
        assert!(identity.log_line().contains("cf_freqs=2 cf_wplanes=2"));

        let key = cache.nearest_key(1.19e9, 1.8, 0, 31.0).unwrap();
        assert_eq!(key.frequency_hz, 1.2e9);
        assert_eq!(key.w_value_lambda, 2.0);
        let cell = cache.load(key).unwrap();
        assert_eq!(cell.imaging.dim(), (16, 16));
        assert_eq!(cell.weight.dim(), (32, 32));
        assert_eq!(cell.imaging[(0, 0)], Complex32::new(3.0, -1.0));
        assert_eq!(cell.weight[(0, 0)], Complex32::new(7.0, 2.0));

        let direct = cache
            .select_key_for_sample(1.19e9, 1.0e9, 1.9, 0, 31.0, false)
            .unwrap();
        assert_eq!(direct.frequency_hz, 1.2e9);
        assert_eq!(direct.w_value_lambda, 2.0);
        let conjugate = cache
            .select_key_for_sample(0.81e9, 1.0e9, 2.1, 0, 31.0, true)
            .unwrap();
        assert_eq!(conjugate.frequency_hz, 1.2e9);
        assert_eq!(conjugate.w_value_lambda, 2.0);
    }

    #[test]
    fn sample_selection_uses_casa_conjugate_mueller_row_for_non_positive_w() {
        let temp = TempDir::new().unwrap();
        for (mueller_index, mueller_element) in [0, 15].into_iter().enumerate() {
            for weight in [false, true] {
                let family = if weight { "WTCFS" } else { "CFS" };
                write_test_cell_with_mueller(
                    temp.path(),
                    &format!("{family}_0_0_CF_0_0_{mueller_index}.im"),
                    1.1e9,
                    0.0,
                    mueller_element,
                    weight,
                );
            }
        }

        let cache = AwConvolutionFunctionCache::open(temp.path()).unwrap();
        assert_eq!(
            cache
                .select_key_for_sample(1.1e9, 1.1e9, 1.0, 0, 30.0, false)
                .unwrap()
                .mueller_element,
            0
        );
        assert_eq!(
            cache
                .select_key_for_sample(1.1e9, 1.1e9, 0.0, 0, 30.0, false)
                .unwrap()
                .mueller_element,
            15
        );
        assert_eq!(
            cache
                .select_key_for_sample(1.1e9, 1.1e9, -1.0, 15, 30.0, false)
                .unwrap()
                .mueller_element,
            0
        );
    }

    #[test]
    fn sample_selection_uses_casa_forward_conjugate_frequency_map() {
        let temp = TempDir::new().unwrap();
        for (frequency_index, (frequency_hz, conjugate_frequency_hz)) in
            [(1.0e9, 1.0e9), (2.0e9, 4.0e9)].into_iter().enumerate()
        {
            for weight in [false, true] {
                let family = if weight { "WTCFS" } else { "CFS" };
                write_test_cell_with_conjugate_frequency(
                    temp.path(),
                    &format!("{family}_0_0_CF_{frequency_index}_0_0.im"),
                    frequency_hz,
                    conjugate_frequency_hz,
                    weight,
                );
            }
        }

        let cache = AwConvolutionFunctionCache::open(temp.path()).unwrap();
        // sqrt(2 * 2 GHz^2 - 1 GHz^2) = sqrt(7) GHz, so CASA selects the
        // 2 GHz primary CF. Inverting the deliberately non-bijective
        // persisted ConjFreq values above would incorrectly select 1 GHz.
        let key = cache
            .select_key_for_sample(1.0e9, 2.0e9, 1.0, 0, 30.0, true)
            .unwrap();
        assert_eq!(key.frequency_hz, 2.0e9);
    }

    #[test]
    fn cache_rejects_an_unpaired_cell() {
        let temp = TempDir::new().unwrap();
        write_test_cell(temp.path(), "CFS_0_0_CF_0_0_0.im", 1.1e9, 0.0, false);

        let error = AwConvolutionFunctionCache::open(temp.path()).unwrap_err();
        assert!(
            error.to_string().contains("missing weight WTCFS"),
            "{error}"
        );
    }

    #[test]
    fn cache_rejects_pair_metadata_mismatch() {
        let temp = TempDir::new().unwrap();
        write_test_cell(temp.path(), "CFS_0_0_CF_0_0_0.im", 1.1e9, 0.0, false);
        let path = write_test_cell(temp.path(), "WTCFS_0_0_CF_0_0_0.im", 1.1e9, 0.0, true);
        let mut image = PagedImage::<Complex32>::open(&path).unwrap();
        let mut misc = image.misc_info();
        misc.upsert("Sampling", Value::Scalar(ScalarValue::Float64(3.0)));
        image.set_misc_info(misc).unwrap();
        image.save().unwrap();

        let error = AwConvolutionFunctionCache::open(temp.path()).unwrap_err();
        assert!(
            error.to_string().contains("sampling does not match"),
            "{error}"
        );
    }

    #[test]
    fn cache_rejects_pair_uv_coordinate_mismatch() {
        let temp = TempDir::new().unwrap();
        write_test_cell(temp.path(), "CFS_0_0_CF_0_0_0.im", 1.1e9, 0.0, false);
        write_test_cell_with_uv_reference(
            temp.path(),
            "WTCFS_0_0_CF_0_0_0.im",
            1.1e9,
            0.0,
            true,
            [1.0, 0.0],
        );

        let error = AwConvolutionFunctionCache::open(temp.path()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("weight CF coordinate does not cover the same UV world window"),
            "{error}"
        );
    }

    #[test]
    fn cache_identity_binds_the_uv_affine_coordinate() {
        let default_root = TempDir::new().unwrap();
        let shifted_root = TempDir::new().unwrap();
        for (root, reference_value) in [
            (default_root.path(), [0.0, 0.0]),
            (shifted_root.path(), [1.0, 0.0]),
        ] {
            write_test_cell_with_uv_reference(
                root,
                "CFS_0_0_CF_0_0_0.im",
                1.1e9,
                0.0,
                false,
                reference_value,
            );
            write_test_cell_with_uv_reference(
                root,
                "WTCFS_0_0_CF_0_0_0.im",
                1.1e9,
                0.0,
                true,
                reference_value,
            );
        }

        let default = AwConvolutionFunctionCache::open(default_root.path()).unwrap();
        let shifted = AwConvolutionFunctionCache::open(shifted_root.path()).unwrap();
        assert_ne!(
            default.identity().uv_coordinates,
            shifted.identity().uv_coordinates
        );
        assert_ne!(
            default.identity().metadata_fingerprint,
            shifted.identity().metadata_fingerprint
        );
    }

    #[test]
    fn resident_cache_is_byte_bounded_and_reuses_the_latest_cell() {
        let temp = TempDir::new().unwrap();
        for (frequency_index, frequency_hz) in [1.1e9, 1.2e9].into_iter().enumerate() {
            let stem = format!("0_0_CF_{frequency_index}_0_0.im");
            write_test_cell(
                temp.path(),
                &format!("CFS_{stem}"),
                frequency_hz,
                0.0,
                false,
            );
            write_test_cell(
                temp.path(),
                &format!("WTCFS_{stem}"),
                frequency_hz,
                0.0,
                true,
            );
        }
        let index = AwConvolutionFunctionCache::open(temp.path()).unwrap();
        let keys = index.keys();
        let one_cell_bytes = (16 * 16 + 32 * 32) * std::mem::size_of::<Complex32>();
        let resident = AwConvolutionFunctionResidentCache::new(index, one_cell_bytes);

        let first = resident.get(keys[0]).unwrap();
        assert!(Arc::ptr_eq(&first, &resident.get(keys[0]).unwrap()));
        let second = resident.get(keys[1]).unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        assert!(Arc::ptr_eq(&second, &resident.get(keys[1]).unwrap()));
        assert_eq!(
            resident.stats().unwrap(),
            AwConvolutionFunctionResidentStats {
                resident_cells: 1,
                resident_bytes: one_cell_bytes,
                loads: 2,
                hits: 2,
                evictions: 1,
            }
        );
    }

    #[test]
    fn synthetic_cache_runs_pointing_aware_awproject_mtmfs_products_end_to_end() {
        let temp = TempDir::new().unwrap();
        let frequencies_hz = [1.35e9, 1.45e9];
        let w_values_lambda = [0.0, 2.0];
        for (frequency_index, frequency_hz) in frequencies_hz.into_iter().enumerate() {
            for (w_index, w_value_lambda) in w_values_lambda.into_iter().enumerate() {
                for (mueller_index, mueller_element) in [0, 15].into_iter().enumerate() {
                    for weight in [false, true] {
                        let family = if weight { "WTCFS" } else { "CFS" };
                        write_test_cell_with_mueller(
                            temp.path(),
                            &format!(
                                "{family}_0_0_CF_{frequency_index}_{w_index}_{mueller_index}.im"
                            ),
                            frequency_hz,
                            w_value_lambda,
                            mueller_element,
                            weight,
                        );
                    }
                }
            }
        }

        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let visibility = VisibilityBatch {
            u_lambda: vec![-95.0, -70.0, -35.0, 22.0, 55.0, 105.0],
            v_lambda: vec![-35.0, 40.0, -80.0, 75.0, -25.0, 50.0],
            w_lambda: vec![-2.0, 0.5, 2.0, -1.0, 1.5, -0.5],
            weight: vec![1.0; 6],
            sumwt_factor: vec![1.0; 6],
            gridable: vec![true; 6],
            visibility: vec![Complex32::new(1.0, 0.0); 6],
        };
        let parallel_hands = AwParallelHandVisibilityBatch {
            first_visibility: vec![
                Complex32::new(1.0, 0.10),
                Complex32::new(0.9, -0.05),
                Complex32::new(1.1, 0.02),
                Complex32::new(0.8, -0.08),
                Complex32::new(1.2, 0.04),
                Complex32::new(0.7, -0.03),
            ],
            second_visibility: vec![
                Complex32::new(0.8, -0.04),
                Complex32::new(1.0, 0.03),
                Complex32::new(0.7, -0.02),
                Complex32::new(1.1, 0.06),
                Complex32::new(0.9, -0.01),
                Complex32::new(1.2, 0.05),
            ],
        };
        let primary_beam_model = PrimaryBeamModel::EvlaLBandCommon;
        let grouped_metadata = GroupedVisibilityMetadataBatch {
            sample_count: visibility.len(),
            groups: vec![
                GroupedVisibilityMetadata {
                    beam_frequency_hz: frequencies_hz[0],
                    primary_beam_model,
                    pointing_direction_rad: [0.0, 0.0],
                    sample_ranges: vec![VisibilitySampleRange { start: 0, end: 3 }],
                },
                GroupedVisibilityMetadata {
                    beam_frequency_hz: frequencies_hz[1],
                    primary_beam_model,
                    pointing_direction_rad: [1.5e-4, -1.0e-4],
                    sample_ranges: vec![VisibilitySampleRange { start: 3, end: 6 }],
                },
            ],
        };
        let mut controls = AwProjectControls::casa_defaults(temp.path().to_path_buf());
        controls.w_plane_count = Some(w_values_lambda.len());
        controls.use_pointing = true;
        let one_cell_bytes = (16 * 16 + 32 * 32) * std::mem::size_of::<Complex32>();
        controls.cf_resident_bytes = 4 * one_cell_bytes;
        let request = MtmfsRequest {
            geometry,
            visibility_batches: Vec::new(),
            sample_frequency_batches_hz: Vec::new(),
            gridder_mode: GridderMode::AwProject(AwProjectGridderConfig {
                controls,
                mosaic: MosaicGridderConfig {
                    phase_center_direction_rad: [0.0, 0.0],
                    primary_beam_model,
                    pb_limit: 0.1,
                    metadata_batches: Vec::new(),
                    grouped_metadata_batches: vec![grouped_metadata.clone()],
                },
            }),
            plane_stokes: PlaneStokes::I,
            weighting: WeightingMode::Natural,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [frequencies_hz[0], frequencies_hz[1]],
            nterms: 2,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            w_term_mode: WTermMode::None,
            w_project_planes: None,
            clean: CleanConfig {
                niter: 0,
                ..CleanConfig::default()
            },
            clean_mask: Some(Array2::from_elem((64, 64), true)),
            compatibility: CompatibilityMode::CasaStandardMfs,
        };
        let run = |request| {
            run_mosaic_mtmfs_from_single_plane_stream(
                request,
                ImagingExecutionPlan::new(
                    DirtyProductFftPolicy::correctness_first(),
                    ImagingResolvedPlan::default(),
                ),
                WeightDensityMode::Combined,
                |_, consumer| {
                    consumer(MosaicMtmfsVisibilityBlock {
                        visibility: visibility.clone(),
                        aw_parallel_hands: Some(parallel_hands.clone()),
                        density: None,
                        sample_frequencies_hz: vec![
                            frequencies_hz[0],
                            frequencies_hz[0],
                            frequencies_hz[0],
                            frequencies_hz[1],
                            frequencies_hz[1],
                            frequencies_hz[1],
                        ],
                        gridder_metadata: grouped_metadata.clone(),
                    })
                },
            )
        };
        let result = run(request.clone()).unwrap();

        assert_eq!(result.psf_terms.len(), 3);
        assert_eq!(result.residual_terms.len(), 2);
        assert_eq!(result.model_terms.len(), 2);
        assert_eq!(result.image_terms.len(), 2);
        assert_eq!(result.sumwt_terms.len(), 3);
        assert_eq!(result.weight_terms.len(), 3);
        assert!(result.alpha.is_some());
        assert!(result.alpha_error.is_some());
        assert!(result.alpha_mask.is_some());
        assert!(
            result
                .weight_terms
                .iter()
                .flatten()
                .any(|value| *value > 0.0)
        );
        assert!(
            result
                .residual_terms
                .iter()
                .flatten()
                .any(|value| *value != 0.0)
        );

        let diagnostics = result.awproject.expect("AWProject diagnostics");
        assert!(diagnostics.plan_key.a_term);
        assert!(diagnostics.plan_key.wb_awp);
        assert!(diagnostics.plan_key.conjugate_beams);
        assert!(diagnostics.plan_key.use_pointing);
        assert_eq!(diagnostics.plan_key.w_plane_count, 2);
        assert_eq!(diagnostics.samples.attempted_samples, 6);
        assert_eq!(diagnostics.samples.accepted_samples, 6);
        assert_eq!(diagnostics.samples.rejected_not_gridable, 0);
        assert_eq!(diagnostics.samples.rejected_invalid_input, 0);
        assert!(diagnostics.resident.loads > 0);
        assert!(diagnostics.resident.hits > 0);
        assert!(diagnostics.resident.evictions > 0);
        assert!(diagnostics.resident.resident_bytes <= diagnostics.resident_budget_bytes);

        let mut clean_request = request;
        clean_request.clean.niter = 2;
        clean_request.clean.minor_cycle_length = 1;
        let clean_result = run(clean_request.clone()).unwrap();
        let repeated_clean_result = run(clean_request).unwrap();
        assert_eq!(clean_result.psf_terms, repeated_clean_result.psf_terms);
        assert_eq!(
            clean_result.residual_terms,
            repeated_clean_result.residual_terms
        );
        assert_eq!(clean_result.model_terms, repeated_clean_result.model_terms);
        assert_eq!(clean_result.image_terms, repeated_clean_result.image_terms);
        assert_eq!(clean_result.sumwt_terms, repeated_clean_result.sumwt_terms);
        assert_eq!(
            clean_result.weight_terms,
            repeated_clean_result.weight_terms
        );
        assert_eq!(clean_result.alpha, repeated_clean_result.alpha);
        assert_eq!(clean_result.alpha_error, repeated_clean_result.alpha_error);
        assert_eq!(clean_result.alpha_mask, repeated_clean_result.alpha_mask);
        assert_eq!(clean_result.psf_terms.len(), 3);
        assert_eq!(clean_result.residual_terms.len(), 2);
        assert_eq!(clean_result.model_terms.len(), 2);
        assert_eq!(clean_result.image_terms.len(), 2);
        assert_eq!(clean_result.sumwt_terms.len(), 3);
        assert_eq!(clean_result.weight_terms.len(), 3);
        assert!(clean_result.alpha.is_some());
        assert!(clean_result.alpha_error.is_some());
        assert!(clean_result.alpha_mask.is_some());
        assert!(
            clean_result
                .model_terms
                .iter()
                .flatten()
                .any(|value| *value != 0.0)
        );
        assert!(clean_result.diagnostics.minor_iterations > 0);
        let clean_diagnostics = clean_result.awproject.expect("clean AWProject diagnostics");
        assert_eq!(clean_diagnostics.samples.attempted_samples, 6);
        assert_eq!(clean_diagnostics.samples.accepted_samples, 6);
        assert!(clean_diagnostics.resident.hits > 0);
        assert!(
            clean_diagnostics.resident.resident_bytes <= clean_diagnostics.resident_budget_bytes
        );
    }

    fn write_test_cell(
        root: &Path,
        name: &str,
        frequency_hz: f64,
        w_value_lambda: f64,
        weight: bool,
    ) -> PathBuf {
        write_test_cell_with_uv_reference(
            root,
            name,
            frequency_hz,
            w_value_lambda,
            weight,
            [0.0, 0.0],
        )
    }

    fn write_test_cell_with_mueller(
        root: &Path,
        name: &str,
        frequency_hz: f64,
        w_value_lambda: f64,
        mueller_element: i32,
        weight: bool,
    ) -> PathBuf {
        write_test_cell_with_uv_reference_and_mueller(
            root,
            name,
            frequency_hz,
            w_value_lambda,
            weight,
            [0.0, 0.0],
            mueller_element,
        )
    }

    fn write_test_cell_with_conjugate_frequency(
        root: &Path,
        name: &str,
        frequency_hz: f64,
        conjugate_frequency_hz: f64,
        weight: bool,
    ) -> PathBuf {
        write_test_cell_with_uv_reference_mueller_and_conjugate_frequency(
            root,
            name,
            frequency_hz,
            0.0,
            weight,
            [0.0, 0.0],
            0,
            conjugate_frequency_hz,
        )
    }

    fn write_test_cell_with_uv_reference(
        root: &Path,
        name: &str,
        frequency_hz: f64,
        w_value_lambda: f64,
        weight: bool,
        uv_reference_value: [f64; 2],
    ) -> PathBuf {
        write_test_cell_with_uv_reference_and_mueller(
            root,
            name,
            frequency_hz,
            w_value_lambda,
            weight,
            uv_reference_value,
            0,
        )
    }

    fn write_test_cell_with_uv_reference_and_mueller(
        root: &Path,
        name: &str,
        frequency_hz: f64,
        w_value_lambda: f64,
        weight: bool,
        uv_reference_value: [f64; 2],
        mueller_element: i32,
    ) -> PathBuf {
        write_test_cell_with_uv_reference_mueller_and_conjugate_frequency(
            root,
            name,
            frequency_hz,
            w_value_lambda,
            weight,
            uv_reference_value,
            mueller_element,
            2.0e9 - frequency_hz,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn write_test_cell_with_uv_reference_mueller_and_conjugate_frequency(
        root: &Path,
        name: &str,
        frequency_hz: f64,
        w_value_lambda: f64,
        weight: bool,
        uv_reference_value: [f64; 2],
        mueller_element: i32,
        conjugate_frequency_hz: f64,
    ) -> PathBuf {
        let path = root.join(name);
        let mut coords = CoordinateSystem::new();
        let (reference_pixel, increment) = if weight {
            ([16.0, 16.0], [-1.0, 1.0])
        } else {
            ([8.0, 8.0], [-2.0, 2.0])
        };
        coords.add_coordinate(
            LinearCoordinate::new(
                2,
                vec!["UU".to_string(), "VV".to_string()],
                vec!["lambda".to_string(), "lambda".to_string()],
            )
            .with_reference_value(uv_reference_value.to_vec())
            .with_reference_pixel(reference_pixel.to_vec())
            .with_increment(increment.to_vec()),
        );
        coords.add_coordinate(StokesCoordinate::new(vec![StokesType::RR]));
        coords.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            frequency_hz,
            1.0,
            0.0,
            frequency_hz,
        ));
        let shape = if weight {
            vec![32, 32, 1, 1]
        } else {
            vec![16, 16, 1, 1]
        };
        let mut image = PagedImage::<Complex32>::create(shape, coords, &path).unwrap();
        image
            .set(if weight {
                Complex32::new(7.0, 2.0)
            } else {
                Complex32::new(3.0, -1.0)
            })
            .unwrap();
        let support = if weight { 4 } else { 2 };
        image
            .set_misc_info(RecordValue::new(vec![
                field("BandName", ScalarValue::String("EVLA_L".to_string())),
                field("ConjFreq", ScalarValue::Float64(conjugate_frequency_hz)),
                field("ConjPoln", ScalarValue::Int32(8)),
                field("Diameter", ScalarValue::Float64(25.0)),
                field("MuellerElement", ScalarValue::Int32(mueller_element)),
                field("Name", ScalarValue::String(name.to_string())),
                field("OpCode", ScalarValue::Bool(false)),
                field("ParallacticAngle", ScalarValue::Float64(30.0)),
                field("Sampling", ScalarValue::Float64(2.0)),
                field("TelescopeName", ScalarValue::String("EVLA".to_string())),
                field("WIncr", ScalarValue::Float64(0.5)),
                field("WValue", ScalarValue::Float64(w_value_lambda)),
                field("Xsupport", ScalarValue::Int32(support)),
                field("Ysupport", ScalarValue::Int32(support)),
            ]))
            .unwrap();
        image.save().unwrap();
        path
    }

    fn field(name: &str, value: ScalarValue) -> RecordField {
        RecordField::new(name, Value::Scalar(value))
    }
}
