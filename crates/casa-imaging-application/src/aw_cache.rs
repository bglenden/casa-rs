// SPDX-License-Identifier: LGPL-3.0-or-later

//! Read-only application adapter for a CASA AWProject convolution-function cache.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt, fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use casa_coordinates::{Coordinate, CoordinateModel, CoordinateSystem, CoordinateType};
use casa_images::PagedImage;
use casa_imaging_model::{
    PreparedArtifactAwInterpretation, PreparedArtifactCellSemantics,
    PreparedArtifactScientificIdentity,
};
use casa_imaging_reconstruction::{
    AwConvolutionCell, AwConvolutionKernel, AwKernelLayout, AwOperatorError, AwPreparedCatalog,
    AwPreparedCellDisposition, AwPreparedCellLease, AwPreparedCellMetadata, AwPreparedCellProvider,
};
use casa_types::{RecordValue, ScalarValue, Value};
use ndarray::{Array2, ArrayD, Axis, Ix4};
use num_complex::{Complex32, Complex64};

const IMAGING_PREFIX: &str = "CFS_";
const WEIGHT_PREFIX: &str = "WTCFS_";
const NORMALIZATION: &str = "discrete-complex-sum";

/// Failure while validating or reading a standard CASA AWProject CF cache.
#[derive(Debug)]
pub struct CasaAwCacheError {
    path: PathBuf,
    detail: String,
}

impl fmt::Display for CasaAwCacheError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "CASA AW cache {}: {}",
            self.path.display(),
            self.detail
        )
    }
}

impl Error for CasaAwCacheError {}

/// Exact scientific lookup key carried by one paired CASA cache cell.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CasaAwCellKey {
    /// Spectral-coordinate reference frequency in Hz.
    pub frequency_hz: f64,
    /// W coordinate in wavelengths.
    pub w_value_lambda: f64,
    /// CASA Mueller-matrix element number.
    pub mueller_element: u32,
    /// Parallactic-angle bin in degrees.
    pub parallactic_angle_deg: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct StableKey {
    frequency: u64,
    w: u64,
    mueller: u32,
    pa: u64,
}

impl From<CasaAwCellKey> for StableKey {
    fn from(key: CasaAwCellKey) -> Self {
        Self {
            frequency: key.frequency_hz.to_bits(),
            w: key.w_value_lambda.to_bits(),
            mueller: key.mueller_element,
            pa: key.parallactic_angle_deg.to_bits(),
        }
    }
}

/// Metadata-only axis inventory of a validated cache.
#[derive(Clone, Debug, PartialEq)]
pub struct CasaAwCacheInventory {
    /// Number of paired cells.
    pub paired_cells: usize,
    /// Sorted unique cell frequencies in Hz.
    pub frequencies_hz: Vec<f64>,
    /// Sorted unique W values in wavelengths.
    pub w_values_lambda: Vec<f64>,
    /// Sorted unique Mueller elements.
    pub mueller_elements: Vec<u32>,
    /// Sorted unique parallactic-angle bins in degrees.
    pub parallactic_angles_deg: Vec<f64>,
}

#[derive(Clone, Debug)]
struct UvCoordinate {
    reference_value: [u64; 2],
    reference_pixel: [u64; 2],
    increment: [u64; 2],
    pc: [[u64; 2]; 2],
}

#[derive(Clone, Debug)]
struct KernelMetadata {
    path: PathBuf,
    shape: [usize; 2],
    sampling: usize,
    support: [usize; 2],
    uv: UvCoordinate,
    telescope: String,
    band: String,
    diameter_m: f64,
    conjugate_frequency_hz: f64,
    conjugate_polarization: u32,
    polarization: u32,
    w_increment: f64,
    rotationally_symmetric: bool,
}

#[derive(Clone, Debug)]
struct Entry {
    key: CasaAwCellKey,
    identity: PreparedArtifactScientificIdentity,
    imaging: KernelMetadata,
    weight: KernelMetadata,
}

/// Metadata-only CASA cache index and non-retaining cold-import pixel provider.
///
/// Opening reads table coordinates and misc-info only. [`Self::load_cell`] and
/// the [`AwPreparedCellProvider`] implementation reopen exactly one selected
/// `CFS_`/`WTCFS_` pair and do not retain any pixel array. Production residency
/// remains owned by the private prepared-artifact layer.
#[derive(Clone, Debug)]
pub struct CasaAwCache {
    root: PathBuf,
    entries: BTreeMap<StableKey, Entry>,
    identities: BTreeMap<[u8; 32], StableKey>,
    inventory: CasaAwCacheInventory,
}

impl CasaAwCache {
    /// Validate and index one standard CASA cache directory without reading pixels.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, CasaAwCacheError> {
        let root = root.as_ref();
        let mut imaging = BTreeMap::new();
        let mut weight = BTreeMap::new();
        for directory in
            fs::read_dir(root).map_err(|error| fail(root, format!("cannot list cache: {error}")))?
        {
            let directory = directory
                .map_err(|error| fail(root, format!("cannot inspect cache entry: {error}")))?;
            let path = directory.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let destination = if name.starts_with(WEIGHT_PREFIX) && name.ends_with(".im") {
                &mut weight
            } else if name.starts_with(IMAGING_PREFIX) && name.ends_with(".im") {
                &mut imaging
            } else {
                continue;
            };
            let (key, metadata) = read_metadata(&path)?;
            if destination.insert(StableKey::from(key), metadata).is_some() {
                return Err(fail(
                    &path,
                    format!("duplicate scientific cell key {key:?}"),
                ));
            }
        }
        if imaging.is_empty() && weight.is_empty() {
            return Err(fail(root, "no CFS_/WTCFS_ image-table directories found"));
        }
        let mut entries = BTreeMap::new();
        for (stable, imaging_metadata) in imaging {
            let weight_metadata = weight
                .remove(&stable)
                .ok_or_else(|| fail(&imaging_metadata.path, "paired WTCFS_ cell is missing"))?;
            let key = key_from(stable);
            validate_pair(key, &imaging_metadata, &weight_metadata)?;
            let identity = mint_identity(key, &imaging_metadata)?;
            entries.insert(
                stable,
                Entry {
                    key,
                    identity,
                    imaging: imaging_metadata,
                    weight: weight_metadata,
                },
            );
        }
        if let Some(metadata) = weight.into_values().next() {
            return Err(fail(&metadata.path, "paired CFS_ cell is missing"));
        }
        let inventory = validate_axes(root, &entries)?;
        let identities = entries
            .iter()
            .map(|(key, entry)| (entry.identity.as_bytes(), *key))
            .collect();
        Ok(Self {
            root: root.to_path_buf(),
            entries,
            identities,
            inventory,
        })
    }

    /// Root directory containing the indexed CASA image tables.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Validated metadata-only cache inventory.
    #[must_use]
    pub fn inventory(&self) -> &CasaAwCacheInventory {
        &self.inventory
    }

    /// Build the reconstruction-owned metadata catalog without loading pixels.
    pub fn prepared_catalog(&self) -> Result<AwPreparedCatalog, CasaAwCacheError> {
        let cells = self
            .entries
            .values()
            .map(prepared_metadata)
            .collect::<Result<Vec<_>, _>>()?;
        AwPreparedCatalog::new(cells)
            .map_err(|error| fail(&self.root, format!("cannot adapt catalog: {error}")))
    }

    /// Load and adapt exactly one indexed imaging/weight pair.
    pub fn load_cell(&self, key: CasaAwCellKey) -> Result<AwConvolutionCell, CasaAwCacheError> {
        let entry = self
            .entries
            .get(&StableKey::from(key))
            .ok_or_else(|| fail(&self.root, format!("no exact paired cell for {key:?}")))?;
        load_entry(entry)
    }
}

impl AwPreparedCellProvider for CasaAwCache {
    fn load(
        &mut self,
        metadata: &AwPreparedCellMetadata,
        ceiling: usize,
    ) -> Result<AwPreparedCellLease, AwOperatorError> {
        let stable = self
            .identities
            .get(&metadata.identity().as_bytes())
            .ok_or(AwOperatorError::PreparedCellUnavailable)?;
        let entry = self
            .entries
            .get(stable)
            .ok_or(AwOperatorError::PreparedCellUnavailable)?;
        let bytes = canonical_count(&entry.imaging)
            .and_then(|count| {
                canonical_count(&entry.weight).and_then(|other| count.checked_add(other))
            })
            .and_then(|count| count.checked_mul(std::mem::size_of::<Complex64>()))
            .ok_or(AwOperatorError::ResidencyCeilingExceeded)?;
        if bytes > ceiling {
            return Err(AwOperatorError::ResidencyCeilingExceeded);
        }
        let cell = load_entry(entry).map_err(|_| AwOperatorError::PreparedCellUnavailable)?;
        Ok(AwPreparedCellLease {
            cell: Arc::new(cell),
            disposition: AwPreparedCellDisposition::Loaded,
            evicted_bytes: 0,
            copied_bytes: bytes,
        })
    }
}

fn prepared_metadata(entry: &Entry) -> Result<AwPreparedCellMetadata, CasaAwCacheError> {
    let imaging = AwKernelLayout::new(entry.imaging.support, entry.imaging.sampling)
        .map_err(|error| fail(&entry.imaging.path, error.to_string()))?;
    let weight = AwKernelLayout::new(entry.weight.support, entry.weight.sampling)
        .map_err(|error| fail(&entry.weight.path, error.to_string()))?;
    AwPreparedCellMetadata::new(
        entry.identity,
        entry.key.frequency_hz,
        entry.key.w_value_lambda,
        entry.imaging.w_increment,
        entry.key.mueller_element,
        entry.key.parallactic_angle_deg,
        imaging,
        weight,
    )
    .map_err(|error| fail(&entry.imaging.path, error.to_string()))
}

fn load_entry(entry: &Entry) -> Result<AwConvolutionCell, CasaAwCacheError> {
    let imaging = adapt_kernel(&entry.imaging)?;
    let weight = adapt_kernel(&entry.weight)?;
    AwConvolutionCell::new(entry.identity, imaging, weight)
        .map_err(|error| fail(&entry.imaging.path, error.to_string()))
}

fn adapt_kernel(metadata: &KernelMetadata) -> Result<AwConvolutionKernel, CasaAwCacheError> {
    let plane = read_pixels(metadata)?;
    let mut taps = Vec::with_capacity(
        canonical_count(metadata)
            .ok_or_else(|| fail(&metadata.path, "logical tap-count overflow"))?,
    );
    let reference = metadata.uv.reference_pixel.map(f64::from_bits);
    let center = reference.map(|value| value.round() as isize);
    if reference
        .into_iter()
        .zip(center)
        .any(|(value, rounded)| value.to_bits() != (rounded as f64).to_bits())
    {
        return Err(fail(
            &metadata.path,
            "UU/VV reference pixels must be integral",
        ));
    }
    for fractional_y in 0..metadata.sampling {
        for fractional_x in 0..metadata.sampling {
            for offset_y in 0..=metadata.support[1] * 2 {
                for offset_x in 0..=metadata.support[0] * 2 {
                    let x = source_index(
                        center[0],
                        offset_x,
                        metadata.support[0],
                        metadata.sampling,
                        fractional_x,
                        metadata.shape[0],
                        &metadata.path,
                    )?;
                    let y = source_index(
                        center[1],
                        offset_y,
                        metadata.support[1],
                        metadata.sampling,
                        fractional_y,
                        metadata.shape[1],
                        &metadata.path,
                    )?;
                    let value = plane[[x, y]];
                    taps.push(Complex64::new(f64::from(value.re), f64::from(value.im)));
                }
            }
        }
    }
    let central_count = (metadata.support[0] * 2 + 1) * (metadata.support[1] * 2 + 1);
    let normalization: Complex64 = taps[..central_count].iter().copied().sum();
    let layout = AwKernelLayout::new(metadata.support, metadata.sampling)
        .map_err(|error| fail(&metadata.path, error.to_string()))?;
    AwConvolutionKernel::new(layout, normalization, taps)
        .map_err(|error| fail(&metadata.path, error.to_string()))
}

fn source_index(
    center: isize,
    offset: usize,
    support: usize,
    sampling: usize,
    fractional: usize,
    bound: usize,
    path: &Path,
) -> Result<usize, CasaAwCacheError> {
    center
        .checked_add((offset as isize - support as isize) * sampling as isize)
        .and_then(|value| value.checked_add(fractional as isize))
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value < bound)
        .ok_or_else(|| {
            fail(
                path,
                "support/oversampling footprint lies outside stored plane",
            )
        })
}

fn canonical_count(metadata: &KernelMetadata) -> Option<usize> {
    metadata.support[0]
        .checked_mul(2)?
        .checked_add(1)?
        .checked_mul(metadata.support[1].checked_mul(2)?.checked_add(1)?)?
        .checked_mul(metadata.sampling)?
        .checked_mul(metadata.sampling)
}

fn read_pixels(metadata: &KernelMetadata) -> Result<Array2<Complex32>, CasaAwCacheError> {
    let image = PagedImage::<Complex32>::open(&metadata.path)
        .map_err(|error| fail(&metadata.path, format!("cannot reopen pixels: {error}")))?;
    let pixels = image
        .get()
        .map_err(|error| fail(&metadata.path, format!("cannot read pixels: {error}")))?;
    first_plane(pixels, metadata)
}

fn first_plane(
    pixels: ArrayD<Complex32>,
    metadata: &KernelMetadata,
) -> Result<Array2<Complex32>, CasaAwCacheError> {
    if pixels.shape() != [metadata.shape[0], metadata.shape[1], 1, 1] {
        return Err(fail(&metadata.path, "pixel shape changed after indexing"));
    }
    if pixels
        .iter()
        .any(|value| !value.re.is_finite() || !value.im.is_finite())
    {
        return Err(fail(
            &metadata.path,
            "pixel array contains non-finite values",
        ));
    }
    let pixels = pixels
        .into_dimensionality::<Ix4>()
        .map_err(|error| fail(&metadata.path, error.to_string()))?;
    Ok(pixels
        .index_axis_move(Axis(3), 0)
        .index_axis_move(Axis(2), 0))
}

fn read_metadata(path: &Path) -> Result<(CasaAwCellKey, KernelMetadata), CasaAwCacheError> {
    let image = PagedImage::<Complex32>::open(path)
        .map_err(|error| fail(path, format!("cannot open Complex32 image: {error}")))?;
    let shape = image.shape();
    if shape.len() != 4 || shape[0] == 0 || shape[1] == 0 || shape[2..] != [1, 1] {
        return Err(fail(
            path,
            format!("expected non-empty [nx, ny, 1, 1], got {shape:?}"),
        ));
    }
    let (uv, polarization, frequency_hz) = validate_coordinates(path, image.coordinates())?;
    let misc = image.misc_info();
    let mueller = required_i32(&misc, "MuellerElement", path)?;
    let key = CasaAwCellKey {
        frequency_hz,
        w_value_lambda: required_f64(&misc, "WValue", path)?,
        mueller_element: u32::try_from(mueller)
            .map_err(|_| fail(path, "MuellerElement must be non-negative"))?,
        parallactic_angle_deg: required_f64(&misc, "ParallacticAngle", path)?,
    };
    if !key.w_value_lambda.is_finite()
        || !key.parallactic_angle_deg.is_finite()
        || key.mueller_element >= 16
    {
        return Err(fail(path, "invalid W, Mueller, or parallactic-angle key"));
    }
    let sampling = positive_usize(required_f64(&misc, "Sampling", path)?, "Sampling", path)?;
    let support = [
        positive_i32(required_i32(&misc, "Xsupport", path)?, "Xsupport", path)?,
        positive_i32(required_i32(&misc, "Ysupport", path)?, "Ysupport", path)?,
    ];
    let conjugate_polarization = u32::try_from(required_i32(&misc, "ConjPoln", path)?)
        .map_err(|_| fail(path, "ConjPoln must be non-negative"))?;
    let metadata = KernelMetadata {
        path: path.to_path_buf(),
        shape: [shape[0], shape[1]],
        sampling,
        support,
        uv,
        telescope: required_string(&misc, "TelescopeName", path)?,
        band: required_string(&misc, "BandName", path)?,
        diameter_m: required_f64(&misc, "Diameter", path)?,
        conjugate_frequency_hz: required_f64(&misc, "ConjFreq", path)?,
        conjugate_polarization,
        polarization,
        w_increment: required_f64(&misc, "WIncr", path)?,
        rotationally_symmetric: required_bool(&misc, "OpCode", path)?,
    };
    validate_kernel(&metadata)?;
    Ok((key, metadata))
}

fn validate_coordinates(
    path: &Path,
    coordinates: &CoordinateSystem,
) -> Result<(UvCoordinate, u32, f64), CasaAwCacheError> {
    if coordinates.n_pixel_axes() != 4 {
        return Err(fail(path, "expected exactly four CF pixel axes"));
    }
    let linear_index = coordinates
        .find_coordinate(CoordinateType::Linear)
        .ok_or_else(|| fail(path, "missing UV linear coordinate"))?;
    let model = coordinates.coordinate(linear_index);
    if model.axis_names() != ["UU", "VV"] || model.axis_units() != ["lambda", "lambda"] {
        return Err(fail(path, "expected UU/VV axes in lambda"));
    }
    let CoordinateModel::Linear(linear) = model else {
        return Err(fail(path, "UV coordinate has wrong model type"));
    };
    if linear.n_pixel_axes() != 2 || linear.n_world_axes() != 2 {
        return Err(fail(path, "UU/VV coordinate must have two axes"));
    }
    let reference_value = two(linear.reference_value(), path, "UV reference value")?;
    let reference_pixel = two(linear.reference_pixel(), path, "UV reference pixel")?;
    let increment = two(linear.increment(), path, "UV increment")?;
    let pc_matrix = linear.pc_matrix();
    let pc = [
        [pc_matrix[[0, 0]], pc_matrix[[0, 1]]],
        [pc_matrix[[1, 0]], pc_matrix[[1, 1]]],
    ];
    if reference_value
        .into_iter()
        .chain(reference_pixel)
        .chain(increment)
        .chain(pc.into_iter().flatten())
        .any(|value| !value.is_finite())
        || increment.contains(&0.0)
    {
        return Err(fail(
            path,
            "invalid non-finite or zero-increment UV coordinate",
        ));
    }
    if pc[0][0] * pc[1][1] - pc[0][1] * pc[1][0] == 0.0 {
        return Err(fail(path, "UV PC matrix must be invertible"));
    }
    let stokes_index = coordinates
        .find_coordinate(CoordinateType::Stokes)
        .ok_or_else(|| fail(path, "missing Stokes coordinate"))?;
    let CoordinateModel::Stokes(stokes) = coordinates.coordinate(stokes_index) else {
        return Err(fail(path, "Stokes coordinate has wrong model type"));
    };
    let [polarization] = stokes.stokes() else {
        return Err(fail(path, "expected one degenerate correlation plane"));
    };
    let spectral_index = coordinates
        .find_coordinate(CoordinateType::Spectral)
        .ok_or_else(|| fail(path, "missing spectral coordinate"))?;
    let frequency = coordinates
        .coordinate(spectral_index)
        .reference_value()
        .first()
        .copied()
        .ok_or_else(|| fail(path, "spectral coordinate has no reference frequency"))?;
    if !frequency.is_finite() || frequency <= 0.0 {
        return Err(fail(path, "spectral frequency must be finite and positive"));
    }
    Ok((
        UvCoordinate {
            reference_value: reference_value.map(f64::to_bits),
            reference_pixel: reference_pixel.map(f64::to_bits),
            increment: increment.map(f64::to_bits),
            pc: pc.map(|row| row.map(f64::to_bits)),
        },
        (*polarization).code() as u32,
        frequency,
    ))
}

fn validate_kernel(metadata: &KernelMetadata) -> Result<(), CasaAwCacheError> {
    if metadata.telescope.trim().is_empty() || metadata.band.trim().is_empty() {
        return Err(fail(&metadata.path, "telescope and band must be non-empty"));
    }
    if !metadata.diameter_m.is_finite()
        || metadata.diameter_m <= 0.0
        || !metadata.conjugate_frequency_hz.is_finite()
        || metadata.conjugate_frequency_hz <= 0.0
        || !metadata.w_increment.is_finite()
        || metadata.w_increment <= 0.0
    {
        return Err(fail(
            &metadata.path,
            "diameter, conjugate frequency, and WIncr must be finite and positive",
        ));
    }
    if canonical_count(metadata).is_none() {
        return Err(fail(&metadata.path, "logical tap-count overflow"));
    }
    let reference = metadata.uv.reference_pixel.map(f64::from_bits);
    let center = reference.map(|value| value.round() as isize);
    if reference
        .into_iter()
        .zip(center)
        .any(|(value, rounded)| value.to_bits() != (rounded as f64).to_bits())
    {
        return Err(fail(
            &metadata.path,
            "UU/VV reference pixels must be integral",
        ));
    }
    for axis in 0..2 {
        source_index(
            center[axis],
            0,
            metadata.support[axis],
            metadata.sampling,
            0,
            metadata.shape[axis],
            &metadata.path,
        )?;
        source_index(
            center[axis],
            metadata.support[axis] * 2,
            metadata.support[axis],
            metadata.sampling,
            metadata.sampling - 1,
            metadata.shape[axis],
            &metadata.path,
        )?;
    }
    Ok(())
}

fn validate_pair(
    key: CasaAwCellKey,
    imaging: &KernelMetadata,
    weight: &KernelMetadata,
) -> Result<(), CasaAwCacheError> {
    let common = imaging.sampling == weight.sampling
        && imaging.telescope == weight.telescope
        && imaging.band == weight.band
        && imaging.diameter_m.to_bits() == weight.diameter_m.to_bits()
        && imaging.conjugate_frequency_hz.to_bits() == weight.conjugate_frequency_hz.to_bits()
        && imaging.conjugate_polarization == weight.conjugate_polarization
        && imaging.polarization == weight.polarization
        && imaging.w_increment.to_bits() == weight.w_increment.to_bits()
        && imaging.rotationally_symmetric == weight.rotationally_symmetric;
    if !common {
        return Err(fail(
            &weight.path,
            format!("metadata does not match paired imaging cell for {key:?}"),
        ));
    }
    if imaging.uv.reference_value != weight.uv.reference_value
        || imaging.uv.pc != weight.uv.pc
        || !same_world_window(imaging, weight)
    {
        return Err(fail(
            &weight.path,
            format!("UV coordinate does not cover paired imaging world window for {key:?}"),
        ));
    }
    Ok(())
}

fn same_world_window(left: &KernelMetadata, right: &KernelMetadata) -> bool {
    (0..2).all(|axis| {
        let left_size = left.shape[axis] as f64;
        let right_size = right.shape[axis] as f64;
        nearly_equal(
            f64::from_bits(left.uv.reference_pixel[axis]) / left_size,
            f64::from_bits(right.uv.reference_pixel[axis]) / right_size,
        ) && nearly_equal(
            f64::from_bits(left.uv.increment[axis]) * left_size,
            f64::from_bits(right.uv.increment[axis]) * right_size,
        )
    })
}

fn validate_axes(
    root: &Path,
    entries: &BTreeMap<StableKey, Entry>,
) -> Result<CasaAwCacheInventory, CasaAwCacheError> {
    let first = entries
        .values()
        .next()
        .ok_or_else(|| fail(root, "cache has no paired cells"))?;
    if entries
        .values()
        .any(|entry| entry.imaging.w_increment.to_bits() != first.imaging.w_increment.to_bits())
    {
        return Err(fail(root, "WIncr must be identical across the cache"));
    }
    let frequency_bits = entries
        .values()
        .map(|entry| entry.key.frequency_hz.to_bits())
        .collect::<BTreeSet<_>>();
    let w_bits = entries
        .values()
        .map(|entry| entry.key.w_value_lambda.to_bits())
        .collect::<BTreeSet<_>>();
    let mueller_elements = entries
        .values()
        .map(|entry| entry.key.mueller_element)
        .collect::<BTreeSet<_>>();
    let pa_bits = entries
        .values()
        .map(|entry| entry.key.parallactic_angle_deg.to_bits())
        .collect::<BTreeSet<_>>();
    let expected = frequency_bits
        .len()
        .checked_mul(w_bits.len())
        .and_then(|n| n.checked_mul(mueller_elements.len()))
        .and_then(|n| n.checked_mul(pa_bits.len()))
        .ok_or_else(|| fail(root, "axis cardinality overflow"))?;
    if entries.len() != expected {
        return Err(fail(
            root,
            format!(
                "cache is not a complete frequency x W x Mueller x PA product: found {}, expected {expected}",
                entries.len()
            ),
        ));
    }
    let frequencies_hz = sorted_bits(frequency_bits);
    let w_values_lambda = sorted_bits(w_bits);
    let parallactic_angles_deg = sorted_bits(pa_bits);
    for (index, actual) in w_values_lambda.iter().copied().enumerate() {
        let expected = (index * index) as f64 / first.imaging.w_increment;
        if !nearly_equal_loose(actual, expected) {
            return Err(fail(
                root,
                format!("W plane {index} is {actual}, expected CASA quadratic value {expected}"),
            ));
        }
    }
    Ok(CasaAwCacheInventory {
        paired_cells: entries.len(),
        frequencies_hz,
        w_values_lambda,
        mueller_elements: mueller_elements.into_iter().collect(),
        parallactic_angles_deg,
    })
}

fn mint_identity(
    key: CasaAwCellKey,
    metadata: &KernelMetadata,
) -> Result<PreparedArtifactScientificIdentity, CasaAwCacheError> {
    let semantics = PreparedArtifactCellSemantics::new(
        key.frequency_hz,
        key.w_value_lambda,
        key.mueller_element,
        metadata.polarization,
        key.parallactic_angle_deg,
        metadata.conjugate_frequency_hz,
        metadata.conjugate_polarization,
        &metadata.telescope,
        &metadata.band,
        metadata.diameter_m,
        metadata.w_increment,
        PreparedArtifactAwInterpretation::Wavelength,
        metadata.rotationally_symmetric,
        NORMALIZATION,
    )
    .map_err(|error| fail(&metadata.path, error.to_string()))?;
    PreparedArtifactScientificIdentity::convolution_function(semantics)
        .map_err(|error| fail(&metadata.path, error.to_string()))
}

fn key_from(key: StableKey) -> CasaAwCellKey {
    CasaAwCellKey {
        frequency_hz: f64::from_bits(key.frequency),
        w_value_lambda: f64::from_bits(key.w),
        mueller_element: key.mueller,
        parallactic_angle_deg: f64::from_bits(key.pa),
    }
}
fn sorted_bits(bits: BTreeSet<u64>) -> Vec<f64> {
    let mut values = bits.into_iter().map(f64::from_bits).collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    values
}
fn nearly_equal(left: f64, right: f64) -> bool {
    (left - right).abs() <= left.abs().max(right.abs()).max(1.0) * 32.0 * f64::EPSILON
}
fn nearly_equal_loose(left: f64, right: f64) -> bool {
    (left - right).abs() <= left.abs().max(right.abs()).max(1.0) * 1.0e-10
}
fn two(values: Vec<f64>, path: &Path, name: &str) -> Result<[f64; 2], CasaAwCacheError> {
    values.try_into().map_err(|values: Vec<_>| {
        fail(
            path,
            format!("{name} must have two values, got {}", values.len()),
        )
    })
}
fn positive_usize(value: f64, name: &str, path: &Path) -> Result<usize, CasaAwCacheError> {
    if value.is_finite() && value > 0.0 && value.fract() == 0.0 && value <= usize::MAX as f64 {
        Ok(value as usize)
    } else {
        Err(fail(path, format!("{name} must be a positive integer")))
    }
}
fn positive_i32(value: i32, name: &str, path: &Path) -> Result<usize, CasaAwCacheError> {
    usize::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| fail(path, format!("{name} must be positive")))
}
fn required_f64(record: &RecordValue, name: &str, path: &Path) -> Result<f64, CasaAwCacheError> {
    match record.get(name) {
        Some(Value::Scalar(ScalarValue::Float64(value))) => Ok(*value),
        Some(Value::Scalar(ScalarValue::Float32(value))) => Ok(f64::from(*value)),
        Some(value) => Err(fail(
            path,
            format!("{name} must be floating point, got {value:?}"),
        )),
        None => Err(fail(path, format!("missing miscinfo field {name}"))),
    }
}
fn required_i32(record: &RecordValue, name: &str, path: &Path) -> Result<i32, CasaAwCacheError> {
    match record.get(name) {
        Some(Value::Scalar(ScalarValue::Int32(value))) => Ok(*value),
        Some(Value::Scalar(ScalarValue::Int64(value))) => {
            i32::try_from(*value).map_err(|_| fail(path, format!("{name} is outside i32 range")))
        }
        Some(value) => Err(fail(path, format!("{name} must be integer, got {value:?}"))),
        None => Err(fail(path, format!("missing miscinfo field {name}"))),
    }
}
fn required_bool(record: &RecordValue, name: &str, path: &Path) -> Result<bool, CasaAwCacheError> {
    match record.get(name) {
        Some(Value::Scalar(ScalarValue::Bool(value))) => Ok(*value),
        Some(value) => Err(fail(path, format!("{name} must be Boolean, got {value:?}"))),
        None => Err(fail(path, format!("missing miscinfo field {name}"))),
    }
}
fn required_string(
    record: &RecordValue,
    name: &str,
    path: &Path,
) -> Result<String, CasaAwCacheError> {
    match record.get(name) {
        Some(Value::Scalar(ScalarValue::String(value))) => Ok(value.clone()),
        Some(value) => Err(fail(path, format!("{name} must be string, got {value:?}"))),
        None => Err(fail(path, format!("missing miscinfo field {name}"))),
    }
}
fn fail(path: impl AsRef<Path>, detail: impl Into<String>) -> CasaAwCacheError {
    CasaAwCacheError {
        path: path.as_ref().to_path_buf(),
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_coordinates::{LinearCoordinate, SpectralCoordinate, StokesCoordinate, StokesType};
    use casa_types::{RecordField, ScalarValue, measures::frequency::FrequencyRef};
    use tempfile::TempDir;

    #[test]
    fn indexes_asymmetric_same_world_window_and_loads_one_pair() {
        let root = TempDir::new().unwrap();
        write_cell(
            root.path(),
            "CFS_one.im",
            false,
            [-2.0, 2.0],
            Complex32::new(3.0, -1.0),
        );
        write_cell(
            root.path(),
            "WTCFS_one.im",
            true,
            [-1.0, 1.0],
            Complex32::new(7.0, 2.0),
        );

        let mut cache = CasaAwCache::open(root.path()).unwrap();
        assert_eq!(cache.inventory().paired_cells, 1);
        let catalog = cache.prepared_catalog().unwrap();
        let metadata = prepared_metadata(cache.entries.values().next().unwrap()).unwrap();
        let expected_identity = PreparedArtifactScientificIdentity::convolution_function(
            PreparedArtifactCellSemantics::new(
                1.0e9,
                0.0,
                0,
                StokesType::RR.code() as u32,
                30.0,
                1.0e9,
                8,
                "EVLA",
                "EVLA_L",
                25.0,
                0.5,
                PreparedArtifactAwInterpretation::Wavelength,
                false,
                NORMALIZATION,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(metadata.identity(), expected_identity);
        let lease = AwPreparedCellProvider::load(&mut cache, &metadata, usize::MAX).unwrap();

        assert_eq!(lease.disposition, AwPreparedCellDisposition::Loaded);
        assert_eq!(lease.evicted_bytes, 0);
        assert_eq!(lease.copied_bytes, 2_176);
        assert_eq!(lease.cell.resident_bytes(), 2_176);
        drop(catalog);
    }

    #[test]
    fn rejects_pair_that_does_not_cover_the_same_world_window() {
        let root = TempDir::new().unwrap();
        write_cell(
            root.path(),
            "CFS_one.im",
            false,
            [-2.0, 2.0],
            Complex32::new(1.0, 0.0),
        );
        write_cell(
            root.path(),
            "WTCFS_one.im",
            true,
            [-0.5, 0.5],
            Complex32::new(1.0, 0.0),
        );

        let error = CasaAwCache::open(root.path()).unwrap_err().to_string();
        assert!(
            error.contains("does not cover paired imaging world window"),
            "{error}"
        );
    }

    #[test]
    fn pixel_validation_is_deferred_until_the_selected_pair_loads() {
        let root = TempDir::new().unwrap();
        write_cell(
            root.path(),
            "CFS_one.im",
            false,
            [-2.0, 2.0],
            Complex32::new(f32::NAN, 0.0),
        );
        write_cell(
            root.path(),
            "WTCFS_one.im",
            true,
            [-1.0, 1.0],
            Complex32::new(1.0, 0.0),
        );

        let cache = CasaAwCache::open(root.path()).unwrap();
        let error = cache.load_cell(key()).unwrap_err().to_string();
        assert!(error.contains("non-finite"), "{error}");
    }

    #[test]
    fn rejects_an_unpaired_cache_cell() {
        let root = TempDir::new().unwrap();
        write_cell(
            root.path(),
            "CFS_one.im",
            false,
            [-2.0, 2.0],
            Complex32::new(1.0, 0.0),
        );
        let error = CasaAwCache::open(root.path()).unwrap_err().to_string();
        assert!(error.contains("paired WTCFS_ cell is missing"), "{error}");
    }

    fn key() -> CasaAwCellKey {
        CasaAwCellKey {
            frequency_hz: 1.0e9,
            w_value_lambda: 0.0,
            mueller_element: 0,
            parallactic_angle_deg: 30.0,
        }
    }

    fn write_cell(root: &Path, name: &str, weight: bool, increment: [f64; 2], value: Complex32) {
        let path = root.join(name);
        let shape = if weight {
            vec![32, 32, 1, 1]
        } else {
            vec![16, 16, 1, 1]
        };
        let reference_pixel = if weight {
            vec![16.0, 16.0]
        } else {
            vec![8.0, 8.0]
        };
        let support = if weight { 2 } else { 1 };
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(
            LinearCoordinate::new(
                2,
                vec!["UU".to_string(), "VV".to_string()],
                vec!["lambda".to_string(), "lambda".to_string()],
            )
            .with_reference_value(vec![0.0, 0.0])
            .with_reference_pixel(reference_pixel)
            .with_increment(increment.to_vec()),
        );
        coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::RR]));
        coordinates.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            1.0e9,
            1.0,
            0.0,
            1.0e9,
        ));
        let mut image = PagedImage::<Complex32>::create(shape, coordinates, &path).unwrap();
        image.set(value).unwrap();
        image
            .set_misc_info(RecordValue::new(vec![
                field("BandName", ScalarValue::String("EVLA_L".to_string())),
                field("ConjFreq", ScalarValue::Float64(1.0e9)),
                field("ConjPoln", ScalarValue::Int32(8)),
                field("Diameter", ScalarValue::Float64(25.0)),
                field("MuellerElement", ScalarValue::Int32(0)),
                field("OpCode", ScalarValue::Bool(false)),
                field("ParallacticAngle", ScalarValue::Float64(30.0)),
                field("Sampling", ScalarValue::Float64(2.0)),
                field("TelescopeName", ScalarValue::String("EVLA".to_string())),
                field("WIncr", ScalarValue::Float64(0.5)),
                field("WValue", ScalarValue::Float64(0.0)),
                field("Xsupport", ScalarValue::Int32(support)),
                field("Ysupport", ScalarValue::Int32(support)),
            ]))
            .unwrap();
        image.save().unwrap();
    }

    fn field(name: &str, value: ScalarValue) -> RecordField {
        RecordField::new(name, Value::Scalar(value))
    }
}
