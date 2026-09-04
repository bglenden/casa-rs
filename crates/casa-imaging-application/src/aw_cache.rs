// SPDX-License-Identifier: LGPL-3.0-or-later

//! Read-only application adapter for a CASA AWProject convolution-function cache.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt, fs,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
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
use casa_imaging_runtime::{
    ArtifactIdentity, ImplementationRegistry, PreparedArtifact, PreparedArtifactDescriptor,
    PreparedArtifactError, PreparedArtifactImportSegment, PreparedArtifactImportSource,
    PreparedArtifactImporter, PreparedArtifactOrder, PreparedArtifactPlaneDescriptor,
    PreparedArtifactPrecision, PreparedArtifactReader, PreparedArtifactReaderResidency,
    PreparedArtifactResidencyMeasurements, PreparedArtifactReuseOutcome,
    PreparedArtifactSegmentDescriptor, PreparedArtifactStore, PreparedArtifactUvAffine,
    StorageDomain, WorkExecutionContext, WorkImplementationId, WorkMeasurements, WorkNodeId,
};
use casa_types::{RecordValue, ScalarValue, Value};
use ndarray::Array2;
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
struct CasaAwCellKey {
    frequency_hz: f64,
    w_value_lambda: f64,
    mueller_element: u32,
    parallactic_angle_deg: f64,
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

/// Metadata-only CASA cache index and read-only cold-import source.
///
/// Opening reads table coordinates and misc-info only. Cold import reopens one
/// selected `CFS_`/`WTCFS_` pair; production pixel access is available only
/// through [`PreparedAwCellProvider`] and the private prepared-artifact layer.
#[derive(Clone, Debug)]
pub struct CasaAwCache {
    root: PathBuf,
    entries: BTreeMap<StableKey, Entry>,
    identities: BTreeMap<[u8; 32], StableKey>,
    inventory: CasaAwCacheInventory,
}

/// One cache cell compiled for the private prepared-artifact owner.
#[derive(Clone, Debug)]
pub struct CasaAwPreparedCell {
    metadata: AwPreparedCellMetadata,
    descriptor: PreparedArtifactDescriptor,
    stable_key: StableKey,
    imaging: KernelMetadata,
    weight: KernelMetadata,
}

impl CasaAwPreparedCell {
    /// Metadata used by the reconstruction-owned selector.
    #[must_use]
    pub const fn metadata(&self) -> &AwPreparedCellMetadata {
        &self.metadata
    }

    /// Exact T50 private-store descriptor for this paired cell.
    #[must_use]
    pub const fn descriptor(&self) -> &PreparedArtifactDescriptor {
        &self.descriptor
    }

    /// Exact decoded complex-pixel residency required by this paired cell.
    #[must_use]
    pub fn decoded_resident_bytes(&self) -> Option<usize> {
        decoded_count(&self.imaging)
            .and_then(|left| decoded_count(&self.weight).and_then(|right| left.checked_add(right)))
            .and_then(|count| count.checked_mul(std::mem::size_of::<Complex64>()))
    }

    /// Conservative encoded workspace required while producing the decoded cell.
    #[must_use]
    pub fn decoder_workspace_bytes(&self) -> Option<usize> {
        [self.imaging.shape, self.weight.shape]
            .into_iter()
            .try_fold(0_usize, |bytes, shape| {
                shape[0]
                    .checked_mul(shape[1])
                    .and_then(|elements| elements.checked_mul(std::mem::size_of::<Complex32>()))
                    .and_then(|plane| bytes.checked_add(plane))
            })?
            .checked_mul(2)
    }

    /// Execute this descriptor's explicit cold-import node. The CASA adapter,
    /// not the generic store, owns all source-table access.
    pub fn import_cold(
        &self,
        cache: &CasaAwCache,
        store: &PreparedArtifactStore,
        source: &PreparedArtifactImportSource,
        context: WorkExecutionContext<'_>,
    ) -> Result<(PreparedArtifact, WorkMeasurements), PreparedArtifactError> {
        let mut importer = cache
            .importer(self)
            .map_err(|_| PreparedArtifactError::SourceIdentityMismatch)?;
        store.import(&context, &self.descriptor, source, &mut importer)
    }

    /// Bind this validated CASA pair as a plan-listed structured load source.
    pub fn import_source(
        &self,
        cache: &CasaAwCache,
        storage_domain: &StorageDomain,
        producer: WorkNodeId,
    ) -> Result<PreparedArtifactImportSource, PreparedArtifactError> {
        let entry = cache
            .entries
            .get(&self.stable_key)
            .filter(|entry| entry.identity == self.metadata.identity())
            .ok_or(PreparedArtifactError::SourceIdentityMismatch)?;
        let source_identity = ArtifactIdentity::from_sha256(entry.identity.as_bytes());
        let segment = |name: &str, metadata: &KernelMetadata| {
            let width = u64::try_from(metadata.shape[0])
                .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?;
            let height = u64::try_from(metadata.shape[1])
                .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?;
            let elements = width
                .checked_mul(height)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
            let bytes = elements
                .checked_mul(8)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
            let operations = elements
                .checked_add(1)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
            PreparedArtifactImportSegment::new(
                name,
                metadata.path.clone(),
                source_identity,
                bytes,
                operations,
                storage_domain,
            )
        };
        PreparedArtifactImportSource::new(
            &self.descriptor,
            producer,
            vec![
                segment("imaging", &entry.imaging)?,
                segment("weight", &entry.weight)?,
            ],
        )
    }

    /// Execute this descriptor's exact warm-reuse node.
    pub fn reuse_warm(
        &self,
        store: &PreparedArtifactStore,
        context: WorkExecutionContext<'_>,
    ) -> Result<(PreparedArtifactReuseOutcome, WorkMeasurements), PreparedArtifactError> {
        store.reuse(&context, &self.descriptor)
    }
}

/// Explicit cold importer which translates one selected CASA pair into the
/// private prepared representation without giving the generic store a CASA path.
pub struct CasaAwCellImporter<'a> {
    entry: &'a Entry,
    loaded: Option<LoadedCasaPlane>,
}

struct LoadedCasaPlane {
    name: &'static str,
    image: PagedImage<Complex32>,
    last: Option<(usize, Complex32)>,
}

/// Cloneable view of one attempt-local, transaction-bound decoded-cell LRU.
#[derive(Clone)]
pub struct PreparedAwCellProvider {
    pool: Arc<PreparedPool>,
}

struct PreparedPool {
    reader: Arc<PreparedArtifactReader>,
    prepared: BTreeMap<[u8; 32], CasaAwPreparedCell>,
    state: Mutex<PreparedPoolState>,
    available: Condvar,
}

#[derive(Default)]
struct PreparedPoolState {
    ceiling: usize,
    workspace_ceiling: usize,
    resident: usize,
    reserved: usize,
    reserved_workspace: usize,
    clock: u64,
    cells: BTreeMap<[u8; 32], ResidentPreparedCell>,
    loading: BTreeSet<[u8; 32]>,
    peak_resident: usize,
    peak_workspace: usize,
    peak_pinned: usize,
    hits: u64,
    loads: u64,
    evicted_bytes: u64,
    copied_bytes: u64,
    closed: bool,
    aborted: bool,
}

struct ResidentPreparedCell {
    cell: Arc<AwConvolutionCell>,
    bytes: usize,
    last_use: u64,
}

impl PreparedPoolState {
    fn ensure_available(&self) -> Result<(), AwOperatorError> {
        if self.closed || self.aborted {
            Err(AwOperatorError::PreparedCellUnavailable)
        } else {
            Ok(())
        }
    }

    fn finish_decoded(
        &mut self,
        decoded: Result<Arc<AwConvolutionCell>, PreparedArtifactError>,
    ) -> Result<Arc<AwConvolutionCell>, AwOperatorError> {
        match decoded {
            Ok(cell) => {
                self.ensure_available()?;
                Ok(cell)
            }
            Err(_) => {
                self.invalidate();
                Err(AwOperatorError::PreparedCellUnavailable)
            }
        }
    }

    fn invalidate(&mut self) {
        self.closed = true;
        self.aborted = true;
        self.cells.clear();
        self.resident = 0;
    }
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

    /// Exact private-cache budget required to retain every canonical payload
    /// and its bounded T50 manifest.
    pub fn prepared_cache_bytes(&self) -> Result<u64, CasaAwCacheError> {
        self.entries.values().try_fold(0_u64, |total, entry| {
            let payload = [entry.imaging.shape, entry.weight.shape]
                .into_iter()
                .try_fold(0_usize, |bytes, shape| {
                    shape[0]
                        .checked_mul(shape[1])
                        .and_then(|elements| elements.checked_mul(std::mem::size_of::<Complex32>()))
                        .and_then(|plane| bytes.checked_add(plane))
                })
                .and_then(|bytes| u64::try_from(bytes).ok())
                .ok_or_else(|| fail(&entry.imaging.path, "prepared payload size overflowed"))?;
            total
                .checked_add(payload)
                .and_then(|bytes| bytes.checked_add(16 * 1024))
                .ok_or_else(|| fail(&entry.imaging.path, "prepared cache budget overflowed"))
        })
    }

    /// Compile one indexed pair into the private-store ownership contract.
    pub fn prepared_cell<R: ImplementationRegistry>(
        &self,
        metadata: &AwPreparedCellMetadata,
        store: &PreparedArtifactStore,
        registry: &R,
        implementation: &WorkImplementationId,
        problem: &casa_imaging_model::CompiledProblem,
    ) -> Result<CasaAwPreparedCell, CasaAwCacheError> {
        let stable_key = *self
            .identities
            .get(&metadata.identity().as_bytes())
            .ok_or_else(|| {
                fail(
                    &self.root,
                    "prepared metadata is not owned by this CASA cache",
                )
            })?;
        let entry = self
            .entries
            .get(&stable_key)
            .ok_or_else(|| fail(&self.root, "prepared CASA cell disappeared from the index"))?;
        let descriptor = PreparedArtifactDescriptor::convolution_function(
            store,
            registry,
            implementation,
            problem,
            entry.identity,
            plane_descriptor(&entry.imaging)?,
            plane_descriptor(&entry.weight)?,
        )
        .map_err(|error| fail(&self.root, format!("cannot compile private cell: {error}")))?;
        Ok(CasaAwPreparedCell {
            metadata: metadata.clone(),
            descriptor,
            stable_key,
            imaging: entry.imaging.clone(),
            weight: entry.weight.clone(),
        })
    }

    /// Compile the complete metadata-only catalog into exact per-cell private
    /// descriptors. No CASA pixel plane is opened by this operation.
    pub fn prepared_cells<R: ImplementationRegistry>(
        &self,
        store: &PreparedArtifactStore,
        registry: &R,
        implementation: &WorkImplementationId,
        problem: &casa_imaging_model::CompiledProblem,
    ) -> Result<Vec<CasaAwPreparedCell>, CasaAwCacheError> {
        self.entries
            .values()
            .map(|entry| {
                let metadata = prepared_metadata(entry)?;
                self.prepared_cell(&metadata, store, registry, implementation, problem)
            })
            .collect()
    }

    /// Create the explicit plan-bound cold importer for one compiled cell.
    pub fn importer<'a>(
        &'a self,
        prepared: &CasaAwPreparedCell,
    ) -> Result<CasaAwCellImporter<'a>, CasaAwCacheError> {
        let entry = self
            .entries
            .get(&prepared.stable_key)
            .filter(|entry| entry.identity == prepared.metadata.identity())
            .ok_or_else(|| fail(&self.root, "compiled cell is not owned by this CASA cache"))?;
        Ok(CasaAwCellImporter {
            entry,
            loaded: None,
        })
    }
}

impl PreparedArtifactImporter for CasaAwCellImporter<'_> {
    fn fill_segment(
        &mut self,
        segment: &PreparedArtifactSegmentDescriptor,
        byte_offset: u64,
        output: &mut [u8],
    ) -> Result<u64, PreparedArtifactError> {
        let (name, metadata) = match segment.name() {
            "imaging" => ("imaging", &self.entry.imaging),
            "weight" => ("weight", &self.entry.weight),
            _ => return Err(PreparedArtifactError::SegmentMismatch),
        };
        let opened = self.loaded.as_ref().map(|loaded| loaded.name) != Some(name);
        if opened {
            self.loaded = Some(LoadedCasaPlane {
                name,
                image: PagedImage::<Complex32>::open(&metadata.path)
                    .map_err(|_| PreparedArtifactError::SourceIdentityMismatch)?,
                last: None,
            });
        }
        let reads = encode_complex32_range(
            self.loaded.as_mut().expect("loaded above"),
            metadata.shape,
            byte_offset,
            output,
        )?;
        reads
            .checked_add(u64::from(opened))
            .ok_or(PreparedArtifactError::ArtifactTooLarge)
    }
}

impl PreparedAwCellProvider {
    /// Bind a fresh decoded-cell LRU to one inactive prepared reader session.
    pub fn new(
        resident_byte_ceiling: usize,
        reader: Arc<PreparedArtifactReader>,
        prepared: Vec<CasaAwPreparedCell>,
    ) -> Result<Self, AwOperatorError> {
        if resident_byte_ceiling == 0 || prepared.is_empty() {
            return Err(AwOperatorError::ResidencyCeilingExceeded);
        }
        let workspace_ceiling = usize::try_from(reader.plan().decoder_workspace_bytes())
            .map_err(|_| AwOperatorError::MeasurementOverflow)?;
        if u64::try_from(resident_byte_ceiling).ok() != Some(reader.plan().decoded_resident_bytes())
            || prepared
                .iter()
                .map(CasaAwPreparedCell::decoder_workspace_bytes)
                .try_fold(0_usize, |maximum, bytes| {
                    bytes.map(|bytes| maximum.max(bytes))
                })
                .filter(|required| *required > 0 && *required <= workspace_ceiling)
                .is_none()
        {
            return Err(AwOperatorError::ResidencyCeilingExceeded);
        }
        let prepared = prepared
            .into_iter()
            .map(|cell| (cell.metadata.identity().as_bytes(), cell))
            .collect::<BTreeMap<_, _>>();
        Ok(Self {
            pool: Arc::new(PreparedPool {
                reader,
                prepared,
                state: Mutex::new(PreparedPoolState {
                    ceiling: resident_byte_ceiling,
                    workspace_ceiling,
                    ..PreparedPoolState::default()
                }),
                available: Condvar::new(),
            }),
        })
    }

    /// Exact decoded bytes currently retained across all prepared cells.
    pub fn resident_bytes(&self) -> Result<usize, AwOperatorError> {
        self.pool
            .state
            .lock()
            .map(|state| state.resident)
            .map_err(|_| AwOperatorError::PreparedCellUnavailable)
    }

    /// Number of exact prepared pairs currently available to operators.
    pub fn resident_cells(&self) -> Result<usize, AwOperatorError> {
        self.pool
            .state
            .lock()
            .map(|state| state.cells.len())
            .map_err(|_| AwOperatorError::PreparedCellUnavailable)
    }
}

impl AwPreparedCellProvider for PreparedAwCellProvider {
    fn load(
        &mut self,
        metadata: &AwPreparedCellMetadata,
        ceiling: usize,
    ) -> Result<AwPreparedCellLease, AwOperatorError> {
        let identity = metadata.identity().as_bytes();
        let prepared = self
            .pool
            .prepared
            .get(&identity)
            .cloned()
            .ok_or(AwOperatorError::PreparedCellUnavailable)?;
        let bytes = prepared
            .decoded_resident_bytes()
            .ok_or(AwOperatorError::MeasurementOverflow)?;
        let workspace = prepared
            .decoder_workspace_bytes()
            .ok_or(AwOperatorError::MeasurementOverflow)?;
        if bytes > ceiling
            || bytes
                > self
                    .pool
                    .state
                    .lock()
                    .map_err(|_| AwOperatorError::PreparedCellUnavailable)?
                    .ceiling
        {
            return Err(AwOperatorError::ResidencyCeilingExceeded);
        }
        let mut evicted = 0_usize;
        'load: loop {
            let mut state = self
                .pool
                .state
                .lock()
                .map_err(|_| AwOperatorError::PreparedCellUnavailable)?;
            state.ensure_available()?;
            if state.cells.contains_key(&identity) {
                state.clock = state
                    .clock
                    .checked_add(1)
                    .ok_or(AwOperatorError::MeasurementOverflow)?;
                let clock = state.clock;
                let cell = {
                    let resident = state
                        .cells
                        .get_mut(&identity)
                        .expect("resident identity was checked");
                    resident.last_use = clock;
                    Arc::clone(&resident.cell)
                };
                state.hits = state
                    .hits
                    .checked_add(1)
                    .ok_or(AwOperatorError::MeasurementOverflow)?;
                observe_pinned(&mut state)?;
                drop(state);
                return Ok(self.lease(cell, AwPreparedCellDisposition::Resident, evicted, 0));
            }
            if state.loading.contains(&identity) {
                drop(
                    self.pool
                        .available
                        .wait(state)
                        .map_err(|_| AwOperatorError::PreparedCellUnavailable)?,
                );
                continue;
            }
            if state
                .reserved_workspace
                .checked_add(workspace)
                .ok_or(AwOperatorError::MeasurementOverflow)?
                > state.workspace_ceiling
            {
                drop(
                    self.pool
                        .available
                        .wait(state)
                        .map_err(|_| AwOperatorError::PreparedCellUnavailable)?,
                );
                continue;
            }
            while state
                .resident
                .checked_add(state.reserved)
                .and_then(|retained| retained.checked_add(bytes))
                .ok_or(AwOperatorError::MeasurementOverflow)?
                > state.ceiling
            {
                let victim = state
                    .cells
                    .iter()
                    .filter(|(_, resident)| Arc::strong_count(&resident.cell) == 1)
                    .min_by_key(|(victim, resident)| (resident.last_use, **victim))
                    .map(|(victim, _)| *victim);
                let Some(victim) = victim else {
                    drop(
                        self.pool
                            .available
                            .wait(state)
                            .map_err(|_| AwOperatorError::PreparedCellUnavailable)?,
                    );
                    continue 'load;
                };
                let victim = state
                    .cells
                    .remove(&victim)
                    .expect("selected resident victim exists");
                state.resident = state
                    .resident
                    .checked_sub(victim.bytes)
                    .ok_or(AwOperatorError::MeasurementOverflow)?;
                evicted = evicted
                    .checked_add(victim.bytes)
                    .ok_or(AwOperatorError::MeasurementOverflow)?;
                state.evicted_bytes = state
                    .evicted_bytes
                    .checked_add(
                        u64::try_from(victim.bytes)
                            .map_err(|_| AwOperatorError::MeasurementOverflow)?,
                    )
                    .ok_or(AwOperatorError::MeasurementOverflow)?;
            }
            if state.cells.contains_key(&identity) || state.loading.contains(&identity) {
                continue;
            }
            state.reserved = state
                .reserved
                .checked_add(bytes)
                .ok_or(AwOperatorError::MeasurementOverflow)?;
            state.reserved_workspace = state
                .reserved_workspace
                .checked_add(workspace)
                .ok_or(AwOperatorError::MeasurementOverflow)?;
            state.peak_workspace = state.peak_workspace.max(state.reserved_workspace);
            state.loading.insert(identity);
            state.loads = state
                .loads
                .checked_add(1)
                .ok_or(AwOperatorError::MeasurementOverflow)?;
            drop(state);

            let decoded = (|| {
                let mut decoder = PreparedCellDecoder::new(&prepared)?;
                self.pool
                    .reader
                    .read(prepared.descriptor().identity(), &mut decoder)?;
                let cell = decoder
                    .finish(&prepared)
                    .map_err(|_| PreparedArtifactError::InvalidLayout)?;
                if cell.resident_bytes() != bytes {
                    return Err(PreparedArtifactError::InvalidLayout);
                }
                Ok::<_, PreparedArtifactError>(Arc::new(cell))
            })();

            let mut state = self
                .pool
                .state
                .lock()
                .map_err(|_| AwOperatorError::PreparedCellUnavailable)?;
            if !state.loading.remove(&identity) {
                return Err(AwOperatorError::PreparedCellUnavailable);
            }
            state.reserved = state
                .reserved
                .checked_sub(bytes)
                .ok_or(AwOperatorError::MeasurementOverflow)?;
            state.reserved_workspace = state
                .reserved_workspace
                .checked_sub(workspace)
                .ok_or(AwOperatorError::MeasurementOverflow)?;
            let cell = match state.finish_decoded(decoded) {
                Ok(cell) => cell,
                Err(error) => {
                    self.pool.available.notify_all();
                    return Err(error);
                }
            };
            state.resident = state
                .resident
                .checked_add(bytes)
                .ok_or(AwOperatorError::MeasurementOverflow)?;
            if state.resident > state.ceiling {
                return Err(AwOperatorError::ResidencyCeilingExceeded);
            }
            state.clock = state
                .clock
                .checked_add(1)
                .ok_or(AwOperatorError::MeasurementOverflow)?;
            let clock = state.clock;
            state.copied_bytes = state
                .copied_bytes
                .checked_add(
                    u64::try_from(bytes).map_err(|_| AwOperatorError::MeasurementOverflow)?,
                )
                .ok_or(AwOperatorError::MeasurementOverflow)?;
            state.peak_resident = state.peak_resident.max(state.resident);
            let lease_cell = Arc::clone(&cell);
            if state
                .cells
                .insert(
                    identity,
                    ResidentPreparedCell {
                        cell,
                        bytes,
                        last_use: clock,
                    },
                )
                .is_some()
            {
                return Err(AwOperatorError::PreparedCellUnavailable);
            }
            observe_pinned(&mut state)?;
            self.pool.available.notify_all();
            drop(state);
            return Ok(self.lease(
                lease_cell,
                AwPreparedCellDisposition::Loaded,
                evicted,
                bytes,
            ));
        }
    }
}

impl PreparedAwCellProvider {
    fn lease(
        &self,
        cell: Arc<AwConvolutionCell>,
        disposition: AwPreparedCellDisposition,
        evicted_bytes: usize,
        copied_bytes: usize,
    ) -> AwPreparedCellLease {
        let pool = Arc::downgrade(&self.pool);
        AwPreparedCellLease::new(cell, disposition, evicted_bytes, copied_bytes)
            .with_release_notifier(move || {
                if let Some(pool) = pool.upgrade() {
                    pool.available.notify_all();
                }
            })
    }

    fn measurements(
        state: &PreparedPoolState,
    ) -> Result<PreparedArtifactResidencyMeasurements, PreparedArtifactError> {
        Ok(PreparedArtifactResidencyMeasurements {
            peak_resident_bytes: u64::try_from(state.peak_resident)
                .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?,
            peak_decoder_workspace_bytes: u64::try_from(state.peak_workspace)
                .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?,
            peak_pinned_bytes: u64::try_from(state.peak_pinned)
                .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?,
            hits: state.hits,
            loads: state.loads,
            evicted_bytes: state.evicted_bytes,
            copied_bytes: state.copied_bytes,
            released_bytes: u64::try_from(state.resident)
                .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?,
        })
    }
}

impl PreparedArtifactReaderResidency for PreparedAwCellProvider {
    fn close(&self) -> Result<PreparedArtifactResidencyMeasurements, PreparedArtifactError> {
        let mut state = self
            .pool
            .state
            .lock()
            .map_err(|_| PreparedArtifactError::PoisonedStore)?;
        state.closed = true;
        if state.reserved != 0
            || state.reserved_workspace != 0
            || !state.loading.is_empty()
            || pinned_bytes(&state)? != 0
        {
            return Err(PreparedArtifactError::ReaderStillInUse);
        }
        Self::measurements(&state)
    }

    fn release(&self) -> Result<PreparedArtifactResidencyMeasurements, PreparedArtifactError> {
        let mut state = self
            .pool
            .state
            .lock()
            .map_err(|_| PreparedArtifactError::PoisonedStore)?;
        state.closed = true;
        if state.reserved != 0 || state.reserved_workspace != 0 || !state.loading.is_empty() {
            return Err(PreparedArtifactError::ReaderStillInUse);
        }
        let measurements = Self::measurements(&state)?;
        state.cells.clear();
        state.resident = 0;
        self.pool.available.notify_all();
        Ok(measurements)
    }

    fn abort(&self) {
        if let Ok(mut state) = self.pool.state.lock() {
            state.invalidate();
            self.pool.available.notify_all();
        }
    }
}

fn pinned_bytes(state: &PreparedPoolState) -> Result<usize, PreparedArtifactError> {
    state
        .cells
        .values()
        .filter(|resident| Arc::strong_count(&resident.cell) > 1)
        .try_fold(0_usize, |total, resident| {
            total
                .checked_add(resident.bytes)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)
        })
}

fn observe_pinned(state: &mut PreparedPoolState) -> Result<(), AwOperatorError> {
    let pinned = pinned_bytes(state).map_err(|_| AwOperatorError::MeasurementOverflow)?;
    state.peak_pinned = state.peak_pinned.max(pinned);
    Ok(())
}

struct PreparedCellDecoder {
    imaging: Vec<u8>,
    weight: Vec<u8>,
    imaging_expected: usize,
    weight_expected: usize,
}

impl PreparedCellDecoder {
    fn new(prepared: &CasaAwPreparedCell) -> Result<Self, PreparedArtifactError> {
        let expected = |segment: &PreparedArtifactSegmentDescriptor| {
            segment
                .shape()
                .iter()
                .try_fold(1_u64, |count, extent| count.checked_mul(*extent))
                .and_then(|count| count.checked_mul(8))
                .and_then(|bytes| usize::try_from(bytes).ok())
                .ok_or(PreparedArtifactError::ArtifactTooLarge)
        };
        let imaging = prepared
            .descriptor
            .imaging_plane()
            .ok_or(PreparedArtifactError::SegmentMismatch)?;
        let weight = prepared
            .descriptor
            .weight_plane()
            .ok_or(PreparedArtifactError::SegmentMismatch)?;
        let imaging_expected = expected(imaging)?;
        let weight_expected = expected(weight)?;
        Ok(Self {
            imaging: Vec::with_capacity(imaging_expected),
            weight: Vec::with_capacity(weight_expected),
            imaging_expected,
            weight_expected,
        })
    }

    fn finish(self, prepared: &CasaAwPreparedCell) -> Result<AwConvolutionCell, CasaAwCacheError> {
        if self.imaging.len() != self.imaging_expected || self.weight.len() != self.weight_expected
        {
            return Err(fail(
                &prepared.imaging.path,
                "private payload ended before its declared shape",
            ));
        }
        let imaging_plane = decode_complex32_plane(self.imaging, &prepared.imaging)?;
        let weight_plane = decode_complex32_plane(self.weight, &prepared.weight)?;
        let imaging = adapt_kernel_from_plane(&prepared.imaging, imaging_plane)?;
        let weight = adapt_kernel_from_plane(&prepared.weight, weight_plane)?;
        AwConvolutionCell::new(prepared.metadata.identity(), imaging, weight)
            .map_err(|error| fail(&prepared.imaging.path, error.to_string()))
    }
}

impl casa_imaging_runtime::PreparedArtifactConsumer for PreparedCellDecoder {
    fn consume_segment(
        &mut self,
        segment: &PreparedArtifactSegmentDescriptor,
        byte_offset: u64,
        input: &[u8],
    ) -> Result<(), PreparedArtifactError> {
        let (output, expected) = match segment.name() {
            "imaging" => (&mut self.imaging, self.imaging_expected),
            "weight" => (&mut self.weight, self.weight_expected),
            _ => return Err(PreparedArtifactError::SegmentMismatch),
        };
        if usize::try_from(byte_offset).ok() != Some(output.len())
            || output.len().saturating_add(input.len()) > expected
        {
            return Err(PreparedArtifactError::SegmentMismatch);
        }
        output.extend_from_slice(input);
        Ok(())
    }
}

fn plane_descriptor(
    metadata: &KernelMetadata,
) -> Result<PreparedArtifactPlaneDescriptor, CasaAwCacheError> {
    let uv = PreparedArtifactUvAffine::new(
        metadata.uv.reference_value.map(f64::from_bits),
        metadata.uv.reference_pixel.map(f64::from_bits),
        metadata.uv.increment.map(f64::from_bits),
        metadata.uv.pc.map(|row| row.map(f64::from_bits)),
    )
    .map_err(|error| fail(&metadata.path, error.to_string()))?;
    PreparedArtifactPlaneDescriptor::new(
        metadata.shape.map(|extent| extent as u64),
        metadata.support.map(|extent| extent as u64),
        metadata.sampling as u64,
        uv,
        PreparedArtifactPrecision::ComplexF32,
        PreparedArtifactOrder::LastAxisContiguousLittleEndian,
    )
    .map_err(|error| fail(&metadata.path, error.to_string()))
}

fn encode_complex32_range(
    loaded: &mut LoadedCasaPlane,
    shape: [usize; 2],
    byte_offset: u64,
    output: &mut [u8],
) -> Result<u64, PreparedArtifactError> {
    let offset =
        usize::try_from(byte_offset).map_err(|_| PreparedArtifactError::ArtifactTooLarge)?;
    let elements = shape[0]
        .checked_mul(shape[1])
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    let scalars = elements
        .checked_mul(2)
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    let total = scalars
        .checked_mul(4)
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    if offset
        .checked_add(output.len())
        .filter(|end| *end <= total)
        .is_none()
    {
        return Err(PreparedArtifactError::SegmentMismatch);
    }
    let mut reads = 0_u64;
    for (output_index, byte) in output.iter_mut().enumerate() {
        let position = offset + output_index;
        let scalar_index = position / 4;
        let element = scalar_index / 2;
        if loaded.last.map(|(index, _)| index) != Some(element) {
            let x = element / shape[1];
            let y = element % shape[1];
            let complex = loaded
                .image
                .get_at(&[x, y, 0, 0])
                .map_err(|_| PreparedArtifactError::SourceIdentityMismatch)?;
            loaded.last = Some((element, complex));
            reads = reads
                .checked_add(1)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        }
        let complex = loaded.last.expect("loaded element").1;
        let scalar = if scalar_index % 2 == 0 {
            complex.re
        } else {
            complex.im
        };
        *byte = scalar.to_le_bytes()[position % 4];
    }
    Ok(reads)
}

fn decode_complex32_plane(
    bytes: Vec<u8>,
    metadata: &KernelMetadata,
) -> Result<Array2<Complex32>, CasaAwCacheError> {
    let chunks = bytes.chunks_exact(8);
    if !chunks.remainder().is_empty() {
        return Err(fail(
            &metadata.path,
            "private complex payload is not element aligned",
        ));
    }
    let values = chunks
        .map(|chunk| {
            Complex32::new(
                f32::from_le_bytes(chunk[..4].try_into().expect("four bytes")),
                f32::from_le_bytes(chunk[4..].try_into().expect("four bytes")),
            )
        })
        .collect::<Vec<_>>();
    Array2::from_shape_vec((metadata.shape[0], metadata.shape[1]), values).map_err(|error| {
        fail(
            &metadata.path,
            format!("cannot decode private plane: {error}"),
        )
    })
}

fn prepared_metadata(entry: &Entry) -> Result<AwPreparedCellMetadata, CasaAwCacheError> {
    let imaging = kernel_layout(&entry.imaging)?;
    let weight = kernel_layout(&entry.weight)?;
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

fn adapt_kernel_from_plane(
    metadata: &KernelMetadata,
    plane: Array2<Complex32>,
) -> Result<AwConvolutionKernel, CasaAwCacheError> {
    let layout = kernel_layout(metadata)?;
    let taps = plane
        .iter()
        .map(|value| Complex64::new(f64::from(value.re), f64::from(value.im)))
        .collect();
    AwConvolutionKernel::new(layout, taps).map_err(|error| fail(&metadata.path, error.to_string()))
}

fn kernel_layout(metadata: &KernelMetadata) -> Result<AwKernelLayout, CasaAwCacheError> {
    let reference = metadata.uv.reference_pixel.map(f64::from_bits);
    let rounded = reference.map(|value| value.round());
    if reference
        .into_iter()
        .zip(rounded)
        .any(|(value, rounded)| value.to_bits() != rounded.to_bits())
    {
        return Err(fail(
            &metadata.path,
            "UU/VV reference pixels must be integral",
        ));
    }
    let center = rounded.map(|value| value as usize);
    AwKernelLayout::new(metadata.support, metadata.sampling, metadata.shape, center)
        .map_err(|error| fail(&metadata.path, error.to_string()))
}

fn decoded_count(metadata: &KernelMetadata) -> Option<usize> {
    metadata.shape[0].checked_mul(metadata.shape[1])
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
    kernel_layout(metadata)?;
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
pub(crate) mod tests {
    use super::*;
    use casa_coordinates::{LinearCoordinate, SpectralCoordinate, StokesCoordinate, StokesType};
    use casa_types::{RecordField, ScalarValue, measures::frequency::FrequencyRef};
    use tempfile::TempDir;

    pub(crate) fn write_test_cache(root: &Path) {
        write_cell(
            root,
            "CFS_one.im",
            false,
            [-2.0, 2.0],
            Complex32::new(3.0, -1.0),
        );
        write_cell(
            root,
            "WTCFS_one.im",
            true,
            [-1.0, 1.0],
            Complex32::new(7.0, 2.0),
        );
    }

    pub(crate) fn write_two_cell_test_cache(root: &Path) {
        write_test_cache(root);
        write_cell_at_w(
            root,
            "CFS_two.im",
            false,
            [-2.0, 2.0],
            2.0,
            Complex32::new(5.0, -2.0),
        );
        write_cell_at_w(
            root,
            "WTCFS_two.im",
            true,
            [-1.0, 1.0],
            2.0,
            Complex32::new(11.0, 4.0),
        );
    }

    #[test]
    fn indexes_asymmetric_same_world_window_without_loading_pixels() {
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

        let cache = CasaAwCache::open(root.path()).unwrap();
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
        drop(catalog);
    }

    #[test]
    fn t51_asymmetric_stored_plane_iteration_is_x_major_then_y() {
        let plane =
            Array2::from_shape_fn((3, 5), |(x, y)| Complex32::new((100 * x + y) as f32, 0.0));
        let stored = plane.iter().copied().collect::<Vec<_>>();

        for x in 0..plane.nrows() {
            for y in 0..plane.ncols() {
                assert_eq!(stored[x * plane.ncols() + y], plane[[x, y]]);
            }
        }
    }

    #[test]
    fn decoded_reader_error_invalidates_residency_and_blocks_later_reuse() {
        let root = TempDir::new().unwrap();
        write_two_cell_test_cache(root.path());
        let cache = CasaAwCache::open(root.path()).unwrap();
        let identities = cache
            .entries
            .values()
            .map(|entry| prepared_metadata(entry).unwrap().identity())
            .collect::<Vec<_>>();
        let resident_identity = identities[0];
        let failed_identity = identities[1];
        let layout = AwKernelLayout::new([0, 0], 1, [3, 3], [1, 1]).unwrap();
        let kernel = AwConvolutionKernel::new(layout, vec![Complex64::new(1.0, 0.0); 9]).unwrap();
        let cell =
            Arc::new(AwConvolutionCell::new(resident_identity, kernel.clone(), kernel).unwrap());
        let bytes = cell.resident_bytes();
        let resident_key = resident_identity.as_bytes();
        let mut state = PreparedPoolState {
            ceiling: bytes * 2,
            resident: bytes,
            cells: BTreeMap::from([(
                resident_key,
                ResidentPreparedCell {
                    cell,
                    bytes,
                    last_use: 1,
                },
            )]),
            ..PreparedPoolState::default()
        };

        assert!(matches!(
            state.finish_decoded(Err(PreparedArtifactError::CorruptArtifact)),
            Err(AwOperatorError::PreparedCellUnavailable)
        ));

        assert!(state.closed);
        assert!(state.aborted);
        assert_eq!(state.resident, 0);
        assert!(state.cells.is_empty());
        assert!(matches!(
            state.ensure_available(),
            Err(AwOperatorError::PreparedCellUnavailable)
        ));
        assert!(!state.cells.contains_key(&resident_key));
        assert!(!state.cells.contains_key(&failed_identity.as_bytes()));
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
    fn pixel_validation_is_deferred_until_cold_import_reads_the_selected_pair() {
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
        let entry = cache.entries.values().next().unwrap();
        assert!(stream_cold_kernel(&entry.imaging).is_err());
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

    #[test]
    #[ignore = "requires the frozen VLASS CASA AW cache"]
    fn t51_frozen_vlass_cache_is_a_complete_bounded_paired_catalog() {
        let root = std::env::var_os("CASA_RS_VLASS_CF_CACHE")
            .map(PathBuf::from)
            .expect("CASA_RS_VLASS_CF_CACHE must name the frozen CASA cache root");
        let cache = CasaAwCache::open(&root).expect("validate frozen paired CASA cache");
        let inventory = cache.inventory();
        assert_eq!(inventory.paired_cells, 1024);
        assert_eq!(inventory.frequencies_hz.len(), 16);
        assert_eq!(inventory.w_values_lambda.len(), 32);
        assert_eq!(inventory.mueller_elements.len(), 2);
        assert_eq!(inventory.parallactic_angles_deg.len(), 1);
        assert_eq!(
            inventory.paired_cells,
            inventory.frequencies_hz.len()
                * inventory.w_values_lambda.len()
                * inventory.mueller_elements.len()
                * inventory.parallactic_angles_deg.len()
        );
        cache
            .prepared_catalog()
            .expect("compile metadata-only catalog");
    }

    fn stream_cold_kernel(
        metadata: &KernelMetadata,
    ) -> Result<AwConvolutionKernel, CasaAwCacheError> {
        let mut loaded = LoadedCasaPlane {
            name: "test",
            image: PagedImage::<Complex32>::open(&metadata.path)
                .map_err(|error| fail(&metadata.path, error.to_string()))?,
            last: None,
        };
        let mut bytes = vec![0_u8; metadata.shape[0] * metadata.shape[1] * 8];
        for (chunk, offset) in bytes.chunks_mut(12).zip((0_u64..).step_by(12)) {
            encode_complex32_range(&mut loaded, metadata.shape, offset, chunk)
                .map_err(|error| fail(&metadata.path, error.to_string()))?;
        }
        let decoded = decode_complex32_plane(bytes, metadata)?;
        adapt_kernel_from_plane(metadata, decoded)
    }

    fn write_cell(root: &Path, name: &str, weight: bool, increment: [f64; 2], value: Complex32) {
        write_cell_at_w(root, name, weight, increment, 0.0, value);
    }

    fn write_cell_at_w(
        root: &Path,
        name: &str,
        weight: bool,
        increment: [f64; 2],
        w_value: f64,
        value: Complex32,
    ) {
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
                field("WValue", ScalarValue::Float64(w_value)),
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
