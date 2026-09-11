// SPDX-License-Identifier: LGPL-3.0-or-later

//! Plan-sized tiled backing for inactive channel-local reconstruction state.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use casa_imaging_model::{ModelSample, ModelSupport, ModelValue};
use casa_imaging_reconstruction::{
    ModelLifecycleError, ModelSampleStorage, ModelStorageFactory, SpectralOperatorError,
    runtime_adapter::{NormalArrayStorage, NormalStorageFactory},
};
use casa_lattices::{
    Lattice, LatticeMut, PagedArray, TiledArrayStorageLayout, TiledFileIoStats, TiledShape,
};
use ndarray::{ArrayD, IxDyn};
use tempfile::TempDir;

const DIRECTORY_RANDOM_CHARS: usize = 6;
const MODEL_DIRECTORY_PREFIX: &str = ".casa-rs-cube-model-";
const NORMAL_DIRECTORY_PREFIX: &str = ".casa-rs-cube-normal-";

/// Geometry and exact physical values layout for one reconstruction-owned array.
///
/// Scalar cardinality comes from the reconstruction owner. Cache tiles and the
/// maximum accessed window are independent: sequential I/O can retain one
/// plane while the caller holds a multi-plane window.
#[derive(Debug, Clone)]
pub(crate) struct CubeArrayLayout {
    logical_scalars: usize,
    window_scalars: usize,
    values: TiledArrayStorageLayout,
}

impl CubeArrayLayout {
    pub(crate) fn new(
        logical_scalars: usize,
        plane_scalars: usize,
        window_scalars: usize,
        cache_tiles: usize,
    ) -> Result<Self, String> {
        if logical_scalars == 0 || plane_scalars == 0 || window_scalars == 0 || cache_tiles == 0 {
            return Err("cube array dimensions, window and cache must be positive".into());
        }
        let tile_scalars = plane_scalars.min(logical_scalars);
        let cache_tiles = cache_tiles.min(logical_scalars.div_ceil(tile_scalars));
        let cache_bytes = tile_scalars
            .checked_mul(cache_tiles)
            .and_then(|n| n.checked_mul(size_of::<f64>()))
            .ok_or("cube array cache capacity overflow")?;
        let shape = TiledShape::with_tile_shape(vec![logical_scalars], vec![tile_scalars])
            .map_err(|error| error.to_string())?;
        let values = PagedArray::<f64>::storage_layout(shape, cache_bytes)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            logical_scalars,
            window_scalars: window_scalars.min(logical_scalars),
            values,
        })
    }

    fn support_layout(&self) -> Result<TiledArrayStorageLayout, String> {
        let shape = TiledShape::with_tile_shape(
            self.values.cube_shape().to_vec(),
            self.values.tile_shape().to_vec(),
        )
        .map_err(|error| error.to_string())?;
        PagedArray::<bool>::storage_layout(
            shape,
            self.values.cache_budget_bytes() / size_of::<f64>(),
        )
        .map_err(|error| error.to_string())
    }

    pub(crate) fn normal_ledger(&self, parent: &Path) -> Result<CubeArrayLedger, String> {
        let directory = planned_directory(parent, NORMAL_DIRECTORY_PREFIX);
        let values = directory.join("values");
        let retained = PagedArray::<f64>::planned_persistent_heap_bytes(&self.values, &values)
            .map_err(|error| error.to_string())?;
        let window = checked_bytes(self.window_scalars, size_of::<f64>())?;
        Ok(CubeArrayLedger {
            retained_bytes: checked_sum(&[
                size_of::<PagedNormalArray>(),
                directory.as_os_str().len(),
                retained,
            ])?,
            cache_bytes: self
                .values
                .cache_payload_bytes()
                .map_err(|error| error.to_string())?,
            cache_index_bytes: self
                .values
                .cache_metadata_bytes()
                .map_err(|error| error.to_string())?,
            read_scratch_bytes: checked_sum(&[
                window,
                self.values
                    .slice_scratch_bytes()
                    .map_err(|error| error.to_string())?,
            ])?,
            write_scratch_bytes: checked_sum(&[
                window,
                self.values
                    .slice_scratch_bytes()
                    .map_err(|error| error.to_string())?,
            ])?,
            flush_scratch_bytes: self
                .values
                .flush_scratch_bytes()
                .map_err(|error| error.to_string())?,
            storage_bytes: self
                .values
                .storage_bytes()
                .map_err(|error| error.to_string())?,
            file_handles: 1,
        })
    }
}

/// Complete owned payload/metadata projection for a created private backing.
///
/// Prepared factory metadata is counted separately by the factory. Read/write
/// scratch excludes the reconstruction owner's input/destination window; it
/// includes this adapter's ndarray copy and underlying typed conversion/stride
/// buffers. File lengths include standard metadata and packed edge-tile padding,
/// not filesystem inode/block-allocation overhead or allocator bookkeeping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CubeArrayLedger {
    pub(crate) retained_bytes: usize,
    pub(crate) cache_bytes: usize,
    pub(crate) cache_index_bytes: usize,
    pub(crate) read_scratch_bytes: usize,
    pub(crate) write_scratch_bytes: usize,
    pub(crate) flush_scratch_bytes: usize,
    pub(crate) storage_bytes: usize,
    pub(crate) file_handles: usize,
}

/// Current physical ownership and typed I/O counters for one backing.
#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct CubeArrayMeasurements {
    pub(crate) owned_bytes: usize,
    pub(crate) storage_bytes: usize,
    pub(crate) file_handles: usize,
    pub(crate) values_io: TiledFileIoStats,
    pub(crate) support_io: Option<TiledFileIoStats>,
}

/// Shared observations survive individual temporary backing lifetimes.
///
/// Owned bytes, file lengths and handles are sampled from created backings;
/// cache/index capacities use the same exact prepared geometry verified by the
/// ownership tests. I/O counters cover typed pixel tile reads/writes, including
/// eviction and final flush, but exclude table metadata I/O and sparse set_len.
#[derive(Debug, Default)]
pub(crate) struct CubeBackingMetrics(Mutex<CubeBackingSnapshot>);

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct CubeBackingSnapshot {
    pub(crate) live_owned_bytes: usize,
    pub(crate) peak_owned_bytes: usize,
    pub(crate) live_cache_bytes: usize,
    pub(crate) peak_cache_bytes: usize,
    pub(crate) live_cache_index_bytes: usize,
    pub(crate) peak_cache_index_bytes: usize,
    pub(crate) live_storage_bytes: usize,
    pub(crate) peak_storage_bytes: usize,
    pub(crate) live_file_handles: usize,
    pub(crate) peak_file_handles: usize,
    pub(crate) live_backings: usize,
    pub(crate) peak_backings: usize,
    pub(crate) max_access_scalars: usize,
    pub(crate) read_bytes: usize,
    pub(crate) write_bytes: usize,
}

impl CubeBackingMetrics {
    pub(crate) fn snapshot(&self) -> CubeBackingSnapshot {
        *self.0.lock().expect("cube backing metrics lock poisoned")
    }

    fn register(self: &Arc<Self>, ledger: CubeArrayLedger) -> BackingObservation {
        let mut state = self.0.lock().expect("cube backing metrics lock poisoned");
        state.live_owned_bytes += ledger.retained_bytes;
        state.peak_owned_bytes = state.peak_owned_bytes.max(state.live_owned_bytes);
        state.live_cache_bytes += ledger.cache_bytes;
        state.peak_cache_bytes = state.peak_cache_bytes.max(state.live_cache_bytes);
        state.live_cache_index_bytes += ledger.cache_index_bytes;
        state.peak_cache_index_bytes = state
            .peak_cache_index_bytes
            .max(state.live_cache_index_bytes);
        state.live_storage_bytes += ledger.storage_bytes;
        state.peak_storage_bytes = state.peak_storage_bytes.max(state.live_storage_bytes);
        state.live_file_handles += ledger.file_handles;
        state.peak_file_handles = state.peak_file_handles.max(state.live_file_handles);
        state.live_backings += 1;
        state.peak_backings = state.peak_backings.max(state.live_backings);
        BackingObservation {
            metrics: self.clone(),
            ledger,
        }
    }
}

#[derive(Debug)]
struct BackingObservation {
    metrics: Arc<CubeBackingMetrics>,
    ledger: CubeArrayLedger,
}

impl BackingObservation {
    fn record(&self, scalars: usize, delta: TiledFileIoStats) {
        let mut state = self
            .metrics
            .0
            .lock()
            .expect("cube backing metrics lock poisoned");
        state.max_access_scalars = state.max_access_scalars.max(scalars);
        state.read_bytes +=
            delta.flat_bulk_read_bytes + delta.lru_read_bytes + delta.lru_batch_load_bytes;
        state.write_bytes += delta.flat_flush_write_bytes
            + delta.lru_flush_write_bytes
            + delta.lru_batch_flush_bytes
            + delta.direct_tile_write_bytes;
    }
}

impl Drop for BackingObservation {
    fn drop(&mut self) {
        let mut state = self
            .metrics
            .0
            .lock()
            .expect("cube backing metrics lock poisoned");
        state.live_owned_bytes -= self.ledger.retained_bytes;
        state.live_cache_bytes -= self.ledger.cache_bytes;
        state.live_cache_index_bytes -= self.ledger.cache_index_bytes;
        state.live_storage_bytes -= self.ledger.storage_bytes;
        state.live_file_handles -= self.ledger.file_handles;
        state.live_backings -= 1;
    }
}

fn array_file_bytes(directory: &Path) -> Result<usize, String> {
    ["table.dat", "table.info", "table.f0", "table.f0_TSM0"]
        .iter()
        .try_fold(0usize, |sum, name| {
            let bytes = std::fs::metadata(directory.join(name))
                .map_err(|error| error.to_string())?
                .len();
            let bytes =
                usize::try_from(bytes).map_err(|_| "cube backing file length exceeds usize")?;
            checked_sum(&[sum, bytes])
        })
}

fn planned_directory(parent: &Path, prefix: &str) -> PathBuf {
    parent.join(format!("{prefix}{}", "x".repeat(DIRECTORY_RANDOM_CHARS)))
}

fn checked_bytes(count: usize, bytes: usize) -> Result<usize, String> {
    count
        .checked_mul(bytes)
        .ok_or_else(|| "cube backing byte count overflow".into())
}

fn checked_sum(values: &[usize]) -> Result<usize, String> {
    values.iter().try_fold(0usize, |sum, &value| {
        sum.checked_add(value)
            .ok_or_else(|| "cube backing byte count overflow".into())
    })
}

/// Physical backing configuration; resource admission belongs to the runtime planner.
#[derive(Debug)]
pub(crate) struct PagedNormalStorageFactory {
    parent: Box<Path>,
    layouts: Box<[CubeArrayLayout]>,
    retention: Arc<dyn std::fmt::Debug + Send + Sync>,
    metrics: Arc<CubeBackingMetrics>,
}

impl PagedNormalStorageFactory {
    pub(crate) fn new(
        parent: &Path,
        layouts: Box<[CubeArrayLayout]>,
        retention: Arc<dyn std::fmt::Debug + Send + Sync>,
        metrics: Arc<CubeBackingMetrics>,
    ) -> Self {
        Self {
            parent: parent.into(),
            layouts,
            retention,
            metrics,
        }
    }

    pub(crate) fn owned_metadata_bytes(&self) -> Result<usize, String> {
        let mut bytes = checked_sum(&[
            size_of::<Self>(),
            self.parent.as_os_str().len(),
            checked_bytes(self.layouts.len(), size_of::<CubeArrayLayout>())?,
        ])?;
        for layout in &self.layouts {
            bytes = checked_sum(&[
                bytes,
                layout
                    .values
                    .owned_heap_bytes()
                    .map_err(|error| error.to_string())?,
            ])?;
        }
        Ok(bytes)
    }
}

impl NormalStorageFactory for PagedNormalStorageFactory {
    fn create(
        &self,
        domain_ordinal: usize,
        scalars: usize,
    ) -> Result<Box<dyn NormalArrayStorage>, SpectralOperatorError> {
        let layout = self
            .layouts
            .get(domain_ordinal)
            .filter(|layout| scalars > 0 && scalars <= layout.logical_scalars)
            .ok_or_else(|| {
                normal_storage_error("normal backing does not match its domain layout")
            })?;
        Ok(Box::new(PagedNormalArray::create(
            &self.parent,
            layout,
            scalars,
            self.retention.clone(),
            self.metrics.clone(),
        )?))
    }
}

/// Exact scalar backing with a fixed tile cache and private file lifetime.
///
/// Each read additionally allocates one `f64` slice-result array, and each write
/// allocates one `f64` ndarray copy of the supplied window. These transient
/// arrays occupy `8 * window.len()` bytes each, beyond the cache and caller's
/// window. Construction borrows pre-encoded standard table metadata.
/// The caller must admit these allocations and bound the windows it supplies.
#[derive(Debug)]
pub(crate) struct PagedNormalArray {
    array: Mutex<PagedArray<f64>>,
    scalars: usize,
    window_scalars: usize,
    // Fields drop in declaration order: close the array before removing its files.
    #[allow(
        dead_code,
        reason = "RAII guard removing the private backing files on drop"
    )]
    directory: TempDir,
    observation: BackingObservation,
    _retention: Arc<dyn std::fmt::Debug + Send + Sync>,
}

impl PagedNormalArray {
    pub(crate) fn create(
        parent: &Path,
        layout: &CubeArrayLayout,
        scalars: usize,
        retention: Arc<dyn std::fmt::Debug + Send + Sync>,
        metrics: Arc<CubeBackingMetrics>,
    ) -> Result<Self, SpectralOperatorError> {
        if scalars == 0 || scalars > layout.logical_scalars {
            return Err(normal_storage_error(
                "normal backing exceeds its planned scalar capacity",
            ));
        }
        let directory = tempfile::Builder::new()
            .prefix(NORMAL_DIRECTORY_PREFIX)
            .rand_bytes(DIRECTORY_RANDOM_CHARS)
            .tempdir_in(parent)
            .map_err(normal_storage_error)?;
        let array = PagedArray::create_planned(&layout.values, directory.path().join("values"))
            .map_err(normal_storage_error)?;
        let mut ledger = layout.normal_ledger(parent).map_err(normal_storage_error)?;
        ledger.retained_bytes = checked_sum(&[
            size_of::<Self>(),
            directory.path().as_os_str().len(),
            array
                .owned_persistent_heap_bytes()
                .map_err(normal_storage_error)?,
        ])
        .map_err(normal_storage_error)?;
        ledger.storage_bytes =
            array_file_bytes(&directory.path().join("values")).map_err(normal_storage_error)?;
        ledger.file_handles = array.owned_file_handles();
        Ok(Self {
            array: Mutex::new(array),
            scalars,
            window_scalars: layout.window_scalars.min(scalars),
            directory,
            observation: metrics.register(ledger),
            _retention: retention,
        })
    }

    #[cfg(test)]
    pub(crate) fn measurements(&self) -> Result<CubeArrayMeasurements, SpectralOperatorError> {
        let array = self.array.lock().map_err(normal_storage_error)?;
        Ok(CubeArrayMeasurements {
            owned_bytes: checked_sum(&[
                size_of::<Self>(),
                self.directory.path().as_os_str().len(),
                array
                    .owned_persistent_heap_bytes()
                    .map_err(normal_storage_error)?,
            ])
            .map_err(normal_storage_error)?,
            storage_bytes: array_file_bytes(&self.directory.path().join("values"))
                .map_err(normal_storage_error)?,
            file_handles: array.owned_file_handles(),
            values_io: array.io_stats(),
            support_io: None,
        })
    }

    fn check_window(&self, start: usize, len: usize) -> Result<(), SpectralOperatorError> {
        let end = start
            .checked_add(len)
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        if end > self.scalars || len > self.window_scalars {
            return Err(SpectralOperatorError::InvalidSlab);
        }
        Ok(())
    }
}

impl NormalArrayStorage for PagedNormalArray {
    fn len(&self) -> usize {
        self.scalars
    }

    fn read(&self, start: usize, values: &mut [f64]) -> Result<(), SpectralOperatorError> {
        self.check_window(start, values.len())?;
        if values.is_empty() {
            return Ok(());
        }
        let array = self.array.lock().map_err(normal_storage_error)?;
        let before = array.io_stats();
        let window = array
            .get_slice(&[start], &[values.len()], &[1])
            .map_err(normal_storage_error)?;
        values.copy_from_slice(
            window
                .as_slice()
                .ok_or_else(|| normal_storage_error("normal backing slice is not contiguous"))?,
        );
        self.observation
            .record(values.len(), array.io_stats().delta_since(before));
        Ok(())
    }

    fn write(&mut self, start: usize, values: &[f64]) -> Result<(), SpectralOperatorError> {
        self.check_window(start, values.len())?;
        if values.is_empty() {
            return Ok(());
        }
        let array = self.array.get_mut().map_err(normal_storage_error)?;
        let before = array.io_stats();
        let window = ArrayD::from_shape_vec(IxDyn(&[values.len()]), values.to_vec())
            .map_err(normal_storage_error)?;
        let result = array
            .put_slice(&window, &[start])
            .map_err(normal_storage_error);
        self.observation
            .record(values.len(), array.io_stats().delta_since(before));
        result
    }
}

impl Drop for PagedNormalArray {
    fn drop(&mut self) {
        if let Ok(array) = self.array.get_mut() {
            let before = array.io_stats();
            let _ = array.flush();
            self.observation
                .record(0, array.io_stats().delta_since(before));
        }
    }
}

fn normal_storage_error(error: impl std::fmt::Display) -> SpectralOperatorError {
    SpectralOperatorError::NormalStorage(error.to_string())
}

/// Prepared model backing; the execution owner supplies the already-approved directory.
#[derive(Debug)]
pub(crate) struct PagedModelStorageFactory {
    parent: Box<Path>,
    layout: CubeArrayLayout,
    support: TiledArrayStorageLayout,
    retention: Arc<dyn std::fmt::Debug + Send + Sync>,
    metrics: Arc<CubeBackingMetrics>,
}

impl PagedModelStorageFactory {
    pub(crate) fn new(
        parent: &Path,
        layout: CubeArrayLayout,
        retention: Arc<dyn std::fmt::Debug + Send + Sync>,
        metrics: Arc<CubeBackingMetrics>,
    ) -> Result<Self, ModelLifecycleError> {
        let support = layout.support_layout().map_err(storage_error)?;
        Ok(Self {
            parent: parent.into(),
            layout,
            support,
            retention,
            metrics,
        })
    }

    pub(crate) fn ledger(&self) -> Result<CubeArrayLedger, String> {
        let directory = planned_directory(&self.parent, MODEL_DIRECTORY_PREFIX);
        let values = PagedArray::<f64>::planned_persistent_heap_bytes(
            &self.layout.values,
            &directory.join("values"),
        )
        .map_err(|error| error.to_string())?;
        let support = PagedArray::<bool>::planned_persistent_heap_bytes(
            &self.support,
            &directory.join("support"),
        )
        .map_err(|error| error.to_string())?;
        let value_window = checked_bytes(self.layout.window_scalars, size_of::<f64>())?;
        let support_window = checked_bytes(self.layout.window_scalars, size_of::<bool>())?;
        let value_scratch = self
            .layout
            .values
            .slice_scratch_bytes()
            .map_err(|error| error.to_string())?;
        let support_scratch = self
            .support
            .slice_scratch_bytes()
            .map_err(|error| error.to_string())?;
        Ok(CubeArrayLedger {
            retained_bytes: checked_sum(&[
                size_of::<PagedModelSamples>(),
                directory.as_os_str().len(),
                values,
                support,
            ])?,
            cache_bytes: checked_sum(&[
                self.layout
                    .values
                    .cache_payload_bytes()
                    .map_err(|error| error.to_string())?,
                self.support
                    .cache_payload_bytes()
                    .map_err(|error| error.to_string())?,
            ])?,
            cache_index_bytes: checked_sum(&[
                self.layout
                    .values
                    .cache_metadata_bytes()
                    .map_err(|error| error.to_string())?,
                self.support
                    .cache_metadata_bytes()
                    .map_err(|error| error.to_string())?,
            ])?,
            read_scratch_bytes: checked_sum(&[value_window, value_scratch])?.max(checked_sum(&[
                value_window,
                support_window,
                support_scratch,
            ])?),
            write_scratch_bytes: checked_sum(&[value_window, value_scratch])?
                .max(checked_sum(&[support_window, support_scratch])?),
            flush_scratch_bytes: self
                .layout
                .values
                .flush_scratch_bytes()
                .map_err(|error| error.to_string())?
                .max(
                    self.support
                        .flush_scratch_bytes()
                        .map_err(|error| error.to_string())?,
                ),
            storage_bytes: checked_sum(&[
                self.layout
                    .values
                    .storage_bytes()
                    .map_err(|error| error.to_string())?,
                self.support
                    .storage_bytes()
                    .map_err(|error| error.to_string())?,
            ])?,
            file_handles: 2,
        })
    }

    pub(crate) fn owned_metadata_bytes(&self) -> Result<usize, String> {
        checked_sum(&[
            size_of::<Self>(),
            self.parent.as_os_str().len(),
            self.layout
                .values
                .owned_heap_bytes()
                .map_err(|error| error.to_string())?,
            self.support
                .owned_heap_bytes()
                .map_err(|error| error.to_string())?,
        ])
    }
}

impl ModelStorageFactory for PagedModelStorageFactory {
    fn create(
        &self,
        sample_count: usize,
    ) -> Result<Box<dyn ModelSampleStorage>, ModelLifecycleError> {
        if sample_count != self.layout.logical_scalars {
            return Err(ModelLifecycleError::SampleCountMismatch {
                expected: self.layout.logical_scalars,
                actual: sample_count,
            });
        }
        Ok(Box::new(PagedModelSamples::create(
            &self.parent,
            &self.layout,
            &self.support,
            self.retention.clone(),
            self.metrics.clone(),
            self.ledger().map_err(storage_error)?,
        )?))
    }
}

/// One logical model retains its private files, not its full pixel payload.
/// Values and support remain independently represented at full precision.
#[derive(Debug)]
pub(crate) struct PagedModelSamples {
    arrays: Mutex<ModelArrays>,
    samples: usize,
    window_samples: usize,
    // Delete after the array handles have flushed and closed.
    #[allow(
        dead_code,
        reason = "RAII guard removing the private backing files on drop"
    )]
    directory: TempDir,
    observation: BackingObservation,
    _retention: Arc<dyn std::fmt::Debug + Send + Sync>,
}

#[derive(Debug)]
struct ModelArrays {
    values: PagedArray<f64>,
    support: PagedArray<bool>,
}

impl PagedModelSamples {
    pub(crate) fn create(
        directory: &Path,
        layout: &CubeArrayLayout,
        support_layout: &TiledArrayStorageLayout,
        retention: Arc<dyn std::fmt::Debug + Send + Sync>,
        metrics: Arc<CubeBackingMetrics>,
        mut ledger: CubeArrayLedger,
    ) -> Result<Self, ModelLifecycleError> {
        let directory = tempfile::Builder::new()
            .prefix(MODEL_DIRECTORY_PREFIX)
            .rand_bytes(DIRECTORY_RANDOM_CHARS)
            .tempdir_in(directory)
            .map_err(storage_error)?;
        let values = PagedArray::create_planned(&layout.values, directory.path().join("values"))
            .map_err(storage_error)?;
        let support = PagedArray::create_planned(support_layout, directory.path().join("support"))
            .map_err(storage_error)?;
        ledger.retained_bytes = checked_sum(&[
            size_of::<Self>(),
            directory.path().as_os_str().len(),
            values
                .owned_persistent_heap_bytes()
                .map_err(storage_error)?,
            support
                .owned_persistent_heap_bytes()
                .map_err(storage_error)?,
        ])
        .map_err(storage_error)?;
        ledger.storage_bytes = checked_sum(&[
            array_file_bytes(&directory.path().join("values")).map_err(storage_error)?,
            array_file_bytes(&directory.path().join("support")).map_err(storage_error)?,
        ])
        .map_err(storage_error)?;
        ledger.file_handles = values.owned_file_handles() + support.owned_file_handles();
        Ok(Self {
            arrays: Mutex::new(ModelArrays { values, support }),
            samples: layout.logical_scalars,
            window_samples: layout.window_scalars,
            directory,
            observation: metrics.register(ledger),
            _retention: retention,
        })
    }

    #[cfg(test)]
    pub(crate) fn measurements(&self) -> Result<CubeArrayMeasurements, ModelLifecycleError> {
        let arrays = self.arrays.lock().map_err(storage_error)?;
        Ok(CubeArrayMeasurements {
            owned_bytes: checked_sum(&[
                size_of::<Self>(),
                self.directory.path().as_os_str().len(),
                arrays
                    .values
                    .owned_persistent_heap_bytes()
                    .map_err(storage_error)?,
                arrays
                    .support
                    .owned_persistent_heap_bytes()
                    .map_err(storage_error)?,
            ])
            .map_err(storage_error)?,
            storage_bytes: checked_sum(&[
                array_file_bytes(&self.directory.path().join("values")).map_err(storage_error)?,
                array_file_bytes(&self.directory.path().join("support")).map_err(storage_error)?,
            ])
            .map_err(storage_error)?,
            file_handles: arrays.values.owned_file_handles() + arrays.support.owned_file_handles(),
            values_io: arrays.values.io_stats(),
            support_io: Some(arrays.support.io_stats()),
        })
    }
}

impl ModelSampleStorage for PagedModelSamples {
    fn sample_count(&self) -> usize {
        self.samples
    }

    fn read(
        &self,
        start: usize,
        destination: &mut [ModelSample],
    ) -> Result<(), ModelLifecycleError> {
        if start
            .checked_add(destination.len())
            .is_none_or(|end| end > self.samples || destination.len() > self.window_samples)
        {
            return Err(ModelLifecycleError::CellOutsideShape);
        }
        if destination.is_empty() {
            return Ok(());
        }
        let arrays = self.arrays.lock().map_err(storage_error)?;
        let before_values = arrays.values.io_stats();
        let before_support = arrays.support.io_stats();
        let values = arrays
            .values
            .get_slice(&[start], &[destination.len()], &[1])
            .map_err(storage_error)?;
        let support = arrays
            .support
            .get_slice(&[start], &[destination.len()], &[1])
            .map_err(storage_error)?;
        self.observation.record(
            destination.len(),
            arrays.values.io_stats().delta_since(before_values),
        );
        self.observation.record(
            destination.len(),
            arrays.support.io_stats().delta_since(before_support),
        );
        let values = values
            .as_slice()
            .ok_or_else(|| storage_error("model value window is not contiguous"))?;
        let support = support
            .as_slice()
            .ok_or_else(|| storage_error("model support window is not contiguous"))?;
        for ((destination, value), supported) in destination
            .iter_mut()
            .zip(values.iter().copied())
            .zip(support.iter().copied())
        {
            *destination = if supported {
                ModelSample::valid(ModelValue::new(value)?)
            } else if value == 0.0 {
                ModelSample::invalid()
            } else {
                return Err(ModelLifecycleError::InvalidSupportPayload);
            };
        }
        Ok(())
    }

    fn write(&mut self, start: usize, samples: &[ModelSample]) -> Result<(), ModelLifecycleError> {
        if start
            .checked_add(samples.len())
            .is_none_or(|end| end > self.samples || samples.len() > self.window_samples)
        {
            return Err(ModelLifecycleError::CellOutsideShape);
        }
        if samples.is_empty() {
            return Ok(());
        }
        let arrays = self.arrays.get_mut().map_err(storage_error)?;
        let before_values = arrays.values.io_stats();
        let before_support = arrays.support.io_stats();
        let values = ArrayD::from_shape_vec(
            IxDyn(&[samples.len()]),
            samples
                .iter()
                .map(|sample| sample.value().value())
                .collect(),
        )
        .map_err(storage_error)?;
        arrays
            .values
            .put_slice(&values, &[start])
            .map_err(storage_error)?;
        drop(values);
        let support = ArrayD::from_shape_vec(
            IxDyn(&[samples.len()]),
            samples
                .iter()
                .map(|sample| sample.support() == ModelSupport::Valid)
                .collect(),
        )
        .map_err(storage_error)?;
        arrays
            .support
            .put_slice(&support, &[start])
            .map_err(storage_error)?;
        self.observation.record(
            samples.len(),
            arrays.values.io_stats().delta_since(before_values),
        );
        self.observation.record(
            samples.len(),
            arrays.support.io_stats().delta_since(before_support),
        );
        Ok(())
    }
}

impl Drop for PagedModelSamples {
    fn drop(&mut self) {
        if let Ok(arrays) = self.arrays.get_mut() {
            let before = arrays.values.io_stats();
            let _ = arrays.values.flush();
            self.observation
                .record(0, arrays.values.io_stats().delta_since(before));
            let before = arrays.support.io_stats();
            let _ = arrays.support.flush();
            self.observation
                .record(0, arrays.support.io_stats().delta_since(before));
        }
    }
}

fn storage_error(error: impl std::fmt::Display) -> ModelLifecycleError {
    ModelLifecycleError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t55_metrics_release_after_files_and_before_retention() {
        #[derive(Debug)]
        struct Retention {
            parent: PathBuf,
            metrics: Arc<CubeBackingMetrics>,
        }
        impl Drop for Retention {
            fn drop(&mut self) {
                assert_eq!(std::fs::read_dir(&self.parent).unwrap().count(), 0);
                assert_eq!(self.metrics.snapshot().live_backings, 0);
            }
        }
        let parent = tempfile::tempdir().unwrap();
        let metrics = Arc::new(CubeBackingMetrics::default());
        let retention = Arc::new(Retention {
            parent: parent.path().into(),
            metrics: metrics.clone(),
        });
        let layout = CubeArrayLayout::new(257, 8, 7, 1).unwrap();
        let ledger = layout.normal_ledger(parent.path()).unwrap();
        let mut array =
            PagedNormalArray::create(parent.path(), &layout, 257, retention, metrics.clone())
                .unwrap();
        array.write(0, &[1.0; 7]).unwrap();
        array.write(16, &[2.0; 7]).unwrap();
        array.read(0, &mut [0.0; 7]).unwrap();
        let live = metrics.snapshot();
        assert_eq!(live.live_owned_bytes, ledger.retained_bytes);
        assert_eq!(live.live_storage_bytes, ledger.storage_bytes);
        assert_eq!(live.live_backings, 1);
        assert_eq!(live.max_access_scalars, 7);
        assert!(live.read_bytes > 0);
        assert!(live.write_bytes > 0);
        drop(array);
        let closed = metrics.snapshot();
        assert_eq!(closed.live_owned_bytes, 0);
        assert_eq!(closed.live_storage_bytes, 0);
        assert_eq!(closed.live_file_handles, 0);
        assert_eq!(closed.live_cache_bytes, 0);
        assert_eq!(closed.live_cache_index_bytes, 0);
        assert_eq!(closed.peak_owned_bytes, ledger.retained_bytes);
        assert_eq!(closed.peak_backings, 1);
    }

    #[test]
    fn t55_paged_normal_preserves_exact_bits_cache_and_last_owner_files() {
        let parent = tempfile::tempdir().unwrap();
        let layout = CubeArrayLayout::new(257, 8, 7, 2).unwrap();
        let ledger = layout.normal_ledger(parent.path()).unwrap();
        let mut storage =
            PagedNormalArray::create(parent.path(), &layout, 257, Arc::new(()), Arc::default())
                .unwrap();
        let path = storage.directory.path().to_path_buf();
        let expected: Vec<f64> = (0..257)
            .map(|i| match i % 5 {
                0 => -0.0,
                1 => f64::from_bits(0x3ff0000000000001),
                2 => f64::from_bits(1),
                3 => f64::from_bits(0x7ff8000000000042),
                _ => -(i as f64) / 8.0,
            })
            .collect();
        for (index, values) in expected.chunks(7).enumerate() {
            storage.write(index * 7, values).unwrap();
        }
        let mut actual = [0.0; 7];
        for start in (0..257).step_by(7) {
            let count = 7.min(257 - start);
            storage.read(start, &mut actual[..count]).unwrap();
            assert_eq!(
                actual[..count]
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>(),
                expected[start..start + count]
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>()
            );
        }
        assert_eq!(storage.len(), 257);
        let measured = storage.measurements().unwrap();
        assert_eq!(measured.owned_bytes, ledger.retained_bytes);
        assert_eq!(measured.storage_bytes, ledger.storage_bytes);
        assert_eq!(measured.file_handles, ledger.file_handles);
        assert!(measured.values_io.put_slice_copied_elements >= 257);
        assert!(measured.support_io.is_none());
        assert_eq!(
            storage.array.lock().unwrap().maximum_cache_size_pixels(),
            16
        );
        let owner = std::sync::Arc::new(storage);
        let last_owner = owner.clone();
        drop(owner);
        assert!(path.exists());
        last_owner.read(256, &mut actual[..1]).unwrap();
        assert_eq!(actual[0].to_bits(), expected[256].to_bits());
        drop(last_owner);
        assert!(!path.exists());
    }

    #[test]
    fn t55_paged_normal_rejects_padding_and_overflow_without_changing_destination() {
        let parent = tempfile::tempdir().unwrap();
        let layout = CubeArrayLayout::new(11, 8, 2, 1).unwrap();
        let factory = PagedNormalStorageFactory::new(
            parent.path(),
            vec![layout].into_boxed_slice(),
            Arc::new(()),
            Arc::default(),
        );
        let mut storage = factory.create(0, 9).unwrap();
        assert!(factory.create(1, 9).is_err());
        assert!(factory.create(0, 12).is_err());
        storage.write(8, &[-0.0]).unwrap();
        let mut destination = [123.0; 2];
        assert_eq!(
            storage.read(8, &mut destination),
            Err(SpectralOperatorError::InvalidSlab)
        );
        assert_eq!(destination, [123.0; 2]);
        assert_eq!(
            storage.write(9, &[1.0]),
            Err(SpectralOperatorError::InvalidSlab)
        );
        assert_eq!(
            storage.read(10, &mut []),
            Err(SpectralOperatorError::InvalidSlab)
        );
        assert_eq!(
            storage.write(usize::MAX, &[1.0]),
            Err(SpectralOperatorError::ResidencyOverflow)
        );
        storage.read(9, &mut []).unwrap();
        storage.write(9, &[]).unwrap();
        storage.read(8, &mut destination[..1]).unwrap();
        assert_eq!(destination[0].to_bits(), (-0.0_f64).to_bits());
    }

    #[test]
    fn t55_paged_normal_preserves_allocation_fault_as_storage_error() {
        let parent = tempfile::tempdir().unwrap();
        let missing = parent.path().join("missing");
        let layout = CubeArrayLayout::new(9, 8, 2, 1).unwrap();
        let factory = PagedNormalStorageFactory::new(
            &missing,
            vec![layout].into_boxed_slice(),
            Arc::new(()),
            Arc::default(),
        );
        let error = factory.create(0, 9).unwrap_err();
        assert!(
            matches!(error, SpectralOperatorError::NormalStorage(message) if !message.is_empty())
        );
        assert!(!missing.exists());
        for (scalars, tile_scalars, cache_tiles) in [(9, 0, 1), (9, 8, 0), (usize::MAX, 8, 1)] {
            assert!(CubeArrayLayout::new(scalars, tile_scalars, 2, cache_tiles).is_err());
        }
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }

    #[test]
    fn paged_model_preserves_values_support_and_last_window_then_removes_its_files() {
        let parent = tempfile::tempdir().unwrap();
        let layout = CubeArrayLayout::new(257, 8, 3, 1).unwrap();
        let factory =
            PagedModelStorageFactory::new(parent.path(), layout, Arc::new(()), Arc::default())
                .unwrap();
        let ledger = factory.ledger().unwrap();
        let mut storage = PagedModelSamples::create(
            parent.path(),
            &factory.layout,
            &factory.support,
            Arc::new(()),
            Arc::default(),
            ledger,
        )
        .unwrap();
        let path = storage.directory.path().to_path_buf();
        let expected = [
            ModelSample::valid(ModelValue::new(-0.125).unwrap()),
            ModelSample::invalid(),
            ModelSample::valid(ModelValue::new(f64::from_bits(0x3ff0000000000001)).unwrap()),
        ];
        storage.write(254, &expected).unwrap();
        let mut actual = [ModelSample::invalid(); 3];
        storage.read(254, &mut actual).unwrap();
        assert_eq!(actual, expected);
        let measured = storage.measurements().unwrap();
        assert_eq!(measured.owned_bytes, ledger.retained_bytes);
        assert_eq!(measured.storage_bytes, ledger.storage_bytes);
        assert_eq!(measured.file_handles, ledger.file_handles);
        assert!(measured.support_io.is_some());
        let arrays = storage.arrays.lock().unwrap();
        assert_eq!(arrays.values.maximum_cache_size_pixels(), 8);
        assert_eq!(arrays.support.maximum_cache_size_pixels(), 8);
        drop(arrays);
        assert!(storage.read(256, &mut actual).is_err());
        for (value, supported) in [(f64::NAN, true), (f64::INFINITY, true), (1.0, false)] {
            let arrays = storage.arrays.get_mut().unwrap();
            arrays
                .values
                .put_slice(&ArrayD::from_elem(IxDyn(&[1]), value), &[254])
                .unwrap();
            arrays
                .support
                .put_slice(&ArrayD::from_elem(IxDyn(&[1]), supported), &[254])
                .unwrap();
            let failure = storage.read(254, &mut actual[..1]).unwrap_err();
            if supported {
                assert!(matches!(failure, ModelLifecycleError::Contract(_)));
            } else {
                assert_eq!(failure, ModelLifecycleError::InvalidSupportPayload);
            }
        }
        drop(storage);
        assert!(!path.exists());
    }
}
