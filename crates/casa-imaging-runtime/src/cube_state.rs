// SPDX-License-Identifier: LGPL-3.0-or-later
//! The paged state of a channel-local run: model and normal planes in one
//! bounded block cache over files in the run's spill directory (plan
//! decision D10).

use std::{io, path::Path, sync::Arc};

use casa_imaging_reconstruction::runtime_adapter::NormalStoragePlan;
use casa_imaging_reconstruction::{ModelLifecycleError, ModelStoragePlan, SpectralOperatorError};

use crate::managed_cube_blocks::{CubeResidency, ManagedPlaneArray};
use crate::managed_model::ManagedModelFactory;
use crate::managed_normal::ManagedNormalFactory;

/// Planes of one channel-local run kept in a bounded cache and paged to
/// disk: two model generations, an invariant PSF and two residual epochs.
pub struct CubeState {
    residency: Arc<CubeResidency>,
    model: Arc<ManagedModelFactory>,
    normal: Arc<ManagedNormalFactory>,
    cells: usize,
}

impl CubeState {
    /// Cache bytes below which a run of `planes` planes of `[width, height]`
    /// cannot proceed, and above which every plane stays resident, for
    /// `workers` concurrent plane accesses.
    pub fn cache_limits(
        directory: &Path,
        shape: [usize; 2],
        planes: usize,
        workers: usize,
    ) -> io::Result<(usize, usize)> {
        let [width, height] = shape;
        let value = ManagedPlaneArray::<f32>::footprint(directory, height, width, planes)?;
        let support = ManagedPlaneArray::<bool>::footprint(directory, height, width, planes)?;
        let fixed = CubeResidency::fixed_owner_bytes();
        let owners = 5 * (value.owner_bytes + value.registry_bytes + value.staging_bytes)
            + 2 * (support.owner_bytes + support.registry_bytes + support.staging_bytes);
        let active = workers * (3 * value.block_bytes + 2 * support.block_bytes);
        let operation = CubeResidency::operation_overhead(5 * workers)?;
        let creation = value.creation_bytes.max(support.creation_bytes);
        let minimum = fixed + owners + active.max(creation) + operation;
        let payload = planes * (5 * value.block_bytes + 2 * support.block_bytes);
        Ok((minimum, (fixed + owners + payload + operation).max(minimum)))
    }

    /// A cache of `cache_bytes` over files in `directory` for `planes`
    /// planes of `[width, height]` pixels and `polarizations` per channel.
    pub fn new(
        directory: &Path,
        shape: [usize; 2],
        planes: usize,
        cache_bytes: usize,
    ) -> io::Result<Self> {
        let [width, height] = shape;
        let residency = CubeResidency::new(cache_bytes)?;
        let retention: Arc<dyn std::fmt::Debug + Send + Sync> = Arc::new(());
        Ok(Self {
            model: Arc::new(ManagedModelFactory::new(
                residency.clone(),
                retention.clone(),
                directory,
                [height, width],
                planes,
            )?),
            normal: Arc::new(ManagedNormalFactory::new(
                residency.clone(),
                retention,
                directory,
                [height, width],
            )?),
            residency,
            cells: width * height,
        })
    }

    /// Model storage reading and writing one plane at a time.
    pub fn model_storage(&self) -> Result<ModelStoragePlan, ModelLifecycleError> {
        ModelStoragePlan::new(self.model.clone(), self.cells)
    }

    /// Normal storage writing at most `window_channels` channels at a time.
    pub fn normal_storage(
        &self,
        window_channels: usize,
    ) -> Result<NormalStoragePlan, SpectralOperatorError> {
        NormalStoragePlan::new(self.normal.clone(), window_channels)
    }

    /// Bytes of plane payload resident now.
    #[must_use]
    pub fn live_payload_bytes(&self) -> usize {
        self.residency.live_payload_bytes()
    }
}

impl std::fmt::Debug for CubeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CubeState")
            .field("cells", &self.cells)
            .finish_non_exhaustive()
    }
}
