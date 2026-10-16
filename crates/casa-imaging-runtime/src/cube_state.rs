// SPDX-License-Identifier: LGPL-3.0-or-later
//! The paged state of a channel-local run: model and normal planes in one
//! bounded block cache over files in the run's paged-state directory (plan
//! decision D10).

use std::{io, path::Path, sync::Arc};

use casa_imaging_deconvolution::runtime_adapter::NormalStoragePlan;
use casa_imaging_deconvolution::{ModelLifecycleError, ModelStoragePlan, SpectralOperatorError};

use crate::Reservation;
use crate::managed_cube_blocks::{CubeResidency, ManagedPlaneArray};
use crate::managed_model::ManagedModelFactory;
use crate::managed_normal::ManagedNormalFactory;

/// Planes of one channel-local run kept in a bounded cache and paged to
/// disk: two model generations, the invariants (the PSF, and a mosaic's
/// weight image beside it) and two residual epochs of every image domain,
/// each domain at its own size.
pub struct CubeState {
    residency: Arc<CubeResidency>,
    model: Arc<ManagedModelFactory>,
    normal: Arc<ManagedNormalFactory>,
    window_samples: usize,
}

/// Payload, fixed and per-access bytes of the paged state of `domains`.
struct Footprint {
    /// Bytes below which no plane access can proceed.
    minimum: usize,
    /// Cache bytes of every block of every array.
    payload: usize,
    /// Bytes the cache needs beyond its payload.
    overhead: usize,
    /// File bytes of every array once every block is paged out.
    disk: usize,
}

fn footprint(
    directory: &Path,
    domains: &[[usize; 2]],
    planes: usize,
    dense_sensitivity: bool,
    workers: usize,
) -> io::Result<Footprint> {
    // The invariant array holds the PSF's planes, and with a dense
    // sensitivity the weight image's as many again.
    let invariant_planes = planes * (1 + usize::from(dense_sensitivity));
    let mut owners = 0;
    let mut active = 0;
    let mut creation = 0;
    let mut payload = 0;
    let mut disk = 0;
    for &[width, height] in domains {
        let value = ManagedPlaneArray::<f32>::footprint(directory, height, width, planes)?;
        let invariant =
            ManagedPlaneArray::<f32>::footprint(directory, height, width, invariant_planes)?;
        let support = ManagedPlaneArray::<bool>::footprint(directory, height, width, planes)?;
        owners += 4 * (value.owner_bytes + value.registry_bytes + value.staging_bytes)
            + invariant.owner_bytes
            + invariant.registry_bytes
            + invariant.staging_bytes
            + 2 * (support.owner_bytes + support.registry_bytes + support.staging_bytes);
        // A plane access pins one field's block at a time.
        active = usize::max(
            active,
            workers * (3 * value.block_bytes + 2 * support.block_bytes),
        );
        creation = usize::max(
            creation,
            value
                .creation_bytes
                .max(invariant.creation_bytes)
                .max(support.creation_bytes),
        );
        payload += planes * (4 * value.block_bytes + 2 * support.block_bytes)
            + invariant_planes * invariant.block_bytes;
        disk += 4 * value.storage_bytes + invariant.storage_bytes + 2 * support.storage_bytes;
    }
    let operation = CubeResidency::operation_overhead(5 * workers)?;
    let overhead = CubeResidency::fixed_owner_bytes() + owners + operation;
    Ok(Footprint {
        minimum: overhead + active.max(creation),
        payload,
        overhead,
        disk,
    })
}

impl CubeState {
    /// Cache bytes below which a run of `planes` planes of each
    /// `[width, height]` in `domains` cannot proceed, and above which every
    /// plane stays resident, for `workers` concurrent plane accesses.
    /// `dense_sensitivity` keeps each plane's weight image beside its PSF.
    pub fn cache_limits(
        directory: &Path,
        domains: &[[usize; 2]],
        planes: usize,
        dense_sensitivity: bool,
        workers: usize,
    ) -> io::Result<(usize, usize)> {
        let footprint = footprint(directory, domains, planes, dense_sensitivity, workers)?;
        Ok((
            footprint.minimum,
            (footprint.overhead + footprint.payload).max(footprint.minimum),
        ))
    }

    /// A cache of the bytes `cache` holds over files in `directory` for
    /// `planes` planes of each `[width, height]` in `domains` (each domain's
    /// planes are its channels × polarizations), with each plane's weight
    /// image too when `dense_sensitivity`.
    ///
    /// The reservation is released when the last storage this state hands
    /// out drops, so a completed run's model and normal state stay charged
    /// for as long as anything holds them.
    ///
    /// Every plane may be paged out, so the directory's file system must have
    /// room for the whole payload; otherwise the error is
    /// [`io::ErrorKind::StorageFull`].
    pub fn new(
        directory: &Path,
        domains: &[[usize; 2]],
        planes: usize,
        dense_sensitivity: bool,
        cache: Reservation,
    ) -> io::Result<Self> {
        let cache_bytes = usize::try_from(cache.memory())
            .map_err(|_| io::Error::other("the cube cache exceeds the address space"))?;
        let disk = footprint(directory, domains, planes, dense_sensitivity, 1)?.disk;
        let available = fs2::available_space(directory)?;
        if available < disk as u64 {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                format!(
                    "the paged cube state needs {disk} bytes but {} has {available} free",
                    directory.display()
                ),
            ));
        }
        let axes = domains
            .iter()
            .map(|[width, height]| [*height, *width])
            .collect::<Vec<_>>();
        let residency = CubeResidency::new(cache_bytes)?;
        let retention: Arc<dyn std::fmt::Debug + Send + Sync> = Arc::new(cache);
        Ok(Self {
            model: Arc::new(ManagedModelFactory::new(
                residency.clone(),
                retention.clone(),
                directory,
                &axes,
                planes,
            )?),
            normal: Arc::new(ManagedNormalFactory::new(
                residency.clone(),
                retention,
                directory,
                &axes,
            )?),
            residency,
            window_samples: domains
                .iter()
                .map(|[width, height]| width * height)
                .max()
                .unwrap_or(0),
        })
    }

    /// Model storage reading and writing at most one plane of the largest
    /// domain at a time.
    pub fn model_storage(&self) -> Result<ModelStoragePlan, ModelLifecycleError> {
        ModelStoragePlan::new(self.model.clone(), self.window_samples)
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
            .field("window_samples", &self.window_samples)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A mosaic cube keeps each plane's weight image beside its PSF: the
    /// invariant array holds twice the planes, so the cache payload grows by
    /// one block per plane and the files by as much again.
    #[test]
    fn a_dense_sensitivity_charges_a_second_invariant_plane_per_plane() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let (domains, planes) = ([[64, 32]], 6);
        let scalar = footprint(directory.path(), &domains, planes, false, 2).unwrap();
        let dense = footprint(directory.path(), &domains, planes, true, 2).unwrap();
        let plane = ManagedPlaneArray::<f32>::footprint(directory.path(), 32, 64, planes).unwrap();
        assert_eq!(dense.payload - scalar.payload, planes * plane.block_bytes);
        assert!(dense.disk > scalar.disk);
        assert!(dense.overhead >= scalar.overhead);
        assert!(dense.minimum >= scalar.minimum);
    }
}
