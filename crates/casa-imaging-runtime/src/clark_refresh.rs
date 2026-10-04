// SPDX-License-Identifier: LGPL-3.0-or-later

//! One synchronous library workspace, admitted for the full application lifetime.

use crate::{
    AcceleratorDemand, AcceleratorKind, AlternativeId, CacheDemand, CapabilityPredicate,
    CapacityViewId, CountDemand, DemandAlternative, DemandAlternatives, DemandEnvelope,
    IoBufferDemand, MemoryDemand, QuiescencePoint, ResourceAuthority, ResourceHeadroom,
    ResourceLease, ResourcePolicy, RuntimeOverheadDemand, ScalingMetadata,
};
use casa_imaging_reconstruction::{
    MinorCycleError,
    runtime_adapter::{ClarkRefresh, ClarkRefreshProvider},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::{Arc, Mutex},
};

#[cfg(all(target_os = "macos", not(coverage)))]
mod graph;

/// Internal run-owned convolution cache. The lease outlives all native handles.
pub struct MetalClarkRefresh {
    shape: [usize; 2],
    bytes: u64,
    #[cfg(all(target_os = "macos", not(coverage)))]
    state: Mutex<Option<graph::Workspace>>,
    _lease: ResourceLease,
}

impl MetalClarkRefresh {
    /// Reserve unified memory, accelerator and command occupancy before compiling.
    pub fn acquire(
        authority: &ResourceAuthority,
        policy: ResourcePolicy,
        shape: [usize; 2],
    ) -> io::Result<Arc<Self>> {
        if !cfg!(all(target_os = "macos", not(coverage))) {
            return Err(io::Error::other(
                "Metal Clark refresh is unavailable on this platform",
            ));
        }
        let bytes = residency_bytes(shape)?;
        let accelerator = authority
            .topology()
            .accelerators
            .iter()
            .find(|a| a.kind == AcceleratorKind::Metal)
            .ok_or_else(|| io::Error::other("no admitted Metal device"))?;
        let view = authority
            .topology()
            .memory_views
            .iter()
            .find(|v| v.id == accelerator.memory_view)
            .ok_or_else(|| io::Error::other("Metal memory view is missing"))?;
        let host = authority
            .topology()
            .memory_views
            .iter()
            .find(|v| v.id == CapacityViewId::new("host-memory"))
            .ok_or_else(|| io::Error::other("host memory view is missing"))?;
        if view.domain != host.domain {
            return Err(io::Error::other(
                "Clark refresh requires unified host/Metal memory",
            ));
        }
        let lease = authority
            .acquire(
                policy,
                DemandAlternatives {
                    required_capabilities: BTreeSet::new(),
                    alternatives: vec![DemandAlternative {
                        id: AlternativeId::new("run-clark-library-refresh"),
                        capabilities: CapabilityPredicate::default(),
                        demand: DemandEnvelope {
                            host_memory_view: host.id.clone(),
                            memory: vec![MemoryDemand {
                                allocation_id: "run-clark-library-refresh".into(),
                                hard_bytes: bytes,
                                preferred_bytes: bytes,
                                views: vec![host.id.clone(), view.id.clone()],
                            }],
                            workers: CountDemand::zero(),
                            overhead: RuntimeOverheadDemand::zero(),
                            storage: vec![],
                            rates: vec![],
                            caches: CacheDemand::zero(),
                            locks: CountDemand::zero(),
                            file_descriptors: CountDemand::zero(),
                            queues: vec![],
                            transfers: vec![],
                            accelerators: vec![AcceleratorDemand {
                                demand_id: "run-clark-library-refresh".into(),
                                accelerator: accelerator.id.clone(),
                                slots: CountDemand::new(1, 1),
                                command_queue_slots: CountDemand::new(1, 1),
                            }],
                            io_buffers: IoBufferDemand::zero(),
                        },
                        headroom: ResourceHeadroom::default(),
                        scaling: ScalingMetadata {
                            minimum_workers: 0,
                            maximum_workers: 0,
                            maximum_batch_size: 1,
                            maximum_tile_width: 1,
                            maximum_tile_height: 1,
                            maximum_slab_depth: 1,
                            memory_bytes_per_worker: BTreeMap::new(),
                        },
                        quiescence_points: BTreeSet::from([QuiescencePoint::MajorCycle]),
                    }],
                },
            )
            .map_err(io::Error::other)?;
        eprintln!(
            "clark_metal_run_workspace admitted_unified_bytes={bytes} shape={shape:?} lifetime=run CPU_refresh_scratch=replaced library_limit=observed_device_allocations"
        );
        Ok(Arc::new(Self {
            shape,
            bytes,
            #[cfg(all(target_os = "macos", not(coverage)))]
            state: Mutex::new(None),
            _lease: lease,
        }))
    }
}

// Eight full complex work fields cover a conservative library workspace envelope,
// including both compiled graphs, for any legal PSF origin. MPSGraph provides no
// preallocation hard-limit API: check device allocations at synchronous boundaries,
// and retain the independent sampled application guard, rather than claiming RSS
// proves device residency. This is a fail-closed experimental backend envelope.
fn residency_bytes(shape: [usize; 2]) -> io::Result<u64> {
    let axis = |n: usize| n.checked_mul(2).and_then(|n| n.checked_sub(1));
    let padded = axis(shape[0])
        .zip(axis(shape[1]))
        .and_then(|(x, y)| x.checked_mul(y))
        .filter(|_| !shape.contains(&0));
    let cells = shape[0].checked_mul(shape[1]);
    padded
        .zip(cells)
        .and_then(|(p, c)| {
            p.checked_mul(64)?
                .checked_add(c.checked_mul(4)?)?
                .checked_add(64 << 20)
        })
        .and_then(|b| u64::try_from(b).ok())
        .ok_or_else(|| io::Error::other("Clark library workspace overflow"))
}

impl ClarkRefreshProvider for MetalClarkRefresh {
    fn prepare<'a>(
        &'a self,
        psf: &[f32],
        shape: [usize; 2],
        center: [usize; 2],
    ) -> Result<Box<dyn ClarkRefresh + 'a>, MinorCycleError> {
        if shape != self.shape || psf.len() != shape[0] * shape[1] {
            return Err(MinorCycleError::ModelShapeMismatch);
        }
        #[cfg(all(target_os = "macos", not(coverage)))]
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| MinorCycleError::ClarkRefresh("workspace lock poisoned".into()))?;
            let padded =
                casa_imaging_reconstruction::runtime_adapter::clark_padded_shape(shape, center)?;
            if state.as_ref().is_some_and(|w| w.padded != padded) {
                state.take();
            }
            if state.is_none() {
                *state = Some(graph::Workspace::new(shape, padded, self.bytes)?);
            }
            state
                .as_mut()
                .expect("initialized workspace")
                .prepare_psf(psf, center)?;
            Ok(Box::new(graph::Session { state }))
        }
        #[cfg(not(all(target_os = "macos", not(coverage))))]
        {
            let _ = (center, self.bytes);
            Err(MinorCycleError::ClarkRefresh("unsupported platform".into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn clark_refresh_residency_scales_with_shape_and_rejects_overflow() {
        assert_eq!(
            residency_bytes([4096, 4096]).unwrap(),
            8191 * 8191 * 64 + 4096 * 4096 * 4 + (64 << 20)
        );
        assert!(residency_bytes([0, 16]).is_err());
        assert!(residency_bytes([usize::MAX, 16]).is_err());
        assert!(residency_bytes([2048, 2048]).unwrap() < residency_bytes([4096, 4096]).unwrap());
    }

    #[cfg(all(target_os = "macos", not(coverage)))]
    #[test]
    fn clark_refresh_admission_rejects_too_small_memory_before_graph_allocation() {
        let authority = ResourceAuthority::production().unwrap();
        let policy = ResourcePolicy::Explicit(crate::ResourceOverride {
            memory_bytes: BTreeMap::from([(crate::CapacityDomainId::new("host-memory"), 1 << 20)]),
            ..crate::ResourceOverride::default()
        });
        assert!(MetalClarkRefresh::acquire(authority, policy, [4096, 4096]).is_err());
    }

    #[cfg(all(target_os = "macos", not(coverage)))]
    #[test]
    fn clark_refresh_reuses_workspace_and_uses_each_supplied_psf() {
        use casa_imaging_reconstruction::runtime_adapter::CpuClarkRefresh;
        let authority = ResourceAuthority::production().unwrap();
        let owner = MetalClarkRefresh::acquire(
            authority,
            ResourcePolicy::Explicit(crate::ResourceOverride {
                memory_bytes: BTreeMap::from([(
                    crate::CapacityDomainId::new("host-memory"),
                    8 << 30,
                )]),
                ..crate::ResourceOverride::default()
            }),
            [24, 32],
        )
        .unwrap();
        let mut first_input = None;
        for cycle in 0..2 {
            let center = [3, 7];
            let mut psf = (0..24 * 32)
                .map(|i| {
                    let x = (i / 32) as f32 - center[0] as f32;
                    let y = (i % 32) as f32 - center[1] as f32;
                    (-(x * x + y * y) / 45.0).exp() + 0.03 * (x * 0.27 + y * 0.13).cos()
                })
                .collect::<Vec<_>>();
            psf[0] += cycle as f32 * 0.2;
            let mut cpu = CpuClarkRefresh::new(&psf, [24, 32], center, 1).unwrap();
            let mut metal = owner.prepare(&psf, [24, 32], center).unwrap();
            let mut expected = vec![0.0; psf.len()];
            let mut actual = expected.clone();
            for batch in 0..3 {
                for (index, flux) in [
                    (0, 0.7),
                    (31, -0.1),
                    (23 * 32, 0.3),
                    (24 * 32 - 1, -0.2),
                    (10 * 32 + 16, 0.5),
                ] {
                    cpu.add(index, flux * (batch + 1) as f64);
                    metal.add(index, flux * (batch + 1) as f64);
                }
                cpu.refresh(&mut expected).unwrap();
                metal.refresh(&mut actual).unwrap();
                let error = actual
                    .iter()
                    .zip(&expected)
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f64>();
                let power = expected.iter().map(|v| v * v).sum::<f64>();
                assert!((error / power).sqrt() <= 1e-3);
            }
            drop(metal);
            let state = owner.state.lock().unwrap();
            let input = state.as_ref().unwrap().input_identity();
            if let Some(first) = first_input {
                assert_eq!(
                    first, input,
                    "multiple solve boundaries reuse the native buffers"
                );
            } else {
                first_input = Some(input);
            }
        }
    }
}
