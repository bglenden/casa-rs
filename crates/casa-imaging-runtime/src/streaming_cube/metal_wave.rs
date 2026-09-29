// SPDX-License-Identifier: LGPL-3.0-or-later

//! Resident GPU grids consume the same borrowed source refills as CPU waves.

use super::bulk_wave::BulkWave;
use crate::{
    AllocationId,
    bounded_stream::BoundedExecution,
    metal_runtime::{CubeDispatch, CubeDispatchKind, MetalBatchAccess, MetalBufferRegion},
    weighting::bulk_source::BulkConsumer,
};
use casa_imaging_reconstruction::{
    ModelGeneration, PolarizationOperator, SpectralOperatorError,
    runtime_adapter::{
        BandPlan, BandResult, CubeSpatialBackend, EpochBand, NativeBlockView, NativeLayout,
        SpatialField, SpatialGridBatch, SpatialPredictionBatch, SpatialTap,
    },
};
use num_complex::Complex32;
use std::io;

fn overflow() -> io::Error {
    io::Error::other("Metal cube workspace overflow")
}

pub(super) struct MetalWaveMemory {
    pub cpu: u64,
    pub device: u64,
    pub scratch: u64,
    pub requests: usize,
}

pub(super) enum WaveConsumer<'a> {
    Cpu(BulkWave<'a>),
    Metal(MetalWave<'a>),
}

impl BulkConsumer for WaveConsumer<'_> {
    type Completion = Vec<BandResult>;
    fn consume(
        &mut self,
        block: NativeBlockView<'_>,
        layout: &NativeLayout,
        selected: std::ops::Range<usize>,
        execution: BoundedExecution<'_>,
    ) -> io::Result<()> {
        match self {
            Self::Cpu(wave) => wave.consume(block, layout, selected, execution),
            Self::Metal(wave) => wave.consume(block, layout, selected, execution),
        }
    }
    fn complete(self, execution: BoundedExecution<'_>) -> io::Result<Vec<BandResult>> {
        match self {
            Self::Cpu(wave) => wave.complete(execution),
            Self::Metal(wave) => wave.complete(execution),
        }
    }
}

impl MetalWaveMemory {
    pub(super) fn new(bands: &[BandPlan], workers: usize, rows: usize) -> io::Result<Self> {
        let cpu = BulkWave::bytes(bands, workers)?;
        let grids = bands.iter().try_fold(0_u64, |sum, band| {
            sum.checked_add(band.spatial_grid_bytes().map_err(io::Error::other)? as u64)
                .ok_or_else(overflow)
        })?;
        let requests = bands
            .iter()
            .map(|b| b.spatial_request_capacity(rows))
            .collect::<Result<Vec<_>, _>>()
            .map_err(io::Error::other)?
            .into_iter()
            .max()
            .unwrap_or(1);
        let scratch = bands
            .iter()
            .map(|b| {
                b.spatial_host_bytes(rows)?
                    .checked_add(
                        b.spatial_command_capacity()
                            * (size_of::<DispatchTarget<'_>>() + size_of::<CubeDispatch<'_>>()),
                    )
                    .ok_or(SpectralOperatorError::ResidencyOverflow)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(io::Error::other)?
            .into_iter()
            .max()
            .unwrap_or(0) as u64;
        let lanes = workers.min(bands.len()) as u64;
        let scratch = scratch.checked_mul(lanes).ok_or_else(overflow)?;
        let device = grids
            .checked_add(BandPlan::spatial_weight_bytes().next_multiple_of(8) as u64)
            .and_then(|n| {
                n.checked_add(
                    (requests as u64)
                        .checked_mul((size_of::<SpatialTap>() + size_of::<Complex32>()) as u64)?
                        .checked_mul(lanes)?,
                )
            })
            .ok_or_else(overflow)?;
        Ok(Self {
            cpu,
            device,
            scratch,
            requests,
        })
    }
    pub(super) fn total(&self) -> io::Result<u64> {
        self.cpu
            .checked_add(self.device)
            .and_then(|n| n.checked_add(self.scratch))
            .ok_or_else(overflow)
    }
    pub(super) fn prefix(
        bands: &[BandPlan],
        budget: u64,
        workers: usize,
        rows: usize,
    ) -> io::Result<usize> {
        let mut count = 0;
        for end in 1..=bands.len() {
            if Self::new(&bands[..end], workers, rows)?.total()? > budget {
                break;
            }
            count = end;
        }
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "Metal wave cannot fit one output band",
            ));
        }
        Ok(count)
    }
}

struct Job<'a> {
    plan: Option<BandPlan>,
    band: Option<EpochBand<'a>>,
    grids: usize,
    fields: [usize; 4],
    shape: [usize; 2],
}

pub(super) struct MetalWave<'a> {
    jobs: Vec<Job<'a>>,
    model: &'a ModelGeneration,
    output_hz: &'a [f64],
    polarization: &'a PolarizationOperator,
    discover: Option<&'a mut [BandPlan]>,
    access: MetalBatchAccess<'a>,
    allocation: &'a AllocationId,
    weights: usize,
    packed: usize,
    predicted: usize,
    capacity: usize,
    staging_stride: usize,
}

impl<'a> MetalWave<'a> {
    pub(super) fn new(
        jobs: Vec<BandPlan>,
        model: &'a ModelGeneration,
        output_hz: &'a [f64],
        polarization: &'a PolarizationOperator,
        discover: Option<&'a mut [BandPlan]>,
        access: MetalBatchAccess<'a>,
        allocation: &'a AllocationId,
        rows: usize,
        workers: usize,
    ) -> io::Result<Self> {
        let memory = MetalWaveMemory::new(&jobs, workers, rows)?;
        let weights = jobs.iter().try_fold(0_usize, |sum, b| {
            sum.checked_add(b.spatial_grid_bytes().map_err(io::Error::other)?)
                .ok_or_else(overflow)
        })?;
        let packed = weights
            .checked_add(BandPlan::spatial_weight_bytes().next_multiple_of(8))
            .ok_or_else(overflow)?;
        let predicted = packed
            .checked_add(
                memory
                    .requests
                    .checked_mul(size_of::<SpatialTap>())
                    .ok_or_else(overflow)?,
            )
            .ok_or_else(overflow)?;
        let mut grids = 0;
        let jobs = jobs
            .into_iter()
            .map(|plan| {
                let offset = grids;
                grids += plan.spatial_grid_bytes().expect("checked wave grids");
                Job {
                    plan: Some(plan),
                    band: None,
                    grids: offset,
                    fields: [0; 4],
                    shape: [0; 2],
                }
            })
            .collect();
        Ok(Self {
            jobs,
            model,
            output_hz,
            polarization,
            discover,
            access,
            allocation,
            weights,
            packed,
            predicted,
            capacity: memory.requests,
            staging_stride: memory
                .requests
                .checked_mul(size_of::<SpatialTap>() + size_of::<Complex32>())
                .ok_or_else(overflow)?,
        })
    }
}

struct ResidentBand<'a, 'r> {
    access: &'a MetalBatchAccess<'r>,
    allocation: &'a AllocationId,
    grids: usize,
    fields: [usize; 4],
    shape: [usize; 2],
    weights: usize,
    packed: usize,
    predicted: usize,
    capacity: usize,
}

struct DispatchTarget<'a> {
    field: SpatialField,
    plane: usize,
    taps: &'a [SpatialTap],
}

impl ResidentBand<'_, '_> {
    fn region(&self, offset: usize, bytes: usize) -> MetalBufferRegion<'_> {
        MetalBufferRegion {
            allocation: self.allocation,
            offset,
            bytes,
        }
    }
    fn field_region(
        &self,
        field: SpatialField,
        plane: usize,
    ) -> Result<MetalBufferRegion<'_>, SpectralOperatorError> {
        let lane = match field {
            SpatialField::Dirty => 0,
            SpatialField::Residual => 1,
            SpatialField::Psf => 2,
            SpatialField::Model => 3,
        };
        if plane >= self.fields[lane] {
            return Err(SpectralOperatorError::InvalidSlab);
        }
        let bytes = self.shape[0] * self.shape[1] * size_of::<Complex32>();
        let index = self.fields[..lane].iter().sum::<usize>() + plane;
        Ok(self.region(self.grids + index * bytes, bytes))
    }
    fn dispatch(
        &self,
        targets: &[DispatchTarget<'_>],
        degrid: bool,
    ) -> Result<(), SpectralOperatorError> {
        let count = targets
            .iter()
            .try_fold(0_usize, |sum, t| sum.checked_add(t.taps.len()))
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        if count > self.capacity {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        if count == 0 {
            return Ok(());
        }
        let samples = self.region(self.packed, count * size_of::<SpatialTap>());
        self.access
            .with_bytes(samples, |bytes| {
                let mut offset = 0;
                for target in targets {
                    let input = bytemuck::cast_slice(target.taps);
                    bytes[offset..offset + input.len()].copy_from_slice(input);
                    offset += input.len();
                }
            })
            .map_err(spatial_error)?;
        let mut offset = 0;
        let mut commands = Vec::with_capacity(targets.len());
        for target in targets.iter().filter(|t| !t.taps.is_empty()) {
            commands.push(CubeDispatch {
                kind: if degrid {
                    CubeDispatchKind::Degrid {
                        predicted: self.region(
                            self.predicted + offset * size_of::<Complex32>(),
                            target.taps.len() * size_of::<Complex32>(),
                        ),
                    }
                } else {
                    CubeDispatchKind::Grid
                },
                samples: self.region(
                    self.packed + offset * size_of::<SpatialTap>(),
                    size_of_val(target.taps),
                ),
                weights: self.region(self.weights, BandPlan::spatial_weight_bytes()),
                grid: self.field_region(target.field, target.plane)?,
                count: target
                    .taps
                    .len()
                    .try_into()
                    .map_err(|_| SpectralOperatorError::ResidencyOverflow)?,
                width: self.shape[0]
                    .try_into()
                    .map_err(|_| SpectralOperatorError::ResidencyOverflow)?,
                height: self.shape[1]
                    .try_into()
                    .map_err(|_| SpectralOperatorError::ResidencyOverflow)?,
            });
            offset += target.taps.len();
        }
        self.access.execute(&commands).map_err(spatial_error)?;
        Ok(())
    }
}

fn spatial_error(error: crate::MetalRuntimeError) -> SpectralOperatorError {
    SpectralOperatorError::SpatialExecution(error.to_string())
}

impl CubeSpatialBackend for ResidentBand<'_, '_> {
    fn initialize(
        &mut self,
        shape: [usize; 2],
        weights: &[[f32; 7]],
        fields: [usize; 4],
        model: &[Complex32],
    ) -> Result<(), SpectralOperatorError> {
        self.shape = shape;
        self.fields = fields;
        self.access
            .with_bytes(self.region(self.weights, size_of_val(weights)), |bytes| {
                bytes.copy_from_slice(bytemuck::cast_slice(weights))
            })
            .map_err(spatial_error)?;
        let cells = shape[0] * shape[1];
        let outputs = fields[..3].iter().sum::<usize>();
        if outputs > 0 {
            self.access
                .with_bytes(self.region(self.grids, outputs * cells * 8), |bytes| {
                    bytes.fill(0)
                })
                .map_err(spatial_error)?;
        }
        if !model.is_empty() {
            self.access
                .with_bytes(
                    self.region(self.grids + outputs * cells * 8, size_of_val(model)),
                    |bytes| bytes.copy_from_slice(bytemuck::cast_slice(model)),
                )
                .map_err(spatial_error)?;
        }
        Ok(())
    }
    fn degrid(
        &mut self,
        batches: &mut [SpatialPredictionBatch],
    ) -> Result<(), SpectralOperatorError> {
        let targets: Vec<_> = batches
            .iter()
            .map(|b| DispatchTarget {
                field: SpatialField::Model,
                plane: b.plane,
                taps: &b.taps,
            })
            .collect();
        self.dispatch(&targets, true)?;
        let mut offset = 0;
        for batch in batches.iter_mut().filter(|b| !b.taps.is_empty()) {
            self.access
                .with_bytes(
                    self.region(
                        self.predicted + offset * size_of::<Complex32>(),
                        size_of_val(batch.values.as_slice()),
                    ),
                    |bytes| {
                        bytemuck::cast_slice_mut::<Complex32, u8>(&mut batch.values)
                            .copy_from_slice(bytes);
                    },
                )
                .map_err(spatial_error)?;
            offset += batch.taps.len();
        }
        Ok(())
    }
    fn grid(&mut self, batches: &[SpatialGridBatch]) -> Result<(), SpectralOperatorError> {
        let targets: Vec<_> = batches
            .iter()
            .map(|b| DispatchTarget {
                field: b.field,
                plane: b.plane,
                taps: &b.taps,
            })
            .collect();
        self.dispatch(&targets, false)
    }
    fn download(
        &mut self,
        field: SpatialField,
        plane: usize,
        values: &mut [Complex32],
    ) -> Result<(), SpectralOperatorError> {
        self.access
            .with_bytes(self.field_region(field, plane)?, |bytes| {
                bytemuck::cast_slice_mut::<Complex32, u8>(values).copy_from_slice(bytes)
            })
            .map_err(spatial_error)
    }
}

impl BulkConsumer for MetalWave<'_> {
    type Completion = Vec<BandResult>;
    fn consume(
        &mut self,
        block: NativeBlockView<'_>,
        layout: &NativeLayout,
        selected: std::ops::Range<usize>,
        execution: BoundedExecution<'_>,
    ) -> io::Result<()> {
        if let Some(bands) = self.discover.as_deref_mut() {
            BandPlan::observe_borrowed(bands, block, self.output_hz).map_err(io::Error::other)?;
        }
        let model = self.model;
        execution.for_each_mut(&mut self.jobs, |_, job| {
            if let Some(plan) = job.plan.take() {
                job.band = Some(plan.prepare(model, None).map_err(io::Error::other)?);
            }
            Ok::<_, io::Error>(())
        })?;
        let access = &self.access;
        let allocation = self.allocation;
        let output_hz = self.output_hz;
        let polarization = self.polarization;
        let weights = self.weights;
        let packed = self.packed;
        let predicted = self.predicted;
        let capacity = self.capacity;
        let staging_stride = self.staging_stride;
        let chunk = self.jobs.len().div_ceil(execution.worker_count());
        let mut lanes: Vec<_> = self.jobs.chunks_mut(chunk).collect();
        execution.for_each_mut(&mut lanes, |lane, jobs| {
            for job in jobs.iter_mut() {
                let mut backend = ResidentBand {
                    access,
                    allocation,
                    grids: job.grids,
                    fields: job.fields,
                    shape: job.shape,
                    weights,
                    packed: packed + lane * staging_stride,
                    predicted: predicted + lane * staging_stride,
                    capacity,
                };
                let band = job.band.as_mut().expect("prepared band");
                if job.shape == [0; 2] {
                    band.initialize_spatial(&mut backend)
                        .map_err(io::Error::other)?;
                    job.shape = backend.shape;
                    job.fields = backend.fields;
                }
                band.consume_source_window_spatial(
                    block,
                    layout,
                    selected.clone(),
                    output_hz,
                    polarization,
                    &mut backend,
                )
                .map_err(io::Error::other)?;
            }
            Ok::<_, io::Error>(())
        })
    }
    fn complete(mut self, execution: BoundedExecution<'_>) -> io::Result<Vec<BandResult>> {
        for job in &mut self.jobs {
            if let Some(plan) = job.plan.take() {
                job.band = Some(plan.prepare(self.model, None).map_err(io::Error::other)?);
            }
            let mut backend = ResidentBand {
                access: &self.access,
                allocation: self.allocation,
                grids: job.grids,
                fields: job.fields,
                shape: job.shape,
                weights: self.weights,
                packed: self.packed,
                predicted: self.predicted,
                capacity: self.capacity,
            };
            let band = job.band.as_mut().expect("prepared band");
            if job.shape == [0; 2] {
                band.initialize_spatial(&mut backend)
                    .map_err(io::Error::other)?;
            }
            band.download_spatial(&mut backend)
                .map_err(io::Error::other)?;
        }
        let mut results: Vec<Option<BandResult>> = (0..self.jobs.len()).map(|_| None).collect();
        let mut jobs: Vec<_> = self.jobs.into_iter().zip(results.iter_mut()).collect();
        let model = self.model;
        execution.for_each_mut(&mut jobs, |_, (job, result)| {
            **result = Some(
                job.band
                    .take()
                    .expect("active band")
                    .complete(model)
                    .map_err(io::Error::other)?
                    .0,
            );
            Ok::<_, io::Error>(())
        })?;
        Ok(results.into_iter().map(Option::unwrap).collect())
    }
}
