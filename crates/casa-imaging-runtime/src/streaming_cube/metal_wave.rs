// SPDX-License-Identifier: LGPL-3.0-or-later

//! Resident GPU grids consume the same borrowed source refills as CPU waves.

use super::bulk_wave::BulkWave;
use crate::{
    AllocationId,
    bounded_stream::BoundedExecution,
    metal_runtime::{
        CubeDispatch, CubeDispatchKind, CubeResidualDispatch, MetalBatchAccess, MetalBufferRegion,
    },
    weighting::bulk_source::BulkConsumer,
};
use casa_imaging_reconstruction::{
    ModelGeneration, PolarizationOperator, SpectralOperatorError,
    runtime_adapter::{
        BandPlan, BandResult, CubeSpatialBackend, DeviceCorrelations, EpochBand, NativeBlockView,
        NativeLayout, NativePrediction, ResidualPrediction, ResidualRefill, ResidualSample,
        SpatialField, SpatialGridBatch, SpatialPredictionBatch, SpatialTap,
    },
};
use num_complex::Complex32;
use std::io;
use std::time::Instant;

fn overflow() -> io::Error {
    io::Error::other("Metal cube workspace overflow")
}

pub(super) struct MetalWaveMemory {
    pub cpu: u64,
    pub device: u64,
    pub scratch: u64,
    pub requests: usize,
}

// One admitted wave owns this inline state, independently of its grid payloads.
#[allow(clippy::large_enum_variant)]
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
        if bands.first().is_some_and(BandPlan::is_residual) {
            let wave = BandPlan::residual_wave(bands).map_err(io::Error::other)?;
            let layout = ResidualLayout::new(&wave, rows)?;
            let device = wave
                .spatial_grid_bytes()
                .map_err(io::Error::other)?
                .checked_add(BandPlan::spatial_weight_bytes().next_multiple_of(8))
                .and_then(|bytes| bytes.checked_add(layout.slot_bytes.checked_mul(2)?))
                .and_then(|bytes| bytes.checked_add(layout.capacities[0].max(1).checked_mul(8)?))
                .ok_or_else(overflow)?;
            let scratch = wave.spatial_host_bytes(1).map_err(io::Error::other)?;
            return Ok(Self {
                cpu: BulkWave::bytes(std::slice::from_ref(&wave), 1)?,
                device: device as u64,
                scratch: scratch as u64,
                requests: layout.capacities[0],
            });
        }
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
        let layout = InitialLayout::new(bands, rows)?;
        let command_bytes = layout
            .command_capacity
            .checked_mul(
                2 * size_of::<CubeDispatch<'_>>()
                    + 4 * size_of::<MetalBufferRegion<'_>>()
                    + 8 * size_of::<(AllocationId, usize, usize)>(),
            )
            .ok_or_else(overflow)?;
        let scratch = scratch
            .checked_mul(lanes)
            .and_then(|n| n.checked_add(command_bytes as u64))
            .and_then(|n| n.checked_add((layout.offsets.len() * 2 * size_of::<usize>()) as u64))
            .ok_or_else(overflow)?;
        let device = grids
            .checked_add(BandPlan::spatial_weight_bytes().next_multiple_of(8) as u64)
            .and_then(|n| {
                n.checked_add(
                    (layout.slot_bytes as u64).checked_mul(2)?.checked_add(
                        (requests as u64)
                            .checked_mul(size_of::<Complex32>() as u64)?
                            .checked_mul(lanes)?,
                    )?,
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
    residual: Option<ResidualDevice>,
    initial: Option<InitialDevice<'a>>,
}

struct InitialLayout {
    offsets: Vec<usize>,
    capacities: Vec<usize>,
    slot_bytes: usize,
    command_capacity: usize,
}

impl InitialLayout {
    fn new(bands: &[BandPlan], rows: usize) -> io::Result<Self> {
        let mut offsets = Vec::with_capacity(bands.len());
        let mut capacities = Vec::with_capacity(bands.len());
        let mut slot_bytes = 0_usize;
        let mut command_capacity = 0_usize;
        for band in bands {
            let capacity = band
                .spatial_request_capacity(rows)
                .map_err(io::Error::other)?;
            offsets.push(slot_bytes);
            capacities.push(capacity);
            slot_bytes = slot_bytes
                .checked_add(
                    capacity
                        .checked_mul(size_of::<SpatialTap>())
                        .ok_or_else(overflow)?,
                )
                .ok_or_else(overflow)?;
            command_capacity = command_capacity
                .checked_add(band.spatial_command_capacity())
                .ok_or_else(overflow)?;
        }
        Ok(Self {
            offsets,
            capacities,
            slot_bytes,
            command_capacity,
        })
    }
}

struct InitialDevice<'a> {
    layout: InitialLayout,
    commands: Vec<Vec<CubeDispatch<'a>>>,
    batch: Vec<CubeDispatch<'a>>,
    tickets: [Option<u64>; 2],
    next_slot: usize,
    predictions: bool,
    refills: u64,
    grid_samples: u64,
}

struct ResidualLayout {
    capacities: [usize; 3],
    offsets: [usize; 7],
    slot_bytes: usize,
    descriptor_bytes: usize,
}

impl ResidualLayout {
    fn new(wave: &BandPlan, rows: usize) -> io::Result<Self> {
        let capacities = wave.residual_capacities(rows).map_err(io::Error::other)?;
        let [predictions, native, fine] = capacities;
        // At most four correlations per Stokes-I input. The source retains its
        // actual correlation count; unused staging capacity is never read.
        let sizes = [
            predictions
                .max(1)
                .checked_mul(size_of::<ResidualPrediction>()),
            native.max(1).checked_mul(size_of::<NativePrediction>()),
            fine.max(1).checked_mul(size_of::<ResidualSample>()),
            native.max(1).checked_mul(4 * 8),
            native.max(1).checked_mul(4 * 4),
            native.max(1).checked_mul(4),
            Some(8),
        ];
        let mut offsets = [0; 7];
        let mut bytes = 0_usize;
        for (i, size) in sizes.iter().enumerate() {
            offsets[i] = bytes;
            bytes = bytes
                .checked_add(
                    size.ok_or_else(overflow)?
                        .checked_next_multiple_of(8)
                        .ok_or_else(overflow)?,
                )
                .ok_or_else(overflow)?;
        }
        Ok(Self {
            capacities,
            offsets,
            slot_bytes: bytes,
            descriptor_bytes: offsets[3],
        })
    }
    fn length(&self, index: usize) -> usize {
        self.offsets
            .get(index + 1)
            .copied()
            .unwrap_or(self.slot_bytes)
            - self.offsets[index]
    }
}

struct ResidualDevice {
    layout: ResidualLayout,
    tickets: [Option<u64>; 2],
    next_slot: usize,
    requested: u64,
    unique: u64,
    staging_bytes: u64,
    refills: u64,
    profile: bool,
    host_seconds: [f64; 6],
    staged_bytes: [u64; 6],
    cleared_bytes: u64,
}

impl<'a> MetalWave<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        mut jobs: Vec<BandPlan>,
        model: &'a ModelGeneration,
        output_hz: &'a [f64],
        polarization: &'a PolarizationOperator,
        discover: Option<&'a mut [BandPlan]>,
        access: MetalBatchAccess<'a>,
        allocation: &'a AllocationId,
        rows: usize,
        workers: usize,
    ) -> io::Result<Self> {
        let residual = if jobs.first().is_some_and(BandPlan::is_residual) {
            let wave = BandPlan::residual_wave(&jobs).map_err(io::Error::other)?;
            let layout = ResidualLayout::new(&wave, rows)?;
            jobs = vec![wave];
            Some(ResidualDevice {
                layout,
                tickets: [None; 2],
                next_slot: 0,
                requested: 0,
                unique: 0,
                staging_bytes: 0,
                refills: 0,
                profile: std::env::var_os("CASA_RS_PROFILE_CUBE").is_some(),
                host_seconds: [0.0; 6],
                staged_bytes: [0; 6],
                cleared_bytes: 0,
            })
        } else {
            None
        };
        let memory = MetalWaveMemory::new(&jobs, workers, rows)?;
        let initial = if residual.is_none() {
            let layout = InitialLayout::new(&jobs, rows)?;
            let chunk = jobs.len().div_ceil(workers);
            let commands = jobs
                .chunks(chunk)
                .map(|bands| {
                    Vec::with_capacity(bands.iter().map(BandPlan::spatial_command_capacity).sum())
                })
                .collect();
            let batch = Vec::with_capacity(layout.command_capacity);
            Some(InitialDevice {
                layout,
                commands,
                batch,
                tickets: [None; 2],
                next_slot: 0,
                predictions: jobs.iter().any(|band| !band.is_initial_zero()),
                refills: 0,
                grid_samples: 0,
            })
        } else {
            None
        };
        let weights = jobs.iter().try_fold(0_usize, |sum, b| {
            sum.checked_add(b.spatial_grid_bytes().map_err(io::Error::other)?)
                .ok_or_else(overflow)
        })?;
        let packed = weights
            .checked_add(BandPlan::spatial_weight_bytes().next_multiple_of(8))
            .ok_or_else(overflow)?;
        let predicted = packed
            .checked_add(if let Some(residual) = &residual {
                residual
                    .layout
                    .slot_bytes
                    .checked_mul(2)
                    .ok_or_else(overflow)?
            } else {
                initial
                    .as_ref()
                    .expect("initial wave layout")
                    .layout
                    .slot_bytes
                    .checked_mul(2)
                    .ok_or_else(overflow)?
            })
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
                .checked_mul(size_of::<Complex32>())
                .ok_or_else(overflow)?,
            residual,
            initial,
        })
    }
}

struct ResidentBand<'a, 'r, 'm> {
    access: &'a MetalBatchAccess<'r>,
    allocation: &'m AllocationId,
    grids: usize,
    fields: [usize; 4],
    shape: [usize; 2],
    weights: usize,
    packed: usize,
    predicted: usize,
    capacity: usize,
    queued: Option<&'a mut Vec<CubeDispatch<'m>>>,
}

struct DispatchTarget<'a> {
    field: SpatialField,
    plane: usize,
    taps: &'a [SpatialTap],
}

impl<'m> ResidentBand<'_, '_, 'm> {
    fn region(&self, offset: usize, bytes: usize) -> MetalBufferRegion<'m> {
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
    ) -> Result<MetalBufferRegion<'m>, SpectralOperatorError> {
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
        &mut self,
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
        if !degrid && let Some(queued) = self.queued.as_deref_mut() {
            queued.extend(commands);
        } else {
            self.access.execute(&commands).map_err(spatial_error)?;
        }
        Ok(())
    }
}

fn spatial_error(error: crate::MetalRuntimeError) -> SpectralOperatorError {
    SpectralOperatorError::SpatialExecution(error.to_string())
}

impl CubeSpatialBackend for ResidentBand<'_, '_, '_> {
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
        if self.residual.is_some() {
            return self.consume_residual(block, layout, selected);
        }
        let access = &self.access;
        let allocation = self.allocation;
        let output_hz = self.output_hz;
        let polarization = self.polarization;
        let weights = self.weights;
        let predicted = self.predicted;
        let staging_stride = self.staging_stride;
        let initial = self.initial.as_mut().expect("initial device state");
        let slot = initial.next_slot;
        // A nonempty initial model needs synchronous host prediction readback.
        // Settle its queued grids before that dependency, never reuse stale tickets.
        if initial.predictions {
            for ticket in &mut initial.tickets {
                if let Some(ticket) = ticket.take() {
                    access.wait(ticket).map_err(io::Error::other)?;
                }
            }
        } else if let Some(ticket) = initial.tickets[slot].take() {
            access.wait(ticket).map_err(io::Error::other)?;
        }
        let packed = self.packed + slot * initial.layout.slot_bytes;
        let offsets = &initial.layout.offsets;
        let capacities = &initial.layout.capacities;
        for commands in &mut initial.commands {
            commands.clear();
        }
        let chunk = self.jobs.len().div_ceil(execution.worker_count());
        let mut lanes: Vec<_> = self
            .jobs
            .chunks_mut(chunk)
            .zip(initial.commands.iter_mut())
            .enumerate()
            .collect();
        execution.for_each_mut(&mut lanes, |_, (lane, (jobs, commands))| {
            for (index, job) in jobs.iter_mut().enumerate() {
                let ordinal = *lane * chunk + index;
                let mut backend = ResidentBand {
                    access,
                    allocation,
                    grids: job.grids,
                    fields: job.fields,
                    shape: job.shape,
                    weights,
                    packed: packed + offsets[ordinal],
                    predicted: predicted + *lane * staging_stride,
                    capacity: capacities[ordinal],
                    queued: Some(&mut **commands),
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
        })?;
        drop(lanes);
        initial.batch.clear();
        for commands in &initial.commands {
            initial.batch.extend_from_slice(commands);
        }
        if !initial.batch.is_empty() {
            let ticket = access.submit(&initial.batch).map_err(io::Error::other)?;
            initial.tickets[slot] = Some(ticket);
            initial.next_slot = (slot + 1) % 2;
            initial.refills += 1;
            initial.grid_samples += initial
                .batch
                .iter()
                .map(|dispatch| u64::from(dispatch.count))
                .sum::<u64>();
        }
        Ok(())
    }
    fn complete(mut self, execution: BoundedExecution<'_>) -> io::Result<Vec<BandResult>> {
        let drain_started = self
            .residual
            .as_ref()
            .is_some_and(|state| state.profile)
            .then(Instant::now);
        self.access.drain().map_err(io::Error::other)?;
        let drain_seconds = drain_started.map_or(0.0, |started| started.elapsed().as_secs_f64());
        if let Some(initial) = &self.initial {
            eprintln!(
                "cube device initial: refills={} grid_samples={} grid_staging_bytes={} slots=2",
                initial.refills,
                initial.grid_samples,
                initial.grid_samples * size_of::<SpatialTap>() as u64
            );
        }
        if let Some(residual) = &self.residual {
            eprintln!(
                "cube device residual: refills={} requested_predictions={} unique_predictions={} staging_bytes={} host_prediction_bytes=0 slots=2",
                residual.refills, residual.requested, residual.unique, residual.staging_bytes
            );
            if residual.profile {
                eprintln!(
                    "cube residual host profile: refills={} slot_wait_seconds={:.6} prepare_seconds={:.6} copy_seconds={:.6} flag_pack_seconds={:.6} clear_seconds={:.6} submit_seconds={:.6} drain_seconds={:.6} prediction_bytes={} native_bytes={} sample_bytes={} value_bytes={} weight_bytes={} flag_bytes={} cleared_bytes={} descriptor_copy_bytes=0",
                    residual.refills,
                    residual.host_seconds[0],
                    residual.host_seconds[1],
                    residual.host_seconds[2],
                    residual.host_seconds[3],
                    residual.host_seconds[4],
                    residual.host_seconds[5],
                    drain_seconds,
                    residual.staged_bytes[0],
                    residual.staged_bytes[1],
                    residual.staged_bytes[2],
                    residual.staged_bytes[3],
                    residual.staged_bytes[4],
                    residual.staged_bytes[5],
                    residual.cleared_bytes,
                );
            }
        }
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
                queued: None,
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
        let mut jobs: Vec<_> = std::mem::take(&mut self.jobs)
            .into_iter()
            .zip(results.iter_mut())
            .collect();
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

impl MetalWave<'_> {
    fn consume_residual(
        &mut self,
        block: NativeBlockView<'_>,
        layout: &NativeLayout,
        selected: std::ops::Range<usize>,
    ) -> io::Result<()> {
        let job = &mut self.jobs[0];
        let band = job.band.as_mut().expect("prepared residual wave");
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
            queued: None,
        };
        if job.shape == [0; 2] {
            band.initialize_spatial(&mut backend)
                .map_err(io::Error::other)?;
            job.shape = backend.shape;
            job.fields = backend.fields;
        }
        let state = self.residual.as_mut().expect("connected residual state");
        let slot = state.next_slot;
        let started = state.profile.then(Instant::now);
        if let Some(ticket) = state.tickets[slot].take() {
            self.access.wait(ticket).map_err(io::Error::other)?;
        }
        if let Some(started) = started {
            state.host_seconds[0] += started.elapsed().as_secs_f64();
        }
        let base = self.packed + slot * state.layout.slot_bytes;
        let started = state.profile.then(Instant::now);
        let (actual, requested) = self
            .access
            .with_bytes(
                MetalBufferRegion {
                    allocation: self.allocation,
                    offset: base,
                    bytes: state.layout.descriptor_bytes,
                },
                |bytes| -> io::Result<_> {
                    let (predictions, tail) = bytes.split_at_mut(state.layout.offsets[1]);
                    let (native, samples) =
                        tail.split_at_mut(state.layout.offsets[2] - state.layout.offsets[1]);
                    let predictions =
                        bytemuck::try_cast_slice_mut::<u8, ResidualPrediction>(predictions)
                            .map_err(|error| {
                                io::Error::other(format!(
                                    "invalid mapped prediction storage: {error:?}"
                                ))
                            })?;
                    let native = bytemuck::try_cast_slice_mut::<u8, NativePrediction>(native)
                        .map_err(|error| {
                            io::Error::other(format!("invalid mapped native storage: {error:?}"))
                        })?;
                    let samples = bytemuck::try_cast_slice_mut::<u8, ResidualSample>(samples)
                        .map_err(|error| {
                            io::Error::other(format!("invalid mapped sample storage: {error:?}"))
                        })?;
                    let mut refill = ResidualRefill {
                        predictions: &mut predictions[..state.layout.capacities[0]],
                        native: &mut native[..state.layout.capacities[1]],
                        samples: &mut samples[..state.layout.capacities[2]],
                        counts: [0; 3],
                        requested_predictions: 0,
                    };
                    band.prepare_residual_refill(
                        block,
                        layout,
                        selected,
                        self.output_hz,
                        &mut refill,
                    )
                    .map_err(io::Error::other)?;
                    Ok((refill.counts, refill.requested_predictions))
                },
            )
            .map_err(io::Error::other)??;
        if let Some(started) = started {
            state.host_seconds[1] += started.elapsed().as_secs_f64();
        }
        if actual
            .iter()
            .zip(state.layout.capacities)
            .any(|(&n, cap)| n > cap)
        {
            return Err(io::Error::other(
                "connected residual refill exceeds admitted capacity",
            ));
        }
        if actual[2] == 0 {
            return Ok(());
        }
        for (i, bytes) in [
            actual[0] * size_of::<ResidualPrediction>(),
            actual[1] * size_of::<NativePrediction>(),
            actual[2] * size_of::<ResidualSample>(),
        ]
        .into_iter()
        .enumerate()
        {
            state.staging_bytes += bytes as u64;
            if state.profile {
                state.staged_bytes[i] += bytes as u64;
            }
        }
        let (values, input_weights, flags, weight_flags) = block.sample_arrays();
        let slot_region = |i: usize| MetalBufferRegion {
            allocation: self.allocation,
            offset: base + state.layout.offsets[i],
            bytes: state.layout.length(i),
        };
        let started = state.profile.then(Instant::now);
        for (i, bytes) in [
            bytemuck::cast_slice(values),
            bytemuck::cast_slice(input_weights),
        ]
        .into_iter()
        .enumerate()
        {
            let i = i + 3;
            if bytes.is_empty() {
                continue;
            }
            self.access
                .with_bytes(slot_region(i), |target| {
                    target[..bytes.len()].copy_from_slice(bytes)
                })
                .map_err(io::Error::other)?;
            state.staging_bytes += bytes.len() as u64;
            if state.profile {
                state.staged_bytes[i] += bytes.len() as u64;
            }
        }
        if let Some(started) = started {
            state.host_seconds[2] += started.elapsed().as_secs_f64();
        }
        let started = state.profile.then(Instant::now);
        self.access
            .with_bytes(slot_region(5), |target| {
                for ((target, &flag), &weight_flag) in
                    target.iter_mut().zip(flags).zip(weight_flags)
                {
                    *target = u8::from(flag) | (u8::from(weight_flag) << 1);
                }
            })
            .map_err(io::Error::other)?;
        state.staging_bytes += flags.len() as u64;
        if let Some(started) = started {
            state.host_seconds[3] += started.elapsed().as_secs_f64();
            state.staged_bytes[5] += flags.len() as u64;
        }
        let started = state.profile.then(Instant::now);
        self.access
            .with_bytes(slot_region(6), |target| target.fill(0))
            .map_err(io::Error::other)?;
        if let Some(started) = started {
            state.host_seconds[4] += started.elapsed().as_secs_f64();
            state.cleared_bytes += slot_region(6).bytes as u64;
        }
        let models = backend
            .field_region(SpatialField::Model, 0)
            .map_err(io::Error::other)?;
        let residual = backend
            .field_region(SpatialField::Residual, 0)
            .map_err(io::Error::other)?;
        let regions = [
            slot_region(0),
            slot_region(1),
            slot_region(2),
            slot_region(3),
            slot_region(4),
            slot_region(5),
            backend.region(self.weights, BandPlan::spatial_weight_bytes()),
            MetalBufferRegion {
                bytes: models.bytes * job.fields[3],
                ..models
            },
            backend.region(self.predicted, state.layout.capacities[0].max(1) * 8),
            MetalBufferRegion {
                bytes: residual.bytes * job.fields[1],
                ..residual
            },
            slot_region(6),
        ];
        let shape = [
            actual[0],
            actual[2],
            actual[1],
            job.fields[3],
            job.shape[0],
            job.shape[1],
            job.fields[1],
            BandPlan::spatial_weight_bytes() / 28,
        ]
        .map(|n| u32::try_from(n).map_err(|_| overflow()));
        let shape: Vec<_> = shape.into_iter().collect::<io::Result<_>>()?;
        let started = state.profile.then(Instant::now);
        let ticket = self
            .access
            .submit_residual(CubeResidualDispatch {
                regions,
                shape: shape.try_into().expect("eight shape entries"),
                correlations: DeviceCorrelations::new(self.polarization)
                    .map_err(io::Error::other)?,
            })
            .map_err(io::Error::other)?;
        if let Some(started) = started {
            state.host_seconds[5] += started.elapsed().as_secs_f64();
        }
        state.tickets[slot] = Some(ticket);
        state.next_slot = (slot + 1) % 2;
        state.requested += requested;
        state.unique += actual[0] as u64;
        state.refills += 1;
        Ok(())
    }
}

impl Drop for MetalWave<'_> {
    fn drop(&mut self) {
        // Source/descriptor failures still drain commands before their arena is released.
        let _ = self.access.drain();
    }
}
