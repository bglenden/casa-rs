// SPDX-License-Identifier: LGPL-3.0-or-later
//! [`MetalBackend`]: one dispatch path (prepare on the host, map, commit,
//! complete) over a ring of slots.

use std::ops::Range;

use casa_imaging_operator::{
    AccumulatorLayout, CellHold, ConvolutionFunctionSet, DeviceCells, DeviceFailure,
    DeviceNativeCells, GridAccumulator, GridBackend, GridScalar, GridStorage, Mode, NativeModel,
    NativeSource, NativeTarget, NativeTerm, OperatorError, PreparedModelGrids, SampleBlock,
    TapLayout, Tile, Work,
};
use num_complex::{Complex32, Complex64};
use objc2::rc::Retained;
use objc2::runtime::ProtocolObject;
use objc2_metal::{
    MTLCommandBuffer, MTLCommandBufferStatus, MTLCommandEncoder, MTLCommandQueue,
    MTLComputeCommandEncoder, MTLSize,
};

use crate::device::{Buffer, Device, Pipeline};
use crate::records::{
    KnownTable, KnownTables, MAX_POLS, MAX_TERMS, Params, SampleRecord, TableRecord, TapsKind,
    Targets, prepare,
};
use crate::{MAX_SUB, RING, SPLIT};

/// The fewest samples in one sub-block: an `apply` is cut into at least
/// [`SPLIT`] sub-blocks once it holds `SPLIT · MIN_SUB` samples.
const MIN_SUB: usize = 512;

/// The most taps per kernel cell for which one thread serves a whole
/// sample, as the pre-refactor cube kernels did for their 7 × 7 taps;
/// wider supports give each sample a SIMD group, whose lanes share its
/// taps.
const THREAD_TAPS: usize = 81;

/// The Metal implementation of [`GridBackend`] for one kernel set.
///
/// Every dispatch is complete when [`GridBackend::apply`] returns, unless
/// the backend defers grid completion
/// ([`Self::with_deferred_grid_completion`]).
/// Inside one `apply` the block is cut into sub-blocks over a ring of
/// slots, so the host locates the samples of one sub-block while the
/// device grids the previous ones. Accumulators must come from
/// [`MetalBackend::accumulator`] (their cells live in memory the device
/// addresses); model grids are copied to the device once and the copy is
/// kept in the grids. Grids are `f32` (D2): an `f64` accumulator or model
/// is a programmer error and panics.
///
/// The backend uploads each kernel cell it meets once and serves only the
/// kernel set it was made for, which outlives it.
pub struct MetalBackend<'cf> {
    device: &'static Device,
    kernels: &'cf dyn ConvolutionFunctionSet,
    known: KnownTables,
    tables: Vec<TableRecord>,
    /// The most taps of any cell in `tables`.
    widest: usize,
    table_buffer: Buffer,
    rows: Arena,
    dense: Arena,
    ring: Vec<Slot>,
    next: usize,
    usage: Usage,
    stage_timing: bool,
    /// Whether grid-only work may still run when `apply` returns.
    defer_grid: bool,
    /// The predictions of a [`Work::PredictNative`] batch, which stay on
    /// the device for the native model.
    batch: Buffer,
}

/// What a backend was asked to dispatch over its life, logged once when it
/// drops if it was made with stage timing: the work is counted before it is
/// dispatched, so after a failure the counts include work the device never
/// did, and `failed` says so. `predicted` counts the samples of
/// [`Work::Predict`] dispatches among `samples`. Also the host's
/// preparation time and its waits for the device, and the device's time.
#[derive(Default)]
struct Usage {
    applies: u64,
    samples: u64,
    predicted: u64,
    sub_blocks: u64,
    prepare: std::time::Duration,
    wait: std::time::Duration,
    /// Seconds the device spent on this backend's commands
    /// (`GPUEndTime − GPUStartTime`, summed), and the host-time span from
    /// the first command's start to the last one's end. Commands of
    /// several backends can run at once, so their sums can exceed the
    /// wall time.
    gpu: f64,
    gpu_span: Option<(f64, f64)>,
    failed: bool,
}

impl Usage {
    /// Count a completed command's device time.
    fn completed(&mut self, command: &ProtocolObject<dyn MTLCommandBuffer>) {
        let (start, end) = (command.GPUStartTime(), command.GPUEndTime());
        if start > 0.0 && end >= start {
            self.gpu += end - start;
            self.gpu_span = Some(self.gpu_span.map_or((start, end), |(first, last)| {
                (first.min(start), last.max(end))
            }));
        }
    }
}

/// A growing device array of kernel values.
struct Arena {
    buffer: Buffer,
    used: usize,
}

/// One ring slot: the buffers of one sub-block and the command using them.
struct Slot {
    command: Option<Command>,
    /// Whether the command's output is read back; a grid-only command,
    /// perhaps left running by an earlier `apply`, is only waited for.
    reads_back: bool,
    range: Range<usize>,
    capacity: usize,
    npol: usize,
    records: Buffer,
    values: Buffer,
    weights: Buffer,
    norms: Buffer,
    out: Buffer,
    /// Native endpoints of a residual at native channels, or the native
    /// terms of a native model.
    sources: Buffer,
}

/// A committed command buffer.
#[derive(Clone)]
struct Command(Retained<ProtocolObject<dyn MTLCommandBuffer>>);

// SAFETY: a committed command buffer is only waited on and queried, which
// Metal allows from any thread; the backend that owns it is used by one
// thread at a time (`GridBackend` is `Send`, not `Sync`), and a clone handed
// to `DeviceCells::defer` is only waited on.
unsafe impl Send for Command {}

impl Command {
    fn wait_until_completed(&self) {
        self.0.waitUntilCompleted();
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kernel {
    Spread,
    Predict,
    Residual,
    /// [`Kernel::Predict`] into the backend's batch buffer, phasor applied.
    PredictNative,
    /// The model at native cells from the batch buffer.
    NativeModel,
    /// [`Kernel::Spread`] of values less the model at native cells.
    NativeResidual,
}

/// What the host does with a slot's output once its command completes.
enum Readback<'o> {
    None,
    /// `out = P · e^{−iφ}`.
    Prediction(&'o mut [Complex32]),
    /// `out = r · e^{−iφ} / W`, zero for a zero weight.
    Residual(&'o mut [Complex32]),
}

impl<'cf> MetalBackend<'cf> {
    /// A backend on the system's Metal device for kernel set `kernels`;
    /// with `stage_timing` it logs its usage when it drops
    /// ([`crate::METAL_STAGE_TIMING`]).
    pub fn new(
        kernels: &'cf dyn ConvolutionFunctionSet,
        stage_timing: bool,
    ) -> Result<Self, OperatorError> {
        let device = Device::shared()?;
        let mut ring = Vec::with_capacity(RING);
        for _ in 0..RING {
            ring.push(Slot {
                command: None,
                reads_back: false,
                range: 0..0,
                capacity: 0,
                npol: 0,
                records: device.buffer(0)?,
                values: device.buffer(0)?,
                weights: device.buffer(0)?,
                norms: device.buffer(0)?,
                out: device.buffer(0)?,
                sources: device.buffer(0)?,
            });
        }
        Ok(Self {
            device,
            kernels,
            known: KnownTables::new(),
            tables: Vec::new(),
            widest: 0,
            table_buffer: device.buffer(0)?,
            rows: Arena {
                buffer: device.buffer(0)?,
                used: 0,
            },
            dense: Arena {
                buffer: device.buffer(0)?,
                used: 0,
            },
            ring,
            next: 0,
            usage: Usage::default(),
            stage_timing,
            defer_grid: false,
            batch: device.buffer(0)?,
        })
    }

    /// This backend, leaving grid-only work ([`Work::Grid`], and
    /// [`Work::ResidualGrid`] without residual samples) running on the
    /// device when `apply` returns, so the host can prepare the next work
    /// meanwhile. The accumulator's cells wait for it before the host next
    /// reaches them ([`DeviceCells::defer`]).
    ///
    /// Only [`Self::complete`], or a later `apply` that reuses the work's
    /// slot, reports a deferred dispatch that failed; reading the cells or
    /// dropping the backend does not, so a caller calls `complete` before
    /// it trusts the grids.
    #[must_use]
    pub fn with_deferred_grid_completion(mut self) -> Self {
        self.defer_grid = true;
        self
    }

    /// Wait for every dispatch still running, and report the first that
    /// failed.
    ///
    /// # Errors
    ///
    /// [`OperatorError::Device`] when a dispatch failed on the device.
    pub fn complete(&mut self) -> Result<(), OperatorError> {
        let mut result = Ok(());
        for slot in 0..RING {
            let waited = self.wait(slot).map(|_| ());
            if result.is_ok() {
                result = waited;
            }
        }
        self.usage.failed |= result.is_err();
        result
    }

    /// `len` zeroed model values at native cells (`npol` per cell) in memory
    /// the device addresses, for [`Work::PredictNative`] to form and
    /// [`Work::NativeResidualGrid`] to read on any backend of this device.
    pub fn native_cells(len: usize) -> Result<DeviceNativeCells, OperatorError> {
        let device = Device::shared()?;
        let buffer = device.buffer(len * size_of::<Complex32>())?;
        let pointer = buffer.contents().cast::<Complex32>();
        // SAFETY: the buffer holds `len` zeroed, aligned complex values and
        // lives as long as its owner box (and as long as any command that
        // binds it, which Metal retains); only device commands write it.
        Ok(unsafe { DeviceNativeCells::new(pointer, len, Box::new(buffer)) })
    }

    /// A zeroed accumulator for `layout` whose cells live in memory the
    /// device addresses ([`GridStorage::Device`]).
    pub fn accumulator(layout: AccumulatorLayout) -> Result<GridAccumulator, OperatorError> {
        let device = Device::shared()?;
        let cells = layout.cells();
        let buffer = device.buffer(cells * size_of::<Complex32>())?;
        let pointer = buffer.contents().cast::<Complex32>();
        // SAFETY: the buffer holds `cells` zeroed, 16-byte aligned complex
        // values and lives as long as its owner box (and as long as any
        // command that binds it, which Metal retains). The host reaches it
        // only through these cells. The device writes it inside `apply`,
        // which holds `&mut` to the accumulator, and afterwards only while
        // work recorded with `DeviceCells::defer` runs, which the cells wait
        // for before the host reaches them.
        let cells = unsafe { DeviceCells::new(pointer, cells, Box::new(buffer)) };
        Ok(GridAccumulator::with_storage(
            layout,
            GridStorage::Device(cells),
        ))
    }

    /// Upload the taps of every cell of `kind` in samples `range` of `block`
    /// the device does not hold yet. `run` calls it for each sub-block just
    /// before preparing it, while its placements are in cache.
    fn learn(
        &mut self,
        block: &SampleBlock<'_>,
        range: Range<usize>,
        kind: TapsKind,
    ) -> Result<(), OperatorError> {
        let mut last = None;
        let known = self.tables.len();
        for placement in &block.placements[range] {
            let key = placement.cf;
            if last == Some(key) || self.known.contains_key(&(key, kind)) {
                last = Some(key);
                continue;
            }
            last = Some(key);
            let mut hold = CellHold::new();
            let taps = kind
                .taps(self.kernels, key, &mut hold)
                .ok_or(OperatorError::WeightKernelUnavailable { key })?;
            let index = u32::try_from(self.tables.len()).expect("kernel tables fit u32");
            let table = match taps {
                TapLayout::SeparableReal {
                    rows,
                    support,
                    oversampling,
                } => TableRecord {
                    offset: self.rows.append(self.device, rows)?,
                    support: [support, support],
                    oversampling,
                    mueller_planes: 1,
                    dense: 0,
                },
                TapLayout::Dense {
                    data,
                    support,
                    oversampling,
                    mueller_planes,
                } => TableRecord {
                    offset: self.dense.append(self.device, data)?,
                    support,
                    oversampling,
                    mueller_planes: u16::from(mueller_planes),
                    dense: 1,
                },
            };
            self.widest = self
                .widest
                .max(usize::from(table.support[0]) * usize::from(table.support[1]));
            self.tables.push(table);
            self.known
                .insert((key, kind), KnownTable::new(index, &taps));
        }
        if self.tables.len() > known {
            // Commands still running read only the records before `known`:
            // a grown buffer is new (they keep the old one), and the current
            // one gains records only after them.
            let bytes = self.tables.len() * size_of::<TableRecord>();
            let from = if self.table_buffer.bytes < bytes {
                self.table_buffer = self.device.buffer(2 * bytes)?;
                0
            } else {
                known
            };
            self.table_buffer
                .slice_mut::<TableRecord>(self.tables.len())[from..]
                .copy_from_slice(&self.tables[from..]);
        }
        Ok(())
    }

    /// The constants of one dispatch over `targets`; `weighted` spreads the
    /// weights (PSF and weight image) instead of the values.
    fn params(&self, block: &SampleBlock<'_>, targets: &Targets<'_>, weighted: bool) -> Params {
        let mueller = self.kernels.mueller();
        let gpols = mueller.grid_pols();
        assert!(
            gpols <= MAX_POLS && block.npol <= MAX_POLS,
            "the Metal kernels serve at most {MAX_POLS} polarizations"
        );
        let table = |forward: bool| {
            let mut out = [[-1_i8; 16]; 2];
            for (w_positive, target) in out.iter_mut().enumerate() {
                for (gpol, row) in mueller.table(w_positive == 1, forward).iter().enumerate() {
                    for (vpol, plane) in row.iter().enumerate() {
                        if let Some(plane) = plane {
                            target[gpol * 4 + vpol] = *plane as i8;
                        }
                    }
                }
            }
            out
        };
        let shape =
            |extent: [usize; 2]| extent.map(|n| u32::try_from(n).expect("grid extent fits u32"));
        let model_terms = targets.model.map_or(0, AccumulatorLayout::terms);
        let (terms, term_base, term_count, tile) = match &targets.adjoint {
            Some((layout, range)) => (layout.terms(), range.start, range.len(), layout.tile()),
            None => {
                let model = targets.model.expect("a prediction has model grids");
                (
                    model_terms,
                    0,
                    model_terms,
                    Tile::full(model.geometry().grid_shape()),
                )
            }
        };
        assert!(
            term_count <= MAX_TERMS,
            "the Metal kernels serve at most {MAX_TERMS} terms"
        );
        Params {
            samples: 0,
            npol: block.npol as u32,
            gpols: gpols as u32,
            terms: terms as u32,
            term_base: term_base as u32,
            term_count: term_count as u32,
            model_terms: model_terms as u32,
            weighted: u32::from(weighted),
            write_residual: 0,
            lanes: 0,
            batch_offset: 0,
            native: 0,
            add: 0,
            pad: 0,
            tile_shape: shape(tile.shape),
            tile_origin: shape(tile.origin),
            grid_shape: shape(targets.adjoint.as_ref().map_or_else(
                || targets.model.expect("a target").geometry().grid_shape(),
                |(layout, _)| layout.geometry().grid_shape(),
            )),
            adjoint: table(false),
            forward: table(true),
        }
    }

    /// Prepare, commit and complete `kernel` over every sample of `block`.
    /// Grid-only work into `cells` is left running when the backend defers
    /// grid completion; the cells then wait for it. With `leave_running`
    /// every sub-block is left running: work that writes only device
    /// buffers later device work reads ([`Kernel::PredictNative`]), its
    /// slots retired on reuse or by [`Self::complete`].
    #[allow(clippy::too_many_arguments)]
    fn run(
        &mut self,
        kernel: Kernel,
        block: &SampleBlock<'_>,
        targets: &Targets<'_>,
        mut sumwt: Option<&mut [f64]>,
        cells: Option<&DeviceCells>,
        model: Option<&Buffer>,
        native: Option<&Buffer>,
        mut readback: Readback<'_>,
        leave_running: bool,
        params: Params,
    ) -> Result<(), OperatorError> {
        let grid = cells.map(|cells| {
            cells
                .owner()
                .downcast_ref::<Buffer>()
                .expect("the Metal backend grids into accumulators from MetalBackend::accumulator")
        });
        let reads_back = !matches!(readback, Readback::None);
        let defer = self.defer_grid && !reads_back && cells.is_some();
        let samples = block.len();
        let sub = samples.div_ceil(SPLIT).clamp(MIN_SUB, MAX_SUB);
        self.usage.applies += 1;
        self.usage.samples += samples as u64;
        if matches!(kernel, Kernel::Predict | Kernel::PredictNative) {
            self.usage.predicted += samples as u64;
        }
        self.usage.sub_blocks += samples.div_ceil(sub) as u64;
        // A misplaced sample panics in `prepare` after earlier sub-blocks are
        // committed: drain the ring before the panic leaves `apply`, so the
        // device never writes the accumulator once the caller holds it again.
        let dispatched = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut start = 0;
            while start < samples {
                let end = (start + sub).min(samples);
                let slot = self.next;
                self.next = (self.next + 1) % RING;
                self.retire(slot, block, &mut readback)?;
                let started = std::time::Instant::now();
                self.learn(block, start..end, targets.kind)?;
                let filled = self.fill(
                    slot,
                    kernel,
                    block,
                    start..end,
                    targets,
                    sumwt.as_deref_mut(),
                );
                self.usage.prepare += started.elapsed();
                filled?;
                self.commit(slot, kernel, grid, model, native, params, start..end)?;
                self.ring[slot].reads_back = reads_back;
                start = end;
            }
            Ok::<_, OperatorError>(())
        }));
        if leave_running && matches!(dispatched, Ok(Ok(()))) {
            return Ok(());
        }
        if defer && matches!(dispatched, Ok(Ok(()))) {
            let last = (self.next + RING - 1) % RING;
            if let Some(command) = self.ring[last].command.clone() {
                let cells = cells.expect("deferred work has cells");
                // SAFETY: every command that writes these cells, from any
                // backend, goes to the one process-wide queue
                // (`Device::shared`), and the grid buffer is tracked, so
                // the device runs them in commit order: once this apply's
                // last command completes, every write issued so far has.
                // Later device writes come only from later applies, which
                // record their own wait.
                unsafe { cells.defer(Box::new(move || command.wait_until_completed())) };
            }
            return Ok(());
        }
        let mut drained = Ok(());
        for slot in 0..RING {
            let retired = self.retire(slot, block, &mut readback);
            if drained.is_ok() {
                drained = retired;
            }
        }
        let result = match dispatched {
            Ok(outcome) => outcome.and(drained),
            Err(panic) => {
                self.usage.failed = true;
                std::panic::resume_unwind(panic)
            }
        };
        self.usage.failed |= result.is_err();
        result
    }

    /// Write the records, values, weights and inverse norms of samples
    /// `range` into ring slot `slot`.
    fn fill(
        &mut self,
        slot: usize,
        kernel: Kernel,
        block: &SampleBlock<'_>,
        range: Range<usize>,
        targets: &Targets<'_>,
        sumwt: Option<&mut [f64]>,
    ) -> Result<(), OperatorError> {
        let npol = block.npol;
        let count = range.len();
        let slot = &mut self.ring[slot];
        if slot.capacity < count || slot.npol != npol {
            let capacity = count.max(slot.capacity);
            let device = self.device;
            slot.records = device.buffer(capacity * size_of::<SampleRecord>())?;
            slot.values = device.buffer(capacity * npol * size_of::<Complex32>())?;
            slot.weights = device.buffer(capacity * npol * size_of::<f32>())?;
            slot.norms = device.buffer(capacity * npol * size_of::<Complex32>())?;
            slot.out = device.buffer(capacity * npol * size_of::<Complex32>())?;
            slot.sources = device.buffer(capacity * size_of::<NativeSource>())?;
            slot.capacity = capacity;
            slot.npol = npol;
        }
        let forward = matches!(
            kernel,
            Kernel::Predict | Kernel::PredictNative | Kernel::Residual
        );
        prepare(
            self.kernels,
            &self.known,
            block,
            range.clone(),
            targets,
            sumwt,
            slot.records.slice_mut(count),
            forward.then(|| slot.norms.slice_mut(count * npol)),
        );
        match kernel {
            Kernel::Predict | Kernel::NativeModel => {}
            Kernel::PredictNative => {
                // The phase-centre phasor of each prediction, `e^{−iφ}`,
                // taken in f64 on the host.
                for (phasor, placement) in slot
                    .values
                    .slice_mut::<Complex32>(count)
                    .iter_mut()
                    .zip(&block.placements[range])
                {
                    let value = Complex64::from_polar(1.0, -placement.phase);
                    *phasor = Complex32::new(value.re as f32, value.im as f32);
                }
            }
            Kernel::Spread | Kernel::Residual | Kernel::NativeResidual => {
                let values = range.start * npol..range.end * npol;
                slot.values
                    .slice_mut(count * npol)
                    .copy_from_slice(&block.values[values.clone()]);
                slot.weights
                    .slice_mut(count * npol)
                    .copy_from_slice(&block.weights[values]);
                if kernel == Kernel::NativeResidual {
                    slot.sources
                        .slice_mut(count)
                        .copy_from_slice(&block.sources[range]);
                }
            }
        }
        Ok(())
    }

    /// Encode and commit `kernel` over the samples (or, for
    /// [`Kernel::NativeModel`], the native terms) in slot `slot`.
    #[allow(clippy::too_many_arguments)]
    fn commit(
        &mut self,
        slot_index: usize,
        kernel: Kernel,
        grid: Option<&Buffer>,
        model: Option<&Buffer>,
        native: Option<&Buffer>,
        mut params: Params,
        range: Range<usize>,
    ) -> Result<(), OperatorError> {
        let failed = OperatorError::Device(DeviceFailure::CommandFailed { code: -1 });
        let device = self.device;
        let pipeline: &Pipeline = match kernel {
            Kernel::Spread | Kernel::NativeResidual => &device.spread,
            Kernel::Predict | Kernel::PredictNative => &device.predict,
            Kernel::Residual => &device.residual,
            Kernel::NativeModel => &device.native,
        };
        params.samples = range.len() as u32;
        params.native = u32::from(matches!(
            kernel,
            Kernel::PredictNative | Kernel::NativeResidual
        ));
        if kernel == Kernel::PredictNative {
            params.batch_offset = u32::try_from(range.start).expect("a batch fits u32 indices");
        }
        let (lanes, samples_per_group) =
            if self.widest <= THREAD_TAPS || kernel == Kernel::NativeModel {
                (1, pipeline.threads)
            } else {
                (pipeline.width, pipeline.samples_per_group)
            };
        params.lanes = lanes as u32;
        let batch = &self.batch;
        let slot = &mut self.ring[slot_index];
        objc2::rc::autoreleasepool(|_| {
            let command = device.queue.commandBuffer().ok_or(failed.clone())?;
            let encoder = command.computeCommandEncoder().ok_or(failed)?;
            encoder.setComputePipelineState(&pipeline.state);
            let bind = |buffer: &Buffer, index: usize| {
                // SAFETY: the command buffer retains every buffer it binds,
                // so each outlives the command; the host reaches the slot
                // buffers only after `wait`, and the grid's cells only
                // through their deferred wait. Indices match the kernels.
                unsafe { encoder.setBuffer_offset_atIndex(Some(&buffer.raw), 0, index) };
            };
            bind(&self.table_buffer, 3);
            bind(&self.rows.buffer, 4);
            bind(&self.dense.buffer, 5);
            match kernel {
                Kernel::Spread | Kernel::NativeResidual => {
                    bind(&slot.records, 0);
                    bind(&slot.values, 1);
                    bind(&slot.weights, 2);
                    bind(grid.expect("spreading has a grid"), 6);
                    // Read only for a residual at native channels.
                    bind(&slot.sources, 8);
                    bind(native.unwrap_or(&slot.out), 9);
                }
                Kernel::Predict | Kernel::PredictNative => {
                    bind(&slot.records, 0);
                    bind(&slot.norms, 1);
                    // The phasors, read only for a native prediction.
                    bind(&slot.values, 2);
                    bind(model.expect("prediction has a model"), 6);
                    let out = if kernel == Kernel::Predict {
                        &slot.out
                    } else {
                        batch
                    };
                    bind(out, 8);
                }
                Kernel::NativeModel => {
                    bind(&slot.sources, 0);
                    bind(batch, 8);
                    bind(native.expect("the native model has cells"), 9);
                }
                Kernel::Residual => {
                    bind(&slot.records, 0);
                    bind(&slot.values, 1);
                    bind(&slot.weights, 2);
                    bind(grid.expect("the residual has a grid"), 6);
                    bind(&slot.out, 8);
                    bind(model.expect("the residual has a model"), 9);
                    bind(&slot.norms, 10);
                }
            }
            let pointer = std::ptr::NonNull::from(&params).cast();
            // SAFETY: Metal copies the `Params` bytes during the call.
            unsafe { encoder.setBytes_length_atIndex(pointer, size_of::<Params>(), 7) };
            encoder.dispatchThreadgroups_threadsPerThreadgroup(
                MTLSize {
                    width: range.len().div_ceil(samples_per_group),
                    height: 1,
                    depth: 1,
                },
                MTLSize {
                    width: pipeline.threads,
                    height: 1,
                    depth: 1,
                },
            );
            encoder.endEncoding();
            command.commit();
            slot.command = Some(Command(command));
            slot.range = range;
            Ok(())
        })
    }

    /// Wait for slot `slot`'s command, if any: the samples whose output it
    /// reads back, when it reads any.
    fn wait(&mut self, slot: usize) -> Result<Option<Range<usize>>, OperatorError> {
        let slot = &mut self.ring[slot];
        let Some(Command(command)) = slot.command.take() else {
            return Ok(None);
        };
        let started = std::time::Instant::now();
        command.waitUntilCompleted();
        self.usage.wait += started.elapsed();
        if self.stage_timing {
            self.usage.completed(&command);
        }
        if command.status() == MTLCommandBufferStatus::Error {
            let code = command.error().map_or(-1, |error| error.code() as i64);
            return Err(OperatorError::Device(DeviceFailure::CommandFailed { code }));
        }
        Ok(slot.reads_back.then(|| slot.range.clone()))
    }

    /// Wait for slot `slot`'s command, if any, and read its output back.
    fn retire(
        &mut self,
        slot: usize,
        block: &SampleBlock<'_>,
        readback: &mut Readback<'_>,
    ) -> Result<(), OperatorError> {
        let Some(range) = self.wait(slot)? else {
            return Ok(());
        };
        let slot = &self.ring[slot];
        let npol = block.npol;
        let device = slot.out.slice::<Complex32>(range.len() * npol);
        let (out, residual) = match readback {
            Readback::None => return Ok(()),
            Readback::Prediction(out) => (out, false),
            Readback::Residual(out) => (out, true),
        };
        for (local, index) in range.enumerate() {
            let phasor = Complex64::from_polar(1.0, -block.placements[index].phase);
            for vpol in 0..npol {
                let value = device[local * npol + vpol];
                let value = Complex64::new(f64::from(value.re), f64::from(value.im)) * phasor;
                let value = if residual {
                    let weight = f64::from(block.weights[index * npol + vpol]);
                    if weight == 0.0 {
                        Complex64::default()
                    } else {
                        value / weight
                    }
                } else {
                    value
                };
                out[index * npol + vpol] = Complex32::new(value.re as f32, value.im as f32);
            }
        }
        Ok(())
    }

    /// The device copy of `model`, made once and kept in the grids.
    fn model_copy<'m>(&self, model: &'m PreparedModelGrids) -> Result<&'m Buffer, OperatorError> {
        let device = self.device;
        model
            .device_copy(|storage| {
                let cells = <f32 as GridScalar>::cells(storage);
                let mut buffer = device.buffer(std::mem::size_of_val(cells))?;
                buffer
                    .slice_mut::<Complex32>(cells.len())
                    .copy_from_slice(cells);
                Ok::<_, OperatorError>(buffer)
            })
            .as_ref()
            .map_err(Clone::clone)
    }
}

impl Arena {
    /// Append `values`, growing the device array when full; returns their
    /// first index. Commands still running read only values appended before
    /// them, which this never rewrites: a grown array is a new buffer, and
    /// they keep the old one.
    fn append<T: Copy>(&mut self, device: &Device, values: &[T]) -> Result<u32, OperatorError> {
        let needed = (self.used + values.len()) * size_of::<T>();
        if self.buffer.bytes < needed {
            let mut grown = device.buffer(needed.max(2 * self.buffer.bytes))?;
            grown
                .slice_mut::<T>(self.used)
                .copy_from_slice(self.buffer.slice::<T>(self.used));
            self.buffer = grown;
        }
        let offset = self.used;
        self.buffer.slice_mut::<T>(self.used + values.len())[offset..].copy_from_slice(values);
        self.used += values.len();
        Ok(u32::try_from(offset).expect("kernel arenas fit u32 indices"))
    }
}

impl Drop for MetalBackend<'_> {
    fn drop(&mut self) {
        // Deferred grid work may still run; a backend dropped without
        // `complete` waits for it, and its usage says whether it failed.
        for slot in &mut self.ring {
            if let Some(Command(command)) = slot.command.take() {
                command.waitUntilCompleted();
                if self.stage_timing {
                    self.usage.completed(&command);
                }
                self.usage.failed |= command.status() == MTLCommandBufferStatus::Error;
            }
        }
        let Usage {
            applies,
            samples,
            predicted,
            sub_blocks,
            prepare,
            wait,
            gpu,
            gpu_span,
            failed,
        } = self.usage;
        if self.stage_timing && applies > 0 {
            tracing::info!(
                target: crate::METAL_STAGE_TIMING,
                completed = !failed && !std::thread::panicking(),
                applies,
                samples,
                predicted,
                sub_blocks,
                kernel_cells = self.tables.len(),
                prepare_seconds = prepare.as_secs_f64(),
                wait_seconds = wait.as_secs_f64(),
                gpu_seconds = gpu,
                gpu_span_seconds = gpu_span.map_or(0.0, |(first, last)| last - first),
                "metal backend"
            );
        }
    }
}

/// The device cells of an accumulator from [`MetalBackend::accumulator`].
fn device_cells(storage: &GridStorage) -> &DeviceCells {
    storage
        .device()
        .expect("the Metal backend grids into accumulators from MetalBackend::accumulator")
}

impl GridBackend for MetalBackend<'_> {
    fn apply(
        &mut self,
        block: &SampleBlock<'_>,
        cf: &dyn ConvolutionFunctionSet,
        work: Work<'_>,
    ) -> Result<(), OperatorError> {
        assert!(
            std::ptr::addr_eq(cf, self.kernels),
            "a Metal backend serves the kernel set it was made for"
        );
        assert_eq!(
            block.npol,
            cf.mueller().visibility_pols(),
            "block polarizations must match the kernel set routing"
        );
        if let Work::PredictNative {
            grids,
            terms,
            model,
            add,
        } = work
        {
            // Rows that placed nothing still have their cells written.
            let NativeTarget::Device(cells) = model else {
                panic!(
                    "the Metal backend forms the model in cells from MetalBackend::native_cells"
                );
            };
            return self.predict_native(block, grids, terms, cells, add);
        }
        if block.is_empty() {
            return Ok(());
        }
        match work {
            Work::Grid { mode, acc } => self.grid(block, mode, acc),
            Work::Predict { model, out } => self.predict(block, model, out),
            Work::ResidualGrid {
                model,
                acc,
                residual_out,
            } => self.residual(block, model, acc, residual_out),
            Work::NativeResidualGrid { model, acc } => {
                let NativeModel::Device(cells) = model else {
                    panic!(
                        "the Metal backend reads the model from cells from MetalBackend::native_cells"
                    );
                };
                self.native_residual(block, cells, acc)
            }
            Work::PredictNative { .. } => unreachable!("dispatched above"),
        }
    }
}

impl MetalBackend<'_> {
    /// [`Work::Grid`]: spread values (data) or weights (PSF, weight image).
    fn grid(
        &mut self,
        block: &SampleBlock<'_>,
        mode: Mode,
        acc: &mut GridAccumulator,
    ) -> Result<(), OperatorError> {
        let kind = TapsKind::of(mode);
        let (layout, storage, sumwt) = acc.backend_parts();
        let terms = layout
            .term_range(mode)
            .unwrap_or_else(|| panic!("accumulator holds no {mode:?} terms"));
        let targets = Targets {
            kind,
            adjoint: Some((layout, terms)),
            model: None,
        };
        let params = self.params(block, &targets, mode != Mode::Data);
        self.run(
            Kernel::Spread,
            block,
            &targets,
            sumwt,
            Some(device_cells(storage)),
            None,
            None,
            Readback::None,
            false,
            params,
        )
    }

    /// [`Work::Predict`]: gather and normalise, phasor applied on read-back.
    fn predict(
        &mut self,
        block: &SampleBlock<'_>,
        model: &PreparedModelGrids,
        out: &mut [Complex32],
    ) -> Result<(), OperatorError> {
        assert_eq!(
            out.len(),
            block.len() * block.npol,
            "prediction output length"
        );
        let buffer = self.model_copy(model)?;
        let targets = Targets {
            kind: TapsKind::Imaging,
            adjoint: None,
            model: Some(model.layout()),
        };
        let params = self.params(block, &targets, false);
        self.run(
            Kernel::Predict,
            block,
            &targets,
            None,
            None,
            Some(buffer),
            None,
            Readback::Prediction(out),
            false,
            params,
        )
    }

    /// [`Work::PredictNative`]: predict the batch into the backend's batch
    /// buffer, then form the model at each term's cell, leaving both
    /// running: device work that reads the cells later runs after them
    /// (one queue, tracked buffers), and their slots are retired on reuse
    /// or by [`Self::complete`].
    fn predict_native(
        &mut self,
        block: &SampleBlock<'_>,
        grids: &PreparedModelGrids,
        terms: &[NativeTerm],
        cells: &DeviceNativeCells,
        add: bool,
    ) -> Result<(), OperatorError> {
        let native = cells
            .owner()
            .downcast_ref::<Buffer>()
            .expect("the Metal backend forms the model in cells from MetalBackend::native_cells");
        let bytes = block.len() * block.npol * size_of::<Complex32>();
        if self.batch.bytes < bytes {
            // Commands still running keep the old buffer.
            self.batch = self.device.buffer(bytes.max(2 * self.batch.bytes))?;
        }
        let targets = Targets {
            kind: TapsKind::Imaging,
            adjoint: None,
            model: Some(grids.layout()),
        };
        let mut params = self.params(block, &targets, false);
        if !block.is_empty() {
            let buffer = self.model_copy(grids)?;
            self.run(
                Kernel::PredictNative,
                block,
                &targets,
                None,
                None,
                Some(buffer),
                None,
                Readback::None,
                true,
                params,
            )?;
        }
        params.add = u32::from(add);
        for part in terms.chunks(MAX_SUB) {
            let slot = self.next;
            self.next = (self.next + 1) % RING;
            self.retire(slot, block, &mut Readback::None)?;
            let started = std::time::Instant::now();
            let ring_slot = &mut self.ring[slot];
            if ring_slot.sources.bytes < size_of_val(part) {
                ring_slot.sources = self.device.buffer(size_of_val(part))?;
            }
            ring_slot
                .sources
                .slice_mut::<NativeTerm>(part.len())
                .copy_from_slice(part);
            self.usage.prepare += started.elapsed();
            self.usage.sub_blocks += 1;
            self.commit(
                slot,
                Kernel::NativeModel,
                None,
                None,
                Some(native),
                params,
                0..part.len(),
            )?;
            self.ring[slot].reads_back = false;
        }
        Ok(())
    }

    /// [`Work::NativeResidualGrid`]: spread each sample's values less the
    /// model at its native endpoints.
    fn native_residual(
        &mut self,
        block: &SampleBlock<'_>,
        cells: &DeviceNativeCells,
        acc: &mut GridAccumulator,
    ) -> Result<(), OperatorError> {
        assert_eq!(
            block.sources.len(),
            block.len(),
            "a native residual block holds every placement's endpoints"
        );
        let native = cells
            .owner()
            .downcast_ref::<Buffer>()
            .expect("the Metal backend reads the model from cells from MetalBackend::native_cells");
        let (layout, storage, sumwt) = acc.backend_parts();
        let terms = layout
            .term_range(Mode::Data)
            .expect("residual gridding needs the data terms");
        let targets = Targets {
            kind: TapsKind::Imaging,
            adjoint: Some((layout, terms)),
            model: None,
        };
        let params = self.params(block, &targets, false);
        self.run(
            Kernel::NativeResidual,
            block,
            &targets,
            sumwt,
            Some(device_cells(storage)),
            None,
            Some(native),
            Readback::None,
            false,
            params,
        )
    }

    /// [`Work::ResidualGrid`]: predict, subtract and spread the residual in
    /// one dispatch, handing the residual samples back when asked.
    fn residual(
        &mut self,
        block: &SampleBlock<'_>,
        model: &PreparedModelGrids,
        acc: &mut GridAccumulator,
        residual_out: Option<&mut [Complex32]>,
    ) -> Result<(), OperatorError> {
        let buffer = self.model_copy(model)?;
        let (layout, storage, sumwt) = acc.backend_parts();
        let terms = layout
            .term_range(Mode::Data)
            .expect("residual gridding needs the data terms");
        let targets = Targets {
            kind: TapsKind::Imaging,
            adjoint: Some((layout, terms)),
            model: Some(model.layout()),
        };
        let mut params = self.params(block, &targets, false);
        let readback = match residual_out {
            Some(out) => {
                assert_eq!(
                    out.len(),
                    block.len() * block.npol,
                    "residual output length"
                );
                params.write_residual = 1;
                Readback::Residual(out)
            }
            None => Readback::None,
        };
        self.run(
            Kernel::Residual,
            block,
            &targets,
            sumwt,
            Some(device_cells(storage)),
            Some(buffer),
            None,
            readback,
            false,
            params,
        )
    }
}
