// SPDX-License-Identifier: LGPL-3.0-or-later
//! [`MetalBackend`]: one dispatch path (prepare on the host, map, commit,
//! complete) over a three-slot ring.

use std::ops::Range;

use casa_imaging_operator::{
    AccumulatorLayout, ConvolutionFunctionSet, DeviceCells, DeviceFailure, GridAccumulator,
    GridBackend, GridScalar, GridStorage, Mode, OperatorError, PreparedModelGrids, SampleBlock,
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
use crate::{MAX_SUB, RING};

/// The fewest samples in one sub-block: an `apply` is cut into at least
/// [`RING`] sub-blocks once it holds `RING · MIN_SUB` samples.
const MIN_SUB: usize = 512;

/// The Metal implementation of [`GridBackend`] for one kernel set.
///
/// Every dispatch is complete when [`GridBackend::apply`] returns. Inside
/// one `apply` the block is cut into sub-blocks over a three-slot ring, so
/// the host locates the samples of one sub-block while the device grids
/// the previous ones. Accumulators must come from
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
    table_buffer: Buffer,
    rows: Arena,
    dense: Arena,
    ring: Vec<Slot>,
    next: usize,
}

/// A growing device array of kernel values.
struct Arena {
    buffer: Buffer,
    used: usize,
}

/// One ring slot: the buffers of one sub-block and the command using them.
struct Slot {
    command: Option<Command>,
    range: Range<usize>,
    capacity: usize,
    npol: usize,
    records: Buffer,
    values: Buffer,
    weights: Buffer,
    norms: Buffer,
    out: Buffer,
}

/// A committed command buffer.
struct Command(Retained<ProtocolObject<dyn MTLCommandBuffer>>);

// SAFETY: a committed command buffer is only waited on and queried, which
// Metal allows from any thread; the backend that owns it is used by one
// thread at a time (`GridBackend` is `Send`, not `Sync`).
unsafe impl Send for Command {}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kernel {
    Spread,
    Predict,
    Residual,
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
    /// A backend on the system's Metal device for kernel set `kernels`.
    pub fn new(kernels: &'cf dyn ConvolutionFunctionSet) -> Result<Self, OperatorError> {
        let device = Device::shared()?;
        let mut ring = Vec::with_capacity(RING);
        for _ in 0..RING {
            ring.push(Slot {
                command: None,
                range: 0..0,
                capacity: 0,
                npol: 0,
                records: device.buffer(0)?,
                values: device.buffer(0)?,
                weights: device.buffer(0)?,
                norms: device.buffer(0)?,
                out: device.buffer(0)?,
            });
        }
        Ok(Self {
            device,
            kernels,
            known: KnownTables::new(),
            tables: Vec::new(),
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
        })
    }

    /// A zeroed accumulator for `layout` whose cells live in memory the
    /// device addresses ([`GridStorage::Device`]).
    pub fn accumulator(layout: AccumulatorLayout) -> Result<GridAccumulator, OperatorError> {
        let device = Device::shared()?;
        let cells = layout.cells();
        let buffer = device.buffer(cells * size_of::<Complex32>())?;
        let pointer = buffer.contents().cast::<Complex32>();
        // SAFETY: the buffer holds `cells` zeroed, 16-byte aligned complex
        // values and lives as long as its owner box. The host reaches it only
        // through these cells, and the device writes it only inside `apply`,
        // which holds `&mut` to the accumulator and completes before
        // returning.
        let cells = unsafe { DeviceCells::new(pointer, cells, Box::new(buffer)) };
        Ok(GridAccumulator::with_storage(
            layout,
            GridStorage::Device(cells),
        ))
    }

    /// Upload the taps of every cell of `kind` in `block` the device does
    /// not hold yet.
    fn learn(&mut self, block: &SampleBlock<'_>, kind: TapsKind) -> Result<(), OperatorError> {
        let mut last = None;
        let mut added = false;
        for placement in block.placements {
            let key = placement.cf;
            if last == Some(key) || self.known.contains_key(&(key, kind)) {
                last = Some(key);
                continue;
            }
            last = Some(key);
            let taps = kind
                .taps(self.kernels, key)
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
            self.tables.push(table);
            self.known
                .insert((key, kind), KnownTable::new(index, &taps));
            added = true;
        }
        if added {
            let bytes = self.tables.len() * size_of::<TableRecord>();
            if self.table_buffer.bytes < bytes {
                self.table_buffer = self.device.buffer(2 * bytes)?;
            }
            self.table_buffer
                .slice_mut::<TableRecord>(self.tables.len())
                .copy_from_slice(&self.tables);
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
    #[allow(clippy::too_many_arguments)]
    fn run(
        &mut self,
        kernel: Kernel,
        block: &SampleBlock<'_>,
        targets: &Targets<'_>,
        mut sumwt: Option<&mut [f64]>,
        grid: Option<&Buffer>,
        model: Option<&Buffer>,
        mut readback: Readback<'_>,
        params: Params,
    ) -> Result<(), OperatorError> {
        let samples = block.len();
        let sub = samples.div_ceil(RING).clamp(MIN_SUB, MAX_SUB);
        let mut outcome = Ok(());
        let mut start = 0;
        while start < samples {
            let end = (start + sub).min(samples);
            let slot = self.next;
            self.next = (self.next + 1) % RING;
            outcome = self.retire(slot, block, &mut readback);
            if outcome.is_err() {
                break;
            }
            outcome = self
                .fill(
                    slot,
                    kernel,
                    block,
                    start..end,
                    targets,
                    sumwt.as_deref_mut(),
                )
                .and_then(|()| self.commit(slot, kernel, grid, model, params, start..end));
            if outcome.is_err() {
                break;
            }
            start = end;
        }
        for slot in 0..RING {
            let retired = self.retire(slot, block, &mut readback);
            if outcome.is_ok() {
                outcome = retired;
            }
        }
        outcome
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
            slot.capacity = capacity;
            slot.npol = npol;
        }
        let forward = kernel != Kernel::Spread;
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
        if kernel != Kernel::Predict {
            let values = range.start * npol..range.end * npol;
            slot.values
                .slice_mut(count * npol)
                .copy_from_slice(&block.values[values.clone()]);
            slot.weights
                .slice_mut(count * npol)
                .copy_from_slice(&block.weights[values]);
        }
        Ok(())
    }

    /// Encode and commit `kernel` over the samples in slot `slot`.
    fn commit(
        &mut self,
        slot_index: usize,
        kernel: Kernel,
        grid: Option<&Buffer>,
        model: Option<&Buffer>,
        mut params: Params,
        range: Range<usize>,
    ) -> Result<(), OperatorError> {
        let failed = OperatorError::Device(DeviceFailure::CommandFailed { code: -1 });
        let device = self.device;
        let pipeline: &Pipeline = match kernel {
            Kernel::Spread => &device.spread,
            Kernel::Predict => &device.predict,
            Kernel::Residual => &device.residual,
        };
        params.samples = range.len() as u32;
        let slot = &mut self.ring[slot_index];
        objc2::rc::autoreleasepool(|_| {
            let command = device.queue.commandBuffer().ok_or(failed.clone())?;
            let encoder = command.computeCommandEncoder().ok_or(failed)?;
            encoder.setComputePipelineState(&pipeline.state);
            let bind = |buffer: &Buffer, index: usize| {
                // SAFETY: every bound buffer outlives the command: the slot,
                // arena and table buffers belong to this backend, the grid
                // and model to the caller's `apply`, and `apply` waits for
                // the command before returning. Indices match the kernels.
                unsafe { encoder.setBuffer_offset_atIndex(Some(&buffer.raw), 0, index) };
            };
            bind(&slot.records, 0);
            bind(&self.table_buffer, 3);
            bind(&self.rows.buffer, 4);
            bind(&self.dense.buffer, 5);
            match kernel {
                Kernel::Spread => {
                    bind(&slot.values, 1);
                    bind(&slot.weights, 2);
                    bind(grid.expect("spreading has a grid"), 6);
                }
                Kernel::Predict => {
                    bind(&slot.norms, 1);
                    bind(model.expect("prediction has a model"), 6);
                    bind(&slot.out, 8);
                }
                Kernel::Residual => {
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
                    width: range.len().div_ceil(pipeline.samples_per_group),
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

    /// Wait for slot `slot`'s command, if any, and read its output back.
    fn retire(
        &mut self,
        slot: usize,
        block: &SampleBlock<'_>,
        readback: &mut Readback<'_>,
    ) -> Result<(), OperatorError> {
        let slot = &mut self.ring[slot];
        let Some(Command(command)) = slot.command.take() else {
            return Ok(());
        };
        command.waitUntilCompleted();
        if command.status() == MTLCommandBufferStatus::Error {
            let code = command.error().map_or(-1, |error| error.code() as i64);
            return Err(OperatorError::Device(DeviceFailure::CommandFailed { code }));
        }
        let npol = block.npol;
        let range = slot.range.clone();
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
    /// first index. Called only while no command is pending.
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

/// The device buffer behind an accumulator from
/// [`MetalBackend::accumulator`].
fn grid_buffer(storage: &GridStorage) -> &Buffer {
    storage
        .device()
        .and_then(|cells| cells.owner().downcast_ref::<Buffer>())
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
        if block.is_empty() {
            return Ok(());
        }
        match work {
            Work::Grid { mode, acc } => {
                let kind = TapsKind::of(mode);
                self.learn(block, kind)?;
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
                let grid = grid_buffer(storage);
                self.run(
                    Kernel::Spread,
                    block,
                    &targets,
                    Some(sumwt),
                    Some(grid),
                    None,
                    Readback::None,
                    params,
                )
            }
            Work::Predict { model, out } => {
                assert_eq!(
                    out.len(),
                    block.len() * block.npol,
                    "prediction output length"
                );
                self.learn(block, TapsKind::Imaging)?;
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
                    Readback::Prediction(out),
                    params,
                )
            }
            Work::ResidualGrid {
                model,
                acc,
                residual_out,
            } => {
                self.learn(block, TapsKind::Imaging)?;
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
                let grid = grid_buffer(storage);
                self.run(
                    Kernel::Residual,
                    block,
                    &targets,
                    Some(sumwt),
                    Some(grid),
                    Some(buffer),
                    readback,
                    params,
                )
            }
        }
    }
}
