// SPDX-License-Identifier: LGPL-3.0-or-later
//! Example A's work units: each native channel of the image is one work
//! unit that grids its channel window of every batch into its own plane,
//! on its own thread. One unit is held back on every batch, and the probe
//! measures what the others can do meanwhile under two ownership
//! strategies.
//!
//! - [`Strategy::SharedBatch`]: every unit borrows its window of one
//!   shared batch ([`FilledBatch`] behind an `Arc`). Nothing is copied, but
//!   a batch's whole allocation stays live until the slowest unit is done
//!   with it, and the cursor can refill a buffer only then.
//! - [`Strategy::PackedPieces`]: the producer copies each unit's window into
//!   storage of its own ([`FilledBatch::pack`]) and refills the batch buffer
//!   at once. The copy is the price; the slow unit pins only its pieces.

use std::fmt;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use casa_imaging_operator::{
    CpuBackend, GridBackend, MeasurementOperator, Mode, ModeSet, NativeRow, PlaneRange, RowContext,
    SampleBuffer, SpectralAxis, SpectralKernel, SpectralResampler, WeightingGeneration, Work,
};
use casa_observation::{
    BatchBuffers, FilledBatch, Observation, OwnedEvent, PartitionId, VisibilityBatch,
};
use ndarray::Array2;

use crate::source::{per_times, request};

/// How long the held-back unit waits before each batch.
const DELAY: Duration = Duration::from_millis(60);
/// Batch buffers the shared strategy circulates.
const SHARED_POOL: usize = 2;
/// Pieces each unit may hold under the packed strategy.
const PIECES_PER_UNIT: usize = 2;
/// Whole times per batch: four of the twelve, three batches.
const BATCH_TIMES: usize = 4;

/// How units get their windows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Strategy {
    /// Borrow from one shared batch.
    SharedBatch,
    /// Own a packed copy.
    PackedPieces,
}

/// What one run measured.
pub struct Report {
    /// Each unit's dirty plane (normalized) and `sumwt`.
    pub planes: Vec<(Array2<f32>, f64)>,
    /// When each unit finished, from the start.
    pub unit_done: Vec<Duration>,
    /// Time the producer waited for storage to come back.
    pub producer_stall: Duration,
    /// Batch buffers ever allocated, and the capacity of the largest.
    pub batch_buffers: usize,
    pub batch_buffer_bytes: usize,
    /// Most batch-buffer bytes not back in the pool at once.
    pub peak_batch_bytes: usize,
    /// Most piece bytes held by units at once.
    pub peak_piece_bytes: usize,
    /// Payload elements copied into pieces.
    pub copied_elements: usize,
    /// Fills made while the held-back unit still had an earlier batch.
    pub fills_while_held: usize,
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ms = |d: Duration| d.as_secs_f64() * 1e3;
        write!(
            f,
            "units done at [{}] ms; producer stalled {:.1} ms; {} batch buffers of {} B, \
             peak {} B out of the pool; peak pieces {} B; copied {} elements; {} fills while held",
            self.unit_done
                .iter()
                .map(|d| format!("{:.1}", ms(*d)))
                .collect::<Vec<_>>()
                .join(", "),
            ms(self.producer_stall),
            self.batch_buffers,
            self.batch_buffer_bytes,
            self.peak_batch_bytes,
            self.peak_piece_bytes,
            self.copied_elements,
            self.fills_while_held,
        )
    }
}

impl Report {
    /// The behaviour each strategy must show: sharing holds the fast units
    /// back behind the slow one's batch; packing lets them finish while it
    /// still works, at the price of the copy, with storage bounded either
    /// way.
    pub fn check(&self, strategy: Strategy) {
        let slow = self.unit_done[0];
        let fast = *self.unit_done[1..].iter().max().expect("fast units");
        assert!(slow >= 3 * DELAY, "the held-back unit waits on every batch");
        match strategy {
            Strategy::SharedBatch => {
                assert!(
                    fast >= DELAY,
                    "a fast unit waits for the slow unit's buffer: {self}"
                );
                assert!(self.batch_buffers <= SHARED_POOL);
                assert!(self.peak_batch_bytes <= SHARED_POOL * self.batch_buffer_bytes);
                assert_eq!(self.copied_elements, 0);
                assert!(self.producer_stall > Duration::ZERO);
                assert!(self.fills_while_held >= 2, "{self}");
            }
            Strategy::PackedPieces => {
                assert!(
                    fast < DELAY,
                    "fast units finish while the slow one works: {self}"
                );
                assert_eq!(self.batch_buffers, 1, "one batch buffer, refilled at once");
                assert!(self.fills_while_held >= 2, "{self}");
                assert!(self.copied_elements > 0);
            }
        }
    }
}

/// What a unit receives.
enum Item {
    Shared(Arc<FilledBatch>),
    Piece(Box<FilledBatch>),
}

/// Per-unit gridding state: one channel's operator plane.
struct Unit {
    operator: MeasurementOperator,
    resampler: SpectralResampler,
    weighting: WeightingGeneration,
    accumulator: casa_imaging_operator::GridAccumulator,
    samples: SampleBuffer,
    backend: CpuBackend,
}

impl Unit {
    fn new(operator: MeasurementOperator, axis: SpectralAxis) -> Self {
        let accumulator =
            operator.accumulator(PlaneRange { start: 0, end: 1 }, None, ModeSet::DATA_PSF);
        let npol = operator.polarization().correlations().len();
        Self {
            operator,
            resampler: SpectralResampler::channel_local(axis, SpectralKernel::Linear),
            weighting: WeightingGeneration::Natural { taper: None },
            accumulator,
            samples: SampleBuffer::new(npol),
            backend: CpuBackend::new(),
        }
    }

    /// Grid `batch`'s only channel, every correlation.
    fn grid(&mut self, batch: &VisibilityBatch<'_>) {
        assert_eq!(batch.channel_count(), 1);
        let values = batch.group.values.expect("values");
        let validity = batch.validity();
        let weights = batch.group.weights.expect("weights");
        let uvw = batch.group.uvw.expect("uvw");
        let frequency = [batch.frequency.hz()[0]];
        let routed = [0, 1, 2, 3];
        self.samples.clear();
        for sample in 0..batch.samples() {
            let coordinate = batch.sample(sample);
            let row_values = routed.map(|c| values[[sample, 0, c]]);
            let row_weights = routed.map(|c| weights[[sample, 0, c]]);
            let row_flags = routed.map(|c| validity.excluded(sample, 0, c));
            let row = NativeRow {
                uvw_m: uvw.metres_at(sample),
                phase_shift_m: 0.0,
                pointing_offset_rad: [0.0; 2],
                frequencies_hz: &frequency,
                values: &row_values,
                weights: &row_weights,
                flags: &row_flags,
                row_flag: false,
                context: RowContext {
                    original_uvw_m: None,
                    time_s: coordinate.time_offset_s,
                    antennas: [
                        coordinate.baseline.antenna1.0,
                        coordinate.baseline.antenna2.0,
                    ],
                    antenna_types: [0, 0],
                    parallactic_angle_rad: [0.0; 2],
                    field: coordinate.field.0,
                    spectral_window: 0,
                },
            };
            self.resampler
                .place(&self.operator, &self.weighting, &row, &mut self.samples)
                .expect("place");
        }
        for mode in [Mode::Data, Mode::Psf] {
            self.backend
                .apply(
                    &self.samples.block(),
                    self.operator.cf(),
                    Work::Grid {
                        mode,
                        acc: &mut self.accumulator,
                    },
                )
                .expect("grid");
        }
    }

    fn finish(self) -> (Array2<f32>, f64) {
        let images = self.operator.finish(self.accumulator).expect("finish");
        let sumwt = images.data_sumwt(0, 0, 0);
        (
            images.data(0, 0, 0).mapv(|v| (f64::from(v) / sumwt) as f32),
            sumwt,
        )
    }
}

/// Bytes out of the pool and in units, shared with the units.
#[derive(Default)]
struct Ledger {
    batch_bytes: usize,
    peak_batch_bytes: usize,
    piece_bytes: usize,
    peak_piece_bytes: usize,
    /// Items sent to the held-back unit and not yet done.
    held: usize,
}

/// Run every channel of `channels` as a work unit over `observation`,
/// holding unit 0 back by [`DELAY`] before each batch.
pub fn run(
    observation: &dyn Observation,
    strategy: Strategy,
    template: &MeasurementOperator,
    axis: &SpectralAxis,
    channels: std::ops::Range<usize>,
) -> Report {
    let units = channels.len();
    let catalogue = observation.catalogue();
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let mut traversal = observation
        .bind_read(&per_times(
            observation,
            request(PartitionId(0), channels.clone(), true, 0),
            BATCH_TIMES,
        ))
        .expect("bind");
    let (free_tx, free_rx) = mpsc::channel::<BatchBuffers>();
    let started = Instant::now();
    let mut report = Report {
        planes: Vec::new(),
        unit_done: vec![Duration::ZERO; units],
        producer_stall: Duration::ZERO,
        batch_buffers: 0,
        batch_buffer_bytes: 0,
        peak_batch_bytes: 0,
        peak_piece_bytes: 0,
        copied_elements: 0,
        fills_while_held: 0,
    };
    std::thread::scope(|scope| {
        let mut senders = Vec::new();
        let mut handles = Vec::new();
        let mut piece_returns = Vec::new();
        for unit in 0..units {
            let (tx, rx) = mpsc::channel::<Item>();
            let (piece_tx, piece_rx) = mpsc::channel::<BatchBuffers>();
            senders.push(tx);
            piece_returns.push(piece_rx);
            let free_tx = free_tx.clone();
            let ledger = ledger.clone();
            let operator = clone_operator(template);
            let axis = SpectralAxis::new(axis.centre_hz(unit as u32), axis.increment_hz(), 1)
                .expect("axis");
            handles.push(scope.spawn(move || {
                let mut state = Unit::new(operator, axis);
                let mut done = Duration::ZERO;
                for item in rx {
                    if unit == 0 {
                        std::thread::sleep(DELAY);
                    }
                    match item {
                        Item::Shared(filled) => {
                            let batch = filled.batch(catalogue);
                            let window = batch.channel_window(unit..unit + 1).expect("window");
                            state.grid(&window);
                            if let Some(filled) = Arc::into_inner(filled) {
                                ledger.lock().expect("ledger").batch_bytes -=
                                    filled.capacity_bytes();
                                let _ = free_tx.send(filled.into_buffers());
                            }
                        }
                        Item::Piece(piece) => {
                            let piece = *piece;
                            state.grid(&piece.batch(catalogue));
                            let bytes = piece.capacity_bytes();
                            ledger.lock().expect("ledger").piece_bytes -= bytes;
                            let _ = piece_tx.send(piece.into_buffers());
                        }
                    }
                    if unit == 0 {
                        ledger.lock().expect("ledger").held -= 1;
                    }
                    done = started.elapsed();
                }
                (state.finish(), done)
            }));
        }
        drop(free_tx);
        let mut pool: Vec<BatchBuffers> = Vec::new();
        let mut pieces: Vec<Vec<BatchBuffers>> = (0..units).map(|_| Vec::new()).collect();
        let mut pieces_out = vec![0_usize; units];
        // The producer asks what comes next before lending a buffer, so no
        // batch count is needed in advance and nothing is lent at the end.
        loop {
            match traversal.next_owned(None).expect("step") {
                OwnedEvent::NeedBuffers => {}
                OwnedEvent::GroupEnd(..) => continue,
                OwnedEvent::End(_) => break,
                OwnedEvent::Batch(_) => unreachable!("no buffers were lent"),
            }
            // A batch buffer: from the pool, a new one while the pool may
            // grow, else wait for one to come back.
            let limit = match strategy {
                Strategy::SharedBatch => SHARED_POOL,
                Strategy::PackedPieces => 1,
            };
            while let Ok(buffers) = free_rx.try_recv() {
                pool.push(buffers);
            }
            let buffers = if let Some(buffers) = pool.pop() {
                buffers
            } else if report.batch_buffers < limit {
                report.batch_buffers += 1;
                BatchBuffers::default()
            } else {
                let waited = Instant::now();
                let buffers = free_rx.recv().expect("a buffer comes back");
                report.producer_stall += waited.elapsed();
                buffers
            };
            if ledger.lock().expect("ledger").held > 0 {
                report.fills_while_held += 1;
            }
            let OwnedEvent::Batch(filled) = traversal.next_owned(Some(buffers)).expect("read")
            else {
                unreachable!("a batch was announced");
            };
            report.batch_buffer_bytes = report.batch_buffer_bytes.max(filled.capacity_bytes());
            match strategy {
                Strategy::SharedBatch => {
                    let mut book = ledger.lock().expect("ledger");
                    book.batch_bytes += filled.capacity_bytes();
                    book.peak_batch_bytes = book.peak_batch_bytes.max(book.batch_bytes);
                    book.held += 1;
                    drop(book);
                    let shared = Arc::new(filled);
                    for sender in &senders {
                        sender
                            .send(Item::Shared(shared.clone()))
                            .expect("unit alive");
                    }
                }
                Strategy::PackedPieces => {
                    let batch = filled.batch(catalogue);
                    // Fast units first, so a full slow queue holds back no one else.
                    for unit in (1..units).chain([0]) {
                        while let Ok(buffers) = piece_returns[unit].try_recv() {
                            pieces[unit].push(buffers);
                            pieces_out[unit] -= 1;
                        }
                        let storage = if let Some(storage) = pieces[unit].pop() {
                            storage
                        } else if pieces_out[unit] < PIECES_PER_UNIT {
                            BatchBuffers::default()
                        } else {
                            let waited = Instant::now();
                            let storage = piece_returns[unit].recv().expect("a piece comes back");
                            report.producer_stall += waited.elapsed();
                            pieces_out[unit] -= 1;
                            storage
                        };
                        let window = batch.channel_window(unit..unit + 1).expect("window");
                        let (piece, copied) =
                            FilledBatch::pack(&window, storage, catalogue).expect("pack");
                        report.copied_elements += copied;
                        pieces_out[unit] += 1;
                        let mut ledger = ledger.lock().expect("ledger");
                        if unit == 0 {
                            ledger.held += 1;
                        }
                        ledger.piece_bytes += piece.capacity_bytes();
                        ledger.peak_piece_bytes = ledger.peak_piece_bytes.max(ledger.piece_bytes);
                        drop(ledger);
                        senders[unit]
                            .send(Item::Piece(Box::new(piece)))
                            .expect("unit alive");
                    }
                    let bytes = filled.capacity_bytes();
                    let mut ledger = ledger.lock().expect("ledger");
                    ledger.peak_batch_bytes = ledger.peak_batch_bytes.max(bytes);
                    drop(ledger);
                    pool.push(filled.into_buffers());
                }
            }
        }
        drop(senders);
        for (unit, handle) in handles.into_iter().enumerate() {
            let (plane, done) = handle.join().expect("unit");
            report.planes.push(plane);
            report.unit_done[unit] = done;
        }
    });
    let ledger = ledger.lock().expect("ledger");
    report.peak_batch_bytes = ledger.peak_batch_bytes;
    report.peak_piece_bytes = ledger.peak_piece_bytes;
    report
}

/// A fresh operator like `template`, for one unit's thread.
fn clone_operator(template: &MeasurementOperator) -> MeasurementOperator {
    crate::operator(template.basis().planes())
}
