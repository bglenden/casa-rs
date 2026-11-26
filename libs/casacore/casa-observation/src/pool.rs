// SPDX-License-Identifier: LGPL-3.0-or-later
//! The bounded pool owned batches are filled into, and the owned batch that
//! gives its buffers back when it is dropped.

use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::ObservationError;
use crate::batch::VisibilityBatch;
use crate::buffers::BatchBuffers;

/// A bounded set of [`BatchBuffers`] that owned batches are filled into:
/// at most `capacity` buffers of at most `envelope` bytes each, free or
/// lent, whatever batches, roles and pieces they are filled with. That
/// product is what the caller admits for the pool; the envelope for a
/// traversal is its estimate's
/// [`peak_buffer_bytes`](crate::Estimate::peak_buffer_bytes), and each live
/// cursor's decode scratch is admitted separately.
///
/// [`Traversal::next_owned`](crate::Traversal::next_owned) takes a free
/// buffer only for a batch, and waits while none is free: that wait is the
/// backpressure a slow consumer applies to the producer. A [`FilledBatch`]
/// gives its buffer back when it is dropped, so a batch shared through an
/// `Arc` comes back when its last user lets go. Cloning the pool shares it,
/// so several cursors, or a producer and the work units packing pieces, can
/// draw on one bound.
///
/// Buffers are allocated as they are first filled and keep their capacity
/// after that, within the envelope: a fill whose batch needs more is
/// refused before anything is allocated, a fill frees what a buffer kept
/// from earlier fills when keeping it would exceed the envelope, and a
/// returned buffer over it is emptied.
///
/// A take waits for a free buffer without limit, unless the handle was made
/// [`Self::waiting_at_most`] a duration: it then fails with
/// [`ObservationError::PoolExhausted`] instead. Without a limit, a thread
/// that takes from a pool must not also be the only holder of every batch
/// the pool has lent, or it waits forever.
#[derive(Clone, Debug)]
pub struct BufferPool {
    shared: Arc<PoolShared>,
    wait: Option<Duration>,
}

#[derive(Debug)]
struct PoolShared {
    free: Mutex<Vec<BatchBuffers>>,
    returned: Condvar,
    capacity: usize,
    envelope: usize,
}

impl BufferPool {
    /// A pool of `capacity` buffers of at most `envelope` bytes each.
    ///
    /// # Panics
    ///
    /// If `capacity` is zero: no batch could ever be filled.
    #[must_use]
    pub fn new(capacity: usize, envelope: usize) -> Self {
        assert!(capacity > 0, "a buffer pool holds at least one buffer");
        Self {
            shared: Arc::new(PoolShared {
                free: Mutex::new(
                    (0..capacity)
                        .map(|_| BatchBuffers::with_envelope(envelope))
                        .collect(),
                ),
                returned: Condvar::new(),
                capacity,
                envelope,
            }),
            wait: None,
        }
    }

    /// A handle on the same buffers whose takes wait at most `limit` for a
    /// free one (zero: never wait). A runtime that must stay cancellable
    /// takes with a short limit and checks for cancellation between tries;
    /// [`Traversal::next_owned`](crate::Traversal::next_owned) refused this
    /// way has consumed nothing, so asking again delivers the same batch.
    #[must_use]
    pub fn waiting_at_most(&self, limit: Duration) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            wait: Some(limit),
        }
    }

    /// Buffers in the pool, free or lent.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.shared.capacity
    }

    /// The most bytes one buffer holds.
    #[must_use]
    pub fn envelope(&self) -> usize {
        self.shared.envelope
    }

    /// Buffers free now.
    #[must_use]
    pub fn free(&self) -> usize {
        self.lock().len()
    }

    /// Bytes the free buffers keep, from their capacities.
    #[must_use]
    pub fn free_bytes(&self) -> usize {
        self.lock().iter().map(BatchBuffers::capacity_bytes).sum()
    }

    /// Fill a free buffer, waiting for one, through `fill`; the buffer goes
    /// back to the pool if `fill` fails or finishes no batch.
    ///
    /// # Errors
    ///
    /// [`ObservationError::PoolExhausted`] when the handle's wait limit
    /// passed with no buffer free (`fill` is then not called); as `fill`
    /// (which refuses a batch over the envelope through
    /// [`BatchBuffers::begin`]); or [`ObservationError::InvalidBatch`] when
    /// it returned without finishing a batch.
    pub fn fill(
        &self,
        fill: impl FnOnce(&mut BatchBuffers) -> Result<(), ObservationError>,
    ) -> Result<FilledBatch, ObservationError> {
        let mut filled = FilledBatch {
            buffers: self.take()?,
            pool: self.clone(),
        };
        fill(&mut filled.buffers)?;
        if filled.buffers.header().is_none() {
            return Err(ObservationError::InvalidBatch {
                reason: "the cursor reported a batch it did not finish".to_string(),
            });
        }
        Ok(filled)
    }

    /// Copy `batch` (typically a compute window of a larger batch) into a
    /// free buffer of this pool, waiting for one ([`BatchBuffers::pack`]):
    /// a piece with storage of its own, and the payload elements copied.
    ///
    /// # Errors
    ///
    /// As [`BatchBuffers::pack`].
    pub fn pack(
        &self,
        batch: &VisibilityBatch<'_>,
    ) -> Result<(FilledBatch, usize), ObservationError> {
        let mut copied = 0;
        let filled = self.fill(|buffers| {
            copied = buffers.pack(batch)?;
            Ok(())
        })?;
        Ok((filled, copied))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<BatchBuffers>> {
        // The buffers hold plain data: a panic elsewhere leaves them usable.
        self.shared
            .free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn take(&self) -> Result<BatchBuffers, ObservationError> {
        let deadline = self.wait.map(|limit| Instant::now() + limit);
        let mut free = self.lock();
        loop {
            if let Some(buffers) = free.pop() {
                return Ok(buffers);
            }
            free = match deadline {
                None => self
                    .shared
                    .returned
                    .wait(free)
                    .unwrap_or_else(PoisonError::into_inner),
                Some(deadline) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return Err(ObservationError::PoolExhausted {
                            capacity: self.shared.capacity,
                        });
                    }
                    self.shared
                        .returned
                        .wait_timeout(free, left)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0
                }
            };
        }
    }

    fn give(&self, mut buffers: BatchBuffers) {
        buffers.clear();
        if buffers.capacity_bytes() > self.shared.envelope {
            // Only a fill that failed after growing an array gets here.
            buffers = BatchBuffers::with_envelope(self.shared.envelope);
        }
        self.lock().push(buffers);
        self.shared.returned.notify_one();
    }
}

/// A batch together with the pooled buffers it was filled into: an owned
/// value a work unit can take, or share read-only through an `Arc`.
///
/// [`VisibilityBatch`] borrows its buffers, so it cannot outlive or leave
/// the caller that filled them. A `FilledBatch` moves with its storage
/// instead, which is how a batch's lifetime is separated from the
/// traversal's: the cursor fills the next buffers while earlier batches are
/// still in use elsewhere, a slow user keeps exactly its own batch's
/// allocation alive, and dropping the batch returns that allocation to its
/// [`BufferPool`].
#[derive(Debug)]
pub struct FilledBatch {
    buffers: BatchBuffers,
    pool: BufferPool,
}

impl FilledBatch {
    /// The batch: views of the owned buffers and of the catalogue they were
    /// checked against.
    #[must_use]
    pub fn batch(&self) -> VisibilityBatch<'_> {
        self.buffers
            .batch()
            .expect("the pool hands out only buffers holding a finished batch")
    }

    /// Bytes the buffers hold, from their capacities.
    #[must_use]
    pub fn capacity_bytes(&self) -> usize {
        self.buffers.capacity_bytes()
    }
}

impl Drop for FilledBatch {
    fn drop(&mut self) {
        self.pool.give(std::mem::take(&mut self.buffers));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use num_complex::Complex32;

    use super::*;
    use crate::buffers::tests::{catalogue, fill};
    use crate::{BatchHeader, CorrelationAxis, FieldId, PartitionId, Roles, Sharing};

    const NO_ROLES: Roles = Roles {
        values: false,
        flags: false,
        weights: false,
        uvw: false,
        time_centroid: false,
        interval: false,
        exposure: false,
        scan: false,
    };

    /// Fill `times` times of the test catalogue's two baselines, two
    /// channels and two products with `roles`' payload.
    fn shaped(
        buffers: &mut BatchBuffers,
        times: usize,
        roles: Roles,
    ) -> Result<(), ObservationError> {
        let header = BatchHeader {
            partition: PartitionId(0),
            data_group: 0,
            times,
            first_channel: 0,
            channels: 2,
            correlations: CorrelationAxis::All,
            roles,
            weights_per_channel: true,
            weight_rule: crate::WeightRule::Stored,
            interval: Sharing::PerSample,
            exposure: Sharing::PerSample,
        };
        let mut fill = buffers.begin(header, &catalogue())?;
        let arrays = fill.arrays();
        let cell = times * 2 * 4;
        arrays
            .time_offsets_s
            .extend((0..times).map(|time| time as f64));
        arrays.field.resize(times, FieldId(0));
        if roles.values {
            arrays.values.resize(cell, Complex32::default());
        }
        if roles.weights {
            arrays.weights.resize(cell, 1.0);
        }
        fill.finish()
    }

    #[test]
    fn a_batch_returns_its_buffers_and_a_full_pool_waits_for_one() {
        let pool = BufferPool::new(1, usize::MAX);
        let first = pool
            .fill(|buffers| fill(buffers, |_, _| {}))
            .expect("first");
        assert_eq!(pool.free(), 0);
        assert_eq!(first.batch().samples(), 4);
        let (sent, received) = mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let second = pool
                    .fill(|buffers| fill(buffers, |_, _| {}))
                    .expect("second");
                sent.send(second.batch().samples()).expect("send");
            });
            std::thread::sleep(Duration::from_millis(20));
            assert!(
                received.try_recv().is_err(),
                "the second fill waits for a buffer"
            );
            drop(first);
            assert_eq!(received.recv().expect("second"), 4);
        });
        assert_eq!(pool.free(), 1, "every buffer is back");
        assert!(
            pool.free_bytes() > 0,
            "returned buffers keep their capacity"
        );
    }

    /// A buffer filled with one batch's values and then another's weights
    /// keeps both allocations unless bounded. A pool buffer never holds more
    /// than its envelope, lent or free, whatever sequence of roles and
    /// shapes it serves; a batch over the envelope is refused before
    /// anything is allocated.
    #[test]
    fn a_pool_buffer_never_holds_more_than_its_envelope() {
        let values = Roles {
            values: true,
            ..NO_ROLES
        };
        let weights = Roles {
            weights: true,
            ..NO_ROLES
        };
        let alone = |times, roles| {
            let mut buffers = BatchBuffers::default();
            shaped(&mut buffers, times, roles).expect("fill");
            buffers.capacity_bytes()
        };
        let (a, b) = (alone(16, values), alone(32, weights));
        let mut unbounded = BatchBuffers::default();
        shaped(&mut unbounded, 16, values).expect("values");
        shaped(&mut unbounded, 32, weights).expect("weights");
        assert!(
            unbounded.capacity_bytes() > a.max(b),
            "unbounded buffers keep both: {} B for fills of {a} B and {b} B",
            unbounded.capacity_bytes()
        );
        let envelope = a.max(b);
        let pool = BufferPool::new(1, envelope);
        for (times, roles) in [(16, values), (32, weights), (16, values), (8, values)] {
            let filled = pool
                .fill(|buffers| shaped(buffers, times, roles))
                .expect("fill");
            assert!(
                filled.capacity_bytes() <= envelope,
                "lent: {times} {roles:?}"
            );
            let (piece, _) = BufferPool::new(1, envelope)
                .pack(&filled.batch().channel_window(0..1).expect("window"))
                .expect("pack");
            assert!(piece.capacity_bytes() <= envelope);
            drop(filled);
            assert!(pool.free_bytes() <= envelope, "free: {times} {roles:?}");
        }
        let over = pool.fill(|buffers| shaped(buffers, 64, values));
        assert!(matches!(over, Err(ObservationError::InvalidRequest { .. })));
        assert!(pool.free_bytes() <= envelope);
        assert_eq!(pool.free(), 1);
    }

    /// A handle with a wait limit fails when every buffer is held, on the
    /// thread that holds them, instead of waiting forever; with one back,
    /// it fills.
    #[test]
    fn a_limited_take_reports_an_exhausted_pool() {
        let pool = BufferPool::new(2, usize::MAX);
        let limited = pool.waiting_at_most(Duration::from_millis(10));
        let held: Vec<FilledBatch> = (0..2)
            .map(|_| {
                limited
                    .fill(|buffers| fill(buffers, |_, _| {}))
                    .expect("fill")
            })
            .collect();
        let started = Instant::now();
        let mut called = false;
        assert!(matches!(
            limited.fill(|_| {
                called = true;
                Ok(())
            }),
            Err(ObservationError::PoolExhausted { capacity: 2 })
        ));
        assert!(!called, "nothing is filled");
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(matches!(
            pool.waiting_at_most(Duration::ZERO)
                .fill(|buffers| fill(buffers, |_, _| {})),
            Err(ObservationError::PoolExhausted { .. })
        ));
        drop(held);
        assert!(limited.fill(|buffers| fill(buffers, |_, _| {})).is_ok());
    }

    #[test]
    fn a_failed_fill_gives_the_buffer_back() {
        let pool = BufferPool::new(1, usize::MAX);
        assert!(
            pool.fill(|buffers| fill(buffers, |_, a| {
                a.flags.pop();
            }))
            .is_err()
        );
        assert!(
            pool.fill(|_| Ok(())).is_err(),
            "a fill that finishes nothing"
        );
        assert_eq!(pool.free(), 1);
        let packed = {
            let source = pool.fill(|buffers| fill(buffers, |_, _| {})).expect("fill");
            let pieces = BufferPool::new(1, usize::MAX);
            let (piece, copied) = pieces
                .pack(&source.batch().channel_window(0..1).expect("window"))
                .expect("pack");
            assert!(std::sync::Arc::ptr_eq(
                piece.batch().catalogue,
                source.batch().catalogue
            ));
            copied
        };
        assert_eq!(packed, 8);
        assert_eq!(pool.free(), 1);
    }
}
