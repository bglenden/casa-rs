// SPDX-License-Identifier: LGPL-3.0-or-later
//! The bounded pool owned batches are filled into, and the owned batch that
//! gives its buffers back when it is dropped.

use std::sync::{Arc, Condvar, Mutex, PoisonError};

use crate::ObservationError;
use crate::batch::VisibilityBatch;
use crate::buffers::BatchBuffers;

/// A bounded set of [`BatchBuffers`] that owned batches are filled into:
/// the memory a traversal's batches can hold at once, which the caller
/// admits as `capacity` times the estimate's
/// [`peak_held_bytes`](crate::Estimate::peak_held_bytes).
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
/// after that. A thread that takes from a pool must not also be the only
/// holder of every batch the pool has lent, or it waits forever.
#[derive(Clone, Debug)]
pub struct BufferPool {
    shared: Arc<PoolShared>,
}

#[derive(Debug)]
struct PoolShared {
    free: Mutex<Vec<BatchBuffers>>,
    returned: Condvar,
    capacity: usize,
}

impl BufferPool {
    /// A pool of `capacity` buffers.
    ///
    /// # Panics
    ///
    /// If `capacity` is zero: no batch could ever be filled.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "a buffer pool holds at least one buffer");
        Self {
            shared: Arc::new(PoolShared {
                free: Mutex::new((0..capacity).map(|_| BatchBuffers::default()).collect()),
                returned: Condvar::new(),
                capacity,
            }),
        }
    }

    /// Buffers in the pool, free or lent.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.shared.capacity
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
    /// As `fill`, or [`ObservationError::InvalidBatch`] when it returned
    /// without finishing a batch.
    pub fn fill(
        &self,
        fill: impl FnOnce(&mut BatchBuffers) -> Result<(), ObservationError>,
    ) -> Result<FilledBatch, ObservationError> {
        let mut filled = FilledBatch {
            buffers: self.take(),
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

    fn take(&self) -> BatchBuffers {
        let mut free = self.lock();
        loop {
            if let Some(buffers) = free.pop() {
                return buffers;
            }
            free = self
                .shared
                .returned
                .wait(free)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    fn give(&self, mut buffers: BatchBuffers) {
        buffers.clear();
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
    use std::time::Duration;

    use super::*;
    use crate::buffers::tests::fill;

    #[test]
    fn a_batch_returns_its_buffers_and_a_full_pool_waits_for_one() {
        let pool = BufferPool::new(1);
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

    #[test]
    fn a_failed_fill_gives_the_buffer_back() {
        let pool = BufferPool::new(1);
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
            let pieces = BufferPool::new(1);
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
