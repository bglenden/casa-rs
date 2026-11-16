// SPDX-License-Identifier: LGPL-3.0-or-later
//! The bounded two-slot stream: a producer thread fills the next block while
//! the caller consumes the current one.

use std::sync::mpsc;

use super::{BlockShape, BoundedSource, Cancel, NativeBlock, PassError, SourceError};

/// Blocks resident at once: one being consumed and one being filled.
pub const SLOTS: usize = 2;

/// Blocks a source divides one run of times into, at least, where it has
/// that many times. Filling overlaps consuming only between blocks, so a
/// pass that is one block reads and then grids, and one of a few blocks
/// waits on its first and last. On a cold `MeasurementSet` (#932), a
/// standard MFS pass of one block took 601 ms, of four 405 ms and of eight
/// 367 ms, against 497 ms on the MAIN-row route the cutover removed.
pub const OVERLAPPED_BLOCKS: usize = 8;

/// Bytes the stream's blocks hold for a source whose largest block is
/// `block`.
pub(super) const fn bytes(block: BlockShape) -> u64 {
    SLOTS as u64 * block.bytes()
}

/// Traverse `source` to exhaustion, handing each filled block to `consume`
/// in source order, its rows ordered by baseline
/// ([`NativeBlock::order_rows_by_baseline`]). Returns the number of blocks
/// consumed.
///
/// The producer stops at the next block boundary after `cancel` is set or
/// `consume` fails; both threads are joined before this returns. A source
/// that fails fails the stream, though its producer stops, and closes the
/// queue of free blocks, before the consumer reads the failure.
///
/// With `timing` it logs one event under
/// [`casa_imaging_metal::METAL_STAGE_TIMING`] when the stream ends: the
/// blocks, the stream's seconds, the producer's time filling and ordering
/// blocks, the consumer's time in `consume`, and how long the consumer
/// waited for a filled block, which says whether reading the source is on
/// the pass's critical path.
pub(super) fn stream_blocks(
    source: &mut dyn BoundedSource,
    cancel: &Cancel,
    timing: bool,
    mut consume: impl FnMut(&NativeBlock) -> Result<(), PassError>,
) -> Result<u64, PassError> {
    let shape = source.maximum_block();
    let started = std::time::Instant::now();
    std::thread::scope(|scope| {
        let (full_sender, full) = mpsc::sync_channel::<Result<NativeBlock, SourceError>>(SLOTS);
        let (free_sender, free) = mpsc::sync_channel::<NativeBlock>(SLOTS);
        for _ in 0..SLOTS {
            free_sender
                .send(NativeBlock::with_capacity(shape))
                .expect("free queue holds every slot");
        }
        let producer = std::thread::Builder::new()
            .name("imaging-source".to_string())
            .spawn_scoped(scope, move || {
                let mut filling = std::time::Duration::ZERO;
                while let Ok(mut block) = free.recv() {
                    if cancel.is_cancelled() {
                        return filling;
                    }
                    let started = std::time::Instant::now();
                    let filled = match source.fill(&mut block) {
                        // Ordered here, off the consumer's path.
                        Ok(true) => {
                            block.order_rows_by_baseline();
                            Ok(block)
                        }
                        Ok(false) => return filling,
                        Err(error) => Err(error),
                    };
                    filling += started.elapsed();
                    let failed = filled.is_err();
                    if full_sender.send(filled).is_err() || failed {
                        return filling;
                    }
                }
                filling
            })
            .map_err(|_| PassError::ProducerPanicked)?;
        let mut blocks = 0_u64;
        let mut result = Ok(());
        let mut waiting = std::time::Duration::ZERO;
        let mut consuming = std::time::Duration::ZERO;
        let mut full = full.into_iter();
        loop {
            let asked = std::time::Instant::now();
            let Some(message) = full.next() else {
                break;
            };
            waiting += asked.elapsed();
            let block = match message {
                Ok(block) => block,
                Err(error) => {
                    result = Err(PassError::Source(error));
                    break;
                }
            };
            if cancel.is_cancelled() {
                result = Err(PassError::Cancelled);
                break;
            }
            debug_assert!(
                block.len() <= shape.rows
                    && block.channels() <= shape.channels()
                    && block.correlations() <= shape.correlations
                    && block.capacity_bytes() <= shape.bytes(),
                "a source fills blocks within its maximum block"
            );
            let consumed = std::time::Instant::now();
            if let Err(error) = consume(&block) {
                result = Err(error);
                break;
            }
            consuming += consumed.elapsed();
            blocks += 1;
            // A producer that has stopped, at the source's end or after
            // sending its failure, takes no more free blocks; what it sent
            // before it stopped is still read.
            let _ = free_sender.send(block);
        }
        drop(free_sender);
        let filling = producer.join().map_err(|_| PassError::ProducerPanicked)?;
        if timing {
            tracing::info!(
                target: casa_imaging_metal::METAL_STAGE_TIMING,
                blocks,
                seconds = started.elapsed().as_secs_f64(),
                fill_seconds = filling.as_secs_f64(),
                consume_seconds = consuming.as_secs_f64(),
                consumer_wait_seconds = waiting.as_secs_f64(),
                "source stream"
            );
        }
        if result.is_ok() && cancel.is_cancelled() {
            result = Err(PassError::Cancelled);
        }
        result.map(|()| blocks)
    })
}
