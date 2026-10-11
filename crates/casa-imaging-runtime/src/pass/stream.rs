// SPDX-License-Identifier: LGPL-3.0-or-later
//! The bounded two-slot stream: a producer thread fills the next block while
//! the caller consumes the current one.

use std::sync::mpsc;

use super::{BlockShape, BoundedSource, Cancel, NativeBlock, PassError, SourceError};

/// Blocks resident at once: one being consumed and one being filled.
const SLOTS: usize = 2;

/// Bytes the stream's blocks hold for a source whose largest block is
/// `block`.
pub(super) const fn bytes(block: BlockShape) -> u64 {
    SLOTS as u64 * block.bytes()
}

/// Traverse `source` to exhaustion, handing each filled block to `consume`
/// in source order. Returns the number of blocks consumed.
///
/// The producer stops at the next block boundary after `cancel` is set or
/// `consume` fails; both threads are joined before this returns.
pub(super) fn stream_blocks(
    source: &mut dyn BoundedSource,
    cancel: &Cancel,
    mut consume: impl FnMut(&NativeBlock) -> Result<(), PassError>,
) -> Result<u64, PassError> {
    let shape = source.maximum_block();
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
                while let Ok(mut block) = free.recv() {
                    if cancel.is_cancelled() {
                        return;
                    }
                    let filled = match source.fill(&mut block) {
                        Ok(true) => Ok(block),
                        Ok(false) => return,
                        Err(error) => Err(error),
                    };
                    let failed = filled.is_err();
                    if full_sender.send(filled).is_err() || failed {
                        return;
                    }
                }
            })
            .map_err(|_| PassError::ProducerPanicked)?;
        let mut blocks = 0_u64;
        let mut result = Ok(());
        for message in full {
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
                    && block.channels() <= shape.channels
                    && block.correlations() <= shape.correlations,
                "a source fills blocks within its maximum block"
            );
            if let Err(error) = consume(&block) {
                result = Err(error);
                break;
            }
            blocks += 1;
            if free_sender.send(block).is_err() {
                break;
            }
        }
        drop(free_sender);
        producer.join().map_err(|_| PassError::ProducerPanicked)?;
        if result.is_ok() && cancel.is_cancelled() {
            result = Err(PassError::Cancelled);
        }
        result.map(|()| blocks)
    })
}
