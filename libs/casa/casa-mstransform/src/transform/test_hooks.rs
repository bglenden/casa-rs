// SPDX-License-Identifier: LGPL-3.0-or-later
//! Opt-in test checkpoints for transform staging.

use std::cell::RefCell;

type Observer = Box<dyn Fn(Checkpoint)>;

thread_local! {
    static CONTROLS: RefCell<Option<Observer>> = const { RefCell::new(None) };
}

/// A boundary reached while operating on the transform's private staging.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Checkpoint {
    /// A bounded chunk of a streamed MAIN scalar column was staged.
    ScalarChunkStaged,
    /// Starting a visibility batch at the given zero-based output row.
    VisibilityBatchStarted(usize),
    /// The visibility batch starting at this output row was staged.
    VisibilityBatchStaged(usize),
    /// All writes to private staging finished, before the final rename.
    BeforePublication,
}

struct Restore(Option<Observer>);

impl Drop for Restore {
    fn drop(&mut self) {
        CONTROLS.set(self.0.take());
    }
}

/// Run a transform with single-row batches and a thread-local test observer.
pub fn with_single_row_batches<T>(
    observe: impl Fn(Checkpoint) + 'static,
    run: impl FnOnce() -> T,
) -> T {
    let _restore = Restore(CONTROLS.replace(Some(Box::new(observe))));
    run()
}

pub(super) fn limit_batch_rows(planned: usize) -> usize {
    CONTROLS.with_borrow(|observer| if observer.is_some() { 1 } else { planned })
}

pub(super) fn notify(checkpoint: Checkpoint) {
    CONTROLS.with_borrow(|observer| {
        if let Some(observe) = observer {
            observe(checkpoint);
        }
    });
}
