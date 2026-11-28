// SPDX-License-Identifier: LGPL-3.0-or-later
//! Test-only checkpoints in statistics loops on the calling thread.

use std::cell::Cell;

/// A boundary around real work in the absolute-deviation pass.
pub enum Checkpoint {
    /// A batch is about to be converted to absolute deviations.
    DeviationBatchStarted,
    /// A batch has been converted to absolute deviations.
    DeviationBatchFinished,
}

type Observer = fn(Checkpoint);

thread_local! {
    static OBSERVER: Cell<Option<Observer>> = const { Cell::new(None) };
}

/// Observe statistics checkpoints on the current thread for this call.
pub fn with_observer<T>(observer: Observer, run: impl FnOnce() -> T) -> T {
    struct Reset(Option<Observer>);
    impl Drop for Reset {
        fn drop(&mut self) {
            OBSERVER.set(self.0);
        }
    }
    let _reset = Reset(OBSERVER.replace(Some(observer)));
    run()
}

pub(crate) fn observe(checkpoint: Checkpoint) {
    if let Some(observer) = OBSERVER.get() {
        observer(checkpoint);
    }
}
