// SPDX-License-Identifier: LGPL-3.0-or-later
//! Test-only checkpoints at lock-free regrid calculation and publication boundaries.

use std::cell::RefCell;

/// A checkpoint on the native task worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Checkpoint {
    /// Coordinate mapping is about to start at this flattened pixel index.
    CoordinateBatchStarted(usize),
    /// This batch of coordinate mappings has finished.
    CoordinateBatchFinished(usize),
    /// Interpolation is about to start at this flattened pixel index.
    InterpolationBatchStarted(usize),
    /// This batch of interpolations has finished.
    InterpolationBatchFinished(usize),
    /// The image is staged and the worker is about to check cancellation.
    BeforePublication,
    /// The final cancellation check has accepted publication.
    PublicationAccepted,
}

type Observer = Box<dyn Fn(Checkpoint)>;

thread_local! {
    static OBSERVER: RefCell<Option<Observer>> = const { RefCell::new(None) };
}

/// Observe regrid checkpoints for this call on the current thread.
pub fn with_observer<T>(observer: impl Fn(Checkpoint) + 'static, run: impl FnOnce() -> T) -> T {
    struct Reset(Option<Observer>);
    impl Drop for Reset {
        fn drop(&mut self) {
            OBSERVER.replace(self.0.take());
        }
    }
    let _reset = Reset(OBSERVER.replace(Some(Box::new(observer))));
    run()
}

pub(crate) fn observe(checkpoint: Checkpoint) {
    OBSERVER.with_borrow(|observer| {
        if let Some(observer) = observer {
            observer(checkpoint);
        }
    });
}
