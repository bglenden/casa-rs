// SPDX-License-Identifier: LGPL-3.0-or-later
//! Test-only checkpoints at lock-free moment calculation and publication boundaries.

use std::cell::RefCell;

/// A checkpoint on the native task worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Checkpoint {
    /// The batch of profiles at this flattened index is about to start.
    ProfileBatchStarted(usize),
    /// The batch of profiles at this flattened index has finished.
    ProfileBatchFinished(usize),
    /// All requested products are staged and the task is about to check cancellation.
    BeforePublication,
    /// The product at this index has been published.
    ProductPublished(usize),
}

type Observer = Box<dyn Fn(Checkpoint)>;

thread_local! {
    static OBSERVER: RefCell<Option<Observer>> = const { RefCell::new(None) };
}

/// Observe moment checkpoints for this call on the current thread.
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
