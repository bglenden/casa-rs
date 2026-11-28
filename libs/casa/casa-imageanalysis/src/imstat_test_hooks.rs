// SPDX-License-Identifier: LGPL-3.0-or-later
//! Test-only observations of lock-free `imstat` calculation boundaries.

use std::cell::RefCell;

/// A calculation boundary on the native task worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Checkpoint {
    /// The next display plane is about to be calculated.
    PlaneStarted,
    /// A display plane has finished its statistics calculation.
    PlaneFinished,
    /// Statistics and any plane report are ready for the task adapter.
    CalculationFinished,
}

type Observer = Box<dyn Fn(Checkpoint)>;

thread_local! {
    static OBSERVER: RefCell<Option<Observer>> = const { RefCell::new(None) };
}

/// Observe calculation boundaries on the current thread for this call.
#[doc(hidden)]
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
