// SPDX-License-Identifier: LGPL-3.0-or-later
//! Test-only checkpoints on the calibration task worker.

use std::cell::RefCell;

/// A completed or starting unit of work, or a publication boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Checkpoint {
    /// One visibility batch has been read and is about to be consumed.
    ObservationBatchStarted(usize),
    /// One visibility batch has been accumulated.
    ObservationBatchFinished(usize),
    /// One batch of read-only prior-calibration inputs is about to be read.
    PriorBatchStarted(usize),
    /// One batch of read-only prior-calibration rows has been processed.
    PriorBatchFinished(usize),
    /// The legacy solver is about to start its input reader thread.
    LegacyPipelineStarted,
    /// A cross-hand delay buffer has been transformed.
    DelayBufferFinished(usize),
    /// One legacy row batch is about to be accumulated.
    LegacyBatchStarted(usize),
    /// One legacy row batch has been accumulated.
    LegacyBatchFinished(usize),
    /// The bandpass solver is about to fit this channel.
    BandpassChannelStarted(usize),
    /// The bandpass solver has fitted this channel.
    BandpassChannelFinished(usize),
    /// The polynomial bandpass solver is about to process this spectrum.
    PolynomialSpectrumStarted(usize),
    /// The polynomial bandpass solver has processed this spectrum.
    PolynomialSpectrumFinished(usize),
    /// All spectra have been collected for this polynomial fit.
    PolynomialSpectraFinished,
    /// The fully staged product is about to be accepted or discarded.
    BeforePublication,
    /// The final guard accepted publication.
    PublicationAccepted,
}

struct Observer {
    callback: Box<dyn Fn(Checkpoint)>,
    small_batches: bool,
}

thread_local! {
    static OBSERVER: RefCell<Option<Observer>> = const { RefCell::new(None) };
}

/// Observe lock-free solver checkpoints on the current thread.
pub fn with_observer<T>(
    callback: impl Fn(Checkpoint) + 'static,
    small_batches: bool,
    run: impl FnOnce() -> T,
) -> T {
    struct Reset(Option<Observer>);
    impl Drop for Reset {
        fn drop(&mut self) {
            OBSERVER.replace(self.0.take());
        }
    }
    let _reset = Reset(OBSERVER.replace(Some(Observer {
        callback: Box::new(callback),
        small_batches,
    })));
    run()
}

pub(crate) fn observe(checkpoint: Checkpoint) {
    OBSERVER.with_borrow(|observer| {
        if let Some(observer) = observer {
            (observer.callback)(checkpoint);
        }
    });
}

pub(crate) fn small_batches() -> bool {
    OBSERVER.with_borrow(|observer| {
        observer
            .as_ref()
            .is_some_and(|observer| observer.small_batches)
    })
}
