// SPDX-License-Identifier: LGPL-3.0-or-later
//! Opt-in checkpoints for the public flagdata signal tests.

use std::cell::RefCell;

/// Flagging boundaries that hold no shared write lock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Checkpoint {
    /// A clip batch is about to read and calculate its flags.
    ClipBatchStarted,
    /// The current clip batch has staged its edits in memory.
    ClipBatchStaged,
    /// A batch of resident FLAG updates is about to be staged.
    FlagStageStarted(usize),
    /// A batch of resident FLAG updates has been staged.
    FlagStageFinished(usize),
    /// A reporting row is about to be read.
    ReportRowStarted,
    /// A reporting row has been counted.
    ReportRowFinished,
    /// MAIN publication has been planned and the write lock has yet to be acquired.
    BeforeCommit,
    /// MAIN has been committed and its lock released before the task writes HISTORY.
    MainCommitted,
}

type Observer = Box<dyn Fn(Checkpoint)>;

thread_local! {
    static OBSERVER: RefCell<Option<Observer>> = const { RefCell::new(None) };
}

/// Observe lock-free checkpoints and use small batches for this run.
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

pub(super) fn clip_batch_rows(default: usize) -> usize {
    OBSERVER.with_borrow(|observer| if observer.is_some() { 1 } else { default })
}

pub(super) fn flag_stage_batch_rows(default: usize) -> usize {
    OBSERVER.with_borrow(|observer| if observer.is_some() { 2 } else { default })
}

pub(super) fn observe(checkpoint: Checkpoint) {
    OBSERVER.with_borrow(|observer| {
        if let Some(observer) = observer {
            observer(checkpoint);
        }
    });
}
