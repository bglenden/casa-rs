// SPDX-License-Identifier: LGPL-3.0-or-later
//! Opt-in SIGINT injection at native immoments checkpoints.

use std::cell::Cell;
use std::sync::mpsc::Sender;

use casa_imageanalysis::immoments_test_hooks::{Checkpoint, with_observer};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::native_tasks::{WorkerMessage, signal_test_worker};

thread_local! {
    static NEXT_IMMOMENTS: Cell<Option<Checkpoint>> = const { Cell::new(None) };
}

#[pyfunction]
pub(crate) fn _arm_immoments_test(stage: &str) -> PyResult<()> {
    NEXT_IMMOMENTS.set(Some(match stage {
        "profile-batch" => Checkpoint::ProfileBatchFinished(0),
        "before-publication" => Checkpoint::BeforePublication,
        "during-publication" => Checkpoint::ProductPublished(0),
        _ => return Err(PyValueError::new_err("unknown immoments test checkpoint")),
    }));
    Ok(())
}

pub(super) fn take(surface: &str) -> Option<Checkpoint> {
    if surface == "immoments" {
        NEXT_IMMOMENTS.take()
    } else {
        None
    }
}

pub(super) fn run<T>(
    interruption: Option<Checkpoint>,
    sender: &Sender<WorkerMessage>,
    run: impl FnOnce() -> T,
) -> T {
    let Some(interruption) = interruption else {
        return run();
    };
    let sender = sender.clone();
    with_observer(
        move |checkpoint| {
            tracing::debug!("immoments test checkpoint {checkpoint:?}");
            if checkpoint == interruption {
                signal_test_worker(&sender);
            }
        },
        run,
    )
}
