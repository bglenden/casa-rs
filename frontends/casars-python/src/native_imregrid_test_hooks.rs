// SPDX-License-Identifier: LGPL-3.0-or-later
//! Opt-in SIGINT injection at native imregrid checkpoints.

use std::cell::Cell;
use std::sync::mpsc::Sender;

use casa_imageanalysis::imregrid_test_hooks::{Checkpoint, with_observer};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::native_tasks::{WorkerMessage, signal_test_worker};

thread_local! {
    static NEXT_IMREGRID: Cell<Option<Checkpoint>> = const { Cell::new(None) };
}

#[pyfunction]
pub(crate) fn _arm_imregrid_test(stage: &str) -> PyResult<()> {
    NEXT_IMREGRID.set(Some(match stage {
        "coordinate-batch" => Checkpoint::CoordinateBatchFinished(0),
        "interpolation-batch" => Checkpoint::InterpolationBatchFinished(0),
        "before-publication" => Checkpoint::BeforePublication,
        "publication-accepted" => Checkpoint::PublicationAccepted,
        _ => return Err(PyValueError::new_err("unknown imregrid test checkpoint")),
    }));
    Ok(())
}

pub(super) fn take(surface: &str) -> Option<Checkpoint> {
    if surface == "imregrid" {
        NEXT_IMREGRID.take()
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
            tracing::debug!("imregrid test checkpoint {checkpoint:?}");
            if checkpoint == interruption {
                signal_test_worker(&sender);
            }
        },
        run,
    )
}
