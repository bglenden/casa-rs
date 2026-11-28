// SPDX-License-Identifier: LGPL-3.0-or-later
//! Opt-in SIGINT injection at native imstat calculation boundaries.

use std::cell::Cell;
use std::sync::mpsc::Sender;

use casa_imageanalysis::imstat_test_hooks::{Checkpoint, with_observer};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::native_tasks::{WorkerMessage, signal_test_worker};

thread_local! {
    static NEXT_IMSTAT: Cell<Option<Checkpoint>> = const { Cell::new(None) };
}

#[pyfunction]
pub(crate) fn _arm_imstat_test(stage: &str) -> PyResult<()> {
    NEXT_IMSTAT.set(Some(match stage {
        "plane" => Checkpoint::PlaneFinished,
        "calculated" => Checkpoint::CalculationFinished,
        _ => return Err(PyValueError::new_err("unknown imstat test checkpoint")),
    }));
    Ok(())
}

pub(super) fn take(surface: &str) -> Option<Checkpoint> {
    if surface == "imstat" {
        NEXT_IMSTAT.take()
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
    let sent = Cell::new(false);
    with_observer(
        move |checkpoint| {
            tracing::debug!("imstat test checkpoint {checkpoint:?}");
            if checkpoint == interruption && !sent.replace(true) {
                signal_test_worker(&sender);
            }
        },
        run,
    )
}
