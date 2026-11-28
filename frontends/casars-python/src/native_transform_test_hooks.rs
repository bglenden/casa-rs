// SPDX-License-Identifier: LGPL-3.0-or-later
//! Private signal harness for the opt-in transform test build.

use std::cell::Cell;
use std::sync::mpsc::{Sender, channel};
use std::time::Duration;

use casa_mstransform::test_hooks::{Checkpoint, with_single_row_batches};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::native_tasks::WorkerMessage;

#[derive(Clone, Copy)]
pub(super) enum Control {
    Observe,
    Interrupt(Checkpoint),
}

thread_local! {
    static NEXT_TRANSFORM: Cell<Option<Control>> = const { Cell::new(None) };
}

#[pyfunction]
pub(crate) fn _arm_transform_test(mode: &str) -> PyResult<()> {
    let control = match mode {
        "observe" => Control::Observe,
        "scalar" => Control::Interrupt(Checkpoint::ScalarChunkStaged),
        "visibility" => Control::Interrupt(Checkpoint::VisibilityBatchStaged(0)),
        _ => return Err(PyValueError::new_err("unknown transform test checkpoint")),
    };
    NEXT_TRANSFORM.set(Some(control));
    Ok(())
}

pub(super) fn take(surface: &str) -> Option<Control> {
    if matches!(surface, "mstransform" | "split") {
        NEXT_TRANSFORM.take()
    } else {
        None
    }
}

pub(super) fn run<T>(
    control: Option<Control>,
    sender: &Sender<WorkerMessage>,
    run: impl FnOnce() -> T,
) -> T {
    let Some(control) = control else {
        return run();
    };
    let sender = sender.clone();
    let sent = Cell::new(false);
    with_single_row_batches(
        move |checkpoint| {
            tracing::debug!("transform test checkpoint {checkpoint:?}");
            if matches!(control, Control::Interrupt(target) if target == checkpoint)
                && !sent.replace(true)
            {
                // The checkpoint has no shared table write lock. The collector
                // sends SIGINT and checks it before releasing this test worker.
                let (resume, wait) = channel();
                sender.send(WorkerMessage::TestInterrupt(resume)).unwrap();
                assert!(matches!(
                    wait.recv_timeout(Duration::from_secs(15)),
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
                ));
            }
        },
        run,
    )
}
