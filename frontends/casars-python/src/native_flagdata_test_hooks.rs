// SPDX-License-Identifier: LGPL-3.0-or-later
//! Private signal harness for the opt-in flagdata test build.

use std::cell::Cell;
use std::sync::mpsc::Sender;

use casa_flagging::test_hooks::{Checkpoint, with_observer};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::native_tasks::{WorkerMessage, signal_test_worker};

#[derive(Clone, Copy)]
pub(super) enum Control {
    Observe,
    Interrupt(Checkpoint),
}

thread_local! {
    static NEXT_FLAGDATA: Cell<Option<Control>> = const { Cell::new(None) };
}

#[pyfunction]
pub(crate) fn _arm_flagdata_test(mode: &str) -> PyResult<()> {
    let control = match mode {
        "observe" => Control::Observe,
        "clip" => Control::Interrupt(Checkpoint::ClipBatchStaged),
        "before_commit" => Control::Interrupt(Checkpoint::BeforeCommit),
        "commit_accepted" => Control::Interrupt(Checkpoint::CommitAccepted),
        _ => return Err(PyValueError::new_err("unknown flagdata test checkpoint")),
    };
    NEXT_FLAGDATA.set(Some(control));
    Ok(())
}

pub(super) fn take(surface: &str) -> Option<Control> {
    if surface == "flagdata" {
        NEXT_FLAGDATA.take()
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
    with_observer(
        move |checkpoint| {
            tracing::debug!("flagdata test checkpoint {checkpoint:?}");
            if matches!(control, Control::Interrupt(target) if target == checkpoint)
                && !sent.replace(true)
            {
                signal_test_worker(&sender);
            }
        },
        run,
    )
}
