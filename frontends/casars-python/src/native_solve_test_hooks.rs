// SPDX-License-Identifier: LGPL-3.0-or-later
//! Opt-in SIGINT at calibration task checkpoints.

use std::cell::Cell;
use std::sync::mpsc::Sender;

use casa_calibration::solve_test_hooks::{Checkpoint, with_observer};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::native_tasks::{WorkerMessage, signal_test_worker};

thread_local! {
    static NEXT_SOLVE: Cell<Option<Checkpoint>> = const { Cell::new(None) };
}

#[pyfunction]
pub(crate) fn _arm_solve_test(stage: &str, channel: usize) -> PyResult<()> {
    NEXT_SOLVE.set(Some(match stage {
        "apply-batch" => Checkpoint::ApplyBatchFinished(0, 0),
        "apply-strict-batch" => Checkpoint::ApplyBatchFinished(1, 0),
        "apply-trial-batch" => Checkpoint::ApplyTrialBatchFinished(0),
        "apply-before-commit" => Checkpoint::ApplyBeforeCommit,
        "apply-main-committed" => Checkpoint::ApplyMainCommitted,
        "observation-batch" => Checkpoint::ObservationBatchFinished(0),
        "legacy-batch" => Checkpoint::LegacyBatchFinished(0),
        "prior-batch" => Checkpoint::PriorBatchFinished(0),
        "polynomial-spectra-finished" => Checkpoint::PolynomialSpectraFinished,
        "bandpass-channel" => Checkpoint::BandpassChannelFinished(channel),
        "polynomial-spectrum" => Checkpoint::PolynomialSpectrumFinished(0),
        "before-publication" => Checkpoint::BeforePublication,
        "publication-accepted" => Checkpoint::PublicationAccepted,
        _ => {
            return Err(PyValueError::new_err(
                "unknown calibration solve test checkpoint",
            ));
        }
    }));
    Ok(())
}

pub(super) fn take(surface: &str) -> Option<Checkpoint> {
    if matches!(surface, "applycal" | "gaincal" | "bandpass") {
        NEXT_SOLVE.take()
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
            tracing::debug!("solve test checkpoint {checkpoint:?}");
            if checkpoint == interruption {
                signal_test_worker(&sender);
            }
        },
        matches!(
            interruption,
            Checkpoint::ObservationBatchFinished(_)
                | Checkpoint::LegacyBatchFinished(_)
                | Checkpoint::PriorBatchFinished(_)
        ),
        run,
    )
}
