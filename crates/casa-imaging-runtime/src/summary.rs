// SPDX-License-Identifier: LGPL-3.0-or-later
//! The run summary (plan section 8.3): the one record a run leaves, a small
//! JSON document beside its products.

use std::path::Path;
use std::time::Instant;

use serde::Serialize;

use crate::pass::{BackendChoice, Cancel};

/// What one run did, O(1) per phase.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RunSummary {
    /// The request as its caller states it.
    pub request: serde_json::Value,
    /// Every phase in the order it ran.
    pub phases: Vec<Phase>,
    /// Workers in the run's team.
    pub workers: usize,
    /// Where the passes gridded.
    pub backend: BackendChoice,
    /// Minor cycles run.
    pub minor_cycles: usize,
    /// Minor-cycle iterations charged to the iteration budget.
    pub minor_iterations: usize,
    /// Products published, by file name.
    pub products: Vec<String>,
}

/// One completed phase.
#[derive(Clone, Debug, Serialize)]
pub struct Phase {
    /// What the phase did.
    pub name: String,
    /// Wall-clock seconds.
    pub seconds: f64,
    /// The process's peak resident memory when the phase ended, in bytes.
    pub peak_rss: u64,
}

/// A phase did not start because the run was cancelled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("the run was cancelled")]
pub struct Cancelled;

impl RunSummary {
    /// Write the summary to `path` as pretty-printed JSON.
    ///
    /// # Errors
    ///
    /// The file could not be written.
    pub fn write(&self, path: &Path) -> std::io::Result<()> {
        let text = serde_json::to_string_pretty(self).map_err(std::io::Error::other)?;
        std::fs::write(path, text + "\n")
    }
}

/// Run one phase named `name`, unless `cancel` is set, and record its wall
/// time and the peak resident memory after it in `summary`.
///
/// A phase that fails is not recorded.
///
/// # Errors
///
/// [`Cancelled`] (through `E`) when cancellation was requested before the
/// phase started; otherwise the step's error.
pub fn run_phase<T, E: From<Cancelled>>(
    name: impl Into<String>,
    cancel: &Cancel,
    summary: &mut RunSummary,
    step: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    if cancel.is_cancelled() {
        return Err(Cancelled.into());
    }
    let started = Instant::now();
    let value = step()?;
    summary.phases.push(Phase {
        name: name.into(),
        seconds: started.elapsed().as_secs_f64(),
        peak_rss: peak_rss(),
    });
    Ok(value)
}

/// The process's peak resident set, in bytes.
fn peak_rss() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` fills the whole struct when it returns 0.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(
        status, 0,
        "getrusage(RUSAGE_SELF) fails only on a bad pointer"
    );
    // SAFETY: initialised by the successful call above.
    let maximum = u64::try_from(unsafe { usage.assume_init() }.ru_maxrss).expect("a resident size");
    // macOS reports bytes, Linux kibibytes.
    if cfg!(target_os = "macos") {
        maximum
    } else {
        maximum.saturating_mul(1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_phase_is_recorded_once_it_succeeds() {
        let cancel = Cancel::new();
        let mut summary = RunSummary::default();
        let value =
            run_phase("first", &cancel, &mut summary, || Ok::<_, Cancelled>(7)).expect("runs");
        assert_eq!(value, 7);
        assert_eq!(summary.phases.len(), 1);
        assert_eq!(summary.phases[0].name, "first");
        assert!(summary.phases[0].peak_rss > 0);
        cancel.cancel();
        let mut ran = false;
        let cancelled = run_phase("second", &cancel, &mut summary, || {
            ran = true;
            Ok::<_, Cancelled>(())
        });
        assert_eq!(cancelled, Err(Cancelled));
        assert!(!ran);
        assert_eq!(summary.phases.len(), 1);
    }
}
