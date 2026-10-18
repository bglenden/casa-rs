// SPDX-License-Identifier: LGPL-3.0-or-later
//! The run summary (plan section 8.3), the one record a run leaves: a small
//! JSON document beside its products, written for every outcome (ADR-0014's
//! final success/failure summary), and the run's diagnostic switches
//! (section 8.1).

use std::path::{Path, PathBuf};
use std::time::Instant;

use casa_imaging_deconvolution::CleanStop;
use casa_imaging_operator::GridPrecision;
use serde::{Deserialize, Serialize, Serializer};

use crate::pass::{BackendChoice, Cancel};
use crate::resources::{ReservationWindow, reserved_memory};

/// The `tracing` target of the [`Diagnostics::stage_timing`] events.
pub const STAGE_TIMING: &str = "casa_imaging::stage_timing";

/// The `tracing` target of the [`Diagnostics::science_trace`] events.
pub const SCIENCE_TRACE: &str = "casa_imaging::science_trace";

/// A run's optional diagnostic output (plan section 8.1). Each switch adds
/// `tracing` events, at `info`, under its own target; none changes what the
/// run computes, and with every switch off a run emits none of them. The
/// provider-contracts catalog owns the defaults.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub struct Diagnostics {
    /// One event per phase ([`STAGE_TIMING`]): the [`Phase`] the summary
    /// records.
    pub stage_timing: bool,
    /// One event per Metal backend and wave
    /// (`casa_imaging::metal_stage_timing`), for each gridding owner and
    /// each row chunk's prediction backend that dispatched: whether its work
    /// completed, its dispatches, samples (and how many it predicted) and
    /// sub-blocks, the host's preparation time and its waits for the device,
    /// and the device's time on its commands; and one event per wave for the
    /// pass's source stream: the producer's fill time and the pass's waits
    /// for filled blocks. A CPU run emits none.
    pub metal_stage_timing: bool,
    /// The controller's science quantities ([`SCIENCE_TRACE`]): the
    /// residual statistics each cleaning decision reads, each minor cycle's
    /// controls and what it cleaned.
    pub science_trace: bool,
}

/// What one run did, O(1) per phase, written with how it ended
/// ([`RunSummary::write`]).
///
/// Every step that can fail runs inside a phase, so for each outcome
/// ([`RunStatus`]) the summary holds:
/// - **completed:** every phase, the totals, the stop reason and the
///   products;
/// - **failed:** the phases that ran, the last not completed, with the
///   minor-cycle totals and stop reason as far as the run got, any products
///   published before the failure, and the error; a run that fails in
///   preparation (request validation, compile, availability, admitting its
///   source) records only its preparation phase, and one that fails in
///   setup (its operators, choosing and forming its team) its setup phase
///   too;
/// - **cancelled:** the phases that ran before the cancellation took
///   effect, the totals so far, and no products, since nothing is
///   published; a run cancelled before it starts records no phase.
///
/// Memory is reported as the process sees it: the resident high-water mark
/// and the reservations of every run in the process.
#[derive(Debug, Serialize)]
pub struct RunSummary {
    /// The request as its caller states it.
    pub request: serde_json::Value,
    /// Why cleaning stopped, with CASA's `stopcode`; `None` for a dirty run,
    /// a run that stopped before deciding, or one whose last minor cycle
    /// cleaned nothing without a stop.
    #[serde(serialize_with = "clean_stop")]
    pub stop: Option<CleanStop>,
    /// Bytes the resource policy lets the process's reservations hold.
    pub capacity_bytes: u64,
    /// Workers in the run's team; zero before the team forms.
    pub workers: usize,
    /// Workers the resource policy allows: more than [`Self::workers`]
    /// when the run's memory did not fit them (#737); zero before the team
    /// forms.
    pub workers_policy: usize,
    /// Why the run has fewer workers than its policy allows, with what the
    /// policy's needed and what was free; `None` when it has as many.
    pub workers_reason: Option<String>,
    /// Where the passes grid.
    pub backend: BackendChoice,
    /// The grid arithmetic the run resolved (`gridprecision`); `None`
    /// before the operators form.
    #[serde(serialize_with = "grid_precision")]
    pub precision: Option<GridPrecision>,
    /// Every phase in the order it ran.
    pub phases: Vec<Phase>,
    /// Minor cycles completed.
    pub minor_cycles: usize,
    /// Minor-cycle iterations charged to the iteration budget.
    pub minor_iterations: usize,
    /// The product images published, by path, in the order they were.
    pub products: Vec<String>,
    /// This run's view of the process's reservations, closed at each phase
    /// end.
    #[serde(skip)]
    reservations: ReservationWindow,
}

/// How a run ended, written beside its [`RunSummary`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum RunStatus {
    /// Every phase completed and the products are published.
    Completed,
    /// A phase failed. Publication replaces the product images one by one,
    /// so a failure while publishing leaves the images published before it
    /// in place, and the summary lists them; a failure before publishing
    /// publishes nothing.
    Failed {
        /// Why.
        error: String,
    },
    /// Cancellation took effect before publishing; nothing was published.
    Cancelled,
}

/// One phase of a run.
#[derive(Clone, Debug, Serialize)]
pub struct Phase {
    /// What the phase did.
    pub name: String,
    /// Whether it completed; a phase that failed is recorded too.
    pub completed: bool,
    /// Wall-clock seconds.
    pub seconds: f64,
    /// The process's resident high-water mark sampled when the phase
    /// ended, in bytes: it covers every earlier phase, and anything else
    /// the process ran, not this phase alone.
    pub process_peak_rss_bytes: u64,
    /// The most the process's memory reservations held at once from the end
    /// of the run's previous phase to the end of this one, in bytes: what
    /// admission had granted at its peak, this run's and any other's in the
    /// process.
    pub peak_reserved_bytes: u64,
    /// The process's reservations still held when the phase ended, in
    /// bytes.
    pub reserved_bytes: u64,
}

/// Where a run writes its summary, and the request it echoes there.
#[derive(Clone, Debug)]
pub struct SummaryTarget {
    /// The summary file.
    pub path: PathBuf,
    /// The request as its caller states it.
    pub request: serde_json::Value,
}

/// A phase did not start because the run was cancelled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("the run was cancelled")]
pub struct Cancelled;

impl RunSummary {
    /// The summary of a run about to start: `request` echoed, nothing run
    /// yet. Its first phase's reservation peak starts here.
    #[must_use]
    pub fn begin(request: serde_json::Value, capacity_bytes: u64, backend: BackendChoice) -> Self {
        Self {
            request,
            stop: None,
            capacity_bytes,
            workers: 0,
            workers_policy: 0,
            workers_reason: None,
            backend,
            precision: None,
            phases: Vec::new(),
            minor_cycles: 0,
            minor_iterations: 0,
            products: Vec::new(),
            reservations: ReservationWindow::open(),
        }
    }

    /// Write the summary of a run that ended as `status` to `path`, as
    /// pretty-printed JSON with the status's fields first.
    ///
    /// # Errors
    ///
    /// The file could not be written.
    pub fn write(&self, status: &RunStatus, path: &Path) -> std::io::Result<()> {
        let text = serde_json::to_string_pretty(&Written {
            status,
            summary: self,
        })
        .map_err(std::io::Error::other)?;
        std::fs::write(path, text + "\n")
    }
}

/// A summary as written: how the run ended, then what it did.
#[derive(Serialize)]
struct Written<'a> {
    #[serde(flatten)]
    status: &'a RunStatus,
    #[serde(flatten)]
    summary: &'a RunSummary,
}

/// Run one phase named `name`, unless `cancel` is set, and record in
/// `summary` whether it completed, its wall time and the memory after it;
/// with [`Diagnostics::stage_timing`], log that record too.
///
/// # Errors
///
/// [`Cancelled`] (through `E`) when cancellation was requested before the
/// phase started, and nothing is recorded; otherwise the step's error.
pub fn run_phase<T, E: From<Cancelled>>(
    name: impl Into<String>,
    cancel: &Cancel,
    diagnostics: Diagnostics,
    summary: &mut RunSummary,
    step: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    if cancel.is_cancelled() {
        return Err(Cancelled.into());
    }
    let started = Instant::now();
    let result = step();
    let phase = Phase {
        name: name.into(),
        completed: result.is_ok(),
        seconds: started.elapsed().as_secs_f64(),
        process_peak_rss_bytes: peak_rss(),
        peak_reserved_bytes: summary.reservations.close(),
        reserved_bytes: reserved_memory(),
    };
    if diagnostics.stage_timing {
        tracing::info!(
            target: STAGE_TIMING,
            phase = ?phase.name,
            completed = phase.completed,
            seconds = phase.seconds,
            process_peak_rss_bytes = phase.process_peak_rss_bytes,
            peak_reserved_bytes = phase.peak_reserved_bytes,
            reserved_bytes = phase.reserved_bytes,
            "imaging phase"
        );
    }
    summary.phases.push(phase);
    result
}

/// CASA's stop reason: its `stopcode` and the reason's name.
fn clean_stop<S: Serializer>(stop: &Option<CleanStop>, serializer: S) -> Result<S::Ok, S::Error> {
    #[derive(Serialize)]
    struct Stop {
        reason: &'static str,
        stopcode: i32,
    }
    stop.map(|stop| Stop {
        reason: stop.name(),
        stopcode: stop.code(),
    })
    .serialize(serializer)
}

/// The catalog's spelling of a grid precision.
fn grid_precision<S: Serializer>(
    precision: &Option<GridPrecision>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    precision
        .map(|precision| match precision {
            GridPrecision::F32 => "f32",
            GridPrecision::F64 => "f64",
        })
        .serialize(serializer)
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

    const QUIET: Diagnostics = Diagnostics {
        stage_timing: false,
        metal_stage_timing: false,
        science_trace: false,
    };

    #[test]
    fn a_phase_is_recorded_whether_it_completes_or_fails_but_not_once_cancelled() {
        let cancel = Cancel::new();
        let mut summary = RunSummary::begin(serde_json::Value::Null, 1, BackendChoice::Cpu);
        let value = run_phase("first", &cancel, QUIET, &mut summary, || {
            Ok::<_, Cancelled>(7)
        })
        .expect("runs");
        assert_eq!(value, 7);
        let failed = run_phase("second", &cancel, QUIET, &mut summary, || {
            Err::<(), _>(Cancelled)
        });
        assert_eq!(failed, Err(Cancelled));
        let recorded = summary
            .phases
            .iter()
            .map(|phase| (phase.name.as_str(), phase.completed))
            .collect::<Vec<_>>();
        assert_eq!(recorded, [("first", true), ("second", false)]);
        assert!(
            summary
                .phases
                .iter()
                .all(|phase| phase.process_peak_rss_bytes > 0)
        );
        cancel.cancel();
        let mut ran = false;
        let cancelled = run_phase("third", &cancel, QUIET, &mut summary, || {
            ran = true;
            Ok::<_, Cancelled>(())
        });
        assert_eq!(cancelled, Err(Cancelled));
        assert!(!ran);
        assert_eq!(summary.phases.len(), 2);
    }

    #[test]
    fn another_runs_phases_take_nothing_from_this_runs_phase() {
        let host = crate::HostResources {
            threads: 1,
            performance_cores: 1,
            available_memory: 1 << 30,
            metal: false,
        };
        let policy = crate::ResourcePolicy::Explicit {
            workers: 1,
            memory: 1 << 30,
        };
        let demand = crate::Demand {
            phase: "transient",
            memory: 5 << 20,
        };
        let cancel = Cancel::new();
        let mut first = RunSummary::begin(serde_json::Value::Null, 1, BackendChoice::Cpu);
        run_phase("first", &cancel, QUIET, &mut first, || {
            drop(crate::admit(&host, &policy, &demand).expect("fits"));
            let mut second = RunSummary::begin(serde_json::Value::Null, 1, BackendChoice::Cpu);
            run_phase("second", &cancel, QUIET, &mut second, || {
                Ok::<_, Cancelled>(())
            })
        })
        .expect("runs");
        // Other tests of this process may hold reservations too.
        assert!(first.phases[0].peak_reserved_bytes >= 5 << 20);
    }

    #[test]
    fn the_summary_spells_its_status_stop_and_precision_as_the_catalog_and_casa_do() {
        let mut summary = RunSummary::begin(serde_json::Value::Null, 1 << 30, BackendChoice::Cpu);
        summary.stop = Some(CleanStop::Threshold);
        summary.precision = Some(GridPrecision::F64);
        let written = |status: &RunStatus| -> serde_json::Value {
            serde_json::to_value(Written {
                status,
                summary: &summary,
            })
            .expect("serializes")
        };
        let json = written(&RunStatus::Failed {
            error: "a pass failed".to_string(),
        });
        assert_eq!(json["status"], "failed");
        assert_eq!(json["error"], "a pass failed");
        assert_eq!(
            json["stop"],
            serde_json::json!({"reason": "threshold", "stopcode": 2})
        );
        assert_eq!(json["precision"], "f64");
        assert_eq!(json["backend"], "cpu");
        assert_eq!(json["capacity_bytes"], 1_u64 << 30);
        assert!(json.get("reservations").is_none());
        let json = written(&RunStatus::Cancelled);
        assert_eq!(json["status"], "cancelled");
        assert!(json.get("error").is_none());
        assert_eq!(written(&RunStatus::Completed)["status"], "completed");
    }
}
