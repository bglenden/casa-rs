// SPDX-License-Identifier: LGPL-3.0-or-later
//! The host, memory, workers and device a session runs on.
//!
//! A session has no budget or worker model of its own (plan §4,
//! Prediction): `close` admits its memory against the imaging runtime's
//! [`ResourcePolicy`] on the detected [`HostResources`], and runs its
//! geometry and noise on the runtime's worker team.
//!
//! The controls are the Python tool's constructor arguments (ADR-0019 item
//! 3), [`ExecutionControls`]:
//! - with neither a memory budget nor a worker count, the session takes the
//!   balanced policy: three quarters of the host's free memory and of its
//!   threads, at most its performance cores;
//! - with either, an explicit policy whose other half is the balanced
//!   policy's on the host.
//!
//! The budget caps everything `close` holds at once: the MeasurementSet
//! writer gets half of what the policy leaves free, and the batch being
//! filled, its geometry and the worker scratch come out of the rest
//! (`write::resources`). The worker count is the runtime's: threads kept
//! busy, the MeasurementSet writer's among them.
//!
//! The backend is where a session predicts its model
//! ([`SessionResources::prediction_backend`]); noise never runs on a device,
//! so a session that predicts nothing writes the same MeasurementSet
//! whatever the request. Every kernel set a session predicts with is the
//! standard `GridFT` one, which the imaging runtime's Metal laws and the
//! simulator's own (`tests/backends.rs`) cover, so `Auto` takes the host's
//! Metal device whenever there is one.

use std::num::NonZeroUsize;
use std::str::FromStr;

use casa_imaging_runtime::pass::BackendChoice;
use casa_imaging_runtime::{CoreAllocation, HostResources, ResourcePolicy};

use super::error::{SimulatorError, SimulatorResult};

/// The device a session asks to predict on.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BackendRequest {
    /// The host's Metal device where it predicts the session's model, and
    /// the CPU otherwise; the CPU while a session predicts nothing.
    #[default]
    Auto,
    /// The CPU.
    Cpu,
    /// The host's Metal device; a host without one is refused when the
    /// session's resources are built.
    Metal,
}

impl BackendRequest {
    /// The request's name, as the Python tool takes it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Cpu => "cpu",
            Self::Metal => "metal",
        }
    }
}

/// A backend name other than `auto`, `cpu` or `metal`.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("backend must be 'auto', 'cpu' or 'metal', not '{0}'")]
pub struct UnknownBackend(pub String);

impl FromStr for BackendRequest {
    type Err = UnknownBackend;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        [Self::Auto, Self::Cpu, Self::Metal]
            .into_iter()
            .find(|request| request.name() == name)
            .ok_or_else(|| UnknownBackend(name.to_string()))
    }
}

/// Controls CASA's `sm` lacks: the Python tool's keyword-only constructor
/// arguments (ADR-0019 item 3).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExecutionControls {
    /// Bytes the session may hold at once while `close` writes, capped at
    /// the host's free memory; `None` for the balanced policy's three
    /// quarters of it.
    pub memory_budget: Option<u64>,
    /// Threads `close` keeps busy, the MeasurementSet writer's among them,
    /// capped at the host's; `None` for the balanced policy's.
    pub workers: Option<NonZeroUsize>,
    /// The device to predict on.
    pub backend: BackendRequest,
}

/// The host, resource policy and backend of a session: what `close` admits
/// its memory against and sizes its worker team from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionResources {
    host: HostResources,
    policy: ResourcePolicy,
    backend: BackendRequest,
}

impl SessionResources {
    /// The resources `controls` ask for on this host, detected once per
    /// process.
    ///
    /// # Errors
    ///
    /// The host's threads or free memory could not be read, or `Metal` was
    /// asked for on a host without a Metal device.
    pub fn from_controls(controls: &ExecutionControls) -> SimulatorResult<Self> {
        let host = HostResources::detect()
            .map_err(|error| SimulatorError::raised(format!("host resources: {error}")))?;
        Self::on_host(host, controls)
    }

    /// The resources `controls` ask for on `host`.
    ///
    /// # Errors
    ///
    /// `Metal` was asked for on a host without a Metal device.
    pub fn on_host(host: HostResources, controls: &ExecutionControls) -> SimulatorResult<Self> {
        let balanced = ResourcePolicy::Balanced;
        let policy = match (controls.memory_budget, controls.workers) {
            (None, None) => balanced,
            (memory, workers) => ResourcePolicy::Explicit {
                workers: workers.map_or_else(|| balanced.workers(&host), NonZeroUsize::get),
                memory: memory.unwrap_or_else(|| balanced.memory(&host)),
            },
        };
        Self::pinned(host, policy, controls.backend)
    }

    /// The resources of `policy` on `host` with `backend`, as given: tests
    /// and benchmarks pin the host rather than detect it.
    ///
    /// # Errors
    ///
    /// `Metal` was asked for on a host without a Metal device.
    pub fn pinned(
        host: HostResources,
        policy: ResourcePolicy,
        backend: BackendRequest,
    ) -> SimulatorResult<Self> {
        if backend == BackendRequest::Metal && !host.metal {
            return Err(SimulatorError::raised(
                "backend 'metal' needs a Metal device, and this host has none",
            ));
        }
        Ok(Self {
            host,
            policy,
            backend,
        })
    }

    /// The host.
    #[must_use]
    pub const fn host(&self) -> &HostResources {
        &self.host
    }

    /// The resource policy.
    #[must_use]
    pub const fn policy(&self) -> &ResourcePolicy {
        &self.policy
    }

    /// The backend asked for.
    #[must_use]
    pub const fn backend(&self) -> BackendRequest {
        self.backend
    }

    /// The policy's workers on the host, before `close` admits its memory:
    /// `close` streams MAIN to the writer's thread, so it runs the
    /// streaming workers beside it.
    #[must_use]
    pub fn cores(&self) -> CoreAllocation {
        self.policy.cores(&self.host)
    }

    /// The backend a session runs on: `Auto` takes Metal only where the
    /// host has a device and the session predicts a model.
    #[must_use]
    pub fn resolved_backend(&self, predicts: bool) -> BackendChoice {
        match self.backend {
            BackendRequest::Cpu => BackendChoice::Cpu,
            BackendRequest::Metal => BackendChoice::Metal,
            BackendRequest::Auto if predicts && self.host.metal => BackendChoice::Metal,
            BackendRequest::Auto => BackendChoice::Cpu,
        }
    }

    /// The backend `close` predicts a session's model on, once it predicts
    /// one: [`Self::resolved_backend`] of a session that predicts. `Auto`
    /// is Metal on a host with a device and the CPU otherwise; `Metal` and
    /// `Cpu` are what they ask for. On Metal, `close` also admits the
    /// device copies of the model grids and the device predictor's bytes.
    #[must_use]
    pub fn prediction_backend(&self) -> BackendChoice {
        self.resolved_backend(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOST: HostResources = HostResources {
        threads: 10,
        performance_cores: 4,
        available_memory: 8 << 30,
        metal: false,
    };

    #[test]
    fn backend_names_round_trip_and_others_are_refused() {
        for request in [
            BackendRequest::Auto,
            BackendRequest::Cpu,
            BackendRequest::Metal,
        ] {
            assert_eq!(request.name().parse::<BackendRequest>(), Ok(request));
        }
        let error = "gpu".parse::<BackendRequest>().expect_err("not a backend");
        assert_eq!(
            error.to_string(),
            "backend must be 'auto', 'cpu' or 'metal', not 'gpu'"
        );
        assert!("CPU".parse::<BackendRequest>().is_err());
    }

    #[test]
    fn metal_is_refused_without_a_device_and_auto_stays_on_the_cpu() {
        let error = SessionResources::pinned(HOST, ResourcePolicy::Balanced, BackendRequest::Metal)
            .expect_err("no device");
        assert!(error.message().contains("Metal device"), "{error}");
        let with_device = HostResources {
            metal: true,
            ..HOST
        };
        let metal =
            SessionResources::pinned(with_device, ResourcePolicy::Balanced, BackendRequest::Metal)
                .expect("a device");
        assert_eq!(metal.resolved_backend(false), BackendChoice::Metal);
        let auto =
            SessionResources::pinned(with_device, ResourcePolicy::Balanced, BackendRequest::Auto)
                .expect("auto");
        assert_eq!(auto.resolved_backend(false), BackendChoice::Cpu);
        assert_eq!(auto.resolved_backend(true), BackendChoice::Metal);
        assert_eq!(auto.prediction_backend(), BackendChoice::Metal);
        assert_eq!(metal.prediction_backend(), BackendChoice::Metal);
        let cpu = SessionResources::pinned(HOST, ResourcePolicy::Balanced, BackendRequest::Auto)
            .expect("auto");
        assert_eq!(cpu.resolved_backend(true), BackendChoice::Cpu);
        assert_eq!(cpu.prediction_backend(), BackendChoice::Cpu);
        let asked =
            SessionResources::pinned(with_device, ResourcePolicy::Balanced, BackendRequest::Cpu)
                .expect("cpu");
        assert_eq!(asked.prediction_backend(), BackendChoice::Cpu);
    }

    #[test]
    fn the_balanced_policy_leaves_the_writer_a_performance_core() {
        let balanced =
            SessionResources::pinned(HOST, ResourcePolicy::Balanced, BackendRequest::Cpu)
                .expect("resources");
        assert_eq!(
            balanced.cores(),
            CoreAllocation {
                workers: 4,
                streaming_workers: 3,
            }
        );
        let one = SessionResources::pinned(
            HOST,
            ResourcePolicy::Explicit {
                workers: 1,
                memory: 1 << 30,
            },
            BackendRequest::Cpu,
        )
        .expect("resources");
        assert_eq!(one.cores().streaming_workers, 1);
    }

    #[test]
    fn either_control_makes_the_policy_explicit_with_the_other_balanced() {
        let on_host = |controls: ExecutionControls| {
            *SessionResources::on_host(HOST, &controls)
                .expect("resources")
                .policy()
        };
        assert_eq!(
            on_host(ExecutionControls::default()),
            ResourcePolicy::Balanced
        );
        let balanced = ResourcePolicy::Balanced;
        assert_eq!(
            on_host(ExecutionControls {
                memory_budget: Some(1 << 28),
                ..ExecutionControls::default()
            }),
            ResourcePolicy::Explicit {
                workers: balanced.workers(&HOST),
                memory: 1 << 28,
            }
        );
        assert_eq!(
            on_host(ExecutionControls {
                workers: NonZeroUsize::new(2),
                ..ExecutionControls::default()
            }),
            ResourcePolicy::Explicit {
                workers: 2,
                memory: balanced.memory(&HOST),
            }
        );
    }
}
