// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared typed parameters, sparse TOML profiles, and managed Last state.
//!
//! This crate owns parameter *lifecycle* rather than provider semantics.  The
//! latter live in [`casa_provider_contracts`], allowing CLI, TUI, GUI/UniFFI,
//! Python, and providers to resolve the same self-contained surface bundle.

mod cli;
mod diagnostic;
mod invocation;
mod normalize;
mod profile;
mod runtime;
mod safety;
mod session;
mod storage;

pub use cli::{
    TaskCliAction, TaskCliError, TaskCliHost, parse_task_cli_action, read_task_request,
    task_cli_machine_help,
};
pub use diagnostic::{Diagnostic, DiagnosticCode, DiagnosticLevel, SourceLocation};
pub use invocation::{
    ProviderInvocationError, parameter_value_is_omitted, project_parameter_value,
    project_provider_invocation, provider_parameter_applies,
};
pub use normalize::{NormalizationError, normalize_value, semantic_eq, validate_value};
pub use profile::{
    PROFILE_FORMAT_VERSION, ParameterProfile, ProfileError, ProfileHeader, ResolvedProfile,
    parse_profile, render_documented_template, render_sparse_profile, resolve_profile,
};
pub use runtime::{
    OpenSessionRequest, ParameterRuntime, ParameterRuntimeError, ParameterTextError,
    parse_parameter_text,
};
pub use safety::{RunSafetyEvaluationError, RunSafetyRequirements, required_run_safety};
pub use session::{
    BaseSource, ParameterOrigin, ParameterSession, ParameterSessionError, ParameterState,
    ResolutionPatch,
};
pub use storage::{
    ManagedProfileKind, ManagedStateError, ManagedStateStore, SessionLastCoordinator,
    StateWriteOutcome, TaskLastCoordinator, write_parameter_profile_atomic,
};
