// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared typed parameters, sparse TOML profiles, and managed Last state.
//!
//! This crate owns parameter *lifecycle* rather than provider semantics.  The
//! latter live in [`casa_provider_contracts`], allowing the `casars` CLI,
//! Python, and providers to resolve the same self-contained surface bundle.

mod cli;
mod completion;
mod diagnostic;
mod front_end;
mod invocation;
mod normalize;
mod profile;
mod resolve;
mod runtime;
mod session;
mod storage;

/// The logging configuration a task binary's `main` reads and hands
/// [`TaskFrontEnd::main`].
pub use casa_logging::{CasaLoggingConfig, CasaLoggingError};
pub use cli::{
    TaskCliAction, TaskCliError, TaskCliHost, parse_parameter_cli_overrides, parse_task_cli_action,
    read_task_request, task_cli_machine_help,
};
pub use completion::{
    RunProductReference, TaskCompletion, TaskCompletionError, TaskOutputValue,
    decode_task_completion,
};
pub use diagnostic::{Diagnostic, DiagnosticCode, DiagnosticLevel, SourceLocation};
pub use front_end::{
    FrontEndOutput, TaskCommandLine, TaskExit, TaskFailure, TaskFrontEnd, TaskInvocation,
    TaskOutcome, TaskSuccess, classify_task_exit, describe, display_value, format_profile_error,
    format_session_error, render_session, task_command_line_help, validate_profile_text,
};
pub use invocation::{
    FRONT_END_FAMILIES, ProviderInvocationError, project_front_end_invocation,
    project_provider_invocation, runs_on_front_end,
};
pub use normalize::{NormalizationError, normalize_value, semantic_eq, validate_value};
pub use profile::{
    PROFILE_FORMAT_VERSION, ParameterProfile, ProfileError, ProfileHeader, ResolvedProfile,
    parse_profile, render_documented_template, render_sparse_profile, resolve_profile,
};
pub use resolve::{ResolveError, resolve_active_values, resolve_plain_values};
pub use runtime::{
    OpenSessionRequest, ParameterRuntime, ParameterRuntimeError, ParameterTextError,
    parse_parameter_text,
};
pub use session::{
    BaseSource, ParameterOrigin, ParameterSession, ParameterSessionError, ParameterState,
    ResolutionPatch,
};
pub use storage::{
    ManagedProfileKind, ManagedStateError, ManagedStateStore, StateWriteOutcome,
    TaskLastCoordinator, write_parameter_profile_atomic,
};
