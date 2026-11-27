// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared typed parameters, sparse TOML profiles, and managed Last state.
//!
//! This crate owns parameter *lifecycle* rather than provider semantics.  The
//! latter live in [`casa_contracts`], allowing each task's command
//! line, Python, and providers to resolve the same self-contained surface
//! bundle.

#![allow(missing_docs)]

mod casa_type_diagnostics;
mod cli;
mod completion;
mod diagnostic;
mod front_end;
mod invocation;
mod normalize;
mod overrides;
mod profile;
mod resolve;
#[allow(clippy::module_inception)]
mod runtime;
mod session;
mod storage;
mod unimplemented;

/// The value a task's execution callback returns in its [`TaskSuccess`].
pub use casa_contracts::CasaValue;
pub use cli::{TaskCliError, parse_parameter_cli_overrides, read_task_request};
pub use completion::{
    RunProductReference, TaskCompletion, TaskCompletionError, TaskOutputValue,
    decode_task_completion,
};
pub use diagnostic::{Diagnostic, DiagnosticCode, DiagnosticLevel, SourceLocation};
#[cfg(feature = "imaging")]
pub use front_end::TaskCancellation;
pub use front_end::{
    EnterWorkspace, FrontEndOutput, NativeTaskOptions, TaskCommandLine, TaskExit, TaskFailure,
    TaskFrontEnd, TaskInvocation, TaskLogLineSink, TaskOutcome, TaskSuccess, classify_task_exit,
    describe, display_value, format_profile_error, format_session_error, render_session,
    run_task_in_process, task_command_line_help, validate_profile_text,
};
pub use invocation::{
    ProviderInvocationError, project_provider_invocation,
    project_provider_invocation_with_originals,
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
