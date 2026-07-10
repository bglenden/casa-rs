// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared typed parameters, sparse TOML profiles, and managed Last state.
//!
//! This crate owns parameter *lifecycle* rather than provider semantics.  The
//! latter live in [`casa_provider_contracts`], allowing CLI, TUI, GUI/UniFFI,
//! Python, and providers to resolve the same self-contained surface bundle.

mod diagnostic;
mod invocation;
mod normalize;
mod profile;
mod safety;
mod session;
mod storage;
mod ui_projection;

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
pub use safety::{RunSafetyEvaluationError, RunSafetyRequirements, required_run_safety};
pub use session::{
    BaseSource, ParameterOrigin, ParameterSession, ParameterSessionError, ParameterState,
    ResolutionPatch,
};
pub use storage::{
    AutomaticSaveReport, ManagedProfileKind, ManagedStateError, ManagedStateStore,
    SessionLastState, StateWriteOutcome, TaskLastState, write_parameter_profile_atomic,
};
pub use ui_projection::project_ui_schema;
