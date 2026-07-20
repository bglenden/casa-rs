// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared canonical provider-contract bundle types.
//!
//! These structs intentionally stay small and transport-oriented so task and
//! session providers can expose one machine-readable schema bundle while
//! while deriving presentation-specific views from those canonical contracts.

mod application;
mod builtin;
mod components;
mod parameters;
mod projection;
mod provider;
mod semantic;
mod session_protocol;

pub use components::{ProviderComponentSchemas, merged_components};
pub use parameters::*;
pub use projection::{
    ProviderCliMachineActions, ProviderCliProjection, ProviderProjectionMetadata, project_ui_form,
};
pub use provider::{
    NoAdditionalProviderSchemas, ProviderContractEnvelope, ProviderContractValidationError,
    ProviderProtocolDescriptor, SessionProviderContract, SessionProviderSchemas,
    TaskProviderContract, TaskProviderSchemas,
};
pub use semantic::{
    ObjectConstructorDescriptor, ObjectMethodDescriptor, ObjectPropertyDescriptor,
    ObjectSemanticContract, ObjectTypeContract, ProviderSurfaceKind, SessionSemanticContract,
    TaskOperationDescriptor, TaskSemanticContract,
};
pub use session_protocol::{
    JSONL_STDIO_TRANSPORT, VersionedSessionEnvelope, encode_session_message,
    jsonl_session_contract, session_contract_json, session_schema_json,
};

#[cfg(test)]
mod tests;
pub use application::{
    ApplicationBrowserKind, ApplicationCatalog, ApplicationDefinition, ApplicationInteraction,
    ApplicationKind, ApplicationLaunchDescriptor, ApplicationLaunchMode, ApplicationShellKind,
    builtin_application_catalog,
};
pub use builtin::{builtin_surface_bundle, builtin_surface_catalog};
