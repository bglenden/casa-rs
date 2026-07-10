// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared canonical provider-contract bundle types.
//!
//! These structs intentionally stay small and transport-oriented so task and
//! session providers can expose one machine-readable schema bundle while
//! retaining compatibility projections such as the legacy `--ui-schema` view.

mod builtin;
mod components;
mod parameters;
mod projection;
mod semantic;

pub use components::{ProviderComponentSchemas, merged_components};
pub use parameters::*;
pub use projection::{
    ProviderCliMachineActions, ProviderCliProjection, ProviderProjectionMetadata,
    derived_ui_schema_annotations, project_ui_schema,
};
pub use semantic::{
    ObjectConstructorDescriptor, ObjectMethodDescriptor, ObjectPropertyDescriptor,
    ObjectSemanticContract, ObjectTypeContract, ProviderSurfaceKind, SessionSemanticContract,
    TaskOperationDescriptor, TaskSemanticContract,
};

#[cfg(test)]
mod tests;
pub use builtin::{builtin_surface_bundle, builtin_surface_catalog};
