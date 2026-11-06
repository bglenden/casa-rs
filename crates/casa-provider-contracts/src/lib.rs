// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared canonical provider-contract bundle types.
//!
//! These structs intentionally stay small and transport-oriented so task
//! providers can expose one machine-readable schema bundle while deriving
//! presentation-specific views from those canonical contracts.

mod application;
mod build_catalog;
mod builtin;
mod casa_value;
mod catalog_number_probe;
mod catalog_parse;
mod components;
mod executable;
mod parameters;
mod projection;
mod provider;
mod reply;
mod semantic;
#[cfg(test)]
mod surface_codegen;
mod value_conversion;

pub use casa_value::{
    CasaEntry, CasaFloat, CasaKey, CasaValue, ComplexDtype, FloatDtype, IntDtype,
};
pub use components::{ProviderComponentSchemas, merged_components};
pub use parameters::*;
pub use projection::{
    ProviderCliMachineActions, ProviderCliProjection, ProviderProjectionMetadata, project_ui_form,
};
pub use provider::{
    NoAdditionalProviderSchemas, ProviderContractEnvelope, ProviderContractValidationError,
    ProviderProtocolDescriptor, TaskProviderContract, TaskProviderSchemas,
};
pub use reply::{
    EXIT_EXECUTION_FAILED, EXIT_INTERRUPTED, EXIT_SUCCEEDED, EXIT_VALIDATION_FAILED,
    TaskFailureKind, TaskFrontEndSchemas, TaskReply, TaskRunRequest,
};
pub use semantic::{
    ObjectConstructorDescriptor, ObjectMethodDescriptor, ObjectPropertyDescriptor,
    ObjectSemanticContract, ObjectTypeContract, ProviderSurfaceKind, TaskOperationDescriptor,
    TaskSemanticContract,
};
pub use value_conversion::{
    casa_numpy_scalar_root, casa_variant_integer, casacore_value_to_casa, empty_parameter_array,
    parameter_array_to_casa, parameter_value_to_casacore, scalar_parameter_array,
};

#[cfg(test)]
mod tests;
pub use application::{
    APPLICATION_CATALOG_SCHEMA_VERSION, ApplicationCatalog, ApplicationDefinition,
    ApplicationLaunchDescriptor, builtin_application_catalog,
};
pub use builtin::{builtin_surface_bundle, builtin_surface_catalog};
#[doc(hidden)]
pub use catalog_number_probe::{
    JsonNumberFingerprint, builtin_provider_fingerprint, builtin_provider_parser_matches,
    provider_document_roundtrips,
};
pub use executable::{SUITE_ROOT_ENV, installed_suite_root, resolve_task_executable};
