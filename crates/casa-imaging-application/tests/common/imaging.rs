// SPDX-License-Identifier: LGPL-3.0-or-later
//! Imaging requests resolved through the provider catalog, as the imager
//! task resolves them, and the context a test runs them in.

use casa_imaging_application::{Cancel, HostResources, ImagingRequest, ResourcePolicy, RunContext};

/// The request for catalog `values`, plain JSON by imager parameter name,
/// defaulted and checked by the catalog. It grids on the CPU unless the
/// values name a `backend`: the catalog's `auto` would move a test onto the
/// GPU wherever the host has one.
pub fn request(values: serde_json::Value) -> ImagingRequest {
    let serde_json::Value::Object(mut values) = values else {
        panic!("request values are a JSON object");
    };
    values.entry("backend").or_insert_with(|| "cpu".into());
    let resolved = casa_task_runtime::resolve_plain_values(
        casa_provider_contracts::builtin_surface_bundle("imager").expect("imager surface"),
        std::path::PathBuf::from("."),
        &values,
    )
    .unwrap_or_else(|error| panic!("catalog values: {error}"));
    serde_json::from_value(serde_json::Value::Object(resolved))
        .unwrap_or_else(|error| panic!("imaging request: {error}"))
}

/// The detected host under `policy`, and no summary.
pub fn context(policy: ResourcePolicy) -> RunContext {
    RunContext {
        host: HostResources::detect().expect("host"),
        policy,
        cancel: Cancel::new(),
        summary: None,
    }
}
