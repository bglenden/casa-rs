// SPDX-License-Identifier: LGPL-3.0-or-later
//! Imaging requests resolved through the provider catalog, as the `tclean`
//! task resolves them, and the context a test runs them in.

use casa_imaging_application::{Cancel, HostResources, ImagingRequest, ResourcePolicy, RunContext};

/// The request for catalog `values`, plain JSON by tclean parameter name,
/// defaulted and checked by the catalog and read as tclean's modes read
/// them. It grids on the CPU unless the values name a `backend`: the
/// catalog's `auto` would move a test onto the GPU wherever the host has
/// one. A value these tests were written against when it was the imager's
/// default fills in for CASA's (#931): a 512-pixel image of 1-arcsecond
/// cells, 1000-iteration minor cycles, W planes derived from the data, and
/// the imager's fixed `conjbeams` and `mosweight`.
/// These component fixtures also keep the old PB opt-in: some intentionally
/// omit telescope metadata. The live default is checked through the task's
/// production entry point in `casars-imager`'s PB-default T1 test.
pub fn request(values: serde_json::Value) -> ImagingRequest {
    let serde_json::Value::Object(mut values) = values else {
        panic!("request values are a JSON object");
    };
    values.entry("backend").or_insert_with(|| "cpu".into());
    values.entry("imsize").or_insert_with(|| 512.into());
    values.entry("cell").or_insert_with(|| "1arcsec".into());
    values.entry("cycleniter").or_insert_with(|| 1000.into());
    values.entry("write_pb").or_insert_with(|| false.into());
    match values.get("gridder").and_then(serde_json::Value::as_str) {
        Some("wproject") => {
            values.entry("wprojplanes").or_insert_with(|| (-1).into());
        }
        Some("awproject") => {
            values.entry("conjbeams").or_insert_with(|| true.into());
        }
        Some("mosaic") => {
            values.entry("mosweight").or_insert_with(|| false.into());
        }
        _ => {}
    }
    let mut resolved = casa_task_runtime::resolve_plain_values(
        casa_provider_contracts::builtin_surface_bundle("tclean").expect("tclean surface"),
        std::path::PathBuf::from("."),
        &values,
    )
    .unwrap_or_else(|error| panic!("catalog values: {error}"));
    // Match the imager boundary: numeric row selectors retain original text.
    for name in ["scan", "observation"] {
        resolved.insert(
            name.to_string(),
            values.get(name).cloned().unwrap_or_else(|| "".into()),
        );
    }
    ImagingRequest::from_tclean(&resolved)
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
