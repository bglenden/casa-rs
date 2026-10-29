// SPDX-License-Identifier: LGPL-3.0-or-later
//! A start model's seeding outside a run, for the counting law in
//! `tests/start_model_memory.rs` (#841 Phase 6b): what the initial pass
//! admits for it, and the samples it then seeds the model with.

use std::path::Path;

use casa_images::RegridTemplate;

use crate::compile::StartModel;

/// The bytes the initial pass admits to seed a model of `template` from the
/// start model at `path`.
///
/// # Errors
///
/// A start model the run would refuse.
pub fn seeding_bytes(path: &Path, template: RegridTemplate) -> Result<u64, String> {
    StartModel::onto(path, template)
        .map(|start| start.seeding_bytes())
        .map_err(|error| error.to_string())
}

/// The main domain's samples the initial pass seeds the model of `template`
/// with from the start model at `path`, in the model's canonical order.
///
/// # Errors
///
/// A start model the run would refuse or cannot read.
pub fn main_samples(path: &Path, template: RegridTemplate) -> Result<Vec<f32>, String> {
    StartModel::onto(path, template)
        .map_err(|error| error.to_string())?
        .main_samples()
        .map_err(|error| error.to_string())
}
