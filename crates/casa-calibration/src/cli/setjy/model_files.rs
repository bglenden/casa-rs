// SPDX-License-Identifier: LGPL-3.0-or-later
//! Discover CASA calibrator image and Solar System model files.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use casa_measures_data::MeasuresRuntime;
use casa_provider_contracts::CasaValue;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use serde::Serialize;

const JPL_2010_OBJECTS: &[&str] = &[
    "Venus", "Mars", "Jupiter", "Uranus", "Neptune", "Pluto", "Io", "Europa", "Ganymede",
    "Callisto", "Titan", "Triton", "Ceres", "Pallas", "Vesta", "Juno", "Victoria", "Davida",
];

#[derive(Serialize)]
struct ModelListing {
    models: Vec<String>,
}

pub(super) fn list(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let models = match task.text("standard")?.as_deref() {
        Some("Butler-JPL-Horizons 2010") => {
            JPL_2010_OBJECTS.iter().map(|&name| name.into()).collect()
        }
        Some("Butler-JPL-Horizons 2012") => solar_system_models(task.workspace()),
        _ => {
            let mut candidates = list_directory(task.workspace(), |name| {
                name.contains(".im") || name.contains(".mod")
            });
            for directory in model_directories(task.workspace(), "CalModels", "nrao/VLA/CalModels")
            {
                candidates.extend(list_directory(&directory, |_| true));
            }
            candidates
        }
    };
    let listing = ModelListing { models };
    let result = TaskSuccess::new(
        CasaValue::Dict {
            entries: Vec::new(),
        },
        &listing,
    )?;
    Ok(result.with_text(listing.models.join("\n")))
}

fn solar_system_models(workspace: &Path) -> Vec<String> {
    let directories = model_directories(workspace, "SolarSystemModels", "alma/SolarSystemModels");
    if directories.is_empty() {
        tracing::warn!("SolarSystemModels was not found in the CASA data directory");
    }
    let mut models = Vec::new();
    for directory in &directories {
        models.extend(list_directory(directory, |name| {
            name.contains("Tb") && name.ends_with(".dat")
        }));
    }
    for directory in &directories {
        models.extend(list_directory(directory, |name| {
            name.ends_with("fd_time.dat")
        }));
    }
    models
}

fn model_directories(workspace: &Path, name: &str, relative: &str) -> Vec<PathBuf> {
    if let Some(directory) = MeasuresRuntime::discover_candidates()
        .unwrap_or_default()
        .into_iter()
        .map(|candidate| candidate.path.join(relative))
        .find(|directory| directory.is_dir())
    {
        return vec![directory];
    }
    let mut pending = vec![workspace.to_path_buf()];
    let mut visited = BTreeSet::new();
    let mut found = BTreeSet::new();
    while let Some(directory) = pending.pop() {
        let Ok(path) = directory.canonicalize() else {
            continue;
        };
        if directory.file_name().is_some_and(|base| base == name) {
            found.insert(path.clone());
        }
        if !visited.insert(path) {
            continue;
        }
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };
        pending.extend(entries.flatten().filter_map(|entry| {
            let basename = entry.file_name();
            let name = basename.to_str()?;
            let path = entry.path();
            (may_contain_models(name) && path.is_dir()).then_some(path)
        }));
    }
    found.into_iter().collect()
}

fn may_contain_models(name: &str) -> bool {
    !name.starts_with('.')
        && !matches!(name, "regression" | "ephemerides" | "geodetic" | "gui")
        && !(cfg!(target_os = "macos") && matches!(name, "Frameworks" | "Library" | "lib"))
        && ![".ms", ".im", ".tab"]
            .iter()
            .any(|extension| name.contains(extension))
}

fn list_directory(directory: &Path, accepts: impl Fn(&str) -> bool) -> Vec<String> {
    let mut names = fs::read_dir(directory)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| !name.starts_with('.') && accepts(name))
        .collect::<Vec<_>>();
    names.sort_unstable();
    names
}
