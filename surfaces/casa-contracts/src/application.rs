// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::{SurfaceContractBundle, builtin_surface_bundle, builtin_surface_catalog};

const APPLICATION_CATALOG_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/application-catalog.json"
));

/// The catalog schema this crate reads. Version 3 describes the CLI and suite inventory.
pub const APPLICATION_CATALOG_SCHEMA_VERSION: u32 = 3;

/// The CLI host for a task and the package that builds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationLaunchDescriptor {
    pub executable: String,
    pub cargo_package: String,
}

/// One task application: the surface it runs and how it is launched and
/// packaged. Every application is a task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationDefinition {
    /// Stable application identity. Task identities reference a canonical surface with this ID.
    pub id: String,
    pub launch: ApplicationLaunchDescriptor,
    /// The kinds of dataset the task reads, such as `measurement_set`.
    pub dataset_kinds: Vec<String>,
    /// Whether `scripts/install-suite.sh` installs the executable.
    pub include_in_suite: bool,
}

impl ApplicationDefinition {
    /// The surface this application runs.
    pub fn surface_bundle(&self) -> Result<SurfaceContractBundle, String> {
        builtin_surface_bundle(&self.id)
    }

    pub fn protocol_family(&self) -> Result<String, String> {
        Ok(self.surface_bundle()?.surface.provider_family().to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationCatalog {
    pub schema_version: u32,
    pub applications: Vec<ApplicationDefinition>,
}

impl ApplicationCatalog {
    pub fn application(&self, id: &str) -> Option<&ApplicationDefinition> {
        self.applications
            .iter()
            .find(|application| application.id == id)
    }

    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        if self.schema_version != APPLICATION_CATALOG_SCHEMA_VERSION {
            errors.push(format!(
                "unsupported application catalog version {}",
                self.schema_version
            ));
        }
        let mut ids = BTreeSet::new();
        let mut task_ids = BTreeSet::new();
        let mut executable_owners = BTreeMap::new();
        for application in &self.applications {
            if !ids.insert(application.id.as_str()) {
                errors.push(format!("duplicate application ID {:?}", application.id));
            }
            if application.launch.executable.is_empty()
                || application.launch.cargo_package.is_empty()
            {
                errors.push(format!(
                    "application {:?} has an incomplete launch descriptor",
                    application.id
                ));
            }
            let package = application.launch.cargo_package.as_str();
            if let Some(previous) =
                executable_owners.insert(application.launch.executable.as_str(), package)
                && previous != package
            {
                errors.push(format!(
                    "executable {:?} has conflicting package mappings",
                    application.launch.executable
                ));
            }
            task_ids.insert(application.id.as_str());
            if let Err(error) = builtin_surface_bundle(&application.id) {
                errors.push(format!(
                    "task application {:?} has no canonical surface: {error}",
                    application.id
                ));
            }
        }
        if let Ok(surfaces) = builtin_surface_catalog() {
            let surface_ids = surfaces
                .surfaces
                .iter()
                .map(|surface| surface.id())
                .collect::<BTreeSet<_>>();
            for missing in surface_ids.difference(&task_ids) {
                errors.push(format!(
                    "canonical surface {missing:?} has no application entry"
                ));
            }
            for extra in task_ids.difference(&surface_ids) {
                errors.push(format!(
                    "task application {extra:?} has no canonical surface"
                ));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

pub fn builtin_application_catalog() -> Result<&'static ApplicationCatalog, String> {
    static CATALOG: OnceLock<Result<ApplicationCatalog, String>> = OnceLock::new();
    CATALOG
        .get_or_init(|| {
            let catalog = serde_json::from_str::<ApplicationCatalog>(APPLICATION_CATALOG_JSON)
                .map_err(|error| format!("parse provider application-catalog.json: {error}"))?;
            catalog.validate().map_err(|errors| errors.join("\n"))?;
            Ok(catalog)
        })
        .as_ref()
        .map_err(Clone::clone)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_catalog_covers_every_surface_with_one_task_each() {
        let catalog = builtin_application_catalog().expect("valid application catalog");
        let surfaces = crate::builtin_surface_catalog().expect("valid parameter catalog");
        let expected: std::collections::BTreeSet<_> = surfaces
            .surfaces
            .iter()
            .map(|surface| surface.id())
            .collect();
        let launched: std::collections::BTreeSet<_> = catalog
            .applications
            .iter()
            .map(|application| application.id.as_str())
            .collect();
        assert_eq!(launched, expected);
        assert_eq!(launched.len(), catalog.applications.len());
        for application in &catalog.applications {
            assert_eq!(
                application.surface_bundle().unwrap().surface.id(),
                application.id
            );
        }
        // The launcher is retired (#931): Python starts each task directly.
        assert!(catalog.application("casars").is_none());
    }

    #[test]
    fn task_presentation_is_owned_by_the_referenced_surface() {
        let application = builtin_application_catalog()
            .unwrap()
            .application("tclean")
            .unwrap();
        let surface = application.surface_bundle().unwrap();
        assert_eq!(surface.surface.display_name(), "Tclean");
        assert_eq!(surface.surface.category(), "Imaging");
        assert_eq!(application.protocol_family().unwrap(), "imager");
    }

    /// Version 2 (#972) holds only what a launch and the suite read: a
    /// version-1 entry's presentation fields are refused, as is the old
    /// version number.
    #[test]
    fn the_retired_presentation_fields_are_refused() {
        let catalog = builtin_application_catalog().unwrap();
        assert_eq!(catalog.schema_version, APPLICATION_CATALOG_SCHEMA_VERSION);
        let mut entry = serde_json::to_value(&catalog.applications[0]).unwrap();
        // As a set: serde_json keeps insertion order when another crate in
        // the build enables its `preserve_order` feature.
        assert_eq!(
            entry
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["dataset_kinds", "id", "include_in_suite", "launch"])
        );
        entry["show_in_tui"] = serde_json::json!(true);
        let error = serde_json::from_value::<ApplicationDefinition>(entry).unwrap_err();
        assert!(error.to_string().contains("show_in_tui"), "{error}");
        let mut old = catalog.clone();
        old.schema_version = 1;
        let errors = old.validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.contains("unsupported application catalog version 1")),
            "{errors:?}"
        );
    }

    #[test]
    fn validation_rejects_an_application_without_a_surface() {
        let mut catalog = builtin_application_catalog().unwrap().clone();
        let mut casars = catalog.applications[0].clone();
        casars.id = "casars".to_string();
        catalog.applications.push(casars);
        let errors = catalog.validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.contains("\"casars\" has no canonical surface")),
            "{errors:?}"
        );
    }

    #[test]
    fn validation_rejects_incomplete_launch_metadata() {
        let mut catalog = builtin_application_catalog().unwrap().clone();
        catalog.applications[0].launch.cargo_package.clear();
        let errors = catalog.validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.contains("incomplete launch descriptor"))
        );
    }

    #[test]
    fn suite_projection_has_stable_complete_executable_ownership() {
        let catalog = builtin_application_catalog().unwrap();
        let suite = catalog
            .applications
            .iter()
            .filter(|application| application.include_in_suite)
            .map(|application| application.launch.executable.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            suite,
            BTreeSet::from([
                "calibrate",
                "concat",
                "casars-imager",
                "importvla",
                "exportfits",
                "feather",
                "flagdata",
                "flagmanager",
                "imexplore",
                "immath",
                "immoments",
                "impbcor",
                "importfits",
                "impv",
                "imregrid",
                "imsubimage",
                "msconvert",
                "msexplore",
                "mstransform",
                "predictcomp",
                "simobserve",
                "tsdimaging",
                "simanalyze",
                "simalma",
            ])
        );
    }
}
