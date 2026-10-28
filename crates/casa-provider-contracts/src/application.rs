// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::{SurfaceContractBundle, builtin_surface_bundle, builtin_surface_catalog};

const APPLICATION_CATALOG_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/application-catalog.json"
));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationKind {
    Task,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationShellKind {
    Inspect,
    Workflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationInteraction {
    OneShot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationLaunchDescriptor {
    pub executable: String,
    pub cargo_package: String,
    pub override_env: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationDefinition {
    /// Stable application identity. Task identities reference a canonical surface with this ID.
    pub id: String,
    pub kind: ApplicationKind,
    pub launch: ApplicationLaunchDescriptor,
    pub shell_kind: ApplicationShellKind,
    pub interaction: ApplicationInteraction,
    pub dataset_kinds: Vec<String>,
    pub show_in_tui: bool,
    pub show_in_swift: bool,
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
        if self.schema_version != 1 {
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
                || application.launch.override_env.is_empty()
            {
                errors.push(format!(
                    "application {:?} has an incomplete launch descriptor",
                    application.id
                ));
            }
            let override_env = &application.launch.override_env;
            if !(override_env.starts_with("CASARS_") && override_env.ends_with("_BIN")) {
                errors.push(format!(
                    "application {:?} override {override_env:?} is not named CASARS_<APP>_BIN",
                    application.id
                ));
            }
            let launch_owner = (
                application.launch.cargo_package.as_str(),
                application.launch.override_env.as_str(),
            );
            if let Some(previous) =
                executable_owners.insert(application.launch.executable.as_str(), launch_owner)
                && previous != launch_owner
            {
                errors.push(format!(
                    "executable {:?} has conflicting package/environment mappings",
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
        assert_eq!(catalog.applications.len(), 26);
        for application in &catalog.applications {
            assert_eq!(application.kind, ApplicationKind::Task);
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
            .application("imager")
            .unwrap();
        let surface = application.surface_bundle().unwrap();
        assert_eq!(surface.surface.display_name(), "Imager");
        assert_eq!(surface.surface.category(), "Imaging");
        assert_eq!(application.protocol_family().unwrap(), "imager");
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
    fn every_override_is_named_for_the_resolver() {
        let mut catalog = builtin_application_catalog().unwrap().clone();
        let impbcor = catalog.application("impbcor").unwrap();
        assert_eq!(impbcor.launch.override_env, "CASARS_IMPBCOR_BIN");
        catalog.applications[0].launch.override_env = "CASA_RS_MSEXPLORE_BIN".to_string();
        let errors = catalog.validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.contains("is not named CASARS_<APP>_BIN")),
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
                "msexplore",
                "mstransform",
                "simobserve",
            ])
        );
    }
}
