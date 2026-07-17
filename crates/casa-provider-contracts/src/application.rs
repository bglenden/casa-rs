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
    Launcher,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationShellKind {
    Inspect,
    Browser,
    Workflow,
    Launcher,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationInteraction {
    OneShot,
    BrowserSession,
    Launcher,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationBrowserKind {
    Table,
    Image,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationLaunchMode {
    InstalledSuite,
    DevelopmentWorkspace,
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
    pub browser_kind: Option<ApplicationBrowserKind>,
    pub dataset_kinds: Vec<String>,
    pub show_in_tui: bool,
    pub show_in_swift: bool,
    pub include_in_suite: bool,
}

impl ApplicationDefinition {
    pub fn surface_bundle(&self) -> Result<Option<SurfaceContractBundle>, String> {
        match self.kind {
            ApplicationKind::Task => builtin_surface_bundle(&self.id).map(Some),
            ApplicationKind::Launcher => Ok(None),
        }
    }

    pub fn protocol_family(&self) -> Result<Option<String>, String> {
        Ok(self
            .surface_bundle()?
            .map(|bundle| bundle.surface.provider_family().to_string()))
    }

    pub const fn supported_launch_modes(&self) -> [ApplicationLaunchMode; 2] {
        [
            ApplicationLaunchMode::InstalledSuite,
            ApplicationLaunchMode::DevelopmentWorkspace,
        ]
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
        let mut launcher_count = 0;
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
            match application.kind {
                ApplicationKind::Task => {
                    task_ids.insert(application.id.as_str());
                    if let Err(error) = builtin_surface_bundle(&application.id) {
                        errors.push(format!(
                            "task application {:?} has no canonical surface: {error}",
                            application.id
                        ));
                    }
                    if application.interaction == ApplicationInteraction::Launcher {
                        errors.push(format!(
                            "task application {:?} uses launcher interaction",
                            application.id
                        ));
                    }
                    match application.interaction {
                        ApplicationInteraction::BrowserSession => {
                            if application.shell_kind != ApplicationShellKind::Browser
                                || application.browser_kind.is_none()
                            {
                                errors.push(format!(
                                    "browser application {:?} has incomplete browser metadata",
                                    application.id
                                ));
                            }
                        }
                        ApplicationInteraction::OneShot => {
                            if application.browser_kind.is_some() {
                                errors.push(format!(
                                    "one-shot application {:?} declares a browser kind",
                                    application.id
                                ));
                            }
                        }
                        ApplicationInteraction::Launcher => {}
                    }
                }
                ApplicationKind::Launcher => {
                    launcher_count += 1;
                    if application.interaction != ApplicationInteraction::Launcher
                        || application.shell_kind != ApplicationShellKind::Launcher
                    {
                        errors.push(format!(
                            "launcher application {:?} has task presentation metadata",
                            application.id
                        ));
                    }
                }
            }
        }
        if launcher_count != 1 {
            errors.push(format!(
                "application catalog must contain exactly one launcher, found {launcher_count}"
            ));
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
    fn builtin_catalog_covers_every_surface_and_one_launcher() {
        let catalog = builtin_application_catalog().expect("valid application catalog");
        assert_eq!(catalog.applications.len(), 43);
        assert_eq!(
            catalog
                .applications
                .iter()
                .filter(|application| application.kind == ApplicationKind::Task)
                .count(),
            42
        );
        let launcher = catalog.application("casars").expect("launcher entry");
        assert_eq!(launcher.kind, ApplicationKind::Launcher);
        assert!(launcher.surface_bundle().unwrap().is_none());
    }

    #[test]
    fn task_presentation_is_owned_by_the_referenced_surface() {
        let application = builtin_application_catalog()
            .unwrap()
            .application("imager")
            .unwrap();
        let surface = application.surface_bundle().unwrap().unwrap();
        assert_eq!(surface.surface.display_name(), "Imager");
        assert_eq!(surface.surface.category(), "Imaging");
        assert_eq!(
            application.protocol_family().unwrap().as_deref(),
            Some("imager")
        );
        assert_eq!(
            application.supported_launch_modes(),
            [
                ApplicationLaunchMode::InstalledSuite,
                ApplicationLaunchMode::DevelopmentWorkspace,
            ]
        );
    }

    #[test]
    fn validation_rejects_incomplete_browser_metadata() {
        let mut catalog = builtin_application_catalog().unwrap().clone();
        let image = catalog
            .applications
            .iter_mut()
            .find(|application| application.id == "imexplore")
            .unwrap();
        image.browser_kind = None;
        let errors = catalog.validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.contains("incomplete browser metadata"))
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
                "casars",
                "casars-casa-task",
                "casars-imager",
                "casars-importvla",
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
            ])
        );
    }
}
