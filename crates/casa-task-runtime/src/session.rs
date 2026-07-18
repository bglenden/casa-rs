// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use casa_provider_contracts::{ParameterValue, SurfaceContractBundle};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::diagnostic::Diagnostic;
use crate::profile::{
    ParameterProfile, ProfileError, evaluate_predicate, render_sparse_profile, resolve_defaults,
    resolve_draft_with_overrides, resolve_profile,
};

/// Mutually exclusive base source for one editor/invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "path", rename_all = "snake_case")]
pub enum BaseSource {
    Defaults,
    Last,
    LastSuccessful,
    File(PathBuf),
}

/// Winning source of one currently resolved value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParameterOrigin {
    Default,
    BaseProfile,
    Context,
    Override,
}

/// Typed field state consumed by TUI, GUI, and Python.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParameterState {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<ParameterValue>,
    pub origin: ParameterOrigin,
    pub active: bool,
    pub required: bool,
    pub explicit: bool,
}

/// Accepted context or explicit override patch.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ResolutionPatch {
    #[serde(default)]
    pub values: BTreeMap<String, ParameterValue>,
    #[serde(default)]
    pub unset: BTreeSet<String>,
}

/// Mutable, typed draft for exactly one surface and one base source.
#[derive(Debug, Clone)]
pub struct ParameterSession {
    bundle: SurfaceContractBundle,
    base_source: BaseSource,
    base_overrides: BTreeMap<String, ParameterValue>,
    context_patch: ResolutionPatch,
    override_patch: ResolutionPatch,
    states: BTreeMap<String, ParameterState>,
    diagnostics: Vec<Diagnostic>,
    dirty: bool,
}

impl ParameterSession {
    /// Start from current defaults.
    pub fn defaults(bundle: SurfaceContractBundle) -> Result<Self, ParameterSessionError> {
        let mut session = Self {
            bundle,
            base_source: BaseSource::Defaults,
            base_overrides: BTreeMap::new(),
            context_patch: ResolutionPatch::default(),
            override_patch: ResolutionPatch::default(),
            states: BTreeMap::new(),
            diagnostics: Vec::new(),
            dirty: false,
        };
        session.recompute()?;
        Ok(session)
    }

    /// Start from exactly one parsed base profile.
    pub fn from_profile(
        bundle: SurfaceContractBundle,
        source: BaseSource,
        profile: &ParameterProfile,
    ) -> Result<Self, ParameterSessionError> {
        if matches!(source, BaseSource::Defaults) {
            return Err(ParameterSessionError::InvalidSource(
                "a parsed profile cannot be labeled Defaults".to_string(),
            ));
        }
        if matches!(source, BaseSource::LastSuccessful)
            && bundle.surface.kind() == casa_provider_contracts::SurfaceKind::Session
        {
            return Err(ParameterSessionError::InvalidSource(
                "session surfaces do not have Last Successful".to_string(),
            ));
        }
        let resolved = resolve_profile(profile, &bundle)?;
        let mut session = Self {
            bundle,
            base_source: source,
            base_overrides: resolved.explicit_overrides,
            context_patch: ResolutionPatch::default(),
            override_patch: ResolutionPatch::default(),
            states: BTreeMap::new(),
            diagnostics: resolved.diagnostics,
            dirty: false,
        };
        session.recompute()?;
        Ok(session)
    }

    pub fn bundle(&self) -> &SurfaceContractBundle {
        &self.bundle
    }

    pub fn base_source(&self) -> &BaseSource {
        &self.base_source
    }

    pub fn states(&self) -> &BTreeMap<String, ParameterState> {
        &self.states
    }

    pub fn diagnostics(&self) -> &[Diagnostic] {
        &self.diagnostics
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub fn values(&self) -> BTreeMap<String, ParameterValue> {
        self.states
            .iter()
            .filter_map(|(name, state)| state.value.clone().map(|value| (name.clone(), value)))
            .collect()
    }

    /// Return the active catalog-owned risks for this resolved draft.
    pub fn required_run_safety(
        &self,
    ) -> Result<crate::RunSafetyRequirements, crate::RunSafetyEvaluationError> {
        crate::required_run_safety(&self.bundle, &self.states)
    }

    /// Replace the draft with another mutually exclusive base source.
    pub fn replace_base(
        &mut self,
        source: BaseSource,
        profile: Option<&ParameterProfile>,
    ) -> Result<(), ParameterSessionError> {
        let mut candidate = self.clone();
        match (&source, profile) {
            (BaseSource::Defaults, None) => {
                candidate.base_source = source;
                candidate.base_overrides.clear();
                candidate.diagnostics.clear();
            }
            (BaseSource::Defaults, Some(_)) => {
                return Err(ParameterSessionError::InvalidSource(
                    "Defaults cannot carry a profile".to_string(),
                ));
            }
            (_, Some(profile)) => {
                if matches!(source, BaseSource::LastSuccessful)
                    && self.bundle.surface.kind() == casa_provider_contracts::SurfaceKind::Session
                {
                    return Err(ParameterSessionError::InvalidSource(
                        "session surfaces do not have Last Successful".to_string(),
                    ));
                }
                let resolved = resolve_profile(profile, &candidate.bundle)?;
                candidate.base_source = source;
                candidate.base_overrides = resolved.explicit_overrides;
                candidate.diagnostics = resolved.diagnostics;
            }
            (_, None) => {
                return Err(ParameterSessionError::InvalidSource(
                    "Last and file sources require a profile".to_string(),
                ));
            }
        }
        candidate.context_patch = ResolutionPatch::default();
        candidate.override_patch = ResolutionPatch::default();
        candidate.recompute()?;
        candidate.dirty = false;
        *self = candidate;
        Ok(())
    }

    /// Apply an accepted launch/project-context patch.
    pub fn apply_context_patch(
        &mut self,
        patch: ResolutionPatch,
    ) -> Result<(), ParameterSessionError> {
        let mut candidate = self.clone();
        candidate.context_patch = patch;
        candidate.recompute()?;
        candidate.dirty = true;
        *self = candidate;
        Ok(())
    }

    /// Apply explicit CLI/Python/UI overrides at the highest precedence.
    pub fn apply_override_patch(
        &mut self,
        patch: ResolutionPatch,
    ) -> Result<(), ParameterSessionError> {
        let mut candidate = self.clone();
        candidate.override_patch = patch;
        candidate.recompute()?;
        candidate.dirty = true;
        *self = candidate;
        Ok(())
    }

    pub fn set(
        &mut self,
        name: impl Into<String>,
        value: ParameterValue,
    ) -> Result<(), ParameterSessionError> {
        let name = name.into();
        self.ensure_parameter(&name)?;
        let mut candidate = self.clone();
        candidate.override_patch.unset.remove(&name);
        candidate.override_patch.values.insert(name, value);
        candidate.recompute()?;
        candidate.dirty = true;
        *self = candidate;
        Ok(())
    }

    /// Remove every lower-precedence override for a field and expose its
    /// current contract default.
    pub fn reset(&mut self, name: &str) -> Result<(), ParameterSessionError> {
        self.ensure_parameter(name)?;
        let mut candidate = self.clone();
        candidate.override_patch.values.remove(name);
        candidate.override_patch.unset.insert(name.to_string());
        candidate.recompute()?;
        candidate.dirty = true;
        *self = candidate;
        Ok(())
    }

    /// Discard explicit edits while retaining the selected base and accepted
    /// context patch.
    pub fn revert(&mut self) -> Result<(), ParameterSessionError> {
        let mut candidate = self.clone();
        candidate.override_patch = ResolutionPatch::default();
        candidate.recompute()?;
        candidate.dirty = false;
        *self = candidate;
        Ok(())
    }

    /// Render the current resolved draft as a current-contract sparse profile.
    pub fn render_sparse(&self) -> Result<String, ParameterSessionError> {
        Ok(render_sparse_profile(&self.bundle, &self.values())?)
    }

    fn ensure_parameter(&self, name: &str) -> Result<(), ParameterSessionError> {
        if self
            .bundle
            .surface
            .bindings()
            .iter()
            .any(|binding| binding.name == name)
        {
            Ok(())
        } else {
            Err(ParameterSessionError::UnknownParameter(name.to_string()))
        }
    }

    fn recompute(&mut self) -> Result<(), ParameterSessionError> {
        let defaults = resolve_defaults(&self.bundle)?;
        let mut combined = self.base_overrides.clone();
        apply_patch(&mut combined, &self.context_patch);
        apply_patch(&mut combined, &self.override_patch);
        let (values, missing_required) = resolve_draft_with_overrides(&self.bundle, &combined)?;
        self.diagnostics.retain(|diagnostic| {
            !matches!(
                diagnostic.code,
                crate::DiagnosticCode::MissingRequired | crate::DiagnosticCode::InactiveParameter
            )
        });
        self.diagnostics.extend(missing_required);

        self.states.clear();
        for binding in self.bundle.surface.bindings() {
            let explicitly_reset = self.override_patch.unset.contains(&binding.name);
            let origin = if self.override_patch.values.contains_key(&binding.name) {
                ParameterOrigin::Override
            } else if !explicitly_reset && self.context_patch.values.contains_key(&binding.name) {
                ParameterOrigin::Context
            } else if !explicitly_reset
                && self.base_overrides.contains_key(&binding.name)
                && !self.context_patch.unset.contains(&binding.name)
            {
                ParameterOrigin::BaseProfile
            } else {
                ParameterOrigin::Default
            };
            let active =
                evaluate_predicate(&binding.active_when, &values, &self.bundle).unwrap_or(false);
            let required = active
                && evaluate_predicate(&binding.required_when, &values, &self.bundle)
                    .unwrap_or(false);
            let explicit = origin != ParameterOrigin::Default;
            self.states.insert(
                binding.name.clone(),
                ParameterState {
                    value: values
                        .get(&binding.name)
                        .cloned()
                        .or_else(|| defaults.get(&binding.name).cloned()),
                    origin,
                    active,
                    required,
                    explicit,
                },
            );
        }
        Ok(())
    }
}

fn apply_patch(values: &mut BTreeMap<String, ParameterValue>, patch: &ResolutionPatch) {
    for name in &patch.unset {
        values.remove(name);
    }
    for (name, value) in &patch.values {
        values.insert(name.clone(), value.clone());
    }
}

/// Mutable session construction/edit failure.
#[derive(Debug, Error)]
pub enum ParameterSessionError {
    #[error(transparent)]
    Profile(#[from] ProfileError),
    #[error("unknown parameter {0:?}")]
    UnknownParameter(String),
    #[error("invalid parameter base source: {0}")]
    InvalidSource(String),
    #[error("invalid parameter lifecycle transition: {0}")]
    InvalidLifecycle(String),
}

#[cfg(test)]
mod tests {
    use casa_provider_contracts::{
        DefaultSpec, ParameterCatalog, ParameterConcept, ParameterConceptId, ParameterDocs,
        ParameterPresentation, ParameterProjections, ParameterRole, ParameterType,
        PersistenceClass, Predicate, SemanticRevision, SurfaceDefinition,
        SurfaceExecutionProjection, SurfaceParameterBinding, TaskDefinition,
        builtin_surface_bundle,
    };

    use super::*;
    use crate::parse_profile;

    fn bundle() -> SurfaceContractBundle {
        let concept = ParameterConcept {
            id: ParameterConceptId::new("test.niter"),
            semantic_revision: SemanticRevision(1),
            casa_name: "niter".into(),
            value_domain: ParameterType::Integer,
            normalization: casa_provider_contracts::NormalizationRule::Identity,
            base_constraints: Vec::new(),
            unit_dimension: None,
            semantic_role: ParameterRole::Algorithm,
            documentation: ParameterDocs {
                summary: "Iterations".into(),
                details: None,
                examples: Vec::new(),
            },
            persistence_class: PersistenceClass::Profile,
        };
        SurfaceContractBundle {
            schema_version: 1,
            surface: SurfaceDefinition::Task(TaskDefinition {
                id: "clean".into(),
                contract_version: 1,
                display_name: "Clean".into(),
                category: "Test".into(),
                summary: "Test".into(),
                provider_family: "test".into(),
                execution: SurfaceExecutionProjection::default(),
                safety_rules: Vec::new(),
                bindings: vec![SurfaceParameterBinding {
                    name: "niter".into(),
                    concept: concept.reference(),
                    order: 0,
                    default: DefaultSpec::Literal {
                        value: ParameterValue::Integer(0),
                    },
                    required_when: Predicate::Never,
                    active_when: Predicate::Always,
                    refinements: Vec::new(),
                    context_role: None,
                    surface_note: None,
                    projections: ParameterProjections {
                        cli: None,
                        provider: None,
                        python: None,
                        presentation: ParameterPresentation {
                            label: "Iterations".into(),
                            group: "Clean".into(),
                            advanced: false,
                            hidden: false,
                        },
                    },
                    aliases: Vec::new(),
                    reviewed_homonym: None,
                }],
                migrations: Vec::new(),
            }),
            catalog: ParameterCatalog {
                schema_version: 1,
                concepts: vec![concept],
            },
        }
    }

    #[test]
    fn precedence_and_reset_are_visible_in_origins() {
        let mut session = ParameterSession::defaults(bundle()).unwrap();
        assert_eq!(session.states()["niter"].origin, ParameterOrigin::Default);
        session
            .apply_context_patch(ResolutionPatch {
                values: BTreeMap::from([("niter".into(), ParameterValue::Integer(5))]),
                unset: BTreeSet::new(),
            })
            .unwrap();
        assert_eq!(session.states()["niter"].origin, ParameterOrigin::Context);
        session.set("niter", ParameterValue::Integer(10)).unwrap();
        assert_eq!(session.states()["niter"].origin, ParameterOrigin::Override);
        session.reset("niter").unwrap();
        assert_eq!(session.states()["niter"].origin, ParameterOrigin::Default);
        assert_eq!(
            session.states()["niter"].value,
            Some(ParameterValue::Integer(0))
        );
    }

    #[test]
    fn loading_a_new_base_replaces_dirty_draft() {
        let mut session = ParameterSession::defaults(bundle()).unwrap();
        session.set("niter", ParameterValue::Integer(10)).unwrap();
        let profile = parse_profile(
            r#"[casars]
format = 1
surface = "clean"
kind = "task"
contract = 1
[parameters]
niter = 2
"#,
        )
        .unwrap();
        session
            .replace_base(BaseSource::Last, Some(&profile))
            .unwrap();
        assert!(!session.is_dirty());
        assert_eq!(
            session.states()["niter"].origin,
            ParameterOrigin::BaseProfile
        );
        assert_eq!(
            session.states()["niter"].value,
            Some(ParameterValue::Integer(2))
        );
    }

    #[test]
    fn failed_mutations_leave_the_prior_draft_unchanged() {
        let mut session = ParameterSession::defaults(bundle()).unwrap();
        let before_states = session.states().clone();
        assert!(
            session
                .set("niter", ParameterValue::String("not-an-integer".into()))
                .is_err()
        );
        assert_eq!(session.states(), &before_states);
        assert!(!session.is_dirty());

        let invalid_patch = ResolutionPatch {
            values: BTreeMap::from([(
                "niter".into(),
                ParameterValue::String("still-not-an-integer".into()),
            )]),
            unset: BTreeSet::new(),
        };
        assert!(session.apply_override_patch(invalid_patch).is_err());
        assert_eq!(session.states(), &before_states);
        assert!(!session.is_dirty());
    }

    #[test]
    fn incomplete_interactive_draft_cannot_be_saved_or_executed() {
        let mut required_bundle = bundle();
        let SurfaceDefinition::Task(definition) = &mut required_bundle.surface else {
            unreachable!()
        };
        let binding = definition.bindings.first_mut().unwrap();
        binding.default = DefaultSpec::Required;
        binding.required_when = Predicate::Always;
        let session = ParameterSession::defaults(required_bundle).unwrap();
        assert!(
            session
                .diagnostics()
                .iter()
                .any(|diagnostic| diagnostic.code == crate::DiagnosticCode::MissingRequired)
        );
        assert!(session.render_sparse().is_err());
    }

    #[test]
    fn aggregate_calibrate_enforces_every_selected_modes_required_inputs() {
        let cases: [(&str, &[&str]); 9] = [
            ("apply", &["vis"]),
            ("summary", &["summary_paths"]),
            ("stats", &["table_path"]),
            ("export_corrected_data", &["vis", "outputvis"]),
            ("continuum_subtract", &["vis", "outputvis", "fit_spw"]),
            ("solve_gain", &["vis", "out_table", "refant"]),
            ("solve_bandpass", &["vis", "out_table", "refant"]),
            (
                "fluxscale",
                &["fluxscale_input", "out_table", "reference_fields"],
            ),
            ("gencal", &["vis", "out_table", "caltype"]),
        ];

        for (mode, required) in cases {
            let mut session =
                ParameterSession::defaults(builtin_surface_bundle("calibrate").unwrap()).unwrap();
            session
                .set("mode", ParameterValue::String(mode.to_string()))
                .unwrap();
            let missing = session
                .diagnostics()
                .iter()
                .filter(|diagnostic| diagnostic.code == crate::DiagnosticCode::MissingRequired)
                .filter_map(|diagnostic| diagnostic.parameter.as_deref())
                .collect::<BTreeSet<_>>();
            assert_eq!(missing, required.iter().copied().collect(), "{mode}");
            assert!(session.render_sparse().is_err(), "{mode}");

            for name in required {
                let value = match *name {
                    "vis" => ParameterValue::String("input.ms".into()),
                    "outputvis" => ParameterValue::String("output.ms".into()),
                    "summary_paths" => ParameterValue::String("summary.cal".into()),
                    "table_path" | "fluxscale_input" => ParameterValue::String("input.cal".into()),
                    "fit_spw" => ParameterValue::String("0:0~7".into()),
                    "out_table" => ParameterValue::String("output.cal".into()),
                    "refant" => ParameterValue::String("ea01".into()),
                    "reference_fields" => ParameterValue::String("0".into()),
                    "caltype" => ParameterValue::String("antpos".into()),
                    other => panic!("missing test value for {other}"),
                };
                session.set(*name, value).unwrap();
            }
            session
                .render_sparse()
                .unwrap_or_else(|error| panic!("complete {mode} should render: {error}"));
        }
    }

    #[test]
    fn aggregate_calibrate_keeps_other_stage_values_active_and_persistable() {
        let mut session =
            ParameterSession::defaults(builtin_surface_bundle("calibrate").unwrap()).unwrap();
        session
            .set("vis", ParameterValue::String("input.ms".into()))
            .unwrap();
        session
            .set(
                "summary_paths",
                ParameterValue::String("future-stage.cal".into()),
            )
            .unwrap();
        assert!(session.states()["summary_paths"].active);
        let profile = session.render_sparse().unwrap();
        assert!(profile.contains("summary_paths = \"future-stage.cal\""));
    }

    #[test]
    fn field_state_predicates_use_catalog_semantic_equality() {
        let mut bundle = builtin_surface_bundle("msexplore").unwrap();
        let SurfaceDefinition::Task(definition) = &mut bundle.surface else {
            unreachable!("msexplore is a task")
        };
        definition
            .bindings
            .iter_mut()
            .find(|binding| binding.name == "format")
            .unwrap()
            .active_when = Predicate::Equals {
            parameter: "vis".to_string(),
            value: ParameterValue::String("./target.ms".to_string()),
        };

        let mut session = ParameterSession::defaults(bundle).unwrap();
        session
            .set(
                "vis",
                ParameterValue::String("data/../target.ms".to_string()),
            )
            .unwrap();
        assert!(session.states()["format"].active);
    }
}
