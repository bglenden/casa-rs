// SPDX-License-Identifier: LGPL-3.0-or-later
//! The dictionary view of the parameter catalog that `casars.parameters`
//! returns from `catalog()`, `definition()` and `contract_bundle()`.
//!
//! Records become dictionaries with their fields in declaration order. A
//! tagged variant leads with a `kind` key naming the variant in snake case,
//! so a path type is `{"kind": "path", "resource_kind": "image"}` and a unit
//! variant such as a boolean type is `{"kind": "bool"}`. Contract
//! enumerations that the catalog stores as words, such as a semantic role or
//! a resource kind, become snake-case strings.

use casa_provider_contracts::{
    NarrowingConstraint, ParameterCatalog, ParameterType, ParameterValue, Predicate,
    SurfaceContractBundle, SurfaceDefinition,
};
use serde::Serialize;

/// The snake-case spelling of an enumeration's `Debug` name, as the
/// dictionary view has always spelled contract words.
pub(crate) fn snake_case_debug(value: impl std::fmt::Debug) -> String {
    let source = format!("{value:?}");
    let mut output = String::with_capacity(source.len());
    for (index, character) in source.chars().enumerate() {
        if character.is_uppercase() {
            if index > 0 {
                output.push('_');
            }
            output.extend(character.to_lowercase());
        } else {
            output.push(character);
        }
    }
    output
}

/// A parameter value inside a predicate, tagged by its type.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum TaggedValue {
    Bool { value: bool },
    Integer { value: i64 },
    Float { value: f64 },
    String { value: String },
    Array { values: Vec<TaggedValue> },
    Table { entries: Vec<TaggedEntry> },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct TaggedEntry {
    name: String,
    value: TaggedValue,
}

impl From<&ParameterValue> for TaggedValue {
    fn from(value: &ParameterValue) -> Self {
        match value {
            ParameterValue::Bool(value) => Self::Bool { value: *value },
            ParameterValue::Integer(value) => Self::Integer { value: *value },
            ParameterValue::Float(value) => Self::Float { value: *value },
            ParameterValue::String(value) => Self::String {
                value: value.clone(),
            },
            ParameterValue::Array(values) => Self::Array {
                values: values.iter().map(Self::from).collect(),
            },
            ParameterValue::Table(entries) => Self::Table {
                entries: entries
                    .iter()
                    .map(|(name, value)| TaggedEntry {
                        name: name.clone(),
                        value: Self::from(value),
                    })
                    .collect(),
            },
        }
    }
}

/// A parameter's value domain.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum TypeView {
    Bool,
    Integer,
    Float,
    String,
    Path {
        resource_kind: Option<String>,
    },
    Choice {
        values: Vec<String>,
    },
    Quantity {
        dimension: String,
        canonical_unit: String,
        special_values: Vec<String>,
    },
    Array {
        elements: Vec<TypeView>,
        min_items: u64,
        max_items: Option<u64>,
        allow_scalar: bool,
    },
    Table {
        fields: Vec<TypeFieldView>,
    },
    Optional {
        values: Vec<TypeView>,
        states: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct TypeFieldView {
    name: String,
    value: TypeView,
}

impl From<&ParameterType> for TypeView {
    fn from(value: &ParameterType) -> Self {
        match value {
            ParameterType::Bool => Self::Bool,
            ParameterType::Integer => Self::Integer,
            ParameterType::Float => Self::Float,
            ParameterType::String => Self::String,
            ParameterType::Path { resource_kind } => Self::Path {
                resource_kind: resource_kind.map(snake_case_debug),
            },
            ParameterType::Choice { values } => Self::Choice {
                values: values.clone(),
            },
            ParameterType::Quantity {
                dimension,
                canonical_unit,
                special_values,
            } => Self::Quantity {
                dimension: snake_case_debug(dimension),
                canonical_unit: canonical_unit.clone(),
                special_values: special_values.clone(),
            },
            ParameterType::Array {
                element,
                min_items,
                max_items,
                allow_scalar,
            } => Self::Array {
                elements: vec![Self::from(element.as_ref())],
                min_items: *min_items as u64,
                max_items: max_items.map(|value| value as u64),
                allow_scalar: *allow_scalar,
            },
            ParameterType::Table { fields } => Self::Table {
                fields: fields
                    .iter()
                    .map(|(name, value)| TypeFieldView {
                        name: name.clone(),
                        value: Self::from(value),
                    })
                    .collect(),
            },
            ParameterType::Optional { value, states } => Self::Optional {
                values: vec![Self::from(value.as_ref())],
                states: states.clone(),
            },
        }
    }
}

/// When a provider field is emitted.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum PredicateView {
    Always,
    Never,
    IsSet {
        parameter: String,
    },
    Equals {
        parameter: String,
        value: TaggedValue,
    },
    Not {
        predicates: Vec<PredicateView>,
    },
    All {
        predicates: Vec<PredicateView>,
    },
    Any {
        predicates: Vec<PredicateView>,
    },
}

impl From<&Predicate> for PredicateView {
    fn from(value: &Predicate) -> Self {
        match value {
            Predicate::Always => Self::Always,
            Predicate::Never => Self::Never,
            Predicate::IsSet { parameter } => Self::IsSet {
                parameter: parameter.clone(),
            },
            Predicate::Equals { parameter, value } => Self::Equals {
                parameter: parameter.clone(),
                value: TaggedValue::from(value),
            },
            Predicate::Not { predicate } => Self::Not {
                predicates: vec![Self::from(predicate.as_ref())],
            },
            Predicate::All { predicates } => Self::All {
                predicates: predicates.iter().map(Self::from).collect(),
            },
            Predicate::Any { predicates } => Self::Any {
                predicates: predicates.iter().map(Self::from).collect(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct DocumentationView {
    summary: String,
    details: Option<String>,
    examples: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct ConceptView {
    id: String,
    semantic_revision: u64,
    casa_name: String,
    value_domain: TypeView,
    unit_dimension: Option<String>,
    semantic_role: String,
    documentation: DocumentationView,
    persistence_class: String,
}

/// The concepts a catalog or bundle defines.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct CatalogView {
    schema_version: u64,
    concepts: Vec<ConceptView>,
}

impl From<&ParameterCatalog> for CatalogView {
    fn from(value: &ParameterCatalog) -> Self {
        Self {
            schema_version: value.schema_version as u64,
            concepts: value
                .concepts
                .iter()
                .map(|concept| ConceptView {
                    id: concept.id.0.clone(),
                    semantic_revision: concept.semantic_revision.0 as u64,
                    casa_name: concept.casa_name.clone(),
                    value_domain: TypeView::from(&concept.value_domain),
                    unit_dimension: concept.unit_dimension.as_ref().map(snake_case_debug),
                    semantic_role: snake_case_debug(concept.semantic_role),
                    documentation: DocumentationView {
                        summary: concept.documentation.summary.clone(),
                        details: concept.documentation.details.clone(),
                        examples: concept.documentation.examples.clone(),
                    },
                    persistence_class: snake_case_debug(concept.persistence_class),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct ConceptReferenceView {
    id: String,
    semantic_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct PresentationView {
    label: String,
    group: String,
    advanced: bool,
    hidden: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct CliProjectionView {
    positional: Option<u64>,
    flags: Vec<String>,
    false_flags: Vec<String>,
    metavar: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct ProviderProjectionView {
    field: String,
    adapter: String,
    emit_when: Option<PredicateView>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct PythonProjectionView {
    name: String,
    type_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct ProjectionsView {
    cli: Option<CliProjectionView>,
    provider: Option<ProviderProjectionView>,
    python: Option<PythonProjectionView>,
    presentation: PresentationView,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct ConstraintView {
    kind: String,
    values: Vec<String>,
}

impl From<&NarrowingConstraint> for ConstraintView {
    fn from(value: &NarrowingConstraint) -> Self {
        let kind = match value {
            NarrowingConstraint::NumberRange { .. } => "number_range",
            NarrowingConstraint::Length { .. } => "length",
            NarrowingConstraint::AllowedValues { .. } => "allowed_values",
            NarrowingConstraint::SelectorCapabilities { .. } => "selector_capabilities",
        };
        let values = match value {
            NarrowingConstraint::AllowedValues { values }
            | NarrowingConstraint::SelectorCapabilities {
                capabilities: values,
            } => values.clone(),
            _ => Vec::new(),
        };
        Self {
            kind: kind.to_string(),
            values,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct BindingView {
    name: String,
    concept: ConceptReferenceView,
    order: u64,
    refinements: Vec<ConstraintView>,
    context_role: Option<String>,
    surface_note: Option<String>,
    projections: ProjectionsView,
}

impl From<&casa_provider_contracts::SurfaceParameterBinding> for BindingView {
    fn from(value: &casa_provider_contracts::SurfaceParameterBinding) -> Self {
        let projections = &value.projections;
        Self {
            name: value.name.clone(),
            concept: ConceptReferenceView {
                id: value.concept.id.0.clone(),
                semantic_revision: value.concept.semantic_revision.0 as u64,
            },
            order: value.order as u64,
            refinements: value.refinements.iter().map(ConstraintView::from).collect(),
            context_role: value.context_role.map(snake_case_debug),
            surface_note: value.surface_note.clone(),
            projections: ProjectionsView {
                cli: projections.cli.as_ref().map(|cli| CliProjectionView {
                    positional: cli.positional.map(|value| value as u64),
                    flags: cli.flags.clone(),
                    false_flags: cli.false_flags.clone(),
                    metavar: cli.metavar.clone(),
                }),
                provider: projections
                    .provider
                    .as_ref()
                    .map(|provider| ProviderProjectionView {
                        field: provider.field.clone(),
                        adapter: snake_case_debug(provider.adapter.clone()),
                        emit_when: provider.emit_when.as_ref().map(PredicateView::from),
                    }),
                python: projections
                    .python
                    .as_ref()
                    .map(|python| PythonProjectionView {
                        name: python.name.clone(),
                        type_hint: python.type_hint.clone(),
                    }),
                presentation: PresentationView {
                    label: projections.presentation.label.clone(),
                    group: projections.presentation.group.clone(),
                    advanced: projections.presentation.advanced,
                    hidden: projections.presentation.hidden,
                },
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct ExecutionView {
    invocation_name: String,
    fixed_args: Vec<String>,
}

/// One surface definition: `definition()`, and an element of `catalog()`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct DefinitionView {
    kind: String,
    id: String,
    contract_version: u64,
    display_name: String,
    category: String,
    summary: String,
    execution: ExecutionView,
    bindings: Vec<BindingView>,
}

impl From<&SurfaceDefinition> for DefinitionView {
    fn from(value: &SurfaceDefinition) -> Self {
        Self {
            kind: value.kind().to_string(),
            id: value.id().to_string(),
            contract_version: value.contract_version() as u64,
            display_name: value.display_name().to_string(),
            category: value.category().to_string(),
            summary: value.summary().to_string(),
            execution: ExecutionView {
                invocation_name: value.execution().invocation_name.clone(),
                fixed_args: value.execution().fixed_args.clone(),
            },
            bindings: value.bindings().iter().map(BindingView::from).collect(),
        }
    }
}

/// `contract_bundle()`: one definition with exactly the concepts it uses.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct BundleView {
    schema_version: u64,
    pub(crate) surface: DefinitionView,
    catalog: CatalogView,
}

impl From<&SurfaceContractBundle> for BundleView {
    fn from(value: &SurfaceContractBundle) -> Self {
        Self {
            schema_version: value.schema_version as u64,
            surface: DefinitionView::from(&value.surface),
            catalog: CatalogView::from(&value.catalog),
        }
    }
}

/// `catalog()`: every concept and every surface definition.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct CatalogEnvelopeView {
    schema_version: u64,
    catalog: CatalogView,
    surfaces: Vec<DefinitionView>,
}

impl CatalogEnvelopeView {
    pub(crate) fn new(catalog: &casa_provider_contracts::SurfaceCatalogBundle) -> Self {
        Self {
            schema_version: catalog.schema_version as u64,
            catalog: CatalogView::from(&catalog.catalog),
            surfaces: catalog.surfaces.iter().map(DefinitionView::from).collect(),
        }
    }
}
