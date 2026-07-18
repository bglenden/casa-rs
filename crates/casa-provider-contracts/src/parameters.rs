// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical, format-neutral parameter contracts shared by every casars UI.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use casa_types::quanta::{Quantity, Unit};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Current schema version for the catalog and surface-definition model.
pub const PARAMETER_CATALOG_SCHEMA_VERSION: u32 = 1;

/// Designated semantic IDs for names whose meaning is repository-wide.
pub const IMSIZE_CONCEPT_ID: &str = "image.geometry.imsize";
pub const CELL_CONCEPT_ID: &str = "image.geometry.cell";
pub const FIELD_CONCEPT_ID: &str = "ms.selection.field";
pub const SPW_CONCEPT_ID: &str = "ms.selection.spw";
pub const OVERWRITE_CONCEPT_ID: &str = "output.overwrite";

/// Stable semantic identity of a parameter concept.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct ParameterConceptId(pub String);

impl ParameterConceptId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ParameterConceptId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for ParameterConceptId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Monotonic revision of one concept's invariant semantics.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct SemanticRevision(pub u32);

/// Revision-pinned reference carried by a surface binding.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
pub struct ParameterConceptRef {
    pub id: ParameterConceptId,
    pub semantic_revision: SemanticRevision,
}

impl ParameterConceptRef {
    pub fn new(id: impl Into<ParameterConceptId>, semantic_revision: u32) -> Self {
        Self {
            id: id.into(),
            semantic_revision: SemanticRevision(semantic_revision),
        }
    }
}

/// JSON/TOML-compatible typed value used before provider adaptation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ParameterValue {
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Array(Vec<ParameterValue>),
    Table(BTreeMap<String, ParameterValue>),
}

impl From<bool> for ParameterValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i64> for ParameterValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<f64> for ParameterValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<&str> for ParameterValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<String> for ParameterValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

/// Filesystem/data role used for validation and context suggestions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Any,
    File,
    Directory,
    MeasurementSet,
    Image,
    Table,
    CalibrationTable,
    Archive,
    Product,
}

/// Physical dimension of a quantity parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum UnitDimension {
    Dimensionless,
    Angle,
    Time,
    Frequency,
    Velocity,
    Length,
    FluxDensity,
    FluxDensityPerBeam,
    Temperature,
    DataSize,
    Custom,
}

/// Accepted semantic value domain.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ParameterType {
    Bool,
    Integer,
    Float,
    String,
    Path {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resource_kind: Option<ResourceKind>,
    },
    Choice {
        values: Vec<String>,
    },
    Quantity {
        dimension: UnitDimension,
        canonical_unit: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        special_values: Vec<String>,
    },
    Array {
        element: Box<ParameterType>,
        min_items: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_items: Option<usize>,
        #[serde(default)]
        allow_scalar: bool,
    },
    Table {
        fields: BTreeMap<String, ParameterType>,
    },
    /// Explicit non-value states such as `auto` or `none` plus a typed value.
    Optional {
        value: Box<ParameterType>,
        states: Vec<String>,
    },
}

/// CASA selector grammar whose parser/normalization is catalog-owned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SelectorGrammar {
    Field,
    SpectralWindow,
    TimeRange,
    UvRange,
    Antenna,
    Scan,
    Correlation,
    Array,
    Observation,
    Intent,
    Feed,
    MsSelect,
    ImageBox,
    ImageChannels,
    ImageRegion,
    Stokes,
}

/// Invariant canonicalization and semantic-equality rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NormalizationRule {
    Identity,
    Trim,
    Lowercase,
    Path,
    CasaSelector {
        grammar: SelectorGrammar,
        #[serde(default)]
        capabilities: Vec<String>,
    },
    Quantity {
        canonical_unit: String,
    },
    Sequence {
        rules: Vec<NormalizationRule>,
    },
}

/// Capability names understood by CASA selector contracts and refinements.
pub const SELECTOR_CAPABILITY_NAMES: [&str; 4] = ["ids", "names", "ranges", "wildcards"];

/// Validate that a selector value uses only the capabilities allowed by one
/// surface binding.
///
/// This is intentionally lexical: provider-specific semantic validation still
/// happens after profile resolution, while ranges, wildcards, numeric IDs, and
/// named atoms can be rejected consistently by every frontend beforehand.
pub fn validate_selector_capabilities(
    grammar: SelectorGrammar,
    value: &ParameterValue,
    allowed: &[String],
) -> Result<(), String> {
    if let Some(unknown) = allowed
        .iter()
        .find(|capability| !SELECTOR_CAPABILITY_NAMES.contains(&capability.as_str()))
    {
        return Err(format!("unknown selector capability {unknown:?}"));
    }

    let mut used = BTreeSet::new();
    collect_selector_capabilities(grammar, value, &mut used);
    if let Some(disallowed) = used
        .into_iter()
        .find(|capability| !allowed.iter().any(|allowed| allowed == capability))
    {
        return Err(format!("selector uses unsupported {disallowed} capability"));
    }
    Ok(())
}

fn collect_selector_capabilities(
    grammar: SelectorGrammar,
    value: &ParameterValue,
    used: &mut BTreeSet<&'static str>,
) {
    match value {
        ParameterValue::String(selector) => {
            let selector = selector.trim();
            if selector.is_empty() || selector.eq_ignore_ascii_case("none") {
                return;
            }
            if selector.contains(['~', '<', '>']) {
                used.insert("ranges");
            }
            if selector.contains(['*', '?']) {
                used.insert("wildcards");
            }

            for atom in selector.split(|character: char| {
                matches!(
                    character,
                    ',' | ';'
                        | '~'
                        | ':'
                        | '&'
                        | '!'
                        | '='
                        | '('
                        | ')'
                        | '['
                        | ']'
                        | '{'
                        | '}'
                        | '<'
                        | '>'
                )
            }) {
                let atom = atom.trim();
                if atom.is_empty() || atom.chars().all(|character| matches!(character, '*' | '?')) {
                    continue;
                }
                let quoted = atom.starts_with(['\'', '"']);
                let atom = atom.trim_matches(['\'', '"']);
                let has_digit = atom.chars().any(|character| character.is_ascii_digit());
                let has_alpha = atom
                    .chars()
                    .any(|character| character.is_ascii_alphabetic());
                let numeric_unit_expression = matches!(
                    grammar,
                    SelectorGrammar::TimeRange
                        | SelectorGrammar::UvRange
                        | SelectorGrammar::ImageBox
                        | SelectorGrammar::ImageChannels
                ) && atom.chars().next().is_some_and(|character| {
                    character.is_ascii_digit() || matches!(character, '+' | '-' | '.')
                });

                if quoted || (has_alpha && !numeric_unit_expression) {
                    used.insert("names");
                } else if has_digit {
                    used.insert("ids");
                }
            }
        }
        ParameterValue::Array(values) => {
            for value in values {
                collect_selector_capabilities(grammar, value, used);
            }
        }
        ParameterValue::Table(values) => {
            for value in values.values() {
                collect_selector_capabilities(grammar, value, used);
            }
        }
        ParameterValue::Bool(_) | ParameterValue::Integer(_) | ParameterValue::Float(_) => {}
    }
}

/// Catalog-owned base constraint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Constraint {
    Finite,
    NonEmpty,
    Positive,
    NumberRange {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max: Option<f64>,
        #[serde(default = "default_true")]
        min_inclusive: bool,
        #[serde(default = "default_true")]
        max_inclusive: bool,
    },
    Length {
        min: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max: Option<usize>,
    },
    AllowedValues {
        values: Vec<String>,
    },
}

const fn default_true() -> bool {
    true
}

/// Stable semantic role; unlike a context role this cannot vary by surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParameterRole {
    InputData,
    OutputData,
    Selection,
    Geometry,
    PhysicalQuantity,
    Algorithm,
    Presentation,
    OutputPolicy,
}

/// Whether a concept may enter a human parameter profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PersistenceClass {
    Profile,
    SuggestionOnly,
    RuntimeControl,
    Confirmation,
    Transient,
}

/// Shared documentation attached to one invariant concept.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ParameterDocs {
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<String>,
}

/// Authoritative invariant parameter concept.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ParameterConcept {
    pub id: ParameterConceptId,
    pub semantic_revision: SemanticRevision,
    pub casa_name: String,
    pub value_domain: ParameterType,
    pub normalization: NormalizationRule,
    #[serde(default)]
    pub base_constraints: Vec<Constraint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit_dimension: Option<UnitDimension>,
    pub semantic_role: ParameterRole,
    pub documentation: ParameterDocs,
    pub persistence_class: PersistenceClass,
}

impl ParameterConcept {
    pub fn reference(&self) -> ParameterConceptRef {
        ParameterConceptRef {
            id: self.id.clone(),
            semantic_revision: self.semantic_revision,
        }
    }
}

/// Checked authoritative concept catalog.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ParameterCatalog {
    pub schema_version: u32,
    pub concepts: Vec<ParameterConcept>,
}

impl ParameterCatalog {
    pub fn concept(&self, reference: &ParameterConceptRef) -> Option<&ParameterConcept> {
        self.concepts.iter().find(|concept| {
            concept.id == reference.id && concept.semantic_revision == reference.semantic_revision
        })
    }

    pub fn concept_by_id(&self, id: &ParameterConceptId) -> Option<&ParameterConcept> {
        self.concepts.iter().find(|concept| &concept.id == id)
    }
}

/// Pure predicate over resolved parameter values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Predicate {
    Always,
    Never,
    IsSet {
        parameter: String,
    },
    Equals {
        parameter: String,
        value: ParameterValue,
    },
    Not {
        predicate: Box<Predicate>,
    },
    All {
        predicates: Vec<Predicate>,
    },
    Any {
        predicates: Vec<Predicate>,
    },
}

impl Predicate {
    pub fn referenced_parameters(&self, output: &mut BTreeSet<String>) {
        match self {
            Self::Always | Self::Never => {}
            Self::IsSet { parameter } | Self::Equals { parameter, .. } => {
                output.insert(parameter.clone());
            }
            Self::Not { predicate } => predicate.referenced_parameters(output),
            Self::All { predicates } | Self::Any { predicates } => {
                for predicate in predicates {
                    predicate.referenced_parameters(output);
                }
            }
        }
    }
}

/// Durable task risk described by the canonical surface contract.
///
/// Frontends keep the actual confirmation gesture outside parameter profiles,
/// but they consume these classes instead of inferring safety from task or
/// parameter names.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum RunSafetyClass {
    /// The task creates one or more persistent products.
    ProductWrite,
    /// The resolved invocation authorizes replacement of an existing product.
    Overwrite,
    /// The resolved invocation mutates an existing input dataset in place.
    InputMutation,
}

/// One catalog-owned safety predicate for a one-shot task.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TaskSafetyRule {
    pub class: RunSafetyClass,
    pub when: Predicate,
}

/// One branch of a deterministic conditional default.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ConditionalDefault {
    pub when: Predicate,
    pub value: ParameterValue,
}

/// Surface-owned default selection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DefaultSpec {
    Required,
    Literal {
        value: ParameterValue,
    },
    Conditional {
        cases: Vec<ConditionalDefault>,
        fallback: ParameterValue,
    },
}

/// Binding-level constraint which must be a subset of the concept domain.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NarrowingConstraint {
    NumberRange {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max: Option<f64>,
        #[serde(default = "default_true")]
        min_inclusive: bool,
        #[serde(default = "default_true")]
        max_inclusive: bool,
    },
    Length {
        min: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max: Option<usize>,
    },
    AllowedValues {
        values: Vec<String>,
    },
    SelectorCapabilities {
        capabilities: Vec<String>,
    },
    /// Two-axis value must use the same value on both axes.
    SquarePair,
}

/// Surface-specific role for project context and suggestions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ContextRole {
    InputDataset,
    OutputProduct,
    FieldSelection,
    SpectralWindowSelection,
    ImageSelection,
    TableSelection,
    RegionReference,
    MaskReference,
    Bookmark,
    Presentation,
}

/// CLI projection for one canonical CASA-named parameter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CliParameterProjection {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub positional: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub flags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub false_flags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metavar: Option<String>,
}

/// Provider-private field mapping; never a public parameter identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ProviderParameterProjection {
    pub field: String,
    #[serde(default)]
    pub adapter: ValueAdapter,
    /// Optional provider-invocation filter. The value remains active and
    /// persistable in the profile; it is omitted only from provider calls for
    /// which this predicate is false (for example another workflow stage).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emit_when: Option<Predicate>,
}

/// Explicit conversion between canonical values and private provider fields.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ValueAdapter {
    #[default]
    Identity,
    OmitNone,
    ScalarOrPairCsv,
    QuantityNumber,
    QuantityString,
    StringListCsv,
}

/// Generated Python spelling/typing metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PythonParameterProjection {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_hint: Option<String>,
}

/// Presentation hints; they do not define meaning or defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ParameterPresentation {
    pub label: String,
    pub group: String,
    #[serde(default)]
    pub advanced: bool,
    #[serde(default)]
    pub hidden: bool,
}

/// Projections derived from a semantic binding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ParameterProjections {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli: Option<CliParameterProjection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<ProviderParameterProjection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub python: Option<PythonParameterProjection>,
    pub presentation: ParameterPresentation,
}

/// Required review record for a spelling that intentionally has another meaning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReviewedHomonym {
    pub review_id: String,
    pub reason: String,
}

/// One surface-specific use of an invariant concept.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SurfaceParameterBinding {
    pub name: String,
    pub concept: ParameterConceptRef,
    pub order: usize,
    pub default: DefaultSpec,
    pub required_when: Predicate,
    pub active_when: Predicate,
    #[serde(default)]
    pub refinements: Vec<NarrowingConstraint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_role: Option<ContextRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface_note: Option<String>,
    pub projections: ParameterProjections,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewed_homonym: Option<ReviewedHomonym>,
}

/// Profile kind pinned in the TOML header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SurfaceKind {
    Task,
    Session,
}

impl fmt::Display for SurfaceKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Task => "task",
            Self::Session => "session",
        })
    }
}

/// Ordered migration between consecutive surface-contract versions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SurfaceMigration {
    pub from_contract: u32,
    pub to_contract: u32,
    #[serde(default)]
    pub steps: Vec<MigrationStep>,
    /// Omitted values whose meaning changes because the current default changed.
    #[serde(default)]
    pub changed_defaults: Vec<String>,
}

/// Machine-readable stdout format produced by a managed task invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ManagedOutputFormat {
    Json,
}

/// Rust-owned decoder for one structured task result family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ManagedResultDecoder {
    #[serde(rename = "measurementset-summary-v1")]
    MeasurementSetSummaryV1,
    #[serde(rename = "calibration-report-v1")]
    CalibrationReportV1,
    #[serde(rename = "imager-run-v1")]
    ImagerRunV1,
}

/// One argument injected to select a provider's managed result representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ManagedOutputArgument {
    pub flag: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

/// Typed contract for a provider's structured task result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ManagedOutputContract {
    pub decoder: ManagedResultDecoder,
    pub stdout_format: ManagedOutputFormat,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inject_arguments: Vec<ManagedOutputArgument>,
    pub raw_stdout_available: bool,
    pub raw_stderr_available: bool,
}

/// Stable semantic role of a produced task resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunProductRole {
    Primary,
    Auxiliary,
    Preview,
}

/// Declared resource kind before an exact domain probe supplies dataset metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunProductKind {
    MeasurementSet,
    CasaImage,
    CasaTable,
    FitsImage,
    File,
}

/// Cardinality of one declared product binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunProductCardinality {
    One,
    Many,
}

/// Exact source of a product path. Consumers never search arbitrary result keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunProductSource {
    Parameter { parameter: String },
    DecodedArtifacts,
}

/// One product descriptor owned by the canonical surface contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RunProductDescriptor {
    pub id: String,
    pub role: RunProductRole,
    pub resource_kind: RunProductKind,
    pub source: RunProductSource,
    pub cardinality: RunProductCardinality,
    pub optional: bool,
    pub probe_supported: bool,
}

/// Explicit product classification for every surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SurfaceProductContract {
    NoProducts,
    Declared { products: Vec<RunProductDescriptor> },
}

impl Default for SurfaceProductContract {
    fn default() -> Self {
        Self::NoProducts
    }
}

impl SurfaceProductContract {
    pub fn descriptors(&self) -> &[RunProductDescriptor] {
        match self {
            Self::NoProducts => &[],
            Self::Declared { products } => products,
        }
    }
}

/// Surface-level provider routing, managed result decoding, and product semantics.
/// Executable selection itself remains inventory/runtime state, never profile data.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SurfaceExecutionProjection {
    #[serde(default)]
    pub invocation_name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fixed_args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_output: Option<ManagedOutputContract>,
    #[serde(default)]
    pub products: SurfaceProductContract,
}

/// Fully projected provider invocation, including an optional private stdin
/// payload for providers whose typed request cannot be represented as flags.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ProviderInvocation {
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdin: Option<String>,
}

impl ProviderInvocation {
    pub fn direct(args: Vec<String>) -> Self {
        Self { args, stdin: None }
    }
}

/// Provider-owned adaptation result used to prove that active parameters with
/// no direct CLI spelling were consumed rather than silently dropped.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ProviderInvocationAdaptation {
    pub invocation: ProviderInvocation,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub consumed_parameters: BTreeSet<String>,
}

impl ProviderInvocationAdaptation {
    pub fn direct(invocation: ProviderInvocation) -> Self {
        Self {
            invocation,
            consumed_parameters: BTreeSet::new(),
        }
    }
}

/// Migration of explicit sparse overrides only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MigrationStep {
    Rename {
        from: String,
        to: String,
    },
    Remove {
        parameter: String,
    },
    ReplaceValue {
        parameter: String,
        from: ParameterValue,
        to: ParameterValue,
    },
    Transform {
        parameter: String,
        transform: MigrationTransform,
    },
}

/// Closed set of safe deterministic migration transforms.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MigrationTransform {
    ScalarToArray { length: usize },
    SingletonArrayToScalar,
    IntegerToString,
    QuantityUnit { canonical_unit: String },
    Lowercase,
    Trim,
}

/// One-shot surface definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TaskDefinition {
    pub id: String,
    pub contract_version: u32,
    pub display_name: String,
    pub category: String,
    pub summary: String,
    pub provider_family: String,
    #[serde(default)]
    pub execution: SurfaceExecutionProjection,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub safety_rules: Vec<TaskSafetyRule>,
    pub bindings: Vec<SurfaceParameterBinding>,
    #[serde(default)]
    pub migrations: Vec<SurfaceMigration>,
}

/// Stateful surface definition containing durable startup/profile parameters only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SessionDefinition {
    pub id: String,
    pub contract_version: u32,
    pub display_name: String,
    pub category: String,
    pub summary: String,
    pub provider_family: String,
    #[serde(default)]
    pub execution: SurfaceExecutionProjection,
    pub bindings: Vec<SurfaceParameterBinding>,
    #[serde(default)]
    pub migrations: Vec<SurfaceMigration>,
}

/// Shared definition for every configurable executable surface.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SurfaceDefinition {
    Task(TaskDefinition),
    Session(SessionDefinition),
}

impl SurfaceDefinition {
    pub fn id(&self) -> &str {
        match self {
            Self::Task(definition) => &definition.id,
            Self::Session(definition) => &definition.id,
        }
    }

    pub fn kind(&self) -> SurfaceKind {
        match self {
            Self::Task(_) => SurfaceKind::Task,
            Self::Session(_) => SurfaceKind::Session,
        }
    }

    pub fn contract_version(&self) -> u32 {
        match self {
            Self::Task(definition) => definition.contract_version,
            Self::Session(definition) => definition.contract_version,
        }
    }

    pub fn provider_family(&self) -> &str {
        match self {
            Self::Task(definition) => &definition.provider_family,
            Self::Session(definition) => &definition.provider_family,
        }
    }

    pub fn bindings(&self) -> &[SurfaceParameterBinding] {
        match self {
            Self::Task(definition) => &definition.bindings,
            Self::Session(definition) => &definition.bindings,
        }
    }

    pub fn execution(&self) -> &SurfaceExecutionProjection {
        match self {
            Self::Task(definition) => &definition.execution,
            Self::Session(definition) => &definition.execution,
        }
    }

    pub fn safety_rules(&self) -> &[TaskSafetyRule] {
        match self {
            Self::Task(definition) => &definition.safety_rules,
            Self::Session(_) => &[],
        }
    }

    pub fn migrations(&self) -> &[SurfaceMigration] {
        match self {
            Self::Task(definition) => &definition.migrations,
            Self::Session(definition) => &definition.migrations,
        }
    }

    pub fn display_name(&self) -> &str {
        match self {
            Self::Task(definition) => &definition.display_name,
            Self::Session(definition) => &definition.display_name,
        }
    }

    pub fn category(&self) -> &str {
        match self {
            Self::Task(definition) => &definition.category,
            Self::Session(definition) => &definition.category,
        }
    }

    pub fn summary(&self) -> &str {
        match self {
            Self::Task(definition) => &definition.summary,
            Self::Session(definition) => &definition.summary,
        }
    }
}

/// Self-contained contract embedded in a provider bundle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SurfaceContractBundle {
    pub schema_version: u32,
    pub surface: SurfaceDefinition,
    pub catalog: ParameterCatalog,
}

impl SurfaceContractBundle {
    pub fn validate(&self) -> Result<(), Vec<ContractValidationError>> {
        validate_surface_bundles_internal(std::slice::from_ref(self), false)
    }
}

/// Aggregate checked catalog used by launchers and generated references.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SurfaceCatalogBundle {
    pub schema_version: u32,
    pub catalog: ParameterCatalog,
    pub surfaces: Vec<SurfaceDefinition>,
}

impl SurfaceCatalogBundle {
    pub fn surface(&self, id: &str) -> Option<&SurfaceDefinition> {
        self.surfaces.iter().find(|surface| surface.id() == id)
    }

    pub fn embedded_surface(&self, id: &str) -> Option<SurfaceContractBundle> {
        let surface = self.surface(id)?.clone();
        let refs = surface
            .bindings()
            .iter()
            .map(|binding| binding.concept.clone())
            .collect::<BTreeSet<_>>();
        let catalog = ParameterCatalog {
            schema_version: self.catalog.schema_version,
            concepts: self
                .catalog
                .concepts
                .iter()
                .filter(|concept| refs.contains(&concept.reference()))
                .cloned()
                .collect(),
        };
        Some(SurfaceContractBundle {
            schema_version: self.schema_version,
            surface,
            catalog,
        })
    }

    pub fn validate(&self) -> Result<(), Vec<ContractValidationError>> {
        let bundles = self
            .surfaces
            .iter()
            .cloned()
            .map(|surface| SurfaceContractBundle {
                schema_version: self.schema_version,
                surface,
                catalog: self.catalog.clone(),
            })
            .collect::<Vec<_>>();
        validate_surface_bundles(&bundles)
    }
}

/// Stable structural validation failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ContractValidationError {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameter: Option<String>,
}

impl ContractValidationError {
    fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
            surface: None,
            parameter: None,
        }
    }

    fn at(mut self, surface: &str, parameter: Option<&str>) -> Self {
        self.surface = Some(surface.to_string());
        self.parameter = parameter.map(str::to_string);
        self
    }
}

/// Validate one or more bundles together, including cross-surface invariants.
pub fn validate_surface_bundles(
    bundles: &[SurfaceContractBundle],
) -> Result<(), Vec<ContractValidationError>> {
    validate_surface_bundles_internal(bundles, true)
}

fn validate_surface_bundles_internal(
    bundles: &[SurfaceContractBundle],
    validate_cross_surface_names: bool,
) -> Result<(), Vec<ContractValidationError>> {
    let mut errors = Vec::new();
    let mut merged = BTreeMap::<(ParameterConceptId, SemanticRevision), ParameterConcept>::new();
    let mut surface_ids = BTreeSet::new();

    for bundle in bundles {
        if bundle.schema_version != PARAMETER_CATALOG_SCHEMA_VERSION
            || bundle.catalog.schema_version != PARAMETER_CATALOG_SCHEMA_VERSION
        {
            errors.push(ContractValidationError::new(
                "schema_version",
                format!(
                    "surface {} uses unsupported parameter schema {} / catalog {}",
                    bundle.surface.id(),
                    bundle.schema_version,
                    bundle.catalog.schema_version
                ),
            ));
        }
        if !surface_ids.insert(bundle.surface.id().to_string()) {
            errors.push(ContractValidationError::new(
                "duplicate_surface",
                format!("duplicate surface {}", bundle.surface.id()),
            ));
        }
        for concept in &bundle.catalog.concepts {
            let key = (concept.id.clone(), concept.semantic_revision);
            match merged.get(&key) {
                Some(existing) if existing != concept => errors.push(ContractValidationError::new(
                    "divergent_concept",
                    format!(
                        "concept {} revision {} has divergent definitions",
                        concept.id, concept.semantic_revision.0
                    ),
                )),
                Some(_) => {}
                None => {
                    if let NormalizationRule::CasaSelector { capabilities, .. } =
                        &concept.normalization
                    {
                        for capability in capabilities.iter().filter(|capability| {
                            !SELECTOR_CAPABILITY_NAMES.contains(&capability.as_str())
                        }) {
                            errors.push(ContractValidationError::new(
                                "unknown_selector_capability",
                                format!(
                                    "concept {} declares unknown selector capability {capability:?}",
                                    concept.id
                                ),
                            ));
                        }
                    }
                    merged.insert(key, concept.clone());
                }
            }
        }
        validate_one_surface(bundle, &mut errors);
    }

    if validate_cross_surface_names {
        validate_repeated_names(bundles, &mut errors);
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn validate_one_surface(bundle: &SurfaceContractBundle, errors: &mut Vec<ContractValidationError>) {
    let surface = &bundle.surface;
    let mut names = BTreeSet::new();
    let mut orders = BTreeSet::new();
    let known = surface
        .bindings()
        .iter()
        .map(|binding| binding.name.clone())
        .collect::<BTreeSet<_>>();

    validate_aliases(surface, &known, errors);

    if surface.id().is_empty() || surface.contract_version() == 0 {
        errors.push(
            ContractValidationError::new(
                "invalid_surface",
                "surface id must be non-empty and contract version must be positive",
            )
            .at(surface.id(), None),
        );
    }

    validate_safety_rules(bundle, &known, errors);

    for binding in surface.bindings() {
        if !names.insert(binding.name.clone()) {
            errors.push(
                ContractValidationError::new(
                    "duplicate_parameter",
                    format!("duplicate parameter {}", binding.name),
                )
                .at(surface.id(), Some(&binding.name)),
            );
        }
        if !orders.insert(binding.order) {
            errors.push(
                ContractValidationError::new(
                    "duplicate_order",
                    format!("duplicate presentation order {}", binding.order),
                )
                .at(surface.id(), Some(&binding.name)),
            );
        }
        validate_profile_parameter_name(binding, surface.id(), errors);
        let Some(concept) = bundle.catalog.concept(&binding.concept) else {
            errors.push(
                ContractValidationError::new(
                    "missing_concept",
                    format!(
                        "missing concept {} revision {}",
                        binding.concept.id, binding.concept.semantic_revision.0
                    ),
                )
                .at(surface.id(), Some(&binding.name)),
            );
            continue;
        };
        if let Err(reason) = validate_parameter_domain(&concept.value_domain) {
            errors.push(
                ContractValidationError::new(
                    "invalid_value_domain",
                    format!(
                        "concept {} has an invalid value domain: {reason}",
                        concept.id
                    ),
                )
                .at(surface.id(), Some(&binding.name)),
            );
        }
        if binding.name != concept.casa_name {
            errors.push(
                ContractValidationError::new(
                    "noncanonical_name",
                    format!(
                        "binding name {:?} differs from concept CASA name {:?}",
                        binding.name, concept.casa_name
                    ),
                )
                .at(surface.id(), Some(&binding.name)),
            );
        }
        if concept.persistence_class != PersistenceClass::Profile {
            errors.push(
                ContractValidationError::new(
                    "nonprofile_concept",
                    format!(
                        "concept {} is {:?} and cannot be bound in a profile",
                        concept.id, concept.persistence_class
                    ),
                )
                .at(surface.id(), Some(&binding.name)),
            );
        }
        match (binding.context_role, concept.semantic_role) {
            (Some(ContextRole::InputDataset), ParameterRole::InputData)
            | (Some(ContextRole::OutputProduct), ParameterRole::OutputData)
            | (None, _) => {}
            (Some(ContextRole::InputDataset), role) => errors.push(
                ContractValidationError::new(
                    "context_role_mismatch",
                    format!("input_dataset requires an input_data concept, got {role:?}"),
                )
                .at(surface.id(), Some(&binding.name)),
            ),
            (Some(ContextRole::OutputProduct), role) => errors.push(
                ContractValidationError::new(
                    "context_role_mismatch",
                    format!("output_product requires an output_data concept, got {role:?}"),
                )
                .at(surface.id(), Some(&binding.name)),
            ),
            (Some(_), _) => {}
        }
        validate_reserved_name(binding, surface.id(), errors);
        validate_predicate_refs(
            &binding.required_when,
            &known,
            surface.id(),
            &binding.name,
            errors,
        );
        validate_predicate_refs(
            &binding.active_when,
            &known,
            surface.id(),
            &binding.name,
            errors,
        );
        if let Some(predicate) = binding
            .projections
            .provider
            .as_ref()
            .and_then(|projection| projection.emit_when.as_ref())
        {
            validate_predicate_refs(predicate, &known, surface.id(), &binding.name, errors);
        }
        if !matches!(binding.required_when, Predicate::Always)
            && matches!(binding.default, DefaultSpec::Required)
        {
            errors.push(
                ContractValidationError::new(
                    "missing_optional_default",
                    "every conditionally optional parameter needs a typed default",
                )
                .at(surface.id(), Some(&binding.name)),
            );
        }
        for value in default_values(&binding.default) {
            if let Err(reason) = validate_parameter_value(value, &concept.value_domain) {
                errors.push(
                    ContractValidationError::new(
                        "invalid_default",
                        format!("default does not match concept domain: {reason}"),
                    )
                    .at(surface.id(), Some(&binding.name)),
                );
            }
            validate_constraints(
                value,
                &concept.base_constraints,
                surface.id(),
                binding,
                errors,
            );
            validate_refinement_value(
                value,
                &concept.normalization,
                &binding.refinements,
                surface.id(),
                binding,
                errors,
            );
        }
        validate_refinements(concept, binding, surface.id(), errors);
    }
    validate_default_cycles(surface, errors);
    validate_migrations(surface, errors);
}

fn validate_aliases(
    surface: &SurfaceDefinition,
    canonical_names: &BTreeSet<String>,
    errors: &mut Vec<ContractValidationError>,
) {
    let mut aliases = BTreeMap::<&str, &str>::new();
    for binding in surface.bindings() {
        for alias in &binding.aliases {
            if canonical_names.contains(alias) {
                errors.push(
                    ContractValidationError::new(
                        "alias_canonical_collision",
                        format!(
                            "alias {alias:?} for parameter {:?} collides with a canonical parameter name",
                            binding.name
                        ),
                    )
                    .at(surface.id(), Some(&binding.name)),
                );
            }
            if let Some(existing) = aliases.insert(alias, &binding.name) {
                errors.push(
                    ContractValidationError::new(
                        "duplicate_alias",
                        format!(
                            "alias {alias:?} for parameter {:?} is already used by parameter {existing:?}",
                            binding.name
                        ),
                    )
                    .at(surface.id(), Some(&binding.name)),
                );
            }
        }
    }
}

/// Runtime authority and UI/process state must never enter a portable profile.
/// Exact names cover existing controls; prefixes make the gate fail closed for
/// newly introduced variants in the same authority class.
const FORBIDDEN_PROFILE_PARAMETER_NAMES: &[&str] = &[
    "binary",
    "confirm",
    "executable",
    "invocation_name",
    "no_save_last",
    "override_env",
    "provider_binary",
    "provider_executable",
    "runtime",
    "telemetry",
    "ui_state",
    "workspace",
];

const FORBIDDEN_PROFILE_PARAMETER_PREFIXES: &[&str] = &[
    "binary_",
    "confirm_",
    "confirmation_",
    "executable_",
    "no_save_",
    "override_env_",
    "runtime_",
    "telemetry_",
    "ui_",
    "workspace_",
];

fn validate_profile_parameter_name(
    binding: &SurfaceParameterBinding,
    surface: &str,
    errors: &mut Vec<ContractValidationError>,
) {
    let exact = FORBIDDEN_PROFILE_PARAMETER_NAMES.contains(&binding.name.as_str());
    let prefix = FORBIDDEN_PROFILE_PARAMETER_PREFIXES
        .iter()
        .find(|prefix| binding.name.starts_with(**prefix));
    if exact || prefix.is_some() {
        let policy = prefix.map_or("exact runtime-authority name", |prefix| *prefix);
        errors.push(
            ContractValidationError::new(
                "forbidden_profile_parameter_name",
                format!(
                    "parameter {:?} is reserved by the non-profile authority policy ({policy})",
                    binding.name
                ),
            )
            .at(surface, Some(&binding.name)),
        );
    }
}

fn validate_safety_rules(
    bundle: &SurfaceContractBundle,
    known: &BTreeSet<String>,
    errors: &mut Vec<ContractValidationError>,
) {
    let surface = &bundle.surface;
    let mut classes = BTreeSet::new();
    for rule in surface.safety_rules() {
        if !classes.insert(rule.class) {
            errors.push(
                ContractValidationError::new(
                    "duplicate_safety_class",
                    format!("duplicate {:?} task safety rule", rule.class),
                )
                .at(surface.id(), None),
            );
        }

        let mut refs = BTreeSet::new();
        rule.when.referenced_parameters(&mut refs);
        for reference in refs.difference(known) {
            errors.push(
                ContractValidationError::new(
                    "unknown_safety_parameter",
                    format!(
                        "{:?} safety predicate references unknown parameter {reference}",
                        rule.class
                    ),
                )
                .at(surface.id(), None),
            );
        }
        validate_safety_predicate_values(&rule.when, bundle, rule.class, errors);
    }

    let overwrite_bindings = surface
        .bindings()
        .iter()
        .filter(|binding| binding.concept.id.as_str() == OVERWRITE_CONCEPT_ID)
        .collect::<Vec<_>>();
    if overwrite_bindings.is_empty() {
        return;
    }

    let Some(rule) = surface
        .safety_rules()
        .iter()
        .find(|rule| rule.class == RunSafetyClass::Overwrite)
    else {
        errors.push(
            ContractValidationError::new(
                "missing_overwrite_safety",
                "an output.overwrite binding requires an overwrite safety rule",
            )
            .at(surface.id(), None),
        );
        return;
    };
    for binding in overwrite_bindings {
        let expected = Predicate::Equals {
            parameter: binding.name.clone(),
            value: ParameterValue::Bool(true),
        };
        if rule.when != expected {
            errors.push(
                ContractValidationError::new(
                    "unsafe_overwrite_predicate",
                    format!("overwrite safety must be exactly {} == true", binding.name),
                )
                .at(surface.id(), Some(&binding.name)),
            );
        }
    }
}

fn validate_safety_predicate_values(
    predicate: &Predicate,
    bundle: &SurfaceContractBundle,
    class: RunSafetyClass,
    errors: &mut Vec<ContractValidationError>,
) {
    match predicate {
        Predicate::Always | Predicate::Never | Predicate::IsSet { .. } => {}
        Predicate::Equals { parameter, value } => {
            let Some(binding) = bundle
                .surface
                .bindings()
                .iter()
                .find(|binding| binding.name == *parameter)
            else {
                return;
            };
            let Some(concept) = bundle.catalog.concept(&binding.concept) else {
                return;
            };
            if let Err(reason) = validate_parameter_value(value, &concept.value_domain) {
                errors.push(
                    ContractValidationError::new(
                        "invalid_safety_value",
                        format!(
                            "{:?} safety predicate value does not match {parameter}: {reason}",
                            class
                        ),
                    )
                    .at(bundle.surface.id(), Some(parameter)),
                );
            }
        }
        Predicate::Not { predicate } => {
            validate_safety_predicate_values(predicate, bundle, class, errors);
        }
        Predicate::All { predicates } | Predicate::Any { predicates } => {
            for predicate in predicates {
                validate_safety_predicate_values(predicate, bundle, class, errors);
            }
        }
    }
}

fn validate_reserved_name(
    binding: &SurfaceParameterBinding,
    surface: &str,
    errors: &mut Vec<ContractValidationError>,
) {
    let designated = match binding.name.as_str() {
        "imsize" => Some(IMSIZE_CONCEPT_ID),
        "cell" => Some(CELL_CONCEPT_ID),
        "field" => Some(FIELD_CONCEPT_ID),
        "spw" => Some(SPW_CONCEPT_ID),
        _ => None,
    };
    if let Some(designated) = designated
        && binding.concept.id.as_str() != designated
    {
        errors.push(
            ContractValidationError::new(
                "reserved_name",
                format!(
                    "reserved parameter {} must bind concept {}",
                    binding.name, designated
                ),
            )
            .at(surface, Some(&binding.name)),
        );
    }
}

fn validate_predicate_refs(
    predicate: &Predicate,
    known: &BTreeSet<String>,
    surface: &str,
    parameter: &str,
    errors: &mut Vec<ContractValidationError>,
) {
    let mut refs = BTreeSet::new();
    predicate.referenced_parameters(&mut refs);
    for reference in refs.difference(known) {
        errors.push(
            ContractValidationError::new(
                "unknown_predicate_parameter",
                format!("predicate references unknown parameter {reference}"),
            )
            .at(surface, Some(parameter)),
        );
    }
}

fn default_values(default: &DefaultSpec) -> Vec<&ParameterValue> {
    match default {
        DefaultSpec::Required => Vec::new(),
        DefaultSpec::Literal { value } => vec![value],
        DefaultSpec::Conditional { cases, fallback } => cases
            .iter()
            .map(|case| &case.value)
            .chain(std::iter::once(fallback))
            .collect(),
    }
}

/// Structural type validation shared with the runtime.
pub fn validate_parameter_value(
    value: &ParameterValue,
    domain: &ParameterType,
) -> Result<(), String> {
    match (value, domain) {
        (ParameterValue::Bool(_), ParameterType::Bool)
        | (ParameterValue::Integer(_), ParameterType::Integer)
        | (ParameterValue::String(_), ParameterType::String)
        | (ParameterValue::String(_), ParameterType::Path { .. }) => Ok(()),
        (
            ParameterValue::String(value),
            ParameterType::Quantity {
                dimension,
                canonical_unit,
                special_values,
            },
        ) => validate_quantity_value(value, *dimension, canonical_unit, special_values),
        (ParameterValue::Integer(_), ParameterType::Float)
        | (ParameterValue::Float(_), ParameterType::Float) => Ok(()),
        (ParameterValue::String(value), ParameterType::Choice { values }) => {
            if values.iter().any(|choice| choice == value) {
                Ok(())
            } else {
                Err(format!("{value:?} is not one of {}", values.join(", ")))
            }
        }
        (
            ParameterValue::Array(values),
            ParameterType::Array {
                element,
                min_items,
                max_items,
                ..
            },
        ) => {
            if values.len() < *min_items || max_items.is_some_and(|max| values.len() > max) {
                return Err(format!(
                    "array length {} is outside {}..={}",
                    values.len(),
                    min_items,
                    max_items.map_or_else(|| "unbounded".to_string(), |max| max.to_string())
                ));
            }
            for value in values {
                validate_parameter_value(value, element)?;
            }
            Ok(())
        }
        (
            value,
            ParameterType::Array {
                element,
                allow_scalar: true,
                ..
            },
        ) => validate_parameter_value(value, element),
        (ParameterValue::Table(values), ParameterType::Table { fields }) => {
            for (name, domain) in fields {
                let value = values
                    .get(name)
                    .ok_or_else(|| format!("missing table field {name}"))?;
                validate_parameter_value(value, domain)?;
            }
            if values.keys().any(|name| !fields.contains_key(name)) {
                return Err("table contains an unknown field".to_string());
            }
            Ok(())
        }
        (ParameterValue::String(value), ParameterType::Optional { states, .. })
            if states.contains(value) =>
        {
            Ok(())
        }
        (value, ParameterType::Optional { value: domain, .. }) => {
            validate_parameter_value(value, domain)
        }
        (ParameterValue::Float(value), _) if !value.is_finite() => {
            Err("non-finite numbers are forbidden".to_string())
        }
        _ => Err(format!("value {value:?} does not match {domain:?}")),
    }
}

fn validate_parameter_domain(domain: &ParameterType) -> Result<(), String> {
    match domain {
        ParameterType::Quantity {
            dimension,
            canonical_unit,
            ..
        } => validate_quantity_domain(*dimension, canonical_unit).map(|_| ()),
        ParameterType::Array { element, .. } | ParameterType::Optional { value: element, .. } => {
            validate_parameter_domain(element)
        }
        ParameterType::Table { fields } => {
            for field in fields.values() {
                validate_parameter_domain(field)?;
            }
            Ok(())
        }
        ParameterType::Bool
        | ParameterType::Integer
        | ParameterType::Float
        | ParameterType::String
        | ParameterType::Path { .. }
        | ParameterType::Choice { .. } => Ok(()),
    }
}

fn validate_quantity_value(
    value: &str,
    dimension: UnitDimension,
    canonical_unit: &str,
    special_values: &[String],
) -> Result<(), String> {
    let canonical = validate_quantity_domain(dimension, canonical_unit)?;
    let value = value.trim();
    if special_values.iter().any(|special| special == value) {
        return Ok(());
    }
    let quantity_text = if value.parse::<f64>().is_ok() {
        format!("{value}{canonical_unit}")
    } else {
        value.to_string()
    };
    let quantity = quantity_text
        .parse::<Quantity>()
        .map_err(|error| format!("invalid quantity {value:?}: {error}"))?;
    if !quantity.value().is_finite() {
        return Err(format!("quantity {value:?} is not finite"));
    }
    let canonical_value = quantity.get_value_in(&canonical).map_err(|error| {
        format!(
            "quantity {value:?} is not conformant with canonical unit {canonical_unit:?}: {error}"
        )
    })?;
    if !canonical_value.is_finite() {
        return Err(format!(
            "quantity {value:?} is non-finite in canonical unit {canonical_unit:?}"
        ));
    }
    Ok(())
}

fn validate_quantity_domain(
    dimension: UnitDimension,
    canonical_unit: &str,
) -> Result<Unit, String> {
    let canonical = parse_unit(canonical_unit)
        .map_err(|error| format!("invalid canonical unit {canonical_unit:?}: {error}"))?;
    let expected = match dimension {
        UnitDimension::Dimensionless => Some(Unit::dimensionless()),
        UnitDimension::Angle => Some(parse_builtin_unit("rad")?),
        UnitDimension::Time => Some(parse_builtin_unit("s")?),
        UnitDimension::Frequency => Some(parse_builtin_unit("Hz")?),
        UnitDimension::Velocity => Some(parse_builtin_unit("m/s")?),
        UnitDimension::Length => Some(parse_builtin_unit("m")?),
        UnitDimension::FluxDensity => Some(parse_builtin_unit("Jy")?),
        UnitDimension::FluxDensityPerBeam => Some(parse_builtin_unit("Jy/beam")?),
        UnitDimension::Temperature => Some(parse_builtin_unit("K")?),
        UnitDimension::DataSize => {
            return Err(
                "data-size quantities are not represented by the casa-types unit registry"
                    .to_string(),
            );
        }
        UnitDimension::Custom => None,
    };
    if expected.is_some_and(|expected| !canonical.conformant(&expected)) {
        return Err(format!(
            "canonical unit {canonical_unit:?} does not match declared dimension {dimension:?}"
        ));
    }
    Ok(canonical)
}

fn parse_unit(name: &str) -> Result<Unit, casa_types::quanta::UnitError> {
    if name.is_empty() {
        Ok(Unit::dimensionless())
    } else {
        Unit::new(name)
    }
}

fn parse_builtin_unit(name: &str) -> Result<Unit, String> {
    parse_unit(name).map_err(|error| format!("internal reference unit {name:?}: {error}"))
}

fn validate_constraints(
    value: &ParameterValue,
    constraints: &[Constraint],
    surface: &str,
    binding: &SurfaceParameterBinding,
    errors: &mut Vec<ContractValidationError>,
) {
    for constraint in constraints {
        let valid = match constraint {
            Constraint::Finite => numeric_values(value).into_iter().all(f64::is_finite),
            Constraint::NonEmpty => value_len(value).is_none_or(|length| length > 0),
            Constraint::Positive => numeric_values(value).into_iter().all(|number| number > 0.0),
            Constraint::NumberRange {
                min,
                max,
                min_inclusive,
                max_inclusive,
            } => numeric_values(value).into_iter().all(|number| {
                min.is_none_or(|min| {
                    if *min_inclusive {
                        number >= min
                    } else {
                        number > min
                    }
                }) && max.is_none_or(|max| {
                    if *max_inclusive {
                        number <= max
                    } else {
                        number < max
                    }
                })
            }),
            Constraint::Length { min, max } => value_len(value)
                .is_none_or(|length| length >= *min && max.is_none_or(|max| length <= max)),
            Constraint::AllowedValues { values } => match value {
                ParameterValue::String(value) => values.contains(value),
                _ => true,
            },
        };
        if !valid {
            errors.push(
                ContractValidationError::new(
                    "default_constraint",
                    format!("default violates base constraint {constraint:?}"),
                )
                .at(surface, Some(&binding.name)),
            );
        }
    }
}

fn validate_refinement_value(
    value: &ParameterValue,
    normalization: &NormalizationRule,
    refinements: &[NarrowingConstraint],
    surface: &str,
    binding: &SurfaceParameterBinding,
    errors: &mut Vec<ContractValidationError>,
) {
    for refinement in refinements {
        let valid = match refinement {
            NarrowingConstraint::NumberRange {
                min,
                max,
                min_inclusive,
                max_inclusive,
            } => numeric_values(value).into_iter().all(|number| {
                min.is_none_or(|min| {
                    if *min_inclusive {
                        number >= min
                    } else {
                        number > min
                    }
                }) && max.is_none_or(|max| {
                    if *max_inclusive {
                        number <= max
                    } else {
                        number < max
                    }
                })
            }),
            NarrowingConstraint::Length { min, max } => value_len(value)
                .is_none_or(|length| length >= *min && max.is_none_or(|max| length <= max)),
            NarrowingConstraint::AllowedValues { values } => match value {
                ParameterValue::String(value) => values.contains(value),
                _ => true,
            },
            NarrowingConstraint::SelectorCapabilities { capabilities } => match normalization {
                NormalizationRule::CasaSelector { grammar, .. } => {
                    validate_selector_capabilities(*grammar, value, capabilities).is_ok()
                }
                _ => false,
            },
            NarrowingConstraint::SquarePair => match value {
                ParameterValue::Array(values) if values.len() == 2 => values[0] == values[1],
                // A scalar accepted by an allow-scalar pair domain normalizes to
                // the same value on both axes and is therefore square.
                ParameterValue::Bool(_)
                | ParameterValue::Integer(_)
                | ParameterValue::Float(_)
                | ParameterValue::String(_) => true,
                _ => false,
            },
        };
        if !valid {
            errors.push(
                ContractValidationError::new(
                    "default_refinement",
                    format!("default violates refinement {refinement:?}"),
                )
                .at(surface, Some(&binding.name)),
            );
        }
    }
}

fn validate_refinements(
    concept: &ParameterConcept,
    binding: &SurfaceParameterBinding,
    surface: &str,
    errors: &mut Vec<ContractValidationError>,
) {
    for refinement in &binding.refinements {
        let narrowing = match refinement {
            NarrowingConstraint::NumberRange { min, max, .. } => {
                let base =
                    concept
                        .base_constraints
                        .iter()
                        .find_map(|constraint| match constraint {
                            Constraint::NumberRange { min, max, .. } => Some((*min, *max)),
                            _ => None,
                        });
                domain_is_numeric(&concept.value_domain)
                    && base.is_none_or(|(base_min, base_max)| {
                        base_min.is_none_or(|base| min.is_some_and(|value| value >= base))
                            && base_max.is_none_or(|base| max.is_some_and(|value| value <= base))
                    })
            }
            NarrowingConstraint::Length { min, max } => match &concept.value_domain {
                ParameterType::Array {
                    min_items,
                    max_items,
                    ..
                } => {
                    min >= min_items
                        && max_items.is_none_or(|base| max.is_some_and(|value| value <= base))
                }
                _ => false,
            },
            NarrowingConstraint::AllowedValues { values } => match &concept.value_domain {
                ParameterType::Choice { values: base } => {
                    values.iter().all(|value| base.contains(value))
                }
                _ => concept
                    .base_constraints
                    .iter()
                    .any(|constraint| match constraint {
                        Constraint::AllowedValues { values: base } => {
                            values.iter().all(|value| base.contains(value))
                        }
                        _ => false,
                    }),
            },
            NarrowingConstraint::SelectorCapabilities { capabilities } => {
                match &concept.normalization {
                    NormalizationRule::CasaSelector {
                        capabilities: base, ..
                    } => {
                        base.iter().all(|capability| {
                            SELECTOR_CAPABILITY_NAMES.contains(&capability.as_str())
                        }) && capabilities.iter().all(|capability| {
                            SELECTOR_CAPABILITY_NAMES.contains(&capability.as_str())
                                && base.contains(capability)
                        })
                    }
                    _ => false,
                }
            }
            NarrowingConstraint::SquarePair => domain_is_two_axis_pair(&concept.value_domain),
        };
        if !narrowing {
            errors.push(
                ContractValidationError::new(
                    "non_narrowing_refinement",
                    format!("refinement {refinement:?} is not a provable subset"),
                )
                .at(surface, Some(&binding.name)),
            );
        }
    }
}

fn domain_is_numeric(domain: &ParameterType) -> bool {
    match domain {
        ParameterType::Integer | ParameterType::Float | ParameterType::Quantity { .. } => true,
        ParameterType::Array { element, .. } => domain_is_numeric(element),
        ParameterType::Optional { value, .. } => domain_is_numeric(value),
        _ => false,
    }
}

fn domain_is_two_axis_pair(domain: &ParameterType) -> bool {
    match domain {
        ParameterType::Array {
            min_items: 2,
            max_items: Some(2),
            ..
        } => true,
        ParameterType::Optional { value, .. } => domain_is_two_axis_pair(value),
        _ => false,
    }
}

fn numeric_values(value: &ParameterValue) -> Vec<f64> {
    match value {
        ParameterValue::Integer(value) => vec![*value as f64],
        ParameterValue::Float(value) => vec![*value],
        ParameterValue::String(value) => quantity_number(value).into_iter().collect(),
        ParameterValue::Array(values) => values.iter().flat_map(numeric_values).collect(),
        ParameterValue::Table(values) => values.values().flat_map(numeric_values).collect(),
        ParameterValue::Bool(_) => Vec::new(),
    }
}

fn quantity_number(value: &str) -> Option<f64> {
    let value = value.trim();
    if let Ok(number) = value.parse::<f64>() {
        return Some(number);
    }
    let split = value
        .find(|character: char| {
            !character.is_ascii_digit()
                && character != '.'
                && character != '-'
                && character != '+'
                && character != 'e'
                && character != 'E'
        })
        .unwrap_or(value.len());
    (split > 0)
        .then(|| value[..split].parse::<f64>().ok())
        .flatten()
}

fn value_len(value: &ParameterValue) -> Option<usize> {
    match value {
        ParameterValue::String(value) => Some(value.len()),
        ParameterValue::Array(value) => Some(value.len()),
        ParameterValue::Table(value) => Some(value.len()),
        _ => None,
    }
}

fn validate_default_cycles(surface: &SurfaceDefinition, errors: &mut Vec<ContractValidationError>) {
    let mut graph = BTreeMap::<String, BTreeSet<String>>::new();
    for binding in surface.bindings() {
        let mut dependencies = BTreeSet::new();
        if let DefaultSpec::Conditional { cases, .. } = &binding.default {
            for case in cases {
                case.when.referenced_parameters(&mut dependencies);
            }
        }
        graph.insert(binding.name.clone(), dependencies);
    }
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for name in graph.keys() {
        if detects_cycle(name, &graph, &mut visiting, &mut visited) {
            errors.push(
                ContractValidationError::new(
                    "default_cycle",
                    format!("conditional default graph contains a cycle at {name}"),
                )
                .at(surface.id(), Some(name)),
            );
            break;
        }
    }
}

fn detects_cycle(
    name: &str,
    graph: &BTreeMap<String, BTreeSet<String>>,
    visiting: &mut BTreeSet<String>,
    visited: &mut BTreeSet<String>,
) -> bool {
    if visited.contains(name) {
        return false;
    }
    if !visiting.insert(name.to_string()) {
        return true;
    }
    if let Some(dependencies) = graph.get(name) {
        for dependency in dependencies {
            if graph.contains_key(dependency) && detects_cycle(dependency, graph, visiting, visited)
            {
                return true;
            }
        }
    }
    visiting.remove(name);
    visited.insert(name.to_string());
    false
}

fn validate_migrations(surface: &SurfaceDefinition, errors: &mut Vec<ContractValidationError>) {
    if surface.contract_version() > 1 && surface.migrations().is_empty() {
        errors.push(
            ContractValidationError::new(
                "missing_migrations",
                "surface contracts newer than 1 require a complete migration chain from contract 1",
            )
            .at(surface.id(), None),
        );
        return;
    }
    let mut expected = 1;
    let known = surface
        .bindings()
        .iter()
        .map(|binding| binding.name.as_str())
        .collect::<BTreeSet<_>>();
    for migration in surface.migrations() {
        if migration.from_contract != expected
            || migration.to_contract != migration.from_contract.saturating_add(1)
        {
            errors.push(
                ContractValidationError::new(
                    "migration_chain",
                    "migrations must be ordered and advance exactly one contract version",
                )
                .at(surface.id(), None),
            );
        }
        for name in &migration.changed_defaults {
            if !known.contains(name.as_str()) {
                errors.push(
                    ContractValidationError::new(
                        "unknown_changed_default",
                        format!("migration names unknown changed-default parameter {name:?}"),
                    )
                    .at(surface.id(), Some(name)),
                );
            }
        }
        for step in &migration.steps {
            let current_name = match step {
                MigrationStep::Rename { to, .. } => Some(to),
                MigrationStep::ReplaceValue { parameter, .. }
                | MigrationStep::Transform { parameter, .. } => Some(parameter),
                MigrationStep::Remove { .. } => None,
            };
            if let Some(name) = current_name
                && !known.contains(name.as_str())
            {
                errors.push(
                    ContractValidationError::new(
                        "unknown_migration_target",
                        format!("migration targets unknown current parameter {name:?}"),
                    )
                    .at(surface.id(), Some(name)),
                );
            }
        }
        expected = migration.to_contract;
    }
    if expected != surface.contract_version() {
        errors.push(
            ContractValidationError::new(
                "migration_target",
                "migration chain must end at the current contract version",
            )
            .at(surface.id(), None),
        );
    }
}

fn validate_repeated_names(
    bundles: &[SurfaceContractBundle],
    errors: &mut Vec<ContractValidationError>,
) {
    let mut grouped = BTreeMap::<
        String,
        BTreeMap<ParameterConceptRef, Vec<(&str, &SurfaceParameterBinding)>>,
    >::new();
    for bundle in bundles {
        for binding in bundle.surface.bindings() {
            grouped
                .entry(binding.name.clone())
                .or_default()
                .entry(binding.concept.clone())
                .or_default()
                .push((bundle.surface.id(), binding));
        }
    }

    for (name, concepts) in grouped {
        let baseline = concepts.keys().next().cloned();
        for (concept, bindings) in concepts {
            let reviewed = bindings
                .iter()
                .filter(|(_, binding)| binding.reviewed_homonym.is_some())
                .collect::<Vec<_>>();
            let needs_review = baseline
                .as_ref()
                .is_some_and(|baseline| baseline != &concept);
            if needs_review && reviewed.is_empty() {
                let (surface, binding) = bindings[0];
                errors.push(
                    ContractValidationError::new(
                        "unreviewed_homonym",
                        format!(
                            "canonical name {name} uses concept {} in addition to baseline {} without one concept-level review",
                            concept.id,
                            baseline.as_ref().expect("multi-concept name has baseline").id
                        ),
                    )
                    .at(surface, Some(&binding.name)),
                );
            }
            let allowed_reviews = usize::from(needs_review);
            for (surface, binding) in reviewed.into_iter().skip(allowed_reviews) {
                errors.push(
                    ContractValidationError::new(
                        "redundant_homonym_review",
                        format!(
                            "canonical name {name} concept {} already has sufficient concept-level homonym evidence",
                            concept.id
                        ),
                    )
                    .at(surface, Some(&binding.name)),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn concept(id: &str, name: &str, value_domain: ParameterType) -> ParameterConcept {
        ParameterConcept {
            id: id.into(),
            semantic_revision: SemanticRevision(1),
            casa_name: name.to_string(),
            value_domain,
            normalization: NormalizationRule::Identity,
            base_constraints: Vec::new(),
            unit_dimension: None,
            semantic_role: ParameterRole::Algorithm,
            documentation: ParameterDocs {
                summary: name.to_string(),
                details: None,
                examples: Vec::new(),
            },
            persistence_class: PersistenceClass::Profile,
        }
    }

    fn binding(concept: &ParameterConcept, default: DefaultSpec) -> SurfaceParameterBinding {
        SurfaceParameterBinding {
            name: concept.casa_name.clone(),
            concept: concept.reference(),
            order: 0,
            default,
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
                    label: concept.casa_name.clone(),
                    group: "General".to_string(),
                    advanced: false,
                    hidden: false,
                },
            },
            aliases: Vec::new(),
            reviewed_homonym: None,
        }
    }

    fn bundle(
        concept: ParameterConcept,
        binding: SurfaceParameterBinding,
    ) -> SurfaceContractBundle {
        SurfaceContractBundle {
            schema_version: PARAMETER_CATALOG_SCHEMA_VERSION,
            surface: SurfaceDefinition::Task(TaskDefinition {
                id: "example".to_string(),
                contract_version: 1,
                display_name: "Example".to_string(),
                category: "Test".to_string(),
                summary: "Example".to_string(),
                provider_family: "test".to_string(),
                execution: SurfaceExecutionProjection::default(),
                safety_rules: Vec::new(),
                bindings: vec![binding],
                migrations: Vec::new(),
            }),
            catalog: ParameterCatalog {
                schema_version: PARAMETER_CATALOG_SCHEMA_VERSION,
                concepts: vec![concept],
            },
        }
    }

    fn set_surface_id(bundle: &mut SurfaceContractBundle, id: &str) {
        match &mut bundle.surface {
            SurfaceDefinition::Task(definition) => definition.id = id.to_string(),
            SurfaceDefinition::Session(definition) => definition.id = id.to_string(),
        }
    }

    fn add_binding(
        bundle: &mut SurfaceContractBundle,
        concept: ParameterConcept,
        binding: SurfaceParameterBinding,
    ) {
        bundle.catalog.concepts.push(concept);
        match &mut bundle.surface {
            SurfaceDefinition::Task(definition) => definition.bindings.push(binding),
            SurfaceDefinition::Session(definition) => definition.bindings.push(binding),
        }
    }

    #[test]
    fn valid_bundle_passes() {
        let concept = concept("example.name", "name", ParameterType::String);
        let binding = binding(
            &concept,
            DefaultSpec::Literal {
                value: "default".into(),
            },
        );
        bundle(concept, binding).validate().unwrap();
    }

    #[test]
    fn unsupported_pattern_constraints_are_rejected() {
        let serialized = serde_json::json!({"kind": "pattern", "pattern": "^value$"});
        assert!(serde_json::from_value::<Constraint>(serialized).is_err());
    }

    #[test]
    fn aliases_cannot_shadow_canonical_parameter_names_regardless_of_order() {
        let first = concept("example.first", "first", ParameterType::String);
        let mut first_binding = binding(
            &first,
            DefaultSpec::Literal {
                value: "first".into(),
            },
        );
        first_binding.aliases.push("second".to_string());
        let mut bundle = bundle(first, first_binding);

        let second = concept("example.second", "second", ParameterType::String);
        let mut second_binding = binding(
            &second,
            DefaultSpec::Literal {
                value: "second".into(),
            },
        );
        second_binding.order = 1;
        add_binding(&mut bundle, second, second_binding);

        let errors = bundle.validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.code == "alias_canonical_collision")
        );
    }

    #[test]
    fn aliases_must_be_unique_across_the_entire_surface() {
        let first = concept("example.first", "first", ParameterType::String);
        let mut first_binding = binding(
            &first,
            DefaultSpec::Literal {
                value: "first".into(),
            },
        );
        first_binding.aliases.push("legacy".to_string());
        let mut bundle = bundle(first, first_binding);

        let second = concept("example.second", "second", ParameterType::String);
        let mut second_binding = binding(
            &second,
            DefaultSpec::Literal {
                value: "second".into(),
            },
        );
        second_binding.order = 1;
        second_binding.aliases.push("legacy".to_string());
        add_binding(&mut bundle, second, second_binding);

        let errors = bundle.validate().unwrap_err();
        assert!(errors.iter().any(|error| error.code == "duplicate_alias"));
    }

    #[test]
    fn overwrite_concepts_require_an_exact_true_predicate() {
        let concept = concept(OVERWRITE_CONCEPT_ID, "overwrite", ParameterType::Bool);
        let binding = binding(
            &concept,
            DefaultSpec::Literal {
                value: ParameterValue::Bool(false),
            },
        );
        let mut bundle = bundle(concept, binding);
        let errors = bundle.validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.code == "missing_overwrite_safety")
        );

        let SurfaceDefinition::Task(definition) = &mut bundle.surface else {
            unreachable!("test bundle is a task")
        };
        definition.safety_rules.push(TaskSafetyRule {
            class: RunSafetyClass::Overwrite,
            when: Predicate::Equals {
                parameter: "overwrite".to_string(),
                value: ParameterValue::Bool(true),
            },
        });
        bundle.validate().unwrap();
    }

    #[test]
    fn safety_rules_reject_duplicate_classes_unknown_parameters_and_bad_types() {
        let concept = concept("example.mode", "mode", ParameterType::String);
        let binding = binding(
            &concept,
            DefaultSpec::Literal {
                value: ParameterValue::String("safe".to_string()),
            },
        );
        let mut bundle = bundle(concept, binding);
        let SurfaceDefinition::Task(definition) = &mut bundle.surface else {
            unreachable!("test bundle is a task")
        };
        definition.safety_rules = vec![
            TaskSafetyRule {
                class: RunSafetyClass::InputMutation,
                when: Predicate::Equals {
                    parameter: "mode".to_string(),
                    value: ParameterValue::Bool(true),
                },
            },
            TaskSafetyRule {
                class: RunSafetyClass::ProductWrite,
                when: Predicate::IsSet {
                    parameter: "missing".to_string(),
                },
            },
            TaskSafetyRule {
                class: RunSafetyClass::ProductWrite,
                when: Predicate::Always,
            },
        ];
        let errors = bundle.validate().unwrap_err();
        for expected in [
            "invalid_safety_value",
            "unknown_safety_parameter",
            "duplicate_safety_class",
        ] {
            assert!(
                errors.iter().any(|error| error.code == expected),
                "missing {expected}: {errors:?}"
            );
        }
    }

    #[test]
    fn reserved_name_requires_designated_concept() {
        let concept = concept("wrong.imsize", "imsize", ParameterType::Integer);
        let binding = binding(
            &concept,
            DefaultSpec::Literal {
                value: 512_i64.into(),
            },
        );
        let errors = bundle(concept, binding).validate().unwrap_err();
        assert!(errors.iter().any(|error| error.code == "reserved_name"));
    }

    #[test]
    fn runtime_authority_names_are_forbidden_from_profile_bindings() {
        for name in [
            "binary",
            "binary_path",
            "executable",
            "executable_args",
            "override_env",
            "override_env_path",
            "confirm_overwrite",
            "confirmation_token",
            "no_save_last",
            "no_save_session",
            "workspace",
            "workspace_root",
            "runtime_timeout",
            "telemetry_elapsed_ms",
            "ui_selected_tab",
        ] {
            let concept = concept(&format!("example.{name}"), name, ParameterType::String);
            let binding = binding(
                &concept,
                DefaultSpec::Literal {
                    value: "value".into(),
                },
            );
            let errors = bundle(concept, binding).validate().unwrap_err();
            assert!(
                errors
                    .iter()
                    .any(|error| error.code == "forbidden_profile_parameter_name"),
                "{name}: {errors:?}"
            );
        }

        let allowed = concept(
            "example.confirmatory_model",
            "confirmatory_model",
            ParameterType::String,
        );
        let allowed_binding = binding(
            &allowed,
            DefaultSpec::Literal {
                value: "science".into(),
            },
        );
        bundle(allowed, allowed_binding).validate().unwrap();
    }

    #[test]
    fn quantity_defaults_use_real_unit_parsing_and_dimension_checks() {
        let mut quantity = concept(
            "example.angle",
            "angle",
            ParameterType::Quantity {
                dimension: UnitDimension::Angle,
                canonical_unit: "arcsec".to_string(),
                special_values: vec!["auto".to_string()],
            },
        );
        quantity.normalization = NormalizationRule::Quantity {
            canonical_unit: "arcsec".to_string(),
        };
        quantity.unit_dimension = Some(UnitDimension::Angle);

        for invalid in ["banana", "1bogus", "1s", "1e999arcsec"] {
            let binding = binding(
                &quantity,
                DefaultSpec::Literal {
                    value: invalid.into(),
                },
            );
            let errors = bundle(quantity.clone(), binding).validate().unwrap_err();
            assert!(
                errors.iter().any(|error| error.code == "invalid_default"),
                "{invalid}: {errors:?}"
            );
        }

        for valid in ["1deg", "0.5arcsec", "1", "auto"] {
            let binding = binding(
                &quantity,
                DefaultSpec::Literal {
                    value: valid.into(),
                },
            );
            bundle(quantity.clone(), binding)
                .validate()
                .unwrap_or_else(|errors| panic!("{valid}: {errors:?}"));
        }

        let pair = concept(
            "example.angle_pair",
            "angle_pair",
            ParameterType::Array {
                element: Box::new(quantity.value_domain.clone()),
                min_items: 2,
                max_items: Some(2),
                allow_scalar: true,
            },
        );
        let malformed_element = binding(
            &pair,
            DefaultSpec::Literal {
                value: ParameterValue::Array(vec!["1arcsec".into(), "banana".into()]),
            },
        );
        let errors = bundle(pair, malformed_element).validate().unwrap_err();
        assert!(errors.iter().any(|error| error.code == "invalid_default"));

        for (canonical_unit, expected_code) in [
            ("bananas", "invalid_value_domain"),
            ("s", "invalid_value_domain"),
        ] {
            let invalid_domain = concept(
                "example.invalid_angle",
                "invalid_angle",
                ParameterType::Quantity {
                    dimension: UnitDimension::Angle,
                    canonical_unit: canonical_unit.to_string(),
                    special_values: vec!["auto".to_string()],
                },
            );
            let binding = binding(
                &invalid_domain,
                DefaultSpec::Literal {
                    value: "auto".into(),
                },
            );
            let errors = bundle(invalid_domain, binding).validate().unwrap_err();
            assert!(
                errors.iter().any(|error| error.code == expected_code),
                "{canonical_unit}: {errors:?}"
            );
        }

        let unsupported_data_size = concept(
            "example.data_size",
            "data_size",
            ParameterType::Quantity {
                dimension: UnitDimension::DataSize,
                canonical_unit: String::new(),
                special_values: vec!["auto".to_string()],
            },
        );
        let binding = binding(
            &unsupported_data_size,
            DefaultSpec::Literal {
                value: "auto".into(),
            },
        );
        let errors = bundle(unsupported_data_size, binding)
            .validate()
            .unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.code == "invalid_value_domain")
        );
    }

    #[test]
    fn scalar_is_valid_for_scalar_or_array_domain() {
        let domain = ParameterType::Array {
            element: Box::new(ParameterType::Integer),
            min_items: 2,
            max_items: Some(2),
            allow_scalar: true,
        };
        assert!(validate_parameter_value(&512_i64.into(), &domain).is_ok());
        assert!(
            validate_parameter_value(
                &ParameterValue::Array(vec![512_i64.into(), 256_i64.into()]),
                &domain
            )
            .is_ok()
        );
    }

    #[test]
    fn divergent_concept_revision_is_rejected() {
        let first = concept("example.name", "name", ParameterType::String);
        let binding = binding(&first, DefaultSpec::Literal { value: "a".into() });
        let first_bundle = bundle(first.clone(), binding.clone());
        let mut second = first;
        second.documentation.summary = "different".to_string();
        let mut second_bundle = bundle(second, binding);
        if let SurfaceDefinition::Task(definition) = &mut second_bundle.surface {
            definition.id = "second".to_string();
        }
        let errors = validate_surface_bundles(&[first_bundle, second_bundle]).unwrap_err();
        assert!(errors.iter().any(|error| error.code == "divergent_concept"));
    }

    #[test]
    fn repeated_names_require_one_concept_or_an_explicit_homonym_review() {
        let primary = concept("example.a_primary", "shared", ParameterType::String);
        let primary_binding = binding(
            &primary,
            DefaultSpec::Literal {
                value: "primary".into(),
            },
        );
        let first = bundle(primary.clone(), primary_binding.clone());
        let mut same_concept = bundle(primary, primary_binding);
        set_surface_id(&mut same_concept, "same-concept");
        validate_surface_bundles(&[first.clone(), same_concept]).unwrap();

        let mut redundant = first.clone();
        set_surface_id(&mut redundant, "redundant");
        let SurfaceDefinition::Task(definition) = &mut redundant.surface else {
            unreachable!("test bundle is a task")
        };
        definition.bindings[0].reviewed_homonym = Some(ReviewedHomonym {
            review_id: "ADR-test".to_string(),
            reason: "Redundant evidence for an identical concept.".to_string(),
        });
        let errors = validate_surface_bundles(&[first.clone(), redundant]).unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.code == "redundant_homonym_review")
        );

        let alternate = concept("example.z_alternate", "shared", ParameterType::String);
        let alternate_binding = binding(
            &alternate,
            DefaultSpec::Literal {
                value: "alternate".into(),
            },
        );
        let mut unreviewed = bundle(alternate.clone(), alternate_binding.clone());
        set_surface_id(&mut unreviewed, "unreviewed");
        let errors = validate_surface_bundles(&[first.clone(), unreviewed]).unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.code == "unreviewed_homonym")
        );

        let mut reviewed = bundle(alternate, alternate_binding);
        set_surface_id(&mut reviewed, "reviewed");
        let SurfaceDefinition::Task(definition) = &mut reviewed.surface else {
            unreachable!("test bundle is a task")
        };
        definition.bindings[0].reviewed_homonym = Some(ReviewedHomonym {
            review_id: "ADR-test".to_string(),
            reason: "The shared spelling has a deliberately distinct meaning.".to_string(),
        });
        reviewed
            .validate()
            .expect("an isolated provider bundle cannot judge catalog-level homonyms");
        validate_surface_bundles(&[first, reviewed]).unwrap();
    }

    #[test]
    fn bindings_reject_broadening_refinements_and_invalid_defaults() {
        let choice = concept(
            "example.choice",
            "choice",
            ParameterType::Choice {
                values: vec!["a".to_string(), "b".to_string()],
            },
        );
        let mut broadening = binding(&choice, DefaultSpec::Literal { value: "a".into() });
        broadening.refinements = vec![NarrowingConstraint::AllowedValues {
            values: vec!["a".to_string(), "c".to_string()],
        }];
        let errors = bundle(choice.clone(), broadening).validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.code == "non_narrowing_refinement")
        );

        let mut excluded_default = binding(&choice, DefaultSpec::Literal { value: "a".into() });
        excluded_default.refinements = vec![NarrowingConstraint::AllowedValues {
            values: vec!["b".to_string()],
        }];
        let errors = bundle(choice, excluded_default).validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.code == "default_refinement")
        );
        assert!(
            errors
                .iter()
                .all(|error| error.code != "non_narrowing_refinement")
        );

        let integer = concept("example.integer", "integer", ParameterType::Integer);
        let wrong_type = binding(
            &integer,
            DefaultSpec::Literal {
                value: "not-an-integer".into(),
            },
        );
        let errors = bundle(integer, wrong_type).validate().unwrap_err();
        assert!(errors.iter().any(|error| error.code == "invalid_default"));

        let mut constrained = concept("example.constrained", "constrained", ParameterType::Integer);
        constrained.base_constraints = vec![Constraint::NumberRange {
            min: Some(0.0),
            max: Some(10.0),
            min_inclusive: true,
            max_inclusive: true,
        }];
        let outside_base_range = binding(
            &constrained,
            DefaultSpec::Literal {
                value: 11_i64.into(),
            },
        );
        let errors = bundle(constrained, outside_base_range)
            .validate()
            .unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.code == "default_constraint")
        );

        let mut pair = concept(
            "example.pair",
            "pair",
            ParameterType::Array {
                element: Box::new(ParameterType::Integer),
                min_items: 2,
                max_items: Some(2),
                allow_scalar: false,
            },
        );
        pair.base_constraints = vec![Constraint::Positive];
        let nonpositive_element = binding(
            &pair,
            DefaultSpec::Literal {
                value: ParameterValue::Array(vec![1_i64.into(), 0_i64.into()]),
            },
        );
        let errors = bundle(pair.clone(), nonpositive_element)
            .validate()
            .unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.code == "default_constraint")
        );

        let mut out_of_range_element = binding(
            &pair,
            DefaultSpec::Literal {
                value: ParameterValue::Array(vec![2_i64.into(), 0_i64.into()]),
            },
        );
        out_of_range_element.refinements = vec![NarrowingConstraint::NumberRange {
            min: Some(1.0),
            max: None,
            min_inclusive: true,
            max_inclusive: true,
        }];
        let errors = bundle(pair, out_of_range_element).validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.code == "default_refinement")
        );
        assert!(
            errors
                .iter()
                .all(|error| error.code != "non_narrowing_refinement")
        );

        let required = concept("example.required", "required", ParameterType::String);
        let conditionally_optional = binding(&required, DefaultSpec::Required);
        let errors = bundle(required, conditionally_optional)
            .validate()
            .unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.code == "missing_optional_default")
        );
    }

    #[test]
    fn selector_refinements_validate_defaults_and_capability_names() {
        let mut selector = concept("example.selector", "selector", ParameterType::String);
        selector.normalization = NormalizationRule::CasaSelector {
            grammar: SelectorGrammar::Field,
            capabilities: vec![
                "ids".into(),
                "names".into(),
                "ranges".into(),
                "wildcards".into(),
            ],
        };
        let mut wildcard_default = binding(
            &selector,
            DefaultSpec::Literal {
                value: "target*".into(),
            },
        );
        wildcard_default.refinements = vec![NarrowingConstraint::SelectorCapabilities {
            capabilities: vec!["ids".into(), "names".into(), "ranges".into()],
        }];
        let errors = bundle(selector.clone(), wildcard_default)
            .validate()
            .unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.code == "default_refinement")
        );

        let mut unknown = binding(
            &selector,
            DefaultSpec::Literal {
                value: "target".into(),
            },
        );
        unknown.refinements = vec![NarrowingConstraint::SelectorCapabilities {
            capabilities: vec!["ids".into(), "typo".into()],
        }];
        let errors = bundle(selector.clone(), unknown).validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.code == "non_narrowing_refinement")
        );

        let mut malformed = selector;
        let NormalizationRule::CasaSelector { capabilities, .. } = &mut malformed.normalization
        else {
            unreachable!()
        };
        capabilities.push("typo".into());
        let malformed_binding = binding(
            &malformed,
            DefaultSpec::Literal {
                value: "target".into(),
            },
        );
        let errors = bundle(malformed, malformed_binding).validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.code == "unknown_selector_capability")
        );
    }

    #[test]
    fn conditional_default_cycles_are_rejected() {
        let first = concept("example.a", "a", ParameterType::String);
        let second = concept("example.b", "b", ParameterType::String);
        let mut first_binding = binding(
            &first,
            DefaultSpec::Conditional {
                cases: vec![ConditionalDefault {
                    when: Predicate::IsSet {
                        parameter: "b".to_string(),
                    },
                    value: "x".into(),
                }],
                fallback: "y".into(),
            },
        );
        first_binding.order = 0;
        let mut second_binding = binding(
            &second,
            DefaultSpec::Conditional {
                cases: vec![ConditionalDefault {
                    when: Predicate::IsSet {
                        parameter: "a".to_string(),
                    },
                    value: "x".into(),
                }],
                fallback: "y".into(),
            },
        );
        second_binding.order = 1;
        let bundle = SurfaceContractBundle {
            schema_version: PARAMETER_CATALOG_SCHEMA_VERSION,
            surface: SurfaceDefinition::Task(TaskDefinition {
                id: "cycle".to_string(),
                contract_version: 1,
                display_name: "Cycle".to_string(),
                category: "Test".to_string(),
                summary: "Cycle".to_string(),
                provider_family: "test".to_string(),
                execution: SurfaceExecutionProjection::default(),
                safety_rules: Vec::new(),
                bindings: vec![first_binding, second_binding],
                migrations: Vec::new(),
            }),
            catalog: ParameterCatalog {
                schema_version: PARAMETER_CATALOG_SCHEMA_VERSION,
                concepts: vec![first, second],
            },
        };
        let errors = bundle.validate().unwrap_err();
        assert!(errors.iter().any(|error| error.code == "default_cycle"));
    }

    #[test]
    fn contract_bumps_require_a_complete_migration_chain_from_v1() {
        let concept = concept("example.name", "name", ParameterType::String);
        let binding = binding(
            &concept,
            DefaultSpec::Literal {
                value: "default".into(),
            },
        );
        let mut bundle = bundle(concept, binding);
        let SurfaceDefinition::Task(definition) = &mut bundle.surface else {
            unreachable!()
        };
        definition.contract_version = 2;
        let errors = bundle.validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.code == "missing_migrations")
        );
    }

    #[test]
    fn input_and_output_context_roles_must_match_concept_roles() {
        let concept = concept("example.path", "path", ParameterType::String);
        let mut binding = binding(
            &concept,
            DefaultSpec::Literal {
                value: "input".into(),
            },
        );
        binding.context_role = Some(ContextRole::InputDataset);
        let errors = bundle(concept, binding).validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.code == "context_role_mismatch")
        );
    }
}
