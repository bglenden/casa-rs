// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical, format-neutral parameter contracts shared by every casars UI.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use casa_core::quanta::{Quantity, Unit};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[path = "../casa_signature.rs"]
mod casa_signature;
#[path = "../surface_validation.rs"]
mod surface_validation;

use surface_validation::{SurfaceContractRef, validate_surface_views};

pub use casa_signature::{UnimplementedForms, UnimplementedParameter, python_float, python_repr};

/// Current schema version for the catalog and surface-definition model.
pub const PARAMETER_CATALOG_SCHEMA_VERSION: u32 = 1;

/// Designated semantic IDs for names whose meaning is repository-wide.
pub const IMSIZE_CONCEPT_ID: &str = "image.geometry.imsize";
pub const CELL_CONCEPT_ID: &str = "image.geometry.cell";
pub const FIELD_CONCEPT_ID: &str = "ms.selection.field";
pub const SPW_CONCEPT_ID: &str = "ms.selection.spw";
pub const OVERWRITE_CONCEPT_ID: &str = "output.overwrite";

/// The CASA tasks that replace existing output products by default.
/// Other task surfaces require explicit overwrite permission.
pub const CASA_OVERWRITE_BY_DEFAULT_TASKS: [&str; 3] = ["simobserve", "simanalyze", "feather"];

mod validation;
mod value_validation;

pub use validation::ContractValidationError;
pub use validation::validate_surface_bundles;
use validation::*;
pub use value_validation::validate_parameter_value;
use value_validation::*;

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

/// Element type for an empty array passed through a task parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EmptyArrayType {
    Bool,
    Integer,
    Float,
    Complex,
    String,
}

/// A NumPy scalar type whose conversion can depend on its surrounding value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NumpyScalarType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    #[serde(rename = "uint8")]
    UInt8,
    #[serde(rename = "uint16")]
    UInt16,
    #[serde(rename = "uint32")]
    UInt32,
    #[serde(rename = "uint64")]
    UInt64,
    Float32,
    Complex64,
    Complex128,
}

/// Typed parameter value used before provider adaptation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ParameterValue {
    Bool(bool),
    Integer(i64),
    Float(
        #[serde(with = "parameter_float_serde")]
        #[schemars(with = "crate::CasaFloat")]
        f64,
    ),
    /// A complex scalar, with real and imaginary components.
    Complex([crate::CasaFloat; 2]),
    /// An empty NumPy array, retaining its dimensions and element type.
    EmptyArray {
        dtype: EmptyArrayType,
        shape: Vec<usize>,
    },
    /// A zero-dimensional NumPy array, retaining its scalar value.
    ScalarArray(Box<ParameterValue>),
    /// A NumPy scalar, including its native dtype and scalar value.
    NumpyScalar {
        dtype: NumpyScalarType,
        value: Box<ParameterValue>,
    },
    String(String),
    Array(Vec<ParameterValue>),
    Table(BTreeMap<String, ParameterValue>),
}

mod parameter_float_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
        crate::CasaFloat(*value).serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
        crate::CasaFloat::deserialize(deserializer).map(|value| value.0)
    }
}

impl ParameterValue {
    /// The value as plain JSON, as requests and profiles spell it: `"none"`,
    /// `512`, `["0.5arcsec", "0.5arcsec"]`. Non-finite floats become
    /// `null`; use typed transport when their identity must be preserved.
    #[must_use]
    pub fn to_plain_json(&self) -> serde_json::Value {
        use serde_json::Value;
        match self {
            Self::Bool(value) => Value::Bool(*value),
            Self::Integer(value) => Value::from(*value),
            Self::Float(value) => {
                serde_json::Number::from_f64(*value).map_or(Value::Null, Value::Number)
            }
            Self::Complex(value) => {
                serde_json::to_value(value).expect("complex components are serializable")
            }
            Self::EmptyArray { .. } => Value::Array(Vec::new()),
            Self::ScalarArray(value) => value.to_plain_json(),
            Self::NumpyScalar { value, .. } => value.to_plain_json(),
            Self::String(value) => Value::String(value.clone()),
            Self::Array(values) => Value::Array(values.iter().map(Self::to_plain_json).collect()),
            Self::Table(values) => Value::Object(
                values
                    .iter()
                    .map(|(name, value)| (name.clone(), value.to_plain_json()))
                    .collect(),
            ),
        }
    }

    /// Whether this value requires typed rather than plain JSON transport.
    pub fn requires_typed_transport(&self) -> bool {
        match self {
            Self::Complex(_)
            | Self::EmptyArray { .. }
            | Self::ScalarArray(_)
            | Self::NumpyScalar { .. } => true,
            Self::Float(value) if !value.is_finite() => true,
            Self::Array(values) => values.iter().any(Self::requires_typed_transport),
            Self::Table(values) => values.values().any(Self::requires_typed_transport),
            _ => false,
        }
    }

    /// Maximum array/record nesting, for bounding typed transport depth.
    pub fn nesting_depth(&self) -> usize {
        match self {
            Self::Array(values) => 1 + values.iter().map(Self::nesting_depth).max().unwrap_or(0),
            Self::Table(values) => 1 + values.values().map(Self::nesting_depth).max().unwrap_or(0),
            Self::ScalarArray(value) => 1 + value.nesting_depth(),
            Self::NumpyScalar { value, .. } => 1 + value.nesting_depth(),
            _ => 0,
        }
    }

    /// A plain JSON value as a typed one: integers stay integers, other
    /// numbers become floats, objects tables. `null`, here or nested, has
    /// no typed value.
    #[must_use]
    pub fn from_plain_json(value: &serde_json::Value) -> Option<Self> {
        use serde_json::Value;
        Some(match value {
            Value::Null => return None,
            Value::Bool(value) => Self::Bool(*value),
            Value::Number(number) => match number.as_i64() {
                Some(value) => Self::Integer(value),
                None => Self::Float(number.as_f64()?),
            },
            Value::String(value) => Self::String(value.clone()),
            Value::Array(values) => Self::Array(
                values
                    .iter()
                    .map(Self::from_plain_json)
                    .collect::<Option<_>>()?,
            ),
            Value::Object(values) => Self::Table(
                values
                    .iter()
                    .map(|(name, value)| Some((name.clone(), Self::from_plain_json(value)?)))
                    .collect::<Option<_>>()?,
            ),
        })
    }
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
    /// A CASA variant: any finite scalar, list, or string-keyed record.
    Variant,
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

/// The token a [`NormalizationRule::ProjectPath`] value may hold for the
/// task's project.
pub const PROJECT_TOKEN: &str = "$project";

/// Invariant canonicalization and semantic-equality rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NormalizationRule {
    Identity,
    /// CASA numeric parameters accept Python booleans as integer 0/1,
    /// including within ordinary parameter lists (but not NumPy bool scalars).
    BooleanAsInteger,
    /// Preserve tclean FIELD text, including literal dollar signs.
    TcleanField,
    /// CASA selector expressions are data parsed natively; preserve every
    /// literal character, including quoted names and regex dollar anchors.
    CasaField,
    /// Preserve an archive URL or remote identifier passed as service data.
    ServiceReference,
    /// A CASA variant whose CLI spelling may be a bare number or a string.
    NumericVariant,
    Trim,
    Lowercase,
    Path,
    /// A path that may name the task's project as [`PROJECT_TOKEN`], which
    /// the task replaces (CASA's `simobserve` names `$project.ptg.txt`):
    /// cleaned as a [`Path`](Self::Path), the token kept. Any other `$` or
    /// backtick in the path is refused.
    ProjectPath,
    CasaSelector {
        grammar: SelectorGrammar,
        #[serde(default)]
        capabilities: Vec<String>,
    },
    Quantity {
        canonical_unit: String,
    },
    /// A number given for a text value is read as its text, as CASA's
    /// variant parameters take one: `tclean`'s `phasecenter=3` names field
    /// 3 and `width=2` two channels (#931).
    NumberAsText,
    /// Canonicalize integer window maps while keeping other variants for
    /// applycal's inactive-map and deferred parameter handling.
    ApplySpwMap,
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
        ParameterValue::ScalarArray(value) => collect_selector_capabilities(grammar, value, used),
        ParameterValue::NumpyScalar { value, .. } => {
            collect_selector_capabilities(grammar, value, used)
        }
        ParameterValue::Bool(_)
        | ParameterValue::Integer(_)
        | ParameterValue::Float(_)
        | ParameterValue::Complex(_)
        | ParameterValue::EmptyArray { .. } => {}
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
    /// A casa-rs-only control on a surface that mimics a CASA task: it
    /// follows CASA's parameters as a keyword-only argument (#931).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub keyword_only: bool,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewed_homonym: Option<ReviewedHomonym>,
    /// Set for a CASA parameter casa-rs accepts only at its default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unimplemented: Option<UnimplementedParameter>,
    /// Set for a CASA parameter whose other CASA forms casa-rs does not
    /// take yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unimplemented_forms: Option<UnimplementedForms>,
}

/// Profile kind pinned in the TOML header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SurfaceKind {
    Task,
}

impl fmt::Display for SurfaceKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Task => "task",
        })
    }
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
    #[serde(rename = "calibration-report-v1")]
    CalibrationReportV1,
    #[serde(rename = "imager-run-v1")]
    ImagerRunV1,
    #[serde(rename = "immoments-run-v1")]
    ImmomentsRunV1,
    #[serde(rename = "plotms-run-v1")]
    PlotmsRunV1,
    #[serde(rename = "predictcomp-run-v1")]
    PredictcompRunV1,
    #[serde(rename = "simobserve-run-v1")]
    SimobserveRunV1,
}

/// Typed contract for a provider's structured task result: the result a
/// front-end binary's reply carries, and the decoder that reads its
/// products and summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ManagedOutputContract {
    pub decoder: ManagedResultDecoder,
    pub stdout_format: ManagedOutputFormat,
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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SurfaceProductContract {
    #[default]
    NoProducts,
    Declared {
        products: Vec<RunProductDescriptor>,
    },
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

/// One-shot surface definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TaskDefinition {
    pub id: String,
    pub contract_version: u32,
    pub display_name: String,
    pub category: String,
    pub summary: String,
    pub provider_family: String,
    /// The CASA task this surface mimics, whose signature it presents
    /// (#931); `None` for a surface not yet CASA-exact or a casa-rs task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub casa_task: Option<String>,
    #[serde(default)]
    pub execution: SurfaceExecutionProjection,
    pub bindings: Vec<SurfaceParameterBinding>,
}

/// Shared definition for every configurable executable surface. Every surface
/// is a one-shot task; the `kind` tag stays in the serialized contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SurfaceDefinition {
    Task(TaskDefinition),
}

impl SurfaceDefinition {
    fn task(&self) -> &TaskDefinition {
        let Self::Task(definition) = self;
        definition
    }

    pub fn id(&self) -> &str {
        &self.task().id
    }

    pub fn kind(&self) -> SurfaceKind {
        SurfaceKind::Task
    }

    pub fn contract_version(&self) -> u32 {
        self.task().contract_version
    }

    pub fn provider_family(&self) -> &str {
        &self.task().provider_family
    }

    /// The CASA task the surface mimics, when it presents CASA's signature.
    pub fn casa_task(&self) -> Option<&str> {
        self.task().casa_task.as_deref()
    }

    pub fn bindings(&self) -> &[SurfaceParameterBinding] {
        &self.task().bindings
    }

    pub fn execution(&self) -> &SurfaceExecutionProjection {
        &self.task().execution
    }

    pub fn display_name(&self) -> &str {
        &self.task().display_name
    }

    pub fn category(&self) -> &str {
        &self.task().category
    }

    pub fn summary(&self) -> &str {
        &self.task().summary
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
    /// The surface `id` names, now or, for a saved profile's header, before
    /// it was renamed.
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
        let bundles: Vec<_> = self
            .surfaces
            .iter()
            .map(|surface| SurfaceContractRef {
                schema_version: self.schema_version,
                surface,
                catalog: &self.catalog,
            })
            .collect();
        validate_surface_views(&bundles, true)
    }
}

#[cfg(test)]
mod tests;
