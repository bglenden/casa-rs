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
/// Canonical concept ID for angular cell size on an image grid.
pub const CELL_CONCEPT_ID: &str = "image.geometry.cell";
/// Canonical concept ID for selecting MeasurementSet fields.
pub const FIELD_CONCEPT_ID: &str = "ms.selection.field";
/// Canonical concept ID for selecting spectral windows and channels.
pub const SPW_CONCEPT_ID: &str = "ms.selection.spw";
/// Canonical concept ID for permission to replace existing outputs.
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
    /// Wrap a canonical concept identifier. Catalog validation checks the spelling.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the stable identifier used in contract references.
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
    /// Canonical concept identifier.
    pub id: ParameterConceptId,
    /// Required semantic revision in the containing catalog.
    pub semantic_revision: SemanticRevision,
}

impl ParameterConceptRef {
    /// Pin a concept identifier and semantic revision for a surface binding.
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
    /// Boolean elements.
    Bool,
    /// Integer elements.
    Integer,
    /// Real floating-point elements.
    Float,
    /// Complex floating-point elements.
    Complex,
    /// Text elements.
    String,
}

/// A NumPy scalar type whose conversion can depend on its surrounding value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NumpyScalarType {
    /// NumPy Boolean scalar.
    Bool,
    /// Signed 8-bit integer.
    Int8,
    /// Signed 16-bit integer.
    Int16,
    /// Signed 32-bit integer.
    Int32,
    /// Signed 64-bit integer.
    Int64,
    #[serde(rename = "uint8")]
    /// Unsigned 8-bit integer.
    UInt8,
    #[serde(rename = "uint16")]
    /// Unsigned 16-bit integer.
    UInt16,
    #[serde(rename = "uint32")]
    /// Unsigned 32-bit integer.
    UInt32,
    #[serde(rename = "uint64")]
    /// Unsigned 64-bit integer.
    UInt64,
    /// Single-precision real scalar.
    Float32,
    /// Complex scalar with two single-precision components.
    Complex64,
    /// Complex scalar with two double-precision components.
    Complex128,
}

/// Typed parameter value used before provider adaptation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ParameterValue {
    /// Boolean parameter value.
    Bool(bool),
    /// Signed 64-bit integer parameter value.
    Integer(i64),
    /// Double-precision parameter value, including typed non-finite values.
    Float(
        #[serde(with = "parameter_float_serde")]
        #[schemars(with = "crate::CasaFloat")]
        f64,
    ),
    /// A complex scalar, with real and imaginary components.
    Complex([crate::CasaFloat; 2]),
    /// An empty NumPy array, retaining its dimensions and element type.
    EmptyArray {
        /// Original element or scalar dtype, retained for provider adaptation.
        dtype: EmptyArrayType,
        /// Original zero-length array dimensions in array-axis order.
        shape: Vec<usize>,
    },
    /// A zero-dimensional NumPy array, retaining its scalar value.
    ScalarArray(Box<ParameterValue>),
    /// A NumPy scalar, including its native dtype and scalar value.
    NumpyScalar {
        /// Original element or scalar dtype, retained for provider adaptation.
        dtype: NumpyScalarType,
        /// Scalar content with its ordinary typed parameter representation.
        value: Box<ParameterValue>,
    },
    /// UTF-8 text parameter value.
    String(String),
    /// Ordered parameter elements, which may themselves be arrays or records.
    Array(Vec<ParameterValue>),
    /// Named values of a parameter record, stored in key order.
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
    /// A path without a domain-specific resource requirement.
    Any,
    /// A regular file.
    File,
    /// A filesystem directory.
    Directory,
    /// A radio-astronomy MeasurementSet.
    MeasurementSet,
    /// A CASA image resource.
    Image,
    /// A casacore table.
    Table,
    /// A calibration solution table.
    CalibrationTable,
    /// A calibration or observation archive supplied as input.
    Archive,
    /// A path to a task-produced resource.
    Product,
}

/// Physical dimension of a quantity parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum UnitDimension {
    /// Pure number with no physical unit.
    Dimensionless,
    /// Plane angle.
    Angle,
    /// Time interval or epoch quantity.
    Time,
    /// Frequency in inverse seconds.
    Frequency,
    /// Length per time.
    Velocity,
    /// Physical distance.
    Length,
    /// Spectral flux density, such as Jy.
    FluxDensity,
    /// Spectral flux density per restoring beam.
    FluxDensityPerBeam,
    /// Thermodynamic or brightness temperature.
    Temperature,
    /// Quantity of stored data, such as bytes.
    DataSize,
    /// A domain-specific physical dimension defined by the concept.
    Custom,
}

/// Accepted semantic value domain.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ParameterType {
    /// A CASA variant: any finite scalar, list, or string-keyed record.
    Variant,
    /// Boolean scalar.
    Bool,
    /// Signed integer scalar.
    Integer,
    /// Real numeric scalar.
    Float,
    /// Text scalar.
    String,
    /// A path with an optional resource-kind hint.
    Path {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Resource role attached to the path, if defined.
        resource_kind: Option<ResourceKind>,
    },
    /// One of a fixed set of text tokens.
    Choice {
        /// Accepted spelling for each enumerated choice.
        values: Vec<String>,
    },
    /// A physical quantity with a dimension and canonical unit.
    Quantity {
        /// Physical dimension required by the concept.
        dimension: UnitDimension,
        /// Unit used to store and compare normalized quantities.
        canonical_unit: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        /// Accepted text tokens that bypass numeric quantity normalization.
        special_values: Vec<String>,
    },
    /// A homogeneous sequence with length restrictions.
    Array {
        /// Allowed domain of each sequence element.
        element: Box<ParameterType>,
        /// Minimum number of elements, inclusive.
        min_items: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Optional maximum number of elements, inclusive.
        max_items: Option<usize>,
        #[serde(default)]
        /// Accept one element directly as an alternative to a sequence.
        allow_scalar: bool,
    },
    /// A record whose field names and value domains are declared.
    Table {
        /// Value domains keyed by record field name.
        fields: BTreeMap<String, ParameterType>,
    },
    /// Explicit non-value states such as `auto` or `none` plus a typed value.
    Optional {
        /// Domain of the actual value when no non-value state is selected.
        value: Box<ParameterType>,
        /// Text spellings of the permitted non-value states.
        states: Vec<String>,
    },
}

/// CASA selector grammar whose parser/normalization is catalog-owned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SelectorGrammar {
    /// MeasurementSet field IDs, names, and expressions.
    Field,
    /// Spectral-window and channel selection.
    SpectralWindow,
    /// MeasurementSet observation-time expressions.
    TimeRange,
    /// Baseline-length selection in supported physical or wavelength units.
    UvRange,
    /// Antenna and baseline selection.
    Antenna,
    /// MeasurementSet scan selection.
    Scan,
    /// Correlation-product selection.
    Correlation,
    /// MeasurementSet array IDs.
    Array,
    /// Observation ID selection.
    Observation,
    /// Observing-intent selection.
    Intent,
    /// Receptor feed selection.
    Feed,
    /// CASA visibility-selection expression.
    MsSelect,
    /// Rectangular image pixel region.
    ImageBox,
    /// Spectral pixel or channel selection.
    ImageChannels,
    /// Named or serialized image region.
    ImageRegion,
    /// Stokes plane or polarization selection.
    Stokes,
}

/// The token a [`NormalizationRule::ProjectPath`] value may hold for the
/// task's project.
pub const PROJECT_TOKEN: &str = "$project";

/// Invariant canonicalization and semantic-equality rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NormalizationRule {
    /// Preserve the incoming parameter representation.
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
    /// Remove surrounding whitespace from text values.
    Trim,
    /// Normalize text spelling to lowercase.
    Lowercase,
    /// Normalize a filesystem path and reject shell expansion syntax.
    Path,
    /// A path that may name the task's project as [`PROJECT_TOKEN`], which
    /// the task replaces (CASA's `simobserve` names `$project.ptg.txt`):
    /// cleaned as a [`Path`](Self::Path), the token kept. Any other `$` or
    /// backtick in the path is refused.
    ProjectPath,
    /// Canonical text for the specified CASA selector grammar.
    CasaSelector {
        /// Selector grammar governing interpretation of the text.
        grammar: SelectorGrammar,
        #[serde(default)]
        /// Allowed lexical forms such as IDs, names, ranges, or wildcards.
        capabilities: Vec<String>,
    },
    /// Convert a physical value to the specified canonical unit.
    Quantity {
        /// Unit in which successive comparisons interpret the value.
        canonical_unit: String,
    },
    /// A number given for a text value is read as its text, as CASA's
    /// variant parameters take one: `tclean`'s `phasecenter=3` names field
    /// 3 and `width=2` two channels (#931).
    NumberAsText,
    /// Canonicalize integer window maps while keeping other variants for
    /// applycal's inactive-map and deferred parameter handling.
    ApplySpwMap,
    /// Apply multiple normalization rules in declared order.
    Sequence {
        /// Normalization stages executed from first to last.
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
    /// Reject numeric values that are NaN or infinite.
    Finite,
    /// Require text or a collection to contain at least one item.
    NonEmpty,
    /// Require a numeric value greater than zero.
    Positive,
    /// Restrict a numeric value to specified bounds.
    NumberRange {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Lower bound of the range. A numeric range can omit it.
        min: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Upper bound of the range if the constraint sets one.
        max: Option<f64>,
        #[serde(default = "default_true")]
        /// Accept a numeric value equal to the lower bound.
        min_inclusive: bool,
        #[serde(default = "default_true")]
        /// Accept a numeric value equal to the upper bound.
        max_inclusive: bool,
    },
    /// Restrict the number of elements in a collection.
    Length {
        /// Lower bound of the range. A numeric range can omit it.
        min: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Upper bound of the range if the constraint sets one.
        max: Option<usize>,
    },
    /// Restrict strings to a declared set.
    AllowedValues {
        /// Allowed text values after normalization.
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
    /// A source data resource.
    InputData,
    /// A destination data resource.
    OutputData,
    /// A subset of source data to process.
    Selection,
    /// An axis, coordinate frame, or pixel geometry.
    Geometry,
    /// A magnitude with physical units or dimensions.
    PhysicalQuantity,
    /// A solver, algorithm, or associated numeric control.
    Algorithm,
    /// A display or reporting choice.
    Presentation,
    /// Publication policy such as replacement permission.
    OutputPolicy,
}

/// Whether a concept may enter a human parameter profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PersistenceClass {
    /// An authored value that can be saved in a parameter profile.
    Profile,
    /// A context suggestion outside persistent profile intent.
    SuggestionOnly,
    /// A control scoped to the current operation.
    RuntimeControl,
    /// Explicit authorization for an operation that requires confirmation.
    Confirmation,
    /// An ephemeral value that is never saved in a profile.
    Transient,
}

/// Shared documentation attached to one invariant concept.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ParameterDocs {
    /// Short explanation used in generated reference and help.
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Additional meaning, units, or error and validation information.
    pub details: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// Representative valid parameter spellings.
    pub examples: Vec<String>,
}

/// Authoritative invariant parameter concept.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ParameterConcept {
    /// Stable semantic identity shared by compatible surfaces.
    pub id: ParameterConceptId,
    /// Revision of the invariant value and interpretation rules.
    pub semantic_revision: SemanticRevision,
    /// Canonical CASA spelling where applicable.
    pub casa_name: String,
    /// Accepted types and semantic values before surface refinements.
    pub value_domain: ParameterType,
    /// Invariant canonicalization and comparison rules.
    pub normalization: NormalizationRule,
    #[serde(default)]
    /// Restrictions all bindings of this concept must satisfy.
    pub base_constraints: Vec<Constraint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Expected physical dimension when the concept defines one.
    pub unit_dimension: Option<UnitDimension>,
    /// Invariant role of the parameter across all surfaces.
    pub semantic_role: ParameterRole,
    /// Concept-level description used by generated references.
    pub documentation: ParameterDocs,
    /// Whether and how this value enters stored profiles.
    pub persistence_class: PersistenceClass,
}

impl ParameterConcept {
    /// Return this concept's identifier together with its current revision.
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
    /// Serialization format version of the concept catalog.
    pub schema_version: u32,
    /// Checked collection of the repository's invariant concepts.
    pub concepts: Vec<ParameterConcept>,
}

impl ParameterCatalog {
    /// Look up a concept by identifier and exact semantic revision.
    pub fn concept(&self, reference: &ParameterConceptRef) -> Option<&ParameterConcept> {
        self.concepts.iter().find(|concept| {
            concept.id == reference.id && concept.semantic_revision == reference.semantic_revision
        })
    }

    /// Look up the canonical concept by identifier.
    pub fn concept_by_id(&self, id: &ParameterConceptId) -> Option<&ParameterConcept> {
        self.concepts.iter().find(|concept| &concept.id == id)
    }
}

/// Pure predicate over resolved parameter values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Predicate {
    /// Predicate satisfied for every set of resolved values.
    Always,
    /// Predicate satisfied for no set of resolved values.
    Never,
    /// Test whether the referenced parameter has a value.
    IsSet {
        /// Name of the other parameter in the same surface.
        parameter: String,
    },
    /// Test whether the resolved value equals a typed value.
    Equals {
        /// Name of the other parameter in the same surface.
        parameter: String,
        /// Typed value used for the equality comparison.
        value: ParameterValue,
    },
    /// Negate the nested predicate.
    Not {
        /// Predicate whose result is inverted.
        predicate: Box<Predicate>,
    },
    /// Require every nested predicate to hold.
    All {
        /// Predicates combined by the Boolean operation.
        predicates: Vec<Predicate>,
    },
    /// Require at least one nested predicate to hold.
    Any {
        /// Predicates combined by the Boolean operation.
        predicates: Vec<Predicate>,
    },
}

impl Predicate {
    /// Add the names examined by this predicate to the supplied set.
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
    /// Predicate that selects this default branch.
    pub when: Predicate,
    /// Value used when this branch is selected.
    pub value: ParameterValue,
}

/// Surface-owned default selection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DefaultSpec {
    /// Caller must supply a value because the surface has no default.
    Required,
    /// Always use the same default value.
    Literal {
        /// Literal default value in the binding's domain.
        value: ParameterValue,
    },
    /// Select a default based on another resolved parameter.
    Conditional {
        /// Branches examined in declared order.
        cases: Vec<ConditionalDefault>,
        /// Default when no branch predicate holds.
        fallback: ParameterValue,
    },
}

/// Binding-level constraint which must be a subset of the concept domain.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NarrowingConstraint {
    /// Narrow the concept's allowed numeric interval.
    NumberRange {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Lower bound of the range. A numeric range can omit it.
        min: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Upper bound of the range if the constraint sets one.
        max: Option<f64>,
        #[serde(default = "default_true")]
        /// Accept a numeric value equal to the lower bound.
        min_inclusive: bool,
        #[serde(default = "default_true")]
        /// Accept a numeric value equal to the upper bound.
        max_inclusive: bool,
    },
    /// Narrow the allowed number of elements.
    Length {
        /// Lower bound of the range. A numeric range can omit it.
        min: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Upper bound of the range if the constraint sets one.
        max: Option<usize>,
    },
    /// Allow only a subset of the concept's text values.
    AllowedValues {
        /// Allowed text values after normalization.
        values: Vec<String>,
    },
    /// Restrict the lexical forms a selector may use on this surface.
    SelectorCapabilities {
        /// Allowed subset of the concept's selector capabilities.
        capabilities: Vec<String>,
    },
}

/// Surface-specific role for project context and suggestions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ContextRole {
    /// Suggest an available input dataset.
    InputDataset,
    /// Suggest a destination resource for a declared product.
    OutputProduct,
    /// Suggest fields from the selected MeasurementSet.
    FieldSelection,
    /// Suggest spectral windows or channels from the selected MeasurementSet.
    SpectralWindowSelection,
    /// Suggest selections related to an image input.
    ImageSelection,
    /// Suggest selections related to a table input.
    TableSelection,
    /// Suggest a previously defined image region.
    RegionReference,
    /// Suggest a named validity or processing mask.
    MaskReference,
    /// Look up a project context bookmark.
    Bookmark,
    /// Treat the value as a presentation choice.
    Presentation,
}

/// CLI projection for one canonical CASA-named parameter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CliParameterProjection {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Zero-based order when the parameter has positional CLI syntax.
    pub positional: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// Flag spellings that set or accept the parameter value.
    pub flags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// Flag spellings that explicitly set a Boolean value to false.
    pub false_flags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Parameter placeholder shown after flags in command-line help.
    pub metavar: Option<String>,
}

/// Provider-private field mapping; never a public parameter identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ProviderParameterProjection {
    /// Private typed field name within the provider's request.
    pub field: String,
    #[serde(default)]
    /// Conversion from the canonical parameter value to that field.
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
    /// Pass the typed value through without adaptation.
    Identity,
    /// Omit the explicit none state from the provider request.
    OmitNone,
    /// Format a scalar or pair in the provider's comma-separated spelling.
    ScalarOrPairCsv,
    /// Pass the numeric component of a canonical quantity.
    QuantityNumber,
    /// Pass a quantity and unit as provider text.
    QuantityString,
    /// Join text elements into the provider's comma-separated input.
    StringListCsv,
}

/// Generated Python spelling/typing metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PythonParameterProjection {
    /// Spelling used in the generated Python signature.
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional annotation expression for the generated signature.
    pub type_hint: Option<String>,
    /// A casa-rs-only control on a surface that mimics a CASA task: it
    /// follows CASA's parameters as a keyword-only argument (#931).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub keyword_only: bool,
}

/// Presentation hints; they do not define meaning or defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ParameterPresentation {
    /// Label shown alongside the control or reference entry.
    pub label: String,
    /// Name of the presentation group.
    pub group: String,
    #[serde(default)]
    /// Mark the parameter as an advanced option.
    pub advanced: bool,
    #[serde(default)]
    /// Omit the parameter from ordinary presentation.
    pub hidden: bool,
}

/// Projections derived from a semantic binding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ParameterProjections {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Command-line spelling and position, when the surface has a CLI mapping.
    pub cli: Option<CliParameterProjection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Private provider field mapping, when the task forwards this value.
    pub provider: Option<ProviderParameterProjection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Name and annotation for the generated Python signature.
    pub python: Option<PythonParameterProjection>,
    /// Display hints shared by the task's frontends.
    pub presentation: ParameterPresentation,
}

/// Required review record for a spelling that intentionally has another meaning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReviewedHomonym {
    /// Identifier of the decision recording the distinct meaning.
    pub review_id: String,
    /// Explanation of the intentionally different semantics.
    pub reason: String,
}

/// One surface-specific use of an invariant concept.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SurfaceParameterBinding {
    /// Parameter spelling on the specific surface.
    pub name: String,
    /// Identifier and revision of the invariant concept being used.
    pub concept: ParameterConceptRef,
    /// Declaration order in the generated signature and help.
    pub order: usize,
    /// Surface's explicit default or requirement for the caller to supply a value.
    pub default: DefaultSpec,
    /// Predicate under which the caller must supply this parameter.
    pub required_when: Predicate,
    /// Predicate under which the parameter participates in the operation.
    pub active_when: Predicate,
    #[serde(default)]
    /// Restrictions that narrow the invariant concept's allowed values.
    pub refinements: Vec<NarrowingConstraint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional role for deriving contextual suggestions.
    pub context_role: Option<ContextRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Additional guidance specific to this surface.
    pub surface_note: Option<String>,
    /// Mapping to CLI, Python, the provider, and presentation.
    pub projections: ParameterProjections,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Review record when this spelling differs semantically from a recognized concept.
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
    /// One-shot task with a provider result.
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
    /// Structured JSON written by the managed CLI route.
    Json,
}

/// Rust-owned decoder for one structured task result family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ManagedResultDecoder {
    #[serde(rename = "calibration-report-v1")]
    /// Calibration result and declared output-table artifacts.
    CalibrationReportV1,
    #[serde(rename = "imager-run-v1")]
    /// Synthesis imaging run summary and declared image products.
    ImagerRunV1,
    #[serde(rename = "immoments-run-v1")]
    /// Image-moments result and its declared image products.
    ImmomentsRunV1,
    #[serde(rename = "plotms-run-v1")]
    /// PlotMS output and plot artifacts.
    PlotmsRunV1,
    #[serde(rename = "predictcomp-run-v1")]
    /// Predicted sky components and associated products.
    PredictcompRunV1,
    #[serde(rename = "simobserve-run-v1")]
    /// Simulation MeasurementSet and other declared artifacts.
    SimobserveRunV1,
}

/// Typed contract for a provider's structured task result: the result a
/// front-end binary's reply carries, and the decoder that reads its
/// products and summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ManagedOutputContract {
    /// Decoder for the structured task reply family.
    pub decoder: ManagedResultDecoder,
    /// Serialization format of the managed CLI reply.
    pub stdout_format: ManagedOutputFormat,
    /// Whether a CLI consumer can access the captured stdout text.
    pub raw_stdout_available: bool,
    /// Whether a CLI consumer can access the captured diagnostic stream.
    pub raw_stderr_available: bool,
}

/// Stable semantic role of a produced task resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunProductRole {
    /// Principal result of the task.
    Primary,
    /// Secondary data needed to inspect or use the principal result.
    Auxiliary,
    /// A rendered preview of the result.
    Preview,
}

/// Declared resource kind before an exact domain probe supplies dataset metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunProductKind {
    /// Radio-astronomy visibility or total-power dataset.
    MeasurementSet,
    /// CASA-compatible image.
    CasaImage,
    /// Casacore table outside the more specific product kinds.
    CasaTable,
    /// Image stored in FITS format.
    FitsImage,
    /// An ordinary file.
    File,
}

/// Cardinality of one declared product binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunProductCardinality {
    /// At most one resource is emitted for the descriptor.
    One,
    /// The descriptor may emit multiple resources.
    Many,
}

/// Exact source of a product path. Consumers never search arbitrary result keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunProductSource {
    /// Obtain the declared output path from a resolved task parameter.
    Parameter {
        /// Name of the parameter holding the destination path.
        parameter: String,
    },
    /// Use explicitly decoded artifact paths from the managed reply.
    DecodedArtifacts,
}

/// One product descriptor owned by the canonical surface contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RunProductDescriptor {
    /// Stable identifier used to classify a declared product.
    pub id: String,
    /// Semantic role of this product in the result.
    pub role: RunProductRole,
    /// Expected resource format before any filesystem probe.
    pub resource_kind: RunProductKind,
    /// Contractual source of the path.
    pub source: RunProductSource,
    /// Number of paths the descriptor may resolve.
    pub cardinality: RunProductCardinality,
    /// A successful invocation may omit this product.
    pub optional: bool,
    /// A domain probe can enrich the product's metadata.
    pub probe_supported: bool,
}

/// Explicit product classification for every surface.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SurfaceProductContract {
    #[default]
    /// Surface has no declared filesystem products.
    NoProducts,
    /// Surface explicitly enumerates its product descriptors.
    Declared {
        /// Product descriptors that consumers may project from the result.
        products: Vec<RunProductDescriptor>,
    },
}

impl SurfaceProductContract {
    /// Borrow the declared descriptors, or an empty slice for a product-free task.
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
    /// Task surface name supplied to the provider.
    pub invocation_name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// Fixed routing arguments preceding projected user parameters.
    pub fixed_args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Structured CLI result contract when the task declares one.
    pub managed_output: Option<ManagedOutputContract>,
    #[serde(default)]
    /// Exact declaration of filesystem products.
    pub products: SurfaceProductContract,
}

/// Fully projected provider invocation, including an optional private stdin
/// payload for providers whose typed request cannot be represented as flags.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ProviderInvocation {
    #[serde(default)]
    /// Arguments mapped from the canonical task parameters.
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional provider-private typed request payload.
    pub stdin: Option<String>,
}

/// One-shot surface definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TaskDefinition {
    /// Stable serialized task surface identifier.
    pub id: String,
    /// Current version of the surface-specific parameter contract.
    pub contract_version: u32,
    /// Human-readable task name used in help and reference pages.
    pub display_name: String,
    /// Category that groups related task surfaces in presentation.
    pub category: String,
    /// Short description of the task outcome.
    pub summary: String,
    /// Family that supplies the native task implementation.
    pub provider_family: String,
    /// The CASA task this surface mimics, whose signature it presents
    /// (#931); `None` for a surface not yet CASA-exact or a casa-rs task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub casa_task: Option<String>,
    #[serde(default)]
    /// Routing and result projection for the task.
    pub execution: SurfaceExecutionProjection,
    /// Surface parameter bindings in declared order.
    pub bindings: Vec<SurfaceParameterBinding>,
}

/// Shared definition for every configurable executable surface. Every surface
/// is a one-shot task; the `kind` tag stays in the serialized contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SurfaceDefinition {
    /// Definition of a one-shot task surface.
    Task(TaskDefinition),
}

impl SurfaceDefinition {
    fn task(&self) -> &TaskDefinition {
        let Self::Task(definition) = self;
        definition
    }

    /// Return the stable task surface identifier.
    pub fn id(&self) -> &str {
        &self.task().id
    }

    /// Return the kind used in serialized profile headers.
    pub fn kind(&self) -> SurfaceKind {
        SurfaceKind::Task
    }

    /// Return the current version accepted for this task's profile.
    pub fn contract_version(&self) -> u32 {
        self.task().contract_version
    }

    /// Return the family providing the task implementation.
    pub fn provider_family(&self) -> &str {
        &self.task().provider_family
    }

    /// The CASA task the surface mimics, when it presents CASA's signature.
    pub fn casa_task(&self) -> Option<&str> {
        self.task().casa_task.as_deref()
    }

    /// Borrow the parameter bindings in declared order.
    pub fn bindings(&self) -> &[SurfaceParameterBinding] {
        &self.task().bindings
    }

    /// Borrow the task's routing and result projection.
    pub fn execution(&self) -> &SurfaceExecutionProjection {
        &self.task().execution
    }

    /// Return the human-readable task name.
    pub fn display_name(&self) -> &str {
        &self.task().display_name
    }

    /// Return the task's presentation category.
    pub fn category(&self) -> &str {
        &self.task().category
    }

    /// Return the task's short help text.
    pub fn summary(&self) -> &str {
        &self.task().summary
    }
}

/// Self-contained contract embedded in a provider bundle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SurfaceContractBundle {
    /// Format version of the provider-local contract bundle.
    pub schema_version: u32,
    /// Canonical definition for the provider's task.
    pub surface: SurfaceDefinition,
    /// Invariant concepts referenced by the surface.
    pub catalog: ParameterCatalog,
}

impl SurfaceContractBundle {
    /// Validate a provider-local bundle and return all contract errors.
    pub fn validate(&self) -> Result<(), Vec<ContractValidationError>> {
        validate_surface_bundles_internal(std::slice::from_ref(self), false)
    }
}

/// Aggregate checked catalog used by launchers and generated references.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SurfaceCatalogBundle {
    /// Format version of the aggregate contract bundle.
    pub schema_version: u32,
    /// Canonical concepts shared by the task surfaces.
    pub catalog: ParameterCatalog,
    /// Task definitions available through the catalog.
    pub surfaces: Vec<SurfaceDefinition>,
}

impl SurfaceCatalogBundle {
    /// The surface `id` names, now or, for a saved profile's header, before
    /// it was renamed.
    pub fn surface(&self, id: &str) -> Option<&SurfaceDefinition> {
        self.surfaces.iter().find(|surface| surface.id() == id)
    }

    /// Copy one task and only its referenced concepts into a provider-local bundle.
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

    /// Validate the combined surface and concept graph and collect all contract errors.
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
