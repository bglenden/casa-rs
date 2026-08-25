// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Sole pre-plan owner of whole-run imaging migration dispatch.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    sync::OnceLock,
};

use casa_imaging_model::{
    CompileProblemError, CompiledProblem, ImageDomainRole, ImagingRequest, InstrumentResponse,
    ModelStateIdentity, PhaseCentreLaw, PolarizationCoordinate, ProductKind, ReconstructionBasis,
    RequiredCapability, SpectralSampling, compile,
};
use serde::Deserialize;

const MIGRATION_MATRIX_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../resources/imaging-architecture/migration-matrix.json"
));

/// Whole-run owner selected before physical planning begins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestDisposition {
    /// Every required matrix row is owned by the native compile/plan/run path.
    Native,
    /// At least one required row has no production implementation during migration.
    TemporarilyUnavailable,
}

/// Task-surface requirement that is not derivable from the backend-independent
/// compiled problem alone.
///
/// These requirements may only add migration constraints. They cannot remove
/// a capability inferred from the compiled problem or force a Native route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskRouteRequirement {
    /// Spectral-cube task surface.
    SpectralCube,
    /// Cubedata task surface.
    SpectralCubedata,
    /// Mosaic gridder request.
    MosaicGridder,
    /// W-projection gridder request.
    WProjection,
    /// A/W-projection gridder request.
    AwProjection,
    /// Facet or outlier-file request.
    FacetsOutliers,
    /// Auto-multithreshold masking request.
    Automasking,
    /// Standalone CLEAN-mask product request.
    MaskProduct,
    /// Initial model supplied by the caller.
    StartModel,
    /// MODEL_DATA persistence request.
    ModelColumnWrite,
    /// Serial CPU execution was selected explicitly.
    SerialCpu,
    /// Automatic execution selection requires the shared planner and registry.
    ExecutionAuto,
    /// Fixed-tile CPU execution override.
    FixedTileCpu,
    /// Metal gridding override.
    MetalGridder,
    /// Metal row-run gridding override.
    MetalRowRunGridder,
    /// Grouped Metal row-run gridding override.
    MetalRowRunGroupedGridder,
    /// Automatic FFT selection requires the shared planner and registry.
    FftAuto,
    /// RustFFT override.
    RustFft,
    /// Accelerate FFT override.
    Accelerate,
    /// FFTW override.
    Fftw,
    /// Metal MPSGraph FFT override.
    MetalMpsGraph,
    /// Task controls outside the exact native serial-continuum v1 contract.
    NativeV1UnsupportedControls,
}

impl TaskRouteRequirement {
    const fn row_id(self) -> &'static str {
        match self {
            Self::SpectralCube => "capability.spectral-cube",
            Self::SpectralCubedata => "capability.spectral-cubedata",
            Self::MosaicGridder => "capability.mosaic-gridder",
            Self::WProjection => "capability.w-projection",
            Self::AwProjection => "capability.aw-projection",
            Self::FacetsOutliers => "capability.facets-outliers",
            Self::Automasking => "capability.automasking",
            Self::MaskProduct => "product.mask",
            Self::StartModel => "capability.start-model",
            Self::ModelColumnWrite => "capability.model-column-write",
            Self::SerialCpu => "backend.serial-cpu",
            Self::ExecutionAuto => "backend.execution-auto",
            Self::FixedTileCpu => "backend.fixed-tile-cpu",
            Self::MetalGridder => "backend.metal-gridder",
            Self::MetalRowRunGridder => "backend.metal-row-run-gridder",
            Self::MetalRowRunGroupedGridder => "backend.metal-row-run-grouped-gridder",
            Self::FftAuto => "backend.fft-auto",
            Self::RustFft => "backend.rustfft",
            Self::Accelerate => "backend.accelerate",
            Self::Fftw => "backend.fftw",
            Self::MetalMpsGraph => "backend.metal-mpsgraph",
            Self::NativeV1UnsupportedControls => "frontend.native-v1-unsupported",
        }
    }
}

/// Opaque native whole-run engine port. Only [`ImagingRouter`] can invoke it.
pub struct NativeEnginePort<Output, EngineError> {
    run: Box<EngineFn<Output, EngineError>>,
}

impl<Output, EngineError> NativeEnginePort<Output, EngineError> {
    /// Seal a native whole-run adapter behind the router-owned port.
    #[must_use]
    pub fn new(
        run: impl Fn(&CompiledProblem, &RouteRecord) -> Result<Output, EngineError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self { run: Box::new(run) }
    }
}

type EngineFn<Output, EngineError> =
    dyn Fn(&CompiledProblem, &RouteRecord) -> Result<Output, EngineError> + Send + Sync;

/// Evidence recorded for one pre-plan whole-run routing decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteRecord {
    matrix_schema_version: u32,
    matrix_contract_revision: u32,
    disposition: RequestDisposition,
    requirements: Vec<RouteRequirement>,
}

impl RouteRecord {
    /// Return the authoritative migration-matrix schema version.
    #[must_use]
    pub const fn matrix_schema_version(&self) -> u32 {
        self.matrix_schema_version
    }

    /// Return the authoritative migration-matrix contract revision.
    #[must_use]
    pub const fn matrix_contract_revision(&self) -> u32 {
        self.matrix_contract_revision
    }

    /// Return the exact selected whole-run owner.
    #[must_use]
    pub const fn disposition(&self) -> RequestDisposition {
        self.disposition
    }

    /// Return canonical matrix evidence for every requirement of the compiled problem.
    #[must_use]
    pub fn requirements(&self) -> &[RouteRequirement] {
        &self.requirements
    }
}

/// Kind of authoritative migration-matrix row required by a request.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MigrationRowKind {
    /// Scientific or operational imaging capability.
    Capability,
    /// Published scientific product.
    Product,
    /// Reconstruction solver.
    Solver,
    /// User-facing request projection.
    Frontend,
    /// Physical execution implementation family.
    Backend,
}

impl MigrationRowKind {
    const fn prefix(self) -> &'static str {
        match self {
            Self::Capability => "capability.",
            Self::Product => "product.",
            Self::Solver => "solver.",
            Self::Frontend => "frontend.",
            Self::Backend => "backend.",
        }
    }
}

/// Status and authoritative acceptance evidence for one required matrix row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteRequirement {
    id: String,
    kind: MigrationRowKind,
    status: RequestDisposition,
    current_owner: String,
    destination_tickets: Vec<String>,
    evidence_issues: Vec<u64>,
    baseline_manifests: Vec<String>,
    acceptance_contract: String,
    transfer_point: String,
    deletion_condition: String,
    source_evidence: Vec<String>,
    obligation: Option<ObligationDetail>,
}

impl RouteRequirement {
    /// Return the stable canonical row identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Return the matrix row kind.
    #[must_use]
    pub const fn kind(&self) -> MigrationRowKind {
        self.kind
    }

    /// Return this requirement's current production disposition.
    #[must_use]
    pub const fn status(&self) -> RequestDisposition {
        self.status
    }

    /// Return the sole current implementation owner.
    #[must_use]
    pub fn current_owner(&self) -> &str {
        &self.current_owner
    }

    /// Return the accepted destination tickets for transfer.
    #[must_use]
    pub fn destination_tickets(&self) -> &[String] {
        &self.destination_tickets
    }

    /// Return authoritative issue evidence carried by this matrix row.
    #[must_use]
    pub fn evidence_issues(&self) -> &[u64] {
        &self.evidence_issues
    }

    /// Return content-pinned baseline-manifest locators.
    #[must_use]
    pub fn baseline_manifests(&self) -> &[String] {
        &self.baseline_manifests
    }

    /// Return the versioned Acceptance Contract identifier.
    #[must_use]
    pub fn acceptance_contract(&self) -> &str {
        &self.acceptance_contract
    }

    /// Return the exact transfer milestone.
    #[must_use]
    pub fn transfer_point(&self) -> &str {
        &self.transfer_point
    }

    /// Return the same-merge deletion or quarantine condition.
    #[must_use]
    pub fn deletion_condition(&self) -> &str {
        &self.deletion_condition
    }

    /// Return repository source locators supporting the current status.
    #[must_use]
    pub fn source_evidence(&self) -> &[String] {
        &self.source_evidence
    }

    /// Return the executable obligation that prevents this row from routing native.
    #[must_use]
    pub fn obligation(&self) -> Option<MigrationObligation<'_>> {
        self.obligation.as_ref().map(|detail| MigrationObligation {
            requirement: self,
            detail,
        })
    }
}

/// Complete transfer obligation for one non-native required capability.
#[derive(Debug, Clone, Copy)]
pub struct MigrationObligation<'a> {
    requirement: &'a RouteRequirement,
    detail: &'a ObligationDetail,
}

impl MigrationObligation<'_> {
    /// Return the capability, product, or solver row blocked by this obligation.
    #[must_use]
    pub fn capability(&self) -> &str {
        self.requirement.id()
    }

    /// Return the sole current owner in which fixes must land before transfer.
    #[must_use]
    pub fn current_owner(&self) -> &str {
        self.requirement.current_owner()
    }

    /// Return the owning transfer ticket.
    #[must_use]
    pub fn ticket(&self) -> &str {
        &self.detail.ticket
    }

    /// Return why this obligation remains open.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.detail.reason
    }

    /// Return every accepted destination ticket for this transfer.
    #[must_use]
    pub fn destination_tickets(&self) -> &[String] {
        self.requirement.destination_tickets()
    }

    /// Return the acceptance milestone that must pass before transfer.
    #[must_use]
    pub fn transfer_point(&self) -> &str {
        self.requirement.transfer_point()
    }

    /// Return the same-merge deletion or quarantine condition.
    #[must_use]
    pub fn deletion_condition(&self) -> &str {
        self.requirement.deletion_condition()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObligationDetail {
    ticket: String,
    reason: String,
}

/// Successful result from exactly one selected whole-run engine.
#[derive(Debug, PartialEq, Eq)]
pub struct DispatchOutcome<Output> {
    route: RouteRecord,
    output: Output,
}

impl<Output> DispatchOutcome<Output> {
    /// Return the recorded pre-plan route.
    #[must_use]
    pub const fn route(&self) -> &RouteRecord {
        &self.route
    }

    /// Return the selected engine's whole-run output.
    #[must_use]
    pub const fn output(&self) -> &Output {
        &self.output
    }

    /// Consume the result while preserving both routing evidence and engine output.
    #[must_use]
    pub fn into_parts(self) -> (RouteRecord, Output) {
        (self.route, self.output)
    }
}

/// Failure before or during the one selected whole-run engine invocation.
#[derive(Debug)]
pub enum DispatchError<EngineError> {
    /// Logical compilation failed; neither engine was invoked.
    Compile(CompileProblemError),
    /// The built-in routing matrix was incomplete or invalid; neither engine was invoked.
    InvalidMatrix(String),
    /// The request contains a capability with no production owner during migration.
    TemporarilyUnavailable(RouteRecord),
    /// The selected native whole-run engine failed without retrying through another engine.
    Native {
        /// Evidence for the route that selected native execution.
        route: RouteRecord,
        /// Failure returned by the selected native engine.
        source: EngineError,
    },
}

impl<EngineError: fmt::Display> fmt::Display for DispatchError<EngineError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compile(error) => {
                write!(formatter, "imaging request compilation failed: {error}")
            }
            Self::InvalidMatrix(error) => {
                write!(formatter, "imaging migration matrix is invalid: {error}")
            }
            Self::TemporarilyUnavailable(_) => {
                formatter.write_str("imaging request is temporarily unavailable")
            }
            Self::Native { source, .. } => write!(formatter, "native imaging run failed: {source}"),
        }
    }
}

impl<EngineError: Error + 'static> Error for DispatchError<EngineError> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Compile(error) => Some(error),
            Self::Native { source, .. } => Some(source),
            Self::InvalidMatrix(_) | Self::TemporarilyUnavailable(_) => None,
        }
    }
}

/// Sole executable pre-plan migration router.
pub struct ImagingRouter<Output, EngineError> {
    native: NativeEnginePort<Output, EngineError>,
    #[cfg(test)]
    matrix: Option<MatrixCatalog>,
}

impl<Output, EngineError> ImagingRouter<Output, EngineError> {
    /// Bind the sole native whole-run engine port. Stages are never exposed.
    #[must_use]
    pub const fn new(native: NativeEnginePort<Output, EngineError>) -> Self {
        Self {
            native,
            #[cfg(test)]
            matrix: None,
        }
    }

    #[cfg(test)]
    fn with_matrix_json(
        native: NativeEnginePort<Output, EngineError>,
        matrix_json: String,
    ) -> Self {
        Self {
            native,
            matrix: Some(parse_matrix(&matrix_json).expect("test migration matrix must be valid")),
        }
    }

    /// Compile, classify, and invoke exactly one whole-run engine before planning.
    pub fn dispatch(
        &self,
        request: ImagingRequest,
    ) -> Result<DispatchOutcome<Output>, DispatchError<EngineError>> {
        self.dispatch_with_task_requirements(request, [])
    }

    /// Compile, classify with task-only requirements, and invoke exactly one
    /// whole-run engine before planning.
    pub fn dispatch_with_task_requirements(
        &self,
        request: ImagingRequest,
        task_requirements: impl IntoIterator<Item = TaskRouteRequirement>,
    ) -> Result<DispatchOutcome<Output>, DispatchError<EngineError>> {
        let problem = compile(request).map_err(DispatchError::Compile)?;
        let route = self
            .route(&problem, task_requirements)
            .map_err(DispatchError::InvalidMatrix)?;
        match route.disposition {
            RequestDisposition::Native => match (self.native.run)(&problem, &route) {
                Ok(output) => Ok(DispatchOutcome { route, output }),
                Err(source) => Err(DispatchError::Native { route, source }),
            },
            RequestDisposition::TemporarilyUnavailable => {
                Err(DispatchError::TemporarilyUnavailable(route))
            }
        }
    }

    fn route(
        &self,
        problem: &CompiledProblem,
        task_requirements: impl IntoIterator<Item = TaskRouteRequirement>,
    ) -> Result<RouteRecord, String> {
        #[cfg(test)]
        if let Some(catalog) = &self.matrix {
            return route_with_catalog_and_task_requirements(problem, catalog, task_requirements);
        }
        route_with_task_requirements(problem, task_requirements)
    }
}

#[derive(Debug, Deserialize)]
struct MatrixDocument {
    schema_version: u32,
    contract_revision: u32,
    status_values: Vec<MatrixStatus>,
    product_kind_inventory: BTreeMap<String, String>,
    polarization_coordinate_inventory: BTreeMap<String, String>,
    cube_interpolation_inventory: BTreeMap<String, String>,
    spectral_mode_inventory: BTreeMap<String, String>,
    gridder_request_inventory: BTreeMap<String, String>,
    deconvolver_inventory: BTreeMap<String, String>,
    rows: Vec<MatrixRow>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
enum MatrixStatus {
    Native,
    TemporarilyUnavailable,
}

impl From<MatrixStatus> for RequestDisposition {
    fn from(status: MatrixStatus) -> Self {
        match status {
            MatrixStatus::Native => Self::Native,
            MatrixStatus::TemporarilyUnavailable => Self::TemporarilyUnavailable,
        }
    }
}

#[derive(Debug, Deserialize)]
struct MatrixRow {
    id: String,
    kind: MigrationRowKind,
    status: MatrixStatus,
    current_owner: String,
    destination_tickets: Vec<String>,
    evidence_issues: Vec<u64>,
    baseline_manifests: Vec<String>,
    acceptance_contract: String,
    transfer_point: String,
    deletion_condition: String,
    migration_obligation: Option<RawObligation>,
    source_evidence: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RawObligation {
    ticket: String,
    reason: String,
}

#[derive(Debug)]
struct MatrixCatalog {
    schema_version: u32,
    contract_revision: u32,
    rows: BTreeMap<String, RouteRequirement>,
    bindings: MatrixBindings,
}

#[derive(Debug)]
struct MatrixBindings {
    products: BTreeMap<String, String>,
    polarizations: BTreeMap<String, String>,
    interpolations: BTreeMap<String, String>,
    spectral_modes: BTreeMap<String, String>,
    gridders: BTreeMap<String, String>,
    solvers: BTreeMap<String, String>,
}

fn route_with_task_requirements(
    problem: &CompiledProblem,
    task_requirements: impl IntoIterator<Item = TaskRouteRequirement>,
) -> Result<RouteRecord, String> {
    let catalog = matrix_catalog()?;
    route_with_catalog_and_task_requirements(problem, catalog, task_requirements)
}

fn route_with_catalog_and_task_requirements<'a>(
    problem: &CompiledProblem,
    catalog: &'a MatrixCatalog,
    task_requirements: impl IntoIterator<Item = TaskRouteRequirement>,
) -> Result<RouteRecord, String> {
    let mut required_rows: BTreeSet<&'a str> = required_rows(problem, catalog)?;
    for requirement in task_requirements {
        let identifier: &'a str = requirement.row_id();
        required_rows.insert(identifier);
    }
    let mut disposition = RequestDisposition::Native;
    let mut requirements = Vec::with_capacity(required_rows.len());
    for identifier in required_rows {
        let requirement = catalog
            .rows
            .get(identifier)
            .cloned()
            .ok_or_else(|| format!("required row {identifier:?} is absent"))?;
        disposition = match (disposition, requirement.status) {
            (_, RequestDisposition::TemporarilyUnavailable) => {
                RequestDisposition::TemporarilyUnavailable
            }
            (RequestDisposition::TemporarilyUnavailable, _) => {
                RequestDisposition::TemporarilyUnavailable
            }
            (current, RequestDisposition::Native) => current,
        };
        requirements.push(requirement);
    }
    Ok(RouteRecord {
        matrix_schema_version: catalog.schema_version,
        matrix_contract_revision: catalog.contract_revision,
        disposition,
        requirements,
    })
}

fn matrix_catalog() -> Result<&'static MatrixCatalog, String> {
    static CATALOG: OnceLock<Result<MatrixCatalog, String>> = OnceLock::new();
    CATALOG
        .get_or_init(|| parse_matrix(MIGRATION_MATRIX_JSON))
        .as_ref()
        .map_err(Clone::clone)
}

fn parse_matrix(json: &str) -> Result<MatrixCatalog, String> {
    let document = serde_json::from_str::<MatrixDocument>(json)
        .map_err(|error| format!("cannot parse built-in migration matrix: {error}"))?;
    if document.schema_version != 1 {
        return Err(format!(
            "unsupported schema version {}",
            document.schema_version
        ));
    }
    if document.status_values != [MatrixStatus::Native, MatrixStatus::TemporarilyUnavailable] {
        return Err("status inventory differs from the two accepted dispositions".to_string());
    }
    if document.contract_revision == 0 {
        return Err("contract revision must be a positive integer".to_string());
    }
    let mut rows = BTreeMap::new();
    for row in document.rows {
        let status = row.status.into();
        if !row.id.starts_with(row.kind.prefix()) {
            return Err(format!(
                "row {:?} does not match its {:?} kind",
                row.id, row.kind
            ));
        }
        for (field, value) in [
            ("current_owner", row.current_owner.as_str()),
            ("acceptance_contract", row.acceptance_contract.as_str()),
            ("transfer_point", row.transfer_point.as_str()),
            ("deletion_condition", row.deletion_condition.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!("row {:?} has empty {field}", row.id));
            }
        }
        if row.destination_tickets.is_empty()
            || row.evidence_issues.is_empty()
            || row.baseline_manifests.is_empty()
            || row.source_evidence.is_empty()
        {
            return Err(format!("row {:?} has incomplete routing evidence", row.id));
        }
        let obligation = match (status, row.migration_obligation) {
            (RequestDisposition::Native, None) => None,
            (RequestDisposition::Native, Some(_)) => {
                return Err(format!("native row {:?} retains an obligation", row.id));
            }
            (_, Some(obligation))
                if !obligation.ticket.trim().is_empty() && !obligation.reason.trim().is_empty() =>
            {
                Some(ObligationDetail {
                    ticket: obligation.ticket,
                    reason: obligation.reason,
                })
            }
            (_, _) => {
                return Err(format!("non-native row {:?} lacks an obligation", row.id));
            }
        };
        let requirement = RouteRequirement {
            id: row.id.clone(),
            kind: row.kind,
            status,
            current_owner: row.current_owner,
            destination_tickets: row.destination_tickets,
            evidence_issues: row.evidence_issues,
            baseline_manifests: row.baseline_manifests,
            acceptance_contract: row.acceptance_contract,
            transfer_point: row.transfer_point,
            deletion_condition: row.deletion_condition,
            source_evidence: row.source_evidence,
            obligation,
        };
        if rows.insert(row.id.clone(), requirement).is_some() {
            return Err(format!("duplicate row {:?}", row.id));
        }
    }
    for (inventory, bindings, kind) in [
        (
            "product_kind_inventory",
            &document.product_kind_inventory,
            MigrationRowKind::Product,
        ),
        (
            "polarization_coordinate_inventory",
            &document.polarization_coordinate_inventory,
            MigrationRowKind::Capability,
        ),
        (
            "cube_interpolation_inventory",
            &document.cube_interpolation_inventory,
            MigrationRowKind::Capability,
        ),
        (
            "spectral_mode_inventory",
            &document.spectral_mode_inventory,
            MigrationRowKind::Capability,
        ),
        (
            "gridder_request_inventory",
            &document.gridder_request_inventory,
            MigrationRowKind::Capability,
        ),
        (
            "deconvolver_inventory",
            &document.deconvolver_inventory,
            MigrationRowKind::Solver,
        ),
    ] {
        if bindings.is_empty() {
            return Err(format!("{inventory} is empty"));
        }
        for (variant, identifier) in bindings {
            let requirement = rows.get(identifier).ok_or_else(|| {
                format!("{inventory}.{variant} references absent row {identifier:?}")
            })?;
            if requirement.kind != kind {
                return Err(format!(
                    "{inventory}.{variant} references a {:?} row instead of {kind:?}",
                    requirement.kind
                ));
            }
        }
    }
    Ok(MatrixCatalog {
        schema_version: document.schema_version,
        contract_revision: document.contract_revision,
        rows,
        bindings: MatrixBindings {
            products: document.product_kind_inventory,
            polarizations: document.polarization_coordinate_inventory,
            interpolations: document.cube_interpolation_inventory,
            spectral_modes: document.spectral_mode_inventory,
            gridders: document.gridder_request_inventory,
            solvers: document.deconvolver_inventory,
        },
    })
}

fn required_rows<'a>(
    problem: &CompiledProblem,
    catalog: &'a MatrixCatalog,
) -> Result<BTreeSet<&'a str>, String> {
    let mut rows = BTreeSet::from([
        "capability.compiled-problem",
        "capability.ms-selection",
        "capability.observation-transaction",
    ]);
    if !problem
        .observation_transaction()
        .write_set()
        .model_columns()
        .is_empty()
    {
        rows.insert("capability.model-column-write");
    }
    for capability in problem.required_capabilities() {
        match capability {
            RequiredCapability::FacetedGeometry => {
                rows.insert("capability.facets-outliers");
            }
            RequiredCapability::SpectralFrameTransform => {
                rows.insert("capability.lsrk-transform");
            }
            RequiredCapability::SpectralResampling => {
                let row = match problem.science().spectral().sampling() {
                    SpectralSampling::Nearest => matrix_binding(
                        &catalog.bindings.interpolations,
                        "Nearest",
                        "cube_interpolation_inventory",
                    )?,
                    SpectralSampling::Linear => matrix_binding(
                        &catalog.bindings.interpolations,
                        "Linear",
                        "cube_interpolation_inventory",
                    )?,
                    SpectralSampling::ChannelAverage { .. } => matrix_binding(
                        &catalog.bindings.spectral_modes,
                        "Cubedata",
                        "spectral_mode_inventory",
                    )?,
                    SpectralSampling::Identity => unreachable!(
                        "compiled capability set cannot require identity spectral resampling"
                    ),
                };
                rows.insert(row);
            }
            RequiredCapability::CommonBeamSpectralCoupling => {
                rows.insert(matrix_binding(
                    &catalog.bindings.products,
                    "Beam",
                    "product_kind_inventory",
                )?);
            }
            RequiredCapability::Polarization(coordinate) => {
                rows.insert(matrix_binding(
                    &catalog.bindings.polarizations,
                    polarization_variant(*coordinate),
                    "polarization_coordinate_inventory",
                )?);
            }
            RequiredCapability::PrimaryBeamResponse => {
                rows.insert(matrix_binding(
                    &catalog.bindings.gridders,
                    "Mosaic",
                    "gridder_request_inventory",
                )?);
            }
            RequiredCapability::FullMuellerResponse => {
                rows.insert(matrix_binding(
                    &catalog.bindings.gridders,
                    "AwProject",
                    "gridder_request_inventory",
                )?);
            }
            RequiredCapability::UvTaper
            | RequiredCapability::UniformWeighting
            | RequiredCapability::BriggsWeighting
            | RequiredCapability::BriggsBandwidthTaperWeighting => {
                rows.insert("capability.global-weighting");
            }
            RequiredCapability::ConstantBasis => {
                rows.insert(matrix_binding(
                    &catalog.bindings.spectral_modes,
                    "Mfs",
                    "spectral_mode_inventory",
                )?);
            }
            RequiredCapability::TaylorBasis => {
                rows.insert("capability.mtmfs");
            }
            RequiredCapability::MtmfsReconstruction => {
                rows.insert("capability.major-minor-cycles");
                rows.insert("capability.mtmfs");
                rows.insert(matrix_binding(
                    &catalog.bindings.solvers,
                    "Mtmfs",
                    "deconvolver_inventory",
                )?);
            }
            RequiredCapability::ChannelLocalBasis => {
                rows.insert(matrix_binding(
                    &catalog.bindings.spectral_modes,
                    "Cube",
                    "spectral_mode_inventory",
                )?);
            }
            RequiredCapability::DirtyReconstruction
            | RequiredCapability::NaturalWeighting
            | RequiredCapability::UnitResponseNormalization
            | RequiredCapability::FlatNoiseNormalization
            | RequiredCapability::FlatSkyNormalization => {}
            RequiredCapability::HogbomReconstruction => {
                rows.insert("capability.major-minor-cycles");
                rows.insert(matrix_binding(
                    &catalog.bindings.solvers,
                    "Hogbom",
                    "deconvolver_inventory",
                )?);
            }
            RequiredCapability::ClarkReconstruction => {
                rows.insert("capability.major-minor-cycles");
                rows.insert(matrix_binding(
                    &catalog.bindings.solvers,
                    "Clark",
                    "deconvolver_inventory",
                )?);
            }
            RequiredCapability::MultiscaleReconstruction => {
                rows.insert("capability.major-minor-cycles");
                rows.insert(matrix_binding(
                    &catalog.bindings.solvers,
                    "Multiscale",
                    "deconvolver_inventory",
                )?);
            }
            RequiredCapability::Product(product) => {
                rows.insert(matrix_binding(
                    &catalog.bindings.products,
                    product_variant(*product),
                    "product_kind_inventory",
                )?);
            }
        }
    }

    match problem
        .science()
        .measurement_equation()
        .instrument_response()
    {
        InstrumentResponse::Scalar => {
            rows.insert(matrix_binding(
                &catalog.bindings.gridders,
                "Standard",
                "gridder_request_inventory",
            )?);
        }
        InstrumentResponse::PrimaryBeam => {
            rows.insert(matrix_binding(
                &catalog.bindings.gridders,
                "Mosaic",
                "gridder_request_inventory",
            )?);
            rows.insert(match problem.reconstruction().basis() {
                ReconstructionBasis::ChannelLocal { .. } => "capability.mosaic-cube",
                ReconstructionBasis::Constant | ReconstructionBasis::Taylor { .. } => {
                    "capability.mosaic-mfs"
                }
            });
        }
        InstrumentResponse::FullMueller => {
            rows.insert(matrix_binding(
                &catalog.bindings.gridders,
                "AwProject",
                "gridder_request_inventory",
            )?);
        }
    }
    if problem
        .geometry()
        .domains()
        .iter()
        .any(|domain| matches!(domain.role(), ImageDomainRole::Outlier(_)))
    {
        rows.insert("capability.facets-outliers");
    }
    if matches!(
        problem.geometry().centres().phase_tracking(),
        PhaseCentreLaw::Ephemeris(_)
    ) {
        rows.insert("capability.moving-source");
    }
    if !matches!(problem.inputs().model(), ModelStateIdentity::Empty) {
        rows.insert("capability.start-model");
    }
    Ok(rows)
}

fn matrix_binding<'a>(
    inventory: &'a BTreeMap<String, String>,
    variant: &str,
    inventory_name: &str,
) -> Result<&'a str, String> {
    inventory
        .get(variant)
        .map(String::as_str)
        .ok_or_else(|| format!("{inventory_name} lacks required variant {variant}"))
}

const fn polarization_variant(coordinate: PolarizationCoordinate) -> &'static str {
    match coordinate {
        PolarizationCoordinate::StokesI => "StokesI",
        PolarizationCoordinate::StokesQ => "StokesQ",
        PolarizationCoordinate::StokesU => "StokesU",
        PolarizationCoordinate::StokesV => "StokesV",
        PolarizationCoordinate::LinearXx => "LinearXx",
        PolarizationCoordinate::LinearXy => "LinearXy",
        PolarizationCoordinate::LinearYx => "LinearYx",
        PolarizationCoordinate::LinearYy => "LinearYy",
        PolarizationCoordinate::CircularRr => "CircularRr",
        PolarizationCoordinate::CircularRl => "CircularRl",
        PolarizationCoordinate::CircularLr => "CircularLr",
        PolarizationCoordinate::CircularLl => "CircularLl",
    }
}

const fn product_variant(product: ProductKind) -> &'static str {
    match product {
        ProductKind::Psf => "Psf",
        ProductKind::Residual => "Residual",
        ProductKind::Model => "Model",
        ProductKind::RestoredImage => "RestoredImage",
        ProductKind::SumWeights => "SumWeights",
        ProductKind::Mask => "Mask",
        ProductKind::Weight => "Weight",
        ProductKind::PrimaryBeam => "PrimaryBeam",
        ProductKind::Sensitivity => "Sensitivity",
        ProductKind::PbCorrectedImage => "PbCorrectedImage",
        ProductKind::TaylorTerms => "TaylorTerms",
        ProductKind::SpectralIndex => "SpectralIndex",
        ProductKind::SpectralIndexError => "SpectralIndexError",
        ProductKind::PbCorrectedSpectralIndex => "PbCorrectedSpectralIndex",
        ProductKind::Beam => "Beam",
    }
}

#[cfg(test)]
mod tests;
