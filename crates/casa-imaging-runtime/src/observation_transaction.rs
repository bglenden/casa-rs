// SPDX-License-Identifier: LGPL-3.0-or-later

//! Plan binding for snapshot-consistent imaging side effects.
//!
//! CASA and LibRA write visibility models while holding MeasurementSet table
//! locks. This contract retains their explicit selection and lock semantics.
//! Conventional products retain their independent publication protocol.
//! `MODEL_DATA` is different: its final-major replay writes selected cells in
//! place under the planned MeasurementSet lock and incomplete-write marker.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

use casa_imaging_model::{
    CompiledProblem, CompiledProblemId, MeasurementSetIdentity, ObservationTransactionId,
    ProductGraphId,
};

use crate::{
    ClaimLifetime, ExecutionDag, FenceId, FenceKind, IoBufferKind, LeaseResource, PhysicalWorkId,
    StorageUseKind, WorkDependency, WorkKind, WorkNode, WorkNodeId,
};

/// Closed publication authority carried by one observation-transaction plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObservationTransactionPublicationScope {
    /// Reconcile reconstruction state without staging or publishing products.
    ReconstructionOnly,
    /// Stage and atomically publish every required Product Graph member.
    ProductPublication,
    /// Publish already sealed conventional products without observation I/O.
    SealedProductPublication,
}

/// Exact execution-DAG events that implement one observation transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservationTransactionWork {
    publication_scope: ObservationTransactionPublicationScope,
    initial_consistency_check: WorkNodeId,
    observation_reads: BTreeSet<WorkDependency>,
    final_model_preparation: Option<WorkNodeId>,
    post_replay_reconciliation: Option<WorkNodeId>,
    product_staging: BTreeSet<WorkDependency>,
    model_column_writeback: Option<WorkNodeId>,
    commit: WorkNodeId,
}

impl ObservationTransactionWork {
    /// Name every checkpoint for a reconstruction-only transaction.
    ///
    /// Observation-read completions are derived from every typed
    /// [`WorkKind::reads_observation`] node during the mandatory plan seal.
    #[must_use]
    pub const fn new_reconstruction(
        initial_consistency_check: WorkNodeId,
        post_replay_reconciliation: WorkNodeId,
        commit: WorkNodeId,
    ) -> Self {
        Self {
            publication_scope: ObservationTransactionPublicationScope::ReconstructionOnly,
            initial_consistency_check,
            observation_reads: BTreeSet::new(),
            final_model_preparation: None,
            post_replay_reconciliation: Some(post_replay_reconciliation),
            product_staging: BTreeSet::new(),
            model_column_writeback: None,
            commit,
        }
    }

    /// Name every checkpoint for an atomic product-publication transaction.
    #[must_use]
    pub const fn new_product_publication(
        initial_consistency_check: WorkNodeId,
        post_replay_reconciliation: WorkNodeId,
        commit: WorkNodeId,
    ) -> Self {
        Self {
            publication_scope: ObservationTransactionPublicationScope::ProductPublication,
            initial_consistency_check,
            observation_reads: BTreeSet::new(),
            final_model_preparation: None,
            post_replay_reconciliation: Some(post_replay_reconciliation),
            product_staging: BTreeSet::new(),
            model_column_writeback: None,
            commit,
        }
    }

    /// Name a publication-only transaction over already sealed products.
    #[must_use]
    pub const fn new_sealed_product_publication(
        publication_check: WorkNodeId,
        commit: WorkNodeId,
    ) -> Self {
        Self {
            publication_scope: ObservationTransactionPublicationScope::SealedProductPublication,
            initial_consistency_check: publication_check,
            observation_reads: BTreeSet::new(),
            final_model_preparation: None,
            post_replay_reconciliation: None,
            product_staging: BTreeSet::new(),
            model_column_writeback: None,
            commit,
        }
    }

    /// Bind the plan node that prepares the immutable final-model candidate.
    pub(crate) fn with_final_model_preparation(mut self, node: WorkNodeId) -> Self {
        self.final_model_preparation = Some(node);
        self
    }

    /// Bind the sole terminal replay that writes selected `MODEL_DATA` cells.
    pub(crate) fn with_model_column_writeback(mut self, node: WorkNodeId) -> Self {
        self.model_column_writeback = Some(node);
        self
    }

    /// Return whether this transaction reconciles only or publishes products.
    #[must_use]
    pub const fn publication_scope(&self) -> ObservationTransactionPublicationScope {
        self.publication_scope
    }

    /// Return the consistency check that must precede observation reads.
    #[must_use]
    pub const fn initial_consistency_check(&self) -> &WorkNodeId {
        &self.initial_consistency_check
    }

    /// Return exact completion events for all physical observation reads.
    ///
    /// Every producer revalidates and consumes the bound read set while
    /// holding one declared table lock per MeasurementSet through this event.
    #[must_use]
    pub const fn observation_reads(&self) -> &BTreeSet<WorkDependency> {
        &self.observation_reads
    }

    /// Return the node that prepares the immutable final-model candidate.
    #[must_use]
    pub const fn final_model_preparation(&self) -> Option<&WorkNodeId> {
        self.final_model_preparation.as_ref()
    }

    /// Return the mandatory post-replay Major-Cycle reconciliation node.
    #[must_use]
    pub fn post_replay_reconciliation(&self) -> &WorkNodeId {
        self.post_replay_reconciliation
            .as_ref()
            .expect("reconstruction transaction has a reconciliation node")
    }

    pub(crate) const fn optional_post_replay_reconciliation(&self) -> Option<&WorkNodeId> {
        self.post_replay_reconciliation.as_ref()
    }

    /// Return exact completion events for every privately staged required product.
    #[must_use]
    pub const fn product_staging(&self) -> &BTreeSet<WorkDependency> {
        &self.product_staging
    }

    /// Return the final-major replay that writes selected `MODEL_DATA` cells.
    #[must_use]
    pub const fn model_column_writeback(&self) -> Option<&WorkNodeId> {
        self.model_column_writeback.as_ref()
    }

    /// Return the sole node permitted to revalidate and publish side effects.
    ///
    /// The node holds every MeasurementSet lock while it rechecks the exact
    /// read/write preconditions. Successful completion of its publication
    /// fence establishes readiness only; the runtime's final publish call
    /// atomically activates conventional-product members. In-place
    /// `MODEL_DATA` completion is owned by its final-major replay instead.
    #[must_use]
    pub const fn commit(&self) -> &WorkNodeId {
        &self.commit
    }
}

/// Validated problem-bound observation transaction in one immutable DAG.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundObservationTransaction {
    problem_id: CompiledProblemId,
    product_graph_id: ProductGraphId,
    transaction_id: ObservationTransactionId,
    physical_work_id: PhysicalWorkId,
    work: ObservationTransactionWork,
}

impl BoundObservationTransaction {
    /// Return the exact compiled problem whose transaction was validated.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem_id
    }

    /// Return the exact compiler-owned product topology validated for publication.
    #[must_use]
    pub const fn product_graph_id(&self) -> ProductGraphId {
        self.product_graph_id
    }

    /// Return the logical read/write contract this physical work implements.
    #[must_use]
    pub const fn transaction_id(&self) -> ObservationTransactionId {
        self.transaction_id
    }

    /// Return the immutable physical DAG whose declarations were validated.
    #[must_use]
    pub const fn physical_work_id(&self) -> PhysicalWorkId {
        self.physical_work_id
    }

    /// Return the validated physical checkpoint and staging events.
    #[must_use]
    pub const fn work(&self) -> &ObservationTransactionWork {
        &self.work
    }
}

/// Failure to bind an observation transaction to physical work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObservationTransactionPlanError {
    /// A node, dependency, resource, or fence violates atomic-side-effect rules.
    InvalidPlan {
        /// Stable diagnostic describing the rejected declaration.
        reason: String,
    },
}

impl fmt::Display for ObservationTransactionPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPlan { reason } => {
                write!(formatter, "invalid observation transaction plan: {reason}")
            }
        }
    }
}

impl Error for ObservationTransactionPlanError {}

/// Validate and bind exact transaction work to an immutable execution DAG.
pub(crate) fn bind_observation_transaction(
    problem: &CompiledProblem,
    dag: &ExecutionDag,
    mut work: ObservationTransactionWork,
    publication_layouts: &crate::PublicationLayoutLedger,
    artifacts: &[crate::PlannedArtifact],
) -> Result<BoundObservationTransaction, ObservationTransactionPlanError> {
    let contract = problem.observation_transaction();
    let measurement_sets = contract
        .read_set()
        .sources()
        .iter()
        .map(|source| source.measurement_set())
        .collect::<BTreeSet<_>>();
    let product_graph = problem.product_graph();
    let expected_products = product_graph
        .publication()
        .members()
        .iter()
        .map(|node_id| crate::PublicationParticipant::Product {
            graph_id: product_graph.graph_id(),
            node_id: *node_id,
        })
        .collect::<BTreeSet<_>>();
    let declared_products = publication_layouts
        .entries()
        .iter()
        .map(|entry| entry.participant())
        .collect::<BTreeSet<_>>();
    match work.publication_scope {
        ObservationTransactionPublicationScope::ReconstructionOnly => {
            if !declared_products.is_empty() {
                return invalid(
                    "reconstruction-only transaction declares product publication layouts",
                );
            }
        }
        ObservationTransactionPublicationScope::ProductPublication => {
            if declared_products != expected_products {
                return invalid(format!(
                    "publication product nodes {declared_products:?} do not match graph members {expected_products:?}"
                ));
            }
        }
        ObservationTransactionPublicationScope::SealedProductPublication => {
            if declared_products != expected_products {
                return invalid(format!(
                    "sealed publication product nodes {declared_products:?} do not match graph members {expected_products:?}"
                ));
            }
        }
    }
    let output_artifacts = artifacts
        .iter()
        .filter(|artifact| artifact.role() == crate::ArtifactRole::Output)
        .map(|artifact| artifact.identity())
        .collect::<BTreeSet<_>>();
    let layout_artifacts = publication_layouts
        .entries()
        .iter()
        .map(|entry| entry.artifact())
        .collect::<BTreeSet<_>>();
    if layout_artifacts != output_artifacts {
        return invalid("publication layout artifacts do not exactly match planned outputs");
    }
    for layout in publication_layouts.entries() {
        let planned = artifacts
            .iter()
            .find(|artifact| artifact.identity() == layout.artifact())
            .expect("layout and output artifact sets were matched");
        if planned.node() != &work.commit {
            return invalid(format!(
                "publication artifact {} belongs to {}, expected sole commit {}",
                layout.artifact(),
                planned.node().as_str(),
                work.commit.as_str()
            ));
        }
    }
    work.product_staging = publication_layouts
        .entries()
        .iter()
        .filter(|entry| {
            matches!(
                entry.participant(),
                crate::PublicationParticipant::Product { .. }
            )
        })
        .map(|entry| entry.staging().terminal().clone())
        .collect();
    match work.publication_scope {
        ObservationTransactionPublicationScope::ReconstructionOnly => {
            if !work.product_staging.is_empty() {
                return invalid("reconstruction-only transaction stages products");
            }
        }
        ObservationTransactionPublicationScope::ProductPublication => {
            if work.product_staging.is_empty() {
                return invalid("product publication layout is empty");
            }
        }
        ObservationTransactionPublicationScope::SealedProductPublication => {
            if work.product_staging.is_empty() {
                return invalid("sealed product publication layout is empty");
            }
        }
    }
    let phase_model_columns = if work.model_column_writeback.is_some() {
        contract.write_set().model_columns().len()
    } else {
        0
    };
    work.observation_reads = validate_transaction_nodes(
        contract.read_set().sources().len(),
        phase_model_columns,
        dag.nodes(),
        &work,
    )?;
    if work.publication_scope != ObservationTransactionPublicationScope::SealedProductPublication {
        validate_measurement_set_lock_identities(&measurement_sets, dag.nodes(), &work)?;
    }
    Ok(BoundObservationTransaction {
        problem_id: problem.problem_id(),
        product_graph_id: product_graph.graph_id(),
        transaction_id: contract.transaction_id(),
        physical_work_id: dag.physical_work_id(),
        work,
    })
}

fn validate_transaction_nodes(
    read_sources: usize,
    model_column_sources: usize,
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    work: &ObservationTransactionWork,
) -> Result<BTreeSet<WorkDependency>, ObservationTransactionPlanError> {
    if work.publication_scope == ObservationTransactionPublicationScope::SealedProductPublication {
        return validate_product_publication_nodes(nodes, work);
    }
    if let Some(node) = nodes
        .values()
        .find(|node| node.kind == WorkKind::Publication && node.id != work.commit)
    {
        return invalid(format!(
            "publication node {} bypasses the atomic commit gate",
            node.id.as_str()
        ));
    }

    let initial = require_node(
        nodes,
        &work.initial_consistency_check,
        "initial consistency",
    )?;
    require_kind(initial, WorkKind::DataCensus, "initial consistency")?;
    require_exact_lock_count(initial, read_sources, "initial consistency")?;
    let initial_completions = completion_events(initial);

    let observation_reads = derive_observation_reads(
        nodes,
        initial,
        &work.commit,
        work.model_column_writeback.as_ref(),
    )?;
    if observation_reads.is_empty() {
        return invalid("observation read event set is empty");
    }
    let read_nodes =
        require_exact_completion_events(nodes, &observation_reads, "observation read")?;
    for producer in read_nodes {
        require_exact_lock_count(producer, read_sources, "observation read")?;
        for completion in &initial_completions {
            require_precedes(nodes, completion, &producer.id, "initial consistency")?;
        }
    }
    for completion in &observation_reads {
        require_precedes(
            nodes,
            completion,
            work.post_replay_reconciliation
                .as_ref()
                .expect("reconstruction transaction has reconciliation"),
            "observation read",
        )?;
        require_precedes(nodes, completion, &work.commit, "observation read")?;
    }

    let model_preparation = work
        .final_model_preparation
        .as_ref()
        .map(|node| require_node(nodes, node, "final-model preparation"))
        .transpose()?;
    if let Some(preparation) = model_preparation {
        require_kind(preparation, WorkKind::Compute, "final-model preparation")?;
        for completion in &initial_completions {
            require_precedes(nodes, completion, &preparation.id, "initial consistency")?;
        }
    }

    let reconciliation = require_node(
        nodes,
        work.post_replay_reconciliation
            .as_ref()
            .expect("reconstruction transaction has reconciliation"),
        "post-replay reconciliation",
    )?;
    require_kind(
        reconciliation,
        WorkKind::Compute,
        "post-replay reconciliation",
    )?;
    let reconciliation_completions = completion_events(reconciliation);

    let mut staged_nodes = Vec::new();
    for producer in
        require_exact_completion_events(nodes, &work.product_staging, "product staging")?
    {
        if producer.kind == WorkKind::Publication {
            return invalid(format!(
                "product staging publishes before the atomic commit gate through {}",
                producer.id.as_str()
            ));
        }
        require_claim(
            producer,
            is_staged_output,
            "staged-output storage",
            "product staging",
        )?;
        staged_nodes.push(producer);
    }

    match (model_column_sources, &work.model_column_writeback) {
        (0, None) => {}
        (0, Some(_)) => return invalid("read-only transaction declares MODEL_DATA writeback"),
        (_, None) => return invalid("write transaction omits MODEL_DATA writeback"),
        (_, Some(model_id)) => {
            let preparation =
                model_preparation.ok_or_else(|| ObservationTransactionPlanError::InvalidPlan {
                    reason: "MODEL_DATA writeback omits final-model preparation".to_string(),
                })?;
            let model = require_node(nodes, model_id, "MODEL_DATA writeback")?;
            require_kind(
                model,
                WorkKind::ObservationReadWriteback,
                "MODEL_DATA writeback",
            )?;
            // An existing MODEL_DATA column is overwritten in place and
            // requires no new persistent capacity. Creation plans carry a
            // FinalOutput claim, but the transaction law requires only the
            // bounded write buffer and terminal I/O fence common to both
            // physical operations.
            require_claim(
                model,
                is_writeback_buffer,
                "writeback buffer",
                "MODEL_DATA writeback",
            )?;
            if !model.fences.contains(&FenceKind::Io) {
                return invalid(format!(
                    "MODEL_DATA writeback node {} omits its terminal I/O fence",
                    model.id.as_str()
                ));
            }
            for completion in completion_events(preparation) {
                require_precedes(nodes, &completion, model_id, "final-model preparation")?;
            }
        }
    }

    let commit = require_node(nodes, &work.commit, "atomic commit")?;
    require_kind(commit, WorkKind::Publication, "atomic commit")?;
    require_claim(
        commit,
        is_staged_output,
        "staged-output storage",
        "atomic commit",
    )?;
    require_claim(
        commit,
        is_publication_buffer,
        "publication buffer",
        "atomic commit",
    )?;
    require_exact_lock_count(commit, read_sources, "atomic commit")?;
    if !commit.fences.contains(&FenceKind::Publication) {
        return invalid(format!(
            "atomic commit node {} omits its publication fence",
            commit.id.as_str()
        ));
    }
    for staged in staged_nodes {
        if !shares_staging_demand(staged, commit) {
            return invalid(format!(
                "staging node {} and atomic commit node {} do not share a staged-output demand",
                staged.id.as_str(),
                commit.id.as_str()
            ));
        }
    }

    for completion in &initial_completions {
        require_precedes(
            nodes,
            completion,
            work.post_replay_reconciliation
                .as_ref()
                .expect("reconstruction transaction has reconciliation"),
            "initial consistency",
        )?;
    }
    for product in &work.product_staging {
        let producer = event_node(product);
        for completion in &reconciliation_completions {
            require_precedes(nodes, completion, producer, "post-replay reconciliation")?;
        }
        require_precedes(nodes, product, &work.commit, "product staging")?;
    }
    if let Some(model) = &work.model_column_writeback {
        for completion in completion_events(&nodes[model]) {
            require_precedes(
                nodes,
                &completion,
                work.post_replay_reconciliation
                    .as_ref()
                    .expect("reconstruction transaction has reconciliation"),
                "terminal MODEL_DATA replay",
            )?;
            require_precedes(nodes, &completion, &work.commit, "MODEL_DATA writeback")?;
        }
    }
    for node in nodes.values().filter(|node| node.id != work.commit) {
        for completion in completion_events(node) {
            require_precedes(
                nodes,
                &completion,
                &work.commit,
                "atomic commit terminal ordering",
            )?;
        }
    }
    Ok(observation_reads)
}

fn validate_product_publication_nodes(
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    work: &ObservationTransactionWork,
) -> Result<BTreeSet<WorkDependency>, ObservationTransactionPlanError> {
    if work.post_replay_reconciliation.is_some()
        || work.final_model_preparation.is_some()
        || work.model_column_writeback.is_some()
    {
        return invalid(
            "conventional product publication declares reconstruction or MODEL_DATA work",
        );
    }
    if let Some(node) = nodes.values().find(|node| {
        node.kind.reads_observation()
            || node
                .claims
                .iter()
                .any(|claim| matches!(claim.resource, LeaseResource::MeasurementSetLock { .. }))
    }) {
        return invalid(format!(
            "conventional product publication node {} reads or locks a MeasurementSet",
            node.id.as_str()
        ));
    }

    let initial = require_node(nodes, &work.initial_consistency_check, "publication check")?;
    require_kind(initial, WorkKind::DataCensus, "publication check")?;
    require_exact_lock_count(initial, 0, "publication check")?;
    let initial_completions = completion_events(initial);

    let mut staged_nodes = Vec::new();
    for producer in
        require_exact_completion_events(nodes, &work.product_staging, "product staging")?
    {
        require_kind(producer, WorkKind::Serialization, "product staging")?;
        require_claim(
            producer,
            is_staged_output,
            "staged-output storage",
            "product staging",
        )?;
        for completion in &initial_completions {
            require_precedes(nodes, completion, &producer.id, "publication check")?;
        }
        staged_nodes.push(producer);
    }

    let commit = require_node(nodes, &work.commit, "atomic member commit")?;
    require_kind(commit, WorkKind::Publication, "atomic member commit")?;
    require_claim(
        commit,
        is_staged_output,
        "staged-output storage",
        "atomic member commit",
    )?;
    require_claim(
        commit,
        is_publication_buffer,
        "publication buffer",
        "atomic member commit",
    )?;
    require_exact_lock_count(commit, 0, "atomic member commit")?;
    if !commit.fences.contains(&FenceKind::Publication) {
        return invalid("atomic member commit omits its publication fence");
    }
    for staged in staged_nodes {
        if !shares_staging_demand(staged, commit) {
            return invalid(format!(
                "staging node {} and atomic member commit {} do not share staged output",
                staged.id.as_str(),
                commit.id.as_str()
            ));
        }
    }
    for product in &work.product_staging {
        require_precedes(nodes, product, &work.commit, "product staging")?;
    }
    for node in nodes.values().filter(|node| node.id != work.commit) {
        for completion in completion_events(node) {
            require_precedes(
                nodes,
                &completion,
                &work.commit,
                "atomic member commit terminal ordering",
            )?;
        }
    }
    Ok(BTreeSet::new())
}

fn derive_observation_reads(
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    initial: &WorkNode,
    commit: &WorkNodeId,
    model_column_writeback: Option<&WorkNodeId>,
) -> Result<BTreeSet<WorkDependency>, ObservationTransactionPlanError> {
    let mut completions = BTreeSet::new();
    for node in nodes.values() {
        let holds_measurement_set_lock = node
            .claims
            .iter()
            .any(|claim| matches!(claim.resource, LeaseResource::MeasurementSetLock { .. }));
        if node.kind.reads_observation() {
            completions.extend(completion_events(node));
        } else if holds_measurement_set_lock
            && node.id != initial.id
            && &node.id != commit
            && model_column_writeback != Some(&node.id)
            && !(node.kind == WorkKind::Release
                && node.claims.iter().all(|claim| {
                    !matches!(claim.resource, LeaseResource::MeasurementSetLock { .. })
                        || claim.lifetime == ClaimLifetime::retained_until(node.id.clone())
                }))
        {
            return invalid(format!(
                "node {} declares a MeasurementSet lock without the observation-read role",
                node.id.as_str()
            ));
        }
    }
    Ok(completions)
}

fn require_node<'a>(
    nodes: &'a BTreeMap<WorkNodeId, WorkNode>,
    id: &WorkNodeId,
    role: &str,
) -> Result<&'a WorkNode, ObservationTransactionPlanError> {
    nodes
        .get(id)
        .ok_or_else(|| ObservationTransactionPlanError::InvalidPlan {
            reason: format!("{role} node {} is absent", id.as_str()),
        })
}

fn require_event<'a>(
    nodes: &'a BTreeMap<WorkNodeId, WorkNode>,
    event: &WorkDependency,
    role: &str,
) -> Result<&'a WorkNode, ObservationTransactionPlanError> {
    let producer = require_node(nodes, event_node(event), role)?;
    if let WorkDependency::Fence(fence) = event
        && !producer.fences.contains(&fence.kind())
    {
        return invalid(format!(
            "{role} references undeclared {:?} fence on node {}",
            fence.kind(),
            producer.id.as_str()
        ));
    }
    Ok(producer)
}

fn completion_events(node: &WorkNode) -> BTreeSet<WorkDependency> {
    if node.fences.is_empty() {
        BTreeSet::from([WorkDependency::Work(node.id.clone())])
    } else {
        node.fences
            .iter()
            .map(|kind| WorkDependency::Fence(FenceId::new(node.id.clone(), *kind)))
            .collect()
    }
}

fn require_exact_completion_events<'a>(
    nodes: &'a BTreeMap<WorkNodeId, WorkNode>,
    declared: &BTreeSet<WorkDependency>,
    role: &str,
) -> Result<Vec<&'a WorkNode>, ObservationTransactionPlanError> {
    let mut producers = BTreeMap::<WorkNodeId, &WorkNode>::new();
    for event in declared {
        let producer = require_event(nodes, event, role)?;
        producers.insert(producer.id.clone(), producer);
    }
    for producer in producers.values() {
        let expected = completion_events(producer);
        let actual = declared
            .iter()
            .filter(|event| event_node(event) == &producer.id)
            .cloned()
            .collect::<BTreeSet<_>>();
        if actual != expected {
            return invalid(format!(
                "{role} node {} completion events {actual:?} do not match terminal events {expected:?}",
                producer.id.as_str()
            ));
        }
    }
    Ok(producers.into_values().collect())
}

fn require_kind(
    node: &WorkNode,
    expected: WorkKind,
    role: &str,
) -> Result<(), ObservationTransactionPlanError> {
    if node.kind == expected {
        Ok(())
    } else {
        invalid(format!(
            "{role} node {} has kind {:?}, expected {expected:?}",
            node.id.as_str(),
            node.kind
        ))
    }
}

fn require_claim(
    node: &WorkNode,
    predicate: fn(&LeaseResource) -> bool,
    resource: &str,
    role: &str,
) -> Result<(), ObservationTransactionPlanError> {
    if node.claims.iter().any(|claim| predicate(&claim.resource)) {
        Ok(())
    } else {
        invalid(format!("{role} node {} omits {resource}", node.id.as_str()))
    }
}

fn require_exact_lock_count(
    node: &WorkNode,
    required: usize,
    role: &str,
) -> Result<(), ObservationTransactionPlanError> {
    let mut measurement_sets = BTreeSet::new();
    for claim in &node.claims {
        match claim.resource {
            LeaseResource::MeasurementSetLock { measurement_set } => {
                if claim.amount != 1 || !measurement_sets.insert(measurement_set) {
                    return invalid(format!(
                        "{role} node {} has an ambiguous MeasurementSet lock claim",
                        node.id.as_str()
                    ));
                }
            }
            LeaseResource::Locks => {
                return invalid(format!(
                    "{role} node {} uses an unscoped aggregate lock claim",
                    node.id.as_str()
                ));
            }
            _ => {}
        }
    }
    if measurement_sets.len() != required {
        return invalid(format!(
            "{role} node {} reserves {} exact MeasurementSet locks, expected {required}",
            node.id.as_str(),
            measurement_sets.len()
        ));
    }
    Ok(())
}

fn validate_measurement_set_lock_identities(
    required: &BTreeSet<MeasurementSetIdentity>,
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    work: &ObservationTransactionWork,
) -> Result<(), ObservationTransactionPlanError> {
    let mut lock_nodes = vec![
        require_node(
            nodes,
            &work.initial_consistency_check,
            "initial consistency",
        )?,
        require_node(nodes, &work.commit, "atomic commit")?,
    ];
    for read in &work.observation_reads {
        lock_nodes.push(require_event(nodes, read, "observation read")?);
    }
    lock_nodes.extend(nodes.values().filter(|node| {
        node.kind == WorkKind::Release
            && node
                .claims
                .iter()
                .any(|claim| matches!(claim.resource, LeaseResource::MeasurementSetLock { .. }))
    }));
    lock_nodes.sort_unstable_by(|left, right| left.id.cmp(&right.id));
    lock_nodes.dedup_by(|left, right| left.id == right.id);
    for node in lock_nodes {
        let actual = node
            .claims
            .iter()
            .filter_map(|claim| match claim.resource {
                LeaseResource::MeasurementSetLock { measurement_set } => Some(measurement_set),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        if &actual != required {
            return invalid(format!(
                "transaction node {} MeasurementSet locks {actual:?} do not match read-set identities {required:?}",
                node.id.as_str()
            ));
        }
    }
    Ok(())
}

fn require_precedes(
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    prerequisite: &WorkDependency,
    target: &WorkNodeId,
    role: &str,
) -> Result<(), ObservationTransactionPlanError> {
    let mut visited = BTreeSet::new();
    if event_precedes(nodes, prerequisite, target, &mut visited) {
        Ok(())
    } else {
        invalid(format!(
            "{role} event does not precede node {}",
            target.as_str()
        ))
    }
}

fn event_precedes(
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    prerequisite: &WorkDependency,
    target: &WorkNodeId,
    visited: &mut BTreeSet<WorkNodeId>,
) -> bool {
    if !visited.insert(target.clone()) {
        return false;
    }
    let Some(node) = nodes.get(target) else {
        return false;
    };
    node.dependencies.contains(prerequisite)
        || node
            .dependencies
            .iter()
            .any(|dependency| event_precedes(nodes, prerequisite, event_node(dependency), visited))
}

fn event_node(event: &WorkDependency) -> &WorkNodeId {
    match event {
        WorkDependency::Work(node) => node,
        WorkDependency::Fence(fence) => fence.node(),
    }
}

fn is_staged_output(resource: &LeaseResource) -> bool {
    matches!(
        resource,
        LeaseResource::Storage {
            use_kind: StorageUseKind::StagedOutput,
            ..
        }
    )
}

fn is_writeback_buffer(resource: &LeaseResource) -> bool {
    matches!(resource, LeaseResource::IoBuffer(IoBufferKind::Writeback))
}

fn is_publication_buffer(resource: &LeaseResource) -> bool {
    matches!(resource, LeaseResource::IoBuffer(IoBufferKind::Publication))
}

fn shares_staging_demand(left: &WorkNode, right: &WorkNode) -> bool {
    left.claims.iter().any(|left_claim| {
        let LeaseResource::Storage {
            demand_id: left_id,
            use_kind: StorageUseKind::StagedOutput,
        } = &left_claim.resource
        else {
            return false;
        };
        right.claims.iter().any(|right_claim| {
            matches!(
                &right_claim.resource,
                LeaseResource::Storage {
                    demand_id: right_id,
                    use_kind: StorageUseKind::StagedOutput,
                } if right_id == left_id
            )
        })
    })
}

fn invalid<T>(reason: impl Into<String>) -> Result<T, ObservationTransactionPlanError> {
    Err(ObservationTransactionPlanError::InvalidPlan {
        reason: reason.into(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use casa_imaging_model::{LogicalIdentity, MeasurementSetIdentity};

    use crate::{
        AllocationAccess, AllocationId, AllocationLayout, AllocationLifetime, AllocationPurpose,
        AllocationUse, AlternativeId, CacheDemand, CapabilityPredicate, CapacityDomainId,
        CapacityViewId, ClaimLifetime, CountDemand, DemandAlternative, DemandEnvelope,
        ExecutionDagSpecification, ExecutionKnobs, FenceId, FenceKind, InitializationPolicy,
        IoBufferDemand, IoBufferKind, LeaseResource, LogicalAllocation, MemoryDemand, PhysicalSlot,
        PhysicalSlotId, QueueDemand, QueueResourceId, QuiescencePoint, RateDemand, RateResourceId,
        ResourceClaim, ResourceHeadroom, RuntimeOverheadDemand, ScalingMetadata, SlotCompatibility,
        StorageDemand, StorageDomainId, StorageMode, StorageUseKind, WorkDependency, WorkDomain,
        WorkImplementationId, WorkKind, WorkNode, WorkNodeId,
    };

    use super::*;

    fn claim(resource: LeaseResource) -> ResourceClaim {
        ResourceClaim {
            resource,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        }
    }

    fn measurement_set(byte: u8) -> MeasurementSetIdentity {
        MeasurementSetIdentity::new(LogicalIdentity::from_sha256([byte; 32]))
    }

    fn measurement_set_lock(byte: u8) -> LeaseResource {
        LeaseResource::MeasurementSetLock {
            measurement_set: measurement_set(byte),
        }
    }

    fn node(
        id: &str,
        kind: WorkKind,
        dependencies: BTreeSet<WorkDependency>,
        claims: Vec<ResourceClaim>,
        fences: BTreeSet<FenceKind>,
    ) -> WorkNode {
        let domain = match kind {
            WorkKind::Io
            | WorkKind::ObservationRead
            | WorkKind::ObservationReadWriteback
            | WorkKind::Writeback
            | WorkKind::Publication => WorkDomain::Io,
            _ => WorkDomain::Cpu,
        };
        let lifetime = match &domain {
            WorkDomain::Io => ClaimLifetime::through_fences(fences.iter().copied()),
            _ => ClaimLifetime::Work,
        };
        let mut claims = claims;
        for claim in &mut claims {
            claim.lifetime = lifetime.clone();
        }
        match &domain {
            WorkDomain::Cpu => claims.push(ResourceClaim {
                resource: LeaseResource::Workers,
                amount: 1,
                lifetime: ClaimLifetime::Work,
            }),
            WorkDomain::Io => claims.extend([
                ResourceClaim {
                    resource: LeaseResource::Rate {
                        demand_id: "transaction-io-rate".to_string(),
                    },
                    amount: 1,
                    lifetime: lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Queue {
                        demand_id: "transaction-io-queue".to_string(),
                    },
                    amount: 1,
                    lifetime,
                },
            ]),
            WorkDomain::Control | WorkDomain::Metal { .. } => unreachable!("fixture domain"),
        }
        WorkNode {
            id: WorkNodeId::new(id),
            kind,
            domain,
            implementation: WorkImplementationId::new("transaction-test"),
            dependencies,
            claims,
            allocations: match kind {
                WorkKind::ObservationReadWriteback | WorkKind::Writeback => vec![AllocationUse {
                    allocation: AllocationId::new("writeback-buffer"),
                    lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                }],
                WorkKind::Publication => vec![AllocationUse {
                    allocation: AllocationId::new("publication-buffer"),
                    lifetime: ClaimLifetime::through_fences([
                        FenceKind::Io,
                        FenceKind::Publication,
                    ]),
                }],
                _ => Vec::new(),
            },
            fences,
            quiescence_after: BTreeSet::new(),
        }
    }

    fn transaction_nodes() -> (BTreeMap<WorkNodeId, WorkNode>, ObservationTransactionWork) {
        let initial = WorkNodeId::new("check-initial");
        let read = WorkNodeId::new("read-observation");
        let preparation = WorkNodeId::new("final-model-preparation");
        let reconciliation = WorkNodeId::new("post-replay-reconciliation");
        let product = WorkNodeId::new("stage-products");
        let model = WorkNodeId::new("terminal-model-replay");
        let commit = WorkNodeId::new("commit-side-effects");
        let staged_storage = || LeaseResource::Storage {
            demand_id: "atomic-output".to_string(),
            use_kind: StorageUseKind::StagedOutput,
        };
        let final_storage = || LeaseResource::Storage {
            demand_id: "model-column".to_string(),
            use_kind: StorageUseKind::FinalOutput,
        };
        let model_completion = WorkDependency::Fence(FenceId::new(model.clone(), FenceKind::Io));
        let product_completion = WorkDependency::Work(product.clone());
        let read_completion = WorkDependency::Fence(FenceId::new(read.clone(), FenceKind::Io));
        let nodes = [
            node(
                initial.as_str(),
                WorkKind::DataCensus,
                BTreeSet::new(),
                vec![claim(measurement_set_lock(1))],
                BTreeSet::new(),
            ),
            node(
                preparation.as_str(),
                WorkKind::Compute,
                BTreeSet::from([WorkDependency::Work(initial.clone())]),
                Vec::new(),
                BTreeSet::new(),
            ),
            node(
                read.as_str(),
                WorkKind::ObservationRead,
                BTreeSet::from([WorkDependency::Work(initial.clone())]),
                vec![claim(measurement_set_lock(1))],
                BTreeSet::from([FenceKind::Io]),
            ),
            node(
                model.as_str(),
                WorkKind::ObservationReadWriteback,
                BTreeSet::from([
                    WorkDependency::Work(preparation.clone()),
                    read_completion.clone(),
                ]),
                vec![
                    claim(measurement_set_lock(1)),
                    claim(final_storage()),
                    claim(LeaseResource::IoBuffer(IoBufferKind::Writeback)),
                ],
                BTreeSet::from([FenceKind::Io]),
            ),
            node(
                reconciliation.as_str(),
                WorkKind::Compute,
                BTreeSet::from([model_completion.clone()]),
                Vec::new(),
                BTreeSet::new(),
            ),
            node(
                product.as_str(),
                WorkKind::Serialization,
                BTreeSet::from([WorkDependency::Work(reconciliation.clone())]),
                vec![claim(staged_storage())],
                BTreeSet::new(),
            ),
            node(
                commit.as_str(),
                WorkKind::Publication,
                BTreeSet::from([
                    product_completion.clone(),
                    WorkDependency::Work(reconciliation.clone()),
                ]),
                vec![
                    claim(measurement_set_lock(1)),
                    claim(staged_storage()),
                    claim(LeaseResource::IoBuffer(IoBufferKind::Publication)),
                ],
                BTreeSet::from([FenceKind::Io, FenceKind::Publication]),
            ),
        ]
        .into_iter()
        .collect::<Vec<_>>();
        let compatibility = |layout| SlotCompatibility {
            memory_domain: CapacityDomainId::new("host-memory"),
            views: BTreeSet::from([CapacityViewId::new("host-memory")]),
            alignment_bytes: 1,
            storage_mode: StorageMode::Host,
            layout: AllocationLayout::new(layout),
            initialization: InitializationPolicy::Preserve,
            access: AllocationAccess::ReadWrite,
        };
        let writeback_compatibility = compatibility("writeback-buffer");
        let publication_compatibility = compatibility("publication-buffer");
        let dag = ExecutionDag::new(ExecutionDagSpecification {
            required_resource_capabilities: BTreeSet::new(),
            resource_alternative: DemandAlternative {
                id: AlternativeId::new("transaction-test"),
                capabilities: CapabilityPredicate::default(),
                demand: DemandEnvelope {
                    host_memory_view: CapacityViewId::new("host-memory"),
                    memory: vec![
                        MemoryDemand {
                            allocation_id: "writeback-slot".to_string(),
                            hard_bytes: 1,
                            preferred_bytes: 1,
                            views: vec![CapacityViewId::new("host-memory")],
                        },
                        MemoryDemand {
                            allocation_id: "publication-slot".to_string(),
                            hard_bytes: 1,
                            preferred_bytes: 1,
                            views: vec![CapacityViewId::new("host-memory")],
                        },
                    ],
                    workers: CountDemand::new(1, 1),
                    overhead: RuntimeOverheadDemand::zero(),
                    storage: vec![
                        StorageDemand {
                            demand_id: "atomic-output".to_string(),
                            domain: StorageDomainId::new("atomic-output"),
                            temporary_bytes: 0,
                            staged_output_bytes: 2,
                            final_output_bytes: 0,
                            persistent_cache_bytes: 0,
                            read_rate: CountDemand::zero(),
                            write_rate: CountDemand::zero(),
                            operations_rate: CountDemand::zero(),
                            queue_slots: CountDemand::zero(),
                        },
                        StorageDemand {
                            demand_id: "model-column".to_string(),
                            domain: StorageDomainId::new("atomic-output"),
                            temporary_bytes: 0,
                            staged_output_bytes: 0,
                            final_output_bytes: 1,
                            persistent_cache_bytes: 0,
                            read_rate: CountDemand::zero(),
                            write_rate: CountDemand::zero(),
                            operations_rate: CountDemand::zero(),
                            queue_slots: CountDemand::zero(),
                        },
                    ],
                    rates: vec![RateDemand {
                        demand_id: "transaction-io-rate".to_string(),
                        resource: RateResourceId::new("transaction-io-rate"),
                        amount: CountDemand::new(1, 1),
                    }],
                    caches: CacheDemand::zero(),
                    locks: CountDemand::new(1, 1),
                    file_descriptors: CountDemand::zero(),
                    queues: vec![QueueDemand {
                        demand_id: "transaction-io-queue".to_string(),
                        resource: QueueResourceId::new("transaction-io-queue"),
                        slots: CountDemand::new(1, 1),
                    }],
                    transfers: Vec::new(),
                    accelerators: Vec::new(),
                    io_buffers: IoBufferDemand {
                        writeback_bytes: 1,
                        publication_bytes: 1,
                        ..IoBufferDemand::zero()
                    },
                },
                headroom: ResourceHeadroom::default(),
                scaling: ScalingMetadata {
                    minimum_workers: 1,
                    maximum_workers: 1,
                    maximum_batch_size: 1,
                    maximum_tile_width: 1,
                    maximum_tile_height: 1,
                    maximum_slab_depth: 1,
                    memory_bytes_per_worker: BTreeMap::new(),
                },
                quiescence_points: BTreeSet::from([QuiescencePoint::RunBoundary]),
            },
            nodes,
            logical_allocations: vec![
                LogicalAllocation {
                    id: AllocationId::new("writeback-buffer"),
                    bytes: 1,
                    purpose: AllocationPurpose::IoBuffer(IoBufferKind::Writeback),
                    compatibility: writeback_compatibility.clone(),
                    physical_slot: PhysicalSlotId::new("writeback-slot"),
                    lifetime: AllocationLifetime {
                        acquire_at: model.clone(),
                        release_after: BTreeSet::from([model_completion.clone()]),
                    },
                },
                LogicalAllocation {
                    id: AllocationId::new("publication-buffer"),
                    bytes: 1,
                    purpose: AllocationPurpose::IoBuffer(IoBufferKind::Publication),
                    compatibility: publication_compatibility.clone(),
                    physical_slot: PhysicalSlotId::new("publication-slot"),
                    lifetime: AllocationLifetime {
                        acquire_at: commit.clone(),
                        release_after: BTreeSet::from([
                            WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Io)),
                            WorkDependency::Fence(FenceId::new(
                                commit.clone(),
                                FenceKind::Publication,
                            )),
                        ]),
                    },
                },
            ],
            physical_slots: vec![
                PhysicalSlot {
                    id: PhysicalSlotId::new("writeback-slot"),
                    lease_resource: LeaseResource::Memory {
                        allocation_id: "writeback-slot".to_string(),
                    },
                    capacity_bytes: 1,
                    compatibility: writeback_compatibility,
                },
                PhysicalSlot {
                    id: PhysicalSlotId::new("publication-slot"),
                    lease_resource: LeaseResource::Memory {
                        allocation_id: "publication-slot".to_string(),
                    },
                    capacity_bytes: 1,
                    compatibility: publication_compatibility,
                },
            ],
            initial_knobs: ExecutionKnobs::serial(),
            adaptations: Vec::new(),
        })
        .expect("canonical transaction test DAG");
        let mut work =
            ObservationTransactionWork::new_product_publication(initial, reconciliation, commit)
                .with_final_model_preparation(preparation)
                .with_model_column_writeback(model);
        work.product_staging = BTreeSet::from([product_completion]);
        (dag.nodes().clone(), work)
    }

    #[test]
    fn observation_reads_form_the_mutation_and_failure_cut() {
        let (nodes, work) = transaction_nodes();
        let observation_reads =
            validate_transaction_nodes(1, 1, &nodes, &work).expect("complete transaction cut");
        assert_eq!(
            observation_reads,
            BTreeSet::from([
                WorkDependency::Fence(FenceId::new(
                    WorkNodeId::new("read-observation"),
                    FenceKind::Io,
                )),
                WorkDependency::Fence(FenceId::new(
                    WorkNodeId::new("terminal-model-replay"),
                    FenceKind::Io,
                )),
            ])
        );

        let mut read_before_check = nodes.clone();
        read_before_check
            .get_mut(&WorkNodeId::new("read-observation"))
            .expect("observation read")
            .dependencies
            .clear();
        assert!(validate_transaction_nodes(1, 1, &read_before_check, &work).is_err());

        let mut unlocked_read = nodes.clone();
        unlocked_read
            .get_mut(&WorkNodeId::new("read-observation"))
            .expect("observation read")
            .claims
            .clear();
        assert!(
            validate_transaction_nodes(1, 1, &unlocked_read, &work).is_err(),
            "every observation read must hold all source locks"
        );

        let mut reconcile_before_read = nodes;
        reconcile_before_read
            .get_mut(&WorkNodeId::new("post-replay-reconciliation"))
            .expect("post-replay reconciliation")
            .dependencies
            .clear();
        assert!(validate_transaction_nodes(1, 1, &reconcile_before_read, &work).is_err());
    }

    #[test]
    fn transaction_boundary_rejects_untyped_source_io() {
        let (mut nodes, work) = transaction_nodes();
        let hidden = WorkNodeId::new("hidden-observation-read");
        let hidden_completion = WorkDependency::Fence(FenceId::new(hidden.clone(), FenceKind::Io));
        nodes.insert(
            hidden.clone(),
            node(
                hidden.as_str(),
                WorkKind::Io,
                BTreeSet::from([WorkDependency::Work(WorkNodeId::new("check-initial"))]),
                vec![claim(measurement_set_lock(1))],
                BTreeSet::from([FenceKind::Io]),
            ),
        );
        nodes
            .get_mut(&WorkNodeId::new("post-replay-reconciliation"))
            .expect("post-replay reconciliation")
            .dependencies
            .insert(hidden_completion);

        assert!(
            validate_transaction_nodes(1, 1, &nodes, &work).is_err(),
            "a lock-bearing generic I/O node cannot hide an observation read outside the transaction cut"
        );
    }

    #[test]
    fn transaction_boundary_requires_terminal_atomic_commit() {
        let (mut nodes, work) = transaction_nodes();
        nodes.insert(
            WorkNodeId::new("post-commit-fallible-io"),
            node(
                "post-commit-fallible-io",
                WorkKind::Io,
                BTreeSet::from([WorkDependency::Work(WorkNodeId::new("commit-side-effects"))]),
                Vec::new(),
                BTreeSet::from([FenceKind::Io]),
            ),
        );

        assert!(
            validate_transaction_nodes(1, 1, &nodes, &work).is_err(),
            "the atomic commit cannot precede another fallible completion"
        );
    }

    #[test]
    fn asynchronous_observation_read_cannot_use_its_launch_as_completion() {
        let (mut nodes, work) = transaction_nodes();
        let read = WorkNodeId::new("read-observation");
        let replay = nodes
            .get_mut(&WorkNodeId::new("terminal-model-replay"))
            .expect("terminal model replay");
        replay
            .dependencies
            .remove(&WorkDependency::Fence(FenceId::new(
                read.clone(),
                FenceKind::Io,
            )));
        replay.dependencies.insert(WorkDependency::Work(read));

        assert!(
            validate_transaction_nodes(1, 1, &nodes, &work).is_err(),
            "a fenced observation read must name every terminal fence, not its launch"
        );
    }

    #[test]
    fn asynchronous_initial_check_and_reconciliation_gate_their_terminal_fences() {
        let (mut nodes, work) = transaction_nodes();
        let initial = WorkNodeId::new("check-initial");
        nodes
            .get_mut(&initial)
            .expect("initial check")
            .fences
            .insert(FenceKind::Io);
        assert!(
            validate_transaction_nodes(1, 1, &nodes, &work).is_err(),
            "observation reads cannot start from an asynchronous initial-check launch"
        );

        let (mut nodes, work) = transaction_nodes();
        let reconciliation = WorkNodeId::new("post-replay-reconciliation");
        nodes
            .get_mut(&reconciliation)
            .expect("post-replay reconciliation")
            .fences
            .insert(FenceKind::Device);
        assert!(
            validate_transaction_nodes(1, 1, &nodes, &work).is_err(),
            "product staging cannot start from an asynchronous reconciliation launch"
        );
    }

    #[test]
    fn mutation_cancellation_and_precommit_failures_cannot_reach_visibility() {
        let (mut nodes, work) = transaction_nodes();
        let commit = WorkNodeId::new("commit-side-effects");
        for (failure, event) in [
            (
                "input mutation",
                WorkDependency::Work(WorkNodeId::new("check-initial")),
            ),
            (
                "observation read",
                WorkDependency::Fence(FenceId::new(
                    WorkNodeId::new("read-observation"),
                    FenceKind::Io,
                )),
            ),
            (
                "numerical reconciliation",
                WorkDependency::Work(WorkNodeId::new("post-replay-reconciliation")),
            ),
            (
                "product output",
                WorkDependency::Work(WorkNodeId::new("stage-products")),
            ),
            (
                "model writeback",
                WorkDependency::Fence(FenceId::new(
                    WorkNodeId::new("terminal-model-replay"),
                    FenceKind::Io,
                )),
            ),
        ] {
            assert!(
                event_precedes(&nodes, &event, &commit, &mut BTreeSet::new()),
                "{failure} and cancellation at that cut must block publication"
            );
        }

        nodes.insert(
            WorkNodeId::new("rogue-publication"),
            node(
                "rogue-publication",
                WorkKind::Publication,
                BTreeSet::from([WorkDependency::Work(WorkNodeId::new("check-initial"))]),
                Vec::new(),
                BTreeSet::new(),
            ),
        );
        assert!(
            validate_transaction_nodes(1, 1, &nodes, &work).is_err(),
            "no failure path may expose a partial generation through another publication node"
        );
    }

    #[test]
    fn atomic_model_transaction_requires_declared_resources() {
        let (nodes, work) = transaction_nodes();
        validate_transaction_nodes(1, 1, &nodes, &work).expect("complete transaction resources");

        let mut existing_model_column = nodes.clone();
        existing_model_column
            .get_mut(&WorkNodeId::new("terminal-model-replay"))
            .expect("MODEL_DATA replay")
            .claims
            .retain(|claim| {
                claim.resource
                    != (LeaseResource::Storage {
                        demand_id: "model-column".to_string(),
                        use_kind: StorageUseKind::FinalOutput,
                    })
            });
        validate_transaction_nodes(1, 1, &existing_model_column, &work)
            .expect("overwriting existing MODEL_DATA requires no new capacity");

        for (node_id, resource) in [
            ("check-initial", measurement_set_lock(1)),
            (
                "terminal-model-replay",
                LeaseResource::IoBuffer(IoBufferKind::Writeback),
            ),
            (
                "commit-side-effects",
                LeaseResource::IoBuffer(IoBufferKind::Publication),
            ),
        ] {
            let mut incomplete = nodes.clone();
            incomplete
                .get_mut(&WorkNodeId::new(node_id))
                .expect("fixture node")
                .claims
                .retain(|claim| claim.resource != resource);
            assert!(
                validate_transaction_nodes(1, 1, &incomplete, &work).is_err(),
                "removing {resource:?} from {node_id} must fail"
            );
        }

        let mut no_commit_fence = nodes;
        no_commit_fence
            .get_mut(&WorkNodeId::new("commit-side-effects"))
            .expect("commit node")
            .fences
            .remove(&FenceKind::Publication);
        assert!(validate_transaction_nodes(1, 1, &no_commit_fence, &work).is_err());
    }

    #[test]
    fn atomic_commit_waits_for_complete_staging_in_one_domain() {
        let (nodes, work) = transaction_nodes();
        validate_transaction_nodes(1, 1, &nodes, &work).expect("complete transaction ordering");

        let mut unstaged_product = nodes.clone();
        unstaged_product
            .get_mut(&WorkNodeId::new("stage-products"))
            .expect("product node")
            .claims
            .clear();
        assert!(validate_transaction_nodes(1, 1, &unstaged_product, &work).is_err());

        let mut split_staging_domains = nodes.clone();
        let commit = split_staging_domains
            .get_mut(&WorkNodeId::new("commit-side-effects"))
            .expect("commit node");
        let storage = commit
            .claims
            .iter_mut()
            .find(|claim| {
                matches!(
                    &claim.resource,
                    LeaseResource::Storage {
                        use_kind: StorageUseKind::StagedOutput,
                        ..
                    }
                )
            })
            .expect("staged storage claim");
        storage.resource = LeaseResource::Storage {
            demand_id: "different-output-domain".to_string(),
            use_kind: StorageUseKind::StagedOutput,
        };
        assert!(validate_transaction_nodes(1, 1, &split_staging_domains, &work).is_err());

        let mut early_commit = nodes;
        early_commit
            .get_mut(&WorkNodeId::new("commit-side-effects"))
            .expect("commit node")
            .dependencies
            .clear();
        assert!(validate_transaction_nodes(1, 1, &early_commit, &work).is_err());
    }

    #[test]
    fn multi_ms_transactions_reserve_every_concurrent_table_lock() {
        let (mut nodes, work) = transaction_nodes();
        assert!(validate_transaction_nodes(2, 2, &nodes, &work).is_err());

        for node_id in [
            "check-initial",
            "read-observation",
            "terminal-model-replay",
            "commit-side-effects",
        ] {
            nodes
                .get_mut(&WorkNodeId::new(node_id))
                .expect("lock-owning node")
                .claims
                .push(claim(measurement_set_lock(2)));
        }
        validate_transaction_nodes(2, 2, &nodes, &work)
            .expect("one concurrent table lock per MeasurementSet");
    }

    #[test]
    fn transaction_lock_claims_are_exact_and_unambiguous() {
        let (mut excess, work) = transaction_nodes();
        excess
            .get_mut(&WorkNodeId::new("check-initial"))
            .expect("initial check")
            .claims
            .iter_mut()
            .find(|claim| matches!(claim.resource, LeaseResource::MeasurementSetLock { .. }))
            .expect("lock claim")
            .amount = 2;
        assert!(
            validate_transaction_nodes(1, 1, &excess, &work).is_err(),
            "one MeasurementSet cannot be represented by an excess lock claim"
        );

        let (mut ambiguous, work) = transaction_nodes();
        ambiguous
            .get_mut(&WorkNodeId::new("check-initial"))
            .expect("initial check")
            .claims
            .push(claim(measurement_set_lock(1)));
        assert!(
            validate_transaction_nodes(1, 1, &ambiguous, &work).is_err(),
            "multiple aggregate lock claims do not identify one exact per-MS lock set"
        );

        let (mut wrong_identity, work) = transaction_nodes();
        for node_id in ["check-initial", "read-observation", "commit-side-effects"] {
            let claim = wrong_identity
                .get_mut(&WorkNodeId::new(node_id))
                .expect("lock-owning node")
                .claims
                .iter_mut()
                .find(|claim| matches!(claim.resource, LeaseResource::MeasurementSetLock { .. }))
                .expect("MeasurementSet lock claim");
            claim.resource = measurement_set_lock(9);
        }
        assert!(
            validate_measurement_set_lock_identities(
                &BTreeSet::from([measurement_set(1)]),
                &wrong_identity,
                &work,
            )
            .is_err(),
            "lock count alone cannot substitute another MeasurementSet identity"
        );
    }

    #[test]
    fn model_writeback_presence_matches_the_logical_write_set() {
        let (nodes, writable) = transaction_nodes();
        validate_transaction_nodes(1, 1, &nodes, &writable).expect("writable transaction");
        assert!(validate_transaction_nodes(1, 0, &nodes, &writable).is_err());

        let mut read_only = ObservationTransactionWork::new_product_publication(
            writable.initial_consistency_check.clone(),
            writable
                .post_replay_reconciliation
                .clone()
                .expect("writable transaction has reconciliation"),
            writable.commit.clone(),
        );
        read_only.product_staging = writable.product_staging.clone();
        let mut read_only_nodes = nodes.clone();
        read_only_nodes.remove(&WorkNodeId::new("final-model-preparation"));
        read_only_nodes.remove(&WorkNodeId::new("terminal-model-replay"));
        read_only_nodes
            .get_mut(&WorkNodeId::new("post-replay-reconciliation"))
            .expect("post-replay reconciliation")
            .dependencies = BTreeSet::from([WorkDependency::Fence(FenceId::new(
            WorkNodeId::new("read-observation"),
            FenceKind::Io,
        ))]);
        validate_transaction_nodes(1, 0, &read_only_nodes, &read_only)
            .expect("read-only transaction");
        assert!(validate_transaction_nodes(1, 1, &nodes, &read_only).is_err());
    }

    #[test]
    fn commit_cannot_bypass_reconciliation_or_replay() {
        let (nodes, work) = transaction_nodes();
        for node_id in ["stage-products", "terminal-model-replay"] {
            let mut bypass = nodes.clone();
            bypass
                .get_mut(&WorkNodeId::new(node_id))
                .expect("pre-commit node")
                .dependencies
                .clear();
            assert!(validate_transaction_nodes(1, 1, &bypass, &work).is_err());
        }

        let mut premature_product_visibility = nodes;
        premature_product_visibility
            .get_mut(&WorkNodeId::new("stage-products"))
            .expect("product staging node")
            .kind = WorkKind::Publication;
        assert!(validate_transaction_nodes(1, 1, &premature_product_visibility, &work).is_err());
    }

    #[test]
    fn post_replay_reconciliation_waits_for_model_io_completion() {
        let (mut nodes, work) = transaction_nodes();
        nodes
            .get_mut(&WorkNodeId::new("post-replay-reconciliation"))
            .expect("post-replay reconciliation")
            .dependencies
            .remove(&WorkDependency::Fence(FenceId::new(
                WorkNodeId::new("terminal-model-replay"),
                FenceKind::Io,
            )));

        assert!(
            validate_transaction_nodes(1, 1, &nodes, &work).is_err(),
            "post-replay reconciliation must wait for terminal MODEL_DATA I/O"
        );
    }

    #[test]
    fn terminal_model_replay_requires_immutable_model_preparation() {
        let (mut nodes, work) = transaction_nodes();
        nodes
            .get_mut(&WorkNodeId::new("terminal-model-replay"))
            .expect("terminal model replay")
            .dependencies
            .remove(&WorkDependency::Work(WorkNodeId::new(
                "final-model-preparation",
            )));

        assert!(
            validate_transaction_nodes(1, 1, &nodes, &work).is_err(),
            "terminal replay cannot predict before the exact final model is prepared"
        );
    }

    #[test]
    fn product_staging_names_every_asynchronous_completion() {
        let (mut nodes, work) = transaction_nodes();
        nodes
            .get_mut(&WorkNodeId::new("stage-products"))
            .expect("product staging node")
            .fences
            .insert(FenceKind::Io);

        assert!(
            validate_transaction_nodes(1, 1, &nodes, &work).is_err(),
            "a synchronous work event cannot stand in for a live product fence"
        );
    }
}
