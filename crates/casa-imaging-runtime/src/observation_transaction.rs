// SPDX-License-Identifier: LGPL-3.0-or-later

//! Plan binding for snapshot-consistent imaging side effects.
//!
//! CASA and LibRA write visibility models while holding MeasurementSet table
//! locks. This contract retains their explicit selection and lock semantics.
//! Conventional products retain their independent publication protocol.
//! Selected visibility columns are not plan nodes: the final major-cycle pass
//! writes their cells in place under the MeasurementSet lock and
//! incomplete-write marker.

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
    /// Publish generated conventional products without observation I/O.
    GeneratedProductPublication,
}

/// Exact execution-DAG events that implement one observation transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservationTransactionWork {
    publication_scope: ObservationTransactionPublicationScope,
    source_free_reconstruction: bool,
    initial_consistency_check: Option<WorkNodeId>,
    observation_reads: BTreeSet<WorkDependency>,
    post_replay_reconciliation: Option<WorkNodeId>,
    product_staging: BTreeSet<WorkDependency>,
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
            source_free_reconstruction: false,
            initial_consistency_check: Some(initial_consistency_check),
            observation_reads: BTreeSet::new(),
            post_replay_reconciliation: Some(post_replay_reconciliation),
            product_staging: BTreeSet::new(),
            commit,
        }
    }

    /// Name checkpoints for publication with independent atomic image replacements.
    #[must_use]
    pub const fn new_product_publication(
        initial_consistency_check: WorkNodeId,
        post_replay_reconciliation: WorkNodeId,
        commit: WorkNodeId,
    ) -> Self {
        Self {
            publication_scope: ObservationTransactionPublicationScope::ProductPublication,
            source_free_reconstruction: false,
            initial_consistency_check: Some(initial_consistency_check),
            observation_reads: BTreeSet::new(),
            post_replay_reconciliation: Some(post_replay_reconciliation),
            product_staging: BTreeSet::new(),
            commit,
        }
    }

    /// Name a publication-only transaction over generated products.
    #[must_use]
    pub const fn new_generated_product_publication(commit: WorkNodeId) -> Self {
        Self {
            publication_scope: ObservationTransactionPublicationScope::GeneratedProductPublication,
            source_free_reconstruction: false,
            initial_consistency_check: None,
            observation_reads: BTreeSet::new(),
            post_replay_reconciliation: None,
            product_staging: BTreeSet::new(),
            commit,
        }
    }

    /// Return whether this transaction reconciles only or publishes products.
    #[must_use]
    pub const fn publication_scope(&self) -> ObservationTransactionPublicationScope {
        self.publication_scope
    }

    pub(crate) const fn source_free_reconstruction(&self) -> bool {
        self.source_free_reconstruction
    }

    /// Return the consistency check that must precede observation reads.
    #[must_use]
    pub const fn initial_consistency_check(&self) -> Option<&WorkNodeId> {
        self.initial_consistency_check.as_ref()
    }

    /// Return exact completion events for all physical observation reads.
    ///
    /// Every producer revalidates and consumes the bound read set while
    /// holding one declared table lock per MeasurementSet through this event.
    #[must_use]
    pub const fn observation_reads(&self) -> &BTreeSet<WorkDependency> {
        &self.observation_reads
    }

    /// Return the post-replay Major-Cycle reconciliation node, when this
    /// transaction performs reconstruction.
    #[must_use]
    pub const fn post_replay_reconciliation(&self) -> Option<&WorkNodeId> {
        self.post_replay_reconciliation.as_ref()
    }

    /// Return exact completion events for every privately staged required product.
    #[must_use]
    pub const fn product_staging(&self) -> &BTreeSet<WorkDependency> {
        &self.product_staging
    }

    /// Return the sole node permitted to revalidate and publish side effects.
    ///
    /// For MS-backed transactions, the node holds source locks while it rechecks
    /// exact read preconditions. Generated-product publication takes no MS
    /// locks. Its fence establishes readiness only; the runtime's final publish
    /// call replaces each image atomically, not the whole output set.
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
        ObservationTransactionPublicationScope::GeneratedProductPublication => {
            if declared_products != expected_products {
                return invalid(format!(
                    "generated publication product nodes {declared_products:?} do not match graph members {expected_products:?}"
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
        ObservationTransactionPublicationScope::GeneratedProductPublication => {
            if work.product_staging.is_empty() {
                return invalid("generated product publication layout is empty");
            }
        }
    }
    let read_sources = if work.source_free_reconstruction {
        0
    } else {
        contract.read_set().sources().len()
    };
    work.observation_reads = validate_transaction_nodes(read_sources, dag.nodes(), &work)?;
    if work.publication_scope != ObservationTransactionPublicationScope::GeneratedProductPublication
        && !work.source_free_reconstruction
    {
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
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    work: &ObservationTransactionWork,
) -> Result<BTreeSet<WorkDependency>, ObservationTransactionPlanError> {
    if work.publication_scope == ObservationTransactionPublicationScope::GeneratedProductPublication
    {
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
        work.initial_consistency_check.as_ref().ok_or_else(|| {
            ObservationTransactionPlanError::InvalidPlan {
                reason: "observation transaction lacks initial consistency check".into(),
            }
        })?,
        "initial consistency",
    )?;
    require_kind(initial, WorkKind::DataCensus, "initial consistency")?;
    require_exact_lock_count(initial, read_sources, "initial consistency")?;
    let initial_completions = completion_events(initial);

    let observation_reads = derive_observation_reads(nodes, initial, &work.commit)?;
    if observation_reads.is_empty() && !work.source_free_reconstruction {
        return invalid("observation read event set is empty");
    }
    if !observation_reads.is_empty() && work.source_free_reconstruction {
        return invalid("source-free reconstruction declares an observation read");
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
    if work.post_replay_reconciliation.is_some() {
        return invalid("conventional product publication declares reconstruction");
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

    if work.initial_consistency_check.is_some() {
        return invalid("generated publication declares an observation consistency check");
    }

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
            work.initial_consistency_check.as_ref().ok_or_else(|| {
                ObservationTransactionPlanError::InvalidPlan {
                    reason: "observation transaction lacks initial consistency check".into(),
                }
            })?,
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
mod tests;
