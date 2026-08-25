// SPDX-License-Identifier: LGPL-3.0-or-later

//! Planned-generation publication authority for the T08 atomic publication path.
//!
//! Physical planning consumes only a [`PlannedContinuumGeneration`], so resource
//! admission and staging layout never depend on post-completion content. After
//! the selected implementation has produced and sealed the generation, the
//! runtime validates its [`PublicationProjection`] against this immutable plan
//! and carries the resulting affine authorization into the sole atomic publish
//! call. Receipts consequently distinguish stable planned identities from the
//! seal- and content-bound identities observed at execution.

use sha2::{Digest, Sha256};
use std::error::Error;
use std::fmt;

use casa_imaging_model::{CompiledProblem, CompiledProblemId, ProductGraphId, ProductNodeId};
use casa_imaging_products::{
    ContinuumSealId, MemberArtifactId, PlannedContinuumGeneration, PlannedGenerationId,
    PublicationProjection,
};

use crate::ArtifactIdentity;

/// Domain and version of the plan-visible artifact identity derivation.
const PLANNED_ARTIFACT_DOMAIN: &[u8] = b"casa-rs-planned-product-artifact";
const PLANNED_ARTIFACT_VERSION: u32 = 1;

/// Domain and version of the seal-bound observed identity derivation.
const SEALED_ARTIFACT_DOMAIN: &[u8] = b"casa-rs-sealed-product-artifact";
const SEALED_ARTIFACT_VERSION: u32 = 1;

/// One plan-bound publication entry for one graph publication member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductPublicationEntry {
    node: ProductNodeId,
    name: String,
    artifact: ArtifactIdentity,
    payload_bytes: u64,
}

impl ProductPublicationEntry {
    /// Return the graph-local node identity.
    #[must_use]
    pub const fn node(&self) -> ProductNodeId {
        self.node
    }

    /// Return the compiled product name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the generation- and planned-member-bound identity.
    #[must_use]
    pub const fn artifact(&self) -> ArtifactIdentity {
        self.artifact
    }

    /// Return the exact binary32 payload byte count.
    #[must_use]
    pub const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }
}

/// Failure to bind one sealed generation into publication planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProductPublicationError {
    /// The planned or sealed generation belongs to another problem or graph.
    ForeignGeneration {
        /// Problem named by the compiled publication plan.
        expected_problem: CompiledProblemId,
        /// Graph named by the compiled problem.
        expected_graph: ProductGraphId,
    },
    /// The planned or completed member set does not match the graph publication set.
    MemberSetMismatch {
        /// Number of graph publication members.
        expected: usize,
        /// Number of members offered for publication.
        actual: usize,
    },
    /// The sealed projection names another planned generation.
    GenerationMismatch {
        /// Generation sealed into the physical plan.
        expected: PlannedGenerationId,
        /// Generation named by the completed projection.
        actual: PlannedGenerationId,
    },
    /// A projected member does not occupy its exact planned slot.
    MemberContractMismatch {
        /// Graph-local node whose planned contract was not preserved.
        node: ProductNodeId,
    },
    /// Staging evidence does not match the authorized seal and content identity.
    ArtifactEvidenceMismatch {
        /// Graph-local member whose evidence was absent or mismatched.
        node: ProductNodeId,
    },
    /// Native product publication reached its gate without a completed projection.
    MissingProjection,
    /// A projection was supplied to a plan without native product authority.
    UnexpectedProjection,
}

impl fmt::Display for ProductPublicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ForeignGeneration { .. } => {
                formatter.write_str("product generation belongs to another problem or graph")
            }
            Self::MemberSetMismatch { expected, actual } => write!(
                formatter,
                "product publication member count mismatch: expected {expected}, got {actual}"
            ),
            Self::GenerationMismatch { .. } => {
                formatter.write_str("completed product generation does not match its plan")
            }
            Self::MemberContractMismatch { node } => write!(
                formatter,
                "product publication member {} does not match its plan",
                node.ordinal()
            ),
            Self::ArtifactEvidenceMismatch { node } => write!(
                formatter,
                "staged product evidence does not authorize node {}",
                node.ordinal()
            ),
            Self::MissingProjection => {
                formatter.write_str("native product publication omitted its completed projection")
            }
            Self::UnexpectedProjection => formatter.write_str(
                "a product projection was supplied without native publication authority",
            ),
        }
    }
}

impl Error for ProductPublicationError {}

/// Exact-once publication plan derived before member production.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductPublicationPlan {
    problem_id: CompiledProblemId,
    graph_id: ProductGraphId,
    generation_id: PlannedGenerationId,
    entries: Box<[ProductPublicationEntry]>,
}

impl ProductPublicationPlan {
    /// Bind one planned generation against its compiled product graph.
    ///
    /// # Errors
    ///
    /// Fails closed when the plan names another graph or its member set is not
    /// exactly the graph publication set in canonical order.
    pub fn bind(
        problem: &CompiledProblem,
        planned: &PlannedContinuumGeneration,
    ) -> Result<Self, ProductPublicationError> {
        let graph = problem.product_graph();
        if planned.problem_id() != problem.problem_id() || planned.graph_id() != graph.graph_id() {
            return Err(ProductPublicationError::ForeignGeneration {
                expected_problem: problem.problem_id(),
                expected_graph: graph.graph_id(),
            });
        }
        let publication = graph.publication().members();
        let members = planned.members();
        if publication.len() != members.len() {
            return Err(ProductPublicationError::MemberSetMismatch {
                expected: publication.len(),
                actual: members.len(),
            });
        }
        let mut entries = Vec::with_capacity(members.len());
        for (node_ordinal, member) in publication.iter().zip(members) {
            let Some(node) = graph.nodes().get(node_ordinal.ordinal()) else {
                return Err(ProductPublicationError::ForeignGeneration {
                    expected_problem: problem.problem_id(),
                    expected_graph: graph.graph_id(),
                });
            };
            if node.node_id() != member.node() || node.name() != Some(member.name()) {
                return Err(ProductPublicationError::ForeignGeneration {
                    expected_problem: problem.problem_id(),
                    expected_graph: graph.graph_id(),
                });
            }
            entries.push(ProductPublicationEntry {
                node: member.node(),
                name: member.name().to_string(),
                artifact: planned_artifact_identity(planned.generation_id(), member.artifact_id()),
                payload_bytes: u64::try_from(member.payload_values())
                    .expect("payload value count fits in u64 on supported targets")
                    * 4,
            });
        }
        Ok(Self {
            problem_id: problem.problem_id(),
            graph_id: graph.graph_id(),
            generation_id: planned.generation_id(),
            entries: entries.into_boxed_slice(),
        })
    }

    /// Return the exact compiled problem authorized for publication.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem_id
    }

    /// Return the bound product graph identity.
    #[must_use]
    pub const fn graph_id(&self) -> ProductGraphId {
        self.graph_id
    }

    /// Return the planned generation identity bound before execution.
    #[must_use]
    pub const fn generation_id(&self) -> PlannedGenerationId {
        self.generation_id
    }

    /// Return the planned entries in canonical publication order.
    #[must_use]
    pub const fn entries(&self) -> &[ProductPublicationEntry] {
        &self.entries
    }

    /// Return the plan-visible artifact identity of one member.
    #[must_use]
    pub fn artifact(&self, node: ProductNodeId) -> Option<ArtifactIdentity> {
        self.entries
            .iter()
            .find(|entry| entry.node == node)
            .map(|entry| entry.artifact)
    }

    /// Validate a post-completion Product Generation seal against this plan.
    ///
    /// # Errors
    ///
    /// Fails closed on any problem, graph, generation, member, planned-artifact,
    /// payload-size, or ordering substitution.
    pub fn authorize(
        &self,
        projection: &PublicationProjection,
    ) -> Result<ProductPublicationAuthorization, ProductPublicationError> {
        if projection.problem_id() != self.problem_id || projection.graph_id() != self.graph_id {
            return Err(ProductPublicationError::ForeignGeneration {
                expected_problem: self.problem_id,
                expected_graph: self.graph_id,
            });
        }
        if projection.generation_id() != self.generation_id {
            return Err(ProductPublicationError::GenerationMismatch {
                expected: self.generation_id,
                actual: projection.generation_id(),
            });
        }
        if projection.members().len() != self.entries.len() {
            return Err(ProductPublicationError::MemberSetMismatch {
                expected: self.entries.len(),
                actual: projection.members().len(),
            });
        }
        let mut entries = Vec::with_capacity(self.entries.len());
        for (planned, completed) in self.entries.iter().zip(projection.members()) {
            if planned.node() != completed.node()
                || planned.name() != completed.name()
                || planned.payload_bytes() != completed.payload_bytes()
                || planned.artifact()
                    != planned_artifact_identity(self.generation_id, completed.artifact_id())
            {
                return Err(ProductPublicationError::MemberContractMismatch {
                    node: planned.node(),
                });
            }
            entries.push(AuthorizedProductPublicationEntry {
                node: planned.node(),
                planned: planned.artifact(),
                observed: sealed_artifact_identity(
                    projection.seal_id(),
                    completed.artifact_id(),
                    completed.content_identity(),
                ),
                payload_bytes: planned.payload_bytes(),
            });
        }
        Ok(ProductPublicationAuthorization {
            problem_id: self.problem_id,
            graph_id: self.graph_id,
            generation_id: self.generation_id,
            seal_id: projection.seal_id(),
            entries: entries.into_boxed_slice(),
        })
    }
}

/// Runtime-validated authority carried only into the terminal publish call.
#[derive(Debug, PartialEq, Eq)]
pub struct ProductPublicationAuthorization {
    problem_id: CompiledProblemId,
    graph_id: ProductGraphId,
    generation_id: PlannedGenerationId,
    seal_id: ContinuumSealId,
    entries: Box<[AuthorizedProductPublicationEntry]>,
}

impl ProductPublicationAuthorization {
    /// Return the exact compiled problem authorized for publication.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem_id
    }

    /// Return the exact compiler-owned Product Graph authorized for publication.
    #[must_use]
    pub const fn graph_id(&self) -> ProductGraphId {
        self.graph_id
    }

    /// Return the completed Product Generation seal.
    #[must_use]
    pub const fn seal_id(&self) -> ContinuumSealId {
        self.seal_id
    }

    /// Return the exact planned generation authorized for publication.
    #[must_use]
    pub const fn generation_id(&self) -> PlannedGenerationId {
        self.generation_id
    }

    /// Return authorized members in canonical publication order.
    #[must_use]
    pub const fn entries(&self) -> &[AuthorizedProductPublicationEntry] {
        &self.entries
    }

    pub(crate) fn validate_staging(
        &self,
        measurements: &crate::WorkMeasurements,
    ) -> Result<(), ProductPublicationError> {
        for entry in &self.entries {
            let Some(measurement) = measurements
                .artifacts()
                .iter()
                .find(|measurement| measurement.planned_identity() == entry.planned)
            else {
                return Err(ProductPublicationError::ArtifactEvidenceMismatch { node: entry.node });
            };
            if measurement.observed_identity() != Some(entry.observed)
                || measurement.disposition() != crate::ArtifactDisposition::Staged
                || measurement.bytes() != entry.payload_bytes
            {
                return Err(ProductPublicationError::ArtifactEvidenceMismatch { node: entry.node });
            }
        }
        Ok(())
    }
}

/// One validated planned-to-observed publication identity binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorizedProductPublicationEntry {
    node: ProductNodeId,
    planned: ArtifactIdentity,
    observed: ArtifactIdentity,
    payload_bytes: u64,
}

impl AuthorizedProductPublicationEntry {
    /// Return the graph-local publication member.
    #[must_use]
    pub const fn node(self) -> ProductNodeId {
        self.node
    }

    /// Return the stable plan-visible artifact identity.
    #[must_use]
    pub const fn planned_identity(self) -> ArtifactIdentity {
        self.planned
    }

    /// Return the seal- and content-bound observed identity.
    #[must_use]
    pub const fn observed_identity(self) -> ArtifactIdentity {
        self.observed
    }

    /// Return the exact staged payload byte count.
    #[must_use]
    pub const fn payload_bytes(self) -> u64 {
        self.payload_bytes
    }
}

fn planned_artifact_identity(
    generation_id: PlannedGenerationId,
    artifact_id: MemberArtifactId,
) -> ArtifactIdentity {
    let mut hasher = Sha256::new();
    hasher.update(PLANNED_ARTIFACT_DOMAIN);
    hasher.update(PLANNED_ARTIFACT_VERSION.to_le_bytes());
    hasher.update(generation_id.as_bytes());
    hasher.update(artifact_id.as_bytes());
    ArtifactIdentity::from_sha256(hasher.finalize().into())
}

/// Derive one artifact identity from the seal, the planned artifact
/// identity, and the bound content identity.
fn sealed_artifact_identity(
    seal_id: ContinuumSealId,
    artifact_id: MemberArtifactId,
    content_identity: MemberArtifactId,
) -> ArtifactIdentity {
    let mut hasher = Sha256::new();
    hasher.update(SEALED_ARTIFACT_DOMAIN);
    hasher.update(SEALED_ARTIFACT_VERSION.to_le_bytes());
    hasher.update(seal_id.as_bytes());
    hasher.update(artifact_id.as_bytes());
    hasher.update(content_identity.as_bytes());
    let digest: [u8; 32] = hasher.finalize().into();
    ArtifactIdentity::from_sha256(digest)
}
