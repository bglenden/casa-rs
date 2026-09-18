// SPDX-License-Identifier: LGPL-3.0-or-later

//! Planned product inventory and resource accounting, independent of pixel content.

use crate::ArtifactIdentity;
use casa_imaging_model::{CompiledProblem, CompiledProblemId, ProductGraphId, ProductNodeId};
use casa_imaging_products::PlannedContinuumGeneration;
use sha2::{Digest, Sha256};
use std::{error::Error, fmt};

/// One graph member and its planned output size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductPublicationEntry {
    node: ProductNodeId,
    name: String,
    artifact: ArtifactIdentity,
    payload_bytes: u64,
}
impl ProductPublicationEntry {
    /// Graph-local node.
    pub const fn node(&self) -> ProductNodeId {
        self.node
    }
    /// Compiled product suffix.
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Stable routing identity, not a content fingerprint.
    pub const fn artifact(&self) -> ArtifactIdentity {
        self.artifact
    }
    /// Exact binary32 payload size.
    pub const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }
}

/// Invalid publication inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProductPublicationError {
    /// The plan names a foreign problem or graph.
    ForeignGeneration {
        /// Compiled problem expected by the output plan.
        expected_problem: CompiledProblemId,
        /// Product graph expected by the output plan.
        expected_graph: ProductGraphId,
    },
    /// The ordered member inventory is incomplete.
    MemberSetMismatch {
        /// Required graph members.
        expected: usize,
        /// Supplied planned members.
        actual: usize,
    },
    /// A member differs from its compiled graph slot.
    MemberContractMismatch {
        /// Graph node with a mismatched output slot.
        node: ProductNodeId,
    },
}
impl fmt::Display for ProductPublicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid product publication inventory: {self:?}")
    }
}
impl Error for ProductPublicationError {}

/// Ordered inventory used by planning; it conveys no publication authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductPublicationPlan {
    problem_id: CompiledProblemId,
    graph_id: ProductGraphId,
    entries: Box<[ProductPublicationEntry]>,
}
impl ProductPublicationPlan {
    /// Check the planned inventory against the compiled product graph.
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
        let inventory = graph.publication().members();
        if inventory.len() != planned.members().len() {
            return Err(ProductPublicationError::MemberSetMismatch {
                expected: inventory.len(),
                actual: planned.members().len(),
            });
        }
        let mut entries = Vec::with_capacity(inventory.len());
        for (node_id, member) in inventory.iter().zip(planned.members()) {
            let node = &graph.nodes()[node_id.ordinal()];
            if node.node_id() != member.node() || node.name() != Some(member.name()) {
                return Err(ProductPublicationError::MemberContractMismatch {
                    node: member.node(),
                });
            }
            let mut hash = Sha256::new();
            hash.update(b"casa-rs-product-routing-v1");
            hash.update(problem.problem_id().as_bytes());
            hash.update(graph.graph_id().as_bytes());
            hash.update((member.node().ordinal() as u64).to_le_bytes());
            entries.push(ProductPublicationEntry {
                node: member.node(),
                name: member.name().to_owned(),
                artifact: ArtifactIdentity::from_sha256(hash.finalize().into()),
                payload_bytes: member.payload_values() as u64 * 4,
            });
        }
        Ok(Self {
            problem_id: problem.problem_id(),
            graph_id: graph.graph_id(),
            entries: entries.into_boxed_slice(),
        })
    }
    /// Compiled problem owning the inventory.
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem_id
    }
    /// Compiled graph owning the inventory.
    pub const fn graph_id(&self) -> ProductGraphId {
        self.graph_id
    }
    /// Members in publication order.
    pub const fn entries(&self) -> &[ProductPublicationEntry] {
        &self.entries
    }
    /// Look up a member's routing identity.
    pub fn artifact(&self, node: ProductNodeId) -> Option<ArtifactIdentity> {
        self.entries
            .iter()
            .find(|entry| entry.node == node)
            .map(|entry| entry.artifact)
    }
}
