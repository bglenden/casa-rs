// SPDX-License-Identifier: LGPL-3.0-or-later

//! Seal-bound publication planning for the T08 atomic publication path.
//!
//! This module is the only hand-off from a Product Generation seal into
//! runtime publication planning: it validates the sealed member set against
//! the compiler-owned Product Graph and derives one stable artifact identity
//! per member from the seal, the planned artifact identity, and the bound
//! content identity. Publication layouts, receipts, and terminal promotion
//! therefore carry authority-derived identities instead of adapter-local
//! synthetic ones.

use sha2::{Digest, Sha256};

use casa_imaging_model::{CompiledProblem, ProductGraphId, ProductNodeId};
use casa_imaging_products::{
    ContinuumSealId, PlannedGenerationId, SealedContinuumGeneration, SealedMember,
};

use crate::ArtifactIdentity;

/// Domain and version of the seal-bound artifact identity derivation.
const SEALED_ARTIFACT_DOMAIN: &[u8] = b"casa-rs-sealed-product-artifact";
const SEALED_ARTIFACT_VERSION: u32 = 1;

/// One seal-bound publication entry for one graph publication member.
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

    /// Return the seal-, planned-artifact-, and content-bound identity.
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
    /// The sealed generation belongs to another product graph.
    ForeignSeal {
        /// Graph named by the compiled problem.
        expected: ProductGraphId,
    },
    /// The sealed member set does not match the graph publication set.
    MemberSetMismatch {
        /// Number of graph publication members.
        expected: usize,
        /// Number of sealed members offered for publication.
        actual: usize,
    },
}

/// Exact-once publication plan derived from one authorized generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductPublicationPlan {
    graph_id: ProductGraphId,
    seal_id: ContinuumSealId,
    generation_id: PlannedGenerationId,
    entries: Box<[ProductPublicationEntry]>,
}

impl ProductPublicationPlan {
    /// Bind one authorized generation against its compiled product graph.
    ///
    /// # Errors
    ///
    /// Fails closed when the seal names another graph or the sealed member
    /// set is not exactly the graph publication set in canonical order.
    pub fn bind(
        problem: &CompiledProblem,
        sealed: &SealedContinuumGeneration,
    ) -> Result<Self, ProductPublicationError> {
        let graph = problem.product_graph();
        let publication = graph.publication().members();
        let members = sealed.members();
        if publication.len() != members.len() {
            return Err(ProductPublicationError::MemberSetMismatch {
                expected: publication.len(),
                actual: members.len(),
            });
        }
        let mut entries = Vec::with_capacity(members.len());
        for (node_ordinal, member) in publication.iter().zip(members) {
            let Some(node) = graph.nodes().get(node_ordinal.ordinal()) else {
                return Err(ProductPublicationError::ForeignSeal {
                    expected: graph.graph_id(),
                });
            };
            if node.node_id() != member.node() || node.name() != Some(member.name()) {
                return Err(ProductPublicationError::ForeignSeal {
                    expected: graph.graph_id(),
                });
            }
            entries.push(ProductPublicationEntry {
                node: member.node(),
                name: member.name().to_string(),
                artifact: sealed_artifact_identity(sealed.seal_id(), member),
                payload_bytes: u64::try_from(member.payload().len())
                    .expect("payload length fits in u64 on supported targets")
                    * 4,
            });
        }
        Ok(Self {
            graph_id: graph.graph_id(),
            seal_id: sealed.seal_id(),
            generation_id: sealed.generation_id(),
            entries: entries.into_boxed_slice(),
        })
    }

    /// Return the bound product graph identity.
    #[must_use]
    pub const fn graph_id(&self) -> ProductGraphId {
        self.graph_id
    }

    /// Return the authorizing seal identity.
    #[must_use]
    pub const fn seal_id(&self) -> ContinuumSealId {
        self.seal_id
    }

    /// Return the planned generation identity behind the seal.
    #[must_use]
    pub const fn generation_id(&self) -> PlannedGenerationId {
        self.generation_id
    }

    /// Return the seal-bound entries in canonical publication order.
    #[must_use]
    pub const fn entries(&self) -> &[ProductPublicationEntry] {
        &self.entries
    }

    /// Return the seal-bound artifact identity of one member.
    #[must_use]
    pub fn artifact(&self, node: ProductNodeId) -> Option<ArtifactIdentity> {
        self.entries
            .iter()
            .find(|entry| entry.node == node)
            .map(|entry| entry.artifact)
    }
}

/// Derive one artifact identity from the seal, the planned artifact
/// identity, and the bound content identity.
fn sealed_artifact_identity(seal_id: ContinuumSealId, member: &SealedMember) -> ArtifactIdentity {
    let mut hasher = Sha256::new();
    hasher.update(SEALED_ARTIFACT_DOMAIN);
    hasher.update(SEALED_ARTIFACT_VERSION.to_le_bytes());
    hasher.update(seal_id.as_bytes());
    hasher.update(member.artifact_id().as_bytes());
    hasher.update(member.content_identity().as_bytes());
    let digest: [u8; 32] = hasher.finalize().into();
    ArtifactIdentity::from_sha256(digest)
}
