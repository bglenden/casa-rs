// SPDX-License-Identifier: LGPL-3.0-or-later

//! Canonical projection of a sealed generation into publication.
//!
//! The projection is the only hand-off into the T08 publication
//! choreography: it lists the exact sealed member set once, in canonical
//! order, with the byte layout each member occupies in staging and final
//! storage. The runtime publication path owns the actual state machine:
//! durable preparation, independently atomic member replacements, per-member
//! receipt checkpointing, and retained evidence for failed or uncertain
//! promotions.

use casa_imaging_model::{CompiledProblemId, ProductGraphId, ProductNodeId};

use crate::authority::{
    ContinuumSealId, MemberArtifactId, PlannedGenerationId, SealedContinuumGeneration,
};
use crate::error::ProductsError;

/// One sealed member projected for independently atomic publication.
#[derive(Debug, Clone)]
pub struct PublicationMemberProjection {
    node: ProductNodeId,
    artifact_id: MemberArtifactId,
    content_identity: MemberArtifactId,
    name: String,
    payload_bytes: u64,
}

impl PublicationMemberProjection {
    /// Return the graph-local node identity.
    #[must_use]
    pub const fn node(&self) -> ProductNodeId {
        self.node
    }

    /// Return the sealed artifact identity.
    #[must_use]
    pub const fn artifact_id(&self) -> MemberArtifactId {
        self.artifact_id
    }

    /// Return the exact content identity authorized for this artifact.
    #[must_use]
    pub const fn content_identity(&self) -> MemberArtifactId {
        self.content_identity
    }

    /// Return the compiled product name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the exact binary32 payload byte count.
    #[must_use]
    pub const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }
}

/// Exact-once publication projection of one sealed generation.
#[derive(Debug, Clone)]
pub struct PublicationProjection {
    problem_id: CompiledProblemId,
    graph_id: ProductGraphId,
    generation_id: PlannedGenerationId,
    seal_id: ContinuumSealId,
    members: Box<[PublicationMemberProjection]>,
}

impl PublicationProjection {
    /// Project the exact sealed member set in canonical publication order.
    ///
    /// # Errors
    ///
    /// Rejects a seal without members; the atomic store protocol requires at
    /// least one published artifact.
    pub fn from_sealed(sealed: &SealedContinuumGeneration) -> Result<Self, ProductsError> {
        if sealed.members().is_empty() {
            return Err(ProductsError::UnsupportedProblem);
        }
        let members = sealed
            .members()
            .iter()
            .map(|member| PublicationMemberProjection {
                node: member.node(),
                artifact_id: member.artifact_id(),
                content_identity: member.content_identity(),
                name: member.name().to_string(),
                payload_bytes: u64::try_from(member.payload().len())
                    .expect("payload length fits in u64 on supported targets")
                    * 4,
            })
            .collect::<Box<[_]>>();
        Ok(Self {
            problem_id: sealed.problem_id(),
            graph_id: sealed.graph_id(),
            generation_id: sealed.generation_id(),
            seal_id: sealed.seal_id(),
            members,
        })
    }

    /// Return the exact compiled problem authorized for publication.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem_id
    }

    /// Return the exact compiled Product Graph authorized for publication.
    #[must_use]
    pub const fn graph_id(&self) -> ProductGraphId {
        self.graph_id
    }

    /// Return the planned generation authorized by the seal.
    #[must_use]
    pub const fn generation_id(&self) -> PlannedGenerationId {
        self.generation_id
    }

    /// Return the exact Product Generation seal.
    #[must_use]
    pub const fn seal_id(&self) -> ContinuumSealId {
        self.seal_id
    }

    /// Return projected members in canonical publication order.
    #[must_use]
    pub const fn members(&self) -> &[PublicationMemberProjection] {
        &self.members
    }

    /// Return aggregate staged (and final) payload bytes across all members.
    #[must_use]
    pub const fn total_payload_bytes(&self) -> u64 {
        let mut total = 0_u64;
        let mut index = 0;
        while index < self.members.len() {
            total += self.members[index].payload_bytes();
            index += 1;
        }
        total
    }
}
