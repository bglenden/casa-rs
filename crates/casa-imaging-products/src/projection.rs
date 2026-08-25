// SPDX-License-Identifier: LGPL-3.0-or-later

//! Canonical projection of a sealed generation into publication.
//!
//! The projection is the only hand-off into the T08 publication
//! choreography: it lists the exact sealed member set once, in canonical
//! order, with the byte layout each member occupies in staging and final
//! storage. The runtime publication path owns the actual state machine:
//! durable preparation, the sole visibility operation, terminal receipt
//! promotion, and retained Prepared evidence on uncertain promotion.

use crate::authority::SealedContinuumGeneration;
use crate::error::ProductsError;

/// One sealed member projected for atomic publication.
#[derive(Debug, Clone)]
pub struct PublicationMemberProjection {
    artifact_id: crate::authority::MemberArtifactId,
    name: String,
    payload_bytes: u64,
}

impl PublicationMemberProjection {
    /// Return the sealed artifact identity.
    #[must_use]
    pub const fn artifact_id(&self) -> crate::authority::MemberArtifactId {
        self.artifact_id
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
    seal_id: crate::authority::ContinuumSealId,
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
                artifact_id: member.artifact_id(),
                name: member.name().to_string(),
                payload_bytes: u64::try_from(member.payload().len())
                    .expect("payload length fits in u64 on supported targets")
                    * 4,
            })
            .collect::<Box<[_]>>();
        Ok(Self {
            seal_id: sealed.seal_id(),
            members,
        })
    }

    /// Return the seal this projection publishes exactly once.
    #[must_use]
    pub const fn seal_id(&self) -> crate::authority::ContinuumSealId {
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
