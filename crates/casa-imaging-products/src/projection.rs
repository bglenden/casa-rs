// SPDX-License-Identifier: LGPL-3.0-or-later

//! Canonical projection of a sealed generation into publication.
//!
//! The projection is the only hand-off into the T08 publication
//! choreography: it lists the exact sealed member set once, in canonical
//! order, with the byte layout each member occupies in staging and final
//! storage. Runtime adapters consume it when building their publication
//! layout ledgers and receipt participants.

use crate::authority::SealedContinuumGeneration;
use crate::error::ProductsError;

/// Publication stage of one projected member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationStage {
    /// Durably prepared under its staged identity; not yet visible.
    Prepared,
    /// Promoted through the sole visibility operation.
    Published,
}

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

/// Fail-closed state of an in-flight atomic publication.
///
/// This mirrors the runtime's T08 choreography over the projection alone:
/// preparing stages every member exactly once, visibility promotes the whole
/// set or nothing, and promotion can never be re-entered after failure.
#[derive(Debug, Default)]
pub struct AtomicPublicationAttempt {
    stage: Option<Stage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Prepared,
    Visible,
    Failed,
}

impl AtomicPublicationAttempt {
    /// Durably prepare every projected member before any visibility.
    ///
    /// # Errors
    ///
    /// Fails when the attempt already advanced past preparation.
    pub fn prepare(
        &mut self,
        projection: &PublicationProjection,
    ) -> Result<Vec<(crate::authority::MemberArtifactId, PublicationStage)>, ProductsError> {
        if self.stage.is_some() {
            return Err(ProductsError::ForeignPlannedGeneration);
        }
        self.stage = Some(Stage::Prepared);
        Ok(projection
            .members()
            .iter()
            .map(|member| (member.artifact_id(), PublicationStage::Prepared))
            .collect())
    }

    /// Perform the sole visibility operation over the whole prepared set.
    ///
    /// # Errors
    ///
    /// Requires a prepared attempt; a failed attempt retains fail-closed
    /// `Prepared` evidence and cannot retry.
    pub fn make_visible(&mut self) -> Result<(), ProductsError> {
        match self.stage {
            Some(Stage::Prepared) => {
                self.stage = Some(Stage::Visible);
                Ok(())
            }
            Some(Stage::Failed) | None => Err(ProductsError::ForeignPlannedGeneration),
            Some(Stage::Visible) => Err(ProductsError::ForeignPlannedGeneration),
        }
    }

    /// Record that receipt promotion could not be confirmed.
    ///
    /// # Errors
    ///
    /// Requires a visible attempt.
    pub fn fail_promotion(&mut self) -> Result<(), ProductsError> {
        match self.stage {
            Some(Stage::Visible) => {
                self.stage = Some(Stage::Failed);
                Ok(())
            }
            _ => Err(ProductsError::ForeignPlannedGeneration),
        }
    }

    /// Whether the attempt retains fail-closed prepared evidence.
    #[must_use]
    pub const fn retains_prepared_evidence(&self) -> bool {
        matches!(self.stage, Some(Stage::Failed))
    }
}
