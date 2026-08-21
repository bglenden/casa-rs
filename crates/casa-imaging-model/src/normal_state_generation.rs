// SPDX-License-Identifier: LGPL-3.0-or-later

//! T20-owned commitment to the final complete-data reconciliation.

use std::fmt;

use thiserror::Error;

use crate::{
    CompiledProblem, CompiledProblemId, ModelGeneration, ModelGenerationId,
    NormalEquationContractId, NumericsContractId, ObservationSnapshotId, WeightingGenerationId,
    compiled_problem::CanonicalEncoder,
};

const FINAL_RECONCILIATION_COMMITMENT_IDENTITY_DOMAIN: &[u8] =
    b"casa-rs-final-reconciliation-commitment";
const FINAL_RECONCILIATION_COMMITMENT_IDENTITY_VERSION: u32 = 1;

/// Stable semantic identity of a planned final complete-data reconciliation.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FinalReconciliationCommitmentId([u8; 32]);

impl FinalReconciliationCommitmentId {
    /// Identity schema version used by the final-reconciliation encoder.
    pub const SCHEMA_VERSION: u32 = FINAL_RECONCILIATION_COMMITMENT_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for FinalReconciliationCommitmentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FinalReconciliationCommitmentId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for FinalReconciliationCommitmentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Immutable owner commitment to reconcile one exact model before products.
#[derive(Debug, Clone)]
pub struct FinalReconciliationCommitment {
    problem_id: CompiledProblemId,
    observation_snapshot_id: ObservationSnapshotId,
    model_generation_id: ModelGenerationId,
    weighting_generation_id: WeightingGenerationId,
    normal_equation_contract_id: NormalEquationContractId,
    numerics_id: NumericsContractId,
}

impl FinalReconciliationCommitment {
    /// Identity schema version used by final-reconciliation commitments.
    pub const SCHEMA_VERSION: u32 = FINAL_RECONCILIATION_COMMITMENT_IDENTITY_VERSION;

    /// Bind the final reconciliation to one compiled problem and authoritative model generation.
    pub fn from_problem_and_model(
        problem: &CompiledProblem,
        model: &ModelGeneration,
    ) -> Result<Self, FinalReconciliationCommitmentError> {
        let authority = model.authority();
        if authority.problem_id != problem.problem_id() {
            return Err(FinalReconciliationCommitmentError::StaleModelContext {
                expected_problem: problem.problem_id(),
                actual_problem: authority.problem_id,
            });
        }
        let expected_snapshot = problem.inputs().observation_snapshot().snapshot_id();
        if authority.observation_snapshot_id != expected_snapshot {
            return Err(
                FinalReconciliationCommitmentError::StaleObservationSnapshot {
                    expected: expected_snapshot,
                    actual: authority.observation_snapshot_id,
                },
            );
        }
        let expected_weighting = problem.normal_equation().weighting().generation_id();
        if authority.weighting_generation_id != expected_weighting {
            return Err(
                FinalReconciliationCommitmentError::StaleWeightingGeneration {
                    expected: expected_weighting,
                    actual: authority.weighting_generation_id,
                },
            );
        }
        let expected_normal_equation = problem.normal_equation().contract_id();
        if authority.normal_equation_contract_id != expected_normal_equation {
            return Err(
                FinalReconciliationCommitmentError::StaleNormalEquationContract {
                    expected: expected_normal_equation,
                    actual: authority.normal_equation_contract_id,
                },
            );
        }
        if authority.numerics_id != problem.numerics_id() {
            return Err(FinalReconciliationCommitmentError::StaleNumericsContract {
                expected: problem.numerics_id(),
                actual: authority.numerics_id,
            });
        }
        Ok(Self {
            problem_id: problem.problem_id(),
            observation_snapshot_id: expected_snapshot,
            model_generation_id: authority.generation_id,
            weighting_generation_id: expected_weighting,
            normal_equation_contract_id: expected_normal_equation,
            numerics_id: problem.numerics_id(),
        })
    }

    /// Return the semantic identity of this exact final reconciliation.
    #[must_use]
    pub fn commitment_id(&self) -> FinalReconciliationCommitmentId {
        final_reconciliation_commitment_id(CommitmentIdentity {
            observation_snapshot: self.observation_snapshot_id.as_bytes(),
            model_generation: self.model_generation_id.as_bytes(),
            weighting_generation: self.weighting_generation_id.as_bytes(),
            normal_equation_contract: self.normal_equation_contract_id.as_bytes(),
            numerics: self.numerics_id.as_bytes(),
        })
    }
}

impl PartialEq for FinalReconciliationCommitment {
    fn eq(&self, other: &Self) -> bool {
        self.problem_id == other.problem_id && self.commitment_id() == other.commitment_id()
    }
}

impl Eq for FinalReconciliationCommitment {}

/// Exact reason a model cannot enter the final complete-data reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FinalReconciliationCommitmentError {
    /// The model belongs to a different compiled problem context.
    #[error(
        "model problem {actual_problem} does not match final-reconciliation problem {expected_problem}"
    )]
    StaleModelContext {
        /// Problem requested for final reconciliation.
        expected_problem: CompiledProblemId,
        /// Problem retained by the supplied model generation.
        actual_problem: CompiledProblemId,
    },
    /// The model was derived from another immutable observation snapshot.
    #[error("model snapshot {actual} does not match expected snapshot {expected}")]
    StaleObservationSnapshot {
        /// Snapshot compiled for final reconciliation.
        expected: ObservationSnapshotId,
        /// Snapshot retained by the supplied model generation.
        actual: ObservationSnapshotId,
    },
    /// The model was derived under another weighting generation.
    #[error("model weighting generation {actual} does not match expected generation {expected}")]
    StaleWeightingGeneration {
        /// Weighting generation compiled for final reconciliation.
        expected: WeightingGenerationId,
        /// Weighting generation retained by the supplied model generation.
        actual: WeightingGenerationId,
    },
    /// The model was derived under another normal-equation contract.
    #[error("model normal-equation contract {actual} does not match expected contract {expected}")]
    StaleNormalEquationContract {
        /// Normal-equation contract compiled for final reconciliation.
        expected: NormalEquationContractId,
        /// Normal-equation contract retained by the supplied model generation.
        actual: NormalEquationContractId,
    },
    /// The model was derived under another numerical contract.
    #[error("model numerics contract {actual} does not match expected contract {expected}")]
    StaleNumericsContract {
        /// Numerical contract compiled for final reconciliation.
        expected: NumericsContractId,
        /// Numerical contract retained by the supplied model generation.
        actual: NumericsContractId,
    },
}

#[derive(Clone, Copy)]
struct CommitmentIdentity {
    observation_snapshot: [u8; 32],
    model_generation: [u8; 32],
    weighting_generation: [u8; 32],
    normal_equation_contract: [u8; 32],
    numerics: [u8; 32],
}

fn final_reconciliation_commitment_id(
    identity: CommitmentIdentity,
) -> FinalReconciliationCommitmentId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(FINAL_RECONCILIATION_COMMITMENT_IDENTITY_DOMAIN);
    encoder.u32(FINAL_RECONCILIATION_COMMITMENT_IDENTITY_VERSION);
    encoder.digest(identity.observation_snapshot);
    encoder.digest(identity.model_generation);
    encoder.digest(identity.weighting_generation);
    encoder.digest(identity.normal_equation_contract);
    encoder.digest(identity.numerics);
    FinalReconciliationCommitmentId(encoder.finish())
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commitment_identity_encodes_every_cross_generation_field() {
        let baseline = CommitmentIdentity {
            observation_snapshot: [1; 32],
            model_generation: [2; 32],
            weighting_generation: [3; 32],
            normal_equation_contract: [4; 32],
            numerics: [5; 32],
        };
        let baseline_id = final_reconciliation_commitment_id(baseline);
        let mutations = [
            CommitmentIdentity {
                observation_snapshot: [9; 32],
                ..baseline
            },
            CommitmentIdentity {
                model_generation: [9; 32],
                ..baseline
            },
            CommitmentIdentity {
                weighting_generation: [9; 32],
                ..baseline
            },
            CommitmentIdentity {
                normal_equation_contract: [9; 32],
                ..baseline
            },
            CommitmentIdentity {
                numerics: [9; 32],
                ..baseline
            },
        ];
        for mutation in mutations {
            assert_ne!(baseline_id, final_reconciliation_commitment_id(mutation));
        }
    }
}
