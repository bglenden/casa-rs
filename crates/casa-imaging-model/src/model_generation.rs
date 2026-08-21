// SPDX-License-Identifier: LGPL-3.0-or-later

//! Compiler-bound model-generation commitments and coefficient membership.

use std::{collections::BTreeSet, fmt};

use thiserror::Error;

use crate::{
    CompiledImageDomain, CompiledProblem, CompiledProblemId, ImageDomainRole,
    ModelCoefficientSpace, ModelInnerProduct, ModelStateIdentity, NormalEquationContractId,
    NumericsContractId, ObservationSnapshotId, PolarizationCoordinate, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionControls, WeightingGenerationId,
    compiled_problem::{
        CanonicalEncoder, polarization_tag, reconstruction_algorithm_tag, reconstruction_basis_tag,
    },
};

const MODEL_GENERATION_COMMITMENT_IDENTITY_DOMAIN: &[u8] = b"casa-rs-model-generation-commitment";
const MODEL_GENERATION_COMMITMENT_IDENTITY_VERSION: u32 = 1;
const MODEL_GENERATION_IDENTITY_DOMAIN: &[u8] = b"casa-rs-model-generation";
const MODEL_GENERATION_IDENTITY_VERSION: u32 = 1;

const _: () = assert!(usize::BITS <= u64::BITS);

/// Stable compiler-derived identity of model-affecting generation commitments.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModelGenerationCommitmentId([u8; 32]);

impl ModelGenerationCommitmentId {
    /// Identity schema version used by the model-generation commitment encoder.
    pub const SCHEMA_VERSION: u32 = MODEL_GENERATION_COMMITMENT_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for ModelGenerationCommitmentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ModelGenerationCommitmentId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for ModelGenerationCommitmentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelGenerationLineageRoot {
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum ModelCoefficientKind {
    Constant,
    Taylor(u64),
    Channel(u64),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ModelCoefficientKey {
    domain: ImageDomainRole,
    coefficient: ModelCoefficientKind,
    polarization: PolarizationCoordinate,
}

/// Immutable compiler-bound inputs that define one future model-generation lineage.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelGenerationCommitment {
    commitment_id: ModelGenerationCommitmentId,
    problem_id: CompiledProblemId,
    lineage_root: ModelGenerationLineageRoot,
    observation_snapshot_id: ObservationSnapshotId,
    coefficient_space: ModelCoefficientSpace,
    reconstruction_algorithm: ReconstructionAlgorithm,
    reconstruction_controls: ReconstructionControls,
    weighting_generation_id: WeightingGenerationId,
    normal_equation_contract_id: NormalEquationContractId,
    numerics_id: NumericsContractId,
    members: Box<[ModelCoefficientKey]>,
}

impl ModelGenerationCommitment {
    /// Identity schema version used by model-generation commitments.
    pub const SCHEMA_VERSION: u32 = MODEL_GENERATION_COMMITMENT_IDENTITY_VERSION;

    /// Derive one model-generation commitment from immutable compiled authority.
    pub fn from_problem(problem: &CompiledProblem) -> Result<Self, ModelGenerationCommitmentError> {
        let root = problem.inputs().observation_snapshot().model();
        let lineage_root = match root {
            ModelStateIdentity::Empty => ModelGenerationLineageRoot::Empty,
            ModelStateIdentity::Seed(_) | ModelStateIdentity::Generation(_) => {
                return Err(ModelGenerationCommitmentError::UnownedLineageRoot { root });
            }
        };
        let coefficient_space = problem
            .normal_equation()
            .measurement_operator()
            .domain()
            .clone();
        let members = coefficient_members(problem.geometry().domains(), &coefficient_space);
        let reconstruction_algorithm = problem.reconstruction().algorithm().clone();
        let reconstruction_controls = problem.reconstruction().controls();
        let observation_snapshot_id = problem.inputs().observation_snapshot().snapshot_id();
        let weighting_generation_id = problem.normal_equation().weighting().generation_id();
        let normal_equation_contract_id = problem.normal_equation().contract_id();
        let numerics_id = problem.numerics_id();
        let commitment_id = commitment_id(CommitmentIdentity {
            lineage_root,
            observation_snapshot_id,
            coefficient_space: &coefficient_space,
            reconstruction_algorithm: &reconstruction_algorithm,
            reconstruction_controls,
            weighting_generation_id,
            normal_equation_contract_id,
            numerics_id,
            members: &members,
        });
        Ok(Self {
            commitment_id,
            problem_id: problem.problem_id(),
            lineage_root,
            observation_snapshot_id,
            coefficient_space,
            reconstruction_algorithm,
            reconstruction_controls,
            weighting_generation_id,
            normal_equation_contract_id,
            numerics_id,
            members: members.into_boxed_slice(),
        })
    }

    /// Return the model-affecting commitment identity.
    #[must_use]
    pub const fn commitment_id(&self) -> ModelGenerationCommitmentId {
        self.commitment_id
    }

    fn canonical_id(&self) -> ModelGenerationCommitmentId {
        commitment_id(CommitmentIdentity {
            lineage_root: self.lineage_root,
            observation_snapshot_id: self.observation_snapshot_id,
            coefficient_space: &self.coefficient_space,
            reconstruction_algorithm: &self.reconstruction_algorithm,
            reconstruction_controls: self.reconstruction_controls,
            weighting_generation_id: self.weighting_generation_id,
            normal_equation_contract_id: self.normal_equation_contract_id,
            numerics_id: self.numerics_id,
            members: &self.members,
        })
    }
}

/// Exact reason a compiler-bound model-generation commitment could not be derived.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ModelGenerationCommitmentError {
    /// The initial model identity has no landed owner capable of proving its lineage.
    #[error("model generation lineage root {root:?} has no authoritative owner")]
    UnownedLineageRoot {
        /// Raw initial model identity rejected by this first owned-lineage schema.
        root: ModelStateIdentity,
    },
}

/// Stable owner-derived identity of one completed model generation.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModelGenerationId([u8; 32]);

impl ModelGenerationId {
    /// Identity schema version used by the model-generation encoder.
    pub const SCHEMA_VERSION: u32 = MODEL_GENERATION_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for ModelGenerationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ModelGenerationId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for ModelGenerationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ModelCoefficientCompletion {
    member: ModelCoefficientKey,
}

/// Opaque evidence that the compiler-owned empty model covers every coefficient exactly once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelGenerationCompletionEvidence {
    problem_id: CompiledProblemId,
    commitment_id: ModelGenerationCommitmentId,
    coefficients: Box<[ModelCoefficientCompletion]>,
}

impl ModelGenerationCompletionEvidence {
    /// Schema version of empty-model completion evidence.
    pub const SCHEMA_VERSION: u32 = 1;

    /// Complete the exact canonical coefficient catalog as a zero-initialized model.
    #[must_use]
    pub fn empty(commitment: &ModelGenerationCommitment) -> Self {
        let coefficients = commitment
            .members
            .iter()
            .cloned()
            .map(|member| ModelCoefficientCompletion { member })
            .collect();
        Self {
            problem_id: commitment.problem_id,
            commitment_id: commitment.commitment_id,
            coefficients,
        }
    }
}

/// Authoritative immutable owner record for one completed model generation.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelGeneration {
    generation_id: ModelGenerationId,
    commitment: ModelGenerationCommitment,
    completion: ModelGenerationCompletionEvidence,
}

pub(crate) struct ModelGenerationAuthority {
    pub(crate) problem_id: CompiledProblemId,
    pub(crate) generation_id: ModelGenerationId,
    pub(crate) observation_snapshot_id: ObservationSnapshotId,
    pub(crate) weighting_generation_id: WeightingGenerationId,
    pub(crate) normal_equation_contract_id: NormalEquationContractId,
    pub(crate) numerics_id: NumericsContractId,
}

impl ModelGeneration {
    /// Identity schema version used by model generations.
    pub const SCHEMA_VERSION: u32 = MODEL_GENERATION_IDENTITY_VERSION;

    /// Validate completion against its exact compiler-bound commitment and own the generation.
    pub fn complete(
        commitment: &ModelGenerationCommitment,
        completion: ModelGenerationCompletionEvidence,
    ) -> Result<Self, ModelGenerationError> {
        let expected_commitment_id = commitment.canonical_id();
        if commitment.commitment_id != expected_commitment_id {
            return Err(ModelGenerationError::StaleCommitment {
                expected: expected_commitment_id,
                actual: commitment.commitment_id,
            });
        }
        if completion.problem_id != commitment.problem_id {
            return Err(ModelGenerationError::StaleCompletion {
                expected_problem: commitment.problem_id,
                actual_problem: completion.problem_id,
            });
        }
        if completion.commitment_id != expected_commitment_id {
            return Err(ModelGenerationError::StaleCommitment {
                expected: expected_commitment_id,
                actual: completion.commitment_id,
            });
        }
        validate_completion_members(&commitment.members, &completion.coefficients)?;
        let generation_id = model_generation_id(expected_commitment_id, &completion.coefficients);
        Ok(Self {
            generation_id,
            commitment: commitment.clone(),
            completion,
        })
    }

    /// Return the owner-derived generation identity.
    #[must_use]
    pub const fn generation_id(&self) -> ModelGenerationId {
        self.generation_id
    }

    /// Return the compiler-bound commitment completed by this generation.
    #[must_use]
    pub const fn commitment_id(&self) -> ModelGenerationCommitmentId {
        self.commitment.commitment_id
    }

    /// Return the exact compiled problem context retained by this generation.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.commitment.problem_id
    }

    /// Return the number of canonical coefficients owned by this generation.
    #[must_use]
    pub fn coefficient_count(&self) -> usize {
        self.completion.coefficients.len()
    }

    pub(crate) const fn authority(&self) -> ModelGenerationAuthority {
        ModelGenerationAuthority {
            problem_id: self.commitment.problem_id,
            generation_id: self.generation_id,
            observation_snapshot_id: self.commitment.observation_snapshot_id,
            weighting_generation_id: self.commitment.weighting_generation_id,
            normal_equation_contract_id: self.commitment.normal_equation_contract_id,
            numerics_id: self.commitment.numerics_id,
        }
    }
}

/// Exact reason completion evidence cannot become an authoritative model generation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ModelGenerationError {
    /// Completion was produced for another compiled problem context.
    #[error(
        "model completion problem {actual_problem} does not match expected problem {expected_problem}"
    )]
    StaleCompletion {
        /// Problem retained by the requested commitment.
        expected_problem: CompiledProblemId,
        /// Problem retained by the supplied completion.
        actual_problem: CompiledProblemId,
    },
    /// Completion names a different model-affecting commitment.
    #[error("model completion commitment {actual} does not match expected commitment {expected}")]
    StaleCommitment {
        /// Commitment requested by the caller.
        expected: ModelGenerationCommitmentId,
        /// Commitment named by the supplied completion.
        actual: ModelGenerationCommitmentId,
    },
    /// Completion does not cover every canonical coefficient exactly once.
    #[error("model completion covers {actual} coefficients, expected {expected}")]
    MemberCoverageCountMismatch {
        /// Canonical coefficient count.
        expected: usize,
        /// Supplied completion count.
        actual: usize,
    },
    /// Completion repeats one coefficient member.
    #[error("model completion repeats coefficient member at ordinal {ordinal}")]
    DuplicateMember {
        /// Canonical position containing the repeated member.
        ordinal: usize,
    },
    /// Completion names the wrong coefficient at one canonical position.
    #[error("model completion coefficient at ordinal {ordinal} is not the expected member")]
    MemberMismatch {
        /// Canonical position containing the wrong member.
        ordinal: usize,
    },
}

fn coefficient_members(
    domains: &[CompiledImageDomain],
    coefficient_space: &ModelCoefficientSpace,
) -> Vec<ModelCoefficientKey> {
    let coefficients = match coefficient_space.basis() {
        ReconstructionBasis::Constant => vec![ModelCoefficientKind::Constant],
        ReconstructionBasis::Taylor { terms } => (0..terms)
            .map(|index| ModelCoefficientKind::Taylor(fixed_index(index)))
            .collect(),
        ReconstructionBasis::ChannelLocal { channels } => (0..channels)
            .map(|index| ModelCoefficientKind::Channel(fixed_index(index)))
            .collect(),
    };
    let mut members = Vec::new();
    for domain in domains {
        for coefficient in &coefficients {
            for polarization in coefficient_space.polarization().coordinates() {
                members.push(ModelCoefficientKey {
                    domain: domain.role().clone(),
                    coefficient: *coefficient,
                    polarization: *polarization,
                });
            }
        }
    }
    members
}

struct CommitmentIdentity<'a> {
    lineage_root: ModelGenerationLineageRoot,
    observation_snapshot_id: ObservationSnapshotId,
    coefficient_space: &'a ModelCoefficientSpace,
    reconstruction_algorithm: &'a ReconstructionAlgorithm,
    reconstruction_controls: ReconstructionControls,
    weighting_generation_id: WeightingGenerationId,
    normal_equation_contract_id: NormalEquationContractId,
    numerics_id: NumericsContractId,
    members: &'a [ModelCoefficientKey],
}

fn commitment_id(identity: CommitmentIdentity<'_>) -> ModelGenerationCommitmentId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(MODEL_GENERATION_COMMITMENT_IDENTITY_DOMAIN);
    encoder.u32(MODEL_GENERATION_COMMITMENT_IDENTITY_VERSION);
    encoder.u8(match identity.lineage_root {
        ModelGenerationLineageRoot::Empty => 0,
    });
    encoder.digest(identity.observation_snapshot_id.as_bytes());
    encode_coefficient_space(&mut encoder, identity.coefficient_space);
    encode_algorithm(&mut encoder, identity.reconstruction_algorithm);
    encoder.u64(fixed_index(
        identity.reconstruction_controls.max_minor_iterations(),
    ));
    encoder.f64(identity.reconstruction_controls.gain());
    encoder.f64(identity.reconstruction_controls.threshold_jy_per_beam());
    encoder.digest(identity.weighting_generation_id.as_bytes());
    encoder.digest(identity.normal_equation_contract_id.as_bytes());
    encoder.digest(identity.numerics_id.as_bytes());
    encoder.u64(fixed_index(identity.members.len()));
    for member in identity.members {
        encode_domain(&mut encoder, &member.domain);
        encode_coefficient(&mut encoder, member.coefficient);
        encoder.u8(polarization_tag(member.polarization));
    }
    ModelGenerationCommitmentId(encoder.finish())
}

fn encode_coefficient_space(encoder: &mut CanonicalEncoder, space: &ModelCoefficientSpace) {
    encoder.digest(space.geometry().as_bytes());
    encode_basis(encoder, space.basis());
    encoder.u64(fixed_index(space.polarization().coordinates().len()));
    for coordinate in space.polarization().coordinates() {
        encoder.u8(polarization_tag(*coordinate));
    }
    encoder.u8(match space.inner_product() {
        ModelInnerProduct::HermitianEuclidean => 0,
    });
}

fn encode_basis(encoder: &mut CanonicalEncoder, basis: ReconstructionBasis) {
    encoder.u8(reconstruction_basis_tag(basis));
    match basis {
        ReconstructionBasis::Constant => {}
        ReconstructionBasis::Taylor { terms } => {
            encoder.u64(fixed_index(terms));
        }
        ReconstructionBasis::ChannelLocal { channels } => {
            encoder.u64(fixed_index(channels));
        }
    }
}

fn encode_algorithm(encoder: &mut CanonicalEncoder, algorithm: &ReconstructionAlgorithm) {
    encoder.u8(reconstruction_algorithm_tag(algorithm));
    match algorithm {
        ReconstructionAlgorithm::Multiscale { scales_px } => {
            encoder.u64(fixed_index(scales_px.len()));
            for scale in scales_px {
                encoder.f64(*scale);
            }
        }
        ReconstructionAlgorithm::Dirty
        | ReconstructionAlgorithm::Hogbom
        | ReconstructionAlgorithm::Clark
        | ReconstructionAlgorithm::Mtmfs => {}
    }
}

fn encode_domain(encoder: &mut CanonicalEncoder, domain: &ImageDomainRole) {
    match domain {
        ImageDomainRole::Main => encoder.u8(0),
        ImageDomainRole::Outlier(name) => {
            encoder.u8(1);
            encoder.bytes(name.as_bytes());
        }
    }
}

fn encode_coefficient(encoder: &mut CanonicalEncoder, coefficient: ModelCoefficientKind) {
    match coefficient {
        ModelCoefficientKind::Constant => encoder.u8(0),
        ModelCoefficientKind::Taylor(index) => {
            encoder.u8(1);
            encoder.u64(index);
        }
        ModelCoefficientKind::Channel(index) => {
            encoder.u8(2);
            encoder.u64(index);
        }
    }
}

const fn fixed_index(value: usize) -> u64 {
    value as u64
}

fn validate_completion_members(
    expected: &[ModelCoefficientKey],
    actual: &[ModelCoefficientCompletion],
) -> Result<(), ModelGenerationError> {
    if actual.len() != expected.len() {
        return Err(ModelGenerationError::MemberCoverageCountMismatch {
            expected: expected.len(),
            actual: actual.len(),
        });
    }
    let mismatch = expected
        .iter()
        .zip(actual)
        .position(|(expected, actual)| expected != &actual.member);
    if let Some(ordinal) = mismatch {
        let mut seen = BTreeSet::new();
        if let Some(duplicate) = actual
            .iter()
            .enumerate()
            .find_map(|(ordinal, actual)| (!seen.insert(&actual.member)).then_some(ordinal))
        {
            return Err(ModelGenerationError::DuplicateMember { ordinal: duplicate });
        }
        return Err(ModelGenerationError::MemberMismatch { ordinal });
    }
    Ok(())
}

fn model_generation_id(
    commitment_id: ModelGenerationCommitmentId,
    coefficients: &[ModelCoefficientCompletion],
) -> ModelGenerationId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(MODEL_GENERATION_IDENTITY_DOMAIN);
    encoder.u32(MODEL_GENERATION_IDENTITY_VERSION);
    encoder.digest(commitment_id.as_bytes());
    encoder.u64(fixed_index(coefficients.len()));
    for completion in coefficients {
        encode_domain(&mut encoder, &completion.member.domain);
        encode_coefficient(&mut encoder, completion.member.coefficient);
        encoder.u8(polarization_tag(completion.member.polarization));
        encoder.u8(0);
    }
    ModelGenerationId(encoder.finish())
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

    fn member(coefficient: ModelCoefficientKind) -> ModelCoefficientKey {
        ModelCoefficientKey {
            domain: ImageDomainRole::Main,
            coefficient,
            polarization: PolarizationCoordinate::StokesI,
        }
    }

    #[test]
    fn completion_coverage_rejects_missing_wrong_and_duplicate_members() {
        let expected = [
            member(ModelCoefficientKind::Taylor(0)),
            member(ModelCoefficientKind::Taylor(1)),
        ];
        let exact = expected
            .iter()
            .cloned()
            .map(|member| ModelCoefficientCompletion { member })
            .collect::<Vec<_>>();

        assert_eq!(
            validate_completion_members(&expected, &exact[..1]),
            Err(ModelGenerationError::MemberCoverageCountMismatch {
                expected: 2,
                actual: 1,
            })
        );

        let mut wrong = exact.clone();
        wrong[1] = ModelCoefficientCompletion {
            member: member(ModelCoefficientKind::Taylor(2)),
        };
        assert_eq!(
            validate_completion_members(&expected, &wrong),
            Err(ModelGenerationError::MemberMismatch { ordinal: 1 })
        );

        let mut duplicate = exact;
        duplicate[1] = duplicate[0].clone();
        assert_eq!(
            validate_completion_members(&expected, &duplicate),
            Err(ModelGenerationError::DuplicateMember { ordinal: 1 })
        );
    }
}
