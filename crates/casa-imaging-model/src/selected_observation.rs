// SPDX-License-Identifier: LGPL-3.0-or-later

//! Compiler-owned scientific identity of one selected-observation evaluation.

use std::fmt;

use crate::{
    compiled_problem::{CanonicalEncoder, SpectralSamplingLaw, encode_spectral_sampling_law},
    geometry::CompiledGeometryId,
    measurement_equation::VisibilityInnerProduct,
    observation::ObservationSnapshotId,
    transaction::{ObservationReadSet, ObservationTransactionContract},
};

const SELECTED_OBSERVATION_COMMITMENT_IDENTITY_DOMAIN: &[u8] =
    b"casa-rs-selected-observation-commitment";
const SELECTED_OBSERVATION_COMMITMENT_IDENTITY_VERSION: u32 = 1;

/// Stable compiler-derived identity of one selected-observation scientific commitment.
///
/// There is deliberately no constructor from raw digest bytes. Only logical
/// problem compilation can mint this identity.
///
/// ```compile_fail
/// use casa_imaging_model::SelectedObservationCommitmentId;
///
/// let _ = SelectedObservationCommitmentId::from_bytes([0; 32]);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SelectedObservationCommitmentId([u8; 32]);

impl SelectedObservationCommitmentId {
    /// Identity schema version used by the canonical encoder.
    pub const SCHEMA_VERSION: u32 = SELECTED_OBSERVATION_COMMITMENT_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for SelectedObservationCommitmentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SelectedObservationCommitmentId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for SelectedObservationCommitmentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Sample-space evaluation semantics owned by the selected-observation compiler seam.
///
/// Reconstruction, product, weighting, numerical, and physical execution
/// choices are deliberately absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedSampleEvaluation {
    visibility_inner_product: VisibilityInnerProduct,
    spectral_sampling: SpectralSamplingLaw,
}

impl SelectedSampleEvaluation {
    /// Return the exact visibility-sample inner product.
    #[must_use]
    pub const fn visibility_inner_product(self) -> VisibilityInnerProduct {
        self.visibility_inner_product
    }

    /// Return the paired spectral sampling applied to selected samples.
    #[must_use]
    pub const fn spectral_sampling(self) -> SpectralSamplingLaw {
        self.spectral_sampling
    }
}

/// Immutable compiler-owned science that every selected-observation traversal must realize.
///
/// The commitment contains no block size, read-ahead depth, double-buffering,
/// worker, backend, resource policy, reconstruction, weighting, or publication
/// choice. The retained read set is derived from the same snapshot during
/// compilation and supplies the exact typed source projection to traversal.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedObservationCommitment {
    commitment_id: SelectedObservationCommitmentId,
    observation_snapshot_id: ObservationSnapshotId,
    geometry_id: CompiledGeometryId,
    sample_evaluation: SelectedSampleEvaluation,
    read_set: ObservationReadSet,
}

impl SelectedObservationCommitment {
    /// Return the compiler-derived identity of this exact scientific commitment.
    #[must_use]
    pub const fn commitment_id(&self) -> SelectedObservationCommitmentId {
        self.commitment_id
    }

    /// Return the immutable atomic snapshot defining source selection and generations.
    #[must_use]
    pub const fn observation_snapshot_id(&self) -> ObservationSnapshotId {
        self.observation_snapshot_id
    }

    /// Return the compiled coordinate, phase, and spectral-geometry identity.
    #[must_use]
    pub const fn geometry_id(&self) -> CompiledGeometryId {
        self.geometry_id
    }

    /// Return the exact selected-sample evaluation semantics.
    #[must_use]
    pub const fn sample_evaluation(&self) -> SelectedSampleEvaluation {
        self.sample_evaluation
    }

    /// Return exact source selection and generation semantics in canonical source order.
    #[must_use]
    pub const fn read_set(&self) -> &ObservationReadSet {
        &self.read_set
    }
}

pub(crate) fn compile_selected_observation_commitment(
    observation_transaction: &ObservationTransactionContract,
    geometry_id: CompiledGeometryId,
    visibility_inner_product: VisibilityInnerProduct,
    spectral_sampling: SpectralSamplingLaw,
) -> SelectedObservationCommitment {
    let observation_snapshot_id = observation_transaction.observation_snapshot_id();
    let sample_evaluation = SelectedSampleEvaluation {
        visibility_inner_product,
        spectral_sampling,
    };
    let commitment_id =
        selected_observation_commitment_id(observation_snapshot_id, geometry_id, sample_evaluation);
    SelectedObservationCommitment {
        commitment_id,
        observation_snapshot_id,
        geometry_id,
        sample_evaluation,
        read_set: observation_transaction.read_set().clone(),
    }
}

fn selected_observation_commitment_id(
    observation_snapshot_id: ObservationSnapshotId,
    geometry_id: CompiledGeometryId,
    sample_evaluation: SelectedSampleEvaluation,
) -> SelectedObservationCommitmentId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(SELECTED_OBSERVATION_COMMITMENT_IDENTITY_DOMAIN);
    encoder.u32(SELECTED_OBSERVATION_COMMITMENT_IDENTITY_VERSION);
    encoder.digest(observation_snapshot_id.as_bytes());
    encoder.digest(geometry_id.as_bytes());
    encode_visibility_inner_product(&mut encoder, sample_evaluation.visibility_inner_product);
    encode_spectral_sampling(&mut encoder, sample_evaluation.spectral_sampling);
    SelectedObservationCommitmentId(encoder.finish())
}

fn encode_visibility_inner_product(
    encoder: &mut CanonicalEncoder,
    inner_product: VisibilityInnerProduct,
) {
    encoder.u8(match inner_product {
        VisibilityInnerProduct::HermitianEuclidean => 0,
    });
}

fn encode_spectral_sampling(encoder: &mut CanonicalEncoder, sampling: SpectralSamplingLaw) {
    encode_spectral_sampling_law(encoder, sampling);
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}
