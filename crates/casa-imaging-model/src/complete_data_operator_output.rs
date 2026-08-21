// SPDX-License-Identifier: LGPL-3.0-or-later

//! T19-owned identity of one complete-data measurement-operator output.

use std::fmt;

use crate::{FinalReconciliationCommitment, compiled_problem::CanonicalEncoder};

const COMPLETE_DATA_OPERATOR_OUTPUT_IDENTITY_DOMAIN: &[u8] =
    b"casa-rs-complete-data-operator-output";
const COMPLETE_DATA_OPERATOR_OUTPUT_IDENTITY_VERSION: u32 = 1;
const COMPLETE_DATA_PRIMITIVE_IDENTITY_DOMAIN: &[u8] = b"casa-rs-complete-data-primitive";
const COMPLETE_DATA_PRIMITIVE_IDENTITY_VERSION: u32 = 1;

/// Stable semantic identity expected for one complete-data operator output.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompleteDataOperatorOutputId([u8; 32]);

impl CompleteDataOperatorOutputId {
    /// Identity schema version used by the complete-data output encoder.
    pub const SCHEMA_VERSION: u32 = COMPLETE_DATA_OPERATOR_OUTPUT_IDENTITY_VERSION;

    /// Derive the output expected from one exact final-reconciliation commitment.
    #[must_use]
    pub fn from_reconciliation(commitment: &FinalReconciliationCommitment) -> Self {
        let mut encoder = CanonicalEncoder::new();
        encoder.bytes(COMPLETE_DATA_OPERATOR_OUTPUT_IDENTITY_DOMAIN);
        encoder.u32(COMPLETE_DATA_OPERATOR_OUTPUT_IDENTITY_VERSION);
        encoder.digest(commitment.commitment_id().as_bytes());
        Self(encoder.finish())
    }

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for CompleteDataOperatorOutputId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CompleteDataOperatorOutputId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for CompleteDataOperatorOutputId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Closed semantic role of one complete-data normal primitive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CompleteDataPrimitiveKind {
    /// Unnormalized normal right-hand side `A* W d`.
    RightHandSide,
    /// Unnormalized final-model normal residual `A* W (d - A x)`.
    NormalResidual,
    /// Unnormalized approximation to the normal operator `A* W A`.
    NormalApproximation,
    /// Complete-data sensitivity coverage.
    Sensitivity,
    /// Normalization and reporting sum-weight state.
    SumWeights,
    /// Valid support of the complete-data normal state.
    ValidSupport,
}

impl CompleteDataPrimitiveKind {
    /// Canonical complete-data primitive order.
    pub const ALL: [Self; 6] = [
        Self::RightHandSide,
        Self::NormalResidual,
        Self::NormalApproximation,
        Self::Sensitivity,
        Self::SumWeights,
        Self::ValidSupport,
    ];

    const fn identity_tag(self) -> u8 {
        match self {
            Self::RightHandSide => 0,
            Self::NormalResidual => 1,
            Self::NormalApproximation => 2,
            Self::Sensitivity => 3,
            Self::SumWeights => 4,
            Self::ValidSupport => 5,
        }
    }
}

/// Stable semantic identity of one complete-data primitive.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompleteDataPrimitiveId([u8; 32]);

impl CompleteDataPrimitiveId {
    /// Identity schema version used by the complete-data primitive encoder.
    pub const SCHEMA_VERSION: u32 = COMPLETE_DATA_PRIMITIVE_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for CompleteDataPrimitiveId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CompleteDataPrimitiveId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for CompleteDataPrimitiveId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Exact fixed identity catalog required from one complete-data operator output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompleteDataPrimitiveCatalog {
    output_id: CompleteDataOperatorOutputId,
    right_hand_side: CompleteDataPrimitiveId,
    normal_residual: CompleteDataPrimitiveId,
    normal_approximation: CompleteDataPrimitiveId,
    sensitivity: CompleteDataPrimitiveId,
    sum_weights: CompleteDataPrimitiveId,
    valid_support: CompleteDataPrimitiveId,
}

impl CompleteDataPrimitiveCatalog {
    /// Derive the exact primitive identities for one final reconciliation.
    #[must_use]
    pub fn from_reconciliation(commitment: &FinalReconciliationCommitment) -> Self {
        let output_id = CompleteDataOperatorOutputId::from_reconciliation(commitment);
        Self {
            output_id,
            right_hand_side: primitive_id(output_id, CompleteDataPrimitiveKind::RightHandSide),
            normal_residual: primitive_id(output_id, CompleteDataPrimitiveKind::NormalResidual),
            normal_approximation: primitive_id(
                output_id,
                CompleteDataPrimitiveKind::NormalApproximation,
            ),
            sensitivity: primitive_id(output_id, CompleteDataPrimitiveKind::Sensitivity),
            sum_weights: primitive_id(output_id, CompleteDataPrimitiveKind::SumWeights),
            valid_support: primitive_id(output_id, CompleteDataPrimitiveKind::ValidSupport),
        }
    }

    /// Return the complete-data output owning this catalog.
    #[must_use]
    pub const fn output_id(self) -> CompleteDataOperatorOutputId {
        self.output_id
    }

    /// Return the exact primitive identity for one closed semantic role.
    #[must_use]
    pub const fn primitive(self, kind: CompleteDataPrimitiveKind) -> CompleteDataPrimitiveId {
        match kind {
            CompleteDataPrimitiveKind::RightHandSide => self.right_hand_side,
            CompleteDataPrimitiveKind::NormalResidual => self.normal_residual,
            CompleteDataPrimitiveKind::NormalApproximation => self.normal_approximation,
            CompleteDataPrimitiveKind::Sensitivity => self.sensitivity,
            CompleteDataPrimitiveKind::SumWeights => self.sum_weights,
            CompleteDataPrimitiveKind::ValidSupport => self.valid_support,
        }
    }
}

fn primitive_id(
    output_id: CompleteDataOperatorOutputId,
    kind: CompleteDataPrimitiveKind,
) -> CompleteDataPrimitiveId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(COMPLETE_DATA_PRIMITIVE_IDENTITY_DOMAIN);
    encoder.u32(COMPLETE_DATA_PRIMITIVE_IDENTITY_VERSION);
    encoder.digest(output_id.as_bytes());
    encoder.u8(kind.identity_tag());
    CompleteDataPrimitiveId(encoder.finish())
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}
