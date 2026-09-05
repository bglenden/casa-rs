// SPDX-License-Identifier: LGPL-3.0-or-later

//! Logical visibility transforms applied before spectral sampling and weighting.

use std::fmt;

use sha2::{Digest, Sha256};
use thiserror::Error;

/// Role of one selected native channel in sequential continuum subtraction.
///
/// The role is part of the compiled selection rather than inferred from the
/// output cube. This allows line-free fit channels to be read without letting
/// them contribute to spectral products or line-model persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ContinuumChannelUse {
    /// Read the channel for the continuum fit but exclude it from line output.
    FitOnly,
    /// Subtract the fitted continuum and contribute the residual to line output.
    ApplyOnly,
    /// Use the channel in the fit and contribute its residual to line output.
    FitAndApply,
}

impl ContinuumChannelUse {
    /// Return whether this channel participates in the polynomial fit.
    #[must_use]
    pub const fn contributes_to_fit(self) -> bool {
        matches!(self, Self::FitOnly | Self::FitAndApply)
    }

    /// Return whether this channel contributes a residual to spectral output.
    #[must_use]
    pub const fn contributes_to_output(self) -> bool {
        matches!(self, Self::ApplyOnly | Self::FitAndApply)
    }
}

/// One resolved native channel and its continuum-transform role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContinuumChannelRole {
    channel_index: u32,
    use_role: ContinuumChannelUse,
}

impl ContinuumChannelRole {
    /// Construct one resolved channel role.
    #[must_use]
    pub const fn new(channel_index: u32, use_role: ContinuumChannelUse) -> Self {
        Self {
            channel_index,
            use_role,
        }
    }

    /// Return the zero-based native channel index.
    #[must_use]
    pub const fn channel_index(self) -> u32 {
        self.channel_index
    }

    /// Return the fit/application role.
    #[must_use]
    pub const fn use_role(self) -> ContinuumChannelUse {
        self.use_role
    }
}

/// CASA-compatible treatment of fit-induced residual covariance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinuumCovariancePolicy {
    /// Preserve input weights without pretending to encode fit-induced correlation.
    NotRepresentedPreserveInputWeights,
}

/// One resolved per-field/per-SPW polynomial-fit rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuumFitRule {
    field_id: i32,
    spectral_window_id: u32,
    requested_order: u8,
    channels: Vec<ContinuumChannelRole>,
}

impl ContinuumFitRule {
    /// Construct a resolved rule, rejecting duplicate channels and empty fit/output sets.
    pub fn new(
        field_id: i32,
        spectral_window_id: u32,
        requested_order: u8,
        mut channels: Vec<ContinuumChannelRole>,
    ) -> Result<Self, ContinuumTransformContractError> {
        channels.sort_unstable_by_key(|channel| channel.channel_index);
        if channels.is_empty()
            || channels
                .windows(2)
                .any(|pair| pair[0].channel_index == pair[1].channel_index)
            || !channels
                .iter()
                .any(|channel| channel.use_role.contributes_to_fit())
            || !channels
                .iter()
                .any(|channel| channel.use_role.contributes_to_output())
        {
            return Err(ContinuumTransformContractError::InvalidChannels);
        }
        Ok(Self {
            field_id,
            spectral_window_id,
            requested_order,
            channels,
        })
    }

    /// Return the selected FIELD_ID.
    #[must_use]
    pub const fn field_id(&self) -> i32 {
        self.field_id
    }

    /// Return the selected SPECTRAL_WINDOW_ID.
    #[must_use]
    pub const fn spectral_window_id(&self) -> u32 {
        self.spectral_window_id
    }

    /// Return the requested polynomial order.
    #[must_use]
    pub const fn requested_order(&self) -> u8 {
        self.requested_order
    }

    /// Return canonical native-channel roles.
    #[must_use]
    pub fn channels(&self) -> &[ContinuumChannelRole] {
        &self.channels
    }

    /// Resolve one selected native channel.
    #[must_use]
    pub fn channel_use(&self, channel_index: u32) -> Option<ContinuumChannelUse> {
        self.channels
            .binary_search_by_key(&channel_index, |channel| channel.channel_index)
            .ok()
            .map(|index| self.channels[index].use_role)
    }
}

/// Stable logical identity of one canonical sequential-continuum contract.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContinuumTransformContractId([u8; 32]);

impl ContinuumTransformContractId {
    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for ContinuumTransformContractId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Stable identity of one transformed ordered selected-observation stream.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContinuumTransformGenerationId([u8; 32]);

impl ContinuumTransformGenerationId {
    /// Construct an owner-minted generation from its canonical digest.
    #[doc(hidden)]
    #[must_use]
    pub const fn from_owner_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for ContinuumTransformGenerationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Stable generation of ordered fit roles, flags, and effective input weights.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContinuumFitWeightGenerationId([u8; 32]);

impl ContinuumFitWeightGenerationId {
    /// Construct an owner-minted generation from its canonical digest.
    #[doc(hidden)]
    #[must_use]
    pub const fn from_owner_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Canonical, backend-independent sequential continuum-transform contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequentialContinuumTransform {
    rules: Vec<ContinuumFitRule>,
    covariance: ContinuumCovariancePolicy,
    contract_id: ContinuumTransformContractId,
}

impl SequentialContinuumTransform {
    /// Construct canonical resolved rules. A field/SPW pair may occur once.
    pub fn new(mut rules: Vec<ContinuumFitRule>) -> Result<Self, ContinuumTransformContractError> {
        rules.sort_unstable_by_key(|rule| (rule.field_id, rule.spectral_window_id));
        if rules.is_empty()
            || rules.windows(2).any(|pair| {
                (pair[0].field_id, pair[0].spectral_window_id)
                    == (pair[1].field_id, pair[1].spectral_window_id)
            })
        {
            return Err(ContinuumTransformContractError::DuplicateRule);
        }
        let covariance = ContinuumCovariancePolicy::NotRepresentedPreserveInputWeights;
        let contract_id = contract_identity(&rules, covariance);
        Ok(Self {
            rules,
            covariance,
            contract_id,
        })
    }

    /// Return canonical per-field/per-SPW rules.
    #[must_use]
    pub fn rules(&self) -> &[ContinuumFitRule] {
        &self.rules
    }

    /// Resolve one selected field/SPW pair.
    #[must_use]
    pub fn rule(&self, field_id: i32, spectral_window_id: u32) -> Option<&ContinuumFitRule> {
        self.rules
            .binary_search_by_key(&(field_id, spectral_window_id), |rule| {
                (rule.field_id, rule.spectral_window_id)
            })
            .ok()
            .map(|index| &self.rules[index])
    }

    /// Return the explicit covariance treatment.
    #[must_use]
    pub const fn covariance(&self) -> ContinuumCovariancePolicy {
        self.covariance
    }

    /// Return the canonical logical identity.
    #[must_use]
    pub const fn contract_id(&self) -> ContinuumTransformContractId {
        self.contract_id
    }
}

/// Invalid resolved continuum-transform contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ContinuumTransformContractError {
    /// A rule lacked fit/output channels or repeated a channel index.
    #[error("continuum-fit rule channels must be unique and include fit and output roles")]
    InvalidChannels,
    /// No rule was supplied or one field/SPW pair occurred more than once.
    #[error("continuum-transform rules must contain unique field/SPW pairs")]
    DuplicateRule,
}

fn contract_identity(
    rules: &[ContinuumFitRule],
    covariance: ContinuumCovariancePolicy,
) -> ContinuumTransformContractId {
    let mut digest = Sha256::new();
    digest.update(b"casa-rs-sequential-continuum-transform");
    digest.update(1_u32.to_le_bytes());
    digest.update((rules.len() as u64).to_le_bytes());
    for rule in rules {
        digest.update(rule.field_id.to_le_bytes());
        digest.update(rule.spectral_window_id.to_le_bytes());
        digest.update([rule.requested_order]);
        digest.update((rule.channels.len() as u64).to_le_bytes());
        for channel in &rule.channels {
            digest.update(channel.channel_index.to_le_bytes());
            digest.update([match channel.use_role {
                ContinuumChannelUse::FitOnly => 0,
                ContinuumChannelUse::ApplyOnly => 1,
                ContinuumChannelUse::FitAndApply => 2,
            }]);
        }
    }
    digest.update([match covariance {
        ContinuumCovariancePolicy::NotRepresentedPreserveInputWeights => 0,
    }]);
    ContinuumTransformContractId(digest.finalize().into())
}
