// SPDX-License-Identifier: LGPL-3.0-or-later

//! Owner-minted lifecycle identities and their canonical SHA-256 encoding.

use std::{fmt, sync::atomic::AtomicU64};

use casa_imaging_model::LogicalIdentity;
use sha2::{Digest, Sha256};

pub(crate) const AUTHORITY_DOMAIN: &[u8] = b"casa-rs-model-lifecycle-authority";
pub(crate) const AUTHORITY_VERSION: u32 = 2;
pub(crate) const GENERATION_DOMAIN: &[u8] = b"casa-rs-model-generation";
pub(crate) const GENERATION_VERSION: u32 = 4;
const REPROJECTION_VERSION: u32 = 3;
pub(crate) const FINAL_COMPLETION_DOMAIN: &[u8] = b"casa-rs-final-model-completion";
pub(crate) const FINAL_COMPLETION_VERSION: u32 = 2;
pub(crate) const FINAL_NORMAL_STATE_DOMAIN: &[u8] = b"casa-rs-final-normal-state";
pub(crate) const FINAL_NORMAL_STATE_VERSION: u32 = 4;
pub(crate) const MAJOR_CYCLE_DOMAIN: &[u8] = b"casa-rs-major-cycle-completion";
pub(crate) const MAJOR_CYCLE_VERSION: u32 = 2;

macro_rules! lifecycle_identity {
    ($name:ident, $version:ident, $summary:literal) => {
        #[doc = $summary]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub(crate) LogicalIdentity);

        impl $name {
            /// Identity schema version used by the canonical encoder.
            pub const SCHEMA_VERSION: u32 = $version;

            /// Return the exact SHA-256 digest.
            #[must_use]
            pub const fn as_bytes(self) -> [u8; 32] {
                self.0.as_bytes()
            }

            /// Return this typed identity as a compiler input commitment.
            #[must_use]
            pub const fn identity(self) -> LogicalIdentity {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($name), "("))?;
                write_hex(formatter, &self.as_bytes())?;
                formatter.write_str(")")
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write_hex(formatter, &self.as_bytes())
            }
        }
    };
}

lifecycle_identity!(
    ModelGenerationId,
    GENERATION_VERSION,
    "Stable owner-minted identity of one complete model generation."
);
/// Process-local event token for one validated, base-bound model update.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModelDeltaId(pub(crate) u64);

impl ModelDeltaId {
    /// Return the event ordinal for compact run-local association.
    pub const fn ordinal(self) -> u64 {
        self.0
    }
}

pub(crate) static NEXT_MODEL_DELTA: AtomicU64 = AtomicU64::new(1);

lifecycle_identity!(
    ModelReprojectionId,
    REPROJECTION_VERSION,
    "Stable identity of one validated canonical reprojection."
);
lifecycle_identity!(
    FinalModelCompletionId,
    FINAL_COMPLETION_VERSION,
    "Stable identity of one affine final-model completion."
);
lifecycle_identity!(
    FinalNormalStateCompletionId,
    FINAL_NORMAL_STATE_VERSION,
    "Stable identity of one authoritative final Normal State completion."
);
lifecycle_identity!(
    MajorCycleCompletionId,
    MAJOR_CYCLE_VERSION,
    "Stable identity of one atomic Major-Cycle reconciliation."
);

pub(crate) fn canonical_f64_bits(value: f64) -> u64 {
    if value == 0.0 { 0 } else { value.to_bits() }
}

pub(crate) fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

pub(crate) struct Encoder(Sha256);

impl Encoder {
    pub(crate) fn new(domain: &[u8], version: u32) -> Self {
        let mut encoder = Self(Sha256::new());
        encoder.bytes(domain);
        encoder.u32(version);
        encoder
    }

    pub(crate) fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }

    pub(crate) fn bytes(&mut self, value: &[u8]) {
        self.usize(value.len());
        self.0.update(value);
    }

    pub(crate) fn identity(&mut self, value: [u8; 32]) {
        self.0.update(value);
    }

    pub(crate) fn u8(&mut self, value: u8) {
        self.0.update([value]);
    }

    pub(crate) fn u32(&mut self, value: u32) {
        self.0.update(value.to_le_bytes());
    }

    pub(crate) fn u64(&mut self, value: u64) {
        self.0.update(value.to_le_bytes());
    }

    pub(crate) fn usize(&mut self, value: usize) {
        self.u64(u64::try_from(value).expect("usize fits in u64 on supported targets"));
    }
}
