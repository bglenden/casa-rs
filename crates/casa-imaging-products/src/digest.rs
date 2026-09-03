// SPDX-License-Identifier: LGPL-3.0-or-later

//! Canonical digest encoder shared by the product identity schemas.

use sha2::{Digest, Sha256};

pub(crate) const COMMITMENT_DOMAIN: &[u8] = b"casa-rs-continuum-commitment";
pub(crate) const COMMITMENT_VERSION: u32 = 4;
pub(crate) const PLANNED_GENERATION_DOMAIN: &[u8] = b"casa-rs-planned-product-generation";
pub(crate) const PLANNED_GENERATION_VERSION: u32 = 1;
pub(crate) const ARTIFACT_IDENTITY_DOMAIN: &[u8] = b"casa-rs-product-artifact";
pub(crate) const ARTIFACT_IDENTITY_VERSION: u32 = 1;
pub(crate) const COMPLETIONS_DOMAIN: &[u8] = b"casa-rs-continuum-completions";
pub(crate) const COMPLETIONS_VERSION: u32 = 3;
pub(crate) const SEAL_DOMAIN: &[u8] = b"casa-rs-product-generation-seal";
pub(crate) const SEAL_VERSION: u32 = 1;

/// Byte-exact canonical encoder for product-owned identities.
///
/// The field order and encodings are schema: changing anything here changes
/// every derived identity and therefore requires a schema-version bump.
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

    /// Encode one canonical `f32` payload bit with `-0.0` folded onto `+0.0`.
    pub(crate) fn f32_bits(&mut self, value: f32) {
        let bits = if value == 0.0 { 0 } else { value.to_bits() };
        self.u32(bits);
    }
}

/// Digest one canonical `f32` plane in storage order.
pub(crate) fn plane_digest(values: &[f32]) -> [u8; 32] {
    let mut encoder = Encoder::new(b"casa-rs-product-plane-content", 1);
    encoder.usize(values.len());
    for value in values {
        encoder.f32_bits(*value);
    }
    encoder.finish()
}

/// Digest one product member's numeric payload and exact validity topology.
pub(crate) fn member_content_digest(values: &[f32], validity: &[bool]) -> [u8; 32] {
    let mut encoder = Encoder::new(b"casa-rs-product-member-content", 1);
    encoder.identity(plane_digest(values));
    encoder.usize(validity.len());
    for valid in validity {
        encoder.u8(u8::from(*valid));
    }
    encoder.finish()
}

#[cfg(test)]
mod tests {
    use super::member_content_digest;

    #[test]
    fn member_identity_binds_validity_independently_of_numeric_pixels() {
        let pixels = [0.0_f32, 1.0, 0.0, 2.0];
        assert_ne!(
            member_content_digest(&pixels, &[true, true, true, true]),
            member_content_digest(&pixels, &[true, false, true, true])
        );
    }
}
