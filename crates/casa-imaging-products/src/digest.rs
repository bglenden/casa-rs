// SPDX-License-Identifier: LGPL-3.0-or-later

//! Canonical digest encoder shared by the product identity schemas.

use std::mem::size_of;

use sha2::{Digest, Sha256};

pub(crate) const COMMITMENT_DOMAIN: &[u8] = b"casa-rs-continuum-commitment";
pub(crate) const COMMITMENT_VERSION: u32 = 4;
pub(crate) const PLANNED_GENERATION_DOMAIN: &[u8] = b"casa-rs-planned-product-generation";
pub(crate) const PLANNED_GENERATION_VERSION: u32 = 1;
pub(crate) const ARTIFACT_IDENTITY_DOMAIN: &[u8] = b"casa-rs-product-artifact";
pub(crate) const ARTIFACT_IDENTITY_VERSION: u32 = 2;
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
    #[cfg(test)]
    pub(crate) fn f32_bits(&mut self, value: f32) {
        let bits = if value == 0.0 { 0 } else { value.to_bits() };
        self.u32(bits);
    }

    /// Encode canonical `f32` payload bits in order, byte-identical to a
    /// [`Self::f32_bits`] loop but hashed in bulk chunks.
    pub(crate) fn f32_bits_slice(&mut self, values: &[f32]) {
        const CHUNK_VALUES: usize = 1024;
        let mut chunk = [0_u8; CHUNK_VALUES * size_of::<f32>()];
        for values in values.chunks(CHUNK_VALUES) {
            let bytes = &mut chunk[..values.len() * size_of::<f32>()];
            for (target, value) in bytes.chunks_exact_mut(size_of::<f32>()).zip(values) {
                let bits = if *value == 0.0 { 0 } else { value.to_bits() };
                target.copy_from_slice(&bits.to_le_bytes());
            }
            self.0.update(bytes);
        }
    }

    /// Encode canonical validity bytes in order, byte-identical to a
    /// [`Self::u8`] loop over `u8::from(value)`.
    pub(crate) fn validity_slice(&mut self, values: &[bool]) {
        const CHUNK_VALUES: usize = 4096;
        let mut chunk = [0_u8; CHUNK_VALUES];
        for values in values.chunks(CHUNK_VALUES) {
            let bytes = &mut chunk[..values.len()];
            for (target, value) in bytes.iter_mut().zip(values) {
                *target = u8::from(*value);
            }
            self.0.update(bytes);
        }
    }
}

/// Digest one canonical `f32` plane in storage order.
#[cfg(test)]
pub(crate) fn plane_digest(values: &[f32]) -> [u8; 32] {
    let mut encoder = Encoder::new(b"casa-rs-product-plane-content", 1);
    encoder.usize(values.len());
    for value in values {
        encoder.f32_bits(*value);
    }
    encoder.finish()
}

/// Digest one product member's numeric payload and exact validity topology.
#[cfg(test)]
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
    use super::{Encoder, member_content_digest};

    #[test]
    fn member_identity_binds_validity_independently_of_numeric_pixels() {
        let pixels = [0.0_f32, 1.0, 0.0, 2.0];
        assert_ne!(
            member_content_digest(&pixels, &[true, true, true, true]),
            member_content_digest(&pixels, &[true, false, true, true])
        );
    }

    #[test]
    fn slice_encoding_is_byte_identical_to_the_per_value_loop() {
        let mut values: Vec<f32> = (0..5000_u32)
            .map(|index| f32::from_bits(index.wrapping_mul(2_654_435_761)))
            .collect();
        values.extend([0.0, -0.0, f32::INFINITY, f32::NEG_INFINITY, f32::NAN, 1.5]);
        let mut looped = Encoder::new(b"casa-rs-product-plane-content", 1);
        looped.usize(values.len());
        for value in &values {
            looped.f32_bits(*value);
        }
        let mut sliced = Encoder::new(b"casa-rs-product-plane-content", 1);
        sliced.usize(values.len());
        sliced.f32_bits_slice(&values);
        assert_eq!(looped.finish(), sliced.finish());

        let validity: Vec<bool> = (0..9000).map(|index| index % 3 == 0).collect();
        let mut looped = Encoder::new(b"casa-rs-product-member-content", 1);
        looped.usize(validity.len());
        for valid in &validity {
            looped.u8(u8::from(*valid));
        }
        let mut sliced = Encoder::new(b"casa-rs-product-member-content", 1);
        sliced.usize(validity.len());
        sliced.validity_slice(&validity);
        assert_eq!(looped.finish(), sliced.finish());
    }
}
