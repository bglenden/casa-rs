// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn read_manifest_counted(
    path: &Path,
    evidence: &mut ValidationEvidence,
) -> Result<(ArtifactManifest, u64), PreparedArtifactError> {
    evidence.store_read_operation();
    let file = File::open(path).map_err(map_incomplete)?;
    evidence.manifest_open();
    evidence.observe_file_descriptors(2);
    let bounded = BoundedFileReader {
        file,
        evidence,
        remaining: MANIFEST_RESERVATION_BYTES,
        exceeded: false,
    };
    let mut reader = BufReader::with_capacity(4096, bounded);
    let reader_resident = u64::try_from(reader.capacity()).unwrap_or(u64::MAX);
    reader.get_mut().evidence.acquire_resident(reader_resident);
    let parsed = serde_json::from_reader(&mut reader);
    let exceeded = reader.get_ref().exceeded();
    drop(reader);
    if exceeded {
        evidence.release_resident(reader_resident);
        return Err(PreparedArtifactError::InvalidManifest);
    }
    match parsed.map_err(PreparedArtifactError::Json) {
        Ok(manifest) => {
            let manifest_resident = observed_manifest_resident_bytes(&manifest);
            evidence.acquire_resident(manifest_resident);
            let admitted = evidence.ensure_resident_budget();
            evidence.release_resident(reader_resident);
            match admitted {
                Ok(()) => Ok((manifest, manifest_resident)),
                Err(error) => {
                    evidence.release_resident(manifest_resident);
                    Err(error)
                }
            }
        }
        Err(error) => {
            evidence.release_resident(reader_resident);
            Err(error)
        }
    }
}

pub(super) fn read_exact_counted<R: Read + ?Sized>(
    input: &mut R,
    output: &mut [u8],
    evidence: &mut ValidationEvidence,
    class: CacheIoClass,
) -> Result<(), PreparedArtifactError> {
    let mut offset = 0;
    while offset < output.len() {
        let bytes = read_counted(input, &mut output[offset..], evidence, class)?;
        if bytes == 0 {
            return Err(PreparedArtifactError::IncompleteArtifact);
        }
        offset += bytes;
    }
    Ok(())
}

pub(super) fn read_counted<R: Read + ?Sized>(
    input: &mut R,
    output: &mut [u8],
    evidence: &mut ValidationEvidence,
    class: CacheIoClass,
) -> Result<usize, PreparedArtifactError> {
    let bytes = input.read(output).map_err(map_incomplete)?;
    evidence.record(class, bytes as u64);
    evidence.record_payload_read(bytes as u64);
    Ok(bytes)
}

pub(super) fn read_exact_source_counted<R: Read + ?Sized>(
    input: &mut R,
    output: &mut [u8],
    evidence: &mut ValidationEvidence,
    demand_id: &str,
) -> Result<(), PreparedArtifactError> {
    let mut offset = 0;
    while offset < output.len() {
        let bytes = read_source_counted(input, &mut output[offset..], evidence, demand_id)?;
        if bytes == 0 {
            return Err(PreparedArtifactError::IncompleteArtifact);
        }
        offset += bytes;
    }
    Ok(())
}

pub(super) fn read_source_counted<R: Read + ?Sized>(
    input: &mut R,
    output: &mut [u8],
    evidence: &mut ValidationEvidence,
    demand_id: &str,
) -> Result<usize, PreparedArtifactError> {
    let bytes = input.read(output).map_err(map_incomplete)?;
    evidence.record_source(demand_id, bytes as u64);
    Ok(bytes)
}

pub(super) fn write_all_counted<W: Write + ?Sized>(
    output: &mut W,
    mut input: &[u8],
    evidence: &mut ValidationEvidence,
    class: CacheIoClass,
) -> Result<(), PreparedArtifactError> {
    while !input.is_empty() {
        let written = output.write(input).map_err(PreparedArtifactError::Io)?;
        evidence.record(class, written as u64);
        if written == 0 {
            return Err(PreparedArtifactError::Io(io::Error::new(
                io::ErrorKind::WriteZero,
                "prepared artifact write made no progress",
            )));
        }
        input = &input[written..];
    }
    Ok(())
}

pub(super) fn validate_finite(
    bytes: &[u8],
    precision: PreparedArtifactPrecision,
    segment: &str,
    first_scalar: u64,
) -> Result<(), PreparedArtifactError> {
    match precision {
        PreparedArtifactPrecision::F32 | PreparedArtifactPrecision::ComplexF32 => {
            // A non-short-circuit reduction lets the finite fast path vectorize;
            // only invalid payloads need the ordered scan for an exact error index.
            if bytes.chunks_exact(4).fold(true, |finite, chunk| {
                finite & f32::from_le_bytes(chunk.try_into().expect("exact f32 chunk")).is_finite()
            }) {
                return Ok(());
            }
            for (offset, chunk) in bytes.chunks_exact(4).enumerate() {
                if !f32::from_le_bytes(chunk.try_into().expect("exact f32 chunk")).is_finite() {
                    return Err(PreparedArtifactError::NonFiniteValue {
                        segment: segment.to_string(),
                        scalar: first_scalar + offset as u64,
                    });
                }
            }
        }
        PreparedArtifactPrecision::F64 | PreparedArtifactPrecision::ComplexF64 => {
            if bytes.chunks_exact(8).fold(true, |finite, chunk| {
                finite & f64::from_le_bytes(chunk.try_into().expect("exact f64 chunk")).is_finite()
            }) {
                return Ok(());
            }
            for (offset, chunk) in bytes.chunks_exact(8).enumerate() {
                if !f64::from_le_bytes(chunk.try_into().expect("exact f64 chunk")).is_finite() {
                    return Err(PreparedArtifactError::NonFiniteValue {
                        segment: segment.to_string(),
                        scalar: first_scalar + offset as u64,
                    });
                }
            }
        }
        PreparedArtifactPrecision::I32
        | PreparedArtifactPrecision::U32
        | PreparedArtifactPrecision::U8 => {}
    }
    Ok(())
}

pub(super) fn validate_manifest_segments(
    descriptor: &PreparedArtifactCompatibility,
    integrity: &[ManifestSegmentIntegrity],
    payload_bytes: u64,
) -> Result<(), PreparedArtifactError> {
    if integrity.len() != descriptor.segments.len() {
        return Err(PreparedArtifactError::SegmentLayoutMismatch);
    }
    let mut offset = 0_u64;
    for (expected, actual) in descriptor.segments.iter().zip(integrity) {
        if actual.offset != offset
            || actual.bytes != expected.byte_len()?
            || decode_digest(&actual.sha256).is_none()
        {
            return Err(PreparedArtifactError::SegmentLayoutMismatch);
        }
        offset = offset
            .checked_add(actual.bytes)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    }
    if offset != payload_bytes || offset != descriptor.payload_bytes()? {
        return Err(PreparedArtifactError::SegmentLayoutMismatch);
    }
    Ok(())
}

pub(super) fn validate_entry_inventory(
    directory: &Path,
    evidence: &mut ValidationEvidence,
) -> Result<u64, PreparedArtifactError> {
    with_directory_paths_counted(directory, evidence, MAX_ENTRY_FILES, |evidence, paths| {
        if paths.len() != MAX_ENTRY_FILES {
            return Err(PreparedArtifactError::IncompleteArtifact);
        }
        let mut manifest = false;
        let mut payload = false;
        let mut bytes = 0_u64;
        for path in paths {
            evidence.store_read_operation();
            let metadata = path.symlink_metadata()?;
            if !metadata.file_type().is_file() {
                return Err(PreparedArtifactError::UnknownCacheEntry(path.to_path_buf()));
            }
            let name = path
                .file_name()
                .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.to_path_buf()))?;
            manifest |= name == MANIFEST_FILE;
            payload |= name == PAYLOAD_FILE;
            if name == PAYLOAD_FILE {
                evidence.payload_metadata_check();
            }
            bytes = bytes
                .checked_add(metadata.len())
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        }
        if !manifest || !payload {
            return Err(PreparedArtifactError::IncompleteArtifact);
        }
        evidence.store_validation();
        Ok(bytes)
    })
    .map_err(|error| match error {
        PreparedArtifactError::Io(error) => map_incomplete(error),
        other => other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finite_validation_preserves_first_error_segment_and_absolute_scalar() {
        for precision in [
            PreparedArtifactPrecision::F32,
            PreparedArtifactPrecision::ComplexF32,
            PreparedArtifactPrecision::F64,
            PreparedArtifactPrecision::ComplexF64,
        ] {
            let width = precision.scalar_bytes();
            let (finite, nan, infinity) = if width == 4 {
                (
                    1.25_f32.to_le_bytes().to_vec(),
                    f32::NAN.to_le_bytes().to_vec(),
                    f32::INFINITY.to_le_bytes().to_vec(),
                )
            } else {
                (
                    1.25_f64.to_le_bytes().to_vec(),
                    f64::NAN.to_le_bytes().to_vec(),
                    f64::INFINITY.to_le_bytes().to_vec(),
                )
            };
            let valid = finite.repeat(257);
            validate_finite(&valid, precision, "test-segment", 4096).unwrap();
            validate_finite(&[], precision, "test-segment", 4096).unwrap();
            for first in [0, 1, 7, 15, 16, 63, 64, 65, 129, 255] {
                let mut bytes = valid.clone();
                bytes[first * width..(first + 1) * width].copy_from_slice(&nan);
                bytes[(first + 1) * width..(first + 2) * width].copy_from_slice(&infinity);
                assert!(matches!(
                    validate_finite(&bytes, precision, "test-segment", 4096),
                    Err(PreparedArtifactError::NonFiniteValue { segment, scalar })
                        if segment == "test-segment" && scalar == 4096 + first as u64
                ));
            }
        }
        for precision in [
            PreparedArtifactPrecision::I32,
            PreparedArtifactPrecision::U32,
            PreparedArtifactPrecision::U8,
        ] {
            validate_finite(&[255; 257], precision, "integer-segment", 4096).unwrap();
        }
    }
}
