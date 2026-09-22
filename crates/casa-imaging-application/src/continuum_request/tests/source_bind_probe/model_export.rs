// SPDX-License-Identifier: LGPL-3.0-or-later

//! Read-only, streamed diagnostic export; never a model import or authority API.

use std::{
    fs::{self, File},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
};

use casa_imaging_model::{ModelSample, ModelSupport, SpectralWcs};
use casa_imaging_reconstruction::MajorCycleCompletion;
use sha2::{Digest, Sha256};

#[derive(Debug, Default)]
struct SampleSummary {
    samples: usize,
    valid_samples: usize,
    nonzero_valid_samples: usize,
    float32_changed_samples: usize,
    maximum_absolute_roundtrip_error: f64,
    maximum_relative_roundtrip_error: f64,
    payload_sha256: String,
    values_sha256: String,
}

fn write_samples(samples: &[ModelSample], writer: &mut impl Write) -> io::Result<SampleSummary> {
    let mut summary = SampleSummary::default();
    let mut digest = Sha256::new();
    let mut values_digest = Sha256::new();
    for sample in samples {
        let value = sample.value().value();
        let valid = sample.support() == ModelSupport::Valid;
        if !value.is_finite() || (!valid && value != 0.0) {
            return Err(io::Error::other("noncanonical model export sample"));
        }
        if valid {
            let rounded = f64::from(value as f32);
            if !rounded.is_finite() {
                return Err(io::Error::other("model coefficient overflows CASA Float"));
            }
            let error = (rounded - value).abs();
            summary.valid_samples += 1;
            summary.nonzero_valid_samples += usize::from(value != 0.0);
            summary.float32_changed_samples += usize::from(rounded != value);
            summary.maximum_absolute_roundtrip_error =
                summary.maximum_absolute_roundtrip_error.max(error);
            if value != 0.0 {
                summary.maximum_relative_roundtrip_error = summary
                    .maximum_relative_roundtrip_error
                    .max(error / value.abs());
            }
        }
        let mut record = [0_u8; 9];
        record[..8].copy_from_slice(&value.to_bits().to_le_bytes());
        record[8] = u8::from(valid);
        writer.write_all(&record)?;
        digest.update(record);
        values_digest.update(&record[..8]);
        summary.samples += 1;
    }
    summary.payload_sha256 = format!("{:x}", digest.finalize());
    summary.values_sha256 = format!("{:x}", values_digest.finalize());
    Ok(summary)
}

fn bits(value: f64) -> String {
    format!("{:016x}", value.to_bits())
}

pub(super) fn export(scientific: &MajorCycleCompletion, root: &Path) -> io::Result<PathBuf> {
    let model = scientific.final_model();
    if scientific.normal_state().final_model_generation() != model.generation_id() {
        return Err(io::Error::other(
            "model and normal-state generations differ",
        ));
    }
    let shape = model.shape();
    let SpectralWcs::Linear {
        channels,
        reference_pixel,
        reference_frequency_hz,
        increment_hz,
    } = shape.spectral().wcs()
    else {
        return Err(io::Error::other(
            "T51 export requires the linear spectral WCS",
        ));
    };
    let domains = shape
        .domains()
        .iter()
        .enumerate()
        .map(|(ordinal, domain)| {
            let direction = shape.direction(ordinal).expect("compiled model direction");
            let reference = direction.reference_direction();
            serde_json::json!({
                "pixels": domain.pixels(),
                "role": format!("{:?}", shape.domain_roles()[ordinal]),
                "projection": format!("{:?}", direction.projection()),
                "frame": format!("{:?}", reference.frame()),
                "reference_direction_rad_bits": [bits(reference.longitude_rad()), bits(reference.latitude_rad())],
                "reference_pixel_bits": direction.reference_pixel().map(bits),
                "increment_rad_bits": direction.increment_rad().map(bits),
                "pc_bits": direction.pc().map(|row| row.map(bits)),
                "pole_deg_bits": direction.pole_deg().map(bits),
            })
        })
        .collect::<Vec<_>>();
    let directory = root.join("authoritative-model");
    fs::create_dir(&directory)?;
    let payload_name = "samples.f64-support.bin";
    let payload = File::create_new(directory.join(payload_name))?;
    let mut writer = BufWriter::with_capacity(1 << 20, payload);
    let samples = model
        .read_samples(0..shape.sample_count())
        .map_err(io::Error::other)?;
    let summary = write_samples(&samples, &mut writer)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    if summary.samples != shape.sample_count() {
        return Err(io::Error::other("model export shape count differs"));
    }
    let manifest = serde_json::json!({
        "schema": "t51-authoritative-model-v1",
        "authority": "MajorCycleCompletion.final_model; physical predictor coefficients, not apparent published images",
        "model_generation": model.generation_id().to_string(),
        "normal_model_generation": scientific.normal_state().final_model_generation().to_string(),
        "major_completion": scientific.completion_id().to_string(),
        "shape_identity": shape.identity().to_string(),
        "geometry_identity": shape.coefficient_space().geometry().to_string(),
        "domains": domains,
        "basis": format!("{:?}", shape.coefficient_space().basis()),
        "coefficients": shape.coefficients(),
        "polarizations": shape.coefficient_space().polarization().coordinates().iter()
            .map(|coordinate| format!("{coordinate:?}")).collect::<Vec<_>>(),
        "spectral": {
            "channels": channels,
            "source_frame": format!("{:?}", shape.spectral().source_frame()),
            "output_frame": format!("{:?}", shape.spectral().output_frame()),
            "anchor": format!("{:?}", shape.spectral().anchor()),
            "reference_pixel_bits": bits(*reference_pixel),
            "reference_frequency_hz_bits": bits(*reference_frequency_hz),
            "increment_hz_bits": bits(*increment_hz),
        },
        "payload": payload_name,
        "record_bytes": 9,
        "encoding": "little-endian IEEE754 F64 followed by support byte: 0 invalid, 1 valid",
        "order": "domain, coefficient, polarization, y, x; x fastest",
        "summary": {
            "samples": summary.samples,
            "valid_samples": summary.valid_samples,
            "nonzero_valid_samples": summary.nonzero_valid_samples,
            "float32_changed_samples": summary.float32_changed_samples,
            "maximum_absolute_roundtrip_error": summary.maximum_absolute_roundtrip_error,
            "maximum_relative_roundtrip_error": summary.maximum_relative_roundtrip_error,
            "payload_sha256": summary.payload_sha256,
            "values_sha256": summary.values_sha256,
        },
        "acceptance": "export only; Float conversion and CASA post-import equivalence are separate checks",
    });
    let path = directory.join("manifest.json");
    let mut output = File::create_new(&path)?;
    serde_json::to_writer_pretty(&mut output, &manifest).map_err(io::Error::other)?;
    output.write_all(b"\n")?;
    output.sync_all()?;
    Ok(path)
}

#[test]
fn model_export_preserves_f64_bits_and_independent_support() {
    use casa_imaging_model::ModelValue;
    let samples = [
        ModelSample::valid(ModelValue::new(1.0).unwrap()),
        ModelSample::valid(ModelValue::new(0.1).unwrap()),
        ModelSample::invalid(),
    ];
    let mut payload = Vec::new();
    let summary = write_samples(&samples, &mut payload).unwrap();
    assert_eq!(payload.len(), 27);
    for (record, sample) in payload.chunks_exact(9).zip(samples) {
        assert_eq!(
            u64::from_le_bytes(record[..8].try_into().unwrap()),
            sample.value().value().to_bits(),
        );
        assert_eq!(record[8], u8::from(sample.support() == ModelSupport::Valid));
    }
    assert_eq!(summary.valid_samples, 2);
    assert_eq!(summary.float32_changed_samples, 1);
    assert_eq!(
        summary.payload_sha256,
        format!("{:x}", Sha256::digest(&payload))
    );
    assert!(summary.maximum_absolute_roundtrip_error > 0.0);
}
