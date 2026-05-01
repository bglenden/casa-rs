// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native synthetic-observation MS writer tests.

use casa_ms::{
    MeasurementSet, SyntheticAntenna, SyntheticObservationRequest, SyntheticSpectralSetup,
    generate_synthetic_observation_ms,
};
use casa_test_support::{discover_casa_python, tutorial_dataset_path};
use casa_types::{ArrayValue, ScalarValue, Value};
use std::process::Command;

fn antennas() -> Vec<SyntheticAntenna> {
    vec![
        SyntheticAntenna::vla("VLA01", "N01", [-1_601_185.4, -5_041_977.5, 3_554_875.9]),
        SyntheticAntenna::vla("VLA02", "N02", [-1_601_085.4, -5_041_977.5, 3_554_875.9]),
        SyntheticAntenna::vla("VLA03", "N03", [-1_601_185.4, -5_041_877.5, 3_554_875.9]),
    ]
}

fn request(root: &std::path::Path) -> SyntheticObservationRequest {
    let model = root.join("ppdisk672_GHz_50pc.fits");
    write_test_fits_model(&model, 16, 16);
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        &model,
        root.join("ppdisk.synthetic.ms"),
        antennas(),
    );
    request.phase_center_rad = [1.23, -0.45];
    request.start_time_mjd_seconds = 59_000.25 * 86_400.0;
    request.duration_seconds = 25.0;
    request.integration_seconds = 10.0;
    request.spectral_setup = SyntheticSpectralSetup {
        name: "band1".to_string(),
        start_frequency_hz: 672.0e9,
        channel_width_hz: 2.0e6,
        channel_count: 4,
    };
    request
}

#[test]
fn generates_vla_ppdisk_synthetic_ms_skeleton() {
    let temp = tempfile::tempdir().unwrap();
    let request = request(temp.path());

    let report = generate_synthetic_observation_ms(&request).unwrap();
    assert_eq!(report.antenna_count, 3);
    assert_eq!(report.baseline_count, 3);
    assert_eq!(report.time_sample_count, 3);
    assert_eq!(report.main_row_count, 9);
    assert_eq!(report.channel_count, 4);
    assert!(report.nonzero_visibility_count > 0);

    let ms = MeasurementSet::open(&request.output_ms).unwrap();
    assert!(ms.validate().unwrap().is_empty());
    assert_eq!(ms.row_count(), 9);
    assert_eq!(ms.antenna().unwrap().row_count(), 3);
    assert_eq!(ms.antenna().unwrap().name(0).unwrap(), "VLA01");
    assert_eq!(ms.field().unwrap().name(0).unwrap(), "ppdisk");
    assert_eq!(ms.spectral_window().unwrap().num_chan(0).unwrap(), 4);
    let observation = ms.observation().unwrap();
    let telescope = observation
        .table()
        .cell_accessor(0, "TELESCOPE_NAME")
        .unwrap()
        .value()
        .unwrap();
    assert_eq!(
        telescope,
        Some(&Value::Scalar(ScalarValue::String("VLA".to_string())))
    );

    let data = ms
        .main_table()
        .cell_accessor(0, "DATA")
        .unwrap()
        .value()
        .unwrap();
    match data {
        Some(Value::Array(ArrayValue::Complex32(values))) => {
            assert_eq!(values.shape(), &[2, 4]);
            assert!(
                values
                    .iter()
                    .any(|value| value.re != 0.0 || value.im != 0.0)
            );
        }
        other => panic!("expected Complex32 DATA array, got {other:?}"),
    }

    let uvw = ms
        .main_table()
        .cell_accessor(0, "UVW")
        .unwrap()
        .value()
        .unwrap();
    match uvw {
        Some(Value::Array(ArrayValue::Float64(values))) => {
            assert_eq!(
                values.iter().copied().collect::<Vec<_>>(),
                vec![100.0, 0.0, 0.0]
            );
        }
        other => panic!("expected Float64 UVW array, got {other:?}"),
    }
}

#[test]
fn missing_model_image_fails_clearly() {
    let temp = tempfile::tempdir().unwrap();
    let request = SyntheticObservationRequest::vla_ppdisk(
        temp.path().join("missing.fits"),
        temp.path().join("missing-model.ms"),
        antennas(),
    );

    let error = generate_synthetic_observation_ms(&request)
        .unwrap_err()
        .to_string();
    assert!(error.contains("model image"));
    assert!(error.contains("does not exist"));
}

#[test]
fn invalid_antenna_configuration_fails_clearly() {
    let temp = tempfile::tempdir().unwrap();
    let mut request = request(temp.path());
    request.antennas.truncate(1);

    let error = generate_synthetic_observation_ms(&request)
        .unwrap_err()
        .to_string();
    assert!(error.contains("at least two antennas"));
}

#[test]
fn unsupported_spectral_setup_fails_clearly() {
    let temp = tempfile::tempdir().unwrap();
    let mut request = request(temp.path());
    request.spectral_setup.channel_count = 0;

    let error = generate_synthetic_observation_ms(&request)
        .unwrap_err()
        .to_string();
    assert!(error.contains("at least one channel"));
}

#[test]
fn tutorial_ppdisk_fits_model_generates_predicted_visibilities_when_available() {
    let Some(model) =
        tutorial_dataset_path("simulation/vla-ppdisk/model-fits").filter(|path| path.exists())
    else {
        eprintln!("skipping tutorial ppdisk simulation test: model FITS not staged");
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        &model,
        temp.path().join("ppdisk-tutorial.synthetic.ms"),
        antennas(),
    );
    request.duration_seconds = 10.0;
    request.integration_seconds = 10.0;
    request.spectral_setup.channel_count = 1;

    let report = generate_synthetic_observation_ms(&request).unwrap();
    assert_eq!(report.main_row_count, 3);
    assert!(
        report.nonzero_visibility_count > 0,
        "tutorial FITS model should predict non-zero visibility samples"
    );

    let ms = MeasurementSet::open(&request.output_ms).unwrap();
    assert!(ms.validate().unwrap().is_empty());
    let observation = ms.observation().unwrap();
    let telescope = observation
        .table()
        .cell_accessor(0, "TELESCOPE_NAME")
        .unwrap()
        .value()
        .unwrap();
    assert_eq!(
        telescope,
        Some(&Value::Scalar(ScalarValue::String("VLA".to_string())))
    );
}

#[test]
fn casa_can_open_generated_synthetic_ms_when_available() {
    let Some(casa) = discover_casa_python() else {
        eprintln!("skipping CASA synthetic MS read test: CASA Python not available");
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let request = request(temp.path());
    let report = generate_synthetic_observation_ms(&request).unwrap();

    let script = r#"
import json
import sys
from casatools import table

path = sys.argv[1]
tb = table()
tb.open(path)
try:
    data = tb.getcell("DATA", 0)
    result = {
        "rows": tb.nrows(),
        "nonzero": int((abs(data) > 0).sum()),
    }
finally:
    tb.close()
print(json.dumps(result, sort_keys=True))
"#;
    let output = Command::new(&casa.program)
        .arg("-c")
        .arg(script)
        .arg(&request.output_ms)
        .output()
        .expect("run CASA Python table reader");
    assert!(
        output.status.success(),
        "CASA table read failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(&format!("\"rows\": {}", report.main_row_count)));
    assert!(stdout.contains("\"nonzero\": "));
    assert!(!stdout.contains("\"nonzero\": 0"));
}

fn write_test_fits_model(path: &std::path::Path, nx: usize, ny: usize) {
    let mut cards = vec![
        fits_card("SIMPLE", "T"),
        fits_card("BITPIX", "-32"),
        fits_card("NAXIS", "2"),
        fits_card("NAXIS1", &nx.to_string()),
        fits_card("NAXIS2", &ny.to_string()),
        fits_card("BSCALE", "1.0"),
        fits_card("BZERO", "0.0"),
        fits_card("CTYPE1", "'RA---SIN'"),
        fits_card("CDELT1", "1.0E-6"),
        fits_card("CUNIT1", "'rad'"),
        fits_card("CTYPE2", "'DEC--SIN'"),
        fits_card("CDELT2", "1.0E-6"),
        fits_card("CUNIT2", "'rad'"),
        "END".to_string(),
    ];
    let mut bytes = Vec::new();
    for card in cards.drain(..) {
        let mut padded = format!("{card:<80}").into_bytes();
        padded.truncate(80);
        bytes.extend(padded);
    }
    while bytes.len() % 2880 != 0 {
        bytes.push(b' ');
    }
    for y in 0..ny {
        for x in 0..nx {
            let value = if x == nx / 2 && y == ny / 2 {
                1.0f32
            } else {
                0.0
            };
            bytes.extend(value.to_bits().to_be_bytes());
        }
    }
    while bytes.len() % 2880 != 0 {
        bytes.push(0);
    }
    std::fs::write(path, bytes).unwrap();
}

fn fits_card(key: &str, value: &str) -> String {
    format!("{key:<8}= {value:>20}")
}
