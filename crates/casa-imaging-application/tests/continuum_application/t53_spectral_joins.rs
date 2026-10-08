// SPDX-License-Identifier: LGPL-3.0-or-later

//! Standard-gridder spectral cubes: rest frequency, one-channel laws and
//! nonidentity spectral sampling. The W, mosaic and AW families of these joins
//! return with the convolution-function sets of IF-3 (#652); cubic cube
//! interpolation is rejected by the installed implementation.

use super::*;

#[test]
fn t53_cube_rest_frequency_uses_selected_native_channels_not_the_output_axis() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = thirty_two_channel_multi_row_measurement_set(root.path());
    let prefix = root.path().join("selected-rest-frequency");
    let mut imaging = request(measurement_set, prefix.clone(), ContinuumAlgorithm::Dirty);
    imaging.spectral_window = Some("0:0~3".into());
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::LSRK,
            interpolation: casa_ms::CubeInterpolation::Nearest,
            start: Some(CubeAxisValue::Channel(0)),
            width: Some(CubeAxisValue::Channel(1)),
            ..CubeAxisConfig::default()
        },
        output_channels: Some(2),
    };
    imaging
        .task_requirements
        .push(TaskRequirement::SpectralCube);
    let result = execute_continuum(imaging).expect("selected spectral application");
    for suffix in result.product_names {
        let product =
            PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", prefix.display())))
                .expect("spectral product");
        let coordinates = product.coordinates();
        let rest_hz = (0..coordinates.n_coordinates())
            .find_map(|index| match coordinates.coordinate(index) {
                casa_coordinates::CoordinateModel::Spectral(spectral) => {
                    Some(spectral.rest_frequency())
                }
                _ => None,
            })
            .expect("published spectral coordinate");
        assert_eq!(
            rest_hz, 44_001_500_000.0,
            "{suffix}: midpoint of selected native channels zero through three"
        );
    }
}

#[test]
fn t53_one_channel_standard_cubes_preserve_all_products() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = thirty_two_channel_multi_row_measurement_set(root.path());
    let mut products = Vec::new();
    for interpolation in [
        None,
        Some(casa_ms::CubeInterpolation::Nearest),
        Some(casa_ms::CubeInterpolation::Linear),
    ] {
        let image_name = root.path().join(format!("standard-{interpolation:?}"));
        let mut imaging = request(
            measurement_set.clone(),
            image_name.clone(),
            ContinuumAlgorithm::Dirty,
        );
        imaging.image_size = 32;
        imaging.cell_arcsec = 1.0;
        imaging.spectral_window = Some("0:0".to_string());
        imaging.field_ids = Some(vec![0, 1]);
        if let Some(interpolation) = interpolation {
            imaging.spectral_mode = SpectralImagingMode::Cube {
                axis: CubeAxisConfig {
                    outframe: FrequencyRef::LSRK,
                    interpolation,
                    start: Some(CubeAxisValue::Channel(0)),
                    width: Some(CubeAxisValue::Channel(1)),
                    ..CubeAxisConfig::default()
                },
                output_channels: Some(1),
            };
            imaging
                .task_requirements
                .push(TaskRequirement::SpectralCube);
        }
        let result = execute_continuum(imaging)
            .unwrap_or_else(|error| panic!("standard {interpolation:?}: {error}"));
        assert!(
            result
                .outcome
                .output
                .scientific
                .normal_state()
                .sum_weights()
                .iter()
                .all(|weight| *weight > 0.0)
        );
        products.push((image_name, result.product_names));
    }
    // Plan decision D2 grids channel-local cubes in single precision and the
    // continuum in double, so the one-channel cube equals the continuum
    // image to single-precision accumulation, not bit for bit.
    for candidate in &products[1..] {
        assert_eq!(products[0].1, candidate.1, "standard product inventory");
        for suffix in &products[0].1 {
            let (continuum_shape, continuum) = read_product(&products[0].0, suffix);
            let (cube_shape, cube) = read_product(&candidate.0, suffix);
            assert_eq!(
                continuum_shape, cube_shape,
                "standard single-channel {suffix}"
            );
            let scale = continuum
                .iter()
                .fold(0.0_f32, |peak, value| peak.max(value.abs()));
            let difference = continuum
                .iter()
                .zip(cube.iter())
                .fold(0.0_f32, |worst, (a, b)| worst.max((a - b).abs()));
            assert!(
                difference <= 1.0e-5 * scale,
                "standard single-channel {suffix}: difference {difference}, peak {scale}"
            );
        }
    }
}

fn read_product(prefix: &Path, suffix: &str) -> (Vec<usize>, ArrayD<f32>) {
    let image = PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", prefix.display())))
        .expect("open complete application product");
    let shape = image.shape().to_vec();
    let values = image
        .get_slice(&[0, 0, 0, 0], &shape)
        .expect("read complete application product");
    (shape, values)
}

#[test]
fn t53_nonidentity_linear_sampling_preserves_affine_spectra() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let mut products = Vec::new();
    for unit in [true, false] {
        let case = root.path().join(format!("standard-{unit}"));
        std::fs::create_dir(&case).expect("case directory");
        let measurement_set = thirty_two_channel_multi_row_measurement_set(&case);
        if unit {
            let mut ms = MeasurementSet::open(&measurement_set).expect("unit spectrum MS");
            for row in 0..ms.row_count() {
                ms.main_table_mut()
                    .row_accessor_mut()
                    .set_cell(
                        row,
                        "DATA",
                        Value::Array(ArrayValue::Complex32(ArrayD::from_elem(
                            vec![1, 32],
                            Complex32::new(1.0, 0.0),
                        ))),
                    )
                    .expect("constant unit spectrum");
            }
            ms.save().expect("save unit spectrum");
        }
        let image_name = case.join("image");
        let mut imaging = request(
            measurement_set,
            image_name.clone(),
            ContinuumAlgorithm::Dirty,
        );
        imaging.image_size = 64;
        imaging.cell_arcsec = 1.0;
        imaging.spectral_window = Some("0:0~1".to_string());
        imaging.channel_count = Some(2);
        let first_hz = 44.0e9;
        imaging.spectral_mode = SpectralImagingMode::Cube {
            axis: CubeAxisConfig {
                outframe: FrequencyRef::TOPO,
                interpolation: casa_ms::CubeInterpolation::Linear,
                start: Some(CubeAxisValue::FrequencyHz {
                    hz: first_hz + 250_000.0,
                    frame: Some(FrequencyRef::TOPO),
                }),
                width: Some(CubeAxisValue::FrequencyHz {
                    hz: 500_000.0,
                    frame: Some(FrequencyRef::TOPO),
                }),
                ..CubeAxisConfig::default()
            },
            output_channels: Some(2),
        };
        imaging.task_requirements = vec![TaskRequirement::SerialCpu, TaskRequirement::SpectralCube];
        imaging.resource_policy = casa_imaging_runtime::ResourcePolicy::Explicit(
            casa_imaging_runtime::ResourceOverride {
                workers: Some(1),
                ..casa_imaging_runtime::ResourceOverride::default()
            },
        );
        let result = execute_continuum(imaging)
            .unwrap_or_else(|error| panic!("standard affine={}: {error}", !unit));
        assert!(
            result
                .outcome
                .output
                .scientific
                .normal_state()
                .sum_weights()
                .iter()
                .all(|weight| *weight > 0.0),
            "standard must cover both output channels"
        );
        products.push((image_name, result.product_names));
    }
    assert_eq!(
        products[0].1, products[1].1,
        "standard complete product inventory"
    );
    for suffix in &products[0].1 {
        let (shape, unit) = read_product(&products[0].0, suffix);
        let (actual_shape, actual) = read_product(&products[1].0, suffix);
        assert_eq!(shape, actual_shape);
        assert_eq!(shape[3], 2, "two output planes for {suffix}");
        if matches!(suffix.as_str(), ".residual" | ".image") {
            // The source spectrum rises from one to two; output channels
            // sample one quarter and three quarters of that interval.
            for (channel, factor) in [1.25_f64, 1.75].into_iter().enumerate() {
                let expected = unit
                    .index_axis(ndarray::Axis(3), channel)
                    .mapv(|value| f64::from(value) * factor);
                let actual = actual.index_axis(ndarray::Axis(3), channel);
                let power = expected.iter().map(|value| value * value).sum::<f64>();
                let error = actual
                    .iter()
                    .zip(&expected)
                    .map(|(actual, expected)| (f64::from(*actual) - expected).powi(2))
                    .sum::<f64>();
                assert!(power > 0.0, "nonzero {suffix} channel {channel}");
                assert!(
                    (error / power).sqrt() < 2.0e-6,
                    "{suffix} channel {channel}: affine spectral NRMS {}",
                    (error / power).sqrt()
                );
            }
        } else {
            assert_eq!(unit, actual, "spectrum-independent {suffix}");
        }
    }
}
