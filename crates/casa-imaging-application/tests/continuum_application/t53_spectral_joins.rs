// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
#[ignore = "requires the CASA-staged T53 shared-phase tiled-UVW fixture"]
fn t53_w_cube_reads_the_current_uvw_column_after_casa_storage_replacement() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let source = PathBuf::from(std::env::var("CASA_RS_T53_DATA_ROOT").expect("T53 fixture root"))
        .join("refim-withline-shared-phase-tiled-v2.ms");
    let staged = root.path().join("input.ms");
    assert!(
        std::process::Command::new("cp")
            .arg("-R")
            .arg(source)
            .arg(&staged)
            .status()
            .expect("copy immutable fixture")
            .success()
    );
    initialize_measurement_set_owner_manifest(&staged).expect("initialize staged owner");
    let mut imaging = request(
        staged,
        root.path().join("w-cube"),
        ContinuumAlgorithm::Dirty,
    );
    imaging.field_ids = Some(vec![0, 1]);
    imaging.image_size = 32;
    imaging.cell_arcsec = 8.0;
    imaging.spectral_window = Some("0:0~3".into());
    imaging.w_projection_planes = Some(8);
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            interpolation: casa_ms::CubeInterpolation::Linear,
            start: Some(CubeAxisValue::Channel(1)),
            width: Some(CubeAxisValue::Channel(1)),
            ..CubeAxisConfig::default()
        },
        output_channels: Some(2),
    };
    imaging.task_requirements = vec![
        TaskRequirement::SerialCpu,
        TaskRequirement::SpectralCube,
        TaskRequirement::WProjection,
        TaskRequirement::WProjectionPlanes,
    ];
    let output =
        execute_continuum(imaging).expect("read current UVW binding, not its renamed origin");
    let weights = output
        .outcome
        .output
        .scientific
        .normal_state()
        .sum_weights();
    assert_eq!(weights.len(), 2);
    assert!(weights.iter().all(|weight| *weight > 0.0));
}

#[test]
fn t53_mosaic_cube_publishes_the_complete_casa_product_inventory() {
    if !isolated_case(
        "t53_mosaic_cube_publishes_the_complete_casa_product_inventory",
        "mosaic",
    ) {
        return;
    }
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let mut imaging = request(
        two_pointing_alma_spectral_measurement_set(root.path()),
        root.path().join("mosaic"),
        ContinuumAlgorithm::Dirty,
    );
    imaging.field_ids = Some(vec![0, 1]);
    imaging.spectral_window = Some("0:0~3".into());
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            interpolation: casa_ms::CubeInterpolation::Nearest,
            start: Some(CubeAxisValue::Channel(1)),
            width: Some(CubeAxisValue::Channel(1)),
            ..CubeAxisConfig::default()
        },
        output_channels: Some(2),
    };
    imaging.task_requirements.extend([
        TaskRequirement::SpectralCube,
        TaskRequirement::MosaicGridder,
    ]);
    imaging.normalization = casa_imaging_model::ProductNormalization::FlatNoise;
    imaging.write_primary_beam = true;
    imaging.pbcor = true;
    let mut products = execute_continuum(imaging)
        .expect("mosaic spectral application")
        .product_names;
    products.sort();
    assert_eq!(
        products,
        [
            ".image",
            ".image.pbcor",
            ".model",
            ".pb",
            ".psf",
            ".residual",
            ".sumwt",
            ".weight"
        ]
    );
}

#[test]
fn t53_cube_rest_frequency_uses_selected_native_channels_not_the_output_axis() {
    if !isolated_case(
        "t53_cube_rest_frequency_uses_selected_native_channels_not_the_output_axis",
        "standard",
    ) {
        return;
    }
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
fn t53_one_channel_standard_w_and_mosaic_cubes_preserve_all_products() {
    if !isolated_case(
        "t53_one_channel_standard_w_and_mosaic_cubes_preserve_all_products",
        "direction-independent",
    ) {
        return;
    }
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let standard_ms = thirty_two_channel_multi_row_measurement_set(root.path());
    let mosaic_ms = two_pointing_alma_spectral_measurement_set(root.path());
    for family in ["standard", "w", "mosaic"] {
        let mut products = Vec::new();
        for interpolation in [
            None,
            Some(casa_ms::CubeInterpolation::Nearest),
            Some(casa_ms::CubeInterpolation::Linear),
            Some(casa_ms::CubeInterpolation::Cubic),
        ] {
            let image_name = root.path().join(format!("{family}-{interpolation:?}"));
            let mut imaging = request(
                if family == "mosaic" {
                    mosaic_ms.clone()
                } else {
                    standard_ms.clone()
                },
                image_name.clone(),
                ContinuumAlgorithm::Dirty,
            );
            imaging.image_size = 32;
            imaging.cell_arcsec = 1.0;
            imaging.spectral_window = Some("0:0".to_string());
            imaging.field_ids = Some(vec![0, 1]);
            if family == "w" {
                imaging.w_projection_planes = Some(5);
                imaging.task_requirements = vec![
                    TaskRequirement::WProjection,
                    TaskRequirement::WProjectionPlanes,
                ];
            } else if family == "mosaic" {
                imaging.task_requirements = vec![TaskRequirement::MosaicGridder];
                imaging.normalization = casa_imaging_model::ProductNormalization::FlatNoise;
                imaging.write_primary_beam = true;
                imaging.pbcor = true;
            }
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
                .unwrap_or_else(|error| panic!("{family} {interpolation:?}: {error}"));
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
        for candidate in &products[1..] {
            assert_eq!(products[0].1, candidate.1, "{family} product inventory");
            for suffix in &products[0].1 {
                assert_eq!(
                    read_product(&products[0].0, suffix),
                    read_product(&candidate.0, suffix),
                    "{family} single-channel {suffix}"
                );
            }
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

fn native_aw_projection(root: &Path) -> ContinuumAwProjection {
    std::fs::create_dir_all(root).expect("native AW fixture directory");
    let surface = root.join("evla.surface");
    let text = (0..=125)
        .map(|i| {
            let r = i as f64 / 10.0;
            format!("{r} {} {}\n", r * r / 36.0, r / 18.0)
        })
        .collect::<String>();
    std::fs::write(&surface, text).expect("explicit synthetic dish surface");
    let mut aw = aw_projection(PathBuf::new(), false);
    aw.source = casa_imaging_application::ContinuumAwCfSource::NativeEvla(
        casa_imaging_application::NativeEvlaAwCache {
            root: root.join("native-cf"),
            surface,
            policy: casa_imaging_application::NativeAwCachePolicy::GenerateMissing,
            working_size: 128,
            oversampling: 4,
            cache_bytes: 64 << 20,
            maximum_cells: 128,
        },
    );
    aw
}

#[test]
fn t53_nonidentity_linear_sampling_preserves_affine_spectra_across_all_families() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    for family in ["standard", "w", "mosaic", "aw"] {
        if !isolated_case(
            "t53_nonidentity_linear_sampling_preserves_affine_spectra_across_all_families",
            family,
        ) {
            continue;
        }
        let mut products = Vec::new();
        for unit in [true, false] {
            let case = root.path().join(format!("{family}-{unit}"));
            std::fs::create_dir(&case).expect("case directory");
            let measurement_set = match family {
                "aw" => native_evla_measurement_set(&case),
                "mosaic" => two_pointing_alma_spectral_measurement_set(&case),
                _ => thirty_two_channel_multi_row_measurement_set(&case),
            };
            if unit {
                let mut ms = MeasurementSet::open(&measurement_set).expect("unit spectrum MS");
                let shape = if family == "aw" {
                    vec![4, 2]
                } else {
                    vec![1, 32]
                };
                for row in 0..ms.row_count() {
                    ms.main_table_mut()
                        .row_accessor_mut()
                        .set_cell(
                            row,
                            "DATA",
                            Value::Array(ArrayValue::Complex32(ArrayD::from_elem(
                                shape.clone(),
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
            imaging.cell_arcsec = if family == "aw" { 60.0 } else { 1.0 };
            imaging.spectral_window = Some("0:0~1".to_string());
            imaging.channel_count = Some(2);
            let first_hz = if family == "aw" { 3.0e9 } else { 44.0e9 };
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
            imaging.task_requirements =
                vec![TaskRequirement::SerialCpu, TaskRequirement::SpectralCube];
            imaging.resource_policy = casa_imaging_runtime::ResourcePolicy::Explicit(
                casa_imaging_runtime::ResourceOverride {
                    workers: Some(1),
                    ..casa_imaging_runtime::ResourceOverride::default()
                },
            );
            if family == "w" {
                imaging.w_projection_planes = Some(5);
                imaging.task_requirements.extend([
                    TaskRequirement::WProjection,
                    TaskRequirement::WProjectionPlanes,
                ]);
            } else if family == "mosaic" {
                imaging
                    .task_requirements
                    .push(TaskRequirement::MosaicGridder);
                imaging.normalization = casa_imaging_model::ProductNormalization::FlatNoise;
                imaging.write_primary_beam = true;
                imaging.pbcor = true;
            } else if family == "aw" {
                imaging.aw_projection = Some(native_aw_projection(&case));
                imaging
                    .task_requirements
                    .push(TaskRequirement::AwProjection);
                imaging.write_primary_beam = true;
            }
            let result = execute_continuum(imaging)
                .unwrap_or_else(|error| panic!("{family} affine={}: {error}", !unit));
            assert!(
                result
                    .outcome
                    .output
                    .scientific
                    .normal_state()
                    .sum_weights()
                    .iter()
                    .all(|weight| *weight > 0.0),
                "{family} must cover both output channels"
            );
            products.push((image_name, result.product_names));
        }
        assert_eq!(
            products[0].1, products[1].1,
            "{family} complete product inventory"
        );
        for suffix in &products[0].1 {
            let (shape, unit) = read_product(&products[0].0, suffix);
            let (actual_shape, actual) = read_product(&products[1].0, suffix);
            assert_eq!(shape, actual_shape);
            assert_eq!(shape[3], 2, "{family} two output planes for {suffix}");
            if matches!(suffix.as_str(), ".residual" | ".image" | ".image.pbcor") {
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
                    assert!(power > 0.0, "{family} nonzero {suffix} channel {channel}");
                    assert!(
                        (error / power).sqrt() < 2.0e-6,
                        "{family} {suffix} channel {channel}: affine spectral NRMS {}",
                        (error / power).sqrt()
                    );
                }
            } else {
                assert_eq!(unit, actual, "{family} spectrum-independent {suffix}");
            }
        }
    }
}

#[test]
fn t53_one_channel_native_aw_cube_reduces_to_the_single_plane_law() {
    if !isolated_case(
        "t53_one_channel_native_aw_cube_reduces_to_the_single_plane_law",
        "aw",
    ) {
        return;
    }
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = native_evla_measurement_set(root.path());
    let mut products = Vec::new();
    for (name, cube) in [("single-plane", false), ("one-channel-cubic", true)] {
        let image_name = root.path().join(name);
        let mut imaging = request(
            measurement_set.clone(),
            image_name.clone(),
            ContinuumAlgorithm::Dirty,
        );
        imaging.image_size = 64;
        imaging.cell_arcsec = 60.0;
        imaging.spectral_window = Some("0:0".to_string());
        imaging.aw_projection = Some(native_aw_projection(
            &root.path().join(format!("{name}-cf")),
        ));
        imaging.task_requirements = vec![TaskRequirement::SerialCpu, TaskRequirement::AwProjection];
        imaging.resource_policy = casa_imaging_runtime::ResourcePolicy::Explicit(
            casa_imaging_runtime::ResourceOverride {
                workers: Some(1),
                ..casa_imaging_runtime::ResourceOverride::default()
            },
        );
        imaging.write_primary_beam = true;
        if cube {
            imaging.spectral_mode = SpectralImagingMode::Cube {
                axis: CubeAxisConfig {
                    outframe: FrequencyRef::TOPO,
                    interpolation: casa_ms::CubeInterpolation::Cubic,
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
        let result = execute_continuum(imaging).expect("one-channel AW production execution");
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
    assert_eq!(products[0].1, products[1].1);
    for suffix in &products[0].1 {
        assert_eq!(
            read_product(&products[0].0, suffix),
            read_product(&products[1].0, suffix),
            "one-channel law for {suffix}"
        );
    }
}

// The production Resource Authority admits one immutable storage calibration
// per process. AW includes its additional prepared-reader queue and IOPS rate.
fn isolated_case(test: &str, family: &str) -> bool {
    let selected = format!("{test}:{family}");
    if let Ok(active) = std::env::var("CASA_RS_T53_ISOLATED_CASE") {
        return active == selected;
    }
    let status = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            format!("t53_spectral_joins::{test}"),
            "--exact".into(),
            "--nocapture".into(),
        ])
        .env("CASA_RS_T53_ISOLATED_CASE", selected)
        .status()
        .expect("isolated production application process");
    assert!(
        status.success(),
        "{test} {family}: isolated execution failed"
    );
    false
}
