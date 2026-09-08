// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn t53_one_channel_native_aw_cube_reduces_to_the_single_plane_law() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = native_evla_measurement_set(root.path());
    let surface = root.path().join("evla.surface");
    let text = (0..=125)
        .map(|i| {
            let r = i as f64 / 10.0;
            format!("{r} {} {}\n", r * r / 36.0, r / 18.0)
        })
        .collect::<String>();
    std::fs::write(&surface, text).expect("explicit synthetic dish surface");
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
        let mut aw = aw_projection(PathBuf::new(), false);
        aw.source = casa_imaging_application::ContinuumAwCfSource::NativeEvla(
            casa_imaging_application::NativeEvlaAwCache {
                root: root.path().join(format!("{name}-cf")),
                surface: surface.clone(),
                policy: casa_imaging_application::NativeAwCachePolicy::GenerateMissing,
                working_size: 128,
                oversampling: 4,
                cache_bytes: 64 << 20,
                maximum_cells: 128,
            },
        );
        imaging.aw_projection = Some(aw);
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
        let read = |prefix: &Path| {
            let product =
                PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", prefix.display())))
                    .expect("reopen native AW product");
            let shape = product.shape().to_vec();
            let values = product
                .get_slice(&[0, 0, 0, 0], &shape)
                .expect("full product values");
            (shape, values)
        };
        assert_eq!(
            read(&products[0].0),
            read(&products[1].0),
            "one-channel law for {suffix}"
        );
    }
}
