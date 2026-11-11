// SPDX-License-Identifier: LGPL-3.0-or-later
//! Numerical quick PSFs against independent CASA 6.7.6.14 images (#1053/#1025).

use std::io::Read;
use std::path::{Path, PathBuf};

use casa_components::{
    ComponentList, ComponentShape, Flux, SkyComponent, SpectralKind, SpectralModel,
};
use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    StokesCoordinate, StokesType,
};
use casa_images::PagedImage;
use casa_imaging_runtime::{HostResources, ResourcePolicy};
use casa_simulation::simobserve::{SimobserveParameters, SimobserveRequest, simobserve};
use casa_simulation::{BackendRequest, SessionResources, SimulatorOptions};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::measures::{EpochRef, MEpoch};
use flate2::read::GzDecoder;
use ndarray::{ArrayD, IxDyn};
use serde_json::Value;

const CENTRE: [f64; 2] = [2.617_993_877_991_494_4, -0.523_598_775_598_298_8];

fn fixture() -> Value {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/simobserve_graphics_casa.json.gz")[..])
        .read_to_string(&mut text)
        .expect("CASA fixture");
    let value: Value = serde_json::from_str(&text).expect("CASA JSON");
    assert_eq!(value["casa_version"], "6.7.6.14");
    value
}

fn inputs(root: &Path) -> (PathBuf, PathBuf, PathBuf) {
    let image = root.join("model.image");
    if image.exists() {
        return (image, root.join("point.cl"), root.join("small.cfg"));
    }
    let mut cs = CoordinateSystem::new();
    cs.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        CENTRE,
        [-2e-6, 2e-6],
        [32.0, 32.0],
    ));
    cs.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    cs.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        230e9,
        1e9,
        0.0,
        230e9,
    ));
    let mut model = PagedImage::<f32>::create(vec![64, 64, 1, 1], cs, &image).expect("image");
    let mut pixels = ArrayD::zeros(IxDyn(&[64, 64, 1, 1]));
    pixels[IxDyn(&[32, 32, 0, 0])] = 1.25;
    pixels[IxDyn(&[41, 28, 0, 0])] = 0.75;
    model.put_slice(&pixels, &[0; 4]).expect("pixels");
    model.set_units("Jy/pixel").expect("units");
    model.save().expect("save");
    let component = root.join("point.cl");
    let mut list = ComponentList::new();
    list.push(SkyComponent::new(
        Flux::stokes_jy(1.0, 0.0, 0.0, 0.0),
        ComponentShape::point(MDirection::from_angles(
            CENTRE[0],
            CENTRE[1],
            DirectionRef::J2000,
        )),
        SpectralModel::new(
            MFrequency::new(230e9, FrequencyRef::LSRK),
            SpectralKind::Constant,
        ),
    ));
    list.write_table(&component).expect("component");
    let pads = [
        (-40, -25),
        (30, -40),
        (60, 10),
        (-50, 45),
        (10, 60),
        (0, 0),
        (-80, -70),
        (90, 80),
    ];
    let config = root.join("small.cfg");
    let mut text = String::from("# observatory=ALMA\n# coordsys=XYZ\n");
    for (i, (x, y)) in pads.into_iter().enumerate() {
        text.push_str(&format!(
            "{:.6} {:.6} -2481681.150540 12.0 A{i:02}\n",
            2_225_061.163_6 + f64::from(x),
            -5_440_057.369_94 + f64::from(y)
        ));
    }
    std::fs::write(&config, text).expect("config");
    (image, component, config)
}

fn run(root: &Path, name: &str, workers: usize, graphics: &str) -> PathBuf {
    run_on(root, name, workers, graphics, BackendRequest::Cpu)
}

fn run_on(
    root: &Path,
    name: &str,
    workers: usize,
    graphics: &str,
    backend: BackendRequest,
) -> PathBuf {
    simobserve(&request(root, name, workers, graphics, backend)).expect("simobserve");
    root.join(name).join(format!("{name}.small.quick.psf"))
}

fn request(
    root: &Path,
    name: &str,
    workers: usize,
    graphics: &str,
    backend: BackendRequest,
) -> SimobserveRequest {
    let (image, components, config) = inputs(root);
    let project = root.join(name);
    let mut parameters = SimobserveParameters {
        project: project.display().to_string(),
        skymodel: image.display().to_string(),
        antennalist: config.display().to_string(),
        mapsize: vec!["1arcsec".into()],
        integration: "10s".into(),
        totaltime: "40s".into(),
        thermalnoise: String::new(),
        graphics: graphics.into(),
        hourangle: "35s".into(),
        ..SimobserveParameters::default()
    };
    if name.starts_with("int_components") {
        parameters.skymodel.clear();
        parameters.complist = components.display().to_string();
        parameters.compwidth = "1GHz".into();
    }
    if name == "sd_image" {
        parameters.obsmode = "sd".into();
        parameters.sdantlist.clone_from(&parameters.antennalist);
        parameters.antennalist.clear();
        parameters.maptype = "square".into();
        parameters.incell = "2arcsec".into();
    }
    let resources = SessionResources::pinned(
        HostResources {
            threads: 8,
            performance_cores: 8,
            available_memory: 8 << 30,
            metal: backend == BackendRequest::Metal,
        },
        ResourcePolicy::Explicit {
            workers,
            memory: 2 << 30,
        },
        backend,
    )
    .expect("resources");
    SimobserveRequest {
        parameters,
        data_roots: Vec::new(),
        options: SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources,
            now: MEpoch::from_mjd(60_000.0, EpochRef::UTC),
        },
    }
}

fn compare_metal(reference: &Value, cpu: &Path, name: &str) {
    let directory = tempfile::tempdir().expect("directory");
    let metal = run_on(directory.path(), name, 3, "file", BackendRequest::Metal);
    compare(reference, &metal);
    let read = |path: &Path| {
        let image = PagedImage::<f32>::open(path).expect("PSF");
        image.get_slice(&[0; 4], image.shape()).expect("pixels")
    };
    let (metal, cpu) = (read(&metal), read(cpu));
    let (mut error, mut peak) = (0.0_f32, 0.0_f32);
    assert_eq!(metal.shape(), cpu.shape());
    for (&m, &c) in metal.iter().zip(&cpu) {
        error = error.max((m - c).abs());
        peak = peak.max(c.abs());
    }
    assert!(peak > 0.0);
    let ratio = error / peak;
    println!("{name}: quick PSF Metal-session/CPU-session maximum relative error {ratio:.9e}");
    assert!(ratio <= 1e-4, "Metal/CPU PSF law: {ratio} exceeds 1e-4");
}

fn flatten(value: &Value, out: &mut Vec<f64>) {
    if let Some(array) = value.as_array() {
        for value in array {
            flatten(value, out);
        }
    } else {
        out.push(value.as_f64().expect("CASA pixel"));
    }
}

fn compare(reference: &Value, path: &Path) {
    assert!(
        path.is_dir(),
        "the requested numerical quick PSF must exist"
    );
    let image = PagedImage::<f32>::open(path).expect("PSF");
    let shape: Vec<_> = reference["shape"]
        .as_array()
        .expect("shape")
        .iter()
        .map(|n| n.as_u64().expect("dimension") as usize)
        .collect();
    assert_eq!(image.shape(), shape);
    assert_eq!(image.units(), reference["unit"].as_str().expect("unit"));
    assert_eq!(
        image.image_info().expect("info").beam_set.is_empty(),
        reference["beam"]
            .as_object()
            .expect("beam metadata")
            .is_empty()
    );
    for (field, values) in [
        (
            "reference",
            (0..3)
                .flat_map(|i| image.coordinates().coordinate(i).reference_value())
                .collect::<Vec<_>>(),
        ),
        (
            "increment",
            (0..3)
                .flat_map(|i| image.coordinates().coordinate(i).increment())
                .collect::<Vec<_>>(),
        ),
    ] {
        let expected = reference[field].as_array().expect("CASA coordinates");
        assert_eq!(values.len(), expected.len());
        for (value, expected) in values.iter().zip(expected) {
            let expected = expected.as_f64().expect("CASA coordinate");
            assert!(
                value.is_finite() && (value - expected).abs() <= 1e-12 * expected.abs().max(1.0),
                "{field}: {value} versus CASA {expected}"
            );
        }
    }
    let mut expected = Vec::new();
    flatten(&reference["pixels"], &mut expected);
    let actual = image.get_slice(&[0; 4], &shape).expect("pixels");
    assert_eq!(actual.len(), expected.len());
    let (mut error, mut power) = (0.0, 0.0);
    for (&actual, expected) in actual.iter().zip(expected) {
        assert!(actual.is_finite() && expected.is_finite());
        error += (f64::from(actual) - expected).powi(2);
        power += expected.powi(2);
    }
    assert!(power > 0.0);
    let nrms = (error / power).sqrt();
    println!("{}: quick PSF DATA NRMS {nrms:.9e}", path.display());
    assert!(nrms <= 1e-3, "quick PSF NRMS {nrms} exceeds 1e-3");
}

fn default_tclean_control(case: &Value, quick: &Path) {
    use casa_imaging_application::{
        BackendRequest as ImagingBackend, Cancel, ImagingRequest, RunContext, execute,
    };
    let project = quick.parent().expect("project");
    let prefix = project.join("default-control");
    let values = serde_json::json!({
        "vis": project.join("int_image.small.ms"), "imagename": prefix,
        "imsize": 128, "cell": "2e-6rad", "weighting": "natural", "datacolumn": "data",
        "niter": 0, "calcres": false, "restoration": false,
    });
    let resolved = casa_task_runtime::resolve_plain_values(
        casa_provider_contracts::builtin_surface_bundle("tclean").expect("surface"),
        std::env::current_dir().expect("workspace"),
        values.as_object().expect("object"),
    )
    .expect("resolved control");
    let mut request = ImagingRequest::from_tclean(&resolved).expect("request");
    request.backend = ImagingBackend::Cpu;
    execute(
        &request,
        RunContext {
            host: HostResources::detect().expect("host"),
            policy: ResourcePolicy::Explicit {
                workers: 3,
                memory: 2 << 30,
            },
            cancel: Cancel::new(),
            summary: None,
        },
    )
    .expect("ordinary imaging control");
    compare(&case["default_tclean_psf"], &prefix.with_extension("psf"));
    let weight = PagedImage::<f32>::open(prefix.with_extension("sumwt")).expect("sumwt");
    let actual = weight.get_slice(&[0; 4], weight.shape()).expect("weight");
    let mut expected = Vec::new();
    flatten(&case["default_tclean_sumwt"]["pixels"], &mut expected);
    assert_eq!(actual.len(), expected.len());
    for (&actual, expected) in actual.iter().zip(expected) {
        assert!(actual.is_finite() && expected.is_finite() && expected > 0.0);
        assert!((f64::from(actual) - expected).abs() <= 5e-5 * expected.abs());
    }
}

#[test]
fn quick_psfs_match_casa_for_image_and_component_models() {
    let oracle = fixture();
    let metal = HostResources::detect().expect("host").metal;
    if !metal {
        eprintln!("Metal quick PSF comparisons skipped: no device");
    }
    for case in oracle["cases"].as_array().expect("cases") {
        let Some(reference) = case.get("psf") else {
            continue;
        };
        for workers in [1, 3] {
            let debug = std::env::var_os("CASA_RS_QUICK_PSF_DEBUG_DIR");
            let directory = if let Some(path) = &debug {
                std::fs::create_dir_all(path).expect("debug directory");
                tempfile::Builder::new()
                    .prefix("quick-")
                    .tempdir_in(path)
                    .expect("directory")
            } else {
                tempfile::tempdir().expect("directory")
            };
            let name = case["name"].as_str().expect("case");
            let psf = run(directory.path(), name, workers, "file");
            let root = directory.path().to_path_buf();
            if debug.is_some() {
                let _kept = directory.keep();
            }
            compare(reference, &psf);
            if workers == 3 && case.get("default_tclean_psf").is_some() {
                default_tclean_control(case, &psf);
            }
            if metal && workers == 3 {
                compare_metal(reference, &psf, name);
            }
            assert!(
                std::fs::read_dir(root.join(name))
                    .expect("project")
                    .all(|entry| !entry
                        .expect("entry")
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".quick-psf-"))
            );
        }
    }
    graphics_mode_checks(&oracle);
}

fn graphics_mode_checks(oracle: &Value) {
    let directory = tempfile::tempdir().expect("directory");
    assert!(!run(directory.path(), "int_image_none", 1, "none").exists());
    let directory = tempfile::tempdir().expect("directory");
    assert!(!run(directory.path(), "sd_image", 1, "file").exists());
    let reference = &oracle["cases"]
        .as_array()
        .expect("cases")
        .iter()
        .find(|case| case["name"] == "int_image")
        .expect("image case")["psf"];
    for graphics in ["screen", "both"] {
        let directory = tempfile::tempdir().expect("directory");
        compare(
            reference,
            &run(
                directory.path(),
                &format!("int_image_{graphics}"),
                1,
                graphics,
            ),
        );
    }
}

#[test]
fn repeated_simobserve_replaces_the_complete_quick_psf() {
    let oracle = fixture();
    let reference = &oracle["cases"]
        .as_array()
        .expect("cases")
        .iter()
        .find(|case| case["name"] == "int_image")
        .expect("image case")["psf"];
    let directory = tempfile::tempdir().expect("directory");
    let path = run(directory.path(), "int_image", 1, "file");
    let mut old = PagedImage::<f32>::open(&path).expect("first PSF");
    let empty = ArrayD::zeros(IxDyn(old.shape()));
    old.put_slice(&empty, &[0; 4]).expect("old pixels");
    old.save().expect("old image");
    drop(old);
    std::fs::write(path.join("previous-generation"), "old product").expect("marker");
    assert_eq!(path, run(directory.path(), "int_image", 1, "file"));
    assert!(!path.join("previous-generation").exists());
    compare(reference, &path);
}

#[test]
fn complete_native_figures_match_casas_independent_plot_data() {
    use casa_simulation::simobserve::simobserve_with_plots;
    let oracle = fixture();
    for case in oracle["cases"].as_array().expect("cases") {
        let name = case["name"].as_str().expect("name");
        for workers in [1, 3] {
            let debug = std::env::var_os("CASA_RS_GRAPHICS_DEBUG_DIR");
            let directory = if let Some(path) = &debug {
                std::fs::create_dir_all(path).expect("debug");
                tempfile::Builder::new()
                    .prefix(&format!("{name}-w{workers}-"))
                    .tempdir_in(path)
                    .expect("directory")
            } else {
                tempfile::tempdir().expect("directory")
            };
            let payload = directory.path().join("arrays.bin");
            std::fs::write(&payload, []).expect("payload");
            let outcome = simobserve_with_plots(
                &request(directory.path(), name, workers, "file", BackendRequest::Cpu),
                &payload,
            )
            .expect("native figures");
            assert!(outcome.warnings.is_empty(), "{:?}", outcome.warnings);
            assert_eq!(outcome.plots.len(), 2);
            let bytes = std::fs::read(&payload).expect("bytes");
            for plot in &outcome.plots {
                plot.document
                    .validate_payload(bytes.len() as u64)
                    .expect("valid views");
                let file = plot
                    .output_file
                    .as_ref()
                    .expect("file mode")
                    .file_name()
                    .expect("name")
                    .to_string_lossy();
                let reference = &case["figures"][file.as_ref()];
                assert!(reference.is_array(), "{file} has no CASA reference");
                for (index, panel) in plot.document.panels.iter().enumerate() {
                    compare_panel(panel, &reference[index], &bytes);
                }
            }
            if debug.is_some() {
                std::fs::write(
                    directory.path().join("plots.json"),
                    serde_json::to_vec(&outcome.plots).expect("JSON"),
                )
                .expect("plots");
                let _kept = directory.keep();
            }
        }
    }
}

fn binary_values(array: &casa_ms::plot_document::PlotBinaryArray, bytes: &[u8]) -> Vec<f64> {
    use casa_ms::plot_document::PlotArrayDtype;
    let size = array.dtype.bytes();
    let strides = if array.strides_bytes.is_empty() {
        if array.shape.len() == 2 {
            vec![array.shape[1] * size, size]
        } else {
            vec![size]
        }
    } else {
        array.strides_bytes.clone()
    };
    let read = |offset: u64| {
        let offset = offset as usize;
        match array.dtype {
            PlotArrayDtype::F32Le => f64::from(f32::from_le_bytes(
                bytes[offset..offset + 4].try_into().expect("f32"),
            )),
            PlotArrayDtype::F64Le => {
                f64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("f64"))
            }
        }
    };
    let mut output = Vec::new();
    for i in 0..array.shape[0] {
        if array.shape.len() == 2 {
            for j in 0..array.shape[1] {
                output.push(read(array.offset_bytes + i * strides[0] + j * strides[1]));
            }
        } else {
            output.push(read(array.offset_bytes + i * strides[0]));
        }
    }
    output
}

fn close_values(actual: &[f64], reference: &Value, allowance: f64, context: &str) {
    let mut expected = Vec::new();
    flatten(reference, &mut expected);
    assert_eq!(actual.len(), expected.len(), "{context}: full sample count");
    let mut maximum = 0.0_f64;
    for (&actual, expected) in actual.iter().zip(expected) {
        assert!(
            actual.is_finite() && expected.is_finite(),
            "{context}: finite samples"
        );
        maximum = maximum.max((actual - expected).abs());
    }
    println!("{context}: maximum difference {maximum:.6e}");
    assert!(
        maximum <= allowance,
        "{context}: {maximum} exceeds {allowance}"
    );
}

fn compare_panel(
    panel: &casa_ms::plot_document::PlotDocumentPanel,
    reference: &Value,
    bytes: &[u8],
) {
    use casa_ms::plot_document::PlotLayerData;
    assert_eq!(panel.title, reference["title"].as_str().expect("title"));
    assert_eq!(
        panel.axes[0].label,
        reference["xlabel"].as_str().expect("xlabel")
    );
    assert_eq!(
        panel.axes[1].label,
        reference["ylabel"].as_str().expect("ylabel")
    );
    close_values(
        &[panel.axes[0].lower, panel.axes[0].upper],
        &reference["xlim"],
        1e-3,
        &format!("{} x limits", panel.id),
    );
    close_values(
        &[panel.axes[1].lower, panel.axes[1].upper],
        &reference["ylim"],
        1e-3,
        &format!("{} y limits", panel.id),
    );
    let (mut line_index, mut image_index, mut circle_index) = (0, 0, 0);
    for layer in &panel.layers {
        match layer.data.as_ref().expect("binary layer") {
            PlotLayerData::Points { x, y } => {
                let expected = &reference["lines"][line_index];
                line_index += 1;
                close_values(
                    &binary_values(x, bytes),
                    &expected["x"],
                    1e-3,
                    &format!("{} {} x", panel.id, layer.id),
                );
                close_values(
                    &binary_values(y, bytes),
                    &expected["y"],
                    1e-3,
                    &format!("{} {} y", panel.id, layer.id),
                );
            }
            PlotLayerData::Image {
                pixels,
                extent,
                range,
                ..
            } => {
                let expected = &reference["images"][image_index];
                image_index += 1;
                let actual = binary_values(pixels, bytes);
                let mut expected_pixels = Vec::new();
                flatten(&expected["pixels"], &mut expected_pixels);
                assert_eq!(actual.len(), expected_pixels.len());
                let error = actual
                    .iter()
                    .zip(&expected_pixels)
                    .map(|(a, e)| (a - e).powi(2))
                    .sum::<f64>();
                let power = expected_pixels.iter().map(|e| e * e).sum::<f64>();
                assert!(
                    power > 0.0 && (error / power).sqrt() <= 1e-3,
                    "{} full image NRMS",
                    panel.id
                );
                close_values(extent, &expected["extent"], 1e-9, "image extent");
                close_values(range, &expected["clim"], 1e-5, "image colour limits");
            }
            PlotLayerData::Circles { x, y, radii, .. } => {
                let (x, y, radii) = (
                    binary_values(x, bytes),
                    binary_values(y, bytes),
                    binary_values(radii, bytes),
                );
                for ((x, y), radius) in x.into_iter().zip(y).zip(radii) {
                    let expected = &reference["circles"][circle_index];
                    circle_index += 1;
                    close_values(&[x, y], &expected["center"], 1e-3, "circle centre");
                    assert!((radius - expected["radius"].as_f64().expect("radius")).abs() <= 1e-9);
                }
            }
        }
    }
    assert_eq!(
        line_index,
        reference["lines"].as_array().expect("lines").len()
    );
    assert_eq!(
        image_index,
        reference["images"].as_array().expect("images").len()
    );
    assert_eq!(
        circle_index,
        reference["circles"].as_array().expect("circles").len()
    );
    assert_eq!(
        panel.annotations.len(),
        reference["texts"].as_array().expect("texts").len()
    );
    for (actual, expected) in panel
        .annotations
        .iter()
        .zip(reference["texts"].as_array().expect("texts"))
    {
        assert_eq!(actual.text, expected["text"].as_str().expect("text"));
        close_values(
            &[actual.x, actual.y],
            &expected["position"],
            1e-3,
            "text position",
        );
    }
}

#[test]
fn a_relative_project_publishes_its_quick_psf() {
    let directory = tempfile::tempdir().expect("directory");
    let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .current_dir(directory.path())
        .args([
            "--exact",
            "relative_project_quick_psf_helper",
            "--ignored",
            "--nocapture",
        ])
        .output()
        .expect("relative-project child");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "subprocess helper; its parent sets the child working directory"]
fn relative_project_quick_psf_helper() {
    let path = run(Path::new("."), "int_image", 1, "file");
    let oracle = fixture();
    let reference = &oracle["cases"]
        .as_array()
        .expect("cases")
        .iter()
        .find(|case| case["name"] == "int_image")
        .expect("image case")["psf"];
    compare(reference, &path);
}
