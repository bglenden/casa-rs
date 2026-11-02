// SPDX-License-Identifier: LGPL-3.0-or-later
//! `simobserve` end to end on a small array (simulation Phase 5c, #841):
//! the products CASA's task writes, the antenna list found in the
//! configuration repository, the calibrator and its component list
//! predicted together with the model's, leakage and noise on copies, and
//! the refusals. Agreement with CASA itself is the parity harness's
//! (`tools/simulation/simobserve_parity.py`).

use std::path::{Path, PathBuf};

use casa_components::{
    ComponentList, ComponentShape, Flux, SkyComponent, SpectralKind, SpectralModel,
};
use casa_ms::MeasurementSet;
use casa_simulation::simobserve::{SimobserveParameters, SimobserveRequest, simobserve};
use casa_simulation::simutil::direction::Directions;
use casa_simulation::{ExecutionControls, SessionResources, SimulatorOptions};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::measures::{EpochRef, MEpoch};
use casa_types::{ArrayValue, Complex32};

/// Six ALMA antennas in local coordinates.
const CONFIGURATION: &str = "\
# observatory=ALMA
# coordsys=LOC (local tangent plane)
-5.850273514   -125.9985379    -1.590364043   12.   A058     DA41
-19.90369337    52.82680653    -1.892119601   12.   A023     DA42
13.45860758    -5.790196849    -2.087805181   12.   A035     DA43
40.2           60.1            -1.2           12.   A040     DA44
-60.7          10.4            -1.9           12.   A041     DA45
85.3          -40.6            -2.2           12.   A042     DA46
";

/// The point's frequency, Hz.
const FREQUENCY_HZ: f64 = 230e9;

struct Fixture {
    directory: tempfile::TempDir,
}

impl Fixture {
    /// A directory with the configuration in `data/alma/simmos` and a 1 Jy
    /// point at J2000 19h −23° in `point.cl`.
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("temporary directory");
        let repository = directory.path().join("data/alma/simmos");
        std::fs::create_dir_all(&repository).expect("the repository");
        std::fs::write(repository.join("tiny.cfg"), CONFIGURATION).expect("the configuration");
        let mut list = ComponentList::new();
        list.push(SkyComponent::new(
            Flux::stokes_jy(1.0, 0.0, 0.0, 0.0),
            ComponentShape::point(MDirection::from_angles(
                285_f64.to_radians(),
                (-23_f64).to_radians(),
                DirectionRef::J2000,
            )),
            SpectralModel::new(
                MFrequency::new(FREQUENCY_HZ, FrequencyRef::LSRK),
                SpectralKind::Constant,
            ),
        ));
        list.write_table(directory.path().join("point.cl"))
            .expect("the component list");
        Self { directory }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.directory.path().join(name)
    }

    /// CASA's defaults for a component-only run of the point on the small
    /// array, with `changes` made.
    fn parameters(&self, changes: impl FnOnce(&mut SimobserveParameters)) -> SimobserveParameters {
        let mut parameters = SimobserveParameters {
            project: self.path("p").display().to_string(),
            complist: self.path("point.cl").display().to_string(),
            compwidth: "8GHz".to_string(),
            antennalist: "tiny.cfg".to_string(),
            totaltime: "60s".to_string(),
            thermalnoise: String::new(),
            graphics: "none".to_string(),
            ..SimobserveParameters::default()
        };
        changes(&mut parameters);
        parameters
    }

    fn request(&self, parameters: SimobserveParameters) -> SimobserveRequest {
        let resources =
            SessionResources::from_controls(&ExecutionControls::default()).expect("resources");
        SimobserveRequest {
            parameters,
            data_roots: vec![self.path("data")],
            options: SimulatorOptions {
                measures: casa_test_support::deterministic_measures_provider(),
                resources,
                now: MEpoch::from_mjd(60_000.0, EpochRef::UTC),
            },
        }
    }
}

/// The names of `paths` within the project directory.
fn names(paths: &[PathBuf]) -> Vec<String> {
    paths
        .iter()
        .map(|path| {
            path.file_name()
                .expect("a name")
                .to_string_lossy()
                .into_owned()
        })
        .collect()
}

/// MAIN's DATA and FIELD_ID.
fn data(ms: &Path) -> Vec<(i32, Vec<Complex32>)> {
    let set = MeasurementSet::open(ms).expect("the MeasurementSet");
    let main = set.main_table();
    let rows: Vec<usize> = (0..main.row_count()).collect();
    let cells = main
        .column_accessor("DATA")
        .expect("DATA")
        .array_cells_owned(&rows)
        .expect("DATA cells");
    let fields = main.column_accessor("FIELD_ID").expect("FIELD_ID");
    cells
        .into_iter()
        .enumerate()
        .map(|(row, cell)| {
            let Some(ArrayValue::Complex32(values)) = cell else {
                panic!("row {row}: complex DATA")
            };
            let field = match fields
                .cell(row)
                .expect("a field")
                .scalar()
                .expect("a scalar")
            {
                casa_types::ScalarValue::Int32(field) => *field,
                other => panic!("FIELD_ID {other:?}"),
            };
            (field, values.iter().copied().collect())
        })
        .collect()
}

#[test]
fn a_component_list_alone_writes_casas_products() {
    let fixture = Fixture::new();
    let outcome = simobserve(&fixture.request(fixture.parameters(|_| {}))).expect("simobserve");
    assert_eq!(outcome.stem, "p.tiny");
    assert_eq!(
        names(&outcome.products),
        [
            "p.tiny.compskymodel",
            "p.tiny.compskymodel.flat",
            "p.tiny.ptg.txt",
            "p.tiny.ms"
        ]
    );
    assert!(outcome.products.iter().all(|path| path.exists()));
    let pointings = std::fs::read_to_string(fixture.path("p/p.tiny.ptg.txt")).expect("pointings");
    assert!(
        pointings.starts_with("#Epoch     RA          DEC      TIME[sec]\n"),
        "{pointings}"
    );
    assert!(
        pointings.contains("J2000 19:00:00.00000 -023.00.00.000000  10.0"),
        "{pointings}"
    );
    let ms = fixture.path("p/p.tiny.ms");
    let set = MeasurementSet::open(&ms).expect("the MeasurementSet");
    let window = set.spectral_window().expect("SPECTRAL_WINDOW");
    // The model's frequency, through GHz as the list's spectrum gives it.
    let reference = window.ref_frequency(0).expect("REF_FREQUENCY");
    assert!(
        (reference / FREQUENCY_HZ - 1.0).abs() < 1e-12,
        "{reference}"
    );
    // The point at the phase centre: 1 Jy at zero phase on every baseline.
    let rows = data(&ms);
    assert!(!rows.is_empty());
    for (_, values) in &rows {
        for value in values {
            assert!(
                (value.re - 1.0).abs() < 1e-4 && value.im.abs() < 1e-4,
                "{value}"
            );
        }
    }
}

/// Whether `path` is a symbolic link.
fn is_link(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink())
}

/// A calibrator, leakage and noise, so the run writes every table it can.
fn with_copies(parameters: &mut SimobserveParameters) {
    parameters.leakage = 0.02;
    parameters.thermalnoise = "tsys-manual".to_string();
    parameters.caldirection = "J2000 19h00m00 -22d00m00".to_string();
    parameters.calflux = "2Jy".to_string();
    parameters.totaltime = "2".to_string();
}

#[test]
fn a_calibrator_leakage_and_noise_write_their_copies() {
    let fixture = Fixture::new();
    let parameters = fixture.parameters(with_copies);
    let outcome = simobserve(&fixture.request(parameters)).expect("simobserve");
    assert_eq!(
        names(&outcome.products),
        [
            "p.tiny.compskymodel",
            "p.tiny.compskymodel.flat",
            "p.tiny.ptg.txt",
            "p.tiny.cal.cclist",
            "p.tiny.ms",
            "p.tiny.fullpol.ms",
            "p.tiny.fullpol.cal",
            "p.tiny.noisier.ms",
        ]
    );
    // The calibrator's scans are predicted, with the map's list: the
    // calibrator at their phase centre, 2 Jy in the parallel hands.
    let observed = data(&fixture.path("p/p.tiny.ms"));
    let calibrator: Vec<_> = observed.iter().filter(|(field, _)| *field == 1).collect();
    assert!(!calibrator.is_empty());
    for (_, values) in calibrator {
        for value in [values[0], values[3]] {
            assert!(
                (value.re - 2.0).abs() < 1e-3 && value.im.abs() < 1e-3,
                "{value}"
            );
        }
    }
    let leaked = data(&fixture.path("p/p.tiny.fullpol.ms"));
    let noisier = data(&fixture.path("p/p.tiny.noisier.ms"));
    assert_eq!(leaked.len(), noisier.len());
    let changed = leaked
        .iter()
        .zip(&noisier)
        .filter(|((_, a), (_, b))| a != b)
        .count();
    assert_eq!(changed, leaked.len(), "the noise reaches every row");
    assert!(!fixture.path("p/p.tiny.noisier.T.cal").exists());
}

/// The pointing file replaces a symbolic link in the project, the link
/// itself (#920 review): copied from `ptgfile` over a link to a file outside
/// the project, which is left as it was, and laid out over a dangling link,
/// whose target is not made. CASA writes through both.
#[cfg(unix)]
#[test]
fn a_pointing_file_replaces_a_link_not_its_target() {
    let fixture = Fixture::new();
    simobserve(&fixture.request(fixture.parameters(|_| {}))).expect("simobserve");
    let pointings = fixture.path("p/p.tiny.ptg.txt");
    let given = fixture.path("given.ptg.txt");
    std::fs::copy(&pointings, &given).expect("a pointing file");
    let expected = std::fs::read_to_string(&given).expect("the pointings");
    let outside = fixture.path("outside.txt");
    std::fs::write(&outside, "outside").expect("a file outside the project");
    let absent = fixture.path("absent.txt");
    for (setpointings, target) in [(false, &outside), (true, &absent)] {
        std::fs::remove_file(&pointings).expect("the pointing file");
        std::os::unix::fs::symlink(target, &pointings).expect("a link");
        let parameters = fixture.parameters(|parameters| {
            parameters.setpointings = setpointings;
            parameters.ptgfile = given.display().to_string();
        });
        simobserve(&fixture.request(parameters)).expect("simobserve");
        assert!(!is_link(&pointings), "setpointings={setpointings}");
        assert_eq!(
            std::fs::read_to_string(&pointings).expect("the pointings"),
            expected,
            "setpointings={setpointings}"
        );
    }
    assert_eq!(
        std::fs::read_to_string(&outside).expect("the file outside"),
        "outside"
    );
    assert!(!absent.exists());
}

/// The tables the task writes replace symbolic links in the project, the
/// links themselves (#920 review): a directory outside the project that a
/// link points to is left as it was, and a dangling link's target is not
/// made. `overwrite=False` counts a dangling link as an existing entry.
#[cfg(unix)]
#[test]
fn a_table_replaces_a_link_not_what_it_points_to() {
    let fixture = Fixture::new();
    let project = fixture.path("p");
    let outside = fixture.path("outside");
    std::fs::create_dir_all(&project).expect("the project");
    let tables = [
        "p.tiny.compskymodel",
        "p.tiny.compskymodel.flat",
        "p.tiny.cal.cclist",
        "p.tiny.ms",
        "p.tiny.fullpol.ms",
        "p.tiny.fullpol.cal",
        "p.tiny.noisier.ms",
    ];
    // Every other link dangles, the MeasurementSet's among them.
    let dangles = |index: usize| index % 2 == 1;
    for (index, table) in tables.iter().enumerate() {
        let target = outside.join(table);
        if !dangles(index) {
            std::fs::create_dir_all(&target).expect("a directory outside the project");
            std::fs::write(target.join("sentinel"), "outside").expect("its sentinel");
        }
        std::os::unix::fs::symlink(&target, project.join(table)).expect("a link");
    }
    let refused = simobserve(&fixture.request(fixture.parameters(|parameters| {
        with_copies(parameters);
        parameters.overwrite = false;
    })))
    .expect_err("an existing MeasurementSet's link");
    assert_eq!(
        refused.to_string(),
        format!("{}/p.tiny.ms exists but overwrite=F", project.display())
    );
    simobserve(&fixture.request(fixture.parameters(with_copies))).expect("simobserve");
    for (index, table) in tables.iter().enumerate() {
        let entry = project.join(table);
        assert!(!is_link(&entry) && entry.is_dir(), "{table}");
        let target = outside.join(table);
        if dangles(index) {
            assert!(!target.exists(), "{table}");
        } else {
            let names: Vec<_> = std::fs::read_dir(&target)
                .expect("the directory outside")
                .map(|entry| entry.expect("an entry").file_name())
                .collect();
            assert_eq!(names, ["sentinel"], "{table}");
            assert_eq!(
                std::fs::read_to_string(target.join("sentinel")).expect("the sentinel"),
                "outside"
            );
        }
    }
    assert!(!data(&project.join("p.tiny.noisier.ms")).is_empty());
}

#[test]
fn what_casa_refuses_is_refused_with_its_message() {
    let fixture = Fixture::new();
    let refusal = |changes: fn(&mut SimobserveParameters)| {
        simobserve(&fixture.request(fixture.parameters(changes)))
            .expect_err("a refusal")
            .to_string()
    };
    assert_eq!(
        refusal(|parameters| parameters.antennalist = "missing.cfg".to_string()),
        "Couldn't find antennalist: missing.cfg"
    );
    assert_eq!(
        refusal(|parameters| parameters.refdate = "2014-01-01".to_string()),
        "Invalid reference date 2014-01-01"
    );
    assert_eq!(
        refusal(|parameters| {
            parameters.direction = Directions::One("J2000 10h00m00 +40d00m00".to_string());
        }),
        "No overlap between model and pointings"
    );
    // CASA's observing loop never ends for pointings of no length.
    let message = refusal(|parameters| parameters.integration = "0s".to_string());
    assert!(message.contains("never ends"), "{message}");
    // An existing MeasurementSet stays unless overwrite is set.
    simobserve(&fixture.request(fixture.parameters(|_| {}))).expect("simobserve");
    assert_eq!(
        refusal(|parameters| parameters.overwrite = false),
        format!(
            "{}/p.tiny.ms exists but overwrite=F",
            fixture.path("p").display()
        )
    );
}
