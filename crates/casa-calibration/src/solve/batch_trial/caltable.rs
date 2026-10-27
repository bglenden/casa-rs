// SPDX-License-Identifier: LGPL-3.0-or-later
//! The caltable round trip (`docs/measurement-set/trial/README.md`,
//! "Caltable round trip"): solve from batches, persist through the store's
//! namespace, close, reopen a store that numbers its entities otherwise,
//! bind, and apply.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_ms::ms::MeasurementSet;
use casa_ms::observation_io::convert::{ConversionOptions, LossKind, PaddingRows, to_msv4};
use casa_ms::observation_io::{
    Msv4Codec, NamespaceAntenna, NamespaceRows, NamespaceWindow, OpenOptions, PersistedNamespace,
    open, persisted_namespace,
};
use casa_ms::selection::MsSelection;
use casa_ms::{
    MeasurementSetBuilder, SyntheticAntenna, SyntheticObservationRequest, SyntheticSpectralSetup,
    generate_synthetic_observation_ms,
};
use casa_observation::{
    AccessRequest, AntennaId, BatchBuffers, BindingTolerance, CorrelationRequest, Grouping,
    Observation, ObservationError, PartitionId, ReadEvent, Roles, TimeSelection, WeightRequest,
};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::{Complex32, ScalarValue};

use super::binding::{CalibrationBinding, StatedAssociations, write_gain_table};
use super::feed::solve_sequential;
use super::{access, fixture, gain_request};
use crate::solve::kernel::SolutionRow;
use crate::solve::{RefAntSelector, solve_gain};
use crate::{
    ApplyCalibrationTableSpec, ApplyInterpolationMode, ApplyMode, ApplyPlanRequest,
    CalibrationTableLookup, execute_apply_from_path,
};

/// A sample's MJD seconds and stations.
type Key = (u64, String, String);

/// Corrected visibilities and flags of one sample.
type Corrected = (Vec<Complex32>, Vec<bool>);

fn conversion_options() -> ConversionOptions {
    ConversionOptions {
        data_groups: None,
        accept: vec![LossKind::Sigma, LossKind::Tables],
        batch_bytes: 1 << 20,
        workers: 1,
        chunks: [5, 0, 0, 0],
        codec: Msv4Codec::BloscLz4,
        padding: PaddingRows::Flagged,
        creator: "casa-rs".to_string(),
        creation_date: "2026-10-27T00:00:00+00:00".to_string(),
    }
}

/// Remove every MAIN row of antenna `antenna` from the MSv2 at `path`.
fn remove_antenna(path: &Path, antenna: i32) {
    let mut ms = MeasurementSet::open(path).expect("ms");
    let main = ms.main_table();
    let of = |row: usize, column: &str| match main
        .cell_accessor(row, column)
        .and_then(|cell| cell.scalar().cloned())
        .expect("antenna")
    {
        ScalarValue::Int32(id) => id,
        other => panic!("{column} is {other:?}"),
    };
    let rows: Vec<usize> = (0..main.row_count())
        .filter(|row| of(*row, "ANTENNA1") == antenna || of(*row, "ANTENNA2") == antenna)
        .collect();
    ms.main_table_mut().remove_rows(&rows).expect("remove");
    ms.save().expect("save");
}

/// The antenna of `observation` at `station`.
fn at_station(observation: &dyn Observation, station: &str) -> AntennaId {
    let index = observation
        .catalogue()
        .antennas
        .iter()
        .position(|antenna| antenna.station == station)
        .expect("station");
    AntennaId(index as u32)
}

/// Every sample of `observation`'s `group`, keyed, with what `correct`
/// makes of its visibilities and flags.
fn corrected(
    observation: &dyn Observation,
    group: &str,
    mut correct: impl FnMut(&casa_observation::VisibilityBatch<'_>, usize) -> Corrected,
) -> BTreeMap<Key, Corrected> {
    let catalogue = observation.catalogue();
    let stations: Vec<String> = catalogue
        .antennas
        .iter()
        .map(|antenna| antenna.station.clone())
        .collect();
    let mut all = BTreeMap::new();
    for id in catalogue.partition_ids() {
        let request = AccessRequest {
            partitions: vec![id],
            data_group: group.to_string(),
            roles: Roles::ALL,
            channels: 0..catalogue.partition(id).frequencies_hz.len(),
            times: TimeSelection::All,
            weights: WeightRequest::Stored,
            correlations: CorrelationRequest::ALL,
            grouping: Grouping::None,
            batch_bytes: usize::MAX,
        };
        let mut traversal = observation.bind_read(&request).expect("bind");
        let mut buffers = BatchBuffers::default();
        while let Some(event) = traversal.next(&mut buffers).expect("read") {
            let ReadEvent::Batch(batch) = event else {
                continue;
            };
            let zero = casa_types::measures::MjdHighPrec::new(0.0, 0.0);
            for sample in 0..batch.samples() {
                let baseline = batch.sample(sample).baseline;
                let key = (
                    batch
                        .time
                        .seconds_since(sample / batch.samples_per_time(), zero)
                        .to_bits(),
                    stations[baseline.antenna1.0 as usize].clone(),
                    stations[baseline.antenna2.0 as usize].clone(),
                );
                all.insert(key, correct(&batch, sample));
            }
        }
    }
    all
}

/// `batch`'s sample `sample` as it is, the delivered flags (a validity
/// rule's NaNs aside).
fn as_stored(batch: &casa_observation::VisibilityBatch<'_>, sample: usize) -> Corrected {
    let cell = ndarray::s![sample, .., ..];
    (
        batch
            .group
            .values
            .expect("values")
            .slice(cell)
            .iter()
            .copied()
            .collect(),
        batch
            .group
            .flags
            .expect("flags")
            .slice(cell)
            .iter()
            .copied()
            .collect(),
    )
}

/// `batch`'s sample `sample` corrected by the gains `lookup` holds for it
/// through `binding`, as CASA's `applycal` applies one G table with
/// `calwt=False` and `applymode='calflag'`: `V / (g₁ g₂*)` per product, and
/// flagged where either antenna has no solution or a flagged one.
fn through_binding(
    batch: &casa_observation::VisibilityBatch<'_>,
    sample: usize,
    binding: &CalibrationBinding,
    lookup: &CalibrationTableLookup,
) -> Corrected {
    let (values, mut flags) = as_stored(batch, sample);
    let baseline = batch.sample(sample).baseline;
    let time = batch.time.seconds_since(
        sample / batch.samples_per_time(),
        casa_types::measures::MjdHighPrec::new(0.0, 0.0),
    );
    let field = binding.fields[batch.field[sample / batch.samples_per_time()].0 as usize];
    let window = binding.spectral_windows[batch.partition.0 as usize].expect("a window");
    let solution = |antenna: AntennaId| {
        let row = binding.antennas[antenna.0 as usize]?;
        lookup
            .solution(
                field.map_or(0, |row| row as i32),
                window as i32,
                row as i32,
                time,
            )
            .expect("lookup")
    };
    let (first, second) = (solution(baseline.antenna1), solution(baseline.antenna2));
    let products = batch.correlations.len();
    let corrected = values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let product = batch.correlations[index % products];
            let (r1, r2) = crate::solve::correlation_receptors(product.casacore_code())
                .expect("a receptor pair");
            match (&first, &second) {
                (Some(a), Some(b)) if !a.flags[r1] && !b.flags[r2] => {
                    *value / (a.value(r1, 0) * b.value(r2, 0).conj())
                }
                _ => {
                    flags[index] = true;
                    *value
                }
            }
        })
        .collect();
    (corrected, flags)
}

/// A solve from batches persisted through an MSv2's namespace is the
/// production `gaincal`'s table: the same `ANTENNA`, `FIELD` and
/// `SPECTRAL_WINDOW` rows (the window collapsed as CASA collapses it),
/// and the same solutions at the same rows. CASA's own association is
/// kept: the table applies to its MeasurementSet unchanged.
#[test]
fn a_batch_solve_persists_the_msv2s_own_namespace() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    let production = gain_request(root.path().join("production.G"));
    solve_gain(&MeasurementSet::open(&path).expect("ms"), &production).expect("production");
    let observation = open(&path, &OpenOptions::default()).expect("open");
    let namespace = persisted_namespace(&path).expect("namespace");
    let request = gain_request(root.path().join("batch.G"));
    let (rows, _) = solve_sequential(&*observation, &request, &access(&*observation, 3), 0);
    write_gain_table(
        &namespace,
        observation.catalogue(),
        &request,
        AntennaId(0),
        &rows,
    )
    .expect("write");
    let ours = NamespaceRows::read(&request.output_table).expect("ours");
    let theirs = NamespaceRows::read(&production.output_table).expect("theirs");
    assert_eq!(ours.antennas, theirs.antennas);
    assert_eq!(ours.spectral_windows, theirs.spectral_windows);
    assert_eq!(
        theirs.spectral_windows[0].frequencies_hz.len(),
        1,
        "collapsed"
    );
    let binding = CalibrationBinding::bind(
        &ours,
        &namespace,
        &BindingTolerance::EXACT,
        &StatedAssociations::default(),
    )
    .expect("bind");
    assert_eq!(binding.antennas, (0..5).map(Some).collect::<Vec<_>>());
    assert_eq!(binding.spectral_windows, vec![Some(0)]);
    let open = |table: &Path| {
        CalibrationTableLookup::open(table, ApplyInterpolationMode::Nearest).expect("lookup")
    };
    let (ours, theirs) = (open(&request.output_table), open(&production.output_table));
    for row in &rows {
        for antenna in 0..5 {
            let solution = |lookup: &CalibrationTableLookup| {
                lookup
                    .solution(row.field_id, 0, antenna, row.time_seconds)
                    .expect("lookup")
                    .expect("a solution")
            };
            let (a, b) = (solution(&ours), solution(&theirs));
            assert_eq!(a.flags, b.flags);
            for (x, y) in a.values.iter().zip(&b.values) {
                assert!((x - y).norm() < 1e-5, "antenna {antenna}: {x} against {y}");
            }
        }
    }
}

/// Solve on an MSv4 converted from the fixture with antenna 0's rows
/// removed, whose catalogue therefore lacks it and numbers the others from
/// 0; persist through that store's namespace; close; reopen the full MSv2,
/// which numbers them from 1; bind and apply from batches. The result is
/// production's: `gaincal` on the MSv2 without antenna 0, then `applycal`
/// on the full MSv2, values to rounding and flags exactly, antenna 0's
/// baselines flagged both ways for want of a solution.
#[test]
fn a_caltable_rebinds_to_a_store_numbered_otherwise() {
    let root = tempfile::tempdir().expect("root");
    let full = fixture(root.path(), "ms_trial.ms");
    let without = fixture(&root.path().join("without"), "ms_trial.ms");
    remove_antenna(&without, 0);
    let converted = root.path().join("without.ps.zarr");
    to_msv4(&without, &converted, &conversion_options()).expect("convert");
    let refant_station = {
        let ms = open(&without, &OpenOptions::default()).expect("open");
        ms.catalogue().antennas[1].station.clone()
    };

    // Production: gaincal on the MSv2 without antenna 0, applycal on all.
    // Four antennas have three baselines each.
    let production = crate::solve::GainSolveRequest {
        refant: RefAntSelector::AntennaId(1),
        min_baselines_per_antenna: 3,
        ..gain_request(root.path().join("production.G"))
    };
    solve_gain(&MeasurementSet::open(&without).expect("ms"), &production).expect("gaincal");
    let applied = fixture(&root.path().join("applied"), "ms_trial.ms");
    execute_apply_from_path(
        &applied,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::CalFlag,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&production.output_table)],
        },
    )
    .expect("applycal");
    let expected = corrected(
        &*open(&applied, &OpenOptions::default()).expect("open"),
        "corrected",
        as_stored,
    );

    // From batches: solve on the MSv4, persist through its namespace.
    let request = crate::solve::GainSolveRequest {
        output_table: root.path().join("batch.G"),
        ..production.clone()
    };
    {
        let observation = open(&converted, &OpenOptions::default()).expect("open");
        assert_eq!(
            observation.catalogue().antennas.len(),
            4,
            "antenna 0 dropped"
        );
        let refant = at_station(&*observation, &refant_station);
        let (rows, _) = solve_sequential(
            &*observation,
            &request,
            &access(&*observation, 3),
            refant.0 as i32,
        );
        let namespace = persisted_namespace(&converted).expect("namespace");
        write_gain_table(&namespace, observation.catalogue(), &request, refant, &rows)
            .expect("write");
    }

    keep_for_casa(&[
        (&request.output_table, "batch.G"),
        (&production.output_table, "production.G"),
        (&without, "without.ms"),
    ]);
    if let Some(keep) = std::env::var_os(KEEP) {
        // The MSv2 whose namespace is the batch table's, for CASA to apply
        // it to by its own association.
        let options = ConversionOptions {
            accept: vec![LossKind::Sigma, LossKind::Tables, LossKind::Columns],
            ..conversion_options()
        };
        casa_ms::observation_io::convert::to_msv2(
            &converted,
            &Path::new(&keep).join("without_v4.ms"),
            &options,
        )
        .expect("convert back");
    }

    // Reopen the full MSv2, bind, apply.
    let target = persisted_namespace(&full).expect("namespace");
    let table = NamespaceRows::read(&request.output_table).expect("table");
    let binding = CalibrationBinding::bind(
        &table,
        &target,
        &BindingTolerance::EXACT,
        &StatedAssociations::default(),
    )
    .expect("bind");
    assert_eq!(
        binding.antennas,
        vec![None, Some(0), Some(1), Some(2), Some(3)]
    );
    let lookup =
        CalibrationTableLookup::open(&request.output_table, ApplyInterpolationMode::Linear)
            .expect("lookup");
    let observation = open(&full, &OpenOptions::default()).expect("open");
    let ours = corrected(&*observation, "base", |batch, sample| {
        through_binding(batch, sample, &binding, &lookup)
    });
    assert_eq!(ours.len(), expected.len());
    let mut flagged = 0;
    for (key, (values, flags)) in &ours {
        let (theirs, their_flags) = &expected[key];
        assert_eq!(flags, their_flags, "{key:?}");
        flagged += usize::from(flags.iter().all(|flag| *flag));
        for ((ours, theirs), flag) in values.iter().zip(theirs).zip(flags) {
            if !flag {
                assert!((ours - theirs).norm() <= 1e-5 * theirs.norm(), "{key:?}");
            }
        }
    }
    // Antenna 0's four baselines (the fixture holds no autocorrelations),
    // at each time.
    let times = ours
        .keys()
        .map(|key| key.0)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(flagged, 4 * times.len());
}

/// When set, the directory [`a_caltable_rebinds_to_a_store_numbered_otherwise`]
/// keeps its tables and stores in for the CASA oracle
/// (`tools/ms-trial/check_caltable.py`).
const KEEP: &str = "CASA_RS_TRIAL_KEEP";

/// Copy each of `tables` (a table directory and the name to keep it as)
/// into the [`KEEP`] directory, when one is set.
fn keep_for_casa(tables: &[(&Path, &str)]) {
    fn copy(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).expect("directory");
        for entry in std::fs::read_dir(from).expect("read") {
            let entry = entry.expect("entry");
            let target = to.join(entry.file_name());
            if entry.file_type().expect("type").is_dir() {
                copy(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).expect("copy");
            }
        }
    }
    let Some(keep) = std::env::var_os(KEEP) else {
        return;
    };
    for (table, name) in tables {
        copy(table, &Path::new(&keep).join(name));
    }
}

fn synthetic_antennas() -> [SyntheticAntenna; 4] {
    [
        SyntheticAntenna::vla("VLA01", "N01", [-1_601_185.4, -5_041_977.5, 3_554_875.9]),
        SyntheticAntenna::vla("VLA02", "N02", [-1_601_085.4, -5_041_977.5, 3_554_875.9]),
        SyntheticAntenna::vla("VLA03", "N03", [-1_601_185.4, -5_041_877.5, 3_554_875.9]),
        SyntheticAntenna::vla("VLA04", "N04", [-1_601_285.4, -5_041_977.5, 3_554_975.9]),
    ]
}

fn window(name: &str, start_hz: f64) -> SyntheticSpectralSetup {
    SyntheticSpectralSetup {
        name: name.to_string(),
        start_frequency_hz: start_hz,
        channel_width_hz: 2.0e6,
        channel_count: 4,
    }
}

/// A simulated MSv2 of `antennas` and `windows`.
fn simulated(
    root: &Path,
    name: &str,
    antennas: Vec<SyntheticAntenna>,
    windows: Vec<SyntheticSpectralSetup>,
) -> PathBuf {
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        root.join(name),
        antennas,
    );
    request.predict_model = false;
    request.phase_center_rad = [1.23, -0.45];
    request.start_time_mjd_seconds = 59_000.25 * 86_400.0;
    request.duration_seconds = 25.0;
    request.integration_seconds = 10.0;
    request.spectral_windows = windows;
    generate_synthetic_observation_ms(&request).expect("simulate");
    request.output_ms
}

/// The gain written for the antenna at `station` in the window starting at
/// `start_hz`, receptor `receptor`: distinct for every triple.
fn known_gain(station: &str, start_hz: f64, receptor: usize) -> Complex32 {
    let antenna: f32 = station[1..].parse().expect("N0k");
    Complex32::new(
        1.0 + 0.1 * antenna + 0.01 * receptor as f32,
        (start_hz / 1.0e9) as f32 * 0.001,
    )
}

/// Write a gain table through the namespace of the MSv2 at `path`: one
/// solution per antenna and partition, [`known_gain`] of its station and
/// window, at the store's first time; return that time.
fn write_known_gains(path: &Path, table: &Path) -> f64 {
    let observation = open(path, &OpenOptions::default()).expect("open");
    let catalogue = observation.catalogue();
    let grid = observation.grid(PartitionId(0), "base").expect("grid");
    let time = grid.times_s[0];
    let mut rows = Vec::new();
    for partition in catalogue.partition_ids() {
        let start = catalogue.partition(partition).frequencies_hz[0];
        for (antenna, entry) in catalogue.antennas.iter().enumerate() {
            rows.push(SolutionRow {
                time_seconds: time,
                field_id: 0,
                spw_id: partition.0 as i32,
                antenna_id: antenna as i32,
                scan_number: 1,
                observation_id: 0,
                refant_id: 0,
                gains: (0..2)
                    .map(|r| known_gain(&entry.station, start, r))
                    .collect(),
                flags: vec![false; 2],
                param_errors: vec![0.0; 2],
                snrs: vec![100.0; 2],
                weights: vec![1.0; 2],
            });
        }
    }
    let namespace = persisted_namespace(path).expect("namespace");
    write_gain_table(
        &namespace,
        catalogue,
        &gain_request(table.to_path_buf()),
        AntennaId(0),
        &rows,
    )
    .expect("write");
    time
}

/// A table written through one store's namespace binds to another that
/// enumerates its antennas and windows otherwise and holds an overlapping
/// subset of the antennas, as an MSv2 and as its MSv4 conversion (whose
/// names XRADIO's way differ); each bound antenna and partition looks up
/// the gain written for its own station and window, and an antenna the
/// table lacks binds to nothing.
#[test]
fn antennas_and_windows_rebind_after_reordering() {
    let root = tempfile::tempdir().expect("root");
    let [a, b, c, d] = synthetic_antennas();
    let (low, high) = (window("low", 672.0e9), window("high", 676.0e9));
    let solved = simulated(
        root.path(),
        "solved.ms",
        vec![a.clone(), b, c.clone()],
        vec![low.clone(), high.clone()],
    );
    let table = root.path().join("known.G");
    let time = write_known_gains(&solved, &table);
    let reordered = simulated(root.path(), "reordered.ms", vec![c, d, a], vec![high, low]);
    let converted = root.path().join("reordered.ps.zarr");
    to_msv4(&reordered, &converted, &conversion_options()).expect("convert");
    let rows = NamespaceRows::read(&table).expect("table");
    let lookup =
        CalibrationTableLookup::open(&table, ApplyInterpolationMode::Nearest).expect("lookup");
    for target in [&reordered, &converted] {
        let observation = open(target, &OpenOptions::default()).expect("open");
        let catalogue = observation.catalogue();
        let namespace = persisted_namespace(target).expect("namespace");
        let binding = CalibrationBinding::bind(
            &rows,
            &namespace,
            &BindingTolerance::EXACT,
            &StatedAssociations::default(),
        )
        .expect("bind");
        let stations: Vec<&str> = catalogue
            .antennas
            .iter()
            .map(|antenna| antenna.station.as_str())
            .collect();
        for (antenna, station) in stations.iter().enumerate() {
            let expected = match *station {
                "N01" => Some(0),
                "N03" => Some(2),
                _ => None,
            };
            assert_eq!(binding.antennas[antenna], expected, "{target:?} {station}");
        }
        for partition in catalogue.partition_ids() {
            let start = catalogue.partition(partition).frequencies_hz[0];
            let window = binding.spectral_windows[partition.0 as usize].expect("a window");
            assert_eq!(window, u32::from(start > 674.0e9), "{target:?}");
            for (antenna, station) in stations.iter().enumerate() {
                let Some(row) = binding.antennas[antenna] else {
                    continue;
                };
                let solution = lookup
                    .solution(0, window as i32, row as i32, time)
                    .expect("lookup")
                    .expect("a solution");
                for receptor in 0..2 {
                    assert_eq!(
                        solution.value(receptor, 0),
                        known_gain(station, start, receptor),
                        "{target:?} {station}"
                    );
                }
            }
        }
    }
}

/// Two windows of equal channels are one entity only by evidence: bound
/// to the store the table was written through (CASA's own association,
/// names included) each window takes its own row; bound to a store that
/// lists them the other way round, content cannot tell them apart and the
/// binding is refused, until the caller states the association, as
/// CASA's `spwmap` states it.
#[test]
fn equal_windows_need_their_own_namespace_or_a_stated_association() {
    let root = tempfile::tempdir().expect("root");
    let [a, b, c, _] = synthetic_antennas();
    let antennas = vec![a, b, c];
    let (first, second) = (window("first", 672.0e9), window("second", 672.0e9));
    let solved = simulated(
        root.path(),
        "solved.ms",
        antennas.clone(),
        vec![first.clone(), second.clone()],
    );
    let table = root.path().join("equal.G");
    write_known_gains(&solved, &table);
    let rows = NamespaceRows::read(&table).expect("table");
    let bind = |target: &Path, stated: &StatedAssociations| {
        CalibrationBinding::bind(
            &rows,
            &persisted_namespace(target).expect("namespace"),
            &BindingTolerance::EXACT,
            stated,
        )
    };
    let own = bind(&solved, &StatedAssociations::default()).expect("its own namespace");
    assert_eq!(own.spectral_windows, vec![Some(0), Some(1)]);
    let swapped = simulated(root.path(), "swapped.ms", antennas, vec![second, first]);
    match bind(&swapped, &StatedAssociations::default()) {
        Err(ObservationError::InvalidRequest { reason }) => {
            assert!(reason.contains("state the association"), "{reason}");
        }
        other => panic!("refused, not {other:?}"),
    }
    // In the swapped store partition 0 is `second`, which the table holds
    // at row 1.
    let stated = StatedAssociations {
        spectral_windows: vec![(PartitionId(0), 1), (PartitionId(1), 0)],
        antennas: Vec::new(),
    };
    let swapped_names: Vec<String> = open(&swapped, &OpenOptions::default())
        .expect("open")
        .catalogue()
        .partitions
        .iter()
        .map(|entry| entry.spectral_window.clone())
        .collect();
    assert_eq!(swapped_names, ["second", "first"]);
    let resolved = bind(&swapped, &stated).expect("stated");
    assert_eq!(resolved.spectral_windows, vec![Some(1), Some(0)]);
}

/// A namespace of `antennas` (name, station, x offset) and `windows`
/// (name, first channel), held by no store: one partition per window.
fn namespace(antennas: &[(&str, &str, f64)], windows: &[(&str, f64)]) -> PersistedNamespace {
    let rows = NamespaceRows {
        antennas: antennas
            .iter()
            .map(|(name, station, x)| NamespaceAntenna {
                name: (*name).to_string(),
                station: (*station).to_string(),
                position_m: [*x, -5_041_977.5, 3_554_875.9],
            })
            .collect(),
        fields: Vec::new(),
        spectral_windows: windows
            .iter()
            .map(|(name, first)| NamespaceWindow {
                name: (*name).to_string(),
                frame: FrequencyRef::TOPO,
                frequencies_hz: vec![*first, first + 1.0e6],
                widths_hz: vec![1.0e6; 2],
            })
            .collect(),
    };
    PersistedNamespace {
        subtables: MeasurementSet::create_memory(MeasurementSetBuilder::new()).expect("ms"),
        antennas: (0..antennas.len() as u32).collect(),
        fields: Vec::new(),
        spectral_windows: (0..windows.len() as u32).collect(),
        observations: vec![0],
        rows,
    }
}

fn bound(
    table: &PersistedNamespace,
    target: &PersistedNamespace,
    stated: &StatedAssociations,
) -> Result<CalibrationBinding, ObservationError> {
    CalibrationBinding::bind(&table.rows, target, &BindingTolerance::EXACT, stated)
}

/// An antenna binds by station and position: renamed, it still binds;
/// another antenna holding a table antenna's name binds to nothing.
#[test]
fn antennas_bind_by_station_and_position_never_by_name() {
    let table = namespace(
        &[
            ("ea01", "N01", 1.0),
            ("ea02", "N02", 2.0),
            ("ea03", "N03", 3.0),
        ],
        &[],
    );
    let target = namespace(
        &[
            ("ea03", "N01", 1.0),
            ("ea01", "N09", 9.0),
            ("ea02", "N02", 2.0),
        ],
        &[],
    );
    let binding = bound(&table, &target, &StatedAssociations::default()).expect("bind");
    assert_eq!(binding.antennas, vec![Some(0), None, Some(1)]);
}

/// Two antennas of one station and position are told apart by their own
/// namespace, row for row with names (CASA's association), or by a stated
/// association; content alone refuses them, whatever their names.
#[test]
fn antennas_of_one_content_need_their_own_namespace_or_a_statement() {
    let twins = [("x", "N01", 1.0), ("y", "N01", 1.0), ("ea02", "N02", 2.0)];
    let own = bound(
        &namespace(&twins, &[]),
        &namespace(&twins, &[]),
        &StatedAssociations::default(),
    )
    .expect("its own namespace");
    assert_eq!(own.antennas, vec![Some(0), Some(1), Some(2)]);
    let table = namespace(&[("ea01", "N01", 1.0), ("ea02", "N02", 2.0)], &[]);
    let target = namespace(&twins, &[]);
    match bound(&table, &target, &StatedAssociations::default()) {
        Err(ObservationError::InvalidRequest { reason }) => {
            assert!(reason.contains("both bind to table row 0"), "{reason}");
        }
        other => panic!("refused, not {other:?}"),
    }
    let stated = StatedAssociations {
        antennas: vec![(AntennaId(1), 0)],
        spectral_windows: Vec::new(),
    };
    let resolved = bound(&table, &target, &stated).expect("stated");
    assert_eq!(resolved.antennas, vec![None, Some(0), Some(1)]);
    let twice = StatedAssociations {
        antennas: vec![(AntennaId(0), 0), (AntennaId(1), 0)],
        spectral_windows: Vec::new(),
    };
    assert!(bound(&table, &target, &twice).is_err(), "one to one");
}

/// Windows bind by frame and channels, renamed or reordered; a window the
/// table lacks binds to nothing; and several may be stated to take one
/// row, as CASA's `spwmap` maps them.
#[test]
fn windows_bind_by_content_and_statements_may_share_a_row() {
    let table = namespace(&[], &[("a", 1.0e9), ("b", 2.0e9)]);
    let target = namespace(&[], &[("B", 2.0e9), ("c", 3.0e9), ("A", 1.0e9)]);
    let binding = bound(&table, &target, &StatedAssociations::default()).expect("bind");
    assert_eq!(binding.spectral_windows, vec![Some(1), None, Some(0)]);
    let stated = StatedAssociations {
        spectral_windows: vec![(PartitionId(1), 0), (PartitionId(2), 0)],
        antennas: Vec::new(),
    };
    let mapped = bound(&table, &target, &stated).expect("stated");
    assert_eq!(mapped.spectral_windows, vec![Some(1), Some(0), Some(0)]);
}
