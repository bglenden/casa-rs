// SPDX-License-Identifier: LGPL-3.0-or-later
//! Laws of the trial observation adapters on the MSv2/MSv4 trial fixtures
//! (`docs/measurement-set/trial/README.md`): the MSv4 store XRADIO wrote
//! from the MSv2, read through both adapters into the same batches.

#[cfg(unix)]
#[path = "observation_io/convert.rs"]
mod convert;
#[cfg(unix)]
#[path = "observation_io/namespace.rs"]
mod namespace;
#[path = "observation_io/refusals.rs"]
mod refusals;
#[path = "observation_io/times.rs"]
mod times;
#[path = "observation_io/update.rs"]
mod update;
#[path = "observation_io/weights.rs"]
mod weights;
#[path = "observation_io/work.rs"]
mod work;
#[path = "observation_io/writer.rs"]
mod writer;

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Duration;

use casa_ms::observation_io::{GroupClosure, Msv2Observation, Msv4Observation, OpenOptions};
use casa_observation::{
    AccessRequest, BatchBuffers, BindingTolerance, BufferPool, Catalogue, Correlation,
    CorrelationRequest, Deliver, FieldId, FlagsFrom, FragmentPosition, GroupKey, Grouping,
    Observation, ObservationError, ObservationId, OwnedEvent, PartitionId, ReadEvent, Roles,
    ScanId, Shared, TimeSelection, Traversal, WeightRequest,
};
use casa_types::measures::MjdHighPrec;
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use flate2::read::GzDecoder;
use num_complex::Complex32;
use tar::Archive;

/// Unpack fixture `name` (`ms_trial.ms` or `ms_trial.ps.zarr`).
pub(crate) fn fixture(root: &Path, name: &str) -> PathBuf {
    let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/ms_trial")
        .join(format!("{name}.tgz"));
    Archive::new(GzDecoder::new(File::open(archive).expect("fixture")))
        .unpack(root)
        .expect("unpack");
    root.join(name)
}

/// Relabel the MSv2 fixture at `path`'s twelve times as scans 1, 2 and 3
/// of four times each (ten rows per time, in time order), so that
/// partitioning by scan splits it in three.
pub(crate) fn three_scans(path: &Path) {
    let mut ms = casa_ms::MeasurementSet::open(path).expect("ms");
    for (scan, rows) in [(2, 40..80), (3, 80..120)] {
        ms.main_table_mut()
            .fill_column_range(
                "SCAN_NUMBER",
                casa_tables::RowRange::new(rows.start, rows.end),
                casa_types::Value::Scalar(casa_types::ScalarValue::Int32(scan)),
            )
            .expect("scan");
    }
    ms.save().expect("save");
}

/// Options that partition an MSv2 by scan as well.
pub(crate) fn by_scan() -> OpenOptions {
    OpenOptions {
        partition_scheme: casa_ms::observation_io::PartitionScheme {
            scans: true,
            ..Default::default()
        },
        ..OpenOptions::default()
    }
}

/// Every role of `channels` of the first partition, under `batch_bytes`.
pub(crate) fn request(channels: std::ops::Range<usize>, batch_bytes: usize) -> AccessRequest {
    AccessRequest {
        partitions: vec![PartitionId(0)],
        data_group: "base".to_string(),
        roles: Roles::ALL,
        channels,
        times: TimeSelection::All,
        weights: WeightRequest::Stored,
        correlations: CorrelationRequest::ALL,
        grouping: Grouping::None,
        batch_bytes,
    }
}

/// `request` budgeted for exactly `times` whole times per batch on
/// `observation`: the estimate's held bytes per time (the batch's and the
/// cursor's scratch), which costs no I/O.
pub(crate) fn per_times(
    observation: &dyn Observation,
    request: AccessRequest,
    times: usize,
) -> AccessRequest {
    let probe = AccessRequest {
        batch_bytes: usize::MAX,
        ..request.clone()
    };
    let estimate = observation.estimate(&probe).expect("estimate");
    let partition = &estimate.partitions[0];
    AccessRequest {
        batch_bytes: partition.held_bytes_per_time() * times,
        ..request
    }
}

/// One sample's payload and time auxiliaries, keyed by MJD seconds and
/// the two antennas' stations, so the formats' different antenna and field
/// naming does not decide the match.
#[derive(Debug, PartialEq)]
pub(crate) struct Sample {
    values: Vec<Complex32>,
    flags: Vec<bool>,
    weights: Vec<f32>,
    uvw: [f64; 3],
    /// MJD seconds of the time centroid.
    centroid: f64,
    interval: f64,
    exposure: f64,
    scan: String,
}

pub(crate) type Key = (u64, String, String);

/// Every sample a traversal delivers, keyed.
pub(crate) fn samples(
    traversal: &mut Traversal<'_>,
    catalogue: &Catalogue,
) -> Result<BTreeMap<Key, Sample>, ObservationError> {
    let stations = stations(catalogue);
    let mjd_zero = MjdHighPrec::new(0.0, 0.0);
    let mut buffers = BatchBuffers::default();
    let mut all = BTreeMap::new();
    while let Some(event) = traversal.next(&mut buffers)? {
        let ReadEvent::Batch(batch) = event else {
            continue;
        };
        let values = batch.group.values.expect("values");
        let flags = batch.group.flags.expect("flags");
        let weights = batch.group.weights.expect("weights");
        let uvw = batch.group.uvw.expect("uvw");
        let centroid = batch.timing.centroid.expect("centroid");
        let per_time = batch.samples_per_time();
        for sample in 0..batch.samples() {
            let coordinate = batch.sample(sample);
            let key = (
                batch
                    .time
                    .seconds_since(sample / per_time, mjd_zero)
                    .to_bits(),
                stations[coordinate.baseline.antenna1.0 as usize].clone(),
                stations[coordinate.baseline.antenna2.0 as usize].clone(),
            );
            let scan = batch.scan(sample).expect("scan");
            let cell = ndarray::s![sample, .., ..];
            let previous = all.insert(
                key,
                Sample {
                    values: values.slice(cell).iter().copied().collect(),
                    flags: flags.slice(cell).iter().copied().collect(),
                    weights: weights.slice(cell).iter().copied().collect(),
                    uvw: uvw.metres_at(sample),
                    centroid: centroid.seconds_since(sample, mjd_zero),
                    interval: batch.interval_s(sample).expect("interval"),
                    exposure: batch.exposure_s(sample).expect("exposure"),
                    scan: catalogue.scans[scan.0 as usize].name.clone(),
                },
            );
            assert!(previous.is_none(), "one acquisition per time and baseline");
        }
    }
    Ok(all)
}

pub(crate) fn stations(catalogue: &Catalogue) -> Vec<String> {
    catalogue
        .antennas
        .iter()
        .map(|antenna| antenna.station.clone())
        .collect()
}

pub(crate) fn open_both(root: &Path) -> (Msv2Observation, Msv4Observation) {
    (
        Msv2Observation::open(fixture(root, "ms_trial.ms")).expect("v2"),
        Msv4Observation::open(fixture(root, "ms_trial.ps.zarr")).expect("v4"),
    )
}

/// The first batch of `request` on `observation`.
fn first_batch<'o>(
    observation: &'o dyn Observation,
    request: &AccessRequest,
    buffers: &mut BatchBuffers,
) -> Traversal<'o> {
    let mut traversal = observation.bind_read(request).expect("bind");
    let Some(ReadEvent::Batch(_)) = traversal.next(buffers).expect("read") else {
        panic!("a batch");
    };
    traversal
}

/// The two catalogues describe the same observation, in each store's own
/// names: XRADIO names antennas `NAME_STATION` and fields `NAME_ID`,
/// declares the visibility unit `unkown`, and states the UVW frame as the
/// phase centre's, which is the frame the MSv2 adapter resolves for MSv2's
/// `ITRF`-labelled UVW. Both open as one partition of the same content
/// (spectral window, correlations, intents), with the same baseline table
/// by station and each store's own time system; the catalogues bind to
/// each other by content, not by name or position.
#[test]
fn catalogues_describe_the_same_observation() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    let (a, b) = (v2.catalogue(), v4.catalogue());
    assert_eq!(stations(a), stations(b));
    for (v2, v4) in a.antennas.iter().zip(&b.antennas) {
        assert_eq!(format!("{}_{}", v2.name, v2.station), v4.name);
        assert_eq!(v2.position.values(), v4.position.values());
        assert_eq!(v2.dish_diameter.value(), v4.dish_diameter.value());
        assert_eq!(v2.dish_diameter.unit().name(), "m");
        assert_eq!(v2.receptors, v4.receptors);
    }
    assert_eq!((a.fields.len(), b.fields.len()), (1, 1));
    assert_eq!(format!("{}_0", a.fields[0].name), b.fields[0].name);
    assert_eq!(
        a.fields[0].phase_centre.as_angles(),
        b.fields[0].phase_centre.as_angles()
    );
    assert_eq!(a.fields[0].phase_centre.refer(), DirectionRef::J2000);
    assert_eq!(b.fields[0].phase_centre.refer(), DirectionRef::J2000);
    assert_eq!(a.scans, b.scans, "scan 1, named by its number, its intents");
    assert_eq!(a.scans[0].number, Some(1));
    assert_eq!(a.scans[0].intents, ["TARGET.ON_SOURCE"]);
    assert_eq!((a.observations.len(), b.observations.len()), (1, 1));
    assert_eq!(a.observations[0].project, "casa-rs-ms-trial");
    assert_eq!(a.observations[0].project, b.observations[0].project);
    assert_eq!(a.observations[0].telescope, b.observations[0].telescope);
    assert_eq!((a.partitions.len(), b.partitions.len()), (1, 1));
    let (p2, p4) = (&a.partitions[0], &b.partitions[0]);
    assert!(p2.same_content(p4), "one partition of the same content");
    assert_eq!(p2.frequencies_hz, p4.frequencies_hz);
    assert_eq!(p2.channel_widths_hz, p4.channel_widths_hz);
    assert_eq!(p2.frequency_frame, FrequencyRef::LSRK);
    let linear = vec![
        Correlation::XX,
        Correlation::XY,
        Correlation::YX,
        Correlation::YY,
    ];
    assert_eq!(p2.correlations, linear);
    assert_eq!(p4.correlations, linear);
    assert_eq!(p2.intents, ["TARGET.ON_SOURCE"]);
    assert_eq!(p4.intents, p2.intents);
    assert_eq!(p2.feeds, Some([0, 0]), "MSv2 keys the feed pair");
    assert_eq!(p4.feeds, None, "MSv4 has no feed axis");
    assert_eq!(p2.fields, [FieldId(0)]);
    assert_eq!((&p4.fields, &p4.scans), (&p2.fields, &p2.scans));
    assert_eq!(p2.scans, [ScanId(0)]);
    assert_eq!(p2.baselines.len(), 10);
    let by_station = |catalogue: &Catalogue, partition: &casa_observation::PartitionEntry| {
        let stations = stations(catalogue);
        partition
            .baselines
            .iter()
            .map(|b| {
                (
                    stations[b.antenna1.0 as usize].clone(),
                    stations[b.antenna2.0 as usize].clone(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(by_station(a, p2), by_station(b, p4));
    assert_eq!(p2.time_origin.day(), 0.0, "MSv2 TIME counts from MJD 0");
    assert_eq!(p4.time_origin.day(), 40_587.0, "MSv4 unix times");
    let (g2, g4) = (&p2.data_groups[0], &p4.data_groups[0]);
    assert_eq!(g2.units, casa_observation::VisibilityUnits::Undeclared);
    assert_eq!(
        g4.units,
        casa_observation::VisibilityUnits::Declared("unkown".to_string())
    );
    assert_eq!(g4.visibilities, "VISIBILITY");
    assert_eq!(g2.uvw_frame.effective, DirectionRef::J2000);
    assert_eq!(g2.uvw_frame.stored_label, Some(DirectionRef::ITRF));
    assert_eq!(g4.uvw_frame.effective, DirectionRef::J2000);
    let binding = a
        .bind_to(b, &BindingTolerance::EXACT)
        .expect("v2 binds to v4");
    assert_eq!(binding.partitions, [PartitionId(0)]);
    assert_eq!(binding.observations, [ObservationId(0)]);
    assert_eq!(binding.antennas.len(), 5);
    assert!(b.bind_to(a, &BindingTolerance::EXACT).is_ok());
}

/// Every sample XRADIO wrote equals its MSv2 row exactly: visibilities,
/// flags, weights (MSv2's per-correlation `WEIGHT` seen through every
/// channel, MSv4's spectral `WEIGHT`), UVW, time, and the time centroid,
/// interval, exposure and scan, over the whole band and channel windows,
/// whatever the batch's whole-time count. The v4 store holds exactly the v2
/// acquisitions: none padded, none lost.
#[test]
fn xradio_samples_equal_their_msv2_rows() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    for channels in [0..8, 2..6, 7..8] {
        let reference = samples(
            &mut v2
                .bind_read(&request(channels.clone(), usize::MAX))
                .expect("bind"),
            v2.catalogue(),
        )
        .expect("v2 read");
        assert_eq!(reference.len(), 120);
        for times in [1, 2, 5, 12] {
            let read = samples(
                &mut v4
                    .bind_read(&per_times(&v4, request(channels.clone(), 0), times))
                    .expect("bind"),
                v4.catalogue(),
            )
            .expect("v4 read");
            assert_eq!(
                read, reference,
                "channels {channels:?}, {times} times per batch"
            );
        }
        for times in [1, 3, 5] {
            let read = samples(
                &mut v2
                    .bind_read(&per_times(&v2, request(channels.clone(), 0), times))
                    .expect("bind"),
                v2.catalogue(),
            )
            .expect("v2 read");
            assert_eq!(
                read, reference,
                "v2 channels {channels:?}, {times} times per batch"
            );
        }
    }
}

/// The time auxiliaries the v2 batch delivers are MAIN's columns, as the
/// production typed column accessors read them cell by cell (not the
/// adapter's bulk fill), not values the adapter makes up; each sample finds
/// its row by time and antennas, since the grid, not MAIN's row order,
/// orders the batch.
#[test]
fn time_auxiliaries_are_main_columns() {
    use casa_ms::columns::exposure_interval::{ExposureColumn, IntervalColumn};
    use casa_ms::columns::time_columns::TimeColumn;
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    let ms = casa_ms::MeasurementSet::open(&path).expect("ms");
    let main = ms.main_table();
    let v2 = Msv2Observation::open(&path).expect("v2");
    let mut buffers = BatchBuffers::default();
    let mut traversal = v2.bind_read(&request(0..8, usize::MAX)).expect("bind");
    let Some(ReadEvent::Batch(batch)) = traversal.next(&mut buffers).expect("read") else {
        panic!("a batch");
    };
    let mjd_zero = MjdHighPrec::new(0.0, 0.0);
    let time = TimeColumn::new(main);
    let antenna1 = casa_ms::columns::main_ids::antenna1(main);
    let antenna2 = casa_ms::columns::main_ids::antenna2(main);
    let rows: BTreeMap<(u64, i32, i32), usize> = (0..main.row_count())
        .map(|row| {
            (
                (
                    time.get_mjd_seconds(row).expect("time").to_bits(),
                    antenna1.get(row).expect("antenna1"),
                    antenna2.get(row).expect("antenna2"),
                ),
                row,
            )
        })
        .collect();
    let centroid = batch.timing.centroid.expect("centroid");
    let per_time = batch.samples_per_time();
    for sample in 0..batch.samples() {
        let coordinate = batch.sample(sample);
        let row = rows[&(
            batch
                .time
                .seconds_since(sample / per_time, mjd_zero)
                .to_bits(),
            coordinate.baseline.antenna1.0 as i32,
            coordinate.baseline.antenna2.0 as i32,
        )];
        assert_eq!(
            centroid.seconds_since(sample, mjd_zero),
            TimeColumn::centroid(main)
                .get_mjd_seconds(row)
                .expect("centroid")
        );
        assert_eq!(
            batch.interval_s(sample),
            Some(IntervalColumn::new(main).get(row).expect("interval"))
        );
        assert_eq!(
            batch.exposure_s(sample),
            Some(ExposureColumn::new(main).get(row).expect("exposure"))
        );
        let scan = batch.scan(sample).expect("scan");
        assert_eq!(
            v2.catalogue().scans[scan.0 as usize].number,
            Some(
                casa_ms::columns::main_ids::scan_number(main)
                    .get(row)
                    .expect("scan")
            )
        );
    }
}

/// Both adapters deliver the one batch shape: whole times of the
/// partition's grid, field and scan per time, the payload
/// `[sample, channel, correlation]` with the MSv4 view borrowing the decoded
/// buffer directly and the frequencies borrowing the catalogue. What the
/// stores hold differently stays visible only in sharing and strides: MSv2
/// `WEIGHT` is one weight per correlation seen through a zero channel
/// stride, its interval per sample; MSv4 weights are spectral, its interval
/// one constant. Times keep their store's origin.
#[test]
fn both_stores_deliver_one_batch_shape() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    for (store, observation) in [("v2", &v2 as &dyn Observation), ("v4", &v4)] {
        let request = per_times(observation, request(0..8, 0), 4);
        let mut buffers = BatchBuffers::default();
        let traversal = first_batch(observation, &request, &mut buffers);
        assert_eq!(traversal.estimate.partitions[0].samples_per_batch(), 40);
        let batch = buffers.batch().expect("batch");
        assert_eq!(
            (batch.times(), batch.samples_per_time()),
            (4, 10),
            "{store}"
        );
        assert_eq!(batch.shape(), [40, 8, 4]);
        assert_eq!(batch.field.len(), 4);
        assert_eq!(batch.scan.expect("scan").len(), 4);
        let weights = batch.group.weights.expect("weights");
        assert_eq!(weights.shape(), [40, 8, 4]);
        if store == "v2" {
            assert_eq!(
                weights.strides(),
                [4, 0, 1],
                "WEIGHT seen through every channel"
            );
            assert!(weights.as_slice().is_none());
            assert!(matches!(
                batch.timing.interval_s,
                Some(Shared::PerSample(_))
            ));
            assert_eq!(batch.time.origin().day(), 0.0);
        } else {
            assert!(weights.is_standard_layout());
            assert!(matches!(
                batch.timing.interval_s,
                Some(Shared::Constant(10.0))
            ));
            assert_eq!(batch.time.origin().day(), 40_587.0);
        }
        assert_eq!(
            batch.group.uvw.expect("uvw").reference(),
            DirectionRef::J2000
        );
        let values = batch.group.values.expect("values");
        assert_eq!(
            values.as_ptr(),
            buffers.arrays().values.as_ptr(),
            "{store}: the decode buffer itself"
        );
        let entry = observation.catalogue().partition(PartitionId(0));
        assert_eq!(
            batch.frequency.hz().as_ptr(),
            entry.frequencies_hz.as_ptr(),
            "{store}: frequencies are the catalogue's"
        );
        assert_eq!(batch.baselines.as_ptr(), entry.baselines.as_ptr());
        // Windows of whole times stay factored; a split time is refused.
        assert!(batch.sample_window(10..30).is_ok());
        assert!(batch.sample_window(5..15).is_err());
        let window = batch.channel_window(2..4).expect("window");
        assert_eq!(window.channels(), 2..4);
        assert_eq!(
            window.frequency.hz().as_ptr(),
            batch.frequency.hz().slice(ndarray::s![2..]).as_ptr()
        );
    }
}

/// The MSv4 time origin re-expresses the MSv2 time exactly: XRADIO's
/// conversion to Unix seconds lost nothing on this fixture.
#[test]
fn time_origins_re_express_each_other_exactly() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    let mjd_zero = MjdHighPrec::new(0.0, 0.0);
    let first_time = |observation: &dyn Observation| {
        let mut buffers = BatchBuffers::default();
        let request = per_times(observation, request(0..8, 0), 1);
        first_batch(observation, &request, &mut buffers);
        let batch = buffers.batch().expect("batch");
        batch.time.seconds_since(0, mjd_zero)
    };
    assert_eq!(first_time(&v2), first_time(&v4));
}

/// The fragments of every 60 s bin, whatever the batch size: each bin's
/// samples once, in order, its first fragment `First` or `Whole`, its last
/// `Last` or `Whole`, and, from a store opened to defer closure, one
/// `GroupEnd` after its last fragment instead. The deferral is an opening
/// option, not part of the request: a consumer handles both. Keys carry
/// the field and the scan, as production `gaincal` keys solutions.
#[test]
fn group_fragments_do_not_depend_on_batch_size() {
    let root = tempfile::tempdir().expect("root");
    let v2_path = fixture(root.path(), "ms_trial.ms");
    let v4_path = fixture(root.path(), "ms_trial.ps.zarr");
    let grouped = |observation: &dyn Observation, times: usize| AccessRequest {
        grouping: Grouping::TimeBins {
            width_s: 60.0,
            combine_fields: false,
            combine_scans: false,
        },
        ..per_times(observation, request(0..8, 0), times)
    };
    for closure in [GroupClosure::AtDelivery, GroupClosure::Deferred] {
        let options = OpenOptions {
            group_closure: closure,
            ..OpenOptions::default()
        };
        let v2 = casa_ms::observation_io::open(&v2_path, &options).expect("v2");
        let v4 = casa_ms::observation_io::open(&v4_path, &options).expect("v4");
        for times in [1, 3, 4, 6, 7, 12] {
            let label = format!("{closure:?}, {times} times per batch");
            let v4_groups = groups(&*v4, &grouped(&*v4, times), closure, &label);
            assert_eq!(v4_groups.len(), 2, "{label}");
            assert!(
                v4_groups.values().all(|samples| *samples == 60),
                "{label}: {v4_groups:?}"
            );
            assert!(
                v4_groups
                    .keys()
                    .all(|key| key.field.is_some() && key.scan.is_some()),
                "{label}: keyed by field and scan"
            );
            let v2_groups = groups(&*v2, &grouped(&*v2, times), closure, &label);
            assert_eq!(
                v2_groups.values().collect::<Vec<_>>(),
                v4_groups.values().collect::<Vec<_>>(),
                "{label}"
            );
        }
    }
}

/// Samples per group, checking each group's fragment positions and
/// announcements as they arrive.
fn groups(
    observation: &dyn Observation,
    request: &AccessRequest,
    closure: GroupClosure,
    label: &str,
) -> BTreeMap<GroupKey, usize> {
    let mut traversal = observation.bind_read(request).expect("bind");
    let mut buffers = BatchBuffers::default();
    let mut samples = BTreeMap::<GroupKey, usize>::new();
    let mut closed = Vec::<GroupKey>::new();
    while let Some(event) = traversal.next(&mut buffers).expect("read") {
        match event {
            ReadEvent::Batch(batch) => {
                let mut covered = 0;
                for fragment in batch.fragments {
                    assert_eq!(
                        fragment.samples.start, covered,
                        "{label}: fragments tile the batch"
                    );
                    covered = fragment.samples.end;
                    assert!(
                        !closed.contains(&fragment.key),
                        "{label}: no data after closure"
                    );
                    let first = !samples.contains_key(&fragment.key);
                    *samples.entry(fragment.key).or_default() += fragment.samples.len();
                    let expect_first = matches!(
                        fragment.position,
                        FragmentPosition::First | FragmentPosition::Whole
                    );
                    assert_eq!(first, expect_first, "{label}: {fragment:?}");
                    if fragment.position.closes() {
                        assert_eq!(closure, GroupClosure::AtDelivery, "{label}");
                        closed.push(fragment.key);
                    }
                }
                assert_eq!(covered, batch.samples(), "{label}");
            }
            ReadEvent::GroupEnd(key) => {
                assert_eq!(closure, GroupClosure::Deferred, "{label}");
                assert!(
                    samples.contains_key(&key) && !closed.contains(&key),
                    "{label}"
                );
                closed.push(key);
            }
        }
    }
    assert_eq!(
        closed.len(),
        samples.len(),
        "{label}: every group closed once"
    );
    samples
}

/// The estimate is the inquiry function: pure over the catalogue and the
/// store's geometry, equal to what binding then confirms. For MSv4 it
/// models the chunks each batch touches: Zarr stores an edge chunk at full
/// size, so the twelve times in five-time chunks touch fifteen, and two of
/// the eight channels from four-channel chunks touch twice the selected
/// channels; the cursor counts what its reads touch by the same model. For
/// MSv2 it reports the tile shape and the rows read. Both report their
/// natural blocks and the bytes one sample costs under the roles.
#[test]
fn the_estimate_is_the_bound_realization_and_models_the_store() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    let payload = |channels, batch_bytes| AccessRequest {
        roles: Roles::PAYLOAD,
        ..request(channels, batch_bytes)
    };
    let whole = v4.estimate(&payload(0..8, usize::MAX)).expect("estimate");
    assert_eq!(whole.amplification(), 15.0 / 12.0);
    assert_eq!(whole.partitions[0].natural_blocks.times, 5);
    assert_eq!(whole.partitions[0].natural_blocks.channels, 4);
    assert_eq!(whole.partitions[0].padded_fraction, 0.0);
    assert_eq!(whole.partitions[0].times_per_batch, 12);
    assert_eq!(
        whole.partitions[0].bytes_per_sample,
        32 * 8 + 32 + 32 * 4 + 24
    );
    assert_eq!(
        whole.partitions[0].scratch_bytes_per_sample, 32,
        "the flag bytes before their conversion"
    );
    let narrow = AccessRequest {
        roles: Roles {
            uvw: false,
            ..Roles::PAYLOAD
        },
        ..payload(2..4, usize::MAX)
    };
    let estimate = v4.estimate(&narrow).expect("estimate");
    assert_eq!(estimate.amplification(), 2.0 * 15.0 / 12.0);
    // Three time chunks (5, 5, 2 times) of one frequency chunk, for each of
    // three variables.
    assert_eq!(estimate.decode_units(), 9);
    let mut traversal = v4.bind_read(&narrow).expect("bind");
    assert_eq!(
        traversal.estimate, estimate,
        "binding confirms the estimate"
    );
    let mut buffers = BatchBuffers::default();
    while traversal.next(&mut buffers).expect("read").is_some() {}
    assert_eq!(traversal.counters().decode_units, 9);
    assert_eq!(traversal.counters().batches, 1);

    let all = request(0..8, usize::MAX);
    let estimate = v2.estimate(&all).expect("estimate");
    let partition = &estimate.partitions[0];
    assert_eq!((partition.times, partition.baselines), (12, 10));
    assert_eq!(partition.decode_units, 120, "rows read");
    assert_eq!(partition.padded_fraction, 0.0);
    assert_eq!(
        partition.bytes_per_sample,
        32 * 8 + 32 + 4 * 4 + 24 + 3 * 8,
        "one WEIGHT per correlation, three per-sample scalars"
    );
    assert_eq!(
        partition.scratch_bytes_per_sample,
        2 * 8 + 1 + 3 * 8,
        "each row's number, sample, FLAG_ROW and three scalars; no padding and \
         every product, so no payload scratch"
    );
    assert_eq!(
        partition.natural_blocks.channels, 8,
        "the fixture's tile spans the band"
    );
    assert!(partition.natural_blocks.times >= 1);
    let traversal = v2.bind_read(&all).expect("bind");
    assert_eq!(traversal.estimate, estimate);
    let v4_all = v4.estimate(&all).expect("estimate").partitions.remove(0);
    assert_eq!(
        v4_all.bytes_per_sample,
        32 * 8 + 32 + 32 * 4 + 24 + 2 * 8,
        "spectral weights, two per-sample scalars (the interval is one constant)"
    );
}

/// A request for a subset of the correlation products delivers those
/// products' values and weights in the partition's order, and flags by the
/// requested policy: their own, or the OR over every stored product (what
/// production Stokes I from `XX` and `YY` needs, so a flagged cross hand
/// drops the sample). Both stores agree, and the estimate counts the
/// subset as selected and the whole axis as decoded.
#[test]
fn a_correlation_subset_carries_the_flags_it_is_asked_for() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    for (store, observation) in [("v2", &v2 as &dyn Observation), ("v4", &v4)] {
        let full = samples(
            &mut observation
                .bind_read(&request(0..8, usize::MAX))
                .expect("bind"),
            observation.catalogue(),
        )
        .expect("full");
        for flags_from in [FlagsFrom::Delivered, FlagsFrom::All] {
            let subset = AccessRequest {
                correlations: CorrelationRequest {
                    deliver: Deliver::These(vec![Correlation::YY, Correlation::XX]),
                    flags_from,
                },
                ..per_times(observation, request(0..8, 0), 5)
            };
            let estimate = observation.estimate(&subset).expect("estimate");
            assert!(
                estimate.partitions[0].selected_elements < estimate.partitions[0].decoded_elements,
                "{store}: a subset selects less than it decodes"
            );
            let every = AccessRequest {
                correlations: CorrelationRequest::ALL,
                ..subset.clone()
            };
            let every = observation.estimate(&every).expect("estimate");
            assert!(
                estimate.partitions[0].scratch_bytes_per_sample
                    > every.partitions[0].scratch_bytes_per_sample,
                "{store}: a subset decodes the whole axis into scratch, which the budget counts"
            );
            let mut traversal = observation.bind_read(&subset).expect("bind");
            let mut buffers = BatchBuffers::default();
            let Some(ReadEvent::Batch(batch)) = traversal.next(&mut buffers).expect("read") else {
                panic!("a batch");
            };
            assert_eq!(
                batch.correlations,
                [Correlation::XX, Correlation::YY],
                "{store}: the partition's order"
            );
            let read = samples(&mut traversal, observation.catalogue()).expect("rest");
            let mut cases = 0;
            for (key, sample) in read {
                let reference = &full[&key];
                let pick = |values: &[f32]| -> Vec<f32> {
                    values
                        .as_chunks::<4>()
                        .0
                        .iter()
                        .flat_map(|cell| [cell[0], cell[3]])
                        .collect()
                };
                let picked_values: Vec<Complex32> = reference
                    .values
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .flat_map(|cell| [cell[0], cell[3]])
                    .collect();
                assert_eq!(sample.values, picked_values, "{store} {key:?}");
                assert_eq!(sample.weights, pick(&reference.weights), "{store} {key:?}");
                let expected_flags: Vec<bool> = reference
                    .flags
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .flat_map(|cell| match flags_from {
                        FlagsFrom::Delivered => [cell[0], cell[3]],
                        FlagsFrom::All => [cell.iter().any(|f| *f); 2],
                    })
                    .collect();
                assert_eq!(
                    sample.flags, expected_flags,
                    "{store} {flags_from:?} {key:?}"
                );
                cases += usize::from(
                    expected_flags != {
                        let own: Vec<bool> = reference
                            .flags
                            .as_chunks::<4>()
                            .0
                            .iter()
                            .flat_map(|cell| [cell[0], cell[3]])
                            .collect();
                        own
                    },
                );
            }
            if flags_from == FlagsFrom::All {
                assert!(
                    cases > 0,
                    "{store}: the fixture flags some cross hands alone"
                );
            }
        }
    }
}

/// D1: the correlation axis lives once. Every batch of an all-products
/// traversal borrows the catalogue's list, every batch of a subset
/// traversal shares the one axis resolved at binding, and a piece packed
/// from a batch shares its batch's: no fill or pack copies the list.
#[test]
fn the_correlation_axis_is_shared_not_copied() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    for (store, observation) in [("v2", &v2 as &dyn Observation), ("v4", &v4)] {
        let listed = observation.catalogue().partitions[0].correlations.as_ptr();
        let subset = CorrelationRequest {
            deliver: Deliver::These(vec![Correlation::XX, Correlation::YY]),
            flags_from: FlagsFrom::Delivered,
        };
        for (correlations, every) in [(CorrelationRequest::ALL, true), (subset, false)] {
            let request = AccessRequest {
                correlations,
                ..per_times(observation, request(0..8, 0), 2)
            };
            let mut traversal = observation.bind_read(&request).expect("bind");
            let pool = BufferPool::new(2, traversal.estimate.peak_buffer_bytes());
            let mut axes = Vec::new();
            loop {
                match traversal.next_owned(&pool).expect("read") {
                    OwnedEvent::Batch(filled) => {
                        let batch = filled.batch();
                        let (piece, _) = pool
                            .pack(&batch.channel_window(0..4).expect("window"))
                            .expect("pack");
                        assert_eq!(
                            piece.batch().correlations.as_ptr(),
                            batch.correlations.as_ptr(),
                            "{store}: a packed piece shares its batch's axis"
                        );
                        axes.push(batch.correlations.as_ptr());
                    }
                    OwnedEvent::GroupEnd(_) => {}
                    OwnedEvent::End => break,
                }
            }
            assert!(axes.len() > 1, "{store}: several batches");
            assert!(
                axes.iter().all(|axis| *axis == axes[0]),
                "{store}: every batch shares one axis"
            );
            assert_eq!(
                axes[0] == listed,
                every,
                "{store}: all products borrow the catalogue's list, a subset its own"
            );
        }
    }
}

/// The budget admits whole times: one time that does not fit fails
/// binding, never a later read. Owned batches drawn from a pool of one
/// buffer each give it back when dropped, and the end takes none. Each
/// batch's buffer holds exactly the estimate's
/// [`buffer_bytes`](casa_observation::PartitionEstimate::buffer_bytes), the
/// pool's envelope; a pool whose envelope is a byte short refuses the
/// first batch before allocating anything.
#[test]
fn the_budget_fits_whole_times_or_refuses_binding() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    for (store, observation) in [("v2", &v2 as &dyn Observation), ("v4", &v4)] {
        let one_time = per_times(observation, request(0..8, 0), 1);
        let short = AccessRequest {
            batch_bytes: one_time.batch_bytes - 1,
            ..one_time.clone()
        };
        assert!(
            matches!(
                observation.bind_read(&short),
                Err(ObservationError::InvalidRequest { .. })
            ),
            "{store}: one time does not fit"
        );
        let mut traversal = observation.bind_read(&one_time).expect("bind");
        assert_eq!(traversal.estimate.batches(), 12, "{store}");
        let envelope = traversal.estimate.peak_buffer_bytes();
        let tight = BufferPool::new(1, envelope - 1);
        assert!(
            matches!(
                traversal.next_owned(&tight),
                Err(ObservationError::InvalidRequest { .. })
            ),
            "{store}: a batch over the envelope is refused"
        );
        assert_eq!(
            (tight.free(), tight.free_bytes()),
            (1, 0),
            "{store}: before anything is allocated"
        );
        // A take that finds the one buffer held fails at once and consumes
        // nothing: asking again delivers the next time.
        let mut traversal = observation.bind_read(&one_time).expect("bind");
        let limited = BufferPool::new(1, envelope).waiting_at_most(Duration::ZERO);
        let times = observation
            .grid(PartitionId(0), "base")
            .expect("grid")
            .times_s;
        let first = owned_batch(traversal.next_owned(&limited));
        assert!(matches!(
            traversal.next_owned(&limited),
            Err(ObservationError::PoolExhausted { capacity: 1 })
        ));
        assert_eq!(first.batch().time.offsets_s()[0], times[0], "{store}");
        drop(first);
        let second = owned_batch(traversal.next_owned(&limited));
        assert_eq!(second.batch().time.offsets_s()[0], times[1], "{store}");
        drop(second);
        let mut traversal = observation.bind_read(&one_time).expect("bind");
        // One buffer serves the traversal: each batch gives it back when
        // dropped, and the end takes none.
        let pool = BufferPool::new(1, envelope);
        let mut batches = 0;
        loop {
            match traversal.next_owned(&pool).expect("read") {
                OwnedEvent::End => break,
                OwnedEvent::GroupEnd(..) => panic!("no groups requested"),
                OwnedEvent::Batch(filled) => {
                    batches += 1;
                    assert_eq!(filled.batch().times(), 1);
                    assert_eq!(
                        filled.capacity_bytes(),
                        envelope,
                        "{store}: a batch's buffer holds what the estimate says"
                    );
                    assert!(
                        envelope <= one_time.batch_bytes,
                        "{store}: within the budget"
                    );
                }
            }
            assert_eq!(
                pool.free(),
                1,
                "{store}: a dropped batch returns its buffer"
            );
        }
        assert_eq!(batches, 12, "{store}");
    }
}

fn owned_batch(event: Result<OwnedEvent, ObservationError>) -> casa_observation::FilledBatch {
    match event.expect("read") {
        OwnedEvent::Batch(filled) => filled,
        other => panic!("a batch, not {other:?}"),
    }
}

/// An MSv2 row absent from the grid (an antenna dropping out, a baseline
/// not written) is padding: its cell is flagged, its values, weights, UVW
/// and time auxiliaries NaN (as XRADIO pads an MSv4), and the validity rule
/// excludes it. Every other sample is unchanged, the batch count is the
/// grid's, and the estimate reports the padded fraction.
#[test]
fn msv2_cells_no_row_fills_are_padding() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    let reference = samples(
        &mut Msv2Observation::open(&path)
            .expect("v2")
            .bind_read(&request(0..8, usize::MAX))
            .expect("bind"),
        Msv2Observation::open(&path).expect("v2").catalogue(),
    )
    .expect("reference");
    let mut ms = casa_ms::MeasurementSet::open(&path).expect("ms");
    ms.main_table_mut().remove_rows(&[7]).expect("remove row 7");
    ms.save().expect("save");
    // The writing handle holds the table lock until it is dropped.
    drop(ms);
    let sparse = Msv2Observation::open(&path).expect("sparse");
    let estimate = sparse
        .estimate(&request(0..8, usize::MAX))
        .expect("estimate");
    assert_eq!(estimate.partitions[0].decode_units, 119);
    assert!((estimate.partitions[0].padded_fraction - 1.0 / 120.0).abs() < 1e-12);
    let mut traversal = sparse
        .bind_read(&per_times(&sparse, request(0..8, 0), 4))
        .expect("bind");
    let mut buffers = BatchBuffers::default();
    let mut padded = 0;
    let mut checked = 0;
    let stations = stations(sparse.catalogue());
    let mjd_zero = MjdHighPrec::new(0.0, 0.0);
    while let Some(event) = traversal.next(&mut buffers).expect("read") {
        let ReadEvent::Batch(batch) = event else {
            continue;
        };
        let validity = batch.validity();
        let flags = batch.group.flags.expect("flags");
        let values = batch.group.values.expect("values");
        let weights = batch.group.weights.expect("weights");
        let uvw = batch.group.uvw.expect("uvw");
        for sample in 0..batch.samples() {
            let coordinate = batch.sample(sample);
            let key = (
                batch.time.seconds_since(sample / 10, mjd_zero).to_bits(),
                stations[coordinate.baseline.antenna1.0 as usize].clone(),
                stations[coordinate.baseline.antenna2.0 as usize].clone(),
            );
            let cell = ndarray::s![sample, .., ..];
            let is_padding = uvw.metres_at(sample).iter().all(|m| m.is_nan());
            if is_padding {
                padded += 1;
                assert!(flags.slice(cell).iter().all(|f| *f), "padding is flagged");
                assert!(
                    values
                        .slice(cell)
                        .iter()
                        .all(|v| v.re.is_nan() && v.im.is_nan())
                );
                assert!(weights.slice(cell).iter().all(|w| w.is_nan()));
                assert!(batch.timing.centroid.expect("centroid").offsets_s()[sample].is_nan());
                assert!(batch.interval_s(sample).expect("interval").is_nan());
                assert!((0..8).all(|c| (0..4).all(|p| validity.excluded(sample, c, p))));
                assert!(reference.contains_key(&key), "the removed row's cell");
            } else {
                let expected = &reference[&key];
                assert_eq!(
                    values.slice(cell).iter().copied().collect::<Vec<_>>(),
                    expected.values
                );
                assert_eq!(
                    flags.slice(cell).iter().copied().collect::<Vec<_>>(),
                    expected.flags
                );
                checked += 1;
            }
        }
    }
    assert_eq!((padded, checked), (1, 119));
    assert_eq!(
        traversal.counters().batches,
        3,
        "the grid's three batches of four times"
    );
}

/// Two MSv2 rows of one time, baseline and partition are refused at
/// opening, naming both rows: never "the last row wins".
#[test]
fn duplicate_rows_are_refused_at_opening() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    let source = casa_ms::MeasurementSet::open(fixture(&root.path().join("source"), "ms_trial.ms"))
        .expect("source");
    let mut ms = casa_ms::MeasurementSet::open(&path).expect("ms");
    ms.main_table_mut()
        .copy_rows_with_mapping(source.main_table(), &[3])
        .expect("duplicate row 3");
    ms.save().expect("save");
    drop((ms, source));
    match Msv2Observation::open(&path) {
        Err(ObservationError::InvalidStore { reason }) => {
            assert!(reason.contains("rows 3 and 120"), "{reason}");
        }
        other => panic!("{:?}", other.map(|_| ())),
    }
}

/// MSv2 `FLAG_ROW` is folded into the delivered flags: a row flagged only
/// by it has every value flagged in the batch (and excluded by the validity
/// rule), while its stored `FLAG` cells are untouched on disk. The MSv4
/// writer takes such a batch as it is: there is no row flag to refuse.
#[test]
fn flag_row_is_folded_into_the_flags() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    let mut ms = casa_ms::MeasurementSet::open(&path).expect("ms");
    ms.main_table_mut()
        .fill_column_range(
            "FLAG_ROW",
            casa_tables::RowRange::new(5, 6),
            casa_types::Value::Scalar(casa_types::ScalarValue::Bool(true)),
        )
        .expect("set FLAG_ROW");
    ms.save().expect("save");
    drop(ms);
    let plain = Msv2Observation::open(fixture(root.path().join("plain").as_path(), "ms_trial.ms"))
        .expect("plain");
    let flagged = Msv2Observation::open(&path).expect("flagged");
    let plain_samples = samples(
        &mut plain.bind_read(&request(0..8, usize::MAX)).expect("bind"),
        plain.catalogue(),
    )
    .expect("plain");
    let flagged_samples = samples(
        &mut flagged.bind_read(&request(0..8, usize::MAX)).expect("bind"),
        flagged.catalogue(),
    )
    .expect("flagged");
    let differing: Vec<&Key> = plain_samples
        .iter()
        .filter(|(key, sample)| flagged_samples[*key] != **sample)
        .map(|(key, _)| key)
        .collect();
    assert_eq!(differing.len(), 1, "one row flagged");
    let sample = &flagged_samples[differing[0]];
    assert!(
        sample.flags.iter().all(|f| *f),
        "every value of the row flagged"
    );
    assert_eq!(
        sample.values, plain_samples[differing[0]].values,
        "values untouched"
    );
    let out = root.path().join("flagged.ps.zarr");
    let (catalogue, grid) = writer::declaration(&flagged, true);
    let set = casa_ms::observation_io::Msv4ProcessingSetWriter::create(&out).expect("create");
    let mut writer = set
        .dataset(
            &catalogue,
            PartitionId(0),
            &["base"],
            &grid,
            &writer::storage(),
        )
        .expect("declare");
    let mut traversal = flagged.bind_read(&request(0..8, usize::MAX)).expect("bind");
    let mut buffers = BatchBuffers::default();
    while let Some(ReadEvent::Batch(batch)) = traversal.next(&mut buffers).expect("read") {
        writer
            .write_batch(&batch)
            .expect("a folded row flag writes");
    }
    let written = writer::publish(set, writer).expect("finish");
    let reopened = Msv4Observation::open(&written).expect("reopen");
    let read = samples(
        &mut reopened
            .bind_read(&request(0..8, usize::MAX))
            .expect("bind"),
        reopened.catalogue(),
    )
    .expect("read back");
    assert_eq!(read, flagged_samples);
}
