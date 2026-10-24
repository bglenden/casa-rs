// SPDX-License-Identifier: LGPL-3.0-or-later
//! Laws of the trial observation adapters on the MSv2/MSv4 trial fixtures
//! (`docs/measurement-set/trial/README.md`): the MSv4 store XRADIO wrote
//! from the MSv2, read through both adapters into the same batches.

#[path = "observation_io/refusals.rs"]
mod refusals;
#[path = "observation_io/work.rs"]
mod work;
#[path = "observation_io/writer.rs"]
mod writer;

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use casa_ms::observation_io::{GroupClosure, Msv2Observation, Msv4Observation, OpenOptions};
use casa_observation::{
    AccessRequest, BatchBuffers, Catalogue, Correlation, FragmentPosition, GroupKey, Grouping,
    Layout, Observation, ObservationError, Occupancy, PartitionId, Precision, ReadEvent, Roles,
    SampleTopology, VisibilityCursor, VisibilityUnits, WeightView,
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

pub(crate) fn request(channels: std::ops::Range<usize>, max_samples: usize) -> AccessRequest {
    AccessRequest {
        partition: PartitionId(0),
        data_group: "base".to_string(),
        roles: Roles::ALL,
        channels,
        layout: Layout::SampleMajor,
        precision: Precision::Complex32 {
            allow_narrowing: false,
        },
        grouping: Grouping::None,
        max_samples,
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
    cursor: &mut dyn VisibilityCursor,
    catalogue: &Catalogue,
) -> Result<BTreeMap<Key, Sample>, ObservationError> {
    let stations = stations(catalogue);
    let mjd_zero = MjdHighPrec::new(0.0, 0.0);
    let mut buffers = BatchBuffers::default();
    let mut all = BTreeMap::new();
    while let Some(event) = cursor.next(&mut buffers)? {
        let ReadEvent::Batch(batch) = event else {
            continue;
        };
        let values = batch
            .group
            .values
            .expect("values")
            .as_complex32()
            .expect("c64");
        let flags = batch.group.flags.expect("flags");
        let weights = batch.group.weights.expect("weights");
        let uvw = batch.group.uvw.expect("uvw");
        let time = batch.topology.time();
        let centroid = batch.timing.centroid.expect("centroid");
        let per_time = batch.topology.samples_per_time();
        for sample in 0..batch.samples() {
            let coordinate = batch.topology.sample(sample);
            let key = (
                time.seconds_since(sample / per_time, mjd_zero).to_bits(),
                stations[coordinate.baseline.antenna1.0 as usize].clone(),
                stations[coordinate.baseline.antenna2.0 as usize].clone(),
            );
            let mut sample_weights = Vec::new();
            for channel in 0..batch.channel_count() {
                for correlation in 0..batch.correlations.len() {
                    sample_weights.push(weights.get(sample, channel, correlation));
                }
            }
            let scan = batch.scan(sample).expect("scan");
            let previous = all.insert(
                key,
                Sample {
                    values: values
                        .slice(ndarray::s![sample, .., ..])
                        .iter()
                        .copied()
                        .collect(),
                    flags: flags
                        .slice(ndarray::s![sample, .., ..])
                        .iter()
                        .copied()
                        .collect(),
                    weights: sample_weights,
                    uvw: uvw.get(sample),
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

/// The two catalogues describe the same observation, in each store's own
/// names: XRADIO names antennas `NAME_STATION` and fields `NAME_ID`,
/// declares the visibility unit `unkown`, and states the UVW frame as the
/// phase centre's, which is the frame the MSv2 adapter resolves for MSv2's
/// `ITRF`-labelled UVW. Positions, diameters and angles are native
/// Measures and Quanta values.
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
    let (p2, p4) = (&a.partitions[0], &b.partitions[0]);
    assert_eq!(p2.frequencies_hz, p4.frequencies_hz);
    assert_eq!(p2.channel_widths_hz, p4.channel_widths_hz);
    assert_eq!(p2.frequency_frame, FrequencyRef::LSRK);
    assert_eq!(p4.frequency_frame, FrequencyRef::LSRK);
    let linear = vec![
        Correlation::XX,
        Correlation::XY,
        Correlation::YX,
        Correlation::YY,
    ];
    assert_eq!(p2.correlations, linear);
    assert_eq!(p4.correlations, linear);
    let (g2, g4) = (&p2.data_groups[0], &p4.data_groups[0]);
    assert_eq!(g2.units, VisibilityUnits::Undeclared);
    assert_eq!(g4.units, VisibilityUnits::Declared("unkown".to_string()));
    assert_eq!(g4.visibilities, "VISIBILITY");
    assert_eq!(g2.uvw_frame.effective, DirectionRef::J2000);
    assert_eq!(g2.uvw_frame.stored_label, Some(DirectionRef::ITRF));
    assert_eq!(g4.uvw_frame.effective, DirectionRef::J2000);
}

/// Every sample XRADIO wrote equals its MSv2 row exactly: visibilities,
/// flags, weights (MSv2's per-correlation `WEIGHT` broadcast over
/// channels, MSv4's spectral `WEIGHT`), UVW, time, and the time centroid,
/// interval, exposure and scan, over the whole band and channel windows,
/// whatever the batch size. The v4 store holds exactly the v2
/// acquisitions: none padded, none lost.
#[test]
fn xradio_samples_equal_their_msv2_rows() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    for channels in [0..8, 2..6, 7..8] {
        let reference = samples(
            &mut v2.bind_read(&request(channels.clone(), 120)).expect("bind"),
            v2.catalogue(),
        )
        .expect("v2 read");
        assert_eq!(reference.len(), 120);
        for max_samples in [10, 20, 50, 120] {
            let read = samples(
                &mut v4
                    .bind_read(&request(channels.clone(), max_samples))
                    .expect("bind"),
                v4.catalogue(),
            )
            .expect("v4 read");
            assert_eq!(
                read, reference,
                "channels {channels:?}, {max_samples} samples per batch"
            );
        }
        for max_samples in [1, 7, 25] {
            let read = samples(
                &mut v2
                    .bind_read(&request(channels.clone(), max_samples))
                    .expect("bind"),
                v2.catalogue(),
            )
            .expect("v2 read");
            assert_eq!(
                read, reference,
                "v2 channels {channels:?}, {max_samples} per batch"
            );
        }
    }
}

/// The time auxiliaries the v2 batch delivers are MAIN's columns, as the
/// production typed column accessors read them cell by cell (not the
/// adapter's bulk fill), not values the adapter makes up.
#[test]
fn time_auxiliaries_are_main_columns() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    let ms = casa_ms::MeasurementSet::open(&path).expect("ms");
    let main = ms.main_table();
    let v2 = Msv2Observation::open(&path).expect("v2");
    let mut cursor = v2.bind_read(&request(0..8, 120)).expect("bind");
    let mut buffers = BatchBuffers::default();
    let Some(ReadEvent::Batch(batch)) = cursor.next(&mut buffers).expect("read") else {
        panic!("a batch");
    };
    let mjd_zero = MjdHighPrec::new(0.0, 0.0);
    let centroid = batch.timing.centroid.expect("centroid");
    for row in 0..main.row_count() {
        use casa_ms::columns::exposure_interval::{ExposureColumn, IntervalColumn};
        use casa_ms::columns::time_columns::TimeColumn;
        assert_eq!(
            centroid.seconds_since(row, mjd_zero),
            TimeColumn::centroid(main)
                .get_mjd_seconds(row)
                .expect("centroid")
        );
        assert_eq!(
            batch.interval_s(row),
            Some(IntervalColumn::new(main).get(row).expect("interval"))
        );
        assert_eq!(
            batch.exposure_s(row),
            Some(ExposureColumn::new(main).get(row).expect("exposure"))
        );
        let scan = batch.scan(row).expect("scan");
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

/// The adapters keep each store's shapes: MSv2 rows are listed samples
/// with a per-correlation weight and known occupancy; MSv4 is a factored
/// time × baseline batch with spectral weights and unknown occupancy, whose
/// rank-three sample-major view borrows the decoded buffer directly, an
/// interval shared by the whole batch and scans shared per time. Times keep
/// their store's origin: seconds from MJD 0, or Unix seconds. Both deliver
/// UVW in the resolved effective frame.
#[test]
fn batches_keep_their_store_shapes() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    let mut buffers = BatchBuffers::default();
    let mut cursor = v2.bind_read(&request(0..8, 40)).expect("bind");
    let Some(ReadEvent::Batch(batch)) = cursor.next(&mut buffers).expect("read") else {
        panic!("a batch");
    };
    assert!(matches!(batch.topology, SampleTopology::Irregular(_)));
    assert_eq!(batch.topology.occupancy(), Occupancy::KnownPresent);
    assert!(matches!(
        batch.group.weights,
        Some(WeightView::PerCorrelation(_))
    ));
    assert!(batch.group.row_flags.is_some());
    assert_eq!(batch.topology.time().origin().day(), 0.0);
    assert_eq!(
        batch.group.uvw.expect("uvw").reference(),
        DirectionRef::J2000
    );

    let mut buffers = BatchBuffers::default();
    let mut cursor = v4.bind_read(&request(0..8, 40)).expect("bind");
    assert_eq!(cursor.realization().samples_per_batch, 40);
    let Some(ReadEvent::Batch(batch)) = cursor.next(&mut buffers).expect("read") else {
        panic!("a batch");
    };
    let SampleTopology::TimeBaseline(topology) = batch.topology else {
        panic!("factored");
    };
    assert_eq!((topology.time.len(), topology.baselines.len()), (4, 10));
    assert_eq!(batch.topology.occupancy(), Occupancy::Unknown);
    assert!(matches!(batch.group.weights, Some(WeightView::Spectral(_))));
    assert!(batch.group.row_flags.is_none());
    assert!(matches!(
        batch.timing.interval_s,
        Some(casa_observation::Shared::Constant(10.0))
    ));
    assert!(matches!(
        batch.scan,
        Some(casa_observation::Shared::PerTime(_))
    ));
    assert_eq!(topology.time.origin().day(), 40_587.0);
    assert_eq!(
        batch.group.uvw.expect("uvw").reference(),
        DirectionRef::J2000
    );
    let values = batch
        .group
        .values
        .expect("values")
        .as_complex32()
        .expect("c64");
    assert_eq!(values.shape(), [40, 8, 4]);
    let view = values.as_ptr();
    // One time is 10 samples; a window of whole times stays factored.
    assert!(batch.sample_window(10..30).is_ok());
    assert!(batch.sample_window(5..15).is_err());
    let window = batch.channel_window(2..4).expect("window");
    assert_eq!(window.channels(), 2..4);
    assert_eq!(
        window.frequency.hz().as_ptr(),
        batch.frequency.hz().slice(ndarray::s![2..]).as_ptr()
    );
    // The view was the decode buffer itself.
    assert_eq!(view, buffers.arrays().values.as_ptr());
}

/// The MSv4 time origin re-expresses the MSv2 time exactly: XRADIO's
/// conversion to Unix seconds lost nothing on this fixture.
#[test]
fn time_origins_re_express_each_other_exactly() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    let mjd_zero = MjdHighPrec::new(0.0, 0.0);
    let first_time = |cursor: &mut dyn VisibilityCursor| {
        let mut buffers = BatchBuffers::default();
        let Some(ReadEvent::Batch(batch)) = cursor.next(&mut buffers).expect("read") else {
            panic!("a batch");
        };
        batch.topology.time().seconds_since(0, mjd_zero)
    };
    let v2_time = first_time(&mut v2.bind_read(&request(0..8, 1)).expect("bind"));
    let v4_time = first_time(&mut v4.bind_read(&request(0..8, 10)).expect("bind"));
    assert_eq!(v2_time, v4_time);
}

/// The fragments of every 60 s bin, whatever the batch size: each bin's
/// samples once, in order, its first fragment `First` or `Whole`, its last
/// `Last` or `Whole`, and, from a store opened to defer closure, one
/// `GroupEnd` after its last fragment instead. The deferral is an opening
/// option, not part of the request: a consumer handles both.
#[test]
fn group_fragments_do_not_depend_on_batch_size() {
    let root = tempfile::tempdir().expect("root");
    let v2_path = fixture(root.path(), "ms_trial.ms");
    let v4_path = fixture(root.path(), "ms_trial.ps.zarr");
    let grouped = |max_samples: usize| AccessRequest {
        grouping: Grouping::TimeBins {
            width_s: 60.0,
            combine_fields: false,
        },
        ..request(0..8, max_samples)
    };
    for closure in [GroupClosure::AtDelivery, GroupClosure::Deferred] {
        let options = OpenOptions {
            group_closure: closure,
        };
        let v2 = casa_ms::observation_io::open(&v2_path, &options).expect("v2");
        let v4 = casa_ms::observation_io::open(&v4_path, &options).expect("v4");
        for max_samples in [10, 30, 40, 60, 70, 120] {
            let label = format!("{closure:?}, {max_samples} per batch");
            let v4_groups = groups(&*v4, &grouped(max_samples), closure, &label);
            assert_eq!(v4_groups.len(), 2, "{label}");
            assert!(
                v4_groups.values().all(|samples| *samples == 60),
                "{label}: {v4_groups:?}"
            );
            let v2_groups = groups(&*v2, &grouped(max_samples), closure, &label);
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
    let mut cursor = observation.bind_read(request).expect("bind");
    let mut buffers = BatchBuffers::default();
    let mut samples = BTreeMap::<GroupKey, usize>::new();
    let mut closed = Vec::<GroupKey>::new();
    while let Some(event) = cursor.next(&mut buffers).expect("read") {
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

/// Reads touch whole chunks, and Zarr stores an edge chunk at full size:
/// the twelve times in five-time chunks touch fifteen. A channel window
/// touches whole frequency chunks too: two of the eight channels from
/// four-channel chunks touch twice the selected channels. The realization
/// models both from the chunk grid before any read, and the cursor counts
/// what its reads touch by the same model (a model of touched chunks, not
/// decodes observed inside zarrs).
#[test]
fn realization_reports_chunk_amplification() {
    let root = tempfile::tempdir().expect("root");
    let v4 = Msv4Observation::open(fixture(root.path(), "ms_trial.ps.zarr")).expect("v4");
    let payload = |channels, max_samples| AccessRequest {
        roles: Roles::PAYLOAD,
        ..request(channels, max_samples)
    };
    let whole = v4.bind_read(&payload(0..8, 120)).expect("bind");
    assert_eq!(whole.realization().amplification(), 15.0 / 12.0);
    let narrow = AccessRequest {
        roles: Roles {
            uvw: false,
            ..Roles::PAYLOAD
        },
        ..payload(2..4, 120)
    };
    let mut cursor = v4.bind_read(&narrow).expect("bind");
    let realization = cursor.realization().clone();
    assert_eq!(realization.amplification(), 2.0 * 15.0 / 12.0);
    // Three time chunks (5, 5, 2 times) of one frequency chunk, for each of
    // three variables.
    assert_eq!(realization.decode_units, 9);
    let mut buffers = BatchBuffers::default();
    while cursor.next(&mut buffers).expect("read").is_some() {}
    assert_eq!(cursor.counters().decode_units, 9);
}
