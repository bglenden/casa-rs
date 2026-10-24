// SPDX-License-Identifier: LGPL-3.0-or-later
//! Laws of the trial observation adapters on the MSv2/MSv4 trial fixtures
//! (`docs/measurement-set/trial/README.md`): the MSv4 store XRADIO wrote
//! from the MSv2, read through both adapters into the same batches.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use casa_ms::observation_io::{Msv2Observation, Msv4Observation};
use casa_observation::{
    AccessRequest, BatchBuffers, Closure, Correlation, FragmentPosition, GroupKey, Grouping,
    Layout, ObservationError, Occupancy, PartitionId, Precision, ReadEvent, Roles, SampleTopology,
    VisibilityCursor, VisibilityUnits, WeightView,
};
use casa_types::measures::MjdHighPrec;
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use flate2::read::GzDecoder;
use num_complex::Complex32;
use tar::Archive;

/// Unpack fixture `name` (`ms_trial.ms` or `ms_trial.ps.zarr`).
fn fixture(root: &Path, name: &str) -> PathBuf {
    let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/ms_trial")
        .join(format!("{name}.tgz"));
    Archive::new(GzDecoder::new(File::open(archive).expect("fixture")))
        .unpack(root)
        .expect("unpack");
    root.join(name)
}

fn request(channels: std::ops::Range<usize>, max_samples: usize) -> AccessRequest {
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
        closure: Closure::AtDelivery,
        max_samples,
    }
}

/// One sample's payload, keyed by MJD seconds and the two antennas'
/// stations, so the formats' different antenna and field naming does not
/// decide the match.
#[derive(Debug, PartialEq)]
struct Sample {
    values: Vec<Complex32>,
    flags: Vec<bool>,
    weights: Vec<f32>,
    uvw: [f64; 3],
}

type Key = (u64, String, String);

/// Every sample a traversal delivers, keyed.
fn samples(
    cursor: &mut impl VisibilityCursor,
    stations: &[String],
) -> Result<BTreeMap<Key, Sample>, ObservationError> {
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
        for sample in 0..batch.samples() {
            let coordinate = batch.topology.sample(sample);
            let seconds = time.origin().day() * 86_400.0 + coordinate.time_offset_s;
            let key = (
                seconds.to_bits(),
                stations[coordinate.baseline.antenna1.0 as usize].clone(),
                stations[coordinate.baseline.antenna2.0 as usize].clone(),
            );
            let mut sample_weights = Vec::new();
            for channel in 0..batch.channel_count() {
                for correlation in 0..batch.correlations.len() {
                    sample_weights.push(weights.get(sample, channel, correlation));
                }
            }
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
                },
            );
            assert!(previous.is_none(), "one acquisition per time and baseline");
        }
    }
    Ok(all)
}

fn stations(catalogue: &casa_observation::Catalogue) -> Vec<String> {
    catalogue
        .antennas
        .iter()
        .map(|antenna| antenna.station.clone())
        .collect()
}

/// The two catalogues describe the same observation, in each store's own
/// names: XRADIO names antennas `NAME_STATION` and fields `NAME_ID`, labels
/// the UVW frame as the phase centre's (CASA `fixvis`'s reading of the
/// mislabelled ITRF), and declares the visibility unit `unkown`.
#[test]
fn catalogues_describe_the_same_observation() {
    let root = tempfile::tempdir().expect("root");
    let v2 = Msv2Observation::open(fixture(root.path(), "ms_trial.ms")).expect("v2");
    let v4 = Msv4Observation::open(fixture(root.path(), "ms_trial.ps.zarr")).expect("v4");
    let (a, b) = (v2.catalogue(), v4.catalogue());
    assert_eq!(stations(a), stations(b));
    for (v2, v4) in a.antennas.iter().zip(&b.antennas) {
        assert_eq!(format!("{}_{}", v2.name, v2.station), v4.name);
    }
    assert_eq!(a.fields.len(), 1);
    assert_eq!(b.fields.len(), 1);
    assert_eq!(format!("{}_0", a.fields[0].name), b.fields[0].name);
    assert_eq!(
        a.fields[0].phase_centre.as_angles(),
        b.fields[0].phase_centre.as_angles()
    );
    assert_eq!(a.fields[0].phase_centre.refer(), DirectionRef::J2000);
    assert_eq!(b.fields[0].phase_centre.refer(), DirectionRef::J2000);
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
    assert_eq!(p2.data_groups[0].units, VisibilityUnits::Undeclared);
    assert_eq!(
        p4.data_groups[0].units,
        VisibilityUnits::Declared("unkown".to_string())
    );
    assert_eq!(p4.data_groups[0].visibilities, "VISIBILITY");
}

/// Every sample XRADIO wrote equals its MSv2 row exactly: visibilities,
/// flags, weights (MSv2's per-correlation `WEIGHT` broadcast over
/// channels, MSv4's spectral `WEIGHT`), UVW and time, over the whole band
/// and a channel window, whatever the batch size. The v4 store holds
/// exactly the v2 acquisitions: none padded, none lost.
#[test]
fn xradio_samples_equal_their_msv2_rows() {
    let root = tempfile::tempdir().expect("root");
    let v2 = Msv2Observation::open(fixture(root.path(), "ms_trial.ms")).expect("v2");
    let v4 = Msv4Observation::open(fixture(root.path(), "ms_trial.ps.zarr")).expect("v4");
    let (s2, s4) = (stations(v2.catalogue()), stations(v4.catalogue()));
    for channels in [0..8, 2..6, 7..8] {
        let reference = samples(
            &mut v2.bind_read(&request(channels.clone(), 120)).expect("bind"),
            &s2,
        )
        .expect("v2 read");
        assert_eq!(reference.len(), 120);
        for max_samples in [10, 20, 50, 120] {
            let read = samples(
                &mut v4
                    .bind_read(&request(channels.clone(), max_samples))
                    .expect("bind"),
                &s4,
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
                &s2,
            )
            .expect("v2 read");
            assert_eq!(
                read, reference,
                "v2 channels {channels:?}, {max_samples} per batch"
            );
        }
    }
}

/// The adapters keep each store's shapes: MSv2 rows are listed samples
/// with a per-correlation weight and known occupancy; MSv4 is a factored
/// time × baseline batch with spectral weights and unknown occupancy, whose
/// rank-three sample-major view borrows the decoded buffer directly. Times
/// keep their store's origin: seconds from MJD 0, or Unix seconds.
#[test]
fn batches_keep_their_store_shapes() {
    let root = tempfile::tempdir().expect("root");
    let v2 = Msv2Observation::open(fixture(root.path(), "ms_trial.ms")).expect("v2");
    let v4 = Msv4Observation::open(fixture(root.path(), "ms_trial.ps.zarr")).expect("v4");
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
        DirectionRef::ITRF
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
    assert_eq!(view, buffers.values.as_ptr());
}

/// The MSv4 time origin re-expresses the MSv2 time exactly: XRADIO's
/// conversion to Unix seconds lost nothing on this fixture.
#[test]
fn time_origins_re_express_each_other_exactly() {
    let root = tempfile::tempdir().expect("root");
    let v2 = Msv2Observation::open(fixture(root.path(), "ms_trial.ms")).expect("v2");
    let v4 = Msv4Observation::open(fixture(root.path(), "ms_trial.ps.zarr")).expect("v4");
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
/// `Last` or `Whole`, and under deferred closure one `GroupEnd` after its
/// last fragment instead.
#[test]
fn group_fragments_do_not_depend_on_batch_size() {
    let root = tempfile::tempdir().expect("root");
    let v2 = Msv2Observation::open(fixture(root.path(), "ms_trial.ms")).expect("v2");
    let v4 = Msv4Observation::open(fixture(root.path(), "ms_trial.ps.zarr")).expect("v4");
    let grouped = |max_samples: usize, closure: Closure| AccessRequest {
        grouping: Grouping::TimeBins {
            width_s: 60.0,
            combine_fields: false,
        },
        closure,
        ..request(0..8, max_samples)
    };
    for closure in [Closure::AtDelivery, Closure::Deferred] {
        for max_samples in [10, 30, 40, 60, 70, 120] {
            let label = format!("{closure:?}, {max_samples} per batch");
            let v4_groups = groups(
                &mut v4.bind_read(&grouped(max_samples, closure)).expect("bind"),
                closure,
                &label,
            );
            assert_eq!(v4_groups.len(), 2, "{label}");
            assert!(
                v4_groups.values().all(|samples| *samples == 60),
                "{label}: {v4_groups:?}"
            );
            let v2_groups = groups(
                &mut v2.bind_read(&grouped(max_samples, closure)).expect("bind"),
                closure,
                &label,
            );
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
    cursor: &mut impl VisibilityCursor,
    closure: Closure,
    label: &str,
) -> BTreeMap<GroupKey, usize> {
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
                        assert_eq!(closure, Closure::AtDelivery, "{label}");
                        closed.push(fragment.key);
                    }
                }
                assert_eq!(covered, batch.samples(), "{label}");
            }
            ReadEvent::GroupEnd(key) => {
                assert_eq!(closure, Closure::Deferred, "{label}");
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

/// Reads decode whole chunks, and Zarr stores an edge chunk at full size:
/// the twelve times in five-time chunks decode fifteen. A channel window
/// decodes whole frequency chunks too: two of the eight channels from
/// four-channel chunks decode twice the selected channels. The realization
/// reports both before any read.
#[test]
fn realization_reports_chunk_amplification() {
    let root = tempfile::tempdir().expect("root");
    let v4 = Msv4Observation::open(fixture(root.path(), "ms_trial.ps.zarr")).expect("v4");
    let whole = v4.bind_read(&request(0..8, 120)).expect("bind");
    assert_eq!(whole.realization().amplification(), 15.0 / 12.0);
    let narrow = AccessRequest {
        roles: Roles {
            uvw: false,
            ..Roles::ALL
        },
        ..request(2..4, 120)
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

/// A dataset spec for the trial writer from `catalogue` and the times,
/// baselines and fields a traversal of `cursor` delivers, with XRADIO's
/// converter naming applied when `rename` (antennas `NAME_STATION`, fields
/// `NAME_ID`).
fn writer_spec(
    catalogue: &casa_observation::Catalogue,
    cursor: &mut impl VisibilityCursor,
    rename: bool,
) -> casa_ms::observation_io::Msv4DatasetSpec {
    let mut times = Vec::<f64>::new();
    let mut baselines = Vec::new();
    let mut fields = Vec::new();
    let mut buffers = BatchBuffers::default();
    while let Some(event) = cursor.next(&mut buffers).expect("read") {
        let ReadEvent::Batch(batch) = event else {
            continue;
        };
        let unix = MjdHighPrec::new(40_587.0, 0.0);
        for sample in 0..batch.samples() {
            let coordinate = batch.topology.sample(sample);
            let slot = match batch.topology {
                SampleTopology::Irregular(_) => sample,
                SampleTopology::TimeBaseline(t) => sample / t.baselines.len(),
            };
            let time = batch.topology.time().seconds_since(slot, unix);
            if times.last() != Some(&time) {
                times.push(time);
                fields.push(coordinate.field);
            }
            if !baselines.contains(&coordinate.baseline) {
                baselines.push(coordinate.baseline);
            }
        }
    }
    let mut antennas = catalogue.antennas.clone();
    let mut field_entries = catalogue.fields.clone();
    if rename {
        for antenna in &mut antennas {
            antenna.name = format!("{}_{}", antenna.name, antenna.station);
        }
        for (index, field) in field_entries.iter_mut().enumerate() {
            field.name = format!("{}_{index}", field.name);
        }
    }
    let partition = &catalogue.partitions[0];
    casa_ms::observation_io::Msv4DatasetSpec {
        name: "ms_trial_0".to_string(),
        creator: "casa-rs".to_string(),
        creation_date: "2026-10-24T00:00:00+00:00".to_string(),
        telescope: catalogue.telescope.clone(),
        observers: catalogue.observers.clone(),
        project: catalogue.project.clone(),
        antennas,
        fields: field_entries,
        baselines,
        scan_per_time: vec!["1".to_string(); times.len()],
        times_unix_s: times,
        time_scale: casa_types::measures::EpochRef::UTC,
        integration_time_s: 10.0,
        field_per_time: fields,
        scan_intents: vec!["TARGET.ON_SOURCE".to_string()],
        spectral_window: partition.spectral_window.clone(),
        frequencies_hz: partition.frequencies_hz.clone(),
        channel_width_hz: partition.channel_widths_hz[0],
        frequency_frame: partition.frequency_frame,
        correlations: partition.correlations.clone(),
        units: partition.data_groups[0].units.clone(),
        uvw_frame: DirectionRef::J2000,
        chunks: [5, 10, 4, 4],
        codec: casa_ms::observation_io::Msv4Codec::BloscLz4,
    }
}

/// The trial writer creates a fresh MSv4 from either store's batches that
/// reads back, through the trial reader, sample for sample equal to the
/// MSv2: from listed MSv2 rows (placed into time × baseline order, the
/// per-correlation weight written per channel) and from factored MSv4
/// batches (encoded straight from their views), with batches that straddle
/// the five-time chunks. XRADIO's own check of such a store is
/// `tools/ms-trial/check_interop.sh`.
#[test]
fn rust_written_msv4_reads_back_equal_to_the_msv2() {
    use casa_ms::observation_io::Msv4Writer;
    let root = tempfile::tempdir().expect("root");
    let v2 = Msv2Observation::open(fixture(root.path(), "ms_trial.ms")).expect("v2");
    let v4 = Msv4Observation::open(fixture(root.path(), "ms_trial.ps.zarr")).expect("v4");
    let reference = samples(
        &mut v2.bind_read(&request(0..8, 120)).expect("bind"),
        &stations(v2.catalogue()),
    )
    .expect("v2");
    for (label, batch_samples) in [("v2", 40), ("v2", 120), ("v4", 40), ("v4", 120)] {
        let out = root.path().join(format!("{label}-{batch_samples}.ps.zarr"));
        let (counters, written) = if label == "v2" {
            let spec = writer_spec(
                v2.catalogue(),
                &mut v2.bind_read(&request(0..8, 120)).expect("bind"),
                true,
            );
            let mut writer = Msv4Writer::create(&out, &spec).expect("create");
            let mut cursor = v2.bind_read(&request(0..8, batch_samples)).expect("bind");
            let mut buffers = BatchBuffers::default();
            while let Some(ReadEvent::Batch(batch)) = cursor.next(&mut buffers).expect("read") {
                writer.write_batch(&batch).expect("write");
            }
            let counters = writer.counters();
            (counters, writer.finish().expect("finish"))
        } else {
            let spec = writer_spec(
                v4.catalogue(),
                &mut v4.bind_read(&request(0..8, 120)).expect("bind"),
                false,
            );
            let mut writer = Msv4Writer::create(&out, &spec).expect("create");
            let mut cursor = v4.bind_read(&request(0..8, batch_samples)).expect("bind");
            let mut buffers = BatchBuffers::default();
            while let Some(ReadEvent::Batch(batch)) = cursor.next(&mut buffers).expect("read") {
                writer.write_batch(&batch).expect("write");
            }
            let counters = writer.counters();
            (counters, writer.finish().expect("finish"))
        };
        let reopened = Msv4Observation::open(&written).expect("reopen");
        let read = samples(
            &mut reopened.bind_read(&request(0..8, 120)).expect("bind"),
            &stations(reopened.catalogue()),
        )
        .expect("read back");
        assert_eq!(read, reference, "{label}, {batch_samples} per batch");
        assert_eq!(
            reopened.catalogue().antennas[3].name,
            v4.catalogue().antennas[3].name
        );
        if label == "v4" {
            assert_eq!(
                counters.packed_elements, 0,
                "factored batches encode from their views"
            );
        } else {
            assert!(counters.packed_elements > 0);
        }
    }
}

/// Nothing is published until every time is written; a writer dropped or
/// failing leaves only its staging directory, and a time is written once.
#[test]
fn the_writer_publishes_only_complete_datasets() {
    use casa_ms::observation_io::Msv4Writer;
    let root = tempfile::tempdir().expect("root");
    let v4 = Msv4Observation::open(fixture(root.path(), "ms_trial.ps.zarr")).expect("v4");
    let spec = writer_spec(
        v4.catalogue(),
        &mut v4.bind_read(&request(0..8, 120)).expect("bind"),
        false,
    );
    let out = root.path().join("partial.ps.zarr");
    let mut writer = Msv4Writer::create(&out, &spec).expect("create");
    let mut cursor = v4.bind_read(&request(0..8, 40)).expect("bind");
    let mut buffers = BatchBuffers::default();
    let Some(ReadEvent::Batch(batch)) = cursor.next(&mut buffers).expect("read") else {
        panic!("a batch");
    };
    writer.write_batch(&batch).expect("first write");
    assert!(
        writer.write_batch(&batch).is_err(),
        "a time is written once"
    );
    assert!(!out.exists(), "nothing is published before finish");
    assert!(
        writer.finish().is_err(),
        "eight of twelve times are missing"
    );
    assert!(!out.exists(), "an incomplete dataset is never published");
    let window = AccessRequest {
        channels: 2..4,
        ..request(2..4, 40)
    };
    let mut writer =
        Msv4Writer::create(&root.path().join("window.ps.zarr"), &spec).expect("create");
    let mut cursor = v4.bind_read(&window).expect("bind");
    let Some(ReadEvent::Batch(batch)) = cursor.next(&mut buffers).expect("read") else {
        panic!("a batch");
    };
    assert!(matches!(
        writer.write_batch(&batch),
        Err(ObservationError::Unsupported { .. })
    ));
}
