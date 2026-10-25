// SPDX-License-Identifier: LGPL-3.0-or-later
//! The trial MSv4 writer: round trips, the declaration it checks every
//! batch against, and its creation guarantee.

use std::path::{Path, PathBuf};

use casa_ms::observation_io::{Msv4Codec, Msv4Observation, Msv4StorageOptions, Msv4Writer};
use casa_observation::{
    BatchArrays, BatchBuffers, BatchHeader, Catalogue, Correlation, FieldId, Observation,
    ObservationError, PartitionId, ReadEvent, Sharing, TimeBaselineGrid, VisibilityBatch,
    VisibilityUnits,
};

use super::{open_both, per_times, request, samples};

/// What a converter declares for `observation`: its catalogue, with
/// XRADIO's naming applied when `rename` (antennas `NAME_STATION`, fields
/// `NAME_ID`), and the grid of a full traversal.
pub(crate) fn declaration(
    observation: &dyn Observation,
    rename: bool,
) -> (Catalogue, TimeBaselineGrid) {
    let mut catalogue = observation.catalogue().as_ref().clone();
    if rename {
        for antenna in &mut catalogue.antennas {
            antenna.name = format!("{}_{}", antenna.name, antenna.station);
        }
        for (index, field) in catalogue.fields.iter_mut().enumerate() {
            field.name = format!("{}_{index}", field.name);
        }
    }
    let grid = observation.grid(PartitionId(0), "base").expect("grid");
    (catalogue, grid)
}

pub(crate) fn storage() -> Msv4StorageOptions {
    Msv4StorageOptions {
        dataset_name: "ms_trial_0".to_string(),
        creator: "casa-rs".to_string(),
        creation_date: "2026-10-24T00:00:00+00:00".to_string(),
        chunks: [5, 10, 4, 4],
        codec: Msv4Codec::BloscLz4,
    }
}

fn create(out: &Path, observation: &dyn Observation, rename: bool) -> Msv4Writer {
    let (catalogue, grid) = declaration(observation, rename);
    Msv4Writer::create(out, &catalogue, PartitionId(0), "base", &grid, &storage()).expect("create")
}

/// Write every batch of `observation` at `times` whole times per batch.
fn write_all(writer: &mut Msv4Writer, observation: &dyn Observation, times: usize) {
    let mut traversal = observation
        .bind_read(&per_times(observation, request(0..8, 0), times))
        .expect("bind");
    let mut buffers = BatchBuffers::default();
    while let Some(ReadEvent::Batch(batch)) = traversal.next(&mut buffers).expect("read") {
        writer.write_batch(&batch).expect("write");
    }
}

/// The trial writer creates a fresh MSv4 from either store's batches that
/// reads back, through the trial reader, sample for sample equal to the
/// MSv2, time auxiliaries and scans included. Both stores deliver the grid
/// the dataset holds, so visibilities and flags are encoded straight from
/// their views; the MSv2 weight shared by every channel is written for each
/// channel (the one packing left), with batches that straddle the five-time
/// chunks. XRADIO's own check of such a store is
/// `tools/ms-trial/check_interop.sh`.
#[test]
fn rust_written_msv4_reads_back_equal_to_the_msv2() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    let reference = samples(
        &mut v2.bind_read(&request(0..8, usize::MAX)).expect("bind"),
        v2.catalogue(),
    )
    .expect("v2");
    for (label, times) in [("v2", 4), ("v2", 12), ("v4", 4), ("v4", 12)] {
        let out = root.path().join(format!("{label}-{times}.ps.zarr"));
        let observation: &dyn Observation = if label == "v2" { &v2 } else { &v4 };
        let mut writer = create(&out, observation, label == "v2");
        write_all(&mut writer, observation, times);
        let counters = writer.counters();
        assert_eq!(writer.chunk_times(), 5);
        assert_eq!(
            counters.partial_chunks,
            if times == 4 { 6 * (1 + 2 + 1) } else { 0 },
            "{label}: four-time batches leave chunks 0..5 and 5..10 partly covered; \
             twelve times cover every chunk whole"
        );
        let written = writer.finish().expect("finish");
        let reopened = Msv4Observation::open(&written).expect("reopen");
        let read = samples(
            &mut reopened
                .bind_read(&request(0..8, usize::MAX))
                .expect("bind"),
            reopened.catalogue(),
        )
        .expect("read back");
        assert_eq!(read, reference, "{label}, {times} times per batch");
        assert_eq!(
            reopened.catalogue().antennas[3].name,
            v4.catalogue().antennas[3].name
        );
        assert_eq!(reopened.catalogue().scans, v4.catalogue().scans);
        assert_eq!(
            reopened.catalogue().partitions[0].intents,
            v4.catalogue().partitions[0].intents
        );
        if label == "v4" {
            assert_eq!(
                counters.packed_elements, 0,
                "MSv4 batches encode from their views"
            );
        } else {
            assert_eq!(
                counters.packed_elements,
                120 * 8 * 4 + 2 * 120,
                "the per-correlation weight written per channel, and the time centroids \
                 and exposures re-expressed from MJD 0 to the Unix epoch"
            );
        }
    }
}

/// A partition with padding (an MSv2 row removed: a baseline dropped at one
/// time) is declared from the index and written: the padded cell has no
/// acquisition, so no interval to compare with the dataset's. It reads back
/// flagged, with NaN values and time auxiliaries, and every acquisition
/// reads back equal to the MSv2's.
#[test]
fn a_padded_partition_is_declared_and_written() {
    let root = tempfile::tempdir().expect("root");
    let path = super::fixture(root.path(), "ms_trial.ms");
    let mut ms = casa_ms::MeasurementSet::open(&path).expect("ms");
    ms.main_table_mut().remove_rows(&[7]).expect("remove row 7");
    ms.save().expect("save");
    drop(ms);
    let sparse = casa_ms::observation_io::Msv2Observation::open(&path).expect("padded");
    let reference = samples(
        &mut sparse.bind_read(&request(0..8, usize::MAX)).expect("bind"),
        sparse.catalogue(),
    )
    .expect("v2");
    let out = root.path().join("padded.ps.zarr");
    let mut writer = create(&out, &sparse, true);
    write_all(&mut writer, &sparse, 4);
    let reopened = Msv4Observation::open(writer.finish().expect("finish")).expect("reopen");
    let read = samples(
        &mut reopened
            .bind_read(&request(0..8, usize::MAX))
            .expect("bind"),
        reopened.catalogue(),
    )
    .expect("read back");
    assert_eq!(read.len(), reference.len(), "every cell of the grid");
    let mut padded = 0;
    for (key, expected) in &reference {
        let back = &read[key];
        if expected.uvw.iter().all(|metres| metres.is_nan()) {
            padded += 1;
            assert!(back.flags.iter().all(|flag| *flag), "padding is flagged");
            assert!(back.values.iter().all(|value| value.re.is_nan()));
            assert!(back.uvw.iter().all(|metres| metres.is_nan()));
            assert!(back.centroid.is_nan() && back.exposure.is_nan());
        } else {
            assert_eq!(back, expected, "{key:?}");
        }
    }
    assert_eq!(padded, 1);
}

/// Staging directories beside `out`.
fn staging(out: &Path) -> Vec<PathBuf> {
    let name = out
        .file_name()
        .expect("name")
        .to_string_lossy()
        .into_owned();
    std::fs::read_dir(out.parent().expect("parent"))
        .expect("list")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name().is_some_and(|file| {
                file.to_string_lossy()
                    .starts_with(&format!(".{name}.staging"))
            })
        })
        .collect()
}

/// Nothing is published until every time is written, a time is written
/// once, a channel window is refused, and a writer dropped or failing
/// removes its staging directory.
#[test]
fn the_writer_publishes_only_complete_datasets() {
    let root = tempfile::tempdir().expect("root");
    let (_, v4) = open_both(root.path());
    let out = root.path().join("partial.ps.zarr");
    let mut writer = create(&out, &v4, false);
    assert_eq!(staging(&out).len(), 1, "one staging directory, of its own");
    let four = per_times(&v4, request(0..8, 0), 4);
    let mut traversal = v4.bind_read(&four).expect("bind");
    let mut buffers = BatchBuffers::default();
    let Some(ReadEvent::Batch(batch)) = traversal.next(&mut buffers).expect("read") else {
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
    assert!(
        staging(&out).is_empty(),
        "a failed finish removes its staging"
    );
    let window = root.path().join("window.ps.zarr");
    let mut writer = create(&window, &v4, false);
    let mut traversal = v4
        .bind_read(&per_times(&v4, request(2..4, 0), 4))
        .expect("bind");
    let Some(ReadEvent::Batch(batch)) = traversal.next(&mut buffers).expect("read") else {
        panic!("a batch");
    };
    assert!(matches!(
        writer.write_batch(&batch),
        Err(ObservationError::Unsupported { .. })
    ));
    drop(writer);
    assert!(
        staging(&window).is_empty(),
        "a dropped writer removes its staging"
    );
}

/// `batch` copied, with its header and arrays edited: a batch of the same
/// shape whose coordinates differ, built through the same checked fill as
/// any other.
fn edited(
    batch: &VisibilityBatch<'_>,
    edit: impl FnOnce(&mut BatchHeader, &mut BatchArrays),
) -> BatchBuffers {
    let mut packed = BatchBuffers::default();
    packed.pack(batch).expect("pack");
    let mut header = packed.header().expect("packed").clone();
    let mut arrays = packed.arrays().clone();
    edit(&mut header, &mut arrays);
    let mut buffers = BatchBuffers::default();
    let mut fill = buffers
        .begin(header, batch.catalogue)
        .expect("an edited header still fits the catalogue");
    *fill.arrays() = arrays;
    fill.finish().expect("an edited batch is still well formed");
    buffers
}

/// Every batch must match the declaration in value, not just in shape.
/// What the catalogue states (frequencies, correlations, units) a batch
/// cannot disagree with on its own any more, so the declaration itself is
/// edited: reversed correlation products, other units, a grid naming a
/// field the catalogue lacks. What the batch states is edited in a copy: a
/// time the dataset lacks, another interval at a time. Each is refused
/// before anything is written, from either store.
#[test]
fn the_writer_refuses_batches_that_disagree_with_the_declaration() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    type Edit = fn(&mut BatchHeader, &mut BatchArrays);
    let cases: [(&str, Edit); 2] = [
        ("a time the dataset lacks", |_, arrays| {
            arrays.time_offsets_s[0] += 1.0
        }),
        (
            "another interval at a time",
            |header, arrays| match header.interval {
                Sharing::Constant(ref mut interval) => *interval += 1.0,
                Sharing::PerSample | Sharing::PerTime => arrays.interval_s[0] += 1.0,
            },
        ),
    ];
    for (store, observation) in [("v2", &v2 as &dyn Observation), ("v4", &v4)] {
        for (label, edit) in cases {
            let out = root
                .path()
                .join(format!("{store}-{}.ps.zarr", label.replace(' ', "-")));
            let mut writer = create(&out, observation, store == "v2");
            let mut traversal = observation
                .bind_read(&per_times(observation, request(0..8, 0), 4))
                .expect("bind");
            let mut buffers = BatchBuffers::default();
            let Some(ReadEvent::Batch(batch)) = traversal.next(&mut buffers).expect("read") else {
                panic!("a batch");
            };
            let changed = edited(&batch, edit);
            let result = writer.write_batch(&changed.batch().expect("batch"));
            assert!(result.is_err(), "{store} {label}: refused");
            assert_eq!(
                writer.counters().region_writes,
                0,
                "{store} {label}: nothing written"
            );
            writer
                .write_batch(&batch)
                .expect("the unedited batch is accepted");
        }
    }
    // The declared correlations and units are values too: the same batches
    // against a declaration of reversed correlations, or of janskys, are
    // refused.
    for (label, edit) in [
        (
            "reversed correlations",
            (|catalogue: &mut Catalogue| {
                catalogue.partitions[0].correlations.reverse();
                assert_eq!(catalogue.partitions[0].correlations[0], Correlation::YY);
            }) as fn(&mut Catalogue),
        ),
        ("other units", |catalogue: &mut Catalogue| {
            catalogue.partitions[0].data_groups[0].units = VisibilityUnits::Jansky;
        }),
    ] {
        let (mut catalogue, grid) = declaration(&v4, false);
        edit(&mut catalogue);
        let mut writer = Msv4Writer::create(
            &root
                .path()
                .join(format!("declared-{}.ps.zarr", label.replace(' ', "-"))),
            &catalogue,
            PartitionId(0),
            "base",
            &grid,
            &storage(),
        )
        .expect("create");
        let mut traversal = v4
            .bind_read(&per_times(&v4, request(0..8, 0), 4))
            .expect("bind");
        let mut buffers = BatchBuffers::default();
        let Some(ReadEvent::Batch(batch)) = traversal.next(&mut buffers).expect("read") else {
            panic!("a batch");
        };
        assert!(writer.write_batch(&batch).is_err(), "{label}");
        assert_eq!(writer.counters().region_writes, 0, "{label}");
    }
    let (catalogue, mut grid) = declaration(&v4, false);
    grid.field_per_time[0] = FieldId(7);
    assert!(
        Msv4Writer::create(
            &root.path().join("unknown-field.ps.zarr"),
            &catalogue,
            PartitionId(0),
            "base",
            &grid,
            &storage(),
        )
        .is_err(),
        "a grid naming a field the catalogue lacks"
    );
}

/// Two live writers for one destination stage apart: the first to finish
/// publishes, the second is refused rather than replacing it, and neither
/// sees the other's bytes. A stale staging directory left by an earlier
/// crash, holding a different dataset, is never adopted.
#[test]
fn writers_stage_apart_and_never_replace_a_published_dataset() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open_both(root.path());
    let out = root.path().join("shared.ps.zarr");
    // An earlier crash: staging directories with another dataset inside,
    // under the old shared name and under this writer's naming scheme.
    let stale = [
        root.path()
            .join(format!("shared.staging-{}", std::process::id())),
        root.path().join(format!(
            ".shared.ps.zarr.staging-{}-0-0",
            std::process::id()
        )),
    ];
    for directory in &stale {
        std::fs::create_dir(directory).expect("stale");
        std::fs::write(directory.join("zarr.json"), "{\"stale\": true}").expect("stale file");
    }
    let mut first = create(&out, &v2, true);
    let mut second = create(&out, &v4, false);
    write_all(&mut first, &v2, 4);
    write_all(&mut second, &v4, 12);
    first.finish().expect("the first finish publishes");
    let refused = second.finish();
    assert!(refused.is_err(), "the second finish does not replace it");
    let published = Msv4Observation::open(&out).expect("published");
    assert_eq!(
        published.catalogue().antennas[0].name,
        format!(
            "{}_{}",
            v2.catalogue().antennas[0].name,
            v2.catalogue().antennas[0].station
        ),
        "the published dataset is the first writer's (renamed antennas)"
    );
    let reference = samples(
        &mut v2.bind_read(&request(0..8, usize::MAX)).expect("bind"),
        v2.catalogue(),
    )
    .expect("v2");
    let read = samples(
        &mut published
            .bind_read(&request(0..8, usize::MAX))
            .expect("bind"),
        published.catalogue(),
    )
    .expect("read");
    assert_eq!(read, reference);
    for directory in &stale {
        assert_eq!(
            std::fs::read_to_string(directory.join("zarr.json")).expect("stale kept"),
            "{\"stale\": true}",
            "a stale staging directory is left as it was"
        );
    }
}
