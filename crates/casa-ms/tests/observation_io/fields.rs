// SPDX-License-Identifier: LGPL-3.0-or-later
//! Field selection (#932, step 2): a request names the fields it reads, and
//! both adapters deliver the whole times phased to them, decoding none of
//! the others, in batches that never span a gap. Groups keep their keys; a
//! group that combines fields is delivered in parts and marked clipped. A
//! time split and an updater take the selection as they take a time range.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use casa_ms::observation_io::convert::{ConversionOptions, LossKind, PaddingRows, to_msv4};
use casa_ms::observation_io::{Msv4Codec, OpenOptions, open, open_update};
use casa_ms::{
    SyntheticAntenna, SyntheticField, SyntheticObservationRequest, SyntheticSpectralSetup,
    generate_synthetic_observation_ms,
};
use casa_observation::{
    AccessRequest, BatchBuffers, FieldId, FieldSelection, Grouping, Observation, PartitionId,
    PatchValues, ReadEvent, TimeRange, TimeSelection, TimeSplit, UpdateTarget, WritableRole,
};
use casa_types::measures::MjdHighPrec;
use ndarray::Array3;

use super::{per_times, request, samples};

/// A simulated mosaic of three fields observed in turn, one integration
/// each, over twelve times, and its MSv4 conversion in `chunk`-time
/// chunks.
fn mosaic(root: &Path, chunk: u64) -> (PathBuf, PathBuf) {
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        root.join("mosaic.ms"),
        vec![
            SyntheticAntenna::vla("VLA01", "N01", [-1_601_185.4, -5_041_977.5, 3_554_875.9]),
            SyntheticAntenna::vla("VLA02", "N02", [-1_601_085.4, -5_041_977.5, 3_554_875.9]),
            SyntheticAntenna::vla("VLA03", "N03", [-1_601_185.4, -5_041_877.5, 3_554_875.9]),
        ],
    );
    request.predict_model = false;
    request.start_time_mjd_seconds = 59_000.25 * 86_400.0;
    request.duration_seconds = 115.0;
    request.integration_seconds = 10.0;
    request.spectral_windows = vec![SyntheticSpectralSetup {
        name: "band".to_string(),
        start_frequency_hz: 44.0e9,
        channel_width_hz: 2.0e6,
        channel_count: 4,
    }];
    request.fields = [-0.45, -0.44, -0.43]
        .iter()
        .enumerate()
        .map(|(index, dec)| SyntheticField {
            name: format!("pointing{index}"),
            phase_center_rad: [1.23, *dec],
        })
        .collect();
    generate_synthetic_observation_ms(&request).expect("simulate");
    let v4 = root.join(format!("mosaic-{chunk}.ps.zarr"));
    to_msv4(
        &request.output_ms,
        &v4,
        &ConversionOptions {
            data_groups: None,
            accept: vec![LossKind::Sigma, LossKind::Tables],
            batch_bytes: 1 << 20,
            workers: 1,
            chunks: [chunk, 0, 0, 0],
            codec: Msv4Codec::BloscLz4,
            padding: PaddingRows::Flagged,
            creator: "casa-rs".to_string(),
            creation_date: "2026-10-27T00:00:00+00:00".to_string(),
        },
    )
    .expect("convert");
    (request.output_ms, v4)
}

/// Each delivered time's field, by the bits of its MJD seconds.
fn fields_by_time(
    observation: &dyn Observation,
    request: &AccessRequest,
) -> BTreeMap<u64, FieldId> {
    let mut traversal = observation.bind_read(request).expect("bind");
    let mut buffers = BatchBuffers::default();
    let mut fields = BTreeMap::new();
    while let Some(event) = traversal.next(&mut buffers).expect("read") {
        if let ReadEvent::Batch(batch) = event {
            for time in 0..batch.times() {
                let mjd = batch.time.seconds_since(time, MjdHighPrec::new(0.0, 0.0));
                fields.insert(mjd.to_bits(), batch.field[time]);
            }
        }
    }
    fields
}

fn only(fields: &[u32]) -> FieldSelection {
    FieldSelection::Only(fields.iter().copied().map(FieldId).collect())
}

/// On both stores a field selection delivers exactly the whole
/// traversal's samples at the times phased to its fields, alone or with a
/// time range; every batch's times are selected, so none spans a gap; and
/// the estimate counts the selected times and their batches. A field the
/// partition never visits selects nothing.
#[test]
fn a_field_selection_delivers_whole_times_of_its_fields() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = mosaic(root.path(), 5);
    for path in [&v2, &v4] {
        let observation = open(path, &OpenOptions::default()).expect("open");
        let catalogue = observation.catalogue();
        assert_eq!(catalogue.fields.len(), 3, "{path:?}");
        let whole_request = per_times(&*observation, request(0..4, 0), 2);
        let whole = samples(
            &mut observation.bind_read(&whole_request).expect("bind"),
            catalogue,
        )
        .expect("whole");
        let field_of = fields_by_time(&*observation, &whole_request);
        assert_eq!(field_of.len(), 12, "{path:?}");
        let grid = observation.grid(PartitionId(0), "base").expect("grid");
        for (selection, range) in [
            (only(&[0]), None),
            (only(&[1, 2]), None),
            (only(&[2]), Some(3..10)),
            (only(&[7]), None),
            (FieldSelection::All, Some(4..9)),
        ] {
            let request = AccessRequest {
                fields: selection.clone(),
                times: range.clone().map_or(TimeSelection::All, |times| {
                    TimeSelection::Within(TimeRange::of_grid(&grid, times))
                }),
                ..whole_request.clone()
            };
            let in_range = |bits: u64| {
                let index = field_of
                    .keys()
                    .position(|key| *key == bits)
                    .expect("a time");
                range.as_ref().is_none_or(|range| range.contains(&index))
            };
            let expected: BTreeMap<_, _> = whole
                .iter()
                .filter(|(key, _)| selection.holds(field_of[&key.0]) && in_range(key.0))
                .collect();
            let mut traversal = observation.bind_read(&request).expect("bind");
            let estimate = traversal.estimate.partitions[0].clone();
            let mut buffers = BatchBuffers::default();
            let mut batches = 0;
            while let Some(event) = traversal.next(&mut buffers).expect("read") {
                if let ReadEvent::Batch(batch) = event {
                    batches += 1;
                    assert!(
                        batch.field.iter().all(|field| selection.holds(*field)),
                        "{path:?} {selection:?}: a batch spans a gap"
                    );
                }
            }
            let delivered = samples(
                &mut observation.bind_read(&request).expect("bind"),
                catalogue,
            )
            .expect("read");
            assert_eq!(
                delivered.iter().collect::<BTreeMap<_, _>>(),
                expected,
                "{path:?} {selection:?} {range:?}"
            );
            let times: BTreeSet<u64> = expected.keys().map(|key| key.0).collect();
            assert_eq!(
                (estimate.times, estimate.time_runs.len(), estimate.batches),
                (times.len(), times.len(), batches),
                "{path:?} {selection:?}"
            );
        }
    }
}

/// A field selection decodes none of the other fields' times: on MSv2 the
/// rows read are the selected times' (a row is a decode unit), and on an
/// MSv4 of one-time chunks the chunks read are theirs; both as modelled.
/// In chunks of six times a selected time still decodes its whole chunk:
/// as many units, six times the elements.
#[test]
fn a_field_selection_decodes_no_unselected_time() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = mosaic(root.path(), 1);
    let (_, coarse) = mosaic(&root.path().join("coarse"), 6);
    // Units modelled, units counted, elements modelled.
    let decoded = |path: &Path, fields: FieldSelection| {
        let observation = open(path, &OpenOptions::default()).expect("open");
        let request = AccessRequest {
            fields,
            ..per_times(&*observation, request(0..4, 0), 1)
        };
        let mut traversal = observation.bind_read(&request).expect("bind");
        let estimate = traversal.estimate.partitions[0].clone();
        let mut buffers = BatchBuffers::default();
        while traversal.next(&mut buffers).expect("read").is_some() {}
        (
            estimate.decode_units,
            traversal.counters().decode_units,
            estimate.decoded_elements,
        )
    };
    for path in [&v2, &v4] {
        let (all, _, _) = decoded(path, FieldSelection::All);
        let (one, counted, _) = decoded(path, only(&[1]));
        assert_eq!(one, counted, "{path:?}: modelled as counted");
        assert_eq!(one * 3, all, "{path:?}: a third of the units");
    }
    let (fine, _, fine_elements) = decoded(&v4, only(&[1]));
    let (one, counted, elements) = decoded(&coarse, only(&[1]));
    assert_eq!((one, counted), (fine, fine), "a unit per selected time");
    assert_eq!(elements, 6 * fine_elements, "each a six-time chunk");
}

/// A group that combines fields, held in parts across a field selection's
/// gaps, is delivered clipped, and a time split of the selection tiles it
/// and names the groups it divides, as for a time range.
#[test]
fn a_field_selection_clips_groups_that_combine_fields_and_splits_tile_it() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = mosaic(root.path(), 5);
    for path in [&v2, &v4] {
        let observation = open(path, &OpenOptions::default()).expect("open");
        let grouped = AccessRequest {
            fields: only(&[0, 2]),
            grouping: Grouping::TimeBins {
                width_s: 60.0,
                combine_fields: true,
                combine_scans: true,
            },
            ..per_times(&*observation, request(0..4, 0), 2)
        };
        let mut traversal = observation.bind_read(&grouped).expect("bind");
        let mut buffers = BatchBuffers::default();
        let mut clipped = 0;
        while let Some(event) = traversal.next(&mut buffers).expect("read") {
            if let ReadEvent::Batch(batch) = event {
                clipped += batch.fragments.iter().filter(|f| f.clipped).count();
            }
        }
        assert!(clipped > 0, "{path:?}: the combined groups are cut");
        let grid = observation.grid(PartitionId(0), "base").expect("grid");
        let cuts: Vec<TimeRange> = [0, 5, 12]
            .windows(2)
            .map(|pair| TimeRange::of_grid(&grid, pair[0]..pair[1]))
            .collect();
        let split = TimeSplit::plan(&*observation, &grouped, &cuts).expect("plan");
        let estimate = observation.estimate(&grouped).expect("estimate");
        let held: usize = split
            .slices()
            .iter()
            .map(|slice| observation.estimate(slice).expect("slice").partitions[0].times)
            .sum();
        assert_eq!(held, estimate.partitions[0].times, "{path:?}");
        assert!(!split.divided().is_empty(), "{path:?}: a bin spans the cut");
    }
}

/// On both stores an updater bound to a field selection writes the
/// selected times and no other.
#[test]
fn a_field_selected_traversal_writes_back_its_times() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = mosaic(root.path(), 5);
    for path in [&v2, &v4] {
        let flagged = |path: &Path| {
            let observation = open(path, &OpenOptions::default()).expect("open");
            let request = per_times(&*observation, request(0..4, 0), 2);
            let field_of = fields_by_time(&*observation, &request);
            let all = samples(
                &mut observation.bind_read(&request).expect("bind"),
                observation.catalogue(),
            )
            .expect("read");
            all.iter()
                .map(|(key, sample)| (field_of[&key.0], sample.flags.iter().all(|flag| *flag)))
                .collect::<Vec<_>>()
        };
        let before = flagged(path);
        assert!(before.iter().all(|(_, flagged)| !flagged), "{path:?}");
        {
            let session = open_update(
                path,
                &OpenOptions::default(),
                &[UpdateTarget::new("base", WritableRole::Flags)],
            )
            .expect("session");
            let request = AccessRequest {
                fields: only(&[1]),
                ..per_times(session.observation(), request(0..4, 0), 2)
            };
            let mut traversal = session.observation().bind_read(&request).expect("bind");
            let mut updater = session.updater(&request, &[0]).expect("updater");
            let mut buffers = BatchBuffers::default();
            while let Some(event) = traversal.next(&mut buffers).expect("read") {
                if let ReadEvent::Batch(batch) = event {
                    let set = Array3::from_elem(batch.shape(), true);
                    updater
                        .write(
                            &batch,
                            &[PatchValues::Flags {
                                set: Some(set.view()),
                                clear: None,
                            }],
                        )
                        .expect("write");
                }
            }
            updater.finish().expect("finish");
        }
        for (field, flagged) in flagged(path) {
            assert_eq!(flagged, field == FieldId(1), "{path:?} {field:?}");
        }
    }
}
