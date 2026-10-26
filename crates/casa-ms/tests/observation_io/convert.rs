// SPDX-License-Identifier: LGPL-3.0-or-later
//! The converter: either store into a new MSv4 or MSv2 through the
//! boundary, every acquisition and selected data group carried, padding
//! kept as padding, every loss refused unless accepted.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_ms::observation_io::convert::{
    ConversionOptions, Loss, LossKind, PaddingRows, to_msv2, to_msv4,
};
use casa_ms::observation_io::{Msv4Codec, OpenOptions, open, open_update};
use casa_ms::{
    SyntheticAntenna, SyntheticObservationRequest, SyntheticSpectralSetup,
    generate_synthetic_observation_ms,
};
use casa_observation::{
    AccessRequest, BatchBuffers, CorrelationRequest, Grouping, ObservationError, PartitionId,
    PatchValues, ReadEvent, Roles, UpdateTarget, WeightChannels, WritableRole,
};
use num_complex::Complex32;

use super::{Key, Sample, fixture, samples};

fn options(accept: &[LossKind]) -> ConversionOptions {
    ConversionOptions {
        data_groups: None,
        accept: accept.to_vec(),
        batch_bytes: 1 << 20,
        workers: 2,
        chunks: [5, 0, 0, 0],
        codec: Msv4Codec::BloscLz4,
        padding: PaddingRows::Flagged,
        creator: "casa-rs".to_string(),
        creation_date: "2026-10-26T00:00:00+00:00".to_string(),
    }
}

/// Every sample of `group` in partition `id` of the store at `path`.
fn read(path: &Path, id: PartitionId, group: &str) -> BTreeMap<Key, Sample> {
    let observation = open(path, &OpenOptions::default()).expect("open");
    let channels = observation.catalogue().partition(id).frequencies_hz.len();
    let request = AccessRequest {
        partitions: vec![id],
        data_group: group.to_string(),
        roles: Roles::ALL,
        channels: 0..channels,
        correlations: CorrelationRequest::ALL,
        grouping: Grouping::None,
        batch_bytes: usize::MAX,
    };
    samples(
        &mut observation.bind_read(&request).expect("bind"),
        observation.catalogue(),
    )
    .expect("read")
}

/// The MSv2 fixture converts to an MSv4 holding every sample as it is,
/// with XRADIO's names: the same store, sample for sample, as XRADIO's own
/// conversion of it. It loses SIGMA, which the conversion refuses until
/// that loss is accepted.
#[test]
fn an_msv2_converts_to_the_msv4_xradio_makes_of_it() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let xradio = fixture(root.path(), "ms_trial.ps.zarr");
    let out = root.path().join("converted.ps.zarr");
    let Err(ObservationError::InvalidRequest { reason }) = to_msv4(&v2, &out, &options(&[])) else {
        panic!("an unaccepted loss refuses the conversion");
    };
    assert!(reason.contains("SIGMA"), "{reason}");
    assert!(!out.exists(), "nothing is written before the refusal");
    let report =
        to_msv4(&v2, &out, &options(&[LossKind::Sigma, LossKind::Tables])).expect("convert");
    assert_eq!(
        report.losses,
        [
            Loss::Sigma,
            Loss::Table("HISTORY".to_string()),
            Loss::Table("POINTING".to_string())
        ],
        "MSv4 holds no sigma; the writer writes no history or pointing"
    );
    assert_eq!((report.partitions, report.samples), (1, 120));
    let converted = read(&out, PartitionId(0), "base");
    assert_eq!(
        converted,
        read(&v2, PartitionId(0), "base"),
        "the MSv2's samples"
    );
    assert_eq!(
        converted,
        read(&xradio, PartitionId(0), "base"),
        "XRADIO's samples"
    );
    let ours = open(&out, &OpenOptions::default()).expect("open");
    let theirs = open(&xradio, &OpenOptions::default()).expect("open");
    let names = |catalogue: &casa_observation::Catalogue| {
        (
            catalogue
                .antennas
                .iter()
                .map(|a| a.name.clone())
                .collect::<Vec<_>>(),
            catalogue
                .fields
                .iter()
                .map(|f| f.name.clone())
                .collect::<Vec<_>>(),
            catalogue.partitions[0].spectral_window.clone(),
        )
    };
    assert_eq!(
        names(ours.catalogue()),
        names(theirs.catalogue()),
        "XRADIO's names"
    );
}

fn antennas() -> Vec<SyntheticAntenna> {
    vec![
        SyntheticAntenna::vla("VLA01", "N01", [-1_601_185.4, -5_041_977.5, 3_554_875.9]),
        SyntheticAntenna::vla("VLA02", "N02", [-1_601_085.4, -5_041_977.5, 3_554_875.9]),
        SyntheticAntenna::vla("VLA03", "N03", [-1_601_185.4, -5_041_877.5, 3_554_875.9]),
    ]
}

/// A simulated MSv2 of two spectral windows of four and three channels
/// (two partitions of different shapes), with `DATA` and a `MODEL_DATA`
/// written through an update session, each value distinct.
fn two_windows(root: &Path) -> PathBuf {
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        root.join("two_windows.ms"),
        antennas(),
    );
    request.predict_model = false;
    request.phase_center_rad = [1.23, -0.45];
    request.start_time_mjd_seconds = 59_000.25 * 86_400.0;
    request.duration_seconds = 65.0;
    request.integration_seconds = 10.0;
    request.spectral_windows = vec![
        SyntheticSpectralSetup {
            name: "low".to_string(),
            start_frequency_hz: 672.0e9,
            channel_width_hz: 2.0e6,
            channel_count: 4,
        },
        SyntheticSpectralSetup {
            name: "high".to_string(),
            start_frequency_hz: 676.0e9,
            channel_width_hz: -1.0e6,
            channel_count: 3,
        },
    ];
    generate_synthetic_observation_ms(&request).expect("simulate");
    let path = request.output_ms;
    let targets = [
        UpdateTarget::new("base", WritableRole::Visibilities),
        UpdateTarget {
            create: true,
            ..UpdateTarget::new("model", WritableRole::Visibilities)
        },
    ];
    let session = open_update(&path, &OpenOptions::default(), &targets).expect("session");
    let catalogue = session.observation().catalogue().clone();
    for id in catalogue.partition_ids() {
        let request = AccessRequest {
            partitions: vec![id],
            data_group: "base".to_string(),
            roles: Roles::ALL,
            channels: 0..catalogue.partition(id).frequencies_hz.len(),
            correlations: CorrelationRequest::ALL,
            grouping: Grouping::None,
            batch_bytes: usize::MAX,
        };
        let mut traversal = session.observation().bind_read(&request).expect("bind");
        let mut updater = session.updater(&request, &[0, 1]).expect("updater");
        let mut buffers = casa_observation::BatchBuffers::default();
        while let Some(casa_observation::ReadEvent::Batch(batch)) =
            traversal.next(&mut buffers).expect("read")
        {
            let value = |offset: f32| {
                ndarray::Array3::from_shape_fn(batch.shape(), |(s, c, p)| {
                    Complex32::new(
                        offset + (id.0 * 1000) as f32 + s as f32,
                        (c * 10 + p) as f32,
                    )
                })
            };
            let (data, model) = (value(0.0), value(0.5));
            updater
                .write(
                    &batch,
                    &[
                        casa_observation::PatchValues::Visibilities(data.view()),
                        casa_observation::PatchValues::Visibilities(model.view()),
                    ],
                )
                .expect("write");
        }
        updater.finish().expect("finish");
    }
    path
}

/// Two partitions of different shapes and two data groups convert into
/// two datasets, each holding `VISIBILITY` and `VISIBILITY_MODEL` beside
/// one `FLAG`, `WEIGHT` and `UVW`: every group of every partition reads
/// back equal to the MSv2's. Batches of whole five-time chunks hold
/// nothing.
#[test]
fn partitions_and_data_groups_convert_together() {
    let root = tempfile::tempdir().expect("root");
    let v2 = two_windows(root.path());
    let out = root.path().join("two_windows.ps.zarr");
    let accept = [LossKind::Sigma, LossKind::Tables];
    let report = to_msv4(&v2, &out, &options(&accept)).expect("convert");
    assert_eq!(report.partitions, 2);
    assert_eq!(
        report.held_elements, 0,
        "five-time batches of five-time chunks"
    );
    assert!(
        out.join("two_windows_0/VISIBILITY_MODEL/zarr.json")
            .is_file()
    );
    assert!(
        out.join("two_windows_1/VISIBILITY_MODEL/zarr.json")
            .is_file()
    );
    let converted = open(&out, &OpenOptions::default()).expect("open");
    assert_eq!(converted.catalogue().partitions.len(), 2);
    for id in [PartitionId(0), PartitionId(1)] {
        for group in ["base", "model"] {
            assert_eq!(
                read(&out, id, group),
                read(&v2, id, group),
                "{id:?} {group}"
            );
        }
    }
    let only_base = ConversionOptions {
        data_groups: Some(vec!["base".to_string()]),
        ..options(&accept)
    };
    let refused = to_msv4(&v2, &root.path().join("base_only.ps.zarr"), &only_base);
    assert!(
        matches!(refused, Err(ObservationError::InvalidRequest { ref reason }) if reason.contains("model")),
        "leaving a group behind is a loss: {refused:?}"
    );
}

/// Every acquisition of `group` in partition `id`: the samples that are not
/// padding (a NaN weight).
fn acquisitions(path: &Path, id: PartitionId, group: &str) -> BTreeMap<Key, Sample> {
    read(path, id, group)
        .into_iter()
        .filter(|(_, sample)| !sample.weights.iter().all(|weight| weight.is_nan()))
        .collect()
}

/// MAIN's row count and `SIGMA`, `WEIGHT` of the MSv2 at `path`.
fn rows_and_sigmas(path: &Path) -> (usize, Vec<(f32, f32)>) {
    let ms = casa_ms::MeasurementSet::open(path).expect("ms");
    let main = ms.main_table();
    let column = |name: &str| {
        main.column_accessor(name)
            .and_then(|column| column.array_cells_owned(&(0..main.row_count()).collect::<Vec<_>>()))
            .expect(name)
    };
    let pairs = column("WEIGHT")
        .into_iter()
        .zip(column("SIGMA"))
        .flat_map(|(weight, sigma)| {
            let (
                Some(casa_types::ArrayValue::Float32(weight)),
                Some(casa_types::ArrayValue::Float32(sigma)),
            ) = (weight, sigma)
            else {
                panic!("float WEIGHT and SIGMA cells");
            };
            weight.into_iter().zip(sigma).collect::<Vec<_>>()
        })
        .collect();
    (main.row_count(), pairs)
}

/// XRADIO's MSv4 of the fixture converts to an MSv2 holding every
/// acquisition as it is, with SIGMA derived as CASA derives it from the
/// weight; that derivation is refused until accepted.
#[test]
fn an_msv4_converts_to_an_msv2_of_every_acquisition() {
    let root = tempfile::tempdir().expect("root");
    let xradio = fixture(root.path(), "ms_trial.ps.zarr");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let out = root.path().join("converted.ms");
    let refused = to_msv2(&xradio, &out, &options(&[LossKind::Tables]));
    assert!(
        matches!(refused, Err(ObservationError::InvalidRequest { ref reason }) if reason.contains("SIGMA")),
        "{refused:?}"
    );
    assert!(!out.exists(), "nothing is written before the refusal");
    let report = to_msv2(
        &xradio,
        &out,
        &options(&[LossKind::Sigma, LossKind::Tables, LossKind::Columns]),
    )
    .expect("convert");
    assert!(
        report.losses.contains(&Loss::SigmaDerived),
        "{:?}",
        report.losses
    );
    assert_eq!(
        (report.partitions, report.samples, report.rows),
        (1, 120, 120)
    );
    let converted = read(&out, PartitionId(0), "base");
    assert_eq!(
        converted,
        read(&xradio, PartitionId(0), "base"),
        "XRADIO's samples"
    );
    assert_eq!(
        converted,
        read(&v2, PartitionId(0), "base"),
        "the MSv2's samples"
    );
    let (rows, pairs) = rows_and_sigmas(&out);
    assert_eq!(rows, 120);
    for (weight, sigma) in pairs {
        assert_eq!(
            sigma,
            if weight > 0.0 {
                1.0 / weight.sqrt()
            } else {
                -1.0
            }
        );
    }
    let ours = open(&out, &OpenOptions::default()).expect("open");
    let theirs = open(&xradio, &OpenOptions::default()).expect("open");
    let (ours, theirs) = (ours.catalogue(), theirs.catalogue());
    // Measures compare through their exact debug form.
    assert_eq!(
        format!("{:?}", ours.antennas),
        format!("{:?}", theirs.antennas),
        "antennas"
    );
    assert_eq!(
        format!("{:?}", ours.fields),
        format!("{:?}", theirs.fields),
        "fields"
    );
    assert_eq!(ours.scans, theirs.scans, "scans");
    assert_eq!(ours.observations, theirs.observations, "observations");
    let (a, b) = (&ours.partitions[0], &theirs.partitions[0]);
    assert_eq!(
        (
            &a.spectral_window,
            &a.frequencies_hz,
            &a.channel_widths_hz,
            a.frequency_frame
        ),
        (
            &b.spectral_window,
            &b.frequencies_hz,
            &b.channel_widths_hz,
            b.frequency_frame
        ),
        "spectral window"
    );
    assert_eq!((&a.correlations, &a.intents), (&b.correlations, &b.intents));
}

/// Two partitions of different shapes and two data groups go to an MSv4
/// and back: the MSv2 holds the original's rows, `DATA` and `MODEL_DATA`
/// of every partition reading back equal.
#[test]
fn an_msv2_survives_the_round_trip_through_msv4() {
    let root = tempfile::tempdir().expect("root");
    let v2 = two_windows(root.path());
    let v4 = root.path().join("two_windows.ps.zarr");
    let back = root.path().join("back.ms");
    let accept = [LossKind::Sigma, LossKind::Tables];
    to_msv4(&v2, &v4, &options(&accept)).expect("to MSv4");
    let report = to_msv2(&v4, &back, &options(&accept)).expect("to MSv2");
    let original = casa_ms::MeasurementSet::open(&v2)
        .expect("ms")
        .main_table()
        .row_count();
    assert_eq!((report.partitions, report.rows), (2, original));
    for id in [PartitionId(0), PartitionId(1)] {
        for group in ["base", "model"] {
            assert_eq!(
                read(&back, id, group),
                read(&v2, id, group),
                "{id:?} {group}"
            );
        }
    }
    // The MSv4 holds XRADIO's names, and the MSv2 made from it says them:
    // the round trip renames as XRADIO's conversion does.
    let names = |path: &Path, id, column: &str| -> Vec<String> {
        let ms = casa_ms::MeasurementSet::open(path).expect("ms");
        let table = ms.subtable(id).expect("subtable");
        (0..table.row_count())
            .map(|row| {
                match table
                    .cell_accessor(row, column)
                    .and_then(|cell| cell.scalar())
                {
                    Ok(casa_types::ScalarValue::String(name)) => name.clone(),
                    other => panic!("{column} of row {row}: {other:?}"),
                }
            })
            .collect()
    };
    let suffixed = |names: Vec<String>, suffixes: Vec<String>| -> Vec<String> {
        names
            .iter()
            .zip(suffixes)
            .map(|(name, suffix)| format!("{name}_{suffix}"))
            .collect()
    };
    let rows = |count: usize| (0..count).map(|row| row.to_string()).collect();
    use casa_ms::SubtableId::{Antenna, Field};
    let fields = names(&v2, Field, "NAME");
    assert_eq!(
        names(&back, Field, "NAME"),
        suffixed(fields.clone(), rows(fields.len()))
    );
    assert_eq!(
        names(&back, Antenna, "NAME"),
        suffixed(names(&v2, Antenna, "NAME"), names(&v2, Antenna, "STATION"))
    );
}

/// A grid cell no acquisition filled is padding in the MSv4. In the MSv2
/// made from it, it is by default a flagged row of NaN data and weights
/// (revision 2 design, D23), with valid keys, which reads back as padding
/// and converts back to padding; declaring that the conversion reproduces
/// the original sparse row set makes it no row.
#[test]
fn padding_becomes_a_flagged_row_or_none_as_declared() {
    let root = tempfile::tempdir().expect("root");
    let sparse = fixture(root.path(), "ms_trial.ms");
    let mut ms = casa_ms::MeasurementSet::open(&sparse).expect("ms");
    ms.main_table_mut().remove_rows(&[7]).expect("remove row 7");
    ms.save().expect("save");
    drop(ms);
    let accept = [LossKind::Sigma, LossKind::Tables];
    let v4 = root.path().join("sparse.ps.zarr");
    to_msv4(&sparse, &v4, &options(&accept)).expect("to MSv4");
    let padded = |path: &Path| {
        read(path, PartitionId(0), "base")
            .into_iter()
            .filter(|(_, sample)| sample.weights.iter().all(|weight| weight.is_nan()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        padded(&v4).len(),
        1,
        "the missing row is padding in the MSv4"
    );
    let expected = acquisitions(&sparse, PartitionId(0), "base");

    let flagged = root.path().join("flagged.ms");
    let report = to_msv2(&v4, &flagged, &options(&accept)).expect("flagged");
    assert_eq!((report.samples, report.rows), (120, 120));
    assert_eq!(rows_and_sigmas(&flagged).0, 120);
    let cells = padded(&flagged);
    assert_eq!(cells.len(), 1, "the padded row reads back as padding");
    let (key, cell) = &cells[0];
    assert!(cell.flags.iter().all(|flag| *flag), "flagged throughout");
    assert!(
        cell.values
            .iter()
            .all(|value| value.re.is_nan() && value.im.is_nan())
    );
    assert_eq!(
        (cell.centroid, cell.interval, cell.exposure),
        (f64::from_bits(key.0), 10.0, 0.0),
        "valid keys: centred on its time, the grid's interval, nothing integrated"
    );
    assert_eq!(acquisitions(&flagged, PartitionId(0), "base"), expected);
    let again = root.path().join("again.ps.zarr");
    to_msv4(&flagged, &again, &options(&accept)).expect("back to MSv4");
    assert_eq!(padded(&again).len(), 1, "still padding");
    assert_eq!(acquisitions(&again, PartitionId(0), "base"), expected);

    let omitted = root.path().join("omitted.ms");
    let declared = ConversionOptions {
        padding: PaddingRows::Omitted,
        ..options(&accept)
    };
    let report = to_msv2(&v4, &omitted, &declared).expect("omitted");
    assert_eq!((report.samples, report.rows), (120, 119));
    assert_eq!(rows_and_sigmas(&omitted).0, 119, "the original's rows");
    assert_eq!(acquisitions(&omitted, PartitionId(0), "base"), expected);
}

/// Weights that vary over channels have no MSv2 `WEIGHT`: the conversion
/// refuses them before creating anything.
#[test]
fn weights_varying_over_channels_are_refused() {
    let root = tempfile::tempdir().expect("root");
    let v4 = fixture(root.path(), "ms_trial.ps.zarr");
    let session = open_update(
        &v4,
        &OpenOptions::default(),
        &[UpdateTarget::new("base", WritableRole::Weights)],
    )
    .expect("session");
    let request = AccessRequest {
        partitions: vec![PartitionId(0)],
        data_group: "base".to_string(),
        roles: Roles::ALL,
        channels: 0..session
            .observation()
            .catalogue()
            .partition(PartitionId(0))
            .frequencies_hz
            .len(),
        correlations: CorrelationRequest::ALL,
        grouping: Grouping::None,
        batch_bytes: usize::MAX,
    };
    let mut traversal = session.observation().bind_read(&request).expect("bind");
    let mut updater = session.updater(&request, &[0]).expect("updater");
    let mut buffers = BatchBuffers::default();
    while let Some(ReadEvent::Batch(batch)) = traversal.next(&mut buffers).expect("read") {
        let weights = ndarray::Array3::from_shape_fn(batch.shape(), |(_, c, _)| 1.0 + c as f32);
        updater
            .write(
                &batch,
                &[PatchValues::Weights {
                    values: weights.view(),
                    channels: WeightChannels::Delivered,
                }],
            )
            .expect("write");
    }
    updater.finish().expect("finish");
    drop(traversal);
    drop(session);
    let out = root.path().join("spectral.ms");
    let refused = to_msv2(
        &v4,
        &out,
        &options(&[LossKind::Sigma, LossKind::Tables, LossKind::Columns]),
    );
    assert!(
        matches!(refused, Err(ObservationError::Unsupported { what }) if what.contains("WEIGHT_SPECTRUM")),
        "{refused:?}"
    );
    assert!(!out.exists());
}

/// The fixture MSv2 with half its times in a second observation (session):
/// the later half, or with `early` the earlier, so that its partition's
/// first row comes before the first session's. Two partitions of one
/// spectral window, data description and state.
fn two_sessions(root: &Path, early: bool) -> PathBuf {
    let path = fixture(root, "ms_trial.ms");
    let mut ms = casa_ms::MeasurementSet::open(&path).expect("ms");
    let rows = ms.main_table().row_count();
    let times: Vec<f64> = (0..rows)
        .map(|row| {
            match ms
                .main_table()
                .cell_accessor(row, "TIME")
                .and_then(|cell| cell.scalar())
            {
                Ok(casa_types::ScalarValue::Float64(time)) => *time,
                other => panic!("TIME of row {row}: {other:?}"),
            }
        })
        .collect();
    let middle = 0.5
        * (times.iter().copied().fold(f64::INFINITY, f64::min)
            + times.iter().copied().fold(f64::NEG_INFINITY, f64::max));
    for (row, time) in times.iter().enumerate() {
        if (*time > middle) != early {
            ms.main_table_mut()
                .cell_accessor_mut(row, "OBSERVATION_ID")
                .and_then(|mut cell| {
                    cell.set(casa_types::Value::Scalar(casa_types::ScalarValue::Int32(1)))
                })
                .expect("second session");
        }
    }
    let string_array = |values: &[&str]| {
        casa_types::Value::Array(casa_types::ArrayValue::String(
            ndarray::ArrayD::from_shape_vec(
                vec![values.len()],
                values.iter().map(ToString::to_string).collect(),
            )
            .expect("shape"),
        ))
    };
    let scalar = |value: casa_types::ScalarValue| casa_types::Value::Scalar(value);
    let session = casa_types::RecordValue::new(vec![
        casa_types::RecordField::new("FLAG_ROW", scalar(casa_types::ScalarValue::Bool(false))),
        casa_types::RecordField::new("LOG", string_array(&[])),
        casa_types::RecordField::new(
            "OBSERVER",
            scalar(casa_types::ScalarValue::String("second".to_string())),
        ),
        casa_types::RecordField::new(
            "PROJECT",
            scalar(casa_types::ScalarValue::String("trial".to_string())),
        ),
        casa_types::RecordField::new(
            "RELEASE_DATE",
            scalar(casa_types::ScalarValue::Float64(0.0)),
        ),
        casa_types::RecordField::new("SCHEDULE", string_array(&[])),
        casa_types::RecordField::new(
            "SCHEDULE_TYPE",
            scalar(casa_types::ScalarValue::String(String::new())),
        ),
        casa_types::RecordField::new(
            "TELESCOPE_NAME",
            scalar(casa_types::ScalarValue::String("VLA".to_string())),
        ),
        casa_types::RecordField::new(
            "TIME_RANGE",
            casa_types::Value::Array(casa_types::ArrayValue::Float64(
                ndarray::ArrayD::from_shape_vec(vec![2], vec![middle, middle]).expect("shape"),
            )),
        ),
    ]);
    ms.subtable_mut(casa_ms::SubtableId::Observation)
        .expect("OBSERVATION")
        .add_row(session)
        .expect("a second session");
    ms.save().expect("save");
    path
}

/// Two partitions of one spectral window keep one window name in the MSv4
/// (XRADIO's `NAME_SPWID`, by the window's row, not the partition's), and
/// come back as one `SPECTRAL_WINDOW` row and one data description, which
/// their sessions tell apart, each session's `TIME_RANGE` its own.
#[test]
fn partitions_of_one_window_share_its_name_and_rows() {
    let root = tempfile::tempdir().expect("root");
    let v2 = two_sessions(root.path(), false);
    let v4 = root.path().join("sessions.ps.zarr");
    let back = root.path().join("back.ms");
    let accept = [LossKind::Sigma, LossKind::Tables];
    let report = to_msv4(&v2, &v4, &options(&accept)).expect("to MSv4");
    assert_eq!(report.partitions, 2, "one partition per session");
    let converted = open(&v4, &OpenOptions::default()).expect("open");
    let windows: Vec<&str> = converted
        .catalogue()
        .partitions
        .iter()
        .map(|entry| entry.spectral_window.as_str())
        .collect();
    assert_eq!(windows.len(), 2);
    assert_eq!(windows[0], windows[1], "one window name: {windows:?}");
    assert!(windows[0].ends_with("_0"), "the window's row: {windows:?}");
    to_msv2(&v4, &back, &options(&accept)).expect("to MSv2");
    let ms = casa_ms::MeasurementSet::open(&back).expect("back");
    let count = |id| ms.subtable(id).expect("subtable").row_count();
    assert_eq!(count(casa_ms::SubtableId::SpectralWindow), 1);
    assert_eq!(count(casa_ms::SubtableId::DataDescription), 1);
    assert_eq!(count(casa_ms::SubtableId::Observation), 2);
    let sessions = ms.observation().expect("OBSERVATION");
    let ranges: Vec<Vec<f64>> = (0..2)
        .map(
            |row| match sessions.array(row, "TIME_RANGE").expect("TIME_RANGE") {
                casa_types::ArrayValue::Float64(range) => range.iter().copied().collect(),
                other => panic!("TIME_RANGE of row {row}: {other:?}"),
            },
        )
        .collect();
    // Edge to edge: contiguous integrations make the sessions meet.
    assert!(
        ranges[0][0] < ranges[0][1] && ranges[0][1] <= ranges[1][0] && ranges[1][0] < ranges[1][1],
        "each session its own times: {ranges:?}"
    );
    for id in [PartitionId(0), PartitionId(1)] {
        assert_eq!(read(&back, id, "base"), read(&v2, id, "base"), "{id:?}");
    }
}

/// What an MSv2 says that the boundary does not carry is reported: an
/// `ARRAY_ID` other than 0 and a `PROCESSOR` of more than one row.
#[test]
fn keys_and_subtable_rows_the_destination_drops_are_losses() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let mut ms = casa_ms::MeasurementSet::open(&v2).expect("ms");
    ms.main_table_mut()
        .cell_accessor_mut(0, "ARRAY_ID")
        .and_then(|mut cell| cell.set(casa_types::Value::Scalar(casa_types::ScalarValue::Int32(1))))
        .expect("a second array");
    let processors = ms
        .subtable(casa_ms::SubtableId::Processor)
        .expect("PROCESSOR")
        .row_count();
    let radiometer = casa_types::RecordValue::new(vec![
        casa_types::RecordField::new(
            "FLAG_ROW",
            casa_types::Value::Scalar(casa_types::ScalarValue::Bool(false)),
        ),
        casa_types::RecordField::new(
            "MODE_ID",
            casa_types::Value::Scalar(casa_types::ScalarValue::Int32(-1)),
        ),
        casa_types::RecordField::new(
            "SUB_TYPE",
            casa_types::Value::Scalar(casa_types::ScalarValue::String(String::new())),
        ),
        casa_types::RecordField::new(
            "TYPE",
            casa_types::Value::Scalar(casa_types::ScalarValue::String("RADIOMETER".to_string())),
        ),
        casa_types::RecordField::new(
            "TYPE_ID",
            casa_types::Value::Scalar(casa_types::ScalarValue::Int32(-1)),
        ),
    ]);
    for _ in processors..2 {
        ms.subtable_mut(casa_ms::SubtableId::Processor)
            .expect("PROCESSOR")
            .add_row(radiometer.clone())
            .expect("a second processor");
    }
    ms.save().expect("save");
    drop(ms);
    let out = root.path().join("dropped.ps.zarr");
    let refused = to_msv4(&v2, &out, &options(&[LossKind::Sigma, LossKind::Tables]));
    assert!(
        matches!(refused, Err(ObservationError::InvalidRequest { ref reason }) if reason.contains("ARRAY_ID")),
        "{refused:?}"
    );
    let report = to_msv4(
        &v2,
        &out,
        &options(&[LossKind::Sigma, LossKind::Tables, LossKind::Columns]),
    )
    .expect("accepted");
    assert!(
        report
            .losses
            .contains(&Loss::Column("ARRAY_ID".to_string()))
    );
    assert!(
        report
            .losses
            .contains(&Loss::Table("PROCESSOR".to_string()))
    );
}

/// A field whose direction moves is refused: one that follows an
/// ephemeris, whose directions casacore reads as offsets, and one whose
/// direction is a polynomial in time (as XRADIO refuses it). The catalogue
/// would hold the offset or the constant term as the phase centre.
#[test]
fn moving_fields_are_refused() {
    let ephemeris_root = tempfile::tempdir().expect("root");
    let ephemeris = fixture(ephemeris_root.path(), "ms_trial.ms");
    let mut ms = casa_ms::MeasurementSet::open(&ephemeris).expect("ms");
    ms.subtable_mut(casa_ms::SubtableId::Field)
        .expect("FIELD")
        .add_column(
            casa_tables::ColumnSchema::scalar("EPHEMERIS_ID", casa_types::PrimitiveType::Int32),
            Some(casa_types::Value::Scalar(casa_types::ScalarValue::Int32(0))),
        )
        .expect("an ephemeris");
    ms.save().expect("save");
    drop(ms);
    let polynomial_root = tempfile::tempdir().expect("root");
    let polynomial = fixture(polynomial_root.path(), "ms_trial.ms");
    let mut ms = casa_ms::MeasurementSet::open(&polynomial).expect("ms");
    ms.field_mut()
        .and_then(|mut fields| fields.set_i32(0, "NUM_POLY", 1))
        .expect("a polynomial");
    ms.save().expect("save");
    drop(ms);
    for (root, source, moving) in [
        (&ephemeris_root, &ephemeris, "ephemeris"),
        (&polynomial_root, &polynomial, "polynomial"),
    ] {
        let out = root.path().join("moving.ps.zarr");
        let refused = to_msv4(source, &out, &options(&[LossKind::Sigma, LossKind::Tables]));
        assert!(
            matches!(refused, Err(ObservationError::Unsupported { what }) if what.contains(moving)),
            "{refused:?}"
        );
        assert!(!out.exists());
    }
}

/// XRADIO names antennas `NAME_STATION` only for ALMA, VLA, NOEMA and
/// EVLA; another telescope's keep their names.
#[test]
fn only_the_telescopes_xradio_renames_get_station_names() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let mut ms = casa_ms::MeasurementSet::open(&v2).expect("ms");
    let names: Vec<String> = {
        let antennas = ms.antenna().expect("ANTENNA");
        (0..antennas.row_count())
            .map(|row| antennas.name(row).expect("NAME"))
            .collect()
    };
    let telescope = ms
        .observation()
        .and_then(|sessions| sessions.string(0, "TELESCOPE_NAME"))
        .expect("TELESCOPE_NAME");
    assert!(
        ["ALMA", "VLA", "NOEMA", "EVLA"].contains(&telescope.as_str()),
        "the fixture's antennas are renamed as it is: {telescope}"
    );
    ms.observation_mut()
        .expect("OBSERVATION")
        .set_string(0, "TELESCOPE_NAME", "MeerKAT")
        .expect("another telescope");
    ms.save().expect("save");
    drop(ms);
    let out = root.path().join("meerkat.ps.zarr");
    to_msv4(&v2, &out, &options(&[LossKind::Sigma, LossKind::Tables])).expect("convert");
    let converted = open(&out, &OpenOptions::default()).expect("open");
    let converted: Vec<String> = converted
        .catalogue()
        .antennas
        .iter()
        .map(|antenna| antenna.name.clone())
        .collect();
    assert_eq!(converted, names);
}

/// `FLAG_CATEGORY` held in one row only, neither the first nor the last,
/// is still found, and refused until flag categories are accepted.
#[test]
fn a_flag_category_held_in_any_row_is_a_loss() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let mut ms = casa_ms::MeasurementSet::open(&v2).expect("ms");
    let middle = ms.main_table().row_count() / 2;
    let mut shape = ms
        .main_table()
        .cell_accessor(middle, "FLAG")
        .and_then(|cell| cell.array_shape())
        .expect("FLAG shape")
        .expect("an array");
    shape.push(1);
    let cells = shape.iter().product();
    ms.main_table_mut()
        .cell_accessor_mut(middle, "FLAG_CATEGORY")
        .and_then(|mut cell| {
            cell.set(casa_types::Value::Array(casa_types::ArrayValue::Bool(
                ndarray::ArrayD::from_shape_vec(shape, vec![false; cells]).expect("shape"),
            )))
        })
        .expect("one category");
    ms.save().expect("save");
    drop(ms);
    let out = root.path().join("categories.ps.zarr");
    let accept = [LossKind::Sigma, LossKind::Tables];
    let refused = to_msv4(&v2, &out, &options(&accept));
    assert!(
        matches!(refused, Err(ObservationError::InvalidRequest { ref reason }) if reason.contains("FLAG_CATEGORY")),
        "{refused:?}"
    );
    assert!(!out.exists());
}

/// An MSv4 dataset lists the antennas its baselines name, as XRADIO's
/// converter writes it, so an antenna no baseline names is written nowhere
/// and is reported.
#[test]
fn antennas_no_baseline_names_are_a_loss() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let mut ms = casa_ms::MeasurementSet::open(&v2).expect("ms");
    let first_row = |ms: &casa_ms::MeasurementSet, id| {
        ms.subtable(id)
            .and_then(|table| table.rows().ok()?.first().cloned())
            .expect("a first row")
    };
    let antennas = ms
        .subtable(casa_ms::SubtableId::Antenna)
        .expect("ANTENNA")
        .row_count();
    let mut spare = first_row(&ms, casa_ms::SubtableId::Antenna);
    for column in ["NAME", "STATION"] {
        spare.upsert(
            column,
            casa_types::Value::Scalar(casa_types::ScalarValue::String("SPARE".to_string())),
        );
    }
    let mut feed = first_row(&ms, casa_ms::SubtableId::Feed);
    feed.upsert(
        "ANTENNA_ID",
        casa_types::Value::Scalar(casa_types::ScalarValue::Int32(
            i32::try_from(antennas).expect("few antennas"),
        )),
    );
    ms.subtable_mut(casa_ms::SubtableId::Antenna)
        .expect("ANTENNA")
        .add_row(spare)
        .expect("a spare antenna");
    ms.subtable_mut(casa_ms::SubtableId::Feed)
        .expect("FEED")
        .add_row(feed)
        .expect("its feed");
    ms.save().expect("save");
    drop(ms);
    let out = root.path().join("spare.ps.zarr");
    let report =
        to_msv4(&v2, &out, &options(&[LossKind::Sigma, LossKind::Tables])).expect("convert");
    assert!(
        report.losses.contains(&Loss::UnusedAntennas(1)),
        "{:?}",
        report.losses
    );
    let converted = open(&out, &OpenOptions::default()).expect("open");
    let names: Vec<&str> = converted
        .catalogue()
        .antennas
        .iter()
        .map(|antenna| antenna.name.as_str())
        .collect();
    assert_eq!(names.len(), antennas, "{names:?}");
    assert!(!names.iter().any(|name| name.starts_with("SPARE")));
}

/// XRADIO numbers an MSv2's datasets in the order their partitions' first
/// MAIN rows appear, not by partition key: a second session holding the
/// earlier times is dataset `_0`.
#[test]
fn datasets_are_numbered_as_their_first_rows_appear() {
    let root = tempfile::tempdir().expect("root");
    let v2 = two_sessions(root.path(), true);
    let v4 = root.path().join("order.ps.zarr");
    to_msv4(&v2, &v4, &options(&[LossKind::Sigma, LossKind::Tables])).expect("to MSv4");
    let converted = casa_ms::observation_io::Msv4Observation::open(&v4).expect("open");
    let mut starts: Vec<(String, f64)> = converted
        .catalogue()
        .partition_ids()
        .into_iter()
        .map(|id| {
            let grid = converted.grid(id, "base").expect("grid");
            let origin = (grid.time_origin.day() + grid.time_origin.frac()) * 86_400.0;
            (
                converted.dataset_name(id).to_string(),
                origin + grid.times_s[0],
            )
        })
        .collect();
    starts.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(starts[0].0, "ms_trial_0");
    assert_eq!(starts[1].0, "ms_trial_1");
    assert!(starts[0].1 < starts[1].1, "{starts:?}");
}

/// A negative `PROCESSOR_ID` names no processor (casacore's UVFITS filler
/// writes -1 in every row) and is no loss; a lone `PROCESSOR` that is not
/// a correlator is one, since an MSv2 destination writes a correlator.
#[test]
fn processors_are_lost_only_when_named_or_not_a_correlator() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let mut ms = casa_ms::MeasurementSet::open(&v2).expect("ms");
    for row in 0..ms.main_table().row_count() {
        ms.main_table_mut()
            .cell_accessor_mut(row, "PROCESSOR_ID")
            .and_then(|mut cell| {
                cell.set(casa_types::Value::Scalar(casa_types::ScalarValue::Int32(
                    -1,
                )))
            })
            .expect("no processor");
    }
    ms.save().expect("save");
    drop(ms);
    let accept = [LossKind::Sigma, LossKind::Tables];
    let report =
        to_msv4(&v2, &root.path().join("uvfits.ps.zarr"), &options(&accept)).expect("no loss");
    assert!(
        !report.losses.iter().any(|loss| matches!(
            loss,
            Loss::Column(column) if column == "PROCESSOR_ID"
        ) || *loss == Loss::Table("PROCESSOR".to_string())),
        "{:?}",
        report.losses
    );
    let scalar = |value| casa_types::Value::Scalar(value);
    let radiometer = casa_types::RecordValue::new(vec![
        casa_types::RecordField::new("FLAG_ROW", scalar(casa_types::ScalarValue::Bool(false))),
        casa_types::RecordField::new("MODE_ID", scalar(casa_types::ScalarValue::Int32(-1))),
        casa_types::RecordField::new(
            "SUB_TYPE",
            scalar(casa_types::ScalarValue::String(String::new())),
        ),
        casa_types::RecordField::new(
            "TYPE",
            scalar(casa_types::ScalarValue::String("RADIOMETER".to_string())),
        ),
        casa_types::RecordField::new("TYPE_ID", scalar(casa_types::ScalarValue::Int32(-1))),
    ]);
    let mut ms = casa_ms::MeasurementSet::open(&v2).expect("ms");
    assert_eq!(
        ms.processor().expect("PROCESSOR").row_count(),
        0,
        "the fixture's PROCESSOR is empty"
    );
    ms.subtable_mut(casa_ms::SubtableId::Processor)
        .expect("PROCESSOR")
        .add_row(radiometer)
        .expect("a lone radiometer");
    ms.save().expect("save");
    drop(ms);
    let report = to_msv4(
        &v2,
        &root.path().join("radiometer.ps.zarr"),
        &options(&accept),
    )
    .expect("accepted");
    assert!(
        report
            .losses
            .contains(&Loss::Table("PROCESSOR".to_string())),
        "{:?}",
        report.losses
    );
}
