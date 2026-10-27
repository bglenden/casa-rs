// SPDX-License-Identifier: LGPL-3.0-or-later
//! The write-back trial: update sessions on both stores, the changes they
//! make and refuse, and what they read to make them.

use casa_ms::observation_io::{OpenOptions, UpdateSession, open_update};
use casa_observation::{
    AccessRequest, AliasScope, BatchBuffers, ObservationError, PatchValues, ReadEvent,
    UpdateReport, UpdateTarget, VisibilityBatch, WeightChannels, WritableRole,
};
use ndarray::Array3;
use num_complex::Complex32;

use super::{Key, Sample, fixture, per_times, request, samples};

/// One target's values for one batch, owned.
enum Owned {
    Visibilities(Array3<Complex32>),
    Flags {
        set: Option<Array3<bool>>,
        clear: Option<Array3<bool>>,
    },
    Weights(Array3<f32>, WeightChannels),
}

impl Owned {
    fn view(&self) -> PatchValues<'_> {
        match self {
            Self::Visibilities(values) => PatchValues::Visibilities(values.view()),
            Self::Flags { set, clear } => PatchValues::Flags {
                set: set.as_ref().map(|mask| mask.view()),
                clear: clear.as_ref().map(|mask| mask.view()),
            },
            Self::Weights(values, channels) => PatchValues::Weights {
                values: values.view(),
                channels: *channels,
            },
        }
    }
}

/// Write every batch of `request`, read through `session`, with the
/// values `values` gives it, to targets `targets`.
fn pass(
    session: &UpdateSession,
    request: &AccessRequest,
    targets: &[usize],
    mut values: impl FnMut(&VisibilityBatch<'_>) -> Vec<Owned>,
) -> Result<UpdateReport, ObservationError> {
    let mut traversal = session.observation().bind_read(request)?;
    let mut updater = session.updater(request, targets)?;
    let mut buffers = BatchBuffers::default();
    while let Some(event) = traversal.next(&mut buffers)? {
        let ReadEvent::Batch(batch) = event else {
            continue;
        };
        let owned = values(&batch);
        let views: Vec<PatchValues<'_>> = owned.iter().map(Owned::view).collect();
        updater.write(&batch, &views)?;
    }
    updater.finish()
}

/// A flag change clearing every delivered flag.
fn clear_all(batch: &VisibilityBatch<'_>) -> Vec<Owned> {
    vec![Owned::Flags {
        set: None,
        clear: Some(Array3::from_elem(batch.shape(), true)),
    }]
}

fn model(value: Complex32) -> Complex32 {
    value * 2.0 + Complex32::new(0.0, 1.0)
}

/// Every sample of data group `group`, channels `channels`, of the store
/// at `path` opened afresh.
fn read(
    path: &std::path::Path,
    group: &str,
    channels: std::ops::Range<usize>,
) -> std::collections::BTreeMap<Key, Sample> {
    let observation = casa_ms::observation_io::open(path, &OpenOptions::default()).expect("open");
    let request = AccessRequest {
        data_group: group.to_string(),
        ..request(channels, usize::MAX)
    };
    samples(
        &mut observation.bind_read(&request).expect("bind"),
        observation.catalogue(),
    )
    .expect("read")
}

/// A model pass is one owner reading one group and writing another: on
/// the MSv2, `MODEL_DATA` is created holding CASA's initial model in every
/// row and then written for every row from the `DATA` the same session
/// reads, cells supplied whole and never read; it then opens as a `model`
/// group with `DATA`'s keywords. An MSv4 store cannot gain a group, so the
/// same pass there is refused at opening and rewrites `base` instead, its
/// chunks supplied whole and never read.
#[test]
fn a_model_pass_reads_one_group_and_writes_another() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let reference = read(&v2, "base", 0..8);
    let created = UpdateTarget {
        create: true,
        ..UpdateTarget::new("model", WritableRole::Visibilities)
    };
    let session =
        open_update(&v2, &OpenOptions::default(), std::slice::from_ref(&created)).expect("v2");
    assert!(session.targets()[0].created);
    assert_eq!(session.targets()[0].variable, "MODEL_DATA");
    let four = per_times(session.observation(), request(0..8, 0), 4);
    let report = pass(&session, &four, &[0], |batch| {
        vec![Owned::Visibilities(
            batch.group.values.expect("values").mapv(model),
        )]
    })
    .expect("v2 pass");
    drop(session);
    assert_eq!(
        (
            report.batches,
            report.samples,
            report.units_written,
            report.units_read
        ),
        (3, 120, 240, 0),
        "120 cells seeded when MODEL_DATA is created, 120 written whole"
    );
    let written = read(&v2, "model", 0..8);
    assert_eq!(read(&v2, "base", 0..8), reference, "DATA unchanged");
    let ms = casa_ms::MeasurementSet::open(&v2).expect("reopen");
    assert_eq!(
        ms.main_table().column_keywords("MODEL_DATA"),
        ms.main_table().column_keywords("DATA"),
        "DATA's keywords"
    );
    drop(ms);
    for (key, sample) in &reference {
        let expected: Vec<Complex32> = sample.values.iter().map(|v| model(*v)).collect();
        assert_eq!(written[key].values, expected, "{key:?}");
        assert_eq!(written[key].flags, sample.flags, "shared FLAG");
    }

    let v4 = fixture(root.path(), "ms_trial.ps.zarr");
    assert!(matches!(
        open_update(&v4, &OpenOptions::default(), &[created]),
        Err(ObservationError::Unsupported { .. })
    ));
    let base = UpdateTarget::new("base", WritableRole::Visibilities);
    let session = open_update(&v4, &OpenOptions::default(), &[base]).expect("v4");
    let four = per_times(session.observation(), request(0..8, 0), 4);
    let report = pass(&session, &four, &[0], |batch| {
        vec![Owned::Visibilities(
            batch.group.values.expect("values").mapv(model),
        )]
    })
    .expect("v4 pass");
    drop(session);
    assert_eq!(
        (report.units_written, report.units_read),
        (6, 0),
        "six chunks of [5, 10, 4, 4] written whole from the held windows"
    );
    assert_eq!(
        report.held_bytes,
        5 * 10 * 8 * 4 * (8 + 1) + 2 * size_of::<usize>(),
        "five times of every element, its mark, and the row's two chunk counts"
    );
    let rewritten = read(&v4, "base", 0..8);
    for (key, sample) in &reference {
        let expected: Vec<Complex32> = sample.values.iter().map(|v| model(*v)).collect();
        let back = rewritten
            .iter()
            .find(|(other, _)| other.0 == key.0 && other.1 == key.1 && other.2 == key.2)
            .map(|(_, sample)| sample)
            .expect("same sample");
        assert_eq!(back.values, expected, "{key:?}");
    }
}

/// A unit a patch supplies whole is never read; one it supplies partly is
/// read once. A two-channel window rewrites part of every MSv2 cell (read
/// each) and part of three of the six MSv4 chunks (read each once, however
/// many batches touch it).
#[test]
fn partial_units_are_read_once_and_whole_ones_never() {
    let root = tempfile::tempdir().expect("root");
    for name in ["ms_trial.ms", "ms_trial.ps.zarr"] {
        let path = fixture(root.path(), name);
        let before = read(&path, "base", 0..8);
        let session = open_update(
            &path,
            &OpenOptions::default(),
            &[UpdateTarget::new("base", WritableRole::Visibilities)],
        )
        .expect("session");
        let window = per_times(session.observation(), request(2..4, 0), 4);
        let report = pass(&session, &window, &[0], |batch| {
            vec![Owned::Visibilities(
                batch.group.values.expect("values").mapv(model),
            )]
        })
        .expect("pass");
        drop(session);
        let expected_reads = if name.ends_with(".ms") { 120 } else { 3 };
        assert_eq!(report.units_read, expected_reads, "{name}");
        assert_eq!(report.units_written, expected_reads, "{name}");
        let after = read(&path, "base", 0..8);
        for (key, sample) in &before {
            let back = &after[key];
            for (index, (old, new)) in sample.values.iter().zip(&back.values).enumerate() {
                let channel = index / 4;
                let expected = if (2..4).contains(&channel) {
                    model(*old)
                } else {
                    *old
                };
                assert_eq!(*new, expected, "{name} {key:?} element {index}");
            }
        }
    }
}

/// An MSv2 `WEIGHT` is one value per row and correlation that every channel
/// reads, so a patch may not change it for some channels: changing the
/// delivered two channels' weight is refused and nothing is written, while
/// changing every channel's is the explicit whole-scalar change. MSv4 holds
/// a weight per channel and changes just the delivered ones.
#[test]
fn a_weight_shared_by_channels_changes_only_as_a_whole() {
    let root = tempfile::tempdir().expect("root");
    let eights = |batch: &VisibilityBatch<'_>| Array3::from_elem(batch.shape(), 8.0_f32);
    for name in ["ms_trial.ms", "ms_trial.ps.zarr"] {
        let path = fixture(root.path(), name);
        let before = read(&path, "base", 0..8);
        let session = open_update(
            &path,
            &OpenOptions::default(),
            &[UpdateTarget::new("base", WritableRole::Weights)],
        )
        .expect("session");
        let two = per_times(session.observation(), request(0..2, 0), 4);
        let delivered = pass(&session, &two, &[0], |batch| {
            vec![Owned::Weights(eights(batch), WeightChannels::Delivered)]
        });
        if name.ends_with(".ms") {
            assert!(
                matches!(delivered, Err(ObservationError::InvalidRequest { .. })),
                "{delivered:?}"
            );
            assert_eq!(read(&path, "base", 0..8), before, "nothing written");
        } else {
            delivered.expect("MSv4 weights are per channel");
            let after = read(&path, "base", 0..8);
            for (key, sample) in &before {
                for (index, (old, new)) in
                    sample.weights.iter().zip(&after[key].weights).enumerate()
                {
                    let expected = if index / 4 < 2 { 8.0 } else { *old };
                    assert_eq!(*new, expected, "{key:?} {index}");
                }
            }
        }
        pass(&session, &two, &[0], |batch| {
            vec![Owned::Weights(eights(batch), WeightChannels::All)]
        })
        .expect("every channel");
        drop(session);
        let after = read(&path, "base", 0..8);
        for (key, sample) in &after {
            assert!(sample.weights.iter().all(|w| *w == 8.0), "{name} {key:?}");
        }
    }
}

/// Data groups share their flags and weights: a target writing a variable
/// another group reads, or will read once the session creates it, must
/// accept that alias, and the resolved target names it.
#[test]
fn a_shared_variable_is_written_only_with_its_aliases_accepted() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    let created = UpdateTarget {
        create: true,
        ..UpdateTarget::new("model", WritableRole::Visibilities)
    };
    let base_flags = |aliases| UpdateTarget {
        aliases,
        ..UpdateTarget::new("base", WritableRole::Flags)
    };
    let Err(ObservationError::InvalidRequest { reason }) = open_update(
        &path,
        &OpenOptions::default(),
        &[created.clone(), base_flags(AliasScope::Refuse)],
    ) else {
        panic!("FLAG is model's too once the session creates it");
    };
    assert!(reason.contains("model"), "{reason}");
    let session = open_update(
        &path,
        &OpenOptions::default(),
        &[created.clone(), base_flags(AliasScope::Accept)],
    )
    .expect("accepted");
    assert_eq!(session.targets()[1].aliases, ["model".to_string()]);
    drop(session);
    let session = open_update(&path, &OpenOptions::default(), &[created]).expect("create");
    let all = per_times(session.observation(), request(0..8, 0), 12);
    pass(&session, &all, &[0], |batch| {
        vec![Owned::Visibilities(
            batch.group.values.expect("values").to_owned(),
        )]
    })
    .expect("seed MODEL_DATA");
    drop(session);
    let flags = |group: &str, aliases| UpdateTarget {
        aliases,
        ..UpdateTarget::new(group, WritableRole::Flags)
    };
    for group in ["base", "model"] {
        let refused = open_update(
            &path,
            &OpenOptions::default(),
            &[flags(group, AliasScope::Refuse)],
        );
        let Err(ObservationError::InvalidRequest { reason }) = refused else {
            panic!("{group}: refused");
        };
        assert!(reason.contains("FLAG"), "{reason}");
        let session = open_update(
            &path,
            &OpenOptions::default(),
            &[flags(group, AliasScope::Accept)],
        )
        .expect("accepted");
        let other = if group == "base" { "model" } else { "base" };
        assert_eq!(session.targets()[0].aliases, [other.to_string()]);
    }
}

/// A flag change sets and clears element flags only: a row flagged by
/// `FLAG_ROW` stays excluded when its `FLAG` cells are cleared, and a
/// padded cell, which has no MSv2 row, receives nothing and may not be
/// cleared.
#[test]
fn flag_changes_keep_row_flags_and_padding() {
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
    ms.main_table_mut().remove_rows(&[7]).expect("pad a cell");
    ms.save().expect("save");
    drop(ms);
    let target = [UpdateTarget::new("base", WritableRole::Flags)];
    let session = open_update(&path, &OpenOptions::default(), &target).expect("session");
    let four = per_times(session.observation(), request(0..8, 0), 4);
    let present = |batch: &VisibilityBatch<'_>| {
        batch
            .group
            .values
            .expect("values")
            .mapv(|value| !value.re.is_nan())
    };
    let everything = pass(&session, &four, &[0], |batch| {
        vec![Owned::Flags {
            set: None,
            clear: Some(Array3::from_elem(batch.shape(), true)),
        }]
    });
    assert!(
        matches!(everything, Err(ObservationError::InvalidRequest { ref reason }) if reason.contains("padding")),
        "{everything:?}"
    );
    let report = pass(&session, &four, &[0], |batch| {
        vec![Owned::Flags {
            set: None,
            clear: Some(present(batch)),
        }]
    })
    .expect("clear present samples");
    drop(session);
    assert_eq!((report.samples, report.unaddressed), (119, 1));
    let after = read(&path, "base", 0..8);
    let flagged: Vec<_> = after
        .values()
        .filter(|sample| sample.flags.iter().any(|f| *f))
        .collect();
    assert_eq!(flagged.len(), 2, "the FLAG_ROW row and the padded cell");
    assert!(flagged.iter().all(|sample| sample.flags.iter().all(|f| *f)));
}

/// An MSv4 store pads with NaN weights: clearing a flag or writing a
/// finite weight there is refused, and so is clearing flags from a
/// traversal that does not read weights.
#[test]
fn msv4_padding_stays_flagged() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ps.zarr");
    let targets = [
        UpdateTarget::new("base", WritableRole::Weights),
        UpdateTarget::new("base", WritableRole::Flags),
    ];
    let session = open_update(&path, &OpenOptions::default(), &targets).expect("session");
    let four = per_times(session.observation(), request(0..8, 0), 4);
    pass(&session, &four, &[0], |batch| {
        let mut weights = batch.group.weights.expect("weights").to_owned();
        weights.slice_mut(ndarray::s![0, .., ..]).fill(f32::NAN);
        vec![Owned::Weights(weights, WeightChannels::Delivered)]
    })
    .expect("pad the first sample of each batch");
    let refused = pass(&session, &four, &[1], clear_all);
    assert!(
        matches!(refused, Err(ObservationError::InvalidRequest { ref reason }) if reason.contains("padding")),
        "{refused:?}"
    );
    let weighted = pass(&session, &four, &[0], |batch| {
        vec![Owned::Weights(
            Array3::from_elem(batch.shape(), 8.0),
            WeightChannels::All,
        )]
    });
    assert!(
        matches!(weighted, Err(ObservationError::InvalidRequest { ref reason }) if reason.contains("padding")),
        "a finite weight would make padding an acquisition: {weighted:?}"
    );
    let flags_only = AccessRequest {
        roles: casa_observation::Roles {
            values: false,
            weights: false,
            ..casa_observation::Roles::ALL
        },
        ..four.clone()
    };
    let blind = pass(&session, &flags_only, &[1], clear_all);
    assert!(
        matches!(blind, Err(ObservationError::InvalidRequest { ref reason }) if reason.contains("weights")),
        "{blind:?}"
    );
}

/// One update session per store in a process, and one live updater per
/// session, binding each variable once; read-only observations open beside
/// them.
#[test]
fn one_update_session_per_store_and_one_updater_per_session() {
    let root = tempfile::tempdir().expect("root");
    for name in ["ms_trial.ms", "ms_trial.ps.zarr"] {
        let path = fixture(root.path(), name);
        let target = [UpdateTarget::new("base", WritableRole::Flags)];
        let session = open_update(&path, &OpenOptions::default(), &target).expect("first");
        assert!(matches!(
            open_update(&path, &OpenOptions::default(), &target),
            Err(ObservationError::Conflict { .. })
        ));
        let reader = casa_ms::observation_io::open(&path, &OpenOptions::default())
            .expect("a reader beside the session");
        drop(reader);
        let one = per_times(session.observation(), request(0..8, 0), 12);
        assert!(
            matches!(
                session.updater(&one, &[0, 0]),
                Err(ObservationError::InvalidRequest { .. })
            ),
            "one variable bound twice"
        );
        let first = session.updater(&one, &[0]).expect("one updater");
        assert!(matches!(
            session.updater(&one, &[0]),
            Err(ObservationError::Conflict { .. })
        ));
        drop(first);
        drop(
            session
                .updater(&one, &[0])
                .expect("again once the first is gone"),
        );
        drop(session);
        drop(open_update(&path, &OpenOptions::default(), &target).expect("again once closed"));
        let guarantees = open_update(&path, &OpenOptions::default(), &target)
            .expect("session")
            .guarantees();
        assert_eq!(
            (guarantees.atomic_units, guarantees.cross_process_lock),
            if name.ends_with(".ms") {
                (false, true)
            } else {
                (true, false)
            }
        );
    }
}

/// The MSv2 updater plans its traversal's rows up front: it takes the
/// batches in delivery order and finishes only once all are written.
#[test]
fn an_msv2_updater_takes_its_batches_in_order() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    let session = open_update(
        &path,
        &OpenOptions::default(),
        &[UpdateTarget::new("base", WritableRole::Visibilities)],
    )
    .expect("session");
    let four = per_times(session.observation(), request(0..8, 0), 4);
    let mut traversal = session.observation().bind_read(&four).expect("bind");
    let mut updater = session.updater(&four, &[0]).expect("updater");
    let mut buffers = BatchBuffers::default();
    let mut delivered = 0;
    while let Some(ReadEvent::Batch(batch)) = traversal.next(&mut buffers).expect("read") {
        delivered += 1;
        if delivered == 1 {
            continue;
        }
        let values = batch.group.values.expect("values").to_owned();
        let refused = updater.write(&batch, &[PatchValues::Visibilities(values.view())]);
        assert!(
            matches!(refused, Err(ObservationError::InvalidRequest { .. })),
            "batch {delivered} after a skipped one: {refused:?}"
        );
    }
    assert_eq!(delivered, 3);
    assert!(
        matches!(
            updater.finish(),
            Err(ObservationError::InvalidRequest { .. })
        ),
        "the planned rows were not written"
    );
}

/// A flag change that names no element (neither mask) keeps every flag:
/// MSv2 rewrites each cell as stored, MSv4 writes nothing.
#[test]
fn an_empty_flag_change_keeps_every_flag() {
    let root = tempfile::tempdir().expect("root");
    for name in ["ms_trial.ms", "ms_trial.ps.zarr"] {
        let path = fixture(root.path(), name);
        let before = read(&path, "base", 0..8);
        assert!(
            before
                .values()
                .any(|sample| sample.flags.iter().any(|f| *f))
        );
        let session = open_update(
            &path,
            &OpenOptions::default(),
            &[UpdateTarget::new("base", WritableRole::Flags)],
        )
        .expect("session");
        let four = per_times(session.observation(), request(0..8, 0), 4);
        let report = pass(&session, &four, &[0], |_| {
            vec![Owned::Flags {
                set: None,
                clear: None,
            }]
        })
        .expect("no change");
        drop(session);
        if name.ends_with(".zarr") {
            assert_eq!(report.units_written, 0, "{name}");
        }
        let after = read(&path, "base", 0..8);
        for (key, sample) in &before {
            assert_eq!(after[key].flags, sample.flags, "{name} {key:?}");
        }
    }
}

/// Padding that casa-rs's writer stores (from an MSv2 with a row removed)
/// is padding to the MSv4 updater too: its weight is NaN, as XRADIO pads,
/// so clearing its flags or giving it a finite weight is refused.
#[test]
fn padding_the_writer_stores_stays_padding() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    let mut ms = casa_ms::MeasurementSet::open(&path).expect("ms");
    ms.main_table_mut().remove_rows(&[7]).expect("remove row 7");
    ms.save().expect("save");
    drop(ms);
    let sparse = casa_ms::observation_io::Msv2Observation::open(&path).expect("padded");
    let out = root.path().join("padded.ps.zarr");
    let (set, mut writer) = super::writer::create(&out, &sparse, true);
    super::writer::write_all(&mut writer, &sparse, 4);
    super::writer::publish(set, writer).expect("finish");
    let targets = [
        UpdateTarget::new("base", WritableRole::Flags),
        UpdateTarget::new("base", WritableRole::Weights),
    ];
    let session = open_update(&out, &OpenOptions::default(), &targets).expect("session");
    let four = per_times(session.observation(), request(0..8, 0), 4);
    let cleared = pass(&session, &four, &[0], clear_all);
    assert!(
        matches!(cleared, Err(ObservationError::InvalidRequest { ref reason }) if reason.contains("padding")),
        "{cleared:?}"
    );
    let weighted = pass(&session, &four, &[1], |batch| {
        vec![Owned::Weights(
            Array3::from_elem(batch.shape(), 8.0),
            WeightChannels::Delivered,
        )]
    });
    assert!(
        matches!(weighted, Err(ObservationError::InvalidRequest { ref reason }) if reason.contains("padding")),
        "{weighted:?}"
    );
    drop(session);
    let padded: Vec<_> = read(&out, "base", 0..8)
        .into_values()
        .filter(|sample| sample.weights.iter().all(|w| w.is_nan()))
        .collect();
    assert_eq!(padded.len(), 1);
    assert!(padded[0].flags.iter().all(|f| *f), "still flagged");
}

/// A session reads what its own updaters wrote: a second traversal of its
/// observation delivers the first pass's values.
#[test]
fn a_session_reads_what_its_updaters_wrote() {
    let root = tempfile::tempdir().expect("root");
    for name in ["ms_trial.ms", "ms_trial.ps.zarr"] {
        let path = fixture(root.path(), name);
        let before = read(&path, "base", 0..8);
        let session = open_update(
            &path,
            &OpenOptions::default(),
            &[UpdateTarget::new("base", WritableRole::Visibilities)],
        )
        .expect("session");
        let four = per_times(session.observation(), request(0..8, 0), 4);
        pass(&session, &four, &[0], |batch| {
            vec![Owned::Visibilities(
                batch.group.values.expect("values").mapv(model),
            )]
        })
        .expect("pass");
        let again = samples(
            &mut session.observation().bind_read(&four).expect("bind"),
            session.observation().catalogue(),
        )
        .expect("read again");
        for (key, sample) in &before {
            let expected: Vec<Complex32> = sample.values.iter().map(|v| model(*v)).collect();
            assert_eq!(again[key].values, expected, "{name} {key:?}");
        }
    }
}

/// Clearing every flag of the MSv4 fixture makes each `FLAG` chunk equal to
/// the fill value (false), which zarrs erases rather than stores; one of
/// them (`c/2/0/0/0`) was never stored. The update still finishes, its
/// changed directories synchronized, and reads back unflagged.
#[test]
fn clearing_every_msv4_flag_erases_chunks_and_finishes() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ps.zarr");
    let session = open_update(
        &path,
        &OpenOptions::default(),
        &[UpdateTarget::new("base", WritableRole::Flags)],
    )
    .expect("session");
    let four = per_times(session.observation(), request(0..8, 0), 4);
    let report = pass(&session, &four, &[0], clear_all).expect("finishes");
    drop(session);
    assert_eq!(report.units_written, 6, "every chunk erased");
    let after = read(&path, "base", 0..8);
    assert!(
        after
            .values()
            .all(|sample| sample.flags.iter().all(|f| !*f))
    );
}

/// An MSv4 updater takes only batches of its traversal's channels: one of
/// other channels is refused before any target takes it.
#[test]
fn an_msv4_batch_of_other_channels_is_refused_whole() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ps.zarr");
    let before = read(&path, "base", 0..8);
    let targets = [
        UpdateTarget::new("base", WritableRole::Weights),
        UpdateTarget::new("base", WritableRole::Flags),
    ];
    let session = open_update(&path, &OpenOptions::default(), &targets).expect("session");
    let narrow = per_times(session.observation(), request(2..4, 0), 4);
    let wide = per_times(session.observation(), request(0..8, 0), 4);
    let mut traversal = session.observation().bind_read(&wide).expect("bind");
    let mut updater = session.updater(&narrow, &[0, 1]).expect("updater");
    let mut buffers = BatchBuffers::default();
    let Some(ReadEvent::Batch(batch)) = traversal.next(&mut buffers).expect("read") else {
        panic!("a batch");
    };
    let weights = Array3::from_elem(batch.shape(), 8.0_f32);
    let set = Array3::from_elem(batch.shape(), true);
    let refused = updater.write(
        &batch,
        &[
            PatchValues::Weights {
                values: weights.view(),
                channels: WeightChannels::Delivered,
            },
            PatchValues::Flags {
                set: Some(set.view()),
                clear: None,
            },
        ],
    );
    assert!(
        matches!(refused, Err(ObservationError::InvalidRequest { ref reason }) if reason.contains("channels")),
        "{refused:?}"
    );
    let report = updater.finish().expect("finish");
    assert_eq!(report.units_written, 0, "nothing of the batch");
    drop(traversal);
    drop(session);
    assert_eq!(read(&path, "base", 0..8), before);
}

/// A model pass over part of an MSv2 may create `MODEL_DATA`: CASA's
/// initial model (1, and 0 in the cross hands `XY`, `YX`, `RL`, `LR`) fills
/// every cell when the column is added, as CASA's `initScrCols` does, and
/// the pass writes its two channels over it, merging each cell with the
/// seed it reads back. The rest of the column is the seed, never `DATA`,
/// and a second pass of the session finds the column and seeds nothing.
#[test]
fn a_partial_model_pass_creates_model_data_over_casas_initial_model() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    let reference = read(&path, "base", 0..8);
    let created = UpdateTarget {
        create: true,
        ..UpdateTarget::new("model", WritableRole::Visibilities)
    };
    let session = open_update(&path, &OpenOptions::default(), &[created]).expect("session");
    let correlations = session.observation().catalogue().partitions[0]
        .correlations
        .clone();
    let two = per_times(session.observation(), request(2..4, 0), 4);
    let report = pass(&session, &two, &[0], |batch| {
        vec![Owned::Visibilities(
            batch.group.values.expect("values").mapv(model),
        )]
    })
    .expect("a two-channel model pass");
    assert_eq!(
        (report.units_written, report.units_read),
        (240, 120),
        "120 cells seeded, then 120 merged with the seed each read back"
    );
    let again = pass(&session, &two, &[0], |batch| {
        vec![Owned::Visibilities(
            batch.group.values.expect("values").mapv(model),
        )]
    })
    .expect("a second pass of the same session");
    assert_eq!(
        (again.units_written, again.units_read),
        (120, 120),
        "the column exists now: nothing is seeded again"
    );
    drop(session);
    let written = read(&path, "model", 0..8);
    let products = correlations.len();
    for (key, sample) in &reference {
        for (index, value) in written[key].values.iter().enumerate() {
            let (channel, product) = (index / products, index % products);
            let expected = if (2..4).contains(&channel) {
                model(sample.values[index])
            } else if matches!(correlations[product].label(), "XY" | "YX" | "RL" | "LR") {
                Complex32::new(0.0, 0.0)
            } else {
                Complex32::new(1.0, 0.0)
            };
            assert_eq!(
                *value, expected,
                "{key:?} channel {channel} product {product}"
            );
        }
    }
}
