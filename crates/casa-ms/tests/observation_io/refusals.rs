// SPDX-License-Identifier: LGPL-3.0-or-later
//! What the adapters refuse, or resolve, on stores that differ from the
//! fixtures in one deliberate way: each case edits a copy of a fixture's
//! metadata and checks that the native views stay true or binding fails.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_ms::MeasurementSet;
use casa_ms::observation_io::{Msv2Observation, Msv4Observation, Msv4ProcessingSetWriter};
use casa_observation::{
    AccessRequest, BatchBuffers, FieldId, Grouping, ObservationError, PartitionId, ReadEvent, Roles,
};
use casa_types::measures::EpochRef;
use casa_types::{ScalarValue, Value};
use zarrs::array::{Array, ArraySubset};
use zarrs::filesystem::FilesystemStore;

use super::writer::storage;
use super::{fixture, request};

/// The visibility dataset's directory inside processing set `store`.
fn dataset(store: &Path) -> PathBuf {
    std::fs::read_dir(store)
        .expect("store")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.join("zarr.json").is_file())
        .expect("a dataset")
}

/// Edit the JSON at `path` in place.
fn edit_json(path: &Path, edit: impl FnOnce(&mut serde_json::Value)) {
    let mut value: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("read")).expect("json");
    edit(&mut value);
    std::fs::write(path, serde_json::to_string_pretty(&value).expect("json")).expect("write");
}

fn first_batch_time_scale(observation: &dyn casa_observation::Observation) -> EpochRef {
    let mut traversal = observation
        .bind_read(&request(0..8, usize::MAX))
        .expect("bind");
    let mut buffers = BatchBuffers::default();
    let Some(ReadEvent::Batch(batch)) = traversal.next(&mut buffers).expect("read") else {
        panic!("a batch");
    };
    let scale = batch.time.reference();
    assert_eq!(
        batch.timing.centroid.expect("centroid").reference(),
        scale,
        "centroids share the time scale"
    );
    scale
}

/// XRADIO's `unix` times follow astropy, whose Unix epoch is UTC in every
/// scale. A `unix`/`tai` store is refused rather than read as TAI seconds
/// from 1970-01-01 TAI, which would misplace every instant by the leap
/// seconds (37 s in 2023).
#[test]
fn an_msv4_unix_time_in_tai_is_refused() {
    let root = tempfile::tempdir().expect("root");
    let store = fixture(root.path(), "ms_trial.ps.zarr");
    edit_json(&dataset(&store).join("time/zarr.json"), |value| {
        value["attributes"]["scale"] = "tai".into();
    });
    assert!(matches!(
        Msv4Observation::open(&store),
        Err(ObservationError::Unsupported { .. })
    ));
}

/// The reader decodes bytes into the caller's typed buffers, so a variable
/// of another data type, even one of the same width (an `int32` weight, a
/// `uint8` bitmask flag, a `float64` "visibility"), or with its dimensions
/// in another order, is refused at opening, before any decode.
#[test]
fn msv4_payload_types_and_dimensions_are_checked_at_opening() {
    let cases: [(&str, &str, serde_json::Value); 4] = [
        ("VISIBILITY", "float64", "NaN".into()),
        ("WEIGHT", "int32", 0.into()),
        ("FLAG", "uint8", 0.into()),
        ("UVW", "int64", 0.into()),
    ];
    for (variable, data_type, fill) in cases {
        let root = tempfile::tempdir().expect("root");
        let store = fixture(root.path(), "ms_trial.ps.zarr");
        edit_json(
            &dataset(&store).join(format!("{variable}/zarr.json")),
            |value| {
                value["data_type"] = data_type.into();
                value["fill_value"] = fill;
            },
        );
        match Msv4Observation::open(&store) {
            Err(ObservationError::InvalidStore { reason }) => {
                assert!(
                    reason.contains(variable) && reason.contains(data_type),
                    "{variable}: {reason}"
                );
            }
            Err(other) => panic!("{variable} as {data_type}: refused by the wrong check: {other}"),
            Ok(_) => panic!("{variable} as {data_type} was accepted"),
        }
    }
    let root = tempfile::tempdir().expect("root");
    let store = fixture(root.path(), "ms_trial.ps.zarr");
    edit_json(&dataset(&store).join("VISIBILITY/zarr.json"), |value| {
        value["dimension_names"] =
            serde_json::json!(["baseline_id", "time", "frequency", "polarization"]);
    });
    assert!(matches!(
        Msv4Observation::open(&store),
        Err(ObservationError::InvalidStore { .. })
    ));
}

/// A role the request reads but the store lacks fails the estimate and the
/// binding, never a later read or a made-up value; a request that does not
/// read it binds.
#[test]
fn missing_metadata_fails_binding() {
    for variable in ["TIME_CENTROID", "EFFECTIVE_INTEGRATION_TIME", "scan_name"] {
        let root = tempfile::tempdir().expect("root");
        let store = fixture(root.path(), "ms_trial.ps.zarr");
        std::fs::remove_dir_all(dataset(&store).join(variable)).expect("remove");
        let v4 = Msv4Observation::open(&store).expect("open without it");
        for result in [
            v4.estimate(&request(0..8, usize::MAX)).map(|_| ()),
            v4.bind_read(&request(0..8, usize::MAX)).map(|_| ()),
        ] {
            match result {
                Err(ObservationError::InvalidRequest { reason }) => {
                    assert!(reason.contains(variable), "{reason}");
                }
                other => panic!("{variable}: {other:?}"),
            }
        }
        let without = AccessRequest {
            roles: Roles {
                time_centroid: variable != "TIME_CENTROID",
                exposure: variable != "EFFECTIVE_INTEGRATION_TIME",
                scan: variable != "scan_name",
                ..Roles::ALL
            },
            ..request(0..8, usize::MAX)
        };
        let mut traversal = v4.bind_read(&without).expect("bind without it");
        let mut buffers = BatchBuffers::default();
        assert!(traversal.next(&mut buffers).expect("read").is_some());
    }
}

/// Rewrite `column`'s `MEASINFO` in the MSv2 at `path`.
fn set_measinfo(path: &Path, column: &str, edit: impl Fn(&mut casa_types::RecordValue)) {
    let mut ms = MeasurementSet::open(path).expect("ms");
    let main = ms.main_table_mut();
    let mut keywords = main.column_keywords(column).expect("keywords").clone();
    let Some(Value::Record(measinfo)) = keywords.get_mut("MEASINFO") else {
        panic!("{column} has a MEASINFO record");
    };
    edit(measinfo);
    main.set_column_keywords(column, keywords);
    ms.save().expect("save");
}

/// The MSv2 adapter reads `TIME`'s reference instead of asserting UTC: a
/// fixture whose `TIME` and `TIME_CENTROID` say TAI delivers TAI epochs
/// (casacore's `MSMainColumns::setEpochRef` permits it), which the MSv4
/// writer then refuses to label as UTC. Disagreeing columns, or a
/// variable reference, are refused at opening.
#[test]
fn the_msv2_time_reference_is_read_not_assumed() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    let tai = |measinfo: &mut casa_types::RecordValue| {
        measinfo.upsert("Ref", Value::Scalar(ScalarValue::String("TAI".to_string())));
    };
    set_measinfo(&path, "TIME", tai);
    assert!(
        matches!(
            Msv2Observation::open(&path),
            Err(ObservationError::Unsupported { .. })
        ),
        "TIME in TAI, TIME_CENTROID in UTC"
    );
    set_measinfo(&path, "TIME_CENTROID", tai);
    let v2 = Msv2Observation::open(&path).expect("TAI throughout");
    assert_eq!(v2.catalogue().partitions[0].time_scale, EpochRef::TAI);
    assert_eq!(first_batch_time_scale(&v2), EpochRef::TAI);
    let grid = v2.grid(PartitionId(0), "base").expect("grid");
    assert_eq!(grid.time_scale, EpochRef::TAI);
    assert!(
        matches!(
            Msv4ProcessingSetWriter::create(&root.path().join("tai.ps.zarr"))
                .expect("create")
                .dataset(v2.catalogue(), PartitionId(0), &["base"], &grid, &storage()),
            Err(ObservationError::Unsupported { .. })
        ),
        "the writer does not label TAI seconds as unix/utc"
    );
    set_measinfo(&path, "TIME", |measinfo| {
        measinfo.upsert(
            "VarRefCol",
            Value::Scalar(ScalarValue::String("TIME_REF".to_string())),
        );
    });
    assert!(matches!(
        Msv2Observation::open(&path),
        Err(ObservationError::Unsupported { .. })
    ));
}

/// Shift the phase centres of the `field_and_source` dataset at `path` by
/// `offset` radians in longitude.
fn shift_phase_centres(store: &Path, path: &str, offset: f64) {
    let store = Arc::new(FilesystemStore::new(store).expect("store"));
    let array = Array::open(store, &format!("{path}/FIELD_PHASE_CENTER_DIRECTION")).expect("open");
    let subset = ArraySubset::new_with_shape(array.shape().to_vec());
    let mut centres: Vec<f64> = array.retrieve_array_subset(&subset).expect("read");
    for longitude in centres.iter_mut().step_by(2) {
        *longitude += offset;
    }
    array.store_array_subset(&subset, &centres).expect("write");
}

/// A dataset with two data groups whose `field_and_source` datasets have
/// different phase centres under the same field name keeps each group's
/// own context: the catalogue has two fields (a shared display name does
/// not merge them), and a traversal of either group delivers its own field
/// in its batches and group keys. A group whose phase centres are in
/// another frame than its UVW is refused.
#[test]
fn each_data_group_keeps_its_own_field_context() {
    let root = tempfile::tempdir().expect("root");
    let store = fixture(root.path(), "ms_trial.ps.zarr");
    let dataset_dir = dataset(&store);
    let name = dataset_dir
        .file_name()
        .expect("name")
        .to_string_lossy()
        .into_owned();
    let copy = |from: &Path, to: &Path| {
        std::fs::create_dir_all(to).expect("mkdir");
        let status = std::process::Command::new("cp")
            .args(["-R"])
            .arg(from.join("."))
            .arg(to)
            .status()
            .expect("cp");
        assert!(status.success());
    };
    copy(
        &dataset_dir.join("field_and_source_base_xds"),
        &dataset_dir.join("field_and_source_shifted_xds"),
    );
    shift_phase_centres(
        &store,
        &format!("/{name}/field_and_source_shifted_xds"),
        1e-3,
    );
    edit_json(&dataset_dir.join("zarr.json"), |value| {
        let groups = &mut value["attributes"]["data_groups"];
        let mut shifted = groups["base"].clone();
        shifted["field_and_source"] = "field_and_source_shifted_xds".into();
        groups["shifted"] = shifted;
    });
    let v4 = Msv4Observation::open(&store).expect("open");
    let catalogue = v4.catalogue();
    assert_eq!(catalogue.fields.len(), 2, "same name, different centres");
    assert_eq!(catalogue.fields[0].name, catalogue.fields[1].name);
    assert_ne!(
        catalogue.fields[0].phase_centre.as_angles(),
        catalogue.fields[1].phase_centre.as_angles()
    );
    assert_eq!(catalogue.partitions[0].data_groups.len(), 2);
    for (group, field) in [("base", FieldId(0)), ("shifted", FieldId(1))] {
        let grouped = AccessRequest {
            data_group: group.to_string(),
            grouping: Grouping::TimeBins {
                width_s: 60.0,
                combine_fields: false,
                combine_scans: false,
            },
            ..request(0..8, usize::MAX)
        };
        let mut traversal = v4.bind_read(&grouped).expect("bind");
        let mut buffers = BatchBuffers::default();
        let Some(ReadEvent::Batch(batch)) = traversal.next(&mut buffers).expect("read") else {
            panic!("a batch");
        };
        assert_eq!(batch.sample(0).field, field, "{group}");
        assert_eq!(batch.group.entry.name, group);
        assert_eq!(batch.fragments[0].key.field, Some(field), "{group}");
    }
    edit_json(
        &dataset_dir.join("field_and_source_shifted_xds/FIELD_PHASE_CENTER_DIRECTION/zarr.json"),
        |value| value["attributes"]["frame"] = "icrs".into(),
    );
    assert!(matches!(
        Msv4Observation::open(&store),
        Err(ObservationError::Unsupported { .. })
    ));
}

/// Copy directory `from` to `to`, recursively.
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("dir");
    for entry in std::fs::read_dir(from)
        .expect("read")
        .filter_map(Result::ok)
    {
        let target = to.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).expect("copy");
        }
    }
}

/// A centroid split into `TIME_CENTROID` and a nonzero
/// `TIME_CENTROID_EXTRA_PRECISION` cannot travel as one `f64` offset per
/// sample: a request for centroids is refused rather than handed the
/// primary component alone, and a request without them reads.
#[test]
fn a_split_msv4_centroid_is_refused_not_truncated() {
    let root = tempfile::tempdir().expect("root");
    let store = fixture(root.path(), "ms_trial.ps.zarr");
    let dataset = dataset(&store);
    // The copy's residuals are the centroids themselves: nonzero.
    copy_dir(
        &dataset.join("TIME_CENTROID"),
        &dataset.join("TIME_CENTROID_EXTRA_PRECISION"),
    );
    let v4 = Msv4Observation::open(&store).expect("open");
    let with = request(0..8, usize::MAX);
    for result in [
        v4.estimate(&with).map(|_| ()),
        v4.bind_read(&with).map(|_| ()),
    ] {
        assert!(
            matches!(result, Err(ObservationError::Unsupported { what }) if what.contains("EXTRA_PRECISION")),
            "{result:?}"
        );
    }
    let without = AccessRequest {
        roles: Roles {
            time_centroid: false,
            ..Roles::ALL
        },
        ..with
    };
    let mut traversal = v4.bind_read(&without).expect("bind without centroids");
    let mut buffers = BatchBuffers::default();
    assert!(traversal.next(&mut buffers).expect("read").is_some());
}

/// Write an uncompressed `fixed_length_utf32` array of two-character
/// `labels`, in one chunk, at `dir`.
fn write_labels(dir: &Path, shape: &[usize], dimensions: &[&str], labels: &[&str]) {
    assert_eq!(labels.len(), shape.iter().product::<usize>());
    let metadata = serde_json::json!({
        "shape": shape,
        "data_type": {"name": "fixed_length_utf32", "configuration": {"length_bytes": 8}},
        "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": shape}},
        "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
        "fill_value": "",
        "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
        "attributes": {},
        "dimension_names": dimensions,
        "zarr_format": 3,
        "node_type": "array",
        "storage_transformers": [],
    });
    let _ = std::fs::remove_dir_all(dir);
    let chunk = shape.iter().fold(dir.join("c"), |path, _| path.join("0"));
    std::fs::create_dir_all(chunk.parent().expect("parent")).expect("dir");
    std::fs::write(
        dir.join("zarr.json"),
        serde_json::to_string_pretty(&metadata).expect("json"),
    )
    .expect("write");
    let bytes: Vec<u8> = labels
        .iter()
        .flat_map(|label| {
            assert_eq!(label.chars().count(), 2);
            label.chars().flat_map(|c| u32::from(c).to_le_bytes())
        })
        .collect();
    std::fs::write(chunk, bytes).expect("write");
}

/// XRADIO's `polarization_mixed[baseline_id, polarization]` gives each
/// baseline its own basis under the generic labels `PP`, `PQ`, `QP`, `QQ`.
/// A partition's correlations are one list for every baseline, so two
/// stores that differ only in that mapping would be indistinguishable at
/// the boundary: both are refused at opening, and the generic labels alone
/// are not read as a basis.
#[test]
fn a_baseline_dependent_polarization_basis_is_refused() {
    const LINEAR: [&str; 4] = ["XX", "XY", "YX", "YY"];
    const CIRCULAR: [&str; 4] = ["RR", "RL", "LR", "LL"];
    for linear_baselines in [1, 5] {
        let root = tempfile::tempdir().expect("root");
        let store = fixture(root.path(), "ms_trial.ps.zarr");
        let dataset = dataset(&store);
        let baselines = Array::open(
            Arc::new(FilesystemStore::new(&dataset).expect("store")),
            "/baseline_id",
        )
        .expect("baseline_id")
        .shape()[0] as usize;
        write_labels(
            &dataset.join("polarization"),
            &[4],
            &["polarization"],
            &["PP", "PQ", "QP", "QQ"],
        );
        let mapping: Vec<&str> = (0..baselines)
            .flat_map(|baseline| {
                if baseline < linear_baselines {
                    LINEAR
                } else {
                    CIRCULAR
                }
            })
            .collect();
        write_labels(
            &dataset.join("polarization_mixed"),
            &[baselines, 4],
            &["baseline_id", "polarization"],
            &mapping,
        );
        assert!(
            matches!(
                Msv4Observation::open(&store),
                Err(ObservationError::Unsupported { what }) if what.contains("polarization_mixed")
            ),
            "{linear_baselines} linear baselines"
        );
        std::fs::remove_dir_all(dataset.join("polarization_mixed")).expect("remove");
        let generic = Msv4Observation::open(&store).expect("generic labels alone");
        assert_eq!(
            generic.catalogue().partitions[0]
                .correlations
                .iter()
                .map(|correlation| correlation.label())
                .collect::<Vec<_>>(),
            ["PP", "PQ", "QP", "QQ"],
            "the written labels are read back as written"
        );
    }
}

/// The validity mask of every batch of `observation` read with `roles`.
fn masks(observation: &dyn casa_observation::Observation, roles: Roles) -> Vec<bool> {
    let request = AccessRequest {
        roles,
        ..request(0..8, usize::MAX)
    };
    let mut traversal = observation.bind_read(&request).expect("bind");
    let mut buffers = BatchBuffers::default();
    let (mut all, mut mask) = (Vec::new(), Vec::new());
    while let Some(event) = traversal.next(&mut buffers).expect("read") {
        if let ReadEvent::Batch(batch) = event {
            batch.validity().mask_into(&mut mask, batch.shape());
            all.extend_from_slice(&mask);
        }
    }
    all
}

/// XRADIO pads a cell no row filled with NaN values and weights and leaves
/// its `FLAG` false; the MSv2 adapter flags its padding. The MSv4 adapter
/// folds a non-finite stored weight into the delivered flags, so on either
/// store the validity rule excludes an absent acquisition whichever roles
/// are read: a flags-only read gives the mask a read of every role gives.
#[test]
fn absent_acquisitions_are_excluded_whichever_roles_are_read() {
    let flags_only = Roles {
        values: false,
        weights: false,
        uvw: false,
        time_centroid: false,
        interval: false,
        exposure: false,
        scan: false,
        ..Roles::ALL
    };
    let root = tempfile::tempdir().expect("root");
    let store = fixture(root.path(), "ms_trial.ps.zarr");
    let zarr = Arc::new(FilesystemStore::new(dataset(&store)).expect("store"));
    // Time 1, baseline 7 becomes XRADIO's padding.
    let cell = ArraySubset::new_with_ranges(&[1..2, 7..8, 0..8, 0..4]);
    let nan = num_complex::Complex32::new(f32::NAN, f32::NAN);
    let open = |name: &str| Array::open(zarr.clone(), name).expect(name);
    open("/VISIBILITY")
        .store_array_subset(&cell, vec![nan; 32])
        .expect("values");
    open("/WEIGHT")
        .store_array_subset(&cell, vec![f32::NAN; 32])
        .expect("weights");
    open("/FLAG")
        .store_array_subset(&cell, vec![false; 32])
        .expect("flags");
    let v4 = Msv4Observation::open(&store).expect("v4");
    let every = masks(&v4, Roles::ALL);
    let at = (10 + 7) * 32;
    assert!(every[at..at + 32].iter().all(|excluded| *excluded));
    assert_eq!(masks(&v4, flags_only), every, "v4: flags alone");

    let path = fixture(root.path(), "ms_trial.ms");
    let mut ms = MeasurementSet::open(&path).expect("ms");
    ms.main_table_mut().remove_rows(&[7]).expect("remove row 7");
    ms.save().expect("save");
    drop(ms);
    let v2 = Msv2Observation::open(&path).expect("padded v2");
    // `msv2_cells_no_row_fills_are_padding` shows the padded cell excluded.
    assert_eq!(
        masks(&v2, flags_only),
        masks(&v2, Roles::ALL),
        "v2: flags alone"
    );
}

/// On an MSv4 dataset without `scan_name`, a grouping keyed by scan is
/// refused at binding, and one that combines scans reads every group.
#[test]
fn a_grouping_needs_the_scans_it_keys_by() {
    let root = tempfile::tempdir().expect("root");
    let store = fixture(root.path(), "ms_trial.ps.zarr");
    std::fs::remove_dir_all(dataset(&store).join("scan_name")).expect("remove");
    let v4 = Msv4Observation::open(&store).expect("open");
    let grouped = |combine_scans| AccessRequest {
        roles: Roles {
            scan: false,
            ..Roles::ALL
        },
        grouping: Grouping::TimeBins {
            width_s: 60.0,
            combine_fields: false,
            combine_scans,
        },
        ..request(0..8, usize::MAX)
    };
    assert!(matches!(
        v4.bind_read(&grouped(false)),
        Err(ObservationError::InvalidRequest { .. })
    ));
    for grouping in [
        Grouping::PerScan {
            combine_fields: true,
        },
        Grouping::PerIntegration,
    ] {
        assert!(
            v4.bind_read(&AccessRequest {
                grouping,
                ..grouped(false)
            })
            .is_err(),
            "{grouping:?}"
        );
    }
    let mut traversal = v4.bind_read(&grouped(true)).expect("bind");
    let mut buffers = BatchBuffers::default();
    let mut fragments = 0;
    while let Some(event) = traversal.next(&mut buffers).expect("read") {
        if let ReadEvent::Batch(batch) = event {
            fragments += batch.fragments.len();
            assert!(
                batch
                    .fragments
                    .iter()
                    .all(|fragment| fragment.key.scan.is_none())
            );
        }
    }
    assert!(fragments > 0);
}

/// A group of the processing set that is not a visibility dataset, and a
/// dataset of another schema version, are refused as unsupported, though
/// opening reads every dataset's antennas before the datasets themselves.
#[test]
fn other_datasets_and_schema_versions_are_refused() {
    let root = tempfile::tempdir().expect("root");
    let store = fixture(root.path(), "ms_trial.ps.zarr");
    let notes = store.join("notes");
    std::fs::create_dir(&notes).expect("a group");
    std::fs::write(
        notes.join("zarr.json"),
        r#"{"zarr_format": 3, "node_type": "group", "attributes": {"type": "notes"}}"#,
    )
    .expect("its metadata");
    assert!(
        matches!(
            Msv4Observation::open(&store),
            Err(ObservationError::Unsupported { what }) if what.contains("visibility")
        ),
        "a group that is not a visibility dataset"
    );
    std::fs::remove_dir_all(&notes).expect("no group");
    edit_json(&dataset(&store).join("zarr.json"), |value| {
        value["attributes"]["schema_version"] = "3.0.0".into();
    });
    assert!(
        matches!(
            Msv4Observation::open(&store),
            Err(ObservationError::Unsupported { what }) if what.contains("schema version")
        ),
        "another schema version"
    );
}
