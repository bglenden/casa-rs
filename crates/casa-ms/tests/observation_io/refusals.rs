// SPDX-License-Identifier: LGPL-3.0-or-later
//! What the adapters refuse, or resolve, on stores that differ from the
//! fixtures in one deliberate way: each case edits a copy of a fixture's
//! metadata and checks that the native views stay true or binding fails.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_ms::MeasurementSet;
use casa_ms::observation_io::{Msv2Observation, Msv4Observation, Msv4Writer};
use casa_observation::{
    AccessRequest, BatchBuffers, FieldId, Grouping, ObservationError, PartitionId, ReadEvent,
    Roles, TimeBaselineGrid,
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
    let mut traversal = v2.bind_read(&request(0..8, usize::MAX)).expect("bind");
    let grid = TimeBaselineGrid::from_cursor(traversal.cursors[0].as_mut()).expect("grid");
    assert_eq!(grid.time_scale, EpochRef::TAI);
    assert!(
        matches!(
            Msv4Writer::create(
                &root.path().join("tai.ps.zarr"),
                v2.catalogue(),
                PartitionId(0),
                "base",
                &grid,
                &storage(),
            ),
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
