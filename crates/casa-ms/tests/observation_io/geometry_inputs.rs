// SPDX-License-Identifier: LGPL-3.0-or-later
//! Geometry's inputs on the boundary (#932, step 3a;
//! `docs/measurement-set/imaging-port.md` §2.2.1): what imaging's per-row
//! geometry reads from a MeasurementSet today, carried by both stores.

use std::path::Path;

use casa_ms::observation_io::{OpenOptions, open};
use casa_ms::{MeasurementSet, SubtableId};
use casa_observation::ObservationError;
use casa_tables::ColumnSchema;
use casa_tables::table_measures::{MeasureType, TableMeasDesc, default_direction_ref_map};
use casa_types::measures::EpochRef;
use casa_types::measures::direction::DirectionRef;
use casa_types::{PrimitiveType, ScalarValue, Value};

use super::fields::mosaic;

/// Give the MSv2 at `path` a variable `PHASE_DIR` reference, as ALMA's
/// `PhaseDir_Ref`: an integer column holding each field's casacore code.
fn variable_phase_references(path: &Path, frames: impl Fn(usize) -> DirectionRef) {
    let mut ms = MeasurementSet::open(path).expect("ms");
    let field = ms.subtable_mut(SubtableId::Field).expect("FIELD");
    field
        .add_column(
            ColumnSchema::scalar("PhaseDir_Ref", PrimitiveType::Int32),
            Some(Value::Scalar(ScalarValue::Int32(0))),
        )
        .expect("column");
    for row in 0..field.row_count() {
        let code = frames(row).casacore_code();
        field
            .fill_column_range(
                "PhaseDir_Ref",
                casa_tables::RowRange::new(row, row + 1),
                Value::Scalar(ScalarValue::Int32(code)),
            )
            .expect("code");
    }
    let (types, codes) = default_direction_ref_map();
    TableMeasDesc::new_variable_int(
        "PHASE_DIR",
        MeasureType::Direction,
        "PhaseDir_Ref",
        types,
        codes,
    )
    .expect("measure")
    .write(field)
    .expect("write");
    ms.save().expect("save");
}

/// Each field's phase centre keeps its stored angles and resolves its frame
/// per row from a variable reference column, and carries `FIELD.TIME` as
/// its reference epoch; fields in different frames are refused. An MSv4
/// field has no reference epoch: its frame is celestial.
#[test]
fn field_directions_resolve_variable_frames_and_carry_reference_epochs() {
    let root = tempfile::tempdir().expect("root");
    let variant = |name: &str| {
        let dir = root.path().join(name);
        std::fs::create_dir(&dir).expect("dir");
        mosaic(&dir, 4)
    };
    let (fixed, v4) = variant("fixed");
    let stored = open(&fixed, &OpenOptions::default()).expect("open");
    let stored = stored.catalogue().fields.clone();
    assert_eq!(stored.len(), 3);
    let times: Vec<f64> = {
        let ms = MeasurementSet::open(&fixed).expect("ms");
        let field = ms.field().expect("FIELD");
        (0..field.row_count())
            .map(|row| field.time(row).expect("TIME"))
            .collect()
    };
    for (field, time_s) in stored.iter().zip(&times) {
        let epoch = field.reference_epoch.as_ref().expect("FIELD.TIME");
        // As the imaging engine states it (`MEpoch::from_mjd(TIME / 86400)`).
        assert_eq!(epoch.value().as_mjd(), time_s / 86_400.0);
        assert_eq!(epoch.refer(), EpochRef::UTC);
    }

    let (path, _) = variant("variable");
    variable_phase_references(&path, |_| DirectionRef::J2000);
    let opened = open(&path, &OpenOptions::default()).expect("variable");
    for (read, field) in opened.catalogue().fields.iter().zip(&stored) {
        assert_eq!(read.phase_centre.refer(), DirectionRef::J2000);
        assert_eq!(
            read.phase_centre.as_angles(),
            field.phase_centre.as_angles()
        );
    }

    let (path, _) = variant("mixed");
    variable_phase_references(&path, |row| {
        if row == 0 {
            DirectionRef::J2000
        } else {
            DirectionRef::ICRS
        }
    });
    assert!(matches!(
        open(&path, &OpenOptions::default()),
        Err(ObservationError::Unsupported { .. })
    ));

    let v4 = open(&v4, &OpenOptions::default()).expect("msv4");
    assert!(
        v4.catalogue()
            .fields
            .iter()
            .all(|field| field.reference_epoch.is_none())
    );
}
