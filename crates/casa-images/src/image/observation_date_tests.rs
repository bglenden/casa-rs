// SPDX-License-Identifier: LGPL-3.0-or-later
//! Legacy observation records retain the full time reference when edited.

use super::*;
use casa_coordinates::LinearCoordinate;
use casa_types::measures::{EpochRef, MEpoch};

#[test]
fn legacy_observation_date_is_replaced_with_the_full_epoch() {
    for nested in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("date.im");
        let mut coords = CoordinateSystem::new();
        coords.add_coordinate(LinearCoordinate::new(1, vec!["x".into()], vec!["m".into()]));
        let mut image = PagedImage::<f32>::create(vec![4], coords, &path).unwrap();
        let Some(Value::Record(coords)) = image.table.keywords_mut().get_mut("coords") else {
            panic!("missing coordinates")
        };
        if nested {
            let mut obs = RecordValue::default();
            obs.upsert(
                "observer",
                Value::Scalar(ScalarValue::String("observer".into())),
            );
            obs.upsert("date", Value::Scalar(ScalarValue::Float64(50000.0)));
            coords.upsert("obsinfo", Value::Record(obs));
        } else {
            coords.upsert("date", Value::Scalar(ScalarValue::Float64(50000.0)));
        }
        image.save().unwrap();
        drop(image);
        let mut image = PagedImage::<f32>::open(&path).unwrap();
        image
            .set_observation_date(MEpoch::from_mjd(59000.25, EpochRef::TAI))
            .unwrap();
        image.save().unwrap();
        drop(image);
        let image = PagedImage::<f32>::open(&path).unwrap();
        let epoch = image.coordinates().obs_info().date.as_ref().unwrap();
        assert_eq!(epoch.refer(), EpochRef::TAI);
        assert_eq!(epoch.value().as_mjd(), 59000.25);
        if nested {
            assert_eq!(image.coordinates().obs_info().observer, "observer");
        }
        let Some(Value::Record(coords)) = image.table.keywords().get("coords") else {
            panic!("missing coordinates")
        };
        let obs = if nested {
            let Some(Value::Record(obs)) = coords.get("obsinfo") else {
                panic!("missing obsinfo")
            };
            obs
        } else {
            coords
        };
        assert!(obs.get("date").is_none());
        assert!(obs.get("obsdate").is_some());
    }
}
