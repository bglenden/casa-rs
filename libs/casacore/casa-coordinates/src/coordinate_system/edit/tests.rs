// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::{
    LinearCoordinate, SpectralCoordinate, StokesCoordinate, StokesType, TabularCoordinate,
};

#[test]
fn direction_frame_edit_preserves_an_implicit_conversion_frame() {
    use crate::{DirectionCoordinate, Projection, ProjectionType};
    use casa_core::measures::direction::DirectionRef;
    let mut original = CoordinateSystem::new();
    original.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.0, 0.0],
        [-1e-4, 1e-4],
        [2.0, 2.0],
    ));
    let mut record = original.to_record();
    let Some(Value::Record(direction)) = record.get_mut("direction0") else {
        panic!("direction")
    };
    direction.remove("conversionSystem");
    let (updated, old) =
        CoordinateSystem::set_direction_reference_in_record(&mut record, "GAL").unwrap();
    assert_eq!(old.as_deref(), Some("J2000"));
    let crate::CoordinateModel::Direction(direction) = updated.coordinate(0) else {
        panic!("direction")
    };
    assert_eq!(direction.direction_ref(), DirectionRef::GALACTIC);
    assert_eq!(direction.conversion_direction_ref(), DirectionRef::J2000);
    let restored = CoordinateSystem::from_record(&record).unwrap();
    let crate::CoordinateModel::Direction(direction) = restored.coordinate(0) else {
        panic!("direction")
    };
    assert_eq!(direction.conversion_direction_ref(), DirectionRef::J2000);
    let before = record.clone();
    assert!(
        CoordinateSystem::set_direction_reference_in_record(&mut record, "JMEAN")
            .unwrap_err()
            .to_string()
            .contains("#977")
    );
    assert_eq!(record, before);
}

#[test]
fn projection_edit_refuses_multiple_direction_coordinates_without_mutation() {
    use crate::{DirectionCoordinate, Projection, ProjectionType};
    use casa_core::measures::direction::DirectionRef;
    let direction = || {
        DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [0.0, 0.0],
            [-0.01, 0.01],
            [0.0, 0.0],
        )
    };
    let mut system = CoordinateSystem::new();
    system.add_coordinate(direction());
    system.add_coordinate(direction());
    let mut record = system.to_record();
    let before = record.clone();
    let error =
        CoordinateSystem::set_direction_projection_in_record(&mut record, "TAN").unwrap_err();
    assert!(error.to_string().contains("#977"));
    assert_eq!(record, before);
}

#[test]
fn unit_edits_select_and_rescale_the_active_rest_frequency() {
    assert_eq!(
        crate::spectral::closest_rest_frequency(&[2., 3.], 2.5),
        Some(0)
    );
    let mut system = CoordinateSystem::new();
    system.add_coordinate(
        SpectralCoordinate::new(
            casa_core::measures::frequency::FrequencyRef::LSRK,
            1.,
            1.,
            0.,
            1.,
        )
        .with_rest_frequencies(vec![1., 2., 3.])
        .with_unit("GHz"),
    );
    let mut record = system.to_record();
    let Some(Value::Record(spectral)) = record.get_mut("spectral0") else {
        panic!("missing spectral coordinate")
    };
    spectral.upsert("restfreq", Value::Scalar(ScalarValue::Float64(2.1)));
    let (updated, old) = CoordinateSystem::set_axis_unit_in_record(&mut record, 0, "Hz").unwrap();
    assert_eq!(old, "GHz");
    let crate::CoordinateModel::Spectral(spectral) = updated.coordinate(0) else {
        panic!("not spectral")
    };
    assert_eq!(spectral.rest_frequency(), 2e9);
    assert_eq!(spectral.rest_frequencies(), &[1e9, 2e9, 3e9]);
    let Some(Value::Record(spectral)) = record.get_mut("spectral0") else {
        unreachable!()
    };
    spectral.upsert("restfreq", Value::Scalar(ScalarValue::Float64(-1.)));
    let before = record.clone();
    assert!(CoordinateSystem::set_axis_unit_in_record(&mut record, 0, "Hz").is_err());
    assert_eq!(record, before);
}

#[test]
fn rest_frequency_edits_preserve_other_entries_and_reject_bad_lists() {
    let mut system = CoordinateSystem::new();
    system.add_coordinate(
        SpectralCoordinate::new(
            casa_core::measures::frequency::FrequencyRef::LSRK,
            1.,
            1.,
            0.,
            1.,
        )
        .with_rest_frequencies(vec![1., 2., 3.]),
    );
    let mut record = system.to_record();
    let Some(Value::Record(spectral)) = record.get_mut("spectral0") else {
        panic!("missing spectral coordinate")
    };
    spectral.upsert("restfreq", Value::Scalar(ScalarValue::Float64(2.)));
    let (updated, old) = CoordinateSystem::set_rest_frequency_in_record(&mut record, 4.).unwrap();
    assert_eq!(old, 2.);
    let crate::CoordinateModel::Spectral(spectral) = updated.coordinate(0) else {
        panic!("not spectral")
    };
    assert_eq!(spectral.rest_frequencies(), &[1., 4., 3.]);
    for numeric in [
        Value::Array(ArrayValue::from_i32_vec(vec![1, 2])),
        Value::Scalar(ScalarValue::Int32(2)),
    ] {
        let Some(Value::Record(spectral)) = record.get_mut("spectral0") else {
            unreachable!()
        };
        spectral.upsert("restfreqs", numeric);
        spectral.upsert("restfreq", Value::Scalar(ScalarValue::Int32(2)));
        let (updated, old) =
            CoordinateSystem::set_rest_frequency_in_record(&mut record, 5.).unwrap();
        assert_eq!(old, 2.);
        let crate::CoordinateModel::Spectral(spectral) = updated.coordinate(0) else {
            panic!("not spectral")
        };
        assert_eq!(spectral.rest_frequency(), 5.);
    }
    for invalid in [
        Value::Scalar(ScalarValue::String("bad".into())),
        Value::Array(ArrayValue::Float64(
            ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[1, 2]), vec![1., 2.]).unwrap(),
        )),
    ] {
        let Some(Value::Record(spectral)) = record.get_mut("spectral0") else {
            unreachable!()
        };
        spectral.upsert("restfreqs", invalid);
        let before = record.clone();
        assert!(CoordinateSystem::set_rest_frequency_in_record(&mut record, 5.).is_err());
        assert_eq!(record, before);
    }
}

#[test]
fn tabular_axis_names_update_both_record_forms() {
    let mut system = CoordinateSystem::new();
    system.add_coordinate(TabularCoordinate::new(
        vec![0., 1.],
        vec![2., 3.],
        "original",
        "Hz",
    ));
    let mut record = system.to_record();
    let Some(Value::Record(tabular)) = record.get_mut("tabular0") else {
        panic!("missing coordinate")
    };
    tabular.upsert("name", Value::Scalar(ScalarValue::String("old".into())));
    let (system, old) = CoordinateSystem::set_axis_name_in_record(&mut record, 0, "new").unwrap();
    assert_eq!(old, "old");
    assert_eq!(system.coordinate(0).axis_names(), vec!["new"]);
    let Some(Value::Record(tabular)) = record.get_mut("tabular0") else {
        panic!("missing coordinate")
    };
    assert_eq!(
        tabular.get("axes"),
        Some(&Value::Array(ArrayValue::from_string_vec(vec![
            "new".into()
        ])))
    );
    tabular.remove("axes");
    let (system, old) = CoordinateSystem::set_axis_name_in_record(&mut record, 0, "last").unwrap();
    assert_eq!(old, "new");
    assert_eq!(system.coordinate(0).axis_names(), vec!["last"]);
    let (updated, old) =
        CoordinateSystem::set_reference_pixel_in_record(&mut record, 0, 9.).unwrap();
    assert_eq!(old, 0.0);
    assert_eq!(updated.coordinate(0).to_world(&[0.0]).unwrap(), vec![-7.0]);
    let mut spectral = CoordinateSystem::new();
    spectral.add_coordinate(
        SpectralCoordinate::from_tabular(
            casa_core::measures::frequency::FrequencyRef::LSRK,
            vec![0., 1.],
            vec![2., 3.],
            2.,
            1.,
            0.,
            2.,
        )
        .unwrap(),
    );
    let mut record = spectral.to_record();
    let (updated, old) =
        CoordinateSystem::set_reference_pixel_in_record(&mut record, 0, 9.).unwrap();
    assert_eq!(old, 0.0);
    assert_eq!(updated.coordinate(0).reference_pixel(), vec![0.0]);
    assert_eq!(updated.coordinate(0).to_world(&[0.0]).unwrap(), vec![-7.0]);
}

#[test]
fn reference_pixel_edits_follow_transposed_maps_and_are_atomic() {
    let mut original = CoordinateSystem::new();
    original.add_coordinate(LinearCoordinate::new(
        1,
        vec!["x".into()],
        vec!["pix".into()],
    ));
    original.add_coordinate(LinearCoordinate::new(
        1,
        vec!["y".into()],
        vec!["pix".into()],
    ));
    let mut record = original.to_record();
    for (key, axis) in [
        ("pixelmap0", 1),
        ("worldmap0", 1),
        ("pixelmap1", 0),
        ("worldmap1", 0),
    ] {
        record.upsert(key, Value::Array(ArrayValue::from_i32_vec(vec![axis])));
    }
    let maps = record.clone();
    let (updated, old) =
        CoordinateSystem::set_reference_pixel_in_record(&mut record, 0, 7.5).unwrap();
    assert_eq!(old, 0.0);
    assert_eq!(updated.coordinate(0).reference_pixel(), vec![7.5]);
    assert_eq!(updated.coordinate(1).reference_pixel(), vec![0.0]);
    for key in ["pixelmap0", "pixelmap1", "worldmap0", "worldmap1"] {
        assert_eq!(record.get(key), maps.get(key));
    }
    let before = record.clone();
    assert!(CoordinateSystem::set_reference_pixel_in_record(&mut record, 2, 1.0).is_err());
    assert_eq!(record, before);
    let mut stokes = CoordinateSystem::new();
    stokes.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    let mut record = stokes.to_record();
    let before = record.clone();
    let (unchanged, old) =
        CoordinateSystem::set_reference_pixel_in_record(&mut record, 0, 1.0).unwrap();
    assert_eq!(old, 0.0);
    assert_eq!(unchanged.pixel_axis_type(0), Some(CoordinateType::Stokes));
    assert_eq!(record, before);
}
