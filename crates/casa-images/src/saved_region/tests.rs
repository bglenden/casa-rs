// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::{ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};

use super::*;

fn direction_coords() -> CoordinateSystem {
    let mut coords = CoordinateSystem::new();
    coords.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.0, std::f64::consts::FRAC_PI_4],
        [-1e-4, 1e-4],
        [1.0, 1.0],
    ));
    coords
}

fn direction_spectral_coords() -> CoordinateSystem {
    let mut coords = direction_coords();
    coords.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        1.42e9,
        1.0e6,
        0.0,
        1.42040575e9,
    ));
    coords
}

/// World vertices on the direction axes for pixel corners in the first plane.
fn world_polygon(coords: &CoordinateSystem, pixels: &[[f64; 2]]) -> Vec<[f64; 2]> {
    pixels
        .iter()
        .map(|&[x, y]| {
            let mut pixel = vec![0.0; coords.n_pixel_axes()];
            pixel[0] = x;
            pixel[1] = y;
            let world = coords.to_world(&pixel).unwrap();
            [world[0], world[1]]
        })
        .collect()
}

fn region_with(coords: &CoordinateSystem, polygons: &[&[[f64; 2]]]) -> PolygonRegion {
    let mut region = PolygonRegion::new(coords, [0, 1]).unwrap();
    for pixels in polygons {
        region.add_polygon(world_polygon(coords, pixels)).unwrap();
    }
    region
}

fn ramp_image(path: &std::path::Path) -> PagedImage<f32> {
    let values = (0..5)
        .flat_map(|x| (0..5).map(move |y| (x * 10 + y) as f32))
        .collect::<Vec<_>>();
    let mut image = PagedImage::<f32>::create(vec![5, 5], direction_coords(), path).unwrap();
    image
        .put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[5, 5]), values).unwrap(),
            &[0, 0],
        )
        .unwrap();
    image.set_units("Jy/beam").unwrap();
    image.save().unwrap();
    image
}

fn string_value(record: &RecordValue, field: &str) -> Option<String> {
    match record.get(field) {
        Some(Value::Scalar(ScalarValue::String(value))) => Some(value.clone()),
        _ => None,
    }
}

#[test]
fn region_statistics_follow_polygon_membership() {
    let dir = tempfile::tempdir().unwrap();
    let image = ramp_image(&dir.path().join("region.image"));
    let region = region_with(
        image.coordinates(),
        &[&[[1.0, 1.0], [3.0, 1.0], [3.0, 3.0], [1.0, 3.0]]],
    );

    let stats = image.region_statistics(&region).unwrap().unwrap();
    assert_eq!(stats.pixel_count, 9);
    assert_eq!(stats.median, 22.0);
    assert_eq!(stats.min, 11.0);
    assert_eq!(stats.max, 33.0);
    assert!((stats.mean - 22.0).abs() < 1e-9);
    assert!((stats.sigma - (606.0f64 / 9.0).sqrt()).abs() < 1e-9);
    assert!((stats.rms - (4962.0f64 / 9.0).sqrt()).abs() < 1e-9);
    assert!((stats.sum - 198.0).abs() < 1e-9);
    assert_eq!(stats.unit, "Jy/beam");

    let polygons = region
        .plane_pixel_polygons(image.coordinates(), &[2.0, 2.0])
        .unwrap();
    assert_eq!(polygons.len(), 1);
    assert!((polygons[0][0][0] - 1.0).abs() < 1e-6);
    assert!((polygons[0][0][1] - 1.0).abs() < 1e-6);
}

#[test]
fn region_statistics_respect_image_mask_and_finite_values() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("region-masked-stats.image");
    let mut image = PagedImage::<f32>::create(vec![3, 3], direction_coords(), &path).unwrap();
    image
        .put_slice(
            &ArrayD::from_shape_vec(
                IxDyn(&[3, 3]),
                vec![1.0, 2.0, 3.0, 4.0, f32::NAN, 6.0, 7.0, 8.0, 9.0],
            )
            .unwrap(),
            &[0, 0],
        )
        .unwrap();
    image
        .put_mask(
            "mask0",
            &ArrayD::from_shape_vec(
                IxDyn(&[3, 3]),
                vec![true, true, true, true, false, true, true, true, true],
            )
            .unwrap(),
        )
        .unwrap();
    image.set_default_mask("mask0").unwrap();
    image.save().unwrap();

    let region = region_with(
        image.coordinates(),
        &[&[[0.0, 0.0], [2.0, 0.0], [2.0, 2.0], [0.0, 2.0]]],
    );
    let stats = image.region_statistics(&region).unwrap().unwrap();
    assert_eq!(stats.pixel_count, 8);
    assert_eq!(stats.min, 1.0);
    assert_eq!(stats.max, 9.0);
    assert_eq!(stats.median, 5.0);
    assert!((stats.mean - 5.0).abs() < 1e-9);
    assert!((stats.sigma - 2.738_612_787_525_830_6).abs() < 1e-12);
    assert!((stats.rms - 5.700_877_125_495_69).abs() < 1e-12);
    assert!((stats.sum - 40.0).abs() < 1e-9);
}

#[test]
fn polygons_cover_every_plane_of_a_cube() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("region-cube.image");
    let coords = direction_spectral_coords();
    let mut image = PagedImage::<f32>::create(vec![2, 2, 3], coords.clone(), &path).unwrap();
    image
        .put_slice(
            &ArrayD::from_shape_vec(
                IxDyn(&[2, 2, 3]),
                vec![
                    1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                ],
            )
            .unwrap(),
            &[0, 0, 0],
        )
        .unwrap();
    image.save().unwrap();

    let whole = region_with(
        &coords,
        &[&[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0]]],
    );
    let stats = image.region_statistics(&whole).unwrap().unwrap();
    assert_eq!(stats.pixel_count, 12);
    assert!((stats.sum - 1110.0).abs() < 1e-9);

    let corner = region_with(&coords, &[&[[0.0, 0.0], [0.4, 0.0], [0.0, 0.4]]]);
    let mask = corner.mask(&coords, image.shape()).unwrap();
    assert_eq!(mask.iter().filter(|&&inside| inside).count(), 3);
    for channel in 0..3 {
        assert!(mask[[0, 0, channel]]);
        assert!(!mask[[1, 1, channel]]);
    }
    let stats = image.region_statistics(&corner).unwrap().unwrap();
    assert_eq!(stats.pixel_count, 3);
    assert!((stats.sum - 111.0).abs() < 1e-9);
}

#[test]
fn a_region_outside_the_image_selects_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let image = ramp_image(&dir.path().join("outside.image"));
    let region = region_with(
        image.coordinates(),
        &[&[[10.0, 10.0], [12.0, 10.0], [12.0, 12.0]]],
    );
    assert!(image.region_statistics(&region).unwrap().is_none());
    let mask = region.mask(image.coordinates(), image.shape()).unwrap();
    assert!(!mask.iter().any(|&inside| inside));
}

#[test]
fn put_region_mask_persists_named_default_mask() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("region-mask.image");
    let mut image = PagedImage::<f32>::create(vec![5, 5], direction_coords(), &path).unwrap();
    let region = region_with(
        image.coordinates(),
        &[&[[1.0, 1.0], [3.0, 1.0], [3.0, 3.0], [1.0, 3.0]]],
    );
    image.put_region_mask("roi", &region, true).unwrap();
    image.save().unwrap();

    let reopened = PagedImage::<f32>::open(&path).unwrap();
    assert_eq!(reopened.default_mask_name().as_deref(), Some("roi"));
    let mask = reopened.get_named_mask("roi").unwrap();
    assert!(!mask[IxDyn(&[0, 0])]);
    assert!(mask[IxDyn(&[1, 1])]);
    assert!(mask[IxDyn(&[2, 2])]);
    assert!(mask[IxDyn(&[3, 3])]);
    assert!(!mask[IxDyn(&[4, 4])]);
    assert_eq!(mask.iter().filter(|&&inside| inside).count(), 9);
}

#[test]
fn saved_region_round_trips_single_polygon() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("saved-region.image");
    let mut image = PagedImage::<f32>::create(vec![5, 5], direction_coords(), &path).unwrap();
    let region = region_with(
        image.coordinates(),
        &[&[[1.0, 1.0], [3.0, 1.0], [2.0, 3.0]]],
    );
    image.put_polygon_region("Region 1", &region).unwrap();
    image.save().unwrap();

    let reopened = PagedImage::<f32>::open(&path).unwrap();
    assert_eq!(reopened.region_names(), vec!["Region 1".to_string()]);
    let record = reopened.get_region_record("Region 1").unwrap();
    assert_eq!(string_value(&record, "name").as_deref(), Some("WCPolygon"));

    let loaded = reopened.get_polygon_region("Region 1").unwrap();
    assert_eq!(loaded, region);
    assert_eq!(loaded.units, ["rad".to_string(), "rad".to_string()]);
}

#[test]
fn saved_region_round_trips_union_of_polygons() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("saved-region-union.image");
    let mut image = PagedImage::<f32>::create(vec![6, 6], direction_coords(), &path).unwrap();
    let region = region_with(
        image.coordinates(),
        &[
            &[[1.0, 1.0], [2.0, 1.0], [1.0, 2.0]],
            &[[4.0, 4.0], [5.0, 4.0], [4.0, 5.0]],
        ],
    );
    image.put_polygon_region("pair", &region).unwrap();
    image.save().unwrap();

    let reopened = PagedImage::<f32>::open(&path).unwrap();
    let record = reopened.get_region_record("pair").unwrap();
    assert_eq!(string_value(&record, "name").as_deref(), Some("WCUnion"));
    let loaded = reopened.get_polygon_region("pair").unwrap();
    assert_eq!(loaded.polygons.len(), 2);
    assert_eq!(loaded, region);
}

#[test]
fn add_polygon_rejects_short_and_crossing_polygons() {
    let coords = direction_coords();
    let mut region = PolygonRegion::new(&coords, [0, 1]).unwrap();
    let short = region
        .add_polygon(world_polygon(&coords, &[[1.0, 1.0], [3.0, 1.0]]))
        .unwrap_err();
    assert!(short.to_string().contains("at least 3 vertices"));
    let crossing = region
        .add_polygon(world_polygon(
            &coords,
            &[[0.0, 0.0], [2.0, 2.0], [0.0, 2.0], [2.0, 0.0]],
        ))
        .unwrap_err();
    assert!(crossing.to_string().contains("self-intersecting"));
    assert!(region.polygons.is_empty());
    assert!(region.to_record(&coords).is_err());
}

#[test]
fn region_axes_must_exist_in_the_coordinate_system() {
    let coords = direction_coords();
    assert!(PolygonRegion::new(&coords, [0, 0]).is_err());
    assert!(PolygonRegion::new(&coords, [0, 2]).is_err());
    let region = region_with(&coords, &[&[[0.0, 0.0], [2.0, 0.0], [0.0, 2.0]]]);
    let error = region.mask(&coords, &[5, 5, 3]).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("coordinate system for every image axis")
    );
}

fn region_record(region_type: i32, class_name: &str) -> RecordValue {
    let mut record = RecordValue::default();
    record.upsert("isRegion", Value::Scalar(ScalarValue::Int32(region_type)));
    record.upsert(
        "name",
        Value::Scalar(ScalarValue::String(class_name.into())),
    );
    record.upsert("comment", Value::Scalar(ScalarValue::String(String::new())));
    record
}

#[test]
fn saved_region_rejects_unsupported_world_classes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("saved-region-unsupported.image");
    let mut image = PagedImage::<f32>::create(vec![5, 5], direction_coords(), &path).unwrap();
    image
        .put_region_record("box", &region_record(2, "WCBox"))
        .unwrap();
    image.save().unwrap();

    let error = image.get_polygon_region("box").unwrap_err();
    assert!(error.to_string().contains("saved region 'box'"));
    assert!(error.to_string().contains("WCBox"));
}

#[test]
fn saved_region_rejects_array_slicers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("saved-region-array-slicer.image");
    let mut image = PagedImage::<f32>::create(vec![5, 5], direction_coords(), &path).unwrap();
    image
        .put_region_record("slicer", &region_record(3, "LCSlicer"))
        .unwrap();
    image.save().unwrap();

    let error = image.get_polygon_region("slicer").unwrap_err();
    assert!(error.to_string().contains("array-slicer"));
    assert!(error.to_string().contains("LCSlicer"));
}

#[test]
fn a_union_of_polygons_on_different_axes_is_rejected() {
    let coords = direction_spectral_coords();
    let first = region_with(&coords, &[&[[0.0, 0.0], [1.0, 0.0], [0.0, 1.0]]]);
    let mut second = PolygonRegion::new(&coords, [0, 2]).unwrap();
    second
        .add_polygon(vec![[0.0, 1.42e9], [1e-4, 1.42e9], [0.0, 1.421e9]])
        .unwrap();

    let mut children = RecordValue::default();
    children.upsert("*1", Value::Record(first.to_record(&coords).unwrap()));
    children.upsert("*2", Value::Record(second.to_record(&coords).unwrap()));
    children.upsert("nr", Value::Scalar(ScalarValue::Int32(2)));
    let mut union = region_record(2, "WCUnion");
    union.upsert("regions", Value::Record(children));

    let error = PolygonRegion::from_record("mixed", &union, &coords).unwrap_err();
    assert!(error.to_string().contains("saved region 'mixed'"));
    assert!(error.to_string().contains("pixel axes"));
}
