// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateSystem, LinearCoordinate};

#[test]
fn a_linear_coordinate_keeps_its_scale_and_reference_through_a_record() {
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(
        LinearCoordinate::new(1, vec!["offset".into()], vec!["m".into()])
            .with_reference_pixel(vec![2.0])
            .with_reference_value(vec![10.0])
            .with_increment(vec![-3.0]),
    );
    let restored = CoordinateSystem::from_record(&coordinates.to_record()).unwrap();
    assert_eq!(restored.to_world(&[5.0]).unwrap(), vec![1.0]);
    assert_eq!(restored.to_pixel(&[1.0]).unwrap(), vec![5.0]);
}
