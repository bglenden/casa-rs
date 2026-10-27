// SPDX-License-Identifier: LGPL-3.0-or-later
//! A rotated direction coordinate through an image on disk.
//!
//! casacore stores a coordinate's linear transform as the transpose of the
//! FITS PC matrix (`LinearXform`), column by column. casa-rs wrote those
//! bytes but read them back as the PC itself, so an image with a rotated
//! (non-symmetric) PC read back rotated the other way, whether casa-rs or
//! CASA wrote it. Reading CASA's world coordinates from casacore's record is
//! `casa_coordinates::direction`'s test
//! `a_casacore_record_reads_to_casas_world_coordinates`.

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, DirectionCoordinate, Projection, ProjectionType,
    StokesCoordinate, stokes::StokesType,
};
use casa_images::PagedImage;
use casa_tables::{Table, TableOptions};
use casa_types::measures::direction::DirectionRef;
use casa_types::{ArrayValue, Value};

fn rotated() -> CoordinateSystem {
    let arcsec = std::f64::consts::PI / 648_000.0;
    let (s, c) = 0.3f64.sin_cos();
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(
        DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [0.6, -0.4],
            [-0.2 * arcsec, 0.2 * arcsec],
            [16.0, 17.0],
        )
        .with_pc_matrix(ndarray::array![[c, -s], [s, c]]),
    );
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    coordinates
}

#[test]
fn a_rotated_image_reads_back_with_its_world_coordinates() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rotated.image");
    let coordinates = rotated();
    let mut image = PagedImage::<f32>::create(vec![33, 35, 1], coordinates.clone(), &path).unwrap();
    image.save().unwrap();
    drop(image);

    let image = PagedImage::<f32>::open(&path).unwrap();
    let (CoordinateModel::Direction(ours), CoordinateModel::Direction(written)) =
        (image.coordinates().coordinate(0), coordinates.coordinate(0))
    else {
        panic!("direction coordinates");
    };
    assert_eq!(ours.pc_matrix(), written.pc_matrix());
    for pixel in [
        [0.0, 0.0, 0.0],
        [32.0, 0.0, 0.0],
        [0.0, 34.0, 0.0],
        [10.5, 3.25, 0.0],
    ] {
        assert_eq!(
            image.coordinates().to_world(&pixel).unwrap(),
            coordinates.to_world(&pixel).unwrap(),
            "{pixel:?}"
        );
    }
}

/// The stored elements are the bytes casa-rs wrote before the fix, which
/// CASA 6.7.6 reads to casa-rs's world coordinates (checked with
/// `coordsys.toworld` on such an image, #952).
#[test]
fn the_stored_matrix_is_casacores_column_by_column() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rotated.image");
    let mut image = PagedImage::<f32>::create(vec![33, 35, 1], rotated(), &path).unwrap();
    image.save().unwrap();
    drop(image);

    let table = Table::open(TableOptions::new(&path)).unwrap();
    let Some(Value::Record(coords)) = table.keywords().get("coords") else {
        panic!("a coords keyword");
    };
    let Some(Value::Record(direction)) = coords.get("direction0") else {
        panic!("a direction coordinate");
    };
    let Some(Value::Array(ArrayValue::Float64(matrix))) = direction.get("pc") else {
        panic!("a pc matrix");
    };
    let (s, c) = 0.3f64.sin_cos();
    // casacore's matrix is the PC transposed, [[c, s], [-s, c]] ...
    assert_eq!(
        matrix.iter().copied().collect::<Vec<_>>(),
        vec![c, s, -s, c]
    );
    // ... and read from disk column by column, the PC's rows.
    assert_eq!(matrix.as_slice_memory_order().unwrap(), &[c, -s, s, c]);
}
