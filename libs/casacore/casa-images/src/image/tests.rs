// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use casa_lattices::LatticeError;
use ndarray::{Dimension, ShapeBuilder};

fn make_coords() -> CoordinateSystem {
    CoordinateSystem::new()
}

#[test]
fn restoring_beam_edits_do_not_replace_malformed_image_info() {
    let directory = tempfile::tempdir().unwrap();
    let mut image =
        PagedImage::<f32>::create(vec![2, 2], make_coords(), directory.path().join("image"))
            .unwrap();
    let invalid = Value::Scalar(ScalarValue::String("not a record".into()));
    image
        .table
        .keywords_mut()
        .upsert("imageinfo", invalid.clone());
    let beam = crate::StoredGaussianBeam::new(
        casa_core::quanta::Quantity::new(2., "arcsec").unwrap(),
        casa_core::quanta::Quantity::new(1., "arcsec").unwrap(),
        casa_core::quanta::Quantity::new(0., "deg").unwrap(),
    )
    .unwrap();
    assert!(image.stored_restoring_beam().is_err());
    assert!(image.set_stored_restoring_beam(&beam).is_err());
    assert!(image.remove_restoring_beams().is_err());
    assert_eq!(image.table.keywords().get("imageinfo"), Some(&invalid));
}

/// Rows from an origin keep their order, with casacore's `LogOrigin`
/// location and `TIME` in MJD seconds, and any pixel type can take them.
#[test]
fn history_from_an_origin_is_appended_as_casacore_logs_it() {
    let directory = tempfile::tempdir().expect("scratch");
    let path = directory.path().join("image");
    PagedImage::<Complex32>::create(vec![2, 2], make_coords(), &path)
        .and_then(|mut image| {
            image.add_history("first")?;
            image.save()
        })
        .expect("an image");
    add_image_history(&path, "tclean", &["a".to_owned(), "b".to_owned()]).expect("add");
    let image = PagedImage::<Complex32>::open(&path).expect("open");
    assert_eq!(image.history().expect("history"), ["first", "a", "b"]);
    let table = Table::open(TableOptions::new(path.join(LOGTABLE_NAME))).expect("logtable");
    let cell = |row, name| {
        table
            .cell_accessor(row, name)
            .and_then(|cell| cell.value().map(|value| value.cloned()))
            .expect("a cell")
    };
    assert_eq!(
        cell(2, "LOCATION"),
        Some(Value::Scalar(ScalarValue::String("::tclean".to_owned())))
    );
    assert_eq!(
        cell(0, "LOCATION"),
        Some(Value::Scalar(ScalarValue::String("casa-images".to_owned())))
    );
    for row in 0..3 {
        let Some(Value::Scalar(ScalarValue::Float64(time))) = cell(row, "TIME") else {
            panic!("TIME");
        };
        assert!((4.5e9..6.0e9).contains(&time), "row {row}: {time}");
    }
    // The append reaches `table.lock`, where casacore reads the row count.
    let published = || {
        TableWriteLock::acquire(path.join(LOGTABLE_NAME), 1)
            .and_then(|lock| lock.modify_counter())
            .expect("the lock's counter")
    };
    let before = published();
    add_image_history(&path, "tclean", &["c".to_owned()]).expect("add");
    assert_ne!(published(), before);
}

struct BareImage<T: ImagePixel> {
    data: ArrayD<T>,
}

impl<T: ImagePixel> BareImage<T> {
    fn new(data: ArrayD<T>) -> Self {
        Self { data }
    }
}

impl<T: ImagePixel> Lattice<T> for BareImage<T> {
    fn shape(&self) -> &[usize] {
        self.data.shape()
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        self.data
            .get(IxDyn(position))
            .copied()
            .ok_or_else(|| LatticeError::IndexOutOfBounds {
                index: position.to_vec(),
                shape: self.data.shape().to_vec(),
            })
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        if start.len() != self.ndim() || shape.len() != self.ndim() || stride.len() != self.ndim() {
            return Err(LatticeError::NdimMismatch {
                expected: self.ndim(),
                got: start.len().max(shape.len()).max(stride.len()),
            });
        }
        let mut out = ArrayD::from_elem(IxDyn(shape), T::default_value());
        for (idx, value) in out.indexed_iter_mut() {
            let src: Vec<usize> = idx
                .slice()
                .iter()
                .zip(start.iter())
                .zip(stride.iter())
                .map(|((&i, &s), &step)| s + i * step)
                .collect();
            *value = self.get_at(&src)?;
        }
        Ok(out)
    }
}

impl<T: ImagePixel> ImageInterface<T> for BareImage<T> {
    fn as_any(&self) -> Option<&dyn Any> {
        None
    }

    fn coordinates(&self) -> &CoordinateSystem {
        static COORDS: std::sync::OnceLock<CoordinateSystem> = std::sync::OnceLock::new();
        COORDS.get_or_init(CoordinateSystem::new)
    }

    fn units(&self) -> &str {
        ""
    }

    fn misc_info(&self) -> RecordValue {
        RecordValue::default()
    }

    fn image_info(&self) -> Result<ImageInfo, ImageError> {
        Ok(ImageInfo::default())
    }

    fn name(&self) -> Option<&Path> {
        None
    }
}

#[test]
fn float32_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("f32.image");
    let mut image = PagedImage::<f32>::create(vec![4, 4], make_coords(), &path).unwrap();
    image.put_at(1.5, &[1, 2]).unwrap();
    image.add_history("hello").unwrap();
    image.make_mask("mask0", true, true).unwrap();
    image.save().unwrap();

    let reopened = PagedImage::<f32>::open(&path).unwrap();
    assert_eq!(reopened.get_at(&[1, 2]).unwrap(), 1.5);
    assert_eq!(reopened.history().unwrap(), vec!["hello".to_string()]);
    assert_eq!(reopened.default_mask_name().as_deref(), Some("mask0"));
    assert_eq!(image_pixel_type(&path).unwrap(), ImagePixelType::Float32);
}

#[test]
fn put_slice_view_writes_borrowed_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("view.image");
    let mut image = PagedImage::<f32>::create(vec![4, 4], make_coords(), &path).unwrap();
    let data = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 4.0]).unwrap();

    image.put_slice_view(data.view(), &[1, 1]).unwrap();

    let observed = image
        .get_slice(&[1, 1], &[2, 2])
        .unwrap()
        .iter()
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(observed, vec![1.0, 2.0, 3.0, 4.0]);
}

#[test]
fn put_slice_view_direct_writes_aligned_tiled_plane() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("direct-plane.image");
    let mut image = PagedImage::<f32>::create_with_tile_shape_and_cache(
        vec![4, 4, 1, 2],
        vec![2, 2, 1, 1],
        make_coords(),
        &path,
        16,
    )
    .unwrap();
    let values = (0..16).map(|value| value as f32).collect::<Vec<_>>();
    let data = ArrayD::from_shape_vec(IxDyn(&[4, 4, 1, 1]), values.clone()).unwrap();

    image.put_slice_view(data.view(), &[0, 0, 0, 1]).unwrap();

    let stats = image.tiled_io_stats().expect("tile io stats");
    assert_eq!(stats.direct_tile_write_calls, 1);
    assert_eq!(stats.direct_tile_write_tiles, 4);
    assert_eq!(stats.direct_tile_write_bytes, 64);
    assert_eq!(stats.put_slice_c_order_calls, 0);
    let observed = image.get_slice(&[0, 0, 0, 1], &[4, 4, 1, 1]).unwrap();
    for x in 0..4 {
        for y in 0..4 {
            assert_eq!(observed[IxDyn(&[x, y, 0, 0])], data[IxDyn(&[x, y, 0, 0])]);
        }
    }
}

#[test]
fn put_slice_view_direct_writes_c_order_plane_groups_across_copy_blocks() {
    for (width, height, tile_x, tile_y) in [(70, 66, 70, 66), (70, 66, 35, 33)] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("direct-c-order-group.image");
        let mut image = PagedImage::<f32>::create_with_tile_shape_and_cache(
            vec![width, height, 1, 5],
            vec![tile_x, tile_y, 1, 1],
            make_coords(),
            &path,
            tile_x * tile_y * 4,
        )
        .unwrap();
        let data = ndarray::Array4::from_shape_fn((width, height, 1, 3), |(x, y, _, c)| {
            (c * 100_000 + x * 100 + y) as f32
        });
        image
            .put_slice_view(data.view().into_dyn(), &[0, 0, 0, 1])
            .unwrap();
        let stats = image.tiled_io_stats().unwrap();
        assert_eq!(stats.direct_tile_write_calls, 1);
        assert_eq!(
            stats.direct_tile_write_tiles,
            3 * (width / tile_x) * (height / tile_y)
        );
        assert_eq!(stats.direct_tile_write_bytes, width * height * 3 * 4);
        assert_eq!(stats.put_slice_c_order_calls, 0);
        image.save().unwrap();
        drop(image);
        let reopened = PagedImage::<f32>::open(&path).unwrap();
        assert_eq!(
            reopened
                .get_slice(&[0, 0, 0, 1], &[width, height, 1, 3])
                .unwrap(),
            data.into_dyn()
        );
        for channel in [0, 4] {
            assert!(
                reopened
                    .get_slice(&[0, 0, 0, channel], &[width, height, 1, 1])
                    .unwrap()
                    .iter()
                    .all(|value| *value == 0.0)
            );
        }
    }
}

#[test]
fn put_slice_view_direct_writes_fortran_aligned_tiled_plane() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("direct-fortran-plane.image");
    let mut image = PagedImage::<f32>::create_with_tile_shape_and_cache(
        vec![4, 4, 1, 2],
        vec![2, 2, 1, 1],
        make_coords(),
        &path,
        16,
    )
    .unwrap();
    let mut data = ndarray::Array4::<f32>::zeros((4, 4, 1, 1).f());
    for x in 0..4 {
        for y in 0..4 {
            data[(x, y, 0, 0)] = (x * 10 + y) as f32;
        }
    }

    image
        .put_slice_view(data.view().into_dyn(), &[0, 0, 0, 1])
        .unwrap();

    let stats = image.tiled_io_stats().expect("tile io stats");
    assert_eq!(stats.direct_tile_write_calls, 1);
    assert_eq!(stats.direct_tile_write_tiles, 4);
    assert_eq!(stats.direct_tile_write_bytes, 64);
    assert_eq!(stats.put_slice_c_order_calls, 0);
    assert_eq!(stats.put_slice_fortran_calls, 0);
    let observed = image.get_slice(&[0, 0, 0, 1], &[4, 4, 1, 1]).unwrap();
    for x in 0..4 {
        for y in 0..4 {
            assert_eq!(observed[IxDyn(&[x, y, 0, 0])], data[(x, y, 0, 0)]);
        }
    }
}

#[test]
fn put_slice_view_direct_writes_fortran_whole_plane_without_pack_copy() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("direct-fortran-whole-plane.image");
    let mut image = PagedImage::<f32>::create_with_tile_shape_and_cache(
        vec![4, 4, 1, 2],
        vec![4, 4, 1, 1],
        make_coords(),
        &path,
        16,
    )
    .unwrap();
    let mut data = ndarray::Array4::<f32>::zeros((4, 4, 1, 1).f());
    for x in 0..4 {
        for y in 0..4 {
            data[(x, y, 0, 0)] = (x * 10 + y) as f32;
        }
    }

    image
        .put_slice_view(data.view().into_dyn(), &[0, 0, 0, 1])
        .unwrap();

    let stats = image.tiled_io_stats().expect("tile io stats");
    assert_eq!(stats.direct_tile_write_calls, 1);
    assert_eq!(stats.direct_tile_write_tiles, 1);
    assert_eq!(stats.direct_tile_write_bytes, 64);
    assert_eq!(stats.direct_tile_pack_ns, 0);
    assert_eq!(stats.put_slice_c_order_calls, 0);
    assert_eq!(stats.put_slice_fortran_calls, 0);
    let observed = image.get_slice(&[0, 0, 0, 1], &[4, 4, 1, 1]).unwrap();
    for x in 0..4 {
        for y in 0..4 {
            assert_eq!(observed[IxDyn(&[x, y, 0, 0])], data[(x, y, 0, 0)]);
        }
    }
}

#[test]
fn put_slice_view_direct_writes_fortran_aligned_tiled_plane_group() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("direct-fortran-plane-group.image");
    let mut image = PagedImage::<f32>::create_with_tile_shape_and_cache(
        vec![4, 4, 1, 4],
        vec![2, 2, 1, 1],
        make_coords(),
        &path,
        16,
    )
    .unwrap();
    let mut data = ndarray::Array4::<f32>::zeros((4, 4, 1, 3).f());
    for x in 0..4 {
        for y in 0..4 {
            for channel in 0..3 {
                data[(x, y, 0, channel)] = (channel * 100 + x * 10 + y) as f32;
            }
        }
    }

    image
        .put_slice_view(data.view().into_dyn(), &[0, 0, 0, 1])
        .unwrap();

    let stats = image.tiled_io_stats().expect("tile io stats");
    assert_eq!(stats.direct_tile_write_calls, 1);
    assert_eq!(stats.direct_tile_write_tiles, 12);
    assert_eq!(stats.direct_tile_write_bytes, 192);
    assert_eq!(stats.put_slice_c_order_calls, 0);
    assert_eq!(stats.put_slice_fortran_calls, 0);
    let observed = image.get_slice(&[0, 0, 0, 1], &[4, 4, 1, 3]).unwrap();
    for x in 0..4 {
        for y in 0..4 {
            for channel in 0..3 {
                assert_eq!(
                    observed[IxDyn(&[x, y, 0, channel])],
                    data[(x, y, 0, channel)]
                );
            }
        }
    }
}

#[test]
fn float64_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("f64.image");
    let mut image = PagedImage::<f64>::create(vec![2, 2], make_coords(), &path).unwrap();
    image.set(2.5).unwrap();
    image.save().unwrap();
    let reopened = PagedImage::<f64>::open(&path).unwrap();
    assert_eq!(reopened.get_at(&[0, 0]).unwrap(), 2.5);
    assert_eq!(image_pixel_type(&path).unwrap(), ImagePixelType::Float64);
}

#[test]
fn complex32_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c32.image");
    let value = Complex32::new(1.0, -2.0);
    let mut image = PagedImage::<Complex32>::create(vec![2, 2], make_coords(), &path).unwrap();
    image.put_at(value, &[1, 1]).unwrap();
    image.save().unwrap();
    let reopened = PagedImage::<Complex32>::open(&path).unwrap();
    assert_eq!(reopened.get_at(&[1, 1]).unwrap(), value);
    assert_eq!(image_pixel_type(&path).unwrap(), ImagePixelType::Complex32);
}

#[test]
fn complex64_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c64.image");
    let value = Complex64::new(3.0, -4.0);
    let mut image = PagedImage::<Complex64>::create(vec![2, 2], make_coords(), &path).unwrap();
    image.put_at(value, &[0, 1]).unwrap();
    image.save().unwrap();
    let reopened = PagedImage::<Complex64>::open(&path).unwrap();
    assert_eq!(reopened.get_at(&[0, 1]).unwrap(), value);
    assert_eq!(image_pixel_type(&path).unwrap(), ImagePixelType::Complex64);
}

#[test]
fn temp_image_materializes_masks_and_history() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("temp.image");
    let mut image = crate::TempImage::<f32>::new(
        vec![3, 3],
        make_coords(),
        casa_lattices::TempStoragePolicy::Memory,
    )
    .unwrap();
    image.make_mask("quality", true, true).unwrap();
    image.add_history("temp").unwrap();
    let _paged = image.save_as(&path).unwrap();

    let reopened = PagedImage::<f32>::open(&path).unwrap();
    assert_eq!(reopened.default_mask_name().as_deref(), Some("quality"));
    let mask = reopened.get_mask().unwrap().unwrap();
    assert!(mask.iter().all(|&value| value));
    assert_eq!(reopened.history().unwrap(), vec!["temp".to_string()]);
}

#[test]
fn default_mask_requires_existing_name() {
    let mut image = crate::TempImage::<f32>::new(
        vec![2, 2],
        make_coords(),
        casa_lattices::TempStoragePolicy::Memory,
    )
    .unwrap();
    assert!(matches!(
        image.set_default_mask("missing"),
        Err(ImageError::MaskNotFound(_))
    ));
}

#[test]
fn open_any_dispatches_on_pixel_type() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dispatch.image");
    let mut image = PagedImage::<f64>::create(vec![2, 2], make_coords(), &path).unwrap();
    image.save().unwrap();

    let opened = AnyPagedImage::open(&path).unwrap();
    assert_eq!(opened.pixel_type(), ImagePixelType::Float64);
    assert_eq!(opened.shape(), &[2, 2]);
}

#[test]
fn tiled_io_plane_by_plane_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tiled.image");
    let size = 16;
    let shape = vec![size, size, size];
    let tile_shape = vec![8, 8, 8];

    let mut img =
        PagedImage::<f32>::create_with_tile_shape(shape.clone(), tile_shape, make_coords(), &path)
            .unwrap();

    // Write plane by plane (z-planes).
    for z in 0..size {
        let plane = ArrayD::from_shape_fn(IxDyn(&[size, size, 1]), |idx| {
            (idx[0] + idx[1] * size + z * size * size) as f32
        });
        img.put_slice(&plane, &[0, 0, z]).unwrap();
    }
    img.save().unwrap();

    // Read back and verify.
    let img2 = PagedImage::<f32>::open(&path).unwrap();
    for z in 0..size {
        let plane = img2.get_slice(&[0, 0, z], &[size, size, 1]).unwrap();
        for x in 0..size {
            for y in 0..size {
                let expected = (x + y * size + z * size * size) as f32;
                assert_eq!(plane[[x, y, 0]], expected, "mismatch at [{x}, {y}, {z}]");
            }
        }
    }

    // Also test full read.
    let full = img2.get().unwrap();
    eprintln!(
        "full shape: {:?}, expected: {:?}",
        full.shape(),
        [size, size, size]
    );
    assert_eq!(full.shape(), &[size, size, size]);
    for z in 0..size {
        for y in 0..size {
            for x in 0..size {
                let expected = (x + y * size + z * size * size) as f32;
                assert_eq!(full[[x, y, z]], expected, "full mismatch at [{x},{y},{z}]");
            }
        }
    }
}

#[test]
fn tiled_io_image_registers_casa_visible_data_manager() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tiled_dm.image");
    let mut img = PagedImage::<f32>::create_with_tile_shape_and_cache(
        vec![16, 16, 1, 4],
        vec![8, 8, 1, 2],
        make_coords(),
        &path,
        4096,
    )
    .unwrap();
    img.put_slice(
        &ArrayD::from_shape_vec(IxDyn(&[16, 16, 1, 1]), vec![1.0; 16 * 16]).unwrap(),
        &[0, 0, 0, 2],
    )
    .unwrap();
    img.save().unwrap();

    let table = Table::open(TableOptions::new(&path)).unwrap();
    let map_dm = table
        .data_manager_info()
        .iter()
        .find(|dm| dm.columns.iter().any(|column| column == MAP_COLUMN))
        .expect("map data manager");
    assert_eq!(map_dm.dm_type, "TiledCellStMan");
    assert_eq!(map_dm.seq_nr, 0);

    let reopened = PagedImage::<f32>::open(&path).unwrap();
    assert_eq!(reopened.shape(), &[16, 16, 1, 4]);
    let plane = reopened.get_slice(&[0, 0, 0, 2], &[16, 16, 1, 1]).unwrap();
    assert!(plane.iter().all(|value| *value == 1.0));
}

#[test]
fn metadata_only_open_can_update_history_and_masks_then_save() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("metadata_only.image");
    let mut image =
        PagedImage::<f32>::create_with_tile_shape(vec![4, 4], vec![2, 2], make_coords(), &path)
            .unwrap();
    image
        .put_slice(&ArrayD::from_elem(IxDyn(&[4, 4]), 1.0f32), &[0, 0])
        .unwrap();
    image.save().unwrap();

    let mut reopened = PagedImage::<f32>::open(&path).unwrap();
    assert_eq!(reopened.table().row_count(), 0);
    reopened.add_history("after-open").unwrap();
    reopened.save().unwrap();

    let reread = PagedImage::<f32>::open(&path).unwrap();
    assert_eq!(reread.history().unwrap(), vec!["after-open".to_string()]);
    assert_eq!(
        reread.get().unwrap(),
        ArrayD::from_elem(IxDyn(&[4, 4]), 1.0f32)
    );
}

#[test]
fn image_interface_defaults_cover_axes_plane_names_and_masks() {
    let mut image = crate::TempImage::<f32>::new(
        vec![3, 4],
        make_coords(),
        casa_lattices::TempStoragePolicy::Memory,
    )
    .unwrap();
    image.set_units("K").unwrap();
    image.make_mask("quality", true, true).unwrap();
    image
        .put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[3, 4]), (0..12).map(|v| v as f32).collect()).unwrap(),
            &[0, 0],
        )
        .unwrap();

    assert_eq!(
        image.axis_types(),
        vec![CoordinateType::Linear, CoordinateType::Linear]
    );
    assert_eq!(
        image.axis_names(),
        vec!["Axis0".to_string(), "Axis1".to_string()]
    );
    assert_eq!(image.find_axis(CoordinateType::Linear), None);
    assert_eq!(image.find_axis_by_name("axis1"), Some(1));
    assert_eq!(
        image.get_plane(1, 2).unwrap(),
        ArrayD::from_shape_vec(IxDyn(&[3, 1]), vec![2.0, 6.0, 10.0]).unwrap()
    );
    assert!(matches!(
        image.get_plane(4, 0),
        Err(ImageError::ShapeMismatch { .. })
    ));
    assert_eq!(image.mask_names(), vec!["quality".to_string()]);
    assert_eq!(
        image.get_named_mask("quality").unwrap(),
        ArrayD::from_elem(IxDyn(&[3, 4]), true)
    );
    assert!(matches!(
        image.get_named_mask("missing"),
        Err(ImageError::MaskNotFound(_))
    ));
    assert_eq!(image.name_string(false), None);
}

#[test]
fn paged_image_wrappers_cover_name_subimage_expr_and_mutation_helpers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wrappers.image");
    let mut image = PagedImage::<f32>::create(vec![4, 4], make_coords(), &path).unwrap();
    image.set_units("Jy").unwrap();
    image.set(1.0).unwrap();

    assert_eq!(image.name_string(true).as_deref(), Some("wrappers.image"));
    assert_eq!(
        image.name_string(false).as_deref(),
        Some(path.to_string_lossy().as_ref())
    );

    let sub = image.sub_image(vec![1, 1], vec![2, 2]).unwrap();
    assert_eq!(sub.get().unwrap(), ArrayD::from_elem(IxDyn(&[2, 2]), 1.0));

    let strided = image
        .sub_image_with_stride(vec![0, 0], vec![2, 2], vec![2, 2])
        .unwrap();
    assert_eq!(
        strided.get().unwrap(),
        ArrayD::from_elem(IxDyn(&[2, 2]), 1.0)
    );

    let expr = image.expr().unwrap().multiply_scalar(2.0);
    assert_eq!(
        expr.compile().unwrap().get().unwrap(),
        ArrayD::from_elem(IxDyn(&[4, 4]), 2.0)
    );
    drop(expr);
    let expr_map = image.expr_map(|value| value + 3.0).unwrap();
    assert_eq!(expr_map.compile().unwrap().get_at(&[0, 0]).unwrap(), 4.0);
    drop(expr_map);

    {
        let mut sub_mut = image
            .sub_image_mut_with_stride(vec![0, 0], vec![2, 2], vec![2, 2])
            .unwrap();
        sub_mut.set(5.0).unwrap();
    }
    assert_eq!(image.get_at(&[0, 0]).unwrap(), 5.0);
    assert_eq!(image.get_at(&[2, 2]).unwrap(), 5.0);
    assert_eq!(image.get_at(&[1, 1]).unwrap(), 1.0);
}

#[test]
fn paged_image_metadata_mask_and_history_wrappers_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("metadata_mask.image");
    let mut image = PagedImage::<f32>::create(vec![3, 3], make_coords(), &path).unwrap();

    assert_eq!(image.channel_plane(0).unwrap(), None);
    assert_eq!(image.stokes_plane(0).unwrap(), None);
    assert!(!image.has_pixel_mask());
    assert_eq!(image.get_mask().unwrap(), None);
    assert!(image.mask_names().is_empty());

    image.set_coordinates(CoordinateSystem::new()).unwrap();
    image.set_units("K").unwrap();
    let info = ImageInfo {
        image_type: crate::image_info::ImageType::Intensity,
        object_name: "NGC5921".to_string(),
        ..ImageInfo::default()
    };
    image.set_image_info(&info).unwrap();
    let misc = RecordValue::new(vec![RecordField::new(
        "observer",
        Value::Scalar(ScalarValue::String("Rusty".to_string())),
    )]);
    image.set_misc_info(misc.clone()).unwrap();

    image.make_mask("quality", true, false).unwrap();
    assert!(image.has_pixel_mask());
    assert_eq!(image.default_mask_name().as_deref(), Some("quality"));
    assert_eq!(image.mask_names(), vec!["quality".to_string()]);
    assert_eq!(
        image.get_mask().unwrap(),
        Some(ArrayD::from_elem(IxDyn(&[3, 3]), false))
    );

    image.unset_default_mask().unwrap();
    assert!(!image.has_pixel_mask());
    assert_eq!(image.get_mask().unwrap(), None);
    image.set_default_mask("quality").unwrap();

    image.add_history("first").unwrap();
    image.add_history("second").unwrap();
    assert_eq!(
        image.history().unwrap(),
        vec!["first".to_string(), "second".to_string()]
    );
    image.clear_history().unwrap();
    assert!(image.history().unwrap().is_empty());
    image.add_history("kept").unwrap();

    let replacement = ArrayD::from_shape_vec(
        IxDyn(&[3, 3]),
        vec![true, false, true, false, true, false, true, false, true],
    )
    .unwrap();
    image.put_mask("quality", &replacement).unwrap();
    assert_eq!(image.get_named_mask("quality").unwrap(), replacement);
    assert!(path.join("quality").exists());

    image.remove_mask("quality").unwrap();
    assert_eq!(image.default_mask_name(), None);
    assert!(image.mask_names().is_empty());
    assert!(!path.join("quality").exists());

    image.make_mask("restored", true, true).unwrap();
    image.save().unwrap();

    let reopened = PagedImage::<f32>::open(&path).unwrap();
    assert_eq!(reopened.units(), "K");
    assert_eq!(reopened.image_info().unwrap(), info);
    assert_eq!(reopened.misc_info(), misc);
    assert_eq!(reopened.default_mask_name().as_deref(), Some("restored"));
    assert_eq!(
        reopened.get_mask().unwrap(),
        Some(ArrayD::from_elem(IxDyn(&[3, 3]), true))
    );
    assert_eq!(reopened.history().unwrap(), vec!["kept".to_string()]);
}

#[test]
fn paged_image_mask_error_paths_are_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mask_errors.image");
    let mut image = PagedImage::<f32>::create(vec![2, 2], make_coords(), &path).unwrap();

    assert!(matches!(
        image.set_default_mask("missing"),
        Err(ImageError::MaskNotFound(_))
    ));
    assert!(matches!(
        image.get_named_mask("missing"),
        Err(ImageError::MaskNotFound(_))
    ));
    assert!(matches!(
        image.remove_mask("missing"),
        Err(ImageError::MaskNotFound(_))
    ));
    assert!(matches!(
        image.put_mask("bad", &ArrayD::from_elem(IxDyn(&[1, 2]), true)),
        Err(ImageError::ShapeMismatch { .. })
    ));
}

#[test]
fn paged_image_mask_slice_reads_persistent_subregion_without_full_mask() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mask_slice.image");
    let mut image = PagedImage::<f32>::create(vec![5, 4, 3], make_coords(), &path).unwrap();
    image
        .put_mask(
            "quality",
            &ArrayD::from_shape_fn(IxDyn(&[5, 4, 3]), |idx| {
                let x = idx[0];
                let y = idx[1];
                let z = idx[2];
                (x + y + z) % 2 == 0
            }),
        )
        .unwrap();
    image.set_default_mask("quality").unwrap();
    image.save().unwrap();

    let reopened = PagedImage::<f32>::open_with_cache(&path, 256).unwrap();
    let slice = reopened
        .get_mask_slice(&[1, 1, 0], &[2, 2, 3], &[2, 1, 1])
        .unwrap()
        .unwrap();
    assert_eq!(slice.shape(), &[2, 2, 3]);
    for x in 0..2 {
        for y in 0..2 {
            for z in 0..3 {
                let src_x = 1 + x * 2;
                let src_y = 1 + y;
                assert_eq!(slice[[x, y, z]], (src_x + src_y + z) % 2 == 0);
            }
        }
    }
}

fn stored_mask_reference(table: &Table, mask: &str) -> Option<Value> {
    let Some(Value::Record(masks)) = table.keywords().get(MASKS_KEYWORD) else {
        return None;
    };
    let Some(Value::Record(region)) = masks.get(mask) else {
        return None;
    };
    region.get("mask").cloned()
}

#[test]
fn persistent_masks_stay_readable_after_the_image_directory_moves() {
    let dir = tempfile::tempdir().unwrap();
    let written = dir.path().join("staging.image");
    let moved = dir.path().join("moved").join("visible.image");
    let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![true, false, false, true]).unwrap();
    let mut image = PagedImage::<f32>::create(vec![2, 2], make_coords(), &written).unwrap();
    image.put_mask("mask0", &expected).unwrap();
    image.set_default_mask("mask0").unwrap();
    image.save().unwrap();
    drop(image);

    std::fs::create_dir_all(moved.parent().unwrap()).unwrap();
    std::fs::rename(&written, &moved).unwrap();
    let reopened = PagedImage::<f32>::open(&moved).unwrap();
    assert_eq!(reopened.get_mask().unwrap(), Some(expected));
    assert_eq!(
        stored_mask_reference(&reopened.table, "mask0"),
        Some(Value::table_ref("././mask0"))
    );
    assert_eq!(
        reopened.table.keywords().get(LOGTABLE_KEYWORD),
        Some(&Value::table_ref("././logtable"))
    );
}

#[test]
fn older_absolute_mask_references_read_in_place_and_save_relative() {
    // casa-rs before #715 stored the mask table's absolute path; a staged
    // product stored the path its image was about to be moved to.
    let dir = tempfile::tempdir().unwrap();
    let staging = dir.path().join("staging").join("image.pb");
    let visible = dir.path().join("visible").join("image.pb");
    let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![false, true, true, true]).unwrap();
    let mut image = PagedImage::<f32>::create(vec![2, 2], make_coords(), &staging).unwrap();
    image.put_mask("mask0", &expected).unwrap();
    image.set_default_mask("mask0").unwrap();
    let legacy = visible.join("mask0").to_string_lossy().into_owned();
    let mut masks = RecordValue::default();
    masks.upsert(
        "mask0",
        Value::Record(make_paged_mask_record(&legacy, &image.shape)),
    );
    image
        .table
        .keywords_mut()
        .upsert(MASKS_KEYWORD, Value::Record(masks));
    image.save().unwrap();
    drop(image);

    std::fs::create_dir_all(visible.parent().unwrap()).unwrap();
    std::fs::rename(&staging, &visible).unwrap();

    // In place, the absolute reference still resolves; a save in place
    // stores it relative to the image, so the image can then be moved.
    let mut in_place = PagedImage::<f32>::open(&visible).unwrap();
    assert_eq!(
        stored_mask_reference(&in_place.table, "mask0"),
        Some(Value::table_ref(legacy))
    );
    assert_eq!(in_place.get_mask().unwrap(), Some(expected.clone()));
    in_place.save().unwrap();
    drop(in_place);
    let moved = dir.path().join("moved.pb");
    std::fs::rename(&visible, &moved).unwrap();
    let reopened = PagedImage::<f32>::open(&moved).unwrap();
    assert_eq!(reopened.get_mask().unwrap(), Some(expected));
    let on_disk = Table::open(TableOptions::new(&moved)).unwrap();
    assert_eq!(
        stored_mask_reference(&on_disk, "mask0"),
        Some(Value::table_ref("././mask0"))
    );
}

#[test]
fn paged_image_mask_slice_write_creates_persistent_mask_without_full_input() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mask_slice_write.image");
    let mut image = PagedImage::<f32>::create_with_tile_shape(
        vec![4, 4, 1, 3],
        vec![4, 4, 1, 1],
        make_coords(),
        &path,
    )
    .unwrap();
    let plane_mask = ArrayD::from_shape_fn(IxDyn(&[2, 2, 1, 1]), |idx| idx[0] == idx[1]);
    image
        .put_mask_slice("mask0", &plane_mask, &[1, 1, 0, 2])
        .unwrap();
    image.set_default_mask("mask0").unwrap();
    image.save().unwrap();

    let reopened = PagedImage::<f32>::open_with_cache(&path, 256).unwrap();
    assert_eq!(reopened.default_mask_name().as_deref(), Some("mask0"));
    let written = reopened
        .get_mask_slice(&[1, 1, 0, 2], &[2, 2, 1, 1], &[1, 1, 1, 1])
        .unwrap()
        .unwrap();
    assert_eq!(written, plane_mask);
    let untouched = reopened
        .get_mask_slice(&[0, 0, 0, 0], &[4, 4, 1, 1], &[1, 1, 1, 1])
        .unwrap()
        .unwrap();
    assert!(untouched.iter().all(|value| !*value));
}

#[test]
fn paged_image_mask_slice_preserves_existing_storage_and_pixels() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("existing_masks.image");
    let shape = vec![4, 3];
    let mut image = PagedImage::<f32>::create(shape.clone(), make_coords(), &path).unwrap();
    let initial = ArrayD::from_shape_fn(IxDyn(&shape), |index| index[0] != 0);
    image.put_mask("backing", &initial).unwrap();
    let mut masks = RecordValue::default();
    masks.upsert("inline", Value::Array(ArrayValue::Bool(initial.clone())));
    for name in ["aliased", "same_backing"] {
        masks.upsert(
            name,
            Value::Record(make_paged_mask_record(
                &subtable_reference("backing"),
                &shape,
            )),
        );
    }
    image
        .table
        .keywords_mut()
        .upsert(MASKS_KEYWORD, Value::Record(masks));
    image.set_default_mask("inline").unwrap();
    image.save().unwrap();
    drop(image);

    let mut image = PagedImage::<f32>::open(&path).unwrap();
    let patch = ArrayD::from_elem(IxDyn(&[1, 1]), false);
    assert!(image.get_named_mask("same_backing").unwrap()[[2, 1]]);
    for name in ["inline", "aliased"] {
        MutableImageInterface::put_mask_slice(&mut image, name, &patch, &[2, 1]).unwrap();
    }
    assert!(!image.get_named_mask("same_backing").unwrap()[[2, 1]]);
    image.save().unwrap();
    drop(image);

    let mut expected = initial;
    expected[[2, 1]] = false;
    let image = PagedImage::<f32>::open(&path).unwrap();
    assert_eq!(image.get_mask().unwrap(), Some(expected.clone()));
    assert_eq!(image.get_named_mask("aliased").unwrap(), expected);
    assert!(!path.join("inline").exists());
    assert!(!path.join("aliased").exists());
}

#[test]
fn paged_image_cache_and_basic_identity_wrappers_are_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache_wrappers.image");
    let mut image = PagedImage::<f32>::create_with_tile_shape_and_cache(
        vec![4, 3],
        vec![2, 2],
        make_coords(),
        &path,
        4096,
    )
    .unwrap();

    assert_eq!(
        PagedImage::<f32>::pixel_type(&path).unwrap(),
        ImagePixelType::Float32
    );
    assert_eq!(image.shape(), &[4, 3]);
    assert_eq!(image.ndim(), 2);
    assert_eq!(image.nelements(), 12);
    assert!(image.is_persistent());
    assert!(image.is_paged());
    assert!(image.is_writable());
    assert_eq!(image.image_type_name(), "PagedImage");
    assert_eq!(image.name(), Some(path.as_path()));
    assert_eq!(
        image.name_string(true).as_deref(),
        Some("cache_wrappers.image")
    );
    assert_eq!(image.tile_shape(), &[2, 2]);
    assert_eq!(image.cache_bytes(), 4096);
    image.set_units("Jy/beam").unwrap();
    let misc = RecordValue::new(vec![RecordField::new(
        "observer",
        Value::Scalar(ScalarValue::String("cache-test".into())),
    )]);
    image.set_misc_info(misc.clone()).unwrap();

    image
        .put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[4, 3]), (0..12).map(|v| v as f32).collect()).unwrap(),
            &[0, 0],
        )
        .unwrap();
    image.save().unwrap();

    let reopened = PagedImage::<f32>::open_with_cache(&path, 2048).unwrap();
    assert_eq!(reopened.cache_bytes(), 2048);
    assert_eq!(reopened.shape(), &[4, 3]);
    assert_eq!(reopened.table().row_count(), 0);
    assert_eq!(reopened.units(), "Jy/beam");
    assert_eq!(reopened.misc_info(), misc);
}

#[test]
fn any_paged_image_opens_supported_pixel_types() {
    let dir = tempfile::tempdir().unwrap();
    let variants = [
        (dir.path().join("any_f32.image"), ImagePixelType::Float32),
        (dir.path().join("any_f64.image"), ImagePixelType::Float64),
        (dir.path().join("any_c32.image"), ImagePixelType::Complex32),
        (dir.path().join("any_c64.image"), ImagePixelType::Complex64),
    ];

    let mut f32_image =
        PagedImage::<f32>::create(vec![1, 1], make_coords(), &variants[0].0).unwrap();
    f32_image.save().unwrap();
    let mut f64_image =
        PagedImage::<f64>::create(vec![1, 1], make_coords(), &variants[1].0).unwrap();
    f64_image.save().unwrap();
    let mut c32_image =
        PagedImage::<Complex32>::create(vec![1, 1], make_coords(), &variants[2].0).unwrap();
    c32_image.save().unwrap();
    let mut c64_image =
        PagedImage::<Complex64>::create(vec![1, 1], make_coords(), &variants[3].0).unwrap();
    c64_image.save().unwrap();

    for (path, expected) in variants {
        let any = AnyPagedImage::open(&path).unwrap();
        assert_eq!(any.pixel_type(), expected);
        assert_eq!(any.shape(), &[1, 1]);
    }
}

#[test]
fn default_mask_keyword_empty_string_is_treated_as_absent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty_default_mask.image");
    let mut image = PagedImage::<f32>::create(vec![2, 2], make_coords(), &path).unwrap();
    image.table.keywords_mut().upsert(
        DEFAULT_MASK_KEYWORD,
        Value::Scalar(ScalarValue::String(String::new())),
    );
    assert_eq!(image.default_mask_name(), None);
    assert!(!image.has_pixel_mask());
}

#[test]
fn image_interface_default_fallbacks_and_invalid_pixel_type_are_explicit() {
    let image = BareImage::new(
        ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0f32, 2.0, 3.0, 4.0]).unwrap(),
    );

    assert!(matches!(
        ImagePixelType::from_primitive_type(PrimitiveType::Int32),
        Err(ImageError::InvalidMetadata(_))
    ));
    assert_eq!(image.default_mask().unwrap(), None);
    assert_eq!(image.default_mask_name(), None);
    assert!(image.mask_names().is_empty());
    assert!(matches!(
        image.get_named_mask("missing"),
        Err(ImageError::MaskNotFound(_))
    ));
    assert_eq!(image.history().unwrap(), Vec::<String>::new());
    assert_eq!(image.name_string(false), None);
    assert_eq!(
        image.axis_types(),
        vec![CoordinateType::Linear, CoordinateType::Linear]
    );
    assert_eq!(
        image.axis_names(),
        vec!["Axis0".to_string(), "Axis1".to_string()]
    );
    assert_eq!(image.find_axis_by_name("axis0"), Some(0));
    assert_eq!(
        image.get_plane(0, 1).unwrap(),
        ArrayD::from_shape_vec(IxDyn(&[1, 2]), vec![3.0, 4.0]).unwrap()
    );
}

#[test]
fn a_sparse_strided_slice_reads_only_intersecting_tiles() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sparse.image");
    let shape = vec![8192, 4096];
    let coords = casa_coordinates::defaults::coordinate_system_for_shape(&shape, false);
    let corners = [[0, 0], [8191, 0], [0, 4095], [8191, 4095]];
    let mut image = PagedImage::<f32>::create(shape, coords, &path).unwrap();
    for (index, corner) in corners.iter().enumerate() {
        image.put_at(index as f32 + 1.0, corner).unwrap();
    }
    image.save().unwrap();
    drop(image);

    let image = PagedImage::<f32>::open(&path).unwrap();
    let values = image
        .get_slice_with_stride(&[0, 0], &[2, 2], &[8191, 4095])
        .unwrap();
    for (index, position) in [[0, 0], [1, 0], [0, 1], [1, 1]].iter().enumerate() {
        assert_eq!(values[IxDyn(position)], index as f32 + 1.0);
    }
    let metrics = image.tiled_io_stats().unwrap();
    let tile = image.tile_shape();
    let selected_tiles = corners
        .iter()
        .map(|position| [position[0] / tile[0], position[1] / tile[1]])
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(metrics.lru_read_tiles, selected_tiles.len());
    assert_eq!(metrics.flat_allocated_bytes, 0);
}

#[test]
fn a_strided_image_slice_gathers_all_samples_with_one_lookup_per_tile() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("strided.image");
    let shape = vec![19, 11, 3, 12];
    let tile_shape = vec![5, 4, 2, 2];
    let coords = casa_coordinates::defaults::coordinate_system_for_shape(&shape, false);
    let mut image =
        PagedImage::<f32>::create_with_tile_shape(shape.clone(), tile_shape.clone(), coords, &path)
            .unwrap();
    let value = |x: usize, y: usize, polarization: usize, channel: usize| {
        (x + 20 * (y + 12 * (polarization + 3 * channel))) as f32
    };
    let pixels = ArrayD::from_shape_fn(IxDyn(&shape).f(), |index| {
        value(index[0], index[1], index[2], index[3])
    });
    image.put_slice(&pixels, &[0; 4]).unwrap();
    image.save().unwrap();
    drop(image);

    let cache_bytes = 2 * tile_shape.iter().product::<usize>() * size_of::<f32>();
    let mut image = PagedImage::<f32>::open_with_cache(&path, cache_bytes).unwrap();
    image.reset_tiled_io_stats();
    let start = [1, 1, 0, 1];
    let step = [3, 2, 1, 4];
    let size = [6, 5, 3, 3];
    let samples = image.get_slice_with_stride(&start, &size, &step).unwrap();
    let expected = ArrayD::from_shape_fn(IxDyn(&size).f(), |index| {
        value(
            1 + 3 * index[0],
            1 + 2 * index[1],
            index[2],
            1 + 4 * index[3],
        )
    });
    assert_eq!(samples, expected);
    let selected_tiles = (0..shape.len())
        .map(|axis| {
            (0..size[axis])
                .map(|index| (start[axis] + index * step[axis]) / tile_shape[axis])
                .collect::<std::collections::BTreeSet<_>>()
                .len()
        })
        .product::<usize>();
    let stats = image.tiled_io_stats().unwrap();
    assert_eq!(stats.flat_allocated_bytes, 0);
    // Read-ahead can turn a subsequent lookup into a cache hit.
    assert_eq!(stats.lru_hits + stats.lru_misses, selected_tiles);
}

#[test]
fn fifty_gib_spectral_cube_streams_selected_planes_with_bounded_storage() {
    const GIB: usize = 1024 * 1024 * 1024;
    let shape = vec![1024, 1024, 1, 12_800];
    let logical_bytes = shape.iter().product::<usize>() * std::mem::size_of::<f32>();
    assert_eq!(logical_bytes, 50 * GIB);

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fifty-gib-spectral.image");
    let mut image = PagedImage::<f32>::create(shape.clone(), make_coords(), &path).unwrap();
    assert_eq!(image.tile_shape(), &[1024, 1024, 1, 1]);

    let plane_shape = vec![1024, 1024, 1, 1];
    let plane = ArrayD::from_elem(IxDyn(&plane_shape), 7.25f32);
    for channel in [0, 6_400, 12_799] {
        image.put_slice(&plane, &[0, 0, 0, channel]).unwrap();
    }
    image.save().unwrap();
    let stats = image.tiled_io_stats().unwrap();
    assert_eq!(stats.direct_tile_write_tiles, 3);
    assert_eq!(stats.direct_tile_write_bytes, 3 * 4 * 1024 * 1024);
    assert_eq!(stats.flat_allocated_bytes, 0);
    assert!(
        image.tiled_storage_resident_bytes().unwrap()
            <= DEFAULT_TILED_ARRAY_CACHE_BYTES + 4 * 1024 * 1024
    );
    drop(image);

    let reopened = PagedImage::<f32>::open(&path).unwrap();
    assert_eq!(reopened.shape(), shape);
    for channel in [0, 6_400, 12_799] {
        let read = reopened
            .get_slice(&[0, 0, 0, channel], &plane_shape)
            .unwrap();
        assert!(read.iter().all(|&value| value == 7.25));
    }
    assert!(
        reopened.tiled_storage_resident_bytes().unwrap()
            <= DEFAULT_TILED_ARRAY_CACHE_BYTES + 4 * 1024 * 1024
    );
}
