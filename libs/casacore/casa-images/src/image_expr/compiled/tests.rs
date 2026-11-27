// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn converted_mask_reads_only_bounded_source_sections() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let pixels_read = Arc::new(AtomicUsize::new(0));
    let source_reads = Arc::clone(&pixels_read);
    let mut metadata = ImageExprBuilder::scalar(0.0f32).meta;
    metadata.shape = vec![1025, 1025];
    let pixels = metadata.shape.iter().product::<usize>();
    let limit: usize = bounded_read_cursor(metadata.shape.clone())
        .unwrap()
        .iter()
        .product();
    let source = ImageExprBuilder::from_section_readers(
        metadata,
        Arc::new(move |_, shape, _| {
            let count = shape.iter().product::<usize>();
            assert!(count <= limit);
            source_reads.fetch_add(count, Ordering::Relaxed);
            Ok(ArrayD::from_elem(IxDyn(shape), 1.0f32))
        }),
        None,
    )
    .map_pixel_type(f64::from)
    .unwrap()
    .compile()
    .unwrap();
    let primary = ImageExprBuilder::from_image(&source).unwrap();
    let mut mask = primary
        .clone()
        .compare_expr(primary.mean_reduce(), ImageExprCompareOp::GreaterThan)
        .unwrap()
        .compile()
        .unwrap();
    mask.set_execution_policy(ExecutionPolicy::Serial);
    assert_eq!(pixels_read.load(Ordering::Relaxed), 0);
    assert!(mask.get_in_sections().unwrap().iter().all(|&value| !value));
    assert_eq!(pixels_read.load(Ordering::Relaxed), 2 * pixels);
}

#[test]
fn replace_reads_validity_on_demand_and_reports_read_error() {
    let mut metadata = ImageExprBuilder::scalar(0.0f32).meta;
    metadata.shape = vec![4, 3];
    let expression = ImageExprBuilder::from_section_readers(
        metadata,
        Arc::new(|_, shape, _| Ok(ArrayD::from_elem(IxDyn(shape), 1.0f32))),
        Some(Arc::new(|_, _, _| {
            Err(ImageError::Io("mask read failed".into()))
        })),
    )
    .replace_invalid(ImageExprBuilder::scalar(0.0))
    .unwrap()
    .compile()
    .unwrap();
    let error = expression.get_slice(&[0, 0], &[1, 2]).unwrap_err();
    assert!(error.to_string().contains("mask read failed"));
}
use crate::ImageInterface;
use casa_lattices::{ExecutionPolicy, Lattice};
use ndarray::IxDyn;

fn make_coords() -> CoordinateSystem {
    CoordinateSystem::new()
}

fn make_temp_image(shape: Vec<usize>, values: Vec<f32>) -> TempImage<f32> {
    let mut image = TempImage::<f32>::new(
        shape.clone(),
        make_coords(),
        casa_lattices::TempStoragePolicy::Memory,
    )
    .unwrap();
    image
        .put_slice(
            &ArrayD::from_shape_vec(IxDyn(&shape), values).unwrap(),
            &vec![0; shape.len()],
        )
        .unwrap();
    image
}

fn make_paged_image(
    dir: &tempfile::TempDir,
    name: &str,
    shape: Vec<usize>,
    tile_shape: Vec<usize>,
    values: Vec<f32>,
) -> PagedImage<f32> {
    let path = dir.path().join(name);
    let mut image =
        PagedImage::<f32>::create_with_tile_shape(shape.clone(), tile_shape, make_coords(), &path)
            .unwrap();
    image
        .put_slice(
            &ArrayD::from_shape_vec(IxDyn(&shape), values).unwrap(),
            &vec![0; shape.len()],
        )
        .unwrap();
    image.save().unwrap();
    PagedImage::<f32>::open(&path).unwrap()
}

#[test]
fn compiled_helper_functions_choose_expected_values() {
    let available = thread_parallelism();

    assert_eq!(
        compiled_source_cache_bytes::<f32>(&[16, 16, 16], 1234).unwrap(),
        1234
    );
    assert_eq!(
        compiled_source_cache_bytes::<f32>(&[16, 16, 16], 0).unwrap(),
        16 * 16 * 16 * std::mem::size_of::<f32>()
    );

    let shape = [1024, 1024, 256];
    let target = work_balanced_pixel_target(&shape, 256).unwrap();
    assert_eq!(
        expand_tile_aligned_cursor_shape(&shape, &[16, 16, 16], target).unwrap(),
        vec![1024, 64, 16]
    );

    let resources = ExecutionResources {
        memory_budget_bytes: 64 * 1024 * 1024,
        worker_limit: 4,
        prefetch_cap: 8,
    };
    let serial = compiled_map_strategy::<f32>(CompiledMapInputs {
        policy: ExecutionPolicy::Auto,
        full_shape: &[64, 64],
        cursor_shape: &[64, 64],
        has_paged_sources: false,
        source_count: 1,
        repeated_source_count: 0,
        output_element_bytes: std::mem::size_of::<f32>(),
        resources,
    })
    .unwrap();
    assert!(matches!(
        serial,
        OrderedCursorMapWriteExecutionStrategy::Serial
    ));

    let pipe = compiled_map_strategy::<f32>(CompiledMapInputs {
        policy: ExecutionPolicy::Auto,
        full_shape: &[512, 512],
        cursor_shape: &[256, 256],
        has_paged_sources: true,
        source_count: 1,
        repeated_source_count: 0,
        output_element_bytes: std::mem::size_of::<f32>(),
        resources,
    })
    .unwrap();
    if available < 2 {
        assert!(matches!(
            pipe,
            OrderedCursorMapWriteExecutionStrategy::Serial
        ));
    } else {
        assert!(matches!(
            pipe,
            OrderedCursorMapWriteExecutionStrategy::Parallel(_)
        ));
    }

    let par = compiled_map_strategy::<f32>(CompiledMapInputs {
        policy: ExecutionPolicy::Auto,
        full_shape: &[2048, 2048],
        cursor_shape: &[256, 256],
        has_paged_sources: true,
        source_count: 1,
        repeated_source_count: 0,
        output_element_bytes: std::mem::size_of::<f32>(),
        resources,
    })
    .unwrap();
    if available < 2 {
        assert!(matches!(
            par,
            OrderedCursorMapWriteExecutionStrategy::Serial
        ));
    } else {
        assert!(matches!(
            par,
            OrderedCursorMapWriteExecutionStrategy::Parallel(_)
        ));
    }
}

#[test]
fn compile_uses_snapshot_for_temp_and_dedups_paged_sources() {
    let temp = make_temp_image(vec![2, 2], vec![1.0, 2.0, 3.0, 4.0]);
    let compiled = ImageExprBuilder::from_image(&temp)
        .unwrap()
        .add_scalar(1.0)
        .compile()
        .unwrap();
    assert_eq!(compiled.arena.sources.len(), 1);
    assert!(matches!(
        compiled.arena.sources[0].kind,
        CompiledImageSourceKind::Snapshot(_)
    ));

    let dir = tempfile::tempdir().unwrap();
    let paged = make_paged_image(
        &dir,
        "dedup.image",
        vec![8, 8, 4],
        vec![2, 2, 2],
        (0..8 * 8 * 4).map(|v| v as f32).collect(),
    );
    let compiled = ImageExprBuilder::from_image(&paged)
        .unwrap()
        .add_image(&paged)
        .unwrap()
        .compile()
        .unwrap();
    assert_eq!(compiled.arena.sources.len(), 1);
    assert!(matches!(
        compiled.arena.sources[0].kind,
        CompiledImageSourceKind::Paged { .. }
    ));
    assert_eq!(
        compiled.arena.sources[0].tile_shape.as_deref(),
        Some(&[2, 2, 2][..])
    );
}

#[cfg(unix)]
#[test]
fn compiled_locked_source_reacquires_read_lock_for_pixel_sections() {
    let directory = tempfile::tempdir().unwrap();
    let initial = make_paged_image(
        &directory,
        "locked.image",
        vec![4, 4],
        vec![2, 2],
        (0..16).map(|value| value as f32).collect(),
    );
    let path = initial.name().unwrap().to_path_buf();
    drop(initial);
    let locked = PagedImage::<f32>::open_read_locked_with_cache(&path, 0).unwrap();
    let unlocked = PagedImage::<f32>::open(&path).unwrap();
    let expressions = [(&unlocked, &locked), (&locked, &unlocked)].map(|(first, second)| {
        ImageExprBuilder::from_image(first)
            .unwrap()
            .add_image(second)
            .unwrap()
            .compile()
            .unwrap()
    });
    drop((locked, unlocked));

    for expression in expressions {
        let arena = &expression.arena;
        assert_eq!(arena.sources.len(), 1);
        let mut context = CompiledEvalContext::new(arena.sources.len());
        let pixels = arena.sources[0]
            .read_slice_uncached(arena, &mut context, 0, &[0, 0], &[2, 2], &[1, 1])
            .unwrap();
        assert_eq!(pixels.shape(), [2, 2]);
        assert!(
            context.opened_sources[0]
                .as_ref()
                .unwrap()
                .has_lock(casa_tables::LockType::Read)
        );
    }
}

#[test]
fn compiled_numeric_expr_supports_all_read_paths() {
    let image = make_temp_image(vec![3, 3], (0..9).map(|v| v as f32).collect());
    let mut compiled = ImageExprBuilder::from_image(&image)
        .unwrap()
        .multiply_scalar(2.0)
        .add_scalar(1.0)
        .compile()
        .unwrap();

    for policy in [ExecutionPolicy::Serial, ExecutionPolicy::Auto] {
        compiled.set_execution_policy(policy);
        assert_eq!(compiled.get_at(&[1, 2]).unwrap(), 11.0);
        assert_eq!(
            compiled.get_slice(&[1, 1], &[2, 2]).unwrap(),
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![9.0, 11.0, 15.0, 17.0]).unwrap()
        );
        assert_eq!(
            compiled
                .get_slice_with_stride(&[0, 0], &[1, 2], &[2, 1])
                .unwrap(),
            ArrayD::from_shape_vec(IxDyn(&[1, 2]), vec![1.0, 3.0]).unwrap()
        );
        assert_eq!(
            <ImageExpression<f32> as Lattice<f32>>::get_slice(&compiled, &[0, 0], &[2, 1], &[1, 2])
                .unwrap(),
            ArrayD::from_shape_vec(IxDyn(&[2, 1]), vec![1.0, 7.0]).unwrap()
        );
    }
}

#[test]
fn compiled_mask_expr_covers_logical_and_reduction_paths() {
    let image = make_temp_image(vec![2, 3], vec![0.0, 1.0, f32::NAN, 3.0, 4.0, 5.0]);
    let mask = ImageExprBuilder::from_image(&image)
        .unwrap()
        .gt_scalar(1.0)
        .or(ImageExprBuilder::from_image(&image).unwrap().isnan())
        .unwrap()
        .logical_not()
        .compile()
        .unwrap();

    assert_eq!(
        mask.get().unwrap(),
        ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![true, true, false, false, false, false])
            .unwrap()
    );
    assert!(mask.get_at(&[0, 0]).unwrap());
    assert_eq!(
        <MaskExpression<f32> as Lattice<bool>>::get_slice(&mask, &[0, 0], &[2, 1], &[1, 2])
            .unwrap(),
        ArrayD::from_shape_vec(IxDyn(&[2, 1]), vec![true, false]).unwrap()
    );

    let all = ImageExprBuilder::from_image(&image)
        .unwrap()
        .gt_scalar(-1.0)
        .all_reduce()
        .compile()
        .unwrap();
    assert!(!all.get_at(&[]).unwrap());

    let any = ImageExprBuilder::from_image(&image)
        .unwrap()
        .gt_scalar(4.5)
        .any_reduce()
        .compile()
        .unwrap();
    assert!(any.get_at(&[]).unwrap());
}

#[test]
fn compiled_conditional_replace_and_mask_counts_work() {
    let image = make_temp_image(vec![2, 2], vec![1.0, 2.0, 3.0, 4.0]);
    let mask = ImageExprBuilder::from_image(&image).unwrap().gt_scalar(2.0);

    let conditional = ImageExprBuilder::iif(
        mask.clone(),
        ImageExprBuilder::from_image(&image)
            .unwrap()
            .add_scalar(100.0),
        ImageExprBuilder::from_image(&image).unwrap(),
    )
    .unwrap()
    .compile()
    .unwrap();
    assert_eq!(
        conditional.get().unwrap(),
        ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 103.0, 104.0]).unwrap()
    );

    let replacement_mask =
        ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![true, false, true, false]).unwrap();
    let replace = ImageExprBuilder::from_image(&image)
        .unwrap()
        .replace(
            ImageExprBuilder::from_image(&image)
                .unwrap()
                .add_scalar(10.0),
            replacement_mask,
        )
        .unwrap()
        .compile()
        .unwrap();
    assert_eq!(
        replace.get().unwrap(),
        ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 12.0, 3.0, 14.0]).unwrap()
    );

    let ntrue = ImageExprBuilder::ntrue(mask.clone()).compile().unwrap();
    let nfalse = ImageExprBuilder::nfalse(mask).compile().unwrap();
    assert_eq!(ntrue.get_at(&[]).unwrap(), 2.0);
    assert_eq!(nfalse.get_at(&[]).unwrap(), 2.0);
}

#[test]
fn compiled_custom_ops_and_reductions_work() {
    let image = make_temp_image(vec![2, 2], vec![10.0, 20.0, 30.0, 40.0]);
    let mapped = ImageExprBuilder::map(&image, |value| value + 1.0)
        .unwrap()
        .compile()
        .unwrap();
    assert_eq!(
        mapped.get().unwrap(),
        ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![11.0, 21.0, 31.0, 41.0]).unwrap()
    );

    let zipped = ImageExprBuilder::zip(&image, &image, |lhs, rhs| lhs + rhs * 0.5)
        .unwrap()
        .compile()
        .unwrap();
    assert_eq!(zipped.get_at(&[1, 0]).unwrap(), 45.0);

    let sum = ImageExprBuilder::from_image(&image)
        .unwrap()
        .sum_reduce()
        .compile()
        .unwrap();
    let mean = ImageExprBuilder::from_image(&image)
        .unwrap()
        .mean_reduce()
        .compile()
        .unwrap();
    let min = ImageExprBuilder::from_image(&image)
        .unwrap()
        .min_reduce()
        .compile()
        .unwrap();
    let max = ImageExprBuilder::from_image(&image)
        .unwrap()
        .max_reduce()
        .compile()
        .unwrap();
    let median = ImageExprBuilder::from_image(&image)
        .unwrap()
        .median_reduce()
        .compile()
        .unwrap();
    let fractile = ImageExprBuilder::from_image(&image)
        .unwrap()
        .fractile(0.5)
        .compile()
        .unwrap();
    let fractile_range = ImageExprBuilder::from_image(&image)
        .unwrap()
        .fractile_range(0.25, 0.75)
        .compile()
        .unwrap();
    assert_eq!(sum.get_at(&[]).unwrap(), 100.0);
    assert_eq!(mean.get_at(&[]).unwrap(), 25.0);
    assert_eq!(min.get_at(&[]).unwrap(), 10.0);
    assert_eq!(max.get_at(&[]).unwrap(), 40.0);
    // CASA 6.7.6.14 (casacore LatticeFractile) averages the two middle
    // elements for even-N median and fractile(..., 0.5): (20 + 30) / 2 = 25.0.
    // The pre-branch test expected 20.0 from floor-index selection.
    assert_eq!(median.get_at(&[]).unwrap(), 25.0);
    assert_eq!(fractile.get_at(&[]).unwrap(), 25.0);
    assert_eq!(fractile_range.get_at(&[]).unwrap(), 20.0);
}

#[test]
fn compiled_save_as_preserves_metadata_and_mask() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("compiled.image");
    let mut image = TempImage::<f32>::new(
        vec![2, 2],
        make_coords(),
        casa_lattices::TempStoragePolicy::Memory,
    )
    .unwrap();
    image
        .put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
            &[0, 0],
        )
        .unwrap();
    image.make_mask("quality", true, true).unwrap();
    image
        .put_mask(
            "quality",
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![true, false, true, false]).unwrap(),
        )
        .unwrap();

    let mut compiled = ImageExprBuilder::from_image(&image)
        .unwrap()
        .add_scalar(5.0)
        .compile()
        .unwrap();
    compiled.set_execution_policy(ExecutionPolicy::Serial);
    let saved = compiled.save_as(&path).unwrap();

    assert_eq!(saved.default_mask_name().as_deref(), Some("compiled_mask"));
    assert_eq!(
        saved.get().unwrap(),
        ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![6.0, 7.0, 8.0, 9.0]).unwrap()
    );
    assert_eq!(
        saved.default_mask().unwrap().unwrap(),
        ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![true, false, true, false]).unwrap()
    );
    assert_eq!(saved.name(), Some(path.as_path()));
}
