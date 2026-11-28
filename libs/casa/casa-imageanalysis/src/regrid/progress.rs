// SPDX-License-Identifier: LGPL-3.0-or-later
//! Cancellation at bounded units of coordinate mapping and resampling.

use casa_core::cancellation::Cancellation;
use casa_images::ImageError;
use ndarray::{ArrayD, ArrayView2, ArrayViewMut2};

pub(super) const BATCH_SIZE: usize = 4096;

pub(crate) fn check(cancellation: Option<&Cancellation>) -> Result<(), ImageError> {
    if cancellation.is_some_and(Cancellation::is_cancelled) {
        return Err(ImageError::Interrupted);
    }
    Ok(())
}

pub(super) fn coordinate_positions<T>(
    count: usize,
    positions: impl Iterator<Item = T>,
    cancellation: Option<&Cancellation>,
) -> Result<Vec<T>, ImageError> {
    if cancellation.is_none() {
        return Ok(positions.collect());
    }
    try_coordinate_positions(count, positions.map(Ok), cancellation)
}

pub(super) fn try_coordinate_positions<T>(
    count: usize,
    mut positions: impl Iterator<Item = Result<T, ImageError>>,
    cancellation: Option<&Cancellation>,
) -> Result<Vec<T>, ImageError> {
    if cancellation.is_none() {
        return positions.collect();
    }
    let mut grid = Vec::with_capacity(count);
    for start in (0..count).step_by(BATCH_SIZE) {
        coordinate_batch(start, cancellation, || {
            for position in positions.by_ref().take(BATCH_SIZE) {
                grid.push(position?);
            }
            Ok(())
        })?;
    }
    check(cancellation)?;
    Ok(grid)
}

pub(super) fn coordinate_batch<T>(
    _start: usize,
    cancellation: Option<&Cancellation>,
    run: impl FnOnce() -> Result<T, ImageError>,
) -> Result<T, ImageError> {
    check(cancellation)?;
    #[cfg(feature = "imregrid-test-hooks")]
    crate::imregrid_test_hooks::observe(
        crate::imregrid_test_hooks::Checkpoint::CoordinateBatchStarted(_start),
    );
    let result = run()?;
    #[cfg(feature = "imregrid-test-hooks")]
    crate::imregrid_test_hooks::observe(
        crate::imregrid_test_hooks::Checkpoint::CoordinateBatchFinished(_start),
    );
    Ok(result)
}

pub(super) fn read_plane<A, B>(
    source: ArrayView2<'_, A>,
    into: &mut [B],
    convert: impl Fn(&A) -> B,
    cancellation: &Cancellation,
) -> Result<(), ImageError> {
    fn read<'a, A: 'a, B>(
        mut source: impl Iterator<Item = &'a A>,
        into: &mut [B],
        convert: impl Fn(&A) -> B,
        cancellation: &Cancellation,
    ) -> Result<(), ImageError> {
        for batch in into.chunks_mut(BATCH_SIZE) {
            check(Some(cancellation))?;
            for (slot, value) in batch.iter_mut().zip(source.by_ref()) {
                *slot = convert(value);
            }
        }
        Ok(())
    }

    let source = source.reversed_axes();
    if let Some(values) = source.as_slice() {
        read(values.iter(), into, convert, cancellation)
    } else {
        read(source.into_iter(), into, convert, cancellation)
    }
}

pub(super) fn write_plane<A: Copy>(
    output: ArrayViewMut2<'_, A>,
    values: &[A],
    cancellation: &Cancellation,
) -> Result<(), ImageError> {
    let mut output = output.reversed_axes();
    if let Some(pixels) = output.as_slice_mut() {
        for (batch, values) in pixels.chunks_mut(BATCH_SIZE).zip(values.chunks(BATCH_SIZE)) {
            check(Some(cancellation))?;
            batch.copy_from_slice(values);
        }
    } else {
        let mut pixels = output.into_iter();
        for batch in values.chunks(BATCH_SIZE) {
            check(Some(cancellation))?;
            for (value, pixel) in batch.iter().zip(pixels.by_ref()) {
                *pixel = *value;
            }
        }
    }
    Ok(())
}

pub(super) fn any_valid(
    mask: &ArrayD<bool>,
    cancellation: Option<&Cancellation>,
) -> Result<bool, ImageError> {
    let mut pixels = mask.iter();
    for _ in (0..mask.len()).step_by(BATCH_SIZE) {
        check(cancellation)?;
        if pixels.by_ref().take(BATCH_SIZE).any(|&bit| bit) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn count_valid(
    mask: &ArrayD<bool>,
    cancellation: Option<&Cancellation>,
) -> Result<usize, ImageError> {
    let mut pixels = mask.iter();
    let mut count = 0;
    for _ in (0..mask.len()).step_by(BATCH_SIZE) {
        check(cancellation)?;
        count += pixels.by_ref().take(BATCH_SIZE).filter(|&&bit| bit).count();
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use ndarray::{Array2, s};

    use super::*;

    #[test]
    fn strided_plane_copy_stops_before_the_next_batch() {
        let pixels = Array2::from_shape_fn((3, BATCH_SIZE + 4), |(row, x)| row + x);
        let plane = pixels.slice(s![..;2, ..]);
        let mut values = vec![usize::MAX; plane.len()];
        let cancellation = Cancellation::new();
        let copied = Cell::new(0);
        let result = read_plane(
            plane,
            &mut values,
            |value| {
                copied.set(copied.get() + 1);
                if copied.get() == BATCH_SIZE {
                    cancellation.cancel();
                }
                *value
            },
            &cancellation,
        );
        assert!(matches!(result, Err(ImageError::Interrupted)));
        assert_eq!(copied.get(), BATCH_SIZE);
        assert_eq!(&values[..4], &[0, 2, 1, 3]);
        assert!(
            values[BATCH_SIZE..]
                .iter()
                .all(|&pixel| pixel == usize::MAX)
        );
    }
}
