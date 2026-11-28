// SPDX-License-Identifier: LGPL-3.0-or-later
//! Cancellation at bounded units of coordinate mapping and resampling.

use casa_core::cancellation::Cancellation;
use casa_images::ImageError;
use ndarray::ArrayD;

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
    for _start in (0..count).step_by(BATCH_SIZE) {
        check(cancellation)?;
        #[cfg(feature = "imregrid-test-hooks")]
        crate::imregrid_test_hooks::observe(
            crate::imregrid_test_hooks::Checkpoint::CoordinateBatchStarted(_start),
        );
        for position in positions.by_ref().take(BATCH_SIZE) {
            grid.push(position?);
        }
        #[cfg(feature = "imregrid-test-hooks")]
        crate::imregrid_test_hooks::observe(
            crate::imregrid_test_hooks::Checkpoint::CoordinateBatchFinished(_start),
        );
    }
    check(cancellation)?;
    Ok(grid)
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
