// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::CoordinateType;
use casa_core::ArrayD;
use casa_lattices::{
    Lattice, LatticeMut, ScratchSpace, TempLattice, TempStoragePolicy, TraversalCursorIter,
    TraversalSpec,
};
use ndarray::IxDyn;

use crate::{ImageError, ImageInterface, ImagePixel, MutableImageInterface};

use super::{SubimagePlan, SubimageRegion, region::SubimageSelection};

/// The overlap between a source selection and the image receiving it.
pub struct ImageInsertion {
    source_selection: SubimageSelection,
    from: Vec<usize>,
    to: Vec<usize>,
    shape: Vec<usize>,
    stride: Vec<usize>,
}

impl ImageInsertion {
    /// Resolve source geometry, align or round the requested place, and clip.
    /// An empty plan has the same successful no-op result as CASA insert.
    /// # Errors
    /// The source region, dimensions, Stokes shape or destination is invalid.
    pub fn new<T: ImagePixel>(
        source: &dyn ImageInterface<T>,
        target: &dyn ImageInterface<T>,
        region: Option<SubimageRegion<'_>>,
        locate: &[f64],
    ) -> Result<Option<Self>, ImageError> {
        let (selection, stride) = SubimageSelection::resolve_for_insert(source, target, region)?;
        let dimensions = target.ndim();
        if selection.shape.len() != dimensions {
            return Err(ImageError::CasaSelection(
                "Exception: The input and output images must have the same number of dimensions."
                    .into(),
            ));
        }
        let by_ref = locate.is_empty() || matches!(locate, [value] if *value < 0.0);
        let (selection, source_ref) = if by_ref {
            let plan = SubimagePlan::from_selection(source, None, selection, false, &[], None)?;
            let pixels = plan
                .coordinates
                .reference_pixel()
                .into_iter()
                .zip(&stride)
                .map(|(pixel, &step)| pixel / step as f64)
                .collect();
            (plan.selection, pixels)
        } else {
            (selection, source.coordinates().reference_pixel())
        };
        let target_ref = target.coordinates().reference_pixel();
        if source_ref.len() != dimensions || target_ref.len() != dimensions {
            return Err(ImageError::InvalidMetadata(
                "Image coordinates do not match the pixel dimensions".into(),
            ));
        }
        let mut limits = Vec::with_capacity(dimensions);
        for (axis, (&input, &output)) in selection.shape.iter().zip(target.shape()).enumerate() {
            if target.coordinates().pixel_axis_type(axis) == Some(CoordinateType::Stokes)
                && input != output
            {
                return Err(ImageError::CasaSelection(
                    "Exception: It is not possible to change the shape of the Stokes axis.".into(),
                ));
            }
            let origin = if by_ref {
                target_ref[axis] - source_ref[axis]
            } else {
                locate
                    .get(axis)
                    .copied()
                    .unwrap_or_else(|| (output as f64 - input as f64) / 2.0)
            };
            if !origin.is_finite() {
                return Err(ImageError::CasaSelection(
                    "Image insert location must be finite".into(),
                ));
            }
            // The integral conversion truncates toward zero, including for
            // negative locations after the half-pixel shift.
            let first = (origin + 0.5) as i64;
            let last = first.saturating_add(input as i64 - 1);
            limits.push((first, last, output as i64));
        }
        // CASA treats this as a no-op when no endpoint is inside a target axis.
        if !limits.iter().any(|&(first, last, length)| {
            (0..length).contains(&first) || (0..length).contains(&last)
        }) {
            return Ok(None);
        }

        let mut from = Vec::with_capacity(dimensions);
        let mut to = Vec::with_capacity(dimensions);
        let mut shape = Vec::with_capacity(dimensions);
        for (axis, (first, last, length)) in limits.into_iter().enumerate() {
            let low = first.max(0);
            let high = last.min(length - 1);
            if high < low {
                return Err(ImageError::CasaSelection("Slicer error:end<start".into()));
            }
            from.push(selection.start[axis] + (low - first) as usize * stride[axis]);
            to.push(low as usize);
            shape.push((high - low + 1) as usize);
        }
        Ok(Some(Self {
            source_selection: selection,
            from,
            to,
            shape,
            stride,
        }))
    }

    /// Insert the selected pixels and transfer validity to an existing default mask.
    /// # Errors
    /// Reading or writing the selected pixels or mask fails.
    pub fn copy<T: ImagePixel>(
        self,
        source: &dyn ImageInterface<T>,
        target: &mut dyn MutableImageInterface<T>,
    ) -> Result<(), ImageError> {
        let mut mask = target.active_pixel_mask_name();
        if let Some(name) = &mask
            && !target.named_mask_is_writable(name)?
        {
            mask = None;
        }
        let aliased = source.name().zip(target.name()).is_some_and(|(src, dst)| {
            src == dst
                || std::fs::canonicalize(src)
                    .ok()
                    .zip(std::fs::canonicalize(dst).ok())
                    .is_some_and(|(src, dst)| src == dst)
        });
        // A separate backing store preserves overlapping self-insertions.
        // Ordinary native and FITS inputs can transfer each cursor directly.
        let policy = || TempStoragePolicy::Auto {
            memory_budget_bytes: 8 * 1024 * 1024,
            scratch: ScratchSpace::SystemTemp,
        };
        let mut snapshot = aliased
            .then(|| TempLattice::new(self.shape.clone(), policy()))
            .transpose()?;
        let mut snapshot_mask = (aliased && mask.is_some())
            .then(|| TempLattice::new(self.shape.clone(), policy()))
            .transpose()?;
        let mut remaining = 1024 * 1024 / T::ELEM_SIZE;
        let cursor_shape: Vec<_> = source
            .nice_cursor_shape()
            .iter()
            .zip(&self.shape)
            .map(|(&preferred, &overlap)| {
                let length = preferred.max(1).min(overlap).min(remaining);
                remaining /= length;
                length
            })
            .collect();
        let cursors = || {
            TraversalCursorIter::new(
                self.shape.clone(),
                cursor_shape.clone(),
                TraversalSpec::tiles(),
            )
        };
        let stride = vec![1; self.shape.len()];
        for cursor in cursors() {
            let cursor = cursor?;
            let from: Vec<_> = self
                .from
                .iter()
                .zip(&cursor.position)
                .zip(&self.stride)
                .map(|((offset, position), step)| offset + position * step)
                .collect();
            let pixels = source.get_slice(&from, &cursor.shape, &self.stride)?;
            let valid = if mask.is_some() {
                let mut valid = source.get_mask_slice(&from, &cursor.shape, &self.stride)?;
                // A strided LCSlicer selects only a rectangular lattice section.
                if self.stride == stride {
                    self.source_selection
                        .apply_window_masks(&mut valid, &from, &cursor.shape);
                }
                Some(valid.unwrap_or_else(|| ArrayD::from_elem(IxDyn(&cursor.shape), true)))
            } else {
                None
            };
            if let Some(saved) = &mut snapshot {
                saved.put_slice(&pixels, &cursor.position)?;
                if let Some((saved, valid)) = snapshot_mask.as_mut().zip(valid) {
                    saved.put_slice(&valid, &cursor.position)?;
                }
            } else {
                self.write_window(
                    target,
                    &cursor.position,
                    &pixels,
                    mask.as_deref().zip(valid.as_ref()),
                )?;
            }
        }
        if let Some(saved) = snapshot {
            for cursor in cursors() {
                let cursor = cursor?;
                let pixels = saved.get_slice(&cursor.position, &cursor.shape, &stride)?;
                let valid = snapshot_mask
                    .as_ref()
                    .map(|mask| mask.get_slice(&cursor.position, &cursor.shape, &stride))
                    .transpose()?;
                self.write_window(
                    target,
                    &cursor.position,
                    &pixels,
                    mask.as_deref().zip(valid.as_ref()),
                )?;
            }
        }
        Ok(())
    }

    fn write_window<T: ImagePixel>(
        &self,
        target: &mut dyn MutableImageInterface<T>,
        position: &[usize],
        pixels: &ArrayD<T>,
        mask: Option<(&str, &ArrayD<bool>)>,
    ) -> Result<(), ImageError> {
        let to: Vec<_> = self
            .to
            .iter()
            .zip(position)
            .map(|(offset, position)| offset + position)
            .collect();
        target.put_slice(pixels, &to)?;
        if let Some((name, valid)) = mask {
            target.put_mask_slice(name, valid, &to)?;
        }
        Ok(())
    }
}
