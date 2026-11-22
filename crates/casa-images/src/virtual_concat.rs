// SPDX-License-Identifier: LGPL-3.0-or-later

//! Section access for a virtual image concatenation.

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_coordinates::CoordinateSystem;
use casa_lattices::{Lattice, LatticeError, TraversalCursorIter, TraversalSpec};
use casa_tables::LockType;
use casa_types::RecordValue;
use ndarray::{ArrayD, Axis, IxDyn, Slice};

use crate::concatenate::ConcatenationPlan;
use crate::image_expr::{bounded_read_cursor, validate_slice_request};
use crate::{ImageError, ImageInfo, ImageInterface, ImagePixel};

/// Section access to one member in a virtual concatenation.
pub trait ConcatMember<T: ImagePixel>: Send + Sync {
    /// The member's resolved (physical) image path.
    fn path(&self) -> &Path;
    /// The member's pixel shape.
    fn shape(&self) -> &[usize];

    /// Whether the member supports pixel writes.
    fn is_writable(&self) -> bool;

    /// Whether the member has nontrivial intrinsic validity.
    fn has_mask(&self) -> bool;

    /// Read pixels from a local section.
    fn read(
        &self,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, ImageError>;

    /// Read the intrinsic validity of a local section.
    fn read_mask(
        &self,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, ImageError>;

    /// Write pixels to a local rectangular section.
    fn write(&self, values: &ArrayD<T>, start: &[usize]) -> Result<(), ImageError>;

    /// Take a table access lock if this member uses locking.
    fn lock(&self, kind: LockType, attempts: u32) -> Result<bool, ImageError>;

    /// Report a member lock.
    fn has_lock(&self, kind: LockType) -> bool;

    /// Release the access locks held by this member.
    fn unlock(&self) -> Result<(), ImageError>;
}

/// An image whose pixels and mask are supplied by ordered member images.
pub struct ConcatImage<T: ImagePixel> {
    path: PathBuf,
    directory: Option<std::fs::File>,
    members: Vec<Arc<dyn ConcatMember<T>>>,
    boundaries: Vec<usize>,
    axis: usize,
    shape: Vec<usize>,
    cursor_shape: Vec<usize>,
    coordinates: CoordinateSystem,
    info: ImageInfo,
    misc: RecordValue,
    units: String,
}

impl<T: ImagePixel> ConcatImage<T> {
    /// Start a virtual image with the checked geometry and ordered members.
    ///
    /// # Errors
    /// Returns an error if the members no longer match the geometry.
    pub fn new(
        path: PathBuf,
        plan: ConcatenationPlan,
        members: Vec<Arc<dyn ConcatMember<T>>>,
        misc: RecordValue,
        units: String,
    ) -> Result<Self, ImageError> {
        let mut boundaries = vec![0usize];
        for member in &members {
            let shape = member.shape();
            if shape.len() != plan.shape.len()
                || shape
                    .iter()
                    .zip(&plan.shape)
                    .enumerate()
                    .any(|(axis, (local, total))| axis != plan.axis && local != total)
            {
                return Err(ImageError::CasaSelection(
                    "Lattice shapes inconsistent".into(),
                ));
            }
            let end = boundaries
                .last()
                .unwrap()
                .checked_add(shape[plan.axis])
                .ok_or_else(|| {
                    ImageError::InvalidMetadata("Concatenated axis length overflow".into())
                })?;
            boundaries.push(end);
        }
        if boundaries.last() != plan.shape.get(plan.axis) {
            return Err(ImageError::CasaSelection(
                "Lattice shapes inconsistent".into(),
            ));
        }

        let cursor_shape = bounded_read_cursor(plan.shape.clone())?;
        #[cfg(unix)]
        let directory = path
            .is_dir()
            .then(|| casa_tables::publication::open_directory_identity(&path))
            .transpose()
            .map_err(|error| ImageError::Io(error.to_string()))?;
        #[cfg(not(unix))]
        let directory = None;
        Ok(Self {
            path,
            directory,
            members,
            boundaries,
            axis: plan.axis,
            shape: plan.shape,
            cursor_shape,
            coordinates: plan.coordinates,
            info: plan.image_info,
            misc,
            units,
        })
    }

    fn windows<'a>(
        &'a self,
        start: &'a [usize],
        count: &'a [usize],
        stride: &'a [usize],
    ) -> impl Iterator<
        Item = (
            &'a dyn ConcatMember<T>,
            Range<usize>,
            Vec<usize>,
            Vec<usize>,
        ),
    > + 'a {
        let axis = self.axis;
        self.members
            .iter()
            .zip(self.boundaries.windows(2))
            .filter_map(move |(member, bounds)| {
                let samples =
                    member_samples(bounds[0]..bounds[1], start[axis], count[axis], stride[axis]);
                if samples.is_empty() {
                    return None;
                }
                let mut local_start = start.to_vec();
                local_start[axis] = start[axis] + samples.start * stride[axis] - bounds[0];
                let mut local_count = count.to_vec();
                local_count[axis] = samples.len();
                Some((member.as_ref(), samples, local_start, local_count))
            })
    }

    pub(crate) fn write_slice(
        &self,
        data: &ArrayD<T>,
        start: &[usize],
    ) -> Result<(), LatticeError> {
        if !self.is_writable() {
            return Err(LatticeError::Table(
                "Some of the underlying lattices are not writable".into(),
            ));
        }
        let stride = vec![1; self.ndim()];
        validate_slice_request(&self.shape, start, data.shape(), &stride)?;
        for (member, samples, start, _) in self.windows(start, data.shape(), &stride) {
            let section = data
                .slice_axis(Axis(self.axis), Slice::from(samples))
                .to_owned();
            member.write(&section, &start).map_err(lattice_error)?;
        }
        Ok(())
    }

    pub(crate) fn fill(&self, value: T) -> Result<(), LatticeError> {
        for cursor in TraversalCursorIter::new(
            self.shape.clone(),
            self.nice_cursor_shape(),
            TraversalSpec::tiles(),
        ) {
            let cursor = cursor?;
            let data = ArrayD::from_elem(IxDyn(&cursor.shape), value);
            self.write_slice(&data, &cursor.position)?;
        }
        Ok(())
    }

    pub(crate) fn write_misc_info(&self, misc: &RecordValue) -> Result<(), ImageError> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let current = self
                .path
                .metadata()
                .map_err(|error| ImageError::Io(error.to_string()))?;
            if !self
                .directory
                .as_ref()
                .and_then(|dir| dir.metadata().ok())
                .is_some_and(|expected| {
                    (expected.dev(), expected.ino()) == (current.dev(), current.ino())
                })
            {
                return Err(ImageError::Io(
                    "The virtual concatenation directory changed".into(),
                ));
            }
        }
        #[cfg(not(unix))]
        let _ = &self.directory;
        crate::concat_file::update_misc_info(&self.path, misc)
    }
    /// Lock the members in manifest order. Release newly acquired locks if
    /// any member refuses the request.
    ///
    /// # Errors
    /// Returns an error if a member's access lock cannot be used.
    pub fn lock(&self, kind: LockType, attempts: u32) -> Result<bool, ImageError> {
        let mut seen = std::collections::HashSet::new();
        let mut acquired: Vec<(&dyn ConcatMember<T>, bool)> = Vec::new();
        for member in &self.members {
            if !seen.insert(member.path()) {
                continue;
            }
            let read = member.has_lock(LockType::Read);
            let write = member.has_lock(LockType::Write);
            match member.lock(kind, attempts) {
                Ok(true) => {
                    let already_held = match kind {
                        LockType::Read => read,
                        LockType::Write => write,
                    };
                    if !already_held {
                        acquired.push((member.as_ref(), read));
                    }
                }
                refused => {
                    for (member, read) in acquired.into_iter().rev() {
                        member.unlock()?;
                        if read {
                            member.lock(LockType::Read, 1)?;
                        }
                    }
                    return refused;
                }
            }
        }
        Ok(true)
    }

    /// Return whether any member holds the requested access lock.
    pub fn has_lock(&self, kind: LockType) -> bool {
        self.members.iter().any(|member| member.has_lock(kind))
    }

    /// Release access locks on all members.
    ///
    /// # Errors
    /// Returns an error if any member fails to unlock.
    pub fn unlock(&self) -> Result<(), ImageError> {
        self.members.iter().try_for_each(|member| member.unlock())
    }
}

impl<T: ImagePixel> Lattice<T> for ConcatImage<T> {
    fn shape(&self) -> &[usize] {
        &self.shape
    }

    fn is_persistent(&self) -> bool {
        true
    }

    fn is_writable(&self) -> bool {
        self.members.iter().all(|member| member.is_writable())
    }

    fn nice_cursor_shape(&self) -> Vec<usize> {
        self.cursor_shape.clone()
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        let one = vec![1; self.ndim()];
        let section = self.get_slice(position, &one, &one)?;
        Ok(section[IxDyn(&vec![0; self.ndim()])])
    }

    fn get_slice(
        &self,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        validate_slice_request(&self.shape, start, count, stride)?;
        let mut output = ArrayD::from_elem(IxDyn(count), T::default_value());
        for (member, samples, start, count) in self.windows(start, count, stride) {
            let pixels = member.read(&start, &count, stride).map_err(lattice_error)?;
            output
                .slice_axis_mut(Axis(self.axis), Slice::from(samples))
                .assign(&pixels);
        }
        Ok(output)
    }
}

impl<T: ImagePixel> ImageInterface<T> for ConcatImage<T> {
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
    fn coordinates(&self) -> &CoordinateSystem {
        &self.coordinates
    }
    fn name(&self) -> Option<&Path> {
        Some(&self.path)
    }
    fn units(&self) -> &str {
        &self.units
    }
    fn image_info(&self) -> Result<ImageInfo, ImageError> {
        Ok(self.info.clone())
    }
    fn misc_info(&self) -> RecordValue {
        self.misc.clone()
    }

    fn has_pixel_mask(&self) -> bool {
        self.members.iter().any(|member| member.has_mask())
    }

    fn default_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        self.get_mask_slice(&vec![0; self.ndim()], &self.shape, &vec![1; self.ndim()])
    }

    fn get_mask_slice(
        &self,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, ImageError> {
        if !self.has_pixel_mask() {
            return Ok(None);
        }
        validate_slice_request(&self.shape, start, count, stride)?;
        let mut output = ArrayD::from_elem(IxDyn(count), true);
        for (member, samples, start, count) in self.windows(start, count, stride) {
            if let Some(mask) = member.read_mask(&start, &count, stride)? {
                output
                    .slice_axis_mut(Axis(self.axis), Slice::from(samples))
                    .assign(&mask);
            }
        }
        Ok(Some(output))
    }
}

fn lattice_error(error: ImageError) -> LatticeError {
    LatticeError::Table(error.to_string())
}

fn member_samples(member: Range<usize>, start: usize, count: usize, step: usize) -> Range<usize> {
    let index = |coordinate: usize| coordinate.saturating_sub(start).div_ceil(step).min(count);
    index(member.start)..index(member.end)
}
