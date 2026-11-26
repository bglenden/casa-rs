// SPDX-License-Identifier: LGPL-3.0-or-later

//! Section access for a virtual image concatenation.

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_coordinates::CoordinateSystem;
use casa_core::RecordValue;
use casa_lattices::{Lattice, LatticeError, TraversalCursorIter, TraversalSpec};
use casa_tables::LockType;
use ndarray::{ArrayD, Axis, IxDyn, Slice};

use crate::concatenate::ConcatenationPlan;
use crate::image_expr::{bounded_read_cursor, validate_slice_request};
use crate::{ImageError, ImageInfo, ImageInterface, ImagePixel};

/// Physical access leases held before a concatenation lock request.
pub enum ConcatLockState {
    /// A single table, or a lattice with no physical lock.
    Leaf(Option<LockType>),
    /// The independent leases of a nested concatenation's members.
    Nested(Vec<ConcatLockState>),
}

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

    /// Whether this member's intrinsic mask supports writes.
    fn mask_writable(&self) -> bool;
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

    /// Write the intrinsic validity of a local rectangular section.
    fn write_mask(&self, values: &ArrayD<bool>, start: &[usize]) -> Result<(), ImageError>;

    /// Check whether the member can accept an image-info update.
    fn check_image_info_write(&self) -> Result<(), ImageError> {
        Ok(())
    }

    /// Update image metadata using the member's section of the joined beam grid.
    fn write_image_info(
        &self,
        info: &ImageInfo,
        record: &RecordValue,
        axis: usize,
        offset: usize,
    ) -> Result<(), ImageError>;
    /// Take a table access lock if this member uses locking.
    fn lock(&self, kind: LockType, attempts: u32) -> Result<bool, ImageError>;

    /// Release the access leases a temporary closure normally releases.
    fn close_after_lock(&self) -> Result<(), ImageError> {
        self.unlock()
    }

    /// Report a member lock.
    fn has_lock(&self, kind: LockType) -> bool;

    /// Whether the member reports both locks without acquiring a storage lease.
    fn has_advisory_lock(&self) -> bool {
        false
    }

    /// Files and image directories needed by any saved-expression member.
    fn expression_operands(&self) -> &[PathBuf] {
        &[]
    }

    /// Capture the physical access leases this member holds.
    fn lock_state(&self) -> ConcatLockState;

    /// Restore a member's leases after an enclosing lock request fails.
    fn restore_locks(&self, state: ConcatLockState) -> Result<(), ImageError>;

    /// Release the access locks held by this member.
    fn unlock(&self) -> Result<(), ImageError>;

    /// Respond to a pending access-lock request without clearing explicit locks.
    fn release_requested_lock(&self) -> Result<(), ImageError>;
}

/// An image whose pixels and mask are supplied by ordered member images.
pub struct ConcatImage<T: ImagePixel> {
    path: PathBuf,
    directory: Option<std::fs::File>,
    members: Vec<Arc<dyn ConcatMember<T>>>,
    operand_entries: Vec<PathBuf>,
    boundaries: Vec<usize>,
    axis: usize,
    shape: Vec<usize>,
    cursor_shape: Vec<usize>,
    coordinates: CoordinateSystem,
    info: ImageInfo,
    misc: RecordValue,
    units: String,
    temp_close: bool,
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
        operand_entries: Vec<PathBuf>,
        misc: RecordValue,
        units: String,
        temp_close: bool,
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
            operand_entries,
            boundaries,
            axis: plan.axis,
            shape: plan.shape,
            cursor_shape,
            coordinates: plan.coordinates,
            info: plan.image_info,
            misc,
            units,
            temp_close,
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

    /// Whether implicit section operations close their member handles.
    pub fn temp_close(&self) -> bool {
        self.temp_close
    }

    /// Whether all members have writable intrinsic masks.
    pub fn mask_writable(&self) -> bool {
        self.members.iter().all(|member| member.mask_writable())
    }

    pub(crate) fn write_mask_slice(
        &self,
        data: &ArrayD<bool>,
        start: &[usize],
    ) -> Result<(), ImageError> {
        if !self.mask_writable() {
            return Err(ImageError::CasaSelection(
                "The mask is not writable. Probably an ImageExpr or SubImage".into(),
            ));
        }
        let stride = vec![1; self.ndim()];
        validate_slice_request(&self.shape, start, data.shape(), &stride)?;
        for (member, samples, start, _) in self.windows(start, data.shape(), &stride) {
            let section = data
                .slice_axis(Axis(self.axis), Slice::from(samples))
                .to_owned();
            member.write_mask(&section, &start)?;
        }
        Ok(())
    }

    pub(crate) fn write_image_info(
        &self,
        info: &ImageInfo,
        record: &RecordValue,
    ) -> Result<(), ImageError> {
        self.check_image_info_write()?;
        for (member, &offset) in self.members.iter().zip(&self.boundaries) {
            member.write_image_info(info, record, self.axis, offset)?;
        }
        Ok(())
    }

    /// Preflight the member metadata writes before updating any image.
    ///
    /// # Errors
    /// Returns an error when a member is no longer writable.
    pub fn check_image_info_write(&self) -> Result<(), ImageError> {
        self.check_directory()?;
        self.members
            .iter()
            .try_for_each(|member| member.check_image_info_write())
    }

    pub(crate) fn write_misc_info(&self, misc: &RecordValue) -> Result<(), ImageError> {
        self.check_directory()?;
        if self.temp_close && !self.members[0].lock(LockType::Read, 0)? {
            return Err(ImageError::CasaSelection(
                "Could not acquire the image Read lock".into(),
            ));
        }
        crate::concat_file::update_misc_info(&self.path, misc)
    }

    /// Verify that the attached manifest directory still occupies its path.
    ///
    /// # Errors
    /// Returns an error if the directory was moved or replaced.
    pub fn check_directory(&self) -> Result<(), ImageError> {
        let current = self
            .path
            .symlink_metadata()
            .map_err(|error| ImageError::Io(error.to_string()))?;
        if !self.matches_directory_metadata(&current) {
            return Err(ImageError::Io(format!(
                "The virtual concatenation directory {} changed",
                self.path.display(),
            )));
        }
        Ok(())
    }

    /// Check a directory entry against the identity held by this attachment.
    pub fn matches_directory_metadata(&self, current: &std::fs::Metadata) -> bool {
        if !current.is_dir() {
            return false;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            self.directory
                .as_ref()
                .and_then(|dir| dir.metadata().ok())
                .is_some_and(|expected| {
                    (expected.dev(), expected.ino()) == (current.dev(), current.ino())
                })
        }
        #[cfg(not(unix))]
        {
            let _ = &self.directory;
            true
        }
    }
    /// Lock the members in manifest order. Release newly acquired locks if
    /// any member refuses the request.
    ///
    /// # Errors
    /// Returns an error if a member's access lock cannot be used.
    pub fn lock(&self, kind: LockType, attempts: u32) -> Result<bool, ImageError> {
        let mut seen = std::collections::HashSet::new();
        let mut acquired = Vec::new();
        for member in &self.members {
            if !seen.insert(member.path()) {
                continue;
            }
            let before = member.lock_state();
            let result = member.lock(kind, attempts);
            if self.temp_close {
                member.close_after_lock()?;
            }
            match result {
                Ok(true) => acquired.push((member, before)),
                refused => {
                    for (member, state) in acquired.into_iter().rev() {
                        member.restore_locks(state)?;
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

    /// Whether a member reports locks even after temporary closure.
    pub fn has_advisory_lock(&self) -> bool {
        self.members.iter().any(|member| member.has_advisory_lock())
    }

    /// Files and image directories referenced by saved expressions in the graph.
    pub fn expression_operands(&self) -> impl Iterator<Item = &PathBuf> {
        self.members
            .iter()
            .flat_map(|member| member.expression_operands())
    }

    /// Direct and transitive operand paths of the concatenation.
    pub fn operand_paths(&self) -> impl Iterator<Item = &Path> {
        self.members
            .iter()
            .map(|member| member.path())
            .chain(self.operand_entries.iter().map(PathBuf::as_path))
    }

    /// Capture each physical member's access leases before a lock request.
    pub fn lock_states(&self) -> Vec<ConcatLockState> {
        self.members
            .iter()
            .map(|member| member.lock_state())
            .collect()
    }

    /// Restore each member's original access leases after a failed request.
    ///
    /// # Errors
    /// Returns an error if an original lock cannot be restored.
    pub fn restore_locks(&self, states: Vec<ConcatLockState>) -> Result<(), ImageError> {
        self.members
            .iter()
            .zip(states)
            .rev()
            .try_for_each(|(member, state)| member.restore_locks(state))
    }

    /// Release access locks on all members.
    ///
    /// # Errors
    /// Returns an error if any member fails to unlock.
    pub fn unlock(&self) -> Result<(), ImageError> {
        self.members.iter().try_for_each(|member| member.unlock())
    }

    /// Release locks requested by other table holders.
    ///
    /// # Errors
    /// Returns an error if a member fails to release a requested lock.
    pub fn release_requested_lock(&self) -> Result<(), ImageError> {
        self.members
            .iter()
            .try_for_each(|member| member.release_requested_lock())
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
