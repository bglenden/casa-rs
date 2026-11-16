// SPDX-License-Identifier: LGPL-3.0-or-later
//! Counting-law access to the same existing CLEAN-mask reader compile uses.

use std::path::Path;

/// An opened mask header, before any pixels are read.
pub struct Input(crate::restart::MaskInput);

impl Input {
    /// Open the header with the production reader's bounded tile cache.
    ///
    /// # Errors
    /// The image cannot be opened as a real single-precision image.
    pub fn open(path: &Path) -> Result<Self, crate::PrepareError> {
        crate::restart::MaskInput::open(path).map(Self)
    }

    /// Bytes admitted before reading and converting the pixels.
    pub fn reading_bytes(&self) -> u64 {
        self.0.reading_bytes()
    }

    /// Bytes retained for the static mask plan and its run-time clone, of
    /// `planes` planes of `cells` pixels each.
    pub fn retained_bytes(planes: casa_imaging_deconvolution::MaskPlanes, cells: usize) -> u64 {
        crate::restart::MaskInput::retained_bytes(planes, cells)
    }

    /// Read exactly the checked grid, in the reconstruction mask's canonical
    /// order: one plane when every plane agrees, else every plane.
    ///
    /// # Errors
    /// Pixel I/O fails or the mask is not binary.
    pub fn read(
        self,
    ) -> Result<(casa_imaging_deconvolution::MaskPlanes, Box<[bool]>), crate::PrepareError> {
        self.0.read()
    }
}
