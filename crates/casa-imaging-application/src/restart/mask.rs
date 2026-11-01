// SPDX-License-Identifier: LGPL-3.0-or-later
//! An existing binary CLEAN mask, read once on its checked grid. CASA's
//! SynthesisDeconvolver::setupMask reuses it when no new user mask is given.

use std::path::{Path, PathBuf};

use casa_images::PagedImage;

use crate::PrepareError;

const CACHE_BYTES: usize = 64 * 1024;

pub(crate) struct MaskInput {
    path: PathBuf,
    pixels: u64,
    tile_bytes: u64,
}

impl MaskInput {
    pub(crate) fn open(path: &Path) -> Result<Self, PrepareError> {
        let image = PagedImage::<f32>::open_with_cache(path, CACHE_BYTES).map_err(|error| {
            PrepareError::Restart {
                path: path.into(),
                reason: error.to_string(),
            }
        })?;
        let pixels = image.nelements() as u64;
        let tile_bytes = image
            .tile_shape()
            .iter()
            .fold(4_u64, |n, &side| n.saturating_mul(side as u64));
        Ok(Self {
            path: path.into(),
            pixels,
            tile_bytes,
        })
    }

    /// The read and its conversion, two support copies (compile and run clone
    /// their plans), the bounded tile cache and two decoded/I/O tile buffers.
    pub(crate) fn reading_bytes(&self) -> u64 {
        self.pixels
            .saturating_mul(2 * 4 + 2)
            .saturating_add(self.tile_bytes.saturating_mul(2))
            .saturating_add(CACHE_BYTES as u64)
    }

    /// Both the original and the cloned static reconstruction-mask plan.
    pub(crate) fn retained_bytes(&self) -> u64 {
        self.pixels.saturating_mul(2)
    }

    pub(crate) fn read(self) -> Result<Box<[bool]>, PrepareError> {
        let refuse = |reason| PrepareError::Restart {
            path: self.path.clone(),
            reason,
        };
        // Reopen after admission: even the bounded tiled reader can allocate one
        // physical tile larger than its requested cache budget.
        let image = PagedImage::<f32>::open_with_cache(&self.path, CACHE_BYTES)
            .map_err(|error| refuse(error.to_string()))?;
        let pixels = image.get().map_err(|error| refuse(error.to_string()))?;
        // Existing reconstruction support is x-major (y fastest), unlike model
        // samples. Array iteration follows that logical order for either layout.
        pixels
            .iter()
            .map(|&value| match value {
                0.0 => Ok(false),
                1.0 => Ok(true),
                _ => Err(refuse(
                    "continuation with a non-binary CLEAN mask is not implemented yet".into(),
                )),
            })
            .collect()
    }
}
