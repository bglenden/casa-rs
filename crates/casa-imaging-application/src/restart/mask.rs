// SPDX-License-Identifier: LGPL-3.0-or-later
//! An existing binary CLEAN mask, read once on its checked grid. CASA's
//! SynthesisDeconvolver::setupMask reuses it when no new user mask is given.
//!
//! A mask has a plane per Stokes parameter and channel, and is reused
//! plane by plane: as one support when its planes agree, as a static mask's
//! do, else as a support for each plane, as an automatic mask's may.

use std::path::{Path, PathBuf};

use casa_images::PagedImage;
use casa_imaging_deconvolution::MaskPlanes;

use crate::PrepareError;

const CACHE_BYTES: usize = 64 * 1024;

pub(crate) struct MaskInput {
    path: PathBuf,
    shape: Vec<usize>,
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
        let tile_bytes = image
            .tile_shape()
            .iter()
            .fold(4_u64, |n, &side| n.saturating_mul(side as u64));
        Ok(Self {
            path: path.into(),
            shape: image.shape().to_vec(),
            tile_bytes,
        })
    }

    fn pixels(&self) -> u64 {
        self.shape.iter().map(|&side| side as u64).product()
    }

    /// The read of every plane and its conversion, two support copies of
    /// every plane (compile and run clone their plans), the bounded tile
    /// cache and two decoded/I/O tile buffers.
    pub(crate) fn reading_bytes(&self) -> u64 {
        self.pixels()
            .saturating_mul(2 * 4)
            .saturating_add(self.pixels().saturating_mul(2))
            .saturating_add(self.tile_bytes.saturating_mul(2))
            .saturating_add(CACHE_BYTES as u64)
    }

    /// Both the original and the cloned static reconstruction-mask plan of
    /// the `planes` of `cells` pixels each that [`Self::read`] kept.
    pub(crate) fn retained_bytes(planes: MaskPlanes, cells: usize) -> u64 {
        (cells as u64)
            .saturating_mul(planes.count() as u64)
            .saturating_mul(2)
    }

    /// The mask's support, its planes as CASA's `.mask` holds them: one
    /// plane when every Stokes plane and channel agrees, as a static mask
    /// casa-rs writes does, else a plane for each.
    pub(crate) fn read(self) -> Result<(MaskPlanes, Box<[bool]>), PrepareError> {
        let refuse = |reason| PrepareError::Restart {
            path: self.path.clone(),
            reason,
        };
        // Reopen after admission: even the bounded tiled reader can allocate one
        // physical tile larger than its requested cache budget.
        let image = PagedImage::<f32>::open_with_cache(&self.path, CACHE_BYTES)
            .map_err(|error| refuse(error.to_string()))?;
        let pixels = image.get().map_err(|error| refuse(error.to_string()))?;
        let binary = |value: f32| match value {
            0.0 => Ok(false),
            1.0 => Ok(true),
            _ => Err(refuse(
                "continuation with a non-binary CLEAN mask is not implemented yet".into(),
            )),
        };
        // The grid check holds the mask to the image's four axes,
        // [x, y, stokes, channel]; reconstruction support is x-major (y
        // fastest), unlike model samples, plane after plane, channel-major.
        let &[nx, ny, stokes, channels] = self.shape.as_slice() else {
            return Err(refuse(format!(
                "a CLEAN mask has four axes, not {:?}",
                self.shape
            )));
        };
        let mut support = Vec::with_capacity(nx * ny * stokes * channels);
        for channel in 0..channels {
            for polarization in 0..stokes {
                for x in 0..nx {
                    for y in 0..ny {
                        support.push(binary(pixels[&[x, y, polarization, channel][..]])?);
                    }
                }
            }
        }
        let cells = nx * ny;
        if support
            .chunks(cells)
            .all(|plane| plane == &support[..cells])
        {
            support.truncate(cells);
            return Ok((MaskPlanes::SHARED, support.into_boxed_slice()));
        }
        let planes =
            MaskPlanes::new(channels, stokes).map_err(|error| refuse(error.to_string()))?;
        Ok((planes, support.into_boxed_slice()))
    }
}
