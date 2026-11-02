// SPDX-License-Identifier: LGPL-3.0-or-later
//! An existing binary CLEAN mask, read once on its checked grid. CASA's
//! SynthesisDeconvolver::setupMask reuses it when no new user mask is given.
//!
//! A mask has a plane per Stokes parameter and channel. casa-rs's
//! reconstruction mask is one spatial support for every plane, so a mask is
//! reused when its planes agree, as every mask casa-rs writes does, and
//! refused otherwise.

use std::path::{Path, PathBuf};

use casa_images::PagedImage;

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

    fn plane_pixels(&self) -> u64 {
        self.shape.iter().take(2).map(|&side| side as u64).product()
    }

    /// The read of every plane and its conversion, two support copies of one
    /// plane (compile and run clone their plans), the bounded tile cache and
    /// two decoded/I/O tile buffers.
    pub(crate) fn reading_bytes(&self) -> u64 {
        self.pixels()
            .saturating_mul(2 * 4)
            .saturating_add(self.plane_pixels().saturating_mul(2))
            .saturating_add(self.tile_bytes.saturating_mul(2))
            .saturating_add(CACHE_BYTES as u64)
    }

    /// Both the original and the cloned static reconstruction-mask plan.
    pub(crate) fn retained_bytes(&self) -> u64 {
        self.plane_pixels().saturating_mul(2)
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
        let binary = |value: f32| match value {
            0.0 => Ok(false),
            1.0 => Ok(true),
            _ => Err(refuse(
                "continuation with a non-binary CLEAN mask is not implemented yet".into(),
            )),
        };
        // The grid check holds the mask to the image's four axes,
        // [x, y, stokes, channel]; reconstruction support is x-major (y
        // fastest), unlike model samples.
        let &[nx, ny, stokes, channels] = self.shape.as_slice() else {
            return Err(refuse(format!(
                "a CLEAN mask has four axes, not {:?}",
                self.shape
            )));
        };
        let planes = stokes * channels;
        let at =
            |x: usize, y: usize, plane: usize| pixels[&[x, y, plane % stokes, plane / stokes][..]];
        let mut support = Vec::with_capacity(nx * ny);
        for x in 0..nx {
            for y in 0..ny {
                let first = binary(at(x, y, 0))?;
                for plane in 1..planes {
                    if binary(at(x, y, plane))? != first {
                        return Err(refuse(
                            "continuation with a CLEAN mask whose Stokes or channel planes \
                             differ is not implemented yet"
                                .into(),
                        ));
                    }
                }
                support.push(first);
            }
        }
        Ok(support.into_boxed_slice())
    }
}
