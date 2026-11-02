// SPDX-License-Identifier: LGPL-3.0-or-later
//! The PSF, sums of weights and residual an earlier run left, read in place
//! of the major cycle that would form them (`calcpsf=False`,
//! `calcres=False`).
//!
//! CASA leaves its images standing when it does not form them again
//! (`task_tclean.py`; `SIImageStore` opens what exists, and
//! `FTMachine::finalizeToSkyNew` keeps a `.sumwt` already filled). The
//! images on disk are normalized: the PSF to its peak, the residual by its
//! sum of weights. The normal state holds them unnormalized, as a pass
//! grids them, and products and minor cycles normalize them again; so the
//! reader returns what a pass would have returned: the PSF times its
//! principal sum of weights (the PSF peak of a direction-independent
//! gridder) and the residual times the sum of weights that normalized it.
//! Normalizing them again gives back the images read, to the rounding of
//! one `f32` product and quotient.
//!
//! Only direction-independent gridders (standard and W-projection, one
//! facet) are read: their sensitivity is the sum of weights alone
//! ([`super::check_inputs`] refuses the rest).

use std::ops::Range;
use std::path::{Path, PathBuf};

use casa_images::{ImageError, PagedImage};
use casa_imaging_deconvolution::PassImages;

use super::{Family, product_path};
use crate::PrepareError;

/// The tile cache each image is read with.
const CACHE_BYTES: usize = 64 * 1024;

/// What a run reads of an earlier run's products at its prefix.
#[derive(Clone, Debug)]
pub(crate) struct ExistingProducts {
    prefix: PathBuf,
    family: Family,
    /// `[nx, ny]`.
    shape: [usize; 2],
    polarizations: usize,
    /// The largest tile of the images read, in bytes.
    tile_bytes: u64,
    /// The PSF and its sums of weights (`calcpsf=False`).
    psf: bool,
    /// The residual (`calcres=False` with one at the prefix).
    residual: bool,
}

impl ExistingProducts {
    /// What a run of `family` images of `shape` and `polarizations`, over
    /// `channels` output channels, reads at `prefix`: the PSF and sums of
    /// weights when `psf`, the residual when `residual`. The images' grids
    /// were checked; their sums of weights are checked here to hold one
    /// value per Stokes plane and channel.
    pub(crate) fn new(
        prefix: &Path,
        family: Family,
        (shape, polarizations, channels): ([usize; 2], usize, usize),
        (psf, residual): (bool, bool),
    ) -> Result<Self, PrepareError> {
        let planes = match family {
            Family::Single => channels,
            Family::Taylor { .. } => 1,
        };
        let mut tile_bytes = 0;
        let mut bases = Vec::new();
        if psf {
            bases.extend(["psf", "sumwt"]);
        }
        if residual {
            bases.push("residual");
        }
        for base in bases {
            for name in family.names(base) {
                let path = product_path(prefix, &name);
                let image = open(&path)?;
                tile_bytes = image
                    .tile_shape()
                    .iter()
                    .fold(4_u64, |n, &side| n.saturating_mul(side as u64))
                    .max(tile_bytes);
                if base == "sumwt" && image.shape() != [1, 1, polarizations, planes] {
                    return Err(PrepareError::Restart {
                        path,
                        reason: format!(
                            "its sums of weights have shape {:?}, not one per Stokes plane and \
                             channel [1, 1, {polarizations}, {planes}]",
                            image.shape()
                        ),
                    });
                }
            }
        }
        Ok(Self {
            prefix: prefix.to_path_buf(),
            family,
            shape,
            polarizations,
            tile_bytes,
            psf,
            residual,
        })
    }

    /// Whether the residual is read.
    pub(crate) const fn reads_residual(&self) -> bool {
        self.residual
    }

    /// The `(terms, moments)` of each channel: one of each, or a Taylor
    /// family's coefficients and PSF moments.
    const fn terms(&self) -> (usize, usize) {
        match self.family {
            Family::Single => (1, 1),
            Family::Taylor { terms } => (terms, 2 * terms - 1),
        }
    }

    /// Bytes reading `channels` channels holds besides the pass images it
    /// returns ([`Self::pass_images`], [`Self::images_bytes`]): one image's
    /// slice of those channels in `f32`, the tile cache and two tile
    /// buffers.
    pub(crate) fn reading_bytes(&self, channels: usize) -> u64 {
        let planes = match self.family {
            Family::Single => channels,
            Family::Taylor { .. } => 1,
        };
        (self.shape[0] * self.shape[1] * self.polarizations * planes * 4) as u64
            + 2 * self.tile_bytes
            + CACHE_BYTES as u64
    }

    /// Bytes the pass images of `channels` channels hold: the residual
    /// terms and PSF moments in `f32` and two sums of weights per moment.
    pub(crate) fn images_bytes(&self, channels: usize) -> u64 {
        let (terms, moments) = self.terms();
        let planes = channels * self.polarizations;
        let cells = self.shape[0] * self.shape[1];
        (planes * (cells * (terms + moments) * 4 + moments * 2 * 8)) as u64
    }

    /// The images of domain 0 a pass forming its PSF and residual would
    /// return for output channels `channels`, from the PSF, sums of weights
    /// and residual read: the residual is zero when it is not read, for a
    /// pass to form.
    pub(crate) fn pass_images(&self, channels: Range<usize>) -> Result<PassImages, ImageError> {
        assert!(self.psf, "the PSF is read");
        let (_, moments) = self.terms();
        let (pols, cells) = (self.polarizations, self.shape[0] * self.shape[1]);
        let mut sums = vec![0.0; channels.len() * moments * pols];
        for (moment, name) in self.family.names("sumwt").iter().enumerate() {
            let read = self.read(name, channels.clone())?;
            for channel in 0..channels.len() {
                for pol in 0..pols {
                    sums[(channel * moments + moment) * pols + pol] =
                        f64::from(read[&[0, 0, pol, self.plane(channel)][..]]);
                }
            }
        }
        // A pass's PSF peaks at its principal sum of weights; every moment
        // shares that one divisor (`SIImageStore::normPSF`).
        let principal = |channel: usize, pol: usize| sums[channel * moments * pols + pol];
        let mut psf = vec![0.0; channels.len() * moments * pols * cells];
        for (moment, name) in self.family.names("psf").iter().enumerate() {
            let read = self.read(name, channels.clone())?;
            for channel in 0..channels.len() {
                for pol in 0..pols {
                    let plane = (channel * moments + moment) * pols + pol;
                    self.write_plane(
                        &mut psf[plane * cells..(plane + 1) * cells],
                        &read,
                        (channel, pol),
                        principal(channel, pol),
                    );
                }
            }
        }
        let mut images = PassImages {
            domain: 0,
            shape: self.shape,
            channels,
            polarizations: pols,
            residual: Vec::new(),
            psf: Some(psf),
            sum_weights: sums.clone(),
            published_sum_weights: sums,
            weight: None,
        };
        self.fill_residual(&mut images)?;
        Ok(images)
    }

    /// Set the residual of `images`, a pass's images of domain 0 with
    /// their published sums of weights and no residual (a pass that formed
    /// the PSF alone), to the residual read times the sum of weights that
    /// normalizes it: each plane's own, or a Taylor family's principal one
    /// for every term (`SIImageStoreMultiTerm::divideResidualByWeight`).
    /// Without a residual read it is zero.
    pub(crate) fn fill_residual(&self, images: &mut PassImages) -> Result<(), ImageError> {
        let (_, moments) = self.terms();
        let pols = images.polarizations;
        let published = &images.published_sum_weights;
        images.residual = self.residual(images.channels.clone(), |channel, pol| {
            published[channel * moments * pols + pol]
        })?;
        Ok(())
    }

    /// The images of domain 0 that refresh a normal state's residual with
    /// the residual read ([`Self::fill_residual`]); `principal` is the
    /// state's published sum of weights of window channel `(channel, pol)`.
    pub(crate) fn residual_images(
        &self,
        channels: Range<usize>,
        principal: impl Fn(usize, usize) -> f64,
    ) -> Result<PassImages, ImageError> {
        Ok(PassImages {
            domain: 0,
            shape: self.shape,
            channels: channels.clone(),
            polarizations: self.polarizations,
            residual: self.residual(channels, principal)?,
            psf: None,
            sum_weights: Vec::new(),
            published_sum_weights: Vec::new(),
            weight: None,
        })
    }

    /// The residual planes of `channels`, `[channel][term][pol]` x-major,
    /// read and times `principal(channel, pol)`, which a Taylor family
    /// takes at `(0, 0)` for every term; zero without a residual read.
    fn residual(
        &self,
        channels: Range<usize>,
        principal: impl Fn(usize, usize) -> f64,
    ) -> Result<Vec<f32>, ImageError> {
        let (terms, _) = self.terms();
        let pols = self.polarizations;
        let cells = self.shape[0] * self.shape[1];
        let mut residual = vec![0.0; channels.len() * terms * pols * cells];
        if !self.residual {
            return Ok(residual);
        }
        let scale = |channel: usize, pol: usize| match self.family {
            Family::Single => principal(channel, pol),
            Family::Taylor { .. } => principal(0, 0),
        };
        for (term, name) in self.family.names("residual").iter().enumerate() {
            let read = self.read(name, channels.clone())?;
            for channel in 0..channels.len() {
                for pol in 0..pols {
                    let plane = (channel * terms + term) * pols + pol;
                    self.write_plane(
                        &mut residual[plane * cells..(plane + 1) * cells],
                        &read,
                        (channel, pol),
                        scale(channel, pol),
                    );
                }
            }
        }
        Ok(residual)
    }

    /// The image plane of window channel `channel`: the channel itself, or
    /// a Taylor image's one plane.
    const fn plane(&self, channel: usize) -> usize {
        match self.family {
            Family::Single => channel,
            Family::Taylor { .. } => 0,
        }
    }

    /// Write `read`'s plane `(channel, pol)` times `scale` to `out`,
    /// x-major (y fastest) as the normal state holds it.
    fn write_plane(
        &self,
        out: &mut [f32],
        read: &ndarray::ArrayD<f32>,
        (channel, pol): (usize, usize),
        scale: f64,
    ) {
        let plane = self.plane(channel);
        let ny = self.shape[1];
        for (index, value) in out.iter_mut().enumerate() {
            let (x, y) = (index / ny, index % ny);
            *value = (f64::from(read[&[x, y, pol, plane][..]]) * scale) as f32;
        }
    }

    /// The pixels of `name` over `channels`: their slice of a single image,
    /// or a Taylor image's one plane.
    fn read(&self, name: &str, channels: Range<usize>) -> Result<ndarray::ArrayD<f32>, ImageError> {
        let path = product_path(&self.prefix, name);
        let image = PagedImage::<f32>::open_with_cache(&path, CACHE_BYTES)?;
        let shape = image.shape().to_vec();
        let (start, count) = match self.family {
            Family::Single => (channels.start, channels.len()),
            Family::Taylor { .. } => (0, 1),
        };
        image.get_slice(&[0, 0, 0, start], &[shape[0], shape[1], shape[2], count])
    }
}

fn open(path: &Path) -> Result<PagedImage<f32>, PrepareError> {
    PagedImage::<f32>::open_with_cache(path, CACHE_BYTES).map_err(|error| PrepareError::Restart {
        path: path.to_path_buf(),
        reason: error.to_string(),
    })
}
