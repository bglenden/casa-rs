// SPDX-License-Identifier: LGPL-3.0-or-later
//! The PSF, sums of weights and residual an earlier run left, read in place
//! of the major cycle that would form them (`calcpsf=False`,
//! `calcres=False`).
//!
//! CASA leaves its images standing when it does not form them again
//! (`task_tclean.py`; `SIImageStore` opens what exists, and
//! `FTMachine::finalizeToSkyNew` keeps a `.sumwt` already filled). The
//! residual is returned in gridding units by multiplying it by its saved
//! sum of weights. The single-term PSF keeps its saved pixels for beam
//! fitting and publication. The solver scales it by its peak for the minor
//! cycle. A Taylor PSF family still uses the principal sum of weights to
//! return all its moments in gridding units.
//!
//! Only direction-independent gridders (standard and W-projection, one
//! facet) are read: their sensitivity is the sum of weights alone
//! ([`super::check_inputs`] refuses the rest).

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_images::{ImageBeamSet, ImageError, PagedImage};
use casa_imaging_deconvolution::{PassImages, RestoringBeam};

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
    /// A complete PSF beam set read from the earlier image, channel-major.
    psf_beams: Option<Arc<[RestoringBeam]>>,
    /// The same beam grid in pixels for minor-cycle preparation.
    psf_pixel_beams: Option<Arc<[RestoringBeam]>>,
    /// The residual beam grid in pixels, used for automatic masking.
    mask_beams: Option<Arc<[RestoringBeam]>>,
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
            psf_beams: None,
            psf_pixel_beams: None,
            mask_beams: None,
        })
    }

    /// Read the earlier PSF's complete beam grid, as `makeImageBeamSet`
    /// reuses it before looking at the new run's cutoff.
    pub(crate) fn read_psf_beams(
        &mut self,
        channels: usize,
        cell_rad: f64,
    ) -> Result<(), PrepareError> {
        let name = self
            .family
            .names("psf")
            .into_iter()
            .next()
            .expect("a PSF name");
        let path = product_path(&self.prefix, &name);
        let image = open(&path)?;
        let info = image.image_info().map_err(|error| PrepareError::Restart {
            path: path.clone(),
            reason: error.to_string(),
        })?;
        let planes = match self.family {
            Family::Single => channels,
            Family::Taylor { .. } => 1,
        };
        self.psf_beams =
            complete_psf_beams(&info.beam_set, planes, self.polarizations).map_err(|error| {
                PrepareError::Restart {
                    path: path.clone(),
                    reason: error.to_string(),
                }
            })?;
        if let Some(beams) = &self.psf_beams {
            let pixels = beams
                .iter()
                .map(|beam| {
                    RestoringBeam::new(
                        beam.major_fwhm_rad() / cell_rad,
                        beam.minor_fwhm_rad() / cell_rad,
                        beam.position_angle_rad(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| PrepareError::Restart {
                    path,
                    reason: error.to_string(),
                })?;
            self.psf_pixel_beams = Some(pixels.into());
        }
        Ok(())
    }

    /// Read the residual beam that `SDMaskHandler::autoMask` prefers over the PSF.
    pub(crate) fn read_mask_beams(
        &mut self,
        channels: usize,
        cell_rad: f64,
    ) -> Result<(), PrepareError> {
        let name = self
            .family
            .names("residual")
            .into_iter()
            .next()
            .expect("a residual name");
        let path = product_path(&self.prefix, &name);
        let image = open(&path)?;
        let info = image.image_info().map_err(|error| PrepareError::Restart {
            path: path.clone(),
            reason: error.to_string(),
        })?;
        let planes = match self.family {
            Family::Single => channels,
            Family::Taylor { .. } => 1,
        };
        self.mask_beams =
            residual_pixel_beams(&info.beam_set, planes, self.polarizations, cell_rad)
                .map_err(|reason| PrepareError::Restart { path, reason })?;
        Ok(())
    }

    /// Residual beam grid in pixels for automatic masking.
    pub(crate) fn mask_beams(&self) -> Option<Arc<[RestoringBeam]>> {
        self.mask_beams.clone()
    }

    /// Shared beam grid retained through imaging and publication.
    pub(crate) fn psf_beams(&self) -> Option<Arc<[RestoringBeam]>> {
        self.psf_beams.clone()
    }

    /// Shared beam grid in pixels for the minor cycle.
    pub(crate) fn psf_pixel_beams(&self) -> Option<Arc<[RestoringBeam]>> {
        self.psf_pixel_beams.clone()
    }

    /// Retained heap of the shared beam grid, including the `Arc` counters.
    pub(crate) fn beam_resident_bytes(&self) -> u64 {
        [&self.psf_beams, &self.psf_pixel_beams, &self.mask_beams]
            .into_iter()
            .flatten()
            .map(|beams| (std::mem::size_of_val(beams.as_ref()) + 2 * size_of::<usize>()) as u64)
            .sum()
    }

    /// Bound an image beam header and beam conversion read together.
    /// Each beam is three quantity records (one field and two nested fields
    /// each), plus the channel and Stokes records. The table keyword tree,
    /// the decoded beam grid, and the converted grid may coexist.
    pub(crate) fn beam_reading_bytes(channels: usize, polarizations: usize) -> u64 {
        let planes = channels.saturating_mul(polarizations);
        let fields = 3 * 3 + 2;
        let record = fields * (size_of::<casa_types::RecordField>() + 3 * size_of::<String>());
        let per_plane = 3 * record
            + size_of::<casa_images::GaussianBeam>()
            + 4 * size_of::<RestoringBeam>()
            + size_of::<Vec<casa_images::GaussianBeam>>();
        (planes.saturating_mul(per_plane) + 4 * CACHE_BYTES + 2 * size_of::<usize>()) as u64
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
        // Single-term consumers fit and publish the saved PSF in image
        // units. Taylor consumers take every moment in the gridding units
        // obtained by undoing the shared principal normalization.
        let psf_scale = |channel: usize, pol: usize| match self.family {
            Family::Single => 1.0,
            Family::Taylor { .. } => sums[channel * moments * pols + pol],
        };
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
                        psf_scale(channel, pol),
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
    /// read and times `principal(channel, pol)`, which each polarization's
    /// Taylor family takes at its principal moment, `(0, pol)`, for every
    /// term; zero without a residual read.
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
            Family::Taylor { .. } => principal(0, pol),
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

/// CASA reuses the PSF header only when its complete beam matrix has the
/// image's channel and polarization shape (`makeImageBeamSet`).
fn complete_psf_beams(
    set: &ImageBeamSet,
    channels: usize,
    polarizations: usize,
) -> Result<Option<Arc<[RestoringBeam]>>, casa_imaging_deconvolution::PsfBeamFitError> {
    if set.shape() != (channels, polarizations) {
        return Ok(None);
    }
    let mut beams = Vec::with_capacity(channels * polarizations);
    for channel in 0..channels {
        for polarization in 0..polarizations {
            let beam = set.beam(channel, polarization);
            beams.push(RestoringBeam::new(
                beam.major,
                beam.minor,
                beam.position_angle,
            )?);
        }
    }
    Ok(Some(beams.into()))
}

fn residual_pixel_beams(
    set: &ImageBeamSet,
    channels: usize,
    polarizations: usize,
    cell_rad: f64,
) -> Result<Option<Arc<[RestoringBeam]>>, String> {
    if set.is_empty() {
        return Ok(None);
    }
    let (nc, np) = set.shape();
    if (nc != 1 && nc != channels) || (np != 1 && np != polarizations) {
        return Err(format!(
            "residual beam grid has shape ({nc}, {np}), expected ({channels}, {polarizations}) or an axis of length one"
        ));
    }
    let mut beams = Vec::with_capacity(channels * polarizations);
    for channel in 0..channels {
        for polarization in 0..polarizations {
            let beam = set.beam(channel, polarization);
            beams.push(
                RestoringBeam::new(
                    beam.major / cell_rad,
                    beam.minor / cell_rad,
                    beam.position_angle,
                )
                .map_err(|error| error.to_string())?,
            );
        }
    }
    Ok(Some(beams.into()))
}

fn open(path: &Path) -> Result<PagedImage<f32>, PrepareError> {
    PagedImage::<f32>::open_with_cache(path, CACHE_BYTES).map_err(|error| PrepareError::Restart {
        path: path.to_path_buf(),
        reason: error.to_string(),
    })
}

#[cfg(test)]
mod beam_tests {
    use super::*;
    use casa_images::GaussianBeam;

    fn beam(major: f64) -> GaussianBeam {
        GaussianBeam {
            major,
            minor: major / 2.0,
            position_angle: 0.25,
        }
    }

    /// CASA accepts only a complete PSF beam grid, in channel/polarization
    /// order; a missing or differently shaped header is refitted.
    #[test]
    fn a_restart_reuses_only_a_complete_psf_beam_grid() {
        let set =
            ImageBeamSet::from_grid(vec![vec![beam(1.0), beam(2.0)], vec![beam(3.0), beam(4.0)]]);
        let result = complete_psf_beams(&set, 2, 2).unwrap().unwrap();
        assert_eq!(
            result
                .iter()
                .map(|b| b.major_fwhm_rad())
                .collect::<Vec<_>>(),
            [1.0, 2.0, 3.0, 4.0]
        );
        assert!(complete_psf_beams(&set, 4, 1).unwrap().is_none());
        assert!(
            complete_psf_beams(&ImageBeamSet::empty(), 2, 2)
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn residual_beams_expand_single_axes_in_channel_major_order() {
        let set = ImageBeamSet::from_grid(vec![vec![beam(2.0), beam(4.0)]]);
        let pixels = residual_pixel_beams(&set, 3, 2, 2.0).unwrap().unwrap();
        assert_eq!(
            pixels
                .iter()
                .map(|b| b.major_fwhm_rad())
                .collect::<Vec<_>>(),
            [1.0, 2.0, 1.0, 2.0, 1.0, 2.0]
        );
        let grid =
            ImageBeamSet::from_grid(vec![vec![beam(2.0), beam(4.0)], vec![beam(6.0), beam(8.0)]]);
        let pixels = residual_pixel_beams(&grid, 2, 2, 2.0).unwrap().unwrap();
        assert_eq!(
            pixels
                .iter()
                .map(|b| b.major_fwhm_rad())
                .collect::<Vec<_>>(),
            [1.0, 2.0, 3.0, 4.0]
        );
        assert!(residual_pixel_beams(&set, 3, 3, 2.0).is_err());
        assert!(
            residual_pixel_beams(&ImageBeamSet::empty(), 3, 2, 2.0)
                .unwrap()
                .is_none()
        );
    }
}
