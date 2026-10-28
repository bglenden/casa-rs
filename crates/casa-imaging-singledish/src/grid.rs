// SPDX-License-Identifier: LGPL-3.0-or-later
//! The image-plane gridder: total-power spectra spread over the pixels
//! around their pointings and normalised by the weight they put there, as
//! CASA's `SDGrid::put` (`ggridsd`) and `SDGrid::getImage` do.
//!
//! For each unflagged spectrum, at each data channel with a positive weight
//! `w` that maps to an image channel, and each unflagged correlation that
//! maps to an image plane, every pixel the kernel reaches
//! ([`KernelTable::taps`]) with weight `k` accumulates
//! - `w · value · k` in the data grid, and
//! - `w · k` in the weight grid.
//!
//! The image is the data grid over the weight grid where the weight is
//! positive, and zero elsewhere. A Stokes I image takes both parallel
//! hands into its one plane (`FTMachine::matchPol`), so it is their mean,
//! and its weight counts each spectrum's weight once per hand. A Stokes
//! image flags a channel in every correlation when any is flagged
//! (`FTMachine::setSpectralFlag`), so it never takes one hand alone; a
//! pseudo-Stokes image takes each correlation's flags as they are
//! ([`FlagMerge`]).
//!
//! Both grids are held whole, as CASA holds them, in single precision.
//! Total-power data are real, so the grids are real where CASA's data grid
//! is complex. The image channels are gridded in parallel on the worker
//! team, each summing its pixels in CASA's order (spectrum, then data
//! channel, then correlation), so a channel's sums are CASA's.

use casa_imaging_runtime::Reservation;
use casa_imaging_runtime::pass::WorkerTeam;
use ndarray::{ArrayD, ArrayView2, ArrayView3, IxDyn, ShapeBuilder};

use crate::SingleDishError;
use crate::kernel::{KernelTable, Tap};

/// The most bytes the gridder's per-pass tap scratch holds: the kernel's
/// taps of the spectra gridded in one pass over the channels.
const TAP_SCRATCH_BYTES: usize = 4 << 20;

/// The shape of an image the gridder fills, in CASA's axis order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GridShape {
    /// Pixels along x.
    pub nx: usize,
    /// Pixels along y.
    pub ny: usize,
    /// Polarization planes.
    pub planes: usize,
    /// Channels.
    pub channels: usize,
}

impl GridShape {
    /// Pixels in one channel's plane of one polarization.
    #[must_use]
    pub const fn plane_pixels(self) -> usize {
        self.nx * self.ny
    }

    /// Pixels in all of the image.
    #[must_use]
    pub const fn pixels(self) -> usize {
        self.plane_pixels() * self.planes * self.channels
    }
}

/// How a channel's flags combine across its correlations
/// (`FTMachine::setSpectralFlag`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlagMerge {
    /// A Stokes image (`stokes='I'`, the default): a value flagged in any
    /// correlation flags the channel in all of them.
    AnyCorrelation,
    /// A pseudo-Stokes image (`stokes` of `XX`, `YY`, `XXYY`, `RR`, `LL` or
    /// `RRLL`): each correlation keeps its own flags.
    PerCorrelation,
}

/// A block of total-power spectra to grid, as one `VisBuffer` hands them to
/// `SDGrid::put`.
#[derive(Debug, Clone, Copy)]
pub struct SpectrumBlock<'a> {
    /// `[spectrum]`: the zero-based fractional image pixel of the spectrum's
    /// pointing, or `None` where it could not be placed (`getXYPos`).
    pub pixels: &'a [Option<[f64; 2]>],
    /// `[spectrum][channel][correlation]`: the spectra.
    pub values: ArrayView3<'a, f32>,
    /// `[spectrum][channel][correlation]`: whether a value is flagged.
    pub flags: ArrayView3<'a, bool>,
    /// `[spectrum]`: whether the whole spectrum is flagged (`FLAG_ROW`).
    pub row_flags: &'a [bool],
    /// `[spectrum][channel]`: the spectrum's weight
    /// ([`crate::weights_from_rows`], [`crate::weights_from_spectra`]).
    pub weights: ArrayView2<'a, f32>,
    /// `[channel]`: the image channel each data channel is gridded into
    /// (`chanMap`), or `None`.
    pub channel_map: &'a [Option<usize>],
    /// `[correlation]`: the image plane each correlation is gridded into
    /// (`polMap`), or `None`.
    pub correlation_map: &'a [Option<usize>],
}

impl SpectrumBlock<'_> {
    fn check(&self, shape: GridShape) -> Result<(), SingleDishError> {
        let (spectra, channels, correlations) = self.values.dim();
        let agrees = self.flags.dim() == (spectra, channels, correlations)
            && self.weights.dim() == (spectra, channels)
            && self.pixels.len() == spectra
            && self.row_flags.len() == spectra
            && self.channel_map.len() == channels
            && self.correlation_map.len() == correlations;
        if !agrees {
            return Err(SingleDishError::Shape(format!(
                "a block of {spectra} spectra of {channels} channels and {correlations} \
                 correlations needs flags, weights, pixels, row flags and maps of those \
                 lengths"
            )));
        }
        let outside = |map: &[Option<usize>], size: usize| map.iter().flatten().any(|&i| i >= size);
        if outside(self.channel_map, shape.channels) || outside(self.correlation_map, shape.planes)
        {
            return Err(SingleDishError::Shape(format!(
                "a channel or correlation maps outside an image of {} channels and {} planes",
                shape.channels, shape.planes
            )));
        }
        Ok(())
    }
}

/// The gridded image and its weight.
#[derive(Debug, Clone, PartialEq)]
pub struct TotalPowerImage {
    /// `[x, y, plane, channel]`: the image, zero where there is no weight.
    pub image: ArrayD<f32>,
    /// `[x, y, plane, channel]`: the weight each pixel accumulated
    /// (`SDGrid::getWeightImage`).
    pub weight: ArrayD<f32>,
}

/// The data and weight grids of an image being gridded.
#[derive(Debug)]
pub struct TotalPowerGrid {
    shape: GridShape,
    kernel: KernelTable,
    flags: FlagMerge,
    /// `[channel][plane][y][x]`.
    data: Vec<f32>,
    /// `[channel][plane][y][x]`.
    weight: Vec<f32>,
    /// The admission of [`Self::bytes`].
    _reservation: Reservation,
}

impl TotalPowerGrid {
    /// Bytes a grid of `shape` holds with `kernel`, known before it is
    /// formed, while it grids blocks of at most `data_channels` channels:
    /// the two grids, the kernel, the taps of one pass's spectra, and each
    /// pass's per-channel bookkeeping.
    #[must_use]
    pub fn bytes(shape: GridShape, kernel: &KernelTable, data_channels: usize) -> u64 {
        let grids = 2 * shape.pixels() * size_of::<f32>();
        let pass = shape.channels
            * (size_of::<ChannelGrids<'_>>() + size_of::<Result<(), SingleDishError>>());
        (grids + Taps::bytes(kernel) + Sources::bytes(shape.channels, data_channels) + pass) as u64
            + kernel.bytes()
    }

    /// An empty grid of `shape`, merging flags as `flags` says, holding
    /// `reservation`, its admission of [`Self::bytes`].
    ///
    /// # Errors
    ///
    /// An image too large to index.
    pub fn new(
        shape: GridShape,
        kernel: KernelTable,
        flags: FlagMerge,
        reservation: Reservation,
    ) -> Result<Self, SingleDishError> {
        if shape.plane_pixels() > u32::MAX as usize {
            return Err(SingleDishError::Shape(format!(
                "an image plane of {} × {} pixels is too large to grid",
                shape.nx, shape.ny
            )));
        }
        Ok(Self {
            shape,
            kernel,
            flags,
            data: vec![0.0; shape.pixels()],
            weight: vec![0.0; shape.pixels()],
            _reservation: reservation,
        })
    }

    /// The image's shape.
    #[must_use]
    pub const fn shape(&self) -> GridShape {
        self.shape
    }

    /// Grid `block`, its image channels in parallel on `team`.
    ///
    /// # Errors
    ///
    /// A block whose arrays and maps disagree in shape, or that maps outside
    /// the image.
    pub fn grid(
        &mut self,
        block: &SpectrumBlock<'_>,
        team: &WorkerTeam,
    ) -> Result<(), SingleDishError> {
        block.check(self.shape)?;
        let sources = Sources::of(block.channel_map, self.shape.channels);
        let per_channel = self.shape.planes * self.shape.plane_pixels();
        let rows = tap_scratch_rows(&self.kernel);
        let mut taps = Taps::new(&self.kernel);
        let mut start = 0;
        while start < block.pixels.len() {
            let end = block.pixels.len().min(start + rows);
            taps.fill(&self.kernel, block, start..end, self.shape);
            let mut channels: Vec<ChannelGrids<'_>> = self
                .data
                .chunks_mut(per_channel)
                .zip(self.weight.chunks_mut(per_channel))
                .enumerate()
                .map(|(channel, (data, weight))| ChannelGrids {
                    data,
                    weight,
                    sources: sources.of_channel(channel),
                })
                .collect();
            let plane_pixels = self.shape.plane_pixels();
            let flags = self.flags;
            team.for_each_mut(&mut channels, |_, grids| {
                grids.accumulate(block, &taps, (plane_pixels, flags));
                Ok::<_, SingleDishError>(())
            })?;
            start = end;
        }
        Ok(())
    }

    /// The image and its weight, as `tsdimaging` forms them: the data grid
    /// over the weight grid where the weight is positive and zero
    /// elsewhere (`SDGrid::getImage` unnormalised, then
    /// `SIImageStore::divideResidualByWeightSD`), and the weight grid
    /// (`SDGrid::getWeightImage`). `tsdimaging`'s `minweight` then masks
    /// the image ([`crate::weight_mask_threshold`]).
    #[must_use]
    pub fn finish(self) -> TotalPowerImage {
        let Self {
            shape,
            mut data,
            weight,
            ..
        } = self;
        for (value, &weight) in data.iter_mut().zip(&weight) {
            *value = if weight > 0.0 { *value / weight } else { 0.0 };
        }
        let dims = IxDyn(&[shape.nx, shape.ny, shape.planes, shape.channels]).f();
        let array = |values| ArrayD::from_shape_vec(dims.clone(), values).expect("grid shape");
        TotalPowerImage {
            image: array(data),
            weight: array(weight),
        }
    }
}

/// A tap of a pointing's kernel as an index into an image plane.
#[derive(Debug, Clone, Copy)]
struct PlaneTap {
    pixel: u32,
    weight: f32,
}

/// The taps of a run of spectra: each spectrum's, in order.
#[derive(Debug)]
struct Taps {
    /// The taps, spectrum after spectrum.
    taps: Vec<PlaneTap>,
    /// `[spectrum in the run + 1]`: where each spectrum's taps start; a
    /// spectrum not gridded has none.
    starts: Vec<usize>,
    /// The first spectrum of the run.
    first: usize,
    /// One spectrum's taps as the kernel gives them.
    scratch: Vec<Tap>,
}

impl Taps {
    /// Room for the taps of a pass's spectra, held at once so that filling
    /// it never grows it.
    fn new(kernel: &KernelTable) -> Self {
        let rows = tap_scratch_rows(kernel);
        let per_spectrum = taps_per_spectrum(kernel);
        Self {
            taps: Vec::with_capacity(rows * per_spectrum),
            starts: Vec::with_capacity(rows + 1),
            first: 0,
            scratch: Vec::with_capacity(per_spectrum),
        }
    }

    /// The bytes [`Self::new`] holds.
    fn bytes(kernel: &KernelTable) -> usize {
        let rows = tap_scratch_rows(kernel);
        let per_spectrum = taps_per_spectrum(kernel);
        rows * per_spectrum * size_of::<PlaneTap>()
            + (rows + 1) * size_of::<usize>()
            + per_spectrum * size_of::<Tap>()
    }

    /// The taps of `block`'s spectra `spectra`.
    fn fill(
        &mut self,
        kernel: &KernelTable,
        block: &SpectrumBlock<'_>,
        spectra: std::ops::Range<usize>,
        shape: GridShape,
    ) {
        self.taps.clear();
        self.starts.clear();
        self.first = spectra.start;
        self.starts.push(0);
        for spectrum in spectra {
            if let (false, Some(pixel)) = (block.row_flags[spectrum], block.pixels[spectrum]) {
                self.scratch.clear();
                kernel.taps(pixel, (shape.nx, shape.ny), &mut self.scratch);
                self.taps.extend(self.scratch.iter().map(|tap| PlaneTap {
                    pixel: (tap.y * shape.nx + tap.x) as u32,
                    weight: tap.weight,
                }));
            }
            self.starts.push(self.taps.len());
        }
    }

    /// The spectra of the run, each with its taps.
    fn spectra(&self) -> impl Iterator<Item = (usize, &[PlaneTap])> {
        self.starts
            .windows(2)
            .enumerate()
            .map(|(i, range)| (self.first + i, &self.taps[range[0]..range[1]]))
    }
}

/// The data channels each image channel takes, in order: a counting sort
/// of `chanMap`.
struct Sources {
    /// `[image channel + 1]`: where each image channel's data channels
    /// start in `order`.
    starts: Vec<usize>,
    /// The mapped data channels, by image channel.
    order: Vec<usize>,
}

impl Sources {
    fn of(channel_map: &[Option<usize>], channels: usize) -> Self {
        let mut starts = vec![0; channels + 1];
        for &target in channel_map.iter().flatten() {
            starts[target + 1] += 1;
        }
        for channel in 0..channels {
            starts[channel + 1] += starts[channel];
        }
        let mut next = starts.clone();
        let mut order = vec![0; starts[channels]];
        for (channel, target) in channel_map.iter().enumerate() {
            if let Some(target) = *target {
                order[next[target]] = channel;
                next[target] += 1;
            }
        }
        Self { starts, order }
    }

    /// The bytes [`Self::of`] holds at most, with its scratch.
    const fn bytes(channels: usize, data_channels: usize) -> usize {
        (2 * (channels + 1) + data_channels) * size_of::<usize>()
    }

    fn of_channel(&self, channel: usize) -> &[usize] {
        &self.order[self.starts[channel]..self.starts[channel + 1]]
    }
}

/// One image channel's grids, and the data channels gridded into it.
struct ChannelGrids<'a> {
    data: &'a mut [f32],
    weight: &'a mut [f32],
    sources: &'a [usize],
}

impl ChannelGrids<'_> {
    /// `ggridsd` for this channel, over the spectra of `taps`.
    fn accumulate(
        &mut self,
        block: &SpectrumBlock<'_>,
        taps: &Taps,
        (plane_pixels, flags): (usize, FlagMerge),
    ) {
        if self.sources.is_empty() {
            return;
        }
        for (spectrum, spectrum_taps) in taps.spectra() {
            if spectrum_taps.is_empty() {
                continue;
            }
            for &channel in self.sources {
                let weight = block.weights[[spectrum, channel]];
                if weight <= 0.0 {
                    continue;
                }
                let merged = || {
                    block
                        .flags
                        .slice(ndarray::s![spectrum, channel, ..])
                        .iter()
                        .any(|&f| f)
                };
                if flags == FlagMerge::AnyCorrelation && merged() {
                    continue;
                }
                for (correlation, plane) in block.correlation_map.iter().enumerate() {
                    let Some(plane) = plane else { continue };
                    if block.flags[[spectrum, channel, correlation]] {
                        continue;
                    }
                    let value = weight * block.values[[spectrum, channel, correlation]];
                    let offset = plane * plane_pixels;
                    let data = &mut self.data[offset..offset + plane_pixels];
                    let grid_weight = &mut self.weight[offset..offset + plane_pixels];
                    for tap in spectrum_taps {
                        let pixel = tap.pixel as usize;
                        data[pixel] += value * tap.weight;
                        grid_weight[pixel] += weight * tap.weight;
                    }
                }
            }
        }
    }
}

/// The most taps the kernel gives one spectrum.
fn taps_per_spectrum(kernel: &KernelTable) -> usize {
    (2 * kernel.support() + 1).pow(2)
}

/// Spectra whose taps one pass holds: as many as fit
/// [`TAP_SCRATCH_BYTES`], at least one.
fn tap_scratch_rows(kernel: &KernelTable) -> usize {
    (TAP_SCRATCH_BYTES / (taps_per_spectrum(kernel) * size_of::<PlaneTap>())).max(1)
}

#[cfg(test)]
mod tests;
