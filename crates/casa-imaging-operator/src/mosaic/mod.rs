// SPDX-License-Identifier: LGPL-3.0-or-later
//! The mosaic primary-beam kernel set, pinned to CASA `HetArrayConvFunc`
//! (`TransformMachines2/HetArrayConvFunc.cc`), `SimplePBConvFunc`,
//! `PBMath1D`/`PBMath1DAiry`/`PBMath1DEVLA`, `MosaicFT` and `fmosaic.f`.
//!
//! One dense complex cell per (dish-class pair, spectral window, frequency
//! cell): `FT[VP₁ · VP₂]` for imaging and `FT[PB₁ · PB₂]` for the weight
//! image, both built on a screen covering the image field at the
//! convolution-function size, quarter-cropped, support-searched on the
//! weight plane, normalised over the support, cropped again and Lanczos
//! resampled to ten fine offsets per cell. The pointing enters only through
//! the per-row phase gradient ([`ConvolutionFunctionSet::pointing_ramp`]).
//!
//! A dish class's voltage pattern is a [`PrimaryBeam`]: the annular Airy
//! aperture `findAntennaSizes` builds per dish diameter ([`AiryDish`]), or
//! CASA's common EVLA beam, which `findAntennaSizes` gives every antenna of
//! an EVLA MeasurementSet (`PBMathInterface::COMMONPB`) and which takes the
//! polynomial fit of each beam frequency (`PBMath1DEVLA::nearestVPArray`).

use casa_numerics::{BeamPattern, PrimaryBeam};
use num_complex::{Complex32, Complex64};

mod cells;
mod screen;

use cells::FrequencyCells;
use screen::{
    LANCZOS_A, PairLattices, conjugate_side, finish_pair, screen_pair,
    simulator_weight_plane_support, weight_plane_support,
};

use crate::convolution::{
    CellHold, ConvolutionFunctionSet, DenseCell, GridEdge, ImageCorrection, KernelNormalisation,
    MuellerRouting, RowContext, TapFootprint, TapLayout,
};
use crate::dense::dense_cell_bytes;
use crate::error::OperatorError;
use crate::fft::PlaneFft;
use crate::geometry::{GridGeometry, next_larger_even_composite};
use crate::polarization::PolarizationRouting;
use crate::sample::CfKey;

/// `MosaicFT::findConvFunction`: fine offsets per cell for
/// `HetArrayConvFunc` (`mosaic.oversampling`, default 10, forced to at
/// least 10). The image-size rule `ceil(5000/max(nx, ny))` applies only to
/// `SimplePBConvFunc`.
pub const MOSAIC_OVERSAMPLING: u16 = 10;
/// `PBMath1DAiry` in `findAntennaSizes`: the reference frequency of the
/// dish-class support radius, in GHz.
const DISH_REFERENCE_GHZ: f64 = 100.0;

/// Which of CASA's two heterogeneous-array mosaic machines a set follows.
/// They build the same crops, normalisation and resampling, and differ in
/// the screen's least size, in the frequency cells, in how the support is
/// found and gridded and, through [`AiryDish`], in the 7 m dish's radius.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MosaicRules {
    /// tclean's `gridder='mosaic'`: `TransformMachines2/HetArrayConvFunc`
    /// (imaging).
    #[default]
    Tclean,
    /// tclean's machine with `conjbeams` on an image of one plane
    /// (`findConvFunction` with `getConjConvFunc`, `nchan_p == 1`): each
    /// beam frequency `ν` has a conjugate `ν_c = √(2ν₀² − ν²)` about the
    /// image frequency `ν₀` (`SynthesisUtils::conjFreq`). The weight cells
    /// are `FT[VP_k(ν) VP_j(ν) VP_k(ν_c) VP_j(ν_c)]`; a window's supports
    /// widen to `√(max ν_c/ν)` times its widest when that is wider
    /// (`conjSupport`); and gridding takes the imaging cell resampled by
    /// `ν_c/ν` (`fillConjConvFunc`), while prediction keeps the cell at `ν`
    /// (`MosaicFT` asks for the conjugate only when it grids).
    TcleanConjugateBeams,
    /// The simulator's `setoptions(ftmachine='mosaic')`: the earlier
    /// `TransformMachines/HetArrayConvFunc`, which `Simulator::
    /// createSkyEquation` builds. Its screen has no least size beside the
    /// beams'; its frequency cells come from the selected channels alone;
    /// its support search walks the weight plane inwards from the edge,
    /// along the row and the column one cell off its centre, and widens the
    /// radius it finds by √2; and its `MosaicFT` grids every row of a
    /// window with the widest pair's support.
    Simulator,
}

impl MosaicRules {
    /// Taps on each side of a pair of `support` in a window whose widest
    /// pair has `window_support`.
    const fn taps(self, support: usize, window_support: usize) -> usize {
        match self {
            Self::Tclean | Self::TcleanConjugateBeams => support,
            Self::Simulator => window_support,
        }
    }

    /// The conjugate of beam frequency `frequency_hz` about the image
    /// frequency `image_frequency_hz` under these rules, if they have one.
    fn conjugate_hz(
        self,
        image_frequency_hz: f64,
        frequency_hz: f64,
    ) -> Result<Option<f64>, OperatorError> {
        if self != Self::TcleanConjugateBeams {
            return Ok(None);
        }
        let conjugate =
            (2.0 * image_frequency_hz * image_frequency_hz - frequency_hz * frequency_hz).sqrt();
        if conjugate.is_finite() && conjugate > 0.0 {
            Ok(Some(conjugate))
        } else {
            Err(OperatorError::ConvolutionFunction {
                reason: "a beam frequency above √2 times the image frequency has no conjugate",
            })
        }
    }
}

/// One dish class of the heterogeneous array: the annular Airy aperture
/// `HetArrayConvFunc::findAntennaSizes` builds per distinct dish diameter.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AiryDish {
    /// Effective aperture diameter in metres.
    pub aperture_m: f64,
    /// Central blockage diameter in metres.
    pub blockage_m: f64,
    /// Support radius of the voltage pattern in arcseconds at 100 GHz.
    pub max_radius_arcsec: f64,
}

impl AiryDish {
    /// `HetArrayConvFunc::findAntennaSizes` for an ALMA or ACA
    /// MeasurementSet: a dish within 0.5 m of 12 m is the 10.7 m aperture
    /// with 0.75 m blockage and radius `max(150″, fov/5)`; any other dish
    /// is the 6.25 m aperture with 0.75 m blockage and radius `max(300″,
    /// fov/3)`, where `fov = max(nx · Δx, ny · Δy)` with CASA's signed
    /// increments.
    #[must_use]
    pub fn casa_alma(dish_diameter_m: f64, geometry: &GridGeometry) -> Self {
        let image = geometry.image();
        let fov_rad = (image.shape[0] as f64 * image.increment_rad[0])
            .max(image.shape[1] as f64 * image.increment_rad[1]);
        let fov_arcsec = fov_rad.to_degrees() * 3600.0;
        if (dish_diameter_m - 12.0).abs() < 0.5 {
            Self {
                aperture_m: 10.7,
                blockage_m: 0.75,
                max_radius_arcsec: 150.0_f64.max(fov_arcsec / 5.0),
            }
        } else {
            Self {
                aperture_m: 6.25,
                blockage_m: 0.75,
                max_radius_arcsec: 300.0_f64.max(fov_arcsec / 3.0),
            }
        }
    }

    /// The simulator's `HetArrayConvFunc::findAntennaSizes`
    /// ([`MosaicRules::Simulator`]) for an ALMA or ACA MeasurementSet: the
    /// apertures of [`Self::casa_alma`], with a 150″ radius at 100 GHz for
    /// every dish.
    #[must_use]
    pub fn casa_alma_simulator(dish_diameter_m: f64) -> Self {
        let (aperture_m, blockage_m) = if (dish_diameter_m - 12.0).abs() < 0.5 {
            (10.7, 0.75)
        } else {
            (6.25, 0.75)
        };
        Self {
            aperture_m,
            blockage_m,
            max_radius_arcsec: 150.0,
        }
    }

    /// `HetArrayConvFunc::findAntennaSizes` for any other telescope, in
    /// either machine: the dish diameter itself, ALMA's blockage ratio
    /// `d/12 · 0.75` and a 150″ radius.
    #[must_use]
    pub fn casa_airy(dish_diameter_m: f64) -> Self {
        Self {
            aperture_m: dish_diameter_m,
            blockage_m: dish_diameter_m / 12.0 * 0.75,
            max_radius_arcsec: 150.0,
        }
    }
}

impl From<AiryDish> for PrimaryBeam {
    /// `PBMath1DAiry(diameter, blockage, radius, 100 GHz)`: the support
    /// radius scaled to 1 GHz, in arcminutes times GHz
    /// (`PBMath1D::maximumRadius_p`).
    fn from(dish: AiryDish) -> Self {
        Self::Airy {
            dish_diameter_m: dish.aperture_m,
            blockage_diameter_m: dish.blockage_m,
            maximum_radius_arcmin_ghz: dish.max_radius_arcsec / 60.0 * DISH_REFERENCE_GHZ,
        }
    }
}

/// The channels of one spectral window the set builds frequency cells for.
#[derive(Clone, Debug, PartialEq)]
pub struct MosaicWindow {
    /// Spectral window identifier, as [`RowContext::spectral_window`] carries
    /// it.
    pub spectral_window: u32,
    /// Every channel frequency of the window in Hz (`SPECTRAL_WINDOW`
    /// `CHAN_FREQ`), selected or not.
    pub window_frequencies_hz: Vec<f64>,
    /// Width of the window's first channel in Hz (`CHAN_WIDTH[0]`).
    pub channel_width_hz: f64,
    /// Frequencies of the selected channels in data order, in Hz, in the
    /// frame [`ConvolutionFunctionSet::key`] receives them in, as the
    /// machine saw them first: CASA evaluates a window's beams at the
    /// frequencies of its first visibility buffer (tclean's in LSRK,
    /// `interpVisFreq_p`).
    pub selected_frequencies_hz: Vec<f64>,
}

/// The mosaic primary-beam kernel set: dense complex cells per dish-class
/// pair and frequency cell with `FT[PB²]` weight cells, unit-sum
/// normalisation, the per-row pointing ramp, scalar Mueller routing and
/// CASA's split sinc correction.
#[derive(Clone, Debug)]
pub struct MosaicPb {
    dishes: usize,
    windows: Vec<FrequencyCells>,
    cells_per_plane: usize,
    imaging: Vec<DenseCell>,
    weight: Vec<DenseCell>,
    /// The imaging cells at the conjugate frequencies, which gridding takes
    /// ([`MosaicRules::TcleanConjugateBeams`]); empty otherwise.
    conjugate: Vec<DenseCell>,
    max_half_support: u16,
    conv_size: usize,
    mueller: MuellerRouting,
    correction: ImageCorrection,
}

/// What sizes a mosaic set before it is built
/// (`HetArrayConvFunc::findConvFunction`): the side of the screen, which
/// the widest dish class sets, and every window's frequency cells.
struct MosaicLayout {
    conv_size: usize,
    cells: Vec<FrequencyCells>,
    cell_count: usize,
}

impl MosaicLayout {
    fn new(
        rules: MosaicRules,
        geometry: &GridGeometry,
        image_frequency_hz: f64,
        dishes: &[PrimaryBeam],
        windows: &[MosaicWindow],
    ) -> Result<Self, OperatorError> {
        if dishes.is_empty() || dishes.len() > usize::from(u8::MAX) {
            return Err(OperatorError::ConvolutionFunction {
                reason: "the mosaic set needs between one and 255 dish classes",
            });
        }
        if windows.is_empty() {
            return Err(OperatorError::ConvolutionFunction {
                reason: "the mosaic set needs at least one spectral window",
            });
        }
        if !image_frequency_hz.is_finite() || image_frequency_hz <= 0.0 {
            return Err(OperatorError::ConvolutionFunction {
                reason: "the mosaic set needs a positive finite image frequency",
            });
        }
        let image = geometry.image();
        let [nx, ny] = image.shape;
        let increment = image.increment_rad;
        // `findConvFunction`: the screen size from the widest dish class,
        // at least a tenth of the image in tclean's machine.
        let mut support = match rules {
            MosaicRules::Tclean | MosaicRules::TcleanConjugateBeams => nx.max(ny) / 10,
            MosaicRules::Simulator => 0,
        };
        for dish in dishes {
            support = support.max(dish.support_pixels(increment[0], image_frequency_hz));
        }
        support = support.min(nx.max(ny));
        // `HetArrayConvFunc::findConvFunction`: the support limited by the
        // image extent, `CompositeNumber::nextLargerEven` (the first even
        // composite strictly above its argument), then `/16*16`.
        // `SimplePBConvFunc` sizes its screen from
        // `Int(max(nx,ny)/2)*2*convSamp` instead; this set follows the
        // heterogeneous-array class whose cells it builds.
        let conv_size = next_larger_even_composite(support + 1) / 16 * 16;
        if conv_size < 16 {
            return Err(OperatorError::ConvolutionFunction {
                reason: "the mosaic screen is smaller than 16 pixels",
            });
        }
        let mut cells = Vec::with_capacity(windows.len());
        let mut cell_count = 0;
        for window in windows {
            let frequency_cells = FrequencyCells::new(rules, window, cell_count)?;
            cell_count += frequency_cells.frequencies_hz.len();
            cells.push(frequency_cells);
        }
        if cell_count > usize::from(u16::MAX) {
            return Err(OperatorError::ConvolutionFunction {
                reason: "the mosaic set has more than 65535 frequency cells",
            });
        }
        Ok(Self {
            conv_size,
            cells,
            cell_count,
        })
    }
}

impl MosaicPb {
    /// Build the cells under `rules` for `geometry`'s image (which is the
    /// grid: the mosaic set grids without padding), `dishes` in CASA's
    /// dish-class order and the selected `windows`: [`Self::screens`], then
    /// [`MosaicScreens::finish`]. `image_frequency_hz` is the image's
    /// frequency at spectral pixel 0, which sizes the screen
    /// (`PBMath1D::support`).
    ///
    /// # Errors
    ///
    /// As those two stages.
    pub fn new(
        rules: MosaicRules,
        geometry: &GridGeometry,
        polarization: &PolarizationRouting,
        image_frequency_hz: f64,
        dishes: &[PrimaryBeam],
        windows: &[MosaicWindow],
    ) -> Result<Self, OperatorError> {
        Self::screens(
            rules,
            geometry,
            polarization,
            image_frequency_hz,
            dishes,
            windows,
        )?
        .finish()
    }

    /// Bytes [`Self::screens`] holds at most for the same arguments: each
    /// dish class's voltage table (an Airy pattern's), the screen every lattice is transformed
    /// on and its planning, one dish pair's beam products, and every pair's
    /// lattices at every beam frequency.
    ///
    /// # Errors
    ///
    /// As [`Self::screens`] refuses the arguments, before it allocates.
    pub fn screens_bytes(
        rules: MosaicRules,
        geometry: &GridGeometry,
        image_frequency_hz: f64,
        dishes: &[PrimaryBeam],
        windows: &[MosaicWindow],
    ) -> Result<u64, OperatorError> {
        let layout = MosaicLayout::new(rules, geometry, image_frequency_hz, dishes, windows)?;
        let conv = layout.conv_size;
        let lattice = conv / 4;
        let pairs = pair_planes(dishes.len());
        let lattices = pairs
            * (layout.cell_count
                * (2 * lattice * lattice * size_of::<Complex32>() + size_of::<PairLattices>())
                + layout.cells.len()
                    * (size_of::<(usize, usize, Vec<PairLattices>)>() + size_of::<usize>()));
        let products = 2 * conv * (conv * size_of::<Complex32>() + size_of::<f32>());
        let tables = dishes.len() * (BeamPattern::BYTES as usize + size_of::<(BeamPattern, f64)>());
        Ok(
            (tables + conv * conv * size_of::<Complex64>() + products + lattices) as u64
                + frequency_cell_bytes(&layout.cells)
                + casa_fft::planning_bytes::<f64>(conv * conv)
                + MuellerRouting::MAXIMUM_BYTES,
        )
    }

    /// The first stage of building the cells: every dish pair's beam
    /// products transformed on the screen at every beam frequency, cropped
    /// to their lattices, with each window's support, the screen sized and
    /// the supports found as `rules`' machine does.
    /// [`MosaicScreens::kernel_bytes`] then says what the cells cropped from
    /// them hold before [`MosaicScreens::finish`] builds them.
    ///
    /// # Errors
    ///
    /// No dish or window, a window without positive finite frequencies, a
    /// screen too small to hold a cell, a failed FFT plan or a zero
    /// support.
    pub fn screens(
        rules: MosaicRules,
        geometry: &GridGeometry,
        polarization: &PolarizationRouting,
        image_frequency_hz: f64,
        dishes: &[PrimaryBeam],
        windows: &[MosaicWindow],
    ) -> Result<MosaicScreens, OperatorError> {
        let MosaicLayout {
            conv_size,
            cells,
            cell_count,
        } = MosaicLayout::new(rules, geometry, image_frequency_hz, dishes, windows)?;
        let image = geometry.image();
        let [nx, ny] = image.shape;
        let increment = image.increment_rad;
        let lattice = conv_size / 4;
        // The screen covers the image field at `conv_size` pixels, so its
        // increment is `Δ · n / convSize` per axis, in degrees as
        // `PBMath1D::apply` works.
        let screen_increment_deg = [
            (increment[0] * nx as f64 / conv_size as f64).to_degrees(),
            (increment[1] * ny as f64 / conv_size as f64).to_degrees(),
        ];
        let patterns = dishes
            .iter()
            .map(|dish| (dish.pattern(), dish.maximum_radius_arcmin_ghz()))
            .collect::<Vec<_>>();
        let mut fft = PlaneFft::<f64>::new([conv_size, conv_size], false)?;
        let mut screen = vec![Complex64::default(); conv_size * conv_size];
        let planes = pair_planes(dishes.len());
        // One lattice per beam frequency of every pair and window, in
        // CASA's `[x][y]` quarter-cropped layout, with the pair's support
        // (`supportAndNormalizeLatt`: from the weight plane of the last beam
        // frequency), in the cell order the keys index.
        let mut screened = Vec::with_capacity(planes * cells.len());
        let mut window_support = vec![0_usize; cells.len()];
        for k in 0..dishes.len() {
            for j in k..dishes.len() {
                for (window_index, window) in cells.iter().enumerate() {
                    let mut lattices = Vec::with_capacity(window.frequencies_hz.len());
                    for &frequency_hz in &window.frequencies_hz {
                        lattices.push(screen_pair(
                            &mut fft,
                            &mut screen,
                            conv_size,
                            screen_increment_deg,
                            [&patterns[k], &patterns[j]],
                            frequency_hz,
                            rules.conjugate_hz(image_frequency_hz, frequency_hz)?,
                        )?);
                    }
                    let last = lattices.last().expect("a window has a cell");
                    let support = match rules {
                        MosaicRules::Tclean | MosaicRules::TcleanConjugateBeams => {
                            weight_plane_support(&last.weight, lattice)?
                        }
                        MosaicRules::Simulator => {
                            simulator_weight_plane_support(&last.weight, lattice)?
                        }
                    };
                    window_support[window_index] = window_support[window_index].max(support);
                    screened.push((window_index, support, lattices));
                }
            }
        }
        // `conjSupport`: every pair of a window takes the support widened
        // by the root of the window's largest `ν_c/ν`, when that is wider.
        let mut ratios = Vec::new();
        let mut widened = vec![false; cells.len()];
        for ((window, support), widened) in cells.iter().zip(&mut window_support).zip(&mut widened)
        {
            let mut largest = f64::NEG_INFINITY;
            for &frequency_hz in &window.frequencies_hz {
                if let Some(conjugate) = rules.conjugate_hz(image_frequency_hz, frequency_hz)? {
                    ratios.push(conjugate / frequency_hz);
                    largest = largest.max(conjugate / frequency_hz);
                }
            }
            let conjugate = (*support as f64 * largest.sqrt() / 2.0) as usize * 2;
            if largest.is_finite() && conjugate > *support {
                *support = conjugate;
                *widened = true;
            }
        }
        Ok(MosaicScreens {
            rules,
            dishes: dishes.len(),
            windows: cells,
            cell_count,
            screened,
            window_support,
            widened,
            ratios,
            lattice,
            conv_size,
            grid: geometry.grid_shape(),
            mueller: MuellerRouting::scalar(polarization.pol_map(), polarization.grid_pols()),
        })
    }

    /// Bytes the set holds: its imaging, weight and conjugate cells and its
    /// corrections.
    #[must_use]
    pub fn resident_bytes(&self) -> u64 {
        self.imaging
            .iter()
            .chain(&self.weight)
            .chain(&self.conjugate)
            .map(|cell| cell.bytes() as u64)
            .sum::<u64>()
            + ((self.imaging.capacity() + self.weight.capacity() + self.conjugate.capacity())
                * size_of::<DenseCell>()) as u64
            + frequency_cell_bytes(&self.windows)
            + self.mueller.bytes()
            + self.correction.resident_bytes()
    }
}

/// Bytes the windows' frequency cells hold, from their capacities.
fn frequency_cell_bytes(windows: &Vec<FrequencyCells>) -> u64 {
    (windows.capacity() * size_of::<FrequencyCells>()
        + windows
            .iter()
            .map(|window| window.frequencies_hz.capacity() * size_of::<f64>())
            .sum::<usize>()) as u64
}

/// A mosaic set's beam products transformed on their screens and cropped
/// to their lattices, with each window's support, before their cells are
/// cut and resampled ([`MosaicPb::screens`], [`Self::finish`]).
#[derive(Debug)]
pub struct MosaicScreens {
    rules: MosaicRules,
    dishes: usize,
    windows: Vec<FrequencyCells>,
    cell_count: usize,
    /// Per dish pair and window: the window, the pair's support and its
    /// lattices at every beam frequency.
    screened: Vec<(usize, usize, Vec<PairLattices>)>,
    window_support: Vec<usize>,
    /// Per window: whether `conjSupport` widened its supports, so every
    /// pair grids with the window's.
    widened: Vec<bool>,
    /// Per frequency cell, in cell order: `ν_c/ν` under
    /// [`MosaicRules::TcleanConjugateBeams`]; empty otherwise.
    ratios: Vec<f64>,
    lattice: usize,
    conv_size: usize,
    grid: [usize; 2],
    mueller: MuellerRouting,
}

impl MosaicScreens {
    /// Bytes the screens hold: every pair's lattices and the supports.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        (self
            .screened
            .iter()
            .map(|(_, _, lattices)| {
                lattices.capacity() * size_of::<PairLattices>()
                    + lattices
                        .iter()
                        .map(|pair| {
                            (pair.imaging.capacity() + pair.weight.capacity())
                                * size_of::<Complex32>()
                        })
                        .sum::<usize>()
            })
            .sum::<usize>()
            + self.screened.capacity() * size_of::<(usize, usize, Vec<PairLattices>)>()
            + self.window_support.capacity() * size_of::<usize>()
            + self.widened.capacity()
            + self.ratios.capacity() * size_of::<f64>()) as u64
            + frequency_cell_bytes(&self.windows)
            + self.mueller.bytes()
    }

    /// Bytes [`Self::finish`] allocates beside the screens: the imaging,
    /// weight and conjugate cells it cuts from them, which the set keeps
    /// ([`MosaicPb::resident_bytes`]), one cell's crop and resampling (and
    /// its conjugate's) at a time, and the corrections.
    #[must_use]
    pub fn kernel_bytes(&self) -> u64 {
        let kinds = if self.ratios.is_empty() { 2 } else { 3 };
        let cells = self
            .screened
            .iter()
            .map(|(window_index, support, lattices)| {
                let half =
                    u16::try_from(self.taps(*window_index, *support)).expect("support fits u16");
                kinds
                    * lattices.len() as u64
                    * dense_cell_bytes(1, MOSAIC_OVERSAMPLING, [half, half])
            })
            .sum::<u64>();
        let crop = self
            .window_support
            .iter()
            .map(|support| self.crop(*support))
            .max()
            .unwrap_or(0);
        let resampled = crop * usize::from(MOSAIC_OVERSAMPLING);
        let mut transients = crop * crop * (size_of::<Complex32>() + 2 * size_of::<f32>())
            + resampled * resampled * size_of::<Complex32>();
        if let Some(largest) = self.ratios.iter().copied().reduce(f64::max) {
            // The imaging plane held while its conjugate is resampled from
            // its parts and set in a plane of its own.
            let scaled = conjugate_side(resampled, largest);
            transients +=
                resampled * resampled * (2 * size_of::<Complex32>() + 2 * size_of::<f32>())
                    + scaled * scaled * size_of::<Complex32>();
        }
        let records =
            kinds as usize * self.cell_count * pair_planes(self.dishes) * size_of::<DenseCell>();
        cells + (transients + records) as u64 + ImageCorrection::bytes(self.grid)
    }

    /// The side every pair of a window of `support` is cropped to.
    fn crop(&self, support: usize) -> usize {
        (2 * (support + 2)).min(self.lattice)
    }

    /// Taps on each side of a pair of `support` in window `window`: the
    /// window's widest when `conjSupport` widened it, else as the rules
    /// grid.
    fn taps(&self, window: usize, support: usize) -> usize {
        if self.widened[window] {
            self.window_support[window]
        } else {
            self.rules.taps(support, self.window_support[window])
        }
    }

    /// The second stage of building the cells: crop every pair of a window
    /// to the widest pair's `2(support + 2)`, normalise each over its own
    /// support, resample, and form the corrections.
    ///
    /// # Errors
    ///
    /// A crop too small to resample, or a non-positive
    /// convolution-function integral.
    pub fn finish(self) -> Result<MosaicPb, OperatorError> {
        let planes = pair_planes(self.dishes);
        // `findConvFunction` crops every pair of a window to the widest
        // pair's `2(support + 2)`; each pair is normalised over its own
        // support. tclean grids each pair with its own support (or the
        // window's, which `conjSupport` widened); the simulator's `MosaicFT`
        // grids every row with the widest.
        let mut imaging = Vec::with_capacity(planes * self.cell_count);
        let mut weight = Vec::with_capacity(planes * self.cell_count);
        let mut conjugate = Vec::with_capacity(if self.ratios.is_empty() {
            0
        } else {
            planes * self.cell_count
        });
        let max_half_support = self.window_support.iter().copied().max().unwrap_or(0);
        for (window_index, support, lattices) in &self.screened {
            let crop = self.crop(self.window_support[*window_index]);
            let taps = self.taps(*window_index, *support);
            // `interpLanczos` zeroes within three input pixels of the edge, so
            // a crop under seven pixels resamples to nothing.
            if crop < 2 * LANCZOS_A as usize + 1 {
                return Err(OperatorError::ConvolutionFunction {
                    reason: "the mosaic beam spans too few screen pixels; use a larger image or finer cells",
                });
            }
            // The lattice bounds the crop; a support `conjSupport` widened
            // past it has no taps to read.
            if taps + 1 > crop / 2 {
                return Err(OperatorError::ConvolutionFunction {
                    reason: "the conjugate beams widen the mosaic support past its screen; use a larger image",
                });
            }
            let first = self.windows[*window_index].first;
            for (index, lattice_pair) in lattices.iter().enumerate() {
                let cells = finish_pair(
                    lattice_pair,
                    self.lattice,
                    (*support, crop, taps),
                    self.ratios.get(first + index).copied(),
                )?;
                imaging.push(cells.imaging);
                weight.push(cells.weight);
                conjugate.extend(cells.conjugate);
            }
        }
        let Self {
            dishes,
            windows: cells,
            cell_count,
            conv_size,
            grid,
            mueller,
            ..
        } = self;
        let half_support = u16::try_from(max_half_support).expect("support fits u16");
        let sinc = |len: usize| {
            (0..len)
                .map(|index| {
                    // `MosaicFT::getImage`, `prepGridForDegrid`: Float
                    // arithmetic, unity at the centre.
                    let x = std::f32::consts::PI * (index as f32 - (len / 2) as f32)
                        / (len as f32 * f32::from(MOSAIC_OVERSAMPLING));
                    if index == len / 2 {
                        1.0
                    } else {
                        f64::from(x.sin() / x)
                    }
                })
                .collect::<Vec<_>>()
        };
        let [grid_nx, grid_ny] = grid;
        let model_x = sinc(grid_nx);
        let model_y = sinc(grid_ny);
        let image_x = model_x.iter().map(|value| 1.0 / value).collect();
        let image_y = model_y.iter().map(|value| 1.0 / value).collect();
        Ok(MosaicPb {
            dishes,
            windows: cells,
            cells_per_plane: cell_count,
            imaging,
            weight,
            conjugate,
            max_half_support: half_support,
            conv_size,
            mueller,
            correction: ImageCorrection::split(image_x, image_y, model_x, model_y),
        })
    }
}

impl MosaicPb {
    /// Number of dish classes.
    #[must_use]
    pub const fn dishes(&self) -> usize {
        self.dishes
    }

    /// The screen size in pixels (`HetArrayConvFunc::convSize_p` before
    /// the quarter crop).
    #[must_use]
    pub const fn screen_size(&self) -> usize {
        self.conv_size
    }

    /// Beam frequencies of `spectral_window` in cell order, or `None` for a
    /// window the set was not built for.
    #[must_use]
    pub fn frequency_cells(&self, spectral_window: u32) -> Option<&[f64]> {
        self.window(spectral_window)
            .map(|window| window.frequencies_hz.as_slice())
    }

    /// Half support of the cell `key` names.
    #[must_use]
    pub fn half_support(&self, key: CfKey) -> u16 {
        self.imaging[self.index(key)].support()[0] / 2
    }

    fn window(&self, spectral_window: u32) -> Option<&FrequencyCells> {
        self.windows
            .iter()
            .find(|window| window.spectral_window == spectral_window)
    }

    fn index(&self, key: CfKey) -> usize {
        usize::from(key.group) * self.cells_per_plane + usize::from(key.cube)
    }
}

/// `HetArrayConvFunc::findConvFunction`: dish-class pairs `(k, j)` with
/// `k ≤ j`.
fn pair_planes(dishes: usize) -> usize {
    dishes * (dishes + 1) / 2
}

/// `HetArrayConvFunc::makerowmap`: the plane of the pair of dish classes
/// `types`, in either order.
#[must_use]
pub fn pair_plane(types: [u8; 2], dishes: usize) -> usize {
    let (first, second) = if types[1] < types[0] {
        (usize::from(types[1]), usize::from(types[0]))
    } else {
        (usize::from(types[0]), usize::from(types[1]))
    };
    let mut plane = 0;
    for jj in 0..first {
        plane += dishes - jj - 1;
    }
    plane + second
}

impl ConvolutionFunctionSet for MosaicPb {
    /// Group: the dish-class pair plane; cube: the frequency cell of the
    /// row's window, by `SimplePBConvFunc::findUsefulChannels`'s `chanMap`.
    /// A window the set was not built for, or a dish class beyond the set's
    /// classes, is a programmer error.
    fn key(&self, row: &RowContext, freq_hz: f64, _w_lambda: f64) -> CfKey {
        let window = self
            .window(row.spectral_window)
            .expect("mosaic set built for every selected spectral window");
        assert!(
            usize::from(row.antenna_types[0]) < self.dishes
                && usize::from(row.antenna_types[1]) < self.dishes,
            "antenna types index the mosaic set's dish classes"
        );
        CfKey {
            group: u16::try_from(pair_plane(row.antenna_types, self.dishes))
                .expect("pair plane fits u16"),
            cube: u16::try_from(window.first + window.cell_of(freq_hz)).expect("cell fits u16"),
        }
    }

    /// The imaging cell, or its conjugate under
    /// [`MosaicRules::TcleanConjugateBeams`]: `MosaicFT` grids the data
    /// and the PSF with the conjugate.
    fn taps<'s>(&'s self, key: CfKey, _hold: &'s mut CellHold) -> TapLayout<'s> {
        self.conjugate
            .get(self.index(key))
            .unwrap_or(&self.imaging[self.index(key)])
            .layout()
    }

    /// The imaging cell at the beam frequency: `MosaicFT` predicts with it
    /// whatever its gridding takes.
    fn prediction_taps<'s>(&'s self, key: CfKey, _hold: &'s mut CellHold) -> TapLayout<'s> {
        self.imaging[self.index(key)].layout()
    }

    fn max_half_support(&self) -> [u16; 2] {
        [self.max_half_support; 2]
    }

    fn weight_taps<'s>(&'s self, key: CfKey, _hold: &'s mut CellHold) -> Option<TapLayout<'s>> {
        Some(self.weight[self.index(key)].layout())
    }

    fn mueller(&self) -> &MuellerRouting {
        &self.mueller
    }

    fn image_correction(&self) -> &ImageCorrection {
        &self.correction
    }

    /// `MosaicFT::finalizeToSky` transforms the gridded weight functions
    /// into `skyCoverage_p` without the sinc `getImage` divides by
    /// (`refim_alma_mosaic` against CASA: the corrected weight sat
    /// `1/sinc` high, 0.4 % at the field edge).
    fn corrects_weight_image(&self) -> bool {
        false
    }

    /// `fmosaic.f` `sectgmosd2`, `gmoswgtd`: `sumwt += weight`; `sectdmos2`
    /// does not divide.
    fn normalisation(&self) -> KernelNormalisation {
        KernelNormalisation::UnitSum
    }

    fn pointing_ramp(&self) -> bool {
        true
    }

    /// `MosaicFT::put` grids through `sectgmosd2` and `gmoswgtd2`, which
    /// clip a support to the grid (`onmosgrid`), and predicts through
    /// `sectdmos2`, which drops a sample that overruns it (`omos`).
    fn grid_edge(&self) -> GridEdge {
        GridEdge::Clipped
    }

    /// Every pair plane's imaging and weight cell of every frequency cell,
    /// all resident.
    fn tap_footprint(&self) -> TapFootprint {
        TapFootprint::of(
            self.imaging
                .iter()
                .chain(&self.weight)
                .chain(&self.conjugate)
                .map(DenseCell::layout),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pair_planes_follow_makerowmap_in_either_order() {
        // Three classes: (0,0)=0 (0,1)=1 (0,2)=2 (1,1)=3 (1,2)=4 (2,2)=5.
        assert_eq!(pair_plane([0, 0], 3), 0);
        assert_eq!(pair_plane([0, 2], 3), 2);
        assert_eq!(pair_plane([1, 1], 3), 3);
        assert_eq!(pair_plane([2, 1], 3), 4);
        assert_eq!(pair_plane([1, 2], 3), 4);
        assert_eq!(pair_plane([2, 2], 3), 5);
        assert_eq!(pair_planes(3), 6);
        assert_eq!(pair_plane([1, 0], 2), 1);
        assert_eq!(pair_plane([1, 1], 2), 2);
    }
}
