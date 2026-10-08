// SPDX-License-Identifier: LGPL-3.0-or-later
//! The mosaic primary-beam kernel set, pinned to CASA `HetArrayConvFunc`
//! (`TransformMachines2/HetArrayConvFunc.cc`), `SimplePBConvFunc`,
//! `PBMath1D`/`PBMath1DAiry`, `MosaicFT` and `fmosaic.f`.
//!
//! One dense complex cell per (dish-class pair, spectral window, frequency
//! cell): `FT[VP₁ · VP₂]` for imaging and `FT[PB₁ · PB₂]` for the weight
//! image, both built on a screen covering the image field at the
//! convolution-function size, quarter-cropped, support-searched on the
//! weight plane, normalised over the support, cropped again and Lanczos
//! resampled to ten fine offsets per cell. The pointing enters only through
//! the per-row phase gradient ([`ConvolutionFunctionSet::pointing_ramp`]).

use casa_numerics::AnnularApertureVoltageTable;
use num_complex::{Complex32, Complex64};

use crate::convolution::{
    CellHold, ConvolutionFunctionSet, DenseCell, ImageCorrection, KernelNormalisation,
    MuellerRouting, RowContext, TapLayout,
};
use crate::dense::{OversampledPlane, dense_cell};
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
/// `HetArrayConvFunc::supportAndNormalizeLatt`: the support ends where the
/// weight plane falls below this fraction of its peak along both axes.
const SUPPORT_CUT_LEVEL: f32 = 2.5e-2;
/// `HetArrayConvFunc::interpLanczos`: the Lanczos window order.
const LANCZOS_A: i64 = 3;
/// `PBMath1DAiry` in `findAntennaSizes`: the reference frequency of the
/// dish-class support radius, in GHz.
const DISH_REFERENCE_GHZ: f64 = 100.0;

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

    /// `HetArrayConvFunc::findAntennaSizes` for any other telescope: the
    /// dish diameter itself, ALMA's blockage ratio `d/12 · 0.75` and a
    /// 150″ radius.
    #[must_use]
    pub fn casa_airy(dish_diameter_m: f64) -> Self {
        Self {
            aperture_m: dish_diameter_m,
            blockage_m: dish_diameter_m / 12.0 * 0.75,
            max_radius_arcsec: 150.0,
        }
    }

    /// `PBMath1D::maximumRadius_p`: the support radius scaled to 1 GHz, in
    /// arcminutes times GHz.
    fn max_radius_arcmin_ghz(&self) -> f64 {
        self.max_radius_arcsec / 60.0 * DISH_REFERENCE_GHZ
    }

    fn table(&self) -> AnnularApertureVoltageTable {
        AnnularApertureVoltageTable::new(
            self.aperture_m,
            self.blockage_m,
            self.max_radius_arcmin_ghz(),
        )
    }

    /// `PBMath1D::support`: the voltage-pattern diameter in image pixels at
    /// `frequency_hz`, from the x increment.
    fn support_pixels(&self, increment_x_rad: f64, frequency_hz: f64) -> usize {
        let radius_rad = (self.max_radius_arcmin_ghz() / 60.0).to_radians();
        (radius_rad / increment_x_rad.abs() * 2.0 * 1.0e9 / frequency_hz).floor() as usize
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
    /// Frequencies of the selected channels in data order, in Hz: what
    /// [`ConvolutionFunctionSet::key`] receives for this window.
    pub selected_frequencies_hz: Vec<f64>,
}

/// The frequency cells of one window and the rule that maps a frequency
/// to one of them.
#[derive(Clone, Debug, PartialEq)]
struct FrequencyCells {
    spectral_window: u32,
    /// Beam frequencies in cell order.
    frequencies_hz: Vec<f64>,
    /// `Some(tol)` applies CASA's `chanMap` rule (first cell within
    /// `tol/2`, else the nearest); `None` is the identity branch, where a
    /// channel's own frequency is a cell.
    tolerance_hz: Option<f64>,
    /// Index of the window's first cell in the set's flat cell list.
    first: usize,
}

impl FrequencyCells {
    /// `SimplePBConvFunc::findUsefulChannels` (the `Vector<Int>& chanMap`
    /// overload): cells every `tol = max(0.5 % of the window's top
    /// frequency, half the selected spacing)` stepping down from the
    /// window's top frequency, the identity when the cell count reaches the
    /// selected channel count less one, a single cell at the bottom
    /// frequency when no cell fits.
    fn new(window: &MosaicWindow, first: usize) -> Result<Self, OperatorError> {
        let selected = &window.selected_frequencies_hz;
        if selected.is_empty()
            || window.window_frequencies_hz.is_empty()
            || selected
                .iter()
                .chain(&window.window_frequencies_hz)
                .any(|value| !value.is_finite() || *value <= 0.0)
        {
            return Err(OperatorError::ConvolutionFunction {
                reason: "a mosaic window needs positive finite channel frequencies",
            });
        }
        let min_freq = selected.iter().copied().fold(f64::INFINITY, f64::min);
        let max_freq = selected.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let original_width = if selected.len() == 1 {
            1.0e12
        } else {
            (max_freq - min_freq) / (selected.len() - 1) as f64
        };
        let window_top = window
            .window_frequencies_hz
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let mut tol = window_top * 0.5 / 100.0;
        if tol < original_width / 2.0 {
            tol = original_width / 2.0;
        }
        let mut top = window_top;
        while top > max_freq {
            top -= tol;
        }
        if top < min_freq {
            top += tol;
        }
        let mut bottom = top;
        let mut cells = 0_usize;
        while bottom > min_freq {
            cells += 1;
            bottom -= tol;
        }
        if cells > 1 {
            cells -= 1;
            bottom += tol;
        }
        if cells > selected.len() {
            cells = selected.len();
            tol = window.channel_width_hz.abs();
            bottom = min_freq;
        }
        let frequencies_hz = if cells + 1 >= selected.len() {
            return Ok(Self {
                spectral_window: window.spectral_window,
                frequencies_hz: selected.clone(),
                tolerance_hz: None,
                first,
            });
        } else if cells == 0 {
            vec![bottom]
        } else {
            (0..cells).map(|k| k as f64 * tol + bottom).collect()
        };
        Ok(Self {
            spectral_window: window.spectral_window,
            frequencies_hz,
            tolerance_hz: Some(tol),
            first,
        })
    }

    /// The cell of `freq_hz` within this window.
    fn cell_of(&self, freq_hz: f64) -> usize {
        let nearest = || {
            self.frequencies_hz
                .iter()
                .enumerate()
                .fold((0, f64::INFINITY), |best, (index, cell)| {
                    let diff = (freq_hz - cell).abs();
                    if diff < best.1 { (index, diff) } else { best }
                })
                .0
        };
        match self.tolerance_hz {
            None => nearest(),
            Some(tol) => self
                .frequencies_hz
                .iter()
                .position(|cell| (freq_hz - cell).abs() <= tol / 2.0)
                .unwrap_or_else(nearest),
        }
    }
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
    max_half_support: u16,
    conv_size: usize,
    mueller: MuellerRouting,
    correction: ImageCorrection,
}

impl MosaicPb {
    /// Build the cells for `geometry`'s image (which is the grid: the
    /// mosaic set grids without padding), `dishes` in CASA's dish-class
    /// order and the selected `windows`. `image_frequency_hz` is the
    /// image's frequency at spectral pixel 0, which sizes the screen
    /// (`PBMath1D::support`).
    ///
    /// # Errors
    ///
    /// No dish or window, a window without positive finite frequencies, a
    /// screen too small to hold a cell, a failed FFT plan, a zero support
    /// or a non-positive convolution-function integral.
    pub fn new(
        geometry: &GridGeometry,
        polarization: &PolarizationRouting,
        image_frequency_hz: f64,
        dishes: &[AiryDish],
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
        // `findConvFunction`: the screen size from the widest dish class.
        let mut support = nx.max(ny) / 10;
        for dish in dishes {
            support = support.max(dish.support_pixels(increment[0], image_frequency_hz));
        }
        support = support.min(nx.max(ny));
        let conv_size = next_larger_even_composite(support) / 16 * 16;
        if conv_size < 16 {
            return Err(OperatorError::ConvolutionFunction {
                reason: "the mosaic screen is smaller than 16 pixels",
            });
        }
        let lattice = conv_size / 4;
        // The screen covers the image field at `conv_size` pixels, so its
        // increment is `Δ · n / convSize` per axis, in degrees as
        // `PBMath1D::apply` works.
        let screen_increment_deg = [
            (increment[0] * nx as f64 / conv_size as f64).to_degrees(),
            (increment[1] * ny as f64 / conv_size as f64).to_degrees(),
        ];
        let mut cells = Vec::with_capacity(windows.len());
        let mut cell_count = 0;
        for window in windows {
            let frequency_cells = FrequencyCells::new(window, cell_count)?;
            cell_count += frequency_cells.frequencies_hz.len();
            cells.push(frequency_cells);
        }
        if cell_count > usize::from(u16::MAX) {
            return Err(OperatorError::ConvolutionFunction {
                reason: "the mosaic set has more than 65535 frequency cells",
            });
        }
        let tables = dishes.iter().map(AiryDish::table).collect::<Vec<_>>();
        let mut fft = PlaneFft::<f64>::new([conv_size, conv_size], false)?;
        let mut screen = vec![Complex64::default(); conv_size * conv_size];
        let planes = pair_planes(dishes.len());
        let mut imaging = Vec::with_capacity(planes * cell_count);
        let mut weight = Vec::with_capacity(planes * cell_count);
        let mut max_half_support = 0_usize;
        for k in 0..dishes.len() {
            for j in k..dishes.len() {
                for window in &cells {
                    // One lattice per beam frequency of this window and
                    // pair, in CASA's `[x][y]` quarter-cropped layout.
                    let mut lattices = Vec::with_capacity(window.frequencies_hz.len());
                    for &frequency_hz in &window.frequencies_hz {
                        lattices.push(screen_pair(
                            &mut fft,
                            &mut screen,
                            conv_size,
                            screen_increment_deg,
                            [&dishes[k], &dishes[j]],
                            [&tables[k], &tables[j]],
                            frequency_hz,
                        )?);
                    }
                    // `supportAndNormalizeLatt`: the support from the weight
                    // plane of the last beam frequency, every cell of the
                    // pair normalised over that support.
                    let last = lattices.last().expect("a window has a cell");
                    let support = weight_plane_support(&last.weight, lattice)?;
                    let crop = (2 * (support + 2)).min(lattice);
                    max_half_support = max_half_support.max(support);
                    for lattice_pair in &lattices {
                        let (imaging_cell, weight_cell) =
                            finish_pair(lattice_pair, lattice, support, crop)?;
                        imaging.push(imaging_cell);
                        weight.push(weight_cell);
                    }
                }
            }
        }
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
        let [grid_nx, grid_ny] = geometry.grid_shape();
        let model_x = sinc(grid_nx);
        let model_y = sinc(grid_ny);
        let image_x = model_x.iter().map(|value| 1.0 / value).collect();
        let image_y = model_y.iter().map(|value| 1.0 / value).collect();
        Ok(Self {
            dishes: dishes.len(),
            windows: cells,
            cells_per_plane: cell_count,
            imaging,
            weight,
            max_half_support: half_support,
            conv_size,
            mueller: MuellerRouting::scalar(polarization.pol_map(), polarization.grid_pols()),
            correction: ImageCorrection::split(image_x, image_y, model_x, model_y),
        })
    }

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
        self.imaging[self.index(key)].support[0] / 2
    }

    /// Bytes of the imaging and weight tap values.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.imaging
            .iter()
            .chain(&self.weight)
            .map(DenseCell::bytes)
            .sum()
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

    fn taps<'s>(&'s self, key: CfKey, _hold: &'s mut CellHold) -> TapLayout<'s> {
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

    /// `fmosaic.f` `sectgmosd2`, `gmoswgtd`: `sumwt += weight`; `sectdmos2`
    /// does not divide.
    fn normalisation(&self) -> KernelNormalisation {
        KernelNormalisation::UnitSum
    }

    fn pointing_ramp(&self) -> bool {
        true
    }
}

/// The quarter-cropped imaging and weight lattices of one pair at one
/// frequency, `side × side`, x fastest, origin at `side/2`.
struct PairLattices {
    imaging: Vec<Complex32>,
    weight: Vec<Complex32>,
}

/// `findConvFunction`'s screens for one dish pair at `frequency_hz`:
/// `VP_k · VP_j` and `PB_k · PB_j` over the centred screen
/// (`PBMath1D::apply` with `iPower` 1 and 2), transformed and
/// quarter-cropped to `convSize/4`.
fn screen_pair(
    fft: &mut PlaneFft<f64>,
    screen: &mut [Complex64],
    conv_size: usize,
    increment_deg: [f64; 2],
    dishes: [&AiryDish; 2],
    tables: [&AnnularApertureVoltageTable; 2],
    frequency_hz: f64,
) -> Result<PairLattices, OperatorError> {
    let centre = conv_size / 2;
    // `PBMath1D::apply`: squared pixel offsets in degrees² held as Float.
    let axis = |increment: f64| {
        (0..conv_size)
            .map(|index| (increment * (index as f64 - centre as f64)).powi(2) as f32)
            .collect::<Vec<_>>()
    };
    let rx2 = axis(increment_deg[0]);
    let ry2 = axis(increment_deg[1]);
    let factor = 60.0 * frequency_hz / 1.0e9;
    let voltage = |dish: &AiryDish, table: &AnnularApertureVoltageTable, r2: f32| -> f32 {
        let rmax2 = (dish.max_radius_arcmin_ghz() / factor).powi(2);
        if f64::from(r2) > rmax2 {
            0.0
        } else {
            let radius = (f64::from(r2).sqrt() * factor) as f32;
            table.evaluate(f64::from(radius))
        }
    };
    let mut imaging = vec![Complex32::default(); conv_size * conv_size];
    let mut weight = vec![Complex32::default(); conv_size * conv_size];
    for (iy, ry2) in ry2.iter().enumerate() {
        for (ix, rx2) in rx2.iter().enumerate() {
            let r2 = rx2 + ry2;
            let vk = voltage(dishes[0], tables[0], r2);
            let vj = voltage(dishes[1], tables[1], r2);
            let index = iy * conv_size + ix;
            imaging[index] = Complex32::new(vk * vj, 0.0);
            weight[index] = Complex32::new((vk * vk) * (vj * vj), 0.0);
        }
    }
    let lattice = conv_size / 4;
    let quarter = lattice * 3 / 2;
    let mut transform = |values: &[Complex32]| -> Result<Vec<Complex32>, OperatorError> {
        for (target, value) in screen.iter_mut().zip(values) {
            *target = Complex64::new(f64::from(value.re), f64::from(value.im));
        }
        fft.transform(screen, false)?;
        let mut cropped = Vec::with_capacity(lattice * lattice);
        for y in 0..lattice {
            for x in 0..lattice {
                let value = screen[(quarter + y) * conv_size + quarter + x];
                cropped.push(Complex32::new(value.re as f32, value.im as f32));
            }
        }
        Ok(cropped)
    };
    Ok(PairLattices {
        imaging: transform(&imaging)?,
        weight: transform(&weight)?,
    })
}

/// `HetArrayConvFunc::supportAndNormalizeLatt` at screen sampling 1: the
/// first radius from the weight plane's peak where both axes fall below
/// the cut level, plus one; at least six when the lattice allows; capped
/// below half the lattice.
fn weight_plane_support(plane: &[Complex32], side: usize) -> Result<usize, OperatorError> {
    let sampling = 1_usize;
    let (mut max_abs, mut min_abs, mut max_pos) = (f32::NEG_INFINITY, f32::INFINITY, [0, 0]);
    for y in 0..side {
        for x in 0..side {
            let value = plane[y * side + x].norm();
            if value > max_abs {
                max_abs = value;
                max_pos = [x, y];
            }
            min_abs = min_abs.min(value);
        }
    }
    let cut = SUPPORT_CUT_LEVEL * max_abs;
    let mut found = false;
    let mut trial = 0;
    // CASA walks from the peak towards the origin; a peak nearer the
    // origin than the walk ends it early.
    let limit = side
        .saturating_sub(max_pos[0].max(max_pos[1]) + 2)
        .min(max_pos[0].min(max_pos[1]) + 1);
    while trial < limit {
        if plane[max_pos[1] * side + max_pos[0] - trial].norm() < cut
            && plane[(max_pos[1] - trial) * side + max_pos[0]].norm() < cut
        {
            found = true;
            break;
        }
        trial += 1;
    }
    if !found {
        if max_abs - min_abs > cut {
            found = true;
        }
        trial = side / 2 - 4 * sampling;
    }
    if !found {
        return Err(OperatorError::ConvolutionFunction {
            reason: "the mosaic weight convolution function has no support",
        });
    }
    if trial < 5 * sampling {
        trial = if 10 * sampling < side {
            5 * sampling
        } else {
            side / 2 - 4 * sampling
        };
    }
    let mut support = (0.5 + trial as f64 / sampling as f64) as usize + 1;
    if support * sampling >= side / 2 {
        support = side / 2 / sampling - 1;
    }
    Ok(support)
}

/// `supportAndNormalizeLatt`'s normalisation, `findConvFunction`'s crop to
/// `2(support + 2)` and the Lanczos resample to [`MOSAIC_OVERSAMPLING`],
/// then the dense cells of `support` taps on each side of the centre.
fn finish_pair(
    lattices: &PairLattices,
    side: usize,
    support: usize,
    crop: usize,
) -> Result<(DenseCell, DenseCell), OperatorError> {
    let cell = |plane: &[Complex32]| -> Result<DenseCell, OperatorError> {
        let integral = block_sum_re(plane, side, support);
        if !integral.is_finite() || integral <= 0.0 {
            return Err(OperatorError::ConvolutionFunction {
                reason: "the mosaic convolution function integral is not positive",
            });
        }
        let scale = (1.0 / integral) as f32;
        let offset = side / 2 - crop / 2;
        let mut cropped = Vec::with_capacity(crop * crop);
        for y in 0..crop {
            for x in 0..crop {
                cropped.push(plane[(offset + y) * side + offset + x] * scale);
            }
        }
        let resampled = lanczos_resample(&cropped, crop, usize::from(MOSAIC_OVERSAMPLING));
        let out_side = crop * usize::from(MOSAIC_OVERSAMPLING);
        let plane = OversampledPlane {
            values: &resampled,
            side: out_side,
            centre: [out_side / 2; 2],
            sampling: MOSAIC_OVERSAMPLING,
        };
        let half = u16::try_from(support).expect("support fits u16");
        Ok(dense_cell(&[plane], [half, half]))
    };
    Ok((cell(&lattices.imaging)?, cell(&lattices.weight)?))
}

/// `Re Σ` over the `[−support, support]²` block about the lattice centre.
fn block_sum_re(plane: &[Complex32], side: usize, support: usize) -> f64 {
    let centre = side / 2;
    let mut sum = 0.0;
    for y in centre - support..=centre + support {
        for x in centre - support..=centre + support {
            sum += f64::from(plane[y * side + x].re);
        }
    }
    sum
}

/// `HetArrayConvFunc::resample`: the `side × side` plane resampled to
/// `side · factor` per axis, output pixel `j` reading input coordinate
/// `j / factor` through the Lanczos-3 window on the real and imaginary
/// parts, zero within three input pixels of the edges.
fn lanczos_resample(input: &[Complex32], side: usize, factor: usize) -> Vec<Complex32> {
    let out_side = side * factor;
    let real = input.iter().map(|value| value.re).collect::<Vec<_>>();
    let imag = input.iter().map(|value| value.im).collect::<Vec<_>>();
    let mut out = Vec::with_capacity(out_side * out_side);
    for k in 0..out_side {
        let y = k as f64 / out_side as f64 * side as f64;
        for j in 0..out_side {
            let x = j as f64 / out_side as f64 * side as f64;
            out.push(Complex32::new(
                interp_lanczos(x, y, side, &real),
                interp_lanczos(x, y, side, &imag),
            ));
        }
    }
    out
}

/// `HetArrayConvFunc::interpLanczos` with `a = 3`.
fn interp_lanczos(x: f64, y: f64, side: usize, data: &[f32]) -> f32 {
    let floor_x = x.floor() as i64;
    let floor_y = y.floor() as i64;
    let n = side as i64;
    if floor_x < LANCZOS_A
        || floor_x >= n - LANCZOS_A
        || floor_y < LANCZOS_A
        || floor_y >= n - LANCZOS_A
    {
        return 0.0;
    }
    let a = LANCZOS_A as f64;
    let mut result = 0.0_f64;
    for i in floor_x - LANCZOS_A + 1..=floor_x + LANCZOS_A {
        let wx = sinc(x - i as f64) * sinc((x - i as f64) / a);
        for j in floor_y - LANCZOS_A + 1..=floor_y + LANCZOS_A {
            let wy = sinc(y - j as f64) * sinc((y - j as f64) / a);
            result += f64::from(data[(j * n + i) as usize]) * wx * wy;
        }
    }
    result as f32
}

/// `HetArrayConvFunc::sinc`: `sin(πx)/(πx)`, one at zero.
fn sinc(x: f64) -> f64 {
    if x == 0.0 {
        1.0
    } else {
        let px = std::f64::consts::PI * x;
        px.sin() / px
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(selected: Vec<f64>, window_top: f64, width: f64) -> MosaicWindow {
        MosaicWindow {
            spectral_window: 3,
            window_frequencies_hz: vec![window_top - 10.0 * width, window_top],
            channel_width_hz: width,
            selected_frequencies_hz: selected,
        }
    }

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

    #[test]
    fn a_single_selected_channel_is_its_own_cell() {
        let cells = FrequencyCells::new(&window(vec![100.0e9], 101.0e9, 1.0e6), 4).expect("cells");
        assert_eq!(cells.frequencies_hz, vec![100.0e9]);
        assert_eq!(cells.tolerance_hz, None);
        assert_eq!(cells.first, 4);
        assert_eq!(cells.cell_of(99.0e9), 0);
    }

    #[test]
    fn a_wide_window_steps_cells_down_from_its_top_frequency() {
        // Window top 101 GHz: tol = 505 MHz; 8 channels from 98 to 100.1
        // GHz (spacing 300 MHz, half 150 MHz < tol): top steps to
        // 100.495, cells down to 98.475 → 5 steps, then one removed: 4
        // cells at 98.98 + k·0.505 GHz; 4 < 8 − 1 so no identity.
        let selected = (0..8)
            .map(|k| 98.0e9 + k as f64 * 0.3e9)
            .collect::<Vec<_>>();
        let cells = FrequencyCells::new(&window(selected, 101.0e9, 1.0e6), 0).expect("cells");
        let tol = 101.0e9 * 0.005;
        let top = 101.0e9 - tol; // 100.495 > 100.1, step again:
        let top = top - tol; // 99.99 ≤ 100.1
        assert_eq!(cells.tolerance_hz, Some(tol));
        // Stepping down from 99.99: 99.99 > 98 (1), 99.485 (2), 98.98 (3),
        // 98.475 (4), 97.97 stops → 4 cells, minus one → 3 cells from
        // 98.475.
        let bottom = top - 3.0 * tol;
        let expected = (0..3).map(|k| bottom + k as f64 * tol).collect::<Vec<_>>();
        for (cell, want) in cells.frequencies_hz.iter().zip(&expected) {
            assert!((cell - want).abs() < 1.0, "{cell} vs {want}");
        }
        assert_eq!(cells.frequencies_hz.len(), 3);
        // chanMap: first cell within tol/2 (252.5 MHz), else nearest.
        assert_eq!(cells.cell_of(98.5e9), 0);
        assert_eq!(cells.cell_of(98.9e9), 1);
        assert_eq!(cells.cell_of(100.1e9), 2);
    }

    #[test]
    fn enough_cells_for_the_channels_is_the_identity() {
        // 3 channels 1 GHz apart at 100 GHz: tol 0.5 GHz → cells from
        // 101.5 down past 100 … reaches ≥ 2 cells = channels − 1 → identity.
        let selected = vec![100.0e9, 101.0e9, 102.0e9];
        let cells =
            FrequencyCells::new(&window(selected.clone(), 102.0e9, 1.0e6), 0).expect("cells");
        assert_eq!(cells.frequencies_hz, selected);
        assert_eq!(cells.tolerance_hz, None);
        assert_eq!(cells.cell_of(101.2e9), 1);
    }

    #[test]
    fn lanczos_resample_reproduces_a_smooth_plane_away_from_the_edges() {
        let side = 16;
        let gaussian = |x: f64, y: f64| (-((x - 8.0).powi(2) + (y - 8.0).powi(2)) / 18.0).exp();
        let input = (0..side * side)
            .map(|index| {
                Complex32::new(
                    gaussian((index % side) as f64, (index / side) as f64) as f32,
                    0.0,
                )
            })
            .collect::<Vec<_>>();
        let out = lanczos_resample(&input, side, 4);
        let out_side = side * 4;
        assert_eq!(out.len(), out_side * out_side);
        // Output pixel (j, k) samples input (j/4, k/4): exact on input
        // pixels, and within the Lanczos-3 window's gain droop (its
        // weights sum to 0.994 at a half-pixel offset, as in CASA)
        // between them.
        assert_eq!(f64::from(out[32 * out_side + 32].re), gaussian(8.0, 8.0));
        for (j, k) in [(30, 35), (25, 40), (40, 20)] {
            let want = gaussian(j as f64 / 4.0, k as f64 / 4.0);
            let got = f64::from(out[k * out_side + j].re);
            assert!(
                (got - want).abs() < 1.5e-2 * want,
                "({j}, {k}): {got} vs {want}"
            );
        }
        // Within three input pixels of an edge the window is zero.
        assert_eq!(out[32 * out_side + 11].re, 0.0);
        assert_eq!(out[32 * out_side + (side - 3) * 4].re, 0.0);
    }

    #[test]
    fn weight_support_is_the_cut_radius_plus_one_within_the_rules() {
        let side = 64;
        let centre = side / 2;
        // A Gaussian weight plane: |w| < 2.5 % of peak at radius ≈ 2.72 σ.
        let sigma = 4.0;
        let plane = (0..side * side)
            .map(|index| {
                let dx = (index % side) as f64 - centre as f64;
                let dy = (index / side) as f64 - centre as f64;
                Complex32::new(
                    (-(dx * dx + dy * dy) / (2.0 * sigma * sigma)).exp() as f32,
                    0.0,
                )
            })
            .collect::<Vec<_>>();
        // trial 11: exp(−121/32) = 0.0228 < 0.025 → support 12.
        assert_eq!(weight_plane_support(&plane, side).expect("support"), 12);
        // A narrow plane stops at the minimum of five, plus one.
        let narrow = (0..side * side)
            .map(|index| {
                let dx = (index % side) as f64 - centre as f64;
                let dy = (index / side) as f64 - centre as f64;
                Complex32::new((-(dx * dx + dy * dy) / 0.5).exp() as f32, 0.0)
            })
            .collect::<Vec<_>>();
        assert_eq!(weight_plane_support(&narrow, side).expect("support"), 6);
    }
}
