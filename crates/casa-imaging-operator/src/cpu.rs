// SPDX-License-Identifier: LGPL-3.0-or-later
//! The CPU gridding backend: support-generic kernels over both tap layouts
//! with a specialised seven-tap separable path.

pub(crate) mod kernel;

use num_complex::{Complex, Complex32, Complex64};

use crate::accumulator::{AccumulatorLayout, GridAccumulator, GridPrecision, GridScalar, Mode};
use crate::backend::{GridBackend, PreparedModelGrids, Work};
use crate::convolution::{CellHold, ConvolutionFunctionSet, KernelNormalisation, TapLayout};
use crate::error::OperatorError;
use crate::geometry::CellLocation;
use crate::native::{NativeModel, NativeTarget, interpolate_terms, residual_values};
use crate::sample::{Placement, SampleBlock};

/// The CPU implementation of [`GridBackend`].
///
/// Grids are addressed through the shared accumulator layout; each sample
/// is located once with [`GridGeometry::locate`](crate::GridGeometry::locate)
/// and spread or gathered through the Mueller table its `w` sign selects.
/// Under [`KernelNormalisation::KernelSum`] a prediction sums the numerator
/// and the kernel norm over every routed Mueller plane and divides once, as
/// CASA's `AWVisResampler` does, and a zero norm predicts zero; the other
/// rules predict the gathered sum.
#[derive(Debug, Default)]
pub struct CpuBackend {
    powers: Vec<f64>,
    prediction: Vec<Complex64>,
    norms: Vec<Complex64>,
    /// One grid polarization's gathers by Mueller plane, each summed over
    /// the terms, for the visibility polarizations that share them.
    gathered: Vec<(u8, Complex64)>,
    residual: Vec<Complex32>,
    /// A block's predictions, or its residual values at native channels.
    batch: Vec<Complex32>,
    hold: CellHold,
}

impl CpuBackend {
    /// A backend with empty scratch buffers.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn grid<T: GridScalar>(
        &mut self,
        block: &SampleBlock<'_>,
        cf: &dyn ConvolutionFunctionSet,
        mode: Mode,
        acc: &mut GridAccumulator,
    ) -> Result<(), OperatorError> {
        let (layout, cells, mut sumwt) = acc.parts_mut::<T>();
        let terms = layout
            .term_range(mode)
            .unwrap_or_else(|| panic!("accumulator holds no {mode:?} terms"));
        let rule = cf.normalisation();
        let Self { powers, hold, .. } = self;
        for (index, placement) in block.placements.iter().enumerate() {
            let taps = match mode {
                Mode::Weight => cf
                    .weight_taps(placement.cf, hold)
                    .ok_or(OperatorError::WeightKernelUnavailable { key: placement.cf })?,
                Mode::Psf => cf.psf_taps(placement.cf, hold),
                Mode::Data => cf.taps(placement.cf, hold),
            };
            let (u, v) = match mode {
                Mode::Weight => (0.0, 0.0),
                Mode::Data | Mode::Psf => (placement.u, placement.v),
            };
            let location = layout.geometry().locate(u, v, taps.oversampling());
            // CASA tests `onGrid` with the support of the cell each mode
            // reads (`AWVisResampler::DataToGrid`), so a PSF gridded with a
            // wider weight cell drops rows the data gridding keeps.
            if !layout.geometry().fits(location, taps.half_support()) {
                continue;
            }
            spectral_powers(powers, placement.spectral, terms.len());
            spread_sample(
                layout,
                cells,
                sumwt.as_deref_mut(),
                cf,
                rule,
                placement,
                &taps,
                location,
                mode,
                &terms,
                powers,
                block.values_of(index),
                block.weights_of(index),
            );
        }
        Ok(())
    }

    fn predict<T: GridScalar>(
        &mut self,
        block: &SampleBlock<'_>,
        cf: &dyn ConvolutionFunctionSet,
        model: &PreparedModelGrids,
        out: &mut [Complex32],
    ) {
        let npol = block.npol;
        assert_eq!(out.len(), block.len() * npol, "prediction output length");
        for (index, placement) in block.placements.iter().enumerate() {
            self.predict_sample::<T>(cf, model, placement, npol);
            let phasor = Complex64::from_polar(1.0, -placement.phase);
            for (vpol, target) in out[index * npol..(index + 1) * npol].iter_mut().enumerate() {
                *target = narrow(self.prediction[vpol] * phasor);
            }
        }
    }

    fn residual<T: GridScalar>(
        &mut self,
        block: &SampleBlock<'_>,
        cf: &dyn ConvolutionFunctionSet,
        model: &PreparedModelGrids,
        acc: &mut GridAccumulator,
        mut residual_out: Option<&mut [Complex32]>,
    ) {
        let npol = block.npol;
        if let Some(out) = residual_out.as_deref() {
            assert_eq!(out.len(), block.len() * npol, "residual output length");
        }
        let rule = cf.normalisation();
        let (layout, cells, mut sumwt) = acc.parts_mut::<T>();
        let terms = layout
            .term_range(Mode::Data)
            .expect("residual gridding needs the data terms");
        self.residual.resize(npol, Complex32::default());
        for (index, placement) in block.placements.iter().enumerate() {
            self.predict_sample::<T>(cf, model, placement, npol);
            let values = block.values_of(index);
            let weights = block.weights_of(index);
            for vpol in 0..npol {
                let weight = f64::from(weights[vpol]);
                let predicted = self.prediction[vpol] * weight;
                self.residual[vpol] = values[vpol] - narrow(predicted);
            }
            if let Some(out) = residual_out.as_deref_mut() {
                let phasor = Complex64::from_polar(1.0, -placement.phase);
                for vpol in 0..npol {
                    let weight = f64::from(weights[vpol]);
                    out[index * npol + vpol] = if weight == 0.0 {
                        Complex32::default()
                    } else {
                        narrow(widen(self.residual[vpol]) * phasor / weight)
                    };
                }
            }
            let Self {
                powers,
                residual,
                hold,
                ..
            } = self;
            let taps = cf.taps(placement.cf, hold);
            let location = layout
                .geometry()
                .locate(placement.u, placement.v, taps.oversampling());
            spectral_powers(powers, placement.spectral, terms.len());
            spread_sample(
                layout,
                cells,
                sumwt.as_deref_mut(),
                cf,
                rule,
                placement,
                &taps,
                location,
                Mode::Data,
                &terms,
                powers,
                residual,
                weights,
            );
        }
    }

    /// Fill `self.prediction` with the raw prediction `P` per visibility
    /// polarization (no phase-centre phasor) for one placement:
    /// `Σ_t s^t Σ_{gpol,m} Σ conj(tap'_m) · model[t][gpol]`, divided once by
    /// `Σ_{gpol,m} norm_m` under [`KernelNormalisation::KernelSum`]. The
    /// gather of a grid polarization through a Mueller plane depends on
    /// neither visibility polarization, so it is made once and added to
    /// each that the plane routes (both correlations of Stokes I).
    fn predict_sample<T: GridScalar>(
        &mut self,
        cf: &dyn ConvolutionFunctionSet,
        model: &PreparedModelGrids,
        placement: &Placement,
        npol: usize,
    ) {
        let layout = model.layout();
        let cells = T::cells(model.storage());
        let terms = layout
            .term_range(Mode::Data)
            .expect("model grids hold the data terms");
        let Self {
            powers,
            prediction,
            norms,
            gathered,
            hold,
            ..
        } = self;
        let taps = cf.prediction_taps(placement.cf, hold);
        let location = layout
            .geometry()
            .locate(placement.u, placement.v, taps.oversampling());
        if !layout.geometry().fits(location, taps.half_support()) {
            // `GridToData` skips a row whose prediction cell overruns the
            // grid; its model visibility stays zero.
            prediction.clear();
            prediction.resize(npol, Complex64::default());
            return;
        }
        let w_positive = placement.prediction_w_positive;
        let table = cf.mueller().table(w_positive, true);
        let plane = layout.planes().local(placement.plane);
        let divide = cf.normalisation() == KernelNormalisation::KernelSum;
        spectral_powers(powers, placement.spectral, terms.len());
        prediction.clear();
        prediction.resize(npol, Complex64::default());
        norms.clear();
        norms.resize(npol, Complex64::default());
        for (gpol, row) in table.iter().enumerate() {
            gathered.clear();
            for (vpol, mueller_plane) in row.iter().enumerate() {
                let Some(mueller) = *mueller_plane else {
                    continue;
                };
                if divide {
                    // `accumulateFromGrid.inc` conjugates the prediction tap
                    // for w ≤ 0 and sums that same tap into its norm: the
                    // forward norm carries the opposite conjugation from
                    // the adjoint's (`w_positive`), as the gather does.
                    norms[vpol] += kernel::norm(&taps, location, mueller, !w_positive);
                }
                let sum = if let Some((_, sum)) = gathered.iter().find(|(m, _)| *m == mueller) {
                    *sum
                } else {
                    let mut sum = Complex64::default();
                    for (power, term) in powers.iter().zip(terms.clone()) {
                        let offset = layout.block_offset(plane, gpol, term);
                        let grid = &cells[offset..offset + layout.block_cells()];
                        sum += kernel::gather::<T>(
                            grid,
                            layout.tile(),
                            location,
                            &taps,
                            mueller,
                            w_positive,
                            placement.gradient,
                        ) * *power;
                    }
                    gathered.push((mueller, sum));
                    sum
                };
                prediction[vpol] += sum;
            }
        }
        if divide {
            for (prediction, norm) in prediction.iter_mut().zip(norms.iter()) {
                *prediction = if *norm == Complex64::default() {
                    Complex64::default()
                } else {
                    *prediction / norm
                };
            }
        }
    }
}

impl GridBackend for CpuBackend {
    fn apply(
        &mut self,
        block: &SampleBlock<'_>,
        cf: &dyn ConvolutionFunctionSet,
        work: Work<'_>,
    ) -> Result<(), OperatorError> {
        assert_eq!(
            block.npol,
            cf.mueller().visibility_pols(),
            "block polarizations must match the kernel set routing"
        );
        let result = match work {
            Work::Grid { mode, acc } => match acc.precision() {
                GridPrecision::F32 => self.grid::<f32>(block, cf, mode, acc),
                GridPrecision::F64 => self.grid::<f64>(block, cf, mode, acc),
            },
            Work::Predict { model, out } => {
                match model.precision() {
                    GridPrecision::F32 => self.predict::<f32>(block, cf, model, out),
                    GridPrecision::F64 => self.predict::<f64>(block, cf, model, out),
                }
                Ok(())
            }
            Work::ResidualGrid {
                model,
                acc,
                residual_out,
            } => {
                assert_eq!(
                    model.precision(),
                    acc.precision(),
                    "model grids and accumulator must share a precision"
                );
                match acc.precision() {
                    GridPrecision::F32 => {
                        self.residual::<f32>(block, cf, model, acc, residual_out);
                    }
                    GridPrecision::F64 => {
                        self.residual::<f64>(block, cf, model, acc, residual_out);
                    }
                }
                Ok(())
            }
            Work::PredictNative {
                grids,
                terms,
                model,
                add,
            } => {
                let NativeTarget::Host { cells, first } = model else {
                    panic!("the CPU backend forms the model in host cells");
                };
                let mut predicted = std::mem::take(&mut self.batch);
                predicted.clear();
                predicted.resize(block.len() * block.npol, Complex32::default());
                match grids.precision() {
                    GridPrecision::F32 => self.predict::<f32>(block, cf, grids, &mut predicted),
                    GridPrecision::F64 => self.predict::<f64>(block, cf, grids, &mut predicted),
                }
                interpolate_terms(&predicted, block.npol, terms, cells, first, add);
                self.batch = predicted;
                Ok(())
            }
            Work::NativeResidualGrid { model, acc } => self.native_residual(block, cf, model, acc),
        };
        self.hold.clear();
        result
    }
}

impl CpuBackend {
    /// [`Work::NativeResidualGrid`]: the block's values less the model at
    /// their native endpoints, gridded as data.
    fn native_residual(
        &mut self,
        block: &SampleBlock<'_>,
        cf: &dyn ConvolutionFunctionSet,
        model: NativeModel<'_>,
        acc: &mut GridAccumulator,
    ) -> Result<(), OperatorError> {
        let NativeModel::Host { cells, first } = model else {
            panic!("the CPU backend reads the model from host cells");
        };
        assert_eq!(
            block.sources.len(),
            block.len(),
            "a native residual block holds every placement's endpoints"
        );
        let npol = block.npol;
        let mut values = std::mem::take(&mut self.batch);
        values.clear();
        values.resize(block.len() * npol, Complex32::default());
        for (index, (source, out)) in block
            .sources
            .iter()
            .zip(values.chunks_exact_mut(npol))
            .enumerate()
        {
            residual_values(block.values_of(index), source, cells, first, out);
        }
        let residual = SampleBlock {
            values: &values,
            ..*block
        };
        let result = match acc.precision() {
            GridPrecision::F32 => self.grid::<f32>(&residual, cf, Mode::Data, acc),
            GridPrecision::F64 => self.grid::<f64>(&residual, cf, Mode::Data, acc),
        };
        self.batch = values;
        result
    }
}

/// Spread one placement's values through every routed (grid pol,
/// visibility pol) pair and term, accumulating `sumwt`, when given, by the
/// set's rule: `W · s^t` times 1, `Re N` or `|N|`.
#[allow(clippy::too_many_arguments)]
fn spread_sample<T: GridScalar>(
    layout: &AccumulatorLayout,
    cells: &mut [Complex<T>],
    mut sumwt: Option<&mut [f64]>,
    cf: &dyn ConvolutionFunctionSet,
    rule: KernelNormalisation,
    placement: &Placement,
    taps: &TapLayout<'_>,
    location: CellLocation,
    mode: Mode,
    terms: &std::ops::Range<usize>,
    powers: &[f64],
    values: &[Complex32],
    weights: &[f32],
) {
    let w_positive = placement.w > 0.0;
    let table = cf.mueller().table(w_positive, false);
    let plane = layout.planes().local(placement.plane);
    let block_cells = layout.block_cells();
    for (gpol, row) in table.iter().enumerate() {
        for (vpol, mueller_plane) in row.iter().enumerate() {
            let Some(mueller) = *mueller_plane else {
                continue;
            };
            let weight = weights[vpol];
            if weight == 0.0 {
                continue;
            }
            let base = match mode {
                Mode::Data => values[vpol],
                Mode::Psf | Mode::Weight => Complex32::new(weight, 0.0),
            };
            let norm = match (&sumwt, rule) {
                (None, _) | (_, KernelNormalisation::UnitSum) => 1.0,
                (_, KernelNormalisation::RealSum) => {
                    kernel::norm(taps, location, mueller, w_positive).re
                }
                (_, KernelNormalisation::KernelSum) => {
                    kernel::norm(taps, location, mueller, w_positive).norm()
                }
            };
            for (power, term) in powers.iter().zip(terms.clone()) {
                let value = Complex::new(
                    T::from_f64(f64::from(base.re) * power),
                    T::from_f64(f64::from(base.im) * power),
                );
                let offset = layout.block_offset(plane, gpol, term);
                kernel::spread::<T>(
                    &mut cells[offset..offset + block_cells],
                    layout.tile(),
                    location,
                    taps,
                    mueller,
                    w_positive,
                    placement.gradient,
                    value,
                );
                if let Some(sumwt) = sumwt.as_deref_mut() {
                    sumwt[layout.block_index(plane, gpol, term)] +=
                        f64::from(weight) * power * norm;
                }
            }
        }
    }
}

/// `powers[t] = spectral^t` for `t < count`.
fn spectral_powers(powers: &mut Vec<f64>, spectral: f32, count: usize) {
    powers.clear();
    let spectral = f64::from(spectral);
    let mut power = 1.0;
    for _ in 0..count {
        powers.push(power);
        power *= spectral;
    }
}

fn narrow(value: Complex64) -> Complex32 {
    Complex32::new(value.re as f32, value.im as f32)
}

fn widen(value: Complex32) -> Complex64 {
    Complex64::new(f64::from(value.re), f64::from(value.im))
}
