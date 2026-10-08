// SPDX-License-Identifier: LGPL-3.0-or-later

//! Synthetic major-cycle passes: the normal state a pass forms for a known
//! sky, assembled through `PassNormalState` without visibilities.
//!
//! The sky holds one model-space plane per data term: per Taylor
//! coefficient for a constant or Taylor basis, per output channel for a
//! channel-local basis. With an analytic Gaussian PSF `P` peaked at the image
//! centre and the basis's spectral moments `m_k` (Taylor) or per-channel sum
//! weights `w_c`, a pass over final model `x` forms
//!
//! ```text
//! residual_t = Σ_s m_{t+s} P ⊛ (sky_s − x_s) + noise_t      (Taylor)
//! residual_c = w_c P ⊛ (sky_c − x_c) + noise_c                (channel-local)
//! psf_k      = m_k P,   sumwt_k = m_k
//! ```
//!
//! which is the normal equation `Hᵀ(d − H x)` of a noiseless point-spread
//! instrument, so cleaning a component and reconciling it lowers the
//! residual exactly as a real pass would. Invalid model support predicts
//! nothing. `noise_t` is a deterministic field that breaks exact symmetry
//! ties and gives robust-RMS estimators a nonzero population.

#![allow(
    dead_code,
    reason = "each integration-test crate uses a subset of the shared fixture"
)]

use casa_imaging_model::{CompiledProblem, ModelSupport, ReconstructionBasis};
use casa_imaging_reconstruction::{
    FinalNormalState, MajorCycleCompletion, MajorCycleOwner, MajorCyclePreparation,
    ModelGeneration, ModelLifecycle, PassImages, PassNormalState, WeightingGenerationId,
    runtime_adapter::{CompleteDataNormalState, NormalStoragePlan},
};

/// Traversal counts the synthetic pass reports; any positive pair proves
/// coverage to the major-cycle owner.
pub const SAMPLES: u64 = 4;
pub const BLOCKS: u64 = 1;

/// Full width at half maximum of the default PSF, in pixels.
const PSF_FWHM_PX: f64 = 2.5;

/// Normalised frequency offsets `(ν − ν₀)/ν₀` and weights of the two
/// spectral samples a Taylor basis's moments are summed over.
const TAYLOR_SAMPLES: [(f64, f64); 2] = [(-0.2, 2.0 / 3.0), (0.2, 1.0 / 3.0)];

/// A known sky and the instrument a synthetic pass images it with.
#[derive(Clone, Debug)]
pub struct Scene {
    shape: [usize; 2],
    channel_local: bool,
    sky: Vec<Vec<f64>>,
    weights: Vec<f64>,
    psf_sigma_px: f64,
    noise: f64,
}

impl Scene {
    /// An empty sky for `problem`'s one Stokes-I domain: unit sum weight per
    /// channel, or Taylor moments of two spectral samples either side of the
    /// reference frequency.
    pub fn new(problem: &CompiledProblem) -> Self {
        let shape = problem.geometry().domains()[0].shape().pixels();
        let (planes, channel_local, weights) = match problem.reconstruction().basis() {
            ReconstructionBasis::Constant => (1, false, vec![1.0]),
            ReconstructionBasis::Taylor { terms }
            | ReconstructionBasis::TaylorViaChannelMajor { terms, .. } => {
                let moments = (0..2 * terms - 1)
                    .map(|order| {
                        TAYLOR_SAMPLES
                            .iter()
                            .map(|(offset, weight)| weight * offset.powi(order as i32))
                            .sum()
                    })
                    .collect();
                (terms, false, moments)
            }
            ReconstructionBasis::ChannelLocal { channels } => (channels, true, vec![1.0; channels]),
        };
        Self {
            shape,
            channel_local,
            sky: vec![vec![0.0; shape[0] * shape[1]]; planes],
            weights,
            psf_sigma_px: PSF_FWHM_PX / (8.0 * std::f64::consts::LN_2).sqrt(),
            noise: 0.0,
        }
    }

    /// Add a point component at `pixel` with one amplitude per plane.
    #[must_use]
    pub fn with_point(mut self, pixel: [usize; 2], amplitudes: &[f64]) -> Self {
        assert_eq!(amplitudes.len(), self.sky.len(), "one amplitude per plane");
        let index = pixel[0] * self.shape[1] + pixel[1];
        for (plane, amplitude) in self.sky.iter_mut().zip(amplitudes) {
            plane[index] += amplitude;
        }
        self
    }

    /// Add a circular Gaussian component of standard deviation `sigma_px`
    /// centred on `centre`, with one peak amplitude per plane.
    #[must_use]
    pub fn with_gaussian(mut self, centre: [f64; 2], sigma_px: f64, amplitudes: &[f64]) -> Self {
        assert_eq!(amplitudes.len(), self.sky.len(), "one amplitude per plane");
        for (plane, amplitude) in self.sky.iter_mut().zip(amplitudes) {
            for x in 0..self.shape[0] {
                for y in 0..self.shape[1] {
                    let r2 = (x as f64 - centre[0]).powi(2) + (y as f64 - centre[1]).powi(2);
                    plane[x * self.shape[1] + y] +=
                        amplitude * (-r2 / (2.0 * sigma_px * sigma_px)).exp();
                }
            }
        }
        self
    }

    /// Replace the sum weights: one per channel for a channel-local basis,
    /// the `2T − 1` normal moments for a Taylor basis. A zero channel weight
    /// is a channel no sample reached.
    #[must_use]
    pub fn with_weights(mut self, weights: Vec<f64>) -> Self {
        assert_eq!(weights.len(), self.weights.len(), "one weight per moment");
        self.weights = weights;
        self
    }

    /// Add a deterministic residual field of at most `amplitude` (times the
    /// plane's weight) to every data plane.
    #[must_use]
    pub fn with_noise(mut self, amplitude: f64) -> Self {
        self.noise = amplitude;
        self
    }

    /// Scale the sky and noise by `factor`.
    #[must_use]
    pub fn scaled(mut self, factor: f64) -> Self {
        for value in self.sky.iter_mut().flatten() {
            *value *= factor;
        }
        self.noise *= factor;
        self
    }

    /// Image shape `[width, height]`.
    pub const fn shape(&self) -> [usize; 2] {
        self.shape
    }

    /// The x-major PSF of unit peak at the image centre.
    pub fn unit_psf(&self) -> Vec<f64> {
        let centre = [self.shape[0] / 2, self.shape[1] / 2];
        let mut psf = Vec::with_capacity(self.shape[0] * self.shape[1]);
        for x in 0..self.shape[0] {
            for y in 0..self.shape[1] {
                psf.push(self.beam(x as f64 - centre[0] as f64, y as f64 - centre[1] as f64));
            }
        }
        psf
    }

    /// The images one pass forms with final model `model`; PSF moments and
    /// sum weights come along on an initial pass.
    pub fn pass_images(&self, model: &ModelGeneration, initial: bool) -> PassImages {
        let cells = self.shape[0] * self.shape[1];
        let planes = self.sky.len();
        let convolved = (0..planes)
            .map(|plane| {
                let model = model_plane(model, plane, self.shape);
                let difference = self.sky[plane]
                    .iter()
                    .zip(&model)
                    .map(|(sky, model)| sky - model)
                    .collect::<Vec<_>>();
                self.convolve(&difference)
            })
            .collect::<Vec<_>>();
        let mut residual = Vec::with_capacity(planes * cells);
        for term in 0..planes {
            let noise_weight = if self.channel_local {
                self.weights[term]
            } else {
                self.weights[0]
            };
            let signal = |cell: usize| -> f64 {
                if self.channel_local {
                    self.weights[term] * convolved[term][cell]
                } else {
                    (0..planes)
                        .map(|source| self.weights[term + source] * convolved[source][cell])
                        .sum()
                }
            };
            residual.extend((0..cells).map(|cell| {
                (signal(cell) + self.noise * noise_weight * hash_unit(term, cell)) as f32
            }));
        }
        let (psf, sum_weights) = if initial {
            let unit = self.unit_psf();
            let psf = self
                .weights
                .iter()
                .flat_map(|weight| unit.iter().map(move |value| (weight * value) as f32))
                .collect();
            (Some(psf), self.weights.clone())
        } else {
            (None, Vec::new())
        };
        PassImages {
            domain: 0,
            shape: self.shape,
            channels: 0..if self.channel_local { planes } else { 1 },
            polarizations: 1,
            residual,
            psf,
            sum_weights,
        }
    }

    /// The complete normal state of an initial pass over `preparation`'s
    /// final model, under a fresh weighting generation.
    pub fn initial(
        &self,
        problem: &CompiledProblem,
        preparation: &MajorCyclePreparation,
    ) -> CompleteDataNormalState {
        self.initial_with(
            problem,
            preparation,
            self.resident_storage(),
            WeightingGenerationId::next(),
        )
    }

    /// An initial pass written to `storage` and tagged with `weighting`.
    pub fn initial_with(
        &self,
        problem: &CompiledProblem,
        preparation: &MajorCyclePreparation,
        storage: NormalStoragePlan,
        weighting: WeightingGenerationId,
    ) -> CompleteDataNormalState {
        let mut state = PassNormalState::initial(
            problem,
            weighting,
            preparation.final_model_generation(),
            storage,
        )
        .expect("initial synthetic pass state");
        state
            .append(self.pass_images(preparation.final_model(), true))
            .expect("append synthetic initial pass images");
        state
            .finish(SAMPLES, BLOCKS)
            .expect("complete synthetic initial pass")
    }

    /// A residual refresh of `previous` for `preparation`'s final model,
    /// under `previous`'s weighting generation.
    pub fn refresh(
        &self,
        problem: &CompiledProblem,
        previous: FinalNormalState,
        preparation: &MajorCyclePreparation,
    ) -> CompleteDataNormalState {
        let mut state = PassNormalState::refresh(
            problem,
            previous,
            preparation.final_model_generation(),
            self.resident_storage(),
        )
        .expect("refresh synthetic pass state");
        state
            .append(self.pass_images(preparation.final_model(), false))
            .expect("append synthetic residual pass images");
        state
            .finish(SAMPLES, BLOCKS)
            .expect("complete synthetic residual pass")
    }

    /// Run an initial pass and reconcile it with `preparation`.
    pub fn reconcile(
        &self,
        problem: &CompiledProblem,
        lifecycle: &mut ModelLifecycle,
        preparation: MajorCyclePreparation,
    ) -> MajorCycleCompletion {
        let complete = self.initial(problem, &preparation);
        MajorCycleOwner::from_complete_data(complete, preparation)
            .expect("major-cycle owner of the synthetic pass")
            .reconcile(lifecycle)
            .expect("reconcile the synthetic pass")
    }

    /// Refresh `previous` and reconcile it with `preparation`.
    pub fn reconcile_refresh(
        &self,
        problem: &CompiledProblem,
        lifecycle: &mut ModelLifecycle,
        previous: FinalNormalState,
        preparation: MajorCyclePreparation,
    ) -> MajorCycleCompletion {
        let complete = self.refresh(problem, previous, &preparation);
        MajorCycleOwner::from_complete_data(complete, preparation)
            .expect("major-cycle owner of the synthetic refresh")
            .reconcile(lifecycle)
            .expect("reconcile the synthetic refresh")
    }

    fn resident_storage(&self) -> NormalStoragePlan {
        let channels = if self.channel_local {
            self.sky.len()
        } else {
            1
        };
        NormalStoragePlan::resident(channels).expect("resident synthetic normal storage")
    }

    fn beam(&self, dx: f64, dy: f64) -> f64 {
        (-(dx * dx + dy * dy) / (2.0 * self.psf_sigma_px * self.psf_sigma_px)).exp()
    }

    /// `P ⊛ image` over the whole plane; the analytic PSF has no truncation.
    fn convolve(&self, image: &[f64]) -> Vec<f64> {
        let [width, height] = self.shape;
        let mut out = vec![0.0; width * height];
        for (source, value) in image.iter().enumerate() {
            if *value == 0.0 {
                continue;
            }
            let [sx, sy] = [source / height, source % height];
            for x in 0..width {
                for y in 0..height {
                    out[x * height + y] +=
                        value * self.beam(x as f64 - sx as f64, y as f64 - sy as f64);
                }
            }
        }
        out
    }
}

/// One x-major model plane; invalid support predicts nothing.
fn model_plane(model: &ModelGeneration, plane: usize, shape: [usize; 2]) -> Vec<f64> {
    let samples = model
        .read_plane(0, plane, 0)
        .expect("read synthetic model plane");
    let [width, height] = shape;
    let mut values = vec![0.0; width * height];
    for x in 0..width {
        for y in 0..height {
            let sample = samples[y * width + x];
            if sample.support() == ModelSupport::Valid {
                values[x * height + y] = sample.value().value();
            }
        }
    }
    values
}

/// A deterministic value in `[-1, 1)` per plane and cell (SplitMix64).
fn hash_unit(plane: usize, cell: usize) -> f64 {
    let mut z = (((plane as u64) << 32) ^ cell as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^= z >> 31;
    (z >> 11) as f64 / (1_u64 << 52) as f64 - 1.0
}
