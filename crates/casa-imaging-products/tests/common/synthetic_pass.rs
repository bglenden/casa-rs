// SPDX-License-Identifier: LGPL-3.0-or-later

//! Synthetic major cycles: the normal state a pass forms for a known sky,
//! assembled through `MajorCycle` from explicit planes.
//!
//! Product tests exercise product generation, not gridding, so the residual,
//! PSF and `sumwt` planes are chosen here instead of gridded. Each image
//! domain holds one sky plane per model coefficient (per Taylor term for a
//! constant or Taylor basis, per output channel for a channel-local basis)
//! and an analytic elliptical Gaussian PSF `P` of unit peak on the domain's
//! centre pixel. With the basis's spectral moments `m_k` or per-channel sum
//! weights `w_c`, a pass over final model `x` forms
//!
//! ```text
//! residual_t = Σ_s m_{t+s} P ⊛ (sky_s − x_s)      (constant, Taylor)
//! residual_c = w_c P ⊛ (sky_c − x_c)              (channel-local)
//! psf_k      = m_k P,   sumwt_k = m_k   (or w_c)
//! ```
//!
//! the normal equation `Hᵀ(d − H x)` of a point-spread instrument. The
//! weights are those of two spectral samples of weight one and two at 1.05
//! and 1.15 GHz: a constant basis sums them (`sumwt = 3`), a Taylor basis
//! takes their moments about the image reference frequency, and a
//! channel-local basis gives output channel `c` weight `1 + c`. Every domain
//! sees every sample unless a test replaces its weights.

use casa_imaging_model::{
    CompiledProblem, ModelDeltaTerm, ModelSupport, ReconstructionBasis, SpectralWcs,
};
use casa_imaging_reconstruction::{
    FinalNormalState, MajorCycle, MajorCycleCompletion, ModelGeneration, ModelLifecycle,
    ModelStoragePlan, PassImages, PreparedFinalModel, runtime_adapter::NormalStoragePlan,
};

/// Traversal counts every synthetic pass reports; any positive pair proves
/// coverage to the major cycle.
pub const SAMPLES: u64 = 2;
pub const BLOCKS: u64 = 1;

/// `(frequency in Hz, weight)` of the spectral samples behind the weights.
const SPECTRAL_SAMPLES: [(f64, f64); 2] = [(1.05e9, 1.0), (1.15e9, 2.0)];

/// Full widths at half maximum `[along x, along y]` of the PSF, in pixels:
/// elliptical, with its major axis along y, and wide enough that an 8 × 8
/// plane holds more than a dozen samples above the beam-fit cutoff.
const PSF_FWHM_PX: [f64; 2] = [3.0, 4.0];

/// One image domain of a [`Scene`].
#[derive(Clone, Debug)]
struct DomainScene {
    shape: [usize; 2],
    sky: Vec<Vec<f64>>,
    weights: Vec<f64>,
}

/// A known sky and the instrument a synthetic pass images it with.
#[derive(Clone, Debug)]
pub struct Scene {
    channel_local: bool,
    channels: usize,
    planes: usize,
    domains: Vec<DomainScene>,
}

impl Scene {
    /// An empty sky over every Stokes-I image domain of `problem`.
    pub fn new(problem: &CompiledProblem) -> Self {
        let reference_hz = match problem.geometry().spectral().wcs() {
            SpectralWcs::Linear {
                reference_frequency_hz,
                ..
            } => *reference_frequency_hz,
            SpectralWcs::Tabular { .. } => panic!("fixture problems have a linear spectral axis"),
        };
        let moment = |order: usize| -> f64 {
            SPECTRAL_SAMPLES
                .iter()
                .map(|(frequency, weight)| {
                    weight * ((frequency - reference_hz) / reference_hz).powi(order as i32)
                })
                .sum()
        };
        let (planes, channel_local, weights) = match problem.reconstruction().basis() {
            ReconstructionBasis::Constant => (1, false, vec![moment(0)]),
            ReconstructionBasis::Taylor { terms } => {
                (terms, false, (0..2 * terms - 1).map(moment).collect())
            }
            ReconstructionBasis::ChannelLocal { channels } => (
                channels,
                true,
                (0..channels).map(|channel| 1.0 + channel as f64).collect(),
            ),
        };
        let domains = problem
            .geometry()
            .domains()
            .iter()
            .map(|domain| {
                let shape = domain.shape().pixels();
                DomainScene {
                    shape,
                    sky: vec![vec![0.0; shape[0] * shape[1]]; planes],
                    weights: weights.clone(),
                }
            })
            .collect();
        Self {
            channel_local,
            channels: problem.geometry().spectral().output_channels(),
            planes,
            domains,
        }
    }

    /// Replace `domain`'s sum weights: one per normal moment for a constant
    /// or Taylor basis, one per output channel for a channel-local basis. A
    /// zero weight is a plane no sample reached with weight.
    #[must_use]
    pub fn with_weights(mut self, domain: usize, weights: Vec<f64>) -> Self {
        let scene = &mut self.domains[domain];
        assert_eq!(weights.len(), scene.weights.len(), "one weight per moment");
        scene.weights = weights;
        self
    }

    /// Add a point component at `pixel = [x, y]` of `domain`, with one
    /// amplitude per model plane.
    #[must_use]
    pub fn with_point(mut self, domain: usize, pixel: [usize; 2], amplitudes: &[f64]) -> Self {
        assert_eq!(amplitudes.len(), self.planes, "one amplitude per plane");
        let scene = &mut self.domains[domain];
        let index = pixel[0] * scene.shape[1] + pixel[1];
        for (plane, amplitude) in scene.sky.iter_mut().zip(amplitudes) {
            plane[index] += amplitude;
        }
        self
    }

    /// The number of model planes: Taylor terms or output channels.
    pub const fn planes(&self) -> usize {
        self.planes
    }

    /// The sum weights a pass reports on `domain`: one per normal moment for
    /// a constant or Taylor basis, one per output channel for a
    /// channel-local basis.
    pub fn weights(&self, domain: usize) -> &[f64] {
        &self.domains[domain].weights
    }

    /// The x-major PSF of unit peak at `domain`'s centre pixel.
    pub fn unit_psf(&self, domain: usize) -> Vec<f64> {
        let [width, height] = self.domains[domain].shape;
        let centre = [width / 2, height / 2];
        let mut psf = Vec::with_capacity(width * height);
        for x in 0..width {
            for y in 0..height {
                psf.push(beam(
                    x as f64 - centre[0] as f64,
                    y as f64 - centre[1] as f64,
                ));
            }
        }
        psf
    }

    /// The images one pass forms on `domain` with final model `model`; PSF
    /// moments and sum weights come along on an initial pass.
    pub fn pass_images(&self, domain: usize, model: &ModelGeneration, initial: bool) -> PassImages {
        let scene = &self.domains[domain];
        let weights = &scene.weights;
        let cells = scene.shape[0] * scene.shape[1];
        let convolved = (0..self.planes)
            .map(|plane| {
                let model = model_plane(model, domain, plane, scene.shape);
                let difference = scene.sky[plane]
                    .iter()
                    .zip(&model)
                    .map(|(sky, model)| sky - model)
                    .collect::<Vec<_>>();
                convolve(&difference, scene.shape)
            })
            .collect::<Vec<_>>();
        let mut residual = Vec::with_capacity(self.planes * cells);
        for term in 0..self.planes {
            residual.extend((0..cells).map(|cell| {
                let value: f64 = if self.channel_local {
                    weights[term] * convolved[term][cell]
                } else {
                    (0..self.planes)
                        .map(|source| weights[term + source] * convolved[source][cell])
                        .sum()
                };
                value as f32
            }));
        }
        let (psf, sum_weights) = if initial {
            let unit = self.unit_psf(domain);
            let psf = weights
                .iter()
                .flat_map(|weight| unit.iter().map(move |value| (weight * value) as f32))
                .collect();
            (Some(psf), weights.clone())
        } else {
            (None, Vec::new())
        };
        PassImages {
            domain,
            shape: scene.shape,
            channels: 0..self.channels,
            polarizations: 1,
            residual,
            psf,
            published_sum_weights: sum_weights.clone(),
            sum_weights,
            weight: None,
        }
    }

    /// Run an initial major cycle with final model `model`: one pass over
    /// every domain, finished with the synthetic traversal counts.
    pub fn reconcile_initial(
        &self,
        problem: &CompiledProblem,
        model: PreparedFinalModel,
    ) -> MajorCycleCompletion {
        let mut cycle = MajorCycle::initial(problem, model, self.storage())
            .expect("initial synthetic major cycle");
        {
            let (model, mut state) = cycle.parts();
            for domain in 0..self.domains.len() {
                state
                    .append(self.pass_images(domain, model, true))
                    .expect("append synthetic initial pass images");
            }
        }
        cycle
            .finish(SAMPLES, BLOCKS)
            .expect("finish the synthetic initial major cycle")
    }

    /// Refresh `previous` with the residual of final model `model`: one
    /// residual pass over every domain, finished with the synthetic
    /// traversal counts.
    pub fn reconcile_refresh(
        &self,
        problem: &CompiledProblem,
        previous: FinalNormalState,
        model: PreparedFinalModel,
    ) -> MajorCycleCompletion {
        let mut cycle = MajorCycle::refresh(problem, previous, model, self.storage())
            .expect("refresh synthetic major cycle");
        {
            let (model, mut state) = cycle.parts();
            for domain in 0..self.domains.len() {
                state
                    .append(self.pass_images(domain, model, false))
                    .expect("append synthetic residual pass images");
            }
        }
        cycle
            .finish(SAMPLES, BLOCKS)
            .expect("finish the synthetic refresh major cycle")
    }

    fn storage(&self) -> NormalStoragePlan {
        let window = if self.channel_local { self.channels } else { 1 };
        NormalStoragePlan::resident(window).expect("resident synthetic normal storage")
    }
}

/// Two major cycles of one run: an initial pass over the empty model and,
/// when `delta` has terms, a residual refresh after applying them. Without
/// terms the initial major cycle is the result.
pub fn two_cycle_round(
    problem: &CompiledProblem,
    scene: &Scene,
    delta: Vec<ModelDeltaTerm>,
) -> MajorCycleCompletion {
    let lifecycle = ModelLifecycle::new(problem, model_storage());
    let empty = lifecycle.initial_empty().expect("empty model");
    let model = lifecycle
        .prepare_final_model(empty, [])
        .expect("initial final model");
    let initial = scene.reconcile_initial(problem, model);
    if delta.is_empty() {
        return initial;
    }
    let (normal, model) = initial.into_parts();
    let model = lifecycle
        .prepare_final_model(model, delta)
        .expect("final model after the nonzero delta");
    scene.reconcile_refresh(problem, normal, model)
}

/// One more major cycle continuing `prior`, with the model unchanged.
pub fn continue_round(
    problem: &CompiledProblem,
    scene: &Scene,
    prior: MajorCycleCompletion,
) -> MajorCycleCompletion {
    let lifecycle = ModelLifecycle::new(problem, model_storage());
    let (normal, model) = prior.into_parts();
    let model = lifecycle
        .prepare_final_model(model, [])
        .expect("unchanged final model");
    scene.reconcile_refresh(problem, normal, model)
}

fn model_storage() -> ModelStoragePlan {
    ModelStoragePlan::resident(usize::MAX).expect("positive model window")
}

/// The unit-peak elliptical Gaussian PSF at offset `(dx, dy)` pixels.
fn beam(dx: f64, dy: f64) -> f64 {
    let sigma = PSF_FWHM_PX.map(|fwhm| fwhm / (8.0 * std::f64::consts::LN_2).sqrt());
    (-0.5 * ((dx / sigma[0]).powi(2) + (dy / sigma[1]).powi(2))).exp()
}

/// `P ⊛ image` over a whole x-major plane; the analytic PSF has no
/// truncation.
fn convolve(image: &[f64], shape: [usize; 2]) -> Vec<f64> {
    let [width, height] = shape;
    let mut out = vec![0.0; width * height];
    for (source, value) in image.iter().enumerate() {
        if *value == 0.0 {
            continue;
        }
        let [sx, sy] = [source / height, source % height];
        for x in 0..width {
            for y in 0..height {
                out[x * height + y] += value * beam(x as f64 - sx as f64, y as f64 - sy as f64);
            }
        }
    }
    out
}

/// One x-major model plane; invalid support predicts nothing.
fn model_plane(
    model: &ModelGeneration,
    domain: usize,
    plane: usize,
    shape: [usize; 2],
) -> Vec<f64> {
    let samples = model
        .read_plane(domain, plane, 0)
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
