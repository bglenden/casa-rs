// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's image-plane maximum-entropy step.
//!
//! `SDAlgorithmMEM::takeOneStep` solves for a dense incremental model.
//! The clean mask limits the entropy calculation, but CASA floors the model
//! on the entire plane. It starts each step afresh from `1 / mask_sum`.

use crate::{
    CycleControls, Delta, Error, LinearRefresh, MinorCycleView, PlaneControl, PlaneOutcome,
    PlaneShape, PlaneStatistics, PlaneStop, casacore_max_abs,
};

/// The result of one CASA MEM minor-cycle step.
#[derive(Debug)]
pub struct MemStep {
    /// The incremental model, in x-major plane order.
    pub model: Vec<f32>,
    /// The image-plane residual left by the step.
    pub residual: Vec<f32>,
    /// Iterations charged by the solver.
    pub iterations: usize,
    /// Final entropy multiplier, saved by CASA in model metadata.
    pub alpha: f32,
    /// Whole-plane residual refreshes performed.
    pub refreshes: usize,
}

/// Heap bytes for one MEM step, excluding its borrowed view. The returned
/// model and residual are included. The four f32 planes are the model,
/// residual, step and convolution intermediate; the f64 plane is FFT input
/// and output scratch.
#[must_use]
pub fn mem_step_bytes(shape: PlaneShape) -> u64 {
    LinearRefresh::bytes(shape).saturating_add(
        (shape.len() as u64).saturating_mul((4 * size_of::<f32>() + size_of::<f64>()) as u64),
    )
}

#[derive(Default)]
struct Gram {
    hh: f64,
    hc: f64,
    cc: f64,
    ff: f64,
    cj: f64,
    jj: f64,
}

struct Mem<'a> {
    view: &'a MinorCycleView<'a>,
    model: Vec<f32>,
    residual: Vec<f32>,
    step: Vec<f32>,
    intermediate: Vec<f32>,
    scratch: Vec<f64>,
    convolution: LinearRefresh,
    default: f32,
    psf_sum: f32,
    alpha: f32,
    q: f32,
    target: f32,
    chisq: f32,
    norm: f32,
    refreshes: usize,
}

impl<'a> Mem<'a> {
    fn new(view: &'a MinorCycleView<'a>, threshold: f32) -> Result<Self, Error> {
        let shape = view.shape;
        let count = view.support.count() as f32;
        let default = 1.0 / count;
        let psf_sum = view.psf[0]
            .iter()
            .map(|&value| f64::from(value as f32))
            .sum::<f64>() as f32;
        if count == 0.0 || !psf_sum.is_finite() || psf_sum == 0.0 {
            return Err(Error::NonFinite);
        }
        Ok(Self {
            view,
            model: view
                .support
                .as_slice()
                .iter()
                .map(|&keep| if keep { default } else { 0.0 })
                .collect(),
            residual: vec![0.0; shape.len()],
            step: vec![0.0; shape.len()],
            intermediate: vec![0.0; shape.len()],
            scratch: vec![0.0; shape.len()],
            convolution: LinearRefresh::new(
                &view.psf[0],
                shape,
                shape.index(shape.nx / 2, shape.ny / 2),
            )?,
            default,
            psf_sum,
            alpha: 0.0,
            q: 10.0,
            target: threshold * threshold * count,
            chisq: 0.0,
            norm: 0.0,
            refreshes: 0,
        })
    }

    fn gram(&self) -> Gram {
        let mut g = Gram::default();
        let log_default = self.default.ln();
        let curvature = 2.0 * self.alpha * self.q;
        for (i, (&model, &residual)) in self.model.iter().zip(&self.residual).enumerate() {
            if !self.view.support.contains(i) {
                continue;
            }
            let h = f64::from(-model.ln() + log_default);
            let c = f64::from(-2.0 * residual);
            let inverse = f64::from(model / (1.0 + curvature * model));
            g.hh += h * inverse * h;
            g.hc += h * inverse * c;
            g.cc += c * inverse * c;
            g.ff += inverse;
        }
        let alpha = f64::from(self.alpha);
        g.cj = g.hc - alpha * g.cc;
        g.jj = g.hh + alpha * alpha * g.cc - 2.0 * alpha * g.hc;
        g
    }

    fn adjust_alpha(&mut self) -> Result<(), Error> {
        let g = self.gram();
        let alpha = f64::from(self.alpha);
        let length = if self.alpha == 0.0 {
            g.ff
        } else {
            g.hh + alpha * alpha * g.cc
        };
        self.norm = if self.alpha == 0.0 {
            0.0
        } else {
            (g.jj / length) as f32
        };
        if self.norm < 0.3 {
            let a = g.cj / g.cc;
            let b = a * a - (g.jj - 0.3 * length) / g.cc;
            let (lower, upper) = if b > 0.0 {
                let root = b.sqrt();
                (a - root, a + root)
            } else {
                (0.0, 0.0)
            };
            let delta = (f64::from(self.chisq) - f64::from(self.target) + g.cj) / g.cc;
            self.alpha = (alpha + delta.min(upper).max(lower)).max(0.0) as f32;
        } else {
            self.alpha = (g.hc / g.cc).max(0.0) as f32;
        }
        if self.alpha.is_finite() && self.norm.is_finite() {
            Ok(())
        } else {
            Err(Error::NonFinite)
        }
    }

    fn convolve(&mut self, source: bool) -> Result<(), Error> {
        self.scratch.fill(0.0);
        let values = if source {
            &self.model
        } else {
            &self.intermediate
        };
        for (index, &value) in values.iter().enumerate() {
            self.convolution.add(index, f64::from(value));
        }
        self.convolution
            .refresh(&mut self.scratch, &self.view.psf[0])?;
        Ok(())
    }

    fn refresh(&mut self) -> Result<(), Error> {
        self.refreshes += 1;
        // LatConvEquation::residual(result, chisq, model) calls its virtual
        // two-argument residual. On IPLatConvEquation this dispatch adds an
        // image-plane convolution before chi-square, and the outer overload
        // applies a second image-plane convolution afterwards.
        self.convolve(true)?;
        for (i, value) in self.intermediate.iter_mut().enumerate() {
            *value = self.view.residual[0][i] as f32 + self.scratch[i] as f32;
        }
        self.convolve(false)?;
        for (value, &convolution) in self.intermediate.iter_mut().zip(&self.scratch) {
            *value = -(convolution as f32) / self.psf_sum;
        }
        self.chisq = self
            .intermediate
            .iter()
            .map(|value| f64::from(value * value))
            .sum::<f64>() as f32;
        self.convolve(false)?;
        for (i, value) in self.residual.iter_mut().enumerate() {
            *value = if self.view.support.contains(i) {
                -(self.scratch[i] as f32) / self.psf_sum
            } else {
                0.0
            };
        }
        Ok(())
    }

    fn iterate(&mut self) -> Result<(), Error> {
        let g = self.gram();
        let alpha = f64::from(self.alpha);
        let length = g.hh + alpha * alpha * g.cc;
        self.norm = (g.jj / if length <= 0.0 { g.ff } else { length }) as f32;
        let scalem = if self.norm > 0.0 {
            0.3 / self.norm
        } else {
            1.0
        };
        let scale = 1.0_f32.min(scalem);
        let log_default = self.default.ln();
        let minimum = self.model.iter().copied().fold(f32::INFINITY, f32::min);
        let floor = 0.1 * minimum + 1e-8;
        for i in 0..self.model.len() {
            let model = self.model[i];
            self.step[i] = if self.view.support.contains(i) {
                let inverse = model / (1.0 + 2.0 * self.alpha * self.q * model);
                inverse * (-model.ln() + log_default + 2.0 * self.alpha * self.residual[i])
            } else {
                0.0
            };
            self.model[i] = (model + scale * self.step[i]).max(floor);
        }
        self.refresh()?;
        let mut gds = 0.0_f64;
        for i in 0..self.model.len() {
            if self.view.support.contains(i) {
                let gradient =
                    -self.model[i].ln() + log_default + 2.0 * self.alpha * self.residual[i];
                gds += f64::from(self.step[i]) * f64::from(gradient);
            }
        }
        let mut eps = if g.jj == gds {
            1.0
        } else {
            g.jj / (g.jj - gds)
        };
        if scale != 0.0 {
            eps = eps.min(f64::from(scalem / scale));
        }
        if eps <= 0.0 {
            eps = 1.0;
        }
        if (eps - 1.0).abs() > 0.3 {
            let correction = (f64::from(scale) * (eps - 1.0)) as f32;
            for (model, &step) in self.model.iter_mut().zip(&self.step) {
                *model = (*model + correction * step).max(floor);
            }
            self.refresh()?;
        }
        self.q = (f64::from(self.q) * (1.0 / eps.clamp(0.5, 2.0) + 1.0) / 2.0) as f32;
        self.adjust_alpha()
    }
}

/// Solve one dense MEM step, using CASA's default entropy and no prior.
///
/// This does not select the number of iterations or apply the increment to
/// a model: the caller owns cycle control and model lifetime.
///
/// # Errors
///
/// When the FFT cannot run or the entropy arithmetic is not finite.
pub fn mem_step(
    view: &MinorCycleView<'_>,
    budget: usize,
    threshold: f32,
) -> Result<MemStep, Error> {
    let mut state = Mem::new(view, threshold)?;
    state.refresh()?;
    state.adjust_alpha()?;
    let mut iterations = 0;
    for iteration in 0..budget {
        state.iterate()?;
        iterations += 1;
        let fit = (state.chisq / state.target).sqrt();
        if mem_converged(iteration, fit, state.norm)? {
            break;
        }
    }
    Ok(MemStep {
        model: state.model,
        residual: state.residual,
        iterations,
        alpha: state.alpha,
        refreshes: state.refreshes,
    })
}

/// `CEMemModel::solve` throws, rather than returning a model, when the
/// normalized gradient exceeds its configured maximum.
fn mem_converged(iteration: usize, fit: f32, norm: f32) -> Result<bool, Error> {
    if norm > 100.0 {
        return Err(Error::MemExcessiveGradient);
    }
    Ok(fit < 1.05 && iteration != 0 && norm < 0.05)
}

struct MemPlaneWork {
    residual: Vec<Vec<f64>>,
    model: Vec<f64>,
}

impl MemPlaneWork {
    fn new(view: &MinorCycleView<'_>) -> Self {
        Self {
            residual: view.residual.to_vec(),
            model: vec![0.0; view.shape.len()],
        }
    }
}

/// Run a dense MEM plane under the same cycle controller as the sparse
/// solvers. Returns the per-plane diagnostics, its x-major model increment,
/// and the final entropy multiplier when a step ran.
///
/// # Errors
///
/// When a MEM step cannot be solved.
pub fn run_mem_plane(
    view: &MinorCycleView<'_>,
    cycle: &CycleControls,
    statistics: &PlaneStatistics,
) -> Result<(PlaneOutcome, Vec<f64>, Option<f32>), Error> {
    if view.residual.len() != 1 || view.psf.len() != 1 {
        return Err(Error::NonFinite);
    }
    let start_peak = statistics.peak();
    let mut control = PlaneControl::new(cycle, statistics, start_peak);
    let mut outcome = PlaneOutcome {
        delta: Delta::new(1),
        iterations: 0,
        components: 0,
        absolute_flux: 0.0,
        model_flux: 0.0,
        start_peak,
        peak: start_peak,
        stop: PlaneStop::ZeroMask,
        trace: Vec::new(),
        refreshes: 0,
        end_residual: Vec::new(),
    };
    let mut stop = control.stop(start_peak);
    if stop.is_some() || view.support.is_empty() {
        outcome.stop = stop.unwrap_or(PlaneStop::ZeroMask);
        return Ok((outcome, Vec::new(), None));
    }
    let mut work = MemPlaneWork::new(view);
    let mut alpha = None;
    let threshold = control.step_threshold() as f32;
    let budget = cycle.step_iterations();
    while stop.is_none() {
        control.observe(outcome.peak);
        let step_view = MinorCycleView {
            residual: &work.residual,
            ..*view
        };
        let step = mem_step(&step_view, budget, threshold)?;
        let iterations = step.iterations;
        alpha = Some(step.alpha);
        for (sum, value) in work.model.iter_mut().zip(step.model) {
            *sum = f64::from((*sum as f32) + value);
        }
        for (out, value) in work.residual[0].iter_mut().zip(step.residual) {
            *out = f64::from(value);
        }
        outcome.model_flux = work.model.iter().sum();
        outcome.absolute_flux = work.model.iter().map(|v| v.abs()).sum();
        outcome.peak = casacore_max_abs(&work.residual[0], view.support).1;
        outcome.iterations += iterations;
        outcome.components += iterations;
        outcome.refreshes += step.refreshes;
        control.charge(iterations);
        stop = control.stop(outcome.peak);
        if stop.is_none() && iterations != budget {
            stop = Some(PlaneStop::Exited);
        }
    }
    outcome.stop = stop.expect("MEM step loop ends on a stop");
    outcome.end_residual = work.residual;
    Ok((outcome, work.model, alpha))
}

/// Maximum heap bytes held by a dense MEM plane solve, excluding the view.
/// Includes the solver step, a copied f64 residual and the accumulated f64
/// model increment.
#[must_use]
pub fn mem_plane_bytes(shape: PlaneShape) -> u64 {
    mem_step_bytes(shape)
        .saturating_add((shape.len() as u64).saturating_mul(2 * size_of::<f64>() as u64))
        .saturating_add(size_of::<Vec<f64>>() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PsfSummary, Support};

    #[test]
    fn alpha_stays_zero_when_the_quadratic_has_no_real_bounds() {
        let shape = PlaneShape::new(32, 32);
        let bytes = include_bytes!("../tests/fixtures/mem_casa_676.f32le");
        let psf = vec![
            bytes[..4096]
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f64::from(f32::from_le_bytes(*b)))
                .collect::<Vec<_>>(),
        ];
        let residual = vec![vec![0.0; shape.len()]];
        let summary = PsfSummary::new(&psf[0], shape).unwrap();
        let support = Support::new(shape, (0..shape.len()).map(|i| i < 51).collect());
        let view = MinorCycleView {
            shape,
            psf: &psf,
            residual: &residual,
            summary: &summary,
            support: &support,
        };
        let mut state = Mem::new(&view, 0.0).unwrap();
        state.model.fill(0.1 * state.default);
        state.model[0] = 10.0 * state.default;
        state.residual.fill(0.5);
        let gram = state.gram();
        let a = gram.cj / gram.cc;
        let discriminant = a * a - (gram.jj - 0.3 * gram.ff) / gram.cc;
        assert!(
            discriminant < 0.0,
            "fixture must exercise the no-real-bounds branch"
        );
        state.adjust_alpha().unwrap();
        assert_eq!(state.alpha, 0.0);
    }

    #[test]
    fn excessive_gradient_fails_instead_of_returning_a_model() {
        assert_eq!(
            mem_converged(1, 1.0, 100.01),
            Err(Error::MemExcessiveGradient)
        );
        assert_eq!(mem_converged(1, 1.0, 100.0), Ok(false));
        assert_eq!(mem_converged(1, 1.0, 0.01), Ok(true));
        assert_eq!(mem_converged(0, 1.0, 0.01), Ok(false));
    }

    #[test]
    fn controller_owned_planes_are_included_in_the_mem_plane_charge() {
        let shape = PlaneShape::new(32, 32);
        let bytes = include_bytes!("../tests/fixtures/mem_casa_676.f32le");
        let psf = vec![
            bytes[..4096]
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f64::from(f32::from_le_bytes(*b)))
                .collect::<Vec<_>>(),
        ];
        let residual = vec![
            bytes[4096..8192]
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f64::from(f32::from_le_bytes(*b)))
                .collect::<Vec<_>>(),
        ];
        let summary = PsfSummary::new(&psf[0], shape).unwrap();
        let support = Support::full(shape);
        let view = MinorCycleView {
            shape,
            psf: &psf,
            residual: &residual,
            summary: &summary,
            support: &support,
        };
        let work = MemPlaneWork::new(&view);
        let held = work.model.capacity() * size_of::<f64>()
            + work.residual.capacity() * size_of::<Vec<f64>>()
            + work
                .residual
                .iter()
                .map(|plane| plane.capacity() * size_of::<f64>())
                .sum::<usize>();
        let admitted = mem_plane_bytes(shape) - mem_step_bytes(shape);
        assert!(
            held as u64 <= admitted,
            "controller planes hold {held} bytes but admit {admitted}"
        );
    }

    #[test]
    fn retained_planes_are_included_in_the_mem_step_charge() {
        let shape = PlaneShape::new(32, 32);
        let bytes = include_bytes!("../tests/fixtures/mem_casa_676.f32le");
        let psf = vec![
            bytes[..4096]
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f64::from(f32::from_le_bytes(*b)))
                .collect::<Vec<_>>(),
        ];
        let residual = vec![
            bytes[4096..8192]
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f64::from(f32::from_le_bytes(*b)))
                .collect::<Vec<_>>(),
        ];
        let summary = PsfSummary::new(&psf[0], shape).unwrap();
        let support = Support::full(shape);
        let view = MinorCycleView {
            shape,
            psf: &psf,
            residual: &residual,
            summary: &summary,
            support: &support,
        };
        let state = Mem::new(&view, 0.2).unwrap();
        let held = (state.model.capacity()
            + state.residual.capacity()
            + state.step.capacity()
            + state.intermediate.capacity())
            * size_of::<f32>()
            + state.scratch.capacity() * size_of::<f64>();
        let admitted = mem_step_bytes(shape) - LinearRefresh::bytes(shape);
        assert!(
            held as u64 <= admitted,
            "retained planes hold {held} bytes but admit {admitted}"
        );
    }
}
