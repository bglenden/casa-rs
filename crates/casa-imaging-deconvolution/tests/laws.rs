// SPDX-License-Identifier: LGPL-3.0-or-later
//! T0 laws of the minor cycle: every solver recovers a known model, and the
//! driver charges and stops as CASA does.

use std::cell::Cell;

use casa_imaging_deconvolution::{
    Candidate, Clark, CycleControls, Delta, Error, Hogbom, MinorCycleView, Multiscale, Next,
    PlaneShape, PlaneStatistics, PlaneStop, PsfSummary, Solver, StepEnd, Support, Taylor,
    run_plane,
};

/// An odd, non-square plane.
const SHAPE: PlaneShape = PlaneShape { nx: 65, ny: 54 };

/// A PSF that vanishes well inside half the plane, so linear, boxed and
/// circular convolutions agree: a Gaussian main lobe and a negative ring,
/// scaled to peak `scale` at the plane centre.
fn psf(width: f64, scale: f64) -> Vec<f64> {
    let [cx, cy] = SHAPE.centre();
    let profile = |r2: f64| {
        let lobe = (-r2 / (2.0 * width * width)).exp();
        let ring = -0.15 * (-(r2.sqrt() - 3.0 * width).powi(2) / (2.0 * width * width)).exp();
        (lobe + ring) * f64::from(u8::from(r2 < 400.0))
    };
    (0..SHAPE.len())
        .map(|index| {
            let [x, y] = SHAPE.pixel(index);
            let r2 = (x as f64 - cx as f64).powi(2) + (y as f64 - cy as f64).powi(2);
            scale * profile(r2) / profile(0.0)
        })
        .collect()
}

/// `Σ model ⊛ psf`, the PSF anchored at the plane centre.
fn convolve(psf: &[f64], model: &[(usize, f64)]) -> Vec<f64> {
    let [cx, cy] = SHAPE.centre();
    let mut out = vec![0.0; SHAPE.len()];
    for &(at, flux) in model {
        let [ax, ay] = SHAPE.pixel(at);
        for (index, value) in out.iter_mut().enumerate() {
            let [x, y] = SHAPE.pixel(index);
            let px = cx as isize + x as isize - ax as isize;
            let py = cy as isize + y as isize - ay as isize;
            if (0..SHAPE.nx as isize).contains(&px) && (0..SHAPE.ny as isize).contains(&py) {
                *value += flux * psf[SHAPE.index(px as usize, py as usize)];
            }
        }
    }
    out
}

fn cycle(iterations: usize, threshold: f64) -> CycleControls {
    CycleControls {
        iterations,
        threshold,
        threshold_reached: true,
        gain: 0.1,
        nsigma: 0.0,
    }
}

fn statistics(residual: &[f64], support: &Support) -> PlaneStatistics {
    PlaneStatistics::measure(residual, support, &Support::full(SHAPE), 0.0, false)
}

struct Plane {
    residual: Vec<Vec<f64>>,
    psf: Vec<Vec<f64>>,
    summary: PsfSummary,
    support: Support,
}

impl Plane {
    fn new(residual: Vec<Vec<f64>>, psf: Vec<Vec<f64>>) -> Self {
        let summary = PsfSummary::new(&psf[0], SHAPE).expect("PSF summary");
        Self {
            residual,
            psf,
            summary,
            support: Support::full(SHAPE),
        }
    }

    fn view(&self) -> MinorCycleView<'_> {
        MinorCycleView {
            shape: SHAPE,
            residual: &self.residual,
            psf: &self.psf,
            summary: &self.summary,
            support: &self.support,
            workers: 1,
        }
    }

    fn run<S: Solver>(
        &self,
        solver: &S,
        cycle: &CycleControls,
    ) -> Result<casa_imaging_deconvolution::PlaneOutcome, Error> {
        run_plane(
            solver,
            &self.view(),
            cycle,
            &statistics(&self.residual[0], &self.support),
            8,
        )
    }
}

/// Flux of `delta` term `term` within `radius` pixels of `at`.
fn flux_near(delta: &Delta, term: usize, at: usize, radius: usize) -> f64 {
    let [ax, ay] = SHAPE.pixel(at);
    delta
        .term(term)
        .filter(|(index, _)| {
            let [x, y] = SHAPE.pixel(*index);
            x.abs_diff(ax) <= radius && y.abs_diff(ay) <= radius
        })
        .map(|(_, flux)| flux)
        .sum()
}

fn two_points() -> [(usize, f64); 2] {
    [(SHAPE.index(20, 31), 1.0), (SHAPE.index(44, 17), -0.45)]
}

/// Högbom, Clark and point-scale multiscale clean two point components to
/// the cycle threshold: the plane stops on the threshold, its residual is
/// below it, and each component's flux is recovered to the threshold.
#[test]
fn point_solvers_recover_two_components_to_the_cycle_threshold() {
    let point = psf(2.0, 1.0);
    let model = two_points();
    let plane = Plane::new(vec![convolve(&point, &model)], vec![point]);
    let threshold = 0.01;
    let cycle = cycle(5000, threshold);
    let outcomes = [
        ("hogbom", plane.run(&Hogbom::new(false), &cycle).unwrap()),
        ("clark", plane.run(&Clark::default(), &cycle).unwrap()),
        (
            "multiscale",
            plane.run(&Multiscale::new(vec![0.0], 0.0), &cycle).unwrap(),
        ),
    ];
    for (name, outcome) in outcomes {
        assert_eq!(outcome.stop, PlaneStop::CycleThreshold, "{name}");
        assert!(outcome.peak <= threshold, "{name}: peak {}", outcome.peak);
        for (at, flux) in model {
            let recovered = flux_near(&outcome.delta, 0, at, 0);
            assert!(
                (recovered - flux).abs() < 2.0 * threshold,
                "{name}: {recovered} for {flux}"
            );
        }
        assert!(
            outcome.iterations > 20 && outcome.iterations < 5000,
            "{name}"
        );
    }
}

/// Multiscale puts an extended component on its scale and a point on the
/// point scale, recovering both fluxes.
#[test]
fn multiscale_recovers_a_point_and_an_extended_component() {
    let point = psf(1.5, 1.0);
    // A 4-pixel scale function as the extended source.
    let mut source = Vec::new();
    let [ex, ey] = [40_usize, 30_usize];
    for dx in -4_isize..=4 {
        for dy in -4_isize..=4 {
            let r2 = (dx * dx + dy * dy) as f64 / 16.0;
            if r2 < 1.0 {
                let weight = (1.0 - r2) * casa_imaging_deconvolution::spheroidal(r2.sqrt());
                source.push((
                    SHAPE.index((ex as isize + dx) as usize, (ey as isize + dy) as usize),
                    weight,
                ));
            }
        }
    }
    let total = source.iter().map(|(_, weight)| weight).sum::<f64>();
    let mut model = source
        .into_iter()
        .map(|(at, weight)| (at, 2.0 * weight / total))
        .collect::<Vec<_>>();
    model.push((SHAPE.index(15, 15), 0.8));
    let plane = Plane::new(vec![convolve(&point, &model)], vec![point]);
    let outcome = plane
        .run(&Multiscale::new(vec![0.0, 4.0], 0.6), &cycle(5000, 0.005))
        .unwrap();
    assert_eq!(outcome.stop, PlaneStop::CycleThreshold);
    let extended = flux_near(&outcome.delta, 0, SHAPE.index(ex, ey), 8);
    let compact = flux_near(&outcome.delta, 0, SHAPE.index(15, 15), 2);
    assert!((extended - 2.0).abs() < 0.1, "extended {extended}");
    assert!((compact - 0.8).abs() < 0.05, "point {compact}");
}

/// Two Taylor terms with distinct spectral behaviour, from PSF moments that
/// are not proportional to one another, are recovered term by term.
#[test]
fn taylor_recovers_two_components_with_their_spectra() {
    let lobe = |scale| psf(2.0, scale);
    let psfs = vec![lobe(1.0), lobe(0.1), lobe(0.05)];
    let model = [
        (SHAPE.index(20, 31), [1.0, -0.7]),
        (SHAPE.index(44, 17), [0.5, 0.3]),
    ];
    let residual = (0..2)
        .map(|term| {
            let mut plane = vec![0.0; SHAPE.len()];
            for k in 0..2 {
                let component = model
                    .iter()
                    .map(|(at, fluxes)| (*at, fluxes[k]))
                    .collect::<Vec<_>>();
                for (value, add) in plane.iter_mut().zip(convolve(&psfs[term + k], &component)) {
                    *value += add;
                }
            }
            plane
        })
        .collect::<Vec<_>>();
    let plane = Plane::new(residual, psfs);
    let outcome = plane
        .run(&Taylor::new(2, vec![0.0], 0.0), &cycle(5000, 0.002))
        .unwrap();
    assert_eq!(outcome.stop, PlaneStop::CycleThreshold);
    for (at, fluxes) in model {
        for (term, flux) in fluxes.into_iter().enumerate() {
            let recovered = flux_near(&outcome.delta, term, at, 0);
            assert!(
                (recovered - flux).abs() < 0.02 * flux.abs().max(0.1) + 0.01,
                "term {term}: {recovered} for {flux}"
            );
        }
    }
}

/// A solver whose steps clean their whole budget with zero flux and report
/// scripted peaks, to test the driver's order alone.
struct Scripted {
    peaks: Vec<f64>,
    step: Cell<usize>,
}

impl Scripted {
    fn new(peaks: &[f64]) -> Self {
        Self {
            peaks: peaks.to_vec(),
            step: Cell::new(0),
        }
    }
}

impl Solver for Scripted {
    type State = ();

    fn initialize(&self, _: &MinorCycleView<'_>, _: &[Vec<f64>], _: f64) -> Result<(), Error> {
        Ok(())
    }

    fn next(&self, _: &mut (), _: &MinorCycleView<'_>, _: &mut [Vec<f64>]) -> Result<Next, Error> {
        Ok(Next::Clean(Candidate::Pixel {
            index: 0,
            scale: 0,
            strength: 0.0,
        }))
    }

    fn accept(
        &self,
        _: &mut (),
        _: &MinorCycleView<'_>,
        _: &mut [Vec<f64>],
        _: Candidate,
        _: f64,
        _: &mut Delta,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn finalize(
        &self,
        _: (),
        _: &MinorCycleView<'_>,
        _: &mut [Vec<f64>],
        _: &Delta,
    ) -> Result<StepEnd, Error> {
        let step = self.step.get();
        self.step.set(step + 1);
        Ok(StepEnd {
            peak: self.peaks[step],
            refreshes: 0,
        })
    }
}

/// `SDAlgorithmBase::deconvolve` notes the peak a step starts from, then
/// tests the step's own signed peak against the minimum of the earlier
/// ones: a step that ends at −0.6 after starting from 0.5 has grown by 20%
/// and stops the plane (code 4), although it is the smallest signed peak.
#[test]
fn a_step_peak_is_tested_against_the_minimum_before_it() {
    let point = psf(2.0, 1.0);
    let residual = convolve(&point, &[(SHAPE.index(20, 31), 0.5)]);
    let plane = Plane::new(vec![residual], vec![point]);
    let cycle = cycle(6000, 0.0);
    let outcome = plane
        .run(&Scripted::new(&[-0.6, 0.3, 0.2]), &cycle)
        .unwrap();
    assert_eq!(
        (outcome.stop, outcome.iterations),
        (PlaneStop::Diverged, 2000)
    );
    // Falling peaks run every 2000-iteration step.
    let outcome = plane
        .run(&Scripted::new(&[0.45, 0.4, 0.35]), &cycle)
        .unwrap();
    assert_eq!(
        (outcome.stop, outcome.iterations),
        (PlaneStop::Iterations, 6000)
    );
}

/// Exactly equal peaks go to casacore's first: x-fastest for Högbom's
/// `hclean` search, list order (x-major, GETBIMF) for Clark. The two point
/// components here are mirror images, so their peaks are equal exactly.
#[test]
fn exact_ties_go_to_casacores_first_pixel() {
    let point = psf(2.0, 1.0);
    let [cx, cy] = SHAPE.centre();
    let early = SHAPE.index(cx + 6, cy - 4);
    let late = SHAPE.index(cx - 6, cy + 4);
    let plane = Plane::new(
        vec![convolve(&point, &[(early, 0.8), (late, 0.8)])],
        vec![point.clone()],
    );
    assert_eq!(plane.residual[0][early], plane.residual[0][late]);
    let cycle = cycle(1, 0.0);
    let first = |outcome: casa_imaging_deconvolution::PlaneOutcome| outcome.trace[0].index;
    assert_eq!(
        first(plane.run(&Hogbom::new(false), &cycle).unwrap()),
        early
    );
    assert_eq!(first(plane.run(&Clark::default(), &cycle).unwrap()), late);
}

/// CASA's Högbom loop does one component more than the cycle's budget and
/// charges the budget; the plane stops on `cycleniter` (code 1).
#[test]
fn inclusive_hogbom_cleans_one_extra_component_and_charges_the_budget() {
    let point = psf(2.0, 1.0);
    let plane = Plane::new(vec![convolve(&point, &two_points())], vec![point]);
    for (inclusive, components) in [(false, 7), (true, 8)] {
        let outcome = plane.run(&Hogbom::new(inclusive), &cycle(7, 0.0)).unwrap();
        assert_eq!(outcome.components, components);
        assert_eq!(outcome.iterations, 7);
        assert_eq!(outcome.stop, PlaneStop::Iterations);
    }
}

/// `MatrixCleaner` charges the iteration that found its threshold stop; the
/// plane then stops on the cycle threshold.
#[test]
fn multiscale_charges_its_threshold_stop() {
    let point = psf(2.0, 1.0);
    let plane = Plane::new(vec![convolve(&point, &two_points())], vec![point]);
    let outcome = plane
        .run(&Multiscale::new(vec![0.0], 0.0), &cycle(1000, 0.2))
        .unwrap();
    assert_eq!(outcome.iterations, outcome.components + 1);
    assert_eq!(outcome.stop, PlaneStop::CycleThreshold);
}

/// From 5000 iterations a cycle runs in 2000-iteration steps; a plane can
/// overshoot `cycleniter` by up to one step (`SDAlgorithmBase`).
#[test]
fn large_cycles_run_in_two_thousand_iteration_steps() {
    let point = psf(2.0, 1.0);
    let plane = Plane::new(vec![convolve(&point, &two_points())], vec![point]);
    let outcome = plane.run(&Hogbom::new(false), &cycle(5001, 0.0)).unwrap();
    assert_eq!(outcome.iterations, 6000);
    assert_eq!(outcome.stop, PlaneStop::Iterations);
}

/// A plane with no cleanable pixel is skipped (code 0); a plane already at
/// the cycle threshold stops before a step (code 2).
#[test]
fn empty_and_converged_planes_do_no_step() {
    let point = psf(2.0, 1.0);
    let mut plane = Plane::new(vec![convolve(&point, &two_points())], vec![point]);
    let outcome = plane.run(&Hogbom::new(false), &cycle(100, 2.0)).unwrap();
    assert_eq!(
        (outcome.stop, outcome.iterations),
        (PlaneStop::CycleThreshold, 0)
    );
    plane.support = Support::new(SHAPE, vec![false; SHAPE.len()]);
    let outcome = plane.run(&Hogbom::new(false), &cycle(100, 0.01)).unwrap();
    assert_eq!((outcome.stop, outcome.iterations), (PlaneStop::ZeroMask, 0));
    assert!(outcome.delta.is_empty());
}

/// Components never leave the support.
#[test]
fn components_stay_on_the_support() {
    let point = psf(2.0, 1.0);
    let mut plane = Plane::new(vec![convolve(&point, &two_points())], vec![point]);
    let inside = |index: usize| {
        let [x, y] = SHAPE.pixel(index);
        x < 32 && y > 10
    };
    plane.support = Support::new(SHAPE, (0..SHAPE.len()).map(inside).collect());
    for solver in [0, 1, 2] {
        let outcome = match solver {
            0 => plane.run(&Hogbom::new(false), &cycle(500, 0.01)),
            1 => plane.run(&Clark::default(), &cycle(500, 0.01)),
            _ => plane.run(&Multiscale::new(vec![0.0, 2.0], 0.6), &cycle(500, 0.01)),
        }
        .unwrap();
        assert!(outcome.components > 0);
        // Point components sit on the support; a scale's footprint may
        // spill past it, but its centre may not.
        assert!(
            outcome
                .trace
                .iter()
                .all(|component| inside(component.index))
        );
        let model = two_points();
        assert!((flux_near(&outcome.delta, 0, model[0].0, 2) - 1.0).abs() < 0.05);
    }
}

/// A non-finite residual past the first pixel stops the solve with a typed
/// error rather than a non-finite model.
#[test]
fn a_nonfinite_residual_is_refused() {
    let point = psf(2.0, 1.0);
    let mut residual = convolve(&point, &two_points());
    residual[SHAPE.index(10, 40)] = f64::NAN;
    let plane = Plane::new(vec![residual], vec![point]);
    let cycle = cycle(100, 0.01);
    assert_eq!(
        plane.run(&Hogbom::new(false), &cycle),
        Err(Error::NonFinite)
    );
    assert_eq!(
        plane.run(&Clark::default(), &cycle).map(|_| ()),
        Err(Error::NonFinite)
    );
    assert_eq!(
        plane
            .run(&Multiscale::new(vec![0.0, 3.0], 0.0), &cycle)
            .map(|_| ()),
        Err(Error::NonFinite)
    );
}
