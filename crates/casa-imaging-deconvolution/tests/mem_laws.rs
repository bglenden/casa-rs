// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA image-plane MEM step laws (#1060).

use casa_imaging_deconvolution::{
    CycleControls, MinorCycleView, PlaneShape, PlaneStatistics, PlaneStop, PsfSummary, Support,
    mem_step, run_mem_plane,
};

const SHAPE: PlaneShape = PlaneShape::new(32, 32);
const FIXTURE: &[u8] = include_bytes!("fixtures/mem_casa_676.f32le");

fn plane(index: usize) -> Vec<f64> {
    let bytes = &FIXTURE[index * 4096..(index + 1) * 4096];
    bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|bytes| f64::from(f32::from_le_bytes(*bytes)))
        .collect()
}

fn solve(mask: Support, budget: usize, threshold: f32, expected: usize, alpha: f32) -> Vec<f32> {
    let psf = vec![plane(0)];
    let residual = vec![plane(1)];
    let summary = PsfSummary::new(&psf[0], SHAPE).unwrap();
    let view = MinorCycleView {
        shape: SHAPE,
        residual: &residual,
        psf: &psf,
        summary: &summary,
        support: &mask,
    };
    let result = mem_step(&view, budget, threshold).unwrap();
    assert_eq!(result.iterations, budget);
    assert!(
        (result.alpha - alpha).abs() < 1e-5,
        "alpha {} versus {alpha}",
        result.alpha
    );
    let expected = plane(expected);
    let (difference, reference) =
        result
            .model
            .iter()
            .zip(&expected)
            .fold((0.0, 0.0), |(d, r), (&actual, &expected)| {
                (
                    d + (f64::from(actual) - expected).powi(2),
                    r + expected.powi(2),
                )
            });
    let nrms = (difference / reference).sqrt();
    assert!(nrms < 1e-5, "CASA MEM model NRMS {nrms}");
    result.model
}

#[test]
fn full_plane_mem_follows_casa_image_plane_residual() {
    solve(Support::full(SHAPE), 1, 0.200_081_21, 2, 0.278_398_78);
}

#[test]
fn masked_mem_floors_even_the_pixels_outside_the_clean_mask() {
    let flags = (0..SHAPE.len())
        .map(|i| {
            let [x, y] = SHAPE.pixel(i);
            (10..=21).contains(&x) && (10..=21).contains(&y)
        })
        .collect();
    let support = Support::new(SHAPE, flags);
    let model = solve(support.clone(), 2, 0.157_688_44, 3, 0.370_483_13);
    for (i, value) in model.iter().enumerate() {
        if !support.contains(i) {
            assert_eq!(*value, 1.1e-8_f32);
        }
    }
}

#[test]
fn mem_plane_obeys_the_cycle_budget_and_threshold() {
    let psf = vec![plane(0)];
    let residual = vec![plane(1)];
    let summary = PsfSummary::new(&psf[0], SHAPE).unwrap();
    let support = Support::full(SHAPE);
    let view = MinorCycleView {
        shape: SHAPE,
        residual: &residual,
        psf: &psf,
        summary: &summary,
        support: &support,
    };
    let statistics = PlaneStatistics::measure(&residual[0], &support, &support, 0.0, false);
    let cycle = CycleControls {
        iterations: 1,
        threshold: 0.200_081_21,
        threshold_reached: false,
        gain: 0.1,
        nsigma: 0.0,
    };
    let (outcome, model, alpha) = run_mem_plane(&view, &cycle, &statistics).unwrap();
    assert_eq!(outcome.iterations, 1);
    assert!((alpha.unwrap() - 0.278_398_78).abs() < 1e-5);
    assert_eq!(outcome.stop, PlaneStop::Iterations);
    let expected = plane(2);
    let (difference, reference) = model
        .iter()
        .zip(&expected)
        .fold((0.0, 0.0), |(d, r), (&a, &b)| {
            (d + (a - b).powi(2), r + b.powi(2))
        });
    assert!((difference / reference).sqrt() < 1e-5);
    let stopped = CycleControls {
        threshold: 10.0,
        ..cycle
    };
    let (outcome, model, alpha) = run_mem_plane(&view, &stopped, &statistics).unwrap();
    assert_eq!(outcome.stop, PlaneStop::CycleThreshold);
    assert_eq!(outcome.iterations, 0);
    assert!(model.is_empty());
    assert_eq!(alpha, None);
}

#[test]
fn mem_uses_the_actual_mask_on_every_tile_of_a_large_plane() {
    let shape = PlaneShape::new(512, 512);
    let mut psf = vec![0.0; shape.len()];
    let mut dirty = vec![0.0; shape.len()];
    let mut mask = vec![false; shape.len()];
    for x in 0..shape.nx {
        for y in 0..shape.ny {
            let index = shape.index(x, y);
            let r2 = ((x as f64 - 256.0).powi(2) + (y as f64 - 256.0).powi(2)) / 8.0;
            psf[index] = (-r2).exp();
            dirty[index] = (-r2).exp();
            mask[index] = (220..=300).contains(&x) && (220..=300).contains(&y);
        }
    }
    let psf = vec![psf];
    let residual = vec![dirty];
    let summary = PsfSummary::new(&psf[0], shape).unwrap();
    let support = Support::new(shape, mask);
    let view = MinorCycleView {
        shape,
        residual: &residual,
        psf: &psf,
        summary: &summary,
        support: &support,
    };
    let result = mem_step(&view, 1, 0.000_1).unwrap();
    assert!(
        result.alpha > 0.0,
        "the supported signal determines the entropy multiplier"
    );
    assert!(
        result.model[shape.index(256, 256)] > result.model[shape.index(220, 220)],
        "the supported signal changes the model where it lies"
    );
}
