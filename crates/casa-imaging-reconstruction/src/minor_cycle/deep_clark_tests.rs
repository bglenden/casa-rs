// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use num_complex::Complex64;

#[test]
#[ignore = "requires CASA/native captured first-minor arrays in CASA_RS_CLARK_FIRST_MINOR_FIXTURE"]
fn captured_first_minor_matches_casa_clark_component_positions() {
    let root = std::path::PathBuf::from(
        std::env::var_os("CASA_RS_CLARK_FIRST_MINOR_FIXTURE").expect("captured fixture directory"),
    );
    let load = |name: &str| {
        let bytes = std::fs::read(root.join(name)).expect("captured f64 plane");
        assert_eq!(bytes.len(), 1024 * 1024 * 8);
        bytes
            .chunks_exact(8)
            .map(|value| f64::from_le_bytes(value.try_into().unwrap()))
            .collect::<Vec<_>>()
    };
    let dirty = load("native-dirty.f64le");
    let psf = load("native-psf.f64le");
    let support = load("native-mask.f64le")
        .into_iter()
        .map(|value| value == 1.0)
        .collect::<Vec<_>>();
    let (problem, lifecycle, base, normal) = crate::major_cycle::native_minor_fixture::build_clark(
        1024,
        dirty
            .into_iter()
            .map(|value| Complex64::new(value, 0.0))
            .collect(),
        psf.into_iter()
            .map(|value| Complex64::new(value, 0.0))
            .collect(),
    );
    let direction = problem.geometry().domains()[0].direction();
    let mask = ReconstructionMask::from_reprojected_support(
        problem.problem_id(),
        base.generation_id(),
        direction,
        [1024, 1024],
        &support,
        direction,
        [1024, 1024],
    )
    .expect("captured CASA mask");
    let controls = MinorCycleProgram::for_algorithm(
        ReconstructionAlgorithm::Clark,
        ReconstructionControls::new(1000, 0.1, 0.0005),
    )
    .unwrap()
    .with_fixed_cycle_threshold(Some(0.102_770_498_116_848_98))
    .record_component_sequence(100)
    .unwrap();
    let result = run_minor_cycle(&lifecycle, &base, &normal, &mask, controls).unwrap();
    let evidence = result.evidence();
    assert_eq!(evidence.iterations(), 29);
    assert_eq!(evidence.clark_refreshes(), 6);
    let positions = evidence
        .recorded_component_sequence()
        .unwrap()
        .iter()
        .map(|component| component.cell().pixel())
        .collect::<Vec<_>>();
    assert_eq!(
        positions,
        [
            [479, 529],
            [479, 529],
            [479, 529],
            [479, 529],
            [478, 529],
            [479, 528],
            [478, 528],
            [479, 529],
            [479, 529],
            [478, 528],
            [479, 528],
            [478, 529],
            [478, 529],
            [479, 528],
            [712, 379],
            [478, 529],
            [479, 528],
            [712, 379],
            [479, 528],
            [712, 379],
            [478, 529],
            [712, 378],
            [478, 529],
            [712, 378],
            [195, 729],
            [479, 528],
            [711, 379],
            [195, 729],
            [715, 378],
        ]
    );
    assert!((evidence.final_peak_flux() - 0.101_358_332_546_765_88).abs() < 2.0e-6);
}

#[test]
fn deep_clark_batches_patch_updates_and_refreshes_the_exact_residual() {
    const EDGE: usize = 32;
    const GAIN: f64 = 0.1;
    let cells = EDGE * EDGE;
    let dirty = (0..cells)
        .map(|i| {
            let x = (i / EDGE) as f64 - 16.0;
            let y = (i % EDGE) as f64 - 16.0;
            0.8 * (-(x * x + y * y) / 180.0).exp()
                + 0.4 * (-((x - 6.3).powi(2) + (y + 3.7).powi(2)) / 8.0).exp()
                + 0.03 * ((i * 7919 % 104729) as f64 / 104729.0)
        })
        .collect::<Vec<_>>();
    let psf = (0..cells)
        .map(|i| {
            let x = (i / EDGE) as f64 - 16.0;
            let y = (i % EDGE) as f64 - 16.0;
            Complex64::new(
                (-(x * x + y * y) / 2.88).exp() + 0.02 * (0.73 * x).sin() * (0.51 * y).sin(),
                0.0,
            )
        })
        .collect::<Vec<_>>();
    let (problem, lifecycle, base, normal) = crate::major_cycle::native_minor_fixture::build_clark(
        EDGE,
        dirty.iter().map(|&v| Complex64::new(v, 0.0)).collect(),
        psf.clone().into_boxed_slice(),
    );
    let mask = ReconstructionMask::full_plane(
        normal.problem_id(),
        base.generation_id(),
        problem.geometry().domains()[0].direction(),
        [EDGE, EDGE],
    )
    .expect("full deep-clean support");

    for iterations in [128, 512] {
        let controls = MinorCycleProgram::for_algorithm(
            ReconstructionAlgorithm::Clark,
            ReconstructionControls::new(iterations, GAIN, 0.0),
        )
        .unwrap()
        .record_component_sequence(iterations)
        .unwrap();
        POINT_PSF_WORK.with(|work| work.set(Some((0, 0, iterations))));
        let actual = run_minor_cycle(&lifecycle, &base, &normal, &mask, controls)
            .expect("complete deep Clark minor cycle");
        let (passes, pixels, _) = POINT_PSF_WORK.with(|work| work.take().unwrap());
        let accepted = actual.evidence().iterations();
        assert!(accepted >= 100, "exercise a deep Clark solve");
        assert!(actual.evidence().clark_refreshes() < accepted / 4);
        assert!(actual.evidence().clark_refreshes() <= 10);
        assert_eq!(passes, 0, "Clark components must not use full PSF updates");
        assert_eq!(pixels, 0);

        // Reconstruct the terminal residual independently with clipped direct
        // PSF updates. The production batch refresh uses a linear FFT.
        let mut reference = dirty.clone();
        let mut terms = BTreeMap::<usize, f64>::new();
        for component in actual.evidence().recorded_component_sequence().unwrap() {
            let pixel = component.cell().pixel();
            *terms.entry(pixel[0] * EDGE + pixel[1]).or_default() += component.flux();
        }
        for (&position, &flux) in &terms {
            let peak = [(position / EDGE) as isize, (position % EDGE) as isize];
            for (target, residual) in reference.iter_mut().enumerate() {
                let x = (target / EDGE) as isize + 16 - peak[0];
                let y = (target % EDGE) as isize + 16 - peak[1];
                if (0..EDGE as isize).contains(&x) && (0..EDGE as isize).contains(&y) {
                    *residual -= flux * psf[x as usize * EDGE + y as usize].re;
                }
            }
        }
        assert!(
            terms.len() >= accepted / 4,
            "must visit many distinct positions"
        );
        let final_peak = reference.iter().fold(0.0_f64, |m, v| m.max(v.abs()));
        assert!((actual.evidence().final_peak_flux() - final_peak).abs() < 1e-10);
        assert!(final_peak < dirty.iter().copied().fold(0.0_f64, f64::max));
        assert_eq!(
            normal
                .read_reconstruction_plane(0, 0, 0)
                .unwrap()
                .residual(),
            dirty
                .iter()
                .map(|&v| Complex64::new(v, 0.0))
                .collect::<Vec<_>>()
        );
        eprintln!(
            "deep_clark iterations={accepted} refreshes={} distinct_positions={} psf_passes={passes} visited_pixels={pixels}",
            actual.evidence().clark_refreshes(),
            terms.len()
        );
    }
}
