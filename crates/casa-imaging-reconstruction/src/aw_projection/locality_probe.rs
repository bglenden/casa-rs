// SPDX-License-Identifier: LGPL-3.0-or-later
use super::*;

fn kernel(support: [usize; 2]) -> AwConvolutionKernel {
    let layout = AwKernelLayout::new(
        support,
        1,
        support.map(|value| 2 * value + 3),
        support.map(|value| value + 1),
    )
    .unwrap();
    AwConvolutionKernel::new(
        layout,
        (0..layout.shape[0] * layout.shape[1])
            .map(|index| Complex64::new(1.0 + (index % 17) as f64 / 17.0, (index % 7) as f64 / 9.0))
            .collect(),
    )
    .unwrap()
}

#[test]
fn grid_plan_locality_preserves_normalization_and_per_cell_arithmetic() {
    let shape = [40, 32];
    let kernel = kernel([2, 3]);
    let sample =
        AwVisibilitySample::new(10.0, 10.0, 1.0, 0, 0.0, [17.35, 11.65], [0.2, -0.3]).unwrap();
    let taps = fused_taps(&kernel, shape, sample, true).unwrap();
    let plan = AwGridPlan::new(
        shape,
        FusedTaps {
            values: taps.values.clone(),
            normalization: taps.normalization,
        },
    );
    assert_eq!(plan.normalization, taps.normalization);
    let mut expected = (0..shape[0] * shape[1])
        .map(|index| Complex64::new(index as f64 / 13.0, index as f64 / 7.0))
        .collect::<Vec<_>>();
    let mut expected_errors = vec![Complex64::new(1e-14, -1e-13); expected.len()];
    let mut actual = expected.clone();
    let mut actual_errors = expected_errors.clone();
    for value in [
        Complex64::new(0.5, -1.5),
        Complex64::new(-1e6, 0.03),
        Complex64::new(1e-9, 9.0),
    ] {
        compensated_taps(&mut expected, &mut expected_errors, &taps.values, value);
        plan.grid_compensated(&mut actual, &mut actual_errors, value)
            .unwrap();
    }
    assert_eq!(actual, expected);
    assert_eq!(actual_errors, expected_errors);
    assert!(
        plan.taps
            .windows(2)
            .all(|pair| pair[0].index < pair[1].index)
    );
}

#[test]
#[ignore = "T51 4096-grid locality timing; release only under a 120s/2GiB supervisor"]
#[allow(clippy::assertions_on_constants)]
fn t51_grid_plan_locality_timing() {
    assert!(!cfg!(debug_assertions), "use a release test binary");
    let shape = [4096, 4096];
    let kernel = kernel([14, 14]);
    let planes = 3;
    let mut grids = (0..planes)
        .map(|_| vec![Complex64::default(); shape[0] * shape[1]])
        .collect::<Vec<_>>();
    let mut errors = grids.clone();
    let samples = 20_000;
    let started = std::time::Instant::now();
    for index in 0..samples {
        let sample = AwVisibilitySample::new(
            10.0,
            10.0,
            1.0,
            0,
            0.0,
            [
                32.25 + ((index * 131) % 4032) as f64,
                32.75 + ((index * 47) % 4032) as f64,
            ],
            [0.002, -0.003],
        )
        .unwrap();
        let taps = fused_taps(&kernel, shape, sample, true).unwrap();
        let plan = AwGridPlan::new(shape, taps);
        for (plane, (grid, errors)) in grids.iter_mut().zip(&mut errors).enumerate() {
            plan.grid_compensated(
                grid,
                errors,
                Complex64::new((plane + 1) as f64 / 5.0, (index % 11) as f64 / 11.0),
            )
            .unwrap();
        }
    }
    let seconds = started.elapsed().as_secs_f64();
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    for value in grids.iter().chain(&errors).flatten() {
        assert!(finite(*value));
        hash.update(value.re.to_le_bytes());
        hash.update(value.im.to_le_bytes());
    }
    eprintln!(
        "t51_grid_plan_locality {}",
        serde_json::json!({
            "samples": samples, "planes": planes, "tap_count": 29 * 29,
            "shape": shape, "seconds": seconds, "grid_and_compensation_sha256": format!("{:x}", hash.finalize()),
            "scope": "synthetic hot-loop control including tap formation and plan ordering; not imaging acceptance",
        })
    );
}
