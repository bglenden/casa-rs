// SPDX-License-Identifier: LGPL-3.0-or-later
//! T0: the Metal backend against the CPU backend on every kernel set of
//! this crate's fixtures (seven-tap spheroidal, five-tap separable, complex
//! dense with gradients and leakage routing), every `Work` variant and
//! mode, Taylor terms, several planes and a tile with a non-zero origin.
//!
//! Tap selection and `sumwt` are the same rule on both backends (the host
//! locates every sample), so grids and samples agree to the order of `f32`
//! additions (1e-4 of the peak) and `sumwt` exactly. Blocks hold thousands
//! of samples so every `apply` rotates through the ring. The laws skip,
//! saying so, on a host without a Metal device.

mod common;

use std::panic::{AssertUnwindSafe, catch_unwind};

use casa_imaging_metal::MetalBackend;
use casa_imaging_operator::{
    Basis, CpuBackend, GridBackend, GridScalar, MeasurementOperator, Mode, ModeSet, ModelPrescale,
    PlaneRange, SampleBuffer, Tile, Work,
};
use common::{
    ALL_KERNELS, Kernels, Rng, assert_close, assert_same_grids, block, metal, model, operator,
    placements,
};
use num_complex::Complex32;

/// Samples per law: more than three ring sub-blocks.
const SAMPLES: usize = 4_000;
const TAYLOR: Basis = Basis::Taylor {
    terms: 2,
    reference_hz: 1.0e9,
};

fn full(operator: &MeasurementOperator) -> Tile {
    Tile::full(operator.geometry().grid_shape())
}

/// A tile away from the grid origin, as a `Regions` owner holds it.
fn region(operator: &MeasurementOperator) -> Tile {
    let [nx, ny] = operator.geometry().grid_shape();
    Tile {
        origin: [nx / 4, ny / 3],
        shape: [nx / 2, ny / 2],
    }
}

/// Grid `buffer` in every mode of `modes` with both backends into
/// accumulators over `tile` and compare them.
fn grid_both(
    operator: &MeasurementOperator,
    metal: &mut MetalBackend<'_>,
    buffer: &SampleBuffer,
    tile: Tile,
    modes: ModeSet,
    label: &str,
) {
    let planes = PlaneRange::new(0, operator.basis().planes());
    let tile = Some(tile).filter(|tile| *tile != full(operator));
    let mut cpu = operator.accumulator(planes, tile, modes);
    let mut device = MetalBackend::accumulator(operator.accumulator_layout(planes, tile, modes))
        .expect("device");
    for mode in [Mode::Data, Mode::Psf, Mode::Weight] {
        if !modes.contains(mode) {
            continue;
        }
        CpuBackend::new()
            .apply(
                &buffer.block(),
                operator.cf(),
                Work::Grid {
                    mode,
                    acc: &mut cpu,
                },
            )
            .expect("cpu grid");
        metal
            .apply(
                &buffer.block(),
                operator.cf(),
                Work::Grid {
                    mode,
                    acc: &mut device,
                },
            )
            .expect("metal grid");
    }
    assert_same_grids(&device, &cpu, label);
}

#[test]
fn every_kernel_set_grids_data_psf_and_weight_like_the_cpu() {
    for kernels in ALL_KERNELS {
        let mut rng = Rng::new(11);
        for (basis, tiled) in [
            (Basis::ChannelLocal { planes: 3 }, false),
            (TAYLOR, false),
            (TAYLOR, true),
        ] {
            let operator = operator(kernels, basis, &mut rng);
            let Some(mut metal) = metal(&operator) else {
                return;
            };
            let tile = if tiled {
                region(&operator)
            } else {
                full(&operator)
            };
            let placed = placements(&operator, tile, SAMPLES, basis.planes(), &mut rng);
            let buffer = block(&placed, &mut rng);
            let modes = match kernels {
                Kernels::Spheroidal => ModeSet::DATA_PSF,
                Kernels::Separable | Kernels::Dense => ModeSet::ALL,
            };
            grid_both(
                &operator,
                &mut metal,
                &buffer,
                tile,
                modes,
                &format!("{kernels:?} {basis:?} tiled {tiled}"),
            );
        }
    }
}

#[test]
fn predictions_match_the_cpu_for_both_tap_layouts() {
    for kernels in ALL_KERNELS {
        for basis in [Basis::ChannelLocal { planes: 3 }, TAYLOR] {
            let mut rng = Rng::new(13);
            let operator = operator(kernels, basis, &mut rng);
            let Some(mut metal) = metal(&operator) else {
                return;
            };
            let prepared = operator
                .prepare_model(&model(&operator, &mut rng), ModelPrescale::Unit)
                .expect("model");
            let placed = placements(
                &operator,
                full(&operator),
                SAMPLES,
                basis.planes(),
                &mut rng,
            );
            let buffer = block(&placed, &mut rng);
            let mut cpu = vec![Complex32::default(); placed.len() * 2];
            let mut device = vec![Complex32::default(); placed.len() * 2];
            CpuBackend::new()
                .apply(
                    &buffer.block(),
                    operator.cf(),
                    Work::Predict {
                        model: &prepared,
                        out: &mut cpu,
                    },
                )
                .expect("cpu predict");
            metal
                .apply(
                    &buffer.block(),
                    operator.cf(),
                    Work::Predict {
                        model: &prepared,
                        out: &mut device,
                    },
                )
                .expect("metal predict");
            assert_close(&device, &cpu, &format!("{kernels:?} {basis:?} prediction"));
        }
    }
}

#[test]
fn fused_residuals_match_the_cpu_with_and_without_the_residual_samples() {
    for kernels in ALL_KERNELS {
        for (basis, tiled) in [(Basis::ChannelLocal { planes: 3 }, false), (TAYLOR, true)] {
            let mut rng = Rng::new(17);
            let operator = operator(kernels, basis, &mut rng);
            let Some(mut metal) = metal(&operator) else {
                return;
            };
            let prepared = operator
                .prepare_model(&model(&operator, &mut rng), ModelPrescale::Unit)
                .expect("model");
            let tile = if tiled {
                region(&operator)
            } else {
                full(&operator)
            };
            let placed = placements(&operator, tile, SAMPLES, basis.planes(), &mut rng);
            let buffer = block(&placed, &mut rng);
            let planes = PlaneRange::new(0, basis.planes());
            let tile = Some(tile).filter(|tile| *tile != full(&operator));
            for keep_samples in [false, true] {
                let label = format!("{kernels:?} {basis:?} tiled {tiled} samples {keep_samples}");
                let mut cpu_acc = operator.accumulator(planes, tile, ModeSet::DATA);
                let mut device_acc = MetalBackend::accumulator(operator.accumulator_layout(
                    planes,
                    tile,
                    ModeSet::DATA,
                ))
                .expect("device");
                let mut cpu = vec![Complex32::default(); placed.len() * 2];
                let mut device = vec![Complex32::default(); placed.len() * 2];
                CpuBackend::new()
                    .apply(
                        &buffer.block(),
                        operator.cf(),
                        Work::ResidualGrid {
                            model: &prepared,
                            acc: &mut cpu_acc,
                            residual_out: keep_samples.then_some(cpu.as_mut_slice()),
                        },
                    )
                    .expect("cpu residual");
                metal
                    .apply(
                        &buffer.block(),
                        operator.cf(),
                        Work::ResidualGrid {
                            model: &prepared,
                            acc: &mut device_acc,
                            residual_out: keep_samples.then_some(device.as_mut_slice()),
                        },
                    )
                    .expect("metal residual");
                assert_same_grids(&device_acc, &cpu_acc, &label);
                if keep_samples {
                    assert_close(&device, &cpu, &label);
                }
            }
        }
    }
}

#[test]
fn sumwt_is_accumulated_in_f64() {
    // One sample of weight 2^24 and then a thousand unit weights: an f32
    // sum would drop every unit increment.
    let mut rng = Rng::new(19);
    let operator = operator(Kernels::Spheroidal, Basis::Constant, &mut rng);
    let Some(mut metal) = metal(&operator) else {
        return;
    };
    let placed = placements(&operator, full(&operator), 1_001, 1, &mut rng);
    let mut buffer = SampleBuffer::new(2);
    for (index, placement) in placed.iter().enumerate() {
        let weight = if index == 0 { 16_777_216.0 } else { 1.0 };
        buffer.push(*placement, &[Complex32::new(weight, 0.0); 2], &[weight; 2]);
    }
    let planes = PlaneRange::single(0);
    let mut cpu = operator.accumulator(planes, None, ModeSet::PSF);
    let mut device =
        MetalBackend::accumulator(operator.accumulator_layout(planes, None, ModeSet::PSF))
            .expect("device");
    let psf = |acc| Work::Grid {
        mode: Mode::Psf,
        acc,
    };
    CpuBackend::new()
        .apply(&buffer.block(), operator.cf(), psf(&mut cpu))
        .expect("cpu");
    metal
        .apply(&buffer.block(), operator.cf(), psf(&mut device))
        .expect("metal");
    assert_eq!(device.sumwt(), cpu.sumwt());
    let total = device.sumwt_at(0, 0, 0);
    assert!(
        total > 16_777_216.0 + 900.0,
        "unit weights were kept: {total}"
    );
}

#[test]
fn a_zero_kernel_norm_predicts_zero_and_adds_no_weight() {
    let mut rng = Rng::new(23);
    let operator = operator(Kernels::Dense, Basis::Constant, &mut rng);
    let Some(mut metal) = metal(&operator) else {
        return;
    };
    let prepared = operator
        .prepare_model(&model(&operator, &mut rng), ModelPrescale::Unit)
        .expect("model");
    // Zero weights on one polarization: the pair is skipped on both
    // backends; the other polarization still predicts.
    let placed = placements(&operator, full(&operator), 600, 1, &mut rng);
    let mut buffer = SampleBuffer::new(2);
    for placement in &placed {
        buffer.push(*placement, &[Complex32::new(1.0, 1.0); 2], &[0.0, 1.0]);
    }
    let planes = PlaneRange::single(0);
    let mut device =
        MetalBackend::accumulator(operator.accumulator_layout(planes, None, ModeSet::DATA))
            .expect("device");
    let mut cpu = operator.accumulator(planes, None, ModeSet::DATA);
    let mut device_samples = vec![Complex32::default(); placed.len() * 2];
    let mut cpu_samples = vec![Complex32::default(); placed.len() * 2];
    metal
        .apply(
            &buffer.block(),
            operator.cf(),
            Work::ResidualGrid {
                model: &prepared,
                acc: &mut device,
                residual_out: Some(&mut device_samples),
            },
        )
        .expect("metal");
    CpuBackend::new()
        .apply(
            &buffer.block(),
            operator.cf(),
            Work::ResidualGrid {
                model: &prepared,
                acc: &mut cpu,
                residual_out: Some(&mut cpu_samples),
            },
        )
        .expect("cpu");
    assert_same_grids(&device, &cpu, "half-weighted residual");
    assert!(
        device_samples
            .chunks(2)
            .all(|sample| sample[0] == Complex32::default()),
        "a zero weight hands back a zero residual"
    );
    assert_close(&device_samples, &cpu_samples, "residual samples");
}

#[test]
fn non_finite_values_reach_the_same_cells_on_both_backends() {
    let mut rng = Rng::new(29);
    let operator = operator(Kernels::Spheroidal, Basis::Constant, &mut rng);
    let Some(mut metal) = metal(&operator) else {
        return;
    };
    let placed = placements(&operator, full(&operator), 64, 1, &mut rng);
    let mut buffer = block(&placed, &mut rng);
    let mut poisoned = SampleBuffer::new(2);
    for (index, placement) in buffer.placements().iter().enumerate() {
        let values = if index == 7 {
            [Complex32::new(f32::NAN, 0.0); 2]
        } else {
            [Complex32::new(1.0, 0.0); 2]
        };
        poisoned.push(*placement, &values, &[1.0; 2]);
    }
    buffer = poisoned;
    let planes = PlaneRange::single(0);
    let mut cpu = operator.accumulator(planes, None, ModeSet::DATA);
    let mut device =
        MetalBackend::accumulator(operator.accumulator_layout(planes, None, ModeSet::DATA))
            .expect("device");
    let data = |acc| Work::Grid {
        mode: Mode::Data,
        acc,
    };
    CpuBackend::new()
        .apply(&buffer.block(), operator.cf(), data(&mut cpu))
        .expect("cpu");
    metal
        .apply(&buffer.block(), operator.cf(), data(&mut device))
        .expect("metal");
    let non_finite = |cells: &[Complex32]| {
        cells
            .iter()
            .map(|cell| !(cell.re.is_finite() && cell.im.is_finite()))
            .collect::<Vec<_>>()
    };
    let device_cells = <f32 as GridScalar>::cells(device.storage());
    assert!(non_finite(device_cells).iter().any(|bad| *bad));
    assert_eq!(
        non_finite(device_cells),
        non_finite(<f32 as GridScalar>::cells(cpu.storage()))
    );
}

#[test]
fn one_backend_serves_successive_accumulators_and_empty_blocks() {
    let mut rng = Rng::new(31);
    let operator = operator(Kernels::Dense, Basis::ChannelLocal { planes: 3 }, &mut rng);
    let Some(mut metal) = metal(&operator) else {
        return;
    };
    let empty = SampleBuffer::new(2);
    for round in 0..3 {
        let placed = placements(&operator, full(&operator), 1_500 + 700 * round, 3, &mut rng);
        let buffer = block(&placed, &mut rng);
        let planes = PlaneRange::new(0, 3);
        let layout = operator.accumulator_layout(planes, None, ModeSet::DATA);
        let mut device = MetalBackend::accumulator(layout).expect("device");
        metal
            .apply(
                &empty.block(),
                operator.cf(),
                Work::Grid {
                    mode: Mode::Data,
                    acc: &mut device,
                },
            )
            .expect("empty");
        assert!(
            <f32 as GridScalar>::cells(device.storage())
                .iter()
                .all(|cell| *cell == Complex32::default())
        );
        let mut cpu = operator.accumulator(planes, None, ModeSet::DATA);
        let data = |acc| Work::Grid {
            mode: Mode::Data,
            acc,
        };
        CpuBackend::new()
            .apply(&buffer.block(), operator.cf(), data(&mut cpu))
            .expect("cpu");
        metal
            .apply(&buffer.block(), operator.cf(), data(&mut device))
            .expect("metal");
        assert_same_grids(&device, &cpu, &format!("round {round}"));
    }
}

#[test]
fn misplaced_samples_and_host_accumulators_are_refused_before_the_device() {
    let mut rng = Rng::new(37);
    let operator = operator(Kernels::Spheroidal, Basis::Constant, &mut rng);
    let Some(mut metal) = metal(&operator) else {
        return;
    };
    // 3,001 samples are three ring sub-blocks of 1,001; the one misplaced
    // sample (anchored left of the tile) sits in the third, so the panic
    // comes after two sub-blocks are committed.
    let tile = region(&operator);
    let [nx, ny] = operator.geometry().grid_shape();
    let outside = Tile {
        origin: [0, 0],
        shape: [nx / 4, ny / 3],
    };
    let mut placed = placements(&operator, tile, 3_000, 1, &mut rng);
    placed.insert(2_500, placements(&operator, outside, 1, 1, &mut rng)[0]);
    let buffer = block(&placed, &mut rng);
    let planes = PlaneRange::single(0);
    let panic_message = |outcome: Result<(), Box<dyn std::any::Any + Send>>| {
        let payload = outcome.expect_err("the dispatch is refused");
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
            .unwrap_or_default()
    };
    let layout = operator.accumulator_layout(planes, Some(tile), ModeSet::DATA);
    let mut tiled = MetalBackend::accumulator(layout.clone()).expect("device");
    let message = panic_message(catch_unwind(AssertUnwindSafe(|| {
        let _ = metal.apply(
            &buffer.block(),
            operator.cf(),
            Work::Grid {
                mode: Mode::Data,
                acc: &mut tiled,
            },
        );
    })));
    assert!(
        message.contains("outside the accumulator tile"),
        "{message}"
    );
    // The panic left `apply` only after the committed sub-blocks finished:
    // the tile already holds exactly their samples.
    let mut committed = SampleBuffer::new(2);
    let whole = buffer.block();
    for sample in 0..2_002 {
        committed.push(
            whole.placements[sample],
            whole.values_of(sample),
            whole.weights_of(sample),
        );
    }
    let mut cpu = operator.accumulator(planes, Some(tile), ModeSet::DATA);
    CpuBackend::new()
        .apply(
            &committed.block(),
            operator.cf(),
            Work::Grid {
                mode: Mode::Data,
                acc: &mut cpu,
            },
        )
        .expect("cpu");
    assert_close(
        <f32 as GridScalar>::cells(tiled.storage()),
        <f32 as GridScalar>::cells(cpu.storage()),
        "the committed sub-blocks",
    );
    let mut host = operator.accumulator(planes, None, ModeSet::DATA);
    let message = panic_message(catch_unwind(AssertUnwindSafe(|| {
        let _ = metal.apply(
            &buffer.block(),
            operator.cf(),
            Work::Grid {
                mode: Mode::Data,
                acc: &mut host,
            },
        );
    })));
    assert!(message.contains("MetalBackend::accumulator"), "{message}");
}
