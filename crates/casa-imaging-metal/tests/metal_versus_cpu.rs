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
    Basis, CellHold, CfKey, CpuBackend, GridBackend, GridPrecision, GridScalar,
    MeasurementOperator, Mode, ModeSet, ModelPrescale, NO_CELL, NativeModel, NativeSource,
    NativeTarget, NativeTerm, PlaneRange, SampleBlock, SampleBuffer, Tile, Work,
};
use common::{
    ALL_KERNELS, Kernels, MOSAIC_DISHES_M, MOSAIC_FREQUENCIES_HZ, Rng, assert_close,
    assert_same_grids, block, edge_placements, metal, model, operator, placements,
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

/// #787 (#653's mosaic rows): on every key of the mosaic set, every pair
/// plane of the two dish classes at every frequency cell, the cells each
/// mode lends share the imaging cell's support: the PSF and prediction
/// cells are the imaging taps, and the weight cell is built at the same
/// support. So the Metal records' one support test per sample holds for
/// every mode on the mosaic set. AW's PSF and prediction cells differ, so
/// it waits for a per-mode test (#653). Across keys the supports differ
/// (#793 review): the 7 m beams are wider than the 12 m ones, and the lower
/// frequency cell's wider still (half supports 9, 7 and 6), so the laws
/// over `ALL_KERNELS` put three supports, and three pair planes, in one
/// dispatch.
#[test]
fn the_mosaic_sets_cells_differ_in_support_but_share_it_across_modes() {
    let mut rng = Rng::new(3);
    let operator = operator(Kernels::Mosaic, Basis::Constant, &mut rng);
    let cf = operator.cf();
    let keys = Kernels::Mosaic.keys();
    assert_eq!(
        keys.len(),
        MOSAIC_DISHES_M.len() * (MOSAIC_DISHES_M.len() + 1) / 2 * MOSAIC_FREQUENCIES_HZ.len(),
        "three pair planes at two frequency cells"
    );
    let mut supports = Vec::new();
    for &key in &keys {
        let mut hold = CellHold::new();
        let imaging = cf.taps(key, &mut hold).half_support();
        let psf = cf.psf_taps(key, &mut hold).half_support();
        let prediction = cf.prediction_taps(key, &mut hold).half_support();
        let weight = cf
            .weight_taps(key, &mut hold)
            .expect("mosaic weight taps")
            .half_support();
        assert_eq!(
            (psf, prediction, weight),
            (imaging, imaging, imaging),
            "{key:?}"
        );
        assert_eq!(
            cf.placement_half_support(key, &mut hold),
            imaging,
            "{key:?}"
        );
        eprintln!("mosaic {key:?}: half support {imaging:?}");
        supports.push(imaging);
    }
    supports.sort_unstable();
    supports.dedup();
    assert!(
        supports.len() >= 2,
        "the fixture's keys lend one support, {supports:?}; the laws need several in one dispatch"
    );
    let footprint = cf.tap_footprint();
    assert_eq!(
        footprint.cells,
        2 * keys.len(),
        "an imaging and a weight cell a key"
    );
}

/// #793 review (Astra 1): a backend's copies of the kernel cells it meets
/// stay within `kernel_bytes` of its set, which the pass admits for every
/// backend it makes, and the device holds no more than every backend's
/// charge, their accumulators and the model copy together. Two owners grid
/// every mode over every key of the mosaic set, so each meets every cell of
/// both kinds, and a predictor meets every imaging cell.
#[test]
fn kernel_copies_stay_within_their_charge() {
    let mut rng = Rng::new(53);
    let operator = operator(Kernels::Mosaic, TAYLOR, &mut rng);
    let charge = casa_imaging_metal::kernel_bytes(operator.cf());
    let footprint = operator.cf().tap_footprint();
    let before = casa_imaging_metal::device_bytes();
    let (Some(mut first), Some(mut second), Some(mut predictor)) =
        (metal(&operator), metal(&operator), metal(&operator))
    else {
        return;
    };
    let keys = Kernels::Mosaic.keys();
    let planes = PlaneRange::new(0, operator.basis().planes());
    let placed = placements(
        &operator,
        &keys,
        full(&operator),
        SAMPLES,
        operator.basis().planes(),
        &mut rng,
    );
    let buffer = block(&placed, &mut rng);
    let layout = operator.accumulator_layout(planes, None, ModeSet::ALL);
    let mut first_acc = MetalBackend::accumulator(layout.clone()).expect("device");
    let mut second_acc = MetalBackend::accumulator(layout.clone()).expect("device");
    for (backend, acc) in [(&mut first, &mut first_acc), (&mut second, &mut second_acc)] {
        for mode in [Mode::Data, Mode::Psf, Mode::Weight] {
            backend
                .apply(&buffer.block(), operator.cf(), Work::Grid { mode, acc })
                .expect("grid");
        }
    }
    let prepared = operator
        .prepare_model(&model(&operator, &mut rng), ModelPrescale::Unit)
        .expect("model");
    let mut out = vec![Complex32::default(); placed.len() * 2];
    predictor
        .apply(
            &buffer.block(),
            operator.cf(),
            Work::Predict {
                model: &prepared,
                out: &mut out,
            },
        )
        .expect("predict");
    for (name, backend) in [
        ("the first owner", &first),
        ("the second owner", &second),
        ("the predictor", &predictor),
    ] {
        let held = backend.kernel_bytes_held();
        assert!(
            held <= charge,
            "{name} holds {held} bytes of kernel copies against a charge of {charge}"
        );
    }
    // The owners met every cell of both kinds, so their norm tables are the
    // whole set's; the arena was reserved whole at the first upload.
    assert!(
        first.kernel_bytes_held() >= footprint.tap_bytes + footprint.norm_bytes,
        "an owner that met every cell holds the set's taps and norms"
    );
    let device = casa_imaging_metal::device_bytes() - before;
    let bound = 3 * (charge + casa_imaging_metal::ring_bytes(2))
        + 2 * layout.bytes(GridPrecision::F32) as u64
        + prepared.layout().bytes(GridPrecision::F32) as u64;
    assert!(
        device <= bound,
        "the device holds {device} bytes against {bound} charged"
    );
}

/// #793 review (Astra 2, Fable 1): on every key of the mosaic set, the
/// samples whose last tap touches each edge of the grid, and of a region
/// tile, grid alike on both backends in every mode. The one sample beyond
/// is `misplaced_samples_and_host_accumulators_are_refused_before_the_device`.
#[test]
fn the_mosaic_sets_edge_placements_grid_alike() {
    let mut rng = Rng::new(59);
    let operator = operator(Kernels::Mosaic, Basis::Constant, &mut rng);
    let Some(mut metal) = metal(&operator) else {
        return;
    };
    let keys = Kernels::Mosaic.keys();
    for (tile, label) in [
        (full(&operator), "the grid"),
        (region(&operator), "a region tile"),
    ] {
        let placed = edge_placements(&operator, &keys, tile, &mut rng);
        assert_eq!(
            placed.len(),
            keys.len() * 8,
            "four edges, both signs of w, every key"
        );
        let buffer = block(&placed, &mut rng);
        grid_both(
            &operator,
            &mut metal,
            &buffer,
            tile,
            ModeSet::ALL,
            &format!("mosaic edges of {label}"),
        );
    }
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
            let placed = placements(
                &operator,
                &kernels.keys(),
                tile,
                SAMPLES,
                basis.planes(),
                &mut rng,
            );
            let buffer = block(&placed, &mut rng);
            let modes = match kernels {
                Kernels::Spheroidal => ModeSet::DATA_PSF,
                Kernels::Separable | Kernels::Dense | Kernels::WideDense | Kernels::Mosaic => {
                    ModeSet::ALL
                }
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

/// A block gridded from parts ([`MetalBackend::apply_parts`]), one of them
/// empty and the others cut across ring sub-blocks, grids as the CPU grids
/// the whole block, in every mode, with `sumwt` summed in the same order.
#[test]
fn parts_grid_as_the_whole_block() {
    for kernels in ALL_KERNELS {
        let mut rng = Rng::new(23);
        let operator = operator(kernels, Basis::ChannelLocal { planes: 3 }, &mut rng);
        let Some(mut metal) = metal(&operator) else {
            return;
        };
        let placed = placements(
            &operator,
            &kernels.keys(),
            full(&operator),
            SAMPLES,
            3,
            &mut rng,
        );
        let whole = block(&placed, &mut rng);
        let whole = whole.block();
        let parts = [0, 1_100, 1_100, 3_000, SAMPLES]
            .windows(2)
            .map(|cut| SampleBlock {
                placements: &whole.placements[cut[0]..cut[1]],
                values: &whole.values[cut[0] * 2..cut[1] * 2],
                weights: &whole.weights[cut[0] * 2..cut[1] * 2],
                sources: &[],
                npol: 2,
            })
            .collect::<Vec<_>>();
        let modes = match kernels {
            Kernels::Spheroidal => ModeSet::DATA_PSF,
            Kernels::Separable | Kernels::Dense | Kernels::WideDense | Kernels::Mosaic => {
                ModeSet::ALL
            }
        };
        let planes = PlaneRange::new(0, operator.basis().planes());
        let mut cpu = operator.accumulator(planes, None, modes);
        let mut device =
            MetalBackend::accumulator(operator.accumulator_layout(planes, None, modes))
                .expect("device");
        for mode in [Mode::Data, Mode::Psf, Mode::Weight] {
            if !modes.contains(mode) {
                continue;
            }
            CpuBackend::new()
                .apply(
                    &whole,
                    operator.cf(),
                    Work::Grid {
                        mode,
                        acc: &mut cpu,
                    },
                )
                .expect("cpu grid");
            metal
                .apply_parts(
                    &parts,
                    operator.cf(),
                    Work::Grid {
                        mode,
                        acc: &mut device,
                    },
                )
                .expect("metal grid");
        }
        assert_same_grids(&device, &cpu, &format!("{kernels:?} parts"));
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
                &kernels.keys(),
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

/// Native cells of the law below.
const NATIVE_CELLS: usize = 3_000;

/// Random native terms for every cell over a batch of `samples`
/// predictions: endpoints in the batch or missing, every fifth term on one
/// prediction (a nearest or direct channel), some polarizations flagged.
fn native_terms(samples: usize, rng: &mut Rng) -> Vec<NativeTerm> {
    let index = |rng: &mut Rng| {
        if rng.next_u64().is_multiple_of(8) {
            NO_CELL
        } else {
            (rng.next_u64() % samples as u64) as u32
        }
    };
    (0..NATIVE_CELLS as u32)
        .map(|cell| {
            let low = index(rng);
            let high = if cell % 5 == 0 { low } else { index(rng) };
            NativeTerm {
                cell,
                low,
                high,
                fraction: rng.unit() as f32,
                flags: if rng.next_u64().is_multiple_of(6) {
                    (rng.next_u64() % 3 + 1) as u32
                } else {
                    0
                },
            }
        })
        .collect()
}

/// A block of random samples with random native endpoints, one endpoint
/// for a third of them.
fn sourced_block(operator: &MeasurementOperator, keys: &[CfKey], rng: &mut Rng) -> SampleBuffer {
    let placed = placements(operator, keys, full(operator), SAMPLES, 1, rng);
    let plain = block(&placed, rng);
    let mut sourced = SampleBuffer::new(2);
    let cell = |rng: &mut Rng| (rng.next_u64() % NATIVE_CELLS as u64) as u32;
    for (index, placement) in placed.iter().enumerate() {
        let one = rng.next_u64().is_multiple_of(3);
        let source = NativeSource {
            cells: [cell(rng), if one { NO_CELL } else { cell(rng) }],
            factors: [rng.unit() as f32, if one { 0.0 } else { rng.unit() as f32 }],
            scale: Complex32::new(rng.signed() as f32, rng.signed() as f32),
        };
        let block = plain.block();
        sourced.push_sourced(
            *placement,
            block.values_of(index),
            block.weights_of(index),
            source,
        );
    }
    sourced
}

/// The model at native cells that two backends form, the first writing and
/// the second adding, as a wave's domains do, and the residual a third
/// grids against it with no host wait between them, equal the CPU's. The
/// second batch has more sub-blocks than the ring holds, and a batch with
/// no samples still writes its terms.
#[test]
fn native_models_and_residuals_match_the_cpu() {
    for kernels in ALL_KERNELS {
        let mut rng = Rng::new(47);
        let operator = operator(kernels, TAYLOR, &mut rng);
        let (Some(mut first), Some(mut second), Some(owner)) =
            (metal(&operator), metal(&operator), metal(&operator))
        else {
            return;
        };
        let mut owner = owner.with_deferred_grid_completion();
        let prepared = operator
            .prepare_model(&model(&operator, &mut rng), ModelPrescale::Unit)
            .expect("model");
        let batches = [SAMPLES / 2 + 7, 7 * 32_768 + 5].map(|count| {
            let placed = placements(
                &operator,
                &kernels.keys(),
                full(&operator),
                count,
                1,
                &mut rng,
            );
            block(&placed, &mut rng)
        });
        let terms = batches
            .each_ref()
            .map(|batch| native_terms(batch.len(), &mut rng));
        let none = (0..NATIVE_CELLS as u32)
            .map(|cell| NativeTerm {
                cell,
                low: NO_CELL,
                high: NO_CELL,
                ..NativeTerm::default()
            })
            .collect::<Vec<_>>();
        let empty = SampleBuffer::new(2);
        let rounds = [
            (&batches[0], terms[0].as_slice()),
            (&batches[1], terms[1].as_slice()),
            (&empty, none.as_slice()),
        ];
        let mut cpu_model = vec![Complex32::default(); NATIVE_CELLS * 2];
        let device = MetalBackend::native_cells(NATIVE_CELLS * 2).expect("native cells");
        for (round, (batch, terms)) in rounds.into_iter().enumerate() {
            let add = round > 0;
            CpuBackend::new()
                .apply(
                    &batch.block(),
                    operator.cf(),
                    Work::PredictNative {
                        grids: &prepared,
                        terms,
                        model: NativeTarget::Host {
                            cells: &mut cpu_model,
                            first: 0,
                        },
                        add,
                    },
                )
                .expect("cpu native model");
            let backend = if round == 1 { &mut second } else { &mut first };
            backend
                .apply(
                    &batch.block(),
                    operator.cf(),
                    Work::PredictNative {
                        grids: &prepared,
                        terms,
                        model: NativeTarget::Device(&device),
                        add,
                    },
                )
                .expect("metal native model");
        }
        let samples = sourced_block(&operator, &kernels.keys(), &mut rng);
        let planes = PlaneRange::new(0, operator.basis().planes());
        let mut cpu = operator.accumulator(planes, None, ModeSet::DATA);
        CpuBackend::new()
            .apply(
                &samples.block(),
                operator.cf(),
                Work::NativeResidualGrid {
                    model: NativeModel::Host {
                        cells: &cpu_model,
                        first: 0,
                    },
                    acc: &mut cpu,
                },
            )
            .expect("cpu native residual");
        let mut metal =
            MetalBackend::accumulator(operator.accumulator_layout(planes, None, ModeSet::DATA))
                .expect("device accumulator");
        owner
            .apply(
                &samples.block(),
                operator.cf(),
                Work::NativeResidualGrid {
                    model: NativeModel::Device(&device),
                    acc: &mut metal,
                },
            )
            .expect("metal native residual");
        for backend in [&mut first, &mut second, &mut owner] {
            backend.complete().expect("device work");
        }
        // SAFETY: every backend that wrote the cells has completed, and none
        // writes them again.
        let device_model = unsafe { device.host_cells() };
        assert_close(
            device_model,
            &cpu_model,
            &format!("{kernels:?} native model"),
        );
        assert_same_grids(&metal, &cpu, &format!("{kernels:?} native residual"));
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
            let placed = placements(
                &operator,
                &kernels.keys(),
                tile,
                SAMPLES,
                basis.planes(),
                &mut rng,
            );
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
    let placed = placements(
        &operator,
        &Kernels::Spheroidal.keys(),
        full(&operator),
        1_001,
        1,
        &mut rng,
    );
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
    let placed = placements(
        &operator,
        &Kernels::Dense.keys(),
        full(&operator),
        600,
        1,
        &mut rng,
    );
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
    let placed = placements(
        &operator,
        &Kernels::Spheroidal.keys(),
        full(&operator),
        64,
        1,
        &mut rng,
    );
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
        let placed = placements(
            &operator,
            &Kernels::Dense.keys(),
            full(&operator),
            1_500 + 700 * round,
            3,
            &mut rng,
        );
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

/// A backend that defers grid completion leaves its grid work running when
/// `apply` returns, and the host's first read of the cells, with nothing
/// between that waits for the device (no prediction or readback, no
/// `complete`, no drop), sees the CPU's grids. The applies before it cover
/// data, PSF, weight and residual gridding without residual samples, and
/// later ones upload kernel tables and taps while earlier ones run: the
/// first block meets one of the two dense cells, the next both, and the
/// weight mode the weight taps.
#[test]
fn deferred_grids_are_whole_at_the_hosts_first_read() {
    let mut rng = Rng::new(43);
    let operator = operator(Kernels::Dense, Basis::ChannelLocal { planes: 3 }, &mut rng);
    let Some(metal) = metal(&operator) else {
        return;
    };
    let mut metal = metal.with_deferred_grid_completion();
    let prepared = operator
        .prepare_model(&model(&operator, &mut rng), ModelPrescale::Unit)
        .expect("model");
    let placed = placements(
        &operator,
        &Kernels::Dense.keys(),
        full(&operator),
        4 * SAMPLES,
        3,
        &mut rng,
    );
    let one_cell: Vec<_> = placed
        .iter()
        .filter(|placement| placement.cf.cube == 0)
        .copied()
        .collect();
    let (first, rest) = (block(&one_cell, &mut rng), block(&placed, &mut rng));
    let planes = PlaneRange::new(0, 3);
    let mut device =
        MetalBackend::accumulator(operator.accumulator_layout(planes, None, ModeSet::ALL))
            .expect("device");
    let mut cpu = operator.accumulator(planes, None, ModeSet::ALL);
    for (backend, acc) in [
        (&mut CpuBackend::new() as &mut dyn GridBackend, &mut cpu),
        (&mut metal, &mut device),
    ] {
        for (buffer, mode) in [
            (&first, Mode::Data),
            (&rest, Mode::Psf),
            (&rest, Mode::Weight),
        ] {
            backend
                .apply(&buffer.block(), operator.cf(), Work::Grid { mode, acc })
                .expect("grid");
        }
        backend
            .apply(
                &rest.block(),
                operator.cf(),
                Work::ResidualGrid {
                    model: &prepared,
                    acc,
                    residual_out: None,
                },
            )
            .expect("residual grid");
    }
    assert_same_grids(&device, &cpu, "first read after deferred grids");
    metal.complete().expect("nothing failed");
}

/// A prediction between deferred grid applies on the same backend reads
/// back its own output, never a deferred grid command's, and the grids
/// still equal the CPU's.
#[test]
fn predictions_between_deferred_grids_read_back_their_own_output() {
    let mut rng = Rng::new(41);
    let operator = operator(
        Kernels::Spheroidal,
        Basis::ChannelLocal { planes: 3 },
        &mut rng,
    );
    let Some(metal) = metal(&operator) else {
        return;
    };
    let mut metal = metal.with_deferred_grid_completion();
    let prepared = operator
        .prepare_model(&model(&operator, &mut rng), ModelPrescale::Unit)
        .expect("model");
    let planes = PlaneRange::new(0, 3);
    let layout = operator.accumulator_layout(planes, None, ModeSet::DATA_PSF);
    let mut device = MetalBackend::accumulator(layout).expect("device");
    let mut cpu = operator.accumulator(planes, None, ModeSet::DATA_PSF);
    for round in 0..3 {
        let placed = placements(
            &operator,
            &Kernels::Spheroidal.keys(),
            full(&operator),
            SAMPLES + 500 * round,
            3,
            &mut rng,
        );
        let buffer = block(&placed, &mut rng);
        for mode in [Mode::Data, Mode::Psf] {
            CpuBackend::new()
                .apply(
                    &buffer.block(),
                    operator.cf(),
                    Work::Grid {
                        mode,
                        acc: &mut cpu,
                    },
                )
                .expect("cpu");
            metal
                .apply(
                    &buffer.block(),
                    operator.cf(),
                    Work::Grid {
                        mode,
                        acc: &mut device,
                    },
                )
                .expect("metal");
        }
        let mut expected = vec![Complex32::default(); placed.len() * 2];
        let mut predicted = vec![Complex32::default(); placed.len() * 2];
        CpuBackend::new()
            .apply(
                &buffer.block(),
                operator.cf(),
                Work::Predict {
                    model: &prepared,
                    out: &mut expected,
                },
            )
            .expect("cpu predict");
        metal
            .apply(
                &buffer.block(),
                operator.cf(),
                Work::Predict {
                    model: &prepared,
                    out: &mut predicted,
                },
            )
            .expect("metal predict");
        assert_close(&predicted, &expected, &format!("round {round} prediction"));
    }
    assert_same_grids(&device, &cpu, "deferred grids");
    metal.complete().expect("nothing failed");
}

#[test]
fn misplaced_samples_and_host_accumulators_are_refused_before_the_device() {
    refused_before_the_device(false);
    // A refused apply drains the ring even when grid work is deferred, so
    // the device never writes a tile its caller holds again.
    refused_before_the_device(true);
}

fn refused_before_the_device(defer: bool) {
    let mut rng = Rng::new(37);
    let operator = operator(Kernels::Spheroidal, Basis::Constant, &mut rng);
    let Some(metal) = metal(&operator) else {
        return;
    };
    let mut metal = if defer {
        metal.with_deferred_grid_completion()
    } else {
        metal
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
    let keys = Kernels::Spheroidal.keys();
    let mut placed = placements(&operator, &keys, tile, 3_000, 1, &mut rng);
    placed.insert(
        2_500,
        placements(&operator, &keys, outside, 1, 1, &mut rng)[0],
    );
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
    // #991: after a mid-dispatch panic, reset must clear device-only
    // correction storage as well. A tiny fresh signal exposes stale losses.
    let mut tiny = SampleBuffer::new(2);
    for placement in &committed.block().placements[..32] {
        tiny.push(*placement, &[Complex32::new(1e-12, -1e-12); 2], &[1e-12; 2]);
    }
    tiled.reset();
    cpu.reset();
    CpuBackend::new()
        .apply(
            &tiny.block(),
            operator.cf(),
            Work::Grid {
                mode: Mode::Data,
                acc: &mut cpu,
            },
        )
        .expect("fresh cpu");
    metal
        .apply(
            &tiny.block(),
            operator.cf(),
            Work::Grid {
                mode: Mode::Data,
                acc: &mut tiled,
            },
        )
        .expect("fresh device after reset");
    metal.complete().expect("fresh device completed");
    assert_same_grids(
        &tiled,
        &cpu,
        "fresh signal after interrupted apply and reset",
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
