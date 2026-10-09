// SPDX-License-Identifier: LGPL-3.0-or-later
//! Gate R2 measurement (#653 acceptance): on Metal, one shared grid updated
//! with atomic adds versus region tiles, one backend per owner as the pass
//! runs them, for wide dense complex kernels, with the CPU's region owners
//! for reference. Samples cluster toward the uv centre as real coverage
//! does, which is where atomics contend. Ignored; run with
//! `cargo test -p casa-imaging-metal --release --test r2_measurement -- --ignored --nocapture`.

mod common;

use std::time::Instant;

use casa_imaging_metal::MetalBackend;
use casa_imaging_operator::{
    CellHold, CfKey, CpuBackend, GridAccumulator, GridBackend, MeasurementOperator, Mode, ModeSet,
    Placement, PlaneRange, SampleBuffer, Tile, Work,
};
use common::{Rng, block, metal, wide_dense_operator};

const IMAGE: usize = 1024;
const SAMPLES: usize = 400_000;
const OWNERS: usize = 4;

/// Placements whose anchors are Gaussian about the uv centre (σ = 15% of
/// the grid), with random `w` signs, phases, pointing gradients and cells.
fn centred_placements(
    operator: &MeasurementOperator,
    count: usize,
    rng: &mut Rng,
) -> Vec<Placement> {
    let geometry = operator.geometry();
    let [nx, ny] = geometry.grid_shape();
    let [sx, sy] = geometry.scale();
    let mut hold = CellHold::new();
    let taps = operator.cf().taps(CfKey::default(), &mut hold);
    let half = taps.half_support().map(i64::from);
    let mut out = Vec::with_capacity(count);
    while out.len() < count {
        let radius = (-2.0 * rng.unit().max(1.0e-12).ln()).sqrt() * 0.15;
        let angle = 2.0 * std::f64::consts::PI * rng.unit();
        let u = radius * angle.cos() * nx as f64 / sx.abs();
        let v = radius * angle.sin() * ny as f64 / sy.abs();
        let location = geometry.locate(u, v, taps.oversampling());
        if location.x - half[0] < 0
            || location.x + half[0] >= nx as i64
            || location.y - half[1] < 0
            || location.y + half[1] >= ny as i64
        {
            continue;
        }
        out.push(Placement {
            u,
            v,
            w: rng.signed() * 50.0,
            phase: rng.signed() * std::f64::consts::PI,
            plane: 0,
            spectral: 0.0,
            cf: CfKey {
                group: 0,
                cube: (rng.next_u64() % 2) as u16,
            },
            gradient: [0.1 * rng.signed() as f32, 0.1 * rng.signed() as f32],
        });
    }
    out
}

/// The samples of `buffer` split into `OWNERS` row strips by anchor, each
/// with its tile (the strip plus the kernel's half support).
fn regions(operator: &MeasurementOperator, buffer: &SampleBuffer) -> Vec<(Tile, SampleBuffer)> {
    let geometry = operator.geometry();
    let [nx, ny] = geometry.grid_shape();
    let halo = usize::from(operator.cf().max_half_support()[1]);
    let mut owned: Vec<(Tile, SampleBuffer)> = (0..OWNERS)
        .map(|owner| {
            let first = (owner * ny / OWNERS).saturating_sub(halo);
            let last = ((owner + 1) * ny / OWNERS + halo).min(ny);
            (
                Tile {
                    origin: [0, first],
                    shape: [nx, last - first],
                },
                SampleBuffer::new(2),
            )
        })
        .collect();
    let block = buffer.block();
    let mut hold = CellHold::new();
    for (index, placement) in block.placements.iter().enumerate() {
        let taps = operator.cf().taps(placement.cf, &mut hold);
        let anchor = geometry.locate(placement.u, placement.v, taps.oversampling());
        let row = anchor.y as usize;
        let owner = (0..OWNERS)
            .find(|owner| row < (owner + 1) * ny / OWNERS)
            .expect("every anchor row has an owner");
        owned[owner]
            .1
            .push(*placement, block.values_of(index), block.weights_of(index));
    }
    owned
}

fn grid(
    backend: &mut dyn GridBackend,
    operator: &MeasurementOperator,
    buffer: &SampleBuffer,
    acc: &mut GridAccumulator,
) {
    backend
        .apply(
            &buffer.block(),
            operator.cf(),
            Work::Grid {
                mode: Mode::Data,
                acc,
            },
        )
        .expect("grid");
}

#[test]
#[ignore = "throughput measurement for gate R2"]
fn dense_kernels_on_one_shared_grid_versus_region_tiles() {
    for support in [9_u16, 33] {
        let mut rng = Rng::new(61);
        let operator = wide_dense_operator(IMAGE, support, &mut rng);
        if metal(&operator).is_none() {
            return;
        }
        let placed = centred_placements(&operator, SAMPLES, &mut rng);
        let buffer = block(&placed, &mut rng);
        let planes = PlaneRange::single(0);
        let rate = |seconds: f64| SAMPLES as f64 / seconds / 1.0e6;

        // Warm the pipelines and the kernel tables.
        let mut warm = MetalBackend::new(operator.cf()).expect("backend");
        let mut acc =
            MetalBackend::accumulator(operator.accumulator_layout(planes, None, ModeSet::DATA))
                .expect("device grid");
        grid(&mut warm, &operator, &buffer, &mut acc);

        let mut shared = MetalBackend::new(operator.cf()).expect("backend");
        let mut acc =
            MetalBackend::accumulator(operator.accumulator_layout(planes, None, ModeSet::DATA))
                .expect("device grid");
        let started = Instant::now();
        grid(&mut shared, &operator, &buffer, &mut acc);
        let shared_seconds = started.elapsed().as_secs_f64();

        let owned = regions(&operator, &buffer);
        let region_seconds = |metal: bool| {
            let started = Instant::now();
            std::thread::scope(|scope| {
                for (tile, samples) in &owned {
                    let operator = &operator;
                    scope.spawn(move || {
                        let layout =
                            operator.accumulator_layout(planes, Some(*tile), ModeSet::DATA);
                        if metal {
                            let mut backend = MetalBackend::new(operator.cf()).expect("backend");
                            let mut acc = MetalBackend::accumulator(layout).expect("device tile");
                            grid(&mut backend, operator, samples, &mut acc);
                        } else {
                            let mut acc = GridAccumulator::new(layout, operator.precision());
                            grid(&mut CpuBackend::new(), operator, samples, &mut acc);
                        }
                    });
                }
            });
            started.elapsed().as_secs_f64()
        };
        let metal_regions = region_seconds(true);
        let cpu_regions = region_seconds(false);
        eprintln!(
            "R2 dense {support}×{support} taps, 2 Mueller planes, {SAMPLES} samples on a {}² grid: \
             Metal shared grid {shared_seconds:.3} s ({:.2} M samples/s); Metal {OWNERS} region \
             tiles {metal_regions:.3} s ({:.2} M/s); CPU {OWNERS} region tiles {cpu_regions:.3} s \
             ({:.2} M/s)",
            operator.geometry().grid_shape()[0],
            rate(shared_seconds),
            rate(metal_regions),
            rate(cpu_regions),
        );
    }
}
