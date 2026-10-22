// SPDX-License-Identifier: LGPL-3.0-or-later
//! T0: the cell noise the Metal backend adds on the device
//! (`MetalBackend::add_cell_noise`, #855) against the host's
//! (`casa_numerics::cell_noise::CellNoise::add_to_row`): the same
//! source, the same words and the same operations, so every sample is the
//! same bit for bit. Rows above 2³², autocorrelations, one to four
//! correlations, a channel count that fills no whole lane batch, terms added
//! in order and a term of zero σ are covered. A second law holds the device
//! memory the noise keeps to `casa_imaging_metal::noise_bytes`. The laws
//! skip, saying so, on a host without a Metal device.

mod common;

use casa_imaging_metal::{MetalBackend, MetalNoiseRow, MetalNoiseTerm};
use casa_imaging_operator::Basis;
use casa_numerics::cell_noise::CellNoise;
use common::{ALL_KERNELS, Rng, metal, operator};
use num_complex::Complex32;

/// Channels of each row: 37, so a row is two lane batches and a partial
/// one on the host.
const CHANNELS: usize = 37;

#[test]
fn the_device_adds_the_cell_noise_the_host_does() {
    let mut rng = Rng::new(61);
    let operator = operator(ALL_KERNELS[0], Basis::Constant, &mut rng);
    let Some(mut backend) = metal(&operator) else {
        return;
    };
    // MAIN rows on both sides of 2³², every fourth an autocorrelation.
    let numbers = [
        0_u64,
        1,
        7,
        1_000_003,
        (1 << 32) - 1,
        1 << 32,
        (1 << 32) + 5,
        u64::MAX,
    ];
    let rows: Vec<MetalNoiseRow> = numbers
        .iter()
        .enumerate()
        .map(|(index, &number)| MetalNoiseRow {
            row: [number as u32, (number >> 32) as u32],
            autocorrelation: u32::from(index % 4 == 3),
        })
        .collect();
    let terms = [
        MetalNoiseTerm {
            key: [11_111, 0x0100_0000],
            sigma: 0.1,
        },
        MetalNoiseTerm {
            key: [11_111, 0x0100_0001],
            sigma: 2.5,
        },
        MetalNoiseTerm {
            key: [0xFFFF_FFFF, 0x0100_00FF],
            sigma: 0.0,
        },
    ];
    for correlations in 1..=4 {
        let samples = rows.len() * CHANNELS * correlations;
        let device = MetalBackend::native_cells(samples).expect("native cells");
        backend
            .add_cell_noise(&device, &rows, &terms, (CHANNELS, correlations))
            .expect("device noise");
        backend.complete().expect("device work");
        let mut host = vec![Complex32::default(); samples];
        for (row, cell) in rows
            .iter()
            .zip(host.chunks_exact_mut(CHANNELS * correlations))
        {
            let number = u64::from(row.row[0]) | (u64::from(row.row[1]) << 32);
            for term in &terms {
                CellNoise::new(term.key, term.sigma).add_to_row(
                    number,
                    row.autocorrelation == 1,
                    correlations,
                    cell,
                );
            }
        }
        // SAFETY: the backend that wrote the cells has completed.
        let found = unsafe { device.host_cells() };
        for (index, (found, expected)) in found.iter().zip(&host).enumerate() {
            assert!(
                found.re.to_bits() == expected.re.to_bits()
                    && found.im.to_bits() == expected.im.to_bits(),
                "{correlations} correlations, sample {index}: device {found} against host \
                 {expected}"
            );
        }
        assert!(
            host.iter().any(|sample| sample.re != 0.0),
            "the cells received noise"
        );
    }
}

/// The backend copies a noise's rows and terms to two device buffers, made
/// at its first noise and grown to the largest, so the device holds exactly
/// `casa_imaging_metal::noise_bytes` of the most rows and the most terms it
/// has been given: none before its first noise, nothing more for a smaller
/// one, and only the grown buffer for a larger.
#[test]
fn the_device_holds_the_noise_bytes_of_the_largest_noise() {
    let mut rng = Rng::new(62);
    let operator = operator(ALL_KERNELS[0], Basis::Constant, &mut rng);
    let Some(mut backend) = metal(&operator) else {
        return;
    };
    let cells = MetalBackend::native_cells(64 * CHANNELS).expect("native cells");
    let rows = |count: usize| {
        (0..count)
            .map(|row| MetalNoiseRow {
                row: [row as u32, 0],
                autocorrelation: 0,
            })
            .collect::<Vec<_>>()
    };
    let terms = [0, 1, 2].map(|ordinal| MetalNoiseTerm {
        key: [5, 0x0100_0000 | ordinal],
        sigma: 1.0,
    });
    let before = casa_imaging_metal::device_bytes();
    // (rows, terms) of each noise, and the most of each so far.
    for ((count, used), (most_rows, most_terms)) in [
        ((1, 1), (1, 1)),
        ((40, 3), (40, 3)),
        ((8, 2), (40, 3)),
        ((64, 1), (64, 3)),
    ] {
        backend
            .add_cell_noise(&cells, &rows(count), &terms[..used], (CHANNELS, 1))
            .expect("device noise");
        backend.complete().expect("device work");
        assert_eq!(
            casa_imaging_metal::device_bytes() - before,
            casa_imaging_metal::noise_bytes(most_rows, most_terms),
            "after {count} rows and {used} terms"
        );
    }
}
