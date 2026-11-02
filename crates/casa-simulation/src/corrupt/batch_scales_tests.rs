// SPDX-License-Identifier: LGPL-3.0-or-later
//! A batch's table reserves the room it is admitted and works its entries
//! out as the terms would.

use casa_atmosphere::{AbsorptionProfile, AtmosphereProfile, ProfileParameters};
use casa_imaging_runtime::Reservation;
use num_complex::Complex32;

use super::{
    BatchJob, BatchScales, BatchScalesBuilder, INDEX_GROUP_BYTES, PhaseEntry, ScaleEntry, ScaleKey,
};
use crate::corrupt::TsysParameters;
use crate::corrupt::jones::{Chunks, Slots, tropospheric};
use crate::corrupt::troposphere::TroposphericPhase;
use crate::corrupt::tsys::RowNoise;
use crate::corrupt::tsys_atm::{self, TsysAtm, WindowAtmosphere};

/// A `tsys-atm` model of one window of one channel at 230 GHz.
fn one_channel_model() -> TsysAtm {
    let atmosphere = AtmosphereProfile::new(&ProfileParameters::default()).expect("profile");
    let channels = [230e9];
    let grid = tsys_atm::window_grid(&channels).expect("grid");
    let totals = tsys_atm::channel_totals(&atmosphere, &channels);
    let profile = AbsorptionProfile::from_channels(atmosphere, grid, totals).expect("absorption");
    let window = WindowAtmosphere::new(&profile, 1e-3, (0.95, 270.0)).expect("window");
    TsysAtm::new(
        &TsysParameters::default(),
        vec![Some(window)],
        Reservation::none(),
    )
}

/// Run a batch's jobs on this thread.
fn run(jobs: &mut [BatchJob<'_, '_>]) -> Result<(), ()> {
    jobs.iter_mut().for_each(BatchJob::run);
    Ok(())
}

/// The #897 review's case: 2,731 integrations of three antennas, three
/// baselines each, in one channel for one term, so 8,193 entries, just past
/// a power of two. Reserved for its batch, the table moves neither its index
/// nor its pending computations as they are added (growing, each would hold
/// its old allocation and a new one twice the size), and what it allocates
/// is within what it is admitted.
#[test]
fn a_table_reserved_for_its_batch_moves_nothing_and_allocates_its_admission() {
    let model = one_channel_model();
    let window = model.window(0);
    let (integrations, antennas) = (2_731, 3);
    let entries = BatchScales::most_entries(1, 3 * integrations, Some((3, antennas)));
    let mut builder = BatchScalesBuilder::with_capacity((entries, 0));
    let room = (
        builder.scales.index.capacity(),
        builder.scales.pending.capacity(),
    );
    for integration in 0..integrations {
        // Each antenna of each integration at an airmass of its own.
        let airmass = |antenna: usize| 1.0 + (integration * antennas + antenna) as f32 * 1e-4;
        let rows = [(0, 1), (0, 2), (1, 2)].map(|(i, j)| RowNoise {
            diameters_m: [12.0; 2],
            airmass: [airmass(i), airmass(j)],
            exposure_s: 10.0,
        });
        builder.add_rows((&model, 0, window), 1e6, rows.into_iter());
    }
    assert_eq!(builder.scales.pending.len(), integrations * antennas);
    assert_eq!(
        (
            builder.scales.index.capacity(),
            builder.scales.pending.capacity()
        ),
        room,
        "adding the entries moved nothing"
    );
    // std's `HashMap` fills 7/8 of its buckets, a power of two.
    let buckets = (room.0 * 8).div_ceil(7).next_power_of_two();
    let allocated = buckets * (size_of::<(ScaleKey, usize)>() + 1)
        + INDEX_GROUP_BYTES
        + room.1 * size_of::<ScaleEntry<'_>>()
        + builder.scales.pending.len() * size_of::<BatchJob<'_, '_>>()
        + builder.scales.values * size_of::<f32>();
    let admitted = BatchScales::bytes((entries, 0), 1);
    assert!(
        allocated as u64 <= admitted,
        "the table allocates {allocated} bytes, beyond the {admitted} admitted"
    );
    let scales = builder.build(4, run).expect("built");
    let last = RowNoise {
        diameters_m: [12.0; 2],
        airmass: [1.0 + 8_192.0 * 1e-4; 2],
        exposure_s: 10.0,
    };
    let [scale] = scales.get((&model, 0, window), &last, 0) else {
        panic!("one channel");
    };
    let mut expected = [0.0];
    model.antenna_scales(window, (12.0, last.airmass[0]), (1e6, 10.0), &mut expected);
    assert_eq!(*scale, expected[0]);
}

/// A tropospheric term's phasors: one entry per antenna and solution,
/// whatever the rows that need it, each `exp(i·δ·φ(c))`, and its room as
/// reserved.
#[test]
fn phasors_are_one_entry_per_antenna_and_solution() {
    let antennas = 3;
    let mut chunks = Chunks::with_capacity(&[4]);
    for time_s in [0.0, 10.0, 20.0, 30.0] {
        chunks.push(0, time_s, 0);
    }
    let slots = Slots::spanning(chunks.span(), 10.0);
    assert_eq!(slots.count, 4);
    let excess: Vec<f32> = (0..slots.count * antennas)
        .map(|index| 0.01 * index as f32)
        .collect();
    let delays = vec![0.5, 1.0, 2.0];
    let term = tropospheric(
        antennas,
        (chunks.sorted(), &slots),
        TroposphericPhase::new(excess.clone(), vec![Some(delays.clone())]),
    );
    // Every baseline of the four integrations, twice over.
    let rows: Vec<(f64, usize, usize)> = [0.0, 10.0, 20.0, 30.0]
        .into_iter()
        .flat_map(|time_s| [(time_s, 0, 1), (time_s, 0, 2), (time_s, 1, 2)])
        .collect();
    let entries = BatchScales::most_entries(1, 2 * rows.len(), Some((3, antennas)));
    let mut builder = BatchScalesBuilder::with_capacity((0, entries));
    let room = builder.phasors.pending.capacity();
    builder.add_phase_rows((&term, 0), rows.iter().copied());
    builder.add_phase_rows((&term, 0), rows.iter().copied());
    // The slots are centred 5 s after each integration: the first two
    // integrations are as near the first slot as the second, and take the
    // earlier.
    let blocks = [0, 0, 1, 2];
    assert_eq!(builder.phasors.pending.len(), 3 * antennas);
    assert_eq!(builder.phasors.pending.capacity(), room);
    assert!(
        (builder.phasors.pending.len() * size_of::<PhaseEntry<'_>>()) as u64
            <= BatchScales::bytes((0, entries), delays.len())
    );
    let table = builder.build(2, run).expect("built");
    for (block, time_s) in blocks.into_iter().zip([0.0, 10.0, 20.0, 30.0]) {
        assert_eq!(term.block_at(0, time_s), block);
        assert_eq!(slots.nearest(time_s), block);
        for antenna in 0..antennas {
            let delta = f64::from(excess[block * antennas + antenna]);
            let expected: Vec<Complex32> = delays
                .iter()
                .map(|delay| {
                    let (sin, cos) = (delta * delay).sin_cos();
                    Complex32::new(cos as f32, sin as f32)
                })
                .collect();
            assert_eq!(table.phasors(&term, (0, block), antenna), expected);
        }
    }
}
