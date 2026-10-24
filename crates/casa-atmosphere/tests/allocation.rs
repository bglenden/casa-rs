// SPDX-License-Identifier: LGPL-3.0-or-later
//! Counting laws for [`AbsorptionProfile::bytes`]: a counting global
//! allocator measures the heap a profile keeps and the peak while it is
//! built, for serial builds and for profiles assembled from channels a
//! caller computed, including channels handed over in an oversized vector.
//!
//! The counts are per thread, so other tests running in parallel do not
//! disturb them; every measured build runs on the test's own thread.

use casa_atmosphere::absorption::{channel_absorption, channel_totals};
use casa_atmosphere::{
    AbsorptionProfile, AtmosphereProfile, ChannelAbsorption, ChannelTotals, ProfileParameters,
    SpectralGrid,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct Counting;

thread_local! {
    static LIVE: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

fn change(bytes: isize) {
    let _ = LIVE.try_with(|live| {
        let now = live.get() + bytes;
        live.set(now);
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(now)));
    });
}

// SAFETY: every call forwards to the system allocator unchanged; the
// counters only record sizes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            change(layout.size() as isize);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forwarded with the caller's pointer and layout.
        unsafe { System.dealloc(pointer, layout) };
        change(-(layout.size() as isize));
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded with the caller's arguments.
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            change(new_size as isize - layout.size() as isize);
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn live() -> isize {
    LIVE.with(Cell::get)
}

/// Restarts the peak from the current live bytes.
fn reset_peak() {
    let now = live();
    PEAK.with(|peak| peak.set(now));
}

fn peak() -> isize {
    PEAK.with(Cell::get)
}

/// What building a profile from inputs made by `inputs` kept and peaked
/// at, relative to before the inputs existed: (kept, peak during the build
/// call above the bytes alive just before it, bytes the inputs held).
fn measure<C>(
    inputs: impl FnOnce() -> (AtmosphereProfile, SpectralGrid, Option<Vec<C>>),
    build: impl FnOnce(AtmosphereProfile, SpectralGrid, Option<Vec<C>>) -> AbsorptionProfile<C>,
) -> (u64, u64, u64, AbsorptionProfile<C>)
where
    C: casa_atmosphere::absorption::AbsorptionChannel,
{
    let start = live();
    let (atmosphere, grid, channels) = inputs();
    let before = live();
    reset_peak();
    let profile = build(atmosphere, grid, channels);
    let during = peak() - before;
    let kept = live() - start;
    (kept as u64, during as u64, (before - start) as u64, profile)
}

fn atmosphere() -> AtmosphereProfile {
    AtmosphereProfile::new(&ProfileParameters::default()).unwrap()
}

/// A grid grown window by window, so its vector of windows has spare
/// capacity.
fn grown_grid() -> SpectralGrid {
    let mut grid = SpectralGrid::new();
    for (centre, channels) in [
        (90.0e9, 5),
        (230.0e9, 8),
        (345.0e9, 3),
        (557.0e9, 1),
        (690.0e9, 6),
    ] {
        grid.add_regular(channels, channels / 2, centre, 0.25e9)
            .unwrap();
    }
    grid
}

#[test]
fn serial_builds_keep_exactly_their_estimate_and_hold_nothing_more() {
    for grid in [SpectralGrid::single(230.0e9).unwrap(), grown_grid()] {
        let layers = atmosphere().layers().len();
        let total = grid.total_channels();
        let estimate = AbsorptionProfile::<ChannelTotals>::bytes(&grid, layers);
        let (kept, during, inputs, profile) = measure(
            || (atmosphere(), grid.clone(), None),
            |atmosphere, grid, _| AbsorptionProfile::new_totals(atmosphere, grid),
        );
        println!(
            "compact, {total} channels: kept {kept} B, estimate {estimate} B, build peak {during} B above its inputs ({inputs} B)"
        );
        assert_eq!(kept, estimate);
        // The build allocates only what the profile keeps beyond its inputs
        // (and releases the inputs' spare capacity).
        assert!(during <= estimate - inputs, "build peaked at {during} B");
        drop(profile);

        let estimate = AbsorptionProfile::<ChannelAbsorption>::bytes(&grid, layers);
        let (kept, during, inputs, profile) = measure(
            || (atmosphere(), grid.clone(), None),
            |atmosphere, grid, _| AbsorptionProfile::new(atmosphere, grid),
        );
        println!(
            "full, {total} channels: kept {kept} B, estimate {estimate} B, build peak {during} B above its inputs ({inputs} B)"
        );
        assert_eq!(kept, estimate);
        assert!(during <= estimate - inputs, "build peaked at {during} B");
        drop(profile);
    }
}

#[test]
fn assembled_profiles_keep_exactly_their_estimate_whatever_the_vector() {
    for spare in [0, 99_999] {
        let grid = SpectralGrid::single(230.0e9).unwrap();
        let layers = atmosphere().layers().len();
        let estimate = AbsorptionProfile::<ChannelTotals>::bytes(&grid, layers);
        let (kept, during, inputs, profile) = measure(
            || {
                let atmosphere = atmosphere();
                let grid = SpectralGrid::single(230.0e9).unwrap();
                let mut channels = Vec::with_capacity(grid.total_channels() + spare);
                channels.extend(
                    grid.frequencies_hz()
                        .map(|f| channel_totals(&atmosphere, f)),
                );
                (atmosphere, grid, Some(channels))
            },
            |atmosphere, grid, channels| {
                AbsorptionProfile::from_channels(atmosphere, grid, channels.unwrap()).unwrap()
            },
        );
        println!(
            "compact, assembled with {spare} spare channels: kept {kept} B, estimate {estimate} B, inputs held {inputs} B, peak {during} B above them"
        );
        assert_eq!(kept, estimate, "{spare} spare channels");
        // Shrinking the vector at most copies the channels once.
        assert!(
            during <= (size_of::<ChannelTotals>()) as u64,
            "assembly peaked at {during} B"
        );
        drop(profile);
    }

    // The full form, from a grown grid and channels converted from it.
    let grid = grown_grid();
    let layers = atmosphere().layers().len();
    let estimate = AbsorptionProfile::<ChannelAbsorption>::bytes(&grid, layers);
    let (kept, _, _, profile) = measure(
        || {
            let atmosphere = atmosphere();
            let mut channels = Vec::with_capacity(1_000);
            channels.extend(
                grid.frequencies_hz()
                    .map(|f| channel_absorption(&atmosphere, f)),
            );
            (atmosphere, grid.clone(), Some(channels))
        },
        |atmosphere, grid, channels| {
            AbsorptionProfile::from_channels(atmosphere, grid, channels.unwrap()).unwrap()
        },
    );
    println!(
        "full, assembled from a grown grid with spare channels: kept {kept} B, estimate {estimate} B"
    );
    assert_eq!(kept, estimate);
    drop(profile);

    // Compact channels converted from full ones are exactly sized too.
    let atmosphere = atmosphere();
    let grid = grown_grid();
    let estimate = AbsorptionProfile::<ChannelTotals>::bytes(&grid, atmosphere.layers().len());
    let full: Vec<ChannelAbsorption> = grid
        .frequencies_hz()
        .map(|f| channel_absorption(&atmosphere, f))
        .collect();
    let start = live();
    let totals: Vec<ChannelTotals> = full.iter().map(ChannelAbsorption::totals).collect();
    let converted = live() - start;
    let channel_heap = estimate
        - (grid.total_channels() * size_of::<f64>()) as u64
        - size_of_val(grid.windows()) as u64
        - size_of_val(atmosphere.layers()) as u64;
    assert_eq!(converted as u64, channel_heap);
    drop(totals);
}

#[test]
fn a_dropped_profile_returns_every_byte() {
    let start = live();
    let profile = AbsorptionProfile::new(atmosphere(), grown_grid());
    assert!(live() > start);
    drop(profile);
    assert_eq!(live(), start);
}
