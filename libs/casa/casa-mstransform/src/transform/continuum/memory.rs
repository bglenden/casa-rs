// SPDX-License-Identifier: LGPL-3.0-or-later
//! Measure the live scratch of an actual continuum fit on its worker thread.

use super::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct CountingHeap;

thread_local! {
    static COUNTS: Cell<Option<(isize, isize)>> = const { Cell::new(None) };
}

fn count(bytes: isize) {
    let _ = COUNTS.try_with(|counts| {
        if let Some((live, peak)) = counts.get() {
            let live = live + bytes;
            counts.set(Some((live, peak.max(live))));
        }
    });
}

// SAFETY: every allocation and deallocation is forwarded unchanged to System.
unsafe impl GlobalAlloc for CountingHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplies a valid layout.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            count(layout.size() as isize);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout are those of this allocation.
        unsafe { System.dealloc(ptr, layout) };
        count(-(layout.size() as isize));
    }
}

#[global_allocator]
static ALLOCATOR: CountingHeap = CountingHeap;

#[test]
fn continuum_fit_peak_stays_within_its_admission() {
    for (channels, order) in [(4097, 0), (4097, 1), (4097, 4), (1, 0), (3, 2), (17, 16)] {
        check_peak(channels, order);
    }
}

fn check_peak(channels: usize, order: usize) {
    let fit = ContinuumFit {
        options: ContinuumFitOptions {
            fit_spw: String::new(),
            order,
            want_continuum: true,
            reweight: true,
            iterations: 2,
            threads: 1,
        },
        windows: BTreeMap::from([(
            0,
            FitWindow {
                channels: (0..channels).collect(),
                frequencies: (0..channels).map(|i| i as f64 / channels as f64).collect(),
                fit_channels: vec![true; channels],
            },
        )]),
    };
    let mut values = ArrayValue::Complex32(
        ndarray::Array2::from_shape_fn((1, channels), |(_, i)| {
            Complex32::new((i % 11) as f32, (i % 7) as f32)
        })
        .into_dyn(),
    );
    let flags = ArrayValue::Bool(ndarray::Array2::from_elem((1, channels), false).into_dyn());
    let weight = ArrayValue::Float32(ndarray::array![1.0].into_dyn());
    let noise = Noise {
        scalar: Some(&weight),
        spectrum: None,
        sigma: false,
    };
    COUNTS.set(Some((0, 0)));
    let result = fit.windows[&0].apply(&mut values, &flags, noise, &fit.options, true);
    let (_, peak) = COUNTS.replace(None).unwrap();
    result.unwrap();
    let admitted = channels * fit.working_bytes_per_sample();
    eprintln!(
        "continuum fit {channels} channels order {order}: peak {peak} bytes, admitted {admitted} bytes"
    );
    assert!(
        peak as usize <= admitted,
        "peak {peak}, admitted {admitted}"
    );
}
