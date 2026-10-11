// SPDX-License-Identifier: LGPL-3.0-or-later
//! A plan pair keeps at most [`PLAN_PAIR_BYTES`] of C heap in FFTW, so the
//! plan cache keeps at most [`casa_fft::PLAN_CACHE_BYTES`].
//!
//! FFTW allocates with the C allocator, which a Rust counting allocator
//! does not see; the law reads every malloc zone's bytes in use before and
//! after a plan pair is made and used, for transforms up to 8192² in both
//! precisions, complex and real, of composite and prime sizes. Each
//! integration test is its own process, so nothing else allocates meanwhile.
//! Transforms past 2048² are planned by estimate, since measuring them
//! takes a minute; measured plans of the same shapes kept at most 0.61 MB
//! (the prime 8191² real transform) when the bound was set.

#![cfg(target_os = "macos")]

use casa_fft::{Fft2, PLAN_PAIR_BYTES, RealFft2};
use num_complex::Complex;

#[repr(C)]
struct Statistics {
    blocks_in_use: u32,
    size_in_use: usize,
    max_size_in_use: usize,
    size_allocated: usize,
}

unsafe extern "C" {
    fn malloc_zone_statistics(zone: *mut std::ffi::c_void, stats: *mut Statistics);
}

/// Bytes in use in every malloc zone.
fn in_use() -> usize {
    let mut stats = Statistics {
        blocks_in_use: 0,
        size_in_use: 0,
        max_size_in_use: 0,
        size_allocated: 0,
    };
    // SAFETY: a null zone sums every zone into `stats`.
    unsafe { malloc_zone_statistics(std::ptr::null_mut(), &mut stats) };
    stats.size_in_use
}

/// Bytes the C heap gains when `transform` makes and uses a plan pair.
fn kept(transform: impl FnOnce()) -> usize {
    let before = in_use();
    transform();
    in_use().saturating_sub(before)
}

/// Shapes past this many cells are planned by estimate.
const MEASURED_CELLS: usize = 2048 * 2048;

#[test]
fn a_plan_pair_keeps_at_most_its_bytes() {
    let complex32 = |shape: [usize; 2]| {
        kept(|| {
            let mut fft = Fft2::<f32>::with_threads(shape, 1).expect("plan");
            if shape[0] * shape[1] > MEASURED_CELLS {
                fft = fft.with_estimated_plan();
            }
            let mut data = vec![Complex::<f32>::default(); shape[0] * shape[1]];
            fft.transform(&mut data, false).expect("forward");
            fft.transform(&mut data, true).expect("inverse");
        })
    };
    let complex64 = |shape: [usize; 2]| {
        kept(|| {
            let mut fft = Fft2::<f64>::with_threads(shape, 1).expect("plan");
            if shape[0] * shape[1] > MEASURED_CELLS {
                fft = fft.with_estimated_plan();
            }
            let mut data = vec![Complex::<f64>::default(); shape[0] * shape[1]];
            fft.transform(&mut data, false).expect("forward");
            fft.transform(&mut data, true).expect("inverse");
        })
    };
    let real32 = |shape: [usize; 2]| {
        kept(|| {
            let mut fft = RealFft2::<f32>::with_threads(shape, 1).expect("plan");
            if shape[0] * shape[1] > MEASURED_CELLS {
                fft = fft.with_estimated_plan();
            }
            let mut data = vec![Complex::<f32>::default(); fft.storage_len()];
            fft.forward(&mut data).expect("forward");
            fft.inverse(&mut data).expect("inverse");
        })
    };
    let real64 = |shape: [usize; 2]| {
        kept(|| {
            let mut fft = RealFft2::<f64>::with_threads(shape, 1).expect("plan");
            if shape[0] * shape[1] > MEASURED_CELLS {
                fft = fft.with_estimated_plan();
            }
            let mut data = vec![Complex::<f64>::default(); fft.storage_len()];
            fft.forward(&mut data).expect("forward");
            fft.inverse(&mut data).expect("inverse");
        })
    };
    let pairs = [
        ("complex f32 1280²", complex32([1280, 1280])),
        ("complex f32 4096²", complex32([4096, 4096])),
        ("complex f64 4916²", complex64([4916, 4916])),
        ("real f32 2047²", real32([2047, 2047])),
        ("real f32 8191²", real32([8191, 8191])),
        ("real f32 8192²", real32([8192, 8192])),
        ("real f64 4097²", real64([4097, 4097])),
    ];
    for (label, bytes) in pairs {
        eprintln!("{label}: {bytes} bytes");
        assert!(
            bytes <= PLAN_PAIR_BYTES,
            "the {label} plan pair keeps {bytes} bytes, more than {PLAN_PAIR_BYTES}"
        );
    }
}
