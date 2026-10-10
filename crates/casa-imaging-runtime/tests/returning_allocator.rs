// SPDX-License-Identifier: LGPL-3.0-or-later
//! Freed working buffers leave the process: with [`ReturningAllocator`] as
//! the global allocator, the process footprint after large buffers are freed
//! returns to what it was before they were allocated.
//!
//! The footprint is the kernel's physical footprint on macOS (what `vmmap`
//! reports, compressed pages included) and the resident set on Linux. The
//! system allocators fail this law: libmalloc keeps freed large blocks dirty
//! in the process, and glibc serves blocks freed after its mmap threshold
//! rises from an arena it seldom trims.

#![cfg(any(target_os = "macos", target_os = "linux"))]

use casa_imaging_runtime::{RETURNED_BLOCK_BYTES, ReturningAllocator};

#[global_allocator]
static ALLOCATOR: ReturningAllocator = ReturningAllocator;

/// The process's footprint in bytes.
#[cfg(target_os = "macos")]
fn footprint() -> u64 {
    unsafe extern "C" {
        fn proc_pid_rusage(pid: i32, flavor: i32, buffer: *mut u64) -> i32;
    }
    /// `RUSAGE_INFO_V4`; `ri_phys_footprint` is its tenth 64-bit word.
    const FLAVOR: i32 = 4;
    const PHYS_FOOTPRINT: usize = 9;
    let mut info = [0_u64; 40];
    let pid = i32::try_from(std::process::id()).expect("a pid");
    // SAFETY: the buffer is larger than `rusage_info_v4`.
    let status = unsafe { proc_pid_rusage(pid, FLAVOR, info.as_mut_ptr()) };
    assert_eq!(status, 0, "proc_pid_rusage");
    info[PHYS_FOOTPRINT]
}

/// The process's footprint in bytes.
#[cfg(target_os = "linux")]
fn footprint() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").expect("/proc/self/statm");
    let resident: u64 = statm
        .split_ascii_whitespace()
        .nth(1)
        .and_then(|pages| pages.parse().ok())
        .expect("resident pages");
    // SAFETY: `sysconf` has no preconditions.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    resident * u64::try_from(page).expect("a page size")
}

/// Bytes the footprint may differ by beyond the buffers: the test harness's
/// own small allocations and page tables.
const SLACK: u64 = 16 << 20;

/// One footprint measurement at a time: the footprint is the process's.
static MEASUREMENT: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn freed_large_buffers_leave_the_footprint() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    // Per-plane buffers of several sizes above the threshold, every page
    // written, allocated and freed in turn as a pass does.
    let sizes = [
        RETURNED_BLOCK_BYTES,
        4 << 20,
        13 << 20,
        64 << 20,
        RETURNED_BLOCK_BYTES + 12_345,
    ];
    let before = footprint();
    let mut peak = before;
    for round in 0..4 {
        let buffers = sizes
            .iter()
            .map(|&size| vec![round as u8 + 1; size])
            .collect::<Vec<_>>();
        peak = peak.max(footprint());
        assert!(
            buffers
                .iter()
                .all(|buffer| buffer[buffer.len() - 1] == round as u8 + 1)
        );
    }
    let held: u64 = sizes.iter().map(|&size| size as u64).sum();
    assert!(
        peak >= before + held - SLACK,
        "the buffers were resident: {before} -> {peak} for {held} bytes"
    );
    let after = footprint();
    assert!(
        after <= before + SLACK,
        "freed buffers stay in the footprint: {before} before, {after} after, {held} freed"
    );
}

#[test]
fn a_growing_buffer_keeps_its_contents_and_returns_its_pages() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    let before = footprint();
    let mut values = Vec::<u64>::new();
    for value in 0..(48 << 20) / 8 {
        values.push(value);
    }
    assert!(
        values
            .iter()
            .enumerate()
            .all(|(index, value)| *value == index as u64)
    );
    values.truncate(1000);
    values.shrink_to_fit();
    assert_eq!(values.iter().sum::<u64>(), 999 * 1000 / 2);
    drop(values);
    let after = footprint();
    assert!(
        after <= before + SLACK,
        "a grown and freed buffer stays in the footprint: {before} -> {after}"
    );
}
