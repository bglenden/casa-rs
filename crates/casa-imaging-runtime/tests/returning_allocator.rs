// SPDX-License-Identifier: LGPL-3.0-or-later
//! Freed working buffers leave the process: with [`ReturningAllocator`] as
//! the global allocator, the process footprint after large buffers are freed
//! returns to what it was before they were allocated.
//!
//! The footprint is the kernel's physical footprint on macOS (what `vmmap`
//! reports, compressed pages included) and the resident set on Linux. The
//! system allocators fail the first law: libmalloc keeps every freed large
//! block dirty in the process (87 MB of the 87 MB freed on this workstation),
//! and glibc serves blocks freed after its mmap threshold rises from an arena
//! it seldom trims (about 20 MB of it on a Linux runner, a margin of 4 MB
//! over the slack).
//!
//! The allocator's failures are laws too: a resize the system refuses
//! returns null and keeps the block, and an unmap it refuses aborts the
//! process rather than unwinding or keeping the pages. The aborting cases
//! run in child processes of this binary.

#![cfg(any(target_os = "macos", target_os = "linux"))]

use std::alloc::{GlobalAlloc, Layout};
use std::os::unix::process::ExitStatusExt;

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

#[test]
fn a_refused_resize_returns_null_and_keeps_the_block() {
    let layout = Layout::from_size_align(3 * RETURNED_BLOCK_BYTES + 5, 16).expect("a layout");
    // SAFETY: a nonzero layout; the block is resized and freed with it.
    unsafe {
        let block = ALLOCATOR.alloc(layout);
        assert!(!block.is_null());
        for offset in 0..layout.size() {
            block.add(offset).write((offset % 251) as u8);
        }
        // No system maps an exbibyte.
        assert!(ALLOCATOR.realloc(block, layout, 1 << 60).is_null());
        assert!((0..layout.size()).all(|offset| block.add(offset).read() == (offset % 251) as u8));
        ALLOCATOR.dealloc(block, layout);
    }
}

/// Run the ignored test `name` of this binary in a child process; assert
/// that it aborted after saying why.
fn assert_aborts(name: &str) {
    let output = std::process::Command::new(std::env::current_exe().expect("this test binary"))
        .args([
            "--ignored",
            "--exact",
            name,
            "--test-threads=1",
            "--nocapture",
        ])
        .output()
        .expect("the child runs");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        output.status.signal(),
        Some(libc::SIGABRT),
        "{name}: {}; {stderr}",
        output.status
    );
    assert!(
        stderr.contains("munmap of a freed block failed"),
        "{name}: {stderr}"
    );
}

#[test]
fn a_refused_unmap_aborts_the_process() {
    assert_aborts("unmap_refused_by_the_system");
}

/// Free a mapped block through a pointer off its page boundary, which
/// `munmap` refuses with `EINVAL`. Run by
/// `a_refused_unmap_aborts_the_process`.
#[test]
#[ignore = "aborts the process; a_refused_unmap_aborts_the_process runs it in a child"]
fn unmap_refused_by_the_system() {
    let layout = Layout::from_size_align(4 * RETURNED_BLOCK_BYTES, 8).expect("a layout");
    // SAFETY: none: freeing an offset pointer breaks `dealloc`'s contract,
    // to make the system refuse the unmap.
    unsafe {
        let block = ALLOCATOR.alloc(layout);
        assert!(!block.is_null());
        ALLOCATOR.dealloc(block.add(1), layout);
    }
    unreachable!("the refused unmap aborts the process");
}

#[cfg(target_os = "linux")]
#[test]
fn an_unmap_at_the_map_count_limit_aborts_the_process() {
    let limit: u64 = std::fs::read_to_string("/proc/sys/vm/max_map_count")
        .ok()
        .and_then(|limit| limit.trim().parse().ok())
        .unwrap_or(u64::MAX);
    if limit > 1 << 20 {
        eprintln!("vm.max_map_count is {limit}: too many mappings to fill");
        return;
    }
    assert_aborts("unmap_at_the_map_count_limit");
}

/// Free the middle of three adjacent blocks, which the kernel merged into
/// one map entry, while the process holds `vm.max_map_count` entries:
/// splitting the entry needs one more, so `munmap` fails with `ENOMEM`
/// (Astra's probe on #700). Run by
/// `an_unmap_at_the_map_count_limit_aborts_the_process`.
#[cfg(target_os = "linux")]
#[test]
#[ignore = "aborts the process; an_unmap_at_the_map_count_limit_aborts_the_process runs it in a child"]
fn unmap_at_the_map_count_limit() {
    let layout = Layout::from_size_align(RETURNED_BLOCK_BYTES, 8).expect("a layout");
    // SAFETY: nonzero layouts; the middle block is freed with its own.
    let mut blocks = (0..8)
        .map(|_| unsafe { ALLOCATOR.alloc(layout) } as usize)
        .collect::<Vec<_>>();
    blocks.sort_unstable();
    let middle = blocks
        .windows(3)
        .find(|run| {
            run[1] == run[0] + RETURNED_BLOCK_BYTES && run[2] == run[1] + RETURNED_BLOCK_BYTES
        })
        .map(|run| run[1])
        .expect("three adjacent mapped blocks");
    // One-page mappings of alternating protection, which cannot merge,
    // until the kernel refuses another entry.
    let mut protection = libc::PROT_READ;
    loop {
        // SAFETY: a fresh anonymous mapping replaces nothing.
        let page = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                1,
                protection,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if page == libc::MAP_FAILED {
            break;
        }
        protection ^= libc::PROT_READ;
    }
    // SAFETY: `middle` is a block allocated with `layout` and not yet freed.
    unsafe { ALLOCATOR.dealloc(middle as *mut u8, layout) };
    unreachable!("the refused unmap aborts the process");
}
