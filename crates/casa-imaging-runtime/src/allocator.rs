// SPDX-License-Identifier: LGPL-3.0-or-later
//! The process allocator of the imaging apps, which returns freed working
//! buffers to the system at once so that a run's footprint follows the memory
//! its phases admit (plan section 7).
//!
//! Admission ([`crate::admit`]) charges the memory each phase holds live. The
//! system allocators also keep large blocks after they are freed:
//!
//! - macOS's libmalloc caches them dirty in the process. `vmmap` lists them as
//!   "Malloc Large (empty)"; `malloc_zone_statistics` does not count them and
//!   `malloc_zone_pressure_relief` does not release them.
//! - glibc raises its mmap threshold after such a block is freed, then serves
//!   later blocks from an arena that it seldom trims.
//!
//! A run that allocates and frees per-plane buffers therefore holds its live
//! charge plus the buffers it freed. The 512-channel cube's footprint reached
//! 15.8 GB against 12.9 GB admitted; with libmalloc's large cache disabled it
//! was 12.7 GB.
//!
//! [`ReturningAllocator`] maps each block of at least
//! [`RETURNED_BLOCK_BYTES`] from the system and unmaps it when it is freed.
//! Smaller blocks go to the system allocator, where the space a freed block
//! leaves is bounded by its size class. The process that runs imaging
//! (`casars-imager`; the other frontends start it) declares the allocator:
//!
//! ```ignore
//! #[global_allocator]
//! static ALLOCATOR: casa_imaging_runtime::ReturningAllocator =
//!     casa_imaging_runtime::ReturningAllocator;
//! ```
//!
//! A returned block's pages fault in afresh when they are allocated again. On
//! the cube that costs about 3% of the run, mostly system time; pooling the
//! pass's per-plane buffers (IF-10) removes most of it.

use std::alloc::{GlobalAlloc, Layout, System};

/// The smallest block [`ReturningAllocator`] maps from the system; smaller
/// blocks use the system allocator.
pub const RETURNED_BLOCK_BYTES: usize = 1 << 20;

/// A global allocator that returns every block of at least
/// [`RETURNED_BLOCK_BYTES`] to the system when it is freed (module docs).
///
/// Mapped blocks are aligned to the system page, which serves every layout
/// whose alignment is at most a page; a block with a larger alignment uses
/// the system allocator. Fresh mappings are zeroed by the system, so zeroed
/// allocation costs nothing beyond the mapping. Resizing a mapped block
/// keeps it in place when its pages suffice, releases the pages it no longer
/// needs when it shrinks, and otherwise grows it in place where the
/// following address range is free (`mremap` on Linux), copying only when it
/// must move. On platforms without `mmap` every block uses the system
/// allocator.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReturningAllocator;

// SAFETY: every block is either the system allocator's, with the caller's
// layout passed through unchanged, or a private anonymous mapping of at
// least `layout.size()` bytes aligned to the page and at least to
// `layout.align()`. Which of the two a block is depends only on its layout,
// which `dealloc` and `realloc` receive unchanged, so each block returns to
// the allocator that made it.
unsafe impl GlobalAlloc for ReturningAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if mapped(layout) {
            map(layout.size())
        } else {
            // SAFETY: the caller's contract for `alloc`.
            unsafe { System.alloc(layout) }
        }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if mapped(layout) {
            // Fresh anonymous pages read as zero.
            map(layout.size())
        } else {
            // SAFETY: the caller's contract for `alloc_zeroed`.
            unsafe { System.alloc_zeroed(layout) }
        }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if mapped(layout) {
            // SAFETY: `alloc` mapped this block for the same layout.
            unsafe { unmap(pointer, layout.size()) };
        } else {
            // SAFETY: the caller's contract for `dealloc`.
            unsafe { System.dealloc(pointer, layout) };
        }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller guarantees `new_size`, rounded up to the
        // alignment, does not overflow `isize`.
        let resized = unsafe { Layout::from_size_align_unchecked(new_size, layout.align()) };
        match (mapped(layout), mapped(resized)) {
            // SAFETY: the caller's contract for `realloc`.
            (false, false) => unsafe { System.realloc(pointer, layout, new_size) },
            // SAFETY: `pointer` is a mapping made for `layout.size()` bytes.
            (true, true) => unsafe { remap(pointer, layout.size(), new_size) },
            _ => {
                // SAFETY: `resized` has a nonzero size and the caller's
                // alignment.
                let moved = unsafe { self.alloc(resized) };
                if !moved.is_null() {
                    // SAFETY: both blocks hold at least the bytes copied and
                    // are distinct allocations.
                    unsafe {
                        std::ptr::copy_nonoverlapping(pointer, moved, layout.size().min(new_size));
                        self.dealloc(pointer, layout);
                    }
                }
                moved
            }
        }
    }
}

/// Whether `layout`'s block is mapped from the system.
#[cfg(unix)]
fn mapped(layout: Layout) -> bool {
    layout.size() >= RETURNED_BLOCK_BYTES && layout.align() <= page_size()
}

#[cfg(not(unix))]
fn mapped(_layout: Layout) -> bool {
    false
}

/// The system page size, read once.
#[cfg(unix)]
fn page_size() -> usize {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static PAGE: AtomicUsize = AtomicUsize::new(0);
    match PAGE.load(Ordering::Relaxed) {
        0 => {
            // SAFETY: `sysconf` has no preconditions.
            let page = usize::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) })
                .ok()
                .filter(|page| page.is_power_of_two())
                .unwrap_or(4096);
            PAGE.store(page, Ordering::Relaxed);
            page
        }
        page => page,
    }
}

/// `bytes` rounded up to whole pages; `None` past the address space.
#[cfg(unix)]
fn pages(bytes: usize) -> Option<usize> {
    let page = page_size();
    Some(bytes.checked_add(page - 1)? & !(page - 1))
}

/// A fresh private mapping of at least `bytes` bytes, or null.
#[cfg(unix)]
fn map(bytes: usize) -> *mut u8 {
    map_at(std::ptr::null_mut(), bytes)
}

/// A fresh private mapping of at least `bytes` bytes, at `hint` if that range
/// is free and elsewhere otherwise; null when the system refuses.
#[cfg(unix)]
fn map_at(hint: *mut u8, bytes: usize) -> *mut u8 {
    let Some(length) = pages(bytes) else {
        return std::ptr::null_mut();
    };
    // SAFETY: an anonymous private mapping without MAP_FIXED replaces
    // nothing; the hint is advisory.
    let mapping = unsafe {
        libc::mmap(
            hint.cast(),
            length,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANON,
            -1,
            0,
        )
    };
    if mapping == libc::MAP_FAILED {
        std::ptr::null_mut()
    } else {
        mapping.cast()
    }
}

/// Unmap the mapping of `bytes` bytes at `pointer`.
///
/// # Safety
///
/// `pointer` is a mapping [`map`] made for `bytes` bytes, or its tail from a
/// page boundary, and nothing uses it after this call.
#[cfg(unix)]
unsafe fn unmap(pointer: *mut u8, bytes: usize) {
    let length = pages(bytes).expect("a mapped block's pages fit the address space");
    // SAFETY: the caller's contract; the range is whole pages of one mapping.
    let status = unsafe { libc::munmap(pointer.cast(), length) };
    debug_assert_eq!(status, 0, "munmap of a mapped block");
}

/// Resize the mapping of `old` bytes at `pointer` to hold `new` bytes; null,
/// leaving the block unchanged, when the system refuses.
///
/// # Safety
///
/// `pointer` is a mapping [`map`] made for `old` bytes, and `new` is at
/// least [`RETURNED_BLOCK_BYTES`].
#[cfg(unix)]
unsafe fn remap(pointer: *mut u8, old: usize, new: usize) -> *mut u8 {
    let (Some(held), Some(needed)) = (pages(old), pages(new)) else {
        return std::ptr::null_mut();
    };
    if needed <= held {
        if needed < held {
            // SAFETY: the tail from a page boundary of this mapping.
            unsafe { unmap(pointer.add(needed), held - needed) };
        }
        return pointer;
    }
    #[cfg(target_os = "linux")]
    {
        // SAFETY: the whole mapping, moved if it cannot grow in place.
        let moved = unsafe { libc::mremap(pointer.cast(), held, needed, libc::MREMAP_MAYMOVE) };
        if moved == libc::MAP_FAILED {
            std::ptr::null_mut()
        } else {
            moved.cast()
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        // Grow in place when the pages after the block are free.
        // SAFETY: `held` bytes past the start of the mapping is its end.
        let end = unsafe { pointer.add(held) };
        let extension = map_at(end, needed - held);
        if extension == end {
            return pointer;
        }
        if !extension.is_null() {
            // SAFETY: a fresh mapping of `needed - held` bytes, unused.
            unsafe { unmap(extension, needed - held) };
        }
        let moved = map(needed);
        if !moved.is_null() {
            // SAFETY: both mappings hold `old` bytes and are distinct.
            unsafe {
                std::ptr::copy_nonoverlapping(pointer, moved, old);
                unmap(pointer, old);
            }
        }
        moved
    }
}

#[cfg(not(unix))]
fn map(_bytes: usize) -> *mut u8 {
    unreachable!("no block is mapped without mmap")
}

#[cfg(not(unix))]
unsafe fn unmap(_pointer: *mut u8, _bytes: usize) {
    unreachable!("no block is mapped without mmap")
}

#[cfg(not(unix))]
unsafe fn remap(_pointer: *mut u8, _old: usize, _new: usize) -> *mut u8 {
    unreachable!("no block is mapped without mmap")
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    const LARGE: usize = 3 * RETURNED_BLOCK_BYTES + 5;

    fn layout(size: usize, align: usize) -> Layout {
        Layout::from_size_align(size, align).expect("a valid layout")
    }

    /// Fill `bytes` bytes at `pointer` with their offsets modulo 251.
    unsafe fn fill(pointer: *mut u8, bytes: usize) {
        for offset in 0..bytes {
            // SAFETY: the caller's block holds `bytes` bytes.
            unsafe { pointer.add(offset).write((offset % 251) as u8) };
        }
    }

    /// Whether the first `bytes` bytes at `pointer` still hold [`fill`]'s
    /// pattern.
    unsafe fn filled(pointer: *const u8, bytes: usize) -> bool {
        // SAFETY: the caller's block holds `bytes` bytes.
        (0..bytes).all(|offset| unsafe { pointer.add(offset).read() } == (offset % 251) as u8)
    }

    #[test]
    fn large_blocks_are_zeroed_aligned_and_writable() {
        let allocator = ReturningAllocator;
        for align in [1, 8, 64, 4096] {
            let layout = layout(LARGE, align);
            // SAFETY: a nonzero layout, freed with the same layout.
            unsafe {
                let block = allocator.alloc_zeroed(layout);
                assert!(!block.is_null());
                assert_eq!(block as usize % align, 0, "aligned to {align}");
                assert!((0..LARGE).all(|offset| block.add(offset).read() == 0));
                fill(block, LARGE);
                assert!(filled(block, LARGE));
                allocator.dealloc(block, layout);
            }
        }
    }

    #[test]
    fn resizing_keeps_the_contents_across_the_threshold() {
        let allocator = ReturningAllocator;
        let small = 1000;
        let sizes = [
            LARGE,
            LARGE + 7,
            4 * LARGE,
            LARGE - 3,
            RETURNED_BLOCK_BYTES,
            small,
            2 * LARGE,
        ];
        // SAFETY: each step resizes the live block with its current layout
        // and the final block is freed with its own.
        unsafe {
            let mut current = layout(small, 16);
            let mut block = allocator.alloc(current);
            fill(block, small);
            for size in sizes {
                let kept = current.size().min(size);
                block = allocator.realloc(block, current, size);
                assert!(!block.is_null(), "{} -> {size}", current.size());
                assert!(filled(block, kept), "{} -> {size}", current.size());
                current = layout(size, 16);
                fill(block, size);
            }
            allocator.dealloc(block, current);
        }
    }

    #[test]
    fn alignment_beyond_a_page_uses_the_system_allocator() {
        let allocator = ReturningAllocator;
        let align = 4 * page_size();
        let layout = layout(LARGE, align);
        assert!(!mapped(layout));
        // SAFETY: a nonzero layout, freed with the same layout.
        unsafe {
            let block = allocator.alloc(layout);
            assert!(!block.is_null());
            assert_eq!(block as usize % align, 0);
            allocator.dealloc(block, layout);
        }
    }
}
