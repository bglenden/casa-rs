// SPDX-License-Identifier: LGPL-3.0-or-later
//! This binary's global allocator: it counts the live heap, its largest
//! value and the most it went beyond the process's reservations, and apart
//! from them the blocks that are not a write batch's.
//!
//! `close` takes each write batch's buffers in a span of its own, which the
//! pass summary's subscriber marks
//! ([`super::pass_summary::in_visibility_batch`]). A block allocated there
//! is tagged as the batch's in a header before the caller's bytes, so its
//! free is counted alike on whichever thread drops it. How many batches are
//! live at `close`'s peak is the writer's pace: one to three on a loaded
//! host, 71.6 MB each at 4,096 channels (#1009). The batches stay in
//! [`LIVE`], [`PEAK`] and [`EXCESS`], and so within the reservations, which
//! hold them to the writer's plan; [`OWN_PEAK`] is what `close` holds apart
//! from them.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use casa_imaging_runtime::reserved_memory;

use super::pass_summary::in_visibility_batch;

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;

/// The system allocator, counting live bytes and their largest value, and
/// apart from them the bytes of blocks that are not a write batch's.
struct CountingHeap;

pub(super) static LIVE: AtomicU64 = AtomicU64::new(0);
pub(super) static PEAK: AtomicU64 = AtomicU64::new(0);
/// The most the live bytes exceeded what the process's reservations held,
/// at any allocation: a heap within its admission at every moment, not only
/// at the two peaks.
pub(super) static EXCESS: AtomicI64 = AtomicI64::new(i64::MIN);
/// Live bytes of the blocks that are not a write batch's, and their largest
/// value.
pub(super) static OWN_LIVE: AtomicU64 = AtomicU64::new(0);
pub(super) static OWN_PEAK: AtomicU64 = AtomicU64::new(0);
/// Bytes allocated as a write batch's: more than none once a batch is
/// told apart.
pub(super) static BATCH_ALLOCATED: AtomicU64 = AtomicU64::new(0);

/// Count a new block of `bytes`, a write batch's if `batch`.
fn grow(bytes: u64, batch: bool) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
    EXCESS.fetch_max(live as i64 - reserved_memory() as i64, Ordering::Relaxed);
    if batch {
        BATCH_ALLOCATED.fetch_add(bytes, Ordering::Relaxed);
    } else {
        let own = OWN_LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
        OWN_PEAK.fetch_max(own, Ordering::Relaxed);
    }
}

/// Count the free of a block of `bytes`, a write batch's if `batch`.
fn shrink(bytes: u64, batch: bool) {
    LIVE.fetch_sub(bytes, Ordering::Relaxed);
    if !batch {
        OWN_LIVE.fetch_sub(bytes, Ordering::Relaxed);
    }
}

/// What the live bytes exceed the reservations by now.
pub(super) fn excess_now() -> i64 {
    LIVE.load(Ordering::Relaxed) as i64 - reserved_memory() as i64
}

/// The header's bytes before a block of `layout`: the tag, padded to keep
/// the caller's bytes aligned.
fn header(layout: Layout) -> usize {
    layout.align().max(16)
}

/// What the system allocates for `size` bytes of `layout` behind a header.
fn widened(layout: Layout, size: usize) -> Option<Layout> {
    let header = header(layout);
    Layout::from_size_align(size.checked_add(header)?, header).ok()
}

/// Write whether the block at `base` is a write batch's into its header,
/// and return the caller's bytes.
///
/// # Safety
///
/// `base` starts a block of at least `header` bytes aligned to `header`,
/// which is at least 16.
unsafe fn tag(base: *mut u8, header: usize, batch: bool) -> *mut u8 {
    // SAFETY: the caller's bytes start `header` bytes in, and the tag is
    // the aligned word before them.
    unsafe {
        let pointer = base.add(header);
        pointer.cast::<u64>().sub(1).write(u64::from(batch));
        pointer
    }
}

/// The start of the block whose caller's bytes are at `pointer`, and
/// whether it is a write batch's.
///
/// # Safety
///
/// `pointer` came from this allocator for `layout`.
unsafe fn untag(pointer: *mut u8, layout: Layout) -> (*mut u8, bool) {
    // SAFETY: `tag` wrote the word before the caller's bytes, `header`
    // bytes into the block.
    unsafe {
        (
            pointer.sub(header(layout)),
            pointer.cast::<u64>().sub(1).read() != 0,
        )
    }
}

impl CountingHeap {
    /// Allocate `layout` behind a header through `allocate`, and tag and
    /// count it.
    ///
    /// # Safety
    ///
    /// `allocate` returns null or a block of the layout it is given.
    unsafe fn tagged(layout: Layout, allocate: impl FnOnce(Layout) -> *mut u8) -> *mut u8 {
        let Some(wide) = widened(layout, layout.size()) else {
            return std::ptr::null_mut();
        };
        let base = allocate(wide);
        if base.is_null() {
            return base;
        }
        let batch = in_visibility_batch();
        grow(layout.size() as u64, batch);
        // SAFETY: `base` is a block of `wide`, which holds the header before
        // the caller's bytes, at its alignment.
        unsafe { tag(base, header(layout), batch) }
    }
}

// SAFETY: forwards to `System` with the caller's layout widened by a
// header before the caller's bytes, aligned as the caller asked, and adds
// atomic bookkeeping only.
unsafe impl GlobalAlloc for CountingHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc`, on `wide`, which is
        // larger; `System` returns null or a block of it.
        unsafe { Self::tagged(layout, |wide| System.alloc(wide)) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc_zeroed`, on `wide`, which
        // is larger; `System` returns null or a block of it.
        unsafe { Self::tagged(layout, |wide| System.alloc_zeroed(wide)) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract for `dealloc`: `pointer` came from
        // `alloc` for `layout`, which widened it alike.
        unsafe {
            let (base, batch) = untag(pointer, layout);
            let wide = widened(layout, layout.size()).unwrap_unchecked();
            System.dealloc(base, wide);
            shrink(layout.size() as u64, batch);
        }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let (old, new) = (layout.size() as u64, new_size as u64);
        if widened(layout, new_size).is_none() {
            return std::ptr::null_mut();
        }
        // SAFETY: the caller's contract for `realloc`, on the block behind
        // `pointer` widened alike; the header moves with the block.
        unsafe {
            let (base, was_batch) = untag(pointer, layout);
            let wide = widened(layout, layout.size()).unwrap_unchecked();
            let moved = System.realloc(base, wide, new_size + header(layout));
            if moved.is_null() {
                return moved;
            }
            // A write batch's block stays the batch's, and one grown while
            // a batch's buffers are taken becomes it.
            let batch = was_batch || in_visibility_batch();
            if moved != base {
                // Old and new were held at once while the block moved.
                PEAK.fetch_max(LIVE.load(Ordering::Relaxed) + new, Ordering::Relaxed);
                if !batch {
                    OWN_PEAK.fetch_max(OWN_LIVE.load(Ordering::Relaxed) + new, Ordering::Relaxed);
                }
            }
            shrink(old, was_batch);
            grow(new, batch);
            tag(moved, header(layout), batch)
        }
    }
}
