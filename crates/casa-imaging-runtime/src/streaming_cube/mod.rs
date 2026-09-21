// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bounded native storage and execution for the replacement cube path.
//! Kept behind the comparison seam until the complete runtime slice is ready.

mod execute;
mod input;
pub(crate) mod normal;
mod phase;
mod plan;
mod prepare;

pub use phase::{InitialCube as CubePhase, NativeReplay};

/// Admit the enclosing process heap in addition to the new wave bound. Only
/// exact live normal-array payload whose owner retains a runtime memory permit
/// is excluded: the authority already charges it. Other reservations remain
/// conservatively double counted; never subtract an estimated capacity.
#[cfg(all(casa_streaming_cube_comparison, not(test), target_os = "macos"))]
fn comparison_live_heap_bytes(retained_resident_bytes: u64) -> std::io::Result<u64> {
    #[repr(C)]
    #[derive(Default)]
    struct Statistics {
        blocks: u32,
        in_use: usize,
        maximum: usize,
        allocated: usize,
    }
    unsafe extern "C" {
        fn malloc_zone_statistics(zone: *mut std::ffi::c_void, stats: *mut Statistics);
    }
    let mut stats = Statistics::default();
    // SAFETY: the SDK's malloc_statistics_t has exactly this C layout. A null
    // zone requests the aggregate of all malloc zones; stats is writable/live.
    unsafe {
        malloc_zone_statistics(std::ptr::null_mut(), &mut stats);
    }
    if stats.in_use == 0 {
        return Err(std::io::Error::other("comparison heap census unavailable"));
    }
    let enclosing = (stats.in_use as u64)
        .checked_sub(retained_resident_bytes)
        .ok_or_else(|| std::io::Error::other("retained normal payload exceeds heap census"))?;
    eprintln!(
        "streaming_cube_enclosing_live_heap_bytes={} retained_normal_payload_bytes={} unreserved_bytes={enclosing}",
        stats.in_use, retained_resident_bytes
    );
    Ok(enclosing)
}

#[cfg(all(casa_streaming_cube_comparison, not(test), not(target_os = "macos")))]
fn comparison_live_heap_bytes(_: u64) -> std::io::Result<u64> {
    Err(std::io::Error::other(
        "comparison enclosing-heap census is currently macOS-only",
    ))
}
