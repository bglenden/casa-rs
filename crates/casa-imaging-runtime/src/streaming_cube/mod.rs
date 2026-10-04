// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bounded native storage and execution for the production cube path.

mod bulk_phase;
mod bulk_wave;
#[cfg(test)]
mod execute;
#[cfg(test)]
mod input;
pub(crate) mod metal_plan;
mod metal_wave;
#[cfg(test)]
mod phase;
#[cfg(test)]
mod plan;
#[cfg(test)]
mod prepare;

pub use bulk_phase::{BulkCubePhase as CubePhase, BulkCubeReplay as NativeReplay};

/// Charge process data not already covered by an authority-owned retention
/// permit. Only resident plane payload is a proved physical contribution to
/// the process census; the cache's conservative admission charge is not.
fn enclosing_memory_bytes(
    retained_resident_bytes: u64,
    managed_live_payload_bytes: u64,
) -> std::io::Result<u64> {
    let observed = process_data_bytes()?;
    let enclosing = unreserved_data_bytes(
        observed,
        retained_resident_bytes,
        managed_live_payload_bytes,
    )?;
    eprintln!(
        "streaming_cube_enclosing_process_data_bytes={observed} retained_normal_payload_bytes={retained_resident_bytes} managed_live_payload_bytes={managed_live_payload_bytes} unreserved_bytes={enclosing}"
    );
    Ok(enclosing)
}

fn unreserved_data_bytes(observed: u64, retained: u64, managed: u64) -> std::io::Result<u64> {
    if observed == 0 {
        return Err(std::io::Error::other("process memory census unavailable"));
    }
    let without_legacy = observed.checked_sub(retained).ok_or_else(|| {
        std::io::Error::other("retained normal payload exceeds process data census")
    })?;
    without_legacy
        .checked_sub(managed)
        .ok_or_else(|| std::io::Error::other("managed live payload exceeds process data census"))
}

#[cfg(target_os = "macos")]
fn process_data_bytes() -> std::io::Result<u64> {
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
    // SAFETY: malloc_statistics_t has this C layout. A null zone requests the
    // aggregate of all malloc zones; stats is a writable live output.
    unsafe {
        malloc_zone_statistics(std::ptr::null_mut(), &mut stats);
    }
    Ok(stats.in_use as u64)
}

#[cfg(target_os = "linux")]
fn process_data_bytes() -> std::io::Result<u64> {
    linux_process_data_bytes(&std::fs::read_to_string("/proc/self/status")?)
}

// VmData includes reserved data mappings, so this is a conservative charge,
// not a live-malloc or RSS claim. It does not depend on the process allocator.
// https://man7.org/linux/man-pages/man5/proc_pid_status.5.html
#[cfg(any(target_os = "linux", test))]
fn linux_process_data_bytes(status: &str) -> std::io::Result<u64> {
    let mut fields = status
        .lines()
        .find_map(|line| line.strip_prefix("VmData:"))
        .ok_or_else(|| std::io::Error::other("process status lacks VmData"))?
        .split_whitespace();
    let value = fields.next().and_then(|value| value.parse::<u64>().ok());
    if fields.next() != Some("kB") || fields.next().is_some() {
        return Err(std::io::Error::other("invalid process data census unit"));
    }
    value
        .and_then(|value| value.checked_mul(1024))
        .filter(|value| *value > 0)
        .ok_or_else(|| std::io::Error::other("invalid process data census size"))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn process_data_bytes() -> std::io::Result<u64> {
    Err(std::io::Error::other(
        "native cube process memory census is unavailable on this platform",
    ))
}

#[cfg(test)]
mod memory_tests {
    use super::*;
    use crate::managed_cube_blocks::{CubeResidency, ManagedPlaneArray};

    #[test]
    fn enclosing_data_subtracts_only_proved_live_payload() {
        assert_eq!(unreserved_data_bytes(4096, 1024, 512).unwrap(), 2560);
        assert_eq!(unreserved_data_bytes(4096, 0, 4096).unwrap(), 0);
        assert!(unreserved_data_bytes(4096, 0, 5000).is_err());
        assert!(unreserved_data_bytes(0, 0, 0).is_err());
        assert!(unreserved_data_bytes(4096, 4097, 0).is_err());
        assert!(enclosing_memory_bytes(0, 0).unwrap() > 0);
    }

    #[test]
    fn uniform_plane_credit_preserves_unrelated_memory_across_eviction_and_reload() {
        let directory = tempfile::tempdir().unwrap();
        let manager = CubeResidency::new(1 << 20).unwrap();
        let array =
            ManagedPlaneArray::<f32>::create(manager.clone(), directory.path(), 2, 3, 2, Some(0.0))
                .unwrap();
        let request = array.request(0, true).unwrap();
        let pins = manager.admit(&[request], 0).unwrap();
        array.write(&pins, 0, 0..6).unwrap().finish();
        drop(pins);
        assert_eq!(manager.live_payload_bytes(), 6 * size_of::<f32>());
        assert!(manager.used_bytes() > manager.live_payload_bytes());
        let unrelated = 8192;
        let observed = manager.live_payload_bytes() as u64 + unrelated;
        assert_eq!(
            unreserved_data_bytes(observed, 0, manager.live_payload_bytes() as u64).unwrap(),
            unrelated
        );
        let request = array.request(0, false).unwrap();
        manager.evict_unpinned(request.id).unwrap();
        assert_eq!(manager.live_payload_bytes(), 0);
        assert_eq!(unreserved_data_bytes(observed, 0, 0).unwrap(), observed);
        let pins = manager.admit(&[request], 0).unwrap();
        assert_eq!(array.read(&pins, 0, 0..6).unwrap().len(), 6);
        assert_eq!(manager.live_payload_bytes(), 6 * size_of::<f32>());
    }

    #[test]
    fn linux_data_census_has_explicit_units_and_fail_closed_bounds() {
        assert_eq!(
            linux_process_data_bytes("Name: test\nVmData:\t1234 kB\n").unwrap(),
            1234 * 1024
        );
        for input in [
            "",
            "VmData: 0 kB",
            "VmData: -1 kB",
            "VmData: 1 MB",
            "VmData: 1 kB extra",
            "VmData: 18446744073709551615 kB",
        ] {
            assert!(linux_process_data_bytes(input).is_err(), "{input}");
        }
    }
}
