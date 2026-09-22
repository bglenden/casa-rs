// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bounded native storage and execution for the production cube path.

mod execute;
mod input;
pub(crate) mod normal;
mod phase;
mod plan;
mod prepare;

pub use phase::{InitialCube as CubePhase, NativeReplay};

/// Charge enclosing process data in addition to the phase's explicit allocations.
/// Subtract only live normal payload backed by authority-owned retention permits.
fn enclosing_memory_bytes(retained_resident_bytes: u64) -> std::io::Result<u64> {
    let observed = process_data_bytes()?;
    let enclosing = unreserved_data_bytes(observed, retained_resident_bytes)?;
    eprintln!(
        "streaming_cube_enclosing_process_data_bytes={observed} retained_normal_payload_bytes={retained_resident_bytes} unreserved_bytes={enclosing}"
    );
    Ok(enclosing)
}

fn unreserved_data_bytes(observed: u64, retained: u64) -> std::io::Result<u64> {
    if observed == 0 {
        return Err(std::io::Error::other("process memory census unavailable"));
    }
    observed
        .checked_sub(retained)
        .ok_or_else(|| std::io::Error::other("retained normal payload exceeds process data census"))
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

    #[test]
    fn enclosing_data_subtracts_only_retained_payload() {
        assert_eq!(unreserved_data_bytes(4096, 1024).unwrap(), 3072);
        assert_eq!(unreserved_data_bytes(4096, 4096).unwrap(), 0);
        assert!(unreserved_data_bytes(0, 0).is_err());
        assert!(unreserved_data_bytes(4096, 4097).is_err());
        assert!(enclosing_memory_bytes(0).unwrap() > 0);
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
