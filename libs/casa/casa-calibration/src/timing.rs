// SPDX-License-Identifier: LGPL-3.0-or-later
//! Opt-in phase evidence without changing CASA's return values.

pub struct Phase {
    name: &'static str,
    start: Option<std::time::Instant>,
}

impl Phase {
    pub fn start(name: &'static str) -> Self {
        Self {
            name,
            start: std::env::var_os("CASA_RS_CALIBRATION_TIMING")
                .map(|_| std::time::Instant::now()),
        }
    }
}

impl Drop for Phase {
    fn drop(&mut self) {
        if let Some(start) = self.start {
            eprintln!(
                "calibration_phase {} {:.9}",
                self.name,
                start.elapsed().as_secs_f64()
            );
        }
    }
}

/// Linux's current address-space peak, excluding a parent's pre-exec peak
/// that can remain in `getrusage`. Report only for opt-in measurements.
pub fn report_process_memory() {
    #[cfg(target_os = "linux")]
    if std::env::var_os("CASA_RS_CALIBRATION_TIMING").is_some()
        && let Ok(status) = std::fs::read_to_string("/proc/self/status")
        && let Some(peak) = status.lines().find_map(|line| {
            line.strip_prefix("VmHWM:")?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
        })
    {
        eprintln!("calibration_peak_rss_kib {peak}");
    }
}
