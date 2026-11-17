// SPDX-License-Identifier: LGPL-3.0-or-later
//! Opt-in phase evidence without changing CASA's return values.

pub(crate) struct Phase {
    name: &'static str,
    start: Option<std::time::Instant>,
}

impl Phase {
    pub(crate) fn start(name: &'static str) -> Self {
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
