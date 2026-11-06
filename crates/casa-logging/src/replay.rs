// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared replay and output-configuration helpers.

use std::ffi::OsString;
use tracing::Level;

use crate::{LOG_STDERR_PRIORITY_FLAG, LOG_TABLE_FLAG, SECONDS_PER_DAY};

/// Replay an INFO record through the configured sinks, preserving its time.
///
/// If no INFO sink is enabled and `fallback_to_text` is true, returns a
/// timestamped CASA-style text line for the task front end to display.
/// Otherwise returns `None`; an explicitly filtered INFO record is not
/// displayed. The original location and message are passed through unchanged.
#[must_use]
pub fn replay_info_record(
    time_mjd_seconds: f64,
    location: &str,
    message: &str,
    fallback_to_text: bool,
) -> Option<String> {
    if tracing::enabled!(Level::INFO) {
        tracing::info!(
            casa.time_mjd_seconds = time_mjd_seconds,
            casa.location = location,
            "{message}"
        );
        None
    } else if fallback_to_text {
        let timestamp = casa_types::quanta::format::format_time(
            time_mjd_seconds / SECONDS_PER_DAY,
            casa_types::quanta::format::FITS | casa_types::quanta::format::USE_SPACE,
            6,
        );
        Some(format!("{timestamp}\tINFO\t{location}\t{message}"))
    } else {
        None
    }
}

pub(crate) fn output_was_configured(args: &[OsString], table: bool, stderr: bool) -> bool {
    table
        || stderr
        || args.iter().filter_map(|arg| arg.to_str()).any(|arg| {
            [LOG_TABLE_FLAG, LOG_STDERR_PRIORITY_FLAG]
                .iter()
                .any(|flag| {
                    arg == *flag
                        || arg
                            .strip_prefix(*flag)
                            .is_some_and(|rest| rest.starts_with('='))
                })
        })
}
