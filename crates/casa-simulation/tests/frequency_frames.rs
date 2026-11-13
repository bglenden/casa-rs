// SPDX-License-Identifier: LGPL-3.0-or-later
//! Actual CASA calculations with distinct channel/reference-frequency frames.
mod existing_ms_calculations;

#[test]
fn distinct_reference_frequency_frames_match_actual_casa() {
    existing_ms_calculations::run(
        include_bytes!("data/sm_frequency_frames_casa.json.gz"),
        "sm_frequency_frames",
        "CASA_RS_SIMULATION_FRAMES_EXPORT",
    );
}
