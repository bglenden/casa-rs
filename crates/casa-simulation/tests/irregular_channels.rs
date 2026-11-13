// SPDX-License-Identifier: LGPL-3.0-or-later
//! Actual CASA calculations on stored irregular channel axes.
mod existing_ms_calculations;

#[test]
fn irregular_channel_predictions_and_bandpass_match_actual_casa() {
    existing_ms_calculations::run(
        include_bytes!("data/sm_irregular_channels_casa.json.gz"),
        "sm_irregular_channels",
        "CASA_RS_SIMULATION_IRREGULAR_EXPORT",
    );
}
