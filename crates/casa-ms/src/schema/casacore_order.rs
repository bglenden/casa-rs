// SPDX-License-Identifier: LGPL-3.0-or-later
//! The order casacore lays out each subtable's required columns in, which a
//! created MeasurementSet follows ([`crate::builder::MeasurementSetBuilder`]).
//!
//! casacore's `MS<Table>::initMaps` (`ms/MeasurementSets/MS<Table>.cc`)
//! adds some columns first, those of fixed shape or holding a measure
//! reference, then every required column in its `MS<Table>Enums` order,
//! skipping those already added; `MeasurementSet::createDefaultSubtables`
//! creates each table from that description. The order is not cosmetic:
//! CASA's `concat` copies some subtables' rows by position
//! (`MSConcat::copyPointing`, `copySysCal`, `copyWeather` call
//! `TableRow::put`, whose conformance check compares the column names in
//! order), so CASA could not append to a MeasurementSet laid out otherwise.
//! Reads are by name and unaffected.

use super::SubtableId;

/// The required columns of subtable `id` in casacore's order.
pub(crate) fn column_order(id: SubtableId) -> &'static [&'static str] {
    match id {
        // OFFSET and POSITION first (MSAntenna.cc).
        SubtableId::Antenna => &[
            "OFFSET",
            "POSITION",
            "TYPE",
            "DISH_DIAMETER",
            "FLAG_ROW",
            "MOUNT",
            "NAME",
            "STATION",
        ],
        SubtableId::DataDescription => &["FLAG_ROW", "POLARIZATION_ID", "SPECTRAL_WINDOW_ID"],
        // POSITION, BEAM_OFFSET, POLARIZATION_TYPE, POL_RESPONSE and
        // RECEPTOR_ANGLE first (MSFeed.cc).
        SubtableId::Feed => &[
            "POSITION",
            "BEAM_OFFSET",
            "POLARIZATION_TYPE",
            "POL_RESPONSE",
            "RECEPTOR_ANGLE",
            "ANTENNA_ID",
            "BEAM_ID",
            "FEED_ID",
            "INTERVAL",
            "NUM_RECEPTORS",
            "SPECTRAL_WINDOW_ID",
            "TIME",
        ],
        // The three directions first (MSField.cc).
        SubtableId::Field => &[
            "DELAY_DIR",
            "PHASE_DIR",
            "REFERENCE_DIR",
            "CODE",
            "FLAG_ROW",
            "NAME",
            "NUM_POLY",
            "SOURCE_ID",
            "TIME",
        ],
        SubtableId::FlagCmd => &[
            "APPLIED", "COMMAND", "INTERVAL", "LEVEL", "REASON", "SEVERITY", "TIME", "TYPE",
        ],
        // APP_PARAMS and CLI_COMMAND first (MSHistory.cc).
        SubtableId::History => &[
            "APP_PARAMS",
            "CLI_COMMAND",
            "APPLICATION",
            "MESSAGE",
            "OBJECT_ID",
            "OBSERVATION_ID",
            "ORIGIN",
            "PRIORITY",
            "TIME",
        ],
        // TIME_RANGE, LOG and SCHEDULE first (MSObservation.cc).
        SubtableId::Observation => &[
            "TIME_RANGE",
            "LOG",
            "SCHEDULE",
            "FLAG_ROW",
            "OBSERVER",
            "PROJECT",
            "RELEASE_DATE",
            "SCHEDULE_TYPE",
            "TELESCOPE_NAME",
        ],
        // DIRECTION first (MSPointing.cc).
        SubtableId::Pointing => &[
            "DIRECTION",
            "ANTENNA_ID",
            "INTERVAL",
            "NAME",
            "NUM_POLY",
            "TARGET",
            "TIME",
            "TIME_ORIGIN",
            "TRACKING",
        ],
        // CORR_TYPE and CORR_PRODUCT first (MSPolarization.cc).
        SubtableId::Polarization => &["CORR_TYPE", "CORR_PRODUCT", "FLAG_ROW", "NUM_CORR"],
        SubtableId::Processor => &["FLAG_ROW", "MODE_ID", "TYPE", "TYPE_ID", "SUB_TYPE"],
        // MEAS_FREQ_REF, CHAN_FREQ, REF_FREQUENCY, CHAN_WIDTH, EFFECTIVE_BW and
        // RESOLUTION first (MSSpectralWindow.cc).
        SubtableId::SpectralWindow => &[
            "MEAS_FREQ_REF",
            "CHAN_FREQ",
            "REF_FREQUENCY",
            "CHAN_WIDTH",
            "EFFECTIVE_BW",
            "RESOLUTION",
            "FLAG_ROW",
            "FREQ_GROUP",
            "FREQ_GROUP_NAME",
            "IF_CONV_CHAIN",
            "NAME",
            "NET_SIDEBAND",
            "NUM_CHAN",
            "TOTAL_BANDWIDTH",
        ],
        SubtableId::State => &[
            "CAL", "FLAG_ROW", "LOAD", "OBS_MODE", "REF", "SIG", "SUB_SCAN",
        ],
        SubtableId::Doppler => &["DOPPLER_ID", "SOURCE_ID", "TRANSITION_ID", "VELDEF"],
        SubtableId::FreqOffset => &[
            "ANTENNA1",
            "ANTENNA2",
            "FEED_ID",
            "INTERVAL",
            "OFFSET",
            "SPECTRAL_WINDOW_ID",
            "TIME",
        ],
        // DIRECTION and PROPER_MOTION first (MSSource.cc).
        SubtableId::Source => &[
            "DIRECTION",
            "PROPER_MOTION",
            "CALIBRATION_GROUP",
            "CODE",
            "INTERVAL",
            "NAME",
            "NUM_LINES",
            "SOURCE_ID",
            "SPECTRAL_WINDOW_ID",
            "TIME",
        ],
        SubtableId::SysCal => &[
            "ANTENNA_ID",
            "FEED_ID",
            "INTERVAL",
            "SPECTRAL_WINDOW_ID",
            "TIME",
        ],
        SubtableId::Weather => &["ANTENNA_ID", "INTERVAL", "TIME"],
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::column_order;
    use crate::schema::{SubtableId, required_columns};

    /// Each order names exactly the subtable's required columns, once.
    #[test]
    fn each_order_is_of_the_required_columns() {
        for id in SubtableId::ALL_REQUIRED
            .iter()
            .chain(SubtableId::ALL_OPTIONAL)
        {
            let order = column_order(*id);
            let ordered: BTreeSet<&str> = order.iter().copied().collect();
            let required: BTreeSet<&str> = required_columns(*id)
                .iter()
                .map(|column| column.name)
                .collect();
            assert_eq!(ordered.len(), order.len(), "{}: a name twice", id.name());
            assert_eq!(ordered, required, "{}", id.name());
        }
    }
}
