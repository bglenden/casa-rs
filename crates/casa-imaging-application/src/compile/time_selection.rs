// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA's `timerange` resolved against a MeasurementSet, as `MSSelection`
//! resolves it for `tclean` (casacore `aaf72eb7`).
//!
//! `SynthesisImagerVi2::selectData` always sets a field and a spw
//! expression (`*` when empty), and `MSSelection::toTableExprNode` parses
//! the time expression last, with the other expressions as its
//! `otherTens`. A partial time (`09:30:00`, `>10:00`) therefore takes its
//! missing date and time from the first MAIN row, in physical order, that
//! the other expressions select, and later comma terms from row 0
//! (`MSTimeParse::getDefaults`; [`casa_ms::TimeSelectorDefaults`]). The
//! grammar itself is the MeasurementSet owner's port
//! ([`casa_ms::TimeSelector`]); this module finds those rows and keeps the
//! resolved conditions, which the survey and the run test at each time.

use std::collections::BTreeSet;

use casa_imaging_model::{IntentSelection, RowSelection, TimeCondition, TimeSelection};
use casa_ms::observation_io::{MainRowKeys, Msv2Observation};
use casa_ms::{TimeDefaults, TimeSelector, TimeSelectorDefaults};
use casa_observation::{ObservationError, StateId};

use super::PrepareError;
use super::opening::OpenedStore;
use super::selection::WindowChoice;
use crate::ImagingRequest;

const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.0;

/// The times `request`'s `timerange` selects in `store`, with the other
/// selections already resolved (`windows`, `rows`, `states`).
///
/// # Errors
///
/// A time expression CASA refuses; a `timerange` of an MSv4 processing set
/// ([`PrepareError::TimeRangeWithoutMainRows`]); an empty MAIN table, which
/// has no logical row zero; a MAIN column that cannot be read.
pub(super) fn resolve_times(
    request: &ImagingRequest,
    store: &OpenedStore,
    windows: &WindowChoice,
    (rows, states): (&RowSelection, &BTreeSet<StateId>),
) -> Result<TimeSelection, PrepareError> {
    let Some(text) = request.timerange.as_deref() else {
        return Ok(TimeSelection::All);
    };
    let selector = TimeSelector::parse(text)?;
    if selector.selects_all() {
        return Ok(TimeSelection::All);
    }
    let observation = store.msv2().ok_or(PrepareError::TimeRangeWithoutMainRows)?;
    if observation.measurement_set().row_count() == 0 {
        return Err(casa_ms::MsError::InvalidInput(
            "MSTimeParse: No logical \"row zero\" found for time selection".to_string(),
        )
        .into());
    }
    let others = OtherSelections::new(observation, store, windows, (rows, states))?;
    let first = observation
        .first_main_row(|keys| others.hold(keys))?
        .unwrap_or(0);
    let defaults = |row: usize| -> Result<TimeDefaults, PrepareError> {
        let (time_mjd_s, exposure_s) = observation.main_row_time_exposure(row)?;
        Ok(TimeDefaults {
            time_mjd_s,
            exposure_s,
        })
    };
    let resolved = selector.resolve(TimeSelectorDefaults {
        first_term: defaults(first)?,
        later_terms: defaults(0)?,
    })?;
    Ok(TimeSelection::AnyOf(
        resolved
            .terms()
            .iter()
            .map(|term| condition(term.condition))
            .collect(),
    ))
}

/// The model's copy of one resolved condition.
fn condition(condition: casa_ms::TimeCondition) -> TimeCondition {
    match condition {
        casa_ms::TimeCondition::Near {
            time_s,
            tolerance_s,
        } => TimeCondition::Near {
            time_s,
            tolerance_s,
        },
        casa_ms::TimeCondition::AtOrAfter { time_s } => TimeCondition::AtOrAfter { time_s },
        casa_ms::TimeCondition::AtOrBefore { time_s } => TimeCondition::AtOrBefore { time_s },
        casa_ms::TimeCondition::Within { lower_s, upper_s } => {
            TimeCondition::Within { lower_s, upper_s }
        }
        casa_ms::TimeCondition::EdgeInclusive {
            lower_s,
            upper_s,
            edge_s,
        } => TimeCondition::EdgeInclusive {
            lower_s,
            upper_s,
            edge_s,
        },
    }
}

/// The selections other than time, as tests of one MAIN row: CASA's
/// `otherTens`.
struct OtherSelections<'a> {
    observation: &'a Msv2Observation,
    rows: &'a RowSelection,
    states: &'a BTreeSet<StateId>,
    /// Each `DATA_DESCRIPTION` row's selection by spw (or `ddid`), and its
    /// window's reference wavelength when a UV distance is in wavelengths.
    descriptions: Vec<(bool, Option<f64>)>,
}

impl<'a> OtherSelections<'a> {
    fn new(
        observation: &'a Msv2Observation,
        store: &OpenedStore,
        windows: &WindowChoice,
        (rows, states): (&'a RowSelection, &'a BTreeSet<StateId>),
    ) -> Result<Self, PrepareError> {
        let table = observation.measurement_set().data_description()?;
        let catalogue = store.catalogue();
        let mut descriptions = Vec::with_capacity(table.row_count());
        for row in 0..table.row_count() {
            let window = table.spectral_window_id(row)?;
            let selected = match windows {
                WindowChoice::All => true,
                WindowChoice::Windows(set) => u32::try_from(window).is_ok_and(|w| set.contains(&w)),
                WindowChoice::DataDescriptions(ddids) => {
                    u32::try_from(row).is_ok_and(|r| ddids.contains(&r))
                }
            };
            let wavelength = usize::try_from(window)
                .ok()
                .and_then(|window| catalogue.windows.get(window))
                .and_then(|window| window.reference_frequency.as_ref())
                .map(casa_types::measures::frequency::MFrequency::hz)
                .filter(|hz| hz.is_finite() && *hz > 0.0)
                .map(|hz| SPEED_OF_LIGHT_M_S / hz);
            descriptions.push((selected, wavelength));
        }
        Ok(Self {
            observation,
            rows,
            states,
            descriptions,
        })
    }

    /// Whether the other selections together select MAIN row `keys`.
    fn hold(&self, keys: &MainRowKeys) -> Result<bool, ObservationError> {
        let Some(&(window_selected, wavelength)) = usize::try_from(keys.data_description)
            .ok()
            .and_then(|row| self.descriptions.get(row))
        else {
            return Ok(false);
        };
        let rows = self.rows;
        let antennas = (u32::try_from(keys.antenna1), u32::try_from(keys.antenna2));
        let held = window_selected
            && rows.fields().holds(i64::from(keys.field))
            && rows.scans().holds(i64::from(keys.scan))
            && rows.observations().holds(i64::from(keys.observation))
            && match antennas {
                (Ok(antenna1), Ok(antenna2)) => rows.baselines().holds(antenna1, antenna2),
                _ => rows.baselines().is_all(),
            }
            && self.state_holds(keys.state);
        if !held || matches!(rows.uv_distances(), casa_imaging_model::UvSelection::All) {
            return Ok(held);
        }
        let uvw = self.observation.main_row_uvw(keys.row)?;
        Ok(rows.uv_distances().holds(uvw, wavelength))
    }

    /// Whether the intent selection holds a row of `STATE_ID` `state`: a
    /// selected state, or, when it selects by mode alone, a selected mode.
    fn state_holds(&self, state: i32) -> bool {
        match self.rows.intents() {
            IntentSelection::All => true,
            IntentSelection::Only(modes) if self.states.is_empty() => modes
                .binary_search_by(|mode| mode.as_str().cmp(self.observation.state_mode(state)))
                .is_ok(),
            IntentSelection::Only(_) => {
                u32::try_from(state).is_ok_and(|state| self.states.contains(&StateId(state)))
            }
        }
    }
}
