// SPDX-License-Identifier: LGPL-3.0-or-later
//! The selection resolved to the observation's partitions (#932).
//!
//! Compile names the partitions that hold the request's samples
//! ([`candidates`]): a partition's data description must be selected, its
//! fields must include a selected one, and, under an intent selection, its
//! `OBS_MODE` must be a selected state's. The survey then keeps those that
//! hold a selected sample, and the compiled problem names them
//! ([`casa_imaging_model::PartitionSelection`]). The run reads the same
//! partitions ([`resolve_partitions`]).

use std::collections::BTreeSet;

use casa_imaging_model::{
    CompiledProblem, IdSelection, IntentSelection, RowSelection, VisibilityColumn, WeightColumn,
};
use casa_ms::observation_io::Msv2Observation;
use casa_observation::{
    Catalogue, Correlation, FieldId, FieldSelection, Observation, PartitionEntry, PartitionId,
};

use crate::imaging::{PartitionSelection, SelectedPartition};

/// Speed of light in m/s, for a window's reference wavelength.
const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.0;

/// Why the selection cannot be read through the observation's partitions.
#[derive(Debug, thiserror::Error)]
pub enum PartitionSelectionError {
    /// What the passes do not read.
    #[error("native imaging does not read {0}")]
    Unsupported(&'static str),
    /// A selected product the partition does not hold, or a window
    /// without channels or reference frequency.
    #[error("the selection is inconsistent with the observation: {0}")]
    Inconsistent(String),
}

/// One partition the request selects, before the survey reads its samples.
pub(super) struct Candidate {
    pub(super) id: PartitionId,
    /// Its spectral window number (MSv2 `SPECTRAL_WINDOW` row).
    pub(super) spectral_window: usize,
    /// Its window's reference wavelength, when a UV-distance interval is in
    /// wavelengths.
    pub(super) wavelength_m: Option<f64>,
}

/// The partitions a request selects, and the data group holding the
/// visibility column it images.
pub(super) struct Candidates {
    pub(super) data_group: String,
    pub(super) partitions: Vec<Candidate>,
}

/// The partitions of `observation` holding samples of the data descriptions
/// `ddids` that `rows` may select, in partition order, with the data group
/// holding `visibility`. A partition's fields must include a selected one;
/// its UV distances and times are the survey's to test.
///
/// # Errors
///
/// [`PartitionSelectionError::Unsupported`] for what the passes do not
/// read: `FLOAT_DATA` (no request names it), partitions whose column
/// belongs to different data groups, and an intent selection by state that
/// splits an `OBS_MODE`. [`PartitionSelectionError::Inconsistent`] for a
/// window without a positive reference frequency under a UV-distance
/// interval in wavelengths.
pub(super) fn candidates(
    observation: &Msv2Observation,
    ddids: &[i32],
    rows: &RowSelection,
    visibility: VisibilityColumn,
) -> Result<Candidates, PartitionSelectionError> {
    let visibilities = match visibility {
        VisibilityColumn::FloatData => {
            return Err(PartitionSelectionError::Unsupported("FLOAT_DATA"));
        }
        visibility => column_name(visibility),
    };
    let fields = field_selection(rows.fields());
    let modes = match rows.intents() {
        IntentSelection::All => None,
        IntentSelection::Only(intents) => Some(selected_modes(observation, intents)?),
    };
    let catalogue = observation.catalogue();
    let mut data_group = None;
    let mut partitions = Vec::new();
    for id in catalogue.partition_ids() {
        let entry = catalogue.partition(id);
        let ddid = observation.data_description_id(id);
        if !ddids
            .iter()
            .any(|selected| i64::from(*selected) == i64::from(ddid))
        {
            continue;
        }
        if modes
            .as_ref()
            .is_some_and(|modes| !modes.contains(observation.obs_mode(id)))
        {
            continue;
        }
        if !entry.fields.iter().any(|field| fields.holds(*field)) {
            continue;
        }
        let group = group_of(entry, visibilities)?;
        match &data_group {
            None => data_group = Some(group.to_string()),
            Some(name) if name == group => {}
            Some(_) => {
                return Err(PartitionSelectionError::Unsupported(
                    "partitions whose column belongs to different data groups",
                ));
            }
        }
        partitions.push(Candidate {
            id,
            spectral_window: entry.window.0 as usize,
            wavelength_m: wavelength(catalogue, entry, rows)?,
        });
    }
    Ok(Candidates {
        data_group: data_group.unwrap_or_default(),
        partitions,
    })
}

/// `problem`'s selection resolved to `observation`'s partitions, in
/// partition order: the partitions compile chose, the channels of their
/// windows, their correlations, the data group holding the visibility
/// column, and the per-sample predicates.
///
/// # Errors
///
/// [`PartitionSelectionError::Unsupported`] for several MeasurementSets,
/// `FLOAT_DATA` and partitions whose column belongs to different data
/// groups; [`PartitionSelectionError::Inconsistent`] for a window the
/// selection names no channel of, a correlation a partition does not hold,
/// and a window without a positive reference frequency under a UV-distance
/// interval in wavelengths.
pub(crate) fn resolve_partitions(
    problem: &CompiledProblem,
    observation: &dyn Observation,
) -> Result<PartitionSelection, PartitionSelectionError> {
    let [source] = problem.observation_transaction().read_set().sources() else {
        return Err(PartitionSelectionError::Unsupported(
            "more than one MeasurementSet",
        ));
    };
    let selection = source.selection();
    let visibilities = match source.selected_columns().visibility() {
        VisibilityColumn::FloatData => {
            return Err(PartitionSelectionError::Unsupported("FLOAT_DATA"));
        }
        visibility => column_name(visibility),
    };
    let catalogue = observation.catalogue();
    let rows = selection.rows_filter();
    let mut data_group = None;
    let mut partitions = Vec::with_capacity(selection.partitions().len());
    for selected in selection.partitions() {
        let entry = catalogue.partition(selected.partition());
        let group = group_of(entry, visibilities)?;
        match &data_group {
            None => data_group = Some(group.to_string()),
            Some(name) if name == group => {}
            Some(_) => {
                return Err(PartitionSelectionError::Unsupported(
                    "partitions whose column belongs to different data groups",
                ));
            }
        }
        let window = selected.spectral_window_id();
        let channels = selection
            .spectral_windows()
            .iter()
            .find(|selected| selected.spectral_window_id() == window)
            .map(|selected| selected.channel_indices().to_vec())
            .filter(|channels| !channels.is_empty())
            .ok_or_else(|| {
                PartitionSelectionError::Inconsistent(format!(
                    "spectral window {window} has no selected channel"
                ))
            })?;
        let correlations: Vec<Correlation> = selected
            .products()
            .iter()
            .map(|product| product.correlation_type())
            .collect();
        if let Some(missing) = correlations
            .iter()
            .find(|product| entry.correlation_index(**product).is_none())
        {
            return Err(PartitionSelectionError::Inconsistent(format!(
                "spectral window {window} lacks {missing:?}"
            )));
        }
        partitions.push(SelectedPartition {
            id: selected.partition(),
            spectral_window: window,
            channels,
            correlations,
            wavelength_m: wavelength(catalogue, entry, rows)?,
        });
    }
    Ok(PartitionSelection {
        data_group: data_group.ok_or(PartitionSelectionError::Unsupported(
            "a selection no partition holds",
        ))?,
        partitions,
        fields: field_selection(rows.fields()),
        uv: rows.uv_distances().clone(),
    })
}

/// The fields `fields` selects, as the boundary names them.
pub(super) fn field_selection(fields: &IdSelection) -> FieldSelection {
    match fields {
        IdSelection::All => FieldSelection::All,
        IdSelection::Only(ids) => FieldSelection::Only(ids.iter().copied().map(FieldId).collect()),
    }
}

/// The name of `entry`'s data group whose visibilities are `visibilities`.
fn group_of<'e>(
    entry: &'e PartitionEntry,
    visibilities: &str,
) -> Result<&'e str, PartitionSelectionError> {
    entry
        .data_groups
        .iter()
        .find(|group| group.visibilities == visibilities)
        .map(|group| group.name.as_str())
        .ok_or(PartitionSelectionError::Unsupported(
            "a visibility column without its data group",
        ))
}

/// `entry`'s window's reference wavelength, when `rows` tests a UV distance
/// in wavelengths.
fn wavelength(
    catalogue: &Catalogue,
    entry: &PartitionEntry,
    rows: &RowSelection,
) -> Result<Option<f64>, PartitionSelectionError> {
    if !rows.uv_distances().needs_wavelengths() {
        return Ok(None);
    }
    let window = entry.window.0;
    catalogue
        .windows
        .get(window as usize)
        .and_then(|window| window.reference_frequency.as_ref())
        .map(casa_types::measures::frequency::MFrequency::hz)
        .filter(|hz| hz.is_finite() && *hz > 0.0)
        .map(|hz| Some(SPEED_OF_LIGHT_M_S / hz))
        .ok_or_else(|| {
            PartitionSelectionError::Inconsistent(format!(
                "spectral window {window} has no positive reference frequency"
            ))
        })
}

/// MAIN's column that holds `visibility`.
fn column_name(visibility: VisibilityColumn) -> &'static str {
    match visibility {
        VisibilityColumn::Data => "DATA",
        VisibilityColumn::CorrectedData => "CORRECTED_DATA",
        VisibilityColumn::FloatData => "FLOAT_DATA",
    }
}

/// The weights the partitions `ids` of `observation` hold beside
/// `visibility`: `WEIGHT_SPECTRUM` when any of them reads it, else
/// `WEIGHT`. The MSv2 adapter decides each partition's by CASA's rule, at
/// the first of its rows, and the passes read each partition's own
/// weights; this names them in the problem's manifest.
pub(super) fn stored_weights(
    catalogue: &Catalogue,
    ids: impl IntoIterator<Item = PartitionId>,
    visibility: VisibilityColumn,
) -> WeightColumn {
    let column = column_name(visibility);
    let spectral = ids.into_iter().any(|id| {
        catalogue
            .partition(id)
            .data_groups
            .iter()
            .any(|group| group.visibilities == column && group.weights == "WEIGHT_SPECTRUM")
    });
    if spectral {
        WeightColumn::WeightSpectrum
    } else {
        WeightColumn::Weight
    }
}

/// The `OBS_MODE`s the MSv2 adapter partitions the rows of the states
/// `selected` names by ([`Msv2Observation::state_mode`]): a partition is
/// read when its mode ([`Msv2Observation::obs_mode`]) is one of them.
///
/// The adapter keys partitions by the whole `OBS_MODE` string, not by
/// `STATE_ID`, so a partition holds the rows of every state with its mode,
/// and the rows without a state (`STATE_ID` -1 or beyond `STATE`) share the
/// empty mode. A selection is refused unless the partitions it reads hold
/// its states' rows alone: no state that MAIN's rows hold and the selection
/// leaves out has a selected state's mode.
fn selected_modes(
    observation: &Msv2Observation,
    selected: &[casa_imaging_model::ResolvedIntent],
) -> Result<BTreeSet<String>, PartitionSelectionError> {
    let chosen = selected
        .iter()
        .map(|intent| i64::from(intent.state_id()))
        .collect::<BTreeSet<_>>();
    let modes = selected
        .iter()
        .map(|intent| {
            let state = i32::try_from(intent.state_id()).unwrap_or(-1);
            observation.state_mode(state).to_string()
        })
        .collect::<BTreeSet<_>>();
    let shared = observation.main_states().iter().any(|state| {
        !chosen.contains(&i64::from(*state)) && modes.contains(observation.state_mode(*state))
    });
    if shared {
        return Err(PartitionSelectionError::Unsupported(
            "an intent selection that takes some of the states sharing an OBS_MODE (the MSv2 \
             adapter partitions their rows together)",
        ));
    }
    Ok(modes)
}
