// SPDX-License-Identifier: LGPL-3.0-or-later
//! The selection resolved to the observation's partitions (#932).
//!
//! Compile names the partitions that hold the request's samples
//! ([`candidates`]): a partition's window must be selected, its fields must
//! include a selected one, and, under an intent selection, its observing
//! mode must be a selected one ([`super::intents`]). The survey then keeps
//! those that hold a selected sample, and the compiled problem names them
//! ([`casa_imaging_model::PartitionSelection`]). The run reads the same
//! partitions ([`resolve_partitions`]).

use std::collections::BTreeSet;

use casa_imaging_model::{
    CompiledProblem, IdSelection, IntentSelection, RowSelection, VisibilityColumn, WeightColumn,
};
use casa_observation::{
    Catalogue, Correlation, FieldId, FieldSelection, Observation, PartitionEntry, PartitionId,
    StateId,
};

use super::intents;
use super::opening::OpenedStore;
use super::selection::{WindowChoice, data_group};
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
    /// Its spectral window number (the catalogue's window; an MSv2
    /// `SPECTRAL_WINDOW` row).
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

/// The partitions of `store` in the windows `windows` selects that `rows`
/// may select, in partition order: a partition's fields must include a
/// selected one and, under an intent selection, its observing mode must be
/// a selected one. A state number in the selection (`states`) must select
/// every state of its partition. A partition's UV distances and times are
/// the survey's to test.
///
/// # Errors
///
/// [`PartitionSelectionError::Unsupported`] for an intent selection by
/// state that splits an `OBS_MODE`.
pub(super) fn selected(
    store: &OpenedStore,
    windows: &WindowChoice,
    (rows, states): (&RowSelection, &BTreeSet<StateId>),
) -> Result<Vec<PartitionId>, PartitionSelectionError> {
    let fields = field_selection(rows.fields());
    let modes = match rows.intents() {
        IntentSelection::All => None,
        IntentSelection::Only(modes) => Some(modes.iter().cloned().collect::<BTreeSet<_>>()),
    };
    let catalogue = store.catalogue();
    let mut selected = Vec::new();
    for id in catalogue.partition_ids() {
        let entry = catalogue.partition(id);
        if !windows.holds(store, id) {
            continue;
        }
        if let Some(modes) = &modes {
            if !modes.contains(&intents::observing_mode(catalogue, entry)) {
                continue;
            }
            if intents::split_state(entry, states) {
                return Err(PartitionSelectionError::Unsupported(
                    "an intent selection that takes some of the states sharing an OBS_MODE (the \
                     MSv2 adapter partitions their rows together)",
                ));
            }
        }
        if entry.fields.iter().any(|field| fields.holds(*field)) {
            selected.push(id);
        }
    }
    Ok(selected)
}

/// The `selected` partitions of `store` ([`selected`]) with the data group
/// holding `visibility`, which each must have.
///
/// # Errors
///
/// [`PartitionSelectionError::Unsupported`] for what the passes do not
/// read: `FLOAT_DATA` (no request names it) and a partition without the data
/// group. [`PartitionSelectionError::Inconsistent`] for a window without a
/// positive reference frequency under a UV-distance interval in
/// wavelengths.
pub(super) fn candidates(
    store: &OpenedStore,
    selected: &[PartitionId],
    rows: &RowSelection,
    visibility: VisibilityColumn,
) -> Result<Candidates, PartitionSelectionError> {
    if visibility == VisibilityColumn::FloatData {
        return Err(PartitionSelectionError::Unsupported("FLOAT_DATA"));
    }
    let group = data_group(visibility);
    let catalogue = store.catalogue();
    let mut partitions = Vec::new();
    for &id in selected {
        let entry = catalogue.partition(id);
        if entry.data_group(group).is_none() {
            return Err(PartitionSelectionError::Unsupported(
                "a selected partition without the imaged data group",
            ));
        }
        partitions.push(Candidate {
            id,
            spectral_window: entry.window.0 as usize,
            wavelength_m: wavelength(catalogue, entry, rows)?,
        });
    }
    Ok(Candidates {
        data_group: group.to_string(),
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
    let visibility = source.selected_columns().visibility();
    if visibility == VisibilityColumn::FloatData {
        return Err(PartitionSelectionError::Unsupported("FLOAT_DATA"));
    }
    let group = data_group(visibility);
    let catalogue = observation.catalogue();
    let rows = selection.rows_filter();
    let mut partitions = Vec::with_capacity(selection.partitions().len());
    for selected in selection.partitions() {
        let entry = catalogue.partition(selected.partition());
        if entry.data_group(group).is_none() {
            return Err(PartitionSelectionError::Unsupported(
                "a selected partition without the imaged data group",
            ));
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
    if partitions.is_empty() {
        return Err(PartitionSelectionError::Unsupported(
            "a selection no partition holds",
        ));
    }
    Ok(PartitionSelection {
        data_group: group.to_string(),
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

/// The weights the partitions `ids` of `store` hold beside `visibility`:
/// `WEIGHT_SPECTRUM` when any of them reads per-channel weights, else
/// `WEIGHT`. The MSv2 adapter decides each partition's by CASA's rule, at
/// the first of its rows; an MSv4's weights are per channel. The passes
/// read each partition's own weights; this names them in the problem's
/// manifest.
pub(super) fn stored_weights(
    store: &OpenedStore,
    ids: impl IntoIterator<Item = PartitionId>,
    visibility: VisibilityColumn,
) -> WeightColumn {
    let group = data_group(visibility);
    let catalogue = store.catalogue();
    let spectral = store.msv2().is_none()
        || ids.into_iter().any(|id| {
            catalogue
                .partition(id)
                .data_group(group)
                .is_some_and(|(_, entry)| entry.weights == "WEIGHT_SPECTRUM")
        });
    if spectral {
        WeightColumn::WeightSpectrum
    } else {
        WeightColumn::Weight
    }
}
