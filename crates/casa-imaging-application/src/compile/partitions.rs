// SPDX-License-Identifier: LGPL-3.0-or-later
//! The compiled selection resolved to an opened MeasurementSet's
//! partitions, for the observation-batch route (#932, step 4).
//!
//! Compile resolves a request's selection against the MeasurementSet:
//! data descriptions, channels, correlations, fields, UV distance and
//! intents (`selection.rs`). This module names the partitions of the same
//! MeasurementSet's [`Msv2Observation`] that hold those samples: a
//! partition's data description must be selected and, under an intent
//! selection, its intents must be a selected state's. It is the only part
//! of the batch route that knows the store; the cutover resolves the
//! selection against the catalogue itself (imaging-port §2.4).

use casa_imaging_model::{
    CompiledProblem, IdSelection, IntentSelection, VisibilityColumn, WeightColumn,
};
use casa_ms::observation_io::Msv2Observation;
use casa_observation::{FieldId, FieldSelection};

use crate::imaging::{PartitionSelection, SelectedPartition};

/// Speed of light in m/s, for a window's reference wavelength.
const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.0;

/// Why the selection cannot be read through the batch route.
#[derive(Debug, thiserror::Error)]
pub(crate) enum PartitionSelectionError {
    /// What the batch route does not read yet.
    #[error("the observation-batch route does not read {0}")]
    Unsupported(&'static str),
    /// A selected product the partition does not hold, or a window
    /// without channels or reference frequency.
    #[error("the selection is inconsistent with the observation: {0}")]
    Inconsistent(String),
    /// The MeasurementSet could not be read.
    #[error(transparent)]
    MeasurementSet(#[from] casa_ms::MsError),
}

/// `problem`'s selection resolved to `observation`'s partitions, in
/// partition order.
///
/// # Errors
///
/// [`PartitionSelectionError::Unsupported`] for what the batch route does
/// not read yet: several MeasurementSets, `FLOAT_DATA`, weights the
/// observation's data group does not hold, and a selected field that
/// follows an ephemeris (until #942 lands, batch geometry would read its
/// `PHASE_DIR` offset as a direction).
pub(crate) fn resolve_partitions(
    problem: &CompiledProblem,
    observation: &Msv2Observation,
) -> Result<PartitionSelection, PartitionSelectionError> {
    let [source] = problem.observation_transaction().read_set().sources() else {
        return Err(PartitionSelectionError::Unsupported(
            "more than one MeasurementSet",
        ));
    };
    let selection = source.selection();
    let columns = source.selected_columns();
    let catalogue = observation.catalogue();
    let visibilities = match columns.visibility() {
        VisibilityColumn::Data => "DATA",
        VisibilityColumn::CorrectedData => "CORRECTED_DATA",
        VisibilityColumn::FloatData => {
            return Err(PartitionSelectionError::Unsupported("FLOAT_DATA"));
        }
    };
    let weights = match columns.weights() {
        WeightColumn::Weight => "WEIGHT",
        WeightColumn::WeightSpectrum => "WEIGHT_SPECTRUM",
    };
    let rows = selection.rows_filter();
    let fields = match rows.fields() {
        IdSelection::All => FieldSelection::All,
        IdSelection::Only(ids) => FieldSelection::Only(ids.iter().copied().map(FieldId).collect()),
    };
    let intents = match rows.intents() {
        IntentSelection::All => None,
        IntentSelection::Only(intents) => Some(
            intents
                .iter()
                .map(|intent| Msv2Observation::mode_intents(intent.observation_mode()))
                .collect::<Vec<_>>(),
        ),
    };
    let measurement_set = observation.measurement_set();
    let windows = measurement_set.spectral_window()?;
    let field_table = measurement_set.field()?;
    let mut data_group = None;
    let mut partitions = Vec::new();
    for id in catalogue.partition_ids() {
        let entry = catalogue.partition(id);
        let data_description = observation.data_description_id(id);
        let Some(description) = selection
            .data_descriptions()
            .iter()
            .find(|selected| selected.data_description_id() == data_description)
        else {
            continue;
        };
        if intents
            .as_ref()
            .is_some_and(|intents| !intents.contains(&entry.intents))
        {
            continue;
        }
        let read = entry
            .fields
            .iter()
            .filter(|field| fields.holds(**field))
            .collect::<Vec<_>>();
        if read.is_empty() {
            continue;
        }
        for field in read {
            if field_table
                .ephemeris_id(field.0 as usize)?
                .is_some_and(|ephemeris| ephemeris >= 0)
            {
                return Err(PartitionSelectionError::Unsupported(
                    "a field that follows an ephemeris (#942)",
                ));
            }
        }
        let group = entry
            .data_groups
            .iter()
            .find(|group| group.visibilities == visibilities)
            .ok_or(PartitionSelectionError::Unsupported(
                "a visibility column without its data group",
            ))?;
        if group.weights != weights {
            return Err(PartitionSelectionError::Unsupported(
                "WEIGHT_SPECTRUM defined in only some selected rows",
            ));
        }
        match &data_group {
            None => data_group = Some(group.name.clone()),
            Some(name) if *name == group.name => {}
            Some(_) => {
                return Err(PartitionSelectionError::Unsupported(
                    "partitions whose column belongs to different data groups",
                ));
            }
        }
        let window = description.spectral_window_id();
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
        let correlations = selection
            .correlations()
            .iter()
            .find(|selected| selected.polarization_id() == description.polarization_id())
            .map(|selected| {
                selected
                    .products()
                    .iter()
                    .map(|product| product.correlation_type())
                    .collect::<Vec<_>>()
            })
            .ok_or_else(|| {
                PartitionSelectionError::Inconsistent(format!(
                    "polarization {} has no selected product",
                    description.polarization_id()
                ))
            })?;
        if let Some(missing) = correlations
            .iter()
            .find(|product| entry.correlation_index(**product).is_none())
        {
            return Err(PartitionSelectionError::Inconsistent(format!(
                "spectral window {window} lacks {missing:?}"
            )));
        }
        let wavelength_m = rows
            .uv_distances()
            .needs_wavelengths()
            .then(|| {
                let frequency_hz = windows.ref_frequency(window as usize)?;
                (frequency_hz.is_finite() && frequency_hz > 0.0)
                    .then(|| SPEED_OF_LIGHT_M_S / frequency_hz)
                    .ok_or_else(|| {
                        PartitionSelectionError::Inconsistent(format!(
                            "spectral window {window} has no positive reference frequency"
                        ))
                    })
            })
            .transpose()?;
        partitions.push(SelectedPartition {
            id,
            spectral_window: window,
            channels,
            correlations,
            wavelength_m,
        });
    }
    Ok(PartitionSelection {
        data_group: data_group.ok_or(PartitionSelectionError::Unsupported(
            "a selection no partition holds",
        ))?,
        partitions,
        fields,
        uv: rows.uv_distances().clone(),
    })
}
