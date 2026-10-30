// SPDX-License-Identifier: LGPL-3.0-or-later

//! The selection a request names, resolved against its observation's
//! catalogue: the spectral windows (or an MSv2 data description) and
//! channels, the per-sample predicates, the visibility column, and the
//! observation selection the survey's partitions make of them.

use std::collections::{BTreeMap, BTreeSet};

use casa_imaging_model::{
    IdSelection, IntentSelection, ObservationSelection, RowSelection, SelectionBound,
    SpectralWindowCoordinateCatalog, SpectralWindowSelection, UvDistanceRange, UvDistanceUnit,
    UvSelection, VisibilityColumn as OwnerVisibilityColumn,
};
use casa_ms::{
    UvDistanceInterval, UvDistanceLimit, VisibilityDataColumn, parse_spw_selector,
    parse_uv_distance_intervals, resolve_channel_selector_selection,
};
use casa_observation::{Catalogue, StateId};
use casa_types::measures::frequency::FrequencyRef;

use super::opening::OpenedStore;
use super::survey::Survey;
use super::{PrepareError, intents};
use crate::{DataColumn, ImagingRequest};

/// One selected spectral window's frequency axis.
pub(super) struct SourceSpectralWindow {
    pub(super) spw_id: usize,
    pub(super) frequency_reference: FrequencyRef,
    pub(super) frequencies_hz: Vec<f64>,
    pub(super) channel_widths_hz: Vec<f64>,
}

/// Selected source channels per spectral window id.
pub(super) type WindowChannels = BTreeMap<usize, Vec<usize>>;

/// The request's per-sample predicates: its fields, UV-distance intervals
/// and the observing modes its `intent` selector names
/// ([`intents::resolve`]), with the states a state number named.
pub(super) fn row_selection(
    request: &ImagingRequest,
    catalogue: &Catalogue,
) -> Result<(RowSelection, BTreeSet<StateId>), PrepareError> {
    let fields = match &request.field {
        Some(ids) => IdSelection::Only(
            ids.iter()
                .map(|id| {
                    u32::try_from(*id).map_err(|_| PrepareError::NegativeId {
                        selector: "field",
                        value: *id,
                    })
                })
                .collect::<Result<_, _>>()?,
        ),
        None => IdSelection::All,
    };
    let uv_distances = match request.uvrange.as_deref() {
        Some(text) => UvSelection::Ranges(
            parse_uv_distance_intervals(text)?
                .into_iter()
                .map(uv_range)
                .collect(),
        ),
        None => UvSelection::All,
    };
    let (intents, states) = match request.intent.as_deref() {
        Some(text) => {
            let resolved = intents::resolve(text, catalogue)?;
            (resolved.selection, resolved.states)
        }
        None => (IntentSelection::All, BTreeSet::new()),
    };
    Ok((RowSelection::new(fields, uv_distances, intents), states))
}

/// One `uvrange` interval as the model states it.
fn uv_range(interval: UvDistanceInterval) -> UvDistanceRange {
    let bound = |limit: UvDistanceLimit| {
        if limit.inclusive {
            SelectionBound::inclusive(limit.value)
        } else {
            SelectionBound::exclusive(limit.value)
        }
    };
    UvDistanceRange::new(
        interval.lower.map(bound),
        interval.upper.map(bound),
        if interval.wavelengths {
            UvDistanceUnit::Wavelengths
        } else {
            UvDistanceUnit::Meters
        },
    )
}

/// The frequency axes of the catalogue's windows `spw_ids`, each as the
/// first of its partitions states it.
pub(super) fn spectral_windows(
    catalogue: &Catalogue,
    spw_ids: impl IntoIterator<Item = usize>,
) -> Vec<SourceSpectralWindow> {
    spw_ids
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|spw_id| {
            let entry = catalogue
                .partitions
                .iter()
                .find(|entry| entry.window.0 as usize == spw_id)
                .expect("a candidate's window is a partition's");
            SourceSpectralWindow {
                spw_id,
                frequency_reference: entry.frequency_frame,
                frequencies_hz: entry.frequencies_hz.clone(),
                channel_widths_hz: entry.channel_widths_hz.clone(),
            }
        })
        .collect()
}

/// The one source frame every selected window shares.
pub(super) fn one_source_frame(
    windows: &[SourceSpectralWindow],
) -> Result<FrequencyRef, PrepareError> {
    let mut frames = windows.iter().map(|window| window.frequency_reference);
    let first = frames
        .next()
        .expect("a selected sample is in a selected window");
    if frames.any(|frame| frame != first) {
        return Err(PrepareError::MixedSourceFrames);
    }
    Ok(first)
}

/// The one selected spectral window of a stage that images one; `stage`
/// names it in the error.
pub(super) fn one_window<'a>(
    windows: &'a [SourceSpectralWindow],
    stage: &'static str,
) -> Result<&'a SourceSpectralWindow, PrepareError> {
    match windows {
        [window] => Ok(window),
        _ => Err(PrepareError::OneSpectralWindow {
            stage,
            selected: windows.len(),
        }),
    }
}

/// The spectral windows a request selects: the catalogue's windows its
/// `spw` selector numbers (a MeasurementSet's `SPECTRAL_WINDOW` rows; an
/// MSv4's windows in the order its datasets name them), or every window, or
/// the one MSv2 data description its `ddid` names.
pub(super) enum WindowChoice {
    /// Every window.
    All,
    /// These windows.
    Windows(BTreeSet<u32>),
    /// The partitions of one MSv2 `DATA_DESCRIPTION` row.
    DataDescription(u32),
}

impl WindowChoice {
    /// Whether partition `id` of `store` is in a selected window.
    pub(super) fn holds(&self, store: &OpenedStore, id: casa_observation::PartitionId) -> bool {
        match self {
            Self::All => true,
            Self::Windows(windows) => windows.contains(&store.catalogue().partition(id).window.0),
            Self::DataDescription(ddid) => store
                .msv2()
                .is_some_and(|observation| observation.data_description_id(id) == *ddid),
        }
    }
}

/// The windows `request` selects in `store` ([`WindowChoice`]).
///
/// # Errors
///
/// A `ddid` of an MSv4, which has no data descriptions, or one that is not
/// a `DATA_DESCRIPTION` row binding a window and a polarization; an `spw`
/// selector that does not parse.
pub(super) fn window_choice(
    request: &ImagingRequest,
    store: &OpenedStore,
) -> Result<WindowChoice, PrepareError> {
    if let Some(ddid) = request.ddid {
        let observation = store
            .msv2()
            .ok_or(PrepareError::NoDataDescriptions { ddid })?;
        let table = observation.measurement_set().data_description()?;
        let binds = usize::try_from(ddid)
            .ok()
            .filter(|row| *row < table.row_count())
            .map(|row| {
                Ok::<_, PrepareError>(
                    table.spectral_window_id(row)? >= 0 && table.polarization_id(row)? >= 0,
                )
            })
            .transpose()?
            .unwrap_or(false);
        if !binds {
            return Err(PrepareError::InvalidDataDescription { ddid });
        }
        return Ok(WindowChoice::DataDescription(
            u32::try_from(ddid).expect("a bound DATA_DESC_ID is nonnegative"),
        ));
    }
    let Some(text) = request.spw.as_deref() else {
        return Ok(WindowChoice::All);
    };
    let windows = parse_spw_selector(text)?
        .into_iter()
        .filter_map(|selector| u32::try_from(selector.spw_id).ok())
        .collect::<BTreeSet<_>>();
    Ok(if windows.is_empty() {
        WindowChoice::All
    } else {
        WindowChoice::Windows(windows)
    })
}

/// The channels of `spw_id` the request selects: those its `spw`
/// selector names, else `channel_start` and `channel_count`.
pub(super) fn selected_channels(
    request: &ImagingRequest,
    spw_id: usize,
    frequencies: &[f64],
) -> Result<Vec<usize>, PrepareError> {
    if let Some(channels) = explicit_spw_channels(request, spw_id, frequencies)? {
        return Ok(channels);
    }
    let start = request.channel_start.unwrap_or(0);
    let count = request
        .channel_count
        .unwrap_or_else(|| frequencies.len().saturating_sub(start));
    match start.checked_add(count) {
        Some(end) if count > 0 && end <= frequencies.len() => Ok((start..end).collect()),
        _ => Err(PrepareError::ChannelRange {
            spw_id,
            start,
            count,
            channels: frequencies.len(),
        }),
    }
}

/// The channels of `spw_id` the request's `spw` selector names, if it
/// names any.
pub(super) fn explicit_spw_channels(
    request: &ImagingRequest,
    spw_id: usize,
    frequencies: &[f64],
) -> Result<Option<Vec<usize>>, PrepareError> {
    let Some(text) = request.spw.as_deref() else {
        return Ok(None);
    };
    let Some(selector) = parse_spw_selector(text)?
        .into_iter()
        .find(|selector| usize::try_from(selector.spw_id).ok() == Some(spw_id))
        .and_then(|selector| selector.channels)
    else {
        return Ok(None);
    };
    Ok(Some(
        resolve_channel_selector_selection(frequencies, &selector)?.indices,
    ))
}

/// The observation selection: the surveyed partitions and samples, and
/// `channels` per window with the window's whole frequency axis (CASA's
/// mosaic kernels select by the complete `CHAN_FREQ` and
/// `|CHAN_WIDTH[0]|`).
pub(super) fn observation_selection(
    survey: Survey,
    channels: &WindowChannels,
) -> Result<ObservationSelection, PrepareError> {
    let spectral_windows = channels
        .iter()
        .map(|(spw_id, channels)| {
            let window = survey
                .spectral_windows
                .iter()
                .find(|window| window.spw_id == *spw_id)
                .expect("the channels read are of selected windows");
            let catalog = window
                .channel_widths_hz
                .first()
                .and_then(|width| {
                    SpectralWindowCoordinateCatalog::new(window.frequencies_hz.clone(), *width)
                })
                .ok_or(PrepareError::SpectralWindowCoordinates { spw_id: *spw_id })?;
            Ok(SpectralWindowSelection::new(
                u32::try_from(*spw_id).expect("SPW ids are nonnegative stored i32 values"),
                channels
                    .iter()
                    .map(|channel| u32::try_from(*channel).expect("a CHAN_FREQ index fits u32"))
                    .collect(),
            )
            .with_coordinate_catalog(catalog))
        })
        .collect::<Result<Vec<_>, PrepareError>>()?;
    Ok(ObservationSelection::new(
        survey.samples,
        survey.row_selection,
        survey.partitions,
        spectral_windows,
    ))
}

/// The visibility column imaged: `DATA` when asked for, else
/// `CORRECTED_DATA` when the observation `holds` it, else `DATA`.
///
/// `datacolumn='corrected'` on a MeasurementSet without `CORRECTED_DATA`
/// therefore images `DATA`, as CASA's `tclean` does (simulation plan §3,
/// decision 6): `SIMapperCollection::grid` and `FTMachine::makeImage` turn a
/// `CORRECTED` request into `OBSERVED` when the column is missing. CASA 6.x
/// logs no message for the fallback, since the one `SynthesisImager::selectData`
/// once logged is commented out. casa-rs logs that message as a warning,
/// because the column imaged is not the one asked for; an unspecified column
/// falls back without one.
///
/// CASA's VisibilityIterator2 also counts the column as missing in an
/// iteration chunk whose first row's cell is undefined (`existsColumn` tests
/// `isDefined(0)` on the chunk's attached table), so a partly written
/// `CORRECTED_DATA` can image `DATA` chunk by chunk. casa-rs decides by the
/// column's presence alone and images an existing `CORRECTED_DATA` as before:
/// a deliberate scope limit, not full parity. casa-tables' metadata-only
/// probe (`array_cell_is_defined_uncached`) answers only for lazy, disk-backed
/// `TiledShapeStMan` columns, and one probe would not reproduce CASA's
/// per-chunk rule.
pub(super) fn visibility_column(
    holds: impl Fn(OwnerVisibilityColumn) -> bool,
    requested: Option<DataColumn>,
) -> Result<OwnerVisibilityColumn, PrepareError> {
    Ok(match requested {
        Some(DataColumn::Data) => OwnerVisibilityColumn::Data,
        _ if holds(OwnerVisibilityColumn::CorrectedData) => OwnerVisibilityColumn::CorrectedData,
        _ if !holds(OwnerVisibilityColumn::Data) => return Err(PrepareError::NoVisibilityColumn),
        Some(DataColumn::Corrected) => {
            tracing::warn!("CORRECTED_DATA column does not exist. Using DATA column instead");
            OwnerVisibilityColumn::Data
        }
        None => OwnerVisibilityColumn::Data,
    })
}

/// The data group holding `visibility` in both stores: `base` for the data
/// (MSv2 `DATA`, MSv4 `VISIBILITY`), `corrected` for the corrected data.
pub(super) fn data_group(visibility: OwnerVisibilityColumn) -> &'static str {
    match visibility {
        OwnerVisibilityColumn::Data => "base",
        OwnerVisibilityColumn::CorrectedData => "corrected",
        OwnerVisibilityColumn::FloatData => "float",
    }
}

/// Whether `store` holds `visibility` for the `selected` partitions: a
/// MeasurementSet when MAIN has its column (its adapter states the `base`
/// group whether or not MAIN has `DATA`), as CASA asks; an MSv4, whose
/// partitions each state their own data groups, when every selected
/// partition has the column's data group.
pub(super) fn holds(
    store: &OpenedStore,
    selected: &[casa_observation::PartitionId],
    visibility: OwnerVisibilityColumn,
) -> bool {
    match store.msv2() {
        Some(observation) => {
            let column = match visibility {
                OwnerVisibilityColumn::Data => VisibilityDataColumn::Data,
                OwnerVisibilityColumn::CorrectedData => VisibilityDataColumn::CorrectedData,
                OwnerVisibilityColumn::FloatData => return false,
            };
            observation.measurement_set().data_column(column).is_ok()
        }
        None => held_by_every(store.catalogue(), selected, data_group(visibility)),
    }
}

/// Whether every `selected` partition of `catalogue` has data group `group`.
fn held_by_every(
    catalogue: &casa_observation::Catalogue,
    selected: &[casa_observation::PartitionId],
    group: &str,
) -> bool {
    selected
        .iter()
        .all(|id| catalogue.partition(*id).data_group(group).is_some())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::json;
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::{Context, SubscriberExt};

    use super::*;
    use crate::request::tests::request;

    /// A partition whose data groups are `groups`.
    fn partition(groups: &[&str]) -> casa_observation::PartitionEntry {
        use casa_observation::{
            DataGroupEntry, ObservationId, PartitionEntry, UvwFrame, VisibilityUnits, WindowId,
        };
        use casa_types::measures::direction::DirectionRef;
        use casa_types::measures::frequency::FrequencyRef;
        use casa_types::measures::{EpochRef, MjdHighPrec};
        PartitionEntry {
            spectral_window: "spw".to_string(),
            frequencies_hz: vec![1.0e9],
            frequency_frame: FrequencyRef::LSRK,
            channel_widths_hz: vec![1.0e6],
            correlations: vec![casa_types::Correlation::XX],
            intents: Vec::new(),
            observation: ObservationId(0),
            feeds: None,
            fields: Vec::new(),
            scans: Vec::new(),
            baselines: Vec::new(),
            time_scale: EpochRef::UTC,
            time_origin: MjdHighPrec::new(0.0, 0.0),
            data_groups: groups
                .iter()
                .map(|name| DataGroupEntry {
                    name: (*name).to_string(),
                    visibilities: String::new(),
                    flags: String::new(),
                    weights: String::new(),
                    uvw: String::new(),
                    units: VisibilityUnits::default(),
                    uvw_frame: UvwFrame {
                        effective: DirectionRef::J2000,
                        stored_label: None,
                    },
                })
                .collect(),
            receptors: std::collections::BTreeMap::new(),
            window: WindowId(0),
            states: Vec::new(),
            processor: None,
        }
    }

    /// An MSv4's partitions each state their own data groups: `corrected` is
    /// held, and so the default, only when every selected partition has it,
    /// whatever an unselected one holds (Astra's review of #1013).
    #[test]
    fn an_msv4_holds_a_group_when_every_selected_partition_does() {
        use casa_observation::{Catalogue, PartitionId};
        let catalogue = Catalogue {
            partitions: vec![
                partition(&["base", "corrected"]),
                partition(&["base"]),
                partition(&["base", "corrected"]),
            ],
            ..Catalogue::default()
        };
        let corrected = |selected: &[u32]| {
            let selected: Vec<PartitionId> = selected.iter().map(|id| PartitionId(*id)).collect();
            let holds = |column| held_by_every(&catalogue, &selected, data_group(column));
            visibility_column(holds, None).expect("a column")
                == OwnerVisibilityColumn::CorrectedData
        };
        assert!(
            !corrected(&[1]),
            "an unselected partition's group is not the default"
        );
        assert!(
            !corrected(&[0, 1]),
            "a selected partition without the group"
        );
        assert!(corrected(&[0, 2]));
    }

    #[test]
    fn channels_past_the_window_are_refused() {
        let request = request(json!({ "channel_start": 1, "channel_count": 2 }));
        assert!(matches!(
            selected_channels(&request, 0, &[1.0e9, 1.1e9]),
            Err(PrepareError::ChannelRange {
                spw_id: 0,
                start: 1,
                count: 2,
                channels: 2,
            })
        ));
    }

    /// The messages of the warnings an event logs.
    #[derive(Clone, Default)]
    struct Warnings(Arc<Mutex<Vec<String>>>);

    impl tracing::field::Visit for Warnings {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.0.lock().expect("warnings").push(format!("{value:?}"));
            }
        }
    }

    impl<S: tracing::Subscriber> Layer<S> for Warnings {
        fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
            if *event.metadata().level() == tracing::Level::WARN {
                event.record(&mut self.clone());
            }
        }
    }

    /// The column `requested` images in an observation holding `columns`,
    /// and the warnings choosing it logs.
    fn chosen(
        columns: &[OwnerVisibilityColumn],
        requested: Option<DataColumn>,
    ) -> (Result<OwnerVisibilityColumn, PrepareError>, Vec<String>) {
        let warnings = Warnings::default();
        let chosen = tracing::subscriber::with_default(
            tracing_subscriber::registry().with(warnings.clone()),
            || visibility_column(|column| columns.contains(&column), requested),
        );
        (chosen, warnings.0.lock().expect("warnings").clone())
    }

    #[test]
    fn an_observation_without_visibilities_has_no_column_to_image() {
        for requested in [None, Some(DataColumn::Corrected)] {
            assert!(matches!(
                chosen(&[], requested).0,
                Err(PrepareError::NoVisibilityColumn)
            ));
        }
        // An explicit DATA is not checked here; reading the missing column
        // refuses it later, as before.
        assert!(matches!(
            chosen(&[], Some(DataColumn::Data)).0,
            Ok(OwnerVisibilityColumn::Data)
        ));
    }

    #[test]
    fn corrected_falls_back_to_data_with_a_warning_when_the_ms_has_no_corrected_data() {
        let (column, warnings) =
            chosen(&[OwnerVisibilityColumn::Data], Some(DataColumn::Corrected));
        assert!(matches!(column, Ok(OwnerVisibilityColumn::Data)));
        assert_eq!(
            warnings,
            ["CORRECTED_DATA column does not exist. Using DATA column instead"]
        );
    }

    #[test]
    fn an_unspecified_column_falls_back_to_data_without_a_warning() {
        let (column, warnings) = chosen(&[OwnerVisibilityColumn::Data], None);
        assert!(matches!(column, Ok(OwnerVisibilityColumn::Data)));
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    #[test]
    fn corrected_images_corrected_data_when_the_ms_has_it() {
        let both = [
            OwnerVisibilityColumn::Data,
            OwnerVisibilityColumn::CorrectedData,
        ];
        for requested in [None, Some(DataColumn::Corrected)] {
            let (column, warnings) = chosen(&both, requested);
            assert!(matches!(column, Ok(OwnerVisibilityColumn::CorrectedData)));
            assert!(warnings.is_empty(), "{warnings:?}");
        }
        assert!(matches!(
            chosen(&both, Some(DataColumn::Data)).0,
            Ok(OwnerVisibilityColumn::Data)
        ));
    }
}
