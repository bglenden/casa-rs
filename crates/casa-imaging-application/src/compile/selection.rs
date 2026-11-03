// SPDX-License-Identifier: LGPL-3.0-or-later

//! The selection a request names, resolved against its observation's
//! catalogue: the spectral windows (or an MSv2 data description) and
//! channels, the per-sample predicates, the visibility column, and the
//! observation selection the survey's partitions make of them.

use std::collections::{BTreeMap, BTreeSet};

use casa_imaging_model::{
    BaselineSelection, IdSelection, IntentSelection, ObservationSelection, RowSelection,
    SelectionBound, SpectralWindowCoordinateCatalog, SpectralWindowSelection, UvDistanceRange,
    UvDistanceUnit, UvSelection, VisibilityColumn as OwnerVisibilityColumn,
};
use casa_ms::{
    UvDistanceInterval, UvDistanceLimit, VisibilityDataColumn, parse_uv_distance_intervals,
};
use casa_observation::{Catalogue, StateId};
use casa_types::measures::frequency::FrequencyRef;

use super::opening::OpenedStore;
use super::survey::Survey;
use super::{PrepareError, intents};
use crate::{DataColumn, ImagingRequest, MeasurementSetSelection};

/// One selected spectral window's frequency axis.
#[derive(Clone)]
pub(super) struct SourceSpectralWindow {
    pub(super) spw_id: usize,
    pub(super) frequency_reference: FrequencyRef,
    pub(super) frequencies_hz: Vec<f64>,
    pub(super) channel_widths_hz: Vec<f64>,
}

/// Selected source channels per spectral window id.
pub(super) type WindowChannels = BTreeMap<usize, Vec<usize>>;

/// A MeasurementSet's per-sample predicates: its fields, UV-distance intervals,
/// scan numbers, observation identifiers, baselines and the observing modes
/// its `intent` selector names ([`intents::resolve`]), with the states a
/// state number named.
///
/// The field selector resolves against the catalogue's fields
/// ([`resolve_fields`]); scan and observation text against CASA's strict
/// numeric grammar, kept as inclusive intervals so a wide range holds no
/// more than its terms.
pub(super) fn row_selection(
    selection: &MeasurementSetSelection,
    catalogue: &Catalogue,
) -> Result<(RowSelection, BTreeSet<StateId>), PrepareError> {
    let fields = match selection.field.as_deref() {
        Some(selector) => resolve_fields(selector, catalogue)?,
        None => IdSelection::All,
    };
    let scans = numeric_row_ids(selection.scan.as_deref(), "scan")?;
    let observations = numeric_row_ids(selection.observation.as_deref(), "observation")?;
    let uv_distances = match selection.uvrange.as_deref() {
        Some(text) => UvSelection::Ranges(
            parse_uv_distance_intervals(text)?
                .into_iter()
                .map(uv_range)
                .collect(),
        ),
        None => UvSelection::All,
    };
    let (intents, states) = match selection.intent.as_deref() {
        Some(text) => {
            let resolved = intents::resolve(text, catalogue)?;
            (resolved.selection, resolved.states)
        }
        None => (IntentSelection::All, BTreeSet::new()),
    };
    let baselines = match selection.antenna.as_deref() {
        Some(text) => resolve_baselines(text, catalogue)?,
        None => BaselineSelection::All,
    };
    Ok((
        RowSelection::new(fields, uv_distances, intents)
            .with_scan_observation(scans, observations)
            .with_baselines(baselines),
        states,
    ))
}

/// The baselines CASA's antenna expression `text` selects among
/// `catalogue`'s antennas, through the shared port of CASA's antenna grammar
/// ([`casa_ms::resolve_antenna_selector`]): names, stations and positions
/// in catalogue order, an antenna's identifier being its place.
fn resolve_baselines(text: &str, catalogue: &Catalogue) -> Result<BaselineSelection, PrepareError> {
    let positions = catalogue
        .antennas
        .iter()
        .map(|antenna| antenna.position.as_itrf())
        .collect::<Vec<_>>();
    let rows = catalogue
        .antennas
        .iter()
        .zip(&positions)
        .map(|(antenna, position_m)| casa_ms::AntennaRow {
            name: &antenna.name,
            station: &antenna.station,
            position_m: *position_m,
        })
        .collect::<Vec<_>>();
    let selected = casa_ms::resolve_antenna_selector(&rows, text)?;
    Ok(BaselineSelection::from_pairs(
        rows.len(),
        |antenna1, antenna2| selected.holds(antenna1, antenna2),
    ))
}

/// The fields `selector` names among `catalogue`'s, through the shared
/// FIELD selector ([`casa_ms::resolve_field_selector_rows`]): IDs and
/// ranges bounded by the fields that exist, exact names and simple `*`
/// patterns over unflagged fields. An MSv2 catalogue's field `i` is
/// `FIELD_ID` `i`; an MSv4's fields are numbered in its catalogue's order
/// and named as stored (XRADIO appends the MSv2 field number), with no row
/// flags. Empty text selects every field; a nonempty selector that matches
/// none is refused, as imaging never reads an empty selection.
fn resolve_fields(selector: &str, catalogue: &Catalogue) -> Result<IdSelection, PrepareError> {
    let rows = catalogue
        .fields
        .iter()
        .map(|field| casa_ms::FieldSelectorRow {
            name: &field.name,
            flagged: field.flagged.unwrap_or(false),
        })
        .collect::<Vec<_>>();
    let ids = casa_ms::resolve_field_selector_rows(&rows, selector)?;
    if ids.is_empty() {
        return Ok(IdSelection::All);
    }
    Ok(IdSelection::Only(
        ids.into_iter()
            .map(|id| u32::try_from(id).expect("a resolved FIELD_ID is nonnegative"))
            .collect(),
    ))
}

/// The scan numbers or observation identifiers `text` names, as inclusive
/// intervals ([`casa_ms::parse_numeric_row_id_ranges`]); absent or empty
/// text selects every one.
fn numeric_row_ids(text: Option<&str>, label: &str) -> Result<IdSelection, PrepareError> {
    let Some(text) = text.filter(|text| !text.is_empty()) else {
        return Ok(IdSelection::All);
    };
    let ranges = casa_ms::parse_numeric_row_id_ranges(text, label)?;
    if ranges.is_empty() {
        return Err(casa_ms::MsError::InvalidInput(format!(
            "{label} selector must contain nonnegative IDs"
        ))
        .into());
    }
    Ok(IdSelection::Ranges(
        ranges
            .into_iter()
            .map(|(start, end)| {
                (
                    u32::try_from(start).expect("a parsed row ID is nonnegative"),
                    u32::try_from(end).expect("a parsed row ID is nonnegative"),
                )
            })
            .collect(),
    ))
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

/// The spectral windows a request selects: every window; the data
/// descriptions of a MeasurementSet whose MAIN rows CASA's `spw` selection
/// keeps, or the one its `ddid` names; or an MSv4's windows `spw` selects
/// (numbered in the order its datasets name them).
pub(super) enum WindowChoice {
    /// Every window.
    All,
    /// These MSv4 windows.
    Windows(BTreeSet<u32>),
    /// The partitions of these MSv2 `DATA_DESCRIPTION` rows.
    DataDescriptions(BTreeSet<u32>),
}

impl WindowChoice {
    /// Whether partition `id` of `store` is in a selected window.
    pub(super) fn holds(&self, store: &OpenedStore, id: casa_observation::PartitionId) -> bool {
        match self {
            Self::All => true,
            Self::Windows(windows) => windows.contains(&store.catalogue().partition(id).window.0),
            Self::DataDescriptions(ddids) => store
                .msv2()
                .is_some_and(|observation| ddids.contains(&observation.data_description_id(id))),
        }
    }
}

/// What a request's `spw` selects: the windows (or data descriptions), and
/// the channels it names per window. A window `spw` names without channels
/// (its whole channel range) is absent from `channels`, so `channel_start`
/// and `channel_count` still apply to it.
pub(super) struct SpwChoice {
    pub(super) windows: WindowChoice,
    pub(super) channels: WindowChannels,
    /// The first and last channel of each channel range `spw` names, per
    /// window, in its order (`MSSelection::getChanList`'s rows); a window
    /// `spw` names without channels is absent, as from `channels`.
    pub(super) ranges: WindowRanges,
}

/// The first and last channel of each channel range of a window, by window
/// number ([`SpwChoice::ranges`]).
pub(super) type WindowRanges = BTreeMap<usize, Vec<[usize; 2]>>;

/// The windows and channels `selection` selects in its `store`
/// ([`SpwChoice`]): those its `spw` selects, except that `request`'s `ddid`
/// (casa-rs's own parameter) names the partitions instead while `spw` still
/// names their channels.
///
/// # Errors
///
/// A `ddid` of an MSv4, which has no data descriptions, or one that is not
/// a `DATA_DESCRIPTION` row binding a window and a polarization; an `spw`
/// selection CASA refuses or this one does not image ([`spw_choice`]).
pub(super) fn window_choice(
    request: &ImagingRequest,
    selection: &MeasurementSetSelection,
    store: &OpenedStore,
) -> Result<SpwChoice, PrepareError> {
    let data_description = data_description_choice(request, store)?;
    let mut choice = match selection.spw.as_deref().filter(|text| !text.is_empty()) {
        Some(text) => spw_choice(text, store)?,
        None => SpwChoice {
            windows: WindowChoice::All,
            channels: WindowChannels::new(),
            ranges: WindowRanges::new(),
        },
    };
    // casa-rs's `ddid` names the partitions; `spw` still names their
    // channels.
    if let Some(windows) = data_description {
        choice.windows = windows;
    }
    Ok(choice)
}

/// The one MSv2 data description `request`'s `ddid` names, if it names one.
fn data_description_choice(
    request: &ImagingRequest,
    store: &OpenedStore,
) -> Result<Option<WindowChoice>, PrepareError> {
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
        return Ok(Some(WindowChoice::DataDescriptions(BTreeSet::from([
            u32::try_from(ddid).expect("a bound DATA_DESC_ID is nonnegative"),
        ]))));
    }
    Ok(None)
}

/// CASA's `spw` selection `text` in `store`, through the port of CASA's
/// grammar ([`casa_ms::resolve_spw_selector`]): on a MeasurementSet, over
/// its SPECTRAL_WINDOW and DATA_DESCRIPTION tables, selecting the MAIN rows
/// of the data descriptions CASA keeps; on an MSv4, over its catalogue's
/// windows, each standing for one unflagged data description. Each window's
/// channels are the union of its channel ranges (`getChanList`), as CASA's
/// iterator slices them; CASA's clamp warnings are logged.
///
/// # Errors
///
/// A selection CASA refuses, and three it accepts but whose imaging is not
/// established here: one in parentheses, which keeps every MAIN row while
/// naming channels of some windows only; one that keeps rows of a window
/// none of whose channel ranges found a channel; and overlapping channel
/// ranges of one window, which CASA's iterator would read twice.
fn spw_choice(text: &str, store: &OpenedStore) -> Result<SpwChoice, PrepareError> {
    let catalogue = store.catalogue();
    let selection = match store.msv2() {
        Some(observation) => observation.measurement_set().resolve_spw_selector(text)?,
        None => catalogue_spw_selection(text, catalogue)?,
    };
    for warning in selection.warnings() {
        tracing::warn!("{warning}");
    }
    let refuse = |reason: String| -> PrepareError {
        casa_ms::MsError::InvalidInput(format!("spw selection {text:?}: {reason}")).into()
    };
    let Some(rows) = selection.row_data_descriptions() else {
        return Err(refuse(
            "a parenthesized expression keeps every MAIN row while naming channels of some \
             windows only, which tclean's imaging of is not established"
                .to_string(),
        ));
    };
    // Every listed window has a data description among `rows`.
    let row_windows = selection.spw_ids().iter().copied().collect::<BTreeSet<_>>();
    let mut channels = WindowChannels::new();
    let mut named = WindowRanges::new();
    for &spw in &row_windows {
        let ranges = selection
            .channel_list()
            .iter()
            .filter(|range| range.spw_id == spw)
            .collect::<Vec<_>>();
        let Some(union) = selection.channels_of(spw)? else {
            return Err(refuse(format!(
                "window {spw}'s rows are selected but none of its channel ranges found a \
                 channel, where CASA's iterator would read the whole window"
            )));
        };
        let counted: usize = ranges
            .iter()
            .map(|range| range.channels().map_or(0, Iterator::count))
            .sum();
        if counted != union.len() {
            return Err(refuse(format!(
                "window {spw}'s channel ranges overlap, so CASA's iterator would read some \
                 channels twice"
            )));
        }
        let whole = catalogue
            .partitions
            .iter()
            .find(|entry| entry.window.0 as usize == spw)
            .map_or(0, |entry| entry.frequencies_hz.len());
        if !(union.len() == whole && ranges.len() == 1 && ranges[0].step == 1) {
            named.insert(
                spw,
                ranges
                    .iter()
                    .filter_map(|range| {
                        let mut channels = range.channels()?;
                        let first = channels.next()?;
                        Some([first, channels.last().unwrap_or(first)])
                    })
                    .collect(),
            );
            channels.insert(spw, union);
        }
    }
    let as_u32 = |values: &[usize]| {
        values
            .iter()
            .map(|value| u32::try_from(*value).expect("a SPECTRAL_WINDOW or DATA_DESCRIPTION row"))
            .collect::<BTreeSet<_>>()
    };
    let windows = if store.msv2().is_some() {
        WindowChoice::DataDescriptions(as_u32(rows))
    } else {
        WindowChoice::Windows(as_u32(rows))
    };
    Ok(SpwChoice {
        windows,
        channels,
        ranges: named,
    })
}

/// [`casa_ms::resolve_spw_selector`] over an MSv4's catalogue: window `i`
/// is NAMEd and channelled as its first partition states it, with its
/// REF_FREQUENCY in the stored frame, its TOTAL_BANDWIDTH (else the sum of
/// its widths) and its FLAG_ROW, and data description `i` is window `i`.
fn catalogue_spw_selection(
    text: &str,
    catalogue: &Catalogue,
) -> Result<casa_ms::SpwSelection, PrepareError> {
    let first_partitions = (0..catalogue.windows.len())
        .map(|window| {
            catalogue
                .partitions
                .iter()
                .find(|entry| entry.window.0 as usize == window)
        })
        .collect::<Vec<_>>();
    let rows = catalogue
        .windows
        .iter()
        .zip(&first_partitions)
        .map(|(window, partition)| {
            let (name, frequencies, widths) = partition.map_or(("", &[][..], &[][..]), |entry| {
                (
                    entry.spectral_window.as_str(),
                    entry.frequencies_hz.as_slice(),
                    entry.channel_widths_hz.as_slice(),
                )
            });
            casa_ms::SpectralWindowRow {
                name,
                chan_freq_hz: frequencies,
                chan_width_hz: widths,
                ref_frequency_hz: window
                    .reference_frequency
                    .as_ref()
                    .map_or(f64::NAN, casa_types::measures::frequency::MFrequency::hz),
                total_bandwidth_hz: window
                    .total_bandwidth_hz
                    .unwrap_or_else(|| widths.iter().map(|width| width.abs()).sum()),
                flag_row: window.flagged.unwrap_or(false),
            }
        })
        .collect::<Vec<_>>();
    let descriptions = (0..rows.len())
        .map(|window| casa_ms::DataDescriptionRow {
            spectral_window_id: i32::try_from(window).expect("a window count fits i32"),
            flag_row: false,
        })
        .collect::<Vec<_>>();
    Ok(casa_ms::resolve_spw_selector(&rows, &descriptions, text)?)
}

/// The channels of `spw_id` the request selects: those its `spw`
/// selector names (`named`, from [`SpwChoice::channels`]), else
/// `channel_start` and `channel_count`.
pub(super) fn selected_channels(
    request: &ImagingRequest,
    named: &WindowChannels,
    spw_id: usize,
    frequencies: &[f64],
) -> Result<Vec<usize>, PrepareError> {
    if let Some(channels) = named.get(&spw_id) {
        return Ok(channels.clone());
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

/// The observation selection: the surveyed partitions and samples, and
/// `channels` per window with the window's whole frequency axis (CASA's
/// mosaic kernels select by the complete `CHAN_FREQ` and
/// `|CHAN_WIDTH[0]|`) and its factor to LSRK at its first visibility
/// buffer, from `lsrk_factors` ([`super::envelope::first_buffer_lsrk_factors`]),
/// and the per-time frequency selection of a window in `bands`
/// ([`super::frequency_bands`]).
pub(super) fn observation_selection(
    survey: Survey,
    channels: &WindowChannels,
    lsrk_factors: &BTreeMap<usize, f64>,
    bands: &super::spectral::WindowBands,
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
            let factor = lsrk_factors
                .get(spw_id)
                .copied()
                .expect("every selected window has its first-buffer factor");
            SpectralWindowSelection::new(
                u32::try_from(*spw_id).expect("SPW ids are nonnegative stored i32 values"),
                channels
                    .iter()
                    .map(|channel| u32::try_from(*channel).expect("a CHAN_FREQ index fits u32"))
                    .collect(),
            )
            .with_coordinate_catalog(catalog)
            .with_first_buffer_lsrk_factor(factor)
            .map(|selection| match bands.get(spw_id) {
                Some(bands) => selection.with_frequency_bands(bands.clone()),
                None => selection,
            })
            .ok_or(PrepareError::SpectralWindowCoordinates { spw_id: *spw_id })
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
            selected_channels(&request, &WindowChannels::new(), 0, &[1.0e9, 1.1e9]),
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
