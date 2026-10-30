// SPDX-License-Identifier: LGPL-3.0-or-later

//! The selection a request names, resolved against its MeasurementSet: the
//! data descriptions, spectral windows and channels, the per-sample
//! predicates, the visibility column, and the observation selection the
//! survey's partitions make of them.

use std::collections::{BTreeMap, BTreeSet};

use casa_imaging_model::{
    IdSelection, IntentSelection, ObservationSelection, ResolvedIntent, RowSelection,
    SelectionBound, SpectralWindowCoordinateCatalog, SpectralWindowSelection, UvDistanceRange,
    UvDistanceUnit, UvSelection, VisibilityColumn as OwnerVisibilityColumn,
};
use casa_ms::{
    MeasurementSet, UvDistanceInterval, UvDistanceLimit, VisibilityDataColumn, parse_spw_selector,
    parse_uv_distance_intervals, resolve_channel_selector_selection,
};
use casa_types::measures::frequency::FrequencyRef;

use super::PrepareError;
use super::survey::Survey;
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
/// and scan intents (the `STATE` rows its `intent` selector names, with
/// their `OBS_MODE`s).
pub(super) fn row_selection(
    request: &ImagingRequest,
    ms: &MeasurementSet,
) -> Result<RowSelection, PrepareError> {
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
    let intents = match request.intent.as_deref() {
        Some(text) => {
            let state = ms.state()?;
            IntentSelection::Only(
                ms.selected_state_ids(text)?
                    .into_iter()
                    .map(|id| {
                        let row = usize::try_from(id).map_err(|_| PrepareError::NegativeId {
                            selector: "intent",
                            value: id,
                        })?;
                        Ok(ResolvedIntent::new(
                            u32::try_from(id).expect("a nonnegative i32 fits u32"),
                            state.string(row, "OBS_MODE")?,
                        ))
                    })
                    .collect::<Result<_, PrepareError>>()?,
            )
        }
        None => IntentSelection::All,
    };
    Ok(RowSelection::new(fields, uv_distances, intents))
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

/// The frequency axes of the spectral windows `spw_ids`.
pub(super) fn spectral_windows(
    ms: &MeasurementSet,
    spw_ids: impl IntoIterator<Item = usize>,
) -> Result<Vec<SourceSpectralWindow>, PrepareError> {
    let spectral_window = ms.spectral_window()?;
    spw_ids
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|spw_id| {
            let code = spectral_window.meas_freq_ref(spw_id)?;
            Ok(SourceSpectralWindow {
                spw_id,
                frequency_reference: FrequencyRef::from_casacore_code(code)
                    .ok_or(PrepareError::UnknownSourceFrame { spw_id, code })?,
                frequencies_hz: spectral_window.chan_freq(spw_id)?,
                channel_widths_hz: spectral_window.chan_width(spw_id)?,
            })
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

/// The data descriptions the request selects: its `ddid`, else those of
/// the windows its `spw` selector names (every window without one) that
/// bind a spectral window and a polarization.
pub(super) fn selected_data_descriptions(
    request: &ImagingRequest,
    table: &casa_ms::MsDataDescription<'_>,
) -> Result<Vec<i32>, PrepareError> {
    if let Some(ddid) = request.ddid {
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
        return Ok(vec![ddid]);
    }
    let selected_spws = request
        .spw
        .as_deref()
        .map(parse_spw_selector)
        .transpose()?
        .unwrap_or_default()
        .into_iter()
        .map(|selector| selector.spw_id)
        .collect::<BTreeSet<_>>();
    let mut ddids = Vec::new();
    for row in 0..table.row_count() {
        let spw = table.spectral_window_id(row)?;
        let polarization = table.polarization_id(row)?;
        if spw >= 0
            && polarization >= 0
            && (selected_spws.is_empty() || selected_spws.contains(&spw))
        {
            ddids.push(i32::try_from(row).expect("DATA_DESCRIPTION rows are addressed by i32 ids"));
        }
    }
    if ddids.is_empty() {
        return Err(PrepareError::NoSelectedDataDescriptions);
    }
    Ok(ddids)
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
/// `CORRECTED_DATA` when present, else `DATA`.
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
    ms: &MeasurementSet,
    requested: Option<DataColumn>,
) -> Result<OwnerVisibilityColumn, PrepareError> {
    let present = |column| ms.data_column(column).is_ok();
    Ok(match requested {
        Some(DataColumn::Data) => OwnerVisibilityColumn::Data,
        _ if present(VisibilityDataColumn::CorrectedData) => OwnerVisibilityColumn::CorrectedData,
        _ if !present(VisibilityDataColumn::Data) => return Err(PrepareError::NoVisibilityColumn),
        Some(DataColumn::Corrected) => {
            tracing::warn!("CORRECTED_DATA column does not exist. Using DATA column instead");
            OwnerVisibilityColumn::Data
        }
        None => OwnerVisibilityColumn::Data,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use casa_ms::{MeasurementSetBuilder, OptionalMainColumn};
    use serde_json::json;
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::{Context, SubscriberExt};

    use super::*;
    use crate::request::tests::request;

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

    /// The column `requested` images in a MeasurementSet with `columns`, and
    /// the warnings choosing it logs.
    fn chosen(
        columns: &[OptionalMainColumn],
        requested: Option<DataColumn>,
    ) -> (Result<OwnerVisibilityColumn, PrepareError>, Vec<String>) {
        let builder = columns
            .iter()
            .fold(MeasurementSetBuilder::new(), |builder, column| {
                builder.with_main_column(*column)
            });
        let ms = MeasurementSet::create_memory(builder).expect("an in-memory MeasurementSet");
        let warnings = Warnings::default();
        let chosen = tracing::subscriber::with_default(
            tracing_subscriber::registry().with(warnings.clone()),
            || visibility_column(&ms, requested),
        );
        (chosen, warnings.0.lock().expect("warnings").clone())
    }

    #[test]
    fn a_measurement_set_without_visibilities_has_no_column_to_image() {
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
        let (column, warnings) = chosen(&[OptionalMainColumn::Data], Some(DataColumn::Corrected));
        assert!(matches!(column, Ok(OwnerVisibilityColumn::Data)));
        assert_eq!(
            warnings,
            ["CORRECTED_DATA column does not exist. Using DATA column instead"]
        );
    }

    #[test]
    fn an_unspecified_column_falls_back_to_data_without_a_warning() {
        let (column, warnings) = chosen(&[OptionalMainColumn::Data], None);
        assert!(matches!(column, Ok(OwnerVisibilityColumn::Data)));
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    #[test]
    fn corrected_images_corrected_data_when_the_ms_has_it() {
        let both = [OptionalMainColumn::Data, OptionalMainColumn::CorrectedData];
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
