// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MeasurementSets of a request, each opened and its own selection
//! surveyed, in the order CASA's tclean images them, and the one whose
//! metadata the image takes.
//!
//! CASA's tclean images a list of MeasurementSets as one problem: its
//! iterator reads every selected sample of each in turn, the weighting
//! density is gridded over all of them, and the products are one image's.
//! The image's coordinates come from one of them, CASA's `msobj`
//! ([`reference()`]), except a continuum image's band, which spans every
//! MeasurementSet's ([`continuum_band`]).

use std::sync::Arc;

use casa_imaging_runtime::{Admitted, Reservation};
use casa_ms::CubeAxisValue;
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::observation_io::Msv2Observation;
use casa_observation::Catalogue;
use casa_types::measures::MeasuresProvider;
use casa_types::measures::doppler::{DopplerRef, MDoppler};
use casa_types::measures::frame::MeasFrame;
use casa_types::measures::frequency::FrequencyRef;

use super::envelope::{self, SpectralEnvelope};
use super::opening::{OpenedStore, open_observation};
use super::selection::{self, WindowChannels};
use super::survey::Survey;
use super::{Columns, PrepareError};
use crate::{ImagingRequest, MeasurementSetSelection, RunContext, SpecMode};

/// Speed of light in m/s, for a velocity `start`.
const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.0;

/// One MeasurementSet of the request, opened once for compile and the run,
/// with its selection surveyed.
pub(super) struct SurveyedStore {
    /// The MeasurementSet and its selection, as the request names them.
    pub(super) selection: MeasurementSetSelection,
    /// The store, with the charge of what its opening holds.
    pub(super) opened: Admitted<OpenedStore>,
    /// Its geometry engine.
    pub(super) engine: MsCalEngine,
    /// Its selection surveyed.
    pub(super) survey: Survey,
    /// The charge of the survey, held until the problem compiles.
    pub(super) survey_charge: Reservation,
    /// The columns compile chose for its selected partitions.
    pub(super) columns: Columns,
}

impl SurveyedStore {
    /// The store's catalogue.
    pub(super) fn catalogue(&self) -> &Catalogue {
        self.opened.value.catalogue()
    }

    /// The MeasurementSet, when the store is one.
    pub(super) fn msv2(&self) -> Option<&Msv2Observation> {
        self.opened.value.msv2()
    }
}

impl OpenedStore {
    /// The earliest time of any of the store's samples, in seconds since
    /// MJD 0 in its time scale; `None` when it holds none.
    fn earliest_time_mjd_seconds(&self) -> Option<f64> {
        match self {
            Self::V2(observation) => observation.earliest_time_s(),
            Self::V4(observation) => observation.earliest_time_mjd_s(),
        }
    }
}

/// Every MeasurementSet of `request`, opened and surveyed with its own
/// selection, in CASA's order ([`order`]), each with its geometry engine
/// evaluating with `measures`.
///
/// A MeasurementSet whose selection holds no sample is left out, as
/// `SynthesisImagerVi2::selectData` leaves out one that selects no row,
/// unless it is the first: CASA then has none and refuses the selection
/// ("Data selection ended with 0 rows").
///
/// # Errors
///
/// When a MeasurementSet cannot be opened or its selection resolved, or the
/// first one's selection holds no sample.
pub(super) fn survey_stores(
    request: &ImagingRequest,
    context: &RunContext,
    measures: &Arc<dyn MeasuresProvider>,
) -> Result<Vec<SurveyedStore>, PrepareError> {
    let opened = request
        .vis
        .iter()
        .map(|selection| {
            let opened = open_observation(&selection.vis, (&context.host, &context.policy))?;
            Ok((selection.clone(), opened))
        })
        .collect::<Result<Vec<_>, PrepareError>>()?;
    let mut stores = Vec::with_capacity(opened.len());
    let ordered = order(opened, |(_, opened)| {
        opened
            .value
            .earliest_time_mjd_seconds()
            .unwrap_or(f64::INFINITY)
    });
    for (selection, opened) in ordered {
        let surveyed = super::survey_observation(request, &selection, &opened.value, context);
        let (
            Admitted {
                value: survey,
                reservation: survey_charge,
            },
            columns,
        ) = match surveyed {
            Err(PrepareError::NoSelectedSamples) if !stores.is_empty() => {
                tracing::warn!(
                    "{}: the selection holds no sample, so it is left out of the image, as \
                     CASA's tclean leaves it out",
                    selection.vis.display()
                );
                continue;
            }
            surveyed => surveyed?,
        };
        let engine = MsCalEngine::from_catalogue(opened.value.catalogue(), Arc::clone(measures))?;
        stores.push(SurveyedStore {
            selection,
            opened,
            engine,
            survey,
            survey_charge,
            columns,
        });
    }
    Ok(stores)
}

/// `stores` in CASA's order: by the `earliest` `TIME` of each
/// (`ImagerParameters::checkAndFixSelectionPars` through
/// `mslisthelper.sort_mslist`, a stable sort of the first of each one's
/// sorted MAIN `TIME`s), then visited as `PySynthesisImager.selectData`
/// visits its `ms0`, `ms1`, … keys, sorted as text, so that `ms10` comes
/// before `ms2`. A store with no sample sorts last (its earliest time is
/// infinite); its survey refuses it.
fn order<T>(stores: Vec<T>, earliest: impl Fn(&T) -> f64) -> Vec<T> {
    let mut timed = stores
        .into_iter()
        .map(|store| (earliest(&store), store))
        .collect::<Vec<_>>();
    timed.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut keyed = timed
        .into_iter()
        .enumerate()
        .map(|(index, (_, store))| (format!("ms{index}"), store))
        .collect::<Vec<_>>();
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    keyed.into_iter().map(|(_, store)| store).collect()
}

/// The store whose metadata the image takes: CASA's `msobj` in
/// `SynthesisParamsImage::buildCoordinateSystem`. It is the first, except
/// that a `cube` or `cubedata` image whose `start` is a frequency or a
/// velocity takes the last store whose selected band holds that start
/// (`getCubeImageStartFreq`, compared with each store's selected channel
/// edges in the image's frame, the data's for `cubedata`, by
/// `MSUtil::getFreqRangeInSpw`), so that the axis is laid on its channels
/// (CAS-12877).
///
/// # Errors
///
/// An unreadable `start`, or a frame conversion that fails.
pub(super) fn reference(
    request: &ImagingRequest,
    stores: &[SurveyedStore],
) -> Result<usize, PrepareError> {
    if stores.len() < 2 || !matches!(request.specmode, SpecMode::Cube | SpecMode::Cubedata) {
        return Ok(0);
    }
    let Some(start_hz) = cube_start_hz(request)? else {
        return Ok(0);
    };
    let mut reference = 0;
    for (index, store) in stores.iter().enumerate() {
        let survey = &store.survey;
        let channels = survey
            .spectral_windows
            .iter()
            .map(|window| {
                let channels = survey
                    .spw_channels
                    .get(&window.spw_id)
                    .cloned()
                    .unwrap_or_else(|| (0..window.frequencies_hz.len()).collect());
                (window.spw_id, channels)
            })
            .collect::<WindowChannels>();
        let frame = match request.specmode {
            SpecMode::Cubedata => survey.source_frequency_reference,
            _ => request.outframe,
        };
        let [lower_hz, upper_hz] = envelope::continuum_envelope(
            (store.catalogue(), &store.engine),
            survey,
            &channels,
            frame,
        )?
        .edges_hz();
        if (lower_hz..=upper_hz).contains(&start_hz) {
            reference = index;
        }
    }
    Ok(reference)
}

/// CASA's `getCubeImageStartFreq`: a frequency `start` as given, a
/// velocity's at the first `restfreq` (none is zero); `None` for a channel
/// or no start, and for zero.
fn cube_start_hz(request: &ImagingRequest) -> Result<Option<f64>, PrepareError> {
    let Some(start) = request.start.as_deref() else {
        return Ok(None);
    };
    let rest_hz = request.restfreq.first().copied().unwrap_or(0.0);
    let ratio = |value: f64, convention: DopplerRef| -> Result<f64, PrepareError> {
        Ok(MDoppler::new(value, convention)
            .convert_to(DopplerRef::RATIO, &MeasFrame::new())
            .map_err(|error| {
                casa_ms::MsError::InvalidInput(format!("cube start {start:?}: {error}"))
            })?
            .value())
    };
    let hertz = match CubeAxisValue::parse(start, request.veltype)? {
        CubeAxisValue::Channel(_) => return Ok(None),
        CubeAxisValue::FrequencyHz { hz, .. } => hz,
        CubeAxisValue::VelocityMs { ms, .. } => {
            rest_hz * ratio(ms / SPEED_OF_LIGHT_M_S, request.veltype)?
        }
        CubeAxisValue::Doppler { value, convention } => rest_hz * ratio(value, convention)?,
    };
    Ok((hertz > 0.0).then_some(hertz))
}

/// A continuum image's band over every store: each store's selected
/// channels per window, and the union of their edges where CASA grids the
/// samples (LSRK) and in the frame the header records, as
/// `buildCoordinateSystem` takes the least and greatest frequency over every
/// MeasurementSet.
pub(super) struct ContinuumBand {
    /// Each store's selected channels, in store order.
    pub(super) channels: Vec<WindowChannels>,
    /// The band in LSRK.
    pub(super) gridded: SpectralEnvelope,
    /// The band in the request's `outframe`.
    pub(super) recorded: SpectralEnvelope,
}

/// The continuum band of `stores` ([`ContinuumBand`]).
///
/// # Errors
///
/// A channel range a window does not hold, or a frame conversion that
/// fails.
pub(super) fn continuum_band(
    request: &ImagingRequest,
    stores: &[SurveyedStore],
) -> Result<ContinuumBand, PrepareError> {
    let mut channels = Vec::with_capacity(stores.len());
    let mut band: Option<(SpectralEnvelope, SpectralEnvelope)> = None;
    for store in stores {
        let survey = &store.survey;
        let selected = survey
            .spectral_windows
            .iter()
            .map(|window| {
                Ok((
                    window.spw_id,
                    selection::selected_channels(
                        request,
                        &survey.spw_channels,
                        window.spw_id,
                        &window.frequencies_hz,
                    )?,
                ))
            })
            .collect::<Result<WindowChannels, PrepareError>>()?;
        let observed = (store.catalogue(), &store.engine);
        // CASA grids an MFS image's samples at their LSRK frequencies
        // (`FTMachine::matchChannel`) whatever frame its header records.
        let gridded =
            envelope::continuum_envelope(observed, survey, &selected, FrequencyRef::LSRK)?;
        let recorded = if request.outframe == FrequencyRef::LSRK {
            gridded
        } else {
            envelope::continuum_envelope(observed, survey, &selected, request.outframe)?
        };
        band = Some(match band {
            None => (gridded, recorded),
            Some((all_gridded, all_recorded)) => {
                (all_gridded.union(gridded), all_recorded.union(recorded))
            }
        });
        channels.push(selected);
    }
    let (gridded, recorded) = band.expect("a request images at least one MeasurementSet");
    Ok(ContinuumBand {
        channels,
        gridded,
        recorded,
    })
}

/// Refuse `stage` for more than one store.
pub(super) fn one_store(stores: &[SurveyedStore], stage: &'static str) -> Result<(), PrepareError> {
    if stores.len() > 1 {
        return Err(PrepareError::SeveralMeasurementSets {
            stage,
            count: stores.len(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CASA sorts the list stably by each MeasurementSet's earliest time,
    /// then visits its `ms<i>` keys as text.
    #[test]
    fn stores_are_ordered_by_earliest_time_then_visited_as_casa_keys() {
        // Equal times keep the list's order.
        let tied = order(vec![(5.0, 'a'), (5.0, 'b')], |(time, _)| *time);
        assert_eq!(tied, [(5.0, 'a'), (5.0, 'b')]);
        let names = order(
            vec![(7.0, 'a'), (3.0, 'b'), (f64::INFINITY, 'c'), (5.0, 'd')],
            |(time, _)| *time,
        )
        .into_iter()
        .map(|(_, name)| name)
        .collect::<String>();
        assert_eq!(names, "bdac");
        // Twelve stores in time order are visited as ms0, ms1, ms10, ms11,
        // ms2, …, ms9.
        let visited = order((0..12).collect(), |index: &i32| f64::from(*index));
        assert_eq!(visited, [0, 1, 10, 11, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    /// CASA's `getCubeImageStartFreq`: a frequency as given, a velocity at
    /// the first rest frequency in the request's convention, no start for a
    /// channel or none.
    #[test]
    fn a_cube_start_is_its_frequency_or_its_velocity_at_the_rest_frequency() {
        use serde_json::json;
        let start = |overrides: serde_json::Value| {
            let mut values = json!({ "specmode": "cube", "nchan": 10 });
            values
                .as_object_mut()
                .expect("an object")
                .extend(overrides.as_object().expect("an object").clone());
            cube_start_hz(&crate::request::tests::request(values)).expect("a start")
        };
        assert_eq!(start(json!({ "start": "1.199989GHz" })), Some(1.199_989e9));
        let hertz = start(json!({
            "start": "11994.3km/s", "restfreq": ["1.25GHz"], "veltype": "radio",
        }))
        .expect("a velocity start");
        let expected = 1.25e9 * (1.0 - 11_994.3e3 / SPEED_OF_LIGHT_M_S);
        assert!((hertz - expected).abs() < 1.0e-3, "{hertz} {expected}");
        assert_eq!(start(json!({ "start": "3" })), None);
        assert_eq!(start(json!({})), None);
    }
}
