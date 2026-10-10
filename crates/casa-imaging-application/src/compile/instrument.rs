// SPDX-License-Identifier: LGPL-3.0-or-later

//! The instrument a run images through: the analytic primary beam of its
//! products, the scientific instrument response of a direction-dependent
//! gridder, and where each row points.

use std::collections::BTreeSet;
use std::ops::Range;

use casa_imaging_model::{
    InstrumentModel, MissingPointingPolicy, ObservationPointingLaw, PointingCentreLaw,
    PointingDirectionColumn, PointingDirectionSemantic, PointingExtrapolation,
    PointingInterpolation, PointingTimeSampling,
};
use casa_imaging_products::AnalyticPrimaryBeamModel;
use casa_ms::MeasurementSet;

use super::boxed;
use crate::{ApplicationError, Gridder};

fn telescopes(ms: &MeasurementSet) -> Result<BTreeSet<String>, ApplicationError> {
    let observation = ms.observation()?;
    (0..observation.row_count())
        .map(|row| {
            observation
                .string(row, "TELESCOPE_NAME")
                .map(|name| name.trim().to_string())
                .map_err(Into::into)
        })
        .collect()
}

/// The analytic primary beam a standard-gridder run writes `.pb` and
/// `.image.pbcor` with.
pub(super) fn standard_primary_beam_model(
    ms: &MeasurementSet,
) -> Result<AnalyticPrimaryBeamModel, ApplicationError> {
    let telescopes = telescopes(ms)?;
    match telescopes
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["ALMA"] => homogeneous_dishes(ms, 10.0..13.0, AnalyticPrimaryBeamModel::CasaAlma12mAiry),
        ["ACA"] => homogeneous_dishes(ms, 6.0..8.0, AnalyticPrimaryBeamModel::CasaAca7mAiry),
        _ => analytic_primary_beam_model_for_telescopes(&telescopes),
    }
}

fn homogeneous_dishes(
    ms: &MeasurementSet,
    diameter_range_m: Range<f64>,
    model: AnalyticPrimaryBeamModel,
) -> Result<AnalyticPrimaryBeamModel, ApplicationError> {
    let antenna = ms.antenna()?;
    if antenna.row_count() == 0 {
        return Err(boxed(
            "ALMA primary-beam publication requires ANTENNA dish metadata",
        ));
    }
    for row in 0..antenna.row_count() {
        let diameter = antenna.dish_diameter(row)?;
        if !diameter.is_finite() || !diameter_range_m.contains(&diameter) {
            return Err(boxed(format!(
                "ALMA primary-beam publication requires one homogeneous dish class; row {row} has \
                 diameter {diameter} m"
            )));
        }
    }
    Ok(model)
}

fn analytic_primary_beam_model_for_telescopes(
    telescopes: &BTreeSet<String>,
) -> Result<AnalyticPrimaryBeamModel, ApplicationError> {
    match telescopes
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["EVLA"] => Ok(AnalyticPrimaryBeamModel::CasaEvlaCommon),
        ["VLA"] => Ok(AnalyticPrimaryBeamModel::CasaVlaBand),
        [] => Err(boxed(
            "standard primary-beam publication requires OBSERVATION telescope metadata",
        )),
        names => Err(boxed(format!(
            "standard primary-beam publication has no installed analytic model for telescope set \
             {names:?}"
        ))),
    }
}

/// The instrument response of a direction-dependent gridder; `None` for the
/// others.
pub(super) fn scientific_instrument_model(
    gridder: &Gridder,
    ms: &MeasurementSet,
) -> Result<Option<InstrumentModel>, ApplicationError> {
    let (model, dishes): (_, fn(f64) -> bool) = match gridder {
        Gridder::Awproject(_) => (InstrumentModel::CasaEvlaWidebandAwV1, |diameter| {
            (diameter - 25.0).abs() < 1.0
        }),
        Gridder::Mosaic { .. } => (
            InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1,
            |diameter| (diameter - 12.0).abs() < 0.5 || (diameter - 7.0).abs() < 1.0,
        ),
        Gridder::Standard | Gridder::Wproject { .. } => return Ok(None),
    };
    let telescopes = telescopes(ms)?;
    let supported = !telescopes.is_empty()
        && telescopes.iter().all(|name| match gridder {
            Gridder::Awproject(_) => matches!(name.as_str(), "VLA" | "EVLA"),
            _ => matches!(name.as_str(), "ALMA" | "ACA"),
        });
    if !supported {
        return Err(boxed(format!(
            "requested instrument response is unsupported for observation metadata {telescopes:?}"
        )));
    }
    let antenna = ms.antenna()?;
    if antenna.row_count() == 0 {
        return Err(boxed(
            "a primary-beam response requires ANTENNA dish metadata",
        ));
    }
    for row in 0..antenna.row_count() {
        let diameter = antenna.dish_diameter(row)?;
        if !(diameter.is_finite() && dishes(diameter)) {
            return Err(boxed(format!(
                "the {model:?} response does not cover ANTENNA row {row}'s {diameter} m dish"
            )));
        }
    }
    Ok(Some(model))
}

/// Where each row points: the POINTING table under tclean `usepointing`
/// (AW samples it at the visibility time, mosaic interpolates it); each
/// row's FIELD direction for a mosaic or A-projection run without it
/// (`usepointing=False`, the tclean default); the phase-tracking centre
/// for a direction-independent run.
pub(super) fn pointing_centre_law(gridder: &Gridder) -> PointingCentreLaw {
    match gridder {
        Gridder::Awproject(aw) if aw.usepointing => {
            PointingCentreLaw::Observation(ObservationPointingLaw::new(
                PointingDirectionColumn::Direction,
                PointingDirectionSemantic::AntennaBoresight,
                PointingTimeSampling::VisibilityTime,
                PointingInterpolation::Nearest,
                PointingExtrapolation::HoldNearest,
                MissingPointingPolicy::UsePhaseTrackingCentre,
            ))
        }
        Gridder::Mosaic {
            usepointing: true, ..
        } => PointingCentreLaw::Observation(ObservationPointingLaw::new(
            PointingDirectionColumn::Direction,
            PointingDirectionSemantic::AntennaBoresight,
            PointingTimeSampling::VisibilityTimeCentroid,
            PointingInterpolation::GreatCircleShortestArc,
            PointingExtrapolation::Reject,
            MissingPointingPolicy::Reject,
        )),
        Gridder::Awproject(_) | Gridder::Mosaic { .. } => PointingCentreLaw::FieldCentre,
        Gridder::Standard | Gridder::Wproject { .. } => PointingCentreLaw::PhaseTrackingCentre,
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::path::PathBuf;

    use casa_imaging_model::ProductNormalization;

    use super::*;
    use crate::{AwCfSource, AwProjection};

    fn aw(usepointing: bool) -> Gridder {
        Gridder::Awproject(AwProjection {
            wprojplanes: NonZeroUsize::new(32),
            usepointing,
            normtype: ProductNormalization::FlatNoise,
            cf_resident_mb: 256,
            pointingoffsetsigdev: Vec::new(),
            cf_source: AwCfSource::CasaImport {
                cfcache: PathBuf::from("cf"),
            },
        })
    }

    #[test]
    fn aw_pointing_compiles_casa_visibility_sampling_law() {
        let PointingCentreLaw::Observation(law) = pointing_centre_law(&aw(true)) else {
            panic!("AW usepointing must compile an observation pointing law");
        };
        assert_eq!(law.time_sampling(), PointingTimeSampling::VisibilityTime);
        assert_eq!(law.interpolation(), PointingInterpolation::Nearest);
        assert_eq!(law.extrapolation(), PointingExtrapolation::HoldNearest);
        assert_eq!(law.missing(), MissingPointingPolicy::UsePhaseTrackingCentre);
        assert!(matches!(
            pointing_centre_law(&aw(false)),
            PointingCentreLaw::FieldCentre
        ));
    }

    #[test]
    fn mosaic_points_at_the_field_centre_unless_usepointing() {
        let mosaic = |usepointing| Gridder::Mosaic {
            usepointing,
            normtype: ProductNormalization::FlatNoise,
        };
        assert!(matches!(
            pointing_centre_law(&Gridder::Standard),
            PointingCentreLaw::PhaseTrackingCentre
        ));
        assert!(matches!(
            pointing_centre_law(&mosaic(false)),
            PointingCentreLaw::FieldCentre
        ));
        let PointingCentreLaw::Observation(law) = pointing_centre_law(&mosaic(true)) else {
            panic!("mosaic usepointing must compile an observation pointing law");
        };
        assert_eq!(
            law.time_sampling(),
            PointingTimeSampling::VisibilityTimeCentroid
        );
        assert_eq!(
            law.interpolation(),
            PointingInterpolation::GreatCircleShortestArc
        );
        assert_eq!(law.extrapolation(), PointingExtrapolation::Reject);
        assert_eq!(law.missing(), MissingPointingPolicy::Reject);
    }

    #[test]
    fn standard_primary_beam_model_is_explicit_and_fails_closed() {
        let named = |name: &str| BTreeSet::from([name.to_string()]);
        assert_eq!(
            analytic_primary_beam_model_for_telescopes(&named("EVLA")).expect("EVLA model"),
            AnalyticPrimaryBeamModel::CasaEvlaCommon
        );
        assert_eq!(
            analytic_primary_beam_model_for_telescopes(&named("VLA")).expect("VLA model"),
            AnalyticPrimaryBeamModel::CasaVlaBand
        );
        assert!(analytic_primary_beam_model_for_telescopes(&named("UNKNOWN")).is_err());
        assert!(analytic_primary_beam_model_for_telescopes(&BTreeSet::new()).is_err());
    }
}
