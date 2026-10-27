// SPDX-License-Identifier: LGPL-3.0-or-later
//! The antennas' pointing directions of a time's rows, and the AW pointing
//! pixels of its epoch.

use std::collections::{BTreeMap, BTreeSet};

use casa_observation::{AntennaId, FieldId, PointingCandidate, PointingSeries, VisibilityBatch};
use casa_types::measures::{EpochRef, MjdHighPrec};

use super::{BatchGeometry, BatchGeometryError, GeometryPlan};
use crate::aw_pointing::{AwPointingEpochs, AwPointingGrouping};
use crate::geometry::{
    ObservationPointingLaw, PointingCentreLaw, PointingTimeSampling, SkyDirection,
};
use crate::pointing_law::{PointingSample, PointingSamples};
use crate::selected_observation_sample::SelectedPointingDirections;

/// One time of a batch, as its geometry is evaluated.
pub(super) struct TimeContext<'a, 'b> {
    pub(super) batch: &'a VisibilityBatch<'b>,
    pub(super) time: usize,
    pub(super) time_mjd_s: f64,
    pub(super) field: FieldId,
    /// The phase-tracking direction: the pointing law's fallback.
    pub(super) phase: SkyDirection,
    /// The field's observation direction.
    pub(super) observation: SkyDirection,
    pub(super) rows: &'a [bool],
}

impl TimeContext<'_, '_> {
    /// The time's row samples.
    fn row_samples(&self) -> impl Iterator<Item = usize> + '_ {
        let per_time = self.batch.samples_per_time();
        (self.time * per_time..(self.time + 1) * per_time).filter(|sample| self.rows[*sample])
    }

    /// The time on the partition's time system.
    fn partition_time_s(&self) -> f64 {
        self.batch.time.offsets_s()[self.time]
    }

    fn scale(&self) -> EpochRef {
        self.batch.time.reference()
    }
}

impl GeometryPlan {
    /// The pointing directions of the time's rows under the plan's centre
    /// law.
    pub(super) fn pointing_directions(
        &self,
        context: &TimeContext<'_, '_>,
        series: Option<&PointingSeries>,
        out: &mut BatchGeometry,
    ) -> Result<(), BatchGeometryError> {
        let both = |direction| SelectedPointingDirections {
            antenna1: direction,
            antenna2: direction,
        };
        let law = match &self.pointing {
            PointingCentreLaw::PhaseTrackingCentre => {
                return fill(context, out, both(context.phase));
            }
            PointingCentreLaw::FieldCentre => return fill(context, out, both(context.observation)),
            PointingCentreLaw::Fixed(direction) => return fill(context, out, both(*direction)),
            PointingCentreLaw::Observation(law) => *law,
        };
        let series = series.ok_or(BatchGeometryError::MissingInput("pointing samples"))?;
        let batch = context.batch;
        let per_time = batch.samples_per_time();
        match law.time_sampling() {
            PointingTimeSampling::VisibilityTime => {
                let mut known = BTreeMap::new();
                for sample in context.row_samples() {
                    let baseline = batch.baselines[sample % per_time];
                    let mut direction = |antenna| -> Result<SkyDirection, BatchGeometryError> {
                        if let Some(direction) = known.get(&antenna) {
                            return Ok(*direction);
                        }
                        let direction = self.antenna_direction(
                            series,
                            law,
                            antenna,
                            [context.partition_time_s(), context.time_mjd_s],
                            context.scale(),
                            context.phase,
                        )?;
                        known.insert(antenna, direction);
                        Ok(direction)
                    };
                    out.pointing[sample] = SelectedPointingDirections {
                        antenna1: direction(baseline.antenna1)?,
                        antenna2: direction(baseline.antenna2)?,
                    };
                }
            }
            PointingTimeSampling::VisibilityTimeCentroid => {
                let centroid = batch
                    .timing
                    .centroid
                    .ok_or(BatchGeometryError::MissingInput("time centroids"))?;
                let mjd_origin = MjdHighPrec::new(0.0, 0.0);
                for sample in context.row_samples() {
                    let baseline = batch.baselines[sample % per_time];
                    let times = [
                        centroid.offsets_s()[sample],
                        centroid.seconds_since(sample, mjd_origin),
                    ];
                    let direction = |antenna| {
                        self.antenna_direction(
                            series,
                            law,
                            antenna,
                            times,
                            context.scale(),
                            context.phase,
                        )
                    };
                    out.pointing[sample] = SelectedPointingDirections {
                        antenna1: direction(baseline.antenna1)?,
                        antenna2: direction(baseline.antenna2)?,
                    };
                }
            }
        }
        Ok(())
    }

    /// The time's AW pointing epoch: its rows' antennas' grouped pixels on
    /// each chart, and each row's mean of its two antennas'.
    pub(super) fn aw_pixels(
        &self,
        context: &TimeContext<'_, '_>,
        series: &PointingSeries,
        grouping: &AwPointingGrouping,
        epochs: &mut AwPointingEpochs<AntennaId>,
        out: &mut BatchGeometry,
    ) -> Result<(), BatchGeometryError> {
        let PointingCentreLaw::Observation(law) = self.pointing else {
            return Err(BatchGeometryError::MissingInput(
                "an observation pointing law",
            ));
        };
        let batch = context.batch;
        let per_time = batch.samples_per_time();
        let mut antennas = BTreeSet::new();
        let mut row_count = 0;
        for sample in context.row_samples() {
            let baseline = batch.baselines[sample % per_time];
            antennas.extend([baseline.antenna1, baseline.antenna2]);
            row_count += 1;
        }
        if row_count == 0 {
            return Ok(());
        }
        let directions = antennas
            .into_iter()
            .map(|antenna| {
                self.antenna_direction(
                    series,
                    law,
                    antenna,
                    [context.partition_time_s(), context.time_mjd_s],
                    context.scale(),
                    context.phase,
                )
                .map(|direction| (antenna, direction))
            })
            .collect::<Result<BTreeMap<_, _>, BatchGeometryError>>()?;
        let pixels = grouping.pixels(&directions)?;
        let grouped = epochs.next(grouping, context.field.0, row_count, &pixels)?;
        for sample in context.row_samples() {
            let baseline = batch.baselines[sample % per_time];
            for (chart, pixels) in grouped.iter().enumerate() {
                let [first, second] =
                    [baseline.antenna1, baseline.antenna2].map(|antenna| pixels[&antenna]);
                out.aw_pixels[sample * out.charts + chart] =
                    [0.5 * (first[0] + second[0]), 0.5 * (first[1] + second[1])];
            }
        }
        Ok(())
    }

    /// `antenna`'s direction under `law` at `times` (on the partition's
    /// time system and in MJD seconds), its samples converted to J2000 at
    /// that time from the observatory.
    fn antenna_direction(
        &self,
        series: &PointingSeries,
        law: ObservationPointingLaw,
        antenna: AntennaId,
        [time_s, time_mjd_s]: [f64; 2],
        scale: EpochRef,
        fallback: SkyDirection,
    ) -> Result<SkyDirection, BatchGeometryError> {
        let bracket = series.bracket(antenna, time_s)?;
        let sample = |candidate: Option<PointingCandidate>| {
            candidate
                .map(|candidate| {
                    Ok::<_, BatchGeometryError>(PointingSample {
                        sample: candidate.sample,
                        time_s: candidate.time_s,
                        direction_rad: self.frames.j2000(
                            time_mjd_s,
                            scale,
                            candidate.direction_rad,
                            series.frame,
                        )?,
                    })
                })
                .transpose()
        };
        let samples = PointingSamples {
            covering: sample(bracket.covering)?,
            before: sample(bracket.before)?,
            after: sample(bracket.after)?,
        };
        law.resolve(samples, time_s, fallback)
            .map_err(|law| BatchGeometryError::Pointing {
                antenna,
                time_s,
                law,
            })
    }
}

/// Every row of the time pointed at `directions`.
fn fill(
    context: &TimeContext<'_, '_>,
    out: &mut BatchGeometry,
    directions: SelectedPointingDirections,
) -> Result<(), BatchGeometryError> {
    for sample in context.row_samples() {
        out.pointing[sample] = directions;
    }
    Ok(())
}
