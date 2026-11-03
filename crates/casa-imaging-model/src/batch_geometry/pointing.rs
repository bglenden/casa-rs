// SPDX-License-Identifier: LGPL-3.0-or-later
//! The antennas' pointing directions of a time's rows, and the AW pointing
//! pixels of its epoch.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use casa_observation::{AntennaId, FieldId, PointingCandidate, PointingSeries, VisibilityBatch};
use casa_types::measures::{EpochRef, MjdHighPrec};

use super::{BatchGeometry, BatchGeometryError, GeometryPlan};
use crate::aw_pointing::{AwPointingEpochs, AwPointingGrouping};
use crate::geometry::{
    DirectionFrame, MissingPointingPolicy, ObservationPointingLaw, PointingCentreLaw,
    PointingTimeSampling, SkyDirection,
};
use crate::pointing_law::{PointingSample, PointingSamples};
use crate::sample_antennas::SelectedPointingDirections;

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
    pub(super) fn row_samples(&self) -> impl Iterator<Item = usize> + '_ {
        let per_time = self.batch.samples_per_time();
        (self.time * per_time..(self.time + 1) * per_time).filter(|sample| self.rows[*sample])
    }

    /// The antennas of the time's rows, in order.
    pub(super) fn row_antennas(&self) -> Vec<AntennaId> {
        let per_time = self.batch.samples_per_time();
        let antennas: BTreeSet<AntennaId> = self
            .row_samples()
            .flat_map(|sample| {
                let baseline = self.batch.baselines[sample % per_time];
                [baseline.antenna1, baseline.antenna2]
            })
            .collect();
        antennas.into_iter().collect()
    }

    /// The time on the partition's time system.
    fn partition_time_s(&self) -> f64 {
        self.batch.time.offsets_s()[self.time]
    }

    fn scale(&self) -> EpochRef {
        self.batch.time.reference()
    }
}

/// The antennas' pointing directions at one time, each resolved once: at
/// the visibility time (for the rows and the AW epoch alike) and at each
/// sample's time centroid.
#[derive(Default)]
pub(super) struct TimeDirections {
    visibility: BTreeMap<AntennaId, SkyDirection>,
    centroid: HashMap<(AntennaId, u64), SkyDirection>,
}

impl TimeDirections {
    pub(super) fn clear(&mut self) {
        self.visibility.clear();
        self.centroid.clear();
    }
}

impl GeometryPlan {
    /// The pointing directions of the time's rows under the plan's centre
    /// law.
    pub(super) fn pointing_directions(
        &self,
        context: &TimeContext<'_, '_>,
        series: Option<&PointingSeries>,
        directions: &mut TimeDirections,
        out: &mut BatchGeometry,
    ) -> Result<(), BatchGeometryError> {
        let both = |direction| SelectedPointingDirections {
            antenna1: direction,
            antenna2: direction,
        };
        let law = match &self.pointing {
            PointingCentreLaw::PhaseTrackingCentre => {
                fill(context, out, both(context.phase));
                return Ok(());
            }
            PointingCentreLaw::FieldCentre => {
                fill(context, out, both(context.observation));
                return Ok(());
            }
            PointingCentreLaw::Fixed(direction) => {
                fill(context, out, both(*direction));
                return Ok(());
            }
            PointingCentreLaw::Observation(law) => *law,
        };
        let series = series.ok_or(BatchGeometryError::MissingInput("pointing samples"))?;
        if !series.store_has_pointing
            && law.missing() == MissingPointingPolicy::FieldCentreWithoutPointingTable
        {
            fill(context, out, both(context.observation));
            return Ok(());
        }
        let batch = context.batch;
        let per_time = batch.samples_per_time();
        let centroid = match law.time_sampling() {
            PointingTimeSampling::VisibilityTime => None,
            PointingTimeSampling::VisibilityTimeCentroid => Some(
                batch
                    .timing
                    .centroid
                    .ok_or(BatchGeometryError::MissingInput("time centroids"))?,
            ),
        };
        let mjd_origin = MjdHighPrec::new(0.0, 0.0);
        for sample in context.row_samples() {
            let baseline = batch.baselines[sample % per_time];
            let mut direction = |antenna| match centroid {
                None => self.visibility_direction(context, series, law, antenna, directions),
                Some(centroid) => {
                    let time_s = centroid.offsets_s()[sample];
                    if let Some(known) = directions.centroid.get(&(antenna, time_s.to_bits())) {
                        return Ok(*known);
                    }
                    let direction = self.antenna_direction(
                        series,
                        law,
                        antenna,
                        [time_s, centroid.seconds_since(sample, mjd_origin)],
                        context.scale(),
                        context.phase,
                    )?;
                    directions
                        .centroid
                        .insert((antenna, time_s.to_bits()), direction);
                    Ok(direction)
                }
            };
            out.pointing[sample] = SelectedPointingDirections {
                antenna1: direction(baseline.antenna1)?,
                antenna2: direction(baseline.antenna2)?,
            };
        }
        Ok(())
    }

    /// The time's rows' antenna-1 pointings placed on each chart
    /// ([`BatchGeometry::chart_pointing`]): each J2000 pointing less the
    /// moving phase centre's shift since the image's epoch, converted to
    /// the chart's frame from the observatory at the time.
    ///
    /// CASA converts the pointing to the image frame first and shifts it
    /// there (`SimplePBConvFunc::toPix`), by the source's motion in the
    /// image frame (`FTMachine::girarUVW`); shifting in J2000 differs by
    /// the frames' relative rotation applied to the shift, below 1e-6 of
    /// it between the equatorial frames.
    pub(super) fn chart_pointings(
        &self,
        context: &TimeContext<'_, '_>,
        out: &mut BatchGeometry,
    ) -> Result<(), BatchGeometryError> {
        let time = (context.time_mjd_s, context.scale());
        let [dx, dy] = self
            .moving_shift(context.time_mjd_s, context.scale(), context.phase)?
            .unwrap_or([0.0; 2]);
        // Rows share their antenna-1 pointing under the field-centre law:
        // each distinct direction is converted once.
        let mut placed: Vec<([u64; 2], Vec<SkyDirection>)> = Vec::new();
        for sample in context.row_samples() {
            let pointing = out.pointing[sample].antenna1;
            let key = [
                pointing.longitude_rad().to_bits(),
                pointing.latitude_rad().to_bits(),
            ];
            let index = match placed.iter().position(|(known, _)| *known == key) {
                Some(index) => index,
                None => {
                    let shifted = [pointing.longitude_rad() - dx, pointing.latitude_rad() - dy];
                    let on_charts = self
                        .charts
                        .iter()
                        .map(|chart| {
                            let frame = chart.centre.frame();
                            let [longitude, latitude] = self.frames.convert(
                                time,
                                shifted,
                                DirectionFrame::J2000.direction_ref(),
                                frame.direction_ref(),
                            )?;
                            Ok(SkyDirection::new(frame, longitude, latitude))
                        })
                        .collect::<Result<Vec<_>, BatchGeometryError>>()?;
                    placed.push((key, on_charts));
                    placed.len() - 1
                }
            };
            let charts = out.charts;
            out.chart_pointing[sample * charts..(sample + 1) * charts]
                .copy_from_slice(&placed[index].1);
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
        directions: &mut TimeDirections,
        epochs: &mut AwPointingEpochs<AntennaId>,
        out: &mut BatchGeometry,
    ) -> Result<(), BatchGeometryError> {
        let PointingCentreLaw::Observation(law) = self.pointing else {
            return Err(BatchGeometryError::MissingInput(
                "an observation pointing law",
            ));
        };
        let row_count = context.row_samples().count();
        if row_count == 0 {
            return Ok(());
        }
        let resolved = context
            .row_antennas()
            .into_iter()
            .map(|antenna| {
                self.visibility_direction(context, series, law, antenna, directions)
                    .map(|direction| (antenna, direction))
            })
            .collect::<Result<BTreeMap<_, _>, BatchGeometryError>>()?;
        let pixels = grouping.pixels(&resolved)?;
        let grouped = epochs.next(grouping, context.field.0, row_count, &pixels)?;
        let per_time = context.batch.samples_per_time();
        for sample in context.row_samples() {
            let baseline = context.batch.baselines[sample % per_time];
            for (chart, pixels) in grouped.iter().enumerate() {
                let [first, second] =
                    [baseline.antenna1, baseline.antenna2].map(|antenna| pixels[&antenna]);
                out.aw_pixels[sample * out.charts + chart] =
                    [0.5 * (first[0] + second[0]), 0.5 * (first[1] + second[1])];
            }
        }
        Ok(())
    }

    /// `antenna`'s direction at the visibility time, resolved once a time.
    fn visibility_direction(
        &self,
        context: &TimeContext<'_, '_>,
        series: &PointingSeries,
        law: ObservationPointingLaw,
        antenna: AntennaId,
        directions: &mut TimeDirections,
    ) -> Result<SkyDirection, BatchGeometryError> {
        if let Some(known) = directions.visibility.get(&antenna) {
            return Ok(*known);
        }
        let direction = self.antenna_direction(
            series,
            law,
            antenna,
            [context.partition_time_s(), context.time_mjd_s],
            context.scale(),
            context.phase,
        )?;
        directions.visibility.insert(antenna, direction);
        Ok(direction)
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
) {
    for sample in context.row_samples() {
        out.pointing[sample] = directions;
    }
}
