// SPDX-License-Identifier: LGPL-3.0-or-later
//! Row geometry over observation batches: what the imaging operators need
//! of each sample, compiled once from the observation's catalogue and
//! applied to each [`VisibilityBatch`].
//!
//! For each row sample (a time and baseline the caller images) and each
//! chart, the stored UVW reprojected to the chart's phase centre with the
//! phase shift that goes with it ([`GridFtProjection`]); the antennas'
//! pointing directions; their parallactic angles; on AW-projection runs
//! with pointing, the grouped pointing pixel on each chart; and the
//! antennas' aperture classes. For each time, the channel frequencies in
//! the output frame. Each quantity is computed as often as it changes: a
//! reprojection per field and chart, a parallactic angle per antenna,
//! field and time, a frequency conversion per time.
//!
//! The arithmetic is the selected-observation route's, statement for
//! statement, through the same shared pieces ([`GridFtProjection`],
//! [`ObservationPointingLaw::resolve`](crate::ObservationPointingLaw::resolve),
//! [`AwPointingEpochs`], `casa-types` Measures), so the two routes agree
//! to rounding on an MSv2.
//!
//! AW pointing groups carry state from epoch to epoch (a time of one
//! partition), in the order the batches arrive: partition by partition,
//! each in time order. [`GeometryTraversal`] holds it for one traversal.

mod frames;
mod pointing;
mod projection;

use std::sync::Arc;

use casa_observation::{AntennaId, Catalogue, FieldId, PointingSeries, VisibilityBatch};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{
    MeasureError, MeasuresProvider, MjdHighPrec, PreparedFrequencyConversion,
};

use self::frames::Frames;
use self::projection::{Chart, direction_ref};
use crate::aw_pointing::{AwPointingEpochs, AwPointingError, AwPointingGrouping};
use crate::compiled_problem::{CompiledProblem, InstrumentModel};
use crate::geometry::{
    DirectionFrame, FrequencyFrame, PhaseCentreLaw, PointingCentreLaw, SkyDirection,
};
use crate::pointing_law::PointingLawError;
use crate::selected_observation_sample::{
    AntennaResponseClass, SelectedAntennaResponses, SelectedPointingDirections,
};
use crate::uvw_projection::GridFtProjection;

/// Why batch geometry cannot be formed.
#[derive(Debug, thiserror::Error)]
pub enum BatchGeometryError {
    /// The catalogue lacks what geometry needs.
    #[error("the observation's catalogue {0}")]
    Catalogue(&'static str),
    /// A fixed phase or pointing centre outside J2000.
    #[error("a fixed centre in {0:?} (imaging geometry takes J2000)")]
    FixedCentreFrame(DirectionFrame),
    /// A faceted domain under the mosaic UVW law.
    #[error("a faceted image domain under the mosaic UVW law")]
    FacetedMosaic,
    /// A batch without what geometry reads: stored UVW, time centroids for
    /// centroid-sampled pointing, or pointing samples for an observation
    /// pointing law.
    #[error("the batch carries no {0}")]
    MissingInput(&'static str),
    /// A rest-frame output frequency without a source velocity.
    #[error("rest-frame frequencies need a source velocity")]
    RestFrameWithoutVelocity,
    /// An antenna the instrument model gives no aperture class.
    #[error("{0:?} has no CASA aperture class")]
    UnclassifiedAntenna(AntennaId),
    /// A Measures conversion failed.
    #[error(transparent)]
    Measures(#[from] MeasureError),
    /// The pointing samples could not be read or evaluated.
    #[error(transparent)]
    Observation(#[from] casa_observation::ObservationError),
    /// The pointing law gives no direction.
    #[error("{antenna:?} at {time_s} s: {law}")]
    Pointing {
        /// The antenna.
        antenna: AntennaId,
        /// The query time, on the partition's time system.
        time_s: f64,
        /// Why.
        law: PointingLawError,
    },
    /// AW pointing pixels could not be formed.
    #[error(transparent)]
    AwPointing(#[from] AwPointingError),
}

/// One chart's reprojection of one sample.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ChartProjection {
    /// The reprojected UVW in metres.
    pub uvw_m: [f64; 3],
    /// The phase shift in metres the gridder applies.
    pub phase_shift_m: f64,
}

impl ChartProjection {
    const ABSENT: Self = Self {
        uvw_m: [f64::NAN; 3],
        phase_shift_m: f64::NAN,
    };
}

/// The geometry of one problem on one observation, compiled once.
pub struct GeometryPlan {
    frames: Frames,
    phase: PhaseCentreLaw,
    pointing: PointingCentreLaw,
    charts: Vec<Chart>,
    mosaic: bool,
    output_frame: FrequencyRef,
    parallactic: bool,
    responses: Responses,
    aw: Option<AwPointingGrouping>,
}

/// The aperture classes the instrument model assigns.
enum Responses {
    None,
    Aca7m,
    Heterogeneous {
        classes: Vec<Option<AntennaResponseClass>>,
        envelope: Option<AntennaResponseClass>,
    },
}

/// One traversal's state: the AW pointing groups carried from epoch to
/// epoch. A traversal starts from a new one, so every pass over the same
/// batches gives the same pixels.
#[derive(Default)]
pub struct GeometryTraversal {
    aw: AwPointingEpochs<AntennaId>,
}

/// The geometry of one batch, in buffers reused from batch to batch.
/// Samples that are not rows hold no geometry (NaN).
#[derive(Default)]
pub struct BatchGeometry {
    charts: usize,
    channels: usize,
    projections: Vec<ChartProjection>,
    pointing: Vec<SelectedPointingDirections>,
    parallactic: Vec<[f64; 2]>,
    aw_pixels: Vec<[f64; 2]>,
    responses: Vec<Option<SelectedAntennaResponses>>,
    frequencies_hz: Vec<f64>,
}

impl BatchGeometry {
    /// Sample `sample`'s reprojection to chart `chart`.
    #[must_use]
    pub fn projection(&self, sample: usize, chart: usize) -> ChartProjection {
        self.projections[sample * self.charts + chart]
    }

    /// Sample `sample`'s antennas' pointing directions (J2000).
    #[must_use]
    pub fn pointing(&self, sample: usize) -> SelectedPointingDirections {
        self.pointing[sample]
    }

    /// Sample `sample`'s antennas' parallactic angles as the polarization
    /// operator takes them (negated), when the problem needs them.
    #[must_use]
    pub fn parallactic_angles(&self, sample: usize) -> Option<[f64; 2]> {
        self.parallactic.get(sample).copied()
    }

    /// Sample `sample`'s AW pointing pixel on chart `chart`, on an AW run
    /// with pointing.
    #[must_use]
    pub fn aw_pointing_pixel(&self, sample: usize, chart: usize) -> Option<[f64; 2]> {
        self.aw_pixels.get(sample * self.charts + chart).copied()
    }

    /// Sample `sample`'s antennas' aperture classes, when the instrument
    /// model assigns them.
    #[must_use]
    pub fn antenna_responses(&self, sample: usize) -> Option<SelectedAntennaResponses> {
        self.responses.get(sample).copied().flatten()
    }

    /// Time `time`'s channel frequencies in the output frame.
    #[must_use]
    pub fn frequencies_hz(&self, time: usize) -> &[f64] {
        &self.frequencies_hz[time * self.channels..(time + 1) * self.channels]
    }
}

impl GeometryPlan {
    /// The geometry of `problem` on the observation `catalogue` describes,
    /// converting with `measures`.
    ///
    /// # Errors
    ///
    /// [`BatchGeometryError`] for a catalogue without what geometry needs,
    /// a centre law it does not take, or a failed conversion.
    pub fn compile(
        problem: &CompiledProblem,
        catalogue: &Catalogue,
        measures: Arc<dyn MeasuresProvider>,
    ) -> Result<Self, BatchGeometryError> {
        let frames = Frames::new(catalogue, measures)?;
        let centres = problem.geometry().centres();
        for fixed in [
            match centres.phase_tracking() {
                PhaseCentreLaw::Fixed(direction) => Some(*direction),
                _ => None,
            },
            match centres.pointing() {
                PointingCentreLaw::Fixed(direction) => Some(*direction),
                _ => None,
            },
        ]
        .into_iter()
        .flatten()
        {
            if fixed.frame() != DirectionFrame::J2000 {
                return Err(BatchGeometryError::FixedCentreFrame(fixed.frame()));
            }
        }
        let (charts, mosaic) = projection::charts(problem)?;
        let responses = match problem.science().instrument_model() {
            None | Some(InstrumentModel::CasaEvlaWidebandAwV1) => Responses::None,
            Some(InstrumentModel::CasaAca7mInterferometricDirectPbV1) => Responses::Aca7m,
            Some(InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1) => {
                let classes = frames.alma_aca_classes(catalogue);
                let envelope = [
                    AntennaResponseClass::CasaAlma12m,
                    AntennaResponseClass::CasaAca7m,
                ]
                .into_iter()
                .find(|class| classes.contains(&Some(*class)));
                Responses::Heterogeneous { classes, envelope }
            }
        };
        Ok(Self {
            frames,
            phase: centres.phase_tracking().clone(),
            pointing: centres.pointing().clone(),
            charts,
            mosaic,
            output_frame: frequency_ref(problem.geometry().spectral().output_frame()),
            parallactic: problem.requires_parallactic_angles(),
            responses,
            aw: AwPointingGrouping::new(problem)?,
        })
    }

    /// The number of charts, domain-major with each faceted domain's facets
    /// in turn.
    #[must_use]
    pub fn charts(&self) -> usize {
        self.charts.len()
    }

    /// Whether [`Self::apply`] needs the batch's pointing samples.
    #[must_use]
    pub fn reads_pointing(&self) -> bool {
        matches!(self.pointing, PointingCentreLaw::Observation(_))
    }

    /// The geometry of `batch`'s row samples (`rows`, one flag a sample)
    /// into `out`; `pointing` holds the batch partition's samples over its
    /// times when [`Self::reads_pointing`].
    ///
    /// # Errors
    ///
    /// [`BatchGeometryError`] for a batch without what geometry reads, a
    /// failed conversion, or a pointing law that gives no direction.
    ///
    /// # Panics
    ///
    /// If `rows` does not have one flag for each sample.
    pub fn apply(
        &self,
        batch: &VisibilityBatch<'_>,
        rows: &[bool],
        pointing: Option<&PointingSeries>,
        traversal: &mut GeometryTraversal,
        out: &mut BatchGeometry,
    ) -> Result<(), BatchGeometryError> {
        assert_eq!(rows.len(), batch.samples(), "one row flag a sample");
        let uvw = batch
            .group
            .uvw
            .ok_or(BatchGeometryError::MissingInput("UVW"))?;
        self.reset(batch, out);
        let partition = batch.catalogue.partition(batch.partition);
        let scale = batch.time.reference();
        let conversion_source = partition.frequency_frame;
        let samples_per_time = batch.samples_per_time();
        let mjd_origin = MjdHighPrec::new(0.0, 0.0);
        let mut reprojections: Option<(FieldId, Vec<Option<GridFtProjection>>)> = None;
        for time in 0..batch.times() {
            let time_mjd_s = batch.time.seconds_since(time, mjd_origin);
            let field = batch.field[time];
            let field_direction = self.frames.field(field);
            let (longitude, latitude) = field_direction.as_angles();
            let observation = SkyDirection::new(DirectionFrame::J2000, longitude, latitude);
            let phase = self.phase_direction(time_mjd_s, scale, observation)?;
            let at_time = self.time_varying();
            if at_time
                || reprojections
                    .as_ref()
                    .is_none_or(|(known, _)| *known != field)
            {
                let shift = self.moving_shift(time_mjd_s, scale, phase)?;
                let machines = self
                    .charts
                    .iter()
                    .map(|chart| {
                        let mut target = self.frames.j2000(
                            time_mjd_s,
                            scale,
                            [chart.centre.longitude_rad(), chart.centre.latitude_rad()],
                            direction_ref(chart.centre.frame()),
                        )?;
                        if let Some([longitude, latitude]) = shift {
                            target[0] += longitude;
                            target[1] += latitude;
                        }
                        Ok(projection::reprojection(
                            observation,
                            field_direction,
                            target,
                            chart.faceted,
                            self.mosaic,
                        ))
                    })
                    .collect::<Result<Vec<_>, BatchGeometryError>>()?;
                reprojections = Some((field, machines));
            }
            let machines = &reprojections.as_ref().expect("reprojections formed").1;
            let samples = time * samples_per_time..(time + 1) * samples_per_time;
            for sample in samples.clone().filter(|sample| rows[*sample]) {
                let raw = uvw.metres_at(sample);
                for (chart, machine) in machines.iter().enumerate() {
                    let (uvw_m, phase_shift_m) =
                        machine.map_or((raw, 0.0), |machine| machine.apply(raw));
                    out.projections[sample * out.charts + chart] = ChartProjection {
                        uvw_m,
                        phase_shift_m,
                    };
                }
                let baseline = batch.baselines[sample % samples_per_time];
                if self.parallactic {
                    let angle = |antenna| {
                        self.frames
                            .parallactic_angle(time_mjd_s, scale, field, antenna)
                            .map(|angle| -angle)
                    };
                    out.parallactic[sample] =
                        [angle(baseline.antenna1)?, angle(baseline.antenna2)?];
                }
                if let Some(responses) = out.responses.get_mut(sample) {
                    *responses = self.antenna_responses(baseline.antenna1, baseline.antenna2)?;
                }
            }
            let context = pointing::TimeContext {
                batch,
                time,
                time_mjd_s,
                field,
                phase,
                observation,
                rows,
            };
            self.pointing_directions(&context, pointing, out)?;
            if let Some(grouping) = &self.aw {
                let series =
                    pointing.ok_or(BatchGeometryError::MissingInput("pointing samples"))?;
                self.aw_pixels(&context, series, grouping, &mut traversal.aw, out)?;
            }
            let conversion =
                self.frequency_conversion(time_mjd_s, scale, field_direction, conversion_source)?;
            let channels = out.channels;
            for (slot, frequency) in out.frequencies_hz[time * channels..(time + 1) * channels]
                .iter_mut()
                .zip(batch.frequency.hz())
            {
                *slot = conversion.convert_hz(*frequency);
            }
        }
        Ok(())
    }

    /// Size `out` for `batch`, every sample without geometry.
    fn reset(&self, batch: &VisibilityBatch<'_>, out: &mut BatchGeometry) {
        let samples = batch.samples();
        out.charts = self.charts.len();
        out.channels = batch.channel_count();
        out.projections.clear();
        out.projections
            .resize(samples * out.charts, ChartProjection::ABSENT);
        let nowhere = SkyDirection::new(DirectionFrame::J2000, f64::NAN, f64::NAN);
        out.pointing.clear();
        out.pointing.resize(
            samples,
            SelectedPointingDirections {
                antenna1: nowhere,
                antenna2: nowhere,
            },
        );
        out.parallactic.clear();
        if self.parallactic {
            out.parallactic.resize(samples, [f64::NAN; 2]);
        }
        out.aw_pixels.clear();
        if self.aw.is_some() {
            out.aw_pixels.resize(samples * out.charts, [f64::NAN; 2]);
        }
        out.responses.clear();
        if !matches!(self.responses, Responses::None) {
            out.responses.resize(samples, None);
        }
        out.frequencies_hz.clear();
        out.frequencies_hz
            .resize(batch.times() * out.channels, f64::NAN);
    }

    /// Whether the charts' targets move with time: a moving phase centre,
    /// or a chart centre outside J2000.
    fn time_varying(&self) -> bool {
        matches!(self.phase, PhaseCentreLaw::Ephemeris(_))
            || self
                .charts
                .iter()
                .any(|chart| chart.centre.frame() != DirectionFrame::J2000)
    }

    /// The phase-tracking direction at `time_mjd_s` of rows observed in
    /// `observation`.
    fn phase_direction(
        &self,
        time_mjd_s: f64,
        scale: casa_types::measures::EpochRef,
        observation: SkyDirection,
    ) -> Result<SkyDirection, BatchGeometryError> {
        Ok(match &self.phase {
            PhaseCentreLaw::Observation => observation,
            PhaseCentreLaw::Fixed(direction) => *direction,
            PhaseCentreLaw::Ephemeris(name) => {
                let (longitude, latitude) = self.frames.named(time_mjd_s, scale, name)?.as_angles();
                SkyDirection::new(DirectionFrame::J2000, longitude, latitude)
            }
        })
    }

    /// Under a moving phase centre, its offset at `time_mjd_s` from the
    /// first chart's centre, which every chart's target follows.
    fn moving_shift(
        &self,
        time_mjd_s: f64,
        scale: casa_types::measures::EpochRef,
        phase: SkyDirection,
    ) -> Result<Option<[f64; 2]>, BatchGeometryError> {
        if !matches!(self.phase, PhaseCentreLaw::Ephemeris(_)) {
            return Ok(None);
        }
        let anchor = self
            .charts
            .first()
            .ok_or(BatchGeometryError::Catalogue(
                "a problem has no image domain",
            ))?
            .centre;
        let anchor = self.frames.j2000(
            time_mjd_s,
            scale,
            [anchor.longitude_rad(), anchor.latitude_rad()],
            direction_ref(anchor.frame()),
        )?;
        Ok(Some([
            phase.longitude_rad() - anchor[0],
            phase.latitude_rad() - anchor[1],
        ]))
    }

    /// The aperture classes of a baseline's antennas and their family.
    fn antenna_responses(
        &self,
        antenna1: AntennaId,
        antenna2: AntennaId,
    ) -> Result<Option<SelectedAntennaResponses>, BatchGeometryError> {
        Ok(match &self.responses {
            Responses::None => None,
            Responses::Aca7m => Some(SelectedAntennaResponses {
                antenna1: AntennaResponseClass::CasaAca7m,
                antenna2: AntennaResponseClass::CasaAca7m,
                family_envelope: AntennaResponseClass::CasaAca7m,
            }),
            Responses::Heterogeneous { classes, envelope } => {
                let class = |antenna: AntennaId| {
                    classes
                        .get(antenna.0 as usize)
                        .copied()
                        .flatten()
                        .ok_or(BatchGeometryError::UnclassifiedAntenna(antenna))
                };
                Some(SelectedAntennaResponses {
                    antenna1: class(antenna1)?,
                    antenna2: class(antenna2)?,
                    family_envelope: envelope
                        .ok_or(BatchGeometryError::UnclassifiedAntenna(antenna1))?,
                })
            }
        })
    }

    /// The conversion of channel frequencies at `time_mjd_s` from the
    /// partition's frame to the output frame, in the observatory's frame
    /// towards the field (CASA's visibility iterator converts towards the
    /// field's own direction, whatever the imaging phase centre).
    fn frequency_conversion(
        &self,
        time_mjd_s: f64,
        scale: casa_types::measures::EpochRef,
        field: &casa_types::measures::direction::MDirection,
        source: FrequencyRef,
    ) -> Result<PreparedFrequencyConversion, BatchGeometryError> {
        if source == self.output_frame {
            return Ok(PreparedFrequencyConversion::new(
                source, source, None, None,
            )?);
        }
        if self.output_frame == FrequencyRef::REST {
            return Err(BatchGeometryError::RestFrameWithoutVelocity);
        }
        let frame = self
            .frames
            .observatory_at(time_mjd_s, scale)
            .with_direction(field.clone());
        Ok(PreparedFrequencyConversion::new(
            source,
            self.output_frame,
            Some(&frame),
            Some(&frame),
        )?)
    }
}

const fn frequency_ref(frame: FrequencyFrame) -> FrequencyRef {
    match frame {
        FrequencyFrame::Rest => FrequencyRef::REST,
        FrequencyFrame::Topocentric => FrequencyRef::TOPO,
        FrequencyFrame::Barycentric => FrequencyRef::BARY,
        FrequencyFrame::Lsrk => FrequencyRef::LSRK,
    }
}
