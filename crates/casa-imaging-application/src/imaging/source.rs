// SPDX-License-Identifier: LGPL-3.0-or-later
//! The selected MeasurementSet as a [`BoundedSource`] of native row blocks.

use casa_imaging_model::{
    CompiledProblem, FiniteValuePolicy, SelectedNumericVisibility, SelectedNumericWeights,
};
use casa_imaging_operator::{PlaneRange, RowContext};
use casa_imaging_runtime::pass::{
    BoundedSource, DomainProjection, NativeBlock, NativeRowHeader, RowAddress, SourceError,
};
use casa_ms::{
    BoundSelectedObservation, SelectedObservationBlock, SelectedObservationBlockConsumer,
    SelectedObservationBlockSource, SelectedObservationNumericGeometry,
};
use num_complex::Complex32;

use super::continuum::subtract_continuum;

/// Seconds per day, for row times in MJD days.
const SECONDS_PER_DAY: f64 = 86_400.0;

/// Under [`FiniteValuePolicy::RejectAll`] a non-finite visibility, weight or
/// uvw fails the run, flagged or not; under
/// [`FiniteValuePolicy::FlagInputRejectGenerated`] it is flagged.
const NONFINITE_INPUT: &str = "a selected visibility, weight or uvw is not finite";

/// Output-frame frequency bounds of a plane range, for windowed traversals
/// of a cube: the low edge of the first plane to the high edge of the last.
pub(crate) type PlaneBounds = Box<dyn Fn(PlaneRange) -> [f64; 2] + Send + Sync>;

#[expect(
    clippy::large_enum_variant,
    reason = "one state per source, idle once per pass; the per-block stream is boxed"
)]
enum Traversal<'a> {
    Idle(BoundSelectedObservation),
    Streaming(Box<Stream<'a>>),
    Failed,
}

struct Stream<'a> {
    source: SelectedObservationBlockSource<'a>,
    consumer: SelectedObservationBlockConsumer<'a>,
    block: SelectedObservationBlock,
    geometry: SelectedObservationNumericGeometry,
    windowed: bool,
}

/// Native rows of the selected observation, projected onto every image
/// domain.
///
/// The first traversal reads every selected channel; it proves the selected
/// row sequence, after which a restricted cube wave may read only the
/// channels whose output-frame frequencies reach its planes (casa-ms keeps
/// the straddling pair at each edge, so linear interpolation keeps both
/// partners). A compiled continuum transform fits each whole row, so its
/// traversals are never restricted. Rows CASA excludes from imaging and
/// flagged rows arrive with their row flag set; autocorrelations arrive
/// unflagged, since CASA weighs them in the density grids and only
/// `GridFT` drops them.
pub(crate) struct MeasurementSetSource<'a> {
    problem: &'a CompiledProblem,
    domains: usize,
    planes: u32,
    bounds: Option<PlaneBounds>,
    proven: bool,
    traversal: Traversal<'a>,
    projections: Vec<DomainProjection>,
    values: Vec<Complex32>,
    weights: Vec<f32>,
    flags: Vec<bool>,
}

impl<'a> MeasurementSetSource<'a> {
    /// A source over `selected` delivering rows projected on every domain.
    /// `bounds` enables restricted traversals for a basis of `planes` planes.
    pub(crate) fn new(
        problem: &'a CompiledProblem,
        selected: BoundSelectedObservation,
        planes: u32,
        bounds: Option<PlaneBounds>,
    ) -> Self {
        Self {
            problem,
            domains: problem.geometry().domains().len(),
            planes,
            bounds,
            proven: false,
            traversal: Traversal::Idle(selected),
            projections: Vec::new(),
            values: Vec::new(),
            weights: Vec::new(),
            flags: Vec::new(),
        }
    }

    fn convert(
        &mut self,
        block: &SelectedObservationBlock,
        geometry: &SelectedObservationNumericGeometry,
        out: &mut NativeBlock,
    ) -> Result<(), SourceError> {
        let rows = geometry.row_count();
        let first = block.numeric_row(geometry, 0)?;
        let channel_indices = first
            .channels
            .iter()
            .map(|channel| channel.channel_index)
            .collect::<Vec<_>>();
        let correlation_indices = first
            .correlations
            .iter()
            .map(|correlation| correlation.correlation_index())
            .collect::<Vec<_>>();
        out.reset(self.domains, &channel_indices, &correlation_indices);
        let channels = channel_indices.len();
        let correlations = correlation_indices.len();
        let reject_nonfinite = matches!(
            self.problem.numerics().finite_values(),
            FiniteValuePolicy::RejectAll
        );
        for row in 0..rows {
            let numeric = block.numeric_row(geometry, row)?;
            self.projections.clear();
            for domain in 0..self.domains {
                let projection = numeric
                    .row
                    .domain_projections()
                    .get(domain as u32)
                    .ok_or("a selected row lacks an image-domain projection")?
                    .model();
                self.projections.push(DomainProjection {
                    uvw_m: projection.transformed_uvw_m(),
                    phase_shift_m: projection.phase_shift_m(),
                });
            }
            let metadata = &numeric.row.metadata;
            let coordinates = &numeric.row.coordinates;
            let nonfinite_uvw = !self.projections.iter().all(|projection| {
                projection.uvw_m.iter().all(|value| value.is_finite())
                    && projection.phase_shift_m.is_finite()
            });
            if nonfinite_uvw && reject_nonfinite {
                return Err(NONFINITE_INPUT.into());
            }
            let header = NativeRowHeader {
                row_flag: numeric.row.row_flag || nonfinite_uvw,
                context: RowContext {
                    time_s: coordinates.time.mjd_days() * SECONDS_PER_DAY,
                    antennas: [metadata.antenna1 as u32, metadata.antenna2 as u32],
                    parallactic_angle_rad: coordinates.parallactic_angles_rad.unwrap_or([0.0; 2]),
                    field: metadata.field_id as u32,
                    pointing_offset_rad: [0.0; 2],
                },
                address: RowAddress {
                    physical_row: numeric.row.physical_row,
                    data_description: numeric.row.data_description_id,
                },
            };
            self.values.clear();
            self.weights.clear();
            self.flags.clear();
            let stride = numeric.stored_correlations;
            for channel in numeric.channels {
                let stored = (channel.channel_index - numeric.first_stored_channel) as usize;
                for correlation in numeric.correlations {
                    let index = stored * stride + correlation.correlation_index() as usize;
                    let value = match numeric.visibility {
                        SelectedNumericVisibility::Complex32(values) => values[index],
                        SelectedNumericVisibility::Float32(values) => {
                            Complex32::new(values[index], 0.0)
                        }
                    };
                    let weight = match numeric.weights {
                        SelectedNumericWeights::PerRow(values) => {
                            values[correlation.correlation_index() as usize]
                        }
                        SelectedNumericWeights::PerChannel(values) => values[index],
                    };
                    let nonfinite =
                        !(value.re.is_finite() && value.im.is_finite() && weight.is_finite());
                    if nonfinite && reject_nonfinite {
                        return Err(NONFINITE_INPUT.into());
                    }
                    self.values.push(value);
                    self.weights.push(weight);
                    self.flags.push(numeric.flags[index] || nonfinite);
                }
            }
            debug_assert_eq!(self.values.len(), channels * correlations);
            if let Some(rule) = self.problem.visibility_transform().and_then(|transform| {
                transform.rule(metadata.field_id, numeric.row.spectral_window_id)
            }) {
                subtract_continuum(
                    rule,
                    &numeric,
                    &mut self.values,
                    &self.weights,
                    &mut self.flags,
                )?;
            }
            out.push_row(
                header,
                &self.projections,
                &geometry.frequencies_hz()[row * channels..(row + 1) * channels],
                &self.values,
                &self.weights,
                &self.flags,
            );
        }
        Ok(())
    }
}

impl BoundedSource for MeasurementSetSource<'_> {
    fn begin(&mut self, planes: PlaneRange, restrict: bool) -> Result<(), SourceError> {
        let selected = match std::mem::replace(&mut self.traversal, Traversal::Failed) {
            Traversal::Idle(selected) => selected,
            Traversal::Streaming(_) | Traversal::Failed => {
                return Err("a traversal began before the previous one finished".into());
            }
        };
        let window = (restrict
            && self.proven
            && self.problem.visibility_transform().is_none()
            && planes != PlaneRange::new(0, self.planes))
        .then(|| self.bounds.as_ref().map(|bounds| bounds(planes)))
        .flatten();
        let (source, consumer) = match window {
            Some(bounds) => selected.into_windowed_block_stream(self.problem, bounds)?,
            None => selected.into_block_stream(self.problem)?,
        };
        let block = source.create_storage(0);
        let channels = self
            .problem
            .selected_observation()
            .read_set()
            .sources()
            .iter()
            .flat_map(|source| source.selection().spectral_windows())
            .map(|window| window.channel_indices().len())
            .max()
            .unwrap_or(0);
        let geometry =
            SelectedObservationNumericGeometry::new(source.maximum_rows_per_block(), channels)?;
        self.traversal = Traversal::Streaming(Box::new(Stream {
            source,
            consumer,
            block,
            geometry,
            windowed: window.is_some(),
        }));
        Ok(())
    }

    fn fill(&mut self, out: &mut NativeBlock) -> Result<bool, SourceError> {
        loop {
            let Traversal::Streaming(mut stream) =
                std::mem::replace(&mut self.traversal, Traversal::Failed)
            else {
                return Err("no traversal is in progress".into());
            };
            let Stream {
                source,
                consumer,
                block,
                geometry,
                ..
            } = &mut *stream;
            if source.fill_next(block)?.is_none() {
                let Stream {
                    source,
                    consumer,
                    windowed,
                    ..
                } = *stream;
                let terminal = source.complete()?;
                let selected = if windowed {
                    consumer.complete_window(terminal)?.0
                } else {
                    self.proven = true;
                    consumer.complete(terminal)?.0
                };
                self.traversal = Traversal::Idle(selected);
                return Ok(false);
            }
            block.project_numeric_geometry(self.problem, geometry)?;
            if geometry.row_count() == 0 {
                self.traversal = Traversal::Streaming(stream);
                continue;
            }
            let mut converted = Ok(());
            consumer.consume_numeric(block, geometry, || {
                converted = self.convert(block, geometry, out);
                Ok::<_, std::convert::Infallible>(())
            })?;
            converted?;
            self.traversal = Traversal::Streaming(stream);
            return Ok(true);
        }
    }
}
