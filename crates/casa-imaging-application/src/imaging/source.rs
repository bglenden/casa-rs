// SPDX-License-Identifier: LGPL-3.0-or-later
//! The selected MeasurementSet as a [`BoundedSource`] of native row blocks.

use casa_imaging_model::{CompiledProblem, SelectedNumericVisibility, SelectedNumericWeights};
use casa_imaging_operator::{PlaneRange, RowContext};
use casa_imaging_runtime::pass::{
    BoundedSource, NativeBlock, NativeRowHeader, RowAddress, SourceError,
};
use casa_ms::{
    BoundSelectedObservation, SelectedObservationBlock, SelectedObservationBlockConsumer,
    SelectedObservationBlockSource, SelectedObservationNumericGeometry,
};
use num_complex::Complex32;

use super::continuum::subtract_continuum;

/// Seconds per day, for row times in MJD days.
const SECONDS_PER_DAY: f64 = 86_400.0;

/// Output-frame frequency bounds of a plane range, for windowed traversals
/// of a cube: the low edge of the first plane to the high edge of the last.
pub(crate) type PlaneBounds = Box<dyn Fn(PlaneRange) -> [f64; 2] + Send + Sync>;

enum Traversal<'a> {
    Idle(BoundSelectedObservation),
    Streaming {
        source: SelectedObservationBlockSource<'a>,
        consumer: SelectedObservationBlockConsumer<'a>,
        block: SelectedObservationBlock,
        geometry: SelectedObservationNumericGeometry,
        windowed: bool,
    },
    Failed,
}

/// Native rows of the selected observation, projected onto one image domain.
///
/// The first traversal reads every selected channel; it proves the selected
/// row sequence, after which a cube wave may restrict a traversal to the
/// channels whose output-frame frequencies reach its planes (casa-ms keeps
/// the straddling pair at each edge, so linear interpolation keeps both
/// partners). Rows CASA excludes from gridding, flagged rows and
/// autocorrelations (`GridFT::put`/`get` with `usezero = false`), arrive with
/// their row flag set. A compiled continuum transform is applied to each row
/// as it is read.
pub(crate) struct MeasurementSetSource<'a> {
    problem: &'a CompiledProblem,
    domain: u32,
    planes: u32,
    bounds: Option<PlaneBounds>,
    proven: bool,
    traversal: Traversal<'a>,
    values: Vec<Complex32>,
    weights: Vec<f32>,
    flags: Vec<bool>,
}

impl<'a> MeasurementSetSource<'a> {
    /// A source over `selected` delivering rows projected on domain 0.
    /// `bounds` enables windowed traversals for a basis of `planes` planes.
    pub(crate) fn new(
        problem: &'a CompiledProblem,
        selected: BoundSelectedObservation,
        planes: u32,
        bounds: Option<PlaneBounds>,
    ) -> Self {
        Self {
            problem,
            domain: 0,
            planes,
            bounds,
            proven: false,
            traversal: Traversal::Idle(selected),
            values: Vec::new(),
            weights: Vec::new(),
            flags: Vec::new(),
        }
    }

    /// Project the rows of later traversals on image domain `domain`.
    pub(crate) fn set_domain(&mut self, domain: u32) {
        self.domain = domain;
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
        out.reset(&channel_indices, &correlation_indices);
        let channels = channel_indices.len();
        let correlations = correlation_indices.len();
        for row in 0..rows {
            let numeric = block.numeric_row(geometry, row)?;
            let projection = numeric
                .row
                .domain_projections()
                .get(self.domain)
                .ok_or("a selected row lacks its image-domain projection")?
                .model();
            let metadata = &numeric.row.metadata;
            let coordinates = &numeric.row.coordinates;
            let header = NativeRowHeader {
                uvw_m: projection.transformed_uvw_m(),
                phase_shift_m: projection.phase_shift_m(),
                row_flag: numeric.row.row_flag || metadata.antenna1 == metadata.antenna2,
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
                    self.values.push(match numeric.visibility {
                        SelectedNumericVisibility::Complex32(values) => values[index],
                        SelectedNumericVisibility::Float32(values) => {
                            Complex32::new(values[index], 0.0)
                        }
                    });
                    self.weights.push(match numeric.weights {
                        SelectedNumericWeights::PerRow(values) => {
                            values[correlation.correlation_index() as usize]
                        }
                        SelectedNumericWeights::PerChannel(values) => values[index],
                    });
                    self.flags.push(numeric.flags[index]);
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
    fn begin(&mut self, planes: PlaneRange) -> Result<(), SourceError> {
        let selected = match std::mem::replace(&mut self.traversal, Traversal::Failed) {
            Traversal::Idle(selected) => selected,
            Traversal::Streaming { .. } | Traversal::Failed => {
                return Err("a traversal began before the previous one finished".into());
            }
        };
        let window = (self.proven && planes != PlaneRange::new(0, self.planes))
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
        self.traversal = Traversal::Streaming {
            source,
            consumer,
            block,
            geometry,
            windowed: window.is_some(),
        };
        Ok(())
    }

    fn fill(&mut self, out: &mut NativeBlock) -> Result<bool, SourceError> {
        loop {
            let Traversal::Streaming {
                mut source,
                mut consumer,
                mut block,
                mut geometry,
                windowed,
            } = std::mem::replace(&mut self.traversal, Traversal::Failed)
            else {
                return Err("no traversal is in progress".into());
            };
            if source.fill_next(&mut block)?.is_none() {
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
            block.project_numeric_geometry(self.problem, &mut geometry)?;
            if geometry.row_count() == 0 {
                self.traversal = Traversal::Streaming {
                    source,
                    consumer,
                    block,
                    geometry,
                    windowed,
                };
                continue;
            }
            let mut converted = Ok(());
            consumer.consume_numeric(&block, &geometry, || {
                converted = self.convert(&block, &geometry, out);
                Ok::<_, std::convert::Infallible>(())
            })?;
            converted?;
            self.traversal = Traversal::Streaming {
                source,
                consumer,
                block,
                geometry,
                windowed,
            };
            return Ok(true);
        }
    }
}
