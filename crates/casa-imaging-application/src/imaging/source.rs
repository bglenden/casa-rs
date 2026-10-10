// SPDX-License-Identifier: LGPL-3.0-or-later
//! The selected MeasurementSet as a [`BoundedSource`] of native row blocks.

use casa_imaging_model::{
    AntennaResponseClass, CompiledProblem, DirectionCoordinateSpec, FiniteValuePolicy,
    SelectedAntennaResponses, SelectedNumericVisibility, SelectedNumericWeights, SkyDirection,
};
use casa_imaging_operator::{PlaneRange, RowContext};
use casa_imaging_reconstruction::direction_world_to_pixel;
use casa_imaging_runtime::pass::{
    BoundedSource, DomainProjection, NativeBlock, NativeRowHeader, RowAddress, SourceError,
};
use casa_ms::{
    BoundSelectedObservation, ProjectedObservationBlock, SelectedObservationBlock,
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

/// The mosaic dish index of each antenna of a row: its aperture class's
/// position in the selection's `classes`, the order
/// [`super::measurement::domain_operator`] lists the dishes in; a row
/// without response classes (every problem without an instrument model)
/// is on the first dish.
fn antenna_types(
    responses: Option<SelectedAntennaResponses>,
    classes: &[AntennaResponseClass],
) -> [u8; 2] {
    let index = |class: AntennaResponseClass| {
        classes
            .iter()
            .position(|known| *known == class)
            .map_or(0, |index| u8::try_from(index).expect("few dish classes"))
    };
    responses.map_or([0, 0], |responses| {
        [index(responses.antenna1), index(responses.antenna2)]
    })
}

/// The offset of a row's pointing from `direction`'s reference pixel as
/// direction cosines along the image axes: the A-projection pointing pixel
/// when the selection evaluated one, else the antenna-1 pointing direction
/// on the image plane (`SimplePBConvFunc::findConvFunction` takes the
/// buffer's first `direction1`; under tclean `usepointing=False` the
/// selection evaluates it as the field's phase-tracking centre).
fn pointing_offset(
    direction: DirectionCoordinateSpec,
    aw_pixel: Option<[f64; 2]>,
    pointing: SkyDirection,
) -> Result<[f64; 2], SourceError> {
    let pixel = match aw_pixel {
        Some(pixel) => pixel,
        None => {
            if pointing.frame() != direction.reference_direction().frame() {
                return Err("a row's pointing direction is not in the image frame".into());
            }
            direction_world_to_pixel(
                direction,
                [pointing.longitude_rad(), pointing.latitude_rad()],
            )?
        }
    };
    let reference = direction.reference_pixel();
    let increment = direction.increment_rad();
    Ok([
        (pixel[0] - reference[0]) * increment[0],
        (pixel[1] - reference[1]) * increment[1],
    ])
}

enum Traversal<'a> {
    Idle(BoundSelectedObservation),
    Streaming(Box<Stream<'a>>),
    Failed,
}

struct Stream<'a> {
    source: SelectedObservationBlockSource<'a>,
    block: SelectedObservationBlock,
    geometry: SelectedObservationNumericGeometry,
}

/// Native rows of the selected observation, projected onto every image
/// domain.
///
/// A restricted cube wave reads only the channels whose output-frame
/// frequencies reach its planes (casa-ms keeps the straddling pair at each
/// edge, so linear interpolation keeps both partners). A compiled continuum
/// transform fits each whole row, so its traversals are never restricted.
/// Rows CASA excludes from imaging and flagged rows arrive with their row
/// flag set; autocorrelations arrive unflagged, since CASA weighs them in the
/// density grids and only `GridFT` drops them.
pub(crate) struct MeasurementSetSource<'a> {
    problem: &'a CompiledProblem,
    domains: usize,
    /// The direction law of every domain when a kernel set ramps each row
    /// to its pointing; empty otherwise.
    pointing: Vec<DirectionCoordinateSpec>,
    /// The selection's aperture classes in dish order.
    dish_classes: Vec<AntennaResponseClass>,
    planes: u32,
    bounds: Option<PlaneBounds>,
    traversal: Traversal<'a>,
    projections: Vec<DomainProjection>,
    values: Vec<Complex32>,
    weights: Vec<f32>,
    flags: Vec<bool>,
}

impl<'a> MeasurementSetSource<'a> {
    /// A source over `selected` delivering rows projected on every domain.
    /// `bounds` enables restricted traversals for a basis of `planes`
    /// planes; `pointing_ramp` carries each row's pointing offset for a
    /// kernel set that ramps to it; `dish_classes` orders the antenna
    /// types the mosaic set keys on.
    pub(crate) fn new(
        problem: &'a CompiledProblem,
        selected: BoundSelectedObservation,
        planes: u32,
        bounds: Option<PlaneBounds>,
        pointing_ramp: bool,
        dish_classes: Vec<AntennaResponseClass>,
    ) -> Self {
        let domains = problem.geometry().domains();
        Self {
            problem,
            domains: domains.len(),
            pointing: if pointing_ramp {
                domains.iter().map(|domain| domain.direction()).collect()
            } else {
                Vec::new()
            },
            dish_classes,
            planes,
            bounds,
            traversal: Traversal::Idle(selected),
            projections: Vec::new(),
            values: Vec::new(),
            weights: Vec::new(),
            flags: Vec::new(),
        }
    }

    fn convert(
        &mut self,
        block: &ProjectedObservationBlock<'_>,
        out: &mut NativeBlock,
    ) -> Result<(), SourceError> {
        let rows = block.row_count();
        let first = block.numeric_row(0);
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
            let numeric = block.numeric_row(row);
            let metadata = &numeric.row.metadata;
            let coordinates = &numeric.row.coordinates;
            self.projections.clear();
            for domain in 0..self.domains {
                let projection = numeric
                    .row
                    .domain_projections()
                    .get(domain as u32)
                    .ok_or("a selected row lacks an image-domain projection")?;
                let model = projection.model();
                let pointing_offset_rad = match self.pointing.get(domain) {
                    Some(direction) => pointing_offset(
                        *direction,
                        projection.aw_pointing_pixel(),
                        coordinates.pointing_directions.antenna1,
                    )?,
                    None => [0.0; 2],
                };
                self.projections.push(DomainProjection {
                    uvw_m: model.transformed_uvw_m(),
                    phase_shift_m: model.phase_shift_m(),
                    pointing_offset_rad,
                });
            }
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
                    original_w_m: Some(coordinates.raw_uvw_m[2]),
                    time_s: coordinates.time.mjd_days() * SECONDS_PER_DAY,
                    antennas: [metadata.antenna1 as u32, metadata.antenna2 as u32],
                    antenna_types: antenna_types(metadata.antenna_responses, &self.dish_classes),
                    parallactic_angle_rad: coordinates.parallactic_angles_rad.unwrap_or([0.0; 2]),
                    field: metadata.field_id as u32,
                    spectral_window: numeric.row.spectral_window_id,
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
                &block.frequencies_hz()[row * channels..(row + 1) * channels],
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
            && self.problem.visibility_transform().is_none()
            && planes != PlaneRange::new(0, self.planes))
        .then(|| self.bounds.as_ref().map(|bounds| bounds(planes)))
        .flatten();
        let source = match window {
            Some(bounds) => selected.into_windowed_block_stream(self.problem, bounds),
            None => selected.into_block_stream(self.problem),
        };
        let block = source.create_storage();
        let channels = self
            .problem
            .observation_transaction()
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
            block,
            geometry,
        }));
        Ok(())
    }

    fn fill(&mut self, out: &mut NativeBlock) -> Result<bool, SourceError> {
        let Traversal::Streaming(mut stream) =
            std::mem::replace(&mut self.traversal, Traversal::Failed)
        else {
            return Err("no traversal is in progress".into());
        };
        let Stream {
            source,
            block,
            geometry,
        } = &mut *stream;
        let Some(filled) = source.fill_next(block)? else {
            self.traversal = Traversal::Idle(stream.source.complete()?);
            return Ok(false);
        };
        let projected = filled.project_numeric_geometry(self.problem, geometry)?;
        self.convert(&projected, out)?;
        self.traversal = Traversal::Streaming(stream);
        Ok(true)
    }
}
