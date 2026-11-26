// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read an already transformed CASA virtual GridFT model and degrid it
//! using the same measurement operator as imaging.

mod focus;
mod legacy_pbw;
mod mosaic;
mod setjy;
mod tracking;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use casa_coordinates::{Coordinate, CoordinateModel, CoordinateType};
use casa_core::measures::MeasFrame;
use casa_core::measures::direction::MDirection;
use casa_core::measures::frequency::FrequencyRef;
use casa_core::{ArrayValue, Complex32, Correlation, RecordValue, ScalarValue, Value};
use casa_images::PagedImage;
use casa_imaging_model::GridFtProjection;
use casa_imaging_operator::{
    Basis, Boxcar, ConvolutionFunctionSet, CpuBackend, GridGeometry, GridPadding, GridPrecision,
    ImageExtent, MeasurementOperator, NativeRow, PaddingFactor, PlaneRange, PolarizationRouting,
    PredictionScratch, PreparedModelGrids, RowContext, SpectralAxis, SpectralKernel,
    SpectralResampler, Spheroidal, WPlaneCount, WPlanes,
};
use casa_imaging_runtime::Reservation;
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::{MeasurementSet, convert_frequency_to_frame};
use ndarray::{ArrayD, Axis};

use super::{FocusSample, GainSolveError, SelectedSolveRow, model_error};
use crate::ft::legacy_aw;
use mosaic::MosaicLayout;

pub(super) struct FourierModel {
    record: RecordValue,
    mosaic: bool,
    legacy_mosaic: bool,
    refocus: bool,
    prepared: Mutex<FourierCache>,
}

#[derive(Default)]
struct FourierCache {
    grids: Option<Arc<FourierGrids>>,
    layouts: BTreeMap<Option<i32>, BTreeMap<Vec<i32>, PreparedFourierModel>>,
    virtual_anchor: Option<MDirection>,
}

struct FourierGrids {
    prepared: PreparedModelGrids,
    _memory: Option<Reservation>,
}

impl FourierGrids {
    fn retaining(prepared: PreparedModelGrids, mut memory: Option<Reservation>) -> Self {
        if let Some(reservation) = &mut memory {
            reservation.retain(prepared.bytes());
        }
        Self {
            prepared,
            _memory: memory,
        }
    }
}

pub(super) enum FourierMerge<'a> {
    VirtualSum,
    ScratchSum,
    ScratchReplaceComponents,
    ScratchAccumulate(&'a mut [Complex32]),
}

pub(super) struct FourierRow<'a> {
    pub(super) ms: &'a MeasurementSet,
    pub(super) row: &'a SelectedSolveRow,
    pub(super) correlations: &'a [i32],
    pub(super) frequencies: &'a [f64],
    pub(super) raw_flags: &'a ArrayD<bool>,
    pub(super) frequency_ref: FrequencyRef,
    pub(super) engine: &'a MsCalEngine,
    pub(super) field: &'a MDirection,
    pub(super) phase_time: f64,
    pub(super) frame: &'a MeasFrame,
    pub(super) uvw: [f64; 3],
    pub(super) uncached_main_reads: bool,
    pub(super) focus_rows: &'a [FocusSample],
    pub(super) legacy_mosaic_pointing: Option<&'a super::MosaicPointing>,
    pub(super) scratch_channels: Option<[i32; 4]>,
}

impl FourierModel {
    pub(super) fn new(record: RecordValue) -> Self {
        let first_term = match record.get("subftm_0").or_else(|| record.get("subftm")) {
            Some(Value::Record(term)) => term,
            _ => &record,
        };
        let mosaic = mosaic::is_mosaic(first_term);
        let legacy_mosaic = mosaic && mosaic::is_legacy_mosaic(first_term);
        let refocus = record_number(first_term, "distance").is_some_and(|distance| distance != 0.0);
        Self {
            record,
            mosaic,
            legacy_mosaic,
            refocus,
            prepared: Mutex::default(),
        }
    }

    pub(super) fn needs_refocus(&self) -> bool {
        self.refocus
    }

    pub(super) fn uses_legacy_mosaic_pointing(&self) -> bool {
        self.legacy_mosaic
    }

    pub(super) fn predict(
        &self,
        row: FourierRow<'_>,
        output: &mut ArrayD<Complex32>,
        merge: FourierMerge<'_>,
    ) -> Result<(), GainSolveError> {
        let mut prepared = self
            .prepared
            .lock()
            .expect("solve model is single consumer");
        let FourierCache {
            grids,
            layouts,
            virtual_anchor,
        } = &mut *prepared;
        let layouts = layouts
            .entry(self.mosaic.then_some(row.row.data_spw_id))
            .or_default();
        if !layouts.contains_key(row.correlations) {
            let scratch = !matches!(merge, FourierMerge::VirtualSum);
            let model = PreparedFourierModel::load(&self.record, &row, grids.clone(), scratch)?;
            *grids = Some(Arc::clone(&model.grids));
            layouts.insert(row.correlations.to_vec(), model);
        }
        layouts
            .get_mut(row.correlations)
            .expect("correlation layout was prepared")
            .predict(row, output, merge, virtual_anchor)
    }
}

struct PreparedFourierModel {
    operator: MeasurementOperator,
    resampler: SpectralResampler,
    grids: Arc<FourierGrids>,
    phase_centre: MDirection,
    tracking: Option<tracking::Tracking>,
    phase_centre_time: Option<f64>,
    timed_fields: BTreeMap<i32, MDirection>,
    project: bool,
    refocus: Option<focus::Refocus>,
    mosaic: Option<MosaicLayout>,
    scale: Option<setjy::FluxScale>,
    legacy_taylor: Option<(i32, f64)>,
    legacy_flags: bool,
    live_interpolation: bool,
    channel_mask: Option<ArrayD<i32>>,
    continuum_axis: Option<SpectralAxis>,
    convert_frequencies: bool,
    backend: CpuBackend,
    scratch: PredictionScratch,
    data: Vec<Complex32>,
    output: Vec<Complex32>,
    covered: Vec<bool>,
    weights: Vec<f32>,
    flags: Vec<bool>,
    frequencies: Vec<f64>,
    legacy_pbw_angle: Option<legacy_pbw::FeedAngle>,
    legacy_ep_offsets: Option<crate::ft::legacy_pbw::pointing::LegacyEpOffsets>,
    legacy_aw: bool,
    _legacy_kernel_memory: Option<Reservation>,
}

impl PreparedFourierModel {
    fn load(
        record: &RecordValue,
        row: &FourierRow<'_>,
        cached_grids: Option<Arc<FourierGrids>>,
        scratch: bool,
    ) -> Result<Self, GainSolveError> {
        let (terms, continuum_basis) = grid_terms(record)?;
        let images = terms
            .iter()
            .map(|term| open_grid(term, row.ms))
            .collect::<Result<Vec<_>, _>>()?;
        let image = &images[0];
        let [nx, ny, npol, channels] = *image.shape() else {
            return Err(model_error(
                "virtual GridFT requires a four-axis complex image",
            ));
        };
        let coords = image.coordinates();
        let direction = coords
            .find_coordinate(CoordinateType::Direction)
            .and_then(|index| match coords.coordinate(index) {
                CoordinateModel::Direction(direction) => Some(direction),
                _ => None,
            })
            .ok_or_else(|| model_error("virtual grid image has no direction coordinate"))?;
        let increments = direction.increment();
        let centre = direction
            .to_world(&[(nx / 2) as f64, (ny / 2) as f64])
            .map_err(model_error)?;
        let phase_centre = MDirection::from_angles(centre[0], centre[1], direction.direction_ref());
        let (image_shape, padding) = grid_input_geometry(terms[0], [nx, ny])?;
        let geometry = GridGeometry::new(
            ImageExtent {
                shape: image_shape,
                increment_rad: [increments[0], increments[1]],
                reference_pixel: image_shape.map(|length| length / 2),
            },
            padding,
        )
        .map_err(model_error)?;
        let polarization = grid_polarization(image, row.correlations)?;
        if polarization.grid_pols() != npol {
            return Err(model_error(
                "virtual grid polarization axis does not match its planes",
            ));
        }
        let axis = image_spectral_axis(image, channels)?;
        let (basis, resampler) = spectral_layout(record, &axis, continuum_basis, false)?;
        let tracking = tracking::Tracking::load(terms[0])?;
        let machine = record_text(terms[0], "name")?;
        let legacy_pbw = matches!(machine, "nPBWProjectFT" | "PBMosaicFT");
        let legacy_aw = machine == legacy_aw::MACHINE;
        let legacy_ep_offsets = if legacy_pbw {
            crate::ft::legacy_pbw::pointing::LegacyEpOffsets::from_record(
                terms[0],
                row.ms.antenna().map_err(model_error)?.row_count(),
            )
            .map_err(model_error)?
        } else {
            None
        };
        let (kernels, mosaic, legacy_kernel_memory): (Box<dyn ConvolutionFunctionSet>, _, _) =
            if legacy_pbw {
                let (kernels, memory) = legacy_pbw::load(
                    terms[0],
                    row,
                    &geometry,
                    &polarization,
                    legacy_ep_offsets.is_some(),
                )?;
                (Box::new(kernels), None, Some(memory))
            } else if legacy_aw {
                let path = complex_model_image_path(terms[0], row.ms)?.join(legacy_aw::CACHE);
                if !path.is_dir() {
                    return Err(model_error(format!(
                        "legacy AW recipe has no cache at {}",
                        path.display()
                    )));
                }
                let conjugate = matches!(
                    terms[0].get("casars_legacy_aw_conjbeams"),
                    Some(Value::Scalar(ScalarValue::Bool(true)))
                );
                let (kernels, memory) = legacy_aw::open_catalog(
                    &path,
                    &geometry,
                    &polarization,
                    axis.first_hz(),
                    conjugate,
                    legacy_aw::record_rotation(terms[0]).map_err(model_error)?,
                    true,
                )
                .map_err(model_error)?;
                let map = legacy_aw::record_frequency_map(terms[0]).map_err(model_error)?;
                (
                    Box::new(kernels.with_legacy_frequency_map(map)),
                    None,
                    Some(memory),
                )
            } else if mosaic::is_mosaic(terms[0]) {
                let (kernels, layout) = MosaicLayout::load(
                    terms[0],
                    row,
                    (direction, coords.obs_info().telescope.contains("VLA")),
                    (&geometry, &polarization),
                    axis.first_hz(),
                    matches!(
                        record.get("freqframevalid"),
                        Some(Value::Scalar(ScalarValue::Bool(true)))
                    ),
                    scratch,
                )?;
                (kernels, Some(layout), None)
            } else {
                (grid_kernel(terms[0], &geometry, &polarization)?, None, None)
            };
        let resampler = if legacy_pbw
            || legacy_aw
            || mosaic
                .as_ref()
                .is_some_and(|layout| layout.autocorrelations)
        {
            resampler.predicting_autocorrelations()
        } else {
            resampler
        };
        let operator =
            MeasurementOperator::new(geometry, basis, polarization, kernels, GridPrecision::F32);
        let grids = match cached_grids {
            Some(grids) => grids,
            None => {
                let memory = if legacy_pbw || legacy_aw {
                    let plane = (nx as u64)
                        .saturating_mul(ny as u64)
                        .saturating_mul(size_of::<Complex32>() as u64);
                    let count = (npol as u64)
                        .saturating_mul(images.len() as u64)
                        .saturating_mul(u64::from(operator.basis().planes()));
                    let peak = plane.saturating_mul(count.saturating_add(4));
                    Some(legacy_pbw::reserve("legacy Fourier image read", peak)?)
                } else {
                    None
                };
                let prepared = load_grids(&operator, &images)?;
                Arc::new(FourierGrids::retaining(prepared, memory))
            }
        };
        let scale = if record_text(terms[0], "name")? == "SetJyGridFT" {
            setjy::FluxScale::from_record(terms[0])?
        } else {
            None
        };
        let project = uses_projected_tangent(terms[0], row.ms)?;
        let legacy_flags = legacy_pbw || legacy_aw || terms[0].get("doconversion").is_some();
        let channel_mask = match terms[0].get("spwchanselflag") {
            Some(Value::Array(ArrayValue::Int32(mask))) if !mask.is_empty() => Some(mask.clone()),
            _ => None,
        };
        Ok(Self {
            _legacy_kernel_memory: legacy_kernel_memory,
            operator,
            resampler,
            grids,
            phase_centre,
            tracking,
            phase_centre_time: record_number(terms[0], "phasecentertime")
                .filter(|epoch| epoch.is_finite() && *epoch > 0.),
            timed_fields: BTreeMap::new(),
            project,
            refocus: focus::Refocus::from_distance(
                record_number(terms[0], "distance").unwrap_or(0.0),
            )?,
            mosaic,
            scale,
            legacy_taylor: legacy_taylor_term(record)?,
            legacy_flags,
            live_interpolation: record.get("casars_live_image_frequency").is_some(),
            channel_mask,
            continuum_axis: (channels == 1).then_some(axis),
            convert_frequencies: matches!(
                record.get("freqframevalid"),
                Some(Value::Scalar(ScalarValue::Bool(true)))
            ),
            backend: CpuBackend::default(),
            scratch: PredictionScratch::default(),
            data: Vec::new(),
            output: Vec::new(),
            covered: Vec::new(),
            weights: Vec::new(),
            flags: Vec::new(),
            frequencies: Vec::new(),
            legacy_pbw_angle: legacy_pbw.then(legacy_pbw::FeedAngle::default),
            legacy_ep_offsets,
            legacy_aw,
        })
    }

    fn predict(
        &mut self,
        row: FourierRow<'_>,
        output: &mut ArrayD<Complex32>,
        merge: FourierMerge<'_>,
        virtual_anchor: &mut Option<MDirection>,
    ) -> Result<(), GainSolveError> {
        let legacy_tracking = self
            .tracking
            .as_ref()
            .is_some_and(tracking::Tracking::is_legacy);
        let reinitialize = matches!(merge, FourierMerge::VirtualSum).then_some(virtual_anchor);
        let observed = self
            .tracking
            .as_mut()
            .map(|tracking| tracking.observe(&row, reinitialize))
            .transpose()?;
        let projection_frame = observed
            .as_ref()
            .filter(|_| legacy_tracking)
            .map_or(row.frame, |(frame, _)| frame);
        let shift = observed.as_ref().map_or([0.; 2], |(_, shift)| *shift);
        let tracked_centre = self
            .tracking
            .as_ref()
            .map(|tracking| tracking.centre(shift));
        let phase_centre = tracked_centre.as_ref().unwrap_or(&self.phase_centre);
        // VisBuffer::phaseCenter(time) uses the current buffer's field
        // and evaluates that direction at the supplied epoch.
        let field: &MDirection = if let Some(epoch) = self.phase_centre_time {
            match self.timed_fields.entry(row.row.field_id) {
                std::collections::btree_map::Entry::Occupied(field) => field.into_mut(),
                std::collections::btree_map::Entry::Vacant(field) => {
                    let field_id = usize::try_from(*field.key()).map_err(model_error)?;
                    field.insert(
                        super::field_direction(row.ms, row.engine, field_id, epoch)
                            .map_err(model_error)?,
                    )
                }
            }
        } else {
            row.field
        };
        let (projection, pointing_offset_rad, antenna_types) = match &mut self.mosaic {
            Some(layout) if legacy_tracking => (
                GridFtProjection::legacy_tracked_mosaic_with_frame(
                    field,
                    &self.phase_centre,
                    shift.map(|angle| -angle),
                    projection_frame,
                ),
                layout.pointing_offset(&row, [0.; 2])?,
                layout.antenna_types(&row)?,
            ),
            Some(layout) => (
                GridFtProjection::tracked_mosaic_with_frame(
                    field,
                    &self.phase_centre,
                    shift,
                    projection_frame,
                ),
                layout.pointing_offset(&row, shift)?,
                layout.antenna_types(&row)?,
            ),
            None if self.legacy_aw => (
                GridFtProjection::mosaic_between_directions_with_frame(
                    field,
                    phase_centre,
                    projection_frame,
                ),
                Some([0.0; 2]),
                [0; 2],
            ),
            None if self.project => (
                GridFtProjection::faceted_between_directions_with_frame(
                    field,
                    phase_centre,
                    projection_frame,
                ),
                Some([0.0; 2]),
                [0; 2],
            ),
            None => (
                GridFtProjection::between_directions_with_frame(
                    field,
                    phase_centre,
                    projection_frame,
                ),
                Some([0.0; 2]),
                [0; 2],
            ),
        };
        let projection = projection.map_err(model_error)?;
        let (uvw_m, mut phase_shift_m) = match &mut self.refocus {
            Some(focus) => focus.apply(&row, &projection)?,
            None => projection.apply(row.uvw),
        };
        if self.legacy_pbw_angle.is_some() {
            // nPBWProjectFT reverses dphase after rotating and refocusing.
            phase_shift_m = -phase_shift_m;
        }
        if row.raw_flags.shape() != [row.correlations.len(), row.frequencies.len()] {
            return Err(model_error(
                "virtual Fourier prediction flags do not match the visibility layout",
            ));
        }
        let cells = row.frequencies.len() * row.correlations.len();
        let is_scratch = self.legacy_flags && !matches!(merge, FourierMerge::VirtualSum);
        let Some(pointing_offset_rad) = pointing_offset_rad else {
            // MosaicFT::get returns with the visibility buffer untouched.
            self.output.resize(cells, Complex32::default());
            self.output.fill(Complex32::default());
            self.covered.resize(cells, false);
            self.covered.fill(false);
            self.accumulate(row, output, 1.0, is_scratch.then_some(merge));
            return Ok(());
        };
        self.data.resize(cells, Complex32::new(0.0, 0.0));
        self.output.resize(cells, Complex32::new(0.0, 0.0));
        self.weights.resize(cells, 1.0);
        self.flags.resize(cells, false);
        let lsrk_ratio = if self.convert_frequencies || self.scale.is_some() {
            lsrk_frequency_ratio(&row)?
        } else {
            1.0
        };
        let ratio = if self.convert_frequencies {
            lsrk_ratio
        } else {
            1.0
        };
        self.frequencies.clear();
        self.frequencies
            .extend(row.frequencies.iter().map(|frequency| frequency * ratio));
        let interpolation_channels = row
            .scratch_channels
            .map_or(row.frequencies.len(), |[_, _, count, _]| count as usize);
        let use_spectral_selection_mask = !self.live_interpolation || interpolation_channels <= 1;
        for (channel, ((flags, raw), &frequency)) in self
            .flags
            .chunks_exact_mut(row.correlations.len())
            .zip(row.raw_flags.axis_iter(Axis(1)))
            .zip(&self.frequencies)
            .enumerate()
        {
            let outside = self
                .continuum_axis
                .as_ref()
                .is_some_and(|axis| axis.nearest_channel(frequency).is_none());
            if self.legacy_flags {
                // Legacy GridFT restores VisBuffer's per-correlation flags.
                // FTMachine::fromRecord discards the original channel mask.
                let masked = scratch_channel_masked(
                    self.channel_mask.as_ref(),
                    row.scratch_channels,
                    channel,
                    use_spectral_selection_mask,
                );
                for (value, &raw_flag) in flags.iter_mut().zip(raw) {
                    *value = outside || masked || raw_flag;
                }
            } else {
                flags.fill(outside || raw.iter().any(|flag| *flag));
            }
        }
        // A one-plane legacy FT matches channels in the converted frame,
        // then uses VisBuffer::frequency directly for its Fourier phase.
        let wavelength_scale = if self.legacy_flags && self.continuum_axis.is_some() {
            ratio.recip()
        } else {
            1.0
        };
        let (parallactic_angle, pointing_offset_rad) =
            self.legacy_pbw_placement(&row, pointing_offset_rad)?;
        let native = NativeRow {
            uvw_m: uvw_m.map(|coordinate| coordinate * wavelength_scale),
            phase_shift_m: phase_shift_m * wavelength_scale,
            pointing_offset_rad,
            frequencies_hz: &self.frequencies,
            values: &self.data,
            weights: &self.weights,
            flags: &self.flags,
            row_flag: row.row.flag_row,
            context: RowContext {
                native_frequency_ratio: 1.0 / ratio,
                original_uvw_m: self.legacy_aw.then_some(row.uvw),
                time_s: row.row.time_seconds,
                antennas: [row.row.antenna1 as u32, row.row.antenna2 as u32],
                antenna_types,
                parallactic_angle_rad: [parallactic_angle; 2],
                field: row.row.field_id as u32,
                spectral_window: row.row.data_spw_id as u32,
            },
        };
        let coverage = is_scratch.then(|| {
            self.covered.resize(cells, false);
            &mut self.covered[..]
        });
        self.resampler
            .set_legacy_prediction_selection(row.scratch_channels.map(
                |[_, start, count, stride]| [start as usize, count as usize, stride as usize],
            ));
        self.resampler
            .predict_row_with_coverage(
                &self.operator,
                &mut self.backend,
                &self.grids.prepared,
                &native,
                &mut self.scratch,
                (&mut self.output, coverage),
            )
            .map_err(model_error)?;
        self.accumulate(row, output, lsrk_ratio, is_scratch.then_some(merge));
        Ok(())
    }

    fn legacy_pbw_placement(
        &mut self,
        row: &FourierRow<'_>,
        centre: [f64; 2],
    ) -> Result<(f64, [f64; 2]), GainSolveError> {
        let angle = self
            .legacy_pbw_angle
            .as_mut()
            .map_or(Ok(0.), |angles| angles.for_row(row))?;
        let centre = self
            .legacy_ep_offsets
            .as_ref()
            .map_or(Ok(centre), |offsets| {
                offsets.baseline_offset([row.row.antenna1, row.row.antenna2], row.row.time_seconds)
            })
            .map_err(model_error)?;
        Ok((angle, centre))
    }

    fn accumulate(
        &self,
        row: FourierRow<'_>,
        output: &mut ArrayD<Complex32>,
        lsrk_ratio: f64,
        mut scratch: Option<FourierMerge<'_>>,
    ) {
        let correlations = row.correlations.len();
        let destination = output
            .as_slice_memory_order_mut()
            .expect("contiguous model output");
        if scratch.is_none() && self.scale.is_none() && self.legacy_taylor.is_none() {
            for (value, &model) in destination.iter_mut().zip(&self.output) {
                *value += model;
            }
            return;
        }
        for (channel, &frequency) in row.frequencies.iter().enumerate() {
            let factor = self
                .scale
                .as_ref()
                .map_or(1.0, |scale| scale.at(frequency * lsrk_ratio));
            let taylor = self.legacy_taylor.map_or(1.0, |(term, reference)| {
                // Legacy MultiTermFT rounds the raw frequency to Float.
                let offset = ((f64::from(frequency as f32) - reference) / reference) as f32;
                offset.powi(term)
            });
            for correlation in 0..correlations {
                let index = channel * correlations + correlation;
                let assigned = scratch.is_none() || self.covered[index];
                if let Some(FourierMerge::ScratchAccumulate(buffer)) = &mut scratch {
                    if assigned {
                        buffer[index] = self.output[index];
                    }
                    buffer[index] *= factor * taylor;
                    destination[index] += buffer[index];
                } else if matches!(scratch, Some(FourierMerge::ScratchReplaceComponents)) {
                    if assigned {
                        destination[index] = self.output[index] * (factor * taylor);
                    }
                } else {
                    // An additive SkyEquation pass adds the original model
                    // to GridFT's output, including samples it did not visit.
                    let sample = if assigned {
                        self.output[index]
                    } else {
                        destination[index]
                    };
                    destination[index] += sample * (factor * taylor);
                }
            }
        }
    }
}

pub(super) fn scratch_channel_masked(
    mask: Option<&ArrayD<i32>>,
    selection: Option<[i32; 4]>,
    channel: usize,
    use_spectral_selection_mask: bool,
) -> bool {
    mask.zip(selection)
        .is_some_and(|(mask, [spw, start, count, stride])| {
            let Some(relative) = channel.checked_sub(start as usize) else {
                return true;
            };
            let compact = relative / stride as usize;
            compact >= count as usize
                || !relative.is_multiple_of(stride as usize)
                || (use_spectral_selection_mask
                    && mask
                        .get([0, spw as usize, compact].as_slice())
                        .is_some_and(|&value| value != 1))
        })
}

pub(super) fn lsrk_frequency_ratio(row: &FourierRow<'_>) -> Result<f64, GainSolveError> {
    if row.frequency_ref == FrequencyRef::LSRK {
        return Ok(1.0);
    }
    let reference = row.frequencies[0];
    let converted = convert_frequency_to_frame(
        row.frequency_ref,
        FrequencyRef::LSRK,
        reference,
        row.row.time_seconds,
        row.row.field_id as usize,
        row.engine,
    )
    .map_err(model_error)?;
    Ok(converted / reference)
}

fn uses_projected_tangent(
    record: &RecordValue,
    ms: &MeasurementSet,
) -> Result<bool, GainSolveError> {
    if !matches!(
        record.get("tangentspecified"),
        Some(Value::Scalar(ScalarValue::Bool(true)))
    ) {
        return Ok(false);
    }
    let telescope = ms
        .observation()
        .map_err(model_error)?
        .string(0, "TELESCOPE_NAME")
        .map_err(model_error)?;
    // FTMachine::rotateUVW uses unprojected rotation for these telescopes.
    Ok(!telescope.contains("ATCA") && !telescope.contains("WSRT"))
}

pub(super) fn spectral_layout(
    record: &RecordValue,
    axis: &SpectralAxis,
    continuum_basis: Basis,
    retain_saved_interpolation: bool,
) -> Result<(Basis, SpectralResampler), GainSolveError> {
    let channels = axis.channels();
    if channels == 1 {
        let resampler = SpectralResampler::direct(continuum_basis).map_err(model_error)?;
        return Ok((continuum_basis, resampler));
    }
    if !matches!(continuum_basis, Basis::Constant) || channels < 2 {
        return Err(model_error(
            "invalid channel axis for virtual Fourier model",
        ));
    }
    // FTMachine::fromRecord restores nearest neighbour for Fourier grids.
    // Native SDGrid recipes retain the interpolation the writer used.
    let live_frequencies = match record.get("casars_live_image_frequency") {
        Some(Value::Array(ArrayValue::Float64(frequencies))) => Some(frequencies),
        _ => None,
    };
    let reopen_with_nearest = !retain_saved_interpolation
        && matches!(
            record.get("tovis"),
            Some(Value::Scalar(ScalarValue::Bool(true)))
        );
    let interpolation = record.get("freqinterpmethod");
    let spline = live_frequencies.is_some()
        && !reopen_with_nearest
        && matches!(interpolation, Some(Value::Scalar(ScalarValue::Int32(3))));
    let kernel = match (reopen_with_nearest, interpolation) {
        (true, _) | (_, Some(Value::Scalar(ScalarValue::Int32(0)))) => SpectralKernel::Nearest,
        (_, Some(Value::Scalar(ScalarValue::Int32(1)))) => SpectralKernel::Linear,
        (_, Some(Value::Scalar(ScalarValue::Int32(2)))) => SpectralKernel::Cubic,
        (_, Some(Value::Scalar(ScalarValue::Int32(3)))) if spline => SpectralKernel::Cubic,
        _ => {
            return Err(model_error(
                "unsupported virtual grid spectral interpolation",
            ));
        }
    };
    let basis = Basis::ChannelLocal { planes: channels };
    let resampler = if let Some(frequencies) = live_frequencies {
        let frequencies = frequencies.iter().copied().collect::<Vec<_>>();
        if frequencies.len() != channels as usize {
            return Err(model_error(
                "legacy imager interpolation axis length does not match its model",
            ));
        }
        let increment = frequencies[1] - frequencies[0];
        let grid = SpectralAxis::tabular(&frequencies, increment).map_err(model_error)?;
        if spline {
            SpectralResampler::legacy_spline(grid, axis.clone())
        } else {
            SpectralResampler::channel_local(grid, kernel).with_legacy_prediction_axis(axis.clone())
        }
    } else {
        SpectralResampler::channel_local(axis.clone(), kernel)
    };
    Ok((basis, resampler))
}

fn image_spectral_axis(
    image: &PagedImage<Complex32>,
    channels: usize,
) -> Result<SpectralAxis, GainSolveError> {
    let coords = image.coordinates();
    let spectral = coords
        .find_coordinate(CoordinateType::Spectral)
        .and_then(|index| match coords.coordinate(index) {
            CoordinateModel::Spectral(spectral) => Some(spectral),
            _ => None,
        })
        .ok_or_else(|| model_error("virtual grid image has no spectral coordinate"))?;
    let spectral = crate::ft::spectral_with_runtime(spectral).map_err(model_error)?;
    let frequency = |channel| {
        spectral
            .world_frequency_hz(channel as f64)
            .map_err(model_error)
    };
    let start = frequency(0)?;
    let width = frequency(1)? - start;
    if channels == 1 {
        return SpectralAxis::new(start, width, 1).map_err(model_error);
    }
    let frequencies = (0..channels)
        .map(frequency)
        .collect::<Result<Vec<_>, _>>()?;
    SpectralAxis::tabular(&frequencies, width).map_err(model_error)
}

fn grid_terms(record: &RecordValue) -> Result<(Vec<&RecordValue>, Basis), GainSolveError> {
    match record_text(record, "name")? {
        "GridFT"
        | "WProjectFT"
        | "MosaicFT"
        | "MosaicFTNew"
        | "SetJyGridFT"
        | "nPBWProjectFT"
        | "PBMosaicFT"
        | legacy_aw::MACHINE => Ok((vec![record], Basis::Constant)),
        "MultiTermFT" => match record.get("subftm") {
            Some(Value::Record(term)) => Ok((vec![term], Basis::Constant)),
            _ => Err(model_error("virtual MultiTermFT has no subftm record")),
        },
        "MultiTermFTNew" => {
            let Some(Value::Scalar(ScalarValue::UInt32(terms))) = record.get("nterms") else {
                return Err(model_error(
                    "virtual MultiTermFTNew has no Taylor term count",
                ));
            };
            let Some(Value::Scalar(ScalarValue::Float64(reference_hz))) = record.get("reffreq")
            else {
                return Err(model_error(
                    "virtual MultiTermFTNew has no reference frequency",
                ));
            };
            if *terms == 0 || *terms as usize > record.fields().len() || *reference_hz <= 0.0 {
                return Err(model_error("invalid virtual MultiTermFTNew Taylor basis"));
            }
            let subrecords = (0..*terms)
                .map(|index| {
                    let name = format!("subftm_{index}");
                    match record.get(&name) {
                        Some(Value::Record(term)) => Ok(term),
                        _ => Err(model_error(format!("virtual model has no {name} record"))),
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok((
                subrecords,
                Basis::Taylor {
                    terms: *terms,
                    reference_hz: *reference_hz,
                },
            ))
        }
        name => Err(model_error(format!(
            "unsupported virtual Fourier machine {name}"
        ))),
    }
}

/// Legacy ft stores each Taylor grid in a separate model entry. Its machine
/// contains one term and scales that term after degridding.
fn legacy_taylor_term(record: &RecordValue) -> Result<Option<(i32, f64)>, GainSolveError> {
    if record_text(record, "name")? != "MultiTermFT" {
        return Ok(None);
    }
    let Some(Value::Scalar(ScalarValue::Int32(term))) = record.get("thisterm") else {
        return Err(model_error("virtual MultiTermFT has no Taylor term index"));
    };
    let Some(reference) = record_number(record, "reffreq") else {
        return Err(model_error(
            "virtual MultiTermFT has no reference frequency",
        ));
    };
    if *term < 0 || !reference.is_finite() || reference <= 0.0 {
        return Err(model_error("invalid virtual MultiTermFT Taylor term"));
    }
    Ok((*term > 0).then_some((*term, reference)))
}

fn open_grid(
    record: &RecordValue,
    ms: &MeasurementSet,
) -> Result<PagedImage<Complex32>, GainSolveError> {
    if !matches!(
        record_text(record, "name")?,
        "GridFT"
            | "WProjectFT"
            | "MosaicFT"
            | "MosaicFTNew"
            | "SetJyGridFT"
            | "nPBWProjectFT"
            | "PBMosaicFT"
            | legacy_aw::MACHINE
    ) {
        return Err(model_error(
            "unsupported Fourier image type in virtual model",
        ));
    }
    open_complex_model_image(record, ms)
}

pub(super) fn open_complex_model_image(
    record: &RecordValue,
    ms: &MeasurementSet,
) -> Result<PagedImage<Complex32>, GainSolveError> {
    PagedImage::open(complex_model_image_path(record, ms)?).map_err(model_error)
}

pub(super) fn complex_model_image_path(
    record: &RecordValue,
    ms: &MeasurementSet,
) -> Result<PathBuf, GainSolveError> {
    let recorded = Path::new(record_text(record, "diskimage")?);
    let path = match (recorded.exists(), ms.path(), recorded.file_name()) {
        (false, Some(ms), Some(name)) => [ms.join("SOURCE").join(name), ms.join(name)]
            .into_iter()
            .find(|path| path.exists())
            .unwrap_or_else(|| recorded.to_path_buf()),
        _ => PathBuf::from(recorded),
    };
    Ok(path)
}

fn record_text<'a>(record: &'a RecordValue, field: &str) -> Result<&'a str, GainSolveError> {
    match record.get(field) {
        Some(Value::Scalar(ScalarValue::String(value))) => Ok(value),
        _ => Err(model_error(format!("virtual model has no {field} string"))),
    }
}

fn grid_input_geometry(
    record: &RecordValue,
    stored: [usize; 2],
) -> Result<([usize; 2], GridPadding), GainSolveError> {
    let Some(Value::Array(ArrayValue::Float64(geometry))) =
        record.get("casars_prediction_geometry")
    else {
        return Ok((stored, grid_padding(record)?));
    };
    let Some([nx, ny, padding]) = geometry.as_slice() else {
        return Err(model_error("invalid imager prediction geometry"));
    };
    Ok((
        [*nx as usize, *ny as usize],
        GridPadding::CasaComposite(PaddingFactor::new(*padding as f32).map_err(model_error)?),
    ))
}

fn grid_padding(record: &RecordValue) -> Result<GridPadding, GainSolveError> {
    if record_text(record, "name")? != "WProjectFT" {
        return Ok(GridPadding::None);
    }
    let Some(padding) = record_number(record, "padding") else {
        return Err(model_error("virtual WProjectFT has no padding factor"));
    };
    Ok(GridPadding::CasaComposite(
        PaddingFactor::new(padding as f32).map_err(model_error)?,
    ))
}

fn grid_kernel(
    record: &RecordValue,
    geometry: &GridGeometry,
    polarization: &PolarizationRouting,
) -> Result<Box<dyn ConvolutionFunctionSet>, GainSolveError> {
    if record_text(record, "name")? == "WProjectFT" {
        let Some(Value::Scalar(ScalarValue::Int32(planes))) = record.get("nwplanes") else {
            return Err(model_error("virtual WProjectFT has no plane count"));
        };
        let planes = u32::try_from(*planes).map_err(model_error)?;
        let max_w = record_number(record, "maxw")
            .filter(|max_w| max_w.is_finite())
            .ok_or_else(|| model_error("virtual WProjectFT has no valid maximum W"))?;
        let saved_scale = record_number(record, "savedwscale")
            .filter(|scale| scale.is_finite() && *scale > 0.0 && planes > 1);
        let count = if let Some(scale) = saved_scale {
            WPlaneCount::Restored {
                planes,
                max_w: (f64::from(planes - 1).powi(2) / scale) / 1.05,
            }
        } else if max_w > 0.0 {
            WPlaneCount::Restored { planes, max_w }
        } else {
            WPlaneCount::Fixed(planes)
        };
        return Ok(Box::new(
            WPlanes::new(geometry, polarization, count).map_err(model_error)?,
        ));
    }
    match record_text(record, "convtype")? {
        "SF" => Ok(Box::new(Spheroidal::new(geometry, polarization))),
        "BOX" => Ok(Box::new(Boxcar::new(geometry, polarization))),
        name => Err(model_error(format!(
            "unsupported virtual grid convolution {name}"
        ))),
    }
}

fn record_number(record: &RecordValue, field: &str) -> Option<f64> {
    match record.get(field) {
        Some(Value::Scalar(ScalarValue::Float64(value))) => Some(*value),
        Some(Value::Scalar(ScalarValue::Float32(value))) => Some(f64::from(*value)),
        _ => None,
    }
}

fn grid_polarization(
    image: &PagedImage<Complex32>,
    correlations: &[i32],
) -> Result<PolarizationRouting, GainSolveError> {
    let coords = image.coordinates();
    let stokes = coords
        .find_coordinate(CoordinateType::Stokes)
        .and_then(|index| match coords.coordinate(index) {
            CoordinateModel::Stokes(stokes) => Some(stokes.stokes()),
            _ => None,
        })
        .ok_or_else(|| model_error("virtual grid image has no Stokes coordinate"))?;
    let coordinates = stokes
        .iter()
        .copied()
        .map(|stokes_type| {
            crate::ft::image::polarization_coordinate(stokes_type)
                .ok_or_else(|| model_error("unsupported polarization plane in virtual grid"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let correlations = correlations
        .iter()
        .map(|&code| {
            Correlation::from_casacore_code(code)
                .ok_or_else(|| model_error(format!("invalid virtual grid correlation {code}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    PolarizationRouting::from_saved_planes(&correlations, &coordinates).map_err(model_error)
}

fn load_grids(
    operator: &MeasurementOperator,
    images: &[PagedImage<Complex32>],
) -> Result<PreparedModelGrids, GainSolveError> {
    let channels = operator.basis().planes();
    let [nx, ny] = operator.geometry().grid_shape();
    let shape = [
        nx,
        ny,
        operator.polarization().grid_pols(),
        channels as usize,
    ];
    if images.iter().any(|image| image.shape() != shape) {
        return Err(model_error(
            "virtual model image axes do not match the grid",
        ));
    }
    let mut grids = operator
        .empty_model_grids(PlaneRange::new(0, channels))
        .map_err(model_error)?;
    for mut plane in grids.planes_mut() {
        for (term, image) in images.iter().enumerate() {
            for polarization in 0..shape[2] {
                let values = image
                    .get_slice(
                        &[0, 0, polarization, plane.plane() as usize],
                        &[nx, ny, 1, 1],
                    )
                    .map_err(model_error)?;
                let cells = values
                    .as_slice_memory_order()
                    .filter(|_| values.strides()[0] == 1)
                    .ok_or_else(|| model_error("virtual grid cells are not contiguous along x"))?;
                plane
                    .load_f32_block(polarization, term, cells)
                    .map_err(model_error)?;
            }
        }
    }
    Ok(grids)
}
