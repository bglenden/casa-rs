// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read an already transformed CASA virtual GridFT model and degrid it
//! using the same measurement operator as imaging.

mod focus;
mod mosaic;
mod setjy;
mod tracking;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use casa_coordinates::{Coordinate, CoordinateModel, CoordinateType};
use casa_images::PagedImage;
use casa_imaging_model::GridFtProjection;
use casa_imaging_operator::{
    Basis, Boxcar, ConvolutionFunctionSet, CpuBackend, GridGeometry, GridPadding, GridPrecision,
    ImageExtent, MeasurementOperator, NativeRow, PaddingFactor, PlaneRange, PolarizationRouting,
    PredictionScratch, PreparedModelGrids, RowContext, SpectralAxis, SpectralKernel,
    SpectralResampler, Spheroidal, WPlaneCount, WPlanes,
};
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::{MeasurementSet, convert_frequency_to_frame};
use casa_types::measures::MeasFrame;
use casa_types::measures::direction::MDirection;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::{Complex32, Correlation, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, Axis};

use super::{FocusSample, GainSolveError, SelectedSolveRow, model_error};
use mosaic::MosaicLayout;

pub(super) struct FourierModel {
    record: RecordValue,
    mosaic: bool,
    refocus: bool,
    prepared: Mutex<FourierCache>,
}

#[derive(Default)]
struct FourierCache {
    grids: Option<Arc<PreparedModelGrids>>,
    layouts: BTreeMap<Option<i32>, BTreeMap<Vec<i32>, PreparedFourierModel>>,
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
    pub(super) focus_rows: &'a [FocusSample],
}

impl FourierModel {
    pub(super) fn new(record: RecordValue) -> Self {
        let first_term = match record.get("subftm_0").or_else(|| record.get("subftm")) {
            Some(Value::Record(term)) => term,
            _ => &record,
        };
        let mosaic = mosaic::is_mosaic(first_term);
        let refocus = record_number(first_term, "distance").is_some_and(|distance| distance != 0.0);
        Self {
            record,
            mosaic,
            refocus,
            prepared: Mutex::default(),
        }
    }

    pub(super) fn needs_refocus(&self) -> bool {
        self.refocus
    }

    pub(super) fn predict(
        &self,
        row: FourierRow<'_>,
        output: &mut ArrayD<Complex32>,
    ) -> Result<(), GainSolveError> {
        let mut prepared = self
            .prepared
            .lock()
            .expect("solve model is single consumer");
        let FourierCache { grids, layouts } = &mut *prepared;
        let layouts = layouts
            .entry(self.mosaic.then_some(row.row.data_spw_id))
            .or_default();
        if !layouts.contains_key(row.correlations) {
            let model = PreparedFourierModel::load(&self.record, &row, grids.clone())?;
            *grids = Some(Arc::clone(&model.grids));
            layouts.insert(row.correlations.to_vec(), model);
        }
        layouts
            .get_mut(row.correlations)
            .expect("correlation layout was prepared")
            .predict(row, output)
    }
}

struct PreparedFourierModel {
    operator: MeasurementOperator,
    resampler: SpectralResampler,
    grids: Arc<PreparedModelGrids>,
    phase_centre: MDirection,
    tracking: Option<tracking::Tracking>,
    fixed_field: Option<MDirection>,
    project: bool,
    refocus: Option<focus::Refocus>,
    mosaic: Option<MosaicLayout>,
    scale: Option<setjy::FluxScale>,
    legacy_taylor: Option<(i32, f64)>,
    legacy_flags: bool,
    continuum_axis: Option<SpectralAxis>,
    convert_frequencies: bool,
    backend: CpuBackend,
    scratch: PredictionScratch,
    data: Vec<Complex32>,
    output: Vec<Complex32>,
    weights: Vec<f32>,
    flags: Vec<bool>,
    frequencies: Vec<f64>,
}

impl PreparedFourierModel {
    fn load(
        record: &RecordValue,
        row: &FourierRow<'_>,
        cached_grids: Option<Arc<PreparedModelGrids>>,
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
        let geometry = GridGeometry::new(
            ImageExtent {
                shape: [nx, ny],
                increment_rad: [increments[0], increments[1]],
                reference_pixel: [nx / 2, ny / 2],
            },
            grid_padding(terms[0])?,
        )
        .map_err(model_error)?;
        let polarization = grid_polarization(image, row.correlations)?;
        if polarization.grid_pols() != npol {
            return Err(model_error(
                "virtual grid polarization axis does not match its planes",
            ));
        }
        let axis = image_spectral_axis(image, channels)?;
        let (basis, resampler) = spectral_layout(record, &axis, continuum_basis)?;
        let tracking = tracking::Tracking::load(terms[0])?;
        let (kernels, mosaic) = if mosaic::is_mosaic(terms[0]) {
            let (kernels, layout) = MosaicLayout::load(
                terms[0],
                row,
                direction,
                &geometry,
                &polarization,
                axis.first_hz(),
                matches!(
                    record.get("freqframevalid"),
                    Some(Value::Scalar(ScalarValue::Bool(true)))
                ),
            )?;
            (kernels, Some(layout))
        } else {
            (grid_kernel(terms[0], &geometry, &polarization)?, None)
        };
        let operator =
            MeasurementOperator::new(geometry, basis, polarization, kernels, GridPrecision::F32);
        let grids = match cached_grids {
            Some(grids) => grids,
            None => Arc::new(load_grids(&operator, &images)?),
        };
        let scale = if record_text(terms[0], "name")? == "SetJyGridFT" {
            setjy::FluxScale::from_record(terms[0])?
        } else {
            None
        };
        let project = uses_projected_tangent(terms[0], row.ms)?;
        let legacy_flags = terms[0].get("doconversion").is_some();
        Ok(Self {
            operator,
            resampler,
            grids,
            phase_centre,
            tracking,
            fixed_field: tracking::fixed_field(terms[0], row)?,
            project,
            refocus: focus::Refocus::from_distance(
                record_number(terms[0], "distance").unwrap_or(0.0),
            )?,
            mosaic,
            scale,
            legacy_taylor: legacy_taylor_term(record)?,
            legacy_flags,
            continuum_axis: (channels == 1).then_some(axis),
            convert_frequencies: matches!(
                record.get("freqframevalid"),
                Some(Value::Scalar(ScalarValue::Bool(true)))
            ),
            backend: CpuBackend::default(),
            scratch: PredictionScratch::default(),
            data: Vec::new(),
            output: Vec::new(),
            weights: Vec::new(),
            flags: Vec::new(),
            frequencies: Vec::new(),
        })
    }

    fn predict(
        &mut self,
        row: FourierRow<'_>,
        output: &mut ArrayD<Complex32>,
    ) -> Result<(), GainSolveError> {
        let shift = self
            .tracking
            .as_mut()
            .map(|tracking| tracking.shift(&row))
            .transpose()?
            .unwrap_or([0.; 2]);
        let tracked_centre = self
            .tracking
            .as_ref()
            .map(|tracking| tracking.centre(shift));
        let phase_centre = tracked_centre.as_ref().unwrap_or(&self.phase_centre);
        let field = self.fixed_field.as_ref().unwrap_or(row.field);
        let (projection, pointing_offset_rad, antenna_types) = match &self.mosaic {
            Some(layout) => (
                GridFtProjection::tracked_mosaic_with_frame(
                    field,
                    &self.phase_centre,
                    shift,
                    row.frame,
                ),
                layout.pointing_offset(&row, shift)?,
                layout.antenna_types(&row)?,
            ),
            None if self.project => (
                GridFtProjection::faceted_between_directions_with_frame(
                    field,
                    phase_centre,
                    row.frame,
                ),
                [0.0; 2],
                [0; 2],
            ),
            None => (
                GridFtProjection::between_directions_with_frame(field, phase_centre, row.frame),
                [0.0; 2],
                [0; 2],
            ),
        };
        let projection = projection.map_err(model_error)?;
        let (uvw_m, phase_shift_m) = match &mut self.refocus {
            Some(focus) => focus.apply(&row, &projection)?,
            None => projection.apply(row.uvw),
        };
        if row.raw_flags.shape() != [row.correlations.len(), row.frequencies.len()] {
            return Err(model_error(
                "virtual Fourier prediction flags do not match the visibility layout",
            ));
        }
        let cells = row.frequencies.len() * row.correlations.len();
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
        for ((flags, raw), &frequency) in self
            .flags
            .chunks_exact_mut(row.correlations.len())
            .zip(row.raw_flags.axis_iter(Axis(1)))
            .zip(&self.frequencies)
        {
            let outside = self
                .continuum_axis
                .as_ref()
                .is_some_and(|axis| axis.nearest_channel(frequency).is_none());
            if self.legacy_flags {
                // Legacy GridFT restores VisBuffer's per-correlation flags.
                // FTMachine::fromRecord discards the original channel mask.
                for (value, &raw_flag) in flags.iter_mut().zip(raw) {
                    *value = outside || raw_flag;
                }
            } else {
                flags.fill(outside || raw.iter().any(|flag| *flag));
            }
        }
        let native = NativeRow {
            uvw_m,
            phase_shift_m,
            pointing_offset_rad,
            frequencies_hz: &self.frequencies,
            values: &self.data,
            weights: &self.weights,
            flags: &self.flags,
            row_flag: row.row.flag_row,
            context: RowContext {
                native_frequency_ratio: 1.0 / ratio,
                original_uvw_m: None,
                time_s: row.row.time_seconds,
                antennas: [row.row.antenna1 as u32, row.row.antenna2 as u32],
                antenna_types,
                parallactic_angle_rad: [0.0, 0.0],
                field: row.row.field_id as u32,
                spectral_window: row.row.data_spw_id as u32,
            },
        };
        self.resampler
            .predict_row(
                &self.operator,
                &mut self.backend,
                &self.grids,
                &native,
                &mut self.scratch,
                &mut self.output,
            )
            .map_err(model_error)?;
        if self.scale.is_some() || self.legacy_taylor.is_some() {
            for (samples, frequency) in self
                .output
                .chunks_exact_mut(row.correlations.len())
                .zip(row.frequencies)
            {
                let factor = self
                    .scale
                    .as_ref()
                    .map_or(1.0, |scale| scale.at(*frequency * lsrk_ratio));
                let taylor = self.legacy_taylor.map_or(1.0, |(term, reference)| {
                    // MultiTermFT::modifyModelVis rounds the raw frequency
                    // to Float before constructing the Taylor coordinate.
                    let offset = ((f64::from(*frequency as f32) - reference) / reference) as f32;
                    offset.powi(term)
                });
                for sample in samples {
                    *sample *= factor * taylor;
                }
            }
        }
        for (destination, &value) in output
            .as_slice_memory_order_mut()
            .expect("virtual model output is contiguous")
            .iter_mut()
            .zip(&self.output)
        {
            *destination += value;
        }
        Ok(())
    }
}

fn lsrk_frequency_ratio(row: &FourierRow<'_>) -> Result<f64, GainSolveError> {
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

fn spectral_layout(
    record: &RecordValue,
    axis: &SpectralAxis,
    continuum_basis: Basis,
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
    // FTMachine::fromRecord sets nearest neighbour when reopening a
    // prediction grid, even if the original tclean used another method.
    let kernel = match (record.get("tovis"), record.get("freqinterpmethod")) {
        (Some(Value::Scalar(ScalarValue::Bool(true))), _)
        | (_, Some(Value::Scalar(ScalarValue::Int32(0)))) => SpectralKernel::Nearest,
        (_, Some(Value::Scalar(ScalarValue::Int32(1)))) => SpectralKernel::Linear,
        (_, Some(Value::Scalar(ScalarValue::Int32(2)))) => SpectralKernel::Cubic,
        _ => {
            return Err(model_error(
                "unsupported virtual grid spectral interpolation",
            ));
        }
    };
    let basis = Basis::ChannelLocal { planes: channels };
    Ok((
        basis,
        SpectralResampler::channel_local(axis.clone(), kernel),
    ))
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
        "GridFT" | "WProjectFT" | "MosaicFT" | "MosaicFTNew" | "SetJyGridFT" => {
            Ok((vec![record], Basis::Constant))
        }
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
        "GridFT" | "WProjectFT" | "MosaicFT" | "MosaicFTNew" | "SetJyGridFT"
    ) {
        return Err(model_error(
            "unsupported Fourier image type in virtual model",
        ));
    }
    let recorded = Path::new(record_text(record, "diskimage")?);
    let path = match (recorded.exists(), ms.path(), recorded.file_name()) {
        (false, Some(ms), Some(name)) => [ms.join("SOURCE").join(name), ms.join(name)]
            .into_iter()
            .find(|path| path.exists())
            .unwrap_or_else(|| recorded.to_path_buf()),
        _ => PathBuf::from(recorded),
    };
    PagedImage::open(path).map_err(model_error)
}

fn record_text<'a>(record: &'a RecordValue, field: &str) -> Result<&'a str, GainSolveError> {
    match record.get(field) {
        Some(Value::Scalar(ScalarValue::String(value))) => Ok(value),
        _ => Err(model_error(format!("virtual model has no {field} string"))),
    }
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
        let count = if max_w > 0.0 {
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
        .map(crate::ft::image::polarization_coordinate)
        .collect::<Vec<_>>();
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
