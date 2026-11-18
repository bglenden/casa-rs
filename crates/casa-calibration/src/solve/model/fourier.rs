// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read an already transformed CASA virtual GridFT model and degrid it
//! using the same measurement operator as imaging.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use casa_coordinates::{Coordinate, CoordinateModel, CoordinateType, StokesType};
use casa_images::PagedImage;
use casa_imaging_model::{GridFtProjection, PolarizationCoordinate};
use casa_imaging_operator::{
    Basis, Boxcar, ConvolutionFunctionSet, CpuBackend, GridGeometry, GridPadding, GridPrecision,
    ImageExtent, MeasurementOperator, NativeRow, PlaneRange, PolarizationRouting,
    PredictionScratch, PreparedModelGrids, RowContext, SpectralAxis, SpectralKernel,
    SpectralResampler, Spheroidal,
};
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::{MeasurementSet, convert_frequency_to_frame};
use casa_types::measures::MeasFrame;
use casa_types::measures::direction::MDirection;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::{Complex32, Correlation, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;

use super::{GainSolveError, SelectedSolveRow, model_error};

pub(super) struct FourierModel {
    record: RecordValue,
    prepared: Mutex<BTreeMap<Vec<i32>, PreparedFourierModel>>,
}

pub(super) struct FourierRow<'a> {
    pub(super) ms: &'a MeasurementSet,
    pub(super) row: &'a SelectedSolveRow,
    pub(super) correlations: &'a [i32],
    pub(super) frequencies: &'a [f64],
    pub(super) frequency_ref: FrequencyRef,
    pub(super) engine: &'a MsCalEngine,
    pub(super) field: &'a MDirection,
    pub(super) frame: &'a MeasFrame,
    pub(super) uvw: [f64; 3],
}

impl FourierModel {
    pub(super) fn new(record: RecordValue) -> Self {
        Self {
            record,
            prepared: Mutex::default(),
        }
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
        if !prepared.contains_key(row.correlations) {
            prepared.insert(
                row.correlations.to_vec(),
                PreparedFourierModel::load(&self.record, row.ms, row.correlations)?,
            );
        }
        prepared
            .get_mut(row.correlations)
            .expect("correlation layout was prepared")
            .predict(row, output)
    }
}

struct PreparedFourierModel {
    operator: MeasurementOperator,
    resampler: SpectralResampler,
    grids: PreparedModelGrids,
    phase_centre: MDirection,
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
        ms: &MeasurementSet,
        correlations: &[i32],
    ) -> Result<Self, GainSolveError> {
        let (terms, continuum_basis) = grid_terms(record)?;
        let images = terms
            .iter()
            .map(|term| open_grid(term, ms))
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
        let centre = direction.reference_value();
        let phase_centre = MDirection::from_angles(centre[0], centre[1], direction.direction_ref());
        let geometry = GridGeometry::new(
            ImageExtent {
                shape: [nx, ny],
                increment_rad: [increments[0], increments[1]],
                reference_pixel: [nx / 2, ny / 2],
            },
            GridPadding::None,
        )
        .map_err(model_error)?;
        let polarization = grid_polarization(image, correlations)?;
        if polarization.grid_pols() != npol {
            return Err(model_error(
                "virtual grid polarization axis does not match its planes",
            ));
        }
        let axis = image_spectral_axis(image, channels)?;
        let (basis, resampler) = spectral_layout(record, &axis, continuum_basis)?;
        let convolution = record_text(terms[0], "convtype")?;
        let kernels: Box<dyn ConvolutionFunctionSet> = match convolution {
            "SF" => Box::new(Spheroidal::new(&geometry, &polarization)),
            "BOX" => Box::new(Boxcar::new(&geometry, &polarization)),
            name => {
                return Err(model_error(format!(
                    "unsupported virtual grid convolution {name}"
                )));
            }
        };
        let operator =
            MeasurementOperator::new(geometry, basis, polarization, kernels, GridPrecision::F32);
        let grids = load_grids(&operator, &images)?;
        Ok(Self {
            operator,
            resampler,
            grids,
            phase_centre,
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
        let projection = GridFtProjection::between_directions_with_frame(
            row.field,
            &self.phase_centre,
            row.frame,
        )
        .map_err(model_error)?;
        let (uvw_m, phase_shift_m) = projection.apply(row.uvw);
        let cells = row.frequencies.len() * row.correlations.len();
        self.data.resize(cells, Complex32::new(0.0, 0.0));
        self.output.resize(cells, Complex32::new(0.0, 0.0));
        self.weights.resize(cells, 1.0);
        self.flags.resize(cells, false);
        let ratio = if self.convert_frequencies && row.frequency_ref != FrequencyRef::LSRK {
            let reference = row.frequencies[0];
            convert_frequency_to_frame(
                row.frequency_ref,
                FrequencyRef::LSRK,
                reference,
                row.row.time_seconds,
                row.row.field_id as usize,
                row.engine,
            )
            .map_err(model_error)?
                / reference
        } else {
            1.0
        };
        self.frequencies.clear();
        self.frequencies
            .extend(row.frequencies.iter().map(|frequency| frequency * ratio));
        if let Some(axis) = &self.continuum_axis {
            for (flags, &frequency) in self
                .flags
                .chunks_exact_mut(row.correlations.len())
                .zip(&self.frequencies)
            {
                flags.fill(axis.nearest_channel(frequency).is_none());
            }
        }
        let native = NativeRow {
            uvw_m,
            phase_shift_m,
            pointing_offset_rad: [0.0; 2],
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
                antenna_types: [0, 0],
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
        "GridFT" => Ok((vec![record], Basis::Constant)),
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

fn open_grid(
    record: &RecordValue,
    ms: &MeasurementSet,
) -> Result<PagedImage<Complex32>, GainSolveError> {
    if record_text(record, "name")? != "GridFT" {
        return Err(model_error("virtual Taylor models require GridFT terms"));
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
    if stokes != [StokesType::I] {
        return Err(model_error(
            "virtual GridFT currently requires a Stokes I grid",
        ));
    }
    let correlations = correlations
        .iter()
        .map(|&code| {
            Correlation::from_casacore_code(code)
                .ok_or_else(|| model_error(format!("invalid virtual grid correlation {code}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    PolarizationRouting::compile(&correlations, &[PolarizationCoordinate::StokesI])
        .map_err(model_error)
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
