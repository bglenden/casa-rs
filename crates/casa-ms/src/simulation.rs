// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native synthetic MeasurementSet generation for tutorial simulation workflows.
//!
//! This module owns the first reusable MS-writing slice of the VLA simulation
//! vertical. It mirrors the CASA `simobserve` setup order at the data-model
//! level: validate a model image path, set array configuration, define the
//! spectral window and field, sample the requested observing time range, and
//! write CASA-compatible MS subtables plus uncorrupted visibility rows.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use casa_imaging::{ImageGeometry, StandardMfsModelPredictor};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{Array2, ArrayD};
use num_complex::Complex32;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::column_def::{ColumnDef, ColumnKind};
use crate::error::{MsError, MsResult};
use crate::schema::{self, SubtableId};
use crate::{MeasurementSet, MeasurementSetBuilder, OptionalMainColumn};

/// Antenna configuration row for a synthetic observation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticAntenna {
    /// Antenna name, for example `VLA01`.
    pub name: String,
    /// Station or pad name, for example `N01`.
    pub station: String,
    /// ITRF position in meters.
    pub position_m: [f64; 3],
    /// Dish diameter in meters.
    pub dish_diameter_m: f64,
}

impl SyntheticAntenna {
    /// Construct a VLA-style ground-based alt-az antenna row.
    pub fn vla(name: impl Into<String>, station: impl Into<String>, position_m: [f64; 3]) -> Self {
        Self {
            name: name.into(),
            station: station.into(),
            position_m,
            dish_diameter_m: 25.0,
        }
    }
}

/// Single spectral-window setup for a synthetic observation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticSpectralSetup {
    /// Spectral-window name.
    pub name: String,
    /// First channel center frequency in Hz.
    pub start_frequency_hz: f64,
    /// Channel width in Hz.
    pub channel_width_hz: f64,
    /// Number of channels.
    pub channel_count: usize,
}

impl SyntheticSpectralSetup {
    /// Return the channel-center frequencies in Hz.
    pub fn channel_frequencies_hz(&self) -> Vec<f64> {
        (0..self.channel_count)
            .map(|index| self.start_frequency_hz + index as f64 * self.channel_width_hz)
            .collect()
    }

    fn total_bandwidth_hz(&self) -> f64 {
        self.channel_width_hz.abs() * self.channel_count as f64
    }
}

/// Request for generating an uncorrupted synthetic MeasurementSet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticObservationRequest {
    /// Existing model image path that defines the tutorial model provenance.
    pub model_image: PathBuf,
    /// Output MeasurementSet path.
    pub output_ms: PathBuf,
    /// Replace an existing output MeasurementSet directory.
    pub overwrite: bool,
    /// Telescope name written to `OBSERVATION`.
    pub telescope_name: String,
    /// Project code written to `OBSERVATION`.
    pub project: String,
    /// Observer name written to `OBSERVATION`.
    pub observer: String,
    /// Field/source name.
    pub field_name: String,
    /// J2000 phase center `[right_ascension, declination]` in radians.
    pub phase_center_rad: [f64; 2],
    /// Observation start time in MJD seconds UTC.
    pub start_time_mjd_seconds: f64,
    /// Requested on-source duration in seconds.
    pub duration_seconds: f64,
    /// Integration time in seconds.
    pub integration_seconds: f64,
    /// Antenna configuration.
    pub antennas: Vec<SyntheticAntenna>,
    /// Spectral-window setup.
    pub spectral_setup: SyntheticSpectralSetup,
    /// Predict visibility samples from the model image into `MAIN.DATA`.
    pub predict_model: bool,
}

impl SyntheticObservationRequest {
    /// Build the tutorial VLA protoplanetary-disk foundation request.
    pub fn vla_ppdisk(
        model_image: impl Into<PathBuf>,
        output_ms: impl Into<PathBuf>,
        antennas: Vec<SyntheticAntenna>,
    ) -> Self {
        Self {
            model_image: model_image.into(),
            output_ms: output_ms.into(),
            overwrite: false,
            telescope_name: "VLA".to_string(),
            project: "casa-rs-vla-ppdisk".to_string(),
            observer: "casa-rs".to_string(),
            field_name: "ppdisk".to_string(),
            phase_center_rad: [0.0, 0.0],
            start_time_mjd_seconds: 59_000.0 * 86_400.0,
            duration_seconds: 60.0,
            integration_seconds: 10.0,
            antennas,
            spectral_setup: SyntheticSpectralSetup {
                name: "band1".to_string(),
                start_frequency_hz: 672.0e9,
                channel_width_hz: 1.0e6,
                channel_count: 1,
            },
            predict_model: true,
        }
    }
}

/// Summary of a generated synthetic MeasurementSet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyntheticObservationReport {
    /// Output MeasurementSet path.
    pub output_ms: PathBuf,
    /// Model image path recorded as provenance.
    pub model_image: PathBuf,
    /// Number of antennas written.
    pub antenna_count: usize,
    /// Number of baseline rows per time sample.
    pub baseline_count: usize,
    /// Number of time samples written.
    pub time_sample_count: usize,
    /// Number of main-table rows written.
    pub main_row_count: usize,
    /// Number of channels written in the spectral window.
    pub channel_count: usize,
    /// Number of complex visibility cells with non-zero predicted model values.
    pub nonzero_visibility_count: usize,
}

/// Generate an uncorrupted CASA-compatible synthetic MeasurementSet.
///
/// The current implementation writes structurally complete MS metadata and can
/// predict uncorrupted visibility samples from a single-plane FITS model image.
pub fn generate_synthetic_observation_ms(
    request: &SyntheticObservationRequest,
) -> MsResult<SyntheticObservationReport> {
    validate_request(request)?;

    if request.output_ms.exists() {
        if request.overwrite {
            fs::remove_dir_all(&request.output_ms).map_err(|error| {
                MsError::SyntheticObservation(format!(
                    "failed to remove existing output {}: {error}",
                    request.output_ms.display()
                ))
            })?;
        } else {
            return Err(MsError::SyntheticObservation(format!(
                "output MeasurementSet {} already exists",
                request.output_ms.display()
            )));
        }
    }

    let mut ms = MeasurementSet::create(
        &request.output_ms,
        MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
    )?;

    populate_antennas(&mut ms, &request.antennas)?;
    populate_field(&mut ms, request)?;
    populate_spectral_window(&mut ms, &request.spectral_setup)?;
    populate_polarization(&mut ms)?;
    populate_data_description(&mut ms)?;
    populate_state(&mut ms)?;
    populate_processor(&mut ms)?;
    populate_feed(&mut ms, request)?;
    populate_observation(&mut ms, request)?;
    populate_history(&mut ms, request)?;

    let time_sample_count =
        time_sample_count(request.duration_seconds, request.integration_seconds);
    let baseline_count = request.antennas.len() * (request.antennas.len() - 1) / 2;
    let model = if request.predict_model {
        Some(read_fits_model_image(&request.model_image)?)
    } else {
        None
    };
    let nonzero_visibility_count =
        populate_main_rows(&mut ms, request, time_sample_count, model.as_ref())?;

    ms.save()?;

    Ok(SyntheticObservationReport {
        output_ms: request.output_ms.clone(),
        model_image: request.model_image.clone(),
        antenna_count: request.antennas.len(),
        baseline_count,
        time_sample_count,
        main_row_count: baseline_count * time_sample_count,
        channel_count: request.spectral_setup.channel_count,
        nonzero_visibility_count,
    })
}

fn validate_request(request: &SyntheticObservationRequest) -> MsResult<()> {
    if !request.model_image.exists() {
        return Err(MsError::SyntheticObservation(format!(
            "model image {} does not exist",
            request.model_image.display()
        )));
    }
    if request.antennas.len() < 2 {
        return Err(MsError::SyntheticObservation(
            "at least two antennas are required for interferometric simulation".to_string(),
        ));
    }
    for antenna in &request.antennas {
        if antenna.name.trim().is_empty() {
            return Err(MsError::SyntheticObservation(
                "antenna name must not be empty".to_string(),
            ));
        }
        if antenna.station.trim().is_empty() {
            return Err(MsError::SyntheticObservation(format!(
                "antenna {} station must not be empty",
                antenna.name
            )));
        }
        if antenna.dish_diameter_m <= 0.0 || !antenna.dish_diameter_m.is_finite() {
            return Err(MsError::SyntheticObservation(format!(
                "antenna {} dish diameter must be positive",
                antenna.name
            )));
        }
        if antenna.position_m.iter().any(|value| !value.is_finite()) {
            return Err(MsError::SyntheticObservation(format!(
                "antenna {} position must be finite",
                antenna.name
            )));
        }
    }
    if request.spectral_setup.channel_count == 0 {
        return Err(MsError::SyntheticObservation(
            "spectral setup must include at least one channel".to_string(),
        ));
    }
    if request.spectral_setup.start_frequency_hz <= 0.0
        || !request.spectral_setup.start_frequency_hz.is_finite()
    {
        return Err(MsError::SyntheticObservation(
            "spectral start frequency must be positive".to_string(),
        ));
    }
    if request.spectral_setup.channel_width_hz == 0.0
        || !request.spectral_setup.channel_width_hz.is_finite()
    {
        return Err(MsError::SyntheticObservation(
            "spectral channel width must be finite and non-zero".to_string(),
        ));
    }
    if request.duration_seconds <= 0.0 || !request.duration_seconds.is_finite() {
        return Err(MsError::SyntheticObservation(
            "observation duration must be positive".to_string(),
        ));
    }
    if request.integration_seconds <= 0.0 || !request.integration_seconds.is_finite() {
        return Err(MsError::SyntheticObservation(
            "integration time must be positive".to_string(),
        ));
    }
    if request
        .phase_center_rad
        .iter()
        .any(|value| !value.is_finite())
    {
        return Err(MsError::SyntheticObservation(
            "phase center coordinates must be finite".to_string(),
        ));
    }
    Ok(())
}

fn populate_antennas(ms: &mut MeasurementSet, antennas: &[SyntheticAntenna]) -> MsResult<()> {
    let mut antenna_table = ms.antenna_mut()?;
    for antenna in antennas {
        antenna_table.add_antenna(
            &antenna.name,
            &antenna.station,
            "GROUND-BASED",
            "ALT-AZ",
            antenna.position_m,
            [0.0; 3],
            antenna.dish_diameter_m,
        )?;
    }
    Ok(())
}

fn populate_field(ms: &mut MeasurementSet, request: &SyntheticObservationRequest) -> MsResult<()> {
    let direction = Value::Array(ArrayValue::Float64(
        ArrayD::from_shape_vec(vec![2, 1], request.phase_center_rad.to_vec()).unwrap(),
    ));
    let row = row_from_defs(
        schema::field::REQUIRED_COLUMNS,
        &[
            ("NAME", s(&request.field_name)),
            ("CODE", s("")),
            ("NUM_POLY", i(0)),
            ("DELAY_DIR", direction.clone()),
            ("PHASE_DIR", direction.clone()),
            ("REFERENCE_DIR", direction),
            ("SOURCE_ID", i(-1)),
            ("TIME", f(request.start_time_mjd_seconds)),
            ("FLAG_ROW", b(false)),
        ],
    );
    subtable_mut(ms, SubtableId::Field)?.add_row(row)?;
    Ok(())
}

fn populate_spectral_window(
    ms: &mut MeasurementSet,
    spectral_setup: &SyntheticSpectralSetup,
) -> MsResult<()> {
    let frequencies = spectral_setup.channel_frequencies_hz();
    let widths = vec![spectral_setup.channel_width_hz; spectral_setup.channel_count];
    let row = row_from_defs(
        schema::spectral_window::REQUIRED_COLUMNS,
        &[
            ("NUM_CHAN", i(spectral_setup.channel_count as i32)),
            ("NAME", s(&spectral_setup.name)),
            ("REF_FREQUENCY", f(spectral_setup.start_frequency_hz)),
            ("TOTAL_BANDWIDTH", f(spectral_setup.total_bandwidth_hz())),
            (
                "CHAN_FREQ",
                f64_array(&frequencies, vec![frequencies.len()]),
            ),
            ("CHAN_WIDTH", f64_array(&widths, vec![widths.len()])),
            ("EFFECTIVE_BW", f64_array(&widths, vec![widths.len()])),
            ("RESOLUTION", f64_array(&widths, vec![widths.len()])),
            ("MEAS_FREQ_REF", i(5)),
            (
                "NET_SIDEBAND",
                i(if spectral_setup.channel_width_hz >= 0.0 {
                    1
                } else {
                    -1
                }),
            ),
            ("FREQ_GROUP", i(0)),
            ("FREQ_GROUP_NAME", s("")),
            ("IF_CONV_CHAIN", i(0)),
            ("FLAG_ROW", b(false)),
        ],
    );
    subtable_mut(ms, SubtableId::SpectralWindow)?.add_row(row)?;
    Ok(())
}

fn populate_polarization(ms: &mut MeasurementSet) -> MsResult<()> {
    let row = row_from_defs(
        schema::polarization::REQUIRED_COLUMNS,
        &[
            ("NUM_CORR", i(2)),
            ("CORR_TYPE", i32_array(&[5, 8], vec![2])),
            ("CORR_PRODUCT", i32_array(&[0, 1, 0, 1], vec![2, 2])),
            ("FLAG_ROW", b(false)),
        ],
    );
    subtable_mut(ms, SubtableId::Polarization)?.add_row(row)?;
    Ok(())
}

fn populate_data_description(ms: &mut MeasurementSet) -> MsResult<()> {
    let row = row_from_defs(
        schema::data_description::REQUIRED_COLUMNS,
        &[
            ("SPECTRAL_WINDOW_ID", i(0)),
            ("POLARIZATION_ID", i(0)),
            ("FLAG_ROW", b(false)),
        ],
    );
    subtable_mut(ms, SubtableId::DataDescription)?.add_row(row)?;
    Ok(())
}

fn populate_state(ms: &mut MeasurementSet) -> MsResult<()> {
    let row = row_from_defs(
        schema::state::REQUIRED_COLUMNS,
        &[
            ("CAL", f(0.0)),
            ("FLAG_ROW", b(false)),
            ("LOAD", f(0.0)),
            ("OBS_MODE", s("TARGET.ON_SOURCE")),
            ("REF", b(false)),
            ("SIG", b(true)),
            ("SUB_SCAN", i(0)),
        ],
    );
    subtable_mut(ms, SubtableId::State)?.add_row(row)?;
    Ok(())
}

fn populate_processor(ms: &mut MeasurementSet) -> MsResult<()> {
    let row = row_from_defs(
        schema::processor::REQUIRED_COLUMNS,
        &[
            ("FLAG_ROW", b(false)),
            ("MODE_ID", i(0)),
            ("SUB_TYPE", s("SYNTHETIC")),
            ("TYPE", s("CORRELATOR")),
            ("TYPE_ID", i(0)),
        ],
    );
    subtable_mut(ms, SubtableId::Processor)?.add_row(row)?;
    Ok(())
}

fn populate_feed(ms: &mut MeasurementSet, request: &SyntheticObservationRequest) -> MsResult<()> {
    for antenna_id in 0..request.antennas.len() {
        let row = row_from_defs(
            schema::feed::REQUIRED_COLUMNS,
            &[
                ("ANTENNA_ID", i(antenna_id as i32)),
                ("BEAM_ID", i(-1)),
                ("BEAM_OFFSET", f64_array(&[0.0, 0.0, 0.0, 0.0], vec![2, 2])),
                ("FEED_ID", i(0)),
                ("INTERVAL", f(request.duration_seconds)),
                ("NUM_RECEPTORS", i(2)),
                (
                    "POL_RESPONSE",
                    complex_array(
                        &[
                            Complex32::new(1.0, 0.0),
                            Complex32::new(0.0, 0.0),
                            Complex32::new(0.0, 0.0),
                            Complex32::new(1.0, 0.0),
                        ],
                        vec![2, 2],
                    ),
                ),
                ("POLARIZATION_TYPE", string_array(&["R", "L"], vec![2])),
                ("POSITION", f64_array(&[0.0, 0.0, 0.0], vec![3])),
                ("RECEPTOR_ANGLE", f64_array(&[0.0, 0.0], vec![2])),
                ("SPECTRAL_WINDOW_ID", i(0)),
                ("TIME", f(request.start_time_mjd_seconds)),
            ],
        );
        subtable_mut(ms, SubtableId::Feed)?.add_row(row)?;
    }
    Ok(())
}

fn populate_observation(
    ms: &mut MeasurementSet,
    request: &SyntheticObservationRequest,
) -> MsResult<()> {
    let end_time = request.start_time_mjd_seconds + request.duration_seconds;
    let row = row_from_defs(
        schema::observation::REQUIRED_COLUMNS,
        &[
            ("FLAG_ROW", b(false)),
            (
                "LOG",
                string_array(&["generated by casa-rs synthetic observation"], vec![1]),
            ),
            ("OBSERVER", s(&request.observer)),
            ("PROJECT", s(&request.project)),
            ("RELEASE_DATE", f(0.0)),
            ("SCHEDULE", string_array(&["synthetic"], vec![1])),
            ("SCHEDULE_TYPE", s("synthetic")),
            ("TELESCOPE_NAME", s(&request.telescope_name)),
            (
                "TIME_RANGE",
                f64_array(&[request.start_time_mjd_seconds, end_time], vec![2]),
            ),
        ],
    );
    subtable_mut(ms, SubtableId::Observation)?.add_row(row)?;
    Ok(())
}

fn populate_history(
    ms: &mut MeasurementSet,
    request: &SyntheticObservationRequest,
) -> MsResult<()> {
    let message = format!(
        "generated synthetic observation from model image {}",
        request.model_image.display()
    );
    let row = row_from_defs(
        schema::history::REQUIRED_COLUMNS,
        &[
            ("APPLICATION", s("casa-rs")),
            ("APP_PARAMS", string_array(&[&message], vec![1])),
            ("CLI_COMMAND", string_array(&[], vec![0])),
            ("MESSAGE", s(&message)),
            ("OBJECT_ID", i(0)),
            ("OBSERVATION_ID", i(0)),
            ("ORIGIN", s("casa_ms::simulation")),
            ("PRIORITY", s("NORMAL")),
            ("TIME", f(request.start_time_mjd_seconds)),
        ],
    );
    subtable_mut(ms, SubtableId::History)?.add_row(row)?;
    Ok(())
}

fn populate_main_rows(
    ms: &mut MeasurementSet,
    request: &SyntheticObservationRequest,
    samples: usize,
    model: Option<&FitsModelImage>,
) -> MsResult<usize> {
    let num_corr = 2usize;
    let num_chan = request.spectral_setup.channel_count;
    let flag = bool_array(&vec![false; num_corr * num_chan], vec![num_corr, num_chan]);
    let flag_category = bool_array(
        &vec![false; num_corr * num_chan],
        vec![1, num_corr, num_chan],
    );
    let weight = f32_array(&vec![1.0; num_corr], vec![num_corr]);
    let sigma = f32_array(&vec![1.0; num_corr], vec![num_corr]);
    let main_defs = ms_main_defs(ms);
    let predictors = model
        .map(|model| build_channel_predictors(model, &request.spectral_setup))
        .transpose()?;
    let mut nonzero_visibility_count = 0usize;

    for sample in 0..samples {
        let time =
            request.start_time_mjd_seconds + (sample as f64 + 0.5) * request.integration_seconds;
        for antenna1 in 0..request.antennas.len() {
            for antenna2 in (antenna1 + 1)..request.antennas.len() {
                let uvw = baseline_uvw(
                    request.antennas[antenna1].position_m,
                    request.antennas[antenna2].position_m,
                );
                let data = predicted_data_array(
                    predictors.as_ref(),
                    &request.spectral_setup,
                    uvw,
                    num_corr,
                    &mut nonzero_visibility_count,
                );
                let row = row_from_defs(
                    &main_defs,
                    &[
                        ("ANTENNA1", i(antenna1 as i32)),
                        ("ANTENNA2", i(antenna2 as i32)),
                        ("ARRAY_ID", i(0)),
                        ("DATA_DESC_ID", i(0)),
                        ("EXPOSURE", f(request.integration_seconds)),
                        ("FEED1", i(0)),
                        ("FEED2", i(0)),
                        ("FIELD_ID", i(0)),
                        ("FLAG", flag.clone()),
                        ("FLAG_CATEGORY", flag_category.clone()),
                        ("FLAG_ROW", b(false)),
                        ("INTERVAL", f(request.integration_seconds)),
                        ("OBSERVATION_ID", i(0)),
                        ("PROCESSOR_ID", i(0)),
                        ("SCAN_NUMBER", i(1)),
                        ("SIGMA", sigma.clone()),
                        ("STATE_ID", i(0)),
                        ("TIME", f(time)),
                        ("TIME_CENTROID", f(time)),
                        ("UVW", f64_array(&uvw, vec![3])),
                        ("WEIGHT", weight.clone()),
                        ("DATA", data.clone()),
                    ],
                );
                ms.main_table_mut().add_row(row)?;
            }
        }
    }
    Ok(nonzero_visibility_count)
}

#[derive(Debug, Clone)]
struct FitsModelImage {
    pixels: Array2<f32>,
    cell_size_rad: [f64; 2],
}

fn build_channel_predictors(
    model: &FitsModelImage,
    spectral_setup: &SyntheticSpectralSetup,
) -> MsResult<Vec<StandardMfsModelPredictor>> {
    let geometry = ImageGeometry {
        image_shape: [model.pixels.shape()[0], model.pixels.shape()[1]],
        cell_size_rad: model.cell_size_rad,
    };
    (0..spectral_setup.channel_count)
        .map(|_| {
            StandardMfsModelPredictor::new(geometry, &model.pixels).map_err(|error| {
                MsError::SyntheticObservation(format!("model prediction setup failed: {error}"))
            })
        })
        .collect()
}

fn predicted_data_array(
    predictors: Option<&Vec<StandardMfsModelPredictor>>,
    spectral_setup: &SyntheticSpectralSetup,
    uvw_m: [f64; 3],
    num_corr: usize,
    nonzero_visibility_count: &mut usize,
) -> Value {
    let mut values = vec![Complex32::new(0.0, 0.0); num_corr * spectral_setup.channel_count];
    if let Some(predictors) = predictors {
        for (channel, predictor) in predictors.iter().enumerate() {
            let frequency_hz = spectral_setup.start_frequency_hz
                + channel as f64 * spectral_setup.channel_width_hz;
            let wavelength_m = 299_792_458.0 / frequency_hz;
            let visibility = predictor.predict(uvw_m[0] / wavelength_m, uvw_m[1] / wavelength_m);
            for corr in 0..num_corr {
                let index = corr * spectral_setup.channel_count + channel;
                values[index] = visibility;
                if visibility.re != 0.0 || visibility.im != 0.0 {
                    *nonzero_visibility_count += 1;
                }
            }
        }
    }
    complex_array(&values, vec![num_corr, spectral_setup.channel_count])
}

fn read_fits_model_image(path: &PathBuf) -> MsResult<FitsModelImage> {
    let mut file = fs::File::open(path).map_err(|error| {
        MsError::SyntheticObservation(format!(
            "failed to open model image {}: {error}",
            path.display()
        ))
    })?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(|error| {
        MsError::SyntheticObservation(format!(
            "failed to read model image {}: {error}",
            path.display()
        ))
    })?;
    let (cards, data_offset) = parse_fits_header(&bytes, path)?;
    let bitpix = fits_i64(&cards, "BITPIX", path)?;
    let naxis = fits_i64(&cards, "NAXIS", path)? as usize;
    if naxis < 2 {
        return Err(MsError::SyntheticObservation(format!(
            "model image {} must have at least two FITS axes",
            path.display()
        )));
    }
    let nx = fits_i64(&cards, "NAXIS1", path)? as usize;
    let ny = fits_i64(&cards, "NAXIS2", path)? as usize;
    if nx < 8 || ny < 8 {
        return Err(MsError::SyntheticObservation(format!(
            "model image {} must be at least 8x8 pixels",
            path.display()
        )));
    }
    let trailing_planes = (3..=naxis)
        .map(|axis| fits_i64(&cards, &format!("NAXIS{axis}"), path).unwrap_or(1))
        .product::<i64>();
    if trailing_planes != 1 {
        return Err(MsError::SyntheticObservation(format!(
            "model image {} must have one Stokes/frequency plane for this Wave 5 slice",
            path.display()
        )));
    }
    let cell_size_rad = [
        fits_axis_cell_rad(&cards, 1, path)?.abs(),
        fits_axis_cell_rad(&cards, 2, path)?.abs(),
    ];
    let pixel_count = nx
        .checked_mul(ny)
        .ok_or_else(|| MsError::SyntheticObservation("model image shape overflows".to_string()))?;
    let bytes_per_pixel = match bitpix {
        -32 => 4usize,
        -64 => 8usize,
        other => {
            return Err(MsError::SyntheticObservation(format!(
                "model image {} uses unsupported BITPIX={other}; expected -32 or -64",
                path.display()
            )));
        }
    };
    let data_len = pixel_count * bytes_per_pixel;
    if bytes.len() < data_offset + data_len {
        return Err(MsError::SyntheticObservation(format!(
            "model image {} is truncated before primary image data",
            path.display()
        )));
    }

    let bscale = fits_optional_f64(&cards, "BSCALE").unwrap_or(1.0);
    let bzero = fits_optional_f64(&cards, "BZERO").unwrap_or(0.0);
    let mut pixels = Array2::<f32>::zeros((nx, ny));
    let data = &bytes[data_offset..data_offset + data_len];
    for y in 0..ny {
        for x in 0..nx {
            let index = y * nx + x;
            let raw = match bitpix {
                -32 => {
                    let start = index * 4;
                    f32::from_bits(u32::from_be_bytes([
                        data[start],
                        data[start + 1],
                        data[start + 2],
                        data[start + 3],
                    ])) as f64
                }
                -64 => {
                    let start = index * 8;
                    f64::from_bits(u64::from_be_bytes([
                        data[start],
                        data[start + 1],
                        data[start + 2],
                        data[start + 3],
                        data[start + 4],
                        data[start + 5],
                        data[start + 6],
                        data[start + 7],
                    ]))
                }
                _ => unreachable!(),
            };
            pixels[(x, y)] = (raw * bscale + bzero) as f32;
        }
    }

    Ok(FitsModelImage {
        pixels,
        cell_size_rad,
    })
}

fn parse_fits_header(bytes: &[u8], path: &Path) -> MsResult<(Vec<String>, usize)> {
    let mut cards = Vec::new();
    for (card_index, chunk) in bytes.chunks(80).enumerate() {
        if chunk.len() < 80 {
            break;
        }
        let card = std::str::from_utf8(chunk).map_err(|error| {
            MsError::SyntheticObservation(format!(
                "model image {} contains non-ASCII FITS header card: {error}",
                path.display()
            ))
        })?;
        cards.push(card.to_string());
        if card.starts_with("END") {
            let header_bytes = (card_index + 1) * 80;
            let data_offset = header_bytes.div_ceil(2880) * 2880;
            return Ok((cards, data_offset));
        }
    }
    Err(MsError::SyntheticObservation(format!(
        "model image {} has no FITS END header card",
        path.display()
    )))
}

fn fits_i64(cards: &[String], key: &str, path: &Path) -> MsResult<i64> {
    fits_value(cards, key)
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(|| {
            MsError::SyntheticObservation(format!(
                "model image {} missing integer FITS key {key}",
                path.display()
            ))
        })
}

fn fits_axis_cell_rad(cards: &[String], axis: usize, path: &Path) -> MsResult<f64> {
    let value = fits_optional_f64(cards, &format!("CDELT{axis}")).ok_or_else(|| {
        MsError::SyntheticObservation(format!(
            "model image {} missing FITS CDELT{axis}",
            path.display()
        ))
    })?;
    let unit = fits_string(cards, &format!("CUNIT{axis}")).unwrap_or_else(|| "deg".to_string());
    match unit.trim().to_ascii_lowercase().as_str() {
        "deg" | "degree" | "degrees" => Ok(value.to_radians()),
        "rad" | "radian" | "radians" => Ok(value),
        other => Err(MsError::SyntheticObservation(format!(
            "model image {} uses unsupported CUNIT{axis}={other:?}; expected deg or rad",
            path.display()
        ))),
    }
}

fn fits_optional_f64(cards: &[String], key: &str) -> Option<f64> {
    fits_value(cards, key).and_then(|value| value.parse::<f64>().ok())
}

fn fits_string(cards: &[String], key: &str) -> Option<String> {
    fits_value(cards, key).map(|value| value.trim().trim_matches('\'').trim().to_string())
}

fn fits_value<'a>(cards: &'a [String], key: &str) -> Option<&'a str> {
    cards.iter().find_map(|card| {
        if card.get(..8)?.trim() != key {
            return None;
        }
        let value = card.get(10..)?;
        Some(value.split('/').next().unwrap_or(value).trim())
    })
}

fn subtable_mut(ms: &mut MeasurementSet, id: SubtableId) -> MsResult<&mut casa_tables::Table> {
    ms.subtable_mut(id)
        .ok_or_else(|| MsError::MissingSubtable(id.name().to_string()))
}

fn time_sample_count(duration_seconds: f64, integration_seconds: f64) -> usize {
    (duration_seconds / integration_seconds).ceil().max(1.0) as usize
}

fn baseline_uvw(a: [f64; 3], b: [f64; 3]) -> [f64; 3] {
    [b[0] - a[0], b[1] - a[1], b[2] - a[2]]
}

fn ms_main_defs(ms: &MeasurementSet) -> Vec<ColumnDef> {
    let all_defs = schema::main_table::REQUIRED_COLUMNS
        .iter()
        .chain(schema::main_table::OPTIONAL_COLUMNS.iter())
        .copied()
        .collect::<Vec<_>>();
    ms.main_table()
        .schema()
        .expect("main table schema")
        .columns()
        .iter()
        .map(|column| {
            *all_defs
                .iter()
                .find(|definition| definition.name == column.name())
                .expect("known MS main column")
        })
        .collect()
}

fn row_from_defs(defs: &[ColumnDef], overrides: &[(&str, Value)]) -> RecordValue {
    let fields = defs
        .iter()
        .map(|definition| {
            if let Some((_, value)) = overrides.iter().find(|(name, _)| *name == definition.name) {
                RecordField::new(definition.name, value.clone())
            } else {
                RecordField::new(definition.name, default_value(definition))
            }
        })
        .collect();
    RecordValue::new(fields)
}

fn default_value(definition: &ColumnDef) -> Value {
    match definition.column_kind {
        ColumnKind::Scalar => match definition.data_type {
            casa_types::PrimitiveType::Bool => b(false),
            casa_types::PrimitiveType::Int32 => i(0),
            casa_types::PrimitiveType::Float64 => f(0.0),
            casa_types::PrimitiveType::String => s(""),
            casa_types::PrimitiveType::Float32 => Value::Scalar(ScalarValue::Float32(0.0)),
            casa_types::PrimitiveType::Complex32 => {
                Value::Scalar(ScalarValue::Complex32(Complex32::new(0.0, 0.0)))
            }
            _ => i(0),
        },
        ColumnKind::FixedArray { shape } => {
            let total = shape.iter().product();
            match definition.data_type {
                casa_types::PrimitiveType::Bool => bool_array(&vec![false; total], shape.to_vec()),
                casa_types::PrimitiveType::Float32 => f32_array(&vec![0.0; total], shape.to_vec()),
                casa_types::PrimitiveType::Int32 => i32_array(&vec![0; total], shape.to_vec()),
                casa_types::PrimitiveType::String => string_array(&vec![""; total], shape.to_vec()),
                _ => f64_array(&vec![0.0; total], shape.to_vec()),
            }
        }
        ColumnKind::VariableArray { ndim } => {
            let shape = vec![1; ndim];
            let total = shape.iter().product();
            match definition.data_type {
                casa_types::PrimitiveType::Bool => bool_array(&vec![false; total], shape),
                casa_types::PrimitiveType::Float32 => f32_array(&vec![0.0; total], shape),
                casa_types::PrimitiveType::Int32 => i32_array(&vec![0; total], shape),
                casa_types::PrimitiveType::String => string_array(&vec![""; total], shape),
                casa_types::PrimitiveType::Complex32 => {
                    complex_array(&vec![Complex32::new(0.0, 0.0); total], shape)
                }
                _ => f64_array(&vec![0.0; total], shape),
            }
        }
    }
}

fn s(value: &str) -> Value {
    Value::Scalar(ScalarValue::String(value.to_string()))
}

fn i(value: i32) -> Value {
    Value::Scalar(ScalarValue::Int32(value))
}

fn f(value: f64) -> Value {
    Value::Scalar(ScalarValue::Float64(value))
}

fn b(value: bool) -> Value {
    Value::Scalar(ScalarValue::Bool(value))
}

fn bool_array(values: &[bool], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::Bool(
        ArrayD::from_shape_vec(shape, values.to_vec()).unwrap(),
    ))
}

fn i32_array(values: &[i32], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::Int32(
        ArrayD::from_shape_vec(shape, values.to_vec()).unwrap(),
    ))
}

fn f32_array(values: &[f32], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::Float32(
        ArrayD::from_shape_vec(shape, values.to_vec()).unwrap(),
    ))
}

fn f64_array(values: &[f64], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::Float64(
        ArrayD::from_shape_vec(shape, values.to_vec()).unwrap(),
    ))
}

fn complex_array(values: &[Complex32], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::Complex32(
        ArrayD::from_shape_vec(shape, values.to_vec()).unwrap(),
    ))
}

fn string_array(values: &[&str], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::String(
        ArrayD::from_shape_vec(
            shape,
            values.iter().map(|value| value.to_string()).collect(),
        )
        .unwrap(),
    ))
}
