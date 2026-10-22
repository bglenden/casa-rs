// SPDX-License-Identifier: LGPL-3.0-or-later
//! What the session prediction tests share: model images, a small array,
//! sessions, MAIN read back, and the per-row reference prediction.

#![allow(dead_code, reason = "each test file uses part of the support")]

use std::path::{Path, PathBuf};

use casa_coordinates::{
    CoordinateSystem as ImageCoordinates, DirectionCoordinate, Projection, ProjectionType,
    SpectralCoordinate, StokesCoordinate, StokesType,
};
use casa_images::PagedImage;
use casa_imaging_model::CorrelationType;
use casa_imaging_runtime::{HostResources, ResourcePolicy};
use casa_ms::MeasurementSet;
use casa_ms::columns::data_columns::DataColumn;
use casa_ms::columns::flag_columns::FlagRowColumn;
use casa_ms::columns::main_ids::{antenna1, antenna2, field_id};
use casa_ms::columns::uvw_column::UvwColumn;
use casa_ms::derived::GridFtProjection;
use casa_simulation::predict::{
    ModelPredictor, PredictOptions, PredictRow, PredictionWorkspace, PrimaryBeam, SkyModel,
};
use casa_simulation::{
    ArrayConfig, BackendRequest, CoordinateSystem, ObserveSpec, PredictSpec, SessionResources,
    Simulator, SimulatorOptions, SpectralWindowSpec, StateSpec,
};
use casa_types::ArrayValue;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, MEpoch};
use ndarray::{ArrayD, IxDyn};
use num_complex::Complex32;

/// 2024-06-01 00:00 UTC.
pub const REFERENCE_MJD: f64 = 60_462.0;
/// Model pixels a side.
pub const N: usize = 64;
/// Model cell, radians (0.41″): the model is 26″ across, near the ALMA
/// beam's 28″ null at 230 GHz.
pub const CELL_RAD: f64 = 2.0e-6;
/// The model's centre, its tangent point, and the field `target`.
pub const CENTRE_RAD: [f64; 2] = [2.617_993_877_991_494_4, 0.593_411_945_678_072];
/// The field `offset`, about 3 and 2 cells from the centre.
pub const OFFSET_RAD: [f64; 2] = [
    2.617_993_877_991_494_4 + 6.0e-6 / 0.833_885_822_067_879_6,
    0.593_411_945_678_072 - 4.0e-6,
];
/// The first channel; the band is four channels of 15.625 MHz.
pub const FREQUENCY_HZ: f64 = 230.0e9;
pub const CHANNEL_HZ: f64 = 15.625e6;
pub const CHANNELS: usize = 4;
pub const ANTENNAS: usize = 8;

/// A one-Stokes CASA model image of `planes` planes, each a list of
/// `(x, y, flux)` points, centred on [`CENTRE_RAD`]: one plane per channel,
/// or one plane spanning the band.
pub fn write_model(dir: &Path, name: &str, planes: &[Vec<(usize, usize, f32)>]) -> PathBuf {
    let path = dir.join(name);
    let mut coordinates = ImageCoordinates::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        CENTRE_RAD,
        [-CELL_RAD, CELL_RAD],
        [(N / 2) as f64, (N / 2) as f64],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    let (first, width) = if planes.len() == 1 {
        (
            FREQUENCY_HZ + 1.5 * CHANNEL_HZ,
            CHANNELS as f64 * CHANNEL_HZ,
        )
    } else {
        (FREQUENCY_HZ, CHANNEL_HZ)
    };
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        first,
        width,
        0.0,
        first,
    ));
    let count = planes.len();
    let mut image = PagedImage::<f32>::create(vec![N, N, 1, count], coordinates, &path)
        .expect("create model image");
    let mut pixels = ArrayD::zeros(IxDyn(&[N, N, 1, count]));
    for (plane, points) in planes.iter().enumerate() {
        for &(x, y, flux) in points {
            pixels[IxDyn(&[x, y, 0, plane])] = flux;
        }
    }
    image
        .put_slice(&pixels, &[0, 0, 0, 0])
        .expect("write pixels");
    image.set_units("Jy/pixel").expect("units");
    image.save().expect("save model image");
    path
}

/// A model of points and a blob spread over the image, one plane spanning
/// the band.
pub fn extended_model(dir: &Path, name: &str) -> PathBuf {
    let mut points = vec![
        (N / 2, N / 2, 1.5),
        (N / 2 + 9, N / 2 - 4, 0.75),
        (8, 50, 0.5),
    ];
    for dx in 0..4 {
        for dy in 0..3 {
            points.push((20 + dx, 14 + dy, 0.05));
        }
    }
    write_model(dir, name, &[points])
}

/// A model with one plane per channel, each with its own flux.
pub fn cube_model(dir: &Path, name: &str) -> PathBuf {
    let planes = (0..CHANNELS)
        .map(|plane| {
            vec![
                (N / 2 - 3, N / 2 + 5, 1.0 + plane as f32 * 0.25),
                (N / 2 + 12, N / 2 + 1, 0.4),
            ]
        })
        .collect::<Vec<_>>();
    write_model(dir, name, &planes)
}

/// A pinned eight-thread host with a policy of `workers` workers and
/// `memory` bytes, predicting on the CPU.
pub fn resources(workers: usize, memory: u64) -> SessionResources {
    SessionResources::pinned(
        HostResources {
            threads: 8,
            performance_cores: 8,
            available_memory: 8 << 30,
            metal: false,
        },
        ResourcePolicy::Explicit { workers, memory },
        BackendRequest::Cpu,
    )
    .expect("resources")
}

pub fn scan(source: &str, start_s: f64, stop_s: f64) -> ObserveSpec {
    ObserveSpec {
        source: source.to_string(),
        spectral_window: "band6".to_string(),
        start_s,
        stop_s,
        add_observation: false,
        state: StateSpec {
            sig: true,
            reference: false,
            cal: 0.0,
            load: 0.0,
            sub_scan: 0,
            obs_mode: "OBSERVE_TARGET.ON_SOURCE".to_string(),
        },
        observer: "tester".to_string(),
        project: "predict".to_string(),
    }
}

/// A session of [`ANTENNAS`] 12 m antennas of `telescope`, 24 m apart in a
/// line (baselines inside the model's padded grid at 230 GHz), one window of
/// `stokes` over the model's band, and the fields `target` (the model's
/// centre) and `offset`, before `observe`. Autocorrelations are written
/// (CASA's default weight).
pub fn session(
    path: &Path,
    telescope: &str,
    stokes: &str,
    resources: SessionResources,
) -> Simulator {
    session_with_channels(path, telescope, stokes, CHANNELS, resources)
}

/// [`session`] with a window of `channels` channels of [`CHANNEL_HZ`] from
/// [`FREQUENCY_HZ`]: a model of as many planes ([`write_model`]) has one
/// plane per channel.
pub fn session_with_channels(
    path: &Path,
    telescope: &str,
    stokes: &str,
    channels: usize,
    resources: SessionResources,
) -> Simulator {
    let mut session = Simulator::open(
        path,
        SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources,
            now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
        },
    );
    let centre = [-1_601_185.0, -5_041_977.0, 3_554_875.0];
    let along = |axis: usize, step: f64| {
        (0..ANTENNAS)
            .map(|index| centre[axis] + step * index as f64)
            .collect()
    };
    session
        .set_config(&ArrayConfig {
            telescope: telescope.to_string(),
            x: along(0, 20.0),
            y: along(1, -6.0),
            z: along(2, 12.0),
            dish_diameter: vec![12.0],
            offset: vec![0.0],
            mount: vec!["alt-az".to_string()],
            antenna_names: vec!["DA".to_string()],
            pad_names: vec!["A".to_string()],
            coordinate_system: CoordinateSystem::Global,
        })
        .expect("setconfig");
    session
        .set_spectral_window(&SpectralWindowSpec {
            name: "band6".to_string(),
            start_frequency_hz: FREQUENCY_HZ,
            channel_width_hz: CHANNEL_HZ,
            resolution_hz: CHANNEL_HZ,
            frame: FrequencyRef::TOPO,
            channel_count: channels,
            stokes: stokes.to_string(),
        })
        .expect("setspwindow");
    for (name, direction) in [("target", CENTRE_RAD), ("offset", OFFSET_RAD)] {
        session
            .set_field(
                name,
                &MDirection::from_angles(direction[0], direction[1], DirectionRef::J2000),
                "",
                0.0,
            )
            .expect("setfield");
    }
    session
        .set_times(10.0, false, MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC))
        .expect("settimes");
    session
}

/// `sm.predict(image, incremental=…)`.
pub fn predict_spec(image: &Path, incremental: bool) -> PredictSpec {
    PredictSpec {
        images: vec![image.to_path_buf()],
        component_list: None,
        incremental,
    }
}

/// One MAIN row as the tests read it.
#[derive(Debug, Clone)]
pub struct Row {
    /// DATA, `[channel][correlation]`.
    pub data: Vec<Complex32>,
    pub uvw_m: [f64; 3],
    pub antennas: [u32; 2],
    pub flag_row: bool,
    pub field: usize,
}

/// MAIN of `path`, and the window's correlations.
pub fn read_main(path: &Path) -> (Vec<Row>, usize) {
    let ms = MeasurementSet::open(path).expect("open");
    let main = ms.main_table();
    let data = DataColumn::data(main).expect("DATA");
    let uvw = UvwColumn::new(main);
    let flag_row = FlagRowColumn::new(main);
    let (first, second, field) = (antenna1(main), antenna2(main), field_id(main));
    let mut correlations = 0;
    let rows = (0..ms.row_count())
        .map(|row| {
            let ArrayValue::Complex32(cell) = data.get(row).expect("DATA cell") else {
                panic!("DATA is not Complex32");
            };
            let [corr, channels] = cell.shape() else {
                panic!("DATA cells are matrices");
            };
            correlations = *corr;
            Row {
                data: (0..*channels)
                    .flat_map(|channel| (0..*corr).map(move |c| (c, channel)))
                    .map(|(c, channel)| cell[IxDyn(&[c, channel])])
                    .collect(),
                uvw_m: uvw.get(row).expect("UVW"),
                antennas: [
                    first.get(row).expect("ANTENNA1") as u32,
                    second.get(row).expect("ANTENNA2") as u32,
                ],
                flag_row: flag_row.get(row).expect("FLAG_ROW"),
                field: field.get(row).expect("FIELD_ID") as usize,
            }
        })
        .collect();
    (rows, correlations)
}

/// The direction of field `field` of [`session`].
pub fn field_direction(field: usize) -> [f64; 2] {
    [CENTRE_RAD, OFFSET_RAD][field]
}

/// Each row's prediction by the per-row reference, [`ModelPredictor`]: the
/// row's UVW rotated to the model's tangent point as `GridFT` rotates it,
/// the beam (when `beam`) centred on the row's field.
pub fn reference(
    rows: &[Row],
    model: &Path,
    correlations: &[CorrelationType],
    beam: Option<PrimaryBeam>,
) -> Vec<Vec<Complex32>> {
    let model = SkyModel::open(model).expect("model");
    let frequencies: Vec<f64> = (0..CHANNELS)
        .map(|channel| FREQUENCY_HZ + channel as f64 * CHANNEL_HZ)
        .collect();
    let options = PredictOptions {
        primary_beam: beam,
        ..PredictOptions::default()
    };
    let predictors = [CENTRE_RAD, OFFSET_RAD].map(|pointing| {
        ModelPredictor::new(&model, correlations, pointing, &options).expect("predictor")
    });
    let projections = [CENTRE_RAD, OFFSET_RAD]
        .map(|phase| GridFtProjection::between_j2000_directions(phase, model.centre_rad()));
    let mut workspace = PredictionWorkspace::default();
    let flags = vec![false; CHANNELS * correlations.len()];
    rows.iter()
        .map(|row| {
            let (uvw_m, phase_shift_m) = projections[row.field].apply(row.uvw_m);
            let mut out = vec![Complex32::new(9.0, 9.0); flags.len()];
            predictors[row.field]
                .predict_row(
                    &mut workspace,
                    &PredictRow {
                        uvw_m,
                        phase_shift_m,
                        frequencies_hz: &frequencies,
                        flags: &flags,
                        row_flag: row.flag_row,
                        antennas: row.antennas,
                    },
                    &mut out,
                )
                .expect("reference");
            out
        })
        .collect()
}

/// The bits of every sample, rows back to back.
pub fn bits<'a>(cells: impl IntoIterator<Item = &'a Vec<Complex32>>) -> Vec<(u32, u32)> {
    cells
        .into_iter()
        .flatten()
        .map(|value| (value.re.to_bits(), value.im.to_bits()))
        .collect()
}

/// DATA of every row.
pub fn data(rows: &[Row]) -> Vec<Vec<Complex32>> {
    rows.iter().map(|row| row.data.clone()).collect()
}
