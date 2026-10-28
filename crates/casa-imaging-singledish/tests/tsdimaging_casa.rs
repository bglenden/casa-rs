// SPDX-License-Identifier: LGPL-3.0-or-later
//! casa-imaging-singledish against CASA 6.7.6's `tsdimaging` (#938 part 1,
//! #841 Phase 6c).
//!
//! `tests/data/tsdimaging_casa.json.gz` is the record of
//! `tools/singledish/tsdimaging_oracle.py`, run with CASA 6.7.6-14's Python.
//! CASA's `simobserve` made a total-power map of one ACA antenna, which the
//! oracle changed so that the hands, their weights and the flags all
//! matter. `tsdimaging` imaged it with each kernel, and the record holds:
//! - each row's values, flags and weights, and its pointing's pixel;
//! - CASA's images, masks and weight images;
//! - CASA's raster sampling and beams.
//!
//! The tests grid the rows with the crate and compare.

use std::f64::consts::PI;
use std::io::Read;
use std::sync::OnceLock;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use casa_imaging_runtime::Reservation;
use casa_imaging_runtime::pass::WorkerTeam;
use casa_imaging_singledish::{
    BeamSetup, FlagMerge, GridFunction, GridShape, KernelParameters, KernelTable, Pointing,
    SpectrumBlock, TotalPowerGrid, TotalPowerImage, beam_sampling, primary_beam_fwhm_arcsec,
    raster_sampling, theoretical_beam_arcsec, weight_mask_threshold, weights_from_rows,
};
use casa_numerics::PrimaryBeam;
use ndarray::{Array2, Array3};
use serde_json::Value;

const ARCSEC: f64 = PI / 180.0 / 3600.0;

fn record() -> &'static Value {
    static RECORD: OnceLock<Value> = OnceLock::new();
    RECORD.get_or_init(|| {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/data/tsdimaging_casa.json.gz"
        );
        let mut text = String::new();
        flate2::read::GzDecoder::new(std::fs::File::open(path).expect("record"))
            .read_to_string(&mut text)
            .expect("gzip");
        serde_json::from_str(&text).expect("json")
    })
}

fn bytes(value: &Value) -> Vec<u8> {
    STANDARD
        .decode(value.as_str().expect("base64"))
        .expect("base64")
}

fn floats(value: &Value) -> Vec<f32> {
    bytes(value)
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| f32::from_le_bytes(*b))
        .collect()
}

fn booleans(value: &Value) -> Vec<bool> {
    bytes(value).into_iter().map(|b| b != 0).collect()
}

fn number(value: &Value) -> f64 {
    value.as_f64().expect("number")
}

/// The MeasurementSet rows the oracle recorded.
struct Rows {
    pixels: Vec<Option<[f64; 2]>>,
    values: Array3<f32>,
    flags: Array3<bool>,
    row_flags: Vec<bool>,
    weight: Array2<f32>,
    pointings: Vec<Pointing>,
}

impl Rows {
    fn of(name: &str) -> Self {
        let rows = &record()["rows"][name];
        let count = rows["rows"].as_u64().expect("rows") as usize;
        let channels = rows["channels"].as_u64().expect("channels") as usize;
        let correlations = rows["correlations"].as_u64().expect("correlations") as usize;
        let shape = (count, channels, correlations);
        let pair = |value: &Value| [number(&value[0]), number(&value[1])];
        let direction: Vec<[f64; 2]> = rows["direction"]
            .as_array()
            .expect("directions")
            .iter()
            .map(pair)
            .collect();
        Self {
            pixels: rows["pixel"]
                .as_array()
                .expect("pixels")
                .iter()
                .map(|p| Some(pair(p)))
                .collect(),
            values: Array3::from_shape_vec(shape, floats(&rows["values"])).expect("values"),
            flags: Array3::from_shape_vec(shape, booleans(&rows["flags"])).expect("flags"),
            row_flags: rows["flag_row"]
                .as_array()
                .expect("row flags")
                .iter()
                .map(|f| f.as_bool().expect("flag"))
                .collect(),
            weight: Array2::from_shape_vec((count, correlations), floats(&rows["weight"]))
                .expect("weight"),
            pointings: rows["time"]
                .as_array()
                .expect("times")
                .iter()
                .zip(direction)
                .map(|(time, direction_rad)| Pointing {
                    time_s: number(time),
                    direction_rad,
                })
                .collect(),
        }
    }
}

fn cell_arcsec() -> f64 {
    number(&record()["cell_arcsec"])
}

/// A kernel parameter in pixels: an angle in arcsec over the cell, or -1.
fn pixels_of(params: &Value, name: &str) -> f64 {
    params.get(name).map_or(-1.0, |value| {
        let text = value.as_str().expect("angle");
        let arcsec: f64 = text.trim_end_matches("arcsec").parse().expect("arcsec");
        arcsec / cell_arcsec()
    })
}

/// The case's kernel, as `createSDFTMachine` and `SDGrid::init` form it.
fn kernel(params: &Value) -> (GridFunction, KernelTable) {
    let name = params["gridfunction"].as_str().expect("gridfunction");
    let parameters = KernelParameters {
        convsupport: params
            .get("convsupport")
            .map_or(-1, |s| s.as_i64().expect("support")),
        truncate: pixels_of(params, "truncate"),
        gwidth: pixels_of(params, "gwidth"),
        jwidth: pixels_of(params, "jwidth"),
    };
    let function = GridFunction::from_casa(name, parameters).expect("kernel");
    let table = match function {
        GridFunction::PrimaryBeam => {
            let telescope = record()["telescope"].as_str().expect("telescope");
            let frequency = number(&record()["image_frequencies_hz"][0]);
            let beam = PrimaryBeam::for_telescope(telescope, frequency).expect("pattern");
            KernelTable::primary_beam(&beam.pattern(), -cell_arcsec() * ARCSEC, frequency)
        }
        function => KernelTable::new(function).expect("table"),
    };
    (function, table)
}

/// The image's shape, `[x, y, plane, channel]`.
fn shape() -> GridShape {
    let imsize = &record()["imsize"];
    let channels = record()["image_frequencies_hz"]
        .as_array()
        .expect("channels")
        .len();
    GridShape {
        nx: imsize[0].as_u64().expect("nx") as usize,
        ny: imsize[1].as_u64().expect("ny") as usize,
        planes: 1,
        channels,
    }
}

/// The rows gridded with `table`, as `tsdimaging` grids them: both hands
/// into Stokes I, each data channel into the image channel of its
/// frequency.
fn grid(rows: &Rows, table: KernelTable) -> TotalPowerImage {
    let frequencies = record()["image_frequencies_hz"].as_array().expect("image");
    assert_eq!(
        frequencies,
        record()["data_frequencies_hz"].as_array().expect("data")
    );
    let (_, channels, _) = rows.values.dim();
    let weights = weights_from_rows(rows.weight.view(), channels).expect("weights");
    let channel_map: Vec<Option<usize>> = (0..channels).map(Some).collect();
    let block = SpectrumBlock {
        pixels: &rows.pixels,
        values: rows.values.view(),
        flags: rows.flags.view(),
        row_flags: &rows.row_flags,
        weights: weights.view(),
        channel_map: &channel_map,
        correlation_map: &[Some(0), Some(0)],
    };
    let mut grid = TotalPowerGrid::new(
        shape(),
        table,
        FlagMerge::AnyCorrelation,
        Reservation::none(),
    )
    .expect("grid");
    grid.grid(&block, &WorkerTeam::new(2).expect("team"))
        .expect("gridded");
    grid.finish()
}

/// The largest difference relative to the largest magnitude.
fn largest_difference(actual: &[f32], expected: &[f32]) -> f64 {
    assert_eq!(actual.len(), expected.len());
    let peak = expected
        .iter()
        .fold(0.0_f64, |m, v| m.max(f64::from(v.abs())));
    let difference = actual
        .iter()
        .zip(expected)
        .fold(0.0_f64, |m, (a, e)| m.max(f64::from((a - e).abs())));
    difference / peak
}

fn cases() -> impl Iterator<Item = &'static Value> {
    record()["cases"].as_array().expect("cases").iter()
}

#[test]
fn images_and_weights_match_casas() {
    let rows = Rows::of("map");
    for case in cases().filter(|case| case["ms"] == "map") {
        let name = case["name"].as_str().expect("name");
        let (_, table) = kernel(&case["params"]);
        let gridded = grid(&rows, table);
        let weight = largest_difference(
            gridded.weight.as_slice_memory_order().expect("weight"),
            &floats(&case["weight"]["pixels"]),
        );
        let image = largest_difference(
            gridded.image.as_slice_memory_order().expect("image"),
            &floats(&case["image"]["pixels"]),
        );
        // The masks from casa-rs's own weights.
        let ours = gridded.weight.as_slice_memory_order().expect("weight");
        let threshold = weight_mask_threshold(ours, 0.1).expect("weights");
        let differ = ours
            .iter()
            .zip(booleans(&case["image"]["mask"]))
            .filter(|(w, casa)| (f64::from(**w) > threshold) != *casa)
            .count();
        eprintln!(
            "{name}: weight {weight:.2e}, image {image:.2e} of the peak, {differ} mask differences"
        );
        // Measured: at most 6.6e-7 and 7.6e-7, BOX exactly; single
        // precision summed in a different row order.
        assert!(weight <= 2.0e-6, "{name}: weight {weight:e}");
        assert!(image <= 2.0e-6, "{name}: image {image:e}");
        assert_eq!(differ, 0, "{name}");
    }
}

#[test]
fn masks_follow_the_median_weight() {
    for case in cases().filter(|case| case["ms"] == "map") {
        let name = case["name"].as_str().expect("name");
        let weight = floats(&case["weight"]["pixels"]);
        let threshold = weight_mask_threshold(&weight, 0.1).expect("weights");
        let mask: Vec<bool> = weight.iter().map(|&w| f64::from(w) > threshold).collect();
        let casa = booleans(&case["image"]["mask"]);
        let differ = mask.iter().zip(&casa).filter(|(a, b)| a != b).count();
        eprintln!("{name}: threshold {threshold:e}, {differ} mask differences");
        assert_eq!(differ, 0, "{name}");
    }
}

#[test]
fn the_raster_sampling_matches_casas() {
    for name in ["map", "gapped"] {
        let rows = Rows::of(name);
        let sampling = raster_sampling(&rows.pointings).expect("sampling");
        let casa = &record()["sampling"][name];
        let interval = sampling.interval_rad.map(|v| v / ARCSEC);
        eprintln!(
            "{name}: {interval:?} against CASA's {}",
            casa["interval_arcsec"]
        );
        for (actual, expected) in interval
            .iter()
            .zip(casa["interval_arcsec"].as_array().expect("i"))
        {
            assert!(
                (actual - number(expected)).abs() <= 1.0e-9 * actual.max(1.0),
                "{name}"
            );
        }
        assert!(
            (sampling.position_angle_rad.to_degrees() - number(&casa["angle_deg"])).abs() < 1.0e-9
        );
    }
}

#[test]
fn the_beams_parts_match_scipys() {
    let parts = &record()["beam_parts"];
    for pair in parts["central_obstruction"].as_array().expect("factors") {
        let (epsilon, factor) = (number(&pair[0]), number(&pair[1]));
        // The factor relative to a 1 m dish with no taper is the ratio of
        // two primary beams.
        let ratio = primary_beam_fwhm_arcsec(1.0e11, 1.0, epsilon, 10.0)
            / primary_beam_fwhm_arcsec(1.0e11, 1.0, 0.0, 10.0);
        assert!(
            (ratio - factor).abs() < 1.0e-12,
            "{epsilon}: {ratio} against {factor}"
        );
    }
    for row in parts["primary_beam_arcsec"].as_array().expect("beams") {
        let fwhm =
            primary_beam_fwhm_arcsec(number(&row[0]), number(&row[1]), number(&row[2]), 10.0);
        assert!((fwhm - number(&row[3])).abs() < 1.0e-12 * fwhm, "{row}");
    }
}

/// The kernel of an `sdbeamutil` setup, at CASA's defaults.
fn beam_kernel(setup: &Value) -> (GridFunction, KernelTable) {
    let parameters = KernelParameters {
        convsupport: setup["convsupport"].as_i64().expect("support"),
        ..KernelParameters::DEFAULT
    };
    let function =
        GridFunction::from_casa(setup["gridfunction"].as_str().expect("name"), parameters)
            .expect("kernel");
    (function, KernelTable::new(function).expect("table"))
}

/// How far casa-rs's beam may lie from CASA's, relative, for a kernel:
/// `BOX`, which both model alike, to rounding; the others by the
/// departure of `docs/CASA (C++) bugs.md` ("tsdimaging's theoretical
/// beam"), measured on these setups as below.
fn beam_tolerance(function: GridFunction) -> std::ops::RangeInclusive<f64> {
    match function {
        GridFunction::Box { .. } => -1.0e-7..=1.0e-7,
        // The gridder's spheroid, read at the truncated radius, against
        // SciPy's of c = 5.356π/2: +1.2e-4 to +6.0e-4, of which the
        // truncated radius is about 40%.
        GridFunction::Spheroidal { .. } => 0.0..=1.0e-3,
        // CASA takes the Gaussian's half width as its full width, and cuts
        // it at 1.5 gwidth, not at `truncate`: +0.60% to +3.3%.
        GridFunction::Gauss { .. } => 5.0e-3..=4.0e-2,
        // The table read at the truncated radius: +5e-5 to +1.2e-4 at the
        // defaults (at the rounded radius it agrees to 1e-6); CASA ignores
        // `truncate`: −0.31% where it is given.
        GridFunction::Gjinc { .. } => -4.0e-3..=3.0e-4,
        // The telescope's pattern against an Airy disk scaled to Baars'
        // width: −4.5e-4.
        GridFunction::PrimaryBeam => -1.0e-3..=0.0,
    }
}

#[test]
fn theoretical_beams_against_casas() {
    for setup in record()["theoretical_beams"].as_array().expect("beams") {
        let (function, table) = beam_kernel(setup);
        let pair = |value: &Value| [number(&value[0]), number(&value[1])];
        let sampling: Vec<f64> = setup["sampling"]
            .as_array()
            .expect("s")
            .iter()
            .map(number)
            .collect();
        let beam = theoretical_beam_arcsec(&BeamSetup {
            diameter_m: number(&setup["diameter"]),
            obscuration_m: number(&setup["blockage"]),
            frequency_hz: number(&setup["frequency_hz"]),
            cell_arcsec: pair(&setup["cell"]),
            kernel: (function, &table),
            sampling_arcsec: &sampling,
        });
        let casa = number(&setup["beam_arcsec"]);
        let relative = beam / casa - 1.0;
        eprintln!(
            "{} {} cell {} sampling {:?} D {} blockage {}: {beam:.6} against CASA's {casa:.6} ({relative:+.2e})",
            setup["gridfunction"],
            setup["convsupport"],
            setup["cell"],
            sampling,
            setup["diameter"],
            setup["blockage"]
        );
        assert!(
            beam_tolerance(function).contains(&relative),
            "{setup}: {relative:e}"
        );
    }
}

#[test]
fn image_beams_against_casas() {
    let antenna = &record()["antenna"];
    for case in cases() {
        let name = case["name"].as_str().expect("name");
        let rows = Rows::of(case["ms"].as_str().expect("ms"));
        let sampling = raster_sampling(&rows.pointings).expect("sampling");
        let chosen = beam_sampling(sampling.interval_rad.map(|v| v / ARCSEC)).expect("valid");
        let (function, table) = kernel(&case["params"]);
        let beam = theoretical_beam_arcsec(&BeamSetup {
            diameter_m: number(&antenna["diameter_m"]),
            obscuration_m: 0.0,
            frequency_hz: number(&record()["restfreq_hz"]),
            cell_arcsec: [cell_arcsec(); 2],
            kernel: (function, &table),
            sampling_arcsec: &chosen,
        });
        let casa = number(&case["image"]["beam_arcsec"]);
        let relative = beam / casa - 1.0;
        eprintln!("{name}: {beam:.6} against CASA's {casa:.6} ({relative:+.2e})");
        assert!(
            beam_tolerance(function).contains(&relative),
            "{name}: {relative:e}"
        );
    }
}
