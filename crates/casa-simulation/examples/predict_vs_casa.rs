// SPDX-License-Identifier: LGPL-3.0-or-later
//! Simulation Phase 1a (#841): compare casa-rs prediction with CASA's
//! `sm.predict`, sample by sample, on a MeasurementSet CASA made.
//!
//! ```text
//! cargo run --release -p casa-simulation --example predict_vs_casa -- \
//!     MS MODEL_IMAGE [--vp alma|vla|vla-nvss|evla] [--padding 1.3 | --wproject PLANES] [--f64]
//! ```
//!
//! `--wproject` predicts through `WProjectFT` with `PLANES` planes, as after
//! `sm.setoptions(ftmachine='wproject', wprojplanes=PLANES)`.
//!
//! `tools/simulation/casa_sm_predict_oracle.py` writes the MeasurementSets
//! and model images. Each row is rotated to the model's tangent point as
//! CASA's `GridFT` rotates it (always, even when the tangent point is the
//! field's phase centre). The
//! program prints one JSON object: the normalized RMS of casa-rs against
//! CASA's DATA, overall and per correlation, the largest difference relative
//! to CASA's peak, and the prediction time.

use std::process::ExitCode;
use std::time::Instant;

use casa_imaging_model::GridFtProjection;
use casa_imaging_operator::{GridPrecision, PaddingFactor};
use casa_ms::MeasurementSet;
use casa_ms::columns::data_columns::DataColumn;
use casa_ms::columns::flag_columns::{FlagColumn, FlagRowColumn};
use casa_ms::columns::main_ids::{antenna1, antenna2};
use casa_ms::columns::uvw_column::UvwColumn;
use casa_simulation::predict::{
    FtMachine, ModelPredictor, PredictOptions, PredictRow, PredictionWorkspace, PrimaryBeam,
    SkyModel,
};
use casa_types::ArrayValue;
use casa_types::Correlation;
use ndarray::IxDyn;
use num_complex::Complex32;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("predict_vs_casa: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [ms_path, model_path] = [args.first(), args.get(1)].map(|value| {
        value.cloned().ok_or(
            "usage: predict_vs_casa MS MODEL_IMAGE [--vp alma|vla|vla-nvss|evla] [--padding P | \
                 --wproject N] \
                 [--f64]",
        )
    });
    let (ms_path, model_path) = (ms_path?, model_path?);
    let mut options = PredictOptions::default();
    let mut flags = args.iter().skip(2);
    while let Some(flag) = flags.next() {
        match flag.as_str() {
            "--vp" => {
                options.primary_beam = match flags.next().map(String::as_str) {
                    Some("alma") => Some(PrimaryBeam::ALMA),
                    Some("vla") => Some(PrimaryBeam::VLA),
                    Some("vla-nvss") => Some(PrimaryBeam::VLA_NVSS),
                    Some("evla") => Some(PrimaryBeam::Evla),
                    other => return Err(format!("unknown voltage pattern {other:?}").into()),
                };
            }
            "--padding" => {
                let factor: f32 = flags.next().ok_or("--padding needs a value")?.parse()?;
                options.ft_machine = FtMachine::Grid(PaddingFactor::new(factor)?);
            }
            "--wproject" => {
                let planes = flags
                    .next()
                    .ok_or("--wproject needs a plane count")?
                    .parse()?;
                options.ft_machine = FtMachine::WProject(planes);
            }
            "--f64" => options.precision = GridPrecision::F64,
            other => return Err(format!("unknown option {other}").into()),
        }
    }

    let ms = MeasurementSet::open(&ms_path)?;
    let frequencies_hz = ms.spectral_window()?.chan_freq(0)?;
    let correlations = ms
        .polarization()?
        .corr_type(0)?
        .into_iter()
        .map(|code| Correlation::from_casacore_code(code).ok_or(format!("correlation code {code}")))
        .collect::<Result<Vec<_>, _>>()?;
    let phase_centre_rad = match ms.field()?.phase_dir(0)? {
        ArrayValue::Float64(direction) => [direction[IxDyn(&[0, 0])], direction[IxDyn(&[1, 0])]],
        other => return Err(format!("unexpected PHASE_DIR {other:?}").into()),
    };

    let model = SkyModel::open(&model_path)?;
    let projection =
        GridFtProjection::between_j2000_directions(phase_centre_rad, model.centre_rad());

    let started = Instant::now();
    let predictor = ModelPredictor::new(&model, &correlations, phase_centre_rad, &options)?;
    let prepare_s = started.elapsed().as_secs_f64();

    let main = ms.main_table();
    let uvw = UvwColumn::new(main);
    let data = DataColumn::data(main)?;
    let flag = FlagColumn::new(main);
    let flag_row = FlagRowColumn::new(main);
    let (first, second) = (antenna1(main), antenna2(main));
    let npol = correlations.len();
    let channels = frequencies_hz.len();
    let mut workspace = PredictionWorkspace::default();
    let mut predicted = vec![Complex32::new(0.0, 0.0); channels * npol];
    let mut sample_flags = vec![false; channels * npol];
    let mut stats = Stats::new(npol);
    let mut predict_s = 0.0;
    for row in 0..ms.row_count() {
        let ArrayValue::Bool(row_flags) = flag.get(row)? else {
            return Err("FLAG is not boolean".into());
        };
        for channel in 0..channels {
            for pol in 0..npol {
                sample_flags[channel * npol + pol] = row_flags[IxDyn(&[pol, channel])];
            }
        }
        let started = Instant::now();
        let (uvw_m, phase_shift_m) = projection.apply(uvw.get(row)?);
        predictor.predict_row(
            &mut workspace,
            &PredictRow {
                uvw_m,
                phase_shift_m,
                frequencies_hz: &frequencies_hz,
                flags: &sample_flags,
                row_flag: flag_row.get(row)?,
                antennas: [
                    u32::try_from(first.get(row)?)?,
                    u32::try_from(second.get(row)?)?,
                ],
            },
            &mut predicted,
        )?;
        predict_s += started.elapsed().as_secs_f64();
        let ArrayValue::Complex32(casa) = data.get(row)? else {
            return Err("DATA is not Complex32".into());
        };
        for channel in 0..channels {
            for pol in 0..npol {
                stats.add(
                    pol,
                    predicted[channel * npol + pol],
                    casa[IxDyn(&[pol, channel])],
                );
            }
        }
    }
    println!(
        "{}",
        stats.report(ms.row_count(), prepare_s, predict_s, &options)
    );
    Ok(())
}

/// Running sums for the normalized RMS of casa-rs against CASA.
struct Stats {
    difference: Vec<f64>,
    reference: Vec<f64>,
    largest_difference: f64,
    largest_reference: f64,
}

impl Stats {
    fn new(npol: usize) -> Self {
        Self {
            difference: vec![0.0; npol],
            reference: vec![0.0; npol],
            largest_difference: 0.0,
            largest_reference: 0.0,
        }
    }

    fn add(&mut self, pol: usize, ours: Complex32, casa: Complex32) {
        let difference = f64::from((ours - casa).norm());
        let reference = f64::from(casa.norm());
        self.difference[pol] += difference * difference;
        self.reference[pol] += reference * reference;
        self.largest_difference = self.largest_difference.max(difference);
        self.largest_reference = self.largest_reference.max(reference);
    }

    fn report(
        &self,
        rows: usize,
        prepare_s: f64,
        predict_s: f64,
        options: &PredictOptions,
    ) -> String {
        let nrms = |difference: f64, reference: f64| (difference / reference).sqrt();
        let per_pol: Vec<String> = self
            .difference
            .iter()
            .zip(&self.reference)
            .map(|(difference, reference)| format!("{:.3e}", nrms(*difference, *reference)))
            .collect();
        format!(
            "{{\"rows\": {rows}, \"ft_machine\": \"{:?}\", \"primary_beam\": {}, \"precision\": \"{:?}\", \
             \"normalized_rms\": {:.3e}, \"per_correlation_normalized_rms\": [{}], \
             \"max_difference_over_peak\": {:.3e}, \"prepare_s\": {prepare_s:.3}, \"predict_s\": {predict_s:.3}}}",
            options.ft_machine,
            options.primary_beam.is_some(),
            options.precision,
            nrms(self.difference.iter().sum(), self.reference.iter().sum()),
            per_pol.join(", "),
            self.largest_difference / self.largest_reference,
        )
    }
}
