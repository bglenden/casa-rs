// SPDX-License-Identifier: LGPL-3.0-or-later
//! Emit the exact casa-rs prepared source order and phase phasors for the
//! frozen four-SPW VLASS prediction-boundary comparison.
//!
//! This is a frontend-only diagnostic. It opens the MeasurementSet four times,
//! once per selected SPW, and does not enter weighting, CF loading, prediction,
//! gridding, FFT, deconvolution, restoration, or product formation.

use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;

use casa_imaging::phase_rotate_visibility;
use num_complex::Complex32;
use serde::Serialize;
use sha2::{Digest, Sha256};

const SELECTED_SPWS: [i32; 4] = [2, 7, 12, 17];

#[derive(Debug, Serialize)]
struct SourceSample {
    source_ordinal: usize,
    row_id: usize,
    ddid: usize,
    spw_id: usize,
    channel: usize,
    frequency_hz: f64,
    phase_shift_m: f64,
    phase_re_bits: u32,
    phase_im_bits: u32,
    collapsed_visibility_re_bits: u32,
    collapsed_visibility_im_bits: u32,
}

#[derive(Debug, Serialize)]
struct SourceTrace {
    kind: &'static str,
    role: &'static str,
    selected_spws: [i32; 4],
    samples: Vec<SourceSample>,
}

fn run() -> Result<(), String> {
    let mut args = env::args_os().skip(1);
    let output = args.next().map(PathBuf::from).ok_or_else(|| {
        "usage: vlass_prediction_boundary_source_trace OUTPUT --ms ...".to_string()
    })?;
    if output.exists() {
        return Err(format!(
            "refusing to overwrite source trace: {}",
            output.display()
        ));
    }
    let base_args = args.collect::<Vec<OsString>>();
    let base_config = casars_imager::CliConfig::parse(base_args)?;
    let mut samples = Vec::new();
    for spw in SELECTED_SPWS {
        let mut config = base_config.clone();
        config.spw = Some(spw);
        config.spw_selector = Some(spw.to_string());
        config.ddid = None;
        let trace = casars_imager::build_prepare_plane_trace_from_config(&config)?;
        for sample in trace.samples {
            if sample.spw_id != spw as usize {
                return Err(format!(
                    "SPW {spw} trace emitted source SPW {}",
                    sample.spw_id
                ));
            }
            if sample.correlation_indices != [0, 3] {
                return Err(format!(
                    "SPW {spw} row {} used correlations {:?}, expected [0, 3]",
                    sample.row_index, sample.correlation_indices
                ));
            }
            let contributions = sample.source_contributions.as_slice();
            let [contribution] = contributions else {
                return Err(format!(
                    "SPW {spw} row {} has {} source contributions, expected one",
                    sample.row_index,
                    contributions.len()
                ));
            };
            let phase = phase_rotate_visibility(
                Complex32::new(1.0, 0.0),
                sample.phase_shift_m,
                sample.output_frequency_hz,
            );
            samples.push(SourceSample {
                source_ordinal: samples.len(),
                row_id: sample.row_index,
                ddid: sample.ddid,
                spw_id: sample.spw_id,
                channel: contribution.source_channel_index,
                frequency_hz: sample.output_frequency_hz,
                phase_shift_m: sample.phase_shift_m,
                phase_re_bits: phase.re.to_bits(),
                phase_im_bits: phase.im.to_bits(),
                collapsed_visibility_re_bits: sample.visibility_re.to_bits(),
                collapsed_visibility_im_bits: sample.visibility_im.to_bits(),
            });
        }
    }
    let trace = SourceTrace {
        kind: "vlass_casars_prediction_boundary_source_trace",
        role: "frontend_only_correctness_trace_not_performance_evidence",
        selected_spws: SELECTED_SPWS,
        samples,
    };
    let payload =
        serde_json::to_vec(&trace).map_err(|error| format!("serialize source trace: {error}"))?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create {}: {error}", parent.display()))?;
    }
    fs::write(&output, &payload).map_err(|error| format!("write {}: {error}", output.display()))?;
    println!(
        "source_trace={} samples={} sha256={:x}",
        output.display(),
        trace.samples.len(),
        Sha256::digest(&payload),
    );
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}
