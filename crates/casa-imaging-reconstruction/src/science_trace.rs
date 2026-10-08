// SPDX-License-Identifier: LGPL-3.0-or-later

//! Opt-in imaging-science boundary digests for parity investigation.

use std::sync::OnceLock;

use crate::{Encoder, canonical_f64_bits};

pub(crate) fn imaging_science_trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var_os("CASA_RS_TRACE_IMAGING_SCIENCE").is_some()
            || std::env::var_os("CASA_RS_IMAGING_SCIENCE_PROBE").is_some()
    })
}

pub(crate) struct ScienceTraceDigest {
    encoder: Encoder,
    count: u64,
    peak_abs: f64,
    sum_re: f64,
    sum_im: f64,
}

impl ScienceTraceDigest {
    pub(crate) fn new() -> Self {
        let encoder = Encoder::new(b"casa-rs-t41-science-trace", 1);
        Self {
            encoder,
            count: 0,
            peak_abs: 0.0,
            sum_re: 0.0,
            sum_im: 0.0,
        }
    }

    pub(crate) fn push_real(&mut self, value: f64) {
        self.encoder.u64(canonical_f64_bits(value));
        self.count += 1;
        self.peak_abs = self.peak_abs.max(value.abs());
        self.sum_re += value;
    }

    pub(crate) fn push_indexed_real(&mut self, index: usize, value: f64) {
        self.encoder.usize(index);
        self.push_real(value);
    }

    pub(crate) fn emit(self, label: &'static str) {
        let checksum = self.encoder.finish();
        let checksum = checksum
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        eprintln!(
            "imaging_science_trace boundary={label} count={} peak_abs={:.17e} sum_re={:.17e} sum_im={:.17e} checksum={checksum}",
            self.count, self.peak_abs, self.sum_re, self.sum_im,
        );
    }
}

pub(crate) fn trace_real_values(label: &'static str, values: &[f64]) {
    if !imaging_science_trace_enabled() {
        return;
    }
    let mut digest = ScienceTraceDigest::new();
    for value in values {
        digest.push_real(*value);
    }
    digest.emit(label);
}
