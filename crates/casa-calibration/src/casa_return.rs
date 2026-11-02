// SPDX-License-Identifier: LGPL-3.0-or-later
//! What CASA 6.7.6.14's calibration tasks return (#931), built from
//! casa-rs's reports. `tools/casa-compat/record_casa_returns.py` records the
//! structure of CASA's own returns, which the task tests compare these with.
//!
//! - `gaincal` and `bandpass` return the tables they applied and solved, the
//!   data they selected and how far their solution intervals got
//!   (`solvestats`, [`SolveStats`]).
//! - `fluxscale` returns its flux densities per field and spectral window,
//!   with the fit across windows.
//! - `applycal` and `gencal` return `None`.

use std::path::Path;

use casa_provider_contracts::{CasaFloat, CasaValue, FloatDtype};

use crate::{
    ApplyCalibrationTableSpec, FluxScaleFieldResult, FluxScaleReport, SelectedData, SolveCounts,
    SolveStats,
};

/// `gaincal`'s and `bandpass`'s return: the tables applied while solving,
/// sorted as CASA lists them, the data selected, the table solved, and the
/// solve's interval counts.
pub fn solve_record(
    applied: &[ApplyCalibrationTableSpec],
    selected: &SelectedData,
    solved: &Path,
    stats: &SolveStats,
) -> CasaValue {
    let mut apply_tables = applied
        .iter()
        .map(|spec| spec.path.display().to_string())
        .collect::<Vec<_>>();
    apply_tables.sort();
    apply_tables.dedup();
    let ids = |ids: &[i32]| CasaValue::int64_vector(ids.iter().map(|&id| i64::from(id)));
    CasaValue::dict([
        ("apply_tables", CasaValue::str_vector(apply_tables)),
        (
            "selectvis",
            CasaValue::dict([
                ("antennas", ids(&selected.antennas)),
                ("field", ids(&selected.fields)),
                ("intents", CasaValue::str_vector(selected.intents.clone())),
                ("observation", ids(&selected.observations)),
                ("scan", ids(&selected.scans)),
                ("spw", ids(&selected.spectral_windows)),
            ]),
        ),
        (
            "solve_tables",
            CasaValue::str_vector([solved.display().to_string()]),
        ),
        ("solvestats", solvestats(stats)),
    ])
}

/// CASA's `solvestats`: the counts summed over the windows, then each
/// window's as `spw<id>`, each with its antennas' as `ant<id>`. CASA sorts
/// every dictionary's keys as text, so `ant10` comes before `ant2`.
fn solvestats(stats: &SolveStats) -> CasaValue {
    let windows = stats.windows.iter().map(|window| {
        let antennas = window.antennas.iter().map(|antenna| {
            let mut entries = counts(&antenna.counts);
            entries.push((
                "used_as_refant".to_string(),
                CasaValue::int64_vector([antenna.used_as_refant as i64]),
            ));
            (format!("ant{}", antenna.antenna_id), sorted(entries))
        });
        let mut entries = counts(&window.counts);
        entries.extend(antennas);
        (format!("spw{}", window.spw_id), sorted(entries))
    });
    let mut entries = counts(&stats.total().unwrap_or_default());
    entries.extend(windows);
    sorted(entries)
}

/// One stage's counts, as CASA names them.
fn counts(counts: &SolveCounts) -> Vec<(String, CasaValue)> {
    let vector = |values: &[u64]| CasaValue::int64_vector(values.iter().map(|&v| v as i64));
    vec![
        (
            "above_minblperant".to_string(),
            vector(&counts.above_minblperant),
        ),
        ("above_minsnr".to_string(), vector(&counts.above_minsnr)),
        ("data_unflagged".to_string(), vector(&counts.data_unflagged)),
        ("expected".to_string(), vector(&counts.expected)),
    ]
}

/// `entries` as a dictionary sorted by key.
fn sorted(mut entries: Vec<(String, CasaValue)>) -> CasaValue {
    entries.sort_by(|(left, _), (right, _)| left.cmp(right));
    CasaValue::dict(entries)
}

/// `fluxscale`'s return: each transfer field's flux densities, keyed by its
/// id, then the windows' frequencies, ids and names.
pub fn fluxscale_record(report: &FluxScaleReport) -> CasaValue {
    let fields = report
        .fields
        .iter()
        .map(|(field_id, field)| (field_id.to_string(), field_record(field)));
    let windows = [
        (
            "freq".to_string(),
            CasaValue::float64_vector(report.frequencies_hz.iter().copied()),
        ),
        (
            "spwID".to_string(),
            CasaValue::int64_vector(report.spw_ids.iter().map(|&id| i64::from(id))),
        ),
        (
            "spwName".to_string(),
            CasaValue::str_vector(report.spw_names.clone()),
        ),
    ];
    CasaValue::dict(fields.chain(windows))
}

/// One field's flux densities per window, keyed by the window's id, and
/// its fit across windows.
fn field_record(field: &FluxScaleFieldResult) -> CasaValue {
    let windows = field.spw_results.iter().map(|(spw_id, result)| {
        (
            spw_id.to_string(),
            CasaValue::dict([
                ("fluxd", CasaValue::float64_vector(result.fluxd)),
                ("fluxdErr", CasaValue::float64_vector(result.fluxd_err)),
                ("numSol", CasaValue::float64_vector(result.num_sol)),
            ]),
        )
    });
    let fit = [
        ("covarMat", matrix(&field.covar_mat)),
        ("fieldName", CasaValue::string(field.field_name.clone())),
        ("fitFluxd", CasaValue::float(field.fit_fluxd)),
        ("fitFluxdErr", CasaValue::float(field.fit_fluxd_err)),
        ("fitRefFreq", CasaValue::float(field.fit_ref_frequency_hz)),
        (
            "spidx",
            CasaValue::float64_vector(field.spidx.iter().copied()),
        ),
        (
            "spidxerr",
            CasaValue::float64_vector(field.spidx_err.iter().copied()),
        ),
    ]
    .map(|(key, value)| (key.to_string(), value));
    CasaValue::dict(windows.chain(fit))
}

/// `rows` as a two-dimensional `float64` array; none is CASA's `(0, 0)`.
fn matrix(rows: &[Vec<f64>]) -> CasaValue {
    let columns = rows.first().map_or(0, Vec::len);
    CasaValue::FloatArray {
        dtype: FloatDtype::Float64,
        shape: vec![rows.len(), columns],
        data: rows.iter().flatten().copied().map(CasaFloat).collect(),
    }
}
