// SPDX-License-Identifier: LGPL-3.0-or-later
//! Frequency-dependent additive Mueller continuum fits.

use std::collections::{BTreeMap, BTreeSet};

use casa_ms::MeasurementSet;

use super::grouping::{
    PriorCalibrationMode, SolveGroupOptions, SolveInputRows, build_solve_groups,
};
use super::{GainSolveError, GainSolveModelSource, GainSolveReport, GainSolveRequest, GainType};
use super::{RefAntSelector, SolvedGains};
use crate::CalibrationOutputMode;
use crate::session::prepare_calibration_solve;

pub(super) struct TableGrid {
    pub(super) terms: usize,
    pub(super) windows: BTreeMap<i32, Window>,
}

pub(super) struct Window {
    pub(super) frequency_domain: [f64; 2],
    pub(super) correlations: usize,
}

pub(crate) fn solve(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    fit_order: i32,
    mode: CalibrationOutputMode,
    legacy: bool,
) -> Result<GainSolveReport, GainSolveError> {
    if !legacy {
        return Err(GainSolveError::Baseline(
            "A Mueller nPar() called before being set.".into(),
        ));
    }
    let terms = usize::try_from(fit_order).map_err(|_| {
        GainSolveError::Baseline("The continuum fitter must first be initialized.".into())
    })? + 1;
    let context = prepare_calibration_solve(
        ms,
        &request.selection,
        &RefAntSelector::Unspecified,
        &request.prior_calibration_tables,
        request.parang,
        true,
        PriorCalibrationMode::Data,
    )?;
    let spectral = ms.spectral_window().map_err(ms_error)?;
    let mut frequencies = BTreeMap::new();
    for row in &context.rows {
        if let std::collections::btree_map::Entry::Vacant(entry) =
            frequencies.entry(row.data_desc_id)
        {
            entry.insert(
                spectral
                    .chan_freq(row.data_spw_id as usize)
                    .map_err(ms_error)?,
            );
        }
    }
    let groups = build_solve_groups(
        ms,
        &context.rows,
        context.preapplied_rows.as_ref(),
        &BTreeMap::new(),
        SolveGroupOptions {
            input_rows: SolveInputRows::LegacyAdditivePolynomial,
            gain_type: GainType::G,
            model_source: GainSolveModelSource::PointSource,
            point_stokes: [1., 0., 0., 0.],
            rotate_model: false,
            solve_interval: request.solve_interval,
            combine: request.combine,
            preaverage_max_span: None,
            correlation_dependent_flags: false,
            robust_seed: false,
            channelized: true,
            polynomial_bandpass: false,
        },
    )?;

    let mut solved = SolvedGains {
        rows: Vec::new(),
        refant_id: -1,
        attempted_spw_ids: BTreeSet::new(),
        channel_selections: BTreeMap::new(),
        selected_data: context.selected_data,
        solve_stats: Default::default(),
    };
    let mut grid = TableGrid {
        terms,
        windows: BTreeMap::new(),
    };
    for group in groups.into_values().filter(|group| group.attempted) {
        let gather = group.additive_polynomial.as_ref().expect("additive gather");
        let Some(&correlations) = group.baseline_correlation_counts.first() else {
            continue;
        };
        if group.baseline_correlation_counts.len() != 1 {
            return Err(GainSolveError::Baseline(
                "incompatible correlation counts for additive polynomial fitting".into(),
            ));
        }
        let window = grid.windows.entry(group.spw_id).or_insert_with(|| {
            let frequency_domain = gather
                .windows
                .iter()
                .flat_map(|(dd, window)| {
                    window
                        .channels
                        .iter()
                        .map(|&channel| frequencies[dd][channel])
                })
                .fold(
                    [f64::INFINITY, f64::NEG_INFINITY],
                    |[low, high], frequency| [low.min(frequency), high.max(frequency)],
                );
            Window {
                frequency_domain,
                correlations,
            }
        });
        if window.correlations != correlations {
            return Err(GainSolveError::Baseline(
                "incompatible additive coefficient shapes in one spectral window".into(),
            ));
        }
        solved.attempted_spw_ids.insert(group.spw_id);
        solved.rows.extend(gather.solutions(
            &group,
            &context.available_antennas,
            &frequencies,
            window.frequency_domain,
            terms,
            request.solve_mode,
        ));
    }
    if request.normalize_average_amplitude {
        super::baseline::normalize(&mut solved.rows, false, request.normalization_type);
    }
    super::writer::write_additive_polynomial_caltable(ms, request, &solved, mode, &grid)
}

fn ms_error(error: casa_ms::MsError) -> GainSolveError {
    GainSolveError::Baseline(error.to_string())
}
