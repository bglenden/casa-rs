// SPDX-License-Identifier: LGPL-3.0-or-later
//! Frequency-dependent additive Mueller continuum fits.

use std::collections::{BTreeMap, BTreeSet};

use casa_ms::MeasurementSet;

use super::grouping::{
    PriorCalibrationMode, SelectedSolveRow, SolveGroupOptions, SolveInputRows, build_solve_groups,
    selected_channels,
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

struct SelectedFrequencies {
    by_description: BTreeMap<i32, Vec<f64>>,
    by_window: BTreeMap<i32, [f64; 2]>,
}

impl SelectedFrequencies {
    fn read(ms: &MeasurementSet, rows: &[SelectedSolveRow]) -> Result<Self, GainSolveError> {
        use std::collections::btree_map::Entry;

        let table = ms.spectral_window().map_err(ms_error)?;
        let mut by_description = BTreeMap::new();
        let mut by_window = BTreeMap::new();
        for row in rows {
            if let Entry::Vacant(entry) = by_description.entry(row.data_desc_id) {
                let id = row.data_spw_id as usize;
                let frequencies = table.chan_freq(id).map_err(ms_error)?;
                if usize::try_from(table.num_chan(id).map_err(ms_error)?).ok()
                    != Some(frequencies.len())
                {
                    return Err(frequency_shape(row.data_spw_id, frequencies.len()));
                }
                if let Entry::Vacant(domain) = by_window.entry(row.data_spw_id) {
                    let selected =
                        selected_channels(row.channel_selection.as_ref(), frequencies.len())?;
                    if selected.is_empty() {
                        return Err(frequency_shape(row.data_spw_id, frequencies.len()));
                    }
                    domain.insert(bounds(selected.iter().map(|&channel| frequencies[channel])));
                }
                entry.insert(frequencies);
            }
        }
        Ok(Self {
            by_description,
            by_window,
        })
    }
}

fn bounds(frequencies: impl Iterator<Item = f64>) -> [f64; 2] {
    frequencies.fold(
        [f64::INFINITY, f64::NEG_INFINITY],
        |[low, high], frequency| [low.min(frequency), high.max(frequency)],
    )
}

fn frequency_shape(window: i32, channels: usize) -> GainSolveError {
    GainSolveError::UnsupportedParameterShape {
        path: format!("SPECTRAL_WINDOW {window} additive fit frequency grid"),
        shape: vec![channels],
    }
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
    let frequencies = SelectedFrequencies::read(ms, &context.rows)?;
    // Keep the frequency basis fixed across all intervals, including those
    // where an entire input window has no usable measurements.
    let combined_domain = request
        .combine
        .spectral_windows
        .then(|| bounds(frequencies.by_window.values().flatten().copied()));
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
        let window = grid.windows.entry(group.spw_id).or_insert_with(|| Window {
            frequency_domain: combined_domain
                .unwrap_or_else(|| frequencies.by_window[&group.spw_id]),
            correlations,
        });
        for (&dd, layout) in &gather.windows {
            let axis = &frequencies.by_description[&dd];
            if layout.channels.iter().any(|&channel| channel >= axis.len()) {
                return Err(frequency_shape(group.spw_id, axis.len()));
            }
        }
        if window.correlations != correlations {
            return Err(GainSolveError::Baseline(
                "incompatible additive coefficient shapes in one spectral window".into(),
            ));
        }
        solved.attempted_spw_ids.insert(group.spw_id);
        solved.rows.extend(gather.solutions(
            &group,
            &context.available_antennas,
            &frequencies.by_description,
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
