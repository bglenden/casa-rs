// SPDX-License-Identifier: LGPL-3.0-or-later
//! The direct tool's display of its retained visibility equation.

use std::path::Path;

use casa_ms::MeasurementSet;

use super::{CalibraterLifecycle, ConfiguredSolve, VisibilityEquation};
use crate::execute::viscal_order;
use crate::solve::grouping::resolve_refants;
use crate::{
    BandpassType, CalibrationOutputMode, CalibrationSolveRequest, GainNormalization,
    GainSolveInterval, GainSolveRequest, RefAntSelector,
};

impl CalibraterLifecycle {
    /// Describe the currently configured apply and solve terms in CASA order.
    /// A detached tool returns `None`.
    #[must_use]
    pub fn state_lines(&self) -> Option<Vec<String>> {
        let equation = self.equation.as_ref()?;
        let apply = describe_apply(equation);
        let mut lines = vec!["The following calibration terms are arranged for apply:".into()];
        if apply.is_empty() {
            lines.push(".   (None)".into());
        } else {
            lines.extend(apply.into_iter().map(|term| format!(".   {term}")));
        }
        lines.push("The following calibration term is arranged for solve:".into());
        let solve = equation.solve.as_ref().map(|solver| {
            equation.solve_description.clone().unwrap_or_else(|| {
                SolveState::of(solver).description(equation, "", "AP", "flex", None)
            })
        });
        lines.extend(
            format!(".   {}", solve.as_deref().unwrap_or("(None)"))
                .lines()
                .map(str::to_owned),
        );
        Some(lines)
    }

    /// Retain the original interval and mode spellings for the configured
    /// tool solve. The resolved numerical controls remain in the solver.
    pub fn record_solve_description(
        &mut self,
        interval: &str,
        apmode: &str,
        refant_mode: &str,
        reference: Option<&RefAntSelector>,
    ) {
        if let Some(equation) = &mut self.equation
            && let Some(solver) = &equation.solve
        {
            let gain = match solver {
                ConfiguredSolve::SingleDish(solver) => {
                    solver.gain_controls().map(|(radius, smooth)| {
                        let smooth = if smooth { "True" } else { "False" };
                        let default = if radius == 0. {
                            " (half of primary beam will be used)"
                        } else {
                            ""
                        };
                        format!(
                            "{}: {} smooth={smooth}\n radius={}{default}",
                            solver.subtype(),
                            solver.request.output_table.display(),
                            format_calibration_number(radius)
                        )
                    })
                }
                _ => None,
            };
            equation.solve_description = Some(gain.unwrap_or_else(|| {
                SolveState::of(solver).description(
                    equation,
                    interval,
                    apmode,
                    refant_mode,
                    reference,
                )
            }));
        }
    }
}

fn describe_apply(equation: &VisibilityEquation) -> Vec<String> {
    let apply = &equation.apply;
    let mut terms = Vec::new();
    for (index, table) in apply.tables.iter().enumerate() {
        if !apply
            .library_tables
            .iter()
            .any(|library| library.tables.contains(&index))
            && let Some(snapshot) = apply.table_snapshots.get(&table.path)
        {
            terms.push((
                (viscal_order(&snapshot.subtype), index, 1),
                snapshot.description.clone(),
            ));
        }
    }
    for library in &apply.library_tables {
        let calwt = apply
            .tables
            .get(library.tables.start)
            .is_some_and(|table| table.calwt);
        terms.push((
            (viscal_order(&library.subtype), library.tables.start, 1),
            format!(
                "{}: table={} (by cal library) calWt={calwt}",
                library.subtype, library.name
            ),
        ));
    }
    terms.extend(apply.tableless_opacity.iter().map(|term| {
        let opacity = term
            .values
            .iter()
            .copied()
            .map(format_calibration_number)
            .collect::<Vec<_>>();
        (
            (viscal_order(term.subtype), term.index, 0),
            format!(
                "{}: opacity=[{}] calWt=false",
                term.subtype,
                opacity.join(", ")
            ),
        )
    }));
    terms.extend((0..apply.parang_terms).map(|_| {
        (
            (viscal_order("P Jones"), usize::MAX, 1),
            "P Jones <pre-computed>".into(),
        )
    }));
    terms.sort_by_key(|(order, _)| *order);
    terms
        .into_iter()
        .map(|(_, description)| description)
        .collect()
}

pub(super) fn traditional_apply_description(
    subtype: &str,
    name: &str,
    interpolation: &str,
    spwmap: &[i32],
    calwt: bool,
) -> String {
    let (time, frequency) = interpolation.split_once(',').unwrap_or((interpolation, ""));
    let time = if time.is_empty() { "linear" } else { time };
    let interpolation = if matches!(
        subtype,
        "A Noise"
            | "B Jones"
            | "B TSYS"
            | "BPOLY"
            | "Df Jones"
            | "Dfgen Jones"
            | "DfLLS Jones"
            | "Xf Jones"
            | "Xfparang Jones"
            | "PosAng Jones"
            | "GlinXphf Jones"
            | "Mf Mueller"
            | "Tf Jones"
            | "TfOpac"
            | "Jf Jones"
    ) {
        format!(
            "{time},{}",
            if frequency.is_empty() {
                "linear"
            } else {
                frequency
            }
        )
    } else {
        time.into()
    };
    let calwt = calwt
        && !matches!(
            subtype,
            "D Jones"
                | "Df Jones"
                | "Dgen Jones"
                | "Dfgen Jones"
                | "DLLS Jones"
                | "DfLLS Jones"
                | "X Jones"
                | "Xf Jones"
                | "Xparang Jones"
                | "Xfparang Jones"
                | "X Mueller"
                | "GlinXph Jones"
                | "GlinXphf Jones"
                | "PosAng Jones"
        );
    format!(
        "{subtype}: table={name} select= interp={interpolation} spwmap={spwmap:?} calWt={calwt}"
    )
}

struct SolveState<'a> {
    subtype: &'static str,
    table: &'a Path,
    interval: GainSolveInterval,
    frequency_dependent: bool,
    append: bool,
    reference: Option<&'a RefAntSelector>,
    min_snr: f32,
    normalization: Option<GainNormalization>,
}

impl<'a> SolveState<'a> {
    fn of(solver: &'a ConfiguredSolve) -> Self {
        match solver {
            ConfiguredSolve::Task {
                request,
                mode,
                bandpass,
                tf,
                ..
            } => {
                let append = *mode == CalibrationOutputMode::Append;
                match request.as_ref() {
                    CalibrationSolveRequest::Gain(gain) if tf.is_some() => Self {
                        frequency_dependent: true,
                        ..Self::gain(gain, "Tf Jones", append)
                    },
                    CalibrationSolveRequest::Gain(gain) => {
                        Self::gain(gain, gain.gain_type.vis_cal(), append)
                    }
                    CalibrationSolveRequest::GainSpline { gain, .. } => {
                        Self::gain(gain, "GSPLINE", append)
                    }
                    CalibrationSolveRequest::Fringe { gain, .. }
                    | CalibrationSolveRequest::FringeTool { gain, .. } => {
                        Self::gain(gain, "Fringe Jones", append)
                    }
                    CalibrationSolveRequest::Accor(gain) => Self::gain(gain, "Accor Jones", append),
                    CalibrationSolveRequest::AdditiveMueller(gain)
                    | CalibrationSolveRequest::AdditiveMuellerPolynomial { gain, .. } => {
                        Self::gain(gain, "A Mueller", append)
                    }
                    CalibrationSolveRequest::Baseline {
                        gain,
                        frequency_dependent,
                        ..
                    } => {
                        let subtype = if *frequency_dependent {
                            "Mf Mueller"
                        } else {
                            "M Mueller"
                        };
                        Self {
                            frequency_dependent: *frequency_dependent,
                            ..Self::gain(gain, subtype, append)
                        }
                    }
                    CalibrationSolveRequest::Bandpass(request) => Self {
                        subtype: if request.band_type == BandpassType::B {
                            "B Jones"
                        } else {
                            "BPOLY"
                        },
                        table: &request.output_table,
                        interval: request.solve_interval,
                        frequency_dependent: true,
                        append,
                        reference: Some(&request.refant),
                        min_snr: if request.band_type == BandpassType::B {
                            request.cuts.as_ref().map_or(0., |cuts| cuts.min_snr)
                        } else {
                            0.
                        },
                        normalization: request
                            .normalize_average_amplitude
                            .then_some(bandpass.normalization),
                    },
                }
            }
            ConfiguredSolve::Polarization { request, .. } => Self {
                subtype: request.kind.subtype(),
                table: &request.output,
                interval: request.interval,
                frequency_dependent: request.kind.channelized(),
                append: request.append,
                reference: (!request.kind.cross_hand() || request.kind.gain_phase())
                    .then_some(&request.refant),
                min_snr: request.min_snr,
                normalization: request.normalization,
            },
            ConfiguredSolve::SingleDish(solver) => Self {
                frequency_dependent: true,
                ..Self::gain(
                    &solver.request,
                    solver.subtype(),
                    solver.mode == CalibrationOutputMode::Append,
                )
            },
            ConfiguredSolve::GatherOnly {
                request,
                mode,
                subtype,
                channelized,
                ..
            } => Self {
                frequency_dependent: *channelized,
                ..Self::gain(request, subtype, *mode == CalibrationOutputMode::Append)
            },
            ConfiguredSolve::LegacyEp(solver) => Self {
                subtype: "EP Jones",
                table: solver.output_path(),
                interval: GainSolveInterval::Seconds(solver.interval_seconds),
                frequency_dependent: false,
                append: solver.mode == CalibrationOutputMode::Append,
                reference: Some(&solver.reference),
                min_snr: 0.,
                normalization: None,
            },
        }
    }

    fn gain(gain: &'a GainSolveRequest, subtype: &'static str, append: bool) -> Self {
        Self {
            subtype,
            table: &gain.output_table,
            interval: gain.solve_interval,
            frequency_dependent: false,
            append,
            reference: Some(&gain.refant),
            min_snr: gain.min_snr,
            normalization: gain
                .normalize_average_amplitude
                .then_some(gain.normalization_type),
        }
    }

    fn description(
        &self,
        equation: &VisibilityEquation,
        interval: &str,
        apmode: &str,
        refant_mode: &str,
        reference: Option<&RefAntSelector>,
    ) -> String {
        let (time, frequency) = interval.split_once(',').unwrap_or((interval, ""));
        let time = match self.interval {
            _ if self.subtype == "EP Jones" => time.into(),
            GainSolveInterval::Infinite => "inf".into(),
            GainSolveInterval::Integration if time.is_empty() => "int".into(),
            GainSolveInterval::Seconds(seconds) if time.is_empty() => format!("{seconds}s"),
            _ if time.trim().parse::<f64>().is_ok() => format!("{time}s"),
            _ => time.into(),
        };
        let interval = if self.frequency_dependent {
            let lower_frequency = frequency.to_ascii_lowercase();
            let frequency = if frequency.is_empty() || lower_frequency.contains("none") {
                "none".into()
            } else if lower_frequency.contains("ch") {
                lower_frequency
            } else if frequency.trim().parse::<f64>().is_ok() {
                format!("{frequency}ch")
            } else {
                frequency.into()
            };
            format!("{time},{frequency}")
        } else {
            time
        };
        let reference = reference
            .or(self.reference)
            .and_then(|selector| antenna_names(equation, selector))
            .unwrap_or_else(|| "none".into());
        let normalization = match self.normalization {
            Some(GainNormalization::Mean) => "true normtype=MEAN",
            Some(GainNormalization::Median) => "true normtype=MEDIAN",
            None => "false",
        };
        format!(
            "{}: table={} append={} solint={interval} refantmode='{refant_mode}' refant='{reference}' minsnr={} apmode={apmode} solnorm={normalization}",
            self.subtype,
            self.table.display(),
            self.append,
            format_calibration_number(f64::from(self.min_snr)),
        )
    }
}

fn format_calibration_number(value: f64) -> String {
    if !value.is_finite() {
        return value.to_string().to_ascii_lowercase();
    }
    // Match the default CASA stream's six significant digits.
    let scientific = format!("{value:.5e}");
    let (mantissa, exponent) = scientific.split_once('e').expect("scientific notation");
    let exponent: i32 = exponent.parse().expect("decimal exponent");
    if (-4..=5).contains(&exponent) {
        let decimals = (5 - exponent) as usize;
        let fixed = format!("{value:.decimals$}");
        trimmed_decimal(&fixed).into()
    } else {
        format!("{}e{exponent:+03}", trimmed_decimal(mantissa))
    }
}

fn trimmed_decimal(value: &str) -> &str {
    if value.contains('.') {
        value.trim_end_matches('0').trim_end_matches('.')
    } else {
        value
    }
}

fn antenna_names(equation: &VisibilityEquation, selector: &RefAntSelector) -> Option<String> {
    if *selector == RefAntSelector::Unspecified {
        return None;
    }
    let ms = MeasurementSet::open(equation.attachment.path()).ok()?;
    let antenna = ms.antenna().ok()?;
    let ids = resolve_refants(&ms, selector).ok()?;
    let names = ids
        .into_iter()
        .filter_map(|id| {
            usize::try_from(id)
                .ok()
                .and_then(|id| antenna.name(id).ok())
        })
        .collect::<Vec<_>>();
    (!names.is_empty()).then(|| names.join(","))
}
