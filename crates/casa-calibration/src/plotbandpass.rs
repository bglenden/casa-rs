// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare selected calibration spectra for the shared plot renderer.

mod atmosphere;
mod companion;
mod document;
mod polynomial;
mod selection;
mod spectrum;
mod statistics;
mod table;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use casa_tables::{Table, TableOptions};
use casa_types::quanta::format::{YMD, format_time};
use serde::Serialize;

pub(crate) use atmosphere::AtmosphericOptions;
use atmosphere::{AtmosphereContext, AtmosphericCurve};
use companion::{Companion, requested_times};

pub(crate) use document::{BandpassPage, ChannelRange, DisplayOptions, pages};
use selection::{GroupKey, SelectedRow, groups};
pub(crate) use selection::{Overlay, Selectors};
use spectrum::{Curve, Derivative, Ordinate, platforming};
use statistics::StatisticsGrid;
pub(crate) use statistics::derivative_record;
use table::{SolutionWindow, Solutions, Spectrum};

pub(crate) struct PlotRequest<'a> {
    pub(crate) caltable: &'a Path,
    pub(crate) vis: Option<&'a Path>,
    pub(crate) companion_tables: [Option<&'a Path>; 2],
    pub(crate) selectors: Selectors,
    pub(crate) overlay: Overlay,
    pub(crate) frequency_axis: bool,
    pub(crate) y_axis: &'a str,
    pub(crate) polarizations: &'a [&'a str],
    pub(crate) threshold_seconds: f64,
    pub(crate) show_flagged: bool,
    pub(crate) show_baseband_number: bool,
    pub(crate) derivative_sigma: f64,
    pub(crate) edge: usize,
    pub(crate) resample: u32,
    pub(crate) platforming_threshold: f64,
    pub(crate) platforming_sigma: f64,
    pub(crate) atmosphere: AtmosphericOptions<'a>,
    pub(crate) show_fdm: bool,
    pub(crate) zoom_intersect: bool,
}

pub(crate) struct BandpassPlots {
    pub(crate) caltable: PathBuf,
    pub(crate) measurement_set: Option<PathBuf>,
    header: String,
    pub(crate) panels: Vec<BandpassPanel>,
    statistics: Option<StatisticsGrid>,
    pub(crate) polynomial: bool,
    pub(crate) returns_none: bool,
    pub(crate) selection_notice: Option<String>,
    immediate_combined_overlay: bool,
}

pub(crate) struct BandpassPanel {
    ordinate: Ordinate,
    pub(crate) title: String,
    pub(crate) x_label: String,
    pub(crate) y_label: &'static str,
    pub(crate) series: Vec<BandpassSeries>,
    pub(crate) suffix: String,
    first_skipped_suffix: Option<String>,
    pub(crate) x_limits: [f64; 2],
    pub(crate) frequency_channels: Vec<f64>,
    atmosphere: Vec<AtmosphericCurve>,
    fdm: Option<Vec<FdmWindow>>,
    diagnostics: Vec<String>,
    epoch: f64,
    overlay: Overlay,
    legend_times: Vec<(usize, f64)>,
}

#[derive(Clone, Serialize)]
struct FdmWindow {
    label: String,
    frequencies: [f64; 2],
}

pub(crate) struct BandpassSeries {
    pub(crate) antenna_name: String,
    pub(crate) window: i32,
    pub(crate) time_index: usize,
    pub(crate) receptor: usize,
    pub(crate) label: String,
    pub(crate) curve: BandpassCurve,
    pub(crate) statistics: Option<DerivativeStatistics>,
    table_index: usize,
    polynomial: bool,
}

pub(crate) struct BandpassCurve {
    pub(crate) x: Vec<f64>,
    pub(crate) y: Vec<f64>,
}

#[derive(Clone, Copy)]
pub(crate) struct DerivativeStatistics {
    pub(crate) mad: f64,
    pub(crate) stdev: f64,
    platforming: Option<PlatformingTime>,
}

#[derive(Clone, Copy)]
enum PlatformingTime {
    Epoch(f64),
    Index(usize),
}

pub(crate) fn prepare_bandpass(
    request: &PlotRequest<'_>,
    panel_limit: Option<usize>,
) -> Result<BandpassPlots, String> {
    let source = Solutions::open(request.caltable, request.vis)?;
    let polynomial = source.subtype == "BPOLY";
    if source.rows.iter().all(|row| row.field < 0) {
        return Ok(empty_plots(
            source,
            false,
            Some("The field_id is -1 (invalid) for all rows of this caltable.".into()),
        ));
    }
    let companions = request
        .companion_tables
        .iter()
        .flatten()
        .map(|path| Companion::open(path, request.vis, request.threshold_seconds))
        .collect::<Result<Vec<_>, _>>()?;
    if invalid_overlay(&source, request, &companions) {
        return Ok(empty_plots(source, false, None));
    }
    let selection::SelectedGroups {
        groups: selected,
        time_count,
        fields,
    } = match groups(
        &source,
        &request.selectors,
        request.overlay,
        request.threshold_seconds,
    ) {
        Ok(selected) => selected,
        Err(error) => {
            if error.invalid_integer {
                return Err(format!(
                    "invalid literal for int() with base 10: '{}'",
                    error.word
                ));
            }
            if error.parameter == "spw" && error.word.contains('!') {
                return Ok(empty_plots(
                    source,
                    false,
                    Some("The ! modifier is not (yet) supported".into()),
                ));
            }
            let message = format!(
                "No bandpass solutions for {}={:?}",
                error.parameter, error.word
            );
            let none = !matches!(error.parameter, "antenna" | "field");
            return Ok(empty_plots(source, none, Some(message)));
        }
    };
    if request.show_baseband_number
        && source
            .windows
            .iter()
            .all(|window| window.baseband.is_none())
    {
        return Ok(empty_plots(
            source,
            true,
            Some("This dataset does not have a BBC_NO column in SPECTRAL_WINDOW.".into()),
        ));
    }
    let reference = request.atmosphere.reference_field;
    if !reference.is_empty()
        && !fields.iter().any(|&id| {
            reference.parse::<i32>().ok() == Some(id) || field_name(&source, id) == Some(reference)
        })
    {
        return Ok(empty_plots(
            source,
            false,
            Some(format!(
                "The showatmfield ({reference}) is outside the selected fields."
            )),
        ));
    }
    let field_origin = if request
        .selectors
        .field
        .iter()
        .any(|value| !value.is_empty())
        && request.selectors.time.iter().all(String::is_empty)
        && request.selectors.scan.iter().all(String::is_empty)
    {
        selected
            .values()
            .flatten()
            .map(|row| row.time_index)
            .min()
            .unwrap_or(0)
    } else {
        0
    };
    let requested_times =
        requested_times(selected.values().flatten().copied(), request, time_count);
    for companion in &companions {
        companion.check_time_count(requested_times)?;
    }
    let mut panels = Vec::new();
    let spectra = select_spectra(&source, &selected)?;
    let ordinates = ordinates(&source, request.y_axis)?;
    let statistics = (request.derivative_sigma > 0.0 && !polynomial).then(|| {
        StatisticsGrid::new(
            selected.values().flatten().map(|selected| {
                let row = source.rows[selected.row];
                let receptor_count = spectra[&selected.row].shape()[0];
                let receptor_count = spectral_window(&source, row.window)
                    .ok()
                    .map_or(receptor_count, |window| {
                        polarization_labels(&source, window.original_id, receptor_count).len()
                    });
                (
                    antenna_name(&source, row.antenna),
                    row.window,
                    selected.time_index,
                    receptor_count,
                )
            }),
            &ordinates,
        )
    });
    let atmosphere = AtmosphereContext::open(&source, &request.atmosphere)?;
    let fdm_windows = fdm_windows(&source, request.show_fdm && request.frequency_axis);
    let context = PanelContext {
        source: &source,
        spectra: &spectra,
        request,
        time_count,
        fields: &fields,
        field_origin,
        companions: &companions,
        atmosphere: atmosphere.as_ref(),
        fdm_windows: fdm_windows.as_deref(),
    };
    let mut first_skipped_suffix = None;
    'groups: for (key, rows) in selected {
        for &axis in &ordinates {
            if panel_limit.is_some_and(|limit| panels.len() >= limit) {
                break 'groups;
            }
            let mut panel = panel(&context, key, &rows, axis)?;
            if panel.series.is_empty() {
                first_skipped_suffix.get_or_insert(panel.suffix);
            } else {
                panel.first_skipped_suffix = first_skipped_suffix.take();
                panels.push(panel);
            }
        }
    }
    Ok(BandpassPlots {
        header: plot_header(&source, request, &companions),
        caltable: source.path,
        measurement_set: source.measurement_set,
        panels,
        statistics,
        polynomial,
        returns_none: false,
        selection_notice: None,
        immediate_combined_overlay: request.overlay.antenna && request.overlay.time,
    })
}

fn empty_plots(
    source: Solutions,
    returns_none: bool,
    selection_notice: Option<String>,
) -> BandpassPlots {
    BandpassPlots {
        header: source.path.display().to_string(),
        caltable: source.path,
        measurement_set: source.measurement_set,
        panels: Vec::new(),
        statistics: None,
        polynomial: source.subtype == "BPOLY",
        returns_none,
        selection_notice,
        immediate_combined_overlay: false,
    }
}

fn plot_header(source: &Solutions, request: &PlotRequest<'_>, companions: &[Companion]) -> String {
    let primary = source.path.display();
    if let [table] = companions
        && !table.polynomial()
        && let Some(path) = request.companion_tables[0]
    {
        return format!("c1={primary}, c2={}", path.display());
    }
    let mut title = primary.to_string();
    if let Some(degrees) = polynomial_degrees(source) {
        title.push_str(&format!(
            " (degamp={}, degphase={})",
            degrees[0], degrees[1]
        ));
    }
    for (path, companion) in request.companion_tables.iter().flatten().zip(companions) {
        title.push_str(&format!(", {}", path.display()));
        if let Some(degrees) = companion.degrees() {
            title.push_str(&format!(
                " (degamp={}, degphase={})",
                degrees[0], degrees[1]
            ));
        }
    }
    title
}

fn polynomial_degrees(source: &Solutions) -> Option<[usize; 2]> {
    if source.subtype == "BPOLY"
        && let Ok(Spectrum::Polynomial(poly)) = source.spectrum(0)
    {
        return Some(poly.degrees());
    }
    None
}

fn invalid_overlay(
    source: &Solutions,
    request: &PlotRequest<'_>,
    companions: &[Companion],
) -> bool {
    (source.subtype == "BPOLY"
        && (!companions.is_empty() || request.overlay.antenna || request.overlay.time))
        || (!companions.is_empty()
            && (!request.frequency_axis || request.overlay.antenna || request.overlay.time))
        || (request.companion_tables[0].is_none() && request.companion_tables[1].is_some())
        || (companions.len() == 2 && companions.iter().any(|table| !table.polynomial()))
}

fn select_spectra(
    source: &Solutions,
    groups: &BTreeMap<GroupKey, Vec<SelectedRow>>,
) -> Result<BTreeMap<usize, table::Spectrum>, String> {
    groups
        .values()
        .flatten()
        .map(|selected| Ok((selected.row, source.spectrum(selected.row)?)))
        .collect()
}

struct PanelContext<'a, 'request> {
    source: &'a Solutions,
    spectra: &'a BTreeMap<usize, table::Spectrum>,
    request: &'a PlotRequest<'request>,
    time_count: usize,
    fields: &'a [i32],
    field_origin: usize,
    companions: &'a [Companion],
    atmosphere: Option<&'a AtmosphereContext<'request>>,
    fdm_windows: Option<&'a [SolutionWindow]>,
}

fn panel(
    context: &PanelContext<'_, '_>,
    key: GroupKey,
    selected: &[SelectedRow],
    ordinate: Ordinate,
) -> Result<BandpassPanel, String> {
    let PanelContext {
        source,
        spectra,
        request,
        companions,
        atmosphere,
        fdm_windows,
        ..
    } = *context;
    let mut series = Vec::new();
    let mut comparison_count: Option<usize> = None;
    let mut metadata = None;
    let mut diagnostics = Vec::new();
    for row in selected {
        let address = source.rows[row.row];
        let samples = &spectra[&row.row];
        let [receptors, total_channels] = samples.shape();
        let window = spectral_window(source, address.window)?;
        let frequency_axis = request.frequency_axis || source.subtype == "BPOLY";
        let frequencies_ghz = frequency_axis.then(|| {
            window
                .sky_frequencies_hz
                .iter()
                .map(|value| value * 1e-9)
                .collect::<Vec<_>>()
        });
        let domain = match (samples, &frequencies_ghz) {
            (Spectrum::Polynomial(poly), _) => poly.domain.map(|hz| hz * 1e-9),
            (_, Some(hz)) => [
                *hz.first().ok_or("empty spectral window")?,
                *hz.last().ok_or("empty spectral window")?,
            ],
            (_, None) => [0.0, total_channels.saturating_sub(1) as f64],
        };
        let derivative = request.derivative_sigma > 0.0
            && ordinate != Ordinate::Decibels
            && source.subtype != "BPOLY";
        metadata
            .get_or_insert_with(|| PanelMetadata::new(window, samples, frequency_axis, derivative))
            .extend_domain(domain);
        let matches = companions
            .iter()
            .map(|companion| {
                companion.match_row(
                    address,
                    window.original_id,
                    row.time_index,
                    request.threshold_seconds,
                )
            })
            .collect::<Vec<_>>();
        let mut comparison_series = (0..matches.len()).map(|_| Vec::new()).collect::<Vec<_>>();
        let labels = polarization_labels(source, window.original_id, receptors);
        let receptors = labels.len();
        for receptor in select_polarizations(&labels, request.polarizations) {
            let raw = samples.curve(
                receptor,
                frequencies_ghz.as_deref(),
                ordinate,
                request.show_flagged,
            )?;
            if raw.x.is_empty() {
                continue;
            }
            if derivative && request.resample > 1 {
                return Err("name 'resampleSolution' is not defined".into());
            }
            for (index, companion) in companions.iter().enumerate() {
                let Some(row_index) = matches[index] else {
                    continue;
                };
                let (curve, channels) =
                    companion.curve(row_index, receptor, ordinate, &raw, request)?;
                if !companion.polynomial() {
                    if request.zoom_intersect
                        && let Some(metadata) = &mut metadata
                    {
                        metadata.use_narrower_solution(&raw.x, &curve.x);
                    }
                    let visible = if channels > total_channels {
                        channels
                    } else {
                        0
                    };
                    comparison_count =
                        Some(comparison_count.map_or(visible, |old| old.max(visible)));
                }
                comparison_series[index].push(BandpassSeries {
                    antenna_name: antenna_name(source, address.antenna),
                    window: address.window,
                    time_index: row.time_index,
                    receptor,
                    label: labels[receptor].into(),
                    curve: BandpassCurve {
                        x: curve.x,
                        y: curve.y,
                    },
                    statistics: None,
                    table_index: index + 1,
                    polynomial: companion.polynomial(),
                });
            }
            let (curve, statistics) = if derivative {
                let differentiated =
                    raw.differentiate(ordinate == Ordinate::Amplitude, request.edge);
                diagnostics.extend(outlier_message(
                    context,
                    &differentiated,
                    address,
                    receptor,
                    ordinate,
                ));
                let Derivative {
                    curve,
                    deviation,
                    rms,
                } = differentiated;
                let threshold = if request.platforming_sigma > 0.0 {
                    request.platforming_sigma * deviation
                } else {
                    request.platforming_threshold
                };
                let event = (ordinate == Ordinate::Amplitude && platforming(&curve.y, threshold))
                    .then_some(if receptors == 1 {
                        PlatformingTime::Index(row.time_index)
                    } else {
                        PlatformingTime::Epoch(address.time)
                    });
                (
                    curve,
                    Some(DerivativeStatistics {
                        mad: deviation,
                        stdev: rms,
                        platforming: event,
                    }),
                )
            } else {
                (raw, None)
            };
            let Curve { x, y } = curve;
            series.push(BandpassSeries {
                antenna_name: antenna_name(source, address.antenna),
                window: address.window,
                time_index: row.time_index,
                receptor,
                label: labels[receptor].into(),
                curve: BandpassCurve { x, y },
                statistics,
                table_index: 0,
                polynomial: source.subtype == "BPOLY",
            });
        }
        series.extend(comparison_series.into_iter().flatten());
    }
    let metadata = metadata.ok_or("empty bandpass panel")?;
    let (title, suffix) = captions(context, key, selected)?;
    let x_limits = metadata.x_limits();
    let atmosphere = if let Some(atmosphere) = atmosphere {
        let first = source.rows[selected[0].row];
        let window = spectral_window(source, first.window)?;
        atmosphere.curves(window, first, request.frequency_axis)?
    } else {
        Vec::new()
    };
    let x_label = match comparison_count {
        Some(count) => metadata
            .label
            .replace(" channels)", &format!(", {count} channels)")),
        None => metadata.label,
    };
    Ok(BandpassPanel {
        ordinate,
        title,
        suffix,
        first_skipped_suffix: None,
        x_label,
        y_label: ordinate_label(
            ordinate,
            request.derivative_sigma > 0.0
                && ordinate != Ordinate::Decibels
                && source.subtype != "BPOLY",
            &source.subtype,
        ),
        series,
        x_limits,
        frequency_channels: metadata.frequency_channels,
        atmosphere,
        fdm: fdm_windows.and_then(|windows| {
            fdm_for_panel(source, &source.rows[selected[0].row], windows, request)
        }),
        diagnostics,
        epoch: source.rows[selected[0].row].time,
        overlay: request.overlay,
        legend_times: overlay_legend_times(context, selected),
    })
}

fn overlay_legend_times(
    context: &PanelContext<'_, '_>,
    selected: &[SelectedRow],
) -> Vec<(usize, f64)> {
    if !context.request.overlay.time {
        return Vec::new();
    }
    selected
        .iter()
        .map(|row| (row.time_index, context.source.rows[row.row].time))
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .collect()
}

fn outlier_message(
    context: &PanelContext<'_, '_>,
    derivative: &Derivative,
    row: table::SolutionRow,
    receptor: usize,
    ordinate: Ordinate,
) -> Option<String> {
    let phase = ordinate == Ordinate::Phase;
    if phase
        && derivative
            .curve
            .y
            .iter()
            .map(|value| value.abs())
            .sum::<f64>()
            < 0.002
    {
        return None;
    }
    let sigma = context.request.derivative_sigma;
    let outliers = derivative.outliers(sigma, context.request.edge)?;
    let name = antenna_name(context.source, row.antenna);
    let kind = if phase { "phs" } else { "amp" };
    Some(format!(
        "{name}, Pol {receptor}, spw {}, {kind}: {} points exceed {sigma:.1} sigma (worst={:.2} at chan {})",
        row.window, outliers.count, outliers.worst_sigma, outliers.coordinate as i32,
    ))
}

fn fdm_windows(source: &Solutions, requested: bool) -> Option<Vec<SolutionWindow>> {
    if !requested {
        return None;
    }
    let ms = source.measurement_set.as_ref()?;
    let table = Table::open(TableOptions::new(ms.join("SPECTRAL_WINDOW"))).ok()?;
    table::windows(&table).ok()
}

fn fdm_for_panel(
    source: &Solutions,
    row: &table::SolutionRow,
    windows: &[SolutionWindow],
    request: &PlotRequest<'_>,
) -> Option<Vec<FdmWindow>> {
    let selected = spectral_window(source, row.window).ok()?;
    if selected.sky_frequencies_hz.len() > 256 {
        return None;
    }
    let baseband = selected.baseband?;
    Some(
        windows
            .iter()
            .filter_map(|window| {
                let channels = window.sky_frequencies_hz.len();
                if channels < 15 || [256, 128, 64, 32, 16, 248, 124, 62].contains(&channels) {
                    return None;
                }
                if !request.show_baseband_number && window.baseband != Some(baseband) {
                    return None;
                }
                let mut label = format!("spw{}", window.original_id);
                if request.show_baseband_number {
                    if let Some(baseband) = window.baseband {
                        label.push_str(&format!("(bb{baseband})"));
                    } else {
                        label.push_str("(bb?)");
                    }
                }
                let first = window.sky_frequencies_hz.first()?;
                let last = window.sky_frequencies_hz.last()?;
                Some(FdmWindow {
                    label,
                    frequencies: [first.min(*last) * 1e-9, first.max(*last) * 1e-9],
                })
            })
            .collect(),
    )
}

struct PanelMetadata {
    label: String,
    domain: [f64; 2],
    frequency_channels: Vec<f64>,
    polynomial: bool,
    comparison_domain: Option<[f64; 2]>,
}

impl PanelMetadata {
    fn new(
        window: &SolutionWindow,
        samples: &Spectrum,
        frequency_axis: bool,
        derivative: bool,
    ) -> Self {
        let [_, channels] = samples.shape();
        let polynomial = matches!(samples, Spectrum::Polynomial(_));
        let count = if derivative {
            channels.saturating_sub(1).max(1)
        } else {
            channels
        };
        let frames = [
            "REST", "LSRK", "LSRD", "BARY", "GEO", "TOPO", "GALACTO", "LGROUP", "CMB",
        ];
        let frame = usize::try_from(window.frame)
            .ok()
            .and_then(|id| frames.get(id))
            .copied()
            .unwrap_or("");
        let high_frequency = window
            .reference_frequency_hz
            .filter(|reference| *reference > 60e9);
        let label = if !frequency_axis {
            format!("Channels ({count})")
        } else if let Some(reference) = high_frequency.filter(|_| !polynomial) {
            let frequencies = &window.sky_frequencies_hz;
            let reversed = frequencies.is_empty() && window.sideband < 0
                || reference > frequencies.iter().sum::<f64>() / frequencies.len() as f64;
            let sideband = if reversed { "LSB" } else { "USB" };
            format!("{frame} {sideband} Frequency (GHz) ({count} channels)")
        } else {
            format!("Frequency (GHz) ({count} channels)")
        };
        Self {
            label,
            domain: [f64::INFINITY, f64::NEG_INFINITY],
            frequency_channels: if !frequency_axis {
                Vec::new()
            } else {
                match samples {
                    Spectrum::Polynomial(poly) => &poly.frequencies_hz,
                    _ => &window.sky_frequencies_hz,
                }
                .iter()
                .map(|value| value * 1e-9)
                .collect()
            },
            polynomial,
            comparison_domain: None,
        }
    }

    fn extend_domain(&mut self, [first, last]: [f64; 2]) {
        self.domain[0] = self.domain[0].min(first.min(last));
        self.domain[1] = self.domain[1].max(first.max(last));
    }

    fn use_narrower_solution(&mut self, primary: &[f64], companion: &[f64]) {
        fn limits(values: &[f64]) -> Option<[f64; 2]> {
            let first = *values.first()?;
            let last = *values.last()?;
            Some([first.min(last), first.max(last)])
        }
        if self.comparison_domain.is_none()
            && let (Some(first), Some(second)) = (limits(primary), limits(companion))
        {
            // CASA uses the narrower window and prefers the companion when
            // the widths agree, even for offset windows.
            self.comparison_domain = Some(if first[1] - first[0] < second[1] - second[0] {
                self.domain
            } else {
                second
            });
        }
    }

    fn x_limits(&self) -> [f64; 2] {
        let [lower, upper] = self.comparison_domain.unwrap_or(self.domain);
        let range = if lower == upper { 0.001 } else { upper - lower };
        let pad = if self.polynomial { 0.0 } else { range * 0.01 };
        [lower - pad, upper + pad]
    }
}

fn ordinates(source: &Solutions, axis: &str) -> Result<Vec<Ordinate>, String> {
    if source.subtype == "K Jones" {
        return Ok(vec![Ordinate::Delay]);
    }
    let normalized = axis.to_ascii_lowercase();
    let amplitude = if normalized.contains("db") {
        Ordinate::Decibels
    } else {
        Ordinate::Amplitude
    };
    if normalized.contains("both") || normalized.starts_with("ap") {
        Ok(vec![amplitude, Ordinate::Phase])
    } else if normalized.contains("phase") {
        Ok(vec![Ordinate::Phase])
    } else if normalized.contains("amp") || normalized == "tsys" {
        Ok(vec![amplitude])
    } else {
        Err(format!("unsupported bandpass y-axis {axis:?}"))
    }
}

fn ordinate_label(axis: Ordinate, derivative: bool, subtype: &str) -> &'static str {
    match (axis, derivative) {
        (Ordinate::Amplitude, true) => "Amp derivative (%_of_median/channel)",
        (Ordinate::Phase, true) => "Phase derivative (deg/channel)",
        (Ordinate::Decibels, true) => "Amp derivative (dB/channel)",
        (Ordinate::Delay, _) => "Delay (ns)",
        (Ordinate::Phase, false) => "Phase (deg)",
        (Ordinate::Decibels, false) => "Amplitude (dB)",
        (Ordinate::Amplitude, false) if subtype.contains("Tsys") || subtype.contains("TSYS") => {
            "Tsys (K)"
        }
        (Ordinate::Amplitude, false) => "Amplitude",
    }
}

fn spectral_window(source: &Solutions, window: i32) -> Result<&SolutionWindow, String> {
    usize::try_from(window)
        .ok()
        .and_then(|index| source.windows.get(index))
        .ok_or_else(|| format!("unknown calibration window {window}"))
}

fn polarization_labels(source: &Solutions, window: i32, receptors: usize) -> Vec<&'static str> {
    if let Some(labels) = source.correlations.get(&window) {
        return labels.iter().take(receptors).copied().collect();
    }
    let basis = if source.polarization_basis.eq_ignore_ascii_case("circular") {
        ["RR", "LL"]
    } else {
        ["XX", "YY"]
    };
    basis.into_iter().take(receptors).collect()
}

fn select_polarizations(available: &[&str], requested: &[&str]) -> Vec<usize> {
    let selected = available
        .iter()
        .enumerate()
        .filter(|(_, label)| {
            requested
                .iter()
                .any(|request| request.eq_ignore_ascii_case(label))
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if selected.is_empty() {
        return (0..available.len()).collect();
    }
    selected
}

fn antenna_name(source: &Solutions, antenna: i32) -> String {
    usize::try_from(antenna)
        .ok()
        .and_then(|id| source.antennas.get(id))
        .cloned()
        .unwrap_or_else(|| format!("{antenna:02}"))
}

fn captions(
    context: &PanelContext<'_, '_>,
    key: GroupKey,
    selected: &[SelectedRow],
) -> Result<(String, String), String> {
    let source = context.source;
    let overlay = context.request.overlay;
    let fields = context.fields;
    let first = selected[0];
    let row = source.rows[first.row];
    let mut title = Vec::new();
    let mut suffix = Vec::new();
    if let Some(antenna) = key.antenna {
        let written = antenna_name(source, antenna);
        title.push(format!("Ant {antenna}: {written}"));
        suffix.push(written);
    }
    if let Some(window) = key.window {
        let original_id = spectral_window(source, window)?.original_id;
        let mut window_title = format!("spw {original_id}");
        if context.request.show_baseband_number {
            let baseband = spectral_window(source, window)?.baseband.unwrap_or(-1);
            window_title.push_str(&format!(", bb{baseband}"));
        }
        title.push(window_title);
        suffix.push(format!("spw{window:02}"));
    } else if overlay.window || overlay.baseband {
        let windows = selected
            .iter()
            .map(|selected| source.rows[selected.row].window)
            .collect::<BTreeSet<_>>();
        let file_window = if overlay.baseband {
            windows.first()
        } else {
            windows.last()
        };
        suffix.push(format!("spw{:02}", file_window.copied().unwrap_or(0)));
        let listed = if overlay.baseband {
            " all".into()
        } else {
            windows
                .iter()
                .map(|id| format!("np.int64({id})"))
                .collect::<Vec<_>>()
                .join(",")
        };
        title.push(format!("spw{listed}"));
    }
    if overlay.time && fields.len() > 1 {
        let ids = fields
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let names = fields
            .iter()
            .filter_map(|&id| field_name(source, id))
            .collect::<Vec<_>>()
            .join(",");
        title.push(format!("fields {ids}: {names}"));
    } else if let Some(name) = field_name(source, row.field) {
        title.push(format!("field {}: {name}", row.field));
    }
    if let Some(index) = key.time_index {
        let date = format_time(row.time / 86_400.0, YMD, 0);
        let clock = date.get(11..).unwrap_or_default();
        let last = match row.scan {
            Some(scan) => format!("scan{scan}  {clock}"),
            None => format!("t{index}/{}  {clock}", context.time_count.saturating_sub(1)),
        };
        title.push(last);
        suffix.push(format!(
            "t{:02}",
            index.saturating_sub(context.field_origin)
        ));
    }
    Ok((title.join(",  "), suffix.join(".")))
}

fn field_name(source: &Solutions, field: i32) -> Option<&str> {
    usize::try_from(field)
        .ok()
        .and_then(|id| source.fields.get(id))
        .map(String::as_str)
}
