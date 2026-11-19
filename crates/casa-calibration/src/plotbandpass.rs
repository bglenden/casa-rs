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

use std::collections::BTreeMap;
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
    pub(crate) x_limits: [f64; 2],
    pub(crate) frequency_channels: Vec<f64>,
    atmosphere: Vec<AtmosphericCurve>,
    fdm: Option<Vec<FdmWindow>>,
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
    let companions = request
        .companion_tables
        .iter()
        .flatten()
        .map(|path| Companion::open(path, request.vis))
        .collect::<Result<Vec<_>, _>>()?;
    if invalid_overlay(&source, request, &companions) {
        return Ok(empty_plots(source, false, None));
    }
    let (selected, time_count) = match groups(
        &source,
        &request.selectors,
        request.overlay,
        request.threshold_seconds,
    ) {
        Ok(selected) => selected,
        Err(error) => {
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
                (
                    antenna_name(&source, row.antenna),
                    row.window,
                    selected.time_index,
                    spectra[&selected.row].shape()[0],
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
        companions: &companions,
        atmosphere: atmosphere.as_ref(),
        fdm_windows: fdm_windows.as_deref(),
    };
    'groups: for (key, rows) in selected {
        for &axis in &ordinates {
            if panel_limit.is_some_and(|limit| panels.len() >= limit) {
                break 'groups;
            }
            panels.push(panel(&context, key, &rows, axis)?);
        }
    }
    Ok(BandpassPlots {
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
        time_count,
        companions,
        atmosphere,
        fdm_windows,
    } = *context;
    let mut series = Vec::new();
    let mut comparison_count: Option<usize> = None;
    let mut metadata = None;
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
        let derivative = request.derivative_sigma > 0.0;
        metadata
            .get_or_insert_with(|| PanelMetadata::new(window, samples, frequency_axis, derivative))
            .extend_domain(domain);
        let matches = companions
            .iter()
            .map(|companion| {
                companion.match_row(address, window.original_id, request.threshold_seconds)
            })
            .collect::<Vec<_>>();
        let mut comparison_series = (0..matches.len()).map(|_| Vec::new()).collect::<Vec<_>>();
        let labels = polarization_labels(&source.polarization_basis, receptors);
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
                        metadata.intersect_domain(&curve.x);
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
                });
            }
            let (curve, statistics) = if derivative {
                let Derivative {
                    curve,
                    deviation,
                    rms,
                } = raw.differentiate(ordinate == Ordinate::Amplitude, request.edge);
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
            });
        }
        series.extend(comparison_series.into_iter().flatten());
    }
    let metadata = metadata.ok_or("empty bandpass panel")?;
    let (title, suffix) = captions(
        source,
        key,
        selected[0],
        request.show_baseband_number,
        time_count,
    )?;
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
        x_label,
        y_label: ordinate_label(ordinate, request.derivative_sigma > 0.0, &source.subtype),
        series,
        x_limits,
        frequency_channels: metadata.frequency_channels,
        atmosphere,
        fdm: fdm_windows.and_then(|windows| {
            fdm_for_panel(source, &source.rows[selected[0].row], windows, request)
        }),
    })
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
        }
    }

    fn extend_domain(&mut self, [first, last]: [f64; 2]) {
        self.domain[0] = self.domain[0].min(first.min(last));
        self.domain[1] = self.domain[1].max(first.max(last));
    }

    fn intersect_domain(&mut self, coordinates: &[f64]) {
        if let (Some(first), Some(last)) = (coordinates.first(), coordinates.last()) {
            let lower = self.domain[0].max(first.min(*last));
            let upper = self.domain[1].min(first.max(*last));
            if lower < upper {
                self.domain = [lower, upper];
            }
        }
    }

    fn x_limits(&self) -> [f64; 2] {
        let [lower, upper] = self.domain;
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

fn polarization_labels(basis: &str, receptors: usize) -> Vec<&'static str> {
    let basis = if basis.eq_ignore_ascii_case("circular") {
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
    source: &Solutions,
    key: GroupKey,
    first: SelectedRow,
    show_baseband: bool,
    time_count: usize,
) -> Result<(String, String), String> {
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
        if show_baseband {
            let baseband = spectral_window(source, window)?.baseband.unwrap_or(-1);
            window_title.push_str(&format!(", bb{baseband}"));
        }
        title.push(window_title);
        suffix.push(format!("spw{window:02}"));
    }
    if let Some(name) = usize::try_from(row.field)
        .ok()
        .and_then(|id| source.fields.get(id))
    {
        title.push(format!("field {}: {name}", row.field));
    }
    if let Some(index) = key.time_index {
        let date = format_time(row.time / 86_400.0, YMD, 0);
        let clock = date.get(11..).unwrap_or_default();
        let last = match row.scan {
            Some(scan) => format!("scan{scan}  {clock}"),
            None => format!("t{index}/{}  {clock}", time_count.saturating_sub(1)),
        };
        title.push(last);
        suffix.push(format!("t{index:02}"));
    }
    Ok((title.join(",  "), suffix.join(".")))
}
