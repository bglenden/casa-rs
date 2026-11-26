// SPDX-License-Identifier: LGPL-3.0-or-later
//! Per-antenna derivatives and aggregate statistics from completed panels.

use std::collections::BTreeMap;

use casa_contracts::{CasaEntry, CasaKey, CasaValue};
use casa_plot_data::calibration::plotbandpass::spectrum::{Ordinate, median};
use casa_plot_data::calibration::plotbandpass::{
    BandpassPlots, DerivativeStatistics, PlatformingTime, StatisticsGrid,
};

type Coordinates = (i32, usize, usize);
type Metric = &'static str;
type Metrics<T> = BTreeMap<Metric, T>;
type Samples = BTreeMap<Coordinates, Metrics<Vec<f64>>>;
type WindowValues<T> = BTreeMap<i32, BTreeMap<usize, BTreeMap<usize, T>>>;
type Platforming = BTreeMap<String, BTreeMap<i32, BTreeMap<usize, Vec<CasaValue>>>>;

pub(crate) fn derivative_record(plots: &BandpassPlots) -> Option<CasaValue> {
    let grid = plots.statistics.as_ref()?;
    let mut antennas = BTreeMap::<&str, WindowValues<Metrics<Option<f64>>>>::new();
    for antenna in &grid.antennas {
        let mut values = WindowValues::new();
        for address in grid.coordinates() {
            *coordinates_mut(&mut values, address) =
                grid.metrics.iter().map(|&metric| (metric, None)).collect();
        }
        antennas.insert(antenna, values);
    }

    let mut samples = Samples::new();
    let mut platforming = Platforming::new();
    for panel in &plots.panels {
        let metric = metric(panel.ordinate);
        let std_metric = if metric == "phase" {
            "phasestd"
        } else {
            "ampstd"
        };
        for series in &panel.series {
            let Some(DerivativeStatistics {
                mad,
                stdev,
                platforming: event,
            }) = series.statistics
            else {
                continue;
            };
            let address = (series.window, series.time_index, series.receptor);
            let values = antennas.entry(&series.antenna_name).or_default();
            let cell = coordinates_mut(values, address);
            cell.insert(metric, Some(mad));
            cell.insert(std_metric, Some(stdev));
            if let Some(event) = event {
                add_event(&mut platforming, &series.antenna_name, address, event);
            }
        }
    }
    for antenna in antennas.values() {
        for address in grid.coordinates() {
            let (window, time, receptor) = address;
            for &metric in &grid.metrics {
                if let Some(mad) = antenna[&window][&time][&receptor][metric] {
                    samples
                        .entry(address)
                        .or_default()
                        .entry(metric)
                        .or_default()
                        .push(mad);
                }
            }
        }
    }

    let mut entries = antennas
        .into_iter()
        .map(|(antenna, windows)| (antenna.to_string(), window_dict(windows, encode_optional)))
        .collect::<Vec<_>>();
    entries.push((
        "platforming".into(),
        CasaValue::dict(platforming.into_iter().map(|(antenna, windows)| {
            (
                antenna,
                int_dict(windows, |receptors| {
                    int_dict(receptors, |items| CasaValue::List { items })
                }),
            )
        })),
    ));
    entries.push(("median".into(), median_record(grid, &samples)));
    Some(CasaValue::dict(entries))
}

fn metric(ordinate: Ordinate) -> Metric {
    if ordinate == Ordinate::Phase {
        "phase"
    } else {
        "amp"
    }
}

fn coordinates_mut<T: Default>(
    values: &mut WindowValues<T>,
    (window, time, receptor): Coordinates,
) -> &mut T {
    values
        .entry(window)
        .or_default()
        .entry(time)
        .or_default()
        .entry(receptor)
        .or_default()
}

fn add_event(
    platforming: &mut Platforming,
    antenna: &str,
    (window, _, receptor): Coordinates,
    event: PlatformingTime,
) {
    let value = match event {
        PlatformingTime::Epoch(time) => CasaValue::float(time),
        PlatformingTime::Index(index) => CasaValue::Int {
            value: index as i64,
        },
    };
    platforming
        .entry(antenna.into())
        .or_default()
        .entry(window)
        .or_default()
        .entry(receptor)
        .or_default()
        .push(value);
}

fn median_record(grid: &StatisticsGrid, samples: &Samples) -> CasaValue {
    let mut windows = WindowValues::<Metrics<Option<f64>>>::new();
    let mut all = Metrics::<Vec<f64>>::new();
    let mut last_address = None;
    for address in grid.coordinates() {
        let cell = coordinates_mut(&mut windows, address);
        last_address = Some(address);
        for &metric in &grid.metrics {
            let values = samples
                .get(&address)
                .and_then(|metrics| metrics.get(metric))
                .map_or(&[][..], Vec::as_slice);
            all.entry(metric).or_default().extend_from_slice(values);
            cell.insert(metric, Some(median(values)));
        }
    }
    let overall = all
        .into_iter()
        .map(|(metric, values)| (metric, Some(median(&values))))
        .collect();
    if grid.metrics == ["phase"] {
        // CASA puts the overall phase median in the final cell in phase-only mode.
        if let Some(address) = last_address {
            *coordinates_mut(&mut windows, address) = overall;
        }
        return window_dict(windows, encode_optional);
    }
    let CasaValue::Dict { mut entries } = window_dict(windows, encode_optional) else {
        unreachable!("window dictionaries always encode as dictionaries");
    };
    entries.push(CasaEntry {
        key: CasaKey::String("median".into()),
        value: encode_optional(overall),
    });
    CasaValue::Dict { entries }
}

fn window_dict<T>(values: WindowValues<T>, mut encode: impl FnMut(T) -> CasaValue) -> CasaValue {
    int_dict(values, |times| {
        int_dict(times, |receptors| int_dict(receptors, &mut encode))
    })
}

fn int_dict<I, T>(items: BTreeMap<I, T>, mut encode: impl FnMut(T) -> CasaValue) -> CasaValue
where
    I: TryInto<i64>,
{
    CasaValue::Dict {
        entries: items
            .into_iter()
            .filter_map(|(key, item)| {
                Some(CasaEntry {
                    key: CasaKey::Int(key.try_into().ok()?),
                    value: encode(item),
                })
            })
            .collect(),
    }
}

fn encode_optional(values: Metrics<Option<f64>>) -> CasaValue {
    CasaValue::dict(
        values
            .into_iter()
            .map(|(key, value)| (key, value.map_or(CasaValue::None, CasaValue::float))),
    )
}
