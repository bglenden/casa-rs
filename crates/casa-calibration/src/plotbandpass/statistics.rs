// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's per-antenna and median derivative records.

use std::collections::BTreeMap;

use casa_provider_contracts::{CasaEntry, CasaKey, CasaValue};

use super::spectrum::{Ordinate, median};
use super::{BandpassPlots, DerivativeStatistics, PlatformingTime};

type WindowValues<T> = BTreeMap<i32, BTreeMap<usize, BTreeMap<usize, T>>>;
type Metrics = BTreeMap<&'static str, f64>;
type Samples = BTreeMap<&'static str, Vec<f64>>;

pub(crate) fn derivative_record(plots: &BandpassPlots) -> CasaValue {
    let mut antennas = BTreeMap::<&str, WindowValues<Metrics>>::new();
    let mut window_samples = WindowValues::<Samples>::new();
    let mut platforming = BTreeMap::<&str, BTreeMap<i32, BTreeMap<usize, Vec<CasaValue>>>>::new();
    for panel in &plots.panels {
        let metric = match panel.ordinate {
            Ordinate::Phase => "phase",
            _ => "amp",
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
            if let Some(event) = event {
                let value = match event {
                    PlatformingTime::Epoch(time) => CasaValue::float(time),
                    PlatformingTime::Index(index) => CasaValue::Int {
                        value: index as i64,
                    },
                };
                platforming
                    .entry(&series.antenna_name)
                    .or_default()
                    .entry(series.window)
                    .or_default()
                    .entry(series.receptor)
                    .or_default()
                    .push(value);
            }
            let spectrum = antennas
                .entry(&series.antenna_name)
                .or_default()
                .entry(series.window)
                .or_default()
                .entry(series.time_index)
                .or_default()
                .entry(series.receptor)
                .or_default();
            spectrum.insert(metric, mad);
            spectrum.insert(
                if metric == "phase" {
                    "phasestd"
                } else {
                    "ampstd"
                },
                stdev,
            );
            window_samples
                .entry(series.window)
                .or_default()
                .entry(series.time_index)
                .or_default()
                .entry(series.receptor)
                .or_default()
                .entry(metric)
                .or_default()
                .push(mad);
        }
    }
    let mut entries = antennas
        .into_iter()
        .map(|(antenna, windows)| CasaEntry {
            key: CasaKey::String(antenna.into()),
            value: int_dict(windows, |times| {
                int_dict(times, |receptors| int_dict(receptors, metrics))
            }),
        })
        .collect::<Vec<_>>();
    entries.push(CasaEntry {
        key: CasaKey::String("platforming".into()),
        value: CasaValue::dict(platforming.into_iter().map(|(antenna, windows)| {
            (
                antenna,
                int_dict(windows, |receptors| {
                    int_dict(receptors, |items| CasaValue::List { items })
                }),
            )
        })),
    });
    entries.push(CasaEntry {
        key: CasaKey::String("median".into()),
        value: int_dict(window_samples, median_window),
    });
    CasaValue::Dict { entries }
}

fn median_window(times: BTreeMap<usize, BTreeMap<usize, Samples>>) -> CasaValue {
    let mut accumulated = Samples::new();
    let mut entries = Vec::new();
    for (time, receptors) in times {
        let value = int_dict(receptors, |sample| {
            let metrics_by_receptor = sample
                .into_iter()
                .map(|(metric, samples)| {
                    let representative = median(&samples);
                    accumulated.entry(metric).or_default().extend(samples);
                    (metric, representative)
                })
                .collect();
            metrics(metrics_by_receptor)
        });
        entries.push(CasaEntry {
            key: CasaKey::Int(time as i64),
            value,
        });
    }
    entries.push(CasaEntry {
        key: CasaKey::String("median".into()),
        value: metrics(
            accumulated
                .into_iter()
                .map(|(name, samples)| (name, median(&samples)))
                .collect(),
        ),
    });
    CasaValue::Dict { entries }
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

fn metrics(values: Metrics) -> CasaValue {
    CasaValue::dict(
        values
            .into_iter()
            .map(|(key, value)| (key, CasaValue::float(value))),
    )
}
