// SPDX-License-Identifier: LGPL-3.0-or-later
//! Index selected visibility rows into antenna, baseline and time panels.

use std::collections::{BTreeMap, HashMap};

use casa_types::quanta::MvTime;

use crate::MeasurementSet;
use crate::subtables::MsAntenna;

use super::{MsIterationAxis, MsScatterPanelPayload, MsScatterSeries, PointBudget};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PanelId {
    Antenna(i32),
    Baseline(i32, i32),
    Time(usize),
}

impl PanelId {
    fn label(self, antenna: &MsAntenna<'_>, times: &[f64]) -> Result<(String, String), String> {
        let station_name = |id| {
            if let Ok(row) = usize::try_from(id) {
                let name = antenna.name(row).map_err(|error| error.to_string())?;
                let station = antenna.station(row).map_err(|error| error.to_string())?;
                Ok(format!("{name}@{station}"))
            } else {
                Ok::<_, String>("*".into())
            }
        };
        Ok(match self {
            Self::Antenna(id) => (format!("antenna-{id}"), station_name(id)?),
            Self::Baseline(left, right) => {
                let mut label = format!("{} & {}", station_name(left)?, station_name(right)?);
                if let (Ok(left), Ok(right)) = (usize::try_from(left), usize::try_from(right)) {
                    let a = antenna.position(left).map_err(|error| error.to_string())?;
                    let b = antenna.position(right).map_err(|error| error.to_string())?;
                    let distance = a
                        .into_iter()
                        .zip(b)
                        .map(|(a, b)| (a - b).powi(2))
                        .sum::<f64>()
                        .sqrt();
                    label.push_str(&format!("_{distance:.0}m"));
                }
                (format!("baseline-{left}-{right}"), label)
            }
            Self::Time(slot) => (
                format!("time-{slot}"),
                MvTime::from_mjd_seconds(times[slot]).format_time(4),
            ),
        })
    }
}

pub(super) fn partition(
    ms: &MeasurementSet,
    axis: MsIterationAxis,
    rows: &[usize],
    [antenna1, antenna2]: [&[i32]; 2],
    times: &[f64],
    series: Vec<MsScatterSeries>,
    point_budget: &mut PointBudget,
) -> Result<Vec<MsScatterPanelPayload>, String> {
    let mut first_time_slots = HashMap::new();
    let membership: HashMap<_, _> = rows
        .iter()
        .copied()
        .enumerate()
        .map(|(slot, row)| {
            let left = antenna1[slot];
            let right = antenna2[slot];
            let ids = match axis {
                MsIterationAxis::Antenna => [
                    (left >= 0).then_some(PanelId::Antenna(left)),
                    (right >= 0 && right != left).then_some(PanelId::Antenna(right)),
                ],
                MsIterationAxis::Baseline => [Some(PanelId::Baseline(left, right)), None],
                MsIterationAxis::Time => {
                    let first = *first_time_slots
                        .entry(times[slot].to_bits())
                        .or_insert(slot);
                    [Some(PanelId::Time(first)), None]
                }
                _ => unreachable!("only row-based iteration uses row membership"),
            };
            (row, ids)
        })
        .collect();
    let mut panels = membership
        .values()
        .copied()
        .flatten()
        .flatten()
        .map(|id| (id, Vec::new()))
        .collect::<BTreeMap<_, _>>();
    for source in series {
        let mut split = BTreeMap::new();
        for (index, point) in source.provenance.iter().enumerate() {
            let ids = membership[&point.row];
            if ids[0].is_some() && ids[1].is_some() {
                point_budget.record_points(1, axis.display_name())?;
            }
            for id in ids.into_iter().flatten() {
                let target = split.entry(id).or_insert_with(|| MsScatterSeries {
                    label: source.label.clone(),
                    color_group: source.color_group.clone(),
                    y_axis: source.y_axis,
                    y_index: source.y_index,
                    points: Vec::new(),
                    provenance: Vec::new(),
                    plotms_metadata: Vec::new(),
                    plotms_reporting_frequencies_hz: Vec::new(),
                    plotms_averaged_ids: None,
                });
                target.points.push(source.points[index]);
                target.provenance.push(point.clone());
                target
                    .plotms_metadata
                    .extend(source.plotms_metadata.get(index).copied());
                target
                    .plotms_reporting_frequencies_hz
                    .extend(source.plotms_reporting_frequencies_hz.get(index).copied());
            }
        }
        for (id, series) in split {
            panels
                .get_mut(&id)
                .expect("selected iteration panel")
                .push(series);
        }
    }
    let antenna = ms.antenna().map_err(|error| error.to_string())?;
    panels
        .into_iter()
        .map(|(id, series)| {
            let (key, label) = id.label(&antenna, times)?;
            Ok(MsScatterPanelPayload {
                summary: format!(
                    "{label}. Points={}.",
                    series.iter().map(|entry| entry.points.len()).sum::<usize>()
                ),
                key,
                label,
                series,
            })
        })
        .collect()
}
