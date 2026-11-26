// SPDX-License-Identifier: LGPL-3.0-or-later
//! PlotMS panel order and page names for calibration metadata axes.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_task_runtime::{TaskFailure, TaskInvocation};
use casa_types::quanta::format::{TIME, format_time};

use super::PlotSeries;
use super::axis::SampleCoordinates;
use super::table_data::CalibrationTable;

pub(super) struct PlotPanels {
    pub(super) grid: [u32; 2],
    pub(super) panels: Vec<Panel>,
    axis: Option<IterationAxis>,
    all_pages: bool,
}

pub(super) struct Panel {
    pub(super) label: String,
    pub(super) series: Vec<PlotSeries>,
    pub(super) overlay: Vec<PlotSeries>,
}

#[derive(Clone, Copy)]
pub(super) enum IterationAxis {
    Scan,
    Field,
    Spw,
    Time,
    Antenna,
    Baseline,
    Polarization,
    AntennaPosition,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Key {
    Id(i32),
    Antennas([i32; 2]),
    Time(u64),
}

impl IterationAxis {
    pub(super) fn from_task(task: &TaskInvocation<'_>) -> Result<Option<Self>, TaskFailure> {
        task.text("iteraxis")?
            .filter(|value| !value.is_empty())
            .map(|axis| Self::parse(&axis))
            .transpose()
    }

    fn parse(name: &str) -> Result<Self, TaskFailure> {
        Ok(match name.to_ascii_lowercase().as_str() {
            "scan" => Self::Scan,
            "field" => Self::Field,
            "spw" => Self::Spw,
            "time" => Self::Time,
            "antenna" => Self::Antenna,
            "baseline" => Self::Baseline,
            "poln" | "correlation" | "corr" | "polarization" => Self::Polarization,
            "antpos" => Self::AntennaPosition,
            _ => {
                return Err(TaskFailure::validation(format!(
                    "unsupported calibration iteration axis {name:?}"
                )));
            }
        })
    }

    fn label(self) -> &'static str {
        match self {
            Self::Scan => "Scan",
            Self::Field => "Field",
            Self::Spw => "Spw",
            Self::Time => "Time",
            Self::Antenna => "Antenna",
            Self::Baseline => "Baseline",
            Self::Polarization => "Poln",
            Self::AntennaPosition => "Antenna Position",
        }
    }

    fn key(self, coordinates: &SampleCoordinates) -> Key {
        match self {
            Self::Scan => Key::Id(coordinates.scan),
            Self::Field => Key::Id(coordinates.field),
            Self::Spw => Key::Id(coordinates.window),
            Self::Time => Key::Time(coordinates.time.to_bits()),
            Self::Antenna => Key::Id(coordinates.antennas[0]),
            Self::Baseline => {
                let [a, b] = coordinates.antennas;
                Key::Antennas([a.min(b), a.max(b)])
            }
            Self::Polarization | Self::AntennaPosition => Key::Id(coordinates.receptor),
        }
    }

    fn name(self, key: Key, table: &CalibrationTable, coordinates: &SampleCoordinates) -> String {
        let antenna = |id| {
            usize::try_from(id)
                .ok()
                .and_then(|id| table.antennas.get(id))
                .map_or_else(|| "*".to_string(), |antenna| antenna.label())
        };
        match key {
            Key::Id(id) if matches!(self, Self::Antenna) => antenna(id),
            Key::Id(id) if matches!(self, Self::Field) => usize::try_from(id)
                .ok()
                .and_then(|id| table.fields.get(id))
                .map_or_else(|| id.to_string(), |field| field.name.clone()),
            Key::Id(id)
                if matches!(self, Self::AntennaPosition | Self::Polarization)
                    && table.position_parameters() =>
            {
                CalibrationTable::position_label(id).to_owned()
            }
            Key::Id(id) if matches!(self, Self::Polarization) && id < 0 => "ratio".into(),
            Key::Id(id) => id.to_string(),
            Key::Antennas(_) => {
                let [a, b] = coordinates.antennas;
                let a_label = antenna(a);
                let b_label = antenna(b);
                let position = |id| {
                    usize::try_from(id)
                        .ok()
                        .and_then(|id| table.antennas.get(id))
                        .map(|antenna| antenna.position)
                };
                let length = match (position(a), position(b)) {
                    (Some(first), Some(second)) => {
                        let distance = first
                            .into_iter()
                            .zip(second)
                            .map(|(a, b)| (a - b).powi(2))
                            .sum::<f64>()
                            .sqrt();
                        format!("_{distance:.0}m")
                    }
                    _ => String::new(),
                };
                format!("{a_label} & {b_label}{length}")
            }
            Key::Time(bits) => format_time(f64::from_bits(bits) / 86_400.0, TIME, 10),
        }
    }
}

impl PlotPanels {
    pub(super) fn prepare(
        task: &TaskInvocation<'_>,
        table: &CalibrationTable,
        series: Vec<PlotSeries>,
        axis: Option<IterationAxis>,
    ) -> Result<Self, TaskFailure> {
        let count = |name| {
            u32::try_from(task.integer(name)?)
                .ok()
                .filter(|&count| count > 0)
                .ok_or_else(|| {
                    TaskFailure::validation(format!("{name} requires a positive grid size"))
                })
        };
        let grid = [count("gridrows")?, count("gridcols")?];
        if grid[0].checked_mul(grid[1]).is_none() {
            return Err(TaskFailure::validation("PlotMS panel grid is too large"));
        }
        let all_pages = task.text("exprange")?.is_some_and(|value| value == "all");
        let panels = if let Some(axis) = axis {
            split(table, axis, series)
        } else {
            vec![Panel {
                label: String::new(),
                series,
                overlay: Vec::new(),
            }]
        };
        Ok(Self {
            grid,
            panels,
            axis,
            all_pages,
        })
    }

    pub(super) fn capacity(&self) -> usize {
        self.grid[0] as usize * self.grid[1] as usize
    }

    pub(super) fn axis_name(&self) -> Option<&'static str> {
        self.axis.map(IterationAxis::label)
    }

    pub(super) fn pages(&self) -> impl Iterator<Item = &[Panel]> {
        let pages = if self.all_pages {
            self.panels.len().div_ceil(self.capacity())
        } else {
            1
        };
        self.panels.chunks(self.capacity()).take(pages)
    }

    pub(super) fn filename(&self, destination: &Path, page: usize, panels: &[Panel]) -> PathBuf {
        let Some(axis) = self.axis_name() else {
            return destination.to_owned();
        };
        crate::ms::msexplore::plotms_figure::iteration_filename(
            destination,
            axis,
            page,
            panels.iter().map(|panel| panel.label.as_str()),
        )
    }
}

fn split(table: &CalibrationTable, axis: IterationAxis, series: Vec<PlotSeries>) -> Vec<Panel> {
    let axes = series
        .iter()
        .map(|series| (series.x, series.y))
        .collect::<Vec<_>>();
    let empty_series = || {
        axes.iter()
            .map(|&(x, y)| PlotSeries {
                x,
                y,
                points: Vec::new(),
            })
            .collect()
    };
    let mut groups = BTreeMap::new();
    for (index, source) in series.into_iter().enumerate() {
        for point in source.points {
            let key = axis.key(&point.coordinates);
            let group: &mut Panel = groups.entry(key).or_insert_with(|| Panel {
                label: axis.name(key, table, &point.coordinates),
                series: empty_series(),
                overlay: Vec::new(),
            });
            group.series[index].points.push(point);
        }
    }
    if groups.is_empty() {
        vec![Panel {
            label: String::new(),
            series: empty_series(),
            overlay: Vec::new(),
        }]
    } else {
        groups.into_values().collect()
    }
}
