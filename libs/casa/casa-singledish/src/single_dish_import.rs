// SPDX-License-Identifier: LGPL-3.0-or-later
//! Conversion of single-dish instrument formats into MeasurementSet v2.

mod asap;
mod metadata;
mod nro;
mod polarization;
mod writer;

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use casa_core::Value;
use casa_tables::{Table, TableOptions};

pub use asap::read_asap;
pub use nro::read_nro;
use polarization::Basis;

use casa_ms::{MeasurementSet, MsError, MsResult};

/// A decoded single-dish scan and the casacore tables it depends on.
pub struct ImportedScanTable {
    /// Spectra and metadata in MeasurementSet v2 form.
    pub measurement_set: MeasurementSet,
    /// The source files and linked tables to protect during publication.
    pub input_tables: Vec<PathBuf>,
    auxiliary_tables: Vec<(String, Table)>,
}

impl ImportedScanTable {
    /// Write the MeasurementSet and instrument-specific tables into a staging directory.
    ///
    /// # Errors
    /// Returns a storage error when any table cannot be written. The caller
    /// can discard the stage before publishing it.
    pub fn write_stage(&mut self, path: &Path) -> MsResult<()> {
        for (name, _) in &self.auxiliary_tables {
            self.measurement_set
                .main_table_mut()
                .keywords_mut()
                .upsert(name, Value::TableRef(format!("././{name}")));
        }
        self.measurement_set.save_as_standard(path)?;
        for (name, table) in &self.auxiliary_tables {
            table.save(TableOptions::new(path.join(name)))?;
        }
        Ok(())
    }
}

struct Antenna {
    name: String,
    station: String,
    position: [f64; 3],
    diameter: f64,
}

fn antenna_diameter(name: &str) -> f64 {
    let name = name.to_ascii_uppercase();
    let bare = name.trim_end_matches(|c: char| c.is_ascii_digit());
    if bare.len() != name.len() {
        if ["DV", "DA", "PM"]
            .iter()
            .any(|suffix| bare.ends_with(suffix))
        {
            return 12.0;
        }
        if bare.ends_with("CM") {
            return 7.0;
        }
    }
    [
        ("GBT", 104.9),
        ("MOPRA", 22.0),
        ("PKS", 64.0),
        ("PARKS", 64.0),
        ("TIDBINBILLA", 70.0),
        ("CEDUNA", 30.0),
        ("HOBART", 26.0),
        ("APEX", 12.0),
        ("ASTE", 10.0),
        ("NRO", 45.0),
    ]
    .into_iter()
    .find_map(|(marker, size)| name.contains(marker).then_some(size))
    .unwrap_or_default()
}

struct Window {
    channels: usize,
    frame: i32,
    reference_channel: f64,
    reference_frequency: f64,
    increment: f64,
}

struct ImportHeader {
    antenna: Vec<Antenna>,
    direction_frame: &'static str,
    telescope: String,
    observer: String,
    project: String,
    range: [f64; 2],
    data_unit: String,
    windows: BTreeMap<i32, Window>,
}

struct Field {
    id: i32,
    name: String,
    source_id: i32,
    time: f64,
    direction: Direction,
}

struct Source {
    id: i32,
    name: String,
    window: i32,
    time: f64,
    interval: f64,
    direction: [f64; 2],
    proper_motion: [f64; 2],
    rest_frequencies: Vec<f64>,
    transitions: Vec<String>,
    velocity: f64,
}

#[derive(Clone, Copy)]
struct Direction {
    position: [f64; 2],
    rate: [f64; 2],
}

#[derive(Clone, Copy, PartialEq)]
struct Weather([f32; 5]);

struct Integration {
    time: f64,
    interval: f64,
    scan: i32,
    antenna: i32,
    field: i32,
    feed: i32,
    window: i32,
    basis: Basis,
    state: (&'static str, i32),
    direction: Direction,
    weather: Option<Weather>,
}

struct Plane {
    index: usize,
    data: Vec<f32>,
    flag: Option<Vec<bool>>,
    flag_row: bool,
    system_temperature: Vec<f32>,
    calibration_temperature: Vec<f32>,
}

struct RawIntegration {
    row: Integration,
    plane: Plane,
}

type IntegrationKey = (i32, i32, i32, i32, Basis, &'static str);

#[derive(Default)]
struct TimeGroups {
    order: HashMap<IntegrationKey, usize>,
    current: BTreeMap<usize, (Integration, BTreeMap<usize, Plane>)>,
}

impl TimeGroups {
    fn push(&mut self, input: RawIntegration) -> MsResult<()> {
        let RawIntegration { row, plane } = input;
        let key = (
            row.antenna,
            row.field,
            row.feed,
            row.window,
            row.basis,
            row.state.0,
        );
        let next = self.order.len();
        let rank = *self.order.entry(key).or_insert(next);
        match self.current.entry(rank) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert((row, BTreeMap::from([(plane.index, plane)])));
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let (coordinates, planes) = entry.get_mut();
                if let std::collections::btree_map::Entry::Vacant(hand) = planes.entry(plane.index)
                {
                    hand.insert(plane);
                    *coordinates = row;
                } else {
                    return Err(MsError::InvalidInput(format!(
                        "Duplicate polarization plane {} at time {}, feed {}, window {}",
                        plane.index, row.time, row.feed, row.window,
                    )));
                }
            }
        }
        Ok(())
    }
}
