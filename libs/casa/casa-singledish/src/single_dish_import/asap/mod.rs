// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read ATNF Spectral Analysis Package scan tables.

mod asap_row;
mod intent;
mod setup;
mod sources;

use std::collections::HashMap;
use std::path::Path;

use casa_core::{RecordValue, Value};
use casa_tables::{Table, TableOptions};

use self::asap_row::{doubles, flags, floats, integer, invalid, number, string, subtable};
use super::{
    Basis, Direction, ImportedScanTable, Integration, Plane, RawIntegration, TimeGroups, Weather,
    metadata, writer::Writer,
};
use casa_ms::{MsResult, OptionalMainColumn};

const DAY: f64 = 86400.0;

/// Read a CASA/ASAP scan table, combining its polarization planes into
/// single-dish integrations with the corresponding MeasurementSet metadata.
///
/// In parallel mode the independent scan-table records are decoded in
/// parallel. Integration and subtable order are the same in both modes.
///
/// # Errors
/// Returns a format error if the scan table cannot be read or contains an
/// unsupported polarization combination, missing cells or inconsistent axes.
pub fn read_asap(path: &Path, parallel: bool) -> MsResult<ImportedScanTable> {
    let table = Table::open(TableOptions::new(path))?;
    if table.row_count() == 0 {
        return Err(invalid("nonempty scan table"));
    }
    let rows = (0..table.row_count())
        .map(|index| table.row_accessor().row(index))
        .collect::<Result<Vec<_>, _>>()?;
    let keywords = table.keywords();
    let mut input_tables = if table.is_reference_table()? {
        table.cache_plain_table_paths()?
    } else {
        vec![path.to_path_buf()]
    };
    let owner = input_tables.first().map_or(path, |path| path.as_path());
    let setup = setup::header(owner, &table)?;
    let molecules = subtable(owner, keywords, "MOLECULES")?;
    let weather = subtable(owner, keywords, "WEATHER")?;
    let tcal = subtable(owner, keywords, "TCAL")?;
    let linked = keywords
        .fields()
        .iter()
        .filter_map(|field| match &field.value {
            Value::TableRef(reference) => Some(casa_tables::add_directory(reference, owner)),
            _ => None,
        })
        .collect::<Vec<_>>();
    input_tables.extend(linked);
    let (sources, names) = sources::split_sources(&rows, &molecules)?;
    let (fields, field_ids) = sources::fields(&rows, &names)?;
    let decoder = Decoder {
        weather: tag_rows(&weather)?,
        tcal: tag_rows(&tcal)?,
        field_ids,
        basis: Basis::parse(string(keywords, "POLTYPE")?)?,
    };
    let mut samples = decoder.decode(&rows, parallel)?;
    samples.sort_by(|a, b| {
        a.row
            .time
            .total_cmp(&b.row.time)
            .then_with(|| a.row.feed.cmp(&b.row.feed))
            .then_with(|| a.row.window.cmp(&b.row.window))
            .then_with(|| a.plane.index.cmp(&b.plane.index))
    });
    let cross_hands =
        decoder.basis.has_parallel_hands() && samples.iter().any(|sample| sample.plane.index == 3);
    let column = if cross_hands {
        OptionalMainColumn::Data
    } else {
        OptionalMainColumn::FloatData
    };
    let ms = metadata::create(&setup, column, &fields, &sources)?;
    let mut writer = Writer::new(ms, column);
    let mut groups = TimeGroups::default();
    let mut time = None;
    let mut weather = Weather([0.; 5]);
    for mut sample in samples {
        if time != Some(sample.row.time) {
            for (row, planes) in std::mem::take(&mut groups.current).into_values() {
                writer.write(&row, planes, setup.windows[&row.window].channels)?;
            }
            time = Some(sample.row.time);
        }
        weather = sample.row.weather.unwrap_or(weather);
        sample.row.weather = Some(weather);
        groups.push(sample)?;
    }
    for (row, planes) in groups.current.into_values() {
        writer.write(&row, planes, setup.windows[&row.window].channels)?;
    }
    Ok(ImportedScanTable {
        measurement_set: writer.finish()?,
        input_tables,
        auxiliary_tables: Vec::new(),
    })
}

struct Decoder<'a> {
    weather: HashMap<i32, &'a RecordValue>,
    tcal: HashMap<i32, &'a RecordValue>,
    field_ids: HashMap<String, i32>,
    basis: Basis,
}

impl Decoder<'_> {
    fn decode(&self, rows: &[&RecordValue], parallel: bool) -> MsResult<Vec<RawIntegration>> {
        let mut samples = if parallel {
            let workers = std::thread::available_parallelism()
                .map_or(2, usize::from)
                .clamp(2, 8);
            std::thread::scope(|scope| {
                let tasks = rows
                    .chunks(rows.len().div_ceil(workers))
                    .map(|chunk| scope.spawn(move || self.decode_chunk(chunk)))
                    .collect::<Vec<_>>();
                let mut result = Vec::with_capacity(rows.len());
                for task in tasks {
                    result.extend(task.join().map_err(|_| invalid("parallel reader"))??);
                }
                Ok::<_, casa_ms::MsError>(result)
            })?
        } else {
            self.decode_chunk(rows)?
        };
        samples.retain(|sample| {
            sample.row.time > 0.
                && sample.row.interval > 0.
                && sample.row.scan >= 0
                && sample.row.feed >= 0
                && sample.row.window >= 0
        });
        Ok(samples)
    }

    fn decode_chunk(&self, rows: &[&RecordValue]) -> MsResult<Vec<RawIntegration>> {
        rows.iter().map(|row| self.record(row)).collect()
    }

    fn record(&self, row: &RecordValue) -> MsResult<RawIntegration> {
        let weather = self
            .weather
            .get(&integer(row, "WEATHER_ID")?)
            .map(|record| read_weather(record))
            .transpose()?;
        let tcal = self
            .tcal
            .get(&integer(row, "TCAL_ID")?)
            .map(|record| floats(record, "TCAL"))
            .transpose()?
            .unwrap_or_default();
        let index = usize::try_from(integer(row, "POLNO")?).map_err(|_| invalid("POLNO"))?;
        Ok(RawIntegration {
            row: Integration {
                time: number(row, "TIME")? * DAY,
                interval: number(row, "INTERVAL")?,
                scan: integer(row, "SCANNO")?,
                antenna: 0,
                field: self.field_ids[string(row, "FIELDNAME")?],
                feed: integer(row, "BEAMNO")?,
                window: integer(row, "IFNO")?,
                basis: self.basis,
                state: intent::state(integer(row, "SRCTYPE")?),
                direction: direction(row, "DIRECTION")?,
                weather,
            },
            plane: Plane {
                index,
                data: floats(row, "SPECTRA")?,
                flag: Some(flags(row, "FLAGTRA")?),
                flag_row: integer(row, "FLAGROW")? != 0,
                system_temperature: floats(row, "TSYS").unwrap_or_default(),
                calibration_temperature: tcal,
            },
        })
    }
}

fn read_weather(record: &RecordValue) -> MsResult<Weather> {
    Ok(Weather([
        number(record, "TEMPERATURE")? as f32,
        number(record, "PRESSURE")? as f32,
        number(record, "HUMIDITY")? as f32,
        number(record, "WINDSPEED")? as f32,
        number(record, "WINDAZ")? as f32,
    ]))
}

fn direction(row: &RecordValue, name: &str) -> MsResult<Direction> {
    Ok(Direction {
        position: doubles(row, name)?.try_into().map_err(|_| invalid(name))?,
        rate: if row.get("SCANRATE").is_some() {
            doubles(row, "SCANRATE")?
                .try_into()
                .map_err(|_| invalid("SCANRATE"))?
        } else {
            [0.; 2]
        },
    })
}

fn tag_rows(table: &Table) -> MsResult<HashMap<i32, &RecordValue>> {
    (0..table.row_count())
        .map(|index| {
            let row = table.row_accessor().row(index)?;
            Ok((integer(row, "ID")?, row))
        })
        .collect()
}
