// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native decoder for NRO 45 m NOSTAR scan exports.

mod binary;
mod header;
mod scan;

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use casa_tables::{ColumnSchema, Table, TableSchema};
use casa_types::{PrimitiveType, RecordField, RecordValue};

use self::binary::{OBSERVATION_HEADER_BYTES, Record, invalid};
use self::header::Header;
use super::{
    Antenna, Direction, Field, ImportHeader, ImportedScanTable, RawIntegration, Source, TimeGroups,
    Window, antenna_diameter, metadata, writer::Writer,
};
use crate::subtable_rows::i;
use crate::{MsError, MsResult, OptionalMainColumn};

const DECODE_BATCH_BYTES: usize = 2 * 1024 * 1024;

/// Convert the observation and scans of a NOSTAR file to MeasurementSet v2.
///
/// Independent scan records can be decoded in parallel, with integration and
/// metadata order preserved. The result includes the NRO_ARRAY mapping.
///
/// # Errors
/// Returns a format error for incomplete records and invalid instrument
/// coordinates, axes, or array mappings.
pub fn read_nro(path: &Path, parallel: bool) -> MsResult<ImportedScanTable> {
    let file = File::open(path).map_err(io_error)?;
    let size = file.metadata().map_err(io_error)?.len();
    let mut input = BufReader::new(file);
    let mut bytes = [0; OBSERVATION_HEADER_BYTES];
    input.read_exact(&mut bytes).map_err(io_error)?;
    let header = Header::read(&bytes, size)?;
    let mut record_bytes = vec![0; header.record_bytes];
    let (setup, fields, sources) = setup(&mut input, &mut record_bytes, &header)?;
    let column = if header.arrays[..header.n_arrays]
        .iter()
        .flatten()
        .any(|array| array.plane == 3)
    {
        OptionalMainColumn::Data
    } else {
        OptionalMainColumn::FloatData
    };
    let ms = metadata::create(&setup, column, &fields, &sources)?;
    let mut writer = Writer::new(ms, column);
    let mut groups = TimeGroups::default();
    let mut time = None;
    let total = header.n_arrays * header.n_scans;
    let batch_size = (DECODE_BATCH_BYTES / header.record_bytes).max(1);
    let mut buffer = vec![0; total.min(batch_size) * header.record_bytes];
    input
        .seek(SeekFrom::Start(OBSERVATION_HEADER_BYTES as u64))
        .map_err(io_error)?;
    for start in (0..total).step_by(batch_size) {
        let count = (total - start).min(batch_size);
        let batch = &mut buffer[..count * header.record_bytes];
        input.read_exact(batch).map_err(io_error)?;
        for sample in decode_batch(batch, &header, parallel)? {
            if time != Some(sample.row.time) {
                for (row, planes) in std::mem::take(&mut groups.current).into_values() {
                    writer.write(&row, planes, header.n_channels)?;
                }
                time = Some(sample.row.time);
            }
            groups.push(sample)?;
        }
    }
    for (row, planes) in groups.current.into_values() {
        writer.write(&row, planes, header.n_channels)?;
    }
    Ok(ImportedScanTable {
        measurement_set: writer.finish()?,
        input_tables: vec![path.to_path_buf()],
        auxiliary_tables: vec![(String::from("NRO_ARRAY"), array_table(&header)?)],
    })
}

fn setup(
    input: &mut BufReader<File>,
    bytes: &mut [u8],
    header: &Header,
) -> MsResult<(ImportHeader, Vec<Field>, Vec<Source>)> {
    let start = scan::start_time(read_record(input, bytes, header, 0)?)?;
    let last = (header.n_scans - 1) * header.n_arrays;
    let end = scan::start_time(read_record(input, bytes, header, last)?)? + header.interval;
    let mid = 0.5 * (start + end);
    let diameter = antenna_diameter(&header.site);
    let antenna = (0..header.n_beam)
        .map(|beam| Antenna {
            name: format!("{}-BEAM{beam}", header.site),
            station: String::new(),
            position: [-3_871_023.5, 3_428_106.8, 3_724_039.5],
            diameter,
        })
        .collect();
    let mut windows = BTreeMap::new();
    let mut sources = Vec::new();
    for id in 0..header.n_windows as i32 {
        let (row, array) = header.arrays[..header.n_arrays]
            .iter()
            .enumerate()
            .find_map(|(row, array)| {
                array
                    .filter(|array| array.window == id)
                    .map(|array| (row, array))
            })
            .ok_or_else(|| invalid("missing spectral window"))?;
        let scan = read_record(input, bytes, header, row)?;
        let rest_frequency = scan.f64(184);
        let frequency_offset = rest_frequency - array.rest;
        let mut frequencies = array
            .frequency
            .map(|value| (value + frequency_offset) * header.doppler_factor);
        let mut step = (frequencies[1] - frequencies[0]) / (array.channel[1] - array.channel[0]);
        if !step.is_finite() {
            return Err(invalid("spectral calibration"));
        }
        if scan.f64(200) > 0. {
            frequencies.swap(0, 1);
            step = -step;
        }
        windows.insert(
            id,
            Window {
                channels: header.n_channels,
                frame: header.frequency_frame,
                reference_channel: array.channel[0] - 1.,
                reference_frequency: frequencies[0],
                increment: step,
            },
        );
        sources.push(Source {
            id: 0,
            name: header.object.clone(),
            window: id,
            time: mid,
            interval: end - start,
            direction: header.direction,
            proper_motion: [0.; 2],
            rest_frequencies: vec![rest_frequency],
            transitions: Vec::new(),
            velocity: header.velocity,
        });
    }
    let field = Field {
        id: 0,
        name: header.object.clone(),
        source_id: 0,
        time: mid,
        direction: Direction {
            position: header.direction,
            rate: [0.; 2],
        },
    };
    let meta = ImportHeader {
        antenna,
        direction_frame: header.direction_frame,
        telescope: header.site.clone(),
        observer: header.observer.clone(),
        project: header.project.clone(),
        range: [start, end],
        data_unit: String::from("K"),
        windows,
    };
    Ok((meta, vec![field], sources))
}

fn read_record<'a>(
    input: &mut BufReader<File>,
    bytes: &'a mut [u8],
    header: &Header,
    row: usize,
) -> MsResult<Record<'a>> {
    let offset = OBSERVATION_HEADER_BYTES + row * header.record_bytes;
    input
        .seek(SeekFrom::Start(offset as u64))
        .map_err(io_error)?;
    input.read_exact(bytes).map_err(io_error)?;
    Ok(Record::new(bytes, header.order))
}

fn decode_batch(bytes: &[u8], header: &Header, parallel: bool) -> MsResult<Vec<RawIntegration>> {
    let decode = |block: &[u8]| {
        let mut previous_stamp = None;
        let mut time = 0.;
        block
            .chunks_exact(header.record_bytes)
            .map(|bytes| {
                let record = Record::new(bytes, header.order);
                let stamp = record.text(8, 24)?;
                if previous_stamp != Some(stamp) {
                    time = scan::start_time(record)?;
                    previous_stamp = Some(stamp);
                }
                scan::decode(record, header, time)
            })
            .collect::<MsResult<Vec<_>>>()
    };
    if !parallel {
        return decode(bytes);
    }
    let workers = std::thread::available_parallelism()
        .map_or(2, usize::from)
        .clamp(2, 8);
    let group = (bytes.len() / header.record_bytes).div_ceil(workers) * header.record_bytes;
    std::thread::scope(|scope| {
        let tasks = bytes
            .chunks(group)
            .map(|group| scope.spawn(move || decode(group)))
            .collect::<Vec<_>>();
        let mut result = Vec::with_capacity(bytes.len() / header.record_bytes);
        for task in tasks {
            result.extend(task.join().map_err(|_| invalid("parallel reader"))??);
        }
        Ok(result)
    })
}

fn array_table(header: &Header) -> MsResult<Table> {
    let names = ["ARRAY", "BEAM", "POLARIZATION", "SPECTRAL_WINDOW"];
    let schema = TableSchema::new(
        names
            .into_iter()
            .map(|column| ColumnSchema::scalar(column, PrimitiveType::Int32))
            .collect(),
    )?;
    let mut table = Table::with_schema(schema);
    for (index, array) in header.arrays.iter().enumerate() {
        let ids = array.map_or([-1; 3], |array| [array.beam, array.code, array.window]);
        let values = [index as i32, ids[0], ids[1], ids[2]];
        let row = RecordValue::new(
            names
                .into_iter()
                .zip(values)
                .map(|(name, id)| RecordField::new(name, i(id)))
                .collect(),
        );
        table.add_row(row)?;
    }
    Ok(table)
}

fn io_error(error: io::Error) -> MsError {
    MsError::InvalidInput(format!("Could not read a NOSTAR record: {error}"))
}
