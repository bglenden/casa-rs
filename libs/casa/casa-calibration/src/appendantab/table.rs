// SPDX-License-Identifier: LGPL-3.0-or-later
//! Publish ANTAB records through casacore-compatible MS subtables.

use std::collections::HashMap;
use std::path::Path;

use casa_core::{ArrayValue, PrimitiveType, RecordField, ScalarValue, Value};
use casa_tables::{
    ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema, TableWriteLock,
    strip_directory,
};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::compute::{Gain, Metadata, Tsys, message};
use super::copy::Subtables;
use crate::solve::{get_f64, get_i32};
use crate::writer::{set_fixed_unit_keyword, set_measinfo_keyword};

type Keys = HashMap<(i32, u64), Vec<(i32, usize)>>;

pub(super) fn write(
    destination: &Path,
    paths: &Subtables,
    metadata: &Metadata,
    gains: Option<&[Gain]>,
    temperatures: Option<&[Tsys]>,
    overwrite: bool,
) -> Result<(), String> {
    if gains.is_none() && temperatures.is_none() {
        return Ok(());
    }
    let gain_path = &paths.gain;
    let tsys_path = &paths.tsys;
    let mut gain_table = (gain_path.exists() || gains.is_some())
        .then(|| open_or_create(gain_path, "GAIN_CURVE"))
        .transpose()?;
    let gain_keys = gain_table
        .as_ref()
        .map(index_existing)
        .transpose()?
        .unwrap_or_default();
    if let Some(temperatures) = temperatures {
        let mut table = open_or_create(tsys_path, "SYSCAL")?;
        if table
            .schema()
            .is_some_and(|schema| schema.column("TSYS").is_none())
        {
            table
                .add_column(
                    ColumnSchema::array_variable("TSYS", PrimitiveType::Float32, None),
                    None,
                )
                .map_err(message)?;
            set_fixed_unit_keyword(&mut table, "TSYS", &["K"]);
        }
        write_temperatures(&mut table, temperatures, &gain_keys, overwrite)?;
        save_subtable(&table, tsys_path)?;
    }
    if let (Some(gains), Some(table)) = (gains, gain_table.as_mut()) {
        write_gains(table, metadata, gains, &gain_keys, overwrite)?;
        save_subtable(table, gain_path)?;
    }
    if gains.is_some() || temperatures.is_some() {
        let mut lock = TableWriteLock::acquire_to_reread(destination, 1).map_err(message)?;
        let mut main =
            Table::open_metadata_only(TableOptions::new(destination)).map_err(message)?;
        if gain_path.is_dir() {
            main.keywords_mut().upsert(
                "GAIN_CURVE",
                Value::table_ref(strip_directory(gain_path, destination)),
            );
        }
        if tsys_path.is_dir() {
            main.keywords_mut().upsert(
                "SYSCAL",
                Value::table_ref(strip_directory(tsys_path, destination)),
            );
        }
        lock.record_write();
        main.save_metadata_only(TableOptions::new(destination))
            .map_err(message)?;
        lock.release().map_err(message)?;
    }
    Ok(())
}

fn save_subtable(table: &Table, path: &Path) -> Result<(), String> {
    let lock = path
        .is_dir()
        .then(|| TableWriteLock::acquire_to_reread(path, 1))
        .transpose()
        .map_err(message)?;
    // Use CASA's writable layout so casacore bulk column edits survive reopening.
    let options = TableOptions::new(path)
        .with_data_manager(DataManagerKind::StandardStMan)
        .with_casacore_default_ssm(true);
    table
        .save_with_write_lock(options, lock.as_ref())
        .map_err(message)?;
    if let Some(lock) = lock {
        lock.release().map_err(message)?;
    }
    Ok(())
}

fn index_existing(table: &Table) -> Result<Keys, String> {
    let mut keys = Keys::new();
    for row in 0..table.row_count() {
        let antenna = get_i32(table, row, "ANTENNA_ID").map_err(message)?;
        let window = get_i32(table, row, "SPECTRAL_WINDOW_ID").map_err(message)?;
        let time = get_f64(table, row, "TIME").map_err(message)?;
        keys.entry((antenna, time.to_bits()))
            .or_default()
            .push((window, row));
    }
    Ok(keys)
}

fn write_temperatures(
    table: &mut Table,
    rows: &[Tsys],
    existing_gain: &Keys,
    overwrite: bool,
) -> Result<(), String> {
    let existing = overwrite
        .then(|| index_existing(table))
        .transpose()?
        .unwrap_or_default();
    for row in rows {
        let key = (row.antenna, row.time.to_bits());
        if !overwrite && existing_gain.contains_key(&key) {
            // CASA checks the GAIN_CURVE antenna/time index on append.
            continue;
        }
        let mut values = identifying(row.antenna, 0, row.window, row.time, 0.);
        values.push(array("TSYS", &[2], row.temperatures.to_vec()));
        let updates = existing
            .get(&key)
            .into_iter()
            .flatten()
            .filter_map(|&(window, row_index)| (window == row.window).then_some(row_index))
            .collect::<Vec<_>>();
        if updates.is_empty() {
            append_row(table, values)?;
        } else {
            for row in updates {
                overwrite_row(table, row, &values)?;
            }
        }
    }
    Ok(())
}

fn write_gains(
    table: &mut Table,
    metadata: &Metadata,
    gains: &[Gain],
    existing: &Keys,
    overwrite: bool,
) -> Result<(), String> {
    for gain in gains {
        if let Some(rows) = existing.get(&(gain.antenna, metadata.gain_time.to_bits())) {
            if overwrite && let Some([low, high]) = gain.frequencies {
                for &(window, row) in rows {
                    let frequencies = metadata.frequencies.get(window as usize);
                    if frequencies.is_some_and(|frequencies| {
                        !frequencies.is_empty()
                            && frequencies
                                .iter()
                                .all(|&frequency| (low..=high).contains(&(frequency * 1e-6)))
                    }) {
                        overwrite_row(table, row, &gain_fields(metadata, gain, window))?;
                    }
                }
            }
            continue;
        }
        for &window in &metadata.spw_ids {
            append_row(table, gain_fields(metadata, gain, window))?;
        }
    }
    Ok(())
}

fn gain_fields(metadata: &Metadata, gain: &Gain, window: i32) -> Vec<RecordField> {
    let mut fields = identifying(
        gain.antenna,
        -1,
        window,
        metadata.gain_time,
        metadata.gain_interval,
    );
    let coefficients = gain
        .coefficients
        .iter()
        .flat_map(|&coefficient| [coefficient; 2])
        .collect();
    fields.extend([
        string("TYPE", gain.kind),
        integer("NUM_POLY", gain.coefficients.len() as i32),
        array("GAIN", &[2, gain.coefficients.len()], coefficients),
        array("SENSITIVITY", &[2], gain.sensitivities.to_vec()),
    ]);
    fields
}

fn overwrite_row(table: &mut Table, row: usize, fields: &[RecordField]) -> Result<(), String> {
    for field in fields {
        table
            .cell_accessor_mut(row, &field.name)
            .and_then(|mut cell| cell.set(field.value.clone()))
            .map_err(message)?;
    }
    Ok(())
}

fn append_row(table: &mut Table, fields: Vec<RecordField>) -> Result<(), String> {
    let mut row = table
        .schema()
        .ok_or("ANTAB subtable has no schema")?
        .default_row();
    for field in fields {
        row.upsert(field.name, field.value);
    }
    table.add_row(row).map_err(message)
}

fn open_or_create(path: &Path, name: &str) -> Result<Table, String> {
    if path.exists() {
        // The staging tree must own any subtable it edits.
        if path.symlink_metadata().map_err(message)?.is_symlink() {
            return Err(format!(
                "calibration subtable {} is a symbolic link",
                path.display()
            ));
        }
        return Table::open(TableOptions::new(path)).map_err(message);
    }
    let int =
        |column, label| ColumnSchema::scalar(column, PrimitiveType::Int32).with_comment(label);
    let double =
        |column, label| ColumnSchema::scalar(column, PrimitiveType::Float64).with_comment(label);
    let float_array = |column, label| {
        ColumnSchema::array_variable(column, PrimitiveType::Float32, None).with_comment(label)
    };
    let mut columns = vec![
        int("ANTENNA_ID", "Antenna identifier"),
        int("FEED_ID", "Feed identifier"),
        double(
            "INTERVAL",
            "Interval for which this set of parameters is accurate",
        ),
        int(
            "SPECTRAL_WINDOW_ID",
            if name == "SYSCAL" {
                "ID for this spectral window setup"
            } else {
                "Spectral window identifier"
            },
        ),
        double(
            "TIME",
            "Midpoint of time for which this set of parameters is accurate",
        ),
    ];
    if name == "SYSCAL" {
        columns.push(float_array(
            "TSYS",
            "System temp. for each of the two receptors",
        ));
    } else {
        columns.insert(2, float_array("GAIN", "Gain polynomial"));
        columns.insert(4, int("NUM_POLY", "Number of terms in polynomial"));
        columns.insert(5, float_array("SENSITIVITY", "Antenna sensitivity"));
        columns.push(
            ColumnSchema::scalar("TYPE", PrimitiveType::String).with_comment("Gain curve type"),
        );
    }
    let mut table = Table::with_schema(TableSchema::new(columns).map_err(message)?);
    set_fixed_unit_keyword(&mut table, "TIME", &["s"]);
    set_measinfo_keyword(&mut table, "TIME", "epoch", Some("UTC"));
    set_fixed_unit_keyword(&mut table, "INTERVAL", &["s"]);
    if name == "SYSCAL" {
        set_fixed_unit_keyword(&mut table, "TSYS", &["K"]);
    } else {
        set_fixed_unit_keyword(&mut table, "SENSITIVITY", &["K/Jy"]);
    }
    Ok(table)
}

fn identifying(antenna: i32, feed: i32, window: i32, time: f64, interval: f64) -> Vec<RecordField> {
    vec![
        integer("ANTENNA_ID", antenna),
        integer("FEED_ID", feed),
        double("INTERVAL", interval),
        integer("SPECTRAL_WINDOW_ID", window),
        double("TIME", time),
    ]
}

fn integer(name: &str, value: i32) -> RecordField {
    RecordField::new(name, Value::Scalar(ScalarValue::Int32(value)))
}

fn double(name: &str, value: f64) -> RecordField {
    RecordField::new(name, Value::Scalar(ScalarValue::Float64(value)))
}

fn string(name: &str, value: &str) -> RecordField {
    RecordField::new(name, Value::Scalar(ScalarValue::String(value.into())))
}

fn array(name: &str, shape: &[usize], values: Vec<f32>) -> RecordField {
    let values =
        ArrayD::from_shape_vec(IxDyn(shape).f(), values).expect("correct ANTAB array dimensions");
    RecordField::new(name, Value::Array(ArrayValue::Float32(values)))
}
