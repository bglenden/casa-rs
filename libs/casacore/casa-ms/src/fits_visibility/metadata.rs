// SPDX-License-Identifier: LGPL-3.0-or-later
//! Metadata for the AIPS extensions of a UVFITS file.

use std::collections::BTreeMap;
use std::ptr;

use casa_core::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use casa_tables::{ColumnSchema, Table, TableSchema};
use ndarray::Array1;

use fitsio::{
    FitsFile, errors,
    hdu::{FitsHdu, HduInfo},
    sys,
    tables::ReadsCol,
};

use super::groups::{GroupLayout, optional_key};
use super::invalid;
use crate::MsResult;

pub(super) struct Antenna {
    pub(super) number: i32,
    pub(super) station: String,
    pub(super) position: [f64; 3],
    pub(super) mount: i32,
    pub(super) axis_offset: f64,
    pub(super) angles: [f64; 2],
    pub(super) diameter: f64,
}

pub(super) struct Array {
    pub(super) name: String,
    pub(super) origin: [f64; 3],
    pub(super) left_handed: bool,
    pub(super) frame_defined: bool,
    pub(super) antennas: BTreeMap<i32, Antenna>,
    pub(super) date: String,
    pub(super) gst: f64,
    pub(super) degrees_per_day: f64,
    pub(super) time_system: String,
}

#[derive(Clone, Copy)]
pub(super) struct SpectralWindow {
    pub(super) frequency: f64,
    pub(super) increment: f64,
    pub(super) bandwidth: f64,
}

pub(super) struct Source {
    pub(super) name: String,
    pub(super) code: String,
    pub(super) epoch: f64,
    pub(super) direction_deg: [f64; 2],
    pub(super) apparent_deg: [f64; 2],
    pub(super) motion_per_day: [f64; 2],
    pub(super) rest_frequency: f64,
    pub(super) velocity: f64,
}

pub(super) struct Header {
    pub(super) telescope: String,
    pub(super) observer: String,
    pub(super) object: String,
    pub(super) epoch: f64,
    pub(super) date: String,
    pub(super) release_date: String,
    pub(super) frame: Option<String>,
    pub(super) rest_frequency: f64,
    pub(super) alternate_value: Option<f64>,
    pub(super) alternate_pixel: Option<f64>,
    pub(super) velocity_reference: Option<i32>,
    pub(super) history: Vec<String>,
}

impl Header {
    pub(super) fn read(fits: &mut FitsFile, binary_hdu: Option<usize>) -> MsResult<Self> {
        let hdu = fits.hdu(binary_hdu.unwrap_or(0)).map_err(invalid)?;
        let telescope = string_key(&hdu, fits, "TELESCOP", "unknown")?;
        let observer = string_key(&hdu, fits, "OBSERVER", "")?;
        let object = string_key(&hdu, fits, "OBJECT", "unknown")?;
        let epoch = optional_key(&hdu, fits, "EPOCH")?
            .or(optional_key(&hdu, fits, "EQUINOX")?)
            .unwrap_or(2000.0);
        let date = optional_key(&hdu, fits, "DATE-OBS")?
            .or(optional_key(&hdu, fits, "DATE")?)
            .unwrap_or_else(|| "2000-01-01".to_owned());
        let release_date = if binary_hdu.is_some() {
            optional_key(&hdu, fits, "DATE-MAP")?.unwrap_or_default()
        } else {
            date.clone()
        };
        let frame = optional_key(&hdu, fits, "SPECSYS")?;
        let rest_frequency = optional_key(&hdu, fits, "RESTFREQ")?
            .or(optional_key(&hdu, fits, "RESTFRQ")?)
            .unwrap_or(0.0);
        let alternate_value = optional_key(&hdu, fits, "ALTRVAL")?;
        let alternate_pixel = optional_key(&hdu, fits, "ALTRPIX")?;
        let velocity_reference = optional_key(&hdu, fits, "VELREF")?;
        let history = history(fits)?;
        Ok(Self {
            telescope,
            observer,
            object,
            epoch,
            date,
            release_date,
            frame,
            rest_frequency,
            alternate_value,
            alternate_pixel,
            velocity_reference,
            history,
        })
    }
}

pub(super) struct Extensions {
    pub(super) array: Array,
    pub(super) spectral: Option<Vec<SpectralWindow>>,
    pub(super) sources: Option<BTreeMap<i32, Source>>,
    pub(super) system_power: Option<Table>,
    pub(super) velocity_type: Option<String>,
}

impl Extensions {
    pub(super) fn read(
        fits: &mut FitsFile,
        header: &Header,
        layout: &GroupLayout,
    ) -> MsResult<Self> {
        let mut array = None;
        let mut spectral = None;
        let mut sources = None;
        let mut system_power = None;
        let mut velocity_type = None;
        let hdu_count = fits.num_hdus().map_err(invalid)?;
        for index in 1..hdu_count {
            let hdu = fits.hdu(index).map_err(invalid)?;
            let HduInfo::TableInfo { num_rows, .. } = &hdu.info else {
                continue;
            };
            let name = hdu.name(fits).map_err(invalid)?;
            let table = BinaryTable {
                fits,
                hdu: &hdu,
                rows: *num_rows,
            };
            if array.is_none() && name.contains("AN") {
                array = Some(read_array(table, &header.telescope)?);
            } else if spectral.is_none() && name.contains("FQ") {
                spectral = Some(read_spectral(table, layout)?);
            } else if sources.is_none() && name.contains("SU") {
                velocity_type = optional_key(table.hdu, table.fits, "VELTYP")?;
                sources = Some(read_sources(table)?);
            } else if system_power.is_none() && name.contains("SY") {
                system_power = Some(read_system_power(table)?);
            }
        }
        let array = array.ok_or_else(|| invalid("Cannot find the required UVFITS AN table"))?;
        fits.primary_hdu().map_err(invalid)?;
        Ok(Self {
            array,
            spectral,
            sources,
            system_power,
            velocity_type,
        })
    }
}

struct BinaryTable<'a> {
    fits: &'a mut FitsFile,
    hdu: &'a FitsHdu,
    rows: usize,
}

impl BinaryTable<'_> {
    fn strings(&mut self, name: &str) -> MsResult<Vec<String>> {
        let HduInfo::TableInfo {
            column_descriptions,
            ..
        } = &self.hdu.info
        else {
            return Err(invalid("Expected a UVFITS binary table"));
        };
        let (index, column) = column_descriptions
            .iter()
            .enumerate()
            .find(|(_, column)| column.name == name)
            .ok_or_else(|| invalid(format!("Cannot find UVFITS column {name}")))?;
        let width = column
            .data_type
            .repeat
            .max(column.data_type.width)
            .checked_add(1)
            .ok_or_else(|| invalid("FITS text width overflow"))?;
        let len = width
            .checked_mul(self.rows)
            .ok_or_else(|| invalid("FITS text column size overflow"))?;
        let mut storage = Vec::new();
        storage.try_reserve_exact(len).map_err(invalid)?;
        storage.resize(len, 0);
        let mut data = storage
            .chunks_exact_mut(width)
            .map(<[_]>::as_mut_ptr)
            .collect::<Vec<_>>();
        self.fits.hdu(self.hdu.number).map_err(invalid)?;
        let mut status = 0;
        // SAFETY: the current HDU is a table and every output string has room
        // for its entire fixed field plus the terminating NUL byte.
        unsafe {
            sys::ffgcvs(
                self.fits.as_raw(),
                (index + 1) as i32,
                1,
                1,
                self.rows as i64,
                ptr::null_mut(),
                data.as_mut_ptr(),
                ptr::null_mut(),
                &mut status,
            );
        }
        errors::check_status(status).map_err(invalid)?;
        Ok(storage
            .chunks_exact(width)
            .map(|row| {
                let value = row
                    .iter()
                    .take_while(|&&byte| byte != 0)
                    .map(|&byte| byte as u8)
                    .collect::<Vec<_>>();
                String::from_utf8_lossy(&value).trim_end().to_owned()
            })
            .collect())
    }

    fn column<T: ReadsCol>(&mut self, name: &str, width: usize) -> MsResult<Vec<T>> {
        let values = self.hdu.read_col(self.fits, name).map_err(invalid)?;
        if self.rows.checked_mul(width) != Some(values.len()) {
            return Err(invalid(format!(
                "UVFITS column {name} needs {width} values per row"
            )));
        }
        Ok(values)
    }

    fn has_column(&self, name: &str) -> bool {
        let HduInfo::TableInfo {
            column_descriptions,
            ..
        } = &self.hdu.info
        else {
            return false;
        };
        column_descriptions.iter().any(|column| column.name == name)
    }

    fn source_vector(&mut self, name: &str, width: usize) -> MsResult<Vec<f64>> {
        let values = self.hdu.read_col::<f64>(self.fits, name).map_err(invalid)?;
        if self.rows.checked_mul(width) == Some(values.len()) {
            Ok(values.chunks_exact(width).map(|row| row[0]).collect())
        } else if values.len() == self.rows {
            Ok(values)
        } else {
            Err(invalid(format!("Inconsistent UVFITS source column {name}")))
        }
    }
}

fn read_array(mut table: BinaryTable<'_>, telescope: &str) -> MsResult<Array> {
    let positions = table.column::<f64>("STABXYZ", 3)?;
    let numbers = table.column::<i32>("NOSTA", 1)?;
    let stations = table.strings("ANNAME")?;
    let mounts = table.column::<i32>("MNTSTA", 1)?;
    let axis_offsets = table.column::<f32>("STAXOF", 1)?;
    let angle_a = table.column::<f32>("POLAA", 1)?;
    let angle_b = table.column::<f32>("POLAB", 1)?;
    let origin = ["ARRAYX", "ARRAYY", "ARRAYZ"].map(|key| table.hdu.read_key(table.fits, key));
    let [x, y, z] = origin;
    let origin = [
        x.map_err(invalid)?,
        y.map_err(invalid)?,
        z.map_err(invalid)?,
    ];
    let name = string_key(table.hdu, table.fits, "ARRNAM", "Unknown")?;
    let left_handed = string_key(table.hdu, table.fits, "XYZHAND", "")? == "LEFT";
    let frame_defined = optional_key::<String>(table.hdu, table.fits, "FRAME")?.is_some();
    let date = string_key(table.hdu, table.fits, "RDATE", "")?;
    let time_system = string_key(table.hdu, table.fits, "TIMSYS", "TAI")?;
    let gst = optional_key::<f64>(table.hdu, table.fits, "GSTIA0")?
        .unwrap_or(0.0)
        .to_radians();
    let degrees_per_day = optional_key(table.hdu, table.fits, "DEGPDY")?.unwrap_or(0.0);
    let diameters = if table.has_column("DIAMETER") {
        table.column::<f32>("DIAMETER", 1)?
    } else {
        Vec::new()
    };
    let use_diameters = diameters.iter().any(|&diameter| diameter > 0.0);
    let mut antennas = BTreeMap::new();
    for (index, number) in numbers.into_iter().enumerate() {
        if number < 1 {
            return Err(invalid("Invalid station number in UVFITS AN table"));
        }
        let station = stations[index]
            .split('\0')
            .next()
            .unwrap_or_default()
            .trim_end();
        let position: [f64; 3] = positions[3 * index..3 * index + 3].try_into().unwrap();
        let diameter = if use_diameters {
            diameters[index]
        } else {
            diameter(telescope, number, station)
        };
        antennas.insert(
            number - 1,
            Antenna {
                number,
                station: station.to_owned(),
                position,
                mount: if telescope == "SMA" { 0 } else { mounts[index] },
                axis_offset: f64::from(axis_offsets[index]),
                angles: [angle_a[index], angle_b[index]].map(|angle| f64::from(angle).to_radians()),
                diameter: f64::from(diameter),
            },
        );
    }
    Ok(Array {
        name,
        origin,
        left_handed,
        frame_defined,
        antennas,
        date,
        gst,
        degrees_per_day,
        time_system,
    })
}

fn diameter(telescope: &str, number: i32, name: &str) -> f32 {
    match telescope {
        "ATCA" => 22.0,
        "SMA" => 6.0,
        "HATCREEK" | "BIMA" | "ATA" => 6.1,
        "GMRT" => 45.0,
        "IRAM_PDB" | "IRAM PDB" => 15.0,
        "OVRO" | "CARMA" if number <= 6 => 10.4,
        "OVRO" | "CARMA" => 6.1,
        "ALMA" if name.starts_with("CM") => 7.0,
        "ALMA"
            if ["DA", "DV", "PM"]
                .iter()
                .any(|prefix| name.starts_with(prefix)) =>
        {
            12.0
        }
        _ => 25.0,
    }
}

fn read_system_power(mut table: BinaryTable<'_>) -> MsResult<Table> {
    let no_if = table
        .hdu
        .read_key::<i32>(table.fits, "NO_IF")
        .map_err(invalid)?;
    let no_pol = table
        .hdu
        .read_key::<i32>(table.fits, "NO_POL")
        .map_err(invalid)?;
    if no_if != 1 || !matches!(no_pol, 1 | 2) {
        return Err(invalid(
            "UVFITS SYSPOWER requires one IF and one or two polarizations",
        ));
    }
    let time = table.column::<f64>("TIME", 1)?;
    let intervals = table.column::<f32>("TIME INTERVAL", 1)?;
    let antennas = table.column::<i32>("ANTENNA NO.", 1)?;
    let windows = table.column::<i32>("FREQ ID", 1)?;
    let power_names = ["SWITCHED_DIFF", "SWITCHED_SUM", "REQUANTIZER_GAIN"];
    let power = ["POWER DIF", "POWER SUM", "POST GAIN"].map(|source| {
        (1..=no_pol)
            .map(|hand| table.column::<f32>(&format!("{source}{hand}"), 1))
            .collect::<MsResult<Vec<_>>>()
    });
    let [dif, sum, gain] = power;
    let power = [dif?, sum?, gain?];
    let mut columns = vec![
        ColumnSchema::scalar("ANTENNA_ID", PrimitiveType::Int32),
        ColumnSchema::scalar("FEED_ID", PrimitiveType::Int32),
        ColumnSchema::scalar("SPECTRAL_WINDOW_ID", PrimitiveType::Int32),
        ColumnSchema::scalar("TIME", PrimitiveType::Float64),
        ColumnSchema::scalar("INTERVAL", PrimitiveType::Float32),
    ];
    columns.extend(
        power_names.map(|name| ColumnSchema::array_variable(name, PrimitiveType::Float32, Some(1))),
    );
    let mut output = Table::with_schema(TableSchema::new(columns).map_err(invalid)?);
    for row in 0..table.rows {
        let mut values = RecordValue::new(vec![
            RecordField::new(
                "ANTENNA_ID",
                Value::Scalar(ScalarValue::Int32(antennas[row] - 1)),
            ),
            RecordField::new("FEED_ID", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new(
                "SPECTRAL_WINDOW_ID",
                Value::Scalar(ScalarValue::Int32(windows[row] - 1)),
            ),
            RecordField::new(
                "TIME",
                Value::Scalar(ScalarValue::Float64(time[row] * 86_400.0)),
            ),
            RecordField::new(
                "INTERVAL",
                Value::Scalar(ScalarValue::Float32(intervals[row] * 86_400.0)),
            ),
        ]);
        for (name, hands) in power_names.into_iter().zip(&power) {
            let array = Array1::from_iter(hands.iter().map(|hand| hand[row]));
            values.upsert(name, Value::Array(ArrayValue::Float32(array.into_dyn())));
        }
        output.add_row(values)?;
    }
    Ok(output)
}

fn read_spectral(
    mut table: BinaryTable<'_>,
    layout: &GroupLayout,
) -> MsResult<Vec<SpectralWindow>> {
    let windows = layout.windows;
    let _ids = table.column::<i32>("FRQSEL", 1)?;
    let offsets = table.column::<f64>("IF FREQ", windows)?;
    let widths = table.column::<f32>("CH WIDTH", windows)?;
    let bandwidths = table.column::<f32>("TOTAL BANDWIDTH", windows)?;
    Ok(offsets
        .into_iter()
        .zip(widths)
        .zip(bandwidths)
        .map(|((offset, increment), bandwidth)| SpectralWindow {
            frequency: layout.frequency.reference + offset,
            increment: f64::from(increment),
            bandwidth: f64::from(bandwidth),
        })
        .collect())
}

fn read_sources(mut table: BinaryTable<'_>) -> MsResult<BTreeMap<i32, Source>> {
    let numbers = table.column::<i32>("ID. NO.", 1)?;
    let names = table.strings("SOURCE")?;
    let qualifiers = table.column::<i32>("QUAL", 1)?;
    let codes = table.strings("CALCODE")?;
    let ra = table.column::<f64>("RAEPO", 1)?;
    let dec = table.column::<f64>("DECEPO", 1)?;
    let app_ra = table.column::<f64>("RAAPP", 1)?;
    let app_dec = table.column::<f64>("DECAPP", 1)?;
    let motion_ra = table.column::<f64>("PMRA", 1)?;
    let motion_dec = table.column::<f64>("PMDEC", 1)?;
    let epochs = table.column::<f64>("EPOCH", 1)?;
    let windows = table
        .hdu
        .read_key::<i32>(table.fits, "NO_IF")
        .map_err(invalid)?;
    let windows = usize::try_from(windows)
        .ok()
        .filter(|&value| value > 0)
        .ok_or_else(|| invalid("Invalid NO_IF in UVFITS source table"))?;
    let frequencies = table.source_vector("RESTFREQ", windows)?;
    let velocities = table.source_vector("LSRVEL", windows)?;
    let multi_qualifier = qualifiers.windows(2).any(|pair| pair[0] != pair[1]);
    let mut sources = BTreeMap::new();
    for (index, number) in numbers.into_iter().enumerate() {
        if number < 1 {
            return Err(invalid("Invalid source number in UVFITS SU table"));
        }
        let mut name = names[index].trim_end().to_owned();
        if multi_qualifier {
            name = format!("{name}_{}", qualifiers[index]);
        }
        sources.insert(
            number - 1,
            Source {
                name,
                code: codes[index].trim_end().to_owned(),
                epoch: epochs[index],
                direction_deg: [ra[index], dec[index]],
                apparent_deg: [app_ra[index], app_dec[index]],
                motion_per_day: [motion_ra[index], motion_dec[index]],
                rest_frequency: frequencies[index],
                velocity: velocities[index],
            },
        );
    }
    Ok(sources)
}

fn string_key(hdu: &FitsHdu, fits: &mut FitsFile, key: &str, fallback: &str) -> MsResult<String> {
    Ok(optional_key::<String>(hdu, fits, key)?
        .map_or_else(|| fallback.to_owned(), |value| value.trim_end().to_owned()))
}

fn history(fits: &mut FitsFile) -> MsResult<Vec<String>> {
    use fitsio::{errors, sys};
    let mut result = Vec::new();
    let (mut status, mut count, mut space) = (0, 0, 0);
    // SAFETY: FFghsp reads the current HDU and writes to these live integers.
    unsafe { sys::ffghsp(fits.as_raw(), &mut count, &mut space, &mut status) };
    errors::check_status(status).map_err(invalid)?;
    for number in 1..=count {
        let mut card = [0; 81];
        // SAFETY: FFgrec writes one 80-byte card and a trailing null byte.
        unsafe { sys::ffgrec(fits.as_raw(), number, card.as_mut_ptr(), &mut status) };
        errors::check_status(status).map_err(invalid)?;
        let card = card.map(|byte| byte as u8);
        if let Some(message) = card.strip_prefix(b"HISTORY ") {
            let length = message
                .iter()
                .position(|&byte| byte == 0)
                .unwrap_or(message.len());
            let message = String::from_utf8_lossy(&message[..length]);
            result.push(message.trim_end().to_owned());
        }
    }
    Ok(result)
}
