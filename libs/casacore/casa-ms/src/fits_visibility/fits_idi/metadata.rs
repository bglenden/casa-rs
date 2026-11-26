// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read FITS-IDI extension metadata into the standard MS subtables.

use std::collections::BTreeMap;

use casa_core::Complex32;
use casa_core::measures::direction::{DirectionRef, MDirection};
use fitsio::FitsFile;
use fitsio::hdu::{FitsHdu, HduInfo};
use fitsio::tables::ReadsCol;

use super::super::groups::optional_key;
use super::super::invalid;
use super::super::uvfits_ms::{column_keyword, fits_date, insert};
use super::rows::TimeRange;
use crate::schema::{self, SubtableId};
use crate::subtable_rows::{
    b, complex_array, f, f64_array, i, row_from_defs, s, string_array, subtable_mut,
};
use crate::{MeasurementSet, MsResult};

pub(super) struct Geometry {
    pub(super) antennas: BTreeMap<i32, i32>,
    pub(super) digitizer_levels: BTreeMap<i32, i32>,
    pub(super) name: String,
    pub(super) epoch: String,
    pub(super) reference_time: f64,
}

pub(super) fn write_geometry(
    ms: &mut MeasurementSet,
    fits: &mut FitsFile,
    hdu: &FitsHdu,
) -> MsResult<Geometry> {
    let mut table = IdiTable::new(fits, hdu)?;
    let name = table.key_text("ARRNAM", "unknown")?;
    let epoch = table.key_text("TIMSYS", "TAI")?;
    let epoch = table.key_text("TIMESYS", &epoch)?;
    let epoch = if epoch == "IAT" {
        "TAI".to_owned()
    } else {
        epoch
    };
    let numbers = table.column::<i32>("NOSTA", 1)?;
    let names = table.column::<String>("ANNAME", 1)?;
    let offset = table.column::<f64>("STAXOF", 3)?;
    let positions = table.column::<f64>("STABXYZ", 3)?;
    let mounts = table.column::<i32>("MNTSTA", 1)?;
    let mut origin = [0.0; 3];
    for (axis, key) in ["ARRAYX", "ARRAYY", "ARRAYZ"].into_iter().enumerate() {
        origin[axis] = table.key(key)?.unwrap_or(0.0);
    }
    let diameter = if table.has("DIAMETER") {
        table.column::<f64>("DIAMETER", 1)?
    } else {
        let diameter = match name.as_str() {
            "ATCA" => 22.0,
            "SMA" => 6.0,
            _ => 0.0,
        };
        vec![diameter; table.rows]
    };
    let mut antennas = BTreeMap::new();
    for (row, number) in numbers.into_iter().enumerate() {
        if antennas.insert(number, row as i32).is_some() {
            return Err(invalid(format!(
                "Repeated FITS-IDI antenna number {number}"
            )));
        }
        let mounts_by_code = [
            "ALT-AZ",
            "EQUATORIAL",
            "ORBITING",
            "X-Y",
            "ALT-AZ+NASMYTH-R",
            "ALT-AZ+NASMYTH-L",
            "ALT-AZ+BWG-R",
            "ALT-AZ+BWG-L",
        ];
        let mount = mounts_by_code
            .get(mounts[row] as usize)
            .copied()
            .unwrap_or("UNKNOWN");
        let antenna = names[row].trim_end();
        let (antenna_name, station) = if antenna.is_empty() {
            (format!("ANT{number:02}"), format!("{name}:{number:02}"))
        } else {
            (antenna.to_owned(), antenna.to_owned())
        };
        let pos: [f64; 3] = std::array::from_fn(|axis| origin[axis] + positions[row * 3 + axis]);
        insert(
            ms,
            SubtableId::Antenna,
            &[
                ("NAME", s(&antenna_name)),
                ("STATION", s(&station)),
                ("TYPE", s("GROUND-BASED")),
                ("MOUNT", s(mount)),
                ("POSITION", f64_array(&pos, vec![3])),
                ("OFFSET", f64_array(&offset[row * 3..row * 3 + 3], vec![3])),
                ("DISH_DIAMETER", f(diameter[row])),
            ],
        )?;
    }
    let antenna = subtable_mut(ms, SubtableId::Antenna)?;
    let date = fits_date(&table.key_text("RDATE", "")?)?;
    let gst = table.key::<f64>("GSTIA0")?.unwrap_or_default().to_radians();
    let rotation = table.key::<f64>("DEGPDY")?.unwrap_or_default();
    let keywords = antenna.keywords_mut();
    for (key, value) in [
        ("RDATE", f(date)),
        ("GSTIA0", f(gst)),
        ("DEGPDY", f(rotation)),
        ("TIMESYS", s(&epoch)),
        ("FRAME", s("GEOCENTRIC")),
    ] {
        keywords.upsert(key, value);
    }
    column_keyword(antenna, "NAME", "ARRAY_NAME", s(&name));
    column_keyword(
        antenna,
        "POSITION",
        "ARRAY_POSITION",
        f64_array(&origin, vec![3]),
    );
    Ok(Geometry {
        name,
        epoch,
        reference_time: date,
        antennas,
        digitizer_levels: BTreeMap::new(),
    })
}

pub(super) fn write_frequency(
    ms: &mut MeasurementSet,
    fits: &mut FitsFile,
    hdu: &FitsHdu,
) -> MsResult<Vec<f64>> {
    let mut table = IdiTable::new(fits, hdu)?;
    let bands = table.count("NO_BAND")?;
    let channels = table.count("NO_CHAN")?;
    let base: f64 = table.required_key("REF_FREQ")?;
    let pixel: f64 = table.required_key("REF_PIXL")?;
    let offset = table.column::<f64>("BANDFREQ", bands)?;
    let increments = table.column::<f64>("CH_WIDTH", bands)?;
    let bandwidths = table.column::<f64>("TOTAL_BANDWIDTH", bands)?;
    let sidebands = table.column::<i32>("SIDEBAND", bands)?;
    for (window, &increment) in increments.iter().enumerate() {
        let first = base + offset[window];
        let frequencies = (0..channels)
            .map(|channel| first + (channel as f64 + 1.0 - pixel) * increment)
            .collect::<Vec<_>>();
        let widths = f64_array(&vec![increment.abs(); channels], vec![channels]);
        insert(
            ms,
            SubtableId::SpectralWindow,
            &[
                ("NAME", s("none")),
                ("FREQ_GROUP_NAME", s("none")),
                ("NUM_CHAN", i(channels as i32)),
                ("CHAN_FREQ", f64_array(&frequencies, vec![channels])),
                ("REF_FREQUENCY", f(first)),
                ("CHAN_WIDTH", widths.clone()),
                ("EFFECTIVE_BW", widths.clone()),
                ("RESOLUTION", widths),
                ("MEAS_FREQ_REF", i(5)),
                ("NET_SIDEBAND", i(sidebands[window])),
                ("TOTAL_BANDWIDTH", f(bandwidths[window])),
                ("IF_CONV_CHAIN", i((window % bands) as i32)),
                ("FREQ_GROUP", i((window / bands) as i32)),
            ],
        )?;
        insert(
            ms,
            SubtableId::DataDescription,
            &[("SPECTRAL_WINDOW_ID", i(window as i32))],
        )?;
    }
    Ok(increments.into_iter().map(f64::abs).collect())
}

pub(super) fn write_feeds(
    ms: &mut MeasurementSet,
    fits: &mut FitsFile,
    hdu: &FitsHdu,
    geometry: &mut Geometry,
) -> MsResult<()> {
    let mut table = IdiTable::new(fits, hdu)?;
    let bands = table.count("NO_BAND")?;
    let numbers = table.column::<i32>("ANTENNA_NO", 1)?;
    let levels = table.column::<i32>("NO_LEVELS", 1)?;
    let times = table.column::<f64>("TIME", 1)?;
    let intervals = table.column::<f64>("TIME_INTERVAL", 1)?;
    let pol_a = table.column::<String>("POLTYA", 1)?;
    let pol_b = table.column::<String>("POLTYB", 1)?;
    let angle_a = table.column_or_replicate::<f64>("POLAA", bands)?;
    let angle_b = table.column_or_replicate::<f64>("POLAB", bands)?;
    for (row, number) in numbers.into_iter().enumerate() {
        let antenna = *geometry
            .antennas
            .get(&number)
            .ok_or_else(|| invalid(format!("Missing FITS-IDI antenna {number}")))?;
        geometry.digitizer_levels.insert(antenna, levels[row]);
        let labels = [pol_a[row].trim_end(), pol_b[row].trim_end()];
        for band in 0..bands {
            let index = row * bands + band;
            let angles = [angle_a[index].to_radians(), angle_b[index].to_radians()];
            let matrix = [
                Complex32::new(1.0, 0.0),
                Complex32::ZERO,
                Complex32::ZERO,
                Complex32::new(1.0, 0.0),
            ];
            insert(
                ms,
                SubtableId::Feed,
                &[
                    ("ANTENNA_ID", i(antenna)),
                    ("BEAM_ID", i(-1)),
                    ("FEED_ID", i(0)),
                    ("INTERVAL", f(intervals[row] * 86_400.0)),
                    ("TIME", f(times[row] * 86_400.0 + geometry.reference_time)),
                    ("SPECTRAL_WINDOW_ID", i(band as i32)),
                    ("NUM_RECEPTORS", i(2)),
                    ("BEAM_OFFSET", f64_array(&[0.0; 4], vec![2, 2])),
                    ("POSITION", f64_array(&[0.0; 3], vec![3])),
                    ("POLARIZATION_TYPE", string_array(&labels, vec![2])),
                    ("POL_RESPONSE", complex_array(&matrix, vec![2, 2])),
                    ("RECEPTOR_ANGLE", f64_array(&angles, vec![2])),
                ],
            )?;
        }
    }
    Ok(())
}

pub(super) fn write_fields(
    ms: &mut MeasurementSet,
    fits: &mut FitsFile,
    hdu: &FitsHdu,
) -> MsResult<DirectionRef> {
    let mut table = IdiTable::new(fits, hdu)?;
    let mut ids = table.column::<i32>(
        if table.has("SOURCE_ID") {
            "SOURCE_ID"
        } else {
            "ID_NO."
        },
        1,
    )?;
    let names = table.column::<String>("SOURCE", 1)?;
    let codes = table.column::<String>("CALCODE", 1)?;
    let ra = table.column::<f64>("RAEPO", 1)?;
    let dec = table.column::<f64>("DECEPO", 1)?;
    let apparent_ra = table.column::<f64>("RAAPP", 1)?;
    let apparent_dec = table.column::<f64>("DECAPP", 1)?;
    let motion_ra = table.column::<f64>("PMRA", 1)?;
    let motion_dec = table.column::<f64>("PMDEC", 1)?;
    let frames = if table.has("EQUINOX") {
        table
            .column::<String>("EQUINOX", 1)?
            .into_iter()
            .map(|value| {
                if value.contains("J2000") {
                    Ok(DirectionRef::J2000)
                } else if value.contains("1950.0B") {
                    Ok(DirectionRef::B1950)
                } else {
                    Err(invalid(format!("Cannot handle FITS-IDI equinox {value}")))
                }
            })
            .collect::<MsResult<Vec<_>>>()?
    } else {
        table
            .column::<f64>("EPOCH", 1)?
            .into_iter()
            .map(|value| {
                if (value - 1950.0).abs() < 0.01 {
                    DirectionRef::B1950
                } else {
                    DirectionRef::J2000
                }
            })
            .collect()
    };
    let frame = frames.first().copied().unwrap_or(DirectionRef::J2000);
    for id in &mut ids {
        if *id == -1 {
            *id = 1;
        }
        if *id < 1 {
            return Err(invalid("Invalid FITS-IDI source identifier"));
        }
    }
    let count = ids.iter().copied().max().unwrap_or_default();
    let rows = ids
        .iter()
        .enumerate()
        .map(|(row, &id)| (id, row))
        .collect::<BTreeMap<_, _>>();
    for id in 1..=count {
        let Some(&row) = rows.get(&id) else {
            write_field(ms, "", "", &[0.0; 2], 0, true)?;
            continue;
        };
        let motion = [motion_ra[row], motion_dec[row]];
        let moving = motion.iter().any(|rate| rate.abs() > 1e-13)
            && motion.iter().all(|rate| rate.abs() <= 1000.0);
        let direction = if moving {
            canonical_direction([apparent_ra[row], apparent_dec[row]])
        } else {
            canonical_direction([ra[row], dec[row]])
        };
        if moving {
            write_field(
                ms,
                names[row].trim_end(),
                codes[row].trim_end(),
                &[
                    direction[0],
                    direction[1],
                    motion[0].to_radians() / 86_400.0,
                    motion[1].to_radians() / 86_400.0,
                ],
                1,
                false,
            )?;
        } else {
            write_field(
                ms,
                names[row].trim_end(),
                codes[row].trim_end(),
                &direction,
                0,
                false,
            )?;
        }
    }
    Ok(frame)
}

fn write_field(
    ms: &mut MeasurementSet,
    name: &str,
    code: &str,
    values: &[f64],
    degree: usize,
    flagged: bool,
) -> MsResult<()> {
    let shape = vec![2, degree + 1];
    let direction = f64_array(values, shape);
    insert(
        ms,
        SubtableId::Field,
        &[
            ("NAME", s(name)),
            ("CODE", s(code)),
            ("SOURCE_ID", i(if flagged { 0 } else { -1 })),
            ("NUM_POLY", i(degree as i32)),
            ("FLAG_ROW", b(flagged)),
            ("DELAY_DIR", direction.clone()),
            ("PHASE_DIR", direction.clone()),
            ("REFERENCE_DIR", direction),
        ],
    )
}

pub(super) fn write_observation(
    ms: &mut MeasurementSet,
    fits: &mut FitsFile,
    hdu: &FitsHdu,
    geometry: &Geometry,
    times: &TimeRange,
) -> MsResult<()> {
    let mut table = IdiTable::new(fits, hdu)?;
    let observer = table.key_text("OBSERVER", "unknown")?;
    let project = table.key_text("OBSCODE", "")?;
    let telescope = table.key_text("TELESCOP", &geometry.name)?;
    let start = times
        .first
        .ok_or_else(|| invalid("FITS-IDI contains no visibility rows"))?;
    let mut observation = row_from_defs(
        schema::observation::REQUIRED_COLUMNS,
        &[
            ("TIME_RANGE", f64_array(&[start, times.last_end], vec![2])),
            ("RELEASE_DATE", f(start)),
            ("TELESCOPE_NAME", s(&telescope)),
            ("OBSERVER", s(&observer)),
            ("PROJECT", s(&project)),
        ],
    );
    observation.remove("LOG");
    observation.remove("SCHEDULE");
    subtable_mut(ms, SubtableId::Observation)?.add_row(observation)?;
    let fields = subtable_mut(ms, SubtableId::Field)?;
    for row in 0..fields.row_count() {
        fields.cell_accessor_mut(row, "TIME")?.set(f(start))?;
    }
    Ok(())
}

pub(super) struct IdiTable<'a> {
    fits: &'a mut FitsFile,
    hdu: &'a FitsHdu,
    pub(super) rows: usize,
}

impl<'a> IdiTable<'a> {
    pub(super) fn new(fits: &'a mut FitsFile, hdu: &'a FitsHdu) -> MsResult<Self> {
        let HduInfo::TableInfo { num_rows, .. } = hdu.info else {
            return Err(invalid("FITS-IDI extension must be a binary table"));
        };
        Ok(Self {
            fits,
            hdu,
            rows: num_rows,
        })
    }

    pub(super) fn has(&self, name: &str) -> bool {
        match &self.hdu.info {
            HduInfo::TableInfo {
                column_descriptions,
                ..
            } => column_descriptions
                .iter()
                .any(|column| column.name == name && column.data_type.repeat != 0),
            _ => false,
        }
    }

    pub(super) fn column<T: ReadsCol>(&mut self, name: &str, width: usize) -> MsResult<Vec<T>> {
        let values = self.hdu.read_col(self.fits, name).map_err(invalid)?;
        if self.rows.checked_mul(width) != Some(values.len()) {
            return Err(invalid(format!("Invalid FITS-IDI column {name} size")));
        }
        Ok(values)
    }

    pub(super) fn column_or_replicate<T: ReadsCol + Copy>(
        &mut self,
        name: &str,
        width: usize,
    ) -> MsResult<Vec<T>> {
        let values = self.hdu.read_col(self.fits, name).map_err(invalid)?;
        if values.len() == self.rows {
            Ok(values
                .into_iter()
                .flat_map(|value| std::iter::repeat_n(value, width))
                .collect())
        } else if self.rows.checked_mul(width) == Some(values.len()) {
            Ok(values)
        } else {
            Err(invalid(format!("Invalid FITS-IDI column {name} size")))
        }
    }

    pub(super) fn count(&mut self, key: &str) -> MsResult<usize> {
        let value: i32 = self.required_key(key)?;
        usize::try_from(value)
            .ok()
            .filter(|&number| number > 0)
            .ok_or_else(|| invalid(format!("Invalid FITS-IDI {key}")))
    }

    pub(super) fn required_key<T: fitsio::headers::ReadsKey>(&mut self, key: &str) -> MsResult<T> {
        self.hdu.read_key(self.fits, key).map_err(invalid)
    }

    pub(super) fn key<T: fitsio::headers::ReadsKey>(&mut self, key: &str) -> MsResult<Option<T>> {
        optional_key(self.hdu, self.fits, key)
    }

    pub(super) fn key_text(&mut self, key: &str, default: &str) -> MsResult<String> {
        Ok(self
            .key::<String>(key)?
            .map_or_else(|| default.to_owned(), |value| value.trim_end().to_owned()))
    }
}

fn canonical_direction(angles_deg: [f64; 2]) -> [f64; 2] {
    let [lon, lat] = angles_deg.map(f64::to_radians);
    let (longitude, latitude) = MDirection::from_angles(lon, lat, DirectionRef::J2000).as_angles();
    [longitude.sin().atan2(longitude.cos()), latitude]
}
