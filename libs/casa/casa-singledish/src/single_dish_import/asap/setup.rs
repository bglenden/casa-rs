// SPDX-License-Identifier: LGPL-3.0-or-later
//! Header and spectral-window metadata from an ASAP scan table.

use std::collections::BTreeMap;
use std::path::Path;

use casa_core::measures::frequency::CasaFrequencyFrame;
use casa_tables::Table;

use super::super::{Antenna, ImportHeader, Window, antenna_diameter};
use super::asap_row::{doubles, flags, integer, invalid, number, string, subtable};
use casa_ms::MsResult;

pub(super) fn header(path: &Path, table: &Table) -> MsResult<ImportHeader> {
    let keywords = table.keywords();
    let raw_name = string(keywords, "AntennaName")?;
    let (observatory, antenna) = raw_name.split_once("//").unwrap_or_else(|| {
        let (observatory, _) = raw_name.split_once('@').unwrap_or((raw_name, ""));
        (observatory, raw_name)
    });
    let (name, station) = antenna.split_once('@').unwrap_or((antenna, ""));
    let position = doubles(keywords, "AntennaPosition")?
        .try_into()
        .map_err(|_| invalid("AntennaPosition"))?;
    let times = (0..table.row_count())
        .map(|index| number(table.row_accessor().row(index)?, "TIME"))
        .collect::<MsResult<Vec<_>>>()?;
    let range = times
        .into_iter()
        .fold([f64::INFINITY, f64::NEG_INFINITY], |[min, max], time| {
            [min.min(time), max.max(time)]
        });
    Ok(ImportHeader {
        antenna: vec![Antenna {
            name: name.into(),
            station: station.into(),
            position,
            diameter: antenna_diameter(name),
        }],
        direction_frame: "J2000",
        telescope: observatory.into(),
        observer: string(keywords, "Observer")?.into(),
        project: string(keywords, "Project")?.into(),
        range: range.map(|time| time * super::DAY),
        data_unit: string(keywords, "FluxUnit")?.into(),
        windows: windows(path, table)?,
    })
}

fn windows(path: &Path, table: &Table) -> MsResult<BTreeMap<i32, Window>> {
    let frequencies = subtable(path, table.keywords(), "FREQUENCIES")?;
    let frame = string(frequencies.keywords(), "BASEFRAME")?.parse::<CasaFrequencyFrame>();
    let frame_code = match frame {
        Ok(CasaFrequencyFrame::Defined(frame)) => frame.casacore_code(),
        Ok(CasaFrequencyFrame::Undefined) => 64,
        Err(_) => 9,
    };
    let by_id = (0..frequencies.row_count())
        .map(|index| {
            let row = frequencies.row_accessor().row(index)?;
            Ok((integer(row, "ID")?, row))
        })
        .collect::<MsResult<BTreeMap<_, _>>>()?;
    let mut iter = BTreeMap::new();
    for index in 0..table.row_count() {
        let row = table.row_accessor().row(index)?;
        let window = integer(row, "IFNO")?;
        if let std::collections::btree_map::Entry::Vacant(entry) = iter.entry(window) {
            let frequency = by_id
                .get(&integer(row, "FREQ_ID")?)
                .ok_or_else(|| invalid("FREQ_ID"))?;
            entry.insert(Window {
                channels: flags(row, "FLAGTRA")?.len(),
                frame: frame_code,
                reference_channel: number(frequency, "REFPIX")?,
                reference_frequency: number(frequency, "REFVAL")?,
                increment: number(frequency, "INCREMENT")?,
            });
        }
    }
    Ok(iter)
}
