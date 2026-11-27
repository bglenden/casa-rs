// SPDX-License-Identifier: LGPL-3.0-or-later
//! Instrument and array metadata from the NOSTAR observation header.

use super::binary::{
    ARRAYS, ByteOrder, OBSERVATION_HEADER_BYTES, Record, SCAN_HEADER_BYTES, invalid,
};
use casa_ms::MsResult;

const FIRST_ARRAY_MAP: usize = 15_028;
const POL_NAMES: usize = 15_100;
const SPEED_OF_LIGHT: f64 = 299_792_458.;

#[derive(Clone, Copy)]
pub(super) struct Array {
    pub beam: i32,
    pub window: i32,
    pub code: i32,
    pub plane: usize,
    pub name: &'static str,
    pub scale: f64,
    pub rest: f64,
    pub frequency: [f64; 2],
    pub channel: [f64; 2],
}

pub(super) struct Header {
    pub order: ByteOrder,
    pub n_arrays: usize,
    pub n_scans: usize,
    pub n_beam: usize,
    pub n_windows: usize,
    pub n_channels: usize,
    pub record_bytes: usize,
    pub interval: f64,
    pub arrays: [Option<Array>; ARRAYS],
    pub direction_frame: &'static str,
    pub direction: [f64; 2],
    pub frequency_frame: i32,
    pub velocity: f64,
    pub doppler_factor: f64,
    pub site: String,
    pub observer: String,
    pub project: String,
    pub object: String,
}

impl Header {
    pub(super) fn read(bytes: &[u8; OBSERVATION_HEADER_BYTES], file_size: u64) -> MsResult<Self> {
        let record = Record::observation(bytes)?;
        let n_arrays = record.i32(144) as usize;
        let n_scans = positive(record.i32(148), "scan count")?;
        let n_beam = positive(record.i32(14_972), "beam count")?;
        let n_windows = positive(record.i32(14_980), "window count")?;
        let n_channels = positive(record.i32(620), "channel count")?;
        let record_bytes = positive(record.i32(14_936), "scan record length")?;
        let bits = positive(record.i32(14_944), "sample bit width")?;
        if bits != 12 {
            return Err(invalid("sample bit width (expected packed 12-bit samples)"));
        }
        let payload = n_channels
            .checked_mul(bits)
            .ok_or_else(|| invalid("spectrum size"))?
            .div_ceil(8);
        if record_bytes < SCAN_HEADER_BYTES || payload > record_bytes - SCAN_HEADER_BYTES {
            return Err(invalid("spectrum size versus scan record length"));
        }
        let raw_bytes = n_arrays
            .checked_mul(n_scans)
            .and_then(|rows| rows.checked_mul(record_bytes))
            .and_then(|data_size| data_size.checked_add(OBSERVATION_HEADER_BYTES));
        if raw_bytes.is_none_or(|length| length as u64 > file_size) {
            return Err(invalid("truncated scan records"));
        }
        if n_beam > n_arrays || n_windows > n_arrays {
            return Err(invalid("array, beam or window count"));
        }
        let interval = record.f64(640);
        if !interval.is_finite() || interval <= 0. {
            return Err(invalid("integration interval"));
        }
        let direction_frame = direction_frame(record)?;
        let velocity = record.f64(456);
        let doppler_factor = match record.text(468, 4)? {
            "RAD" => 1. - velocity / SPEED_OF_LIGHT,
            "OPT" => (1. + velocity / SPEED_OF_LIGHT).recip(),
            _ => 1.,
        };
        let mut arrays = [None; ARRAYS];
        for (index, item) in arrays.iter_mut().enumerate() {
            *item = array_mapping(record, index, n_beam, n_windows)?;
        }
        Ok(Self {
            order: record.order(),
            n_arrays,
            n_scans,
            n_beam,
            n_windows,
            n_channels,
            record_bytes,
            interval,
            arrays,
            direction_frame,
            direction: [record.f64(296), record.f64(304)],
            frequency_frame: match record.text(464, 4)? {
                "LSR" => 1,
                "HEL" => 3,
                "GAL" => 6,
                _ => 64,
            },
            velocity,
            doppler_factor,
            site: record.text(14_948, 8)?.into(),
            observer: record.text(72, 40)?.into(),
            project: record.text(32, 16)?.into(),
            object: record.text(272, 16)?.into(),
        })
    }
}

fn array_mapping(
    record: Record<'_>,
    index: usize,
    beams: usize,
    windows: usize,
) -> MsResult<Option<Array>> {
    let mapping = i32::from(record.i16(FIRST_ARRAY_MAP + 2 * index));
    if mapping < 1101 {
        return Ok(None);
    }
    let beam = mapping / 1000 - 1;
    let window = mapping % 100 - 1;
    let pol = (mapping / 100) % 10 - 1;
    if !(0..beams as i32).contains(&beam) || !(0..windows as i32).contains(&window) || pol < 0 {
        return Err(invalid("array mapping"));
    }
    let (plane, code, name) = match record.text(POL_NAMES + 3 * pol as usize, 3)? {
        "XX" => (0, 9, "XX"),
        "YY" => (1, 12, "YY"),
        "XY" => (2, 10, "XY"),
        "YX" => (3, 11, "YX"),
        _ => return Err(invalid("array polarization")),
    };
    let calibration = index * 10 * 8;
    Ok(Some(Array {
        beam,
        window,
        code,
        plane,
        name,
        scale: record.f64(4576 + 8 * index),
        rest: record.f64(6256 + 8 * index),
        frequency: [
            record.f64(6536 + calibration),
            record.f64(6544 + calibration),
        ],
        channel: [
            record.f64(9336 + calibration),
            record.f64(9344 + calibration),
        ],
    }))
}

fn direction_frame(record: Record<'_>) -> MsResult<&'static str> {
    match (record.i32(332), record.text(288, 8)?) {
        (0, "J2000") => Ok("J2000"),
        (0, "B1950") => Ok("B1950"),
        (1, _) => Ok("GALACTIC"),
        (2, _) => Ok("AZEL"),
        _ => Err(invalid("direction reference frame")),
    }
}

fn positive(value: i32, field: &str) -> MsResult<usize> {
    usize::try_from(value)
        .ok()
        .filter(|&value| value != 0)
        .ok_or_else(|| invalid(field))
}
