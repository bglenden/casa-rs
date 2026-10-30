// SPDX-License-Identifier: LGPL-3.0-or-later
//! Make the MSv2/MSv4 trial's MSv2 a moving-field observation, for the
//! ephemeris fixtures (`tools/ms-trial/make_ephemeris_fixtures.sh`): its
//! field follows a Moon-like body (`FIELD/EPHEM0_Moonish.tab`, J2000,
//! sampled every minute from five minutes before the first time to five
//! after the last), its `PHASE_DIR` the offset from the body.
//!
//! ```text
//! cargo run -p casa-ms --example ms_trial_ephemeris -- MS
//! ```

use std::path::PathBuf;

use casa_tables::{ColumnSchema, RowRange};
use casa_test_support::ephemeris_table::{EphemerisFrame, EphemerisRow, write_field_ephemeris_in};
use casa_types::{ArrayValue, PrimitiveType, ScalarValue, Value};

/// The field's offset from the body, radians.
const OFFSET_RAD: [f64; 2] = [1.0e-5, -5.0e-6];

/// Sample spacing, seconds.
const STEP_S: f64 = 60.0;

/// The body at `t` seconds after the table's first sample: about 0.5
/// arcsec a second in right ascension, a little in declination, at about
/// the Moon's distance and receding slowly.
fn body(mjd: f64, t: f64) -> EphemerisRow {
    [
        mjd,
        120.0 + 1.5e-4 * t + 2.0e-9 * t * t,
        18.0 - 4.0e-5 * t,
        0.00257 + 1.0e-9 * t,
        1.0e-5 + 1.0e-10 * t,
    ]
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: ms_trial_ephemeris MS")?;
    let mut ms = casa_ms::MeasurementSet::open(&path)?;
    let times: Vec<f64> = ms
        .main_table()
        .rows()?
        .iter()
        .filter_map(|row| match row.get("TIME") {
            Some(Value::Scalar(ScalarValue::Float64(time))) => Some(*time),
            _ => None,
        })
        .collect();
    let (first, last) = times
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), t| {
            (lo.min(*t), hi.max(*t))
        });
    let start_s = ((first - 300.0) / STEP_S).floor() * STEP_S;
    let samples = ((last + 300.0 - start_s) / STEP_S).ceil() as usize + 1;
    let field = ms
        .subtable_mut(casa_ms::SubtableId::Field)
        .ok_or("the MS has no FIELD")?;
    field.add_column(
        ColumnSchema::scalar("EPHEMERIS_ID", PrimitiveType::Int32),
        Some(Value::Scalar(ScalarValue::Int32(0))),
    )?;
    let offset = Value::Array(ArrayValue::Float64(ndarray::ArrayD::from_shape_vec(
        vec![2, 1],
        OFFSET_RAD.to_vec(),
    )?));
    for column in ["PHASE_DIR", "DELAY_DIR", "REFERENCE_DIR"] {
        field.fill_column_range(column, RowRange::new(0, 1), offset.clone())?;
    }
    ms.save()?;
    drop(ms);
    let rows: Vec<EphemerisRow> = (0..samples)
        .map(|sample| {
            let t = sample as f64 * STEP_S;
            body((start_s + t) / 86_400.0, t)
        })
        .collect();
    write_field_ephemeris_in(&path, 0, "Moonish", &rows, EphemerisFrame::J2000);
    Ok(())
}
