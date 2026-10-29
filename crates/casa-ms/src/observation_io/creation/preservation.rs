// SPDX-License-Identifier: LGPL-3.0-or-later
//! What an MSv2 input's subtables hold beyond what a plan writes of them
//! (r3 §6, the preservation boundary). Every column of every subtable the
//! plan writes is one of:
//! - carried by the catalogue, and written as stated;
//! - allowlisted, and rewritten as the writer states it (`FEED`'s `TIME`
//!   and `INTERVAL`, the observation span, which covers every row);
//! - anything else: a declared loss ([`Loss::Column`]) when a row holds a
//!   value other than the one written (MSv2's default, or the writer's own
//!   constant: an antenna `TYPE` of `GROUND-BASED`, a feed's `BEAM_ID` of
//!   -1 and identity `POL_RESPONSE`).
//!
//! A column outside the MSv2 schema holding a value is a loss too.
//! `POINTING` is inventoried by the pointing plan, and MAIN by the losses'
//! inventory.

use casa_types::{ArrayValue, ScalarValue, Value};

use super::Loss;
use crate::MeasurementSet;
use crate::schema::{SubtableId, optional_columns, required_columns};

/// What the writer writes of a subtable's columns.
struct Written {
    id: SubtableId,
    /// The columns the catalogue carries, or the writer rewrites under the
    /// allowlist.
    carried: &'static [&'static str],
}

/// The subtables written, and what of each is carried.
const WRITTEN: &[Written] = &[
    Written {
        id: SubtableId::Antenna,
        carried: &[
            "NAME",
            "STATION",
            "MOUNT",
            "POSITION",
            "DISH_DIAMETER",
            "FLAG_ROW",
        ],
    },
    Written {
        id: SubtableId::Feed,
        carried: &[
            "ANTENNA_ID",
            "FEED_ID",
            "SPECTRAL_WINDOW_ID",
            "NUM_RECEPTORS",
            "POLARIZATION_TYPE",
            "RECEPTOR_ANGLE",
            "TIME",
            "INTERVAL",
        ],
    },
    Written {
        id: SubtableId::Field,
        carried: &[
            "NAME",
            "CODE",
            "TIME",
            "NUM_POLY",
            "DELAY_DIR",
            "PHASE_DIR",
            "REFERENCE_DIR",
            "SOURCE_ID",
            "FLAG_ROW",
            "EPHEMERIS_ID",
        ],
    },
    Written {
        id: SubtableId::Observation,
        carried: &[
            "TELESCOPE_NAME",
            "OBSERVER",
            "PROJECT",
            "TIME_RANGE",
            "RELEASE_DATE",
            "FLAG_ROW",
            "SCHEDULE",
        ],
    },
    Written {
        id: SubtableId::Polarization,
        carried: &["NUM_CORR", "CORR_TYPE", "CORR_PRODUCT", "FLAG_ROW"],
    },
    Written {
        id: SubtableId::DataDescription,
        carried: &["SPECTRAL_WINDOW_ID", "POLARIZATION_ID", "FLAG_ROW"],
    },
    Written {
        id: SubtableId::SpectralWindow,
        carried: &[
            "NAME",
            "NUM_CHAN",
            "REF_FREQUENCY",
            "CHAN_FREQ",
            "CHAN_WIDTH",
            "EFFECTIVE_BW",
            "RESOLUTION",
            "MEAS_FREQ_REF",
            "TOTAL_BANDWIDTH",
            "NET_SIDEBAND",
            "IF_CONV_CHAIN",
            "FREQ_GROUP",
            "FREQ_GROUP_NAME",
            "FLAG_ROW",
        ],
    },
    Written {
        id: SubtableId::State,
        carried: &[
            "OBS_MODE", "CAL", "LOAD", "SIG", "REF", "SUB_SCAN", "FLAG_ROW",
        ],
    },
    Written {
        id: SubtableId::Processor,
        carried: &[
            "TYPE", "SUB_TYPE", "TYPE_ID", "MODE_ID", "PASS_ID", "FLAG_ROW",
        ],
    },
    Written {
        id: SubtableId::Source,
        carried: &[
            "SOURCE_ID",
            "SPECTRAL_WINDOW_ID",
            "NAME",
            "TIME",
            "INTERVAL",
            "CODE",
            "CALIBRATION_GROUP",
            "DIRECTION",
            "PROPER_MOTION",
            "NUM_LINES",
            "POSITION",
            "PULSAR_ID",
            "REST_FREQUENCY",
            "SYSVEL",
            "TRANSITION",
        ],
    },
];

/// Whether `value` is the one the writer writes in `table`'s `column`:
/// its own constant there, or else MSv2's default (zero, false, empty, or
/// -1 for an identifier that names nothing).
fn as_written(table: SubtableId, column: &str, value: &Value) -> bool {
    match (table, column, value) {
        (SubtableId::Antenna, "TYPE", Value::Scalar(ScalarValue::String(kind))) => {
            kind.is_empty() || kind == "GROUND-BASED"
        }
        (SubtableId::Feed, "POL_RESPONSE", Value::Array(ArrayValue::Complex32(response))) => {
            let shape = response.shape();
            response.indexed_iter().all(|(index, value)| {
                let diagonal = shape.len() == 2 && index[0] == index[1];
                let expected = if diagonal { 1.0 } else { 0.0 };
                value.re == expected && value.im == 0.0
            }) || response.iter().all(|value| value.norm_sqr() == 0.0)
        }
        _ => default(value),
    }
}

/// Whether `value` is MSv2's default.
fn default(value: &Value) -> bool {
    match value {
        Value::Scalar(scalar) => default_scalar(scalar),
        Value::Array(array) => match array {
            ArrayValue::Bool(values) => values.iter().all(|value| !value),
            ArrayValue::Int32(values) => values.iter().all(|value| *value == 0 || *value == -1),
            ArrayValue::Float32(values) => values.iter().all(|value| *value == 0.0),
            ArrayValue::Float64(values) => values.iter().all(|value| *value == 0.0),
            ArrayValue::Complex32(values) => values.iter().all(|value| value.norm_sqr() == 0.0),
            ArrayValue::String(values) => values.iter().all(String::is_empty),
            // Another element type is not one MSv2's default is known for,
            // so it is not taken for one.
            _ => false,
        },
        Value::Record(record) => record.is_empty(),
        _ => false,
    }
}

fn default_scalar(scalar: &ScalarValue) -> bool {
    match scalar {
        ScalarValue::Bool(value) => !value,
        ScalarValue::Int32(value) => *value == 0 || *value == -1,
        ScalarValue::Float32(value) => *value == 0.0,
        ScalarValue::Float64(value) => *value == 0.0,
        ScalarValue::String(value) => value.is_empty(),
        _ => false,
    }
}

/// Note in `losses` each column of `ms`'s written subtables that holds what
/// the writer does not write.
pub(super) fn inventory(ms: &MeasurementSet, losses: &mut Vec<Loss>) {
    for written in WRITTEN {
        let Some(table) = ms.subtable(written.id) else {
            continue;
        };
        let Some(schema) = table.schema() else {
            continue;
        };
        let known = |name: &str| {
            required_columns(written.id)
                .iter()
                .chain(optional_columns(written.id))
                .any(|definition| definition.name == name)
        };
        let rows = table.row_accessor();
        for column in schema.columns() {
            let name = column.name();
            if written.carried.contains(&name) {
                continue;
            }
            let held = (0..rows.row_count()).any(|row| {
                rows.row(row)
                    .ok()
                    .and_then(|record| record.get(name))
                    .is_some_and(|value| !as_written(written.id, name, value))
            });
            if held {
                let outside = if known(name) {
                    ""
                } else {
                    " (outside the MSv2 schema)"
                };
                losses.push(Loss::Column(format!(
                    "{} {name}{outside}",
                    written.id.name()
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use casa_types::{ArrayValue, ScalarValue, Value};
    use ndarray::ArrayD;
    use num_complex::Complex32;

    use super::as_written;
    use crate::schema::SubtableId;

    #[test]
    fn written_values_are_the_writers_constants_or_defaults() {
        let text = |value: &str| Value::Scalar(ScalarValue::String(value.to_string()));
        assert!(as_written(
            SubtableId::Antenna,
            "TYPE",
            &text("GROUND-BASED")
        ));
        assert!(!as_written(
            SubtableId::Antenna,
            "TYPE",
            &text("SPACE-BASED")
        ));
        let offset = |values: Vec<f64>| {
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(vec![3], values).expect("shape"),
            ))
        };
        assert!(as_written(
            SubtableId::Antenna,
            "OFFSET",
            &offset(vec![0.0; 3])
        ));
        assert!(!as_written(
            SubtableId::Antenna,
            "OFFSET",
            &offset(vec![0.0, 0.0, 1.5])
        ));
        let response = |values: Vec<Complex32>| {
            Value::Array(ArrayValue::Complex32(
                ArrayD::from_shape_vec(vec![2, 2], values).expect("shape"),
            ))
        };
        let one = Complex32::new(1.0, 0.0);
        let zero = Complex32::new(0.0, 0.0);
        assert!(as_written(
            SubtableId::Feed,
            "POL_RESPONSE",
            &response(vec![one, zero, zero, one])
        ));
        assert!(!as_written(
            SubtableId::Feed,
            "POL_RESPONSE",
            &response(vec![one, Complex32::new(0.1, 0.0), zero, one])
        ));
        assert!(as_written(
            SubtableId::Feed,
            "BEAM_ID",
            &Value::Scalar(ScalarValue::Int32(-1))
        ));
    }
}
