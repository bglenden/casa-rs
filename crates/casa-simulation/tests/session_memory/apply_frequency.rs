// SPDX-License-Identifier: LGPL-3.0-or-later
//! Wide applied tables charge their values as well as their row metadata (#1053).
use super::apply::{check, measured};
use super::{ANTENNAS, charge_process_caches, configured, resources, scan};
use casa_calibration::{
    SimulatedJones, SimulatedSolution, SolutionValues, write_simulated_jones_table,
};
use casa_simulation::ApplySpec;
use num_complex::Complex32;

#[test]
fn wide_applied_tables_and_terms_hold_their_admission() {
    if !super::alone("apply_frequency::wide_applied_tables_and_terms_hold_their_admission") {
        return;
    }
    run_case(("Tf", 4096, 16), 4096, 48, false, "linear");
}
#[test]
fn few_solution_rows_admit_very_wide_parameter_and_frequency_axes() {
    if !super::alone(
        "apply_frequency::few_solution_rows_admit_very_wide_parameter_and_frequency_axes",
    ) {
        return;
    }
    run_case(("Tf", 65536, 1), 4, 4, true, "linear");
}
#[test]
fn two_receptor_tables_admit_time_and_frequency_interpolation_scratch() {
    if !super::alone(
        "apply_frequency::two_receptor_tables_admit_time_and_frequency_interpolation_scratch",
    ) {
        return;
    }
    run_case(("B", 8192, 2), 4096, 48, true, "linear");
}
#[test]
fn scalar_spline_scratch_is_admitted_above_a_vector_growth_boundary() {
    if !super::alone(
        "apply_frequency::scalar_spline_scratch_is_admitted_above_a_vector_growth_boundary",
    ) {
        return;
    }
    run_case(("Tf", 131073, 1), 4, 4, true, "nearest,spline");
}

#[test]
fn scalar_time_spline_coefficients_hold_their_admission() {
    if !super::alone("apply_frequency::scalar_time_spline_coefficients_hold_their_admission") {
        return;
    }
    run_case(("Tf", 4096, 16), 4, 4, true, "spline,linear");
}

#[test]
fn two_receptor_time_spline_coefficients_hold_their_admission() {
    if !super::alone("apply_frequency::two_receptor_time_spline_coefficients_hold_their_admission")
    {
        return;
    }
    run_case(("B", 8192, 16), 4, 4, true, "spline,linear");
}

#[test]
fn long_time_spline_axes_hold_coefficient_formation_scratch() {
    if !super::alone("apply_frequency::long_time_spline_axes_hold_coefficient_formation_scratch") {
        return;
    }
    run_case(("Tf", 4, 8193), 4, 4, true, "spline,linear");
}

#[test]
fn time_context_headers_remain_bounded_across_many_read_batches() {
    if !super::alone(
        "apply_frequency::time_context_headers_remain_bounded_across_many_read_batches",
    ) {
        return;
    }
    run_case(("Tf", 4, 8193), 4, 4, true, "linearperscan,linear");
}

#[test]
fn wide_phase_delay_cycle_arrays_hold_their_admission() {
    if !super::alone("apply_frequency::wide_phase_delay_cycle_arrays_hold_their_admission") {
        return;
    }
    run_case(("B", 8192, 16), 4, 4, true, "nearestPD,linear");
}
fn run_case(
    (kind, channels, slots): (&str, usize, usize),
    data_channels: usize,
    integrations: usize,
    align_times: bool,
    interp: &str,
) {
    const BUDGET: u64 = 256 << 20;
    let resources = resources(BUDGET);
    charge_process_caches(resources.host(), resources.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().unwrap();
    let reference_path = root.path().join("reference.ms");
    let reference_channels = channels.min(4096);
    let mut reference = configured(&reference_path, &measures, BUDGET, reference_channels);
    reference.observe(&scan(10)).unwrap();
    reference.close().unwrap();
    let table = root.path().join("wide.cal");
    let reference_ms = casa_ms::MeasurementSet::open(&reference_path).unwrap();
    let first = match reference_ms
        .main_table()
        .cell_accessor(0, "TIME")
        .unwrap()
        .scalar()
        .unwrap()
    {
        casa_types::ScalarValue::Float64(time) => *time,
        _ => panic!("TIME"),
    };
    let rows = (0..slots).flat_map(|index| {
        (0..ANTENNAS as i32).map(move |antenna| SimulatedSolution {
            time_s: if align_times {
                first
                    + 10.0 * (integrations - 1) as f64 * index as f64
                        / slots.saturating_sub(1).max(1) as f64
            } else {
                5.0e9 + 10.0 * index as f64
            },
            field_id: 0,
            spectral_window_id: 0,
            antenna,
            values: SolutionValues::Channels(
                (0..channels)
                    .map(|channel| {
                        Complex32::new(
                            1.0 + if align_times {
                                0.01 * index as f32
                            } else {
                                0.0
                            },
                            0.0001 * channel as f32,
                        )
                    })
                    .collect(),
            ),
        })
    });
    write_simulated_jones_table(&reference_ms, &table, SimulatedJones::Troposphere, rows).unwrap();
    if kind == "B" {
        use casa_tables::{Table, TableOptions};
        use casa_types::{ArrayValue, ScalarValue, Value};
        use ndarray::{ArrayD, IxDyn, ShapeBuilder};
        let mut calibration = Table::open(TableOptions::new(&table)).unwrap();
        for row in 0..calibration.row_count() {
            let ArrayValue::Complex32(values) = calibration
                .cell_accessor(row, "CPARAM")
                .unwrap()
                .array()
                .unwrap()
            else {
                panic!("CPARAM")
            };
            let values = ArrayD::from_shape_fn(IxDyn(&[2, channels]).f(), |index| {
                values[[0, index[1]]]
                    * if index[0] == 0 {
                        Complex32::new(1.0, 0.0)
                    } else {
                        Complex32::new(0.8, 0.3)
                    }
            });
            calibration
                .cell_accessor_mut(row, "CPARAM")
                .unwrap()
                .set(Value::Array(ArrayValue::Complex32(values)))
                .unwrap();
            for column in ["FLAG", "PARAMERR", "SNR"] {
                let value = if column == "FLAG" {
                    ArrayValue::Bool(ArrayD::from_elem(IxDyn(&[2, channels]).f(), false))
                } else {
                    ArrayValue::Float32(ArrayD::from_elem(IxDyn(&[2, channels]).f(), 0.0))
                };
                calibration
                    .cell_accessor_mut(row, column)
                    .unwrap()
                    .set(Value::Array(value))
                    .unwrap();
            }
        }
        calibration.keywords_mut().upsert(
            "VisCal",
            Value::Scalar(ScalarValue::String("B Jones".into())),
        );
        let mut info = calibration.info().clone();
        info.sub_type = "B Jones".into();
        calibration.set_info(info);
        calibration.save(TableOptions::new(&table)).unwrap();
    }
    if channels != reference_channels {
        // The calibration axis, rather than a huge unrelated reference MS,
        // is what this loading law must exercise.
        use casa_tables::{Table, TableOptions};
        use casa_types::{ArrayValue, ScalarValue, Value};
        use ndarray::{ArrayD, IxDyn};
        let path = table.join("SPECTRAL_WINDOW");
        let mut spectral = Table::open(TableOptions::new(&path)).unwrap();
        for column in ["CHAN_FREQ", "CHAN_WIDTH", "EFFECTIVE_BW", "RESOLUTION"] {
            let ArrayValue::Float64(old) =
                spectral.cell_accessor(0, column).unwrap().array().unwrap()
            else {
                panic!("frequency axis")
            };
            let first = old[[0]];
            let step = if column == "CHAN_FREQ" {
                old[[1]] - first
            } else {
                0.0
            };
            let values = ArrayD::from_shape_vec(
                IxDyn(&[channels]),
                (0..channels)
                    .map(|channel| first + step * channel as f64)
                    .collect(),
            )
            .unwrap();
            spectral
                .cell_accessor_mut(0, column)
                .unwrap()
                .set(Value::Array(ArrayValue::Float64(values)))
                .unwrap();
        }
        spectral
            .cell_accessor_mut(0, "NUM_CHAN")
            .unwrap()
            .set(Value::Scalar(ScalarValue::Int32(channels as i32)))
            .unwrap();
        let width = reference_ms
            .spectral_window()
            .unwrap()
            .chan_width(0)
            .unwrap()[0];
        spectral
            .cell_accessor_mut(0, "TOTAL_BANDWIDTH")
            .unwrap()
            .set(Value::Scalar(ScalarValue::Float64(width * channels as f64)))
            .unwrap();
        spectral.save(TableOptions::new(&path)).unwrap();
    }
    let apply = ApplySpec {
        table: table.to_string_lossy().into_owned(),
        kind: String::new(),
        interval_s: 0.0,
        interp: interp.to_string(),
        calwt: false,
        spwmap: vec![-1],
        opacity: 0.0,
    };
    let session = |name: &str| {
        let mut sm = configured(&root.path().join(name), &measures, BUDGET, data_channels);
        sm.observe(&scan(integrations)).unwrap();
        sm
    };
    let mut warm = session("warm.ms");
    if warm.set_apply(&apply).is_ok() {
        warm.corrupt().unwrap();
    }
    drop(warm);
    let mut sm = session("measured.ms");
    let mut result = None;
    let loaded = measured(|| result = Some(sm.set_apply(&apply)));
    // Inspect peak admission before requiring success so the original
    // loader's under-charge is independently observable even though Tf
    // application was not implemented at that revision.
    check("wide setapply", &loaded);
    result.unwrap().unwrap();
    let corrupted = measured(|| sm.corrupt().unwrap());
    check("wide corrupt", &corrupted);
}
