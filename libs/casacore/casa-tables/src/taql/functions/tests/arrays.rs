// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

fn make_2d_array_f(rows: usize, cols: usize, vals: Vec<f64>) -> ExprValue {
    ExprValue::Array(super::super::super::eval::ArrayValue {
        shape: vec![rows, cols],
        data: vals.into_iter().map(ExprValue::Float).collect(),
    })
}

#[test]
fn sums_full_reduction() {
    let v = call("sums", vec![make_array(vec![1.0, 2.0, 3.0])]);
    assert_eq!(v, ExprValue::Float(6.0));
}

#[test]
fn means_full_reduction() {
    let v = call("means", vec![make_array(vec![2.0, 4.0, 6.0])]);
    assert_eq!(v, ExprValue::Float(4.0));
}

#[test]
fn mins_full_reduction() {
    let v = call("mins", vec![make_array(vec![3.0, 1.0, 4.0])]);
    assert_eq!(v, ExprValue::Float(1.0));
}

#[test]
fn sums_axis0_2d() {
    // 2x3 array: [[1,2,3],[4,5,6]] stored in column-major flat order
    // Sum along axis 1 (1-based) = axis 0 (0-based) → [5, 7, 9]
    let v = call(
        "sums",
        vec![
            make_2d_array_f(2, 3, vec![1.0, 4.0, 2.0, 5.0, 3.0, 6.0]),
            ExprValue::Int(1),
        ],
    );
    match v {
        ExprValue::Array(a) => {
            assert_eq!(a.shape, vec![3]);
            let vals: Vec<f64> = a.data.iter().map(|v| v.to_float().unwrap()).collect();
            assert_eq!(vals, vec![5.0, 7.0, 9.0]);
        }
        _ => panic!("expected Array"),
    }
}

#[test]
fn sums_axis1_2d() {
    // 2x3 array: [[1,2,3],[4,5,6]] stored in column-major flat order
    // Sum along axis 2 (1-based) = axis 1 (0-based) → [6, 15]
    let v = call(
        "sums",
        vec![
            make_2d_array_f(2, 3, vec![1.0, 4.0, 2.0, 5.0, 3.0, 6.0]),
            ExprValue::Int(2),
        ],
    );
    match v {
        ExprValue::Array(a) => {
            assert_eq!(a.shape, vec![2]);
            let vals: Vec<f64> = a.data.iter().map(|v| v.to_float().unwrap()).collect();
            assert_eq!(vals, vec![6.0, 15.0]);
        }
        _ => panic!("expected Array"),
    }
}

#[test]
fn means_axis0_2d() {
    // 2x3 array: [[1,2,3],[4,5,6]] stored in column-major flat order
    // Mean along axis 1 (1-based) = axis 0 (0-based)
    let v = call(
        "means",
        vec![
            make_2d_array_f(2, 3, vec![1.0, 4.0, 2.0, 5.0, 3.0, 6.0]),
            ExprValue::Int(1),
        ],
    );
    match v {
        ExprValue::Array(a) => {
            assert_eq!(a.shape, vec![3]);
            let vals: Vec<f64> = a.data.iter().map(|v| v.to_float().unwrap()).collect();
            assert_eq!(vals, vec![2.5, 3.5, 4.5]);
        }
        _ => panic!("expected Array"),
    }
}

#[test]
fn maxs_axis1_2d() {
    // 2x3 array: [[1,5,3],[4,2,6]] stored in column-major flat order
    // Max along axis 2 (1-based) = axis 1 (0-based)
    let v = call(
        "maxs",
        vec![
            make_2d_array_f(2, 3, vec![1.0, 4.0, 5.0, 2.0, 3.0, 6.0]),
            ExprValue::Int(2),
        ],
    );
    match v {
        ExprValue::Array(a) => {
            assert_eq!(a.shape, vec![2]);
            let vals: Vec<f64> = a.data.iter().map(|v| v.to_float().unwrap()).collect();
            assert_eq!(vals, vec![5.0, 6.0]);
        }
        _ => panic!("expected Array"),
    }
}

#[test]
fn sums_axis_out_of_range() {
    // 1D array: axis 2 (1-based) is out of range
    let result = call_err("sums", &[make_array(vec![1.0, 2.0]), ExprValue::Int(2)]);
    assert!(result.is_err());
}

#[test]
fn anys_full_reduction() {
    let v = call("anys", vec![make_array(vec![0.0, 0.0, 1.0])]);
    assert_eq!(v, ExprValue::Bool(true));
}

#[test]
fn alls_full_reduction() {
    let v = call("alls", vec![make_array(vec![1.0, 1.0, 0.0])]);
    assert_eq!(v, ExprValue::Bool(false));
}
#[test]
fn transpose_2d() {
    // [[1,2,3],[4,5,6]] → [[1,4],[2,5],[3,6]]
    // Input column-major flat: [1,4,2,5,3,6]
    // Output column-major flat: [1,2,3,4,5,6]
    let v = call(
        "transpose",
        vec![make_2d_array_f(2, 3, vec![1.0, 4.0, 2.0, 5.0, 3.0, 6.0])],
    );
    match v {
        ExprValue::Array(a) => {
            assert_eq!(a.shape, vec![3, 2]);
            let vals: Vec<f64> = a.data.iter().map(|v| v.to_float().unwrap()).collect();
            assert_eq!(vals, vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
        }
        _ => panic!("expected Array"),
    }
}

#[test]
fn diagonal_3x3() {
    // 3x3 array: [[1,2,3],[4,5,6],[7,8,9]] stored in column-major flat order
    let v = call(
        "diagonal",
        vec![make_2d_array_f(
            3,
            3,
            vec![1.0, 4.0, 7.0, 2.0, 5.0, 8.0, 3.0, 6.0, 9.0],
        )],
    );
    match v {
        ExprValue::Array(a) => {
            assert_eq!(a.shape, vec![3]);
            let vals: Vec<f64> = a.data.iter().map(|v| v.to_float().unwrap()).collect();
            assert_eq!(vals, vec![1.0, 5.0, 9.0]);
        }
        _ => panic!("expected Array"),
    }
}
