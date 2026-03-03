// SPDX-License-Identifier: LGPL-3.0-or-later
use casacore_test_support::{
    AipsIoCrossError, cpp_backend_available, primitive_cross_check_values, run_aipsio_cross_matrix,
};

#[test]
fn primitive_aipsio_rust_cpp_cross_matrix() {
    if !cpp_backend_available() {
        eprintln!("skipping C++ AipsIO cross-matrix test: casacore backend unavailable");
        return;
    }

    let values = primitive_cross_check_values();
    let result = run_aipsio_cross_matrix(&values);

    if let Err(err) = result {
        match err {
            AipsIoCrossError::CppUnavailable => {
                eprintln!("skipping C++ AipsIO cross-matrix test: {err}");
            }
            other => panic!("cross-matrix failed: {other}"),
        }
    }
}
