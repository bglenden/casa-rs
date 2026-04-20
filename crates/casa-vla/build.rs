// SPDX-License-Identifier: LGPL-3.0-or-later
use std::env;

fn main() {
    println!("cargo:rustc-check-cfg=cfg(has_casacore_cpp)");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_CPP_INTEROP_TESTS");

    if env::var_os("CARGO_FEATURE_CPP_INTEROP_TESTS").is_none() {
        return;
    }

    if pkg_config::Config::new().probe("casacore").is_ok() {
        println!("cargo:rustc-cfg=has_casacore_cpp");
    }
}
