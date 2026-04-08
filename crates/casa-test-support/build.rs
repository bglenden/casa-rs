// SPDX-License-Identifier: LGPL-3.0-or-later
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    println!("cargo:rustc-check-cfg=cfg(has_casacore_cpp)");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/cpp");

    let casacore = pkg_config::Config::new().probe("casacore");
    let casacore = match casacore {
        Ok(lib) => lib,
        Err(err) => {
            println!(
                "cargo:warning=casacore pkg-config not found; C++ cross-tests disabled: {err}"
            );
            return;
        }
    };

    let mut build = cc::Build::new();
    build.cpp(true);
    build.flag_if_supported("-std=c++17");
    build.include("src/cpp");
    build.file("src/cpp/casacore_cpp_aipsio_shim.cpp");
    build.file("src/cpp/casacore_cpp_table_aipsio.cpp");
    build.file("src/cpp/casacore_cpp_table_ssm.cpp");
    build.file("src/cpp/casacore_cpp_table_misc.cpp");
    build.file("src/cpp/casacore_cpp_table_ism.cpp");
    build.file("src/cpp/casacore_cpp_table_tiled.cpp");
    build.file("src/cpp/casacore_cpp_table_virtual.cpp");
    build.file("src/cpp/casacore_cpp_table_aipsio_vararray.cpp");
    build.file("src/cpp/casacore_cpp_table_ssm_vararray.cpp");
    build.file("src/cpp/casacore_cpp_table_vararray_bench.cpp");
    build.file("src/cpp/casacore_cpp_taql.cpp");
    build.file("src/cpp/casacore_cpp_table_perf_bench.cpp");
    build.file("src/cpp/casacore_cpp_quanta_shim.cpp");
    build.file("src/cpp/casacore_cpp_table_quantum.cpp");
    build.file("src/cpp/casacore_cpp_measures_shim.cpp");
    build.file("src/cpp/casacore_cpp_table_complex_vararray.cpp");
    build.file("src/cpp/casacore_cpp_ms.cpp");
    build.file("src/cpp/casacore_cpp_image_shim.cpp");
    build.file("src/cpp/casacore_cpp_lattice_stats.cpp");

    for include in &casacore.include_paths {
        build.include(include);
    }

    if env::var("CARGO_CFG_TARGET_VENDOR").as_deref() == Ok("apple") {
        build.cargo_metadata(false);
        let objects = build
            .try_compile_intermediates()
            .expect("compile casacore_cpp_shims objects");
        let out_dir =
            PathBuf::from(env::var_os("OUT_DIR").expect("Cargo sets OUT_DIR for build scripts"));
        let archive = out_dir.join("libcasacore_cpp_shims.a");
        archive_static_library(&build, &archive, &objects);
        println!("cargo:rustc-link-lib=static=casacore_cpp_shims");
        println!("cargo:rustc-link-search=native={}", out_dir.display());
        println!("cargo:rustc-link-lib=c++");
    } else {
        build.compile("casacore_cpp_shims");
    }

    for path in &casacore.link_paths {
        println!("cargo:rustc-link-search=native={}", path.display());
    }

    for lib in &casacore.libs {
        println!("cargo:rustc-link-lib={lib}");
    }

    println!("cargo:rustc-cfg=has_casacore_cpp");
}

fn archive_static_library(build: &cc::Build, archive: &Path, objects: &[PathBuf]) {
    let _ = fs::remove_file(archive);

    let mut ar = build.try_get_archiver().expect("resolve archiver");
    ar.env("ZERO_AR_DATE", "1");
    ar.arg("cq").arg(archive);
    ar.args(objects);
    run_checked(&mut ar, "archive casacore_cpp_shims");

    let mut ranlib = build.try_get_ranlib().expect("resolve ranlib");
    ranlib.env("ZERO_AR_DATE", "1");
    ranlib.arg(archive);
    run_checked(&mut ranlib, "index casacore_cpp_shims");
}

fn run_checked(command: &mut Command, action: &str) {
    let status = command
        .status()
        .unwrap_or_else(|err| panic!("{action} failed to start: {err}"));
    assert!(status.success(), "{action} failed with status {status}");
}
