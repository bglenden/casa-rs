// SPDX-License-Identifier: LGPL-3.0-or-later
fn main() {
    println!("cargo:rustc-check-cfg=cfg(has_casacore_cpp)");

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

    for include in &casacore.include_paths {
        build.include(include);
    }

    build.compile("casacore_cpp_shims");

    for path in &casacore.link_paths {
        println!("cargo:rustc-link-search=native={}", path.display());
    }

    for lib in &casacore.libs {
        println!("cargo:rustc-link-lib={lib}");
    }

    println!("cargo:rustc-cfg=has_casacore_cpp");
}
