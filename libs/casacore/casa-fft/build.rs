// SPDX-License-Identifier: LGPL-3.0-or-later

fn main() {
    println!("cargo:rerun-if-env-changed=DOCS_RS");
    let docs_rs = std::env::var_os("DOCS_RS").is_some();
    let linux = std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux");
    // The docs.rs Linux image may either lack FFTW or ship shared libraries.
    let linkage = if linux && docs_rs { "dylib" } else { "static" };
    // Link the GPL FFTW objects into the affected binaries. The independent
    // Rust sources retain their LGPL grant; linked distributions use GPLv3.
    let mut link_paths = Vec::new();
    for library in ["fftw3f", "fftw3"] {
        match pkg_config::Config::new()
            .cargo_metadata(false)
            .probe(library)
        {
            Ok(found) => link_paths.extend(found.link_paths),
            Err(_) if docs_rs => return,
            Err(error) => panic!("{library} development library required: {error}"),
        }
    }
    for path in link_paths {
        println!("cargo:rustc-link-search=native={}", path.display());
    }
    for library in ["fftw3f_threads", "fftw3_threads", "fftw3f", "fftw3"] {
        println!("cargo:rustc-link-lib={linkage}={library}");
    }
    if linux {
        println!("cargo:rustc-link-lib=m");
        println!("cargo:rustc-link-lib=pthread");
    }
}
