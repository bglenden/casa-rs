// SPDX-License-Identifier: LGPL-3.0-or-later

fn main() {
    // Link the GPL FFTW objects into the affected binaries. The independent
    // Rust sources retain their LGPL grant; linked distributions use GPLv3.
    for library in ["fftw3f", "fftw3"] {
        let found = pkg_config::Config::new()
            .cargo_metadata(false)
            .probe(library)
            .unwrap_or_else(|error| panic!("{library} development library required: {error}"));
        for path in found.link_paths {
            println!("cargo:rustc-link-search=native={}", path.display());
        }
    }
    for library in ["fftw3f_threads", "fftw3_threads", "fftw3f", "fftw3"] {
        println!("cargo:rustc-link-lib=static={library}");
    }
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
        println!("cargo:rustc-link-lib=m");
        println!("cargo:rustc-link-lib=pthread");
    }
}
