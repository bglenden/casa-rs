// SPDX-License-Identifier: LGPL-3.0-or-later

fn main() {
    println!("cargo:rustc-check-cfg=cfg(has_casacore_cpp)");
}
