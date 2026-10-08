// SPDX-License-Identifier: LGPL-3.0-or-later

fn main() {
    println!("cargo:rustc-check-cfg=cfg(coverage)");
    println!("cargo:rerun-if-changed=src/kernels.metal");
}
