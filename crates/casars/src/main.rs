// SPDX-License-Identifier: LGPL-3.0-or-later
fn main() {
    if let Err(error) = casars::run() {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}
