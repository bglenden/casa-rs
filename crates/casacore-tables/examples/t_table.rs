// SPDX-License-Identifier: LGPL-3.0-or-later
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = casacore_tables::demo::run_ttable_like_demo()?;
    print!("{output}");
    Ok(())
}
