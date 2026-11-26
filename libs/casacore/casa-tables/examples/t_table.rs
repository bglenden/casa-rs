// SPDX-License-Identifier: LGPL-3.0-or-later
#[path = "support/table_demos.rs"]
mod table_demos;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = table_demos::run_ttable_like_demo()?;
    print!("{output}");
    Ok(())
}
