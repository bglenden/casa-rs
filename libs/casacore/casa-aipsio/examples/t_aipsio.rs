// SPDX-License-Identifier: LGPL-3.0-or-later
#[path = "support/taipsio.rs"]
mod taipsio;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = taipsio::run_taipsio_like_demo()?;
    print!("{output}");
    Ok(())
}
