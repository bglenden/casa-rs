// SPDX-License-Identifier: LGPL-3.0-or-later

#[path = "common/mod.rs"]
mod common;

use common::imexplore_movie::{Stage2Config, run_stage2};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Stage2Config::parse_from_env_args()?;
    run_stage2(config)
}
