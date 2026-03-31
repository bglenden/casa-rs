// SPDX-License-Identifier: LGPL-3.0-or-later

#[path = "common/mod.rs"]
mod common;

use common::imexplore_movie::{Stage1Config, run_stage1};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Stage1Config::parse_from_env_args()?;
    run_stage1(config)
}
