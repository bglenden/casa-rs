// SPDX-License-Identifier: LGPL-3.0-or-later
mod antenna_selection_oracle;
mod field_selection_oracle;
#[cfg(feature = "slow-tests")]
mod moving_geometry_casa;
mod ms_create_roundtrip;
mod ms_library_coverage;
#[cfg(feature = "performance-tests")]
mod ms_perf_vs_cpp;
#[cfg(feature = "msv4")]
mod observation_io;
mod pointing_subtable_regression;
#[cfg(feature = "slow-tests")]
mod scratch_columns_casa;
mod source_subtable_regression;
mod spw_selection_oracle;
#[cfg(feature = "slow-tests")]
mod virtual_model_casa;
