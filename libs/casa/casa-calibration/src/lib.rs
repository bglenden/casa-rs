// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

#[cfg(feature = "solve-test-hooks")]
#[doc(hidden)]
pub mod solve_test_hooks;

mod accumulate;
mod antpos_vla;
pub mod appendantab;
pub mod apply;
pub mod attachment;
pub mod bandpass;
mod calanalysis;
mod calibrater_lifecycle;
pub mod calibration_service;
pub mod callib;
pub mod constants;
pub mod continuum;
pub mod corrected_export;
mod execute;
mod flagdata_model;
pub mod fluxscale;
pub mod ft;
pub mod gencal;
#[cfg(test)]
mod imaging_memory_test;
pub mod initweights;
pub mod interpolation;
mod legacy_spline;
pub mod listing;
mod lookup;
mod lookup_frequency;
mod model;
pub mod model_columns;
pub mod models;
pub mod output;
mod output_merge;
pub mod paths;
pub mod pccor;
mod plan;
pub mod polfromgain;
pub mod rerefant;
pub mod sd_solver;
mod selected_data;
mod session;
mod simulated;
pub mod smooth;
mod solution_edit;
pub mod solve;
pub mod solve_stats;
mod stats;
pub mod statwt;
mod summary;
pub mod table;
pub mod table_maintenance;
pub mod timing;
pub mod transform_calibration;
mod transform_models;
mod uvmodelfit;
pub mod uvsub;
pub mod weather;
mod weight_spectra;
mod writer;
pub mod wvr;
