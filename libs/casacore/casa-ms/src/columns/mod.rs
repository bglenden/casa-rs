// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed column accessors that return physical measure types.
//!
//! These accessors wrap raw table columns and return values as
//! `MEpoch`, `MDirection`, `MPosition`, or `MFrequency`
//! instead of raw doubles, providing a type-safe interface to
//! the physical quantities stored in the MS.

pub mod data_columns;
pub mod direction_columns;
pub mod exposure_interval;
pub mod flag_columns;
pub mod frequency_columns;
pub mod main_ids;
pub mod position_columns;
pub mod time_columns;
pub mod uvw_column;
pub mod weight_columns;
