// SPDX-License-Identifier: LGPL-3.0-or-later

//! T1 end-to-end capability tests (imaging foundation plan, section 7):
//! analytic skies observed by the synthetic MeasurementSet generator and
//! imaged through `casars-imager`'s production route, checked against
//! analytic flux, position, beam, noise, inventory and WCS expectations.

#[path = "t1/aw_projection.rs"]
mod aw_projection;
#[path = "t1/casa_return.rs"]
mod casa_return;
#[path = "t1/cube.rs"]
mod cube;
#[path = "t1/diagnostics.rs"]
mod diagnostics;
#[path = "t1/ephemeris.rs"]
mod ephemeris;
#[path = "t1/fixture.rs"]
mod fixture;
#[path = "t1/interrupt.rs"]
mod interrupt;
#[path = "t1/metal.rs"]
mod metal;
#[path = "t1/model_column.rs"]
mod model_column;
#[path = "t1/mosaic.rs"]
mod mosaic;
#[path = "t1/mtmfs.rs"]
mod mtmfs;
#[path = "t1/non_square.rs"]
mod non_square;
#[path = "t1/pb_default.rs"]
mod pb_default;
#[path = "t1/restart.rs"]
mod restart;
#[path = "t1/routes.rs"]
mod routes;
#[path = "t1/standard_mfs.rs"]
mod standard_mfs;
#[path = "t1/standard_mfs_masks.rs"]
mod standard_mfs_masks;
#[path = "t1/start_model.rs"]
mod start_model;
#[path = "t1/uv_taper.rs"]
mod uv_taper;
#[path = "t1/w_projection.rs"]
mod w_projection;
