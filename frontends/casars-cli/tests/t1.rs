// SPDX-License-Identifier: LGPL-3.0-or-later

//! T1 end-to-end capability tests (the T1 tier of `TESTING.md`):
//! analytic skies observed by the synthetic MeasurementSet generator and
//! imaged through `casars-imager`'s production route, checked against
//! analytic flux, position, beam, noise, inventory and WCS expectations.

#[path = "t1_cases/aw_projection.rs"]
mod aw_projection;
#[path = "t1_cases/awp2.rs"]
mod awp2;
#[path = "t1_cases/casa_return.rs"]
mod casa_return;
#[path = "t1_cases/continuation.rs"]
mod continuation;
#[path = "t1_cases/cube.rs"]
mod cube;
#[path = "t1_cases/diagnostics.rs"]
mod diagnostics;
#[path = "t1_cases/ephemeris.rs"]
mod ephemeris;
#[path = "t1_cases/first_visibility.rs"]
mod first_visibility;
#[path = "t1_cases/fixture.rs"]
mod fixture;
#[path = "t1_cases/interrupt.rs"]
mod interrupt;
#[path = "t1_cases/metal.rs"]
mod metal;
#[path = "t1_cases/model_column.rs"]
mod model_column;
#[path = "t1_cases/mosaic.rs"]
mod mosaic;
#[path = "t1_cases/mosaic_evla.rs"]
mod mosaic_evla;
#[path = "t1_cases/mosaic_psf.rs"]
mod mosaic_psf;
#[path = "t1_cases/msv4.rs"]
mod msv4;
#[path = "t1_cases/mtmfs.rs"]
mod mtmfs;
#[path = "t1_cases/mvc.rs"]
mod mvc;
#[path = "t1_cases/non_square.rs"]
mod non_square;
#[path = "t1_cases/pb_default.rs"]
mod pb_default;
#[path = "t1_cases/plane_masks.rs"]
mod plane_masks;
#[path = "t1_cases/projection.rs"]
mod projection;
#[path = "t1_cases/reference_frequency.rs"]
mod reference_frequency;
#[path = "t1_cases/restart/mod.rs"]
mod restart;
#[path = "t1_cases/restoration.rs"]
mod restoration;
#[path = "t1_cases/routes.rs"]
mod routes;
#[path = "t1_cases/selection.rs"]
mod selection;
#[path = "t1_cases/selection_antenna_time.rs"]
mod selection_antenna_time;
#[path = "t1_cases/selection_numeric.rs"]
mod selection_numeric;
#[path = "t1_cases/selection_spw.rs"]
mod selection_spw;
#[path = "t1_cases/standard_mfs.rs"]
mod standard_mfs;
#[path = "t1_cases/standard_mfs_masks.rs"]
mod standard_mfs_masks;
#[path = "t1_cases/start_model.rs"]
mod start_model;
#[path = "t1_cases/stokes.rs"]
mod stokes;
#[path = "t1_cases/uv_taper.rs"]
mod uv_taper;
#[path = "t1_cases/virtual_model.rs"]
mod virtual_model;
#[path = "t1_cases/vis_list.rs"]
mod vis_list;
#[path = "t1_cases/w_projection.rs"]
mod w_projection;
#[path = "t1_cases/weighting.rs"]
mod weighting;
