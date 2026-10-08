// SPDX-License-Identifier: LGPL-3.0-or-later
//! Laws of the major-cycle pass on synthetic native rows: partition and
//! worker-count invariance, waves equal one resident pass, the residual of a
//! predicted model vanishes (across image domains, and in waves whose output
//! channels are narrower than native ones), residency planning, the streamed
//! density grid equals the one-shot build, and cancellation stops the pass.

mod domains;
mod fixture;
mod invariance;
mod residency;
mod residual;
