// SPDX-License-Identifier: LGPL-3.0-or-later
//! Laws of the major-cycle pass on synthetic native rows: partition and
//! worker-count invariance, waves equal one resident pass, the residual of a
//! predicted model vanishes (across image domains, and in waves whose output
//! channels are narrower than native ones), residency planning, the streamed
//! density grid equals the one-shot build, cancellation stops the pass, and
//! the pass on Metal equals the pass on the CPU.

mod domains;
mod fixture;
mod invariance;
mod metal;
mod residency;
mod residual;
