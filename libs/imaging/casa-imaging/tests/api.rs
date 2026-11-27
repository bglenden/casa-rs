// SPDX-License-Identifier: LGPL-3.0-or-later

#[allow(dead_code)]
mod common;
#[allow(dead_code)]
#[path = "common/imaging.rs"]
mod imaging;
#[allow(dead_code)]
#[path = "common/synthetic_observation.rs"]
mod synthetic_observation;

mod availability;
mod continuum_application;
mod ms_trial_imaging;
mod single_dish_task_casa;
mod w_facets_memory;
