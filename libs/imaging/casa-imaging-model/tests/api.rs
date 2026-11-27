// SPDX-License-Identifier: LGPL-3.0-or-later

#[path = "fixtures/batch_observation.rs"]
mod batch_observation;
#[allow(dead_code)]
mod common;
#[allow(dead_code)]
#[path = "fixtures/model_lifecycle.rs"]
mod model_lifecycle_fixture;

mod batch_geometry;
mod batch_geometry_ephemeris;
mod compiled_geometry;
mod compiled_problem;
mod measurement_equation_contract;
mod observation_snapshot;
mod observation_transaction;
mod optimum_size_casa;
