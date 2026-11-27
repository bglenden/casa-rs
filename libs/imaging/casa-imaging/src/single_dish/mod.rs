// SPDX-License-Identifier: LGPL-3.0-or-later
//! Total-power imaging through the observation boundary and the shared SD gridder.
mod coordinates;
mod parameters;
mod plan;
mod pointing;
mod products;
mod stream;
pub use parameters::{TotalPowerParameters, default_parameters};
pub use products::{
    PublishedTotalPower, TotalPowerProducts, image_observation, publish_total_power,
};

use crate::RunContext;
use casa_coordinates::{CoordinateSystem, DirectionCoordinate};
use casa_imaging_runtime::{Admitted, Demand, admit, charge_process_caches};
use casa_imaging_singledish::{GridFunction, Pointing, TotalPowerImage};
use casa_observation::Observation;
use std::ops::Range;

/// Explicit geometry and native-channel gridding requested by `tsdimaging`.
pub struct TotalPowerRequest {
    /// Direction coordinate, including phase centre, signed cells and reference pixels.
    pub direction: DirectionCoordinate,
    /// Image width and height.
    pub size: [usize; 2],
    /// Native input channels, without spectral regridding.
    pub channels: Range<usize>,
    /// Exact observing intent; an empty string selects all intents.
    pub intent: String,
    /// CASA's gridding function and resolved pixel parameters.
    pub kernel: GridFunction,
    /// Rest frequency override, Hz; otherwise SOURCE, then mean SPW frequency.
    pub rest_frequency_hz: Option<f64>,
    /// Selected spectral windows; empty selects all compatible windows.
    pub windows: Vec<casa_observation::WindowId>,
    /// Selected autocorrelation antennas; empty selects all antennas.
    pub antennas: Vec<casa_observation::AntennaId>,
}

/// The admitted scientific arrays and their coordinates, before publication.
pub struct TotalPowerOutcome {
    /// Image and weight, with the grid's retained reservation.
    pub arrays: Admitted<TotalPowerImage>,
    /// Shared image/weight WCS.
    pub coordinates: CoordinateSystem,
    /// Selected unflagged sample pointings, for raster/beam estimation.
    pub sampling: Vec<Pointing>,
    source_partition: casa_observation::PartitionId,
}

/// Why total-power imaging could not be prepared or run.
#[derive(Debug, thiserror::Error)]
pub enum TotalPowerError {
    /// Invalid setup, failed admission or observation/image I/O.
    #[error("tsdimaging: {0}")]
    Execution(String),
    /// A data-dependent mode outside the task's current core.
    #[error("tsdimaging: {behavior} is not implemented; see #{issue}")]
    Unsupported {
        /// Requested observable behavior.
        behavior: &'static str,
        /// Work item owning the remaining implementation.
        issue: u32,
    },
}

fn failure(error: impl std::fmt::Display) -> TotalPowerError {
    TotalPowerError::Execution(error.to_string())
}

/// Open the input with bounded admission and execute resolved CASA parameters.
///
/// # Errors
/// Invalid parameters, observation opening, admission, gridding or publication.
pub fn image_from_parameters(
    values: &serde_json::Map<String, serde_json::Value>,
    context: &RunContext,
) -> Result<PublishedTotalPower, TotalPowerError> {
    let parameters = TotalPowerParameters(values);
    charge_process_caches(&context.host, &context.policy).map_err(failure)?;
    let _metadata = admit(
        &context.host,
        &context.policy,
        &Demand {
            phase: "total-power task parameters",
            memory: 16 << 10,
        },
    )
    .map_err(failure)?;
    let opened = crate::compile::opening::open_observation(
        &parameters.input()?,
        Default::default(),
        (&context.host, &context.policy),
    )
    .map_err(failure)?;
    let observation = opened.value.observation();
    let _selection = admit(
        &context.host,
        &context.policy,
        &Demand {
            phase: "total-power parameter selection",
            memory: (8 * observation.catalogue().partitions.len()) as u64,
        },
    )
    .map_err(failure)?;
    let (request, products) = parameters.request(observation.catalogue())?;
    image_observation(observation, &request, &products, context)
}

/// Merge sparse `tsdimaging` overrides over [`default_parameters`] and execute
/// total-power imaging.
///
/// # Errors
///
/// Invalid parameters, observation opening, admission, gridding or publication.
pub fn image_from_sparse_parameters(
    overrides: &serde_json::Map<String, serde_json::Value>,
    context: &RunContext,
) -> Result<PublishedTotalPower, TotalPowerError> {
    charge_process_caches(&context.host, &context.policy).map_err(failure)?;
    let _sparse = admit(
        &context.host,
        &context.policy,
        &Demand {
            phase: "total-power sparse parameter defaults",
            memory: 16 << 10,
        },
    )
    .map_err(failure)?;
    let mut merged = default_parameters();
    for (key, value) in overrides {
        merged.insert(key.clone(), value.clone());
    }
    image_from_parameters(&merged, context)
}

/// Grid an observation's selected autocorrelations in the image plane.
///
/// The numerical core reads MSv2 and MSv4 through `casa-observation`.
/// Inputs stay read-only; publication is a separate final operation.
///
/// # Errors
/// Missing intent/data, incompatible spectral axes, unsupported correlation or
/// pointing cases, invalid geometry, resource admission or observation I/O.
pub fn grid_observation(
    observation: &dyn Observation,
    request: &TotalPowerRequest,
    context: &RunContext,
) -> Result<TotalPowerOutcome, TotalPowerError> {
    if context.cancel.is_cancelled() {
        return Err(failure("cancelled before gridding"));
    }
    charge_process_caches(&context.host, &context.policy).map_err(failure)?;
    let mut metadata = admit(
        &context.host,
        &context.policy,
        &Demand {
            phase: "total-power planning and coordinates",
            memory: (16 << 10) + (8 * observation.catalogue().partitions.len()) as u64,
        },
    )
    .map_err(failure)?;
    let plan = plan::Plan::new(observation, request)?;
    let _measures_memory = admit(
        &context.host,
        &context.policy,
        &Demand {
            phase: "total-power measures",
            memory: casa_ms::MEASURES_RUNTIME_BYTES as u64,
        },
    )
    .map_err(failure)?;
    let measures = casa_ms::open_measures_runtime().map_err(failure)?;
    let (mut grid, _kernel_memory) = plan.grid(observation, request, context)?;
    let (sampling, sampling_memory, date) = stream::grid(
        observation,
        request,
        context,
        &plan.partitions,
        &measures,
        &mut grid,
    )?;
    let mut arrays = grid.finish();
    arrays.reservation.join(sampling_memory);
    metadata.retain(16 << 10);
    arrays.reservation.join(metadata);
    let mut coordinates = plan.coordinates;
    let catalogue = observation.catalogue();
    let source = catalogue.observation(catalogue.partition(plan.partitions[0]).observation);
    let information = coordinates.obs_info_mut();
    information.telescope = source.telescope.clone();
    information.observer = source.observers.first().cloned().unwrap_or_default();
    information.date = date;
    Ok(TotalPowerOutcome {
        arrays,
        coordinates,
        sampling,
        source_partition: plan.partitions[0],
    })
}
