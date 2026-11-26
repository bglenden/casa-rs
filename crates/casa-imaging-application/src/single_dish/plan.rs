// SPDX-License-Identifier: LGPL-3.0-or-later
//! Select compatible native spectral axes and admit the grid before forming it.
use super::{TotalPowerError, TotalPowerRequest, coordinates::rest_frequency, failure};
use crate::RunContext;
use casa_coordinates::{
    Coordinate, CoordinateSystem, SpectralCoordinate, StokesCoordinate, StokesType,
};
use casa_imaging_runtime::{Demand, Reservation, admit};
use casa_imaging_singledish::{FlagMerge, GridFunction, GridShape, KernelTable, TotalPowerGrid};
use casa_numerics::PrimaryBeam;
use casa_observation::{Catalogue, Observation, PartitionId, WindowId};

pub(super) fn selected_partitions(
    catalogue: &Catalogue,
    intent: &str,
    windows: &[WindowId],
) -> Result<Vec<PartitionId>, TotalPowerError> {
    let mut partitions = if intent.is_empty() {
        catalogue.partition_ids()
    } else {
        catalogue.partitions_with_intent(intent).map_err(failure)?
    };
    partitions
        .retain(|id| windows.is_empty() || windows.contains(&catalogue.partition(*id).window));
    if partitions.is_empty() {
        return Err(failure(
            "No match found for the requested intent and spectral windows",
        ));
    }
    Ok(partitions)
}

pub(super) struct Plan {
    pub partitions: Vec<PartitionId>,
    pub shape: GridShape,
    pub coordinates: CoordinateSystem,
}

impl Plan {
    pub fn new(
        observation: &dyn Observation,
        request: &TotalPowerRequest,
    ) -> Result<Self, TotalPowerError> {
        let catalogue = observation.catalogue();
        let partitions = selected_partitions(catalogue, &request.intent, &request.windows)?;
        let first = *partitions
            .first()
            .ok_or_else(|| failure("No match found for the requested intent"))?;
        let entry = catalogue.partition(first);
        if request.channels.is_empty() || request.channels.end > entry.frequencies_hz.len() {
            return Err(failure(
                "channel selection is outside the input spectral window",
            ));
        }
        let frequencies = &entry.frequencies_hz[request.channels.clone()];
        let step = entry.channel_widths_hz[request.channels.start];
        if frequencies
            .windows(2)
            .any(|v| ((v[1] - v[0]) - step).abs() > step.abs() * 1e-8)
        {
            return Err(failure(
                "nonuniform spectral channels require regridding; see #938",
            ));
        }
        for &id in &partitions {
            let current = catalogue.partition(id);
            if current
                .fields
                .iter()
                .any(|field| catalogue.fields[field.0 as usize].ephemeris.is_some())
            {
                return Err(TotalPowerError::Unsupported {
                    behavior: "moving-source total-power imaging",
                    issue: 938,
                });
            }
            if current.frequency_frame != entry.frequency_frame
                || current.frequencies_hz != entry.frequencies_hz
            {
                return Err(failure(
                    "spectral regridding between different windows is not implemented; see #938",
                ));
            }
            if !(1..=2).contains(&current.correlations.len())
                || current.correlations.iter().any(|c| {
                    !matches!(
                        c,
                        casa_core::Correlation::XX
                            | casa_core::Correlation::YY
                            | casa_core::Correlation::RR
                            | casa_core::Correlation::LL
                            | casa_core::Correlation::I
                    )
                })
            {
                return Err(failure(
                    "single-dish Stokes I imaging requires one or two parallel hands",
                ));
            }
        }
        let shape = GridShape {
            nx: request.size[0],
            ny: request.size[1],
            planes: 1,
            channels: frequencies.len(),
        };
        if shape.nx == 0
            || shape.ny == 0
            || shape
                .nx
                .checked_mul(shape.ny)
                .is_none_or(|n| n > u32::MAX as usize || n.checked_mul(shape.channels).is_none())
        {
            return Err(failure("image dimensions are zero or too large"));
        }
        if request
            .direction
            .increment()
            .iter()
            .any(|v| !v.is_finite() || *v == 0.)
        {
            return Err(failure("image cells must be finite and nonzero"));
        }
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(request.direction.clone());
        coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
        coordinates.add_coordinate(SpectralCoordinate::new(
            entry.frequency_frame,
            frequencies[0],
            step,
            0.,
            rest_frequency(
                catalogue,
                first,
                request.rest_frequency_hz,
                !request.windows.is_empty(),
            )?,
        ));
        Ok(Self {
            partitions,
            shape,
            coordinates,
        })
    }

    pub fn grid(
        &self,
        observation: &dyn Observation,
        request: &TotalPowerRequest,
        context: &RunContext,
    ) -> Result<(TotalPowerGrid, Reservation), TotalPowerError> {
        let catalogue = observation.catalogue();
        let entry = catalogue.partition(self.partitions[0]);
        let frequency = entry.frequencies_hz[request.channels.start];
        let primary = if request.kernel == GridFunction::PrimaryBeam {
            Some(
                PrimaryBeam::for_telescope(
                    &catalogue.observation(entry.observation).telescope,
                    frequency,
                )
                .ok_or_else(|| failure("unsupported telescope primary beam"))?,
            )
        } else {
            None
        };
        let pixels = primary.as_ref().map_or(0, |beam| {
            beam.pattern()
                .support_pixels(request.direction.increment()[0], frequency)
        });
        if request.kernel.support().unwrap_or(pixels) > (i32::MAX as usize / 300) {
            return Err(failure("kernel support exceeds CASA's index range"));
        }
        let mut forming = admit(
            &context.host,
            &context.policy,
            &Demand {
                phase: "total-power kernel",
                memory: request.kernel.forming_bytes(pixels),
            },
        )
        .map_err(failure)?;
        let table = if let Some(primary) = primary {
            KernelTable::primary_beam(
                &primary.pattern(),
                request.direction.increment()[0],
                frequency,
            )
        } else {
            KernelTable::new(request.kernel).map_err(failure)?
        };
        forming.retain(table.bytes());
        let grid_memory = admit(
            &context.host,
            &context.policy,
            &Demand {
                phase: "total-power grid",
                memory: TotalPowerGrid::bytes(self.shape, &table, self.shape.channels),
            },
        )
        .map_err(failure)?;
        let grid = TotalPowerGrid::new(self.shape, table, FlagMerge::AnyCorrelation, grid_memory)
            .map_err(failure)?;
        // The grid's reservation includes its kernel; no allocation escapes admission.
        forming.retain(0);
        Ok((grid, forming))
    }
}
