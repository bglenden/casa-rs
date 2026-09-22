// SPDX-License-Identifier: LGPL-3.0-or-later

//! Test-only direct output collection for the bounded product-generation API.
//!
//! The production API deliberately exposes no product readback. These
//! fixtures collect the owned windows into ordinary test memory so scientific
//! assertions can inspect the exact generated values without adding a backing
//! or digest capability to the library.

// The continuum and Taylor integration binaries exercise different fixture members.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use casa_imaging_model::ProductNodeId;
use casa_imaging_products::{
    PlannedContinuumGeneration, ProductMemberContract, ProductOutput, ProductStoragePlan,
    ProductWindow, ProductWindowLayout, ProductWriter, ProductsError, PublishedContinuumGeneration,
    RestoringBeam,
};

/// Sum the actual owned summary fields independently of the production planner.
pub fn retained_metadata_bytes(generation: &PublishedContinuumGeneration) -> u64 {
    use casa_imaging_model::{ImageDomainRole, SpectralWcs};
    use std::mem::size_of_val;
    let mut bytes = size_of_val(generation)
        + size_of_val(generation.members())
        + size_of_val(generation.fitted_beams())
        + size_of_val(generation.restoring_beams());
    for member in generation.members() {
        let contract = member.contract();
        bytes += member.name().len()
            + size_of_val(contract.dependencies())
            + size_of_val(contract.axes().polarization())
            + size_of_val(member.resolved_beams());
        if let ImageDomainRole::Outlier(name) = contract.axes().domain() {
            bytes += name.len();
        }
        if let SpectralWcs::Tabular {
            channel_centres_hz,
            channel_boundaries_hz,
        } = contract.axes().spectral().wcs()
        {
            bytes += size_of_val(channel_centres_hz.as_slice())
                + size_of_val(channel_boundaries_hz.as_slice());
        }
    }
    bytes as u64
}

pub fn full_window(planned: &PlannedContinuumGeneration) -> ProductStoragePlan {
    ProductStoragePlan::new(
        planned
            .members()
            .iter()
            .map(|member| member.axes().spectral().output_channels())
            .max()
            .unwrap(),
        1,
    )
    .unwrap()
}

#[derive(Debug, Clone)]
struct CapturedMember {
    layout: ProductWindowLayout,
    payload: Vec<f32>,
    validity: Vec<bool>,
    writes: usize,
    finished: bool,
}

/// In-memory output used only by product algorithm tests.
#[derive(Debug, Clone, Default)]
pub struct MemoryProductOutput {
    members: Arc<Mutex<Vec<(ProductNodeId, CapturedMember)>>>,
    fail_write: bool,
    fail_finish: bool,
}

impl MemoryProductOutput {
    /// Construct a collector that returns an output error on the first write.
    #[must_use]
    pub fn failing_write() -> Self {
        Self {
            fail_write: true,
            ..Self::default()
        }
    }

    /// Construct a collector that returns an output error when a member is finished.
    #[must_use]
    pub fn failing_finish() -> Self {
        Self {
            fail_finish: true,
            ..Self::default()
        }
    }

    /// Return the exact number of begun members.
    #[must_use]
    pub fn begun_members(&self) -> usize {
        self.members.lock().unwrap().len()
    }

    /// Return the exact number of windows transferred to one member.
    #[must_use]
    pub fn write_count(&self, node: ProductNodeId) -> usize {
        self.members
            .lock()
            .unwrap()
            .iter()
            .find_map(|(member, captured)| (*member == node).then_some(captured.writes))
            .unwrap_or(0)
    }

    /// Return whether the output owner received a successful finish for one member.
    #[must_use]
    pub fn finished(&self, node: ProductNodeId) -> bool {
        self.members
            .lock()
            .unwrap()
            .iter()
            .find_map(|(member, captured)| (*member == node).then_some(captured.finished))
            .unwrap_or(false)
    }

    fn capture(&self, node: ProductNodeId) -> CapturedMember {
        self.members
            .lock()
            .unwrap()
            .iter()
            .find_map(|(member, captured)| (*member == node).then_some(captured.clone()))
            .unwrap_or_else(|| panic!("generated member {node:?} was not written"))
    }
}

impl ProductOutput for MemoryProductOutput {
    fn begin_member<'a>(
        &'a self,
        member: &casa_imaging_products::PlannedMember,
        layout: ProductWindowLayout,
        _: &[Option<RestoringBeam>],
    ) -> Result<Box<dyn ProductWriter + 'a>, ProductsError> {
        let values = layout.values()?;
        self.members.lock().unwrap().push((
            member.node(),
            CapturedMember {
                layout,
                payload: vec![0.0; values],
                validity: vec![false; values],
                writes: 0,
                finished: false,
            },
        ));
        Ok(Box::new(MemoryProductWriter {
            output: self.members.clone(),
            node: member.node(),
            fail_write: self.fail_write,
            fail_finish: self.fail_finish,
        }))
    }
}

#[derive(Debug)]
struct MemoryProductWriter {
    output: Arc<Mutex<Vec<(ProductNodeId, CapturedMember)>>>,
    node: ProductNodeId,
    fail_write: bool,
    fail_finish: bool,
}

impl MemoryProductWriter {
    fn indices(
        layout: ProductWindowLayout,
        start: [usize; 4],
        shape: [usize; 4],
    ) -> Result<Vec<usize>, ProductsError> {
        let values = shape
            .into_iter()
            .try_fold(1usize, |count, extent| count.checked_mul(extent))
            .ok_or(ProductsError::InvalidWindow)?;
        if values > layout.maximum_values()
            || (0..4).any(|axis| {
                start[axis]
                    .checked_add(shape[axis])
                    .is_none_or(|end| end > layout.shape()[axis])
            })
        {
            return Err(ProductsError::InvalidWindow);
        }
        Ok((0..values)
            .map(|local| {
                let mut remainder = local;
                let mut coordinates = [0; 4];
                for axis in (0..4).rev() {
                    coordinates[axis] = remainder % shape[axis] + start[axis];
                    remainder /= shape[axis];
                }
                coordinates
                    .into_iter()
                    .zip(layout.shape())
                    .fold(0, |index, (coordinate, extent)| index * extent + coordinate)
            })
            .collect())
    }
}

impl ProductWriter for MemoryProductWriter {
    fn write(&mut self, window: ProductWindow) -> Result<(), ProductsError> {
        if self.fail_write {
            return Err(ProductsError::Storage("test write failure".to_string()));
        }
        let (start, shape, payload, validity) = window.into_parts();
        let mut members = self.output.lock().unwrap();
        let (_, captured) = members
            .iter_mut()
            .find(|(node, _)| *node == self.node)
            .ok_or_else(|| ProductsError::Storage("test member disappeared".to_string()))?;
        for (local, index) in Self::indices(captured.layout, start, shape)?
            .into_iter()
            .enumerate()
        {
            captured.payload[index] = payload[local];
            captured.validity[index] = validity[local];
        }
        captured.writes += 1;
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<(), ProductsError> {
        if self.fail_finish {
            return Err(ProductsError::Storage("test finish failure".to_string()));
        }
        let mut members = self.output.lock().unwrap();
        let (_, captured) = members
            .iter_mut()
            .find(|(node, _)| *node == self.node)
            .ok_or_else(|| ProductsError::Storage("test member disappeared".to_string()))?;
        captured.finished = true;
        Ok(())
    }
}

/// A test-owned copy of one generated member's metadata and collected arrays.
#[derive(Debug, Clone)]
pub struct GeneratedMember {
    node: ProductNodeId,
    name: String,
    contract: ProductMemberContract,
    resolved_beams: Box<[Option<RestoringBeam>]>,
    payload: Vec<f32>,
    validity: Vec<bool>,
}

impl GeneratedMember {
    #[must_use]
    pub const fn node(&self) -> ProductNodeId {
        self.node
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn contract(&self) -> &ProductMemberContract {
        &self.contract
    }

    #[must_use]
    pub const fn resolved_beams(&self) -> &[Option<RestoringBeam>] {
        &self.resolved_beams
    }

    #[must_use]
    pub fn resolved_beam(&self) -> Option<&RestoringBeam> {
        match self.resolved_beams.as_ref() {
            [Some(beam)] => Some(beam),
            _ => None,
        }
    }

    #[must_use]
    pub fn payload(&self) -> &[f32] {
        &self.payload
    }

    #[must_use]
    pub fn validity(&self) -> &[bool] {
        &self.validity
    }
}

/// Test-owned generation result assembled from the direct output collector.
#[derive(Debug, Clone)]
pub struct GeneratedProducts {
    problem_id: casa_imaging_model::CompiledProblemId,
    graph_id: casa_imaging_model::ProductGraphId,
    fitted_beams: Box<[Option<RestoringBeam>]>,
    restoring_beams: Box<[Option<RestoringBeam>]>,
    members: Box<[GeneratedMember]>,
}

impl GeneratedProducts {
    /// Copy output arrays from a completed direct generation.
    #[must_use]
    pub fn from_output(
        generation: &PublishedContinuumGeneration,
        output: &MemoryProductOutput,
    ) -> Self {
        let members = generation
            .members()
            .iter()
            .map(|member| {
                let captured = output.capture(member.node());
                GeneratedMember {
                    node: member.node(),
                    name: member.name().to_string(),
                    contract: member.contract().clone(),
                    resolved_beams: member.resolved_beams().to_vec().into_boxed_slice(),
                    payload: captured.payload,
                    validity: captured.validity,
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            problem_id: generation.problem_id(),
            graph_id: generation.graph_id(),
            fitted_beams: generation.fitted_beams().to_vec().into_boxed_slice(),
            restoring_beams: generation.restoring_beams().to_vec().into_boxed_slice(),
            members,
        }
    }

    #[must_use]
    pub const fn problem_id(&self) -> casa_imaging_model::CompiledProblemId {
        self.problem_id
    }

    #[must_use]
    pub const fn graph_id(&self) -> casa_imaging_model::ProductGraphId {
        self.graph_id
    }

    #[must_use]
    pub const fn fitted_beams(&self) -> &[Option<RestoringBeam>] {
        &self.fitted_beams
    }

    #[must_use]
    pub const fn restoring_beams(&self) -> &[Option<RestoringBeam>] {
        &self.restoring_beams
    }

    #[must_use]
    pub fn restoring_beam(&self) -> Option<&RestoringBeam> {
        match self.restoring_beams.as_ref() {
            [Some(beam)] => Some(beam),
            _ => None,
        }
    }

    #[must_use]
    pub const fn members(&self) -> &[GeneratedMember] {
        &self.members
    }
}
