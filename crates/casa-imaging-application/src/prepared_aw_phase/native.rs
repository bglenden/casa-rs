// SPDX-License-Identifier: LGPL-3.0-or-later

//! Native origin for the shared plan/run-owned prepared AW phase.

use casa_imaging_model::{EvlaAwCellRequest, NativeAwRequest};
use casa_imaging_reconstruction::{
    AwPreparedCatalog, AwPreparedCellMetadata, EvlaApertureModel, EvlaAwWorkspace,
};
use casa_imaging_runtime::{
    PreparedArtifactError, PreparedArtifactGenerator, PreparedArtifactNativeCatalogOutcome,
    PreparedArtifactNativeEntryOutcome, PreparedArtifactNativeGenerator,
    PreparedArtifactNativeLayout, PreparedArtifactNativeOperation,
    PreparedArtifactNativePlanFragment, PreparedArtifactNativePlaneLayout,
    PreparedArtifactNativeRequest, PreparedArtifactSegmentDescriptor,
};

use super::*;

pub(super) struct NativeGenerator {
    model: EvlaApertureModel,
    workspace: Option<EvlaAwWorkspace>,
}

impl NativeGenerator {
    pub(super) fn new(request: &NativeAwRequest) -> Self {
        Self {
            model: EvlaApertureModel::new(request.input().surface.clone()),
            workspace: None,
        }
    }
}

impl PreparedArtifactNativeGenerator for NativeGenerator {
    fn generate_cell(
        &mut self,
        _: usize,
        cell: EvlaAwCellRequest,
        workspace_limit: u64,
    ) -> Result<PreparedArtifactNativeLayout, PreparedArtifactError> {
        if self.workspace.is_none() {
            self.workspace = Some(
                EvlaAwWorkspace::new(
                    cell,
                    usize::try_from(workspace_limit)
                        .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?,
                )
                .map_err(|_| PreparedArtifactError::InvalidLayout)?,
            );
        }
        let pair = self
            .workspace
            .as_mut()
            .expect("workspace acquired above")
            .generate(&self.model, cell)
            .map_err(|error| PreparedArtifactError::Io(io::Error::other(error)))?;
        let plane = |size, support| PreparedArtifactNativePlaneLayout {
            shape: [size as u64; 2],
            support: [support as u64; 2],
        };
        Ok(PreparedArtifactNativeLayout {
            imaging: plane(pair.imaging.size, pair.imaging.support),
            weight: plane(pair.weight.size, pair.weight.support),
        })
    }

    fn workspace_peak_bytes(&self) -> u64 {
        self.workspace
            .as_ref()
            .map_or(0, |workspace| workspace.resident_bytes() as u64)
    }
}

impl PreparedArtifactGenerator for NativeGenerator {
    fn fill_segment(
        &mut self,
        segment: &PreparedArtifactSegmentDescriptor,
        byte_offset: u64,
        output: &mut [u8],
    ) -> Result<(), PreparedArtifactError> {
        let pair = self
            .workspace
            .as_ref()
            .and_then(EvlaAwWorkspace::pair)
            .ok_or(PreparedArtifactError::IncompleteArtifact)?;
        let plane = match segment.name() {
            "imaging" => pair.imaging,
            "weight" => pair.weight,
            _ => return Err(PreparedArtifactError::SegmentMismatch),
        };
        if segment.shape() != [plane.size as u64; 2]
            || segment.support() != [plane.support as u64; 2]
        {
            return Err(PreparedArtifactError::SegmentMismatch);
        }
        let offset =
            usize::try_from(byte_offset).map_err(|_| PreparedArtifactError::ArtifactTooLarge)?;
        if offset
            .checked_add(output.len())
            .is_none_or(|end| end > plane.values.len() * 8)
        {
            return Err(PreparedArtifactError::SegmentMismatch);
        }
        for (index, byte) in output.iter_mut().enumerate() {
            let position = offset + index;
            let element = position / 8;
            // One canonical private codec: X-major, last axis contiguous.
            let value = plane.values[element / plane.size + plane.size * (element % plane.size)];
            let component = if position % 8 < 4 { value.re } else { value.im };
            *byte = component.to_le_bytes()[position % 4];
        }
        Ok(())
    }
}

pub(super) fn prepare(
    problem: &CompiledProblem,
    deployment: ApplicationAwPreparation,
    runtime: &ApplicationRuntime,
) -> Result<PreparedAwPhase, ApplicationError> {
    let crate::ApplicationAwSource::NativeEvla {
        input,
        policy,
        cache_bytes,
    } = deployment.source
    else {
        return Err(boxed("native AW phase requires a native source"));
    };
    let request = NativeAwRequest::new(problem.geometry().geometry_id(), *input)?;
    let entries = request.cell_count();
    std::fs::create_dir_all(&deployment.private_root)?;
    let store = Arc::new(PreparedArtifactStore::open(
        &deployment.private_root,
        &deployment.storage_domain,
        casa_imaging_runtime::PreparedArtifactBudget::new(cache_bytes, entries, 8 << 20)?,
    )?);
    let owner =
        crate::PlanningRegistry::new(runtime.registry, runtime.implementation.clone(), problem);
    let native = PreparedArtifactNativeRequest::new(
        &store,
        &owner,
        &runtime.implementation,
        problem,
        request.clone(),
    )?;
    eprintln!(
        "imaging_native_aw_plan source=native_evla requested_action={policy:?} cells={entries}"
    );
    eprintln!(
        "imaging_native_aw_request_summary cells={} frequencies={} w_values={} parallactic_angles={} working_size={} oversampling={} cache_bytes={} generation_workspace_bound_bytes={} scientific_input_bytes={}",
        entries,
        request.input().frequencies.len(),
        request.input().w_values.len(),
        request.input().pa_values.len(),
        request.input().grid.size,
        request.input().grid.oversampling,
        cache_bytes,
        request.generation_workspace_bytes()?,
        request.resident_bytes()
    );
    let (mut outcome, receipt) = run_native(
        problem,
        runtime,
        Arc::clone(&store),
        native.clone(),
        PreparedArtifactNativeOperation::Reuse,
        2,
    )?;
    let mut receipts = vec![receipt];
    let complete = outcome
        .entries()
        .iter()
        .all(|entry| matches!(entry, PreparedArtifactNativeEntryOutcome::Ready { .. }));
    let operation = match policy {
        crate::NativeAwCachePolicy::ReuseOnly => None,
        crate::NativeAwCachePolicy::GenerateMissing => {
            (!complete).then_some(PreparedArtifactNativeOperation::Generate)
        }
        crate::NativeAwCachePolicy::Regenerate => Some(PreparedArtifactNativeOperation::Regenerate),
    };
    if let Some(operation) = operation {
        let (generated, receipt) =
            run_native(problem, runtime, Arc::clone(&store), native, operation, 3)?;
        outcome = generated;
        receipts.push(receipt);
    }
    eprintln!(
        "imaging_native_aw_generation_summary charged_workspace_peak_bytes={} expected_cells={}",
        outcome.generation_workspace_peak_bytes(),
        entries
    );
    let artifacts = outcome.into_complete()?;
    if artifacts.len() != entries {
        return Err(boxed("native AW outcome omitted requested cells"));
    }
    let prepared = artifacts
        .iter()
        .enumerate()
        .map(|(index, (descriptor, _))| {
            let (cell, identity) = request
                .cell(index)
                .ok_or_else(|| boxed("native AW result is outside its requested catalog"))?;
            let layout = |segment| crate::aw_cache::stored_kernel_layout(segment);
            let metadata = AwPreparedCellMetadata::new(
                identity,
                cell.frequency_hz,
                cell.w_wavelengths,
                request.input().w_increment,
                cell.mueller as u32,
                cell.parallactic_angle_rad.to_degrees(),
                layout(
                    descriptor
                        .imaging_plane()
                        .ok_or(PreparedArtifactError::SegmentMismatch)?,
                )?,
                layout(
                    descriptor
                        .weight_plane()
                        .ok_or(PreparedArtifactError::SegmentMismatch)?,
                )?,
            )?;
            PreparedAwCell::new(metadata, descriptor.clone())
                .map_err(|error| Box::new(error) as ApplicationError)
        })
        .collect::<Result<Vec<_>, ApplicationError>>()?;
    let largest = prepared
        .iter()
        .map(PreparedAwCell::decoded_resident_bytes)
        .collect::<Option<Vec<_>>>()
        .and_then(|bytes| bytes.into_iter().max())
        .ok_or_else(|| boxed("native AW decoded residency overflowed"))?;
    if largest > deployment.resident_bytes {
        return Err(Box::new(io::Error::other(format!(
            "native AW cell requires {largest} decoded bytes, exceeding {}",
            deployment.resident_bytes
        ))));
    }
    let decoder_bytes = prepared
        .iter()
        .map(PreparedAwCell::decoder_workspace_bytes)
        .collect::<Option<Vec<_>>>()
        .and_then(|bytes| bytes.into_iter().max())
        .ok_or_else(|| boxed("native AW decoder workspace overflowed"))?;
    let catalog = AwPreparedCatalog::new(
        prepared
            .iter()
            .map(|cell| cell.metadata().clone())
            .collect(),
    )?;
    let reader = PreparedArtifactReaderFactory::new(
        store,
        artifacts,
        runtime.implementation.clone(),
        deployment.resident_bytes as u64,
        decoder_bytes as u64,
    )?;
    Ok(PreparedAwPhase {
        catalog,
        prepared,
        reader,
        conjugate_beams: deployment.conjugate_beams,
        resident_bytes: deployment.resident_bytes,
        receipts,
    })
}

fn run_native(
    problem: &CompiledProblem,
    runtime: &ApplicationRuntime,
    store: Arc<PreparedArtifactStore>,
    request: PreparedArtifactNativeRequest,
    operation: PreparedArtifactNativeOperation,
    phase: u64,
) -> Result<(PreparedArtifactNativeCatalogOutcome, ExecutionReceipt), ApplicationError> {
    let producer = casa_imaging_runtime::WorkNodeId::new("prepared-phase-producer");
    let commit = casa_imaging_runtime::WorkNodeId::new("prepared-phase-commit");
    let id = PreparedArtifactNativePlanFragment::new(
        &request,
        &store,
        operation,
        producer.clone(),
        commit.clone(),
        runtime.implementation.clone(),
    )?
    .work_implementation_id();
    let registry = PhaseRegistry {
        id: runtime.registry,
        metadata: ImplementationContractMetadata::new(
            problem.problem_id(),
            problem.numerics_id(),
            problem.required_capabilities().clone(),
        ),
        implementations: BTreeMap::from([
            (
                id.clone(),
                PhaseImplementation::Catalog(Box::new(CatalogAdapter {
                    id,
                    store: Arc::clone(&store),
                    input: CatalogOperation::Native {
                        request: Box::new(request),
                        operation,
                    },
                    result: Mutex::new(None),
                })),
            ),
            (
                runtime.implementation.clone(),
                PhaseImplementation::Base {
                    id: runtime.implementation.clone(),
                    sources: vec![],
                },
            ),
        ]),
        prepared_artifact: crate::prepared_aw_registration(runtime.implementation.clone()),
    };
    let CatalogOperation::Native { request, operation } = &registry.catalog().input else {
        unreachable!("native operation constructed above")
    };
    let base = PreparedArtifactNativePlanFragment::standalone_base(
        &registry,
        runtime.implementation.clone(),
        request,
        &store,
        runtime.stage_nanos,
        runtime.confidence_parts_per_million,
    )?;
    let physical = PreparedArtifactNativePlanFragment::new(
        request,
        &store,
        *operation,
        producer,
        commit,
        runtime.implementation.clone(),
    )?
    .compose(&base)?;
    let reuse = *operation == PreparedArtifactNativeOperation::Reuse;
    let (outcome, receipt) = run_phase(problem, runtime, registry, physical, reuse, phase)?;
    let CatalogPhaseResult::Native(outcome) = outcome else {
        return Err(boxed("native AW phase returned a CASA import outcome"));
    };
    Ok((outcome, receipt))
}
