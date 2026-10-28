// SPDX-License-Identifier: LGPL-3.0-or-later
//! Imaging port step 4's source counting law (#932): the observation-batch
//! route's source holds at most what its admission charges it. A whole
//! run's counting law (`tests/admission_memory.rs`) cannot see the source's
//! charge beside the rest of the run's, so this reads the source alone,
//! through the traversals a run makes, on a route opened and admitted as a
//! run admits it. A request that writes visibilities back also drives its
//! write (step 5): the source and the sink of a writing pass hold at most
//! the route's charge and the write's. Evidence for the port only: the
//! cutover makes the batch route the run's, and its law moves with it.

use casa_imaging_model::{CompiledProblem, ReconstructionBasis};
use casa_imaging_operator::PlaneRange;
use casa_imaging_runtime::pass::{BoundedSource, NativeBlock, SampleContent, source_block_bytes};
use num_complex::Complex32;

use crate::imaging::{self, ObservationSource, VisibilityWriteTarget, VisibilityWriter};
use crate::{ApplicationDispatchError, ImagingRequest, RunContext, SourceRoute};

/// What [`drive_batch_source`] admitted and read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceDrive {
    /// Bytes admitted for the opened observation and the source: the
    /// route's charge without the passes' blocks.
    pub charged: u64,
    /// Bytes of the one block the drive fills
    /// ([`NativeBlock::capacity_bytes`]), which the passes' share holds.
    pub block: u64,
    /// Blocks filled, over every traversal.
    pub blocks: usize,
}

/// Compile `request` on the observation-batch route, admit the route as a
/// run does, read its source once, then call `mark` and read it again with
/// a fresh source: flags and weights over every plane, visibilities over
/// every plane and, for a cube, visibilities in restricted waves of
/// `planes_per_wave` planes. The first reading leaves the opened store's
/// own state (its storage managers' column caches, which the run's law
/// bounds by the MeasurementSet's size) and the process caches as a run's
/// later passes find them, so what a caller counts from `mark` on is the
/// source's: its batches, survey, pointing, geometry and packing, and the
/// block it fills. The admission is held until this returns. Kernel sets do
/// not ramp to the pointing here.
///
/// # Errors
///
/// When the request does not compile, the route is refused or not
/// admitted, or a traversal fails.
pub fn drive_batch_source(
    request: &ImagingRequest,
    context: &RunContext,
    planes_per_wave: u32,
    mark: &mut dyn FnMut(),
) -> Result<SourceDrive, ApplicationDispatchError> {
    let mut request = request.clone();
    request.source_route = SourceRoute::ObservationBatches;
    let resolved = std::cell::Cell::new(None);
    let (problem, input, native, _resident) = crate::compile_run(&request, context, &resolved)?;
    drop(input.initial_access);
    let admitted = crate::native::batch_route(
        &problem,
        &input.observation,
        (false, false),
        &native.runtime,
    )?;
    let planes = planes(&problem);
    let source = || {
        ObservationSource::new(
            &problem,
            &admitted.value,
            planes,
            imaging::plane_bounds(&problem),
            false,
        )
    };
    read(source(), planes, planes_per_wave)?;
    mark();
    let (blocks, block) = read(source(), planes, planes_per_wave)?;
    // The passes' share of the charge: what they hold for blocks of the
    // source's largest.
    let passes = source_block_bytes(source().maximum_block());
    Ok(SourceDrive {
        charged: admitted.reservation.memory().saturating_sub(passes),
        block,
        blocks,
    })
}

/// Compile `request`, which writes visibilities back, on the
/// observation-batch route, admit the route as a run does, read its source
/// once, then call `mark` and drive a writing pass: a sink begun for the
/// next traversal, every block of a fresh source's traversal of every
/// plane handed to it with a zero prediction for each sample, and the
/// write completed. What a caller counts from `mark` on is the source's and
/// the write's: the updaters, the patches and the provenance; `charged`
/// adds the write's charge (the batch route's `write_bytes`) to the
/// route's. The predictions handed to the sink are allocated before
/// `mark`: the passes hold them.
///
/// # Errors
///
/// As [`drive_batch_source`], and when the write fails.
pub fn drive_batch_write(
    request: &ImagingRequest,
    context: &RunContext,
    mark: &mut dyn FnMut(),
) -> Result<SourceDrive, ApplicationDispatchError> {
    let mut request = request.clone();
    request.source_route = SourceRoute::ObservationBatches;
    let resolved = std::cell::Cell::new(None);
    let (problem, input, native, _resident) = crate::compile_run(&request, context, &resolved)?;
    drop(input.initial_access);
    let admitted = crate::native::batch_route(
        &problem,
        &input.observation,
        (input.write_model_column, input.write_corrected_data),
        &native.runtime,
    )?;
    let failed = |error: casa_imaging_runtime::pass::SourceError| {
        ApplicationDispatchError::Native(error.to_string().into())
    };
    let planes = planes(&problem);
    let source = || {
        ObservationSource::new(
            &problem,
            &admitted.value,
            planes,
            imaging::plane_bounds(&problem),
            false,
        )
    };
    read(source(), planes, planes)?;
    let target = VisibilityWriteTarget::Batches(&admitted.value);
    let write = target.retained_bytes().map_err(failed)?;
    let mut source = source();
    let maximum = source.maximum_block();
    let mut block = NativeBlock::with_capacity(maximum);
    let predictions =
        vec![Complex32::default(); maximum.rows * maximum.channels() * maximum.correlations];
    mark();
    let mut writer =
        VisibilityWriter::begin(&target, problem.visibility_transform()).map_err(failed)?;
    source
        .begin(
            PlaneRange::new(0, planes),
            false,
            SampleContent::Visibilities,
        )
        .map_err(failed)?;
    let mut blocks = 0;
    while source.fill(&mut block).map_err(failed)? {
        let cells = block.len() * block.channels() * block.correlations();
        writer
            .write(&block, &predictions[..cells])
            .map_err(failed)?;
        blocks += 1;
    }
    writer.complete().map_err(failed)?;
    let passes = source_block_bytes(maximum);
    Ok(SourceDrive {
        charged: admitted.reservation.memory().saturating_sub(passes) + write,
        block: block.capacity_bytes(),
        blocks,
    })
}

/// Read `source` through a run's traversals of `planes` planes, waves of
/// `planes_per_wave` included, into one block: the blocks filled, and the
/// block's bytes.
fn read(
    mut source: ObservationSource<'_>,
    planes: u32,
    planes_per_wave: u32,
) -> Result<(usize, u64), ApplicationDispatchError> {
    let mut block = NativeBlock::with_capacity(source.maximum_block());
    let mut blocks = 0;
    let mut traverse = |range: PlaneRange, restrict: bool, content: SampleContent| {
        source.begin(range, restrict, content)?;
        while source.fill(&mut block)? {
            blocks += 1;
        }
        Ok::<_, casa_imaging_runtime::pass::SourceError>(())
    };
    let all = PlaneRange::new(0, planes);
    let step = planes_per_wave.clamp(1, planes);
    traverse(all, false, SampleContent::FlagsAndWeights)
        .and_then(|()| traverse(all, false, SampleContent::Visibilities))
        .and_then(|()| {
            (0..planes)
                .step_by(step as usize)
                .filter(|_| step < planes)
                .try_for_each(|start| {
                    traverse(
                        PlaneRange::new(start, (start + step).min(planes)),
                        true,
                        SampleContent::Visibilities,
                    )
                })
        })
        .map_err(|error| ApplicationDispatchError::Native(error.to_string().into()))?;
    Ok((blocks, block.capacity_bytes()))
}

/// The planes of `problem`'s basis a wave divides: a cube's channels, or
/// one.
fn planes(problem: &CompiledProblem) -> u32 {
    match problem.reconstruction().basis() {
        ReconstructionBasis::ChannelLocal { channels } => {
            u32::try_from(channels).expect("a cube's channels fit a plane index")
        }
        _ => 1,
    }
}
