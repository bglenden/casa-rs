// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    collections::HashMap,
    error::Error,
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use casa_imaging_model::{
    AxisOrder, CentreLaws, CorrelationProduct, CorrelationSelection, CorrelationType,
    DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, Epoch, FacetLayout, FiniteValuePolicy, FrequencyFrame, GeometryInput,
    ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest, InstrumentResponse,
    ItrfPosition, LogicalIdentity, MeasurementEquationContract, ModelBounds, ModelCell,
    ModelColumnWrite, ModelDeltaTerm, ModelExecutionAttemptId, ModelInnerProduct,
    ModelInputCommitment, ModelLifecycleRequirements, ModelStateIdentity, NumericPrecision,
    NumericalStage, NumericsContract, ObservationSelection, ObservationTransactionRequirements,
    PhaseCentreLaw, PointingCentreLaw, PolarizationContract, PolarizationCoordinate,
    PrimaryBeamValidityPolicy, ProblemInputIdentities, ProblemSpecification, ProductBlankingPolicy,
    ProductKind, ProductNormalization, ProductRequirements, ProductSupportComparison,
    ProductValidityPolicies, Projection, ReconstructionAlgorithm, ReconstructionBasis,
    ReconstructionContract, ReconstructionControls, ReductionPolicy, RestFrequency,
    RestoringBeamPolicy, RowSelection, ScientificContract, SelectedMainRow, SelectedRowsBuilder,
    SelectedVisibilitySample, SelectionBound, SkyDirection, SpectralContract,
    SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor, SpectralSamplingLaw,
    SpectralWcs, SpectralWindowSelection, StageErrorBudget, TaylorSupportReference,
    TaylorValidityPolicy, TimeRange, TimeScale, TimeSelection, UvwCoordinateLaw, VisibilityColumn,
    VisibilityInnerProduct, WeightColumn, WeightDensityScope, WeightingContract, WeightingScheme,
    compile, compile_observation,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, FrozenWeightingCoverageProof, MajorCycleOwner, MajorCyclePreparation,
    ModelLifecycle, SpectralOperatorSpecification, WeightingExecutionLimits,
    begin_weighting_generation, plan_weighting,
    runtime_adapter::{
        GRIDDED_NORMAL_OPERATOR_RECORD_BYTES, SpectralOperatorPass, prepare_spectral_operator,
        spectral_operator_workload,
    },
};
use casa_ms::{
    BoundSelectedObservation, MeasurementSet, MsSelectionIoBudget, SelectedObservationBlock,
    SelectedObservationContentBudget, SelectedObservationReplayProof,
    SelectedObservationResolutionRequest, SelectedObservationRow, VisibilityDataColumn,
    resolve_selected_observation,
};
use casa_types::measures::{epoch::EpochRef, frequency::FrequencyRef};
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use super::*;
use crate::{
    GriddedNormalReplayStorage, ProductionStorageProfile, ResourceAuthority,
    complete_data_operator::{
        GriddedNormalCompilationMeasurements, GriddedNormalReplayCompilation,
        project_gridded_normal_artifact_budget,
    },
    gridded_normal_artifact::{GriddedNormalArtifactMeasurements, GriddedNormalArtifactSeal},
};

const DATA_ROOT_ENV: &str = "CASA_RS_IMPERF_DATA_ROOT";
const REPLAY_ARTIFACT_ENV: &str = "CASA_RS_IMPERF_REPLAY_ARTIFACT";
const DATASET_RELATIVE_PATH: &str = "wave1/vla/single/medium/ms/wave1-vla-single-medium.ms";
const EXPECTED_FULL_SELECTED_ROWS: u64 = 4_094_064;
const WINDOW_ROWS: u64 = 65_536;
const WINDOW_STARTS: [u64; 4] = [0, 1_342_843, 2_685_685, 4_028_528];
const EXPECTED_SELECTED_ROWS: u64 = 263_250;
const EXPECTED_SELECTED_SAMPLES: u64 = 33_696_000;
const FULL_WORKLOAD_SELECTED_SAMPLES: u64 = 524_040_192;
const CAPTURED_RESIDENCY_BYTES: usize = 1 << 30;
const CAPTURED_BLOCK_LIMIT: usize = 16;
const EXPECTED_CAPTURED_BLOCKS: usize = 6;
const EXPECTED_CAPTURED_LOGICAL_BYTES: u64 = 330_905_250;
const EXPECTED_CAPTURED_READ_OPERATIONS: u64 = 114;
const EXPECTED_CAPTURED_CURRENT_BYTES: u64 = 375_131_250;
const EXPECTED_CAPTURED_CAPACITY_BYTES: u64 = 378_169_650;
const EXPECTED_WEIGHTED_BLOCKS: u64 = 8_227;
const WEIGHTED_BLOCK_SAMPLES: usize = 4_096;
const EXPECTED_NORMAL_STATE_IDENTITY: &str =
    "e6368112404a3ce2b3b3b9e988bde85dadd5726e09de8d87ca4499dc27a71b91";
const EXPECTED_INITIAL_WEIGHTED_NORMAL_STATE_IDENTITY: &str =
    "29697a529f90bfa832a45461469fd7a20ddbb0688ec4f4cb52ec5ce816807f8a";
const EXPECTED_INITIAL_WEIGHTED_ARTIFACT_IDENTITY: &str =
    "e622ef9bd43c09136f8bd58953beaec608326232001a29c111bc405f71647404";
const EXPECTED_INITIAL_WEIGHTED_ARTIFACT_SHA256: &str =
    "8ba96df08553820c4441f3a87fd84d90f324b21d14c8d8c7985e6164934ce154";
const EXPECTED_INITIAL_WEIGHTING_GENERATION: &str =
    "7c777736897881dc952ad18ec490d23f70351f8b78419ba0e960cb59c22e8808";
const EXPECTED_INITIAL_WEIGHTING_REPLAY: &str =
    "3fa31ee1ebe5c4fbf9c8a42a445dd14901efb8ff1cb9280e600f7c5e9085e1e4";
const EXPECTED_INITIAL_WEIGHTING_COVERAGE: &str =
    "68125bafbe2e1a53cd3dfac4b5198997687f61fefefc1264604d26546537bacb";
const EXPECTED_INITIAL_WEIGHTING_RESIDENCY_BYTES: usize = 60_031_360;
const EXPECTED_INITIAL_ARTIFACT_MAXIMUM_BYTES: u64 = 1_078_864_440;
const EXPECTED_INITIAL_ARTIFACT_IO_BUFFER_BYTES: u64 = 131_144;
const EXPECTED_INITIAL_COVERAGE_PROOF_BYTES: u64 = 2_864_160_146;
const EXPECTED_INITIAL_COVERAGE_PROOF_HASH_CALLS: u64 = 33_696_007;
const BASELINE_REPEATABILITY_LIMIT: f64 = 0.03;
const OBSERVER_OVERHEAD_LIMIT: f64 = 0.02;
const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;
const NORMAL_REPLAY_SUPPORT: isize = 3;
const NORMAL_REPLAY_OVERSAMPLING: isize = 100;
const NORMAL_REPLAY_RECORD_BYTES: u64 = 16;
const HLL_PRECISION: u32 = 18;

#[derive(Debug, thiserror::Error)]
enum ReplayProbeError {
    #[error("normal-replay artifact I/O failed")]
    Artifact(#[from] std::io::Error),
    #[error("normal-replay operator failed")]
    Operator(#[from] casa_imaging_reconstruction::SpectralOperatorError),
}

struct ReplayArtifactSink {
    path: PathBuf,
    writer: BufWriter<File>,
    records: u64,
    bytes: u64,
    write_elapsed: Duration,
}

struct ReplayArtifactObservation {
    path: PathBuf,
    records: u64,
    bytes: u64,
    write_elapsed: Duration,
    read_elapsed: Duration,
    read_bytes: u64,
    sha256: String,
}

impl ReplayArtifactSink {
    fn from_environment() -> std::io::Result<Option<Self>> {
        let Some(path) = std::env::var_os(REPLAY_ARTIFACT_ENV).map(PathBuf::from) else {
            return Ok(None);
        };
        Ok(Some(Self {
            writer: BufWriter::with_capacity(1 << 20, File::create(&path)?),
            path,
            records: 0,
            bytes: 0,
            write_elapsed: Duration::ZERO,
        }))
    }

    fn write_block(&mut self, records: &[(u64, f64)]) -> std::io::Result<()> {
        let mut bytes = Vec::with_capacity(records.len() * NORMAL_REPLAY_RECORD_BYTES as usize);
        for (key, coefficient) in records {
            bytes.extend_from_slice(&key.to_le_bytes());
            bytes.extend_from_slice(&coefficient.to_le_bytes());
        }
        let started = Instant::now();
        self.writer.write_all(&bytes)?;
        self.write_elapsed += started.elapsed();
        self.records += u64::try_from(records.len()).expect("artifact record count fits u64");
        self.bytes += u64::try_from(bytes.len()).expect("artifact byte count fits u64");
        Ok(())
    }

    fn finish(mut self) -> std::io::Result<ReplayArtifactObservation> {
        let started = Instant::now();
        self.writer.flush()?;
        self.write_elapsed += started.elapsed();
        drop(self.writer);

        let read_started = Instant::now();
        let mut reader = BufReader::with_capacity(8 << 20, File::open(&self.path)?);
        let mut buffer = vec![0_u8; 8 << 20];
        let mut hasher = Sha256::new();
        let mut read_bytes = 0_u64;
        loop {
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
            read_bytes += u64::try_from(count).expect("artifact read count fits u64");
        }
        Ok(ReplayArtifactObservation {
            path: self.path,
            records: self.records,
            bytes: self.bytes,
            write_elapsed: self.write_elapsed,
            read_elapsed: read_started.elapsed(),
            read_bytes,
            sha256: format!("{:x}", hasher.finalize()),
        })
    }
}

struct NormalReplayCardinalityProbe {
    grid_shape: [usize; 2],
    du_lambda: f64,
    dv_lambda: f64,
    registers: Box<[u8]>,
    raw_records: u64,
    consecutive_reduced_records: u64,
    last_key: Option<u64>,
    block_reduced_records: u64,
    contributing_blocks: u64,
    artifact: Option<ReplayArtifactSink>,
}

impl NormalReplayCardinalityProbe {
    fn new(problem: &CompiledProblem, grid_shape: [usize; 2]) -> std::io::Result<Self> {
        assert!(
            grid_shape.into_iter().all(|extent| extent <= 4_096),
            "diagnostic tap key reserves 12 bits per grid coordinate"
        );
        let increment = problem.geometry().domains()[0].direction().increment_rad();
        Ok(Self {
            grid_shape,
            du_lambda: 1.0 / (grid_shape[0] as f64 * increment[0].abs()),
            dv_lambda: 1.0 / (grid_shape[1] as f64 * increment[1].abs()),
            registers: vec![0; 1 << HLL_PRECISION].into_boxed_slice(),
            raw_records: 0,
            consecutive_reduced_records: 0,
            last_key: None,
            block_reduced_records: 0,
            contributing_blocks: 0,
            artifact: ReplayArtifactSink::from_environment()?,
        })
    }

    fn observe(&mut self, block: &ReconstructionWeightedBlock) -> std::io::Result<()> {
        let mut block_coefficients = HashMap::with_capacity(block.samples().len());
        for weighted in block.samples() {
            let selected = weighted.selected();
            let finite_visibility = match selected.visibility() {
                SelectedVisibilitySample::Float32(value) => value.is_finite(),
                SelectedVisibilitySample::Complex32(value) => value.into_iter().all(f32::is_finite),
            };
            if !selected
                .address()
                .correlation_type
                .contributes_to_stokes_i()
                || selected.row_flag()
                || selected.parallel_hand_group_flag()
                || !selected.address().frequency_centre_hz.is_finite()
                || !selected.phase_shift_m().is_finite()
                || !finite_visibility
                || selected
                    .transformed_uvw_m()
                    .iter()
                    .any(|coordinate| !coordinate.is_finite())
            {
                continue;
            }
            for spectral in weighted.spectral_values() {
                let contribution = spectral.contribution();
                if spectral.imaging_weight() == 0.0 || contribution.output_channel() != 0 {
                    continue;
                }
                let scale = contribution.evaluation_frequency_hz() / SPEED_OF_LIGHT_M_PER_S;
                let uv = selected.transformed_uvw_m();
                let Some(key) = self.key([uv[0] * scale, uv[1] * scale]) else {
                    continue;
                };
                self.raw_records = self
                    .raw_records
                    .checked_add(1)
                    .expect("normal replay raw-record count does not overflow");
                if self.last_key != Some(key) {
                    self.consecutive_reduced_records = self
                        .consecutive_reduced_records
                        .checked_add(1)
                        .expect("normal replay consecutive-record count does not overflow");
                    self.last_key = Some(key);
                }
                *block_coefficients.entry(key).or_insert(0.0) +=
                    spectral.imaging_weight() * contribution.factor() * contribution.factor();
                self.observe_key(key);
            }
        }
        if !block_coefficients.is_empty() {
            self.contributing_blocks += 1;
            self.block_reduced_records = self
                .block_reduced_records
                .checked_add(
                    u64::try_from(block_coefficients.len()).expect("block record count fits u64"),
                )
                .expect("normal replay block-record count does not overflow");
            if let Some(artifact) = &mut self.artifact {
                let mut records = block_coefficients.into_iter().collect::<Vec<_>>();
                records.sort_unstable_by_key(|(key, _)| *key);
                artifact.write_block(&records)?;
            }
        }
        Ok(())
    }

    fn finish_artifact(&mut self) -> std::io::Result<Option<ReplayArtifactObservation>> {
        self.artifact
            .take()
            .map(ReplayArtifactSink::finish)
            .transpose()
    }

    fn key(&self, uv_lambda: [f64; 2]) -> Option<u64> {
        let x = self.tap_span(
            uv_lambda[0] / self.du_lambda + self.grid_shape[0] as f64 / 2.0,
            self.grid_shape[0],
        )?;
        let y = self.tap_span(
            -uv_lambda[1] / self.dv_lambda + self.grid_shape[1] as f64 / 2.0,
            self.grid_shape[1],
        )?;
        Some(
            u64::try_from(x.0).ok()?
                | (u64::try_from(y.0).ok()? << 12)
                | (u64::try_from(x.1).ok()? << 24)
                | (u64::try_from(y.1).ok()? << 31),
        )
    }

    fn tap_span(&self, coordinate: f64, size: usize) -> Option<(usize, usize)> {
        if !coordinate.is_finite() {
            return None;
        }
        let anchor = coordinate.round() as isize;
        let offset =
            ((anchor as f64 - coordinate) * NORMAL_REPLAY_OVERSAMPLING as f64).round() as isize;
        let start = anchor - NORMAL_REPLAY_SUPPORT;
        let end = anchor + NORMAL_REPLAY_SUPPORT;
        let index = offset + NORMAL_REPLAY_OVERSAMPLING / 2;
        (start >= 0 && end < size as isize && (0..=NORMAL_REPLAY_OVERSAMPLING).contains(&index))
            .then_some((start as usize, index as usize))
    }

    fn observe_key(&mut self, key: u64) {
        let hash = mix64(key);
        let index = (hash >> (64 - HLL_PRECISION)) as usize;
        let remaining = hash << HLL_PRECISION;
        let rank = remaining.leading_zeros().saturating_add(1) as u8;
        self.registers[index] = self.registers[index].max(rank);
    }

    fn estimated_global_records(&self) -> u64 {
        let buckets = self.registers.len() as f64;
        let harmonic = self
            .registers
            .iter()
            .map(|rank| 2.0_f64.powi(-i32::from(*rank)))
            .sum::<f64>();
        let alpha = 0.7213 / (1.0 + 1.079 / buckets);
        (alpha * buckets * buckets / harmonic).round() as u64
    }
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

struct ProbeProblem {
    problem: CompiledProblem,
    request: SelectedObservationResolutionRequest,
    selected: BoundSelectedObservation,
    selected_rows: u64,
}

struct CapturedBlocks<'a> {
    blocks: Vec<SelectedObservationBlock>,
    consumer: SelectedObservationBlockConsumer<'a>,
    terminal: SelectedObservationTerminal,
    logical_bytes: u64,
    read_operations: u64,
    current_bytes: u64,
    capacity_bytes: u64,
    elapsed: Duration,
}

struct StageLocalReplayStorage {
    _root: TempDir,
    storage: GriddedNormalReplayStorage,
    resource_signature: [String; 4],
    maximum_artifact_bytes: u64,
    io_buffer_bytes: u64,
}

struct InitialWeightedProbe<'a> {
    problem: &'a CompiledProblem,
    request: &'a SelectedObservationResolutionRequest,
    plan: &'a casa_imaging_reconstruction::WeightingPlan,
    blocks: &'a [SelectedObservationBlock],
    selected_generation: SelectedObservationGenerationId,
    selected_replay_proof: &'a SelectedObservationReplayProof,
    replay_storage: &'a StageLocalReplayStorage,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct InitialWeightedSignature {
    weighting_generation: String,
    weighting_replay: String,
    weighting_coverage: String,
    weighting_residency_bytes: usize,
    selected_generation_proof_bytes: u64,
    selected_generation_proof_hash_calls: u64,
    weighting_coverage_proof_bytes: u64,
    weighting_coverage_proof_hash_calls: u64,
    operator_coverage_proof_bytes: u64,
    operator_coverage_proof_hash_calls: u64,
    normal_state_identity: String,
    artifact_identity: String,
    artifact_seal: GriddedNormalArtifactSeal,
    compilation: GriddedNormalCompilationMeasurements,
    write: GriddedNormalArtifactMeasurements,
    resource_signature: [String; 4],
    maximum_artifact_bytes: u64,
    io_buffer_bytes: u64,
    emitted_blocks: u64,
    predicted_samples: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct InitialWeightedTimings {
    stream: Duration,
    weighting_and_contributions: Duration,
    science_consume: Duration,
    compile_block: Duration,
    append_write: Duration,
    stream_orchestration: Duration,
    finish_seal: Duration,
    science_finish: Duration,
    artifact_seal: Duration,
    compiler_finish: Duration,
}

impl InitialWeightedTimings {
    fn total(self) -> Duration {
        self.stream.saturating_add(self.finish_seal)
    }
}

struct InitialWeightedObservation {
    signature: InitialWeightedSignature,
    timings: InitialWeightedTimings,
}

impl StageLocalReplayStorage {
    fn new(problem: &CompiledProblem, max_block_samples: usize) -> Result<Self, Box<dyn Error>> {
        let budget = project_gridded_normal_artifact_budget(problem, max_block_samples)?;
        let root = tempfile::tempdir()?;
        let profile = ProductionStorageProfile::new(
            root.path(),
            budget.maximum_artifact_bytes(),
            budget.maximum_artifact_bytes(),
            1 << 30,
            1 << 30,
            1,
            1,
        )?;
        let authority = ResourceAuthority::detected_with_storage_profile(&profile)?;
        let resources = profile.io_resources();
        let resource_signature = [
            resources.domain().as_str().to_string(),
            resources.read_rate().as_str().to_string(),
            resources.write_rate().as_str().to_string(),
            resources.queue().as_str().to_string(),
        ];
        let storage = GriddedNormalReplayStorage::bind(&authority, resources, root.path())?;
        Ok(Self {
            _root: root,
            storage,
            resource_signature,
            maximum_artifact_bytes: budget.maximum_artifact_bytes(),
            io_buffer_bytes: budget.io_buffer_bytes(),
        })
    }
}

fn rebuild_density_for_stage_local_probe(
    problem: &CompiledProblem,
    plan: &casa_imaging_reconstruction::WeightingPlan,
    blocks: &[SelectedObservationBlock],
    request: &SelectedObservationResolutionRequest,
    proof: &SelectedObservationReplayProof,
) -> Result<casa_imaging_reconstruction::WeightingDensityPhase, Box<dyn Error>> {
    let consumer = fresh_rebound_consumer(request, problem, proof)?;
    let mut kernel = DensityBlockKernel {
        problem,
        consumer,
        density: begin_weighting_generation(problem, plan)?,
        spectral_contributions: SpectralContributionCache::new(),
    };
    for block in blocks {
        kernel.consume_selected_block(block)?;
    }
    Ok(kernel.complete()?.density)
}

impl InitialWeightedProbe<'_> {
    fn observe(&self, observe_timings: bool) -> Result<InitialWeightedObservation, Box<dyn Error>> {
        let problem = self.problem;
        let request = self.request;
        let plan = self.plan;
        let blocks = self.blocks;
        let selected_generation = self.selected_generation;
        let selected_replay_proof = self.selected_replay_proof;
        let replay_storage = self.replay_storage;
        let density = rebuild_density_for_stage_local_probe(
            problem,
            plan,
            blocks,
            request,
            selected_replay_proof,
        )?;
        let initial_weights = density.finish_into_stream(problem, plan)?;
        let lifecycle = ModelLifecycle::bind(
            ExecutableModelProblem::from_compiled(problem.clone())?,
            attempt(3),
            3,
        )?;
        let initial_model = lifecycle.initial_empty()?;
        let initial_preparation = MajorCyclePreparation::prepare(&lifecycle, initial_model, None)?;
        let specification = SpectralOperatorSpecification::new(problem)?;
        let workload = spectral_operator_workload(
            &specification,
            plan.limits().max_block_samples(),
            SpectralOperatorPass::InitialMajor,
        )?;
        let mut operator =
            prepare_spectral_operator(specification, workload)?.begin_streaming(problem)?;
        operator.bind_major_cycle_model(initial_preparation.final_model(), None)?;
        let consumer = fresh_rebound_consumer(request, problem, selected_replay_proof)?;
        let mut compilation = GriddedNormalReplayCompilation::new_stage_local_probe(
            problem,
            &replay_storage.storage,
            plan.limits().max_block_samples(),
            observe_timings,
        )?;

        let mut science_consume = Duration::ZERO;
        let mut callback_elapsed = Duration::ZERO;
        let mut predicted_samples = 0_u64;
        let mut emitted_blocks = 0_u64;
        let stream_started = Instant::now();
        let WeightingBlockKernelCompletion {
            consumer: replay_consumer,
            weights: (_weighting, replay_summary),
            ..
        } = {
            let mut emit = |block: &ReconstructionWeightedBlock| {
                let callback_started = observe_timings.then(Instant::now);
                let science_started = observe_timings.then(Instant::now);
                let predicted = operator.consume_block(block)?;
                if let Some(started) = science_started {
                    science_consume += started.elapsed();
                }
                compilation.consume_block(block)?;
                predicted_samples = predicted_samples
                    .checked_add(u64::try_from(predicted.len()).expect("prediction count fits u64"))
                    .expect("prediction count does not overflow");
                emitted_blocks = emitted_blocks
                    .checked_add(1)
                    .expect("block count does not overflow");
                if let Some(started) = callback_started {
                    callback_elapsed += started.elapsed();
                }
                Ok::<(), ReplayProbeError>(())
            };
            replay_weighting_kernel(
                WeightingBlockKernel {
                    problem,
                    consumer,
                    weights: initial_weights,
                    continuum: None,
                    spectral_support_sample_count: 0,
                    spectral_contributions: SpectralContributionCache::new(),
                    emit: &mut emit,
                },
                blocks,
            )?
        };
        let stream = stream_started.elapsed();

        let finish_started = Instant::now();
        let science_finish_started = observe_timings.then(Instant::now);
        let result = operator.complete(&replay_summary, selected_generation, None)?;
        let science_finish =
            science_finish_started.map_or(Duration::ZERO, |started| started.elapsed());
        compilation.seal()?;
        let compilation_timings = compilation.stage_timings().unwrap_or_default();
        let compilation_measurements = compilation.compilation_measurements();
        let write_measurements = compilation.write_measurements();
        let compiler_finish_started = observe_timings.then(Instant::now);
        let frozen =
            compilation.complete_stage_local_probe(&replay_summary, selected_generation)?;
        let compiler_finish =
            compiler_finish_started.map_or(Duration::ZERO, |started| started.elapsed());
        let finish_seal = finish_started.elapsed();

        let measured_callback = science_consume
            .saturating_add(compilation_timings.compile_block)
            .saturating_add(compilation_timings.append_frame);
        let weighting_and_contributions = if observe_timings {
            stream.saturating_sub(callback_elapsed)
        } else {
            Duration::ZERO
        };
        let stream_orchestration = callback_elapsed.saturating_sub(measured_callback);
        let descriptor = frozen.descriptor();
        let artifact_seal = frozen.stage_local_artifact_seal();
        let signature = InitialWeightedSignature {
            weighting_generation: replay_summary.weighting_generation().to_string(),
            weighting_replay: replay_summary.replay_id().to_string(),
            weighting_coverage: replay_summary.coverage().to_string(),
            weighting_residency_bytes: replay_summary.residency().peak_bytes(),
            selected_generation_proof_bytes: replay_consumer.generation_proof_bytes(),
            selected_generation_proof_hash_calls: replay_consumer.generation_proof_hash_calls(),
            weighting_coverage_proof_bytes: replay_summary.coverage_proof_bytes(),
            weighting_coverage_proof_hash_calls: replay_summary.coverage_proof_hash_calls(),
            operator_coverage_proof_bytes: result.completion().coverage_proof_bytes(),
            operator_coverage_proof_hash_calls: result.completion().coverage_proof_hash_calls(),
            normal_state_identity: result
                .primitives()
                .normal_state_content_identity()
                .to_string(),
            artifact_identity: descriptor.identity().to_string(),
            artifact_seal,
            compilation: compilation_measurements,
            write: write_measurements,
            resource_signature: replay_storage.resource_signature.clone(),
            maximum_artifact_bytes: replay_storage.maximum_artifact_bytes,
            io_buffer_bytes: replay_storage.io_buffer_bytes,
            emitted_blocks,
            predicted_samples,
        };
        Ok(InitialWeightedObservation {
            signature,
            timings: InitialWeightedTimings {
                stream,
                weighting_and_contributions,
                science_consume,
                compile_block: compilation_timings.compile_block,
                append_write: compilation_timings.append_frame,
                stream_orchestration,
                finish_seal,
                science_finish,
                artifact_seal: compilation_timings.seal,
                compiler_finish,
            },
        })
    }
}

#[test]
#[ignore = "requires the mounted VLA medium performance dataset"]
fn medium_vla_64ch_owner_validated_open() -> Result<(), Box<dyn Error>> {
    let probe = build_problem(&dataset_path()?)?;
    assert_eq!(probe.selected_rows, EXPECTED_SELECTED_ROWS);
    Ok(())
}

#[test]
#[ignore = "requires the mounted VLA medium performance dataset"]
fn medium_vla_64ch_initial_weighted_construction_discriminator() -> Result<(), Box<dyn Error>> {
    let ProbeProblem {
        problem,
        request,
        selected,
        selected_rows,
    } = build_problem(&dataset_path()?)?;
    let selected_samples = selected_rows
        .checked_mul(64 * 2)
        .ok_or("selected sample count overflowed")?;
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(WEIGHTED_BLOCK_SAMPLES, 1)?,
    )?;
    let CapturedBlocks {
        blocks,
        consumer,
        terminal,
        logical_bytes,
        read_operations,
        current_bytes,
        capacity_bytes,
        elapsed: capture_elapsed,
    } = capture_blocks(selected, &problem)?;
    assert_eq!(
        blocks.len(),
        EXPECTED_CAPTURED_BLOCKS,
        "bounded source block shape changed"
    );
    assert!(
        blocks.len() <= CAPTURED_BLOCK_LIMIT
            && usize::try_from(capacity_bytes)? <= CAPTURED_RESIDENCY_BYTES,
        "captured source residency exceeded its fixed bound"
    );
    assert_eq!(
        [
            logical_bytes,
            read_operations,
            current_bytes,
            capacity_bytes,
        ],
        [
            EXPECTED_CAPTURED_LOGICAL_BYTES,
            EXPECTED_CAPTURED_READ_OPERATIONS,
            EXPECTED_CAPTURED_CURRENT_BYTES,
            EXPECTED_CAPTURED_CAPACITY_BYTES,
        ],
        "captured source I/O or residency invariants changed"
    );

    // This owner-validated traversal only mints the replay capability used by
    // all three measurements. Density reconstruction and artifact admission
    // are intentionally outside the timed stage-local discriminator.
    let (selected_generation, selected_replay_proof, _density) = freeze_density(
        &problem,
        &plan,
        &blocks,
        consumer,
        terminal,
        [current_bytes, capacity_bytes],
        selected_samples,
    )?;
    let replay_storage = StageLocalReplayStorage::new(&problem, plan.limits().max_block_samples())?;
    let probe = InitialWeightedProbe {
        problem: &problem,
        request: &request,
        plan: &plan,
        blocks: &blocks,
        selected_generation,
        selected_replay_proof: &selected_replay_proof,
        replay_storage: &replay_storage,
    };
    let baseline_before = probe.observe(false)?;
    let observed = probe.observe(true)?;
    let baseline_after = probe.observe(false)?;

    assert_eq!(
        baseline_before.signature, observed.signature,
        "enabling observation changed scientific, allocation, or resource identity"
    );
    assert_eq!(
        baseline_before.signature, baseline_after.signature,
        "the repeated baseline changed scientific, allocation, or resource identity"
    );
    let signature = &observed.signature;
    let compilation = signature.compilation;
    let write = signature.write;
    let seal = signature.artifact_seal;
    assert_eq!(
        [selected_rows, selected_samples],
        [EXPECTED_SELECTED_ROWS, EXPECTED_SELECTED_SAMPLES],
        "fixture cardinality changed"
    );
    assert_eq!(
        [signature.emitted_blocks, signature.predicted_samples],
        [EXPECTED_WEIGHTED_BLOCKS, EXPECTED_SELECTED_SAMPLES],
        "weighted block shape or prediction count changed"
    );
    assert_eq!(compilation.blocks, EXPECTED_WEIGHTED_BLOCKS);
    assert!(
        compilation.source_group_count >= compilation.reduced_group_count
            && compilation.source_record_count >= compilation.reduced_record_count,
        "block-local reduction increased group or record cardinality"
    );
    assert_eq!(
        compilation.reduced_group_count, compilation.reduction_map_entry_insertions,
        "one map insertion must mint each reduced group"
    );
    assert_eq!(
        compilation.reduced_record_count,
        write.record_count(),
        "compiled and written record counts differ"
    );
    assert_eq!(
        compilation.encoded_buffer_bytes,
        write.payload_bytes(),
        "compiled and written payload bytes differ"
    );
    assert_eq!(
        compilation.reduced_record_count * u64::try_from(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)?,
        compilation.encoded_buffer_bytes,
        "fixed-width encoded record accounting changed"
    );
    assert_eq!(
        [
            seal.frame_count(),
            seal.record_count(),
            seal.payload_bytes()
        ],
        [
            write.frame_count(),
            write.record_count(),
            write.payload_bytes()
        ],
        "sealed artifact and writer counters differ"
    );
    assert_eq!(seal.artifact_bytes(), write.artifact_bytes());
    assert_ne!(seal.global_sha256(), [0; 32]);
    assert_eq!(write.frame_count(), EXPECTED_WEIGHTED_BLOCKS);
    assert_eq!(write.transferred_bytes(), write.artifact_bytes());
    assert_eq!(write.operations(), write.frame_count() + 2);
    assert_eq!(write.sha256_calls(), write.frame_count() + 1);
    assert_eq!(write.payload_copy_bytes(), write.payload_bytes());
    assert_eq!(
        write.payload_copy_operations(),
        compilation.encoded_buffer_allocations,
        "one writer copy must correspond to each non-empty encoded block"
    );
    assert_eq!(write.buffer_allocations(), 1);
    assert_eq!(write.buffer_reuses(), write.frame_count() - 1);
    assert_eq!(write.peak_buffer_bytes(), signature.io_buffer_bytes);
    assert!(write.artifact_bytes() <= signature.maximum_artifact_bytes);
    assert_eq!(
        [
            signature.selected_generation_proof_bytes,
            signature.selected_generation_proof_hash_calls,
        ],
        [0, 0],
        "rebound selected-generation proof must perform zero replay hashing"
    );
    assert_eq!(
        [
            signature.weighting_coverage_proof_bytes,
            signature.weighting_coverage_proof_hash_calls,
        ],
        [
            signature.operator_coverage_proof_bytes,
            signature.operator_coverage_proof_hash_calls,
        ],
        "weighting and science owners must account for the same initial coverage work"
    );
    assert_eq!(
        [
            signature.weighting_generation.as_str(),
            signature.weighting_replay.as_str(),
            signature.weighting_coverage.as_str(),
            signature.normal_state_identity.as_str(),
            signature.artifact_identity.as_str(),
            sha256_hex(seal.global_sha256()).as_str(),
        ],
        [
            EXPECTED_INITIAL_WEIGHTING_GENERATION,
            EXPECTED_INITIAL_WEIGHTING_REPLAY,
            EXPECTED_INITIAL_WEIGHTING_COVERAGE,
            EXPECTED_INITIAL_WEIGHTED_NORMAL_STATE_IDENTITY,
            EXPECTED_INITIAL_WEIGHTED_ARTIFACT_IDENTITY,
            EXPECTED_INITIAL_WEIGHTED_ARTIFACT_SHA256,
        ],
        "initial weighted scientific or artifact identity changed"
    );
    assert_eq!(
        [
            compilation.blocks,
            compilation.source_group_count,
            compilation.source_record_count,
            compilation.reduced_group_count,
            compilation.reduced_record_count,
            compilation.source_group_vector_allocations,
            compilation.source_group_capacity_growth_bytes,
            compilation.reduction_map_entry_insertions,
            compilation.multiplicity_vector_allocations,
            compilation.multiplicity_capacity_growth_bytes,
            compilation.encoded_buffer_allocations,
            compilation.encoded_buffer_bytes,
            compilation.descriptor_vector_allocations,
            compilation.descriptor_capacity_growth_bytes,
        ],
        [
            8_227,
            29_169_920,
            29_169_920,
            14_520_731,
            14_520_731,
            29_169_920,
            4_667_187_200,
            14_520_731,
            14_521_550,
            464_689_600,
            8_137,
            464_663_392,
            13,
            786_432,
        ],
        "initial weighted compiler allocation or cardinality signature changed"
    );
    assert_eq!(
        [
            write.artifact_bytes(),
            write.payload_bytes(),
            write.frame_count(),
            write.record_count(),
            write.transferred_bytes(),
            write.operations(),
            write.sha256_bytes(),
            write.sha256_calls(),
            write.peak_buffer_bytes(),
            write.payload_copy_bytes(),
            write.payload_copy_operations(),
            write.buffer_allocations(),
            write.buffer_reuses(),
        ],
        [
            465_255_832,
            464_663_392,
            8_227,
            14_520_731,
            465_255_832,
            8_229,
            929_919_144,
            8_228,
            EXPECTED_INITIAL_ARTIFACT_IO_BUFFER_BYTES,
            464_663_392,
            8_137,
            1,
            8_226,
        ],
        "initial weighted artifact write/copy signature changed"
    );
    assert_eq!(
        [
            signature.weighting_coverage_proof_bytes,
            signature.weighting_coverage_proof_hash_calls,
            signature.operator_coverage_proof_bytes,
            signature.operator_coverage_proof_hash_calls,
        ],
        [
            EXPECTED_INITIAL_COVERAGE_PROOF_BYTES,
            EXPECTED_INITIAL_COVERAGE_PROOF_HASH_CALLS,
            EXPECTED_INITIAL_COVERAGE_PROOF_BYTES,
            EXPECTED_INITIAL_COVERAGE_PROOF_HASH_CALLS,
        ],
        "initial weighted proof-work signature changed"
    );
    assert_eq!(
        [
            signature.weighting_residency_bytes,
            usize::try_from(signature.maximum_artifact_bytes)?,
            usize::try_from(signature.io_buffer_bytes)?,
        ],
        [
            EXPECTED_INITIAL_WEIGHTING_RESIDENCY_BYTES,
            usize::try_from(EXPECTED_INITIAL_ARTIFACT_MAXIMUM_BYTES)?,
            usize::try_from(EXPECTED_INITIAL_ARTIFACT_IO_BUFFER_BYTES)?,
        ],
        "initial weighted residency or artifact budget signature changed"
    );

    let baseline_mean_seconds = (baseline_before.timings.total().as_secs_f64()
        + baseline_after.timings.total().as_secs_f64())
        / 2.0;
    let repeatability = (baseline_before.timings.total().as_secs_f64()
        - baseline_after.timings.total().as_secs_f64())
    .abs()
        / baseline_mean_seconds;
    let observer_overhead = (observed.timings.total().as_secs_f64() - baseline_mean_seconds)
        .max(0.0)
        / baseline_mean_seconds;
    let measured_stages = observed
        .timings
        .weighting_and_contributions
        .saturating_add(observed.timings.science_consume)
        .saturating_add(observed.timings.compile_block)
        .saturating_add(observed.timings.append_write)
        .saturating_add(observed.timings.stream_orchestration)
        .saturating_add(observed.timings.science_finish)
        .saturating_add(observed.timings.artifact_seal)
        .saturating_add(observed.timings.compiler_finish);
    let orchestration = observed.timings.total().saturating_sub(measured_stages);

    println!(
        "{}",
        serde_json::to_string(&json!({
            "schema": "casa-rs-initial-weighted-discriminator-v1",
            "source_revision": source_revision()?,
            "dataset": DATASET_RELATIVE_PATH,
            "problem_id": problem.problem_id().to_string(),
            "workers": 1,
            "partitions_per_block": 1,
            "selected_rows": selected_rows,
            "selected_samples": selected_samples,
            "captured_blocks": blocks.len(),
            "captured_logical_bytes": logical_bytes,
            "captured_read_operations": read_operations,
            "captured_current_bytes": current_bytes,
            "captured_capacity_bytes": capacity_bytes,
            "capture_ms": milliseconds(capture_elapsed),
            "baseline_before_ms": milliseconds(baseline_before.timings.total()),
            "observed_ms": milliseconds(observed.timings.total()),
            "baseline_after_ms": milliseconds(baseline_after.timings.total()),
            "baseline_repeatability_fraction": repeatability,
            "observer_overhead_fraction": observer_overhead,
            "stages_ms": {
                "weighting_and_contribution_formation": milliseconds(observed.timings.weighting_and_contributions),
                "initial_science_operator_consume": milliseconds(observed.timings.science_consume),
                "reconstruction_compile_block": milliseconds(observed.timings.compile_block),
                "runtime_artifact_append_write": milliseconds(observed.timings.append_write),
                "stream_callback_orchestration": milliseconds(observed.timings.stream_orchestration),
                "science_operator_finish": milliseconds(observed.timings.science_finish),
                "runtime_artifact_finish_seal": milliseconds(observed.timings.artifact_seal),
                "reconstruction_compiler_finish": milliseconds(observed.timings.compiler_finish),
                "stage_orchestration": milliseconds(orchestration),
            },
            "timing_boundaries_ms": {
                "stream": milliseconds(observed.timings.stream),
                "finish_seal": milliseconds(observed.timings.finish_seal),
            },
            "identity": {
                "weighting_generation": signature.weighting_generation,
                "weighting_replay": signature.weighting_replay,
                "weighting_coverage": signature.weighting_coverage,
                "normal_state": signature.normal_state_identity,
                "artifact": signature.artifact_identity,
                "artifact_sha256": sha256_hex(seal.global_sha256()),
            },
            "residency": {
                "weighting_peak_bytes": signature.weighting_residency_bytes,
                "artifact_maximum_bytes": signature.maximum_artifact_bytes,
                "artifact_io_buffer_bytes": signature.io_buffer_bytes,
                "artifact_peak_buffer_bytes": write.peak_buffer_bytes(),
            },
            "resource_signature": signature.resource_signature,
            "compilation_counters": {
                "blocks": compilation.blocks,
                "source_groups": compilation.source_group_count,
                "source_records": compilation.source_record_count,
                "reduced_groups": compilation.reduced_group_count,
                "reduced_records": compilation.reduced_record_count,
                "source_group_vector_allocations": compilation.source_group_vector_allocations,
                "source_group_capacity_growth_bytes": compilation.source_group_capacity_growth_bytes,
                "reduction_map_entry_insertions": compilation.reduction_map_entry_insertions,
                "multiplicity_vector_allocations": compilation.multiplicity_vector_allocations,
                "multiplicity_capacity_growth_bytes": compilation.multiplicity_capacity_growth_bytes,
                "encoded_buffer_allocations": compilation.encoded_buffer_allocations,
                "encoded_buffer_bytes": compilation.encoded_buffer_bytes,
                "descriptor_vector_allocations": compilation.descriptor_vector_allocations,
                "descriptor_capacity_growth_bytes": compilation.descriptor_capacity_growth_bytes,
            },
            "write_counters": {
                "artifact_bytes": write.artifact_bytes(),
                "payload_bytes": write.payload_bytes(),
                "frames": write.frame_count(),
                "records": write.record_count(),
                "transferred_bytes": write.transferred_bytes(),
                "operations": write.operations(),
                "sha256_bytes": write.sha256_bytes(),
                "sha256_calls": write.sha256_calls(),
                "payload_copy_bytes": write.payload_copy_bytes(),
                "payload_copy_operations": write.payload_copy_operations(),
                "buffer_allocations": write.buffer_allocations(),
                "buffer_reuses": write.buffer_reuses(),
            },
            "proof_counters": {
                "selected_generation_bytes": signature.selected_generation_proof_bytes,
                "selected_generation_hash_calls": signature.selected_generation_proof_hash_calls,
                "weighting_coverage_bytes": signature.weighting_coverage_proof_bytes,
                "weighting_coverage_hash_calls": signature.weighting_coverage_proof_hash_calls,
                "operator_coverage_bytes": signature.operator_coverage_proof_bytes,
                "operator_coverage_hash_calls": signature.operator_coverage_proof_hash_calls,
            },
        }))?
    );
    assert!(
        repeatability <= BASELINE_REPEATABILITY_LIMIT,
        "OFF/OFF baseline repeatability exceeded the three-percent bound: {repeatability:.6}"
    );
    assert!(
        observer_overhead <= OBSERVER_OVERHEAD_LIMIT,
        "stage observation exceeded the two-percent overhead bound: {observer_overhead:.6}"
    );
    Ok(())
}

#[test]
#[ignore = "requires the mounted VLA medium performance dataset"]
fn medium_vla_64ch_residual_refresh() -> Result<(), Box<dyn Error>> {
    let total_start = Instant::now();
    let ProbeProblem {
        problem,
        request,
        selected,
        selected_rows,
    } = build_problem(&dataset_path()?)?;
    let selected_samples = selected_rows
        .checked_mul(64 * 2)
        .ok_or("selected sample count overflowed")?;
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(WEIGHTED_BLOCK_SAMPLES, 1)?,
    )?;
    let captured = capture_blocks(selected, &problem)?;
    assert!(
        captured.blocks.len() <= CAPTURED_BLOCK_LIMIT,
        "captured block count exceeded the fixed residency bound"
    );
    assert_eq!(
        captured.blocks.len(),
        EXPECTED_CAPTURED_BLOCKS,
        "bounded source block shape changed"
    );
    assert!(
        usize::try_from(captured.capacity_bytes)? <= CAPTURED_RESIDENCY_BYTES,
        "captured block capacity exceeded the fixed residency bound"
    );

    let CapturedBlocks {
        blocks,
        consumer: density_consumer,
        terminal,
        logical_bytes,
        read_operations,
        current_bytes,
        capacity_bytes,
        elapsed: capture_elapsed,
    } = captured;
    let setup_start = Instant::now();
    let (selected_generation, selected_replay_proof, density) = freeze_density(
        &problem,
        &plan,
        &blocks,
        density_consumer,
        terminal,
        [current_bytes, capacity_bytes],
        selected_samples,
    )?;
    let mut lifecycle = ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone())?,
        attempt(1),
        1,
    )?;
    let initial_model = lifecycle.initial_empty()?;
    let delta = lifecycle.compile_delta(
        &initial_model,
        [ModelDeltaTerm::new(
            ModelCell::new(0, 0, 0, [512, 512]),
            casa_imaging_model::ModelValue::new(1.0)?,
        )],
    )?;
    let initial_preparation =
        MajorCyclePreparation::prepare(&lifecycle, initial_model, Some(delta))?;

    let initial_weights = density.finish_into_stream(&problem, &plan)?;
    let specification = SpectralOperatorSpecification::new(&problem)?;
    let mut normal_replay_probe =
        NormalReplayCardinalityProbe::new(&problem, specification.grid_shape())?;
    let workload = spectral_operator_workload(
        &specification,
        plan.limits().max_block_samples(),
        SpectralOperatorPass::InitialMajor,
    )?;
    let mut initial_operator =
        prepare_spectral_operator(specification, workload)?.begin_streaming(&problem)?;
    initial_operator.bind_major_cycle_model(initial_preparation.final_model(), None)?;
    let initial_consumer = fresh_consumer(&request, &problem)?;
    let (weighting, initial_summary) = {
        let mut initial_emit = |block: &ReconstructionWeightedBlock| {
            initial_operator.consume_block(block)?;
            Ok::<(), casa_imaging_reconstruction::SpectralOperatorError>(())
        };
        let initial_kernel = WeightingBlockKernel {
            problem: &problem,
            consumer: initial_consumer,
            weights: initial_weights,
            continuum: None,
            spectral_support_sample_count: 0,
            spectral_contributions: SpectralContributionCache::new(),
            emit: &mut initial_emit,
        };
        let WeightingBlockKernelCompletion {
            weights: (weighting, summary),
            ..
        } = replay_weighting_kernel(initial_kernel, &blocks)?;
        (weighting, summary)
    };
    let initial_complete =
        initial_operator.complete(&initial_summary, selected_generation, None)?;
    let coverage_proof = FrozenWeightingCoverageProof::seal(
        &problem,
        &weighting,
        &initial_summary,
        selected_generation,
        selected_samples,
        None,
    )?;
    let initial_join = MajorCycleOwner::from_complete_data(initial_complete, initial_preparation)?
        .reconcile(&mut lifecycle)?;
    let (prior_normal_state, continuation) = initial_join.into_continuation();
    let (continued_lifecycle, carried_model) = ModelLifecycle::continue_from(
        ExecutableModelProblem::from_compiled(problem.clone())?,
        attempt(2),
        2,
        continuation,
    )?;
    let preparation = MajorCyclePreparation::prepare(&continued_lifecycle, carried_model, None)?;
    let setup_elapsed = setup_start.elapsed();

    let specification = SpectralOperatorSpecification::new(&problem)?;
    let workload = spectral_operator_workload(
        &specification,
        plan.limits().max_block_samples(),
        SpectralOperatorPass::ResidualRefresh,
    )?;
    let mut operator =
        prepare_spectral_operator(specification, workload)?.begin(&problem, &weighting)?;
    operator.bind_major_cycle_model(preparation.final_model(), Some(prior_normal_state))?;
    operator.authorize_derived_coverage(coverage_proof)?;
    let replay = weighting.begin_derived_replay(&problem, &plan, coverage_proof, None)?;
    let consumer = fresh_rebound_consumer(&request, &problem, &selected_replay_proof)?;
    let (
        replay_summary,
        replay_consumer,
        replay_elapsed,
        operator_elapsed,
        normal_replay_probe_elapsed,
        predicted_samples,
        emitted_blocks,
    ) = {
        let mut operator_elapsed = Duration::ZERO;
        let mut normal_replay_probe_elapsed = Duration::ZERO;
        let mut predicted_samples = 0_u64;
        let mut emitted_blocks = 0_u64;
        let mut emit = |block: &ReconstructionWeightedBlock| {
            let probe_started = Instant::now();
            normal_replay_probe.observe(block)?;
            normal_replay_probe_elapsed += probe_started.elapsed();
            let started = Instant::now();
            let predicted = operator.consume_block(block)?;
            operator_elapsed += started.elapsed();
            predicted_samples = predicted_samples
                .checked_add(u64::try_from(predicted.len()).expect("prediction count fits u64"))
                .expect("prediction count does not overflow");
            emitted_blocks = emitted_blocks
                .checked_add(1)
                .expect("block count does not overflow");
            Ok::<(), ReplayProbeError>(())
        };
        let kernel = WeightingBlockKernel {
            problem: &problem,
            consumer,
            weights: replay,
            continuum: None,
            spectral_support_sample_count: 0,
            spectral_contributions: SpectralContributionCache::new(),
            emit: &mut emit,
        };
        let replay_started = Instant::now();
        let WeightingBlockKernelCompletion {
            consumer,
            weights: replay_summary,
            ..
        } = replay_weighting_kernel(kernel, &blocks)?;
        (
            replay_summary,
            consumer,
            replay_started.elapsed(),
            operator_elapsed,
            normal_replay_probe_elapsed,
            predicted_samples,
            emitted_blocks,
        )
    };
    assert_eq!(replay_summary.sample_count(), selected_samples);
    coverage_proof.validate_derived_replay(
        selected_generation,
        selected_samples,
        None,
        &replay_summary,
    )?;
    let finish_started = Instant::now();
    let result = operator.complete(&replay_summary, selected_generation, None)?;
    let finish_elapsed = finish_started.elapsed();
    let selected_generation_proof_bytes = replay_consumer.generation_proof_bytes();
    let selected_generation_proof_hash_calls = replay_consumer.generation_proof_hash_calls();
    let weighting_coverage_proof_bytes = replay_summary.coverage_proof_bytes();
    let weighting_coverage_proof_hash_calls = replay_summary.coverage_proof_hash_calls();
    let operator_coverage_proof_bytes = result.completion().coverage_proof_bytes();
    let operator_coverage_proof_hash_calls = result.completion().coverage_proof_hash_calls();
    let total_coverage_proof_bytes = weighting_coverage_proof_bytes
        .checked_add(operator_coverage_proof_bytes)
        .ok_or("coverage proof byte count overflowed")?;
    let total_coverage_proof_hash_calls = weighting_coverage_proof_hash_calls
        .checked_add(operator_coverage_proof_hash_calls)
        .ok_or("coverage proof hash-call count overflowed")?;
    let checksum = result.primitives().normal_state_content_identity();
    let checksum_text = checksum.to_string();
    let source_revision = source_revision()?;
    let replay_without_probe = replay_elapsed.saturating_sub(normal_replay_probe_elapsed);
    let weighting_exclusive = replay_without_probe.saturating_sub(operator_elapsed);
    let estimated_global_records = normal_replay_probe.estimated_global_records();
    let replay_artifact = normal_replay_probe.finish_artifact()?;
    if let Some(artifact) = &replay_artifact {
        assert_eq!(
            artifact.bytes,
            artifact.records * NORMAL_REPLAY_RECORD_BYTES,
            "normal-replay artifact record width changed"
        );
        assert_eq!(
            artifact.read_bytes, artifact.bytes,
            "normal-replay artifact readback length changed"
        );
    }
    let replay_artifact_json = replay_artifact.as_ref().map(|artifact| {
        json!({
            "path": artifact.path,
            "records": artifact.records,
            "bytes": artifact.bytes,
            "write_ms": milliseconds(artifact.write_elapsed),
            "sequential_read_ms": milliseconds(artifact.read_elapsed),
            "sequential_read_bytes": artifact.read_bytes,
            "sha256": artifact.sha256,
        })
    });
    let projected_full_block_records = normal_replay_probe
        .block_reduced_records
        .checked_mul(FULL_WORKLOAD_SELECTED_SAMPLES)
        .and_then(|records| records.checked_add(EXPECTED_SELECTED_SAMPLES - 1))
        .map(|records| records / EXPECTED_SELECTED_SAMPLES)
        .ok_or("normal replay full-workload projection overflowed")?;
    let projected_full_consecutive_records = normal_replay_probe
        .consecutive_reduced_records
        .checked_mul(FULL_WORKLOAD_SELECTED_SAMPLES)
        .and_then(|records| records.checked_add(EXPECTED_SELECTED_SAMPLES - 1))
        .map(|records| records / EXPECTED_SELECTED_SAMPLES)
        .ok_or("normal replay consecutive full-workload projection overflowed")?;
    let normal_replay_probe_json = json!({
        "representation": "tap-span-key-plus-f64-coefficient",
        "record_bytes": NORMAL_REPLAY_RECORD_BYTES,
        "hll_precision": HLL_PRECISION,
        "hll_resident_bytes": normal_replay_probe.registers.len(),
        "probe_ms": milliseconds(normal_replay_probe_elapsed),
        "raw_contributions": normal_replay_probe.raw_records,
        "contributing_blocks": normal_replay_probe.contributing_blocks,
        "consecutive_reduced_records": normal_replay_probe.consecutive_reduced_records,
        "consecutive_reduction_ratio": normal_replay_probe.raw_records as f64
            / normal_replay_probe.consecutive_reduced_records as f64,
        "block_reduced_records": normal_replay_probe.block_reduced_records,
        "estimated_global_records": estimated_global_records,
        "block_reduction_ratio": normal_replay_probe.raw_records as f64
            / normal_replay_probe.block_reduced_records as f64,
        "estimated_global_reduction_ratio": normal_replay_probe.raw_records as f64
            / estimated_global_records as f64,
        "captured_block_reduced_bytes": normal_replay_probe.block_reduced_records
            * NORMAL_REPLAY_RECORD_BYTES,
        "captured_consecutive_reduced_bytes": normal_replay_probe.consecutive_reduced_records
            * NORMAL_REPLAY_RECORD_BYTES,
        "captured_estimated_global_bytes": estimated_global_records * NORMAL_REPLAY_RECORD_BYTES,
        "projected_full_block_reduced_records": projected_full_block_records,
        "projected_full_block_reduced_bytes": projected_full_block_records
            * NORMAL_REPLAY_RECORD_BYTES,
        "projected_full_consecutive_reduced_records": projected_full_consecutive_records,
        "projected_full_consecutive_reduced_bytes": projected_full_consecutive_records
            * NORMAL_REPLAY_RECORD_BYTES,
        "artifact": replay_artifact_json,
    });
    println!(
        "{}",
        serde_json::to_string(&json!({
            "schema": "casa-rs-serial-compute-discriminator-v1",
            "source_revision": source_revision,
            "dataset": DATASET_RELATIVE_PATH,
            "problem_id": problem.problem_id().to_string(),
            "workers": 1,
            "partitions_per_block": 1,
            "selected_rows": selected_rows,
            "selected_samples": replay_summary.sample_count(),
            "captured_blocks": blocks.len(),
            "captured_logical_bytes": logical_bytes,
            "captured_read_operations": read_operations,
            "captured_current_bytes": current_bytes,
            "captured_capacity_bytes": capacity_bytes,
            "weighted_blocks": emitted_blocks,
            "predicted_samples": predicted_samples,
            "capture_ms": milliseconds(capture_elapsed),
            "setup_ms": milliseconds(setup_elapsed),
            "replay_ms": milliseconds(replay_elapsed),
            "replay_without_probe_ms": milliseconds(replay_without_probe),
            "operator_consume_ms": milliseconds(operator_elapsed),
            "projection_spectral_weighting_ms": milliseconds(weighting_exclusive),
            "operator_finish_ms": milliseconds(finish_elapsed),
            "normal_replay_probe": normal_replay_probe_json,
            "selected_generation_proof_bytes": selected_generation_proof_bytes,
            "selected_generation_proof_hash_calls": selected_generation_proof_hash_calls,
            "selected_generation_proof_terminalized": false,
            "weighting_coverage_proof_bytes": weighting_coverage_proof_bytes,
            "weighting_coverage_proof_hash_calls": weighting_coverage_proof_hash_calls,
            "operator_coverage_proof_bytes": operator_coverage_proof_bytes,
            "operator_coverage_proof_hash_calls": operator_coverage_proof_hash_calls,
            "total_coverage_proof_bytes": total_coverage_proof_bytes,
            "total_coverage_proof_hash_calls": total_coverage_proof_hash_calls,
            "total_ms": milliseconds(total_start.elapsed()),
            "normal_state_identity": checksum_text,
        }))?
    );
    assert_eq!(
        selected_rows, EXPECTED_SELECTED_ROWS,
        "fixture row count changed"
    );
    assert_eq!(
        replay_summary.sample_count(),
        EXPECTED_SELECTED_SAMPLES,
        "fixture sample count changed"
    );
    assert_eq!(
        [
            logical_bytes,
            read_operations,
            current_bytes,
            capacity_bytes
        ],
        [
            EXPECTED_CAPTURED_LOGICAL_BYTES,
            EXPECTED_CAPTURED_READ_OPERATIONS,
            EXPECTED_CAPTURED_CURRENT_BYTES,
            EXPECTED_CAPTURED_CAPACITY_BYTES,
        ],
        "captured source I/O or residency invariants changed"
    );
    assert_eq!(
        [emitted_blocks, predicted_samples],
        [EXPECTED_WEIGHTED_BLOCKS, EXPECTED_SELECTED_SAMPLES],
        "weighted block shape or prediction count changed"
    );
    assert_eq!(
        checksum_text, EXPECTED_NORMAL_STATE_IDENTITY,
        "scientific checksum changed"
    );
    assert!(
        selected_generation_proof_bytes == 0 && selected_generation_proof_hash_calls == 0,
        "rebound selected-generation proof must perform zero timed hashing"
    );
    assert!(
        weighting_coverage_proof_bytes == 0 && weighting_coverage_proof_hash_calls == 0,
        "derived weighting coverage must perform zero timed hashing"
    );
    assert_eq!(
        [
            weighting_coverage_proof_bytes,
            weighting_coverage_proof_hash_calls,
        ],
        [
            operator_coverage_proof_bytes,
            operator_coverage_proof_hash_calls,
        ],
        "weighting and operator coverage derivation must perform the same zero work"
    );
    assert!(
        replay_without_probe.as_secs_f64() <= 8.919_854_174_7,
        "timed candidate replay exceeded the approved discriminator ceiling"
    );
    Ok(())
}

fn build_problem(path: &Path) -> Result<ProbeProblem, Box<dyn Error>> {
    let ms = MeasurementSet::open(path)?;
    let data_description = ms.data_description()?;
    let spectral_window = ms.spectral_window()?;
    let polarization = ms.polarization()?;
    let ddid = 0_usize;
    let spw_id = usize::try_from(data_description.spectral_window_id(ddid)?)?;
    let polarization_id = usize::try_from(data_description.polarization_id(ddid)?)?;
    if spw_id != 0 {
        return Err("probe fixture DDID 0 no longer selects SPW 0".into());
    }
    let row_selection = ms.selected_observation_row_selection(&[0], Some(&[0]), None, None)?;
    let content_budget =
        SelectedObservationContentBudget::new(CAPTURED_RESIDENCY_BYTES, CAPTURED_BLOCK_LIMIT, 4);
    let selection_io = MsSelectionIoBudget {
        available_bytes: content_budget.available_bytes(),
        maximum_live_blocks: content_budget.maximum_live_blocks(),
        requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
        storage_alignment_rows: None,
    };
    let mut full_selected_rows = 0_u64;
    let mut time_bounds = [(None, None); WINDOW_STARTS.len()];
    ms.visit_selected_observation_rows(&row_selection, selection_io, |row| {
        let ordinal = full_selected_rows;
        full_selected_rows += 1;
        for (window, start) in WINDOW_STARTS.into_iter().enumerate() {
            if ordinal == start {
                time_bounds[window].0 = Some(row.time_mjd_seconds());
            }
            if ordinal == start + WINDOW_ROWS - 1 {
                time_bounds[window].1 = Some(row.time_mjd_seconds());
            }
        }
    })?;
    if full_selected_rows != EXPECTED_FULL_SELECTED_ROWS {
        return Err(format!(
            "fixture selected-row count changed: expected {EXPECTED_FULL_SELECTED_ROWS}, got {full_selected_rows}"
        )
        .into());
    }
    let time_ranges = time_bounds
        .into_iter()
        .map(|(lower, upper)| {
            Ok(TimeRange::new(
                Some(SelectionBound::inclusive(
                    lower.ok_or("probe window has no lower timestamp")?,
                )),
                Some(SelectionBound::inclusive(
                    upper.ok_or("probe window has no upper timestamp")?,
                )),
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    let base_rows = row_selection.rows();
    let row_filter = RowSelection::new(
        base_rows.fields().clone(),
        TimeSelection::Ranges(time_ranges),
        base_rows.uv_distances().clone(),
        base_rows.antennas().clone(),
        base_rows.scans().clone(),
        base_rows.observations().clone(),
        base_rows.intents().clone(),
        base_rows.arrays().clone(),
    );
    let mut selected_rows =
        SelectedRowsBuilder::with_data_description_capacity(u64::try_from(ms.row_count())?, 1);
    let mut selected_rows_error = None;
    let mut first_selected_time = None;
    ms.visit_selected_observation_rows(&row_selection, selection_io, |row| {
        let selected = time_bounds.into_iter().any(|(lower, upper)| {
            lower.is_some_and(|lower| row.time_mjd_seconds() >= lower)
                && upper.is_some_and(|upper| row.time_mjd_seconds() <= upper)
        });
        if selected {
            first_selected_time.get_or_insert(row.time_mjd_seconds());
            if row.field_id() != 0 || row.data_description_id() != 0 {
                selected_rows_error = Some("probe window crossed field or DDID".to_string());
            } else if selected_rows_error.is_none() {
                selected_rows_error = selected_rows
                    .push(SelectedMainRow::new(
                        u64::try_from(row.physical_row()).expect("MS row fits u64"),
                        0,
                    ))
                    .err()
                    .map(|error| error.to_string());
            }
        }
    })?;
    if let Some(error) = selected_rows_error {
        return Err(error.into());
    }
    let rows = selected_rows.finish();
    let selected_row_count = rows.selected_row_count();
    if selected_row_count != EXPECTED_SELECTED_ROWS {
        return Err(format!(
            "probe row windows changed: expected {EXPECTED_SELECTED_ROWS}, got {selected_row_count}"
        )
        .into());
    }

    let frequencies = spectral_window.chan_freq(spw_id)?;
    if frequencies.len() < 64 {
        return Err("probe SPW no longer contains 64 channels".into());
    }
    let channels = (0_u32..64).collect::<Vec<_>>();
    let correlation_codes = polarization.corr_type(polarization_id)?;
    let correlations = correlation_codes
        .iter()
        .enumerate()
        .map(|(index, code)| {
            Ok(CorrelationProduct::new(
                u32::try_from(index)?,
                correlation_type(*code)?,
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    if correlations.len() != 2 {
        return Err("probe fixture must retain exactly two correlations".into());
    }
    let observation_selection = ObservationSelection::new(
        rows,
        row_filter,
        row_selection.data_descriptions().to_vec(),
        vec![SpectralWindowSelection::new(spw_id as u32, channels)],
        vec![CorrelationSelection::new(
            u32::try_from(polarization_id)?,
            correlations,
        )],
    );

    let phase = casa_ms::derived::engine::resolve_field_phase_direction_j2000(&ms, 0)?;
    let (right_ascension, declination) = phase.as_angles();
    let cell = 0.25 * std::f64::consts::PI / (180.0 * 3600.0);
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, right_ascension, declination),
        [512.0, 512.0],
        [-cell, cell],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let frame_engine = casa_ms::derived::engine::MsCalEngine::new(&ms)?;
    let source_frequency_reference =
        FrequencyRef::from_casacore_code(spectral_window.meas_freq_ref(spw_id)?)
            .ok_or("unsupported source frequency frame")?;
    let source_frame = imaging_frequency_frame(source_frequency_reference)?;
    let output_reference = FrequencyRef::LSRK;
    let output_frame = FrequencyFrame::Lsrk;
    let anchor_time = first_selected_time.ok_or("probe selected no rows")?;
    let source_reference_frequency = frequencies[..64].iter().sum::<f64>() / 64.0;
    let output_reference_frequency = casa_ms::convert_frequency_to_frame(
        source_frequency_reference,
        output_reference,
        source_reference_frequency,
        anchor_time,
        0,
        &frame_engine,
    )?;
    let [x_metres, y_metres, z_metres] = frame_engine.observatory_position().as_itrf();
    let spectral_anchor = if source_frame == output_frame {
        SpectralFrameAnchor::NotApplicable
    } else {
        SpectralFrameAnchor::Conversion {
            epoch: Epoch::new(
                anchor_time / 86_400.0,
                imaging_time_scale(frame_engine.time_reference())?,
            ),
            direction: direction.reference_direction(),
            observatory_position: ItrfPosition::new(x_metres, y_metres, z_metres),
        }
    };
    let geometry = GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(1024, 1024),
            direction,
            FacetLayout::Single,
            AxisOrder::new([
                ImageAxis::DirectionLongitude,
                ImageAxis::DirectionLatitude,
                ImageAxis::Polarization,
                ImageAxis::Spectral,
            ]),
        )],
        CentreLaws::new(
            PhaseCentreLaw::Fixed(direction.reference_direction()),
            DelayCentreLaw::PhaseTrackingCentre,
            PointingCentreLaw::PhaseTrackingCentre,
        ),
        UvwCoordinateLaw::PhaseTrackingCentre,
        SpectralCoordinateSpec::new(
            source_frame,
            output_frame,
            spectral_anchor,
            SpectralWcs::Linear {
                channels: 1,
                reference_pixel: 0.0,
                reference_frequency_hz: output_reference_frequency,
                increment_hz: 1.0,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let specification = specification()?;
    let visibility = if ms.data_column(VisibilityDataColumn::CorrectedData).is_ok() {
        VisibilityColumn::CorrectedData
    } else {
        VisibilityColumn::Data
    };
    let weights = if ms.main_table().column_accessor("WEIGHT_SPECTRUM").is_ok() {
        WeightColumn::WeightSpectrum
    } else {
        WeightColumn::Weight
    };
    let request = SelectedObservationResolutionRequest::new(
        path.display().to_string(),
        LogicalIdentity::from_sha256([0x64; 32]),
        observation_selection,
        visibility,
        weights,
        Vec::new(),
        ModelStateIdentity::Empty,
        content_budget,
        casa_ms::open_measures_runtime()?,
    );
    let resolved = resolve_selected_observation(request.clone())?;
    let (snapshot, access) = resolved.into_parts();
    let observation = compile_observation(snapshot)?;
    let problem = compile(ImagingRequest::new(
        specification,
        geometry,
        ProblemInputIdentities::new(observation),
        ModelLifecycleRequirements::new(
            ModelBounds::new(1024 * 1024, 1, 1, 1024 * 1024, 1.0e30, 1.0e30)?,
            NumericPrecision::F64,
            ModelInputCommitment::Empty,
        ),
    ))?;
    access.certify_residency(&problem)?;
    let selected = access.open(&problem)?;
    Ok(ProbeProblem {
        problem,
        request,
        selected,
        selected_rows: selected_row_count,
    })
}

fn specification() -> Result<ProblemSpecification, Box<dyn Error>> {
    Ok(ProblemSpecification::new(
        ScientificContract::new(
            SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
            MeasurementEquationContract::new(
                InstrumentResponse::Scalar,
                DeclaredInnerProducts::new(
                    ModelInnerProduct::HermitianEuclidean,
                    VisibilityInnerProduct::HermitianEuclidean,
                ),
            ),
        ),
        ReconstructionContract::new(
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Hogbom,
            ReconstructionControls::new(500, 0.1, 0.0).with_cycle_limits(50, None),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        WeightingContract::new(
            WeightingScheme::Briggs { robust: 0.5 },
            WeightDensityScope::GlobalSelection,
        ),
        ProductRequirements::new(
            vec![
                ProductKind::Psf,
                ProductKind::Residual,
                ProductKind::Model,
                ProductKind::RestoredImage,
                ProductKind::SumWeights,
                ProductKind::Mask,
                ProductKind::Beam,
            ],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::PerPlane,
            ProductValidityPolicies::new(
                PrimaryBeamValidityPolicy::new(
                    0.2,
                    ProductSupportComparison::StrictlyGreater,
                    ProductBlankingPolicy::ZeroAndFalseMask,
                )?,
                TaylorValidityPolicy::new(
                    TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
                    0.1,
                    ProductSupportComparison::StrictlyGreater,
                    ProductBlankingPolicy::ZeroAndFalseMask,
                )?,
            ),
        ),
        ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
        NumericsContract::new(
            vec![NumericPrecision::F64],
            ReductionPolicy::Compensated,
            FiniteValuePolicy::FlagInputRejectGenerated,
            NumericalStage::ALL
                .into_iter()
                .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
                .collect(),
        ),
    ))
}

fn capture_blocks<'a>(
    selected: BoundSelectedObservation,
    problem: &'a CompiledProblem,
) -> Result<CapturedBlocks<'a>, Box<dyn Error>> {
    let started = Instant::now();
    let (mut source, consumer) = selected.into_block_stream(problem)?;
    let mut blocks = Vec::new();
    let mut logical_bytes = 0_u64;
    let mut read_operations = 0_u64;
    let mut current_bytes = 0_u64;
    let mut capacity_bytes = 0_u64;
    loop {
        let mut block = source.create_storage(blocks.len());
        if source.fill_next(&mut block)?.is_none() {
            break;
        }
        logical_bytes = logical_bytes
            .checked_add(block.logical_bytes())
            .ok_or("byte overflow")?;
        read_operations = read_operations
            .checked_add(block.source_read_operations())
            .ok_or("read-operation overflow")?;
        current_bytes = current_bytes
            .checked_add(block.resident_current_bytes()?)
            .ok_or("current-byte overflow")?;
        capacity_bytes = capacity_bytes
            .checked_add(block.resident_capacity_bytes()?)
            .ok_or("capacity-byte overflow")?;
        blocks.push(block);
        if blocks.len() > CAPTURED_BLOCK_LIMIT
            || usize::try_from(capacity_bytes)? > CAPTURED_RESIDENCY_BYTES
        {
            return Err("captured source residency exceeded its fixed bound".into());
        }
    }
    let terminal = source.complete()?;
    Ok(CapturedBlocks {
        blocks,
        consumer,
        terminal,
        logical_bytes,
        read_operations,
        current_bytes,
        capacity_bytes,
        elapsed: started.elapsed(),
    })
}

fn freeze_density<'a>(
    problem: &'a CompiledProblem,
    plan: &casa_imaging_reconstruction::WeightingPlan,
    blocks: &[SelectedObservationBlock],
    consumer: SelectedObservationBlockConsumer<'a>,
    mut terminal: SelectedObservationTerminal,
    residency_bytes: [u64; 2],
    expected_samples: u64,
) -> Result<
    (
        SelectedObservationGenerationId,
        SelectedObservationReplayProof,
        casa_imaging_reconstruction::WeightingDensityPhase,
    ),
    Box<dyn Error>,
> {
    let density = begin_weighting_generation(problem, plan)?;
    let mut kernel = DensityBlockKernel {
        problem,
        consumer,
        density,
        spectral_contributions: SpectralContributionCache::new(),
    };
    for block in blocks {
        kernel.consume_selected_block(block)?;
    }
    let resolved = kernel.complete()?;
    terminal.record_runtime_residency(blocks.len(), residency_bytes[0], residency_bytes[1])?;
    let (_, completion) = resolved.consumer.complete(terminal)?;
    if completion.sample_count() != expected_samples {
        return Err(format!(
            "selected sample count changed: expected {expected_samples}, got {}",
            completion.sample_count()
        )
        .into());
    }
    let replay_proof = completion
        .replay_proof()
        .ok_or("owner-validated density traversal omitted replay proof")?;
    Ok((completion.generation_id(), replay_proof, resolved.density))
}

fn fresh_consumer<'a>(
    request: &SelectedObservationResolutionRequest,
    problem: &'a CompiledProblem,
) -> Result<SelectedObservationBlockConsumer<'a>, Box<dyn Error>> {
    let resolved = resolve_selected_observation(request.clone())?;
    let (_, access) = resolved.into_parts();
    access.certify_residency(problem)?;
    let selected = access.open(problem)?;
    let (source, consumer) = selected.into_block_stream(problem)?;
    drop(source);
    Ok(consumer)
}

fn fresh_rebound_consumer<'a>(
    request: &SelectedObservationResolutionRequest,
    problem: &'a CompiledProblem,
    proof: &SelectedObservationReplayProof,
) -> Result<SelectedObservationBlockConsumer<'a>, Box<dyn Error>> {
    let resolved = resolve_selected_observation(request.clone())?;
    let (_, access) = resolved.into_parts();
    access.certify_residency(problem)?;
    let selected = access.rebind(problem, proof)?;
    let (source, consumer) = selected.into_block_stream(problem)?;
    drop(source);
    Ok(consumer)
}

fn replay_weighting_kernel<'a, W, F, E>(
    mut kernel: WeightingBlockKernel<'a, W, F>,
    blocks: &[SelectedObservationBlock],
) -> Result<WeightingBlockKernelCompletion<'a, W::Finish>, WeightingBlockKernelError<E>>
where
    W: StreamingWeightPhase + Sync,
    F: FnMut(&ReconstructionWeightedBlock) -> Result<(), E> + Sync,
    E: Error + Send + 'static,
{
    for block in blocks {
        kernel.consume_selected_block(block)?;
    }
    kernel.complete()
}

fn dataset_path() -> Result<PathBuf, Box<dyn Error>> {
    let root = std::env::var_os(DATA_ROOT_ENV).ok_or("CASA_RS_IMPERF_DATA_ROOT is not set")?;
    let path = PathBuf::from(root).join(DATASET_RELATIVE_PATH);
    if !path.is_dir() {
        return Err(format!("medium dataset is missing at {}", path.display()).into());
    }
    Ok(path)
}

fn attempt(byte: u8) -> ModelExecutionAttemptId {
    ModelExecutionAttemptId::new(LogicalIdentity::from_sha256([byte; 32]))
}

fn imaging_frequency_frame(reference: FrequencyRef) -> Result<FrequencyFrame, Box<dyn Error>> {
    match reference {
        FrequencyRef::TOPO => Ok(FrequencyFrame::Topocentric),
        FrequencyRef::BARY => Ok(FrequencyFrame::Barycentric),
        FrequencyRef::LSRK => Ok(FrequencyFrame::Lsrk),
        _ => Err(format!("unsupported source frequency frame {reference}").into()),
    }
}

fn imaging_time_scale(reference: EpochRef) -> Result<TimeScale, Box<dyn Error>> {
    match reference {
        EpochRef::UTC => Ok(TimeScale::Utc),
        EpochRef::TAI => Ok(TimeScale::Tai),
        EpochRef::TT => Ok(TimeScale::Tt),
        EpochRef::TDB => Ok(TimeScale::Tdb),
        _ => Err(format!("unsupported epoch reference {reference}").into()),
    }
}

fn correlation_type(code: i32) -> Result<CorrelationType, Box<dyn Error>> {
    use CorrelationType::*;
    Ok(match code {
        1 => StokesI,
        2 => StokesQ,
        3 => StokesU,
        4 => StokesV,
        5 => CircularRr,
        6 => CircularRl,
        7 => CircularLr,
        8 => CircularLl,
        9 => LinearXx,
        10 => LinearXy,
        11 => LinearYx,
        12 => LinearYy,
        _ => return Err(format!("unsupported fixture correlation code {code}").into()),
    })
}

fn source_revision() -> Result<String, Box<dyn Error>> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()?;
    if !output.status.success() {
        return Err("git rev-parse HEAD failed".into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn sha256_hex(bytes: [u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
