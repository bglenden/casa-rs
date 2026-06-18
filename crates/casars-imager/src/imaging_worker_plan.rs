// SPDX-License-Identifier: LGPL-3.0-or-later

/// The unitless worker-concurrency plan used by imaging runtime planners.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImagingWorkerPlan {
    pub(crate) worker_count: usize,
    pub(crate) modeled_cost_units: u128,
    pub(crate) model: &'static str,
    pub(crate) candidate_costs: String,
    pub(crate) backend_command_target_ms: Option<u64>,
}

/// The parallelism shape being planned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImagingWorkerParallelism {
    /// Workers split one image plane internally.
    IntraPlane,
    /// Workers own independent image planes.
    PlaneParallel,
}

/// The backend family whose scaling curve is being modeled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImagingWorkerBackend {
    Cpu,
    WProjectCpu,
    Metal,
    MetalMultiscaleMinorCycle,
}

/// Shape and backend inputs for the shared imaging worker-count planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ImagingWorkerPlanInput {
    pub(crate) output_planes: usize,
    pub(crate) image_pixels: usize,
    pub(crate) work_iterations_per_plane: usize,
    pub(crate) scale_count: usize,
    pub(crate) max_workers: usize,
    pub(crate) hardware_threads: usize,
    pub(crate) parallelism: ImagingWorkerParallelism,
    pub(crate) backend: ImagingWorkerBackend,
}

const METAL_MULTISCALE_WAVE_TAIL_MILLI: u128 = 200;
const METAL_MULTISCALE_WORKER_OVERHEAD_MILLI: u128 = 100;
const METAL_WAVE_TAIL_MILLI: u128 = 50;
const METAL_WORKER_OVERHEAD_MILLI: u128 = 25;

impl ImagingWorkerPlanInput {
    fn normalized(self) -> Self {
        let hardware_threads = self.hardware_threads.max(1);
        let mut max_workers = self.max_workers.max(1).min(hardware_threads);
        if self.parallelism == ImagingWorkerParallelism::PlaneParallel {
            max_workers = max_workers.min(self.output_planes.max(1));
        }
        Self {
            output_planes: self.output_planes.max(1),
            image_pixels: self.image_pixels.max(1),
            work_iterations_per_plane: self.work_iterations_per_plane.max(1),
            scale_count: self.scale_count.max(1),
            max_workers,
            hardware_threads,
            parallelism: self.parallelism,
            backend: self.backend,
        }
    }
}

/// Pick a worker count by enumerating candidate counts and minimizing a simple
/// modeled wall-time cost. The coefficients are deliberately local to this
/// model so later calibration can replace them without touching every imaging
/// mode's runtime planner.
pub(crate) fn plan_imaging_worker_count(input: ImagingWorkerPlanInput) -> ImagingWorkerPlan {
    let input = input.normalized();
    let mut best = None::<WorkerCandidate>;
    let mut fragments = Vec::new();

    for workers in 1..=input.max_workers {
        let candidate = model_worker_candidate(input, workers);
        if fragments.len() < 16 {
            fragments.push(format!(
                "{}:cost={},waves={},contention_milli={},eff_speedup_milli={},command_target_ms={}",
                workers,
                candidate.cost_units,
                candidate.waves,
                candidate.contention_milli,
                candidate.effective_speedup_milli,
                candidate
                    .backend_command_target_ms
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string())
            ));
        }
        let replace = best.is_none_or(|current| {
            (
                candidate.cost_units,
                candidate.waves,
                usize::MAX.saturating_sub(candidate.workers),
            ) < (
                current.cost_units,
                current.waves,
                usize::MAX.saturating_sub(current.workers),
            )
        });
        if replace {
            best = Some(candidate);
        }
    }

    let best = best.expect("normalized worker planner has at least one candidate");
    ImagingWorkerPlan {
        worker_count: best.workers,
        modeled_cost_units: best.cost_units,
        model: worker_model_label(input.parallelism, input.backend),
        candidate_costs: fragments.join(";"),
        backend_command_target_ms: best.backend_command_target_ms,
    }
}

/// Model the cost contribution for a worker count inside a larger planner that
/// already enumerates memory and I/O candidates.
pub(crate) fn modeled_worker_runtime_cost_units(
    input: ImagingWorkerPlanInput,
    worker_count: usize,
) -> u128 {
    model_worker_candidate(input.normalized(), worker_count.max(1)).cost_units
}

pub(crate) fn planned_backend_command_target_ms(
    input: ImagingWorkerPlanInput,
    worker_count: usize,
) -> Option<u64> {
    model_worker_candidate(input.normalized(), worker_count.max(1)).backend_command_target_ms
}

#[derive(Debug, Clone, Copy)]
struct WorkerCandidate {
    workers: usize,
    waves: usize,
    contention_milli: u128,
    effective_speedup_milli: u128,
    cost_units: u128,
    backend_command_target_ms: Option<u64>,
}

fn model_worker_candidate(input: ImagingWorkerPlanInput, workers: usize) -> WorkerCandidate {
    let workers = workers.max(1).min(input.max_workers);
    let work_units = (input.image_pixels as u128)
        .saturating_mul(input.work_iterations_per_plane as u128)
        .saturating_mul(input.scale_count as u128);
    match input.parallelism {
        ImagingWorkerParallelism::PlaneParallel => {
            let waves = input.output_planes.div_ceil(workers).max(1);
            let contention_milli = plane_parallel_contention_milli(input, workers);
            let total_plane_work_units =
                work_units.saturating_mul(input.output_planes.max(1) as u128);
            let worker_throughput_milli = workers as u128 * 1_000;
            let throughput_cost_units = total_plane_work_units.saturating_mul(contention_milli)
                / worker_throughput_milli.max(1);
            let wave_tail_cost_units = work_units
                .saturating_mul(waves as u128)
                .saturating_mul(plane_parallel_wave_tail_milli(input))
                / 1_000;
            let worker_overhead_cost_units = work_units
                .saturating_mul(workers as u128)
                .saturating_mul(plane_parallel_worker_overhead_milli(input))
                / 1_000;
            let cost_units = throughput_cost_units
                .saturating_add(wave_tail_cost_units)
                .saturating_add(worker_overhead_cost_units);
            WorkerCandidate {
                workers,
                waves,
                contention_milli,
                effective_speedup_milli: worker_throughput_milli.saturating_mul(1_000)
                    / contention_milli.max(1),
                cost_units,
                backend_command_target_ms: backend_command_target_ms(input, workers),
            }
        }
        ImagingWorkerParallelism::IntraPlane => {
            let waves = 1;
            let effective_speedup_milli = intra_plane_effective_speedup_milli(input, workers);
            let cost_units = work_units.saturating_mul(1_000) / effective_speedup_milli.max(1);
            WorkerCandidate {
                workers,
                waves,
                contention_milli: 1_000,
                effective_speedup_milli,
                cost_units,
                backend_command_target_ms: backend_command_target_ms(input, workers),
            }
        }
    }
}

fn backend_command_target_ms(input: ImagingWorkerPlanInput, workers: usize) -> Option<u64> {
    match input.backend {
        ImagingWorkerBackend::MetalMultiscaleMinorCycle => Some(
            metal_multiscale_minor_cycle_command_target_ms(input, workers),
        ),
        ImagingWorkerBackend::Cpu
        | ImagingWorkerBackend::WProjectCpu
        | ImagingWorkerBackend::Metal => None,
    }
}

fn metal_multiscale_minor_cycle_command_target_ms(
    input: ImagingWorkerPlanInput,
    workers: usize,
) -> u64 {
    const BASE_TARGET_MS: u64 = 2_000;
    let _ = (input, workers);
    BASE_TARGET_MS
}

fn plane_parallel_contention_milli(input: ImagingWorkerPlanInput, workers: usize) -> u128 {
    match input.backend {
        ImagingWorkerBackend::MetalMultiscaleMinorCycle => {
            let extra = workers.saturating_sub(1) as u128;
            let plane_amortization = input.output_planes;
            let soft_queue_depth =
                (((input.hardware_threads.max(1) * 3) / 2) + plane_amortization).max(1) as u128;
            1_000 + extra.saturating_mul(extra).saturating_mul(1_000) / soft_queue_depth
        }
        ImagingWorkerBackend::Metal => {
            let extra = workers.saturating_sub(1) as u128;
            let plane_amortization = input.output_planes.div_ceil(2);
            let soft_queue_depth =
                ((input.hardware_threads.max(1) * 4) + plane_amortization).max(1) as u128;
            1_000 + extra.saturating_mul(extra).saturating_mul(1_000) / soft_queue_depth
        }
        ImagingWorkerBackend::Cpu | ImagingWorkerBackend::WProjectCpu => {
            let extra = workers.saturating_sub(1) as u128;
            1_000 + extra.saturating_mul(10)
        }
    }
}

fn plane_parallel_wave_tail_milli(input: ImagingWorkerPlanInput) -> u128 {
    match input.backend {
        ImagingWorkerBackend::MetalMultiscaleMinorCycle => METAL_MULTISCALE_WAVE_TAIL_MILLI,
        ImagingWorkerBackend::Metal => METAL_WAVE_TAIL_MILLI,
        ImagingWorkerBackend::Cpu | ImagingWorkerBackend::WProjectCpu => 0,
    }
}

fn plane_parallel_worker_overhead_milli(input: ImagingWorkerPlanInput) -> u128 {
    match input.backend {
        ImagingWorkerBackend::MetalMultiscaleMinorCycle => METAL_MULTISCALE_WORKER_OVERHEAD_MILLI,
        ImagingWorkerBackend::Metal => METAL_WORKER_OVERHEAD_MILLI,
        ImagingWorkerBackend::Cpu | ImagingWorkerBackend::WProjectCpu => 0,
    }
}

fn intra_plane_effective_speedup_milli(input: ImagingWorkerPlanInput, workers: usize) -> u128 {
    let workers_milli = workers.max(1) as u128 * 1_000;
    let extra = workers.saturating_sub(1) as u128;
    let penalty_milli = match input.backend {
        ImagingWorkerBackend::WProjectCpu => 1_000 + extra.saturating_mul(extra).saturating_mul(20),
        ImagingWorkerBackend::Cpu => 1_000 + extra.saturating_mul(extra).saturating_mul(80),
        ImagingWorkerBackend::Metal | ImagingWorkerBackend::MetalMultiscaleMinorCycle => {
            1_000 + extra.saturating_mul(extra).saturating_mul(120)
        }
    };
    workers_milli.saturating_mul(1_000) / penalty_milli.max(1)
}

fn worker_model_label(
    parallelism: ImagingWorkerParallelism,
    backend: ImagingWorkerBackend,
) -> &'static str {
    match (parallelism, backend) {
        (ImagingWorkerParallelism::IntraPlane, ImagingWorkerBackend::Cpu) => {
            "intra_plane_cpu_diminishing_return_v1"
        }
        (ImagingWorkerParallelism::IntraPlane, ImagingWorkerBackend::WProjectCpu) => {
            "intra_plane_wproject_cpu_diminishing_return_v1"
        }
        (ImagingWorkerParallelism::IntraPlane, ImagingWorkerBackend::Metal) => {
            "intra_plane_metal_diminishing_return_v1"
        }
        (ImagingWorkerParallelism::IntraPlane, ImagingWorkerBackend::MetalMultiscaleMinorCycle) => {
            "intra_plane_metal_multiscale_diminishing_return_v1"
        }
        (ImagingWorkerParallelism::PlaneParallel, ImagingWorkerBackend::Cpu) => {
            "plane_parallel_cpu_throughput_v2"
        }
        (ImagingWorkerParallelism::PlaneParallel, ImagingWorkerBackend::WProjectCpu) => {
            "plane_parallel_wproject_cpu_throughput_v2"
        }
        (ImagingWorkerParallelism::PlaneParallel, ImagingWorkerBackend::Metal) => {
            "plane_parallel_metal_throughput_v2"
        }
        (
            ImagingWorkerParallelism::PlaneParallel,
            ImagingWorkerBackend::MetalMultiscaleMinorCycle,
        ) => "plane_parallel_metal_multiscale_throughput_v2",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metal_multiscale_plane_parallel_selects_from_cost_curve() {
        let plan = plan_imaging_worker_count(ImagingWorkerPlanInput {
            output_planes: 8,
            image_pixels: 2048 * 2048,
            work_iterations_per_plane: 10_000,
            scale_count: 3,
            max_workers: 10,
            hardware_threads: 10,
            parallelism: ImagingWorkerParallelism::PlaneParallel,
            backend: ImagingWorkerBackend::MetalMultiscaleMinorCycle,
        });

        assert_eq!(plan.worker_count, 4, "{plan:?}");
        assert!(plan.candidate_costs.contains("8:cost="));
        assert_eq!(plan.backend_command_target_ms, Some(2_000));
    }

    #[test]
    fn metal_multiscale_many_planes_prefers_more_workers() {
        let plan = plan_imaging_worker_count(ImagingWorkerPlanInput {
            output_planes: 64,
            image_pixels: 1024 * 1024,
            work_iterations_per_plane: 10_000,
            scale_count: 3,
            max_workers: 10,
            hardware_threads: 10,
            parallelism: ImagingWorkerParallelism::PlaneParallel,
            backend: ImagingWorkerBackend::MetalMultiscaleMinorCycle,
        });

        assert_eq!(plan.worker_count, 10, "{plan:?}");
        assert_eq!(plan.model, "plane_parallel_metal_multiscale_throughput_v2");
        assert_eq!(plan.backend_command_target_ms, Some(2_000));
    }

    #[test]
    fn cpu_plane_parallel_prefers_more_planes_when_memory_allows() {
        let plan = plan_imaging_worker_count(ImagingWorkerPlanInput {
            output_planes: 64,
            image_pixels: 2048 * 2048,
            work_iterations_per_plane: 1,
            scale_count: 1,
            max_workers: 10,
            hardware_threads: 10,
            parallelism: ImagingWorkerParallelism::PlaneParallel,
            backend: ImagingWorkerBackend::Cpu,
        });

        assert_eq!(plan.worker_count, 10, "{plan:?}");
        assert_eq!(plan.backend_command_target_ms, None);
    }

    #[test]
    fn cpu_intra_plane_limit_is_modeled_not_capped() {
        let plan = plan_imaging_worker_count(ImagingWorkerPlanInput {
            output_planes: 1,
            image_pixels: 2048 * 2048,
            work_iterations_per_plane: 1,
            scale_count: 1,
            max_workers: 10,
            hardware_threads: 10,
            parallelism: ImagingWorkerParallelism::IntraPlane,
            backend: ImagingWorkerBackend::Cpu,
        });

        assert_eq!(plan.worker_count, 4, "{plan:?}");
        assert!(plan.candidate_costs.contains("10:cost="));
    }
}
