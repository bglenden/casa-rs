// SPDX-License-Identifier: LGPL-3.0-or-later

use crate::types::{Deconvolver, WTermMode, WeightingMode};

/// Spectral shape for a single-plane imaging execution plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinglePlaneSpectralPlan {
    /// Multi-frequency synthesis.
    Mfs,
    /// Cube-like request that resolves to one output channel.
    CubeLikeOneChannel,
    /// Cube-like request with multiple output channels.
    CubeLikeMultiChannel,
}

impl SinglePlaneSpectralPlan {
    /// Stable log label.
    pub fn label(self) -> &'static str {
        match self {
            Self::Mfs => "mfs",
            Self::CubeLikeOneChannel => "cube-like-one-channel",
            Self::CubeLikeMultiChannel => "cube-like-multi-channel",
        }
    }

    /// Returns true when the plan has one output channel.
    pub fn is_one_output_channel(self) -> bool {
        matches!(self, Self::Mfs | Self::CubeLikeOneChannel)
    }
}

/// Projection family for a single-plane execution plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinglePlaneProjectionPlan {
    /// Standard gridder.
    Standard,
    /// Standard gridder unless the app-level request infers mosaic handling.
    StandardOrMosaicInferred,
    /// W-projection or direct W-term single-plane handling.
    WProjection,
    /// Mosaic/screen-projection handling.
    Mosaic,
    /// Combined A+W projection backed by a validated convolution-function cache.
    AwProject,
}

impl SinglePlaneProjectionPlan {
    /// Stable log label.
    pub fn label(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::StandardOrMosaicInferred => "standard-or-mosaic-inferred",
            Self::WProjection => "wproject",
            Self::Mosaic => "mosaic",
            Self::AwProject => "awproject",
        }
    }
}

/// Primary-beam responsibility in a single-plane plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinglePlanePrimaryBeamRequirement {
    /// No primary-beam product or projection work.
    None,
    /// Single-field primary-beam products are requested.
    SingleFieldProducts,
    /// W-projection primary-beam products are requested.
    WProjectionProducts,
    /// Mosaic projection requires primary-beam handling.
    MosaicProjection,
    /// AWProject requires convolution-function weight/PB handling.
    AwProjection,
}

impl SinglePlanePrimaryBeamRequirement {
    /// Stable log label.
    pub fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::SingleFieldProducts => "single-field-products",
            Self::WProjectionProducts => "wprojection-products",
            Self::MosaicProjection => "mosaic-projection",
            Self::AwProjection => "awprojection",
        }
    }
}

/// Deconvolver family used by a single-plane plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinglePlaneDeconvolverPlan {
    /// Single-term clean family such as Hogbom or Clark.
    SingleTerm,
    /// Multiscale single-term clean.
    Multiscale,
    /// Multi-term MFS clean.
    Mtmfs,
}

impl SinglePlaneDeconvolverPlan {
    /// Convert a core deconvolver enum to its planning family.
    pub fn from_deconvolver(deconvolver: Deconvolver) -> Self {
        match deconvolver {
            Deconvolver::Mtmfs => Self::Mtmfs,
            Deconvolver::Multiscale => Self::Multiscale,
            Deconvolver::Hogbom | Deconvolver::Clark => Self::SingleTerm,
        }
    }

    /// Stable log label.
    pub fn label(self) -> &'static str {
        match self {
            Self::SingleTerm => "single-term",
            Self::Multiscale => "multiscale",
            Self::Mtmfs => "mtmfs",
        }
    }
}

/// App-selected acceleration policy used by the shared single-plane planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinglePlaneAccelerationPolicy {
    /// Force scalar CPU behavior.
    Cpu,
    /// Let the planner choose among available backends.
    Auto,
    /// Force multi-worker CPU behavior where eligible.
    MultiCpu,
    /// Prefer Metal where eligible and available.
    Metal,
}

/// Cube interpolation class relevant to single-plane acceleration eligibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinglePlaneCubeInterpolation {
    /// Nearest-neighbor interpolation.
    Nearest,
    /// Any interpolation that requires the cube-specific path for cleaned cubes.
    Other,
}

/// Backend eligibility and diagnostic reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendEligibility {
    /// Whether the backend can execute this plan.
    pub eligible: bool,
    /// Stable reason or selected strategy label.
    pub reason: String,
}

impl BackendEligibility {
    fn eligible(reason: impl Into<String>) -> Self {
        Self {
            eligible: true,
            reason: reason.into(),
        }
    }

    fn ineligible(reason: impl Into<String>) -> Self {
        Self {
            eligible: false,
            reason: reason.into(),
        }
    }
}

/// Public input contract for building a single-plane execution plan.
#[derive(Debug, Clone, PartialEq)]
pub struct SinglePlaneExecutionPlanInput {
    /// Spectral shape.
    pub(crate) spectral: SinglePlaneSpectralPlan,
    /// Projection family.
    pub(crate) projection: SinglePlaneProjectionPlan,
    /// Deconvolver family.
    pub(crate) deconvolver: SinglePlaneDeconvolverPlan,
    /// Weighting mode.
    pub(crate) weighting: WeightingMode,
    /// Number of output channels requested by the app.
    pub(crate) output_channel_count: usize,
    /// Whether single-field primary-beam products are requested.
    pub(crate) primary_beam_products: bool,
    /// Whether `.pb` products should be written.
    pub(crate) write_pb: bool,
    /// Whether `.image.pbcor` products should be written.
    pub(crate) pbcor: bool,
    /// Whether a negative mosaic PB limit requires PB products for compatibility.
    pub(crate) mosaic_pb_limit_negative: bool,
    /// MT-MFS Taylor term count.
    pub(crate) nterms: usize,
    /// Whether the standard-MFS shared strategy is eligible.
    pub(crate) standard_mfs_eligible: bool,
    /// Whether the mosaic-MFS shared strategy is eligible.
    pub(crate) mosaic_mfs_eligible: bool,
    /// App-selected acceleration policy.
    pub(crate) acceleration: SinglePlaneAccelerationPolicy,
    /// Optional app-level grid thread override.
    pub(crate) grid_threads_override: Option<usize>,
    /// Auto-selected standard-MFS CPU worker count.
    pub(crate) standard_auto_grid_threads: usize,
    /// Auto-selected W-projection CPU worker count.
    pub(crate) wproject_auto_grid_threads: usize,
    /// Whether the Metal backend is available on this host.
    pub(crate) metal_device_available: bool,
    /// Whether the request uses a cube-like spectral mode.
    pub(crate) cube_like: bool,
    /// Cube interpolation class.
    pub(crate) cube_interpolation: SinglePlaneCubeInterpolation,
    /// Whether the request is dirty-only or has zero clean iterations.
    pub(crate) dirty_or_zero_niter: bool,
    /// Whether pointing-table based projection is requested.
    pub(crate) use_pointing: bool,
    /// Whether the clean mask mode is the simple user-mask mode.
    pub(crate) user_mask_only: bool,
    /// W-term mode requested by the app.
    pub(crate) w_term_mode: WTermMode,
}

impl SinglePlaneExecutionPlanInput {
    /// Create a single-plane execution-plan input from app-normalized facts.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        spectral: SinglePlaneSpectralPlan,
        projection: SinglePlaneProjectionPlan,
        deconvolver: SinglePlaneDeconvolverPlan,
        weighting: WeightingMode,
        output_channel_count: usize,
        primary_beam_products: bool,
        write_pb: bool,
        pbcor: bool,
        mosaic_pb_limit_negative: bool,
        nterms: usize,
        standard_mfs_eligible: bool,
        mosaic_mfs_eligible: bool,
        acceleration: SinglePlaneAccelerationPolicy,
        grid_threads_override: Option<usize>,
        standard_auto_grid_threads: usize,
        wproject_auto_grid_threads: usize,
        metal_device_available: bool,
        cube_like: bool,
        cube_interpolation: SinglePlaneCubeInterpolation,
        dirty_or_zero_niter: bool,
        use_pointing: bool,
        user_mask_only: bool,
        w_term_mode: WTermMode,
    ) -> Self {
        Self {
            spectral,
            projection,
            deconvolver,
            weighting,
            output_channel_count,
            primary_beam_products,
            write_pb,
            pbcor,
            mosaic_pb_limit_negative,
            nterms,
            standard_mfs_eligible,
            mosaic_mfs_eligible,
            acceleration,
            grid_threads_override,
            standard_auto_grid_threads,
            wproject_auto_grid_threads,
            metal_device_available,
            cube_like,
            cube_interpolation,
            dirty_or_zero_niter,
            use_pointing,
            user_mask_only,
            w_term_mode,
        }
    }
}

/// Single-plane execution, product, projection, and backend-capability plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinglePlaneExecutionPlan {
    /// Spectral shape.
    pub spectral: SinglePlaneSpectralPlan,
    /// Projection family.
    pub projection: SinglePlaneProjectionPlan,
    /// Deconvolver family.
    pub deconvolver: SinglePlaneDeconvolverPlan,
    /// Stable weighting label.
    pub weighting: &'static str,
    /// Number of output channels.
    pub output_channel_count: usize,
    /// Whether primary-beam products are requested.
    pub primary_beam_products: bool,
    /// Primary-beam responsibility.
    pub primary_beam_requirement: SinglePlanePrimaryBeamRequirement,
    /// Product suffixes expected from this plan.
    pub output_products: Vec<String>,
    /// Multi-worker CPU capability.
    pub cpu_multi_worker: BackendEligibility,
    /// Metal GPU capability.
    pub gpu_metal: BackendEligibility,
    /// Stable timing-attribution label.
    pub stage_timing_attribution: &'static str,
    /// Whether this plan should keep the standard-MFS regression sentinel active.
    pub standard_mfs_regression_sentinel: bool,
}

impl SinglePlaneExecutionPlan {
    /// Stable diagnostic line for frontend logs.
    pub fn log_line(&self) -> String {
        format!(
            "single_plane_execution_plan spectral={} projection={} deconvolver={} weighting={} output_channels={} one_output_channel={} source_stream=bounded source_stream_memory=planner pb_products={} pb_requirement={} output_products={} cpu_multi_worker_eligible={} cpu_multi_worker_reason={} gpu_metal_eligible={} gpu_metal_reason={} stage_timing_attribution={} standard_mfs_regression_sentinel={}",
            self.spectral.label(),
            self.projection.label(),
            self.deconvolver.label(),
            self.weighting,
            self.output_channel_count,
            self.spectral.is_one_output_channel(),
            self.primary_beam_products,
            self.primary_beam_requirement.label(),
            self.output_products.join(","),
            self.cpu_multi_worker.eligible,
            self.cpu_multi_worker.reason,
            self.gpu_metal.eligible,
            self.gpu_metal.reason,
            self.stage_timing_attribution,
            self.standard_mfs_regression_sentinel,
        )
    }
}

/// Build the library-owned single-plane execution plan.
pub fn build_single_plane_execution_plan(
    input: SinglePlaneExecutionPlanInput,
) -> SinglePlaneExecutionPlan {
    let primary_beam_requirement = primary_beam_requirement(
        input.projection,
        input.primary_beam_products,
        input.use_pointing,
    );
    let output_products = output_products(&input, primary_beam_requirement);
    let one_output_channel = input.spectral.is_one_output_channel();
    let standard_mfs_regression_sentinel = matches!(input.spectral, SinglePlaneSpectralPlan::Mfs)
        && matches!(
            input.projection,
            SinglePlaneProjectionPlan::Standard
                | SinglePlaneProjectionPlan::StandardOrMosaicInferred
        )
        && matches!(
            input.deconvolver,
            SinglePlaneDeconvolverPlan::SingleTerm
                | SinglePlaneDeconvolverPlan::Multiscale
                | SinglePlaneDeconvolverPlan::Mtmfs
        );

    SinglePlaneExecutionPlan {
        spectral: input.spectral,
        projection: input.projection,
        deconvolver: input.deconvolver,
        weighting: weighting_label(&input.weighting),
        output_channel_count: input.output_channel_count,
        primary_beam_products: input.primary_beam_products,
        primary_beam_requirement,
        output_products,
        cpu_multi_worker: cpu_multi_worker_eligibility(&input, one_output_channel),
        gpu_metal: gpu_metal_eligibility(&input, one_output_channel),
        stage_timing_attribution: "frontend-core-product-stages",
        standard_mfs_regression_sentinel,
    }
}

fn primary_beam_requirement(
    projection: SinglePlaneProjectionPlan,
    primary_beam_products: bool,
    use_pointing: bool,
) -> SinglePlanePrimaryBeamRequirement {
    if matches!(projection, SinglePlaneProjectionPlan::AwProject) {
        SinglePlanePrimaryBeamRequirement::AwProjection
    } else if matches!(projection, SinglePlaneProjectionPlan::Mosaic) {
        SinglePlanePrimaryBeamRequirement::MosaicProjection
    } else if primary_beam_products && matches!(projection, SinglePlaneProjectionPlan::WProjection)
    {
        SinglePlanePrimaryBeamRequirement::WProjectionProducts
    } else if primary_beam_products {
        SinglePlanePrimaryBeamRequirement::SingleFieldProducts
    } else if use_pointing {
        SinglePlanePrimaryBeamRequirement::MosaicProjection
    } else {
        SinglePlanePrimaryBeamRequirement::None
    }
}

fn output_products(
    input: &SinglePlaneExecutionPlanInput,
    primary_beam_requirement: SinglePlanePrimaryBeamRequirement,
) -> Vec<String> {
    let mut products = Vec::new();
    if input.deconvolver == SinglePlaneDeconvolverPlan::Mtmfs {
        let nterms = input.nterms.max(1);
        for term in 0..nterms {
            products.push(format!(".image.tt{term}"));
            products.push(format!(".residual.tt{term}"));
            products.push(format!(".model.tt{term}"));
        }
        for term in 0..(2 * nterms - 1) {
            products.push(format!(".psf.tt{term}"));
            products.push(format!(".sumwt.tt{term}"));
            if matches!(
                primary_beam_requirement,
                SinglePlanePrimaryBeamRequirement::MosaicProjection
                    | SinglePlanePrimaryBeamRequirement::AwProjection
            ) {
                products.push(format!(".weight.tt{term}"));
            }
        }
        if nterms > 1 {
            products.push(".alpha".to_string());
            products.push(".alpha.error".to_string());
        }
        if input.write_pb || input.pbcor || input.mosaic_pb_limit_negative {
            products.push(".pb.tt0".to_string());
        }
        if input.pbcor {
            for term in 0..nterms {
                products.push(format!(".image.tt{term}.pbcor"));
            }
            if nterms > 1
                && !matches!(
                    primary_beam_requirement,
                    SinglePlanePrimaryBeamRequirement::MosaicProjection
                        | SinglePlanePrimaryBeamRequirement::AwProjection
                )
            {
                products.push(".alpha.pbcor".to_string());
            }
        }
        return products;
    }

    for product in [".image", ".residual", ".model", ".psf", ".sumwt"] {
        products.push(product.to_string());
    }
    if matches!(
        primary_beam_requirement,
        SinglePlanePrimaryBeamRequirement::MosaicProjection
            | SinglePlanePrimaryBeamRequirement::AwProjection
    ) {
        products.push(".weight".to_string());
    }
    if input.write_pb || input.pbcor || input.mosaic_pb_limit_negative {
        products.push(".pb".to_string());
    }
    if input.pbcor {
        products.push(".image.pbcor".to_string());
    }
    products
}

fn weighting_label(weighting: &WeightingMode) -> &'static str {
    match weighting {
        WeightingMode::Natural => "natural",
        WeightingMode::Uniform => "uniform",
        WeightingMode::Briggs { .. } => "briggs",
        WeightingMode::BriggsBwTaper { .. } => "briggsbwtaper",
    }
}

fn cpu_multi_worker_eligibility(
    input: &SinglePlaneExecutionPlanInput,
    one_output_channel: bool,
) -> BackendEligibility {
    if !one_output_channel {
        return BackendEligibility::ineligible("not-one-output-channel");
    }
    if input.projection == SinglePlaneProjectionPlan::AwProject {
        let workers = match input.acceleration {
            SinglePlaneAccelerationPolicy::Cpu => 1,
            SinglePlaneAccelerationPolicy::Auto
            | SinglePlaneAccelerationPolicy::MultiCpu
            | SinglePlaneAccelerationPolicy::Metal => input
                .grid_threads_override
                .unwrap_or(input.standard_auto_grid_threads),
        };
        return if workers > 1 {
            BackendEligibility::eligible(format!(
                "awproject-disjoint-taylor-plane-workers-{workers}"
            ))
        } else {
            BackendEligibility::ineligible("awproject-resolved-one-plane-worker")
        };
    }
    if input.standard_mfs_eligible || input.mosaic_mfs_eligible {
        let workers = match input.acceleration {
            SinglePlaneAccelerationPolicy::Cpu => 1,
            SinglePlaneAccelerationPolicy::Auto
            | SinglePlaneAccelerationPolicy::MultiCpu
            | SinglePlaneAccelerationPolicy::Metal => {
                if let Some(override_workers) = input.grid_threads_override {
                    override_workers
                } else if matches!(input.w_term_mode, WTermMode::WProject) {
                    input.wproject_auto_grid_threads
                } else {
                    input.standard_auto_grid_threads
                }
            }
        };
        if workers > 1 {
            if input.mosaic_mfs_eligible {
                if input.cube_like && input.output_channel_count == 1 {
                    BackendEligibility::eligible(format!(
                        "mosaic-cube-like-one-channel-parallel-prepare-workers-{workers}-single-grid-owner"
                    ))
                } else if input.acceleration == SinglePlaneAccelerationPolicy::MultiCpu {
                    BackendEligibility::eligible(format!(
                        "mosaic-sample-range-workers-{workers}-diagnostic"
                    ))
                } else {
                    BackendEligibility::ineligible(
                        "mosaic-auto-uses-single-grid-owner-or-metal-groups",
                    )
                }
            } else if matches!(input.w_term_mode, WTermMode::WProject) {
                BackendEligibility::eligible(format!("wproject-streaming-workers-{workers}"))
            } else if input.cube_like {
                BackendEligibility::eligible(format!(
                    "standard-cube-like-one-channel-parallel-prepare-workers-{workers}"
                ))
            } else {
                BackendEligibility::eligible(format!("standard-mfs-fixed-tile-workers-{workers}"))
            }
        } else {
            BackendEligibility::ineligible("standard-mfs-policy-selected-single-worker")
        }
    } else {
        BackendEligibility::ineligible(shared_strategy_gap_reason(input))
    }
}

fn gpu_metal_eligibility(
    input: &SinglePlaneExecutionPlanInput,
    one_output_channel: bool,
) -> BackendEligibility {
    if !one_output_channel {
        return BackendEligibility::ineligible("not-one-output-channel");
    }
    if input.projection == SinglePlaneProjectionPlan::AwProject {
        if matches!(
            input.acceleration,
            SinglePlaneAccelerationPolicy::Cpu | SinglePlaneAccelerationPolicy::MultiCpu
        ) {
            return BackendEligibility::ineligible("awproject-policy-disabled-metal");
        }
        return if input.metal_device_available {
            BackendEligibility::eligible("awproject-metal-cf-kernel")
        } else {
            BackendEligibility::ineligible("metal-device-unavailable")
        };
    }
    if !(input.standard_mfs_eligible || input.mosaic_mfs_eligible) {
        return BackendEligibility::ineligible(shared_strategy_gap_reason(input));
    }
    if matches!(
        input.acceleration,
        SinglePlaneAccelerationPolicy::Cpu | SinglePlaneAccelerationPolicy::MultiCpu
    ) {
        return BackendEligibility::ineligible("standard-mfs-policy-disabled-metal");
    }
    if input.metal_device_available {
        if input.mosaic_mfs_eligible {
            BackendEligibility::eligible("mosaic-screen-projector-metal-single-grid-owner")
        } else if matches!(input.w_term_mode, WTermMode::WProject) {
            BackendEligibility::eligible("wproject-metal-kernel")
        } else if input.cube_like {
            BackendEligibility::eligible("standard-cube-like-one-channel-grouped-metal")
        } else if input.deconvolver == SinglePlaneDeconvolverPlan::Mtmfs {
            BackendEligibility::eligible("mtmfs-metal-sample-cache")
        } else {
            BackendEligibility::eligible("standard-mfs-grouped-metal")
        }
    } else {
        BackendEligibility::ineligible("metal-device-unavailable")
    }
}

fn shared_strategy_gap_reason(input: &SinglePlaneExecutionPlanInput) -> String {
    if input.cube_like && input.output_channel_count != 1 {
        return "not-one-output-channel".to_string();
    }
    if input.cube_like && !matches!(input.w_term_mode, WTermMode::None) {
        return "cube-like-wterm-requires-cube-path".to_string();
    }
    if input.cube_like
        && input.cube_interpolation != SinglePlaneCubeInterpolation::Nearest
        && !input.dirty_or_zero_niter
    {
        return "cleaned-linear-cube-like-requires-cube-path".to_string();
    }
    if input.use_pointing || matches!(input.w_term_mode, WTermMode::Direct) {
        return "shared-strategy-not-yet-adapted-to-projection-family".to_string();
    }
    if !input.user_mask_only {
        return "automask-not-supported-by-shared-strategy".to_string();
    }
    "standard-mfs-eligibility-check-rejected".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_input() -> SinglePlaneExecutionPlanInput {
        SinglePlaneExecutionPlanInput {
            spectral: SinglePlaneSpectralPlan::Mfs,
            projection: SinglePlaneProjectionPlan::Standard,
            deconvolver: SinglePlaneDeconvolverPlan::SingleTerm,
            weighting: WeightingMode::Briggs { robust: 0.5 },
            output_channel_count: 1,
            primary_beam_products: false,
            write_pb: false,
            pbcor: false,
            mosaic_pb_limit_negative: false,
            nterms: 1,
            standard_mfs_eligible: true,
            mosaic_mfs_eligible: false,
            acceleration: SinglePlaneAccelerationPolicy::Auto,
            grid_threads_override: Some(4),
            standard_auto_grid_threads: 4,
            wproject_auto_grid_threads: 2,
            metal_device_available: false,
            cube_like: false,
            cube_interpolation: SinglePlaneCubeInterpolation::Nearest,
            dirty_or_zero_niter: false,
            use_pointing: false,
            user_mask_only: true,
            w_term_mode: WTermMode::None,
        }
    }

    #[test]
    fn standard_plan_reports_products_and_capabilities() {
        let plan = build_single_plane_execution_plan(base_input());

        assert_eq!(plan.projection, SinglePlaneProjectionPlan::Standard);
        assert_eq!(
            plan.output_products,
            vec![".image", ".residual", ".model", ".psf", ".sumwt"]
        );
        assert_eq!(
            plan.cpu_multi_worker.reason,
            "standard-mfs-fixed-tile-workers-4"
        );
        assert_eq!(plan.gpu_metal.reason, "metal-device-unavailable");
        assert!(plan.standard_mfs_regression_sentinel);
    }

    #[test]
    fn mtmfs_product_set_includes_taylor_and_pbcor_products() {
        let mut input = base_input();
        input.deconvolver = SinglePlaneDeconvolverPlan::Mtmfs;
        input.nterms = 2;
        input.write_pb = true;
        input.pbcor = true;

        let plan = build_single_plane_execution_plan(input);

        assert_eq!(
            plan.output_products,
            vec![
                ".image.tt0",
                ".residual.tt0",
                ".model.tt0",
                ".image.tt1",
                ".residual.tt1",
                ".model.tt1",
                ".psf.tt0",
                ".sumwt.tt0",
                ".psf.tt1",
                ".sumwt.tt1",
                ".psf.tt2",
                ".sumwt.tt2",
                ".alpha",
                ".alpha.error",
                ".pb.tt0",
                ".image.tt0.pbcor",
                ".image.tt1.pbcor",
                ".alpha.pbcor",
            ]
        );
    }

    #[test]
    fn mosaic_mtmfs_plan_reports_complete_casa_taylor_topology() {
        let mut input = base_input();
        input.projection = SinglePlaneProjectionPlan::Mosaic;
        input.deconvolver = SinglePlaneDeconvolverPlan::Mtmfs;
        input.nterms = 2;
        input.write_pb = true;
        input.pbcor = true;

        let plan = build_single_plane_execution_plan(input);

        assert_eq!(
            plan.output_products,
            vec![
                ".image.tt0",
                ".residual.tt0",
                ".model.tt0",
                ".image.tt1",
                ".residual.tt1",
                ".model.tt1",
                ".psf.tt0",
                ".sumwt.tt0",
                ".weight.tt0",
                ".psf.tt1",
                ".sumwt.tt1",
                ".weight.tt1",
                ".psf.tt2",
                ".sumwt.tt2",
                ".weight.tt2",
                ".alpha",
                ".alpha.error",
                ".pb.tt0",
                ".image.tt0.pbcor",
                ".image.tt1.pbcor",
            ]
        );
    }

    #[test]
    fn unsupported_cube_like_multi_channel_reports_gap_reason() {
        let mut input = base_input();
        input.spectral = SinglePlaneSpectralPlan::CubeLikeMultiChannel;
        input.output_channel_count = 4;
        input.cube_like = true;
        input.standard_mfs_eligible = false;

        let plan = build_single_plane_execution_plan(input);

        assert_eq!(plan.cpu_multi_worker.reason, "not-one-output-channel");
        assert_eq!(plan.gpu_metal.reason, "not-one-output-channel");
    }
}
