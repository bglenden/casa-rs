// SPDX-License-Identifier: LGPL-3.0-or-later
#![allow(missing_docs)]

use casa_imaging::{Deconvolver, WTermMode, WeightingMode};
use casa_ms::CubeInterpolation;

use crate::{
    CleanMaskMode, CliConfig, SaveModelMode, SpectralMode, StandardMfsAccelerationPolicy,
    can_plan_mosaic_mfs_acceleration, can_plan_standard_mfs_acceleration,
    needs_single_field_primary_beam_products, standard_mfs_auto_grid_threads,
    standard_mfs_wproject_auto_grid_threads,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SinglePlaneSpectralPlan {
    Mfs,
    CubeLikeOneChannel,
    CubeLikeMultiChannel,
}

impl SinglePlaneSpectralPlan {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Mfs => "mfs",
            Self::CubeLikeOneChannel => "cube-like-one-channel",
            Self::CubeLikeMultiChannel => "cube-like-multi-channel",
        }
    }

    pub(crate) fn is_one_output_channel(self) -> bool {
        matches!(self, Self::Mfs | Self::CubeLikeOneChannel)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SinglePlaneProjectionPlan {
    Standard,
    StandardOrMosaicInferred,
    WProjection,
    Mosaic,
}

impl SinglePlaneProjectionPlan {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::StandardOrMosaicInferred => "standard-or-mosaic-inferred",
            Self::WProjection => "wproject",
            Self::Mosaic => "mosaic",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SinglePlanePrimaryBeamRequirement {
    None,
    SingleFieldProducts,
    WProjectionProducts,
    MosaicProjection,
}

impl SinglePlanePrimaryBeamRequirement {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::SingleFieldProducts => "single-field-products",
            Self::WProjectionProducts => "wprojection-products",
            Self::MosaicProjection => "mosaic-projection",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SinglePlaneDeconvolverPlan {
    SingleTerm,
    Multiscale,
    Mtmfs,
}

impl SinglePlaneDeconvolverPlan {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::SingleTerm => "single-term",
            Self::Multiscale => "multiscale",
            Self::Mtmfs => "mtmfs",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackendEligibility {
    pub(crate) eligible: bool,
    pub(crate) reason: String,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SinglePlaneExecutionPlan {
    pub(crate) spectral: SinglePlaneSpectralPlan,
    pub(crate) projection: SinglePlaneProjectionPlan,
    pub(crate) deconvolver: SinglePlaneDeconvolverPlan,
    pub(crate) weighting: &'static str,
    pub(crate) output_channel_count: usize,
    pub(crate) primary_beam_products: bool,
    pub(crate) primary_beam_requirement: SinglePlanePrimaryBeamRequirement,
    pub(crate) output_products: Vec<String>,
    pub(crate) cpu_multi_worker: BackendEligibility,
    pub(crate) gpu_metal: BackendEligibility,
    pub(crate) stage_timing_attribution: &'static str,
    pub(crate) standard_mfs_regression_sentinel: bool,
}

impl SinglePlaneExecutionPlan {
    pub(crate) fn log_line(&self) -> String {
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

pub(crate) fn build_single_plane_execution_plan(
    config: &CliConfig,
    force_standard_gridder: bool,
    ms_count: usize,
) -> SinglePlaneExecutionPlan {
    let output_channel_count = output_channel_count(config);
    let spectral = spectral_plan(config.spectral_mode, output_channel_count);
    let projection = projection_plan(config, force_standard_gridder);
    let deconvolver = deconvolver_plan(config.deconvolver);
    let primary_beam_products = needs_single_field_primary_beam_products(config);
    let primary_beam_requirement =
        primary_beam_requirement(config, projection, primary_beam_products);
    let output_products = output_products(config, primary_beam_requirement);
    let standard_mfs_eligible =
        can_plan_standard_mfs_acceleration(config, force_standard_gridder, ms_count);
    let mosaic_mfs_eligible = can_plan_mosaic_mfs_acceleration(config, ms_count);
    let one_output_channel = spectral.is_one_output_channel();
    let standard_mfs_regression_sentinel = matches!(spectral, SinglePlaneSpectralPlan::Mfs)
        && matches!(
            projection,
            SinglePlaneProjectionPlan::Standard
                | SinglePlaneProjectionPlan::StandardOrMosaicInferred
        )
        && matches!(
            deconvolver,
            SinglePlaneDeconvolverPlan::SingleTerm
                | SinglePlaneDeconvolverPlan::Multiscale
                | SinglePlaneDeconvolverPlan::Mtmfs
        );

    SinglePlaneExecutionPlan {
        spectral,
        projection,
        deconvolver,
        weighting: weighting_label(&config.weighting),
        output_channel_count,
        primary_beam_products,
        primary_beam_requirement,
        output_products,
        cpu_multi_worker: cpu_multi_worker_eligibility(
            config,
            standard_mfs_eligible,
            mosaic_mfs_eligible,
            one_output_channel,
        ),
        gpu_metal: gpu_metal_eligibility(
            config,
            standard_mfs_eligible,
            mosaic_mfs_eligible,
            one_output_channel,
        ),
        stage_timing_attribution: "frontend-core-product-stages",
        standard_mfs_regression_sentinel,
    }
}

fn output_channel_count(config: &CliConfig) -> usize {
    match config.spectral_mode {
        SpectralMode::Mfs => 1,
        SpectralMode::Cube | SpectralMode::Cubedata => config.channel_count.unwrap_or(1),
    }
}

fn spectral_plan(mode: SpectralMode, output_channel_count: usize) -> SinglePlaneSpectralPlan {
    match mode {
        SpectralMode::Mfs => SinglePlaneSpectralPlan::Mfs,
        SpectralMode::Cube | SpectralMode::Cubedata if output_channel_count == 1 => {
            SinglePlaneSpectralPlan::CubeLikeOneChannel
        }
        SpectralMode::Cube | SpectralMode::Cubedata => {
            SinglePlaneSpectralPlan::CubeLikeMultiChannel
        }
    }
}

fn projection_plan(config: &CliConfig, force_standard_gridder: bool) -> SinglePlaneProjectionPlan {
    if matches!(config.w_term_mode, WTermMode::WProject | WTermMode::Direct) {
        return SinglePlaneProjectionPlan::WProjection;
    }
    if config.use_pointing || needs_single_field_primary_beam_products(config) {
        return SinglePlaneProjectionPlan::Mosaic;
    }
    if !force_standard_gridder
        && !config.force_standard_gridder
        && (config.field_ids.as_ref().is_some_and(|ids| ids.len() > 1)
            || config.phasecenter_field.is_some()
            || config.phasecenter.is_some())
    {
        return SinglePlaneProjectionPlan::Mosaic;
    }
    if force_standard_gridder || config.force_standard_gridder {
        SinglePlaneProjectionPlan::Standard
    } else {
        SinglePlaneProjectionPlan::StandardOrMosaicInferred
    }
}

fn deconvolver_plan(deconvolver: Deconvolver) -> SinglePlaneDeconvolverPlan {
    match deconvolver {
        Deconvolver::Mtmfs => SinglePlaneDeconvolverPlan::Mtmfs,
        Deconvolver::Multiscale => SinglePlaneDeconvolverPlan::Multiscale,
        Deconvolver::Hogbom | Deconvolver::Clark => SinglePlaneDeconvolverPlan::SingleTerm,
    }
}

fn primary_beam_requirement(
    config: &CliConfig,
    projection: SinglePlaneProjectionPlan,
    primary_beam_products: bool,
) -> SinglePlanePrimaryBeamRequirement {
    if matches!(projection, SinglePlaneProjectionPlan::Mosaic) {
        SinglePlanePrimaryBeamRequirement::MosaicProjection
    } else if primary_beam_products && matches!(projection, SinglePlaneProjectionPlan::WProjection)
    {
        SinglePlanePrimaryBeamRequirement::WProjectionProducts
    } else if primary_beam_products {
        SinglePlanePrimaryBeamRequirement::SingleFieldProducts
    } else if config.use_pointing {
        SinglePlanePrimaryBeamRequirement::MosaicProjection
    } else {
        SinglePlanePrimaryBeamRequirement::None
    }
}

fn output_products(
    config: &CliConfig,
    primary_beam_requirement: SinglePlanePrimaryBeamRequirement,
) -> Vec<String> {
    let mut products = Vec::new();
    if config.deconvolver == Deconvolver::Mtmfs {
        let nterms = config.nterms.max(1);
        for term in 0..nterms {
            products.push(format!(".image.tt{term}"));
            products.push(format!(".residual.tt{term}"));
            products.push(format!(".model.tt{term}"));
            products.push(format!(".psf.tt{term}"));
            products.push(format!(".sumwt.tt{term}"));
        }
        if nterms > 1 {
            products.push(".alpha".to_string());
            products.push(".alpha.error".to_string());
        }
        if config.write_pb || config.pbcor || config.mosaic_pb_limit < 0.0 {
            for term in 0..nterms {
                products.push(format!(".pb.tt{term}"));
            }
        }
        if config.pbcor {
            for term in 0..nterms {
                products.push(format!(".image.tt{term}.pbcor"));
            }
            if nterms > 1 {
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
    ) {
        products.push(".weight".to_string());
    }
    if config.write_pb || config.pbcor || config.mosaic_pb_limit < 0.0 {
        products.push(".pb".to_string());
    }
    if config.pbcor {
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
    config: &CliConfig,
    standard_mfs_eligible: bool,
    mosaic_mfs_eligible: bool,
    one_output_channel: bool,
) -> BackendEligibility {
    if !one_output_channel {
        return BackendEligibility::ineligible("not-one-output-channel");
    }
    if standard_mfs_eligible || mosaic_mfs_eligible {
        let workers = match config.standard_mfs_acceleration {
            StandardMfsAccelerationPolicy::Cpu => 1,
            StandardMfsAccelerationPolicy::Auto
            | StandardMfsAccelerationPolicy::MultiCpu
            | StandardMfsAccelerationPolicy::Metal => {
                if let Some(override_workers) =
                    single_plane_grid_threads_override(config.standard_mfs_grid_threads.as_deref())
                {
                    override_workers
                } else if matches!(config.w_term_mode, WTermMode::WProject) {
                    standard_mfs_wproject_auto_grid_threads()
                } else {
                    standard_mfs_auto_grid_threads()
                }
            }
        };
        if workers > 1 {
            if mosaic_mfs_eligible {
                if config.spectral_mode.is_cube_like() && config.channel_count == Some(1) {
                    BackendEligibility::eligible(format!(
                        "mosaic-cube-like-one-channel-parallel-prepare-workers-{workers}-single-grid-owner"
                    ))
                } else if config.standard_mfs_acceleration
                    == StandardMfsAccelerationPolicy::MultiCpu
                {
                    BackendEligibility::eligible(format!(
                        "mosaic-sample-range-workers-{workers}-diagnostic"
                    ))
                } else {
                    BackendEligibility::ineligible(
                        "mosaic-auto-uses-single-grid-owner-or-metal-groups",
                    )
                }
            } else if matches!(config.w_term_mode, WTermMode::WProject) {
                BackendEligibility::eligible(format!("wproject-streaming-workers-{workers}"))
            } else if config.spectral_mode.is_cube_like() {
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
        BackendEligibility::ineligible(shared_strategy_gap_reason(config))
    }
}

fn single_plane_grid_threads_override(value: Option<&str>) -> Option<usize> {
    let value = value?.trim();
    if value.eq_ignore_ascii_case("auto") {
        return None;
    }
    value.parse::<usize>().ok().filter(|threads| *threads > 0)
}

fn gpu_metal_eligibility(
    config: &CliConfig,
    standard_mfs_eligible: bool,
    mosaic_mfs_eligible: bool,
    one_output_channel: bool,
) -> BackendEligibility {
    if !one_output_channel {
        return BackendEligibility::ineligible("not-one-output-channel");
    }
    if !(standard_mfs_eligible || mosaic_mfs_eligible) {
        return BackendEligibility::ineligible(shared_strategy_gap_reason(config));
    }
    if config.standard_mfs_acceleration == StandardMfsAccelerationPolicy::Cpu
        || config.standard_mfs_acceleration == StandardMfsAccelerationPolicy::MultiCpu
    {
        return BackendEligibility::ineligible("standard-mfs-policy-disabled-metal");
    }
    if casa_imaging::standard_mfs_metal_device_available() {
        if mosaic_mfs_eligible {
            BackendEligibility::eligible("mosaic-screen-projector-metal-single-grid-owner")
        } else if matches!(config.w_term_mode, WTermMode::WProject) {
            BackendEligibility::eligible("wproject-metal-kernel")
        } else if config.spectral_mode.is_cube_like() {
            BackendEligibility::eligible("standard-cube-like-one-channel-grouped-metal")
        } else if config.deconvolver == Deconvolver::Mtmfs {
            BackendEligibility::eligible("mtmfs-metal-sample-cache")
        } else {
            BackendEligibility::eligible("standard-mfs-grouped-metal")
        }
    } else {
        BackendEligibility::ineligible("metal-device-unavailable")
    }
}

fn shared_strategy_gap_reason(config: &CliConfig) -> String {
    if config.spectral_mode.is_cube_like() && config.channel_count != Some(1) {
        return "not-one-output-channel".to_string();
    }
    if config.spectral_mode.is_cube_like() && !matches!(config.w_term_mode, WTermMode::None) {
        return "cube-like-wterm-requires-cube-path".to_string();
    }
    if config.spectral_mode.is_cube_like()
        && config.cube_axis.interpolation != CubeInterpolation::Nearest
        && !(config.dirty_only || config.niter == 0)
    {
        return "cleaned-linear-cube-like-requires-cube-path".to_string();
    }
    if config.use_pointing || matches!(config.w_term_mode, WTermMode::Direct) {
        return "shared-strategy-not-yet-adapted-to-projection-family".to_string();
    }
    if config.save_model != SaveModelMode::None {
        return "savemodel-requires-traced-path".to_string();
    }
    if config.use_mask != CleanMaskMode::User {
        return "automask-not-supported-by-shared-strategy".to_string();
    }
    "standard-mfs-eligibility-check-rejected".to_string()
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::*;

    fn parse(args: impl IntoIterator<Item = &'static str>) -> CliConfig {
        let mut argv = vec![
            "--ms",
            "example.ms",
            "--imagename",
            "target/example",
            "--imsize",
            "128",
            "--cell-arcsec",
            "1.0",
        ];
        argv.extend(args);
        CliConfig::parse(argv.into_iter().map(OsString::from)).expect("parse config")
    }

    fn products(plan: &SinglePlaneExecutionPlan) -> Vec<&str> {
        plan.output_products
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    }

    #[test]
    fn standard_mfs_plan_reports_cpu_and_metal_eligibility_separately() {
        let config = parse([
            "--gridder",
            "standard",
            "--weighting",
            "briggs",
            "--niter",
            "10",
        ]);
        let plan = build_single_plane_execution_plan(&config, false, 1);

        assert_eq!(plan.spectral, SinglePlaneSpectralPlan::Mfs);
        assert_eq!(plan.projection, SinglePlaneProjectionPlan::Standard);
        assert!(plan.standard_mfs_regression_sentinel);
        assert!(plan.cpu_multi_worker.reason.starts_with("standard-mfs"));
        assert_eq!(
            plan.primary_beam_requirement,
            SinglePlanePrimaryBeamRequirement::None
        );
        assert_eq!(
            products(&plan),
            vec![".image", ".residual", ".model", ".psf", ".sumwt"]
        );
        assert!(
            plan.gpu_metal.reason.contains("standard-mfs")
                || plan.gpu_metal.reason == "metal-device-unavailable"
        );
        let log = plan.log_line();
        assert!(log.contains("output_products=.image,.residual,.model,.psf,.sumwt"));
        assert!(log.contains("source_stream=bounded"));
        assert!(log.contains("stage_timing_attribution=frontend-core-product-stages"));
    }

    #[test]
    fn clark_and_hogbom_share_single_term_family_when_outputs_match() {
        let hogbom = build_single_plane_execution_plan(
            &parse(["--gridder", "standard", "--deconvolver", "hogbom"]),
            false,
            1,
        );
        let clark = build_single_plane_execution_plan(
            &parse(["--gridder", "standard", "--deconvolver", "clark"]),
            false,
            1,
        );

        assert_eq!(hogbom.deconvolver, SinglePlaneDeconvolverPlan::SingleTerm);
        assert_eq!(clark.deconvolver, SinglePlaneDeconvolverPlan::SingleTerm);
        assert_eq!(hogbom.output_products, clark.output_products);
        assert_eq!(
            hogbom.cpu_multi_worker.reason,
            clark.cpu_multi_worker.reason
        );
    }

    #[test]
    fn one_channel_cube_reuses_standard_single_plane_acceleration() {
        let config = parse([
            "--specmode",
            "cube",
            "--channel-count",
            "1",
            "--gridder",
            "standard",
            "--interpolation",
            "nearest",
        ]);
        let plan = build_single_plane_execution_plan(&config, false, 1);

        assert_eq!(plan.spectral, SinglePlaneSpectralPlan::CubeLikeOneChannel);
        assert!(plan.spectral.is_one_output_channel());
        assert!(plan.cpu_multi_worker.eligible);
        assert_eq!(
            products(&plan),
            vec![".image", ".residual", ".model", ".psf", ".sumwt"]
        );
        assert!(
            plan.cpu_multi_worker
                .reason
                .starts_with("standard-cube-like-one-channel")
        );
        assert!(
            plan.gpu_metal.reason == "standard-cube-like-one-channel-grouped-metal"
                || plan.gpu_metal.reason == "metal-device-unavailable"
        );
    }

    #[test]
    fn cleaned_linear_one_channel_cube_keeps_cube_specific_path() {
        let config = parse([
            "--specmode",
            "cube",
            "--channel-count",
            "1",
            "--gridder",
            "standard",
            "--interpolation",
            "linear",
            "--niter",
            "10",
        ]);
        let plan = build_single_plane_execution_plan(&config, false, 1);

        assert_eq!(plan.spectral, SinglePlaneSpectralPlan::CubeLikeOneChannel);
        assert!(!plan.cpu_multi_worker.eligible);
        assert_eq!(
            plan.cpu_multi_worker.reason,
            "cleaned-linear-cube-like-requires-cube-path"
        );
    }

    #[test]
    fn cubedata_one_channel_reports_first_class_single_plane_eligibility() {
        let config = parse([
            "--specmode",
            "cubedata",
            "--channel-count",
            "1",
            "--gridder",
            "standard",
            "--interpolation",
            "nearest",
        ]);
        let plan = build_single_plane_execution_plan(&config, false, 1);

        assert_eq!(plan.spectral, SinglePlaneSpectralPlan::CubeLikeOneChannel);
        assert!(plan.spectral.is_one_output_channel());
        assert!(plan.cpu_multi_worker.eligible);
        assert!(
            plan.cpu_multi_worker
                .reason
                .starts_with("standard-cube-like-one-channel")
        );
        assert_eq!(
            products(&plan),
            vec![".image", ".residual", ".model", ".psf", ".sumwt"]
        );
        assert!(
            plan.gpu_metal.reason == "standard-cube-like-one-channel-grouped-metal"
                || plan.gpu_metal.reason == "metal-device-unavailable"
        );
    }

    #[test]
    fn cubedata_multi_channel_reports_cube_like_multi_channel_plan() {
        let config = parse([
            "--specmode",
            "cubedata",
            "--channel-count",
            "2",
            "--gridder",
            "standard",
        ]);
        let plan = build_single_plane_execution_plan(&config, false, 1);

        assert_eq!(plan.spectral, SinglePlaneSpectralPlan::CubeLikeMultiChannel);
        assert!(!plan.spectral.is_one_output_channel());
        assert!(!plan.cpu_multi_worker.eligible);
        assert_eq!(plan.cpu_multi_worker.reason, "not-one-output-channel");
        assert_eq!(plan.gpu_metal.reason, "not-one-output-channel");
    }

    #[test]
    fn wproject_plan_keeps_projection_family_explicit() {
        let config = parse(["--gridder", "wproject"]);
        let plan = build_single_plane_execution_plan(&config, false, 1);

        assert_eq!(plan.projection, SinglePlaneProjectionPlan::WProjection);
        assert!(
            plan.cpu_multi_worker
                .reason
                .starts_with("wproject-streaming")
        );
        assert!(
            plan.gpu_metal.reason == "wproject-metal-kernel"
                || plan.gpu_metal.reason == "metal-device-unavailable"
        );
    }

    #[test]
    fn wproject_with_pb_products_keeps_pb_requirement_explicit() {
        let config = parse(["--gridder", "wproject", "--write-pb", "--pbcor"]);
        let plan = build_single_plane_execution_plan(&config, false, 1);

        assert_eq!(plan.projection, SinglePlaneProjectionPlan::WProjection);
        assert_eq!(
            plan.primary_beam_requirement,
            SinglePlanePrimaryBeamRequirement::WProjectionProducts
        );
        assert!(plan.primary_beam_products);
        assert_eq!(
            products(&plan),
            vec![
                ".image",
                ".residual",
                ".model",
                ".psf",
                ".sumwt",
                ".pb",
                ".image.pbcor"
            ]
        );
        assert!(
            plan.log_line()
                .contains("pb_requirement=wprojection-products")
        );
    }

    #[test]
    fn mtmfs_standard_mfs_plan_uses_shared_acceleration_controls() {
        let config = parse([
            "--gridder",
            "standard",
            "--deconvolver",
            "mtmfs",
            "--nterms",
            "2",
            "--niter",
            "10",
        ]);
        let plan = build_single_plane_execution_plan(&config, false, 1);

        assert_eq!(plan.deconvolver, SinglePlaneDeconvolverPlan::Mtmfs);
        assert!(plan.standard_mfs_regression_sentinel);
        assert!(plan.cpu_multi_worker.reason.starts_with("standard-mfs"));
        assert_ne!(
            plan.gpu_metal.reason,
            "shared-strategy-not-yet-adapted-to-mtmfs"
        );
        assert_eq!(
            products(&plan),
            vec![
                ".image.tt0",
                ".residual.tt0",
                ".model.tt0",
                ".psf.tt0",
                ".sumwt.tt0",
                ".image.tt1",
                ".residual.tt1",
                ".model.tt1",
                ".psf.tt1",
                ".sumwt.tt1",
                ".alpha",
                ".alpha.error"
            ]
        );
    }

    #[test]
    fn mosaic_mfs_plan_reports_single_grid_owner_metal_by_default() {
        let config = parse([
            "--gridder",
            "mosaic",
            "--field",
            "0,1,2",
            "--phasecenter-field",
            "0",
            "--deconvolver",
            "multiscale",
            "--scales",
            "0,5,15",
            "--niter",
            "10",
        ]);
        let plan = build_single_plane_execution_plan(&config, false, 1);

        assert_eq!(plan.spectral, SinglePlaneSpectralPlan::Mfs);
        assert_eq!(plan.projection, SinglePlaneProjectionPlan::Mosaic);
        assert_eq!(
            plan.primary_beam_requirement,
            SinglePlanePrimaryBeamRequirement::MosaicProjection
        );
        assert_eq!(
            plan.cpu_multi_worker.reason,
            "mosaic-auto-uses-single-grid-owner-or-metal-groups"
        );
        assert!(
            plan.gpu_metal.reason == "mosaic-screen-projector-metal-single-grid-owner"
                || plan.gpu_metal.reason == "metal-device-unavailable"
        );
    }

    #[test]
    fn mosaic_pb_products_are_explicit_in_planner_output() {
        let config = parse([
            "--gridder",
            "mosaic",
            "--field",
            "0,1,2",
            "--phasecenter-field",
            "0",
            "--write-pb",
            "--pbcor",
            "--niter",
            "10",
        ]);
        let plan = build_single_plane_execution_plan(&config, false, 1);

        assert_eq!(
            products(&plan),
            vec![
                ".image",
                ".residual",
                ".model",
                ".psf",
                ".sumwt",
                ".weight",
                ".pb",
                ".image.pbcor"
            ]
        );
        let log = plan.log_line();
        assert!(log.contains("pb_requirement=mosaic-projection"));
        assert!(log.contains(
            "output_products=.image,.residual,.model,.psf,.sumwt,.weight,.pb,.image.pbcor"
        ));
    }

    #[test]
    fn mosaic_mfs_plan_reports_sample_range_workers_only_when_explicit() {
        let config = parse([
            "--gridder",
            "mosaic",
            "--field",
            "0,1,2",
            "--phasecenter-field",
            "0",
            "--standard-mfs-acceleration",
            "multi-cpu",
            "--deconvolver",
            "multiscale",
            "--scales",
            "0,5,15",
            "--niter",
            "10",
        ]);
        let plan = build_single_plane_execution_plan(&config, false, 1);

        assert!(
            plan.cpu_multi_worker
                .reason
                .starts_with("mosaic-sample-range-workers")
        );
        assert!(plan.cpu_multi_worker.reason.ends_with("-diagnostic"));
        assert_eq!(plan.gpu_metal.reason, "standard-mfs-policy-disabled-metal");
    }

    #[test]
    fn representative_standard_rejection_keeps_explicit_reason() {
        let config = parse([
            "--gridder",
            "standard",
            "--savemodel",
            "modelcolumn",
            "--niter",
            "10",
        ]);
        let plan = build_single_plane_execution_plan(&config, false, 1);

        assert!(!plan.cpu_multi_worker.eligible);
        assert_eq!(
            plan.cpu_multi_worker.reason,
            "savemodel-requires-traced-path"
        );
        assert_eq!(plan.gpu_metal.reason, "savemodel-requires-traced-path");
    }

    #[test]
    fn representative_automask_rejection_keeps_explicit_reason() {
        let config = parse([
            "--gridder",
            "standard",
            "--usemask",
            "auto-multithresh",
            "--niter",
            "10",
        ]);
        let plan = build_single_plane_execution_plan(&config, false, 1);

        assert!(!plan.cpu_multi_worker.eligible);
        assert_eq!(
            plan.cpu_multi_worker.reason,
            "automask-not-supported-by-shared-strategy"
        );
        assert_eq!(
            plan.gpu_metal.reason,
            "automask-not-supported-by-shared-strategy"
        );
    }
}
