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
    CubeOneChannel,
    CubedataOneChannel,
    CubeMultiChannel,
    CubedataMultiChannel,
}

impl SinglePlaneSpectralPlan {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Mfs => "mfs",
            Self::CubeOneChannel => "cube-one-channel",
            Self::CubedataOneChannel => "cubedata-one-channel",
            Self::CubeMultiChannel => "cube-multi-channel",
            Self::CubedataMultiChannel => "cubedata-multi-channel",
        }
    }

    pub(crate) fn is_one_output_channel(self) -> bool {
        matches!(
            self,
            Self::Mfs | Self::CubeOneChannel | Self::CubedataOneChannel
        )
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
    pub(crate) cpu_multi_worker: BackendEligibility,
    pub(crate) gpu_metal: BackendEligibility,
    pub(crate) standard_mfs_regression_sentinel: bool,
}

impl SinglePlaneExecutionPlan {
    pub(crate) fn log_line(&self) -> String {
        format!(
            "single_plane_execution_plan spectral={} projection={} deconvolver={} weighting={} output_channels={} one_output_channel={} pb_products={} cpu_multi_worker_eligible={} cpu_multi_worker_reason={} gpu_metal_eligible={} gpu_metal_reason={} standard_mfs_regression_sentinel={}",
            self.spectral.label(),
            self.projection.label(),
            self.deconvolver.label(),
            self.weighting,
            self.output_channel_count,
            self.spectral.is_one_output_channel(),
            self.primary_beam_products,
            self.cpu_multi_worker.eligible,
            self.cpu_multi_worker.reason,
            self.gpu_metal.eligible,
            self.gpu_metal.reason,
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
        SpectralMode::Cube if output_channel_count == 1 => SinglePlaneSpectralPlan::CubeOneChannel,
        SpectralMode::Cube => SinglePlaneSpectralPlan::CubeMultiChannel,
        SpectralMode::Cubedata if output_channel_count == 1 => {
            SinglePlaneSpectralPlan::CubedataOneChannel
        }
        SpectralMode::Cubedata => SinglePlaneSpectralPlan::CubedataMultiChannel,
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
                BackendEligibility::eligible(format!("mosaic-sample-range-workers-{workers}"))
            } else if matches!(config.w_term_mode, WTermMode::WProject) {
                BackendEligibility::eligible(format!("wproject-streaming-workers-{workers}"))
            } else if matches!(config.spectral_mode, SpectralMode::Cube) {
                BackendEligibility::eligible(format!(
                    "standard-cube-one-channel-parallel-prepare-workers-{workers}"
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
            BackendEligibility::eligible("mosaic-screen-projector-metal")
        } else if matches!(config.w_term_mode, WTermMode::WProject) {
            BackendEligibility::eligible("wproject-metal-kernel")
        } else if matches!(config.spectral_mode, SpectralMode::Cube) {
            BackendEligibility::eligible("standard-cube-one-channel-grouped-metal")
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
    if !matches!(config.spectral_mode, SpectralMode::Mfs | SpectralMode::Cube) {
        return "shared-strategy-not-yet-adapted-to-spectral-mode".to_string();
    }
    if matches!(config.spectral_mode, SpectralMode::Cube) && config.channel_count != Some(1) {
        return "not-one-output-channel".to_string();
    }
    if matches!(config.spectral_mode, SpectralMode::Cube)
        && !matches!(config.w_term_mode, WTermMode::None)
    {
        return "cube-wterm-requires-cube-path".to_string();
    }
    if matches!(config.spectral_mode, SpectralMode::Cube)
        && config.cube_axis.interpolation != CubeInterpolation::Nearest
        && !(config.dirty_only || config.niter == 0)
    {
        return "cleaned-linear-cube-requires-cube-path".to_string();
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
        assert!(
            plan.gpu_metal.reason.contains("standard-mfs")
                || plan.gpu_metal.reason == "metal-device-unavailable"
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

        assert_eq!(plan.spectral, SinglePlaneSpectralPlan::CubeOneChannel);
        assert!(plan.spectral.is_one_output_channel());
        assert!(plan.cpu_multi_worker.eligible);
        assert!(
            plan.cpu_multi_worker
                .reason
                .starts_with("standard-cube-one-channel")
        );
        assert!(
            plan.gpu_metal.reason == "standard-cube-one-channel-grouped-metal"
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

        assert_eq!(plan.spectral, SinglePlaneSpectralPlan::CubeOneChannel);
        assert!(!plan.cpu_multi_worker.eligible);
        assert_eq!(
            plan.cpu_multi_worker.reason,
            "cleaned-linear-cube-requires-cube-path"
        );
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
    }

    #[test]
    fn mosaic_mfs_plan_reports_sample_range_and_metal_eligibility() {
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
        assert!(
            plan.cpu_multi_worker
                .reason
                .starts_with("mosaic-sample-range-workers")
        );
        assert!(
            plan.gpu_metal.reason == "mosaic-screen-projector-metal"
                || plan.gpu_metal.reason == "metal-device-unavailable"
        );
    }
}
