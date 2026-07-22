// SPDX-License-Identifier: LGPL-3.0-or-later
#![allow(missing_docs)]

#[cfg(test)]
use casa_imaging::SinglePlanePrimaryBeamRequirement;
use casa_imaging::{
    SinglePlaneAccelerationPolicy, SinglePlaneCubeInterpolation, SinglePlaneDeconvolverPlan,
    SinglePlaneExecutionPlan, SinglePlaneExecutionPlanInput, SinglePlaneProjectionPlan,
    SinglePlaneSpectralPlan, WTermMode, build_single_plane_execution_plan as build_core_plan,
};
use casa_ms::CubeInterpolation;

use crate::{
    CleanMaskMode, CliConfig, SpectralMode, StandardMfsAccelerationPolicy,
    can_plan_mosaic_mfs_acceleration, can_plan_standard_mfs_acceleration,
    needs_single_field_primary_beam_products, phasecenter_field_matches_single_selected_field,
    standard_mfs_auto_grid_threads, standard_mfs_wproject_auto_grid_threads,
};

pub(crate) fn build_single_plane_execution_plan(
    config: &CliConfig,
    force_standard_gridder: bool,
    ms_count: usize,
) -> SinglePlaneExecutionPlan {
    let output_channel_count = output_channel_count(config);
    let spectral = spectral_plan(config.spectral_mode, output_channel_count);
    let projection = projection_plan(config, force_standard_gridder);
    let primary_beam_products = needs_single_field_primary_beam_products(config);
    build_core_plan(SinglePlaneExecutionPlanInput::new(
        spectral,
        projection,
        SinglePlaneDeconvolverPlan::from_deconvolver(config.deconvolver),
        config.weighting,
        output_channel_count,
        primary_beam_products,
        config.write_pb,
        config.pbcor,
        config.mosaic_pb_limit < 0.0,
        config.nterms,
        can_plan_standard_mfs_acceleration(config, force_standard_gridder, ms_count),
        can_plan_mosaic_mfs_acceleration(config, ms_count),
        acceleration_policy(config.standard_mfs_acceleration),
        single_plane_grid_threads_override(config.standard_mfs_grid_threads.as_deref()),
        standard_mfs_auto_grid_threads(),
        standard_mfs_wproject_auto_grid_threads(),
        casa_imaging::standard_mfs_metal_device_available(),
        config.spectral_mode.is_cube_like(),
        cube_interpolation(config.cube_axis.interpolation),
        config.dirty_only || config.niter == 0,
        config.use_pointing,
        config.use_mask == CleanMaskMode::User,
        config.w_term_mode,
    ))
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

fn acceleration_policy(policy: StandardMfsAccelerationPolicy) -> SinglePlaneAccelerationPolicy {
    match policy {
        StandardMfsAccelerationPolicy::Cpu => SinglePlaneAccelerationPolicy::Cpu,
        StandardMfsAccelerationPolicy::Auto => SinglePlaneAccelerationPolicy::Auto,
        StandardMfsAccelerationPolicy::MultiCpu => SinglePlaneAccelerationPolicy::MultiCpu,
        StandardMfsAccelerationPolicy::Metal => SinglePlaneAccelerationPolicy::Metal,
    }
}

fn cube_interpolation(interpolation: CubeInterpolation) -> SinglePlaneCubeInterpolation {
    match interpolation {
        CubeInterpolation::Nearest => SinglePlaneCubeInterpolation::Nearest,
        CubeInterpolation::Linear | CubeInterpolation::Cubic => SinglePlaneCubeInterpolation::Other,
    }
}

fn projection_plan(config: &CliConfig, force_standard_gridder: bool) -> SinglePlaneProjectionPlan {
    if config.aw_project.is_some() {
        return SinglePlaneProjectionPlan::AwProject;
    }
    if matches!(config.w_term_mode, WTermMode::WProject | WTermMode::Direct) {
        return SinglePlaneProjectionPlan::WProjection;
    }
    if config.use_pointing || needs_single_field_primary_beam_products(config) {
        return SinglePlaneProjectionPlan::Mosaic;
    }
    if !force_standard_gridder
        && !config.force_standard_gridder
        && (config.field_ids.as_ref().is_some_and(|ids| ids.len() > 1)
            || (config.phasecenter_field.is_some()
                && !phasecenter_field_matches_single_selected_field(config))
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

fn single_plane_grid_threads_override(value: Option<&str>) -> Option<usize> {
    let value = value?.trim();
    if value.eq_ignore_ascii_case("auto") {
        return None;
    }
    value.parse::<usize>().ok().filter(|threads| *threads > 0)
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
    fn awproject_plan_keeps_combined_projection_and_cache_products_explicit() {
        let config = parse([
            "--gridder",
            "awproject",
            "--cfcache",
            "/tmp/casa-aw-cache",
            "--usepointing",
            "--deconvolver",
            "mtmfs",
            "--nterms",
            "2",
            "--write-pb",
        ]);
        let plan = build_single_plane_execution_plan(&config, false, 1);

        assert_eq!(plan.projection, SinglePlaneProjectionPlan::AwProject);
        assert_eq!(
            plan.primary_beam_requirement,
            SinglePlanePrimaryBeamRequirement::AwProjection
        );
        assert_eq!(
            plan.cpu_multi_worker.reason,
            "awproject-currently-uses-one-grid-owner"
        );
        assert_eq!(
            plan.gpu_metal.reason,
            "awproject-metal-kernel-not-implemented"
        );
        assert_eq!(
            products(&plan),
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
            ]
        );
        let log = plan.log_line();
        assert!(log.contains("projection=awproject"));
        assert!(log.contains("pb_requirement=awprojection"));
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
    fn bounded_model_writeback_keeps_standard_acceleration_eligible() {
        let config = parse([
            "--gridder",
            "standard",
            "--savemodel",
            "modelcolumn",
            "--niter",
            "10",
        ]);
        let plan = build_single_plane_execution_plan(&config, false, 1);

        assert!(plan.cpu_multi_worker.eligible);
        assert!(
            plan.cpu_multi_worker
                .reason
                .starts_with("standard-mfs-fixed-tile-workers-")
        );
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
