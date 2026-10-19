// SPDX-License-Identifier: LGPL-3.0-or-later
//! Laws of [`AwPlan::deploy`] (#735): a native cache generated for a wider
//! SPW selection names the subset's cells and holds different ones, so
//! `generate-missing` regenerates it and `reuse-only` refuses it.

use casa_imaging_model::{
    EvlaDishSurface, NativeAwFrequencyGroup, NativeAwGrid, NativeAwRequestInput, NativeAwTerms,
};
use casa_imaging_operator::{AwCatalog, AwIndexing, NativeManifest};

use super::{AwPlan, NativeAwCachePolicy, PrepareError};
use crate::{HostResources, ResourcePolicy};

const CELL_RAD: f64 = 1.0e-4;
const SIZE: usize = 64;
const OVERSAMPLING: usize = 4;
const REFERENCE_HZ: f64 = 1.5e9;

/// A host whose memory no deployment here reaches.
const HOST: HostResources = HostResources {
    threads: 1,
    performance_cores: 1,
    available_memory: 1 << 40,
    metal: false,
};
/// One worker and all of [`HOST`]'s memory.
const POLICY: ResourcePolicy = ResourcePolicy::Explicit {
    workers: 1,
    memory: 1 << 40,
};
const AMPLE: (&HostResources, &ResourcePolicy) = (&HOST, &POLICY);

/// Two frequency groups about the reference, conjugate beams on: each
/// group's conjugate beam is the other group.
fn wide() -> NativeAwRequestInput {
    let working = CELL_RAD * OVERSAMPLING as f64;
    NativeAwRequestInput {
        surface: EvlaDishSurface::new(
            (0..=125)
                .map(|i| {
                    let r = f64::from(i) / 10.0;
                    [r, r * r / 36.0, r / 18.0]
                })
                .collect(),
        )
        .expect("surface"),
        antenna_diameter_m: 25.0,
        frequencies: [1.4e9, 1.6e9]
            .iter()
            .enumerate()
            .map(|(window, frequency)| NativeAwFrequencyGroup {
                spectral_window: window as u32,
                channel_frequencies_hz: vec![*frequency],
                cf_frequency_hz: *frequency,
            })
            .collect(),
        w_values: vec![0.0],
        w_increment: 1.0,
        pa_values: vec![0.31],
        mueller_elements: vec![0, 15],
        reference_frequency_hz: REFERENCE_HZ,
        grid: NativeAwGrid {
            size: SIZE,
            sky_increment_rad: [-working, working],
            oversampling: OVERSAMPLING,
        },
        terms: NativeAwTerms {
            aperture: true,
            w_term: true,
            prolate_spheroidal: false,
            wideband: true,
            conjugate_beams: true,
        },
        maximum_cells: 16,
    }
}

/// The first group alone: a subset selection whose 1.4 GHz cell has itself
/// as conjugate beam.
fn subset() -> NativeAwRequestInput {
    let mut subset = wide();
    subset.frequencies.truncate(1);
    subset
}

fn plan(root: &std::path::Path, policy: NativeAwCachePolicy) -> AwPlan {
    plan_for(root, policy, subset())
}

fn plan_for(
    root: &std::path::Path,
    policy: NativeAwCachePolicy,
    input: NativeAwRequestInput,
) -> AwPlan {
    AwPlan {
        root: root.to_path_buf(),
        native: Some((policy, input)),
        indexing: AwIndexing {
            conjugate_beams: true,
            image_reference_hz: REFERENCE_HZ,
        },
        resident_bytes: 1 << 20,
        automatic: false,
    }
}

/// A cache directory generated for [`wide`].
fn wide_cache() -> tempfile::TempDir {
    let root = tempfile::tempdir().expect("cache directory");
    AwCatalog::generate_native(root.path(), &wide(), false, 1).expect("generate");
    root
}

#[test]
fn generate_missing_regenerates_a_cache_of_another_selection() {
    let root = wide_cache();
    plan(root.path(), NativeAwCachePolicy::GenerateMissing)
        .deploy(AMPLE)
        .expect("deploy");
    let state = AwCatalog::native_cache_state(root.path(), &subset()).expect("state");
    assert!(state.reusable(), "{state}");
}

#[test]
fn reuse_only_refuses_a_cache_of_another_selection() {
    let root = wide_cache();
    let Err(PrepareError::AwCacheNotReusable { state }) =
        plan(root.path(), NativeAwCachePolicy::ReuseOnly).deploy(AMPLE)
    else {
        panic!("reuse-only accepted a cache of another selection");
    };
    assert_eq!(state.manifest, NativeManifest::Differs);
    assert_eq!(state.present, state.expected);
    assert!(state.foreign > 0);
}

#[test]
fn generate_missing_keeps_a_cache_of_this_request() {
    let root = tempfile::tempdir().expect("cache directory");
    AwCatalog::generate_native(root.path(), &subset(), false, 1).expect("generate");
    let cell = root.path().join("CFS_0_0_CF_0_0_0.im/table.dat");
    let written = std::fs::metadata(&cell)
        .expect("cell")
        .modified()
        .expect("mtime");
    plan(root.path(), NativeAwCachePolicy::GenerateMissing)
        .deploy(AMPLE)
        .expect("deploy");
    let kept = std::fs::metadata(&cell)
        .expect("cell")
        .modified()
        .expect("mtime");
    assert_eq!(written, kept, "a reusable cache is not regenerated");
}

/// #739 review: a run's deployment holds the cache's shared lock for as
/// long as it lives, since its catalog reads cells lazily. Another
/// request's generate-missing, or a regenerate, is then refused rather than
/// replacing the cells under it, and the cache stays the first run's. Once
/// that run ends the cache is regenerated for the other request.
#[test]
fn a_cache_in_use_is_not_regenerated_under_its_run() {
    let root = tempfile::tempdir().expect("cache directory");
    let running = plan_for(root.path(), NativeAwCachePolicy::GenerateMissing, wide())
        .deploy(AMPLE)
        .expect("the first run deploys");
    for policy in [
        NativeAwCachePolicy::GenerateMissing,
        NativeAwCachePolicy::Regenerate,
    ] {
        let Err(PrepareError::AwCatalog(error)) = plan(root.path(), policy).deploy(AMPLE) else {
            panic!("{policy:?} replaced a cache in use");
        };
        assert!(error.to_string().contains("another run"), "{error}");
        assert!(
            AwCatalog::native_cache_state(root.path(), &wide())
                .expect("state")
                .reusable(),
            "{policy:?} touched the cache in use"
        );
    }
    // Another reader of the same request shares it.
    let sharing = plan_for(root.path(), NativeAwCachePolicy::ReuseOnly, wide())
        .deploy(AMPLE)
        .expect("a second reader shares the cache");
    drop((running, sharing));
    plan(root.path(), NativeAwCachePolicy::GenerateMissing)
        .deploy(AMPLE)
        .expect("regenerated once the runs end");
    assert!(
        AwCatalog::native_cache_state(root.path(), &subset())
            .expect("state")
            .reusable()
    );
}

/// #708, #747 review: generation runs on the most workers whose workspaces
/// fit the policy, and on no more than there are cells to generate. Of
/// eight cells, under a policy of four workers that holds two workspaces,
/// it runs on two, where three would be refused; a fill of one missing cell
/// runs on one, however many fit; and a complete cache is reused.
#[test]
fn native_generation_takes_the_workers_its_policy_fits() {
    let root = tempfile::tempdir().expect("cache directory");
    let host = HostResources {
        threads: 4,
        performance_cores: 4,
        ..HOST
    };
    let mut input = subset();
    input.w_values = vec![0.0, 10.0, 20.0, 30.0];
    input.maximum_cells = 8;
    let two = ResourcePolicy::Explicit {
        workers: 4,
        memory: AwCatalog::generation_bytes(&input, 2),
    };
    assert!(AwCatalog::generation_bytes(&input, 3) > two.memory(&host));
    let workers = |policy: &ResourcePolicy| {
        plan_for(
            root.path(),
            NativeAwCachePolicy::GenerateMissing,
            input.clone(),
        )
        .deploy((&host, policy))
        .expect("deployed")
        .generation_workers
    };
    assert_eq!(workers(&two), Some(2));
    let state = AwCatalog::native_cache_state(root.path(), &input).expect("state");
    assert!(state.reusable(), "{state}");

    for prefix in ["CFS_", "WTCFS_"] {
        std::fs::remove_dir_all(root.path().join(format!("{prefix}0_0_CF_0_2_1.im")))
            .expect("remove a cell");
    }
    let four = ResourcePolicy::Explicit {
        workers: 4,
        memory: AwCatalog::generation_bytes(&input, 4),
    };
    assert_eq!(workers(&four), Some(1), "one cell to generate");
    assert_eq!(workers(&four), None, "a complete cache");
}

/// #738: native generation is admitted before it starts. Under a policy
/// that cannot hold [`AwCatalog::generation_bytes`] it is refused, typed,
/// and writes no cell.
#[test]
fn native_generation_is_admitted_before_it_writes_a_cell() {
    let root = tempfile::tempdir().expect("cache directory");
    let tight = ResourcePolicy::Explicit {
        workers: 1,
        memory: AwCatalog::generation_bytes(&subset(), 1) - 1,
    };
    let Err(PrepareError::Admission(admission)) =
        plan(root.path(), NativeAwCachePolicy::GenerateMissing).deploy((&HOST, &tight))
    else {
        panic!("generation ran beyond its policy");
    };
    assert_eq!(admission.phase, "native AW generation");
    let cells = std::fs::read_dir(root.path())
        .expect("cache directory")
        .filter(|entry| {
            entry
                .as_ref()
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .contains("CFS_")
        })
        .count();
    assert_eq!(cells, 0, "no cell was written");
}
