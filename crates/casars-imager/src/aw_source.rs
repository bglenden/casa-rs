// SPDX-License-Identifier: LGPL-3.0-or-later

//! One transport projection for the application-owned paired AW cell source.

use std::{collections::BTreeMap, path::PathBuf};

use casa_imaging_application::{ContinuumAwCfSource, NativeAwCachePolicy, NativeEvlaAwCache};
use casa_provider_contracts::ParameterValue;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Explicit native private-cache action; no missing-file fallback is implied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ImagerNativeAwCachePolicy {
    /// Validate and read an already complete native catalog without generation.
    ReuseOnly,
    /// Generate absent cells, preserving valid completed members.
    GenerateMissing,
    /// Explicitly replace all requested members, including rejected cells.
    Regenerate,
}

impl From<NativeAwCachePolicy> for ImagerNativeAwCachePolicy {
    fn from(value: NativeAwCachePolicy) -> Self {
        match value {
            NativeAwCachePolicy::ReuseOnly => Self::ReuseOnly,
            NativeAwCachePolicy::GenerateMissing => Self::GenerateMissing,
            NativeAwCachePolicy::Regenerate => Self::Regenerate,
        }
    }
}

impl From<ImagerNativeAwCachePolicy> for NativeAwCachePolicy {
    fn from(value: ImagerNativeAwCachePolicy) -> Self {
        match value {
            ImagerNativeAwCachePolicy::ReuseOnly => Self::ReuseOnly,
            ImagerNativeAwCachePolicy::GenerateMissing => Self::GenerateMissing,
            ImagerNativeAwCachePolicy::Regenerate => Self::Regenerate,
        }
    }
}

/// Mutually exclusive source of paired AW imaging and weight cells.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ImagerAwCfSource {
    /// Read-only import of an existing CASA `CFS_`/`WTCFS_` directory.
    CasaImport {
        /// Explicit input directory; no managed or implicit cache discovery.
        cf_cache: PathBuf,
    },
    /// Frozen EVLA model using a private cache that is not CASA-readable.
    NativeEvla {
        /// Native cache directory, writable for generation policies.
        root: PathBuf,
        /// Explicit EVLA radius/height/slope model input file.
        surface: PathBuf,
        /// Requested action, never inferred from existing files.
        policy: ImagerNativeAwCachePolicy,
        /// Full even FFT extent before support selection.
        working_size: usize,
        /// Integer convolution-plane oversampling.
        oversampling: usize,
        /// Hard catalog-wide durable-storage cap in bytes.
        cache_bytes: u64,
        /// Maximum metadata-only cell count admitted by this request.
        maximum_cells: usize,
    },
}

impl From<&ContinuumAwCfSource> for ImagerAwCfSource {
    fn from(value: &ContinuumAwCfSource) -> Self {
        match value {
            ContinuumAwCfSource::CasaImport(cf_cache) => Self::CasaImport {
                cf_cache: cf_cache.clone(),
            },
            ContinuumAwCfSource::NativeEvla(value) => Self::NativeEvla {
                root: value.root.clone(),
                surface: value.surface.clone(),
                policy: value.policy.into(),
                working_size: value.working_size,
                oversampling: value.oversampling,
                cache_bytes: value.cache_bytes,
                maximum_cells: value.maximum_cells,
            },
        }
    }
}

impl ImagerAwCfSource {
    pub(crate) fn into_application(self) -> Result<ContinuumAwCfSource, String> {
        match self {
            Self::CasaImport { cf_cache } => {
                if cf_cache.as_os_str().is_empty()
                    || matches!(cf_cache.to_str(), Some("none" | "auto"))
                {
                    return Err(
                        "CASA AW import requires an explicit cfcache input directory".into(),
                    );
                }
                Ok(ContinuumAwCfSource::CasaImport(cf_cache))
            }
            Self::NativeEvla {
                root,
                surface,
                policy,
                working_size,
                oversampling,
                cache_bytes,
                maximum_cells,
            } => {
                let cache = NativeEvlaAwCache {
                    root,
                    surface,
                    policy: policy.into(),
                    working_size,
                    oversampling,
                    cache_bytes,
                    maximum_cells,
                };
                cache.validate().map_err(|error| error.to_string())?;
                Ok(ContinuumAwCfSource::NativeEvla(cache))
            }
        }
    }
}

pub(crate) fn source_from_parameters(
    values: &BTreeMap<String, ParameterValue>,
) -> Result<ContinuumAwCfSource, String> {
    let text = |name| super::parameter_text(values, name);
    let required_usize = |name| {
        super::optional_usize(values, name)?
            .ok_or_else(|| format!("native EVLA AW requires {name}"))
    };
    let source = match text("aw_cf_source")?.as_str() {
        "casa-import" => ImagerAwCfSource::CasaImport {
            cf_cache: PathBuf::from(text("cfcache")?),
        },
        "native-evla" => {
            if values
                .get("cfcache")
                .is_some_and(|value| value != &ParameterValue::String("none".into()))
            {
                return Err("native-evla and CASA cfcache are mutually exclusive".into());
            }
            let policy = match text("native_cf_policy")?.as_str() {
                "reuse-only" => ImagerNativeAwCachePolicy::ReuseOnly,
                "generate-missing" => ImagerNativeAwCachePolicy::GenerateMissing,
                "regenerate" => ImagerNativeAwCachePolicy::Regenerate,
                value => return Err(format!("unsupported native_cf_policy {value:?}")),
            };
            ImagerAwCfSource::NativeEvla {
                root: PathBuf::from(text("native_cf_cache")?),
                surface: PathBuf::from(text("evla_surface")?),
                policy,
                working_size: required_usize("native_cf_working_size")?,
                oversampling: required_usize("native_cf_oversampling")?,
                cache_bytes: u64::try_from(super::parameter_integer(
                    values,
                    "native_cf_cache_bytes",
                )?)
                .map_err(|error| error.to_string())?,
                maximum_cells: required_usize("native_cf_maximum_cells")?,
            }
        }
        value => return Err(format!("unsupported aw_cf_source {value:?}")),
    };
    source.into_application()
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use casa_provider_contracts::{ParameterRole, RunSafetyClass, builtin_surface_bundle};
    use casa_task_runtime::{
        BaseSource, ParameterSession, parse_parameter_cli_overrides, parse_profile,
        project_provider_invocation, render_sparse_profile, resolve_profile,
    };

    use super::*;
    use crate::{ImagerRunTaskRequest, ImagerTaskRequest};

    fn native_args(policy: &str) -> Vec<OsString> {
        [
            "--ms",
            "absent.ms",
            "--imagename",
            "products/native",
            "--gridder",
            "awproject",
            "--aw-cf-source",
            "native-evla",
            "--native-cf-cache",
            "native-cache",
            "--evla-surface",
            "models/EVLA.surface",
            "--native-cf-policy",
            policy,
            "--native-cf-working-size",
            "256",
            "--native-cf-oversampling",
            "20",
            "--native-cf-cache-bytes",
            "2147483648",
            "--native-cf-maximum-cells",
            "1024",
            "--wprojplanes",
            "32",
        ]
        .into_iter()
        .map(OsString::from)
        .collect()
    }

    #[test]
    fn t52_cli_json_resolved_parameters_and_sparse_profile_share_the_native_source() {
        for (policy, native_policy) in [
            ("reuse-only", NativeAwCachePolicy::ReuseOnly),
            ("generate-missing", NativeAwCachePolicy::GenerateMissing),
            ("regenerate", NativeAwCachePolicy::Regenerate),
        ] {
            let args = native_args(policy);
            let cli = crate::request_from_parameter_cli_args(&args).unwrap();
            let expected = ContinuumAwCfSource::NativeEvla(NativeEvlaAwCache {
                root: PathBuf::from("native-cache"),
                surface: PathBuf::from("models/EVLA.surface"),
                policy: native_policy,
                working_size: 256,
                oversampling: 20,
                cache_bytes: 2_147_483_648,
                maximum_cells: 1024,
            });
            assert_eq!(
                cli.to_cli_config().unwrap().aw_project.unwrap().source,
                expected
            );
            let encoded = serde_json::to_string(&cli).unwrap();
            let decoded: ImagerRunTaskRequest = serde_json::from_str(&encoded).unwrap();
            assert_eq!(decoded, cli);
            let application =
                crate::native_application::application_request(&decoded.to_cli_config().unwrap())
                    .unwrap();
            assert_eq!(application.aw_projection.unwrap().source, expected);

            let bundle = builtin_surface_bundle("imager").unwrap();
            let patch = parse_parameter_cli_overrides(&bundle, &args).unwrap();
            let mut session = ParameterSession::defaults(bundle.clone()).unwrap();
            session.apply_override_patch(patch).unwrap();
            assert_eq!(
                session
                    .required_run_safety()
                    .unwrap()
                    .requires_overwrite_confirmation(),
                policy == "regenerate"
            );
            let sparse = render_sparse_profile(&bundle, &session.values()).unwrap();
            assert!(!sparse.contains("cfcache ="));
            assert_eq!(
                sparse.contains("native_cf_policy ="),
                policy != "reuse-only"
            );
            let parsed = parse_profile(&sparse).unwrap();
            let restored = ParameterSession::from_profile(
                bundle.clone(),
                BaseSource::File(PathBuf::from("native.toml")),
                &parsed,
            )
            .unwrap();
            let invocation = project_provider_invocation(&restored, |_, values, direct| {
                crate::imager_provider_invocation(values, direct.args)
            })
            .unwrap();
            let ImagerTaskRequest::Run(profile_request) =
                serde_json::from_str(invocation.stdin.as_deref().unwrap()).unwrap();
            assert_eq!(profile_request, cli);
            assert_eq!(
                render_sparse_profile(&bundle, &resolve_profile(&parsed, &bundle).unwrap().values)
                    .unwrap(),
                sparse
            );
        }
    }

    #[test]
    fn t52_invalid_sources_and_bounds_fail_before_measurement_set_open() {
        for (extra, expected) in [
            (vec!["--cfcache", "casa-cache"], "inactive"),
            (vec!["--native-cf-working-size", "255"], "working"),
            (vec!["--native-cf-oversampling", "65"], "oversampling"),
            (vec!["--native-cf-cache-bytes", "0"], "positive"),
            (vec!["--native-cf-maximum-cells", "none"], "required"),
            (vec!["--native-cf-cache", "none"], "required"),
            (vec!["--evla-surface", "none"], "required"),
            (vec!["--gridder", "standard"], "inactive"),
        ] {
            let mut args = native_args("generate-missing");
            // Change one canonical flag value, except for the new conflicting input.
            if let Some(index) = args.iter().position(|arg| arg == extra[0]) {
                args[index + 1] = OsString::from(extra[1]);
            } else {
                args.extend(extra.iter().map(OsString::from));
            }
            let error = crate::request_from_parameter_cli_args(&args).unwrap_err();
            assert!(error.contains(expected), "{extra:?}: {error}");
            assert!(!error.contains("open MeasurementSet"), "{error}");
        }
        let request = crate::request_from_parameter_cli_args(&native_args("reuse-only")).unwrap();
        let mut json = serde_json::to_value(request).unwrap();
        json["aw_project"]["source"]["cf_cache"] = "casa-cache".into();
        assert!(
            serde_json::from_value::<ImagerRunTaskRequest>(json).is_err(),
            "task JSON cannot combine source variants"
        );
    }

    #[test]
    fn t52_catalog_preserves_input_output_and_explicit_overwrite_roles() {
        let bundle = builtin_surface_bundle("imager").unwrap();
        for (name, role) in [
            ("cfcache", ParameterRole::InputData),
            ("evla_surface", ParameterRole::InputData),
            ("native_cf_cache", ParameterRole::OutputData),
        ] {
            let binding = bundle
                .surface
                .bindings()
                .iter()
                .find(|binding| binding.name == name)
                .unwrap();
            assert_eq!(
                bundle
                    .catalog
                    .concept(&binding.concept)
                    .unwrap()
                    .semantic_role,
                role
            );
        }
        assert!(
            bundle
                .surface
                .safety_rules()
                .iter()
                .any(|rule| rule.class == RunSafetyClass::Overwrite)
        );
    }
}
