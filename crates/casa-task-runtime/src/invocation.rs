// SPDX-License-Identifier: LGPL-3.0-or-later
//! How a resolved run starts its task binary.
//!
//! Every task binary runs on the shared task front end (#931), so every run
//! is one invocation shape: the surface's fixed arguments, which name the
//! surface a family binary hosts, then `--json-run -` with a
//! [`TaskRunRequest`] of the values the caller set. The binary resolves them
//! against the same contract, so defaults are not repeated.

use casa_provider_contracts::{ParameterValue, ProviderInvocation, TaskRunRequest, ValueAdapter};
use thiserror::Error;

use crate::ParameterSession;

/// The invocation of `session`'s task binary: the surface's fixed
/// arguments, then `--json-run -` with a [`TaskRunRequest`] holding the
/// values the caller set, from the base profile and the overrides, that are
/// active.
pub fn project_provider_invocation(
    session: &ParameterSession,
) -> Result<ProviderInvocation, ProviderInvocationError> {
    let values = session
        .states()
        .iter()
        .filter(|(_, state)| state.explicit && state.active)
        .filter_map(|(name, state)| Some((name.clone(), state.value.as_ref()?.to_plain_json())))
        .collect();
    let request = TaskRunRequest {
        surface: session.bundle().surface.id().to_string(),
        values,
    };
    let stdin = serde_json::to_string(&request)
        .map_err(|error| ProviderInvocationError::Request(error.to_string()))?;
    let mut args = session.bundle().surface.execution().fixed_args.clone();
    args.extend(["--json-run".to_string(), "-".to_string()]);
    Ok(ProviderInvocation {
        args,
        stdin: Some(stdin),
    })
}

/// Failure to project a resolved parameter session into a task process.
#[derive(Debug, Error)]
pub enum ProviderInvocationError {
    /// The task request could not be written.
    #[error("cannot write the task request: {0}")]
    Request(String),
}

/// Whether `value` is the explicit absence its provider adapter omits:
/// `"none"` or `"auto"` under `omit_none`, or an empty list under any.
pub fn parameter_value_is_omitted(value: &ParameterValue, adapter: Option<&ValueAdapter>) -> bool {
    matches!(value, ParameterValue::Array(values) if values.is_empty())
        || (matches!(adapter, Some(ValueAdapter::OmitNone)) && is_explicit_absence(value))
}

fn is_explicit_absence(value: &ParameterValue) -> bool {
    match value {
        ParameterValue::String(state) => state == "none" || state == "auto",
        ParameterValue::Array(values) => {
            !values.is_empty() && values.iter().all(is_explicit_absence)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use casa_provider_contracts::{ParameterValue, builtin_surface_bundle};

    use crate::ParameterSession;

    use super::*;

    /// The task request `session` sends its binary.
    fn request(session: &ParameterSession) -> TaskRunRequest {
        let invocation = project_provider_invocation(session).unwrap();
        serde_json::from_str(invocation.stdin.as_deref().unwrap()).unwrap()
    }

    #[test]
    fn a_run_is_one_json_run_of_the_values_set() {
        let mut session = ParameterSession::defaults(builtin_surface_bundle("feather").unwrap())
            .expect("feather defaults");
        for (name, path) in [
            ("imagename", "products/feathered.image"),
            ("highres", "high.image"),
            ("lowres", "low.image"),
        ] {
            session
                .set(name, ParameterValue::String(path.into()))
                .unwrap();
        }
        let invocation = project_provider_invocation(&session).unwrap();
        assert_eq!(invocation.args, ["--json-run", "-"]);
        let feather = request(&session);
        assert_eq!(feather.surface, "feather");
        // The values set, and no defaults such as sdfactor or overwrite.
        assert_eq!(
            feather.values.keys().collect::<Vec<_>>(),
            ["highres", "imagename", "lowres"]
        );

        // A family binary's surface is named by its fixed arguments too.
        let mut imstat =
            ParameterSession::defaults(builtin_surface_bundle("imstat").unwrap()).unwrap();
        imstat
            .set("imagename", ParameterValue::String("in.image".into()))
            .unwrap();
        let invocation = project_provider_invocation(&imstat).unwrap();
        assert_eq!(invocation.args, ["imstat", "--json-run", "-"]);
        let mut gaincal =
            ParameterSession::defaults(builtin_surface_bundle("gaincal").unwrap()).unwrap();
        gaincal
            .set("vis", ParameterValue::String("in.ms".into()))
            .unwrap();
        let invocation = project_provider_invocation(&gaincal).unwrap();
        assert_eq!(invocation.args, ["gaincal", "--json-run", "-"]);
        assert_eq!(request(&gaincal).surface, "gaincal");
    }

    #[test]
    fn an_inactive_value_is_not_sent() {
        let mut session =
            ParameterSession::defaults(builtin_surface_bundle("applycal").unwrap()).unwrap();
        session
            .set("vis", ParameterValue::String("in.ms".into()))
            .unwrap();
        session
            .set("spw", ParameterValue::String("0".into()))
            .unwrap();
        session
            .set("selectdata", ParameterValue::Bool(false))
            .unwrap();
        let values = request(&session).values;
        assert_eq!(values["selectdata"], serde_json::json!(false));
        assert!(!values.contains_key("spw"), "{values:?}");
    }

    #[test]
    fn a_vlass_awproject_profile_is_one_complete_request() {
        let bundle = builtin_surface_bundle("imager").unwrap();
        let mut session = ParameterSession::defaults(bundle).unwrap();
        let set = [
            ("vis", ParameterValue::String("vlass.ms".into())),
            ("imagename", ParameterValue::String("products/vlass".into())),
            ("field", ParameterValue::String("1525".into())),
            ("spw", ParameterValue::String("9".into())),
            ("uvrange", ParameterValue::String("<12km".into())),
            ("intent", ParameterValue::String("*TARGET*".into())),
            ("stokes", ParameterValue::String("I".into())),
            ("gridder", ParameterValue::String("awproject".into())),
            ("cfcache", ParameterValue::String("/cache/vlass".into())),
            ("deconvolver", ParameterValue::String("mtmfs".into())),
            ("scales", ParameterValue::String("0,5,12".into())),
            ("normtype", ParameterValue::String("flatnoise".into())),
            ("parallel", ParameterValue::Bool(false)),
            ("usepointing", ParameterValue::Bool(true)),
            ("write_pb", ParameterValue::Bool(true)),
            ("nterms", ParameterValue::Integer(2)),
            ("wprojplanes", ParameterValue::Integer(32)),
            ("cf_resident_mb", ParameterValue::Integer(256)),
            ("pblimit", ParameterValue::Float(0.0001)),
        ];
        for (name, value) in &set {
            session
                .set(*name, value.clone())
                .unwrap_or_else(|error| panic!("set VLASS profile parameter {name}: {error}"));
        }
        let request = request(&session);
        assert_eq!(request.surface, "imager");
        for (name, _) in &set {
            assert_eq!(
                request.values.get(*name),
                session.states()[*name]
                    .value
                    .as_ref()
                    .map(ParameterValue::to_plain_json)
                    .as_ref(),
                "{name}"
            );
        }
        assert_eq!(request.values["vis"], serde_json::json!(["vlass.ms"]));
    }

    #[test]
    fn importvla_archivefiles_travel_as_a_list() {
        let archivefiles = ParameterValue::Array(vec![
            ParameterValue::String("raw/one.exp".into()),
            ParameterValue::String("raw/two.xp1".into()),
        ]);
        let mut session =
            ParameterSession::defaults(builtin_surface_bundle("importvla").unwrap()).unwrap();
        session.set("archivefiles", archivefiles).unwrap();

        let sparse = session.render_sparse().unwrap();
        assert!(sparse.contains("archivefiles = [\"raw/one.exp\", \"raw/two.xp1\"]"));
        assert_eq!(
            request(&session).values["archivefiles"],
            serde_json::json!(["raw/one.exp", "raw/two.xp1"])
        );
    }

    #[test]
    fn auto_and_none_sentinels_are_omitted_but_real_values_are_kept() {
        let omit_none = Some(&ValueAdapter::OmitNone);
        for sentinel in ["auto", "none"] {
            let value = ParameterValue::String(sentinel.into());
            assert!(parameter_value_is_omitted(&value, omit_none), "{sentinel}");
            assert!(!parameter_value_is_omitted(
                &value,
                Some(&ValueAdapter::Identity)
            ));
        }
        assert!(!parameter_value_is_omitted(
            &ParameterValue::String("spectral".into()),
            omit_none
        ));
        let pair = ParameterValue::Array(vec![
            ParameterValue::Integer(256),
            ParameterValue::Integer(128),
        ]);
        assert!(!parameter_value_is_omitted(&pair, omit_none));
    }

    #[test]
    fn omit_none_drops_scalar_and_collection_absence_sentinels_only() {
        let omit = Some(&ValueAdapter::OmitNone);
        let auto = || ParameterValue::String("auto".into());
        assert!(parameter_value_is_omitted(&auto(), omit));
        assert!(parameter_value_is_omitted(
            &ParameterValue::String("none".into()),
            omit
        ));
        assert!(parameter_value_is_omitted(
            &ParameterValue::Array(vec![auto(), auto()]),
            omit
        ));
        assert!(parameter_value_is_omitted(
            &ParameterValue::Array(Vec::new()),
            None
        ));
        assert!(!parameter_value_is_omitted(&auto(), None));
        assert!(!parameter_value_is_omitted(
            &ParameterValue::Array(vec![auto(), ParameterValue::String("1arcsec".into())]),
            omit
        ));
    }
}
