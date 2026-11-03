// SPDX-License-Identifier: LGPL-3.0-or-later
//! The command-line pieces the task front end shares: reading a
//! `--json-run` request, and parsing a surface's declared parameter flags.

use std::ffi::OsString;
use std::fs;
use std::io::{self, Read};
use std::path::Path;

use casa_provider_contracts::{
    DefaultSpec, ParameterType, ParameterValue, SurfaceContractBundle, SurfaceParameterBinding,
};

use crate::{ResolutionPatch, normalize_value, parse_parameter_text, validate_value};

/// Why a `--json-run` request could not be read.
#[derive(Debug, thiserror::Error)]
pub enum TaskCliError {
    #[error("failed to read JSON request from stdin: {0}")]
    ReadStdin(io::Error),
    #[error("failed to read JSON request from {path}: {source}")]
    ReadFile { path: String, source: io::Error },
    #[error("failed to parse task request: {0}")]
    ParseRequest(serde_json::Error),
}

/// The text of a `--json-run` request: from stdin for `-`, or from the
/// file `source` names.
pub fn read_task_request(source: &str) -> Result<String, TaskCliError> {
    if source == "-" {
        let mut payload = String::new();
        io::stdin()
            .read_to_string(&mut payload)
            .map_err(TaskCliError::ReadStdin)?;
        return Ok(payload);
    }
    fs::read_to_string(source).map_err(|error| TaskCliError::ReadFile {
        path: Path::new(source).display().to_string(),
        source: error,
    })
}

/// Parse catalog-declared CLI flags and positional values into one typed
/// highest-precedence parameter patch.
///
/// This parser accepts only spellings declared by the surface contract. Value
/// parsing, unit conversion, normalization, and constraints are delegated to
/// the canonical parameter edit contract rather than reimplemented by a
/// frontend.
pub fn parse_parameter_cli_overrides(
    bundle: &SurfaceContractBundle,
    args: &[OsString],
) -> Result<ResolutionPatch, String> {
    let mut patch = ResolutionPatch::default();
    let mut positional = bundle
        .surface
        .bindings()
        .iter()
        .filter_map(|binding| {
            binding
                .projections
                .cli
                .as_ref()
                .and_then(|projection| projection.positional)
                .map(|position| (position, binding))
        })
        .collect::<Vec<_>>();
    positional.sort_by_key(|(position, _)| *position);
    let mut positional_index = 0usize;
    let mut index = 0usize;
    while index < args.len() {
        let raw = args[index]
            .to_str()
            .ok_or_else(|| "parameter arguments must be valid UTF-8".to_string())?;
        if raw == "--unset" {
            let name = args
                .get(index + 1)
                .and_then(|value| value.to_str())
                .ok_or_else(|| "missing --unset parameter".to_string())?;
            ensure_parameter_binding(bundle, name)?;
            patch.values.remove(name);
            patch.unset.insert(name.to_string());
            index += 2;
            continue;
        }
        // A surface may declare a short spelling too, such as msexplore's `-o`.
        let declared_short = !raw.starts_with("--")
            && raw.starts_with('-')
            && cli_flag_binding(bundle, raw).is_some();
        if raw.starts_with("--") || declared_short {
            let (flag, inline) = raw
                .split_once('=')
                .map_or((raw, None), |(flag, value)| (flag, Some(value)));
            let (binding, false_flag) = cli_flag_binding(bundle, flag)
                .ok_or_else(|| format!("unknown {} parameter flag {flag}", bundle.surface.id()))?;
            let concept = bundle
                .catalog
                .concept(&binding.concept)
                .ok_or_else(|| format!("missing concept for {}", binding.name))?;
            let (value, consumed) = if is_bool_domain(&concept.value_domain) {
                if inline.is_some() {
                    return Err(format!("boolean {flag} does not take a value"));
                }
                (ParameterValue::Bool(!false_flag), 1)
            } else {
                if false_flag {
                    return Err(format!(
                        "non-boolean parameter {} declares boolean flag {flag}",
                        binding.name
                    ));
                }
                let text = match inline {
                    Some(value) => value,
                    None => args
                        .get(index + 1)
                        .and_then(|value| value.to_str())
                        .ok_or_else(|| format!("{flag} requires a value"))?,
                };
                (
                    validated_cli_value(bundle, binding, text)?,
                    if inline.is_some() { 1 } else { 2 },
                )
            };
            // A list without a fixed size gathers what each occurrence
            // names, as `--archivefile a.exp --archivefile b.exp` names two
            // archives; any other value takes its last occurrence.
            let value = match (patch.values.remove(&binding.name), value) {
                (Some(ParameterValue::Array(mut earlier)), ParameterValue::Array(more))
                    if is_open_list_domain(&concept.value_domain) =>
                {
                    earlier.extend(more);
                    checked_value(bundle, binding, &ParameterValue::Array(earlier))?
                }
                (_, value) => value,
            };
            patch.unset.remove(&binding.name);
            patch.values.insert(binding.name.clone(), value);
            index += consumed;
            continue;
        }

        let Some((_, binding)) = positional.get(positional_index) else {
            return Err(format!("unexpected positional parameter {raw:?}"));
        };
        patch.unset.remove(&binding.name);
        patch.values.insert(
            binding.name.clone(),
            validated_cli_value(bundle, binding, raw)?,
        );
        positional_index += 1;
        index += 1;
    }
    Ok(patch)
}

/// Whether `flag` is one of `bundle`'s own parameter spellings, which win
/// over the front end's flags of the same spelling (imregrid's `--template`).
pub(crate) fn surface_declares_flag(bundle: &SurfaceContractBundle, flag: &str) -> bool {
    cli_flag_binding(bundle, flag).is_some()
}

/// The binding a flag names, and whether it is a false flag.
///
/// The surface's declared flags win. Every parameter is also reachable by its
/// own name, `--vis` or `--gaintable` (#931), and a binding that declares no
/// spelling by its dashed name.
fn cli_flag_binding<'a>(
    bundle: &'a SurfaceContractBundle,
    flag: &str,
) -> Option<(&'a SurfaceParameterBinding, bool)> {
    let bindings = bundle.surface.bindings();
    let declared = bindings.iter().find_map(|binding| {
        let projection = binding.projections.cli.as_ref()?;
        if projection.flags.iter().any(|candidate| candidate == flag) {
            Some((binding, false))
        } else if projection
            .false_flags
            .iter()
            .any(|candidate| candidate == flag)
        {
            Some((binding, true))
        } else {
            None
        }
    });
    declared.or_else(|| {
        let name = flag.strip_prefix("--")?;
        bindings
            .iter()
            .find(|binding| {
                binding.name == name
                    || (binding.projections.cli.as_ref().is_some_and(|projection| {
                        projection.flags.is_empty()
                            && projection.false_flags.is_empty()
                            && projection.positional.is_none()
                    }) && binding.name.replace('_', "-") == name)
            })
            .map(|binding| (binding, false))
    })
}

fn ensure_parameter_binding<'a>(
    bundle: &'a SurfaceContractBundle,
    name: &str,
) -> Result<&'a SurfaceParameterBinding, String> {
    bundle
        .surface
        .bindings()
        .iter()
        .find(|binding| binding.name == name)
        .ok_or_else(|| format!("unknown {} parameter {name:?}", bundle.surface.id()))
}

/// Parse, normalize and validate one command-line value through the
/// binding's concept and refinements.
fn validated_cli_value(
    bundle: &SurfaceContractBundle,
    binding: &SurfaceParameterBinding,
    text: &str,
) -> Result<ParameterValue, String> {
    let concept = bundle
        .catalog
        .concept(&binding.concept)
        .ok_or_else(|| format!("missing parameter concept for {:?}", binding.name))?;
    // An explicitly present exact empty string is a value when a string binding
    // declares it as its literal default. A missing argument is handled by the
    // flag parser; whitespace is never silently turned into that default.
    let exact_empty_default = text.is_empty()
        && matches!(concept.value_domain, ParameterType::String)
        && matches!(&binding.default,
            DefaultSpec::Literal { value: ParameterValue::String(value) } if value.is_empty());
    if text.trim().is_empty() && !exact_empty_default {
        return Err(format!("parameter {:?} has no typed value", binding.name));
    }
    let parsed = parse_parameter_text(text, &concept.value_domain)
        .map_err(|error| format!("parameter {:?}: {error}", binding.name))?;
    checked_value(bundle, binding, &parsed)
}

/// `value` normalized and validated through `binding`'s concept and
/// refinements; a refusal names the parameter.
pub(crate) fn checked_value(
    bundle: &SurfaceContractBundle,
    binding: &SurfaceParameterBinding,
    value: &ParameterValue,
) -> Result<ParameterValue, String> {
    let named = |error: crate::NormalizationError| {
        let note = error.binding_note(binding);
        format!("parameter {:?}: {error}{note}", binding.name)
    };
    let concept = bundle
        .catalog
        .concept(&binding.concept)
        .ok_or_else(|| format!("missing parameter concept for {:?}", binding.name))?;
    let normalized = normalize_value(value, concept).map_err(named)?;
    validate_value(&normalized, concept, &binding.refinements).map_err(named)?;
    Ok(normalized)
}

fn is_bool_domain(domain: &ParameterType) -> bool {
    match domain {
        ParameterType::Bool => true,
        ParameterType::Optional { value, .. } => is_bool_domain(value),
        _ => false,
    }
}

/// Whether `domain` is a list with no fixed size, which a repeated flag
/// extends.
fn is_open_list_domain(domain: &ParameterType) -> bool {
    match domain {
        ParameterType::Array { max_items, .. } => max_items.is_none(),
        ParameterType::Optional { value, .. } => is_open_list_domain(value),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_provider_contracts::ParameterValue;

    #[test]
    fn a_request_is_read_from_a_file_or_reports_the_path() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(temp.path(), r#"{"surface": "imstat"}"#).unwrap();
        assert_eq!(
            read_task_request(temp.path().to_str().unwrap()).unwrap(),
            r#"{"surface": "imstat"}"#
        );
        let missing = read_task_request("/definitely/missing/request.json").unwrap_err();
        assert!(matches!(missing, TaskCliError::ReadFile { .. }));
        assert!(missing.to_string().contains("failed to read JSON request"));
    }

    #[test]
    fn catalog_cli_parser_accepts_only_declared_flags_and_normalizes_values() {
        let bundle = casa_provider_contracts::builtin_surface_bundle("tclean").unwrap();
        let patch = parse_parameter_cli_overrides(
            &bundle,
            &[
                "--ms".into(),
                "input.ms".into(),
                "--imsize".into(),
                "1024".into(),
                "--cell-arcsec=0.0002777777777777778deg".into(),
                "--gridder".into(),
                "awproject".into(),
                "--no-parallel".into(),
                "--gridprecision".into(),
                "f64".into(),
            ],
        )
        .unwrap();

        assert_eq!(
            patch.values["vis"],
            ParameterValue::Array(vec![ParameterValue::String("input.ms".into())])
        );
        assert_eq!(
            patch.values["imsize"],
            ParameterValue::Array(vec![ParameterValue::Integer(1024)])
        );
        assert_eq!(
            patch.values["cell"],
            ParameterValue::Array(vec![ParameterValue::String("1arcsec".into())])
        );
        assert_eq!(patch.values["parallel"], ParameterValue::Bool(false));
        assert_eq!(
            patch.values["gridprecision"],
            ParameterValue::String("f64".into())
        );

        // The parameter's own name reaches the same binding and value.
        let named =
            parse_parameter_cli_overrides(&bundle, &["--threshold".into(), "1Jy".into()]).unwrap();
        let declared =
            parse_parameter_cli_overrides(&bundle, &["--threshold-jy".into(), "1Jy".into()])
                .unwrap();
        assert_eq!(named, declared);
        let unknown =
            parse_parameter_cli_overrides(&bundle, &["--thresh".into(), "1Jy".into()]).unwrap_err();
        assert!(unknown.contains("unknown tclean parameter flag --thresh"));
    }

    #[test]
    fn exact_empty_cli_defaults_are_values_but_missing_or_blank_arguments_are_not() {
        let mut bundle = casa_provider_contracts::builtin_surface_bundle("tclean").unwrap();
        // Evidence-only exact-text concept: production declarations stay intact.
        for concept in &mut bundle.catalog.concepts {
            if matches!(
                concept.id.as_str(),
                "ms.selection.scan" | "ms.selection.observation"
            ) {
                concept.normalization = casa_provider_contracts::NormalizationRule::Identity;
            }
        }
        for name in ["scan", "observation"] {
            let flag = format!("--{name}");
            for arguments in [
                vec![flag.clone().into(), "".into()],
                vec![format!("{flag}=").into()],
            ] {
                let patch = parse_parameter_cli_overrides(&bundle, &arguments).unwrap();
                assert_eq!(patch.values[name], ParameterValue::String(String::new()));
            }
            assert!(parse_parameter_cli_overrides(&bundle, &[flag.clone().into()]).is_err());
            for text in [" ", "  ", "\t"] {
                assert!(
                    parse_parameter_cli_overrides(&bundle, &[flag.clone().into(), text.into()])
                        .is_err()
                );
            }
        }
        for name in ["vis", "imsize", "threshold"] {
            assert!(
                parse_parameter_cli_overrides(&bundle, &[format!("--{name}").into(), "".into()])
                    .is_err()
            );
        }
    }

    #[test]
    fn every_parameter_is_reachable_by_its_own_name() {
        let catalog = casa_provider_contracts::builtin_surface_catalog().unwrap();
        for surface in &catalog.surfaces {
            let bundle = casa_provider_contracts::builtin_surface_bundle(surface.id()).unwrap();
            for binding in bundle.surface.bindings() {
                let flag = format!("--{}", binding.name);
                let (found, _) = cli_flag_binding(&bundle, &flag)
                    .unwrap_or_else(|| panic!("{}: {flag} names nothing", surface.id()));
                assert_eq!(found.name, binding.name, "{}: {flag}", surface.id());
            }
        }

        let bundle = casa_provider_contracts::builtin_surface_bundle("applycal").unwrap();
        let patch = parse_parameter_cli_overrides(
            &bundle,
            &["--vis".into(), "target.ms".into(), "--no-flagbackup".into()],
        )
        .unwrap();
        assert_eq!(
            patch.values["vis"],
            ParameterValue::Array(vec![ParameterValue::String("target.ms".into())])
        );
        let declared =
            parse_parameter_cli_overrides(&bundle, &["--ms".into(), "target.ms".into()]).unwrap();
        assert_eq!(declared.values["vis"], patch.values["vis"]);
        assert_eq!(patch.values["flagbackup"], ParameterValue::Bool(false));
    }

    #[test]
    fn every_declared_spelling_reaches_its_parameter_short_ones_included() {
        // msexplore's `-o` was read as a positional before.
        let catalog = casa_provider_contracts::builtin_surface_catalog().unwrap();
        let mut short = 0;
        for surface in &catalog.surfaces {
            let bundle = casa_provider_contracts::builtin_surface_bundle(surface.id()).unwrap();
            for binding in bundle.surface.bindings() {
                let Some(cli) = binding.projections.cli.as_ref() else {
                    continue;
                };
                for flag in cli.flags.iter().chain(&cli.false_flags) {
                    let Some((found, _)) = cli_flag_binding(&bundle, flag) else {
                        panic!("{}: {flag} names nothing", surface.id());
                    };
                    assert_eq!(found.name, binding.name, "{}: {flag}", surface.id());
                    if !flag.starts_with("--") {
                        short += 1;
                    }
                }
            }
        }
        assert!(short > 0, "the catalog declares no short spelling to test");

        let bundle = casa_provider_contracts::builtin_surface_bundle("msexplore").unwrap();
        let patch = parse_parameter_cli_overrides(
            &bundle,
            &["-o".into(), "summary.txt".into(), "in.ms".into()],
        )
        .unwrap();
        assert_eq!(
            patch.values["output"],
            ParameterValue::String("summary.txt".into())
        );
        assert_eq!(
            patch.values["vis"],
            ParameterValue::Array(vec![ParameterValue::String("in.ms".into())])
        );
    }

    #[test]
    fn catalog_cli_parser_applies_unsets_in_argument_order() {
        let bundle = casa_provider_contracts::builtin_surface_bundle("tclean").unwrap();
        let patch = parse_parameter_cli_overrides(
            &bundle,
            &[
                "--niter".into(),
                "7".into(),
                "--unset".into(),
                "niter".into(),
                "--niter=9".into(),
            ],
        )
        .unwrap();
        assert_eq!(patch.values["niter"], ParameterValue::Integer(9));
        assert!(!patch.unset.contains("niter"));
    }

    #[test]
    fn cli_values_are_normalized_through_the_binding_concept() {
        let bundle = casa_provider_contracts::builtin_surface_bundle("msexplore").unwrap();
        let field = ensure_parameter_binding(&bundle, "field").unwrap();
        assert_eq!(
            validated_cli_value(&bundle, field, " 0, 2 ").unwrap(),
            ParameterValue::String("0,2".to_string())
        );
        assert!(
            validated_cli_value(&bundle, field, "  ")
                .unwrap_err()
                .contains("no typed value")
        );
        assert!(ensure_parameter_binding(&bundle, "not_a_parameter").is_err());
        // A value the concept refuses names its parameter.
        let imager = casa_provider_contracts::builtin_surface_bundle("tclean").unwrap();
        let imsize = ensure_parameter_binding(&imager, "imsize").unwrap();
        let refused = validated_cli_value(&imager, imsize, "abc").unwrap_err();
        assert!(refused.starts_with("parameter \"imsize\": "), "{refused}");
        let refused = checked_value(&imager, imsize, &ParameterValue::Bool(true)).unwrap_err();
        assert!(refused.starts_with("parameter \"imsize\": "), "{refused}");
    }
}
