// SPDX-License-Identifier: LGPL-3.0-or-later
//! The base source, overrides and lifecycle controls of one task command line,
//! shared by the task front end and `casars run`.

use std::ffi::OsString;
use std::path::PathBuf;

use casa_provider_contracts::SurfaceContractBundle;

use crate::{BaseSource, OpenSessionRequest, ResolutionPatch, parse_parameter_cli_overrides};

/// The parsed source, overrides and lifecycle controls of one task command
/// line.
///
/// Resolution order is current defaults, then the one base source, then the
/// overrides; choosing another base source replaces the base rather than
/// merging into it.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskCommandLine {
    /// The one base source: `--defaults` (the default), `--last`,
    /// `--last-successful` or `--params FILE`.
    pub source: BaseSource,
    /// `--workspace DIR`: where relative paths resolve and Last lives; the
    /// current directory when absent.
    pub workspace: PathBuf,
    /// The catalog-declared flags and positionals, and `--unset NAME`, as one
    /// highest-precedence patch.
    pub overrides: ResolutionPatch,
    /// `--save-params FILE`: also write the resolved sparse profile here.
    pub save_params: Option<PathBuf>,
    /// `--no-save-last`: leave Last and Last Successful untouched.
    pub no_save_last: bool,
}

impl TaskCommandLine {
    /// Parse `args` against `bundle`'s declared flags.
    ///
    /// Every argument that is not a source or lifecycle control must be a flag
    /// or positional the surface declares; anything else is an error naming
    /// it.
    pub fn parse(bundle: &SurfaceContractBundle, args: &[OsString]) -> Result<Self, String> {
        let mut source = None;
        let mut workspace = None;
        let mut save_params = None;
        let mut no_save_last = false;
        let mut parameter_args = Vec::new();
        let mut index = 0usize;
        while index < args.len() {
            let raw = args[index]
                .to_str()
                .ok_or_else(|| "parameter arguments must be valid UTF-8".to_string())?;
            match raw {
                "--defaults" => set_source(&mut source, BaseSource::Defaults)?,
                "--last" => set_source(&mut source, BaseSource::Last)?,
                "--last-successful" => set_source(&mut source, BaseSource::LastSuccessful)?,
                "--params" => {
                    index += 1;
                    let path = required_path(args.get(index), "--params file")?;
                    set_source(&mut source, BaseSource::File(path))?;
                }
                "--workspace" => {
                    index += 1;
                    workspace = Some(required_path(args.get(index), "--workspace directory")?);
                }
                "--save-params" => {
                    index += 1;
                    save_params = Some(required_path(args.get(index), "--save-params file")?);
                }
                "--no-save-last" => no_save_last = true,
                _ => parameter_args.push(args[index].clone()),
            }
            index += 1;
        }
        let overrides = parse_parameter_cli_overrides(bundle, &parameter_args)?;
        let workspace = workspace
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
        Ok(Self {
            source: source.unwrap_or(BaseSource::Defaults),
            workspace,
            overrides,
            save_params,
            no_save_last,
        })
    }

    /// The session request this command line describes for `bundle`.
    pub fn open_session_request(&self, bundle: SurfaceContractBundle) -> OpenSessionRequest {
        OpenSessionRequest {
            bundle,
            workspace: self.workspace.clone(),
            source: self.source.clone(),
            profile_text: None,
            context_patch: ResolutionPatch::default(),
            override_patch: self.overrides.clone(),
            managed_save: !self.no_save_last,
        }
    }
}

fn set_source(target: &mut Option<BaseSource>, source: BaseSource) -> Result<(), String> {
    if target.is_some() {
        return Err(
            "base sources are mutually exclusive: choose exactly one of Defaults, Last, Last Successful, or --params"
                .to_string(),
        );
    }
    *target = Some(source);
    Ok(())
}

pub(crate) fn required_path(value: Option<&OsString>, label: &str) -> Result<PathBuf, String> {
    value
        .map(PathBuf::from)
        .ok_or_else(|| format!("missing {label}"))
}

/// The SOURCE, OVERRIDES and CONTROLS help shared by every task command line.
pub fn task_command_line_help() -> &'static str {
    "SOURCE is exactly one of --defaults (the default), --last, --last-successful, or --params FILE.\n\
OVERRIDES use the flags the task declares, such as --vis or --imsize, or --unset NAME.\n\
CONTROLS: --workspace DIR (where relative paths resolve and Last and Last Successful live),\n\
  --save-params FILE, --no-save-last. CASA's parameters are the consent: overwrite=true\n\
  replaces a product.\n"
}

#[cfg(test)]
mod tests {
    use casa_provider_contracts::{ParameterValue, builtin_surface_bundle};

    use super::*;
    use crate::ParameterRuntime;

    #[test]
    fn source_options_are_mutually_exclusive() {
        let bundle = builtin_surface_bundle("imager").unwrap();
        let error =
            TaskCommandLine::parse(&bundle, &["--defaults".into(), "--last".into()]).unwrap_err();
        assert!(error.contains("mutually exclusive"));
    }

    #[test]
    fn lifecycle_controls_are_separated_from_parameter_overrides() {
        let bundle = builtin_surface_bundle("imstat").unwrap();
        let line = TaskCommandLine::parse(
            &bundle,
            &[
                "input.image".into(),
                "--save-params".into(),
                "saved.toml".into(),
                "--no-save-last".into(),
                "--workspace".into(),
                "work".into(),
            ],
        )
        .unwrap();
        assert_eq!(line.save_params, Some(PathBuf::from("saved.toml")));
        assert!(line.no_save_last);
        assert_eq!(line.workspace, PathBuf::from("work"));
        let session = ParameterRuntime::default()
            .open_session(line.open_session_request(bundle))
            .unwrap();
        assert_eq!(
            session.values()["imagename"],
            ParameterValue::Array(vec![ParameterValue::String("input.image".into())])
        );
    }

    #[test]
    fn undeclared_flags_are_refused() {
        let bundle = builtin_surface_bundle("imstat").unwrap();
        for flag in ["--notebook", "--confirm-overwrite"] {
            let error =
                TaskCommandLine::parse(&bundle, &["input.image".into(), flag.into()]).unwrap_err();
            assert!(error.contains(flag), "{error}");
        }
    }
}
