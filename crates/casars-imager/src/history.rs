// SPDX-License-Identifier: LGPL-3.0-or-later
//! The history a finished `tclean` leaves in its images' `logtable`s:
//! `cleanhelper.write_tclean_history`.

use std::path::{Path, PathBuf};

use casa_images::image::add_image_history;
use casa_provider_contracts::{DefaultSpec, ParameterValue, python_repr};
use casa_task_runtime::TaskInvocation;

use crate::imager_task_schema_bundle;

/// Add the task's name, casa-rs's version and every parameter to the images
/// of prefix `imagename`. As in CASA's task, a failure is a warning: the run
/// has published its products.
pub(crate) fn write_tclean_history(imagename: &Path, task: &TaskInvocation<'_>) {
    let history = history_lines(|name| task.session().given(name).cloned());
    for path in image_paths(imagename) {
        if let Err(error) = add_image_history(&path, "tclean", &history) {
            tracing::warn!("updating the history of {}: {error}", path.display());
        }
    }
}

/// CASA's lines: the task, the version, and each parameter of tclean's in
/// order as the caller gave it, or its default, with casa-rs's own beside
/// CASA's where its surface has them.
fn history_lines(given: impl Fn(&str) -> Option<ParameterValue>) -> Vec<String> {
    let bundle = imager_task_schema_bundle();
    let mut bindings = bundle.parameter_surfaces[0]
        .surface
        .bindings()
        .iter()
        .collect::<Vec<_>>();
    bindings.sort_by_key(|binding| binding.order);
    let mut lines = vec![
        "taskname=tclean".to_owned(),
        format!("version: casa-rs {}", env!("CARGO_PKG_VERSION")),
    ];
    lines.extend(bindings.into_iter().filter_map(|binding| {
        let value = given(&binding.name).or_else(|| match &binding.default {
            DefaultSpec::Literal { value }
            | DefaultSpec::Conditional {
                fallback: value, ..
            } => Some(value.clone()),
            DefaultSpec::Required => None,
        })?;
        // `task_tclean` rewrites this local before the history reads it.
        let value = match value {
            ParameterValue::String(text) if binding.name == "specmode" && text == "cont" => {
                ParameterValue::String("mfs".to_owned())
            }
            value => value,
        };
        Some(format!("{:<11} = {}", binding.name, python_str(&value)))
    }));
    lines
}

/// `value` as Python's `str` writes it, a string in double quotes as
/// `write_task_history` adds them.
fn python_str(value: &ParameterValue) -> String {
    match value {
        ParameterValue::String(text) => format!("\"{text}\""),
        ParameterValue::Float(value) => python_float(*value),
        other => python_repr(other),
    }
}

/// `value` as Python's `str` writes it: its exponent signed, of two digits at
/// least (`1e-05`, where Rust writes `1e-5`).
fn python_float(value: f64) -> String {
    let text = format!("{value:?}");
    let Some((mantissa, exponent)) = text.split_once('e') else {
        return text;
    };
    let (sign, digits) = match exponent.strip_prefix('-') {
        Some(digits) => ('-', digits),
        None => ('+', exponent),
    };
    format!("{}e{sign}{digits:0>2}", mantissa.trim_end_matches(".0"))
}

/// `write_tclean_history`'s images: directories named by the prefix, an
/// optional number and `_number`, a dot and a suffix, holding a `logtable`,
/// and not one of its temporary or text suffixes.
fn image_paths(imagename: &Path) -> Vec<PathBuf> {
    // By text, as CASA's glob reads it: a trailing slash names no file, and
    // its images are the directory's own.
    let text = imagename.to_string_lossy();
    let (parent, prefix) = match text.rsplit_once('/') {
        Some(("", prefix)) => ("/", prefix),
        Some(split) => split,
        None => (".", text.as_ref()),
    };
    let Ok(entries) = std::fs::read_dir(parent) else {
        return Vec::new();
    };
    let mut paths = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| is_image_name(prefix, name))
                && path.join("logtable").join("table.info").is_file()
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

/// Whether `name` is `prefix`, `[0-9]*_?[0-9]*`, a dot and a suffix.
fn is_image_name(prefix: &str, name: &str) -> bool {
    const NOT_IMAGES: [&str; 5] = [".cf", ".cfcache", ".workdirectory", ".work.temp", ".txt"];
    if NOT_IMAGES.iter().any(|suffix| name.ends_with(suffix)) {
        return false;
    }
    let Some(rest) = name.strip_prefix(prefix) else {
        return false;
    };
    let rest = rest.trim_start_matches(|c: char| c.is_ascii_digit());
    let rest = rest.strip_prefix('_').unwrap_or(rest);
    let rest = rest.trim_start_matches(|c: char| c.is_ascii_digit());
    rest.strip_prefix('.')
        .is_some_and(|suffix| !suffix.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_names_follow_casas_pattern() {
        for name in [
            "im.image",
            "im.image.tt0",
            "im1.psf",
            "im_2.mask",
            "im1_2.pb",
        ] {
            assert!(is_image_name("im", name), "{name}");
        }
        for name in [
            "other.image",
            "imx.image",
            "im",
            "im.",
            "im.txt",
            "im.work.temp",
            "im.cf",
        ] {
            assert!(!is_image_name("im", name), "{name}");
        }
    }

    /// As `write_task_history` writes them (probed with CASA 6.7.6.14).
    #[test]
    fn values_are_written_as_casas_history_writes_them() {
        let text = |value: &str| ParameterValue::String(value.to_owned());
        assert_eq!(python_str(&text("0:0~7")), "\"0:0~7\"");
        assert_eq!(python_str(&ParameterValue::Bool(true)), "True");
        assert_eq!(python_str(&ParameterValue::Float(0.35)), "0.35");
        assert_eq!(python_str(&ParameterValue::Float(1e-5)), "1e-05");
        assert_eq!(python_str(&ParameterValue::Float(1.5e16)), "1.5e+16");
        assert_eq!(python_str(&ParameterValue::Integer(100)), "100");
        assert_eq!(
            python_str(&ParameterValue::Array(vec![
                ParameterValue::Integer(100),
                ParameterValue::Integer(100),
            ])),
            "[100, 100]"
        );
        assert_eq!(
            python_str(&ParameterValue::Array(vec![text("a"), text("b")])),
            "['a', 'b']"
        );
    }

    /// The caller's value as given, a default otherwise, in tclean's order.
    #[test]
    fn the_lines_start_with_the_task_and_keep_tclean_order() {
        let lines = history_lines(|name| match name {
            "vis" => Some(ParameterValue::String("a.ms".to_owned())),
            "niter" => Some(ParameterValue::Integer(10)),
            _ => None,
        });
        assert_eq!(lines[0], "taskname=tclean");
        assert!(lines[1].starts_with("version: casa-rs "));
        assert_eq!(lines[2], "vis         = \"a.ms\"");
        assert!(lines.contains(&"niter       = 10".to_owned()));
        assert!(lines.contains(&"spw         = \"\"".to_owned()));
    }

    /// `task_tclean` rewrites `specmode` before the history reads it.
    #[test]
    fn a_continuum_specmode_is_written_as_tcleans_mfs() {
        let lines = history_lines(|name| {
            (name == "specmode").then(|| ParameterValue::String("cont".to_owned()))
        });
        assert!(lines.contains(&"specmode    = \"mfs\"".to_owned()));
    }

    /// A trailing slash names the directory's own images, not its siblings.
    #[test]
    fn the_images_of_a_prefix_are_found_by_text() {
        let directory = tempfile::tempdir().expect("scratch");
        let image = |name: &str| {
            let logtable = directory.path().join(name).join("logtable");
            std::fs::create_dir_all(&logtable).expect("logtable");
            std::fs::write(logtable.join("table.info"), "").expect("table.info");
            directory.path().join(name)
        };
        let [ours, numbered] = [image("im.image"), image("im1_2.psf")];
        image("other.image");
        std::fs::create_dir_all(directory.path().join("im.nolog")).expect("not an image");
        assert_eq!(image_paths(&directory.path().join("im")), [ours, numbered]);
        let inside = image("im/.image");
        let mut slashed = directory.path().join("im").into_os_string();
        slashed.push("/");
        assert_eq!(image_paths(Path::new(&slashed)), [inside]);
    }
}
