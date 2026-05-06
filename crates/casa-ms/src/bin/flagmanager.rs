// SPDX-License-Identifier: LGPL-3.0-or-later
//! `flagmanager` - native CASA-style flag-version management.

use std::env;
use std::path::PathBuf;
use std::process;

use casa_ms::{
    FlagMerge, MeasurementSet, delete_flag_version, list_flag_versions, rename_flag_version,
    restore_flag_version, save_flag_version,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("Error: {error}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!("{}", usage());
        return Ok(());
    }
    let request = parse_args(&args)?;
    let mut ms = MeasurementSet::open(&request.vis).map_err(|error| error.to_string())?;
    let value = match request.mode.as_str() {
        "list" => serde_json::to_value(list_flag_versions(&ms).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?,
        "save" => {
            save_flag_version(
                &ms,
                request.versionname.as_deref().ok_or_else(usage)?,
                request.comment.as_deref().unwrap_or(""),
                request.merge,
            )
            .map_err(|error| error.to_string())?;
            serde_json::json!({"mode":"save","versionname":request.versionname})
        }
        "restore" => {
            restore_flag_version(
                &mut ms,
                request.versionname.as_deref().ok_or_else(usage)?,
                request.merge,
            )
            .map_err(|error| error.to_string())?;
            ms.save_main_table_only_assuming_valid()
                .map_err(|error| error.to_string())?;
            serde_json::json!({"mode":"restore","versionname":request.versionname})
        }
        "delete" => {
            delete_flag_version(&ms, request.versionname.as_deref().ok_or_else(usage)?)
                .map_err(|error| error.to_string())?;
            serde_json::json!({"mode":"delete","versionname":request.versionname})
        }
        "rename" => {
            rename_flag_version(
                &ms,
                request.oldname.as_deref().ok_or_else(usage)?,
                request.versionname.as_deref().ok_or_else(usage)?,
                request.comment.as_deref().unwrap_or(""),
            )
            .map_err(|error| error.to_string())?;
            serde_json::json!({"mode":"rename","oldname":request.oldname,"versionname":request.versionname})
        }
        other => return Err(format!("unsupported mode {other:?}\n{}", usage())),
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&value).map_err(|error| error.to_string())?
    );
    Ok(())
}

fn parse_args(args: &[String]) -> Result<Request, String> {
    let mut request = Request {
        vis: PathBuf::new(),
        mode: "list".to_string(),
        versionname: None,
        oldname: None,
        comment: None,
        merge: FlagMerge::Replace,
    };
    let mut index = 0usize;
    while index < args.len() {
        match args[index].as_str() {
            "--vis" | "--ms" => {
                index += 1;
                request.vis = PathBuf::from(args.get(index).ok_or_else(usage)?);
            }
            "--mode" => {
                index += 1;
                request.mode = args.get(index).ok_or_else(usage)?.to_ascii_lowercase();
            }
            "--versionname" => {
                index += 1;
                request.versionname = Some(args.get(index).ok_or_else(usage)?.clone());
            }
            "--oldname" => {
                index += 1;
                request.oldname = Some(args.get(index).ok_or_else(usage)?.clone());
            }
            "--comment" => {
                index += 1;
                request.comment = Some(args.get(index).ok_or_else(usage)?.clone());
            }
            "--merge" => {
                index += 1;
                request.merge = parse_merge(args.get(index).ok_or_else(usage)?)?;
            }
            other => return Err(format!("unknown argument {other:?}\n{}", usage())),
        }
        index += 1;
    }
    if request.vis.as_os_str().is_empty() {
        return Err(usage());
    }
    Ok(request)
}

fn parse_merge(value: &str) -> Result<FlagMerge, String> {
    match value.to_ascii_lowercase().as_str() {
        "replace" => Ok(FlagMerge::Replace),
        "or" => Ok(FlagMerge::Or),
        "and" => Ok(FlagMerge::And),
        other => Err(format!("unsupported merge {other:?}")),
    }
}

fn usage() -> String {
    "usage: flagmanager --vis <ms> --mode list|save|restore|delete|rename [--versionname <name>] [--oldname <name>] [--comment <text>] [--merge replace|or|and]".to_string()
}

struct Request {
    vis: PathBuf,
    mode: String,
    versionname: Option<String>,
    oldname: Option<String>,
    comment: Option<String>,
    merge: FlagMerge,
}
