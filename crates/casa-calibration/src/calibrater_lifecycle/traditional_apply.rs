// SPDX-License-Identifier: LGPL-3.0-or-later
//! Direct tool registration of traditional calibration tables.

use std::path::{Path, PathBuf};

use casa_core::Value;
use casa_ms::MeasurementSet;
use casa_tables::{Table, TableOptions, add_directory, strip_directory};

use super::{CalibraterLifecycle, CalibraterLifecycleError};
use crate::constants::{STANDARD_SUBTABLE_KEYWORDS, TABLE_INFO_TYPE};
use crate::plan::{ApplyCalibrationTableSpec, ApplyInterpolationMode, GainFieldSelector};
use crate::summarize_table;

pub(super) struct RegisteredApplySnapshot {
    pub(super) name: String,
    pub(super) subtype: String,
    pub(super) description: String,
    _directory: tempfile::TempDir,
}

pub(super) struct TablelessOpacity {
    pub(super) subtype: &'static str,
    pub(super) index: usize,
    pub(super) values: Vec<f64>,
}

impl CalibraterLifecycle {
    /// Register the obsolete table-free opacity vector accepted by CASA's
    /// TOpac and TfOpac calibrater factories. Its apply path requires a table.
    ///
    /// # Errors
    /// The tool has no attached MS or the supplied values do not specify opacity.
    pub fn set_tableless_opacity(
        &mut self,
        kind: &str,
        mut opacity: Vec<f64>,
    ) -> Result<(), CalibraterLifecycleError> {
        let equation = self.equation_mut()?;
        let invalid = || {
            CalibraterLifecycleError::ApplyConfiguration("Error in Calibrater::setapply.".into())
        };
        let subtype = calibration_factory_type(kind)
            .filter(|kind| matches!(*kind, "TOpac" | "TfOpac"))
            .ok_or_else(invalid)?;
        let last = opacity.last().copied().ok_or_else(invalid)?;
        if opacity.iter().sum::<f64>().partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater) {
            return Err(invalid());
        }
        let windows = if let Some(count) = equation.opacity_spectral_windows {
            count
        } else {
            let ms = MeasurementSet::open(equation.attachment.path()).map_err(|_| invalid())?;
            let count = ms.spectral_window().map_err(|_| invalid())?.row_count();
            equation.opacity_spectral_windows = Some(count);
            count
        };
        if opacity.len() < windows {
            opacity.resize(windows, last);
        }
        equation.apply.tableless_opacity.push(TablelessOpacity {
            subtype,
            index: equation.apply.tables.len(),
            values: opacity,
        });
        Ok(())
    }

    /// Configure a traditional calibration table or the parallactic-angle
    /// term. Keep the current chain when a new term cannot initialize.
    /// # Errors
    /// Returns an error if there is no attached MeasurementSet, the type
    /// and table disagree, or the interpolation or mappings are invalid.
    pub fn set_traditional_apply(
        &mut self,
        mut spec: ApplyCalibrationTableSpec,
        kind: &str,
        gainfield: &str,
        interpolation: &str,
    ) -> Result<(), CalibraterLifecycleError> {
        let equation = self.equation_mut()?;
        let generic_error = || {
            CalibraterLifecycleError::ApplyConfiguration("Error in Calibrater::setapply.".into())
        };
        if calibration_factory_type(kind) == Some("P Jones") {
            equation.apply.parang_terms += 1;
            equation.apply.parang_calwt |= spec.calwt;
            return Ok(());
        }
        let path = spec.path.display();
        let inferred = kind.is_empty();
        let table = open_apply_table(&spec.path).map_err(|_| {
            if inferred {
                CalibraterLifecycleError::ApplyConfiguration(format!(
                    "Table {path} does not exist."
                ))
            } else {
                generic_error()
            }
        })?;
        let info = table.info();
        if info.table_type != TABLE_INFO_TYPE {
            return Err(if inferred {
                non_calibration_table(&path.to_string(), &info.table_type)
            } else {
                generic_error()
            });
        }
        let requested = if inferred { &info.sub_type } else { kind };
        let subtype = calibration_factory_type(requested).ok_or_else(generic_error)?;
        if calibration_factory_type(&info.sub_type) != Some(subtype) {
            return Err(generic_error());
        }
        if subtype == "P Jones" {
            equation.apply.parang_terms += 1;
            equation.apply.parang_calwt |= spec.calwt;
            return Ok(());
        }
        if subtype == "X Mueller" {
            spec.calwt = false;
        }

        let legacy_polynomial = matches!(subtype, "BPOLY" | "GSPLINE");
        if !legacy_polynomial {
            spec.interp =
                ApplyInterpolationMode::parse_casa(interpolation).map_err(|_| generic_error())?;
            spec.gainfield = GainFieldSelector::parse_casa(gainfield);
            let ms =
                MeasurementSet::open(equation.attachment.path()).map_err(|_| generic_error())?;
            let windows = ms
                .spectral_window()
                .map_err(|_| generic_error())?
                .row_count();
            if spec.spwmap.len() > windows {
                return Err(generic_error());
            }
            if !spec.spwmap.is_empty() && spec.spwmap != [-1] && spec.spwmap[0] != -999 {
                let calibration_windows =
                    Table::open(TableOptions::new(spec.path.join("SPECTRAL_WINDOW")))
                        .map_err(|_| generic_error())?
                        .row_count();
                if spec
                    .spwmap
                    .iter()
                    .any(|&spw| spw < 0 || spw as usize >= calibration_windows)
                {
                    return Err(generic_error());
                }
            }
            if spec.gainfield.is_some() {
                let summary = summarize_table(&spec.path).map_err(|_| generic_error())?;
                crate::plan::resolve_gainfield(&ms, &spec, &summary, &[])
                    .map_err(|_| generic_error())?;
            }
        }
        let (snapshot_path, mut snapshot) = snapshot_table(&spec.path, &table).map_err(|error| {
            tracing::error!(%error, table = %spec.path.display(), "could not retain registered calibration table");
            generic_error()
        })?;
        snapshot.subtype = subtype.into();
        snapshot.description = super::state::traditional_apply_description(
            subtype,
            &snapshot.name,
            interpolation,
            &spec.spwmap,
            spec.calwt,
        );
        spec.path = snapshot_path.clone();
        equation
            .apply
            .table_snapshots
            .insert(snapshot_path, snapshot);
        equation.apply.tables.push(spec);
        Ok(())
    }
}

pub(super) fn open_apply_table(path: &Path) -> Result<Table, casa_tables::TableError> {
    let options = TableOptions::new(path);
    #[cfg(unix)]
    return Table::open_with_lock(
        options,
        casa_tables::LockOptions::new(casa_tables::LockMode::PermanentLockingWait).read_only(true),
    );
    #[cfg(not(unix))]
    Table::open(options)
}

pub(super) fn snapshot_table(
    source: &Path,
    table: &Table,
) -> Result<(PathBuf, RegisteredApplySnapshot), Box<dyn std::error::Error>> {
    let directory = tempfile::Builder::new().prefix("calibrater-").tempdir()?;
    let destination = directory.path().canonicalize()?.join("caltable");
    // Copy the on-disk managers by value. Materialize a selected reference
    // table so future writes to its plain parent cannot change its solutions.
    if table.is_reference_table()? {
        table.to_memory()?.save(TableOptions::new(&destination))?;
    } else {
        crate::paths::copy_editable_table_tree(source, &destination)?;
    }
    let owner = crate::paths::subtable_owner(table, source)?;
    let mut external = Vec::new();
    for name in STANDARD_SUBTABLE_KEYWORDS {
        let Some(stored) = table.keywords().get(name).and_then(Value::as_table_ref) else {
            continue;
        };
        let candidate = add_directory(stored, &destination);
        if candidate
            .canonicalize()
            .is_ok_and(|path| path.starts_with(&destination))
        {
            continue;
        }
        let original = add_directory(stored, &owner);
        if original.is_dir() {
            let copied = destination.join("linked").join(name);
            crate::paths::copy_editable_table_tree(&original, &copied)?;
            external.push((
                *name,
                Value::table_ref(strip_directory(&copied, &destination)),
            ));
        }
    }
    if !external.is_empty() {
        let mut copied = Table::open_metadata_only(TableOptions::new(&destination))?;
        for (name, reference) in external {
            copied.keywords_mut().upsert(name, reference);
        }
        copied.save_metadata_only(TableOptions::new(&destination))?;
    }
    Ok((
        destination,
        RegisteredApplySnapshot {
            name: source.display().to_string(),
            subtype: table.info().sub_type.to_string(),
            description: String::new(),
            _directory: directory,
        },
    ))
}

pub(super) fn non_calibration_table(name: &str, kind: &str) -> CalibraterLifecycleError {
    CalibraterLifecycleError::ApplyConfiguration(format!(
        "Table {name} is not a valid Calibration table. (expected type = \"Calibration\"; type found = \"{kind}\")"
    ))
}

/// Canonical types of the metadata-aware CASA VisCal factory.
pub(super) fn calibration_factory_type(kind: &str) -> Option<&'static str> {
    let name = kind.to_ascii_uppercase();
    let prefix = name
        .split_once('+')
        .map_or(name.as_str(), |(prefix, _)| prefix);
    Some(match name.as_str() {
        "P" | "P JONES" => "P Jones",
        "B" | "B JONES" => "B Jones",
        "G" | "G JONES" => "G Jones",
        "BPOLY" => "BPOLY",
        "GSPLINE" => "GSPLINE",
        "TF" | "TF JONES" | "TF NOISE" => "Tf Jones",
        "T" | "T JONES" | "T NOISE" => "T Jones",
        "D JONES" => "D Jones",
        "DF JONES" => "Df Jones",
        "DLLS JONES" => "DLLS Jones",
        "DFLLS JONES" => "DfLLS Jones",
        "DGEN JONES" => "Dgen Jones",
        "DFGEN JONES" => "Dfgen Jones",
        "J" | "J JONES" => "J Jones",
        "JF" | "JF JONES" => "Jf Jones",
        "M" | "M MUELLER" => "M Mueller",
        "A" | "A MUELLER" => "A Mueller",
        "N" | "A NOISE" => "A Noise",
        "MF" | "MF MUELLER" => "Mf Mueller",
        "X" | "X MUELLER" => "X Mueller",
        "XJ" | "X JONES" => "X Jones",
        "XF" | "XF JONES" => "Xf Jones",
        "K" | "K JONES" => "K Jones",
        "KCROSS" | "KCROSS JONES" => "Kcross Jones",
        "GLINXPH" | "GLINXPH JONES" | "XY+QU" => "GlinXph Jones",
        "GLINXPHF" | "GLINXPHF JONES" | "XYF+QU" => "GlinXphf Jones",
        "XPARANG" | "XPARANG JONES" | "XPARANG+QU" => "Xparang Jones",
        "XFPARANG" | "XFPARANG JONES" | "XFPARANG+QU" => "Xfparang Jones",
        "POSANG" | "POSANG JONES" => "PosAng Jones",
        "KMBD" | "KMBD JONES" => "KMBD Jones",
        "TOPAC" => "TOpac",
        "TFOPAC" => "TfOpac",
        _ => match prefix {
            "D" | "DLIN" => "D Jones",
            "DF" | "DFLIN" => "Df Jones",
            "DLLS" => "DLLS Jones",
            "DFLLS" => "DfLLS Jones",
            "DGEN" | "DGENERAL" => "Dgen Jones",
            "DFGEN" | "DFGENERAL" => "Dfgen Jones",
            _ => {
                [
                    ("KANTPOS", "KAntPos Jones"),
                    ("TSYS", "B TSYS"),
                    ("EVLASWP", "G EVLASWPOW"),
                    ("GAINCURVE", "EGainCurve"),
                    ("POWERCURVE", "EPowerCurve"),
                    ("TEC", "F Jones"),
                    ("F JONES", "F Jones"),
                    ("FRINGE", "Fringe Jones"),
                    ("ACCOR", "Accor Jones"),
                    ("SDSKY_PS", "SDSKY_PS"),
                    ("SDSKY_RASTER", "SDSKY_RASTER"),
                    ("SDGAIN_OTFD", "SDGAIN_OTFD"),
                ]
                .into_iter()
                .find(|(pattern, _)| name.contains(pattern))?
                .1
            }
        },
    })
}
