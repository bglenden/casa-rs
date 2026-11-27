// SPDX-License-Identifier: LGPL-3.0-or-later
//! Weight initialization on the calibrater's attached MeasurementSet.

use std::path::Path;

use casa_ms::MsError;
use casa_tables::{Table, TableOptions};

use crate::apply::{ApplyCalibrationTableSpec, ApplyInterpolationMode, GainFieldSelector};
use crate::initweights::{Mode, initialize, initialize_tsys};
use crate::weight_spectra::{SIGMA_SPECTRUM, WEIGHT_SPECTRUM, has_column};

use super::{
    ApplyTerms, CalibraterLifecycle, CalibraterLifecycleError, OpenedMs, VisibilityEquation,
};

impl CalibraterLifecycle {
    /// Initialize weights across the attached MS using the shared task kernels.
    /// Ordinary modes retain the selection. A validated Tsys table resets it.
    ///
    /// # Errors
    /// The tool is unattached, a weight setting is invalid, or a table operation fails.
    pub fn init_weights(
        &mut self,
        wtmode: &str,
        dowtsp: bool,
        tsystable: &Path,
        gainfield: &str,
        interp: &str,
        spwmap: Vec<i32>,
    ) -> Result<(), CalibraterLifecycleError> {
        let equation = self.equation_mut()?;
        let result = if wtmode.contains("tsys") {
            initialize_with_tsys(
                equation, wtmode, dowtsp, tsystable, gainfield, interp, spwmap,
            )
        } else {
            Mode::from_name(wtmode)
                .ok_or_else(|| {
                    MsError::InvalidInput(format!("Unrecognized wtmode specified: {wtmode}"))
                })
                .and_then(|mode| {
                    initialize_attachment(
                        equation.attachment.path(),
                        |table| has_column(table, mode.removed_column()),
                        |opened| {
                            initialize(&mut opened.measurement_set, mode, dowtsp, &mut opened.lock)
                        },
                    )
                })
        };
        if result.is_err() {
            equation.apply = ApplyTerms::default();
        }
        result.map_err(CalibraterLifecycleError::InitWeights)
    }
}

fn initialize_with_tsys(
    equation: &mut VisibilityEquation,
    wtmode: &str,
    dowtsp: bool,
    path: &Path,
    gainfield: &str,
    interp: &str,
    spwmap: Vec<i32>,
) -> Result<(), MsError> {
    let info = Table::open(TableOptions::new(path))?.info().clone();
    if info.table_type != "Calibration" || !info.sub_type.contains("TSYS") {
        return Err(MsError::InvalidInput(
            "Invalid calibration table type for Tsys weighting.".into(),
        ));
    }
    equation.attachment.reset_selection();
    equation.apply = ApplyTerms::default();
    let by_exposure = match wtmode {
        "tsys" => false,
        "tinttsys" => true,
        _ => {
            return Err(MsError::InvalidInput(format!(
                "Unrecognized wtmode specified: {wtmode}"
            )));
        }
    };
    let mut spec = ApplyCalibrationTableSpec::new(path.to_owned());
    spec.gainfield = GainFieldSelector::parse_casa(gainfield);
    spec.interp = ApplyInterpolationMode::parse_casa(interp).map_err(MsError::InvalidInput)?;
    spec.spwmap = spwmap;
    initialize_attachment(
        equation.attachment.path(),
        |table| !dowtsp && !has_column(table, WEIGHT_SPECTRUM) && has_column(table, SIGMA_SPECTRUM),
        |opened| {
            initialize_tsys(
                &mut opened.measurement_set,
                spec.clone(),
                by_exposure,
                dowtsp,
                &mut opened.lock,
            )
        },
    )
}

fn initialize_attachment(
    path: &Path,
    removes_column: impl Fn(&Table) -> bool,
    update: impl Fn(&mut OpenedMs) -> Result<(), MsError>,
) -> Result<(), MsError> {
    let mut opened = OpenedMs::open(path)?;
    let table = opened.measurement_set.main_table();
    if table.info().sub_type != "CONCATENATED" {
        update(&mut opened)?;
        return opened.release();
    }

    let parts = Table::concatenated_source_paths(path)?
        .ok_or_else(|| MsError::InvalidInput("Multi-MS has no concatenation".into()))?;
    let reject_removal = |table: &Table| {
        if removes_column(table) {
            Err(MsError::InvalidInput(
                "ConcatTable cannot remove columns".into(),
            ))
        } else {
            Ok(())
        }
    };
    reject_removal(table)?;
    for part in parts {
        let mut member = OpenedMs::open(&part)?;
        reject_removal(member.measurement_set.main_table())?;
        update(&mut member)?;
        member.release()?;
    }
    opened.release()
}
