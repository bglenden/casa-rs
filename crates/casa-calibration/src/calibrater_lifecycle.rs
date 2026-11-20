// SPDX-License-Identifier: LGPL-3.0-or-later
//! Attachment and visibility-equation state shared by calibrater operations.

use std::path::Path;

use casa_ms::MsSelection;

use crate::attachment::OpenedMs;

use crate::{
    ApplyCalibrationTableSpec, AttachmentError, AttachmentMode, AttachmentOptions,
    CalibrationAttachment, CalibrationDataset, CalibrationError, CalibrationOutputMode,
    CalibrationSolveRequest, CalibrationSolveResult, GainSolveModelSource, point_model_stokes,
    reset_calibration_set, solve_calibration_with_output_mode,
};

/// State shared by the calibrater's model, apply and solve operations.
#[derive(Default)]
pub struct CalibraterLifecycle {
    legacy_iterator: bool,
    equation: Option<VisibilityEquation>,
}

struct VisibilityEquation {
    attachment: CalibrationAttachment,
    model: CalibraterModel,
    apply: ApplyTerms,
    solve: Option<(CalibrationSolveRequest, CalibrationOutputMode)>,
}

#[derive(Default)]
struct ApplyTerms {
    tables: Vec<ApplyCalibrationTableSpec>,
    parang: bool,
}

#[derive(Clone, Copy, Default)]
enum CalibraterModel {
    #[default]
    FromMeasurementSet,
    Point([f32; 4]),
}

/// A stateful calibrater operation failed.
#[derive(Debug, thiserror::Error)]
pub enum CalibraterLifecycleError {
    /// The operation needs an attached MeasurementSet.
    #[error("calibrater has no open MeasurementSet")]
    Unattached,
    /// No solver is configured in the current visibility equation.
    #[error("Please run setsolve before attempting to solve.")]
    NoSolver,
    /// A point model must contain at most four finite CASA Stokes values.
    #[error("a point model requires up to four finite Stokes values")]
    InvalidPointModel,
    /// MeasurementSet attachment or visibility selection failed.
    #[error(transparent)]
    Attachment(#[from] AttachmentError),
    /// The configured solver could not complete.
    #[error(transparent)]
    Solve(#[from] CalibrationError),
}

impl CalibraterLifecycle {
    /// Choose the visibility iterator before opening a MeasurementSet.
    ///
    /// Returns false when a MeasurementSet is already attached.
    #[must_use]
    pub fn set_legacy_iterator(&mut self, legacy: bool) -> bool {
        if self.equation.is_some() {
            return false;
        }
        self.legacy_iterator = legacy;
        true
    }

    /// Whether the calibrater is configured to open the legacy iterator.
    #[must_use]
    pub fn legacy_iterator(&self) -> bool {
        self.legacy_iterator
    }

    /// Attach a MeasurementSet and start a new visibility equation.
    ///
    /// The equation reads its model from the attached MeasurementSet until an
    /// explicit point model is installed.
    ///
    /// # Errors
    ///
    /// Returns an error when the MeasurementSet cannot be opened.
    pub fn open(&mut self, path: &Path) -> Result<(), CalibraterLifecycleError> {
        let attachment = CalibrationAttachment::attach(
            path,
            AttachmentMode::Open,
            AttachmentOptions::default(),
            MsSelection::default(),
        )?;
        self.equation = Some(VisibilityEquation {
            attachment,
            model: CalibraterModel::FromMeasurementSet,
            apply: ApplyTerms::default(),
            solve: None,
        });
        Ok(())
    }

    /// The path of the currently attached MeasurementSet.
    #[must_use]
    pub fn dataset_path(&self) -> Option<&Path> {
        self.equation
            .as_ref()
            .map(|equation| equation.attachment.path())
    }

    /// Release the current equation and attachment. The iterator choice is
    /// retained for the next call to `open`.
    pub fn close(&mut self) {
        self.equation = None;
    }

    /// Replace the selected rows and channels and reset apply and solve.
    ///
    /// A successful selection keeps the installed point model. A selection
    /// error reinitializes the equation over the same MeasurementSet.
    ///
    /// # Errors
    ///
    /// Returns an error without an open dataset or if a selector fails.
    pub fn select(&mut self, selection: MsSelection) -> Result<(), CalibraterLifecycleError> {
        let equation = self.equation_mut()?;
        let selected = equation.attachment.select(selection);
        equation.apply = ApplyTerms::default();
        equation.solve = None;
        if let Err(error) = selected {
            equation.attachment.reset_selection();
            equation.model = CalibraterModel::FromMeasurementSet;
            return Err(error.into());
        }
        Ok(())
    }

    /// Install an explicit constant Stokes model. Empty and four-zero
    /// vectors both install zero flux.
    ///
    /// # Errors
    ///
    /// Returns an error without an open dataset or for an invalid vector.
    pub fn set_point_model(&mut self, stokes: &[f64]) -> Result<(), CalibraterLifecycleError> {
        let equation = self.equation_mut()?;
        let stokes =
            point_model_stokes(stokes).ok_or(CalibraterLifecycleError::InvalidPointModel)?;
        equation.model = CalibraterModel::Point(stokes);
        Ok(())
    }

    /// Append an applied calibration term in call order.
    ///
    /// # Errors
    ///
    /// Returns an error without an open dataset.
    pub fn add_apply(
        &mut self,
        apply: ApplyCalibrationTableSpec,
    ) -> Result<(), CalibraterLifecycleError> {
        self.equation_mut()?.apply.tables.push(apply);
        Ok(())
    }

    /// Set the parallactic-angle apply term in the current equation.
    ///
    /// # Errors
    ///
    /// Returns an error without an open dataset.
    pub fn set_parallactic_angle(&mut self, apply: bool) -> Result<(), CalibraterLifecycleError> {
        self.equation_mut()?.apply.parang = apply;
        Ok(())
    }

    /// Reset the selected scratch calibration columns with the iterator
    /// that opened this MeasurementSet.
    ///
    /// # Errors
    ///
    /// Returns an error without an open dataset, when the iterator does not
    /// support the operation, or when the scratch update fails.
    pub fn init_cal_set(&self, calset: i64) -> Result<(), CalibraterLifecycleError> {
        let equation = self.equation()?;
        let mut opened =
            OpenedMs::open(equation.attachment.path()).map_err(AttachmentError::from)?;
        opened.lock = reset_calibration_set(
            &mut opened.measurement_set,
            equation.attachment.selection(),
            calset,
            self.legacy_iterator,
            opened.lock,
        )
        .map_err(AttachmentError::from)?;
        opened.release().map_err(AttachmentError::from)?;
        Ok(())
    }

    /// Configure the solver for the current visibility equation.
    ///
    /// # Errors
    ///
    /// Returns an error without an open dataset.
    pub fn set_solve(
        &mut self,
        request: CalibrationSolveRequest,
        mode: CalibrationOutputMode,
    ) -> Result<(), CalibraterLifecycleError> {
        self.equation_mut()?.solve = Some((request, mode));
        Ok(())
    }

    /// Clear the requested terms in the current visibility equation.
    ///
    /// # Errors
    ///
    /// Returns an error without an open dataset.
    pub fn reset(&mut self, apply: bool, solve: bool) -> Result<(), CalibraterLifecycleError> {
        let equation = self.equation_mut()?;
        if apply {
            equation.apply = ApplyTerms::default();
        }
        if solve {
            equation.solve = None;
        }
        Ok(())
    }

    /// Run the configured solver using this equation's model, selection and
    /// prior calibration terms.
    ///
    /// # Errors
    ///
    /// Returns an error without an open dataset or configured solver, or
    /// when the solve fails.
    pub fn solve(&self) -> Result<CalibrationSolveResult, CalibraterLifecycleError> {
        let equation = self.equation()?;
        let (mut request, mode) = equation
            .solve
            .clone()
            .ok_or(CalibraterLifecycleError::NoSolver)?;
        let (model_source, stokes) = match equation.model {
            CalibraterModel::FromMeasurementSet => {
                (GainSolveModelSource::FromMeasurementSet, [1., 0., 0., 0.])
            }
            CalibraterModel::Point(stokes) => (GainSolveModelSource::PointSource, stokes),
        };
        let selection = equation.attachment.selection().clone();
        let apply = equation.apply.tables.clone();
        match &mut request {
            CalibrationSolveRequest::Gain(gain) => {
                gain.selection = selection;
                gain.model_source = model_source;
                gain.smodel = stokes;
                gain.prior_calibration_tables = apply;
                gain.parang = equation.apply.parang;
            }
            CalibrationSolveRequest::Bandpass(bandpass) => {
                bandpass.selection = selection;
                bandpass.model_source = model_source;
                bandpass.smodel = stokes;
                bandpass.prior_calibration_tables = apply;
                bandpass.parang = equation.apply.parang;
            }
        }
        Ok(solve_calibration_with_output_mode(
            CalibrationDataset::path(equation.attachment.path()),
            request,
            mode,
        )?)
    }

    fn equation(&self) -> Result<&VisibilityEquation, CalibraterLifecycleError> {
        self.equation
            .as_ref()
            .ok_or(CalibraterLifecycleError::Unattached)
    }

    fn equation_mut(&mut self) -> Result<&mut VisibilityEquation, CalibraterLifecycleError> {
        self.equation
            .as_mut()
            .ok_or(CalibraterLifecycleError::Unattached)
    }
}
