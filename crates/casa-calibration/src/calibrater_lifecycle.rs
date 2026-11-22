// SPDX-License-Identifier: LGPL-3.0-or-later
//! Attachment and visibility-equation state shared by calibrater operations.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use casa_components::ComponentList;
use casa_ms::{MeasurementSet, MsSelection, SubtableId};
use casa_provider_contracts::CasaValue;
use casa_tables::RequiredScalarColumnValues;
use casa_types::{ScalarValue, Value};

use crate::attachment::OpenedMs;
use crate::bandpass::BandpassSolveControls;
use crate::execute::{ApplyTermCounts, LibraryApplyRegistration};
use crate::listing::{CalibrationListingReport, list_calibration, publish_listing};

use crate::{
    ApplyCalibrationTableSpec, AttachmentError, AttachmentMode, AttachmentOptions,
    CalibrationAttachment, CalibrationDataset, CalibrationError, CalibrationOutputMode,
    CalibrationSolveRequest, CalibrationSolveResult, GainNormalization, GainSolveMode,
    GainSolveModelSource, VisibilityFitRequest, delete_calibration_model, fit_visibility_component,
    point_model_stokes, reset_calibration_set,
};

mod apply;
pub(crate) mod ep_cache;
mod leakage;
pub(crate) mod legacy_ep;
mod state;
mod table_tools;
mod traditional_apply;
mod weights;

use legacy_ep::LegacyEpSolver;

/// State shared by the calibrater's model, apply and solve operations.
#[derive(Default)]
pub struct CalibraterLifecycle {
    legacy_iterator: bool,
    equation: Option<VisibilityEquation>,
    last_activity: Option<CasaValue>,
    last_solve_stats: Option<CasaValue>,
    correlation_dependent_flags: Option<bool>,
    using_callibrary: bool,
}

struct VisibilityEquation {
    attachment: CalibrationAttachment,
    model: CalibraterModel,
    apply: ApplyTerms,
    solve: Option<ConfiguredSolve>,
    solve_description: Option<String>,
    opacity_spectral_windows: Option<usize>,
}

impl VisibilityEquation {
    fn check_opacity_parameters(&self) -> Result<(), CalibraterLifecycleError> {
        if self.apply.tableless_opacity.is_empty() {
            return Ok(());
        }
        let failure = CalibraterLifecycleError::ApplyConfiguration;
        if !self.apply.tables.is_empty() {
            let ms = MeasurementSet::open(self.attachment.path()).map_err(AttachmentError::from)?;
            let request = crate::ApplyPlanRequest {
                selection: self.attachment.selection().clone(),
                apply_mode: crate::ApplyMode::Trial,
                parang: self.apply.parang_terms > 0,
                calibration_tables: self.apply.tables.clone(),
            };
            let (plan, _) = crate::plan::plan_with_visibility_metadata(&ms, &request, None)
                .map_err(|error| failure(error.to_string()))?;
            if !crate::execute::has_usable_apply_rows(&plan) {
                return Ok(());
            }
        }
        Err(failure("Error in TOpac::calcPar()".into()))
    }
}

enum ConfiguredSolve {
    Task {
        request: Box<CalibrationSolveRequest>,
        mode: CalibrationOutputMode,
        invalid_mode: bool,
        bandpass: BandpassSolveControls,
        spline_mode: Option<String>,
        tf: Option<crate::solve::TfSolveSettings>,
    },
    LegacyEp(Box<LegacyEpSolver>),
    Polarization {
        request: Box<crate::solve::grouping::leakage::LeakageSolveRequest>,
        invalid_mode: bool,
    },
}

#[derive(Default)]
struct ApplyTerms {
    tables: Vec<ApplyCalibrationTableSpec>,
    table_snapshots: BTreeMap<PathBuf, traditional_apply::RegisteredApplySnapshot>,
    library_tables: Vec<LibraryApplyRegistration>,
    counts: ApplyTermCounts,
    parang_terms: usize,
    parang_calwt: bool,
    tableless_opacity: Vec<traditional_apply::TablelessOpacity>,
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
    /// The requested amplitude/phase solve mode is rejected during execution.
    #[error("unrecognized amplitude/phase solve mode")]
    InvalidSolveMode,
    /// The installed CASA tool executes the spline solver via VI1.
    #[error("gain spline solves require the legacy visibility iterator")]
    SplineNeedsLegacyIterator,
    /// The specialized setter could not parse a reference antenna selection.
    #[error("{0}")]
    ReferenceAntennaSelection(String),
    /// Delay solving requires an explicit reference antenna.
    #[error("specify a reference antenna explicitly")]
    DelayNeedsReference,
    /// The image setter requires a configured solver.
    #[error("Calibrater::setmodel() called before Calibrater::setsolve()")]
    ImageNeedsSolver,
    /// Only the legacy EP solver can own an image model.
    #[error("Internal error: setModel() not yet supported for non EPJones type.")]
    ImageNeedsEp,
    /// The EP solver uses the original visibility iterator.
    #[error("Error in Calibrater::setsolve.")]
    EpNeedsLegacyIterator,
    /// Executing the full EP solve is owned by the solver-tool implementation.
    #[error("calibrater.solve with EP is pending in #1528")]
    EpSolvePending,
    /// The polarization solve could not be configured or completed.
    #[error("{0}")]
    LeakageSolve(String),
    /// A point model must contain at most four finite CASA Stokes values.
    #[error("a point model requires up to four finite Stokes values")]
    InvalidPointModel,
    /// CASA 6.7.6 uses the SPW selector for channel selection.
    #[error(
        "Error in Calibrater::selectvis(): Old-fashioned chanmode selection is no longer supported!"
    )]
    ObsoleteChannelMode,
    /// The legacy iterator's velocity selector is disabled in pinned CASA.
    #[error("Error in Calibrater::selectvis(): Exception: Method not implemented.")]
    LegacyVelocityMode,
    /// The legacy channel window lies outside a selected spectral window.
    #[error(
        "Error in Calibrater::selectvis(): Failed AlwaysAssert start >= 0 && start + channelGroupSize_p <= nChan_p"
    )]
    LegacyChannelBounds,
    /// CASA's visibility component fitter uses the legacy iterator.
    #[error("Calibrater::modelfit not implemented")]
    FitNeedsLegacyIterator,
    /// The attached tool could not initialize its visibility weights.
    #[error("Error in Calibrater::initWeights.")]
    InitWeights(#[source] casa_ms::MsError),
    /// The newer CASA visibility iterator does not implement accumulation.
    #[error("Calibrater::accumulate not implemented")]
    AccumulateNeedsLegacyIterator,
    /// CASA reports a generic accumulation failure after logging its cause.
    #[error("Error in Calibrater::accumulate.")]
    Accumulate(#[source] casa_ms::MsError),
    /// Creating a calibration table failed.
    #[error("{0}")]
    CreateTable(String),
    /// Creating or updating the calibration table from supplied parameters failed.
    #[error("Error in Calibrater::specifycal.")]
    Specify(#[source] Box<crate::GencalError>),
    /// Transferring a flux scale using the attached dataset failed.
    #[error("{0}")]
    FluxScale(String),
    /// Smoothing an attached calibration table failed.
    #[error("Error in Calibrater::smooth.")]
    Smooth(#[source] casa_ms::MsError),
    /// Re-referencing an attached calibration table failed.
    #[error("Error in Calibrater::reRefant.")]
    Rereference(#[source] casa_ms::MsError),
    /// MeasurementSet attachment or visibility selection failed.
    #[error(transparent)]
    Attachment(#[from] AttachmentError),
    /// A tool apply term could not be configured.
    #[error("{0}")]
    ApplyConfiguration(String),
    /// The configured solver could not complete.
    #[error(transparent)]
    Solve(#[from] CalibrationError),
}

impl CalibraterLifecycle {
    /// Transfer a flux scale and record the direct tool's HISTORY entries.
    ///
    /// # Errors
    /// Returns an error without an attachment, valid selection or writable product.
    pub fn fluxscale_tool(
        &self,
        mut request: crate::FluxScaleRequest,
    ) -> Result<crate::FluxScaleReport, CalibraterLifecycleError> {
        let path = self.equation()?.attachment.path();
        request.options.measurement_set = Some(path.to_owned());
        crate::fluxscale::fluxscale_tool(&request, path)
            .map_err(CalibraterLifecycleError::FluxScale)
    }

    /// Create or update a prior table using the attached dataset's axes.
    ///
    /// # Errors
    /// Returns an error without an attachment or if the prescription fails.
    pub fn specify_calibration(
        &self,
        specification: crate::CalibrationSpecification<'_>,
    ) -> Result<(), CalibraterLifecycleError> {
        let attachment = &self.equation()?.attachment;
        specification
            .write(
                attachment.path(),
                self.legacy_iterator,
                attachment.selection(),
            )
            .map_err(|error| {
                tracing::error!(%error, "Calibration specification failed");
                CalibraterLifecycleError::Specify(Box::new(error))
            })
    }

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
        self.open_with_scratch(path, false, false, false)
    }

    /// Attach a writable MeasurementSet, initializing requested scratch
    /// columns that are absent. Existing columns retain their values.
    ///
    /// # Errors
    /// The MeasurementSet cannot be opened for update or scratch cannot be
    /// initialized.
    pub fn open_with_scratch(
        &mut self,
        path: &Path,
        compress: bool,
        add_corrected: bool,
        add_model: bool,
    ) -> Result<(), CalibraterLifecycleError> {
        self.equation = None;
        let attachment = CalibrationAttachment::attach(
            path,
            AttachmentMode::Open,
            AttachmentOptions::default(),
            MsSelection::default(),
        )?;
        std::fs::OpenOptions::new()
            .write(true)
            .open(attachment.path().join("table.dat"))
            .map_err(|error| {
                AttachmentError::from(casa_ms::MsError::InvalidInput(format!(
                    "cannot open {} for update: {error}",
                    attachment.path().display(),
                )))
            })?;
        if add_corrected || add_model {
            let opened = OpenedMs::open(attachment.path())
                .and_then(|opened| opened.initialize_calibrater(compress, add_corrected, add_model))
                .map_err(AttachmentError::from)?;
            opened.release().map_err(AttachmentError::from)?;
        }
        self.equation = Some(VisibilityEquation {
            attachment,
            model: CalibraterModel::FromMeasurementSet,
            apply: ApplyTerms::default(),
            solve: None,
            solve_description: None,
            opacity_spectral_windows: None,
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

    /// List calibration solutions using the attached MeasurementSet's
    /// antenna, field and spectral-window selections. A nonempty listfile
    /// is created atomically and refused when it already exists.
    ///
    /// # Errors
    ///
    /// The tool is detached, a table or selector is invalid, or a listing
    /// file cannot be published.
    pub fn list_calibration(
        &self,
        caltable: &Path,
        field: &str,
        antenna: &str,
        spw: &str,
        listfile: &str,
        page_rows: i32,
    ) -> Result<CalibrationListingReport, String> {
        let Some(dataset) = self.dataset_path() else {
            return Err(if caltable.exists() {
                "MSSelection::getTEN() called without setting the MS".into()
            } else {
                format!("Table {} does not exist.", caltable.display())
            });
        };
        #[cfg(unix)]
        let ms = MeasurementSet::open_retained_read(dataset);
        #[cfg(not(unix))]
        let ms = MeasurementSet::open(dataset);
        let ms = ms.map_err(|error| error.to_string())?;
        let listing = list_calibration(&ms, caltable, field, antenna, spw, page_rows)?;
        if listfile.is_empty() {
            return Ok(CalibrationListingReport {
                listing: Some(listing),
                listfile: None,
            });
        }
        if let Some(error) = listing.error {
            return Err(error);
        }
        publish_listing(Path::new(listfile), &listing.lines())?;
        Ok(CalibrationListingReport {
            listing: None,
            listfile: Some(listfile.into()),
        })
    }

    /// Release the current equation and attachment. The iterator choice is
    /// retained for the next call to `open`.
    pub fn close(&mut self) {
        self.equation = None;
        self.last_activity = None;
        self.last_solve_stats = None;
        self.correlation_dependent_flags = None;
        self.using_callibrary = false;
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
        self.select_with_channel_mode(selection, "none", [1, 0, 1])
    }

    /// Select data with the Python tool's channel mode and legacy channel
    /// count, start and step. The modern iterator uses the SPW expression.
    ///
    /// # Errors
    /// The dataset or selector is invalid, or the removed channel-mode
    /// selector was requested.
    pub fn select_with_channel_mode(
        &mut self,
        mut selection: MsSelection,
        channel_mode: &str,
        legacy_channels: [i64; 3],
    ) -> Result<(), CalibraterLifecycleError> {
        let legacy_iterator = self.legacy_iterator;
        let equation = self.equation_mut()?;
        let selected = equation
            .attachment
            .select(selection.clone())
            .map_err(Into::into)
            .and_then(|()| match channel_mode {
                "none" => Ok(()),
                _ if !legacy_iterator => Err(CalibraterLifecycleError::ObsoleteChannelMode),
                "velocity" | "opticalvelocity" => Err(CalibraterLifecycleError::LegacyVelocityMode),
                mode => {
                    selection.spw = Some(legacy_spw_expression(
                        &equation.attachment,
                        (mode == "channel").then_some(legacy_channels),
                    )?);
                    equation.attachment.select(selection).map_err(Into::into)
                }
            });
        equation.apply = ApplyTerms::default();
        equation.solve = None;
        if let Err(error) = selected {
            equation.attachment.reset_selection();
            equation.model = CalibraterModel::FromMeasurementSet;
            return Err(error);
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
        let terms = &mut self.equation_mut()?.apply;
        terms.parang_terms = usize::from(apply);
        terms.parang_calwt = false;
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

    /// Delete model data on the actual attachment. The explicit field and
    /// SPW expressions address virtual models. Scratch removal is global.
    ///
    /// # Errors
    /// The MeasurementSet is missing, a selector is invalid, or a write fails.
    pub fn delete_model(
        &self,
        field: Option<&str>,
        spw: Option<&str>,
        virtual_model: bool,
        scratch: bool,
    ) -> Result<(), CalibraterLifecycleError> {
        let equation = self.equation()?;
        if !virtual_model && !scratch {
            return Ok(());
        }
        let mut opened =
            OpenedMs::open(equation.attachment.path()).map_err(AttachmentError::from)?;
        delete_calibration_model(
            &mut opened.measurement_set,
            field,
            spw,
            virtual_model,
            scratch,
            &mut opened.lock,
        )
        .map_err(AttachmentError::from)?;
        opened.release().map_err(AttachmentError::from)?;
        Ok(())
    }

    /// Describe the existing MAIN and SOURCE virtual model field records.
    ///
    /// # Errors
    /// The tool has no open MeasurementSet or its model records cannot be read.
    pub fn model_header_lines(&self) -> Result<Vec<String>, CalibraterLifecycleError> {
        let equation = self.equation()?;
        equation
            .attachment
            .with_write(|ms, _, _| {
                let main = ms.main_table();
                if main.row_count() == 0 {
                    return Ok(Vec::new());
                }
                let mut fields = std::collections::BTreeSet::new();
                for rows in (0..main.row_count()).step_by(65_536) {
                    let end = (rows + 65_536).min(main.row_count());
                    let indices = (rows..end).collect::<Vec<_>>();
                    let columns =
                        main.required_scalar_columns_owned_for_rows(&["FIELD_ID"], &indices)?;
                    let Some(RequiredScalarColumnValues::Int32(ids)) = columns.get("FIELD_ID")
                    else {
                        return Err(casa_ms::MsError::InvalidInput(
                            "FIELD_ID must be Int32".into(),
                        )
                        .into());
                    };
                    fields.extend(ids.iter().filter_map(|&id| usize::try_from(id).ok()));
                }
                let field = ms.field()?;
                let mut lines = vec!["MS Header field records:".into()];
                let mut has_pointer = false;
                for table in [Some(main), ms.subtable(SubtableId::Source)]
                    .into_iter()
                    .flatten()
                {
                    let keywords = table.keywords();
                    for &id in &fields {
                        if let Some(Value::Scalar(ScalarValue::String(key))) =
                            keywords.get(&format!("definedmodel_field_{id}"))
                        {
                            has_pointer = true;
                            if keywords.get(key).is_some() {
                                lines.push(format!(" {} (id = {id})", field.name(id)?));
                            }
                        }
                    }
                }
                if !has_pointer {
                    lines.push(" None.".into());
                }
                Ok(lines)
            })
            .map_err(Into::into)
    }

    /// Fit a visibility component using the tool's current selection.
    ///
    /// # Errors
    /// The dataset or selection is invalid, the fit fails, or the requested
    /// component table cannot be written.
    pub fn fit_model(
        &self,
        request: VisibilityFitRequest<'_>,
        output: Option<&Path>,
    ) -> Result<Vec<f64>, CalibraterLifecycleError> {
        if !self.legacy_iterator {
            return Err(CalibraterLifecycleError::FitNeedsLegacyIterator);
        }
        let equation = self.equation()?;
        equation
            .attachment
            .with_write(|ms, selection, _| {
                let fit = fit_visibility_component(ms, selection, request)?;
                if let Some(path) = output {
                    ComponentList::from(vec![fit.component])
                        .write_table(path)
                        .map_err(|error| casa_ms::MsError::InvalidInput(error.to_string()))?;
                }
                Ok(fit.parameters)
            })
            .map_err(Into::into)
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
        self.configure_solve(
            request,
            mode,
            Some(GainSolveMode::AmplitudePhase),
            GainNormalization::Mean,
            -1.,
            None,
        )
    }

    /// Return whether CASA recognizes this calibration factory name.
    #[must_use]
    pub fn recognizes_calibration_type(kind: &str) -> bool {
        traditional_apply::calibration_factory_type(kind).is_some()
    }

    /// Set the Python tool's solve configuration, including modes validated
    /// only when CASA actually attempts a fit.
    ///
    /// # Errors
    /// Returns an error without an open dataset or a required reference antenna.
    pub fn set_tool_solve(
        &mut self,
        request: CalibrationSolveRequest,
        mode: CalibrationOutputMode,
        visibility_mode: Option<GainSolveMode>,
        normalization: GainNormalization,
        preaverage_seconds: f64,
    ) -> Result<(), CalibraterLifecycleError> {
        let equation = self.equation()?;
        if let CalibrationSolveRequest::Gain(gain) = &request
            && matches!(gain.gain_type, crate::GainType::K | crate::GainType::KCross)
        {
            let ms =
                MeasurementSet::open(equation.attachment.path()).map_err(AttachmentError::from)?;
            let refants = crate::solve::grouping::resolve_refants(&ms, &gain.refant)
                .map_err(CalibrationError::from)?;
            if !refants.first().is_some_and(|&id| id >= 0) {
                return Err(CalibraterLifecycleError::DelayNeedsReference);
            }
        }
        self.configure_solve(
            request,
            mode,
            visibility_mode,
            normalization,
            preaverage_seconds,
            None,
        )
    }

    /// Configure the dedicated legacy gain spline solver and its literal mode.
    /// The mode selects fitted components and is preserved in the table.
    ///
    /// # Errors
    /// Returns an error when no MeasurementSet is open.
    pub fn set_tool_spline(
        &mut self,
        gain: crate::GainSolveRequest,
        controls: crate::GainSplineControls,
        mode: CalibrationOutputMode,
        spline_mode: String,
    ) -> Result<(), CalibraterLifecycleError> {
        self.configure_solve(
            CalibrationSolveRequest::GainSpline { gain, controls },
            mode,
            Some(GainSolveMode::AmplitudePhase),
            GainNormalization::Mean,
            -1.,
            Some(spline_mode),
        )
    }

    /// Configure the Tf Jones factory with the scalar gain fitter.
    ///
    /// # Errors
    /// Returns an error when no MeasurementSet is open.
    pub fn set_tool_tf(
        &mut self,
        mut gain: crate::GainSolveRequest,
        mode: CalibrationOutputMode,
        frequency: Option<crate::BandpassFrequencyInterval>,
        invalid_mode: bool,
    ) -> Result<(), CalibraterLifecycleError> {
        let equation = self.equation_mut()?;
        gain.gain_type = crate::GainType::T;
        equation.solve = Some(ConfiguredSolve::Task {
            request: Box::new(CalibrationSolveRequest::Gain(gain)),
            mode,
            invalid_mode,
            bandpass: BandpassSolveControls::default(),
            spline_mode: None,
            tf: Some(crate::solve::TfSolveSettings { frequency }),
        });
        equation.solve_description = None;
        Ok(())
    }

    /// Resolve the primary reference antenna before replacing the previous
    /// solve configuration. Use this for families that consume at most one
    /// reference antenna.
    ///
    /// # Errors
    /// Returns an error if the attached MS or antenna selection is invalid.
    pub fn resolve_primary_tool_refant(
        &self,
        reference: crate::RefAntSelector,
    ) -> Result<crate::RefAntSelector, CalibraterLifecycleError> {
        use crate::RefAntSelector;

        let RefAntSelector::Expression { selection, .. } = reference else {
            return Ok(reference);
        };
        let ms = MeasurementSet::open(self.equation()?.attachment.path())
            .map_err(AttachmentError::from)?;
        let antennas = ms.resolve_reference_antennas(&selection).map_err(|error| {
            let casa_ms::MsError::InvalidInput(detail) = error else {
                return CalibraterLifecycleError::ReferenceAntennaSelection(error.to_string());
            };
            let detail = detail
                .strip_prefix(&format!("antenna selection {selection:?}: "))
                .unwrap_or(&detail);
            let message = if detail.starts_with("Antenna Expression: No match found") {
                format!("{detail}  ")
            } else if detail.starts_with("No match found for the antenna specificion") {
                format!("{detail} ")
            } else {
                detail.replacen(" (near char.", "\n(near char.", 1)
            };
            CalibraterLifecycleError::ReferenceAntennaSelection(message)
        })?;
        Ok(antennas.first().map_or(RefAntSelector::Unspecified, |&id| {
            RefAntSelector::AntennaId(id as i32)
        }))
    }

    fn configure_solve(
        &mut self,
        request: CalibrationSolveRequest,
        mode: CalibrationOutputMode,
        visibility_mode: Option<GainSolveMode>,
        normalization: GainNormalization,
        preaverage_seconds: f64,
        spline_mode: Option<String>,
    ) -> Result<(), CalibraterLifecycleError> {
        let equation = self.equation_mut()?;
        equation.solve = Some(ConfiguredSolve::Task {
            request: Box::new(request),
            mode,
            invalid_mode: visibility_mode.is_none(),
            bandpass: BandpassSolveControls {
                solve_mode: visibility_mode.unwrap_or(GainSolveMode::AmplitudePhase),
                normalization,
                preaverage_seconds,
                modern_polynomial: false,
            },
            spline_mode,
            tf: None,
        });
        equation.solve_description = None;
        Ok(())
    }

    /// Configure the EP solver and its disk cache on the legacy iterator.
    /// # Errors
    /// The iterator is not legacy, no dataset is attached, or the cache
    /// configuration fails.
    pub fn set_ep_solve(
        &mut self,
        table: &Path,
        cache: &Path,
        angle_deg: f64,
    ) -> Result<(), CalibraterLifecycleError> {
        self.set_tool_ep_solve(
            table,
            cache,
            angle_deg,
            "",
            CalibrationOutputMode::Replace,
            crate::RefAntSelector::Unspecified,
        )
    }

    /// Configure legacy EP with the public tool's interval, table mode, and reference list.
    /// # Errors
    /// The legacy iterator, an attached dataset, a valid interval, and a writable cache are required.
    pub fn set_tool_ep_solve(
        &mut self,
        table: &Path,
        cache: &Path,
        angle_deg: f64,
        interval: &str,
        mode: CalibrationOutputMode,
        reference: crate::RefAntSelector,
    ) -> Result<(), CalibraterLifecycleError> {
        let legacy_iterator = self.legacy_iterator;
        let equation = self.equation_mut()?;
        if !legacy_iterator {
            tracing::error!("EP not yet supported via EPJones(msmc)");
            return Err(CalibraterLifecycleError::EpNeedsLegacyIterator);
        }
        let (solver, interval_label) =
            LegacyEpSolver::new(table, cache, angle_deg, interval, mode, reference)
                .map_err(AttachmentError::from)?;
        equation.solve = Some(ConfiguredSolve::LegacyEp(Box::new(solver)));
        self.record_solve_description(&interval_label, "", "flex", None);
        Ok(())
    }

    /// Prepare the configured EP solver's Fourier model and convolution.
    /// # Errors
    /// No EP solver is configured or model and convolution preparation fail.
    pub fn set_image_model(&mut self, path: &Path) -> Result<(), CalibraterLifecycleError> {
        let Some(equation) = self.equation.as_mut() else {
            return Err(CalibraterLifecycleError::ImageNeedsSolver);
        };
        match equation.solve.as_mut() {
            Some(ConfiguredSolve::LegacyEp(solver)) => {
                solver.set_model(&equation.attachment, path)?
            }
            Some(ConfiguredSolve::Task { .. } | ConfiguredSolve::Polarization { .. }) => {
                return Err(CalibraterLifecycleError::ImageNeedsEp);
            }
            None => return Err(CalibraterLifecycleError::ImageNeedsSolver),
        }
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
            equation.solve_description = None;
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
        self.solve_configured(None)
    }

    fn solve_configured(
        &self,
        activity: Option<&mut crate::solve_stats::ToolSolveActivity>,
    ) -> Result<CalibrationSolveResult, CalibraterLifecycleError> {
        let equation = self.equation()?;
        let (mut request, mode, mut bandpass, spline_mode, invalid_mode, tf) =
            match equation.solve.as_ref() {
                Some(ConfiguredSolve::Task {
                    request,
                    mode,
                    invalid_mode,
                    bandpass,
                    spline_mode,
                    tf,
                }) => {
                    if *invalid_mode
                        && !(activity.is_some()
                            && matches!(**request, CalibrationSolveRequest::Accor(_)))
                    {
                        return Err(CalibraterLifecycleError::InvalidSolveMode);
                    }
                    (
                        (**request).clone(),
                        *mode,
                        *bandpass,
                        spline_mode.as_deref(),
                        *invalid_mode,
                        *tf,
                    )
                }
                Some(ConfiguredSolve::LegacyEp(_)) => {
                    return Err(CalibraterLifecycleError::EpSolvePending);
                }
                Some(ConfiguredSolve::Polarization { .. }) => {
                    return Err(CalibraterLifecycleError::LeakageSolve(
                        "execute a direct leakage configuration with solve_tool".into(),
                    ));
                }
                None => return Err(CalibraterLifecycleError::NoSolver),
            };
        equation.check_opacity_parameters()?;
        if activity.is_some() && !self.legacy_iterator {
            if matches!(&request, CalibrationSolveRequest::GainSpline { .. }) {
                return Err(CalibraterLifecycleError::SplineNeedsLegacyIterator);
            }
            bandpass.modern_polynomial = true;
        }
        let (model_source, stokes) = match equation.model {
            CalibraterModel::FromMeasurementSet => {
                (GainSolveModelSource::FromMeasurementSet, [1., 0., 0., 0.])
            }
            CalibraterModel::Point(stokes) => (GainSolveModelSource::PointSource, stokes),
        };
        let selection = equation.attachment.selection().clone();
        let apply = equation.apply.tables.clone();
        match &mut request {
            CalibrationSolveRequest::Gain(gain)
            | CalibrationSolveRequest::GainSpline { gain, .. }
            | CalibrationSolveRequest::Fringe { gain, .. }
            | CalibrationSolveRequest::FringeTool { gain, .. }
            | CalibrationSolveRequest::Accor(gain)
            | CalibrationSolveRequest::Baseline { gain, .. } => {
                gain.selection = selection;
                gain.model_source = model_source;
                gain.smodel = stokes;
                gain.correlation_dependent_flags = self
                    .correlation_dependent_flags
                    .unwrap_or(gain.correlation_dependent_flags);
                gain.prior_calibration_tables = apply;
                gain.parang = equation.apply.parang_terms > 0;
            }
            CalibrationSolveRequest::Bandpass(bandpass) => {
                bandpass.selection = selection;
                bandpass.model_source = model_source;
                bandpass.smodel = stokes;
                bandpass.correlation_dependent_flags = self
                    .correlation_dependent_flags
                    .unwrap_or(bandpass.correlation_dependent_flags);
                bandpass.prior_calibration_tables = apply;
                bandpass.parang = equation.apply.parang_terms > 0;
            }
        }
        Ok(crate::session::solve_calibration_with_activity(
            CalibrationDataset::path(equation.attachment.path()),
            request,
            mode,
            activity,
            crate::session::ToolSolveOptions {
                bandpass,
                spline_mode,
                legacy_iterator: self.legacy_iterator,
                invalid_mode,
                tf,
            },
        )?)
    }

    /// Set the engine's pre-solve correlation flag handling.
    pub fn set_correlation_dependent_flags(&mut self, enabled: bool) {
        self.correlation_dependent_flags = Some(enabled);
    }

    /// Execute the attached tool solver and retain its result records.
    ///
    /// # Errors
    /// Returns an error if the current equation cannot be solved.
    pub fn solve_tool(&mut self) -> Result<(), CalibraterLifecycleError> {
        let equation = self.equation()?;
        if matches!(equation.solve, Some(ConfiguredSolve::Polarization { .. })) {
            self.last_solve_stats = None;
            return match self.solve_polarization_tool() {
                Ok(()) => Ok(()),
                Err(error) => {
                    self.reset(true, true)?;
                    Err(error)
                }
            };
        }
        let generic = matches!(&equation.solve, Some(ConfiguredSolve::Task { request, invalid_mode, .. })
            if !invalid_mode || matches!(**request, CalibrationSolveRequest::Accor(_)));
        if generic {
            self.last_solve_stats = None;
        }
        let mut activity = crate::solve_stats::ToolSolveActivity::default();
        let solved = match self.solve_configured(Some(&mut activity)) {
            Ok(solved) => solved,
            Err(error) => {
                self.reset(true, true)?;
                return Err(error);
            }
        };
        let stats = match &solved {
            CalibrationSolveResult::Gain(gain) => &gain.solve_stats,
            CalibrationSolveResult::Bandpass(bandpass) => &bandpass.solve_stats,
        };
        if !self.legacy_iterator && !stats.windows.is_empty() {
            self.last_solve_stats = Some(crate::casa_return::solvestats(stats));
        }
        if let Some(record) = activity.record() {
            self.last_activity = Some(record);
        }
        Ok(())
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

fn legacy_spw_expression(
    attachment: &CalibrationAttachment,
    channels: Option<[i64; 3]>,
) -> Result<String, CalibraterLifecycleError> {
    let selected = attachment.resolve()?;
    let windows: BTreeSet<_> = selected
        .selected_rows
        .iter()
        .map(|row| row.spectral_window_id)
        .collect();
    let Some([count, start, step]) = channels else {
        return Ok(windows
            .into_iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(","));
    };
    let ms = MeasurementSet::open(attachment.path()).map_err(AttachmentError::from)?;
    let spw = ms.spectral_window().map_err(AttachmentError::from)?;
    let window_channels = |id| {
        spw.num_chan(id as usize)
            .map(i64::from)
            .map_err(AttachmentError::from)
    };
    let count = if step < 1 {
        1
    } else if count < 1 || start < 0 {
        window_channels(*windows.first().ok_or(AttachmentError::EmptySelection)?)?
    } else {
        count
    };
    let stop = start
        .checked_add(count)
        .ok_or(CalibraterLifecycleError::LegacyChannelBounds)?;
    let mut expressions = Vec::new();
    for id in windows {
        if start < 0 || stop > window_channels(id)? {
            return Err(CalibraterLifecycleError::LegacyChannelBounds);
        }
        expressions.push(format!("{id}:{start}~{}", stop - 1));
    }
    Ok(expressions.join(","))
}
