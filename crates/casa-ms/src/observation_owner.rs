// SPDX-License-Identifier: LGPL-3.0-or-later

//! Selected-observation access for native imaging: resolve a selected
//! MeasurementSet into compiler input, and write selected visibilities back.
//!
//! A MeasurementSet is read and written as CASA does. casacore's table locks
//! are the only coordination, and casa-rs stores nothing of its own in it: no
//! keywords, marker files, generations or identities.

use std::{path::Path, sync::Arc};

use casa_imaging_model::{
    FlagPolicy, LogicalIdentity, ModelStateIdentity, MsColumnKind, ObservationSelection,
    ObservationSnapshotInput, ObservationSourceInput, ObservationSourceProvenance,
    ReferenceDataKind, SelectedColumns, SelectedMainRow, SelectedRowsBuilder,
    SpectralWindowCoordinateCatalog, SpectralWindowSelection, VisibilityColumn, WeightColumn,
};
use casa_tables::{ColumnSchema, LockType, Table};
use casa_types::{
    ArrayValue, Complex32, PrimitiveType, ScalarValue, Value, measures::MeasuresProvider,
};
use thiserror::Error;

use crate::selected_observation::validate_selected_coordinates;
use crate::selected_pointing::SelectedPointingQueryDomain;
use crate::subtables::SubTable;
use crate::{
    BoundObservationSourceError, BoundSelectedObservation, BoundSelectedObservationError,
    MeasurementSet, MsError, ObservationSourceBinding, SelectedObservationContentBudget,
    SelectedObservationEphemeris, SelectedObservationMeasures, SelectedObservationMeasuresError,
    SelectedObservationResidencyCertificate, SelectedObservationRowSelection,
};

const VISIBILITY_WRITE_BATCH_ROWS: u64 = 10_000;

/// Explicit production inputs for resolving one selected MeasurementSet.
///
/// Cloning duplicates only this immutable resolution description. Every call to
/// [`resolve_selected_observation`] opens the MeasurementSet afresh and
/// returns a new affine access capability; live table authority is never cloned.
#[derive(Clone)]
pub struct SelectedObservationResolutionRequest {
    locator: String,
    selection_request: LogicalIdentity,
    selection: Arc<ObservationSelection>,
    visibility: VisibilityColumn,
    weights: WeightColumn,
    reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
    model: ModelStateIdentity,
    content_budget: SelectedObservationContentBudget,
    measures_provider: Arc<dyn MeasuresProvider>,
    ephemeris: Option<SelectedObservationEphemeris>,
}

impl SelectedObservationResolutionRequest {
    /// Use a finalized physical content budget for subsequent owner resolutions.
    #[must_use]
    pub fn with_content_budget(mut self, budget: SelectedObservationContentBudget) -> Self {
        self.content_budget = budget;
        self
    }

    /// Construct one single-source production observation resolution.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        locator: impl Into<String>,
        selection_request: LogicalIdentity,
        selection: ObservationSelection,
        visibility: VisibilityColumn,
        weights: WeightColumn,
        reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
        model: ModelStateIdentity,
        content_budget: SelectedObservationContentBudget,
        measures_provider: Arc<dyn MeasuresProvider>,
    ) -> Self {
        Self {
            locator: locator.into(),
            selection_request,
            selection: Arc::new(selection),
            visibility,
            weights,
            reference_data,
            model,
            content_budget,
            measures_provider,
            ephemeris: None,
        }
    }

    /// Bind immutable moving-source reference data to this owner resolution.
    #[must_use]
    pub fn with_ephemeris(mut self, ephemeris: Option<SelectedObservationEphemeris>) -> Self {
        self.ephemeris = ephemeris;
        self
    }

    /// Return the storage-owner locator used for each fresh resolution.
    #[must_use]
    pub fn locator(&self) -> &str {
        &self.locator
    }

    /// Return shared exact row, channel, and correlation selection authority.
    #[must_use]
    pub fn selection(&self) -> Arc<ObservationSelection> {
        Arc::clone(&self.selection)
    }
}

/// Selected visibility columns written by one bounded owner transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedVisibilityWriteTargets {
    model_data: bool,
    corrected_data: bool,
}

impl SelectedVisibilityWriteTargets {
    /// Construct the exact destination set. At least one destination is required.
    #[must_use]
    pub const fn new(model_data: bool, corrected_data: bool) -> Self {
        Self {
            model_data,
            corrected_data,
        }
    }

    /// Return whether `MODEL_DATA` is written.
    #[must_use]
    pub const fn model_data(self) -> bool {
        self.model_data
    }

    /// Return whether existing `CORRECTED_DATA` is written.
    #[must_use]
    pub const fn corrected_data(self) -> bool {
        self.corrected_data
    }
}

/// Bounded in-place selected-visibility writer following ordinary casacore semantics.
///
/// The writer holds the MAIN write lock until it completes. Completion flushes
/// every selected destination and releases the lock. A failure may leave
/// partially written derived values, as an interrupted CASA write does; the
/// next run recomputes them. No backup, staging column, rollback, snapshot,
/// marker, or content digest is created.
#[cfg(unix)]
pub struct SelectedVisibilityWrite {
    measurement_set: Option<MeasurementSet>,
    targets: SelectedVisibilityWriteTargets,
    pending_cell: Option<PendingVisibilityCells>,
    pending_rows: Vec<usize>,
    completed: bool,
}

struct PendingVisibilityCells {
    row: usize,
    model_data: Option<ndarray::ArrayD<Complex32>>,
    corrected_data: Option<ndarray::ArrayD<Complex32>>,
}

/// Owner-derived storage plan for one bounded selected-visibility write.
///
/// The plan is derived from MAIN row/DDID coordinates and the standard
/// DATA_DESCRIPTION, SPECTRAL_WINDOW, and POLARIZATION metadata. It never reads
/// a visibility payload merely to discover a cell shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedVisibilityStoragePlan {
    additional_persistent_bytes: u64,
    write_bytes: u64,
    maximum_cell_bytes: u64,
    write_buffer_bytes: u64,
}

impl SelectedVisibilityStoragePlan {
    /// New persistent capacity required for this write.
    ///
    /// This includes complete logical `MODEL_DATA` capacity when creation and
    /// zero-initialization are required. Existing destinations add no capacity.
    #[must_use]
    pub const fn additional_persistent_bytes(self) -> u64 {
        self.additional_persistent_bytes
    }

    /// Bytes written by the initial column creation and selected-cell update.
    #[must_use]
    pub const fn write_bytes(self) -> u64 {
        self.write_bytes
    }

    /// Largest single-row cell copied or updated by the bounded writer.
    #[must_use]
    pub const fn maximum_cell_bytes(self) -> u64 {
        self.maximum_cell_bytes
    }

    /// Maximum payload bytes retained by the bounded row-batch writer.
    #[must_use]
    pub const fn write_buffer_bytes(self) -> u64 {
        self.write_buffer_bytes
    }
}

#[cfg(unix)]
impl SelectedVisibilityWrite {
    /// Take the MAIN write lock and start an in-place write. `MODEL_DATA` is
    /// created, zero-filled, when MAIN lacks it, as CASA does.
    ///
    /// The MeasurementSet is opened with retained read locks, which wait for
    /// a writer in another process, and the MAIN write lock is then tried
    /// once, without waiting. Unlike the other in-place writers, this one
    /// upgrades a read lock it holds: waiting for the write lock while holding
    /// it would deadlock with another process upgrading its own, because
    /// casa-rs does not yet release a lock on request
    /// ([#694](https://github.com/bglenden/casa-rs/issues/694)).
    pub fn begin(
        path: impl AsRef<Path>,
        targets: SelectedVisibilityWriteTargets,
    ) -> Result<Self, ObservationOwnerError> {
        if !targets.model_data && !targets.corrected_data {
            return Err(ObservationOwnerError::EmptyWriteTargets);
        }
        let mut measurement_set = MeasurementSet::open_retained_read(path.as_ref())?;
        if !measurement_set.main_table_mut().lock(LockType::Write, 1)? {
            return Err(ObservationOwnerError::WriteLockUnavailable);
        }
        let has_model = {
            let schema = measurement_set.main_table().schema().ok_or_else(|| {
                MsError::InvalidInput("MeasurementSet MAIN has no schema".to_string())
            })?;
            schema.contains_column("MODEL_DATA")
        };
        let has_corrected = measurement_set
            .main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column("CORRECTED_DATA"));
        if targets.corrected_data && !has_corrected {
            return Err(ObservationOwnerError::MissingCorrectedDataDestination);
        }
        if targets.model_data && !has_model {
            measurement_set.main_table_mut().add_column(
                ColumnSchema::array_variable("MODEL_DATA", PrimitiveType::Complex32, Some(2)),
                None,
            )?;
            measurement_set
                .main_table_mut()
                .prepare_write()
                .add_tiled_column_clone("DATA", "MODEL_DATA", "TiledModelData")?;
            let row_count = measurement_set.main_table().row_count();
            let mut pending_rows = Vec::with_capacity(VISIBILITY_WRITE_BATCH_ROWS as usize);
            for row in 0..row_count {
                let data_description_id = main_data_description_id(&measurement_set, row)?;
                let shape = model_cell_shape(&measurement_set, data_description_id)?;
                queue_visibility_cell(
                    measurement_set.main_table_mut(),
                    "MODEL_DATA",
                    row,
                    ArrayValue::Complex32(ndarray::ArrayD::from_elem(
                        ndarray::IxDyn(&shape),
                        Complex32::new(0.0, 0.0),
                    )),
                )?;
                pending_rows.push(row);
                if pending_rows.len() == VISIBILITY_WRITE_BATCH_ROWS as usize {
                    persist_visibility_rows(
                        measurement_set.main_table_mut(),
                        &["MODEL_DATA"],
                        &pending_rows,
                    )?;
                    pending_rows.clear();
                }
            }
            if !pending_rows.is_empty() {
                persist_visibility_rows(
                    measurement_set.main_table_mut(),
                    &["MODEL_DATA"],
                    &pending_rows,
                )?;
            }
        }
        Ok(Self {
            measurement_set: Some(measurement_set),
            targets,
            pending_cell: None,
            pending_rows: Vec::with_capacity(VISIBILITY_WRITE_BATCH_ROWS as usize),
            completed: false,
        })
    }

    /// Write one selected prediction at its physical row/channel/correlation.
    pub fn write(
        &mut self,
        column: MsColumnKind,
        row: u64,
        channel: u32,
        correlation: u32,
        value: Complex32,
    ) -> Result<(), ObservationOwnerError> {
        let row = usize::try_from(row).map_err(|_| ObservationOwnerError::PredictionAddress)?;
        if self.pending_cell.as_ref().map(|cell| cell.row) != Some(row) {
            self.flush_pending_cell()?;
            let measurement_set = self
                .measurement_set
                .as_ref()
                .ok_or(ObservationOwnerError::TransactionClosed)?;
            let load = |name: &str| -> Result<ndarray::ArrayD<Complex32>, ObservationOwnerError> {
                let current = measurement_set
                    .main_table()
                    .column_accessor(name)?
                    .get(row)?
                    .cloned()
                    .ok_or(ObservationOwnerError::PredictionAddress)?;
                let Value::Array(ArrayValue::Complex32(values)) = current else {
                    return Err(ObservationOwnerError::PredictionAddress);
                };
                Ok(values)
            };
            self.pending_cell = Some(PendingVisibilityCells {
                row,
                model_data: self
                    .targets
                    .model_data
                    .then(|| load("MODEL_DATA"))
                    .transpose()?,
                corrected_data: self
                    .targets
                    .corrected_data
                    .then(|| load("CORRECTED_DATA"))
                    .transpose()?,
            });
        }
        let pending = self
            .pending_cell
            .as_mut()
            .ok_or(ObservationOwnerError::TransactionClosed)?;
        let values = match column {
            MsColumnKind::ModelData if self.targets.model_data => pending.model_data.as_mut(),
            MsColumnKind::CorrectedData if self.targets.corrected_data => {
                pending.corrected_data.as_mut()
            }
            _ => None,
        }
        .ok_or(ObservationOwnerError::UnselectedWriteTarget)?;
        let index = [correlation as usize, channel as usize];
        let Some(cell) = values.get_mut(index) else {
            return Err(ObservationOwnerError::PredictionAddress);
        };
        *cell = value;
        Ok(())
    }

    /// Flush the in-place write and release the MAIN write lock.
    ///
    /// The selected cells were persisted in bounded row batches, so only the
    /// table metadata (a newly created `MODEL_DATA`) remains to flush.
    pub fn complete(mut self) -> Result<(), ObservationOwnerError> {
        self.flush_pending_cell()?;
        self.persist_pending_rows()?;
        let measurement_set = self
            .measurement_set
            .as_mut()
            .ok_or(ObservationOwnerError::TransactionClosed)?;
        measurement_set.main_table_mut().unlock_metadata_only()?;
        self.measurement_set = None;
        self.completed = true;
        Ok(())
    }

    fn flush_pending_cell(&mut self) -> Result<(), ObservationOwnerError> {
        let Some(cell) = self.pending_cell.take() else {
            return Ok(());
        };
        let measurement_set = self
            .measurement_set
            .as_mut()
            .ok_or(ObservationOwnerError::TransactionClosed)?;
        if let Some(values) = cell.model_data {
            queue_visibility_cell(
                measurement_set.main_table_mut(),
                "MODEL_DATA",
                cell.row,
                ArrayValue::Complex32(values),
            )?;
        }
        if let Some(values) = cell.corrected_data {
            queue_visibility_cell(
                measurement_set.main_table_mut(),
                "CORRECTED_DATA",
                cell.row,
                ArrayValue::Complex32(values),
            )?;
        }
        self.pending_rows.push(cell.row);
        if self.pending_rows.len() == VISIBILITY_WRITE_BATCH_ROWS as usize {
            self.persist_pending_rows()?;
        }
        Ok(())
    }

    fn persist_pending_rows(&mut self) -> Result<(), ObservationOwnerError> {
        if self.pending_rows.is_empty() {
            return Ok(());
        }
        let columns = match (self.targets.model_data, self.targets.corrected_data) {
            (true, true) => &["MODEL_DATA", "CORRECTED_DATA"][..],
            (true, false) => &["MODEL_DATA"][..],
            (false, true) => &["CORRECTED_DATA"][..],
            (false, false) => return Err(ObservationOwnerError::EmptyWriteTargets),
        };
        let measurement_set = self
            .measurement_set
            .as_mut()
            .ok_or(ObservationOwnerError::TransactionClosed)?;
        persist_visibility_rows(
            measurement_set.main_table_mut(),
            columns,
            &self.pending_rows,
        )?;
        self.pending_rows.clear();
        Ok(())
    }
}

#[cfg(unix)]
impl Drop for SelectedVisibilityWrite {
    /// An abandoned write releases the MAIN write lock without flushing the
    /// cells still queued: the batches persisted so far stay, as after an
    /// interrupted CASA write, and MAIN is not rewritten.
    fn drop(&mut self) {
        if !self.completed {
            if let Some(measurement_set) = self.measurement_set.as_mut() {
                let _ = measurement_set.main_table_mut().unlock_metadata_only();
            }
            self.measurement_set = None;
        }
    }
}

/// Compiler input paired inseparably with the owner access used to probe it.
pub struct ResolvedSelectedObservation {
    snapshot_input: ObservationSnapshotInput,
    access: ResolvedSelectedObservationAccess,
}

impl ResolvedSelectedObservation {
    /// Split the compiler input from the access capability retained for execution.
    #[must_use]
    pub fn into_parts(self) -> (ObservationSnapshotInput, ResolvedSelectedObservationAccess) {
        (self.snapshot_input, self.access)
    }
}

/// Measures capability and resource binding for one resolved source.
pub struct ResolvedSelectedObservationAccess {
    binding: ObservationSourceBinding,
    measures: SelectedObservationMeasures,
    visibility_storage: SelectedVisibilityStoragePlanner,
}

struct SelectedVisibilityStoragePlanner {
    locator: String,
    selection: Arc<ObservationSelection>,
    content_budget: SelectedObservationContentBudget,
}

impl ResolvedSelectedObservationAccess {
    /// Inspect source requirements under short-lived read locks.
    ///
    /// The result retains only a checked sizing curve, bound to this compiled
    /// problem and source. No catalog or source-specific execution plan is built.
    #[cfg(unix)]
    pub fn content_requirements(
        &self,
        problem: &casa_imaging_model::CompiledProblem,
    ) -> Result<crate::SelectedObservationContentRequirements, BoundSelectedObservationError> {
        BoundSelectedObservation::single_source_content_requirements(
            problem,
            &self.measures,
            &self.binding,
        )
    }

    /// Finalize both source-read and selected-output storage budgets from one quote.
    pub fn with_content_budget(
        mut self,
        problem: &casa_imaging_model::CompiledProblem,
        requirements: &crate::SelectedObservationContentRequirements,
        budget: SelectedObservationContentBudget,
    ) -> Result<Self, BoundSelectedObservationError> {
        if !requirements.matches(problem, self.binding.measurement_set()) {
            return Err(BoundSelectedObservationError::ProblemMismatch);
        }
        requirements
            .plan(budget)
            .map_err(|error| BoundSelectedObservationError::Source {
                measurement_set: self.binding.measurement_set(),
                error: Box::new(crate::BoundObservationSourceError::ContentPlan(error)),
            })?;
        self.binding.set_content_budget(budget);
        self.visibility_storage.content_budget = budget;
        Ok(self)
    }

    /// Transfer this resolved source into the deferred execution capability.
    #[must_use]
    pub fn into_deferred(self) -> crate::DeferredSelectedObservationAccess {
        crate::DeferredSelectedObservationAccess::new(self.measures, vec![self.binding])
    }

    /// Return the exact resource binding captured with the compiler input.
    #[must_use]
    pub const fn source_binding(&self) -> &ObservationSourceBinding {
        &self.binding
    }

    /// Return the payload-free storage plan for writing this resolved selection.
    pub fn selected_visibility_storage_plan(
        &self,
        targets: SelectedVisibilityWriteTargets,
    ) -> Result<SelectedVisibilityStoragePlan, ObservationOwnerError> {
        if !targets.model_data && !targets.corrected_data {
            return Err(ObservationOwnerError::EmptyWriteTargets);
        }
        let measurement_set = MeasurementSet::open_retained_read(&self.visibility_storage.locator)?;
        let empty = SelectedVisibilityStoragePlan {
            additional_persistent_bytes: 0,
            write_bytes: 0,
            maximum_cell_bytes: 0,
            write_buffer_bytes: 0,
        };
        let corrected = if targets.corrected_data {
            if !measurement_set
                .main_table()
                .schema()
                .is_some_and(|schema| schema.contains_column("CORRECTED_DATA"))
            {
                return Err(ObservationOwnerError::MissingCorrectedDataDestination);
            }
            derive_column_storage_plan(
                &measurement_set,
                &self.visibility_storage.selection,
                "CORRECTED_DATA",
                false,
                self.visibility_storage.content_budget,
            )?
        } else {
            empty
        };
        let model = if targets.model_data {
            derive_column_storage_plan(
                &measurement_set,
                &self.visibility_storage.selection,
                "MODEL_DATA",
                true,
                self.visibility_storage.content_budget,
            )?
        } else {
            empty
        };
        let maximum_cell_bytes = model
            .maximum_cell_bytes
            .checked_add(corrected.maximum_cell_bytes)
            .ok_or(ObservationOwnerError::PredictionAddress)?;
        Ok(SelectedVisibilityStoragePlan {
            additional_persistent_bytes: model.additional_persistent_bytes,
            write_bytes: model
                .write_bytes
                .checked_add(corrected.write_bytes)
                .ok_or(ObservationOwnerError::PredictionAddress)?,
            maximum_cell_bytes,
            write_buffer_bytes: model
                .write_buffer_bytes
                .checked_add(corrected.write_buffer_bytes)
                .ok_or(ObservationOwnerError::PredictionAddress)?,
        })
    }

    /// Mint the scheduler-visible residency certificate for the compiled problem.
    pub fn certify_residency(
        &self,
        problem: &casa_imaging_model::CompiledProblem,
    ) -> Result<SelectedObservationResidencyCertificate, BoundSelectedObservationError> {
        BoundSelectedObservation::certify_residency(problem, std::slice::from_ref(&self.binding))
    }

    /// Consume this resolution and open bounded retained observation access.
    #[cfg(unix)]
    pub fn open(
        self,
        problem: &casa_imaging_model::CompiledProblem,
    ) -> Result<BoundSelectedObservation, BoundSelectedObservationError> {
        self.into_deferred().open(problem)
    }
}

/// Resolve a MeasurementSet, as CASA wrote it, into compiler input and access.
///
/// The physical schema, selected rows, Measures provider, and resource binding
/// are all evaluated while one retained MeasurementSet read capability is
/// alive. The selected data, flag, and weight columns must exist.
#[cfg(unix)]
pub fn resolve_selected_observation(
    request: SelectedObservationResolutionRequest,
) -> Result<ResolvedSelectedObservation, ObservationOwnerError> {
    if request.reference_data.iter().any(|(kind, _)| {
        matches!(
            kind,
            ReferenceDataKind::Measures | ReferenceDataKind::Ephemeris
        )
    }) {
        return Err(ObservationOwnerError::MeasuresReferenceIsOwnerSupplied);
    }
    let measurement_set = MeasurementSet::open_retained_read(&request.locator)?;
    let schema = measurement_set.main_table().schema().ok_or_else(|| {
        MsError::InvalidInput("MeasurementSet MAIN table has no schema".to_string())
    })?;
    let visibility_column = match request.visibility {
        VisibilityColumn::Data => "DATA",
        VisibilityColumn::CorrectedData => "CORRECTED_DATA",
        VisibilityColumn::FloatData => "FLOAT_DATA",
    };
    let weight_column = match request.weights {
        WeightColumn::Weight => "WEIGHT",
        WeightColumn::WeightSpectrum => "WEIGHT_SPECTRUM",
    };
    for column in [visibility_column, "FLAG", "FLAG_ROW", weight_column] {
        if !schema.contains_column(column) {
            return Err(ObservationOwnerError::MissingColumn { column });
        }
    }
    let corrected_data_present = schema.contains_column("CORRECTED_DATA");
    let selection = Arc::new(bind_physical_spectral_coordinates(
        &measurement_set,
        &request.selection,
        request.content_budget,
    )?);
    let pointing_query_domain =
        validate_physical_selection(&measurement_set, &selection, request.content_budget)?;
    let visibility_storage = SelectedVisibilityStoragePlanner {
        locator: request.locator.clone(),
        selection: Arc::clone(&selection),
        content_budget: request.content_budget,
    };
    let source = ObservationSourceInput::new(
        ObservationSourceProvenance::new(request.locator, request.selection_request),
        Arc::unwrap_or_clone(selection),
        SelectedColumns::new(
            request.visibility,
            FlagPolicy::FlagOrFlagRow,
            request.weights,
        ),
        corrected_data_present,
    );
    let measures = SelectedObservationMeasures::new(request.measures_provider)?;
    let mut reference_data = request.reference_data;
    reference_data.push((ReferenceDataKind::Measures, measures.identity()));
    if let Some(ephemeris) = request.ephemeris.as_ref() {
        reference_data.push((ReferenceDataKind::Ephemeris, ephemeris.identity()));
    }
    let snapshot_input = ObservationSnapshotInput::new(vec![source], reference_data, request.model);
    // The one resolved source is the snapshot's first.
    let binding = ObservationSourceBinding::new(0, request.content_budget)
        .with_ephemeris(request.ephemeris)
        .with_pointing_query_domain(pointing_query_domain);
    Ok(ResolvedSelectedObservation {
        snapshot_input,
        access: ResolvedSelectedObservationAccess {
            binding,
            measures,
            visibility_storage,
        },
    })
}

fn bind_physical_spectral_coordinates(
    measurement_set: &MeasurementSet,
    selection: &ObservationSelection,
    content_budget: SelectedObservationContentBudget,
) -> Result<ObservationSelection, ObservationOwnerError> {
    let spectral_window = measurement_set.spectral_window()?;
    let mut retained_catalog_bytes = 0usize;
    let mut spectral_windows = Vec::with_capacity(selection.spectral_windows().len());
    for selected in selection.spectral_windows() {
        let row = usize::try_from(selected.spectral_window_id())
            .map_err(|_| ObservationOwnerError::PhysicalSelectionMismatch)?;
        let frequency_shape = spectral_window
            .table()
            .array_shape(row, "CHAN_FREQ")?
            .filter(|shape| shape.len() == 1 && shape[0] > 0)
            .ok_or(ObservationOwnerError::PhysicalSelectionMismatch)?;
        let width_shape = spectral_window
            .table()
            .array_shape(row, "CHAN_WIDTH")?
            .filter(|shape| shape.len() == 1 && shape[0] > 0)
            .ok_or(ObservationOwnerError::PhysicalSelectionMismatch)?;
        if frequency_shape != width_shape {
            return Err(ObservationOwnerError::PhysicalSelectionMismatch);
        }
        let values = frequency_shape[0];
        let simultaneous_array_bytes = values
            .checked_mul(2)
            .and_then(|values| values.checked_mul(std::mem::size_of::<f64>()))
            .ok_or(ObservationOwnerError::PhysicalSelectionMismatch)?;
        let catalog_bytes = values
            .checked_mul(std::mem::size_of::<f64>())
            .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<usize>()))
            .ok_or(ObservationOwnerError::PhysicalSelectionMismatch)?;
        // Arc<[f64]> construction briefly overlaps the source Vec with the
        // destination allocation. CHAN_WIDTH is dropped first, so both the
        // two-array read peak and the Arc conversion peak are exactly covered.
        let construction_peak_bytes = retained_catalog_bytes
            .checked_add(simultaneous_array_bytes)
            .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<usize>()))
            .ok_or(ObservationOwnerError::PhysicalSelectionMismatch)?;
        retained_catalog_bytes = retained_catalog_bytes
            .checked_add(catalog_bytes)
            .ok_or(ObservationOwnerError::PhysicalSelectionMismatch)?;
        let required_bytes = construction_peak_bytes.max(retained_catalog_bytes);
        if required_bytes > content_budget.available_bytes() {
            return Err(ObservationOwnerError::SpectralCoordinateCatalogBudget {
                required_bytes,
                available_bytes: content_budget.available_bytes(),
            });
        }
        let frequencies_hz = spectral_window.chan_freq(row)?;
        let widths_hz = spectral_window.chan_width(row)?;
        if frequencies_hz.len() != values || widths_hz.len() != values {
            return Err(ObservationOwnerError::PhysicalSelectionMismatch);
        }
        let first_channel_width_hz = widths_hz[0];
        drop(widths_hz);
        let coordinate_catalog =
            SpectralWindowCoordinateCatalog::new(frequencies_hz, first_channel_width_hz)
                .ok_or(ObservationOwnerError::PhysicalSelectionMismatch)?;
        spectral_windows.push(
            SpectralWindowSelection::new(
                selected.spectral_window_id(),
                selected.channel_indices().to_vec(),
            )
            .with_coordinate_catalog(coordinate_catalog),
        );
    }
    Ok(ObservationSelection::new(
        selection.rows().clone(),
        selection.rows_filter().clone(),
        selection.data_descriptions().to_vec(),
        spectral_windows,
        selection.correlations().to_vec(),
    ))
}

/// Failure to resolve a selected MeasurementSet or write its visibilities.
#[derive(Debug, Error)]
pub enum ObservationOwnerError {
    /// MeasurementSet or subtable access failed.
    #[error(transparent)]
    MeasurementSet(#[from] MsError),
    /// MAIN-table locking or persistence failed.
    #[error(transparent)]
    Table(#[from] casa_tables::TableError),
    /// Measures acquisition or bounded-state preparation failed.
    #[error(transparent)]
    Measures(#[from] SelectedObservationMeasuresError),
    /// The compiled physical coordinates do not match the retained MS.
    #[error(transparent)]
    SelectedCoordinates(#[from] BoundObservationSourceError),
    /// A selected data, flag, or weight column is not in MAIN.
    #[error("the MeasurementSet has no {column} column")]
    MissingColumn {
        /// Standard MAIN column name.
        column: &'static str,
    },
    /// The selected physical MAIN row sequence differs from the compiled selection.
    #[error("selected physical MAIN rows no longer match the compiled observation selection")]
    PhysicalSelectionMismatch,
    /// The complete physical SPW coordinate catalog cannot fit its explicit source budget.
    #[error(
        "spectral coordinate catalog requires {required_bytes} bytes but the content budget has {available_bytes} bytes"
    )]
    SpectralCoordinateCatalogBudget {
        /// Peak catalog construction or retained bytes.
        required_bytes: usize,
        /// Total source content budget.
        available_bytes: usize,
    },
    /// Measures identity is always injected by the acquired provider.
    #[error(
        "reference_data must not include Measures; the storage owner injects it from the acquired provider"
    )]
    MeasuresReferenceIsOwnerSupplied,
    /// The MAIN write lock could not be acquired.
    #[error("could not acquire the MeasurementSet MAIN write lock")]
    WriteLockUnavailable,
    /// A selected visibility addressed a cell outside its destination.
    #[error("selected visibility address is outside its destination column")]
    PredictionAddress,
    /// The bounded selected-visibility writer was already completed or released.
    #[error("selected visibility write is closed")]
    TransactionClosed,
    /// No destination was selected for a write transaction.
    #[error("selected visibility write requires at least one destination")]
    EmptyWriteTargets,
    /// CORRECTED_DATA persistence requires an existing column.
    #[error("selected visibility write requires an existing CORRECTED_DATA column")]
    MissingCorrectedDataDestination,
    /// A cell write named a column outside the bound destination set.
    #[error("selected visibility write named an unselected destination")]
    UnselectedWriteTarget,
}

fn validate_physical_selection(
    measurement_set: &MeasurementSet,
    selection: &ObservationSelection,
    content_budget: SelectedObservationContentBudget,
) -> Result<SelectedPointingQueryDomain, ObservationOwnerError> {
    validate_selected_coordinates(measurement_set, selection)?;
    let row_selection = SelectedObservationRowSelection::from_compiled(selection);
    let mut actual = SelectedRowsBuilder::with_data_description_capacity(
        u64::try_from(measurement_set.row_count())
            .map_err(|_| ObservationOwnerError::PhysicalSelectionMismatch)?,
        selection.data_descriptions().len(),
    );
    let mut pointing_query_domain = SelectedPointingQueryDomain::builder();
    let mut pointing_domain_error = None;
    let mut invalid = false;
    measurement_set.visit_selected_observation_rows(
        &row_selection,
        content_budget.row_io_budget(),
        |row| {
            if !invalid {
                invalid = actual
                    .push(SelectedMainRow::new(
                        row.physical_row() as u64,
                        u32::try_from(row.data_description_id()).unwrap_or(u32::MAX),
                    ))
                    .is_err();
                if !invalid && pointing_domain_error.is_none() {
                    pointing_domain_error = pointing_query_domain
                        .observe_row(
                            row.antenna1(),
                            row.antenna2(),
                            row.time_mjd_seconds(),
                            row.time_centroid_mjd_seconds(),
                        )
                        .err();
                }
            }
        },
    )?;
    if invalid || &actual.finish() != selection.rows() {
        return Err(ObservationOwnerError::PhysicalSelectionMismatch);
    }
    if let Some(error) = pointing_domain_error {
        return Err(error.into());
    }
    pointing_query_domain
        .finish()?
        .ok_or(ObservationOwnerError::PhysicalSelectionMismatch)
}

#[cfg(all(test, unix))]
pub(crate) fn validate_test_physical_selection(
    measurement_set: &MeasurementSet,
    selection: &ObservationSelection,
    content_budget: SelectedObservationContentBudget,
) -> Result<SelectedPointingQueryDomain, ObservationOwnerError> {
    validate_physical_selection(measurement_set, selection, content_budget)
}

fn main_data_description_id(
    measurement_set: &MeasurementSet,
    row: usize,
) -> Result<usize, ObservationOwnerError> {
    let value = measurement_set
        .main_table()
        .column_accessor("DATA_DESC_ID")?
        .get(row)?
        .ok_or(ObservationOwnerError::PredictionAddress)?;
    let Value::Scalar(ScalarValue::Int32(data_description_id)) = value else {
        return Err(ObservationOwnerError::PredictionAddress);
    };
    usize::try_from(*data_description_id).map_err(|_| ObservationOwnerError::PredictionAddress)
}

fn model_cell_shape(
    measurement_set: &MeasurementSet,
    data_description_id: usize,
) -> Result<[usize; 2], ObservationOwnerError> {
    let data_description = measurement_set.data_description()?;
    let spectral_window_id =
        usize::try_from(data_description.spectral_window_id(data_description_id)?)
            .map_err(|_| ObservationOwnerError::PredictionAddress)?;
    let polarization_id = usize::try_from(data_description.polarization_id(data_description_id)?)
        .map_err(|_| ObservationOwnerError::PredictionAddress)?;
    let channels = usize::try_from(
        measurement_set
            .spectral_window()?
            .num_chan(spectral_window_id)?,
    )
    .map_err(|_| ObservationOwnerError::PredictionAddress)?;
    let correlations = usize::try_from(measurement_set.polarization()?.num_corr(polarization_id)?)
        .map_err(|_| ObservationOwnerError::PredictionAddress)?;
    Ok([correlations, channels])
}

fn model_cell_bytes(
    measurement_set: &MeasurementSet,
    data_description_id: usize,
) -> Result<u64, ObservationOwnerError> {
    let [correlations, channels] = model_cell_shape(measurement_set, data_description_id)?;
    u64::try_from(correlations)
        .ok()
        .and_then(|correlations| {
            u64::try_from(channels)
                .ok()
                .and_then(|channels| correlations.checked_mul(channels))
        })
        .and_then(|samples| samples.checked_mul(std::mem::size_of::<Complex32>() as u64))
        .ok_or(ObservationOwnerError::PredictionAddress)
}

fn derive_column_storage_plan(
    measurement_set: &MeasurementSet,
    selection: &ObservationSelection,
    column: &str,
    create_if_absent: bool,
    content_budget: SelectedObservationContentBudget,
) -> Result<SelectedVisibilityStoragePlan, ObservationOwnerError> {
    let has_column = measurement_set
        .main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column(column));
    let data_description_count = measurement_set.data_description()?.row_count();
    let mut bytes_by_data_description = Vec::with_capacity(data_description_count);
    for data_description_id in 0..data_description_count {
        bytes_by_data_description.push(model_cell_bytes(measurement_set, data_description_id)?);
    }
    let mut selected_write_bytes = 0_u64;
    let mut maximum_cell_bytes = 0_u64;
    let mut selected_error = false;
    measurement_set.visit_selected_observation_rows(
        &SelectedObservationRowSelection::from_compiled(selection),
        content_budget.row_io_budget(),
        |row| {
            let Some(bytes) = usize::try_from(row.data_description_id())
                .ok()
                .and_then(|id| bytes_by_data_description.get(id))
                .copied()
            else {
                selected_error = true;
                return;
            };
            selected_write_bytes = selected_write_bytes.saturating_add(bytes);
            maximum_cell_bytes = maximum_cell_bytes.max(bytes);
        },
    )?;
    if selected_error || selected_write_bytes == u64::MAX {
        return Err(ObservationOwnerError::PredictionAddress);
    }
    let mut additional_persistent_bytes = 0_u64;
    if create_if_absent && !has_column {
        let mut invalid_data_description = false;
        let plan =
            crate::MsReadPlan::new(measurement_set.row_count(), content_budget.row_io_budget())
                .map_err(|_| ObservationOwnerError::PredictionAddress)?;
        measurement_set.visit_main_row_selection_blocks(plan, |block| {
            for offset in 0..block.len() {
                let fact = block
                    .row(offset)
                    .expect("offset is bounded by MAIN selection block length");
                let Some(bytes) = usize::try_from(fact.data_description_id())
                    .ok()
                    .and_then(|id| bytes_by_data_description.get(id))
                    .copied()
                else {
                    invalid_data_description = true;
                    continue;
                };
                additional_persistent_bytes = additional_persistent_bytes.saturating_add(bytes);
                maximum_cell_bytes = maximum_cell_bytes.max(bytes);
            }
        })?;
        if invalid_data_description || additional_persistent_bytes == u64::MAX {
            return Err(ObservationOwnerError::PredictionAddress);
        }
    }
    let write_bytes = additional_persistent_bytes
        .checked_add(selected_write_bytes)
        .ok_or(ObservationOwnerError::PredictionAddress)?;
    let possible_buffer_rows = if create_if_absent && !has_column {
        u64::try_from(measurement_set.row_count())
            .map_err(|_| ObservationOwnerError::PredictionAddress)?
    } else {
        selection.rows().selected_row_count()
    }
    .min(VISIBILITY_WRITE_BATCH_ROWS);
    Ok(SelectedVisibilityStoragePlan {
        additional_persistent_bytes,
        write_bytes,
        maximum_cell_bytes,
        write_buffer_bytes: maximum_cell_bytes
            .checked_mul(possible_buffer_rows)
            .ok_or(ObservationOwnerError::PredictionAddress)?,
    })
}

#[cfg(unix)]
fn queue_visibility_cell(
    table: &mut Table,
    column: &str,
    row: usize,
    value: ArrayValue,
) -> Result<(), ObservationOwnerError> {
    {
        let mut prepared = table.row_accessor_mut().prepare(&[column])?;
        prepared.seek(row)?;
        prepared.set_value_at(0, Value::Array(value))?;
    }
    Ok(())
}

#[cfg(unix)]
fn persist_visibility_rows(
    table: &mut Table,
    columns: &[&str],
    rows: &[usize],
) -> Result<(), ObservationOwnerError> {
    table.prepare_write().save_selected_rows(columns, rows)?;
    table.discard_persisted_cell_updates(columns, rows);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MeasurementSetBuilder, OptionalMainColumn, SubtableId, column_def::ColumnDef, schema,
        test_helpers::default_value_for_def,
    };
    use casa_imaging_model::{
        AntennaSelection, CorrelationProduct, CorrelationSelection, CorrelationType,
        DataDescriptionSelection, IdSelection, IntentSelection, PointingTimeSampling, RowSelection,
        SelectedMainRow, SelectedRows, SpectralWindowSelection, TimeSelection, UvSelection,
        compile_observation,
    };
    use casa_tables::TableOptions;
    use casa_types::{ArrayValue, Complex32, RecordField, RecordValue};
    use ndarray::ArrayD;

    fn identity(byte: u8) -> LogicalIdentity {
        LogicalIdentity::from_bytes([byte; 32])
    }

    fn one_row_selection() -> ObservationSelection {
        one_row_selection_with_channels(vec![0])
    }

    fn one_row_selection_with_channels(channel_indices: Vec<u32>) -> ObservationSelection {
        one_row_selection_with_total_rows(channel_indices, 1)
    }

    fn one_row_selection_with_total_rows(
        channel_indices: Vec<u32>,
        total_rows: usize,
    ) -> ObservationSelection {
        let rows = (0..total_rows)
            .map(|row| SelectedMainRow::new(row as u64, 0))
            .collect::<Vec<_>>();
        ObservationSelection::new(
            SelectedRows::from_ordered_main_rows(total_rows as u64, rows)
                .expect("ordered selection manifest"),
            RowSelection::new(
                IdSelection::All,
                TimeSelection::All,
                UvSelection::All,
                AntennaSelection::All,
                IdSelection::All,
                IdSelection::All,
                IntentSelection::All,
                IdSelection::All,
            ),
            vec![DataDescriptionSelection::new(0, 0, 0)],
            vec![SpectralWindowSelection::new(0, channel_indices)],
            vec![CorrelationSelection::new(
                0,
                vec![CorrelationProduct::new(0, CorrelationType::CircularRr)],
            )],
        )
    }

    #[test]
    #[cfg(unix)]
    fn resolution_binds_exact_full_nonuniform_spw_for_a_subselection() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("nonuniform-subselection.ms");
        let frequencies_hz = [1.0e9, 1.001e9, 1.004e9, 1.010e9];
        let widths_hz = [-1.0e6, -1.1e6, -2.5e6, -4.0e6];
        create_ms_columns_with_spectral_coordinates(
            &path,
            false,
            false,
            &frequencies_hz,
            &widths_hz,
        );
        let request = SelectedObservationResolutionRequest::new(
            path.display().to_string(),
            identity(2),
            one_row_selection_with_channels(vec![1, 3]),
            VisibilityColumn::Data,
            WeightColumn::Weight,
            Vec::new(),
            ModelStateIdentity::Empty,
            SelectedObservationContentBudget::new(1 << 20, 1, 4),
            casa_test_support::deterministic_measures_provider_for_identity([90; 32]),
        );

        let (snapshot_input, _) = resolve_selected_observation(request)
            .expect("resolve exact physical catalog")
            .into_parts();
        let snapshot = compile_observation(snapshot_input).expect("compile bound catalog");
        let spectral_window = &snapshot.sources()[0].selection().spectral_windows()[0];
        assert_eq!(spectral_window.channel_indices(), &[1, 3]);
        let catalog = spectral_window
            .coordinate_catalog()
            .expect("owner-certified full SPW catalog");
        assert_eq!(catalog.channel_frequencies_hz(), frequencies_hz);
        assert_eq!(catalog.first_channel_width_hz(), widths_hz[0]);
        let measurement_set = MeasurementSet::open(&path).expect("reopen physical catalog");
        validate_selected_coordinates(&measurement_set, snapshot.sources()[0].selection())
            .expect("runtime access accepts the exact owner-certified catalog");
    }

    #[test]
    #[cfg(unix)]
    fn full_spw_catalog_construction_obeys_the_source_content_budget() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("catalog-budget.ms");
        create_ms_columns_with_spectral_coordinates(
            &path,
            false,
            false,
            &[1.0e9, 1.001e9, 1.004e9, 1.010e9],
            &[1.0e6; 4],
        );
        let measurement_set = MeasurementSet::open(&path).expect("open test MS");

        assert!(matches!(
            bind_physical_spectral_coordinates(
                &measurement_set,
                &one_row_selection_with_channels(vec![1, 3]),
                SelectedObservationContentBudget::new(79, 1, 4),
            ),
            Err(ObservationOwnerError::SpectralCoordinateCatalogBudget {
                required_bytes: 80,
                available_bytes: 79,
            })
        ));
    }

    fn request(path: &Path) -> SelectedObservationResolutionRequest {
        SelectedObservationResolutionRequest::new(
            path.display().to_string(),
            identity(2),
            one_row_selection(),
            VisibilityColumn::Data,
            WeightColumn::Weight,
            Vec::new(),
            ModelStateIdentity::Empty,
            SelectedObservationContentBudget::new(1 << 20, 1, 4),
            casa_test_support::deterministic_measures_provider_for_identity([90; 32]),
        )
    }

    #[test]
    fn physical_selection_batches_rows_within_the_owner_content_budget() {
        let content_budget = SelectedObservationContentBudget::new(64 << 20, 2, 4);
        let io_budget = content_budget.row_io_budget();

        assert_eq!(
            io_budget,
            crate::MsSelectionIoBudget {
                available_bytes: 64 << 20,
                maximum_live_blocks: 2,
                requested_bytes_per_row: crate::SelectedObservationRow::STORAGE_BYTES_PER_ROW,
                storage_alignment_rows: None,
            }
        );
        let plan = crate::MsReadPlan::new(42_320, io_budget).expect("budget admits the scan");
        assert_eq!(plan.rows_per_block, 42_320);
        assert_eq!(plan.row_count, 42_320);
    }

    #[test]
    #[cfg(unix)]
    fn physical_selection_binds_per_antenna_time_and_centroid_domains() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("pointing-query-domain.ms");
        create_ms_columns_with_spectral_coordinates_and_rows(
            &path,
            false,
            false,
            &[1.0e9],
            &[1.0e6],
            3,
        );
        let mut measurement_set = MeasurementSet::open(&path).expect("open test MS");
        for (row, antenna1, antenna2, time, centroid) in [
            (0, 0, 1, 10.0, 11.0),
            (1, 1, 2, 20.0, 22.0),
            (2, 0, 2, 30.0, 33.0),
        ] {
            crate::subtables::set_scalar(
                measurement_set.main_table_mut(),
                row,
                "ANTENNA1",
                ScalarValue::Int32(antenna1),
            )
            .expect("set ANTENNA1");
            crate::subtables::set_scalar(
                measurement_set.main_table_mut(),
                row,
                "ANTENNA2",
                ScalarValue::Int32(antenna2),
            )
            .expect("set ANTENNA2");
            crate::subtables::set_scalar(
                measurement_set.main_table_mut(),
                row,
                "TIME",
                ScalarValue::Float64(time),
            )
            .expect("set TIME");
            crate::subtables::set_scalar(
                measurement_set.main_table_mut(),
                row,
                "TIME_CENTROID",
                ScalarValue::Float64(centroid),
            )
            .expect("set TIME_CENTROID");
        }
        measurement_set.save().expect("save selected MAIN facts");
        drop(measurement_set);
        let measurement_set =
            MeasurementSet::open_retained_read(&path).expect("reopen lazy selected MAIN facts");

        let selection = one_row_selection_with_total_rows(vec![0], 3);
        let domain = validate_physical_selection(
            &measurement_set,
            &selection,
            SelectedObservationContentBudget::new(1 << 20, 1, 4),
        )
        .expect("bind selected pointing query domain");

        assert_eq!(domain.antenna_ids().collect::<Vec<_>>(), vec![0, 1, 2]);
        assert_eq!(
            domain.time_bounds_mjd_seconds(0, PointingTimeSampling::VisibilityTime),
            Some([10.0, 30.0])
        );
        assert_eq!(
            domain.time_bounds_mjd_seconds(1, PointingTimeSampling::VisibilityTime),
            Some([10.0, 20.0])
        );
        assert_eq!(
            domain.time_bounds_mjd_seconds(2, PointingTimeSampling::VisibilityTimeCentroid),
            Some([22.0, 33.0])
        );
        assert_eq!(
            domain.time_bounds_mjd_seconds(3, PointingTimeSampling::VisibilityTimeCentroid),
            None
        );
    }

    fn create_ms(path: &Path, model_data: bool) {
        create_ms_columns(path, model_data, false);
    }

    fn create_ms_columns(path: &Path, model_data: bool, corrected_data: bool) {
        create_ms_columns_with_spectral_coordinates(
            path,
            model_data,
            corrected_data,
            &[1.0e9],
            &[1.0e6],
        );
    }

    fn create_ms_columns_with_spectral_coordinates(
        path: &Path,
        model_data: bool,
        corrected_data: bool,
        channel_frequencies_hz: &[f64],
        channel_widths_hz: &[f64],
    ) {
        create_ms_columns_with_spectral_coordinates_and_rows(
            path,
            model_data,
            corrected_data,
            channel_frequencies_hz,
            channel_widths_hz,
            1,
        );
    }

    fn create_ms_columns_with_spectral_coordinates_and_rows(
        path: &Path,
        model_data: bool,
        corrected_data: bool,
        channel_frequencies_hz: &[f64],
        channel_widths_hz: &[f64],
        row_count: usize,
    ) {
        assert_eq!(channel_frequencies_hz.len(), channel_widths_hz.len());
        assert!(!channel_frequencies_hz.is_empty());
        assert!(row_count > 0);
        let channel_count = channel_frequencies_hz.len();
        let mut builder = MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data);
        if model_data {
            builder = builder.with_main_column(OptionalMainColumn::ModelData);
        }
        if corrected_data {
            builder = builder.with_main_column(OptionalMainColumn::CorrectedData);
        }
        let mut measurement_set =
            MeasurementSet::create(path, builder).expect("create test MeasurementSet");

        measurement_set
            .subtable_mut(SubtableId::Polarization)
            .expect("POLARIZATION subtable")
            .add_row(row(
                schema::polarization::REQUIRED_COLUMNS,
                &[
                    ("NUM_CORR", Value::Scalar(ScalarValue::Int32(1))),
                    (
                        "CORR_TYPE",
                        Value::Array(ArrayValue::Int32(
                            ArrayD::from_shape_vec(vec![1], vec![5]).expect("one RR code"),
                        )),
                    ),
                    (
                        "CORR_PRODUCT",
                        Value::Array(ArrayValue::Int32(
                            ArrayD::from_shape_vec(vec![2, 1], vec![0, 0])
                                .expect("one receptor pair"),
                        )),
                    ),
                ],
            ))
            .expect("add POLARIZATION row");
        measurement_set
            .subtable_mut(SubtableId::SpectralWindow)
            .expect("SPECTRAL_WINDOW subtable")
            .add_row(row(
                schema::spectral_window::REQUIRED_COLUMNS,
                &[
                    (
                        "NUM_CHAN",
                        Value::Scalar(ScalarValue::Int32(
                            i32::try_from(channel_count).expect("test channel count"),
                        )),
                    ),
                    (
                        "CHAN_FREQ",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(
                                vec![channel_count],
                                channel_frequencies_hz.to_vec(),
                            )
                            .expect("channel frequencies"),
                        )),
                    ),
                    (
                        "CHAN_WIDTH",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(vec![channel_count], channel_widths_hz.to_vec())
                                .expect("channel widths"),
                        )),
                    ),
                    (
                        "EFFECTIVE_BW",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(
                                vec![channel_count],
                                channel_widths_hz.iter().map(|width| width.abs()).collect(),
                            )
                            .expect("effective bandwidths"),
                        )),
                    ),
                    (
                        "RESOLUTION",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(
                                vec![channel_count],
                                channel_widths_hz.iter().map(|width| width.abs()).collect(),
                            )
                            .expect("channel resolutions"),
                        )),
                    ),
                    (
                        "REF_FREQUENCY",
                        Value::Scalar(ScalarValue::Float64(channel_frequencies_hz[0])),
                    ),
                    (
                        "TOTAL_BANDWIDTH",
                        Value::Scalar(ScalarValue::Float64(
                            channel_widths_hz.iter().map(|width| width.abs()).sum(),
                        )),
                    ),
                    ("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(5))),
                ],
            ))
            .expect("add SPECTRAL_WINDOW row");
        measurement_set
            .subtable_mut(SubtableId::DataDescription)
            .expect("DATA_DESCRIPTION subtable")
            .add_row(row(
                schema::data_description::REQUIRED_COLUMNS,
                &[
                    ("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(0))),
                    ("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                ],
            ))
            .expect("add DATA_DESCRIPTION row");

        let main_schema = measurement_set
            .main_table()
            .schema()
            .expect("MAIN schema")
            .clone();
        let main = main_schema
            .columns()
            .iter()
            .map(|column| {
                let value = match column.name() {
                    "DATA_DESC_ID" => Value::Scalar(ScalarValue::Int32(0)),
                    "DATA" | "MODEL_DATA" | "CORRECTED_DATA" => {
                        Value::Array(ArrayValue::Complex32(
                            ArrayD::from_shape_vec(
                                vec![1, channel_count],
                                vec![Complex32::new(1.0, 0.0); channel_count],
                            )
                            .expect("channel visibilities"),
                        ))
                    }
                    "FLAG" => Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(vec![1, channel_count], vec![false; channel_count])
                            .expect("channel flags"),
                    )),
                    "WEIGHT" => Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![1], vec![1.0]).expect("one weight"),
                    )),
                    _ => crate::test_helpers::default_value(column.name()),
                };
                RecordField::new(column.name(), value)
            })
            .collect();
        let main = RecordValue::new(main);
        for _ in 0..row_count {
            measurement_set
                .main_table_mut()
                .add_row(main.clone())
                .expect("add MAIN row");
        }
        measurement_set.save().expect("save test MeasurementSet");
    }

    fn row(definitions: &[ColumnDef], overrides: &[(&str, Value)]) -> RecordValue {
        RecordValue::new(
            definitions
                .iter()
                .map(|definition| {
                    let value = overrides
                        .iter()
                        .find_map(|(name, value)| (*name == definition.name).then(|| value.clone()))
                        .unwrap_or_else(|| default_value_for_def(definition));
                    RecordField::new(definition.name, value)
                })
                .collect(),
        )
    }

    /// MAIN's keywords and the MeasurementSet directory's entries, apart from
    /// casacore's own `table.lock`, which any locked open creates.
    fn persisted_state(path: &Path) -> (RecordValue, Vec<String>) {
        let keywords = MeasurementSet::open(path)
            .expect("open MeasurementSet")
            .main_table()
            .keywords()
            .clone();
        let mut entries = std::fs::read_dir(path)
            .expect("list MeasurementSet")
            .map(|entry| {
                entry
                    .expect("directory entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .filter(|name| name != "table.lock")
            .collect::<Vec<_>>();
        entries.sort();
        (keywords, entries)
    }

    #[test]
    #[cfg(unix)]
    fn a_measurement_set_as_casa_wrote_it_resolves_and_is_left_unchanged() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("plain.ms");
        create_ms(&path, false);
        let before = persisted_state(&path);

        let resolved = resolve_selected_observation(request(&path)).expect("resolve plain MS");
        let (snapshot_input, access) = resolved.into_parts();
        assert_eq!(access.source_binding().measurement_set(), 0);
        let snapshot = compile_observation(snapshot_input).expect("compile snapshot");
        let source = &snapshot.sources()[0];
        assert_eq!(source.input_ordinal(), 0);
        assert_eq!(source.selection().rows(), one_row_selection().rows());
        assert_eq!(
            source.columns(),
            SelectedColumns::new(
                VisibilityColumn::Data,
                FlagPolicy::FlagOrFlagRow,
                WeightColumn::Weight
            )
        );
        assert!(!source.corrected_data_present());
        assert_eq!(
            snapshot.reference_data(),
            &[(ReferenceDataKind::Measures, identity(90))]
        );
        assert_eq!(persisted_state(&path), before);
    }

    #[test]
    #[cfg(unix)]
    fn a_selected_column_missing_from_main_is_refused() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("no-corrected.ms");
        create_ms(&path, false);
        let request = SelectedObservationResolutionRequest::new(
            path.display().to_string(),
            identity(2),
            one_row_selection(),
            VisibilityColumn::CorrectedData,
            WeightColumn::Weight,
            Vec::new(),
            ModelStateIdentity::Empty,
            SelectedObservationContentBudget::new(1 << 20, 1, 4),
            casa_test_support::deterministic_measures_provider_for_identity([90; 32]),
        );

        assert!(matches!(
            resolve_selected_observation(request),
            Err(ObservationOwnerError::MissingColumn {
                column: "CORRECTED_DATA"
            })
        ));
    }

    #[test]
    #[cfg(unix)]
    fn interrupted_model_column_write_leaves_existing_cells_without_rollback() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("model-abort.ms");
        create_ms(&path, true);

        {
            let mut writer = SelectedVisibilityWrite::begin(
                &path,
                SelectedVisibilityWriteTargets::new(true, false),
            )
            .expect("begin write");
            writer
                .write(MsColumnKind::ModelData, 0, 0, 0, Complex32::new(9.0, -2.0))
                .expect("write prediction");
        }

        let raw = Table::open(TableOptions::new(&path)).expect("inspect marked MAIN directly");
        let Value::Array(ArrayValue::Complex32(values)) = raw
            .column_accessor("MODEL_DATA")
            .expect("MODEL_DATA accessor")
            .get(0)
            .expect("read existing MODEL_DATA")
            .expect("defined MODEL_DATA cell")
        else {
            panic!("MODEL_DATA is complex")
        };
        assert_eq!(
            values[[0, 0]],
            Complex32::new(1.0, 0.0),
            "begin and an unflushed prediction must not persist a destructive zero pass"
        );
        // As after an interrupted CASA write, the MeasurementSet opens; the
        // next run recomputes MODEL_DATA.
        MeasurementSet::open(&path).expect("an interrupted write leaves a readable MS");
    }

    #[test]
    #[cfg(unix)]
    fn model_column_storage_plan_reserves_capacity_only_for_creation() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("model-plan-create.ms");
        create_ms(&path, false);
        let resolved = resolve_selected_observation(request(&path)).expect("resolve owner");
        let plan = resolved
            .access
            .selected_visibility_storage_plan(SelectedVisibilityWriteTargets::new(true, false))
            .expect("MODEL_DATA storage plan");

        assert_eq!(plan.additional_persistent_bytes(), 8);
        assert_eq!(plan.write_bytes(), 16);
        assert_eq!(plan.maximum_cell_bytes(), 8);
        assert_eq!(plan.write_buffer_bytes(), 8);

        let existing_path = directory.path().join("model-plan-overwrite.ms");
        create_ms(&existing_path, true);
        let existing = resolve_selected_observation(request(&existing_path))
            .expect("resolve existing MODEL_DATA owner");
        let existing_plan = existing
            .access
            .selected_visibility_storage_plan(SelectedVisibilityWriteTargets::new(true, false))
            .expect("MODEL_DATA storage plan");

        assert_eq!(existing_plan.additional_persistent_bytes(), 0);
        assert_eq!(existing_plan.write_bytes(), 8);
        assert_eq!(existing_plan.maximum_cell_bytes(), 8);
        assert_eq!(existing_plan.write_buffer_bytes(), 8);
    }

    #[test]
    #[cfg(unix)]
    fn read_only_resolution_does_not_traverse_unselected_rows_for_a_write_plan() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("read-only-plan.ms");
        create_ms(&path, false);

        let mut measurement_set = MeasurementSet::open(&path).expect("reopen test MS");
        let schema = measurement_set
            .main_table()
            .schema()
            .expect("MAIN schema")
            .clone();
        let second_row = schema
            .columns()
            .iter()
            .map(|column| {
                let value = match column.name() {
                    "DATA_DESC_ID" => Value::Scalar(ScalarValue::Int32(99)),
                    "FIELD_ID" => Value::Scalar(ScalarValue::Int32(1)),
                    "DATA" => Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(vec![1, 1], vec![Complex32::new(1.0, 0.0)])
                            .expect("one visibility"),
                    )),
                    "FLAG" => Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(vec![1, 1], vec![false]).expect("one flag"),
                    )),
                    "WEIGHT" => Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![1], vec![1.0]).expect("one weight"),
                    )),
                    _ => crate::test_helpers::default_value(column.name()),
                };
                RecordField::new(column.name(), value)
            })
            .collect();
        measurement_set
            .main_table_mut()
            .add_row(RecordValue::new(second_row))
            .expect("add unselected row");
        measurement_set.save().expect("save two-row MS");

        let selection = ObservationSelection::new(
            SelectedRows::from_ordered_main_rows(2, [SelectedMainRow::new(0, 0)])
                .expect("selected row manifest"),
            RowSelection::new(
                IdSelection::Only(vec![0]),
                TimeSelection::All,
                UvSelection::All,
                AntennaSelection::All,
                IdSelection::All,
                IdSelection::All,
                IntentSelection::All,
                IdSelection::All,
            ),
            vec![DataDescriptionSelection::new(0, 0, 0)],
            vec![SpectralWindowSelection::new(0, vec![0])],
            vec![CorrelationSelection::new(
                0,
                vec![CorrelationProduct::new(0, CorrelationType::CircularRr)],
            )],
        );
        let request = SelectedObservationResolutionRequest::new(
            path.display().to_string(),
            identity(2),
            selection,
            VisibilityColumn::Data,
            WeightColumn::Weight,
            Vec::new(),
            ModelStateIdentity::Empty,
            SelectedObservationContentBudget::new(1 << 20, 1, 4),
            casa_test_support::deterministic_measures_provider_for_identity([90; 32]),
        );

        resolve_selected_observation(request)
            .expect("read-only resolution must not plan an unrequested MODEL_DATA write");
    }

    #[test]
    #[cfg(unix)]
    fn completed_model_column_write_creates_model_data_and_adds_nothing_else() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("model-commit.ms");
        create_ms(&path, false);
        let (keywords_before, _) = persisted_state(&path);

        let mut writer =
            SelectedVisibilityWrite::begin(&path, SelectedVisibilityWriteTargets::new(true, false))
                .expect("begin write");
        writer
            .write(MsColumnKind::ModelData, 0, 0, 0, Complex32::new(4.5, -1.25))
            .expect("write prediction");
        writer.complete().expect("complete write");

        let reopened = MeasurementSet::open(&path).expect("reopen committed MS");
        let schema = reopened.main_table().schema().expect("MAIN schema");
        assert!(schema.contains_column("MODEL_DATA"));
        let model_column = reopened
            .data_column(crate::VisibilityDataColumn::ModelData)
            .expect("MODEL_DATA column");
        let ArrayValue::Complex32(values) = model_column.get(0).expect("MODEL_DATA cell") else {
            panic!("MODEL_DATA is complex")
        };
        assert_eq!(values[[0, 0]], Complex32::new(4.5, -1.25));
        drop(reopened);
        let (keywords_after, entries) = persisted_state(&path);
        assert_eq!(keywords_after, keywords_before);
        assert!(
            entries.iter().all(|name| !name.contains("casa-rs")),
            "the write left casa-rs files: {entries:?}"
        );
    }

    #[test]
    #[cfg(unix)]
    fn model_column_creation_is_not_persisted_one_row_at_a_time() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("model-create-throughput.ms");
        create_ms_columns_with_spectral_coordinates_and_rows(
            &path,
            false,
            false,
            &[1.0e9],
            &[1.0e6],
            4_096,
        );

        let started = std::time::Instant::now();
        let writer =
            SelectedVisibilityWrite::begin(&path, SelectedVisibilityWriteTargets::new(true, false))
                .expect("begin MODEL_DATA creation");
        let elapsed = started.elapsed();
        drop(writer);

        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "creating a 4096-row MODEL_DATA column took {elapsed:?}; the creation path must not persist one row per transaction"
        );
    }

    #[test]
    #[cfg(unix)]
    fn model_column_updates_are_persisted_in_bounded_row_batches() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("model-update-throughput.ms");
        create_ms_columns_with_spectral_coordinates_and_rows(
            &path,
            true,
            false,
            &[1.0e9],
            &[1.0e6],
            4_096,
        );

        let started = std::time::Instant::now();
        let mut writer =
            SelectedVisibilityWrite::begin(&path, SelectedVisibilityWriteTargets::new(true, false))
                .expect("begin MODEL_DATA update");
        for row in 0..4_096 {
            writer
                .write(
                    MsColumnKind::ModelData,
                    row,
                    0,
                    0,
                    Complex32::new(row as f32, -1.0),
                )
                .expect("write predicted row");
        }
        writer.complete().expect("complete MODEL_DATA update");
        let elapsed = started.elapsed();

        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "updating a 4096-row MODEL_DATA column took {elapsed:?}; row updates must use bounded batches"
        );
        let reopened = MeasurementSet::open(&path).expect("reopen updated MODEL_DATA");
        let model = reopened
            .data_column(crate::VisibilityDataColumn::ModelData)
            .expect("MODEL_DATA column");
        let ArrayValue::Complex32(last) = model.get(4_095).expect("last MODEL_DATA row") else {
            panic!("MODEL_DATA is complex")
        };
        assert_eq!(last[[0, 0]], Complex32::new(4_095.0, -1.0));
    }

    #[test]
    #[cfg(unix)]
    fn combined_visibility_write_updates_both_columns_under_one_lock() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("combined-write.ms");
        create_ms_columns(&path, true, true);
        let before = persisted_state(&path);

        let mut writer =
            SelectedVisibilityWrite::begin(&path, SelectedVisibilityWriteTargets::new(true, true))
                .expect("begin combined write");
        writer
            .write(MsColumnKind::ModelData, 0, 0, 0, Complex32::new(4.0, -1.0))
            .expect("write model");
        writer
            .write(
                MsColumnKind::CorrectedData,
                0,
                0,
                0,
                Complex32::new(2.5, 0.5),
            )
            .expect("write corrected");
        writer.complete().expect("complete combined write");

        let reopened = MeasurementSet::open(&path).expect("reopen committed MS");
        let model_column = reopened
            .data_column(crate::VisibilityDataColumn::ModelData)
            .expect("MODEL_DATA");
        let ArrayValue::Complex32(model) = model_column.get(0).expect("model cell") else {
            panic!("MODEL_DATA is complex")
        };
        let corrected_column = reopened
            .data_column(crate::VisibilityDataColumn::CorrectedData)
            .expect("CORRECTED_DATA");
        let ArrayValue::Complex32(corrected) = corrected_column.get(0).expect("corrected cell")
        else {
            panic!("CORRECTED_DATA is complex")
        };
        assert_eq!(model[[0, 0]], Complex32::new(4.0, -1.0));
        assert_eq!(corrected[[0, 0]], Complex32::new(2.5, 0.5));
        drop(reopened);
        assert_eq!(persisted_state(&path), before);
    }
}
