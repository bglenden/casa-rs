// SPDX-License-Identifier: LGPL-3.0-or-later

//! Selected-observation access for native imaging: resolve a selected
//! MeasurementSet into compiler input, and write selected visibilities back.
//!
//! A MeasurementSet is read and written as CASA does. casacore's table locks
//! are the only coordination, and casa-rs stores nothing of its own in it: no
//! keywords, marker files, generations or identities.

use std::sync::Arc;

use casa_imaging_model::{
    FlagPolicy, ObservationSelection, ObservationSnapshotInput, ObservationSourceInput,
    ObservationSourceProvenance, SelectedColumns, SpectralWindowCoordinateCatalog,
    SpectralWindowSelection, VisibilityColumn, WeightColumn,
};
use casa_types::measures::MeasuresProvider;
use thiserror::Error;

use crate::selected_pointing::SelectedPointingQueryDomain;
use crate::subtables::SubTable;
use crate::{
    BoundObservationSourceError, BoundSelectedObservation, BoundSelectedObservationError,
    MeasurementSet, MsError, ObservationSourceBinding, SelectedObservationContentBudget,
    SelectedObservationEphemeris, SelectedObservationMeasures, SelectedObservationMeasuresError,
    SelectedObservationRowSelection,
};

mod visibility_write;

#[cfg(unix)]
pub use visibility_write::SelectedVisibilityWrite;
pub use visibility_write::SelectedVisibilityWriteTargets;

/// Explicit production inputs for resolving one selected MeasurementSet.
///
/// Cloning duplicates only this immutable resolution description. Every call to
/// [`resolve_selected_observation`] opens the MeasurementSet afresh and
/// returns a new affine access capability; live table authority is never cloned.
#[derive(Clone)]
pub struct SelectedObservationResolutionRequest {
    locator: String,
    selection: Arc<ObservationSelection>,
    visibility: VisibilityColumn,
    weights: WeightColumn,
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
    #[must_use]
    pub fn new(
        locator: impl Into<String>,
        selection: ObservationSelection,
        visibility: VisibilityColumn,
        weights: WeightColumn,
        content_budget: SelectedObservationContentBudget,
        measures_provider: Arc<dyn MeasuresProvider>,
    ) -> Self {
        Self {
            locator: locator.into(),
            selection: Arc::new(selection),
            visibility,
            weights,
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

    /// Set the source-read budget planned from [`Self::content_requirements`].
    #[must_use]
    pub fn with_content_budget(mut self, budget: SelectedObservationContentBudget) -> Self {
        self.binding.set_content_budget(budget);
        self
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
    let selection = bind_physical_spectral_coordinates(
        &measurement_set,
        &request.selection,
        request.content_budget,
    )?;
    let pointing_query_domain =
        selected_pointing_query_domain(&measurement_set, &selection, request.content_budget)?;
    let source = ObservationSourceInput::new(
        ObservationSourceProvenance::new(request.locator),
        selection,
        SelectedColumns::new(
            request.visibility,
            FlagPolicy::FlagOrFlagRow,
            request.weights,
        ),
        corrected_data_present,
    );
    let measures = SelectedObservationMeasures::new(request.measures_provider)?;
    let snapshot_input = ObservationSnapshotInput::new(vec![source]);
    // The one resolved source is the snapshot's first.
    let binding = ObservationSourceBinding::new(0, request.content_budget)
        .with_ephemeris(request.ephemeris)
        .with_pointing_query_domain(pointing_query_domain);
    Ok(ResolvedSelectedObservation {
        snapshot_input,
        access: ResolvedSelectedObservationAccess { binding, measures },
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
    /// The MeasurementSet cannot realize the requested selection: a selected
    /// SPECTRAL_WINDOW row is missing or its CHAN_FREQ and CHAN_WIDTH are
    /// empty or of unequal length, or no MAIN row is selected.
    #[error(
        "the MeasurementSet's spectral windows or MAIN rows do not realize the observation selection"
    )]
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
    /// The MAIN write lock could not be acquired: another handle in this
    /// process holds it, or waiting for it would deadlock with another
    /// process.
    #[error(
        "could not acquire the MeasurementSet MAIN write lock: another handle in this process \
         holds it, or waiting would deadlock with another process"
    )]
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
    /// A destination cannot be written in place in bounded memory: its
    /// storage manager's save rewrites every row of its group (ADR-0008).
    #[error("cannot write {column} in place in bounded memory: {reason}")]
    UnboundedWriteDestination {
        /// The destination column.
        column: &'static str,
        /// The storage that makes the write unbounded.
        reason: String,
    },
    /// A cell write named a column outside the bound destination set.
    #[error("selected visibility write named an unselected destination")]
    UnselectedWriteTarget,
}

/// The antennas and times of the selected rows, the domain the POINTING
/// catalog is prepared for.
fn selected_pointing_query_domain(
    measurement_set: &MeasurementSet,
    selection: &ObservationSelection,
    content_budget: SelectedObservationContentBudget,
) -> Result<SelectedPointingQueryDomain, ObservationOwnerError> {
    let row_selection = SelectedObservationRowSelection::from_compiled(selection);
    let mut pointing_query_domain = SelectedPointingQueryDomain::builder();
    let mut pointing_domain_error = None;
    measurement_set.visit_selected_observation_rows(
        &row_selection,
        content_budget.row_io_budget(),
        |row| {
            if pointing_domain_error.is_none() {
                pointing_domain_error = pointing_query_domain
                    .observe_row(
                        row.antenna1(),
                        row.antenna2(),
                        row.time_mjd_seconds(),
                        row.time_centroid_mjd_seconds(),
                    )
                    .err();
            }
        },
    )?;
    if let Some(error) = pointing_domain_error {
        return Err(error.into());
    }
    pointing_query_domain
        .finish()?
        .ok_or(ObservationOwnerError::PhysicalSelectionMismatch)
}

#[cfg(all(test, unix))]
pub(crate) fn test_pointing_query_domain(
    measurement_set: &MeasurementSet,
    selection: &ObservationSelection,
    content_budget: SelectedObservationContentBudget,
) -> Result<SelectedPointingQueryDomain, ObservationOwnerError> {
    selected_pointing_query_domain(measurement_set, selection, content_budget)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MeasurementSetBuilder, OptionalMainColumn, SubtableId, column_def::ColumnDef, schema,
        test_helpers::default_value_for_def,
    };
    use casa_imaging_model::{
        CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
        IdSelection, IntentSelection, MsColumnKind, PointingTimeSampling, RowSelection,
        SelectedMainRow, SelectedRows, SpectralWindowSelection, UvSelection, compile_observation,
    };
    use casa_tables::{Table, TableOptions};
    use casa_types::{ArrayValue, Complex32, RecordField, RecordValue, ScalarValue, Value};
    use ndarray::ArrayD;
    use std::path::Path;

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
            RowSelection::new(IdSelection::All, UvSelection::All, IntentSelection::All),
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
            one_row_selection_with_channels(vec![1, 3]),
            VisibilityColumn::Data,
            WeightColumn::Weight,
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
            one_row_selection(),
            VisibilityColumn::Data,
            WeightColumn::Weight,
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
        let domain = selected_pointing_query_domain(
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
            one_row_selection(),
            VisibilityColumn::CorrectedData,
            WeightColumn::Weight,
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
    fn resolution_ignores_unselected_rows() {
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
                UvSelection::All,
                IntentSelection::All,
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
            selection,
            VisibilityColumn::Data,
            WeightColumn::Weight,
            SelectedObservationContentBudget::new(1 << 20, 1, 4),
            casa_test_support::deterministic_measures_provider_for_identity([90; 32]),
        );

        resolve_selected_observation(request)
            .expect("an unselected row with an unknown DATA_DESC_ID is not read");
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
