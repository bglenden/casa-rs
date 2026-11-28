// SPDX-License-Identifier: LGPL-3.0-or-later
//! Physical creation shared by grid-based plans and row-native producers.
//! A row-native producer can supply flags, categories and weights as recorded
//! instead of reconstructing those columns from the logical observation flags.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::Path;

use casa_core::{ArrayValue, ScalarValue};
use casa_observation::ObservationError;
use casa_tables::{ColumnOverrides, streamed_tiled::StreamedTiledShapeValueType};
use ndarray::ArrayD;

use crate::observation_io::update::{request_error, storage};
use crate::write_session::{
    MeasurementSetArrayColumnPlan, MeasurementSetArrayShapePlan, MeasurementSetColumnStorage,
    MeasurementSetWritePlan, MeasurementSetWriteResources, MeasurementSetWriteSession,
    MeasurementSetWriteTelemetry, standard_main_scalar_column_plans,
};
use crate::{MeasurementSet, MeasurementSetBuilder};

/// The native MAIN cells of one acquisition. The producer's metadata must
/// supply the referenced subtable rows and the measure frames and units.
/// Boolean element flags, the row flag and each category are independent.
pub struct Msv2CreationRow<'a> {
    /// Antenna rows in stored baseline order.
    pub antennas: [i32; 2],
    /// Observing array number.
    pub array: i32,
    /// Row of DATA_DESCRIPTION.
    pub data_description: i32,
    /// Row of FIELD.
    pub field: i32,
    /// Row of OBSERVATION.
    pub observation: i32,
    /// Scan number in the observing session.
    pub scan: i32,
    /// Time in seconds in MAIN's epoch frame.
    pub time: f64,
    /// Interval and exposure in seconds, for producers supplying one value.
    pub integration: f64,
    /// Row flag, independent of element and category flags.
    pub flagged: bool,
    /// UVW in metres in MAIN's UVW frame.
    pub uvw: [f64; 3],
    /// DATA, CORRECTED_DATA, MODEL_DATA, correlation × channel.
    pub visibilities: [&'a ArrayValue; 3],
    /// FLAG, correlation × channel.
    pub flags: &'a ArrayValue,
    /// FLAG_CATEGORY, correlation × channel × category.
    pub categories: &'a ArrayValue,
    /// Stored WEIGHT, per correlation.
    pub weight: &'a ArrayValue,
    /// Stored SIGMA, per correlation.
    pub sigma: &'a ArrayValue,
}

/// Writes MAIN's variable-shaped acquisition cells and scalar keys to a new
/// MSv2. All columns and row shapes are planned before their first write.
pub struct Msv2MainWriter {
    pub(super) session: MeasurementSetWriteSession,
    overrides: ColumnOverrides,
}

impl Msv2MainWriter {
    /// Construct the metadata and start the MAIN writers at the caller's
    /// staging path. The caller populates metadata, then finalizes with
    /// [`Self::finish`] before publishing its stage.
    ///
    /// # Errors
    /// If the descriptor or planned writers cannot be created.
    pub fn create(
        path: &Path,
        builder: MeasurementSetBuilder,
        plan: MeasurementSetWritePlan,
    ) -> Result<(MeasurementSet, Self), ObservationError> {
        let ms = MeasurementSet::create(path, builder).map_err(storage)?;
        let writer = Self::start(path, plan)?;
        Ok((ms, writer))
    }

    /// Start the planned MAIN writers for an existing, unsaved creation
    /// descriptor at the staging path.
    ///
    /// # Errors
    /// If the planned writers cannot be created.
    pub fn start(path: &Path, plan: MeasurementSetWritePlan) -> Result<Self, ObservationError> {
        let mut overrides = ColumnOverrides::for_row_count(plan.row_count());
        for column in plan
            .columns()
            .iter()
            .filter(|c| c.storage_manager != MeasurementSetColumnStorage::Standard)
        {
            overrides.insert_deferred(&column.name);
        }
        let session = MeasurementSetWriteSession::start(path, plan).map_err(storage)?;
        Ok(Self { session, overrides })
    }

    /// Append an acquisition with raw flags and per-correlation weights.
    /// The source provides all three visibility columns. Feed IDs are 0,
    /// processor and state IDs are -1, and time centroid equals time.
    ///
    /// # Errors
    /// If a cell's shape or value kind disagrees with the plan, or a column
    /// cannot be written. The caller then discards its staging product.
    pub fn push_acquisition(&mut self, row: Msv2CreationRow<'_>) -> Result<(), ObservationError> {
        let writer = &mut self.session;
        for (column, value) in ["DATA", "CORRECTED_DATA", "MODEL_DATA"]
            .into_iter()
            .zip(row.visibilities)
        {
            let ArrayValue::Complex32(array) = value else {
                return Err(request_error(format!(
                    "MAIN.{column} requires a Complex32 array"
                )));
            };
            writer
                .push_complex32_row(column, array.shape(), &casa_order(array))
                .map_err(storage)?;
        }
        for (column, value) in [("FLAG", row.flags), ("FLAG_CATEGORY", row.categories)] {
            let ArrayValue::Bool(array) = value else {
                return Err(request_error(format!(
                    "MAIN.{column} requires a Bool array"
                )));
            };
            writer
                .push_bool_row(column, array.shape(), &casa_order(array))
                .map_err(storage)?;
        }
        for (column, value) in [("WEIGHT", row.weight), ("SIGMA", row.sigma)] {
            let ArrayValue::Float32(array) = value else {
                return Err(request_error(format!(
                    "MAIN.{column} requires a Float32 array"
                )));
            };
            writer
                .push_f32_row(column, array.shape(), &casa_order(array))
                .map_err(storage)?;
        }
        writer
            .push_f64_row("UVW", &[3], &row.uvw)
            .map_err(storage)?;
        for (column, value) in [
            ("ANTENNA1", ScalarValue::Int32(row.antennas[0])),
            ("ANTENNA2", ScalarValue::Int32(row.antennas[1])),
            ("ARRAY_ID", ScalarValue::Int32(row.array)),
            ("DATA_DESC_ID", ScalarValue::Int32(row.data_description)),
            ("EXPOSURE", ScalarValue::Float64(row.integration)),
            ("FEED1", ScalarValue::Int32(0)),
            ("FEED2", ScalarValue::Int32(0)),
            ("FIELD_ID", ScalarValue::Int32(row.field)),
            ("FLAG_ROW", ScalarValue::Bool(row.flagged)),
            ("INTERVAL", ScalarValue::Float64(row.integration)),
            ("OBSERVATION_ID", ScalarValue::Int32(row.observation)),
            ("PROCESSOR_ID", ScalarValue::Int32(-1)),
            ("SCAN_NUMBER", ScalarValue::Int32(row.scan)),
            ("STATE_ID", ScalarValue::Int32(-1)),
            ("TIME", ScalarValue::Float64(row.time)),
            ("TIME_CENTROID", ScalarValue::Float64(row.time)),
        ] {
            writer
                .push_scalar_row(column, Some(value))
                .map_err(storage)?;
        }
        Ok(())
    }

    /// Install the streamed columns with the final metadata and return
    /// writer telemetry, before the caller publishes its staging product.
    ///
    /// # Errors
    /// If the plan's columns are incomplete or a write fails.
    pub fn finish(
        self,
        ms: &mut MeasurementSet,
    ) -> Result<MeasurementSetWriteTelemetry, ObservationError> {
        self.session
            .save_and_finish(ms, &self.overrides)
            .map_err(storage)
    }
}

fn casa_order<T: Copy>(array: &ArrayD<T>) -> Cow<'_, [T]> {
    let casa_order = array.view().reversed_axes();
    match casa_order.to_slice() {
        Some(values) => Cow::Borrowed(values),
        None => Cow::Owned(casa_order.iter().copied().collect()),
    }
}

/// A producer's histogram of rows by (correlation count, channel count).
/// Its array-column plans share the standard casacore visibility tile
/// layout and keep the category count supplied by the producer.
pub fn msv2_array_columns(
    shapes: &BTreeMap<(usize, usize), usize>,
    data_columns: &[&str],
    telescope: &str,
    categories: usize,
) -> Result<Vec<MeasurementSetArrayColumnPlan>, ObservationError> {
    let mut by_correlations = BTreeMap::<usize, usize>::new();
    let mut total = 0usize;
    for (&(correlations, _), &rows) in shapes {
        total = total
            .checked_add(rows)
            .ok_or_else(|| request_error("MAIN row count overflow"))?;
        *by_correlations.entry(correlations).or_default() += rows;
    }
    if total == 0 {
        return Err(request_error(
            "the source holds nothing to write as an MSv2 row",
        ));
    }
    let visibility: Vec<_> = shapes
        .iter()
        .map(|(&(corr, chan), &rows)| {
            MeasurementSetArrayShapePlan::visibility(corr, chan, rows, telescope)
        })
        .collect();
    let flag_category = shapes
        .iter()
        .map(|(&(corr, chan), &rows)| {
            MeasurementSetArrayShapePlan::flag_category(corr, chan, categories, rows, telescope)
        })
        .collect();
    let weight: Vec<_> = by_correlations
        .into_iter()
        .map(|(corr, rows)| MeasurementSetArrayShapePlan::weight(corr, rows, telescope))
        .collect();
    let uvw = MeasurementSetArrayShapePlan {
        cell_shape: vec![3],
        row_count: total,
        tile_shape: crate::ms::casa_uvw_tile_shape(&visibility[0].tile_shape),
    };
    let shaped = MeasurementSetColumnStorage::TiledShape;
    let column = |name: &str, value_type, shapes, storage_manager| MeasurementSetArrayColumnPlan {
        name: name.to_string(),
        value_type,
        shapes,
        storage_manager,
    };
    use StreamedTiledShapeValueType::{Bool, Complex32, Float32, Float64};
    let mut columns: Vec<_> = data_columns
        .iter()
        .map(|name| {
            let element = if name.ends_with("_SPECTRUM") {
                Float32
            } else {
                Complex32
            };
            column(name, element, visibility.clone(), shaped)
        })
        .collect();
    columns.extend([
        column("FLAG", Bool, visibility, shaped),
        column("FLAG_CATEGORY", Bool, flag_category, shaped),
        column(
            "UVW",
            Float64,
            vec![uvw],
            MeasurementSetColumnStorage::TiledColumn,
        ),
        column("WEIGHT", Float32, weight.clone(), shaped),
        column("SIGMA", Float32, weight, shaped),
    ]);
    Ok(columns)
}

/// Admit and plan the standard variable-shaped MAIN columns for `rows`.
///
/// # Errors
/// If the shape counts disagree with `rows` or the writers need more
/// memory than the resources allow.
pub fn msv2_main_plan(
    rows: usize,
    arrays: Vec<MeasurementSetArrayColumnPlan>,
    resources: MeasurementSetWriteResources,
) -> Result<MeasurementSetWritePlan, ObservationError> {
    MeasurementSetWritePlan::variable_array_creation(
        rows,
        arrays,
        standard_main_scalar_column_plans(),
        resources,
    )
    .map_err(storage)
}
