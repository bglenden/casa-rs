// SPDX-License-Identifier: LGPL-3.0-or-later

use crate::subtables::SubTable;
use crate::{MeasurementSet, MsError, MsReadPlan, MsResult, MsSelectionIoBudget};
use casa_imaging_model::{
    AntennaSelection, DataDescriptionSelection, IdSelection, IntentSelection, ObservationSelection,
    ResolvedIntent, RowSelection, SelectionBound, TimeSelection, UvDistanceRange, UvDistanceUnit,
    UvSelection,
};

use super::row_selection::{CompiledRowPredicate, RowSelectionEvaluationError, StoredMainRow};
use crate::selection::{UvBound, UvBoundOp, UvSelectionRange, UvUnit};

const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;

/// Canonical resolved row predicates used by native selected-observation access.
///
/// This is an opaque projection of application selectors into the same model
/// contract and row evaluator used by [`super::BoundSelectedObservation`].
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedObservationRowSelection {
    rows: RowSelection,
    data_descriptions: Vec<DataDescriptionSelection>,
}

impl SelectedObservationRowSelection {
    pub(crate) fn from_compiled(selection: &ObservationSelection) -> Self {
        Self {
            rows: selection.rows_filter().clone(),
            data_descriptions: selection.data_descriptions().to_vec(),
        }
    }

    /// Return the canonical row predicate resolved from the application selectors.
    #[must_use]
    pub fn rows(&self) -> &RowSelection {
        &self.rows
    }

    /// Return the selected data-description bindings resolved by the storage owner.
    #[must_use]
    pub fn data_descriptions(&self) -> &[DataDescriptionSelection] {
        &self.data_descriptions
    }
}

/// One selected MAIN row reported by the canonical bounded row traversal.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedObservationRow {
    physical_row: usize,
    data_description_id: i32,
    field_id: i32,
    antenna1: i32,
    antenna2: i32,
    observation_id: i32,
    time_mjd_seconds: f64,
    time_centroid_mjd_seconds: f64,
    flag_row: bool,
    uvw_m: [f64; 3],
}

impl SelectedObservationRow {
    /// Exact stored bytes read per MAIN row while evaluating the canonical predicate.
    pub const STORAGE_BYTES_PER_ROW: usize = 73;

    /// Return the physical MAIN row index.
    #[must_use]
    pub const fn physical_row(self) -> usize {
        self.physical_row
    }

    /// Return the stored `DATA_DESC_ID`.
    #[must_use]
    pub const fn data_description_id(self) -> i32 {
        self.data_description_id
    }

    /// Return the stored `FIELD_ID`.
    #[must_use]
    pub const fn field_id(self) -> i32 {
        self.field_id
    }

    /// Return the stored first antenna identifier.
    #[must_use]
    pub const fn antenna1(self) -> i32 {
        self.antenna1
    }

    /// Return the stored second antenna identifier.
    #[must_use]
    pub const fn antenna2(self) -> i32 {
        self.antenna2
    }

    /// Return the stored `OBSERVATION_ID`.
    #[must_use]
    pub const fn observation_id(self) -> i32 {
        self.observation_id
    }

    /// Return the stored `TIME` in MJD seconds.
    #[must_use]
    pub const fn time_mjd_seconds(self) -> f64 {
        self.time_mjd_seconds
    }

    /// Return the stored `TIME_CENTROID` in MJD seconds.
    #[must_use]
    pub const fn time_centroid_mjd_seconds(self) -> f64 {
        self.time_centroid_mjd_seconds
    }

    /// Return the stored row-level flag.
    #[must_use]
    pub const fn flag_row(self) -> bool {
        self.flag_row
    }

    /// Return the stored UVW coordinates in metres.
    #[must_use]
    pub const fn uvw_m(self) -> [f64; 3] {
        self.uvw_m
    }
}

impl MeasurementSet {
    /// Resolve the imaging frontend's supported selectors into the native row contract.
    ///
    /// Text parsing ends here. The returned value contains only exact DDID,
    /// field, UV-distance, and STATE/intent predicates.
    pub fn selected_observation_row_selection(
        &self,
        data_description_ids: &[i32],
        field_ids: Option<&[i32]>,
        uvrange: Option<&str>,
        intent: Option<&str>,
    ) -> MsResult<SelectedObservationRowSelection> {
        let data_description = self.data_description()?;
        let mut selected_descriptions = data_description_ids
            .iter()
            .copied()
            .map(|data_description_id| {
                let row = usize::try_from(data_description_id).map_err(|_| {
                    MsError::InvalidInput(format!(
                        "selected DATA_DESC_ID {data_description_id} is negative"
                    ))
                })?;
                let spectral_window_id = data_description.spectral_window_id(row)?;
                let polarization_id = data_description.polarization_id(row)?;
                Ok(DataDescriptionSelection::new(
                    u32::try_from(data_description_id).expect("nonnegative i32 fits u32"),
                    u32::try_from(spectral_window_id).map_err(|_| {
                        MsError::InvalidInput(format!(
                            "selected SPECTRAL_WINDOW_ID {spectral_window_id} is negative"
                        ))
                    })?,
                    u32::try_from(polarization_id).map_err(|_| {
                        MsError::InvalidInput(format!(
                            "selected POLARIZATION_ID {polarization_id} is negative"
                        ))
                    })?,
                ))
            })
            .collect::<MsResult<Vec<_>>>()?;
        selected_descriptions.sort_unstable();
        selected_descriptions.dedup();

        let fields = match field_ids {
            Some(ids) => IdSelection::Only(canonical_nonnegative_ids(ids, "FIELD_ID")?),
            None => IdSelection::All,
        };
        let uv_distances = match uvrange {
            Some(selector) => UvSelection::Ranges(
                crate::selection::parser::parse_uvrange_selector(selector)?
                    .into_iter()
                    .map(model_uv_range)
                    .collect(),
            ),
            None => UvSelection::All,
        };
        let intents = match intent {
            Some(selector) => {
                let state = self.state()?;
                let mut resolved = crate::selection::parser::parse_state_selector(self, selector)?
                    .into_iter()
                    .map(|state_id| {
                        let row = usize::try_from(state_id).map_err(|_| {
                            MsError::InvalidInput(format!(
                                "selected STATE_ID {state_id} is negative"
                            ))
                        })?;
                        Ok(ResolvedIntent::new(
                            u32::try_from(state_id).expect("nonnegative i32 fits u32"),
                            state.string(row, "OBS_MODE")?,
                        ))
                    })
                    .collect::<MsResult<Vec<_>>>()?;
                resolved.sort_unstable_by_key(ResolvedIntent::state_id);
                IntentSelection::Only(resolved)
            }
            None => IntentSelection::All,
        };
        Ok(SelectedObservationRowSelection {
            rows: RowSelection::new(
                fields,
                TimeSelection::All,
                uv_distances,
                AntennaSelection::All,
                IdSelection::All,
                IdSelection::All,
                intents,
                IdSelection::All,
            ),
            data_descriptions: selected_descriptions,
        })
    }

    /// Visit selected MAIN rows in canonical physical order with bounded residency.
    ///
    /// Predicate evaluation and row reporting share one terminally fallible
    /// traversal. The storage owner never materializes a second selected-row list.
    pub fn visit_selected_observation_rows(
        &self,
        selection: &SelectedObservationRowSelection,
        io: MsSelectionIoBudget,
        mut visit: impl FnMut(SelectedObservationRow),
    ) -> MsResult<()> {
        if io.requested_bytes_per_row < SelectedObservationRow::STORAGE_BYTES_PER_ROW {
            return Err(MsError::InvalidInput(format!(
                "selected-observation row traversal requires at least {} bytes per row",
                SelectedObservationRow::STORAGE_BYTES_PER_ROW
            )));
        }
        let wavelengths = selected_wavelengths(self, &selection.data_descriptions)?;
        let predicate = CompiledRowPredicate::new(
            &selection.rows,
            &selection.data_descriptions,
            |data_description_id| {
                wavelengths
                    .iter()
                    .find(|(candidate, _)| *candidate == data_description_id)
                    .map(|(_, wavelength_m)| *wavelength_m)
            },
        )
        .map_err(|error| match error {
            RowSelectionEvaluationError::MissingReferenceWavelength {
                data_description_id,
            } => MsError::InvalidInput(format!(
                "selected DATA_DESC_ID {data_description_id} has no positive finite reference wavelength"
            )),
        })?;
        let plan = MsReadPlan::new(self.row_count(), io)
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        self.visit_main_row_selection_blocks(plan, |block| {
            for offset in 0..block.len() {
                let fact = block
                    .row(offset)
                    .expect("offset is bounded by MAIN selection block length");
                if predicate.matches(StoredMainRow::from(fact)) {
                    visit(SelectedObservationRow {
                        physical_row: fact.physical_row(),
                        data_description_id: fact.data_description_id(),
                        field_id: fact.field_id(),
                        antenna1: fact.antenna1(),
                        antenna2: fact.antenna2(),
                        observation_id: fact.observation_id(),
                        time_mjd_seconds: fact.time_mjd_seconds(),
                        time_centroid_mjd_seconds: fact.time_centroid_mjd_seconds(),
                        flag_row: fact.flag_row(),
                        uvw_m: fact.uvw_m(),
                    });
                }
            }
        })
    }
}

fn canonical_nonnegative_ids(ids: &[i32], label: &str) -> MsResult<Vec<u32>> {
    let mut ids = ids
        .iter()
        .copied()
        .map(|value| {
            u32::try_from(value)
                .map_err(|_| MsError::InvalidInput(format!("selected {label} {value} is negative")))
        })
        .collect::<MsResult<Vec<_>>>()?;
    ids.sort_unstable();
    ids.dedup();
    if ids.is_empty() {
        return Err(MsError::InvalidInput(format!(
            "selected {label} set is empty"
        )));
    }
    Ok(ids)
}

fn model_uv_range(range: UvSelectionRange) -> UvDistanceRange {
    let (scale, unit) = match range.unit {
        UvUnit::Meters(scale) => (scale, UvDistanceUnit::Meters),
        UvUnit::Lambda(scale) => (scale, UvDistanceUnit::Wavelengths),
    };
    UvDistanceRange::new(
        range.lower.map(|bound| model_bound(bound, scale)),
        range.upper.map(|bound| model_bound(bound, scale)),
        unit,
    )
}

fn model_bound(bound: UvBound, scale: f64) -> SelectionBound {
    let value = bound.value * scale;
    match bound.op {
        UvBoundOp::Greater | UvBoundOp::Less => SelectionBound::exclusive(value),
        UvBoundOp::GreaterEqual | UvBoundOp::LessEqual => SelectionBound::inclusive(value),
    }
}

fn selected_wavelengths(
    measurement_set: &MeasurementSet,
    data_descriptions: &[DataDescriptionSelection],
) -> MsResult<Vec<(u32, f64)>> {
    let spectral_windows = measurement_set.spectral_window()?;
    let rows = data_descriptions
        .iter()
        .map(|description| {
            usize::try_from(description.spectral_window_id()).map_err(|_| {
                MsError::InvalidInput("SPECTRAL_WINDOW_ID exceeds host index domain".to_string())
            })
        })
        .collect::<MsResult<Vec<_>>>()?;
    let reference_frequencies = spectral_windows
        .table()
        .column_accessor("REF_FREQUENCY")?
        .scalar_cells_owned_for_rows(&rows)?;
    data_descriptions
        .iter()
        .zip(rows)
        .zip(reference_frequencies)
        .map(|((description, row), frequency)| {
            let reference_frequency_hz = match frequency {
                Some(casa_types::ScalarValue::Float64(value)) => value,
                Some(other) => {
                    return Err(MsError::ColumnTypeMismatch {
                        column: "REF_FREQUENCY".to_string(),
                        table: "SPECTRAL_WINDOW".to_string(),
                        expected: "Float64".to_string(),
                        found: format!("{:?}", other.primitive_type()),
                    });
                }
                None => {
                    return Err(MsError::MissingColumn {
                        column: format!("REF_FREQUENCY[row={row}]"),
                        table: "SPECTRAL_WINDOW".to_string(),
                    });
                }
            };
            Ok((
                description.data_description_id(),
                SPEED_OF_LIGHT_M_PER_S / reference_frequency_hz,
            ))
        })
        .collect()
}
