// SPDX-License-Identifier: LGPL-3.0-or-later

use std::mem::size_of;

use crate::{MainRowSelectionFact, SelectedStoredSample};
use casa_imaging_model::{
    AntennaSelection, DataDescriptionSelection, IdSelection, IntentSelection, ObservationSource,
    RowSelection, SelectionBound, TimeSelection, UvDistanceUnit, UvSelection,
};
use thiserror::Error;

#[derive(Debug, Clone, Copy)]
pub(crate) struct StoredMainRow {
    pub(crate) data_description_id: i32,
    pub(crate) field_id: i32,
    pub(crate) antenna1: i32,
    pub(crate) antenna2: i32,
    pub(crate) time_mjd_seconds: f64,
    pub(crate) scan_number: i32,
    pub(crate) state_id: i32,
    pub(crate) observation_id: i32,
    pub(crate) array_id: i32,
    pub(crate) uvw_m: [f64; 3],
}

impl From<MainRowSelectionFact> for StoredMainRow {
    fn from(fact: MainRowSelectionFact) -> Self {
        Self {
            data_description_id: fact.data_description_id(),
            field_id: fact.field_id(),
            antenna1: fact.antenna1(),
            antenna2: fact.antenna2(),
            time_mjd_seconds: fact.time_mjd_seconds(),
            scan_number: fact.scan_number(),
            state_id: fact.state_id(),
            observation_id: fact.observation_id(),
            array_id: fact.array_id(),
            uvw_m: fact.uvw_m(),
        }
    }
}

impl From<SelectedStoredSample> for StoredMainRow {
    fn from(sample: SelectedStoredSample) -> Self {
        Self {
            data_description_id: sample.data_description_id(),
            field_id: sample.field_id(),
            antenna1: sample.antenna1(),
            antenna2: sample.antenna2(),
            time_mjd_seconds: sample.time_mjd_seconds(),
            scan_number: sample.scan_number(),
            state_id: sample.state_id(),
            observation_id: sample.observation_id(),
            array_id: sample.array_id(),
            uvw_m: sample.uvw_m(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum RowSelectionEvaluationError {
    #[error(
        "selected DATA_DESC_ID {data_description_id} has no positive finite reference wavelength"
    )]
    MissingReferenceWavelength { data_description_id: u32 },
}

pub(crate) struct CompiledRowPredicate {
    catalog: PredicateCatalog,
    wavelengths_by_ddid: Box<[(u32, f64)]>,
}

enum PredicateCatalog {
    SharedSource(ObservationSource),
    OwnedProjection {
        selection: RowSelection,
        data_descriptions: Box<[DataDescriptionSelection]>,
    },
}

impl PredicateCatalog {
    fn selection(&self) -> &RowSelection {
        match self {
            Self::SharedSource(source) => source.selection().rows_filter(),
            Self::OwnedProjection { selection, .. } => selection,
        }
    }

    fn data_descriptions(&self) -> &[DataDescriptionSelection] {
        match self {
            Self::SharedSource(source) => source.selection().data_descriptions(),
            Self::OwnedProjection {
                data_descriptions, ..
            } => data_descriptions,
        }
    }
}

impl CompiledRowPredicate {
    pub(crate) fn shared_retained_heap_bytes(source: &ObservationSource) -> Option<usize> {
        let wavelength_bytes = if needs_reference_wavelengths(source.selection().rows_filter()) {
            source
                .selection()
                .data_descriptions()
                .len()
                .checked_mul(size_of::<(u32, f64)>())?
        } else {
            0
        };
        source
            .provenance()
            .locator()
            .len()
            .checked_add(wavelength_bytes)
    }

    pub(crate) fn new_shared(
        source: &ObservationSource,
        reference_wavelength: impl FnMut(u32) -> Option<f64>,
    ) -> Result<Self, RowSelectionEvaluationError> {
        Self::from_catalog(
            PredicateCatalog::SharedSource(source.clone()),
            reference_wavelength,
        )
    }

    pub(crate) fn new(
        selection: &RowSelection,
        data_descriptions: &[DataDescriptionSelection],
        reference_wavelength: impl FnMut(u32) -> Option<f64>,
    ) -> Result<Self, RowSelectionEvaluationError> {
        Self::from_catalog(
            PredicateCatalog::OwnedProjection {
                selection: selection.clone(),
                data_descriptions: data_descriptions.into(),
            },
            reference_wavelength,
        )
    }

    fn from_catalog(
        catalog: PredicateCatalog,
        mut reference_wavelength: impl FnMut(u32) -> Option<f64>,
    ) -> Result<Self, RowSelectionEvaluationError> {
        let mut wavelengths_by_ddid = Vec::new();
        if needs_reference_wavelengths(catalog.selection()) {
            let data_descriptions = catalog.data_descriptions();
            wavelengths_by_ddid.reserve(data_descriptions.len());
            for description in data_descriptions {
                let data_description_id = description.data_description_id();
                let Some(wavelength_m) = reference_wavelength(data_description_id)
                    .filter(|value| value.is_finite() && *value > 0.0)
                else {
                    return Err(RowSelectionEvaluationError::MissingReferenceWavelength {
                        data_description_id,
                    });
                };
                wavelengths_by_ddid.push((data_description_id, wavelength_m));
            }
        }
        Ok(Self {
            catalog,
            wavelengths_by_ddid: wavelengths_by_ddid.into_boxed_slice(),
        })
    }

    pub(crate) fn matches(&self, row: StoredMainRow) -> bool {
        let selection = self.catalog.selection();
        let data_descriptions = self.catalog.data_descriptions();
        let Ok(data_description_id) = u32::try_from(row.data_description_id) else {
            return false;
        };
        data_descriptions
            .iter()
            .any(|description| description.data_description_id() == data_description_id)
            && id_matches(selection.fields(), row.field_id)
            && time_matches(selection.times(), row.time_mjd_seconds)
            && uv_matches(
                selection.uv_distances(),
                row.uvw_m,
                data_description_id,
                &self.wavelengths_by_ddid,
            )
            && antenna_matches(selection.antennas(), row.antenna1, row.antenna2)
            && id_matches(selection.scans(), row.scan_number)
            && id_matches(selection.observations(), row.observation_id)
            && intent_matches(selection.intents(), row.state_id)
            && id_matches(selection.arrays(), row.array_id)
    }

    pub(crate) fn requires_every_source_row(&self, data_description_count: usize) -> bool {
        let selection = self.catalog.selection();
        let data_descriptions = self.catalog.data_descriptions();
        matches!(selection.fields(), IdSelection::All)
            && matches!(selection.times(), TimeSelection::All)
            && matches!(selection.uv_distances(), UvSelection::All)
            && matches!(selection.antennas(), AntennaSelection::All)
            && matches!(selection.scans(), IdSelection::All)
            && matches!(selection.observations(), IdSelection::All)
            && matches!(selection.intents(), IntentSelection::All)
            && matches!(selection.arrays(), IdSelection::All)
            && data_descriptions.len() == data_description_count
            && data_descriptions
                .iter()
                .enumerate()
                .all(|(index, description)| {
                    usize::try_from(description.data_description_id()).ok() == Some(index)
                })
    }
}

fn needs_reference_wavelengths(selection: &RowSelection) -> bool {
    matches!(selection.uv_distances(), UvSelection::Ranges(ranges) if ranges.iter().any(|range| range.unit() == UvDistanceUnit::Wavelengths))
}

fn id_matches(selection: &IdSelection, value: i32) -> bool {
    match selection {
        IdSelection::All => true,
        IdSelection::Only(ids) => u32::try_from(value)
            .ok()
            .is_some_and(|value| ids.binary_search(&value).is_ok()),
    }
}

fn time_matches(selection: &TimeSelection, value: f64) -> bool {
    match selection {
        TimeSelection::All => true,
        TimeSelection::Ranges(ranges) => {
            value.is_finite()
                && ranges.iter().any(|range| {
                    lower_matches(range.lower(), value) && upper_matches(range.upper(), value)
                })
        }
    }
}

fn uv_matches(
    selection: &UvSelection,
    uvw_m: [f64; 3],
    data_description_id: u32,
    wavelengths_by_ddid: &[(u32, f64)],
) -> bool {
    match selection {
        UvSelection::All => true,
        UvSelection::Ranges(ranges) => {
            let distance_m = uvw_m[0].hypot(uvw_m[1]);
            distance_m.is_finite()
                && ranges.iter().any(|range| {
                    let value = match range.unit() {
                        UvDistanceUnit::Meters => distance_m,
                        UvDistanceUnit::Wavelengths => {
                            let Some((_, wavelength_m)) = wavelengths_by_ddid
                                .iter()
                                .find(|(ddid, _)| *ddid == data_description_id)
                            else {
                                return false;
                            };
                            distance_m / wavelength_m
                        }
                    };
                    lower_matches(range.lower(), value) && upper_matches(range.upper(), value)
                })
        }
    }
}

fn lower_matches(bound: Option<SelectionBound>, value: f64) -> bool {
    bound.is_none_or(|bound| {
        if bound.is_inclusive() {
            value >= bound.value()
        } else {
            value > bound.value()
        }
    })
}

fn upper_matches(bound: Option<SelectionBound>, value: f64) -> bool {
    bound.is_none_or(|bound| {
        if bound.is_inclusive() {
            value <= bound.value()
        } else {
            value < bound.value()
        }
    })
}

fn antenna_matches(selection: &AntennaSelection, antenna1: i32, antenna2: i32) -> bool {
    match selection {
        AntennaSelection::All => true,
        AntennaSelection::Only(baselines) => {
            let (Ok(antenna1), Ok(antenna2)) = (u32::try_from(antenna1), u32::try_from(antenna2))
            else {
                return false;
            };
            let pair = if antenna1 <= antenna2 {
                [antenna1, antenna2]
            } else {
                [antenna2, antenna1]
            };
            baselines
                .binary_search_by_key(&pair, |baseline| baseline.antennas())
                .is_ok()
        }
    }
}

fn intent_matches(selection: &IntentSelection, state_id: i32) -> bool {
    match selection {
        IntentSelection::All => true,
        IntentSelection::Only(intents) => u32::try_from(state_id).ok().is_some_and(|state_id| {
            intents
                .binary_search_by_key(&state_id, |intent| intent.state_id())
                .is_ok()
        }),
    }
}

#[cfg(test)]
mod tests {
    use casa_imaging_model::{
        AntennaBaseline, AntennaSelection, DataDescriptionSelection, IdSelection, IntentSelection,
        ResolvedIntent, RowSelection, SelectionBound, TimeRange, TimeSelection, UvDistanceRange,
        UvDistanceUnit, UvSelection,
    };

    use super::{CompiledRowPredicate, RowSelectionEvaluationError, StoredMainRow};

    fn exact_selection(uv_distances: UvSelection) -> RowSelection {
        RowSelection::new(
            IdSelection::Only(vec![3]),
            TimeSelection::Ranges(vec![TimeRange::new(
                Some(SelectionBound::inclusive(10.0)),
                Some(SelectionBound::exclusive(20.0)),
            )]),
            uv_distances,
            AntennaSelection::Only(vec![AntennaBaseline::new(1, 4)]),
            IdSelection::Only(vec![7]),
            IdSelection::Only(vec![9]),
            IntentSelection::Only(vec![ResolvedIntent::new(5, "CALIBRATE_PHASE".to_string())]),
            IdSelection::Only(vec![2]),
        )
    }

    fn matching_row() -> StoredMainRow {
        StoredMainRow {
            data_description_id: 6,
            field_id: 3,
            antenna1: 4,
            antenna2: 1,
            time_mjd_seconds: 15.0,
            scan_number: 7,
            state_id: 5,
            observation_id: 9,
            array_id: 2,
            uvw_m: [3.0, 4.0, 12.0],
        }
    }

    #[test]
    fn compiled_row_predicate_evaluates_every_resolved_selector() {
        let descriptions = [DataDescriptionSelection::new(6, 8, 10)];
        let selection = exact_selection(UvSelection::Ranges(vec![UvDistanceRange::new(
            Some(SelectionBound::inclusive(5.0)),
            Some(SelectionBound::inclusive(5.0)),
            UvDistanceUnit::Meters,
        )]));
        let predicate = CompiledRowPredicate::new(&selection, &descriptions, |_| None)
            .expect("metre UV selection needs no wavelength metadata");
        let row = matching_row();
        assert!(predicate.matches(row));

        for rejected in [
            StoredMainRow {
                data_description_id: 4,
                ..row
            },
            StoredMainRow { field_id: 4, ..row },
            StoredMainRow { antenna2: 2, ..row },
            StoredMainRow {
                time_mjd_seconds: 20.0,
                ..row
            },
            StoredMainRow {
                scan_number: 8,
                ..row
            },
            StoredMainRow { state_id: 6, ..row },
            StoredMainRow {
                observation_id: 8,
                ..row
            },
            StoredMainRow { array_id: 1, ..row },
            StoredMainRow {
                uvw_m: [6.0, 0.0, 0.0],
                ..row
            },
        ] {
            assert!(!predicate.matches(rejected), "accepted {rejected:?}");
        }
    }

    #[test]
    fn compiled_row_predicate_resolves_wavelength_uv_ranges_by_ddid() {
        let descriptions = [DataDescriptionSelection::new(6, 8, 10)];
        let selection = exact_selection(UvSelection::Ranges(vec![UvDistanceRange::new(
            Some(SelectionBound::inclusive(4.9)),
            Some(SelectionBound::inclusive(5.1)),
            UvDistanceUnit::Wavelengths,
        )]));
        let predicate =
            CompiledRowPredicate::new(&selection, &descriptions, |ddid| (ddid == 6).then_some(1.0))
                .expect("selected DDID has a positive finite reference wavelength");
        let row = StoredMainRow {
            uvw_m: [3.0, 4.0, 0.0],
            ..matching_row()
        };
        assert!(predicate.matches(row));

        assert!(matches!(
            CompiledRowPredicate::new(&selection, &descriptions, |_| None),
            Err(RowSelectionEvaluationError::MissingReferenceWavelength {
                data_description_id: 6
            })
        ));
    }

    #[test]
    fn unconstrained_time_and_uv_do_not_filter_unreadable_values() {
        let descriptions = [DataDescriptionSelection::new(6, 8, 10)];
        let selection = RowSelection::new(
            IdSelection::All,
            TimeSelection::All,
            UvSelection::All,
            AntennaSelection::All,
            IdSelection::All,
            IdSelection::All,
            IntentSelection::All,
            IdSelection::All,
        );
        let predicate = CompiledRowPredicate::new(&selection, &descriptions, |_| None).unwrap();
        assert!(predicate.matches(StoredMainRow {
            time_mjd_seconds: f64::NAN,
            uvw_m: [f64::NAN; 3],
            ..matching_row()
        }));
    }
}
