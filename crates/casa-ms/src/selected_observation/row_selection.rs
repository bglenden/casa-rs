// SPDX-License-Identifier: LGPL-3.0-or-later

use std::mem::size_of;

use crate::SelectedObservationRow;
use casa_imaging_model::{
    DataDescriptionSelection, IdSelection, IntentSelection, ObservationSource, RowSelection,
    SelectionBound, UvDistanceUnit, UvSelection,
};
use thiserror::Error;

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

    pub(crate) fn matches(&self, row: SelectedObservationRow) -> bool {
        let selection = self.catalog.selection();
        let data_descriptions = self.catalog.data_descriptions();
        let Ok(data_description_id) = u32::try_from(row.data_description_id) else {
            return false;
        };
        data_descriptions
            .iter()
            .any(|description| description.data_description_id() == data_description_id)
            && id_matches(selection.fields(), row.field_id)
            && uv_matches(
                selection.uv_distances(),
                row.uvw_m,
                data_description_id,
                &self.wavelengths_by_ddid,
            )
            && intent_matches(selection.intents(), row.state_id)
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
        DataDescriptionSelection, IdSelection, IntentSelection, ResolvedIntent, RowSelection,
        SelectionBound, UvDistanceRange, UvDistanceUnit, UvSelection,
    };

    use super::{CompiledRowPredicate, RowSelectionEvaluationError};
    use crate::SelectedObservationRow;

    fn exact_selection(uv_distances: UvSelection) -> RowSelection {
        RowSelection::new(
            IdSelection::Only(vec![3]),
            uv_distances,
            IntentSelection::Only(vec![ResolvedIntent::new(5, "CALIBRATE_PHASE".to_string())]),
        )
    }

    fn matching_row() -> SelectedObservationRow {
        SelectedObservationRow {
            physical_row: 0,
            data_description_id: 6,
            field_id: 3,
            antenna1: 0,
            antenna2: 1,
            time_mjd_seconds: 0.0,
            time_centroid_mjd_seconds: 0.0,
            state_id: 5,
            observation_id: 0,
            flag_row: false,
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
            SelectedObservationRow {
                data_description_id: 4,
                ..row
            },
            SelectedObservationRow { field_id: 4, ..row },
            SelectedObservationRow { state_id: 6, ..row },
            SelectedObservationRow {
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
        let row = SelectedObservationRow {
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
    fn unconstrained_uv_does_not_filter_unreadable_values() {
        let descriptions = [DataDescriptionSelection::new(6, 8, 10)];
        let selection = RowSelection::new(IdSelection::All, UvSelection::All, IntentSelection::All);
        let predicate = CompiledRowPredicate::new(&selection, &descriptions, |_| None).unwrap();
        assert!(predicate.matches(SelectedObservationRow {
            uvw_m: [f64::NAN; 3],
            ..matching_row()
        }));
    }
}
