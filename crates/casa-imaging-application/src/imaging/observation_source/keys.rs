// SPDX-License-Identifier: LGPL-3.0-or-later
//! How the rows of several stores name their dishes, spectral windows and
//! fields to the kernel sets and densities of one run.
//!
//! A row's spectral window and field keys are its own `SPECTRAL_WINDOW` and
//! `FIELD` rows, offset by the rows of the stores before it, as if the
//! MeasurementSets were concatenated in the snapshot's order. CASA keys the
//! same pairs: a mosaic's beams by `msId_spw`
//! (`SimplePBConvFunc::convIndex`), a `mosweight` density by `msId_field`
//! (`VisImagingWeight::multiFieldMap_p`). The first store's keys are its own
//! numbers, so a run over one store keys its rows as before.

use casa_imaging_model::AntennaResponseClass;
use casa_observation::Catalogue;

/// Where one store's window and field keys start.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct KeyBase {
    window: u32,
    field: u32,
}

/// The keys of a run's stores: the aperture classes of them all, and where
/// each store's window and field keys start.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct StoreKeys {
    /// Every store's aperture classes in dish order: a row's antenna types
    /// index them. CASA's `HetArrayConvFunc::findAntennaSizes` adds the
    /// dishes of each MeasurementSet it meets to those of the ones before.
    pub(crate) dish_classes: Vec<AntennaResponseClass>,
    bases: Vec<KeyBase>,
}

impl StoreKeys {
    /// The keys of stores with `catalogues`, in the snapshot's order, whose
    /// aperture classes are `dish_classes`.
    pub(crate) fn new<'c>(
        catalogues: impl IntoIterator<Item = &'c Catalogue>,
        dish_classes: Vec<AntennaResponseClass>,
    ) -> Self {
        Self::counted(
            catalogues
                .into_iter()
                .map(|catalogue| (catalogue.windows.len(), catalogue.fields.len())),
            dish_classes,
        )
    }

    /// The keys of stores of so many `(windows, fields)`.
    fn counted(
        counts: impl IntoIterator<Item = (usize, usize)>,
        mut dish_classes: Vec<AntennaResponseClass>,
    ) -> Self {
        dish_classes.sort_unstable();
        dish_classes.dedup();
        let mut next = KeyBase::default();
        let bases = counts
            .into_iter()
            .map(|(windows, fields)| {
                let base = next;
                next.window += u32::try_from(windows).expect("windows fit u32");
                next.field += u32::try_from(fields).expect("fields fit u32");
                base
            })
            .collect();
        Self {
            dish_classes,
            bases,
        }
    }

    fn base(&self, store: usize) -> KeyBase {
        self.bases.get(store).copied().unwrap_or_default()
    }

    /// The key of store `store`'s spectral window `spectral_window`.
    pub(crate) fn window(&self, store: usize, spectral_window: u32) -> u32 {
        self.base(store).window + spectral_window
    }

    /// The key of store `store`'s field `field`.
    pub(crate) fn field(&self, store: usize, field: u32) -> u32 {
        self.base(store).field + field
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each store's keys follow the windows and fields of the stores
    /// before it; the first store's are its own numbers, and so are the
    /// keys of a store the run does not know.
    #[test]
    fn keys_run_over_every_store_in_order() {
        let keys = StoreKeys::counted(
            [(2, 3), (1, 5), (4, 1)],
            vec![
                AntennaResponseClass::CasaAca7m,
                AntennaResponseClass::CasaAlma12m,
                AntennaResponseClass::CasaAca7m,
            ],
        );
        assert_eq!(keys.window(0, 1), 1);
        assert_eq!(keys.field(0, 2), 2);
        assert_eq!(keys.window(1, 0), 2);
        assert_eq!(keys.field(1, 4), 7);
        assert_eq!(keys.window(2, 3), 6);
        assert_eq!(keys.field(2, 0), 8);
        assert_eq!(keys.window(5, 3), 3);
        let mut classes = vec![
            AntennaResponseClass::CasaAca7m,
            AntennaResponseClass::CasaAlma12m,
        ];
        classes.sort_unstable();
        assert_eq!(keys.dish_classes, classes);
    }
}
