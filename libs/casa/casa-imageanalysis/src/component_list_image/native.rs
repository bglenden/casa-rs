// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard};

use casa_components::ComponentList;
use casa_coordinates::CoordinateSystem;
use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
use casa_lattices::Lattice;
use casa_tables::{Table, TableInfo, TableOptions, TableWriteLock};

use casa_images::subtable_reference;
use casa_images::{ImageError, ImageInfo, PagedImage};

use super::masks::{ComponentMasks, validate_coverage};
use super::{ComponentListImage, invalid};

#[derive(Clone)]
pub(crate) struct ComponentTable {
    pub(super) path: PathBuf,
    pub(super) info: ImageInfo,
    pub(super) misc: RecordValue,
    pub(super) masks: Arc<RwLock<ComponentMasks>>,
}

impl ComponentListImage {
    /// Recognize the native Component List table of a component-list image.
    /// A bare component list lacks the image's coordinate keyword.
    pub fn recognizes_native(path: &Path) -> bool {
        std::fs::read_to_string(path.join("table.info"))
            .is_ok_and(|info| TableInfo::parse(&info).table_type == "Component List")
            && Table::open_metadata_only(TableOptions::new(path))
                .is_ok_and(|table| table.keywords().get("coords").is_some())
    }

    /// Attach the component model and metadata stored in a native CASA table.
    /// The returned coordinate record retains the file's axis mappings.
    /// # Errors
    /// Returns an error for unreadable or invalid component or image metadata.
    pub fn open_native(path: &Path, cache: bool) -> Result<(Self, RecordValue), ImageError> {
        let table = Table::open_metadata_only(TableOptions::new(path))?;
        let keywords = table.keywords();
        let record = match keywords.get("coords") {
            Some(Value::Record(record)) => record.clone(),
            _ => return Err(invalid("coords is not present in the component-list image")),
        };
        let coordinates = CoordinateSystem::from_record(&record)?;
        let shape = match keywords.get("shape") {
            Some(Value::Array(ArrayValue::Int32(shape))) if shape.ndim() == 1 => shape
                .iter()
                .map(|&length| usize::try_from(length))
                .collect::<Result<Vec<_>, _>>(),
            Some(Value::Array(ArrayValue::Int64(shape))) if shape.ndim() == 1 => shape
                .iter()
                .map(|&length| usize::try_from(length))
                .collect::<Result<Vec<_>, _>>(),
            _ => {
                return Err(invalid(
                    "shape is not an integer vector in the component-list image",
                ));
            }
        }
        .map_err(|_| invalid("All shape elements must be positive"))?;
        let info = match keywords.get("imageinfo") {
            Some(Value::Record(record)) => ImageInfo::from_record(record)?,
            _ => ImageInfo::default(),
        };
        if !info.beam_set.is_empty() {
            return Err(invalid("A ComponentListImage may not have a beam(s)"));
        }
        let native = ComponentTable {
            path: path.to_path_buf(),
            info,
            misc: keyword_record(keywords, "miscinfo"),
            masks: Arc::new(RwLock::new(ComponentMasks::from_keywords(keywords))),
        };
        let active = native.mask_state().active().map(str::to_owned);
        if let Some(name) = active.as_deref() {
            let mask = native.open_mask(name)?;
            validate_coverage(name, mask.shape(), &shape)?;
        }
        let components = ComponentList::read_table(path)
            .map_err(|error| ImageError::CasaSelection(error.to_string()))?;
        let mut image = Self::new(shape, components, coordinates, cache)?;
        image.native = Some(native);
        Ok((image, record))
    }

    /// Write a new Component List image table at the caller's staging path.
    /// Raw spectral records keep the input's reference offsets alongside the
    /// materialized frequencies already present in the sampled component model.
    /// # Errors
    /// Returns an error if the component values, image geometry, or native
    /// table metadata cannot be stored.
    pub fn write_native(
        &self,
        path: &Path,
        coordinate_record: &RecordValue,
        spectral_records: &BTreeMap<usize, RecordValue>,
    ) -> Result<(), ImageError> {
        let shape = self
            .shape
            .iter()
            .map(|&length| i32::try_from(length))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| invalid("Component image shape exceeds the native integer range"))?;
        self.components
            .write_table_with_spectral_records(path, spectral_records)
            .map_err(|error| ImageError::CasaSelection(error.to_string()))?;
        PagedImage::<f32>::reset_logtable(&path.join("logtable"))?;
        update_keywords(path, |keywords| {
            keywords.upsert(
                "units",
                Value::Scalar(ScalarValue::String("Jy/pixel".into())),
            );
            keywords.upsert("shape", Value::Array(ArrayValue::from_i32_vec(shape)));
            keywords.upsert("coords", Value::Record(coordinate_record.clone()));
            keywords.upsert("logtable", Value::TableRef(subtable_reference("logtable")));
        })
    }
}

impl ComponentTable {
    pub(super) fn mask_state(&self) -> RwLockReadGuard<'_, ComponentMasks> {
        self.masks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(super) fn keyword_record(keywords: &RecordValue, name: &str) -> RecordValue {
    match keywords.get(name) {
        Some(Value::Record(record)) => record.clone(),
        _ => RecordValue::default(),
    }
}

pub(crate) fn write_component_keyword(
    path: &Path,
    name: &str,
    value: Value,
) -> Result<(), ImageError> {
    update_keywords(path, |keywords| {
        keywords.upsert(name, value);
    })
}

fn update_keywords(path: &Path, update: impl FnOnce(&mut RecordValue)) -> Result<(), ImageError> {
    let mut lock = TableWriteLock::acquire_to_reread(path, 1)?;
    let mut table = Table::open_metadata_only(TableOptions::new(path))?;
    update(table.keywords_mut());
    table.save_keywords_atomically(path)?;
    lock.record_write();
    lock.release()?;
    Ok(())
}
