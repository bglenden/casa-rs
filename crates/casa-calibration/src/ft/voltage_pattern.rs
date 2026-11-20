// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare the legacy imager's radial voltage pattern on the model image.

use std::path::{Path, PathBuf};

use casa_coordinates::DirectionCoordinate;
use casa_images::PagedImage;
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::{MeasurementSet, MsResult, ResolvedMsSelection, SubtableId, voltage_pattern};
use casa_numerics::{BeamPattern, PrimaryBeam};
use casa_tables::{Table, TableOptions};
use casa_types::measures::{MeasFrame, direction::MDirection};
use casa_types::{RecordValue, ScalarValue, Value};
use ndarray::Array2;

use super::invalid;

const MANAGED_VP_TABLE: &str = "CASARS_VOLTAGE_PATTERNS";

pub(super) fn legacy_mosaic_table(source: &Path, grid: &Path) -> MsResult<PathBuf> {
    let table = voltage_pattern::VoltagePatternTable::open(source).map_err(invalid)?;
    let definitions = table.definitions().map_err(invalid)?;
    let mut replacements = Vec::new();
    let destination = grid.join(MANAGED_VP_TABLE);
    for (index, mut record) in definitions.into_iter().enumerate() {
        let (Some(real), Some(imaginary)) =
            (text(&record, "realimage"), text(&record, "imagimage"))
        else {
            continue;
        };
        if text(&record, "name") != Some("IMAGE") || record.get("imagim").is_some() {
            continue;
        }
        let imaginary = if imaginary.is_empty() {
            let real = PagedImage::<f32>::open(real).map_err(invalid)?;
            let path = grid.join(format!("zero-vp-imaginary-{index}.im"));
            let mut zero =
                PagedImage::<f32>::create(real.shape().to_vec(), real.coordinates().clone(), &path)
                    .map_err(invalid)?;
            zero.save().map_err(invalid)?;
            path.to_string_lossy().into_owned()
        } else {
            imaginary.to_owned()
        };
        record.upsert("imagim", Value::Scalar(ScalarValue::String(imaginary)));
        replacements.push((index, record));
    }
    if replacements.is_empty() {
        return Ok(source.to_owned());
    }
    // The pinned reader needs the misspelled field. Keep the caller's
    // table intact and own the compatible copy with the Fourier image.
    let mut copy = table.copy_to(&destination).map_err(invalid)?;
    for (index, record) in replacements {
        copy.row_accessor_mut()
            .set_record_cell(index, "pbdescription", record)?;
    }
    copy.save(TableOptions::new(&destination))?;
    Ok(destination)
}

pub(super) fn relocate_mosaic_table(
    grid_record: &mut RecordValue,
    source_grid: &Path,
    target_grid: &Path,
) -> MsResult<()> {
    let Some(Value::Record(beam)) = grid_record.get("pbconvfunc") else {
        return Ok(());
    };
    let source = source_grid.join(MANAGED_VP_TABLE);
    if text(beam, "vptable").map(Path::new) != Some(source.as_path()) {
        return Ok(());
    }
    let target = target_grid.join(MANAGED_VP_TABLE);
    let mut table = Table::open(TableOptions::new(&target))?;
    for row in 0..table.row_count() {
        let mut record = table.cell_accessor(row, "pbdescription")?.record()?;
        let Some(relative) =
            text(&record, "imagim").and_then(|path| Path::new(path).strip_prefix(source_grid).ok())
        else {
            continue;
        };
        let path = target_grid.join(relative).to_string_lossy().into_owned();
        record.upsert("imagim", Value::Scalar(ScalarValue::String(path)));
        table
            .row_accessor_mut()
            .set_record_cell(row, "pbdescription", record)?;
    }
    table.save(TableOptions::new(&target))?;
    let mut beam = beam.clone();
    beam.upsert(
        "vptable",
        Value::Scalar(ScalarValue::String(target.to_string_lossy().into_owned())),
    );
    grid_record.upsert("pbconvfunc", Value::Record(beam));
    Ok(())
}

fn text<'a>(record: &'a RecordValue, key: &str) -> Option<&'a str> {
    match record.get(key) {
        Some(Value::Scalar(ScalarValue::String(value))) => Some(value),
        _ => None,
    }
}

/// A radial voltage pattern selected for the imager's next prediction.
#[derive(Clone, Debug, Default)]
pub struct ImagerVoltagePattern {
    /// Custom pattern table. With no table, use the telescope's default.
    pub table: Option<PathBuf>,
}

#[derive(Clone)]
pub(super) struct PreparedVoltagePattern {
    pub(super) pattern: BeamPattern,
    centre: MDirection,
    frame: MeasFrame,
}

impl ImagerVoltagePattern {
    pub(super) fn prepare(
        &self,
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
    ) -> MsResult<PreparedVoltagePattern> {
        let main = ms.main_table();
        let mut first = None;
        for row in &selected.selected_rows {
            let ScalarValue::Int32(array) =
                main.cell_accessor(row.row_index, "ARRAY_ID")?.scalar()?
            else {
                return Err(invalid("ARRAY_ID must be Int32"));
            };
            let key = (*array, row.field_id, row.data_desc_id, row.time_mjd_seconds);
            if first.as_ref().is_none_or(|(previous, _)| key < *previous) {
                first = Some((key, row));
            }
        }
        let Some(((array, field, _, time), first)) = first else {
            return Err(invalid("a voltage pattern needs a selected visibility"));
        };
        let telescope = ms
            .observation()?
            .string(usize::try_from(array).map_err(invalid)?, "TELESCOPE_NAME")?;
        let pattern = if let Some(path) = &self.table {
            let table = voltage_pattern::VoltagePatternTable::open(path).map_err(invalid)?;
            let record = table
                .default_record(&telescope)
                .map_err(invalid)?
                .ok_or_else(|| invalid(format!("no voltage pattern for {telescope}")))?;
            voltage_pattern::radial_pattern(&record)
                .map_err(invalid)?
                .pattern()
        } else {
            let frequency = ms.spectral_window()?.ref_frequency(0)?;
            match PrimaryBeam::for_telescope(&telescope, frequency) {
                Some(pattern) => pattern.pattern(),
                None => BeamPattern::for_dish_diameter(ms.antenna()?.dish_diameter(0)?)
                    .ok_or_else(|| {
                        invalid("no usable dish diameter for a default voltage pattern")
                    })?,
            }
        };
        let engine = MsCalEngine::new(ms)?;
        let pointing = if ms.subtable(SubtableId::Pointing).is_some() {
            // A homogeneous BeamSkyJones shares one entry for both antennas.
            // Its update stores the second antenna's direction last.
            ms.pointing()?.legacy_direction(first.antenna2_id, time)?
        } else {
            None
        };
        let centre = match pointing {
            Some(centre) => centre,
            None => engine
                .observation_direction_j2000(time, usize::try_from(field).map_err(invalid)?)?,
        };
        let frame = engine.spectral_frame_observatory_direction(time, centre.clone())?;
        Ok(PreparedVoltagePattern {
            pattern,
            centre,
            frame,
        })
    }
}

impl PreparedVoltagePattern {
    pub(super) fn separations(
        &self,
        direction: &DirectionCoordinate,
        [nx, ny]: [usize; 2],
    ) -> MsResult<Array2<f64>> {
        let centre = self
            .centre
            .convert_to(direction.direction_ref(), &self.frame)
            .map_err(invalid)?;
        let (lon0, lat0) = centre.as_angles();
        Ok(Array2::from_shape_fn((ny, nx), |(y, x)| {
            let Ok([lon, lat]) = direction.pixel_to_world([x as f64, y as f64]) else {
                return f64::INFINITY;
            };
            let separation = ((lat - lat0) * 0.5).sin().powi(2)
                + lat.cos() * lat0.cos() * ((lon - lon0) * 0.5).sin().powi(2);
            2.0 * separation.clamp(0., 1.).sqrt().asin()
        }))
    }
}
