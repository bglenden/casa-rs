// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare the legacy imager's voltage pattern on the model image.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_coordinates::{Coordinate, DirectionCoordinate};
use casa_images::PagedImage;
use casa_imaging_runtime::{Admitted, Demand, HostResources, ResourcePolicy, admit};
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::{MeasurementSet, MsResult, ResolvedMsSelection, SubtableId, voltage_pattern};
use casa_numerics::{BeamPattern, PrimaryBeam};
use casa_tables::{Table, TableOptions};
use casa_types::measures::ephemeris::offset_true_angle;
use casa_types::measures::{
    MeasFrame,
    direction::{DirectionRef, MDirection},
};
use casa_types::{RecordValue, ScalarValue, Value};
use ndarray::{Array2, ArrayD};

use super::invalid;
use crate::execute::single_receptor_angle;

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

pub(super) fn legacy_radial_pattern(path: &Path, telescope: &str) -> MsResult<PrimaryBeam> {
    let host = HostResources::detect().map_err(invalid)?;
    let _table_memory = admit(
        &host,
        &ResourcePolicy::Balanced,
        &Demand {
            phase: "legacy voltage-pattern table",
            memory: voltage_pattern::VoltagePatternTable::reading_bytes(path).map_err(invalid)?,
        },
    )
    .map_err(invalid)?;
    let table = Table::open(TableOptions::new(path))?;
    // VPSkyJones installs the first concrete root row for each telescope.
    // VPManager's defaults table belongs to a separate selection path.
    for row in 0..table.row_count() {
        let name = table.cell_accessor(row, "telescope")?.scalar()?;
        if !matches!(name, ScalarValue::String(name) if name == telescope) {
            continue;
        }
        let record = table.cell_accessor(row, "pbdescription")?.record()?;
        match record.get("name") {
            Some(Value::Scalar(ScalarValue::String(name))) if name != "REFERENCE" => {
                return voltage_pattern::radial_pattern(&record).map_err(invalid);
            }
            Some(Value::Scalar(ScalarValue::String(_))) | None => {}
            _ => return Err(invalid("legacy voltage pattern name must be a string")),
        }
    }
    Err(invalid(format!("no voltage pattern for {telescope}")))
}

/// A radial voltage pattern selected for the imager's next prediction.
#[derive(Clone, Debug, Default)]
pub struct ImagerVoltagePattern {
    /// Custom pattern table. With no table, use the telescope's default.
    pub table: Option<PathBuf>,
}

#[derive(Clone)]
pub(super) struct PreparedVoltagePattern {
    pattern: Arc<Admitted<BeamPattern>>,
    centre: MDirection,
    frame: MeasFrame,
    projected_model: bool,
    squint: Option<BeamSquint>,
}

#[derive(Clone, Copy)]
struct BeamSquint {
    beam: PrimaryBeam,
    reference_hz: f64,
    angle: f64,
}

impl ImagerVoltagePattern {
    pub(super) fn prepare(
        &self,
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
        squint: bool,
        scratch: bool,
    ) -> MsResult<PreparedVoltagePattern> {
        let main = ms.main_table();
        let mut first = None;
        let mut first_array = None;
        for row in &selected.selected_rows {
            let ScalarValue::Int32(array) =
                main.cell_accessor(row.row_index, "ARRAY_ID")?.scalar()?
            else {
                return Err(invalid("ARRAY_ID must be Int32"));
            };
            let key = (*array, row.field_id, row.data_desc_id, row.time_mjd_seconds);
            if squint && scratch && *first_array.get_or_insert(*array) != *array {
                return Err(invalid(
                    "legacy voltage-pattern squint for a field observed by multiple arrays is pending #1520",
                ));
            }
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
        let host = HostResources::detect().map_err(invalid)?;
        let reservation = reserve_radial_lookup(&host)?;
        let default_beam = self
            .table
            .is_none()
            .then(|| -> MsResult<_> {
                let frequency = ms.spectral_window()?.ref_frequency(0)?;
                Ok(PrimaryBeam::for_telescope(&telescope, frequency).map(|beam| (beam, frequency)))
            })
            .transpose()?
            .flatten();
        let pattern = if let Some(path) = &self.table {
            legacy_radial_pattern(path, &telescope)?.pattern()
        } else {
            match default_beam {
                Some((pattern, _)) => pattern.pattern(),
                None => BeamPattern::for_dish_diameter(ms.antenna()?.dish_diameter(0)?)
                    .ok_or_else(|| {
                        invalid("no usable dish diameter for a default voltage pattern")
                    })?,
            }
        };
        let engine = if squint {
            MsCalEngine::for_calibration(ms)?
        } else {
            MsCalEngine::new(ms)?
        };
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
        let squint = if squint {
            if self.table.is_some() || telescope != "VLA" {
                return Err(invalid(
                    "this legacy voltage-pattern squint is pending #1520",
                ));
            }
            if scratch
                && matches!(
                    centre.refer(),
                    DirectionRef::HADEC
                        | DirectionRef::AZEL
                        | DirectionRef::AZELSW
                        | DirectionRef::AZELGEO
                        | DirectionRef::AZELSWGEO
                )
                && selected
                    .selected_rows
                    .iter()
                    .any(|row| row.time_mjd_seconds != time)
            {
                return Err(invalid(
                    "legacy voltage-pattern squint with changing local pointing is pending #1520",
                ));
            }
            let (beam, frequency) = default_beam
                .filter(|(pattern, _)| pattern.squints())
                .ok_or_else(|| invalid("this VLA band has no supported squint (#1520)"))?;
            let ScalarValue::Int32(feed) =
                main.cell_accessor(first.row_index, "FEED2")?.scalar()?
            else {
                return Err(invalid("FEED2 must be Int32"));
            };
            let angle = engine.feed_parallactic_angle(
                time,
                usize::try_from(field).map_err(invalid)?,
                usize::try_from(first.antenna2_id).map_err(invalid)?,
            )? + single_receptor_angle(
                ms,
                first.antenna2_id,
                *feed,
                first.spectral_window_id,
                time,
            )?;
            Some(BeamSquint {
                beam,
                reference_hz: frequency,
                angle,
            })
        } else {
            None
        };
        Ok(PreparedVoltagePattern {
            pattern: Arc::new(Admitted {
                value: pattern,
                reservation,
            }),
            centre,
            frame,
            projected_model: telescope == "VLA" && self.table.is_none(),
            squint,
        })
    }
}

pub(super) fn reserve_radial_lookup(
    host: &HostResources,
) -> MsResult<casa_imaging_runtime::Reservation> {
    admit(
        host,
        &ResourcePolicy::Balanced,
        &Demand {
            phase: "legacy voltage-pattern lookup",
            memory: BeamPattern::BYTES + 64,
        },
    )
    .map_err(invalid)
}

impl PreparedVoltagePattern {
    pub(super) fn pattern(&self) -> &BeamPattern {
        &self.pattern.value
    }

    #[cfg(test)]
    pub(super) fn test_pattern() -> Self {
        use casa_types::measures::direction::DirectionRef;

        Self {
            pattern: Arc::new(Admitted {
                reservation: reserve_radial_lookup(&HostResources::detect().unwrap()).unwrap(),
                value: BeamPattern::for_dish_diameter(12.).unwrap(),
            }),
            centre: MDirection::from_angles(1., 0.4, DirectionRef::J2000),
            frame: MeasFrame::new(),
            projected_model: false,
            squint: None,
        }
    }

    pub(super) fn separations(
        &self,
        direction: &DirectionCoordinate,
        [nx, ny]: [usize; 2],
    ) -> MsResult<Array2<f64>> {
        let centre = self.centre_in_image_frame(direction)?;
        Self::separations_from(direction, [nx, ny], &centre)
    }

    pub(super) fn has_squint(&self) -> bool {
        self.squint.is_some()
    }

    pub(super) fn model_separations(
        &self,
        direction: &DirectionCoordinate,
        shape: [usize; 2],
    ) -> MsResult<Option<Array2<f64>>> {
        if self.projected_model {
            Ok(None)
        } else {
            self.separations(direction, shape).map(Some)
        }
    }

    fn shifted_pixels(
        &self,
        direction: &DirectionCoordinate,
        centre: &MDirection,
        frequency: f64,
    ) -> MsResult<Option<[[f64; 2]; 2]>> {
        let Some(BeamSquint {
            beam,
            reference_hz,
            angle,
        }) = self.squint
        else {
            return Ok(None);
        };
        let shifted = |right| {
            let offset = beam.squint_displacement_rad(reference_hz, frequency, angle, right);
            let shifted_centre = offset_true_angle(centre, offset).map_err(invalid)?;
            pixel_centre(direction, &shifted_centre)
        };
        Ok(Some([shifted(true)?, shifted(false)?]))
    }

    pub(super) fn model_plane(
        &self,
        direction: &DirectionCoordinate,
        radial: Option<&Array2<f64>>,
        values: &ArrayD<f32>,
        frequency: f64,
    ) -> MsResult<Vec<Array2<f32>>> {
        let [nx, ny, npol, _] = *values.shape() else {
            unreachable!("legacy model plane shape");
        };
        let projected = if radial.is_none() {
            let centre = self.centre_in_image_frame(direction)?;
            let shifted = if npol == 4 {
                self.shifted_pixels(direction, &centre, frequency)?
            } else {
                None
            };
            Some((pixel_centre(direction, &centre)?, shifted))
        } else {
            None
        };
        let pattern = self.pattern().at_frequency(frequency);
        let power = |distance: f64| pattern.power(distance.to_degrees() * 60.0 * frequency / 1e9);
        let increments = direction.increment();
        let increment_deg = [increments[0].to_degrees(), increments[1].to_degrees()];
        let projected_power =
            |x, y, centre| pattern.power(projected_radius(x, y, centre, increment_deg, frequency));
        Ok((0..npol)
            .map(|pol| {
                Array2::from_shape_fn((ny, nx), |(y, x)| {
                    if let Some((centre, shifted)) = projected {
                        if let Some([right, left]) = shifted
                            && matches!(pol, 0 | 3)
                        {
                            let i = values[[x, y, 0, 0]];
                            let v = values[[x, y, 3, 0]];
                            let right = (i + v) * projected_power(x, y, right);
                            let left = (i - v) * projected_power(x, y, left);
                            // Apply the displaced parallel-hand beams in correlation space.
                            return if pol == 0 {
                                0.5 * (right + left)
                            } else {
                                0.5 * (right - left)
                            };
                        }
                        return values[[x, y, pol, 0]] * projected_power(x, y, centre);
                    }
                    values[[x, y, pol, 0]] * power(radial.expect("radial plane")[[y, x]])
                })
            })
            .collect())
    }

    fn centre_in_image_frame(&self, direction: &DirectionCoordinate) -> MsResult<MDirection> {
        self.centre
            .convert_to(direction.direction_ref(), &self.frame)
            .map_err(invalid)
    }

    fn separations_from(
        direction: &DirectionCoordinate,
        [nx, ny]: [usize; 2],
        centre: &MDirection,
    ) -> MsResult<Array2<f64>> {
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

fn pixel_centre(direction: &DirectionCoordinate, centre: &MDirection) -> MsResult<[f64; 2]> {
    let (lon, lat) = centre.as_angles();
    direction.world_to_pixel([lon, lat]).map_err(invalid)
}

fn projected_radius(
    x: usize,
    y: usize,
    centre: [f64; 2],
    increment_deg: [f64; 2],
    frequency: f64,
) -> f64 {
    // PBMath1D rounds the squared components, radius and lookup position to Float.
    let x2 = ((x as f64 - centre[0]) * increment_deg[0]).powi(2) as f32;
    let y2 = ((y as f64 - centre[1]) * increment_deg[1]).powi(2) as f32;
    f64::from((f64::from((x2 + y2).sqrt()) * 60.0 * frequency / 1e9) as f32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imaging_memory_test::{isolated_test, peak_since, reset_peak};
    use casa_imaging_runtime::ReservationWindow;

    #[test]
    fn legacy_pattern_table_decode_fits_its_reservation() {
        if !isolated_test(
            module_path!(),
            "legacy_pattern_table_decode_fits_its_reservation",
        ) {
            return;
        }
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../casa-simulation/tests/data/sm_voltage_patterns/gaussian_pb.vp");
        let _ = HostResources::detect().unwrap();
        let reservations = ReservationWindow::open();
        let baseline = reset_peak();
        let definition = legacy_radial_pattern(&path, "ALMA").unwrap();
        let peak = peak_since(baseline);
        let charge = reservations.close();
        assert!(matches!(definition, PrimaryBeam::Gaussian { .. }));
        assert!(peak <= charge, "{peak} bytes used over {charge}");
    }
}
