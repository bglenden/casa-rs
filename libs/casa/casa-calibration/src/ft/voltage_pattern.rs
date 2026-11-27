// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare the legacy imager's voltage pattern on the model image.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use casa_components::{ComponentList, Flux, Polarisation, SkyComponent, SpectralKind, stokes_in};
use casa_coordinates::{Coordinate, DirectionCoordinate};
use casa_core::measures::ephemeris::offset_true_angle;
use casa_core::measures::frequency::{FrequencyRef, MFrequency};
use casa_core::measures::{
    MeasFrame,
    direction::{DirectionRef, MDirection},
};
use casa_core::{Correlation, RecordValue, ScalarValue, Value};
use casa_images::PagedImage;
use casa_imaging_operator::GridPolarization;
use casa_imaging_runtime::{Admitted, Demand, HostResources, ResourcePolicy, admit};
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::{MeasurementSet, MsResult, ResolvedMsSelection, ResolvedMsSelectionRow, SubtableId};
use casa_numerics::{BeamPattern, PrimaryBeam};
use casa_tables::{Table, TableOptions};
use ndarray::{Array2, ArrayD};
use num_complex::Complex64;

use super::invalid;
use crate::execute::{load_parallactic_angle_state, single_receptor_angle};

const MANAGED_VP_TABLE: &str = "CASARS_VOLTAGE_PATTERNS";

pub(super) fn legacy_mosaic_table(source: &Path, grid: &Path) -> MsResult<PathBuf> {
    let table = casa_ms::VoltagePatternTable::open(source).map_err(invalid)?;
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
    read_legacy_radial(&host, path, telescope, |record| {
        casa_ms::radial_pattern(record).map_err(invalid)
    })
}

fn read_legacy_radial<T>(
    host: &HostResources,
    path: &Path,
    telescope: &str,
    decode: impl FnOnce(&RecordValue) -> MsResult<T>,
) -> MsResult<T> {
    let _table_memory = admit(
        host,
        &ResourcePolicy::Balanced,
        &Demand {
            phase: "legacy voltage-pattern table",
            memory: casa_ms::VoltagePatternTable::reading_bytes(path).map_err(invalid)?,
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
                return decode(&record);
            }
            Some(Value::Scalar(ScalarValue::String(_))) | None => {}
            _ => return Err(invalid("legacy voltage pattern name must be a string")),
        }
    }
    Err(invalid(format!("no voltage pattern for {telescope}")))
}

fn legacy_radial_setup(
    host: &HostResources,
    record: &RecordValue,
    requested_squint: bool,
    allow_curves: bool,
) -> MsResult<TableVoltagePattern> {
    let (peak, retained) = casa_ms::radial_curve_storage(record);
    let mut reservation = admit(
        host,
        &ResourcePolicy::Balanced,
        &Demand {
            phase: "legacy voltage-pattern lookup",
            memory: peak,
        },
    )
    .map_err(invalid)?;
    let value = if allow_curves {
        casa_ms::radial_lookup(record)
    } else {
        casa_ms::radial_pattern(record).map(PrimaryBeam::pattern)
    }
    .map_err(invalid)?;
    reservation.retain(retained);
    let squint = if requested_squint {
        casa_ms::radial_squint(record)
            .map_err(invalid)?
            .map(|(offset_rad, reference_hz)| BeamSquintLaw::Table {
                offset_rad,
                reference_hz,
            })
    } else {
        None
    };
    let projected_image = allow_curves
        && matches!(
            text(record, "name"),
            Some("POLY" | "IPOLY" | "COSPOLY" | "NUMERIC")
        );
    Ok(TableVoltagePattern {
        pattern: Arc::new(Admitted { value, reservation }),
        squint,
        projected_image,
    })
}

struct TableVoltagePattern {
    pattern: Arc<Admitted<BeamPattern>>,
    squint: Option<BeamSquintLaw>,
    projected_image: bool,
}

/// A radial voltage pattern selected for the imager's next prediction.
#[derive(Clone, Debug, Default)]
pub struct ImagerVoltagePattern {
    /// Custom pattern table. With no table, use the telescope's default.
    pub table: Option<PathBuf>,
}

#[derive(Clone)]
pub(crate) struct PreparedVoltagePattern {
    pattern: Arc<Admitted<BeamPattern>>,
    centre: MDirection,
    frame: MeasFrame,
    projected_model: bool,
    squint: Option<BeamSquint>,
    // GOFIGURE prepares circular component flux even on a radial beam.
    component_squint: bool,
}

#[derive(Clone, Copy)]
struct BeamSquint {
    law: BeamSquintLaw,
    angle: f64,
}

#[derive(Clone, Copy)]
enum BeamSquintLaw {
    Telescope {
        beam: PrimaryBeam,
        reference_hz: f64,
    },
    Table {
        offset_rad: [f64; 2],
        reference_hz: f64,
    },
}

impl BeamSquint {
    fn displacement(self, frequency: f64, right: bool) -> [f64; 2] {
        match self.law {
            BeamSquintLaw::Telescope { beam, reference_hz } => {
                beam.squint_displacement_rad(reference_hz, frequency, self.angle, right)
            }
            BeamSquintLaw::Table {
                offset_rad,
                reference_hz,
            } => PrimaryBeam::stored_squint_displacement_rad(
                offset_rad,
                reference_hz,
                frequency,
                self.angle,
                right,
            ),
        }
    }
}

struct ComponentVoltage<'a> {
    pattern: &'a PreparedVoltagePattern,
    component: &'a SkyComponent,
    datum_basis: Polarisation,
    original: Flux,
    beam_flux: Flux,
    stokes: [f64; 4],
    centre: MDirection,
    separation: f64,
}

impl ComponentVoltage<'_> {
    fn flux_at(&self, frequency: f64, first_channel: bool) -> MsResult<[Complex64; 4]> {
        let pattern = self.pattern.pattern();
        let power = |separation| f64::from(pattern.power_at(separation, frequency));
        let central = power(self.separation);
        let mut gains = [central; 4];
        if let Some(squint) = self.pattern.squint {
            for (index, right) in [(0, true), (3, false)] {
                let shift = squint.displacement(frequency, right);
                let centre = offset_true_angle(&self.centre, shift).map_err(invalid)?;
                gains[index] = power(angular_separation(
                    self.component.shape().reference_direction().as_angles(),
                    centre.as_angles(),
                ));
            }
        }
        let mut flux = self.beam_flux.clone();
        let values = *flux.values();
        flux.set_values(std::array::from_fn(|index| values[index] * gains[index]));
        flux.convert_polarisation(self.original.polarisation());
        let sample_basis = if self.pattern.component_squint && !first_channel {
            // PBMath converts the input to circular during the first channel's
            // application. Later channels sample that converted component.
            Polarisation::Circular
        } else {
            self.original.polarisation()
        };
        let spectrum = stokes_in(sample_basis, self.sample_stokes(frequency)?);
        // BeamSkyJones samples a point spectrum in the original basis,
        // normalizes by the magnitude of the stored flux and tabulates
        // the result. This also preserves its signed-flux behavior.
        let values = *flux.values();
        flux.set_values(std::array::from_fn(|index| {
            let amplitude = self.original.values()[index].norm();
            let scale = if amplitude > 0. {
                amplitude.recip()
            } else {
                1.
            };
            values[index] * spectrum[index] * scale
        }));
        flux.convert_polarisation(self.datum_basis);
        Ok(*flux.values())
    }

    fn sample_stokes(&self, frequency: f64) -> MsResult<[f64; 4]> {
        let spectrum = self.component.spectrum();
        let in_frame = |reference| {
            MFrequency::new(frequency, FrequencyRef::LSRK)
                .convert_to(reference, &MeasFrame::new())
                .map(|converted| converted.hz())
                .map_err(invalid)
        };
        match spectrum.kind() {
            SpectralKind::Constant => Ok(self.stokes),
            SpectralKind::Tabular(table) => {
                let sample = if matches!(table.frame(), FrequencyRef::LSRK | FrequencyRef::REST) {
                    // The scalar tabular sampler uses its reference when the
                    // channel and table already use the same frame.
                    spectrum.reference_frequency().hz()
                } else {
                    in_frame(table.frame())?
                };
                let frequencies = table.frequencies_hz();
                if !frequencies
                    .first()
                    .zip(frequencies.last())
                    .is_some_and(|(&first, &last)| {
                        (first.min(last)..=first.max(last)).contains(&sample)
                    })
                {
                    return Err(invalid(
                        "component beam sample is outside the tabular spectrum",
                    ));
                }
                spectrum.sample_stokes(sample, self.stokes).map_err(invalid)
            }
            kind => {
                let sample = in_frame(spectrum.reference_frequency().refer())?;
                let mut stokes = spectrum
                    .sample_stokes(sample, self.stokes)
                    .map_err(invalid)?;
                if matches!(kind, SpectralKind::SpectralIndex(index) if index.stokes_indices().is_none())
                {
                    stokes[1..].copy_from_slice(&self.stokes[1..]);
                }
                Ok(stokes)
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
struct SpectrumBufferKey {
    source: usize,
    window: i32,
    epoch: u64,
    start_channel: usize,
    basis: Polarisation,
}

pub(crate) struct ApparentComponentSpectra {
    values: Vec<[Complex64; 4]>,
    _memory: casa_imaging_runtime::Reservation,
}

impl ApparentComponentSpectra {
    pub(crate) fn component(&self, index: usize, channels: usize) -> &[[Complex64; 4]] {
        &self.values[index * channels..(index + 1) * channels]
    }
}

/// One apparent spectrum per component in the current legacy buffer. In
/// particular, the buffer's epoch follows frame-dependent sky directions.
pub(crate) struct ComponentVoltageCache {
    pattern: PreparedVoltagePattern,
    last: Mutex<Option<(SpectrumBufferKey, Arc<ApparentComponentSpectra>)>>,
}

impl ComponentVoltageCache {
    pub(crate) fn new(pattern: PreparedVoltagePattern) -> Self {
        Self {
            pattern,
            last: Mutex::new(None),
        }
    }

    pub(crate) fn spectra(
        &self,
        components: &ComponentList,
        frequencies: &[f64],
        basis: Polarisation,
        (window, epoch, start_channel): (i32, f64, usize),
        frame: &MeasFrame,
    ) -> MsResult<Arc<ApparentComponentSpectra>> {
        let key = SpectrumBufferKey {
            source: std::ptr::from_ref(components) as usize,
            window,
            epoch: epoch.to_bits(),
            start_channel,
            basis,
        };
        let mut last = self.last.lock().map_err(invalid)?;
        if let Some((held, spectra)) = last.as_ref()
            && *held == key
        {
            return Ok(Arc::clone(spectra));
        }
        *last = None;
        let cells = components
            .len()
            .checked_mul(frequencies.len())
            .ok_or_else(|| invalid("component beam spectrum exceeds addressable storage"))?;
        let host = HostResources::detect().map_err(invalid)?;
        let memory = admit(
            &host,
            &ResourcePolicy::Balanced,
            &Demand {
                phase: "legacy component voltage spectrum",
                memory: (cells as u64).saturating_mul(size_of::<[Complex64; 4]>() as u64),
            },
        )
        .map_err(invalid)?;
        let mut values = Vec::with_capacity(cells);
        for component in components.iter() {
            let response = self.pattern.component(component, basis, frame)?;
            for (channel, &frequency) in frequencies.iter().enumerate() {
                values.push(response.flux_at(frequency, channel == start_channel)?);
            }
        }
        let spectra = Arc::new(ApparentComponentSpectra {
            values,
            _memory: memory,
        });
        *last = Some((key, Arc::clone(&spectra)));
        Ok(spectra)
    }
}

/// Partition scratch rows in legacy visibility-buffer order. The first row
/// in each state supplies the apparent-sky image for that state.
pub(super) fn scratch_voltage_batches<'a>(
    ms: &MeasurementSet,
    selection: &'a ResolvedMsSelection,
    increment: f64,
    local_pointings: bool,
) -> MsResult<impl Iterator<Item = Vec<ResolvedMsSelectionRow>> + use<'a>> {
    let host = HostResources::detect().map_err(invalid)?;
    let order_charge = admit(
        &host,
        &ResourcePolicy::Balanced,
        &Demand {
            phase: "legacy voltage-pattern scratch batches",
            memory: voltage_order_bytes(selection.selected_rows.len()),
        },
    )
    .map_err(invalid)?;
    let main = ms.main_table();
    let observations = ms.observation()?.row_count();
    let key = |row: &ResolvedMsSelectionRow| -> MsResult<_> {
        let ScalarValue::Int32(array) = main.cell_accessor(row.row_index, "ARRAY_ID")?.scalar()?
        else {
            return Err(invalid("ARRAY_ID must be Int32"));
        };
        if usize::try_from(*array).map_or(true, |id| id >= observations) {
            return Err(invalid(
                "ARRAY_ID has no observation for this voltage pattern",
            ));
        }
        Ok((*array, row.field_id, row.data_desc_id))
    };
    let mut ordered = selection
        .selected_rows
        .iter()
        .map(|row| Ok((key(row)?, row)))
        .collect::<MsResult<Vec<_>>>()?;
    ordered.sort_unstable_by(|(a, left), (b, right)| {
        a.cmp(b)
            .then_with(|| left.time_mjd_seconds.total_cmp(&right.time_mjd_seconds))
            .then_with(|| left.row_index.cmp(&right.row_index))
    });
    let buffers = usize::from(!ordered.is_empty())
        + ordered
            .windows(2)
            .filter(|pair| {
                pair[0].0 != pair[1].0 || pair[0].1.time_mjd_seconds != pair[1].1.time_mjd_seconds
            })
            .count();
    let _working_charge = admit(
        &host,
        &ResourcePolicy::Balanced,
        &Demand {
            phase: "legacy voltage-pattern scheduling workspace",
            memory: voltage_planning_bytes(ms, buffers, local_pointings),
        },
    )
    .map_err(invalid)?;
    let feed_geometry = load_parallactic_angle_state(ms)?;
    let pointing = local_pointings.then(|| ms.pointing()).transpose()?;
    let mut buffer = None;
    let mut held = None;
    let mut starts = Vec::new();
    for (index, &((array, field, dd), row)) in ordered.iter().enumerate() {
        let time = row.time_mjd_seconds;
        let current = (array, field, dd, time);
        // CubeSkyEquation's internal-change probe stops at row zero in this
        // path. The other rows in a legacy visibility buffer share its beam.
        if buffer == Some(current) {
            continue;
        }
        buffer = Some(current);
        let angle = |antenna, column| -> MsResult<f32> {
            let ScalarValue::Int32(feed) = main.cell_accessor(row.row_index, column)?.scalar()?
            else {
                return Err(invalid(format!("{column} must be Int32")));
            };
            Ok(feed_geometry
                .feed_parallactic_angle(time, field, antenna, *feed, row.spectral_window_id)
                .map_err(invalid)? as f32)
        };
        let first = angle(row.antenna1_id, "FEED1")?;
        let second = angle(row.antenna2_id, "FEED2")?;
        let refresh = held.as_ref().is_none_or(|state: &HeldVoltageState| {
            (array, field) != state.field
                || (state.local && time != state.time)
                || f64::from((first - state.angle).abs()) > increment
                || f64::from((second - state.angle).abs()) > increment
        });
        if refresh {
            starts.push(index);
            let local = if let Some(pointing) = &pointing {
                pointing
                    .legacy_direction(row.antenna2_id, time)?
                    .is_some_and(|direction| is_local_direction(direction.refer()))
            } else {
                false
            };
            held = Some(HeldVoltageState {
                field: (array, field),
                time,
                angle: second,
                local,
            });
        }
    }
    starts.push(ordered.len());
    tracing::debug!(
        batches = starts.len() - 1,
        rows = ordered.len(),
        "planned voltage-pattern refreshes"
    );
    Ok((0..starts.len() - 1).map(move |batch| {
        let _charge = &order_charge;
        let mut rows = ordered[starts[batch]..starts[batch + 1]]
            .iter()
            .map(|(_, row)| (**row).clone())
            .collect::<Vec<_>>();
        rows.sort_unstable_by_key(|row| row.row_index);
        rows
    }))
}

fn voltage_order_bytes(rows: usize) -> u64 {
    let per_row = size_of::<((i32, i32, i32), &ResolvedMsSelectionRow)>()
        + 2 * size_of::<usize>()
        + size_of::<ResolvedMsSelectionRow>();
    (rows as u64).saturating_mul(per_row as u64)
}

fn voltage_planning_bytes(ms: &MeasurementSet, buffers: usize, local_pointings: bool) -> u64 {
    let feeds = ms.feed().map_or(0, |feed| feed.row_count()) as u64;
    let antennas = ms.antenna().map_or(0, |antennas| antennas.row_count()) as u64;
    let fields = ms.field().map_or(0, |fields| fields.row_count()) as u64;
    // The Measures provider can load its catalogs on the first angle.
    // Include FEED hash buckets and vector capacity, the engine's antenna
    // and field state, and caches for both antennas per visibility buffer.
    (casa_ms::MEASURES_RUNTIME_BYTES as u64)
        .saturating_add(128_u64 << 10)
        .saturating_add(feeds.saturating_mul(256))
        .saturating_add(antennas.saturating_mul(256))
        .saturating_add(fields.saturating_mul(512))
        .saturating_add((buffers as u64).saturating_mul(1024))
        .saturating_add(if local_pointings {
            voltage_pointing_bytes(ms)
        } else {
            0
        })
}

fn voltage_pointing_bytes(ms: &MeasurementSet) -> u64 {
    // A local pointing lookup can collect every timestamp for one antenna.
    let rows = ms.pointing().map_or(0, |pointing| pointing.row_count()) as u64;
    rows.saturating_mul(96)
}

struct HeldVoltageState {
    field: (i32, i32),
    time: f64,
    angle: f32,
    local: bool,
}

pub(super) fn pointing_may_use_local_frame(ms: &MeasurementSet) -> bool {
    let Some(pointing) = ms.subtable(SubtableId::Pointing) else {
        return false;
    };
    if pointing.row_count() == 0 {
        return false;
    }
    let Some(Value::Record(info)) = pointing
        .column_keywords("DIRECTION")
        .and_then(|keywords| keywords.get("MEASINFO"))
    else {
        return false;
    };
    info.get("VarRefCol").is_some()
        || text(info, "Ref")
            .and_then(|reference| reference.parse().ok())
            .is_some_and(is_local_direction)
}

fn is_local_direction(reference: DirectionRef) -> bool {
    matches!(
        reference,
        DirectionRef::HADEC
            | DirectionRef::AZEL
            | DirectionRef::AZELSW
            | DirectionRef::AZELGEO
            | DirectionRef::AZELSWGEO
    )
}

impl ImagerVoltagePattern {
    pub(super) fn prepare(
        &self,
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
        squint: bool,
        scratch: bool,
        allow_curves: bool,
    ) -> MsResult<PreparedVoltagePattern> {
        let main = ms.main_table();
        let mut first = None;
        let mut first_array = None;
        let mut mixed_arrays = false;
        for row in &selected.selected_rows {
            let ScalarValue::Int32(array) =
                main.cell_accessor(row.row_index, "ARRAY_ID")?.scalar()?
            else {
                return Err(invalid("ARRAY_ID must be Int32"));
            };
            let key = (*array, row.field_id, row.data_desc_id, row.time_mjd_seconds);
            mixed_arrays |= *first_array.get_or_insert(*array) != *array;
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
        let default_beam = self
            .table
            .is_none()
            .then(|| -> MsResult<_> {
                let frequency = ms.spectral_window()?.ref_frequency(0)?;
                Ok(PrimaryBeam::for_telescope(&telescope, frequency).map(|beam| (beam, frequency)))
            })
            .transpose()?
            .flatten();
        let custom = self
            .table
            .as_ref()
            .map(|path| {
                read_legacy_radial(&host, path, &telescope, |record| {
                    legacy_radial_setup(&host, record, squint, allow_curves)
                })
            })
            .transpose()?;
        let component_squint = squint;
        let squint_law = if squint {
            match (custom.as_ref(), default_beam) {
                (Some(custom), _) => custom.squint,
                (None, Some((beam, reference_hz)))
                    if beam.squint_displacement_rad(reference_hz, reference_hz, 0., true)
                        != [0.; 2] =>
                {
                    Some(BeamSquintLaw::Telescope { beam, reference_hz })
                }
                // CASA accepts GOFIGURE for the other default profiles and
                // its dish-diameter fallback, which have no physical offset.
                (None, _) => None,
            }
        } else {
            None
        };
        if squint_law.is_some() && scratch && mixed_arrays {
            return Err(invalid(
                "legacy voltage-pattern squint for a field observed by multiple arrays is pending #1520",
            ));
        }
        let projected_model = custom.as_ref().is_some_and(|custom| custom.projected_image)
            || (telescope == "VLA" && self.table.is_none())
            || squint_law.is_some();
        let pattern = if let Some(custom) = custom {
            custom.pattern
        } else {
            let reservation = reserve_radial_lookup(&host)?;
            let value = match default_beam {
                Some((pattern, _)) => pattern.pattern(),
                None => BeamPattern::for_dish_diameter(ms.antenna()?.dish_diameter(0)?)
                    .ok_or_else(|| {
                        invalid("no usable dish diameter for a default voltage pattern")
                    })?,
            };
            Arc::new(Admitted { value, reservation })
        };
        let engine = if squint_law.is_some() {
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
        let squint = if let Some(law) = squint_law {
            if scratch
                && is_local_direction(centre.refer())
                && selected
                    .selected_rows
                    .iter()
                    .any(|row| row.time_mjd_seconds != time)
            {
                return Err(invalid(
                    "legacy voltage-pattern squint with changing local pointing is pending #1520",
                ));
            }
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
            Some(BeamSquint { law, angle })
        } else {
            None
        };
        Ok(PreparedVoltagePattern {
            pattern,
            centre,
            frame,
            projected_model,
            squint,
            component_squint,
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

    fn component<'a>(
        &'a self,
        component: &'a SkyComponent,
        datum_basis: Polarisation,
        frame: &MeasFrame,
    ) -> MsResult<ComponentVoltage<'a>> {
        let direction = component.shape().reference_direction();
        let centre = self
            .centre
            .convert_to(direction.refer(), frame)
            .map_err(invalid)?;
        let separation = angular_separation(direction.as_angles(), centre.as_angles());
        let basis = match component.flux().polarisation() {
            Polarisation::Stokes => datum_basis,
            basis => basis,
        };
        let [i, q, u, v] = component.flux().stokes_jy_values().map_err(invalid)?;
        let mut original = Flux::stokes_jy(i, q, u, v);
        original.convert_polarisation(basis);
        let mut beam_flux = original.clone();
        if self.component_squint {
            beam_flux.convert_polarisation(Polarisation::Circular);
        }
        Ok(ComponentVoltage {
            pattern: self,
            component,
            datum_basis,
            original,
            beam_flux,
            stokes: [i, q, u, v],
            centre,
            separation,
        })
    }

    #[cfg(test)]
    pub(super) fn test_pattern() -> Self {
        use casa_core::measures::direction::DirectionRef;

        Self {
            pattern: Arc::new(Admitted {
                reservation: reserve_radial_lookup(&HostResources::detect().unwrap()).unwrap(),
                value: BeamPattern::for_dish_diameter(12.).unwrap(),
            }),
            centre: MDirection::from_angles(1., 0.4, DirectionRef::J2000),
            frame: MeasFrame::new(),
            projected_model: false,
            squint: None,
            component_squint: false,
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
        let Some(squint) = self.squint else {
            return Ok(None);
        };
        let shifted = |right| {
            let offset = squint.displacement(frequency, right);
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
            Some(pixel_centre(direction, &centre)?)
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
                    if let Some(centre) = projected {
                        return values[[x, y, pol, 0]] * projected_power(x, y, centre);
                    }
                    values[[x, y, pol, 0]] * power(radial.expect("radial plane")[[y, x]])
                })
            })
            .collect())
    }

    pub(super) fn squinted_model_tapers(
        &self,
        direction: &DirectionCoordinate,
        [nx, ny]: [usize; 2],
        frequency: f64,
        polarizations: &[GridPolarization],
    ) -> MsResult<Vec<Array2<f32>>> {
        let centre = self.centre_in_image_frame(direction)?;
        let shifted = self.shifted_pixels(direction, &centre, frequency)?;
        let centre = pixel_centre(direction, &centre)?;
        let [right, left] = shifted.unwrap_or([centre; 2]);
        let pattern = self.pattern().at_frequency(frequency);
        let increments = direction.increment();
        let increment_deg = [increments[0].to_degrees(), increments[1].to_degrees()];
        Ok(polarizations
            .iter()
            .map(|polarization| {
                let centre = match polarization {
                    GridPolarization::Correlation(Correlation::RR) => right,
                    GridPolarization::Correlation(Correlation::LL) => left,
                    _ => centre,
                };
                Array2::from_shape_fn((ny, nx), |(y, x)| {
                    pattern.power(projected_radius(x, y, centre, increment_deg, frequency))
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
        let centre = centre.as_angles();
        Ok(Array2::from_shape_fn((ny, nx), |(y, x)| {
            let Ok([lon, lat]) = direction.pixel_to_world([x as f64, y as f64]) else {
                return f64::INFINITY;
            };
            angular_separation((lon, lat), centre)
        }))
    }
}

fn angular_separation((lon, lat): (f64, f64), (lon0, lat0): (f64, f64)) -> f64 {
    let separation = ((lat - lat0) * 0.5).sin().powi(2)
        + lat.cos() * lat0.cos() * ((lon - lon0) * 0.5).sin().powi(2);
    2.0 * separation.clamp(0., 1.).sqrt().asin()
}

#[cfg(test)]
#[test]
fn apparent_component_spectra_are_shared_by_baselines_at_one_epoch() {
    use casa_components::{ComponentShape, SpectralModel};

    let mut components = ComponentList::new();
    components.push(SkyComponent::new(
        Flux::stokes_jy(3., 0., 0., 0.),
        ComponentShape::point(MDirection::from_angles(1., 0.4, DirectionRef::J2000)),
        SpectralModel::constant(MFrequency::new(1.4e9, FrequencyRef::LSRK)),
    ));
    let cache = ComponentVoltageCache::new(PreparedVoltagePattern::test_pattern());
    let frame = MeasFrame::new();
    let sample = |time| {
        cache
            .spectra(
                &components,
                &[1.4e9, 1.5e9],
                Polarisation::Linear,
                (0, time, 0),
                &frame,
            )
            .unwrap()
    };
    let first = sample(100.);
    for _ in 0..32 {
        assert!(Arc::ptr_eq(&first, &sample(100.)));
    }
    let next = sample(101.);
    assert!(!Arc::ptr_eq(&first, &next));
    assert_eq!(first.component(0, 2), next.component(0, 2));
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
    use casa_core::ArrayValue;
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

    #[test]
    fn sparse_voltage_schedule_accounts_for_large_local_pointing_table() {
        if !isolated_test(
            module_path!(),
            "sparse_voltage_schedule_accounts_for_large_local_pointing_table",
        ) {
            return;
        }
        if casa_ms::open_measures_runtime().is_err() {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../casacore/casa-ms/tests/fixtures/mssel_test_small.ms.tgz");
        tar::Archive::new(flate2::read::GzDecoder::new(
            std::fs::File::open(archive).unwrap(),
        ))
        .unpack(root.path())
        .unwrap();
        let mut ms = MeasurementSet::open(root.path().join("mssel_test_small.ms")).unwrap();
        let mut selection = crate::session::resolve_casa_calibration_selection(
            &ms,
            &casa_ms::MsSelection::default(),
        )
        .unwrap();
        selection.selected_rows.truncate(1);
        let row = &selection.selected_rows[0];
        let first = row.time_mjd_seconds;
        let mut pointing = ms.pointing_mut().unwrap();
        for i in 1..=16_384 {
            let time = first + f64::from(i);
            let record = RecordValue::new(vec![
                casa_core::RecordField::new(
                    "ANTENNA_ID",
                    Value::Scalar(ScalarValue::Int32(row.antenna2_id)),
                ),
                casa_core::RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time))),
                casa_core::RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(1.))),
                casa_core::RecordField::new(
                    "TIME_ORIGIN",
                    Value::Scalar(ScalarValue::Float64(time)),
                ),
                casa_core::RecordField::new(
                    "NAME",
                    Value::Scalar(ScalarValue::String(String::new())),
                ),
                casa_core::RecordField::new("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
                casa_core::RecordField::new(
                    "DIRECTION",
                    Value::Array(ArrayValue::Float64(ArrayD::zeros(vec![2, 1]))),
                ),
                casa_core::RecordField::new(
                    "TARGET",
                    Value::Array(ArrayValue::Float64(ArrayD::zeros(vec![2, 1]))),
                ),
                casa_core::RecordField::new("TRACKING", Value::Scalar(ScalarValue::Bool(true))),
            ]);
            pointing.table_mut().add_row(record).unwrap();
        }
        let _ = HostResources::detect().unwrap();
        let pointing = ms.pointing().unwrap();
        let baseline = reset_peak();
        let _ = pointing.legacy_direction(row.antenna2_id, first).unwrap();
        let pointing_peak = peak_since(baseline);
        assert!(pointing_peak > 128 << 10);
        assert!(pointing_peak <= voltage_pointing_bytes(&ms));
        let reservations = ReservationWindow::open();
        let baseline = reset_peak();
        let mut batches = scratch_voltage_batches(&ms, &selection, 0., true).unwrap();
        assert_eq!(batches.next().unwrap()[0].row_index, row.row_index);
        assert!(batches.next().is_none());
        let peak = peak_since(baseline);
        let charge = reservations.close();
        assert!(peak <= charge, "{peak} bytes used over {charge}");
    }
}
