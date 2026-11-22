// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read each legacy visibility chunk once and fit a pointing solution.

use std::collections::BTreeMap;

use casa_coordinates::{Coordinate, CoordinateModel, StokesType};
use casa_imaging_model::GridFtProjection;
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::{MeasurementSet, MsResult, ResolvedMsSelection, ResolvedMsSelectionRow};
use casa_tables::RequiredScalarColumnValues;
use casa_types::ArrayValue;
use casa_types::measures::direction::MDirection;

use super::degrid::PointingSample;
use super::fit::{self, Baseline, Observation};
use super::{LegacyEpSolver, PreparedEpModel, invalid, table};

const SPEED_OF_LIGHT: f64 = 299_792_458.;

pub(super) struct Slot {
    pub(super) spw: i32,
    pub(super) field: i32,
    pub(super) time: f64,
    pub(super) offsets: Vec<[f32; 4]>,
}

struct Rows<'a> {
    ordered: Vec<(&'a ResolvedMsSelectionRow, [i32; 4])>,
    start: usize,
    interval: f64,
}

impl<'a> Rows<'a> {
    fn new(
        ms: &MeasurementSet,
        selected: &'a ResolvedMsSelection,
        interval: f64,
    ) -> MsResult<Self> {
        let rows = selected.row_indices().collect::<Vec<_>>();
        let keys = ms
            .main_table()
            .required_scalar_columns_owned_for_rows(&["ARRAY_ID", "SCAN_NUMBER"], &rows)?;
        let [
            Some(RequiredScalarColumnValues::Int32(arrays)),
            Some(RequiredScalarColumnValues::Int32(scans)),
        ] = [keys.get("ARRAY_ID"), keys.get("SCAN_NUMBER")]
        else {
            return Err(invalid("EP solve requires ARRAY_ID and SCAN_NUMBER"));
        };
        let mut ordered = selected
            .selected_rows
            .iter()
            .zip(arrays)
            .zip(scans)
            .map(|((row, &array), &scan)| {
                (
                    row,
                    [
                        array,
                        if interval == 0. { scan } else { 0 },
                        row.field_id,
                        row.data_desc_id,
                    ],
                )
            })
            .collect::<Vec<_>>();
        ordered.sort_by(|(a, ka), (b, kb)| {
            ka.cmp(kb)
                .then(a.time_mjd_seconds.total_cmp(&b.time_mjd_seconds))
        });
        Ok(Self {
            ordered,
            start: 0,
            interval,
        })
    }

    fn next(&mut self) -> Option<&[(&'a ResolvedMsSelectionRow, [i32; 4])]> {
        let start = self.start;
        let &(first, key) = self.ordered.get(start)?;
        self.start += 1;
        while let Some((row, next_key)) = self.ordered.get(self.start) {
            if *next_key != key
                || (self.interval > 0.
                    && row.time_mjd_seconds - first.time_mjd_seconds >= self.interval)
            {
                break;
            }
            self.start += 1;
        }
        Some(&self.ordered[start..self.start])
    }
}

impl LegacyEpSolver {
    pub(in crate::calibrater_lifecycle) fn fit_attached(
        &self,
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
    ) -> MsResult<usize> {
        let model = self
            .model
            .as_ref()
            .ok_or_else(|| invalid("EP solve needs an image model"))?;
        if model.correlation_planes.as_slice() != [StokesType::RR, StokesType::LL] {
            return Err(invalid(
                "EP fit needs the two circular parallel hands RR and LL",
            ));
        }
        let engine = MsCalEngine::new(ms)?;
        let antenna_count = ms.antenna()?.row_count();
        let mut rows = Rows::new(ms, selected, self.interval_seconds)?;
        let mut offsets_by_spw = BTreeMap::new();
        let mut results = Vec::new();
        while let Some(chunk) = rows.next() {
            let first = chunk[0].0;
            let baselines =
                load_chunk(ms, selected, model, &engine, chunk, self.preaverage_seconds)?;
            if baselines
                .iter()
                .any(|baseline| baseline.antennas.iter().any(|&id| id >= antenna_count))
            {
                return Err(invalid("EP visibility references an unknown antenna"));
            }
            let offsets = offsets_by_spw
                .entry(first.spectral_window_id)
                .or_insert_with(|| vec![[0.; 4]; antenna_count]);
            if fit::solve(&baselines, offsets) {
                results.push(Slot {
                    spw: first.spectral_window_id,
                    field: first.field_id,
                    time: first.time_mjd_seconds,
                    offsets: offsets.clone(),
                });
            }
        }
        table::write(ms, self, &results).map_err(invalid)?;
        Ok(results.len() * antenna_count)
    }
}

fn load_chunk(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    model: &PreparedEpModel,
    geometry: &MsCalEngine,
    chunk: &[(&ResolvedMsSelectionRow, [i32; 4])],
    preaverage_seconds: f64,
) -> MsResult<Vec<Baseline>> {
    let rows = chunk
        .iter()
        .map(|(row, _)| *row)
        .filter(|row| !row.flag_row && row.antenna1_id != row.antenna2_id)
        .collect::<Vec<_>>();
    let Some(first) = rows.first() else {
        return Ok(Vec::new());
    };
    let spectra = ms.spectral_window()?;
    let spw = first.spectral_window_id;
    let frequencies = spectra.chan_freq(spw as usize)?;
    let channels = selected.channel_selections.get(&spw).map_or_else(
        || Ok((0..frequencies.len()).collect()),
        |selection| selection.indices(frequencies.len()),
    )?;
    let CoordinateModel::Spectral(axis) = model.coordinates.coordinate(2) else {
        return Err(invalid("EP model is missing its spectral coordinate"));
    };
    let matches = crate::ft::image::match_channels(axis, 1, &frequencies);
    let CoordinateModel::Direction(direction) = model.coordinates.coordinate(0) else {
        return Err(invalid("EP model is missing its direction coordinate"));
    };
    let centre = direction.reference_value();
    let target = MDirection::from_angles(centre[0], centre[1], direction.direction_ref());
    let field = first.field_id as usize;
    let source = geometry.field_direction_j2000(field)?;
    let projection = GridFtProjection::between_directions(source, &target);
    let indices = rows.iter().map(|row| row.row_index).collect::<Vec<_>>();
    let read = |column| {
        ms.main_table()
            .column_accessor(column)?
            .array_cells_owned_uncached(&indices)
    };
    let data = read("DATA")?;
    let flags = read("FLAG")?;
    let uvw = read("UVW")?;
    let weights = read("WEIGHT")?;
    let mut baselines = BTreeMap::<_, Baseline>::new();
    let mut angles = BTreeMap::new();
    let mut epoch_start = first.time_mjd_seconds;
    let mut bucket = 0;
    for ((((row, data), flags), uvw), weights) in
        rows.iter().zip(data).zip(flags).zip(uvw).zip(weights)
    {
        let (
            Some(ArrayValue::Complex32(data)),
            Some(ArrayValue::Bool(flags)),
            Some(ArrayValue::Float64(uvw)),
            Some(ArrayValue::Float32(weights)),
        ) = (data, flags, uvw, weights)
        else {
            return Err(invalid(
                "EP requires defined DATA, FLAG, UVW and WEIGHT cells",
            ));
        };
        if data.shape() != [2, frequencies.len()]
            || data.shape() != flags.shape()
            || uvw.len() != 3
            || weights.len() != 2
        {
            return Err(invalid(
                "EP visibility shape disagrees with its spectral window",
            ));
        }
        let weight = (weights[0] + weights[1]) / 2.;
        if weight <= 0. || !weight.is_finite() {
            continue;
        }
        if row.time_mjd_seconds - epoch_start > preaverage_seconds {
            epoch_start = row.time_mjd_seconds;
            bucket += 1;
        }
        let angle = match angles.entry(row.time_mjd_seconds.to_bits()) {
            std::collections::btree_map::Entry::Occupied(slot) => *slot.get(),
            std::collections::btree_map::Entry::Vacant(slot) => *slot.insert(f64::from(
                geometry.feed_parallactic_angle(row.time_mjd_seconds, field, 0)? as f32,
            )),
        };
        let ([u, v, _], phase_m) = projection.apply([uvw[0], uvw[1], uvw[2]]);
        let hands = std::array::from_fn(|hand| {
            let mut result = fit::ObservedHand::default();
            for &channel in &channels {
                if flags[[hand, channel]] {
                    continue;
                }
                result.data += data[[hand, channel]];
                result.channels += 1;
                if matches[channel] < 0 {
                    continue;
                }
                let scale = frequencies[channel] / SPEED_OF_LIGHT;
                let pol = model.correlation_planes[hand] as usize - StokesType::RR as usize;
                result.samples.push(PointingSample::new(
                    model,
                    pol,
                    1 - hand,
                    [-u * scale, -v * scale],
                    angle,
                    std::f64::consts::TAU * phase_m * scale,
                ));
            }
            result
        });
        let antennas = [row.antenna1_id as usize, row.antenna2_id as usize];
        baselines
            .entry((bucket, antennas))
            .or_insert_with(|| Baseline {
                antennas,
                observations: Vec::new(),
            })
            .observations
            .push(Observation { weight, hands });
    }
    Ok(baselines.into_values().collect())
}
