// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read each legacy visibility chunk once and fit a pointing solution.

use std::collections::BTreeMap;
use std::sync::Arc;

use casa_coordinates::{Coordinate, CoordinateModel, StokesType};
use casa_imaging_model::GridFtProjection;
use casa_imaging_runtime::HostResources;
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::{MeasurementSet, MsResult, ResolvedMsSelection, ResolvedMsSelectionRow};
use casa_tables::RequiredScalarColumnValues;
use casa_types::measures::direction::MDirection;
use casa_types::{ArrayValue, Complex32};
use ndarray::ArrayD;

use super::degrid::PointingSample;
use super::fit::{self, Baseline, Observation};
use super::{
    EpCacheProducts, FirstBuffer, LegacyEpSolver, PreparedEpModel, convolution, invalid, table,
};

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

struct LoadedChunk {
    baselines: Vec<Baseline>,
    reference_time: f64,
}

struct ConvolutionSelection<'a> {
    solver: &'a LegacyEpSolver,
    model: &'a PreparedEpModel,
    selected_angle: f64,
    selected: Arc<EpCacheProducts>,
    host: Option<HostResources>,
}

impl ConvolutionSelection<'_> {
    fn at(&mut self, angle: f64) -> MsResult<&Arc<EpCacheProducts>> {
        if angle != self.selected_angle
            && (angle - self.selected_angle).abs().to_degrees() > self.solver.angle_increment_deg
        {
            if self.host.is_none() {
                self.host = Some(HostResources::detect().map_err(invalid)?);
            }
            let buffer = FirstBuffer {
                correlations: self.model.correlation_planes.clone(),
                frequency_hz: self.model.reference_frequency_hz,
                angle_rad: angle,
            };
            let products = convolution(
                &self.solver.cache,
                (self.solver.angle_increment_deg, 1),
                self.host.as_ref().expect("host resources detected"),
                &buffer,
                [self.model.side, self.model.side, 4, 1],
                self.model.cell_rad,
                &self.model.coordinates,
            )?;
            self.selected = Arc::new(products);
            self.selected_angle = angle;
        }
        Ok(&self.selected)
    }
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
        let engine = MsCalEngine::new(ms)?;
        let antenna_count = ms.antenna()?.row_count();
        let mut rows = Rows::new(ms, selected, self.interval_seconds)?;
        let mut results = Vec::new();
        let mut kernels = ConvolutionSelection {
            solver: self,
            model,
            selected_angle: model.initial_angle_rad,
            selected: Arc::clone(&model.products),
            host: None,
        };
        while let Some(chunk) = rows.next() {
            let first = chunk[0].0;
            let input = crate::timing::Phase::start("ep_gather");
            let Some(LoadedChunk {
                baselines,
                reference_time,
            }) = load_chunk(ms, selected, &engine, chunk, &mut kernels)?
            else {
                continue;
            };
            if baselines
                .iter()
                .any(|baseline| baseline.antennas.iter().any(|&id| id >= antenna_count))
            {
                return Err(invalid("EP visibility references an unknown antenna"));
            }
            drop(input);
            let _fit = crate::timing::Phase::start("ep_fit");
            // EPJones::solveRPar aliases pointPar_, which guessPar zeros
            // at the beginning of every independent solution interval.
            let mut offsets = vec![[0.; 4]; antenna_count];
            if fit::solve(model, &baselines, &mut offsets) {
                results.push(Slot {
                    spw: first.spectral_window_id,
                    field: first.field_id,
                    time: reference_time,
                    offsets,
                });
            }
        }
        if results.is_empty() {
            return Err(invalid("No valid calibration solutions; no table written."));
        }
        let _write = crate::timing::Phase::start("ep_write");
        table::write(ms, self, &results)?;
        Ok(results.len() * antenna_count)
    }
}

fn load_chunk(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    geometry: &MsCalEngine,
    chunk: &[(&ResolvedMsSelectionRow, [i32; 4])],
    kernels: &mut ConvolutionSelection<'_>,
) -> MsResult<Option<LoadedChunk>> {
    let first = chunk[0].0;
    let rows = chunk
        .iter()
        .map(|(row, _)| *row)
        .filter(|row| !row.flag_row)
        .collect::<Vec<_>>();
    if rows.is_empty() {
        return Ok(None);
    }
    let model = kernels.model;
    let spectral = Window::prepare(ms, selected, model, first)?;
    let hand_count = spectral.input_hands.len();
    let field = first.field_id as usize;
    let projection = projection(geometry, model, field)?;
    let preaverage_seconds = kernels.solver.preaverage_seconds;
    let mut accumulated = BTreeMap::<_, Baseline>::new();
    let mut angles = BTreeMap::new();
    let mut epoch_start = first.time_mjd_seconds;
    let mut bucket = 0;
    let mut weighted_time = 0.;
    let mut time_weight = 0.;
    for block in rows.chunks(256) {
        let inputs = visibility_cells(ms, block)?;
        for (row, cells) in block.iter().zip(inputs) {
            let visibility = Visibility::decode(cells, hand_count, spectral.frequencies.len())?;
            let weight = visibility.weight();
            if weight <= 0. || !weight.is_finite() {
                continue;
            }
            // syncSolveMeta uses flagRow and the weighted clock of the first VB.
            if row.time_mjd_seconds == first.time_mjd_seconds {
                weighted_time += row.time_mjd_seconds * f64::from(weight);
                time_weight += f64::from(weight);
            }
            if row.antenna1_id == row.antenna2_id {
                continue;
            }
            if row.time_mjd_seconds - epoch_start >= preaverage_seconds
                && row.time_mjd_seconds != epoch_start
            {
                epoch_start = row.time_mjd_seconds;
                bucket += 1;
            }
            let angle = match angles.entry(row.time_mjd_seconds.to_bits()) {
                std::collections::btree_map::Entry::Occupied(slot) => *slot.get(),
                std::collections::btree_map::Entry::Vacant(slot) => *slot.insert(f64::from(
                    geometry.feed_parallactic_angle(row.time_mjd_seconds, field, 0)? as f32,
                )),
            };
            let kernels = kernels.at(angle)?;
            let channels = spectral
                .selected
                .iter()
                .copied()
                .filter(|&channel| (0..hand_count).all(|hand| !visibility.flags[[hand, channel]]))
                .collect::<Vec<_>>();
            if channels.is_empty() {
                continue;
            }
            let antennas = [row.antenna1_id as usize, row.antenna2_id as usize];
            let baseline = accumulated
                .entry((bucket, antennas))
                .or_insert_with(|| Baseline {
                    antennas,
                    observations: Vec::new(),
                    good_channels: Default::default(),
                });
            baseline.observations.push(spectral.observe(
                model,
                kernels,
                &visibility,
                &channels,
                &projection,
                angle,
            ));
            baseline.good_channels.extend(channels);
        }
    }
    Ok((time_weight > 0.).then(|| LoadedChunk {
        reference_time: weighted_time / time_weight,
        baselines: accumulated.into_values().collect(),
    }))
}

fn projection(
    geometry: &MsCalEngine,
    model: &PreparedEpModel,
    field: usize,
) -> MsResult<GridFtProjection> {
    let CoordinateModel::Direction(direction) = model.coordinates.coordinate(0) else {
        return Err(invalid("EP model is missing its direction coordinate"));
    };
    let centre = direction.reference_value();
    let target = MDirection::from_angles(centre[0], centre[1], direction.direction_ref());
    let source = geometry.field_direction_j2000(field)?;
    Ok(GridFtProjection::between_directions(source, &target))
}

struct Window {
    frequencies: Vec<f64>,
    selected: Vec<usize>,
    matches: Vec<i32>,
    input_hands: Vec<usize>,
}

impl Window {
    fn prepare(
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
        model: &PreparedEpModel,
        first: &ResolvedMsSelectionRow,
    ) -> MsResult<Self> {
        let spw = first.spectral_window_id;
        let frequencies = ms.spectral_window()?.chan_freq(spw as usize)?;
        let selected = selected.channel_selections.get(&spw).map_or_else(
            || Ok((0..frequencies.len()).collect()),
            |selection| selection.indices(frequencies.len()),
        )?;
        let CoordinateModel::Spectral(axis) = model.coordinates.coordinate(2) else {
            return Err(invalid("EP model is missing its spectral coordinate"));
        };
        let matches = crate::ft::image::match_channels(axis, 1, &frequencies);
        let input_hands = input_hand_indices(ms, first.polarization_id, &model.correlation_planes)?;
        Ok(Self {
            frequencies,
            selected,
            matches,
            input_hands,
        })
    }

    fn observe(
        &self,
        model: &PreparedEpModel,
        products: &Arc<EpCacheProducts>,
        visibility: &Visibility,
        channels: &[usize],
        projection: &GridFtProjection,
        angle: f64,
    ) -> Observation {
        let ([u, v, _], phase_m) = projection.apply(visibility.uvw);
        let hands = std::array::from_fn(|hand| {
            let Some(&input_hand) = self.input_hands.get(hand) else {
                return fit::ObservedHand::default();
            };
            let mut result = fit::ObservedHand {
                weight: visibility.weights[input_hand],
                ..Default::default()
            };
            for &channel in channels {
                result.data += visibility.data[[input_hand, channel]];
                // A single circular hand has no conjugate kernel to sample.
                if self.input_hands.len() == 1 || self.matches[channel] < 0 {
                    continue;
                }
                let scale = self.frequencies[channel] / SPEED_OF_LIGHT;
                let pol = model.correlation_planes[hand] as usize - StokesType::RR as usize;
                result.samples.extend(PointingSample::new(
                    model,
                    products,
                    (pol, 1 - hand),
                    [-u * scale, -v * scale],
                    angle,
                    std::f64::consts::TAU * phase_m * scale,
                ));
            }
            result
        });
        Observation {
            weight: visibility.weight(),
            hands,
        }
    }
}

struct Visibility {
    data: ArrayD<Complex32>,
    flags: ArrayD<bool>,
    uvw: [f64; 3],
    weights: ArrayD<f32>,
}

impl Visibility {
    fn decode(cells: [Option<ArrayValue>; 4], hands: usize, channels: usize) -> MsResult<Self> {
        let [
            Some(ArrayValue::Complex32(data)),
            Some(ArrayValue::Bool(flags)),
            Some(ArrayValue::Float64(uvw)),
            Some(ArrayValue::Float32(weights)),
        ] = cells
        else {
            return Err(invalid(
                "EP requires defined DATA, FLAG, UVW and WEIGHT cells",
            ));
        };
        if data.shape() != [hands, channels]
            || data.shape() != flags.shape()
            || uvw.shape() != [3]
            || weights.shape() != [hands]
        {
            return Err(invalid(
                "EP visibility shape disagrees with its spectral window",
            ));
        }
        Ok(Self {
            data,
            flags,
            uvw: [uvw[0], uvw[1], uvw[2]],
            weights,
        })
    }

    fn weight(&self) -> f32 {
        self.weights.iter().copied().sum::<f32>() / self.weights.len() as f32
    }
}

fn visibility_cells(
    ms: &MeasurementSet,
    rows: &[&ResolvedMsSelectionRow],
) -> MsResult<impl Iterator<Item = [Option<ArrayValue>; 4]> + use<>> {
    let indices = rows.iter().map(|row| row.row_index).collect::<Vec<_>>();
    let read = |column| {
        ms.main_table()
            .column_accessor(column)?
            .array_cells_owned_uncached(&indices)
    };
    Ok(read("DATA")?
        .into_iter()
        .zip(read("FLAG")?)
        .zip(read("UVW")?)
        .zip(read("WEIGHT")?)
        .map(|(((data, flags), uvw), weights)| [data, flags, uvw, weights]))
}

fn input_hand_indices(
    ms: &MeasurementSet,
    polarization: i32,
    model: &[StokesType],
) -> MsResult<Vec<usize>> {
    let correlations = ms.polarization()?.corr_type(polarization as usize)?;
    let incompatible =
        || invalid("EP selected a different set of circular hands from the image model");
    if correlations.len() != model.len() {
        return Err(incompatible());
    }
    model
        .iter()
        .map(|hand| {
            correlations
                .iter()
                .position(|&code| code == *hand as i32)
                .ok_or_else(incompatible)
        })
        .collect()
}
