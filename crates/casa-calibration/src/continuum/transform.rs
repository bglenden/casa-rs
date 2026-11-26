// SPDX-License-Identifier: LGPL-3.0-or-later
//! Frequency-domain `contsub` fits supplied to the ordinary MS transform.

use std::collections::{BTreeMap, HashMap};

use casa_core::{ArrayValue, Complex32};
use casa_ms::transform::{PreparedTransformInputView, TransformInputColumns, TransformInputView};
use casa_ms::{MeasurementSet, MsSelection, ResolvedMsSelectionRow};
use casa_tables::{RequiredScalarColumnValues, Table, TableError};
use ndarray::{Array2, ArrayD, Ix2, ShapeBuilder};

use super::fit::ComplexPolynomial;
use crate::session::resolve_casa_calibration_selection;

/// The input transformation of the historical named-output `ms.contsub`.
/// The MS transform applies subsequent selection, ID remapping and publication.
#[derive(Debug, Clone)]
pub struct LegacyContinuumTransform {
    /// Visibility column whose complex fitted spectrum is removed.
    pub column: &'static str,
    /// Degree of the fitted continuum polynomial.
    pub order: usize,
    /// Spectral windows and channels used to estimate the continuum.
    pub fit_spw: String,
    /// Union of the fit and output windows, plus the shared row selectors.
    pub union: MsSelection,
    /// Which axes to move after integration time in the grouping order.
    pub combine: String,
}

impl TransformInputView for LegacyContinuumTransform {
    fn order_output_rows(
        &self,
        input: &MeasurementSet,
        rows: &mut [ResolvedMsSelectionRow],
    ) -> Result<(), TableError> {
        let ordered: Vec<_> = grouped_rows(input.main_table(), rows, &self.combine)?
            .into_iter()
            .flatten()
            .map(|slot| rows[slot].clone())
            .collect();
        rows.clone_from_slice(&ordered);
        Ok(())
    }

    fn prepare<'a>(
        &self,
        input: &'a MeasurementSet,
        rows: &[ResolvedMsSelectionRow],
        _: &BTreeMap<i32, &[usize]>,
    ) -> Result<Box<dyn PreparedTransformInputView + 'a>, TableError> {
        let selected = resolve_casa_calibration_selection(input, &self.union)
            .map_err(error)?
            .selected_rows;
        let (grid, domain) = FitGrid::read(input, self, &selected)?;
        let groups = grouped_rows(input.main_table(), &selected, &self.combine)?;
        let output = rows
            .iter()
            .map(|row| row.row_index)
            .collect::<std::collections::HashSet<_>>();
        let mut model = HashMap::with_capacity(output.len());
        let mut estimates = Vec::new();
        for group in groups {
            if group
                .iter()
                .all(|&position| !output.contains(&selected[position].row_index))
            {
                continue;
            }
            let coefficients =
                fit_group(input.main_table(), self, &selected, &group, &grid, domain)?;
            let index = estimates.len();
            for &slot in &group {
                let row = &selected[slot];
                if output.contains(&row.row_index) {
                    model.insert(
                        row.row_index,
                        (index, row.spectral_window_id, baseline(row)),
                    );
                }
            }
            estimates.push(coefficients);
        }
        Ok(Box::new(Residuals {
            column: self.column,
            domain,
            grid,
            model,
            estimates,
        }))
    }

    fn additional_bytes_per_sample(&self) -> usize {
        2 * std::mem::size_of::<Complex32>()
    }

    fn provides(&self, _input: &MeasurementSet, _column: &str) -> bool {
        false
    }
}

struct Window {
    frequencies: Vec<f64>,
    channels: Vec<usize>,
}

struct FitGrid(BTreeMap<i32, Window>);

#[derive(Clone, Copy)]
struct FrequencyDomain {
    midpoint: f64,
    scale: f64,
}

impl FrequencyDomain {
    fn coordinate(self, frequency: f64) -> f64 {
        (frequency - self.midpoint) * self.scale
    }
}

impl FitGrid {
    fn read(
        input: &MeasurementSet,
        options: &LegacyContinuumTransform,
        selected: &[ResolvedMsSelectionRow],
    ) -> Result<(Self, FrequencyDomain), TableError> {
        let windows = input.spectral_window().map_err(error)?;
        let fit = input
            .resolve_spw_selector(if options.fit_spw.is_empty() {
                "*"
            } else {
                &options.fit_spw
            })
            .map_err(error)?;
        let mut grid = BTreeMap::<i32, Window>::new();
        for row in selected {
            if let std::collections::btree_map::Entry::Vacant(entry) =
                grid.entry(row.spectral_window_id)
            {
                entry.insert(Window {
                    frequencies: windows
                        .chan_freq(row.spectral_window_id as usize)
                        .map_err(error)?,
                    channels: Vec::new(),
                });
            }
        }
        for selection in fit.channel_list() {
            if let Some(window) = grid.get_mut(&(selection.spw_id as i32))
                && let Some(channels) = selection.channels()
            {
                window.channels.extend(channels);
            }
        }
        let mut bounds = [f64::INFINITY, f64::NEG_INFINITY];
        for window in grid
            .values_mut()
            .filter(|window| !window.channels.is_empty())
        {
            window.channels.sort_unstable();
            window.channels.dedup();
            for &frequency in &window.frequencies {
                bounds[0] = bounds[0].min(frequency);
                bounds[1] = bounds[1].max(frequency);
            }
        }
        if !bounds[0].is_finite() {
            return Err(error("The continuum fitter must first be initialized."));
        }
        let scale = if bounds[0] == bounds[1] {
            0.0
        } else {
            2.0 / (bounds[1] - bounds[0])
        };
        Ok((
            Self(grid),
            FrequencyDomain {
                scale,
                midpoint: 0.5 * (bounds[0] + bounds[1]),
            },
        ))
    }
}

type Baseline = (i32, i32);
type Coefficients = HashMap<Baseline, Vec<ComplexPolynomial>>;
type Samples = BTreeMap<Baseline, Vec<Vec<(f64, Complex32, f64)>>>;
type AveragingKey = (Baseline, i32, i32);

fn baseline(row: &ResolvedMsSelectionRow) -> Baseline {
    (row.antenna1_id, row.antenna2_id)
}

fn grouped_rows(
    table: &Table,
    rows: &[ResolvedMsSelectionRow],
    combine: &str,
) -> Result<Vec<Vec<usize>>, TableError> {
    let ids = rows.iter().map(|row| row.row_index).collect::<Vec<_>>();
    let columns = ["ARRAY_ID", "SCAN_NUMBER", "STATE_ID", "OBSERVATION_ID"];
    let mut metadata = table.required_scalar_columns_owned_for_rows(&columns, &ids)?;
    let mut ints = |name| match metadata.remove(name) {
        Some(RequiredScalarColumnValues::Int32(ids)) => Ok(ids),
        _ => Err(error(format!("{name} must contain integer cells"))),
    };
    let arrays = ints("ARRAY_ID")?;
    let scans = ints("SCAN_NUMBER")?;
    let states = ints("STATE_ID")?;
    let observations = ints("OBSERVATION_ID")?;
    let includes = ["scan", "state", "spw", "obs"].map(|axis| !combine.contains(axis));
    let mut order = (0..rows.len()).collect::<Vec<_>>();
    let key = |position: usize| {
        let row = &rows[position];
        let scan = scans[position];
        let state = states[position];
        let observation = observations[position];
        let ddid = row.data_desc_id;
        let watch = |axis: usize, id| if includes[axis] { id } else { 0 };
        let after = |axis: usize, id| if includes[axis] { 0 } else { id };
        (
            [
                arrays[position],
                watch(0, scan),
                watch(1, state),
                row.field_id,
                watch(2, ddid),
            ],
            row.time_mjd_seconds,
            [
                watch(3, observation),
                after(0, scan),
                after(1, state),
                after(2, ddid),
                after(3, observation),
            ],
            row.row_index,
        )
    };
    order.sort_unstable_by(|&a, &b| {
        let a = key(a);
        let b = key(b);
        a.0.cmp(&b.0)
            .then_with(|| a.1.total_cmp(&b.1))
            .then_with(|| a.2.cmp(&b.2))
            .then_with(|| a.3.cmp(&b.3))
    });
    Ok(order
        .chunk_by(|&a, &b| rows[a].time_mjd_seconds == rows[b].time_mjd_seconds)
        .map(<[usize]>::to_vec)
        .collect())
}

fn fit_group(
    table: &Table,
    options: &LegacyContinuumTransform,
    rows: &[ResolvedMsSelectionRow],
    slots: &[usize],
    fit: &FitGrid,
    domain: FrequencyDomain,
) -> Result<Coefficients, TableError> {
    let windows = slots
        .iter()
        .map(|&slot| &rows[slot])
        .filter(|row| !fit.0[&row.spectral_window_id].channels.is_empty())
        .collect::<Vec<_>>();
    if windows.is_empty() {
        return Err(error("The continuum fitter must first be initialized."));
    }
    let fitting = windows
        .into_iter()
        .filter(|row| !row.flag_row)
        .collect::<Vec<_>>();
    if fitting.is_empty() {
        return Ok(HashMap::new());
    }
    let ids = fitting.iter().map(|row| row.row_index).collect::<Vec<_>>();
    let data = array_cells(table, options.column, &ids)?;
    let flags = array_cells(table, "FLAG", &ids)?;
    let weights = array_cells(table, "WEIGHT", &ids)?;
    let mut averages = BTreeMap::new();
    for (((row, data), flags), weight) in fitting.into_iter().zip(data).zip(flags).zip(weights) {
        let key = (baseline(row), row.spectral_window_id, row.field_id);
        accumulate_row(
            &mut averages,
            key,
            &fit.0[&row.spectral_window_id],
            (data, flags, weight),
        )?;
    }
    let mut group = Samples::new();
    for ((baseline, window, _), average) in averages {
        average.add_to_fit(&mut group, baseline, &fit.0[&window], domain)?;
    }
    let terms = options.order.saturating_add(1);
    Ok(group
        .into_iter()
        .map(|(baseline, samples)| {
            let fits = samples
                .into_iter()
                .map(|hand| ComplexPolynomial::fit(hand.into_iter(), terms))
                .collect();
            (baseline, fits)
        })
        .collect())
}

type FitCells = (Option<ArrayValue>, Option<ArrayValue>, Option<ArrayValue>);

fn accumulate_row(
    groups: &mut BTreeMap<AveragingKey, WindowAverage>,
    key: AveragingKey,
    window: &Window,
    (data, flags, weight): FitCells,
) -> Result<(), TableError> {
    let (
        Some(ArrayValue::Complex32(data)),
        Some(ArrayValue::Bool(flags)),
        Some(ArrayValue::Float32(weight)),
    ) = (data, flags, weight)
    else {
        return Err(error(
            "continuum fitting requires complex data, FLAG and WEIGHT",
        ));
    };
    let data = data.into_dimensionality::<Ix2>().map_err(error)?;
    let flags = flags.into_dimensionality::<Ix2>().map_err(error)?;
    if data.shape() != flags.shape()
        || data.nrows() != weight.len()
        || data.ncols() != window.frequencies.len()
        || weight.is_empty()
    {
        return Err(error(
            "continuum fitting received a different channel or correlation shape",
        ));
    }
    let average = groups
        .entry(key)
        .or_insert_with(|| WindowAverage::new(data.dim()));
    if average.sum.dim() != data.dim() {
        return Err(error(
            "continuum fitting received different shapes for an averaged baseline",
        ));
    }
    let mut good = false;
    for (channel, flags) in flags.columns().into_iter().enumerate() {
        if flags.iter().all(|&flag| !flag) {
            good = true;
            average.unflagged[channel] = true;
            for (hand, (value, weight)) in
                data.column(channel).iter().zip(weight.iter()).enumerate()
            {
                average.sum[[hand, channel]] += *value * *weight;
            }
        }
    }
    if good {
        for (sum, &weight) in average.weights.iter_mut().zip(weight.iter()) {
            *sum += weight;
        }
    }
    Ok(())
}

// The old VisBuffAccumulator maintains a weight per hand of a baseline,
// not per channel. Any unflagged channel contributes the row's weight to
// that baseline before the continuum channel mask is applied.
struct WindowAverage {
    sum: Array2<Complex32>,
    weights: Vec<f32>,
    unflagged: Vec<bool>,
}

impl WindowAverage {
    fn new((hands, channels): (usize, usize)) -> Self {
        Self {
            sum: Array2::zeros((hands, channels).f()),
            weights: vec![0.0; hands],
            unflagged: vec![false; channels],
        }
    }

    fn add_to_fit(
        self,
        group: &mut Samples,
        baseline: Baseline,
        window: &Window,
        domain: FrequencyDomain,
    ) -> Result<(), TableError> {
        let row_weight = (self.weights[0] + self.weights[self.weights.len() - 1]) * 0.5;
        if row_weight <= 0.0 {
            return Ok(());
        }
        let samples = group
            .entry(baseline)
            .or_insert_with(|| vec![Vec::new(); self.weights.len()]);
        if samples.len() != self.weights.len() {
            return Err(error(
                "continuum fitting cannot combine unequal correlation counts",
            ));
        }
        for &channel in &window.channels {
            if !self.unflagged[channel] {
                continue;
            }
            let frequency = domain.coordinate(window.frequencies[channel]);
            for (hand, &weight) in self.weights.iter().enumerate() {
                let observed = if weight > 0.0 {
                    self.sum[[hand, channel]] / weight
                } else {
                    Complex32::ZERO
                };
                let weight = if (1e-20..=1e20).contains(&weight) {
                    f64::from(weight)
                } else {
                    1.0
                };
                samples[hand].push((frequency, observed, weight / self.sum.ncols() as f64));
            }
        }
        Ok(())
    }
}

struct Residuals {
    column: &'static str,
    domain: FrequencyDomain,
    grid: FitGrid,
    model: HashMap<usize, (usize, i32, Baseline)>,
    estimates: Vec<Coefficients>,
}

impl PreparedTransformInputView for Residuals {
    fn read(
        &self,
        input: &Table,
        rows: &[usize],
        columns: &[&str],
    ) -> Result<TransformInputColumns, TableError> {
        let mut output = BTreeMap::new();
        if columns.contains(&self.column) {
            let mut cells = array_cells(input, self.column, rows)?;
            for (&row, cell) in rows.iter().zip(&mut cells) {
                if let Some(ArrayValue::Complex32(data)) = cell {
                    self.subtract(row, data)?;
                }
            }
            output.insert(self.column, cells);
        }
        if columns.contains(&"FLAG") {
            let mut cells = array_cells(input, "FLAG", rows)?;
            for (&row, cell) in rows.iter().zip(&mut cells) {
                if let Some(ArrayValue::Bool(flags)) = cell {
                    self.flag_missing_fit(row, flags)?;
                }
            }
            output.insert("FLAG", cells);
        }
        if self.column != "DATA" && columns.contains(&"SIGMA") {
            let mut sigmas = array_cells(input, "WEIGHT", rows)?;
            for sigma in &mut sigmas {
                let Some(ArrayValue::Float32(weight)) = sigma else {
                    return Err(error("continuum fitting requires float32 WEIGHT"));
                };
                weight.mapv_inplace(|weight| {
                    if weight > 0.0 {
                        f64::from(weight).sqrt().recip() as f32
                    } else {
                        f32::MAX
                    }
                });
            }
            output.insert("SIGMA", sigmas);
        }
        Ok(output)
    }
}

impl Residuals {
    fn flag_missing_fit(&self, row: usize, flags: &mut ArrayD<bool>) -> Result<(), TableError> {
        let Some(&(group, _, baseline)) = self.model.get(&row) else {
            return Ok(());
        };
        let Some(coefficients) = self.estimates[group].get(&baseline) else {
            flags.fill(true);
            return Ok(());
        };
        let mut flags = flags
            .view_mut()
            .into_dimensionality::<Ix2>()
            .map_err(error)?;
        if flags.nrows() != coefficients.len() {
            return Err(error("continuum fit and output flag axes disagree"));
        }
        for (mut hand, fit) in flags.rows_mut().into_iter().zip(coefficients) {
            if !fit.is_defined() {
                hand.fill(true);
            }
        }
        Ok(())
    }

    fn subtract(&self, row: usize, data: &mut ArrayD<Complex32>) -> Result<(), TableError> {
        let Some(&(group, window, baseline)) = self.model.get(&row) else {
            return Ok(());
        };
        let Some(coefficients) = self.estimates[group].get(&baseline) else {
            return Ok(());
        };
        let mut data = data
            .view_mut()
            .into_dimensionality::<Ix2>()
            .map_err(error)?;
        let frequencies = &self.grid.0[&window].frequencies;
        if data.nrows() != coefficients.len() || data.ncols() != frequencies.len() {
            return Err(error("continuum fit and output visibility axes disagree"));
        }
        for (hand, fit) in coefficients.iter().enumerate() {
            for (channel, &frequency) in frequencies.iter().enumerate() {
                data[[hand, channel]] -= fit.at(self.domain.coordinate(frequency));
            }
        }
        Ok(())
    }
}

fn array_cells(
    table: &Table,
    name: &str,
    rows: &[usize],
) -> Result<Vec<Option<ArrayValue>>, TableError> {
    table
        .column_accessor(name)?
        .array_cells_owned_uncached(rows)
}

fn error(error: impl std::fmt::Display) -> TableError {
    TableError::Storage(error.to_string())
}
