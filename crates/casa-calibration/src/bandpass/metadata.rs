// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selected B-table channelization, including CASA's persisted width defects.
use super::{BandpassFrequencyInterval, BandpassSolveError};
use crate::solve::grouping::SelectedSolveRow;
use casa_ms::{ChannelSelection, ms::MeasurementSet};
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, ScalarValue, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

pub(crate) fn bin_widths(
    ms: &MeasurementSet,
    rows: &[SelectedSolveRow],
    interval: Option<BandpassFrequencyInterval>,
) -> Result<BTreeMap<i32, usize>, BandpassSolveError> {
    match interval {
        Some(BandpassFrequencyInterval::Channels(count)) if count > 1.0 => Ok(rows
            .iter()
            .map(|row| (row.data_spw_id, count as usize))
            .collect()),
        Some(BandpassFrequencyInterval::Hertz(hertz)) => bandwidth_bin_widths(ms, hertz),
        _ => Ok(BTreeMap::new()),
    }
}

fn bandwidth_bin_widths(
    ms: &MeasurementSet,
    hertz: f64,
) -> Result<BTreeMap<i32, usize>, BandpassSolveError> {
    let fail = |reason: String| BandpassSolveError::UnsupportedConfiguration { reason };
    let window = ms
        .spectral_window()
        .map_err(|error| fail(error.to_string()))?;
    let mut widths = BTreeMap::new();
    for spw in 0..window.row_count() {
        let channels = window
            .chan_width(spw)
            .map_err(|error| fail(error.to_string()))?;
        let width = channels
            .first()
            .filter(|width| width.is_finite() && **width != 0.0)
            .ok_or_else(|| fail(format!("SPW {spw} has no valid first channel width")))?;
        widths.insert(spw as i32, (hertz / width.abs()).floor().max(1.0) as usize);
    }
    // Once the layer is installed, even windows with a width of one use
    // the frequency averager's scalar weights for the time centroid.
    if widths.values().all(|&width| width == 1) {
        widths.clear();
    }
    Ok(widths)
}

pub(crate) fn write(
    path: &Path,
    solution_windows: &BTreeSet<i32>,
    channel_bin_widths: &BTreeMap<i32, usize>,
    selected_rows: &[SelectedSolveRow],
    combine_windows: bool,
) -> Result<(), BandpassSolveError> {
    let mut table = Table::open(TableOptions::new(path)).map_err(metadata_error)?;
    let selections = selected_rows
        .iter()
        .map(|row| (row.data_spw_id, row.channel_selection.as_ref()))
        .collect::<BTreeMap<_, _>>();
    let grid = |table: &Table, spw| {
        select_grid(
            table,
            spw as usize,
            selections.get(&spw).copied().flatten(),
            channel_bin_widths.get(&spw).copied().unwrap_or(1),
        )
    };
    let aggregate = if combine_windows && selections.len() > 1 {
        Some(combine_grids(selections.keys().copied(), |spw| {
            grid(&table, spw)
        })?)
    } else {
        None
    };
    for row in 0..table.row_count() {
        let spw = row as i32;
        let present = solution_windows.contains(&spw);
        table
            .cell_accessor_mut(row, "FLAG_ROW")
            .and_then(|mut c| c.set(Value::Scalar(ScalarValue::Bool(!present))))
            .map_err(metadata_error)?;
        if let Some((reference, combined)) = &aggregate {
            if *reference == spw {
                combined.write(&mut table, row)?;
            }
        } else if present {
            grid(&table, spw)?.write(&mut table, row)?;
        }
    }
    table.save(TableOptions::new(path)).map_err(metadata_error)
}

#[derive(Clone, Copy)]
pub(crate) enum GatherFrequencyLayout {
    Collapsed,
    WidthWeighted,
    Channelized(Option<BandpassFrequencyInterval>),
}

/// Check the frequency grid used by the gather and any append destination.
pub(crate) fn check_gather_frequency(
    ms: &MeasurementSet,
    destination: Option<&Path>,
    rows: &[SelectedSolveRow],
    layout: GatherFrequencyLayout,
    combine_windows: bool,
) -> Result<(), BandpassSolveError> {
    let existing = destination
        .map(|path| Table::open(TableOptions::new(path.join("SPECTRAL_WINDOW"))))
        .transpose()
        .map_err(metadata_error)?;
    let source = ms
        .subtable(casa_ms::schema::SubtableId::SpectralWindow)
        .ok_or_else(|| metadata_error("missing MeasurementSet SPECTRAL_WINDOW"))?;
    let selections = rows
        .iter()
        .map(|row| (row.data_spw_id, row.channel_selection.as_ref()))
        .collect::<BTreeMap<_, _>>();
    let widths = match layout {
        GatherFrequencyLayout::Channelized(interval) => bin_widths(ms, rows, interval)?,
        _ => BTreeMap::new(),
    };
    let grid = |spw| {
        let width = match layout {
            GatherFrequencyLayout::Collapsed => usize::MAX,
            GatherFrequencyLayout::WidthWeighted => 1,
            GatherFrequencyLayout::Channelized(_) => widths.get(&spw).copied().unwrap_or(1),
        };
        let grid = select_grid(source, spw as usize, selections[&spw], width)?;
        if matches!(layout, GatherFrequencyLayout::WidthWeighted) {
            grid.collapse_weighted()
        } else {
            Ok(grid)
        }
    };
    let compare = |spw: i32, grid: ChannelGrid| -> Result<(), BandpassSolveError> {
        let Some(existing) = &existing else {
            return Ok(());
        };
        let row = spw as usize;
        let scalar = |column| {
            existing
                .cell_accessor(row, column)
                .and_then(|cell| cell.scalar())
                .map_err(metadata_error)
        };
        if matches!(scalar("FLAG_ROW")?, ScalarValue::Bool(true)) {
            return Ok(());
        }
        let count = scalar("NUM_CHAN")?;
        let frequencies = existing
            .cell_accessor(row, "CHAN_FREQ")
            .and_then(|cell| cell.array())
            .map_err(metadata_error)?;
        let matches_grid = matches!(count, ScalarValue::Int32(n) if *n as usize == grid.frequencies.len())
            && matches!(frequencies, ArrayValue::Float64(values) if values.iter().copied().eq(grid.frequencies));
        if matches_grid {
            Ok(())
        } else {
            Err(metadata_error(format!(
                "frequency metadata differs for appended SPW {spw}"
            )))
        }
    };
    if combine_windows && selections.len() > 1 {
        let (spw, combined) = combine_grids(selections.keys().copied(), grid)?;
        compare(spw, combined)
    } else {
        for spw in selections.keys().copied() {
            compare(spw, grid(spw)?)?;
        }
        Ok(())
    }
}

fn combine_grids(
    mut selected: impl Iterator<Item = i32>,
    grid: impl Fn(i32) -> Result<ChannelGrid, BandpassSolveError>,
) -> Result<(i32, ChannelGrid), BandpassSolveError> {
    let reference = selected.next().expect("nonempty windows");
    let mut combined = grid(reference)?;
    let frequency_offset = combined.frequencies[0];
    let width_scale = combined.widths[0];
    for index in 0..combined.frequencies.len() {
        let width = combined.widths[index] / width_scale;
        combined.frequencies[index] = (combined.frequencies[index] - frequency_offset) * width;
        combined.effective_bandwidths[index] = width;
        combined.widths[index] = width * width;
    }
    for spw in selected {
        combined.accumulate(&grid(spw)?, frequency_offset, width_scale)?;
    }
    for index in 0..combined.frequencies.len() {
        let bandwidth = combined.effective_bandwidths[index];
        if !bandwidth.is_finite() || bandwidth <= 0.0 {
            return Err(metadata_error(
                "Cannot combine windows with invalid channel widths",
            ));
        }
        combined.frequencies[index] = combined.frequencies[index] / bandwidth + frequency_offset;
        combined.widths[index] = combined.widths[index] / bandwidth * width_scale;
        combined.effective_bandwidths[index] *= width_scale;
    }
    Ok((reference, combined))
}

struct ChannelGrid {
    frequencies: Vec<f64>,
    widths: Vec<f64>,
    effective_bandwidths: Vec<f64>,
}

fn select_grid(
    table: &Table,
    row: usize,
    selection: Option<&ChannelSelection>,
    bin_width: usize,
) -> Result<ChannelGrid, BandpassSolveError> {
    let array = |column| -> Result<Vec<f64>, BandpassSolveError> {
        let ArrayValue::Float64(values) = table
            .cell_accessor(row, column)
            .and_then(|c| c.array())
            .map_err(metadata_error)?
        else {
            return Err(metadata_error(format!("SPW {row} {column} is not Float64")));
        };
        Ok(values.iter().copied().collect())
    };
    let frequencies = array("CHAN_FREQ")?;
    let widths = array("CHAN_WIDTH")?;
    if frequencies.len() != widths.len() {
        return Err(metadata_error("SPW channel width shape mismatch"));
    }
    let channels = crate::solve::grouping::selected_channels(selection, frequencies.len())?;
    let bins = channels.chunks(bin_width);
    let frequencies = average_frequencies(&frequencies, &channels, bin_width);
    let widths = bins
        .map(|bin| bin.iter().map(|&channel| widths[channel].abs()).sum())
        .collect::<Vec<_>>();
    Ok(ChannelGrid {
        frequencies,
        effective_bandwidths: widths.clone(),
        widths,
    })
}

pub(super) fn average_frequencies(
    frequencies: &[f64],
    channels: &[usize],
    bin_width: usize,
) -> Vec<f64> {
    channels
        .chunks(bin_width)
        .map(|bin| bin.iter().map(|&channel| frequencies[channel]).sum::<f64>() / bin.len() as f64)
        .collect()
}

impl ChannelGrid {
    fn collapse_weighted(self) -> Result<Self, BandpassSolveError> {
        let width = self.widths.iter().sum::<f64>();
        let first = self
            .frequencies
            .first()
            .copied()
            .ok_or_else(|| metadata_error("empty frequency grid"))?;
        if !width.is_finite() || width <= 0.0 {
            return Err(metadata_error("invalid total channel width"));
        }
        let offset = self
            .frequencies
            .iter()
            .zip(&self.widths)
            .map(|(frequency, width)| (frequency - first) * width)
            .sum::<f64>();
        Ok(Self {
            frequencies: vec![first + offset / width],
            widths: vec![width],
            effective_bandwidths: vec![width],
        })
    }

    fn accumulate(
        &mut self,
        other: &Self,
        frequency_offset: f64,
        width_scale: f64,
    ) -> Result<(), BandpassSolveError> {
        if self.frequencies.len() != other.frequencies.len() {
            return Err(metadata_error(
                "Cannot combine spectral windows with different channel counts",
            ));
        }
        for index in 0..self.frequencies.len() {
            let width = other.widths[index] / width_scale;
            self.frequencies[index] += (other.frequencies[index] - frequency_offset) * width;
            self.widths[index] += width * width;
            self.effective_bandwidths[index] += width;
        }
        Ok(())
    }

    fn write(&self, table: &mut Table, row: usize) -> Result<(), BandpassSolveError> {
        let count = self.frequencies.len();
        let total: f64 = self.frequencies.iter().sum();
        // SolvableVisCal::setCTFrequencies aliases resolution to frequency
        // and sums frequencies as bandwidth. CASA requires exact agreement
        // when appending; these fields are not used to apply the solutions.
        for (column, values) in [
            ("CHAN_FREQ", &self.frequencies),
            ("RESOLUTION", &self.frequencies),
            ("CHAN_WIDTH", &self.widths),
            ("EFFECTIVE_BW", &self.effective_bandwidths),
        ] {
            table
                .cell_accessor_mut(row, column)
                .and_then(|mut c| c.set(Value::Array(ArrayValue::from_f64_vec(values.clone()))))
                .map_err(metadata_error)?;
        }
        for (column, value) in [
            ("NUM_CHAN", Value::Scalar(ScalarValue::Int32(count as i32))),
            (
                "TOTAL_BANDWIDTH",
                Value::Scalar(ScalarValue::Float64(total)),
            ),
        ] {
            table
                .cell_accessor_mut(row, column)
                .and_then(|mut c| c.set(value))
                .map_err(metadata_error)?;
        }
        Ok(())
    }
}

fn metadata_error(error: impl std::fmt::Display) -> BandpassSolveError {
    BandpassSolveError::UnsupportedConfiguration {
        reason: error.to_string(),
    }
}
