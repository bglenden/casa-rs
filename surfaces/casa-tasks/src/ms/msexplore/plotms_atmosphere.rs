// SPDX-License-Identifier: LGPL-3.0-or-later
//! Atmospheric overlays on visibility plots, with the original point metadata.

use std::collections::BTreeMap;

use crate::TaskFailure;

use super::calibration::{
    atmosphere::{Chunk, Context, Options},
    axis::Axis,
};
use super::plotms_text::Metadata;
use casa_ms::{MeasurementSet, MsSelection};
use casa_plot_data::msexplore::{
    MsAxis, MsColorAxis, MsPlotPayload, MsPlotSpec, MsScatterPlotMsMetadata, MsScatterPointRef,
    MsScatterSeries, plot_selected_channel_bins,
};

pub(super) struct Curve {
    pub(super) axis: Axis,
    pub(super) samples: Vec<CurveSample>,
}

pub(super) struct CurveSample {
    pub(super) x: f64,
    pub(super) y: f64,
    pub(super) point: MsScatterPointRef,
    pub(super) display: MsScatterPlotMsMetadata,
    pub(super) frequency: f64,
    pub(super) color_group: String,
}

pub(super) fn build(
    ms: &MeasurementSet,
    selection: &MsSelection,
    payload: &MsPlotPayload,
    options: Options,
    spec: &MsPlotSpec,
) -> Result<Vec<Vec<Curve>>, TaskFailure> {
    let context = Context::open_ms(ms, selection, options.image).map_err(TaskFailure::execution)?;
    let grids = grids(ms, selection, spec).map_err(TaskFailure::execution)?;
    let mut metadata =
        Metadata::new(ms).map_err(|error| TaskFailure::execution(error.to_string()))?;
    let (x_axis, panels) = match payload {
        MsPlotPayload::Scatter(plot) => (plot.x_axis, vec![plot.series.as_slice()]),
        MsPlotPayload::ScatterGrid(grid) => (
            grid.x_axis,
            grid.panels
                .iter()
                .map(|panel| panel.series.as_slice())
                .collect(),
        ),
        _ => {
            return Err(TaskFailure::validation(
                "PlotMS atmosphere requires visibility scatter data",
            ));
        }
    };
    let mut models = BTreeMap::new();
    panels
        .into_iter()
        .map(|series| {
            let chunks = chunks(series, &mut metadata)
                .map_err(|error| TaskFailure::execution(error.to_string()))?;
            let mut signal = Curve {
                axis: options.axis(),
                samples: Vec::new(),
            };
            let mut image = Curve {
                axis: Axis::ImageSideband,
                samples: Vec::new(),
            };
            for ((_, scan, _, window, _), group) in chunks {
                let grid = grids
                    .get(&window)
                    .ok_or_else(|| TaskFailure::execution("missing atmospheric spectral grid"))?;
                let (signal_values, image_values) = match models.entry((window, scan)) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => entry.insert(model(
                        &context,
                        (window, scan),
                        &grid.frequencies,
                        &options,
                    )?),
                };
                for ((channel, sample), (&correlation, color)) in group
                    .channels
                    .into_iter()
                    .zip(group.correlations.iter().cycle())
                {
                    let Some(&bin) = grid.indices.get(&channel) else {
                        continue;
                    };
                    let mut point = sample.point;
                    point.corr = correlation;
                    let color = if spec.color_by == MsColorAxis::Correlation {
                        color
                    } else {
                        &sample.color_group
                    };
                    let display = MsScatterPlotMsMetadata {
                        channel: grid.display_channels[bin],
                        average_time: sample.average_time,
                        averaging_group: None,
                    };
                    let result = |y| CurveSample {
                        x: sample.x,
                        y,
                        point: point.clone(),
                        display,
                        frequency: grid.frequencies[bin],
                        color_group: color.clone(),
                    };
                    signal.samples.push(result(signal_values[bin]));
                    if options.image {
                        image.samples.push(result(image_values[bin]));
                    }
                    if x_axis == MsAxis::Time {
                        break;
                    }
                }
            }
            let mut curves = vec![signal];
            if options.image {
                curves.push(image);
            }
            Ok(curves)
        })
        .collect()
}

fn model(
    context: &Context,
    chunk: Chunk,
    frequencies: &[f64],
    options: &Options,
) -> Result<(Vec<f64>, Vec<f64>), TaskFailure> {
    let atmospheric_error =
        |error: casa_atmosphere::AtmosphereError| TaskFailure::execution(error.to_string());
    let conditions = context.conditions(chunk);
    let transmission = conditions
        .curve(frequencies, options.kind)
        .map_err(atmospheric_error)?;
    let image = if options.image {
        context
            .image_curve(chunk.0, frequencies, &conditions, options.kind)
            .map_err(atmospheric_error)?
    } else {
        Vec::new()
    };
    Ok((transmission, image))
}

type Integration = (i32, i32, i32, i32, u64);

#[derive(Default)]
struct IntegrationPoints {
    channels: BTreeMap<usize, SourcePoint>,
    correlations: BTreeMap<usize, String>,
}

struct SourcePoint {
    x: f64,
    point: MsScatterPointRef,
    average_time: Option<f64>,
    color_group: String,
}

fn chunks(
    series: &[MsScatterSeries],
    metadata: &mut Metadata<'_>,
) -> std::io::Result<BTreeMap<Integration, IntegrationPoints>> {
    let mut chunks = BTreeMap::<_, IntegrationPoints>::new();
    let first_axis = series.first().map(|series| series.y_axis);
    for series in series
        .iter()
        .filter(|series| Some(series.y_axis) == first_axis)
    {
        for (index, ((x, _), point)) in series.points.iter().zip(&series.provenance).enumerate() {
            let row = metadata.row(point.row)?;
            let average = series
                .plotms_metadata
                .get(index)
                .and_then(|display| display.average_time);
            let key = (
                row.observation,
                row.scan,
                row.field,
                row.spw,
                average.unwrap_or(row.time).to_bits(),
            );
            let chunk = chunks.entry(key).or_default();
            chunk
                .correlations
                .entry(point.corr)
                .or_insert_with(|| series.color_group.clone());
            let slots = chunk.channels.entry(point.chan_start);
            use std::collections::btree_map::Entry;
            let sample = || SourcePoint {
                x: *x,
                point: point.clone(),
                average_time: average,
                color_group: series.color_group.clone(),
            };
            match slots {
                Entry::Vacant(slot) => {
                    slot.insert(sample());
                }
                Entry::Occupied(mut slot)
                    if (point.row, point.corr) < (slot.get().point.row, slot.get().point.corr) =>
                {
                    slot.insert(sample());
                }
                Entry::Occupied(_) => {}
            }
        }
    }
    Ok(chunks)
}

struct Grid {
    frequencies: Vec<f64>,
    display_channels: Vec<usize>,
    indices: BTreeMap<usize, usize>,
}

fn grids(
    ms: &MeasurementSet,
    selection: &MsSelection,
    spec: &MsPlotSpec,
) -> Result<BTreeMap<i32, Grid>, String> {
    let spw = selection
        .spw
        .as_deref()
        .filter(|_| selection.selectdata)
        .unwrap_or("*");
    let selection = ms
        .resolve_spw_selector(spw)
        .map_err(|error| error.to_string())?;
    let spectral_window = ms.spectral_window().map_err(|error| error.to_string())?;
    let mut windows = BTreeMap::<i32, Vec<_>>::new();
    for &range in selection.channel_list() {
        windows.entry(range.spw_id as i32).or_default().push(range);
    }
    windows
        .into_iter()
        .map(|(window, ranges)| {
            let frequencies = spectral_window
                .chan_freq(window as usize)
                .map_err(|error| error.to_string())?;
            let bins = plot_selected_channel_bins(frequencies.len(), spec, &ranges)?;
            let mut grid = Grid {
                frequencies: Vec::with_capacity(bins.len()),
                display_channels: Vec::with_capacity(bins.len()),
                indices: BTreeMap::new(),
            };
            for bin in bins {
                let group = &frequencies[bin.start..bin.end];
                grid.indices.insert(bin.start, grid.frequencies.len());
                grid.frequencies
                    .push(group.iter().sum::<f64>() / group.len() as f64);
                grid.display_channels.push(bin.display_channel());
            }
            Ok((window, grid))
        })
        .collect()
}
