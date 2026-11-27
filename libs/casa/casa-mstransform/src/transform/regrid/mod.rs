// SPDX-License-Identifier: LGPL-3.0-or-later
//! Plan the reference spectral grid, and project each visibility buffer onto it.

mod axis;
pub use axis::LegacySpectralGrid;
mod frame_only;
pub use frame_only::{reframe_staged_ms, stage_frame_only_ms};
mod interpolate;
mod partition;
mod shift;
mod tool_grid;

pub(super) use partition::{Partitions, write_spectrum};
pub use tool_grid::{
    ToolGridFrameTables, ToolGridPhaseCentre, ToolSpectralWindow, tool_channel_frequencies,
    tool_grid_frame_at_epoch, tool_grid_reference_epoch,
};

use std::collections::BTreeMap;
use std::sync::Arc;

use casa_core::measures::MeasFrame;
use casa_core::measures::doppler::{DopplerRef, MDoppler};
use casa_core::measures::frequency::FrequencyRef;
use casa_core::measures::frequency_conversion::PreparedFrequencyConversion;
use casa_core::measures::radial_velocity::RadialVelocityRef;
use casa_core::{ArrayValue, ScalarValue, Value};
use casa_tables::{RequiredScalarColumnValues, Table, TableError, TableOptions};
use ndarray::{Array1, Ix2, Ix3};

use casa_ms::CasaChannelFrequencies;
use casa_ms::MeasurementSet;
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::derived::parse_phase_direction;
use casa_ms::subtables::FieldDirectionPolynomial;

use super::channel_average::{AveragedStatistics, sigma_from_weight, weight_from_sigma};
use super::{
    CompactSpectralMetadata, MsRegridOptions, MsTransformError, MsTransformRequest,
    TransformChannelSelection, combine, spectral_metadata,
};
use interpolate::{Interpolation, Method};
use shift::Shift;

pub(super) struct Plan {
    windows: BTreeMap<i32, Window>,
    field_layout: Option<frame_only::Layout>,
    largest_windows: BTreeMap<i32, i32>,
    combined: bool,
    legacy: bool,
}

struct Window {
    frequencies: Vec<f64>,
    axis: CasaChannelFrequencies,
    partitions: Partitions,
    frame: FrequencyRef,
    conversion: Option<Arc<Frame>>,
    sample_grid: Vec<f64>,
    channel_map: Option<Vec<Option<usize>>>,
    interpolation: Interpolation,
    method: Method,
    recent: Option<Recent>,
    reference_centre_hz: f64,
    noise_scale: f32,
}

struct Recent {
    time: f64,
    field: usize,
    interpolation: Option<Interpolation>,
    shift: Option<Shift>,
}

struct Frame {
    geometry: Arc<FrameGeometry>,
    target: MeasFrame,
    target_reference: FrequencyRef,
    metadata_reference: FrequencyRef,
    source_velocity: Option<f64>,
}

struct FrameGeometry {
    engine: MsCalEngine,
    field_polynomials: BTreeMap<usize, FieldDirectionPolynomial>,
}

const MIN_SOURCE_VELOCITY_M_PER_S: f64 = 1e-6;

impl Plan {
    pub(super) fn new(
        input: &MeasurementSet,
        (options, legacy): (&MsRegridOptions, Option<LegacySpectralGrid>),
        combined: Option<&mut combine::Grid>,
        channel_selection: &BTreeMap<i32, TransformChannelSelection>,
        channel_bins: &mut BTreeMap<i32, Vec<Vec<usize>>>,
        (first_selected_row, legacy_rows): (usize, Option<&[usize]>),
        budget_bytes: usize,
    ) -> Result<Self, MsTransformError> {
        let method = Method::parse(&options.interpolation).ok_or_else(|| {
            invalid(format!(
                "Invalid interpolation {:?}.",
                options.interpolation
            ))
        })?;
        let cvel = matches!(legacy, Some(LegacySpectralGrid::Cvel));
        if let Some(rows) = legacy_rows
            && !cvel
            && let Some(layout) = frame_only::Layout::new(input, options, Some(rows))?
        {
            return Self::with_field_layout(
                input,
                (options, legacy),
                method,
                layout,
                channel_bins,
                budget_bytes,
            );
        }
        let first_integration = match legacy_rows {
            Some(rows) if cvel => earliest_integration(input.main_table(), rows)?,
            _ => first_selected_row,
        };
        let frame =
            Frame::for_main_integration(input, options, first_integration, legacy.is_some())?
                .map(Arc::new);
        let mut budget = GridBudget::new(budget_bytes, frame.is_some());
        let table = input.spectral_window().map_err(ms_error)?;
        let is_combined = combined.is_some();
        let mut windows = BTreeMap::new();
        if let Some(grid) = combined {
            let id = *grid.windows.keys().next().expect("nonempty combined grid");
            let source = table
                .reference_frequency_reference(id as usize)
                .map_err(ms_error)?;
            let centres = grid
                .channels
                .iter()
                .map(|channel| channel.frequency)
                .collect::<Vec<_>>();
            let widths = grid
                .channels
                .iter()
                .map(|channel| channel.width)
                .collect::<Vec<_>>();
            let window = Window::new(
                (options, legacy),
                method,
                (&centres, &widths, source),
                &mut grid.bins,
                frame.clone(),
                &mut budget,
            )?;
            windows.insert(id, window);
        } else {
            for (&id, bins) in channel_bins {
                let row = id as usize;
                let source = table.reference_frequency_reference(row).map_err(ms_error)?;
                let mut centres = table.chan_freq(row).map_err(ms_error)?;
                let mut widths = table.chan_width(row).map_err(ms_error)?;
                let selection = &channel_selection[&id];
                if selection.channels != selection.metadata_ends {
                    let footprints =
                        spectral_metadata::selected_footprints(selection, &centres, &widths)
                            .ok_or_else(|| {
                                invalid(format!(
                                    "SPW {id} metadata does not cover the selected channels."
                                ))
                            })?;
                    for (&channel, (centre, width)) in selection
                        .channels
                        .iter()
                        .zip(footprints.0.into_iter().zip(footprints.1))
                    {
                        centres[channel] = centre;
                        widths[channel] = width;
                    }
                }
                let window = Window::new(
                    (options, legacy),
                    method,
                    (&centres, &widths, source),
                    bins,
                    frame.clone(),
                    &mut budget,
                )?;
                windows.insert(id, window);
            }
        }
        Ok(Self {
            windows,
            field_layout: None,
            largest_windows: BTreeMap::new(),
            combined: is_combined,
            legacy: legacy.is_some(),
        })
    }

    fn with_field_layout(
        input: &MeasurementSet,
        (options, legacy): (&MsRegridOptions, Option<LegacySpectralGrid>),
        method: Method,
        layout: frame_only::Layout,
        channel_bins: &BTreeMap<i32, Vec<Vec<usize>>>,
        budget_bytes: usize,
    ) -> Result<Self, MsTransformError> {
        let table = input.spectral_window().map_err(ms_error)?;
        let mut budget = GridBudget::new(budget_bytes, true);
        let geometry = FrameGeometry::open(input, options, true)?;
        let mut windows = BTreeMap::<i32, Window>::new();
        let mut largest_windows = BTreeMap::<i32, i32>::new();
        for (id, origin) in layout.windows.iter().enumerate() {
            let id = id as i32;
            let row = origin.spw as usize;
            let centres = table.chan_freq(row).map_err(ms_error)?;
            let widths = table.chan_width(row).map_err(ms_error)?;
            let mut bins = channel_bins[&origin.spw].clone();
            let frame = origin.frame_at_reference(options, &geometry)?.map(Arc::new);
            let window = Window::new(
                (options, legacy),
                method,
                (&centres, &widths, origin.reference),
                &mut bins,
                frame,
                &mut budget,
            )?;
            let largest = largest_windows.entry(origin.spw).or_insert(id);
            if windows
                .get(largest)
                .is_none_or(|other| window.axis.centres_hz.len() > other.axis.centres_hz.len())
            {
                *largest = id;
            }
            windows.insert(id, window);
        }
        Ok(Self {
            windows,
            field_layout: Some(layout),
            largest_windows,
            combined: false,
            legacy: true,
        })
    }

    pub(super) fn output_description(&self, row: usize) -> Option<i32> {
        self.field_layout.as_ref()?.description(row)
    }

    pub(super) fn is_legacy(&self) -> bool {
        self.legacy
    }

    pub(super) fn field_channel_count(&self, row: usize) -> Option<usize> {
        let id = self.field_layout.as_ref()?.window(row)?;
        self.windows
            .get(&id)
            .map(|window| window.axis.centres_hz.len())
    }

    fn window_id(&self, spw: i32) -> i32 {
        if self.combined {
            *self.windows.keys().next().expect("combined window")
        } else {
            self.largest_windows.get(&spw).copied().unwrap_or(spw)
        }
    }

    pub(super) fn channel_count(&self, spw: i32) -> usize {
        self.windows[&self.window_id(spw)].axis.centres_hz.len()
    }

    pub(super) fn parts(&self, spw: i32) -> &Partitions {
        &self.windows[&self.window_id(spw)].partitions
    }

    pub(super) fn split(&self) -> Option<&Partitions> {
        let first = &self.windows.values().next()?.partitions;
        (first.count() > 1).then_some(first)
    }

    pub(super) fn row(
        &mut self,
        spw: i32,
        input: &Table,
        row: usize,
    ) -> Result<Projection<'_>, MsTransformError> {
        let id = if let Some(layout) = &self.field_layout {
            layout
                .window(row)
                .ok_or_else(|| invalid(format!("Missing regrid window for MAIN row {row}.")))?
        } else {
            self.window_id(spw)
        };
        let window = self.windows.get_mut(&id).expect("selected window");
        let dynamic = window.conversion.as_ref().filter(|frame| {
            window.frame != frame.target_reference || frame.source_velocity.is_some()
        });
        let (interpolation, shift) = if let Some(frame) = dynamic {
            let time = casa_ms::subtables::get_f64(input, row, "TIME").map_err(ms_error)?;
            let field =
                casa_ms::subtables::get_i32(input, row, "FIELD_ID").map_err(ms_error)? as usize;
            if !window
                .recent
                .as_ref()
                .is_some_and(|recent| time == recent.time && field == recent.field)
            {
                let convert = frame.row_conversion(window.frame, time, field)?;
                let mut recent = Recent {
                    time,
                    field,
                    interpolation: None,
                    shift: None,
                };
                if window.method == Method::FftShift {
                    let output = &window.axis.centres_hz;
                    if output.len() > 1 {
                        let centre = window.frequencies[window.frequencies.len() / 2];
                        let bandwidth =
                            output[output.len() - 1] - output[0] + output[1] - output[0];
                        let offset = convert(centre) - window.reference_centre_hz;
                        recent.shift = Shift::new(-offset / bandwidth).map_err(ms_error)?;
                    }
                } else {
                    let grid = window
                        .frequencies
                        .iter()
                        .copied()
                        .map(convert)
                        .collect::<Vec<_>>();
                    recent.interpolation = Some(
                        Interpolation::plan(&grid, &window.sample_grid, window.method)
                            .map_err(ms_error)?,
                    );
                }
                window.recent = Some(recent);
            }
            let recent = window.recent.as_ref().expect("cached grid");
            (
                recent
                    .interpolation
                    .as_ref()
                    .unwrap_or(&window.interpolation),
                recent.shift,
            )
        } else {
            (&window.interpolation, None)
        };
        Ok(Projection {
            interpolation,
            shift,
            channel_map: window.channel_map.as_deref(),
            output_len: window.axis.centres_hz.len(),
            noise_scale: window.noise_scale,
        })
    }

    pub(super) fn write_metadata(
        &self,
        request: &MsTransformRequest,
        metadata: &CompactSpectralMetadata,
    ) -> Result<(), MsTransformError> {
        if let Some(layout) = &self.field_layout {
            return layout.write_resampled(&request.output_ms, |id, _, record| {
                let window = &self.windows[&(id as i32)];
                let range = 0..window.axis.centres_hz.len();
                for (name, value) in window.metadata_cells(range, self.legacy) {
                    record.upsert(name, value);
                }
                Ok(())
            });
        }
        let path = request.output_ms.join("SPECTRAL_WINDOW");
        let error = |source| MsTransformError::MutateMeasurementSet {
            path: path.display().to_string(),
            source: Box::new(source),
        };
        let mut table = Table::open(TableOptions::new(&path)).map_err(error)?;
        if let Some(parts) = self.split() {
            let template = table.row_accessor().row(0).map_err(error)?.clone();
            for _ in 1..parts.count() {
                table.add_row(template.clone()).map_err(error)?;
            }
            parts.write_descriptions(request)?;
            parts.write_window_references(request)?;
        }
        for (&original, window) in &self.windows {
            let row = if self.combined {
                0
            } else {
                metadata.spw_map[&original] as usize
            };
            for (part, range) in window.partitions.ranges().iter().enumerate() {
                for (name, value) in window.metadata_cells(range.clone(), self.legacy) {
                    table
                        .column_accessor_mut(name)
                        .and_then(|mut col| col.set(row + part, value))
                        .map_err(error)?;
                }
            }
        }
        table
            .prepare_write()
            .save(TableOptions::new(&path))
            .map_err(error)
    }
}

fn earliest_integration(main: &Table, rows: &[usize]) -> Result<usize, MsTransformError> {
    let mut columns = main
        .required_scalar_columns_owned_for_rows(&["TIME"], rows)
        .map_err(ms_error)?;
    let Some(RequiredScalarColumnValues::Float64(times)) = columns.remove("TIME") else {
        return Err(invalid(
            "Invalid TIME column for cvel frame conversion.".into(),
        ));
    };
    rows.iter()
        .copied()
        .zip(times)
        .min_by(|(left, a), (right, b)| a.total_cmp(b).then_with(|| left.cmp(right)))
        .map(|(row, _)| row)
        .ok_or_else(|| invalid("No integration for cvel frame conversion.".into()))
}

struct GridBudget {
    remaining: usize,
    bytes_per_channel: usize,
}

impl GridBudget {
    fn new(bytes: usize, dynamic: bool) -> Self {
        // Allow for retained centre/width arrays, interpolation records and
        // temporary grid construction. A dynamic frame also retains a
        // second interpolation plan for the most recent visibility buffer.
        Self {
            remaining: bytes,
            bytes_per_channel: if dynamic { 256 } else { 128 },
        }
    }

    fn maximum_output_channels(&self, input: usize) -> Result<usize, MsTransformError> {
        let count = (self.remaining / self.bytes_per_channel)
            .saturating_sub(input)
            .min(i32::MAX as usize);
        if count == 0 {
            Err(invalid(
                "Input regridding channels exceed the remaining grid budget.".into(),
            ))
        } else {
            Ok(count)
        }
    }

    fn charge(&mut self, input: usize, output: usize) -> Result<(), MsTransformError> {
        let bytes = input
            .saturating_add(output)
            .saturating_mul(self.bytes_per_channel);
        self.remaining = self.remaining.checked_sub(bytes).ok_or_else(|| {
            invalid(format!(
                "Regridding {output} channels exceeds the {} byte grid budget.",
                self.remaining
            ))
        })?;
        Ok(())
    }
}

impl Window {
    fn new(
        (options, legacy): (&MsRegridOptions, Option<LegacySpectralGrid>),
        method: Method,
        (source_frequencies, source_widths, frame): (&[f64], &[f64], FrequencyRef),
        bins: &mut Vec<Vec<usize>>,
        conversion: Option<Arc<Frame>>,
        budget: &mut GridBudget,
    ) -> Result<Self, MsTransformError> {
        let needs_weight_scaling = legacy.is_some()
            || conversion
                .as_ref()
                .is_some_and(|target| frame != target.target_reference);
        let converted_frame = conversion
            .as_ref()
            .map(|target| target.prepare(frame, &target.target))
            .transpose()?;
        let source_shift = conversion
            .as_ref()
            .and_then(|target| target.source_velocity)
            .filter(|velocity| velocity.abs() > MIN_SOURCE_VELOCITY_M_PER_S)
            .map(|velocity| radial_shift(-velocity));
        let into_frame = |frequency| {
            converted_frame
                .as_ref()
                .map_or(frequency, |convert| convert.convert_hz(frequency))
        };
        let shifted = |frequency| {
            source_shift
                .as_ref()
                .map_or(frequency, |shift| shift.shift_frequency_hz(frequency))
        };
        let convert = |frequency| shifted(into_frame(frequency));
        let (mut frequencies, widths) = averaged_axis(source_frequencies, source_widths, bins);
        let converted = frequencies.iter().copied().map(convert).collect::<Vec<_>>();
        let converted_widths = frequencies
            .iter()
            .zip(widths.iter())
            .map(|(&f, &width)| {
                if converted_frame.is_some() {
                    // CASA converts the two edges into the frame, differences
                    // them, then Doppler-scales the width as an interval.
                    shifted(into_frame(f + width / 2.) - into_frame(f - width / 2.))
                } else {
                    width
                }
            })
            .collect::<Vec<_>>();
        let channel_bounds = matches!(legacy, Some(LegacySpectralGrid::CenteredChannels { .. }))
            || (matches!(legacy, Some(LegacySpectralGrid::Cvel)) && options.mode == "channel_b");
        if channel_bounds
            && method == Method::FftShift
            && !legacy_fft_channel_spacing(&converted, &converted_widths)
        {
            return Err(MsTransformError::LegacyRegridUnchanged);
        }
        let maximum_channels = budget.maximum_output_channels(frequencies.len())?;
        let axis = match legacy {
            Some(
                grid @ (LegacySpectralGrid::CenteredChannels { .. }
                | LegacySpectralGrid::CenteredNumeric { .. }),
            ) => grid.centered_frequencies(&converted, &converted_widths, maximum_channels)?,
            Some(LegacySpectralGrid::FrameOnly) => CasaChannelFrequencies {
                centres_hz: converted.clone(),
                steps_hz: converted_widths.clone(),
            },
            _ => axis::output_axis(options, &converted, &converted_widths, maximum_channels)?,
        };
        let count = axis.centres_hz.len();
        budget.charge(frequencies.len(), count)?;
        let mut noise_scale = if needs_weight_scaling {
            (axis.steps_hz[0] / converted_widths[0]) * bins[0].len() as f64
        } else {
            1.
        };
        let partitions = Partitions::new(count, options.nspw)?;
        let mean =
            |values: &[f64]| values.iter().map(|v| v.abs()).sum::<f64>() / values.len() as f64;
        let ratio = mean(&axis.steps_hz) / mean(&converted_widths);
        let pre_bin = (ratio + 0.001).floor() as usize;
        let mut sample_grid = axis.centres_hz.clone();
        let mut channel_map = None;
        // SubMS writes interpolation at the output centres directly. The
        // fine intermediate grid is specific to the mstransform iterator.
        if legacy.is_none()
            && bins.iter().all(|bin| bin.len() == 1)
            && pre_bin >= 2
            && pre_bin <= frequencies.len() / 2
        {
            if options.preaverage {
                let selected = bins.iter().flatten().copied().collect::<Vec<_>>();
                *bins = selected.chunks(pre_bin).map(<[usize]>::to_vec).collect();
                (frequencies, _) = averaged_axis(source_frequencies, source_widths, bins);
                if needs_weight_scaling {
                    noise_scale /= pre_bin as f64;
                }
            } else {
                (sample_grid, channel_map) = fine_grid(&axis, frequencies.len());
            }
        }
        let reference_frequencies = if method == Method::FftShift {
            frequencies
                .iter()
                .copied()
                .map(into_frame)
                .collect::<Vec<_>>()
        } else {
            frequencies.iter().copied().map(convert).collect::<Vec<_>>()
        };
        let reference_centre_hz = reference_frequencies[reference_frequencies.len() / 2];
        let interpolation =
            Interpolation::plan(&reference_frequencies, &sample_grid, method).map_err(ms_error)?;
        Ok(Self {
            frequencies,
            axis,
            partitions,
            frame,
            conversion,
            sample_grid,
            channel_map,
            interpolation,
            method,
            recent: None,
            reference_centre_hz,
            noise_scale: noise_scale as f32,
        })
    }

    fn metadata_cells(
        &self,
        range: std::ops::Range<usize>,
        legacy: bool,
    ) -> [(&'static str, Value); 8] {
        let centres = &self.axis.centres_hz[range.clone()];
        let widths = &self.axis.steps_hz[range];
        let signed_widths = if legacy || self.axis.centres_hz.first() < self.axis.centres_hz.last()
        {
            widths.to_vec()
        } else {
            widths.iter().map(|width| -width).collect()
        };
        let array = |values: &[f64]| {
            Value::Array(ArrayValue::Float64(
                Array1::from_vec(values.to_vec()).into_dyn(),
            ))
        };
        let reference = self
            .conversion
            .as_ref()
            .map_or(self.frame, |frame| frame.metadata_reference);
        let bandwidth = (centres[centres.len() - 1] - centres[0]).abs()
            + (widths[0] + widths[widths.len() - 1]) / 2.;
        [
            ("CHAN_FREQ", array(centres)),
            ("CHAN_WIDTH", array(&signed_widths)),
            ("EFFECTIVE_BW", array(&signed_widths)),
            ("RESOLUTION", array(&signed_widths)),
            (
                "NUM_CHAN",
                Value::Scalar(ScalarValue::Int32(centres.len() as i32)),
            ),
            (
                "REF_FREQUENCY",
                Value::Scalar(ScalarValue::Float64(centres[0])),
            ),
            (
                "TOTAL_BANDWIDTH",
                Value::Scalar(ScalarValue::Float64(bandwidth)),
            ),
            (
                "MEAS_FREQ_REF",
                Value::Scalar(ScalarValue::Int32(reference.casacore_code())),
            ),
        ]
    }
}

fn legacy_fft_channel_spacing(centres: &[f64], widths: &[f64]) -> bool {
    let mut previous_spacing = widths.first().map_or(0., |width| width.abs());
    centres
        .windows(2)
        .zip(widths.windows(2))
        .all(|(centres, widths)| {
            let spacing = (centres[1] - centres[0]).abs();
            let equal = (widths[1].abs() - widths[0].abs()).abs() <= 0.1
                && (spacing - previous_spacing).abs() <= 0.1;
            previous_spacing = spacing;
            equal
        })
}

fn averaged_axis(frequencies: &[f64], widths: &[f64], bins: &[Vec<usize>]) -> (Vec<f64>, Vec<f64>) {
    bins.iter()
        .map(|bin| {
            (
                bin.iter().map(|&i| frequencies[i]).sum::<f64>() / bin.len() as f64,
                bin.iter().map(|&i| widths[i].abs()).sum::<f64>(),
            )
        })
        .unzip()
}

fn fine_grid(
    axis: &CasaChannelFrequencies,
    input_count: usize,
) -> (Vec<f64>, Option<Vec<Option<usize>>>) {
    let centres = &axis.centres_hz;
    let widths = &axis.steps_hz;
    let start = if centres.first() > centres.last() {
        centres.len() - 1
    } else {
        0
    };
    let count_ratio = input_count as f64 / centres.len() as f64;
    let mut frequencies = Vec::with_capacity(input_count);
    let mut frequency = centres[start] - widths[start] / 2.;
    frequencies.push(frequency);
    for index in 1..input_count {
        let width = widths[(index as f64 / count_ratio) as usize] / count_ratio;
        frequency += width;
        frequencies.push(frequency);
    }
    let descending = centres.first() > centres.last();
    let ordered = |index| {
        if descending {
            centres.len() - index - 1
        } else {
            index
        }
    };
    let map = frequencies
        .iter()
        .map(|&frequency| {
            let mut low = 0;
            let mut high = centres.len();
            while low < high {
                let mid = (low + high) / 2;
                let i = ordered(mid);
                if centres[i] + widths[i] / 2. <= frequency {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            if low == centres.len() {
                return None;
            }
            let index = ordered(low);
            (frequency >= centres[index] - widths[index] / 2.).then_some(index)
        })
        .collect();
    (frequencies, Some(map))
}

impl FrameGeometry {
    fn open(
        input: &MeasurementSet,
        options: &MsRegridOptions,
        legacy: bool,
    ) -> Result<Arc<Self>, MsTransformError> {
        // SubMS evaluates FIELD.phaseDirMeas at the integration epoch for
        // every frequency frame, including the direction of an ephemeris.
        let engine = if legacy {
            MsCalEngine::for_spectral_rewrite(input)
        } else if options.outframe.eq_ignore_ascii_case("SOURCE") {
            MsCalEngine::for_calibration(input)
        } else {
            MsCalEngine::new(input)
        }
        .map_err(ms_error)?;
        let mut field_polynomials = BTreeMap::new();
        if legacy {
            let fields = input.field().map_err(ms_error)?;
            for field in 0..fields.row_count() {
                if let Some(polynomial) =
                    fields.phase_direction_polynomial(field).map_err(ms_error)?
                {
                    field_polynomials.insert(field, polynomial);
                }
            }
        }
        Ok(Arc::new(Self {
            engine,
            field_polynomials,
        }))
    }

    fn at(&self, time: f64, field: usize) -> casa_ms::MsResult<MeasFrame> {
        let Some(polynomial) = self.field_polynomials.get(&field) else {
            return self.engine.spectral_frame_observatory(time, field);
        };
        let direction = polynomial.at_seconds(time);
        let frame = self
            .engine
            .spectral_frame_observatory_direction(time, direction.direction.clone())?;
        let resolved = direction.resolve_offset(&frame)?;
        Ok(frame.with_direction(resolved))
    }
}

impl Frame {
    fn for_main_integration(
        input: &MeasurementSet,
        options: &MsRegridOptions,
        row: usize,
        legacy: bool,
    ) -> Result<Option<Self>, MsTransformError> {
        if options.outframe.trim().is_empty() {
            return Ok(None);
        }
        let main = input.main_table();
        let time = casa_ms::subtables::get_f64(main, row, "TIME").map_err(ms_error)?;
        let field = casa_ms::subtables::get_i32(main, row, "FIELD_ID").map_err(ms_error)?;
        let geometry = FrameGeometry::open(input, options, legacy)?;
        Self::new(options, time, field as usize, &geometry).map(Some)
    }

    fn new(
        options: &MsRegridOptions,
        reference_time: f64,
        reference_field: usize,
        geometry: &Arc<FrameGeometry>,
    ) -> Result<Self, MsTransformError> {
        let name = options.outframe.to_ascii_uppercase();
        let target_reference = if name == "SOURCE" {
            FrequencyRef::GEO
        } else {
            name.parse::<FrequencyRef>()
                .ok()
                .filter(|reference| {
                    !matches!(reference, FrequencyRef::REST | FrequencyRef::Undefined)
                })
                .ok_or_else(|| invalid_choice("outframe", &options.outframe))?
        };
        let metadata_reference = if target_reference == FrequencyRef::GEO {
            FrequencyRef::REST
        } else {
            target_reference
        };
        let phasecenter = options.phasecenter.trim();
        let field = if phasecenter.is_empty() {
            Some(reference_field)
        } else {
            phasecenter.parse::<usize>().ok()
        };
        let mut source_velocity = (name == "SOURCE").then_some(0.);
        let target = if let Some(field) = field {
            if name == "SOURCE" {
                let velocity = geometry
                    .engine
                    .attached_radial_velocity(reference_time, field)
                    .map_err(ms_error)?;
                if phasecenter.is_empty()
                    && velocity
                        .as_ref()
                        .is_none_or(|velocity| velocity.refer() != RadialVelocityRef::GEO)
                {
                    return Err(invalid(format!(
                        "SOURCE regridding needs a GEO ephemeris on FIELD_ID {field}."
                    )));
                }
                source_velocity = Some(velocity.map_or(0., |velocity| velocity.ms()));
            }
            geometry.at(reference_time, field)
        } else {
            let direction = parse_phase_direction(phasecenter).ok_or_else(|| {
                invalid(format!(
                    "Cannot interpret phasecenter {phasecenter:?} as a FIELD ID or direction."
                ))
            })?;
            geometry
                .engine
                .spectral_frame_observatory_direction(reference_time, direction)
        }
        .map_err(ms_error)?;
        Ok(Self {
            geometry: Arc::clone(geometry),
            target,
            target_reference,
            metadata_reference,
            source_velocity,
        })
    }

    fn prepare(
        &self,
        source: FrequencyRef,
        origin: &MeasFrame,
    ) -> Result<PreparedFrequencyConversion, MsTransformError> {
        if source == self.target_reference {
            PreparedFrequencyConversion::new(source, self.target_reference, None, None)
        } else {
            PreparedFrequencyConversion::from_measure_references(
                source,
                self.target_reference,
                origin,
                &self.target,
            )
        }
        .map_err(ms_error)
    }

    fn row_conversion(
        &self,
        source: FrequencyRef,
        time: f64,
        field: usize,
    ) -> Result<impl Fn(f64) -> f64 + use<>, MsTransformError> {
        let origin = self.geometry.at(time, field).map_err(ms_error)?;
        let conversion = self.prepare(source, &origin)?;
        let correction = if let Some(offset) = self.source_velocity {
            self.geometry
                .engine
                .attached_radial_velocity(time, field)
                .map_err(ms_error)?
                .map(|velocity| velocity.ms())
                .filter(|velocity| velocity.abs() > MIN_SOURCE_VELOCITY_M_PER_S)
                .map_or(0., |velocity| velocity - 2. * offset)
        } else {
            0.
        };
        let shift = radial_shift(correction);
        Ok(move |frequency| shift.shift_frequency_hz(conversion.convert_hz(frequency)))
    }
}

fn radial_shift(m_per_s: f64) -> MDoppler {
    MDoppler::new(m_per_s / 299_792_458., DopplerRef::BETA)
}

pub(super) struct Projection<'a> {
    interpolation: &'a Interpolation,
    shift: Option<Shift>,
    channel_map: Option<&'a [Option<usize>]>,
    output_len: usize,
    noise_scale: f32,
}

impl Projection<'_> {
    pub(super) fn fft_input_flags(
        &self,
        value: &ArrayValue,
    ) -> Result<Option<ArrayValue>, TableError> {
        self.shift.map(|_| self.project(value.clone())).transpose()
    }

    pub(super) fn apply(
        &self,
        value: ArrayValue,
        fft_flags: Option<&ArrayValue>,
    ) -> Result<ArrayValue, TableError> {
        let value = self.project(value)?;
        if let Some(shift) = self.shift {
            shift.apply(value, fft_flags)
        } else {
            Ok(value)
        }
    }

    fn project(&self, value: ArrayValue) -> Result<ArrayValue, TableError> {
        let value = self.interpolation.apply(value)?;
        self.aggregate(value)
    }

    pub(super) fn apply_categories(&self, value: ArrayValue) -> Result<ArrayValue, TableError> {
        let ArrayValue::Bool(values) = value else {
            return Err(TableError::Schema("FLAG_CATEGORY must be Bool.".into()));
        };
        let view = values
            .view()
            .into_dimensionality::<Ix3>()
            .map_err(|_| TableError::Schema("FLAG_CATEGORY must have rank 3.".into()))?;
        let ArrayValue::Bool(value) = self.interpolation.apply_categories(view) else {
            unreachable!()
        };
        let Some(map) = self.channel_map else {
            return Ok(ArrayValue::Bool(value));
        };
        let mut output = ndarray::Array3::default((view.dim().0, self.output_len, view.dim().2));
        for (from, &to) in map.iter().enumerate() {
            if let Some(to) = to {
                for corr in 0..view.dim().0 {
                    for cat in 0..view.dim().2 {
                        output[[corr, to, cat]] |= value[[corr, from, cat]];
                    }
                }
            }
        }
        Ok(ArrayValue::Bool(output.into_dyn()))
    }

    fn aggregate(&self, value: ArrayValue) -> Result<ArrayValue, TableError> {
        let Some(map) = self.channel_map else {
            return Ok(value);
        };
        let correlations = value.shape()[0];
        macro_rules! aggregate {
            ($kind:ident, $value:ident, $update:expr) => {{
                let mut output = ndarray::Array2::default((correlations, self.output_len));
                let mut counts = vec![0; self.output_len];
                for (from, &to) in map.iter().enumerate() {
                    if let Some(to) = to {
                        counts[to] += 1;
                        for corr in 0..correlations {
                            let old = output[[corr, to]];
                            let new = $value[[corr, from]];
                            output[[corr, to]] = ($update)(old, new, counts[to]);
                        }
                    }
                }
                Ok(ArrayValue::$kind(output.into_dyn()))
            }};
        }
        match value {
            ArrayValue::Float32(data) => aggregate!(Float32, data, |old, new, count| old
                + (new - old) / count as f32),
            ArrayValue::Complex32(data) => aggregate!(Complex32, data, |old, new, count| old
                + (new - old) / count as f32),
            ArrayValue::Bool(flags) => aggregate!(Bool, flags, |old, new, _| old || new),
            _ => Err(TableError::Schema(
                "Unexpected regridded visibility type.".into(),
            )),
        }
    }

    pub(super) fn scale_noise(
        &self,
        values: &mut ArrayValue,
        sigma: bool,
        hanning: bool,
    ) -> Result<(), TableError> {
        let ArrayValue::Float32(values) = values else {
            return Err(TableError::Schema(
                "Spectral weights and sigma must be Float32.".into(),
            ));
        };
        let scale = if sigma {
            // SubMS calculates SIGMA before refreshing the WEIGHT factors.
            let factor = if hanning { 1.32 } else { 1.0 };
            (self.noise_scale * factor).sqrt().recip()
        } else {
            self.noise_scale
        };
        values.mapv_inplace(|value| value * scale);
        Ok(())
    }

    pub(super) fn statistics(
        &self,
        weight: ArrayValue,
        sigma: ArrayValue,
        output_flags: &ArrayValue,
        fft_flags: Option<&ArrayValue>,
    ) -> Result<AveragedStatistics, TableError> {
        let ArrayValue::Float32(sigma) = sigma else {
            return Err(TableError::Schema("SIGMA_SPECTRUM must be Float32.".into()));
        };
        let ArrayValue::Float32(weight_spectrum) = self.apply(weight, fft_flags)? else {
            unreachable!()
        };
        let weight_spectrum =
            ArrayValue::Float32(weight_spectrum.mapv(|weight| weight * self.noise_scale));
        let ArrayValue::Float32(inverse_variance) = self.apply(
            ArrayValue::Float32(sigma.mapv(weight_from_sigma)),
            fft_flags,
        )?
        else {
            unreachable!()
        };
        let sigma_spectrum = ArrayValue::Float32(
            inverse_variance.mapv(|weight| sigma_from_weight(weight * self.noise_scale)),
        );
        let ArrayValue::Bool(flags) = output_flags else {
            return Err(TableError::Schema("FLAG must be Bool.".into()));
        };
        let flags = flags
            .view()
            .into_dimensionality::<Ix2>()
            .map_err(|_| TableError::Schema("FLAG must have rank 2.".into()))?;
        Ok(AveragedStatistics {
            weight: combine::mean_noise(&weight_spectrum, flags)?,
            sigma: combine::mean_noise(&sigma_spectrum, flags)?,
            weight_spectrum,
            sigma_spectrum,
        })
    }
}

fn invalid_choice(parameter: &'static str, value: &str) -> MsTransformError {
    MsTransformError::RegridParameter {
        parameter,
        reason: format!("unallowed value {value}"),
    }
}

fn invalid(message: String) -> MsTransformError {
    MsTransformError::Spectrum(message)
}

fn ms_error(source: impl ToString) -> MsTransformError {
    invalid(source.to_string())
}

pub(super) fn input_noise(
    (weight, sigma): (&ArrayValue, &ArrayValue),
    spectra: super::channel_average::SpectrumCells<'_>,
    flags: &ArrayValue,
    bins: &[Vec<usize>],
    columns: &super::visibility_columns::VisibilityColumns,
    averaging_time: bool,
) -> Result<(ArrayValue, ArrayValue), TableError> {
    use super::channel_average::unaveraged_spectra;
    use casa_ms::OptionalMainColumn::{CorrectedData, Data, ModelData};
    let has = |source| columns.copies().iter().any(|copy| copy.source == source);
    if columns
        .copies()
        .iter()
        .all(|column| column.source == ModelData)
    {
        let shape = [flags.shape()[0], bins.len()];
        let unit = ArrayValue::Float32(ndarray::Array2::from_elem(shape, 1.).into_dyn());
        return Ok((unit.clone(), unit));
    }
    let (mut weight, sigma) = unaveraged_spectra(
        weight,
        sigma,
        spectra,
        flags,
        bins,
        !has(Data) && !averaging_time,
    )?;
    if has(Data) && !has(CorrectedData) && !averaging_time {
        let ArrayValue::Float32(sigma) = &sigma else {
            unreachable!()
        };
        weight = ArrayValue::Float32(sigma.mapv(weight_from_sigma));
    }
    Ok((weight, sigma))
}

#[cfg(test)]
mod tests {
    use super::GridBudget;

    #[test]
    fn independent_windows_share_the_grid_allowance() {
        let mut budget = GridBudget::new(256_000, true);
        assert_eq!(budget.maximum_output_channels(500).unwrap(), 500);
        budget.charge(500, 500).unwrap();
        assert!(budget.maximum_output_channels(500).is_err());
    }
}
