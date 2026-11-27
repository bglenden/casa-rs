// SPDX-License-Identifier: LGPL-3.0-or-later
//! FITS-IDI UV_DATA rows, antennas and weighted spectrum conventions.

use std::ops::Range;

use casa_core::{Complex32, Correlation};
use fitsio::FitsFile;
use fitsio::hdu::{FitsHdu, HduInfo};
use fitsio::tables::{ColumnDataType, ReadsCol};
use ndarray::{Array1, Array2, ShapeBuilder};

use super::digital::DigitalCorrection;
use super::metadata::Geometry;
use casa_ms::MsResult;
use casa_ms::fits_visibility::reader_support::{
    ImportColumns, MainRow, MainSamples, fits_correlation, invalid, optional_key,
};

const DAY: f64 = 86_400.0;
const SPEED_OF_LIGHT: f64 = 299_792_458.0;

pub(super) struct DataLayout {
    pub(super) bands: usize,
    pub(super) channels: usize,
    pub(super) correlations: Vec<Correlation>,
    pub(super) components: usize,
    normal: Vec<usize>,
    reversed: Vec<usize>,
    floats: usize,
}

impl DataLayout {
    pub(super) fn read(fits: &mut FitsFile, hdu: &FitsHdu) -> MsResult<Self> {
        let axes: i32 = hdu.read_key(fits, "MAXIS").map_err(invalid)?;
        if !(4..=999).contains(&axes) {
            return Err(invalid("Invalid FITS-IDI visibility axes"));
        }
        let mut components = None;
        let mut band = None;
        let mut frequency = None;
        let mut input = None;
        for number in 1..=axes {
            let key = |kind| format!("{kind}{number}");
            let name: String = hdu.read_key(fits, &key("CTYPE")).map_err(invalid)?;
            let samples: i32 = hdu.read_key(fits, &key("MAXIS")).map_err(invalid)?;
            let samples = usize::try_from(samples)
                .ok()
                .filter(|&size| size > 0)
                .ok_or_else(|| invalid("Invalid FITS-IDI axis length"))?;
            match name.trim_end() {
                "COMPLEX" => components = Some(samples),
                "STOKES" => {
                    if samples > 8 {
                        return Err(invalid("Invalid FITS-IDI STOKES axis length"));
                    }
                    let reference: f64 = hdu.read_key(fits, &key("CRVAL")).map_err(invalid)?;
                    let pixel: f64 = hdu.read_key(fits, &key("CRPIX")).map_err(invalid)?;
                    let step: f64 = hdu.read_key(fits, &key("CDELT")).map_err(invalid)?;
                    input = Some(
                        (0..samples)
                            .map(|index| {
                                let code = (reference + (index as f64 + 1.0 - pixel) * step + 0.5)
                                    .floor() as i32;
                                if code == 1 {
                                    // The FITS-IDI reader treats SMA's +1 as a receptor pair.
                                    Ok(Correlation::XX)
                                } else {
                                    fits_correlation(code, false)
                                }
                            })
                            .collect::<MsResult<Vec<_>>>()?,
                    );
                }
                "FREQ" => frequency = Some(samples),
                "BAND" => band = Some(samples),
                _ if samples == 1 => {}
                _ => {
                    return Err(invalid(format!(
                        "FITS-IDI visibility axis {name} must be degenerate"
                    )));
                }
            }
        }
        let components = components
            .filter(|value| matches!(value, 2 | 3))
            .ok_or_else(|| invalid("FITS-IDI needs two or three complex components"))?;
        let input = input.ok_or_else(|| invalid("FITS-IDI has no STOKES axis"))?;
        let mut correlations = input.clone();
        correlations.sort_unstable();
        correlations.dedup();
        if correlations.len() != input.len() {
            return Err(invalid("Repeated FITS-IDI correlations"));
        }
        let position = |target| correlations.iter().position(|&value| value == target);
        let normal = input
            .iter()
            .map(|&value| position(value).unwrap())
            .collect();
        let reversed = input
            .iter()
            .map(|&value| {
                use Correlation::{LR, RL, XY, YX};
                position(match value {
                    RL => LR,
                    LR => RL,
                    XY => YX,
                    YX => XY,
                    other => other,
                })
                .unwrap_or_else(|| position(value).unwrap())
            })
            .collect();
        let bands = band.unwrap_or(1);
        let channels = frequency.ok_or_else(|| invalid("FITS-IDI has no FREQ axis"))?;
        let floats = channels
            .checked_mul(input.len())
            .and_then(|cells| cells.checked_mul(bands))
            .and_then(|cells| cells.checked_mul(components))
            .ok_or_else(|| invalid("FITS-IDI visibility row size overflow"))?;
        let HduInfo::TableInfo {
            column_descriptions,
            ..
        } = &hdu.info
        else {
            return Err(invalid("UV_DATA is not a FITS binary table"));
        };
        let row_bytes = hdu.read_key::<i64>(fits, "NAXIS1").map_err(invalid)?;
        if !column_descriptions.iter().any(|column| {
            column.name == "FLUX"
                && column.data_type.typ == ColumnDataType::Float
                && column.data_type.repeat == floats
        }) || floats.checked_mul(size_of::<f32>()).is_none_or(|bytes| {
            usize::try_from(row_bytes).map_or(true, |row_bytes| bytes > row_bytes)
        }) {
            return Err(invalid(
                "FITS-IDI FLUX repeat disagrees with the visibility axes",
            ));
        }
        Ok(Self {
            bands,
            channels,
            correlations,
            components,
            normal,
            reversed,
            floats,
        })
    }

    fn floats(&self) -> usize {
        self.floats
    }

    pub(super) fn same_output_shape(&self, other: &Self) -> bool {
        self.channels == other.channels
            && self.bands == other.bands
            && self.correlations == other.correlations
            && self.components == other.components
    }
}

pub(super) struct VisibilityInput<'a> {
    pub(super) resolutions: &'a [f64],
    pub(super) field_count: usize,
    pub(super) correlator: &'a str,
    pub(super) correlator_version: f64,
    pub(super) van_vleck: f64,
}

#[derive(Default)]
pub(super) struct TimeRange {
    pub(super) first: Option<f64>,
    pub(super) last_end: f64,
}

pub(super) fn append_hdu(
    fits: &mut FitsFile,
    hdu: &FitsHdu,
    columns: &mut ImportColumns,
    geometry: &Geometry,
    input: &VisibilityInput<'_>,
    scan: i32,
    times: &mut TimeRange,
) -> MsResult<()> {
    let layout = DataLayout::read(fits, hdu)?;
    let HduInfo::TableInfo { num_rows, .. } = hdu.info else {
        return Err(invalid("UV_DATA is not a FITS binary table"));
    };
    let scale = optional_key::<f64>(hdu, fits, "VIS_SCAL")?.unwrap_or(1.0);
    let weight_type = optional_key::<String>(hdu, fits, "WEIGHTYP")?
        .map_or(matches!(input.correlator, "DIFX" | "VLBA"), |value| {
            value.trim().eq_ignore_ascii_case("CORRELAT")
        });
    let rows = InputColumns::resolve(hdu)?;
    let inferred = if rows.interval {
        None
    } else {
        Some(infer_interval(fits, hdu, &rows, num_rows)?)
    };
    let mut buffers = DecodeBuffers::new(&layout, input.resolutions);
    let mut digital = DigitalCorrection::for_correlator(input.correlator, input.van_vleck);
    let rows_per_block = (262_144 / layout.floats()).clamp(1, 1024);
    for start in (0..num_rows).step_by(rows_per_block) {
        let range = start..(start + rows_per_block).min(num_rows);
        let values = RowBlock::read(fits, hdu, &rows, &layout, range)?;
        for index in 0..values.date.len() {
            let visibility = values.header(index, geometry, inferred.unwrap_or(1.0))?;
            if visibility.field as usize >= input.field_count {
                return Err(invalid("FITS-IDI SOURCE_ID has no matching FIELD row"));
            }
            times.first.get_or_insert(visibility.time);
            times.last_end = visibility.time + f64::from(visibility.interval);
            let visibility_divisor =
                if input.correlator == "VLBA" && input.correlator_version >= 4.17 {
                    scale * f64::from(visibility.interval)
                } else {
                    scale
                };
            let decoded = DecodeScale {
                visibility: scale,
                visibility_divisor: weight_type.then_some(visibility_divisor as f32),
                digital: digital.as_mut(),
                levels: visibility.antennas.map(|antenna| {
                    geometry
                        .digitizer_levels
                        .get(&antenna)
                        .copied()
                        .unwrap_or_default()
                }),
                scan,
            };
            buffers.append_row(columns, &values, index, visibility, decoded)?;
        }
    }
    Ok(())
}

struct InputColumns {
    uvw: [String; 3],
    offset_time: bool,
    interval: bool,
    field: Option<&'static str>,
    frequency: bool,
    weight: bool,
}

impl InputColumns {
    fn resolve(hdu: &FitsHdu) -> MsResult<Self> {
        let HduInfo::TableInfo {
            column_descriptions,
            ..
        } = &hdu.info
        else {
            return Err(invalid("UV_DATA is not a FITS binary table"));
        };
        let has = |name: &str| column_descriptions.iter().any(|column| column.name == name);
        let [u, v, w] = ["UU", "VV", "WW"].map(|prefix| {
            column_descriptions
                .iter()
                .find(|column| column.name.contains(prefix))
                .map(|column| column.name.clone())
                .ok_or_else(|| invalid("Missing FITS-IDI UVW coordinates"))
        });
        Ok(Self {
            uvw: [u?, v?, w?],
            offset_time: has("TIME"),
            interval: has("INTTIM"),
            field: ["SOURCE_ID", "SOURCE"].into_iter().find(|name| has(name)),
            frequency: has("FREQID"),
            weight: has("WEIGHT"),
        })
    }
}

fn column<T: ReadsCol>(
    fits: &mut FitsFile,
    hdu: &FitsHdu,
    name: &str,
    rows: &Range<usize>,
    width: usize,
) -> MsResult<Vec<T>> {
    let values = hdu.read_col_range(fits, name, rows).map_err(invalid)?;
    if values.len() != rows.len() * width {
        return Err(invalid(format!(
            "FITS-IDI column {name} requires {width} values per row"
        )));
    }
    Ok(values)
}

fn infer_interval(
    fits: &mut FitsFile,
    hdu: &FitsHdu,
    columns: &InputColumns,
    rows: usize,
) -> MsResult<f32> {
    let mut first = None;
    for start in (0..rows).step_by(1024) {
        let range = start..(start + 1024).min(rows);
        let dates = column::<f64>(fits, hdu, "DATE", &range, 1)?;
        let offsets = if columns.offset_time {
            column(fits, hdu, "TIME", &range, 1)?
        } else {
            vec![0.0; range.len()]
        };
        for (date, offset) in dates.into_iter().zip(offsets) {
            let now = (date - 2_400_000.5 + offset) * DAY;
            let start = *first.get_or_insert(now);
            if now > start {
                return Ok((now - start) as f32);
            }
        }
    }
    Ok(1.0)
}

struct RowBlock {
    date: Vec<f64>,
    time: Vec<f64>,
    baseline: Vec<f64>,
    uvw: [Vec<f64>; 3],
    field: Vec<i32>,
    frequency: Vec<i32>,
    interval: Vec<f32>,
    flux: Vec<f32>,
    weights: Vec<f32>,
}

impl RowBlock {
    fn read(
        fits: &mut FitsFile,
        hdu: &FitsHdu,
        names: &InputColumns,
        layout: &DataLayout,
        rows: Range<usize>,
    ) -> MsResult<Self> {
        let [u, v, w] = names
            .uvw
            .each_ref()
            .map(|name| column(fits, hdu, name, &rows, 1));
        let uvw = [u?, v?, w?];
        let mut integer = |name: Option<&str>| match name {
            Some(name) => column(fits, hdu, name, &rows, 1),
            None => Ok(vec![1; rows.len()]),
        };
        let field = integer(names.field)?;
        let frequency = integer(names.frequency.then_some("FREQID"))?;
        Ok(Self {
            field,
            frequency,
            uvw,
            date: column(fits, hdu, "DATE", &rows, 1)?,
            baseline: column(fits, hdu, "BASELINE", &rows, 1)?,
            time: if names.offset_time {
                column(fits, hdu, "TIME", &rows, 1)?
            } else {
                vec![0.0; rows.len()]
            },
            interval: if names.interval {
                column(fits, hdu, "INTTIM", &rows, 1)?
            } else {
                Vec::new()
            },
            flux: column(fits, hdu, "FLUX", &rows, layout.floats())?,
            weights: if names.weight && layout.components == 2 {
                column(
                    fits,
                    hdu,
                    "WEIGHT",
                    &rows,
                    layout.bands * layout.correlations.len(),
                )?
            } else {
                Vec::new()
            },
        })
    }

    fn header(
        &self,
        row: usize,
        geometry: &Geometry,
        inferred_interval: f32,
    ) -> MsResult<VisibilityHeader> {
        let baseline = self.baseline[row] as f32;
        let packed = baseline as i32;
        let array = (100.0 * (f64::from(baseline) - f64::from(packed) + 0.001)) as i32;
        let [a, b] = [packed / 256, packed % 256].map(|number| {
            geometry
                .antennas
                .get(&number)
                .copied()
                .ok_or_else(|| invalid(format!("Unknown FITS-IDI antenna number {number}")))
        });
        let (mut antennas, mut uvw) = (
            [a?, b?],
            self.uvw
                .each_ref()
                .map(|column| column[row] * SPEED_OF_LIGHT),
        );
        let reverse = antennas[0] > antennas[1];
        if reverse {
            antennas.swap(0, 1);
            uvw = uvw.map(|value| -value);
        }
        let time = (self.date[row] - 2_400_000.5 + self.time[row]) * DAY;
        let interval = self.interval.get(row).copied().unwrap_or(inferred_interval);
        let field = self.field[row] - 1;
        let frequency = self.frequency[row] - 1;
        if array < 0 || field < 0 || frequency < 0 || !time.is_finite() || !interval.is_finite() {
            return Err(invalid("Invalid FITS-IDI row metadata"));
        }
        Ok(VisibilityHeader {
            antennas,
            uvw,
            reverse,
            time,
            interval,
            array,
            field,
            frequency,
        })
    }
}

struct VisibilityHeader {
    antennas: [i32; 2],
    uvw: [f64; 3],
    reverse: bool,
    time: f64,
    interval: f32,
    array: i32,
    field: i32,
    frequency: i32,
}

struct DecodeScale<'a> {
    visibility: f64,
    visibility_divisor: Option<f32>,
    digital: Option<&'a mut DigitalCorrection>,
    levels: [i32; 2],
    scan: i32,
}

struct DecodeBuffers<'a> {
    layout: &'a DataLayout,
    bandwidths: &'a [f64],
    data: Array2<Complex32>,
    flags: Array2<bool>,
    weights: Array2<f32>,
    sigmas: Option<Array2<f32>>,
    row_weights: Array1<f32>,
    row_sigmas: Array1<f32>,
    scratch: Vec<f32>,
}

impl<'a> DecodeBuffers<'a> {
    fn new(layout: &'a DataLayout, bandwidths: &'a [f64]) -> Self {
        let hands = layout.correlations.len();
        let shape = (hands, layout.channels).f();
        Self {
            layout,
            bandwidths,
            data: Array2::zeros(shape),
            flags: Array2::from_elem(shape, false),
            weights: Array2::zeros(shape),
            sigmas: (layout.components == 3).then(|| Array2::zeros(shape)),
            row_weights: Array1::zeros(hands),
            row_sigmas: Array1::zeros(hands),
            scratch: Vec::with_capacity(layout.channels),
        }
    }

    fn append_row(
        &mut self,
        columns: &mut ImportColumns,
        input: &RowBlock,
        row: usize,
        header: VisibilityHeader,
        mut scale: DecodeScale<'_>,
    ) -> MsResult<()> {
        let interval = f64::from(header.interval);
        let autocorrelation = header.antennas[0] == header.antennas[1];
        for band in 0..self.layout.bands {
            let window = header.frequency as usize * self.layout.bands + band;
            let bandwidth = *self
                .bandwidths
                .get(window)
                .ok_or_else(|| invalid("Missing FITS-IDI frequency band"))?;
            self.load_band(input, row, band, header.reverse);
            if let Some(digital) = &mut scale.digital {
                digital.apply(
                    autocorrelation,
                    scale.levels,
                    &self.layout.correlations,
                    &mut self.data,
                    &self.weights,
                )?;
            }
            if let Some(divisor) = scale.visibility_divisor {
                self.data
                    .zip_mut_with(&self.weights, |sample, &weight| *sample /= divisor * weight);
            }
            let weight_scale =
                scale.visibility * interval * bandwidth * if autocorrelation { 1.0 } else { 2.0 };
            self.finish_weights(weight_scale);
            columns.append(
                MainRow {
                    antennas: header.antennas,
                    uvw: header.uvw,
                    time: header.time,
                    centroid: header.time + 0.5 * interval,
                    interval: Some(interval),
                    array: header.array,
                    field: header.field,
                    window: window as i32,
                    scan: scale.scan,
                },
                MainSamples {
                    data: &self.data,
                    flags: &self.flags,
                    weight: &self.row_weights,
                    sigma: &self.row_sigmas,
                    weight_spectrum: self.sigmas.as_ref().map(|_| &self.weights),
                    sigma_spectrum: self.sigmas.as_ref(),
                },
            )?;
        }
        Ok(())
    }

    fn load_band(&mut self, input: &RowBlock, row: usize, band: usize, reverse: bool) {
        let layout = self.layout;
        let hands = layout.correlations.len();
        let offset = row * layout.floats() + band * hands * layout.channels * layout.components;
        let order = if reverse {
            &layout.reversed
        } else {
            &layout.normal
        };
        for channel in 0..layout.channels {
            for (hand, &destination) in order.iter().enumerate() {
                let pos = offset + (channel * hands + hand) * layout.components;
                let signed = if layout.components == 3 {
                    input.flux[pos + 2]
                } else {
                    input
                        .weights
                        .get((row * layout.bands + band) * hands + hand)
                        .copied()
                        .unwrap_or(1.0)
                };
                let imaginary = if reverse {
                    input.flux[pos + 1]
                } else {
                    -input.flux[pos + 1]
                };
                self.data[[destination, channel]] = Complex32::new(input.flux[pos], imaginary);
                self.flags[[destination, channel]] = signed <= 0.0;
                self.weights[[destination, channel]] = signed.abs();
            }
        }
    }

    fn finish_weights(&mut self, scale: f64) {
        if let Some(sigmas) = &mut self.sigmas {
            self.weights
                .mapv_inplace(|weight| (f64::from(weight) * scale) as f32);
            sigmas.zip_mut_with(&self.weights, |sigma, &weight| {
                *sigma = sigma_for_weight(weight)
            });
            median_spectrum(&self.weights, &mut self.row_weights, &mut self.scratch);
            median_spectrum(sigmas, &mut self.row_sigmas, &mut self.scratch);
        } else {
            for ((weight, sigma), &input) in self
                .row_weights
                .iter_mut()
                .zip(&mut self.row_sigmas)
                .zip(self.weights.column(0))
            {
                *weight = (f64::from(input) * scale) as f32;
                *sigma = sigma_for_weight(*weight);
            }
        }
    }
}

fn sigma_for_weight(weight: f32) -> f32 {
    if weight > 0.0 {
        weight.sqrt().recip()
    } else {
        1.0
    }
}

fn median_spectrum(spectrum: &Array2<f32>, output: &mut Array1<f32>, entries: &mut Vec<f32>) {
    for (result, row) in output.iter_mut().zip(spectrum.rows()) {
        entries.clear();
        entries.extend(row.iter().copied());
        // casacore's partialMedians uses the lower middle for even lengths.
        let middle = (entries.len() - 1) / 2;
        *result = *entries.select_nth_unstable_by(middle, f32::total_cmp).1;
    }
}
