// SPDX-License-Identifier: LGPL-3.0-or-later
//! FITS-IDI UV_DATA rows, antennas and weighted spectrum conventions.

use std::ops::Range;

use casa_core::{ArrayValue, Complex32, Correlation, Value};
use casa_tables::Table;
use fitsio::FitsFile;
use fitsio::hdu::{FitsHdu, HduInfo};
use fitsio::tables::ReadsCol;
use ndarray::{Array1, Array2, Array3, ShapeBuilder};

use super::super::groups::{fits_correlation, optional_key};
use super::super::invalid;
use super::digital::DigitalCorrection;
use super::metadata::Geometry;
use crate::MsResult;
use crate::schema::main_table::REQUIRED_COLUMNS;
use crate::subtable_rows::{b, f, f64_array, i, row_from_defs};

const DAY: f64 = 86_400.0;
const SPEED_OF_LIGHT: f64 = 299_792_458.0;

pub(super) struct DataLayout {
    pub(super) bands: usize,
    pub(super) channels: usize,
    pub(super) correlations: Vec<Correlation>,
    pub(super) components: usize,
    input: Vec<Correlation>,
    normal: Vec<usize>,
    reversed: Vec<usize>,
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
                    let reference: f64 = hdu.read_key(fits, &key("CRVAL")).map_err(invalid)?;
                    let pixel: f64 = hdu.read_key(fits, &key("CRPIX")).map_err(invalid)?;
                    let step: f64 = hdu.read_key(fits, &key("CDELT")).map_err(invalid)?;
                    input = Some(
                        (0..samples)
                            .map(|index| {
                                fits_correlation(
                                    (reference + (index as f64 + 1.0 - pixel) * step + 0.5).floor()
                                        as i32,
                                    true,
                                )
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
        Ok(Self {
            bands: band.unwrap_or(1),
            channels: frequency.ok_or_else(|| invalid("FITS-IDI has no FREQ axis"))?,
            correlations,
            components,
            input,
            normal,
            reversed,
        })
    }

    fn floats(&self) -> usize {
        self.channels * self.input.len() * self.bands * self.components
    }
}

pub(super) struct SpectralInput<'a> {
    pub(super) resolutions: &'a [f64],
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
    table: &mut Table,
    geometry: &Geometry,
    spectral: &SpectralInput<'_>,
    scan: i32,
    times: &mut TimeRange,
) -> MsResult<DataLayout> {
    let layout = DataLayout::read(fits, hdu)?;
    let HduInfo::TableInfo { num_rows, .. } = hdu.info else {
        return Err(invalid("UV_DATA is not a FITS binary table"));
    };
    let scale = optional_key::<f64>(hdu, fits, "VIS_SCAL")?.unwrap_or(1.0);
    let weight_type = optional_key::<String>(hdu, fits, "WEIGHTYP")?.map_or(
        spectral.correlator == "VLBA" && spectral.correlator_version >= 4.17,
        |value| value.trim().eq_ignore_ascii_case("CORRELAT"),
    );
    let rows = InputColumns::resolve(hdu)?;
    let inferred = if rows.interval {
        None
    } else {
        Some(infer_interval(fits, hdu, &rows, num_rows)?)
    };
    let mut digital = DigitalCorrection::for_correlator(spectral.correlator, spectral.van_vleck);
    let rows_per_block = (262_144 / layout.floats()).clamp(1, 1024);
    for start in (0..num_rows).step_by(rows_per_block) {
        let range = start..(start + rows_per_block).min(num_rows);
        let values = RowBlock::read(fits, hdu, &rows, &layout, range)?;
        for index in 0..values.date.len() {
            let visibility = values.header(index, geometry, inferred.unwrap_or(1.0))?;
            times.first.get_or_insert(visibility.time);
            times.last_end = visibility.time + f64::from(visibility.interval);
            let visibility_divisor =
                if spectral.correlator == "VLBA" && spectral.correlator_version >= 4.17 {
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
            append_row(
                table,
                &layout,
                spectral.resolutions,
                &values,
                index,
                visibility,
                decoded,
            )?;
        }
    }
    Ok(layout)
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

fn append_row(
    table: &mut Table,
    layout: &DataLayout,
    bandwidths: &[f64],
    input: &RowBlock,
    row: usize,
    header: VisibilityHeader,
    mut scale: DecodeScale<'_>,
) -> MsResult<()> {
    let interval = f64::from(header.interval);
    let hands = layout.correlations.len();
    let dimensions = (hands, layout.channels).f();
    for band in 0..layout.bands {
        let window = header.frequency as usize * layout.bands + band;
        let bandwidth = *bandwidths
            .get(window)
            .ok_or_else(|| invalid("Missing FITS-IDI frequency band"))?;
        let mut data = Array2::zeros(dimensions);
        let mut flags = Array2::from_elem(dimensions, false);
        let mut weights = Array2::zeros(dimensions);
        let mut sigmas = Array2::zeros(dimensions);
        let offset = row * layout.floats() + band * hands * layout.channels * layout.components;
        let weighted = scale.visibility
            * interval
            * bandwidth
            * if header.antennas[0] == header.antennas[1] {
                1.0
            } else {
                2.0
            };
        let order = if header.reverse {
            &layout.reversed
        } else {
            &layout.normal
        };
        for channel in 0..layout.channels {
            for (hand, &destination) in order.iter().enumerate() {
                let pos = offset + (channel * hands + hand) * layout.components;
                let signed = match layout.components {
                    3 => input.flux[pos + 2],
                    _ => input
                        .weights
                        .get((row * layout.bands + band) * hands + hand)
                        .copied()
                        .unwrap_or(1.0),
                };
                let vis = Complex32::new(
                    input.flux[pos],
                    if header.reverse {
                        input.flux[pos + 1]
                    } else {
                        -input.flux[pos + 1]
                    },
                );
                data[[destination, channel]] = vis;
                flags[[destination, channel]] = signed <= 0.0;
                weights[[destination, channel]] = signed.abs();
            }
        }
        if let Some(digital) = &mut scale.digital {
            digital.apply(
                header.antennas[0] == header.antennas[1],
                scale.levels,
                &layout.correlations,
                &mut data,
                &weights,
            )?;
        }
        if let Some(divisor) = scale.visibility_divisor {
            data.zip_mut_with(&weights, |sample, &weight| *sample /= divisor * weight);
        }
        weights.mapv_inplace(|weight| (f64::from(weight) * weighted) as f32);
        sigmas.zip_mut_with(&weights, |sigma, &weight| {
            *sigma = if weight > 0.0 {
                weight.sqrt().recip()
            } else {
                1.0
            };
        });
        let mut row = row_from_defs(
            REQUIRED_COLUMNS,
            &[
                ("TIME", f(header.time)),
                ("TIME_CENTROID", f(header.time + 0.5 * interval)),
                ("EXPOSURE", f(interval)),
                ("INTERVAL", f(interval)),
                ("SCAN_NUMBER", i(scale.scan)),
                ("ANTENNA1", i(header.antennas[0])),
                ("ANTENNA2", i(header.antennas[1])),
                ("ARRAY_ID", i(header.array)),
                ("FIELD_ID", i(header.field)),
                ("DATA_DESC_ID", i(window as i32)),
                ("PROCESSOR_ID", i(-1)),
                ("STATE_ID", i(-1)),
                ("UVW", f64_array(&header.uvw, vec![3])),
                ("FLAG_ROW", b(flags.iter().all(|&flag| flag))),
            ],
        );
        let categories = Array3::from_shape_fn(
            (hands, layout.channels, 3).f(),
            |(hand, channel, category)| category == 0 && flags[[hand, channel]],
        );
        let row_weights = median_spectrum(&weights);
        let row_sigmas = median_spectrum(&sigmas);
        for (name, array) in [
            ("DATA", ArrayValue::Complex32(data.into_dyn())),
            ("FLAG", ArrayValue::Bool(flags.into_dyn())),
            ("FLAG_CATEGORY", ArrayValue::Bool(categories.into_dyn())),
            ("WEIGHT", ArrayValue::Float32(row_weights.into_dyn())),
            ("SIGMA", ArrayValue::Float32(row_sigmas.into_dyn())),
        ] {
            row.upsert(name, Value::Array(array));
        }
        if layout.components == 3 {
            row.upsert(
                "WEIGHT_SPECTRUM",
                Value::Array(ArrayValue::Float32(weights.into_dyn())),
            );
            row.upsert(
                "SIGMA_SPECTRUM",
                Value::Array(ArrayValue::Float32(sigmas.into_dyn())),
            );
        }
        table.add_row(row)?;
    }
    Ok(())
}

fn median_spectrum(spectrum: &Array2<f32>) -> Array1<f32> {
    let mut entries = Vec::with_capacity(spectrum.ncols());
    Array1::from_iter(spectrum.rows().into_iter().map(|row| {
        entries.clear();
        entries.extend(row.iter().copied());
        let middle = entries.len() / 2;
        let (lower, median, _) = entries.select_nth_unstable_by(middle, f32::total_cmp);
        if spectrum.ncols().is_multiple_of(2) {
            (lower.iter().max_by(|a, b| a.total_cmp(b)).unwrap() + *median) * 0.5
        } else {
            *median
        }
    }))
}
