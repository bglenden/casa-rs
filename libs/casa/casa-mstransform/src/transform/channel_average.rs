// SPDX-License-Identifier: LGPL-3.0-or-later
//! Flag-aware channel averaging for a single visibility row.

use std::borrow::Cow;
use std::ops::{AddAssign, Div, Mul};

use casa_core::ArrayValue;
use casa_tables::TableError;
use ndarray::{Array1, Array2, ArrayD, ArrayView1, ArrayView2, Ix1, Ix2};

use casa_ms::schema::main_table::OptionalMainColumn;

use super::visibility_columns::VisibilityColumns;

struct ChannelMatrix<'a, T: Clone> {
    values: Cow<'a, [T]>,
    strides: [usize; 2],
}

impl<'a, T: Copy> ChannelMatrix<'a, T> {
    fn new(matrix: ArrayView2<'a, T>) -> Self {
        let corr = matrix.strides()[0];
        let chan = matrix.strides()[1];
        // Keep either contiguous axis order. A sliced, non-contiguous cell
        // is copied once in logical order before the inner averaging loop.
        if let (Ok(corr), Ok(chan), Some(values)) = (
            usize::try_from(corr),
            usize::try_from(chan),
            matrix.to_slice_memory_order(),
        ) {
            return Self {
                values: Cow::Borrowed(values),
                strides: [corr, chan],
            };
        }
        Self {
            values: Cow::Owned(matrix.iter().copied().collect()),
            strides: [matrix.ncols(), 1],
        }
    }

    #[inline(always)]
    fn get(&self, corr: usize, chan: usize) -> T {
        self.values[corr * self.strides[0] + chan * self.strides[1]]
    }
}

/// Weight convention backing a visibility data column.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum WeightKind {
    /// Observed weights (`WEIGHT` / `SIGMA`).
    Observed,
    /// Calibrated weights (`WEIGHT_SPECTRUM` / `WEIGHT` on corrected data).
    Calibrated,
    /// Unit weights for model or lag data.
    Unit,
}

/// Choose the weight convention matching the requested visibility `column`.
pub fn visibility_weight_kind(
    column: OptionalMainColumn,
    has_observed: bool,
    has_corrected: bool,
    time_averaged: bool,
) -> WeightKind {
    use OptionalMainColumn::{Data, FloatData, LagData, ModelData};
    match column {
        Data => WeightKind::Observed,
        FloatData if time_averaged && has_corrected => WeightKind::Observed,
        ModelData if has_corrected => WeightKind::Calibrated,
        ModelData if has_observed => WeightKind::Observed,
        ModelData | LagData => WeightKind::Unit,
        _ => WeightKind::Calibrated,
    }
}

pub(super) struct InputStatistic<'a> {
    scalar: ArrayView1<'a, f32>,
    spectrum: Option<ChannelMatrix<'a, f32>>,
}

#[derive(Clone, Copy, Default)]
pub(super) struct SpectrumCells<'a> {
    pub(super) weight: Option<&'a ArrayValue>,
    pub(super) sigma: Option<&'a ArrayValue>,
}

impl<'a> InputStatistic<'a> {
    pub(super) fn from_cells(
        scalar: &'a ArrayValue,
        spectrum: Option<&'a ArrayValue>,
        shape: [usize; 2],
        label: &str,
    ) -> Result<Self, TableError> {
        let ArrayValue::Float32(scalar) = scalar else {
            return Err(TableError::Schema(format!("{label} must hold Float32")));
        };
        let scalar = scalar
            .view()
            .into_dimensionality::<Ix1>()
            .ok()
            .filter(|value| value.len() == shape[0])
            .ok_or_else(|| {
                TableError::Schema(format!("{label} does not match the correlation axis"))
            })?;
        let invalid_spectrum = || {
            TableError::Schema(format!(
                "{label}_SPECTRUM does not match FLAG's correlation and channel axes"
            ))
        };
        let spectrum = match spectrum {
            None => None,
            Some(ArrayValue::Float32(spectrum)) if spectrum.is_empty() => None,
            Some(ArrayValue::Float32(spectrum)) if spectrum.shape() == shape => {
                Some(ChannelMatrix::new(
                    spectrum
                        .view()
                        .into_dimensionality::<Ix2>()
                        .map_err(|_| invalid_spectrum())?,
                ))
            }
            Some(_) => return Err(invalid_spectrum()),
        };
        Ok(Self { scalar, spectrum })
    }

    pub(super) fn value(&self, corr: usize, chan: usize) -> f32 {
        self.spectrum
            .as_ref()
            .map_or_else(|| self.scalar[corr], |array| array.get(corr, chan))
    }

    pub(super) fn scalar(&self, corr: usize) -> f32 {
        self.scalar[corr]
    }
}

pub(super) struct AveragedStatistics {
    pub(super) weight: ArrayValue,
    pub(super) sigma: ArrayValue,
    pub(super) weight_spectrum: ArrayValue,
    pub(super) sigma_spectrum: ArrayValue,
}

pub(super) struct ChannelAverages<'a> {
    flags: ChannelMatrix<'a, bool>,
    input_shape: (usize, usize),
    bins: &'a [Vec<usize>],
    bin_statistics: Array2<BinStatistics>,
    sigma: InputStatistic<'a>,
    weight: InputStatistic<'a>,
    data: bool,
    corrected: bool,
    model: bool,
    floating: bool,
    main: OptionalMainColumn,
    propagate_weights: bool,
    time_averaged: bool,
}

struct BinStatistics {
    flagged: bool,
    weights: [f32; 3],
}

impl<'a> ChannelAverages<'a> {
    pub(super) fn new(
        flags: &'a ArrayValue,
        bins: &'a [Vec<usize>],
        weight: &'a ArrayValue,
        sigma: &'a ArrayValue,
        spectra: SpectrumCells<'a>,
        columns: &VisibilityColumns,
        (propagate_weights, time_averaged): (bool, bool),
    ) -> Result<Self, TableError> {
        use OptionalMainColumn::{CorrectedData, Data, FloatData, LagData, ModelData};

        let ArrayValue::Bool(flags) = flags else {
            return Err(TableError::Schema("FLAG must hold Bool".to_string()));
        };
        let flags = flags
            .view()
            .into_dimensionality::<Ix2>()
            .map_err(|_| TableError::Schema("FLAG must be rank-2 [corr, chan]".to_string()))?;
        let (correlations, channels) = flags.dim();
        let flags = ChannelMatrix::new(flags);
        super::validate_channel_bins(bins, channels)?;
        let shape = [correlations, channels];
        let weight = InputStatistic::from_cells(weight, spectra.weight, shape, "WEIGHT")?;
        let sigma = InputStatistic::from_cells(sigma, spectra.sigma, shape, "SIGMA")?;
        let has = |source| columns.copies().iter().any(|copy| copy.source == source);
        let data = has(Data);
        let corrected = has(CorrectedData);
        let model = has(ModelData);
        let floating = has(FloatData);
        let main = [Data, CorrectedData, ModelData, FloatData, LagData]
            .into_iter()
            .find(|&column| has(column))
            .ok_or_else(|| {
                TableError::Schema("channel averaging requires a visibility column".to_string())
            })?;
        let bin_statistics = Array2::from_shape_fn((correlations, bins.len()), |(corr, bin)| {
            let flagged = bins[bin].iter().all(|&chan| flags.get(corr, chan));
            let mut weights = [0.0; 3];
            for &chan in &bins[bin] {
                if flags.get(corr, chan) == flagged {
                    weights[WeightKind::Observed as usize] +=
                        weight_from_sigma(sigma.value(corr, chan));
                    weights[WeightKind::Calibrated as usize] += weight.value(corr, chan);
                    weights[WeightKind::Unit as usize] += 1.0;
                }
            }
            BinStatistics { flagged, weights }
        });
        Ok(Self {
            flags,
            input_shape: (correlations, channels),
            bins,
            bin_statistics,
            sigma,
            weight,
            data,
            corrected,
            model,
            floating,
            main,
            propagate_weights,
            time_averaged,
        })
    }

    pub(super) fn visibility(
        &self,
        column: OptionalMainColumn,
        value: ArrayValue,
    ) -> Result<ArrayValue, TableError> {
        let weight = self.visibility_weight(column);
        match value {
            ArrayValue::Complex32(array) => self
                .mean_visibility(&array, weight)
                .map(ArrayValue::Complex32),
            ArrayValue::Float32(array) => self
                .mean_visibility(&array, weight)
                .map(ArrayValue::Float32),
            _ => Err(TableError::Schema(
                "visibility arrays must hold Complex32 or Float32".to_string(),
            )),
        }
    }

    pub(super) fn flags(&self) -> ArrayValue {
        let kind = self.visibility_weight(self.main);
        let output = self.bin_statistics.map(|bin| {
            let weight = bin.weights[kind as usize];
            bin.flagged || weight <= 0.0 || weight.is_nan()
        });
        ArrayValue::Bool(output.into_dyn())
    }

    pub(super) fn statistics(&self) -> AveragedStatistics {
        let (weight, variance) = self.weight_spectra();
        let sigma = variance.mapv(sigma_from_weight);
        AveragedStatistics {
            weight: ArrayValue::Float32(self.mean_bins(&weight).into_dyn()),
            sigma: ArrayValue::Float32(self.mean_bins(&sigma).into_dyn()),
            weight_spectrum: ArrayValue::Float32(weight.into_dyn()),
            sigma_spectrum: ArrayValue::Float32(sigma.into_dyn()),
        }
    }

    pub(super) fn weight_spectra(&self) -> (Array2<f32>, Array2<f32>) {
        let observed = self.data || (self.time_averaged && self.floating);
        if self.time_averaged && !(observed && self.corrected) {
            let weights = self.summed_weights(WeightKind::Calibrated);
            (weights.clone(), weights)
        } else if self.corrected {
            let calibrated = self.summed_weights(WeightKind::Calibrated);
            let observed = if observed {
                self.summed_weights(WeightKind::Observed)
            } else {
                calibrated.clone()
            };
            (calibrated, observed)
        } else {
            let kind = if self.data {
                WeightKind::Observed
            } else if self.model {
                WeightKind::Unit
            } else {
                WeightKind::Calibrated
            };
            let weights = self.summed_weights(kind);
            (weights.clone(), weights)
        }
    }

    pub(super) fn bin_flags(&self) -> Array2<bool> {
        self.bin_statistics.map(|bin| bin.flagged)
    }

    fn visibility_weight(&self, column: OptionalMainColumn) -> WeightKind {
        // CASA uses the scalar statistics for output weights and sigmas, but
        // uses an unweighted visibility average when weight spectra are absent.
        if !self.propagate_weights {
            return WeightKind::Unit;
        }
        visibility_weight_kind(column, self.data, self.corrected, self.time_averaged)
    }

    fn weight_at(&self, kind: WeightKind, corr: usize, chan: usize) -> f32 {
        match kind {
            WeightKind::Observed => weight_from_sigma(self.sigma.value(corr, chan)),
            WeightKind::Calibrated => self.weight.value(corr, chan),
            WeightKind::Unit => 1.0,
        }
    }

    fn summed_weights(&self, kind: WeightKind) -> Array2<f32> {
        self.bin_statistics.map(|bin| bin.weights[kind as usize])
    }

    fn mean_visibility<T>(
        &self,
        values: &ArrayD<T>,
        kind: WeightKind,
    ) -> Result<ArrayD<T>, TableError>
    where
        T: Copy + Default + AddAssign + Div<f32, Output = T> + Mul<f32, Output = T>,
    {
        let values = values
            .view()
            .into_dimensionality::<Ix2>()
            .ok()
            .filter(|values| values.dim() == self.input_shape)
            .ok_or_else(|| TableError::Schema("visibility and FLAG shapes differ".to_string()))?;
        let values = ChannelMatrix::new(values);
        Ok(
            Array2::from_shape_fn(self.bin_statistics.raw_dim(), |(corr, bin)| {
                let statistics = &self.bin_statistics[[corr, bin]];
                let normalization = statistics.weights[kind as usize];
                if normalization > 0.0 {
                    let mut sum = T::default();
                    for &chan in &self.bins[bin] {
                        if self.flags.get(corr, chan) == statistics.flagged {
                            sum += values.get(corr, chan) * self.weight_at(kind, corr, chan);
                        }
                    }
                    sum / normalization
                } else {
                    T::default()
                }
            })
            .into_dyn(),
        )
    }

    fn mean_bins(&self, values: &Array2<f32>) -> Array1<f32> {
        Array1::from_shape_fn(values.nrows(), |corr| {
            mean_weight_preferring_unflagged(
                self.bin_statistics
                    .row(corr)
                    .iter()
                    .zip(values.row(corr).iter().copied())
                    .map(|(bin, value)| (bin.flagged, value)),
            )
        })
    }
}

/// Reduce channel weights, using flagged channels only when all are flagged.
pub fn mean_weight_preferring_unflagged(bins: impl Iterator<Item = (bool, f32)>) -> f32 {
    let mut sums = [0.0, 0.0];
    let mut counts = [0, 0];
    for (flagged, value) in bins {
        let index = usize::from(flagged);
        sums[index] += value;
        counts[index] += 1;
    }
    let index = usize::from(counts[0] == 0);
    sums[index] / counts[index].max(1) as f32
}

pub(super) fn unaveraged_spectra(
    weight: &ArrayValue,
    sigma: &ArrayValue,
    spectra: SpectrumCells<'_>,
    flags: &ArrayValue,
    bins: &[Vec<usize>],
    corrected_becomes_data: bool,
) -> Result<(ArrayValue, ArrayValue), TableError> {
    let &[corr, chan] = flags.shape() else {
        return Err(TableError::Schema(
            "FLAG must be rank-2 [corr, chan]".to_string(),
        ));
    };
    super::validate_channel_bins(bins, chan)?;
    let weight = InputStatistic::from_cells(weight, spectra.weight, [corr, chan], "WEIGHT")?;
    let sigma = InputStatistic::from_cells(sigma, spectra.sigma, [corr, chan], "SIGMA")?;
    let weight = Array2::from_shape_fn((corr, bins.len()), |(corr, bin)| {
        weight.value(corr, bins[bin][0])
    });
    let sigma = if corrected_becomes_data {
        weight.mapv(sigma_from_weight)
    } else {
        Array2::from_shape_fn((corr, bins.len()), |(corr, bin)| {
            sigma.value(corr, bins[bin][0])
        })
    };
    Ok((
        ArrayValue::Float32(weight.into_dyn()),
        ArrayValue::Float32(sigma.into_dyn()),
    ))
}

/// Convert a CASA sigma value to its inverse-variance weight.
pub fn weight_from_sigma(sigma: f32) -> f32 {
    if sigma > f32::MIN_POSITIVE {
        1.0 / (sigma * sigma)
    } else {
        0.0
    }
}

/// Convert an inverse-variance weight to a CASA sigma value.
pub fn sigma_from_weight(weight: f32) -> f32 {
    if weight > f32::MIN_POSITIVE {
        1.0 / weight.sqrt()
    } else {
        -1.0
    }
}
