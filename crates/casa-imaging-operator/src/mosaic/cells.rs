// SPDX-License-Identifier: LGPL-3.0-or-later
//! A mosaic window's frequency cells: the beam frequencies its kernels are
//! built at and the rule that keys a sample's frequency to one of them
//! (`SimplePBConvFunc::findUsefulChannels`).

use super::{MosaicRules, MosaicWindow};
use crate::error::OperatorError;

/// The frequency cells of one window and the rule that maps a frequency
/// to one of them.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct FrequencyCells {
    pub(super) spectral_window: u32,
    /// Beam frequencies in cell order.
    pub(super) frequencies_hz: Vec<f64>,
    /// `Some(tol)` applies CASA's `chanMap` rule (first cell within
    /// `tol/2`, else the nearest); `None` is the identity branch, where a
    /// channel's own frequency is a cell.
    tolerance_hz: Option<f64>,
    /// Whether the `tol/2` test is made in `Float`, as the simulator's
    /// machine makes it.
    single_precision: bool,
    /// Index of the window's first cell in the set's flat cell list.
    pub(super) first: usize,
}

/// A window's selected frequencies, their least and greatest, and their
/// spacing (CASA's `origwidth`, `1e12` for one channel).
struct SelectedSpan<'w> {
    selected: &'w [f64],
    min_freq: f64,
    max_freq: f64,
    original_width: f64,
}

impl<'w> SelectedSpan<'w> {
    fn new(window: &'w MosaicWindow) -> Result<Self, OperatorError> {
        let selected = &window.selected_frequencies_hz;
        if selected.is_empty()
            || window.window_frequencies_hz.is_empty()
            || selected
                .iter()
                .chain(&window.window_frequencies_hz)
                .any(|value| !value.is_finite() || *value <= 0.0)
        {
            return Err(OperatorError::ConvolutionFunction {
                reason: "a mosaic window needs positive finite channel frequencies",
            });
        }
        let min_freq = selected.iter().copied().fold(f64::INFINITY, f64::min);
        let max_freq = selected.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let original_width = if selected.len() == 1 {
            1.0e12
        } else {
            (max_freq - min_freq) / (selected.len() - 1) as f64
        };
        Ok(Self {
            selected,
            min_freq,
            max_freq,
            original_width,
        })
    }
}

impl FrequencyCells {
    /// The window's cells as `rules`' machine finds them.
    pub(super) fn new(
        rules: MosaicRules,
        window: &MosaicWindow,
        first: usize,
    ) -> Result<Self, OperatorError> {
        match rules {
            MosaicRules::Tclean | MosaicRules::TcleanConjugateBeams => Self::tclean(window, first),
            MosaicRules::Simulator => Self::simulator(window, first),
        }
    }

    /// The identity branch: every selected channel its own cell.
    fn identity(window: &MosaicWindow, first: usize) -> Self {
        Self {
            spectral_window: window.spectral_window,
            frequencies_hz: window.selected_frequencies_hz.clone(),
            tolerance_hz: None,
            single_precision: false,
            first,
        }
    }

    /// `SimplePBConvFunc::findUsefulChannels` (`TransformMachines2/`, the
    /// `Vector<Int>& chanMap` overload): cells every `tol = max(0.5 % of
    /// the window's top frequency, half the selected spacing)` stepping
    /// down from the window's top frequency, the identity when the cell
    /// count reaches the selected channel count less one, a single cell at
    /// the bottom frequency when no cell fits.
    fn tclean(window: &MosaicWindow, first: usize) -> Result<Self, OperatorError> {
        let SelectedSpan {
            selected,
            min_freq,
            max_freq,
            original_width,
        } = SelectedSpan::new(window)?;
        let window_top = window
            .window_frequencies_hz
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let mut tol = window_top * 0.5 / 100.0;
        if tol < original_width / 2.0 {
            tol = original_width / 2.0;
        }
        let mut top = window_top;
        while top > max_freq {
            top -= tol;
        }
        if top < min_freq {
            top += tol;
        }
        let mut bottom = top;
        let mut cells = 0_usize;
        while bottom > min_freq {
            cells += 1;
            bottom -= tol;
        }
        if cells > 1 {
            cells -= 1;
            bottom += tol;
        }
        if cells > selected.len() {
            cells = selected.len();
            tol = window.channel_width_hz.abs();
            bottom = min_freq;
        }
        let frequencies_hz = if cells + 1 >= selected.len() {
            return Ok(Self::identity(window, first));
        } else if cells == 0 {
            vec![bottom]
        } else {
            (0..cells).map(|k| k as f64 * tol + bottom).collect()
        };
        Ok(Self {
            spectral_window: window.spectral_window,
            frequencies_hz,
            tolerance_hz: Some(tol),
            single_precision: false,
            first,
        })
    }

    /// The simulator's `SimplePBConvFunc::findUsefulChannels`
    /// (`TransformMachines/`): `lround(span / tol)` cells, at most one a
    /// channel, with `tol` 0.5 % of the top selected frequency; the
    /// identity when that reaches the channel count less one, and one cell
    /// at the first selected frequency when it is zero. Otherwise the cells
    /// are `(span + spacing)/n` apart from half that above a spacing below
    /// the bottom, and a frequency takes the first within half of it, in
    /// `Float`. (CASA leaves a frequency with none unmapped; the cells
    /// cover the selected span, so none is.)
    fn simulator(window: &MosaicWindow, first: usize) -> Result<Self, OperatorError> {
        let SelectedSpan {
            selected,
            min_freq,
            max_freq,
            original_width,
        } = SelectedSpan::new(window)?;
        let tol = max_freq * 0.5 / 100.0;
        let cells = (((max_freq - min_freq) / tol).round() as usize).min(selected.len());
        if cells + 1 >= selected.len() {
            return Ok(Self::identity(window, first));
        }
        let (frequencies_hz, tol) = if cells == 0 {
            (vec![selected[0]], tol.max(original_width))
        } else {
            let tol = (max_freq - min_freq + original_width) / cells as f64;
            let cells = (0..cells)
                .map(|k| min_freq - original_width + tol / 2.0 + tol * k as f64)
                .collect();
            (cells, tol)
        };
        Ok(Self {
            spectral_window: window.spectral_window,
            frequencies_hz,
            tolerance_hz: Some(tol),
            single_precision: true,
            first,
        })
    }

    /// The cell of `freq_hz` within this window.
    pub(super) fn cell_of(&self, freq_hz: f64) -> usize {
        let nearest = || {
            self.frequencies_hz
                .iter()
                .enumerate()
                .fold((0, f64::INFINITY), |best, (index, cell)| {
                    let diff = (freq_hz - cell).abs();
                    if diff < best.1 { (index, diff) } else { best }
                })
                .0
        };
        let within = |cell: &f64, tol: f64| {
            let difference = (freq_hz - cell).abs();
            if self.single_precision {
                difference as f32 <= (tol / 2.0) as f32
            } else {
                difference <= tol / 2.0
            }
        };
        match self.tolerance_hz {
            None => nearest(),
            Some(tol) => self
                .frequencies_hz
                .iter()
                .position(|cell| within(cell, tol))
                .unwrap_or_else(nearest),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(selected: Vec<f64>, window_top: f64, width: f64) -> MosaicWindow {
        MosaicWindow {
            spectral_window: 3,
            window_frequencies_hz: vec![window_top - 10.0 * width, window_top],
            channel_width_hz: width,
            selected_frequencies_hz: selected,
        }
    }

    #[test]
    fn a_single_selected_channel_is_its_own_cell() {
        let cells =
            FrequencyCells::tclean(&window(vec![100.0e9], 101.0e9, 1.0e6), 4).expect("cells");
        assert_eq!(cells.frequencies_hz, vec![100.0e9]);
        assert_eq!(cells.tolerance_hz, None);
        assert_eq!(cells.first, 4);
        assert_eq!(cells.cell_of(99.0e9), 0);
    }

    #[test]
    fn a_wide_window_steps_cells_down_from_its_top_frequency() {
        // Window top 101 GHz: tol = 505 MHz; 8 channels from 98 to 100.1
        // GHz (spacing 300 MHz, half 150 MHz < tol): top steps to
        // 100.495, cells down to 98.475 → 5 steps, then one removed: 4
        // cells at 98.98 + k·0.505 GHz; 4 < 8 − 1 so no identity.
        let selected = (0..8)
            .map(|k| 98.0e9 + k as f64 * 0.3e9)
            .collect::<Vec<_>>();
        let cells = FrequencyCells::tclean(&window(selected, 101.0e9, 1.0e6), 0).expect("cells");
        let tol = 101.0e9 * 0.005;
        let top = 101.0e9 - tol; // 100.495 > 100.1, step again:
        let top = top - tol; // 99.99 ≤ 100.1
        assert_eq!(cells.tolerance_hz, Some(tol));
        // Stepping down from 99.99: 99.99 > 98 (1), 99.485 (2), 98.98 (3),
        // 98.475 (4), 97.97 stops → 4 cells, minus one → 3 cells from
        // 98.475.
        let bottom = top - 3.0 * tol;
        let expected = (0..3).map(|k| bottom + k as f64 * tol).collect::<Vec<_>>();
        for (cell, want) in cells.frequencies_hz.iter().zip(&expected) {
            assert!((cell - want).abs() < 1.0, "{cell} vs {want}");
        }
        assert_eq!(cells.frequencies_hz.len(), 3);
        // chanMap: first cell within tol/2 (252.5 MHz), else nearest.
        assert_eq!(cells.cell_of(98.5e9), 0);
        assert_eq!(cells.cell_of(98.9e9), 1);
        assert_eq!(cells.cell_of(100.1e9), 2);
    }

    /// #713's multi-channel MFS mosaic: 16 channels of 2 MHz at 230.0 GHz in
    /// a window topping at 230.51 GHz. tol = 0.5 % of the top, 1.15255 GHz,
    /// is wider than the band, so the top steps below the band, steps back
    /// up, and the one cell lands a tolerance below the top (229.35745 GHz,
    /// below the band): every channel takes that one beam, in CASA's
    /// `findUsefulChannels` and here.
    #[test]
    fn a_narrow_band_takes_one_cell_a_tolerance_below_the_window_top() {
        let selected = (0..16)
            .map(|k| 230.002e9 + k as f64 * 2.0e6)
            .collect::<Vec<_>>();
        let cells = FrequencyCells::tclean(&window(selected, 230.51e9, 2.0e6), 0).expect("cells");
        let tol = 230.51e9 * 0.005;
        assert_eq!(cells.tolerance_hz, Some(tol));
        assert_eq!(cells.frequencies_hz.len(), 1);
        assert!((cells.frequencies_hz[0] - (230.51e9 - tol)).abs() < 1.0);
        for frequency in [230.002e9, 230.032e9] {
            assert_eq!(cells.cell_of(frequency), 0);
        }
    }

    #[test]
    fn enough_cells_for_the_channels_is_the_identity() {
        // 3 channels 1 GHz apart at 100 GHz: tol 0.5 GHz → cells from
        // 101.5 down past 100 … reaches ≥ 2 cells = channels − 1 → identity.
        let selected = vec![100.0e9, 101.0e9, 102.0e9];
        let cells =
            FrequencyCells::tclean(&window(selected.clone(), 102.0e9, 1.0e6), 0).expect("cells");
        assert_eq!(cells.frequencies_hz, selected);
        assert_eq!(cells.tolerance_hz, None);
        assert_eq!(cells.cell_of(101.2e9), 1);
    }
}
