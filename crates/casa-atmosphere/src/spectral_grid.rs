// SPDX-License-Identifier: LGPL-3.0-or-later
// Ported from ATM's ATMSpectralGrid.cc, (c) Instituto de Estructura de la
// Materia, Pardo et al., LGPL-2.1-or-later.
//! The frequencies ATM evaluates the atmosphere at (ATM's `SpectralGrid`).
//!
//! A grid is a list of spectral windows, each a list of channel
//! frequencies. A regular window is defined, as in ATM, by its channel
//! count, a 0-based reference channel, the reference channel's frequency
//! and the channel separation; channel `i` is at
//! `(f_ref − Δf·i_ref) + i·Δf`. The separation may be negative. ATM's
//! sideband pairing (a DSB window and its image) is not ported.

use crate::AtmosphereError;

/// One spectral window: its channel frequencies and reference channel.
#[derive(Clone, Debug, PartialEq)]
pub struct SpectralWindow {
    frequencies_hz: Vec<f64>,
    reference_channel: usize,
    reference_frequency_hz: f64,
    channel_separation_hz: f64,
}

impl SpectralWindow {
    /// The channel frequencies, Hz.
    #[must_use]
    pub fn frequencies_hz(&self) -> &[f64] {
        &self.frequencies_hz
    }

    /// The number of channels.
    #[must_use]
    pub fn num_channels(&self) -> usize {
        self.frequencies_hz.len()
    }

    /// The 0-based reference channel.
    #[must_use]
    pub fn reference_channel(&self) -> usize {
        self.reference_channel
    }

    /// The reference channel's frequency, Hz.
    #[must_use]
    pub fn reference_frequency_hz(&self) -> f64 {
        self.reference_frequency_hz
    }

    /// The separation of consecutive channels, Hz; 0 for an irregular
    /// window (as ATM records it).
    #[must_use]
    pub fn channel_separation_hz(&self) -> f64 {
        self.channel_separation_hz
    }

    /// The lowest channel frequency, Hz.
    #[must_use]
    pub fn min_frequency_hz(&self) -> f64 {
        self.frequencies_hz
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min)
    }

    /// The highest channel frequency, Hz.
    #[must_use]
    pub fn max_frequency_hz(&self) -> f64 {
        self.frequencies_hz
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max)
    }

    /// The width of channel `channel`, Hz, as ATM's `getChanWidth` gives it:
    /// the difference from the previous channel, or to the next for
    /// channel 0. `None` for a one-channel window or a channel past the end.
    #[must_use]
    pub fn channel_width_hz(&self, channel: usize) -> Option<f64> {
        let f = &self.frequencies_hz;
        match channel {
            0 => Some(f.get(1)? - f[0]),
            c => Some(f.get(c)? - f[c - 1]),
        }
    }
}

/// The spectral windows ATM evaluates the atmosphere at.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SpectralGrid {
    windows: Vec<SpectralWindow>,
}

impl SpectralGrid {
    /// An empty grid.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A grid of one window of one channel at `frequency_hz`.
    ///
    /// # Errors
    ///
    /// As [`SpectralGrid::add_regular`].
    pub fn single(frequency_hz: f64) -> Result<Self, AtmosphereError> {
        let mut grid = Self::new();
        grid.add_regular(1, 0, frequency_hz, 0.0)?;
        Ok(grid)
    }

    /// Adds a regular window of `num_channels` channels whose channel
    /// `reference_channel` (0-based) is at `reference_frequency_hz`, spaced
    /// by `channel_separation_hz`. Returns the window's index.
    ///
    /// # Errors
    ///
    /// [`AtmosphereError::InvalidParameter`] for no channels, a reference
    /// channel past the end, or a non-finite frequency or separation.
    pub fn add_regular(
        &mut self,
        num_channels: usize,
        reference_channel: usize,
        reference_frequency_hz: f64,
        channel_separation_hz: f64,
    ) -> Result<usize, AtmosphereError> {
        if num_channels == 0 {
            return Err(AtmosphereError::InvalidParameter(
                "a spectral window needs channels".into(),
            ));
        }
        if reference_channel >= num_channels {
            return Err(AtmosphereError::InvalidParameter(format!(
                "reference channel {reference_channel} is past the {num_channels} channels"
            )));
        }
        if !reference_frequency_hz.is_finite() || !channel_separation_hz.is_finite() {
            return Err(AtmosphereError::InvalidParameter(
                "frequencies must be finite".into(),
            ));
        }
        let offset = reference_frequency_hz - channel_separation_hz * reference_channel as f64;
        let frequencies_hz = (0..num_channels)
            .map(|i| offset + i as f64 * channel_separation_hz)
            .collect();
        self.windows.push(SpectralWindow {
            frequencies_hz,
            reference_channel,
            reference_frequency_hz,
            channel_separation_hz,
        });
        Ok(self.windows.len() - 1)
    }

    /// Adds a window of explicit channel frequencies (Hz) with 0-based
    /// reference channel `reference_channel`. Returns the window's index.
    ///
    /// The separation is recorded as ATM records it: the first difference
    /// when every difference is within 1e-12 Hz of it, otherwise 0.
    ///
    /// # Errors
    ///
    /// [`AtmosphereError::InvalidParameter`] for no channels, a reference
    /// channel past the end, or a non-finite frequency.
    pub fn add_channels(
        &mut self,
        frequencies_hz: &[f64],
        reference_channel: usize,
    ) -> Result<usize, AtmosphereError> {
        if frequencies_hz.is_empty() {
            return Err(AtmosphereError::InvalidParameter(
                "a spectral window needs channels".into(),
            ));
        }
        if reference_channel >= frequencies_hz.len() {
            return Err(AtmosphereError::InvalidParameter(format!(
                "reference channel {reference_channel} is past the {} channels",
                frequencies_hz.len()
            )));
        }
        if frequencies_hz.iter().any(|f| !f.is_finite()) {
            return Err(AtmosphereError::InvalidParameter(
                "frequencies must be finite".into(),
            ));
        }
        let separation = frequencies_hz.get(1).map_or(0.0, |f| f - frequencies_hz[0]);
        let regular = frequencies_hz
            .windows(2)
            .all(|pair| (pair[1] - pair[0] - separation).abs() <= 1e-12);
        self.windows.push(SpectralWindow {
            frequencies_hz: frequencies_hz.to_vec(),
            reference_channel,
            reference_frequency_hz: frequencies_hz[reference_channel],
            channel_separation_hz: if regular { separation } else { 0.0 },
        });
        Ok(self.windows.len() - 1)
    }

    /// The windows, in the order added.
    #[must_use]
    pub fn windows(&self) -> &[SpectralWindow] {
        &self.windows
    }

    /// Releases spare vector capacity, so the grid keeps exactly its
    /// windows and their channels (what a profile's memory estimate counts).
    pub(crate) fn shrink_to_fit(&mut self) {
        self.windows.shrink_to_fit();
        for window in &mut self.windows {
            window.frequencies_hz.shrink_to_fit();
        }
    }

    /// The number of channels in all windows.
    #[must_use]
    pub fn total_channels(&self) -> usize {
        self.windows.iter().map(SpectralWindow::num_channels).sum()
    }

    /// Every channel frequency (Hz), window by window: ATM's flat channel
    /// index order.
    pub fn frequencies_hz(&self) -> impl Iterator<Item = f64> + '_ {
        self.windows
            .iter()
            .flat_map(|window| window.frequencies_hz.iter().copied())
    }

    /// The flat index of channel `channel` of window `window`, if both
    /// exist.
    #[must_use]
    pub fn flat_index(&self, window: usize, channel: usize) -> Option<usize> {
        let target = self.windows.get(window)?;
        if channel >= target.num_channels() {
            return None;
        }
        Some(
            self.windows[..window]
                .iter()
                .map(SpectralWindow::num_channels)
                .sum::<usize>()
                + channel,
        )
    }
}
