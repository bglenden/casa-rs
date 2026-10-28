// SPDX-License-Identifier: LGPL-3.0-or-later
//! An [`MFrequency`] conversion prepared once for many frequencies.

use super::MeasFrame;
use super::error::MeasureError;
use super::frequency::{FrequencyRef, MFrequency, find_freq_path};

/// The most hops a route between two frequency frames takes.
const MOST_HOPS: usize = FrequencyRef::ALL.len() - 1;

/// A conversion of frequencies from one frame to another, prepared once:
/// the factor of each hop of the route between the frames, evaluated in
/// that hop's frame, so converting a frequency costs one multiplication a
/// hop. A hop's factor is the hop applied to 1 Hz, which every hop's
/// Doppler shift scales linearly.
///
/// The route is the one [`MFrequency::convert_to`] takes. A conversion
/// between two frames can carry a different context for each (a source
/// direction and epoch, and the output's own velocity or position): each
/// hop is evaluated in the source frame, except `LSRK → REST`,
/// `BARY → LSRK`, `BARY → LSRD`, `LSRD → GALACTO`, `BARY → LGROUP`,
/// `BARY → CMB`, `BARY → GEO` and `GEO → TOPO`, which are evaluated in the
/// target frame.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PreparedFrequencyConversion {
    factors: [f64; MOST_HOPS],
    hops: usize,
}

impl PreparedFrequencyConversion {
    /// The conversion from `source` to `target`, the hops evaluated in
    /// `source_frame` or `target_frame` as the type states. Equal frames
    /// convert unchanged and need neither.
    ///
    /// # Errors
    ///
    /// [`MeasureError::MissingFrameData`] when a frame the route needs is
    /// absent; the hop's own error when a frame lacks what it needs.
    pub fn new(
        source: FrequencyRef,
        target: FrequencyRef,
        source_frame: Option<&MeasFrame>,
        target_frame: Option<&MeasFrame>,
    ) -> Result<Self, MeasureError> {
        let mut factors = [1.0; MOST_HOPS];
        if source == target {
            return Ok(Self { factors, hops: 0 });
        }
        let source_frame = source_frame.ok_or(MeasureError::MissingFrameData {
            what: "source frame of a cross-frame frequency conversion",
        })?;
        let target_frame = target_frame.ok_or(MeasureError::MissingFrameData {
            what: "target frame of a cross-frame frequency conversion",
        })?;
        let path = find_freq_path(source, target)?;
        let mut current = source;
        for (hop, next) in path.iter().copied().enumerate() {
            let frame = if uses_target_frame(current, next) {
                target_frame
            } else {
                source_frame
            };
            factors[hop] = MFrequency::new(1.0, current).convert_to(next, frame)?.hz();
            current = next;
        }
        Ok(Self {
            factors,
            hops: path.len(),
        })
    }

    /// `frequency_hz` converted.
    #[must_use]
    pub fn convert_hz(&self, frequency_hz: f64) -> f64 {
        self.factors[..self.hops]
            .iter()
            .fold(frequency_hz, |converted, factor| converted * factor)
    }
}

/// Whether the hop from `from` to `to` is evaluated in the target frame.
const fn uses_target_frame(from: FrequencyRef, to: FrequencyRef) -> bool {
    matches!(
        (from, to),
        (FrequencyRef::LSRK, FrequencyRef::REST)
            | (FrequencyRef::BARY, FrequencyRef::LSRK)
            | (FrequencyRef::BARY, FrequencyRef::LSRD)
            | (FrequencyRef::LSRD, FrequencyRef::GALACTO)
            | (FrequencyRef::BARY, FrequencyRef::LGROUP)
            | (FrequencyRef::BARY, FrequencyRef::CMB)
            | (FrequencyRef::BARY, FrequencyRef::GEO)
            | (FrequencyRef::GEO, FrequencyRef::TOPO)
    )
}

#[cfg(test)]
mod tests {
    use super::PreparedFrequencyConversion;
    use crate::measures::MeasFrame;
    use crate::measures::error::MeasureError;
    use crate::measures::frequency::FrequencyRef;

    /// Equal frames need no frame; different ones need both.
    #[test]
    fn a_cross_frame_conversion_needs_both_frames() {
        let same =
            PreparedFrequencyConversion::new(FrequencyRef::LSRK, FrequencyRef::LSRK, None, None)
                .expect("equal frames");
        assert_eq!(same.convert_hz(6.667_582_296e9), 6.667_582_296e9);
        for (source, target) in [
            (None, Some(MeasFrame::new())),
            (Some(MeasFrame::new()), None),
        ] {
            let missing = PreparedFrequencyConversion::new(
                FrequencyRef::TOPO,
                FrequencyRef::LSRK,
                source.as_ref(),
                target.as_ref(),
            );
            assert!(matches!(
                missing,
                Err(MeasureError::MissingFrameData { .. })
            ));
        }
    }
}
