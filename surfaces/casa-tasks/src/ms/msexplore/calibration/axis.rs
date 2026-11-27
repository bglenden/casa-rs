// SPDX-License-Identifier: LGPL-3.0-or-later
//! Coordinates and parameter projections for PlotMS calibration tables.

use num_complex::Complex64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::ms::msexplore) enum Axis {
    Channel,
    Frequency,
    Time,
    Interval,
    Scan,
    Field,
    Window,
    Row,
    Antenna1,
    Antenna2,
    Feed1,
    Receptor,
    Observation,
    Amplitude,
    Phase,
    Real,
    Imaginary,
    GainAmplitude,
    GainPhase,
    GainReal,
    GainImaginary,
    SignalToNoise,
    Flag,
    Delay,
    Rate,
    DispersiveDelay,
    SystemTemperature,
    Opacity,
    SwitchedPower,
    Tec,
    AntennaPosition,
    AtmosphericTransmission,
    SkyTemperature,
    ImageSideband,
}

impl Axis {
    pub(super) fn parse(value: &str) -> Result<Self, String> {
        use Axis::*;
        let normalized = value.to_ascii_lowercase().replace([' ', '-'], "");
        let axis = match normalized.as_str() {
            "chan" | "channel" => Channel,
            "freq" | "frequency" => Frequency,
            "time" => Time,
            "interval" | "timeint" | "timeinterval" | "time_interval" => Interval,
            "scan" => Scan,
            "field" => Field,
            "spw" => Window,
            "row" => Row,
            "antenna" | "antenna1" | "ant" | "ant1" => Antenna1,
            "antenna2" | "ant2" => Antenna2,
            "feed1" => Feed1,
            "poln" | "polarization" | "corr" | "correlation" => Receptor,
            "observation" => Observation,
            "amp" | "amplitude" => Amplitude,
            "phase" => Phase,
            "real" => Real,
            "imag" | "imaginary" => Imaginary,
            "gainamp" | "gamp" => GainAmplitude,
            "gainphase" | "gphase" => GainPhase,
            "gainreal" | "greal" => GainReal,
            "gainimag" | "gimag" => GainImaginary,
            "snr" => SignalToNoise,
            "flag" => Flag,
            "delay" | "del" => Delay,
            "rate" | "delayrate" => Rate,
            "disp" | "dispdelay" => DispersiveDelay,
            "tsys" => SystemTemperature,
            "opacity" | "opac" => Opacity,
            "swp" | "swpower" | "switchedpower" | "spgain" => SwitchedPower,
            "tec" => Tec,
            "antennaposition" | "antpos" => AntennaPosition,
            _ => return Err(format!("unknown calibration plot axis {value:?}")),
        };
        Ok(axis)
    }

    pub(in crate::ms::msexplore) fn label(self) -> &'static str {
        use Axis::*;
        match self {
            Channel => "Channel",
            Frequency => "Frequency",
            Time => "Time",
            Interval => "Interval",
            Scan => "Scan",
            Field => "Field",
            Window => "Spw",
            Row => "Row",
            Antenna1 => "Antenna1",
            Antenna2 => "Antenna2",
            Feed1 => "Feed1",
            Receptor => "Poln",
            Observation => "Observation",
            Amplitude => "Amp",
            Phase => "Phase",
            Real => "Real",
            Imaginary => "Imag",
            GainAmplitude => "Gain Amp",
            GainPhase => "Gain Phase",
            GainReal => "Gain Real",
            GainImaginary => "Gain Imag",
            SignalToNoise => "SNR",
            Flag => "Flag",
            Delay => "Delay",
            Rate => "Delay Rate",
            DispersiveDelay => "Disp Delay",
            SystemTemperature => "Tsys",
            Opacity => "Opacity",
            SwitchedPower => "Switched Power",
            Tec => "TEC",
            AntennaPosition => "Antenna Position",
            AtmosphericTransmission => "Atm Transmission",
            SkyTemperature => "Tsky",
            ImageSideband => "Image Sideband",
        }
    }

    pub(in crate::ms::msexplore) fn unit(self) -> &'static str {
        match self {
            Self::Frequency => "GHz",
            Self::Time => "MJD seconds",
            Self::Interval => "s",
            Self::Phase | Self::GainPhase => "deg",
            Self::Delay => "ns",
            Self::Rate => "ps/s",
            Self::SystemTemperature | Self::SkyTemperature => "K",
            Self::AtmosphericTransmission => "%",
            Self::AntennaPosition => "m",
            _ => "",
        }
    }

    pub(super) fn value(self, metadata: &SampleCoordinates, value: Complex64, snr: f64) -> f64 {
        use Axis::*;
        match self {
            Channel => metadata.channel as f64,
            Frequency => metadata.frequency_ghz,
            Time => metadata.time,
            Interval => metadata.interval,
            Scan => f64::from(metadata.scan),
            Field => f64::from(metadata.field),
            Window => f64::from(metadata.window),
            Row => metadata.row as f64,
            Antenna1 => f64::from(metadata.antennas[0]),
            Antenna2 => f64::from(metadata.antennas[1]),
            Feed1 => f64::from(metadata.feed1),
            Receptor => metadata.receptor as f64,
            Observation => f64::from(metadata.observation),
            Amplitude | GainAmplitude => value.norm(),
            Phase | GainPhase => value.arg().to_degrees(),
            Imaginary | GainImaginary => value.im,
            SignalToNoise => snr,
            Real
            | GainReal
            | Delay
            | Rate
            | DispersiveDelay
            | SystemTemperature
            | Opacity
            | SwitchedPower
            | Tec
            | AntennaPosition
            | Flag
            | AtmosphericTransmission
            | SkyTemperature
            | ImageSideband => value.re,
        }
    }

    pub(super) fn uses_receptor(self) -> bool {
        use Axis::*;
        matches!(
            self,
            Receptor
                | Amplitude
                | Phase
                | Real
                | Imaginary
                | GainAmplitude
                | GainPhase
                | GainReal
                | GainImaginary
                | SignalToNoise
                | Flag
        )
    }

    pub(super) fn uses_spline_antenna(self) -> bool {
        self == Self::Antenna1 || (self != Self::Receptor && self.uses_receptor())
    }
}

#[derive(Clone, Copy)]
pub(super) struct SampleCoordinates {
    pub(super) row: usize,
    pub(super) channel: usize,
    pub(super) receptor: i32,
    pub(super) frequency_ghz: f64,
    pub(super) time: f64,
    pub(super) interval: f64,
    pub(super) scan: i32,
    pub(super) field: i32,
    pub(super) window: i32,
    pub(super) antennas: [i32; 2],
    pub(super) feed1: i32,
    pub(super) observation: i32,
}
