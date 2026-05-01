// SPDX-License-Identifier: LGPL-3.0-or-later
//! Logical-record decoding helpers for VLA export archives.

use casa_types::Complex32;

use crate::VlaError;
use crate::modcomp::{decode_f32, decode_f64, decode_i16, decode_i32, decode_u16, decode_u32};

/// A minimal random-access cursor over VLA logical-record bytes.
#[derive(Debug, Clone, Copy)]
pub struct ModcompCursor<'a> {
    bytes: &'a [u8],
}

impl<'a> ModcompCursor<'a> {
    /// Create a new cursor over immutable bytes.
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Read a signed 32-bit integer from `offset`.
    pub fn i32_at(&self, offset: usize) -> Result<i32, String> {
        decode_i32(self.slice(offset, 4)?)
    }

    /// Read an unsigned 32-bit integer from `offset`.
    pub fn u32_at(&self, offset: usize) -> Result<u32, String> {
        decode_u32(self.slice(offset, 4)?)
    }

    /// Read a signed 16-bit integer from `offset`.
    pub fn i16_at(&self, offset: usize) -> Result<i16, String> {
        decode_i16(self.slice(offset, 2)?)
    }

    /// Read an unsigned 16-bit integer from `offset`.
    pub fn u16_at(&self, offset: usize) -> Result<u16, String> {
        decode_u16(self.slice(offset, 2)?)
    }

    /// Read a signed 8-bit integer from `offset`.
    pub fn i8_at(&self, offset: usize) -> Result<i8, String> {
        Ok(self.u8_at(offset)? as i8)
    }

    /// Read an unsigned 8-bit integer from `offset`.
    pub fn u8_at(&self, offset: usize) -> Result<u8, String> {
        Ok(self.slice(offset, 1)?[0])
    }

    /// Read a ModComp single-precision float from `offset`.
    pub fn f32_at(&self, offset: usize) -> Result<f32, String> {
        decode_f32(self.slice(offset, 4)?)
    }

    /// Read a ModComp double-precision float from `offset`.
    pub fn f64_at(&self, offset: usize) -> Result<f64, String> {
        decode_f64(self.slice(offset, 8)?)
    }

    /// Read a fixed-width ASCII field from `offset`.
    pub fn ascii_at(&self, offset: usize, len: usize) -> Result<String, String> {
        let bytes = self.slice(offset, len)?;
        Ok(String::from_utf8_lossy(bytes).into_owned())
    }

    /// Borrow a raw slice from `offset`.
    pub fn bytes_at(&self, offset: usize, len: usize) -> Result<&'a [u8], String> {
        self.slice(offset, len)
    }

    fn slice(&self, offset: usize, len: usize) -> Result<&'a [u8], String> {
        self.bytes.get(offset..offset + len).ok_or_else(|| {
            format!(
                "logical record too short: need bytes [{offset}..{}), have {}",
                offset + len,
                self.bytes.len()
            )
        })
    }
}

/// Record Control Area (RCA) view onto a logical record.
///
/// This mirrors the subset of CASA's `VLARCA` API needed for archive reading
/// and later filler work.
#[derive(Debug, Clone, Copy)]
pub struct RecordControlArea<'a> {
    cursor: ModcompCursor<'a>,
}

impl<'a> RecordControlArea<'a> {
    /// Construct an RCA view over the provided logical-record bytes.
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            cursor: ModcompCursor::new(bytes),
        }
    }

    /// Length of the logical record in bytes.
    pub fn length_bytes(&self) -> Result<u32, String> {
        let words = self.cursor.i32_at(0)?;
        if words <= 0 {
            return Err(format!("invalid logical-record length in words: {words}"));
        }
        Ok((words as u32) * 2)
    }

    /// Archive revision number.
    pub fn revision(&self) -> Result<u16, String> {
        self.cursor.u16_at(2 * 3)
    }

    /// Observation day in archive day units.
    pub fn obs_day(&self) -> Result<u32, String> {
        self.cursor.u32_at(2 * 4)
    }

    /// Offset to the SDA in bytes.
    pub fn sda_offset_bytes(&self) -> Result<u32, String> {
        Ok(self.cursor.u32_at(2 * 12)? * 2)
    }

    /// Offset to the selected ADA in bytes.
    pub fn ada_offset_bytes(&self, which: usize) -> Result<u32, String> {
        let antennas = self.n_antennas()? as usize;
        if which >= antennas {
            return Err(format!("ADA index out of range: {which} >= {antennas}"));
        }
        let offset = self.cursor.u32_at(2 * 14)?;
        if which == 0 {
            return Ok(offset * 2);
        }
        Ok((offset + which as u32 * self.ada_size_words()? as u32) * 2)
    }

    /// Size of each ADA in bytes.
    pub fn ada_size_bytes(&self) -> Result<u16, String> {
        Ok(self.ada_size_words()? * 2)
    }

    /// Number of antennas in this logical record.
    pub fn n_antennas(&self) -> Result<u16, String> {
        self.cursor.u16_at(2 * 17)
    }

    /// Offset to the selected CDA in bytes.
    pub fn cda_offset_bytes(&self, which: usize) -> Result<u32, String> {
        if which >= 4 {
            return Err(format!("CDA index out of range: {which}"));
        }
        Ok(self.cursor.u32_at(2 * (which * 4 + 18))? * 2)
    }

    /// Header size of the selected CDA baseline record in bytes.
    pub fn cda_header_bytes(&self, which: usize) -> Result<u16, String> {
        if which >= 4 {
            return Err(format!("CDA index out of range: {which}"));
        }
        Ok(self.cursor.u16_at(2 * (which * 4 + 20))? * 2)
    }

    /// Total size of the selected CDA baseline record in bytes.
    pub fn cda_baseline_bytes(&self, which: usize) -> Result<u16, String> {
        if which >= 4 {
            return Err(format!("CDA index out of range: {which}"));
        }
        Ok(self.cursor.u16_at(2 * (which * 4 + 21))? * 2)
    }

    /// Count the number of CDAs present in the logical record.
    pub fn used_cda_count(&self) -> Result<usize, String> {
        let mut count = 0;
        for which in 0..4 {
            if self.cda_offset_bytes(which)? != 0 {
                count += 1;
            }
        }
        Ok(count)
    }

    fn ada_size_words(&self) -> Result<u16, String> {
        self.cursor.u16_at(2 * 16)
    }
}

/// VLA IF identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IfId {
    /// IF A.
    A,
    /// IF B.
    B,
    /// IF C.
    C,
    /// IF D.
    D,
}

impl IfId {
    /// Zero-based IF index.
    pub fn index(self) -> usize {
        match self {
            Self::A => 0,
            Self::B => 1,
            Self::C => 2,
            Self::D => 3,
        }
    }
}

/// VLA CDA identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdaId {
    /// CDA 0.
    Cda0,
    /// CDA 1.
    Cda1,
    /// CDA 2.
    Cda2,
    /// CDA 3.
    Cda3,
}

impl CdaId {
    /// Zero-based CDA index.
    pub fn index(self) -> usize {
        match self {
            Self::Cda0 => 0,
            Self::Cda1 => 1,
            Self::Cda2 => 2,
            Self::Cda3 => 3,
        }
    }
}

/// Correlator mode codes from CASA's `VLAEnum::CorrMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorrelatorMode {
    /// Continuum mode.
    Continuum,
    /// One spectral product using IF A.
    A,
    /// One spectral product using IF B.
    B,
    /// One spectral product using IF C.
    C,
    /// One spectral product using IF D.
    D,
    /// Two spectral products using IFs A and B.
    Ab,
    /// Two spectral products using IFs A and C.
    Ac,
    /// Two spectral products using IFs A and D.
    Ad,
    /// Two spectral products using IFs B and C.
    Bc,
    /// Two spectral products using IFs B and D.
    Bd,
    /// Two spectral products using IFs C and D.
    Cd,
    /// Four spectral products using IFs A, B, C, and D.
    Abcd,
    /// Full-polarization spectral mode for A/C.
    Pa,
    /// Full-polarization spectral mode for B/D.
    Pb,
    /// Unknown or unsupported mode string.
    Unknown,
}

/// Frequency rest-frame code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrequencyFrame {
    /// Topocentric.
    Topocentric,
    /// Geocentric.
    Geocentric,
    /// Barycentric.
    Barycentric,
    /// LSRK.
    Lsrk,
}

/// Doppler-definition code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DopplerDefinition {
    /// Optical convention.
    Optical,
    /// Radio convention.
    Radio,
    /// Unspecified.
    Unknown,
}

/// Direction epoch code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectionEpoch {
    /// J2000.
    J2000,
    /// CASA's historic `B1950_VLA`.
    B1950Vla,
    /// Apparent.
    Apparent,
    /// Any unrecognized year code.
    Unknown(i16),
}

/// Circular IF polarization for an antenna path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircularPolarization {
    /// Right circular.
    Right,
    /// Left circular.
    Left,
}

/// Baseline Stokes product derived from IF usage and transfer-switch state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StokesProduct {
    /// RR.
    Rr,
    /// LL.
    Ll,
    /// RL.
    Rl,
    /// LR.
    Lr,
}

/// IF usage for one correlation product.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IfUsage {
    /// IF from antenna 1.
    pub ant1: IfId,
    /// IF from antenna 2.
    pub ant2: IfId,
}

/// SDA view onto a logical record.
#[derive(Debug, Clone, Copy)]
pub struct SubarrayDataArea<'a> {
    cursor: ModcompCursor<'a>,
}

impl<'a> SubarrayDataArea<'a> {
    /// Construct an SDA view over the logical-record bytes and SDA offset.
    pub fn new(record_bytes: &'a [u8], offset_bytes: u32) -> Result<Self, String> {
        let offset = offset_bytes as usize;
        let bytes = record_bytes
            .get(offset..)
            .ok_or_else(|| format!("SDA offset out of range: {offset}"))?;
        Ok(Self {
            cursor: ModcompCursor::new(bytes),
        })
    }

    /// Construct the SDA directly from a full logical record.
    pub fn from_record(record_bytes: &'a [u8]) -> Result<Self, String> {
        let rca = RecordControlArea::new(record_bytes);
        Self::new(record_bytes, rca.sda_offset_bytes()?)
    }

    /// Number of archived channels for the CDA, including channel zero.
    pub fn true_channels(&self, cda: CdaId) -> Result<u32, String> {
        let offset = 2 * 18 + if cda.index() > 1 { 1 } else { 0 };
        let byte = self.cursor.u8_at(offset)?;
        let exponent = match cda {
            CdaId::Cda0 | CdaId::Cda2 => (byte & 0xf0) >> 4,
            CdaId::Cda1 | CdaId::Cda3 => byte & 0x0f,
        };
        Ok(1_u32 << exponent)
    }

    /// Number of spectral channels exposed to the filler, excluding channel zero.
    pub fn n_channels(&self, cda: CdaId) -> Result<u32, String> {
        let channels = self.true_channels(cda)?;
        Ok(if channels > 1 { channels - 1 } else { channels })
    }

    /// Return the observed center frequency in Hz.
    pub fn observed_frequency_hz(&self, cda: CdaId) -> Result<f64, String> {
        Ok(self.edge_frequency_hz(cda)?
            + self.n_channels(cda)? as f64 / 2.0 * self.channel_width_hz(cda)?)
    }

    /// Return the lower edge frequency of the archived band in Hz.
    pub fn edge_frequency_hz(&self, cda: CdaId) -> Result<f64, String> {
        let mut which = cda.index();
        match self.correlator_mode()? {
            CorrelatorMode::Pa => which = 0,
            CorrelatorMode::Pb => which = 1,
            _ => {}
        }

        let mut edge = self.cursor.f64_at(2 * (40 + which * 4))? * 1.0e9;
        let center = self.cursor.f64_at(2 * (56 + which * 4))? * 1.0e9;
        if edge > center {
            edge -= self.correlated_bandwidth_hz(cda)?;
        }

        if self.n_channels(cda)? > 1 {
            let offset = self.cursor.u16_at(2 * (162 + which))?;
            if offset > 0 {
                edge += f64::from(offset) * self.channel_width_hz(cda)?;
            }
            edge += 0.5 * self.channel_width_hz(cda)?;
        }
        Ok(edge)
    }

    /// Whether Doppler tracking was enabled for the CDA.
    pub fn doppler_tracking(&self, cda: CdaId) -> Result<bool, String> {
        let code = self.cursor.u8_at(2 * (153 + cda.index()) + 1)? as char;
        Ok(!matches!(code, 'F' | ' '))
    }

    /// Observer-supplied rest frequency in Hz.
    pub fn rest_frequency_hz(&self, cda: CdaId) -> Result<f64, String> {
        Ok(self.cursor.f64_at(2 * (137 + cda.index() * 4))? * 1.0e6)
    }

    /// Observer-supplied radial velocity in m/s.
    pub fn radial_velocity_mps(&self, cda: CdaId) -> Result<f64, String> {
        Ok(self.cursor.f64_at(2 * (121 + cda.index() * 4))? * 1.0e3)
    }

    /// Rest-frame code for the Doppler-tracked frequency.
    pub fn rest_frame(&self, cda: CdaId) -> Result<FrequencyFrame, String> {
        let code = self.cursor.u8_at(2 * (153 + cda.index()))? as char;
        Ok(match code {
            'G' => FrequencyFrame::Geocentric,
            'B' => FrequencyFrame::Barycentric,
            'L' => FrequencyFrame::Lsrk,
            _ => FrequencyFrame::Topocentric,
        })
    }

    /// Doppler convention used for the radial velocity.
    pub fn doppler_definition(&self, cda: CdaId) -> Result<DopplerDefinition, String> {
        let code = self.cursor.u8_at(2 * (153 + cda.index()) + 1)? as char;
        Ok(match code {
            'Z' => DopplerDefinition::Optical,
            'V' => DopplerDefinition::Radio,
            _ => DopplerDefinition::Unknown,
        })
    }

    /// Archived channel width in Hz.
    pub fn channel_width_hz(&self, cda: CdaId) -> Result<f64, String> {
        if self.true_channels(cda)? == 1 {
            return self.correlated_bandwidth_hz(cda);
        }
        let exponent = self.cursor.i16_at(2 * (166 + cda.index()))?;
        Ok(50.0 / f64::from(1_i32 << exponent) * 1.0e6)
    }

    /// Total correlated bandwidth in Hz.
    pub fn correlated_bandwidth_hz(&self, cda: CdaId) -> Result<f64, String> {
        let code = self.bandwidth_nibble(cda, 2 * 100)?;
        if self.true_channels(cda)? > 1 {
            Ok(match code {
                7 => 25.0 / 32.0 * 1.0e6,
                9 => 25.0 / 128.0 * 1.0e6,
                _ => 50.0 / f64::from(1_u32 << code) * 1.0e6,
            })
        } else {
            Ok(match code {
                7 => 25.0 / 128.0 * 1.0e6,
                8 => 70.0 * 1.0e6,
                9 => f64::INFINITY,
                _ => 50.0 / f64::from(1_u32 << code) * 1.0e6,
            })
        }
    }

    /// Front-end filter bandwidth in Hz.
    pub fn filter_bandwidth_hz(&self, cda: CdaId) -> Result<f64, String> {
        let code = self.bandwidth_nibble(cda, 2 * 101)?;
        Ok(match code {
            4 => f64::NAN,
            3 => 1.0e200,
            _ => 50.0 / f64::from(1_u32 << code) * 1.0e6,
        })
    }

    /// Correlator mode for the record.
    pub fn correlator_mode(&self) -> Result<CorrelatorMode, String> {
        let raw = self.cursor.ascii_at(2 * 157, 4)?;
        let trimmed = raw.trim_end_matches(' ');
        Ok(match trimmed {
            "" => CorrelatorMode::Continuum,
            "1A" => CorrelatorMode::A,
            "1B" => CorrelatorMode::B,
            "1C" => CorrelatorMode::C,
            "1D" => CorrelatorMode::D,
            "2AB" => CorrelatorMode::Ab,
            "2AC" => CorrelatorMode::Ac,
            "2AD" | "2A" | "2D" => CorrelatorMode::Ad,
            "2BC" => CorrelatorMode::Bc,
            "2BD" | "2B" => CorrelatorMode::Bd,
            "2CD" | "2C" => CorrelatorMode::Cd,
            "4" => CorrelatorMode::Abcd,
            "PA" => CorrelatorMode::Pa,
            "PB" => CorrelatorMode::Pb,
            _ => CorrelatorMode::Unknown,
        })
    }

    /// Number of polarization products in the CDA.
    pub fn n_polarizations(&self, cda: CdaId) -> Result<u32, String> {
        Ok(self.if_usage(cda)?.len() as u32)
    }

    /// IF usage per correlation product for the CDA.
    pub fn if_usage(&self, cda: CdaId) -> Result<Vec<IfUsage>, String> {
        Ok(match self.correlator_mode()? {
            CorrelatorMode::Continuum => match cda {
                CdaId::Cda0 => vec![
                    IfUsage {
                        ant1: IfId::A,
                        ant2: IfId::A,
                    },
                    IfUsage {
                        ant1: IfId::C,
                        ant2: IfId::C,
                    },
                    IfUsage {
                        ant1: IfId::A,
                        ant2: IfId::C,
                    },
                    IfUsage {
                        ant1: IfId::C,
                        ant2: IfId::A,
                    },
                ],
                CdaId::Cda1 => vec![
                    IfUsage {
                        ant1: IfId::B,
                        ant2: IfId::B,
                    },
                    IfUsage {
                        ant1: IfId::D,
                        ant2: IfId::D,
                    },
                    IfUsage {
                        ant1: IfId::B,
                        ant2: IfId::D,
                    },
                    IfUsage {
                        ant1: IfId::D,
                        ant2: IfId::B,
                    },
                ],
                _ => Vec::new(),
            },
            CorrelatorMode::A => single_if_usage(cda, CdaId::Cda0, IfId::A),
            CorrelatorMode::B => single_if_usage(cda, CdaId::Cda1, IfId::B),
            CorrelatorMode::C => single_if_usage(cda, CdaId::Cda2, IfId::C),
            CorrelatorMode::D => single_if_usage(cda, CdaId::Cda3, IfId::D),
            CorrelatorMode::Ab => match cda {
                CdaId::Cda0 => vec![IfUsage {
                    ant1: IfId::A,
                    ant2: IfId::A,
                }],
                CdaId::Cda1 => vec![IfUsage {
                    ant1: IfId::B,
                    ant2: IfId::B,
                }],
                _ => Vec::new(),
            },
            CorrelatorMode::Ac => match cda {
                CdaId::Cda0 => vec![IfUsage {
                    ant1: IfId::A,
                    ant2: IfId::A,
                }],
                CdaId::Cda2 => vec![IfUsage {
                    ant1: IfId::C,
                    ant2: IfId::C,
                }],
                _ => Vec::new(),
            },
            CorrelatorMode::Ad => match cda {
                CdaId::Cda0 => vec![IfUsage {
                    ant1: IfId::A,
                    ant2: IfId::A,
                }],
                CdaId::Cda3 => vec![IfUsage {
                    ant1: IfId::D,
                    ant2: IfId::D,
                }],
                _ => Vec::new(),
            },
            CorrelatorMode::Bc => match cda {
                CdaId::Cda1 => vec![IfUsage {
                    ant1: IfId::B,
                    ant2: IfId::B,
                }],
                CdaId::Cda2 => vec![IfUsage {
                    ant1: IfId::C,
                    ant2: IfId::C,
                }],
                _ => Vec::new(),
            },
            CorrelatorMode::Bd => match cda {
                CdaId::Cda1 => vec![IfUsage {
                    ant1: IfId::B,
                    ant2: IfId::B,
                }],
                CdaId::Cda3 => vec![IfUsage {
                    ant1: IfId::D,
                    ant2: IfId::D,
                }],
                _ => Vec::new(),
            },
            CorrelatorMode::Cd => match cda {
                CdaId::Cda2 => vec![IfUsage {
                    ant1: IfId::C,
                    ant2: IfId::C,
                }],
                CdaId::Cda3 => vec![IfUsage {
                    ant1: IfId::D,
                    ant2: IfId::D,
                }],
                _ => Vec::new(),
            },
            CorrelatorMode::Abcd => match cda {
                CdaId::Cda0 => vec![IfUsage {
                    ant1: IfId::A,
                    ant2: IfId::A,
                }],
                CdaId::Cda1 => vec![IfUsage {
                    ant1: IfId::B,
                    ant2: IfId::B,
                }],
                CdaId::Cda2 => vec![IfUsage {
                    ant1: IfId::C,
                    ant2: IfId::C,
                }],
                CdaId::Cda3 => vec![IfUsage {
                    ant1: IfId::D,
                    ant2: IfId::D,
                }],
            },
            CorrelatorMode::Pa => match cda {
                CdaId::Cda0 => vec![IfUsage {
                    ant1: IfId::A,
                    ant2: IfId::A,
                }],
                CdaId::Cda1 => vec![IfUsage {
                    ant1: IfId::C,
                    ant2: IfId::C,
                }],
                CdaId::Cda2 => vec![IfUsage {
                    ant1: IfId::A,
                    ant2: IfId::C,
                }],
                CdaId::Cda3 => vec![IfUsage {
                    ant1: IfId::C,
                    ant2: IfId::A,
                }],
            },
            CorrelatorMode::Pb => match cda {
                CdaId::Cda0 => vec![IfUsage {
                    ant1: IfId::B,
                    ant2: IfId::B,
                }],
                CdaId::Cda1 => vec![IfUsage {
                    ant1: IfId::D,
                    ant2: IfId::D,
                }],
                CdaId::Cda2 => vec![IfUsage {
                    ant1: IfId::B,
                    ant2: IfId::D,
                }],
                CdaId::Cda3 => vec![IfUsage {
                    ant1: IfId::D,
                    ant2: IfId::B,
                }],
            },
            CorrelatorMode::Unknown => Vec::new(),
        })
    }

    /// Electronic path index selected for the CDA.
    pub fn electronic_path(&self, cda: CdaId) -> Result<u32, String> {
        Ok(match self.correlator_mode()? {
            CorrelatorMode::Pa => 0,
            CorrelatorMode::Pb => 1,
            _ if matches!(cda, CdaId::Cda0 | CdaId::Cda2) => 0,
            _ => 1,
        })
    }

    /// Subarray identifier.
    pub fn subarray_id(&self) -> Result<u16, String> {
        let id = self.cursor.i16_at(0)?;
        if id < 0 {
            return Err(format!("invalid subarray id: {id}"));
        }
        Ok(id as u16)
    }

    /// Array configuration string.
    pub fn array_configuration(&self) -> Result<String, String> {
        Ok(self
            .cursor
            .ascii_at(2 * 10, 2)?
            .trim_end_matches(' ')
            .to_string())
    }

    /// Source direction `[ra, dec]` in radians.
    pub fn source_direction_radians(&self) -> Result<[f64; 2], String> {
        Ok([self.cursor.f64_at(2 * 24)?, self.cursor.f64_at(2 * 28)?])
    }

    /// Source name with trailing blanks removed.
    pub fn source_name(&self) -> Result<String, String> {
        Ok(self
            .cursor
            .ascii_at(2, 16)?
            .trim_end_matches(' ')
            .to_string())
    }

    /// Source qualifier / scan number.
    pub fn source_qualifier(&self) -> Result<i16, String> {
        self.cursor.i16_at(2 * 9)
    }

    /// Integration time in seconds.
    pub fn integration_time_seconds(&self) -> Result<f64, String> {
        Ok(f64::from(self.cursor.i16_at(2 * 19)?) / 19.2)
    }

    /// Observation time in seconds from local midnight of the archive day.
    pub fn observation_time_seconds(&self) -> Result<f64, String> {
        let radians = self.cursor.f64_at(2 * 72)?;
        Ok(radians / (2.0 * std::f64::consts::PI) * 86_400.0
            - self.integration_time_seconds()? / 2.0)
    }

    /// Project / observation identifier with trailing blanks removed.
    pub fn observation_id(&self) -> Result<String, String> {
        Ok(self
            .cursor
            .ascii_at(2 * 11, 6)?
            .trim_end_matches(' ')
            .to_string())
    }

    /// Raw two-character observing mode code, preserving embedded spaces.
    pub fn observation_mode_code(&self) -> Result<String, String> {
        self.cursor.ascii_at(2 * 15, 2)
    }

    /// Human-readable observing mode description matching CASA.
    pub fn observation_mode_description(&self) -> Result<String, String> {
        let code = self.observation_mode_code()?;
        Ok(match code.as_str() {
            "  " => "Standard Observing",
            "D " => "Delay center determination mode",
            "H " => "Holography raster mode",
            "IR" => "Interferometer reference pointing mode",
            "IA" => "Interferometer pointing mode (IF A)",
            "IB" => "Interferometer pointing mode (IF B)",
            "IC" => "Interferometer pointing mode (IF C)",
            "ID" => "Interferometer pointing mode (IF D)",
            "JA" => "JPL mode (IF A)",
            "JB" => "JPL mode (IF B)",
            "JC" => "JPL mode (IF C)",
            "JD" => "JPL mode (IF D)",
            "PA" => "Single dish pointing mode (IF A)",
            "PB" => "Single dish pointing mode (IF B)",
            "PC" => "Single dish pointing mode (IF C)",
            "PD" => "Single dish pointing mode (IF D)",
            "S " => "Solar observing configuration",
            "SP" => "Solar observing configuration (low accuracy empheris)",
            "TB" => "Test back-end and front-end",
            "TE" => "Tipping curve",
            "TF" => "Test front-end",
            "VA" => "Self-phasing mode for VLBI phased-array (IFs A and D)",
            "VB" => "Self-phasing mode for VLBI phased-array (IFs B and C)",
            "VL" => "Self-phasing mode for VLBI phased-array (IFs C and D)",
            "VR" => "Self-phasing mode for VLBI phased-array (IFs A and B)",
            "VS" => "Single dish VLBI",
            "VX" => "Applies last phase update from source line using VA mode",
            _ => return Ok(format!("Unknown mode: {code}")),
        }
        .to_string())
    }

    /// Single-character calibration code.
    pub fn calibration_code(&self) -> Result<String, String> {
        self.cursor.ascii_at(2 * 16, 1)
    }

    /// Direction epoch code.
    pub fn direction_epoch(&self) -> Result<DirectionEpoch, String> {
        let year = self.cursor.i16_at(2 * 161)?;
        Ok(match year {
            2000 => DirectionEpoch::J2000,
            1950 => DirectionEpoch::B1950Vla,
            -1 => DirectionEpoch::Apparent,
            other => DirectionEpoch::Unknown(other),
        })
    }

    /// Whether on-line Hanning smoothing was applied.
    pub fn smoothed(&self) -> Result<bool, String> {
        let first = self.cursor.u8_at(2 * 159)? as char;
        let second = self.cursor.u8_at(2 * 159 + 1)? as char;
        Ok(first == 'H' || second == 'H')
    }

    fn bandwidth_nibble(&self, cda: CdaId, base_offset: usize) -> Result<u8, String> {
        let offset = base_offset + if cda.index() > 1 { 1 } else { 0 };
        let byte = self.cursor.u8_at(offset)?;
        // Match CASA's current `VLASDA` implementation exactly, including the
        // upstream typo that always selects the upper nibble.
        let code = if matches!(cda, CdaId::Cda0) || (CdaId::Cda2.index() != 0) {
            (byte & 0xf0) >> 4
        } else {
            byte & 0x0f
        };
        Ok(code)
    }
}

/// ADA view onto a logical record.
#[derive(Debug, Clone, Copy)]
pub struct AntennaDataArea<'a> {
    cursor: ModcompCursor<'a>,
}

impl<'a> AntennaDataArea<'a> {
    /// Construct an ADA view over the logical-record bytes and ADA offset.
    pub fn new(record_bytes: &'a [u8], offset_bytes: u32) -> Result<Self, String> {
        let offset = offset_bytes as usize;
        let bytes = record_bytes
            .get(offset..)
            .ok_or_else(|| format!("ADA offset out of range: {offset}"))?;
        Ok(Self {
            cursor: ModcompCursor::new(bytes),
        })
    }

    /// Return the antenna identifier attached to the steel.
    pub fn antenna_id(&self) -> Result<u8, String> {
        self.cursor.u8_at(0)
    }

    /// Return the antenna name, matching CASA's old/new-style naming logic.
    pub fn antenna_name(&self, new_style: bool) -> Result<String, String> {
        let mut id = format!("{:02}", self.antenna_id()?);
        if !new_style {
            return Ok(id);
        }
        let bits = self.cursor.i16_at(2)?;
        let fiber = (bits & 0x0040) != 0;
        if fiber {
            if self.antenna_id()? == 29 {
                id.insert_str(0, "VA");
            } else {
                id.insert_str(0, "EA");
            }
        } else {
            id.insert_str(0, "VA");
        }
        Ok(id)
    }

    /// Front-end temperature for the IF.
    pub fn front_end_temperature(&self, which: IfId) -> Result<f32, String> {
        self.cursor.f32_at(2 * (48 + which.index() * 2))
    }

    /// UVW `u` coordinate in meters.
    pub fn u_meters(&self) -> Result<f64, String> {
        Ok(self.cursor.f64_at(2 * 28)? * meters_per_nanosecond())
    }

    /// UVW `v` coordinate in meters.
    pub fn v_meters(&self) -> Result<f64, String> {
        Ok(self.cursor.f64_at(2 * 30)? * meters_per_nanosecond())
    }

    /// UVW `w` coordinate in meters.
    pub fn w_meters(&self) -> Result<f64, String> {
        Ok(self.cursor.f64_at(2 * 32)? * meters_per_nanosecond())
    }

    /// Bx coordinate in meters.
    pub fn bx_meters(&self) -> Result<f64, String> {
        Ok(self.cursor.f64_at(2 * 34)? * meters_per_nanosecond())
    }

    /// By coordinate in meters.
    pub fn by_meters(&self) -> Result<f64, String> {
        Ok(self.cursor.f64_at(2 * 38)? * meters_per_nanosecond())
    }

    /// Bz coordinate in meters.
    pub fn bz_meters(&self) -> Result<f64, String> {
        Ok(self.cursor.f64_at(2 * 42)? * meters_per_nanosecond())
    }

    /// Antenna position `[bx, by, bz]` in meters.
    pub fn position_meters(&self) -> Result<[f64; 3], String> {
        Ok([self.bx_meters()?, self.by_meters()?, self.bz_meters()?])
    }

    /// Quality/status nibble for the IF.
    pub fn if_status(&self, which: IfId) -> Result<u8, String> {
        let mut offset = 2 * 3;
        if matches!(which, IfId::C | IfId::D) {
            offset += 1;
        }
        let mut status = self.cursor.u8_at(offset)?;
        if matches!(which, IfId::A | IfId::C) {
            status >>= 4;
        }
        Ok(status & 0x0f)
    }

    /// Nominal sensitivity multiplier for the IF.
    pub fn nominal_sensitivity(&self, which: IfId) -> Result<f32, String> {
        self.cursor.f32_at(2 * (4 + which.index() * 2))
    }

    /// Circular polarization routed through the IF.
    pub fn if_polarization(&self, which: IfId) -> Result<CircularPolarization, String> {
        let bits = self.cursor.u8_at(2)?;
        let swap = (bits & 0x80) != 0;
        Ok(match which {
            IfId::A | IfId::B => {
                if swap {
                    CircularPolarization::Left
                } else {
                    CircularPolarization::Right
                }
            }
            IfId::C | IfId::D => {
                if swap {
                    CircularPolarization::Right
                } else {
                    CircularPolarization::Left
                }
            }
        })
    }

    /// Whether nominal sensitivity scaling has already been applied.
    pub fn nominal_sensitivity_applied(&self, which: IfId, revision: u16) -> Result<bool, String> {
        if revision < 25 {
            return Ok(true);
        }
        let bits = self.cursor.i16_at(2 * (64 + which.index()))?;
        Ok((bits & 0x2000) != 0)
    }

    /// Array-family label matching CASA's ADA helper.
    pub fn array_name(&self) -> Result<String, String> {
        let bits = self.cursor.i16_at(2)?;
        let fiber = (bits & 0x0040) != 0;
        if fiber {
            if self.antenna_id()? > 28 {
                Ok("VLA:_".to_string())
            } else {
                Ok("EVLA:".to_string())
            }
        } else {
            Ok("VLA:_".to_string())
        }
    }

    /// Pad label matching CASA's `VLAADA::padName()`.
    pub fn pad_name(&self) -> Result<String, String> {
        const PAD_NAMES: [(&str, f64); 74] = [
            ("W1", 77.0),
            ("W2", 49.0),
            ("W3", 96.0),
            ("W4", 156.0),
            ("W5", 229.0),
            ("W6", 312.0),
            ("W7", 406.0),
            ("W8", 510.0),
            ("W9", 623.0),
            ("W10", 747.0),
            ("W12", 1021.0),
            ("W14", 1328.0),
            ("W16", 1667.0),
            ("W18", 2041.0),
            ("W20", 2446.0),
            ("W24", 3354.0),
            ("W28", 4391.0),
            ("W32", 5470.0),
            ("W36", 6671.0),
            ("W40", 7988.0),
            ("W48", 10926.0),
            ("W56", 14206.0),
            ("W64", 17843.0),
            ("W72", 21803.0),
            ("E1", 151.0),
            ("E2", 38.0),
            ("E3", 73.0),
            ("E4", 119.0),
            ("E5", 173.0),
            ("E6", 236.0),
            ("E7", 305.0),
            ("E8", 382.0),
            ("E9", 466.0),
            ("E10", 558.0),
            ("E12", 765.0),
            ("E14", 1000.0),
            ("E16", 1257.0),
            ("E18", 1548.0),
            ("E20", 1868.0),
            ("E24", 2552.0),
            ("E28", 3331.0),
            ("E32", 4180.0),
            ("E36", 5119.0),
            ("E40", 6127.0),
            ("E48", 8325.0),
            ("E56", 10814.0),
            ("E64", 13620.0),
            ("E72", 16204.0),
            ("N1", 2.5),
            ("N2", -100.0),
            ("N3", -175.0),
            ("N4", -250.0),
            ("N5", -362.0),
            ("N6", -495.0),
            ("N7", -646.0),
            ("N8", -813.0),
            ("N9", -995.0),
            ("N10", -1193.0),
            ("N12", -1632.0),
            ("N14", -2126.0),
            ("N16", -2673.0),
            ("N18", -3271.0),
            ("N20", -3917.0),
            ("N24", -5539.0),
            ("N28", -6976.0),
            ("N32", -8770.0),
            ("N36", -10733.0),
            ("N40", -12858.0),
            ("N48", -17583.0),
            ("N56", -22919.0),
            ("N64", -28827.0),
            ("N72", -35283.0),
            ("MPD", 1148.0),
            ("VPT", -46201.0),
        ];

        let x = self.bx_meters()?;
        let prefix = self.array_name()?;
        for (name, nominal_ns) in PAD_NAMES {
            let nominal_x = nominal_ns * meters_per_nanosecond();
            if (x - nominal_x).abs() <= 0.5 {
                return Ok(format!("{prefix}{name}"));
            }
        }
        Ok("UNKNOWN".to_string())
    }
}

/// CDA view onto a logical record.
#[derive(Debug, Clone, Copy)]
pub struct CorrelatorDataArea<'a> {
    cursor: ModcompCursor<'a>,
    offset: usize,
    baseline_size_bytes: usize,
    n_antennas: usize,
    n_channels_true: usize,
}

impl<'a> CorrelatorDataArea<'a> {
    /// Construct a CDA view over the logical-record bytes.
    pub fn new(
        record_bytes: &'a [u8],
        offset_bytes: u32,
        baseline_size_bytes: u16,
        n_antennas: u16,
        n_channels_true: u32,
    ) -> Result<Self, String> {
        let offset = offset_bytes as usize;
        if offset == 0 {
            return Err("CDA is not present in this logical record".to_string());
        }
        let end = offset
            .checked_add(baseline_size_bytes as usize)
            .ok_or_else(|| "CDA baseline size overflow".to_string())?;
        let _ = record_bytes
            .get(offset..end)
            .ok_or_else(|| format!("CDA offset out of range: {offset}"))?;
        Ok(Self {
            cursor: ModcompCursor::new(record_bytes),
            offset,
            baseline_size_bytes: baseline_size_bytes as usize,
            n_antennas: n_antennas as usize,
            n_channels_true: n_channels_true as usize,
        })
    }

    /// Whether the CDA is present.
    pub fn is_valid(&self) -> bool {
        self.offset != 0
    }

    /// Number of antennas represented in this CDA.
    pub fn n_antennas(&self) -> usize {
        self.n_antennas
    }

    /// Baseline record size in bytes.
    pub fn baseline_size_bytes(&self) -> usize {
        self.baseline_size_bytes
    }

    /// Number of archived channels including channel zero.
    pub fn n_channels_true(&self) -> usize {
        self.n_channels_true
    }

    /// Number of archived cross-correlation baselines.
    pub fn n_cross_correlations(&self) -> usize {
        self.n_antennas * (self.n_antennas.saturating_sub(1)) / 2
    }

    /// Decode the selected auto-correlation baseline.
    pub fn auto_corr(&self, which: usize) -> Result<BaselineRecord<'a>, String> {
        if which >= self.n_antennas {
            return Err(format!(
                "auto-correlation index out of range: {which} >= {}",
                self.n_antennas
            ));
        }
        self.baseline_at(self.offset + self.baseline_size_bytes * which)
    }

    /// Decode the selected cross-correlation baseline.
    pub fn cross_corr(&self, which: usize) -> Result<BaselineRecord<'a>, String> {
        let total = self.n_cross_correlations();
        if which >= total {
            return Err(format!(
                "cross-correlation index out of range: {which} >= {total}"
            ));
        }
        let start = self.offset + self.baseline_size_bytes * self.n_antennas;
        self.baseline_at(start + self.baseline_size_bytes * which)
    }

    fn baseline_at(&self, offset: usize) -> Result<BaselineRecord<'a>, String> {
        let bytes = self.cursor.bytes_at(offset, self.baseline_size_bytes)?;
        if self.n_channels_true == 1 {
            Ok(BaselineRecord::Continuum(ContinuumBaselineRecord {
                cursor: ModcompCursor::new(bytes),
            }))
        } else {
            Ok(BaselineRecord::SpectralLine(SpectralLineRecord {
                cursor: ModcompCursor::new(bytes),
                n_channels_true: self.n_channels_true,
            }))
        }
    }
}

/// Decoded baseline record.
#[derive(Debug, Clone, Copy)]
pub enum BaselineRecord<'a> {
    /// Continuum-format baseline.
    Continuum(ContinuumBaselineRecord<'a>),
    /// Spectral-line baseline.
    SpectralLine(SpectralLineRecord<'a>),
}

impl<'a> BaselineRecord<'a> {
    /// Scale factor applied to raw correlation integers.
    pub fn scale(&self) -> Result<u32, String> {
        match self {
            Self::Continuum(record) => record.scale(),
            Self::SpectralLine(record) => record.scale(),
        }
    }

    /// Zero-based antenna index for antenna 1.
    pub fn ant1(&self) -> Result<u8, String> {
        match self {
            Self::Continuum(record) => record.ant1(),
            Self::SpectralLine(record) => record.ant1(),
        }
    }

    /// Zero-based antenna index for antenna 2.
    pub fn ant2(&self) -> Result<u8, String> {
        match self {
            Self::Continuum(record) => record.ant2(),
            Self::SpectralLine(record) => record.ant2(),
        }
    }

    /// Scaled complex data values.
    pub fn data(&self) -> Result<Vec<Complex32>, String> {
        match self {
            Self::Continuum(record) => record.data(),
            Self::SpectralLine(record) => record.data(),
        }
    }

    /// Per-sample flags.
    pub fn flags(&self) -> Result<Vec<bool>, String> {
        match self {
            Self::Continuum(record) => record.flags(),
            Self::SpectralLine(record) => record.flags(),
        }
    }
}

/// Continuum baseline record.
#[derive(Debug, Clone, Copy)]
pub struct ContinuumBaselineRecord<'a> {
    cursor: ModcompCursor<'a>,
}

impl<'a> ContinuumBaselineRecord<'a> {
    /// Scale factor for the archived visibility integers.
    pub fn scale(&self) -> Result<u32, String> {
        baseline_scale(&self.cursor, 0)
    }

    /// Zero-based antenna index for antenna 1.
    pub fn ant1(&self) -> Result<u8, String> {
        baseline_ant1(&self.cursor, 2)
    }

    /// Zero-based antenna index for antenna 2.
    pub fn ant2(&self) -> Result<u8, String> {
        baseline_ant2(&self.cursor, 2)
    }

    /// Four continuum correlation products.
    pub fn data(&self) -> Result<Vec<Complex32>, String> {
        let scale = self.scale()? as f32;
        let mut values = Vec::with_capacity(4);
        for index in 0..4 {
            let base = 4 + index * 6;
            let real = self.cursor.i16_at(base)? as f32 / scale;
            let imag = self.cursor.i16_at(base + 2)? as f32 / scale;
            values.push(Complex32::new(real, imag));
        }
        Ok(values)
    }

    /// Continuum correlation flags.
    pub fn flags(&self) -> Result<Vec<bool>, String> {
        Ok(self
            .cursor
            .bytes_at(2, 4)?
            .iter()
            .map(|&value| value != 0)
            .collect())
    }

    /// Continuum variances scaled like CASA's `variance()` helper.
    pub fn variance(&self) -> Result<Vec<f32>, String> {
        let scale = self.scale()? as f32;
        let mut values = Vec::with_capacity(4);
        for index in 0..4 {
            let base = 8 + index * 6;
            values.push(self.cursor.u16_at(base)? as f32 / scale);
        }
        Ok(values)
    }
}

/// Spectral-line baseline record.
#[derive(Debug, Clone, Copy)]
pub struct SpectralLineRecord<'a> {
    cursor: ModcompCursor<'a>,
    n_channels_true: usize,
}

impl<'a> SpectralLineRecord<'a> {
    /// Scale factor for the archived visibility integers.
    pub fn scale(&self) -> Result<u32, String> {
        baseline_scale(&self.cursor, self.header_offset_bytes())
    }

    /// Zero-based antenna index for antenna 1.
    pub fn ant1(&self) -> Result<u8, String> {
        baseline_ant1(&self.cursor, self.header_offset_bytes() + 2)
    }

    /// Zero-based antenna index for antenna 2.
    pub fn ant2(&self) -> Result<u8, String> {
        baseline_ant2(&self.cursor, self.header_offset_bytes() + 2)
    }

    /// Spectral-line correlations, excluding the archived channel-zero average.
    pub fn data(&self) -> Result<Vec<Complex32>, String> {
        let true_channels = self.true_channels_without_average();
        let scale = self.scale()? as f32;
        let base = 2 * (self.header_words() + 4);
        let mut values = Vec::with_capacity(true_channels);
        let raw = self.cursor.bytes_at(base, true_channels * 4)?;
        for chunk in raw.chunks_exact(4) {
            let real = decode_i16(&chunk[..2])? as f32 / scale;
            let imag = decode_i16(&chunk[2..])? as f32 / scale;
            values.push(Complex32::new(real, imag));
        }
        Ok(values)
    }

    /// CASA currently exposes spectral flags as all-false.
    pub fn flags(&self) -> Result<Vec<bool>, String> {
        Ok(vec![false; self.true_channels_without_average()])
    }

    fn header_words(&self) -> usize {
        self.n_channels_true.div_ceil(16)
    }

    fn header_offset_bytes(&self) -> usize {
        2 * self.header_words()
    }

    fn true_channels_without_average(&self) -> usize {
        self.n_channels_true.saturating_sub(1)
    }
}

fn single_if_usage(cda: CdaId, expected: CdaId, if_id: IfId) -> Vec<IfUsage> {
    if cda == expected {
        vec![IfUsage {
            ant1: if_id,
            ant2: if_id,
        }]
    } else {
        Vec::new()
    }
}

fn baseline_scale(cursor: &ModcompCursor<'_>, header_offset: usize) -> Result<u32, String> {
    let exponent = cursor.i8_at(header_offset + 1)? as i32;
    Ok(1_u32.wrapping_shl((exponent + 8) as u32))
}

fn baseline_ant1(cursor: &ModcompCursor<'_>, header_offset: usize) -> Result<u8, String> {
    let bytes = cursor.bytes_at(header_offset, 2)?;
    let high = (bytes[0] & 0xe0) >> 5;
    let low = (bytes[1] & 0x03) << 2;
    Ok(high | low)
}

fn baseline_ant2(cursor: &ModcompCursor<'_>, header_offset: usize) -> Result<u8, String> {
    Ok(cursor.u8_at(header_offset + 1)? & 0x1f)
}

fn meters_per_nanosecond() -> f64 {
    299_792_458.0 / 1.0e9
}

pub(crate) fn invalid_record(path: &std::path::Path, message: impl Into<String>) -> VlaError {
    VlaError::invalid_archive(path, message)
}

#[cfg(test)]
mod tests {
    use casa_types::Complex32;

    use super::{
        AntennaDataArea, BaselineRecord, CdaId, CircularPolarization, CorrelatorDataArea,
        CorrelatorMode, DirectionEpoch, DopplerDefinition, FrequencyFrame, IfId, IfUsage,
        RecordControlArea, SubarrayDataArea,
    };

    #[test]
    fn rca_decodes_ada_layout() {
        let mut bytes = vec![0_u8; 128];
        bytes[0..4].copy_from_slice(&(64_i32).to_be_bytes());
        bytes[2 * 3..2 * 3 + 2].copy_from_slice(&27_u16.to_be_bytes());
        bytes[2 * 4..2 * 4 + 4].copy_from_slice(&12345_u32.to_be_bytes());
        bytes[2 * 12..2 * 12 + 4].copy_from_slice(&100_u32.to_be_bytes());
        bytes[2 * 14..2 * 14 + 4].copy_from_slice(&150_u32.to_be_bytes());
        bytes[2 * 16..2 * 16 + 2].copy_from_slice(&8_u16.to_be_bytes());
        bytes[2 * 17..2 * 17 + 2].copy_from_slice(&3_u16.to_be_bytes());
        bytes[2 * 18..2 * 18 + 4].copy_from_slice(&200_u32.to_be_bytes());
        bytes[2 * 20..2 * 20 + 2].copy_from_slice(&14_u16.to_be_bytes());
        bytes[2 * 21..2 * 21 + 2].copy_from_slice(&28_u16.to_be_bytes());
        bytes[2 * 22..2 * 22 + 4].copy_from_slice(&300_u32.to_be_bytes());

        let rca = RecordControlArea::new(&bytes);
        assert_eq!(rca.length_bytes().unwrap(), 128);
        assert_eq!(rca.revision().unwrap(), 27);
        assert_eq!(rca.obs_day().unwrap(), 12345);
        assert_eq!(rca.sda_offset_bytes().unwrap(), 200);
        assert_eq!(rca.ada_size_bytes().unwrap(), 16);
        assert_eq!(rca.ada_offset_bytes(0).unwrap(), 300);
        assert_eq!(rca.ada_offset_bytes(2).unwrap(), 332);
        assert_eq!(rca.cda_offset_bytes(0).unwrap(), 400);
        assert_eq!(rca.cda_header_bytes(0).unwrap(), 28);
        assert_eq!(rca.cda_baseline_bytes(0).unwrap(), 56);
        assert_eq!(rca.cda_offset_bytes(1).unwrap(), 600);
        assert_eq!(rca.used_cda_count().unwrap(), 2);
        assert!(
            rca.ada_offset_bytes(3)
                .unwrap_err()
                .contains("out of range")
        );
        assert!(
            rca.cda_offset_bytes(4)
                .unwrap_err()
                .contains("out of range")
        );
        assert!(
            RecordControlArea::new(&[0, 0, 0, 0])
                .length_bytes()
                .unwrap_err()
                .contains("invalid logical-record length")
        );
    }

    #[test]
    fn sda_decodes_mode_channel_counts_and_strings() {
        let mut bytes = vec![0_u8; 512];
        bytes[2..18].copy_from_slice(b"3C286           ");
        bytes[2 * 9..2 * 9 + 2].copy_from_slice(&7_i16.to_be_bytes());
        bytes[2 * 10..2 * 10 + 2].copy_from_slice(b"D ");
        bytes[2 * 11..2 * 11 + 6].copy_from_slice(b"AB123 ");
        bytes[2 * 15..2 * 15 + 2].copy_from_slice(b"PA");
        bytes[2 * 16] = b'C';
        bytes[2 * 18] = 0x20;
        bytes[2 * 19..2 * 19 + 2].copy_from_slice(&192_i16.to_be_bytes());
        bytes[2 * 157..2 * 157 + 4].copy_from_slice(b"PA  ");
        bytes[2 * 159] = b'H';
        bytes[2 * 161..2 * 161 + 2].copy_from_slice(&2000_i16.to_be_bytes());

        let sda = SubarrayDataArea::new(&bytes, 0).unwrap();
        assert_eq!(sda.source_name().unwrap(), "3C286");
        assert_eq!(sda.source_qualifier().unwrap(), 7);
        assert_eq!(sda.array_configuration().unwrap(), "D");
        assert_eq!(sda.observation_id().unwrap(), "AB123");
        assert_eq!(sda.observation_mode_code().unwrap(), "PA");
        assert_eq!(sda.calibration_code().unwrap(), "C");
        assert_eq!(sda.correlator_mode().unwrap(), CorrelatorMode::Pa);
        assert_eq!(sda.true_channels(CdaId::Cda0).unwrap(), 4);
        assert_eq!(sda.n_channels(CdaId::Cda0).unwrap(), 3);
        assert_eq!(sda.integration_time_seconds().unwrap(), 10.0);
        assert!(sda.smoothed().unwrap());
        assert_eq!(
            sda.observation_mode_description().unwrap(),
            "Single dish pointing mode (IF A)"
        );
        assert_eq!(sda.direction_epoch().unwrap(), DirectionEpoch::J2000);
        assert_eq!(sda.electronic_path(CdaId::Cda2).unwrap(), 0);
        assert_eq!(
            sda.if_usage(CdaId::Cda2).unwrap(),
            vec![IfUsage {
                ant1: IfId::A,
                ant2: IfId::C
            }]
        );
    }

    #[test]
    fn sda_mode_tables_cover_casa_if_usage_and_observing_codes() {
        let cases = [
            (
                b"    ",
                CorrelatorMode::Continuum,
                CdaId::Cda0,
                vec![
                    (IfId::A, IfId::A),
                    (IfId::C, IfId::C),
                    (IfId::A, IfId::C),
                    (IfId::C, IfId::A),
                ],
            ),
            (
                b"1A  ",
                CorrelatorMode::A,
                CdaId::Cda0,
                vec![(IfId::A, IfId::A)],
            ),
            (
                b"1B  ",
                CorrelatorMode::B,
                CdaId::Cda1,
                vec![(IfId::B, IfId::B)],
            ),
            (
                b"1C  ",
                CorrelatorMode::C,
                CdaId::Cda2,
                vec![(IfId::C, IfId::C)],
            ),
            (
                b"1D  ",
                CorrelatorMode::D,
                CdaId::Cda3,
                vec![(IfId::D, IfId::D)],
            ),
            (
                b"2AB ",
                CorrelatorMode::Ab,
                CdaId::Cda1,
                vec![(IfId::B, IfId::B)],
            ),
            (
                b"2AC ",
                CorrelatorMode::Ac,
                CdaId::Cda2,
                vec![(IfId::C, IfId::C)],
            ),
            (
                b"2AD ",
                CorrelatorMode::Ad,
                CdaId::Cda3,
                vec![(IfId::D, IfId::D)],
            ),
            (
                b"2BC ",
                CorrelatorMode::Bc,
                CdaId::Cda1,
                vec![(IfId::B, IfId::B)],
            ),
            (
                b"2BD ",
                CorrelatorMode::Bd,
                CdaId::Cda3,
                vec![(IfId::D, IfId::D)],
            ),
            (
                b"2CD ",
                CorrelatorMode::Cd,
                CdaId::Cda2,
                vec![(IfId::C, IfId::C)],
            ),
            (
                b"4   ",
                CorrelatorMode::Abcd,
                CdaId::Cda3,
                vec![(IfId::D, IfId::D)],
            ),
            (
                b"PB  ",
                CorrelatorMode::Pb,
                CdaId::Cda2,
                vec![(IfId::B, IfId::D)],
            ),
            (b"ZZ  ", CorrelatorMode::Unknown, CdaId::Cda0, vec![]),
        ];
        for (raw_mode, expected_mode, cda, expected_usage) in cases {
            let mut bytes = vec![0_u8; 512];
            bytes[2 * 157..2 * 157 + 4].copy_from_slice(raw_mode);
            let sda = SubarrayDataArea::new(&bytes, 0).unwrap();
            assert_eq!(sda.correlator_mode().unwrap(), expected_mode);
            let usage = expected_usage
                .into_iter()
                .map(|(ant1, ant2)| IfUsage { ant1, ant2 })
                .collect::<Vec<_>>();
            assert_eq!(sda.if_usage(cda).unwrap(), usage);
        }

        let mut bytes = vec![0_u8; 512];
        for (code, expected) in [
            (b"D ", "Delay center determination mode"),
            (b"IR", "Interferometer reference pointing mode"),
            (b"TB", "Test back-end and front-end"),
            (
                b"VA",
                "Self-phasing mode for VLBI phased-array (IFs A and D)",
            ),
            (b"??", "Unknown mode: ??"),
        ] {
            bytes[2 * 15..2 * 15 + 2].copy_from_slice(code);
            let sda = SubarrayDataArea::new(&bytes, 0).unwrap();
            assert_eq!(sda.observation_mode_description().unwrap(), expected);
        }
    }

    #[test]
    fn sda_frequency_metadata_decodes_doppler_and_bandwidth_branches() {
        let mut bytes = vec![0_u8; 512];
        bytes[2 * 18] = 0x10;
        bytes[2 * 100] = 0x70;
        bytes[2 * 101] = 0x30;
        bytes[2 * 153] = b'L';
        bytes[2 * 153 + 1] = b'V';
        bytes[2 * 154] = b'B';
        bytes[2 * 154 + 1] = b'Z';
        bytes[2 * 159 + 1] = b'H';
        bytes[2 * 161..2 * 161 + 2].copy_from_slice(&1950_i16.to_be_bytes());
        bytes[2 * 166..2 * 166 + 2].copy_from_slice(&2_i16.to_be_bytes());

        let sda = SubarrayDataArea::new(&bytes, 0).unwrap();
        assert_eq!(sda.true_channels(CdaId::Cda0).unwrap(), 2);
        assert_eq!(sda.n_channels(CdaId::Cda0).unwrap(), 1);
        assert_eq!(sda.channel_width_hz(CdaId::Cda0).unwrap(), 12.5e6);
        assert_eq!(
            sda.correlated_bandwidth_hz(CdaId::Cda0).unwrap(),
            25.0 / 32.0 * 1.0e6
        );
        assert_eq!(sda.filter_bandwidth_hz(CdaId::Cda0).unwrap(), 1.0e200);
        assert!(sda.doppler_tracking(CdaId::Cda0).unwrap());
        assert_eq!(sda.rest_frame(CdaId::Cda0).unwrap(), FrequencyFrame::Lsrk);
        assert_eq!(
            sda.doppler_definition(CdaId::Cda0).unwrap(),
            DopplerDefinition::Radio
        );
        assert_eq!(
            sda.rest_frame(CdaId::Cda1).unwrap(),
            FrequencyFrame::Barycentric
        );
        assert_eq!(
            sda.doppler_definition(CdaId::Cda1).unwrap(),
            DopplerDefinition::Optical
        );
        assert!(sda.smoothed().unwrap());
        assert_eq!(sda.direction_epoch().unwrap(), DirectionEpoch::B1950Vla);

        bytes[2 * 18] = 0x00;
        bytes[2 * 100] = 0x90;
        bytes[2 * 101] = 0x40;
        bytes[2 * 153 + 1] = b'F';
        bytes[2 * 161..2 * 161 + 2].copy_from_slice(&(-1_i16).to_be_bytes());
        let sda = SubarrayDataArea::new(&bytes, 0).unwrap();
        assert_eq!(sda.true_channels(CdaId::Cda0).unwrap(), 1);
        assert!(
            sda.correlated_bandwidth_hz(CdaId::Cda0)
                .unwrap()
                .is_infinite()
        );
        assert!(sda.filter_bandwidth_hz(CdaId::Cda0).unwrap().is_nan());
        assert!(!sda.doppler_tracking(CdaId::Cda0).unwrap());
        assert_eq!(sda.direction_epoch().unwrap(), DirectionEpoch::Apparent);
    }

    #[test]
    fn ada_decodes_identity_and_if_bits() {
        let mut bytes = vec![0_u8; 256];
        bytes[0] = 12;
        bytes[2..4].copy_from_slice(&0x8040_u16.to_be_bytes());
        bytes[2 * 3] = 0xa0;
        bytes[2 * 3 + 1] = 0x50;
        bytes[2 * (64 + IfId::B.index())..2 * (64 + IfId::B.index()) + 2]
            .copy_from_slice(&0x2000_i16.to_be_bytes());

        let ada = AntennaDataArea::new(&bytes, 0).unwrap();
        assert_eq!(ada.antenna_id().unwrap(), 12);
        assert_eq!(ada.antenna_name(true).unwrap(), "EA12");
        assert_eq!(ada.antenna_name(false).unwrap(), "12");
        assert_eq!(ada.array_name().unwrap(), "EVLA:");
        assert_eq!(ada.if_status(IfId::A).unwrap(), 0x0a);
        assert_eq!(ada.if_status(IfId::D).unwrap(), 0x00);
        assert_eq!(
            ada.if_polarization(IfId::A).unwrap(),
            CircularPolarization::Left
        );
        assert_eq!(
            ada.if_polarization(IfId::C).unwrap(),
            CircularPolarization::Right
        );
        assert!(ada.nominal_sensitivity_applied(IfId::B, 25).unwrap());
        assert!(ada.nominal_sensitivity_applied(IfId::B, 24).unwrap());

        bytes[0] = 29;
        let ada = AntennaDataArea::new(&bytes, 0).unwrap();
        assert_eq!(ada.antenna_name(true).unwrap(), "VA29");
        assert_eq!(ada.array_name().unwrap(), "VLA:_");
    }

    #[test]
    fn ada_pad_names_and_offsets_cover_position_helpers() {
        let mut bytes = vec![0_u8; 256];
        bytes[0] = 1;
        // Zero-valued ModComp doubles decode to zero ns, outside the 0.5 m pad-name tolerance.
        let ada = AntennaDataArea::new(&bytes, 0).unwrap();
        assert_eq!(ada.position_meters().unwrap(), [0.0, 0.0, 0.0]);
        assert_eq!(ada.u_meters().unwrap(), 0.0);
        assert_eq!(ada.v_meters().unwrap(), 0.0);
        assert_eq!(ada.w_meters().unwrap(), 0.0);
        assert_eq!(ada.pad_name().unwrap(), "UNKNOWN");
    }

    #[test]
    fn cda_decodes_continuum_baseline() {
        let mut bytes = vec![0_u8; 64];
        let offset = 4_u32;
        bytes[offset as usize + 1] = 0;
        bytes[offset as usize + 2] = 0x40;
        bytes[offset as usize + 3] = 0x02;

        let samples = [
            (100_i16, -200_i16, 25_u16),
            (-300_i16, 400_i16, 26_u16),
            (500_i16, -600_i16, 27_u16),
            (-700_i16, 800_i16, 28_u16),
        ];
        for (index, (real, imag, variance)) in samples.into_iter().enumerate() {
            let base = offset as usize + 4 + index * 6;
            bytes[base..base + 2].copy_from_slice(&real.to_be_bytes());
            bytes[base + 2..base + 4].copy_from_slice(&imag.to_be_bytes());
            bytes[base + 4..base + 6].copy_from_slice(&variance.to_be_bytes());
        }

        let cda = CorrelatorDataArea::new(&bytes, offset, 28, 1, 1).unwrap();
        assert!(cda.is_valid());
        assert_eq!(cda.n_antennas(), 1);
        assert_eq!(cda.baseline_size_bytes(), 28);
        assert_eq!(cda.n_channels_true(), 1);
        assert_eq!(cda.n_cross_correlations(), 0);
        assert!(cda.cross_corr(0).unwrap_err().contains("out of range"));
        let baseline = cda.auto_corr(0).unwrap();
        assert_eq!(baseline.scale().unwrap(), 256);
        assert_eq!(baseline.ant1().unwrap(), 10);
        assert_eq!(baseline.ant2().unwrap(), 2);
        assert_eq!(baseline.flags().unwrap(), vec![true, true, false, true]);
        assert_eq!(
            baseline.data().unwrap(),
            vec![
                Complex32::new(100.0 / 256.0, -200.0 / 256.0),
                Complex32::new(-300.0 / 256.0, 400.0 / 256.0),
                Complex32::new(500.0 / 256.0, -600.0 / 256.0),
                Complex32::new(-700.0 / 256.0, 800.0 / 256.0),
            ]
        );

        let BaselineRecord::Continuum(record) = baseline else {
            panic!("expected continuum record");
        };
        assert_eq!(
            record.variance().unwrap(),
            vec![25.0 / 256.0, 26.0 / 256.0, 27.0 / 256.0, 28.0 / 256.0]
        );
    }

    #[test]
    fn cda_decodes_spectral_line_baseline() {
        let mut bytes = vec![0_u8; 64];
        let offset = 4_u32;
        bytes[offset as usize + 2] = 0x20;
        bytes[offset as usize + 3] = 0x00;
        bytes[offset as usize + 4] = 0x20;
        bytes[offset as usize + 5] = 0x00;
        bytes[offset as usize + 10..offset as usize + 12].copy_from_slice(&10_i16.to_be_bytes());
        bytes[offset as usize + 12..offset as usize + 14].copy_from_slice(&20_i16.to_be_bytes());
        bytes[offset as usize + 14..offset as usize + 16].copy_from_slice(&30_i16.to_be_bytes());
        bytes[offset as usize + 16..offset as usize + 18].copy_from_slice(&40_i16.to_be_bytes());

        let cda = CorrelatorDataArea::new(&bytes, offset, 18, 1, 3).unwrap();
        let baseline = cda.auto_corr(0).unwrap();
        assert_eq!(baseline.scale().unwrap(), 256);
        assert_eq!(baseline.ant1().unwrap(), 1);
        assert_eq!(baseline.ant2().unwrap(), 0);
        assert_eq!(
            baseline.data().unwrap(),
            vec![
                Complex32::new(10.0 / 256.0, 20.0 / 256.0),
                Complex32::new(30.0 / 256.0, 40.0 / 256.0),
            ]
        );
        assert_eq!(baseline.flags().unwrap(), vec![false, false]);
    }

    #[test]
    fn cda_matches_casa_wrapping_scale_behavior_for_large_exponents() {
        let mut bytes = vec![0_u8; 64];
        let offset = 4_u32;
        bytes[offset as usize + 1] = 48;

        let cda = CorrelatorDataArea::new(&bytes, offset, 28, 1, 1).unwrap();
        let baseline = cda.auto_corr(0).unwrap();
        assert_eq!(baseline.scale().unwrap(), 16_777_216);
    }

    #[test]
    fn cda_constructor_and_cross_correlation_validate_layout() {
        assert!(
            CorrelatorDataArea::new(&[0; 16], 0, 8, 2, 1)
                .unwrap_err()
                .contains("not present")
        );
        assert!(
            CorrelatorDataArea::new(&[0; 16], u32::MAX - 1, 8, 2, 1)
                .unwrap_err()
                .contains("out of range")
        );

        let mut bytes = vec![0_u8; 128];
        let offset = 8_u32;
        let baseline_size = 28_u16;
        let cross_offset = offset as usize + baseline_size as usize * 2;
        bytes[cross_offset + 1] = 0;
        bytes[cross_offset + 2] = 0x60;
        bytes[cross_offset + 3] = 0x03;
        bytes[cross_offset + 4..cross_offset + 6].copy_from_slice(&64_i16.to_be_bytes());
        bytes[cross_offset + 6..cross_offset + 8].copy_from_slice(&(-128_i16).to_be_bytes());

        let cda = CorrelatorDataArea::new(&bytes, offset, baseline_size, 2, 1).unwrap();
        assert_eq!(cda.n_cross_correlations(), 1);
        assert!(cda.auto_corr(2).unwrap_err().contains("out of range"));
        let cross = cda.cross_corr(0).unwrap();
        assert_eq!(cross.ant1().unwrap(), 15);
        assert_eq!(cross.ant2().unwrap(), 3);
        assert_eq!(
            cross.data().unwrap()[0],
            Complex32::new(64.0 / 256.0, -128.0 / 256.0)
        );
    }
}
