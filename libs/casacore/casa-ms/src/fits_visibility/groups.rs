// SPDX-License-Identifier: LGPL-3.0-or-later
//! Random-groups UVFITS primary visibility arrays.

use std::ffi::{CStr, CString, c_char, c_long};
use std::ops::Range;
use std::path::Path;
use std::ptr;

use casa_core::{Complex32, Correlation};
use fitsio::{
    FileOpenMode, FitsFile, errors,
    hdu::{FitsHdu, HduInfo},
    headers::ReadsKey,
    sys,
};
use ndarray::{Array1, Array2, ShapeBuilder};

use crate::{MsError, MsResult};

const SECONDS_PER_DAY: f64 = 86_400.0;
const MJD_ZERO_JULIAN_DATE: f64 = 2_400_000.5;
const SPEED_OF_LIGHT: f64 = 299_792_458.0;

pub(super) struct Axis {
    name: String,
    pub(super) length: usize,
    pub(super) reference: f64,
    pub(super) pixel: f64,
    pub(super) increment: f64,
    stride: usize,
}

impl Axis {
    fn coordinate(&self, index: usize) -> f64 {
        self.reference + (index as f64 + 1.0 - self.pixel) * self.increment
    }
}

struct Parameter {
    name: String,
    scale: f64,
    zero: f64,
}

enum Baseline {
    Packed(usize),
    Separate { antennas: [usize; 2], array: usize },
}

struct GroupParameters {
    uvw: [usize; 3],
    date: Vec<usize>,
    source: Option<usize>,
    frequency: Option<usize>,
    interval: Option<usize>,
    baseline: Baseline,
}

impl GroupParameters {
    fn resolve(parameters: &[Parameter]) -> MsResult<Self> {
        let position = |name| {
            parameters
                .iter()
                .position(|parameter| parameter.name == name)
        };
        let required = |name| {
            position(name).ok_or_else(|| invalid(format!("Missing UVFITS {name} group parameter")))
        };
        let uvw = ["UU", "VV", "WW"].map(|label| {
            parameters
                .iter()
                .position(|parameter| parameter.name.contains(label))
                .ok_or_else(|| invalid(format!("Missing UVFITS {label} group parameter")))
        });
        let [u, v, w] = uvw;
        let mut date = vec![required("DATE")?];
        if let Some((index, _)) = parameters
            .iter()
            .enumerate()
            .skip(date[0] + 1)
            .find(|(_, parameter)| parameter.name == "DATE")
        {
            date.push(index);
        }
        let baseline = match position("BASELINE") {
            Some(index) => Baseline::Packed(index),
            None => Baseline::Separate {
                antennas: [required("ANTENNA1")?, required("ANTENNA2")?],
                array: required("SUBARRAY")?,
            },
        };
        Ok(Self {
            uvw: [u?, v?, w?],
            date,
            source: position("SOURCE"),
            frequency: position("FREQSEL"),
            interval: position("INTTIM"),
            baseline,
        })
    }

    fn metadata(&self, values: &[f64]) -> MsResult<GroupMetadata> {
        let date = values[self.date[0]] - MJD_ZERO_JULIAN_DATE;
        let remainder = self.date.get(1).map_or(0.0, |&index| values[index]);
        let time = (date + remainder) * SECONDS_PER_DAY;
        let (antennas, array) = match self.baseline {
            Baseline::Packed(index) => {
                let value = values[index] as f32;
                let packed = value as i32;
                let array = (100.0 * (f64::from(value) - f64::from(packed) + 0.001)) as i32;
                ([packed / 256, packed % 256], array)
            }
            Baseline::Separate { antennas, array } => (
                antennas.map(|index| values[index] as i32),
                values[array] as i32 - 1,
            ),
        };
        let field = self.source.map_or(1, |index| values[index] as i32);
        let frequency = self.frequency.map_or(1, |index| values[index] as i32);
        if !time.is_finite()
            || antennas.iter().any(|&antenna| antenna < 1)
            || array < 0
            || field < 1
            || frequency < 1
        {
            return Err(invalid(
                "Invalid time, antenna, array, source or frequency in UVFITS group",
            ));
        }
        Ok(GroupMetadata {
            time,
            antennas: antennas.map(|number| number - 1),
            array,
            field: field - 1,
            frequency: frequency - 1,
            uvw: self.uvw.map(|index| values[index] * SPEED_OF_LIGHT),
            interval: self.interval.map(|index| values[index]),
        })
    }
}

pub(super) struct GroupMetadata {
    pub(super) time: f64,
    pub(super) antennas: [i32; 2],
    pub(super) array: i32,
    pub(super) field: i32,
    pub(super) frequency: i32,
    pub(super) uvw: [f64; 3],
    pub(super) interval: Option<f64>,
}

pub(super) struct GroupLayout {
    pub(super) corr: Vec<Correlation>,
    pub(super) frequency: Axis,
    pub(super) direction_deg: [f64; 2],
    pub(super) windows: usize,
    corr_stride: usize,
    window_stride: usize,
    input_corr_order: Vec<usize>,
    sample_count: usize,
}

impl GroupLayout {
    fn from_header(fits: &mut FitsFile, hdu: &FitsHdu) -> MsResult<Self> {
        let naxis = hdu.read_key::<i32>(fits, "NAXIS").map_err(invalid)?;
        if !(6..=999).contains(&naxis) || hdu.read_key::<i32>(fits, "NAXIS1").map_err(invalid)? != 0
        {
            return Err(invalid("Invalid UVFITS random-groups axis layout"));
        }
        let mut stride = 1usize;
        let mut axes = Vec::new();
        for number in 2..=naxis {
            let length = positive_count(hdu, fits, &format!("NAXIS{number}"))?;
            let key = |prefix| format!("{prefix}{number}");
            let name: String = optional_key(hdu, fits, &key("CTYPE"))?.unwrap_or_default();
            axes.push(Axis {
                name: name.trim_end().to_owned(),
                length,
                reference: optional_key(hdu, fits, &key("CRVAL"))?.unwrap_or_default(),
                pixel: optional_key(hdu, fits, &key("CRPIX"))?.unwrap_or(1.0),
                increment: optional_key(hdu, fits, &key("CDELT"))?.unwrap_or(1.0),
                stride,
            });
            stride = stride
                .checked_mul(length)
                .ok_or_else(|| invalid("UVFITS axis product overflow"))?;
        }
        Self::from_axes(axes, stride, false)
    }

    fn from_table(fits: &mut FitsFile, hdu: &FitsHdu, column: usize) -> MsResult<Self> {
        let dim: String = if let Some(dim) = optional_key(hdu, fits, &format!("TDIM{column}"))? {
            dim
        } else {
            hdu.read_key(fits, "TDIM").map_err(invalid)?
        };
        let mut axes = Vec::new();
        let mut stride = 1usize;
        for (number, length) in (1..).zip(dim.trim_matches(['(', ')']).split(',')) {
            let length = length.trim().parse::<usize>().map_err(invalid)?;
            if length == 0 {
                return Err(invalid("Zero axis length in UVFITS visibility table"));
            }
            let key = |name| format!("{number}{name}{column}");
            axes.push(Axis {
                name: hdu
                    .read_key::<String>(fits, &key("CTYP"))
                    .map_err(invalid)?
                    .trim_end()
                    .to_owned(),
                length,
                reference: hdu.read_key(fits, &key("CRVL")).map_err(invalid)?,
                pixel: hdu.read_key(fits, &key("CRPX")).map_err(invalid)?,
                increment: hdu.read_key(fits, &key("CDLT")).map_err(invalid)?,
                stride,
            });
            stride = stride
                .checked_mul(length)
                .ok_or_else(|| invalid("UVFITS axis product overflow"))?;
        }
        Self::from_axes(axes, stride, true)
    }

    fn from_axes(mut axes: Vec<Axis>, stride: usize, binary: bool) -> MsResult<Self> {
        let direction_deg = ["RA", "DEC"].map(|name| {
            axes.iter()
                .find(|axis| {
                    axis.name == name
                        || axis
                            .name
                            .strip_prefix(name)
                            .is_some_and(|suffix| suffix.starts_with('-'))
                })
                .map(|axis| axis.reference)
                .ok_or_else(|| invalid(format!("UVFITS has no {name} axis")))
        });
        let [ra, dec] = direction_deg;
        let (windows, window_stride) = axes
            .iter()
            .find(|axis| axis.name == "IF")
            .map_or((1, 0), |axis| (axis.length, axis.stride));
        let stokes = axes
            .iter()
            .find(|axis| axis.name == "STOKES")
            .ok_or_else(|| invalid("UVFITS has no STOKES axis"))?;
        let (corr, input_corr_order) = correlation_order(stokes, binary)?;
        let corr_stride = stokes.stride;
        let frequency = axes
            .iter()
            .position(|axis| axis.name == "FREQ")
            .map(|index| axes.remove(index))
            .ok_or_else(|| invalid("UVFITS has no FREQ axis"))?;
        if !axes
            .iter()
            .any(|axis| axis.name == "COMPLEX" && axis.length == 3 && axis.stride == 1)
            || 3_usize
                .checked_mul(corr.len())
                .and_then(|n| n.checked_mul(frequency.length))
                .and_then(|n| n.checked_mul(windows))
                != Some(stride)
        {
            return Err(invalid(
                "UVFITS visibility samples require real, imaginary and weight components",
            ));
        }
        Ok(Self {
            corr,
            frequency,
            direction_deg: [ra?, dec?],
            windows,
            corr_stride,
            window_stride,
            input_corr_order,
            sample_count: stride,
        })
    }

    pub(super) fn visibility(&self, samples: &[f32], window: usize) -> GroupVisibility {
        let correlations = self.corr.len();
        let channels = self.frequency.length;
        let shape = (correlations, channels).f();
        let length = correlations * channels;
        let mut data = Vec::with_capacity(length);
        let mut weight_spectrum = Vec::with_capacity(length);
        let mut flag = Vec::with_capacity(length);
        let mut weight = Array1::<f32>::zeros(correlations);
        let window_offset = window * self.window_stride;
        for channel in 0..channels {
            let base = window_offset + channel * self.frequency.stride;
            for (correlation, &input) in self.input_corr_order.iter().enumerate() {
                let offset = base + input * self.corr_stride;
                let value = &samples[offset..offset + 3];
                let unsigned_weight = value[2].abs();
                data.push(Complex32::new(value[0], value[1]));
                flag.push(value[2] <= 0.0);
                weight_spectrum.push(unsigned_weight);
                weight[correlation] += unsigned_weight;
            }
        }
        let data = Array2::from_shape_vec(shape, data).expect("one visibility per cell");
        let flag = Array2::from_shape_vec(shape, flag).expect("one flag per cell");
        let weight_spectrum =
            Array2::from_shape_vec(shape, weight_spectrum).expect("one weight per cell");
        let sigma = weight.mapv(|value| {
            if value > 0.0 {
                f64::from(value).recip().sqrt() as f32
            } else {
                0.0
            }
        });
        GroupVisibility {
            data,
            weight_spectrum,
            weight,
            sigma,
            flag,
        }
    }
}

pub(super) struct GroupVisibility {
    pub(super) data: Array2<Complex32>,
    pub(super) weight_spectrum: Array2<f32>,
    pub(super) weight: Array1<f32>,
    pub(super) sigma: Array1<f32>,
    pub(super) flag: Array2<bool>,
}

pub(super) struct RandomGroups {
    pub(super) layout: GroupLayout,
    pub(super) count: usize,
    parameters: Vec<Parameter>,
    indices: GroupParameters,
    values: Vec<f64>,
    samples: Vec<f32>,
}

impl RandomGroups {
    pub(super) fn read_header(fits: &mut FitsFile) -> MsResult<Self> {
        let primary = fits.primary_hdu().map_err(invalid)?;
        if !optional_key::<bool>(&primary, fits, "GROUPS")?.unwrap_or(false) {
            return Err(invalid("UVFITS primary HDU has no random groups"));
        }
        let bitpix: i32 = primary.read_key(fits, "BITPIX").map_err(invalid)?;
        if !matches!(bitpix, 16 | 32 | -32) {
            return Err(invalid(format!(
                "Unhandled UVFITS random groups BITPIX {bitpix}"
            )));
        }
        let count = positive_count(&primary, fits, "GCOUNT")?;
        let layout = GroupLayout::from_header(fits, &primary)?;
        let parameter_count = positive_count(&primary, fits, "PCOUNT")?;
        let sample_bytes = (bitpix.unsigned_abs() / 8) as usize;
        let payload = parameter_count
            .checked_add(layout.sample_count)
            .and_then(|samples| samples.checked_mul(count))
            .and_then(|samples| samples.checked_mul(sample_bytes))
            .ok_or_else(|| invalid("UVFITS random-groups payload size overflow"))?;
        verify_payload(fits, payload)?;
        let parameters = (1..=parameter_count)
            .map(|index| {
                let key = |prefix| format!("{prefix}{index}");
                let name: String = primary.read_key(fits, &key("PTYPE")).map_err(invalid)?;
                Ok(Parameter {
                    name: name.trim_end().to_owned(),
                    scale: optional_key(&primary, fits, &key("PSCAL"))?.unwrap_or(1.0),
                    zero: optional_key(&primary, fits, &key("PZERO"))?.unwrap_or_default(),
                })
            })
            .collect::<MsResult<Vec<_>>>()?;
        let indices = GroupParameters::resolve(&parameters)?;
        let mut samples = Vec::new();
        samples
            .try_reserve_exact(layout.sample_count)
            .map_err(invalid)?;
        samples.resize(layout.sample_count, 0.0);
        Ok(Self {
            layout,
            count,
            parameters,
            indices,
            values: vec![0.0; parameter_count],
            samples,
        })
    }

    pub(super) fn read(
        &mut self,
        fits: &mut FitsFile,
        index: usize,
    ) -> MsResult<(GroupMetadata, &GroupLayout, &[f32])> {
        let group = c_long::try_from(index + 1).map_err(invalid)?;
        let parameter_count = c_long::try_from(self.values.len()).map_err(invalid)?;
        let sample_count = i64::try_from(self.samples.len()).map_err(invalid)?;
        let mut status = 0;
        let mut any_null = 0;
        // SAFETY: FitsFile owns the open, exclusive handle. Both arrays have
        // the lengths passed to CFITSIO, and the primary HDU is selected.
        unsafe {
            let handle = fits.as_raw();
            sys::ffggpd(
                handle,
                group,
                1,
                parameter_count,
                self.values.as_mut_ptr(),
                &mut status,
            );
            sys::ffgpve(
                handle,
                group,
                1,
                sample_count,
                0.0,
                self.samples.as_mut_ptr(),
                &mut any_null,
                &mut status,
            );
        }
        errors::check_status(status).map_err(invalid)?;
        // CFITSIO reads the random-group parameters without individual
        // PSCALn/PZEROn. Keep them separate from BSCALE/BZERO for the array.
        for (value, parameter) in self.values.iter_mut().zip(&self.parameters) {
            *value = *value * parameter.scale + parameter.zero;
        }
        Ok((
            self.indices.metadata(&self.values)?,
            &self.layout,
            &self.samples,
        ))
    }
}

pub(super) enum VisibilityGroups {
    Random(Box<RandomGroups>),
    Binary(Box<BinaryGroups>),
}

impl VisibilityGroups {
    pub(super) fn open(fits: &mut FitsFile) -> MsResult<Self> {
        let primary = fits.primary_hdu().map_err(invalid)?;
        let marker = optional_key::<i64>(&primary, fits, "NAXIS1")? == Some(777_777_701)
            && optional_key::<i64>(&primary, fits, "NAXIS2")? == Some(0);
        if marker {
            Ok(Self::Binary(Box::new(BinaryGroups::open(fits)?)))
        } else {
            Ok(Self::Random(Box::new(RandomGroups::read_header(fits)?)))
        }
    }

    pub(super) fn layout(&self) -> &GroupLayout {
        match self {
            Self::Random(groups) => &groups.layout,
            Self::Binary(groups) => &groups.layout,
        }
    }

    pub(super) fn count(&self) -> usize {
        match self {
            Self::Random(groups) => groups.count,
            Self::Binary(groups) => groups.rows,
        }
    }

    pub(super) fn binary_hdu(&self) -> Option<usize> {
        match self {
            Self::Random(_) => None,
            Self::Binary(groups) => Some(groups.hdu.number),
        }
    }

    pub(super) fn read(
        &mut self,
        fits: &mut FitsFile,
        index: usize,
    ) -> MsResult<(GroupMetadata, &GroupLayout, &[f32])> {
        match self {
            Self::Random(groups) => groups.read(fits, index),
            Self::Binary(groups) => groups.read(fits, index),
        }
    }
}

pub(super) struct BinaryGroups {
    hdu: FitsHdu,
    layout: GroupLayout,
    rows: usize,
    columns: Vec<String>,
    parameters: GroupParameters,
    values: Vec<f64>,
    samples: Vec<f32>,
    block: Range<usize>,
    block_columns: Vec<Vec<f64>>,
}

impl BinaryGroups {
    fn open(fits: &mut FitsFile) -> MsResult<Self> {
        let count = fits.num_hdus().map_err(invalid)?;
        for index in 1..count {
            if !binary_extension_name(fits, index)?.is_some_and(|name| name.contains("UV")) {
                continue;
            }
            let hdu = binary_extension(fits, index)?;
            let HduInfo::TableInfo {
                column_descriptions,
                num_rows,
            } = &hdu.info
            else {
                continue;
            };
            let Some(column) = column_descriptions
                .iter()
                .position(|col| col.name == "VISIBILITIES")
            else {
                continue;
            };
            let layout = GroupLayout::from_table(fits, &hdu, column + 1)?;
            if column_descriptions[column].data_type.repeat != layout.sample_count {
                return Err(invalid(
                    "UVFITS binary visibility row does not match its TDIM",
                ));
            }
            let rows = *num_rows;
            let keys = [
                "BASELINE", "ANTENNA1", "ANTENNA2", "SUBARRAY", "DATE", "SOURCE", "FREQSEL",
            ];
            let columns = column_descriptions
                .iter()
                .filter(|col| {
                    let name = col.name.as_str();
                    keys.contains(&name) || ["UU", "VV", "WW"].iter().any(|key| name.contains(key))
                })
                .map(|col| {
                    if col.data_type.repeat == 1 {
                        Ok(col.name.clone())
                    } else {
                        Err(invalid(format!(
                            "UVFITS column {} must be scalar",
                            col.name
                        )))
                    }
                })
                .collect::<MsResult<Vec<_>>>()?;
            let definitions = columns
                .iter()
                .map(|name| Parameter {
                    name: name.clone(),
                    scale: 1.0,
                    zero: 0.0,
                })
                .collect::<Vec<_>>();
            let parameters = GroupParameters::resolve(&definitions)?;
            let values = vec![0.0; columns.len()];
            return Ok(Self {
                hdu,
                layout,
                rows,
                columns,
                parameters,
                values,
                samples: Vec::new(),
                block: 0..0,
                block_columns: Vec::new(),
            });
        }
        Err(invalid("Cannot find a UVFITS binary visibility table"))
    }

    fn read(
        &mut self,
        fits: &mut FitsFile,
        index: usize,
    ) -> MsResult<(GroupMetadata, &GroupLayout, &[f32])> {
        if !self.block.contains(&index) {
            let count = (65_536 / self.layout.sample_count).clamp(1, 1024);
            let block = index..self.rows.min(index + count);
            self.samples = self
                .hdu
                .read_col_range(fits, "VISIBILITIES", &block)
                .map_err(invalid)?;
            if self.samples.len() != block.len() * self.layout.sample_count {
                return Err(invalid(
                    "UVFITS binary visibility row does not match its TDIM",
                ));
            }
            self.block_columns = self
                .columns
                .iter()
                .map(|column| {
                    self.hdu
                        .read_col_range(fits, column, &block)
                        .map_err(invalid)
                })
                .collect::<MsResult<Vec<_>>>()?;
            self.block = block;
        }
        let offset = index - self.block.start;
        for (value, column) in self.values.iter_mut().zip(&self.block_columns) {
            *value = *column
                .get(offset)
                .ok_or_else(|| invalid("Missing UVFITS binary row parameters"))?;
        }
        let row = offset * self.layout.sample_count;
        let samples = &self.samples[row..row + self.layout.sample_count];
        Ok((
            self.parameters.metadata(&self.values)?,
            &self.layout,
            samples,
        ))
    }
}

fn correlation_order(stokes: &Axis, binary: bool) -> MsResult<(Vec<Correlation>, Vec<usize>)> {
    if stokes.length > 12 {
        return Err(invalid("Too many UVFITS Stokes values"));
    }
    let inputs = (0..stokes.length)
        .map(|index| {
            let code = (stokes.coordinate(index) + 0.5).floor() as i32;
            let correlation = fits_correlation(code, binary)?;
            Ok((correlation, index))
        })
        .collect::<MsResult<Vec<_>>>()?;
    let mut inputs = inputs;
    inputs.sort_unstable_by_key(|&(correlation, _)| correlation.casacore_code());
    if inputs.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(invalid("Repeated UVFITS Stokes values"));
    }
    Ok(inputs.into_iter().unzip())
}

pub(super) fn fits_correlation(code: i32, allow_stokes: bool) -> MsResult<Correlation> {
    use Correlation::{I, LL, LR, Q, RL, RR, U, V, XX, XY, YX, YY};
    Ok(match code {
                -1 => RR,
                -2 => LL,
                -3 => RL,
                -4 => LR,
                -5 => XX,
                -6 => YY,
                -7 => XY,
                -8 => YX,
                1 if allow_stokes => I,
                2 if allow_stokes => Q,
                3 if allow_stokes => U,
                4 if allow_stokes => V,
                1..=4 => {
                    return Err(invalid(
                        "UVFITS Stokes parameters cannot be decomposed into correlations",
                    ));
                }
                _ => return Err(invalid(format!("Unknown UVFITS correlation type {code}"))),
    })
}

pub(super) fn binary_extension_name(fits: &mut FitsFile, index: usize) -> MsResult<Option<String>> {
    let number = i32::try_from(index + 1).map_err(invalid)?;
    let mut typ = 0;
    let mut status = 0;
    // SAFETY: the live handle belongs to fits and CFITSIO writes one int to each output.
    unsafe { sys::ffmahd(fits.as_raw(), number, &mut typ, &mut status) };
    errors::check_status(status).map_err(invalid)?;
    if typ != sys::BINARY_TBL as i32 {
        return Ok(None);
    }
    raw_text_key(fits, c"EXTNAME")
}

/// Read a recognized binary table after checking the descriptor types supported by fitsio.
pub(super) fn binary_extension(fits: &mut FitsFile, index: usize) -> MsResult<FitsHdu> {
    let mut fields = 0;
    let mut status = 0;
    // SAFETY: the live FITS handle is positioned on the binary extension by the caller.
    unsafe { sys::ffgncl(fits.as_raw(), &mut fields, &mut status) };
    errors::check_status(status).map_err(invalid)?;
    for column in 1..=fields {
        let key = CString::new(format!("TFORM{column}")).map_err(invalid)?;
        let form = raw_text_key(fits, &key)?.unwrap_or_default();
        let typ = form
            .trim_start_matches(|c: char| c.is_ascii_digit())
            .bytes()
            .next();
        if !typ.is_some_and(|code| b"LXBSEJDIKAUVW".contains(&code)) {
            return Err(invalid(format!(
                "Unsupported UVFITS binary column format {form:?}"
            )));
        }
    }
    let hdu = fits.hdu(index).map_err(invalid)?;
    let width = hdu.read_key::<i64>(fits, "NAXIS1").map_err(invalid)?;
    let rows = hdu.read_key::<i64>(fits, "NAXIS2").map_err(invalid)?;
    let width = usize::try_from(width).map_err(invalid)?;
    let rows = usize::try_from(rows).map_err(invalid)?;
    let bytes = rows
        .checked_mul(width)
        .ok_or_else(|| invalid("UVFITS table byte size overflow"))?;
    verify_payload(fits, bytes)?;
    Ok(hdu)
}

fn verify_payload(fits: &mut FitsFile, bytes: usize) -> MsResult<()> {
    let Some(last_byte) = bytes.checked_sub(1) else {
        return Ok(());
    };
    let offset = i64::try_from(last_byte).map_err(invalid)?;
    let mut value = 0_u8;
    let mut status = 0;
    // SAFETY: the FITS handle has the desired HDU selected. The function reads
    // exactly one byte at the checked data-relative offset into value.
    unsafe {
        sys::ffgextn(
            fits.as_raw(),
            offset,
            1,
            (&mut value as *mut u8).cast(),
            &mut status,
        )
    };
    errors::check_status(status)
        .map_err(|error| invalid(format!("Incomplete UVFITS payload: {error}")))
}

fn raw_text_key(fits: &mut FitsFile, key: &CStr) -> MsResult<Option<String>> {
    let mut value = [0 as c_char; sys::FLEN_VALUE as usize];
    let mut status = 0;
    // SAFETY: CFITSIO expects a NUL-terminated key and a FLEN_VALUE text
    // buffer. It terminates the returned value and permits omitting comments.
    unsafe {
        sys::ffgkys(
            fits.as_raw(),
            key.as_ptr(),
            value.as_mut_ptr(),
            ptr::null_mut(),
            &mut status,
        );
    }
    if status == sys::KEY_NO_EXIST as i32 {
        return Ok(None);
    }
    errors::check_status(status).map_err(invalid)?;
    // SAFETY: the buffer contains a terminated CFITSIO string.
    Ok(Some(
        unsafe { CStr::from_ptr(value.as_ptr()) }
            .to_string_lossy()
            .trim()
            .to_owned(),
    ))
}

fn positive_count(hdu: &FitsHdu, fits: &mut FitsFile, key: &str) -> MsResult<usize> {
    let value: i64 = hdu.read_key(fits, key).map_err(invalid)?;
    usize::try_from(value)
        .ok()
        .filter(|&value| value > 0)
        .ok_or_else(|| invalid(format!("Invalid UVFITS {key}: {value}")))
}

pub(super) fn optional_key<T: ReadsKey>(
    hdu: &FitsHdu,
    fits: &mut FitsFile,
    key: &str,
) -> MsResult<Option<T>> {
    match hdu.read_key(fits, key) {
        Ok(value) => Ok(Some(value)),
        Err(errors::Error::Fits(error)) if error.status == sys::KEY_NO_EXIST as i32 => Ok(None),
        Err(error) => Err(invalid(error)),
    }
}

pub(super) fn open_file(path: &Path) -> MsResult<FitsFile> {
    let name = path
        .to_str()
        .ok_or_else(|| invalid("UVFITS path must be UTF-8"))?;
    let name = CString::new(name).map_err(invalid)?;
    let mut handle = ptr::null_mut();
    let mut status = 0;
    // SAFETY: For a disk-only open CFITSIO takes a live C path and initializes
    // the output handle on success. FITS paths may contain literal '+' signs.
    unsafe { sys::ffdkopn(&mut handle, name.as_ptr(), 0, &mut status) };
    errors::check_status(status).map_err(invalid)?;
    // SAFETY: Transfer the non-null read-only handle to its owning wrapper.
    match unsafe { FitsFile::from_raw(handle, FileOpenMode::READONLY) } {
        Ok(fits) => Ok(fits),
        Err(error) => {
            // SAFETY: from_raw failed before transferring ownership.
            unsafe { sys::ffclos(handle, &mut status) };
            Err(invalid(error))
        }
    }
}

fn invalid(error: impl std::fmt::Display) -> MsError {
    MsError::InvalidInput(error.to_string())
}
