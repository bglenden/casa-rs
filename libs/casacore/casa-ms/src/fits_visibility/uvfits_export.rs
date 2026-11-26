// SPDX-License-Identifier: LGPL-3.0-or-later
//! Write selected MeasurementSet visibilities as AIPS random groups.

use std::ffi::{CString, c_long};
use std::path::Path;
use std::ptr;

use casa_core::measures::frequency::FrequencyRef;
use casa_core::quanta::format::{FITS, NO_TIME, format_time};
use casa_core::{ArrayValue, Complex32, ScalarValue};
use casa_tables::Table;
use fitsio::{FileOpenMode, FitsFile, errors, hdu::FitsHdu, sys};

use super::export_layout::{OutputGroup, OutputLayout, VisibilityRow, has_column};
use super::export_metadata::{ExportHeader, write_extensions};
use super::invalid;
use crate::{MeasurementSet, MsResult, MsSelection};

/// Data and metadata selections for AIPS UVFITS export.
#[derive(Clone, Debug)]
pub struct UvFitsExportOptions<'a> {
    /// Visibility column requested. If scratch data are absent, use DATA.
    pub column: &'a str,
    /// Main-table row and spectral channel selectors.
    pub selection: MsSelection,
    /// Include system calibration and gain curves where available.
    pub writesyscal: bool,
    /// Preserve source identities in a separate AIPS SU table.
    pub multisource: bool,
    /// Combine simultaneous spectral windows into the UVFITS IF axis.
    pub combinespw: bool,
    /// Prefer station names in the antenna extension.
    pub writestation: bool,
    /// Fill missing IFs in a combined group with flagged zero data.
    pub padwithflags: bool,
    /// Replace an existing destination after the new file has been written.
    pub overwrite: bool,
}

/// Write the selected rows as CASA-compatible AIPS UVFITS random groups.
///
/// # Errors
/// The selection, data or axes cannot be represented in UVFITS, or the
/// destination could not be written. An existing file requires overwrite.
pub fn export_uvfits(
    ms: &MeasurementSet,
    output: &Path,
    options: &UvFitsExportOptions<'_>,
) -> MsResult<()> {
    if output.as_os_str().is_empty() || output.file_name().is_none() {
        return Err(invalid("No UVFITS output filename was specified"));
    }
    if !options.overwrite && output.symlink_metadata().is_ok() {
        return Err(invalid(format!(
            "UVFITS output {} already exists",
            output.display()
        )));
    }
    let layout = OutputLayout::resolve(ms, options)?;
    let metadata = ExportHeader::read(ms, &layout)?;
    let directory = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let stage = tempfile::tempdir_in(directory).map_err(invalid)?;
    let payload = stage.path().join("visibility.fits");
    let mut fits = new_primary(&payload, &layout, &metadata)?;
    let table = ms.main_table();
    let column = visibility_column(table, options.column);
    write_primary_keys(&mut fits, &layout, &metadata, table, column)?;
    write_groups(&mut fits, table, column, &layout, &metadata.antenna_numbers)?;
    write_extensions(&mut fits, ms, &layout, &metadata, options)?;
    let mut status = 0;
    // SAFETY: the live handle is borrowed from fits and the status is writable.
    unsafe {
        sys::ffflus(fits.as_raw(), &mut status);
    }
    errors::check_status(status).map_err(invalid)?;
    drop(fits);
    if options.overwrite {
        std::fs::rename(&payload, output).map_err(invalid)
    } else {
        std::fs::hard_link(&payload, output).map_err(invalid)
    }
}

fn new_primary(path: &Path, layout: &OutputLayout, metadata: &ExportHeader) -> MsResult<FitsFile> {
    let path = CString::new(path.as_os_str().as_encoded_bytes()).map_err(invalid)?;
    let mut handle = ptr::null_mut();
    let mut status = 0;
    let base = if metadata.separate_baselines() { 9 } else { 7 };
    let parameters = base + 2 * i64::from(layout.multi);
    let ifs = if layout.combine {
        layout.windows.len()
    } else {
        1
    };
    let mut axes = [
        0,
        3,
        layout.correlations.len() as i64,
        layout.channels.len() as i64,
        ifs as i64,
        1,
        1,
    ];
    // SAFETY: the path is NUL-terminated. The dimensions and status outlive
    // the calls, and the raw handle is either closed or transferred.
    unsafe {
        sys::ffdkinit(&mut handle, path.as_ptr(), &mut status);
        if status == 0 {
            sys::ffphprll(
                handle,
                1,
                -32,
                axes.len() as i32,
                axes.as_mut_ptr(),
                parameters,
                layout.groups.len() as i64,
                1,
                &mut status,
            );
        }
        if status == 0 {
            match FitsFile::from_raw(handle, FileOpenMode::READWRITE) {
                Ok(fits) => return Ok(fits),
                Err(error) => {
                    sys::ffclos(handle, &mut 0);
                    return Err(invalid(error));
                }
            }
        }
        if !handle.is_null() {
            sys::ffclos(handle, &mut 0);
        }
    }
    errors::check_status(status).map_err(invalid)?;
    Err(invalid(
        "Could not create the UVFITS random-groups primary HDU",
    ))
}

fn write_primary_keys(
    fits: &mut FitsFile,
    layout: &OutputLayout,
    metadata: &ExportHeader,
    table: &Table,
    column: &str,
) -> MsResult<()> {
    let hdu = fits.primary_hdu().map_err(invalid)?;
    for (key, value) in [
        ("OBJECT", metadata.object.as_str()),
        ("TELESCOP", metadata.telescope.as_str()),
        ("INSTRUME", metadata.telescope.as_str()),
        ("OBSERVER", metadata.observer.as_str()),
        ("SORTORD", "TB"),
        ("ORIGIN", "casacore"),
        ("SPECSYS", fits_frame(layout.frame)?),
        ("DATE-OBS", &fits_date(metadata.time, false)),
    ] {
        hdu.write_key(fits, key, value).map_err(invalid)?;
    }
    let unit = match table
        .column_keywords(column)
        .and_then(|keywords| keywords.get("QuantumUnit"))
    {
        Some(casa_core::Value::Scalar(ScalarValue::String(value))) => value.to_uppercase(),
        _ => "UNCALIB".to_owned(),
    };
    hdu.write_key(fits, "BUNIT", unit).map_err(invalid)?;
    hdu.write_key(fits, "EPOCH", metadata.epoch)
        .map_err(invalid)?;
    if metadata.rest_frequency > 0.0 {
        hdu.write_key(fits, "RESTFREQ", metadata.rest_frequency)
            .map_err(invalid)?;
    }
    write_group_axes(fits, &hdu, layout, metadata)?;
    let mut parameters = vec!["UU", "VV", "WW", "DATE", "DATE"];
    if metadata.separate_baselines() {
        parameters.extend(["SUBARRAY", "ANTENNA1", "ANTENNA2"]);
    } else {
        parameters.push("BASELINE");
    }
    parameters.push("FREQSEL");
    if layout.multi {
        parameters.extend(["SOURCE", "INTTIM"]);
    }
    for (index, name) in (1..).zip(parameters) {
        hdu.write_key(fits, &format!("PTYPE{index}"), name)
            .map_err(invalid)?;
        hdu.write_key(fits, &format!("PSCAL{index}"), 1.0)
            .map_err(invalid)?;
        hdu.write_key(fits, &format!("PZERO{index}"), 0.0)
            .map_err(invalid)?;
    }
    let mut status = 0;
    // SAFETY: the FITS handle is live and the history string is NUL-terminated.
    unsafe {
        sys::ffpdat(fits.as_raw(), &mut status);
        sys::ffphis(fits.as_raw(), c"AIPS WTSCAL = 1.0".as_ptr(), &mut status);
    }
    errors::check_status(status).map_err(invalid)
}

fn write_group_axes(
    fits: &mut FitsFile,
    hdu: &FitsHdu,
    layout: &OutputLayout,
    metadata: &ExportHeader,
) -> MsResult<()> {
    let stokes = layout.correlations[0].0;
    let stokes_step = layout.correlations.get(1).map_or(1, |next| next.0 - stokes);
    for (axis, (name, reference, increment, pixel)) in (2..).zip([
        ("COMPLEX", 1.0, 1.0, 1.0),
        ("STOKES", f64::from(stokes), f64::from(stokes_step), 1.0),
        ("FREQ", layout.reference, layout.step, layout.pixel as f64),
        ("IF", 1.0, 1.0, 1.0),
        ("RA", metadata.direction_deg[0], 1.0, 1.0),
        ("DEC", metadata.direction_deg[1], 1.0, 1.0),
    ]) {
        hdu.write_key(fits, &format!("CTYPE{axis}"), name)
            .map_err(invalid)?;
        for (key, value) in [
            ("CRVAL", reference),
            ("CDELT", increment),
            ("CRPIX", pixel),
            ("CROTA", 0.0),
        ] {
            hdu.write_key(fits, &format!("{key}{axis}"), value)
                .map_err(invalid)?;
        }
    }
    Ok(())
}

fn fits_frame(frame: i32) -> MsResult<&'static str> {
    use FrequencyRef::*;
    match FrequencyRef::from_casacore_code(frame) {
        Some(REST) => Ok("SOURCE"),
        Some(LSRK) => Ok("LSRK"),
        Some(LSRD) => Ok("LSRD"),
        Some(BARY) => Ok("BARYCENT"),
        Some(GEO) => Ok("GEOCENTR"),
        Some(TOPO) => Ok("TOPOCENT"),
        Some(GALACTO) => Ok("GALACTOC"),
        Some(LGROUP) => Ok("LOCALGRP"),
        Some(CMB) => Ok("CMBDIPOL"),
        _ => Err(invalid(format!(
            "Frequency reference code {frame} cannot be written to UVFITS"
        ))),
    }
}

pub(super) fn fits_date(time: f64, no_time: bool) -> String {
    format_time(
        time / 86_400.0,
        FITS | if no_time { NO_TIME } else { 0 },
        12,
    )
}

fn visibility_column(table: &Table, name: &str) -> &'static str {
    match name.to_ascii_uppercase().as_str() {
        "CORRECTED" | "CORRECTED_DATA" if has_column(table, "CORRECTED_DATA") => "CORRECTED_DATA",
        "MODEL" | "MODEL_DATA" if has_column(table, "MODEL_DATA") => "MODEL_DATA",
        _ => "DATA",
    }
}

fn write_groups(
    fits: &mut FitsFile,
    table: &Table,
    column: &str,
    layout: &OutputLayout,
    antennas: &[i32],
) -> MsResult<()> {
    let separate = antennas.iter().any(|&id| id >= 256);
    let samples_per_if = 3 * layout.correlations.len() * layout.channels.len();
    let ifs = if layout.combine {
        layout.windows.len()
    } else {
        1
    };
    let mut samples = vec![0.0; samples_per_if * ifs];
    let mut parameters = Vec::with_capacity(11);
    let target_shape = [layout.correlations.len(), layout.input_channels];
    // CASA decides globally from the first selected input row before sorting.
    let first = layout
        .rows
        .iter()
        .map(|row| row.index)
        .min()
        .ok_or_else(|| invalid("No UVFITS rows"))?;
    let spectrum = has_column(table, "WEIGHT_SPECTRUM")
        && table
            .array_shape(first, "WEIGHT_SPECTRUM")?
            .is_some_and(|shape| shape == target_shape);
    let batch_groups = visibility_batch_groups(
        layout.input_channels,
        layout.correlations.len(),
        ifs,
        spectrum,
    );
    let mut number = 0;
    for groups in layout.groups.chunks(batch_groups) {
        let mut visibility = read_visibility_block(table, column, layout, groups, spectrum)?;
        let rows = groups
            .iter()
            .map(|group| layout.rows[group.first].index)
            .collect::<Vec<_>>();
        let uvw = table
            .column_accessor("UVW")?
            .array_cells_owned_uncached(&rows)?;
        for (group, coordinates) in groups.iter().zip(uvw) {
            for (slot, output) in group
                .slots
                .iter()
                .zip(samples.chunks_exact_mut(samples_per_if))
            {
                if let Some(row) = slot {
                    let values = visibility
                        .next()
                        .ok_or_else(|| invalid("Missing UVFITS visibility cells"))?;
                    write_samples(values, &layout.rows[*row], layout, output)?;
                } else {
                    output.fill(0.0);
                }
            }
            parameters.clear();
            let Some(ArrayValue::Float64(coordinates)) = coordinates else {
                return Err(invalid("Invalid UVW coordinates"));
            };
            parameters.extend(group_parameters(
                &coordinates,
                &layout.rows[group.first],
                layout,
                antennas,
                separate,
            )?);
            number += 1;
            write_group(fits, number, &mut parameters, &mut samples)?;
        }
    }
    Ok(())
}

fn visibility_batch_groups(
    channels: usize,
    correlations: usize,
    ifs: usize,
    spectrum: bool,
) -> usize {
    const BUDGET: usize = 16 * 1024 * 1024;
    let sample = size_of::<casa_core::Complex32>()
        + size_of::<bool>()
        + usize::from(spectrum) * size_of::<f32>();
    let row_sample_bytes = channels
        .saturating_mul(sample)
        .saturating_add(size_of::<f32>());
    let visibility = correlations
        .saturating_mul(row_sample_bytes)
        .saturating_add(4 * size_of::<ArrayValue>());
    let group = ifs
        .saturating_mul(visibility)
        .saturating_add(3 * size_of::<f64>());
    (BUDGET / group.max(1)).clamp(1, 1024)
}

#[cfg(test)]
#[test]
fn uvfits_export_batches_follow_input_visibility_size() {
    assert_eq!(visibility_batch_groups(63, 2, 1, true), 1024);
    assert!(visibility_batch_groups(131_072, 4, 2, true) <= 2);
    assert_eq!(visibility_batch_groups(1_048_576, 4, 2, true), 1);
}

fn write_group(
    fits: &mut FitsFile,
    number: c_long,
    parameters: &mut [f64],
    samples: &mut [f32],
) -> MsResult<()> {
    let mut status = 0;
    // SAFETY: the buffers have their declared lengths and the FITS handle is live.
    unsafe {
        let handle = fits.as_raw();
        sys::ffpgpd(
            handle,
            number,
            1,
            parameters.len() as c_long,
            parameters.as_mut_ptr(),
            &mut status,
        );
        sys::ffppre(
            handle,
            number,
            1,
            samples.len() as i64,
            samples.as_mut_ptr(),
            &mut status,
        );
    }
    errors::check_status(status).map_err(invalid)
}

struct VisibilityValues {
    data: Option<ArrayValue>,
    flags: Option<ArrayValue>,
    weight: Option<ArrayValue>,
    spectrum: Option<ArrayValue>,
}

fn read_visibility_block(
    table: &Table,
    column: &str,
    layout: &OutputLayout,
    groups: &[OutputGroup],
    spectrum: bool,
) -> MsResult<impl Iterator<Item = VisibilityValues>> {
    let rows = groups
        .iter()
        .flat_map(|group| group.slots.iter().flatten())
        .map(|&position| layout.rows[position].index)
        .collect::<Vec<_>>();
    let read = |name| {
        table
            .column_accessor(name)?
            .array_cells_owned_uncached(&rows)
    };
    let data = read(column)?;
    let flags = read("FLAG")?;
    let weight = read("WEIGHT")?;
    let spectrum = if spectrum {
        read("WEIGHT_SPECTRUM")?
    } else {
        vec![None; rows.len()]
    };
    Ok(data.into_iter().zip(flags).zip(weight).zip(spectrum).map(
        |(((data, flags), weight), spectrum)| VisibilityValues {
            data,
            flags,
            weight,
            spectrum,
        },
    ))
}

fn group_parameters(
    uvw: &ndarray::ArrayD<f64>,
    row: &VisibilityRow,
    layout: &OutputLayout,
    antennas: &[i32],
    separate: bool,
) -> MsResult<impl Iterator<Item = f64>> {
    let mut parameters = [0.0; 11];
    if uvw.len() != 3 {
        return Err(invalid("UVW must store three coordinates"));
    }
    for (output, value) in parameters.iter_mut().zip(uvw.iter()) {
        *output = value / 299_792_458.0;
    }
    let julian_date = 2_400_000.5 + row.time / 86_400.0;
    parameters[3] = julian_date.floor();
    parameters[4] = julian_date.fract();
    let mut pair = [0; 2];
    for (entry, id) in pair.iter_mut().zip(row.antenna) {
        *entry = *antennas
            .get(id as usize)
            .ok_or_else(|| invalid("Invalid antenna index"))?;
    }
    let array = row.array;
    let mut count = 5;
    if separate {
        parameters[5..8].copy_from_slice(&[
            f64::from(array + 1),
            f64::from(pair[0]),
            f64::from(pair[1]),
        ]);
        count += 3;
    } else {
        parameters[5] = f64::from(pair[0] * 256 + pair[1]) + f64::from(array) * 0.01;
        count += 1;
    }
    parameters[count] = if layout.combine {
        1.0
    } else {
        row.window as f64 + 1.0
    };
    count += 1;
    if layout.multi {
        parameters[count] = f64::from(layout.fields[&row.field]);
        parameters[count + 1] = row.exposure;
        count += 2;
    }
    Ok(parameters.into_iter().take(count))
}

fn write_samples(
    values: VisibilityValues,
    row: &VisibilityRow,
    layout: &OutputLayout,
    output: &mut [f32],
) -> MsResult<()> {
    let VisibilityValues {
        data,
        flags,
        weight,
        spectrum,
    } = values;
    let (
        Some(ArrayValue::Complex32(data)),
        Some(ArrayValue::Bool(flags)),
        Some(ArrayValue::Float32(weight)),
    ) = (data, flags, weight)
    else {
        return Err(invalid(
            "UVFITS needs complex DATA, Boolean FLAG and float32 WEIGHT",
        ));
    };
    let spectrum = match &spectrum {
        Some(ArrayValue::Float32(values)) if values.shape() == data.shape() => Some(values),
        _ => None,
    };
    let shape = [layout.correlations.len(), layout.input_channels];
    if data.shape() != shape || flags.shape() != shape || weight.len() != shape[0] {
        return Err(invalid(
            "Visibility, flag and weight cells do not match their data description",
        ));
    }
    let indices = layout.channels.iter().flat_map(|&channel| {
        layout
            .correlations
            .iter()
            .map(move |&(_, corr)| [corr, channel])
    });
    for (sample, [corr, channel]) in output.as_chunks_mut::<3>().0.iter_mut().zip(indices) {
        if let Some(width) = layout.astron_bin_width {
            let samples = (channel..channel + width)
                .map(|channel| [corr, channel])
                .filter(|&index| !flags[index]);
            let (sum, total) = samples.fold((Complex32::ZERO, 0.0), |(sum, total), index| {
                let weight = spectrum.map_or(weight[[corr]], |values| values[index]);
                (sum + data[index] * weight, total + weight)
            });
            let value = if total > 0.0 {
                sum / total
            } else {
                Complex32::ZERO
            };
            let total = if row.flagged { -total } else { total };
            *sample = [value.re, value.im, total];
            continue;
        }
        let index = [corr, channel];
        let visibility = data[index];
        let weight = spectrum.map_or(weight[[corr]] / shape[1] as f32, |values| values[index]);
        sample.copy_from_slice(&[
            visibility.re,
            visibility.im,
            if row.flagged {
                -weight.abs()
            } else if flags[index] {
                -weight.max(0.0)
            } else {
                weight
            },
        ]);
    }
    Ok(())
}
