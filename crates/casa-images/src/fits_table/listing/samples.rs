// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ffi::c_long;
use std::fmt::Write;

use fitsio::sys;

use super::{Debug, FitsListingMessage, Info, integer, real};
use crate::fits_table::{Reader, Result, cards::Header, column::Column, fits_status};

pub(super) fn image(
    reader: &mut Reader,
    axes: &[i64],
    messages: &mut Vec<FitsListingMessage>,
) -> Result<()> {
    let &[width, height] = axes else {
        return Ok(());
    };
    let width = width.min(60) as usize;
    let height = height.min(60) as usize;
    if width == 0 || height == 0 {
        return Ok(());
    }
    let mut pixels = vec![0.0; width * height];
    let mut start = [1, 1];
    let mut end = [width as c_long, height as c_long];
    let mut step = [1, 1];
    let (mut any_null, mut status) = (0, 0);
    // SAFETY: Two-axis vectors describe the explicitly bounded subset;
    // pixels has an f64 for each requested position.
    unsafe {
        sys::ffgsvd(
            reader.ptr,
            1,
            2,
            [axes[0] as c_long, axes[1] as c_long].as_mut_ptr(),
            start.as_mut_ptr(),
            end.as_mut_ptr(),
            step.as_mut_ptr(),
            0.0,
            pixels.as_mut_ptr(),
            &mut any_null,
            &mut status,
        );
    }
    fits_status(status)?;
    let mut display = String::new();
    for x in 0..width {
        for y in 0..height {
            let _ = writeln!(display, "({x},{y}) = {}", real(pixels[y * width + x]));
        }
    }
    messages.push(Info(display));
    Ok(())
}

pub(super) fn groups(
    reader: &mut Reader,
    header: &Header,
    axes: &[i64],
    messages: &mut Vec<FitsListingMessage>,
) -> Result<()> {
    let group_count = integer(header, "GCOUNT");
    let parameter_count = integer(header, "PCOUNT") as c_long;
    let len = axes
        .get(1..)
        .unwrap_or_default()
        .iter()
        .fold(1i64, |size, axis| size.saturating_mul(*axis))
        .min(18);
    let mut display =
        format!("{group_count} groups total, display first 6 elements of the first 2 groups\n");
    for group in 1..=group_count.min(2) as c_long {
        let _ = write!(display, "Group {} parms: ", group - 1);
        group_parameters(reader, header, group, parameter_count, &mut display)?;
        let _ = write!(display, "\nGroup {} data: ", group - 1);
        let mut data = [0.0; 18];
        let (mut any_null, mut status) = (0, 0);
        // SAFETY: len is bounded by data and only an existing group is read.
        unsafe {
            sys::ffgpvd(
                reader.ptr,
                group,
                1,
                len,
                0.0,
                data.as_mut_ptr(),
                &mut any_null,
                &mut status,
            )
        };
        fits_status(status)?;
        for &value in &data[..len as usize] {
            let _ = write!(display, " {}", real(value));
        }
        display.push_str("... \n");
    }
    messages.push(Info(display));
    Ok(())
}

fn group_parameters(
    reader: &mut Reader,
    header: &Header,
    group: c_long,
    count: c_long,
    display: &mut String,
) -> Result<()> {
    let mut values = [0.0; 64];
    for start in (1..=count).step_by(values.len()) {
        let len = (count - start + 1).min(values.len() as c_long);
        let mut status = 0;
        // SAFETY: len is bounded by the buffer and the group's parameter count.
        unsafe {
            sys::ffggpd(
                reader.ptr,
                group,
                start,
                len,
                values.as_mut_ptr(),
                &mut status,
            )
        };
        fits_status(status)?;
        for (index, &raw) in (start..).zip(&values[..len as usize]) {
            let scale = header.number(&format!("PSCAL{index}")).unwrap_or(1.0);
            let zero = header.number(&format!("PZERO{index}")).unwrap_or(0.0);
            let _ = write!(display, " {}", real(raw * scale + zero));
        }
    }
    Ok(())
}

pub(super) fn table(
    reader: &mut Reader,
    header: &Header,
    axes: &[i64],
    bytes: u64,
    messages: &mut Vec<FitsListingMessage>,
) -> Result<()> {
    let rows = axes.get(1).copied().unwrap_or(0);
    let columns = integer(header, "TFIELDS");
    let main_bytes = axes.first().copied().unwrap_or(0).saturating_mul(rows);
    let heap_offset = header
        .number("THEAP")
        .map_or(main_bytes, |offset| offset as i64);
    let heap_bytes = bytes.saturating_sub(heap_offset.max(main_bytes) as u64);
    messages.push(Info(format!(
        "{rows} rows, {columns} cols, {bytes} bytes total\n"
    )));
    let mut description = String::from("\nTable Data\n");
    let mut displayed = Vec::new();
    for index in 1..=columns as i32 {
        let column = Column::describe_listing(reader, header, index)?;
        column.listing_description(header, &mut description);
        if index <= 20 {
            displayed.push(column);
        }
    }
    messages.push(Debug(description));
    messages.push(Info("the first 20 cols of the first 5 rows:\n".into()));
    let mut preview = String::new();
    for row in 1..=rows.min(5) {
        let _ = write!(preview, "{}: | ", row - 1);
        for (index, column) in displayed.iter().enumerate() {
            if index > 0 {
                preview.push_str("| ");
            }
            column.listing_cell(reader, row, heap_bytes, &mut preview)?;
        }
        preview.push_str(if columns > 20 { "| ...\n" } else { "|\n" });
    }
    if !preview.is_empty() {
        messages.push(Info(preview));
    }
    Ok(())
}
