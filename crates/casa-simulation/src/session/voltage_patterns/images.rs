// SPDX-License-Identifier: LGPL-3.0-or-later
//! Admitted PBMath2DImage inputs selected from a VPManager table.
use super::{Simulator, SimulatorResult, raised, reserve, text};
use crate::predict::jones::{ImagePlane, JonesImage};
use casa_components::Polarisation;
use casa_coordinates::{CoordinateModel, CoordinateType};
use casa_images::{ImagePixel, PagedImage};
use casa_imaging_runtime::Admitted;
use casa_types::RecordValue;
use ndarray::IxDyn;
use num_complex::Complex32;
use std::{path::Path, sync::Arc};

const CACHE: usize = 64 << 10;

fn reading_bytes(path: &Path) -> SimulatorResult<u64> {
    let rows = casa_tables::Table::rows_on_disk(path).map_err(raised)? as u64;
    let mut bytes = rows.saturating_mul(4096).saturating_add(256 << 10);
    for entry in std::fs::read_dir(path).map_err(raised)? {
        let entry = entry.map_err(raised)?;
        let metadata = entry.metadata().map_err(raised)?;
        if metadata.is_file() {
            // Tiled pixel files contain fixed-width numeric data, unlike
            // VPManager record columns. Budget a wire copy and decoded data;
            // metadata keeps the record reader's larger expansion bound.
            let factor = if entry.file_name().to_string_lossy().contains("_TSM") {
                2
            } else {
                4
            };
            bytes = bytes.saturating_add(metadata.len().saturating_mul(factor));
        }
    }
    Ok(bytes.saturating_add(CACHE as u64))
}

fn read<T: ImagePixel + Copy>(
    session: &Simulator,
    path: &Path,
    convert: impl Fn(T) -> Complex32,
) -> SimulatorResult<Admitted<ImagePlane>> {
    let _reading = reserve(session, reading_bytes(path)?)?;
    let image = PagedImage::<T>::open_with_cache(path, CACHE).map_err(raised)?;
    let shape = image.shape();
    let coords = image.coordinates();
    if shape.len() != 4
        || shape[0] == 0
        || shape[1] == 0
        || shape[2] != 4
        || shape[3] == 0
        || coords.find_coordinate(CoordinateType::Direction) != Some(0)
        || coords.find_coordinate(CoordinateType::Stokes) != Some(1)
        || coords.find_coordinate(CoordinateType::Spectral) != Some(2)
    {
        return Err(raised(
            "Jones images require direction, four correlation and spectral axes",
        ));
    }
    let codes = (0..4)
        .map(|p| {
            coords
                .coordinate(1)
                .to_world(&[p as f64])
                .map(|v| v[0] as i32)
                .map_err(raised)
        })
        .collect::<SimulatorResult<Vec<_>>>()?;
    let (basis, first) = if codes.iter().all(|v| (5..=8).contains(v)) {
        (Polarisation::Circular, 5)
    } else if codes.iter().all(|v| (9..=12).contains(v)) {
        (Polarisation::Linear, 9)
    } else {
        return Err(raised(
            "Jones image polarization is neither circular nor linear",
        ));
    };
    let positions: [usize; 4] = std::array::from_fn(|p| {
        codes
            .iter()
            .position(|v| *v == first + p as i32)
            .unwrap_or(4)
    });
    if positions.contains(&4) {
        return Err(raised("Jones image must contain all four correlations"));
    }
    let CoordinateModel::Direction(direction) = coords.coordinate(0) else {
        unreachable!()
    };
    let spectral = coords.coordinate(2);
    let frequency_hz = casa_types::quanta::Quantity::new(
        spectral.to_world(&[0.]).map_err(raised)?[0],
        &spectral.axis_units()[0],
    )
    .and_then(|value| value.get_value_in(&casa_types::quanta::Unit::new("Hz")?))
    .map_err(raised)?;
    if !frequency_hz.is_finite() || frequency_hz <= 0. {
        return Err(raised("invalid Jones image frequency"));
    }
    let count = shape[0]
        .checked_mul(shape[1])
        .ok_or_else(|| raised("Jones image size overflow"))?;
    let bytes = (count as u64)
        .checked_mul(size_of::<[Complex32; 4]>() as u64)
        .ok_or_else(|| raised("Jones image size overflow"))?;
    let mut reservation = reserve(session, 2 * bytes + (16 << 10))?;
    let values = image
        .get_slice(&[0, 0, 0, 0], &[shape[0], shape[1], 4, 1])
        .map_err(raised)?;
    let pixels: Vec<[Complex32; 4]> = (0..count)
        .map(|i| {
            std::array::from_fn(|p| {
                convert(values[IxDyn(&[i % shape[0], i / shape[0], positions[p], 0])])
            })
        })
        .collect();
    if pixels
        .iter()
        .flatten()
        .any(|v| !v.re.is_finite() || !v.im.is_finite())
    {
        return Err(raised("Jones image contains a non-finite voltage sample"));
    }
    reservation.retain(bytes + (16 << 10));
    Ok(Admitted {
        value: ImagePlane {
            shape: [shape[0], shape[1]],
            pixels,
            direction: direction.clone(),
            frequency_hz,
            basis,
        },
        reservation,
    })
}

pub(super) fn load(session: &Simulator, record: &RecordValue) -> SimulatorResult<Arc<JonesImage>> {
    let (real, imaginary) = if record.get("compleximage").is_some() {
        (
            read::<Complex32>(session, Path::new(text(record, "compleximage")?), |v| v)?,
            None,
        )
    } else {
        let real = read::<f32>(session, Path::new(text(record, "realimage")?), |v| {
            Complex32::new(v, 0.)
        })?;
        let path = Path::new(text(record, "imagimage")?);
        let imaginary = if path.as_os_str().is_empty() || !path.exists() {
            None
        } else {
            Some(read::<f32>(session, path, |v| Complex32::new(v, 0.))?)
        };
        if imaginary
            .as_ref()
            .is_some_and(|v| v.value.shape != real.value.shape)
        {
            return Err(raised(
                "real and imaginary primary beam images have different shapes",
            ));
        }
        (real, imaginary)
    };
    Ok(Arc::new(JonesImage { real, imaginary }))
}
