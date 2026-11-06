// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read CASA's per-plane FITS BEAMS extension.

use casa_types::{
    RecordValue, Value,
    quanta::{Quantity, Unit},
};
use fitsio::hdu::HduInfo;

use super::PrimaryHdu;
use crate::{GaussianBeam, ImageBeamSet, ImageError, StoredGaussianBeam};

pub(super) fn read_beams(
    primary: &mut PrimaryHdu,
) -> Result<Option<(ImageBeamSet, RecordValue)>, ImageError> {
    if primary.header.get("CASAMBM") != Some(&casa_coordinates::FitsValue::Logical(true)) {
        return Ok(None);
    }
    let fits = &mut primary.fits;
    let count = fits
        .num_hdus()
        .map_err(|error| ImageError::Io(error.to_string()))?;
    for index in 1..count {
        let hdu = fits
            .hdu(index)
            .map_err(|error| ImageError::Io(error.to_string()))?;
        let HduInfo::TableInfo {
            ref column_descriptions,
            num_rows,
        } = hdu.info
        else {
            continue;
        };
        let name = hdu.read_key::<String>(fits, "EXTNAME").unwrap_or_default();
        if !name.contains("BEAMS") || num_rows == 0 {
            continue;
        }
        let key = |fits: &mut fitsio::FitsFile, name: &str| {
            hdu.read_key::<i64>(fits, name)
                .map_err(|error| ImageError::Io(error.to_string()))
        };
        let channels = usize::try_from(key(fits, "NCHAN")?).unwrap_or(0);
        let stokes = usize::try_from(key(fits, "NPOL")?).unwrap_or(0);
        let size = channels
            .checked_mul(stokes)
            .filter(|&size| size > 0 && size <= num_rows)
            .ok_or_else(|| ImageError::InvalidMetadata("invalid FITS BEAMS dimensions".into()))?;
        let units = ["BMAJ", "BMIN", "BPA"].map(|name| {
            let column = column_descriptions
                .iter()
                .position(|column| column.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| {
                    ImageError::InvalidMetadata(format!("FITS BEAMS has no {name} column"))
                })?;
            let unit = hdu
                .read_key::<String>(fits, &format!("TUNIT{}", column + 1))
                .map_err(|error| ImageError::Io(error.to_string()))?;
            Unit::new(unit.trim()).map_err(|error| ImageError::InvalidMetadata(error.to_string()))
        });
        let [major_unit, minor_unit, angle_unit] = units;
        let (major_unit, minor_unit, angle_unit) = (major_unit?, minor_unit?, angle_unit?);
        let major = hdu
            .read_col::<f32>(fits, "BMAJ")
            .map_err(|error| ImageError::Io(error.to_string()))?;
        let minor = hdu
            .read_col::<f32>(fits, "BMIN")
            .map_err(|error| ImageError::Io(error.to_string()))?;
        let angle = hdu
            .read_col::<f32>(fits, "BPA")
            .map_err(|error| ImageError::Io(error.to_string()))?;
        let channel = hdu
            .read_col::<i32>(fits, "CHAN")
            .map_err(|error| ImageError::Io(error.to_string()))?;
        let polarization = hdu
            .read_col::<i32>(fits, "POL")
            .map_err(|error| ImageError::Io(error.to_string()))?;
        let mut beams =
            ImageBeamSet::with_shape(channels, stokes, GaussianBeam::new(0.0, 0.0, 0.0));
        let mut records = RecordValue::default();
        let mut seen = vec![false; size];
        for row in 0..num_rows {
            let chan = usize::try_from(channel[row]).unwrap_or(usize::MAX);
            let pol = usize::try_from(polarization[row]).unwrap_or(usize::MAX);
            if chan >= channels || pol >= stokes {
                return Err(ImageError::InvalidMetadata(
                    "FITS BEAMS row is outside its dimensions".into(),
                ));
            }
            let beam = StoredGaussianBeam::new(
                Quantity::with_unit(f64::from(major[row]), major_unit.clone()),
                Quantity::with_unit(f64::from(minor[row]), minor_unit.clone()),
                Quantity::with_unit(f64::from(angle[row]), angle_unit.clone()),
            )?;
            let record = beam.to_record();
            beams.set_beam(Some(chan), Some(pol), GaussianBeam::from_record(&record)?)?;
            let position = pol * channels + chan;
            seen[position] = true;
            records.upsert(format!("*{position}"), Value::Record(record));
        }
        if seen.iter().any(|present| !present) {
            return Err(ImageError::InvalidMetadata(
                "FITS BEAMS has missing planes".into(),
            ));
        }
        return Ok(Some((beams, records)));
    }
    Ok(None)
}
