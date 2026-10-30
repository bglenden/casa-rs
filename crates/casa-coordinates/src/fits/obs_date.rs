// SPDX-License-Identifier: LGPL-3.0-or-later
//! The observation date in a FITS header: `DATE-OBS` and `TIMESYS`.
//!
//! casacore's `ObsInfo::toFITS` writes the date with `FITSDateUtil::toFITS`:
//! `MVTime::string(MVTime::FITS, 12)` of the epoch in its own time scale,
//! `yyyy-mm-ddThh:mm:ss.ssssss`, with that scale in `TIMESYS`.
//! `FITSCoordinateUtil::getObsInfo` reads it back: `MJD-OBS` when the header
//! has it, else `DATE-OBS` (`FITSDateUtil::fromFITS`, which also reads
//! FITS's old `dd/mm/yy`), in the scale `TIMESYS` names, UTC without one.

use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::quanta::Quantity;
use casa_types::quanta::format::{FITS, format_time};

/// `DATE-OBS` and `TIMESYS` for `epoch`.
///
/// casacore names Terrestrial Time `TDT` and UT1 `UT`; this writes the FITS
/// standard's `TT` and `UT1`, which casacore reads as the same scales.
pub(super) fn fits_date(epoch: &MEpoch) -> (String, String) {
    (
        format_time(epoch.value().as_mjd(), FITS, 12),
        epoch.refer().as_str().to_string(),
    )
}

/// The epoch of a header's `MJD-OBS`, or else its `DATE-OBS`, in the scale
/// of its `TIMESYS`; `None` when it has neither date, or the date or scale
/// does not decode, as casacore then sets no date.
pub(super) fn obs_epoch(
    mjd_obs: Option<f64>,
    date_obs: Option<&str>,
    timesys: Option<&str>,
) -> Option<MEpoch> {
    let reference = match timesys.map(str::trim) {
        None | Some("") => EpochRef::UTC,
        Some(name) => name.parse().ok()?,
    };
    let mjd = match (mjd_obs, date_obs) {
        (Some(mjd), _) => mjd,
        (None, Some(date)) => date_mjd(date.trim())?,
        (None, None) => return None,
    };
    Some(MEpoch::from_mjd(mjd, reference))
}

/// `date` in MJD days: `yyyy-mm-dd[Thh:mm:ss[.s…]]`, or the old `dd/mm/yy`
/// of the twentieth century.
fn date_mjd(date: &str) -> Option<f64> {
    if date.contains('/') {
        let parts = date.split('/').collect::<Vec<_>>();
        let [day, month, year] = parts.as_slice() else {
            return None;
        };
        let two_digits = |part: &&str| part.len() == 2 && part.bytes().all(|b| b.is_ascii_digit());
        if ![day, month, year].into_iter().all(two_digits) {
            return None;
        }
        return days(&format!("19{year}/{month}/{day}"));
    }
    days(date)
}

/// `text` read as `MVTime::read` reads a date, in days.
fn days(text: &str) -> Option<f64> {
    let quantity = Quantity::read(text).ok()?;
    (quantity.unit().name() == "d").then(|| quantity.value())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mjd(epoch: &MEpoch) -> f64 {
        epoch.value().as_mjd()
    }

    #[test]
    fn a_date_is_written_as_casa_writes_it() {
        // CASA 6.7.6's exportfits of an image observed at MJD
        // 51544.123456789 UTC, and of one at 59000.5 TAI.
        assert_eq!(
            fits_date(&MEpoch::from_mjd(51_544.123_456_789, EpochRef::UTC)),
            ("2000-01-01T02:57:46.666569".to_string(), "UTC".to_string())
        );
        assert_eq!(
            fits_date(&MEpoch::from_mjd(59_000.5, EpochRef::TAI)),
            ("2020-05-31T12:00:00.000000".to_string(), "TAI".to_string())
        );
    }

    #[test]
    fn a_written_date_reads_back_in_its_scale() {
        for (value, reference) in [
            (51_544.123_456_789, EpochRef::UTC),
            (59_000.5, EpochRef::TAI),
            (60_000.25, EpochRef::TT),
        ] {
            let (date, timesys) = fits_date(&MEpoch::from_mjd(value, reference));
            let epoch = obs_epoch(None, Some(&date), Some(&timesys)).unwrap();
            assert_eq!(epoch.refer(), reference);
            // To the microsecond the date is written to.
            assert!((mjd(&epoch) - value).abs() * 86_400.0 < 1e-6, "{date}");
        }
    }

    #[test]
    fn headers_read_as_casacore_reads_them() {
        // No TIMESYS is UTC; casacore's aliases name their scales.
        let epoch = obs_epoch(None, Some("2020-05-31T12:00:00"), None).unwrap();
        assert_eq!((mjd(&epoch), epoch.refer()), (59_000.5, EpochRef::UTC));
        let epoch = obs_epoch(None, Some("2020-05-31"), Some("TDT ")).unwrap();
        assert_eq!((mjd(&epoch), epoch.refer()), (59_000.0, EpochRef::TT));
        // MJD-OBS before DATE-OBS.
        let epoch = obs_epoch(Some(51_544.5), Some("2020-05-31"), Some("IAT")).unwrap();
        assert_eq!((mjd(&epoch), epoch.refer()), (51_544.5, EpochRef::TAI));
        // FITS's old form, of the twentieth century.
        let epoch = obs_epoch(None, Some("31/12/99"), None).unwrap();
        assert_eq!(mjd(&epoch), 51_543.0);
    }

    #[test]
    fn a_date_or_scale_that_does_not_decode_gives_no_date() {
        // casa-rs's old placeholder, an unknown scale, and no date at all.
        assert!(obs_epoch(None, Some("MJD 51544.0000000000"), None).is_none());
        assert!(obs_epoch(None, Some("2020-05-31"), Some("GPS-ish")).is_none());
        assert!(obs_epoch(None, Some("1/2/3"), None).is_none());
        assert!(obs_epoch(None, None, Some("UTC")).is_none());
    }
}
