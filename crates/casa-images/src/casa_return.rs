// SPDX-License-Identifier: LGPL-3.0-or-later
//! What CASA's image tasks return, built from casa-rs's summaries (#931).
//!
//! CASA's `imstat` returns its statistics record and `imhead` its header
//! record, dictionaries whose entries are NumPy arrays with CASA's dtypes
//! and shapes, their keys in alphabetical order. These functions build the
//! entries casa-rs computes. The keys CASA returns that casa-rs does not
//! compute yet are listed with each function and tracked in #977.

use casa_provider_contracts::{CasaEntry, CasaKey, CasaValue};

use crate::analysis::{ImageHeaderSummary, ImageStatisticsSummary, RestoringBeamSummary};

/// `imstat`'s statistics record over the whole selection, as CASA returns
/// it with `axes=[]`: each statistic a one-element `float64` array, and the
/// corners and extremum positions `int64` pixel arrays.
///
/// Not computed yet (#977): `blcf`, `trcf`, `minposf` and `maxposf` (the
/// formatted world coordinates) and the robust statistics `medabsdevmed`,
/// `q1`, `q3` and `quartile`.
pub fn imstat_record(summary: &ImageStatisticsSummary) -> CasaValue {
    let statistic = |value: f64| CasaValue::float64_vector([value]);
    let mut entries = vec![
        ("blc", pixels(&summary.blc)),
        ("max", statistic(summary.max)),
        ("mean", statistic(summary.mean)),
        ("median", statistic(summary.median)),
        ("min", statistic(summary.min)),
        ("npts", statistic(summary.npts)),
        ("rms", statistic(summary.rms)),
        ("sigma", statistic(summary.sigma)),
        ("sum", statistic(summary.sum)),
        ("sumsq", statistic(summary.sumsq)),
        ("trc", pixels(&summary.trc)),
    ];
    if let Some(flux) = summary.flux {
        entries.push(("flux", statistic(flux)));
    }
    if let Some(position) = &summary.maxpos {
        entries.push(("maxpos", pixels(position)));
    }
    if let Some(position) = &summary.minpos {
        entries.push(("minpos", pixels(position)));
    }
    sorted(entries)
}

/// `imhead`'s `summary` record.
///
/// Not computed yet (#977): `messages` and `tileshape`.
pub fn imhead_summary_record(summary: &ImageHeaderSummary) -> CasaValue {
    let axes = &summary.axes;
    let mut entries = vec![
        (
            "axisnames",
            CasaValue::str_vector(axes.iter().map(|axis| axis.name.clone())),
        ),
        (
            "axisunits",
            CasaValue::str_vector(axes.iter().map(|axis| axis.unit.clone())),
        ),
        (
            "defaultmask",
            CasaValue::string(summary.default_mask.clone().unwrap_or_default()),
        ),
        (
            "hasmask",
            CasaValue::Bool {
                value: !summary.masks.is_empty(),
            },
        ),
        ("imagetype", CasaValue::string(summary.image_type.clone())),
        (
            "incr",
            CasaValue::float64_vector(axes.iter().map(|axis| axis.increment)),
        ),
        ("masks", CasaValue::str_vector(summary.masks.clone())),
        (
            "ndim",
            CasaValue::Int {
                value: summary.shape.len() as i64,
            },
        ),
        (
            "refpix",
            CasaValue::float64_vector(axes.iter().map(|axis| axis.reference_pixel)),
        ),
        (
            "refval",
            CasaValue::float64_vector(axes.iter().map(|axis| axis.reference_value)),
        ),
        ("shape", pixels(&summary.shape)),
        ("unit", CasaValue::string(summary.units.clone())),
    ];
    if let Some(beam) = &summary.restoring_beam {
        entries.push(("restoringbeam", beam_record(beam)));
    }
    sorted(entries)
}

/// `imhead`'s `list` record: the header keywords, one `crval`, `crpix`,
/// `cdelt`, `ctype` and `cunit` per axis, numbered from 1 as FITS numbers
/// them.
///
/// Not computed yet (#977): `datamax`, `datamin`, `date-obs`, `equinox`,
/// `maxpixpos`, `maxpos`, `minpixpos`, `minpos`, `observer`, `projection`,
/// `reffreqtype`, `restfreq` and `telescope`.
pub fn imhead_list_record(summary: &ImageHeaderSummary) -> CasaValue {
    let mut entries = vec![
        (
            "bunit".to_string(),
            CasaValue::string(summary.units.clone()),
        ),
        (
            "imtype".to_string(),
            CasaValue::string(summary.image_type.clone()),
        ),
        (
            "masks".to_string(),
            CasaValue::str_vector(summary.masks.clone()),
        ),
        (
            "object".to_string(),
            CasaValue::string(summary.object_name.clone()),
        ),
        ("shape".to_string(), pixels(&summary.shape)),
    ];
    for (index, axis) in summary.axes.iter().enumerate() {
        let number = index + 1;
        entries.extend([
            (format!("cdelt{number}"), CasaValue::float(axis.increment)),
            (
                format!("crpix{number}"),
                CasaValue::float(axis.reference_pixel),
            ),
            (
                format!("crval{number}"),
                CasaValue::float(axis.reference_value),
            ),
            (
                format!("ctype{number}"),
                CasaValue::string(axis.name.clone()),
            ),
            (
                format!("cunit{number}"),
                CasaValue::string(axis.unit.clone()),
            ),
        ]);
    }
    if let Some(beam) = &summary.restoring_beam {
        entries.extend([
            (
                "beammajor".to_string(),
                quantity(beam.major_arcsec, "arcsec"),
            ),
            (
                "beamminor".to_string(),
                quantity(beam.minor_arcsec, "arcsec"),
            ),
            (
                "beampa".to_string(),
                quantity(beam.position_angle_deg, "deg"),
            ),
        ]);
    }
    sorted(entries)
}

/// Pixel coordinates or a shape, as CASA's `int64` arrays.
fn pixels(values: &[usize]) -> CasaValue {
    CasaValue::int64_vector(values.iter().map(|&value| value as i64))
}

/// A quantity as CASA's records hold it: `{'unit': ..., 'value': ...}`.
fn quantity(value: f64, unit: &str) -> CasaValue {
    CasaValue::dict([
        ("unit", CasaValue::string(unit)),
        ("value", CasaValue::float(value)),
    ])
}

fn beam_record(beam: &RestoringBeamSummary) -> CasaValue {
    CasaValue::dict([
        ("major", quantity(beam.major_arcsec, "arcsec")),
        ("minor", quantity(beam.minor_arcsec, "arcsec")),
        ("positionangle", quantity(beam.position_angle_deg, "deg")),
    ])
}

/// A dictionary with its keys in alphabetical order, as CASA's records are.
fn sorted<K: Into<String>>(entries: Vec<(K, CasaValue)>) -> CasaValue {
    let mut entries = entries
        .into_iter()
        .map(|(key, value)| (key.into(), value))
        .collect::<Vec<(String, CasaValue)>>();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    CasaValue::Dict {
        entries: entries
            .into_iter()
            .map(|(key, value)| CasaEntry {
                key: CasaKey::String(key),
                value,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::ImageAxisSummary;

    fn keys(value: &CasaValue) -> Vec<String> {
        let CasaValue::Dict { entries } = value else {
            panic!("not a dict: {value:?}");
        };
        entries
            .iter()
            .map(|entry| match &entry.key {
                CasaKey::String(key) => key.clone(),
                CasaKey::Int(key) => key.to_string(),
            })
            .collect()
    }

    fn header() -> ImageHeaderSummary {
        let axis = |axis: usize, name: &str, unit: &str| ImageAxisSummary {
            axis,
            coordinate_type: "Direction".into(),
            name: name.into(),
            shape: 16,
            reference_value: 0.5,
            reference_pixel: 8.0,
            increment: -1e-5,
            unit: unit.into(),
        };
        ImageHeaderSummary {
            imagename: "ramp.im".into(),
            pixel_type: "Float32".into(),
            shape: vec![16, 16],
            units: "Jy/beam".into(),
            object_name: String::new(),
            image_type: "Intensity".into(),
            default_mask: None,
            masks: Vec::new(),
            regions: Vec::new(),
            restoring_beam: Some(RestoringBeamSummary {
                major_arcsec: 3.0,
                minor_arcsec: 2.0,
                position_angle_deg: 10.0,
            }),
            axes: vec![
                axis(0, "Right Ascension", "rad"),
                axis(1, "Declination", "rad"),
            ],
        }
    }

    #[test]
    fn imstat_returns_casas_keys_in_casas_order_with_its_dtypes() {
        let summary = ImageStatisticsSummary {
            imagename: "ramp.im".into(),
            blc: vec![0, 0, 0, 0],
            trc: vec![15, 15, 0, 7],
            npts: 2048.0,
            min: 0.0,
            max: 20.47,
            sum: 1.0,
            sumsq: 2.0,
            mean: 3.0,
            rms: 4.0,
            sigma: 5.0,
            median: 6.0,
            minpos: Some(vec![0, 0, 0, 0]),
            maxpos: Some(vec![15, 15, 0, 7]),
            flux: Some(1.5),
            units: "Jy/beam".into(),
        };
        let record = imstat_record(&summary);
        assert_eq!(
            keys(&record),
            [
                "blc", "flux", "max", "maxpos", "mean", "median", "min", "minpos", "npts", "rms",
                "sigma", "sum", "sumsq", "trc"
            ]
        );
        let plain = record.to_plain_json();
        assert_eq!(plain["npts"], serde_json::json!([2048.0]));
        assert_eq!(plain["trc"], serde_json::json!([15, 15, 0, 7]));
        let unbeamed = ImageStatisticsSummary {
            flux: None,
            ..summary
        };
        assert!(!keys(&imstat_record(&unbeamed)).contains(&"flux".to_string()));
    }

    #[test]
    fn imhead_summary_and_list_follow_casas_records() {
        let summary = imhead_summary_record(&header());
        assert_eq!(
            keys(&summary),
            [
                "axisnames",
                "axisunits",
                "defaultmask",
                "hasmask",
                "imagetype",
                "incr",
                "masks",
                "ndim",
                "refpix",
                "refval",
                "restoringbeam",
                "shape",
                "unit"
            ]
        );
        let plain = summary.to_plain_json();
        assert_eq!(
            plain["restoringbeam"]["positionangle"],
            serde_json::json!({"unit": "deg", "value": 10.0})
        );
        let list = imhead_list_record(&header());
        assert_eq!(
            keys(&list),
            [
                "beammajor",
                "beamminor",
                "beampa",
                "bunit",
                "cdelt1",
                "cdelt2",
                "crpix1",
                "crpix2",
                "crval1",
                "crval2",
                "ctype1",
                "ctype2",
                "cunit1",
                "cunit2",
                "imtype",
                "masks",
                "object",
                "shape"
            ]
        );
        assert_eq!(list.to_plain_json()["ctype2"], "Declination");
    }
}
