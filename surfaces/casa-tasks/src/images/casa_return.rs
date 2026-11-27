// SPDX-License-Identifier: LGPL-3.0-or-later
//! What CASA's image tasks return, built from casa-rs's summaries (#931).
//!
//! CASA's `imstat` returns its statistics record and `imhead` its header
//! record, dictionaries whose entries are NumPy arrays with CASA's dtypes
//! and shapes, their keys in alphabetical order. These functions build the
//! entries casa-rs computes. The keys CASA returns that casa-rs does not
//! compute yet are listed with each function and tracked in #977.

use casa_contracts::{CasaEntry, CasaFloat, CasaKey, CasaValue, FloatDtype};

use casa_imageanalysis::analysis::{
    ImageHeaderSummary, ImageStatisticsSummary, ImstatPlanesSummary, RestoringBeamSummary,
};

use casa_lattices::StatisticsAlgorithmKind;

mod imhead_get;
pub use imhead_get::{ImheadGetError, imhead_get_value};

/// `imstat`'s statistics record over the whole selection, as CASA returns
/// it with `axes=[]`: each statistic a one-element `float64` array, and the
/// corners and extremum positions `int64` pixel arrays. A selection with no
/// valid pixel has each statistic, `flux` among them, as an empty `float64`
/// array and no extremum positions, as CASA 6.7.6.14 returns it.
///
/// Not computed yet (#977): `blcf`, `trcf`, `minposf` and `maxposf` (the
/// formatted world coordinates) and the robust statistics `medabsdevmed`,
/// `q1`, `q3` and `quartile`.
pub fn imstat_record(summary: &ImageStatisticsSummary, kind: StatisticsAlgorithmKind) -> CasaValue {
    let empty = summary.npts == 0.0;
    let statistic = |value: f64| {
        if empty {
            CasaValue::float64_vector([])
        } else {
            CasaValue::float64_vector([value])
        }
    };
    if kind == StatisticsAlgorithmKind::Biweight {
        // The biweight reports only its location and scale, with the
        // sample's count and extrema; no flux, positions or order
        // statistics.
        return sorted(vec![
            ("blc", pixels(&summary.blc)),
            ("blcf", CasaValue::string(summary.blcf.clone())),
            ("max", statistic(summary.max)),
            ("mean", statistic(summary.mean)),
            ("min", statistic(summary.min)),
            ("npts", statistic(summary.npts)),
            ("sigma", statistic(summary.sigma)),
            ("trc", pixels(&summary.trc)),
            ("trcf", CasaValue::string(summary.trcf.clone())),
        ]);
    }
    let mut entries = vec![
        ("blc", pixels(&summary.blc)),
        ("blcf", CasaValue::string(summary.blcf.clone())),
        ("max", statistic(summary.max)),
        ("mean", statistic(summary.mean)),
        ("medabsdevmed", statistic(summary.medabsdevmed)),
        ("median", statistic(summary.median)),
        ("min", statistic(summary.min)),
        ("npts", statistic(summary.npts)),
        ("q1", statistic(summary.q1)),
        ("q3", statistic(summary.q3)),
        ("quartile", statistic(summary.quartile)),
        ("rms", statistic(summary.rms)),
        ("sigma", statistic(summary.sigma)),
        ("sum", statistic(summary.sum)),
        ("sumsq", statistic(summary.sumsq)),
        ("trc", pixels(&summary.trc)),
        ("trcf", CasaValue::string(summary.trcf.clone())),
    ];
    if let Some(flux) = summary.flux {
        entries.push(("flux", statistic(flux)));
    }
    if !empty {
        if let Some(position) = &summary.maxpos {
            entries.push(("maxpos", pixels(position)));
            if let Some(text) = &summary.maxposf {
                entries.push(("maxposf", CasaValue::string(text.clone())));
            }
        }
        if let Some(position) = &summary.minpos {
            entries.push(("minpos", pixels(position)));
            if let Some(text) = &summary.minposf {
                entries.push(("minposf", CasaValue::string(text.clone())));
            }
        }
    }
    sorted(entries)
}

/// `imstat`'s per-plane record: every statistic an array over the
/// display positions, with CASA's degenerate display axes dropped and
/// no extremum positions.
pub fn imstat_planes_record(summary: &ImstatPlanesSummary) -> CasaValue {
    let shape = &summary.display_shape;
    let planes = |values: &[f64]| {
        if values.is_empty() {
            CasaValue::float64_vector([])
        } else {
            CasaValue::FloatArray {
                dtype: FloatDtype::Float64,
                shape: shape.clone(),
                data: values.iter().map(|value| CasaFloat(*value)).collect(),
            }
        }
    };
    if summary.biweight {
        // The biweight reports only its location and scale, with each
        // plane's count and extrema; no flux, positions or order
        // statistics.
        return sorted(vec![
            ("blc", pixels(&summary.blc)),
            ("blcf", CasaValue::string(summary.blcf.clone())),
            ("max", planes(&summary.max)),
            ("mean", planes(&summary.mean)),
            ("min", planes(&summary.min)),
            ("npts", planes(&summary.npts)),
            ("sigma", planes(&summary.sigma)),
            ("trc", pixels(&summary.trc)),
            ("trcf", CasaValue::string(summary.trcf.clone())),
        ]);
    }
    let mut entries = vec![
        ("blc", pixels(&summary.blc)),
        ("blcf", CasaValue::string(summary.blcf.clone())),
        ("max", planes(&summary.max)),
        ("mean", planes(&summary.mean)),
        ("medabsdevmed", planes(&summary.medabsdevmed)),
        ("median", planes(&summary.median)),
        ("min", planes(&summary.min)),
        ("npts", planes(&summary.npts)),
        ("q1", planes(&summary.q1)),
        ("q3", planes(&summary.q3)),
        ("quartile", planes(&summary.quartile)),
        ("rms", planes(&summary.rms)),
        ("sigma", planes(&summary.sigma)),
        ("sum", planes(&summary.sum)),
        ("sumsq", planes(&summary.sumsq)),
        ("trc", pixels(&summary.trc)),
        ("trcf", CasaValue::string(summary.trcf.clone())),
    ];
    if let Some(flux) = &summary.flux {
        entries.push(("flux", planes(flux)));
    }
    sorted(entries)
}

/// `imhead`'s `summary` record.
///
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
        // Whether a mask applies: the default mask, not any stored one.
        (
            "hasmask",
            CasaValue::Bool {
                value: summary.has_mask || summary.default_mask.is_some(),
            },
        ),
        ("imagetype", CasaValue::string(summary.image_type.clone())),
        (
            "incr",
            CasaValue::float64_vector(axes.iter().map(|axis| axis.increment)),
        ),
        ("masks", CasaValue::str_vector(summary.masks.clone())),
        // imhead calls summary with list=True, so casacore posts the
        // coordinate messages instead of collecting them in its return.
        ("messages", CasaValue::str_vector(Vec::<String>::new())),
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
        ("tileshape", pixels(&summary.tile_shape)),
        ("unit", CasaValue::string(summary.units.clone())),
    ];
    if let Some(beam) = &summary.restoring_beam {
        entries.push(("restoringbeam", beam_record(beam)));
    }
    if let Some(beams) = &summary.per_plane_beams {
        entries.push(("perplanebeams", summary_beam_grid(beams)));
    }
    sorted(entries)
}

/// `imhead`'s `list` record: the header keywords, one `crval`, `crpix`,
/// `cdelt`, `ctype` and `cunit` per axis, numbered from 1 as FITS numbers
/// them.
///
pub fn imhead_list_record(summary: &ImageHeaderSummary) -> CasaValue {
    let mut entries = vec![
        (
            "bunit".to_string(),
            CasaValue::string(summary.units.clone()),
        ),
        (
            "date-obs".to_string(),
            CasaValue::string(&summary.observation_date),
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
        ("observer".to_string(), CasaValue::string(&summary.observer)),
        ("shape".to_string(), pixels(&summary.shape)),
        (
            "telescope".to_string(),
            CasaValue::string(&summary.telescope),
        ),
    ];
    if let Some(beams) = &summary.per_plane_beams {
        entries.push(("perplanebeams".to_string(), listed_beam_grid(beams)));
    }
    if let Some(frame) = &summary.direction_reference {
        entries.push(("equinox".to_string(), CasaValue::string(frame)));
    }
    if let Some(projection) = &summary.direction_projection {
        entries.push(("projection".to_string(), CasaValue::string(projection)));
    }
    if let Some(spectral) = &summary.spectral {
        entries.push((
            "reffreqtype".to_string(),
            CasaValue::string(&spectral.reference_frame),
        ));
        entries.push((
            "restfreq".to_string(),
            CasaValue::float64_vector(spectral.rest_frequencies.iter().copied()),
        ));
    }
    if let Some(extrema) = &summary.extrema {
        entries.extend([
            ("datamin".to_string(), CasaValue::float(extrema.min)),
            ("datamax".to_string(), CasaValue::float(extrema.max)),
            ("minpixpos".to_string(), pixels(&extrema.min_position)),
            ("maxpixpos".to_string(), pixels(&extrema.max_position)),
            ("minpos".to_string(), CasaValue::string(&extrema.min_world)),
            ("maxpos".to_string(), CasaValue::string(&extrema.max_world)),
        ]);
    }
    for (index, axis) in summary.world_axes.iter().enumerate() {
        let number = index + 1;
        entries.extend([
            (format!("cdelt{number}"), CasaValue::float(axis.increment)),
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
    for (index, axis) in summary.axes.iter().enumerate() {
        entries.push((
            format!("crpix{}", index + 1),
            CasaValue::float(axis.reference_pixel),
        ));
    }
    if let Some(beam) = &summary.restoring_beam {
        entries.extend([
            (
                "beammajor".to_string(),
                quantity(beam.major.value, &beam.major.unit),
            ),
            (
                "beamminor".to_string(),
                quantity(beam.minor.value, &beam.minor.unit),
            ),
            (
                "beampa".to_string(),
                quantity(
                    beam.wrapped_position_angle.value,
                    &beam.wrapped_position_angle.unit,
                ),
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
        ("major", quantity(beam.major.value, &beam.major.unit)),
        ("minor", quantity(beam.minor.value, &beam.minor.unit)),
        (
            "positionangle",
            quantity(beam.position_angle.value, &beam.position_angle.unit),
        ),
    ])
}

fn summary_beam_grid(grid: &casa_imageanalysis::analysis::PerPlaneBeamSummary) -> CasaValue {
    let channels = (0..grid.channels)
        .map(|channel| {
            let stokes = (0..grid.stokes)
                .map(|polarization| {
                    (
                        format!("*{polarization}"),
                        beam_record(&grid.beams[polarization * grid.channels + channel]),
                    )
                })
                .collect();
            (format!("*{channel}"), sorted(stokes))
        })
        .collect();
    CasaValue::dict([
        ("beams", sorted(channels)),
        (
            "nChannels",
            CasaValue::Int {
                value: grid.channels as i64,
            },
        ),
        (
            "nStokes",
            CasaValue::Int {
                value: grid.stokes as i64,
            },
        ),
    ])
}

fn listed_beam_grid(grid: &casa_imageanalysis::analysis::PerPlaneBeamSummary) -> CasaValue {
    let mut entries: Vec<(String, CasaValue)> = grid
        .beams
        .iter()
        .enumerate()
        .map(|(index, beam)| (format!("*{index}"), beam_record(beam)))
        .collect();
    entries.push((
        "median area beam".into(),
        beam_record(&grid.beams[grid.median_index]),
    ));
    entries.push((
        "nChannels".into(),
        CasaValue::Int {
            value: grid.channels as i64,
        },
    ));
    entries.push((
        "nStokes".into(),
        CasaValue::Int {
            value: grid.stokes as i64,
        },
    ));
    sorted(entries)
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
    use casa_imageanalysis::analysis::{BeamQuantity, ImageAxisSummary};

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
            stokes: Vec::new(),
        };
        let axes = vec![
            axis(0, "Right Ascension", "rad"),
            axis(1, "Declination", "rad"),
        ];
        ImageHeaderSummary {
            imagename: "ramp.im".into(),
            pixel_type: "Float32".into(),
            shape: vec![16, 16],
            tile_shape: vec![8, 8],
            units: "Jy/beam".into(),
            object_name: String::new(),
            observer: String::new(),
            telescope: String::new(),
            observation_date: "1858/11/17/00:00:00.000000".into(),
            direction_reference: None,
            direction_projection: None,
            spectral: None,
            image_type: "Intensity".into(),
            default_mask: None,
            has_mask: false,
            masks: Vec::new(),
            regions: Vec::new(),
            restoring_beam: Some(Box::new(RestoringBeamSummary {
                major_arcsec: 3.0,
                minor_arcsec: 2.0,
                position_angle_deg: 10.0,
                major: BeamQuantity {
                    value: 3.0,
                    unit: "arcsec".into(),
                },
                minor: BeamQuantity {
                    value: 2.0,
                    unit: "arcsec".into(),
                },
                position_angle: BeamQuantity {
                    value: 10.0,
                    unit: "deg".into(),
                },
                wrapped_position_angle: BeamQuantity {
                    value: 10.0,
                    unit: "deg".into(),
                },
            })),
            has_multiple_beams: false,
            per_plane_beams: None,
            extrema: None,
            world_axes: axes.clone(),
            axes,
        }
    }

    fn classic_summary() -> ImageStatisticsSummary {
        ImageStatisticsSummary {
            imagename: "ramp.im".into(),
            blc: vec![0, 0, 0, 0],
            trc: vec![15, 15, 0, 7],
            blcf: "00:00:16.000, -00.04.00.000, I, 1.415e+09Hz".into(),
            trcf: "23:59:48.000, +00.03.00.000, I, 1.415e+09Hz".into(),
            npts: 2048.0,
            min: 0.0,
            max: 20.47,
            sum: 1.0,
            sumsq: 2.0,
            mean: 3.0,
            rms: 4.0,
            sigma: 5.0,
            median: 6.0,
            medabsdevmed: 7.0,
            q1: 8.0,
            q3: 9.0,
            quartile: 1.0,
            minpos: Some(vec![0, 0, 0, 0]),
            minposf: Some("00:00:16.000, -00.04.00.000, I, 1.415e+09Hz".into()),
            maxpos: Some(vec![15, 15, 0, 7]),
            maxposf: Some("23:59:48.000, +00.03.00.000, I, 1.415e+09Hz".into()),
            flux: Some(1.5),
            units: "Jy/beam".into(),
        }
    }

    #[test]
    fn imstat_returns_casas_keys_in_casas_order_with_its_dtypes() {
        let summary = classic_summary();
        let record = imstat_record(&summary, StatisticsAlgorithmKind::Classic);
        assert_eq!(
            keys(&record),
            [
                "blc",
                "blcf",
                "flux",
                "max",
                "maxpos",
                "maxposf",
                "mean",
                "medabsdevmed",
                "median",
                "min",
                "minpos",
                "minposf",
                "npts",
                "q1",
                "q3",
                "quartile",
                "rms",
                "sigma",
                "sum",
                "sumsq",
                "trc",
                "trcf"
            ]
        );
        let plain = record.to_plain_json();
        assert_eq!(plain["npts"], serde_json::json!([2048.0]));
        assert_eq!(plain["trc"], serde_json::json!([15, 15, 0, 7]));
        assert_eq!(
            plain["blcf"],
            serde_json::json!("00:00:16.000, -00.04.00.000, I, 1.415e+09Hz")
        );
        let unbeamed = ImageStatisticsSummary {
            flux: None,
            ..summary
        };
        assert!(
            !keys(&imstat_record(&unbeamed, StatisticsAlgorithmKind::Classic))
                .contains(&"flux".to_string())
        );
    }

    #[test]
    fn imstat_returns_the_biweights_reduced_record() {
        let summary = ImageStatisticsSummary {
            npts: 2048.0,
            min: 0.0,
            max: 20.47,
            mean: 10.235,
            sigma: 6.3021,
            ..classic_summary()
        };
        let record = imstat_record(&summary, StatisticsAlgorithmKind::Biweight);
        assert_eq!(
            keys(&record),
            [
                "blc", "blcf", "max", "mean", "min", "npts", "sigma", "trc", "trcf"
            ]
        );
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
                "messages",
                "ndim",
                "refpix",
                "refval",
                "restoringbeam",
                "shape",
                "tileshape",
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
                "date-obs",
                "imtype",
                "masks",
                "object",
                "observer",
                "shape",
                "telescope"
            ]
        );
        assert_eq!(list.to_plain_json()["ctype2"], "Declination");
    }
}
