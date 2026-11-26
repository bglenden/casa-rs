// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::image::ImageHistoryEntry;
use casa_coordinates::{
    DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate, StokesCoordinate,
    StokesType,
};
use casa_core::measures::direction::DirectionRef;
use casa_core::measures::frequency::FrequencyRef;

#[test]
fn export_axis_plan_places_spectral_before_stokes_when_stokeslast() {
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.0, 0.5],
        [-1.0e-6, 1.0e-6],
        [4.0, 5.0],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        372.0e9,
        1.0e6,
        0.0,
        372.0e9,
    ));

    let plan = ExportAxisPlan::build(
        &coordinates,
        &[10, 11, 1, 7],
        false,
        false,
        true,
        false,
        false,
    )
    .unwrap();
    assert_eq!(plan.kept_orig_axes, vec![0, 1, 3, 2]);
    assert_eq!(plan.output_shape, vec![10, 11, 7, 1]);
    let base_header = try_to_fits_header(&coordinates, &[10, 11, 1, 7]).unwrap();
    let keywords = build_export_keywords(&coordinates, &base_header, &plan, false, false, None)
        .unwrap()
        .into_iter()
        .collect::<HashMap<_, _>>();
    assert_eq!(
        keywords.get("CTYPE3"),
        Some(&FitsValue::String("FREQ".to_string()))
    );
    assert_eq!(
        keywords.get("CTYPE4"),
        Some(&FitsValue::String("STOKES".to_string()))
    );
}

#[test]
fn wrap_history_utf8_bytes_and_write_fits_logical_key() {
    let accented = "a".repeat(71) + "éb";
    let chunks = wrap_fits_history_text(&accented);
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0], "a".repeat(71));
    assert_eq!(chunks[1], ">éb");
    assert!(chunks.iter().all(|c| c.len() <= 72));

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("logical.fits");
    let desc = ImageDescription {
        data_type: FitsImageType::Float,
        dimensions: &[2, 2],
    };
    {
        let mut fits = FitsFile::create(&path)
            .with_custom_primary(&desc)
            .open()
            .unwrap();
        let hdu = fits.primary_hdu().unwrap();
        write_fits_key(&hdu, &mut fits, "FLAGT", &FitsValue::Logical(true)).unwrap();
        write_fits_key(&hdu, &mut fits, "FLAGF", &FitsValue::Logical(false)).unwrap();
        write_history_cards(
            &mut fits,
            &[
                ImageHistoryEntry {
                    time_mjd_seconds: -1.0,
                    priority: String::new(),
                    location: String::new(),
                    message: "line1\nline2".to_string(),
                },
                ImageHistoryEntry {
                    time_mjd_seconds: 5_000_000_000.0,
                    priority: "WARN".to_string(),
                    location: "multi".to_string(),
                    message: accented,
                },
            ],
        )
        .unwrap();
        hdu.write_image(&mut fits, &[0.0_f32; 4]).unwrap();
    }
    let raw = fs::read(&path).unwrap();
    let text = String::from_utf8_lossy(&raw);
    assert!(text.contains("FLAGT   =                    T"));
    assert!(text.contains("FLAGF   =                    F"));
    let entries = crate::image::image_history_entries(&path).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].message, "line1");
    assert_eq!(entries[1].message, "line2");
    assert_eq!(entries[2].location, "::multi");
    // CFITSIO `ffphis` scrubs non-ASCII FITS card bytes (`é` -> `"  "`) while keeping pairing intact.
    assert_eq!(entries[2].message, format!("{}  b", "a".repeat(71)));
}
