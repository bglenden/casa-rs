// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA 6.7.6.14's interpretation of the public ALMA ppdisk tutorial input.
use casa_coordinates::{Coordinate, CoordinateModel};
use casa_core::measures::frequency::FrequencyRef;
use casa_images::{PagedImage, analysis::import_fits, read_fits_image};
use std::path::Path;

#[test]
fn tutorial_fits_import_keeps_pixels_and_casacore_units_and_frequency() {
    let input = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/ppdisk672_GHz_50pc.fits");
    let fits = read_fits_image(&input).unwrap();
    // Independently recorded by casatools.image on the original public FITS,
    // not obtained by round-tripping casa-rs's FITS writer.
    assert_eq!(fits.shape, [257, 257, 1, 1]);
    assert_eq!(fits.units, "Jy/pixel");
    let CoordinateModel::Spectral(spectral) = fits.coordinates.coordinate(2) else {
        panic!("the fourth FITS axis is spectral");
    };
    assert_eq!(spectral.frequency_ref(), FrequencyRef::LSRK);
    assert!((spectral.reference_value()[0] - 672_000_001_103.0).abs() < 0.1);
    assert!((spectral.increment()[0] - 32_000_000_029.8).abs() < 0.1);
    assert!((spectral.rest_frequency() - 1_420_405_758.37).abs() < 0.01);
    let scratch = tempfile::tempdir().unwrap();
    let path = scratch.path().join("model.image");
    let imported = import_fits(input, &path, false).unwrap();
    assert_eq!(imported.units, "Jy/pixel");
    let image = PagedImage::<f32>::open(path).unwrap();
    assert_eq!(image.units(), "Jy/pixel");
    assert_eq!(image.get().unwrap(), fits.pixels);
}
