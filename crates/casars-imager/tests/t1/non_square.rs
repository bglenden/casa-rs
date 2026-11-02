// SPDX-License-Identifier: LGPL-3.0-or-later

//! Non-square images (#841 Phase 6b): `imsize` and `cell` of two values,
//! x then y, as CASA's `tclean` takes them. The model planes, the grids, the
//! masks and the products each carry x and y; a transposition between any
//! two of them would move a source or the mask here, where on a square
//! image it could not be seen.

use serde_json::{Value, json};

use super::fixture::{
    Component, Geometry, Observation, ProductContract, Products, Setup, SpectralAxis, box_sum, peak,
};

/// 192 × 128 cells of 0.012″ × 0.009″; the natural beam's minor axis spans
/// about four x cells and five y cells.
pub(super) const GEOMETRY: Geometry = Geometry {
    image_size: [192, 128],
    cell_arcsec: [0.012, 0.009],
};
const EAST: Component = Component {
    offset_px: [40, -30],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};
const WEST: Component = Component {
    offset_px: [-60, 25],
    flux_jy: 0.6,
    fwhm_px: None,
    spectral_index: 0.0,
};

/// The two-source track on [`GEOMETRY`] and the controls that clean it.
pub(super) fn non_square_run() -> (Observation, Value) {
    let mut setup = Setup::vla_q_band(4);
    setup.geometry = GEOMETRY;
    let observation = Observation::synthesise_setup(setup, &[("east", EAST), ("west", WEST)]);
    let controls = json!({
        "weighting": "natural",
        "deconvolver": "hogbom",
        "niter": 500,
        "threshold": "0.005Jy",
    });
    (observation, controls)
}

#[test]
fn a_non_square_image_keeps_its_sources_psf_and_mask_on_their_own_axes() {
    let (observation, controls) = non_square_run();
    let (_, products) = observation.image("non-square", controls.clone());
    observation.assert_product_contract(
        &products,
        &ProductContract {
            inventory: &[".image", ".mask", ".model", ".psf", ".residual", ".sumwt"],
            spectral: SpectralAxis::BandCentre,
            natural: true,
            pblimit: 0.2,
            uncorrected_masks: false,
        },
    );
    let image = products.get(".image");
    observation.assert_image_wcs(image);
    assert_eq!(image.pixels.dim(), (192, 128));
    let (psf_peak, psf_pixel, _) = peak(&products.get(".psf").pixels);
    assert_eq!(psf_pixel, GEOMETRY.centre());
    assert!((psf_peak - 1.0).abs() < 1.0e-6, "PSF peak {psf_peak}");
    for component in [EAST, WEST] {
        assert_cleaned(&products, component);
    }

    // A box about the east source alone: the model stays inside it, and
    // the west source is left in the residual.
    let [x, y] = GEOMETRY.pixel(EAST);
    let mut masked = controls;
    masked["mask"] = json!(format!("{},{},{},{}", x - 6, y - 4, x + 6, y + 4));
    let (_, products) = observation.image("non-square-box", masked);
    let mask = &products.get(".mask").pixels;
    for ((mx, my), value) in mask.indexed_iter() {
        let inside = mx.abs_diff(x) <= 6 && my.abs_diff(y) <= 4;
        assert_eq!(*value, f32::from(inside), "mask pixel [{mx}, {my}]");
    }
    let model = &products.get(".model").pixels;
    for ((mx, my), value) in model.indexed_iter() {
        assert!(
            *value == 0.0 || mask[[mx, my]] == 1.0,
            "model {value} Jy outside the mask at [{mx}, {my}]"
        );
    }
    assert_cleaned(&products, EAST);
    let west = GEOMETRY.pixel(WEST);
    let residual = products.get(".residual").pixels[west];
    assert!(
        f64::from(residual) > 0.5 * WEST.flux_jy,
        "the masked-out west source left {residual} Jy/beam"
    );
}

/// `component` is cleaned at its own pixel: the model holds its flux there
/// and the restored image peaks there.
fn assert_cleaned(products: &Products, component: Component) {
    let pixel = GEOMETRY.pixel(component);
    let model_jy = box_sum(&products.get(".model").pixels, pixel, 2);
    assert!(
        (model_jy - component.flux_jy).abs() < 0.02 * component.flux_jy,
        "{component:?}: model {model_jy} Jy at {pixel:?}"
    );
    let image = &products.get(".image").pixels;
    let neighbourhood = |[cx, cy]: [usize; 2]| {
        (cx - 3..=cx + 3).flat_map(move |x| (cy - 3..=cy + 3).map(move |y| [x, y]))
    };
    let brightest = neighbourhood(pixel)
        .max_by(|a, b| image[*a].total_cmp(&image[*b]))
        .expect("a neighbourhood");
    assert_eq!(brightest, pixel, "{component:?}: restored peak");
}
