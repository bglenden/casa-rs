// SPDX-License-Identifier: LGPL-3.0-or-later

//! Shared T1 fixture: an analytic sky observed by a synthetic VLA-A track,
//! imaged through `casars-imager`'s production request route, and read back
//! from the persisted CASA products for analytic checks.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Once;

use casa_coordinates::{CoordinateModel, ProjectionType, StokesType};
use casa_images::{GaussianBeam, PagedImage};
use casa_ms::{
    SyntheticAnalyticComponent, SyntheticAnalyticSpectrum, SyntheticCorruptionConfig,
    SyntheticNoiseCorruption, SyntheticNoiseMode, SyntheticObservationRequest, SyntheticSkyModel,
    SyntheticSpectralSetup, generate_synthetic_observation_ms,
    initialize_measurement_set_owner_manifest, tutorial_vla_a_antennas,
};
use casars_imager::{ImagerRunTaskRequest, RunSummary, run_from_request};
use ndarray::{Array2, Axis};
use serde_json::{Value, json};

/// Image side in pixels.
pub const IMAGE_SIZE: usize = 256;
/// Cell size; the natural-weighting beam minor axis (0.047 arcsec at this
/// southern declination) spans about four cells.
pub const CELL_ARCSEC: f64 = 0.012;
/// Per-component (real and imaginary) visibility noise injected by the simulator.
pub const VISIBILITY_NOISE_JY: f32 = 0.65;

const CHANNEL_COUNT: usize = 4;
const START_FREQUENCY_HZ: f64 = 44.0e9;
const CHANNEL_WIDTH_HZ: f64 = 128.0e6;
const DURATION_SECONDS: f64 = 3_600.0;
const INTEGRATION_SECONDS: f64 = 24.0;
/// Parallel-hand correlations combined into Stokes I.
const STOKES_I_CORRELATIONS: usize = 2;

/// One analytic component centred on an image pixel.
#[derive(Debug, Clone, Copy)]
pub struct Component {
    /// Pixel offset `[x, y]` from the image reference pixel; `+x` is west.
    pub offset_px: [i32; 2],
    /// Flux density in Jy, flat across the band.
    pub flux_jy: f64,
    /// Circular FWHM in pixels; `None` for a point source.
    pub fwhm_px: Option<f64>,
}

impl Component {
    /// Direction cosines `[l, m]`: `l` grows east, opposite to image `x`.
    fn direction_cosines(self) -> [f64; 2] {
        let cell_rad = CELL_ARCSEC.to_radians() / 3_600.0;
        [
            -f64::from(self.offset_px[0]) * cell_rad,
            f64::from(self.offset_px[1]) * cell_rad,
        ]
    }

    fn analytic(self, name: &str) -> SyntheticAnalyticComponent {
        let [l_rad, m_rad] = self.direction_cosines();
        let spectrum = SyntheticAnalyticSpectrum {
            flux_jy: self.flux_jy,
            spectral_index: 0.0,
            reference_frequency_hz: None,
            line_peak_jy: 0.0,
            line_center_fraction: 0.5,
            line_sigma_fraction: 0.1,
            absorption_peak_jy: 0.0,
            absorption_center_fraction: 0.5,
            absorption_sigma_fraction: 0.1,
        };
        let name = Some(name.to_string());
        match self.fwhm_px {
            None => SyntheticAnalyticComponent::Point {
                name,
                l_rad,
                m_rad,
                spectrum,
            },
            Some(fwhm_px) => {
                let fwhm_rad = fwhm_px * CELL_ARCSEC.to_radians() / 3_600.0;
                SyntheticAnalyticComponent::Gaussian {
                    name,
                    l_rad,
                    m_rad,
                    major_fwhm_rad: fwhm_rad,
                    minor_fwhm_rad: fwhm_rad,
                    position_angle_rad: 0.0,
                    spectrum,
                }
            }
        }
    }
}

/// A synthetic observation on disk, owned by its temporary directory.
pub struct Observation {
    root: tempfile::TempDir,
    measurement_set: PathBuf,
    phase_center_rad: [f64; 2],
    channel_frequencies_hz: Vec<f64>,
    unflagged_row_channels: usize,
}

impl Observation {
    /// Observe `components` with the VLA-A layout in one Q-band window.
    pub fn synthesise(components: &[(&str, Component)]) -> Self {
        let root = tempfile::tempdir().expect("T1 fixture directory");
        let measurement_set = root.path().join("t1.ms");
        let mut request = SyntheticObservationRequest::vla_ppdisk(
            root.path().join("unused.fits"),
            &measurement_set,
            tutorial_vla_a_antennas(),
        );
        request.field_name = "t1".to_string();
        request.duration_seconds = DURATION_SECONDS;
        request.integration_seconds = INTEGRATION_SECONDS;
        request.spectral_windows = vec![SyntheticSpectralSetup {
            name: "t1-qband".to_string(),
            start_frequency_hz: START_FREQUENCY_HZ,
            channel_width_hz: CHANNEL_WIDTH_HZ,
            channel_count: CHANNEL_COUNT,
        }];
        request.model = Some(SyntheticSkyModel::AnalyticComponents {
            path: None,
            schema_version: Some(1),
            name: Some("t1-sky".to_string()),
            components: components
                .iter()
                .map(|(name, component)| component.analytic(name))
                .collect(),
        });
        request.corruption = Some(SyntheticCorruptionConfig {
            seed: 20_261_007,
            noise: Some(SyntheticNoiseCorruption {
                mode: SyntheticNoiseMode::SimpleNoise,
                simplenoise_jy: VISIBILITY_NOISE_JY,
            }),
            gain: None,
            bandpass: None,
            leakage: None,
            pointing: None,
        });
        let report = generate_synthetic_observation_ms(&request).expect("synthesise T1 MS");
        initialize_measurement_set_owner_manifest(&measurement_set)
            .expect("initialise T1 MS owner manifest");
        Self {
            phase_center_rad: request.phase_center_rad,
            channel_frequencies_hz: request.spectral_windows[0].channel_frequencies_hz(),
            unflagged_row_channels: (report.main_row_count - report.flagged_row_count)
                * report.channel_count,
            measurement_set,
            root,
        }
    }

    /// Unflagged row-channel samples.
    pub fn row_channel_samples(&self) -> usize {
        self.unflagged_row_channels
    }

    /// Unflagged parallel-hand samples that enter Stokes I.
    pub fn stokes_i_samples(&self) -> usize {
        STOKES_I_CORRELATIONS * self.unflagged_row_channels
    }

    /// Natural-weighting Stokes I image noise for unit-weight visibilities:
    /// `sigma / sqrt(N)` over the Stokes I samples.
    pub fn image_noise_jy(&self) -> f64 {
        f64::from(VISIBILITY_NOISE_JY) / (self.stokes_i_samples() as f64).sqrt()
    }

    /// Run the production route with `controls` merged over the T1 geometry
    /// and read back every product it reports.
    pub fn image(&self, name: &str, controls: Value) -> (RunSummary, Products) {
        set_production_io_environment();
        let image_name = self.root.path().join(name);
        let mut request = json!({
            "measurement_set": self.measurement_set,
            "image_name": image_name,
            "image_size": IMAGE_SIZE,
            "cell_arcsec": CELL_ARCSEC,
            "write_preview_pngs": false,
        });
        let fields = request.as_object_mut().expect("request object");
        for (key, value) in controls.as_object().expect("controls object") {
            fields.insert(key.clone(), value.clone());
        }
        let request: ImagerRunTaskRequest =
            serde_json::from_value(request).expect("typed T1 imager request");
        let summary = run_from_request(&request).expect("production imaging route");
        let products = Products::read(&image_name, &summary.output_products);
        (summary, products)
    }

    /// Image-plane checks that depend on the observation geometry.
    pub fn assert_image_wcs(&self, image: &Product) {
        let coordinates = image.image.coordinates();
        let CoordinateModel::Direction(direction) = coordinates.coordinate(0) else {
            panic!(".image coordinate 0 is not a direction coordinate");
        };
        assert_eq!(
            direction.projection().projection_type(),
            ProjectionType::SIN
        );
        let reference = coordinates.coordinate(0).reference_value();
        for (axis, expected) in self.phase_center_rad.iter().enumerate() {
            assert!(
                angle_difference_rad(reference[axis], *expected).abs() < 1.0e-9,
                "direction reference axis {axis}: {} rad, expected {expected} rad",
                reference[axis]
            );
        }
        let cell_rad = CELL_ARCSEC.to_radians() / 3_600.0;
        let increment = coordinates.coordinate(0).increment();
        assert!(
            (increment[0] + cell_rad).abs() < 1.0e-6 * cell_rad,
            "{increment:?}"
        );
        assert!(
            (increment[1] - cell_rad).abs() < 1.0e-6 * cell_rad,
            "{increment:?}"
        );
        assert_eq!(
            coordinates.coordinate(0).reference_pixel(),
            vec![(IMAGE_SIZE / 2) as f64; 2]
        );
        let CoordinateModel::Stokes(stokes) = coordinates.coordinate(1) else {
            panic!(".image coordinate 1 is not a Stokes coordinate");
        };
        assert_eq!(stokes.stokes(), [StokesType::I]);
        let CoordinateModel::Spectral(_) = coordinates.coordinate(2) else {
            panic!(".image coordinate 2 is not a spectral coordinate");
        };
        let band_centre_hz = 0.5
            * (self.channel_frequencies_hz[0]
                + self.channel_frequencies_hz[self.channel_frequencies_hz.len() - 1]);
        let frequency_hz = coordinates.coordinate(2).reference_value()[0];
        assert!(
            (frequency_hz - band_centre_hz).abs() < 1.0,
            "MFS reference frequency {frequency_hz} Hz, band centre {band_centre_hz} Hz"
        );
        assert_eq!(image.pixels.dim(), (IMAGE_SIZE, IMAGE_SIZE));
    }
}

fn angle_difference_rad(actual: f64, expected: f64) -> f64 {
    (actual - expected + std::f64::consts::PI).rem_euclid(std::f64::consts::TAU)
        - std::f64::consts::PI
}

fn set_production_io_environment() {
    static ONCE: Once = Once::new();
    // The application requires measured spill rates and refuses to guess;
    // these are test calibration facts, not measurements.
    ONCE.call_once(|| unsafe {
        std::env::set_var("CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND", "1000000000");
        std::env::set_var("CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND", "1000000000");
    });
}

/// One persisted product: the opened image and its first plane `[x, y]`.
pub struct Product {
    pub image: PagedImage<f32>,
    pub pixels: Array2<f32>,
}

impl Product {
    fn open(path: &Path) -> Self {
        let image = PagedImage::<f32>::open(path)
            .unwrap_or_else(|error| panic!("open {}: {error}", path.display()));
        let mut pixels = image
            .get()
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        while pixels.ndim() > 2 {
            let last = Axis(pixels.ndim() - 1);
            pixels = pixels.index_axis_move(last, 0);
        }
        let pixels = pixels.into_dimensionality().expect("two-dimensional plane");
        Self { image, pixels }
    }

    /// The restoring beam recorded in the product's image info.
    pub fn restoring_beam(&self) -> GaussianBeam {
        self.image
            .image_info()
            .expect("image info")
            .beam_set
            .single_beam()
            .expect("single restoring beam")
    }
}

/// The products every deconvolving run writes, after opening every product
/// the run reported (conventional CASA suffixes).
pub struct Products {
    pub suffixes: BTreeSet<String>,
    pub image: Product,
    pub residual: Product,
    pub psf: Product,
    pub model: Product,
    pub sumwt: Product,
}

impl Products {
    fn read(image_name: &Path, reported: &[String]) -> Self {
        let mut opened = reported
            .iter()
            .map(|suffix| {
                let path = PathBuf::from(format!("{}{suffix}", image_name.display()));
                (suffix.clone(), Product::open(&path))
            })
            .collect::<BTreeMap<_, _>>();
        let suffixes = opened.keys().cloned().collect();
        let mut take = |suffix: &str| {
            opened
                .remove(suffix)
                .unwrap_or_else(|| panic!("run did not report {suffix}"))
        };
        Self {
            image: take(".image"),
            residual: take(".residual"),
            psf: take(".psf"),
            model: take(".model"),
            sumwt: take(".sumwt"),
            suffixes,
        }
    }
}

/// Pixel `[x, y]` of a component offset from the image reference pixel.
pub fn component_pixel(component: Component) -> [usize; 2] {
    let centre = (IMAGE_SIZE / 2) as i32;
    [
        usize::try_from(centre + component.offset_px[0]).expect("component inside image"),
        usize::try_from(centre + component.offset_px[1]).expect("component inside image"),
    ]
}

/// The brightest pixel and its parabolic sub-pixel offset `[dx, dy]`.
pub fn peak(pixels: &Array2<f32>) -> (f32, [usize; 2], [f64; 2]) {
    let ((x, y), value) = pixels
        .indexed_iter()
        .max_by(|left, right| left.1.total_cmp(right.1))
        .map(|(index, value)| (index, *value))
        .expect("non-empty plane");
    let vertex = |below: f32, at: f32, above: f32| {
        let curvature = f64::from(below) - 2.0 * f64::from(at) + f64::from(above);
        0.5 * f64::from(below - above) / curvature
    };
    let dx = vertex(pixels[[x - 1, y]], value, pixels[[x + 1, y]]);
    let dy = vertex(pixels[[x, y - 1]], value, pixels[[x, y + 1]]);
    (value, [x, y], [dx, dy])
}

/// Sum of `pixels` over the square of half-width `half_width` around `centre`.
pub fn box_sum(pixels: &Array2<f32>, centre: [usize; 2], half_width: usize) -> f64 {
    pixels
        .slice(ndarray::s![
            centre[0] - half_width..=centre[0] + half_width,
            centre[1] - half_width..=centre[1] + half_width
        ])
        .iter()
        .map(|value| f64::from(*value))
        .sum()
}

/// Independent PSF main-lobe Gaussian: a weighted least-squares fit of a
/// quadratic to `ln(psf)` over the connected lobe above `cutoff` of the peak.
/// Returns FWHM `[major, minor]` in radians.
pub fn fit_psf_main_lobe(psf: &Array2<f32>, cutoff: f32) -> [f64; 2] {
    let (peak_value, [x0, y0], _) = peak(psf);
    let mut lobe = vec![[x0, y0]];
    let mut seen = BTreeSet::from([[x0, y0]]);
    let mut next = 0;
    while next < lobe.len() {
        let [x, y] = lobe[next];
        next += 1;
        for [nx, ny] in [[x + 1, y], [x - 1, y], [x, y + 1], [x, y - 1]] {
            if psf[[nx, ny]] >= cutoff * peak_value && seen.insert([nx, ny]) {
                lobe.push([nx, ny]);
            }
        }
    }
    let rows = lobe
        .iter()
        .map(|&[x, y]| {
            let dx = x as f64 - x0 as f64;
            let dy = y as f64 - y0 as f64;
            let value = f64::from(psf[[x, y]] / peak_value);
            (
                vec![1.0, dx, dy, dx * dx, dx * dy, dy * dy],
                value.ln(),
                value * value,
            )
        })
        .collect::<Vec<_>>();
    let c = casa_numerics::solve_weighted_least_squares(&rows, 6).expect("PSF lobe fit");
    // ln(psf) = ... - 0.5 r^T S^-1 r, so S^-1 = -2 [[c3, c4/2], [c4/2, c5]].
    let (a, b, d) = (-2.0 * c[3], -c[4], -2.0 * c[5]);
    let mean = 0.5 * (a + d);
    let spread = (0.25 * (a - d) * (a - d) + b * b).sqrt();
    let cell_rad = CELL_ARCSEC.to_radians() / 3_600.0;
    let fwhm = |precision: f64| 2.0 * (2.0 * 2.0_f64.ln() / precision).sqrt() * cell_rad;
    [fwhm(mean - spread), fwhm(mean + spread)]
}

/// RMS of `pixels` inside the central half of the image, excluding discs of
/// `radius_px` around `exclusions`.
pub fn off_source_rms(pixels: &Array2<f32>, exclusions: &[[usize; 2]], radius_px: f64) -> f64 {
    let quarter = IMAGE_SIZE / 4;
    let (sum, count) = pixels
        .indexed_iter()
        .filter(|((x, y), _)| {
            (quarter..IMAGE_SIZE - quarter).contains(x)
                && (quarter..IMAGE_SIZE - quarter).contains(y)
                && exclusions.iter().all(|[cx, cy]| {
                    let dx = *x as f64 - *cx as f64;
                    let dy = *y as f64 - *cy as f64;
                    dx.hypot(dy) > radius_px
                })
        })
        .fold((0.0, 0usize), |(sum, count), (_, value)| {
            (sum + f64::from(*value).powi(2), count + 1)
        });
    (sum / count as f64).sqrt()
}
