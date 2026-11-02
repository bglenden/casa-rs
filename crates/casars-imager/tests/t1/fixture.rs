// SPDX-License-Identifier: LGPL-3.0-or-later

//! Shared T1 fixture: an analytic sky observed by a synthetic VLA-A track,
//! imaged through `casars-imager`'s production request route, and read back
//! from the persisted CASA products and MeasurementSet for analytic checks.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use casa_coordinates::{CoordinateModel, ProjectionType, StokesType};
use casa_images::{GaussianBeam, PagedImage};
use casa_imaging_application::store_law::compare_stores;
use casa_imaging_application::{Cancel, HostResources, ImagingRequest, RunContext};
use casa_ms::{
    MeasurementSet, SyntheticAnalyticComponent, SyntheticAnalyticSpectrum, SyntheticAntenna,
    SyntheticCorruptionConfig, SyntheticField, SyntheticNoiseCorruption, SyntheticNoiseMode,
    SyntheticObservationRequest, SyntheticSkyModel, SyntheticSpectralSetup, VisibilityDataColumn,
    generate_synthetic_observation_ms, tutorial_vla_a_antennas,
};
use casa_types::{ArrayValue, Complex32};
use casars_imager::{ImagerArtifact, ImagerRunReport, ImagingParameters, resolve_request, run};
use ndarray::{Array2, Axis};
use serde_json::{Value, json};

/// Image side in pixels.
pub const IMAGE_SIZE: usize = 256;
/// Cell size; the natural-weighting beam minor axis (0.047 arcsec at this
/// southern declination) spans about four cells.
pub const CELL_ARCSEC: f64 = 0.012;
/// Per-component (real and imaginary) visibility noise injected by the simulator.
pub const VISIBILITY_NOISE_JY: f32 = 0.65;
/// Native channels of the default MFS band.
pub const MFS_CHANNELS: usize = 4;
/// Frequency at which component fluxes are stated.
pub const REFERENCE_FREQUENCY_HZ: f64 = 45.0e9;

const START_FREQUENCY_HZ: f64 = 44.0e9;
const CHANNEL_WIDTH_HZ: f64 = 128.0e6;
const DURATION_SECONDS: f64 = 3_600.0;
const INTEGRATION_SECONDS: f64 = 24.0;
/// Parallel-hand correlations combined into Stokes I.
const STOKES_I_CORRELATIONS: usize = 2;
/// J2000 phase centre of every T1 track: 18h at declination −23°, the
/// `vla_ppdisk` tutorial field.
pub const PHASE_CENTER_RAD: [f64; 2] = [4.712_391_234_768_306, -0.401_423_788_703_971_4];

/// The image geometry a run images and a component's pixel offsets refer to.
#[derive(Debug, Clone, Copy)]
pub struct Geometry {
    /// Pixels along x and y.
    pub image_size: [usize; 2],
    /// Cell along x and y in arcseconds.
    pub cell_arcsec: [f64; 2],
}

impl Geometry {
    /// The default T1 geometry: [`IMAGE_SIZE`] cells of [`CELL_ARCSEC`].
    pub const DEFAULT: Self = Self::square(IMAGE_SIZE, CELL_ARCSEC);

    /// `image_size` cells of `cell_arcsec` on both axes.
    pub const fn square(image_size: usize, cell_arcsec: f64) -> Self {
        Self {
            image_size: [image_size; 2],
            cell_arcsec: [cell_arcsec; 2],
        }
    }

    /// Cell along x and y in radians.
    pub fn cell_rad(self) -> [f64; 2] {
        self.cell_arcsec.map(|cell| cell.to_radians() / 3_600.0)
    }

    /// The image reference pixel: half the image on each axis.
    pub fn centre(self) -> [usize; 2] {
        self.image_size.map(|side| side / 2)
    }

    /// The imager's `imsize` and `cell` parameters for this geometry.
    pub fn parameters(self) -> [(&'static str, Value); 2] {
        [
            ("imsize", json!(self.image_size)),
            (
                "cell",
                json!(self.cell_arcsec.map(|cell| format!("{cell}arcsec"))),
            ),
        ]
    }

    /// Pixel `[x, y]` of a component offset from the image reference pixel.
    pub fn pixel(self, component: Component) -> [usize; 2] {
        let centre = self.centre();
        [0, 1].map(|axis| {
            usize::try_from(centre[axis] as i32 + component.offset_px[axis])
                .expect("component inside image")
        })
    }
}

/// The instrument, pointings and band of a synthetic track.
pub struct Setup {
    /// `OBSERVATION.TELESCOPE_NAME`; selects the simulator's primary beam.
    pub telescope: String,
    /// The array.
    pub antennas: Vec<SyntheticAntenna>,
    /// Pointings `[ra, dec]` in radians observed round-robin per
    /// integration; empty for one field at [`PHASE_CENTER_RAD`].
    pub fields: Vec<[f64; 2]>,
    /// The spectral windows, observed simultaneously. The observation's
    /// channel, sample and noise helpers describe the first window, so
    /// they hold for one window only.
    pub spectral_windows: Vec<SyntheticSpectralSetup>,
    /// The geometry component offsets refer to and runs image.
    pub geometry: Geometry,
    /// Per-component (real and imaginary) visibility noise in Jy.
    pub noise_jy: f32,
}

impl Setup {
    /// The VLA-A Q-band track: `channels` 128 MHz channels from 44 GHz on
    /// the default geometry.
    pub fn vla_q_band(channels: usize) -> Self {
        Self {
            telescope: "VLA".to_string(),
            antennas: tutorial_vla_a_antennas(),
            fields: Vec::new(),
            spectral_windows: vec![SyntheticSpectralSetup {
                name: "t1-qband".to_string(),
                start_frequency_hz: START_FREQUENCY_HZ,
                channel_width_hz: CHANNEL_WIDTH_HZ,
                channel_count: channels,
            }],
            geometry: Geometry::DEFAULT,
            noise_jy: VISIBILITY_NOISE_JY,
        }
    }

    /// `[ra, dec]` of a pointing `[Δl, Δm]` radians east and north of
    /// [`PHASE_CENTER_RAD`].
    pub fn pointing(offset_rad: [f64; 2]) -> [f64; 2] {
        [
            PHASE_CENTER_RAD[0] + offset_rad[0] / PHASE_CENTER_RAD[1].cos(),
            PHASE_CENTER_RAD[1] + offset_rad[1],
        ]
    }
}

/// One analytic component centred on an image pixel.
#[derive(Debug, Clone, Copy)]
pub struct Component {
    /// Pixel offset `[x, y]` from the image reference pixel; `+x` is west.
    pub offset_px: [i32; 2],
    /// Flux density in Jy at [`REFERENCE_FREQUENCY_HZ`].
    pub flux_jy: f64,
    /// Circular FWHM in pixels; `None` for a point source.
    pub fwhm_px: Option<f64>,
    /// Spectral index `α` of `S(ν) = S₀ (ν/ν₀)^α`.
    pub spectral_index: f64,
}

impl Component {
    /// Flux density in Jy at `frequency_hz`.
    pub fn flux_at(self, frequency_hz: f64) -> f64 {
        self.flux_jy * (frequency_hz / REFERENCE_FREQUENCY_HZ).powf(self.spectral_index)
    }

    /// Direction cosines `[l, m]` on `geometry`: `l` grows east, opposite
    /// to image `x`.
    pub fn direction_cosines(self, geometry: Geometry) -> [f64; 2] {
        let [cell_x, cell_y] = geometry.cell_rad();
        [
            -f64::from(self.offset_px[0]) * cell_x,
            f64::from(self.offset_px[1]) * cell_y,
        ]
    }

    fn analytic(self, name: &str, geometry: Geometry) -> SyntheticAnalyticComponent {
        let [l_rad, m_rad] = self.direction_cosines(geometry);
        let spectrum = SyntheticAnalyticSpectrum {
            flux_jy: self.flux_jy,
            spectral_index: self.spectral_index,
            reference_frequency_hz: Some(REFERENCE_FREQUENCY_HZ),
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
                // A circular component, sized in x cells.
                let fwhm_rad = fwhm_px * geometry.cell_rad()[0];
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
    unflagged_rows: usize,
    geometry: Geometry,
    noise_jy: f32,
}

impl Observation {
    /// Observe `components` with the VLA-A layout in one Q-band window of
    /// [`MFS_CHANNELS`] channels.
    pub fn synthesise(components: &[(&str, Component)]) -> Self {
        Self::synthesise_band(MFS_CHANNELS, components)
    }

    /// Observe `components` in one Q-band window of `channels` 128 MHz
    /// channels from 44 GHz.
    pub fn synthesise_band(channels: usize, components: &[(&str, Component)]) -> Self {
        Self::synthesise_setup(Setup::vla_q_band(channels), components)
    }

    /// Observe `components` with `setup`.
    pub fn synthesise_setup(setup: Setup, components: &[(&str, Component)]) -> Self {
        let root = tempfile::tempdir().expect("T1 fixture directory");
        let measurement_set = root.path().join("t1.ms");
        let mut request = SyntheticObservationRequest::vla_ppdisk(
            root.path().join("unused.fits"),
            &measurement_set,
            setup.antennas,
        );
        request.telescope_name = setup.telescope;
        request.field_name = "t1".to_string();
        request.phase_center_rad = PHASE_CENTER_RAD;
        request.fields = setup
            .fields
            .iter()
            .enumerate()
            .map(|(index, phase_center_rad)| SyntheticField {
                name: format!("t1-{index}"),
                phase_center_rad: *phase_center_rad,
            })
            .collect();
        request.duration_seconds = DURATION_SECONDS;
        request.integration_seconds = INTEGRATION_SECONDS;
        request.spectral_windows = setup.spectral_windows;
        request.model = Some(SyntheticSkyModel::AnalyticComponents {
            path: None,
            schema_version: Some(1),
            name: Some("t1-sky".to_string()),
            components: components
                .iter()
                .map(|(name, component)| component.analytic(name, setup.geometry))
                .collect(),
        });
        request.corruption = Some(SyntheticCorruptionConfig {
            seed: 20_261_007,
            noise: Some(SyntheticNoiseCorruption {
                mode: SyntheticNoiseMode::SimpleNoise,
                simplenoise_jy: setup.noise_jy,
            }),
            gain: None,
            bandpass: None,
            leakage: None,
            pointing: None,
        });
        let report = generate_synthetic_observation_ms(&request).expect("synthesise T1 MS");
        Self {
            phase_center_rad: request.phase_center_rad,
            channel_frequencies_hz: request.spectral_windows[0].channel_frequencies_hz(),
            unflagged_rows: report.main_row_count - report.flagged_row_count,
            measurement_set,
            root,
            geometry: setup.geometry,
            noise_jy: setup.noise_jy,
        }
    }

    /// The geometry the track was synthesised for and runs image.
    pub fn geometry(&self) -> Geometry {
        self.geometry
    }

    /// A path for a run's private inputs inside the observation's directory.
    pub fn scratch(&self, name: &str) -> PathBuf {
        self.root.path().join(name)
    }

    /// The synthesised MeasurementSet.
    pub fn measurement_set(&self) -> &Path {
        &self.measurement_set
    }

    /// Native channel centres in Hz.
    pub fn channel_frequencies_hz(&self) -> &[f64] {
        &self.channel_frequencies_hz
    }

    /// Unflagged row-channel samples.
    pub fn row_channel_samples(&self) -> usize {
        self.unflagged_rows * self.channel_frequencies_hz.len()
    }

    /// Unflagged parallel-hand samples that enter Stokes I.
    pub fn stokes_i_samples(&self) -> usize {
        STOKES_I_CORRELATIONS * self.row_channel_samples()
    }

    /// Unflagged parallel-hand samples of one native channel.
    pub fn stokes_i_samples_per_channel(&self) -> usize {
        STOKES_I_CORRELATIONS * self.unflagged_rows
    }

    /// Natural-weighting Stokes I image noise for unit-weight visibilities:
    /// `sigma / sqrt(N)` over the Stokes I samples.
    pub fn image_noise_jy(&self) -> f64 {
        f64::from(self.noise_jy) / (self.stokes_i_samples() as f64).sqrt()
    }

    /// Natural-weighting image noise of one native channel.
    pub fn channel_noise_jy(&self) -> f64 {
        f64::from(self.noise_jy) / (self.stokes_i_samples_per_channel() as f64).sqrt()
    }

    /// Run the production route with `controls`, tclean parameters by
    /// catalog name, merged over the T1 geometry and read back every
    /// product it reports.
    pub fn image(&self, name: &str, controls: Value) -> (ImagerRunReport, Products) {
        self.try_image(name, controls)
            .expect("production imaging route")
    }

    /// [`Observation::image`], or the route's refusal.
    pub fn try_image(
        &self,
        name: &str,
        controls: Value,
    ) -> Result<(ImagerRunReport, Products), String> {
        self.try_image_store(&self.measurement_set, name, controls)
    }

    /// [`Observation::try_image`] of `store`, another store of this
    /// observation (an MSv4 conversion).
    pub fn try_image_store(
        &self,
        store: &Path,
        name: &str,
        controls: Value,
    ) -> Result<(ImagerRunReport, Products), String> {
        let image_name = self.root.path().join(name);
        // The CPU unless `controls` name a backend: the catalog's `auto`
        // would move a law onto the GPU wherever the host has one.
        let mut parameters = json!({
            "vis": store,
            "imagename": image_name,
            "backend": "cpu",
        });
        let fields = parameters.as_object_mut().expect("parameters object");
        for (key, value) in self.geometry.parameters() {
            fields.insert(key.to_string(), value);
        }
        for (key, value) in controls.as_object().expect("controls object") {
            fields.insert(key.clone(), value.clone());
        }
        imager_defaults(fields);
        let Value::Object(parameters) = parameters else {
            unreachable!("parameters are an object");
        };
        let (parameters, request) = resolve_request(&ImagingParameters(parameters))?;
        let result = run(parameters, &request)?;
        let products = Products::read(&image_name, &result.artifacts);
        Ok((result.run, products))
    }

    /// The store law (#932, #942): under `controls`' geometry the batch
    /// geometry of every sample equals its batch geometry in `converted`,
    /// another store of the same observation (an MSv4 conversion), to the
    /// law's 1e-12.
    pub fn assert_store_geometry(&self, name: &str, controls: Value, converted: &Path) {
        let request = self.request(name, controls);
        let comparison = compare_stores(&request, &run_context(&request), converted)
            .expect("both stores' geometry");
        eprintln!("{name}: store geometry law {comparison:?}");
        assert_eq!(comparison.unmatched, 0, "{name}: {comparison:?}");
        assert!(
            comparison.holds(),
            "{name}: the converted store's geometry departs: {comparison:?}"
        );
    }

    /// The request of `controls` merged over the T1 geometry.
    fn request(&self, name: &str, controls: Value) -> ImagingRequest {
        let mut parameters = json!({
            "vis": self.measurement_set,
            "imagename": self.root.path().join(name),
            "backend": "cpu",
        });
        let fields = parameters.as_object_mut().expect("parameters object");
        for (key, value) in self.geometry.parameters() {
            fields.insert(key.to_string(), value);
        }
        for (key, value) in controls.as_object().expect("controls object") {
            fields.insert(key.clone(), value.clone());
        }
        imager_defaults(fields);
        let Value::Object(parameters) = parameters else {
            unreachable!("parameters are an object");
        };
        resolve_request(&ImagingParameters(parameters))
            .expect("request")
            .1
    }

    /// `(DATA, MODEL_DATA)` of every unflagged parallel-hand sample.
    pub fn data_and_model(&self) -> Vec<(Complex32, Complex32)> {
        let ms = MeasurementSet::open(&self.measurement_set).expect("reopen T1 MS");
        let data = ms
            .data_column(VisibilityDataColumn::Data)
            .expect("DATA column");
        let model = ms
            .data_column(VisibilityDataColumn::ModelData)
            .expect("MODEL_DATA column");
        let flags = ms.flag_column();
        let mut samples = Vec::new();
        for row in 0..ms.row_count() {
            let (
                ArrayValue::Complex32(data),
                ArrayValue::Complex32(model),
                ArrayValue::Bool(flags),
            ) = (
                data.get(row).expect("DATA row"),
                model.get(row).expect("MODEL_DATA row"),
                flags.get(row).expect("FLAG row"),
            )
            else {
                panic!("row {row}: DATA and MODEL_DATA are complex, FLAG boolean");
            };
            samples.extend(
                data.iter()
                    .zip(model.iter())
                    .zip(flags.iter())
                    .filter(|(_, flagged)| !**flagged)
                    .map(|((data, model), _)| (*data, *model)),
            );
        }
        samples
    }

    /// Image-plane checks that depend on the observation geometry; an MFS
    /// image's spectral reference sits at the band centre.
    pub fn assert_image_wcs(&self, image: &Product) {
        self.assert_direction_and_stokes(image);
        let CoordinateModel::Spectral(_) = image.image.coordinates().coordinate(2) else {
            panic!(".image coordinate 2 is not a spectral coordinate");
        };
        let band_centre_hz = 0.5
            * (self.channel_frequencies_hz[0]
                + self.channel_frequencies_hz[self.channel_frequencies_hz.len() - 1]);
        let frequency_hz = image.channel_frequency_hz(0);
        assert!(
            (frequency_hz - band_centre_hz).abs() < 1.0,
            "MFS reference frequency {frequency_hz} Hz, band centre {band_centre_hz} Hz"
        );
        let [nx, ny] = self.geometry.image_size;
        assert_eq!(image.pixels.dim(), (nx, ny));
    }

    /// Direction and Stokes axes: SIN projection at the phase centre with
    /// the requested cell and a Stokes I axis.
    pub fn assert_direction_and_stokes(&self, image: &Product) {
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
        let [cell_x, cell_y] = self.geometry.cell_rad();
        let increment = coordinates.coordinate(0).increment();
        assert!(
            (increment[0] + cell_x).abs() < 1.0e-6 * cell_x,
            "{increment:?}"
        );
        assert!(
            (increment[1] - cell_y).abs() < 1.0e-6 * cell_y,
            "{increment:?}"
        );
        assert_eq!(
            coordinates.coordinate(0).reference_pixel(),
            self.geometry.centre().map(|pixel| pixel as f64).to_vec()
        );
        let CoordinateModel::Stokes(stokes) = coordinates.coordinate(1) else {
            panic!(".image coordinate 1 is not a Stokes coordinate");
        };
        assert_eq!(stokes.stokes(), [StokesType::I]);
    }
}

/// The imager's defaults these laws were written against, where CASA's
/// tclean defaults differ (#931): 1000-iteration minor cycles, W planes
/// derived from the data, and the imager's fixed `conjbeams` and
/// `mosweight`.
fn imager_defaults(fields: &mut serde_json::Map<String, Value>) {
    // These component laws predate default PB production and include moving
    // fields and frequencies outside the analytic PB's implemented scope.
    // The production default and explicit opt-out have their own pb_default T1.
    fields.entry("write_pb").or_insert_with(|| json!(false));
    fields.entry("cycleniter").or_insert_with(|| json!(1000));
    match fields.get("gridder").and_then(Value::as_str) {
        Some("wproject") => {
            fields.entry("wprojplanes").or_insert_with(|| json!(-1));
        }
        Some("awproject") => {
            fields.entry("conjbeams").or_insert_with(|| json!(true));
        }
        Some("mosaic") => {
            fields.entry("mosweight").or_insert_with(|| json!(false));
        }
        _ => {}
    }
}

/// A run context for `request` on this host.
fn run_context(request: &ImagingRequest) -> RunContext {
    RunContext {
        host: HostResources::detect().expect("host resources"),
        policy: request.resource_policy(),
        cancel: Cancel::default(),
        summary: None,
    }
}

fn angle_difference_rad(actual: f64, expected: f64) -> f64 {
    (actual - expected + std::f64::consts::PI).rem_euclid(std::f64::consts::TAU)
        - std::f64::consts::PI
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

    /// Spectral channels on the image's frequency axis.
    pub fn channels(&self) -> usize {
        self.image.shape()[3]
    }

    /// The `[x, y]` plane of `channel` (Stokes I).
    pub fn plane(&self, channel: usize) -> Array2<f32> {
        let shape = self.image.shape();
        self.image
            .get_slice(&[0, 0, 0, channel], &[shape[0], shape[1], 1, 1])
            .expect("read product plane")
            .index_axis_move(Axis(3), 0)
            .index_axis_move(Axis(2), 0)
            .into_dimensionality()
            .expect("two-dimensional plane")
    }

    /// Every value, in the image's axis order.
    pub fn values(&self) -> ndarray::ArrayD<f32> {
        self.image.get().expect("read product")
    }

    /// The default pixel mask, when the product stores one.
    pub fn mask(&self) -> Option<ndarray::ArrayD<bool>> {
        self.image.default_mask_name()?;
        let shape = self.image.shape().to_vec();
        self.image
            .get_mask_slice(&vec![0; shape.len()], &shape, &vec![1; shape.len()])
            .expect("read product mask")
    }

    /// The value of a single-pixel-per-channel product (`.sumwt`) per channel.
    pub fn channel_values(&self) -> Vec<f32> {
        let values = self.image.get().expect("read product");
        values.iter().copied().collect()
    }

    /// World frequency of `channel` in Hz.
    pub fn channel_frequency_hz(&self, channel: usize) -> f64 {
        let spectral = self.image.coordinates().coordinate(2);
        spectral.reference_value()[0]
            + (channel as f64 - spectral.reference_pixel()[0]) * spectral.increment()[0]
    }

    /// Signed channel width in Hz.
    pub fn channel_width_hz(&self) -> f64 {
        self.image.coordinates().coordinate(2).increment()[0]
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

/// Every product a run reported, keyed by its CASA suffix.
pub struct Products {
    opened: BTreeMap<String, Product>,
}

impl Products {
    fn read(image_name: &Path, reported: &[ImagerArtifact]) -> Self {
        let prefix = image_name.display().to_string();
        Self {
            opened: reported
                .iter()
                .map(|artifact| {
                    let suffix = artifact
                        .path
                        .strip_prefix(&prefix)
                        .unwrap_or_else(|| panic!("{} outside {prefix}", artifact.path));
                    (suffix.to_string(), Product::open(Path::new(&artifact.path)))
                })
                .collect(),
        }
    }

    /// The reported suffixes.
    pub fn suffixes(&self) -> BTreeSet<String> {
        self.opened.keys().cloned().collect()
    }

    /// The product with `suffix`.
    pub fn get(&self, suffix: &str) -> &Product {
        self.opened
            .get(suffix)
            .unwrap_or_else(|| panic!("run did not report {suffix}"))
    }
}

/// The spectral axis every product of a run shares.
#[derive(Debug, Clone, Copy)]
pub enum SpectralAxis {
    /// One plane at the band centre: MFS and MT-MFS.
    BandCentre,
    /// The native channels, within half a channel: a cube.
    NativeChannels,
}

/// The product contract (plan section 5.7) a T1 run is held to, beyond its
/// science: what it publishes, their coordinates, and the normalization rows
/// the fixture can check from the observation alone.
pub struct ProductContract<'a> {
    /// The exact product suffixes.
    pub inventory: &'a [&'a str],
    /// The spectral axis the products share.
    pub spectral: SpectralAxis,
    /// Whether the run weights naturally, so `.sumwt` counts the unit-weight
    /// Stokes I samples.
    pub natural: bool,
    /// `|pblimit|`: the PB is zero, and masked, at and below it.
    pub pblimit: f32,
    /// Whether `.image` and `.residual` carry the PB's mask (pblimit not
    /// negative).
    pub uncorrected_masks: bool,
}

impl Observation {
    /// Hold `products` to `contract`:
    /// - the exact inventory;
    /// - every member's direction, Stokes and spectral coordinates;
    /// - a unit-peak PSF in every plane with weight;
    /// - `.sumwt` as the natural-weighting sample count;
    /// - a PB in [0, 1], zero and masked at or below pblimit, the root of
    ///   `.weight` over its peak where a weight image is published;
    /// - `.image.pbcor` as `.image` over the PB inside its support;
    /// - the PB's mask on `.image` and `.residual` exactly when pblimit is
    ///   not negative.
    pub fn assert_product_contract(&self, products: &Products, contract: &ProductContract<'_>) {
        assert_eq!(
            products.suffixes(),
            contract
                .inventory
                .iter()
                .map(|suffix| suffix.to_string())
                .collect::<BTreeSet<_>>(),
            "inventory"
        );
        for (suffix, product) in &products.opened {
            self.assert_direction_and_stokes(product);
            self.assert_spectral_axis(suffix, product, contract.spectral);
        }
        for (suffix, product) in &products.opened {
            let term0 = !suffix.ends_with(".tt1") && !suffix.ends_with(".tt2");
            if suffix.starts_with(".psf") && term0 {
                for channel in 0..product.channels() {
                    let peak = product
                        .plane(channel)
                        .fold(f32::MIN, |peak, value| peak.max(*value));
                    assert!(
                        peak == 0.0 || (peak - 1.0).abs() < 1.0e-6,
                        "{suffix} channel {channel} peak {peak}"
                    );
                }
            }
            if suffix.starts_with(".sumwt") {
                // CASA tools read `useweightimage` from `.sumwt` to learn
                // whether the residual was divided by a weight image.
                let weight_image = contract
                    .inventory
                    .iter()
                    .any(|name| name.starts_with(".weight"));
                let recorded = product.image.misc_info().get("useweightimage").cloned();
                assert_eq!(
                    recorded,
                    Some(casa_types::Value::Scalar(casa_types::ScalarValue::Bool(
                        weight_image
                    ))),
                    "{suffix} useweightimage"
                );
                if term0
                    && contract.natural
                    && matches!(contract.spectral, SpectralAxis::BandCentre)
                {
                    assert_eq!(
                        f64::from(product.channel_values()[0]),
                        self.stokes_i_samples() as f64,
                        "{suffix} counts the Stokes I samples"
                    );
                }
            }
        }
        let Some(pb) = [".pb", ".pb.tt0"]
            .into_iter()
            .find_map(|suffix| products.opened.get(suffix))
        else {
            return;
        };
        let support = assert_primary_beam(pb, contract.pblimit);
        if let Some(weight) = [".weight", ".weight.tt0"]
            .into_iter()
            .find_map(|suffix| products.opened.get(suffix))
        {
            assert_root_of_weight(pb, weight, &support);
        }
        for (image, corrected) in [
            (".image", ".image.pbcor"),
            (".image.tt0", ".image.tt0.pbcor"),
        ] {
            if let Some(corrected) = products.opened.get(corrected) {
                assert_pb_corrected(products.get(image), pb, corrected, &support);
            }
        }
        for (suffix, product) in &products.opened {
            if suffix.starts_with(".image") && !suffix.ends_with(".pbcor")
                || suffix.starts_with(".residual")
            {
                match (contract.uncorrected_masks, product.mask()) {
                    (true, Some(mask)) => {
                        assert_eq!(mask, support, "{suffix} carries the PB's mask")
                    }
                    (false, None) => {}
                    (expected, mask) => {
                        panic!(
                            "{suffix}: PB mask expected {expected}, found {}",
                            mask.is_some()
                        )
                    }
                }
            }
        }
    }

    fn assert_spectral_axis(&self, suffix: &str, product: &Product, axis: SpectralAxis) {
        let native = &self.channel_frequencies_hz;
        match axis {
            SpectralAxis::BandCentre => {
                let band_centre_hz = 0.5 * (native[0] + native[native.len() - 1]);
                let frequency_hz = product.channel_frequency_hz(0);
                assert!(
                    (frequency_hz - band_centre_hz).abs() < 1.0,
                    "{suffix}: reference frequency {frequency_hz} Hz, band centre \
                     {band_centre_hz} Hz"
                );
            }
            SpectralAxis::NativeChannels => {
                let width_hz = native[1] - native[0];
                assert_eq!(product.channels(), native.len(), "{suffix} channels");
                for (channel, native_hz) in native.iter().enumerate() {
                    assert!(
                        (product.channel_frequency_hz(channel) - native_hz).abs() < 0.5 * width_hz,
                        "{suffix} channel {channel}"
                    );
                }
            }
        }
    }
}

/// The PB is in [0, 1] with a unit peak, zero at or below `pblimit`, and
/// masked exactly there; returns its support.
fn assert_primary_beam(pb: &Product, pblimit: f32) -> ndarray::ArrayD<bool> {
    let values = pb.values();
    let peak = values.iter().fold(0.0_f32, |peak, value| peak.max(*value));
    assert!((peak - 1.0).abs() < 1.0e-3, "PB peak {peak}");
    assert!(
        values
            .iter()
            .all(|value| (0.0..=1.0 + 1.0e-6).contains(value)),
        "the PB lies in [0, 1]"
    );
    let support = values.mapv(|value| value > pblimit);
    assert!(
        values
            .iter()
            .zip(&support)
            .all(|(value, inside)| *inside || *value == 0.0),
        "the PB is zero at or below pblimit {pblimit}"
    );
    assert_eq!(pb.mask().expect("the PB carries its mask"), support);
    support
}

/// `.pb` is `sqrt(weight / max(weight))` inside its support, each channel
/// over its own plane's peak (`SIImageStore::getPbMax(pol, chan)`).
fn assert_root_of_weight(pb: &Product, weight: &Product, support: &ndarray::ArrayD<bool>) {
    let (pb, weight) = (pb.values(), weight.values());
    for channel in 0..weight.shape()[3] {
        let plane = |values: &ndarray::ArrayD<f32>| values.index_axis(Axis(3), channel).to_owned();
        let weight = plane(&weight);
        let peak = weight.iter().fold(0.0_f32, |peak, value| peak.max(*value));
        let inside = support.index_axis(Axis(3), channel);
        for ((pb, weight), inside) in plane(&pb).iter().zip(&weight).zip(inside) {
            if *inside {
                let expected = (weight.abs() / peak).sqrt();
                assert!(
                    (pb - expected).abs() < 1.0e-5,
                    "channel {channel}: PB {pb}, root of the weight over its peak {expected}"
                );
            }
        }
    }
}

/// `.image.pbcor` is `.image` over the PB inside the PB's support, and zero
/// and masked outside it.
fn assert_pb_corrected(
    image: &Product,
    pb: &Product,
    corrected: &Product,
    support: &ndarray::ArrayD<bool>,
) {
    let (image, beam, corrected_values) = (image.values(), pb.values(), corrected.values());
    for (((image, beam), corrected), inside) in
        image.iter().zip(&beam).zip(&corrected_values).zip(support)
    {
        if *inside {
            let expected = image / beam;
            assert!(
                (corrected - expected).abs() <= 1.0e-5 * expected.abs().max(1.0e-3),
                "pbcor {corrected}, image over PB {expected}"
            );
        } else {
            assert_eq!(*corrected, 0.0, "pbcor outside the PB support");
        }
    }
    assert_eq!(
        corrected.mask().expect("pbcor carries the PB's mask"),
        *support
    );
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
/// Returns FWHM `[major, minor]` in radians on the default geometry.
pub fn fit_psf_main_lobe(psf: &Array2<f32>, cutoff: f32) -> [f64; 2] {
    fit_psf_main_lobe_cells(psf, cutoff).map(|fwhm| fwhm * Geometry::DEFAULT.cell_rad()[0])
}

/// [`fit_psf_main_lobe`] in cells.
pub fn fit_psf_main_lobe_cells(psf: &Array2<f32>, cutoff: f32) -> [f64; 2] {
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
    let fwhm = |precision: f64| 2.0 * (2.0 * 2.0_f64.ln() / precision).sqrt();
    [fwhm(mean - spread), fwhm(mean + spread)]
}

/// RMS of `pixels` inside the central half of the image, excluding discs of
/// `radius_px` around `exclusions`.
pub fn off_source_rms(pixels: &Array2<f32>, exclusions: &[[usize; 2]], radius_px: f64) -> f64 {
    let (nx, ny) = pixels.dim();
    let (sum, count) = pixels
        .indexed_iter()
        .filter(|((x, y), _)| {
            (nx / 4..nx - nx / 4).contains(x)
                && (ny / 4..ny - ny / 4).contains(y)
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
