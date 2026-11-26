// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare image models with the existing gridder for CASA's legacy imager.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, DirectionCoordinate, ObsInfo,
    SpectralCoordinate, StokesCoordinate, StokesType,
};
use casa_core::measures::frequency::FrequencyRef;
use casa_core::measures::record::direction_from_record;
use casa_core::{ArrayValue, Complex32, Correlation, RecordValue, ScalarValue, Value};
use casa_images::PagedImage;
use casa_imaging_model::PolarizationCoordinate;
use casa_imaging_operator::{
    Basis, Boxcar, ConvolutionFunctionSet, GridGeometry, GridPadding, GridPolarization,
    GridPrecision, ImageCorrection, ImageExtent, MOSAIC_OVERSAMPLING, MeasurementOperator,
    ModelImages, ModelPlane, ModelPrescale, MosaicPb, PaddingFactor, PolarizationRouting,
    PreparedModelGrids, SpectralAxis, SpectralKernel, SpectralResampler, Spheroidal, WPlaneCount,
    WPlanes, grid_planning_bytes,
};
use casa_imaging_runtime::pass::{WORKER_STACK_BYTES, WorkerTeam};
use casa_imaging_runtime::{Demand, HostResources, Reservation, ResourcePolicy, admit};
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::schema::ft_record::{
    FirstBuffer, GridFt, casa_spectral, legacy_common_mosaic, legacy_image_mosaic, legacy_mosaic,
    legacy_multi_term, set_grid_motion, set_grid_tangent,
};
use casa_ms::{
    MeasurementSet, MsResult, ResolvedMsSelection, VirtualModelEntry, VirtualModelKind,
    convert_frequency_to_frame_with_frame, virtual_model_image_path,
};
use ndarray::{Array2, ArrayD, IxDyn, ShapeBuilder};

use super::moving_source::MovingSourceAnchor;
use super::settings::{ImagerGridder, PredictionGeometry};
use super::voltage_pattern::{legacy_mosaic_table, relocate_mosaic_table};
use super::{channel_span, invalid, model_entry, spectral_with_runtime};

/// An uncommitted Fourier image. The scratch path uses it until the write
/// finishes. The virtual model can keep it once its on-disk record is filed.
pub struct ImageModel {
    pub entry: VirtualModelEntry,
    pub(super) nonzero: bool,
    backing: GridLifetime,
}

impl ImageModel {
    pub(super) fn prepare(
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
        keep_zero: bool,
        scratch: bool,
        write: impl FnOnce(&Path) -> MsResult<(RecordValue, bool)>,
    ) -> MsResult<Option<Self>> {
        let mut path = virtual_model_image_path(ms)?;
        if scratch {
            let name = path.file_name().expect("the model image has a name");
            path.set_file_name(format!("scratch-{}", name.to_string_lossy()));
        }
        let backing = GridLifetime {
            path,
            retained: false,
        };
        let (container, nonzero) = write(&backing.path)?;
        if !nonzero && !keep_zero {
            return Ok(None);
        }
        let entry = model_entry(ms, selected, VirtualModelKind::FtMachine, container)?;
        Ok(Some(Self {
            entry,
            nonzero,
            backing,
        }))
    }

    pub(super) fn track(&mut self, tracking: &MovingSourceAnchor) -> MsResult<()> {
        let record = &mut self.entry.container;
        let Some(Value::Record(image)) = record.get("mimage_rec") else {
            return Err(invalid("the Fourier grid has no image direction"));
        };
        let reference = direction_from_record(image).map_err(invalid)?.refer();
        let anchor = tracking.source.direction(&tracking.frame, reference)?;
        let location = tracking
            .frame
            .position()
            .expect("moving-source frame has a location");
        let set_motion = |record: &mut RecordValue| {
            set_grid_motion(record, tracking.source.model(), &anchor, location);
        };
        if let Some(Value::Record(grid)) = record.get("subftm") {
            let mut grid = grid.clone();
            set_motion(&mut grid);
            record.upsert("subftm", Value::Record(grid));
        }
        set_motion(record);
        Ok(())
    }

    pub fn copy_to(&self, target: &MeasurementSet) -> MsResult<Self> {
        let backing = GridLifetime {
            path: virtual_model_image_path(target)?,
            retained: false,
        };
        crate::paths::copy_dir_recursive(&self.backing.path, &backing.path).map_err(invalid)?;
        let copied_lock = backing.path.join("table.lock");
        if copied_lock.is_file() {
            std::fs::remove_file(copied_lock).map_err(invalid)?;
        }
        let mut entry = self.entry.clone();
        let path = Value::Scalar(ScalarValue::String(
            backing.path.to_string_lossy().into_owned(),
        ));
        if let Some(Value::Record(grid)) = entry.container.get("subftm") {
            let mut grid = grid.clone();
            grid.upsert("diskimage", path);
            entry.container.upsert("subftm", Value::Record(grid));
        } else {
            entry.container.upsert("diskimage", path);
            relocate_mosaic_table(&mut entry.container, &self.backing.path, &backing.path)?;
        }
        Ok(Self {
            entry,
            nonzero: self.nonzero,
            backing,
        })
    }

    pub fn retain(&mut self) {
        self.backing.retained = true;
    }
}

struct GridLifetime {
    path: PathBuf,
    retained: bool,
}

impl Drop for GridLifetime {
    fn drop(&mut self) {
        if !self.retained && self.path.is_dir() {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

#[derive(Clone, Copy)]
pub struct TaylorTerm {
    pub count: i32,
    pub index: i32,
    pub reference_hz: f64,
}

/// The image spectral coordinate determines the default Taylor frequency.
pub fn reference_frequency(path: &Path) -> MsResult<f64> {
    if path.is_file() {
        let fits = casa_images::fits_read::FitsToolImport::open(path, 0, -1).map_err(invalid)?;
        return spectral_reference_frequency(fits.coordinates());
    }
    let image = PagedImage::<f32>::open(path).map_err(invalid)?;
    image_reference_frequency(&image)
}

pub(super) fn image_reference_frequency(image: &PagedImage<f32>) -> MsResult<f64> {
    spectral_reference_frequency(image.coordinates())
}

fn spectral_reference_frequency(coordinates: &CoordinateSystem) -> MsResult<f64> {
    let index = coordinate_index(coordinates, CoordinateType::Spectral)?;
    let CoordinateModel::Spectral(spectral) = coordinates.coordinate(index) else {
        unreachable!("a spectral coordinate index names a spectral coordinate")
    };
    let pixel = spectral.reference_pixel()[0];
    spectral.world_frequency_hz(pixel).map_err(invalid)
}

pub fn prepare_image(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    path: &Path,
    taylor: Option<TaylorTerm>,
    keep_zero: bool,
    scratch: bool,
) -> MsResult<Option<ImageModel>> {
    let image = PagedImage::<f32>::open(path).map_err(invalid)?;
    prepare_configured_images(
        ms,
        selected,
        &image,
        taylor,
        keep_zero,
        scratch,
        &mut PredictionGeometry::default(),
    )
    .map(|images| images.into_iter().next())
}

pub(super) fn prepare_configured_images(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    image: &PagedImage<f32>,
    taylor: Option<TaylorTerm>,
    keep_zero: bool,
    scratch: bool,
    options: &mut PredictionGeometry,
) -> MsResult<Vec<ImageModel>> {
    let [nx, ny, _, _] = *image.shape() else {
        return Err(invalid(
            "ft requires four direction, Stokes and spectral image axes",
        ));
    };
    let facets = options.facets.max(1);
    let shape = [nx / facets, ny / facets];
    if shape.contains(&0) {
        return Err(invalid(
            "the facet count exceeds the model image width or height",
        ));
    }
    let mut outputs = Vec::new();
    for y in 0..facets {
        for x in 0..facets {
            let window = ModelWindow {
                origin: [x * shape[0], y * shape[1]],
                shape,
            };
            let layout =
                ImageTransform::new(ms, selected, image, scratch, options.clone(), window)?;
            outputs.extend(write_model_grid(
                ms, selected, image, taylor, keep_zero, scratch, layout,
            )?);
            if facets > 1 && matches!(options.settings.gridder, ImagerGridder::Mosaic) {
                // CubeSkyEquation assigns the first image to MosaicFT and
                // the remaining facets and outliers to independent GridFTs.
                options.use_mosaic_outlier_grid();
            }
        }
    }
    Ok(outputs)
}

fn write_model_grid(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    image: &PagedImage<f32>,
    taylor: Option<TaylorTerm>,
    keep_zero: bool,
    scratch: bool,
    layout: ImageTransform,
) -> MsResult<Option<ImageModel>> {
    ImageModel::prepare(ms, selected, keep_zero, scratch, |path| {
        let nonzero = layout.grid(image, path)?;
        let record = layout.record(ms, selected, path, scratch)?;
        let record = match taylor {
            Some(term) => legacy_multi_term(record, term.count, term.index, term.reference_hz),
            None => record,
        };
        Ok((record, nonzero))
    })
}

#[derive(Clone, Copy)]
struct ModelWindow {
    origin: [usize; 2],
    shape: [usize; 2],
}

impl ModelWindow {
    fn direction(self, original: &DirectionCoordinate) -> DirectionCoordinate {
        let reference = original.reference_pixel();
        original
            .clone()
            .with_reference_pixel(std::array::from_fn(|axis| {
                reference[axis] - self.origin[axis] as f64
            }))
    }
}

struct ImageTransform {
    operator: MeasurementOperator,
    coordinates: CoordinateSystem,
    frequencies: Vec<f64>,
    interpolation_frequencies: Option<Vec<f64>>,
    increment: [f64; 2],
    phase_center: [f64; 2],
    frame: String,
    options: PredictionGeometry,
    distance_m: Option<f64>,
    w_record: Option<WRecord>,
    mosaic: Option<MosaicImage>,
    window: ModelWindow,
}

struct MosaicImage {
    stokes: String,
    beam: MosaicBeam,
    sampling: u16,
}

enum MosaicBeam {
    Array(i32),
    Common(String),
    Image(PathBuf),
}

struct WRecord {
    planes: usize,
    size: usize,
    scale: f64,
    supports: Vec<i32>,
    statistics: [f64; 3],
}

impl WRecord {
    fn new(kernels: &WPlanes, count: WPlaneCount) -> Self {
        let statistics = match count {
            WPlaneCount::Fixed(_) => [-1.; 3],
            WPlaneCount::Restored { max_w, .. } => [-1., max_w, -1.],
            WPlaneCount::Auto {
                min_w,
                max_w,
                rms_w,
            } => [min_w, max_w, rms_w],
        };
        Self {
            statistics,
            planes: kernels.planes(),
            size: kernels.screen_size(),
            scale: kernels.w_scale(),
            supports: (0..kernels.planes())
                .map(|plane| i32::from(kernels.half_support(plane)))
                .collect(),
        }
    }

    fn save(&self, record: &mut RecordValue, scratch: bool, geometry: &GridGeometry) {
        record.upsert(
            "name",
            Value::Scalar(ScalarValue::String("WProjectFT".into())),
        );
        record.remove("convtype");
        let count = self.planes as i32;
        for (key, value) in [
            ("nwplanes", count),
            ("wconvsize", count),
            ("convsize", self.size as i32),
            ("convsampling", if count > 1 { 4 } else { 1 }),
            ("lastindex", 0),
        ] {
            record.upsert(key, Value::Scalar(ScalarValue::Int32(value)));
        }
        for (key, value) in ["minw", "maxw", "rmsw"].into_iter().zip(self.statistics) {
            record.upsert(key, Value::Scalar(ScalarValue::Float64(value)));
        }
        record.upsert(
            "savedwscale",
            Value::Scalar(ScalarValue::Float64(self.scale)),
        );
        record.upsert(
            "convsizes",
            Value::Array(ArrayValue::from_i32_vec(Vec::new())),
        );
        record.upsert(
            "convsupport",
            Value::Array(ArrayValue::from_i32_vec(self.supports.clone())),
        );
        if let Some(Value::Array(ArrayValue::Float64(scale))) = record.get("uvscale") {
            let mut scale = scale.iter().copied().collect::<Vec<f64>>();
            scale.push(self.scale);
            record.upsert("uvscale", Value::Array(ArrayValue::from_f64_vec(scale)));
        }
        if let Some(Value::Array(ArrayValue::Float64(offset))) = record.get("uvoffset") {
            let mut offset = offset.iter().copied().collect::<Vec<f64>>();
            offset.push(0.0);
            record.upsert("uvoffset", Value::Array(ArrayValue::from_f64_vec(offset)));
        }
        if scratch {
            // The temporary record does not leave this call. Preserve the
            // geometry used by the legacy iterator before its grid was padded.
            let GridPadding::CasaComposite(padding) = geometry.padding() else {
                unreachable!("W projection has a padded grid");
            };
            let [nx, ny] = geometry.image().shape;
            record.upsert(
                "casars_prediction_geometry",
                Value::Array(ArrayValue::from_f64_vec(vec![
                    nx as f64,
                    ny as f64,
                    f64::from(padding.get()),
                ])),
            );
        }
    }
}

fn effective_padding(options: &PredictionGeometry) -> MsResult<GridPadding> {
    // The legacy imager forwards its configured padding to WProjectFT.
    // Its GridFT factory uses unit padding even when setoptions supplies
    // another value.
    let factor = match options.settings.gridder {
        ImagerGridder::WProject(_) | ImagerGridder::AutoWProject => options.settings.padding,
        ImagerGridder::Spheroidal | ImagerGridder::Boxcar if options.tangent.is_some() => {
            options.settings.padding
        }
        ImagerGridder::Spheroidal | ImagerGridder::Boxcar => 1.0,
        ImagerGridder::Mosaic => return Ok(GridPadding::None),
    };
    Ok(GridPadding::CasaComposite(
        PaddingFactor::new(factor).map_err(invalid)?,
    ))
}

pub(super) fn automatic_w_planes(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
) -> MsResult<WPlaneCount> {
    let mut stats = WStatistics::new();
    stats.add_selection(ms, selected)?;
    stats.plane_count()
}

/// Selection-wide legacy WProjectFT statistics, accumulated in MS iterator order.
pub(super) struct WStatistics {
    min_w: f64,
    max_w: f64,
    square_sum: f64,
    rows: usize,
    last_frequency: f64,
}

impl WStatistics {
    pub(super) fn new() -> Self {
        Self {
            min_w: f64::INFINITY,
            max_w: 0.,
            square_sum: 0.,
            rows: 0,
            last_frequency: 0.,
        }
    }

    pub(super) fn add_selection(
        &mut self,
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
    ) -> MsResult<()> {
        if selected.selected_rows.is_empty() {
            return Ok(());
        }
        let spectral = ms.spectral_window()?;
        let windows = selected
            .selected_rows
            .iter()
            .map(|row| row.spectral_window_id)
            .collect::<BTreeSet<_>>();
        let mut ranges = BTreeMap::new();
        for window in windows {
            let frequencies = spectral.chan_freq(window as usize)?;
            let channels = match selected.channel_selections.get(&window) {
                Some(selection) => selection.indices(frequencies.len())?,
                None => (0..frequencies.len()).collect(),
            };
            let mut low = f64::INFINITY;
            let mut high: f64 = 0.;
            for channel in channels {
                let frequency = frequencies[channel];
                low = low.min(frequency);
                high = high.max(frequency);
            }
            ranges.insert(window, (low, high));
        }
        let main = ms.main_table();
        let uvw = main.column_accessor("UVW")?;
        let arrays = (ranges.len() > 1)
            .then(|| main.column_accessor("ARRAY_ID"))
            .transpose()?;
        let mut last_buffer = None;
        for rows in selected.selected_rows.chunks(256) {
            let row_ids = rows.iter().map(|row| row.row_index).collect::<Vec<_>>();
            let w_rows = uvw.array_cells_owned_uncached(&row_ids)?;
            let array_ids = arrays
                .as_ref()
                .map(|column| column.scalar_cells_owned_for_rows(&row_ids))
                .transpose()?;
            for (index, (row, value)) in rows.iter().zip(w_rows).enumerate() {
                let Some(ArrayValue::Float64(values)) = value else {
                    return Err(invalid("UVW must contain three doubles"));
                };
                let w = values
                    .iter()
                    .nth(2)
                    .ok_or_else(|| invalid("UVW must contain a W coordinate"))?
                    .abs();
                let (low, high) = ranges[&row.spectral_window_id];
                // CASA reduces each buffer in wavelengths before taking extrema.
                self.min_w = self.min_w.min(w * low);
                self.max_w = self.max_w.max(w * high);
                self.square_sum += w * w;
                let array = if let Some(array_ids) = &array_ids {
                    let Some(ScalarValue::Int32(array)) = &array_ids[index] else {
                        return Err(invalid("ARRAY_ID must be Int32"));
                    };
                    *array
                } else {
                    0
                };
                let key = (array, row.field_id, row.data_desc_id);
                if last_buffer.is_none_or(|last| key > last) {
                    last_buffer = Some(key);
                    self.last_frequency = high;
                }
            }
            self.rows += rows.len();
        }
        Ok(())
    }

    pub(super) fn plane_count(&self) -> MsResult<WPlaneCount> {
        if self.rows == 0 {
            return Err(invalid("automatic W projection needs a nonempty selection"));
        }
        let velocity = 299_792_458.0;
        // WProjectFT::wStat sums metres squared and scales its RMS by the
        // maximum frequency retained from the final buffer.
        Ok(WPlaneCount::Auto {
            min_w: self.min_w / velocity,
            max_w: self.max_w / velocity,
            rms_w: (self.square_sum / self.rows as f64).sqrt() * self.last_frequency / velocity,
        })
    }
}

pub(super) fn validate_short_cubic(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    matching_frequencies: &[f64],
    convert: bool,
) -> MsResult<()> {
    let increment = matching_frequencies[1] - matching_frequencies[0];
    let axis = SpectralAxis::tabular(matching_frequencies, increment).map_err(invalid)?;
    let resampler = SpectralResampler::channel_local(axis.clone(), SpectralKernel::Cubic)
        .with_legacy_prediction_axis(axis);
    let engine = convert.then(|| MsCalEngine::new(ms)).transpose()?;
    let windows = ms.spectral_window()?;
    let mut by_window = BTreeMap::new();
    let mut buffers = BTreeSet::new();
    for row in &selected.selected_rows {
        let id = row.spectral_window_id;
        let (reference, frequencies) = match by_window.entry(id) {
            std::collections::btree_map::Entry::Occupied(window) => window.into_mut(),
            std::collections::btree_map::Entry::Vacant(window) => {
                let raw = windows.chan_freq(id as usize)?;
                let frequencies = if let Some(channel) = selected.channel_selections.get(&id) {
                    let (span, stride) = channel_span(channel);
                    raw.get(span)
                        .ok_or_else(|| invalid("legacy cubic selection is outside its window"))?
                        .iter()
                        .step_by(stride)
                        .copied()
                        .collect()
                } else {
                    raw
                };
                window.insert((windows.frequency_reference(id as usize)?, frequencies))
            }
        };
        if frequencies.len() < 2 {
            continue;
        }
        let frame = engine.as_ref().filter(|_| *reference != FrequencyRef::LSRK);
        let key = frame.map(|_| (row.field_id, row.time_mjd_seconds.to_bits()));
        if !buffers.insert((id, key)) {
            continue;
        }
        if let Some(engine) = frame {
            let frame =
                engine.spectral_frame_observatory(row.time_mjd_seconds, row.field_id as usize)?;
            let ratio = convert_frequency_to_frame_with_frame(
                *reference,
                FrequencyRef::LSRK,
                1.,
                Some(&frame),
            )?;
            let frequencies = frequencies
                .iter()
                .map(|value| value * ratio)
                .collect::<Vec<_>>();
            resampler
                .validate_legacy_prediction(&frequencies)
                .map_err(invalid)?;
        } else {
            resampler
                .validate_legacy_prediction(frequencies)
                .map_err(invalid)?;
        }
    }
    Ok(())
}

impl ImageTransform {
    fn new(
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
        image: &PagedImage<f32>,
        scratch: bool,
        options: PredictionGeometry,
        window: ModelWindow,
    ) -> MsResult<Self> {
        let [_, _, npol, channels] = *image.shape() else {
            return Err(invalid(
                "ft requires four direction, Stokes and spectral image axes",
            ));
        };
        let coords = image.coordinates();
        let direction = coordinate_index(coords, CoordinateType::Direction)?;
        let polarization = coordinate_index(coords, CoordinateType::Stokes)?;
        let spectral = coordinate_index(coords, CoordinateType::Spectral)?;
        let CoordinateModel::Direction(direction) = coords.coordinate(direction) else {
            unreachable!()
        };
        let direction = window.direction(direction);
        let [nx, ny] = window.shape;
        let CoordinateModel::Stokes(stokes) = coords.coordinate(polarization) else {
            unreachable!()
        };
        let CoordinateModel::Spectral(spectral) = coords.coordinate(spectral) else {
            unreachable!()
        };
        let interpolation_frequencies =
            if scratch && channels > 1 && matches!(options.settings.frequency_interpolation, 1..=3)
            {
                // FTMachine saves these image world frequencies before stripping
                // the reference conversion from the channel-matching coordinate.
                let original = spectral_with_runtime(spectral)?;
                Some(
                    (0..channels)
                        .map(|channel| original.world_frequency_hz(channel as f64).map_err(invalid))
                        .collect::<MsResult<Vec<_>>>()?,
                )
            } else {
                None
            };
        // A new legacy imager has no active frequency conversion even when
        // the input image carries a saved reference conversion.
        let spectral = if options.frequency_frame_valid {
            spectral_with_runtime(spectral)?.into_owned()
        } else {
            spectral.clone().without_conversion()
        };
        if npol != stokes.n_stokes() {
            return Err(invalid(
                "the model image's Stokes axis does not match its planes",
            ));
        }
        if options
            .voltage_pattern
            .as_ref()
            .is_some_and(|beam| beam.has_squint())
        {
            use StokesType::{I, LL, Q, RR, U, V};
            if !matches!(
                stokes.stokes(),
                [I] | [Q] | [V] | [I, Q] | [I, U] | [U, I] | [I, V] | [RR, LL] | [I, Q, U, V]
            ) {
                return Err(invalid(
                    "legacy voltage-pattern squint for these Stokes axes is pending #1520",
                ));
            }
        }
        let correlations = first_correlations(ms, selected)?
            .into_iter()
            .map(|code| {
                Correlation::from_casacore_code(code)
                    .ok_or_else(|| invalid("invalid correlation code"))
            })
            .collect::<MsResult<Vec<_>>>()?;
        let requested = stokes
            .stokes()
            .iter()
            .map(|code| {
                polarization_coordinate(*code)
                    .ok_or_else(|| invalid("unsupported polarization plane in model image"))
            })
            .collect::<MsResult<Vec<_>>>()?;
        use PolarizationCoordinate::{StokesI, StokesQ, StokesU};
        let routing = if matches!(
            requested.as_slice(),
            [StokesQ] | [StokesI, StokesU] | [StokesU, StokesI]
        ) {
            // CASA can save the nonzero cross-hand model even when the
            // selected MeasurementSet contains only parallel hands. I/U
            // also retains the original U plane even if no row uses it.
            PolarizationRouting::from_saved_planes(&correlations, &requested)
        } else {
            PolarizationRouting::compile(&correlations, &requested)
        }
        .map_err(invalid)?;
        let increment = [direction.increment()[0], direction.increment()[1]];
        // GridFT phases on the world direction of the integer image
        // midpoint, which need not be the WCS reference pixel.
        let reference = [nx / 2, ny / 2];
        let padding = effective_padding(&options)?;
        let geometry = GridGeometry::new(
            ImageExtent {
                shape: [nx, ny],
                increment_rad: increment,
                reference_pixel: reference,
            },
            padding,
        )
        .map_err(invalid)?;
        let [gx, gy] = geometry.grid_shape();
        let phase_center = direction
            .pixel_to_world([reference[0] as f64, reference[1] as f64])
            .map_err(invalid)?;
        let mut coordinates = CoordinateSystem::new();
        let grid_direction = if scratch && matches!(options.settings.gridder, ImagerGridder::Mosaic)
        {
            // The live pointing test uses the subimage WCS. The saved image
            // has its reference direction at the center of the Fourier grid.
            direction.clone()
        } else {
            direction
                .clone()
                .with_reference_value(phase_center)
                .with_reference_pixel([(gx / 2) as f64, (gy / 2) as f64])
        };
        coordinates.add_coordinate(grid_direction);
        coordinates.add_coordinate(StokesCoordinate::new(
            routing
                .grid()
                .iter()
                .map(grid_stokes)
                .collect::<MsResult<_>>()?,
        ));
        coordinates.add_coordinate(spectral.clone());
        let mosaic = matches!(options.settings.gridder, ImagerGridder::Mosaic)
            .then(|| {
                mosaic_image(
                    ms,
                    stokes,
                    [nx, ny],
                    options.mosaic_voltage_table.as_deref(),
                )
            })
            .transpose()?;
        *coordinates.obs_info_mut() = ObsInfo::new("UNKNOWN").with_observer("UNKNOWN");
        let frequencies = (0..channels)
            .map(|index| spectral.world_frequency_hz(index as f64).map_err(invalid))
            .collect::<MsResult<Vec<_>>>()?;
        if scratch && options.settings.frequency_interpolation == 2 && (2..4).contains(&channels) {
            validate_short_cubic(ms, selected, &frequencies, options.frequency_frame_valid)?;
        }
        let basis = Basis::ChannelLocal {
            planes: channels as u32,
        };
        let mut w_record = None;
        let kernels: Box<dyn ConvolutionFunctionSet> = match options.settings.gridder {
            ImagerGridder::Spheroidal => Box::new(Spheroidal::new(&geometry, &routing)),
            ImagerGridder::Boxcar | ImagerGridder::Mosaic => {
                Box::new(Boxcar::new(&geometry, &routing))
            }
            gridder => {
                let planes = match gridder {
                    ImagerGridder::WProject(planes) => planes,
                    ImagerGridder::AutoWProject => automatic_w_planes(ms, selected)?,
                    _ => unreachable!("ordinary gridders were already constructed"),
                };
                let kernels = WPlanes::new(&geometry, &routing, planes).map_err(invalid)?;
                w_record = Some(WRecord::new(&kernels, planes));
                Box::new(kernels)
            }
        };
        let operator = MeasurementOperator::new(
            geometry,
            basis,
            routing,
            kernels,
            options.settings.precision,
        );
        let distance_m = match image.misc_info().get("distance") {
            None => None,
            Some(Value::Scalar(ScalarValue::Float64(distance))) => Some(*distance),
            Some(Value::Scalar(ScalarValue::Float32(distance))) => Some(f64::from(*distance)),
            Some(_) => return Err(invalid("model image distance must be a number in metres")),
        };
        Ok(Self {
            operator,
            coordinates,
            frequencies,
            interpolation_frequencies,
            increment,
            phase_center,
            frame: direction.direction_ref().as_str().to_owned(),
            mosaic,
            options,
            distance_m,
            w_record,
            window,
        })
    }

    fn grid(&self, image: &PagedImage<f32>, path: &Path) -> MsResult<bool> {
        let [_, _, npol, channels] = *image.shape() else {
            unreachable!()
        };
        let [nx, ny] = self.window.shape;
        let [first_x, first_y] = self.window.origin;
        let [gx, gy] = self.operator.geometry().grid_shape();
        let mosaic = self
            .mosaic
            .as_ref()
            .map(|mosaic| ImageCorrection::mosaic_sinc([gx, gy], mosaic.sampling));
        let beam = self
            .options
            .voltage_pattern
            .as_ref()
            .map(|beam| {
                let coords = image.coordinates();
                let index = coordinate_index(coords, CoordinateType::Direction)?;
                let CoordinateModel::Direction(direction) = coords.coordinate(index) else {
                    unreachable!("direction coordinate");
                };
                let direction = self.window.direction(direction);
                let separations = beam.model_separations(&direction, [nx, ny])?;
                Ok::<_, casa_ms::MsError>((beam, direction, separations))
            })
            .transpose()?;
        let grid = self.operator.polarization().grid();
        let squint = beam.as_ref().is_some_and(|(beam, _, _)| beam.has_squint())
            && grid.iter().any(|plane| {
                matches!(
                    plane,
                    GridPolarization::Correlation(Correlation::RR | Correlation::LL)
                )
            });
        let grid_pols = grid.len();
        let settings = self.options.settings;
        let plane_bytes = gx.saturating_mul(gy).saturating_mul(8);
        let cache_bytes = usize::try_from(settings.cache_size)
            .ok()
            .filter(|pixels| *pixels > 0)
            .map_or(plane_bytes, |pixels| {
                pixels.saturating_mul(8).min(plane_bytes)
            });
        let tile = usize::try_from(settings.tile_size)
            .ok()
            .filter(|tile| *tile > 0 && *tile != 16);
        let tile_shape = vec![
            tile.map_or(gx, |tile| gx.min(tile)),
            tile.map_or(gy, |tile| gy.min(tile)),
            1,
            1,
        ];
        let mut output = PagedImage::<Complex32>::create_with_tile_shape_and_cache(
            vec![gx, gy, grid_pols, channels],
            tile_shape,
            self.coordinates.clone(),
            path,
            cache_bytes,
        )
        .map_err(invalid)?;
        let taper_bytes = if squint {
            model_taper_bytes([nx, ny], grid_pols)
        } else {
            0
        };
        let work_bytes =
            model_plane_memory([nx, ny], npol, [gx, gy], grid_pols).saturating_add(taper_bytes);
        let (workers, reservation) =
            model_workers(self.options.native_threads, channels, work_bytes);
        let (worker_memory, team) = match WorkerTeam::new(workers) {
            Ok(team) => (reservation, team),
            Err(_) => {
                drop(reservation);
                (None, WorkerTeam::new(1).map_err(invalid)?)
            }
        };
        let _serial_taper_memory = if squint && worker_memory.is_none() {
            let host = HostResources::detect().map_err(invalid)?;
            Some(
                admit(
                    &host,
                    &ResourcePolicy::Balanced,
                    &Demand {
                        phase: "legacy squinted image tapers",
                        memory: taper_bytes,
                    },
                )
                .map_err(invalid)?,
            )
        } else {
            None
        };
        tracing::debug!(
            requested = self.options.native_threads,
            workers = team.workers(),
            channels,
            "preparing imager Fourier model planes"
        );
        let nonzero = prepare_model_planes(
            &team,
            channels,
            |plane| {
                image
                    .get_slice(&[first_x, first_y, 0, plane], &[nx, ny, npol, 1])
                    .map_err(invalid)
            },
            |plane, values| {
                let plain_images = || {
                    (0..npol)
                        .map(|pol| Array2::from_shape_fn((ny, nx), |(y, x)| values[[x, y, pol, 0]]))
                        .collect()
                };
                let frequency = self.frequencies[plane];
                let (images, factors) = match &beam {
                    Some((beam, direction, _)) if squint => {
                        // CASA converts to correlation planes before applying
                        // PBMath to the displaced RR and LL receptors.
                        let tapers =
                            beam.squinted_model_tapers(direction, [nx, ny], frequency, grid)?;
                        (plain_images(), Some(tapers))
                    }
                    Some((beam, direction, separation)) => (
                        beam.model_plane(direction, separation.as_ref(), values, frequency)?,
                        None,
                    ),
                    None => (plain_images(), None),
                };
                let prescale = factors
                    .as_deref()
                    .map_or(ModelPrescale::Unit, ModelPrescale::GridPlanes);
                let model = ModelImages {
                    first_plane: plane as u32,
                    planes: vec![ModelPlane { images }],
                };
                match &mosaic {
                    Some(correction) => self
                        .operator
                        .prepare_model_with_correction(&model, prescale, correction),
                    None => self.operator.prepare_model(&model, prescale),
                }
                .map_err(invalid)
            },
            |plane, grid| self.write_plane(&mut output, grid, plane),
        )?;
        output.save().map_err(invalid)?;
        Ok(nonzero)
    }

    fn write_plane(
        &self,
        output: &mut PagedImage<Complex32>,
        grid: &PreparedModelGrids,
        plane: usize,
    ) -> MsResult<()> {
        let [gx, gy] = self.operator.geometry().grid_shape();
        for pol in 0..self.operator.polarization().grid_pols() {
            let shape = IxDyn(&[gx, gy, 1, 1]).f();
            let values = match self.operator.precision() {
                GridPrecision::F32 => {
                    let cells = grid.block::<f32>(0, pol, 0);
                    ArrayD::from_shape_fn(shape, |index| cells[index[1] * gx + index[0]])
                }
                GridPrecision::F64 => {
                    let cells = grid.block::<f64>(0, pol, 0);
                    ArrayD::from_shape_fn(shape, |index| {
                        let cell = cells[index[1] * gx + index[0]];
                        Complex32::new(cell.re as f32, cell.im as f32)
                    })
                }
            };
            output
                .put_slice(&values, &[0, 0, pol, plane])
                .map_err(invalid)?;
        }
        Ok(())
    }

    fn record(
        &self,
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
        path: &Path,
        scratch: bool,
    ) -> MsResult<RecordValue> {
        let engine = MsCalEngine::new(ms).map_err(invalid)?;
        let (lon, lat, radius) = engine.observatory_position().as_spherical();
        let row = &selected.selected_rows[0];
        let spectral = ms.spectral_window()?;
        let window = row.spectral_window_id as usize;
        let mut channels = spectral.chan_freq(window)?;
        let routing = self.operator.polarization();
        let axis = coordinate_index(&self.coordinates, CoordinateType::Spectral)?;
        let CoordinateModel::Spectral(axis) = self.coordinates.coordinate(axis) else {
            unreachable!();
        };
        let frequency_frame_valid = self.options.frequency_frame_valid;
        if frequency_frame_valid {
            let frame =
                engine.spectral_frame_observatory(row.time_mjd_seconds, row.field_id as usize)?;
            let ratio = convert_frequency_to_frame_with_frame(
                spectral.frequency_reference(window)?,
                FrequencyRef::LSRK,
                1.,
                Some(&frame),
            )?;
            for frequency in &mut channels {
                *frequency *= ratio;
            }
        }
        let channel_map = match_channels(axis, self.frequencies.len(), &channels);
        let buffer = FirstBuffer {
            field: row.field_id,
            frequencies_lsrk_hz: channels,
            channel_map,
            correlations: first_correlations(ms, selected)?,
            polarization_map: routing
                .pol_map()
                .iter()
                .map(|pol| pol.map_or(-1, i32::from))
                .collect(),
        };
        let spectral_record = casa_spectral(&CoordinateModel::Spectral(axis.clone()).to_record());
        let mut record = GridFt {
            disk_image: path,
            grid: self.operator.geometry().grid_shape(),
            polarizations: routing.grid_pols(),
            image_frequencies_hz: &self.frequencies,
            increment_rad: self.increment,
            phase_centre_rad: self.phase_center,
            direction_frame: &self.frame,
            observatory: self
                .options
                .settings
                .observatory
                .unwrap_or([lon, lat, radius]),
            spectral: spectral_record,
            buffer: &buffer,
            interpolation: self.options.settings.frequency_interpolation,
            frequency_frame_valid,
        }
        .record();
        if let Some(tangent) = &self.options.tangent {
            set_grid_tangent(&mut record, tangent);
        }
        if let Some(frequencies) = &self.interpolation_frequencies {
            // This temporary record never leaves the active scratch writer.
            record.upsert("tovis", Value::Scalar(ScalarValue::Bool(false)));
            record.upsert(
                "casars_live_image_frequency",
                Value::Array(ArrayValue::from_f64_vec(frequencies.clone())),
            );
        }
        let settings = self.options.settings;
        record.upsert(
            "usedoublegrid",
            Value::Scalar(ScalarValue::Bool(settings.precision == GridPrecision::F64)),
        );
        record.upsert(
            "cachesize",
            Value::Scalar(ScalarValue::Int64(settings.cache_size)),
        );
        record.upsert(
            "tilesize",
            Value::Scalar(ScalarValue::Int32(settings.tile_size)),
        );
        record.upsert(
            "numthreads",
            Value::Scalar(ScalarValue::Int32(settings.threads)),
        );
        if let Some(epoch) = self.options.phase_centre_time {
            record.upsert(
                "phasecentertime",
                Value::Scalar(ScalarValue::Float64(epoch)),
            );
        }
        if let Some(distance) = self.distance_m {
            record.upsert("distance", Value::Scalar(ScalarValue::Float64(distance)));
        }
        if matches!(settings.gridder, ImagerGridder::Boxcar) {
            record.upsert("convtype", Value::Scalar(ScalarValue::String("BOX".into())));
        }
        if let Some(w_record) = &self.w_record {
            w_record.save(&mut record, scratch, self.operator.geometry());
        }
        if let Some(mosaic) = &self.mosaic {
            match &mosaic.beam {
                MosaicBeam::Array(class) => {
                    legacy_mosaic(&mut record, &mosaic.stokes, *class, mosaic.sampling);
                }
                MosaicBeam::Common(telescope) => {
                    legacy_common_mosaic(&mut record, &mosaic.stokes, telescope, mosaic.sampling);
                }
                MosaicBeam::Image(table) => {
                    let table = if scratch {
                        table.clone()
                    } else {
                        legacy_mosaic_table(table, path)?
                    };
                    legacy_image_mosaic(
                        &mut record,
                        &mosaic.stokes,
                        &table.to_string_lossy(),
                        mosaic.sampling,
                    );
                }
            }
        }
        record.upsert(
            "spwchanselflag",
            Value::Array(ArrayValue::Int32(selection_mask(ms, selected)?)),
        );
        record_selection(&mut record, ms, selected, axis, self.frequencies.len())?;
        Ok(record)
    }
}

fn prepare_model_planes(
    team: &WorkerTeam,
    channels: usize,
    mut read: impl FnMut(usize) -> MsResult<ArrayD<f32>>,
    prepare: impl Fn(usize, &ArrayD<f32>) -> MsResult<PreparedModelGrids> + Send + Sync,
    mut write: impl FnMut(usize, &PreparedModelGrids) -> MsResult<()>,
) -> MsResult<bool> {
    let mut nonzero = false;
    let workers = team.workers();
    for first_plane in (0..channels).step_by(workers) {
        let end = (first_plane + workers).min(channels);
        let mut planes = Vec::with_capacity(end - first_plane);
        for plane in first_plane..end {
            let values = read(plane)?;
            nonzero |= values.iter().any(|value| *value != 0.0);
            planes.push((values, None));
        }
        team.for_each_mut(&mut planes, |index, (values, grid)| {
            *grid = Some(prepare(first_plane + index, values)?);
            Ok::<_, casa_ms::MsError>(())
        })?;
        for (index, (_, grid)) in planes.into_iter().enumerate() {
            write(
                first_plane + index,
                &grid.expect("the Fourier plane was prepared"),
            )?;
        }
    }
    Ok(nonzero)
}

fn model_worker_memory(workers: usize, work_bytes: u64) -> u64 {
    // Match the runtime's other owners of a dedicated worker team.
    let team_worker_bytes = (WORKER_STACK_BYTES + 65_536) as u64;
    work_bytes
        .saturating_add(team_worker_bytes)
        .saturating_mul(workers as u64)
}

fn model_plane_memory(
    [nx, ny]: [usize; 2],
    image_polarizations: usize,
    [gx, gy]: [usize; 2],
    grid_polarizations: usize,
) -> u64 {
    let image_bytes = nx
        .saturating_mul(ny)
        .saturating_mul(image_polarizations)
        .saturating_mul(2 * size_of::<f32>());
    let grid_cells = gx.saturating_mul(gy);
    let grid_bytes = grid_cells
        .saturating_mul(grid_polarizations + 1)
        .saturating_mul(2 * size_of::<f64>());
    (image_bytes.saturating_add(grid_bytes) as u64).saturating_add(grid_planning_bytes(grid_cells))
}

fn model_taper_bytes([nx, ny]: [usize; 2], grid_polarizations: usize) -> u64 {
    nx.saturating_mul(ny)
        .saturating_mul(grid_polarizations)
        .saturating_mul(size_of::<f32>()) as u64
}

#[cfg(test)]
#[path = "image_worker_tests.rs"]
mod worker_tests;

fn model_workers(threads: i32, planes: usize, work_bytes: u64) -> (usize, Option<Reservation>) {
    if threads == 1 || planes <= 1 {
        return (1, None);
    }
    let Ok(host) = HostResources::detect() else {
        return (1, None);
    };
    let requested = usize::try_from(threads)
        .ok()
        .filter(|count| *count > 0)
        .unwrap_or(host.threads);
    let workers = requested.min(host.threads).min(planes);
    if workers <= 1 {
        return (1, None);
    }
    let policy = ResourcePolicy::Balanced;
    let demand = Demand {
        phase: "legacy imager model planes",
        memory: model_worker_memory(workers, work_bytes),
    };
    match admit(&host, &policy, &demand) {
        Ok(reservation) => (workers, Some(reservation)),
        Err(_) => (1, None),
    }
}

fn mosaic_image(
    ms: &MeasurementSet,
    stokes: &StokesCoordinate,
    shape: [usize; 2],
    voltage_table: Option<&Path>,
) -> MsResult<MosaicImage> {
    let beam = if let Some(table) = voltage_table {
        MosaicBeam::Image(table.to_owned())
    } else {
        legacy_mosaic_beam(ms.observation()?.string(0, "TELESCOPE_NAME")?)
    };
    let sampling = if matches!(beam, MosaicBeam::Common(_)) {
        MosaicPb::simple_oversampling(shape)
    } else {
        MOSAIC_OVERSAMPLING
    };
    Ok(MosaicImage {
        stokes: stokes.stokes().iter().map(|stokes| stokes.name()).collect(),
        beam,
        sampling,
    })
}

fn legacy_mosaic_beam(telescope: String) -> MosaicBeam {
    // CASA enumerates these names before selecting SimplePBConvFunc.
    match telescope.as_str() {
        "EVLA" => MosaicBeam::Array(1),
        "ATCA_L1" | "ATCA_L2" | "ATCA_L3" | "ATCA_16" | "ATCA_S" | "ATCA_C" | "ATCA_C_RI"
        | "ATCA" | "ATCA_X" | "ATCA_K" | "ATCA_Q" | "ATCA_W" | "HATCREEK" | "BIMA" | "NGVLA"
        | "VLA" | "VLA_INVERSE" | "VLA_NVSS" | "VLA_2NULL" | "VLA_4" | "VLA_P" | "VLA_L"
        | "VLA_C" | "VLA_X" | "VLA_U" | "VLA_K" | "VLA_Q" | "WSRT" | "WSRT_LOW" | "GBT"
        | "GMRT" | "NRAO12M" | "IRAMPDB" | "IRAM30M" | "NRAO140FT" | "ALMASD" | "SMA" | "ATA"
        | "NONE" | "DEFAULT" => MosaicBeam::Common(telescope),
        _ => MosaicBeam::Array(2),
    }
}

pub(crate) fn match_channels(
    axis: &SpectralCoordinate,
    channels: usize,
    frequencies: &[f64],
) -> Vec<i32> {
    frequencies
        .iter()
        .map(|&frequency| match_channel(axis, channels, frequency))
        .collect()
}

pub(crate) fn match_channel(axis: &SpectralCoordinate, channels: usize, frequency: f64) -> i32 {
    axis.pixel_at_frequency_hz(frequency)
        .ok()
        .map(f64::round)
        .filter(|&index| (0.0..channels as f64).contains(&index))
        .map_or(-1, |index| index as i32)
}

pub(super) fn record_selection(
    record: &mut RecordValue,
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    axis: &SpectralCoordinate,
    channels: usize,
) -> MsResult<()> {
    let windows = ms.spectral_window()?;
    let selected_windows: BTreeSet<_> = selected
        .selected_rows
        .iter()
        .map(|row| row.spectral_window_id)
        .collect();
    let count = selected_windows.last().map_or(0, |&last| last as usize + 1);
    let mut maps = RecordValue::default();
    let mut counts = Vec::new();
    for window in 0..count {
        let map = if selected_windows.contains(&(window as i32)) {
            let frequencies = windows.chan_freq(window)?;
            counts.push(frequencies.len() as i32);
            match_channels(axis, channels, &frequencies)
        } else {
            Vec::new()
        };
        maps.upsert(
            format!("*{}", window + 1),
            Value::Array(ArrayValue::from_i32_vec(map)),
        );
    }
    record.upsert(
        "selectedspw",
        Value::Array(ArrayValue::from_i32_vec(
            selected_windows.into_iter().collect(),
        )),
    );
    record.upsert(
        "nvischanmulti",
        Value::Array(ArrayValue::from_i32_vec(counts)),
    );
    record.upsert("multichanmaprec", Value::Record(maps));
    record.upsert(
        "doconversion",
        Value::Array(ArrayValue::Bool(ArrayD::from_elem(IxDyn(&[count]), false))),
    );
    Ok(())
}

pub(super) fn selection_mask(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
) -> MsResult<ArrayD<i32>> {
    let windows = ms.spectral_window()?;
    let counts = (0..windows.row_count())
        .map(|spw| windows.num_chan(spw))
        .collect::<MsResult<Vec<_>>>()?;
    let max_channels = counts.iter().copied().max().unwrap_or(0) as usize;
    let mut mask = ArrayD::zeros(IxDyn(&[1, counts.len(), max_channels]).f());
    let selected_windows: BTreeSet<_> = selected
        .selected_rows
        .iter()
        .map(|row| row.spectral_window_id)
        .collect();
    for id in selected_windows {
        for channel in 0..counts[id as usize] as usize {
            mask[[0, id as usize, channel]] = 1;
        }
    }
    for (&window, channels) in &selected.channel_selections {
        let count = counts[window as usize] as usize;
        for channel in 0..count {
            mask[[0, window as usize, channel]] = 0;
        }
        let (span, _) = channel_span(channels);
        // Imager::setdata shifts its mask to the first selected channel.
        // GridFT then indexes this mask with the compact buffer index.
        for channel in channels.indices(count)? {
            mask[[0, window as usize, channel - span.start]] = 1;
        }
    }
    Ok(mask)
}

pub(super) fn first_correlations(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
) -> MsResult<Vec<i32>> {
    ms.polarization()?
        .corr_type(selected.selected_rows[0].polarization_id as usize)
}

pub(crate) fn coordinate_index(
    coordinates: &CoordinateSystem,
    kind: CoordinateType,
) -> MsResult<usize> {
    coordinates
        .find_coordinate(kind)
        .ok_or_else(|| invalid(format!("model image has no {kind:?} coordinate")))
}

pub(super) fn grid_stokes(plane: &GridPolarization) -> MsResult<StokesType> {
    match plane {
        GridPolarization::StokesI => Ok(StokesType::I),
        GridPolarization::Correlation(correlation) => {
            StokesType::from_code(correlation.casacore_code())
                .ok_or_else(|| invalid("invalid grid polarization"))
        }
    }
}

pub(crate) fn polarization_coordinate(stokes: StokesType) -> Option<PolarizationCoordinate> {
    match stokes {
        StokesType::I => Some(PolarizationCoordinate::StokesI),
        StokesType::Q => Some(PolarizationCoordinate::StokesQ),
        StokesType::U => Some(PolarizationCoordinate::StokesU),
        StokesType::V => Some(PolarizationCoordinate::StokesV),
        StokesType::XX => Some(PolarizationCoordinate::LinearXx),
        StokesType::XY => Some(PolarizationCoordinate::LinearXy),
        StokesType::YX => Some(PolarizationCoordinate::LinearYx),
        StokesType::YY => Some(PolarizationCoordinate::LinearYy),
        StokesType::RR => Some(PolarizationCoordinate::CircularRr),
        StokesType::RL => Some(PolarizationCoordinate::CircularRl),
        StokesType::LR => Some(PolarizationCoordinate::CircularLr),
        StokesType::LL => Some(PolarizationCoordinate::CircularLl),
        _ => None,
    }
}
