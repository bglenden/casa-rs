// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare image models with the existing gridder for CASA's legacy imager.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, ObsInfo, SpectralCoordinate,
    StokesCoordinate, StokesType,
};
use casa_images::PagedImage;
use casa_imaging_model::PolarizationCoordinate;
use casa_imaging_operator::{
    Basis, Boxcar, ConvolutionFunctionSet, GridGeometry, GridPadding, GridPolarization,
    GridPrecision, ImageExtent, MeasurementOperator, ModelImages, ModelPlane, ModelPrescale,
    PaddingFactor, PolarizationRouting, SpectralAxis, SpectralKernel, SpectralResampler,
    Spheroidal, WPlaneCount, WPlanes,
};
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::virtual_model::ft_record::{FirstBuffer, GridFt, casa_spectral, legacy_multi_term};
use casa_ms::{
    MeasurementSet, MsResult, ResolvedMsSelection, VirtualModelEntry, VirtualModelKind,
    convert_frequency_to_frame_with_frame, virtual_model_image_path,
};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::{ArrayValue, Complex32, Correlation, RecordValue, ScalarValue, Value};
use ndarray::{Array2, ArrayD, IxDyn, ShapeBuilder};

use super::settings::{ImagerGridder, PredictionGeometry};
use super::{channel_span, invalid, model_entry, spectral_with_runtime};

/// An uncommitted Fourier image. The scratch path uses it until the write
/// finishes. The virtual model can keep it once its on-disk record is filed.
pub(crate) struct ImageModel {
    pub(crate) entry: VirtualModelEntry,
    backing: GridLifetime,
}

impl ImageModel {
    pub(crate) fn copy_to(&self, target: &MeasurementSet) -> MsResult<Self> {
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
        }
        Ok(Self { entry, backing })
    }

    pub(crate) fn retain(&mut self) {
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
pub(crate) struct TaylorTerm {
    pub(crate) count: i32,
    pub(crate) index: i32,
    pub(crate) reference_hz: f64,
}

/// The image spectral coordinate determines the default Taylor frequency.
pub(crate) fn reference_frequency(path: &Path) -> MsResult<f64> {
    let image = PagedImage::<f32>::open(path).map_err(invalid)?;
    let index = coordinate_index(image.coordinates(), CoordinateType::Spectral)?;
    let CoordinateModel::Spectral(spectral) = image.coordinates().coordinate(index) else {
        unreachable!("a spectral coordinate index names a spectral coordinate")
    };
    let pixel = spectral.reference_pixel()[0];
    spectral.world_frequency_hz(pixel).map_err(invalid)
}

pub(crate) fn prepare_image(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    path: &Path,
    taylor: Option<TaylorTerm>,
    keep_zero: bool,
    scratch: bool,
) -> MsResult<Option<ImageModel>> {
    prepare_configured_image(
        ms,
        selected,
        path,
        taylor,
        keep_zero,
        scratch,
        PredictionGeometry::default(),
    )
}

pub(super) fn check_scratch_cube(
    scratch: bool,
    interpolation: i32,
    shape: &[usize],
) -> MsResult<()> {
    if scratch && interpolation == 3 && shape.get(3).is_some_and(|&channels| channels > 1) {
        return Err(invalid(
            "legacy scratch cube prediction with spline frequency interpolation is pending in #1520",
        ));
    }
    Ok(())
}

pub(super) fn prepare_configured_image(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    path: &Path,
    taylor: Option<TaylorTerm>,
    keep_zero: bool,
    scratch: bool,
    options: PredictionGeometry,
) -> MsResult<Option<ImageModel>> {
    let image = PagedImage::<f32>::open(path).map_err(invalid)?;
    check_scratch_cube(
        scratch,
        options.settings.frequency_interpolation,
        image.shape(),
    )?;
    let layout = ImageTransform::new(ms, selected, &image, scratch, options)?;
    let mut path = virtual_model_image_path(ms)?;
    if scratch {
        let name = path
            .file_name()
            .expect("the grid has a name")
            .to_string_lossy();
        path.set_file_name(format!("scratch-{name}"));
    }
    let backing = GridLifetime {
        path,
        retained: false,
    };
    if !layout.grid(&image, &backing.path)? && !keep_zero {
        return Ok(None);
    }
    let record = layout.record(ms, selected, &backing.path, scratch)?;
    let container = match taylor {
        Some(term) => legacy_multi_term(record, term.count, term.index, term.reference_hz),
        None => record,
    };
    let entry = model_entry(ms, selected, VirtualModelKind::FtMachine, container)?;
    Ok(Some(ImageModel { entry, backing }))
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
        ImagerGridder::Spheroidal | ImagerGridder::Boxcar => 1.0,
    };
    Ok(GridPadding::CasaComposite(
        PaddingFactor::new(factor).map_err(invalid)?,
    ))
}

fn automatic_w_planes(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
) -> MsResult<WPlaneCount> {
    let velocity = 299_792_458.0;
    let spectral = ms.spectral_window()?;
    let windows = selected
        .selected_rows
        .iter()
        .map(|row| row.spectral_window_id)
        .collect::<BTreeSet<_>>();
    let mut min_frequency = f64::INFINITY;
    let mut max_frequency: f64 = 0.;
    for window in windows {
        let frequencies = spectral.chan_freq(window as usize)?;
        let channels = match selected.channel_selections.get(&window) {
            Some(selection) => selection.indices(frequencies.len())?,
            None => (0..frequencies.len()).collect(),
        };
        for channel in channels {
            min_frequency = min_frequency.min(frequencies[channel]);
            max_frequency = max_frequency.max(frequencies[channel]);
        }
    }
    let uvw = ms.main_table().column_accessor("UVW")?;
    let mut min_w = f64::INFINITY;
    let mut max_w: f64 = 0.;
    let mut square_sum: f64 = 0.;
    for rows in selected.selected_rows.chunks(256) {
        let row_ids = rows.iter().map(|row| row.row_index).collect::<Vec<_>>();
        for value in uvw.array_cells_owned_uncached(&row_ids)? {
            let Some(ArrayValue::Float64(values)) = value else {
                return Err(invalid("UVW must contain three doubles"));
            };
            let w = values
                .iter()
                .nth(2)
                .ok_or_else(|| invalid("UVW must contain a W coordinate"))?
                .abs();
            min_w = min_w.min(w);
            max_w = max_w.max(w);
            square_sum += w * w;
        }
    }
    Ok(WPlaneCount::Auto {
        min_w: min_w * min_frequency / velocity,
        max_w: max_w * max_frequency / velocity,
        rms_w: (square_sum / selected.selected_rows.len() as f64).sqrt() * max_frequency / velocity,
    })
}

fn validate_short_cubic(
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
                    channel
                        .indices(raw.len())?
                        .into_iter()
                        .map(|index| raw[index])
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
    ) -> MsResult<Self> {
        let [nx, ny, npol, channels] = *image.shape() else {
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
        let CoordinateModel::Stokes(stokes) = coords.coordinate(polarization) else {
            unreachable!()
        };
        let CoordinateModel::Spectral(spectral) = coords.coordinate(spectral) else {
            unreachable!()
        };
        let interpolation_frequencies =
            if scratch && channels > 1 && matches!(options.settings.frequency_interpolation, 1 | 2)
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
        let routing = PolarizationRouting::compile(&correlations, &requested).map_err(invalid)?;
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
        coordinates.add_coordinate(
            direction
                .clone()
                .with_reference_value(phase_center)
                .with_reference_pixel([(gx / 2) as f64, (gy / 2) as f64]),
        );
        coordinates.add_coordinate(StokesCoordinate::new(
            routing
                .grid()
                .iter()
                .map(grid_stokes)
                .collect::<MsResult<_>>()?,
        ));
        coordinates.add_coordinate(spectral.clone());
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
            ImagerGridder::Boxcar => Box::new(Boxcar::new(&geometry, &routing)),
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
            options,
            distance_m,
            w_record,
        })
    }

    fn grid(&self, image: &PagedImage<f32>, path: &Path) -> MsResult<bool> {
        let [nx, ny, npol, channels] = *image.shape() else {
            unreachable!()
        };
        let [gx, gy] = self.operator.geometry().grid_shape();
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
                Ok::<_, casa_ms::MsError>((&beam.pattern, beam.separations(direction, [nx, ny])?))
            })
            .transpose()?;
        let grid_pols = self.operator.polarization().grid_pols();
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
        let mut nonzero = false;
        for plane in 0..channels {
            let values = image
                .get_slice(&[0, 0, 0, plane], &[nx, ny, npol, 1])
                .map_err(invalid)?;
            nonzero |= values.iter().any(|value| *value != 0.0);
            let frequency = self.frequencies[plane];
            let beam = beam
                .as_ref()
                .map(|(pattern, separation)| (pattern.at_frequency(frequency), separation));
            let images = (0..npol)
                .map(|pol| {
                    Array2::from_shape_fn((ny, nx), |(y, x)| {
                        let scale = beam.as_ref().map_or(1., |(pattern, separation)| {
                            let radius = separation[[y, x]].to_degrees() * 60.0 * frequency / 1e9;
                            pattern.power(radius)
                        });
                        values[[x, y, pol, 0]] * scale
                    })
                })
                .collect();
            let model = ModelImages {
                first_plane: plane as u32,
                planes: vec![ModelPlane { images }],
            };
            let grid = self
                .operator
                .prepare_model(&model, ModelPrescale::Unit)
                .map_err(invalid)?;
            for pol in 0..grid_pols {
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
        }
        output.save().map_err(invalid)?;
        Ok(nonzero)
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
        record.upsert(
            "spwchanselflag",
            Value::Array(ArrayValue::Int32(selection_mask(ms, selected)?)),
        );
        record_selection(&mut record, ms, selected, axis, self.frequencies.len())?;
        Ok(record)
    }
}

fn match_channels(axis: &SpectralCoordinate, channels: usize, frequencies: &[f64]) -> Vec<i32> {
    frequencies
        .iter()
        .map(|&frequency| {
            axis.pixel_at_frequency_hz(frequency)
                .ok()
                .map(f64::round)
                .filter(|&index| (0.0..channels as f64).contains(&index))
                .map_or(-1, |index| index as i32)
        })
        .collect()
}

fn record_selection(
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

fn selection_mask(ms: &MeasurementSet, selected: &ResolvedMsSelection) -> MsResult<ArrayD<i32>> {
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

fn first_correlations(ms: &MeasurementSet, selected: &ResolvedMsSelection) -> MsResult<Vec<i32>> {
    ms.polarization()?
        .corr_type(selected.selected_rows[0].polarization_id as usize)
}

fn coordinate_index(coordinates: &CoordinateSystem, kind: CoordinateType) -> MsResult<usize> {
    coordinates
        .find_coordinate(kind)
        .ok_or_else(|| invalid(format!("model image has no {kind:?} coordinate")))
}

fn grid_stokes(plane: &GridPolarization) -> MsResult<StokesType> {
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
