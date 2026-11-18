// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare image models with the existing gridder for CASA's legacy imager.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, ObsInfo, StokesCoordinate,
    StokesType,
};
use casa_images::PagedImage;
use casa_imaging_model::PolarizationCoordinate;
use casa_imaging_operator::{
    Basis, GridGeometry, GridPadding, GridPolarization, GridPrecision, ImageExtent,
    MeasurementOperator, ModelImages, ModelPlane, ModelPrescale, PolarizationRouting, Spheroidal,
};
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::virtual_model::ft_record::{FirstBuffer, GridFt, casa_spectral, legacy_multi_term};
use casa_ms::{
    MeasurementSet, MsResult, ResolvedMsSelection, VirtualModelEntry, VirtualModelKind,
    virtual_model_image_path,
};
use casa_types::{ArrayValue, Complex32, Correlation, RecordValue, Value};
use ndarray::{Array2, ArrayD, IxDyn, ShapeBuilder};

use super::{invalid, model_entry};

/// An uncommitted Fourier image. The scratch path uses it until the write
/// finishes. The virtual model can keep it once its on-disk record is filed.
pub(crate) struct ImageModel {
    pub(crate) entry: VirtualModelEntry,
    backing: GridLifetime,
}

impl ImageModel {
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
    let image = PagedImage::<f32>::open(path).map_err(invalid)?;
    let layout = ImageTransform::new(ms, selected, &image)?;
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
    let record = layout.record(ms, selected, &backing.path)?;
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
    increment: [f64; 2],
    phase_center: [f64; 2],
    frame: String,
}

impl ImageTransform {
    fn new(
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
        image: &PagedImage<f32>,
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
            .map(|code| polarization_coordinate(*code))
            .collect::<Vec<_>>();
        let routing = PolarizationRouting::compile(&correlations, &requested).map_err(invalid)?;
        let increment = [direction.increment()[0], direction.increment()[1]];
        let reference = direction.reference_pixel();
        let reference = [reference[0].round() as usize, reference[1].round() as usize];
        let padding = if reference == [nx / 2, ny / 2] && nx % 2 == 0 && ny % 2 == 0 {
            GridPadding::None
        } else {
            GridPadding::CasaComposite(2.0)
        };
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
            .to_world(&[reference[0] as f64, reference[1] as f64])
            .map_err(invalid)?;
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(
            direction
                .clone()
                .with_reference_pixel([gx as f64 / 2.0, gy as f64 / 2.0]),
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
            .collect::<MsResult<_>>()?;
        let basis = Basis::ChannelLocal {
            planes: channels as u32,
        };
        let kernels = Box::new(Spheroidal::new(&geometry, &routing));
        let operator =
            MeasurementOperator::new(geometry, basis, routing, kernels, GridPrecision::F64);
        Ok(Self {
            operator,
            coordinates,
            frequencies,
            increment,
            phase_center: [phase_center[0], phase_center[1]],
            frame: direction.direction_ref().as_str().to_owned(),
        })
    }

    fn grid(&self, image: &PagedImage<f32>, path: &Path) -> MsResult<bool> {
        let [nx, ny, npol, channels] = *image.shape() else {
            unreachable!()
        };
        let [gx, gy] = self.operator.geometry().grid_shape();
        let grid_pols = self.operator.polarization().grid_pols();
        let mut output = PagedImage::<Complex32>::create_with_tile_shape_and_cache(
            vec![gx, gy, grid_pols, channels],
            vec![gx, gy, 1, 1],
            self.coordinates.clone(),
            path,
            gx * gy * 8,
        )
        .map_err(invalid)?;
        let mut nonzero = false;
        for plane in 0..channels {
            let values = image
                .get_slice(&[0, 0, 0, plane], &[nx, ny, npol, 1])
                .map_err(invalid)?;
            nonzero |= values.iter().any(|value| *value != 0.0);
            let images = (0..npol)
                .map(|pol| Array2::from_shape_fn((ny, nx), |(y, x)| values[[x, y, pol, 0]]))
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
                let cells = grid.block::<f64>(0, pol, 0);
                let values = ArrayD::from_shape_fn(IxDyn(&[gx, gy, 1, 1]).f(), |index| {
                    let cell = cells[index[1] * gx + index[0]];
                    Complex32::new(cell.re as f32, cell.im as f32)
                });
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
    ) -> MsResult<RecordValue> {
        let engine = MsCalEngine::new(ms).map_err(invalid)?;
        let (lon, lat, radius) = engine.observatory_position().as_spherical();
        let row = &selected.selected_rows[0];
        let spectral = ms.spectral_window()?;
        let channels = spectral.chan_freq(row.spectral_window_id as usize)?;
        let routing = self.operator.polarization();
        let axis = coordinate_index(&self.coordinates, CoordinateType::Spectral)?;
        let CoordinateModel::Spectral(axis) = self.coordinates.coordinate(axis) else {
            unreachable!();
        };
        let channel_map = channels
            .iter()
            .map(|&frequency| {
                axis.pixel_at_frequency_hz(frequency)
                    .ok()
                    .map(f64::round)
                    .filter(|&index| (0.0..self.frequencies.len() as f64).contains(&index))
                    .map_or(-1, |index| index as i32)
            })
            .collect();
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
            observatory: [lon, lat, radius],
            spectral: spectral_record,
            buffer: &buffer,
            interpolation: 0,
            frequency_frame_valid: false,
        }
        .record();
        record.upsert(
            "spwchanselflag",
            Value::Array(ArrayValue::Int32(selection_mask(ms, selected)?)),
        );
        Ok(record)
    }
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
        for channel in channels.indices(count)? {
            mask[[0, window as usize, channel]] = 1;
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

pub(crate) fn polarization_coordinate(stokes: StokesType) -> PolarizationCoordinate {
    match stokes {
        StokesType::I => PolarizationCoordinate::StokesI,
        StokesType::Q => PolarizationCoordinate::StokesQ,
        StokesType::U => PolarizationCoordinate::StokesU,
        StokesType::V => PolarizationCoordinate::StokesV,
        StokesType::XX => PolarizationCoordinate::LinearXx,
        StokesType::XY => PolarizationCoordinate::LinearXy,
        StokesType::YX => PolarizationCoordinate::LinearYx,
        StokesType::YY => PolarizationCoordinate::LinearYy,
        StokesType::RR => PolarizationCoordinate::CircularRr,
        StokesType::RL => PolarizationCoordinate::CircularRl,
        StokesType::LR => PolarizationCoordinate::CircularLr,
        StokesType::LL => PolarizationCoordinate::CircularLl,
    }
}
