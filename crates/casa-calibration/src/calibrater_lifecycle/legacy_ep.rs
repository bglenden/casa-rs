// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare the image and convolution owned by the legacy EP solver.

mod degrid;
mod fit;
mod solve;
mod table;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, StokesCoordinate, StokesType,
};
use casa_core::Complex32;
use casa_core::quanta::{Quantity, Unit};
use casa_images::PagedImage;
use casa_imaging_model::EvlaDishSurface;
use casa_imaging_operator::{
    EvlaApertureModel, LegacyEpConvolution, LegacyEpWPlanePlan, NativeCacheLock,
};
use casa_imaging_runtime::{Demand, HostResources, Reservation, ResourcePolicy, admit};
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::{MeasurementSet, MsResult, ResolvedMsSelection, ResolvedMsSelectionRow};
use casa_tables::RequiredScalarColumnValues;

use super::ep_cache::{EpCache, EpCacheProducts, invalid};
use crate::{AttachmentError, CalibrationAttachment, CalibrationOutputMode, RefAntSelector};

pub(super) struct LegacyEpSolver {
    table: PathBuf,
    cache: EpCache,
    angle_increment_deg: f64,
    pub(super) interval_seconds: f64,
    preaverage_seconds: f64,
    pub(super) mode: CalibrationOutputMode,
    pub(super) reference: RefAntSelector,
    model: Option<PreparedEpModel>,
}

struct PreparedEpModel {
    fourier: [Vec<Complex32>; 4],
    products: Arc<EpCacheProducts>,
    correlation_planes: Vec<StokesType>,
    side: usize,
    cell_rad: [f64; 2],
    scale: [f64; 2],
    reference_frequency_hz: f64,
    initial_angle_rad: f64,
    coordinates: CoordinateSystem,
    _memory: Reservation,
}

pub(crate) struct FirstBuffer {
    pub(crate) correlations: Vec<StokesType>,
    frequency_hz: f64,
    pub(crate) angle_rad: f64,
}

impl LegacyEpSolver {
    pub(super) fn output_path(&self) -> &Path {
        &self.table
    }

    pub(super) fn new(
        table: &Path,
        cache: &Path,
        angle_increment_deg: f64,
        (interval, preaverage_seconds): (&str, f64),
        mode: CalibrationOutputMode,
        reference: RefAntSelector,
    ) -> MsResult<(Self, String)> {
        if !angle_increment_deg.is_finite() {
            return Err(invalid("EP painc must be a finite angle"));
        }
        let cache = EpCache::open(cache)?;
        let interval_quantity = Quantity::read(interval).map_err(invalid)?;
        let (interval_seconds, interval_label) = interval_quantity
            .get_value_in(&Unit::new("s").expect("known unit"))
            .map_or_else(
                |_| (interval_quantity.value(), format!("{interval}s")),
                |seconds| (seconds, interval.into()),
            );
        if !interval_seconds.is_finite() {
            return Err(invalid("EP solint must be finite"));
        }
        let solver = Self {
            table: table.to_owned(),
            cache,
            angle_increment_deg,
            interval_seconds,
            preaverage_seconds: if preaverage_seconds < 0. {
                if interval_seconds > 0. {
                    interval_seconds
                } else {
                    f64::MAX
                }
            } else {
                preaverage_seconds
            },
            mode,
            reference,
            model: None,
        };
        Ok((solver, interval_label))
    }

    pub(super) fn set_model(
        &mut self,
        attachment: &CalibrationAttachment,
        path: &Path,
    ) -> Result<(), AttachmentError> {
        if !path.is_dir() {
            return Err(invalid(format!("Table {} does not exist", path.display())).into());
        }
        let image = PagedImage::<f32>::open(path).map_err(invalid)?;
        let (shape, cell_rad, coordinates) = model_geometry(&image)?;
        let host = HostResources::detect().map_err(invalid)?;
        let memory = reserve_model(&host, shape[0])?;
        let pixels = image.get().map_err(invalid)?;
        if pixels.iter().any(|value| !value.is_finite()) {
            return Err(invalid("EP model contains non-finite IQUV values").into());
        }
        let buffer = attachment.with_write(|ms, selection, _| Ok(first_buffer(ms, selection)?))?;
        match buffer.correlations.as_slice() {
            [StokesType::RR | StokesType::LL]
            | [StokesType::RR, StokesType::LL]
            | [StokesType::LL, StokesType::RR] => {}
            _ => return Err(invalid("legacy EP needs circular parallel-hand data").into()),
        }
        let products = convolution(
            &self.cache,
            (self.angle_increment_deg, 1),
            &host,
            &buffer,
            shape,
            cell_rad,
            &coordinates,
        )?;
        let fourier =
            LegacyEpConvolution::prepare_model(&pixels, &products.average_beam, products.sampling)
                .map_err(invalid)?;
        tracing::debug!(table = %self.table.display(), support = products.support[0],
            convolution_samples = products.convolution.len(), weight_samples = products.weights.len(),
            "legacy EP prepared its Fourier model");
        self.model = Some(PreparedEpModel {
            fourier,
            products: Arc::new(products),
            correlation_planes: buffer.correlations,
            side: shape[0],
            cell_rad,
            scale: cell_rad.map(|increment| increment * shape[0] as f64),
            reference_frequency_hz: buffer.frequency_hz,
            initial_angle_rad: buffer.angle_rad,
            coordinates,
            _memory: memory,
        });
        Ok(())
    }
}

pub(crate) fn convolution(
    cache: &EpCache,
    (angle_increment_deg, w_planes): (f64, usize),
    host: &HostResources,
    buffer: &FirstBuffer,
    shape: [usize; 4],
    cell_rad: [f64; 2],
    beam_coordinates: &CoordinateSystem,
) -> MsResult<EpCacheProducts> {
    let read = || {
        cache.read(
            host,
            buffer.angle_rad,
            angle_increment_deg,
            &shape,
            &buffer.correlations,
            w_planes,
        )
    };
    {
        let _lock = NativeCacheLock::shared(cache.path()).map_err(invalid)?;
        if let Some(products) = read()? {
            return Ok(products);
        }
    }
    let _lock = NativeCacheLock::exclusive(cache.path()).map_err(invalid)?;
    cache.recover()?;
    if let Some(products) = read()? {
        return Ok(products);
    }
    let screen_memory = admit(
        host,
        &ResourcePolicy::Balanced,
        &Demand {
            phase: "legacy EP convolution screen",
            memory: LegacyEpConvolution::SCREEN_WORKSPACE_BYTES,
        },
    )
    .map_err(invalid)?;
    let aperture = EvlaApertureModel::new(legacy_vla_surface()?);
    let (products, kernel_memory, w_scale) = if w_planes == 1 {
        let products = LegacyEpConvolution::vla_l_band(
            &aperture,
            shape[0],
            cell_rad[0],
            buffer.frequency_hz,
            buffer.angle_rad,
        )
        .map_err(invalid)?;
        (products, None, None)
    } else {
        let plan = LegacyEpWPlanePlan::new(
            &aperture,
            shape[0],
            cell_rad,
            buffer.frequency_hz,
            buffer.angle_rad,
            w_planes,
        )
        .map_err(invalid)?;
        let memory = admit(
            host,
            &ResourcePolicy::Balanced,
            &Demand {
                phase: "legacy EP cold convolution cells",
                memory: plan.retained_kernel_bytes(),
            },
        )
        .map_err(invalid)?;
        let w_scale = plan.w_scale();
        (plan.build().map_err(invalid)?, Some(memory), Some(w_scale))
    };
    cache.publish(
        host,
        (buffer.angle_rad, cell_rad),
        beam_coordinates,
        shape,
        &buffer.correlations,
        &products,
    )?;
    drop((products, kernel_memory, screen_memory));
    // The persisted angle is single precision and written as decimal.
    let mut products = cache
        .read(
            host,
            buffer.angle_rad,
            angle_increment_deg.max(0.001),
            &shape,
            &buffer.correlations,
            w_planes,
        )?
        .ok_or_else(|| invalid("legacy EP generated an unusable convolution cache"))?;
    products.w_scale = w_scale;
    Ok(products)
}

pub(crate) fn reserve_model(host: &HostResources, side: usize) -> MsResult<Reservation> {
    // Model and average-beam pixels, four retained Fourier grids, and
    // the image-sized Jones, power and FFT workspace of the average beam.
    let memory = (side as u64)
        .saturating_mul(side as u64)
        .saturating_mul(128)
        .saturating_add(4 << 20);
    admit(
        host,
        &ResourcePolicy::Balanced,
        &Demand {
            phase: "legacy EP model",
            memory,
        },
    )
    .map_err(invalid)
}

pub(crate) fn model_geometry(
    image: &PagedImage<f32>,
) -> MsResult<([usize; 4], [f64; 2], CoordinateSystem)> {
    let [side, ny, 4, 1] = *image.shape() else {
        return Err(invalid("legacy EP needs a single-channel IQUV image"));
    };
    if side != ny || side < 4 || !side.is_multiple_of(2) {
        return Err(invalid(
            "legacy EP needs a square image with even pixel lengths",
        ));
    }
    let mut coordinates = image.coordinates().clone();
    if coordinates.n_coordinates() != 3 {
        return Err(invalid(
            "legacy EP needs direction, IQUV and frequency coordinates",
        ));
    }
    let CoordinateModel::Direction(direction) = coordinates.coordinate(0) else {
        return Err(invalid("legacy EP model is missing direction axes"));
    };
    let cell_rad = direction
        .increment()
        .try_into()
        .map_err(|_| invalid("EP direction coordinate must have two increments"))?;
    let [dx, dy]: [f64; 2] = cell_rad;
    if !dx.is_finite() || !dy.is_finite() || dx == 0. || (dx.abs() - dy.abs()).abs() > 1e-15 {
        return Err(invalid("legacy EP needs square angular pixels"));
    }
    let circular = [
        StokesType::RR,
        StokesType::RL,
        StokesType::LR,
        StokesType::LL,
    ];
    if !matches!(coordinates.coordinate(1), CoordinateModel::Stokes(stokes)
            if stokes.stokes() == [StokesType::I, StokesType::Q, StokesType::U, StokesType::V])
        || coordinates.find_coordinate(CoordinateType::Spectral) != Some(2)
    {
        return Err(invalid(
            "legacy EP needs direction, IQUV and frequency coordinates",
        ));
    }
    coordinates.replace_coordinate(1, StokesCoordinate::new(circular.to_vec()));
    Ok(([side, ny, 4, 1], cell_rad, coordinates))
}

pub(crate) fn first_buffer(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
) -> MsResult<FirstBuffer> {
    let telescope = ms.observation()?.string(0, "TELESCOPE_NAME")?;
    if telescope != "VLA" {
        return Err(invalid(
            "legacy EP aperture model is implemented for the (E)VLA only",
        ));
    }
    let row = first_cross_row(ms, selected)?;
    let codes = ms.polarization()?.corr_type(row.polarization_id as usize)?;
    let circular = [
        StokesType::RR,
        StokesType::RL,
        StokesType::LR,
        StokesType::LL,
    ];
    let correlations = codes
        .iter()
        .map(|&code| {
            circular
                .iter()
                .find(|value| **value as i32 == code)
                .copied()
                .ok_or_else(|| invalid("legacy EP requires circular correlations"))
        })
        .collect::<MsResult<Vec<_>>>()?;
    if correlations.is_empty() {
        return Err(invalid("legacy EP first buffer has no correlations"));
    }
    let spw = row.spectral_window_id;
    let frequencies = ms.spectral_window()?.chan_freq(spw as usize)?;
    let channels = match selected.channel_selections.get(&spw) {
        Some(selection) => selection.indices(frequencies.len())?,
        None => (0..frequencies.len()).collect(),
    };
    let frequency_hz = channels
        .into_iter()
        .map(|channel| frequencies[channel])
        .fold(0_f64, f64::max);
    if !(1.0e9..=2.0e9).contains(&frequency_hz) {
        return Err(invalid(
            "legacy EP aperture model requires VLA L-band observations",
        ));
    }
    let engine = MsCalEngine::new(ms)?;
    let angle_rad =
        f64::from(
            engine.feed_parallactic_angle(row.time_mjd_seconds, row.field_id as usize, 0)? as f32,
        );
    Ok(FirstBuffer {
        correlations,
        frequency_hz,
        angle_rad,
    })
}

fn first_cross_row<'a>(
    ms: &MeasurementSet,
    selected: &'a ResolvedMsSelection,
) -> MsResult<&'a ResolvedMsSelectionRow> {
    let mut first = None;
    for rows in selected.selected_rows.chunks(8192) {
        let indices = rows.iter().map(|row| row.row_index).collect::<Vec<_>>();
        let columns = ms
            .main_table()
            .required_scalar_columns_owned_for_rows(&["ARRAY_ID"], &indices)?;
        let Some(RequiredScalarColumnValues::Int32(arrays)) = columns.get("ARRAY_ID") else {
            return Err(invalid("legacy EP requires ARRAY_ID"));
        };
        for (row, &array) in rows.iter().zip(arrays) {
            let time = row.time_mjd_seconds;
            let key = (array, row.field_id, row.data_desc_id, time.to_bits());
            if first.as_ref().is_none_or(|(old, _)| key < *old) {
                first = Some((key, None));
            }
            if let Some((held, cross)) = first.as_mut()
                && key == *held
                && cross.is_none()
                && !row.flag_row
                && row.antenna1_id != row.antenna2_id
            {
                *cross = Some(row);
            }
        }
    }
    first.and_then(|(_, cross)| cross).ok_or_else(|| {
        invalid("legacy EP needs an unflagged cross-correlation in its first visibility buffer")
    })
}

pub(crate) fn legacy_vla_surface() -> MsResult<EvlaDishSurface> {
    use std::io::Read;
    let relative = "nrao/VLA/VLA.surface";
    let path = std::env::var_os("CASARS_EP_VLA_SURFACE")
        .map(PathBuf::from)
        .or_else(|| {
            [
                std::env::var_os("CASA_RS_MEASURESPATH").map(PathBuf::from),
                std::env::var_os("HOME").map(|root| PathBuf::from(root).join(".casa/data")),
            ]
            .into_iter()
            .flatten()
            .map(|root| root.join(relative))
            .find(|path| path.is_file())
        })
        .ok_or_else(|| {
            invalid("legacy EP needs nrao/VLA/VLA.surface from the CASA runtime data")
        })?;
    let mut text = String::new();
    std::fs::File::open(path)
        .and_then(|file| file.take(1_048_577).read_to_string(&mut text))
        .map_err(invalid)?;
    if text.len() > 1_048_576 {
        return Err(invalid("legacy EP aperture surface exceeds 1 MiB"));
    }
    EvlaDishSurface::from_surface_text(&text).map_err(invalid)
}

#[cfg(test)]
mod tests {
    use casa_core::ArrayValue;

    use crate::CalibraterLifecycle;
    use crate::calibrater_lifecycle::ConfiguredSolve;
    use crate::session::resolve_casa_calibration_selection;

    use super::*;

    #[test]
    fn legacy_ep_model_memory_is_admitted_before_materializing_pixels() {
        let host = HostResources {
            threads: 2,
            performance_cores: 2,
            available_memory: 1 << 30,
            metal: false,
        };
        assert!(reserve_model(&host, 256).is_ok());
        let error = reserve_model(&host, 16_384).unwrap_err();
        assert!(error.to_string().contains("legacy EP model needs"));
    }

    #[test]
    #[ignore = "requires the pinned CASA legacy PB/W first-buffer MeasurementSet and model"]
    fn legacy_ep_retains_the_casa_first_buffer_model() {
        let dataset = PathBuf::from(std::env::var_os("CASARS_EP_FIRST_BUFFER_MS").unwrap());
        let image = PathBuf::from(std::env::var_os("CASARS_EP_MODEL").unwrap());
        let scratch = tempfile::tempdir().unwrap();
        let caches = [
            ("cold", scratch.path().join("cache")),
            (
                "CASA warm",
                PathBuf::from(std::env::var_os("CASARS_EP_REFERENCE_CACHE").unwrap()),
            ),
            (
                "Rust warm",
                PathBuf::from(std::env::var_os("CASARS_EP_NATIVE_CACHE").unwrap()),
            ),
        ];
        let mut tool = CalibraterLifecycle::default();
        assert!(tool.set_legacy_iterator(true));
        tool.open(&dataset).unwrap();
        for (label, cache) in caches {
            tool.set_ep_solve(&scratch.path().join("fit.EP"), &cache, 360.)
                .unwrap();
            tool.set_image_model(&image).unwrap();
            let equation = tool.equation().unwrap();
            let Some(ConfiguredSolve::LegacyEp(solver)) = &equation.solve else {
                panic!("EP solver is configured");
            };
            compare_retained(
                label,
                solver.model.as_ref().unwrap(),
                &dataset,
                &image,
                equation.attachment.selection(),
            );
        }
    }

    fn compare_retained(
        label: &str,
        model: &PreparedEpModel,
        dataset: &Path,
        image: &Path,
        selection: &casa_ms::MsSelection,
    ) {
        let stored = PagedImage::<f32>::open(image).unwrap();
        let (shape, increment, _) = model_geometry(&stored).unwrap();
        let ms = MeasurementSet::open(dataset).unwrap();
        let selected = resolve_casa_calibration_selection(&ms, selection).unwrap();
        let first = first_cross_row(&ms, &selected).unwrap();
        let channels = ms
            .spectral_window()
            .unwrap()
            .chan_freq(first.spectral_window_id as usize)
            .unwrap();
        let rows = selected
            .selected_rows
            .iter()
            .filter(|row| {
                row.field_id == first.field_id
                    && row.data_desc_id == first.data_desc_id
                    && row.time_mjd_seconds == first.time_mjd_seconds
                    && !row.flag_row
                    && row.antenna1_id != row.antenna2_id
            })
            .map(|row| row.row_index)
            .collect::<Vec<_>>();
        let values = |column| {
            ms.main_table()
                .column_accessor(column)
                .unwrap()
                .array_cells_owned_uncached(&rows)
                .unwrap()
        };
        let mut norm = 0.;
        let mut difference = 0.;
        let mut samples = 0;
        let mut imaginary = 0_f32;
        for (uvw, stored) in values("UVW").into_iter().zip(values("MODEL_DATA")) {
            let Some(ArrayValue::Float64(uvw)) = uvw else {
                panic!("UVW")
            };
            let Some(ArrayValue::Complex32(expected)) = stored else {
                panic!("MODEL_DATA")
            };
            let [u, v, _] = uvw.iter().copied().collect::<Vec<_>>().try_into().unwrap();
            let wavelength = 299_792_458. / channels[0];
            let position = [u, v].map(|value| -value / wavelength);
            for channel in 1..channels.len() {
                assert!(
                    expected
                        .index_axis(ndarray::Axis(1), channel)
                        .iter()
                        .all(|v| v.norm() == 0.)
                );
            }
            for (correlation, polarization) in model.correlation_planes.iter().enumerate() {
                let actual = evaluate_sample(
                    model,
                    shape[0],
                    correlation,
                    *polarization,
                    [position[0] * increment[0], position[1] * increment[1]],
                );
                let expected = expected[[correlation, 0]];
                difference += f64::from((actual - expected).norm_sqr());
                norm += f64::from(expected.norm_sqr());
                imaginary = imaginary.max(expected.im.abs());
                samples += 1;
            }
        }
        assert!(samples > 600 && imaginary > 0.1);
        let error = (difference / norm).sqrt();
        eprintln!(
            "legacy EP {label}: first buffer {samples} complex samples, normalized RMS {error:.9e}"
        );
        assert!(error < 1e-3, "{error}");
    }

    fn evaluate_sample(
        model: &PreparedEpModel,
        side: usize,
        correlation: usize,
        polarization: StokesType,
        position: [f64; 2],
    ) -> Complex32 {
        let centre = side / 2 + 1;
        let position = position.map(|value| (value * side as f64 + centre as f64) as f32);
        let loc = position.map(|value| value.round() as isize - 1);
        let products = &model.products;
        let sampling = products.sampling as isize;
        let subpixel = std::array::from_fn::<_, 2, _>(|axis| {
            (((loc[axis] + 1) as f32 - position[axis]) * sampling as f32).round() as isize
        });
        let support = products.support[0] as isize;
        if loc
            .iter()
            .any(|&value| value < support || value + support >= side as isize)
        {
            return Complex32::default();
        }
        let first = products.convolution.shape()[0] as isize / 2;
        let grid = &model.fourier[polarization as usize - StokesType::RR as usize];
        let mut value = Complex32::default();
        let mut norm = Complex32::default();
        for y in -support..=support {
            for x in -support..=support {
                let kx = (first + x * sampling + subpixel[0]) as usize;
                let ky = (first + y * sampling + subpixel[1]) as usize;
                let weight = products.convolution[[kx, ky, correlation, 0]];
                value += weight * grid[(loc[0] + x) as usize + side * (loc[1] + y) as usize];
                norm += weight;
            }
        }
        value / norm
    }
}
