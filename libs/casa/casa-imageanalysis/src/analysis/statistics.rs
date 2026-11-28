// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[cfg(feature = "imstat-test-hooks")]
use crate::imstat_test_hooks::{Checkpoint, observe};

pub(super) fn imstat_typed<T>(
    path: &Path,
    image: &PagedImage<T>,
    request: &ImstatRequest,
    cancellation: Option<&Cancellation>,
) -> Result<ImstatOutcome, ImageError>
where
    T: ImagePixel + ImageExprValue + PartialOrd + ExprValueConvert + StatsElement,
{
    check_imstat_cancelled(cancellation)?;
    let algorithm = parse_imstat_algorithm(request).map_err(ImageError::InvalidMetadata)?;
    let axes =
        parse_axes(request.axes.as_deref(), image.ndim()).map_err(ImageError::CasaSelection)?;
    let region_selection = region_pixels(image, request.region.as_deref())?;
    if request
        .box_pixels
        .as_deref()
        .is_some_and(|text| !text.trim().is_empty())
        && region_selection.is_some()
    {
        return Err(ImageError::InvalidMetadata(
            "imstat accepts either box or region, not both".to_string(),
        ));
    }
    let explicit_box = match request
        .box_pixels
        .as_deref()
        .filter(|text| !text.trim().is_empty())
    {
        Some(box_text) => Some(parse_box_region(
            box_text,
            image.shape(),
            direction_plane_axes(image)?,
        )?),
        None => None,
    };
    let effective_region = explicit_box.as_ref().or(region_selection.as_ref());
    let selection = Selection::new(
        image,
        effective_region,
        request.chans.as_deref(),
        request.stokes.as_deref(),
    )?;
    check_imstat_cancelled(cancellation)?;
    let pixel_mask = selection_pixel_mask(
        image,
        effective_region,
        &selection,
        request.mask.as_deref(),
        request.stretch,
    )?;
    let coordinates = image.coordinates();
    let world_text = |position: &[usize]| -> Result<String, ImageError> {
        let pixel: Vec<f64> = position.iter().map(|&value| value as f64).collect();
        Ok(casa_coordinates::format_world_display(coordinates, &pixel)?)
    };
    let blc = selection.start.clone();
    let trc = trc_pixels(&selection.start, &selection.shape);
    let blcf = world_text(&blc)?;
    let trcf = world_text(&trc)?;
    check_imstat_cancelled(cancellation)?;

    let display_axes: Vec<usize> = match &axes {
        AxisSelection::All => Vec::new(),
        AxisSelection::Collapsed(collapse) => (0..image.ndim())
            .filter(|axis| !collapse.contains(axis))
            .collect(),
    };
    let outcome = if display_axes.is_empty() {
        let summary = imstat_global_summary(
            path,
            image,
            request,
            &algorithm,
            &selection,
            pixel_mask.as_ref(),
            blc,
            trc,
            blcf,
            trcf,
            &world_text,
            cancellation,
        )?;
        ImstatOutcome::Global(summary)
    } else {
        let summary = imstat_planes_summary(
            path,
            image,
            request,
            &algorithm,
            &selection,
            pixel_mask.as_ref(),
            &display_axes,
            blc,
            trc,
            blcf,
            trcf,
            cancellation,
        )?;
        ImstatOutcome::Planes(summary)
    };
    Ok(outcome)
}

/// The whole selection's statistics, as `imstat` reports them with every
/// axis collapsed.
#[allow(clippy::too_many_arguments)]
pub(super) fn imstat_global_summary<T>(
    path: &Path,
    image: &PagedImage<T>,
    request: &ImstatRequest,
    algorithm: &StatisticsAlgorithm,
    selection: &Selection,
    pixel_mask: Option<&ndarray::ArrayD<bool>>,
    blc: Vec<usize>,
    trc: Vec<usize>,
    blcf: String,
    trcf: String,
    world_text: &dyn Fn(&[usize]) -> Result<String, ImageError>,
    cancellation: Option<&Cancellation>,
) -> Result<ImageStatisticsSummary, ImageError>
where
    T: ImagePixel + StatsElement,
{
    // The statistics the algorithm reports, with the extremum positions
    // relative to the original image.
    let computed = match algorithm {
        StatisticsAlgorithm::Classic
            if !selection.stokes_has_gaps() && !selection.channels_have_gaps() =>
        {
            // The classical moments stream through the lattice
            // statistics; a gapped Stokes set needs the sample route.
            let sub = image.sub_image(selection.start.clone(), selection.shape.clone())?;
            let mut stats = LatticeStatistics::new(&sub);
            stats.set_cancellation(cancellation);
            if let Some([min, max]) = request.includepix {
                stats.set_include_range(min, max);
            }
            if let Some(mask) = pixel_mask.cloned() {
                stats.set_pixel_mask(mask);
            }
            let (minpos, maxpos) = stats.get_min_max_pos()?;
            ComputedStats {
                npts: scalar_stat(&stats, Statistic::Npts)?,
                sum: scalar_stat(&stats, Statistic::Sum)?,
                min: scalar_stat(&stats, Statistic::Min)?,
                max: scalar_stat(&stats, Statistic::Max)?,
                sumsq: scalar_stat(&stats, Statistic::SumSq)?,
                mean: scalar_stat(&stats, Statistic::Mean)?,
                rms: scalar_stat(&stats, Statistic::Rms)?,
                sigma: scalar_stat(&stats, Statistic::Sigma)?,
                median: scalar_stat(&stats, Statistic::Median)?,
                medabsdevmed: scalar_stat(&stats, Statistic::MedAbsDevMed)?,
                q1: scalar_stat(&stats, Statistic::Q1)?,
                q3: scalar_stat(&stats, Statistic::Q3)?,
                quartile: scalar_stat(&stats, Statistic::Quartile)?,
                minpos: minpos.map(|pos| offset_pos(&selection.start, pos)),
                maxpos: maxpos.map(|pos| offset_pos(&selection.start, pos)),
            }
        }
        robust => {
            let pixels = image.get_slice(&selection.start, &selection.shape)?;
            let sample = sample_points(
                &pixels,
                pixel_mask,
                request.includepix,
                selection.channel_axis,
                selection.channel_indices.as_deref(),
                selection.stokes_axis,
                selection.stokes_indices.as_deref(),
                &selection.start,
                cancellation,
            )?;
            match compute_robust_stats_with_cancellation(&sample, robust, cancellation)? {
                RobustStats::Sample(stats) => ComputedStats {
                    npts: stats.npts,
                    sum: stats.sum,
                    min: stats.min,
                    max: stats.max,
                    sumsq: stats.sumsq,
                    mean: stats.mean,
                    rms: stats.rms,
                    sigma: stats.sigma,
                    median: stats.median,
                    medabsdevmed: stats.medabsdevmed,
                    q1: stats.q1,
                    q3: stats.q3,
                    quartile: stats.quartile,
                    minpos: stats.minpos.map(|linear| {
                        offset_pos(
                            &selection.start,
                            selection_position(linear, &selection.shape),
                        )
                    }),
                    maxpos: stats.maxpos.map(|linear| {
                        offset_pos(
                            &selection.start,
                            selection_position(linear, &selection.shape),
                        )
                    }),
                },
                RobustStats::Biweight { location, scale } => {
                    // The biweight reports only its location and scale,
                    // with the sample's count and extrema.
                    let npts = sample.len() as f64;
                    let (min, max) = sample_extrema(&sample, cancellation)?;
                    ComputedStats {
                        npts,
                        sum: f64::NAN,
                        min,
                        max,
                        sumsq: f64::NAN,
                        mean: location,
                        rms: f64::NAN,
                        sigma: scale,
                        median: f64::NAN,
                        medabsdevmed: f64::NAN,
                        q1: f64::NAN,
                        q3: f64::NAN,
                        quartile: f64::NAN,
                        minpos: None,
                        maxpos: None,
                    }
                }
            }
        }
    };

    let minposf = match &computed.minpos {
        Some(position) => Some(world_text(position)?),
        None => None,
    };
    let maxposf = match &computed.maxpos {
        Some(position) => Some(world_text(position)?),
        None => None,
    };
    let flux = if algorithm.kind() == StatisticsAlgorithmKind::Biweight {
        None
    } else {
        image_flux(image, computed.sum, selection)
    };
    Ok(ImageStatisticsSummary {
        blcf,
        trcf,
        npts: computed.npts,
        min: computed.min,
        max: computed.max,
        sum: computed.sum,
        sumsq: computed.sumsq,
        mean: computed.mean,
        rms: computed.rms,
        sigma: computed.sigma,
        median: computed.median,
        medabsdevmed: computed.medabsdevmed,
        q1: computed.q1,
        q3: computed.q3,
        quartile: computed.quartile,
        minposf,
        maxposf,
        minpos: computed.minpos,
        maxpos: computed.maxpos,
        imagename: path.display().to_string(),
        blc,
        trc,
        flux,
        units: image.units().to_string(),
    })
}

/// Each display plane's statistics, as `imstat` reports them with only
/// some axes collapsed: one value per display position, in display
/// order, with CASA's degenerate display axes dropped from the record's
/// shape.
#[allow(clippy::too_many_arguments)]
pub(super) fn imstat_planes_summary<T>(
    path: &Path,
    image: &PagedImage<T>,
    request: &ImstatRequest,
    algorithm: &StatisticsAlgorithm,
    selection: &Selection,
    pixel_mask: Option<&ndarray::ArrayD<bool>>,
    display_axes: &[usize],
    blc: Vec<usize>,
    trc: Vec<usize>,
    blcf: String,
    trcf: String,
    cancellation: Option<&Cancellation>,
) -> Result<ImstatPlanesSummary, ImageError>
where
    T: ImagePixel + StatsElement,
{
    let display_positions: Vec<Vec<usize>> = display_axes
        .iter()
        .map(|&axis| selection.axis_positions(axis))
        .collect();
    let display_counts: Vec<usize> = display_positions.iter().map(|p| p.len()).collect();
    let display_shape: Vec<usize> = display_counts
        .iter()
        .copied()
        .filter(|&count| count > 1)
        .collect();
    let plane_count: usize = display_counts.iter().product();

    // One pass over the selection's pixels, bucketed by display
    // position: each pixel's plane is its display coordinates' index.
    let pixels = image.get_slice(&selection.start, &selection.shape)?;
    check_imstat_cancelled(cancellation)?;
    let mut buckets: Vec<Vec<SamplePoint>> = vec![Vec::new(); plane_count];
    let stokes_collapsed = selection
        .stokes_axis
        .is_some_and(|axis| display_axes.iter().all(|&display| display != axis));
    let mut strides = Vec::with_capacity(pixels.shape().len());
    let mut stride = 1u64;
    for &axis_length in pixels.shape() {
        strides.push(stride);
        stride *= axis_length as u64;
    }
    for (visited, (index, value)) in pixels.indexed_iter().enumerate() {
        if visited.is_multiple_of(4096) {
            check_imstat_cancelled(cancellation)?;
        }
        if let Some(mask) = pixel_mask {
            let mask_index: &[usize] = index.slice();
            if !mask[mask_index] {
                continue;
            }
        }
        let number = value.to_f64_stats();
        if let Some([min, max]) = request.includepix
            && !(min <= number && number <= max)
        {
            continue;
        }
        let mut linear = 0u64;
        for (axis, &axis_stride) in strides.iter().enumerate() {
            linear += index[axis] as u64 * axis_stride;
        }
        let mut bucket = 0usize;
        // The display positions run in Fortran order, the order casacore
        // keeps array data in and casa-rs's records carry.
        let mut slot_stride = 1usize;
        let mut in_selection = true;
        for (slot, &axis) in display_axes.iter().enumerate() {
            let absolute = index[axis] + selection.start[axis];
            let Some(slot_index) = display_positions[slot]
                .iter()
                .position(|&position| position == absolute)
            else {
                in_selection = false;
                break;
            };
            bucket += slot_index * slot_stride;
            slot_stride *= display_counts[slot];
        }
        if !in_selection {
            continue;
        }
        if stokes_collapsed {
            let axis = selection.stokes_axis.expect("the axis was found above");
            let kept = selection.stokes_indices.as_deref().unwrap_or(&[]);
            let absolute = index[axis] + selection.start[axis];
            if !kept.contains(&absolute) {
                continue;
            }
        }
        buckets[bucket].push(SamplePoint {
            value: number,
            position: linear,
        });
    }

    let biweight = algorithm.kind() == StatisticsAlgorithmKind::Biweight;
    let spectral_collapsed = image
        .find_axis(CoordinateType::Spectral)
        .is_some_and(|axis| display_axes.iter().all(|&display| display != axis));
    // CASA's flux density exists per plane only when the direction plane
    // is at least partly collapsed, as casacore beam-integrates the sky
    // it sums: a plane that still varies over the whole sky has none.
    let [plane_x, plane_y] = selection.plane;
    let flux_over_sky = !display_axes.contains(&plane_x) || !display_axes.contains(&plane_y);
    let mut planes = ImstatPlanesSummary {
        imagename: path.display().to_string(),
        blc,
        trc,
        blcf,
        trcf,
        display_shape,
        plane_log: String::new(),
        npts: Vec::with_capacity(plane_count),
        min: Vec::with_capacity(plane_count),
        max: Vec::with_capacity(plane_count),
        sum: Vec::with_capacity(plane_count),
        sumsq: Vec::with_capacity(plane_count),
        mean: Vec::with_capacity(plane_count),
        rms: Vec::with_capacity(plane_count),
        sigma: Vec::with_capacity(plane_count),
        median: Vec::with_capacity(plane_count),
        medabsdevmed: Vec::with_capacity(plane_count),
        q1: Vec::with_capacity(plane_count),
        q3: Vec::with_capacity(plane_count),
        quartile: Vec::with_capacity(plane_count),
        flux: None,
        biweight,
    };
    let mut iterations = Vec::new();
    for mut sample in buckets {
        check_imstat_cancelled(cancellation)?;
        #[cfg(feature = "imstat-test-hooks")]
        observe(Checkpoint::PlaneStarted);
        sample.sort_by_key(|point| point.position);
        match compute_robust_stats_with_cancellation(&sample, algorithm, cancellation)? {
            RobustStats::Sample(stats) => {
                iterations.push(stats.chauvenet_iterations);
                planes.npts.push(stats.npts);
                planes.min.push(stats.min);
                planes.max.push(stats.max);
                planes.sum.push(stats.sum);
                planes.sumsq.push(stats.sumsq);
                planes.mean.push(stats.mean);
                planes.rms.push(stats.rms);
                planes.sigma.push(stats.sigma);
                planes.median.push(stats.median);
                planes.medabsdevmed.push(stats.medabsdevmed);
                planes.q1.push(stats.q1);
                planes.q3.push(stats.q3);
                planes.quartile.push(stats.quartile);
                let flux = if flux_over_sky {
                    image_flux_with_integration(image, stats.sum, selection, spectral_collapsed)
                } else {
                    None
                };
                if let Some(flux) = flux {
                    planes
                        .flux
                        .get_or_insert_with(|| Vec::with_capacity(plane_count))
                        .push(flux);
                }
            }
            RobustStats::Biweight { location, scale } => {
                let count = sample.len() as f64;
                let (min, max) = sample_extrema(&sample, cancellation)?;
                planes.npts.push(count);
                planes.min.push(min);
                planes.max.push(max);
                planes.sum.push(f64::NAN);
                planes.sumsq.push(f64::NAN);
                planes.mean.push(location);
                planes.rms.push(f64::NAN);
                planes.sigma.push(scale);
                planes.median.push(f64::NAN);
                planes.medabsdevmed.push(f64::NAN);
                planes.q1.push(f64::NAN);
                planes.q3.push(f64::NAN);
                planes.quartile.push(f64::NAN);
            }
        }
        #[cfg(feature = "imstat-test-hooks")]
        observe(Checkpoint::PlaneFinished);
    }
    if biweight {
        planes.flux = None;
    }
    if request.listit
        || request
            .logfile
            .as_deref()
            .is_some_and(|name| !name.is_empty())
    {
        planes.plane_log = imstat_log::plane_table(
            image,
            &planes,
            display_axes,
            &display_positions,
            &iterations,
            cancellation,
        )?;
    }
    Ok(planes)
}

fn sample_extrema(
    sample: &[SamplePoint],
    cancellation: Option<&Cancellation>,
) -> Result<(f64, f64), ImageError> {
    let (mut min, mut max) = (f64::NAN, f64::NAN);
    for batch in sample.chunks(4096) {
        check_imstat_cancelled(cancellation)?;
        for point in batch {
            min = min.min(point.value);
            max = max.max(point.value);
        }
    }
    Ok((min, max))
}

/// The statistics one `imstat` computation reports, before the record and
/// the world-coordinate strings are built.
pub(super) struct ComputedStats {
    pub(super) npts: f64,
    pub(super) min: f64,
    pub(super) max: f64,
    pub(super) sum: f64,
    pub(super) sumsq: f64,
    pub(super) mean: f64,
    pub(super) rms: f64,
    pub(super) sigma: f64,
    pub(super) median: f64,
    pub(super) medabsdevmed: f64,
    pub(super) q1: f64,
    pub(super) q3: f64,
    pub(super) quartile: f64,
    pub(super) minpos: Option<Vec<usize>>,
    pub(super) maxpos: Option<Vec<usize>>,
}

/// Converts a sum of image pixels to the flux reported by CASA statistics.
///
/// The factor uses square arcseconds for temperature and pixels per beam for
/// per-beam brightness. If the spectral axis is collapsed over multiple
/// channels, `channels` gives the first two measured spectral pixels.
pub fn statistics_flux_scale(
    coordinates: &CoordinateSystem,
    units: &str,
    beam: Option<GaussianBeam>,
    channels: Option<[usize; 2]>,
) -> Option<f64> {
    let pixel_area = direction_pixel_area(coordinates)?;
    let flux_factor = if units.contains('K') {
        let arcsec_per_rad = 180.0 * 3600.0 / std::f64::consts::PI;
        pixel_area * arcsec_per_rad * arcsec_per_rad
    } else if units.contains("/beam") {
        let beam_area = beam?.area();
        if beam_area <= 0.0 {
            return None;
        }
        pixel_area / beam_area
    } else {
        return None;
    };
    let channel_width = channels
        .and_then(|pixels| spectral_integration(coordinates, pixels))
        .unwrap_or(1.0);
    Some(flux_factor * channel_width)
}

/// The width casacore integrates `flux` over: one channel of the selected
/// spectral axis, when the selection holds more than one.
pub(super) fn spectral_integration(
    coordinates: &CoordinateSystem,
    [first, second]: [usize; 2],
) -> Option<f64> {
    let coordinate = coordinates.coordinate(coordinates.find_coordinate(CoordinateType::Spectral)?);
    let rest = spectral_rest_frequency(coordinates).unwrap_or(0.0);
    if rest > 0.0 {
        // The selection's own first two channels, as casacore's sub-image.
        let f0 = coordinate.to_world(&[first as f64]).ok()?[0];
        let f1 = coordinate.to_world(&[second as f64]).ok()?[0];
        Some(SPEED_OF_LIGHT_KM_S * (f1 - f0).abs() / rest)
    } else {
        Some(coordinate.increment()[0] * second.abs_diff(first) as f64)
    }
}

pub(super) fn direction_pixel_area(coords: &CoordinateSystem) -> Option<f64> {
    for idx in 0..coords.n_coordinates() {
        let coord = coords.coordinate(idx);
        if coord.coordinate_type() == CoordinateType::Direction && coord.n_pixel_axes() >= 2 {
            let increment = coord.increment();
            return Some(increment[0].abs() * increment[1].abs());
        }
    }
    None
}
