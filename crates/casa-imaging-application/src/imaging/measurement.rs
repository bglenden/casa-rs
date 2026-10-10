// SPDX-License-Identifier: LGPL-3.0-or-later
//! The measurement operator, spectral resampler and weighting rule of one
//! compiled problem: plan section 5.3 projected from `CompiledProblem`.

use casa_imaging_model::{
    AntennaResponseClass, CompiledImageDomain, CompiledProblem, CorrelationType, InstrumentModel,
    ReconstructionBasis, SpectralKernel, SpectralWcs, WeightDensityScope,
};
use casa_imaging_operator::{
    AiryDish, AwCatalog, Basis, ConvolutionFunctionSet, DensityCellRule, DensityGridShape,
    GridGeometry, GridPadding, GridPrecision, ImageExtent, MeasurementOperator, MosaicPb,
    MosaicWindow, PolarizationRouting, SpectralAxis, SpectralResampler, Spheroidal, WPlaneCount,
    WPlanes,
};
use casa_imaging_runtime::pass::BackendChoice;

use super::ImagingError;
use crate::AwCatalogDeployment;

/// The operator of one image domain and the resampler that places rows on
/// its planes.
pub(crate) struct DomainOperator {
    pub(crate) operator: MeasurementOperator,
    pub(crate) resampler: SpectralResampler,
    /// Whether the kernel set grids a sensitivity (weight) image
    /// (`ConvolutionFunctionSet::weight_taps`): mosaic and AW.
    pub(crate) weight_image: bool,
}

/// Correlation types every block delivers, in block order: the first
/// source's selection, which every source must share.
pub(crate) fn selected_correlations(
    problem: &CompiledProblem,
) -> Result<Vec<CorrelationType>, ImagingError> {
    let mut layouts = problem
        .selected_observation()
        .read_set()
        .sources()
        .iter()
        .flat_map(|source| source.selection().correlations())
        .map(|selection| {
            selection
                .products()
                .iter()
                .map(|product| product.correlation_type())
                .collect::<Vec<_>>()
        });
    let first = layouts.next().ok_or(ImagingError::Unsupported {
        reason: "the selection names no correlations",
    })?;
    if layouts.any(|layout| layout != first) {
        return Err(ImagingError::Unsupported {
            reason: "selected polarization setups differ in their correlations",
        });
    }
    Ok(first)
}

/// The grid basis of the compiled reconstruction basis. A Taylor basis
/// expands about the image's reference frequency. Taylor-via-channel-major
/// (CASA `mvc`) has no pass implementation and no request names it; #656
/// removes the basis.
pub(crate) fn basis(problem: &CompiledProblem) -> Result<Basis, ImagingError> {
    Ok(match problem.reconstruction().basis() {
        ReconstructionBasis::Constant => Basis::Constant,
        ReconstructionBasis::Taylor { terms } => Basis::Taylor {
            terms: terms as u32,
            reference_hz: reference_frequency_hz(problem)?,
        },
        ReconstructionBasis::ChannelLocal { channels } => Basis::ChannelLocal {
            planes: channels as u32,
        },
        ReconstructionBasis::TaylorViaChannelMajor { .. } => {
            return Err(ImagingError::Unsupported {
                reason: "Taylor terms via channel cubes have no pass implementation",
            });
        }
    })
}

/// The image's spectral reference frequency (CASA `reffreq`).
pub(crate) fn reference_frequency_hz(problem: &CompiledProblem) -> Result<f64, ImagingError> {
    match problem.geometry().spectral().wcs() {
        SpectralWcs::Linear {
            reference_frequency_hz,
            ..
        } => Ok(*reference_frequency_hz),
        SpectralWcs::Tabular { .. } => Err(ImagingError::Unsupported {
            reason: "a tabular spectral axis has no uniform channel width",
        }),
    }
}

/// The grid precision: the requested one, else plan decision D2's rule
/// (`gridprecision = auto`): f32 on Metal, which accumulates in `f32` for
/// every basis; on the CPU f64 for the constant and Taylor bases and f32
/// for channel-local cubes.
pub(crate) const fn precision(
    basis: Basis,
    backend: BackendChoice,
    requested: Option<GridPrecision>,
) -> GridPrecision {
    if let Some(precision) = requested {
        return precision;
    }
    match (backend, basis) {
        (BackendChoice::Cpu, Basis::Constant | Basis::Taylor { .. }) => GridPrecision::F64,
        (BackendChoice::Cpu, Basis::ChannelLocal { .. }) | (BackendChoice::Metal, _) => {
            GridPrecision::F32
        }
    }
}

/// Whether the problem grids with the standard kernel set, the one the
/// Metal backend implements.
pub(crate) fn standard_kernel_set(problem: &CompiledProblem) -> bool {
    kernel_set_kind(problem) == KernelSetKind::Standard
}

/// Which kernel set the compiled problem's measurement equation names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KernelSetKind {
    /// The standard spheroidal set (`GridFT`).
    Standard,
    /// W-projection planes (`WProjectFT`).
    WPlanes,
    /// The heterogeneous-array mosaic beams (`MosaicFT` with
    /// `HetArrayConvFunc`).
    Mosaic,
    /// The AW catalog (`AWProjectFT`).
    Aw,
}

fn kernel_set_kind(problem: &CompiledProblem) -> KernelSetKind {
    let equation = problem.science().measurement_equation();
    if equation.aw_projection().is_some() {
        KernelSetKind::Aw
    } else if equation.w_projection().is_some() {
        KernelSetKind::WPlanes
    } else if problem.science().instrument_model()
        == Some(InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1)
    {
        KernelSetKind::Mosaic
    } else {
        KernelSetKind::Standard
    }
}

/// The operator of `domain` with the kernel set the problem names: the
/// standard spheroidal set, W-projection planes sized by the W contract,
/// the mosaic primary beams of the selected windows, or the AW catalog of
/// `aw_catalog`, in the requested precision or the one `backend` grids at.
/// The mosaic set takes one dish per selected aperture class in
/// `dish_classes` (`HetArrayConvFunc::findAntennaSizes`), the order the
/// rows' antenna types index. Mosaic and AW grid without padding (CASA
/// `MosaicFT`, `AWProjectFT`); the others on CASA's composite-padded grid.
pub(crate) fn domain_operator(
    problem: &CompiledProblem,
    domain: &CompiledImageDomain,
    correlations: &[CorrelationType],
    backend: BackendChoice,
    requested_precision: Option<GridPrecision>,
    aw_catalog: Option<&AwCatalogDeployment>,
    dish_classes: &[AntennaResponseClass],
) -> Result<DomainOperator, ImagingError> {
    let kind = kernel_set_kind(problem);
    let padding = match kind {
        KernelSetKind::Standard | KernelSetKind::WPlanes => GridPadding::CasaComposite,
        KernelSetKind::Mosaic | KernelSetKind::Aw => GridPadding::None,
    };
    let geometry = GridGeometry::new(image_extent(domain)?, padding)?;
    let polarization = PolarizationRouting::compile(
        correlations,
        problem.reconstruction().polarization().coordinates(),
    )?;
    let basis = basis(problem)?;
    if matches!(basis, Basis::ChannelLocal { .. })
        && matches!(kind, KernelSetKind::Mosaic | KernelSetKind::Aw)
    {
        // The channel-local normal state keeps a scalar sumwt per plane;
        // the dense sensitivity these sets grid has no plane to live in
        // until the cube tickets.
        return Err(ImagingError::Unsupported {
            reason: "cube mosaic and A-projection imaging wait for the cube tickets' per-plane sensitivity",
        });
    }
    let resampler = resampler(problem, basis)?;
    let cf: Box<dyn ConvolutionFunctionSet> = match kind {
        KernelSetKind::Standard => Box::new(Spheroidal::new(&geometry, &polarization)),
        KernelSetKind::WPlanes => {
            let contract = problem
                .science()
                .measurement_equation()
                .w_projection()
                .expect("the kind names a W contract");
            let count = match (contract.planes(), contract.statistics()) {
                (Some(planes), _) => WPlaneCount::Fixed(planes.get() as u32),
                (None, Some(statistics)) => WPlaneCount::Auto {
                    min_w: statistics.minimum_abs_w_lambda(),
                    max_w: contract.maximum_abs_w_lambda(),
                    rms_w: statistics.rms_w_lambda(),
                },
                (None, None) => {
                    return Err(ImagingError::Unsupported {
                        reason: "an automatic W-plane count needs the selection's w statistics",
                    });
                }
            };
            Box::new(WPlanes::new(&geometry, &polarization, count)?)
        }
        KernelSetKind::Mosaic => {
            if dish_classes.is_empty() {
                return Err(ImagingError::Unsupported {
                    reason: "the mosaic set needs ALMA or ACA dishes in the selection",
                });
            }
            let dishes = dish_classes
                .iter()
                .map(|class| {
                    AiryDish::casa_alma(
                        match class {
                            AntennaResponseClass::CasaAlma12m => 12.0,
                            AntennaResponseClass::CasaAca7m => 7.0,
                        },
                        &geometry,
                    )
                })
                .collect::<Vec<_>>();
            Box::new(MosaicPb::new(
                &geometry,
                &polarization,
                reference_frequency_hz(problem)?,
                &dishes,
                &mosaic_windows(problem)?,
            )?)
        }
        KernelSetKind::Aw => {
            let deployment = aw_catalog.ok_or(ImagingError::Unsupported {
                reason: "an A-projection run needs its convolution-function catalog",
            })?;
            Box::new(AwCatalog::open_casa(
                &deployment.root,
                deployment.indexing,
                &geometry,
                &polarization,
                deployment.resident_bytes,
            )?)
        }
    };
    Ok(DomainOperator {
        operator: MeasurementOperator::new(
            geometry,
            basis,
            polarization,
            cf,
            precision(basis, backend, requested_precision),
        ),
        resampler,
        weight_image: matches!(kind, KernelSetKind::Mosaic | KernelSetKind::Aw),
    })
}

/// The selected spectral windows as the mosaic set keys them: every
/// channel of the window, its first channel width and the selected
/// channels' frequencies (`HetArrayConvFunc` beams per window).
fn mosaic_windows(problem: &CompiledProblem) -> Result<Vec<MosaicWindow>, ImagingError> {
    let mut windows = Vec::new();
    for source in problem.selected_observation().read_set().sources() {
        for window in source.selection().spectral_windows() {
            if windows
                .iter()
                .any(|known: &MosaicWindow| known.spectral_window == window.spectral_window_id())
            {
                continue;
            }
            let catalog = window
                .coordinate_catalog()
                .ok_or(ImagingError::Unsupported {
                    reason: "the mosaic set needs every window's channel frequencies",
                })?;
            let selected = window
                .channel_indices()
                .iter()
                .map(|index| {
                    catalog
                        .channel_frequency_hz(*index as usize)
                        .ok_or(ImagingError::Unsupported {
                            reason: "a selected channel lies outside its window",
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            windows.push(MosaicWindow {
                spectral_window: window.spectral_window_id(),
                window_frequencies_hz: catalog.channel_frequencies_hz().to_vec(),
                channel_width_hz: catalog.first_channel_width_hz(),
                selected_frequencies_hz: selected,
            });
        }
    }
    Ok(windows)
}

fn image_extent(domain: &CompiledImageDomain) -> Result<ImageExtent, ImagingError> {
    let direction = domain.direction();
    let reference = direction.reference_pixel();
    if reference
        .iter()
        .any(|pixel| pixel.fract() != 0.0 || *pixel < 0.0)
    {
        return Err(ImagingError::Unsupported {
            reason: "the image reference pixel must be a whole pixel",
        });
    }
    Ok(ImageExtent {
        shape: domain.shape().pixels(),
        increment_rad: direction.increment_rad(),
        reference_pixel: [reference[0] as usize, reference[1] as usize],
    })
}

/// CASA `FTMachine` channel mapping for a channel-local basis; every native
/// channel on plane 0 otherwise.
fn resampler(problem: &CompiledProblem, basis: Basis) -> Result<SpectralResampler, ImagingError> {
    let Basis::ChannelLocal { planes } = basis else {
        return Ok(SpectralResampler::direct(basis)?);
    };
    let SpectralWcs::Linear {
        reference_pixel,
        reference_frequency_hz,
        increment_hz,
        ..
    } = problem.geometry().spectral().wcs()
    else {
        return Err(ImagingError::Unsupported {
            reason: "a tabular spectral axis has no uniform channel width",
        });
    };
    let kernel = match problem.science().spectral().sampling().kernel() {
        SpectralKernel::Nearest | SpectralKernel::Identity => {
            casa_imaging_operator::SpectralKernel::Nearest
        }
        SpectralKernel::Linear => casa_imaging_operator::SpectralKernel::Linear,
        SpectralKernel::Cubic | SpectralKernel::ChannelIntegration { .. } => {
            return Err(ImagingError::Unsupported {
                reason: "cube interpolation is nearest or linear",
            });
        }
    };
    let first_hz = reference_frequency_hz - reference_pixel * increment_hz;
    Ok(SpectralResampler::channel_local(
        SpectralAxis::new(first_hz, *increment_hz, planes)?,
        kernel,
    ))
}

/// Relative allowance for the Doppler factor between the stored frame of the
/// spectral-window catalog and the output frame rows are delivered in
/// (3000 km/s); the pass checks every row against the widened bound.
const FRAME_MARGIN: f64 = 0.01;

/// The widest spacing between adjacent selected native channels of any
/// selected spectral window, from the storage owner's `CHAN_FREQ` catalog,
/// widened by [`FRAME_MARGIN`]. A window without a catalog makes it
/// infinite, so every wave holds the whole model.
pub(crate) fn native_spacing_hz(problem: &CompiledProblem) -> f64 {
    let mut widest = 0.0_f64;
    for window in problem
        .selected_observation()
        .read_set()
        .sources()
        .iter()
        .flat_map(|source| source.selection().spectral_windows())
    {
        let Some(catalog) = window.coordinate_catalog() else {
            return f64::INFINITY;
        };
        for pair in window.channel_indices().windows(2) {
            let (Some(first), Some(second)) = (
                catalog.channel_frequency_hz(pair[0] as usize),
                catalog.channel_frequency_hz(pair[1] as usize),
            ) else {
                return f64::INFINITY;
            };
            widest = widest.max((second - first).abs());
        }
    }
    widest * (1.0 + FRAME_MARGIN)
}

/// Shape of the weight-density grid of `domain`: one global plane on CASA's
/// `VisImagingWeight` cells, or one plane per output channel, plus the
/// compiled padding planes on each side, on `BriggsCubeWeightor` cells.
pub(crate) fn density_shape(
    problem: &CompiledProblem,
    domain: &CompiledImageDomain,
) -> Result<DensityGridShape, ImagingError> {
    let [width, height] = domain.shape().pixels();
    let (planes, padding, rule) = match problem.weighting().density_scope() {
        WeightDensityScope::PerOutputChannel => {
            // CASA always pads the cube density axis
            // (`BriggsCubeWeightor::estimateSwingChanPad`); a contract without
            // the padding cannot reproduce it.
            let padding = problem.weighting().casa_cube_density_padding().ok_or(
                ImagingError::Unsupported {
                    reason: "a per-channel weight density needs its compiled padding",
                },
            )?;
            let planes = padding
                .checked_mul(2)
                .and_then(|padding| {
                    padding.checked_add(problem.geometry().spectral().output_channels())
                })
                .ok_or(ImagingError::Unsupported {
                    reason: "the padded density axis overflows",
                })?;
            let padding = u32::try_from(padding).map_err(|_| ImagingError::Unsupported {
                reason: "the padded density axis overflows",
            })?;
            (planes, padding, DensityCellRule::Cube)
        }
        WeightDensityScope::NotApplicable | WeightDensityScope::GlobalSelection => {
            (1, 0, DensityCellRule::Standard)
        }
    };
    Ok(DensityGridShape {
        width,
        height,
        planes,
        padding,
        increment_rad: domain.direction().increment_rad(),
        rule,
    })
}
