// SPDX-License-Identifier: LGPL-3.0-or-later
//! The measurement operator, spectral resampler and weighting rule of one
//! compiled problem: plan section 5.3 projected from `CompiledProblem`.

use casa_imaging_model::{
    CompiledImageDomain, CompiledProblem, CorrelationType, ReconstructionBasis, SpectralKernel,
    SpectralWcs, WeightDensityScope,
};
use casa_imaging_operator::{
    Basis, DensityCellRule, DensityGridShape, GridGeometry, GridPadding, GridPrecision,
    ImageExtent, MeasurementOperator, PolarizationRouting, SpectralAxis, SpectralResampler,
    Spheroidal,
};
use casa_imaging_runtime::pass::BackendChoice;

use super::ImagingError;

/// The operator of one image domain and the resampler that places rows on
/// its planes.
pub(crate) struct DomainOperator {
    pub(crate) operator: MeasurementOperator,
    pub(crate) resampler: SpectralResampler,
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
/// (CASA `mvc`) always carries a primary-beam response and waits for the
/// primary-beam operators (IF-3).
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
                reason: "Taylor terms via channel cubes need the primary-beam operators",
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

/// Grid precision of plan decision D2 (`gridprecision = auto`): f32 on
/// Metal, which accumulates in `f32` for every basis; on the CPU f64 for
/// the constant and Taylor bases and f32 for channel-local cubes.
pub(crate) const fn precision(basis: Basis, backend: BackendChoice) -> GridPrecision {
    match (backend, basis) {
        (BackendChoice::Cpu, Basis::Constant | Basis::Taylor { .. }) => GridPrecision::F64,
        (BackendChoice::Cpu, Basis::ChannelLocal { .. }) | (BackendChoice::Metal, _) => {
            GridPrecision::F32
        }
    }
}

/// The operator of `domain` for the standard kernel set, in the precision
/// `backend` grids at.
pub(crate) fn domain_operator(
    problem: &CompiledProblem,
    domain: &CompiledImageDomain,
    correlations: &[CorrelationType],
    backend: BackendChoice,
) -> Result<DomainOperator, ImagingError> {
    let geometry = GridGeometry::new(image_extent(domain)?, GridPadding::CasaComposite)?;
    let polarization = PolarizationRouting::compile(
        correlations,
        problem.reconstruction().polarization().coordinates(),
    )?;
    let basis = basis(problem)?;
    let resampler = resampler(problem, basis)?;
    let cf = Spheroidal::new(&geometry, &polarization);
    Ok(DomainOperator {
        operator: MeasurementOperator::new(
            geometry,
            basis,
            polarization,
            Box::new(cf),
            precision(basis, backend),
        ),
        resampler,
    })
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
