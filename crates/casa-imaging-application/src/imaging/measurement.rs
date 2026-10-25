// SPDX-License-Identifier: LGPL-3.0-or-later
//! The measurement operator, spectral resampler and weighting rule of one
//! compiled problem: plan section 5.3 projected from `CompiledProblem`.

use casa_imaging_model::{
    AntennaResponseClass, CompiledImageDomain, CompiledProblem, InstrumentModel,
    ReconstructionBasis, SpectralKernel, SpectralWcs, WeightDensityScope,
};
use casa_imaging_operator::{
    AiryDish, AwCatalog, Basis, CacheBound, CacheCharge, ConvolutionFunctionSet, DensityCellRule,
    DensityGridShape, GridGeometry, GridPadding, GridPrecision, ImageExtent, MeasurementOperator,
    MosaicPb, MosaicRules, MosaicWindow, PaddingFactor, PolarizationRouting, SpectralAxis,
    SpectralResampler, Spheroidal, WPlaneCount, WPlanes,
};
use casa_imaging_runtime::pass::{BackendChoice, RowSamples};
use casa_imaging_runtime::{
    Demand, FRAME_MARGIN, HostResources, Reservation, ResourcePolicy, admit, row_layout,
};
use casa_types::Correlation;

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
    /// The charge of the kernel set, held while the operator lives.
    kernels: Kernels,
}

/// The charge of a kernel set: admitted before the set was built or, for
/// an AW catalog whose holders are the passes' workers, admitted once the
/// run has chosen its workers (#737).
enum Kernels {
    Admitted {
        _charge: Reservation,
    },
    /// An AW catalog's charge at its cache's bound, for at most `ceiling`
    /// holders. An `automatic` bound (`cf_resident_mb = auto`) rises, as the
    /// charge is admitted, into the memory the chosen workers leave (#449).
    Catalog {
        charge: CacheCharge,
        bound: CacheBound,
        ceiling: usize,
        automatic: bool,
    },
}

impl DomainOperator {
    /// Bytes the kernel set still to be admitted holds for passes on
    /// `workers` streaming workers, at most the ceiling it was opened for,
    /// at its cache's bound now; zero for a set already admitted.
    pub(crate) fn pending_kernel_bytes(&self, workers: usize) -> u64 {
        match &self.kernels {
            Kernels::Admitted { .. } => 0,
            Kernels::Catalog {
                charge,
                bound,
                ceiling,
                ..
            } => charge.bytes(workers.clamp(1, *ceiling), bound.bytes()),
        }
    }

    /// Admit the kernel set's charge for passes on `workers` streaming
    /// workers, if it is still to be admitted. An automatic cell cache first
    /// takes what it can of `spare`, the bytes the run's chosen workers
    /// leave free: every cell when they fit, otherwise as much more bound as
    /// `spare` allows. What the raise costs comes off `spare`, and what it
    /// saves goes back for the next catalog ([`CacheCharge::raise_within`]).
    pub(crate) fn admit_kernels(
        &mut self,
        admission: KernelAdmission<'_>,
        workers: usize,
        spare: &mut u64,
    ) -> Result<(), ImagingError> {
        if let Kernels::Catalog {
            charge,
            bound,
            ceiling,
            automatic,
        } = &self.kernels
        {
            let holders = workers.clamp(1, *ceiling);
            if *automatic {
                charge.raise_within(holders, bound, spare);
                tracing::info!(
                    "AW cell cache: {:.0} MB of {:.0} MB of cells resident at most (auto)",
                    bound.bytes().min(charge.every_bytes() as usize) as f64 / 1e6,
                    charge.every_bytes() as f64 / 1e6
                );
            }
            let memory = charge.bytes(holders, bound.bytes());
            self.kernels = Kernels::Admitted {
                _charge: admission.admit("convolution functions", memory)?,
            };
        }
        Ok(())
    }
}

/// Correlation types every block delivers, in block order: the first
/// source's selection, which every source must share.
pub(crate) fn selected_correlations(
    problem: &CompiledProblem,
) -> Result<Vec<Correlation>, ImagingError> {
    let mut layouts = problem
        .observation_transaction()
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

/// The most samples a row of `problem`'s source places and predicts on any
/// of its image domains, which share one resampler
/// ([`SpectralResampler::samples_per_row`],
/// [`SpectralResampler::prediction_samples_per_row`]).
pub(crate) fn row_samples(problem: &CompiledProblem) -> Result<RowSamples, ImagingError> {
    let resampler = resampler(problem, basis(problem)?)?;
    let spectrum = row_layout(problem).spectrum;
    Ok(RowSamples {
        placed: resampler.samples_per_row(spectrum),
        predicted: resampler.prediction_samples_per_row(spectrum.channels),
    })
}

/// The grid basis of the compiled reconstruction basis. A Taylor basis
/// expands about the image's reference frequency.
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

/// The grid precision: the requested one, else plan decision D2's rule as
/// the owner amended it on 2026-10-15 (`gridprecision = auto`): f64 on the
/// CPU for every basis, since CASA accumulates its grids in double
/// (`useDoublePrec`, `SynthesisUtilMethods.cc:3722`), and f32 on Metal,
/// which accumulates in `f32` only.
///
/// A cube is no exception: an f32 running sum drifts with the number of
/// samples a cell takes, and a mosaic's weight image takes every sample of
/// a channel at the uv origin (#659).
pub(crate) const fn precision(
    backend: BackendChoice,
    requested: Option<GridPrecision>,
) -> GridPrecision {
    if let Some(precision) = requested {
        return precision;
    }
    match backend {
        BackendChoice::Cpu => GridPrecision::F64,
        BackendChoice::Metal => GridPrecision::F32,
    }
}

/// Whether the problem grids with the standard kernel set, the one facets
/// take.
pub(crate) fn standard_kernel_set(problem: &CompiledProblem) -> bool {
    kernel_set_kind(problem) == KernelSetKind::Standard
}

/// Whether the Metal backend grids the problem's kernel set: the standard
/// spheroidal set, and the mosaic primary beams (#787). The mosaic's PSF
/// and prediction cells are its imaging cells, and its weight cells share
/// their support, so Metal's one tap route per mode serves it. Its weight
/// image keeps f32 precision because the weight owner sums it by
/// contribution first (#792). Each Metal backend of a pass copies the
/// cells it meets to the device, which the wave demand admits per backend
/// (`casa_imaging_metal::kernel_bytes`, #793 review). W-planes and the AW
/// catalog run on the CPU until #653's rows for them.
pub(crate) fn metal_kernel_set(problem: &CompiledProblem) -> bool {
    matches!(
        kernel_set_kind(problem),
        KernelSetKind::Standard | KernelSetKind::Mosaic
    )
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

/// The operator of a chart of `extent` (an image domain, or one of its
/// facets) with the kernel set the problem names: the
/// standard spheroidal set, W-projection planes sized by the W contract,
/// the mosaic primary beams of the selected windows, or the AW catalog of
/// `aw_catalog`, in the requested precision or the one `backend` grids at.
/// The mosaic set takes one dish per selected aperture class in
/// `dish_classes` (`HetArrayConvFunc::findAntennaSizes`), the order the
/// rows' antenna types index. Mosaic and AW grid without padding (CASA
/// `MosaicFT`, `AWProjectFT`); the others on CASA's composite-padded grid.
/// The kernel set is admitted as [`kernel_set`] says, and its charge lives
/// with the operator.
#[allow(clippy::too_many_arguments)]
pub(crate) fn domain_operator(
    problem: &CompiledProblem,
    extent: ImageExtent,
    correlations: &[Correlation],
    backend: BackendChoice,
    requested_precision: Option<GridPrecision>,
    aw_catalog: Option<&AwCatalogDeployment>,
    dish_classes: &[AntennaResponseClass],
    admission: KernelAdmission<'_>,
) -> Result<DomainOperator, ImagingError> {
    let kind = kernel_set_kind(problem);
    // Each CASA facet's GridFT pads its own facet grid.
    let padding = match kind {
        KernelSetKind::Standard | KernelSetKind::WPlanes => {
            GridPadding::CasaComposite(PaddingFactor::TCLEAN)
        }
        KernelSetKind::Mosaic | KernelSetKind::Aw => GridPadding::None,
    };
    let geometry = GridGeometry::new(extent, padding)?;
    let polarization = PolarizationRouting::compile(
        correlations,
        problem.reconstruction().polarization().coordinates(),
    )?;
    // An AW cube is refused by the gate (`Unsupported::CubeAwProjection`).
    let basis = basis(problem)?;
    let resampler = resampler(problem, basis)?;
    let (cf, kernels) = kernel_set(
        problem,
        kind,
        (&geometry, &polarization),
        aw_catalog,
        dish_classes,
        admission,
    )?;
    Ok(DomainOperator {
        operator: MeasurementOperator::new(
            geometry,
            basis,
            polarization,
            cf,
            precision(backend, requested_precision),
        ),
        resampler,
        weight_image: matches!(kind, KernelSetKind::Mosaic | KernelSetKind::Aw),
        kernels,
    })
}

/// Where a run admits its kernel sets: its host and policy, and the most
/// workers that fetch cells from a paged set, the policy's; the run admits
/// a paged set's charge once it has chosen how many do
/// ([`DomainOperator::admit_kernels`]).
#[derive(Clone, Copy)]
pub(crate) struct KernelAdmission<'a> {
    pub(crate) host: &'a HostResources,
    pub(crate) policy: &'a ResourcePolicy,
    pub(crate) workers: usize,
}

impl KernelAdmission<'_> {
    fn admit(self, phase: &'static str, memory: u64) -> Result<Reservation, ImagingError> {
        Ok(admit(self.host, self.policy, &Demand { phase, memory })?)
    }
}

/// The kernel set of `kind` on `geometry` routing `polarization`, with the
/// charge of what it holds, admitted before it is built. W-projection and
/// mosaic sets are built in two stages, their screens and then the kernels
/// cut from them, each admitted before it allocates; the screens' charge
/// is released once the kernels are built. An AW catalog's charge, its
/// cache bound and what its workers can hold beyond it
/// ([`AwCatalog::charge_bytes`]), is known once its index is read, for
/// each count of workers up to `admission`'s, and is admitted when the run
/// has chosen its workers.
fn kernel_set(
    problem: &CompiledProblem,
    kind: KernelSetKind,
    (geometry, polarization): (&GridGeometry, &PolarizationRouting),
    aw_catalog: Option<&AwCatalogDeployment>,
    dish_classes: &[AntennaResponseClass],
    admission: KernelAdmission<'_>,
) -> Result<(Box<dyn ConvolutionFunctionSet>, Kernels), ImagingError> {
    const PHASE: &str = "convolution functions";
    Ok(match kind {
        KernelSetKind::Standard => {
            let kernels = admission.admit(PHASE, Spheroidal::bytes(geometry))?;
            (
                Box::new(Spheroidal::new(geometry, polarization)),
                Kernels::Admitted { _charge: kernels },
            )
        }
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
            let screening = admission.admit(
                "W-projection screens",
                WPlanes::screens_bytes(geometry, count)?,
            )?;
            let screens = WPlanes::screens(geometry, polarization, count)?;
            let mut kernels = admission.admit(PHASE, screens.kernel_bytes())?;
            let set = screens.finish()?;
            drop(screening);
            kernels.retain(set.resident_bytes());
            (Box::new(set), Kernels::Admitted { _charge: kernels })
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
                        geometry,
                    )
                })
                .collect::<Vec<_>>();
            let frequency_hz = reference_frequency_hz(problem)?;
            let windows = mosaic_windows(problem)?;
            let rules = MosaicRules::Tclean;
            let screening = admission.admit(
                "mosaic screens",
                MosaicPb::screens_bytes(rules, geometry, frequency_hz, &dishes, &windows)?,
            )?;
            let screens = MosaicPb::screens(
                rules,
                geometry,
                polarization,
                frequency_hz,
                &dishes,
                &windows,
            )?;
            let mut kernels = admission.admit(PHASE, screens.kernel_bytes())?;
            let set = screens.finish()?;
            drop(screening);
            kernels.retain(set.resident_bytes());
            (Box::new(set), Kernels::Admitted { _charge: kernels })
        }
        KernelSetKind::Aw => {
            let deployment = aw_catalog.ok_or(ImagingError::Unsupported {
                reason: "an A-projection run needs its convolution-function catalog",
            })?;
            let catalog = AwCatalog::open_casa(
                &deployment.root,
                deployment.indexing,
                geometry,
                polarization,
                deployment.resident_bytes,
            )?;
            let kernels = Kernels::Catalog {
                charge: catalog.cache_charge(),
                bound: catalog.cache_bound(),
                ceiling: admission.workers.max(1),
                automatic: deployment.automatic,
            };
            (Box::new(catalog), kernels)
        }
    })
}

/// The selected spectral windows as the mosaic set keys them: every
/// channel of the window, its first channel width and the selected
/// channels' frequencies (`HetArrayConvFunc` beams per window).
fn mosaic_windows(problem: &CompiledProblem) -> Result<Vec<MosaicWindow>, ImagingError> {
    let mut windows = Vec::new();
    for source in problem.observation_transaction().read_set().sources() {
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

pub(crate) fn image_extent(domain: &CompiledImageDomain) -> Result<ImageExtent, ImagingError> {
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

/// The widest spacing between adjacent selected native channels of any
/// selected spectral window, from the storage owner's `CHAN_FREQ` catalog,
/// widened by [`FRAME_MARGIN`]. A window without a catalog makes it
/// infinite, so every wave holds the whole model.
pub(crate) fn native_spacing_hz(problem: &CompiledProblem) -> f64 {
    let mut widest = 0.0_f64;
    for window in problem
        .observation_transaction()
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
