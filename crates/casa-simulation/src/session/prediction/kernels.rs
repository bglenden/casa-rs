// SPDX-License-Identifier: LGPL-3.0-or-later
//! Admitted preparation of simulation convolution functions.
use super::*;

impl Simulator {
    /// Build `layout`'s kernels, admitted as they are built, as imaging
    /// admits them: the spheroidal set's bytes, or the W-projection screens
    /// ([`WPlanes::screens_bytes`]) and then the kernels cropped from them
    /// ([`WScreens::kernel_bytes`](casa_imaging_operator::WScreens::kernel_bytes)),
    /// the screens released once the kernels are built. The reservation
    /// returned keeps what the kernels hold.
    pub(super) fn admitted_kernels(
        &self,
        layout: &ModelLayout,
    ) -> SimulatorResult<(Box<dyn ConvolutionFunctionSet>, Reservation)> {
        let resources = &self.options.resources;
        let (host, policy) = (resources.host(), resources.policy());
        charge_process_caches(host, policy).map_err(SimulatorError::admission)?;
        let admitted = |phase, memory| {
            admit(host, policy, &Demand { phase, memory }).map_err(SimulatorError::admission)
        };
        let (geometry, polarization) = (layout.geometry(), layout.polarization());
        let raised =
            |error: OperatorError| SimulatorError::raised(format!("Caught exception: {error}"));
        Ok(match layout.machine() {
            FtMachine::Grid(_) | FtMachine::Both(_) => (
                Box::new(Spheroidal::new(geometry, polarization)),
                admitted(KERNEL_PHASE, Spheroidal::bytes(geometry))?,
            ),
            FtMachine::GridBox(_) => {
                let reservation =
                    admitted(KERNEL_PHASE, casa_imaging_operator::Boxcar::bytes(geometry))?;
                (
                    Box::new(casa_imaging_operator::Boxcar::new(geometry, polarization)),
                    reservation,
                )
            }
            FtMachine::WProject(planes) => {
                let count = WPlaneCount::Fixed(planes);
                let screening = admitted(
                    W_SCREENS_PHASE,
                    WPlanes::screens_bytes(geometry, count).map_err(raised)?,
                )?;
                let screens = WPlanes::screens(geometry, polarization, count).map_err(raised)?;
                let mut kernels = admitted(KERNEL_PHASE, screens.kernel_bytes())?;
                let set = screens.finish().map_err(raised)?;
                drop(screening);
                kernels.retain(set.resident_bytes());
                tracing::info!(
                    planes,
                    screen = set.screen_size(),
                    bytes = set.resident_bytes(),
                    "W projection using {planes} planes"
                );
                (Box::new(set), kernels)
            }
            FtMachine::Mosaic => {
                let beams = layout.beams().expect("a mosaic layout has its beams");
                let (dishes, windows) = (&beams.dishes, &beams.windows);
                if beams.common {
                    let screening = admitted(
                        MOSAIC_SCREENS_PHASE,
                        MosaicPb::simple_screens_bytes(geometry, windows).map_err(raised)?,
                    )?;
                    let screens = beams
                        .simple_screens(geometry, polarization)
                        .map_err(model_error)?;
                    let mut kernels = admitted(KERNEL_PHASE, screens.kernel_bytes())?;
                    let set = screens.finish().map_err(raised)?;
                    drop(screening);
                    kernels.retain(set.resident_bytes());
                    return Ok((Box::new(set), kernels));
                }
                let (rules, frequency_hz) = (MosaicRules::Simulator, layout.image_frequency_hz());
                tracing::info!("Performing Mosaic gridding for model images (uv convolution)");
                let screening = admitted(
                    MOSAIC_SCREENS_PHASE,
                    MosaicPb::screens_bytes(rules, geometry, frequency_hz, dishes, windows)
                        .map_err(raised)?,
                )?;
                let screens =
                    MosaicPb::screens(rules, geometry, polarization, frequency_hz, dishes, windows)
                        .map_err(raised)?;
                let mut kernels = admitted(KERNEL_PHASE, screens.kernel_bytes())?;
                let set = screens.finish().map_err(raised)?;
                drop(screening);
                kernels.retain(set.resident_bytes());
                tracing::info!(
                    dishes = set.dishes(),
                    screen = set.screen_size(),
                    bytes = set.resident_bytes(),
                    "convolution function support: {}",
                    set.max_half_support()[0]
                );
                (Box::new(set), kernels)
            }
        })
    }
}
