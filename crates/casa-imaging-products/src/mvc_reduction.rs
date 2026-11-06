// SPDX-License-Identifier: LGPL-3.0-or-later

//! Stream a cube's published normals into CASA's two-term Taylor images.

use crate::{
    ContinuumProductInputs, PlannedContinuumGeneration, ProductsError, TaylorMomentAccumulator,
    TaylorPrimaryBeamAccumulator, read_mvc_cube_plane, read_mvc_cube_primary_beam,
};

/// Reduce a cube normal state into CASA MVC's two-term Taylor products.
///
/// The caller owns and admits every output plane and each channel's transient
/// workspace. `mean_pb` is one x-major plane, `psf` three contiguous planes,
/// and `residual` two. The first pass reads only one PB and weight at a time;
/// the second reads one PSF, PB and corrected residual at a time. No cube-wide
/// image cache is created. `reference_hz` is the Taylor expansion reference,
/// which is independent of the Taylor image WCS.
///
/// # Errors
///
/// Invalid shapes or data, absent cube products, failed bounded reads, or
/// non-finite arithmetic. A failure leaves partial outputs to be discarded.
pub fn reduce_mvc_cube(
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
    reference_hz: f64,
    mean_pb: &mut [f32],
    psf: &mut [f32],
    residual: &mut [f32],
    sumwt: &mut [f32; 3],
) -> Result<(), ProductsError> {
    let [width, height] = inputs.normal_state().shape();
    let cells = width
        .checked_mul(height)
        .ok_or(ProductsError::InvalidTaylorMomentReduction)?;
    if cells == 0
        || mean_pb.len() != cells
        || cells.checked_mul(3) != Some(psf.len())
        || cells.checked_mul(2) != Some(residual.len())
        || !reference_hz.is_finite()
        || reference_hz <= 0.0
    {
        return Err(ProductsError::InvalidTaylorMomentReduction);
    }
    let channels = inputs.normal_state().channel_count();
    let mut beams = TaylorPrimaryBeamAccumulator::new(mean_pb)?;
    for channel in 0..channels {
        let beam = read_mvc_cube_primary_beam(planned, inputs, channel)?;
        beams.add(&beam.pixels, beam.sum_weight)?;
    }
    let mean_pb = beams.finish()?;
    let corrected = inputs.clone().with_cube_taylor_primary_beam(mean_pb)?;
    let cutoff = inputs
        .problem()
        .products()
        .validity()
        .primary_beam()
        .cutoff();
    let mut moments =
        TaylorMomentAccumulator::new(mean_pb, psf, residual, sumwt, reference_hz, cutoff)?;
    for channel in 0..channels {
        let plane = read_mvc_cube_plane(planned, &corrected, channel)?;
        moments.add(
            &plane.psf,
            &plane.residual,
            &plane.primary_beam,
            plane.sum_weight,
            plane.frequency_hz,
        )?;
    }
    moments.finish()
}
