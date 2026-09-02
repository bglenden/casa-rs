// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bounded evaluation of compiled scalar primary-beam power responses.

use casa_numerics::AnnularApertureVoltageTable;

use crate::SpectralOperatorError;

const CASA_ALMA_ACA_DIRECT_PB_SUPPORT_ARCMIN_GHZ: f64 = 3.568 * 60.0;

#[derive(Clone)]
pub(crate) struct PreparedPrimaryBeamPower {
    table: AnnularApertureVoltageTable,
    reference_pixel: [f64; 2],
    increment_rad: [f64; 2],
    pc: [[f64; 2]; 2],
    shape: [usize; 2],
    cutoff: f32,
}

impl PreparedPrimaryBeamPower {
    pub(crate) fn casa_alma_aca_interferometric_direct(
        reference_pixel: [f64; 2],
        increment_rad: [f64; 2],
        shape: [usize; 2],
        cutoff: f32,
    ) -> Result<Self, SpectralOperatorError> {
        if shape.contains(&0) || !cutoff.is_finite() || cutoff < 0.0 {
            return Err(SpectralOperatorError::UnsupportedProblem);
        }
        Ok(Self {
            table: AnnularApertureVoltageTable::new(
                6.25,
                0.75,
                CASA_ALMA_ACA_DIRECT_PB_SUPPORT_ARCMIN_GHZ,
            ),
            reference_pixel,
            increment_rad,
            pc: [[1.0, 0.0], [0.0, 1.0]],
            shape,
            cutoff,
        })
    }

    pub(crate) fn fill_power_plane_into(
        &self,
        frequency_hz: f64,
        output: &mut [f32],
    ) -> Result<(), SpectralOperatorError> {
        let cells = self.shape[0]
            .checked_mul(self.shape[1])
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        if output.len() != cells || !frequency_hz.is_finite() || frequency_hz <= 0.0 {
            return Err(SpectralOperatorError::InvalidSample);
        }
        let mut maximum = 0.0_f32;
        for x in 0..self.shape[0] {
            let pixel_x = x as f64 - self.reference_pixel[0];
            for y in 0..self.shape[1] {
                let pixel_y = y as f64 - self.reference_pixel[1];
                let axis_x = pixel_x * self.increment_rad[0];
                let axis_y = pixel_y * self.increment_rad[1];
                let l_rad = self.pc[0][0] * axis_x + self.pc[0][1] * axis_y;
                let m_rad = self.pc[1][0] * axis_x + self.pc[1][1] * axis_y;
                // PBMath1D stores the two angular offsets and radius as Float.
                let l_deg = l_rad.to_degrees() as f32;
                let m_deg = m_rad.to_degrees() as f32;
                let radius_deg = (l_deg * l_deg + m_deg * m_deg).sqrt();
                let radius_arcmin_ghz =
                    (f64::from(radius_deg) * 60.0 * (frequency_hz / 1.0e9)) as f32 as f64;
                let voltage = self.table.evaluate(radius_arcmin_ghz);
                let power = voltage * voltage;
                output[x * self.shape[1] + y] = power;
                maximum = maximum.max(power);
            }
        }
        if maximum > 1.0 {
            output.iter_mut().for_each(|value| *value /= maximum);
        }
        output.iter_mut().for_each(|value| {
            if !value.is_finite() || *value <= self.cutoff {
                *value = 0.0;
            }
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, path::PathBuf};

    use casa_images::PagedImage;

    use super::*;

    #[test]
    fn direct_alma_power_is_streamed_into_one_caller_plane() {
        let response = PreparedPrimaryBeamPower::casa_alma_aca_interferometric_direct(
            [4.0, 4.0],
            [-4.848_136_811_095_36e-7, 4.848_136_811_095_36e-7],
            [8, 8],
            0.1,
        )
        .expect("compiled response");
        let mut plane = [0.0; 64];
        response
            .fill_power_plane_into(230.0e9, &mut plane)
            .expect("power plane");
        assert_eq!(plane[4 * 8 + 4], 1.0);
        assert!(plane.iter().all(|value| value.is_finite()));
        assert!(plane[0] < 1.0);
    }

    #[test]
    #[ignore = "requires the frozen CASA T41 MVC primary-beam cube"]
    fn t41_alma_mvc_primary_beam_owner_matches_frozen_cube() -> Result<(), Box<dyn Error>> {
        let prefix = PathBuf::from(
            std::env::var_os("CASA_RS_T41_MVC_CASA_PREFIX")
                .ok_or("CASA_RS_T41_MVC_CASA_PREFIX is not set")?,
        );
        let casa = PagedImage::<f32>::open(PathBuf::from(format!("{}.pb", prefix.display())))?;
        assert_eq!(casa.shape(), [512, 512, 1, 40]);
        let shape = casa.shape().to_vec();
        let expected = casa.get_slice(&vec![0; shape.len()], &shape)?;
        let valid = casa
            .get_mask_slice(&vec![0; shape.len()], &shape, &vec![1; shape.len()])?
            .map_or_else(
                || vec![true; shape.iter().product()],
                |mask| mask.iter().copied().collect(),
            );
        let response = PreparedPrimaryBeamPower::casa_alma_aca_interferometric_direct(
            [256.0, 256.0],
            [-4.848_136_811_095_359e-7, 4.848_136_811_095_359e-7],
            [512, 512],
            0.1,
        )?;
        let mut generated = vec![0.0; 512 * 512];
        for channel in [0, 39] {
            let frequency = 230_449_729_492.188_84 + channel as f64 * 122_982_578.274_169_92;
            response.fill_power_plane_into(frequency, &mut generated)?;
            let (error, reference) = generated
                .iter()
                .enumerate()
                .filter(|(cell, _)| valid[cell * 40 + channel])
                .fold((0.0, 0.0), |(error, reference), (cell, actual)| {
                    let actual = f64::from(*actual);
                    let expected = f64::from(expected[[cell / 512, cell % 512, 0, channel]]);
                    (
                        error + (actual - expected).powi(2),
                        reference + expected.powi(2),
                    )
                });
            let nrms = (error / reference.max(f64::MIN_POSITIVE)).sqrt();
            eprintln!("t41_mvc_pb_owner channel={channel} nrms={nrms:.9e}");
            assert!(nrms <= 5.0e-6, "channel {channel} PB NRMS {nrms:.6e}");
        }
        Ok(())
    }
}
