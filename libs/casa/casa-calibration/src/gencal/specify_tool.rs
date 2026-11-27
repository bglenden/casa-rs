// SPDX-License-Identifier: LGPL-3.0-or-later
//! Direct calibrater parameter prescriptions.

use casa_ms::MsSelection;
use std::path::Path;

use super::{
    GencalError, GencalRequest, GencalType, generate_prior, open_measurement_set, specified_rows,
    write_prior_caltable,
};
use crate::output::CalibrationOutputMode;

pub(super) struct PriorCalibrationTool<'a> {
    pub(super) legacy_iterator: bool,
    pub(super) selection: &'a MsSelection,
}

/// A parameter prescription passed to the attached calibrater tool.
pub struct CalibrationSpecification<'a> {
    /// Calibration table to create or update.
    pub table: &'a Path,
    /// Spectral-window selection.
    pub spw: &'a str,
    /// Antenna selection.
    pub antenna: &'a str,
    /// Receptor selection and order.
    pub polarization: &'a str,
    /// CASA tool factory name or parameter type.
    pub caltype: &'a str,
    /// Numeric prescription in selection order.
    pub parameters: Vec<f64>,
    /// Optional calibration input used by the selected family.
    pub infile: Option<&'a Path>,
    /// Use uniform rather than exposure weights where the family supports it.
    pub uniform: bool,
}

impl CalibrationSpecification<'_> {
    pub(crate) fn write(
        self,
        dataset: &Path,
        legacy_iterator: bool,
        selection: &MsSelection,
    ) -> Result<(), GencalError> {
        if self.table.as_os_str().is_empty() {
            return Err(GencalError::InvalidRequest(
                "No output calibration table was specified.".into(),
            ));
        }
        let kind = self.caltype.to_ascii_uppercase();
        let (caltype, sky_subtype) = if let Some(caltype) = prior_type(&kind) {
            (caltype, None)
        } else if let Some(subtype) = ["SDSKY_PS", "SDSKY_RASTER", "SDSKY_OTF"]
            .into_iter()
            .find(|subtype| kind.contains(subtype))
        {
            if subtype == "SDSKY_OTF" && !legacy_iterator {
                return Err(GencalError::InvalidRequest(
                    "SDSKY_OTF not yet supported via SingleDishOtfCal(msmc)".into(),
                ));
            }
            // The generic sky and delay prescriptions use the same two real
            // additive parameters. The writer receives the actual sky subtype.
            (GencalType::Sbd, Some(subtype))
        } else {
            return Err(GencalError::InvalidRequest(format!(
                "Unrecognized caltype {:?}.",
                self.caltype
            )));
        };
        let antpos = matches!(caltype, GencalType::Antpos | GencalType::AntposVla);
        let antenna = if antpos && self.antenna.trim().is_empty() {
            "*"
        } else {
            self.antenna
        };
        let request = GencalRequest {
            measurement_set: dataset.to_owned(),
            output_table: self.table.to_owned(),
            caltype,
            antenna: antenna.to_owned(),
            spw: self.spw.to_owned(),
            parameter: self.parameters,
            gaincurve_table: None,
        };
        if let Some(subtype) = sky_subtype {
            let ms = open_measurement_set(dataset)?;
            // Pinned CASA's sky factories can create a specified table, but
            // reject loading their explicit rows again for an update.
            let mode = CalibrationOutputMode::Create;
            let output = crate::output::CalibrationOutput::prepare(self.table, mode)?;
            let rows = specified_rows(&ms, &request, self.polarization, false)?;
            write_prior_caltable(&ms, &request, subtype, &rows, output, mode, true)?;
        } else {
            generate_prior(
                &request,
                self.polarization,
                self.infile.filter(|_| !antpos),
                None,
                self.uniform,
                CalibrationOutputMode::Update,
                Some(PriorCalibrationTool {
                    legacy_iterator,
                    selection,
                }),
            )?;
        }
        Ok(())
    }
}

fn prior_type(kind: &str) -> Option<GencalType> {
    Some(match kind {
        kind if kind.contains("PH") => GencalType::Ph,
        kind if kind == "G" || kind.contains("AMP") => GencalType::Amp,
        kind if kind == "K"
            || ["SBD", "DELAY", "MBD"]
                .iter()
                .any(|name| kind.contains(name)) =>
        {
            GencalType::Sbd
        }
        kind if kind.contains("ANTPOS") && kind.contains("VLA") => GencalType::AntposVla,
        kind if kind.contains("ANTPOS") => GencalType::Antpos,
        kind if kind.contains("TSYS") => GencalType::Tsys,
        kind if ["EVLAGAIN", "SWP", "RQ", "WTS"]
            .iter()
            .any(|token| kind.contains(token)) =>
        {
            [
                ("SWP/RQ", GencalType::SwpOverRq),
                ("SWPOW", GencalType::Swpow),
                ("EVLAGAIN", GencalType::Evlagain),
                ("RQ", GencalType::Rq),
                ("SWPWTS", GencalType::Swpwts),
            ]
            .into_iter()
            .find_map(|(token, family)| kind.contains(token).then_some(family))?
        }
        kind if kind.contains("OPAC") => GencalType::Opac,
        kind if kind.contains("GC") && kind.contains("EFF") => GencalType::Gceff,
        kind if kind.contains("GC") => GencalType::Gc,
        kind if kind.contains("EFF") => GencalType::Eff,
        "TECIM" => GencalType::Tecim,
        _ => return None,
    })
}
