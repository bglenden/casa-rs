// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;

use casa_core::{ArrayD, ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::IxDyn;

use super::axis::ProfileAxis;
use super::curve::Curve;
use super::models::{LineKind, ProfileModels, SpectralPolynomial};
use super::solver::Solution;
use super::{ProfileLineReport, ProfileReport};

const NAMES: [&str; 8] = [
    "amp",
    "ampErr",
    "center",
    "centerErr",
    "fwhm",
    "fwhmErr",
    "integral",
    "integralErr",
];

pub(super) struct ProfileResults {
    pub(super) attempted: ArrayD<bool>,
    pub(super) succeeded: ArrayD<bool>,
    pub(super) converged: ArrayD<bool>,
    pub(super) valid: ArrayD<bool>,
    iterations: ArrayD<i64>,
    components: ArrayD<i64>,
    direction: ArrayD<String>,
    model_types: ArrayD<String>,
    gaussian: LineResults,
    lorentzian: LineResults,
    multiplets: Vec<LineResults>,
    spectral: Option<(SpectralPolynomial, ArrayD<f64>, ArrayD<f64>)>,
    pub(super) profile_reports: Option<BTreeMap<Vec<usize>, ProfileReport>>,
}

impl ProfileResults {
    pub(super) fn new(
        shape: Vec<usize>,
        models: &ProfileModels,
        automatic: usize,
        polynomial: bool,
        log_details: bool,
    ) -> Self {
        let count = models.singlets.len()
            + models.multiplets.len()
            + automatic
            + usize::from(polynomial)
            + usize::from(models.spectral_polynomial.is_some());
        let gaussian_count = automatic
            + models
                .singlets
                .iter()
                .filter(|line| matches!(line.kind, LineKind::Gaussian))
                .count();
        let lorentzian_count = models.singlets.len() + automatic - gaussian_count;
        let gaussian = LineResults::new(&shape, gaussian_count, "Gaussian singlets results");
        let lorentzian = LineResults::new(&shape, lorentzian_count, "Lorentzian singlets");
        let multiplets = models
            .multiplets
            .iter()
            .enumerate()
            .map(|(index, multiplet)| {
                LineResults::new(
                    &shape,
                    multiplet.components.len(),
                    &format!("Gaussian multiplet number {} results", index + 1),
                )
            })
            .collect();
        let spectral = models.spectral_polynomial.as_ref().map(|model| {
            let dimensions = with_axis(&shape, model.values.len());
            let sizes = IxDyn(&dimensions);
            (
                model.kind,
                ArrayD::from_elem(sizes.clone(), f64::NAN),
                ArrayD::from_elem(sizes, f64::NAN),
            )
        });
        let size = IxDyn(&shape);
        Self {
            attempted: ArrayD::from_elem(size.clone(), false),
            succeeded: ArrayD::from_elem(size.clone(), false),
            converged: ArrayD::from_elem(size.clone(), false),
            valid: ArrayD::from_elem(size.clone(), false),
            iterations: ArrayD::from_elem(size.clone(), -1),
            components: ArrayD::from_elem(size.clone(), -1),
            direction: ArrayD::from_elem(size, String::new()),
            model_types: ArrayD::from_elem(IxDyn(&with_axis(&shape, count)), "UNDEF".to_owned()),
            gaussian,
            lorentzian,
            multiplets,
            spectral,
            profile_reports: log_details.then(BTreeMap::new),
        }
    }

    pub(super) fn direction(&mut self, pixel: &[usize], direction: String) {
        self.direction[IxDyn(pixel)] = direction;
    }

    pub(super) fn record_fit(
        &mut self,
        pixel: &[usize],
        world_pixel: &[f64],
        axis: &ProfileAxis<'_>,
        curve: &Curve,
        solution: &Solution,
        valid: bool,
    ) -> Result<(), crate::ImageError> {
        self.succeeded[IxDyn(pixel)] = true;
        self.converged[IxDyn(pixel)] = solution.converged;
        if solution.converged
            && let Some(report) = self
                .profile_reports
                .as_mut()
                .and_then(|reports| reports.get_mut(pixel))
        {
            report.iterations = Some(solution.iterations);
        }
        if !valid || !solution.converged {
            return Ok(());
        }
        self.valid[IxDyn(pixel)] = true;
        self.iterations[IxDyn(pixel)] = solution.iterations as i64;
        let increment = axis.signed_increment(world_pixel)?.abs();
        let mut index = pixel.to_vec();
        index.push(0);
        let mut types = 0;
        let mut gaussian = 0;
        let mut lorentzian = 0;
        let mut last_group = None;
        let mut multi_index = 0;
        let mut report_lines = self.profile_reports.is_some().then(Vec::new);
        for line in &curve.lines {
            let (field, kind) = if let Some(group) = line.multiplet {
                if last_group != Some(group) {
                    multi_index = 0;
                }
                let index = multi_index;
                multi_index += 1;
                (index, "GAUSSIAN MULTIPLET")
            } else if matches!(line.kind, LineKind::Gaussian) {
                let index = gaussian;
                gaussian += 1;
                (index, "GAUSSIAN")
            } else {
                let index = lorentzian;
                lorentzian += 1;
                (index, "LORENTZIAN")
            };
            if line.multiplet.is_none() || line.multiplet != last_group {
                self.store_type(pixel, types, kind);
                types += 1;
            }
            last_group = line.multiplet;
            let record = match line.multiplet {
                Some(group) => &mut self.multiplets[group],
                None if matches!(line.kind, LineKind::Gaussian) => &mut self.gaussian,
                None => &mut self.lorentzian,
            };
            let [peak, center, width] = line.parameters(&solution.values);
            let [peak_error, center_error, width_error] = line.errors(&solution.errors);
            let integral_error = line
                .integral(peak_error, width)
                .hypot(line.integral(peak, width_error))
                * increment;
            *index.last_mut().expect("parameter index") = field;
            let center_world = [axis.world(world_pixel, center)?, center_error * increment];
            let fwhm_world = [width.abs() * increment, width_error * increment];
            let integral = [line.integral(peak, width) * increment, integral_error];
            record.set(
                &index,
                [
                    peak,
                    peak_error,
                    center_world[0],
                    center_world[1],
                    fwhm_world[0],
                    fwhm_world[1],
                    integral[0],
                    integral[1],
                ],
            );
            if let Some(report_lines) = &mut report_lines {
                let pixels = axis.report_pixels(center, center_world, fwhm_world, increment);
                report_lines.push(ProfileLineReport {
                    kind,
                    component: types - 1,
                    subcomponent: line.multiplet.map(|_| field),
                    peak: [peak, peak_error],
                    center: center_world,
                    pixel_center: pixels.map(|(center, _)| center),
                    fwhm: fwhm_world,
                    pixel_fwhm: pixels.map(|(_, width)| width),
                    channel_fwhm: width.abs(),
                    integral,
                    fixed: line.fixed(&curve.fixed),
                });
            }
        }
        if let Some((report, lines)) = self
            .profile_reports
            .as_mut()
            .and_then(|reports| reports.get_mut(pixel))
            .zip(report_lines)
        {
            report.lines = lines;
        }
        if let Some((kind, range)) = &curve.spectral {
            if let Some((_, values, errors)) = &mut self.spectral {
                for (field, parameter) in range.clone().enumerate() {
                    *index.last_mut().expect("coefficient index") = field;
                    values[IxDyn(&index)] = solution.values[parameter];
                    errors[IxDyn(&index)] = solution.errors[parameter];
                }
            }
            let label = match kind {
                SpectralPolynomial::PowerLog => "POWER LOGARITHMIC POLYNOMIAL",
                SpectralPolynomial::LogTransformed => "LOGARITHMIC TRANSFORMED POLYNOMIAL",
            };
            self.store_type(pixel, types, label);
            types += 1;
        }
        if curve.polynomial.is_some() {
            self.store_type(pixel, types, "POLYNOMIAL");
            types += 1;
        }
        self.components[IxDyn(pixel)] = types as i64;
        Ok(())
    }

    fn store_type(&mut self, pixel: &[usize], column: usize, value: &str) {
        let mut index = pixel.to_vec();
        index.push(column);
        self.model_types[IxDyn(&index)] = value.to_owned();
    }

    pub(super) fn finish(self, x_unit: &str, y_unit: &str, has_direction: bool) -> RecordValue {
        let mut result = RecordValue::default();
        for (name, value) in [
            ("attempted", ArrayValue::Bool(self.attempted)),
            ("succeeded", ArrayValue::Bool(self.succeeded)),
            ("converged", ArrayValue::Bool(self.converged)),
            ("valid", ArrayValue::Bool(self.valid)),
            ("niter", ArrayValue::Int64(self.iterations)),
            ("ncomps", ArrayValue::Int64(self.components)),
            ("type", ArrayValue::String(self.model_types)),
        ] {
            result.upsert(name, Value::Array(value));
        }
        if has_direction {
            result.upsert(
                "direction",
                Value::Array(ArrayValue::String(self.direction)),
            );
        }
        result.upsert("xUnit", text(x_unit));
        result.upsert("yUnit", text(y_unit));
        if let Some(record) = self.gaussian.record() {
            result.upsert("gs", Value::Record(record));
        }
        if let Some(record) = self.lorentzian.record() {
            result.upsert("ls", Value::Record(record));
        }
        for (index, results) in self.multiplets.into_iter().enumerate() {
            if let Some(record) = results.record() {
                result.upsert(format!("gm{index}"), Value::Record(record));
            }
        }
        if let Some((kind, values, errors)) = self.spectral {
            let mut spectrum = RecordValue::default();
            spectrum.upsert("solution", Value::Array(ArrayValue::Float64(values)));
            spectrum.upsert("error", Value::Array(ArrayValue::Float64(errors)));
            let label = match kind {
                SpectralPolynomial::PowerLog => "plp",
                SpectralPolynomial::LogTransformed => "ltp",
            };
            result.upsert(label, Value::Record(spectrum));
        }
        result
    }
}

struct LineResults {
    description: String,
    arrays: [ArrayD<f64>; 8],
}

impl LineResults {
    fn new(shape: &[usize], count: usize, description: &str) -> Self {
        let extents = with_axis(shape, count);
        let arrays = std::array::from_fn(|_| ArrayD::from_elem(IxDyn(&extents), f64::NAN));
        Self {
            description: description.to_owned(),
            arrays,
        }
    }

    fn set(&mut self, index: &[usize], values: [f64; 8]) {
        for (array, value) in self.arrays.iter_mut().zip(values) {
            array[IxDyn(index)] = value;
        }
    }

    fn record(self) -> Option<RecordValue> {
        if self.arrays[0].is_empty() {
            return None;
        }
        let mut result = RecordValue::default();
        for (name, array) in NAMES.into_iter().zip(self.arrays) {
            result.upsert(name, Value::Array(ArrayValue::Float64(array)));
        }
        result.upsert("description", text(&self.description));
        Some(result)
    }
}

fn with_axis(shape: &[usize], count: usize) -> Vec<usize> {
    let mut result = shape.to_vec();
    result.push(count);
    result
}

fn text(value: &str) -> Value {
    Value::Scalar(ScalarValue::String(value.to_owned()))
}
