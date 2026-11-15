// SPDX-License-Identifier: LGPL-3.0-or-later
//! Project discovery and the tclean call made by simutil.imtclean.
use super::{Parameters, failure};
use crate::{
    SessionResources,
    simobserve::SimobserveError,
    simutil::{analysis, qa},
};
use casa_coordinates::{Coordinate, CoordinateModel};
use casa_images::PagedImage;
use casa_ms::columns::uvw_column::UvwColumn;
use casa_ms::{MeasurementSet, SubtableId};
use casa_types::{ScalarValue, quanta::Quantity};
use serde_json::{Map, Value, json};
use std::path::{Path, PathBuf};

pub(super) struct Plan {
    pub directory: PathBuf,
    pub stem: String,
    pub root: PathBuf,
    pub model: PathBuf,
    pub ms: PathBuf,
    pub imaging: Map<String, Value>,
}
impl Plan {
    pub(super) fn file(&self, suffix: &str) -> PathBuf {
        suffixed(&self.root, suffix)
    }
    pub(super) fn analysis_bytes(&self) -> Result<u64, SimobserveError> {
        let cells = |path: &Path| -> Result<u64, SimobserveError> {
            let image = PagedImage::<f32>::open(path).map_err(failure)?;
            image.shape().iter().try_fold(1u64, |count, &axis| {
                count
                    .checked_mul(axis as u64)
                    .ok_or_else(|| failure("image size overflows"))
            })
        };
        analysis_memory(cells(&self.model)?, cells(&self.file(".image"))?)
    }
    pub(super) fn products(&self) -> Result<Vec<PathBuf>, SimobserveError> {
        let mut paths: Vec<_> = [
            ".psf",
            ".model",
            ".residual",
            ".image",
            ".image.pbcor",
            ".pb",
            ".sumwt",
            ".mask",
            ".image.flat",
            ".diff",
            ".fidelity",
        ]
        .iter()
        .map(|suffix| self.file(suffix))
        .filter(|path| path.exists())
        .collect();
        let flat = suffixed(&self.model, ".flat");
        for path in [
            flat.clone(),
            suffixed(&flat, ".regrid"),
            suffixed(&flat, ".regrid.conv"),
        ] {
            if path.exists() && !paths.contains(&path) {
                paths.push(path);
            }
        }
        paths.sort();
        Ok(paths)
    }
}

// Peak bound for flat-image/regrid/convolution and fidelity statistics.
// Source: float input, flattened products, masks and interpolation work.
// Output: retained regrid, convolution's complex FFT work, masks, difference,
// fidelity and robust-statistic Float64 work. These phases run sequentially;
// charging their sum is conservative. Table/cache headroom is fixed.
fn analysis_memory(source: u64, output: u64) -> Result<u64, SimobserveError> {
    source
        .checked_mul(48)
        .and_then(|bytes| {
            output
                .checked_mul(96)
                .and_then(|out| bytes.checked_add(out))
        })
        .and_then(|bytes| bytes.checked_add(128 << 20))
        .ok_or_else(|| failure("analysis memory overflows"))
}

#[cfg(test)]
mod tests {
    use super::analysis_memory;

    #[test]
    fn analysis_counting_law_scales_with_both_image_grids_and_refuses_overflow() {
        let base = analysis_memory(0, 0).unwrap();
        for cells in [1, 65_536, 16_777_216] {
            assert_eq!(analysis_memory(cells, 0).unwrap() - base, cells * 48);
            assert_eq!(analysis_memory(0, cells).unwrap() - base, cells * 96);
            assert_eq!(analysis_memory(cells, cells).unwrap() - base, cells * 144);
        }
        assert!(analysis_memory(u64::MAX, 1).is_err());
        assert!(analysis_memory(1, u64::MAX).is_err());
    }
}

pub(super) fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}
fn files(directory: &Path) -> Result<Vec<PathBuf>, SimobserveError> {
    let mut files = std::fs::read_dir(directory)
        .map_err(failure)?
        .map(|e| e.map(|e| e.path()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(failure)?;
    files.sort();
    Ok(files)
}
fn locate(directory: &Path, input: &str) -> PathBuf {
    let in_project = directory.join(input);
    if in_project.exists() {
        in_project
    } else {
        PathBuf::from(input)
    }
}

pub(super) fn prepare(
    p: &Parameters<'_>,
    resources: SessionResources,
) -> Result<Plan, SimobserveError> {
    if !p.text("logfile").is_empty() {
        return Err(failure("custom simulation log files remain on #841"));
    }
    let directory = PathBuf::from(p.text("project"));
    if !directory.is_dir() {
        return Err(failure(format!(
            "{} directory doesn't exist - the task expects to find results from creating the datasets there, like the skymodel.",
            directory.display()
        )));
    }
    let candidates = files(&directory)?;
    let inputs = p.strings("vis");
    let mut sets = if inputs.is_empty() || inputs == ["default"] {
        candidates
            .iter()
            .filter(|path| path.to_string_lossy().ends_with(".ms"))
            .filter(|path| {
                let name = path.file_name().unwrap().to_string_lossy();
                name.contains("noisy") || !directory.join(name.replace(".ms", ".noisy.ms")).exists()
            })
            .cloned()
            .collect::<Vec<_>>()
    } else {
        inputs
            .iter()
            .flat_map(|v| v.split(','))
            .map(|s| locate(&directory, s.trim()))
            .collect()
    };
    sets.retain(|path| path.exists());
    if sets.len() != 1 {
        return Err(failure(if sets.is_empty() {
            "No MS found to image"
        } else {
            "multiple-MS simulation imaging requires concat (#939)"
        }));
    }
    let ms = sets.remove(0);
    if analysis::is_total_power(&ms).map_err(failure)? {
        return Err(failure(
            "total-power simulation imaging requires tsdimaging (#938)",
        ));
    }
    if !p.text("featherimage").is_empty() {
        return Err(failure("simulation feather integration remains on #841"));
    }
    let stem = if p.flag("image") {
        ms.file_name()
            .unwrap()
            .to_string_lossy()
            .trim_end_matches(".ms")
            .to_owned()
    } else if matches!(p.text("imagename"), "" | "default") {
        candidates
            .iter()
            .find(|p| p.to_string_lossy().ends_with(".image"))
            .and_then(|p| p.file_name())
            .ok_or_else(|| failure("can't find any image in project directory"))?
            .to_string_lossy()
            .trim_end_matches(".image")
            .to_owned()
    } else {
        Path::new(p.text("imagename"))
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .trim_end_matches(".image")
            .to_owned()
    };
    let root = directory.join(&stem);
    let mut models: Vec<_> = candidates
        .iter()
        .filter(|path| {
            [".skymodel", ".newmodel", ".compskymodel"]
                .iter()
                .any(|suffix| path.to_string_lossy().ends_with(suffix))
        })
        .cloned()
        .collect();
    models.sort_by_key(|path| {
        (
            !path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains(stem.trim_end_matches(".noisy")),
            path.clone(),
        )
    });
    let model = if !p.text("skymodel").is_empty() {
        locate(&directory, p.text("skymodel"))
    } else {
        models
            .into_iter()
            .next()
            .ok_or_else(|| failure("Can't find a model image in your project directory"))?
    };
    let imaging = imaging(p, &ms, &root, &model, &directory, resources)?;
    Ok(Plan {
        directory,
        stem,
        root,
        model,
        ms,
        imaging,
    })
}

fn imaging(
    p: &Parameters<'_>,
    ms_path: &Path,
    root: &Path,
    model_path: &Path,
    directory: &Path,
    resources: SessionResources,
) -> Result<Map<String, Value>, SimobserveError> {
    let model = PagedImage::<f32>::open(model_path).map_err(failure)?;
    let model_cell = analysis::cell_size(model.coordinates()).map_err(failure)?;
    let CoordinateModel::Direction(direction) = model.coordinates().coordinate(0) else {
        return Err(failure("model has no direction axes"));
    };
    let world = direction
        .to_world(&[model.shape()[0] as f64 / 2., model.shape()[1] as f64 / 2.])
        .map_err(failure)?;
    let refdir = format!(
        "{} {} {}",
        direction.direction_ref().as_str(),
        qa::hms(&Quantity::new(world[0], "rad").map_err(failure)?).map_err(failure)?,
        qa::dms(&Quantity::new(world[1], "rad").map_err(failure)?).map_err(failure)?
    );
    let provided = p.strings("cell");
    let cell = if provided.is_empty() || provided[0].is_empty() {
        model_cell
    } else {
        [0, 1]
            .map(|a| qa::value_in(&qa::read(&provided[a.min(provided.len() - 1)])?, "rad"))
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(failure)?
            .try_into()
            .expect("two cells")
    };
    let mut size: Vec<_> = p.0["imsize"]
        .as_array()
        .expect("resolved imsize")
        .iter()
        .map(|v| v.as_i64().expect("integer"))
        .collect();
    if size.is_empty() {
        size.push(0);
    }
    if size.len() == 1 {
        size.push(size[0]);
    }
    for axis in 0..2 {
        if size[axis] <= 0 {
            size[axis] = (model.shape()[axis] as f64 * model_cell[axis] / cell[axis]).ceil() as i64;
        }
    }
    let frequency = model.coordinates().coordinate(2).reference_value()[0];
    let minimum = minimum_size(ms_path, cell[0], frequency, resources)?;
    let size =
        [0, 1].map(|a| casa_imaging_model::optimum_image_size((size[a] as usize).max(minimum)));
    let ms = MeasurementSet::open(ms_path).map_err(failure)?;
    let source = ms
        .subtable(SubtableId::Source)
        .ok_or_else(|| failure("MS has no SOURCE table"))?;
    let codes = source.column_accessor("CODE").map_err(failure)?;
    let fields = (0..source.row_count())
        .filter(|&row| matches!(codes.scalar_cell(row), Ok(ScalarValue::String(v)) if v=="OBJ"))
        .count();
    let startmodel = p.text("modelimage").replace("$project", p.text("project"));
    let startmodel = locate(directory, &startmodel);
    let nchan = ms
        .spectral_window()
        .map_err(failure)?
        .num_chan(0)
        .map_err(failure)?;
    Ok(json!({ "vis": ms_path, "selectdata": false, "imagename": root,
        "cell": cell.map(|v|format!("{}rad",v.abs())), "imsize": size,
        "phasecenter": if p.text("imdirection").is_empty() {refdir.as_str()} else {p.text("imdirection")},
        "stokes": p.text("stokes"), "startmodel": if startmodel.exists() && !p.text("modelimage").is_empty() {startmodel.to_string_lossy().into_owned()} else {String::new()},
        "specmode": if nchan==1 {"mfs"} else {"cube"}, "gridder": if fields==1 {"standard"} else {"mosaic"},
        "deconvolver": "clark", "weighting": p.text("weighting"), "niter": p.0["niter"],
        "threshold": if p.0["niter"].as_i64().unwrap_or(0)>0 {p.text("threshold")} else {""},
        "interactive": p.flag("interactive"), "usemask": "user", "mask": p.0["mask"],
        "uvtaper": p.0["outertaper"], "pbcor": p.flag("pbcor"), "pbmask": 0.,
        "restoringbeam": "common", "savemodel": "none", "calcres": true, "calcpsf": true,
        "restoration": true, "restart": true, "fastnoise": true, "parallel": false
    }).as_object().unwrap().clone())
}

fn minimum_size(
    path: &Path,
    cell: f64,
    frequency: f64,
    resources: SessionResources,
) -> Result<usize, SimobserveError> {
    casa_imaging_runtime::charge_process_caches(resources.host(), resources.policy())
        .map_err(failure)?;
    let ms = MeasurementSet::open(path).map_err(failure)?;
    let uvw = UvwColumn::new(ms.main_table());
    let mut maximum = 0f64;
    for row in 0..ms.main_table().row_count() {
        let [u, v, _] = uvw.get(row).map_err(failure)?;
        maximum = maximum.max(u).max(v);
    }
    if maximum <= 0. {
        return Err(failure("an interferometric image needs positive u or v"));
    }
    Ok(8 * (300_000_000. / frequency / maximum / cell.abs()).floor() as usize)
}
