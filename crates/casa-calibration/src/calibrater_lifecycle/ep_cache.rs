// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's one-W-plane legacy EP convolution cache.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::str::SplitWhitespace;

use casa_coordinates::{
    CoordinateSystem, CoordinateType, LinearCoordinate, StokesCoordinate, StokesType,
};
use casa_images::{ImagePixel, PagedImage};
use casa_imaging_operator::LegacyEpConvolution;
use casa_imaging_runtime::{Demand, HostResources, Reservation, ResourcePolicy, admit};
use casa_ms::{MsError, MsResult};
use casa_types::Complex32;
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

const AUX: &str = "aux.dat";
const BEAM: &str = "avgPB";
const STAGE_PREFIX: &str = ".ep-cache-";
const JOURNAL: &str = "pending-slot";

pub(super) struct EpCache {
    root: PathBuf,
}

pub(super) struct EpCacheProducts {
    pub average_beam: ArrayD<f32>,
    pub convolution: ArrayD<Complex32>,
    pub weights: ArrayD<Complex32>,
    pub support: usize,
    pub sampling: usize,
    _memory: Reservation,
}

struct Slot {
    angle_deg: f64,
    support: usize,
    sampling: usize,
}

impl EpCache {
    pub(super) fn open(path: &Path) -> MsResult<Self> {
        if path.as_os_str().is_empty() {
            return Err(invalid(
                "Got null string for disk cache dir. in ConvFuncDiskCache::initCache()",
            ));
        }
        std::fs::create_dir_all(path).map_err(invalid)?;
        let cache = Self {
            root: path.canonicalize().map_err(invalid)?,
        };
        cache.slots()?;
        Ok(cache)
    }

    pub(super) fn path(&self) -> &Path {
        &self.root
    }

    fn slots(&self) -> MsResult<Vec<Slot>> {
        let path = self.root.join(AUX);
        match std::fs::read_to_string(&path) {
            Ok(document) => decode_slots(&document),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(error) => Err(invalid(format!("cannot read {}: {error}", path.display()))),
        }
    }

    pub(super) fn read(
        &self,
        host: &HostResources,
        angle_rad: f64,
        maximum_gap_deg: f64,
        shape: &[usize],
        correlations: &[StokesType],
    ) -> MsResult<Option<EpCacheProducts>> {
        let slots = self.slots()?;
        let angle = angle_rad.to_degrees();
        let matching = slots
            .iter()
            .enumerate()
            .map(|(index, slot)| ((slot.angle_deg - angle).abs(), index, slot))
            .filter(|(gap, _, _)| *gap <= maximum_gap_deg)
            .min_by(|left, right| left.0.total_cmp(&right.0));
        let Some((_, index, slot)) = matching else {
            return Ok(None);
        };
        let convolution = self.kernel("CF", index, slot, correlations.len())?;
        let weights = self.kernel("CFWT", index, slot, correlations.len())?;
        let bytes = |image: &PagedImage<Complex32>| {
            image
                .shape()
                .iter()
                .fold(size_of::<Complex32>() as u64, |size, &length| {
                    size.saturating_mul(length as u64)
                })
        };
        let memory = admit(
            host,
            &ResourcePolicy::Balanced,
            &Demand {
                phase: "legacy EP cached convolutions",
                memory: bytes(&convolution).saturating_add(bytes(&weights)),
            },
        )
        .map_err(invalid)?;
        let convolution = read_kernel(&convolution)?;
        let weights = read_kernel(&weights)?;
        let beam = PagedImage::<f32>::open(self.root.join(BEAM)).map_err(invalid)?;
        if shape != beam.shape() {
            return Err(invalid(
                "legacy EP cache average beam has a different image shape",
            ));
        }
        let average_beam = beam.get().map_err(invalid)?;
        if average_beam.iter().any(|value| !value.is_finite())
            || !average_beam.iter().any(|value| *value >= 1e-4)
        {
            return Err(invalid(
                "legacy EP cache average beam contains invalid values",
            ));
        }
        Ok(Some(EpCacheProducts {
            average_beam,
            convolution,
            weights,
            support: slot.support,
            sampling: slot.sampling,
            _memory: memory,
        }))
    }

    fn kernel(
        &self,
        prefix: &str,
        index: usize,
        slot: &Slot,
        pols: usize,
    ) -> MsResult<PagedImage<Complex32>> {
        let path = self.root.join(format!("{prefix}0_{index}"));
        let image = PagedImage::<Complex32>::open(&path).map_err(invalid)?;
        let span = slot
            .support
            .checked_mul(slot.sampling)
            .and_then(|span| span.checked_mul(2));
        let [nx, ny, count, 1] = *image.shape() else {
            return Err(invalid(format!(
                "invalid legacy EP cache kernel {}",
                path.display()
            )));
        };
        if nx != ny || count != pols || span.is_none_or(|span| span >= nx) {
            return Err(invalid(format!(
                "invalid legacy EP cache kernel {}",
                path.display()
            )));
        }
        Ok(image)
    }

    pub(super) fn publish(
        &self,
        angle_rad: f64,
        cell_rad: [f64; 2],
        beam_coordinates: &CoordinateSystem,
        shape: [usize; 4],
        correlations: &[StokesType],
        products: &LegacyEpConvolution,
    ) -> MsResult<()> {
        let mut slots = self.slots()?;
        let index = slots.len();
        let names = product_names(index);
        let staged = tempfile::Builder::new()
            .prefix(STAGE_PREFIX)
            .tempdir_in(&self.root)
            .map_err(invalid)?;
        let mut kernel_coordinates = beam_coordinates.clone();
        let linear =
            LinearCoordinate::new(2, vec!["UU".into(), "VV".into()], vec!["lambda".into(); 2])
                .with_reference_pixel(vec![1024.; 2])
                .with_increment(
                    cell_rad
                        .map(|cell| 1. / (cell * (products.sampling * shape[0]) as f64))
                        .to_vec(),
                );
        let axis = kernel_coordinates
            .find_coordinate(CoordinateType::Direction)
            .ok_or_else(|| invalid("legacy EP model is missing the direction axes"))?;
        kernel_coordinates.replace_coordinate(axis, linear);
        let pol = kernel_coordinates
            .find_coordinate(CoordinateType::Stokes)
            .ok_or_else(|| invalid("legacy EP model is missing the polarization axis"))?;
        kernel_coordinates.replace_coordinate(pol, StokesCoordinate::new(correlations.to_vec()));
        for (name, kernel) in names[..2].iter().zip(&products.kernels) {
            let side = products.kernel_side;
            let shape = [side, side, correlations.len(), 1];
            let pixels = ArrayD::from_shape_fn(IxDyn(&shape).f(), |i| kernel[i[0] + side * i[1]]);
            save_image(staged.path().join(name), kernel_coordinates.clone(), pixels)?;
        }
        if index == 0 {
            let pixels = ArrayD::from_shape_fn(IxDyn(&shape).f(), |i| {
                products.average_beam[i[0] + shape[0] * i[1]]
            });
            save_image(staged.path().join(BEAM), beam_coordinates.clone(), pixels)?;
        } else {
            let beam = PagedImage::<f32>::open(self.root.join(BEAM)).map_err(invalid)?;
            if beam.shape() != shape {
                return Err(invalid(
                    "legacy EP cache average beam has a different image shape",
                ));
            }
        }
        let angle = format!("{:.4}", angle_rad.to_degrees());
        slots.push(Slot {
            angle_deg: angle.parse().map_err(invalid)?,
            support: products.support,
            sampling: products.sampling,
        });
        let mut manifest = format!("{} 1\n", slots.len());
        for slot in slots {
            writeln!(
                manifest,
                "{} {} {}  {}",
                slot.angle_deg, slot.support, slot.support, slot.sampling
            )
            .expect("writing to a String cannot fail");
        }
        std::fs::write(staged.path().join(AUX), manifest).map_err(invalid)?;
        self.publish_files(staged.path(), index, &names)
    }

    /// Clean up a previously abandoned publication while holding the cache's
    /// exclusive lock. Only a stage with a journal can have changed the root.
    pub(super) fn recover(&self) -> MsResult<()> {
        let committed = self.slots()?.len();
        for entry in std::fs::read_dir(&self.root).map_err(invalid)? {
            let entry = entry.map_err(invalid)?;
            if !entry
                .file_name()
                .to_string_lossy()
                .starts_with(STAGE_PREFIX)
                || !entry.file_type().map_err(invalid)?.is_dir()
            {
                continue;
            }
            let stage = entry.path();
            let marker = stage.join(JOURNAL);
            let document = match std::fs::read_to_string(marker) {
                Ok(document) => document,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(invalid(error)),
            };
            let index: usize = document.parse().map_err(invalid)?;
            if committed == index {
                self.rollback(&stage, &product_names(index))?;
            } else if committed < index {
                return Err(invalid(
                    "legacy EP cache manifest predates its publication journal",
                ));
            }
            std::fs::remove_dir_all(stage).map_err(invalid)?;
        }
        Ok(())
    }

    fn rollback(&self, stage: &Path, names: &[String]) -> MsResult<()> {
        for name in names {
            let target = self.root.join(name);
            // A product still present in the stage was never promoted.
            if !stage.join(name).exists() && target.is_dir() {
                std::fs::remove_dir_all(target).map_err(invalid)?;
            }
        }
        Ok(())
    }

    fn publish_files(&self, staged: &Path, index: usize, names: &[String]) -> MsResult<()> {
        for name in names {
            let target = self.root.join(name);
            if target.exists() {
                return Err(invalid(format!(
                    "legacy EP cache already contains {}",
                    target.display()
                )));
            }
        }
        std::fs::write(staged.join(JOURNAL), index.to_string()).map_err(invalid)?;
        let published = (|| -> std::io::Result<()> {
            for name in names {
                std::fs::rename(staged.join(name), self.root.join(name))?;
            }
            // Readers see the previous or complete new slot list.
            std::fs::rename(staged.join(AUX), self.root.join(AUX))
        })();
        if let Err(error) = published {
            self.rollback(staged, names)?;
            return Err(invalid(error));
        }
        Ok(())
    }
}

fn read_kernel(image: &PagedImage<Complex32>) -> MsResult<ArrayD<Complex32>> {
    let kernels = image.get().map_err(invalid)?;
    if kernels
        .iter()
        .any(|v| !v.re.is_finite() || !v.im.is_finite())
        || !kernels.iter().any(|v| v.re != 0. || v.im != 0.)
    {
        return Err(invalid("invalid sample in legacy EP cache kernel"));
    }
    Ok(kernels)
}

fn product_names(index: usize) -> Vec<String> {
    let mut names = vec![format!("CF0_{index}"), format!("CFWT0_{index}")];
    if index == 0 {
        names.push(BEAM.to_owned());
    }
    names
}

fn save_image<T: ImagePixel>(
    path: PathBuf,
    coordinates: CoordinateSystem,
    pixels: ArrayD<T>,
) -> MsResult<()> {
    let mut image =
        PagedImage::create(pixels.shape().to_vec(), coordinates, path).map_err(invalid)?;
    image.put_slice(&pixels, &[0; 4]).map_err(invalid)?;
    image.save().map_err(invalid)
}

fn decode_slots(document: &str) -> MsResult<Vec<Slot>> {
    let mut fields = document.split_whitespace();
    let slots = parse::<usize>(&mut fields)?;
    let planes = parse::<usize>(&mut fields)?;
    if planes != 1 {
        return Err(invalid("legacy EP requires a one-plane convolution cache"));
    }
    let mut decoded = Vec::new();
    for _ in 0..slots {
        let angle_deg = parse::<f64>(&mut fields)?;
        let support = parse::<usize>(&mut fields)?;
        let _ = parse::<usize>(&mut fields)?; // CASA reload uses X support for both axes.
        let sampling = parse::<f64>(&mut fields)?;
        if !angle_deg.is_finite()
            || support == 0
            || !(1. ..usize::MAX as f64).contains(&sampling)
            || sampling.fract() != 0.
        {
            return Err(invalid(
                "invalid support or angle in legacy EP cache aux.dat",
            ));
        }
        decoded.push(Slot {
            angle_deg,
            support,
            sampling: sampling as usize,
        });
    }
    if fields.next().is_some() {
        return Err(invalid("extra fields in legacy EP cache aux.dat"));
    }
    Ok(decoded)
}

fn parse<T: std::str::FromStr>(fields: &mut SplitWhitespace<'_>) -> Result<T, MsError> {
    fields
        .next()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| invalid("invalid legacy EP cache aux.dat"))
}

pub(super) fn invalid(error: impl std::fmt::Display) -> MsError {
    MsError::InvalidInput(error.to_string())
}
