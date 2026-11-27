// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's legacy EP convolution cache and its per-W-plane image tables.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::str::SplitWhitespace;

use casa_coordinates::{
    CoordinateSystem, CoordinateType, LinearCoordinate, StokesCoordinate, StokesType,
};
use casa_core::Complex32;
use casa_images::{ImagePixel, PagedImage};
use casa_imaging_operator::LegacyEpConvolution;
use casa_imaging_runtime::{Demand, HostResources, Reservation, ResourcePolicy, admit};
use casa_ms::{MsError, MsResult};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

const AUX: &str = "aux.dat";
const BEAM: &str = "avgPB";
const STAGE_PREFIX: &str = ".ep-cache-";
const PREPARING: &str = "preparing-slot";
const JOURNAL: &str = "pending-slot";
const JOURNAL_TEMP: &str = "pending-slot.new";

pub(crate) struct EpCache {
    root: PathBuf,
}

pub(crate) struct EpCacheProducts {
    pub average_beam: ArrayD<f32>,
    pub convolution: ArrayD<Complex32>,
    pub convolution_coordinates: CoordinateSystem,
    pub weights: ArrayD<Complex32>,
    pub support: Vec<usize>,
    pub sampling: usize,
    pub reference_angle_rad: f64,
    pub w_scale: Option<f64>,
    _memory: Reservation,
}

struct Slot {
    angle_deg: f64,
    support: Vec<usize>,
    sampling: usize,
}

impl EpCache {
    pub(crate) fn open(path: &Path) -> MsResult<Self> {
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

    pub(crate) fn read(
        &self,
        host: &HostResources,
        angle_rad: f64,
        maximum_gap_deg: f64,
        shape: &[usize],
        correlations: &[StokesType],
        planes: usize,
    ) -> MsResult<Option<EpCacheProducts>> {
        if planes == 0 {
            return Err(invalid("legacy EP cache requires a positive W-plane count"));
        }
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
        let support = slot.support.get(..planes).ok_or_else(|| {
            invalid("legacy EP cache contains fewer W planes than the requested transform")
        })?;
        let convolution = self.kernel(
            "CF",
            (index, 0),
            support[0],
            slot.sampling,
            correlations.len(),
        )?;
        let weights = self.kernel(
            "CFWT",
            (index, 0),
            support[0],
            slot.sampling,
            correlations.len(),
        )?;
        let bytes = |image: &PagedImage<Complex32>| {
            image
                .shape()
                .iter()
                .fold(size_of::<Complex32>() as u64, |size, &length| {
                    size.saturating_mul(length as u64)
                })
        };
        let conv_bytes = bytes(&convolution);
        let weight_bytes = bytes(&weights);
        let memory = admit(
            host,
            &ResourcePolicy::Balanced,
            &Demand {
                phase: "legacy EP cached convolutions",
                memory: conv_bytes
                    .saturating_add(weight_bytes)
                    .saturating_mul(planes as u64)
                    .saturating_add(if planes > 1 {
                        conv_bytes.max(weight_bytes)
                    } else {
                        0
                    }),
            },
        )
        .map_err(invalid)?;
        let convolution_coordinates = convolution.coordinates().clone();
        let convolution = self.read_kernels(host, "CF", index, slot, support, convolution)?;
        let weights = self.read_kernels(host, "CFWT", index, slot, support, weights)?;
        let beam = PagedImage::<f32>::open_with_cache(self.root.join(BEAM), 1).map_err(invalid)?;
        if shape != beam.shape() {
            return Err(invalid(
                "legacy EP cache average beam has a different image shape",
            ));
        }
        let average_beam = read_pixels(host, beam)?;
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
            convolution_coordinates,
            weights,
            support: support.to_vec(),
            sampling: slot.sampling,
            reference_angle_rad: slot.angle_deg.to_radians(),
            w_scale: None,
            _memory: memory,
        }))
    }

    fn read_kernels(
        &self,
        host: &HostResources,
        prefix: &str,
        index: usize,
        slot: &Slot,
        support: &[usize],
        first: PagedImage<Complex32>,
    ) -> MsResult<ArrayD<Complex32>> {
        let [nx, ny, pols, 1] = *first.shape() else {
            unreachable!("validated legacy EP cache image");
        };
        let pixels = read_kernel(host, first, support[0] != 0)?;
        if support.len() == 1 {
            return Ok(pixels);
        }
        let mut packed = ArrayD::zeros(IxDyn(&[nx, ny, pols, support.len()]).f());
        let mut planes = packed.axis_iter_mut(ndarray::Axis(3));
        planes
            .next()
            .unwrap()
            .assign(&pixels.index_axis(ndarray::Axis(3), 0));
        drop(pixels);
        for (plane, (&half_width, mut destination)) in
            support.iter().skip(1).zip(planes).enumerate()
        {
            let image = self.kernel(prefix, (index, plane + 1), half_width, slot.sampling, pols)?;
            if image.shape() != [nx, ny, pols, 1] {
                return Err(invalid(
                    "legacy EP cache W planes have different image shapes",
                ));
            }
            let pixels = read_kernel(host, image, half_width != 0)?;
            destination.assign(&pixels.index_axis(ndarray::Axis(3), 0));
        }
        Ok(packed)
    }

    fn kernel(
        &self,
        prefix: &str,
        (index, plane): (usize, usize),
        support: usize,
        sampling: usize,
        pols: usize,
    ) -> MsResult<PagedImage<Complex32>> {
        let path = self.root.join(format!("{prefix}{plane}_{index}"));
        let image = PagedImage::<Complex32>::open_with_cache(&path, 1).map_err(invalid)?;
        let span = support
            .checked_mul(sampling)
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
        host: &HostResources,
        (angle_rad, cell_rad): (f64, [f64; 2]),
        beam_coordinates: &CoordinateSystem,
        shape: [usize; 4],
        correlations: &[StokesType],
        products: &LegacyEpConvolution,
    ) -> MsResult<()> {
        let mut slots = self.slots()?;
        let planes = products.support.len();
        if slots.iter().any(|slot| slot.support.len() != planes) {
            return Err(invalid(
                "the new legacy EP slot has a different W-plane count from the cache",
            ));
        }
        let index = slots.len();
        let names = product_names(index, planes);
        let staged = tempfile::Builder::new()
            .prefix(STAGE_PREFIX)
            .tempdir_in(&self.root)
            .map_err(invalid)?;
        std::fs::File::create(staged.path().join(PREPARING)).map_err(invalid)?;
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
        for (name, kernel) in names.iter().zip(products.kernels.iter().flatten()) {
            let side = products.kernel_side;
            save_image(
                host,
                staged.path().join(name),
                kernel_coordinates.clone(),
                [side, side, correlations.len(), 1],
                |x, y| kernel[x + side * y],
            )?;
        }
        if index == 0 {
            save_image(
                host,
                staged.path().join(BEAM),
                beam_coordinates.clone(),
                shape,
                |x, y| products.average_beam[x + shape[0] * y],
            )?;
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
            support: products.support.clone(),
            sampling: products.sampling,
        });
        let mut manifest = format!("{} {planes}\n", slots.len());
        for slot in slots {
            write!(manifest, "{}", slot.angle_deg).unwrap();
            for width in slot.support {
                write!(manifest, " {width} {width}").unwrap();
            }
            writeln!(manifest, "  {}", slot.sampling).unwrap();
        }
        std::fs::write(staged.path().join(AUX), manifest).map_err(invalid)?;
        self.publish_files(staged.path(), [index, planes], &names)
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
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    if stage.join(PREPARING).is_file() {
                        std::fs::remove_dir_all(stage).map_err(invalid)?;
                    }
                    continue;
                }
                Err(error) => return Err(invalid(error)),
            };
            // An older writer could be interrupted while writing the marker.
            // Promotions start only after a complete marker has been written.
            if document.trim().is_empty() {
                std::fs::remove_dir_all(stage).map_err(invalid)?;
                continue;
            }
            let mut fields = document.split_whitespace();
            let index: usize = parse(&mut fields)?;
            let planes = fields.next().map_or(Ok(1), str::parse).map_err(invalid)?;
            if !(1..=usize::from(u16::MAX)).contains(&planes) || fields.next().is_some() {
                return Err(invalid("invalid legacy EP cache publication journal"));
            }
            if committed == index {
                self.rollback(&stage, &product_names(index, planes))?;
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

    fn publish_files(
        &self,
        staged: &Path,
        [index, planes]: [usize; 2],
        names: &[String],
    ) -> MsResult<()> {
        for name in names {
            let target = self.root.join(name);
            if target.exists() {
                return Err(invalid(format!(
                    "legacy EP cache already contains {}",
                    target.display()
                )));
            }
        }
        std::fs::write(staged.join(JOURNAL_TEMP), format!("{index} {planes}")).map_err(invalid)?;
        std::fs::rename(staged.join(JOURNAL_TEMP), staged.join(JOURNAL)).map_err(invalid)?;
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

fn read_pixels<T: ImagePixel>(host: &HostResources, image: PagedImage<T>) -> MsResult<ArrayD<T>> {
    let tile = image
        .tile_shape()
        .iter()
        .fold(size_of::<T>() as u64, |bytes, &axis| {
            bytes.saturating_mul(axis as u64)
        });
    let _tile_memory = admit(
        host,
        &ResourcePolicy::Balanced,
        &Demand {
            phase: "legacy EP kernel tile buffers",
            // One cached tile plus transient raw bytes and decoding scratch.
            memory: tile.saturating_mul(3),
        },
    )
    .map_err(invalid)?;
    let pixels = image.get().map_err(invalid);
    drop(image);
    pixels
}

fn read_kernel(
    host: &HostResources,
    image: PagedImage<Complex32>,
    nonzero: bool,
) -> MsResult<ArrayD<Complex32>> {
    let kernels = read_pixels(host, image)?;
    if kernels
        .iter()
        .any(|v| !v.re.is_finite() || !v.im.is_finite())
        || (nonzero && !kernels.iter().any(|v| v.re != 0. || v.im != 0.))
    {
        return Err(invalid("invalid sample in legacy EP cache kernel"));
    }
    Ok(kernels)
}

fn product_names(index: usize, planes: usize) -> Vec<String> {
    let mut names = (0..planes)
        .flat_map(|plane| [format!("CF{plane}_{index}"), format!("CFWT{plane}_{index}")])
        .collect::<Vec<_>>();
    if index == 0 {
        names.push(BEAM.to_owned());
    }
    names
}

fn save_image<T: ImagePixel>(
    host: &HostResources,
    path: PathBuf,
    coordinates: CoordinateSystem,
    shape: [usize; 4],
    sample: impl Fn(usize, usize) -> T,
) -> MsResult<()> {
    let (tile, memory) = image_write_layout::<T>(shape);
    let _memory = admit(
        host,
        &ResourcePolicy::Balanced,
        &Demand {
            phase: "legacy EP cache publication",
            memory,
        },
    )
    .map_err(invalid)?;
    let [nx, ny, pols, _] = shape;
    let mut image = PagedImage::create_with_tile_shape_and_cache(
        shape.to_vec(),
        tile.to_vec(),
        coordinates,
        path,
        1,
    )
    .map_err(invalid)?;
    let pixels = ArrayD::from_shape_fn(IxDyn(&[nx, ny, 1, 1]).f(), |i| sample(i[0], i[1]));
    for pol in 0..pols {
        image
            .put_slice_view(pixels.view(), &[0, 0, pol, 0])
            .map_err(invalid)?;
    }
    image.save().map_err(invalid)
}

fn image_write_layout<T: ImagePixel>([nx, ny, pols, _]: [usize; 4]) -> ([usize; 4], u64) {
    let tile = [nx.min(64), ny.min(64), 1, 1];
    let tile_bytes = (tile[0] * tile[1] * size_of::<T>()) as u64;
    let tiles = (nx.div_ceil(tile[0]) * ny.div_ceil(tile[1]) * pols) as u64;
    let plane_bytes = (nx as u64)
        .saturating_mul(ny as u64)
        .saturating_mul(size_of::<T>() as u64);
    // Include the image table and logtable schemas, coordinates and headers
    // even when the image fits in one tile.
    let bytes = plane_bytes
        .saturating_mul(2)
        .saturating_add(3 * tile_bytes)
        .saturating_add(32 * tiles)
        .saturating_add(256 << 10);
    (tile, bytes)
}

fn decode_slots(document: &str) -> MsResult<Vec<Slot>> {
    let mut fields = document.split_whitespace();
    let slots = parse::<usize>(&mut fields)?;
    let planes = parse::<usize>(&mut fields)?;
    if !(1..=usize::from(u16::MAX)).contains(&planes) {
        return Err(invalid("invalid W-plane count in legacy EP cache aux.dat"));
    }
    let mut decoded = Vec::new();
    for _ in 0..slots {
        let angle_deg = parse::<f64>(&mut fields)?;
        let mut support = Vec::new();
        for _ in 0..planes {
            support.push(parse::<usize>(&mut fields)?);
            let _ = parse::<usize>(&mut fields)?; // CASA reload uses X support for both axes.
        }
        let sampling = parse::<f64>(&mut fields)?;
        if !angle_deg.is_finite()
            || (planes == 1 && support[0] == 0)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imaging_memory_test::{cube_coordinates, isolated_test, peak_since, reset_peak};

    #[test]
    fn image_publication_includes_metadata_in_its_charge() {
        if !isolated_test(
            module_path!(),
            "image_publication_includes_metadata_in_its_charge",
        ) {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let host = HostResources::detect().unwrap();
        let beam = [
            StokesType::RR,
            StokesType::RL,
            StokesType::LR,
            StokesType::LL,
        ];
        let coordinates = cube_coordinates(beam.to_vec());
        let shape = [4, 4, 4, 1];
        let baseline = reset_peak();
        save_image(
            &host,
            root.path().join("beam"),
            coordinates,
            shape,
            |_, _| 1_f32,
        )
        .unwrap();
        let actual = peak_since(baseline);
        let admitted = image_write_layout::<f32>(shape).1;
        assert!(
            actual <= admitted,
            "small beam held {actual}, admitted {admitted}"
        );

        let coordinates = cube_coordinates(vec![StokesType::RR, StokesType::LL]);
        let shape = [161, 161, 2, 1];
        let baseline = reset_peak();
        save_image(
            &host,
            root.path().join("kernel"),
            coordinates,
            shape,
            |_, _| Complex32::from(1.),
        )
        .unwrap();
        let actual = peak_since(baseline);
        let admitted = image_write_layout::<Complex32>(shape).1;
        assert!(
            actual <= admitted,
            "kernel held {actual}, admitted {admitted}"
        );
    }
}
