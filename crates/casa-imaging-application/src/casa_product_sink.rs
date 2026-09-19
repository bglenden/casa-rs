// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    collections::BTreeMap,
    ffi::CString,
    fs,
    fs::File,
    path::{Path, PathBuf},
    sync::Mutex,
};

use casa_coordinates::CoordinateSystem;
use casa_images::{GaussianBeam, ImageBeamSet, ImageInfo, ImageType, PagedImage};
use casa_imaging_model::{ImageDomainRole, ProductPixelMask, ProductRole, ProductUnit};
use casa_imaging_products::{
    ContinuumGenerationDemand, PlannedContinuumGeneration, PlannedMember, ProductOutput,
    ProductWindow, ProductWindowLayout, ProductWriter, ProductsError, RestoringBeam,
};
use casa_imaging_runtime::{ProductSinkResidency, SerialProductPublicationSink};
use casa_types::{RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};

struct StagedProduct {
    _directory: tempfile::TempDir,
    staging: PathBuf,
    target: PathBuf,
}

/// Production sink for conventional independently published CASA image members.
pub struct CasaImageProductSink {
    domains: BTreeMap<ImageDomainRole, CasaImageDomainOutput>,
    staged: Mutex<Vec<StagedProduct>>,
}

/// Storage binding for one compiled user-visible image domain.
#[derive(Clone)]
pub struct CasaImageDomainOutput {
    role: ImageDomainRole,
    base: PathBuf,
    coordinates: CoordinateSystem,
}

impl CasaImageDomainOutput {
    /// Bind one compiled domain role to its output root and exact coordinates.
    #[must_use]
    pub fn new(role: ImageDomainRole, base: PathBuf, coordinates: CoordinateSystem) -> Self {
        Self {
            role,
            base,
            coordinates,
        }
    }
}

impl CasaImageProductSink {
    /// Bind an output prefix and its complete CASA coordinate system.
    #[must_use]
    pub fn new(base: PathBuf, coordinates: CoordinateSystem) -> Self {
        Self::for_domains([CasaImageDomainOutput::new(
            ImageDomainRole::Main,
            base,
            coordinates,
        )])
        .expect("one main image-domain output is valid")
    }

    /// Bind every compiled image-domain role to one unique output root and WCS.
    pub fn for_domains(
        domains: impl IntoIterator<Item = CasaImageDomainOutput>,
    ) -> Result<Self, std::io::Error> {
        let mut outputs = BTreeMap::new();
        let mut roots = std::collections::BTreeSet::new();
        for output in domains {
            if !roots.insert(output.base.clone())
                || outputs.insert(output.role.clone(), output).is_some()
            {
                return Err(std::io::Error::other(
                    "CASA image-domain output roles and roots must be unique",
                ));
            }
        }
        if outputs.is_empty() || !outputs.contains_key(&ImageDomainRole::Main) {
            return Err(std::io::Error::other(
                "CASA image-domain outputs require one main domain",
            ));
        }
        Ok(Self {
            domains: outputs,
            staged: Mutex::new(Vec::new()),
        })
    }
}

impl SerialProductPublicationSink for CasaImageProductSink {
    type Error = std::io::Error;

    fn residency(
        &self,
        planned: &PlannedContinuumGeneration,
        demand: &ContinuumGenerationDemand,
    ) -> Result<ProductSinkResidency, Self::Error> {
        const IMAGE_ADAPTER_ENVELOPE_BYTES: u64 = 4_096;
        const STAGED_MEMBER_RECORD_BYTES: u64 = 512;
        let coordinate_bytes = self.domains.values().try_fold(0_u64, |maximum, output| {
            let bytes = output
                .coordinates
                .to_record()
                .retained_heap_bytes()
                .and_then(|bytes| bytes.checked_mul(4))
                .and_then(|bytes| u64::try_from(bytes).ok())
                .ok_or_else(|| std::io::Error::other("coordinate metadata residency overflow"))?;
            Ok::<_, std::io::Error>(maximum.max(bytes))
        })?;
        let beam_bytes = planned
            .members()
            .iter()
            .try_fold(0_u64, |maximum, member| {
                let count = if member.storage().attach_beam() {
                    member
                        .axes()
                        .spectral()
                        .output_channels()
                        .checked_mul(member.axes().polarization().len())
                        .ok_or_else(|| std::io::Error::other("beam count overflow"))?
                } else {
                    0
                };
                Ok::<_, std::io::Error>(maximum.max(beam_metadata_residency_bytes(count)?))
            })?;
        let registry_bytes = planned.members().iter().try_fold(0_u64, |total, member| {
            let output = self.domains.get(member.axes().domain()).ok_or_else(|| {
                std::io::Error::other("product domain has no CASA output binding")
            })?;
            let base_bytes = u64::try_from(output.base.to_string_lossy().len())
                .map_err(|_| std::io::Error::other("product path length exceeds u64"))?;
            let name_bytes = u64::try_from(member.name().len())
                .map_err(|_| std::io::Error::other("product name length exceeds u64"))?;
            let path_bytes = base_bytes
                .checked_add(name_bytes)
                .and_then(|bytes| bytes.checked_mul(2))
                .ok_or_else(|| std::io::Error::other("product path residency overflow"))?;
            total
                .checked_add(path_bytes)
                .and_then(|bytes| bytes.checked_add(STAGED_MEMBER_RECORD_BYTES))
                .ok_or_else(|| std::io::Error::other("product registry residency overflow"))
        })?;
        let writer_bytes = demand
            .maximum_window_payload_bytes()
            .checked_mul(4)
            .and_then(|bytes| {
                bytes.checked_add(demand.maximum_window_validity_bytes().checked_mul(2)?)
            })
            .and_then(|bytes| bytes.checked_add(IMAGE_ADAPTER_ENVELOPE_BYTES))
            .and_then(|bytes| bytes.checked_add(coordinate_bytes))
            .and_then(|bytes| bytes.checked_add(beam_bytes))
            .ok_or_else(|| std::io::Error::other("product staging residency overflow"))?;
        Ok(ProductSinkResidency {
            writer_bytes,
            retained_bytes: registry_bytes,
        })
    }

    fn publish(&self) -> Result<(), Self::Error> {
        let staged =
            std::mem::take(&mut *self.staged.lock().map_err(|_| {
                std::io::Error::other("CASA product staging registry lock poisoned")
            })?);
        for product in staged {
            promote_atomically(&product.staging, &product.target)?;
        }
        Ok(())
    }
}

impl ProductOutput for CasaImageProductSink {
    fn begin_member<'a>(
        &'a self,
        member: &PlannedMember,
        layout: ProductWindowLayout,
        beams: &[Option<RestoringBeam>],
    ) -> Result<Box<dyn ProductWriter + 'a>, ProductsError> {
        self.begin_image(member, layout, beams)
            .map(|writer| Box::new(writer) as Box<dyn ProductWriter>)
            .map_err(|error| ProductsError::Storage(error.to_string()))
    }
}

impl CasaImageProductSink {
    fn begin_image(
        &self,
        member: &PlannedMember,
        layout: ProductWindowLayout,
        beams: &[Option<RestoringBeam>],
    ) -> Result<CasaProductWriter<'_>, std::io::Error> {
        let output = self
            .domains
            .get(member.axes().domain())
            .ok_or_else(|| std::io::Error::other("product domain has no CASA output binding"))?;
        let target = PathBuf::from(format!("{}{}", output.base.display(), member.name()));
        let parent = target.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let target_name = target
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| std::io::Error::other("CASA product target is not UTF-8"))?;
        let directory = tempfile::Builder::new()
            .prefix(&format!(".{target_name}.casa-rs-stage-"))
            .tempdir_in(parent)?;
        let staging = directory.path().join("image");
        let mut tile = layout.shape();
        tile[layout.spectral_axis()] = 1;
        let mut image = PagedImage::<f32>::create_with_tile_shape_and_cache(
            layout.shape().to_vec(),
            tile.to_vec(),
            output.coordinates.clone(),
            &staging,
            layout
                .maximum_values()
                .checked_mul(4)
                .ok_or_else(|| std::io::Error::other("image cache overflow"))?,
        )
        .map_err(|error| std::io::Error::other(error.to_string()))?;
        let storage = member.storage();
        let explicit_mask = matches!(storage.pixel_mask(), ProductPixelMask::Explicit(_));
        image
            .set_units(storage.unit().map_or("", unit_label))
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let role = role_label(member.role());
        let beam_set = if storage.attach_beam() {
            persisted_beam_set(beams)
        } else {
            ImageBeamSet::default()
        };
        image
            .set_image_info(&ImageInfo {
                beam_set,
                image_type: match role {
                    "psf" => ImageType::Beam,
                    "sumwt" => ImageType::Undefined,
                    _ => ImageType::Intensity,
                },
                object_name: role.to_string(),
            })
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        image
            .set_misc_info(RecordValue::new(vec![RecordField::new(
                "casars_imager_role",
                Value::Scalar(ScalarValue::String(role.to_string())),
            )]))
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        Ok(CasaProductWriter {
            image,
            explicit_mask,
            sink: self,
            staged: StagedProduct {
                _directory: directory,
                staging,
                target,
            },
        })
    }
}

struct CasaProductWriter<'a> {
    image: PagedImage<f32>,
    explicit_mask: bool,
    sink: &'a CasaImageProductSink,
    staged: StagedProduct,
}

impl ProductWriter for CasaProductWriter<'_> {
    fn write(&mut self, window: ProductWindow) -> Result<(), ProductsError> {
        let (start, shape, payload, validity) = window.into_parts();
        let data = ArrayD::from_shape_vec(IxDyn(&shape), payload)
            .map_err(|error| ProductsError::Storage(error.to_string()))?;
        self.image
            .put_slice_view(data.view(), &start)
            .map_err(|error| ProductsError::Storage(error.to_string()))?;
        if self.explicit_mask {
            let mask = ArrayD::from_shape_vec(IxDyn(&shape), validity)
                .map_err(|error| ProductsError::Storage(error.to_string()))?;
            self.image
                .put_mask_slice("mask0", &mask, &start)
                .map_err(|error| ProductsError::Storage(error.to_string()))?;
        }
        Ok(())
    }
    fn finish(mut self: Box<Self>) -> Result<(), ProductsError> {
        if self.explicit_mask {
            self.image
                .set_default_mask("mask0")
                .map_err(|error| ProductsError::Storage(error.to_string()))?;
        }
        self.image.prepare_relocation(&self.staged.target);
        self.image
            .save()
            .map_err(|error| ProductsError::Storage(error.to_string()))?;
        let Self {
            image,
            sink,
            staged,
            ..
        } = *self;
        drop(image);
        sink.staged
            .lock()
            .map_err(|_| {
                ProductsError::Storage("CASA product staging registry lock poisoned".into())
            })?
            .push(staged);
        Ok(())
    }
}

fn beam_metadata_residency_bytes(count: usize) -> Result<u64, std::io::Error> {
    if count == 0 {
        return Ok(0);
    }
    let overflow = || std::io::Error::other("beam metadata residency overflow");
    let beam_record = GaussianBeam::new(1.0, 1.0, 0.0)
        .to_record()
        .retained_heap_bytes()
        .ok_or_else(overflow)?;
    let key_bytes = count.ilog10() as usize + 2;
    // The growable outer keyword record has at most twice its live field count.
    // Save overlaps owned keywords, the storage snapshot, table.dat keywords,
    // and the encoded control buffer; the record heap bounds its encoded form.
    let record = count
        .checked_add(2)
        .and_then(|fields| fields.checked_mul(2 * std::mem::size_of::<RecordField>()))
        .and_then(|bytes| {
            bytes.checked_add(count.checked_mul(beam_record.checked_add(key_bytes)?)?)
        })
        .and_then(|bytes| bytes.checked_mul(4))
        .ok_or_else(overflow)?;
    let arrays = count
        .checked_mul(
            std::mem::size_of::<RestoringBeam>()
                + std::mem::size_of::<Vec<GaussianBeam>>()
                + std::mem::size_of::<GaussianBeam>(),
        )
        .ok_or_else(overflow)?;
    u64::try_from(record.checked_add(arrays).ok_or_else(overflow)?).map_err(|_| overflow())
}

fn persisted_beam_set(beams: &[Option<RestoringBeam>]) -> ImageBeamSet {
    if beams.is_empty() {
        return ImageBeamSet::default();
    }
    let valid = beams.iter().flatten().copied().collect::<Vec<_>>();
    if let Some(first) = valid.first().copied()
        && valid.len() == beams.len()
        && valid.iter().all(|beam| *beam == first)
    {
        return ImageBeamSet::new(gaussian_beam(first));
    }
    let filler = valid
        .iter()
        .copied()
        .max_by(|left, right| {
            beam_area(*left)
                .partial_cmp(&beam_area(*right))
                .expect("validated beam areas are finite")
        })
        .map(gaussian_beam)
        .unwrap_or_else(|| {
            let one_microarcsecond_rad = std::f64::consts::PI / (180.0 * 3_600_000_000.0);
            GaussianBeam::new(one_microarcsecond_rad, one_microarcsecond_rad, 0.0)
        });
    ImageBeamSet::from_grid(
        beams
            .iter()
            .map(|beam| vec![beam.map_or(filler, gaussian_beam)])
            .collect(),
    )
}

fn gaussian_beam(beam: RestoringBeam) -> GaussianBeam {
    GaussianBeam::new(
        beam.major_fwhm_rad(),
        beam.minor_fwhm_rad(),
        beam.position_angle_rad(),
    )
}

fn beam_area(beam: RestoringBeam) -> f64 {
    beam.major_fwhm_rad() * beam.minor_fwhm_rad()
}

#[cfg(test)]
mod tests {
    use super::{CasaImageDomainOutput, CasaImageProductSink, persisted_beam_set};
    use casa_coordinates::CoordinateSystem;
    use casa_imaging_model::ImageDomainRole;
    use casa_imaging_products::RestoringBeam;

    #[test]
    fn beam_metadata_demand_scales_with_planes_and_bounds_keyword_storage() {
        let mut previous = 0;
        for count in [1, 16, 512, 16_384] {
            let bytes = super::beam_metadata_residency_bytes(count).unwrap();
            assert!(bytes > previous);
            previous = bytes;
        }
        let beams = (0..16)
            .map(|index| {
                Some(RestoringBeam::new(2.0e-6 + index as f64 * 1.0e-8, 1.0e-6, 0.0).unwrap())
            })
            .collect::<Vec<_>>();
        let record_bytes = persisted_beam_set(&beams)
            .to_record()
            .retained_heap_bytes()
            .unwrap();
        assert!(super::beam_metadata_residency_bytes(16).unwrap() >= 4 * record_bytes as u64);
        assert!(super::beam_metadata_residency_bytes(usize::MAX).is_err());
    }

    #[test]
    fn domain_outputs_require_unique_roles_roots_and_one_main() {
        let main = CasaImageDomainOutput::new(
            ImageDomainRole::Main,
            "main".into(),
            CoordinateSystem::new(),
        );
        let outlier = CasaImageDomainOutput::new(
            ImageDomainRole::Outlier("north".into()),
            "north".into(),
            CoordinateSystem::new(),
        );
        let sink = CasaImageProductSink::for_domains([main.clone(), outlier])
            .expect("unique domain outputs");
        assert_eq!(sink.domains.len(), 2);
        assert!(CasaImageProductSink::for_domains([main.clone(), main]).is_err());
        assert!(
            CasaImageProductSink::for_domains([CasaImageDomainOutput::new(
                ImageDomainRole::Outlier("north".into()),
                "north".into(),
                CoordinateSystem::new(),
            )])
            .is_err()
        );
    }

    #[test]
    fn blank_beam_slots_use_the_largest_valid_casa_persistence_filler() {
        let small = RestoringBeam::new(2.0e-6, 1.0e-6, 0.1).expect("small beam");
        let large = RestoringBeam::new(4.0e-6, 3.0e-6, -0.2).expect("large beam");
        let persisted = persisted_beam_set(&[Some(small), None, Some(large)]);

        assert_eq!(persisted.shape(), (3, 1));
        assert_eq!(persisted.beam(0, 0).major, small.major_fwhm_rad());
        assert_eq!(persisted.beam(1, 0).major, large.major_fwhm_rad());
        assert_eq!(persisted.beam(2, 0).major, large.major_fwhm_rad());
    }

    #[test]
    fn all_blank_beam_slots_use_only_the_casa_imageinfo_placeholder() {
        let persisted = persisted_beam_set(&[None, None]);
        let filler = persisted.beam(0, 0);
        assert_eq!(persisted.shape(), (2, 1));
        assert!(filler.major > 0.0);
        assert_eq!(filler.major, filler.minor);
        assert_eq!(persisted.beam(1, 0), filler);
    }

    #[test]
    fn individual_outputs_replace_atomically_and_failed_sets_require_rerun() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("image");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("pixels"), b"old").unwrap();
        let private = tempfile::tempdir_in(root.path()).unwrap();
        let staged = private.path().join("image");
        std::fs::create_dir(&staged).unwrap();
        std::fs::write(staged.join("pixels"), b"new").unwrap();
        super::promote_atomically(&staged, &target).unwrap();
        assert_eq!(std::fs::read(target.join("pixels")).unwrap(), b"new");
        assert_eq!(std::fs::read(staged.join("pixels")).unwrap(), b"old");
        let missing = private.path().join("missing");
        assert!(super::promote_atomically(&missing, &target).is_err());
        assert_eq!(std::fs::read(target.join("pixels")).unwrap(), b"new");
    }
}

const fn unit_label(unit: ProductUnit) -> &'static str {
    match unit {
        ProductUnit::NotApplicable | ProductUnit::Dimensionless | ProductUnit::VisibilityWeight => {
            ""
        }
        ProductUnit::JyPerBeam => "Jy/beam",
        ProductUnit::JyPerPixel => "Jy/pixel",
    }
}

const fn role_label(role: ProductRole) -> &'static str {
    match role {
        ProductRole::Psf(_) => "psf",
        ProductRole::Residual(_) => "residual",
        ProductRole::Model(_) => "model",
        ProductRole::RestoredImage(_) => "image",
        ProductRole::SumWeights(_) => "sumwt",
        ProductRole::CleanMask => "mask",
        ProductRole::Weight(_) => "weight",
        ProductRole::PrimaryBeam(_) => "pb",
        ProductRole::Sensitivity => "sensitivity",
        ProductRole::PbCorrectedImage(_) => "pbcor.image",
        ProductRole::SpectralIndex => "alpha",
        ProductRole::SpectralIndexError => "alpha.error",
        ProductRole::PbCorrectedSpectralIndex => "alpha.pbcor",
        ProductRole::BeamMetadata => "beam",
        _ => "product",
    }
}

fn promote_atomically(staging: &Path, target: &Path) -> std::io::Result<()> {
    let parent = target.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "product target has no parent directory",
        )
    })?;
    if staging.parent().and_then(Path::parent) != Some(parent) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "product staging must be private to the output directory",
        ));
    }
    if target.exists() {
        exchange_directories(staging, target)?;
    } else {
        fs::rename(staging, target)?;
    }
    File::open(parent).and_then(|directory| directory.sync_all())?;
    File::open(
        staging
            .parent()
            .expect("validated private output directory"),
    )
    .and_then(|directory| directory.sync_all())?;
    Ok(())
}

#[cfg(target_os = "macos")]
fn exchange_directories(staging: &Path, target: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let staging = CString::new(staging.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("staging path contains NUL"))?;
    let target = CString::new(target.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("target path contains NUL"))?;
    // SAFETY: both C strings remain valid for the duration of this atomic call.
    let status = unsafe { libc::renamex_np(staging.as_ptr(), target.as_ptr(), libc::RENAME_SWAP) };
    (status == 0)
        .then_some(())
        .ok_or_else(std::io::Error::last_os_error)
}

#[cfg(target_os = "linux")]
fn exchange_directories(staging: &Path, target: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let staging = CString::new(staging.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("staging path contains NUL"))?;
    let target = CString::new(target.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("target path contains NUL"))?;
    // SAFETY: both C strings remain valid for the duration of this atomic call.
    let status = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            staging.as_ptr(),
            libc::AT_FDCWD,
            target.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    (status == 0)
        .then_some(())
        .ok_or_else(std::io::Error::last_os_error)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn exchange_directories(_: &Path, _: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic non-empty directory exchange is unavailable",
    ))
}
