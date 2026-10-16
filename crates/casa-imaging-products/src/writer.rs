// SPDX-License-Identifier: LGPL-3.0-or-later

//! The CASA image writer (plan section 5.7).
//!
//! Each member is written to a private staging directory beside its target.
//! Publishing then replaces each target on its own: the staged image is
//! renamed into place, or exchanged with an existing image in one call
//! (`renamex_np` with `RENAME_SWAP` on macOS, `renameat2` with
//! `RENAME_EXCHANGE` on Linux), and both directories are synced. An open
//! after the replacement sees the whole new image, and one before it the
//! whole old image. A member already replaced stays valid if a later one
//! fails; the set is then incomplete and the run must be repeated.
//!
//! Limits:
//! - **A held reader keeps only what it has opened.** casacore opens some
//!   storage, such as a pixel mask, by path when it is first read. A reader
//!   that holds the old image across a replacement and first reads such
//!   storage afterwards reads the new image's, or fails if the new image has
//!   none. Reopen an image after a run replaces it. CASA's own in-place
//!   overwrite gives a held reader no coherence at all.
//! - **Exchange needs local filesystem support.** A filesystem without an
//!   atomic exchange (SMB, some NFS) refuses a run into an existing image,
//!   with [`ProductsError::Io`].
//! - **A killed run leaves its staging.** A failed generation discards what
//!   it staged; a killed process leaves `.<name>.casa-rs-stage-*`
//!   directories beside the targets, which are safe to delete.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CString, OsString};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};
use std::time::Instant;

use casa_coordinates::CoordinateSystem;
use casa_images::{GaussianBeam, ImageBeamSet, ImageInfo, ImageType, PagedImage};
use casa_imaging_model::{ImageDomainRole, ProductPixelMask, ProductRole, ProductUnit};
use casa_types::{RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};

use crate::storage::{ProductWindow, ProductWindowLayout};
use crate::{PlannedContinuumGeneration, PlannedMember, ProductsError, RestoringBeam};

/// The CASA image member set of one run, staged until [`Self::publish`].
pub struct CasaImageWriter {
    domains: BTreeMap<ImageDomainRole, CasaImageDomainOutput>,
    staged: Mutex<Vec<StagedImage>>,
}

/// Where one compiled image domain's members are written, and their
/// coordinates.
#[derive(Clone)]
pub struct CasaImageDomainOutput {
    role: ImageDomainRole,
    base: PathBuf,
    coordinates: CoordinateSystem,
}

impl CasaImageDomainOutput {
    /// Bind one compiled domain role to its output prefix and coordinates.
    #[must_use]
    pub fn new(role: ImageDomainRole, base: PathBuf, coordinates: CoordinateSystem) -> Self {
        Self {
            role,
            base,
            coordinates,
        }
    }
}

struct StagedImage {
    _directory: tempfile::TempDir,
    staging: PathBuf,
    target: PathBuf,
}

impl CasaImageWriter {
    /// Write the main domain's members under `base` with `coordinates`.
    #[must_use]
    pub fn new(base: PathBuf, coordinates: CoordinateSystem) -> Self {
        Self::for_domains([CasaImageDomainOutput::new(
            ImageDomainRole::Main,
            base,
            coordinates,
        )])
    }

    /// Write every compiled image domain's members under its own prefix.
    ///
    /// # Panics
    ///
    /// If two outputs share a role or a prefix, or none is the main domain.
    /// Compiling a request refuses domains that name one output, and always
    /// compiles the main domain.
    #[must_use]
    pub fn for_domains(domains: impl IntoIterator<Item = CasaImageDomainOutput>) -> Self {
        let mut outputs = BTreeMap::new();
        let mut roots = BTreeSet::new();
        for output in domains {
            let new_root = roots.insert(output.base.clone());
            let new_role = outputs.insert(output.role.clone(), output).is_none();
            assert!(
                new_root && new_role,
                "compiled image domains have distinct roles and output roots"
            );
        }
        assert!(
            outputs.contains_key(&ImageDomainRole::Main),
            "compiled image domains include the main domain"
        );
        Self {
            domains: outputs,
            staged: Mutex::new(Vec::new()),
        }
    }

    fn output(&self, role: &ImageDomainRole) -> &CasaImageDomainOutput {
        self.domains
            .get(role)
            .expect("the writer binds every compiled image domain")
    }

    /// Bytes the writer holds while it writes `planned` a window at a time:
    /// the image's tile cache and serialization buffers, the pixel mask's
    /// cache (opened with the image's budget, so up to four windows of
    /// validity), its coordinates and beam metadata, and the record of each
    /// staged member kept until [`Self::publish`].
    pub(crate) fn residency_bytes(
        &self,
        planned: &PlannedContinuumGeneration,
        window_payload_bytes: u64,
        window_validity_bytes: u64,
    ) -> Result<u64, ProductsError> {
        // The open image's table, data-manager and record metadata. An 8×8
        // PB-only member with empty coordinates measured 46 kB live against
        // 18 kB charged with a 4 kB envelope (review of #701).
        const IMAGE_ENVELOPE_BYTES: u64 = 64 * 1024;
        const STAGED_MEMBER_RECORD_BYTES: u64 = 512;
        let overflow = || ProductsError::ResourceDemandOverflow("CASA image writer");
        let coordinate_bytes = self.domains.values().try_fold(0_u64, |maximum, output| {
            let bytes = output
                .coordinates
                .to_record()
                .retained_heap_bytes()
                .and_then(|bytes| bytes.checked_mul(4))
                .and_then(|bytes| u64::try_from(bytes).ok())
                .ok_or_else(overflow)?;
            Ok::<_, ProductsError>(maximum.max(bytes))
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
                        .ok_or_else(overflow)?
                } else {
                    0
                };
                Ok::<_, ProductsError>(maximum.max(beam_metadata_residency_bytes(count)?))
            })?;
        let registry_bytes = planned.members().iter().try_fold(0_u64, |total, member| {
            let base = self.output(member.axes().domain()).base.as_os_str().len();
            let path_bytes = base
                .checked_add(member.name().len())
                .and_then(|bytes| bytes.checked_mul(2))
                .and_then(|bytes| u64::try_from(bytes).ok())
                .ok_or_else(overflow)?;
            total
                .checked_add(path_bytes)
                .and_then(|bytes| bytes.checked_add(STAGED_MEMBER_RECORD_BYTES))
                .ok_or_else(overflow)
        })?;
        window_payload_bytes
            .checked_mul(4)
            // The validity pack and its copy, and the mask's cache.
            .and_then(|bytes| bytes.checked_add(window_validity_bytes.checked_mul(6)?))
            .and_then(|bytes| bytes.checked_add(IMAGE_ENVELOPE_BYTES))
            .and_then(|bytes| bytes.checked_add(coordinate_bytes))
            .and_then(|bytes| bytes.checked_add(beam_bytes))
            .and_then(|bytes| bytes.checked_add(registry_bytes))
            .ok_or_else(overflow)
    }

    /// The number of members staged and not yet published.
    pub(crate) fn staged_count(&self) -> usize {
        self.staged
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Discard the members staged after the first `kept`, deleting their
    /// staging: a failed generation publishes none of its members.
    pub(crate) fn discard_staged_after(&self, kept: usize) {
        self.staged
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .truncate(kept);
    }

    /// Replace every staged member's target, in generation order, telling
    /// `published` each target once it is replaced.
    ///
    /// # Errors
    ///
    /// The first replacement that fails. Members replaced before it stay
    /// replaced, and `published` has heard of each; the run must be
    /// repeated.
    pub fn publish(&self, mut published: impl FnMut(&Path)) -> Result<(), ProductsError> {
        let staged =
            std::mem::take(&mut *self.staged.lock().unwrap_or_else(PoisonError::into_inner));
        for image in staged {
            promote_atomically(&image.staging, &image.target).map_err(|source| {
                ProductsError::Io {
                    operation: "replacing",
                    path: image.target.clone(),
                    source,
                }
            })?;
            published(&image.target);
        }
        Ok(())
    }

    /// Create one member's image in private staging beside its target, with
    /// its units, beams and metadata.
    pub(crate) fn begin(
        &self,
        member: &PlannedMember,
        layout: ProductWindowLayout,
        beams: &[Option<RestoringBeam>],
        uses_weight_image: bool,
    ) -> Result<MemberImage<'_>, ProductsError> {
        let started = Instant::now();
        let output = self.output(member.axes().domain());
        let mut target = output.base.clone().into_os_string();
        target.push(member.name());
        let target = PathBuf::from(target);
        let io = |operation, path: &Path| {
            let path = path.to_path_buf();
            move |source| ProductsError::Io {
                operation,
                path,
                source,
            }
        };
        let parent = target.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(io("creating", parent))?;
        let mut prefix = OsString::from(".");
        prefix.push(target.file_name().expect("a product target names a file"));
        prefix.push(".casa-rs-stage-");
        let directory = tempfile::Builder::new()
            .prefix(&prefix)
            .tempdir_in(parent)
            .map_err(io("staging", parent))?;
        let staging = directory.path().join("image");
        let image_error = |source| ProductsError::Image {
            path: target.clone(),
            source,
        };
        let mut tile = layout.shape();
        tile[layout.spectral_axis()] = 1;
        let mut image = PagedImage::<f32>::create_with_tile_shape_and_cache(
            layout.shape().to_vec(),
            tile.to_vec(),
            output.coordinates.clone(),
            &staging,
            layout.maximum_values() * size_of::<f32>(),
        )
        .map_err(image_error)?;
        let storage = member.storage();
        image
            .set_units(storage.unit().map_or("", unit_label))
            .map_err(image_error)?;
        let role = role_label(member.role());
        image
            .set_image_info(&ImageInfo {
                beam_set: if storage.attach_beam() {
                    persisted_beam_set(beams)
                } else {
                    ImageBeamSet::default()
                },
                image_type: match role {
                    "psf" => ImageType::Beam,
                    "sumwt" => ImageType::Undefined,
                    _ => ImageType::Intensity,
                },
                object_name: role.to_string(),
            })
            .map_err(image_error)?;
        let mut miscinfo = vec![RecordField::new(
            "casars_imager_role",
            Value::Scalar(ScalarValue::String(role.to_string())),
        )];
        // CASA tools read `.sumwt`'s `useweightimage` to learn whether the
        // residual was divided by a weight image (`SIImageStore`
        // `getUseWeightImage`).
        if matches!(member.role(), ProductRole::SumWeights(_)) {
            miscinfo.push(RecordField::new(
                "useweightimage",
                Value::Scalar(ScalarValue::Bool(uses_weight_image)),
            ));
        }
        image
            .set_misc_info(RecordValue::new(miscinfo))
            .map_err(image_error)?;
        Ok(MemberImage {
            image,
            explicit_mask: matches!(storage.pixel_mask(), ProductPixelMask::Explicit(_)),
            writer: self,
            staged: StagedImage {
                _directory: directory,
                staging,
                target,
            },
            begin_nanos: started.elapsed().as_nanos(),
            window_nanos: 0,
        })
    }
}

/// One member's image in staging, written a window at a time.
pub(crate) struct MemberImage<'a> {
    image: PagedImage<f32>,
    explicit_mask: bool,
    writer: &'a CasaImageWriter,
    staged: StagedImage,
    begin_nanos: u128,
    window_nanos: u128,
}

impl MemberImage<'_> {
    /// Write one generated window's values and, where the member stores
    /// one, its pixel mask.
    pub(crate) fn write(&mut self, window: ProductWindow) -> Result<(), ProductsError> {
        let started = Instant::now();
        let (start, shape, payload, validity) = window.into_parts();
        let image_error = |source| ProductsError::Image {
            path: self.staged.target.clone(),
            source,
        };
        let data = ArrayD::from_shape_vec(IxDyn(&shape), payload)
            .expect("a product window's payload fills its shape");
        self.image
            .put_slice_view(data.view(), &start)
            .map_err(image_error)?;
        if self.explicit_mask {
            let mask = ArrayD::from_shape_vec(IxDyn(&shape), validity)
                .expect("a product window's validity fills its shape");
            self.image
                .put_mask_slice("mask0", &mask, &start)
                .map_err(image_error)?;
        }
        self.window_nanos += started.elapsed().as_nanos();
        Ok(())
    }

    /// Save the image in staging and register it for [`CasaImageWriter::publish`].
    pub(crate) fn finish(mut self) -> Result<(), ProductsError> {
        let started = Instant::now();
        let target = self.staged.target.clone();
        let image_error = |source| ProductsError::Image {
            path: target.clone(),
            source,
        };
        if self.explicit_mask {
            self.image.set_default_mask("mask0").map_err(image_error)?;
        }
        self.image.save().map_err(image_error)?;
        if let Some(stats) = self.image.tiled_io_stats() {
            tracing::debug!(
                target = %self.staged.target.display(),
                pack_nanos = stats.direct_tile_pack_ns,
                swap_nanos = stats.direct_tile_swap_ns,
                write_nanos = stats.direct_tile_write_ns,
                direct_bytes = stats.direct_tile_write_bytes,
                direct_calls = stats.direct_tile_write_calls,
                "product image pixels written"
            );
        }
        let Self {
            image,
            writer,
            staged,
            begin_nanos,
            window_nanos,
            ..
        } = self;
        drop(image);
        tracing::debug!(
            target = %staged.target.display(),
            begin_nanos,
            window_nanos,
            finish_nanos = started.elapsed().as_nanos(),
            "product image staged"
        );
        writer
            .staged
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(staged);
        Ok(())
    }
}

fn beam_metadata_residency_bytes(count: usize) -> Result<u64, ProductsError> {
    if count == 0 {
        return Ok(0);
    }
    let overflow = || ProductsError::ResourceDemandOverflow("beam metadata");
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
        .and_then(|fields| fields.checked_mul(2 * size_of::<RecordField>()))
        .and_then(|bytes| {
            bytes.checked_add(count.checked_mul(beam_record.checked_add(key_bytes)?)?)
        })
        .and_then(|bytes| bytes.checked_mul(4))
        .ok_or_else(overflow)?;
    let arrays = count
        .checked_mul(
            size_of::<RestoringBeam>() + size_of::<Vec<GaussianBeam>>() + size_of::<GaussianBeam>(),
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

#[cfg(test)]
mod tests {
    use super::{CasaImageDomainOutput, CasaImageWriter, persisted_beam_set};
    use crate::RestoringBeam;
    use casa_coordinates::CoordinateSystem;
    use casa_imaging_model::ImageDomainRole;

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
    fn domain_outputs_bind_every_role() {
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
        let writer = CasaImageWriter::for_domains([main, outlier]);
        assert_eq!(writer.domains.len(), 2);
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
