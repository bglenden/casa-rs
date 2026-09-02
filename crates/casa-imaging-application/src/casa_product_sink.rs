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
use casa_imaging_model::{ImageDomainRole, ProductRole, ProductUnit, ProductValidityRule};
use casa_imaging_products::{
    ContinuumGenerationDemand, PlannedContinuumGeneration, RestoringBeam, SealedMember,
};
use casa_imaging_runtime::{
    ArtifactIdentity, AuthorizedProductPublicationEntry, MemberPromotionFailure,
    SerialProductPublicationSink,
};
use casa_types::{RecordField, RecordValue, ScalarValue, Value};
use ndarray::{Array4, ArrayD, IxDyn};

struct StagedProduct {
    observed: ArtifactIdentity,
    staging: PathBuf,
    target: PathBuf,
}

/// Production sink for conventional independently published CASA image members.
pub struct CasaImageProductSink {
    domains: BTreeMap<ImageDomainRole, CasaImageDomainOutput>,
    staged: Mutex<BTreeMap<ArtifactIdentity, StagedProduct>>,
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
            staged: Mutex::new(BTreeMap::new()),
        })
    }
}

impl SerialProductPublicationSink for CasaImageProductSink {
    type Error = std::io::Error;

    fn staging_residency_bytes(
        &self,
        planned: &PlannedContinuumGeneration,
        demand: &ContinuumGenerationDemand,
    ) -> Result<u64, Self::Error> {
        const IMAGE_ADAPTER_ENVELOPE_BYTES: u64 = 4_096;
        const STAGED_MEMBER_RECORD_BYTES: u64 = 512;
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
        demand
            .maximum_member_payload_bytes()
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(demand.maximum_member_validity_bytes()))
            .and_then(|bytes| bytes.checked_add(IMAGE_ADAPTER_ENVELOPE_BYTES))
            .and_then(|bytes| bytes.checked_add(registry_bytes))
            .ok_or_else(|| std::io::Error::other("product staging residency overflow"))
    }

    fn stage(
        &self,
        planned: ArtifactIdentity,
        observed: ArtifactIdentity,
        member: &SealedMember,
    ) -> Result<(), Self::Error> {
        let output = self
            .domains
            .get(member.contract().axes().domain())
            .ok_or_else(|| std::io::Error::other("product domain has no CASA output binding"))?;
        let target = PathBuf::from(format!("{}{}", output.base.display(), member.name()));
        let parent = target.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let target_name = target
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| std::io::Error::other("CASA product target is not UTF-8"))?;
        let staging = parent.join(format!(
            ".{target_name}.casa-rs-stage-{}",
            identity_hex(observed)
        ));
        if staging.exists() {
            fs::remove_dir_all(&staging)?;
        }
        let data =
            Array4::from_shape_vec(member.contract().axes().shape(), member.payload().to_vec())
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        let mut image =
            PagedImage::<f32>::create(data.shape().to_vec(), output.coordinates.clone(), &staging)
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        image
            .put_slice_view(data.view().into_dyn(), &[0, 0, 0, 0])
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        if member.contract().validity() != ProductValidityRule::All {
            let validity = ArrayD::from_shape_vec(
                IxDyn(&member.contract().axes().shape()),
                member.validity().to_vec(),
            )
            .map_err(|error| std::io::Error::other(error.to_string()))?;
            image
                .put_mask("mask0", &validity)
                .and_then(|()| image.set_default_mask("mask0"))
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        }
        image
            .set_units(unit_label(member.contract().unit()))
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let role = role_label(member.contract().role());
        let beam_set = persisted_beam_set(member.resolved_beams());
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
            .set_misc_info(RecordValue::new(vec![
                RecordField::new(
                    "casars_imager_role",
                    Value::Scalar(ScalarValue::String(role.to_string())),
                ),
                RecordField::new(
                    "casa_rs_planned_product_identity",
                    Value::Scalar(ScalarValue::String(identity_hex(planned))),
                ),
                RecordField::new(
                    "casa_rs_observed_product_identity",
                    Value::Scalar(ScalarValue::String(identity_hex(observed))),
                ),
            ]))
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        image.prepare_relocation(&target);
        image
            .save()
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        self.staged
            .lock()
            .map_err(|_| std::io::Error::other("CASA product staging registry lock poisoned"))?
            .insert(
                planned,
                StagedProduct {
                    observed,
                    staging,
                    target,
                },
            );
        Ok(())
    }

    fn promote(
        &self,
        entry: AuthorizedProductPublicationEntry,
    ) -> Result<(), MemberPromotionFailure<Self::Error>> {
        let staged = self
            .staged
            .lock()
            .map_err(|_| {
                MemberPromotionFailure::failed(std::io::Error::other(
                    "CASA product staging registry lock poisoned",
                ))
            })?
            .remove(&entry.planned_identity())
            .ok_or_else(|| {
                MemberPromotionFailure::failed(std::io::Error::other(
                    "authorized CASA product has no private staging entry",
                ))
            })?;
        if staged.observed != entry.observed_identity() {
            return Err(MemberPromotionFailure::failed(std::io::Error::other(
                "staged CASA product identity does not match authority",
            )));
        }
        if visible_identity(&staged.target).as_deref()
            == Some(identity_hex(entry.observed_identity()).as_str())
        {
            let _ = fs::remove_dir_all(&staged.staging);
            return Ok(());
        }
        promote_atomically(&staged.staging, &staged.target).map_err(|error| match error {
            PromotionError::Failed(error) => MemberPromotionFailure::failed(error),
            PromotionError::Uncertain(error) => MemberPromotionFailure::uncertain(error),
        })
    }
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
}

fn identity_hex(identity: ArtifactIdentity) -> String {
    identity
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn visible_identity(path: &Path) -> Option<String> {
    if !path.exists() {
        return None;
    }
    let image = PagedImage::<f32>::open(path).ok()?;
    match image.misc_info().get("casa_rs_observed_product_identity") {
        Some(Value::Scalar(ScalarValue::String(identity))) => Some(identity.clone()),
        _ => None,
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

#[derive(Debug)]
enum PromotionError {
    Failed(std::io::Error),
    Uncertain(std::io::Error),
}

fn promote_atomically(staging: &Path, target: &Path) -> Result<(), PromotionError> {
    let parent = target.parent().ok_or_else(|| {
        PromotionError::Failed(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "product target has no parent directory",
        ))
    })?;
    if staging.parent() != Some(parent) {
        return Err(PromotionError::Failed(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "product staging and target must be siblings",
        )));
    }
    if target.exists() {
        exchange_directories(staging, target)?;
    } else {
        fs::rename(staging, target).map_err(PromotionError::Failed)?;
    }
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(PromotionError::Uncertain)?;
    if staging.exists() {
        let _ = fs::remove_dir_all(staging);
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn exchange_directories(staging: &Path, target: &Path) -> Result<(), PromotionError> {
    use std::os::unix::ffi::OsStrExt;
    let staging = CString::new(staging.as_os_str().as_bytes())
        .map_err(|_| PromotionError::Failed(std::io::Error::other("staging path contains NUL")))?;
    let target = CString::new(target.as_os_str().as_bytes())
        .map_err(|_| PromotionError::Failed(std::io::Error::other("target path contains NUL")))?;
    // SAFETY: both C strings remain valid for the duration of this atomic call.
    let status = unsafe { libc::renamex_np(staging.as_ptr(), target.as_ptr(), libc::RENAME_SWAP) };
    (status == 0)
        .then_some(())
        .ok_or_else(|| PromotionError::Failed(std::io::Error::last_os_error()))
}

#[cfg(target_os = "linux")]
fn exchange_directories(staging: &Path, target: &Path) -> Result<(), PromotionError> {
    use std::os::unix::ffi::OsStrExt;
    let staging = CString::new(staging.as_os_str().as_bytes())
        .map_err(|_| PromotionError::Failed(std::io::Error::other("staging path contains NUL")))?;
    let target = CString::new(target.as_os_str().as_bytes())
        .map_err(|_| PromotionError::Failed(std::io::Error::other("target path contains NUL")))?;
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
        .ok_or_else(|| PromotionError::Failed(std::io::Error::last_os_error()))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn exchange_directories(_: &Path, _: &Path) -> Result<(), PromotionError> {
    Err(PromotionError::Failed(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic non-empty directory exchange is unavailable",
    )))
}
