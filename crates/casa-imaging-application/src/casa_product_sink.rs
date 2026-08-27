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
use casa_imaging_model::{ProductRole, ProductUnit};
use casa_imaging_products::SealedMember;
use casa_imaging_runtime::{
    ArtifactIdentity, AuthorizedProductPublicationEntry, MemberPromotionFailure,
    SerialProductPublicationSink,
};
use casa_types::{RecordField, RecordValue, ScalarValue, Value};
use ndarray::Array4;

struct StagedProduct {
    observed: ArtifactIdentity,
    staging: PathBuf,
    target: PathBuf,
}

/// Production sink for conventional independently published CASA image members.
pub struct CasaImageProductSink {
    base: PathBuf,
    coordinates: CoordinateSystem,
    staged: Mutex<BTreeMap<ArtifactIdentity, StagedProduct>>,
}

impl CasaImageProductSink {
    /// Bind an output prefix and its complete CASA coordinate system.
    #[must_use]
    pub fn new(base: PathBuf, coordinates: CoordinateSystem) -> Self {
        Self {
            base,
            coordinates,
            staged: Mutex::new(BTreeMap::new()),
        }
    }
}

impl SerialProductPublicationSink for CasaImageProductSink {
    type Error = std::io::Error;

    fn stage(
        &self,
        planned: ArtifactIdentity,
        observed: ArtifactIdentity,
        member: &SealedMember,
    ) -> Result<(), Self::Error> {
        let target = PathBuf::from(format!("{}{}", self.base.display(), member.name()));
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
            PagedImage::<f32>::create(data.shape().to_vec(), self.coordinates.clone(), &staging)
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        image
            .put_slice_view(data.view().into_dyn(), &[0, 0, 0, 0])
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        image
            .set_units(unit_label(member.contract().unit()))
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let role = role_label(member.contract().role());
        let beam_set = match member.resolved_beams() {
            [] => ImageBeamSet::default(),
            [Some(beam)] => ImageBeamSet::new(GaussianBeam::new(
                beam.major_fwhm_rad(),
                beam.minor_fwhm_rad(),
                beam.position_angle_rad(),
            )),
            beams => ImageBeamSet::from_grid(
                beams
                    .iter()
                    .map(|beam| {
                        vec![beam.map_or_else(GaussianBeam::default, |beam| {
                            GaussianBeam::new(
                                beam.major_fwhm_rad(),
                                beam.minor_fwhm_rad(),
                                beam.position_angle_rad(),
                            )
                        })]
                    })
                    .collect(),
            ),
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
            .and_then(|()| image.save())
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
