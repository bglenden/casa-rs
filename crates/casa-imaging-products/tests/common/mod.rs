// SPDX-License-Identifier: LGPL-3.0-or-later

//! Products written as CASA images to a temporary directory and read back
//! with `casa-images`, so each test sees what a reader of the published
//! images sees. The submodules build the compiled problems and the
//! major-cycle completions products are generated from.

// The continuum and Taylor integration binaries exercise different fixture members.
#![allow(dead_code)]

pub mod continuum;
pub mod observation;
pub mod synthetic_pass;

use std::collections::BTreeMap;
use std::ops::Deref;
use std::path::PathBuf;

use casa_coordinates::CoordinateSystem;
use casa_images::PagedImage;
use casa_imaging_model::{CompiledProblem, ImageDomainRole, ProductId};
use casa_imaging_products::{
    CasaImageDomainOutput, CasaImageWriter, PlannedContinuumGeneration, PlannedMember,
    ProductStoragePlan, PublishedContinuumGeneration, RestoringBeam,
};

/// Sum the actual owned summary fields independently of the production planner.
pub fn retained_metadata_bytes(generation: &PublishedContinuumGeneration) -> u64 {
    use std::mem::size_of_val;
    let mut bytes = size_of_val(generation)
        + size_of_val(generation.members())
        + size_of_val(generation.fitted_beams())
        + size_of_val(generation.restoring_beams());
    for member in generation.members() {
        bytes += member.name().len() + size_of_val(member.resolved_beams());
    }
    bytes as u64
}

pub fn full_window(planned: &PlannedContinuumGeneration) -> ProductStoragePlan {
    ProductStoragePlan::new(
        planned
            .members()
            .iter()
            .map(|member| member.axes().spectral().output_channels())
            .max()
            .unwrap(),
        1,
    )
    .unwrap()
}

/// A CASA image writer over a temporary directory, one output prefix per
/// image domain of the problem.
pub struct ImageOutput {
    directory: tempfile::TempDir,
    bases: BTreeMap<ImageDomainRole, PathBuf>,
    writer: CasaImageWriter,
}

impl ImageOutput {
    /// Write `problem`'s domains under a fresh temporary directory.
    pub fn for_problem(problem: &CompiledProblem) -> Self {
        let directory = tempfile::tempdir().expect("product output directory");
        let bases = problem
            .geometry()
            .domains()
            .iter()
            .enumerate()
            .map(|(index, domain)| {
                (
                    domain.role().clone(),
                    directory.path().join(format!("domain{index}")),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let writer = CasaImageWriter::for_domains(bases.iter().map(|(role, base)| {
            CasaImageDomainOutput::new(role.clone(), base.clone(), CoordinateSystem::new())
        }));
        Self {
            directory,
            bases,
            writer,
        }
    }

    /// The published image of `member`.
    pub fn path(&self, member: &PlannedMember) -> PathBuf {
        let mut path = self.bases[member.axes().domain()].clone().into_os_string();
        path.push(member.name());
        PathBuf::from(path)
    }

    /// Entries in the output directory: published images and any staging
    /// left behind.
    pub fn entries(&self) -> Vec<String> {
        let mut entries = std::fs::read_dir(self.directory.path())
            .expect("output directory")
            .map(|entry| {
                entry
                    .expect("output entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect::<Vec<_>>();
        entries.sort();
        entries
    }

    /// Read a published member's values and pixel mask in storage order;
    /// a member without a mask is valid everywhere.
    pub fn read(&self, member: &PlannedMember) -> (Vec<f32>, Vec<bool>) {
        let image = PagedImage::<f32>::open(self.path(member)).expect("published image");
        let shape = image.shape().to_vec();
        assert_eq!(shape, member.shape(), "{} shape", member.name());
        let start = vec![0; shape.len()];
        let payload = image
            .get_slice(&start, &shape)
            .expect("image pixels")
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let validity = match image.default_mask_name() {
            Some(_) => image
                .get_mask_slice(&start, &shape, &vec![1; shape.len()])
                .expect("image mask")
                .expect("default mask")
                .iter()
                .copied()
                .collect(),
            None => vec![true; payload.len()],
        };
        (payload, validity)
    }
}

impl Deref for ImageOutput {
    type Target = CasaImageWriter;

    fn deref(&self) -> &CasaImageWriter {
        &self.writer
    }
}

/// A test-owned copy of one generated member's metadata and published
/// arrays.
#[derive(Debug, Clone)]
pub struct GeneratedMember {
    id: ProductId,
    name: String,
    contract: PlannedMember,
    resolved_beams: Box<[Option<RestoringBeam>]>,
    payload: Vec<f32>,
    validity: Vec<bool>,
}

impl GeneratedMember {
    #[must_use]
    pub const fn id(&self) -> ProductId {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The planned member this one was generated from.
    #[must_use]
    pub const fn contract(&self) -> &PlannedMember {
        &self.contract
    }

    #[must_use]
    pub const fn resolved_beams(&self) -> &[Option<RestoringBeam>] {
        &self.resolved_beams
    }

    #[must_use]
    pub fn resolved_beam(&self) -> Option<&RestoringBeam> {
        match self.resolved_beams.as_ref() {
            [Some(beam)] => Some(beam),
            _ => None,
        }
    }

    #[must_use]
    pub fn payload(&self) -> &[f32] {
        &self.payload
    }

    #[must_use]
    pub fn validity(&self) -> &[bool] {
        &self.validity
    }
}

/// A generation's published members, read back from their images.
#[derive(Debug, Clone)]
pub struct GeneratedProducts {
    fitted_beams: Box<[Option<RestoringBeam>]>,
    restoring_beams: Box<[Option<RestoringBeam>]>,
    members: Box<[GeneratedMember]>,
}

impl GeneratedProducts {
    /// Publish a completed generation of `planned` and read every member's
    /// image back.
    #[must_use]
    pub fn from_output(
        planned: &PlannedContinuumGeneration,
        generation: &PublishedContinuumGeneration,
        output: &ImageOutput,
    ) -> Self {
        output.publish(|_| ()).expect("publish product images");
        let members = generation
            .members()
            .iter()
            .zip(planned.members())
            .map(|(member, planned)| {
                let (payload, validity) = output.read(planned);
                GeneratedMember {
                    id: member.id(),
                    name: member.name().to_string(),
                    contract: planned.clone(),
                    resolved_beams: member.resolved_beams().to_vec().into_boxed_slice(),
                    payload,
                    validity,
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            fitted_beams: generation.fitted_beams().to_vec().into_boxed_slice(),
            restoring_beams: generation.restoring_beams().to_vec().into_boxed_slice(),
            members,
        }
    }

    #[must_use]
    pub const fn fitted_beams(&self) -> &[Option<RestoringBeam>] {
        &self.fitted_beams
    }

    #[must_use]
    pub const fn restoring_beams(&self) -> &[Option<RestoringBeam>] {
        &self.restoring_beams
    }

    #[must_use]
    pub fn restoring_beam(&self) -> Option<&RestoringBeam> {
        match self.restoring_beams.as_ref() {
            [Some(beam)] => Some(beam),
            _ => None,
        }
    }

    #[must_use]
    pub const fn members(&self) -> &[GeneratedMember] {
        &self.members
    }
}
