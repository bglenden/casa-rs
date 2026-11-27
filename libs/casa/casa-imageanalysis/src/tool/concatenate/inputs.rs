// SPDX-License-Identifier: LGPL-3.0-or-later

//! Input inspection, revision tracking, and metadata combination for image concatenation.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use casa_coordinates::CoordinateSystem;
use casa_core::{RecordField, RecordValue, Value};
use casa_images::{
    ConcatInput, ConcatenationPlan, ImageHistoryRow, ImageInfo, ImagePixelType,
    MutableImageInterface, image_history_rows,
};
use casa_tables::lexical_absolute;

use super::super::beams::{image_info, image_info_record, plane_axes, stored_beam_record};
use super::super::error::ImageToolResult;
use super::super::handle::{ImageHandle, Storage, same_file_snapshot, with_pixels, with_storage};
use super::super::saved_expression::{self, SavedExpressionInputs, StagedOutput};

/// Snapshot of an input image's filesystem metadata and table modification generation.
#[derive(Clone)]
pub struct InputRevision {
    /// Filesystem metadata captured when reading the input.
    pub file: fs::Metadata,
    /// Table modify counter for paged images, when available.
    pub generation: Option<u32>,
}

impl InputRevision {
    /// Reads the current filesystem snapshot and optional modify counter for `image` at `path`.
    pub fn read(path: &Path, image: &ImageHandle) -> ImageToolResult<Self> {
        let file = fs::metadata(path)?;
        #[cfg(unix)]
        let generation = with_pixels!(image, storage => match storage {
            Storage::Paged(image) if !image.uses_advisory_locks() => {
                Some(image.table().locked_modify_counter()?)
            }
            Storage::Paged(_) | Storage::Temp { .. } => None,
        });
        #[cfg(not(unix))]
        let generation = {
            let _ = image;
            None
        };
        Ok(Self { file, generation })
    }

    /// Returns whether two snapshots represent the same unmodified input image revision.
    pub fn matches(&self, other: &Self) -> bool {
        self.generation == other.generation && same_file_snapshot(&self.file, &other.file)
    }
}

/// Validated metadata and optional open handle for one concatenation input image.
pub struct ConcatInputEntry<'a> {
    /// User-supplied path to the input image.
    pub path: &'a Path,
    /// Resolved physical/canonical path to the input image.
    pub resolved_path: PathBuf,
    /// Pixel shape of the input image.
    pub shape: Vec<usize>,
    /// Pixel type of the input image.
    pub kind: ImagePixelType,
    /// Parsed coordinate system.
    pub coordinates: CoordinateSystem,
    /// Raw coordinate record.
    pub coordinate_record: RecordValue,
    /// Parsed image info (beam set and image type).
    pub info: ImageInfo,
    /// Raw `imageinfo` record if present.
    pub info_record: Option<RecordValue>,
    /// Brightness unit string.
    pub units: String,
    /// `miscinfo` record.
    pub misc: RecordValue,
    /// History entries copied from this input.
    pub history: Vec<ImageHistoryRow>,
    /// Whether the input carries an active pixel mask or FITS blank mask.
    pub has_mask: bool,
    /// Whether the input pixels are writable.
    pub writable: bool,
    /// Whether the active pixel mask is writable.
    pub writable_mask: bool,
    /// Whether the input uses advisory locking (MIRIAD / tempclose).
    pub advisory_lock: bool,
    /// Retained handle when not using `tempclose` or when expression operands are held.
    pub opened: Option<ImageHandle>,
    /// Bound saved-expression operands when this input is an `.imgexpr`.
    pub expression: Option<SavedExpressionInputs>,
    /// All expression operand paths referenced by this input.
    pub expression_operands: Vec<PathBuf>,
    /// Nested concatenation operand paths referenced by this input.
    pub nested_operands: Vec<PathBuf>,
    /// Revision snapshot captured for `tempclose` validation.
    pub revision: Option<InputRevision>,
}

impl<'a> ConcatInputEntry<'a> {
    /// Opens an input image at `path` for concatenation.
    pub fn open(path: &'a Path, tempclose: bool) -> ImageToolResult<Self> {
        Self::open_resolved(path, path, tempclose, &mut HashSet::new(), None)
    }

    /// Opens an input image at `resolved` (handling `.imgexpr` and staged substitution).
    pub fn open_resolved(
        path: &'a Path,
        resolved: &Path,
        tempclose: bool,
        active: &mut HashSet<PathBuf>,
        output: Option<&StagedOutput<'_>>,
    ) -> ImageToolResult<Self> {
        if !casa_images::is_image_concat(resolved)
            && casa_images::expr_file::is_image_expr(resolved)
        {
            let (image, inputs) =
                saved_expression::open_for_concatenation(resolved, active, output)?;
            return Self::from_image(path, image, tempclose, Some(inputs));
        }
        let image = ImageHandle::open_for_concatenation(resolved)?;
        Self::from_image(path, image, tempclose, None)
    }

    /// Wraps an already-opened [`ImageHandle`] into a [`ConcatInputEntry`].
    pub fn from_image(
        path: &'a Path,
        image: ImageHandle,
        tempclose: bool,
        mut expression: Option<SavedExpressionInputs>,
    ) -> ImageToolResult<Self> {
        let resolved_path = image.path().unwrap_or(path).to_path_buf();
        let coordinate_record = image.coordinate_record();
        let coordinates = CoordinateSystem::from_record(&coordinate_record)?;
        let (units, misc, has_mask) = with_pixels!(&image, storage => with_storage!(storage, image => (
            image.units().to_string(),
            image.misc_info(),
            image.has_pixel_mask(),
        )));
        let history = with_pixels!(&image, storage => match storage {
            Storage::Paged(_) => image_history_rows(path)?,
            Storage::Temp { image, .. } => image.history_rows()?.into_owned(),
        });
        let revision = tempclose
            .then(|| InputRevision::read(&resolved_path, &image))
            .transpose()?;
        let writable = with_pixels!(&image, storage => match storage {
            Storage::Paged(image) => {
                image.is_writable()
                    && casa_tables::publication::check_write_access(&resolved_path.join("table.dat"))
                        .is_ok()
            }
            Storage::Temp { image, fits, .. } => fits.is_none() && image.is_writable(),
        });
        let writable_mask = with_pixels!(&image, storage => match storage {
            Storage::Paged(image) => image
                .active_pixel_mask_name()
                .is_some_and(|name| image.named_mask_is_writable(&name).unwrap_or(false)),
            Storage::Temp { image, .. } => image
                .concat_source()
                .is_some_and(|source| source.mask_writable()),
        }) && writable;
        let advisory_lock = with_pixels!(&image, storage => match storage {
            Storage::Paged(_) => false,
            Storage::Temp { image, .. } => {
                image.image_type_name() == "MIRIADImage"
                    || image
                        .concat_source()
                        .is_some_and(|source| source.has_advisory_lock())
            }
        });
        let expression_operands = match &mut expression {
            Some(expression) => std::mem::take(&mut expression.paths),
            None => image.concat_expression_operands(),
        };
        let mut nested_operands = image.concat_operands();
        if path != resolved_path {
            nested_operands.push(lexical_absolute(path));
        }
        Ok(Self {
            path,
            resolved_path,
            shape: image.shape().to_vec(),
            kind: image.kind(),
            coordinates,
            coordinate_record,
            info: image_info(&image)?,
            info_record: image_info_record(&image).cloned(),
            units,
            misc,
            history,
            has_mask: has_mask || image.fits().is_some_and(|fits| fits.has_mask),
            writable,
            writable_mask,
            advisory_lock,
            opened: (!tempclose || !expression_operands.is_empty()).then_some(image),
            expression,
            expression_operands,
            nested_operands,
            revision,
        })
    }

    /// Returns a borrowed [`ConcatInput`] descriptor for geometry planning.
    pub fn geometry(&self) -> ConcatInput<'_> {
        ConcatInput {
            path: self.path,
            shape: &self.shape,
            coordinates: &self.coordinates,
            image_info: &self.info,
            brightness_unit: &self.units,
        }
    }
}

/// Checks that overwriting `output` cannot clobber any expression operand referenced by `inputs`.
pub fn protect_expression_sources(
    output: &Path,
    inputs: &[ConcatInputEntry<'_>],
) -> ImageToolResult<()> {
    saved_expression::check_output_dependencies(
        output,
        inputs.iter().flat_map(|input| &input.expression_operands),
        true,
    )
}

/// Updates `first`'s coordinate record when concatenation converted a coordinate type.
pub fn coordinate_record(
    first: &ConcatInputEntry<'_>,
    plan: &ConcatenationPlan,
) -> ImageToolResult<RecordValue> {
    let mut record = first.coordinate_record.clone();
    if let Some(index) = plan.changed_coordinate {
        let coordinates = CoordinateSystem::record_coordinates(&record)?;
        let old = &coordinates[index].field;
        let old_kind = old.trim_end_matches(|character: char| character.is_ascii_digit());
        let replacement = plan.coordinates.to_record();
        let coordinates = CoordinateSystem::record_coordinates(&replacement)?;
        let new = &coordinates[index].field;
        let new_kind = new.trim_end_matches(|character: char| character.is_ascii_digit());
        let value = replacement
            .get(new)
            .expect("replacement coordinate")
            .clone();
        record.remove(old);
        record.upsert(format!("{new_kind}{}", &old[old_kind.len()..]), value);
    }
    Ok(record)
}

/// Merges `source`'s `miscinfo` fields into `output`, disambiguating collisions with `_N`.
pub fn merge_misc(output: &mut RecordValue, source: &RecordValue) {
    for field in source.fields() {
        let mut name = field.name.clone();
        let mut suffix = 1;
        while output.get(&name).is_some() {
            name = format!("{}_{suffix}", field.name);
            suffix += 1;
        }
        output.push(RecordField::new(name, field.value.clone()));
    }
}

/// Builds the combined `imageinfo` record (global or per-plane beams) across `sources`.
pub fn beam_records(
    plan: &ConcatenationPlan,
    sources: &[ConcatInputEntry<'_>],
) -> ImageToolResult<RecordValue> {
    let first = &sources[plan.order[0]];
    let mut record = first
        .info_record
        .clone()
        .unwrap_or_else(|| first.info.to_record());
    let beams = &plan.image_info.beam_set;
    if beams == &first.info.beam_set {
        return Ok(record);
    }
    record.remove("restoringbeam");
    record.remove("perplanebeams");
    if beams.is_empty() {
        return Ok(record);
    }
    let (spectral, stokes) = plane_axes(&first.coordinate_record)?;
    let channel_concat = spectral == Some(plan.axis) && beams.n_channels() == plan.shape[plan.axis];
    let stokes_concat = stokes == Some(plan.axis) && beams.n_stokes() == plan.shape[plan.axis];
    let origins = if channel_concat || stokes_concat {
        plan.order
            .iter()
            .flat_map(|&index| {
                (0..sources[index].shape[plan.axis]).map(move |local| (index, local))
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let equivalent = plan
        .order
        .iter()
        .map(|&index| &sources[index])
        .find(|input| &input.info.beam_set == beams);
    let beam = |channel, plane| {
        let (input, source_channel, source_plane) = if channel_concat {
            let (index, local) = origins[channel];
            (Some(&sources[plan.beam_sources[index]]), local, plane)
        } else if stokes_concat {
            let (index, local) = origins[plane];
            (Some(&sources[plan.beam_sources[index]]), channel, local)
        } else {
            (equivalent, channel, plane)
        };
        Value::Record(input.map_or_else(
            || beams.beam(channel, plane).to_record(),
            |input| {
                let source = &input.info.beam_set;
                let channel = if source.n_channels() == 1 {
                    0
                } else {
                    source_channel
                };
                let plane = if source.n_stokes() == 1 {
                    0
                } else {
                    source_plane
                };
                stored_beam_record(input.info_record.as_ref(), source, channel, plane)
            },
        ))
    };
    if beams.is_single() {
        record.upsert("restoringbeam", beam(0, 0));
    } else if beams.is_multi() {
        let mut grid = beams.to_record();
        for plane in 0..beams.n_stokes() {
            for channel in 0..beams.n_channels() {
                grid.upsert(
                    format!("*{}", plane * beams.n_channels() + channel),
                    beam(channel, plane),
                );
            }
        }
        record.upsert("perplanebeams", Value::Record(grid));
    }
    Ok(record)
}
