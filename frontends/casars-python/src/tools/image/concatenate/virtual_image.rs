// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use casa_core::RecordValue;
use casa_images::ConcatenationPlan;
use casa_images::{ConcatImage, ConcatLockState, ConcatMember};
use casa_images::{ConcatManifest, is_image_concat};
use casa_images::{
    ImageError, ImageInfo, ImageInterface, ImagePixel, ImagePixelType, MutableImageInterface,
    TempImage,
};
use casa_lattices::Lattice;
use casa_tables::LockType;
use ndarray::ArrayD;
use num_complex::{Complex32, Complex64};
use pyo3::prelude::*;

use super::owned_members::OwnedOperands;
use super::{
    Input, Request, beam_records, coordinate_record, merge_misc, plane_axes,
    protect_expression_sources,
};
use crate::tools::LogLine;
use crate::tools::image::attachment::{AttachedImage, registry};
use crate::tools::image::handle::{
    ImageHandle, Storage, raised, same_file_identity, with_pixels, with_storage,
};
use crate::tools::image::saved_expression::SavedExpressionInputs;
use crate::tools::image::saved_expression::StagedOutput;

pub(in crate::tools::image) fn open(path: &Path) -> PyResult<AttachedImage> {
    let image = load(path, false, &mut HashSet::new(), None)?;
    AttachedImage::with_virtual_concat(image, path, &mut registry())
}

pub(in crate::tools::image) fn open_handle(path: &Path) -> Result<ImageHandle, ImageError> {
    load(path, false, &mut HashSet::new(), None)
        .map_err(|error| ImageError::CasaSelection(error.to_string()))
}

pub(in crate::tools::image) fn open_expression_leaf(
    path: &Path,
    active: &mut HashSet<PathBuf>,
    output: Option<&StagedOutput<'_>>,
) -> PyResult<ImageHandle> {
    load(path, false, active, output)
}

pub(in crate::tools::image) fn open_metadata(
    path: &Path,
    output: &StagedOutput<'_>,
) -> PyResult<ImageHandle> {
    let image = load(path, false, &mut HashSet::new(), Some(output))?;
    super::saved_expression::check_output_dependencies(
        output.target,
        image
            .concat_operands()
            .iter()
            .chain(&image.concat_expression_operands()),
        false,
    )?;
    Ok(image)
}

pub(in crate::tools::image) fn check_relocation(source: &Path, target: &Path) -> PyResult<()> {
    super::relocation::check_rename(source, target)
}

fn load(
    path: &Path,
    outer_tempclose: bool,
    active: &mut HashSet<PathBuf>,
    output: Option<&StagedOutput<'_>>,
) -> PyResult<ImageHandle> {
    let root = path.canonicalize().map_err(raised)?;
    if !active.insert(root.clone()) {
        return Err(raised(format!(
            "Cycle in virtual image concatenation at {}",
            root.display()
        )));
    }
    let manifest = ConcatManifest::open(&root).map_err(raised)?;
    let entry = root.join("imageconcat.json");
    let referent = entry.canonicalize().map_err(raised)?;
    let mut operand_entries = vec![entry];
    if referent != operand_entries[0] {
        operand_entries.push(referent);
    }
    let mut paths = manifest.paths(&root);
    if let Some(output) = output {
        for path in &mut paths {
            if let Some(staged) = output.substitute(path) {
                *path = staged;
            }
        }
    }
    let tempclose = outer_tempclose || manifest.temp_close();
    let inputs = open_members(&paths, tempclose, active, output)?;
    let geometry: Vec<_> = inputs.iter().map(Input::geometry).collect();
    let kind = manifest.pixel_type().map_err(raised)?;
    validate_types(&inputs, kind)?;
    let axis = i64::try_from(manifest.axis()).map_err(raised)?;
    let plan = ConcatenationPlan::from_manifest(&geometry, axis).map_err(geometry_error)?;
    let output = assemble(
        &root,
        inputs,
        operand_entries,
        plan,
        manifest.misc_info().map_err(raised)?,
        tempclose,
        kind,
    )?;
    active.remove(&root);
    Ok(output)
}

pub(super) fn stage_nomove(
    request: &Request<'_>,
    paths: &[PathBuf],
    final_path: &Path,
    stage: &Path,
    logs: &mut Vec<LogLine>,
) -> PyResult<ImageHandle> {
    let output_key = final_path
        .canonicalize()
        .or_else(|_| -> std::io::Result<PathBuf> {
            let parent = final_path.parent().expect("named virtual output parent");
            Ok(parent.canonicalize()?.join(final_path.file_name().unwrap()))
        })
        .map_err(raised)?;
    for path in paths {
        if path
            .canonicalize()
            .map_err(raised)?
            .starts_with(&output_key)
        {
            return Err(raised(
                "The concatenation output contains one of its input images",
            ));
        }
    }
    let inputs = open_members(paths, request.tempclose, &mut HashSet::new(), None)?;
    protect_expression_sources(final_path, &inputs)?;
    let (kind, plan, misc) = creation_plan(request, &inputs, logs)?;
    let manifest = ConcatManifest::new(
        final_path,
        kind,
        plan.axis,
        request.tempclose,
        plan.order
            .iter()
            .map(|&index| inputs[index].path.to_path_buf()),
        &misc,
    )
    .map_err(raised)?;
    fs::create_dir(stage).map_err(raised)?;
    manifest.write(stage).map_err(raised)?;
    assemble(
        final_path,
        inputs,
        Vec::new(),
        plan,
        Some(misc),
        request.tempclose,
        kind,
    )
}

pub(super) fn stage_owned(
    request: &Request<'_>,
    paths: &[PathBuf],
    final_path: &Path,
    stage: &Path,
    moving: bool,
    logs: &mut Vec<LogLine>,
) -> PyResult<OwnedOperands> {
    let mut members = OwnedOperands::inspect(paths, final_path, moving)?;
    let validation_paths = members.validation_paths();
    let inputs = open_members(&validation_paths, true, &mut HashSet::new(), None)?;
    protect_expression_sources(final_path, &inputs)?;
    let (kind, plan, misc) = creation_plan(request, &inputs, logs)?;
    members.capture_validated_inputs(inputs)?;
    let manifest = ConcatManifest::new(
        final_path,
        kind,
        plan.axis,
        request.tempclose,
        plan.order
            .iter()
            .map(|&index| members.destination(final_path, index)),
        &misc,
    )
    .map_err(raised)?;
    fs::create_dir(stage).map_err(raised)?;
    manifest.write(stage).map_err(raised)?;
    Ok(members)
}

fn creation_plan(
    request: &Request<'_>,
    inputs: &[Input<'_>],
    logs: &mut Vec<LogLine>,
) -> PyResult<(ImagePixelType, ConcatenationPlan, RecordValue)> {
    let kind = inputs[0].kind;
    validate_types(inputs, kind)?;
    let geometry: Vec<_> = inputs.iter().map(Input::geometry).collect();
    let plan = ConcatenationPlan::new(&geometry, request.axis, request.relax, request.reorder)
        .map_err(geometry_error)?;
    logs.extend(plan.warnings.iter().cloned().map(LogLine::warning));
    let mut misc = RecordValue::default();
    for &index in &plan.order {
        merge_misc(&mut misc, &inputs[index].misc);
    }
    Ok((kind, plan, misc))
}

fn open_members<'a>(
    paths: &'a [PathBuf],
    tempclose: bool,
    active: &mut HashSet<PathBuf>,
    output: Option<&StagedOutput<'_>>,
) -> PyResult<Vec<Input<'a>>> {
    paths
        .iter()
        .map(|path| {
            let resolved = path.canonicalize().map_err(raised)?;
            if is_image_concat(&resolved) {
                let image = load(&resolved, tempclose, active, output)?;
                Input::from_image(path, image, tempclose, None)
            } else {
                Input::open_resolved(path, &resolved, tempclose, active, output)
            }
        })
        .collect()
}

fn validate_types(inputs: &[Input<'_>], kind: ImagePixelType) -> PyResult<()> {
    if inputs.iter().any(|input| input.kind != kind) {
        return Err(raised(
            "Concatenation of images of different data types is not supported",
        ));
    }
    Ok(())
}

fn geometry_error(error: ImageError) -> PyErr {
    match error {
        ImageError::CasaSelection(message)
            if message == "Lattice shapes are not the same on non-concatenation axes" =>
        {
            raised("Lattice shapes inconsistent")
        }
        other => raised(other),
    }
}

fn assemble(
    path: &Path,
    inputs: Vec<Input<'_>>,
    operand_entries: Vec<PathBuf>,
    plan: ConcatenationPlan,
    misc: Option<RecordValue>,
    tempclose: bool,
    kind: ImagePixelType,
) -> PyResult<ImageHandle> {
    let first = &inputs[plan.order[0]];
    let coords = coordinate_record(first, &plan)?;
    let beams = beam_records(&plan, &inputs)?;
    let units = first.units.clone();
    let misc = misc.unwrap_or_else(|| {
        let mut misc = RecordValue::default();
        for &index in &plan.order {
            merge_misc(&mut misc, &inputs[index].misc);
        }
        misc
    });
    let mut ordered: Vec<_> = inputs.into_iter().map(Some).collect();
    let inputs: Vec<_> = plan
        .order
        .iter()
        .map(|&index| ordered[index].take().unwrap())
        .collect();
    match kind {
        ImagePixelType::Float32 => typed(
            path,
            inputs,
            operand_entries,
            plan,
            misc,
            units,
            coords,
            beams,
            tempclose,
        )
        .map(ImageHandle::Float),
        ImagePixelType::Float64 => typed(
            path,
            inputs,
            operand_entries,
            plan,
            misc,
            units,
            coords,
            beams,
            tempclose,
        )
        .map(ImageHandle::Double),
        ImagePixelType::Complex32 => typed(
            path,
            inputs,
            operand_entries,
            plan,
            misc,
            units,
            coords,
            beams,
            tempclose,
        )
        .map(ImageHandle::Complex),
        ImagePixelType::Complex64 => typed(
            path,
            inputs,
            operand_entries,
            plan,
            misc,
            units,
            coords,
            beams,
            tempclose,
        )
        .map(ImageHandle::DComplex),
    }
}

#[allow(clippy::too_many_arguments)]
fn typed<T: MemberPixel>(
    path: &Path,
    inputs: Vec<Input<'_>>,
    mut operand_entries: Vec<PathBuf>,
    plan: ConcatenationPlan,
    misc: RecordValue,
    units: String,
    coords: RecordValue,
    beams: RecordValue,
    tempclose: bool,
) -> PyResult<Storage<T>> {
    let mut members: Vec<Arc<dyn ConcatMember<T>>> = Vec::with_capacity(inputs.len());
    let mut by_path: HashMap<_, Arc<dyn ConcatMember<T>>> = HashMap::new();
    let mut history = Vec::new();
    for mut input in inputs {
        history.append(&mut input.history);
        operand_entries.append(&mut input.nested_operands);
        let path = input.resolved_path;
        if let Some(member) = by_path.get(&path) {
            members.push(Arc::clone(member));
            continue;
        }
        let expected = input
            .revision
            .map(|revision| revision.file)
            .map_or_else(|| fs::metadata(&path).map_err(raised), Ok)?;
        let storage = input
            .opened
            .take()
            .map(T::unpack)
            .transpose()
            .map_err(raised)?;
        let member: Arc<dyn ConcatMember<T>> = Arc::new(Member::<T> {
            path: path.clone(),
            expected,
            shape: input.shape,
            masked: input.has_mask,
            writable: input.writable,
            writable_mask: input.writable_mask,
            advisory_lock: input.advisory_lock,
            beam_axes: plane_axes(&input.coordinate_record)?,
            tempclose: tempclose && input.expression_operands.is_empty(),
            storage: Mutex::new(storage),
            expression: input.expression,
            expression_operands: input.expression_operands,
        });
        by_path.insert(path, Arc::clone(&member));
        members.push(member);
    }
    let joined = ConcatImage::new(
        path.to_path_buf(),
        plan,
        members,
        operand_entries,
        misc,
        units,
        tempclose,
    )
    .map_err(raised)?;
    let mut image = TempImage::from_virtual_concat(joined, beams).map_err(raised)?;
    image.append_history_entries(&history).map_err(raised)?;
    Ok(Storage::Temp {
        image: Box::new(image),
        record: coords,
        fits: None,
    })
}

struct Member<T: MemberPixel> {
    path: PathBuf,
    expected: fs::Metadata,
    shape: Vec<usize>,
    masked: bool,
    writable: bool,
    writable_mask: bool,
    advisory_lock: bool,
    beam_axes: (Option<usize>, Option<usize>),
    tempclose: bool,
    storage: Mutex<Option<Storage<T>>>,
    expression: Option<SavedExpressionInputs>,
    expression_operands: Vec<PathBuf>,
}

impl<T: MemberPixel> Member<T> {
    fn slot(&self) -> MutexGuard<'_, Option<Storage<T>>> {
        self.storage.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn check_path(&self) -> Result<(), ImageError> {
        if let Some(expression) = &self.expression {
            return expression.check_name(&self.path).map_err(expression_error);
        }
        if fs::metadata(&self.path)
            .is_ok_and(|current| same_file_identity(&self.expected, &current))
        {
            return Ok(());
        }
        Err(ImageError::Io(format!(
            "Concatenation member {} changed",
            self.path.display()
        )))
    }

    fn reopen(&self) -> Result<Storage<T>, ImageError> {
        self.check_path()?;
        let image = ImageHandle::open_for_concatenation(&self.path)?;
        if image.shape() != self.shape {
            return Err(ImageError::Io(format!(
                "Concatenation member {} changed shape",
                self.path.display(),
            )));
        }
        self.check_path()?;
        T::unpack(image)
    }

    fn use_storage<R>(
        &self,
        operation: impl FnOnce(&mut Storage<T>) -> Result<R, ImageError>,
    ) -> Result<R, ImageError> {
        self.check_path()?;
        let mut storage = self.slot();
        if self.tempclose
            && let Some(mut opened) = storage.take()
        {
            return operation(&mut opened);
        }
        match storage.as_mut() {
            Some(storage) => operation(storage),
            None => operation(&mut self.reopen()?),
        }
    }

    fn read_storage<R>(
        &self,
        read: impl FnOnce(&Storage<T>) -> Result<R, ImageError>,
    ) -> Result<R, ImageError> {
        if let Some(expression) = &self.expression
            && !lock_expression_inputs(&expression.retained, LockType::Read, 0, true)?
        {
            return Err(ImageError::Io(
                "Could not acquire the expression Read lock".into(),
            ));
        }
        self.use_storage(|storage| {
            require(storage, LockType::Read)?;
            read(storage)
        })
    }
}

impl<T: MemberPixel> ConcatMember<T> for Member<T> {
    fn path(&self) -> &Path {
        &self.path
    }
    fn shape(&self) -> &[usize] {
        &self.shape
    }
    fn has_mask(&self) -> bool {
        self.masked
    }

    fn mask_writable(&self) -> bool {
        self.writable_mask
    }
    fn is_writable(&self) -> bool {
        self.writable
    }

    fn read(
        &self,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, ImageError> {
        self.read_storage(|storage| T::read(storage, start, count, stride))
    }

    fn read_mask(
        &self,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, ImageError> {
        self.read_storage(|storage| T::read_mask(storage, start, count, stride))
    }

    fn write(&self, values: &ArrayD<T>, start: &[usize]) -> Result<(), ImageError> {
        self.use_storage(|storage| {
            require(storage, LockType::Write)?;
            with_storage!(storage, image => image.put_slice(values, start))?;
            storage.save()
        })
    }

    fn write_mask(&self, values: &ArrayD<bool>, start: &[usize]) -> Result<(), ImageError> {
        self.use_storage(|storage| {
            require(storage, LockType::Write)?;
            with_storage!(storage, image => {
                let name = image.active_pixel_mask_name().ok_or_else(|| ImageError::CasaSelection(
                    "The member's pixel mask is no longer available".into(),
                ))?;
                image.put_mask_slice(&name, values, start)?;
            });
            storage.save()
        })
    }

    fn check_image_info_write(&self) -> Result<(), ImageError> {
        self.use_storage(|storage| match storage {
            Storage::Paged(image)
                if !image.is_writable()
                    || casa_tables::publication::check_write_access(
                        &self.path.join("table.dat"),
                    )
                    .is_err() =>
            {
                Err(ImageError::CasaSelection(format!(
                    "Table {} cannot be opened for read/write",
                    self.path.display(),
                )))
            }
            Storage::Temp { image, .. } => image
                .concat_source()
                .map_or(Ok(()), ConcatImage::check_image_info_write),
            Storage::Paged(_) => Ok(()),
        })
    }

    fn write_image_info(
        &self,
        info: &ImageInfo,
        record: &RecordValue,
        axis: usize,
        offset: usize,
    ) -> Result<(), ImageError> {
        let beams = &info.beam_set;
        let nchan = beams.n_channels();
        let nstokes = beams.n_stokes();
        let split = if self.beam_axes.0 == Some(axis) && nchan > 1 {
            Some((offset..offset + self.shape[axis], 0..nstokes))
        } else if self.beam_axes.1 == Some(axis) && nstokes > 1 {
            Some((0..nchan, offset..offset + self.shape[axis]))
        } else {
            None
        };
        let local = if let Some((channels, stokes)) = split {
            let mut local = info.clone();
            local.beam_set =
                beams.subset(&channels.collect::<Vec<_>>(), &stokes.collect::<Vec<_>>())?;
            local.to_record()
        } else {
            record.clone()
        };
        self.use_storage(|storage| {
            require(storage, LockType::Write)?;
            with_storage!(storage, image => image.set_image_info_record(local))?;
            storage.save()
        })
    }

    fn lock(&self, kind: LockType, attempts: u32) -> Result<bool, ImageError> {
        self.check_path()?;
        if let Some(expression) = &self.expression {
            return lock_expression_inputs(&expression.retained, kind, attempts, false);
        }
        let mut slot = self.slot();
        if let Some(storage) = slot.as_mut() {
            return lock(storage, kind, attempts);
        }
        let mut storage = self.reopen()?;
        if lock(&mut storage, kind, attempts)? {
            *slot = Some(storage);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn close_after_lock(&self) -> Result<(), ImageError> {
        if self.tempclose {
            self.unlock()
        } else {
            Ok(())
        }
    }

    fn has_lock(&self, kind: LockType) -> bool {
        if let Some(expression) = &self.expression {
            return expression
                .retained
                .iter()
                .all(|input| input.lock().has_storage_lock(kind));
        }
        self.advisory_lock
            || self
                .slot()
                .as_ref()
                .is_some_and(|storage| has_lock(storage, kind))
    }

    fn has_advisory_lock(&self) -> bool {
        self.advisory_lock
    }

    fn expression_operands(&self) -> &[PathBuf] {
        &self.expression_operands
    }

    fn lock_state(&self) -> ConcatLockState {
        if let Some(expression) = &self.expression {
            return ConcatLockState::Nested(
                expression
                    .retained
                    .iter()
                    .map(expression_lock_state)
                    .collect(),
            );
        }
        match self.slot().as_ref() {
            Some(Storage::Paged(image)) => ConcatLockState::Leaf(table_lock_state(image)),
            Some(Storage::Temp { image, .. }) => image
                .concat_source()
                .map_or(ConcatLockState::Leaf(None), |source| {
                    ConcatLockState::Nested(source.lock_states())
                }),
            None => ConcatLockState::Leaf(None),
        }
    }

    fn restore_locks(&self, state: ConcatLockState) -> Result<(), ImageError> {
        if let Some(expression) = &self.expression {
            let ConcatLockState::Nested(states) = state else {
                unreachable!("captured expression locks")
            };
            return expression
                .retained
                .iter()
                .zip(states)
                .rev()
                .try_for_each(|(input, state)| restore_expression_lock(input, state));
        }
        let mut slot = self.slot();
        if let Some(storage) = slot.as_mut() {
            match (&mut *storage, state) {
                (Storage::Paged(image), ConcatLockState::Leaf(previous)) => {
                    if table_lock_state(image) != previous {
                        unlock(storage)?;
                        if let Some(kind) = previous
                            && !lock(storage, kind, 1)?
                        {
                            return Err(ImageError::Io(
                                "Could not restore a concatenation member lock".into(),
                            ));
                        }
                    }
                }
                (Storage::Temp { image, .. }, ConcatLockState::Nested(states)) => {
                    if let Some(source) = image.concat_source() {
                        source.restore_locks(states)?;
                    }
                }
                (_, ConcatLockState::Leaf(None)) => {}
                _ => unreachable!("captured locks belong to the same member"),
            }
        }
        Ok(())
    }

    fn unlock(&self) -> Result<(), ImageError> {
        if let Some(expression) = &self.expression {
            return expression
                .retained
                .iter()
                .try_for_each(|input| input.lock().unlock_storage().map_err(expression_error));
        }
        let mut storage = self.slot();
        if let Some(storage) = storage.as_mut() {
            unlock(storage)?;
        }
        if self.tempclose {
            *storage = None;
        }
        Ok(())
    }

    fn release_requested_lock(&self) -> Result<(), ImageError> {
        if let Some(expression) = &self.expression {
            return expression.retained.iter().try_for_each(|input| {
                input
                    .lock()
                    .release_requested_lock()
                    .map_err(expression_error)
            });
        }
        let mut slot = self.slot();
        let Some(storage) = slot.as_mut() else {
            return Ok(());
        };
        match storage {
            #[cfg(unix)]
            Storage::Paged(image) => image.release_requested_lock()?,
            Storage::Temp { image, .. } => {
                if let Some(source) = image.concat_source() {
                    source.release_requested_lock()?;
                }
            }
            #[cfg(not(unix))]
            Storage::Paged(_) => {}
        }
        if self.tempclose
            && !has_lock(storage, LockType::Read)
            && !has_lock(storage, LockType::Write)
        {
            *slot = None;
        }
        Ok(())
    }
}

fn expression_error(error: PyErr) -> ImageError {
    ImageError::CasaSelection(error.to_string())
}

fn expression_lock_state(input: &AttachedImage) -> ConcatLockState {
    let image = input.lock();
    if let Some(state) = with_pixels!(&*image, storage => match storage {
        Storage::Temp { image, .. } => image.concat_source().map(ConcatImage::lock_states),
        Storage::Paged(_) => None,
    }) {
        return ConcatLockState::Nested(state);
    }
    ConcatLockState::Leaf(
        [LockType::Write, LockType::Read]
            .into_iter()
            .find(|&kind| image.has_storage_lock(kind)),
    )
}

fn restore_expression_lock(
    input: &AttachedImage,
    state: ConcatLockState,
) -> Result<(), ImageError> {
    let mut image = input.lock();
    let previous = match state {
        ConcatLockState::Leaf(previous) => previous,
        ConcatLockState::Nested(states) => {
            return with_pixels!(&*image, storage => match storage {
                Storage::Temp { image, .. } => image.concat_source()
                    .expect("captured concatenation locks").restore_locks(states),
                Storage::Paged(_) => unreachable!("captured concatenation locks"),
            });
        }
    };
    let current = [LockType::Write, LockType::Read]
        .into_iter()
        .find(|&kind| image.has_storage_lock(kind));
    if current != previous {
        image.unlock_storage().map_err(expression_error)?;
        if let Some(kind) = previous
            && !image.lock_storage(kind, 1).map_err(expression_error)?
        {
            return Err(ImageError::Io(
                "Could not restore an expression operand lock".into(),
            ));
        }
    }
    Ok(())
}

fn lock_expression_inputs(
    inputs: &[AttachedImage],
    kind: LockType,
    attempts: u32,
    section_read: bool,
) -> Result<bool, ImageError> {
    let mut acquired = Vec::new();
    for input in inputs {
        let prior = expression_lock_state(input);
        let result = input
            .lock_expression_operand(kind, attempts, section_read)
            .map_err(expression_error);
        match result {
            Ok(true) => acquired.push((input, prior)),
            refused => {
                for (input, state) in acquired.into_iter().rev() {
                    restore_expression_lock(input, state)?;
                }
                return refused;
            }
        }
    }
    Ok(true)
}

fn table_lock_state<T: ImagePixel>(image: &casa_images::PagedImage<T>) -> Option<LockType> {
    #[cfg(unix)]
    return [LockType::Write, LockType::Read]
        .into_iter()
        .find(|&kind| !image.uses_advisory_locks() && image.has_lock(kind));
    #[cfg(not(unix))]
    {
        let _ = image;
        None
    }
}

fn require<T: ImagePixel>(storage: &mut Storage<T>, kind: LockType) -> Result<(), ImageError> {
    if lock(storage, kind, 0)? {
        Ok(())
    } else {
        Err(ImageError::CasaSelection(format!(
            "Could not acquire the image {kind:?} lock"
        )))
    }
}

fn lock<T: ImagePixel>(
    storage: &mut Storage<T>,
    kind: LockType,
    attempts: u32,
) -> Result<bool, ImageError> {
    match storage {
        #[cfg(unix)]
        Storage::Paged(image) => image.lock(kind, attempts),
        Storage::Temp { image, .. } => image
            .concat_source()
            .map_or(Ok(true), |source| source.lock(kind, attempts)),
        #[cfg(not(unix))]
        Storage::Paged(_) => Ok(true),
    }
}

fn has_lock<T: ImagePixel>(storage: &Storage<T>, kind: LockType) -> bool {
    match storage {
        #[cfg(unix)]
        Storage::Paged(image) => image.has_lock(kind),
        Storage::Temp { image, .. } => image
            .concat_source()
            .is_none_or(|source| source.has_lock(kind)),
        #[cfg(not(unix))]
        Storage::Paged(_) => true,
    }
}

fn unlock<T: ImagePixel>(storage: &mut Storage<T>) -> Result<(), ImageError> {
    match storage {
        #[cfg(unix)]
        Storage::Paged(image) => image.unlock(),
        Storage::Temp { image, .. } => image.concat_source().map_or(Ok(()), ConcatImage::unlock),
        #[cfg(not(unix))]
        Storage::Paged(_) => Ok(()),
    }
}

trait MemberPixel: ImagePixel {
    fn unpack(image: ImageHandle) -> Result<Storage<Self>, ImageError>;

    fn read(
        storage: &Storage<Self>,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<Self>, ImageError> {
        with_storage!(storage, image => Lattice::get_slice(image.as_ref(), start, count, stride))
            .map_err(Into::into)
    }

    fn read_mask(
        storage: &Storage<Self>,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, ImageError> {
        with_storage!(storage, image => image.get_mask_slice(start, count, stride))
    }
}

fn changed_type() -> ImageError {
    ImageError::CasaSelection("The concatenation member's pixel type changed".into())
}

impl MemberPixel for f32 {
    fn unpack(image: ImageHandle) -> Result<Storage<Self>, ImageError> {
        match image {
            ImageHandle::Float(storage) => Ok(storage),
            _ => Err(changed_type()),
        }
    }

    fn read(
        storage: &Storage<Self>,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<Self>, ImageError> {
        storage
            .with_float_view(|view| view.get_slice(start, count, stride))
            .map_err(Into::into)
    }

    fn read_mask(
        storage: &Storage<Self>,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, ImageError> {
        storage.with_float_view(|view| view.get_mask_slice(start, count, stride))
    }
}

macro_rules! native_members {
    ($($pixel:ty => $kind:ident),* $(,)?) => {
        $(impl MemberPixel for $pixel {
            fn unpack(image: ImageHandle) -> Result<Storage<Self>, ImageError> {
                match image {
                    ImageHandle::$kind(storage) => Ok(storage),
                    _ => Err(changed_type()),
                }
            }
        })*
    };
}

native_members!(f64 => Double, Complex32 => Complex, Complex64 => DComplex);
