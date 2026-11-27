// SPDX-License-Identifier: LGPL-3.0-or-later
//! Own the MAIN-table lock and the MeasurementSet read under that lock.

use std::path::{Path, PathBuf};

use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::{MeasurementSet, MsResult, MsSelection, MsSelectionError, ResolvedMsSelection};
use casa_tables::{TableError, TableWriteLock, table_cache_name};

use crate::model_columns::{has_column, initialize_calibrater_columns, initialize_scratch_column};
use crate::session::resolve_casa_calibration_selection;

/// An open MeasurementSet and the owned write lock held across a calibration mutation.
pub struct OpenedMs {
    path: PathBuf,
    /// MeasurementSet read while holding the MAIN-table lock.
    pub measurement_set: MeasurementSet,
    /// Exclusive MAIN-table lock; hand it to storage-changing operations that reopen the table.
    pub lock: TableWriteLock,
}

impl OpenedMs {
    /// Canonicalizes the table path, acquires its MAIN write lock, then reads the MeasurementSet.
    /// Lock and open failures propagate.
    pub fn open(path: &Path) -> MsResult<Self> {
        let path = table_cache_name(path)?;
        let lock = TableWriteLock::acquire_to_reread(&path, 0)?;
        Self::read_holding(path, lock)
    }

    fn read_holding(path: PathBuf, lock: TableWriteLock) -> MsResult<Self> {
        let measurement_set = MeasurementSet::open(&path)?;
        Ok(Self {
            path,
            measurement_set,
            lock,
        })
    }

    /// Initializes the requested scratch visibility column and retains the resulting lock.
    pub fn initialize(mut self, column: VisibilityDataColumn) -> MsResult<Self> {
        self.lock = initialize_scratch_column(&mut self.measurement_set, column, self.lock)?;
        Ok(self)
    }

    /// Initializes the requested corrected and model columns under the retained write lock,
    /// optionally using compressed storage.
    pub fn initialize_calibrater(
        mut self,
        compress: bool,
        add_corrected: bool,
        add_model: bool,
    ) -> MsResult<Self> {
        self.lock = initialize_calibrater_columns(
            &mut self.measurement_set,
            compress,
            add_corrected,
            add_model,
            self.lock,
        )?;
        Ok(self)
    }

    /// Resolves CASA calibration selectors against the locked MeasurementSet's current metadata.
    pub fn resolve(
        &self,
        selection: &MsSelection,
    ) -> Result<ResolvedMsSelection, MsSelectionError> {
        resolve_casa_calibration_selection(&self.measurement_set, selection)
    }

    /// Drops and rereads the MeasurementSet while retaining the exclusive lock.
    pub fn reload(self) -> MsResult<Self> {
        drop(self.measurement_set);
        Self::read_holding(self.path, self.lock)
    }

    /// Closes the MeasurementSet and explicitly releases the lock, returning any release error.
    pub fn release(self) -> MsResult<()> {
        drop(self.measurement_set);
        self.lock.release()?;
        Ok(())
    }
}

/// How calibration selected the backing MeasurementSet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachmentMode {
    /// A direct open fixes the MeasurementSet until close or another open.
    Open,
    /// A selection attaches its own MeasurementSet.
    Selection,
}

/// Access options retained with an attachment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttachmentOptions {
    /// Open the data for later mutation.
    pub write_access: bool,
    /// Attach a scratch MODEL_DATA iterator on writable inputs.
    pub use_scratch: bool,
}

impl Default for AttachmentOptions {
    fn default() -> Self {
        Self {
            write_access: true,
            use_scratch: false,
        }
    }
}

/// An attachment or selection failed before prediction could write data.
#[derive(Debug, thiserror::Error)]
pub enum AttachmentError {
    /// No MeasurementSet is attached to this tool.
    #[error("no MeasurementSet is attached")]
    Unattached,
    /// The requested selection matched no MAIN rows.
    #[error("Selection is empty: you may want to review this MSs selection")]
    EmptySelection,
    /// An explicit selection was rejected and needs a successful replacement.
    #[error("the current visibility selection was rejected")]
    RejectedSelection,
    /// A direct open has no selected spectral windows for a UV cut.
    #[error("uvrange requires a configured visibility selection")]
    UvRangeRequiresSelection,
    /// The named attachment was opened read-only.
    #[error("Please make sure MS is writable when using Imager::ft")]
    ReadOnly,
    /// The active Multi-MS iterator includes an earlier read-only dataset.
    #[error("an active MeasurementSet is read-only")]
    ReadOnlyInput,
    /// The supplied UV interval is outside CASA's accepted domain.
    #[error("Invalid uvmin and uvmax: {0}, {1}")]
    InvalidUvRange(f64, f64),
    /// A MeasurementSet or its write lock could not be opened.
    #[error(transparent)]
    MeasurementSet(#[from] casa_ms::MsError),
    /// The stored selection could not be resolved against the attached dataset.
    #[error(transparent)]
    Selection(#[from] MsSelectionError),
    /// The supplied image or component model could not be prepared.
    #[error(transparent)]
    PredictionPreparation(casa_ms::MsError),
}

impl From<TableError> for AttachmentError {
    fn from(error: TableError) -> Self {
        Self::MeasurementSet(error.into())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SelectionState {
    Initial,
    Selected,
    Rejected,
}

/// Canonical path and selector inputs retained between calibration calls.
///
/// Physical row ids and channel mappings are resolved on the reopened
/// MeasurementSet under its MAIN lock whenever a caller mutates the dataset.
#[derive(Clone, Debug, PartialEq)]
pub struct CalibrationAttachment {
    path: PathBuf,
    mode: AttachmentMode,
    options: AttachmentOptions,
    selection: MsSelection,
    imager_channels: Option<ImagerChannelSelection>,
    state: SelectionState,
    uv_ranges: Vec<[f64; 2]>,
}

impl CalibrationAttachment {
    /// Attach the specified MeasurementSet, optionally initializing its scratch model.
    ///
    /// # Errors
    ///
    /// Returns an error if the table, its lock, or the selection cannot be read.
    pub fn attach(
        path: &Path,
        mode: AttachmentMode,
        options: AttachmentOptions,
        selection: MsSelection,
    ) -> Result<Self, AttachmentError> {
        Self::attach_with_channels(path, mode, options, selection, None)
    }

    fn attach_with_channels(
        path: &Path,
        mode: AttachmentMode,
        options: AttachmentOptions,
        selection: MsSelection,
        imager_channels: Option<ImagerChannelSelection>,
    ) -> Result<Self, AttachmentError> {
        let path = table_cache_name(path)?;
        let attachment = Self {
            path,
            mode,
            options,
            selection,
            imager_channels,
            state: match mode {
                AttachmentMode::Open => SelectionState::Initial,
                AttachmentMode::Selection => SelectionState::Selected,
            },
            uv_ranges: Vec::new(),
        };
        if options.write_access && options.use_scratch {
            let opened = OpenedMs::open(&attachment.path)?;
            let opened = opened.initialize(VisibilityDataColumn::ModelData)?;
            if mode == AttachmentMode::Selection {
                attachment.require_selected_rows(&opened.measurement_set)?;
            }
            opened.release()?;
        } else {
            let ms = MeasurementSet::open(&attachment.path)?;
            if mode == AttachmentMode::Selection {
                if attachment.imager_channels.is_some()
                    && options.use_scratch
                    && !has_column(&ms, VisibilityDataColumn::ModelData)
                {
                    return Err(casa_ms::MsError::InvalidInput(format!(
                        "Invalid Table operation: Table::addColumn; table {} is not writable",
                        attachment.path.display(),
                    ))
                    .into());
                }
                attachment.require_selected_rows(&ms)?;
            }
        }
        Ok(attachment)
    }

    fn require_selected_rows(&self, ms: &MeasurementSet) -> Result<(), AttachmentError> {
        if self.resolve_holding(ms, &[])?.selected_rows.is_empty() {
            return Err(AttachmentError::EmptySelection);
        }
        Ok(())
    }

    /// Canonical path used for both table access and locking.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Mode that established the association between this tool and the MS.
    pub fn mode(&self) -> AttachmentMode {
        self.mode
    }

    /// Retained write and scratch options.
    pub fn options(&self) -> AttachmentOptions {
        self.options
    }

    /// Resolve the current selection for a read.
    ///
    /// # Errors
    ///
    /// Returns an error if the attached dataset or selector cannot be read.
    pub fn resolve(&self) -> Result<ResolvedMsSelection, AttachmentError> {
        let ms = MeasurementSet::open(&self.path)?;
        self.resolve_holding(&ms, &self.uv_ranges)
    }

    /// Replace the selection on this attachment and reset prior UV narrowing.
    ///
    /// An empty or invalid selector disables mutations until the next
    /// successful selection, matching CASA's direct-open state.
    ///
    /// # Errors
    ///
    /// Returns an error if the table or selection cannot be read.
    pub fn replace_selection(&mut self, selection: MsSelection) -> Result<bool, AttachmentError> {
        self.replace_selection_with_channels(selection, None)
    }

    fn replace_selection_with_channels(
        &mut self,
        selection: MsSelection,
        channels: Option<ImagerChannelSelection>,
    ) -> Result<bool, AttachmentError> {
        match self.select_with_channels(selection, channels) {
            Ok(()) => Ok(true),
            Err(
                AttachmentError::EmptySelection
                | AttachmentError::Selection(MsSelectionError::Domain(
                    casa_ms::MsError::InvalidInput(_) | casa_ms::MsError::VersionError(_),
                )),
            ) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Validates and replaces the attached input's selection. An empty or invalid selection is
    /// reported and leaves the attachment in a rejected selection state.
    pub fn select(&mut self, selection: MsSelection) -> Result<(), AttachmentError> {
        self.select_with_channels(selection, None)
    }

    fn select_with_channels(
        &mut self,
        selection: MsSelection,
        channels: Option<ImagerChannelSelection>,
    ) -> Result<(), AttachmentError> {
        let ms = MeasurementSet::open(&self.path)?;
        self.state = SelectionState::Rejected;
        self.uv_ranges.clear();
        let resolved = imager_selection(&ms, &selection, channels.as_ref())?;
        if resolve_casa_calibration_selection(&ms, &resolved)?
            .selected_rows
            .is_empty()
        {
            return Err(AttachmentError::EmptySelection);
        }
        self.selection = selection;
        self.imager_channels = channels;
        self.state = SelectionState::Selected;
        Ok(())
    }

    /// Borrows the currently stored selector. Selection state separately tracks whether it was
    /// rejected.
    pub fn selection(&self) -> &MsSelection {
        &self.selection
    }

    /// Clears explicit row and channel selections and wavelength bounds.
    pub fn reset_selection(&mut self) {
        self.selection = MsSelection::default();
        self.imager_channels = None;
        self.state = SelectionState::Initial;
        self.uv_ranges.clear();
    }

    /// Narrow the current selection using strict wavelength bounds.
    ///
    /// CASA leaves the current selection in place when a valid interval
    /// matches no rows. A nonpositive upper bound uses its 1e10 default.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds or a failed selection.
    pub fn narrow_uvrange(&mut self, minimum: f64, maximum: f64) -> Result<(), AttachmentError> {
        let maximum = if maximum <= 0.0 { 1e10 } else { maximum };
        if !(minimum >= 0.0 && maximum > minimum && maximum.is_finite()) {
            return Err(AttachmentError::InvalidUvRange(minimum, maximum));
        }
        match self.state {
            SelectionState::Initial => return Err(AttachmentError::UvRangeRequiresSelection),
            SelectionState::Rejected => return Err(AttachmentError::RejectedSelection),
            SelectionState::Selected => {}
        }
        let ms = MeasurementSet::open(&self.path)?;
        let mut ranges = self.uv_ranges.clone();
        ranges.push([minimum, maximum]);
        if !self.resolve_holding(&ms, &ranges)?.selected_rows.is_empty() {
            self.uv_ranges = ranges;
        }
        Ok(())
    }

    /// Reopen under MAIN and supply the selection and its matching write lock.
    ///
    /// Publish through operations that accept the held lock.
    ///
    /// # Errors
    ///
    /// Returns an error for read-only inputs, failed locking or selection, a
    /// rejected operation, or a failed lock release.
    pub fn with_write<T>(
        &self,
        operation: impl FnOnce(
            &mut MeasurementSet,
            &ResolvedMsSelection,
            &mut TableWriteLock,
        ) -> Result<T, AttachmentError>,
    ) -> Result<T, AttachmentError> {
        if !self.options.write_access {
            return Err(AttachmentError::ReadOnly);
        }
        if self.state == SelectionState::Rejected {
            return Err(AttachmentError::RejectedSelection);
        }
        let mut opened = OpenedMs::open(&self.path)?;
        let selected = self.resolve_holding(&opened.measurement_set, &self.uv_ranges);
        let result = selected.and_then(|selected| {
            operation(&mut opened.measurement_set, &selected, &mut opened.lock)
        });
        let release = opened.release();
        result.and_then(|value| {
            release?;
            Ok(value)
        })
    }

    fn resolve_holding(
        &self,
        ms: &MeasurementSet,
        uv_ranges: &[[f64; 2]],
    ) -> Result<ResolvedMsSelection, AttachmentError> {
        if self.state == SelectionState::Rejected {
            return Err(AttachmentError::RejectedSelection);
        }
        let selected = imager_selection(ms, &self.selection, self.imager_channels.as_ref())?;
        if uv_ranges.is_empty() {
            return Ok(resolve_casa_calibration_selection(ms, &selected)?);
        }
        let mut selection = if selected.selectdata {
            selected.into_owned()
        } else {
            MsSelection::default()
        };
        let factor = mean_inverse_wavelength(ms, selection.spw.as_deref())?.powi(2);
        let distance = format!("(SQUARE(UVW[1]) + SQUARE(UVW[2]))*{factor:.5e}");
        let mut predicates = selection
            .msselect
            .take()
            .into_iter()
            .filter(|text| !text.trim().is_empty())
            .map(|text| format!("({text})"))
            .collect::<Vec<_>>();
        for &[minimum, maximum] in uv_ranges {
            predicates.push(format!(
                "({distance}>{:.5e} AND {distance}<{:.5e})",
                minimum * minimum,
                maximum * maximum,
            ));
        }
        selection.msselect = Some(predicates.join(" AND "));
        Ok(resolve_casa_calibration_selection(ms, &selection)?)
    }
}

/// Channel counts, offsets and strides for the legacy imager selector.
#[derive(Clone, Debug, PartialEq)]
pub struct ImagerChannelSelection {
    /// Number of channels for each selected spectral window, or one for all windows.
    pub count: Vec<i64>,
    /// First channel for each selected window, or one for all windows.
    pub start: Vec<i64>,
    /// Stride for each selected window, or one for all windows.
    pub step: Vec<i64>,
}

fn imager_selection<'a>(
    ms: &MeasurementSet,
    selection: &'a MsSelection,
    channels: Option<&ImagerChannelSelection>,
) -> Result<std::borrow::Cow<'a, MsSelection>, AttachmentError> {
    let Some(channels) =
        channels.filter(|channels| !matches!(channels.count.as_slice(), [n] if *n < 0))
    else {
        return Ok(std::borrow::Cow::Borrowed(selection));
    };
    let windows = ms.resolve_spw_selector(selection.spw.as_deref().unwrap_or("*"))?;
    let mut ids = windows.spw_ids().to_vec();
    ids.sort_unstable();
    ids.dedup();
    // The legacy selector keeps its SPW and row selection when the iterator
    // cannot apply a malformed explicit channel vector.
    if [&channels.count, &channels.start, &channels.step]
        .iter()
        .any(|values| values.len() != 1 && values.len() != ids.len())
    {
        return Ok(std::borrow::Cow::Borrowed(selection));
    }
    let argument = |values: &[i64], index| {
        if values.len() == 1 {
            values[0]
        } else {
            values[index]
        }
    };
    let spectral = ms.spectral_window()?;
    let mut ranges = Vec::with_capacity(ids.len());
    for (index, &id) in ids.iter().enumerate() {
        let count = argument(&channels.count, index);
        let start = argument(&channels.start, index);
        let step = argument(&channels.step, index).max(1);
        let total = i64::from(spectral.num_chan(id)?);
        if !(0..total).contains(&start) {
            return Ok(std::borrow::Cow::Borrowed(selection));
        }
        // Keep a valid first channel when the stride runs past the end of
        // the window. This boundary can abort the legacy iterator.
        let available = ((total - start) / step).max(1);
        let count = if count <= 0 {
            available
        } else {
            count.min(available)
        };
        let end = start + (count - 1) * step;
        ranges.push(format!("{id}:{start}~{end}^{step}"));
    }
    let mut selected = selection.clone();
    selected.spw = Some(ranges.join(","));
    Ok(std::borrow::Cow::Owned(selected))
}

/// MeasurementSet associations for one imager object.
#[derive(Debug, Default)]
pub struct ImagerAttachments {
    inputs: Vec<CalibrationAttachment>,
    current_only: bool,
}

impl ImagerAttachments {
    /// Replace the existing associations with an explicit MeasurementSet.
    ///
    /// # Errors
    ///
    /// Returns an error if the new table or required scratch preparation fails.
    pub fn open(&mut self, path: &Path, options: AttachmentOptions) -> Result<(), AttachmentError> {
        let input = CalibrationAttachment::attach(
            path,
            AttachmentMode::Open,
            options,
            MsSelection::default(),
        )?;
        self.inputs = vec![input];
        self.current_only = false;
        Ok(())
    }

    /// Replace the selection on an explicitly opened MS, or add another input.
    ///
    /// A direct open keeps the original dataset and its access options.
    /// Selection-only mode retains its path, options and selector per input.
    ///
    /// # Errors
    ///
    /// Returns an error if an attachment or selector cannot be read.
    pub fn select(
        &mut self,
        path: Option<&Path>,
        options: AttachmentOptions,
        selection: MsSelection,
    ) -> Result<bool, AttachmentError> {
        self.select_channels(path, options, selection, None)
    }

    /// Select rows and explicit imager channel vectors, retaining the request for each input.
    ///
    /// # Errors
    /// The input, channel vectors or selector cannot be applied.
    pub fn select_channels(
        &mut self,
        path: Option<&Path>,
        options: AttachmentOptions,
        selection: MsSelection,
        channels: Option<ImagerChannelSelection>,
    ) -> Result<bool, AttachmentError> {
        if let Some(input) = self.inputs.first_mut()
            && input.mode == AttachmentMode::Open
        {
            return input.replace_selection_with_channels(selection, channels);
        }
        let path = path.ok_or(AttachmentError::Unattached)?;
        let input = CalibrationAttachment::attach_with_channels(
            path,
            AttachmentMode::Selection,
            options,
            selection,
            channels,
        )?;
        if self.current_only
            && let Some(previous) = self.inputs.last_mut()
        {
            previous.uv_ranges.clear();
        }
        self.inputs.push(input);
        self.current_only = false;
        Ok(true)
    }

    /// The attached inputs in the order supplied by the caller.
    pub fn inputs(&self) -> &[CalibrationAttachment] {
        &self.inputs
    }

    /// Inputs currently used by the visibility iterator.
    pub fn active_inputs(&self) -> &[CalibrationAttachment] {
        if self.current_only {
            &self.inputs[self.inputs.len().saturating_sub(1)..]
        } else {
            &self.inputs
        }
    }

    /// Validate the active group before prediction writes to any input.
    ///
    /// # Errors
    ///
    /// Returns an error when inputs are absent, read-only, or rejected.
    pub fn writable_inputs(&self) -> Result<&[CalibrationAttachment], AttachmentError> {
        let active = self.active_inputs();
        let current = active.last().ok_or(AttachmentError::Unattached)?;
        if !current.options.write_access {
            return Err(AttachmentError::ReadOnly);
        }
        if active.iter().any(|input| !input.options.write_access) {
            return Err(AttachmentError::ReadOnlyInput);
        }
        if active
            .iter()
            .any(|input| input.state == SelectionState::Rejected)
        {
            return Err(AttachmentError::RejectedSelection);
        }
        Ok(active)
    }

    /// Replaces the latest input's setjy selection and updates which attachments are active. Fails
    /// if the imager has no attached input or validation fails.
    pub fn retain_setjy_selection(
        &mut self,
        selection: MsSelection,
    ) -> Result<(), AttachmentError> {
        let input = self.inputs.last_mut().ok_or(AttachmentError::Unattached)?;
        input.replace_selection(selection)?;
        self.current_only = input.mode == AttachmentMode::Selection;
        Ok(())
    }

    /// Narrow the currently selected input.
    ///
    /// # Errors
    ///
    /// Returns an error for an unattached imager or a failed UV-range selection.
    pub fn narrow_uvrange(&mut self, minimum: f64, maximum: f64) -> Result<(), AttachmentError> {
        let current = self.inputs.last_mut().ok_or(AttachmentError::Unattached)?;
        current.narrow_uvrange(minimum, maximum)?;
        // The legacy iterator is rebuilt over the current input, including
        // when the UV predicate matched no rows and retained its selection.
        self.current_only = current.mode == AttachmentMode::Selection;
        Ok(())
    }

    /// Release the stored table associations and per-input selectors.
    pub fn close(&mut self) {
        self.inputs.clear();
        self.current_only = false;
    }
}

fn mean_inverse_wavelength(ms: &MeasurementSet, selector: Option<&str>) -> MsResult<f64> {
    const LIGHT_SPEED_M_S: f64 = 299_792_458.0;
    let spectral = ms.spectral_window()?;
    let mut windows = match selector
        .map(|selector| ms.resolve_spw_selector(selector))
        .transpose()?
    {
        Some(selection) if !selection.selects_all() => selection.spw_ids().to_vec(),
        _ => (0..spectral.row_count()).collect(),
    };
    // CASA's UV cut uses the whole selected spectral windows in table order.
    windows.sort_unstable();
    windows.dedup();
    let (mut sum, mut count) = (0.0, 0);
    for window in windows {
        let frequencies = spectral.chan_freq(window)?;
        sum += frequencies.iter().sum::<f64>();
        count += frequencies.len();
    }
    if count == 0 {
        return Err(casa_ms::MsError::InvalidInput(
            "selected spectral windows have no channels".into(),
        ));
    }
    Ok(sum / (count as f64 * LIGHT_SPEED_M_S))
}
