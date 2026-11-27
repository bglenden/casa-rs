// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_calibration::continuum::{
    ContinuumScratchFit, ContinuumScratchOptions, LegacyContinuumTransform,
    initialize_continuum_scratch_holding,
};
use casa_ms::schema::main_table::OptionalMainColumn;
use casa_ms::{MeasurementSet, MsSelection};
use casa_mstransform::{LegacyMsTool, LegacyTimeOptions, MsTransformTaskOptions};
use casa_tables::TableWriteLock;
use pyo3::prelude::*;

use super::metadata_link::MetadataLink;
use super::partition::{
    all_if_minus_one, combine_argument, intent_argument, obs_argument, optional_expression,
    output_argument, visibility_columns, whichcol_argument,
};
use super::selection_indices::selection_text;
use super::visibility::VisibilityMain;
use super::{PyMsTool, runtime_error};

const SCRATCH: [&str; 2] = ["CORRECTED_DATA", "MODEL_DATA"];

#[pymethods]
impl PyMsTool {
    /// Fit the selected continuum using correlation-specific visibility weights.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (field = None, fitspw = None, spw = None, solint = None, fitorder = 0, mode = "subtract".to_owned()), text_signature = "($self, field=[], fitspw=[], spw=[], solint=[], fitorder=0, mode='subtract')")]
    fn continuumsub(
        &self,
        py: Python<'_>,
        field: Option<&Bound<'_, PyAny>>,
        fitspw: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        solint: Option<&Bound<'_, PyAny>>,
        fitorder: i64,
        #[pyo3(from_py_with = mode_argument)] mode: String,
    ) -> PyResult<bool> {
        let options = scratch_options(field, fitspw, spw, solint, fitorder, mode, false)?;
        py.detach(|| self.fit_continuum_scratch(&options))
    }

    /// Fit the continuum with the legacy visibility iterator's flag handling.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (field = None, fitspw = None, spw = None, solint = None, fitorder = 0, mode = "subtract".to_owned()), text_signature = "($self, field=[], fitspw=[], spw=[], solint=[], fitorder=0, mode='subtract')")]
    fn continuumsubold(
        &self,
        py: Python<'_>,
        field: Option<&Bound<'_, PyAny>>,
        fitspw: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        solint: Option<&Bound<'_, PyAny>>,
        fitorder: i64,
        #[pyo3(from_py_with = mode_argument)] mode: String,
    ) -> PyResult<bool> {
        let options = scratch_options(field, fitspw, spw, solint, fitorder, mode, true)?;
        py.detach(|| self.fit_continuum_scratch(&options))
    }

    /// Write a selected MeasurementSet after removing a frequency polynomial.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outputms = String::new(), fitspw = None, fitorder = 1, combine = String::new(), spw = None, unionspw = None, field = None, scan = None, intent = String::new(), correlation = String::new(), obs = String::new(), whichcol = "CORRECTED_DATA".to_owned()), text_signature = "($self, outputms='', fitspw=[], fitorder=1, combine='', spw='*', unionspw='*', field='', scan='', intent='', correlation='', obs='', whichcol='CORRECTED_DATA')")]
    fn contsub(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = output_argument)] outputms: String,
        fitspw: Option<&Bound<'_, PyAny>>,
        fitorder: i64,
        #[pyo3(from_py_with = combine_argument)] combine: String,
        spw: Option<&Bound<'_, PyAny>>,
        unionspw: Option<&Bound<'_, PyAny>>,
        field: Option<&Bound<'_, PyAny>>,
        scan: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = intent_argument)] intent: String,
        #[pyo3(from_py_with = correlation_argument)] correlation: String,
        #[pyo3(from_py_with = obs_argument)] obs: String,
        #[pyo3(from_py_with = whichcol_argument)] whichcol: String,
    ) -> PyResult<bool> {
        let union = all_if_minus_one(unionspw)?.unwrap_or_else(|| "*".into());
        let output = all_if_minus_one(spw)?.unwrap_or_else(|| "*".into());
        let fields = all_if_minus_one(field)?;
        let selection = MsSelection {
            field: fields.clone(),
            scan: optional_expression(scan)?,
            intent: nonempty(intent),
            correlation: nonempty(correlation.to_ascii_uppercase()),
            observation: nonempty(obs),
            ..Default::default()
        };
        let Some(columns) = visibility_columns(&whichcol)? else {
            return Ok(false);
        };
        let parameters = format!(
            "fieldids={} spwids={output} whichcol='{whichcol}'",
            fields.unwrap_or_default()
        );
        let fit_spw = all_if_minus_one(fitspw)?.unwrap_or_default();
        let input_view = if let Ok(order) = usize::try_from(fitorder) {
            let [column] = columns.as_slice() else {
                return Ok(false);
            };
            if !matches!(
                column,
                OptionalMainColumn::Data
                    | OptionalMainColumn::CorrectedData
                    | OptionalMainColumn::ModelData
            ) {
                return Ok(false);
            }
            Some(Arc::new(LegacyContinuumTransform {
                column: column.name(),
                order,
                fit_spw,
                union: MsSelection {
                    spw: Some(union.clone()),
                    ..selection.clone()
                },
                combine: combine.to_ascii_lowercase(),
            })
                as Arc<dyn casa_mstransform::TransformInputView>)
        } else {
            None
        };
        let options = MsTransformTaskOptions {
            visibility_columns: Some(columns),
            input_view,
            legacy_continuum_output: (fitorder >= 0).then_some(output),
            legacy_tool: Some(LegacyMsTool::Partition(LegacyTimeOptions {
                combine_observations: false,
            })),
            ..Default::default()
        };
        self.write_subms(
            py,
            outputms,
            union,
            selection,
            options,
            ("contsub", Some(parameters)),
        )
    }
}

impl PyMsTool {
    fn fit_continuum_scratch(&self, options: &ContinuumScratchOptions) -> PyResult<bool> {
        let mut attachment = self.attached.lock().map_err(runtime_error)?;
        let attached = attachment
            .as_mut()
            .ok_or_else(|| runtime_error("Table column DATA_DESC_ID is unknown"))?;
        if !attached.writable {
            return Err(runtime_error("MeasurementSet is not writable"));
        }
        let link = attached.working_metadata_link();
        let view = link.working_view().map_err(runtime_error)?;
        run_continuum_scratch(&link, view.path(), options, &mut attached.write_locks)?;
        Ok(true)
    }
}

fn run_continuum_scratch(
    link: &MetadataLink,
    path: &Path,
    options: &ContinuumScratchOptions,
    retained: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> PyResult<()> {
    let initial = VisibilityMain::open(path)?;
    let view = MeasurementSet::open(path).map_err(runtime_error)?;
    let table = view.main_table();
    let observed = if table.column_accessor("FLOAT_DATA").is_ok() {
        "FLOAT_DATA"
    } else {
        "DATA"
    };
    let rows: Vec<_> = (0..view.row_count()).collect();
    let targets = initial.write_targets(&rows, &[observed])?;
    let root = table
        .reference_root_projection(&[], &[])
        .map_err(runtime_error)?
        .map(|projection| projection.path.canonicalize().map_err(runtime_error))
        .transpose()?;
    let paths = targets
        .iter()
        .map(|target| target.path.clone())
        .chain(root.iter().cloned())
        .collect::<BTreeSet<_>>();
    let mut temporary = Vec::new();
    for path in &paths {
        if !retained.values().any(|lock| lock.matches_table_path(path)) {
            temporary.push(TableWriteLock::acquire_to_reread(path, 0).map_err(runtime_error)?);
        }
    }
    let mut locks: BTreeMap<_, _> = retained
        .values_mut()
        .chain(&mut temporary)
        .map(|lock| (lock.path().to_path_buf(), lock))
        .collect();
    let mut written = BTreeSet::new();
    link.verify_directory().map_err(runtime_error)?;
    initial.verify_write_layout(&written)?;
    let publication = root
        .as_ref()
        .map(|root| {
            table
                .prepare_reference_columns_holding(&SCRATCH, continuum_lock(&mut locks, root)?)
                .map_err(runtime_error)
        })
        .transpose()?
        .flatten();
    let scratch = publication.as_ref().map_or(SCRATCH, |plan| {
        [plan.columns[0].as_str(), plan.columns[1].as_str()]
    });
    for target in targets {
        initial.verify_write_layout(&written)?;
        let mut member = MeasurementSet::open(&target.path).map_err(runtime_error)?;
        initialize_continuum_scratch_holding(
            &mut member,
            &target.rows.keys().copied().collect::<Vec<_>>(),
            &target.names[0],
            scratch,
            continuum_lock(&mut locks, &target.path)?,
        )
        .map_err(runtime_error)?;
        written.insert(target.path);
    }
    if let (Some(root), Some(publication)) = (&root, publication) {
        initial.verify_write_layout(&written)?;
        let root_table = MeasurementSet::open(root).map_err(runtime_error)?;
        publication
            .publish(root_table.main_table(), continuum_lock(&mut locks, root)?)
            .map_err(runtime_error)?;
    }
    let view = MeasurementSet::open(path).map_err(runtime_error)?;
    let fit = ContinuumScratchFit::prepare(&view, options).map_err(runtime_error)?;
    let rows = fit.row_indices().collect::<Vec<_>>();
    if rows.is_empty() {
        return Ok(());
    }
    let layout = VisibilityMain::open(path)?;
    written.clear();
    let mut members = Vec::new();
    for target in layout.write_targets(&rows, &SCRATCH)? {
        layout.verify_write_layout(&written)?;
        continuum_lock(&mut locks, &target.path)?;
        let member = MeasurementSet::open(&target.path).map_err(runtime_error)?;
        fit.check_destinations(&member, &target.rows, [&target.names[0], &target.names[1]])
            .map_err(runtime_error)?;
        members.push((target, member));
    }
    for (target, mut member) in members {
        layout.verify_write_layout(&written)?;
        let lock = continuum_lock(&mut locks, &target.path)?;
        fit.write_holding(
            &mut member,
            &target.rows,
            [&target.names[0], &target.names[1]],
            lock,
        )
        .map_err(runtime_error)?;
        written.insert(target.path);
    }
    Ok(())
}

fn continuum_lock<'a>(
    locks: &'a mut BTreeMap<PathBuf, &mut TableWriteLock>,
    path: &Path,
) -> PyResult<&'a mut TableWriteLock> {
    locks
        .get_mut(path)
        .filter(|lock| lock.matches_table_path(path))
        .map(|lock| &mut **lock)
        .ok_or_else(|| {
            runtime_error(format!(
                "Continuum write lock missing for {}",
                path.display()
            ))
        })
}

fn scratch_options(
    field: Option<&Bound<'_, PyAny>>,
    fitspw: Option<&Bound<'_, PyAny>>,
    spw: Option<&Bound<'_, PyAny>>,
    solint: Option<&Bound<'_, PyAny>>,
    fitorder: i64,
    mode: String,
    old_iterator: bool,
) -> PyResult<ContinuumScratchOptions> {
    Ok(ContinuumScratchOptions {
        field: optional_expression(field)?,
        fit_spw: optional_expression(fitspw)?.unwrap_or_default(),
        apply_spw: optional_expression(spw)?.unwrap_or_default(),
        solution_interval: optional_expression(solint)?.unwrap_or_default(),
        order: usize::try_from(fitorder)
            .map_err(|_| runtime_error("fitorder must be nonnegative"))?,
        mode,
        old_iterator,
    })
}

fn mode_argument(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "mode")
}

fn correlation_argument(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "correlation")
}

fn nonempty(text: String) -> Option<String> {
    (!text.is_empty()).then_some(text)
}
