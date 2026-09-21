// SPDX-License-Identifier: LGPL-3.0-or-later

//! Single bounded conversion at the existing weighted callback boundary.
//! Terminal source authority remains with WeightingReplayCompletion; this owner
//! guarantees ordered complete consumption into the native file and band support.

use super::input::{NativeStore, NativeStoreWriter, StorePlan};
use crate::{
    ManagedSpillStorage, WeightingExecutionState, WeightingPlanFragment, WeightingStreamingMode,
    WorkExecutionContext,
};
use casa_imaging_model::{CompiledProblem, FiniteValuePolicy, MeasurementSetReadAccess};
use casa_imaging_reconstruction::{
    WeightingReplayChunk, WeightingReplaySummary,
    runtime_adapter::{BandPlan, NativeBlock, NativeInput, NativeLayout},
};
use std::{io, time::Instant};

pub(super) struct NativePreparation<'a> {
    input: Option<NativeInput>,
    source: &'a MeasurementSetReadAccess,
    plan: StorePlan,
    finite_values: FiniteValuePolicy,
    writer: NativeStoreWriter,
    bands: Vec<BandPlan>,
    output_hz: &'a [f64],
    blocks: u64,
    samples: u64,
    failed: bool,
    callback_nanos: u64,
    support_nanos: u64,
    write_nanos: u64,
    pair_visits: u64,
}

pub(super) struct PreparedNative {
    pub(super) store: NativeStore,
    pub(super) bands: Vec<BandPlan>,
    pub(super) layout: NativeLayout,
}

impl<'a> NativePreparation<'a> {
    /// Execute the admitted selected-source traversal. This returns only a
    /// prepared store: replay completion still belongs to the scheduler's I/O
    /// fence callback, and band results cannot enter reconciliation before it.
    pub(super) fn traverse(
        mut self,
        context: WorkExecutionContext<'_>,
        fragment: &WeightingPlanFragment<'_>,
        problem: &CompiledProblem,
        selected: casa_ms::BoundSelectedObservation,
        weighting: &mut WeightingExecutionState,
    ) -> io::Result<PreparedNative> {
        if fragment.streaming_mode() != Some(WeightingStreamingMode::NaturalInitial) {
            return Err(io::Error::other(
                "native cube preparation requires natural weighting",
            ));
        }
        weighting
            .traverse_initial_bounded_stream(
                context,
                fragment,
                problem,
                Some(selected),
                |chunk, _| self.consume(chunk),
            )
            .map_err(io::Error::other)?;
        let (replay, _, _) = weighting.pending_replay_inputs().ok_or_else(|| {
            io::Error::other("native preparation lacks terminal source traversal")
        })?;
        self.finish(replay)
    }

    /// The caller admits this plan's flat block/encoding arena together with
    /// the weighted source chunk, band plans and other live preparation owners.
    pub(super) fn new(
        storage: &ManagedSpillStorage,
        plan: StorePlan,
        problem: &'a CompiledProblem,
        output_hz: &'a [f64],
        bands: Vec<BandPlan>,
    ) -> io::Result<Self> {
        let [source] = problem.selected_observation().read_set().sources() else {
            return Err(io::Error::other("native cube requires one selected source"));
        };
        let selection = source.selection();
        let ([dd], [spw], [pol]) = (
            selection.data_descriptions(),
            selection.spectral_windows(),
            selection.correlations(),
        ) else {
            return Err(io::Error::other(
                "native cube requires one homogeneous selection",
            ));
        };
        if bands.is_empty()
            || dd.spectral_window_id() != spw.spectral_window_id()
            || dd.polarization_id() != pol.polarization_id()
            || selection.rows().selected_row_count() != plan.rows
            || spw.channel_indices().len() != plan.channels
            || pol.products().len() != plan.correlations
            || output_hz.len() != problem.geometry().spectral().output_channels()
            || output_hz.iter().enumerate().any(|(index, hz)| {
                problem.geometry().spectral().channel_centre_hz(index) != Some(*hz)
            })
            || bands[0].core().start != 0
            || bands
                .last()
                .is_none_or(|band| band.core().end != output_hz.len())
            || bands
                .windows(2)
                .any(|pair| pair[0].core().end != pair[1].core().start)
        {
            return Err(io::Error::other(
                "invalid native preparation layout or output coverage",
            ));
        }
        Ok(Self {
            input: None,
            source,
            plan,
            finite_values: problem.numerics().finite_values(),
            writer: NativeStoreWriter::create(storage, plan)?,
            bands,
            output_hz,
            blocks: 0,
            samples: 0,
            failed: false,
            callback_nanos: 0,
            support_nanos: 0,
            write_nanos: 0,
            pair_visits: 0,
        })
    }

    pub(super) fn consume(&mut self, chunk: &WeightingReplayChunk) -> io::Result<()> {
        let started = Instant::now();
        if self.failed {
            return Err(io::Error::other("native preparation failed earlier"));
        }
        self.failed = true;
        if chunk.sequence() != self.blocks || chunk.samples().is_empty() {
            return Err(io::Error::other("native preparation chunk order mismatch"));
        }
        if self.input.is_none() {
            let address = chunk.samples()[0].selected().address();
            let selection = self.source.selection();
            let dd = selection.data_descriptions()[0];
            if address.measurement_set != self.source.measurement_set()
                || u32::try_from(address.data_description_id).ok() != Some(dd.data_description_id())
                || address.spectral_window_id != dd.spectral_window_id()
                || address.polarization_id != dd.polarization_id()
            {
                return Err(io::Error::other(
                    "native input does not match selected source",
                ));
            }
            let layout = NativeLayout::new(
                address,
                selection.spectral_windows()[0].channel_indices().to_vec(),
                selection.correlations()[0]
                    .products()
                    .iter()
                    .map(|product| (product.correlation_index(), product.correlation_type()))
                    .collect(),
            )?;
            self.input = Some(NativeInput::new(
                layout,
                NativeBlock::new(
                    self.plan.block_rows,
                    self.plan.channels,
                    self.plan.correlations,
                )?,
                self.finite_values,
            )?);
        }
        let Self {
            input,
            writer,
            bands,
            output_hz,
            support_nanos,
            write_nanos,
            pair_visits,
            ..
        } = self;
        input
            .as_mut()
            .expect("initialized above")
            .push(chunk.samples(), |block| {
                let started = Instant::now();
                *pair_visits +=
                    BandPlan::observe_all(bands, block, output_hz).map_err(io::Error::other)?;
                *support_nanos += started.elapsed().as_nanos() as u64;
                let started = Instant::now();
                let result = writer.append(block);
                *write_nanos += started.elapsed().as_nanos() as u64;
                result
            })?;
        self.samples = self
            .samples
            .checked_add(chunk.samples().len() as u64)
            .ok_or_else(|| io::Error::other("native preparation sample count overflow"))?;
        self.blocks = self
            .blocks
            .checked_add(1)
            .ok_or_else(|| io::Error::other("native preparation block count overflow"))?;
        self.failed = false;
        self.callback_nanos += started.elapsed().as_nanos() as u64;
        Ok(())
    }

    pub(super) fn finish(self, replay: &WeightingReplaySummary) -> io::Result<PreparedNative> {
        if self.failed
            || self.samples != replay.sample_count()
            || self.blocks != replay.block_count()
        {
            return Err(io::Error::other(
                "native preparation did not consume the complete weighted replay",
            ));
        }
        let Self {
            input,
            mut writer,
            mut bands,
            output_hz,
            mut callback_nanos,
            mut support_nanos,
            mut write_nanos,
            mut pair_visits,
            ..
        } = self;
        let started = Instant::now();
        let (_, layout) = input
            .ok_or_else(|| io::Error::other("native preparation is empty"))?
            .finish(|block| {
                let started = Instant::now();
                pair_visits += BandPlan::observe_all(&mut bands, block, output_hz)
                    .map_err(io::Error::other)?;
                support_nanos += started.elapsed().as_nanos() as u64;
                let started = Instant::now();
                let result = writer.append(block);
                write_nanos += started.elapsed().as_nanos() as u64;
                result
            })?;
        callback_nanos += started.elapsed().as_nanos() as u64;
        eprintln!(
            "streaming_cube_preparation callback_inclusive_nanos={callback_nanos} support_nanos={support_nanos} write_nanos={write_nanos} pair_visits={pair_visits} coverage_bytes={} coverage_hash_calls={}",
            replay.coverage_proof_bytes(),
            replay.coverage_proof_hash_calls(),
        );
        Ok(PreparedNative {
            store: writer.finish()?,
            bands,
            layout,
        })
    }
}
