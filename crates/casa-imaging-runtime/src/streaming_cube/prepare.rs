// SPDX-License-Identifier: LGPL-3.0-or-later

//! Ordered, zero-copy handoff from worker-owned native rows to the store.
//! Source traversal completion remains with the existing weighting I/O fence.

use super::input::{NativeStore, NativeStoreWriter, StorePlan};
use crate::{
    ManagedSpillStorage, WeightingExecutionState, WeightingPlanFragment, WorkExecutionContext,
};
use casa_imaging_model::{CompiledProblem, MeasurementSetReadAccess};
use casa_imaging_reconstruction::{
    WeightingReplaySummary,
    runtime_adapter::{BandPlan, NativeBlock, NativeLayout},
};
use std::{io, time::Instant};

pub(super) struct NativePreparation<'a> {
    layout: Option<NativeLayout>,
    source: &'a MeasurementSetReadAccess,
    plan: StorePlan,
    writer: NativeStoreWriter,
    bands: Vec<BandPlan>,
    output_hz: &'a [f64],
    rows: u64,
    previous_row: Option<u64>,
    failed: bool,
    support_nanos: u128,
    write_nanos: u128,
    pair_visits: u64,
}

pub(super) struct PreparedNative {
    pub(super) store: NativeStore,
    pub(super) bands: Vec<BandPlan>,
    pub(super) layout: NativeLayout,
}

impl<'a> NativePreparation<'a> {
    pub(super) fn traverse(
        mut self,
        context: WorkExecutionContext<'_>,
        fragment: &WeightingPlanFragment<'_>,
        problem: &CompiledProblem,
        selected: casa_ms::BoundSelectedObservation,
        weighting: &mut WeightingExecutionState,
    ) -> io::Result<PreparedNative> {
        weighting
            .traverse_native_initial_stream(
                context,
                fragment,
                problem,
                selected,
                |parts, layout| self.consume(parts, layout),
            )
            .map_err(io::Error::other)?;
        let (replay, _, _) = weighting.pending_replay_inputs().ok_or_else(|| {
            io::Error::other("native preparation lacks terminal source traversal")
        })?;
        self.finish(replay)
    }

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
            || output_hz
                .iter()
                .enumerate()
                .any(|(i, hz)| problem.geometry().spectral().channel_centre_hz(i) != Some(*hz))
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
            layout: None,
            source,
            plan,
            writer: NativeStoreWriter::create(storage, plan)?,
            bands,
            output_hz,
            rows: 0,
            previous_row: None,
            failed: false,
            support_nanos: 0,
            write_nanos: 0,
            pair_visits: 0,
        })
    }

    pub(super) fn consume(
        &mut self,
        parts: &[&NativeBlock],
        layout: &NativeLayout,
    ) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other("native preparation failed earlier"));
        }
        self.failed = true;
        let selection = self.source.selection();
        let dd = selection.data_descriptions()[0];
        let address = layout.address;
        if address.measurement_set != self.source.measurement_set()
            || u32::try_from(address.data_description_id).ok() != Some(dd.data_description_id())
            || address.spectral_window_id != dd.spectral_window_id()
            || address.polarization_id != dd.polarization_id()
            || layout.channels != selection.spectral_windows()[0].channel_indices()
            || !layout
                .correlations
                .iter()
                .copied()
                .eq(selection.correlations()[0]
                    .products()
                    .iter()
                    .map(|c| (c.correlation_index(), c.correlation_type())))
        {
            return Err(io::Error::other(
                "native input does not match selected source",
            ));
        }
        for block in parts {
            for row in &block.metadata {
                if self
                    .previous_row
                    .is_some_and(|previous| row.physical_row <= previous)
                {
                    return Err(io::Error::other("native input row order mismatch"));
                }
                self.previous_row = Some(row.physical_row);
            }
            let started = Instant::now();
            self.pair_visits += BandPlan::observe_all(&mut self.bands, block, self.output_hz)
                .map_err(io::Error::other)?;
            self.support_nanos += started.elapsed().as_nanos();
            self.rows = self
                .rows
                .checked_add(block.metadata.len() as u64)
                .ok_or_else(|| io::Error::other("native row count overflow"))?;
        }
        let started = Instant::now();
        self.writer.append_parts(parts)?;
        self.write_nanos += started.elapsed().as_nanos();
        if self.layout.is_none() {
            self.layout = Some(layout.clone());
        }
        self.failed = false;
        Ok(())
    }

    pub(super) fn finish(self, replay: &WeightingReplaySummary) -> io::Result<PreparedNative> {
        if self.failed
            || self.rows != self.plan.rows
            || self
                .rows
                .checked_mul(self.plan.channels as u64)
                .and_then(|n| n.checked_mul(self.plan.correlations as u64))
                != Some(replay.sample_count())
        {
            return Err(io::Error::other(
                "native preparation did not consume the complete source",
            ));
        }
        eprintln!(
            "streaming_cube_preparation support_nanos={} write_nanos={} pair_visits={} coverage_bytes={} coverage_hash_calls={}",
            self.support_nanos,
            self.write_nanos,
            self.pair_visits,
            replay.coverage_proof_bytes(),
            replay.coverage_proof_hash_calls()
        );
        Ok(PreparedNative {
            store: self.writer.finish()?,
            bands: self.bands,
            layout: self
                .layout
                .ok_or_else(|| io::Error::other("native preparation is empty"))?,
        })
    }
}
