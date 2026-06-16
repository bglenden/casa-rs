// SPDX-License-Identifier: LGPL-3.0-or-later

// Wave 4 defines the shared spectral-plane contracts before every spectral mode
// consumes every piece of the contract.
#![allow(dead_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[allow(dead_code)]
pub(crate) enum ImagingPassKind {
    WeightingDensity,
    Psf,
    InitialDirty,
    MinorCycleDiagnostics,
    MinorCycleUpdate,
    ResidualRefresh,
    ProductWrite,
}

impl ImagingPassKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::WeightingDensity => "weighting_density",
            Self::Psf => "psf",
            Self::InitialDirty => "initial_dirty",
            Self::MinorCycleDiagnostics => "minor_cycle_diagnostics",
            Self::MinorCycleUpdate => "minor_cycle_update",
            Self::ResidualRefresh => "residual_refresh",
            Self::ProductWrite => "product_write",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum SpectralEventStage {
    SourceRead,
    RowBlockPreparation,
    VisibilityRouting,
    WeightingDensity,
    PsfDirty,
    MinorCycleDiagnostics,
    MinorCycleUpdate,
    ResidualRefresh,
    PlaneStateLoad,
    PlaneStateStore,
    ProductWrite,
    CacheFill,
    CacheHit,
    CacheMiss,
    BackendExecution,
    Planner,
}

impl SpectralEventStage {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::SourceRead => "source_read",
            Self::RowBlockPreparation => "row_block_prepare",
            Self::VisibilityRouting => "visibility_routing",
            Self::WeightingDensity => "weighting_density",
            Self::PsfDirty => "psf_dirty",
            Self::MinorCycleDiagnostics => "minor_cycle_diagnostics",
            Self::MinorCycleUpdate => "minor_cycle_update",
            Self::ResidualRefresh => "residual_refresh",
            Self::PlaneStateLoad => "plane_state_load",
            Self::PlaneStateStore => "plane_state_store",
            Self::ProductWrite => "product_write",
            Self::CacheFill => "cache_fill",
            Self::CacheHit => "cache_hit",
            Self::CacheMiss => "cache_miss",
            Self::BackendExecution => "backend_execution",
            Self::Planner => "planner",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpectralObservabilityEvent {
    pub(crate) mode: &'static str,
    pub(crate) pass_kind: ImagingPassKind,
    pub(crate) stage: SpectralEventStage,
    pub(crate) slab_id: Option<usize>,
    pub(crate) plane_start: usize,
    pub(crate) plane_end: usize,
    pub(crate) row_block_rows: Option<usize>,
    pub(crate) bytes_read: Option<usize>,
    pub(crate) bytes_written: Option<usize>,
    pub(crate) worker_count: Option<usize>,
    pub(crate) backend: &'static str,
    pub(crate) elapsed_ms: Option<u64>,
    pub(crate) estimated_resident_bytes: Option<usize>,
}

impl SpectralObservabilityEvent {
    pub(crate) fn log_line(&self) -> String {
        format!(
            "spectral_slab_event mode={} pass_kind={} stage={} slab_id={} plane_start={} plane_end={} row_block_rows={} bytes_read={} bytes_written={} worker_count={} backend={} elapsed_ms={} estimated_resident_bytes={}",
            self.mode,
            self.pass_kind.as_str(),
            self.stage.as_str(),
            option_usize(self.slab_id),
            self.plane_start,
            self.plane_end,
            option_usize(self.row_block_rows),
            option_usize(self.bytes_read),
            option_usize(self.bytes_written),
            option_usize(self.worker_count),
            self.backend,
            self.elapsed_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unset".to_string()),
            option_usize(self.estimated_resident_bytes),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum SpectralInterpolationPolicy {
    Nearest,
    Linear,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SpectralPlaneDescriptor {
    pub(crate) output_index: usize,
    pub(crate) spw_id: Option<i32>,
    pub(crate) source_channel_start: usize,
    pub(crate) source_channel_count: usize,
    pub(crate) output_frequency_hz: Option<f64>,
    pub(crate) stokes: &'static str,
    pub(crate) field_ids: Vec<i32>,
    pub(crate) interpolation: SpectralInterpolationPolicy,
    pub(crate) coordinate_label: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpectralSlabManifest {
    pub(crate) slab_id: usize,
    pub(crate) plane_start: usize,
    pub(crate) plane_end: usize,
}

impl SpectralSlabManifest {
    pub(crate) fn for_planes(nplanes: usize, active_planes: usize) -> Vec<Self> {
        let active_planes = active_planes.max(1);
        let mut slabs = Vec::new();
        let mut plane_start = 0usize;
        while plane_start < nplanes {
            let plane_end = (plane_start + active_planes).min(nplanes);
            slabs.push(Self {
                slab_id: slabs.len(),
                plane_start,
                plane_end,
            });
            plane_start = plane_end;
        }
        slabs
    }
}

pub(crate) fn basic_plane_descriptors(
    nplanes: usize,
    spw_id: Option<i32>,
    channel_start: usize,
    field_ids: &[i32],
    interpolation: SpectralInterpolationPolicy,
) -> Vec<SpectralPlaneDescriptor> {
    (0..nplanes)
        .map(|output_index| SpectralPlaneDescriptor {
            output_index,
            spw_id,
            source_channel_start: channel_start + output_index,
            source_channel_count: match interpolation {
                SpectralInterpolationPolicy::Nearest => 1,
                SpectralInterpolationPolicy::Linear => 2,
            },
            output_frequency_hz: None,
            stokes: "I",
            field_ids: field_ids.to_vec(),
            interpolation,
            coordinate_label: format!("plane-{output_index}"),
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum PlaneComponent {
    Model,
    Image,
    Residual,
    Psf,
    Sumwt,
    Weight,
    PrimaryBeam,
    Pbcor,
    Mask,
    DeconvolverState,
    Diagnostics,
    BeamMetadata,
    ProductMetadata,
}

impl PlaneComponent {
    fn as_str(self) -> &'static str {
        match self {
            Self::Model => "model",
            Self::Image => "image",
            Self::Residual => "residual",
            Self::Psf => "psf",
            Self::Sumwt => "sumwt",
            Self::Weight => "weight",
            Self::PrimaryBeam => "pb",
            Self::Pbcor => "pbcor",
            Self::Mask => "mask",
            Self::DeconvolverState => "deconvolver_state",
            Self::Diagnostics => "diagnostics",
            Self::BeamMetadata => "beam_metadata",
            Self::ProductMetadata => "product_metadata",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum PlaneStateResidency {
    #[default]
    FullActiveGroup,
    StreamingPlaneResults,
}

impl PlaneStateResidency {
    fn as_str(self) -> &'static str {
        match self {
            Self::FullActiveGroup => "full_active_group",
            Self::StreamingPlaneResults => "streaming_plane_results",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum SourceBufferResidency {
    #[default]
    RowBlockStream,
    FullSlabRawSource,
}

impl SourceBufferResidency {
    fn as_str(self) -> &'static str {
        match self {
            Self::RowBlockStream => "row_block_stream",
            Self::FullSlabRawSource => "full_slab_raw_source",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PlaneStateRequirements {
    required: BTreeSet<PlaneComponent>,
    mutable: BTreeSet<PlaneComponent>,
    residency: PlaneStateResidency,
}

impl PlaneStateRequirements {
    pub(crate) fn dirty_standard() -> Self {
        Self::new([
            PlaneComponent::Model,
            PlaneComponent::Image,
            PlaneComponent::Residual,
            PlaneComponent::Psf,
            PlaneComponent::Sumwt,
            PlaneComponent::BeamMetadata,
            PlaneComponent::ProductMetadata,
            PlaneComponent::Diagnostics,
        ])
    }

    pub(crate) fn bounded_clean() -> Self {
        Self::dirty_standard().with_mutable([
            PlaneComponent::Model,
            PlaneComponent::Residual,
            PlaneComponent::DeconvolverState,
            PlaneComponent::Diagnostics,
        ])
    }

    pub(crate) fn multiscale_clean() -> Self {
        Self::bounded_clean().with_components([PlaneComponent::Mask])
    }

    pub(crate) fn mosaic_pb_aware() -> Self {
        Self::bounded_clean().with_components([
            PlaneComponent::Weight,
            PlaneComponent::PrimaryBeam,
            PlaneComponent::Pbcor,
        ])
    }

    fn new<const N: usize>(components: [PlaneComponent; N]) -> Self {
        Self {
            required: components.into_iter().collect(),
            mutable: BTreeSet::new(),
            residency: PlaneStateResidency::FullActiveGroup,
        }
    }

    fn with_components<const N: usize>(mut self, components: [PlaneComponent; N]) -> Self {
        self.required.extend(components);
        self
    }

    fn with_mutable<const N: usize>(mut self, components: [PlaneComponent; N]) -> Self {
        self.required.extend(components);
        self.mutable.extend(components);
        self
    }

    pub(crate) fn with_streaming_plane_results(mut self) -> Self {
        self.residency = PlaneStateResidency::StreamingPlaneResults;
        self
    }

    pub(crate) fn estimated_bytes_per_plane(&self, image_pixels: usize) -> usize {
        self.required
            .iter()
            .map(|component| self.estimated_component_bytes(*component, image_pixels))
            .sum()
    }

    pub(crate) fn estimated_resident_bytes_for_active_planes(
        &self,
        image_pixels: usize,
        active_planes: usize,
    ) -> usize {
        match self.residency {
            PlaneStateResidency::FullActiveGroup => self
                .estimated_bytes_per_plane(image_pixels)
                .saturating_mul(active_planes),
            PlaneStateResidency::StreamingPlaneResults => 0,
        }
    }

    pub(crate) fn residency_name(&self) -> &'static str {
        self.residency.as_str()
    }

    fn estimated_component_bytes(&self, component: PlaneComponent, image_pixels: usize) -> usize {
        match component {
            PlaneComponent::Model
            | PlaneComponent::Image
            | PlaneComponent::Residual
            | PlaneComponent::Psf
            | PlaneComponent::Weight
            | PlaneComponent::PrimaryBeam
            | PlaneComponent::Pbcor
            | PlaneComponent::Mask => image_pixels.saturating_mul(std::mem::size_of::<f32>()),
            PlaneComponent::Sumwt => std::mem::size_of::<f64>(),
            PlaneComponent::DeconvolverState => image_pixels.saturating_mul(2),
            PlaneComponent::Diagnostics
            | PlaneComponent::BeamMetadata
            | PlaneComponent::ProductMetadata => 1024,
        }
    }

    pub(crate) fn component_memory_breakdown(&self, image_pixels: usize) -> String {
        self.required
            .iter()
            .map(|component| {
                format!(
                    "{}:{}",
                    component.as_str(),
                    self.estimated_component_bytes(*component, image_pixels)
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    pub(crate) fn component_list(&self) -> String {
        self.required
            .iter()
            .map(|component| component.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PlaneStateDirtyMask {
    dirty: BTreeSet<PlaneComponent>,
}

impl PlaneStateDirtyMask {
    pub(crate) fn mark(&mut self, component: PlaneComponent) {
        self.dirty.insert(component);
    }

    #[allow(dead_code)]
    pub(crate) fn contains(&self, component: PlaneComponent) -> bool {
        self.dirty.contains(&component)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpectralPlaneState {
    pub(crate) descriptor: SpectralPlaneDescriptorKey,
    pub(crate) requirements: PlaneStateRequirements,
    pub(crate) dirty_mask: PlaneStateDirtyMask,
    pub(crate) coordinate_metadata: String,
    pub(crate) beam_metadata: String,
    pub(crate) mask_metadata: String,
    pub(crate) primary_beam_metadata: String,
    pub(crate) pbcor_metadata: String,
    pub(crate) sumwt_present: bool,
    pub(crate) product_write_state: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SpectralPlaneDescriptorKey {
    pub(crate) output_index: usize,
}

pub(crate) trait PlaneStateStore {
    fn load(&mut self, key: SpectralPlaneDescriptorKey) -> Option<SpectralPlaneState>;
    fn store(&mut self, state: SpectralPlaneState);
    fn cleanup(&mut self, key: SpectralPlaneDescriptorKey) -> Option<SpectralPlaneState>;
    fn trace(&self) -> &[PlaneStateStoreTrace];
    fn io_stats(&self) -> PlaneStateStoreIoStats;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlaneStateStoreTrace {
    pub(crate) op: &'static str,
    pub(crate) output_index: usize,
    pub(crate) components: String,
    pub(crate) dirty_components: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PlaneStateStoreIoStats {
    pub(crate) load_count: usize,
    pub(crate) store_count: usize,
    pub(crate) cleanup_count: usize,
    pub(crate) bytes_read: usize,
    pub(crate) bytes_written: usize,
}

#[derive(Debug, Default)]
pub(crate) struct InMemoryPlaneStateStore {
    states: BTreeMap<SpectralPlaneDescriptorKey, SpectralPlaneState>,
    trace: Vec<PlaneStateStoreTrace>,
    io_stats: PlaneStateStoreIoStats,
}

impl PlaneStateStore for InMemoryPlaneStateStore {
    fn load(&mut self, key: SpectralPlaneDescriptorKey) -> Option<SpectralPlaneState> {
        let state = self.states.get(&key).cloned();
        self.io_stats.load_count = self.io_stats.load_count.saturating_add(1);
        self.io_stats.bytes_read = self.io_stats.bytes_read.saturating_add(
            state
                .as_ref()
                .map(estimated_state_record_bytes)
                .unwrap_or(0),
        );
        self.trace.push(PlaneStateStoreTrace {
            op: "load",
            output_index: key.output_index,
            components: state
                .as_ref()
                .map(|state| state.requirements.component_list())
                .unwrap_or_default(),
            dirty_components: state.as_ref().map(dirty_component_list).unwrap_or_default(),
        });
        state
    }

    fn store(&mut self, state: SpectralPlaneState) {
        self.io_stats.store_count = self.io_stats.store_count.saturating_add(1);
        self.io_stats.bytes_written = self
            .io_stats
            .bytes_written
            .saturating_add(estimated_state_record_bytes(&state));
        self.trace.push(PlaneStateStoreTrace {
            op: "store",
            output_index: state.descriptor.output_index,
            components: state.requirements.component_list(),
            dirty_components: dirty_component_list(&state),
        });
        self.states.insert(state.descriptor, state);
    }

    fn cleanup(&mut self, key: SpectralPlaneDescriptorKey) -> Option<SpectralPlaneState> {
        let state = self.states.remove(&key);
        self.io_stats.cleanup_count = self.io_stats.cleanup_count.saturating_add(1);
        self.trace.push(PlaneStateStoreTrace {
            op: "cleanup",
            output_index: key.output_index,
            components: state
                .as_ref()
                .map(|state| state.requirements.component_list())
                .unwrap_or_default(),
            dirty_components: state.as_ref().map(dirty_component_list).unwrap_or_default(),
        });
        state
    }

    fn trace(&self) -> &[PlaneStateStoreTrace] {
        &self.trace
    }

    fn io_stats(&self) -> PlaneStateStoreIoStats {
        self.io_stats
    }
}

#[derive(Debug)]
pub(crate) struct ProductBackedPlaneStateStore {
    image_pixels: usize,
    write_through_components: BTreeSet<PlaneComponent>,
    states: BTreeMap<SpectralPlaneDescriptorKey, SpectralPlaneState>,
    trace: Vec<PlaneStateStoreTrace>,
    io_stats: PlaneStateStoreIoStats,
}

impl ProductBackedPlaneStateStore {
    pub(crate) fn new(
        image_pixels: usize,
        write_through_components: impl IntoIterator<Item = PlaneComponent>,
    ) -> Self {
        Self {
            image_pixels,
            write_through_components: write_through_components.into_iter().collect(),
            states: BTreeMap::new(),
            trace: Vec::new(),
            io_stats: PlaneStateStoreIoStats::default(),
        }
    }

    fn product_bytes_for_state(&self, state: &SpectralPlaneState) -> usize {
        self.write_through_components
            .iter()
            .filter(|component| state.requirements.required.contains(component))
            .map(|component| {
                state
                    .requirements
                    .estimated_component_bytes(*component, self.image_pixels)
            })
            .sum()
    }
}

impl PlaneStateStore for ProductBackedPlaneStateStore {
    fn load(&mut self, key: SpectralPlaneDescriptorKey) -> Option<SpectralPlaneState> {
        let state = self.states.get(&key).cloned();
        self.io_stats.load_count = self.io_stats.load_count.saturating_add(1);
        self.io_stats.bytes_read = self.io_stats.bytes_read.saturating_add(
            state
                .as_ref()
                .map(|state| {
                    estimated_state_record_bytes(state)
                        .saturating_add(self.product_bytes_for_state(state))
                })
                .unwrap_or(0),
        );
        self.trace.push(PlaneStateStoreTrace {
            op: "product_load",
            output_index: key.output_index,
            components: state
                .as_ref()
                .map(|state| state.requirements.component_list())
                .unwrap_or_default(),
            dirty_components: state.as_ref().map(dirty_component_list).unwrap_or_default(),
        });
        state
    }

    fn store(&mut self, state: SpectralPlaneState) {
        self.io_stats.store_count = self.io_stats.store_count.saturating_add(1);
        self.io_stats.bytes_written = self.io_stats.bytes_written.saturating_add(
            estimated_state_record_bytes(&state)
                .saturating_add(self.product_bytes_for_state(&state)),
        );
        self.trace.push(PlaneStateStoreTrace {
            op: "product_store",
            output_index: state.descriptor.output_index,
            components: state.requirements.component_list(),
            dirty_components: dirty_component_list(&state),
        });
        self.states.insert(state.descriptor, state);
    }

    fn cleanup(&mut self, key: SpectralPlaneDescriptorKey) -> Option<SpectralPlaneState> {
        let state = self.states.remove(&key);
        self.io_stats.cleanup_count = self.io_stats.cleanup_count.saturating_add(1);
        self.trace.push(PlaneStateStoreTrace {
            op: "product_cleanup",
            output_index: key.output_index,
            components: state
                .as_ref()
                .map(|state| state.requirements.component_list())
                .unwrap_or_default(),
            dirty_components: state.as_ref().map(dirty_component_list).unwrap_or_default(),
        });
        state
    }

    fn trace(&self) -> &[PlaneStateStoreTrace] {
        &self.trace
    }

    fn io_stats(&self) -> PlaneStateStoreIoStats {
        self.io_stats
    }
}

fn estimated_state_record_bytes(state: &SpectralPlaneState) -> usize {
    std::mem::size_of::<SpectralPlaneDescriptorKey>()
        .saturating_add(state.requirements.component_list().len())
        .saturating_add(dirty_component_list(state).len())
        .saturating_add(state.coordinate_metadata.len())
        .saturating_add(state.beam_metadata.len())
        .saturating_add(state.mask_metadata.len())
        .saturating_add(state.primary_beam_metadata.len())
        .saturating_add(state.pbcor_metadata.len())
        .saturating_add(std::mem::size_of::<bool>())
        .saturating_add(state.product_write_state.len())
}

fn dirty_component_list(state: &SpectralPlaneState) -> String {
    state
        .dirty_mask
        .dirty
        .iter()
        .map(|component| component.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VisibilityRequirement {
    None,
    ReadOnly,
    ModelDependent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum CachePolicy {
    Disabled,
    GeometryOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VisibilityCachePolicy {
    Disabled,
    FullSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImagingScheduleKind {
    SourceFirst,
    SlabFirst,
    Hybrid,
}

const ALL_IMAGING_SCHEDULES: [ImagingScheduleKind; 3] = [
    ImagingScheduleKind::SourceFirst,
    ImagingScheduleKind::Hybrid,
    ImagingScheduleKind::SlabFirst,
];

impl ImagingScheduleKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::SourceFirst => "source_first",
            Self::SlabFirst => "slab_first",
            Self::Hybrid => "hybrid",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SpectralExecutorCapabilities {
    pub(crate) source_first_output_spill: bool,
    pub(crate) slab_first: bool,
    pub(crate) hybrid_full_source_cache: bool,
}

impl SpectralExecutorCapabilities {
    pub(crate) fn all() -> Self {
        Self {
            source_first_output_spill: true,
            slab_first: true,
            hybrid_full_source_cache: true,
        }
    }

    pub(crate) fn slab_runner_without_output_spill_or_full_source_cache() -> Self {
        Self {
            source_first_output_spill: false,
            slab_first: true,
            hybrid_full_source_cache: false,
        }
    }

    fn as_str(self) -> &'static str {
        match (
            self.source_first_output_spill,
            self.slab_first,
            self.hybrid_full_source_cache,
        ) {
            (true, true, true) => "all",
            (false, true, true) => "full_slab_no_output_spill",
            (false, true, false) => "slab_first_only",
            (false, false, false) => "single_resident_source_first_only",
            _ => "custom",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VisibilitySlabShape {
    pub(crate) active_planes: usize,
    pub(crate) slab_count: usize,
    pub(crate) source_channel_visits: usize,
    pub(crate) max_slab_source_channels: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PreparedVisibilityResidency {
    pub(crate) sample_lanes_per_source_channel: usize,
    pub(crate) bucket_sample_bytes: usize,
    pub(crate) max_live_row_blocks: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VisibilityResidentLayout {
    pub(crate) uvw_bytes: usize,
    pub(crate) weight_bytes: usize,
    pub(crate) field_id_bytes: usize,
    pub(crate) spw_id_bytes: usize,
    pub(crate) polarization_id_bytes: usize,
    pub(crate) is_cross_bytes: usize,
    pub(crate) channel_origin_bytes: usize,
    pub(crate) spectral_route_bytes: usize,
    pub(crate) ms_row_index_bytes: usize,
    pub(crate) unresolved_time_bytes: usize,
    pub(crate) antenna_id_bytes: usize,
    pub(crate) pointing_sidecar_bytes: usize,
}

impl VisibilityResidentLayout {
    pub(crate) fn standard_spectral_cube_columnar(
        corr_count: usize,
        weight_element_bytes: usize,
    ) -> Self {
        Self {
            uvw_bytes: 3usize.saturating_mul(std::mem::size_of::<f64>()),
            weight_bytes: corr_count.saturating_mul(weight_element_bytes),
            field_id_bytes: std::mem::size_of::<usize>(),
            spw_id_bytes: std::mem::size_of::<usize>(),
            polarization_id_bytes: std::mem::size_of::<usize>(),
            is_cross_bytes: std::mem::size_of::<bool>(),
            channel_origin_bytes: std::mem::size_of::<usize>(),
            spectral_route_bytes: std::mem::size_of::<u64>(),
            ms_row_index_bytes: 0,
            unresolved_time_bytes: 0,
            antenna_id_bytes: 0,
            pointing_sidecar_bytes: 0,
        }
    }

    pub(crate) fn with_ms_row_index(mut self) -> Self {
        self.ms_row_index_bytes = std::mem::size_of::<usize>();
        self
    }

    pub(crate) fn with_unresolved_time(mut self, time_element_bytes: usize) -> Self {
        self.unresolved_time_bytes = time_element_bytes;
        self
    }

    pub(crate) fn with_antenna_ids(mut self, antenna_element_bytes: usize) -> Self {
        self.antenna_id_bytes = 2usize.saturating_mul(antenna_element_bytes);
        self
    }

    pub(crate) fn with_pointing_sidecar(mut self) -> Self {
        self.pointing_sidecar_bytes = 4usize.saturating_mul(std::mem::size_of::<f64>());
        self
    }

    fn bytes_per_row(&self) -> usize {
        self.uvw_bytes
            .saturating_add(self.weight_bytes)
            .saturating_add(self.field_id_bytes)
            .saturating_add(self.spw_id_bytes)
            .saturating_add(self.polarization_id_bytes)
            .saturating_add(self.is_cross_bytes)
            .saturating_add(self.channel_origin_bytes)
            .saturating_add(self.spectral_route_bytes)
            .saturating_add(self.ms_row_index_bytes)
            .saturating_add(self.unresolved_time_bytes)
            .saturating_add(self.antenna_id_bytes)
            .saturating_add(self.pointing_sidecar_bytes)
    }

    fn component_breakdown(&self) -> String {
        [
            ("uvw", self.uvw_bytes),
            ("weight", self.weight_bytes),
            ("field_id", self.field_id_bytes),
            ("spw_id", self.spw_id_bytes),
            ("polarization_id", self.polarization_id_bytes),
            ("is_cross", self.is_cross_bytes),
            ("channel_origin", self.channel_origin_bytes),
            ("spectral_route", self.spectral_route_bytes),
            ("ms_row_index", self.ms_row_index_bytes),
            ("unresolved_time", self.unresolved_time_bytes),
            ("antenna_ids", self.antenna_id_bytes),
            ("pointing_sidecar", self.pointing_sidecar_bytes),
        ]
        .into_iter()
        .filter(|(_, bytes)| *bytes > 0)
        .map(|(name, bytes)| format!("{name}:{bytes}"))
        .collect::<Vec<_>>()
        .join(",")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VisibilitySourceShape {
    pub(crate) active_rows: usize,
    pub(crate) full_source_channel_count: usize,
    pub(crate) source_cell_channel_count: usize,
    pub(crate) corr_count: usize,
    pub(crate) data_element_bytes: usize,
    pub(crate) flag_element_bytes: usize,
    pub(crate) weight_element_bytes: usize,
    pub(crate) weight_spectrum_element_bytes: Option<usize>,
    pub(crate) data_channel_read_granularity: VisibilityChannelReadGranularity,
    pub(crate) flag_channel_read_granularity: VisibilityChannelReadGranularity,
    pub(crate) weight_spectrum_channel_read_granularity: Option<VisibilityChannelReadGranularity>,
    pub(crate) uvw_element_bytes: usize,
    pub(crate) antenna_element_bytes: usize,
    pub(crate) time_element_bytes: Option<usize>,
    pub(crate) pointing_id_element_bytes: Option<usize>,
    pub(crate) resident_layout: VisibilityResidentLayout,
    pub(crate) prepared_sample_bytes: usize,
    pub(crate) full_source_cacheable: bool,
    pub(crate) slab_shapes: Vec<VisibilitySlabShape>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VisibilityChannelReadGranularity {
    RequestedRange,
    FullCell,
}

impl VisibilityChannelReadGranularity {
    fn as_str(self) -> &'static str {
        match self {
            Self::RequestedRange => "requested_range",
            Self::FullCell => "full_cell",
        }
    }
}

impl VisibilitySourceShape {
    fn row_channel_bytes(&self) -> usize {
        self.corr_count
            .saturating_mul(self.data_element_bytes)
            .saturating_add(self.corr_count.saturating_mul(self.flag_element_bytes))
            .saturating_add(
                self.weight_spectrum_element_bytes
                    .map(|bytes| self.corr_count.saturating_mul(bytes))
                    .unwrap_or(0),
            )
    }

    fn row_fixed_physical_read_bytes(&self) -> usize {
        self.corr_count
            .saturating_mul(self.weight_element_bytes)
            .saturating_add(3usize.saturating_mul(self.uvw_element_bytes))
            .saturating_add(2usize.saturating_mul(self.antenna_element_bytes))
            .saturating_add(self.time_element_bytes.unwrap_or(0))
            .saturating_add(self.pointing_id_element_bytes.unwrap_or(0))
    }

    fn row_fixed_resident_bytes(&self) -> usize {
        self.resident_layout.bytes_per_row()
    }

    fn row_cache_overhead_bytes(&self) -> usize {
        self.row_fixed_resident_bytes()
            .saturating_sub(self.row_fixed_physical_read_bytes())
    }

    fn resident_layout_breakdown(&self) -> String {
        self.resident_layout.component_breakdown()
    }

    fn physical_channel_visits(
        &self,
        logical_channel_visits: usize,
        fixed_row_reads: usize,
        granularity: VisibilityChannelReadGranularity,
    ) -> usize {
        match granularity {
            VisibilityChannelReadGranularity::RequestedRange => logical_channel_visits,
            VisibilityChannelReadGranularity::FullCell => fixed_row_reads.saturating_mul(
                self.source_cell_channel_count
                    .max(self.full_source_channel_count),
            ),
        }
    }

    fn source_read_channel_bytes(
        &self,
        logical_channel_visits: usize,
        fixed_row_reads: usize,
    ) -> usize {
        let data_channel_visits = self.physical_channel_visits(
            logical_channel_visits,
            fixed_row_reads,
            self.data_channel_read_granularity,
        );
        let flag_channel_visits = self.physical_channel_visits(
            logical_channel_visits,
            fixed_row_reads,
            self.flag_channel_read_granularity,
        );
        let weight_spectrum_channel_bytes = self
            .weight_spectrum_element_bytes
            .zip(self.weight_spectrum_channel_read_granularity)
            .map(|(element_bytes, granularity)| {
                self.physical_channel_visits(logical_channel_visits, fixed_row_reads, granularity)
                    .saturating_mul(self.corr_count)
                    .saturating_mul(element_bytes)
            })
            .unwrap_or(0);
        data_channel_visits
            .saturating_mul(self.corr_count)
            .saturating_mul(self.data_element_bytes)
            .saturating_add(
                flag_channel_visits
                    .saturating_mul(self.corr_count)
                    .saturating_mul(self.flag_element_bytes),
            )
            .saturating_add(weight_spectrum_channel_bytes)
    }

    fn source_read_bytes(&self, logical_channel_visits: usize, fixed_row_reads: usize) -> usize {
        self.active_rows.saturating_mul(
            self.source_read_channel_bytes(logical_channel_visits, fixed_row_reads)
                .saturating_add(
                    fixed_row_reads.saturating_mul(self.row_fixed_physical_read_bytes()),
                ),
        )
    }

    fn full_source_cache_bytes(&self) -> usize {
        self.active_rows.saturating_mul(
            self.row_fixed_resident_bytes().saturating_add(
                self.full_source_channel_count
                    .saturating_mul(self.row_channel_bytes()),
            ),
        )
    }

    pub(crate) fn source_buffer_bytes_for_rows(
        &self,
        residency: PreparedVisibilityResidency,
        rows: usize,
        source_channels: usize,
    ) -> usize {
        self.raw_source_buffer_bytes_for_rows(rows, source_channels)
            .saturating_add(self.live_prepared_bytes_for_rows(residency, rows, source_channels))
            .saturating_add(self.live_bucket_bytes_for_rows(residency, rows, source_channels))
    }

    pub(crate) fn resident_source_buffer_bytes(
        &self,
        source_residency: SourceBufferResidency,
        prepared_residency: PreparedVisibilityResidency,
        row_block_rows: usize,
        source_channels: usize,
    ) -> usize {
        match source_residency {
            SourceBufferResidency::RowBlockStream => self.source_buffer_bytes_for_rows(
                prepared_residency,
                row_block_rows,
                source_channels,
            ),
            SourceBufferResidency::FullSlabRawSource => self
                .raw_source_buffer_bytes_for_rows(self.active_rows, source_channels)
                .saturating_add(self.live_source_scratch_bytes_for_rows(
                    prepared_residency,
                    row_block_rows,
                    source_channels,
                )),
        }
    }

    pub(crate) fn live_source_scratch_bytes_for_rows(
        &self,
        residency: PreparedVisibilityResidency,
        rows: usize,
        source_channels: usize,
    ) -> usize {
        self.live_prepared_bytes_for_rows(residency, rows, source_channels)
            .saturating_add(self.live_bucket_bytes_for_rows(residency, rows, source_channels))
    }

    pub(crate) fn raw_source_buffer_bytes_for_rows(
        &self,
        rows: usize,
        source_channels: usize,
    ) -> usize {
        rows.saturating_mul(
            self.row_fixed_resident_bytes()
                .saturating_add(source_channels.saturating_mul(self.row_channel_bytes())),
        )
    }

    pub(crate) fn prepared_staging_bytes_for_source_channels(
        &self,
        _residency: PreparedVisibilityResidency,
        _source_channels: usize,
    ) -> usize {
        0
    }

    fn live_prepared_bytes_for_rows(
        &self,
        residency: PreparedVisibilityResidency,
        rows: usize,
        source_channels: usize,
    ) -> usize {
        rows.saturating_mul(source_channels)
            .saturating_mul(residency.sample_lanes_per_source_channel)
            .saturating_mul(self.prepared_sample_bytes)
            .saturating_mul(residency.max_live_row_blocks.max(1))
    }

    fn live_bucket_bytes_for_rows(
        &self,
        residency: PreparedVisibilityResidency,
        rows: usize,
        source_channels: usize,
    ) -> usize {
        rows.saturating_mul(source_channels)
            .saturating_mul(residency.bucket_sample_bytes)
            .saturating_mul(residency.max_live_row_blocks.max(1))
    }

    fn no_cache_source_read_bytes(&self, candidate: VisibilitySlabShape) -> usize {
        self.source_read_bytes(candidate.source_channel_visits, candidate.slab_count)
    }

    fn cache_read_bytes(&self, candidate: VisibilitySlabShape) -> usize {
        self.active_rows.saturating_mul(
            candidate
                .source_channel_visits
                .saturating_mul(self.row_channel_bytes())
                .saturating_add(
                    candidate
                        .slab_count
                        .saturating_mul(self.row_fixed_resident_bytes()),
                ),
        )
    }

    fn full_source_read_bytes(&self) -> usize {
        self.source_read_bytes(self.full_source_channel_count, 1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum DeterministicOrderingPolicy {
    PlaneMajor,
    SlabMajor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PassRequirements {
    pub(crate) pass_kind: ImagingPassKind,
    pub(crate) required_components: BTreeSet<PlaneComponent>,
    pub(crate) mutable_components: BTreeSet<PlaneComponent>,
    pub(crate) visibility: VisibilityRequirement,
    pub(crate) scalar_reductions: BTreeSet<ScalarReductionKind>,
    pub(crate) cache_policy: CachePolicy,
    pub(crate) ordering: DeterministicOrderingPolicy,
}

impl PassRequirements {
    pub(crate) fn for_pass(pass_kind: ImagingPassKind) -> Self {
        let mut required_components = BTreeSet::new();
        let mut mutable_components = BTreeSet::new();
        let mut scalar_reductions = BTreeSet::new();
        let visibility;
        let cache_policy = CachePolicy::GeometryOnly;
        match pass_kind {
            ImagingPassKind::WeightingDensity => {
                visibility = VisibilityRequirement::ReadOnly;
                mutable_components.insert(PlaneComponent::Weight);
            }
            ImagingPassKind::Psf | ImagingPassKind::InitialDirty => {
                visibility = VisibilityRequirement::ReadOnly;
                mutable_components.extend([PlaneComponent::Psf, PlaneComponent::Residual]);
                scalar_reductions.insert(ScalarReductionKind::GlobalPeak);
            }
            ImagingPassKind::MinorCycleDiagnostics => {
                visibility = VisibilityRequirement::None;
                required_components.extend([PlaneComponent::Residual, PlaneComponent::Mask]);
                scalar_reductions.extend([
                    ScalarReductionKind::GlobalPeak,
                    ScalarReductionKind::NsigmaThreshold,
                    ScalarReductionKind::CycleThreshold,
                    ScalarReductionKind::MaskStats,
                    ScalarReductionKind::StopReason,
                    ScalarReductionKind::MajorCycleTransition,
                ]);
            }
            ImagingPassKind::MinorCycleUpdate => {
                visibility = VisibilityRequirement::None;
                mutable_components.extend([PlaneComponent::Model, PlaneComponent::Residual]);
                scalar_reductions.insert(ScalarReductionKind::NiterAccounting);
            }
            ImagingPassKind::ResidualRefresh => {
                visibility = VisibilityRequirement::ModelDependent;
                required_components.insert(PlaneComponent::Model);
                mutable_components.insert(PlaneComponent::Residual);
            }
            ImagingPassKind::ProductWrite => {
                visibility = VisibilityRequirement::None;
                required_components.extend([
                    PlaneComponent::Model,
                    PlaneComponent::Residual,
                    PlaneComponent::Psf,
                    PlaneComponent::Sumwt,
                    PlaneComponent::BeamMetadata,
                    PlaneComponent::ProductMetadata,
                ]);
            }
        }
        required_components.extend(mutable_components.iter().copied());
        Self {
            pass_kind,
            required_components,
            mutable_components,
            visibility,
            scalar_reductions,
            cache_policy,
            ordering: DeterministicOrderingPolicy::SlabMajor,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[allow(dead_code)]
pub(crate) enum ScalarReductionKind {
    GlobalPeak,
    NsigmaThreshold,
    CycleThreshold,
    NiterAccounting,
    StopReason,
    MaskStats,
    MajorCycleTransition,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct ScalarReductionState {
    pub(crate) global_peak_jy_per_beam: f32,
    pub(crate) nsigma_threshold_jy_per_beam: f32,
    pub(crate) cycle_threshold_jy_per_beam: f32,
    pub(crate) niter_done: usize,
    pub(crate) mask_pixels: usize,
    pub(crate) stop: bool,
}

impl ScalarReductionState {
    pub(crate) fn merge(self, other: Self) -> Self {
        Self {
            global_peak_jy_per_beam: self
                .global_peak_jy_per_beam
                .max(other.global_peak_jy_per_beam),
            nsigma_threshold_jy_per_beam: self
                .nsigma_threshold_jy_per_beam
                .max(other.nsigma_threshold_jy_per_beam),
            cycle_threshold_jy_per_beam: self
                .cycle_threshold_jy_per_beam
                .max(other.cycle_threshold_jy_per_beam),
            niter_done: self.niter_done.saturating_add(other.niter_done),
            mask_pixels: self.mask_pixels.saturating_add(other.mask_pixels),
            stop: self.stop || other.stop,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProcessMemorySnapshot {
    pub(crate) current_rss_bytes: Option<usize>,
    pub(crate) peak_rss_bytes: Option<usize>,
}

pub(crate) fn current_process_memory_snapshot() -> ProcessMemorySnapshot {
    ProcessMemorySnapshot {
        current_rss_bytes: current_rss_bytes(),
        peak_rss_bytes: peak_rss_bytes(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpectralMemoryObservation {
    pub(crate) mode: &'static str,
    pub(crate) stage: &'static str,
    pub(crate) slab_id: Option<usize>,
    pub(crate) plane_start: usize,
    pub(crate) plane_end: usize,
    pub(crate) current_rss_bytes: Option<usize>,
    pub(crate) peak_rss_bytes: Option<usize>,
    pub(crate) delta_from_baseline_bytes: Option<isize>,
    pub(crate) delta_from_previous_bytes: Option<isize>,
    pub(crate) estimated_resident_bytes: Option<usize>,
    pub(crate) planned_active_bytes: usize,
    pub(crate) visibility_staging_bytes: usize,
    pub(crate) plane_state_bytes: usize,
    pub(crate) product_scratch_bytes: usize,
    pub(crate) cache_budget_bytes: usize,
    pub(crate) note: &'static str,
}

impl SpectralMemoryObservation {
    pub(crate) fn log_line(&self) -> String {
        format!(
            "spectral_slab_memory mode={} stage={} slab_id={} plane_start={} plane_end={} current_rss_bytes={} peak_rss_bytes={} delta_from_baseline_bytes={} delta_from_previous_bytes={} estimated_resident_bytes={} planned_active_bytes={} visibility_staging_bytes={} plane_state_bytes={} product_scratch_bytes={} cache_budget_bytes={} note={}",
            self.mode,
            self.stage,
            option_usize(self.slab_id),
            self.plane_start,
            self.plane_end,
            option_usize(self.current_rss_bytes),
            option_usize(self.peak_rss_bytes),
            option_isize(self.delta_from_baseline_bytes),
            option_isize(self.delta_from_previous_bytes),
            option_usize(self.estimated_resident_bytes),
            self.planned_active_bytes,
            self.visibility_staging_bytes,
            self.plane_state_bytes,
            self.product_scratch_bytes,
            self.cache_budget_bytes,
            self.note,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImagingOutputShape {
    pub(crate) plane_count: usize,
    pub(crate) image_shape: [usize; 2],
}

impl ImagingOutputShape {
    fn image_pixels(&self) -> usize {
        self.image_shape[0].saturating_mul(self.image_shape[1])
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpectralMemoryPlan {
    pub(crate) best_modeled_schedule_kind: ImagingScheduleKind,
    pub(crate) schedule_kind: ImagingScheduleKind,
    pub(crate) executor_capabilities: SpectralExecutorCapabilities,
    pub(crate) nplanes: usize,
    pub(crate) image_shape: [usize; 2],
    pub(crate) active_planes: usize,
    pub(crate) slab_count: usize,
    pub(crate) slab_manifest: Vec<SpectralSlabManifest>,
    pub(crate) row_block_rows: usize,
    pub(crate) cache_budget_bytes: usize,
    pub(crate) cache_kind: CachePolicy,
    pub(crate) visibility_cache_policy: VisibilityCachePolicy,
    pub(crate) prepared_residency: PreparedVisibilityResidency,
    pub(crate) visibility_cache_bytes: usize,
    pub(crate) visibility_cache_source_channels: usize,
    pub(crate) worker_count: usize,
    pub(crate) backend: &'static str,
    pub(crate) memory_target_bytes: usize,
    pub(crate) fixed_frontend_bytes: usize,
    pub(crate) source_stream_buffer_bytes: usize,
    pub(crate) worker_staging_bytes: usize,
    pub(crate) per_plane_state_bytes: usize,
    pub(crate) component_memory_breakdown: String,
    pub(crate) visibility_staging_bytes_per_plane: usize,
    pub(crate) prepared_visibility_staging_bytes: usize,
    pub(crate) live_prepared_visibility_bytes: usize,
    pub(crate) live_bucket_bytes: usize,
    pub(crate) product_scratch_bytes: usize,
    pub(crate) product_batch_planes: usize,
    pub(crate) resident_plane_state_bytes: usize,
    pub(crate) plane_state_residency: PlaneStateResidency,
    pub(crate) source_buffer_residency: SourceBufferResidency,
    pub(crate) gpu_staging_bytes: usize,
    pub(crate) safety_margin_bytes: usize,
    pub(crate) planned_active_bytes: usize,
    pub(crate) source_channel_visits: usize,
    pub(crate) max_slab_source_channels: usize,
    pub(crate) full_source_channel_count: usize,
    pub(crate) source_cell_channel_count: usize,
    pub(crate) corr_count: usize,
    pub(crate) visibility_data_element_bytes: usize,
    pub(crate) data_channel_read_granularity: VisibilityChannelReadGranularity,
    pub(crate) flag_channel_read_granularity: VisibilityChannelReadGranularity,
    pub(crate) weight_spectrum_channel_read_granularity: Option<VisibilityChannelReadGranularity>,
    pub(crate) visibility_row_channel_bytes: usize,
    pub(crate) visibility_row_fixed_bytes: usize,
    pub(crate) visibility_row_fixed_resident_bytes: usize,
    pub(crate) visibility_row_cache_overhead_bytes: usize,
    pub(crate) visibility_resident_cache_layout: String,
    pub(crate) best_modeled_total_io_bytes: usize,
    pub(crate) best_modeled_source_read_bytes: usize,
    pub(crate) best_modeled_visibility_cache_io_bytes: usize,
    pub(crate) best_modeled_output_spill_io_bytes: usize,
    pub(crate) best_modeled_product_write_bytes: usize,
    pub(crate) best_modeled_active_planes: usize,
    pub(crate) best_modeled_slab_count: usize,
    pub(crate) best_modeled_source_channel_visits: usize,
    pub(crate) modeled_total_io_bytes: usize,
    pub(crate) modeled_source_read_bytes: usize,
    pub(crate) modeled_visibility_cache_fill_bytes: usize,
    pub(crate) modeled_visibility_cache_read_bytes: usize,
    pub(crate) modeled_visibility_cache_io_bytes: usize,
    pub(crate) modeled_output_spill_read_bytes: usize,
    pub(crate) modeled_output_spill_write_bytes: usize,
    pub(crate) modeled_output_spill_io_bytes: usize,
    pub(crate) modeled_product_write_bytes: usize,
    pub(crate) modeled_no_cache_source_read_bytes: usize,
    pub(crate) modeled_full_cache_source_read_bytes: usize,
    pub(crate) visibility_cache_saved_read_bytes: usize,
    pub(crate) candidate_io_costs: String,
    pub(crate) warnings: Vec<String>,
}

impl SpectralMemoryPlan {
    pub(crate) fn log_line(&self) -> String {
        format!(
            "spectral_slab_plan schedule={} best_modeled_schedule={} executor_capabilities={} nplanes={} image_shape={}x{} active_planes={} slab_count={} row_block_rows={} cache_budget_bytes={} cache_kind={} visibility_cache_policy={} prepared_residency={} visibility_cache_bytes={} visibility_cache_source_channels={} worker_count={} backend={} memory_target_bytes={} fixed_frontend_bytes={} source_stream_buffer_bytes={} worker_staging_bytes={} per_plane_state_bytes={} component_memory_bytes={} visibility_staging_bytes_per_plane={} prepared_visibility_staging_bytes={} live_prepared_visibility_bytes={} live_bucket_bytes={} product_scratch_bytes={} product_batch_planes={} resident_plane_state_bytes={} plane_state_residency={} source_buffer_residency={} gpu_staging_bytes={} safety_margin_bytes={} planned_active_bytes={} source_channel_visits={} max_slab_source_channels={} full_source_channel_count={} source_cell_channel_count={} corr_count={} visibility_data_element_bytes={} data_channel_read_granularity={} flag_channel_read_granularity={} weight_spectrum_channel_read_granularity={} visibility_row_channel_bytes={} visibility_row_fixed_bytes={} visibility_row_fixed_resident_bytes={} visibility_row_cache_overhead_bytes={} visibility_resident_cache_layout={} best_modeled_total_io_bytes={} best_modeled_source_read_bytes={} best_modeled_visibility_cache_io_bytes={} best_modeled_output_spill_io_bytes={} best_modeled_product_write_bytes={} best_modeled_active_planes={} best_modeled_slab_count={} best_modeled_source_channel_visits={} modeled_total_io_bytes={} modeled_source_read_bytes={} modeled_visibility_cache_fill_bytes={} modeled_visibility_cache_read_bytes={} modeled_visibility_cache_io_bytes={} modeled_output_spill_read_bytes={} modeled_output_spill_write_bytes={} modeled_output_spill_io_bytes={} modeled_product_write_bytes={} modeled_no_cache_source_read_bytes={} modeled_full_cache_source_read_bytes={} visibility_cache_saved_read_bytes={} candidate_io_costs={} warnings={}",
            self.schedule_kind.as_str(),
            self.best_modeled_schedule_kind.as_str(),
            self.executor_capabilities.as_str(),
            self.nplanes,
            self.image_shape[0],
            self.image_shape[1],
            self.active_planes,
            self.slab_count,
            self.row_block_rows,
            self.cache_budget_bytes,
            cache_policy_name(self.cache_kind),
            visibility_cache_policy_name(self.visibility_cache_policy),
            prepared_residency_name(self.prepared_residency),
            self.visibility_cache_bytes,
            self.visibility_cache_source_channels,
            self.worker_count,
            self.backend,
            self.memory_target_bytes,
            self.fixed_frontend_bytes,
            self.source_stream_buffer_bytes,
            self.worker_staging_bytes,
            self.per_plane_state_bytes,
            self.component_memory_breakdown,
            self.visibility_staging_bytes_per_plane,
            self.prepared_visibility_staging_bytes,
            self.live_prepared_visibility_bytes,
            self.live_bucket_bytes,
            self.product_scratch_bytes,
            self.product_batch_planes,
            self.resident_plane_state_bytes,
            self.plane_state_residency.as_str(),
            self.source_buffer_residency.as_str(),
            self.gpu_staging_bytes,
            self.safety_margin_bytes,
            self.planned_active_bytes,
            self.source_channel_visits,
            self.max_slab_source_channels,
            self.full_source_channel_count,
            self.source_cell_channel_count,
            self.corr_count,
            self.visibility_data_element_bytes,
            self.data_channel_read_granularity.as_str(),
            self.flag_channel_read_granularity.as_str(),
            self.weight_spectrum_channel_read_granularity
                .map(VisibilityChannelReadGranularity::as_str)
                .unwrap_or("absent"),
            self.visibility_row_channel_bytes,
            self.visibility_row_fixed_bytes,
            self.visibility_row_fixed_resident_bytes,
            self.visibility_row_cache_overhead_bytes,
            self.visibility_resident_cache_layout,
            self.best_modeled_total_io_bytes,
            self.best_modeled_source_read_bytes,
            self.best_modeled_visibility_cache_io_bytes,
            self.best_modeled_output_spill_io_bytes,
            self.best_modeled_product_write_bytes,
            self.best_modeled_active_planes,
            self.best_modeled_slab_count,
            self.best_modeled_source_channel_visits,
            self.modeled_total_io_bytes,
            self.modeled_source_read_bytes,
            self.modeled_visibility_cache_fill_bytes,
            self.modeled_visibility_cache_read_bytes,
            self.modeled_visibility_cache_io_bytes,
            self.modeled_output_spill_read_bytes,
            self.modeled_output_spill_write_bytes,
            self.modeled_output_spill_io_bytes,
            self.modeled_product_write_bytes,
            self.modeled_no_cache_source_read_bytes,
            self.modeled_full_cache_source_read_bytes,
            self.visibility_cache_saved_read_bytes,
            self.candidate_io_costs,
            if self.warnings.is_empty() {
                "none".to_string()
            } else {
                self.warnings.join("|")
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn with_active_planes_for_testing(
        mut self,
        active_planes: usize,
    ) -> Result<Self, String> {
        if active_planes == 0 || active_planes > self.nplanes {
            return Err(format!(
                "test spectral slab override active_planes={active_planes} outside 1..={}",
                self.nplanes
            ));
        }
        self.active_planes = active_planes;
        self.slab_count = self.nplanes.div_ceil(active_planes);
        self.slab_manifest = SpectralSlabManifest::for_planes(self.nplanes, active_planes);
        self.worker_count = self.worker_count.min(active_planes).max(1);
        if self.slab_count > 1 {
            self.schedule_kind = ImagingScheduleKind::SlabFirst;
        }
        self.warnings
            .push(format!("test_forced_active_planes={active_planes}"));
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpectralMemoryPlannerInput {
    pub(crate) output: ImagingOutputShape,
    pub(crate) visibility: VisibilitySourceShape,
    pub(crate) executor_capabilities: SpectralExecutorCapabilities,
    pub(crate) memory_target_bytes: usize,
    pub(crate) fixed_frontend_bytes: usize,
    pub(crate) worker_staging_bytes: usize,
    pub(crate) gpu_staging_bytes: usize,
    pub(crate) safety_margin_bytes: usize,
    pub(crate) executor_scratch: ExecutorScratchShape,
    pub(crate) source_buffer_residency: SourceBufferResidency,
    pub(crate) product_write_bytes_per_plane: usize,
    pub(crate) max_row_block_rows: usize,
    pub(crate) max_worker_count: usize,
    pub(crate) requirements: PlaneStateRequirements,
    pub(crate) prepared_residency: PreparedVisibilityResidency,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ExecutorScratchShape {
    pub(crate) fixed_bytes: usize,
    pub(crate) per_active_plane_bytes: usize,
    pub(crate) per_slab_source_channel_bytes: usize,
    pub(crate) per_worker_bytes: usize,
    pub(crate) per_worker_row_block_bytes: usize,
    pub(crate) per_worker_row_block_limit_bytes: usize,
    pub(crate) per_product_batch_plane_bytes: usize,
    pub(crate) per_product_pending_plane_bytes: usize,
}

impl ExecutorScratchShape {
    pub(crate) fn bytes_for_worker_count_and_rows(
        self,
        active_planes: usize,
        max_slab_source_channels: usize,
        worker_count: usize,
        row_block_rows: usize,
        product_batch_planes: usize,
    ) -> usize {
        let worker_count = worker_count.max(1);
        self.fixed_bytes
            .saturating_add(
                self.per_active_plane_bytes
                    .saturating_mul(active_planes.max(1)),
            )
            .saturating_add(
                self.per_slab_source_channel_bytes
                    .saturating_mul(max_slab_source_channels.max(1)),
            )
            .saturating_add(self.per_worker_bytes.saturating_mul(worker_count))
            .saturating_add(
                self.per_product_batch_plane_bytes
                    .saturating_mul(product_batch_planes.max(1)),
            )
            .saturating_add(if product_batch_planes > 1 {
                self.per_product_pending_plane_bytes
                    .saturating_mul(product_batch_planes)
            } else {
                0
            })
            .saturating_add(
                self.per_worker_row_block_bytes
                    .saturating_mul(row_block_rows)
                    .saturating_mul(worker_count),
            )
    }

    fn max_row_block_rows(self) -> Option<usize> {
        if self.per_worker_row_block_bytes == 0 || self.per_worker_row_block_limit_bytes == 0 {
            return None;
        }
        Some((self.per_worker_row_block_limit_bytes / self.per_worker_row_block_bytes).max(1))
    }

    fn max_product_batch_planes(
        self,
        _active_planes: usize,
        _worker_count: usize,
        _memory_target_bytes: usize,
    ) -> usize {
        1
    }
}

#[derive(Clone, Copy)]
struct CandidatePlan {
    schedule_kind: ImagingScheduleKind,
    shape: VisibilitySlabShape,
    worker_count: usize,
    prepared_residency: PreparedVisibilityResidency,
    visibility_cache_policy: VisibilityCachePolicy,
    visibility_cache_bytes: usize,
    visibility_cache_source_channels: usize,
    source_stream_buffer_bytes: usize,
    row_block_rows: usize,
    prepared_visibility_staging_bytes: usize,
    visibility_staging_bytes_per_plane: usize,
    live_prepared_visibility_bytes: usize,
    live_bucket_bytes: usize,
    product_scratch_bytes: usize,
    product_batch_planes: usize,
    resident_plane_state_bytes: usize,
    planned_active_bytes: usize,
    modeled_total_io_bytes: usize,
    modeled_source_read_bytes: usize,
    modeled_visibility_cache_fill_bytes: usize,
    modeled_visibility_cache_read_bytes: usize,
    modeled_output_spill_read_bytes: usize,
    modeled_output_spill_write_bytes: usize,
    modeled_product_write_bytes: usize,
    modeled_no_cache_source_read_bytes: usize,
    modeled_product_write_groups: usize,
    modeled_runtime_cost_bytes: usize,
}

impl SpectralExecutorCapabilities {
    fn supports(self, candidate: CandidatePlan) -> bool {
        match candidate.schedule_kind {
            ImagingScheduleKind::SourceFirst => {
                let output_spill_bytes = candidate
                    .modeled_output_spill_read_bytes
                    .saturating_add(candidate.modeled_output_spill_write_bytes);
                output_spill_bytes == 0 || self.source_first_output_spill
            }
            ImagingScheduleKind::SlabFirst => self.slab_first,
            ImagingScheduleKind::Hybrid => self.hybrid_full_source_cache,
        }
    }
}

pub(crate) fn plan_spectral_memory(
    input: SpectralMemoryPlannerInput,
) -> Result<SpectralMemoryPlan, String> {
    if input.output.plane_count == 0 {
        return Err("imaging memory planner requires at least one output plane".to_string());
    }
    if input.visibility.active_rows == 0 {
        return Err("imaging memory planner requires at least one active source row".to_string());
    }
    if input.visibility.full_source_channel_count == 0 {
        return Err("imaging memory planner requires at least one source channel".to_string());
    }
    if input.visibility.corr_count == 0 {
        return Err("imaging memory planner requires at least one correlation".to_string());
    }
    if input.visibility.slab_shapes.is_empty() {
        return Err(
            "imaging memory planner requires at least one slab shape candidate".to_string(),
        );
    }
    let nplanes = input.output.plane_count;
    let image_pixels = input.output.image_pixels();
    let per_plane_state_bytes = input.requirements.estimated_bytes_per_plane(image_pixels);
    let component_memory_breakdown = input.requirements.component_memory_breakdown(image_pixels);
    let modeled_product_write_bytes = input.product_write_bytes_per_plane.saturating_mul(nplanes);
    let fixed_reserved_bytes = input
        .fixed_frontend_bytes
        .saturating_add(input.worker_staging_bytes)
        .saturating_add(input.gpu_staging_bytes)
        .saturating_add(input.safety_margin_bytes);
    let row_channel_bytes = input.visibility.row_channel_bytes();
    let row_fixed_physical_bytes = input.visibility.row_fixed_physical_read_bytes();
    let row_fixed_resident_bytes = input.visibility.row_fixed_resident_bytes();
    let row_cache_overhead_bytes = input.visibility.row_cache_overhead_bytes();
    let modeled_full_cache_source_read_bytes = input.visibility.full_source_read_bytes();
    let max_row_block_rows = input
        .max_row_block_rows
        .max(1)
        .min(input.visibility.active_rows);

    let mut best_modeled = None::<CandidatePlan>;
    let mut selected = None::<CandidatePlan>;
    let mut rejected = Vec::<String>::new();
    let mut full_source_cache_candidate_fit = false;
    let mut candidate_io_costs = Vec::<String>::new();
    let mut candidate_count = 0usize;
    let prepared_residency = input.prepared_residency;

    for shape in input.visibility.slab_shapes.iter().copied() {
        if shape.active_planes == 0 || shape.active_planes > nplanes {
            rejected.push(format!("invalid_active_planes={}", shape.active_planes));
            continue;
        }
        let expected_slab_count = nplanes.div_ceil(shape.active_planes);
        if shape.slab_count != expected_slab_count {
            rejected.push(format!(
                "slab_count_mismatch_active_planes={}:expected={}:actual={}",
                shape.active_planes, expected_slab_count, shape.slab_count
            ));
            continue;
        }
        let active_plane_state_bytes = input
            .requirements
            .estimated_resident_bytes_for_active_planes(image_pixels, shape.active_planes);
        let prepared_visibility_staging_bytes =
            input.visibility.prepared_staging_bytes_for_source_channels(
                prepared_residency,
                shape.max_slab_source_channels,
            );
        let no_cache_source_read_bytes = input.visibility.no_cache_source_read_bytes(shape);
        let cache_read_bytes = input.visibility.cache_read_bytes(shape);
        let max_shape_workers = input
            .max_worker_count
            .max(1)
            .min(shape.active_planes.max(1));

        for worker_count in 1..=max_shape_workers {
            let max_product_batch_planes = input.executor_scratch.max_product_batch_planes(
                shape.active_planes,
                worker_count,
                input.memory_target_bytes,
            );
            for product_batch_planes in 1..=max_product_batch_planes {
                let base_scratch_bytes = input
                    .executor_scratch
                    .fixed_bytes
                    .saturating_add(
                        input
                            .executor_scratch
                            .per_active_plane_bytes
                            .saturating_mul(shape.active_planes),
                    )
                    .saturating_add(
                        input
                            .executor_scratch
                            .per_slab_source_channel_bytes
                            .saturating_mul(shape.max_slab_source_channels),
                    )
                    .saturating_add(
                        input
                            .executor_scratch
                            .per_worker_bytes
                            .saturating_mul(worker_count),
                    )
                    .saturating_add(
                        input
                            .executor_scratch
                            .per_product_batch_plane_bytes
                            .saturating_mul(product_batch_planes),
                    );
                let base_scratch_bytes = if product_batch_planes > 1 {
                    base_scratch_bytes.saturating_add(
                        input
                            .executor_scratch
                            .per_product_pending_plane_bytes
                            .saturating_mul(product_batch_planes),
                    )
                } else {
                    base_scratch_bytes
                };
                let slab_resident_bytes = active_plane_state_bytes
                    .saturating_add(prepared_visibility_staging_bytes)
                    .saturating_add(base_scratch_bytes);
                let static_resident_bytes =
                    fixed_reserved_bytes.saturating_add(slab_resident_bytes);

                for schedule_kind in ALL_IMAGING_SCHEDULES {
                    let candidate = match schedule_kind {
                        ImagingScheduleKind::SourceFirst => {
                            let source_channels = input.visibility.full_source_channel_count;
                            build_streaming_candidate(
                                schedule_kind,
                                shape,
                                worker_count,
                                prepared_residency,
                                VisibilityCachePolicy::Disabled,
                                0,
                                0,
                                source_channels,
                                static_resident_bytes,
                                prepared_visibility_staging_bytes,
                                active_plane_state_bytes,
                                modeled_full_cache_source_read_bytes,
                                modeled_full_cache_source_read_bytes,
                                0,
                                0,
                                per_plane_state_bytes,
                                modeled_product_write_bytes,
                                max_row_block_rows,
                                product_batch_planes,
                                &input,
                            )
                        }
                        ImagingScheduleKind::SlabFirst => {
                            let source_channels = shape.max_slab_source_channels;
                            build_streaming_candidate(
                                schedule_kind,
                                shape,
                                worker_count,
                                prepared_residency,
                                VisibilityCachePolicy::Disabled,
                                0,
                                0,
                                source_channels,
                                static_resident_bytes,
                                prepared_visibility_staging_bytes,
                                active_plane_state_bytes,
                                no_cache_source_read_bytes,
                                no_cache_source_read_bytes,
                                0,
                                0,
                                per_plane_state_bytes,
                                modeled_product_write_bytes,
                                max_row_block_rows,
                                product_batch_planes,
                                &input,
                            )
                        }
                        ImagingScheduleKind::Hybrid => {
                            if !input.visibility.full_source_cacheable {
                                rejected.push("hybrid_full_source_cache_not_enabled".to_string());
                                continue;
                            }
                            let visibility_cache_bytes = input.visibility.full_source_cache_bytes();
                            let source_channels = input.visibility.full_source_channel_count;
                            let candidate = build_streaming_candidate(
                                schedule_kind,
                                shape,
                                worker_count,
                                prepared_residency,
                                VisibilityCachePolicy::FullSource,
                                visibility_cache_bytes,
                                input.visibility.full_source_channel_count,
                                source_channels,
                                static_resident_bytes,
                                prepared_visibility_staging_bytes,
                                active_plane_state_bytes,
                                no_cache_source_read_bytes,
                                modeled_full_cache_source_read_bytes,
                                visibility_cache_bytes,
                                cache_read_bytes,
                                per_plane_state_bytes,
                                modeled_product_write_bytes,
                                max_row_block_rows,
                                product_batch_planes,
                                &input,
                            );
                            if candidate.is_some() {
                                full_source_cache_candidate_fit = true;
                            }
                            candidate
                        }
                    };
                    let Some(candidate) = candidate else {
                        rejected.push(format!(
                            "{}_does_not_fit_active_planes={}_workers={}_product_batch_planes={}",
                            schedule_kind.as_str(),
                            shape.active_planes,
                            worker_count,
                            product_batch_planes
                        ));
                        continue;
                    };
                    let executable = input.executor_capabilities.supports(candidate);
                    candidate_count = candidate_count.saturating_add(1);
                    if candidate_io_costs.len() < 16 {
                        candidate_io_costs.push(candidate_io_cost_fragment(candidate, executable));
                    }
                    consider_candidate(&mut best_modeled, candidate);
                    if executable {
                        consider_candidate(&mut selected, candidate);
                    } else {
                        rejected.push(format!(
                            "{}_not_supported_by_executor_active_planes={}_workers={}_product_batch_planes={}",
                            schedule_kind.as_str(),
                            shape.active_planes,
                            worker_count,
                            product_batch_planes
                        ));
                    }
                }
            }
        }
    }

    let Some(best_modeled) = best_modeled else {
        return Err(format!(
            "imaging memory target cannot fit any source/output schedule candidate: memory_target_bytes={} fixed_reserved_bytes={} per_plane_state_bytes={} row_channel_bytes={} row_fixed_physical_bytes={} row_fixed_resident_bytes={} row_cache_overhead_bytes={} rejected={}",
            input.memory_target_bytes,
            fixed_reserved_bytes,
            per_plane_state_bytes,
            row_channel_bytes,
            row_fixed_physical_bytes,
            row_fixed_resident_bytes,
            row_cache_overhead_bytes,
            if rejected.is_empty() {
                "none".to_string()
            } else {
                rejected.join("|")
            }
        ));
    };
    let Some(best) = selected else {
        return Err(format!(
            "imaging memory target cannot fit any executor-supported source/output schedule candidate: memory_target_bytes={} executor_capabilities={} best_modeled_schedule={} best_modeled_total_io_bytes={} rejected={}",
            input.memory_target_bytes,
            input.executor_capabilities.as_str(),
            best_modeled.schedule_kind.as_str(),
            best_modeled.modeled_total_io_bytes,
            if rejected.is_empty() {
                "none".to_string()
            } else {
                rejected.join("|")
            }
        ));
    };
    let candidate_io_costs = summarized_candidate_io_costs(
        candidate_io_costs,
        candidate_count,
        best_modeled,
        best,
        input.executor_capabilities.supports(best_modeled),
    );
    let remaining = input
        .memory_target_bytes
        .saturating_sub(best.planned_active_bytes);
    let cache_budget_bytes = remaining;
    let mut warnings = Vec::new();
    if best.visibility_cache_policy == VisibilityCachePolicy::Disabled
        && input.visibility.full_source_cacheable
    {
        if !full_source_cache_candidate_fit
            && modeled_full_cache_source_read_bytes < best.modeled_no_cache_source_read_bytes
        {
            warnings.push("full_source_visibility_cache_does_not_fit".to_string());
        } else if full_source_cache_candidate_fit {
            warnings.push("full_source_visibility_cache_not_selected_by_modeled_io".to_string());
        }
    }
    if best.schedule_kind != best_modeled.schedule_kind
        || best.shape.active_planes != best_modeled.shape.active_planes
        || best.visibility_cache_policy != best_modeled.visibility_cache_policy
    {
        warnings.push(format!(
            "best_modeled_schedule_not_executable_by_executor:best={}:selected={}",
            best_modeled.schedule_kind.as_str(),
            best.schedule_kind.as_str()
        ));
    }
    let visibility_cache_saved_read_bytes = best
        .modeled_no_cache_source_read_bytes
        .saturating_sub(best.modeled_source_read_bytes);
    let source_channel_visits = match best.schedule_kind {
        ImagingScheduleKind::SourceFirst | ImagingScheduleKind::Hybrid => {
            input.visibility.full_source_channel_count
        }
        ImagingScheduleKind::SlabFirst => best.shape.source_channel_visits,
    };
    let best_modeled_source_channel_visits = source_channel_visits_for_candidate(
        best_modeled,
        input.visibility.full_source_channel_count,
    );
    Ok(SpectralMemoryPlan {
        best_modeled_schedule_kind: best_modeled.schedule_kind,
        schedule_kind: best.schedule_kind,
        executor_capabilities: input.executor_capabilities,
        nplanes,
        image_shape: input.output.image_shape,
        active_planes: best.shape.active_planes,
        slab_count: best.shape.slab_count,
        slab_manifest: SpectralSlabManifest::for_planes(nplanes, best.shape.active_planes),
        row_block_rows: best.row_block_rows,
        cache_budget_bytes,
        cache_kind: CachePolicy::GeometryOnly,
        visibility_cache_policy: best.visibility_cache_policy,
        prepared_residency: best.prepared_residency,
        visibility_cache_bytes: best.visibility_cache_bytes,
        visibility_cache_source_channels: best.visibility_cache_source_channels,
        worker_count: best.worker_count,
        backend: "cpu_slab",
        memory_target_bytes: input.memory_target_bytes,
        fixed_frontend_bytes: input.fixed_frontend_bytes,
        source_stream_buffer_bytes: best.source_stream_buffer_bytes,
        worker_staging_bytes: input.worker_staging_bytes,
        per_plane_state_bytes,
        component_memory_breakdown,
        visibility_staging_bytes_per_plane: best.visibility_staging_bytes_per_plane,
        prepared_visibility_staging_bytes: best.prepared_visibility_staging_bytes,
        live_prepared_visibility_bytes: best.live_prepared_visibility_bytes,
        live_bucket_bytes: best.live_bucket_bytes,
        product_scratch_bytes: best.product_scratch_bytes,
        product_batch_planes: best.product_batch_planes,
        resident_plane_state_bytes: best.resident_plane_state_bytes,
        plane_state_residency: input.requirements.residency,
        source_buffer_residency: input.source_buffer_residency,
        gpu_staging_bytes: input.gpu_staging_bytes,
        safety_margin_bytes: input.safety_margin_bytes,
        planned_active_bytes: best.planned_active_bytes,
        source_channel_visits,
        max_slab_source_channels: best.shape.max_slab_source_channels,
        full_source_channel_count: input.visibility.full_source_channel_count,
        source_cell_channel_count: input.visibility.source_cell_channel_count,
        corr_count: input.visibility.corr_count,
        visibility_data_element_bytes: input.visibility.data_element_bytes,
        data_channel_read_granularity: input.visibility.data_channel_read_granularity,
        flag_channel_read_granularity: input.visibility.flag_channel_read_granularity,
        weight_spectrum_channel_read_granularity: input
            .visibility
            .weight_spectrum_channel_read_granularity,
        visibility_row_channel_bytes: row_channel_bytes,
        visibility_row_fixed_bytes: row_fixed_physical_bytes,
        visibility_row_fixed_resident_bytes: row_fixed_resident_bytes,
        visibility_row_cache_overhead_bytes: row_cache_overhead_bytes,
        visibility_resident_cache_layout: input.visibility.resident_layout_breakdown(),
        best_modeled_total_io_bytes: best_modeled.modeled_total_io_bytes,
        best_modeled_source_read_bytes: best_modeled.modeled_source_read_bytes,
        best_modeled_visibility_cache_io_bytes: best_modeled
            .modeled_visibility_cache_fill_bytes
            .saturating_add(best_modeled.modeled_visibility_cache_read_bytes),
        best_modeled_output_spill_io_bytes: best_modeled
            .modeled_output_spill_read_bytes
            .saturating_add(best_modeled.modeled_output_spill_write_bytes),
        best_modeled_product_write_bytes: best_modeled.modeled_product_write_bytes,
        best_modeled_active_planes: best_modeled.shape.active_planes,
        best_modeled_slab_count: best_modeled.shape.slab_count,
        best_modeled_source_channel_visits,
        modeled_total_io_bytes: best.modeled_total_io_bytes,
        modeled_source_read_bytes: best.modeled_source_read_bytes,
        modeled_visibility_cache_fill_bytes: best.modeled_visibility_cache_fill_bytes,
        modeled_visibility_cache_read_bytes: best.modeled_visibility_cache_read_bytes,
        modeled_visibility_cache_io_bytes: best
            .modeled_visibility_cache_fill_bytes
            .saturating_add(best.modeled_visibility_cache_read_bytes),
        modeled_output_spill_read_bytes: best.modeled_output_spill_read_bytes,
        modeled_output_spill_write_bytes: best.modeled_output_spill_write_bytes,
        modeled_output_spill_io_bytes: best
            .modeled_output_spill_read_bytes
            .saturating_add(best.modeled_output_spill_write_bytes),
        modeled_product_write_bytes: best.modeled_product_write_bytes,
        modeled_no_cache_source_read_bytes: best.modeled_no_cache_source_read_bytes,
        modeled_full_cache_source_read_bytes,
        visibility_cache_saved_read_bytes,
        candidate_io_costs: if candidate_io_costs.is_empty() {
            "none".to_string()
        } else {
            candidate_io_costs.join(";")
        },
        warnings,
    })
}

fn summarized_candidate_io_costs(
    mut fragments: Vec<String>,
    candidate_count: usize,
    best_modeled: CandidatePlan,
    selected: CandidatePlan,
    best_modeled_executable: bool,
) -> Vec<String> {
    let sampled = fragments.len();
    if candidate_count > sampled {
        fragments.push(format!(
            "omitted_candidate_count={}",
            candidate_count.saturating_sub(sampled)
        ));
    }
    fragments.push(format!(
        "best_modeled={}",
        candidate_io_cost_fragment(best_modeled, best_modeled_executable)
    ));
    fragments.push(format!(
        "selected={}",
        candidate_io_cost_fragment(selected, true)
    ));
    fragments
}

#[allow(clippy::too_many_arguments)]
fn build_streaming_candidate(
    schedule_kind: ImagingScheduleKind,
    shape: VisibilitySlabShape,
    worker_count: usize,
    prepared_residency: PreparedVisibilityResidency,
    visibility_cache_policy: VisibilityCachePolicy,
    visibility_cache_bytes: usize,
    visibility_cache_source_channels: usize,
    source_channels: usize,
    static_resident_bytes: usize,
    prepared_visibility_staging_bytes: usize,
    resident_plane_state_bytes: usize,
    modeled_no_cache_source_read_bytes: usize,
    modeled_source_read_bytes: usize,
    modeled_visibility_cache_fill_bytes: usize,
    modeled_visibility_cache_read_bytes: usize,
    per_plane_state_bytes: usize,
    modeled_product_write_bytes: usize,
    max_row_block_rows: usize,
    product_batch_planes: usize,
    input: &SpectralMemoryPlannerInput,
) -> Option<CandidatePlan> {
    let static_plus_cache_bytes = static_resident_bytes.saturating_add(visibility_cache_bytes);
    if static_plus_cache_bytes >= input.memory_target_bytes {
        return None;
    }
    let raw_full_slab_source_bytes = match input.source_buffer_residency {
        SourceBufferResidency::RowBlockStream => 0,
        SourceBufferResidency::FullSlabRawSource => input
            .visibility
            .raw_source_buffer_bytes_for_rows(input.visibility.active_rows, source_channels),
    };
    let static_plus_cache_and_raw_source_bytes =
        static_plus_cache_bytes.saturating_add(raw_full_slab_source_bytes);
    if static_plus_cache_and_raw_source_bytes >= input.memory_target_bytes {
        return None;
    }
    let per_source_row_buffer_bytes = match input.source_buffer_residency {
        SourceBufferResidency::RowBlockStream => {
            input
                .visibility
                .source_buffer_bytes_for_rows(prepared_residency, 1, source_channels)
        }
        SourceBufferResidency::FullSlabRawSource => input
            .visibility
            .live_source_scratch_bytes_for_rows(prepared_residency, 1, source_channels),
    };
    if per_source_row_buffer_bytes == 0 {
        return None;
    }
    let max_row_block_rows = input
        .executor_scratch
        .max_row_block_rows()
        .map(|limit| max_row_block_rows.min(limit))
        .unwrap_or(max_row_block_rows)
        .min(source_resident_locality_row_block_cap(
            schedule_kind,
            shape,
            input.source_buffer_residency,
            input.visibility.active_rows,
            worker_count,
        ))
        .max(1);
    let per_row_executor_scratch_bytes = input
        .executor_scratch
        .per_worker_row_block_bytes
        .saturating_mul(worker_count.max(1));
    let per_resident_row_bytes =
        per_source_row_buffer_bytes.saturating_add(per_row_executor_scratch_bytes);
    if per_resident_row_bytes == 0 {
        return None;
    }
    let row_buffer_budget = input.memory_target_bytes - static_plus_cache_and_raw_source_bytes;
    let row_block_rows = (row_buffer_budget / per_resident_row_bytes).clamp(1, max_row_block_rows);
    let product_scratch_bytes = input.executor_scratch.bytes_for_worker_count_and_rows(
        shape.active_planes,
        shape.max_slab_source_channels,
        worker_count,
        row_block_rows,
        product_batch_planes,
    );
    let row_block_executor_scratch_bytes =
        per_row_executor_scratch_bytes.saturating_mul(row_block_rows);
    let source_stream_buffer_bytes = input.visibility.resident_source_buffer_bytes(
        input.source_buffer_residency,
        prepared_residency,
        row_block_rows,
        source_channels,
    );
    let live_prepared_visibility_bytes = input.visibility.live_prepared_bytes_for_rows(
        prepared_residency,
        row_block_rows,
        source_channels,
    );
    let live_bucket_bytes = input.visibility.live_bucket_bytes_for_rows(
        prepared_residency,
        row_block_rows,
        source_channels,
    );
    let planned_active_bytes = static_plus_cache_bytes
        .saturating_add(source_stream_buffer_bytes)
        .saturating_add(row_block_executor_scratch_bytes);
    if planned_active_bytes > input.memory_target_bytes {
        return None;
    }
    let (modeled_output_spill_read_bytes, modeled_output_spill_write_bytes) = match schedule_kind {
        ImagingScheduleKind::SourceFirst => source_first_output_spill_bytes(
            input.output.plane_count,
            shape.active_planes,
            per_plane_state_bytes,
            input.visibility.active_rows,
            row_block_rows,
        ),
        ImagingScheduleKind::SlabFirst | ImagingScheduleKind::Hybrid => (0, 0),
    };
    let modeled_total_io_bytes = modeled_source_read_bytes
        .saturating_add(modeled_visibility_cache_fill_bytes)
        .saturating_add(modeled_visibility_cache_read_bytes)
        .saturating_add(modeled_output_spill_read_bytes)
        .saturating_add(modeled_output_spill_write_bytes)
        .saturating_add(modeled_product_write_bytes);
    let modeled_product_write_groups = input
        .output
        .plane_count
        .div_ceil(product_batch_planes.max(1));
    Some(CandidatePlan {
        schedule_kind,
        shape,
        worker_count,
        prepared_residency,
        visibility_cache_policy,
        visibility_cache_bytes,
        visibility_cache_source_channels,
        source_stream_buffer_bytes,
        row_block_rows,
        prepared_visibility_staging_bytes,
        visibility_staging_bytes_per_plane: prepared_visibility_staging_bytes
            / shape.active_planes.max(1),
        live_prepared_visibility_bytes,
        live_bucket_bytes,
        product_scratch_bytes,
        product_batch_planes,
        resident_plane_state_bytes,
        planned_active_bytes,
        modeled_total_io_bytes,
        modeled_source_read_bytes,
        modeled_visibility_cache_fill_bytes,
        modeled_visibility_cache_read_bytes,
        modeled_output_spill_read_bytes,
        modeled_output_spill_write_bytes,
        modeled_product_write_bytes,
        modeled_no_cache_source_read_bytes,
        modeled_product_write_groups,
        modeled_runtime_cost_bytes: modeled_runtime_cost_bytes(
            modeled_total_io_bytes,
            per_plane_state_bytes,
            input
                .product_write_bytes_per_plane
                .saturating_mul(modeled_product_write_groups),
            input.output.plane_count,
            input.visibility.active_rows,
            row_block_rows,
            shape,
            worker_count,
        ),
    })
}

fn source_first_output_spill_bytes(
    nplanes: usize,
    active_planes: usize,
    per_plane_state_bytes: usize,
    active_rows: usize,
    row_block_rows: usize,
) -> (usize, usize) {
    if active_planes >= nplanes {
        return (0, 0);
    }
    let row_block_count = active_rows.div_ceil(row_block_rows.max(1)).max(1);
    let all_plane_state_bytes = per_plane_state_bytes.saturating_mul(nplanes);
    let bytes = all_plane_state_bytes.saturating_mul(row_block_count);
    (bytes, bytes)
}

fn source_resident_locality_row_block_cap(
    schedule_kind: ImagingScheduleKind,
    shape: VisibilitySlabShape,
    source_buffer_residency: SourceBufferResidency,
    active_rows: usize,
    worker_count: usize,
) -> usize {
    if source_buffer_residency != SourceBufferResidency::FullSlabRawSource
        || (schedule_kind == ImagingScheduleKind::SlabFirst && shape.slab_count > 1)
    {
        return active_rows.max(1);
    }
    let min_source_blocks = if worker_count <= 1 {
        1
    } else {
        integer_sqrt(worker_count).saturating_add(2)
    };
    active_rows.div_ceil(min_source_blocks).max(1)
}

fn integer_sqrt(value: usize) -> usize {
    if value <= 1 {
        return value;
    }
    let mut root = 1usize;
    while root
        .saturating_add(1)
        .saturating_mul(root.saturating_add(1))
        <= value
    {
        root += 1;
    }
    root
}

#[allow(clippy::too_many_arguments)]
fn modeled_runtime_cost_bytes(
    modeled_total_io_bytes: usize,
    per_plane_state_bytes: usize,
    modeled_product_write_group_overhead_bytes: usize,
    nplanes: usize,
    active_rows: usize,
    row_block_rows: usize,
    shape: VisibilitySlabShape,
    worker_count: usize,
) -> usize {
    let worker_count = worker_count.max(1);
    let source_block_count = shape
        .slab_count
        .saturating_mul(active_rows.div_ceil(row_block_rows.max(1)));
    modeled_total_io_bytes
        .saturating_add(
            per_plane_state_bytes.saturating_mul(total_plane_worker_waves(
                nplanes,
                shape.active_planes,
                worker_count,
            )),
        )
        .saturating_add(
            per_plane_state_bytes
                .saturating_div(worker_count)
                .saturating_mul(source_block_count),
        )
        .saturating_add(modeled_product_write_group_overhead_bytes)
}

fn total_plane_worker_waves(nplanes: usize, active_planes: usize, worker_count: usize) -> usize {
    let active_planes = active_planes.max(1);
    let worker_count = worker_count.max(1);
    let full_slabs = nplanes / active_planes;
    let remainder = nplanes % active_planes;
    full_slabs
        .saturating_mul(active_planes.div_ceil(worker_count))
        .saturating_add(if remainder == 0 {
            0
        } else {
            remainder.div_ceil(worker_count)
        })
}

fn consider_candidate(best: &mut Option<CandidatePlan>, candidate: CandidatePlan) {
    let replace = best.is_none_or(|current| {
        (
            candidate.modeled_runtime_cost_bytes,
            candidate.modeled_total_io_bytes,
            candidate.modeled_source_read_bytes,
            candidate
                .modeled_visibility_cache_fill_bytes
                .saturating_add(candidate.modeled_visibility_cache_read_bytes),
            candidate
                .modeled_output_spill_read_bytes
                .saturating_add(candidate.modeled_output_spill_write_bytes),
            candidate.modeled_product_write_groups,
            schedule_rank(candidate.schedule_kind),
            match candidate.visibility_cache_policy {
                VisibilityCachePolicy::FullSource => 0usize,
                VisibilityCachePolicy::Disabled => 1usize,
            },
            usize::MAX.saturating_sub(candidate.worker_count),
            candidate.shape.slab_count,
            usize::MAX.saturating_sub(candidate.row_block_rows),
            prepared_residency_rank(candidate.prepared_residency),
        ) < (
            current.modeled_runtime_cost_bytes,
            current.modeled_total_io_bytes,
            current.modeled_source_read_bytes,
            current
                .modeled_visibility_cache_fill_bytes
                .saturating_add(current.modeled_visibility_cache_read_bytes),
            current
                .modeled_output_spill_read_bytes
                .saturating_add(current.modeled_output_spill_write_bytes),
            current.modeled_product_write_groups,
            schedule_rank(current.schedule_kind),
            match current.visibility_cache_policy {
                VisibilityCachePolicy::FullSource => 0usize,
                VisibilityCachePolicy::Disabled => 1usize,
            },
            usize::MAX.saturating_sub(current.worker_count),
            current.shape.slab_count,
            usize::MAX.saturating_sub(current.row_block_rows),
            prepared_residency_rank(current.prepared_residency),
        )
    });
    if replace {
        *best = Some(candidate);
    }
}

fn source_channel_visits_for_candidate(
    candidate: CandidatePlan,
    full_source_channel_count: usize,
) -> usize {
    match candidate.schedule_kind {
        ImagingScheduleKind::SourceFirst | ImagingScheduleKind::Hybrid => full_source_channel_count,
        ImagingScheduleKind::SlabFirst => candidate.shape.source_channel_visits,
    }
}

fn candidate_io_cost_fragment(candidate: CandidatePlan, executable: bool) -> String {
    let cache_io = candidate
        .modeled_visibility_cache_fill_bytes
        .saturating_add(candidate.modeled_visibility_cache_read_bytes);
    let spill_io = candidate
        .modeled_output_spill_read_bytes
        .saturating_add(candidate.modeled_output_spill_write_bytes);
    format!(
        "{}:runtime={},total={},source={},cache={},spill={},product={},product_groups={},active_planes={},slab_count={},row_block_rows={},worker_count={},scratch={},product_batch_planes={},cache_policy={},residency={},executable={}",
        candidate.schedule_kind.as_str(),
        candidate.modeled_runtime_cost_bytes,
        candidate.modeled_total_io_bytes,
        candidate.modeled_source_read_bytes,
        cache_io,
        spill_io,
        candidate.modeled_product_write_bytes,
        candidate.modeled_product_write_groups,
        candidate.shape.active_planes,
        candidate.shape.slab_count,
        candidate.row_block_rows,
        candidate.worker_count,
        candidate.product_scratch_bytes,
        candidate.product_batch_planes,
        visibility_cache_policy_name(candidate.visibility_cache_policy),
        prepared_residency_name(candidate.prepared_residency),
        executable,
    )
}

fn schedule_rank(schedule: ImagingScheduleKind) -> usize {
    match schedule {
        ImagingScheduleKind::SourceFirst => 0,
        ImagingScheduleKind::Hybrid => 1,
        ImagingScheduleKind::SlabFirst => 2,
    }
}

fn option_usize(value: Option<usize>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unset".to_string())
}

fn option_isize(value: Option<isize>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unset".to_string())
}

fn cache_policy_name(policy: CachePolicy) -> &'static str {
    match policy {
        CachePolicy::Disabled => "disabled",
        CachePolicy::GeometryOnly => "geometry_only",
    }
}

fn visibility_cache_policy_name(policy: VisibilityCachePolicy) -> &'static str {
    match policy {
        VisibilityCachePolicy::Disabled => "disabled",
        VisibilityCachePolicy::FullSource => "full_source",
    }
}

fn prepared_residency_name(policy: PreparedVisibilityResidency) -> &'static str {
    let _ = policy;
    "row_block_stream"
}

fn prepared_residency_rank(policy: PreparedVisibilityResidency) -> usize {
    let _ = policy;
    0
}

#[cfg(target_os = "macos")]
fn current_rss_bytes() -> Option<usize> {
    #[repr(C)]
    #[derive(Default)]
    struct ProcTaskInfo {
        virtual_size: u64,
        resident_size: u64,
        total_user: u64,
        total_system: u64,
        threads_user: u64,
        threads_system: u64,
        policy: i32,
        faults: i32,
        pageins: i32,
        cow_faults: i32,
        messages_sent: i32,
        messages_received: i32,
        syscalls_mach: i32,
        syscalls_unix: i32,
        csw: i32,
        threadnum: i32,
        numrunning: i32,
        priority: i32,
    }

    const PROC_PIDTASKINFO: i32 = 4;

    unsafe extern "C" {
        fn proc_pidinfo(
            pid: libc::c_int,
            flavor: libc::c_int,
            arg: u64,
            buffer: *mut libc::c_void,
            buffersize: libc::c_int,
        ) -> libc::c_int;
    }

    let mut info = ProcTaskInfo::default();
    let size = std::mem::size_of::<ProcTaskInfo>();
    let status = unsafe {
        proc_pidinfo(
            libc::getpid(),
            PROC_PIDTASKINFO,
            0,
            std::ptr::addr_of_mut!(info).cast::<libc::c_void>(),
            size as libc::c_int,
        )
    };
    if status == size as libc::c_int {
        usize::try_from(info.resident_size).ok()
    } else {
        None
    }
}

#[cfg(target_os = "linux")]
fn current_rss_bytes() -> Option<usize> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages = statm.split_whitespace().nth(1)?.parse::<usize>().ok()?;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return None;
    }
    resident_pages.checked_mul(page_size as usize)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn current_rss_bytes() -> Option<usize> {
    None
}

fn peak_rss_bytes() -> Option<usize> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return None;
    }
    let usage = unsafe { usage.assume_init() };
    let raw = usize::try_from(usage.ru_maxrss).ok()?;
    #[cfg(target_os = "linux")]
    {
        raw.checked_mul(1024)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Some(raw)
    }
}

impl fmt::Display for SpectralPlaneDescriptorKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "plane-{}", self.output_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observability_log_line_uses_stable_labels() {
        let line = SpectralObservabilityEvent {
            mode: "cube",
            pass_kind: ImagingPassKind::InitialDirty,
            stage: SpectralEventStage::SourceRead,
            slab_id: Some(2),
            plane_start: 4,
            plane_end: 8,
            row_block_rows: Some(32768),
            bytes_read: Some(1024),
            bytes_written: None,
            worker_count: Some(4),
            backend: "cpu_slab",
            elapsed_ms: Some(12),
            estimated_resident_bytes: Some(4096),
        }
        .log_line();
        assert!(line.starts_with("spectral_slab_event "));
        assert!(line.contains("mode=cube"));
        assert!(line.contains("pass_kind=initial_dirty"));
        assert!(line.contains("stage=source_read"));
        assert!(line.contains("slab_id=2"));
        assert!(line.contains("plane_start=4"));
        assert!(line.contains("plane_end=8"));
        assert!(line.contains("bytes_written=unset"));
    }

    #[test]
    fn observability_stage_and_pass_vocabularies_cover_wave_contract() {
        let pass_labels = [
            (ImagingPassKind::WeightingDensity, "weighting_density"),
            (ImagingPassKind::Psf, "psf"),
            (ImagingPassKind::InitialDirty, "initial_dirty"),
            (
                ImagingPassKind::MinorCycleDiagnostics,
                "minor_cycle_diagnostics",
            ),
            (ImagingPassKind::MinorCycleUpdate, "minor_cycle_update"),
            (ImagingPassKind::ResidualRefresh, "residual_refresh"),
            (ImagingPassKind::ProductWrite, "product_write"),
        ];
        for (kind, expected) in pass_labels {
            assert_eq!(kind.as_str(), expected);
        }

        let stage_labels = [
            (SpectralEventStage::SourceRead, "source_read"),
            (SpectralEventStage::RowBlockPreparation, "row_block_prepare"),
            (SpectralEventStage::VisibilityRouting, "visibility_routing"),
            (SpectralEventStage::WeightingDensity, "weighting_density"),
            (SpectralEventStage::PsfDirty, "psf_dirty"),
            (
                SpectralEventStage::MinorCycleDiagnostics,
                "minor_cycle_diagnostics",
            ),
            (SpectralEventStage::MinorCycleUpdate, "minor_cycle_update"),
            (SpectralEventStage::ResidualRefresh, "residual_refresh"),
            (SpectralEventStage::PlaneStateLoad, "plane_state_load"),
            (SpectralEventStage::PlaneStateStore, "plane_state_store"),
            (SpectralEventStage::ProductWrite, "product_write"),
            (SpectralEventStage::CacheFill, "cache_fill"),
            (SpectralEventStage::CacheHit, "cache_hit"),
            (SpectralEventStage::CacheMiss, "cache_miss"),
            (SpectralEventStage::BackendExecution, "backend_execution"),
            (SpectralEventStage::Planner, "planner"),
        ];
        for (stage, expected) in stage_labels {
            assert_eq!(stage.as_str(), expected);
        }
    }

    #[test]
    fn spectral_memory_observation_log_line_reports_actual_and_planned_memory() {
        let line = SpectralMemoryObservation {
            mode: "cube",
            stage: "after_prepare",
            slab_id: Some(1),
            plane_start: 2,
            plane_end: 4,
            current_rss_bytes: Some(1024),
            peak_rss_bytes: Some(2048),
            delta_from_baseline_bytes: Some(512),
            delta_from_previous_bytes: Some(-128),
            estimated_resident_bytes: Some(4096),
            planned_active_bytes: 8192,
            visibility_staging_bytes: 256,
            plane_state_bytes: 128,
            product_scratch_bytes: 64,
            cache_budget_bytes: 32,
            note: "prepared",
        }
        .log_line();
        assert!(line.starts_with("spectral_slab_memory "));
        assert!(line.contains("stage=after_prepare"));
        assert!(line.contains("current_rss_bytes=1024"));
        assert!(line.contains("peak_rss_bytes=2048"));
        assert!(line.contains("delta_from_previous_bytes=-128"));
        assert!(line.contains("visibility_staging_bytes=256"));
    }

    #[test]
    fn descriptors_are_stable_for_first_middle_last_and_line_peak() {
        let descriptors =
            basic_plane_descriptors(5, Some(0), 10, &[1, 3], SpectralInterpolationPolicy::Linear);
        assert_eq!(descriptors[0].output_index, 0);
        assert_eq!(descriptors[0].spw_id, Some(0));
        assert_eq!(descriptors[0].source_channel_start, 10);
        assert_eq!(descriptors[2].source_channel_start, 12);
        assert_eq!(descriptors[4].source_channel_start, 14);
        assert_eq!(descriptors[2].source_channel_count, 2);
        assert_eq!(descriptors[2].field_ids, vec![1, 3]);
        assert_eq!(
            descriptors[2].interpolation,
            SpectralInterpolationPolicy::Linear
        );
        assert_eq!(descriptors[2].coordinate_label, "plane-2");
        let repeat =
            basic_plane_descriptors(5, Some(0), 10, &[1, 3], SpectralInterpolationPolicy::Linear);
        assert_eq!(descriptors, repeat);
    }

    #[test]
    fn slab_manifest_splits_plane_ranges_deterministically() {
        let slabs = SpectralSlabManifest::for_planes(9, 4);
        assert_eq!(
            slabs,
            vec![
                SpectralSlabManifest {
                    slab_id: 0,
                    plane_start: 0,
                    plane_end: 4,
                },
                SpectralSlabManifest {
                    slab_id: 1,
                    plane_start: 4,
                    plane_end: 8,
                },
                SpectralSlabManifest {
                    slab_id: 2,
                    plane_start: 8,
                    plane_end: 9,
                },
            ]
        );
    }

    #[test]
    fn plane_state_requirements_estimate_component_memory() {
        let dirty = PlaneStateRequirements::dirty_standard();
        let clean = PlaneStateRequirements::bounded_clean();
        let multiscale = PlaneStateRequirements::multiscale_clean();
        let mosaic = PlaneStateRequirements::mosaic_pb_aware();
        assert!(dirty.estimated_bytes_per_plane(1024) > 0);
        assert!(clean.estimated_bytes_per_plane(1024) > dirty.estimated_bytes_per_plane(1024));
        assert!(multiscale.estimated_bytes_per_plane(1024) > clean.estimated_bytes_per_plane(1024));
        assert!(mosaic.estimated_bytes_per_plane(1024) > clean.estimated_bytes_per_plane(1024));
        assert!(dirty.component_list().contains("model"));
        assert!(dirty.component_list().contains("image"));
        assert!(multiscale.component_list().contains("mask"));
        assert!(mosaic.component_list().contains("pbcor"));
        let breakdown = mosaic.component_memory_breakdown(1024);
        assert!(breakdown.contains("residual:"));
        assert!(breakdown.contains("pbcor:"));
    }

    #[test]
    fn in_memory_plane_state_store_traces_deterministic_load_store_order() {
        let mut store = InMemoryPlaneStateStore::default();
        let mut dirty_mask = PlaneStateDirtyMask::default();
        dirty_mask.mark(PlaneComponent::Residual);
        let state = SpectralPlaneState {
            descriptor: SpectralPlaneDescriptorKey { output_index: 3 },
            requirements: PlaneStateRequirements::dirty_standard(),
            dirty_mask,
            coordinate_metadata: "coords".into(),
            beam_metadata: "beam".into(),
            mask_metadata: "mask full-plane".into(),
            primary_beam_metadata: "pb none".into(),
            pbcor_metadata: "pbcor none".into(),
            sumwt_present: true,
            product_write_state: "pending".into(),
        };
        store.store(state.clone());
        assert_eq!(
            store.load(SpectralPlaneDescriptorKey { output_index: 3 }),
            Some(state.clone())
        );
        assert_eq!(store.trace()[0].op, "store");
        assert_eq!(store.trace()[1].op, "load");
        assert!(store.trace()[0].dirty_components.contains("residual"));
        assert_eq!(store.io_stats().store_count, 1);
        assert_eq!(store.io_stats().load_count, 1);
        assert!(store.io_stats().bytes_written > 0);
        assert!(store.io_stats().bytes_read > 0);
        assert_eq!(
            store.cleanup(SpectralPlaneDescriptorKey { output_index: 3 }),
            Some(state)
        );
        assert_eq!(store.trace()[2].op, "cleanup");
        assert_eq!(store.io_stats().cleanup_count, 1);
    }

    #[test]
    fn product_backed_plane_state_store_preserves_metadata_and_reports_spill_io() {
        let mut store = ProductBackedPlaneStateStore::new(
            1024,
            [
                PlaneComponent::Psf,
                PlaneComponent::Residual,
                PlaneComponent::Image,
                PlaneComponent::Sumwt,
            ],
        );
        let mut dirty_mask = PlaneStateDirtyMask::default();
        dirty_mask.mark(PlaneComponent::Psf);
        dirty_mask.mark(PlaneComponent::Residual);
        let state = SpectralPlaneState {
            descriptor: SpectralPlaneDescriptorKey { output_index: 7 },
            requirements: PlaneStateRequirements::dirty_standard(),
            dirty_mask,
            coordinate_metadata: "direction+spectral coords".into(),
            beam_metadata: "beam major minor pa".into(),
            mask_metadata: "mask none".into(),
            primary_beam_metadata: "pb none".into(),
            pbcor_metadata: "pbcor none".into(),
            sumwt_present: true,
            product_write_state: "write-through complete".into(),
        };

        store.store(state.clone());
        assert_eq!(store.trace()[0].op, "product_store");
        assert_eq!(store.trace()[0].output_index, 7);
        assert!(store.trace()[0].components.contains("psf"));
        assert!(store.trace()[0].dirty_components.contains("residual"));
        assert!(store.io_stats().bytes_written >= 1024 * std::mem::size_of::<f32>() * 3);

        let loaded = store
            .load(SpectralPlaneDescriptorKey { output_index: 7 })
            .expect("product-backed store should reload state metadata");
        assert_eq!(loaded, state);
        assert_eq!(store.trace()[1].op, "product_load");
        assert!(store.io_stats().bytes_read > 0);

        let cleaned = store.cleanup(SpectralPlaneDescriptorKey { output_index: 7 });
        assert_eq!(cleaned, Some(state));
        assert_eq!(store.trace()[2].op, "product_cleanup");
        assert_eq!(store.io_stats().cleanup_count, 1);
        assert_eq!(
            store.load(SpectralPlaneDescriptorKey { output_index: 7 }),
            None
        );
    }

    #[test]
    fn pass_requirements_model_scalar_reductions_and_mutation() {
        let diagnostics = PassRequirements::for_pass(ImagingPassKind::MinorCycleDiagnostics);
        assert_eq!(diagnostics.visibility, VisibilityRequirement::None);
        assert!(
            diagnostics
                .scalar_reductions
                .contains(&ScalarReductionKind::GlobalPeak)
        );
        assert!(
            diagnostics
                .scalar_reductions
                .contains(&ScalarReductionKind::CycleThreshold)
        );
        assert!(
            diagnostics
                .scalar_reductions
                .contains(&ScalarReductionKind::MajorCycleTransition)
        );

        let refresh = PassRequirements::for_pass(ImagingPassKind::ResidualRefresh);
        assert_eq!(refresh.visibility, VisibilityRequirement::ModelDependent);
        assert!(refresh.required_components.contains(&PlaneComponent::Model));
        assert!(
            refresh
                .mutable_components
                .contains(&PlaneComponent::Residual)
        );
    }

    #[test]
    fn scalar_reductions_are_slab_size_independent_for_max_and_counts() {
        let all_at_once = ScalarReductionState {
            global_peak_jy_per_beam: 7.0,
            nsigma_threshold_jy_per_beam: 3.0,
            cycle_threshold_jy_per_beam: 2.0,
            niter_done: 5,
            mask_pixels: 11,
            stop: true,
        };
        let merged = ScalarReductionState {
            global_peak_jy_per_beam: 2.0,
            nsigma_threshold_jy_per_beam: 1.0,
            cycle_threshold_jy_per_beam: 2.0,
            niter_done: 2,
            mask_pixels: 4,
            stop: false,
        }
        .merge(ScalarReductionState {
            global_peak_jy_per_beam: 7.0,
            nsigma_threshold_jy_per_beam: 3.0,
            cycle_threshold_jy_per_beam: 1.5,
            niter_done: 3,
            mask_pixels: 7,
            stop: true,
        });
        assert_eq!(merged, all_at_once);
    }

    #[test]
    fn representative_cube_clean_scalar_reductions_are_slab_size_independent() {
        let per_plane = [
            ScalarReductionState {
                global_peak_jy_per_beam: 3.0,
                nsigma_threshold_jy_per_beam: 1.2,
                cycle_threshold_jy_per_beam: 0.9,
                niter_done: 1,
                mask_pixels: 12,
                stop: false,
            },
            ScalarReductionState {
                global_peak_jy_per_beam: 8.0,
                nsigma_threshold_jy_per_beam: 1.2,
                cycle_threshold_jy_per_beam: 1.7,
                niter_done: 4,
                mask_pixels: 3,
                stop: false,
            },
            ScalarReductionState {
                global_peak_jy_per_beam: 5.0,
                nsigma_threshold_jy_per_beam: 2.0,
                cycle_threshold_jy_per_beam: 1.1,
                niter_done: 0,
                mask_pixels: 19,
                stop: true,
            },
            ScalarReductionState {
                global_peak_jy_per_beam: 1.0,
                nsigma_threshold_jy_per_beam: 0.8,
                cycle_threshold_jy_per_beam: 0.5,
                niter_done: 2,
                mask_pixels: 0,
                stop: false,
            },
        ];
        let reduce_with_slab_width = |slab_width: usize| {
            per_plane
                .chunks(slab_width)
                .map(|slab| {
                    slab.iter()
                        .copied()
                        .fold(ScalarReductionState::default(), ScalarReductionState::merge)
                })
                .fold(ScalarReductionState::default(), ScalarReductionState::merge)
        };

        let all_planes = reduce_with_slab_width(per_plane.len());
        assert_eq!(reduce_with_slab_width(1), all_planes);
        assert_eq!(reduce_with_slab_width(2), all_planes);
        assert_eq!(reduce_with_slab_width(4), all_planes);
        assert_eq!(all_planes.global_peak_jy_per_beam, 8.0);
        assert_eq!(all_planes.nsigma_threshold_jy_per_beam, 2.0);
        assert_eq!(all_planes.cycle_threshold_jy_per_beam, 1.7);
        assert_eq!(all_planes.niter_done, 7);
        assert_eq!(all_planes.mask_pixels, 34);
        assert!(all_planes.stop);
    }

    fn test_slab_shapes(
        nplanes: usize,
        source_channels_per_plane: usize,
    ) -> Vec<VisibilitySlabShape> {
        (1..=nplanes)
            .map(|active_planes| {
                let slab_count = nplanes.div_ceil(active_planes);
                VisibilitySlabShape {
                    active_planes,
                    slab_count,
                    source_channel_visits: nplanes.saturating_mul(source_channels_per_plane),
                    max_slab_source_channels: active_planes
                        .saturating_mul(source_channels_per_plane),
                }
            })
            .collect()
    }

    fn reread_all_source_slab_shapes(
        nplanes: usize,
        full_source_channels: usize,
    ) -> Vec<VisibilitySlabShape> {
        (1..=nplanes)
            .map(|active_planes| {
                let slab_count = nplanes.div_ceil(active_planes);
                VisibilitySlabShape {
                    active_planes,
                    slab_count,
                    source_channel_visits: slab_count.saturating_mul(full_source_channels),
                    max_slab_source_channels: full_source_channels,
                }
            })
            .collect()
    }

    fn overlapping_channel_slab_shapes(
        nplanes: usize,
        full_source_channels: usize,
    ) -> Vec<VisibilitySlabShape> {
        (1..=nplanes)
            .map(|active_planes| {
                let slabs = SpectralSlabManifest::for_planes(nplanes, active_planes);
                let mut source_channel_visits = 0usize;
                let mut max_slab_source_channels = 0usize;
                for slab in &slabs {
                    let plane_count = slab.plane_end - slab.plane_start;
                    let source_channels = plane_count.saturating_add(1).min(full_source_channels);
                    source_channel_visits = source_channel_visits.saturating_add(source_channels);
                    max_slab_source_channels = max_slab_source_channels.max(source_channels);
                }
                VisibilitySlabShape {
                    active_planes,
                    slab_count: slabs.len(),
                    source_channel_visits,
                    max_slab_source_channels,
                }
            })
            .collect()
    }

    fn test_visibility_shape(
        active_rows: usize,
        full_source_channel_count: usize,
        corr_count: usize,
        data_element_bytes: usize,
        full_source_cacheable: bool,
        slab_shapes: Vec<VisibilitySlabShape>,
    ) -> VisibilitySourceShape {
        VisibilitySourceShape {
            active_rows,
            full_source_channel_count,
            source_cell_channel_count: full_source_channel_count,
            corr_count,
            data_element_bytes,
            flag_element_bytes: 1,
            weight_element_bytes: 4,
            weight_spectrum_element_bytes: Some(4),
            data_channel_read_granularity: VisibilityChannelReadGranularity::RequestedRange,
            flag_channel_read_granularity: VisibilityChannelReadGranularity::RequestedRange,
            weight_spectrum_channel_read_granularity: Some(
                VisibilityChannelReadGranularity::RequestedRange,
            ),
            uvw_element_bytes: 8,
            antenna_element_bytes: 4,
            time_element_bytes: Some(8),
            pointing_id_element_bytes: None,
            resident_layout: VisibilityResidentLayout::standard_spectral_cube_columnar(
                corr_count, 4,
            ),
            prepared_sample_bytes: 64,
            full_source_cacheable,
            slab_shapes,
        }
    }

    fn planner_input(
        nplanes: usize,
        image_shape: [usize; 2],
        visibility: VisibilitySourceShape,
        memory_target_bytes: usize,
    ) -> SpectralMemoryPlannerInput {
        let corr_count = visibility.corr_count;
        SpectralMemoryPlannerInput {
            output: ImagingOutputShape {
                plane_count: nplanes,
                image_shape,
            },
            visibility,
            executor_capabilities: SpectralExecutorCapabilities::all(),
            memory_target_bytes,
            fixed_frontend_bytes: 0,
            worker_staging_bytes: 0,
            gpu_staging_bytes: 0,
            safety_margin_bytes: 0,
            executor_scratch: ExecutorScratchShape {
                fixed_bytes: image_shape[0]
                    .saturating_mul(image_shape[1])
                    .saturating_mul(std::mem::size_of::<f32>())
                    .saturating_mul(3),
                per_active_plane_bytes: 0,
                per_slab_source_channel_bytes: 0,
                per_worker_bytes: 0,
                per_worker_row_block_bytes: 0,
                per_worker_row_block_limit_bytes: 0,
                per_product_batch_plane_bytes: 0,
                per_product_pending_plane_bytes: 0,
            },
            source_buffer_residency: SourceBufferResidency::RowBlockStream,
            product_write_bytes_per_plane: image_shape[0]
                .saturating_mul(image_shape[1])
                .saturating_mul(std::mem::size_of::<f32>())
                .saturating_mul(3)
                .saturating_add(std::mem::size_of::<f32>()),
            max_row_block_rows: 32_768,
            max_worker_count: 4,
            requirements: PlaneStateRequirements::dirty_standard(),
            prepared_residency: PreparedVisibilityResidency {
                sample_lanes_per_source_channel: corr_count,
                bucket_sample_bytes: 32,
                max_live_row_blocks: 1,
            },
        }
    }

    #[test]
    fn memory_planner_fits_minimum_planes_and_logs_breakdown() {
        let visibility = test_visibility_shape(10_000, 10, 4, 8, false, test_slab_shapes(10, 1));
        let plan =
            plan_spectral_memory(planner_input(10, [512, 512], visibility, 512 * 1024 * 1024))
                .unwrap();
        assert!(plan.active_planes >= 2);
        assert!(plan.active_planes >= 4);
        assert_eq!(plan.slab_count, 10usize.div_ceil(plan.active_planes));
        assert_eq!(plan.slab_manifest.len(), plan.slab_count);
        assert_eq!(plan.cache_kind, CachePolicy::GeometryOnly);
        assert!(plan.log_line().starts_with("spectral_slab_plan "));
        assert!(plan.log_line().contains("source_stream_buffer_bytes="));
        assert!(plan.log_line().contains("component_memory_bytes="));
        assert!(plan.log_line().contains("modeled_source_read_bytes="));
        assert!(plan.log_line().contains("schedule="));
        assert!(plan.log_line().contains("best_modeled_schedule="));
        assert!(plan.log_line().contains("candidate_io_costs="));
        assert!(plan.log_line().contains("modeled_total_io_bytes="));
        assert!(plan.log_line().contains("source_cell_channel_count="));
        assert!(plan.log_line().contains("data_channel_read_granularity="));
        assert!(
            plan.log_line()
                .contains("visibility_row_fixed_resident_bytes=")
        );
        assert!(
            plan.log_line()
                .contains("visibility_row_cache_overhead_bytes=")
        );
        assert!(
            plan.log_line()
                .contains("visibility_resident_cache_layout=")
        );
        assert!(
            plan.visibility_resident_cache_layout
                .contains("spectral_route")
        );
    }

    #[test]
    fn memory_planner_prefers_source_first_when_output_state_fits() {
        let visibility =
            test_visibility_shape(10_000, 8, 4, 8, false, reread_all_source_slab_shapes(8, 8));
        let plan =
            plan_spectral_memory(planner_input(8, [128, 128], visibility, 512 * 1024 * 1024))
                .unwrap();
        assert_eq!(plan.schedule_kind, ImagingScheduleKind::SourceFirst);
        assert_eq!(plan.active_planes, 8);
        assert_eq!(plan.modeled_output_spill_io_bytes, 0);
        assert_eq!(
            plan.modeled_source_read_bytes,
            plan.modeled_full_cache_source_read_bytes
        );
        assert_eq!(
            plan.best_modeled_schedule_kind,
            ImagingScheduleKind::SourceFirst
        );
    }

    #[test]
    fn memory_planner_uses_worker_capacity_for_resident_planes() {
        let visibility = test_visibility_shape(
            10_000,
            10,
            4,
            8,
            false,
            reread_all_source_slab_shapes(10, 10),
        );
        let mut input = planner_input(10, [128, 128], visibility, 512 * 1024 * 1024);
        input.max_worker_count = 16;

        let plan = plan_spectral_memory(input).unwrap();

        assert_eq!(plan.active_planes, 10);
        assert_eq!(plan.worker_count, 10);
    }

    #[test]
    fn memory_planner_caps_plane_workers_to_memory_resident_planes() {
        let visibility =
            test_visibility_shape(64, 8, 1, 8, false, reread_all_source_slab_shapes(8, 8));
        let mut input = planner_input(8, [512, 512], visibility, 10 * 1024 * 1024);
        input.max_worker_count = 16;

        let plan = plan_spectral_memory(input).unwrap();

        assert!(plan.active_planes < 8);
        assert_eq!(plan.worker_count, plan.active_planes);
    }

    #[test]
    fn source_read_cost_uses_source_cell_count_for_full_cell_storage() {
        let mut visibility =
            test_visibility_shape(32, 8, 2, 8, false, reread_all_source_slab_shapes(4, 8));
        visibility.source_cell_channel_count = 64;
        visibility.data_channel_read_granularity = VisibilityChannelReadGranularity::FullCell;
        visibility.flag_channel_read_granularity = VisibilityChannelReadGranularity::FullCell;
        visibility.weight_spectrum_channel_read_granularity =
            Some(VisibilityChannelReadGranularity::FullCell);
        let shape = VisibilitySlabShape {
            active_planes: 2,
            slab_count: 3,
            source_channel_visits: 12,
            max_slab_source_channels: 4,
        };
        let expected = visibility.active_rows.saturating_mul(
            shape
                .slab_count
                .saturating_mul(visibility.source_cell_channel_count)
                .saturating_mul(visibility.row_channel_bytes())
                .saturating_add(
                    shape
                        .slab_count
                        .saturating_mul(visibility.row_fixed_physical_read_bytes()),
                ),
        );
        assert_eq!(visibility.no_cache_source_read_bytes(shape), expected);
        assert!(
            visibility.no_cache_source_read_bytes(shape)
                > visibility.active_rows.saturating_mul(
                    shape
                        .source_channel_visits
                        .saturating_mul(visibility.row_channel_bytes())
                        .saturating_add(
                            shape
                                .slab_count
                                .saturating_mul(visibility.row_fixed_physical_read_bytes())
                        ),
                )
        );
    }

    #[test]
    fn source_read_cost_excludes_decoded_cache_row_overhead() {
        let mut visibility =
            test_visibility_shape(32, 8, 2, 8, true, reread_all_source_slab_shapes(4, 8));
        visibility.resident_layout.spectral_route_bytes = 8 * 1024;
        let shape = VisibilitySlabShape {
            active_planes: 2,
            slab_count: 3,
            source_channel_visits: 12,
            max_slab_source_channels: 4,
        };
        let physical_source_bytes = visibility.no_cache_source_read_bytes(shape);
        let resident_cache_bytes = visibility.full_source_cache_bytes();

        assert_eq!(
            visibility.row_fixed_resident_bytes(),
            visibility.resident_layout.bytes_per_row()
        );
        assert_eq!(
            physical_source_bytes,
            visibility.active_rows.saturating_mul(
                shape
                    .source_channel_visits
                    .saturating_mul(visibility.row_channel_bytes())
                    .saturating_add(
                        shape
                            .slab_count
                            .saturating_mul(visibility.row_fixed_physical_read_bytes())
                    )
            )
        );
        assert!(resident_cache_bytes > visibility.full_source_read_bytes());
        assert_eq!(
            visibility.full_source_read_bytes(),
            visibility.active_rows.saturating_mul(
                visibility
                    .full_source_channel_count
                    .saturating_mul(visibility.row_channel_bytes())
                    .saturating_add(visibility.row_fixed_physical_read_bytes())
            )
        );
    }

    #[test]
    fn memory_planner_uses_slab_first_only_when_spill_exceeds_rereads() {
        let visibility =
            test_visibility_shape(64, 8, 1, 8, false, reread_all_source_slab_shapes(8, 8));
        let plan = plan_spectral_memory(planner_input(8, [512, 512], visibility, 10 * 1024 * 1024))
            .unwrap();
        assert_eq!(plan.schedule_kind, ImagingScheduleKind::SlabFirst);
        assert!(plan.active_planes < 8);
        assert_eq!(plan.modeled_output_spill_io_bytes, 0);
        assert!(
            plan.modeled_no_cache_source_read_bytes > plan.modeled_full_cache_source_read_bytes
        );
        assert_eq!(
            plan.best_modeled_schedule_kind,
            ImagingScheduleKind::SlabFirst
        );
    }

    #[test]
    fn memory_planner_selects_executable_schedule_without_hiding_best_modeled_schedule() {
        let visibility = test_visibility_shape(
            200_000,
            64,
            4,
            8,
            false,
            vec![VisibilitySlabShape {
                active_planes: 4,
                slab_count: 2,
                source_channel_visits: 128,
                max_slab_source_channels: 64,
            }],
        );
        let mut input = planner_input(8, [128, 128], visibility, 5 * 1024 * 1024 * 1024);
        input.executor_capabilities =
            SpectralExecutorCapabilities::slab_runner_without_output_spill_or_full_source_cache();

        let plan = plan_spectral_memory(input).unwrap();

        assert_eq!(
            plan.best_modeled_schedule_kind,
            ImagingScheduleKind::SourceFirst
        );
        assert_eq!(plan.schedule_kind, ImagingScheduleKind::SlabFirst);
        assert!(plan.best_modeled_output_spill_io_bytes > 0);
        assert_eq!(plan.modeled_output_spill_io_bytes, 0);
        assert!(
            plan.warnings.iter().any(|warning| warning
                .contains("best_modeled_schedule_not_executable_by_executor"))
        );
        assert!(plan.candidate_io_costs.contains("source_first:"));
        assert!(plan.candidate_io_costs.contains("executable=false"));
        assert!(
            plan.log_line()
                .contains("executor_capabilities=slab_first_only")
        );
    }

    #[test]
    fn memory_planner_does_not_cap_large_image_slabs_by_image_size_alone() {
        let visibility = test_visibility_shape(10_000, 512, 4, 8, false, test_slab_shapes(512, 1));
        let plan = plan_spectral_memory(planner_input(
            512,
            [2048, 2048],
            visibility,
            16 * 1024 * 1024 * 1024,
        ))
        .unwrap();
        assert!(plan.active_planes > 2);
        assert!(
            !plan
                .warnings
                .iter()
                .any(|warning| warning.contains("large_image_active_plane_cap"))
        );
    }

    #[test]
    fn memory_planner_balances_workers_scratch_and_row_blocks_for_large_dirty_cube() {
        let visibility =
            test_visibility_shape(866_313, 1024, 2, 8, false, test_slab_shapes(1024, 1));
        let mut input = planner_input(1024, [4096, 4096], visibility, 16 * 1024 * 1024 * 1024);
        input.executor_capabilities =
            SpectralExecutorCapabilities::slab_runner_without_output_spill_or_full_source_cache();
        input.fixed_frontend_bytes = 60_445_282;
        input.executor_scratch = ExecutorScratchShape {
            fixed_bytes: 450_080_580,
            per_active_plane_bytes: 0,
            per_slab_source_channel_bytes: 0,
            per_worker_bytes: 1_130_801_990,
            per_worker_row_block_bytes: 64,
            per_worker_row_block_limit_bytes: 0,
            per_product_batch_plane_bytes: 0,
            per_product_pending_plane_bytes: 0,
        };
        input.requirements =
            PlaneStateRequirements::dirty_standard().with_streaming_plane_results();
        input.source_buffer_residency = SourceBufferResidency::FullSlabRawSource;
        input.max_row_block_rows = 866_313;
        input.max_worker_count = 10;
        input.prepared_residency = PreparedVisibilityResidency {
            sample_lanes_per_source_channel: 2,
            bucket_sample_bytes: 32,
            max_live_row_blocks: 1,
        };

        let max_worker_count = input.max_worker_count;
        let plan = plan_spectral_memory(input).unwrap();

        assert!(plan.active_planes >= 45);
        assert!(plan.slab_count <= 23);
        assert!(plan.row_block_rows > 20_000);
        assert!(plan.worker_count >= 3);
        assert!(plan.worker_count <= max_worker_count);
        assert_eq!(plan.schedule_kind, ImagingScheduleKind::SlabFirst);
        assert_eq!(
            plan.plane_state_residency,
            PlaneStateResidency::StreamingPlaneResults
        );
        assert_eq!(
            plan.source_buffer_residency,
            SourceBufferResidency::FullSlabRawSource
        );
        assert!(plan.planned_active_bytes <= plan.memory_target_bytes);
    }

    #[test]
    fn memory_planner_keeps_source_locality_cap_for_one_slab_dirty_cube() {
        let mut visibility =
            test_visibility_shape(3_086_235, 64, 2, 8, false, test_slab_shapes(64, 64));
        visibility.weight_spectrum_element_bytes = None;
        visibility.weight_spectrum_channel_read_granularity = None;
        let mut input = planner_input(64, [2048, 2048], visibility, 28 * 1024 * 1024 * 1024);
        input.executor_capabilities =
            SpectralExecutorCapabilities::slab_runner_without_output_spill_or_full_source_cache();
        input.fixed_frontend_bytes = 233_378_040;
        input.executor_scratch = ExecutorScratchShape {
            fixed_bytes: 651_443_456,
            per_active_plane_bytes: 0,
            per_slab_source_channel_bytes: 0,
            per_worker_bytes: 243_526_803,
            per_worker_row_block_bytes: 64,
            per_worker_row_block_limit_bytes: 0,
            per_product_batch_plane_bytes: 0,
            per_product_pending_plane_bytes: 0,
        };
        input.requirements =
            PlaneStateRequirements::dirty_standard().with_streaming_plane_results();
        input.source_buffer_residency = SourceBufferResidency::FullSlabRawSource;
        input.max_row_block_rows = 3_086_235;
        input.max_worker_count = 10;

        let plan = plan_spectral_memory(input).unwrap();

        assert_eq!(plan.active_planes, 64);
        assert_eq!(plan.slab_count, 1);
        assert_eq!(plan.worker_count, 10);
        assert_eq!(plan.row_block_rows, 617_247);
        assert_eq!(plan.schedule_kind, ImagingScheduleKind::SourceFirst);
        assert_eq!(
            plan.plane_state_residency,
            PlaneStateResidency::StreamingPlaneResults
        );
        assert!(plan.planned_active_bytes <= plan.memory_target_bytes);
    }

    #[test]
    fn memory_planner_allows_medium_dirty_cube_full_active_group_when_it_fits() {
        let mut visibility =
            test_visibility_shape(3_086_235, 64, 2, 8, false, test_slab_shapes(64, 1));
        visibility.weight_spectrum_element_bytes = None;
        visibility.weight_spectrum_channel_read_granularity = None;
        let mut input = planner_input(64, [2048, 2048], visibility, 28 * 1024 * 1024 * 1024);
        input.executor_capabilities =
            SpectralExecutorCapabilities::slab_runner_without_output_spill_or_full_source_cache();
        input.fixed_frontend_bytes = 233_378_040;
        input.executor_scratch = ExecutorScratchShape {
            fixed_bytes: 651_443_456,
            per_active_plane_bytes: 0,
            per_slab_source_channel_bytes: 0,
            per_worker_bytes: 243_526_803,
            per_worker_row_block_bytes: 64,
            per_worker_row_block_limit_bytes: 50_331_648,
            per_product_batch_plane_bytes: 0,
            per_product_pending_plane_bytes: 0,
        };
        input.requirements = PlaneStateRequirements::dirty_standard();
        input.source_buffer_residency = SourceBufferResidency::FullSlabRawSource;
        input.max_row_block_rows = 3_086_235;
        input.max_worker_count = 10;

        let plan = plan_spectral_memory(input).unwrap();

        assert_eq!(plan.active_planes, 64);
        assert_eq!(plan.slab_count, 1);
        assert_eq!(plan.worker_count, 10);
        assert_eq!(
            plan.plane_state_residency,
            PlaneStateResidency::FullActiveGroup
        );
        assert!(plan.resident_plane_state_bytes > 0);
        assert!(plan.planned_active_bytes <= plan.memory_target_bytes);
    }

    #[test]
    fn memory_planner_uses_bounded_row_block_visibility_residency() {
        let visibility = test_visibility_shape(
            3_086_235,
            64,
            4,
            8,
            false,
            reread_all_source_slab_shapes(128, 64),
        );
        let plan = plan_spectral_memory(planner_input(
            128,
            [1024, 1024],
            visibility,
            16 * 1024 * 1024 * 1024,
        ))
        .unwrap();

        assert_eq!(
            plan.prepared_residency,
            PreparedVisibilityResidency {
                sample_lanes_per_source_channel: 4,
                bucket_sample_bytes: 32,
                max_live_row_blocks: 1,
            }
        );
        assert_eq!(plan.prepared_visibility_staging_bytes, 0);
        assert_eq!(plan.visibility_staging_bytes_per_plane, 0);
        assert!(plan.source_stream_buffer_bytes > 0);
        assert!(plan.live_prepared_visibility_bytes > 0);
        assert!(plan.live_bucket_bytes > 0);
        assert!(plan.row_block_rows < 3_086_235);
        assert!(plan.row_block_rows > 1_000);
        assert!(plan.planned_active_bytes <= plan.memory_target_bytes);
        assert!(
            plan.log_line()
                .contains("prepared_residency=row_block_stream")
        );
    }

    #[test]
    fn memory_planner_keeps_medium_cube_row_blocks_after_source_residency_accounting() {
        let mut visibility =
            test_visibility_shape(3_086_235, 512, 2, 8, false, test_slab_shapes(512, 1));
        visibility.weight_spectrum_element_bytes = None;
        visibility.weight_spectrum_channel_read_granularity = None;
        let mut input = planner_input(512, [2048, 2048], visibility, 16 * 1024 * 1024 * 1024);
        input.executor_capabilities =
            SpectralExecutorCapabilities::slab_runner_without_output_spill_or_full_source_cache();
        input.fixed_frontend_bytes = 233_378_040;
        input.executor_scratch = ExecutorScratchShape {
            fixed_bytes: 651_443_456,
            per_active_plane_bytes: 0,
            per_slab_source_channel_bytes: 0,
            per_worker_bytes: 243_526_803,
            per_worker_row_block_bytes: 64,
            per_worker_row_block_limit_bytes: 50_331_648,
            per_product_batch_plane_bytes: 0,
            per_product_pending_plane_bytes: 0,
        };
        input.requirements =
            PlaneStateRequirements::dirty_standard().with_streaming_plane_results();
        input.source_buffer_residency = SourceBufferResidency::FullSlabRawSource;
        input.max_row_block_rows = 3_086_235;
        input.max_worker_count = 10;

        let plan = plan_spectral_memory(input).unwrap();

        assert!(plan.active_planes >= 100);
        assert!(plan.slab_count <= 5);
        assert!(plan.row_block_rows >= 40_000);
        assert!(plan.row_block_rows <= 900_000);
        assert!(plan.worker_count >= 5);
        assert_eq!(plan.prepared_visibility_staging_bytes, 0);
        assert!(plan.source_stream_buffer_bytes > 0);
        assert!(plan.planned_active_bytes <= plan.memory_target_bytes);
    }

    #[test]
    fn memory_planner_keeps_large_dirty_cube_product_writes_plane_local() {
        let mut visibility =
            test_visibility_shape(866_313, 1024, 2, 8, false, test_slab_shapes(1024, 347));
        visibility.weight_spectrum_element_bytes = None;
        visibility.weight_spectrum_channel_read_granularity = None;
        let mut input = planner_input(1024, [4096, 4096], visibility, 30_064_771_072);
        let image_pixels = 4096usize * 4096usize;
        let one_plane_run_result_bytes = image_pixels * std::mem::size_of::<f32>() * 5;
        let standard_mfs_workspace_bytes = 4_914usize * 4_914usize * 16 * 2;
        let one_plane_write_clone_bytes = image_pixels * std::mem::size_of::<f32>() * 2;
        input.executor_capabilities =
            SpectralExecutorCapabilities::slab_runner_without_output_spill_or_full_source_cache();
        input.fixed_frontend_bytes = 60_445_282;
        input.executor_scratch = ExecutorScratchShape {
            fixed_bytes: 554_133_144,
            per_active_plane_bytes: 0,
            per_slab_source_channel_bytes: 0,
            per_worker_bytes: one_plane_run_result_bytes + standard_mfs_workspace_bytes,
            per_worker_row_block_bytes: 64,
            per_worker_row_block_limit_bytes: 0,
            per_product_batch_plane_bytes: one_plane_write_clone_bytes,
            per_product_pending_plane_bytes: one_plane_run_result_bytes,
        };
        input.requirements =
            PlaneStateRequirements::dirty_standard().with_streaming_plane_results();
        input.source_buffer_residency = SourceBufferResidency::FullSlabRawSource;
        input.product_write_bytes_per_plane = one_plane_write_clone_bytes + 4;
        input.max_row_block_rows = 866_313;
        input.max_worker_count = 10;

        let plan = plan_spectral_memory(input).unwrap();

        assert_eq!(plan.product_batch_planes, 1, "{}", plan.log_line());
        assert!(plan.worker_count >= 8, "{}", plan.log_line());
        assert!(plan.planned_active_bytes <= plan.memory_target_bytes);
    }

    #[test]
    fn candidate_tie_break_prefers_fewer_source_blocks() {
        let base_shape = VisibilitySlabShape {
            active_planes: 47,
            slab_count: 2,
            source_channel_visits: 64,
            max_slab_source_channels: 47,
        };
        let residency = PreparedVisibilityResidency {
            sample_lanes_per_source_channel: 2,
            bucket_sample_bytes: 32,
            max_live_row_blocks: 1,
        };
        let base_candidate = CandidatePlan {
            schedule_kind: ImagingScheduleKind::SlabFirst,
            shape: base_shape,
            worker_count: 4,
            prepared_residency: residency,
            visibility_cache_policy: VisibilityCachePolicy::Disabled,
            visibility_cache_bytes: 0,
            visibility_cache_source_channels: 0,
            source_stream_buffer_bytes: 1_422_123_402,
            row_block_rows: 168_518,
            prepared_visibility_staging_bytes: 9_283_394_880,
            visibility_staging_bytes_per_plane: 197_519_040,
            live_prepared_visibility_bytes: 1_013_804_288,
            live_bucket_bytes: 253_451_072,
            product_scratch_bytes: 256,
            product_batch_planes: 1,
            resident_plane_state_bytes: 9_283_394_880,
            planned_active_bytes: 17_179_862_010,
            modeled_total_io_bytes: 8_146_785_696,
            modeled_source_read_bytes: 3_851_621_280,
            modeled_visibility_cache_fill_bytes: 0,
            modeled_visibility_cache_read_bytes: 0,
            modeled_output_spill_read_bytes: 0,
            modeled_output_spill_write_bytes: 0,
            modeled_product_write_bytes: 4_295_164_416,
            modeled_no_cache_source_read_bytes: 3_851_621_280,
            modeled_product_write_groups: 2,
            modeled_runtime_cost_bytes: 17_179_862_010,
        };
        let mut best = Some(base_candidate);

        consider_candidate(
            &mut best,
            CandidatePlan {
                shape: VisibilitySlabShape {
                    active_planes: 32,
                    max_slab_source_channels: 32,
                    ..base_shape
                },
                source_stream_buffer_bytes: 5_391_592_020,
                row_block_rows: 934_580,
                prepared_visibility_staging_bytes: 6_320_609_280,
                visibility_staging_bytes_per_plane: 197_519_040,
                live_prepared_visibility_bytes: 3_828_039_680,
                live_bucket_bytes: 957_009_920,
                planned_active_bytes: 17_179_865_868,
                ..base_candidate
            },
        );

        let selected = best.expect("candidate selected");
        assert_eq!(selected.shape.active_planes, 32);
        assert_eq!(selected.row_block_rows, 934_580);
    }

    #[test]
    fn memory_planner_costs_full_source_visibility_cache_as_hybrid_candidate() {
        let visibility = test_visibility_shape(
            20_000,
            64,
            4,
            8,
            true,
            reread_all_source_slab_shapes(64, 64),
        );
        let plan = plan_spectral_memory(planner_input(
            64,
            [2048, 2048],
            visibility,
            2 * 1024 * 1024 * 1024,
        ))
        .unwrap();
        assert!(plan.candidate_io_costs.contains("hybrid:"));
        assert!(plan.candidate_io_costs.contains("cache_policy=full_source"));
        assert_eq!(plan.schedule_kind, plan.best_modeled_schedule_kind);
    }

    #[test]
    fn memory_planner_disables_full_source_cache_when_it_does_not_fit() {
        let visibility = test_visibility_shape(
            1_000_000,
            64,
            4,
            16,
            true,
            overlapping_channel_slab_shapes(64, 64),
        );
        let plan = plan_spectral_memory(planner_input(
            64,
            [1024, 1024],
            visibility,
            1024 * 1024 * 1024,
        ))
        .unwrap();
        assert_eq!(
            plan.visibility_cache_policy,
            VisibilityCachePolicy::Disabled
        );
        assert!(
            plan.warnings
                .iter()
                .any(|warning| warning == "full_source_visibility_cache_does_not_fit")
        );
    }

    #[test]
    fn memory_planner_uses_actual_visibility_data_element_widths() {
        let shapes = reread_all_source_slab_shapes(8, 128);
        let complex32 = test_visibility_shape(10_000, 128, 4, 8, true, shapes.clone());
        let complex64 = test_visibility_shape(10_000, 128, 4, 16, true, shapes);
        let plan32 = plan_spectral_memory(planner_input(
            8,
            [256, 256],
            complex32,
            4 * 1024 * 1024 * 1024,
        ))
        .unwrap();
        let plan64 = plan_spectral_memory(planner_input(
            8,
            [256, 256],
            complex64,
            4 * 1024 * 1024 * 1024,
        ))
        .unwrap();
        assert_eq!(plan32.visibility_data_element_bytes, 8);
        assert_eq!(plan64.visibility_data_element_bytes, 16);
        assert!(
            plan64.modeled_full_cache_source_read_bytes
                > plan32.modeled_full_cache_source_read_bytes
        );
    }

    #[test]
    fn memory_planner_rejects_too_small_memory_target() {
        let mut input = planner_input(
            4,
            [512, 512],
            test_visibility_shape(100, 4, 4, 8, false, test_slab_shapes(4, 1)),
            1024,
        );
        input.requirements = PlaneStateRequirements::mosaic_pb_aware();
        let error = plan_spectral_memory(input).unwrap_err();
        assert!(error.contains("cannot fit any source/output schedule candidate"));
    }
}
