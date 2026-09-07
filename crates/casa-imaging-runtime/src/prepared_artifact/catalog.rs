// SPDX-License-Identifier: LGPL-3.0-or-later

//! Ordered preparation with one admitted workspace and per-object transactions.

use super::*;

pub(super) fn validate_catalog_sources(
    descriptors: &[PreparedArtifactDescriptor],
    sources: &[Option<PreparedArtifactImportSource>],
) -> Result<(), PreparedArtifactError> {
    if descriptors.len() != sources.len() {
        return Err(PreparedArtifactError::SegmentMismatch);
    }
    for (descriptor, source) in descriptors.iter().zip(sources) {
        let Some(source) = source else { continue };
        validate_import_segments(descriptor, &source.segments)?;
        if derive_import_source_identity(descriptor, &source.segments)? != source.identity {
            return Err(PreparedArtifactError::SourceIdentityMismatch);
        }
    }
    Ok(())
}

fn source_lane(segment: &PreparedArtifactImportSegment) -> String {
    let root = encode_hex(&segment.storage_root_identity);
    let base = format!(
        "private-prepared-import-lane-{}-{root}",
        segment.storage_domain.as_str()
    );
    match &segment.storage_operations_rate {
        Some(rate) => format!("{base}-operations-{}", rate.as_str()),
        None => base,
    }
}

pub(super) fn catalog_source_demands(
    sources: &[Option<PreparedArtifactImportSource>],
) -> BTreeMap<String, StorageDomainId> {
    sources
        .iter()
        .flatten()
        .flat_map(|source| &source.segments)
        .map(|segment| (source_lane(segment), segment.storage_domain.clone()))
        .collect()
}

pub(super) fn catalog_metadata_reservation(
    descriptors: &[PreparedArtifactDescriptor],
    sources: Option<&[Option<PreparedArtifactImportSource>]>,
) -> Result<u64, PreparedArtifactError> {
    // Each retained descriptor fits the existing manifest-decoding envelope.
    // Handles and outcomes retain no payload, and every source path is bounded
    // by the existing source-descriptor policy.
    let descriptors_bytes = u64::try_from(descriptors.len())
        .ok()
        .and_then(|entries| entries.checked_mul(MANIFEST_RESIDENT_BYTES))
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    let mut bytes = descriptors_bytes
        .checked_add(catalog_outcome_resident_bytes(descriptors.len())?)
        .and_then(|bytes| {
            bytes.checked_add(
                fixed_vec_resident_reservation::<PreparedArtifact>(descriptors.len()).ok()?,
            )
        })
        .and_then(|bytes| {
            bytes.checked_add(
                fixed_vec_resident_reservation::<ArtifactMeasurement>(
                    descriptors.len().checked_mul(2)?,
                )
                .ok()?,
            )
        })
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    if let Some(sources) = sources {
        bytes = bytes
            .checked_add(fixed_vec_resident_reservation::<
                Option<PreparedArtifactImportSource>,
            >(sources.len())?)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    }
    for source in sources.into_iter().flatten().flatten() {
        bytes = bytes
            .checked_add(source_descriptor_reservation(source.segments.len())?)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    }
    let artifacts = catalog_planned_artifacts(descriptors, sources)?;
    let artifacts = artifacts.into_iter().chain(
        sources
            .into_iter()
            .flatten()
            .flatten()
            .map(|source| source.planned_artifact()),
    );
    bytes
        .checked_add(
            crate::receipt::artifact_workspace_bytes(artifacts)
                .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?,
        )
        .ok_or(PreparedArtifactError::ArtifactTooLarge)
}

pub(super) fn catalog_io_operation_prediction(
    store: &PreparedArtifactStore,
    descriptors: &[PreparedArtifactDescriptor],
    sources: Option<&[Option<PreparedArtifactImportSource>]>,
) -> Result<u64, PreparedArtifactError> {
    // A traffic estimate, not an admission ceiling: root inventory entries,
    // payload transfers through the configured buffer, and declared source I/O.
    let mut operations =
        u64::try_from(store.budget.entries).map_err(|_| PreparedArtifactError::ArtifactTooLarge)?;
    for (index, descriptor) in descriptors.iter().enumerate() {
        let buffer = streaming_buffer_len(store.budget, &descriptor.compatibility)? as u64;
        for segment in &descriptor.compatibility.segments {
            let transfers = segment.byte_len()?.div_ceil(buffer);
            operations = operations
                .checked_add(transfers)
                .and_then(|count| {
                    count.checked_add(
                        u64::from(sources.is_some_and(|sources| sources[index].is_some()))
                            * transfers,
                    )
                })
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        }
    }
    for source in sources.into_iter().flatten().flatten() {
        operations = operations
            .checked_add(source.source_operations()?)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    }
    Ok(operations)
}

impl PreparedArtifactStore {
    pub(super) fn catalog_reservation(
        &self,
        descriptors: &[PreparedArtifactDescriptor],
        sources: Option<&[Option<PreparedArtifactImportSource>]>,
    ) -> Result<PreparedArtifactReservation, PreparedArtifactError> {
        validate_catalog_descriptors(self, descriptors)?;
        if let Some(sources) = sources {
            validate_catalog_sources(descriptors, sources)?;
        }
        let mut reservation =
            self.reservation(&descriptors[0], PreparedArtifactOperation::Reuse)?;
        let mut required_cache = 0_u64;
        for (index, descriptor) in descriptors.iter().enumerate() {
            let operation = if sources.is_some_and(|sources| sources[index].is_some()) {
                PreparedArtifactOperation::Load
            } else {
                PreparedArtifactOperation::Reuse
            };
            let entry = self.reservation(descriptor, operation)?;
            required_cache = required_cache
                .checked_add(entry.entry_bytes)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
            reservation.entry_bytes = reservation.entry_bytes.max(entry.entry_bytes);
            reservation.temporary_staging_bytes = reservation
                .temporary_staging_bytes
                .max(entry.temporary_staging_bytes);
            reservation.streaming_buffer_bytes = reservation
                .streaming_buffer_bytes
                .max(entry.streaming_buffer_bytes);
            reservation.resident_buffer_bytes = reservation
                .resident_buffer_bytes
                .max(entry.resident_buffer_bytes);
            reservation.file_descriptors = reservation.file_descriptors.max(entry.file_descriptors);
        }
        if required_cache > self.budget.cache_bytes {
            return Err(PreparedArtifactError::CacheBudgetExceeded {
                required: required_cache,
                budget: self.budget.cache_bytes,
            });
        }
        if descriptors.len() > self.budget.entries {
            return Err(PreparedArtifactError::CacheEntryBudgetExceeded {
                required: descriptors.len(),
                budget: self.budget.entries,
            });
        }
        reservation.source_read_bytes =
            sources
                .into_iter()
                .flatten()
                .flatten()
                .try_fold(0_u64, |bytes, source| {
                    bytes
                        .checked_add(source.source_read_bytes())
                        .ok_or(PreparedArtifactError::ArtifactTooLarge)
                })?;
        reservation.resident_buffer_bytes = reservation
            .resident_buffer_bytes
            .checked_add(catalog_metadata_reservation(descriptors, sources)?)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        // Reuse also owns bounded reconciliation of abandoned private staging.
        // Those old files may occupy the cache policy's full byte envelope.
        if sources.is_none() {
            reservation.temporary_staging_bytes = self.budget.cache_bytes;
        }
        Ok(reservation)
    }

    /// Prepare an ordered catalog through one plan-selected cache phase.
    ///
    /// A missing source selects revalidation/reuse, never an implicit import.
    /// The factory is called only for a present source, after its binding is
    /// revalidated, and its importer is dropped before advancing. Each object
    /// uses the ordinary independently durable private-store transaction and
    /// lock. A later failure retains the completed prefix in its measurements;
    /// it does not roll back earlier publications or claim catalog completion.
    /// Cooperative stop requests are checked before each cell. This phase's
    /// evictions exclude every selected catalog member; this is not a pin
    /// against another execution between the per-cell lock acquisitions.
    pub fn import_catalog<I: PreparedArtifactImporter>(
        &self,
        context: &WorkExecutionContext<'_>,
        descriptors: &[PreparedArtifactDescriptor],
        sources: &[Option<PreparedArtifactImportSource>],
        mut importer: impl FnMut(usize) -> Result<I, PreparedArtifactError>,
    ) -> Result<(Vec<PreparedArtifact>, WorkMeasurements), PreparedArtifactError> {
        let reservation = self.catalog_reservation(descriptors, Some(sources))?;
        validate_catalog_plan_binding(*context, self, descriptors, Some(sources), reservation)?;
        let metadata_bytes = catalog_metadata_reservation(descriptors, Some(sources))?;
        let mut aggregate = CatalogMeasurements::new(*context, descriptors.len());
        let mut artifacts = Vec::with_capacity(descriptors.len());
        for (index, (descriptor, source)) in descriptors.iter().zip(sources).enumerate() {
            let operation = if source.is_some() {
                PreparedArtifactOperation::Load
            } else {
                PreparedArtifactOperation::Reuse
            };
            if context.stop_requested() {
                return Err(
                    PreparedArtifactError::Interrupted.with_measurements(aggregate.finish())
                );
            }
            let mut evidence =
                ValidationEvidence::for_operation(self.budget, reservation.resident_buffer_bytes);
            evidence.acquire_resident(metadata_bytes);
            if let Some(source) = source {
                evidence.observe_source_inputs(PreparedArtifactSourceBinding::Import(source));
            }
            let published = (|| {
                evidence.ensure_resident_budget()?;
                let mut current = if let Some(source) = source {
                    validate_source_binding(
                        *context,
                        descriptor,
                        PreparedArtifactSourceBinding::Import(source),
                    )?;
                    Some(importer(index)?)
                } else {
                    None
                };
                self.settle_transaction(
                    descriptor,
                    if source.is_some() {
                        ArtifactDisposition::Loaded
                    } else {
                        ArtifactDisposition::Reused
                    },
                    source
                        .as_ref()
                        .zip(current.as_mut())
                        .map(
                            |(source, importer)| PreparedArtifactMaterialization::Import {
                                source,
                                importer,
                            },
                        ),
                    self.reservation(descriptor, operation)?,
                    descriptors,
                    &mut evidence,
                )
            })();
            // Source identity remains the authorization key. Only the resource
            // counter is projected onto the admitted serial storage lane.
            for counter in &mut evidence.source_reads {
                let source = source.as_ref().expect("only imports have source counters");
                let segment = source
                    .segments
                    .iter()
                    .find(|segment| segment.storage_demand_id(source.identity) == counter.demand_id)
                    .expect("counter belongs to the validated source");
                counter.demand_id = source_lane(segment);
            }
            match published {
                Ok((validated, disposition, cache_bytes)) => {
                    let observed = measurements(
                        *context,
                        descriptor,
                        disposition,
                        &validated,
                        MeasurementInput {
                            operation,
                            cache_bytes,
                            evidence,
                        },
                    );
                    aggregate.record(observed)?;
                    artifacts.push(validated.into_handle(descriptor));
                }
                Err(error) => {
                    aggregate.record(failed_measurements(
                        *context, descriptor, operation, &evidence,
                    ))?;
                    return Err(error.with_measurements(aggregate.finish()));
                }
            }
        }
        Ok((artifacts, aggregate.finish()))
    }
}

struct CatalogMeasurements {
    resources: Vec<ResourceMeasurement>,
    io: IoCounter,
    artifacts: Vec<ArtifactMeasurement>,
}

impl CatalogMeasurements {
    fn new(context: WorkExecutionContext<'_>, entries: usize) -> Self {
        Self {
            resources: context
                .resources()
                .iter()
                .map(|resource| {
                    ResourceMeasurement::new(
                        resource.resource().clone(),
                        resource.lifetime().clone(),
                        0,
                    )
                })
                .collect(),
            io: IoCounter::default(),
            artifacts: Vec::with_capacity(entries.saturating_mul(2)),
        }
    }

    fn record(&mut self, observed: WorkMeasurements) -> Result<(), PreparedArtifactError> {
        for (total, current) in self.resources.iter_mut().zip(observed.resources()) {
            *total = ResourceMeasurement::new(
                total.resource().clone(),
                total.lifetime().clone(),
                total.peak().max(current.peak()),
            );
        }
        for io in observed.io() {
            self.io.bytes = self
                .io
                .bytes
                .checked_add(io.bytes())
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
            self.io.operations = self
                .io
                .operations
                .checked_add(io.operations())
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        }
        self.artifacts.extend_from_slice(observed.artifacts());
        Ok(())
    }

    fn finish(self) -> WorkMeasurements {
        WorkMeasurements::new(
            self.resources,
            vec![IoMeasurement::new(
                IoBufferKind::StorageManager,
                self.io.bytes,
                self.io.operations,
            )],
            self.artifacts,
        )
    }
}
