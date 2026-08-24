// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl PreparedArtifactStore {
    /// Open an explicitly configured private casa-rs cache root.
    ///
    /// Canonicalization and CASA-boundary checks happen before any missing
    /// directory is created. Opening the store does not scan, validate, evict,
    /// or otherwise mutate cache entries.
    pub fn open(
        root: impl AsRef<Path>,
        budget: PreparedArtifactBudget,
    ) -> Result<Self, PreparedArtifactError> {
        Self::open_in_domain(root, StorageDomainId::new("atomic-output"), budget)
    }

    /// Open a private cache explicitly bound to one Resource Authority storage domain.
    pub fn open_in_domain(
        root: impl AsRef<Path>,
        storage_domain: StorageDomainId,
        budget: PreparedArtifactBudget,
    ) -> Result<Self, PreparedArtifactError> {
        if !valid_identifier(storage_domain.as_str()) {
            return Err(PreparedArtifactError::InvalidDescriptor);
        }
        let root = prepare_private_root(root.as_ref())?;
        reject_casa_cache_contents(&root)?;
        let cache = root.join(CACHE_DIRECTORY);
        ensure_private_child_directory(&root, &cache)?;
        let lock_path = root.join(LOCK_FILE);
        match lock_path.symlink_metadata() {
            Ok(metadata) if !metadata.file_type().is_file() => {
                return Err(PreparedArtifactError::UnknownCacheEntry(lock_path));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
            Ok(_) => {}
        }
        let state = root_state(&root)?;
        let scope = CacheScope::new(&root, &storage_domain, budget)?;
        Ok(Self {
            root,
            cache,
            lock_path,
            budget,
            scope,
            storage_domain,
            state,
            #[cfg(test)]
            fail_after_evictions: None,
            #[cfg(test)]
            fail_after_publication_rename: false,
        })
    }

    /// Return the explicit private root, which is never a CASA cache path.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Return the complete policy committed into owner-derived cache identities.
    #[must_use]
    pub const fn budget(&self) -> PreparedArtifactBudget {
        self.budget
    }

    /// Return the storage domain identity cryptographically bound to this canonical root.
    #[must_use]
    pub const fn storage_domain(&self) -> &StorageDomainId {
        &self.storage_domain
    }

    /// Derive exact resource/storage bounds for one explicit cache operation.
    pub fn reservation(
        &self,
        descriptor: &PreparedArtifactDescriptor,
        operation: PreparedArtifactOperation,
    ) -> Result<PreparedArtifactReservation, PreparedArtifactError> {
        if descriptor.cache_scope != self.scope {
            return Err(PreparedArtifactError::CachePolicyMismatch);
        }
        let payload_bytes = descriptor.payload_bytes()?;
        let entry_bytes = payload_bytes
            .checked_add(MANIFEST_RESERVATION_BYTES)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        if entry_bytes > self.budget.cache_bytes {
            return Err(PreparedArtifactError::CacheBudgetExceeded {
                required: entry_bytes,
                budget: self.budget.cache_bytes,
            });
        }
        let streaming_buffer_bytes = u64::try_from(streaming_buffer_len(self.budget, descriptor)?)
            .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?;
        let inventory_resident_bytes = inventory_resident_reservation(&self.cache, self.budget)?;
        let source_descriptor_bytes = if operation == PreparedArtifactOperation::Load {
            source_descriptor_reservation(descriptor.segments.len())?
        } else {
            0
        };
        let source_read_bytes = if operation == PreparedArtifactOperation::Load {
            payload_bytes
                .checked_add(
                    u64::try_from(descriptor.segments.len())
                        .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?,
                )
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?
        } else {
            0
        };
        let resident_buffer_bytes = streaming_buffer_bytes
            .checked_add(MANIFEST_RESIDENT_BYTES)
            .and_then(|bytes| bytes.checked_add(inventory_resident_bytes))
            .and_then(|bytes| bytes.checked_add(source_descriptor_bytes))
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        Ok(PreparedArtifactReservation {
            persistent_cache_bytes: self.budget.cache_bytes,
            entry_bytes,
            temporary_staging_bytes: if operation == PreparedArtifactOperation::Reuse {
                0
            } else {
                entry_bytes
            },
            source_read_bytes,
            file_descriptors: match operation {
                PreparedArtifactOperation::Load => 3,
                PreparedArtifactOperation::Generate | PreparedArtifactOperation::Reuse => 2,
            },
            source_descriptor_bytes,
            streaming_buffer_bytes,
            resident_buffer_bytes,
        })
    }

    /// Generate, validate, and atomically publish exact cold bytes.
    ///
    /// The returned identity exposes no payload access. The measurements cover
    /// the complete private-store operation through final validation.
    pub fn generate(
        &self,
        context: &WorkExecutionContext<'_>,
        descriptor: &PreparedArtifactDescriptor,
        generator: &mut dyn PreparedArtifactGenerator,
    ) -> Result<(PreparedArtifact, WorkMeasurements), PreparedArtifactError> {
        self.publish(
            context,
            descriptor,
            PreparedArtifactOperation::Generate,
            ArtifactDisposition::Built,
            PreparedArtifactMaterialization::Generate(generator),
        )
    }

    /// Load, validate, and atomically publish bytes from a separately validated source.
    ///
    /// This API conveys no CASA-cache provenance and never opens a CASA path.
    /// The returned identity and measurements have the same operation boundary
    /// as [`Self::generate`].
    pub fn load(
        &self,
        context: &WorkExecutionContext<'_>,
        descriptor: &PreparedArtifactDescriptor,
        source: &PreparedArtifactLoadSource,
    ) -> Result<(PreparedArtifact, WorkMeasurements), PreparedArtifactError> {
        self.publish(
            context,
            descriptor,
            PreparedArtifactOperation::Load,
            ArtifactDisposition::Loaded,
            PreparedArtifactMaterialization::Load(source),
        )
    }

    /// Revalidate and reuse the exact warm artifact selected by planning.
    ///
    /// A successful result exposes identity only; a rejection returns durable
    /// evidence and no payload access. The
    /// measurements include the lock, cache scan, metadata, manifest, payload,
    /// and integrity operations performed before the disposition is known.
    pub fn reuse(
        &self,
        context: &WorkExecutionContext<'_>,
        descriptor: &PreparedArtifactDescriptor,
    ) -> Result<(PreparedArtifactReuseOutcome, WorkMeasurements), PreparedArtifactError> {
        let reservation = self.reservation(descriptor, PreparedArtifactOperation::Reuse)?;
        validate_plan_binding(
            *context,
            descriptor,
            PreparedArtifactOperation::Reuse,
            reservation,
            None,
        )?;
        let mut evidence =
            ValidationEvidence::for_operation(self.budget, reservation.resident_buffer_bytes);
        if let Err(error) = evidence.ensure_resident_budget() {
            let measurements = failed_measurements(
                *context,
                descriptor,
                PreparedArtifactOperation::Reuse,
                &evidence,
            );
            return Err(error.with_measurements(measurements));
        }
        let mut lock = match self.lock(&mut evidence) {
            Ok(lock) => lock,
            Err(error) => {
                let measurements = failed_measurements(
                    *context,
                    descriptor,
                    PreparedArtifactOperation::Reuse,
                    &evidence,
                );
                return Err(error.with_measurements(measurements));
            }
        };
        let evaluation = self.reuse_locked(descriptor, &mut evidence);
        let unlock = lock.release(&mut evidence);
        let evaluation = match (evaluation, unlock) {
            (Ok(evaluation), Ok(())) => evaluation,
            (Err(error), _) | (Ok(_), Err(error)) => {
                let measurements = failed_measurements(
                    *context,
                    descriptor,
                    PreparedArtifactOperation::Reuse,
                    &evidence,
                );
                return Err(error.with_measurements(measurements));
            }
        };
        match evaluation {
            ReuseEvaluation::Rejected {
                rejection,
                path,
                cache_bytes,
            } => {
                let measurements = rejected_measurements(
                    *context,
                    descriptor,
                    rejection,
                    &path,
                    cache_bytes,
                    evidence,
                );
                Ok((
                    PreparedArtifactReuseOutcome::Rejected(rejection),
                    measurements,
                ))
            }
            ReuseEvaluation::Reused {
                validated,
                cache_bytes,
            } => {
                let measurements = measurements(
                    *context,
                    descriptor,
                    ArtifactDisposition::Reused,
                    &validated,
                    MeasurementInput {
                        operation: PreparedArtifactOperation::Reuse,
                        cache_bytes,
                        evidence,
                    },
                );
                Ok((
                    PreparedArtifactReuseOutcome::Reused(validated.into_handle(descriptor)),
                    measurements,
                ))
            }
        }
    }

    fn reuse_locked(
        &self,
        descriptor: &PreparedArtifactDescriptor,
        evidence: &mut ValidationEvidence,
    ) -> Result<ReuseEvaluation, PreparedArtifactError> {
        let cache_bytes = self.validate_raw_budget(descriptor.identity, evidence)?;
        let path = self.entry_path(descriptor.identity);
        evidence.store_read_operation();
        match path.symlink_metadata() {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Ok(ReuseEvaluation::Rejected {
                    rejection: PreparedArtifactRejection::Missing,
                    path,
                    cache_bytes,
                })
            }
            Err(error) => Err(error.into()),
            Ok(_) => match self.validate_entry_with_evidence(
                descriptor.identity,
                Some(descriptor),
                evidence,
            ) {
                Ok(validated) => Ok(ReuseEvaluation::Reused {
                    validated,
                    cache_bytes,
                }),
                Err(error) => {
                    let Some(rejection) = rejection_for(&error) else {
                        return Err(error);
                    };
                    Ok(ReuseEvaluation::Rejected {
                        rejection,
                        path,
                        cache_bytes,
                    })
                }
            },
        }
    }

    fn publish(
        &self,
        context: &WorkExecutionContext<'_>,
        descriptor: &PreparedArtifactDescriptor,
        operation: PreparedArtifactOperation,
        disposition: ArtifactDisposition,
        materialization: PreparedArtifactMaterialization<'_>,
    ) -> Result<(PreparedArtifact, WorkMeasurements), PreparedArtifactError> {
        let reservation = self.reservation(descriptor, operation)?;
        let source = match &materialization {
            PreparedArtifactMaterialization::Generate(_) => None,
            PreparedArtifactMaterialization::Load(source) => Some(*source),
        };
        validate_plan_binding(*context, descriptor, operation, reservation, source)?;
        let mut evidence =
            ValidationEvidence::for_operation(self.budget, reservation.resident_buffer_bytes);
        if let Some(source) = source {
            evidence.observe_source_inputs(&source.segments);
        }
        if let Err(error) = evidence.ensure_resident_budget() {
            let measurements = failed_measurements(*context, descriptor, operation, &evidence);
            return Err(error.with_measurements(measurements));
        }
        let mut lock = match self.lock(&mut evidence) {
            Ok(lock) => lock,
            Err(error) => {
                let measurements = failed_measurements(*context, descriptor, operation, &evidence);
                return Err(error.with_measurements(measurements));
            }
        };
        let mut published = self.publish_bytes_locked(
            descriptor,
            disposition,
            materialization,
            reservation,
            &mut evidence,
        );
        if published.is_err()
            && let Err(rollback) = self.rollback_materialized(&mut evidence)
        {
            published = Err(rollback);
        }
        let unlock = lock.release(&mut evidence);
        let (validated, final_disposition, cache_bytes) = match (published, unlock) {
            (Ok(published), Ok(())) => published,
            (Err(error), _) => {
                let measurements = failed_measurements(*context, descriptor, operation, &evidence);
                return Err(error.with_measurements(measurements));
            }
            (Ok(_), Err(error)) => {
                let error = match self.rollback_materialized(&mut evidence) {
                    Ok(()) => error,
                    Err(rollback) => rollback,
                };
                let measurements = failed_measurements(*context, descriptor, operation, &evidence);
                return Err(error.with_measurements(measurements));
            }
        };
        let measurements = measurements(
            *context,
            descriptor,
            final_disposition,
            &validated,
            MeasurementInput {
                operation,
                cache_bytes,
                evidence,
            },
        );
        Ok((validated.into_handle(descriptor), measurements))
    }

    fn publish_bytes_locked(
        &self,
        descriptor: &PreparedArtifactDescriptor,
        disposition: ArtifactDisposition,
        mut materialization: PreparedArtifactMaterialization<'_>,
        reservation: PreparedArtifactReservation,
        evidence: &mut ValidationEvidence,
    ) -> Result<(ValidatedArtifact, ArtifactDisposition, u64), PreparedArtifactError> {
        self.validate_raw_budget(descriptor.identity, evidence)?;
        self.remove_orphan_staging(evidence)?;
        evidence.store_write_operation();
        let staging = Builder::new()
            .prefix(STAGING_PREFIX)
            .tempdir_in(&self.cache)?;
        let staging_path = staging.keep();
        let result = (|| -> Result<_, PreparedArtifactError> {
            let payload_path = staging_path.join(PAYLOAD_FILE);
            evidence.store_write_operation();
            let payload_file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&payload_path)?;
            evidence.observe_file_descriptors(2);
            let mut payload = payload_file;
            let buffer_len = usize::try_from(reservation.streaming_buffer_bytes)
                .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?;
            let mut buffer = vec![0_u8; buffer_len];
            let mut payload_hasher = Sha256::new();
            let mut offset = 0_u64;
            let mut manifest_segments = Vec::with_capacity(descriptor.segments.len());
            let mut manifest_resident =
                observed_manifest_segments_resident_bytes(&manifest_segments);
            evidence.acquire_resident(manifest_resident);
            let streamed =
                evidence.with_resident(observed_vec_resident_bytes(&buffer), |evidence| {
                    for (index, segment) in descriptor.segments.iter().enumerate() {
                        let bytes = segment.byte_len()?;
                        let digest = match &mut materialization {
                            PreparedArtifactMaterialization::Generate(generator) => {
                                generate_segment(
                                    *generator,
                                    &mut payload,
                                    &mut payload_hasher,
                                    &mut buffer,
                                    segment,
                                    evidence,
                                )?
                            }
                            PreparedArtifactMaterialization::Load(source) => {
                                let input = &source.segments[index];
                                let mut file =
                                    self.open_segment_source(input, segment, evidence)?;
                                let digest = stream_segment(
                                    &mut file,
                                    &mut payload,
                                    &mut payload_hasher,
                                    &mut buffer,
                                    segment,
                                    evidence,
                                )?;
                                if digest != input.sha256 {
                                    return Err(PreparedArtifactError::SourceIdentityMismatch);
                                }
                                digest
                            }
                        };
                        manifest_segments.push(ManifestSegment {
                            descriptor: segment.clone(),
                            offset,
                            bytes,
                            sha256: encode_hex(&digest),
                        });
                        evidence.resize_resident(
                            &mut manifest_resident,
                            observed_manifest_segments_resident_bytes(&manifest_segments),
                        );
                        evidence.ensure_resident_budget()?;
                        offset = offset
                            .checked_add(bytes)
                            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
                    }
                    Ok(())
                });
            drop(buffer);
            if let Err(error) = streamed {
                evidence.release_resident(manifest_resident);
                return Err(error);
            }
            evidence.store_write_operation();
            payload.sync_all()?;
            drop(payload);
            let payload_sha256: [u8; 32] = payload_hasher.finalize().into();
            let manifest = ArtifactManifest {
                schema: CACHE_SCHEMA.to_string(),
                schema_version: CACHE_SCHEMA_VERSION,
                identity: descriptor.identity.to_string(),
                cache_identity: descriptor.cache_identity.to_string(),
                descriptor: ManifestDescriptor::from_descriptor(descriptor),
                payload_sha256: encode_hex(&payload_sha256),
                payload_bytes: offset,
                segments: manifest_segments,
            };
            evidence.resize_resident(
                &mut manifest_resident,
                observed_manifest_resident_bytes(&manifest),
            );
            let serialized = (|| -> Result<(), PreparedArtifactError> {
                evidence.ensure_resident_budget()?;
                let manifest_path = staging_path.join(MANIFEST_FILE);
                evidence.store_write_operation();
                let mut manifest_output = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&manifest_path)?;
                evidence.observe_file_descriptors(2);
                {
                    let mut bounded_manifest = BoundedFileWriter::new(
                        &mut manifest_output,
                        MANIFEST_RESERVATION_BYTES,
                        evidence,
                    );
                    let serialization = serde_json::to_writer(&mut bounded_manifest, &manifest);
                    if bounded_manifest.exceeded() {
                        return Err(PreparedArtifactError::ManifestReservationExceeded {
                            actual: MANIFEST_RESERVATION_BYTES.saturating_add(1),
                            reserved: MANIFEST_RESERVATION_BYTES,
                        });
                    }
                    serialization?;
                    if let Err(error) = bounded_manifest.write_all(b"\n") {
                        if bounded_manifest.exceeded() {
                            return Err(PreparedArtifactError::ManifestReservationExceeded {
                                actual: MANIFEST_RESERVATION_BYTES.saturating_add(1),
                                reserved: MANIFEST_RESERVATION_BYTES,
                            });
                        }
                        return Err(error.into());
                    }
                }
                evidence.store_write_operation();
                manifest_output.sync_all()?;
                Ok(())
            })();
            drop(manifest);
            evidence.release_resident(manifest_resident);
            serialized?;
            sync_directory_counted(&staging_path, evidence)?;

            let incoming_bytes = directory_size_counted(&staging_path, evidence)?;
            evidence.observe_temporary_storage(incoming_bytes);
            if incoming_bytes > reservation.entry_bytes {
                return Err(PreparedArtifactError::ManifestReservationExceeded {
                    actual: incoming_bytes,
                    reserved: reservation.entry_bytes,
                });
            }
            let mut staged = self.validate_entry_at_path(
                staging_path.clone(),
                descriptor.identity,
                Some(descriptor),
                evidence,
            )?;
            if staged.payload_sha256 != payload_sha256 {
                return Err(PreparedArtifactError::CorruptArtifact);
            }
            let target = self.entry_path(descriptor.identity);
            evidence.store_read_operation();
            match target.symlink_metadata() {
                Ok(_) => {
                    let existing = self.validate_entry_with_evidence(
                        descriptor.identity,
                        Some(descriptor),
                        evidence,
                    )?;
                    if existing.payload_sha256 != payload_sha256 {
                        return Err(PreparedArtifactError::PublicationConflict);
                    }
                    let cache_bytes = self.validate_budget_without_eviction(evidence)?;
                    Ok((existing, disposition, cache_bytes))
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    self.evict_for(descriptor.identity, incoming_bytes, evidence)?;
                    let cache_bytes = self.validate_budget_with_incoming(
                        descriptor.identity,
                        incoming_bytes,
                        evidence,
                    )?;
                    sync_directory_counted(&self.cache, evidence)?;
                    self.rename_staging_for_publication(
                        &staging_path,
                        &target,
                        MaterializedArtifactEvidence {
                            payload_sha256: staged.payload_sha256,
                            payload_bytes: staged.payload_bytes,
                            path: target.clone(),
                            disposition,
                        },
                        evidence,
                    )?;
                    staged.path = target;
                    Ok((staged, disposition, cache_bytes))
                }
                Err(error) => Err(error.into()),
            }
        })();
        let cleanup = remove_staging_counted(&staging_path, evidence);
        match (result, cleanup) {
            (Ok(result), Ok(())) => Ok(result),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        }
    }

    fn open_segment_source(
        &self,
        input: &PreparedArtifactSourceSegment,
        segment: &PreparedArtifactSegmentDescriptor,
        evidence: &mut ValidationEvidence,
    ) -> Result<File, PreparedArtifactError> {
        reject_casa_visible_root(&input.source)?;
        evidence.source_read_operation();
        let source = fs::canonicalize(&input.source).map_err(map_incomplete)?;
        evidence.with_resident(observed_owned_path_bytes(&source), |evidence| {
            reject_casa_source_path(&source, evidence)?;
            if source.starts_with(&self.root) {
                return Err(PreparedArtifactError::InvalidSource);
            }
            evidence.source_read_operation();
            let file = File::open(&source).map_err(map_incomplete)?;
            evidence.observe_file_descriptors(3);
            evidence.source_read_operation();
            let metadata = file.metadata().map_err(map_incomplete)?;
            if !metadata.file_type().is_file() {
                return Err(PreparedArtifactError::InvalidSource);
            }
            let expected = segment.byte_len()?;
            match metadata.len().cmp(&expected) {
                std::cmp::Ordering::Less => Err(PreparedArtifactError::IncompleteArtifact),
                std::cmp::Ordering::Greater => Err(PreparedArtifactError::OversizedArtifact),
                std::cmp::Ordering::Equal => Ok(file),
            }
        })
    }

    pub(super) fn rename_staging_for_publication(
        &self,
        staging_path: &Path,
        target: &Path,
        materialized: MaterializedArtifactEvidence,
        evidence: &mut ValidationEvidence,
    ) -> Result<(), PreparedArtifactError> {
        evidence.store_write_operation();
        fs::rename(staging_path, target)?;
        evidence.materialized = Some(materialized);
        let completed = self.complete_publication_rename(evidence);
        if let Err(error) = completed {
            remove_staging_counted(target, evidence)?;
            return Err(error);
        }
        Ok(())
    }

    fn complete_publication_rename(
        &self,
        evidence: &mut ValidationEvidence,
    ) -> Result<(), PreparedArtifactError> {
        #[cfg(test)]
        if self.fail_after_publication_rename {
            return Err(io::Error::other("injected post-publication-rename failure").into());
        }
        sync_directory_counted(&self.cache, evidence)
    }

    fn rollback_materialized(
        &self,
        evidence: &mut ValidationEvidence,
    ) -> Result<(), PreparedArtifactError> {
        let Some(path) = evidence
            .materialized
            .as_ref()
            .map(|materialized| materialized.path.clone())
        else {
            return Ok(());
        };
        remove_staging_counted(&path, evidence)
    }

    pub(super) fn evict_for(
        &self,
        incoming: ArtifactIdentity,
        incoming_bytes: u64,
        evidence: &mut ValidationEvidence,
    ) -> Result<(), PreparedArtifactError> {
        self.with_entries(evidence, |evidence, entries| {
            entries.retain(|entry| entry.identity != incoming);
            let mut total = incoming_bytes;
            let mut existing_bytes = 0_u64;
            for entry in entries.iter() {
                existing_bytes = existing_bytes
                    .checked_add(entry.bytes)
                    .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
                total = total
                    .checked_add(entry.bytes)
                    .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
            }
            evidence.observe_cache_bytes(existing_bytes);
            evidence.ensure_resident_budget()?;
            let mut evicted = 0_usize;
            while total > self.budget.cache_bytes
                || entries.len().saturating_sub(evicted).saturating_add(1) > self.budget.entries
            {
                let Some(entry) = entries.get(evicted).copied() else {
                    return Err(PreparedArtifactError::CacheBudgetExceeded {
                        required: total,
                        budget: self.budget.cache_bytes,
                    });
                };
                let entry_path = self.entry_path(entry.identity);
                let eviction_path = self
                    .cache
                    .join(format!("{STAGING_PREFIX}evicted-{}", entry.identity));
                evidence.store_write_operation();
                fs::rename(entry_path, &eviction_path)?;
                evidence.record_eviction(entry);
                total = total.saturating_sub(entry.bytes);
                evicted = evicted.saturating_add(1);
                #[cfg(test)]
                if self.fail_after_evictions == Some(evicted) {
                    return Err(io::Error::other("injected post-eviction failure").into());
                }
                evidence.store_write_operation();
                fs::remove_dir_all(eviction_path)?;
            }
            Ok(())
        })
    }

    fn validate_budget_without_eviction(
        &self,
        evidence: &mut ValidationEvidence,
    ) -> Result<u64, PreparedArtifactError> {
        self.with_entries(evidence, |evidence, entries| {
            let total = entries.iter().try_fold(0_u64, |total, entry| {
                total
                    .checked_add(entry.bytes)
                    .ok_or(PreparedArtifactError::ArtifactTooLarge)
            })?;
            evidence.observe_cache_bytes(total);
            if total > self.budget.cache_bytes {
                return Err(PreparedArtifactError::CacheBudgetExceeded {
                    required: total,
                    budget: self.budget.cache_bytes,
                });
            }
            if entries.len() > self.budget.entries {
                return Err(PreparedArtifactError::CacheEntryBudgetExceeded {
                    required: entries.len(),
                    budget: self.budget.entries,
                });
            }
            Ok(total)
        })
    }

    fn validate_budget_with_incoming(
        &self,
        incoming: ArtifactIdentity,
        incoming_bytes: u64,
        evidence: &mut ValidationEvidence,
    ) -> Result<u64, PreparedArtifactError> {
        self.with_entries(evidence, |evidence, entries| {
            if entries.iter().any(|entry| entry.identity == incoming) {
                return Err(PreparedArtifactError::PublicationConflict);
            }
            let total = entries.iter().try_fold(incoming_bytes, |total, entry| {
                total
                    .checked_add(entry.bytes)
                    .ok_or(PreparedArtifactError::ArtifactTooLarge)
            })?;
            let count = entries
                .len()
                .checked_add(1)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
            evidence.observe_cache_bytes(total);
            if total > self.budget.cache_bytes {
                return Err(PreparedArtifactError::CacheBudgetExceeded {
                    required: total,
                    budget: self.budget.cache_bytes,
                });
            }
            if count > self.budget.entries {
                return Err(PreparedArtifactError::CacheEntryBudgetExceeded {
                    required: count,
                    budget: self.budget.entries,
                });
            }
            Ok(total)
        })
    }

    pub(super) fn validate_raw_budget(
        &self,
        planned: ArtifactIdentity,
        evidence: &mut ValidationEvidence,
    ) -> Result<u64, PreparedArtifactError> {
        let (total, count) = with_directory_paths_counted(
            &self.cache,
            evidence,
            root_inventory_limit(self.budget)?,
            |evidence, paths| {
                let mut total = 0_u64;
                let mut count = 0_usize;
                for path in paths {
                    let name = path
                        .file_name()
                        .ok_or_else(|| {
                            PreparedArtifactError::UnknownCacheEntry(path.to_path_buf())
                        })?
                        .to_string_lossy();
                    evidence.store_read_operation();
                    let metadata = path.symlink_metadata()?;
                    let bytes = if name.starts_with(STAGING_PREFIX) {
                        if !metadata.file_type().is_dir() {
                            return Err(PreparedArtifactError::UnknownCacheEntry(
                                path.to_path_buf(),
                            ));
                        }
                        directory_size_counted(path, evidence)?
                    } else {
                        let digest = decode_digest(&name)
                            .filter(|digest| name == encode_hex(digest))
                            .ok_or_else(|| {
                                PreparedArtifactError::UnknownCacheEntry(path.to_path_buf())
                            })?;
                        let identity = ArtifactIdentity::from_owner_digest(digest);
                        if metadata.file_type().is_dir() {
                            if identity != planned {
                                validate_entry_inventory(path, evidence)?;
                            }
                            directory_size_counted(path, evidence)?
                        } else if identity == planned {
                            metadata.len()
                        } else {
                            return Err(PreparedArtifactError::UnknownCacheEntry(
                                path.to_path_buf(),
                            ));
                        }
                    };
                    total = total
                        .checked_add(bytes)
                        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
                    count = count
                        .checked_add(1)
                        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
                }
                Ok((total, count))
            },
        )?;
        evidence.observe_cache_bytes(total);
        if total > self.budget.cache_bytes {
            return Err(PreparedArtifactError::CacheBudgetExceeded {
                required: total,
                budget: self.budget.cache_bytes,
            });
        }
        if count > self.budget.entries {
            return Err(PreparedArtifactError::CacheEntryBudgetExceeded {
                required: count,
                budget: self.budget.entries,
            });
        }
        Ok(total)
    }

    fn with_entries<T>(
        &self,
        evidence: &mut ValidationEvidence,
        use_entries: impl FnOnce(
            &mut ValidationEvidence,
            &mut Vec<CacheInventoryEntry>,
        ) -> Result<T, PreparedArtifactError>,
    ) -> Result<T, PreparedArtifactError> {
        let mut entries = Vec::with_capacity(self.budget.entries);
        let resident_bytes = observed_vec_resident_bytes(&entries);
        evidence.with_resident(resident_bytes, |evidence| {
            with_directory_paths_counted(
                &self.cache,
                evidence,
                root_inventory_limit(self.budget)?,
                |evidence, paths| {
                    for path in paths {
                        let name = path
                            .file_name()
                            .ok_or_else(|| {
                                PreparedArtifactError::UnknownCacheEntry(path.to_path_buf())
                            })?
                            .to_string_lossy();
                        evidence.store_read_operation();
                        if !path.symlink_metadata()?.file_type().is_dir() {
                            return Err(PreparedArtifactError::UnknownCacheEntry(
                                path.to_path_buf(),
                            ));
                        }
                        if name.starts_with(STAGING_PREFIX) {
                            continue;
                        }
                        let digest = decode_digest(&name)
                            .filter(|digest| name == encode_hex(digest))
                            .ok_or_else(|| {
                                PreparedArtifactError::UnknownCacheEntry(path.to_path_buf())
                            })?;
                        let identity = ArtifactIdentity::from_owner_digest(digest);
                        if entries.len() == entries.capacity() {
                            return Err(PreparedArtifactError::CacheEntryBudgetExceeded {
                                required: entries.len().saturating_add(1),
                                budget: self.budget.entries,
                            });
                        }
                        validate_entry_inventory(path, evidence)?;
                        entries.push(CacheInventoryEntry {
                            identity,
                            bytes: directory_size_counted(path, evidence)?,
                        });
                    }
                    Ok(())
                },
            )?;
            entries.sort_unstable_by_key(|entry| entry.identity);
            use_entries(evidence, &mut entries)
        })
    }

    fn remove_orphan_staging(
        &self,
        evidence: &mut ValidationEvidence,
    ) -> Result<(), PreparedArtifactError> {
        with_directory_paths_counted(
            &self.cache,
            evidence,
            root_inventory_limit(self.budget)?,
            |evidence, paths| {
                let mut orphan_bytes = 0_u64;
                let mut orphan_entries = 0_usize;
                for path in paths.iter().filter(|path| {
                    path.file_name()
                        .is_some_and(|name| name.to_string_lossy().starts_with(STAGING_PREFIX))
                }) {
                    evidence.store_read_operation();
                    if !path.symlink_metadata()?.file_type().is_dir() {
                        return Err(PreparedArtifactError::UnknownCacheEntry(path.to_path_buf()));
                    }
                    orphan_bytes = orphan_bytes
                        .checked_add(directory_size_counted(path, evidence)?)
                        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
                    orphan_entries = orphan_entries
                        .checked_add(1)
                        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
                }
                evidence.observe_temporary_storage(orphan_bytes);
                evidence.ensure_resident_budget()?;

                for path in paths.iter().filter(|path| {
                    path.file_name()
                        .is_some_and(|name| name.to_string_lossy().starts_with(STAGING_PREFIX))
                }) {
                    let bytes = directory_size_counted(path, evidence)?;
                    let identity = derive_orphan_staging_evidence_identity(path, bytes)?;
                    evidence.store_write_operation();
                    fs::remove_dir_all(path)?;
                    evidence.record_eviction(CacheInventoryEntry { identity, bytes });
                }
                if orphan_entries > 0 {
                    sync_directory_counted(&self.cache, evidence)?;
                }
                Ok(())
            },
        )
    }

    fn validate_entry_with_evidence(
        &self,
        identity: ArtifactIdentity,
        expected: Option<&PreparedArtifactDescriptor>,
        evidence: &mut ValidationEvidence,
    ) -> Result<ValidatedArtifact, PreparedArtifactError> {
        let directory = self.entry_path(identity);
        self.validate_entry_at_path(directory, identity, expected, evidence)
    }

    fn validate_entry_at_path(
        &self,
        directory: PathBuf,
        identity: ArtifactIdentity,
        expected: Option<&PreparedArtifactDescriptor>,
        evidence: &mut ValidationEvidence,
    ) -> Result<ValidatedArtifact, PreparedArtifactError> {
        evidence.store_read_operation();
        let directory_type = directory
            .symlink_metadata()
            .map_err(map_incomplete)?
            .file_type();
        if !directory_type.is_dir() {
            return Err(PreparedArtifactError::UnknownCacheEntry(directory));
        }
        validate_entry_inventory(&directory, evidence)?;
        let manifest_path = directory.join(MANIFEST_FILE);
        evidence.store_read_operation();
        if manifest_path.symlink_metadata()?.len() > MANIFEST_RESERVATION_BYTES {
            return Err(PreparedArtifactError::InvalidManifest);
        }
        let (manifest, mut manifest_resident) = read_manifest_counted(&manifest_path, evidence)?;
        let validated = (|| -> Result<ValidatedArtifact, PreparedArtifactError> {
            if manifest.schema != CACHE_SCHEMA || manifest.schema_version != CACHE_SCHEMA_VERSION {
                return Err(PreparedArtifactError::UnknownSchema {
                    schema: manifest.schema,
                    version: manifest.schema_version,
                });
            }
            evidence.store_validation();
            if manifest.segments.is_empty() || manifest.segments.len() > MAX_MANIFEST_SEGMENTS {
                return Err(PreparedArtifactError::InvalidManifest);
            }
            if manifest
                .segments
                .windows(2)
                .any(|pair| pair[0].descriptor.name.as_str() >= pair[1].descriptor.name.as_str())
            {
                return Err(PreparedArtifactError::SegmentLayoutMismatch);
            }
            let ArtifactManifest {
                identity: manifest_identity,
                cache_identity: manifest_cache_identity,
                descriptor: manifest_descriptor,
                payload_sha256: manifest_payload_sha256,
                payload_bytes: manifest_payload_bytes,
                segments,
                ..
            } = manifest;
            let mut segment_descriptors = Vec::with_capacity(segments.len());
            let mut segment_integrity = Vec::with_capacity(segments.len());
            let transform_resident = manifest_resident
                .saturating_add(observed_vec_resident_bytes(&segment_descriptors))
                .saturating_add(observed_segment_integrity_resident_bytes(
                    &segment_integrity,
                ));
            evidence.resize_resident(&mut manifest_resident, transform_resident);
            evidence.ensure_resident_budget()?;
            for segment in segments {
                segment_descriptors.push(segment.descriptor);
                segment_integrity.push(ManifestSegmentIntegrity {
                    offset: segment.offset,
                    bytes: segment.bytes,
                    sha256: segment.sha256,
                });
            }
            let descriptor = manifest_descriptor.into_descriptor(segment_descriptors)?;
            evidence.resize_resident(
                &mut manifest_resident,
                observed_validation_state_resident_bytes(
                    &descriptor,
                    &segment_integrity,
                    [
                        &manifest_identity,
                        &manifest_cache_identity,
                        &manifest_payload_sha256,
                    ],
                ),
            );
            evidence.ensure_resident_budget()?;
            if descriptor.identity != identity
                || manifest_identity != identity.to_string()
                || manifest_cache_identity != descriptor.cache_identity().to_string()
                || descriptor.cache_scope.root_identity != self.scope.root_identity
            {
                return Err(PreparedArtifactError::IdentityMismatch);
            }
            if expected.is_some_and(|expected| expected != &descriptor) {
                return Err(PreparedArtifactError::StaleArtifact);
            }
            validate_manifest_segments(&descriptor, &segment_integrity, manifest_payload_bytes)?;
            evidence.store_validation();
            let expected_payload_digest = decode_digest(&manifest_payload_sha256)
                .ok_or(PreparedArtifactError::InvalidManifest)?;
            let payload_path = directory.join(PAYLOAD_FILE);
            let disk_bytes = directory_size_counted(&directory, evidence)?;
            evidence.store_read_operation();
            let payload = File::open(&payload_path).map_err(map_incomplete)?;
            evidence.observe_file_descriptors(2);
            let buffer_len = streaming_buffer_len(self.budget, &descriptor)?;
            let (payload_sha256, payload_bytes) = validate_payload(
                &payload,
                &descriptor.segments,
                &segment_integrity,
                buffer_len,
                evidence,
            )?;
            if payload_bytes != manifest_payload_bytes || payload_sha256 != expected_payload_digest
            {
                return Err(PreparedArtifactError::CorruptArtifact);
            }
            evidence.store_validation();
            Ok(ValidatedArtifact {
                payload_sha256,
                payload_bytes,
                disk_bytes,
                path: directory,
            })
        })();
        evidence.release_resident(manifest_resident);
        validated
    }

    pub(super) fn entry_path(&self, identity: ArtifactIdentity) -> PathBuf {
        self.cache.join(identity.to_string())
    }

    pub(super) fn lock(
        &self,
        evidence: &mut ValidationEvidence,
    ) -> Result<StoreLock<'_>, PreparedArtifactError> {
        let in_process = self
            .state
            .mutation
            .lock()
            .map_err(|_| PreparedArtifactError::PoisonedStore)?;
        evidence.observe_locks(1);
        evidence.store_control_operation();
        let file = match OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&self.lock_path)
        {
            Ok(file) => {
                evidence.store_write_operation();
                file
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                evidence.store_control_operation();
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&self.lock_path)?
            }
            Err(error) => return Err(error.into()),
        };
        evidence.observe_file_descriptors(1);
        evidence.store_control_operation();
        FileExt::lock_exclusive(&file)?;
        Ok(StoreLock {
            _in_process: in_process,
            file,
            locked: true,
        })
    }
}
