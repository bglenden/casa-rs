// SPDX-License-Identifier: LGPL-3.0-or-later

//! Explicit Planner Cost Model Profile promotion from reviewed receipts.
//!
//! Planner behavior changes only through the explicit command in this module.
//! Successful runs never train future plans; failed, aborted, cancelled, and
//! otherwise non-completed receipts are refused here even though they remain
//! receipted evidence of infeasible or interrupted regions.

use std::{
    fmt, fs,
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    BuildIdentity, ExecutionAttemptId, ExecutionReceiptStore, ImplementationRegistryId,
    PlannerCostModelProfileId, ReceiptError, ReceiptStatus, ResourcePolicyId, WorkNodeId,
    execution_bindings::CanonicalEncoder, receipt::ExecutionReceipt,
};

const PROFILE_SCHEMA_NAME: &str = "casa-rs-planner-cost-model-profile";
const PROFILE_SCHEMA_VERSION: u32 = 2;
const PROFILE_IDENTITY_DOMAIN: &[u8] = b"casa-rs-planner-cost-model-profile";

/// Operator review evidence authorizing one explicit profile promotion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProfileReview {
    reviewer: String,
    note: String,
}

impl ProfileReview {
    /// Record the reviewer identity and the review justification.
    pub fn new(
        reviewer: impl Into<String>,
        note: impl Into<String>,
    ) -> Result<Self, ProfilePromotionError> {
        let reviewer = reviewer.into();
        let note = note.into();
        if reviewer.is_empty() || note.is_empty() {
            return Err(ProfilePromotionError::InvalidReview);
        }
        Ok(Self { reviewer, note })
    }

    /// Return the reviewer identity.
    #[must_use]
    pub fn reviewer(&self) -> &str {
        &self.reviewer
    }

    /// Return the review justification.
    #[must_use]
    pub fn note(&self) -> &str {
        &self.note
    }
}

/// One reviewed comparable receipt's per-node calibration observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProfileEvidenceEntry {
    attempt: ExecutionAttemptId,
    node: WorkNodeId,
    predicted_nanos: u64,
    actual_nanos: u64,
}

/// Explicit deployment authority for the initial planner profile.
///
/// A bootstrap is intentionally distinct from a promoted profile: it may be
/// selected only while constructing the first planning bindings, whereas every
/// later profile must be reopened from durable evidence or produced by the
/// reviewed promotion command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlannerCostModelProfileBootstrap {
    profile_id: PlannerCostModelProfileId,
}

impl PlannerCostModelProfileBootstrap {
    /// Select the deployment baseline used before the first reviewed profile
    /// promotion.
    #[must_use]
    pub const fn new(profile_id: PlannerCostModelProfileId) -> Self {
        Self { profile_id }
    }

    /// Return the deployment-selected baseline identity.
    #[must_use]
    pub const fn profile_id(self) -> PlannerCostModelProfileId {
        self.profile_id
    }
}

impl ProfileEvidenceEntry {
    /// Return the receipt attempt this observation was promoted from.
    #[must_use]
    pub const fn attempt(&self) -> ExecutionAttemptId {
        self.attempt
    }

    /// Return the exact plan node observed.
    #[must_use]
    pub const fn node(&self) -> &WorkNodeId {
        &self.node
    }

    /// Return the plan's predicted elapsed nanoseconds.
    #[must_use]
    pub const fn predicted_nanos(&self) -> u64 {
        self.predicted_nanos
    }

    /// Return the receipted actual elapsed nanoseconds.
    #[must_use]
    pub const fn actual_nanos(&self) -> u64 {
        self.actual_nanos
    }
}

/// Versioned auditable Planner Cost Model Profile produced only by explicit
/// promotion over reviewed comparable completed receipts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannerCostModelProfileRecord {
    profile_id: PlannerCostModelProfileId,
    lineage_cost_model: PlannerCostModelProfileId,
    plan_identity: [u8; 32],
    implementation_registry_identity: ImplementationRegistryId,
    resource_policy_identity: ResourcePolicyId,
    build_identity: BuildIdentity,
    prediction_confidence_ppm: u32,
    promoted_unix_millis: u64,
    review: ProfileReview,
    entries: Vec<ProfileEvidenceEntry>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProfileComparabilityBoundary {
    plan_identity: [u8; 32],
    implementation_registry_identity: ImplementationRegistryId,
    resource_policy_identity: ResourcePolicyId,
    build_identity: BuildIdentity,
}

impl PlannerCostModelProfileRecord {
    /// Bind the immutable initial planner profile before any reviewed
    /// promotion has been performed.
    ///
    /// Initial profiles are selected by deployment configuration and are not
    /// persisted promotion documents. Every subsequent profile must come
    /// from [`promote_cost_model_profile`] or [`open_cost_model_profile`].
    #[must_use]
    pub(crate) fn initial(profile_id: PlannerCostModelProfileId) -> Self {
        Self {
            profile_id,
            lineage_cost_model: profile_id,
            plan_identity: [0; 32],
            implementation_registry_identity: ImplementationRegistryId::from_sha256([0; 32]),
            resource_policy_identity: ResourcePolicyId::from_sha256([0; 32]),
            build_identity: BuildIdentity::from_sha256([0; 32]),
            prediction_confidence_ppm: 0,
            promoted_unix_millis: 0,
            review: ProfileReview {
                reviewer: "initial-baseline".to_string(),
                note: "deployment-selected initial planner profile".to_string(),
            },
            entries: Vec::new(),
        }
    }

    /// Return the stable content identity of this profile.
    ///
    /// The digest covers the lineage profile, prediction confidence, review
    /// evidence, and every calibration entry; it deliberately excludes the
    /// promotion timestamp so re-running one identical command cannot fork a
    /// second profile identity.
    #[must_use]
    pub const fn profile_id(&self) -> PlannerCostModelProfileId {
        self.profile_id
    }

    /// Return the cost-model profile identity the promoted plans ran under.
    #[must_use]
    pub const fn lineage_cost_model(&self) -> PlannerCostModelProfileId {
        self.lineage_cost_model
    }

    /// Return the exact effective plan identity represented by the promoted evidence.
    #[must_use]
    pub const fn plan_identity(&self) -> [u8; 32] {
        self.plan_identity
    }

    /// Return the exact implementation-registry snapshot represented by the evidence.
    #[must_use]
    pub const fn implementation_registry_identity(&self) -> ImplementationRegistryId {
        self.implementation_registry_identity
    }

    /// Return the exact resource-policy identity represented by the evidence.
    #[must_use]
    pub const fn resource_policy_identity(&self) -> ResourcePolicyId {
        self.resource_policy_identity
    }

    /// Return the exact executable build identity represented by the evidence.
    #[must_use]
    pub const fn build_identity(&self) -> BuildIdentity {
        self.build_identity
    }

    /// Return the plans' fixed-point prediction confidence.
    #[must_use]
    pub const fn prediction_confidence_ppm(&self) -> u32 {
        self.prediction_confidence_ppm
    }

    /// Return when the promotion was executed.
    #[must_use]
    pub const fn promoted_unix_millis(&self) -> u64 {
        self.promoted_unix_millis
    }

    /// Return the operator review evidence authorizing this promotion.
    #[must_use]
    pub const fn review(&self) -> &ProfileReview {
        &self.review
    }

    /// Return every calibration entry in deterministic order.
    #[must_use]
    pub fn entries(&self) -> &[ProfileEvidenceEntry] {
        &self.entries
    }
}

impl From<PlannerCostModelProfileBootstrap> for PlannerCostModelProfileRecord {
    fn from(bootstrap: PlannerCostModelProfileBootstrap) -> Self {
        Self::initial(bootstrap.profile_id())
    }
}

/// Failure from an explicit profile promotion or profile reopening.
#[derive(Debug)]
pub enum ProfilePromotionError {
    /// Receipt opening or integrity validation failed.
    Receipt(ReceiptError),
    /// Profile persistence failed.
    Io {
        /// Persistence step that failed.
        action: &'static str,
        /// Underlying error.
        source: std::io::Error,
    },
    /// Profile document encoding or decoding failed.
    Json {
        /// Underlying error.
        source: serde_json::Error,
    },
    /// Promotion was requested without reviewed receipts.
    EmptyEvidence,
    /// Review evidence lacked a reviewer identity or justification.
    InvalidReview,
    /// A receipt was not a completed run, so it must never train a profile.
    NotCompleted {
        /// Refused attempt.
        attempt: ExecutionAttemptId,
        /// Actual terminal or active status retained by the receipt.
        status: ReceiptStatus,
    },
    /// A receipt did not share the reviewed comparability boundary.
    NotComparable {
        /// Refused attempt.
        attempt: ExecutionAttemptId,
        /// Identity field that diverged from the first reviewed receipt.
        field: &'static str,
    },
    /// A reviewed attempt appeared more than once in one promotion request.
    DuplicateEvidence {
        /// Duplicated attempt.
        attempt: ExecutionAttemptId,
    },
    /// A comparable receipt omitted predicted or actual stage evidence.
    MissingStageEvidence {
        /// Refused attempt.
        attempt: ExecutionAttemptId,
        /// Node lacking calibration evidence.
        node: WorkNodeId,
    },
    /// An identical profile was already promoted; profiles are immutable.
    AlreadyPromoted {
        /// Existing profile identity.
        profile: PlannerCostModelProfileId,
    },
    /// The requested profile does not exist under the profile root.
    UnknownProfile {
        /// Requested profile identity.
        profile: PlannerCostModelProfileId,
    },
    /// A stored profile failed its checksum or identity recomputation.
    CorruptProfile {
        /// Stored profile identity.
        profile: PlannerCostModelProfileId,
    },
}

impl fmt::Display for ProfilePromotionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Receipt(error) => write!(formatter, "receipt evidence failed: {error}"),
            Self::Io { action, source } => write!(formatter, "{action} failed: {source}"),
            Self::Json { source } => write!(formatter, "profile document failed: {source}"),
            Self::EmptyEvidence => formatter.write_str("promotion requires reviewed receipts"),
            Self::DuplicateEvidence { attempt } => {
                write!(formatter, "attempt {attempt} was offered more than once")
            }
            Self::InvalidReview => {
                formatter.write_str("promotion requires a reviewer identity and justification")
            }
            Self::NotCompleted { attempt, status } => write!(
                formatter,
                "attempt {attempt} has status {status:?}; only completed runs promote profiles"
            ),
            Self::NotComparable { attempt, field } => {
                write!(formatter, "attempt {attempt} diverges on {field}")
            }
            Self::MissingStageEvidence { attempt, node } => write!(
                formatter,
                "attempt {attempt} lacks predicted or actual evidence for node {}",
                node.as_str()
            ),
            Self::AlreadyPromoted { profile } => {
                write!(formatter, "profile {profile} is already promoted")
            }
            Self::UnknownProfile { profile } => {
                write!(formatter, "profile {profile} is not present")
            }
            Self::CorruptProfile { profile } => {
                write!(formatter, "profile {profile} failed integrity validation")
            }
        }
    }
}

impl std::error::Error for ProfilePromotionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Receipt(error) => Some(error),
            Self::Io { source, .. } => Some(source),
            Self::Json { source } => Some(source),
            _ => None,
        }
    }
}

/// Promote one versioned auditable Planner Cost Model Profile from explicitly
/// reviewed comparable completed receipts.
///
/// This command is the only way planner behavior changes after planning began.
/// Every offered attempt must reopen from the receipt store as an integrity-
/// checked `Completed` run sharing the first receipt's effective plan,
/// implementation registry, resource policy, build identity, and lineage
/// cost-model identity. Evidence is canonicalized: the profile identity is a
/// function of the reviewed evidence *set*, so caller order never matters and
/// duplicate attempts are refused.
pub fn promote_cost_model_profile(
    profiles_root: impl AsRef<Path>,
    receipts: &ExecutionReceiptStore,
    attempts: &[ExecutionAttemptId],
    review: ProfileReview,
    now_unix_millis: u64,
) -> Result<PlannerCostModelProfileRecord, ProfilePromotionError> {
    if attempts.is_empty() {
        return Err(ProfilePromotionError::EmptyEvidence);
    }
    let mut ordered = attempts.to_vec();
    ordered.sort();
    for pair in ordered.windows(2) {
        if pair[0] == pair[1] {
            return Err(ProfilePromotionError::DuplicateEvidence { attempt: pair[0] });
        }
    }
    let opened = ordered
        .iter()
        .map(|attempt| {
            receipts
                .open(*attempt)
                .map_err(ProfilePromotionError::Receipt)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let first = &opened[0];
    let lineage_cost_model = first.cost_model_identity();
    let boundary = ProfileComparabilityBoundary {
        plan_identity: first.plan_identity(),
        implementation_registry_identity: ImplementationRegistryId::from_sha256(
            first.implementation_registry_identity(),
        ),
        resource_policy_identity: ResourcePolicyId::from_sha256(first.resource_policy_identity()),
        build_identity: first.build_identity(),
    };
    for (attempt, receipt) in ordered.iter().zip(&opened) {
        validate_comparability(*attempt, first, receipt)?;
    }
    let confidence_ppm = first.prediction_confidence_ppm();
    let mut entries = Vec::new();
    for (attempt, receipt) in ordered.iter().zip(&opened) {
        for node in receipt.plan_node_identities() {
            let Some(predicted_nanos) = receipt.stage_predicted_elapsed_nanos(&node) else {
                return Err(ProfilePromotionError::MissingStageEvidence {
                    attempt: *attempt,
                    node,
                });
            };
            let Some(actual_nanos) = receipt.stage_actual_elapsed_nanos(&node) else {
                return Err(ProfilePromotionError::MissingStageEvidence {
                    attempt: *attempt,
                    node,
                });
            };
            entries.push(ProfileEvidenceEntry {
                attempt: *attempt,
                node,
                predicted_nanos,
                actual_nanos,
            });
        }
    }
    let record = PlannerCostModelProfileRecord {
        profile_id: profile_identity(&record_digest_input(
            lineage_cost_model,
            boundary,
            confidence_ppm,
            &review,
            &entries,
        )),
        lineage_cost_model: PlannerCostModelProfileId::from_sha256(lineage_cost_model),
        plan_identity: boundary.plan_identity,
        implementation_registry_identity: boundary.implementation_registry_identity,
        resource_policy_identity: boundary.resource_policy_identity,
        build_identity: boundary.build_identity,
        prediction_confidence_ppm: confidence_ppm,
        promoted_unix_millis: now_unix_millis,
        review,
        entries,
    };
    persist_profile(profiles_root, &record)?;
    Ok(record)
}

fn validate_comparability(
    attempt: ExecutionAttemptId,
    first: &ExecutionReceipt,
    receipt: &ExecutionReceipt,
) -> Result<(), ProfilePromotionError> {
    let status = receipt.status();
    if status != ReceiptStatus::Completed {
        return Err(ProfilePromotionError::NotCompleted { attempt, status });
    }
    let boundaries = [
        (
            first.plan_identity() != receipt.plan_identity(),
            "effective plan",
        ),
        (
            first.implementation_registry_identity() != receipt.implementation_registry_identity(),
            "implementation registry",
        ),
        (
            first.resource_policy_identity() != receipt.resource_policy_identity(),
            "resource policy",
        ),
        (
            first.build_identity() != receipt.build_identity(),
            "build identity",
        ),
        (
            first.cost_model_identity() != receipt.cost_model_identity(),
            "lineage cost model",
        ),
    ];
    for (diverged, field) in boundaries {
        if diverged {
            return Err(ProfilePromotionError::NotComparable { attempt, field });
        }
    }
    Ok(())
}

fn record_digest_input(
    lineage_cost_model: [u8; 32],
    boundary: ProfileComparabilityBoundary,
    confidence_ppm: u32,
    review: &ProfileReview,
    entries: &[ProfileEvidenceEntry],
) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::new();
    encoder.string(std::str::from_utf8(PROFILE_IDENTITY_DOMAIN).expect("ASCII domain"));
    encoder.u32(PROFILE_SCHEMA_VERSION);
    encoder.digest(lineage_cost_model);
    encoder.digest(boundary.plan_identity);
    encoder.digest(boundary.implementation_registry_identity.as_bytes());
    encoder.digest(boundary.resource_policy_identity.as_bytes());
    encoder.digest(boundary.build_identity.as_bytes());
    encoder.u32(confidence_ppm);
    encoder.string(review.reviewer());
    encoder.string(review.note());
    encoder.usize(entries.len());
    for entry in entries {
        encoder.digest(entry.attempt.as_bytes());
        encoder.string(entry.node.as_str());
        encoder.u64(entry.predicted_nanos);
        encoder.u64(entry.actual_nanos);
    }
    encoder.finish().to_vec()
}

fn profile_identity(input: &[u8]) -> PlannerCostModelProfileId {
    PlannerCostModelProfileId::from_sha256(Sha256::digest(input).into())
}

#[derive(Serialize, Deserialize)]
struct ProfileDocument {
    schema: ProfileSchema,
    payload_sha256: String,
    profile: ProfileBody,
}

#[derive(Serialize, Deserialize)]
struct ProfileSchema {
    name: String,
    version: u32,
}

#[derive(Clone, Serialize, Deserialize)]
struct ProfileBody {
    profile_id: String,
    lineage_cost_model: String,
    plan_identity: String,
    implementation_registry_identity: String,
    resource_policy_identity: String,
    build_identity: String,
    prediction_confidence_ppm: u32,
    promoted_unix_millis: u64,
    reviewer: String,
    note: String,
    entries: Vec<EvidenceEntryBody>,
}

#[derive(Clone, Serialize, Deserialize)]
struct EvidenceEntryBody {
    attempt: String,
    node: String,
    predicted_nanos: u64,
    actual_nanos: u64,
}

fn encode_profile(
    record: &PlannerCostModelProfileRecord,
) -> Result<Vec<u8>, ProfilePromotionError> {
    let body = ProfileBody {
        profile_id: hex(&record.profile_id.as_bytes()),
        lineage_cost_model: hex(&record.lineage_cost_model.as_bytes()),
        plan_identity: hex(&record.plan_identity),
        implementation_registry_identity: hex(&record.implementation_registry_identity.as_bytes()),
        resource_policy_identity: hex(&record.resource_policy_identity.as_bytes()),
        build_identity: hex(&record.build_identity.as_bytes()),
        prediction_confidence_ppm: record.prediction_confidence_ppm(),
        promoted_unix_millis: record.promoted_unix_millis(),
        reviewer: record.review.reviewer().to_string(),
        note: record.review.note().to_string(),
        entries: record
            .entries()
            .iter()
            .map(|entry| EvidenceEntryBody {
                attempt: hex(&entry.attempt.as_bytes()),
                node: entry.node.as_str().to_string(),
                predicted_nanos: entry.predicted_nanos(),
                actual_nanos: entry.actual_nanos(),
            })
            .collect(),
    };
    let payload =
        serde_json::to_vec(&body).map_err(|source| ProfilePromotionError::Json { source })?;
    let document = ProfileDocument {
        schema: ProfileSchema {
            name: PROFILE_SCHEMA_NAME.to_string(),
            version: PROFILE_SCHEMA_VERSION,
        },
        payload_sha256: hex(&Sha256::digest(&payload)),
        profile: body,
    };
    serde_json::to_vec_pretty(&document).map_err(|source| ProfilePromotionError::Json { source })
}

fn decode_profile(bytes: &[u8]) -> Result<PlannerCostModelProfileRecord, ProfilePromotionError> {
    let document: ProfileDocument =
        serde_json::from_slice(bytes).map_err(|source| ProfilePromotionError::Json { source })?;
    if document.schema.name != PROFILE_SCHEMA_NAME
        || document.schema.version != PROFILE_SCHEMA_VERSION
    {
        return Err(ProfilePromotionError::CorruptProfile {
            profile: declared_profile_id(&document.profile),
        });
    }
    let payload = serde_json::to_vec(&document.profile)
        .map_err(|source| ProfilePromotionError::Json { source })?;
    if document.payload_sha256 != hex(&Sha256::digest(&payload)) {
        return Err(ProfilePromotionError::CorruptProfile {
            profile: declared_profile_id(&document.profile),
        });
    }
    let body = document.profile;
    let declared_id = declared_profile_id(&body);
    let review = ProfileReview::new(body.reviewer, body.note)?;
    let entries = body
        .entries
        .iter()
        .map(|entry| {
            let attempt =
                parse_hex32(&entry.attempt).ok_or(ProfilePromotionError::CorruptProfile {
                    profile: declared_id,
                })?;
            Ok(ProfileEvidenceEntry {
                attempt: ExecutionAttemptId::from_sha256(attempt),
                node: WorkNodeId::new(entry.node.clone()),
                predicted_nanos: entry.predicted_nanos,
                actual_nanos: entry.actual_nanos,
            })
        })
        .collect::<Result<Vec<_>, ProfilePromotionError>>()?;
    let Some(lineage_bytes) = parse_hex32(&body.lineage_cost_model) else {
        return Err(ProfilePromotionError::CorruptProfile {
            profile: declared_id,
        });
    };
    let lineage_cost_model = PlannerCostModelProfileId::from_sha256(lineage_bytes);
    let Some(plan_identity) = parse_hex32(&body.plan_identity) else {
        return Err(ProfilePromotionError::CorruptProfile {
            profile: declared_id,
        });
    };
    let Some(implementation_registry_bytes) = parse_hex32(&body.implementation_registry_identity)
    else {
        return Err(ProfilePromotionError::CorruptProfile {
            profile: declared_id,
        });
    };
    let Some(resource_policy_bytes) = parse_hex32(&body.resource_policy_identity) else {
        return Err(ProfilePromotionError::CorruptProfile {
            profile: declared_id,
        });
    };
    let Some(build_bytes) = parse_hex32(&body.build_identity) else {
        return Err(ProfilePromotionError::CorruptProfile {
            profile: declared_id,
        });
    };
    let boundary = ProfileComparabilityBoundary {
        plan_identity,
        implementation_registry_identity: ImplementationRegistryId::from_sha256(
            implementation_registry_bytes,
        ),
        resource_policy_identity: ResourcePolicyId::from_sha256(resource_policy_bytes),
        build_identity: BuildIdentity::from_sha256(build_bytes),
    };
    let expected = profile_identity(&record_digest_input(
        lineage_bytes,
        boundary,
        body.prediction_confidence_ppm,
        &review,
        &entries,
    ));
    if expected != declared_id {
        return Err(ProfilePromotionError::CorruptProfile {
            profile: declared_id,
        });
    }
    Ok(PlannerCostModelProfileRecord {
        profile_id: declared_id,
        lineage_cost_model,
        plan_identity: boundary.plan_identity,
        implementation_registry_identity: boundary.implementation_registry_identity,
        resource_policy_identity: boundary.resource_policy_identity,
        build_identity: boundary.build_identity,
        prediction_confidence_ppm: body.prediction_confidence_ppm,
        promoted_unix_millis: body.promoted_unix_millis,
        review,
        entries,
    })
}

fn persist_profile(
    profiles_root: impl AsRef<Path>,
    record: &PlannerCostModelProfileRecord,
) -> Result<(), ProfilePromotionError> {
    let root = profiles_root.as_ref();
    fs::create_dir_all(root).map_err(|source| ProfilePromotionError::Io {
        action: "create profile directory",
        source,
    })?;
    let path = profile_path(root, record.profile_id());
    let bytes = encode_profile(record)?;
    let temporary = tempfile::Builder::new()
        .prefix(".profile-staging-")
        .suffix(".tmp")
        .tempfile_in(root)
        .map_err(|source| ProfilePromotionError::Io {
            action: "stage profile document",
            source,
        })?;
    temporary
        .as_file()
        .write_all(&bytes)
        .map_err(|source| ProfilePromotionError::Io {
            action: "write staged profile document",
            source,
        })?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|source| ProfilePromotionError::Io {
            action: "sync staged profile document",
            source,
        })?;
    match temporary.persist_noclobber(&path) {
        Ok(_) => sync_directory(root),
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(ProfilePromotionError::AlreadyPromoted {
                profile: record.profile_id(),
            })
        }
        Err(error) => Err(ProfilePromotionError::Io {
            action: "publish profile document",
            source: error.error,
        }),
    }
}

fn profile_path(root: &Path, profile: PlannerCostModelProfileId) -> PathBuf {
    root.join(format!("{profile}.json"))
}

/// Reopen and integrity-check one promoted profile by its content identity.
///
/// The stored document must recompute to exactly the requested profile
/// identity; a self-consistent document stored under a different name is
/// rejected as corrupt rather than silently accepted.
pub fn open_cost_model_profile(
    profiles_root: impl AsRef<Path>,
    profile: PlannerCostModelProfileId,
) -> Result<PlannerCostModelProfileRecord, ProfilePromotionError> {
    let path = profile_path(profiles_root.as_ref(), profile);
    let bytes = fs::read(&path).map_err(|source| {
        if source.kind() == std::io::ErrorKind::NotFound {
            ProfilePromotionError::UnknownProfile { profile }
        } else {
            ProfilePromotionError::Io {
                action: "read profile document",
                source,
            }
        }
    })?;
    let record = decode_profile(&bytes)?;
    if record.profile_id() != profile {
        return Err(ProfilePromotionError::CorruptProfile { profile });
    }
    Ok(record)
}

fn sync_directory(path: &Path) -> Result<(), ProfilePromotionError> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| ProfilePromotionError::Io {
            action: "sync profile directory",
            source,
        })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn declared_profile_id(body: &ProfileBody) -> PlannerCostModelProfileId {
    PlannerCostModelProfileId::from_sha256(parse_hex32(&body.profile_id).unwrap_or([0; 32]))
}

fn parse_hex32(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 || !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let mut digest = [0u8; 32];
    for (index, pair) in text.as_bytes().chunks(2).enumerate() {
        digest[index] = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(digest)
}
