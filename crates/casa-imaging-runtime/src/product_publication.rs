// SPDX-License-Identifier: LGPL-3.0-or-later

//! Product-generation binding to the existing receipted publication seam.

use std::{error::Error, fmt};

use casa_imaging_model::{
    CompiledProblem, ProductGeneration, ProductGenerationId, ProductGraphId, ProductNodeId,
};

use crate::{
    ArtifactIdentity, ArtifactRole, PhysicalWorkBinding, PublicationParticipant, WorkKind,
    WorkNodeId,
};

/// One product node bound to its T11 planned output artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductPlannedArtifact {
    product_node: ProductNodeId,
    artifact: ArtifactIdentity,
}

impl ProductPlannedArtifact {
    /// Return the graph-local product node.
    #[must_use]
    pub const fn product_node(self) -> ProductNodeId {
        self.product_node
    }

    /// Return the exact T11 planned output artifact.
    #[must_use]
    pub const fn artifact(self) -> ArtifactIdentity {
        self.artifact
    }
}

/// One exact product generation bound to T11's sole receipted publication node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductPublicationBinding {
    generation_id: ProductGenerationId,
    publication_node: WorkNodeId,
    artifacts: Box<[ProductPlannedArtifact]>,
}

impl ProductPublicationBinding {
    /// Return the exact product generation being published.
    #[must_use]
    pub const fn generation_id(&self) -> ProductGenerationId {
        self.generation_id
    }

    /// Return the sole T11 Publication work node and fence owner.
    #[must_use]
    pub const fn publication_node(&self) -> &WorkNodeId {
        &self.publication_node
    }

    /// Return every logical product and its receipted output artifact.
    #[must_use]
    pub const fn artifacts(&self) -> &[ProductPlannedArtifact] {
        &self.artifacts
    }
}

/// Failure to join a product generation to T11's publication contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProductPublicationBindingError {
    /// The generation was compiled for a different product graph.
    StaleGeneration {
        /// Graph required by the compiled problem.
        expected: ProductGraphId,
        /// Graph carried by the generation.
        actual: ProductGraphId,
    },
    /// The physical DAG does not contain exactly one Publication node.
    PublicationNodeCount {
        /// Actual number of Publication work nodes.
        actual: usize,
    },
    /// One product-generation artifact is absent from the T11 ledger.
    MissingArtifact {
        /// Product node whose exact artifact is absent.
        product_node: ProductNodeId,
        /// Expected T11 artifact identity.
        artifact: ArtifactIdentity,
    },
    /// A matching planned artifact is not an output.
    ArtifactRole {
        /// Product node being bound.
        product_node: ProductNodeId,
        /// Matching T11 artifact identity.
        artifact: ArtifactIdentity,
        /// Incorrect planned role.
        actual: ArtifactRole,
    },
    /// A product artifact is assigned outside the sole Publication node.
    PublicationNode {
        /// Product node being bound.
        product_node: ProductNodeId,
        /// Sole Publication node required by the generation.
        expected: WorkNodeId,
        /// Work node that owns the planned artifact.
        actual: WorkNodeId,
    },
    /// MODEL_DATA cannot publish without the bound observation transaction's exact write set.
    MissingObservationWriteSetAuthority {
        /// Participant whose MeasurementSet identity lacks write authority.
        participant: PublicationParticipant,
    },
    /// A physical layout was assigned to the wrong semantic publication participant.
    PhysicalLayoutParticipant {
        /// Exact output artifact whose participant was wrong.
        artifact: ArtifactIdentity,
        /// Exact Product participant, or `None` when MODEL_DATA is required.
        expected: Option<PublicationParticipant>,
        /// Participant declared by the selected physical layout.
        actual: PublicationParticipant,
    },
}

impl fmt::Display for ProductPublicationBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StaleGeneration { expected, actual } => write!(
                formatter,
                "product generation belongs to graph {actual}, expected {expected}"
            ),
            Self::PublicationNodeCount { actual } => write!(
                formatter,
                "product generation requires exactly one Publication node, found {actual}"
            ),
            Self::MissingArtifact {
                product_node,
                artifact,
            } => write!(
                formatter,
                "product node {product_node:?} is missing planned output artifact {artifact}"
            ),
            Self::ArtifactRole {
                product_node,
                artifact,
                actual,
            } => write!(
                formatter,
                "product node {product_node:?} artifact {artifact} has role {actual:?}, expected Output"
            ),
            Self::PublicationNode {
                product_node,
                expected,
                actual,
            } => write!(
                formatter,
                "product node {product_node:?} is planned by {}, expected sole Publication node {}",
                actual.as_str(),
                expected.as_str()
            ),
            Self::MissingObservationWriteSetAuthority { participant } => write!(
                formatter,
                "publication participant {participant:?} requires exact observation write-set authority"
            ),
            Self::PhysicalLayoutParticipant {
                artifact,
                expected,
                actual,
            } => write!(
                formatter,
                "publication artifact {artifact} has physical participant {actual:?}, expected {expected:?}"
            ),
        }
    }
}

impl Error for ProductPublicationBindingError {}

/// Bind a complete product generation to T11's existing output ledger and publication fence.
pub(crate) fn bind_product_publication(
    problem: &CompiledProblem,
    generation: &ProductGeneration,
    physical_work: &PhysicalWorkBinding,
) -> Result<ProductPublicationBinding, ProductPublicationBindingError> {
    let graph = problem.product_graph();
    if generation.graph_id() != graph.graph_id() {
        return Err(ProductPublicationBindingError::StaleGeneration {
            expected: graph.graph_id(),
            actual: generation.graph_id(),
        });
    }
    let publication_nodes = physical_work
        .execution_dag()
        .nodes()
        .values()
        .filter(|node| node.kind == WorkKind::Publication)
        .map(|node| node.id.clone())
        .collect::<Vec<_>>();
    let [publication_node] = publication_nodes.as_slice() else {
        return Err(ProductPublicationBindingError::PublicationNodeCount {
            actual: publication_nodes.len(),
        });
    };
    let mut artifacts = Vec::with_capacity(graph.publication().members().len());
    for product_node in graph.publication().members() {
        let artifact = ArtifactIdentity::from_sha256(
            generation
                .artifact_id(*product_node)
                .expect("publication members belong to the bound product graph")
                .as_bytes(),
        );
        let Some(planned) = physical_work
            .artifacts()
            .iter()
            .find(|planned| planned.identity() == artifact)
        else {
            return Err(ProductPublicationBindingError::MissingArtifact {
                product_node: *product_node,
                artifact,
            });
        };
        if planned.role() != ArtifactRole::Output {
            return Err(ProductPublicationBindingError::ArtifactRole {
                product_node: *product_node,
                artifact,
                actual: planned.role(),
            });
        }
        if planned.node() != publication_node {
            return Err(ProductPublicationBindingError::PublicationNode {
                product_node: *product_node,
                expected: publication_node.clone(),
                actual: planned.node().clone(),
            });
        }
        artifacts.push(ProductPlannedArtifact {
            product_node: *product_node,
            artifact,
        });
    }
    for layout in physical_work.publication_layouts().entries() {
        if matches!(layout.participant(), PublicationParticipant::ModelData(_)) {
            return Err(
                ProductPublicationBindingError::MissingObservationWriteSetAuthority {
                    participant: layout.participant(),
                },
            );
        }
        let expected = artifacts
            .iter()
            .find(|artifact| artifact.artifact == layout.artifact())
            .map(|artifact| PublicationParticipant::Product(artifact.product_node));
        if expected != Some(layout.participant()) {
            return Err(ProductPublicationBindingError::PhysicalLayoutParticipant {
                artifact: layout.artifact(),
                expected,
                actual: layout.participant(),
            });
        }
    }
    Ok(ProductPublicationBinding {
        generation_id: generation.generation_id(),
        publication_node: publication_node.clone(),
        artifacts: artifacts.into_boxed_slice(),
    })
}
