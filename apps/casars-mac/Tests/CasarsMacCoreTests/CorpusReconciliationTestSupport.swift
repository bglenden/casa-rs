// SPDX-License-Identifier: LGPL-3.0-or-later

import CasarsFrontendServices
@testable import CasarsMacCore

extension AssistantPersistenceClient {
    func prepareTestReconciliation(
        projectRoot: String,
        sources: [AssistantProjectCorpusSourceRequest],
        generation: UInt64 = 1,
        scope: AssistantCorpusReconciliationScope = .projectDocuments
    ) throws -> AssistantPreparedCorpusReconciliationState {
        try prepareCorpusReconciliation(
            projectRoot: projectRoot,
            sources: sources,
            generation: generation,
            scope: scope
        )
    }

    func applyTestReconciliation(
        projectRoot: String,
        documents: [AssistantCorpusDocumentRequest],
        removeMissingLayers: Set<String>,
        projectSources: [AssistantProjectCorpusSourceRequest],
        failedProjectSources: Set<String>
    ) throws -> AssistantCorpusIndexReportState {
        let prepared = try prepareTestReconciliation(
            projectRoot: projectRoot,
            sources: projectSources
        )
        let outcomes = prepared.extractPaths.map { path in
            AssistantProjectSourceExtractionOutcome(
                relativePath: path,
                status: failedProjectSources.contains(path) ? .failed : .succeeded,
                diagnostic: failedProjectSources.contains(path) ? "test extraction failure" : nil
            )
        }
        return try applyCorpusReconciliation(
            projectRoot: projectRoot,
            prepared: prepared,
            documents: documents,
            removeMissingLayers: removeMissingLayers,
            outcomes: outcomes
        )
    }
}
