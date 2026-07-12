import CasarsFrontendServices
import Foundation

package enum TutorialAcquisitionPhase: String, Codable, Equatable {
    case missing, downloading, verifying, unpacking, checking, materializing, ready, cancelled
    case networkFailed = "network_failed"
    case checksumFailed = "checksum_failed"
    case unsafeArchive = "unsafe_archive"
    case destinationCollision = "destination_collision"

    package var isRunning: Bool {
        [.downloading, .verifying, .unpacking, .checking, .materializing].contains(self)
    }
}

package struct TutorialSectionState: Codable, Equatable, Identifiable {
    package var id: String
    package var title: String
    package var datasetIds: [String]
    package var cellIds: [String]
}

package struct TutorialOptionalCheckState: Codable, Equatable, Identifiable {
    package var id: String
    package var label: String
    package var kind: String
    package var path: String
}

package struct TutorialUnpackState: Codable, Equatable {
    package var format: String
    package var archiveRoot: String?
    package var maxEntries: UInt64
    package var maxExpandedBytes: UInt64
}

package struct TutorialCheckOutcomeState: Codable, Equatable, Identifiable {
    package var id: String { checkId }
    package var checkId: String
    package var status: String
    package var detail: String
}

package struct TutorialDatasetAttemptState: Codable, Equatable, Identifiable {
    package var id: UInt64 { generation }
    package var generation: UInt64
    package var kind: String
    package var phase: TutorialAcquisitionPhase
    package var requestedUri: String
    package var resolvedUri: String
    package var redirects: [String]
    package var expectedSizeBytes: UInt64?
    package var expectedSha256: String?
    package var approvalSha256: String
    package var approvedMissingDigest: Bool
    package var skippedCheckIds: [String]
    package var downloadedBytes: UInt64
    package var computedSha256: String?
    package var checks: [TutorialCheckOutcomeState]
    package var error: String?
    package var startedAt: UInt64
    package var finishedAt: UInt64?
}

package struct TutorialDatasetState: Codable, Equatable, Identifiable {
    package var id: String
    package var displayName: String
    package var uri: String
    package var destination: String
    package var expectedSizeBytes: UInt64?
    package var sha256: String?
    package var unpack: TutorialUnpackState?
    package var checks: [TutorialOptionalCheckState]
    package var phase: TutorialAcquisitionPhase
    package var staged: Bool
    package var currentGeneration: UInt64
    package var pinnedSha256: String?
    package var attempts: [TutorialDatasetAttemptState]

    package var currentAttempt: TutorialDatasetAttemptState? {
        attempts.last { $0.generation == currentGeneration }
    }
}

package struct TutorialLockState: Codable, Equatable {
    package var schemaVersion: UInt32
    package var registryVersion: UInt32
    package var notebookId: String
    package var notebookFilename: String
    package var tutorialId: String
    package var title: String
    package var templateSha256: String
    package var sections: [TutorialSectionState]
    package var datasets: [TutorialDatasetState]
}

package struct TutorialProjectState: Codable, Equatable {
    package var notebook: NotebookDocumentState
    package var tutorial: TutorialLockState
}

package struct TutorialAcquisitionPlanState: Codable, Equatable {
    package var approvalSha256: String
    package var registryVersion: UInt32
    package var notebookId: String
    package var datasetId: String
    package var scheme: String
    package var requestedUri: String
    package var resolvedUri: String
    package var redirects: [String]
    package var expectedSizeBytes: UInt64?
    package var resolvedSizeBytes: UInt64?
    package var destination: String
    package var expectedSha256: String?
    package var requiredDiskBytes: UInt64
    package var availableDiskBytes: UInt64
    package var unpack: TutorialUnpackState?
    package var checks: [TutorialOptionalCheckState]
    package var missingDigest: Bool

    package var hasEnoughDisk: Bool { availableDiskBytes >= requiredDiskBytes }
}

package struct TutorialAcquisitionApprovalState: Codable, Equatable {
    package var approvalSha256: String
    package var allowMissingDigest: Bool
    package var skippedCheckIds: [String]
}

package protocol TutorialPersistenceClient {
    func list(projectRoot: String) throws -> [TutorialProjectState]
    func fork(projectRoot: String, templatePath: String, filename: String) throws -> TutorialProjectState
    func migrate(packPath: String, destination: String) throws
    func plan(
        projectRoot: String,
        notebookID: String,
        datasetID: String,
        sourceOverride: String?
    ) throws -> TutorialAcquisitionPlanState
    func begin(
        projectRoot: String,
        plan: TutorialAcquisitionPlanState,
        approval: TutorialAcquisitionApprovalState
    ) throws -> TutorialDatasetState
    func action(
        _ action: TutorialPersistenceAction,
        projectRoot: String,
        notebookID: String,
        datasetID: String,
        generation: UInt64?
    ) throws -> TutorialDatasetState
}

package enum TutorialPersistenceAction {
    case resume, restart, retry, cancel, advance
}

package struct UniFFITutorialPersistenceClient: TutorialPersistenceClient {
    private let encoder: JSONEncoder
    private let decoder: JSONDecoder

    package init() {
        encoder = JSONEncoder()
        encoder.keyEncodingStrategy = .convertToSnakeCase
        decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
    }

    package func list(projectRoot: String) throws -> [TutorialProjectState] {
        try decode(CasarsFrontendServices.tutorialProjectListJson(projectRoot: projectRoot))
    }

    package func fork(
        projectRoot: String,
        templatePath: String,
        filename: String
    ) throws -> TutorialProjectState {
        try call(
            TutorialForkRequest(projectRoot: projectRoot, templatePath: templatePath, filename: filename),
            CasarsFrontendServices.tutorialForkJson
        )
    }

    package func migrate(packPath: String, destination: String) throws {
        _ = try call(
            TutorialMigrateRequest(packPath: packPath, destination: destination),
            CasarsFrontendServices.tutorialMigrateV0Json
        ) as TutorialTemplateState
    }

    package func plan(
        projectRoot: String,
        notebookID: String,
        datasetID: String,
        sourceOverride: String?
    ) throws -> TutorialAcquisitionPlanState {
        try call(
            TutorialPlanRequest(
                projectRoot: projectRoot,
                notebookId: notebookID,
                datasetId: datasetID,
                sourceOverride: sourceOverride
            ),
            CasarsFrontendServices.tutorialPlanAcquisitionJson
        )
    }

    package func begin(
        projectRoot: String,
        plan: TutorialAcquisitionPlanState,
        approval: TutorialAcquisitionApprovalState
    ) throws -> TutorialDatasetState {
        try call(
            TutorialBeginRequest(projectRoot: projectRoot, plan: plan, approval: approval),
            CasarsFrontendServices.tutorialBeginAcquisitionJson
        )
    }

    package func action(
        _ action: TutorialPersistenceAction,
        projectRoot: String,
        notebookID: String,
        datasetID: String,
        generation: UInt64?
    ) throws -> TutorialDatasetState {
        let request = TutorialActionRequest(
            projectRoot: projectRoot,
            notebookId: notebookID,
            datasetId: datasetID,
            generation: generation,
            maxDownloadBytes: action == .advance ? 1_048_576 : nil
        )
        switch action {
        case .resume: return try call(request, CasarsFrontendServices.tutorialResumeAcquisitionJson)
        case .restart: return try call(request, CasarsFrontendServices.tutorialRestartAcquisitionJson)
        case .retry: return try call(request, CasarsFrontendServices.tutorialRetryAcquisitionJson)
        case .cancel: return try call(request, CasarsFrontendServices.tutorialCancelAcquisitionJson)
        case .advance: return try call(request, CasarsFrontendServices.tutorialAdvanceAcquisitionJson)
        }
    }

    private func call<Request: Encodable, Response: Decodable>(
        _ request: Request,
        _ operation: (String) throws -> String
    ) throws -> Response {
        try decode(operation(String(decoding: try encoder.encode(request), as: UTF8.self)))
    }

    private func decode<Response: Decodable>(_ json: String) throws -> Response {
        try decoder.decode(Response.self, from: Data(json.utf8))
    }
}

private struct TutorialForkRequest: Encodable {
    var projectRoot: String
    var templatePath: String
    var filename: String
}

private struct TutorialMigrateRequest: Encodable {
    var packPath: String
    var destination: String
}

private struct TutorialPlanRequest: Encodable {
    var projectRoot: String
    var notebookId: String
    var datasetId: String
    var sourceOverride: String?
}

private struct TutorialBeginRequest: Encodable {
    var projectRoot: String
    var plan: TutorialAcquisitionPlanState
    var approval: TutorialAcquisitionApprovalState
}

private struct TutorialActionRequest: Encodable {
    var projectRoot: String
    var notebookId: String
    var datasetId: String
    var generation: UInt64?
    var maxDownloadBytes: UInt64?
}

private struct TutorialTemplateState: Decodable {
    var root: String
    var contentSha256: String
}
