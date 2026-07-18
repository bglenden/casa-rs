import CasarsFrontendServices
import Foundation

extension TutorialAcquisitionPhase {
    package var rawValue: String {
        switch self {
        case .missing: "missing"
        case .downloading: "downloading"
        case .verifying: "verifying"
        case .unpacking: "unpacking"
        case .checking: "checking"
        case .materializing: "materializing"
        case .ready: "ready"
        case .cancelled: "cancelled"
        case .networkFailed: "network_failed"
        case .checksumFailed: "checksum_failed"
        case .unsafeArchive: "unsafe_archive"
        case .destinationCollision: "destination_collision"
        }
    }

    package var isRunning: Bool {
        [.downloading, .verifying, .unpacking, .checking, .materializing].contains(self)
    }
}

extension TutorialSectionState: Identifiable {}
extension TutorialOptionalCheckState: Identifiable {}

extension TutorialCheckOutcomeState: Identifiable {
    public var id: String { checkId }
}

extension TutorialDatasetAttemptState: Identifiable {
    public var id: UInt64 { generation }
}

extension TutorialDatasetState: Identifiable {
    package var currentAttempt: TutorialDatasetAttemptState? {
        attempts.last { $0.generation == currentGeneration }
    }
}

package struct TutorialProjectState: Codable, Equatable {
    package var notebook: NotebookDocumentState
    package var tutorial: TutorialLockState

    package init(projection: TutorialProjectProjection) {
        notebook = NotebookDocumentState(projection: projection.notebook)
        tutorial = projection.tutorial
    }
}

extension TutorialAcquisitionPlanState {
    package var hasEnoughDisk: Bool { availableDiskBytes >= requiredDiskBytes }
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

package struct UniFFITutorialPersistenceClient: TutorialPersistenceClient {
    package init() {}

    package func list(projectRoot: String) throws -> [TutorialProjectState] {
        try CasarsFrontendServices.tutorialProjectList(projectRoot: projectRoot)
            .map(TutorialProjectState.init(projection:))
    }

    package func fork(
        projectRoot: String,
        templatePath: String,
        filename: String
    ) throws -> TutorialProjectState {
        TutorialProjectState(projection: try CasarsFrontendServices.tutorialFork(request: TutorialForkRequest(
            projectRoot: projectRoot,
            templatePath: templatePath,
            filename: filename
        )))
    }

    package func migrate(packPath: String, destination: String) throws {
        _ = try CasarsFrontendServices.tutorialMigrateV0(request: TutorialMigrateRequest(
            packPath: packPath,
            destination: destination
        ))
    }

    package func plan(
        projectRoot: String,
        notebookID: String,
        datasetID: String,
        sourceOverride: String?
    ) throws -> TutorialAcquisitionPlanState {
        try CasarsFrontendServices.tutorialPlanAcquisition(request: TutorialPlanRequest(
            projectRoot: projectRoot,
            notebookId: notebookID,
            datasetId: datasetID,
            sourceOverride: sourceOverride
        ))
    }

    package func begin(
        projectRoot: String,
        plan: TutorialAcquisitionPlanState,
        approval: TutorialAcquisitionApprovalState
    ) throws -> TutorialDatasetState {
        try CasarsFrontendServices.tutorialBeginAcquisition(request: TutorialBeginRequest(
            projectRoot: projectRoot,
            plan: plan,
            approval: approval
        ))
    }

    package func action(
        _ action: TutorialPersistenceAction,
        projectRoot: String,
        notebookID: String,
        datasetID: String,
        generation: UInt64?
    ) throws -> TutorialDatasetState {
        let request = TutorialActionRequest(
            action: action,
            projectRoot: projectRoot,
            notebookId: notebookID,
            datasetId: datasetID,
            generation: generation,
            maxDownloadBytes: action == .advance ? 1_048_576 : nil
        )
        return try CasarsFrontendServices.tutorialAcquisitionAction(request: request)
    }
}
