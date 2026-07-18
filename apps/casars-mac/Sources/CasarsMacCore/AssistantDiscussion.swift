import CasarsFrontendServices
import Foundation

extension AssistantAuthorityState: Identifiable {
    public var id: String {
        switch self {
        case .explore: "explore"
        case .work: "work"
        case .fullAccess: "full_access"
        }
    }

    package var label: String {
        switch self {
        case .explore: "Explore"
        case .work: "Work"
        case .fullAccess: "Full access"
        }
    }

    package var codexSettings: (sandbox: String, approvalPolicy: String) {
        switch self {
        case .explore: ("read-only", "never")
        case .work: ("workspace-write", "on-request")
        case .fullAccess: ("danger-full-access", "never")
        }
    }
}

extension AssistantSessionProfileState {
    package init() {
        self.init(
            profileVersion: 1,
            backendId: "codex_app_server",
            authority: .work,
            model: "",
            effort: "medium",
            agentCommand: "codex",
            pythonCommand: "python3",
            pythonProvenance: nil
        )
    }
}

extension AssistantAttachmentState: Identifiable {
    public var id: String { "\(kind):\(identifier)" }
}

extension AssistantCitationState: Identifiable {}

extension AssistantContextItemState: Identifiable {}

extension AssistantActivityState: Identifiable {}

extension AssistantTaskSuggestionState: Identifiable {
    package init(id: String, taskId: String, parameters: [String: String]) {
        self.init(id: id, taskId: taskId, parameters: parameters, validatedPatch: nil)
    }
}

extension AssistantPinState: Identifiable {}
extension AssistantMessageState: Identifiable {}
extension AssistantConversationState: Identifiable {}

package enum AssistantDiscussionPresentation: String, Codable, Equatable {
    case closed
    case drawer
    case tab
}

package enum AssistantDiscussionActivity: String, Codable, Equatable {
    case unavailable
    case starting
    case ready
    case streaming
    case completed
    case restartRequired
}

package struct AssistantModelState: Codable, Equatable, Identifiable {
    package var id: String
    package var label: String
    package var defaultEffort: String
    package var supportedEfforts: [String]
    package var isDefault: Bool
    package var inputCapacityUnits: UInt64? = nil
    package var outputReserveUnits: UInt64? = nil
}

package struct AssistantAccountState: Codable, Equatable {
    package var email: String?
    package var plan: String?
    package var requiresLogin: Bool
}

package struct AssistantUsageState: Codable, Equatable {
    package var primaryPercentUsed: Double? = nil
    package var secondaryPercentUsed: Double? = nil
    package var primaryResetAt: UInt64? = nil
    package var secondaryResetAt: UInt64? = nil
}

package struct AssistantApprovalRequestState: Codable, Equatable, Identifiable {
    package var id: String
    package var method: String
    package var summary: String
}

package struct AssistantDiscussionState: Codable, Equatable {
    package var presentation: AssistantDiscussionPresentation = .closed
    package var activity: AssistantDiscussionActivity = .unavailable
    package var conversations: [AssistantConversationState] = []
    package var activeConversationID: String?
    package var models: [AssistantModelState] = []
    package var contexts: [AssistantContextItemState] = []
    package var selectedContextIDs: Set<String> = []
    package var account = AssistantAccountState(email: nil, plan: nil, requiresLogin: true)
    package var usage = AssistantUsageState()
    package var streamingText = ""
    /// Ephemeral, user-visible progress from actual App Server events. This is
    /// deliberately distinct from hidden model reasoning, which the host does
    /// not request or persist.
    package var liveActivity: AssistantActivityState?
    package var lastActivityAt: UInt64?
    package var activeTurnID: String?
    package var pendingApproval: AssistantApprovalRequestState?
    package var lastError: String?
    package var corpusStatus = "Not indexed"
    package var corpusIndexReport: AssistantCorpusIndexReportState?
    package var corpusDiagnostics: [String] = []
    package var pendingAuthenticationURL: String?

    package var activeConversation: AssistantConversationState? {
        conversations.first { $0.id == activeConversationID }
    }

    package var selectedContexts: [AssistantContextItemState] {
        contexts.filter { selectedContextIDs.contains($0.id) }
    }
}

package struct AssistantCorpusRefreshMetricsState: Codable, Equatable {
    package var projectMetadataReads = 0
    package var projectContentReads = 0
    package var projectPDFExtractions = 0
    package var projectOCRCalls = 0
}

package protocol AssistantPersistenceClient {
    func conversations(projectRoot: String) throws -> [AssistantConversationState]
    func createConversation(
        projectRoot: String,
        title: String,
        attachment: AssistantAttachmentState,
        profile: AssistantSessionProfileState
    ) throws -> AssistantConversationState
    func saveConversation(projectRoot: String, transcript: AssistantConversationState) throws
    func indexCorpus(
        projectRoot: String,
        documents: [AssistantCorpusDocumentRequest],
        removeMissingLayers: Set<String>
    ) throws -> AssistantCorpusIndexReportState
    func prepareCorpusReconciliation(
        projectRoot: String,
        sources: [AssistantProjectCorpusSourceRequest],
        generation: UInt64,
        scope: AssistantCorpusReconciliationScope
    ) throws -> AssistantPreparedCorpusReconciliationState
    func applyCorpusReconciliation(
        projectRoot: String,
        prepared: AssistantPreparedCorpusReconciliationState,
        documents: [AssistantCorpusDocumentRequest],
        removeMissingLayers: Set<String>,
        outcomes: [AssistantProjectSourceExtractionOutcome]
    ) throws -> AssistantCorpusIndexReportState
    func searchCorpus(projectRoot: String, query: String, limit: Int) throws -> [AssistantCorpusSearchHitState]
    func createPin(_ request: AssistantCreatePinRequest) throws -> AssistantPinState
}

package struct UniFFIAssistantPersistenceClient: AssistantPersistenceClient {
    package init() {}

    package func conversations(projectRoot: String) throws -> [AssistantConversationState] {
        try CasarsFrontendServices.assistantConversations(projectRoot: projectRoot)
    }

    package func createConversation(
        projectRoot: String,
        title: String,
        attachment: AssistantAttachmentState,
        profile: AssistantSessionProfileState
    ) throws -> AssistantConversationState {
        let request = AssistantCreateConversationRequest(
            projectRoot: projectRoot,
            title: title,
            primaryAttachment: attachment,
            profile: profile
        )
        return try CasarsFrontendServices.assistantCreateConversation(request: request)
    }

    package func saveConversation(projectRoot: String, transcript: AssistantConversationState) throws {
        try CasarsFrontendServices.assistantSaveConversation(request: AssistantSaveConversationRequest(
            projectRoot: projectRoot,
            transcript: transcript
        ))
    }

    package func indexCorpus(
        projectRoot: String,
        documents: [AssistantCorpusDocumentRequest],
        removeMissingLayers: Set<String>
    ) throws -> AssistantCorpusIndexReportState {
        let request = AssistantCorpusIndexRequest(
            projectRoot: projectRoot,
            documents: documents,
            removeMissingLayers: removeMissingLayers.sorted()
        )
        return try CasarsFrontendServices.assistantCorpusIndex(request: request)
    }

    package func prepareCorpusReconciliation(
        projectRoot: String,
        sources: [AssistantProjectCorpusSourceRequest],
        generation: UInt64,
        scope: AssistantCorpusReconciliationScope
    ) throws -> AssistantPreparedCorpusReconciliationState {
        let request = AssistantPrepareCorpusReconciliationRequest(
            projectRoot: projectRoot,
            sources: sources,
            generation: generation,
            scope: scope
        )
        return try CasarsFrontendServices.assistantPrepareCorpusReconciliation(request: request)
    }

    package func applyCorpusReconciliation(
        projectRoot: String,
        prepared: AssistantPreparedCorpusReconciliationState,
        documents: [AssistantCorpusDocumentRequest],
        removeMissingLayers: Set<String>,
        outcomes: [AssistantProjectSourceExtractionOutcome]
    ) throws -> AssistantCorpusIndexReportState {
        try CasarsFrontendServices.assistantApplyCorpusReconciliation(
            request: AssistantApplyCorpusReconciliationRequest(
                projectRoot: projectRoot,
                prepared: prepared,
                documents: documents,
                removeMissingLayers: removeMissingLayers.sorted(),
                outcomes: outcomes
            )
        )
    }

    package func searchCorpus(
        projectRoot: String,
        query: String,
        limit: Int
    ) throws -> [AssistantCorpusSearchHitState] {
        let request = AssistantCorpusSearchRequest(
            projectRoot: projectRoot,
            query: query,
            limit: UInt64(limit),
            layers: []
        )
        return try CasarsFrontendServices.assistantCorpusSearch(request: request)
    }

    package func createPin(_ request: AssistantCreatePinRequest) throws -> AssistantPinState {
        try CasarsFrontendServices.assistantCreatePin(request: request)
    }
}
