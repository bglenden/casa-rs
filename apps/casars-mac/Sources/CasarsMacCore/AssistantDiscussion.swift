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

/// Deterministically shares one bounded context window across every open tab.
/// Four or fewer tabs may contribute up to 16 KiB each; larger tab sets divide
/// the 64 KiB projection budget fairly instead of multiplying a per-tab cap.
package enum AssistantContextBudgetPolicy {
    package static let totalProjectionBytes = 64 * 1_024
    package static let maximumExcerptBytes = 16 * 1_024

    package static func excerptLimits(openTabCount: Int) -> [Int] {
        guard openTabCount > 0 else { return [] }
        let fairShare = totalProjectionBytes / openTabCount
        let remainder = totalProjectionBytes % openTabCount
        return (0..<openTabCount).map { index in
            min(maximumExcerptBytes, fairShare + (index < remainder ? 1 : 0))
        }
    }

    package static func truncate(_ value: String, byteLimit: Int) -> String {
        guard byteLimit > 0 else { return "" }
        guard value.utf8.count > byteLimit else { return value }
        let marker = "\n[… bounded by CASA-RS host …]"
        let markerBytes = marker.utf8.count
        guard byteLimit > markerBytes else { return utf8Prefix(value, byteLimit: byteLimit) }
        return utf8Prefix(value, byteLimit: byteLimit - markerBytes) + marker
    }

    private static func utf8Prefix(_ value: String, byteLimit: Int) -> String {
        var bytes = Array(value.utf8.prefix(byteLimit))
        while !bytes.isEmpty {
            if let prefix = String(bytes: bytes, encoding: .utf8) { return prefix }
            bytes.removeLast()
        }
        return ""
    }
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
        removeMissingLayers: Set<String>,
        projectSources: [AssistantProjectCorpusSourceRequest]?,
        failedProjectSources: Set<String>
    ) throws -> AssistantCorpusIndexReportState
    func projectCorpusPlan(
        projectRoot: String,
        sources: [AssistantProjectCorpusSourceRequest]
    ) throws -> AssistantProjectCorpusPlanState
    func searchCorpus(projectRoot: String, query: String, limit: Int) throws -> [AssistantCorpusSearchHitState]
    func createPin(_ request: AssistantCreatePinRequest) throws -> AssistantPinState
}

extension AssistantPersistenceClient {
    package func indexCorpus(
        projectRoot: String,
        documents: [AssistantCorpusDocumentRequest],
        removeMissingLayers: Set<String>
    ) throws -> AssistantCorpusIndexReportState {
        try indexCorpus(
            projectRoot: projectRoot,
            documents: documents,
            removeMissingLayers: removeMissingLayers,
            projectSources: nil,
            failedProjectSources: []
        )
    }
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
        removeMissingLayers: Set<String>,
        projectSources: [AssistantProjectCorpusSourceRequest]?,
        failedProjectSources: Set<String>
    ) throws -> AssistantCorpusIndexReportState {
        let request = AssistantCorpusIndexRequest(
            projectRoot: projectRoot,
            documents: documents,
            removeMissingLayers: removeMissingLayers.sorted(),
            projectSources: projectSources,
            failedProjectSources: failedProjectSources.sorted()
        )
        return try CasarsFrontendServices.assistantCorpusIndex(request: request)
    }

    package func projectCorpusPlan(
        projectRoot: String,
        sources: [AssistantProjectCorpusSourceRequest]
    ) throws -> AssistantProjectCorpusPlanState {
        let request = AssistantProjectCorpusPlanRequest(
            projectRoot: projectRoot,
            sources: sources
        )
        return try CasarsFrontendServices.assistantProjectCorpusPlan(request: request)
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
