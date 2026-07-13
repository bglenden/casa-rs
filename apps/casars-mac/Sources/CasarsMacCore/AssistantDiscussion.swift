import CasarsFrontendServices
import Foundation

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

package enum AssistantAuthorityState: String, Codable, Equatable, CaseIterable, Identifiable {
    case explore
    case work
    case fullAccess

    package var id: String { rawValue }
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

package struct AssistantSessionProfileState: Codable, Equatable {
    package var profileVersion: UInt32 = 1
    package var backendId = "codex_app_server"
    package var authority: AssistantAuthorityState = .work
    package var model = ""
    package var effort = "medium"
    package var agentCommand = "codex"
    package var pythonCommand = "python3"
    package var pythonProvenance: AssistantPythonProvenanceState? = nil
}

package struct AssistantPythonProvenanceState: Codable, Equatable {
    package var selectedCommand: String
    package var resolvedPath: String
    package var implementation: String
    package var version: String
    package var environmentLabel: String
    package var casaRsVersion: String?
    package var packages: [String: String]
}

package struct AssistantBackendSessionState: Codable, Equatable {
    package var backendId: String
    package var sessionId: String
}

package struct AssistantAttachmentState: Codable, Equatable, Identifiable {
    package var kind: String
    package var identifier: String
    package var label: String
    package var primary: Bool
    package var id: String { "\(kind):\(identifier)" }
}

package struct AssistantCitationState: Codable, Equatable, Identifiable {
    package var id: String
    package var kind: String
    package var label: String
    package var locator: String
    package var excerpt: String
    package var sourcePath: String?
    package var page: UInt32?
    package var section: String?
    package var lineStart: UInt32?
    package var lineEnd: UInt32?
    package var release: String?
    package var commit: String?
}

package struct AssistantContextItemState: Codable, Equatable, Identifiable {
    package var id: String
    package var kind: String
    package var label: String
    package var summary: String
    package var excerpt: String
    package var byteCount: UInt64
    package var contentSha256: String
    package var untrustedEvidence: Bool
    package var selected: Bool = true

    private enum CodingKeys: String, CodingKey {
        case id, kind, label, summary, excerpt, byteCount, contentSha256, untrustedEvidence, selected
    }
}

extension AssistantContextItemState {
    package init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        id = try values.decode(String.self, forKey: .id)
        kind = try values.decode(String.self, forKey: .kind)
        label = try values.decode(String.self, forKey: .label)
        summary = try values.decode(String.self, forKey: .summary)
        excerpt = try values.decode(String.self, forKey: .excerpt)
        byteCount = try values.decode(UInt64.self, forKey: .byteCount)
        contentSha256 = try values.decode(String.self, forKey: .contentSha256)
        untrustedEvidence = try values.decode(Bool.self, forKey: .untrustedEvidence)
        selected = try values.decodeIfPresent(Bool.self, forKey: .selected) ?? true
    }
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

package struct AssistantActivityState: Codable, Equatable, Identifiable {
    package var id: String
    package var label: String
    package var state: String
    package var summary: String?
}

package struct AssistantTaskSuggestionState: Codable, Equatable, Identifiable {
    package var id: String
    package var taskId: String
    package var parameters: [String: String]
}

package struct AssistantPinState: Codable, Equatable, Identifiable {
    package var id: String
    package var conversationId: String
    package var notebookId: String
    package var messageId: String
    package var representation: String
    package var destination: String
    package var snapshotContent: String
    package var createdAt: UInt64
    package var contentSha256: String
}

package struct AssistantMessageState: Codable, Equatable, Identifiable {
    package var id: String
    package var role: String
    package var content: String
    package var createdAt: UInt64
    package var agentId: String?
    package var model: String?
    package var citations: [AssistantCitationState]
    package var usedContext: [AssistantContextItemState]
    package var activities: [AssistantActivityState]
    package var taskSuggestions: [AssistantTaskSuggestionState]
    package var pins: [AssistantPinState]
}

package struct AssistantConversationState: Codable, Equatable, Identifiable {
    package var schemaVersion: UInt32
    package var id: String
    package var title: String
    package var createdAt: UInt64
    package var updatedAt: UInt64
    package var profile: AssistantSessionProfileState
    package var backendSession: AssistantBackendSessionState?
    package var attachments: [AssistantAttachmentState]
    package var messages: [AssistantMessageState]
    package var draft: String
    package var selectedContextIds: [String]
    package var scrollAnchorMessageId: String?
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
    package var account = AssistantAccountState(email: nil, plan: nil, requiresLogin: true)
    package var usage = AssistantUsageState()
    package var streamingText = ""
    package var activeTurnID: String?
    package var pendingApproval: AssistantApprovalRequestState?
    package var lastError: String?
    package var corpusStatus = "Not indexed"
    package var pendingAuthenticationURL: String?

    package var activeConversation: AssistantConversationState? {
        conversations.first { $0.id == activeConversationID }
    }
}

package struct AssistantCorpusDocumentRequest: Encodable {
    package var id: String
    package var layer: String
    package var title: String
    package var sourceIdentity: String
    package var content: String
    package var citation: AssistantCorpusCitationRequest
    package var redistributionCleared: Bool
}

package struct AssistantCorpusCitationRequest: Codable, Equatable {
    package var label: String
    package var locator: String
    package var sourcePath: String?
    package var page: UInt32?
    package var section: String?
    package var lineStart: UInt32?
    package var lineEnd: UInt32?
    package var release: String?
    package var commit: String?
}

package struct AssistantCorpusSearchHitState: Codable, Equatable {
    package var chunkId: String
    package var documentId: String
    package var layer: String
    package var title: String
    package var text: String
    package var score: Float
    package var citation: AssistantCorpusCitationRequest
    package var untrustedEvidence: Bool
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
    ) throws -> String
    func searchCorpus(projectRoot: String, query: String, limit: Int) throws -> [AssistantCorpusSearchHitState]
    func createPin(_ request: AssistantCreatePinEnvelope) throws -> AssistantPinState
}

package struct UniFFIAssistantPersistenceClient: AssistantPersistenceClient {
    private let encoder: JSONEncoder
    private let decoder: JSONDecoder

    package init() {
        encoder = JSONEncoder()
        encoder.keyEncodingStrategy = .convertToSnakeCase
        decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
    }

    package func conversations(projectRoot: String) throws -> [AssistantConversationState] {
        let json = try CasarsFrontendServices.assistantConversationsJson(projectRoot: projectRoot)
        return try decoder.decode([AssistantConversationState].self, from: Data(json.utf8))
    }

    package func createConversation(
        projectRoot: String,
        title: String,
        attachment: AssistantAttachmentState,
        profile: AssistantSessionProfileState
    ) throws -> AssistantConversationState {
        let request = AssistantCreateConversationEnvelope(
            projectRoot: projectRoot,
            title: title,
            primaryAttachment: attachment,
            profile: profile
        )
        let json = try CasarsFrontendServices.assistantCreateConversationJson(
            requestJson: String(decoding: try encoder.encode(request), as: UTF8.self)
        )
        return try decoder.decode(AssistantConversationState.self, from: Data(json.utf8))
    }

    package func saveConversation(projectRoot: String, transcript: AssistantConversationState) throws {
        let request = AssistantSaveConversationEnvelope(projectRoot: projectRoot, transcript: transcript)
        try CasarsFrontendServices.assistantSaveConversationJson(
            requestJson: String(decoding: try encoder.encode(request), as: UTF8.self)
        )
    }

    package func indexCorpus(
        projectRoot: String,
        documents: [AssistantCorpusDocumentRequest],
        removeMissingLayers: Set<String>
    ) throws -> String {
        let request = AssistantCorpusIndexEnvelope(
            projectRoot: projectRoot,
            documents: documents,
            removeMissingLayers: removeMissingLayers
        )
        return try CasarsFrontendServices.assistantCorpusIndexJson(
            requestJson: String(decoding: try encoder.encode(request), as: UTF8.self)
        )
    }

    package func searchCorpus(
        projectRoot: String,
        query: String,
        limit: Int
    ) throws -> [AssistantCorpusSearchHitState] {
        let request = AssistantCorpusSearchEnvelope(projectRoot: projectRoot, query: query, limit: limit)
        let json = try CasarsFrontendServices.assistantCorpusSearchJson(
            requestJson: String(decoding: try encoder.encode(request), as: UTF8.self)
        )
        return try decoder.decode([AssistantCorpusSearchHitState].self, from: Data(json.utf8))
    }

    package func createPin(_ request: AssistantCreatePinEnvelope) throws -> AssistantPinState {
        let json = try CasarsFrontendServices.assistantCreatePinJson(
            requestJson: String(decoding: try encoder.encode(request), as: UTF8.self)
        )
        return try decoder.decode(AssistantPinState.self, from: Data(json.utf8))
    }
}

private struct AssistantCreateConversationEnvelope: Encodable {
    var projectRoot: String
    var title: String
    var primaryAttachment: AssistantAttachmentState
    var profile: AssistantSessionProfileState
}

private struct AssistantSaveConversationEnvelope: Encodable {
    var projectRoot: String
    var transcript: AssistantConversationState
}

private struct AssistantCorpusIndexEnvelope: Encodable {
    var projectRoot: String
    var documents: [AssistantCorpusDocumentRequest]
    var removeMissingLayers: Set<String>
}

private struct AssistantCorpusSearchEnvelope: Encodable {
    var projectRoot: String
    var query: String
    var limit: Int
}

package struct AssistantCreatePinEnvelope: Encodable {
    package var conversationId: String
    package var notebookId: String
    package var messageId: String
    package var representation: String
    package var snapshotContent: String
}
