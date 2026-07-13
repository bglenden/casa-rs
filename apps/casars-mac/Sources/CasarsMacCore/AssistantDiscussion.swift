import CasarsFrontendServices
import CryptoKit
import Darwin
import Foundation
import Security

private func assistantUTF8Prefix(_ value: String, maximumBytes: Int) -> String {
    guard value.utf8.count > maximumBytes else { return value }
    var byteCount = 0
    var end = value.startIndex
    while end < value.endIndex {
        let next = value.index(after: end)
        let bytes = value[end..<next].utf8.count
        guard byteCount + bytes <= maximumBytes else { break }
        byteCount += bytes
        end = next
    }
    return String(value[..<end])
}

package enum AssistantDiscussionPresentation: String, Codable, Equatable {
    case closed
    case drawer
    case tab
}

package enum AssistantDiscussionActivity: String, Codable, Equatable {
    case unavailable
    case starting
    case ready
    case authenticating
    case streaming
    case completed
    case failed
    case cancelled
    case restartRequired
}

package struct AssistantProviderModelState: Codable, Equatable, Identifiable {
    package var id: String
    package var label: String
    package var contextWindow: UInt64
    package var supportsImages: Bool
    package var supportsTools: Bool
}

package struct AssistantProviderState: Codable, Equatable, Identifiable {
    package var id: String
    package var label: String
    package var authentication: String
    package var configured: Bool
    package var models: [AssistantProviderModelState]
}

package struct AssistantProviderCatalogState: Codable, Equatable {
    package var protocolVersion: UInt32
    package var providers: [AssistantProviderState]
}

package struct AssistantAttachmentState: Codable, Equatable {
    package var kind: String
    package var identifier: String
    package var label: String
    package var primary: Bool
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
    package var providerVisible: Bool
    package var untrustedEvidence: Bool

    package init(
        id: String,
        kind: String,
        label: String,
        summary: String,
        excerpt: String,
        providerVisible: Bool,
        untrustedEvidence: Bool
    ) {
        self.id = id
        self.kind = kind
        self.label = label
        self.summary = summary
        self.excerpt = excerpt
        byteCount = UInt64(excerpt.utf8.count)
        contentSha256 = SHA256.hash(data: Data(excerpt.utf8))
            .map { String(format: "%02x", $0) }
            .joined()
        self.providerVisible = providerVisible
        self.untrustedEvidence = untrustedEvidence
    }
}

package struct AssistantEgressState: Codable, Equatable {
    package var provider: String
    package var model: String
    package var destination: String
    package var items: [AssistantContextItemState]
    package var estimatedBytes: UInt64

    package static func providerBound(
        provider: String,
        model: String,
        destination: String,
        contexts: [AssistantContextItemState]
    ) -> Self {
        let items = boundedProviderItems(contexts)
        return Self(
            provider: provider,
            model: model,
            destination: destination,
            items: items,
            estimatedBytes: items.reduce(0) { $0 + $1.byteCount }
        )
    }

    /// Applies one host-owned budget before context crosses the provider
    /// boundary. The per-item floor preserves a useful excerpt when many tabs
    /// are open, while the ceiling prevents one surface from consuming the
    /// whole turn budget.
    package static func boundedProviderItems(
        _ contexts: [AssistantContextItemState],
        maximumBytes: Int = 65_536
    ) -> [AssistantContextItemState] {
        let visible = contexts.filter(\.providerVisible)
        guard !visible.isEmpty, maximumBytes > 0 else { return [] }
        let perItem = min(16_384, max(512, maximumBytes / visible.count))
        var remaining = maximumBytes
        return visible.compactMap { item in
            guard remaining > 0 else { return nil }
            let budget = min(perItem, remaining)
            let excerpt = assistantUTF8Prefix(item.excerpt, maximumBytes: budget)
            remaining -= excerpt.utf8.count
            return AssistantContextItemState(
                id: item.id,
                kind: item.kind,
                label: item.label,
                summary: item.summary,
                excerpt: excerpt,
                providerVisible: true,
                untrustedEvidence: item.untrustedEvidence
            )
        }
    }

}

package struct AssistantApprovalState: Codable, Equatable {
    package var proposalSha256: String
    package var approvedAt: UInt64
    package var authority: String
}

package struct AssistantExecutableState: Codable, Equatable {
    package var path: String
    package var version: String
    package var sha256: String
}

package struct AssistantExecutionBindingState: Codable, Equatable {
    package var operationType: String
    package var canonicalParameters: JSONValue
    package var exactSource: String?
    package var inputPaths: [String]
    package var outputPaths: [String]
    package var workingDirectory: String
    package var executable: AssistantExecutableState
}

package struct AssistantProposalDestinationState: Codable, Equatable {
    package var surface: String
    package var identifier: String
    package var position: String
}

package struct AssistantInsertionBindingState: Codable, Equatable {
    package var destination: AssistantProposalDestinationState
    package var exactContent: String
    package var contentSha256: String
}

package struct AssistantProposalState: Codable, Equatable, Identifiable {
    package var id: String
    package var kind: String
    package var title: String
    package var authority: String
    package var payload: JSONValue
    package var execution: AssistantExecutionBindingState
    package var insertion: AssistantInsertionBindingState
    package var payloadSha256: String
    package var state: String
    package var approval: AssistantApprovalState?
    package var insertionApproval: AssistantApprovalState?
    package var affectedPaths: [String]
    package var result: String?
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
    package var provider: String?
    package var model: String?
    package var citations: [AssistantCitationState]
    package var egress: AssistantEgressState?
    package var proposals: [AssistantProposalState]
    package var pins: [AssistantPinState]
}

/// Minimal visible transcript projection allowed to cross into the untrusted
/// provider sidecar. Durable receipts, context manifests, proposals, and pins
/// remain host-only.
package struct AssistantProviderMessageState: Encodable, Equatable {
    package var id: String
    package var role: String
    package var content: String
    package var createdAt: UInt64
    package var provider: String?
    package var model: String?

    package init(message: AssistantMessageState) {
        id = message.id
        role = message.role
        content = message.content
        createdAt = message.createdAt
        provider = message.provider
        model = message.model
    }

    package static func providerBound(
        _ messages: [AssistantMessageState],
        maximumBytes: Int = 262_144,
        maximumMessages: Int = 128
    ) -> [Self] {
        var remaining = maximumBytes
        var projected: [Self] = []
        for message in messages.suffix(maximumMessages).reversed() {
            guard remaining > 0 else { break }
            var item = Self(message: message)
            item.content = assistantUTF8Prefix(message.content, maximumBytes: min(65_536, remaining))
            remaining -= item.content.utf8.count
            projected.append(item)
        }
        return Array(projected.reversed())
    }
}

package struct AssistantConversationState: Codable, Equatable, Identifiable {
    package var schemaVersion: UInt32
    package var id: String
    package var title: String
    package var createdAt: UInt64
    package var updatedAt: UInt64
    package var provider: String
    package var model: String
    package var attachments: [AssistantAttachmentState]
    package var messages: [AssistantMessageState]
    package var draft: String
    package var selectedContextIds: [String]
    package var scrollAnchorMessageId: String?
}

package struct AssistantDiscussionState: Codable, Equatable {
    package var presentation: AssistantDiscussionPresentation = .closed
    package var activity: AssistantDiscussionActivity = .unavailable
    package var conversations: [AssistantConversationState] = []
    package var activeConversationID: String?
    package var providers: [AssistantProviderState] = []
    package var contexts: [AssistantContextItemState] = []
    package var streamingText = ""
    package var lastError: String?
    package var corpusStatus = "Not indexed"
    package var pendingAuthenticationURL: String?
    package var pendingAuthenticationInstructions: String?
    package var pendingAuthenticationPrompt: AssistantAuthenticationPromptState?
    package var notebookSuggestionFocusGeneration = 0

    package var activeConversation: AssistantConversationState? {
        conversations.first { $0.id == activeConversationID }
    }

    package var selectedProvider: AssistantProviderState? {
        guard let conversation = activeConversation else { return providers.first }
        return providers.first { $0.id == conversation.provider }
    }
}

package struct AssistantAuthenticationPromptState: Codable, Equatable, Identifiable {
    package var requestID: String
    package var promptID: String
    package var message: String
    package var secret: Bool
    package var id: String { "\(requestID):\(promptID)" }
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
    package var cosineScore: Float
    package var keywordScore: Float
    package var citation: AssistantCorpusCitationRequest
    package var untrustedEvidence: Bool
}

package protocol AssistantPersistenceClient {
    func conversations(projectRoot: String) throws -> [AssistantConversationState]
    func createConversation(
        projectRoot: String,
        title: String,
        attachment: AssistantAttachmentState,
        provider: String,
        model: String
    ) throws -> AssistantConversationState
    func saveConversation(projectRoot: String, transcript: AssistantConversationState) throws
    func indexCorpus(
        projectRoot: String,
        documents: [AssistantCorpusDocumentRequest],
        removeMissingLayers: Set<String>
    ) throws -> String
    func searchCorpus(projectRoot: String, query: String, limit: Int) throws -> [AssistantCorpusSearchHitState]
    func createProposal(_ request: AssistantCreateProposalEnvelope) throws -> AssistantProposalState
    func createPin(_ request: AssistantCreatePinEnvelope) throws -> AssistantPinState
    func approveProposalInsertion(
        proposal: AssistantProposalState,
        authority: String
    ) throws -> AssistantProposalState
    func approveProposalExecution(
        proposal: AssistantProposalState,
        authority: String
    ) throws -> AssistantProposalState
    func rejectProposal(
        proposal: AssistantProposalState,
        authority: String
    ) throws -> AssistantProposalState
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
        provider: String,
        model: String
    ) throws -> AssistantConversationState {
        let request = AssistantCreateConversationEnvelope(
            projectRoot: projectRoot,
            title: title,
            primaryAttachment: attachment,
            provider: provider,
            model: model
        )
        let json = try CasarsFrontendServices.assistantCreateConversationJson(
            requestJson: String(decoding: try encoder.encode(request), as: UTF8.self)
        )
        return try decoder.decode(AssistantConversationState.self, from: Data(json.utf8))
    }

    package func saveConversation(projectRoot: String, transcript: AssistantConversationState) throws {
        let request = AssistantSaveConversationEnvelope(
            projectRoot: projectRoot,
            transcript: transcript
        )
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
        let request = AssistantCorpusSearchEnvelope(
            projectRoot: projectRoot,
            query: query,
            limit: limit
        )
        let json = try CasarsFrontendServices.assistantCorpusSearchJson(
            requestJson: String(decoding: try encoder.encode(request), as: UTF8.self)
        )
        return try decoder.decode([AssistantCorpusSearchHitState].self, from: Data(json.utf8))
    }

    package func createProposal(
        _ request: AssistantCreateProposalEnvelope
    ) throws -> AssistantProposalState {
        let json = try CasarsFrontendServices.assistantCreateProposalJson(
            requestJson: String(decoding: try encoder.encode(request), as: UTF8.self)
        )
        return try decoder.decode(AssistantProposalState.self, from: Data(json.utf8))
    }

    package func createPin(_ request: AssistantCreatePinEnvelope) throws -> AssistantPinState {
        let json = try CasarsFrontendServices.assistantCreatePinJson(
            requestJson: String(decoding: try encoder.encode(request), as: UTF8.self)
        )
        return try decoder.decode(AssistantPinState.self, from: Data(json.utf8))
    }

    package func approveProposalInsertion(
        proposal: AssistantProposalState,
        authority: String
    ) throws -> AssistantProposalState {
        let request = AssistantApproveProposalEnvelope(proposal: proposal, authority: authority)
        let json = try CasarsFrontendServices.assistantApproveProposalInsertionJson(
            requestJson: String(decoding: try encoder.encode(request), as: UTF8.self)
        )
        return try decoder.decode(AssistantProposalState.self, from: Data(json.utf8))
    }

    package func approveProposalExecution(
        proposal: AssistantProposalState,
        authority: String
    ) throws -> AssistantProposalState {
        let request = AssistantApproveProposalEnvelope(proposal: proposal, authority: authority)
        let json = try CasarsFrontendServices.assistantApproveProposalExecutionJson(
            requestJson: String(decoding: try encoder.encode(request), as: UTF8.self)
        )
        return try decoder.decode(AssistantProposalState.self, from: Data(json.utf8))
    }

    package func rejectProposal(
        proposal: AssistantProposalState,
        authority: String
    ) throws -> AssistantProposalState {
        let request = AssistantApproveProposalEnvelope(proposal: proposal, authority: authority)
        let json = try CasarsFrontendServices.assistantRejectProposalJson(
            requestJson: String(decoding: try encoder.encode(request), as: UTF8.self)
        )
        return try decoder.decode(AssistantProposalState.self, from: Data(json.utf8))
    }

}

private struct AssistantCreateConversationEnvelope: Encodable {
    var projectRoot: String
    var title: String
    var primaryAttachment: AssistantAttachmentState
    var provider: String
    var model: String
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

package struct AssistantCreateProposalEnvelope: Encodable {
    package var kind: String
    package var title: String
    package var authority: String
    package var payload: JSONValue
    package var execution: AssistantExecutionBindingState
    package var insertion: AssistantInsertionBindingState
    package var affectedPaths: [String]
}

package struct AssistantCreatePinEnvelope: Encodable {
    package var conversationId: String
    package var notebookId: String
    package var messageId: String
    package var representation: String
    package var destination: String
    package var snapshotContent: String
}

private struct AssistantApproveProposalEnvelope: Encodable {
    var proposal: AssistantProposalState
    var authority: String
}

package protocol AssistantCredentialVault {
    func load(provider: String) throws -> AssistantCredentialLeaseState?
    func save(_ lease: AssistantCredentialLeaseState) throws
    func delete(provider: String) throws
}

package struct AssistantCredentialLeaseState: Codable, Equatable {
    package var provider: String
    package var credentialType: String
    package var secret: String
    package var expiresAt: UInt64?
}

package enum AssistantCredentialVaultError: Error {
    case keychain(OSStatus)
    case invalidData
}

package struct KeychainAssistantCredentialVault: AssistantCredentialVault {
    private let service = "org.casa-rs.casars-mac.assistant"

    package init() {}

    package func load(provider: String) throws -> AssistantCredentialLeaseState? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: provider,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne,
        ]
        var item: CFTypeRef?
        let status = SecItemCopyMatching(query as CFDictionary, &item)
        if status == errSecItemNotFound { return nil }
        guard status == errSecSuccess else { throw AssistantCredentialVaultError.keychain(status) }
        guard let data = item as? Data else { throw AssistantCredentialVaultError.invalidData }
        return try JSONDecoder().decode(AssistantCredentialLeaseState.self, from: data)
    }

    package func save(_ lease: AssistantCredentialLeaseState) throws {
        let data = try JSONEncoder().encode(lease)
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: lease.provider,
        ]
        let attributes: [String: Any] = [
            kSecValueData as String: data,
            kSecAttrAccessible as String: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly,
        ]
        let update = SecItemUpdate(query as CFDictionary, attributes as CFDictionary)
        if update == errSecSuccess { return }
        guard update == errSecItemNotFound else { throw AssistantCredentialVaultError.keychain(update) }
        var add = query
        attributes.forEach { add[$0.key] = $0.value }
        let status = SecItemAdd(add as CFDictionary, nil)
        guard status == errSecSuccess else { throw AssistantCredentialVaultError.keychain(status) }
    }

    package func delete(provider: String) throws {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: provider,
        ]
        let status = SecItemDelete(query as CFDictionary)
        guard status == errSecSuccess || status == errSecItemNotFound else {
            throw AssistantCredentialVaultError.keychain(status)
        }
    }
}
