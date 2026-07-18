// SPDX-License-Identifier: LGPL-3.0-or-later

import Foundation

package protocol AssistantScheduling {
    func schedule(after delay: TimeInterval, _ action: @escaping () -> Void) -> DispatchWorkItem
}

package struct DispatchAssistantScheduler: AssistantScheduling {
    package func schedule(
        after delay: TimeInterval,
        _ action: @escaping () -> Void
    ) -> DispatchWorkItem {
        let item = DispatchWorkItem(block: action)
        DispatchQueue.main.asyncAfter(deadline: .now() + delay, execute: item)
        return item
    }
}

package protocol AssistantClock {
    func timestamp() -> UInt64
}

package struct SystemAssistantClock: AssistantClock {
    package func timestamp() -> UInt64 {
        UInt64(Date().timeIntervalSince1970 * 1_000)
    }
}

/// Assistant-owned transient state. Durable conversations remain owned by the
/// generated persistence boundary; Workbench receives only explicit host
/// effects such as opening a task or appending an approved notebook pin.
package struct AssistantFeatureState {
    package var conversationStartPending = false
    package var projectNonce = UUID().uuidString + UUID().uuidString
    package var pendingCitations: [AssistantCitationState] = []
    package var pendingActivities: [AssistantActivityState] = []
    package var pendingTaskSuggestions: [AssistantTaskSuggestionState] = []
    package var pendingStreamText = ""
    package var suggestedParameters: [String: Set<String>] = [:]
}

package enum AssistantControllerEffect: Equatable {
    case persistConversation
    case sendTurn(AgentTurnRequest)
    case openAuthenticationURL(String)
    case refreshAccount
    case restartConversation
    case scheduleStreamFlush
    case scheduleResponseTimeout(conversationID: String?)
}

package final class AssistantController {
    /// A response timeout is a UI liveness policy, not prompt or retrieval capacity.
    package static let responseLivenessTimeout: TimeInterval = 120
    package static let streamCoalescingDelay: TimeInterval = 0.05
    package static let draftPersistenceDelay: TimeInterval = 0.35

    package var state = AssistantFeatureState()
    package var session: AgentSession?
    package var activeAgentCommand: String?
    package var streamFlushWorkItem: DispatchWorkItem?
    package var responseTimeoutWorkItem: DispatchWorkItem?
    package var responseTimeout = responseLivenessTimeout
    package var draftSaveWorkItem: DispatchWorkItem?
    package let corpusCoordinator = AssistantCorpusReconciliationCoordinator()

    package let scheduler: AssistantScheduling
    package let clock: AssistantClock

    package init(
        scheduler: AssistantScheduling = DispatchAssistantScheduler(),
        clock: AssistantClock = SystemAssistantClock()
    ) {
        self.scheduler = scheduler
        self.clock = clock
    }

    package func resetSessionState() {
        state.conversationStartPending = false
        state.pendingCitations.removeAll()
        state.pendingActivities.removeAll()
        state.pendingTaskSuggestions.removeAll()
        state.pendingStreamText = ""
        streamFlushWorkItem?.cancel()
        streamFlushWorkItem = nil
        responseTimeoutWorkItem?.cancel()
        responseTimeoutWorkItem = nil
        draftSaveWorkItem?.cancel()
        draftSaveWorkItem = nil
        corpusCoordinator.reset()
    }

    package var timestamp: UInt64 { clock.timestamp() }

    package var projectNonce: String { state.projectNonce }

    package var mcpServerName: String { "casa_rs_\(state.projectNonce.prefix(12))" }

    package func replaceProjectNonce(_ nonce: String? = nil) {
        state.projectNonce = nonce ?? UUID().uuidString + UUID().uuidString
    }

    package func beginConversation() -> Bool {
        guard !state.conversationStartPending else { return false }
        state.conversationStartPending = true
        return true
    }

    package func cancelConversationStart() {
        state.conversationStartPending = false
    }

    package func beginPrompt() {
        streamFlushWorkItem?.cancel()
        streamFlushWorkItem = nil
        state.pendingStreamText = ""
        state.pendingCitations = []
        state.pendingActivities = []
        state.pendingTaskSuggestions = []
    }

    package func handle(
        _ event: AgentSessionEvent,
        discussion: inout AssistantDiscussionState
    ) -> [AssistantControllerEffect] {
        var effects: [AssistantControllerEffect] = []
        switch event {
        case let .conversationStarted(threadID):
            state.conversationStartPending = false
            updateActiveConversation(in: &discussion) {
                $0.backendSession = AssistantBackendSessionState(
                    backendId: "codex_app_server",
                    sessionId: threadID
                )
            }
            effects.append(.persistConversation)
            if discussion.activity == .streaming,
               let prompt = discussion.activeConversation?.messages.last?.content
            {
                effects.append(.sendTurn(AgentTurnRequest(
                    threadID: threadID,
                    text: prompt,
                    model: discussion.activeConversation?.profile.model ?? "",
                    effort: discussion.activeConversation?.profile.effort ?? "medium"
                )))
            }
        case let .models(models):
            discussion.models = models.map { model in
                AssistantModelState(
                    id: model.id,
                    label: model.label,
                    defaultEffort: model.defaultEffort,
                    supportedEfforts: model.supportedEfforts.isEmpty
                        ? ["low", "medium", "high"] : model.supportedEfforts,
                    isDefault: model.isDefault,
                    inputCapacityUnits: model.inputCapacityUnits,
                    outputReserveUnits: model.outputReserveUnits
                )
            }
        case let .account(account):
            discussion.account = AssistantAccountState(
                email: account.email,
                plan: account.plan,
                requiresLogin: account.requiresLogin
            )
        case let .usage(usage):
            if let plan = usage.plan { discussion.account.plan = plan }
            discussion.usage = AssistantUsageState(
                primaryPercentUsed: usage.primaryPercentUsed,
                secondaryPercentUsed: usage.secondaryPercentUsed,
                primaryResetAt: usage.primaryResetAt,
                secondaryResetAt: usage.secondaryResetAt
            )
        case let .authenticationURL(url):
            discussion.pendingAuthenticationURL = url
            effects.append(.openAuthenticationURL(url))
        case .refreshAccount:
            effects.append(.refreshAccount)
        case .accountLoggedOut:
            discussion.account = AssistantAccountState(email: nil, plan: nil, requiresLogin: true)
            discussion.usage = AssistantUsageState()
            discussion.pendingAuthenticationURL = nil
        case let .messageDelta(delta):
            setLiveActivity(id: "response", label: "Writing response", discussion: &discussion)
            if !delta.isEmpty {
                state.pendingStreamText += delta
                effects.append(.scheduleStreamFlush)
            }
        case let .turnStarted(id):
            setLiveActivity(id: "turn", label: "Agent accepted request", discussion: &discussion)
            discussion.activeTurnID = id
        case let .turnCompleted(status, error):
            if finishTurn(status: status, errorMessage: error, discussion: &discussion) {
                effects.append(.persistConversation)
            }
        case let .item(item):
            recordItem(item, discussion: &discussion)
        case .mcpStatus:
            break
        case let .approval(approval):
            discussion.pendingApproval = AssistantApprovalRequestState(
                id: approval.id,
                method: approval.method,
                summary: approval.summary
            )
        case let .unsupported(method):
            let activity = AssistantActivityState(
                id: "unsupported:\(method)",
                label: "Unsupported agent event",
                state: "failed",
                summary: method
            )
            state.pendingActivities.append(activity)
            discussion.liveActivity = activity
        case let .backendExited(status, pendingRequests):
            state.conversationStartPending = false
            let pending = pendingRequests.isEmpty
                ? "no pending requests"
                : "pending: \(pendingRequests.joined(separator: ", "))"
            recordError(
                "Agent backend exited with status \(status) (\(pending)).",
                discussion: &discussion
            )
        case let .failed(message):
            state.conversationStartPending = false
            recordError(message, discussion: &discussion)
        case let .resumeFailed(detail):
            state.conversationStartPending = false
            let message = AssistantMessageState(
                id: UUID().uuidString.lowercased(),
                role: "activity",
                content: "Previous Codex session could not be resumed. A new backend session is starting.",
                createdAt: timestamp,
                agentId: "codex_app_server",
                model: nil,
                citations: [],
                usedContext: [],
                activities: [AssistantActivityState(
                    id: UUID().uuidString.lowercased(),
                    label: "Session handoff",
                    state: "failed",
                    summary: detail
                )],
                taskSuggestions: [],
                pins: []
            )
            updateActiveConversation(in: &discussion) {
                $0.messages.append(message)
                $0.backendSession = nil
            }
            effects += [.persistConversation, .restartConversation]
        }
        if discussion.activity == .streaming {
            discussion.lastActivityAt = timestamp
            effects.append(.scheduleResponseTimeout(conversationID: discussion.activeConversation?.id))
        }
        return effects
    }

    package func flushPendingStream(into discussion: inout AssistantDiscussionState) {
        guard !state.pendingStreamText.isEmpty else { return }
        discussion.streamingText += state.pendingStreamText
        state.pendingStreamText = ""
    }

    package func scheduleStreamFlush(_ action: @escaping () -> Void) {
        guard streamFlushWorkItem == nil else { return }
        streamFlushWorkItem = scheduler.schedule(after: Self.streamCoalescingDelay) { [weak self] in
            self?.streamFlushWorkItem = nil
            action()
        }
    }

    package func scheduleResponseTimeout(
        conversationID: String?,
        _ action: @escaping (String?) -> Void
    ) {
        responseTimeoutWorkItem?.cancel()
        responseTimeoutWorkItem = scheduler.schedule(after: responseTimeout) {
            action(conversationID)
        }
    }

    package func cancelResponseTimeout() {
        responseTimeoutWorkItem?.cancel()
        responseTimeoutWorkItem = nil
    }

    package func scheduleDraftSave(_ action: @escaping () -> Void) {
        draftSaveWorkItem?.cancel()
        draftSaveWorkItem = scheduler.schedule(after: Self.draftPersistenceDelay, action)
    }

    package func isParameterSuggested(sessionKey: String, name: String) -> Bool {
        state.suggestedParameters[sessionKey]?.contains(name) == true
    }

    package func setSuggestedParameters(sessionKey: String, names: Set<String>) {
        state.suggestedParameters[sessionKey] = names
    }

    package func clearSuggestedParameters(sessionKey: String, names: Set<String>? = nil) {
        guard let names else {
            state.suggestedParameters.removeValue(forKey: sessionKey)
            return
        }
        guard var suggested = state.suggestedParameters[sessionKey] else { return }
        suggested.subtract(names)
        if suggested.isEmpty {
            state.suggestedParameters.removeValue(forKey: sessionKey)
        } else {
            state.suggestedParameters[sessionKey] = suggested
        }
    }

    package func recordError(_ message: String, discussion: inout AssistantDiscussionState) {
        cancelResponseTimeout()
        discussion.lastError = message
        discussion.activity = .restartRequired
        discussion.liveActivity = nil
        discussion.lastActivityAt = nil
    }

    private func recordItem(
        _ item: AgentItemDescriptor,
        discussion: inout AssistantDiscussionState
    ) {
        let trusted = item.kind == "mcpToolCall" && item.server == mcpServerName
        let activity = AssistantActivityState(
            id: item.id,
            label: item.tool.map { trusted ? "CASA \($0)" : $0 } ?? item.kind,
            state: item.error == nil ? (item.completed ? "succeeded" : "running") : "failed",
            summary: item.error
        )
        if let index = state.pendingActivities.firstIndex(where: { $0.id == item.id }) {
            state.pendingActivities[index] = activity
        } else {
            state.pendingActivities.append(activity)
        }
        discussion.liveActivity = activity
        discussion.lastActivityAt = timestamp
        guard item.completed, trusted else { return }
        for citation in item.citations where !state.pendingCitations.contains(where: { $0.id == citation.id }) {
            state.pendingCitations.append(citation)
        }
        for (index, value) in item.taskSuggestions.enumerated() {
            let id = index == 0 ? item.id : "\(item.id):\(index)"
            let suggestion = AssistantTaskSuggestionState(
                id: id,
                taskId: value.taskId,
                parameters: value.parameters,
                validatedPatch: value.validatedPatch
            )
            if let existing = state.pendingTaskSuggestions.firstIndex(where: { $0.id == id }) {
                state.pendingTaskSuggestions[existing] = suggestion
            } else {
                state.pendingTaskSuggestions.append(suggestion)
            }
        }
    }

    @discardableResult
    private func finishTurn(
        status: String,
        errorMessage: String?,
        discussion: inout AssistantDiscussionState
    ) -> Bool {
        cancelResponseTimeout()
        streamFlushWorkItem?.cancel()
        streamFlushWorkItem = nil
        flushPendingStream(into: &discussion)
        let text = discussion.streamingText
        let durableText: String? = if !text.isEmpty {
            text
        } else if status == "failed" {
            errorMessage ?? "Agent turn failed before producing an answer."
        } else if ["cancelled", "interrupted"].contains(status) {
            "Agent response cancelled."
        } else if !state.pendingActivities.isEmpty {
            "Agent completed without a text response."
        } else {
            nil
        }
        if let durableText {
            let conversation = discussion.activeConversation
            let message = AssistantMessageState(
                id: UUID().uuidString.lowercased(),
                role: "assistant",
                content: durableText,
                createdAt: timestamp,
                agentId: "codex_app_server",
                model: conversation?.profile.model,
                citations: state.pendingCitations,
                usedContext: discussion.selectedContexts,
                activities: state.pendingActivities,
                taskSuggestions: state.pendingTaskSuggestions,
                pins: []
            )
            updateActiveConversation(in: &discussion) { $0.messages.append(message) }
        }
        discussion.streamingText = ""
        discussion.liveActivity = nil
        discussion.lastActivityAt = nil
        discussion.activeTurnID = nil
        discussion.activity = status == "failed" ? .restartRequired : .completed
        state.pendingCitations = []
        state.pendingActivities = []
        state.pendingTaskSuggestions = []
        if status == "failed" {
            recordError(errorMessage ?? "Agent turn failed", discussion: &discussion)
        }
        return durableText != nil
    }

    private func setLiveActivity(
        id: String,
        label: String,
        discussion: inout AssistantDiscussionState
    ) {
        guard discussion.activity == .streaming else { return }
        discussion.liveActivity = AssistantActivityState(
            id: id,
            label: label,
            state: "running",
            summary: nil
        )
        discussion.lastActivityAt = timestamp
    }

    private func updateActiveConversation(
        in discussion: inout AssistantDiscussionState,
        _ update: (inout AssistantConversationState) -> Void
    ) {
        guard let id = discussion.activeConversationID,
              let index = discussion.conversations.firstIndex(where: { $0.id == id })
        else { return }
        update(&discussion.conversations[index])
    }
}
