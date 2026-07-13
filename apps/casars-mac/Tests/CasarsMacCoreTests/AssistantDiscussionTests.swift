import Foundation
@testable import CasarsMacCore
import XCTest

final class AssistantDiscussionTests: XCTestCase {
    func testPersistedUsedContextDefaultsMissingTransientSelectionToSelected() throws {
        let data = Data(#"{"id":"context-1","kind":"notebook","label":"Analysis","summary":"Open notebook","excerpt":"notes","byte_count":5,"content_sha256":"hash","untrusted_evidence":false}"#.utf8)
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase

        let context = try decoder.decode(AssistantContextItemState.self, from: data)

        XCTAssertTrue(context.selected)
        XCTAssertEqual(context.byteCount, 5)
    }

    func testOptInRetainedLiveTranscriptLoadsThroughProductionBoundary() throws {
        guard let project = ProcessInfo.processInfo.environment["CASA_RS_LIVE_TRANSCRIPT_PROJECT"] else {
            throw XCTSkip("set by the opt-in live GUI acceptance harness after a failure")
        }
        let conversations = try UniFFIAssistantPersistenceClient().conversations(projectRoot: project)
        print(
            "CASA_RS_RETAINED_LIVE_TRANSCRIPT conversations=\(conversations.count) "
                + "messages=\(conversations.map { $0.messages.count })"
        )
        XCTAssertFalse(conversations.isEmpty)
    }

    func testContextBudgetScalesAcrossOpenTabsAndCountsUTF8Bytes() {
        XCTAssertEqual(AssistantContextBudgetPolicy.excerptLimits(openTabCount: 1), [16 * 1_024])
        XCTAssertEqual(AssistantContextBudgetPolicy.excerptLimits(openTabCount: 4), Array(repeating: 16 * 1_024, count: 4))

        let many = AssistantContextBudgetPolicy.excerptLimits(openTabCount: 9)
        XCTAssertEqual(many.reduce(0, +), 64 * 1_024)
        XCTAssertLessThanOrEqual(many.max() ?? 0, 16 * 1_024)
        XCTAssertLessThanOrEqual((many.max() ?? 0) - (many.min() ?? 0), 1)

        let unicode = String(repeating: "α", count: 100)
        let bounded = AssistantContextBudgetPolicy.truncate(unicode, byteLimit: 61)
        XCTAssertLessThanOrEqual(bounded.utf8.count, 61)
        XCTAssertTrue(bounded.isEmpty || bounded.last != "�")
    }

    func testAuthorityPresetsMapToNativeCodexControls() {
        XCTAssertEqual(AssistantAuthorityState.explore.codexSettings.sandbox, "read-only")
        XCTAssertEqual(AssistantAuthorityState.explore.codexSettings.approvalPolicy, "never")
        XCTAssertEqual(AssistantAuthorityState.work.codexSettings.sandbox, "workspace-write")
        XCTAssertEqual(AssistantAuthorityState.work.codexSettings.approvalPolicy, "on-request")
        XCTAssertEqual(AssistantAuthorityState.fullAccess.codexSettings.sandbox, "danger-full-access")
        XCTAssertEqual(AssistantAuthorityState.fullAccess.codexSettings.approvalPolicy, "never")
    }

    func testAgentAndPythonCommandsAreUserSelectableNotHashPinned() throws {
        var profile = AssistantSessionProfileState()
        profile.agentCommand = "/opt/local/bin/codex"
        profile.pythonCommand = "/Users/scientist/.venv/bin/python"
        profile.pythonProvenance = AssistantPythonProvenanceState(
            selectedCommand: profile.pythonCommand,
            resolvedPath: profile.pythonCommand,
            implementation: "CPython",
            version: "3.13.5",
            environmentLabel: "science-venv",
            casaRsVersion: "0.24.1",
            packages: ["casatools": "6.6.6"]
        )
        let encoded = try JSONEncoder().encode(profile)
        let decoded = try JSONDecoder().decode(AssistantSessionProfileState.self, from: encoded)
        let json = String(decoding: encoded, as: UTF8.self)
        XCTAssertEqual(decoded.agentCommand, "/opt/local/bin/codex")
        XCTAssertEqual(decoded.pythonCommand, "/Users/scientist/.venv/bin/python")
        XCTAssertEqual(decoded.pythonProvenance?.packages["casatools"], "6.6.6")
        XCTAssertFalse(json.contains("sha256"))
    }

    func testConfigurationDiscoveryAcceptsUserSelectedExecutableLocations() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-agent-config-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let codex = directory.appendingPathComponent("custom-codex")
        let mcp = directory.appendingPathComponent("custom-mcp")
        let python = directory.appendingPathComponent("scientific-python")
        try "#!/bin/sh\n".write(to: codex, atomically: true, encoding: .utf8)
        try "#!/bin/sh\n".write(to: mcp, atomically: true, encoding: .utf8)
        try "#!/bin/sh\n".write(to: python, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: codex.path)
        try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: mcp.path)
        try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: python.path)

        let configuration = try AgentSessionConfiguration.discover(environment: [
            "CASA_RS_AGENT_COMMAND": codex.path,
            "CASA_RS_PROJECT_MCP": mcp.path,
            "PATH": "",
        ])
        XCTAssertEqual(configuration.agentExecutable, codex.path)
        XCTAssertEqual(configuration.projectMCPExecutable, mcp.path)
        XCTAssertEqual(
            AgentSessionConfiguration.resolveExecutable(
                "scientific-python",
                environment: ["PATH": directory.path]
            ),
            python.path
        )
    }

    func testExploreLaunchAndThreadConfigRemoveGenericAuthority() {
        let arguments = CodexAppServerSession.launchArguments(exploreRestricted: true)
        for feature in ["shell_tool", "unified_exec", "code_mode_host", "browser_use", "apps"] {
            XCTAssertTrue(arguments.contains(feature), "missing denied feature \(feature)")
        }
        XCTAssertTrue(arguments.contains("mcp_servers={}"))
        XCTAssertEqual(
            CodexAppServerSession.launchArguments(exploreRestricted: false),
            ["app-server"]
        )

        let session = CodexAppServerSession(configuration: AgentSessionConfiguration(
            agentExecutable: "/usr/bin/false",
            projectMCPExecutable: "/project/bin/casars-project-mcp"
        ))
        let request = AgentConversationRequest(
            projectRoot: "/project",
            model: "",
            effort: "low",
            resumeThreadID: "opaque-thread",
            runtimeProfile: CasaAgentRuntimeProfile(
                authority: .explore,
                sessionNonce: String(repeating: "n", count: 32),
                pythonCommand: "python3"
            )
        )
        let config = session.threadConfig(request)
        let features = config["features"] as? [String: Bool]
        XCTAssertEqual(features?["shell_tool"], false)
        XCTAssertEqual(features?["browser_use"], false)
        XCTAssertEqual(config["project_doc_max_bytes"] as? Int, 0)
        let servers = config["mcp_servers"] as? [String: [String: Any]]
        let serverName = request.runtimeProfile.mcpServerName
        XCTAssertEqual(servers?[serverName]?["required"] as? Bool, true)
        XCTAssertEqual(
            servers?[serverName]?["args"] as? [String],
            ["--project-root", "/project", "--nonce", String(repeating: "n", count: 32)]
        )
    }

    func testPersistenceRoundTripStoresAgentNeutralProfileAndOpaqueResumeID() throws {
        let project = try temporaryProject()
        defer { try? FileManager.default.removeItem(at: project) }
        let client = UniFFIAssistantPersistenceClient()
        var profile = AssistantSessionProfileState()
        profile.model = "fixture-model"
        var conversation = try client.createConversation(
            projectRoot: project.path,
            title: "Analysis",
            attachment: AssistantAttachmentState(
                kind: "notebook",
                identifier: "Analysis.md",
                label: "Analysis",
                primary: true
            ),
            profile: profile
        )
        conversation.backendSession = AssistantBackendSessionState(
            backendId: "codex_app_server",
            sessionId: "thread-123"
        )
        conversation.draft = "Continue this discussion"
        try client.saveConversation(projectRoot: project.path, transcript: conversation)

        let reloaded = try XCTUnwrap(client.conversations(projectRoot: project.path).first)
        XCTAssertEqual(reloaded.profile.backendId, "codex_app_server")
        XCTAssertEqual(reloaded.backendSession?.sessionId, "thread-123")
        XCTAssertEqual(reloaded.draft, "Continue this discussion")
        let serialized = String(decoding: try JSONEncoder().encode(reloaded), as: UTF8.self)
        XCTAssertFalse(serialized.contains("credential"))
        XCTAssertFalse(serialized.contains("provider_envelope"))
        XCTAssertFalse(serialized.contains("proposal"))
    }

    func testModelAndEffortSelectionStayOnBackendAndApplyToNextTurn() throws {
        let project = try temporaryProject()
        defer { try? FileManager.default.removeItem(at: project) }
        let client = UniFFIAssistantPersistenceClient()
        var conversation = try client.createConversation(
            projectRoot: project.path,
            title: "Analysis",
            attachment: AssistantAttachmentState(
                kind: "notebook",
                identifier: "Analysis.md",
                label: "Analysis",
                primary: true
            ),
            profile: AssistantSessionProfileState()
        )
        conversation.backendSession = AssistantBackendSessionState(
            backendId: "codex_app_server",
            sessionId: "thread-existing"
        )
        var discussion = AssistantDiscussionState()
        discussion.conversations = [conversation]
        discussion.activeConversationID = conversation.id
        discussion.models = [AssistantModelState(
            id: "alternate-model",
            label: "Alternate model",
            defaultEffort: "high",
            supportedEfforts: ["low", "high"],
            isDefault: false
        )]
        var state = FixtureWorkbench.makeState()
        state.project.rootPath = project.path
        state.assistantDiscussion = discussion
        let store = WorkbenchStore(state: state)
        let agent = FixtureAgentSession()
        store.installAgentSessionForTesting(agent)

        store.selectAssistantModel("alternate-model")
        store.selectAssistantEffort("low")

        XCTAssertEqual(agent.restartCount, 0)
        XCTAssertTrue(agent.conversations.isEmpty)
        XCTAssertEqual(
            store.state.assistantDiscussion?.activeConversation?.backendSession?.sessionId,
            "thread-existing"
        )

        store.setAssistantDraft("Use the selected model")
        store.sendAssistantPrompt()

        XCTAssertEqual(agent.turns.last?.threadID, "thread-existing")
        XCTAssertEqual(agent.turns.last?.model, "alternate-model")
        XCTAssertEqual(agent.turns.last?.effort, "low")
    }

    func testAccountLogoutClearsVisibleSubscriptionStateAfterBackendConfirmation() {
        var discussion = AssistantDiscussionState()
        discussion.account = AssistantAccountState(
            email: "scientist@example.invalid",
            plan: "pro",
            requiresLogin: false
        )
        discussion.usage = AssistantUsageState(primaryPercentUsed: 31)
        discussion.pendingAuthenticationURL = "https://example.invalid/auth"
        var state = FixtureWorkbench.makeState()
        state.assistantDiscussion = discussion
        let store = WorkbenchStore(state: state)
        let agent = FixtureAgentSession()
        store.installAgentSessionForTesting(agent)

        store.logoutAssistantAccount()

        XCTAssertEqual(agent.accountLogoutCount, 1)
        XCTAssertFalse(store.state.assistantDiscussion?.account.requiresLogin ?? true)

        agent.emit(["method": "casa/accountLogout/completed", "params": [:]])

        XCTAssertTrue(store.state.assistantDiscussion?.account.requiresLogin ?? false)
        XCTAssertNil(store.state.assistantDiscussion?.account.email)
        XCTAssertNil(store.state.assistantDiscussion?.account.plan)
        XCTAssertNil(store.state.assistantDiscussion?.usage.primaryPercentUsed)
        XCTAssertNil(store.state.assistantDiscussion?.pendingAuthenticationURL)
    }

    func testRuntimeProfileChangeRestartsBeforeStartingReplacementConversation() throws {
        let project = try temporaryProject()
        defer { try? FileManager.default.removeItem(at: project) }
        let client = UniFFIAssistantPersistenceClient()
        var conversation = try client.createConversation(
            projectRoot: project.path,
            title: "Analysis",
            attachment: AssistantAttachmentState(
                kind: "notebook",
                identifier: "Analysis.md",
                label: "Analysis",
                primary: true
            ),
            profile: AssistantSessionProfileState()
        )
        conversation.backendSession = AssistantBackendSessionState(
            backendId: "codex_app_server",
            sessionId: "thread-existing"
        )
        var discussion = AssistantDiscussionState()
        discussion.conversations = [conversation]
        discussion.activeConversationID = conversation.id
        var state = FixtureWorkbench.makeState()
        state.project.rootPath = project.path
        state.assistantDiscussion = discussion
        let store = WorkbenchStore(state: state)
        let agent = FixtureAgentSession()
        store.installAgentSessionForTesting(agent)

        store.selectAssistantAuthority(.explore)

        XCTAssertEqual(agent.restartCount, 1)
        XCTAssertEqual(agent.conversations.count, 1)
        XCTAssertNil(agent.conversations.last?.resumeThreadID)
        XCTAssertEqual(agent.conversations.last?.runtimeProfile.authority, .explore)

        store.setAssistantDraft("Wait for the replacement thread")
        store.sendAssistantPrompt()
        XCTAssertEqual(agent.conversations.count, 1)
        XCTAssertTrue(agent.turns.isEmpty)

        agent.emit(["result": ["thread": ["id": "thread-replacement"]]])
        XCTAssertEqual(agent.turns.last?.threadID, "thread-replacement")
        XCTAssertEqual(agent.turns.last?.text, "Wait for the replacement thread")
    }

    func testOpeningClosedDiscussionReloadsDurableConversationBeforeResume() throws {
        let project = try temporaryProject()
        defer { try? FileManager.default.removeItem(at: project) }
        let client = UniFFIAssistantPersistenceClient()
        var durable = try client.createConversation(
            projectRoot: project.path,
            title: "Project discussion",
            attachment: AssistantAttachmentState(
                kind: "notebook",
                identifier: "Analysis.md",
                label: "Analysis",
                primary: true
            ),
            profile: AssistantSessionProfileState()
        )
        durable.backendSession = AssistantBackendSessionState(
            backendId: "codex_app_server",
            sessionId: "thread-durable"
        )
        durable.messages.append(AssistantMessageState(
            id: UUID().uuidString.lowercased(),
            role: "assistant",
            content: "Durable answer",
            createdAt: 1,
            agentId: "codex_app_server",
            model: nil,
            citations: [],
            usedContext: [],
            activities: [],
            taskSuggestions: [],
            pins: []
        ))
        try client.saveConversation(projectRoot: project.path, transcript: durable)

        var stale = durable
        stale.id = UUID().uuidString.lowercased()
        stale.backendSession = nil
        stale.messages = []
        var discussion = AssistantDiscussionState()
        discussion.presentation = .closed
        discussion.conversations = [stale]
        discussion.activeConversationID = stale.id
        var state = FixtureWorkbench.makeState()
        state.project.rootPath = project.path
        state.assistantDiscussion = discussion
        let store = WorkbenchStore(state: state)
        let agent = FixtureAgentSession()
        store.installAgentSessionForTesting(agent)

        store.openAssistantDiscussion()

        XCTAssertEqual(store.state.assistantDiscussion?.activeConversation?.id, durable.id)
        XCTAssertEqual(
            store.state.assistantDiscussion?.activeConversation?.messages.last?.content,
            "Durable answer"
        )
        XCTAssertEqual(agent.conversations.last?.resumeThreadID, "thread-durable")
    }

    func testCorpusIngestionLayersBaselineProjectDocumentsAndReleaseSource() throws {
        let project = try temporaryProject()
        let source = try temporaryProject()
        defer {
            try? FileManager.default.removeItem(at: project)
            try? FileManager.default.removeItem(at: source)
        }
        try FileManager.default.createDirectory(
            at: project.appendingPathComponent("documents"),
            withIntermediateDirectories: true
        )
        try "A cited project paper.".write(
            to: project.appendingPathComponent("documents/paper.md"),
            atomically: true,
            encoding: .utf8
        )
        try "# Architecture".write(
            to: source.appendingPathComponent("ARCHITECTURE.md"),
            atomically: true,
            encoding: .utf8
        )
        try #"{"schema_version":1,"release":"v1.2.3","commit":"abc123"}"#.write(
            to: source.appendingPathComponent("casars-source.json"),
            atomically: true,
            encoding: .utf8
        )
        let result = AssistantCorpusIngestor().collect(
            projectRoot: project.path,
            environment: ["CASA_RS_SOURCE_ROOT": source.path]
        )

        XCTAssertTrue(result.documents.contains { $0.layer == "baseline" && $0.redistributionCleared })
        XCTAssertTrue(result.documents.contains {
            $0.layer == "project_document" && $0.citation.sourcePath == "documents/paper.md"
        })
        XCTAssertTrue(result.documents.contains {
            $0.layer == "release_source" && $0.citation.release == "v1.2.3"
                && $0.citation.commit == "abc123"
        })
        XCTAssertEqual(result.refreshedLayers, ["baseline", "project_document", "release_source", "live_source"])
    }

    func testPinDestinationIsAlwaysChronologicalTail() throws {
        let project = try temporaryProject()
        defer { try? FileManager.default.removeItem(at: project) }
        let client = UniFFIAssistantPersistenceClient()
        let conversationID = UUID().uuidString.lowercased()
        let notebookID = UUID().uuidString.lowercased()
        let messageID = UUID().uuidString.lowercased()
        let pin = try client.createPin(AssistantCreatePinEnvelope(
            conversationId: conversationID,
            notebookId: notebookID,
            messageId: messageID,
            representation: "answer_with_citations",
            snapshotContent: "### AI snapshot\nAnswer [1]"
        ))
        XCTAssertEqual(pin.destination, "chronological_tail")
        XCTAssertEqual(pin.messageId, messageID)
    }

    func testTypedTaskSuggestionOpensCanonicalTaskWithHighlightedValues() throws {
        let project = try temporaryProject()
        defer { try? FileManager.default.removeItem(at: project) }
        let client = UniFFIAssistantPersistenceClient()
        var conversation = try client.createConversation(
            projectRoot: project.path,
            title: "Analysis",
            attachment: AssistantAttachmentState(
                kind: "notebook",
                identifier: "Analysis.md",
                label: "Analysis",
                primary: true
            ),
            profile: AssistantSessionProfileState()
        )
        conversation.messages.append(AssistantMessageState(
            id: "answer",
            role: "assistant",
            content: "Try Briggs weighting.",
            createdAt: 1,
            agentId: "fixture",
            model: "fixture",
            citations: [],
            usedContext: [],
            activities: [],
            taskSuggestions: [AssistantTaskSuggestionState(
                id: "suggestion",
                taskId: "imager",
                parameters: ["robust": "-0.5", "weighting": "briggs"]
            )],
            pins: []
        ))
        var discussion = AssistantDiscussionState()
        discussion.conversations = [conversation]
        discussion.activeConversationID = conversation.id
        var state = FixtureWorkbench.makeState()
        state.project.rootPath = project.path
        state.assistantDiscussion = discussion
        let store = WorkbenchStore(state: state)

        store.openAssistantTaskSuggestion(messageID: "answer", suggestionID: "suggestion")

        let tab = try XCTUnwrap(store.state.tabs.first { $0.id == store.state.activeTabID })
        XCTAssertEqual(tab.kind, .task)
        XCTAssertEqual(tab.taskID, "imager")
        XCTAssertEqual(store.parameterText(surfaceID: "imager", instanceID: tab.id, name: "robust"), "-0.5")
        XCTAssertTrue(store.parameterIsAssistantSuggested(
            surfaceID: "imager",
            instanceID: tab.id,
            name: "robust"
        ))
    }

    func testCompletedCasaCorpusToolProducesDurableCitationAndActivity() throws {
        let project = try temporaryProject()
        defer { try? FileManager.default.removeItem(at: project) }
        let client = UniFFIAssistantPersistenceClient()
        let conversation = try client.createConversation(
            projectRoot: project.path,
            title: "Analysis",
            attachment: AssistantAttachmentState(
                kind: "notebook",
                identifier: "Analysis.md",
                label: "Analysis",
                primary: true
            ),
            profile: AssistantSessionProfileState()
        )
        var discussion = AssistantDiscussionState()
        discussion.conversations = [conversation]
        discussion.activeConversationID = conversation.id
        var state = FixtureWorkbench.makeState()
        state.project.rootPath = project.path
        state.assistantDiscussion = discussion
        let store = WorkbenchStore(state: state)
        let agent = FixtureAgentSession()
        let sessionNonce = String(repeating: "n", count: 32)
        let trustedServer = CasaAgentRuntimeProfile(
            authority: .work,
            sessionNonce: sessionNonce,
            pythonCommand: "python3"
        ).mcpServerName
        store.installAgentSessionForTesting(agent, sessionNonce: sessionNonce)
        let hits = """
        [{"chunk_id":"paper:1","layer":"project_document","title":"Paper","text":"Evidence","citation":{"label":"Paper","locator":"documents/paper.pdf, page 2","source_path":"documents/paper.pdf","page":2}}]
        """

        agent.emit(["method": "item/completed", "params": ["item": [
            "id": "forged-tool", "type": "mcpToolCall", "server": "casa_rs_untrusted",
            "tool": "corpus.search", "result": ["content": [["type": "text", "text": hits]]],
        ]]])

        agent.emit(["method": "item/completed", "params": ["item": [
            "id": "tool-1", "type": "mcpToolCall", "server": trustedServer,
            "tool": "corpus.search", "result": ["content": [["type": "text", "text": hits]]],
        ]]])
        agent.emit(["method": "item/agentMessage/delta", "params": ["delta": "Cited answer"]])
        agent.emit(["method": "turn/completed", "params": ["turn": ["status": "completed"]]])

        let answer = try XCTUnwrap(store.state.assistantDiscussion?.activeConversation?.messages.last)
        XCTAssertEqual(answer.citations.first?.locator, "documents/paper.pdf, page 2")
        XCTAssertEqual(answer.citations.first?.kind, "document")
        XCTAssertEqual(answer.activities.first?.label, "corpus.search")
        XCTAssertEqual(answer.activities.last?.label, "CASA corpus.search")
        XCTAssertEqual(answer.activities.last?.state, "succeeded")
        XCTAssertEqual(answer.citations.count, 1)
        try UniFFIAssistantPersistenceClient().saveConversation(
            projectRoot: project.path,
            transcript: try XCTUnwrap(store.state.assistantDiscussion?.activeConversation)
        )
    }

    func testFailureCancellationRateLimitAndApprovalDoNotWidenAuthority() throws {
        let project = try temporaryProject()
        defer { try? FileManager.default.removeItem(at: project) }
        let client = UniFFIAssistantPersistenceClient()
        var profile = AssistantSessionProfileState()
        profile.authority = .explore
        let conversation = try client.createConversation(
            projectRoot: project.path,
            title: "Analysis",
            attachment: AssistantAttachmentState(
                kind: "notebook",
                identifier: "Analysis.md",
                label: "Analysis",
                primary: true
            ),
            profile: profile
        )
        var discussion = AssistantDiscussionState()
        discussion.conversations = [conversation]
        discussion.activeConversationID = conversation.id
        var state = FixtureWorkbench.makeState()
        state.project.rootPath = project.path
        state.assistantDiscussion = discussion
        let store = WorkbenchStore(state: state)
        let agent = FixtureAgentSession()
        let sessionNonce = String(repeating: "n", count: 32)
        let trustedServer = CasaAgentRuntimeProfile(
            authority: .explore,
            sessionNonce: sessionNonce,
            pythonCommand: "python3"
        ).mcpServerName
        store.installAgentSessionForTesting(agent, sessionNonce: sessionNonce)

        agent.emit(["method": "item/completed", "params": ["item": [
            "id": "tool-failure", "type": "mcpToolCall", "server": trustedServer,
            "tool": "corpus.search", "error": ["message": "index unavailable"],
        ]]])
        agent.emit(["method": "turn/completed", "params": ["turn": ["status": "completed"]]])
        XCTAssertEqual(
            store.state.assistantDiscussion?.activeConversation?.messages.last?.activities.first?.state,
            "failed"
        )

        agent.emit(["method": "turn/completed", "params": ["turn": ["status": "cancelled"]]])
        XCTAssertEqual(
            store.state.assistantDiscussion?.activeConversation?.messages.last?.content,
            "Agent response cancelled."
        )

        agent.emit(["method": "account/rateLimits/updated", "params": [
            "primary": ["usedPercent": 100.0, "resetsAt": 42],
        ]])
        XCTAssertEqual(store.state.assistantDiscussion?.usage.primaryPercentUsed, 100.0)

        agent.emit([
            "id": "approval-1",
            "method": "item/commandExecution/requestApproval",
            "params": ["command": "touch outside-project"],
        ])
        store.resolveAssistantApproval("decline")
        XCTAssertEqual(agent.approvals.last?.requestID, "approval-1")
        XCTAssertEqual(agent.approvals.last?.decision, "decline")

        agent.emit(["method": "casa/error", "params": ["message": "agent process exited"]])
        XCTAssertEqual(store.state.assistantDiscussion?.activity, .restartRequired)
        XCTAssertEqual(store.state.assistantDiscussion?.lastError, "agent process exited")
        XCTAssertEqual(store.state.assistantDiscussion?.activeConversation?.profile.authority, .explore)
    }

    func testIncompatibleResumeCreatesVisibleHandoffBeforeFreshSession() throws {
        let project = try temporaryProject()
        defer { try? FileManager.default.removeItem(at: project) }
        let client = UniFFIAssistantPersistenceClient()
        var conversation = try client.createConversation(
            projectRoot: project.path,
            title: "Analysis",
            attachment: AssistantAttachmentState(
                kind: "notebook",
                identifier: "Analysis.md",
                label: "Analysis",
                primary: true
            ),
            profile: AssistantSessionProfileState()
        )
        conversation.backendSession = AssistantBackendSessionState(
            backendId: "codex_app_server",
            sessionId: "missing-thread"
        )
        conversation.messages.append(AssistantMessageState(
            id: UUID().uuidString.lowercased(),
            role: "assistant",
            content: "Earlier durable answer",
            createdAt: 1,
            agentId: "codex_app_server",
            model: nil,
            citations: [],
            usedContext: [],
            activities: [],
            taskSuggestions: [],
            pins: []
        ))
        var discussion = AssistantDiscussionState()
        discussion.conversations = [conversation]
        discussion.activeConversationID = conversation.id
        var state = FixtureWorkbench.makeState()
        state.project.rootPath = project.path
        state.assistantDiscussion = discussion
        let store = WorkbenchStore(state: state)
        let agent = FixtureAgentSession()
        store.installAgentSessionForTesting(agent)

        agent.emit([
            "method": "casa/resumeFailed",
            "params": ["message": "thread not found"],
        ])

        let updated = try XCTUnwrap(store.state.assistantDiscussion?.activeConversation)
        XCTAssertNil(updated.backendSession)
        XCTAssertEqual(updated.messages.first?.content, "Earlier durable answer")
        XCTAssertEqual(updated.messages.count, 2)
        XCTAssertEqual(updated.messages.last?.role, "activity")
        XCTAssertTrue(updated.messages.last?.content.contains("new backend session") == true)
        XCTAssertEqual(agent.conversations.last?.resumeThreadID, nil)
    }

    func testFixtureAgentImplementsSameSessionBoundary() {
        let fixture = FixtureAgentSession()
        let ready = expectation(description: "ready")
        fixture.prepare { result in
            if case let .failure(error) = result { XCTFail("fixture failed: \(error)") }
            ready.fulfill()
        }
        fixture.startConversation(AgentConversationRequest(
            projectRoot: "/tmp/project",
            model: "fixture-model",
            effort: "medium",
            resumeThreadID: nil,
            runtimeProfile: CasaAgentRuntimeProfile(
                authority: .work,
                sessionNonce: String(repeating: "n", count: 24),
                pythonCommand: "python3"
            )
        ))
        XCTAssertEqual(fixture.conversations.count, 1)
        wait(for: [ready], timeout: 1)
    }

    func testOptInCodexSubscriptionSmoke() throws {
        guard ProcessInfo.processInfo.environment["CASA_RS_CODEX_LIVE_SMOKE"] == "1" else {
            throw XCTSkip("set CASA_RS_CODEX_LIVE_SMOKE=1 to exercise the installed Codex App Server subscription login")
        }
        let session = CodexAppServerSession(configuration: try .discover())
        defer { session.terminate() }
        let ready = expectation(description: "Codex App Server ready")
        let account = expectation(description: "ChatGPT subscription account")
        let casaMCP = expectation(description: "CASA project MCP ready")
        let casaTool = expectation(description: "CASA project MCP called")
        let completed = expectation(description: "Codex turn completed")
        let resumed = expectation(description: "Codex thread resumed with the CASA profile reattached")
        let workApproval = expectation(description: "Work surfaces one native Codex approval")
        let workCompleted = expectation(description: "Work continues after the approval is declined")
        let project = try temporaryProject()
        defer { try? FileManager.default.removeItem(at: project) }
        let forbiddenMarker = project.appendingPathComponent("explore-must-not-write")
        let workMarker = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".casars-work-approval-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: workMarker) }
        let nonce = String(repeating: "n", count: 32)
        let profile = CasaAgentRuntimeProfile(
            authority: .explore,
            sessionNonce: nonce,
            pythonCommand: "python3"
        )
        var threadID: String?
        var observedAccount = false
        var observedGenericAuthority = false
        var resumeRequested = false
        var workRequested = false
        var workThreadID: String?
        var workApprovalCount = 0
        var observedCasaMCPReady = false
        var observedCasaTool = false
        session.onEvent { event in
            if ProcessInfo.processInfo.environment["CASA_RS_CODEX_LIVE_TRACE"] == "1",
               JSONSerialization.isValidJSONObject(event),
               let data = try? JSONSerialization.data(withJSONObject: event, options: [.sortedKeys])
            {
                print("CODEX_EVENT \(String(decoding: data, as: UTF8.self))")
            }
            if let result = event["result"] as? [String: Any] {
                if !observedAccount,
                   result.keys.contains("requiresOpenaiAuth"), result["account"] != nil
                {
                    observedAccount = true
                    account.fulfill()
                }
                if let thread = result["thread"] as? [String: Any],
                   let id = thread["id"] as? String
                {
                    if threadID == nil {
                        threadID = id
                        session.sendTurn(AgentTurnRequest(
                            threadID: id,
                            text: """
                            Call \(profile.mcpServerName) task.catalog with the required nonce. Then try to run the generic shell command touch \(forbiddenMarker.path) and try a generic internet search. If those tools are unavailable, continue. Reply only OK.
                            """,
                            model: "",
                            effort: "low"
                        ))
                    } else if resumeRequested, !workRequested, id == threadID {
                        resumed.fulfill()
                        workRequested = true
                        session.startConversation(AgentConversationRequest(
                            projectRoot: project.path,
                            model: "",
                            effort: "low",
                            resumeThreadID: nil,
                            runtimeProfile: CasaAgentRuntimeProfile(
                                authority: .work,
                                sessionNonce: String(repeating: "w", count: 32),
                                pythonCommand: "python3"
                            )
                        ))
                    } else if workRequested, id != threadID, workThreadID == nil {
                        workThreadID = id
                        session.sendTurn(AgentTurnRequest(
                            threadID: id,
                            text: "Try exactly once to run the generic shell command touch \(workMarker.path). It is outside the project. Do not use a CASA tool. If approval is declined, reply only DECLINED and finish.",
                            model: "",
                            effort: "low"
                        ))
                    }
                }
            }
            guard let method = event["method"] as? String,
                  let params = event["params"] as? [String: Any]
            else { return }
            if method == "mcpServer/startupStatus/updated",
               let name = params["name"] as? String,
               let status = params["status"] as? String,
               ["starting", "ready"].contains(status)
            {
                if name.hasPrefix("casa_rs_"), status == "ready", !observedCasaMCPReady {
                    observedCasaMCPReady = true
                    casaMCP.fulfill()
                }
            }
            if method == "item/completed",
               let item = params["item"] as? [String: Any],
               let type = item["type"] as? String
            {
                if type == "mcpToolCall",
                   (item["server"] as? String)?.hasPrefix("casa_rs_") == true,
                   !observedCasaTool
                {
                    observedCasaTool = true
                    casaTool.fulfill()
                } else if !workRequested
                    && (["commandExecution", "webSearch", "computerUse"].contains(type)
                    || (type == "mcpToolCall"
                        && (item["server"] as? String)?.hasPrefix("casa_rs_") != true))
                {
                    observedGenericAuthority = true
                }
            }
            if method == "turn/completed", !resumeRequested {
                completed.fulfill()
                resumeRequested = true
                session.startConversation(AgentConversationRequest(
                    projectRoot: project.path,
                    model: "",
                    effort: "low",
                    resumeThreadID: threadID,
                    runtimeProfile: profile
                ))
            } else if method == "turn/completed", workRequested, workThreadID != nil {
                workCompleted.fulfill()
            }
            if method == "item/commandExecution/requestApproval", workRequested,
               let requestID = event["id"]
            {
                workApprovalCount += 1
                if workApprovalCount == 1 { workApproval.fulfill() }
                session.approve(requestID: String(describing: requestID), decision: "decline")
            }
        }
        session.prepare { result in
            if case let .failure(error) = result { XCTFail("Codex failed: \(error)") }
            ready.fulfill()
        }
        wait(for: [ready, account], timeout: 20)
        session.startConversation(AgentConversationRequest(
            projectRoot: project.path,
            model: "",
            effort: "low",
            resumeThreadID: nil,
            runtimeProfile: profile
        ))
        wait(
            for: [casaMCP, casaTool, completed, resumed, workApproval, workCompleted],
            timeout: 55
        )
        XCTAssertFalse(observedGenericAuthority)
        XCTAssertFalse(FileManager.default.fileExists(atPath: forbiddenMarker.path))
        XCTAssertEqual(workApprovalCount, 1, "CASA must relay one Codex-native approval without duplicating it")
        XCTAssertFalse(FileManager.default.fileExists(atPath: workMarker.path))
    }

    private func temporaryProject() throws -> URL {
        let project = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-assistant-tests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: project, withIntermediateDirectories: true)
        return project
    }
}

private final class FixtureAgentSession: AgentSession {
    var conversations: [AgentConversationRequest] = []
    var turns: [AgentTurnRequest] = []
    var restartCount = 0
    var accountLogoutCount = 0
    var approvals: [(requestID: String, decision: String)] = []
    private var eventHandler: (([String: Any]) -> Void)?
    private var stateHandler: ((AssistantDiscussionActivity) -> Void)?

    func onEvent(_ handler: @escaping ([String: Any]) -> Void) { eventHandler = handler }
    func onStateChange(_ handler: @escaping (AssistantDiscussionActivity) -> Void) { stateHandler = handler }
    func prepare(_ completion: @escaping (Result<Void, Error>) -> Void) {
        stateHandler?(.ready)
        completion(.success(()))
    }
    func startConversation(_ request: AgentConversationRequest) { conversations.append(request) }
    func sendTurn(_ request: AgentTurnRequest) { turns.append(request) }
    func cancel(threadID: String, turnID: String) {}
    func approve(requestID: String, decision: String) {
        approvals.append((requestID, decision))
    }
    func requestAccountLogin() {}
    func requestAccountLogout() { accountLogoutCount += 1 }
    func refreshAccount() {}
    func restart() { restartCount += 1 }
    func terminate() {}
    func emit(_ event: [String: Any]) { eventHandler?(event) }
}
