import Foundation
@testable import CasarsMacCore
import XCTest

final class AssistantDiscussionTests: XCTestCase {
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
        store.installAgentSessionForTesting(agent)
        let hits = """
        [{"chunk_id":"paper:1","layer":"project_document","title":"Paper","text":"Evidence","citation":{"label":"Paper","locator":"documents/paper.pdf, page 2","source_path":"documents/paper.pdf","page":2}}]
        """

        agent.emit(["method": "item/completed", "params": ["item": [
            "id": "tool-1", "type": "mcpToolCall", "server": "casa_rs_fixture",
            "tool": "corpus.search", "result": ["content": [["type": "text", "text": hits]]],
        ]]])
        agent.emit(["method": "item/agentMessage/delta", "params": ["delta": "Cited answer"]])
        agent.emit(["method": "turn/completed", "params": ["turn": ["status": "completed"]]])

        let answer = try XCTUnwrap(store.state.assistantDiscussion?.activeConversation?.messages.last)
        XCTAssertEqual(answer.citations.first?.locator, "documents/paper.pdf, page 2")
        XCTAssertEqual(answer.citations.first?.kind, "document")
        XCTAssertEqual(answer.activities.first?.label, "CASA corpus.search")
        XCTAssertEqual(answer.activities.first?.state, "succeeded")
        try UniFFIAssistantPersistenceClient().saveConversation(
            projectRoot: project.path,
            transcript: try XCTUnwrap(store.state.assistantDiscussion?.activeConversation)
        )
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
    private var eventHandler: (([String: Any]) -> Void)?
    private var stateHandler: ((AssistantDiscussionActivity) -> Void)?

    func onEvent(_ handler: @escaping ([String: Any]) -> Void) { eventHandler = handler }
    func onStateChange(_ handler: @escaping (AssistantDiscussionActivity) -> Void) { stateHandler = handler }
    func prepare(_ completion: @escaping (Result<Void, Error>) -> Void) {
        stateHandler?(.ready)
        completion(.success(()))
    }
    func startConversation(_ request: AgentConversationRequest) { conversations.append(request) }
    func sendTurn(_ request: AgentTurnRequest) {}
    func cancel(threadID: String, turnID: String) {}
    func approve(requestID: String, decision: String) {}
    func requestAccountLogin() {}
    func refreshAccount() {}
    func restart() {}
    func terminate() {}
    func emit(_ event: [String: Any]) { eventHandler?(event) }
}
