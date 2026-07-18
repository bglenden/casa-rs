import Foundation
import AppKit
import CoreGraphics
import CasarsFrontendServices
@testable import CasarsMacCore
import XCTest

final class AssistantDiscussionTests: XCTestCase {
    func testGeneratedContextContractDecodesPersistedOwnerShapeWithoutTransientSelection() throws {
        let data = Data(#"{"id":"context-1","kind":"notebook","label":"Analysis","summary":"Open notebook","excerpt":"notes","byte_count":5,"content_sha256":"hash","untrusted_evidence":false}"#.utf8)
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase

        let context = try decoder.decode(AssistantContextItemState.self, from: data)

        XCTAssertEqual(context.id, "context-1")
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

    func testResourcePlannerReservesCapacityAndExcludesUnselectedContext() throws {
        let plan = try AssistantResourcePlanner.plan(
            capacity: AssistantModelCapacity(inputUnits: 100, outputReserveUnits: 20),
            reservations: [AssistantResourceReservation(id: "history", units: 20)],
            contexts: [
                AssistantContextResourceRequest(
                    id: "active", desiredUnits: 100, selected: true, active: true
                ),
                AssistantContextResourceRequest(
                    id: "selected", desiredUnits: 100, selected: true, active: false
                ),
                AssistantContextResourceRequest(
                    id: "unselected", desiredUnits: 100, selected: false, active: true
                ),
            ],
            corpusDesiredUnits: 100
        )
        XCTAssertEqual(plan.contextUnits, ["active": 30, "selected": 15])
        XCTAssertEqual(plan.contextUnits["unselected"], nil)
        XCTAssertEqual(plan.corpusUnits, 15)

        let unicode = String(repeating: "α", count: 100)
        let bounded = AssistantResourcePlanner.truncate(unicode, unitLimit: 61)
        XCTAssertLessThanOrEqual(AssistantResourcePlanner.encodedStringUnits(bounded), 61)
        XCTAssertTrue(bounded.isEmpty || bounded.last != "�")

        let escaped = String(repeating: "\\\"\n", count: 100)
        let escapedBounded = AssistantResourcePlanner.truncate(escaped, unitLimit: 80)
        XCTAssertLessThanOrEqual(AssistantResourcePlanner.encodedStringUnits(escapedBounded), 80)
    }

    func testResourcePlannerRedistributesUnusedDemandAndRejectsDuplicateContexts() throws {
        let plan = try AssistantResourcePlanner.plan(
            capacity: AssistantModelCapacity(inputUnits: 100, outputReserveUnits: 0),
            reservations: [],
            contexts: [
                AssistantContextResourceRequest(
                    id: "small-active", desiredUnits: 1, selected: true, active: true
                ),
                AssistantContextResourceRequest(
                    id: "large-selected", desiredUnits: 100, selected: true, active: false
                ),
            ],
            corpusDesiredUnits: 100
        )

        XCTAssertEqual(plan.contextUnits["small-active"], 1)
        XCTAssertEqual(plan.contextUnits["large-selected"], 50)
        XCTAssertEqual(plan.corpusUnits, 49)
        XCTAssertEqual(plan.contextUnits.values.reduce(0, +) + plan.corpusUnits, 100)

        XCTAssertThrowsError(try AssistantResourcePlanner.plan(
            capacity: AssistantModelCapacity(inputUnits: 100, outputReserveUnits: 0),
            reservations: [],
            contexts: [
                AssistantContextResourceRequest(
                    id: "duplicate", desiredUnits: 10, selected: true, active: true
                ),
                AssistantContextResourceRequest(
                    id: "duplicate", desiredUnits: 10, selected: true, active: false
                ),
            ],
            corpusDesiredUnits: 0
        )) { error in
            XCTAssertEqual(error as? AssistantResourcePlannerError, .duplicateContextID("duplicate"))
        }
    }

    func testResourcePlannerRepresentativeMeasurementFixture() throws {
        let capacity = AssistantModelCapacity(inputUnits: 32_768, outputReserveUnits: 4_096)
        let reservations = [
            AssistantResourceReservation(id: "runtime_instructions", units: 1_900),
            AssistantResourceReservation(id: "conversation_history", units: 4_096),
            AssistantResourceReservation(id: "context_metadata", units: 768),
        ]
        let contexts = [
            AssistantContextResourceRequest(
                id: "notebook", desiredUnits: 12_000, selected: true, active: true
            ),
            AssistantContextResourceRequest(
                id: "task", desiredUnits: 512, selected: true, active: false
            ),
            AssistantContextResourceRequest(
                id: "python", desiredUnits: 4_096, selected: true, active: false
            ),
            AssistantContextResourceRequest(
                id: "unselected-history", desiredUnits: 8_000, selected: false, active: false
            ),
        ]

        let started = CFAbsoluteTimeGetCurrent()
        var plan = try AssistantResourcePlanner.plan(
            capacity: capacity,
            reservations: reservations,
            contexts: contexts,
            corpusDesiredUnits: 8_000
        )
        for _ in 1..<10_000 {
            plan = try AssistantResourcePlanner.plan(
                capacity: capacity,
                reservations: reservations,
                contexts: contexts,
                corpusDesiredUnits: 8_000
            )
        }
        let elapsedMilliseconds = (CFAbsoluteTimeGetCurrent() - started) * 1_000
        print(
            "ASSISTANT_RESOURCE_MEASUREMENT iterations=10000 elapsed_ms=\(elapsedMilliseconds) "
                + "allocations=\(plan.contextUnits) corpus=\(plan.corpusUnits)"
        )

        XCTAssertEqual(plan.contextUnits["notebook"], 11_534)
        XCTAssertEqual(plan.contextUnits["task"], 512)
        XCTAssertEqual(plan.contextUnits["python"], 4_096)
        XCTAssertNil(plan.contextUnits["unselected-history"])
        XCTAssertEqual(plan.corpusUnits, 5_766)
        XCTAssertEqual(plan.contextUnits.values.reduce(0, +) + plan.corpusUnits, 21_908)
    }

    func testAssistantControllerUsesInjectedClockAndSchedulerWithoutSleeping() {
        let scheduler = RecordingAssistantScheduler()
        let controller = AssistantController(
            scheduler: scheduler,
            clock: FixedAssistantClock(value: 42)
        )
        var discussion = AssistantDiscussionState()
        discussion.activity = .streaming

        let effects = controller.handle(.messageDelta("hello"), discussion: &discussion)

        XCTAssertEqual(discussion.lastActivityAt, 42)
        XCTAssertTrue(effects.contains(.scheduleStreamFlush))
        XCTAssertTrue(effects.contains(.scheduleResponseTimeout(conversationID: nil)))
        var flushed = false
        controller.scheduleStreamFlush { flushed = true }
        XCTAssertEqual(scheduler.delays, [AssistantController.streamCoalescingDelay])
        XCTAssertFalse(flushed)
        scheduler.items.first?.perform()
        XCTAssertTrue(flushed)
        controller.flushPendingStream(into: &discussion)
        XCTAssertEqual(discussion.streamingText, "hello")
    }

    func testResourcePlannerRejectsOverflowAndOversubscribedReserves() {
        XCTAssertThrowsError(try AssistantResourcePlanner.plan(
            capacity: AssistantModelCapacity(inputUnits: 10, outputReserveUnits: 8),
            reservations: [AssistantResourceReservation(id: "history", units: 3)],
            contexts: [],
            corpusDesiredUnits: 0
        )) { error in
            XCTAssertEqual(
                error as? AssistantResourcePlannerError,
                .reservesExceedCapacity(required: 11, available: 10)
            )
        }
        XCTAssertThrowsError(try AssistantResourcePlanner.plan(
            capacity: AssistantModelCapacity(inputUnits: UInt64.max, outputReserveUnits: UInt64.max),
            reservations: [AssistantResourceReservation(id: "overflow", units: 1)],
            contexts: [],
            corpusDesiredUnits: 0
        ))
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
            "CASARS_LAUNCH_MODE": "installed_suite",
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

    func testDevelopmentProjectMCPLaunchUsesExactCargoPackageAndBinary() throws {
        let configuration = try AgentSessionConfiguration.discover(environment: [
            "CASA_RS_AGENT_COMMAND": "/usr/bin/false",
            "CASARS_LAUNCH_MODE": "development_workspace",
            "CASA_RS_REPO_ROOT": "/checkout",
            "CARGO": "/toolchain/cargo",
            "PATH": "",
        ])

        XCTAssertEqual(configuration.projectMCPExecutable, "/usr/bin/env")
        XCTAssertEqual(
            configuration.projectMCPArguments,
            [
                "/toolchain/cargo",
                "run", "--manifest-path", "/checkout/Cargo.toml", "-q",
                "-p", "casars-frontend-services", "--bin", "casars-project-mcp", "--",
            ]
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

    func testRuntimeApplicationContextSupersedesAResumedThreadsStaleNonce() throws {
        let staleNonce = String(repeating: "s", count: 32)
        let currentNonce = String(repeating: "c", count: 32)
        let currentProfile = CasaAgentRuntimeProfile(
            authority: .work,
            sessionNonce: currentNonce,
            pythonCommand: "/Users/scientist/bin/python"
        )

        let contexts = CodexAppServerSession.runtimeAdditionalContext(currentProfile)
        let context = try XCTUnwrap(contexts["casa-rs-runtime-profile"])

        XCTAssertEqual(context["kind"], "application")
        XCTAssertTrue(context["value"]?.contains(currentNonce) == true)
        XCTAssertTrue(context["value"]?.contains(currentProfile.mcpServerName) == true)
        XCTAssertFalse(context["value"]?.contains(staleNonce) == true)
        XCTAssertTrue(context["value"]?.contains("supersedes any earlier") == true)
        XCTAssertTrue(context["value"]?.contains("task.catalog") == true)
    }

    func testAppServerDeclaresCapabilityRequiredByPerTurnRuntimeContext() throws {
        let capabilities = try XCTUnwrap(
            CodexAppServerSession.initializeParams["capabilities"] as? [String: Bool]
        )

        XCTAssertEqual(capabilities["experimentalApi"], true)
    }

    func testAppServerTurnStartErrorBecomesVisibleAgentError() throws {
        let session = CodexAppServerSession(configuration: AgentSessionConfiguration(
            agentExecutable: "/usr/bin/false",
            projectMCPExecutable: "/project/bin/casars-project-mcp"
        ))
        let visibleError = expectation(description: "turn error surfaced")
        session.onEvent { event in
            if case let .failed(message) = event,
               message.contains("fixture rejected turn") {
                visibleError.fulfill()
            }
        }
        try session.receiveTurnStartErrorForTesting(
            requestID: 42,
            message: "fixture rejected turn"
        )
        wait(for: [visibleError], timeout: 1)
    }

    func testAppServerRequestLifecycleRejectsDuplicateUnknownLateMalformedAndUnsupportedEvents() throws {
        let session = CodexAppServerSession(configuration: AgentSessionConfiguration(
            agentExecutable: "/usr/bin/false",
            projectMCPExecutable: "/project/bin/casars-project-mcp"
        ))
        let surfaced = expectation(description: "protocol outcomes surfaced")
        surfaced.expectedFulfillmentCount = 5
        var events: [AgentSessionEvent] = []
        session.onEvent { event in
            events.append(event)
            surfaced.fulfill()
        }

        try session.registerRequestForTesting(requestID: 7, method: "account/read")
        try session.receiveJSONLineForTesting(["id": 7, "result": [:]])
        try session.receiveJSONLineForTesting(["id": 7, "result": [:]])
        try session.receiveJSONLineForTesting(["id": 8, "result": [:]])
        try session.registerRequestForTesting(requestID: 9, method: "model/list")
        session.terminate()
        try session.receiveJSONLineForTesting(["id": 9, "result": [:]])
        session.receiveRawLineForTesting("not-json")
        try session.receiveJSONLineForTesting([
            "method": "future/notification",
            "params": [:],
        ])

        wait(for: [surfaced], timeout: 1)
        XCTAssertTrue(events.contains { event in
            guard case let .failed(message) = event else { return false }
            return message.contains("duplicate terminal response")
        })
        XCTAssertTrue(events.contains { event in
            guard case let .failed(message) = event else { return false }
            return message.contains("unknown request 8")
        })
        XCTAssertTrue(events.contains { event in
            guard case let .failed(message) = event else { return false }
            return message.contains("unknown request 9")
        })
        XCTAssertTrue(events.contains { event in
            guard case let .failed(message) = event else { return false }
            return message.contains("not-json")
        })
        XCTAssertTrue(events.contains(.unsupported(method: "future/notification")))
    }

    func testAssistantControllerMakesBackendExitExplicit() {
        let controller = AssistantController()
        var discussion = AssistantDiscussionState()
        discussion.activity = .streaming

        let effects = controller.handle(
            .backendExited(status: 17, pendingRequests: ["turn/start", "model/list"]),
            discussion: &discussion
        )

        XCTAssertTrue(effects.isEmpty)
        XCTAssertEqual(discussion.activity, .restartRequired)
        XCTAssertEqual(
            discussion.lastError,
            "Agent backend exited with status 17 (pending: turn/start, model/list)."
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

    func testRapidStreamingDeltasAreFlushedIntoTheCompletedAnswer() throws {
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

        let chunks = (0..<500).map { "chunk-\($0);" }
        for chunk in chunks {
            agent.emit(.messageDelta(chunk))
        }
        XCTAssertEqual(store.state.assistantDiscussion?.streamingText, "")

        agent.emit(.turnCompleted(status: "completed", error: nil))

        XCTAssertEqual(
            store.state.assistantDiscussion?.activeConversation?.messages.last?.content,
            chunks.joined()
        )
        XCTAssertEqual(store.state.assistantDiscussion?.streamingText, "")
    }

    func testUnresponsiveAgentCannotLeaveConversationThinkingIndefinitely() throws {
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
            sessionId: "thread-unresponsive"
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
        store.setAssistantDraft("List the supported CASA-RS tasks")
        store.sendAssistantPrompt()

        XCTAssertEqual(store.state.assistantDiscussion?.activity, .streaming)
        XCTAssertEqual(agent.turns.count, 1)

        store.expireAssistantResponseForTesting()

        XCTAssertEqual(agent.restartCount, 1)
        XCTAssertTrue(
            store.state.assistantDiscussion?.lastError?.contains("did not report any activity") == true
        )
        XCTAssertNil(store.state.assistantDiscussion?.activeTurnID)
    }

    func testVisibleProgressTracksRealAgentEventsWithoutExposingReasoning() throws {
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
            sessionId: "thread-progress"
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

        store.setAssistantDraft("List supported tasks")
        store.sendAssistantPrompt()
        XCTAssertEqual(store.state.assistantDiscussion?.liveActivity?.label, "Request sent")
        XCTAssertNotNil(store.state.assistantDiscussion?.lastActivityAt)

        agent.emit(.turnStarted(id: "turn-progress"))
        XCTAssertEqual(store.state.assistantDiscussion?.liveActivity?.label, "Agent accepted request")

        agent.emit(.item(AgentItemDescriptor(
            id: "tool-progress", kind: "mcpToolCall", server: "other",
            tool: "task.catalog", completed: false, error: nil
        )))
        XCTAssertEqual(store.state.assistantDiscussion?.liveActivity?.label, "task.catalog")
        XCTAssertEqual(store.state.assistantDiscussion?.liveActivity?.state, "running")
        XCTAssertNil(store.state.assistantDiscussion?.liveActivity?.summary)
        XCTAssertEqual(store.debugSnapshot().assistantDiscussion?.liveActivityLabel, "task.catalog")
        XCTAssertNotNil(store.debugSnapshot().assistantDiscussion?.lastActivityAt)

        agent.emit(.messageDelta("Available tasks"))
        XCTAssertEqual(store.state.assistantDiscussion?.liveActivity?.label, "Writing response")
        agent.emit(.turnCompleted(status: "completed", error: nil))
        XCTAssertNil(store.state.assistantDiscussion?.liveActivity)
        XCTAssertNil(store.state.assistantDiscussion?.lastActivityAt)
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

        agent.emit(.accountLoggedOut)

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

        agent.emit(.conversationStarted(threadID: "thread-replacement"))
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
        let result = collectAllCorpus(
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
        XCTAssertEqual(result.refreshedLayers, ["baseline", "release_source", "live_source"])
    }

    func testProjectPDFCorpusSupportsPageCitationsDiagnosticsAndReplacementLifecycle() throws {
        let project = try temporaryProject()
        let source = try temporaryProject()
        defer {
            try? FileManager.default.removeItem(at: project)
            try? FileManager.default.removeItem(at: source)
        }
        let documents = project.appendingPathComponent("documents", isDirectory: true)
        try FileManager.default.createDirectory(at: documents, withIntermediateDirectories: true)
        try "# Architecture".write(
            to: source.appendingPathComponent("ARCHITECTURE.md"),
            atomically: true,
            encoding: .utf8
        )
        try #"{"schema_version":1,"release":"v-test","commit":"abc123"}"#.write(
            to: source.appendingPathComponent("casars-source.json"),
            atomically: true,
            encoding: .utf8
        )
        let paper = documents.appendingPathComponent("paper.pdf")
        try writeTextPDF([
            "First-page control phrase amber telescope.",
            "Second-page scientific phrase quadrature zephyr.",
        ], to: paper)
        try Data("not a PDF".utf8).write(to: documents.appendingPathComponent("broken.pdf"))
        try Data([0, 1, 2]).write(to: documents.appendingPathComponent("unsupported.docx"))

        let environment = ["CASA_RS_SOURCE_ROOT": source.path]
        let client = UniFFIAssistantPersistenceClient()
        let first = collectAllCorpus(
            projectRoot: project.path,
            environment: environment
        )
        XCTAssertTrue(first.diagnostics.contains { $0.contains("Could not open PDF documents/broken.pdf") })
        XCTAssertTrue(first.diagnostics.contains { $0.contains("Unsupported corpus file type documents/unsupported.docx") })
        XCTAssertEqual(
            first.documents.filter { $0.citation.sourcePath == "documents/paper.pdf" }.map(\.citation.page),
            [1, 2]
        )
        let firstReport = try client.applyTestReconciliation(
            projectRoot: project.path,
            documents: first.documents,
            removeMissingLayers: first.refreshedLayers,
            projectSources: first.projectSources,
            failedProjectSources: first.failedProjectSources
        )
        XCTAssertGreaterThan(firstReport.indexedDocuments, 0)
        let pageTwo = try XCTUnwrap(
            client.searchCorpus(projectRoot: project.path, query: "quadrature zephyr", limit: 4).first
        )
        XCTAssertEqual(pageTwo.citation.sourcePath, "documents/paper.pdf")
        XCTAssertEqual(pageTwo.citation.page, 2)

        try writeTextPDF(["Replacement phrase ultraviolet marmalade."], to: paper)
        let changed = collectAllCorpus(
            projectRoot: project.path,
            environment: environment
        )
        let changedReport = try client.applyTestReconciliation(
            projectRoot: project.path,
            documents: changed.documents,
            removeMissingLayers: changed.refreshedLayers,
            projectSources: changed.projectSources,
            failedProjectSources: changed.failedProjectSources
        )
        XCTAssertGreaterThanOrEqual(changedReport.indexedDocuments, 1)
        XCTAssertGreaterThanOrEqual(changedReport.removedDocuments, 1)
        XCTAssertTrue(try client.searchCorpus(
            projectRoot: project.path,
            query: "quadrature zephyr",
            limit: 4
        ).allSatisfy { $0.citation.sourcePath != "documents/paper.pdf" })
        XCTAssertEqual(
            try client.searchCorpus(projectRoot: project.path, query: "ultraviolet marmalade", limit: 4)
                .first?.citation.page,
            1
        )

        try FileManager.default.removeItem(at: paper)
        let removed = collectAllCorpus(
            projectRoot: project.path,
            environment: environment
        )
        let removedReport = try client.applyTestReconciliation(
            projectRoot: project.path,
            documents: removed.documents,
            removeMissingLayers: removed.refreshedLayers,
            projectSources: removed.projectSources,
            failedProjectSources: removed.failedProjectSources
        )
        XCTAssertGreaterThanOrEqual(removedReport.removedDocuments, 1)
        XCTAssertTrue(try client.searchCorpus(
            projectRoot: project.path,
            query: "ultraviolet marmalade",
            limit: 4
        ).allSatisfy { $0.citation.sourcePath != "documents/paper.pdf" })
    }

    func testOptInPublicScientificPDFUsesProductionExtractionAndCitationBoundary() throws {
        guard let pdfPath = ProcessInfo.processInfo.environment["CASA_RS_WAVE5B_PUBLIC_PDF"] else {
            throw XCTSkip("Set CASA_RS_WAVE5B_PUBLIC_PDF to a downloaded public scientific PDF.")
        }
        let sourceRoot = ProcessInfo.processInfo.environment["CASA_RS_SOURCE_ROOT"]
            ?? FileManager.default.currentDirectoryPath
        let project = try temporaryProject()
        defer { try? FileManager.default.removeItem(at: project) }
        let documents = project.appendingPathComponent("documents", isDirectory: true)
        try FileManager.default.createDirectory(at: documents, withIntermediateDirectories: true)
        let staged = documents.appendingPathComponent("sidereal-visibility-averaging.pdf")
        try FileManager.default.copyItem(atPath: pdfPath, toPath: staged.path)

        let result = collectAllCorpus(
            projectRoot: project.path,
            environment: ["CASA_RS_SOURCE_ROOT": sourceRoot]
        )
        let pages = result.documents.filter {
            $0.citation.sourcePath == "documents/sidereal-visibility-averaging.pdf"
        }
        XCTAssertEqual(pages.count, 9)
        XCTAssertEqual(pages.map(\.citation.page), Array(1 ... 9).map(UInt32.init))

        let client = UniFFIAssistantPersistenceClient()
        let report = try client.applyTestReconciliation(
            projectRoot: project.path,
            documents: result.documents,
            removeMissingLayers: result.refreshedLayers,
            projectSources: result.projectSources,
            failedProjectSources: result.failedProjectSources
        )
        let hits = try client.searchCorpus(
            projectRoot: project.path,
            query: "3000 hours factor 169 14-fold",
            limit: 8
        )
        let scientificHit = try XCTUnwrap(hits.first {
            $0.citation.sourcePath == "documents/sidereal-visibility-averaging.pdf"
                && $0.citation.page == 1
                && $0.text.contains("169")
                && $0.text.contains("14-fold")
        })
        XCTAssertEqual(scientificHit.citation.locator, "documents/sidereal-visibility-averaging.pdf, page 1")
        let baselineHit = try XCTUnwrap(
            client.searchCorpus(
                projectRoot: project.path,
                query: "redistribution-cleared concise orientation substitute observatory documentation",
                limit: 12
            ).first {
                $0.layer == "baseline"
                    && $0.citation.sourcePath?.hasSuffix("radio-interferometry-primer.md") == true
            }
        )
        let sourceHit = try XCTUnwrap(
            client.searchCorpus(
                projectRoot: project.path,
                query: "CORPUS_SCHEMA_VERSION MAX_CHUNK_BYTES retrieval unit",
                limit: 12
            ).first {
                $0.layer == "live_source"
                    && $0.citation.sourcePath == "crates/casa-notebook/src/corpus.rs"
                    && $0.citation.commit != nil
                    && $0.citation.lineStart != nil
                    && $0.citation.lineEnd != nil
            }
        )
        print(
            "CASA_RS_WAVE5B_PDF indexed=\(report.indexedDocuments) chunks=\(report.chunkCount) "
                + "citation=\(scientificHit.citation.locator) baseline=\(baselineHit.citation.locator) "
                + "source=\(sourceHit.citation.locator) commit=\(sourceHit.citation.commit ?? "missing") "
                + "lines=\(sourceHit.citation.lineStart ?? 0)-\(sourceHit.citation.lineEnd ?? 0) "
                + "diagnostics=\(result.diagnostics)"
        )
    }

    func testPinDestinationIsAlwaysChronologicalTail() throws {
        let project = try temporaryProject()
        defer { try? FileManager.default.removeItem(at: project) }
        let client = UniFFIAssistantPersistenceClient()
        let conversationID = UUID().uuidString.lowercased()
        let notebookID = UUID().uuidString.lowercased()
        let messageID = UUID().uuidString.lowercased()
        let pin = try client.createPin(AssistantCreatePinRequest(
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
                parameters: [
                    "vis": "input.ms",
                    "imagename": "products/image",
                    "robust": "-0.5",
                    "weighting": "briggs",
                ],
                validatedPatch: SurfaceParameterPatch(values: [
                    "vis": .string("input.ms"),
                    "imagename": .string("products/image"),
                    "robust": .float(-0.5),
                    "weighting": .string("briggs"),
                ])
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

        store.setGenericTaskValue(
            taskID: "imager",
            instanceID: tab.id,
            argumentID: "robust",
            value: "-0.25"
        )
        XCTAssertFalse(store.parameterIsAssistantSuggested(
            surfaceID: "imager",
            instanceID: tab.id,
            name: "robust"
        ))
        XCTAssertTrue(store.parameterIsAssistantSuggested(
            surfaceID: "imager",
            instanceID: tab.id,
            name: "weighting"
        ))
    }

    func testPinningTaskSuggestionAppendsTypedTaskCellAtNotebookTail() throws {
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
        let messageID = "019f0000-0000-7000-8000-000000000516"
        conversation.messages.append(AssistantMessageState(
            id: messageID,
            role: "assistant",
            content: "Start with a small ALMA mosaic.",
            createdAt: 1,
            agentId: "fixture",
            model: "fixture",
            citations: [],
            usedContext: [],
            activities: [],
            taskSuggestions: [AssistantTaskSuggestionState(
                id: "simobserve-suggestion",
                taskId: "simobserve",
                parameters: [
                    "request_kind": "family",
                    "telescope": "ALMA",
                    "array_config": "alma.cycle10.5.cfg",
                    "band": "Band 6",
                    "pointing_count": "4",
                    "output_ms": "products/alma-mosaic.ms",
                ]
            )],
            pins: []
        ))
        try client.saveConversation(projectRoot: project.path, transcript: conversation)
        var discussion = AssistantDiscussionState()
        discussion.conversations = [conversation]
        discussion.activeConversationID = conversation.id
        var state = FixtureWorkbench.makeState()
        state.project.rootPath = project.path
        state.assistantDiscussion = discussion
        let store = WorkbenchStore(state: state)
        store.createScientificNotebook(filename: "Analysis.md", title: "Analysis")

        store.pinAssistantMessage(messageID)

        let notebook = try XCTUnwrap(store.state.scientificNotebooks?.activeNotebook)
        XCTAssertTrue(notebook.source.contains("casa-rs-ai-pin:v1"), notebook.source)
        XCTAssertTrue(notebook.source.contains("surface = \"simobserve\""), notebook.source)
        XCTAssertTrue(notebook.source.contains("pointing_count = 4"), notebook.source)
        let task = try XCTUnwrap(notebook.cells.last(where: { $0.kind == "task" }))
        XCTAssertEqual(task.taskIntent?.surface, "simobserve")
        XCTAssertEqual(task.taskIntent?.parameters["band"], .string("Band 6"))
        XCTAssertEqual(task.taskIntent?.parameters["pointing_count"], .number(4))
        XCTAssertFalse(
            store.state.assistantDiscussion?.activeConversation?.messages.last?.pins.isEmpty ?? true
        )
    }

    func testTaskSuggestionAppliesModeDependentParametersAtomicallyAndRejectsInvalidDrafts() throws {
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
            content: "Create an ALMA mosaic.",
            createdAt: 1,
            agentId: "fixture",
            model: "fixture",
            citations: [],
            usedContext: [],
            activities: [],
            taskSuggestions: [
                AssistantTaskSuggestionState(
                    id: "invalid",
                    taskId: "simobserve",
                    parameters: [
                        "request_kind": "family",
                        "telescope": "ALMA",
                        "polarization_basis": "linear",
                    ]
                ),
                AssistantTaskSuggestionState(
                    id: "valid",
                    taskId: "simobserve",
                    parameters: [
                        "request_kind": "family",
                        "telescope": "ALMA",
                        "array_config": "alma.cycle10.5.cfg",
                        "band": "Band 6",
                        "pointing_count": "4",
                        "output_ms": "products/alma-mosaic.ms",
                    ],
                    validatedPatch: SurfaceParameterPatch(values: [
                        "request_kind": .string("family"),
                        "telescope": .string("ALMA"),
                        "array_config": .string("alma.cycle10.5.cfg"),
                        "band": .string("Band 6"),
                        "pointing_count": .integer(4),
                        "output_ms": .string("products/alma-mosaic.ms"),
                    ])
                ),
            ],
            pins: []
        ))
        var discussion = AssistantDiscussionState()
        discussion.conversations = [conversation]
        discussion.activeConversationID = conversation.id
        var state = FixtureWorkbench.makeState()
        state.project.rootPath = project.path
        state.assistantDiscussion = discussion
        let store = WorkbenchStore(state: state)

        let initialTabs = store.state.tabs.count
        store.openAssistantTaskSuggestion(messageID: "answer", suggestionID: "invalid")
        XCTAssertEqual(store.state.tabs.count, initialTabs)

        store.openAssistantTaskSuggestion(messageID: "answer", suggestionID: "valid")
        let tab = try XCTUnwrap(store.state.tabs.first { $0.id == store.state.activeTabID })
        XCTAssertEqual(tab.taskID, "simobserve")
        XCTAssertEqual(
            store.parameterText(surfaceID: "simobserve", instanceID: tab.id, name: "request_kind"),
            "family"
        )
        XCTAssertEqual(
            store.parameterText(surfaceID: "simobserve", instanceID: tab.id, name: "array_config"),
            "alma.cycle10.5.cfg"
        )
        XCTAssertFalse(
            try XCTUnwrap(store.parameterSession(surfaceID: "simobserve", instanceID: tab.id)).hasErrors
        )
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
        agent.emit(.item(AgentItemDescriptor(
            id: "forged-tool", kind: "mcpToolCall", server: "casa_rs_untrusted",
            tool: "corpus.search", completed: true, error: nil
        )))

        agent.emit(.item(AgentItemDescriptor(
            id: "tool-1", kind: "mcpToolCall", server: trustedServer,
            tool: "corpus.search", completed: true, error: nil,
            citations: [AssistantCitationState(
                id: "paper:1",
                kind: "document",
                label: "Paper",
                locator: "documents/paper.pdf, page 2",
                excerpt: "Evidence",
                sourcePath: "documents/paper.pdf",
                page: 2,
                section: nil,
                lineStart: nil,
                lineEnd: nil,
                release: nil,
                commit: nil
            )]
        )))
        agent.emit(.messageDelta("Cited answer"))
        agent.emit(.turnCompleted(status: "completed", error: nil))

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

        agent.emit(.item(AgentItemDescriptor(
            id: "tool-failure", kind: "mcpToolCall", server: trustedServer,
            tool: "corpus.search", completed: true, error: "index unavailable"
        )))
        agent.emit(.turnCompleted(status: "completed", error: nil))
        XCTAssertEqual(
            store.state.assistantDiscussion?.activeConversation?.messages.last?.activities.first?.state,
            "failed"
        )

        agent.emit(.turnCompleted(status: "cancelled", error: nil))
        XCTAssertEqual(
            store.state.assistantDiscussion?.activeConversation?.messages.last?.content,
            "Agent response cancelled."
        )

        agent.emit(.usage(AgentUsageDescriptor(
            plan: nil, primaryPercentUsed: 100, secondaryPercentUsed: nil,
            primaryResetAt: 42, secondaryResetAt: nil
        )))
        XCTAssertEqual(store.state.assistantDiscussion?.usage.primaryPercentUsed, 100.0)

        agent.emit(.approval(AgentApprovalDescriptor(
            id: "approval-1",
            method: "item/commandExecution/requestApproval",
            summary: "touch outside-project"
        )))
        store.resolveAssistantApproval("decline")
        XCTAssertEqual(agent.approvals.last?.requestID, "approval-1")
        XCTAssertEqual(agent.approvals.last?.decision, "decline")

        agent.emit(.failed("agent process exited"))
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

        agent.emit(.resumeFailed("thread not found"))

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

    func testDeterministicAgentTaskSuggestionIncludesValidatedPatch() throws {
        let fixture = DeterministicAgentSession()
        var events: [AgentSessionEvent] = []
        fixture.onEvent { events.append($0) }
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
        fixture.sendTurn(AgentTurnRequest(
            threadID: "fixture-codex-thread",
            text: "Suggest an imaging task",
            model: "fixture-model",
            effort: "medium"
        ))

        let toolEvent = try XCTUnwrap(events.compactMap { event -> AgentItemDescriptor? in
            guard case let .item(item) = event, item.tool == "task.suggest" else { return nil }
            return item
        }.first)
        let suggestion = try XCTUnwrap(toolEvent.taskSuggestions.first)
        let patch = suggestion.validatedPatch

        XCTAssertEqual(patch.values["vis"], .string("input.ms"))
        XCTAssertEqual(patch.values["weighting"], .string("briggs"))
        XCTAssertEqual(patch.values["robust"], .float(-0.5))
        XCTAssertTrue(patch.unset.isEmpty)
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
            if ProcessInfo.processInfo.environment["CASA_RS_CODEX_LIVE_TRACE"] == "1" {
                print("CODEX_EVENT \(event)")
            }
            switch event {
            case let .account(accountState):
                if !observedAccount, !accountState.requiresLogin {
                    observedAccount = true
                    account.fulfill()
                }
            case let .conversationStarted(id):
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
            case let .mcpStatus(name, status):
                if name.hasPrefix("casa_rs_"), status == "ready", !observedCasaMCPReady {
                    observedCasaMCPReady = true
                    casaMCP.fulfill()
                }
            case let .item(item) where item.completed:
                if item.kind == "mcpToolCall",
                   item.server?.hasPrefix("casa_rs_") == true,
                   !observedCasaTool
                {
                    observedCasaTool = true
                    casaTool.fulfill()
                } else if !workRequested
                    && (["commandExecution", "webSearch", "computerUse"].contains(item.kind)
                    || (item.kind == "mcpToolCall"
                        && item.server?.hasPrefix("casa_rs_") != true))
                {
                    observedGenericAuthority = true
                }
            case .turnCompleted where !resumeRequested:
                    completed.fulfill()
                    resumeRequested = true
                    session.startConversation(AgentConversationRequest(
                        projectRoot: project.path,
                        model: "",
                        effort: "low",
                        resumeThreadID: threadID,
                        runtimeProfile: profile
                    ))
            case .turnCompleted where workRequested && workThreadID != nil:
                workCompleted.fulfill()
            case let .approval(approval) where workRequested:
                workApprovalCount += 1
                if workApprovalCount == 1 { workApproval.fulfill() }
                session.approve(requestID: approval.id, decision: "decline")
            default:
                break
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

    func testOptInCodexSubscriptionUsesScientificAndSourceCorpusTools() throws {
        guard ProcessInfo.processInfo.environment["CASA_RS_CODEX_LIVE_CORPUS"] == "1",
              let pdfPath = ProcessInfo.processInfo.environment["CASA_RS_WAVE5B_PUBLIC_PDF"]
        else {
            throw XCTSkip("Set CASA_RS_CODEX_LIVE_CORPUS=1 and CASA_RS_WAVE5B_PUBLIC_PDF.")
        }
        let project = try temporaryProject()
        defer { try? FileManager.default.removeItem(at: project) }
        let documents = project.appendingPathComponent("documents", isDirectory: true)
        try FileManager.default.createDirectory(at: documents, withIntermediateDirectories: true)
        try FileManager.default.copyItem(
            atPath: pdfPath,
            toPath: documents.appendingPathComponent("sidereal-visibility-averaging.pdf").path
        )
        let sourceRoot = ProcessInfo.processInfo.environment["CASA_RS_SOURCE_ROOT"]
            ?? FileManager.default.currentDirectoryPath
        let ingestion = collectAllCorpus(
            projectRoot: project.path,
            environment: ["CASA_RS_SOURCE_ROOT": sourceRoot]
        )
        let expectedCommit = try XCTUnwrap(ingestion.documents.first {
            $0.citation.sourcePath == "crates/casa-notebook/src/corpus.rs"
        }?.citation.commit)
        let persistence = UniFFIAssistantPersistenceClient()
        _ = try persistence.applyTestReconciliation(
            projectRoot: project.path,
            documents: ingestion.documents,
            removeMissingLayers: ingestion.refreshedLayers,
            projectSources: ingestion.projectSources,
            failedProjectSources: ingestion.failedProjectSources
        )

        let session = CodexAppServerSession(configuration: try .discover())
        defer { session.terminate() }
        let ready = expectation(description: "Codex App Server ready")
        let account = expectation(description: "ChatGPT subscription account")
        let threadStarted = expectation(description: "project-aware Codex thread started")
        let turnFinished = expectation(description: "retrieval turn reached a terminal event")
        let nonce = String(repeating: "r", count: 32)
        let profile = CasaAgentRuntimeProfile(
            authority: .explore,
            sessionNonce: nonce,
            pythonCommand: "python3"
        )
        var observedAccount = false
        var observedCorpusTool = false
        var observedSourceTool = false
        var sent = false
        var answer = ""
        var agentError: String?
        var eventTrace: [String] = []
        session.onEvent { event in
            eventTrace.append(String(describing: event))
            switch event {
            case let .account(accountState):
                if !observedAccount, !accountState.requiresLogin {
                    observedAccount = true
                    account.fulfill()
                }
            case let .conversationStarted(id) where !sent:
                    sent = true
                    threadStarted.fulfill()
                    session.sendTurn(AgentTurnRequest(
                        threadID: id,
                        text: """
                        Use only the \(profile.mcpServerName) project tools, with the exact current nonce. First call corpus.search for the 3000-hour SVA estimate in the project PDF. Then call source.search for CORPUS_SCHEMA_VERSION. Reply with the estimated data-volume and computing-time reductions, the PDF page locator, and the casa-rs source path, commit, and line range. Do not use shell or web.
                        """,
                        model: "",
                        effort: "low"
                    ))
            case let .failed(message):
                if agentError == nil {
                    agentError = message
                    if sent { turnFinished.fulfill() }
                }
            case let .item(item)
                where item.completed && item.kind == "mcpToolCall"
                    && item.server == profile.mcpServerName:
                if item.tool == "corpus.search", !observedCorpusTool {
                    observedCorpusTool = true
                }
                if item.tool == "source.search", !observedSourceTool {
                    observedSourceTool = true
                }
            case let .messageDelta(delta):
                answer += delta
            case .turnCompleted:
                if agentError == nil { turnFinished.fulfill() }
            default:
                break
            }
        }
        session.prepare { result in
            if case let .failure(error) = result { XCTFail("Codex failed: \(error)") }
            ready.fulfill()
        }
        wait(for: [ready, account], timeout: 25)
        session.startConversation(AgentConversationRequest(
            projectRoot: project.path,
            model: "",
            effort: "low",
            resumeThreadID: nil,
            runtimeProfile: profile
        ))
        guard XCTWaiter.wait(for: [threadStarted], timeout: 25) == .completed else {
            XCTFail("Project-aware Codex thread did not start. Events: \(eventTrace)")
            return
        }
        guard XCTWaiter.wait(for: [turnFinished], timeout: 120) == .completed else {
            XCTFail("Agent retrieval turn did not complete. Events: \(eventTrace)")
            return
        }
        XCTAssertNil(agentError, "Agent failed: \(agentError ?? "unknown"). Events: \(eventTrace)")
        XCTAssertTrue(observedCorpusTool, "corpus.search was not called. Events: \(eventTrace)")
        XCTAssertTrue(observedSourceTool, "source.search was not called. Events: \(eventTrace)")
        XCTAssertTrue(answer.contains("169"), answer)
        XCTAssertTrue(answer.contains("14"), answer)
        XCTAssertTrue(answer.contains("page 1"), answer)
        XCTAssertTrue(answer.contains("crates/casa-notebook/src/corpus.rs"), answer)
        XCTAssertTrue(answer.contains(expectedCommit), answer)
        print("CASA_RS_WAVE5B_LIVE_AGENT \(answer)")
    }

    private func writeTextPDF(_ pages: [String], to url: URL) throws {
        try? FileManager.default.removeItem(at: url)
        guard let consumer = CGDataConsumer(url: url as CFURL) else {
            throw CocoaError(.fileWriteUnknown)
        }
        var mediaBox = CGRect(x: 0, y: 0, width: 612, height: 792)
        guard let context = CGContext(consumer: consumer, mediaBox: &mediaBox, nil) else {
            throw CocoaError(.fileWriteUnknown)
        }
        for text in pages {
            context.beginPDFPage(nil)
            let graphics = NSGraphicsContext(cgContext: context, flipped: false)
            NSGraphicsContext.saveGraphicsState()
            NSGraphicsContext.current = graphics
            (text as NSString).draw(
                in: NSRect(x: 72, y: 650, width: 468, height: 72),
                withAttributes: [.font: NSFont.systemFont(ofSize: 18)]
            )
            NSGraphicsContext.restoreGraphicsState()
            context.endPDFPage()
        }
        context.closePDF()
    }

    private func collectAllCorpus(
        projectRoot: String,
        environment: [String: String]
    ) -> AssistantCorpusIngestionResult {
        let ingestor = AssistantCorpusIngestor()
        let inventory = ingestor.projectDocumentInventory(projectRoot: projectRoot)
        return ingestor.collect(
            projectRoot: projectRoot,
            environment: environment,
            projectInventory: inventory,
            extractProjectPaths: Set(inventory.sources.map(\.relativePath)),
            scope: .allLayers
        )
    }

    private func temporaryProject() throws -> URL {
        let project = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-assistant-tests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: project, withIntermediateDirectories: true)
        return project
    }
}

private struct FixedAssistantClock: AssistantClock {
    let value: UInt64

    func timestamp() -> UInt64 { value }
}

private final class RecordingAssistantScheduler: AssistantScheduling {
    var delays: [TimeInterval] = []
    var items: [DispatchWorkItem] = []

    func schedule(after delay: TimeInterval, _ action: @escaping () -> Void) -> DispatchWorkItem {
        let item = DispatchWorkItem(block: action)
        delays.append(delay)
        items.append(item)
        return item
    }
}

private final class FixtureAgentSession: AgentSession {
    var conversations: [AgentConversationRequest] = []
    var turns: [AgentTurnRequest] = []
    var restartCount = 0
    var accountLogoutCount = 0
    var approvals: [(requestID: String, decision: String)] = []
    private var eventHandler: ((AgentSessionEvent) -> Void)?
    private var stateHandler: ((AssistantDiscussionActivity) -> Void)?

    func onEvent(_ handler: @escaping (AgentSessionEvent) -> Void) { eventHandler = handler }
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
    func emit(_ event: AgentSessionEvent) { eventHandler?(event) }
}
