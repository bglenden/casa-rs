@testable import CasarsMacCore
import XCTest

final class AIChatPrototypeTests: XCTestCase {
    func testFactoryCreatesCodexSubscriptionFixtureWithoutProductionCalls() throws {
        let store = WorkbenchStore.aiPrototype()
        let projection = try XCTUnwrap(store.state.prototypeAI)

        XCTAssertTrue(store.isAIPrototypeRuntime)
        XCTAssertEqual(projection.agents.map(\.id), ["codex-app-server", "opencode-acp"])
        XCTAssertTrue(projection.agents.first?.enabled == true)
        XCTAssertTrue(projection.agents.last?.enabled == false)
        XCTAssertEqual(projection.selectedAgentID, "codex-app-server")
        XCTAssertEqual(projection.account.label, "ChatGPT Pro")
        XCTAssertEqual(projection.account.funding, "Subscription · no API billing")
        XCTAssertEqual(projection.reasoningEffort, .medium)
        XCTAssertEqual(projection.usage.fiveHourRemainingPercent, 72)
        XCTAssertEqual(projection.usage.weeklyRemainingPercent, 44)
        XCTAssertEqual(projection.trustPreset, .work)
        XCTAssertEqual(projection.selectedPythonEnvironmentID, "casa-python")
        XCTAssertEqual(projection.presentation, .closed)
        XCTAssertEqual(store.state.activeTabID, "tab-scientific-notebook")
        XCTAssertEqual(projection.openTabSources.count, 5)
        XCTAssertTrue(projection.workspaceSources.contains { $0.id == "corpus-radio" })
        XCTAssertTrue(projection.workspaceSources.contains { $0.id == "source-casars" })
        XCTAssertEqual(store.prototypeProductionBoundaryInvocationCount, 0)
    }

    func testDrawerAndExpandedTabShareDraftAndConversationState() throws {
        let store = WorkbenchStore.aiPrototype()
        store.setAIPrototypeDraft("Explain the current Imager parameters")
        store.openAIPrototypeDrawer()
        XCTAssertEqual(store.state.prototypeAI?.presentation, .drawer)
        XCTAssertFalse(store.state.tabs.contains { $0.id == "tab-ai-prototype" })

        store.expandAIPrototypeConversation()
        XCTAssertEqual(store.state.prototypeAI?.presentation, .tab)
        XCTAssertEqual(store.state.activeTabID, "tab-ai-prototype")
        XCTAssertEqual(store.state.prototypeAI?.draft, "Explain the current Imager parameters")

        store.dockAIPrototypeConversation()
        XCTAssertEqual(store.state.prototypeAI?.presentation, .drawer)
        XCTAssertEqual(store.state.activeTabID, "tab-scientific-notebook")

        store.closeAIPrototypeConversation()
        XCTAssertEqual(store.state.prototypeAI?.presentation, .closed)
        XCTAssertEqual(store.state.prototypeAI?.draft, "Explain the current Imager parameters")
        XCTAssertTrue(store.state.prototypeAI?.messages.isEmpty == true)
        XCTAssertEqual(store.prototypeProductionBoundaryInvocationCount, 0)
    }

    func testModelEffortAndConsolidatedSettingsAreLiveFixtures() throws {
        let store = WorkbenchStore.aiPrototype()

        store.selectAIPrototypeAgent("opencode-acp")
        XCTAssertEqual(store.state.prototypeAI?.selectedAgentID, "codex-app-server")

        store.selectAIPrototypeModel("gpt-5.3-codex")
        store.selectAIPrototypeReasoningEffort(.high)
        store.selectAIPrototypeTrustPreset(.explore)
        store.selectAIPrototypePythonEnvironment("login-python")

        let projection = try XCTUnwrap(store.state.prototypeAI)
        XCTAssertEqual(projection.selectedModel, "gpt-5.3-codex")
        XCTAssertEqual(projection.reasoningEffort, .high)
        XCTAssertEqual(projection.trustPreset, .explore)
        XCTAssertEqual(projection.selectedPythonEnvironment?.label, "Login-shell Python")
        XCTAssertEqual(projection.contexts.map(\.id), ["project", "paper", "source", "semantics"])
        XCTAssertEqual(store.prototypeProductionBoundaryInvocationCount, 0)
    }

    func testAnswerCanAppendOnceAtNotebookTailAndOpenCanonicalTask() throws {
        let store = WorkbenchStore.aiPrototype()
        let originalMarkdown = try XCTUnwrap(store.state.prototypeNotebook?.draftMarkdown)
        let completed = expectation(description: "fixture answer")

        store.sendAIPrototypePrompt("Compare the plot")
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.2) {
            completed.fulfill()
        }
        wait(for: [completed], timeout: 2)

        let message = try XCTUnwrap(store.state.prototypeAI?.messages.last)
        XCTAssertEqual(message.role, .assistant)
        XCTAssertEqual(message.activity.count, 3)
        XCTAssertEqual(message.suggestedTaskID, "imager")

        store.pinAIPrototypeMessage(message.id)
        let appended = try XCTUnwrap(store.state.prototypeNotebook?.draftMarkdown)
        XCTAssertTrue(appended.hasPrefix(originalMarkdown))
        XCTAssertTrue(appended.hasSuffix("- [casa-ms source] crates/casa-ms/src/msexplore.rs · build_plot_document"))
        XCTAssertEqual(store.state.prototypeAI?.pinnedMessageCount, 1)
        XCTAssertEqual(store.state.prototypeAI?.notebookPinFocusGeneration, 1)

        store.pinAIPrototypeMessage(message.id)
        XCTAssertEqual(store.state.prototypeNotebook?.draftMarkdown, appended)
        XCTAssertEqual(store.state.prototypeAI?.pinnedMessageCount, 1)

        store.openAIPrototypeTaskSuggestion()
        let taskTab = try XCTUnwrap(store.state.tabs.first { $0.id == "tab-ai-context-task" })
        XCTAssertEqual(taskTab.kind, .task)
        XCTAssertEqual(taskTab.taskID, "imager")
        XCTAssertEqual(taskTab.prototypeReceiptID, "receipt-imager-cancelled")
        let task = try XCTUnwrap(
            store.state.prototypeNotebook?.receipts.first { $0.id == "receipt-imager-cancelled" }
        )
        XCTAssertEqual(task.parameterRows.first { $0.parameterID == "robust" }?.value, "-0.5")
        XCTAssertEqual(store.prototypeProductionBoundaryInvocationCount, 0)
    }

    func testRateLimitRequiresExplicitRetryAndThenCompletes() throws {
        var projection = PrototypeAIChatFixtureAdapter.make(scenario: .rateLimited)
        let firstGeneration = try XCTUnwrap(projection.beginResponse(prompt: "Explain the plot"))
        projection.completeResponse(generation: firstGeneration)
        XCTAssertEqual(projection.responseState, .rateLimited)
        XCTAssertEqual(projection.messages.count, 1)

        let prompt = try XCTUnwrap(projection.activePrompt)
        projection.restartResponse()
        let retryGeneration = try XCTUnwrap(projection.beginResponse(prompt: prompt))
        projection.completeResponse(generation: retryGeneration)

        XCTAssertEqual(projection.responseState, .completed)
        XCTAssertEqual(projection.messages.last?.citations.count, 2)
        XCTAssertEqual(projection.messages.last?.agentLabel, "Codex")
    }

    func testNonresponsiveCancellationRequiresExplicitAgentRestart() throws {
        var projection = PrototypeAIChatFixtureAdapter.make(scenario: .nonresponsive)
        let generation = try XCTUnwrap(projection.beginResponse(prompt: "Keep working"))
        projection.completeResponse(generation: generation)
        XCTAssertEqual(projection.responseState, .streaming)

        projection.cancelResponse()
        XCTAssertEqual(projection.responseState, .restartRequired)
        projection.restartResponse()
        XCTAssertEqual(projection.responseState, .idle)
        XCTAssertNil(projection.activePrompt)
    }

    func testFullAccessSelectionIsExplicitFixtureState() {
        let store = WorkbenchStore.aiPrototype()
        XCTAssertEqual(store.state.prototypeAI?.trustPreset, .work)
        store.selectAIPrototypeTrustPreset(.fullAccess)
        XCTAssertEqual(store.state.prototypeAI?.trustPreset, .fullAccess)
        store.selectAIPrototypeTrustPreset(.work)
        XCTAssertEqual(store.state.prototypeAI?.trustPreset, .work)
        XCTAssertEqual(store.prototypeProductionBoundaryInvocationCount, 0)
    }

    func testDebugSnapshotExposesReplacementPrototypeReviewState() throws {
        let store = WorkbenchStore.aiPrototype()
        store.setAIPrototypeDraft("Explain the open tabs")
        store.openAIPrototypeDrawer()
        store.selectAIPrototypeTrustPreset(.explore)
        store.selectAIPrototypePythonEnvironment("login-python")

        let debug = try XCTUnwrap(store.debugSnapshot().prototypeAI)
        XCTAssertEqual(debug.scenario, .primary)
        XCTAssertEqual(debug.presentation, .drawer)
        XCTAssertEqual(debug.primaryAttachment, "notebooks/Analysis.md")
        XCTAssertEqual(debug.draft, "Explain the open tabs")
        XCTAssertEqual(debug.openTabSourceIDs.count, 5)
        XCTAssertTrue(debug.workspaceSourceIDs.contains("schema-casars"))
        XCTAssertEqual(debug.agent, "Codex")
        XCTAssertEqual(debug.reasoningEffort, .medium)
        XCTAssertEqual(debug.account, "ChatGPT Pro · Connected through Codex · fixture")
        XCTAssertEqual(debug.usageRemaining, "5h 72% · week 44%")
        XCTAssertEqual(debug.trustPreset, .explore)
        XCTAssertEqual(debug.pythonEnvironment, "Login-shell Python")
        XCTAssertEqual(debug.availableContextIDs, ["project", "paper", "source", "semantics"])
        XCTAssertEqual(debug.pinnedMessageCount, 0)
        XCTAssertEqual(debug.productionBoundaryCalls, 0)
    }
}
