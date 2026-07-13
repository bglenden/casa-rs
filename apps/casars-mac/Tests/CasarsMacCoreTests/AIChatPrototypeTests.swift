@testable import CasarsMacCore
import XCTest

final class AIChatPrototypeTests: XCTestCase {
    func testFactoryCreatesProviderNeutralIsolatedFixture() throws {
        let store = WorkbenchStore.aiPrototype()
        let projection = try XCTUnwrap(store.state.prototypeAI)

        XCTAssertTrue(store.isAIPrototypeRuntime)
        XCTAssertEqual(projection.providers.map(\.id), ["fixture-openai", "fixture-zen"])
        XCTAssertEqual(Set(projection.providers.map { $0.models.isEmpty }), [false])
        XCTAssertEqual(projection.proposals.map(\.kind), [.task, .python, .plot, .download])
        XCTAssertEqual(projection.presentation, .closed)
        XCTAssertEqual(store.state.activeTabID, "tab-scientific-notebook")
        XCTAssertEqual(projection.openTabSources.count, 5)
        XCTAssertTrue(projection.workspaceSources.contains { $0.id == "corpus-radio" })
        XCTAssertTrue(projection.workspaceSources.contains { $0.id == "source-casars" })
        XCTAssertEqual(store.prototypeProductionBoundaryInvocationCount, 0)
        XCTAssertEqual(projection.productionBoundaryCalls, 0)
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
        XCTAssertEqual(store.state.prototypeAI?.draft, "Explain the current Imager parameters")

        store.closeAIPrototypeConversation()
        XCTAssertEqual(store.state.prototypeAI?.presentation, .closed)
        XCTAssertEqual(store.state.prototypeAI?.draft, "Explain the current Imager parameters")
        XCTAssertTrue(store.state.prototypeAI?.messages.isEmpty == true)
        XCTAssertEqual(store.prototypeProductionBoundaryInvocationCount, 0)
    }

    func testProviderContextAndEgressSelectionRemainExplicit() throws {
        let store = WorkbenchStore.aiPrototype()

        store.selectAIPrototypeProvider("fixture-zen")
        store.selectAIPrototypeModel("GLM-4.5")
        store.toggleAIPrototypeContext("plot")
        store.toggleAIPrototypeContext("paper")

        let projection = try XCTUnwrap(store.state.prototypeAI)
        XCTAssertEqual(projection.selectedProviderID, "fixture-zen")
        XCTAssertEqual(projection.selectedModel, "GLM-4.5")
        XCTAssertEqual(projection.selectedContexts.map(\.id), ["project", "source", "plot"])
        XCTAssertTrue(projection.selectedContexts.allSatisfy { !$0.egressSummary.isEmpty })
        XCTAssertEqual(store.prototypeProductionBoundaryInvocationCount, 0)
    }

    func testSuggestionsReturnToNotebookAndTaskProposalOpensCanonicalTaskTab() throws {
        let store = WorkbenchStore.aiPrototype()
        store.expandAIPrototypeConversation()

        let focusGeneration = try XCTUnwrap(
            store.state.prototypeAI?.notebookSuggestionFocusGeneration
        )
        store.showAIPrototypeNotebookSuggestions()
        XCTAssertEqual(store.state.prototypeAI?.presentation, .drawer)
        XCTAssertEqual(store.state.activeTabID, "tab-scientific-notebook")
        XCTAssertEqual(
            store.state.prototypeAI?.notebookSuggestionFocusGeneration,
            focusGeneration + 1
        )

        store.openAIPrototypeTaskProposal("proposal-task")
        let taskTab = try XCTUnwrap(store.state.tabs.first { $0.id == "tab-ai-context-task" })
        XCTAssertEqual(taskTab.kind, .task)
        XCTAssertEqual(taskTab.taskID, "imager")
        XCTAssertEqual(taskTab.prototypeReceiptID, "receipt-imager-cancelled")
        XCTAssertEqual(store.state.activeTabID, "tab-ai-context-task")
        let proposalTask = try XCTUnwrap(
            store.state.prototypeNotebook?.receipts.first { $0.id == "receipt-imager-cancelled" }
        )
        XCTAssertEqual(proposalTask.parameterRows.first { $0.parameterID == "robust" }?.value, "-0.5")
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
        XCTAssertEqual(projection.messages.last?.providerLabel, "OpenAI subscription")
    }

    func testNonresponsiveCancellationRequiresExplicitWorkerRestart() throws {
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

    func testProposalRejectionFailureCancellationAndRetryDoNotBroadenAuthority() throws {
        var projection = PrototypeAIChatFixtureAdapter.make(scenario: .toolFailure)
        projection.rejectProposal("proposal-download")
        XCTAssertEqual(projection.proposals.first { $0.id == "proposal-download" }?.state, .rejected)

        let failedGeneration = try XCTUnwrap(projection.beginProposal("proposal-task"))
        projection.completeProposal("proposal-task", generation: failedGeneration)
        XCTAssertEqual(projection.proposals.first { $0.id == "proposal-task" }?.state, .failed)

        projection.retryProposal("proposal-task")
        let retryGeneration = try XCTUnwrap(projection.beginProposal("proposal-task"))
        projection.cancelProposal("proposal-task")
        projection.completeProposal("proposal-task", generation: retryGeneration)
        XCTAssertEqual(projection.proposals.first { $0.id == "proposal-task" }?.state, .cancelled)
        XCTAssertEqual(projection.productionBoundaryCalls, 0)
    }

    func testDebugSnapshotExposesPrototypeReviewState() throws {
        let store = WorkbenchStore.aiPrototype()
        store.setAIPrototypeDraft("Explain the open tabs")
        store.openAIPrototypeDrawer()
        store.rejectAIPrototypeProposal("proposal-plot")

        let debug = try XCTUnwrap(store.debugSnapshot().prototypeAI)
        XCTAssertEqual(debug.scenario, .primary)
        XCTAssertEqual(debug.presentation, .drawer)
        XCTAssertEqual(debug.primaryAttachment, "notebooks/Analysis.md")
        XCTAssertEqual(debug.draft, "Explain the open tabs")
        XCTAssertEqual(debug.openTabSourceIDs.count, 5)
        XCTAssertTrue(debug.workspaceSourceIDs.contains("schema-casars"))
        XCTAssertEqual(debug.provider, "OpenAI subscription")
        XCTAssertEqual(debug.pinnedMessageCount, 0)
        XCTAssertEqual(debug.proposalStates["proposal-plot"], .rejected)
        XCTAssertEqual(debug.productionBoundaryCalls, 0)
    }
}
