import XCTest
@testable import CasarsMacCore

final class TutorialNotebookPrototypeStoreTests: XCTestCase {
    func testFactoryUsesFreshIsolatedRuntimeAndDebugProjection() throws {
        let store = WorkbenchStore.tutorialPrototype(scenario: .happyPath)

        XCTAssertTrue(store.isTutorialPrototypeRuntime)
        XCTAssertTrue(store.isPrototypeRuntime)
        XCTAssertEqual(store.state.activeTabID, "tab-tutorial-prototype")
        XCTAssertEqual(store.state.dockMode, .notebooks)
        XCTAssertFalse(store.state.leftDockCollapsed)
        XCTAssertTrue(store.state.inspectorCollapsed)
        XCTAssertEqual(store.prototypeProductionBoundaryInvocationCount, 0)

        let debug = try XCTUnwrap(store.debugSnapshot().prototypeTutorial)
        XCTAssertEqual(debug.prototypeKind, .tutorial)
        XCTAssertEqual(debug.scenario, .happyPath)
        XCTAssertEqual(debug.datasetPhase, .missing)
        XCTAssertFalse(debug.datasetIsStaged)
        XCTAssertEqual(debug.currentGeneration, 0)
        XCTAssertFalse(debug.activeApproval)
    }

    func testOpeningAndSelectingTutorialNeverStartsAcquisition() throws {
        let store = WorkbenchStore.tutorialPrototype(scenario: .happyPath)

        store.openDefaultTab(kind: .notebook)
        store.selectTutorialPrototypeSection("tutorial-section-acquisition")

        let tutorial = try XCTUnwrap(store.state.prototypeTutorial)
        XCTAssertEqual(tutorial.dataset.phase, .missing)
        XCTAssertTrue(tutorial.dataset.attempts.isEmpty)
        XCTAssertNil(tutorial.activeApproval)
        XCTAssertEqual(store.prototypeProductionBoundaryInvocationCount, 0)
    }

    func testRawAndRichEditsShareOneEditableInMemoryDraft() throws {
        let store = WorkbenchStore.tutorialPrototype(scenario: .happyPath)
        let initialMarkdown = try XCTUnwrap(
            store.state.prototypeTutorial?.learnerNotebook.draftMarkdown
        )
        let rawMarkdown = initialMarkdown.replacingOccurrences(
            of: "My goal: understand the calibrated observation before imaging.",
            with: "Raw-mode note: compare calibrated amplitudes before imaging."
        )

        store.setTutorialPrototypeViewMode(.raw)
        store.setTutorialPrototypeDraft(rawMarkdown)
        XCTAssertTrue(try XCTUnwrap(store.state.prototypeTutorial).learnerNotebook.isDirty)

        store.setTutorialPrototypeViewMode(.rich)
        var richDocument = PrototypeNotebookRichDocument(
            markdown: try XCTUnwrap(
                store.state.prototypeTutorial?.learnerNotebook.draftMarkdown
            )
        )
        let noteElement = try XCTUnwrap(richDocument.elements.first {
            $0.editableSource?.contains("Raw-mode note") == true
        })
        XCTAssertTrue(richDocument.replaceEditableSource(
            elementID: noteElement.id,
            with: "Rich-mode note: compare calibrated amplitudes and phases before imaging."
        ))
        store.setTutorialPrototypeDraft(richDocument.markdown)

        let finalMarkdown = try XCTUnwrap(
            store.state.prototypeTutorial?.learnerNotebook.draftMarkdown
        )
        XCTAssertTrue(
            finalMarkdown.contains(
                "Rich-mode note: compare calibrated amplitudes and phases before imaging."
            )
        )
        XCTAssertFalse(finalMarkdown.contains("Raw-mode note"))
        XCTAssertTrue(finalMarkdown.contains("<!-- casa-rs-cell:v1 id=tutorial-task-twhya-imager kind=task -->"))

        store.saveTutorialPrototypeDraft()
        XCTAssertFalse(try XCTUnwrap(store.state.prototypeTutorial).learnerNotebook.isDirty)
        XCTAssertEqual(store.prototypeProductionBoundaryInvocationCount, 0)
    }

    func testReadyDatasetEnablesFixtureTaskTabWithoutProviderCalls() throws {
        let store = WorkbenchStore.tutorialPrototype(scenario: .happyPath)
        store.showTutorialPrototypeApproval()
        store.approveTutorialPrototypeAcquisition()
        let generation = try XCTUnwrap(store.state.prototypeTutorial?.currentGeneration)

        for _ in 0..<4 {
            XCTAssertTrue(store.advanceTutorialPrototypeAcquisition(generation: generation))
        }
        XCTAssertTrue(try XCTUnwrap(store.state.prototypeTutorial).dataset.isReady)

        store.openPrototypeTutorialTask(taskID: "tutorial-task-twhya-imager")
        let tab = try XCTUnwrap(store.state.tabs.first {
            $0.id == "tab-prototype-task-tutorial-task-twhya-imager"
        })
        XCTAssertEqual(tab.kind, .task)
        XCTAssertEqual(tab.taskID, "imager")
        XCTAssertEqual(tab.prototypeReceiptID, "tutorial-task-twhya-imager")
        XCTAssertEqual(store.prototypeProductionBoundaryInvocationCount, 0)
    }

    func testProductionStoreCannotOpenFixtureTutorialTask() {
        let store = WorkbenchStore.empty()

        store.openPrototypeTutorialTask(taskID: "tutorial-task-twhya-imager")

        XCTAssertFalse(store.state.tabs.contains { $0.prototypeReceiptID != nil })
        XCTAssertTrue(store.state.lastErrors.contains {
            $0.contains("unavailable until the fixture dataset is ready")
        })
    }
}
