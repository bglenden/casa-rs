import Foundation
import XCTest
@testable import CasarsMacCore

final class PythonNotebookPrototypeTests: XCTestCase {
    func testFactoryCreatesIsolatedPythonPrototypeAndDebugProjection() throws {
        let store = WorkbenchStore.pythonPrototype(scenario: .primary)

        XCTAssertTrue(store.isPrototypeRuntime)
        XCTAssertTrue(store.isPythonPrototypeRuntime)
        XCTAssertFalse(store.isNotebookPrototypeRuntime)
        XCTAssertEqual(store.prototypeProductionBoundaryInvocationCount, 0)
        XCTAssertEqual(store.state.prototypePython?.cells.count, 5)
        XCTAssertEqual(store.state.prototypePython?.selectedCellID, "python-cell-plot")
        XCTAssertEqual(store.state.tabs.map(\.kind), [.python])

        let debug = try XCTUnwrap(store.debugSnapshot().prototypePython)
        XCTAssertEqual(debug.prototypeKind, .python)
        XCTAssertEqual(debug.scenario, .primary)
        XCTAssertEqual(debug.kernelState, .ready)
        XCTAssertEqual(debug.insertedPlotCount, 0)
    }

    func testPresetRevisionsRecordTheExactFixtureSourceDigest() throws {
        for (scenario, cellID) in [
            (PythonPrototypeScenario.primary, "python-cell-plot"),
            (.failure, "python-cell-repair"),
            (.nonresponsive, "python-cell-nonresponsive"),
        ] {
            let store = WorkbenchStore.pythonPrototype(scenario: scenario)
            let cell = try cell(store, id: cellID)
            XCTAssertEqual(cell.latestRevision?.sourceDigest, cell.sourceDigest)
        }
    }

    func testRunPreservesOrderedStreamsAndCreatesPlotRevision() throws {
        let store = WorkbenchStore.pythonPrototype(scenario: .primary)
        let initialCount = try XCTUnwrap(
            store.state.prototypePython?.cells.first { $0.id == "python-cell-plot" }
        ).revisions.count

        store.runPrototypePythonCell("python-cell-plot")
        XCTAssertEqual(store.state.prototypePython?.kernelState, .running)
        waitUntil { store.state.prototypePython?.kernelState == .ready }

        let cell = try XCTUnwrap(
            store.state.prototypePython?.cells.first { $0.id == "python-cell-plot" }
        )
        XCTAssertEqual(cell.revisions.count, initialCount + 1)
        XCTAssertEqual(cell.latestRevision?.status, .succeeded)
        XCTAssertEqual(cell.latestRevision?.outputs.map(\.order), [1, 2])
        XCTAssertEqual(cell.latestRevision?.outputs.map(\.channel), [.stdout, .stderr])
        XCTAssertNotNil(cell.latestRevision?.plot)
        XCTAssertEqual(store.prototypeProductionBoundaryInvocationCount, 0)
    }

    func testNonresponsiveInterruptRequiresRestart() throws {
        let store = WorkbenchStore.pythonPrototype(scenario: .nonresponsive)

        XCTAssertEqual(store.state.prototypePython?.kernelState, .running)
        store.interruptPrototypePythonKernel()
        XCTAssertEqual(store.state.prototypePython?.kernelState, .restartRequired)
        XCTAssertEqual(
            store.state.prototypePython?.cells.first { $0.id == "python-cell-nonresponsive" }?.latestRevision?.status,
            .interrupted
        )

        store.restartPrototypePythonKernel()
        XCTAssertEqual(store.state.prototypePython?.kernelState, .ready)
        XCTAssertNil(store.state.prototypePython?.runningCellID)
    }

    func testAIApprovalBindsToExactSourceAndEditInvalidatesIt() throws {
        let store = WorkbenchStore.pythonPrototype(scenario: .primary)
        let cellID = "python-cell-ai"

        XCTAssertFalse(try cell(store, id: cellID).approvalIsValid)
        store.runPrototypePythonCell(cellID)
        XCTAssertEqual(try cell(store, id: cellID).revisions.count, 0)

        store.approvePrototypePythonSource(cellID: cellID)
        XCTAssertTrue(try cell(store, id: cellID).approvalIsValid)
        store.setPrototypePythonSource(cellID: cellID, source: try cell(store, id: cellID).source + "\n# changed")
        XCTAssertFalse(try cell(store, id: cellID).approvalIsValid)
    }

    func testRegenerateAndInsertKeepImmutablePlotRevisions() throws {
        let store = WorkbenchStore.pythonPrototype(scenario: .primary)
        let cellID = "python-cell-plot"
        let original = try XCTUnwrap(cell(store, id: cellID).latestRevision?.plot)

        store.regeneratePrototypePythonPlot(cellID: cellID)
        let cellAfterRegeneration = try cell(store, id: cellID)
        XCTAssertEqual(cellAfterRegeneration.revisions.compactMap(\.plot).count, 2)
        XCTAssertEqual(cellAfterRegeneration.revisions.first?.plot, original)

        let newest = try XCTUnwrap(cellAfterRegeneration.latestRevision?.plot)
        store.insertPrototypePythonPlot(cellID: cellID, plotID: newest.id)
        XCTAssertFalse(try XCTUnwrap(cell(store, id: cellID).revisions.first?.plot).insertedInNotebook)
        XCTAssertTrue(try XCTUnwrap(cell(store, id: cellID).latestRevision?.plot).insertedInNotebook)
        XCTAssertEqual(store.state.prototypePython?.insertedPlotCount, 1)
    }

    private func cell(_ store: WorkbenchStore, id: String) throws -> PrototypePythonCell {
        try XCTUnwrap(store.state.prototypePython?.cells.first { $0.id == id })
    }

    private func waitUntil(
        timeout: TimeInterval = 4,
        condition: () -> Bool
    ) {
        let deadline = Date().addingTimeInterval(timeout)
        while !condition(), Date() < deadline {
            RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.02))
        }
        XCTAssertTrue(condition())
    }
}
