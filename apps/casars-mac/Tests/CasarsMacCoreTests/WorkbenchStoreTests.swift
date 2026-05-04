import XCTest
@testable import CasarsMacCore

final class WorkbenchStoreTests: XCTestCase {
    func testFixtureStateExposesInitialDebugSnapshot() throws {
        let store = WorkbenchStore.fixture()

        let snapshot = store.debugSnapshot()

        XCTAssertEqual(snapshot.activeProject, "VLA spectral-line demo")
        XCTAssertEqual(snapshot.activeLeftDockMode, .datasets)
        XCTAssertFalse(snapshot.leftDockCollapsed)
        XCTAssertEqual(snapshot.selectedDataset, "IRC+10216.ms")
        XCTAssertEqual(snapshot.pythonOwner, .user)
        XCTAssertTrue(snapshot.openTabs.contains("AI Chat"))
        XCTAssertEqual(snapshot.aiProposalStates["proposal-spw"], .pending)
        XCTAssertEqual(
            DockMode.allCases.map(\.rawValue),
            ["datasets", "files", "history"]
        )
    }

    func testSelectionInspectorAndTabsAreActionDriven() {
        let store = WorkbenchStore.fixture()

        store.selectDockMode(.history)
        store.setLeftDockCollapsed(true)
        store.setInspectorCollapsed(true)
        store.selectDataset("image-cube")
        store.openDefaultTab(kind: .history)

        XCTAssertEqual(store.state.dockMode, .history)
        XCTAssertTrue(store.debugSnapshot().leftDockCollapsed)
        store.toggleLeftDock()
        XCTAssertFalse(store.debugSnapshot().leftDockCollapsed)
        XCTAssertTrue(store.state.inspectorCollapsed)
        store.setInspectorCollapsed(false)
        XCTAssertFalse(store.debugSnapshot().inspectorCollapsed)
        XCTAssertEqual(store.state.selectedDataset?.name, "IRC+10216.clean.image")
        XCTAssertEqual(store.state.tabs.first { $0.id == store.state.activeTabID }?.kind, .history)
    }

    func testCommandQueryRoutesWorkbenchShellSurfaces() {
        let store = WorkbenchStore.fixture()

        store.setCommandQuery("show inspector")
        store.setInspectorCollapsed(true)
        store.runCommandQuery()
        XCTAssertFalse(store.state.inspectorCollapsed)

        store.setCommandQuery("open python")
        store.runCommandQuery()
        XCTAssertEqual(store.state.tabs.first { $0.id == store.state.activeTabID }?.kind, .python)

        store.setLeftDockCollapsed(true)
        store.setCommandQuery("show sidebar")
        store.runCommandQuery()
        XCTAssertFalse(store.state.leftDockCollapsed)

        store.setCommandQuery("show timeline")
        store.setLeftDockCollapsed(true)
        store.runCommandQuery()
        XCTAssertEqual(store.state.dockMode, .history)
        XCTAssertFalse(store.state.leftDockCollapsed)
        XCTAssertEqual(store.state.tabs.first { $0.id == store.state.activeTabID }?.kind, .history)
        XCTAssertEqual(store.debugSnapshot().commandQuery, "show timeline")
    }

    func testAIProposalMustBeAppliedBeforeItMutatesTaskParameters() {
        let store = WorkbenchStore.fixture()

        XCTAssertEqual(store.state.taskParameters.selectedSpectralWindow, "spw 1: 1.42 GHz")
        store.setTaskSpectralWindow("all")
        store.appendAIChatMessage("Please narrow this to the line SPW.")

        XCTAssertEqual(store.state.taskParameters.selectedSpectralWindow, "all")
        store.applyAIProposal("proposal-spw")

        XCTAssertEqual(store.state.taskParameters.selectedSpectralWindow, "spw 1: 1.42 GHz")
        XCTAssertEqual(store.state.aiProposals.first?.state, .applied)
        XCTAssertTrue(store.state.history.contains { $0.title == "AI proposal applied" })
    }

    func testTaskRunAndPythonOwnershipAreInspectable() {
        let store = WorkbenchStore.fixture()

        store.setPythonOwner(.ai)
        store.runTask()

        let snapshot = store.debugSnapshot()
        XCTAssertEqual(snapshot.pythonOwner, .ai)
        XCTAssertEqual(snapshot.taskState, .completed)
        XCTAssertTrue(snapshot.processingHistoryEvents.contains("Fixture task completed"))
        XCTAssertNoThrow(try store.debugJSON())
    }

    func testUnknownActionsRecordDebugErrors() {
        let store = WorkbenchStore.fixture()

        store.selectDataset("missing")
        store.activateTab("missing")

        XCTAssertEqual(store.debugSnapshot().lastErrors.count, 2)
    }

    func testOpenProjectIngestsProbeResultsIntoDatasetDockAndInspectorState() {
        let probedDataset = DatasetSummary(
            id: "/data/probed.ms",
            name: "probed.ms",
            path: "/data/probed.ms",
            kind: .measurementSet,
            size: "12 rows, 1 fields, 1 spw, 2 antennas",
            units: "Jy, Hz, seconds",
            fields: ["0: Target"],
            spectralWindows: ["spw 0: 4 chan, 1.420000 GHz center"],
            scans: ["scan 1: 12 rows, Target"],
            notes: "Recognized by Rust probe."
        )
        let client = StubProjectProbeClient(
            result: ProjectFixtureProbe(
                project: ProjectFixture(
                    name: "Real Project",
                    rootPath: "/data",
                    datasets: [probedDataset],
                    source: .probed
                ),
                diagnostics: ["skipped /data/notes.txt"]
            )
        )
        let store = WorkbenchStore(state: FixtureWorkbench.makeState(), probeClient: client)

        store.openProject(path: "/data")

        let snapshot = store.debugSnapshot()
        XCTAssertEqual(snapshot.activeProject, "Real Project")
        XCTAssertEqual(snapshot.activeProjectRoot, "/data")
        XCTAssertEqual(snapshot.activeProjectSource, .probed)
        XCTAssertEqual(snapshot.selectedDataset, "probed.ms")
        XCTAssertEqual(snapshot.discoveredDatasets, ["probed.ms"])
        XCTAssertEqual(snapshot.probeDiagnostics, ["skipped /data/notes.txt"])
        XCTAssertEqual(store.state.selectedDataset?.spectralWindows, ["spw 0: 4 chan, 1.420000 GHz center"])
    }
}

private struct StubProjectProbeClient: ProjectProbeClient {
    var result: ProjectFixtureProbe

    func probeProject(path: String) throws -> ProjectFixtureProbe {
        result
    }
}
