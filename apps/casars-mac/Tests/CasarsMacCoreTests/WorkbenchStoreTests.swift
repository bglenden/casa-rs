import Foundation
import XCTest
@testable import CasarsMacCore

final class WorkbenchStoreTests: XCTestCase {
    func testDefaultStateStartsWithoutFixtureProject() throws {
        let store = WorkbenchStore()

        let snapshot = store.debugSnapshot()

        XCTAssertEqual(snapshot.activeProject, "No Project")
        XCTAssertEqual(snapshot.activeProjectSource, .none)
        XCTAssertNil(snapshot.selectedDataset)
        XCTAssertNil(snapshot.selectedDatasetSummary)
        XCTAssertTrue(snapshot.discoveredDatasets.isEmpty)
        XCTAssertTrue(snapshot.openTabs.isEmpty)
        XCTAssertEqual(snapshot.activeTab, "")
    }

    func testFixtureStateExposesInitialDebugSnapshot() throws {
        let store = WorkbenchStore.fixture()

        let snapshot = store.debugSnapshot()

        XCTAssertEqual(snapshot.activeProject, "VLA spectral-line demo")
        XCTAssertEqual(snapshot.activeLeftDockMode, .datasets)
        XCTAssertFalse(snapshot.leftDockCollapsed)
        XCTAssertEqual(snapshot.selectedDataset, "IRC+10216.ms")
        XCTAssertEqual(snapshot.pythonOwner, .user)
        XCTAssertEqual(snapshot.interfaceFontSize, WorkbenchState.defaultInterfaceFontSize)
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
        let tabCount = store.state.tabs.count
        store.selectDataset("image-cube")
        XCTAssertEqual(store.state.tabs.count, tabCount)
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

    func testDatasetExplorerOpenUsesSelectedDatasetTypeAndDatasetID() {
        let store = WorkbenchStore.fixture()

        store.selectDataset("image-cube")
        store.openDefaultTab(kind: .datasetExplorer)

        var activeTab = store.state.tabs.first { $0.id == store.state.activeTabID }
        XCTAssertEqual(activeTab?.kind, .datasetExplorer)
        XCTAssertEqual(activeTab?.datasetID, "image-cube")
        XCTAssertEqual(activeTab?.title, "Image: IRC+10216.clean.image")

        store.openDatasetExplorer("phase-cal")

        activeTab = store.state.tabs.first { $0.id == store.state.activeTabID }
        XCTAssertEqual(store.state.selectedDatasetID, "phase-cal")
        XCTAssertEqual(activeTab?.kind, .datasetExplorer)
        XCTAssertEqual(activeTab?.datasetID, "phase-cal")
        XCTAssertEqual(activeTab?.title, "Cal: phase.cal")
    }

    func testTabDismissalKeepsOrMovesActiveTab() {
        let store = WorkbenchStore.fixture()
        let initialActiveTabID = store.state.activeTabID

        store.closeTab("tab-ai")

        XCTAssertFalse(store.state.tabs.contains { $0.id == "tab-ai" })
        XCTAssertEqual(store.state.activeTabID, initialActiveTabID)

        store.closeTab(initialActiveTabID)

        XCTAssertEqual(store.state.activeTabID, "tab-task")

        store.closeActiveTab()

        XCTAssertEqual(store.state.activeTabID, "tab-python")

        store.closeActiveTab()

        XCTAssertTrue(store.state.tabs.isEmpty)
        XCTAssertEqual(store.state.activeTabID, "")
    }

    func testClosingUnknownTabRecordsDebugError() {
        let store = WorkbenchStore.fixture()

        store.closeTab("missing")

        XCTAssertEqual(store.debugSnapshot().lastErrors, ["Unknown tab missing"])
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
            antennas: ["ea01", "ea02"],
            correlations: ["XX", "YY"],
            columns: ["UVW", "DATA", "FLAG"],
            dataColumns: ["DATA"],
            subtables: ["ANTENNA (required)", "FIELD (required)"],
            shape: [12],
            notes: "Recognized by Rust probe.",
            diagnostics: ["probe note"]
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
        XCTAssertEqual(snapshot.selectedDatasetSummary?.dataColumns, ["DATA"])
        XCTAssertEqual(snapshot.selectedDatasetSummary?.subtables, ["ANTENNA (required)", "FIELD (required)"])
        XCTAssertEqual(snapshot.selectedDatasetSummary?.shape, [12])
        XCTAssertEqual(snapshot.discoveredDatasets, ["probed.ms"])
        XCTAssertEqual(snapshot.probeDiagnostics, ["skipped /data/notes.txt"])
        XCTAssertEqual(store.state.selectedDataset?.spectralWindows, ["spw 0: 4 chan, 1.420000 GHz center"])
        XCTAssertEqual(store.state.tabs.first { $0.id == store.state.activeTabID }?.title, "MS: probed.ms")
    }

    func testFakeExecutionTabsAreGatedOutsideDemoProjectButRealImagingTaskOpens() {
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
                diagnostics: []
            )
        )
        let store = WorkbenchStore(probeClient: client)

        store.openDefaultTab(kind: .aiChat)
        store.openDefaultTab(kind: .python)
        store.openDefaultTab(kind: .task)
        XCTAssertTrue(store.state.tabs.isEmpty)
        XCTAssertEqual(store.state.lastErrors.count, 3)

        store.openProject(path: "/data")
        store.openDefaultTab(kind: .aiChat)
        store.openDefaultTab(kind: .python)
        store.openDefaultTab(kind: .task)

        XCTAssertEqual(store.state.tabs.count, 2)
        XCTAssertEqual(store.state.tabs.first?.kind, .datasetExplorer)
        XCTAssertEqual(store.state.tabs.last?.title, "Dirty Image: probed.ms")
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.measurementSetPath, "/data/probed.ms")
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.selectedField, "0: Target")
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.phaseCenterField, "0: Target")
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.selectedSpectralWindow, "spw 0: 4 chan, 1.420000 GHz center")
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.outputPrefix, "/data/casa-rs-runs/dirty-imaging-1/probed.ms-dirty")
        XCTAssertTrue(store.state.lastErrors.contains("AI chat is not connected yet"))
        XCTAssertTrue(store.state.lastErrors.contains("Python is not connected yet"))
        XCTAssertFalse(store.state.lastErrors.contains("Task panels are not connected for real projects yet"))

        store.openFixtureProject()
        store.openDefaultTab(kind: .aiChat)
        store.openDefaultTab(kind: .python)
        store.openDefaultTab(kind: .task)

        XCTAssertTrue(store.state.tabs.contains { $0.kind == .aiChat })
        XCTAssertTrue(store.state.tabs.contains { $0.kind == .python })
        XCTAssertTrue(store.state.tabs.contains { $0.kind == .task })
    }

    func testDirtyImagingTaskCanOpenWhenSelectedDatasetIsAnImage() {
        let msDataset = DatasetSummary(
            id: "/data/probed.ms",
            name: "probed.ms",
            path: "/data/probed.ms",
            kind: .measurementSet,
            size: "12 rows, 1 fields, 1 spw, 2 antennas",
            units: "Jy, Hz, seconds",
            fields: ["0: Target"],
            spectralWindows: ["spw 0: 4 chan, 1.420000 GHz center"],
            dataColumns: ["DATA"],
            notes: "Recognized by Rust probe."
        )
        let imageDataset = DatasetSummary(
            id: "/data/output.image",
            name: "output.image",
            path: "/data/output.image",
            kind: .imageCube,
            size: "256 x 256",
            units: "float32",
            shape: [256, 256],
            notes: "Produced image."
        )
        let store = WorkbenchStore(
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(
                        name: "Real Project",
                        rootPath: "/data",
                        datasets: [msDataset, imageDataset],
                        source: .probed
                    ),
                    diagnostics: []
                )
            )
        )

        store.openProject(path: "/data")
        store.selectDataset(imageDataset.id)
        store.openDefaultTab(kind: .task)

        XCTAssertEqual(store.state.selectedDatasetID, imageDataset.id)
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.datasetID, "")
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.measurementSetPath, "")
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.selectedField, nil)
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.selectedSpectralWindow, nil)
        XCTAssertEqual(store.state.tabs.first(where: { $0.kind == .task })?.title, "Dirty Image")
        XCTAssertFalse(store.state.lastErrors.contains("Dataset output.image is not a MeasurementSet"))

        store.setDirtyImagingDataset(msDataset.id)

        XCTAssertEqual(store.state.selectedDatasetID, msDataset.id)
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.datasetID, msDataset.id)
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.measurementSetPath, msDataset.path)
        XCTAssertEqual(store.state.tabs.first(where: { $0.kind == .task })?.title, "Dirty Image: probed.ms")
    }

    func testRealDirtyImagingRunUsesTaskClientAndRecordsDebugHistory() throws {
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
            correlations: ["XX", "YY"],
            columns: ["UVW", "DATA", "FLAG"],
            dataColumns: ["DATA"],
            notes: "Recognized by Rust probe."
        )
        let probeClient = StubProjectProbeClient(
            result: ProjectFixtureProbe(
                project: ProjectFixture(
                    name: "Real Project",
                    rootPath: "/data",
                    datasets: [probedDataset],
                    source: .probed
                ),
                diagnostics: []
            ),
            probedPaths: [
                "/data/casa-rs-runs/output.image": DatasetSummary(
                    id: "/data/casa-rs-runs/output.image",
                    name: "output.image",
                    path: "/data/casa-rs-runs/output.image",
                    kind: .imageCube,
                    size: "256 x 256",
                    units: "float32",
                    sizeBytes: 4096,
                    shape: [256, 256],
                    notes: "Recognized by opening the path as a casa-rs image."
                )
            ]
        )
        let taskClient = StubDirtyImagingTaskClient()
        let store = WorkbenchStore(probeClient: probeClient, dirtyImagingClient: taskClient)

        store.openProject(path: "/data")
        store.openDefaultTab(kind: .task)
        store.setDirtyImagingImageSize(256)
        store.setDirtyImagingImageHeight(256)
        store.setDirtyImagingCellArcsec(0.25)
        store.setDirtyImagingWeighting(.briggs)
        store.setDirtyImagingChannelStart("2")
        store.setDirtyImagingChannelCount("4")
        store.runTask()

        XCTAssertEqual(taskClient.requests.count, 1)
        let encoded = String(decoding: try taskClient.requests[0].encodedImagerJSON(), as: UTF8.self)
        XCTAssertTrue(encoded.contains(#""dirty_only" : true"#))
        XCTAssertTrue(encoded.contains(#""field_ids" : ["#))
        XCTAssertTrue(encoded.contains(#""phasecenter_field" : 0"#))
        XCTAssertTrue(encoded.contains(#""spw_selector" : "0""#))
        XCTAssertTrue(encoded.contains(#""channel_start" : 2"#))
        XCTAssertTrue(encoded.contains(#""channel_count" : 4"#))
        XCTAssertTrue(encoded.contains(#""kind" : "briggs""#))

        let snapshot = store.debugSnapshot()
        XCTAssertEqual(snapshot.taskState, .succeeded)
        XCTAssertEqual(snapshot.taskRequest?.imageSize, 256)
        XCTAssertEqual(snapshot.taskRequest?.imageHeight, 256)
        XCTAssertEqual(snapshot.taskRequest?.cellArcsec, 0.25)
        XCTAssertTrue(snapshot.taskOutputPaths.contains("/data/casa-rs-runs/output.image"))
        XCTAssertTrue(snapshot.processingHistoryEvents.contains("Dirty imaging completed"))
        let producedDataset = store.state.project.datasets.first { $0.path == "/data/casa-rs-runs/output.image" }
        XCTAssertEqual(producedDataset?.kind, .imageCube)
        XCTAssertEqual(producedDataset?.size, "256 x 256")
        XCTAssertEqual(producedDataset?.units, "float32")
        XCTAssertEqual(producedDataset?.shape, [256, 256])
        XCTAssertNoThrow(try store.debugJSON())
    }

    func testDirtyImagingTaskInputMeasurementSetCanBeChangedInsideTaskTab() {
        let first = DatasetSummary(
            id: "/data/first.ms",
            name: "first.ms",
            path: "/data/first.ms",
            kind: .measurementSet,
            size: "12 rows, 1 fields, 1 spw, 2 antennas",
            units: "Jy, Hz, seconds",
            fields: ["0: First"],
            spectralWindows: ["spw 0: 4 chan, 1.420000 GHz center"],
            dataColumns: ["DATA"],
            notes: "First MS."
        )
        let second = DatasetSummary(
            id: "/data/second.ms",
            name: "second.ms",
            path: "/data/second.ms",
            kind: .measurementSet,
            size: "24 rows, 1 fields, 2 spw, 3 antennas",
            units: "Jy, Hz, seconds",
            fields: ["1: Second"],
            spectralWindows: ["spw 1: 8 chan, 1.500000 GHz center"],
            dataColumns: ["CORRECTED_DATA"],
            notes: "Second MS."
        )
        let store = WorkbenchStore(
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(
                        name: "Real Project",
                        rootPath: "/data",
                        datasets: [first, second],
                        source: .probed
                    ),
                    diagnostics: []
                )
            )
        )

        store.openProject(path: "/data")
        store.openDefaultTab(kind: .task)
        store.setDirtyImagingImageSize(1024)
        store.setDirtyImagingImageHeight(768)
        store.setDirtyImagingCellArcsec(0.5)
        store.setDirtyImagingDataset(second.id)

        XCTAssertEqual(store.state.selectedDatasetID, second.id)
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.datasetID, second.id)
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.measurementSetPath, second.path)
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.selectedField, "1: Second")
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.selectedSpectralWindow, "spw 1: 8 chan, 1.500000 GHz center")
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.dataColumn, "CORRECTED_DATA")
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.imageSize, 1024)
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.imageHeight, 768)
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.cellArcsec, 0.5)
        XCTAssertEqual(store.state.tabs.first(where: { $0.kind == .task })?.title, "Dirty Image: second.ms")
    }

    func testBundledSampleDirtyImagingDefaultsChooseLineTarget() throws {
        let sample = DatasetSummary(
            id: "/data/mssel_test_small_multifield_spw.ms",
            name: "mssel_test_small_multifield_spw.ms",
            path: "/data/mssel_test_small_multifield_spw.ms",
            kind: .measurementSet,
            size: "14985 rows, 9 fields, 6 spw, 10 antennas",
            units: "Jy, Hz, seconds",
            fields: [
                "0: 3C273-F0",
                "1: 2",
                "2: NGC4826-F0",
                "3: NGC4826-F1",
                "4: 2000",
                "5: NGC4826-F3",
                "6: NGC4826-F4",
                "7: NGC4826-F5",
                "8: NGC4826-F6"
            ],
            spectralWindows: [
                "spw 0: 1 chan, 115.138579 GHz center",
                "spw 1: 1 chan, 115.217017 GHz center",
                "spw 2: 64 chan, 114.999607 GHz center",
                "spw 3: 64 chan, 115.089621 GHz center",
                "spw 4: 64 chan, 115.179362 GHz center",
                "spw 5: 64 chan, 115.269376 GHz center"
            ],
            correlations: ["YY"],
            dataColumns: ["DATA"],
            notes: "Bundled real sample MS."
        )
        let store = WorkbenchStore(
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(
                        name: "Sample Project",
                        rootPath: "/data",
                        datasets: [sample],
                        source: .probed
                    ),
                    diagnostics: []
                )
            )
        )

        store.openProject(path: "/data")
        store.openDefaultTab(kind: .task)

        let parameters = try XCTUnwrap(store.state.dirtyImagingTaskParameters)
        XCTAssertEqual(parameters.selectedField, "5: NGC4826-F3")
        XCTAssertEqual(parameters.phaseCenterField, "5: NGC4826-F3")
        XCTAssertEqual(parameters.selectedSpectralWindow, "spw 5: 64 chan, 115.269376 GHz center")
        XCTAssertEqual(parameters.correlation, "YY")

        let request = DirtyImagingTaskRequest(runID: "run-sample", parameters: parameters)
        let requestJSON = String(data: try request.encodedImagerJSON(), encoding: .utf8)
        XCTAssertTrue(try XCTUnwrap(requestJSON).contains("\"field_ids\" : [\n      5\n    ]"))
        XCTAssertTrue(try XCTUnwrap(requestJSON).contains("\"spw_selector\" : \"5\""))
        XCTAssertTrue(try XCTUnwrap(requestJSON).contains("\"correlation\" : \"YY\""))
    }

    func testDirtyImagingValidationFailuresAreDebugVisible() {
        let probedDataset = DatasetSummary(
            id: "/data/probed.ms",
            name: "probed.ms",
            path: "/data/probed.ms",
            kind: .measurementSet,
            size: "12 rows, 1 fields, 1 spw, 2 antennas",
            units: "Jy, Hz, seconds",
            fields: ["0: Target"],
            spectralWindows: ["spw 0: 4 chan, 1.420000 GHz center"],
            notes: "Recognized by Rust probe."
        )
        let probeClient = StubProjectProbeClient(
            result: ProjectFixtureProbe(
                project: ProjectFixture(
                    name: "Real Project",
                    rootPath: "/data",
                    datasets: [probedDataset],
                    source: .probed
                ),
                diagnostics: []
            )
        )
        let taskClient = StubDirtyImagingTaskClient()
        let store = WorkbenchStore(probeClient: probeClient, dirtyImagingClient: taskClient)

        store.openProject(path: "/data")
        store.openDefaultTab(kind: .task)
        store.setDirtyImagingImageSize(0)
        store.setDirtyImagingImageHeight(128)
        store.setDirtyImagingCellArcsec(-1)
        store.runTask()

        XCTAssertEqual(taskClient.requests.count, 0)
        XCTAssertEqual(store.debugSnapshot().taskState, .failed)
        XCTAssertTrue(store.debugSnapshot().taskDiagnostics.contains("Image width must be positive."))
        XCTAssertTrue(store.debugSnapshot().taskDiagnostics.contains("Cell size must be a positive finite arcsecond value."))
    }

    func testDirtyImagingRectangularImageSizeIsVisibleButNotRunnableYet() {
        let probedDataset = DatasetSummary(
            id: "/data/probed.ms",
            name: "probed.ms",
            path: "/data/probed.ms",
            kind: .measurementSet,
            size: "12 rows, 1 fields, 1 spw, 2 antennas",
            units: "Jy, Hz, seconds",
            fields: ["0: Target"],
            spectralWindows: ["spw 0: 4 chan, 1.420000 GHz center"],
            notes: "Recognized by Rust probe."
        )
        let store = WorkbenchStore(
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(
                        name: "Real Project",
                        rootPath: "/data",
                        datasets: [probedDataset],
                        source: .probed
                    ),
                    diagnostics: []
                )
            ),
            dirtyImagingClient: StubDirtyImagingTaskClient()
        )

        store.openProject(path: "/data")
        store.openDefaultTab(kind: .task)
        store.setDirtyImagingImageSize(512)
        store.setDirtyImagingImageHeight(256)
        store.runTask()

        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.imageSize, 512)
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.imageHeight, 256)
        XCTAssertEqual(store.debugSnapshot().taskState, .failed)
        XCTAssertTrue(store.debugSnapshot().taskDiagnostics.contains("Rectangular image sizes are not supported by the current casars-imager backend yet."))
    }

    func testDirtyImagingImageSizeAssessmentAndAdjustmentPreferFftFriendlyValues() {
        XCTAssertEqual(DirtyImagingTaskParameters.imageDimensionAssessment(512).severity, .good)
        XCTAssertEqual(DirtyImagingTaskParameters.imageDimensionAssessment(1000).severity, .good)
        XCTAssertEqual(DirtyImagingTaskParameters.imageDimensionAssessment(511).severity, .warning)
        XCTAssertEqual(DirtyImagingTaskParameters.imageDimensionAssessment(257).severity, .terrible)
        XCTAssertEqual(DirtyImagingTaskParameters.nearestNiceImageDimension(to: 257), 270)
        XCTAssertEqual(DirtyImagingTaskParameters.nearestNiceImageDimension(to: 511), 512)
        XCTAssertEqual(DirtyImagingTaskParameters.nearestNiceImageDimension(to: 513), 540)
        XCTAssertEqual(DirtyImagingTaskParameters.nearestNiceImageDimension(to: 8191), 8192)

        let probedDataset = DatasetSummary(
            id: "/data/probed.ms",
            name: "probed.ms",
            path: "/data/probed.ms",
            kind: .measurementSet,
            size: "12 rows, 1 fields, 1 spw, 2 antennas",
            units: "Jy, Hz, seconds",
            fields: ["0: Target"],
            spectralWindows: ["spw 0: 4 chan, 1.420000 GHz center"],
            notes: "Recognized by Rust probe."
        )
        let store = WorkbenchStore(
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(
                        name: "Real Project",
                        rootPath: "/data",
                        datasets: [probedDataset],
                        source: .probed
                    ),
                    diagnostics: []
                )
            )
        )

        store.openProject(path: "/data")
        store.openDefaultTab(kind: .task)
        store.setDirtyImagingImageSize(257)
        store.setDirtyImagingImageHeight(511)
        store.adjustDirtyImagingImageWidthToNiceSize()
        store.adjustDirtyImagingImageHeightToNiceSize()

        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.imageSize, 270)
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.imageHeight, 512)
    }

    func testRealMeasurementSetPlotRunUsesPlotClientAndDebugState() {
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
            antennas: ["ea01", "ea02"],
            correlations: ["XX", "YY"],
            columns: ["UVW", "DATA", "FLAG"],
            dataColumns: ["DATA"],
            subtables: ["ANTENNA (required)", "FIELD (required)"],
            shape: [12],
            notes: "Recognized by Rust probe."
        )
        let probeClient = StubProjectProbeClient(
            result: ProjectFixtureProbe(
                project: ProjectFixture(
                    name: "Real Project",
                    rootPath: "/data",
                    datasets: [probedDataset],
                    source: .probed
                ),
                diagnostics: []
            )
        )
        let plotClient = StubMeasurementSetPlotClient()
        let store = WorkbenchStore(probeClient: probeClient, plotClient: plotClient)

        store.openProject(path: "/data")
        store.runMeasurementSetPlot(datasetID: probedDataset.id)
        waitFor("plot job to finish") {
            store.debugSnapshot().measurementSetPlots[probedDataset.id]?.status == .ready
        }

        var snapshot = store.debugSnapshot()
        XCTAssertEqual(snapshot.measurementSetPlots[probedDataset.id]?.status, .ready)
        XCTAssertEqual(snapshot.measurementSetPlots[probedDataset.id]?.title, "UV Coverage")
        XCTAssertEqual(snapshot.measurementSetPlots[probedDataset.id]?.imageByteCount, 8)
        XCTAssertEqual(snapshot.measurementSetPlots[probedDataset.id]?.renderedPointCount, 42)
        XCTAssertEqual(plotClient.requests.last?.preset, .uvCoverage)
        XCTAssertNil(plotClient.requests.last?.field)
        XCTAssertNil(plotClient.requests.last?.spectralWindow)
        XCTAssertEqual(plotClient.requests.count, 1)

        store.setMeasurementSetPlotPreset(.amplitudeVsUvDistance, datasetID: probedDataset.id)
        snapshot = store.debugSnapshot()
        XCTAssertEqual(snapshot.measurementSetPlots[probedDataset.id]?.preset, .amplitudeVsUvDistance)
        XCTAssertEqual(snapshot.measurementSetPlots[probedDataset.id]?.status, .idle)
        XCTAssertNil(snapshot.measurementSetPlots[probedDataset.id]?.resultPreset)
        XCTAssertNil(snapshot.measurementSetPlots[probedDataset.id]?.title)

        store.setMeasurementSetPlotField("0: Target", datasetID: probedDataset.id)
        store.setMeasurementSetPlotSpectralWindow("spw 0: 4 chan, 1.420000 GHz center", datasetID: probedDataset.id)
        store.runMeasurementSetPlot(datasetID: probedDataset.id)
        waitFor("filtered plot job to finish") {
            store.debugSnapshot().measurementSetPlots[probedDataset.id]?.resultPreset == .amplitudeVsUvDistance
        }

        snapshot = store.debugSnapshot()
        XCTAssertEqual(snapshot.measurementSetPlots[probedDataset.id]?.preset, .amplitudeVsUvDistance)
        XCTAssertEqual(snapshot.measurementSetPlots[probedDataset.id]?.resultPreset, .amplitudeVsUvDistance)
        XCTAssertEqual(plotClient.requests.last?.field, "0")
        XCTAssertEqual(plotClient.requests.last?.spectralWindow, "0")
        XCTAssertEqual(plotClient.requests.last?.dataColumn, "DATA")
        XCTAssertEqual(plotClient.requests.count, 2)

        store.setMeasurementSetPlotField("all", datasetID: probedDataset.id)
        store.setMeasurementSetPlotSpectralWindow("all", datasetID: probedDataset.id)
        store.setMeasurementSetPlotPreset(.uvCoverage, datasetID: probedDataset.id)

        snapshot = store.debugSnapshot()
        XCTAssertEqual(snapshot.measurementSetPlots[probedDataset.id]?.status, .ready)
        XCTAssertEqual(snapshot.measurementSetPlots[probedDataset.id]?.resultPreset, .uvCoverage)
        XCTAssertEqual(snapshot.measurementSetPlots[probedDataset.id]?.title, "UV Coverage")
        XCTAssertEqual(plotClient.requests.count, 2)

        store.runMeasurementSetPlot(datasetID: probedDataset.id)

        XCTAssertEqual(plotClient.requests.count, 2)
    }

    func testMeasurementSetPlotJobDoesNotBlockUnrelatedWorkbenchActions() {
        let msDataset = DatasetSummary(
            id: "/data/probed.ms",
            name: "probed.ms",
            path: "/data/probed.ms",
            kind: .measurementSet,
            size: "12 rows, 1 fields, 1 spw, 2 antennas",
            units: "Jy, Hz, seconds",
            fields: ["0: Target"],
            spectralWindows: ["spw 0: 4 chan, 1.420000 GHz center"],
            dataColumns: ["DATA"],
            notes: "Recognized by Rust probe."
        )
        let imageDataset = DatasetSummary(
            id: "/data/output.image",
            name: "output.image",
            path: "/data/output.image",
            kind: .imageCube,
            size: "256 x 256",
            units: "float32",
            shape: [256, 256],
            notes: "Produced image."
        )
        let plotClient = BlockingMeasurementSetPlotClient()
        let store = WorkbenchStore(
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(
                        name: "Real Project",
                        rootPath: "/data",
                        datasets: [msDataset, imageDataset],
                        source: .probed
                    ),
                    diagnostics: []
                )
            ),
            plotClient: plotClient
        )

        store.openProject(path: "/data")
        store.runMeasurementSetPlot(datasetID: msDataset.id)
        XCTAssertEqual(plotClient.waitForStartedCount(1), .success)
        XCTAssertEqual(store.debugSnapshot().measurementSetPlots[msDataset.id]?.status, .running)
        XCTAssertEqual(store.debugSnapshot().runningJobCount, 1)

        store.selectDataset(imageDataset.id)
        store.openDatasetExplorer(imageDataset.id)
        store.setInspectorCollapsed(true)
        store.setCommandQuery("show inspector")
        store.runCommandQuery()
        store.activateTab(msDataset.explorerTabID)

        XCTAssertEqual(store.state.selectedDatasetID, imageDataset.id)
        XCTAssertEqual(store.state.tabs.first { $0.id == store.state.activeTabID }?.id, msDataset.explorerTabID)
        XCTAssertFalse(store.debugSnapshot().inspectorCollapsed)
        XCTAssertEqual(store.debugSnapshot().measurementSetPlots[msDataset.id]?.status, .running)

        plotClient.releaseAll()
        waitFor("blocked plot job to finish") {
            store.debugSnapshot().measurementSetPlots[msDataset.id]?.status == .ready
        }
        XCTAssertEqual(store.debugSnapshot().jobs.first?.status, .succeeded)
    }

    func testTwoMeasurementSetTabsHoldIndependentPlotJobs() {
        let first = DatasetSummary(
            id: "/data/first.ms",
            name: "first.ms",
            path: "/data/first.ms",
            kind: .measurementSet,
            size: "12 rows, 1 fields, 1 spw, 2 antennas",
            units: "Jy, Hz, seconds",
            fields: ["0: First"],
            spectralWindows: ["spw 0: 4 chan, 1.420000 GHz center"],
            dataColumns: ["DATA"],
            notes: "First MS."
        )
        let second = DatasetSummary(
            id: "/data/second.ms",
            name: "second.ms",
            path: "/data/second.ms",
            kind: .measurementSet,
            size: "24 rows, 1 fields, 2 spw, 3 antennas",
            units: "Jy, Hz, seconds",
            fields: ["1: Second"],
            spectralWindows: ["spw 1: 8 chan, 1.500000 GHz center"],
            dataColumns: ["DATA"],
            notes: "Second MS."
        )
        let plotClient = BlockingMeasurementSetPlotClient()
        let store = WorkbenchStore(
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(
                        name: "Real Project",
                        rootPath: "/data",
                        datasets: [first, second],
                        source: .probed
                    ),
                    diagnostics: []
                )
            ),
            plotClient: plotClient
        )

        store.openProject(path: "/data")
        store.runMeasurementSetPlot(datasetID: first.id)
        XCTAssertEqual(plotClient.waitForStartedCount(1), .success)
        store.openDatasetExplorer(second.id)
        store.runMeasurementSetPlot(datasetID: second.id)
        XCTAssertEqual(plotClient.waitForStartedCount(2), .success)

        let snapshot = store.debugSnapshot()
        XCTAssertEqual(snapshot.runningJobCount, 2)
        XCTAssertEqual(snapshot.activeJobIDsByTab.keys.sorted(), [first.explorerTabID, second.explorerTabID])
        XCTAssertEqual(snapshot.jobs.map(\.status), [.running, .running])

        plotClient.releaseAll()
        waitFor("both plot jobs to finish") {
            let latest = store.debugSnapshot()
            return latest.measurementSetPlots[first.id]?.status == .ready
                && latest.measurementSetPlots[second.id]?.status == .ready
        }
        XCTAssertEqual(store.debugSnapshot().runningJobCount, 0)
        XCTAssertEqual(store.debugSnapshot().jobs.map(\.status), [.succeeded, .succeeded])
    }

    func testCancellingDirtyImagingJobIsScopedToThatJob() {
        let probedDataset = DatasetSummary(
            id: "/data/probed.ms",
            name: "probed.ms",
            path: "/data/probed.ms",
            kind: .measurementSet,
            size: "12 rows, 1 fields, 1 spw, 2 antennas",
            units: "Jy, Hz, seconds",
            fields: ["0: Target"],
            spectralWindows: ["spw 0: 4 chan, 1.420000 GHz center"],
            dataColumns: ["DATA"],
            notes: "Recognized by Rust probe."
        )
        let taskClient = HoldingDirtyImagingTaskClient()
        let store = WorkbenchStore(
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(
                        name: "Real Project",
                        rootPath: "/data",
                        datasets: [probedDataset],
                        source: .probed
                    ),
                    diagnostics: []
                )
            ),
            dirtyImagingClient: taskClient
        )

        store.openProject(path: "/data")
        store.openDefaultTab(kind: .task)
        store.runTask()

        let runID = tryUnwrap(store.state.taskRun.runID)
        XCTAssertEqual(store.debugSnapshot().runningJobCount, 1)
        XCTAssertEqual(store.debugSnapshot().jobs.first?.kind, .dirtyImagingTask)
        XCTAssertEqual(store.debugSnapshot().jobs.first?.status, .running)

        store.stopTask()

        let snapshot = store.debugSnapshot()
        XCTAssertEqual(taskClient.execution.didCancel, true)
        XCTAssertEqual(snapshot.runningJobCount, 0)
        XCTAssertEqual(snapshot.jobs.first?.id, runID)
        XCTAssertEqual(snapshot.jobs.first?.status, .cancelled)
        XCTAssertEqual(snapshot.jobs.first?.cancellationRequested, true)
        XCTAssertEqual(snapshot.taskState, .cancelled)
        XCTAssertTrue(snapshot.activeJobIDsByTab.isEmpty)

        taskClient.emitSucceeded()
        XCTAssertEqual(store.debugSnapshot().jobs.first?.status, .cancelled)
        XCTAssertEqual(store.debugSnapshot().taskState, .cancelled)
    }

    func testInterfaceFontSizeIsAdjustableClampedAndPreservedAcrossFixtureOpen() {
        let store = WorkbenchStore.fixture()

        store.adjustInterfaceFontSize(by: 3)
        XCTAssertEqual(store.state.interfaceFontSize, WorkbenchState.defaultInterfaceFontSize + 3)

        store.setInterfaceFontSize(100)
        XCTAssertEqual(store.state.interfaceFontSize, WorkbenchState.maximumInterfaceFontSize)

        store.setInterfaceFontSize(5)
        XCTAssertEqual(store.state.interfaceFontSize, WorkbenchState.minimumInterfaceFontSize)

        store.setInterfaceFontSize(17)
        store.openFixtureProject()
        XCTAssertEqual(store.debugSnapshot().interfaceFontSize, 17)

        store.resetInterfaceFontSize()
        XCTAssertEqual(store.state.interfaceFontSize, WorkbenchState.defaultInterfaceFontSize)
    }

    func testPlotImageCacheIDTracksFullImageBytes() {
        let first = makePlotResult(imageBytes: Data([0x89, 0x50, 1, 2, 3, 0x0a]))
        let second = makePlotResult(imageBytes: Data([0x89, 0x50, 1, 9, 3, 0x0a]))

        XCTAssertNotEqual(first.imageCacheID, second.imageCacheID)
    }

    func testDirtyImagingClientFindsBundledImagerHelperAfterEnvironmentOverride() {
        let bundleExecutable = URL(fileURLWithPath: "/Applications/casars-mac.app/Contents/MacOS/casars-mac")

        XCTAssertEqual(
            ProcessDirtyImagingTaskClient.resolvedExecutablePath(
                environment: ["CASARS_IMAGER_BIN": "/custom/casars-imager"],
                bundleExecutableURL: bundleExecutable,
                isExecutable: { _ in true }
            ),
            "/custom/casars-imager"
        )
        XCTAssertEqual(
            ProcessDirtyImagingTaskClient.resolvedExecutablePath(
                environment: [:],
                bundleExecutableURL: bundleExecutable,
                isExecutable: { $0 == "/Applications/casars-mac.app/Contents/MacOS/casars-imager" }
            ),
            "/Applications/casars-mac.app/Contents/MacOS/casars-imager"
        )
        XCTAssertNil(
            ProcessDirtyImagingTaskClient.resolvedExecutablePath(
                environment: [:],
                bundleExecutableURL: bundleExecutable,
                isExecutable: { _ in false }
            )
        )
    }
}

private struct StubProjectProbeClient: ProjectProbeClient {
    var result: ProjectFixtureProbe
    var probedPaths: [String: DatasetSummary] = [:]

    func probeProject(path: String) throws -> ProjectFixtureProbe {
        result
    }

    func probePath(path: String) throws -> DatasetSummary? {
        probedPaths[path]
    }
}

private final class StubMeasurementSetPlotClient: MeasurementSetPlotClient {
    private let lock = NSLock()
    private var recordedRequests: [MeasurementSetPlotBuildRequest] = []

    var requests: [MeasurementSetPlotBuildRequest] {
        lock.lock()
        defer { lock.unlock() }
        return recordedRequests
    }

    func buildPlot(request: MeasurementSetPlotBuildRequest) throws -> MeasurementSetPlotResultSummary {
        lock.lock()
        recordedRequests.append(request)
        lock.unlock()
        return makePlotResult(
            preset: request.preset,
            presetLabel: request.preset.title,
            title: request.preset.title,
            datasetPath: request.datasetPath,
            dataColumn: request.dataColumn.lowercased(),
            requestedMaxPoints: request.maxPlotPoints,
            imageWidth: request.width,
            imageHeight: request.height
        )
    }
}

private final class BlockingMeasurementSetPlotClient: MeasurementSetPlotClient {
    private let lock = NSLock()
    private let startedSemaphore = DispatchSemaphore(value: 0)
    private let releaseSemaphore = DispatchSemaphore(value: 0)
    private var startedCount = 0

    func buildPlot(request: MeasurementSetPlotBuildRequest) throws -> MeasurementSetPlotResultSummary {
        lock.lock()
        startedCount += 1
        lock.unlock()
        startedSemaphore.signal()
        releaseSemaphore.wait()
        return makePlotResult(
            preset: request.preset,
            presetLabel: request.preset.title,
            title: request.preset.title,
            datasetPath: request.datasetPath,
            dataColumn: request.dataColumn,
            requestedMaxPoints: request.maxPlotPoints,
            imageWidth: request.width,
            imageHeight: request.height
        )
    }

    func waitForStartedCount(_ count: Int) -> DispatchTimeoutResult {
        while true {
            lock.lock()
            let current = startedCount
            lock.unlock()
            if current >= count {
                return .success
            }
            if startedSemaphore.wait(timeout: .now() + 1) == .timedOut {
                return .timedOut
            }
        }
    }

    func releaseAll() {
        lock.lock()
        let count = max(startedCount, 1)
        lock.unlock()
        for _ in 0..<count {
            releaseSemaphore.signal()
        }
    }
}

private func makePlotResult(
    preset: MeasurementSetExplorerPlotPreset = .uvCoverage,
    presetLabel: String = "UV Coverage",
    title: String = "UV Coverage",
    datasetPath: String = "/data/probed.ms",
    dataColumn: String = "DATA",
    requestedMaxPoints: UInt64 = 250_000,
    imageWidth: UInt32 = 960,
    imageHeight: UInt32 = 600,
    imageBytes: Data
        = Data([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a])
) -> MeasurementSetPlotResultSummary {
    MeasurementSetPlotResultSummary(
        preset: preset,
        presetLabel: presetLabel,
        title: title,
        summary: "Synthetic plot result for tests.",
        datasetPath: datasetPath,
        dataColumn: dataColumn,
        selectionSummary: "data column \(dataColumn)",
        xAxis: PlotAxisSummary(id: "frequency", label: "Frequency (Hz)", unit: "Hz"),
        yAxis: PlotAxisSummary(id: "amplitude", label: "Amplitude", unit: ""),
        series: [
            PlotSeriesSummary(label: "Target", colorGroup: "field-0", pointCount: 42, firstRow: 0, lastRow: 11)
        ],
        requestedMaxPoints: requestedMaxPoints,
        renderedPointCount: 42,
        diagnostics: [],
        renderer: "stub renderer",
        imageFormat: "png",
        imageWidth: imageWidth,
        imageHeight: imageHeight,
        imageBytes: imageBytes
    )
}

private final class StubDirtyImagingTaskClient: DirtyImagingTaskClient {
    var requests: [DirtyImagingTaskRequest] = []
    var event: DirtyImagingTaskEvent?

    func startDirtyImaging(
        request: DirtyImagingTaskRequest,
        eventHandler: @escaping (DirtyImagingTaskEvent) -> Void
    ) throws -> DirtyImagingTaskExecution {
        requests.append(request)
        let result = DirtyImagingTaskResult(
            request: request,
            report: DirtyImagingRunReport(
                warnings: ["synthetic warning"],
                griddedSamples: 128,
                majorCycles: 1,
                minorIterations: 0,
                channelCount: 1
            ),
            artifacts: [
                DirtyImagingArtifact(
                    kind: "image",
                    label: "Dirty Image",
                    path: "/data/casa-rs-runs/output.image",
                    exists: true,
                    previewPngPath: "/data/casa-rs-runs/output.image.png",
                    previewPngExists: true
                )
            ],
            requestJSONPath: "/data/casa-rs-runs/output.casars-request.json",
            stdoutPath: "/data/casa-rs-runs/output.casars-result.json",
            stderrPath: "/data/casa-rs-runs/output.casars-stderr.log",
            protocolSummary: #"{"protocol_name":"casars_imager_task"}"#,
            diagnostics: ["synthetic warning"]
        )
        eventHandler(event ?? .succeeded(result))
        return StubDirtyImagingExecution()
    }
}

private final class HoldingDirtyImagingTaskClient: DirtyImagingTaskClient {
    var requests: [DirtyImagingTaskRequest] = []
    var handler: ((DirtyImagingTaskEvent) -> Void)?
    let execution = StubDirtyImagingExecution()

    func startDirtyImaging(
        request: DirtyImagingTaskRequest,
        eventHandler: @escaping (DirtyImagingTaskEvent) -> Void
    ) throws -> DirtyImagingTaskExecution {
        requests.append(request)
        handler = eventHandler
        return execution
    }

    func emitSucceeded() {
        guard let request = requests.last else { return }
        handler?(.succeeded(DirtyImagingTaskResult(
            request: request,
            report: DirtyImagingRunReport(
                warnings: [],
                griddedSamples: 128,
                majorCycles: 1,
                minorIterations: 0,
                channelCount: 1
            ),
            artifacts: [],
            requestJSONPath: "/data/casa-rs-runs/output.casars-request.json",
            stdoutPath: "/data/casa-rs-runs/output.casars-result.json",
            stderrPath: "/data/casa-rs-runs/output.casars-stderr.log",
            protocolSummary: #"{"protocol_name":"casars_imager_task"}"#,
            diagnostics: []
        )))
    }
}

private func waitFor(
    _ description: String,
    timeout: TimeInterval = 2,
    condition: () -> Bool
) {
    let deadline = Date().addingTimeInterval(timeout)
    while Date() < deadline {
        if condition() {
            return
        }
        RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.01))
    }
    XCTFail("Timed out waiting for \(description)")
}

private func tryUnwrap<T>(_ value: T?, file: StaticString = #filePath, line: UInt = #line) -> T {
    guard let value else {
        XCTFail("Expected non-nil value", file: file, line: line)
        fatalError("Expected non-nil value")
    }
    return value
}

private final class StubDirtyImagingExecution: DirtyImagingTaskExecution {
    var didCancel = false

    func cancel() {
        didCancel = true
    }
}
