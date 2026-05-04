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
            )
        )
        let taskClient = StubDirtyImagingTaskClient()
        let store = WorkbenchStore(probeClient: probeClient, dirtyImagingClient: taskClient)

        store.openProject(path: "/data")
        store.openDefaultTab(kind: .task)
        store.setDirtyImagingImageSize(256)
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
        XCTAssertEqual(snapshot.taskRequest?.cellArcsec, 0.25)
        XCTAssertTrue(snapshot.taskOutputPaths.contains("/data/casa-rs-runs/output.image"))
        XCTAssertTrue(snapshot.processingHistoryEvents.contains("Dirty imaging completed"))
        XCTAssertTrue(store.state.project.datasets.contains { $0.path == "/data/casa-rs-runs/output.image" })
        XCTAssertNoThrow(try store.debugJSON())
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
        store.setDirtyImagingCellArcsec(-1)
        store.runTask()

        XCTAssertEqual(taskClient.requests.count, 0)
        XCTAssertEqual(store.debugSnapshot().taskState, .failed)
        XCTAssertTrue(store.debugSnapshot().taskDiagnostics.contains("Image size must be positive."))
        XCTAssertTrue(store.debugSnapshot().taskDiagnostics.contains("Cell size must be a positive finite arcsecond value."))
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
}

private struct StubProjectProbeClient: ProjectProbeClient {
    var result: ProjectFixtureProbe

    func probeProject(path: String) throws -> ProjectFixtureProbe {
        result
    }
}

private final class StubMeasurementSetPlotClient: MeasurementSetPlotClient {
    var requests: [MeasurementSetPlotBuildRequest] = []

    func buildPlot(request: MeasurementSetPlotBuildRequest) throws -> MeasurementSetPlotResultSummary {
        requests.append(request)
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

private final class StubDirtyImagingExecution: DirtyImagingTaskExecution {
    var didCancel = false

    func cancel() {
        didCancel = true
    }
}
