import Foundation
import XCTest
@testable import CasarsMacCore

final class NotebookPrototypeRuntimeIsolationTests: XCTestCase {
    func testPrototypeFactoryStartsFreshWithNoProductionBoundaryCalls() {
        let recorder = PrototypeBoundaryRecorder()
        var store: WorkbenchStore? = WorkbenchStore.notebookPrototype(
            scenario: .primary,
            dependencies: recorder.dependencies
        )

        XCTAssertEqual(recorder.invocations, [])
        XCTAssertEqual(store?.isNotebookPrototypeRuntime, true)
        XCTAssertEqual(store?.state.isNotebookPrototype, true)
        XCTAssertEqual(store?.state.project.source, .fixture)
        XCTAssertTrue(store?.state.jobs.isEmpty == true)
        XCTAssertTrue(store?.state.parameterSessions.isEmpty == true)

        store = nil
        XCTAssertEqual(recorder.invocations, [], "Prototype teardown must not invoke demo cleanup or persistence.")
    }

    func testProductionStoreCannotTransitionIntoPrototypeRuntime() {
        let store = WorkbenchStore()

        store.openScientificNotebookPrototype(scenario: .primary)

        XCTAssertFalse(store.isNotebookPrototypeRuntime)
        XCTAssertFalse(store.state.isNotebookPrototype)
        XCTAssertEqual(store.state.project.source, .none)
        XCTAssertTrue(store.state.lastErrors.contains { $0.contains("fresh dedicated CLI/dev launch") })
    }

    func testProductionStoreWithRunningJobRejectsPrototypeWithoutCancellingOrReplacingState() throws {
        var initialState = FixtureWorkbench.makeState()
        let job = WorkbenchJob(
            id: "existing-run",
            tabID: initialState.activeTabID,
            kind: .genericTask,
            owner: .user,
            status: .running,
            progress: 0.4,
            title: "Existing production task",
            detail: "Must remain owned by the production runtime."
        )
        initialState.jobs[job.id] = job
        initialState.activeJobIDsByTab[job.tabID] = job.id
        let store = WorkbenchStore(state: initialState)

        store.openScientificNotebookPrototype(scenario: .externalConflict)

        XCTAssertFalse(store.isNotebookPrototypeRuntime)
        XCTAssertFalse(store.state.isNotebookPrototype)
        XCTAssertEqual(try XCTUnwrap(store.state.jobs[job.id]), job)
        XCTAssertEqual(store.state.activeJobIDsByTab[job.tabID], job.id)
        XCTAssertFalse(try XCTUnwrap(store.state.jobs[job.id]).cancellationRequested)
    }

    func testPrototypeRejectsProductionRoutesBeforeEveryInjectedAdapter() {
        let recorder = PrototypeBoundaryRecorder()
        let store = WorkbenchStore.notebookPrototype(
            scenario: .primary,
            dependencies: recorder.dependencies
        )
        let originalProject = store.state.project
        let originalTabs = store.state.tabs
        let periodicErrorCount = store.state.lastErrors.count

        store.refreshProjectFromDiskIfNeeded(now: .distantFuture)
        XCTAssertEqual(store.state.lastErrors.count, periodicErrorCount)

        store.openProject(path: "/never/project")
        store.openExternalMeasurementSetForImaging(path: "/never/input.ms")
        store.openFixtureProject()
        store.openTutorialTemplate(path: "/never/pack.json")
        store.refreshProjectFromDisk()
        store.selectDockMode(.history)
        store.openDefaultTab(kind: .history)
        store.openTab(WorkbenchTab(id: "forbidden-history", title: "History", kind: .history))
        store.openImageExplorerPath("/never/image")
        store.openTableBrowserPath("/never/table")
        store.openSelectedDatasetExplorer()
        store.loadTaskUISchemaIfNeeded("imager")
        store.setGenericTaskValue(taskID: "imager", argumentID: "niter", value: "10")
        store.selectParameterSource(.defaults, surfaceID: "imager")
        store.saveParameterProfile(surfaceID: "imager", to: "/never/profile.toml")
        store.runTask()
        store.runMeasurementSetPlot(datasetID: "prototype-twhya-ms")
        store.refreshImageExplorer(datasetID: "prototype-twhya-ms")
        store.refreshTableBrowser(datasetID: "prototype-twhya-ms")
        store.requestTableBrowserCellWindow(
            rowStart: 0,
            rowLimit: 10,
            columnStart: 0,
            columnLimit: 5,
            datasetID: "prototype-twhya-ms"
        )
        var cellValueRejected = false
        store.loadTableBrowserCellValue(
            rowIndex: 0,
            columnIndex: 0,
            datasetID: "prototype-twhya-ms"
        ) { result in
            if case .failure = result {
                cellValueRejected = true
            }
        }
        store.loadImageExplorerRegionFile(path: "/never/region.crtf", datasetID: "prototype-twhya-ms")
        store.exportImageExplorerRegionFile(datasetID: "prototype-twhya-ms", path: "/never/export.crtf")
        store.saveActiveTaskOutput(to: "/never/result.txt")

        XCTAssertTrue(cellValueRejected)
        XCTAssertEqual(recorder.invocations, [])
        XCTAssertEqual(store.state.project, originalProject)
        XCTAssertEqual(store.state.tabs, originalTabs)
        XCTAssertEqual(store.state.dockMode, .notebooks)
        XCTAssertTrue(store.state.taskUISchemas.isEmpty)
        XCTAssertTrue(store.state.parameterSessions.isEmpty)
        XCTAssertTrue(store.state.jobs.isEmpty)
        XCTAssertTrue(store.state.measurementSetPlots.isEmpty)
        XCTAssertTrue(store.state.imageExplorers.isEmpty)
        XCTAssertTrue(store.state.tableBrowsers.isEmpty)
    }

    func testStaleFixtureCompletionCannotFinishNewerRetry() throws {
        let store = WorkbenchStore.notebookPrototype(scenario: .primary)
        let receiptID = "receipt-imager-mfs"

        XCTAssertNotNil(store.restartPrototypeNotebookTask(receiptID: receiptID))
        let firstRevisionID = try XCTUnwrap(store.runningPrototypeRevisionID(receiptID: receiptID))
        store.cancelPrototypeNotebookTaskRun(receiptID: receiptID)

        XCTAssertNotNil(store.restartPrototypeNotebookTask(receiptID: receiptID))
        let secondRevisionID = try XCTUnwrap(store.runningPrototypeRevisionID(receiptID: receiptID))
        XCTAssertNotEqual(firstRevisionID, secondRevisionID)

        store.completePrototypeNotebookTaskRevision(
            receiptID: receiptID,
            revisionID: firstRevisionID
        )
        XCTAssertEqual(store.runningPrototypeRevisionID(receiptID: receiptID), secondRevisionID)
        XCTAssertEqual(
            store.state.prototypeNotebook?.task(receiptID: receiptID)?.latestRevision?.status,
            .running
        )

        store.completePrototypeNotebookTaskRevision(
            receiptID: receiptID,
            revisionID: secondRevisionID
        )
        XCTAssertNil(store.runningPrototypeRevisionID(receiptID: receiptID))
        XCTAssertEqual(
            store.state.prototypeNotebook?.task(receiptID: receiptID)?.latestRevision?.status,
            .succeeded
        )
    }
}

private enum PrototypeBoundaryRecorderError: Error {
    case invoked(String)
}

private final class PrototypeBoundaryRecorder:
    ProjectProbeClient,
    DemoProjectClient,
    MeasurementSetPlotClient,
    ImageExplorerClient,
    TableBrowserClient,
    GenericTaskClient,
    TaskUISchemaClient,
    SurfaceParameterClient
{
    private(set) var invocations: [String] = []

    var dependencies: NotebookPrototypeRuntimeDependencies {
        NotebookPrototypeRuntimeDependencies(
            probeClient: self,
            demoProjectClient: self,
            plotClient: self,
            imageExplorerClient: self,
            tableBrowserClient: self,
            genericTaskClient: self,
            taskUISchemaClient: self,
            surfaceParameterClient: self
        )
    }

    private func fail<T>(_ boundary: String) throws -> T {
        invocations.append(boundary)
        throw PrototypeBoundaryRecorderError.invoked(boundary)
    }

    func probeProject(path: String) throws -> ProjectFixtureProbe { try fail("project.probe") }
    func probePath(path: String) throws -> DatasetSummary? { try fail("dataset.probe") }
    func createDemoProject() throws -> ProjectFixtureProbe { try fail("demo.create") }
    func cleanupDemoProject(rootPath: String) { invocations.append("demo.cleanup") }
    func buildPlot(request: MeasurementSetPlotBuildRequest) throws -> MeasurementSetPlotResultSummary {
        try fail("plot")
    }
    func buildSnapshot(request: ImageExplorerSnapshotRequest) throws -> ImageExplorerSnapshot {
        try fail("image")
    }
    func buildSnapshot(request: TableBrowserSnapshotRequest) throws -> TableBrowserSnapshot {
        try fail("table.snapshot")
    }
    func buildCellWindow(request: TableBrowserCellWindowRequest) throws -> TableBrowserCellWindowSnapshot {
        try fail("table.window")
    }
    func buildCellValue(request: TableBrowserCellValueRequest) throws -> String {
        try fail("table.value")
    }
    func startTask(
        request: GenericTaskRequest,
        eventHandler: @escaping (GenericTaskEvent) -> Void
    ) throws -> TaskExecution {
        try fail("process")
    }
    func loadTaskUISchema(taskID: String) throws -> TaskUISchema { try fail("schema") }
    func loadBundle(surfaceID: String) throws -> SurfaceParameterBundle { try fail("parameter.bundle") }
    func defaults(surfaceID: String) throws -> SurfaceParameterSnapshot { try fail("parameter.defaults") }
    func last(surfaceID: String, workspace: String, successful: Bool) throws -> SurfaceParameterSnapshot? {
        try fail("parameter.last")
    }
    func load(surfaceID: String, profileTOML: String, sourcePath: String) throws -> SurfaceParameterSnapshot {
        try fail("parameter.load")
    }
    func resolve(
        surfaceID: String,
        baseSource: SurfaceParameterBaseSource,
        profileTOML: String?,
        profilePath: String?,
        context: SurfaceParameterPatch,
        override: SurfaceParameterPatch
    ) throws -> SurfaceParameterSnapshot {
        try fail("parameter.resolve")
    }
    func save(
        surfaceID: String,
        values: [String: SurfaceParameterValue],
        destinationPath: String
    ) throws -> SurfaceParameterWriteResult {
        try fail("parameter.save")
    }
    func writeLast(
        surfaceID: String,
        workspace: String,
        values: [String: SurfaceParameterValue],
        successful: Bool
    ) throws -> SurfaceParameterWriteResult {
        try fail("parameter.writeLast")
    }
    func runSafety(
        surfaceID: String,
        values: [String: SurfaceParameterValue]
    ) throws -> SurfaceRunSafety {
        try fail("parameter.runSafety")
    }
    func providerInvocation(
        surfaceID: String,
        values: [String: SurfaceParameterValue]
    ) throws -> SurfaceProviderInvocation {
        try fail("parameter.providerInvocation")
    }
}
