import Foundation
import XCTest
@testable import CasarsMacCore

final class WorkbenchStoreTests: XCTestCase {
    func testAssistantContextsUseEachTaskTabSessionAndPreserveUserSelection() throws {
        let parameters = RecordingSurfaceParameterClient()
        let bundle = try parameters.loadBundle(surfaceID: "imager")
        let snapshot = try parameters.defaults(surfaceID: "imager")
        var first = SurfaceParameterSession(
            bundle: bundle,
            snapshot: snapshot,
            selectedSource: .defaults,
            baseProfileTOML: nil,
            baseProfilePath: nil,
            workspace: "/project"
        )
        var second = first
        first.draftText["robust"] = "0.25"
        second.draftText["robust"] = "-1.0"

        var discussion = AssistantDiscussionState()
        discussion.contexts = [
            AssistantContextItemState(
                id: "task:task-a",
                kind: "task",
                label: "First",
                summary: "previous",
                excerpt: "previous",
                byteCount: 8,
                contentSha256: "old",
                untrustedEvidence: true,
                selected: false
            ),
            AssistantContextItemState(
                id: "task:task-b",
                kind: "task",
                label: "Second",
                summary: "previous",
                excerpt: "previous",
                byteCount: 8,
                contentSha256: "old",
                untrustedEvidence: true,
                selected: true
            ),
        ]
        var state = EmptyWorkbench.makeState()
        state.tabs = [
            WorkbenchTab(id: "task-a", title: "First", kind: .task, taskID: "imager"),
            WorkbenchTab(id: "task-b", title: "Second", kind: .task, taskID: "imager"),
        ]
        state.parameterSessions = [
            "task-a::imager": first,
            "task-b::imager": second,
        ]
        state.assistantDiscussion = discussion
        let store = WorkbenchStore(state: state)

        store.refreshAssistantDiscussionContexts()

        let contexts = try XCTUnwrap(store.state.assistantDiscussion?.contexts)
        let firstContext = try XCTUnwrap(contexts.first { $0.id == "task:task-a" })
        let secondContext = try XCTUnwrap(contexts.first { $0.id == "task:task-b" })
        XCTAssertTrue(firstContext.excerpt.contains("robust = 0.25"))
        XCTAssertTrue(secondContext.excerpt.contains("robust = -1.0"))
        XCTAssertFalse(firstContext.selected)
        XCTAssertTrue(secondContext.selected)
    }

    func testAssistantSendRefreshesTheBoundedMCPProjectionFromCurrentTabs() throws {
        let project = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-assistant-context-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: project, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: project) }

        let parameters = RecordingSurfaceParameterClient()
        let bundle = try parameters.loadBundle(surfaceID: "imager")
        let snapshot = try parameters.defaults(surfaceID: "imager")
        var session = SurfaceParameterSession(
            bundle: bundle,
            snapshot: snapshot,
            selectedSource: .defaults,
            baseProfileTOML: nil,
            baseProfilePath: nil,
            workspace: project.path
        )
        session.draftText["robust"] = "-0.75"

        let persistence = UniFFIAssistantPersistenceClient()
        var conversation = try persistence.createConversation(
            projectRoot: project.path,
            title: "Context refresh",
            attachment: AssistantAttachmentState(
                kind: "task",
                identifier: "imager",
                label: "Imager",
                primary: true
            ),
            profile: AssistantSessionProfileState()
        )
        conversation.draft = "Inspect the current imager parameters"
        var discussion = AssistantDiscussionState()
        discussion.conversations = [conversation]
        discussion.activeConversationID = conversation.id
        discussion.contexts = [AssistantContextItemState(
            id: "task:task-a",
            kind: "task",
            label: "Stale",
            summary: "stale",
            excerpt: "robust = 0.0",
            byteCount: 12,
            contentSha256: "old",
            untrustedEvidence: true,
            selected: true
        )]

        var state = EmptyWorkbench.makeState()
        state.project.rootPath = project.path
        state.project.source = .probed
        state.tabs = [WorkbenchTab(id: "task-a", title: "Imager", kind: .task, taskID: "imager")]
        state.parameterSessions = ["task-a::imager": session]
        state.assistantDiscussion = discussion
        let store = WorkbenchStore(state: state)

        store.sendAssistantPrompt()

        let projectionURL = project.appendingPathComponent(".casa-rs/assistant-context.json")
        let projectionData = try Data(contentsOf: projectionURL)
        let projection = try XCTUnwrap(
            JSONSerialization.jsonObject(with: projectionData) as? [String: Any]
        )
        let openTabs = try XCTUnwrap(projection["open_tabs"] as? [[String: Any]])
        XCTAssertEqual(openTabs.count, 1)
        XCTAssertTrue((openTabs[0]["excerpt"] as? String)?.contains("robust = -0.75") == true)
        XCTAssertEqual(
            store.state.assistantDiscussion?.activeConversation?.selectedContextIds,
            ["task:task-a"]
        )
    }

    func testLocalTWHyaMeasurementSetPlotTimingDiagnostic() throws {
        guard ProcessInfo.processInfo.environment["CASA_RS_RUN_LOCAL_TIMING"] == "1" else {
            throw XCTSkip("Set CASA_RS_RUN_LOCAL_TIMING=1 to run local TW Hya plot timing diagnostics.")
        }
        let msPath = ProcessInfo.processInfo.environment["CASA_RS_TWHYA_MS"]
            ?? "/private/tmp/casa-rs-wave6-prof/twhya_calibrated.ms"
        guard FileManager.default.fileExists(atPath: msPath) else {
            throw XCTSkip("\(msPath) is not staged")
        }

        let client = UniFFIMeasurementSetPlotClient()
        for preset in [
            MeasurementSetExplorerPlotPreset.scanTimeline,
            .amplitudeVsTime,
            .phaseVsTime,
            .amplitudePhaseVsTimeStacked,
        ] {
            let startedAt = Date()
            let result = try client.buildPlot(
                request: MeasurementSetPlotBuildRequest(
                    datasetPath: msPath,
                    preset: preset,
                    field: nil,
                    spectralWindow: nil,
                    correlation: nil,
                    dataColumn: "DATA"
                )
            )
            let elapsedMilliseconds = Date().timeIntervalSince(startedAt) * 1000
            print(
                "\(preset.rawValue): total=\(String(format: "%.0f", elapsedMilliseconds)) ms, points=\(result.renderedPointCount), layers=\(result.plotDocument.layers.count), panels=\(result.plotDocument.panels.count), diagnostics=\(result.diagnostics)"
            )
        }
    }

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

    func testScientificNotebookPrototypeOpensExpectedDocumentAndReceipts() throws {
        let store = WorkbenchStore.notebookPrototype(scenario: .primary)

        let notebook = try XCTUnwrap(store.state.prototypeNotebook)
        let snapshot = try XCTUnwrap(store.debugSnapshot().prototypeNotebook)
        XCTAssertEqual(store.state.project.source, .fixture)
        XCTAssertEqual(store.state.dockMode, .notebooks)
        XCTAssertEqual(store.state.activeTabID, "tab-scientific-notebook")
        XCTAssertEqual(notebook.filename, "Analysis.md")
        XCTAssertEqual(notebook.notebooks.map(\.filename), ["Analysis.md", "Observation Log.md"])
        XCTAssertEqual(notebook.receipts.count, 3)
        XCTAssertEqual(notebook.receipts.compactMap(\.latestRevision?.status), [.succeeded, .failed, .cancelled])
        let markdown = notebook.draftMarkdown
        let firstCell = try XCTUnwrap(markdown.range(of: "id=receipt-imager-mfs"))
        let betweenNote = try XCTUnwrap(markdown.range(of: "Correct the reference image"))
        let secondCell = try XCTUnwrap(markdown.range(of: "id=receipt-impbcor-failed"))
        XCTAssertLessThan(firstCell.lowerBound, betweenNote.lowerBound)
        XCTAssertLessThan(betweenNote.lowerBound, secondCell.lowerBound)
        XCTAssertTrue(markdown.contains("```toml"))
        XCTAssertEqual(snapshot.prototypeKind, .notebook)
        XCTAssertEqual(snapshot.scenario, .primary)
        XCTAssertEqual(snapshot.activeNotebookID, "notebook-twhya-analysis")
        XCTAssertEqual(snapshot.notebookFilenames, ["Analysis.md", "Observation Log.md"])
        XCTAssertEqual(snapshot.receiptIDs, notebook.receipts.map(\.id))
        XCTAssertFalse(snapshot.isDirty)
        XCTAssertFalse(snapshot.hasExternalConflict)
    }

    func testNotebookPrototypeFactoryUsesFixtureBootstrapContracts() {
        let store = WorkbenchStore.notebookPrototype(scenario: .primary)

        XCTAssertTrue(store.state.taskCatalog.isEmpty)
        XCTAssertTrue(store.state.taskExecutionMatrixRows.isEmpty)
        XCTAssertTrue(store.state.lastErrors.isEmpty)
        XCTAssertTrue(store.state.isNotebookPrototype)
    }

    func testScientificNotebookPrototypeSelectsNamedNotebooksAndPreservesDraftsInMemory() throws {
        let store = WorkbenchStore.notebookPrototype(scenario: .primary)
        let original = try XCTUnwrap(store.state.prototypeNotebook?.draftMarkdown)

        store.setPrototypeNotebookDraft(original + "\n\nA user note.")
        XCTAssertEqual(store.debugSnapshot().prototypeNotebook?.isDirty, true)
        store.selectPrototypeNotebook("notebook-twhya-observation-log")
        XCTAssertEqual(store.state.prototypeNotebook?.filename, "Observation Log.md")
        XCTAssertEqual(store.state.prototypeNotebook?.receipts.map(\.taskID), ["listobs"])
        store.setPrototypeNotebookDraft("Updated observation log")
        store.selectPrototypeNotebook("notebook-twhya-analysis")
        XCTAssertEqual(store.state.prototypeNotebook?.draftMarkdown, original + "\n\nA user note.")
        store.savePrototypeNotebookDraft()
        XCTAssertEqual(store.debugSnapshot().prototypeNotebook?.isDirty, false)

        store.openScientificNotebookPrototype(scenario: .externalConflict)
        XCTAssertEqual(store.debugSnapshot().prototypeNotebook?.hasExternalConflict, true)
        XCTAssertEqual(store.debugSnapshot().prototypeNotebook?.isDirty, true)
        store.savePrototypeNotebookDraft()
        XCTAssertEqual(store.debugSnapshot().prototypeNotebook?.hasExternalConflict, true)
        XCTAssertEqual(store.debugSnapshot().prototypeNotebook?.isDirty, true)

        store.resolvePrototypeNotebookConflict(keepingDraft: false)
        XCTAssertEqual(store.debugSnapshot().prototypeNotebook?.hasExternalConflict, false)
        XCTAssertEqual(store.debugSnapshot().prototypeNotebook?.isDirty, false)

        store.openScientificNotebookPrototype(scenario: .externalConflict)
        let localDraft = try XCTUnwrap(store.state.prototypeNotebook?.draftMarkdown)
        store.resolvePrototypeNotebookConflict(keepingDraft: true)
        XCTAssertEqual(store.state.prototypeNotebook?.draftMarkdown, localDraft)
        XCTAssertEqual(store.debugSnapshot().prototypeNotebook?.hasExternalConflict, false)
        XCTAssertEqual(store.debugSnapshot().prototypeNotebook?.isDirty, true)
        store.savePrototypeNotebookDraft()
        XCTAssertEqual(store.debugSnapshot().prototypeNotebook?.isDirty, false)
    }

    func testPrototypeNotebookTaskTabUsesOnlyFixtureProjectionParameters() throws {
        let taskClient = StubGenericTaskClient()
        let parameterClient = RecordingSurfaceParameterClient()
        let store = WorkbenchStore.notebookPrototype(
            dependencies: notebookPrototypeDependencies(
                genericTaskClient: taskClient,
                surfaceParameterClient: parameterClient
            )
        )
        store.openPrototypeNotebookTask(receiptID: "receipt-imager-mfs")

        let taskTab = try XCTUnwrap(store.state.tabs.first { $0.prototypeReceiptID != nil })
        XCTAssertEqual(taskTab.taskID, "imager")
        XCTAssertEqual(taskTab.prototypeReceiptID, "receipt-imager-mfs")
        XCTAssertNil(taskTab.datasetID)
        let task = try XCTUnwrap(store.state.prototypeNotebook?.task(receiptID: "receipt-imager-mfs"))
        XCTAssertEqual(task.parameterRows.first { $0.parameterID == "niter" }?.value, "1000")
        XCTAssertEqual(task.parameterRows.first { $0.parameterID == "vis" }?.value, "data/twhya_calibrated.ms")
        XCTAssertFalse(task.annotation.isEmpty)
        XCTAssertTrue(store.state.parameterSessions.isEmpty)
        XCTAssertTrue(taskClient.requests.isEmpty)
        XCTAssertEqual(parameterClient.invocationCount, 0)

        store.openDefaultTab(kind: .task)
        XCTAssertEqual(store.state.tabs.filter { $0.prototypeReceiptID != nil }.count, 1)
        XCTAssertEqual(store.state.activeTabID, taskTab.id)
        XCTAssertTrue(taskClient.requests.isEmpty)
        XCTAssertEqual(parameterClient.invocationCount, 0)
    }

    func testPrototypeRawTaskCellEditsDriveRichProjectionAndTaskTab() throws {
        let taskClient = StubGenericTaskClient()
        let parameterClient = RecordingSurfaceParameterClient()
        let store = WorkbenchStore.notebookPrototype(
            dependencies: notebookPrototypeDependencies(
                genericTaskClient: taskClient,
                surfaceParameterClient: parameterClient
            )
        )
        let markdown = try XCTUnwrap(store.state.prototypeNotebook?.draftMarkdown)
            .replacingOccurrences(of: "niter = 1000", with: "niter = 2000")

        store.setPrototypeNotebookDraft(markdown)
        store.openPrototypeNotebookTask(receiptID: "receipt-imager-mfs")

        let task = try XCTUnwrap(store.state.prototypeNotebook?.task(receiptID: "receipt-imager-mfs"))
        XCTAssertEqual(task.parameterRows.first { $0.parameterID == "niter" }?.value, "2000")
        XCTAssertTrue(task.sparseProfileTOML.contains("niter = 2000"))
        XCTAssertEqual(
            store.state.tabs.first { $0.prototypeReceiptID == "receipt-imager-mfs" }?.taskID,
            "imager"
        )
        XCTAssertTrue(taskClient.requests.isEmpty)
        XCTAssertEqual(parameterClient.invocationCount, 0)
    }

    func testPrototypeNotebookRestartPreservesRevisionsWithoutProductionAdapters() throws {
        let taskClient = StubGenericTaskClient()
        let parameterClient = RecordingSurfaceParameterClient()
        let store = WorkbenchStore.notebookPrototype(
            dependencies: notebookPrototypeDependencies(
                genericTaskClient: taskClient,
                surfaceParameterClient: parameterClient
            )
        )
        let receiptID = "receipt-imager-mfs"
        let initialRevisions = try XCTUnwrap(
            store.state.prototypeNotebook?.task(receiptID: receiptID)?.revisions
        )

        XCTAssertEqual(try XCTUnwrap(store.restartPrototypeNotebookTask(receiptID: receiptID)), receiptID)
        var revisions = try XCTUnwrap(store.state.prototypeNotebook?.task(receiptID: receiptID)?.revisions)
        XCTAssertEqual(Array(revisions.prefix(initialRevisions.count)), initialRevisions)
        XCTAssertEqual(revisions.count, initialRevisions.count + 1)
        XCTAssertEqual(revisions.last?.status, .running)
        XCTAssertNil(store.restartPrototypeNotebookTask(receiptID: receiptID))
        XCTAssertEqual(revisions.filter { $0.status == .running }.count, 1)
        store.cancelPrototypeNotebookTaskRun(receiptID: receiptID)
        XCTAssertEqual(store.state.prototypeNotebook?.task(receiptID: receiptID)?.latestRevision?.status, .cancelled)

        XCTAssertEqual(try XCTUnwrap(store.restartPrototypeNotebookTask(receiptID: receiptID)), receiptID)
        store.completePrototypeNotebookTaskRun(receiptID: receiptID)
        revisions = try XCTUnwrap(store.state.prototypeNotebook?.task(receiptID: receiptID)?.revisions)
        XCTAssertEqual(Array(revisions.prefix(initialRevisions.count)), initialRevisions)
        XCTAssertEqual(revisions.count, initialRevisions.count + 2)
        XCTAssertEqual(revisions.last?.status, .succeeded)
        XCTAssertFalse(revisions.last?.products.isEmpty ?? true)
        XCTAssertTrue(taskClient.requests.isEmpty)
        XCTAssertEqual(parameterClient.invocationCount, 0)
    }

    func testPrototypeGuardsRejectProductionExplorersAndTaskExecution() throws {
        let taskClient = StubGenericTaskClient()
        let parameterClient = RecordingSurfaceParameterClient()
        let store = WorkbenchStore.notebookPrototype(
            dependencies: notebookPrototypeDependencies(
                genericTaskClient: taskClient,
                surfaceParameterClient: parameterClient
            )
        )
        let originalTabs = store.state.tabs

        store.openSelectedDatasetExplorer()
        store.openDatasetExplorer("prototype-twhya-ms")
        store.openDatasetTableBrowser("prototype-twhya-ms")
        store.openImageExplorerPath("/PrototypeProjects/never-open.image")
        store.openTableBrowserPath("/PrototypeProjects/never-open.table")
        store.openImagerTaskForSelectedDataset()
        store.openTab(WorkbenchTab(id: "forbidden-task", title: "Forbidden", kind: .task))
        store.runTask()

        XCTAssertEqual(store.state.tabs, originalTabs)
        XCTAssertFalse(store.state.tabs.contains { $0.kind == .datasetExplorer || $0.kind == .tableBrowser || $0.kind == .task })
        XCTAssertTrue(store.state.imageExplorers.isEmpty)
        XCTAssertTrue(store.state.tableBrowsers.isEmpty)
        XCTAssertTrue(store.state.parameterSessions.isEmpty)
        XCTAssertTrue(taskClient.requests.isEmpty)
        XCTAssertEqual(parameterClient.invocationCount, 0)
        XCTAssertTrue(store.state.lastErrors.contains { $0.contains("in-memory notebook prototype") })
    }

    func testTaskTabsCanOpenMultiplePanesAndRenameToSelectedTask() throws {
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            taskCatalogClient: StubTaskCatalogClient(tasks: [
                makeImheadTaskCatalogEntry(),
                makeTaskCatalogEntry(id: "immoments", displayName: "Image Moments"),
            ]),
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImheadTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.openDefaultTab(kind: .task)
        let firstTaskTabID = try XCTUnwrap(store.state.activeTabID)
        store.selectTask("imhead", tabID: firstTaskTabID)
        store.openDefaultTab(kind: .task)
        let secondTaskTabID = try XCTUnwrap(store.state.activeTabID)
        XCTAssertEqual(store.taskID(forTab: secondTaskTabID), "")
        XCTAssertEqual(store.state.tabs.last?.title, "Tasks")
        store.selectTask("immoments", tabID: secondTaskTabID)

        XCTAssertNotEqual(firstTaskTabID, secondTaskTabID)
        XCTAssertEqual(store.state.tabs.map(\.id), [firstTaskTabID, secondTaskTabID])
        XCTAssertEqual(store.state.tabs.map(\.title), ["Image Header", "Image Moments"])
        XCTAssertEqual(store.taskID(forTab: firstTaskTabID), "imhead")
        XCTAssertEqual(store.taskID(forTab: secondTaskTabID), "immoments")
    }

    func testRegionDatasetDoesNotSeedImagePathInput() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-region-seed-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let imageURL = rootURL.appendingPathComponent("twhya_cont.image")
        let regionURL = rootURL.appendingPathComponent("regions/twhya_cont.image-region.crtf")
        try FileManager.default.createDirectory(at: imageURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: regionURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        try "#CRTFv0 CASA Region Text Format version 0\nbox[[100pix,100pix],[150pix,150pix]]\n"
            .write(to: regionURL, atomically: true, encoding: .utf8)
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(
            name: "Tutorial",
            rootPath: rootURL.path,
            datasets: [
                DatasetSummary(
                    id: imageURL.path,
                    name: "twhya_cont.image",
                    path: imageURL.path,
                    kind: .imageCube,
                    size: "250 x 250",
                    units: "Jy/beam",
                    notes: "image"
                ),
                DatasetSummary(
                    id: regionURL.path,
                    name: regionURL.lastPathComponent,
                    path: regionURL.path,
                    kind: .region,
                    size: "region file",
                    units: "pixels",
                    notes: "region"
                )
            ],
            source: .probed
        )
        state.selectedDatasetID = regionURL.path
        state.taskCatalog = [
            TaskCatalogEntry(
                id: "imstat",
                category: "Images",
                displayName: "Image Statistics",
                binaryName: "imexplore",
                cargoPackage: "casa-images",
                overrideEnv: "CASARS_IMEXPLORE_BIN",
                shellKind: "workflow",
                interaction: "one_shot",
                browserKind: nil,
                datasetKinds: ["image_cube"],
                schemaSource: "binary",
                showInTUI: true,
                showInSwift: true,
                includeInSuite: true
            )
        ]
        let store = WorkbenchStore(
            state: state,
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImstatTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.loadTaskUISchemaIfNeeded("imstat")

        XCTAssertNil(store.state.genericTaskValues["imstat"]?["imagename"])
        XCTAssertEqual(store.state.genericTaskValues["imstat"]?["region"], "regions/twhya_cont.image-region.crtf")
    }

    func testRegionDatasetCanLoadIntoMatchingImageExplorer() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-region-load-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let imageURL = rootURL.appendingPathComponent("twhya_cont.image")
        let regionURL = rootURL.appendingPathComponent("regions/twhya_cont.image-region.crtf")
        try FileManager.default.createDirectory(at: imageURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: regionURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        try "#CRTFv0 CASA Region Text Format version 0\nbox[[100pix,100pix],[150pix,150pix]]\n"
            .write(to: regionURL, atomically: true, encoding: .utf8)
        let imageDataset = DatasetSummary(
            id: imageURL.path,
            name: "twhya_cont.image",
            path: imageURL.path,
            kind: .imageCube,
            size: "250 x 250",
            units: "Jy/beam",
            notes: "image"
        )
        let regionDataset = DatasetSummary(
            id: regionURL.path,
            name: regionURL.lastPathComponent,
            path: regionURL.path,
            kind: .region,
            size: "region file",
            units: "pixels",
            notes: "region",
            diagnostics: ["Region source image: twhya_cont.image"]
        )
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(
            name: "Tutorial",
            rootPath: rootURL.path,
            datasets: [imageDataset, regionDataset],
            source: .probed
        )
        state.selectedDatasetID = regionDataset.id
        let imageClient = StubImageExplorerClient(snapshot: makeImageExplorerSnapshot())
        let store = WorkbenchStore(state: state, imageExplorerClient: imageClient)

        store.loadRegionFileIntoImageExplorer(regionDatasetID: regionDataset.id)

        XCTAssertEqual(store.state.activeTabID, imageDataset.explorerTabID)
        XCTAssertEqual(store.state.selectedDatasetID, imageDataset.id)
        XCTAssertEqual(imageClient.requests.last?.datasetPath, imageDataset.path)
        XCTAssertEqual(imageClient.requests.last?.commands, [.appendRegionFile(path: regionDataset.path)])
        XCTAssertEqual(imageClient.requests.last?.transientCommands, [])
    }

    func testImageExplorerCanDeleteIndividualRegionShapes() throws {
        let imageDataset = DatasetSummary(
            id: "/data/restored.image",
            name: "restored.image",
            path: "/data/restored.image",
            kind: .imageCube,
            size: "4 x 4 x 8",
            units: "Jy/beam",
            shape: [4, 4, 8],
            notes: "Recognized by Rust probe."
        )
        var snapshot = makeImageExplorerSnapshot()
        snapshot.displayAxes = [
            ImageExplorerSnapshot.DisplayAxis(axis: 0, name: "Right Ascension", unit: "pix", blc: 0, trc: 3, inc: 1, sampledLen: 4),
            ImageExplorerSnapshot.DisplayAxis(axis: 1, name: "Declination", unit: "pix", blc: 0, trc: 3, inc: 1, sampledLen: 4)
        ]
        snapshot.region = ImageExplorerSnapshot.Region(
            label: "active region",
            shapeCount: 2,
            closedShapeCount: 2,
            editing: false,
            overlayShapes: [
                ImageExplorerSnapshot.Region.OverlayShape(
                    vertices: [
                        ImageExplorerSnapshot.Region.OverlayVertex(sampledX: 0, sampledY: 0),
                        ImageExplorerSnapshot.Region.OverlayVertex(sampledX: 1, sampledY: 0),
                        ImageExplorerSnapshot.Region.OverlayVertex(sampledX: 1, sampledY: 1)
                    ],
                    closed: true
                ),
                ImageExplorerSnapshot.Region.OverlayShape(
                    vertices: [
                        ImageExplorerSnapshot.Region.OverlayVertex(sampledX: 2, sampledY: 2),
                        ImageExplorerSnapshot.Region.OverlayVertex(sampledX: 3, sampledY: 2),
                        ImageExplorerSnapshot.Region.OverlayVertex(sampledX: 3, sampledY: 3)
                    ],
                    closed: true
                )
            ]
        )
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(name: "Real Project", rootPath: "/data", datasets: [imageDataset], source: .probed)
        state.imageExplorers[imageDataset.id] = ImageExplorerSessionState(
            datasetID: imageDataset.id,
            selectedView: "plane",
            status: .ready,
            snapshot: snapshot
        )
        let imageClient = StubImageExplorerClient(snapshot: snapshot)
        let store = WorkbenchStore(state: state, imageExplorerClient: imageClient)

        store.deleteImageExplorerRegionShape(index: 0, datasetID: imageDataset.id)

        let commands = imageClient.requests.last?.commands ?? []
        XCTAssertEqual(commands.map(\.command), [
            "start_region_shape",
            "append_region_vertex",
            "append_region_vertex",
            "append_region_vertex",
            "close_region_shape",
        ])
        XCTAssertEqual(commands.compactMap(\.x), [2, 3, 3])
        XCTAssertEqual(commands.compactMap(\.y), [2, 2, 3])
    }

    func testReloadingDirtyRegionFileRestoresSavedFileCommand() throws {
        let imageDataset = DatasetSummary(
            id: "/data/restored.image",
            name: "restored.image",
            path: "/data/restored.image",
            kind: .imageCube,
            size: "4 x 4 x 8",
            units: "Jy/beam",
            shape: [4, 4, 8],
            notes: "Recognized by Rust probe."
        )
        let regionPath = "/data/regions/restored.crtf"
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(name: "Real Project", rootPath: "/data", datasets: [imageDataset], source: .probed)
        var explorerState = ImageExplorerSessionState(
            datasetID: imageDataset.id,
            selectedView: "plane",
            status: .ready,
            snapshot: makeImageExplorerSnapshot()
        )
        explorerState.activeRegionFilePath = regionPath
        explorerState.regionCommands = [.loadRegionFile(path: regionPath)]
        state.imageExplorers[imageDataset.id] = explorerState
        let imageClient = StubImageExplorerClient(snapshot: makeImageExplorerSnapshot())
        let store = WorkbenchStore(state: state, imageExplorerClient: imageClient)

        store.setImageExplorerRegionShapes([[(x: 1, y: 1), (x: 3, y: 1), (x: 3, y: 3), (x: 1, y: 3)]], datasetID: imageDataset.id)
        XCTAssertEqual(store.state.imageExplorers[imageDataset.id]?.activeRegionFilePath, regionPath)
        XCTAssertEqual(imageClient.requests.last?.commands.map(\.command), [
            "start_region_shape",
            "append_region_vertex",
            "append_region_vertex",
            "append_region_vertex",
            "append_region_vertex",
            "close_region_shape",
        ])

        store.appendImageExplorerRegionFile(path: regionPath, datasetID: imageDataset.id)

        XCTAssertEqual(imageClient.requests.last?.commands, [.loadRegionFile(path: regionPath)])
        XCTAssertEqual(store.state.imageExplorers[imageDataset.id]?.activeRegionFilePath, regionPath)
    }

    func testRegionFileInspectionReportsPixelAndWorldExtents() throws {
        let pixel = RegionFileInspection.inspect(
            text: "#CRTFv0 CASA Region Text Format version 0\nbox[[100pix,100pix],[150pix,160pix]]\n"
        )
        XCTAssertEqual(pixel?.kind, "Box")
        XCTAssertEqual(pixel?.coordinateSystem, "Pixel")
        XCTAssertEqual(pixel?.xExtentLabel, "50 px")
        XCTAssertEqual(pixel?.yExtentLabel, "60 px")

        let world = RegionFileInspection.inspect(
            text: "#CRTFv0 CASA Region Text Format version 0\nbox[[0rad,0rad],[0.00024240684055476798rad,0.00012120342027738399rad]]\n"
        )
        XCTAssertEqual(world?.coordinateSystem, "World")
        XCTAssertEqual(world?.xExtentLabel, "50.00 arcsec")
        XCTAssertEqual(world?.yExtentLabel, "25.00 arcsec")
    }

    func testTaskCatalogLoadsFromFrontendServicesIntoDebugState() throws {
        let store = WorkbenchStore()

        let tasks = store.debugSnapshot().taskCatalog

        XCTAssertTrue(tasks.contains { $0.id == "msexplore" && $0.includeInSuite })
        XCTAssertTrue(tasks.contains { $0.id == "tablebrowser" && !$0.includeInSuite })
        XCTAssertTrue(tasks.contains { $0.id == "imager" && $0.binaryName == "casars-imager" })
    }

    func testTaskExecutionMatrixLoadsFromFrontendServices() throws {
        let matrix = try UniFFITaskExecutionMatrixClient().loadTaskExecutionMatrix()

        XCTAssertTrue(matrix.rows.contains { $0.taskID == "msexplore" && $0.tuiStatus == "invokable" })
        XCTAssertTrue(matrix.rows.contains { $0.taskID == "imager" && $0.guiStatus == "invokable" })
        XCTAssertTrue(matrix.rows.contains { $0.taskID == "flagdata" && $0.tuiStatus == "invokable" })
        XCTAssertTrue(matrix.rows.contains { $0.taskID == "mstransform" && $0.tuiStatus == "invokable" })
    }

    func testTaskContextOptionsDecodeRealDataChoices() throws {
        let optionsJSON = """
        {
          "schema_version": 1,
          "dataset_path": "/data/probed.ms",
          "dataset_kind": "measurement_set",
          "fields": ["0: Target"],
          "spectral_windows": ["spw 0: 4 chan, 1.420000 GHz center"],
          "scans": ["scan 1"],
          "arrays": [],
          "observations": [],
          "antennas": ["ea01"],
          "intents": [],
          "feeds": [],
          "correlations": ["I"],
          "columns": ["DATA", "FLAG"],
          "data_columns": ["DATA"],
          "subtables": ["ANTENNA (required)"],
          "shape": [4],
          "defaults": {
            "field": "0: Target",
            "spectral_window": "spw 0: 4 chan, 1.420000 GHz center",
            "data_column": "DATA"
          },
          "diagnostics": []
        }
        """
        let options = try JSONDecoder().decode(
            TaskContextOptionsEnvelope.self,
            from: Data(optionsJSON.utf8)
        )

        XCTAssertEqual(options.datasetKind, "measurement_set")
        XCTAssertEqual(options.spectralWindows, ["spw 0: 4 chan, 1.420000 GHz center"])
        XCTAssertEqual(options.defaults["spectral_window"], options.spectralWindows.first)
        XCTAssertEqual(options.dataColumns, ["DATA"])
    }

    func testTaskUISchemaLoadsFromFrontendServices() throws {
        let schema = try UniFFITaskUISchemaClient().loadTaskUISchema(taskID: "flagdata")

        XCTAssertEqual(schema.commandID, "flagdata")
        XCTAssertTrue(schema.arguments.contains { argument in
            argument.id == "mode" && argument.parser.choices?.contains("summary") == true
        })

        let applycalSchema = try UniFFITaskUISchemaClient().loadTaskUISchema(taskID: "applycal")
        XCTAssertEqual(applycalSchema.commandID, "applycal")
        XCTAssertFalse(applycalSchema.arguments.contains { $0.id == "mode" })
        let applycalBundle = try UniFFISurfaceParameterClient().loadBundle(surfaceID: "applycal")
        XCTAssertEqual(applycalBundle.surface.execution.fixedArgs, ["--mode", "apply"])

        let gencalSchema = try UniFFITaskUISchemaClient().loadTaskUISchema(taskID: "gencal")
        XCTAssertTrue(gencalSchema.arguments.contains { argument in
            argument.id == "caltype"
                && argument.conceptID == "parameter.caltype"
                && argument.parser.choices?.contains("opac") == true
        })
    }

    func testImagerTaskSchemaExposesTutorialControlsAndManagedOutput() throws {
        let schema = try UniFFITaskUISchemaClient().loadTaskUISchema(taskID: "imager")
        let argumentIDs = Set(schema.arguments.filter { !$0.hiddenInTUI }.map(\.id))
        let tutorialArguments = [
            "vis", "imagename", "imsize", "cell", "field", "phasecenter_field",
            "spw", "datacolumn", "specmode", "channel_count", "start", "width",
            "outframe", "restfreq", "deconvolver", "weighting", "robust",
            "gridder", "standard_mfs_acceleration", "perchanweightdensity",
            "restoringbeam", "niter", "nmajor", "gain",
            "threshold", "usemask", "noisethreshold", "sidelobethreshold",
            "lownoisethreshold", "minbeamfrac", "negativethreshold",
            "deconvolver", "scales", "smallscalebias", "wterm", "wprojplanes",
            "nterms", "savemodel", "outlierfile", "write_pb", "pbcor", "pblimit"
        ]

        for argumentID in tutorialArguments {
            XCTAssertTrue(argumentIDs.contains(argumentID), "missing \(argumentID)")
        }
        XCTAssertEqual(schema.managedOutput?.renderer, "imager-run-v1")
        XCTAssertEqual(schema.managedOutput?.injectArguments.first?.flag, "--managed-output")
        XCTAssertEqual(schema.managedOutput?.injectArguments.first?.value, "true")
    }

    func testGenericTaskArgumentsUseRequiredCanonicalProviderInvocation() throws {
        let invocation = SurfaceProviderInvocation(
            args: ["--vis", "/data/input.ms", "--mode", "summary", "--no-flagbackup"]
        )
        let request = GenericTaskRequest(
            runID: "run-1",
            task: makeTaskCatalogEntry(id: "flagdata", displayName: "Flag Data"),
            providerInvocation: invocation
        )

        XCTAssertEqual(try ProcessGenericTaskClient.arguments(for: request), invocation.args)
    }

    func testGenericImagerArgumentsOnlyAppendRuntimeControlsToCanonicalInvocation() throws {
        let providerArguments = [
            "--vis", "/data/input.ms",
            "--specmode", "cube",
            "--perchanweightdensity"
        ]
        let request = GenericTaskRequest(
            runID: "run-cube",
            task: makeImagerTaskCatalogEntry(),
            providerInvocation: SurfaceProviderInvocation(args: providerArguments)
        )

        let arguments = try ProcessGenericTaskClient.arguments(for: request)
        XCTAssertEqual(Array(arguments.prefix(providerArguments.count)), providerArguments)
        XCTAssertFalse(arguments.contains("--no-perchanweightdensity"))
        XCTAssertEqual(
            Array(arguments.suffix(6)),
            ["--progress", "true", "--progress-max-uv-points", "16384", "--progress-min-interval-ms", "250"]
        )
    }

    func testGenericTaskArgumentsPreserveCatalogProjectedHiddenDefaults() throws {
        let invocation = SurfaceProviderInvocation(
            args: ["--mode", "apply", "--ms", "/data/input.ms"]
        )
        let request = GenericTaskRequest(
            runID: "run-1",
            task: makeTaskCatalogEntry(id: "applycal", displayName: "Applycal"),
            providerInvocation: invocation,
            parameterBundle: try UniFFISurfaceParameterClient().loadBundle(surfaceID: "applycal"),
            parameterValues: ["measurement_set": .string("/data/input.ms")]
        )

        XCTAssertEqual(try ProcessGenericTaskClient.arguments(for: request), invocation.args)
    }

    func testGenericTaskArgumentsPreserveCatalogProjectedFixedArguments() throws {
        let invocation = SurfaceProviderInvocation(
            args: ["imhead", "/data/image.im", "--mode", "list"]
        )
        let request = GenericTaskRequest(
            runID: "run-1",
            task: makeImheadTaskCatalogEntry(),
            providerInvocation: invocation,
            parameterBundle: try UniFFISurfaceParameterClient().loadBundle(surfaceID: "imhead"),
            parameterValues: [
                "imagename": .string("/data/image.im"),
                "mode": .string("list")
            ]
        )

        XCTAssertEqual(try ProcessGenericTaskClient.arguments(for: request), invocation.args)
    }

    func testGenericTaskCreatesParentDirectoriesForOutputPaths() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-output-parent-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let outputDirectory = rootURL.appendingPathComponent("casa-rs-runs", isDirectory: true)
        let request = GenericTaskRequest(
            runID: "run-1",
            task: TaskCatalogEntry(
                id: "exportfits",
                category: "Images",
                displayName: "Export FITS",
                binaryName: "exportfits",
                cargoPackage: "casa-images",
                overrideEnv: "CASARS_EXPORTFITS_BIN",
                shellKind: "workflow",
                interaction: "one_shot",
                browserKind: nil,
                datasetKinds: ["image"],
                schemaSource: "binary",
                showInTUI: true,
                showInSwift: true,
                includeInSuite: true
            ),
            providerInvocation: SurfaceProviderInvocation(
                args: ["twhya_cont.image", "casa-rs-runs/twhya_cont.fits", "--overwrite"]
            ),
            parameterBundle: try UniFFISurfaceParameterClient().loadBundle(surfaceID: "exportfits"),
            parameterValues: [
                "imagename": .string("twhya_cont.image"),
                "fitsimage": .string("casa-rs-runs/twhya_cont.fits"),
                "overwrite": .bool(true)
            ],
            workingDirectoryPath: rootURL.path
        )

        XCTAssertFalse(FileManager.default.fileExists(atPath: outputDirectory.path))

        try ProcessGenericTaskClient.createOutputParentDirectories(for: request)

        var isDirectory = ObjCBool(false)
        XCTAssertTrue(FileManager.default.fileExists(atPath: outputDirectory.path, isDirectory: &isDirectory))
        XCTAssertTrue(isDirectory.boolValue)
        XCTAssertEqual(
            ProcessGenericTaskClient.outputArgumentPaths(for: request),
            ["casa-rs-runs/twhya_cont.fits"]
        )
    }

    func testGenericJsonTaskRequiresExplicitSaveToRunFile() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-generic-json-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }

        let imageURL = rootURL.appendingPathComponent("twhya_cont.image", isDirectory: true)
        let taskClient = HoldingGenericTaskClient()
        taskClient.stdout = #"{"shape":[250,250,1,1],"units":"Jy/beam"}"#
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(
            name: "Tutorial",
            rootPath: rootURL.path,
            datasets: [
                DatasetSummary(
                    id: imageURL.path,
                    name: "twhya_cont.image",
                    path: imageURL.path,
                    kind: .imageCube,
                    size: "250 x 250 x 1 x 1",
                    units: "Jy/beam",
                    shape: [250, 250, 1, 1],
                    notes: "test image"
                )
            ],
            source: .probed
        )
        state.selectedDatasetID = imageURL.path
        state.taskCatalog = [makeImheadTaskCatalogEntry()]
        state.activeTaskID = "imhead"

        let store = WorkbenchStore(
            state: state,
            genericTaskClient: taskClient,
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImheadTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.loadTaskUISchemaIfNeeded("imhead")
        store.setGenericTaskValue(taskID: "imhead", argumentID: "imagename", value: imageURL.path)
        store.setGenericTaskValue(taskID: "imhead", argumentID: "mode", value: "summary")
        store.runTask()
        try taskClient.emitSucceeded()
        waitFor("generic JSON task output to finish") {
            store.state.taskRun.state == .succeeded
        }

        XCTAssertTrue(store.hasSaveableActiveTaskOutput())
        XCTAssertEqual(store.taskOutputSaveDirectory(), rootURL.path)
        XCTAssertEqual(store.taskOutputSaveFilename(), "imhead-result.json")
        XCTAssertTrue(store.state.taskRun.outputPaths.isEmpty)

        let outputPath = rootURL.appendingPathComponent("imhead-result.json").path
        store.saveActiveTaskOutput(to: outputPath)

        XCTAssertTrue(outputPath.hasSuffix("/imhead-result.json"))
        XCTAssertTrue(FileManager.default.fileExists(atPath: outputPath))
        let saved = try String(contentsOfFile: outputPath, encoding: .utf8)
        XCTAssertTrue(saved.contains(#""units":"Jy/beam""#))
        XCTAssertTrue(store.debugSnapshot().taskOutputPaths.contains(outputPath))
        XCTAssertTrue(store.state.history.last?.affectedPaths.contains(outputPath) == true)
    }

    func testSimobserveParameterProfileSavesReopensAndRewritesSparseTOML() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-simobserve-family-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }

        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(name: "Synthetic", rootPath: rootURL.path, datasets: [], source: .probed)
        state.taskCatalog = [makeSimobserveTaskCatalogEntry()]
        state.activeTaskID = "simobserve"
        let store = WorkbenchStore(
            state: state,
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.loadTaskUISchemaIfNeeded("simobserve")
        store.setGenericTaskValue(taskID: "simobserve", argumentID: "request_kind", value: "family")
        store.setGenericTaskValue(taskID: "simobserve", argumentID: "telescope", value: "ALMA")
        store.setGenericTaskValue(taskID: "simobserve", argumentID: "pointing_count", value: "3")

        let profileURL = rootURL.appendingPathComponent("profiles/simobserve.toml")
        store.saveActiveParameterProfile(to: profileURL.path)

        let saved = try String(contentsOf: profileURL, encoding: .utf8)
        XCTAssertTrue(saved.contains("surface = \"simobserve\""))
        XCTAssertTrue(saved.contains("kind = \"task\""))
        XCTAssertTrue(saved.contains("request_kind = \"family\""))
        XCTAssertTrue(saved.contains("telescope = \"ALMA\""))
        XCTAssertTrue(saved.contains("pointing_count = 3"))
        XCTAssertFalse(saved.contains("measure_actual_size"), "default-valued parameters stay sparse")

        store.setGenericTaskValue(taskID: "simobserve", argumentID: "pointing_count", value: "99")
        store.loadActiveParameterProfile(from: profileURL.path, discardEdits: true)

        XCTAssertEqual(store.state.genericTaskValues["simobserve"]?["pointing_count"], "3")
        XCTAssertEqual(store.parameterOrigin(surfaceID: "simobserve", name: "pointing_count"), "base_profile")
        XCTAssertEqual(store.parameterSession(surfaceID: "simobserve")?.selectedSource, .file)

        store.setGenericTaskValue(taskID: "simobserve", argumentID: "pointing_count", value: "5")
        store.saveActiveParameterProfile(to: profileURL.path)
        let edited = try String(contentsOf: profileURL, encoding: .utf8)
        XCTAssertTrue(edited.contains("pointing_count = 5"))
    }

    func testParameterSourceReplacementRequiresConfirmationAndReappliesDatasetContext() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-parameter-source-\(UUID().uuidString)", isDirectory: true)
        let imageURL = rootURL.appendingPathComponent("input.image", isDirectory: true)
        let image = DatasetSummary(
            id: imageURL.path,
            name: imageURL.lastPathComponent,
            path: imageURL.path,
            kind: .imageCube,
            size: "4 x 4",
            units: "Jy/beam",
            shape: [4, 4],
            notes: "test image"
        )
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(name: "Project", rootPath: rootURL.path, datasets: [image], source: .probed)
        state.selectedDatasetID = image.id
        state.taskCatalog = [makeImheadTaskCatalogEntry()]
        state.activeTaskID = "imhead"
        let store = WorkbenchStore(
            state: state,
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImheadTaskUISchema()),
            surfaceParameterClient: RecordingSurfaceParameterClient(),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.loadTaskUISchemaIfNeeded("imhead")
        store.setGenericTaskValue(taskID: "imhead", argumentID: "mode", value: "list")
        store.selectParameterSource(.defaults, surfaceID: "imhead")

        XCTAssertEqual(store.parameterText(surfaceID: "imhead", name: "mode"), "list")
        XCTAssertTrue(store.state.lastErrors.contains { $0.contains("only after confirming") })

        store.selectParameterSource(.defaults, surfaceID: "imhead", discardEdits: true)

        XCTAssertEqual(store.parameterText(surfaceID: "imhead", name: "mode"), "summary")
        XCTAssertEqual(store.parameterText(surfaceID: "imhead", name: "imagename"), "input.image")
        XCTAssertEqual(store.parameterSession(surfaceID: "imhead")?.selectedSource, .defaults)
    }

    func testRejectedGenericParameterDraftBlocksRunUntilSuccessfulRecovery() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-rejected-parameter-draft-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }

        let imageURL = rootURL.appendingPathComponent("input.image", isDirectory: true)
        let taskClient = HoldingGenericTaskClient()
        let parameterClient = RecordingSurfaceParameterClient()
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(name: "Project", rootPath: rootURL.path, datasets: [], source: .probed)
        state.taskCatalog = [makeImheadTaskCatalogEntry()]
        state.activeTaskID = "imhead"
        let store = WorkbenchStore(
            state: state,
            genericTaskClient: taskClient,
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImheadTaskUISchema()),
            surfaceParameterClient: parameterClient,
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.loadTaskUISchemaIfNeeded("imhead")
        store.setGenericTaskValue(taskID: "imhead", argumentID: "imagename", value: imageURL.path)
        store.setGenericTaskValue(taskID: "imhead", argumentID: "mode", value: "list")
        XCTAssertFalse(try XCTUnwrap(store.parameterSession(surfaceID: "imhead")).hasErrors)

        parameterClient.resolveFailure = { _, override in
            guard override.values["mode"] == .string("not-a-mode") else { return nil }
            return NSError(
                domain: "SurfaceParameterResolution",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "rejected mode draft"]
            )
        }
        store.setGenericTaskValue(taskID: "imhead", argumentID: "mode", value: "not-a-mode")

        let rejected = try XCTUnwrap(store.parameterSession(surfaceID: "imhead"))
        XCTAssertEqual(store.parameterText(surfaceID: "imhead", name: "mode"), "not-a-mode")
        XCTAssertEqual(rejected.snapshot.states["mode"]?.value, .string("list"))
        XCTAssertTrue(rejected.hasErrors)
        XCTAssertTrue(rejected.snapshot.dirty)
        XCTAssertTrue(rejected.snapshot.diagnostics.contains {
            $0.level == "error"
                && $0.code == "draft_resolution_failed"
                && $0.parameter == "mode"
                && $0.message.contains("rejected mode draft")
        })
        XCTAssertFalse(store.hasSaveableActiveParameterProfile())

        store.runTask()

        XCTAssertTrue(taskClient.requests.isEmpty)
        XCTAssertEqual(store.state.taskRun.state, .failed)
        XCTAssertTrue(store.state.taskRun.diagnostics.contains { $0.contains("rejected mode draft") })

        parameterClient.resolveFailure = nil
        store.setGenericTaskValue(taskID: "imhead", argumentID: "mode", value: "summary")

        let recovered = try XCTUnwrap(store.parameterSession(surfaceID: "imhead"))
        XCTAssertFalse(recovered.hasErrors)
        XCTAssertFalse(recovered.snapshot.diagnostics.contains { $0.code == "draft_resolution_failed" })
        XCTAssertEqual(store.parameterText(surfaceID: "imhead", name: "mode"), "summary")
        XCTAssertEqual(recovered.snapshot.states["mode"]?.value, .string("summary"))
        XCTAssertTrue(store.hasSaveableActiveParameterProfile())

        store.runTask()

        XCTAssertEqual(taskClient.requests.count, 1)
    }

    func testRejectedDatasetContextBlocksRunUntilContextResolves() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-rejected-parameter-context-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }

        let imageURL = rootURL.appendingPathComponent("input.image", isDirectory: true)
        let image = DatasetSummary(
            id: imageURL.path,
            name: imageURL.lastPathComponent,
            path: imageURL.path,
            kind: .imageCube,
            size: "4 x 4",
            units: "Jy/beam",
            shape: [4, 4],
            notes: "test image"
        )
        let taskClient = HoldingGenericTaskClient()
        let parameterClient = RecordingSurfaceParameterClient()
        parameterClient.resolveFailure = { context, _ in
            guard context.values["imagename"] != nil else { return nil }
            return NSError(
                domain: "SurfaceParameterResolution",
                code: 2,
                userInfo: [NSLocalizedDescriptionKey: "rejected dataset context"]
            )
        }
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(
            name: "Project",
            rootPath: rootURL.path,
            datasets: [image],
            source: .probed
        )
        state.selectedDatasetID = image.id
        state.taskCatalog = [makeImheadTaskCatalogEntry()]
        state.activeTaskID = "imhead"
        let store = WorkbenchStore(
            state: state,
            genericTaskClient: taskClient,
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImheadTaskUISchema()),
            surfaceParameterClient: parameterClient,
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.loadTaskUISchemaIfNeeded("imhead")

        let rejected = try XCTUnwrap(store.parameterSession(surfaceID: "imhead"))
        XCTAssertEqual(rejected.contextPatch.values["imagename"], .string("input.image"))
        XCTAssertNil(rejected.snapshot.states["imagename"]?.value)
        XCTAssertTrue(rejected.hasErrors)
        XCTAssertTrue(rejected.snapshot.diagnostics.contains {
            $0.level == "error"
                && $0.code == "draft_resolution_failed"
                && $0.parameter == "imagename"
                && $0.message.contains("rejected dataset context")
        })

        store.runTask()

        XCTAssertTrue(taskClient.requests.isEmpty)
        XCTAssertEqual(store.state.taskRun.state, .failed)
        XCTAssertTrue(store.state.taskRun.diagnostics.contains { $0.contains("rejected dataset context") })

        parameterClient.resolveFailure = nil
        store.loadTaskUISchemaIfNeeded("imhead")

        let recovered = try XCTUnwrap(store.parameterSession(surfaceID: "imhead"))
        XCTAssertFalse(recovered.hasErrors)
        XCTAssertFalse(recovered.snapshot.diagnostics.contains { $0.code == "draft_resolution_failed" })
        XCTAssertEqual(store.parameterText(surfaceID: "imhead", name: "imagename"), "input.image")
        XCTAssertEqual(recovered.snapshot.states["imagename"]?.value, .array([.string("input.image")]))

        store.runTask()

        XCTAssertEqual(taskClient.requests.count, 1)
    }

    func testParameterDraftsAreIndependentPerTaskTab() throws {
        var state = EmptyWorkbench.makeState()
        state.taskCatalog = [makeImheadTaskCatalogEntry()]
        state.activeTaskID = "imhead"
        state.tabs = [
            WorkbenchTab(id: "tab-task-a", title: "A", kind: .task, taskID: "imhead"),
            WorkbenchTab(id: "tab-task-b", title: "B", kind: .task, taskID: "imhead"),
        ]
        state.activeTabID = "tab-task-a"
        let store = WorkbenchStore(
            state: state,
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImheadTaskUISchema()),
            surfaceParameterClient: RecordingSurfaceParameterClient(),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.loadTaskUISchemaIfNeeded("imhead", instanceID: "tab-task-a")
        store.setGenericTaskValue(
            taskID: "imhead",
            instanceID: "tab-task-a",
            argumentID: "mode",
            value: "list"
        )
        store.loadTaskUISchemaIfNeeded("imhead", instanceID: "tab-task-b")
        store.setGenericTaskValue(
            taskID: "imhead",
            instanceID: "tab-task-b",
            argumentID: "mode",
            value: "summary"
        )

        XCTAssertEqual(
            store.parameterText(surfaceID: "imhead", instanceID: "tab-task-a", name: "mode"),
            "list"
        )
        XCTAssertEqual(
            store.parameterText(surfaceID: "imhead", instanceID: "tab-task-b", name: "mode"),
            "summary"
        )
        XCTAssertNotEqual(
            store.parameterSession(surfaceID: "imhead", instanceID: "tab-task-a")?.snapshot,
            store.parameterSession(surfaceID: "imhead", instanceID: "tab-task-b")?.snapshot
        )
    }

    func testSwiftUniFFIResolvesSharedImagerCrossSurfaceProfile() throws {
        let fixtureRoot = repositoryRootURL().appendingPathComponent("resources/test-profiles", isDirectory: true)
        let profile = try String(
            contentsOf: fixtureRoot.appendingPathComponent("imager-cross-surface.toml"),
            encoding: .utf8
        )
        let expectedData = try Data(
            contentsOf: fixtureRoot.appendingPathComponent("imager-cross-surface.expected.json")
        )
        let expectedRoot = try XCTUnwrap(
            JSONSerialization.jsonObject(with: expectedData) as? [String: Any]
        )
        let expectedValues = try XCTUnwrap(expectedRoot["values"] as? [String: Any])
        let snapshot = try UniFFISurfaceParameterClient().load(
            surfaceID: "imager",
            profileTOML: profile,
            sourcePath: fixtureRoot.appendingPathComponent("imager-cross-surface.toml").path
        )

        for name in ["vis", "imagename", "imsize", "cell", "niter"] {
            let actual = try XCTUnwrap(snapshot.states[name]?.value)
            let actualJSON = try JSONSerialization.data(
                withJSONObject: ["value": canonicalJSONObject(actual)],
                options: [.sortedKeys]
            )
            let expectedJSON = try JSONSerialization.data(
                withJSONObject: ["value": try XCTUnwrap(expectedValues[name])],
                options: [.sortedKeys]
            )
            XCTAssertEqual(actualJSON, expectedJSON, "canonical mismatch for \(name)")
        }
    }

    func testSwiftUniFFIUsesCatalogProviderInvocationIncludingStdin() throws {
        let client = UniFFISurfaceParameterClient()
        let invocation = try client.providerInvocation(
            surfaceID: "simobserve",
            values: ["request_kind": .string("family")]
        )

        XCTAssertEqual(invocation.args, ["--json-run", "-"])
        let stdin = try XCTUnwrap(invocation.stdin)
        let request = try XCTUnwrap(
            JSONSerialization.jsonObject(with: Data(stdin.utf8)) as? [String: Any]
        )
        XCTAssertEqual(request["kind"] as? String, "family")
        XCTAssertNotNil(request["request"] as? [String: Any])
    }

    func testSwiftUniFFIRunSafetyIsValueDependent() throws {
        let client = UniFFISurfaceParameterClient()
        let summary = try client.runSafety(
            surfaceID: "flagdata",
            values: ["mode": .string("summary")]
        )
        let manual = try client.runSafety(
            surfaceID: "flagdata",
            values: ["mode": .string("manual")]
        )

        XCTAssertFalse(summary.requiresInteractiveConfirmation)
        XCTAssertTrue(manual.requiresInteractiveConfirmation)
        XCTAssertTrue(manual.requiresInputMutationConfirmation)
        XCTAssertEqual(manual.classes, ["input_mutation"])
    }

    func testTaskLastWritesAttemptThenSuccessAndHonorsNoSaveLast() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-task-last-\(UUID().uuidString)", isDirectory: true)
        let imageURL = rootURL.appendingPathComponent("input.image", isDirectory: true)
        let image = DatasetSummary(
            id: imageURL.path,
            name: imageURL.lastPathComponent,
            path: imageURL.path,
            kind: .imageCube,
            size: "4 x 4",
            units: "Jy/beam",
            shape: [4, 4],
            notes: "test image"
        )
        let taskClient = HoldingGenericTaskClient()
        let parameterClient = RecordingSurfaceParameterClient()
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(name: "Project", rootPath: rootURL.path, datasets: [image], source: .probed)
        state.selectedDatasetID = image.id
        state.taskCatalog = [makeImheadTaskCatalogEntry()]
        state.activeTaskID = "imhead"
        let store = WorkbenchStore(
            state: state,
            genericTaskClient: taskClient,
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImheadTaskUISchema()),
            surfaceParameterClient: parameterClient,
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.loadTaskUISchemaIfNeeded("imhead")
        store.runTask()
        XCTAssertEqual(parameterClient.writes.map(\.successful), [false])
        XCTAssertEqual(
            parameterClient.writes.first?.values["imagename"],
            .array([.string("input.image")])
        )

        store.stopTask()
        XCTAssertEqual(parameterClient.writes.map(\.successful), [false])

        store.runTask()
        XCTAssertEqual(parameterClient.writes.map(\.successful), [false, false])
        try taskClient.emitSucceeded()
        waitFor("Last Successful parameter write") {
            parameterClient.writes.count == 3
        }
        XCTAssertEqual(parameterClient.writes.map(\.successful), [false, false, true])

        store.setParameterSaveLast(surfaceID: "imhead", enabled: false)
        store.runTask()
        XCTAssertEqual(parameterClient.writes.map(\.successful), [false, false, true])
        store.stopTask()
    }

    func testGenericTaskNotebookRecordingTracksSuccessCancellationAndOneRunBypass() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-notebook-task-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }

        let imageURL = rootURL.appendingPathComponent("input.image", isDirectory: true)
        let taskClient = HoldingGenericTaskClient()
        let notebookClient = RecordingNotebookPersistenceClient()
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(
            name: "Project",
            rootPath: rootURL.path,
            datasets: [DatasetSummary(
                id: imageURL.path,
                name: imageURL.lastPathComponent,
                path: imageURL.path,
                kind: .imageCube,
                size: "4 x 4",
                units: "Jy/beam",
                shape: [4, 4],
                notes: "test image"
            )],
            source: .probed
        )
        state.selectedDatasetID = imageURL.path
        state.taskCatalog = [makeImheadTaskCatalogEntry()]
        state.activeTaskID = "imhead"
        let store = WorkbenchStore(
            state: state,
            genericTaskClient: taskClient,
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImheadTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )
        store.installNotebookPersistenceClientForTesting(notebookClient)
        store.loadTaskUISchemaIfNeeded("imhead")

        store.runTask()
        try taskClient.emitSucceeded()
        waitFor("successful notebook receipt") {
            notebookClient.finalizeRequests.last?.finalization.status == "succeeded"
        }
        XCTAssertEqual(notebookClient.beginRequests.first?.request.initiatingSurface, "gui")
        XCTAssertEqual(notebookClient.beginRequests.first?.request.operationId, "imhead")
        XCTAssertEqual(notebookClient.beginRequests.first?.policy, "record")
        let notebookDebug = try XCTUnwrap(store.debugSnapshot().scientificNotebook)
        XCTAssertEqual(notebookDebug.notebookFilenames, ["default.md"])
        XCTAssertEqual(notebookDebug.receiptStatuses.values.sorted(), ["succeeded"])

        store.runTask()
        store.stopTask()
        XCTAssertEqual(notebookClient.finalizeRequests.last?.finalization.status, "cancelled")

        let tabID = store.state.activeTabID.isEmpty ? "tab-task-imhead" : store.state.activeTabID
        store.setNotebookRecordingBypassOnce(tabID: tabID, enabled: true)
        let finalizedCount = notebookClient.finalizeRequests.count
        store.runTask()
        XCTAssertEqual(notebookClient.beginRequests.last?.policy, "bypass_once")
        store.stopTask()
        XCTAssertEqual(notebookClient.finalizeRequests.count, finalizedCount)
        XCTAssertFalse(store.notebookRecordingBypassOnce(tabID: tabID))
    }

    func testNotebookRecordingFailureDoesNotBlockTaskAndRetainsVisibleWarning() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-notebook-warning-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }

        let taskClient = HoldingGenericTaskClient()
        let notebookClient = RecordingNotebookPersistenceClient()
        notebookClient.beginError = NSError(
            domain: "NotebookRecorder",
            code: 17,
            userInfo: [NSLocalizedDescriptionKey: "fixture recorder unavailable"]
        )
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(name: "Project", rootPath: rootURL.path, datasets: [], source: .probed)
        state.taskCatalog = [makeImheadTaskCatalogEntry()]
        state.activeTaskID = "imhead"
        let store = WorkbenchStore(
            state: state,
            genericTaskClient: taskClient,
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImheadTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )
        store.installNotebookPersistenceClientForTesting(notebookClient)
        store.loadTaskUISchemaIfNeeded("imhead")
        store.setGenericTaskValue(taskID: "imhead", argumentID: "imagename", value: "input.image")

        store.runTask()

        XCTAssertEqual(store.state.taskRun.state, .running)
        XCTAssertEqual(taskClient.requests.count, 1)
        XCTAssertTrue(store.state.lastErrors.contains { $0.contains("Notebook recording warning") })
    }

    func testAuthoredNotebookTaskCellLoadsWithoutReceiptAndDirtyReplacementRequiresTypedPreview() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-notebook-replay-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }

        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(name: "Project", rootPath: rootURL.path, datasets: [], source: .probed)
        state.taskCatalog = [makeImheadTaskCatalogEntry()]
        let store = WorkbenchStore(
            state: state,
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImheadTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )
        store.createScientificNotebook(filename: "Analysis.md", title: "Analysis")
        let created = try XCTUnwrap(store.state.scientificNotebooks?.activeNotebook)
        let cellID = UUID().uuidString.lowercased()
        let taskCell = """

        <!-- casa-rs-cell:v1 id=\(cellID) kind=task -->
        ```toml
        [casars]
        format = 1
        surface = "imhead"
        kind = "task"
        contract = 1

        [parameters]
        imagename = ["input.image"]
        mode = "list"
        ```
        <!-- /casa-rs-cell -->
        """
        store.setScientificNotebookDraft(created.source + taskCell)
        store.saveScientificNotebook()

        let saved = try XCTUnwrap(store.state.scientificNotebooks?.activeNotebook)
        XCTAssertTrue(saved.receipts.isEmpty)
        XCTAssertEqual(saved.cells.first?.taskIntent?.parameters["mode"], .string("list"))

        store.openScientificNotebookTask(cellID: cellID)
        let taskTab = try XCTUnwrap(store.state.tabs.first { $0.kind == .task })
        XCTAssertEqual(store.parameterText(surfaceID: "imhead", instanceID: taskTab.id, name: "mode"), "list")
        XCTAssertEqual(store.state.taskRun.state, .idle)

        store.setGenericTaskValue(
            taskID: "imhead",
            instanceID: taskTab.id,
            argumentID: "mode",
            value: "summary"
        )
        XCTAssertEqual(store.parameterSession(surfaceID: "imhead", instanceID: taskTab.id)?.snapshot.dirty, true)

        store.openScientificNotebookTask(cellID: cellID)
        let preview = try XCTUnwrap(store.state.pendingNotebookTaskReplacement)
        XCTAssertEqual(preview.targetTabID, taskTab.id)
        XCTAssertEqual(preview.differences.first { $0.parameter == "mode" }?.currentValue, .string("summary"))
        XCTAssertEqual(preview.differences.first { $0.parameter == "mode" }?.notebookValue, .string("list"))
        XCTAssertEqual(store.parameterText(surfaceID: "imhead", instanceID: taskTab.id, name: "mode"), "summary")

        store.cancelNotebookTaskReplacement()
        XCTAssertNil(store.state.pendingNotebookTaskReplacement)
        XCTAssertEqual(store.parameterText(surfaceID: "imhead", instanceID: taskTab.id, name: "mode"), "summary")

        store.openScientificNotebookTask(cellID: cellID)
        store.confirmNotebookTaskReplacement()
        XCTAssertNil(store.state.pendingNotebookTaskReplacement)
        XCTAssertEqual(store.parameterText(surfaceID: "imhead", instanceID: taskTab.id, name: "mode"), "list")
        XCTAssertEqual(store.state.tabs.filter { $0.kind == .task }.count, 1)

        let currentSource = try XCTUnwrap(store.state.scientificNotebooks?.activeNotebook?.draftSource)
        store.setScientificNotebookDraft(
            currentSource.replacingOccurrences(of: "mode = \"list\"", with: "mode = \"summary\"")
        )
        XCTAssertEqual(
            store.state.scientificNotebooks?.activeNotebook?.cells.first?.taskIntent?.parameters["mode"],
            .string("summary")
        )
        store.openScientificNotebookTask(cellID: cellID)
        XCTAssertNil(store.state.pendingNotebookTaskReplacement)
        XCTAssertEqual(store.parameterText(surfaceID: "imhead", instanceID: taskTab.id, name: "mode"), "summary")
    }

    func testPersistentPythonCellRecordsExactV2ReceiptAndReopensOrderedOutput() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-python-notebook-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let selectedInput = rootURL.appendingPathComponent("selected.ms").path
        let dataset = DatasetSummary(
            id: selectedInput,
            name: "selected.ms",
            path: selectedInput,
            kind: .measurementSet,
            size: "test",
            units: "Jy",
            notes: "selected Python input"
        )
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(
            name: "Project",
            rootPath: rootURL.path,
            datasets: [dataset],
            source: .probed
        )
        state.selectedDatasetID = selectedInput
        let store = WorkbenchStore(state: state)
        store.installPythonExecutableForTesting(try resolvedTestPython())
        store.createScientificNotebook(filename: "Python.md", title: "Python")
        let created = try XCTUnwrap(store.state.scientificNotebooks?.activeNotebook)
        let cellID = UUID().uuidString.lowercased()
        let source = "value = 40 + 2\nprint(value)\n"
        store.setScientificNotebookDraft(created.source + """

        <!-- casa-rs-cell:v1 id=\(cellID) kind=python -->
        ```python
        \(source)```
        <!-- /casa-rs-cell -->
        """)
        store.saveScientificNotebook()
        let savedCell = try XCTUnwrap(store.state.scientificNotebooks?.activeNotebook?.cells.first)
        XCTAssertFalse(savedCell.body.isEmpty, "saved source: \(store.state.scientificNotebooks?.activeNotebook?.source ?? "missing")")

        store.runScientificPythonCell(cellID)
        waitFor("Python receipt", timeout: 10) {
            store.state.scientificNotebooks?.activeNotebook?.receipts.first?.status == "succeeded"
        }
        let receipt = try XCTUnwrap(
            store.state.scientificNotebooks?.activeNotebook?.receipts.first,
            "\(store.state.lastErrors)"
        )
        XCTAssertEqual(receipt.schemaVersion, 2)
        XCTAssertEqual(receipt.executionInput?.kind, "python")
        XCTAssertEqual(receipt.executionInput?.details.source, source)
        XCTAssertEqual(receipt.executionInput?.details.authority, "user")
        XCTAssertEqual(receipt.executionInput?.details.inputReferences, [selectedInput])
        XCTAssertTrue(receipt.executionInput?.details.environment.fingerprintSHA256.isEmpty == false)
        XCTAssertTrue(receipt.orderedOutputs?.map(\.text).joined().contains("42") == true)
        XCTAssertTrue(receipt.artifacts.contains { $0.role == "ordered_output" })

        var reopenedState = EmptyWorkbench.makeState()
        reopenedState.project = state.project
        let reopened = WorkbenchStore(state: reopenedState)
        reopened.loadScientificNotebooks()
        let reopenedReceipt = try XCTUnwrap(reopened.state.scientificNotebooks?.activeNotebook?.receipts.first)
        XCTAssertEqual(reopenedReceipt.executionInput?.details.source, source)
        XCTAssertTrue(reopenedReceipt.orderedOutputs?.map(\.text).joined().contains("42") == true)
    }

    func testMeasurementSetVisualizationNewUpdateAndOpenExplorerRoundTrip() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-visualization-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let msPath = rootURL.appendingPathComponent("tutorial.ms").path
        let dataset = DatasetSummary(
            id: msPath,
            name: "tutorial.ms",
            path: msPath,
            kind: .measurementSet,
            size: "test",
            units: "Jy",
            notes: "tutorial"
        )
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(name: "Project", rootPath: rootURL.path, datasets: [dataset], source: .probed)
        state.selectedDatasetID = dataset.id
        state.measurementSetPlots[dataset.id] = MeasurementSetExplorerPlotState(
            datasetID: dataset.id,
            preset: .amplitudeVsTime,
            selectedField: "0",
            selectedSpectralWindow: "0",
            selectedCorrelation: "XX",
            dataColumn: "DATA",
            status: .ready,
            lastError: nil,
            result: makePlotResult(
                preset: .amplitudeVsTime,
                title: "Amplitude vs time",
                datasetPath: msPath,
                imageBytes: Data()
            )
        )
        let store = WorkbenchStore(state: state)
        store.createScientificNotebook(filename: "Plots.md", title: "Plots")
        let renderedImage = NotebookVisualizationImage(
            data: Data([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
            fileExtension: "png",
            mediaType: "image/png",
            width: 960,
            height: 600,
            renderer: "test Workbench renderer"
        )

        store.saveMeasurementSetPlotToNotebook(datasetID: dataset.id, renderedImage: renderedImage)
        let first = try XCTUnwrap(store.state.scientificNotebooks?.activeNotebook?.visualizations.first)
        XCTAssertEqual(first.revisions.count, 1)
        XCTAssertEqual(first.revisions[0].reopen.parameters["selectedField"], .string("0"))
        XCTAssertTrue(FileManager.default.fileExists(atPath: rootURL
            .appendingPathComponent(first.revisions[0].assetPath).path))

        store.saveMeasurementSetPlotToNotebook(
            datasetID: dataset.id,
            updating: first.id,
            renderedImage: renderedImage
        )
        let updated = try XCTUnwrap(store.state.scientificNotebooks?.activeNotebook?.visualizations.first)
        XCTAssertEqual(updated.id, first.id)
        XCTAssertEqual(updated.revisions.count, 2)
        XCTAssertNotEqual(updated.revisions[0].assetPath, updated.revisions[1].assetPath)

        store.openNotebookVisualization(updated.id)
        XCTAssertEqual(store.state.measurementSetPlots[dataset.id]?.preset, .amplitudeVsTime)
        XCTAssertTrue(store.state.tabs.contains(where: { $0.kind == .datasetExplorer }))
        XCTAssertTrue(store.shouldPresentMeasurementSetPlotSurface(datasetID: dataset.id))
    }

    func testLocalRealMeasurementSetVisualizationSaveDiagnostic() throws {
        guard let msPath = ProcessInfo.processInfo.environment["CASA_RS_WAVE5C_MS"] else {
            throw XCTSkip("Set CASA_RS_WAVE5C_MS to diagnose a real MeasurementSet visualization save.")
        }
        guard FileManager.default.fileExists(atPath: msPath) else {
            throw XCTSkip("\(msPath) is not staged")
        }

        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-real-visualization-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let dataset = DatasetSummary(
            id: msPath,
            name: URL(fileURLWithPath: msPath).lastPathComponent,
            path: msPath,
            kind: .measurementSet,
            size: "diagnostic",
            units: "Jy",
            notes: "real Wave 5C plot"
        )
        let result = try UniFFIMeasurementSetPlotClient().buildPlot(
            request: MeasurementSetPlotBuildRequest(
                datasetPath: msPath,
                preset: .uvCoverage,
                field: nil,
                spectralWindow: nil,
                correlation: nil,
                dataColumn: "DATA"
            )
        )
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(
            name: "Real visualization diagnostic",
            rootPath: rootURL.path,
            datasets: [dataset],
            source: .probed
        )
        state.measurementSetPlots[dataset.id] = MeasurementSetExplorerPlotState(
            datasetID: dataset.id,
            preset: .uvCoverage,
            selectedField: "",
            selectedSpectralWindow: "",
            selectedCorrelation: "",
            dataColumn: "DATA",
            status: .ready,
            lastError: nil,
            result: result
        )
        let store = WorkbenchStore(state: state)
        store.createScientificNotebook(filename: "Plots.md", title: "Plots")

        XCTAssertTrue(result.imageBytes.isEmpty, "the production plot contract is data-first")
        store.saveMeasurementSetPlotToNotebook(
            datasetID: dataset.id,
            renderedImage: NotebookVisualizationImage(
                data: Data([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
                fileExtension: "png",
                mediaType: "image/png",
                width: result.imageWidth,
                height: result.imageHeight,
                renderer: "test Workbench renderer"
            )
        )

        let visualization = try XCTUnwrap(
            store.state.scientificNotebooks?.activeNotebook?.visualizations.first,
            "save errors: \(store.state.lastErrors)"
        )
        XCTAssertEqual(visualization.revisions.count, 1)
        XCTAssertTrue(
            FileManager.default.fileExists(
                atPath: rootURL.appendingPathComponent(visualization.revisions[0].assetPath).path
            )
        )
    }

    func testImageVisualizationSaveCreatesStableNotebookPNG() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-image-visualization-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let imagePath = rootURL.appendingPathComponent("tutorial.image").path
        let dataset = DatasetSummary(
            id: imagePath,
            name: "tutorial.image",
            path: imagePath,
            kind: .imageCube,
            size: "2 x 2",
            units: "Jy/beam",
            shape: [2, 2],
            notes: "tutorial"
        )
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(name: "Project", rootPath: rootURL.path, datasets: [dataset], source: .probed)
        let store = WorkbenchStore(
            state: state,
            imageExplorerClient: StubImageExplorerClient(snapshot: makeImageExplorerSnapshot())
        )
        store.createScientificNotebook(filename: "Images.md", title: "Images")
        store.openDatasetExplorer(dataset.id)
        waitFor("image explorer snapshot") {
            store.state.imageExplorers[dataset.id]?.snapshot?.plane != nil
        }

        store.saveImageExplorerToNotebook(datasetID: dataset.id)
        let saved = try XCTUnwrap(store.state.scientificNotebooks?.activeNotebook?.visualizations.first)
        let revision = try XCTUnwrap(saved.revisions.first)
        XCTAssertEqual(revision.reopen.surface, "imexplore")
        XCTAssertEqual(revision.render.mediaType, "image/png")
        let data = try Data(contentsOf: rootURL.appendingPathComponent(revision.assetPath))
        XCTAssertTrue(data.starts(with: [0x89, 0x50, 0x4e, 0x47]))
    }

    private func resolvedTestPython() throws -> String {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/xcrun")
        process.arguments = ["-f", "python3"]
        let stdout = Pipe()
        process.standardOutput = stdout
        try process.run()
        process.waitUntilExit()
        return String(decoding: stdout.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self)
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }

    func testSessionLastRequiresSuccessfulOpenAndIgnoresTransientNavigation() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-session-last-\(UUID().uuidString)", isDirectory: true)
        let imageURL = rootURL.appendingPathComponent("input.image", isDirectory: true)
        let image = DatasetSummary(
            id: imageURL.path,
            name: imageURL.lastPathComponent,
            path: imageURL.path,
            kind: .imageCube,
            size: "4 x 4 x 8",
            units: "Jy/beam",
            shape: [4, 4, 8],
            notes: "test image"
        )
        let imageClient = StubImageExplorerClient(snapshot: makeImageExplorerSnapshot())
        imageClient.error = NSError(
            domain: "StubImageExplorerClient",
            code: 17,
            userInfo: [NSLocalizedDescriptionKey: "open failed"]
        )
        let parameterClient = RecordingSurfaceParameterClient()
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(name: "Project", rootPath: rootURL.path, datasets: [image], source: .probed)
        state.selectedDatasetID = image.id
        let store = WorkbenchStore(
            state: state,
            imageExplorerClient: imageClient,
            surfaceParameterClient: parameterClient,
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.openDatasetExplorer(image.id)
        store.closeActiveTab()
        XCTAssertTrue(parameterClient.writes.isEmpty, "a failed session open must preserve Last")

        imageClient.error = nil
        store.openDatasetExplorer(image.id)
        waitFor("successful session Last write") {
            parameterClient.writes.count == 1
        }
        XCTAssertEqual(parameterClient.writes.first?.surfaceID, "imexplore")
        XCTAssertEqual(parameterClient.writes.first?.successful, false)

        store.setImageExplorerCursor(x: 1, y: 1, datasetID: image.id)
        runCurrentRunLoop(for: 0.4)
        XCTAssertEqual(parameterClient.writes.count, 1, "cursor navigation is not durable profile state")

        store.setImageExplorerColorMap(.inferno, datasetID: image.id)
        waitFor("accepted durable session update") {
            parameterClient.writes.count == 2
        }
        XCTAssertEqual(parameterClient.writes.last?.values["colormap"], .string("inferno"))

        store.stopImageExplorerMovie(datasetID: image.id)
        runCurrentRunLoop(for: 0.4)
        XCTAssertEqual(parameterClient.writes.count, 2, "playback-running state is transient")
    }

    func testImageExplorerContentModeProfileAppliesAtStartupAndAcceptedLiveUpdatePersistsLast() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-image-contentmode-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let imageURL = rootURL.appendingPathComponent("input.image", isDirectory: true)
        let image = DatasetSummary(
            id: imageURL.path,
            name: imageURL.lastPathComponent,
            path: imageURL.path,
            kind: .imageCube,
            size: "4 x 4 x 8",
            units: "Jy/beam",
            shape: [4, 4, 8],
            notes: "test image"
        )
        let imageClient = StubImageExplorerClient(snapshot: makeImageExplorerSnapshot())
        let parameterClient = RecordingSurfaceParameterClient()
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(
            name: "Project",
            rootPath: rootURL.path,
            datasets: [image],
            source: .probed
        )
        state.selectedDatasetID = image.id
        let store = WorkbenchStore(
            state: state,
            imageExplorerClient: imageClient,
            surfaceParameterClient: parameterClient,
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )
        let profileURL = rootURL.appendingPathComponent("image-spreadsheet.toml")
        try """
        [casars]
        format = 1
        surface = "imexplore"
        kind = "session"
        contract = 1

        [parameters]
        image = "\(image.path)"
        contentmode = "spreadsheet"
        """.write(to: profileURL, atomically: true, encoding: .utf8)

        store.selectParameterSource(
            .file,
            surfaceID: "imexplore",
            instanceID: image.explorerTabID,
            profilePath: profileURL.path,
            discardEdits: true
        )
        store.openDatasetExplorer(image.id)

        XCTAssertEqual(imageClient.requests.first?.planeContentMode, "spreadsheet")
        XCTAssertEqual(store.state.imageExplorers[image.id]?.planeContentMode, "spreadsheet")
        XCTAssertEqual(
            store.parameterSession(surfaceID: "imexplore", instanceID: image.explorerTabID)?
                .snapshot.states["contentmode"]?.value,
            .string("spreadsheet")
        )
        waitFor("startup content mode Last write") {
            parameterClient.writes.last?.values["contentmode"] == .string("spreadsheet")
        }
        let startupWriteCount = parameterClient.writes.count

        store.setImageExplorerPlaneContentMode("raster", datasetID: image.id)

        XCTAssertEqual(imageClient.requests.last?.planeContentMode, "raster")
        XCTAssertEqual(store.state.imageExplorers[image.id]?.planeContentMode, "raster")
        XCTAssertEqual(
            store.parameterSession(surfaceID: "imexplore", instanceID: image.explorerTabID)?
                .snapshot.states["contentmode"]?.value,
            .string("raster")
        )
        waitFor("accepted live content mode Last write") {
            parameterClient.writes.count == startupWriteCount + 1
                && parameterClient.writes.last?.values["contentmode"] == .string("raster")
        }
        let namedProfile = try String(contentsOf: profileURL, encoding: .utf8)
        XCTAssertTrue(namedProfile.contains("contentmode = \"spreadsheet\""))
    }

    func testSharedImexploreProfileRoutesLabeledAxesAndTypedRegionReference() throws {
        let fixtureURL = repositoryRootURL()
            .appendingPathComponent("resources/test-profiles/imexplore-cross-surface.toml")
        let image = DatasetSummary(
            id: "fixtures/science.image",
            name: "science.image",
            path: "fixtures/science.image",
            kind: .imageCube,
            size: "4 x 4 x 8 x 4",
            units: "Jy/beam",
            shape: [4, 4, 8, 4],
            notes: "shared profile image"
        )
        var snapshot = makeImageExplorerSnapshot()
        snapshot.nonDisplayAxes = [
            ImageExplorerSnapshot.NonDisplayAxis(axis: 2, label: "Frequency", index: 0, length: 8, pixel: 0),
            ImageExplorerSnapshot.NonDisplayAxis(axis: 3, label: "Stokes", index: 0, length: 4, pixel: 0),
        ]
        let imageClient = StubImageExplorerClient(snapshot: snapshot)
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(
            name: "Shared profile",
            rootPath: repositoryRootURL().path,
            datasets: [image],
            source: .probed
        )
        state.selectedDatasetID = image.id
        let store = WorkbenchStore(
            state: state,
            imageExplorerClient: imageClient,
            surfaceParameterClient: RecordingSurfaceParameterClient(),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.selectParameterSource(
            .file,
            surfaceID: "imexplore",
            instanceID: image.explorerTabID,
            profilePath: fixtureURL.path,
            discardEdits: true
        )
        store.applySurfaceParameterProfile(
            surfaceID: "imexplore",
            datasetID: image.id,
            instanceID: image.explorerTabID
        )

        XCTAssertEqual(imageClient.requests.count, 2, "a label-selected profile axis requires one metadata resolution pass")
        XCTAssertEqual(imageClient.requests.last?.selectedProfileAxis, 3)
        XCTAssertEqual(store.state.imageExplorers[image.id]?.movieAxis, 2)
        XCTAssertEqual(store.state.imageExplorers[image.id]?.planeContentMode, "spreadsheet")
        XCTAssertEqual(
            imageClient.requests.last?.commands,
            [
                .setSelectionReference(.definition(name: "source")),
                .setDefaultMask(name: "mask0"),
            ]
        )
        XCTAssertEqual(
            store.parameterSession(surfaceID: "imexplore", instanceID: image.explorerTabID)?
                .snapshot.states["profileaxis"]?.value,
            .string("Stokes")
        )
    }

    func testImageExplorerProfileRoutesEveryDeclarativeRegionVariantAndFailsClosed() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-image-region-profile-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let image = DatasetSummary(
            id: rootURL.appendingPathComponent("input.image").path,
            name: "input.image",
            path: rootURL.appendingPathComponent("input.image").path,
            kind: .imageCube,
            size: "4 x 4 x 8",
            units: "Jy/beam",
            shape: [4, 4, 8],
            notes: "region profile image"
        )

        func routedReference(_ value: String, rejectCommands: Bool = false) throws
            -> (ImageExplorerRegionReference?, ExplorerSessionStatus, Int)
        {
            let profileURL = rootURL.appendingPathComponent("region-\(UUID().uuidString).toml")
            try """
            [casars]
            format = 1
            surface = "imexplore"
            kind = "session"
            contract = 2

            [parameters]
            image = "\(image.path)"
            region = "\(value)"
            """.write(to: profileURL, atomically: true, encoding: .utf8)
            let imageClient = StubImageExplorerClient(snapshot: makeImageExplorerSnapshot())
            imageClient.failWhenCommandsAreQueued = rejectCommands
            let parameterClient = RecordingSurfaceParameterClient()
            var state = EmptyWorkbench.makeState()
            state.project = ProjectFixture(name: "Project", rootPath: rootURL.path, datasets: [image], source: .probed)
            let store = WorkbenchStore(
                state: state,
                imageExplorerClient: imageClient,
                surfaceParameterClient: parameterClient,
                taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
            )
            store.selectParameterSource(
                .file,
                surfaceID: "imexplore",
                instanceID: image.explorerTabID,
                profilePath: profileURL.path,
                discardEdits: true
            )
            store.applySurfaceParameterProfile(
                surfaceID: "imexplore",
                datasetID: image.id,
                instanceID: image.explorerTabID
            )
            let reference = imageClient.requests.last?.commands.compactMap(\.region).first
            return (reference, store.state.imageExplorers[image.id]?.status ?? .failed, parameterClient.writes.count)
        }

        XCTAssertEqual(try routedReference("source").0, .definition(name: "source"))
        XCTAssertEqual(try routedReference("file:regions/source.crtf").0, .file(path: "regions/source.crtf"))
        XCTAssertEqual(try routedReference("regions/source.crtf").0, .file(path: "regions/source.crtf"))
        XCTAssertEqual(
            try routedReference("box[[0pix,0pix],[2pix,2pix]]").0,
            .expression(expression: "box[[0pix,0pix],[2pix,2pix]]")
        )
        let rejected = try routedReference("definition:missing", rejectCommands: true)
        XCTAssertEqual(rejected.0, .definition(name: "missing"), "durable profile commands must survive recovery attempts")
        XCTAssertEqual(rejected.1, .failed)
        XCTAssertEqual(rejected.2, 0, "a rejected durable profile must not be accepted or saved as Last")
    }

    func testSessionLastDedupIsScopedByWorkspaceForEqualValues() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-session-workspaces-\(UUID().uuidString)", isDirectory: true)
        let workspaceURLs = ["workspace-a", "workspace-b"].map {
            rootURL.appendingPathComponent($0, isDirectory: true)
        }
        for workspaceURL in workspaceURLs {
            try FileManager.default.createDirectory(at: workspaceURL, withIntermediateDirectories: true)
        }
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let images = ["first.image", "second.image"].map { name in
            DatasetSummary(
                id: rootURL.appendingPathComponent(name, isDirectory: true).path,
                name: name,
                path: rootURL.appendingPathComponent(name, isDirectory: true).path,
                kind: .imageCube,
                size: "4 x 4",
                units: "Jy/beam",
                shape: [4, 4],
                notes: "test image"
            )
        }
        let parameterClient = RecordingSurfaceParameterClient()
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(name: "Project", rootPath: rootURL.path, datasets: images, source: .probed)
        let store = WorkbenchStore(
            state: state,
            imageExplorerClient: StubImageExplorerClient(snapshot: makeImageExplorerSnapshot()),
            surfaceParameterClient: parameterClient
        )

        for (image, workspaceURL) in zip(images, workspaceURLs) {
            store.openDatasetExplorer(image.id)
            store.setParameterWorkspace(
                surfaceID: "imexplore",
                instanceID: image.explorerTabID,
                path: workspaceURL.path
            )
            store.setGenericTaskValue(
                taskID: "imexplore",
                instanceID: image.explorerTabID,
                argumentID: "image",
                value: "shared.image"
            )
            store.setImageExplorerColorMap(.inferno, datasetID: image.id)
            store.closeActiveTab()
        }

        XCTAssertEqual(parameterClient.writes.map(\.workspace), workspaceURLs.map(\.path))
        XCTAssertEqual(parameterClient.writes.count, 2)
        XCTAssertEqual(parameterClient.writes[0].values, parameterClient.writes[1].values)
        XCTAssertEqual(parameterClient.writes[0].values["image"], .string("shared.image"))
        XCTAssertEqual(parameterClient.writes[0].values["colormap"], .string("inferno"))
    }

    func testSameNormalizedWorkspaceStaleSessionCannotOverwriteNewerLast() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-session-order-\(UUID().uuidString)", isDirectory: true)
        let nestedURL = rootURL.appendingPathComponent("nested", isDirectory: true)
        try FileManager.default.createDirectory(at: nestedURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let images = ["first.image", "second.image"].map { name in
            DatasetSummary(
                id: rootURL.appendingPathComponent(name, isDirectory: true).path,
                name: name,
                path: rootURL.appendingPathComponent(name, isDirectory: true).path,
                kind: .imageCube,
                size: "4 x 4",
                units: "Jy/beam",
                shape: [4, 4],
                notes: "test image"
            )
        }
        let parameterClient = RecordingSurfaceParameterClient()
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(name: "Project", rootPath: rootURL.path, datasets: images, source: .probed)
        let store = WorkbenchStore(
            state: state,
            imageExplorerClient: StubImageExplorerClient(snapshot: makeImageExplorerSnapshot()),
            surfaceParameterClient: parameterClient
        )

        store.openDatasetExplorer(images[0].id)
        store.setParameterWorkspace(
            surfaceID: "imexplore",
            instanceID: images[0].explorerTabID,
            path: "\(nestedURL.path)/.."
        )
        store.openDatasetExplorer(images[1].id)
        store.setParameterWorkspace(
            surfaceID: "imexplore",
            instanceID: images[1].explorerTabID,
            path: rootURL.path
        )
        store.setImageExplorerColorMap(.inferno, datasetID: images[1].id)
        store.closeActiveTab()

        XCTAssertEqual(parameterClient.writes.count, 1)
        XCTAssertEqual(parameterClient.writes.last?.workspace, rootURL.path)
        XCTAssertEqual(parameterClient.writes.last?.values["colormap"], .string("inferno"))
        runCurrentRunLoop(for: 0.4)

        XCTAssertEqual(parameterClient.writes.count, 1)
        XCTAssertEqual(parameterClient.writes.last?.values["colormap"], .string("inferno"))
        store.closeActiveTab()
        XCTAssertEqual(parameterClient.writes.count, 1)
    }

    func testTableBrowserProfileAppliesBookmarkAndPresentationBeforeUpdatingLast() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-table-profile-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let table = DatasetSummary(
            id: rootURL.appendingPathComponent("MAIN").path,
            name: "MAIN",
            path: rootURL.appendingPathComponent("MAIN").path,
            kind: .table,
            size: "12 rows",
            units: "casacore table",
            columns: ["TIME", "DATA"],
            subtables: ["ANTENNA"],
            shape: [12],
            notes: "test table"
        )
        let parameterClient = RecordingSurfaceParameterClient()
        let tableClient = StubTableBrowserClient(snapshot: makeTableBrowserSnapshot(path: table.path))
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            probeClient: StubProjectProbeClient(result: ProjectFixtureProbe(
                project: ProjectFixture(name: "Project", rootPath: rootURL.path, datasets: [table], source: .probed),
                diagnostics: []
            )),
            tableBrowserClient: tableClient,
            surfaceParameterClient: parameterClient
        )
        store.openProject(path: rootURL.path)
        runCurrentRunLoop(for: 0.4)
        let initialWrites = parameterClient.writes.count
        let instanceID = table.explorerTabID

        let goodProfileURL = rootURL.appendingPathComponent("table-good.toml")
        try """
        [casars]
        format = 1
        surface = "tablebrowser"
        kind = "session"
        contract = 1

        [parameters]
        table = "\(table.path)"
        view = "rows"
        bookmark = "cell:2:DATA"
        nrow = 7
        contentmode = "detailed"
        """.write(to: goodProfileURL, atomically: true, encoding: .utf8)
        store.selectParameterSource(
            .file,
            surfaceID: "tablebrowser",
            instanceID: instanceID,
            profilePath: goodProfileURL.path,
            discardEdits: true
        )
        store.applySurfaceParameterProfile(
            surfaceID: "tablebrowser",
            datasetID: table.id,
            instanceID: instanceID
        )

        let applied = try XCTUnwrap(store.state.tableBrowsers[table.id])
        XCTAssertEqual(applied.contentMode, "detailed")
        XCTAssertEqual(applied.bookmark, "cell:2:DATA")
        XCTAssertEqual(applied.cellWindowRowLimit, 7)
        XCTAssertEqual(applied.snapshot?.selectedAddress?.row, 2)
        XCTAssertEqual(applied.snapshot?.selectedAddress?.column, "DATA")
        waitFor("accepted table profile Last write") {
            parameterClient.writes.count == initialWrites + 1
        }

        let failedProfileURL = rootURL.appendingPathComponent("table-failed.toml")
        try """
        [casars]
        format = 1
        surface = "tablebrowser"
        kind = "session"
        contract = 1

        [parameters]
        table = "\(table.path)"
        bookmark = "invalid-bookmark"
        """.write(to: failedProfileURL, atomically: true, encoding: .utf8)
        store.selectParameterSource(
            .file,
            surfaceID: "tablebrowser",
            instanceID: instanceID,
            profilePath: failedProfileURL.path,
            discardEdits: true
        )
        store.applySurfaceParameterProfile(
            surfaceID: "tablebrowser",
            datasetID: table.id,
            instanceID: instanceID
        )
        runCurrentRunLoop(for: 0.4)

        XCTAssertEqual(store.state.tableBrowsers[table.id]?.status, .failed)
        XCTAssertEqual(
            parameterClient.writes.count,
            initialWrites + 1,
            "failed backend profile application must preserve managed Last"
        )
    }

    func testSharedTableBrowserProfileUsesOneTypedConfigureCommandWithSlashBookmark() throws {
        let fixtureURL = repositoryRootURL()
            .appendingPathComponent("resources/test-profiles/tablebrowser-cross-surface.toml")
        let table = DatasetSummary(
            id: "fixtures/MAIN",
            name: "MAIN",
            path: "fixtures/MAIN",
            kind: .table,
            size: "100 rows",
            units: "casacore table",
            columns: ["TIME", "DATA"],
            subtables: ["ANTENNA"],
            shape: [100],
            notes: "shared profile table"
        )
        let tableClient = StubTableBrowserClient(snapshot: makeTableBrowserSnapshot(path: table.path))
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(
            name: "Shared profile",
            rootPath: repositoryRootURL().path,
            datasets: [table],
            source: .probed
        )
        state.tabs = [WorkbenchTab(id: table.explorerTabID, title: "Table", kind: .tableBrowser, datasetID: table.id)]
        state.activeTabID = table.explorerTabID
        let store = WorkbenchStore(
            state: state,
            tableBrowserClient: tableClient,
            surfaceParameterClient: RecordingSurfaceParameterClient(),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.selectParameterSource(
            .file,
            surfaceID: "tablebrowser",
            instanceID: table.explorerTabID,
            profilePath: fixtureURL.path,
            discardEdits: true
        )
        store.applySurfaceParameterProfile(
            surfaceID: "tablebrowser",
            datasetID: table.id,
            instanceID: table.explorerTabID
        )

        let request = try XCTUnwrap(tableClient.requests.last?.request)
        XCTAssertEqual(request.commands.count, 1)
        guard case .configure(let parameters) = try XCTUnwrap(request.commands.first) else {
            return XCTFail("table startup must use exactly one configure command")
        }
        XCTAssertEqual(parameters.view, "keywords")
        XCTAssertEqual(parameters.rowStart, 4)
        XCTAssertEqual(parameters.rowCount, 17)
        XCTAssertEqual(parameters.linkedTable, "ANTENNA")
        XCTAssertEqual(parameters.bookmark, .tableKeyword(path: ["OBSERVATION", "TIME_RANGE"]))
        XCTAssertEqual(parameters.contentMode, "detailed")
        let encoded = try JSONSerialization.jsonObject(with: JSONEncoder().encode(request)) as? [String: Any]
        let commands = try XCTUnwrap(encoded?["commands"] as? [[String: Any]])
        XCTAssertEqual(commands.first?["command"] as? String, "configure")
    }

    func testMeasurementSetExplorerUsesCanonicalMsexploreDraft() throws {
        let measurementSet = DatasetSummary(
            id: "/data/input.ms",
            name: "input.ms",
            path: "/data/input.ms",
            kind: .measurementSet,
            size: "4 rows",
            units: "Jy",
            dataColumns: ["DATA"],
            notes: "test MeasurementSet"
        )
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            probeClient: StubProjectProbeClient(result: ProjectFixtureProbe(
                project: ProjectFixture(name: "Project", rootPath: "/data", datasets: [measurementSet], source: .probed),
                diagnostics: []
            ))
        )

        store.openProject(path: "/data")
        let session = try XCTUnwrap(store.parameterSession(
            surfaceID: "msexplore",
            instanceID: measurementSet.explorerTabID
        ))
        XCTAssertEqual(session.snapshot.states["vis"]?.value, .array([.string(measurementSet.path)]))
        XCTAssertEqual(store.state.measurementSetPlots[measurementSet.id]?.maxPlotPoints, 10_000_000)

        store.setMeasurementSetPlotPreset(.amplitudeVsFrequency, datasetID: measurementSet.id)
        XCTAssertEqual(
            store.parameterSession(surfaceID: "msexplore", instanceID: measurementSet.explorerTabID)?
                .snapshot.states["preset"]?.value,
            .string("amplitude_vs_frequency")
        )
    }

    func testProcessGenericTaskRunsSimobserveThroughCanonicalProviderInvocation() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-simobserve-run-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let binaryURL = try writeStubSimobserveBinary(
            rootURL: rootURL,
            script: """
            #!/bin/sh
            printf '%s\n' "$@"
            cat
            exit 0
            """
        )
        setenv("CASARS_SIMOBSERVE_BIN", binaryURL.path, 1)
        defer { unsetenv("CASARS_SIMOBSERVE_BIN") }

        let request = try makeSimobserveGenericTaskRequest(rootURL: rootURL)
        let client = ProcessGenericTaskClient(queue: DispatchQueue(label: "test.simobserve.family"))
        let semaphore = DispatchSemaphore(value: 0)
        var event: GenericTaskEvent?
        _ = try client.startTask(request: request) {
            event = $0
            semaphore.signal()
        }
        XCTAssertEqual(semaphore.wait(timeout: .now() + 5), .success)

        guard case let .succeeded(result) = event else {
            XCTFail("expected simobserve success")
            return
        }
        XCTAssertEqual(result.arguments, ["--json-run", "-"])
        XCTAssertTrue(result.stdout.contains("model.image"))
        XCTAssertTrue(result.stdout.contains("products/family.ms"))
    }

    func testProcessGenericTaskSurfacesSimobserveValidationFailure() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-simobserve-fail-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let binaryURL = try writeStubSimobserveBinary(
            rootURL: rootURL,
            script: """
            #!/bin/sh
            echo "Error: target_ms_size_gib must be positive" >&2
            exit 1
            """
        )
        setenv("CASARS_SIMOBSERVE_BIN", binaryURL.path, 1)
        defer { unsetenv("CASARS_SIMOBSERVE_BIN") }

        let request = try makeSimobserveGenericTaskRequest(rootURL: rootURL)
        let client = ProcessGenericTaskClient(queue: DispatchQueue(label: "test.simobserve.family.failure"))
        let semaphore = DispatchSemaphore(value: 0)
        var event: GenericTaskEvent?
        _ = try client.startTask(request: request) {
            event = $0
            semaphore.signal()
        }
        XCTAssertEqual(semaphore.wait(timeout: .now() + 5), .success)

        guard case let .failed(failure) = event else {
            XCTFail("expected simobserve family failure")
            return
        }
        XCTAssertEqual(failure.message, "simobserve exited with 1.")
        XCTAssertTrue(failure.diagnostics.joined(separator: "\n").contains("target_ms_size_gib must be positive"))
    }

    func testGenericTaskRequestSummaryDisplaysProjectRelativePaths() throws {
        let schema = try JSONDecoder().decode(TaskUISchema.self, from: Data("""
        {
          "schema_version": 1,
          "command_id": "flagdata",
          "invocation_name": "flagdata",
          "display_name": "Flag Data",
          "category": "Flagging",
          "summary": "Run native CASA-style MeasurementSet flagging.",
          "usage": "flagdata",
          "arguments": [
            {"id":"vis","label":"MeasurementSet","order":0,"parser":{"kind":"option","flags":["--vis"],"metavar":"MS","choices":[]},"value_kind":"path","parameter_type":"measurement_set_path","required":true,"default":"/data/project/input.ms","help":"","group":"Input","advanced":false,"hidden_in_tui":false},
            {"id":"mode","label":"Mode","order":1,"parser":{"kind":"option","flags":["--mode"],"metavar":"MODE","choices":["summary","manual"]},"value_kind":"choice","required":true,"default":"summary","help":"","group":"Flagging","advanced":false,"hidden_in_tui":false}
          ]
        }
        """.utf8))
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(
            name: "project",
            rootPath: "/data/project",
            datasets: [],
            source: .probed
        )
        state.activeTaskID = "flagdata"
        let store = WorkbenchStore(
            state: state,
            taskUISchemaClient: StubTaskUISchemaClient(schema: schema)
        )

        store.loadTaskUISchemaIfNeeded("flagdata")
        store.setGenericTaskValue(taskID: "flagdata", argumentID: "vis", value: "/data/project/input.ms")

        XCTAssertEqual(store.state.genericTaskValues["flagdata"]?["vis"], "input.ms")
        let summary = try XCTUnwrap(store.state.taskRun.requestSummary)
        XCTAssertTrue(summary.contains("vis=input.ms"))
        XCTAssertFalse(summary.contains("/data/project/input.ms"))
    }

    func testGenericTaskRegistersRelativeOutputProductUnderProjectRoot() throws {
        let schema = try JSONDecoder().decode(TaskUISchema.self, from: Data("""
        {
          "schema_version": 1,
          "command_id": "immoments",
          "invocation_name": "immoments",
          "display_name": "Image Moments",
          "category": "Images",
          "summary": "Create CASA-style image moment maps.",
          "usage": "immoments <imagename> --outfile <path>",
          "arguments": [
            {"id":"imagename","label":"Image","order":0,"parser":{"kind":"positional","metavar":"imagename"},"value_kind":"path","required":true,"default":null,"help":"","group":"Input","advanced":false,"hidden_in_tui":false},
            {"id":"outfile","label":"Output","order":1,"parser":{"kind":"option","flags":["--outfile"],"metavar":"path","choices":[]},"value_kind":"path","required":true,"default":null,"help":"","group":"Output","advanced":false,"hidden_in_tui":false}
          ]
        }
        """.utf8))
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-generic-product-\(UUID().uuidString)", isDirectory: true)
        let outputURL = rootURL.appendingPathComponent(".casa-rs/workspace/native/mom0.image", isDirectory: true)
        try FileManager.default.createDirectory(at: outputURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let outputPath = outputURL.standardizedFileURL.path
        let outputDataset = DatasetSummary(
            id: outputPath,
            name: "mom0.image",
            path: outputPath,
            kind: .imageCube,
            size: "250 x 250 x 1 x 1",
            units: "Jy/beam.km/s",
            sizeBytes: 1024,
            notes: "Recognized by Rust probe."
        )
        let probeClient = StubProjectProbeClient(
            result: ProjectFixtureProbe(
                project: ProjectFixture(name: "project", rootPath: rootURL.path, datasets: [], source: .probed),
                diagnostics: []
            ),
            probedPaths: [outputPath: outputDataset]
        )
        let taskClient = StubGenericTaskClient()
        taskClient.stdout = """
        {
          "kind": "immoments",
          "result": {
            "outfile": ".casa-rs/workspace/native/mom0.image",
            "shape": [250, 250, 1, 1],
            "units": "Jy/beam.km/s"
          }
        }
        """
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(name: "project", rootPath: rootURL.path, datasets: [], source: .probed)
        state.taskCatalog = [
            TaskCatalogEntry(
                id: "immoments",
                category: "Images",
                displayName: "Image Moments",
                binaryName: "immoments",
                cargoPackage: "casa-images",
                overrideEnv: "CASARS_IMMOMENTS_BIN",
                shellKind: "workflow",
                interaction: "one_shot",
                browserKind: nil,
                datasetKinds: ["image"],
                schemaSource: "binary",
                showInTUI: true,
                showInSwift: true,
                includeInSuite: true
            )
        ]
        state.activeTaskID = "immoments"
        let store = WorkbenchStore(
            state: state,
            probeClient: probeClient,
            genericTaskClient: taskClient,
            taskUISchemaClient: StubTaskUISchemaClient(schema: schema)
        )

        store.loadTaskUISchemaIfNeeded("immoments")
        store.setGenericTaskValue(taskID: "immoments", argumentID: "imagename", value: "twhya_n2hp.image")
        store.setGenericTaskValue(taskID: "immoments", argumentID: "outfile", value: ".casa-rs/workspace/native/mom0.image")
        store.setGenericTaskConfirmation(taskID: "immoments", confirmed: true)
        store.runTask()
        waitFor("generic product task completion") {
            store.state.taskRun.state != .running
        }

        XCTAssertEqual(store.state.taskRun.state, TaskRunState.succeeded)
        XCTAssertEqual(store.state.taskRun.outputPaths, [outputPath])
        XCTAssertTrue(store.state.project.datasets.contains { $0.path == outputPath })
        XCTAssertEqual(store.state.runProductGroups.first?.products.first?.path, outputPath)
        XCTAssertEqual(taskClient.requests.first?.workingDirectoryPath, rootURL.path)
    }

    func testGenericImportFitsRegistersImagenameOutputProduct() throws {
        let schema = try JSONDecoder().decode(TaskUISchema.self, from: Data("""
        {
          "schema_version": 1,
          "command_id": "importfits",
          "invocation_name": "importfits",
          "display_name": "Import FITS",
          "category": "Images",
          "summary": "Import FITS files as CASA images.",
          "usage": "importfits <fitsimage> <imagename>",
          "arguments": [
            {"id":"fitsimage","label":"FITS","order":0,"parser":{"kind":"positional","metavar":"fitsimage"},"value_kind":"path","parameter_type":"fits_path","required":true,"default":null,"help":"","group":"Input","advanced":false,"hidden_in_tui":false},
            {"id":"imagename","label":"Image","order":1,"parser":{"kind":"positional","metavar":"imagename"},"value_kind":"path","parameter_type":"output_image_path","required":true,"default":null,"help":"","group":"Output","advanced":false,"hidden_in_tui":false}
          ]
        }
        """.utf8))
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-importfits-product-\(UUID().uuidString)", isDirectory: true)
        let outputURL = rootURL.appendingPathComponent("twhya_cont-importfits.image", isDirectory: true)
        try FileManager.default.createDirectory(at: outputURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let outputPath = outputURL.standardizedFileURL.path
        let outputDataset = DatasetSummary(
            id: outputPath,
            name: "twhya_cont-importfits.image",
            path: outputPath,
            kind: .imageCube,
            size: "250 x 250 x 1 x 1",
            units: "Jy/beam",
            notes: "Recognized by Rust probe."
        )
        let probeClient = StubProjectProbeClient(
            result: ProjectFixtureProbe(
                project: ProjectFixture(name: "project", rootPath: rootURL.path, datasets: [], source: .probed),
                diagnostics: []
            ),
            probedPaths: [outputPath: outputDataset]
        )
        let taskClient = StubGenericTaskClient()
        taskClient.stdout = """
        {
          "kind": "importfits",
          "result": {
            "fitsimage": "twhya_cont.fits",
            "imagename": "twhya_cont-importfits.image",
            "shape": [250, 250, 1, 1]
          }
        }
        """
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(name: "project", rootPath: rootURL.path, datasets: [], source: .probed)
        state.taskCatalog = [makeTaskCatalogEntry(id: "importfits", displayName: "Import FITS")]
        state.activeTaskID = "importfits"
        let store = WorkbenchStore(
            state: state,
            probeClient: probeClient,
            genericTaskClient: taskClient,
            taskUISchemaClient: StubTaskUISchemaClient(schema: schema)
        )

        store.loadTaskUISchemaIfNeeded("importfits")
        store.setGenericTaskValue(taskID: "importfits", argumentID: "fitsimage", value: "twhya_cont.fits")
        store.setGenericTaskValue(taskID: "importfits", argumentID: "imagename", value: "twhya_cont-importfits.image")
        store.setGenericTaskConfirmation(taskID: "importfits", confirmed: true)
        store.runTask()
        waitFor("importfits product registration") {
            store.state.taskRun.state != .running
        }

        XCTAssertEqual(store.state.taskRun.state, TaskRunState.succeeded)
        XCTAssertEqual(store.state.taskRun.outputPaths, [outputPath])
        XCTAssertTrue(store.state.project.datasets.contains { $0.path == outputPath })
    }

    func testProjectDiskRefreshSurfacesLooseFitsFileFromProjectTree() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-disk-refresh-\(UUID().uuidString)", isDirectory: true)
        let outputURL = rootURL.appendingPathComponent("casa-rs-runs/twhya_cont.fits")
        try FileManager.default.createDirectory(at: outputURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        try Data([0x53, 0x49, 0x4d]).write(to: outputURL)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(name: "project", rootPath: rootURL.path, datasets: [], source: .probed),
                    diagnostics: []
                )
            )
        )

        store.openProject(path: rootURL.path)
        store.refreshProjectFromDisk()

        let producedDataset = try XCTUnwrap(store.state.project.datasets.first { $0.path == outputURL.standardizedFileURL.path })
        XCTAssertEqual(producedDataset.kind, .runProduct)
        XCTAssertEqual(producedDataset.name, "twhya_cont.fits")
        XCTAssertEqual(producedDataset.diagnostics, ["Project-relative path: casa-rs-runs/twhya_cont.fits"])
    }

    func testProjectDiskRefreshDeduplicatesDatasetsByCanonicalPath() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-disk-refresh-dedupe-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let imagePath = rootURL.appendingPathComponent("twhya_cont.image", isDirectory: true).path
        let original = DatasetSummary(
            id: imagePath,
            name: "twhya_cont.image",
            path: imagePath,
            kind: .imageCube,
            size: "unprobed",
            units: "",
            notes: "manifest"
        )
        let replacement = DatasetSummary(
            id: imagePath,
            name: "twhya_cont.image",
            path: imagePath,
            kind: .imageCube,
            size: "250 x 250 x 1 x 1",
            units: "Jy/beam",
            notes: "probe"
        )
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(
                        name: "project",
                        rootPath: rootURL.path,
                        datasets: [original, replacement],
                        source: .probed
                    ),
                    diagnostics: []
                )
            )
        )

        store.openProject(path: rootURL.path)
        store.refreshProjectFromDisk()

        let matching = store.state.project.datasets.filter {
            URL(fileURLWithPath: $0.path).standardizedFileURL.path == URL(fileURLWithPath: imagePath).standardizedFileURL.path
        }
        XCTAssertEqual(matching.count, 1)
        XCTAssertEqual(matching.first?.size, "250 x 250 x 1 x 1")
    }

    func testProjectDiskRefreshSurfacesLooseCasaImageDirectoryFromProjectTree() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-disk-refresh-image-\(UUID().uuidString)", isDirectory: true)
        let outputURL = rootURL.appendingPathComponent("twhya_cont-importfits.image", isDirectory: true)
        try FileManager.default.createDirectory(at: outputURL, withIntermediateDirectories: true)
        try Data([0x54, 0x61, 0x62, 0x6c, 0x65]).write(to: outputURL.appendingPathComponent("table.dat"))
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let outputPath = outputURL.standardizedFileURL.path
        let outputDataset = DatasetSummary(
            id: outputPath,
            name: "twhya_cont-importfits.image",
            path: outputPath,
            kind: .imageCube,
            size: "250 x 250 x 1 x 1",
            units: "Jy/beam",
            notes: "Recognized by Rust probe."
        )
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(name: "project", rootPath: rootURL.path, datasets: [], source: .probed),
                    diagnostics: []
                ),
                probedPaths: [outputPath: outputDataset]
            )
        )

        store.openProject(path: rootURL.path)
        store.refreshProjectFromDisk()

        let producedDataset = try XCTUnwrap(store.state.project.datasets.first { $0.path == outputPath })
        XCTAssertEqual(producedDataset.kind, .imageCube)
        XCTAssertEqual(producedDataset.name, "twhya_cont-importfits.image")
    }

    func testProjectDiskRefreshSurfacesLooseImagerProductDirectoriesFromProjectTree() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-disk-refresh-imager-products-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let productNames = [
            "phase_cal.psf",
            "phase_cal.residual",
            "phase_cal.model",
            "phase_cal.image",
            "phase_cal.image.pbcor",
            "phase_cal.psf.tt0",
            "phase_cal.alpha.error",
        ]
        var probedPaths: [String: DatasetSummary] = [:]
        for name in productNames {
            let productURL = rootURL.appendingPathComponent(name, isDirectory: true)
            try FileManager.default.createDirectory(at: productURL, withIntermediateDirectories: true)
            try Data([0x54, 0x61, 0x62, 0x6c, 0x65]).write(to: productURL.appendingPathComponent("table.dat"))
            let path = productURL.standardizedFileURL.path
            probedPaths[path] = DatasetSummary(
                id: path,
                name: name,
                path: path,
                kind: .imageCube,
                size: "250 x 250 x 1 x 1",
                units: "Jy/beam",
                notes: "Recognized by Rust probe."
            )
        }
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(name: "project", rootPath: rootURL.path, datasets: [], source: .probed),
                    diagnostics: []
                ),
                probedPaths: probedPaths
            )
        )

        store.openProject(path: rootURL.path)
        store.refreshProjectFromDisk()

        let surfacedNames = Set(store.state.project.datasets.map(\.name))
        XCTAssertTrue(productNames.allSatisfy { surfacedNames.contains($0) })
    }

    func testProjectDiskRefreshDoesNotProbeLooseDirectoryBySuffixAlone() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-disk-refresh-not-table-\(UUID().uuidString)", isDirectory: true)
        let outputURL = rootURL.appendingPathComponent("phase_cal.psf", isDirectory: true)
        try FileManager.default.createDirectory(at: outputURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let outputPath = outputURL.standardizedFileURL.path
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(name: "project", rootPath: rootURL.path, datasets: [], source: .probed),
                    diagnostics: []
                ),
                probedPaths: [
                    outputPath: DatasetSummary(
                        id: outputPath,
                        name: "phase_cal.psf",
                        path: outputPath,
                        kind: .imageCube,
                        size: "250 x 250 x 1 x 1",
                        units: "Jy/beam",
                        notes: "Should not be used without table.dat."
                    )
                ]
            )
        )

        store.openProject(path: rootURL.path)
        store.refreshProjectFromDisk()

        XCTAssertFalse(store.state.project.datasets.contains { $0.path == outputPath })
    }

    func testProjectDiskRefreshSurfacesTopLevelRegionFile() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-disk-refresh-region-\(UUID().uuidString)", isDirectory: true)
        let regionURL = rootURL.appendingPathComponent("twhya_cont.image-region.crtf")
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        try "#CRTFv0 CASA Region Text Format version 0\nbox[[100pix,100pix],[150pix,150pix]]\n"
            .write(to: regionURL, atomically: true, encoding: .utf8)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(name: "project", rootPath: rootURL.path, datasets: [], source: .probed),
                    diagnostics: []
                )
            )
        )

        store.openProject(path: rootURL.path)
        store.refreshProjectFromDisk()

        let regionDataset = try XCTUnwrap(store.state.project.datasets.first { $0.path == regionURL.standardizedFileURL.path })
        XCTAssertEqual(regionDataset.kind, .region)
        XCTAssertEqual(regionDataset.name, "twhya_cont.image-region.crtf")
        XCTAssertTrue(regionDataset.diagnostics.contains("Region parameter syntax: --region twhya_cont.image-region.crtf"))
    }

    func testGenericImageTaskSeedsImagenameFromSelectedImage() throws {
        let schema = try JSONDecoder().decode(TaskUISchema.self, from: Data("""
        {
          "schema_version": 1,
          "command_id": "immoments",
          "invocation_name": "immoments",
          "display_name": "Image Moments",
          "category": "Images",
          "summary": "Create CASA-style image moment maps.",
          "usage": "immoments <imagename> --outfile <path>",
          "arguments": [
            {"id":"imagename","label":"Image","order":0,"parser":{"kind":"positional","metavar":"imagename"},"value_kind":"path","required":true,"default":null,"help":"","group":"Input","advanced":false,"hidden_in_tui":false},
            {"id":"outfile","label":"Output","order":1,"parser":{"kind":"option","flags":["--outfile"],"metavar":"path","choices":[]},"value_kind":"path","required":true,"default":null,"help":"","group":"Output","advanced":false,"hidden_in_tui":false}
          ]
        }
        """.utf8))
        let image = DatasetSummary(
            id: "/data/project/twhya_n2hp.image",
            name: "twhya_n2hp.image",
            path: "/data/project/twhya_n2hp.image",
            kind: .imageCube,
            size: "250 x 250 x 1 x 15",
            units: "Jy/beam",
            shape: [250, 250, 1, 15],
            notes: "test image"
        )
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(
            name: "project",
            rootPath: "/data/project",
            datasets: [image],
            source: .probed
        )
        state.selectedDatasetID = image.id
        state.activeTaskID = "immoments"
        let store = WorkbenchStore(
            state: state,
            taskUISchemaClient: StubTaskUISchemaClient(schema: schema)
        )

        store.loadTaskUISchemaIfNeeded("immoments")

        XCTAssertEqual(store.state.genericTaskValues["immoments"]?["imagename"], "twhya_n2hp.image")
    }

    func testGenericExportFitsSeedsSelectedImageAndManagedFitsOutput() throws {
        let schema = try JSONDecoder().decode(TaskUISchema.self, from: Data("""
        {
          "schema_version": 1,
          "command_id": "exportfits",
          "invocation_name": "exportfits",
          "display_name": "Export FITS",
          "category": "Images",
          "summary": "Export CASA images to FITS.",
          "usage": "exportfits <imagename> <fitsimage>",
          "arguments": [
            {"id":"imagename","label":"Image","order":0,"parser":{"kind":"positional","metavar":"imagename"},"value_kind":"path","parameter_type":"image_path","required":true,"default":null,"help":"","group":"Input","advanced":false,"hidden_in_tui":false},
            {"id":"fitsimage","label":"FITS","order":1,"parser":{"kind":"positional","metavar":"fitsimage"},"value_kind":"path","parameter_type":"output_fits_path","required":true,"default":null,"help":"","group":"Output","advanced":false,"hidden_in_tui":false}
          ]
        }
        """.utf8))
        let image = DatasetSummary(
            id: "/data/project/twhya_cont.image",
            name: "twhya_cont.image",
            path: "/data/project/twhya_cont.image",
            kind: .imageCube,
            size: "250 x 250 x 1 x 1",
            units: "Jy/beam",
            shape: [250, 250, 1, 1],
            notes: "test image"
        )
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(
            name: "project",
            rootPath: "/data/project",
            datasets: [image],
            source: .probed
        )
        state.selectedDatasetID = image.id
        state.activeTaskID = "exportfits"
        let store = WorkbenchStore(
            state: state,
            taskUISchemaClient: StubTaskUISchemaClient(schema: schema)
        )

        store.loadTaskUISchemaIfNeeded("exportfits")

        XCTAssertEqual(store.state.genericTaskValues["exportfits"]?["imagename"], "twhya_cont.image")
        XCTAssertEqual(
            store.state.genericTaskValues["exportfits"]?["fitsimage"],
            "twhya_cont.fits"
        )
    }

    func testGenericImportFitsKeepsFitsInputSeparateFromManagedImageOutput() throws {
        let schema = try JSONDecoder().decode(TaskUISchema.self, from: Data("""
        {
          "schema_version": 1,
          "command_id": "importfits",
          "invocation_name": "importfits",
          "display_name": "Import FITS",
          "category": "Images",
          "summary": "Import FITS files as CASA images.",
          "usage": "importfits <fitsimage> <imagename>",
          "arguments": [
            {"id":"fitsimage","label":"FITS","order":0,"parser":{"kind":"positional","metavar":"fitsimage"},"value_kind":"path","parameter_type":"fits_path","required":true,"default":null,"help":"","group":"Input","advanced":false,"hidden_in_tui":false},
            {"id":"imagename","label":"Image","order":1,"parser":{"kind":"positional","metavar":"imagename"},"value_kind":"path","parameter_type":"output_image_path","required":true,"default":null,"help":"","group":"Output","advanced":false,"hidden_in_tui":false}
          ]
        }
        """.utf8))
        let fits = DatasetSummary(
            id: "/data/project/twhya_cont.fits",
            name: "twhya_cont.fits",
            path: "/data/project/twhya_cont.fits",
            kind: .runProduct,
            size: "369 KB",
            units: "FITS",
            notes: "test FITS"
        )
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(
            name: "project",
            rootPath: "/data/project",
            datasets: [fits],
            source: .probed
        )
        state.selectedDatasetID = fits.id
        state.activeTaskID = "importfits"
        let store = WorkbenchStore(
            state: state,
            taskUISchemaClient: StubTaskUISchemaClient(schema: schema)
        )

        store.loadTaskUISchemaIfNeeded("importfits")

        XCTAssertEqual(store.state.genericTaskValues["importfits"]?["fitsimage"], "twhya_cont.fits")
        XCTAssertEqual(
            store.state.genericTaskValues["importfits"]?["imagename"],
            "twhya_cont-importfits.image"
        )
    }

    func testGenericImportFitsSeedsSubdirectoryFitsPathRelativeToProject() throws {
        let schema = try JSONDecoder().decode(TaskUISchema.self, from: Data("""
        {
          "schema_version": 1,
          "command_id": "importfits",
          "invocation_name": "importfits",
          "display_name": "Import FITS",
          "category": "Images",
          "summary": "Import FITS files as CASA images.",
          "usage": "importfits <fitsimage> <imagename>",
          "arguments": [
            {"id":"fitsimage","label":"FITS","order":0,"parser":{"kind":"positional","metavar":"fitsimage"},"value_kind":"path","parameter_type":"fits_path","required":true,"default":null,"help":"","group":"Input","advanced":false,"hidden_in_tui":false},
            {"id":"imagename","label":"Image","order":1,"parser":{"kind":"positional","metavar":"imagename"},"value_kind":"path","parameter_type":"output_image_path","required":true,"default":null,"help":"","group":"Output","advanced":false,"hidden_in_tui":false}
          ]
        }
        """.utf8))
        let fits = DatasetSummary(
            id: "/data/project/casa-rs-runs/twhya_cont.fits",
            name: "twhya_cont.fits",
            path: "/data/project/casa-rs-runs/twhya_cont.fits",
            kind: .runProduct,
            size: "369 KB",
            units: "FITS",
            notes: "test FITS"
        )
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(
            name: "project",
            rootPath: "/data/project",
            datasets: [fits],
            source: .probed
        )
        state.selectedDatasetID = fits.id
        state.activeTaskID = "importfits"
        let store = WorkbenchStore(
            state: state,
            taskUISchemaClient: StubTaskUISchemaClient(schema: schema)
        )

        store.loadTaskUISchemaIfNeeded("importfits")

        XCTAssertEqual(store.state.genericTaskValues["importfits"]?["fitsimage"], "casa-rs-runs/twhya_cont.fits")
        XCTAssertEqual(
            store.state.genericTaskValues["importfits"]?["imagename"],
            "twhya_cont-importfits.image"
        )
    }

    func testGenericImagerExecutionPreservesCatalogArgumentsAndAddsRuntimeControls() throws {
        let providerArguments = [
            "--vis", "/data/twhya.ms",
            "--imagename", "/data/casa-rs-runs/twhya",
            "--managed-output", "true",
            "--specmode", "cube",
            "--channel-count", "15",
            "--start", "0.0km/s",
            "--width", "0.5km/s",
            "--outframe", "LSRK",
            "--restfreq", "372.67249GHz",
            "--deconvolver", "mtmfs",
            "--weighting", "briggsbwtaper",
            "--perchanweightdensity",
            "--gridder", "wproject",
            "--standard-mfs-acceleration", "metal",
            "--write-pb",
            "--pbcor",
            "--no-preview-pngs"
        ]
        let request = GenericTaskRequest(
            runID: "run-1",
            task: makeImagerTaskCatalogEntry(),
            providerInvocation: SurfaceProviderInvocation(args: providerArguments)
        )

        let arguments = try ProcessGenericTaskClient.arguments(for: request)
        XCTAssertEqual(Array(arguments.prefix(providerArguments.count)), providerArguments)
        XCTAssertEqual(
            Array(arguments.suffix(6)),
            ["--progress", "true", "--progress-max-uv-points", "16384", "--progress-min-interval-ms", "250"]
        )
    }

    func testGenericMutatingTaskRequiresConfirmationBeforeStart() throws {
        let schema = try JSONDecoder().decode(TaskUISchema.self, from: Data("""
        {
          "schema_version": 1,
          "command_id": "flagdata",
          "invocation_name": "flagdata",
          "display_name": "Flag Data",
          "category": "Flagging",
          "summary": "Run native CASA-style MeasurementSet flagging.",
          "usage": "flagdata",
          "arguments": [
            {"id":"vis","label":"MeasurementSet","order":0,"parser":{"kind":"option","flags":["--vis"],"metavar":"MS","choices":[]},"value_kind":"path","required":true,"default":"/data/input.ms","help":"","group":"Input","advanced":false,"hidden_in_tui":false},
            {"id":"mode","label":"Mode","order":1,"parser":{"kind":"option","flags":["--mode"],"metavar":"MODE","choices":["summary","manual"]},"value_kind":"choice","required":true,"default":"summary","help":"","group":"Flagging","advanced":false,"hidden_in_tui":false}
          ]
        }
        """.utf8))
        let task = TaskCatalogEntry(
            id: "flagdata",
            category: "Flagging",
            displayName: "Flag Data",
            binaryName: "flagdata",
            cargoPackage: "casa-ms",
            overrideEnv: "CASARS_FLAGDATA_BIN",
            shellKind: "workflow",
            interaction: "one_shot",
            browserKind: nil,
            datasetKinds: ["measurement_set"],
            schemaSource: "binary",
            showInTUI: true,
            showInSwift: true,
            includeInSuite: true
        )
        let matrixRow = TaskExecutionMatrixRow(
            taskID: "flagdata",
            displayName: "Flag Data",
            category: "Flagging",
            catalogPresence: "catalog",
            binaryName: "flagdata",
            cargoPackage: "casa-ms",
            datasetKinds: ["measurement_set"],
            suiteInstall: "installed",
            localInstall: "installed",
            releaseInstall: "installed",
            tuiStatus: "invokable",
            guiStatus: "invokable",
            optionSource: "ui_schema_and_dataset_probe",
            fullControlStatus: "partial",
            mutationClass: "mutates_input",
            confirmation: "required_backup_confirmation",
            smokeEvidence: "unit test"
        )
        let taskClient = StubGenericTaskClient()
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(
            name: "project",
            rootPath: "/data/project",
            datasets: [],
            source: .probed
        )
        state.taskCatalog = [task]
        state.taskExecutionMatrixRows = [matrixRow]
        state.activeTaskID = "flagdata"
        let store = WorkbenchStore(
            state: state,
            genericTaskClient: taskClient,
            taskUISchemaClient: StubTaskUISchemaClient(schema: schema),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [matrixRow])
        )

        store.loadTaskUISchemaIfNeeded("flagdata")
        store.setGenericTaskValue(taskID: "flagdata", argumentID: "vis", value: "/data/project/input.ms")
        XCTAssertFalse(
            store.taskRequiresConfirmation(taskID: "flagdata"),
            "historical mutationClass must not override catalog safety for summary mode"
        )
        store.setGenericTaskValue(taskID: "flagdata", argumentID: "mode", value: "manual")
        XCTAssertTrue(store.taskRequiresConfirmation(taskID: "flagdata"))
        store.runTask()

        XCTAssertTrue(taskClient.requests.isEmpty)
        XCTAssertEqual(store.state.taskRun.state, .failed)
        XCTAssertTrue(store.state.taskRun.diagnostics.contains { $0.contains("catalog-declared run risks") })

        store.setGenericTaskConfirmation(taskID: "flagdata", confirmed: true)
        store.runTask()

        XCTAssertEqual(taskClient.requests.count, 1)
        XCTAssertEqual(taskClient.requests.first?.workingDirectoryPath, "/data/project")
    }

    func testMeasurementSetPlotMaxPointParserAcceptsSuffixes() {
        XCTAssertEqual(WorkbenchState.parseMeasurementSetPlotMaxPoints("250000"), 250_000)
        XCTAssertEqual(WorkbenchState.parseMeasurementSetPlotMaxPoints("250k"), 250_000)
        XCTAssertEqual(WorkbenchState.parseMeasurementSetPlotMaxPoints("2M"), 2_000_000)
        XCTAssertEqual(WorkbenchState.parseMeasurementSetPlotMaxPoints("1.5m"), 1_500_000)
        XCTAssertEqual(WorkbenchState.parseMeasurementSetPlotMaxPoints("12M"), 12_000_000)
        XCTAssertNil(WorkbenchState.parseMeasurementSetPlotMaxPoints(""))
        XCTAssertNil(WorkbenchState.parseMeasurementSetPlotMaxPoints("many"))
        XCTAssertNil(WorkbenchState.parseMeasurementSetPlotMaxPoints("-10k"))
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
            ["datasets", "notebooks", "files", "history"]
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

        store.setCommandQuery("open plot samples")
        store.runCommandQuery()
        XCTAssertEqual(store.state.tabs.first { $0.id == store.state.activeTabID }?.kind, .plotSamples)

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

    func testCommandQueryDoesNotTreatNoteSubstringAsNotebookCommand() {
        let store = WorkbenchStore.fixture()

        store.setCommandQuery("denote the selected source")
        store.runCommandQuery()

        XCTAssertEqual(store.state.tabs.first { $0.id == store.state.activeTabID }?.kind, .aiChat)
    }

    func testFixturePlotSamplesAreInspectable() throws {
        let store = WorkbenchStore.fixture()

        let snapshot = store.debugSnapshot()

        XCTAssertEqual(snapshot.workbenchPlots.map(\.id), [
            "sample-plotms-visibility",
            "sample-uv-coverage",
            "sample-million-point-pixels",
            "sample-continuous-point-pixels",
            "sample-antenna-layout",
            "sample-metadata-intervals",
            "sample-stacked-amp-phase",
            "sample-profile-spectrum",
            "sample-image-display"
        ])
        XCTAssertEqual(snapshot.workbenchPlots[0].layerCount, 3)
        XCTAssertGreaterThan(snapshot.workbenchPlots[0].pointCount, 250)
        XCTAssertEqual(snapshot.workbenchPlots[0].boundedLayerCount, 3)
        XCTAssertEqual(snapshot.workbenchPlots[0].payloadStrategies, [
            "inlineDisplayPoints",
            "inlineDisplayPoints",
            "inlineDisplayPoints"
        ])
        XCTAssertEqual(snapshot.workbenchPlots[2].pointCloudCount, 2_000_000)
        XCTAssertEqual(snapshot.workbenchPlots[2].pointRasterLayerCount, 1)
        XCTAssertEqual(snapshot.workbenchPlots[2].sourceSampleCount, 2_000_000)
        XCTAssertLessThan(snapshot.workbenchPlots[2].displaySampleCount, snapshot.workbenchPlots[2].pointCloudCount)
        XCTAssertEqual(snapshot.workbenchPlots[2].payloadStrategies, ["channelBinPointRaster"])
        XCTAssertEqual(snapshot.workbenchPlots[3].pointCloudCount, 2_000_000)
        XCTAssertEqual(snapshot.workbenchPlots[3].pointRasterLayerCount, 1)
        XCTAssertEqual(snapshot.workbenchPlots[3].payloadStrategies, ["singlePixelPointRaster"])
        XCTAssertEqual(snapshot.workbenchPlots[4].pointCount, 8)
        XCTAssertEqual(snapshot.workbenchPlots[5].intervalLayerCount, 2)
        XCTAssertEqual(snapshot.workbenchPlots[5].displaySampleCount, 7)
        XCTAssertEqual(snapshot.workbenchPlots[6].panelCount, 2)
        XCTAssertEqual(snapshot.workbenchPlots[6].layerCount, 3)
        XCTAssertEqual(snapshot.workbenchPlots[7].layerCount, 2)
        XCTAssertGreaterThan(snapshot.workbenchPlots[7].pointCount, 120)
        XCTAssertEqual(snapshot.workbenchPlots[8].rasterLayerCount, 1)
        XCTAssertEqual(snapshot.workbenchPlots[8].overlayShapeCount, 2)
        XCTAssertEqual(snapshot.workbenchPlots[8].payloadStrategies, ["rasterOverview"])
        XCTAssertFalse(snapshot.workbenchPlots[8].dataFingerprint.isEmpty)
        XCTAssertNoThrow(try store.debugJSON())
    }

    func testWorkbenchPlotGapSamplesCoverExplorePayloadShapes() throws {
        let antennaPlot = WorkbenchPlotSamples.antennaLayout()
        let antennaLayer = try XCTUnwrap(antennaPlot.layers.first)
        XCTAssertTrue(antennaLayer.points.allSatisfy { $0.label != nil })
        XCTAssertTrue(antennaLayer.points.contains { ($0.symbolSize ?? 0) > antennaLayer.style.symbolSize })

        let intervals = WorkbenchPlotSamples.metadataIntervals()
        XCTAssertEqual(intervals.axes[1].laneLabels, ["scan 1", "scan 2", "spw 0", "spw 1", "spw 2"])
        XCTAssertEqual(intervals.layers.filter { $0.kind == .interval }.count, 2)
        XCTAssertEqual(intervals.layers.reduce(0) { $0 + $1.intervals.count }, 7)

        let stacked = WorkbenchPlotSamples.stackedAmplitudePhase()
        XCTAssertEqual(stacked.panels.count, 2)
        XCTAssertTrue(stacked.panels[0].axes.contains { $0.id == "phase" && $0.drawsOnTrailingEdge })
        XCTAssertEqual(stacked.allLayers.map(\.id), ["amp-time", "phase-time", "residual-time"])

        let profile = WorkbenchPlotSamples.profileSpectrum()
        let profileLayer = try XCTUnwrap(profile.layers.first { $0.id == "masked-profile" })
        XCTAssertTrue(profileLayer.points.contains { $0.lineBreakBefore })
        XCTAssertTrue(profileLayer.points.contains { $0.selected })

        let image = WorkbenchPlotSamples.imageDisplay()
        XCTAssertEqual(image.overlayShapes.count, 2)
        XCTAssertTrue(image.overlayShapes.contains { !$0.closed })
    }

    func testWorkbenchPlotPointRasterBinsPointCloudToPixels() throws {
        let pointCloud = WorkbenchPlotPointCloud(
            xValues: [0.1, 0.9, 1.2, 2.0, 3.0],
            yValues: [0.1, 0.8, 1.4, 2.0, 1.0]
        )
        let pointRaster = WorkbenchPlotPointRaster.build(
            from: pointCloud,
            xRange: WorkbenchPlotRange(lower: 0, upper: 2),
            yRange: WorkbenchPlotRange(lower: 0, upper: 2),
            width: 2,
            height: 2
        )

        XCTAssertEqual(pointRaster.totalCount, 4)
        XCTAssertEqual(pointRaster.occupiedPixelCount, 2)
        XCTAssertEqual(pointRaster.maxCount, 2)
        XCTAssertEqual(pointRaster.countAt(x: 0, y: 0), 2)
        XCTAssertEqual(pointRaster.countAt(x: 1, y: 1), 2)
        XCTAssertEqual(pointRaster.countAt(x: 2, y: 0), 0)
    }

    func testWorkbenchPlotPointRasterCanFillChannelBinFootprints() throws {
        let pointCloud = WorkbenchPlotPointCloud(
            xValues: [0, 1, 2],
            yValues: [0.5, 0.5, 0.5]
        )
        let pointRaster = WorkbenchPlotPointRaster.build(
            from: pointCloud,
            xRange: WorkbenchPlotRange(lower: 0, upper: 2),
            yRange: WorkbenchPlotRange(lower: 0, upper: 1),
            width: 8,
            height: 1
        )
        let channelBinRaster = WorkbenchPlotPointRaster.build(
            from: pointCloud,
            xRange: WorkbenchPlotRange(lower: 0, upper: 2),
            yRange: WorkbenchPlotRange(lower: 0, upper: 1),
            width: 8,
            height: 1,
            xFootprintDataWidth: 1.0
        )

        XCTAssertEqual(pointRaster.totalCount, 3)
        XCTAssertEqual(pointRaster.occupiedPixelCount, 3)
        XCTAssertEqual(channelBinRaster.totalCount, 3)
        XCTAssertEqual(channelBinRaster.occupiedPixelCount, 8)
        XCTAssertGreaterThan(channelBinRaster.countAt(x: 1, y: 0), 0)
        XCTAssertGreaterThan(channelBinRaster.countAt(x: 3, y: 0), 0)
        XCTAssertGreaterThan(channelBinRaster.countAt(x: 6, y: 0), 0)
    }

    func testWorkbenchPlotChannelPointCloudUsesChannelBinRasterFootprint() throws {
        let plot = WorkbenchPlotSamples.millionPointPixels()
        let layer = try XCTUnwrap(plot.layers.first)
        let pointCloud = try XCTUnwrap(layer.pointCloud)
        let pointRaster = try XCTUnwrap(layer.pointRaster)
        let pointOnlyRaster = WorkbenchPlotPointRaster.build(
            from: pointCloud,
            xRange: plot.axes[0].range,
            yRange: plot.axes[1].range,
            width: pointRaster.width,
            height: pointRaster.height
        )

        XCTAssertEqual(layer.points.count, 0)
        XCTAssertEqual(pointCloud.count, 2_000_000)
        XCTAssertEqual(pointRaster.totalCount, 2_000_000)
        XCTAssertGreaterThan(pointRaster.occupiedPixelCount, 20_000)
        XCTAssertGreaterThan(pointRaster.occupiedPixelCount, pointOnlyRaster.occupiedPixelCount)
        XCTAssertLessThan(pointRaster.occupiedPixelCount, pointCloud.count)
        XCTAssertEqual(layer.dataProfile.strategy, .channelBinPointRaster)
        XCTAssertEqual(layer.dataProfile.xBinWidth, 1.0)
        XCTAssertEqual(layer.dataProfile.sourceSampleCount, UInt64(pointCloud.count))
        XCTAssertEqual(layer.dataProfile.displaySampleCount, pointRaster.occupiedPixelCount)
        XCTAssertLessThanOrEqual(layer.dataProfile.displaySampleCount, layer.dataProfile.pointBudget)
        XCTAssertTrue(layer.dataProfile.isDisplayPayloadBounded)
        XCTAssertEqual(layer.style.symbolSize, 1.0)
    }

    func testWorkbenchPlotContinuousPointCloudUsesSinglePixelRaster() throws {
        let plot = WorkbenchPlotSamples.continuousPointPixels()
        let layer = try XCTUnwrap(plot.layers.first)
        let pointCloud = try XCTUnwrap(layer.pointCloud)
        let pointRaster = try XCTUnwrap(layer.pointRaster)

        XCTAssertEqual(layer.points.count, 0)
        XCTAssertEqual(pointCloud.count, 2_000_000)
        XCTAssertEqual(pointRaster.totalCount, 2_000_000)
        XCTAssertGreaterThan(pointRaster.occupiedPixelCount, 70_000)
        XCTAssertLessThan(pointRaster.occupiedPixelCount, pointCloud.count)
        XCTAssertEqual(layer.dataProfile.strategy, .singlePixelPointRaster)
        XCTAssertNil(layer.dataProfile.xBinWidth)
        XCTAssertEqual(layer.dataProfile.sourceSampleCount, UInt64(pointCloud.count))
        XCTAssertEqual(layer.dataProfile.displaySampleCount, pointRaster.occupiedPixelCount)
        XCTAssertLessThanOrEqual(layer.dataProfile.displaySampleCount, layer.dataProfile.pointBudget)
        XCTAssertTrue(layer.dataProfile.isDisplayPayloadBounded)
        XCTAssertEqual(layer.style.symbolSize, 1.0)
    }

    func testWorkbenchPlotDisplayEditsDoNotRegeneratePayload() throws {
        let store = WorkbenchStore.fixture()
        let plotID = "sample-plotms-visibility"
        let original = try XCTUnwrap(store.state.plotDocuments.first { $0.id == plotID })
        let originalFingerprint = original.dataFingerprint
        let layerID = try XCTUnwrap(original.layers.first?.id)

        store.applyWorkbenchPlotEdit(
            plotID: plotID,
            action: .setLayerSymbolSize(layerID: layerID, size: 9.5)
        )
        store.applyWorkbenchPlotEdit(
            plotID: plotID,
            action: .setLayerOpacity(layerID: layerID, opacity: 0.35)
        )
        store.applyWorkbenchPlotEdit(
            plotID: plotID,
            action: .addAnnotation(id: "fit-note", x: 60, y: 4.4, text: "fit candidate")
        )

        let edited = try XCTUnwrap(store.state.plotDocuments.first { $0.id == plotID })
        XCTAssertEqual(edited.dataFingerprint, originalFingerprint)
        XCTAssertEqual(edited.styleRevision, 3)
        XCTAssertEqual(edited.layers.first?.style.symbolSize, 9.5)
        XCTAssertEqual(edited.layers.first?.style.opacity, 0.35)
        XCTAssertTrue(edited.annotations.contains { $0.id == "fit-note" })

        let encodedAction = try JSONEncoder().encode(
            WorkbenchPlotEditAction.setLayerLineWidth(layerID: "gaussian-fit", width: 3.0)
        )
        XCTAssertFalse(encodedAction.isEmpty)
    }

    func testWorkbenchPointRasterSymbolSizeIsDisplayOnly() throws {
        let store = WorkbenchStore.fixture()
        let plotID = "sample-million-point-pixels"
        let original = try XCTUnwrap(store.state.plotDocuments.first { $0.id == plotID })
        let originalFingerprint = original.dataFingerprint
        let layerID = try XCTUnwrap(original.layers.first?.id)
        let originalRaster = try XCTUnwrap(original.layers.first?.pointRaster)

        store.applyWorkbenchPlotEdit(
            plotID: plotID,
            action: .setLayerSymbolSize(layerID: layerID, size: 11.0)
        )

        let edited = try XCTUnwrap(store.state.plotDocuments.first { $0.id == plotID })
        let editedRaster = try XCTUnwrap(edited.layers.first?.pointRaster)
        XCTAssertEqual(edited.dataFingerprint, originalFingerprint)
        XCTAssertEqual(edited.styleRevision, 1)
        XCTAssertEqual(edited.layers.first?.style.symbolSize, 11.0)
        XCTAssertEqual(editedRaster.totalCount, originalRaster.totalCount)
        XCTAssertEqual(editedRaster.occupiedPixelCount, originalRaster.occupiedPixelCount)
    }

    func testWorkbenchImageSampleStretchAndColorMapAreDisplayOnly() throws {
        let store = WorkbenchStore.fixture()
        let plotID = "sample-image-display"
        let original = try XCTUnwrap(store.state.plotDocuments.first { $0.id == plotID })
        let originalFingerprint = original.dataFingerprint
        let layerID = try XCTUnwrap(original.layers.first?.id)

        store.applyWorkbenchPlotEdit(
            plotID: plotID,
            action: .setRasterStretch(layerID: layerID, stretch: .logarithmic)
        )
        store.applyWorkbenchPlotEdit(
            plotID: plotID,
            action: .setRasterColorMap(layerID: layerID, colorMap: .magma)
        )
        store.applyWorkbenchPlotEdit(
            plotID: plotID,
            action: .setAxisLabelsVisible(axisID: "ra", visible: false)
        )

        let edited = try XCTUnwrap(store.state.plotDocuments.first { $0.id == plotID })
        XCTAssertEqual(edited.dataFingerprint, originalFingerprint)
        XCTAssertEqual(edited.styleRevision, 3)
        XCTAssertEqual(edited.layers.first?.raster?.stretch, .logarithmic)
        XCTAssertEqual(edited.layers.first?.raster?.colorMap, .magma)
        XCTAssertEqual(edited.axes.first?.labelsVisible, false)
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

    func testRealImageExplorerUsesRustBackedSnapshotState() throws {
        let imageDataset = DatasetSummary(
            id: "/data/restored.image",
            name: "restored.image",
            path: "/data/restored.image",
            kind: .imageCube,
            size: "4 x 4 x 8",
            units: "Jy/beam",
            shape: [4, 4, 8],
            notes: "Recognized by Rust probe."
        )
        let imageClient = StubImageExplorerClient(snapshot: makeImageExplorerSnapshot())
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(name: "Real Project", rootPath: "/data", datasets: [imageDataset], source: .probed),
                    diagnostics: []
                )
            ),
            imageExplorerClient: imageClient
        )

        store.openProject(path: "/data")

        let state = try XCTUnwrap(store.state.imageExplorers[imageDataset.id])
        XCTAssertEqual(state.status, .ready)
        XCTAssertEqual(state.selectedView, "plane")
        XCTAssertEqual(state.snapshot?.activeView, "plane")
        XCTAssertEqual(state.snapshot?.plane?.width, 2)
        XCTAssertEqual(state.snapshot?.profile?.samples.count, 2)
        XCTAssertEqual(imageClient.paths, ["/data/restored.image"])
        XCTAssertEqual(store.debugSnapshot().imageExplorers[imageDataset.id]?.planeSize, "2x2")

        store.setImageExplorerView("spectrum", datasetID: imageDataset.id)
        XCTAssertEqual(store.state.imageExplorers[imageDataset.id]?.selectedView, "spectrum")
        XCTAssertEqual(imageClient.requests.map(\.selectedView), ["plane", "spectrum"])

        store.stepImageExplorerNonDisplayAxis(axis: 2, delta: 1, datasetID: imageDataset.id)
        XCTAssertEqual(imageClient.requests.last?.nonDisplayIndices, [1])
        XCTAssertEqual(store.state.imageExplorers[imageDataset.id]?.nonDisplayIndices, [1])
        store.setImageExplorerNonDisplayAxisIndex(axis: 2, index: 5, datasetID: imageDataset.id)
        XCTAssertEqual(imageClient.requests.last?.nonDisplayIndices, [5])
        XCTAssertEqual(store.state.imageExplorers[imageDataset.id]?.nonDisplayIndices, [5])
        store.setImageExplorerNonDisplayAxisIndex(axis: 2, index: 1, datasetID: imageDataset.id)

        store.startImageExplorerMovie(axis: 2, framesPerSecond: 12, loop: true, datasetID: imageDataset.id)
        XCTAssertEqual(store.state.imageExplorers[imageDataset.id]?.moviePlaying, true)
        XCTAssertEqual(store.state.imageExplorers[imageDataset.id]?.movieAxis, 2)
        XCTAssertEqual(store.state.imageExplorers[imageDataset.id]?.movieFramesPerSecond, 12)
        XCTAssertEqual(store.debugSnapshot().imageExplorers[imageDataset.id]?.moviePlaying, true)
        store.advanceImageExplorerMovieFrame(datasetID: imageDataset.id)
        XCTAssertEqual(imageClient.requests.last?.nonDisplayIndices, [2])
        for expectedIndex in 3...7 {
            store.advanceImageExplorerMovieFrame(datasetID: imageDataset.id)
            XCTAssertEqual(imageClient.requests.last?.nonDisplayIndices, [expectedIndex])
        }
        store.advanceImageExplorerMovieFrame(datasetID: imageDataset.id)
        XCTAssertEqual(imageClient.requests.last?.nonDisplayIndices, [0])
        store.setImageExplorerMovieLoop(false, datasetID: imageDataset.id)
        for expectedIndex in 1...7 {
            store.advanceImageExplorerMovieFrame(datasetID: imageDataset.id)
            XCTAssertEqual(imageClient.requests.last?.nonDisplayIndices, [expectedIndex])
        }
        store.advanceImageExplorerMovieFrame(datasetID: imageDataset.id)
        XCTAssertEqual(store.state.imageExplorers[imageDataset.id]?.moviePlaying, false)

        store.setImageExplorerFocus("inspector", datasetID: imageDataset.id)
        store.setImageExplorerPlaneContentMode("spreadsheet", datasetID: imageDataset.id)
        store.setImageExplorerCursor(x: 2, y: 3, datasetID: imageDataset.id)
        store.setImageExplorerParameters(
            ImageExplorerParameters(blc: "0,0", trc: "3,3", inc: "1,1", stretch: "minmax"),
            datasetID: imageDataset.id
        )
        XCTAssertEqual(imageClient.requests.last?.focus, "inspector")
        XCTAssertEqual(imageClient.requests.last?.planeContentMode, "spreadsheet")
        XCTAssertEqual(imageClient.requests.last?.cursorX, 2)
        XCTAssertEqual(imageClient.requests.last?.cursorY, 3)
        XCTAssertEqual(imageClient.requests.last?.parameters.stretch, "minmax")

        let requestCountBeforeColorMap = imageClient.requests.count
        XCTAssertEqual(store.state.imageExplorers[imageDataset.id]?.planeColorMap, .grayscale)
        store.cycleImageExplorerColorMap(datasetID: imageDataset.id)
        XCTAssertEqual(store.state.imageExplorers[imageDataset.id]?.planeColorMap, .viridis)
        store.setImageExplorerColorMap(.inferno, datasetID: imageDataset.id)
        XCTAssertEqual(store.state.imageExplorers[imageDataset.id]?.planeColorMap, .inferno)
        XCTAssertEqual(imageClient.requests.count, requestCountBeforeColorMap)

        store.setImageExplorerManualClip(low: -0.125, high: 2.75, datasetID: imageDataset.id)
        XCTAssertEqual(imageClient.requests.last?.parameters.stretch, "manual")
        XCTAssertEqual(imageClient.requests.last?.parameters.clipLow, "-0.125")
        XCTAssertEqual(imageClient.requests.last?.parameters.clipHigh, "2.75")

        store.appendImageExplorerRegionCommand(.startRegionShape, datasetID: imageDataset.id)
        store.appendImageExplorerRegionCommand(.appendRegionVertex(x: 2, y: 3), datasetID: imageDataset.id)
        XCTAssertEqual(imageClient.requests.last?.commands.map(\.command), ["start_region_shape", "append_region_vertex"])
        store.runImageExplorerCommandOnce(.setDefaultMask(name: "mask0"), datasetID: imageDataset.id)
        XCTAssertEqual(imageClient.requests.last?.transientCommands.map(\.command), ["set_default_mask"])
        XCTAssertEqual(store.state.imageExplorers[imageDataset.id]?.transientCommands, [])
    }

    func testImageRegionAndMaskWritesRecordSuccessFailureAndOneRunBypass() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-notebook-image-write-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let imagePath = rootURL.appendingPathComponent("restored.image", isDirectory: true).path
        let imageDataset = DatasetSummary(
            id: imagePath,
            name: "restored.image",
            path: imagePath,
            kind: .imageCube,
            size: "4 x 4 x 8",
            units: "Jy/beam",
            shape: [4, 4, 8],
            notes: "Test image"
        )
        let imageClient = StubImageExplorerClient(snapshot: makeImageExplorerSnapshot())
        let notebookClient = RecordingNotebookPersistenceClient()
        var state = EmptyWorkbench.makeState()
        state.project = ProjectFixture(name: "Project", rootPath: rootURL.path, datasets: [imageDataset], source: .probed)
        state.selectedDatasetID = imageDataset.id
        state.tabs = [WorkbenchTab(
            id: imageDataset.explorerTabID,
            title: imageDataset.name,
            kind: .datasetExplorer,
            datasetID: imageDataset.id
        )]
        state.activeTabID = imageDataset.explorerTabID
        let store = WorkbenchStore(
            state: state,
            imageExplorerClient: imageClient,
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )
        store.installNotebookPersistenceClientForTesting(notebookClient)
        store.refreshImageExplorer(datasetID: imageDataset.id)

        store.runImageExplorerCommandOnce(.setDefaultMask(name: "mask0"), datasetID: imageDataset.id)
        XCTAssertEqual(notebookClient.beginRequests.last?.request.operationId, "imexplore.set_default_mask")
        XCTAssertEqual(notebookClient.beginRequests.last?.request.runSafety.classification, "input_mutation")
        XCTAssertEqual(notebookClient.beginRequests.last?.request.runSafety.affectedPaths, [imagePath])
        XCTAssertNil(notebookClient.beginRequests.last?.request.taskIntent)
        XCTAssertEqual(notebookClient.finalizeRequests.last?.finalization.status, "succeeded")

        store.setNotebookRecordingBypassOnce(tabID: imageDataset.explorerTabID, enabled: true)
        let finalizedBeforeBypass = notebookClient.finalizeRequests.count
        store.runImageExplorerCommandOnce(.unsetDefaultMask, datasetID: imageDataset.id)
        XCTAssertEqual(notebookClient.beginRequests.last?.policy, "bypass_once")
        XCTAssertEqual(notebookClient.finalizeRequests.count, finalizedBeforeBypass)
        XCTAssertFalse(store.notebookRecordingBypassOnce(tabID: imageDataset.explorerTabID))

        imageClient.failWhenCommandsAreQueued = true
        store.runImageExplorerCommandOnce(.deleteMask(name: "mask0"), datasetID: imageDataset.id)
        XCTAssertEqual(notebookClient.finalizeRequests.last?.finalization.status, "failed")
        XCTAssertTrue(
            notebookClient.finalizeRequests.last?.finalization.diagnostics.contains {
                $0.contains("bad region command sequence")
            } == true
        )
    }

    func testImageExplorerBoxRegionReplacesQueuedRegionCommands() throws {
        let imageDataset = DatasetSummary(
            id: "/data/restored.image",
            name: "restored.image",
            path: "/data/restored.image",
            kind: .imageCube,
            size: "4 x 4 x 8",
            units: "Jy/beam",
            shape: [4, 4, 8],
            notes: "Recognized by Rust probe."
        )
        let imageClient = StubImageExplorerClient(snapshot: makeImageExplorerSnapshot())
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(name: "Real Project", rootPath: "/data", datasets: [imageDataset], source: .probed),
                    diagnostics: []
                )
            ),
            imageExplorerClient: imageClient
        )

        store.openProject(path: "/data")
        store.appendImageExplorerRegionCommand(.startRegionShape, datasetID: imageDataset.id)
        store.setImageExplorerBoxRegion("150, 100, 100, 150", datasetID: imageDataset.id)

        let commands = try XCTUnwrap(imageClient.requests.last?.commands)
        XCTAssertEqual(
            commands.map(\.command),
            [
                "start_region_shape",
                "append_region_vertex",
                "append_region_vertex",
                "append_region_vertex",
                "append_region_vertex",
                "close_region_shape",
            ]
        )
        XCTAssertEqual(commands.compactMap(\.x), [100, 150, 150, 100])
        XCTAssertEqual(commands.compactMap(\.y), [100, 100, 150, 150])
    }

    func testImageExplorerBoxRegionCanAppendToExistingRegionCommands() throws {
        let imageDataset = DatasetSummary(
            id: "/data/restored.image",
            name: "restored.image",
            path: "/data/restored.image",
            kind: .imageCube,
            size: "4 x 4 x 8",
            units: "Jy/beam",
            shape: [4, 4, 8],
            notes: "Recognized by Rust probe."
        )
        let imageClient = StubImageExplorerClient(snapshot: makeImageExplorerSnapshot())
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(name: "Real Project", rootPath: "/data", datasets: [imageDataset], source: .probed),
                    diagnostics: []
                )
            ),
            imageExplorerClient: imageClient
        )

        store.openProject(path: "/data")
        store.setImageExplorerBoxRegion("10,20,30,40", datasetID: imageDataset.id)
        store.appendImageExplorerBoxRegion("100,110,120,130", datasetID: imageDataset.id)

        let commands = try XCTUnwrap(imageClient.requests.last?.commands)
        XCTAssertEqual(
            commands.map(\.command),
            [
                "start_region_shape",
                "append_region_vertex",
                "append_region_vertex",
                "append_region_vertex",
                "append_region_vertex",
                "close_region_shape",
                "start_region_shape",
                "append_region_vertex",
                "append_region_vertex",
                "append_region_vertex",
                "append_region_vertex",
                "close_region_shape",
            ]
        )
        XCTAssertEqual(commands.compactMap(\.x), [10, 30, 30, 10, 100, 120, 120, 100])
        XCTAssertEqual(commands.compactMap(\.y), [20, 20, 40, 40, 110, 110, 130, 130])
    }

    func testImageExplorerPolygonRegionReplacesQueuedRegionCommands() throws {
        let imageDataset = DatasetSummary(
            id: "/data/restored.image",
            name: "restored.image",
            path: "/data/restored.image",
            kind: .imageCube,
            size: "4 x 4 x 8",
            units: "Jy/beam",
            shape: [4, 4, 8],
            notes: "Recognized by Rust probe."
        )
        let imageClient = StubImageExplorerClient(snapshot: makeImageExplorerSnapshot())
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(name: "Real Project", rootPath: "/data", datasets: [imageDataset], source: .probed),
                    diagnostics: []
                )
            ),
            imageExplorerClient: imageClient
        )

        store.openProject(path: "/data")
        store.appendImageExplorerRegionCommand(.startRegionShape, datasetID: imageDataset.id)
        store.setImageExplorerPolygonRegion(vertices: [(1, 2), (3, 4), (5, 6)], datasetID: imageDataset.id)

        let commands = try XCTUnwrap(imageClient.requests.last?.commands)
        XCTAssertEqual(
            commands.map(\.command),
            [
                "start_region_shape",
                "append_region_vertex",
                "append_region_vertex",
                "append_region_vertex",
                "close_region_shape",
            ]
        )
        XCTAssertEqual(commands.compactMap(\.x), [1, 3, 5])
        XCTAssertEqual(commands.compactMap(\.y), [2, 4, 6])
    }

    func testImageExplorerRegionShapesReplaceAllShapesWithoutDroppingUntouchedShapes() throws {
        let imageDataset = DatasetSummary(
            id: "/data/restored.image",
            name: "restored.image",
            path: "/data/restored.image",
            kind: .imageCube,
            size: "4 x 4 x 8",
            units: "Jy/beam",
            shape: [4, 4, 8],
            notes: "Recognized by Rust probe."
        )
        let imageClient = StubImageExplorerClient(snapshot: makeImageExplorerSnapshot())
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(name: "Real Project", rootPath: "/data", datasets: [imageDataset], source: .probed),
                    diagnostics: []
                )
            ),
            imageExplorerClient: imageClient
        )

        store.openProject(path: "/data")
        store.setImageExplorerRegionShapes(
            [
                [(x: 1, y: 1), (x: 3, y: 1), (x: 3, y: 3), (x: 1, y: 3)],
                [(x: 10, y: 10), (x: 12, y: 10), (x: 11, y: 12)],
            ],
            datasetID: imageDataset.id
        )

        let commands = try XCTUnwrap(imageClient.requests.last?.commands)
        XCTAssertEqual(commands.map(\.command).filter { $0 == "start_region_shape" }.count, 2)
        XCTAssertEqual(commands.map(\.command).filter { $0 == "close_region_shape" }.count, 2)
        XCTAssertEqual(commands.compactMap(\.x), [1, 3, 3, 1, 10, 12, 11])
        XCTAssertEqual(commands.compactMap(\.y), [1, 1, 3, 3, 10, 10, 12])
    }

    func testImageExplorerClearsBadQueuedRegionCommandsAndRecovers() throws {
        let imageDataset = DatasetSummary(
            id: "/data/restored.image",
            name: "restored.image",
            path: "/data/restored.image",
            kind: .imageCube,
            size: "4 x 4 x 8",
            units: "Jy/beam",
            shape: [4, 4, 8],
            notes: "Recognized by Rust probe."
        )
        let imageClient = StubImageExplorerClient(snapshot: makeImageExplorerSnapshot())
        imageClient.failWhenCommandsAreQueued = true
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(name: "Real Project", rootPath: "/data", datasets: [imageDataset], source: .probed),
                    diagnostics: []
                )
            ),
            imageExplorerClient: imageClient
        )

        store.openProject(path: "/data")
        store.appendImageExplorerRegionCommand(.appendRegionVertex(x: 2, y: 3), datasetID: imageDataset.id)

        let recovered = try XCTUnwrap(store.state.imageExplorers[imageDataset.id])
        XCTAssertEqual(recovered.status, .ready)
        XCTAssertNil(recovered.lastError)
        XCTAssertNotNil(recovered.snapshot)
        XCTAssertEqual(recovered.regionCommands, [])
        XCTAssertEqual(recovered.transientCommands, [])
        XCTAssertEqual(imageClient.requests.suffix(2).map { $0.commands.map(\.command) }, [["append_region_vertex"], []])
        XCTAssertTrue(
            store.state.lastErrors.contains {
                $0.contains("Cleared invalid image explorer region command sequence")
            }
        )
    }

    func testRealTableBrowserUsesRustBackedSnapshotState() throws {
        let tableDataset = DatasetSummary(
            id: "/data/MAIN",
            name: "MAIN",
            path: "/data/MAIN",
            kind: .table,
            size: "12 rows, 3 columns",
            units: "casacore table",
            columns: ["TIME", "DATA", "FLAG"],
            subtables: ["ANTENNA"],
            shape: [12],
            notes: "Recognized by Rust probe."
        )
        let tableClient = StubTableBrowserClient(snapshot: makeTableBrowserSnapshot(path: tableDataset.path))
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(name: "Real Project", rootPath: "/data", datasets: [tableDataset], source: .probed),
                    diagnostics: []
                )
            ),
            tableBrowserClient: tableClient
        )

        store.openProject(path: "/data")

        let state = try XCTUnwrap(store.state.tableBrowsers[tableDataset.id])
        XCTAssertEqual(state.status, .ready)
        XCTAssertEqual(state.profileView, "summary")
        XCTAssertEqual(state.snapshot?.view, "overview")
        XCTAssertEqual(state.snapshot?.contentLines.first, "Cells  row=1/12  col=1/3  focus=Main")
        XCTAssertEqual(tableClient.paths, ["/data/MAIN"])
        XCTAssertEqual(store.debugSnapshot().tableBrowsers[tableDataset.id]?.inspectorTitle, "Column DATA")

        store.setTableBrowserView("keywords", datasetID: tableDataset.id)
        XCTAssertEqual(store.state.tableBrowsers[tableDataset.id]?.selectedView, "keywords")
        XCTAssertEqual(tableClient.requests.map(\.selectedView), ["overview", "keywords"])
    }

    func testMeasurementSetCanOpenInDedicatedTableBrowserTab() throws {
        let msDataset = DatasetSummary(
            id: "/data/example.ms",
            name: "example.ms",
            path: "/data/example.ms",
            kind: .measurementSet,
            size: "12 rows, 3 columns",
            units: "Jy, Hz, seconds",
            columns: ["TIME", "DATA", "FLAG"],
            subtables: ["ANTENNA"],
            shape: [12],
            notes: "Recognized by Rust probe."
        )
        let tableClient = StubTableBrowserClient(snapshot: makeTableBrowserSnapshot(path: msDataset.path))
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(name: "Real Project", rootPath: "/data", datasets: [msDataset], source: .probed),
                    diagnostics: []
                )
            ),
            tableBrowserClient: tableClient
        )

        store.openProject(path: "/data")
        store.openDatasetTableBrowser(msDataset.id)
        store.setTableBrowserView("cells", datasetID: msDataset.id)

        XCTAssertEqual(store.state.activeTabID, "tab-tablebrowser-\(msDataset.id)")
        XCTAssertEqual(store.state.tabs.last?.kind, .tableBrowser)
        XCTAssertEqual(store.state.tabs.last?.title, "Table: example.ms")
        XCTAssertEqual(tableClient.paths, [msDataset.path, msDataset.path])
        waitFor("initial table cell window") {
            store.state.tableBrowsers[msDataset.id]?.cellWindow?.rowCount == 12
        }
        XCTAssertEqual(tableClient.cellWindowRequests.map(\.datasetPath), [msDataset.path])
        XCTAssertEqual(store.state.tableBrowsers[msDataset.id]?.status, .ready)
        XCTAssertEqual(store.state.tableBrowsers[msDataset.id]?.cellWindow?.rowCount, 12)
    }

    func testTableBrowserCellWindowRequestsDoNotRebuildWholeSnapshot() throws {
        let tableDataset = DatasetSummary(
            id: "/data/example.table",
            name: "example.table",
            path: "/data/example.table",
            kind: .table,
            size: "12 rows, 3 columns",
            units: "Jy, Hz, seconds",
            columns: ["TIME", "DATA", "FLAG"],
            subtables: [],
            shape: [12],
            notes: "Recognized by Rust probe."
        )
        let tableClient = StubTableBrowserClient(snapshot: makeTableBrowserSnapshot(path: tableDataset.path))
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(name: "Real Project", rootPath: "/data", datasets: [tableDataset], source: .probed),
                    diagnostics: []
                )
            ),
            tableBrowserClient: tableClient
        )

        store.openProject(path: "/data")
        store.openDatasetTableBrowser(tableDataset.id)
        let snapshotRequestCount = tableClient.requests.count

        store.requestTableBrowserCellWindow(
            rowStart: 8,
            rowLimit: 32,
            columnStart: 2,
            columnLimit: 12,
            datasetID: tableDataset.id
        )

        waitFor("requested table cell window") {
            store.state.tableBrowsers[tableDataset.id]?.cellWindow?.rowStart == 8
        }
        XCTAssertEqual(tableClient.requests.count, snapshotRequestCount)
        XCTAssertEqual(tableClient.cellWindowRequests.last?.rowStart, 8)
        XCTAssertEqual(tableClient.cellWindowRequests.last?.columnStart, 2)
        XCTAssertEqual(store.state.tableBrowsers[tableDataset.id]?.cellWindow?.rowStart, 8)
        XCTAssertEqual(store.state.tableBrowsers[tableDataset.id]?.cellWindow?.columnStart, 2)

        let cellWindowRequestCount = tableClient.cellWindowRequests.count
        store.selectTableBrowserVisibleCell(
            rowIndex: 9,
            selectedVisibleColumn: nil,
            targetVisibleColumn: 3,
            datasetID: tableDataset.id
        )
        XCTAssertEqual(tableClient.requests.count, snapshotRequestCount)
        XCTAssertEqual(tableClient.cellWindowRequests.count, cellWindowRequestCount)
        XCTAssertEqual(store.state.tableBrowsers[tableDataset.id]?.selectedCellRow, 9)
        XCTAssertEqual(store.state.tableBrowsers[tableDataset.id]?.selectedCellColumn, 3)
    }

    func testSelectedSubtableOpensInNewTableBrowserTab() throws {
        let msDataset = DatasetSummary(
            id: "/data/example.ms",
            name: "example.ms",
            path: "/data/example.ms",
            kind: .measurementSet,
            size: "12 rows, 3 columns",
            units: "Jy, Hz, seconds",
            columns: ["TIME", "DATA", "FLAG"],
            subtables: ["ANTENNA"],
            shape: [12],
            notes: "Recognized by Rust probe."
        )
        var snapshot = makeTableBrowserSnapshot(path: msDataset.path)
        snapshot.view = "subtables"
        snapshot.selectedAddress = TableBrowserSnapshot.SelectedAddress(
            kind: "subtable",
            tablePath: msDataset.path,
            row: nil,
            column: nil,
            keywordPath: nil,
            valuePath: nil,
            source: "table keyword",
            targetPath: "/data/example.ms/ANTENNA"
        )
        let tableClient = StubTableBrowserClient(snapshot: snapshot)
        let store = WorkbenchStore(
            state: EmptyWorkbench.makeState(),
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(name: "Real Project", rootPath: "/data", datasets: [msDataset], source: .probed),
                    diagnostics: []
                )
            ),
            tableBrowserClient: tableClient
        )

        store.openProject(path: "/data")
        store.openDatasetTableBrowser(msDataset.id)
        store.openSelectedTableBrowserSubtable(datasetID: msDataset.id)

        XCTAssertEqual(store.state.activeTabID, "tab-tablebrowser-/data/example.ms/ANTENNA")
        XCTAssertEqual(store.state.tabs.last?.title, "Table: ANTENNA")
        XCTAssertEqual(store.state.project.datasets.last?.path, "/data/example.ms/ANTENNA")
        XCTAssertEqual(tableClient.paths, [msDataset.path, "/data/example.ms/ANTENNA"])
    }

    func testExplorerTabsExposeTypedRealDatasetRoutesInDebugSnapshot() {
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
            id: "/data/restored.image",
            name: "restored.image",
            path: "/data/restored.image",
            kind: .imageCube,
            size: "256 x 256 x 8",
            units: "Jy/beam",
            columns: ["map"],
            shape: [256, 256, 8],
            notes: "Recognized by Rust image probe.",
            diagnostics: [
                "Pixel type: float32",
                "Cell size: 0.1 x 0.1 arcsec",
                "Center: RA 10:00:00.000 Dec -30.00.00.00",
                "Cube center frequency: 372.533 GHz",
                "Total bandwidth: 384 MHz",
                "Channel separation: 1 MHz",
                "Beam: 0.42 x 0.31 arcsec, PA 12 deg"
            ]
        )
        let tableDataset = DatasetSummary(
            id: "/data/G.cal",
            name: "G.cal",
            path: "/data/G.cal",
            kind: .table,
            size: "3 rows, 4 columns",
            units: "Calibration Table",
            columns: ["ANTENNA1", "FIELD_ID", "CPARAM", "FLAG"],
            shape: [3],
            notes: "Recognized by Rust table probe."
        )
        let store = WorkbenchStore(
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(
                        name: "Real Project",
                        rootPath: "/data",
                        datasets: [msDataset, imageDataset, tableDataset],
                        source: .probed
                    ),
                    diagnostics: []
                )
            )
        )

        store.openProject(path: "/data")
        store.openDatasetExplorer(imageDataset.id)
        store.openDatasetExplorer(tableDataset.id)

        let snapshot = store.debugSnapshot()
        XCTAssertEqual(snapshot.openTabs, ["MS: probed.ms", "Image: restored.image", "Table: G.cal"])
        XCTAssertEqual(snapshot.explorerTabs.map(\.datasetKind), [.measurementSet, .imageCube, .table])
        XCTAssertEqual(snapshot.explorerTabs.map(\.path), [msDataset.path, imageDataset.path, tableDataset.path])
        XCTAssertEqual(snapshot.selectedDatasetSummary?.kind, .table)
        XCTAssertEqual(snapshot.selectedDatasetSummary?.columns, ["ANTENNA1", "FIELD_ID", "CPARAM", "FLAG"])
    }

    func testFakeExecutionTabsAreGatedOutsideDemoProjectButRealImagingTaskOpens() throws {
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
        let store = WorkbenchStore(
            probeClient: client,
            taskCatalogClient: StubTaskCatalogClient(tasks: [makeImagerTaskCatalogEntry()]),
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImagerTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.openDefaultTab(kind: .aiChat)
        store.openDefaultTab(kind: .python)
        store.openDefaultTab(kind: .task)
        XCTAssertEqual(store.state.tabs.count, 1)
        XCTAssertEqual(store.state.tabs.first?.title, "Tasks")
        XCTAssertEqual(store.state.lastErrors.count, 2)

        store.openProject(path: "/data")
        store.openDefaultTab(kind: .aiChat)
        store.openDefaultTab(kind: .python)
        store.openDefaultTab(kind: .task)

        XCTAssertEqual(store.state.tabs.count, 2)
        XCTAssertEqual(store.state.tabs.first?.kind, .datasetExplorer)
        XCTAssertEqual(store.state.tabs.last?.title, "Tasks")
        XCTAssertNil(store.debugSnapshot().taskImagerProgress)
        store.openImagerTaskForSelectedDataset()
        XCTAssertEqual(store.state.tabs.count, 3)
        XCTAssertEqual(store.state.tabs.last?.title, "Imager: probed.ms")
        XCTAssertEqual(store.state.activeTaskID, "imager")
        XCTAssertEqual(store.state.genericTaskValues["imager"]?["vis"], "probed.ms")
        XCTAssertEqual(store.state.genericTaskValues["imager"]?["field"], "0")
        XCTAssertEqual(store.state.genericTaskValues["imager"]?["phasecenter_field"], "none")
        XCTAssertEqual(store.state.genericTaskValues["imager"]?["spw"], "0")
        XCTAssertEqual(store.state.genericTaskValues["imager"]?["imagename"], "casa-rs-runs/imager-1/probed.ms-imager")
        XCTAssertEqual(store.state.genericTaskToggles["imager"]?["dirty_only"], true)
        XCTAssertFalse(store.state.lastErrors.contains("AI chat is not connected yet"))
        XCTAssertTrue(store.state.lastErrors.contains("Python is not connected yet"))
        XCTAssertFalse(store.state.lastErrors.contains("Task panels are not connected for real projects yet"))

        let fixtureStore = WorkbenchStore.fixture()
        fixtureStore.openDefaultTab(kind: .aiChat)
        fixtureStore.openDefaultTab(kind: .python)
        fixtureStore.openDefaultTab(kind: .task)

        XCTAssertTrue(fixtureStore.state.tabs.contains { $0.kind == .aiChat })
        XCTAssertTrue(fixtureStore.state.tabs.contains { $0.kind == .python })
        XCTAssertTrue(fixtureStore.state.tabs.contains { $0.kind == .task })
    }

    func testImagerProgressStubUsesRowsDownChannelsAcrossAndWholeXYPlanes() throws {
        let source = StubImagerProgressSource()
        let snapshot = try XCTUnwrap(source.snapshot(for: ImagerProgressRequest(
            taskID: "imager",
            runID: "imager-1",
            taskState: .running,
            progress: 0.35,
            datasetName: "probed.ms",
            requestSummary: "mock"
        )))

        XCTAssertLessThan(snapshot.measurementSetWindow.rowStartFraction, snapshot.measurementSetWindow.rowEndFraction)
        XCTAssertLessThan(snapshot.measurementSetWindow.channelStartFraction, snapshot.measurementSetWindow.channelEndFraction)
        XCTAssertGreaterThan(snapshot.measurementSetWindow.activeRowCount, snapshot.measurementSetWindow.activeChannelCount)
        XCTAssertEqual(snapshot.outputCube.xPixels, 2_048)
        XCTAssertEqual(snapshot.outputCube.yPixels, 2_048)
        XCTAssertEqual(snapshot.outputCube.zPlanes, 1_024)
        XCTAssertEqual(snapshot.outputCube.zAxisDisplayScale, 0.5, accuracy: 0.001)
        XCTAssertTrue(snapshot.outputCube.activeRangeSpansWholeXYPlanes)
        XCTAssertEqual(snapshot.outputCube.activePlaneCount, 256)
        XCTAssertGreaterThan(snapshot.workEstimate.completedUnits, 0)
        XCTAssertGreaterThan(snapshot.workEstimate.totalUnits, snapshot.workEstimate.completedUnits)
        XCTAssertEqual(snapshot.workEstimate.basis, "output-plane midpoint plus upper-bound minor iterations")
        XCTAssertEqual(snapshot.uvCoverage.measured.count, snapshot.uvCoverage.conjugate.count)
        XCTAssertGreaterThan(snapshot.deconvolution.residualHistoryMilliJyPerBeam.count, 3)
        XCTAssertEqual(snapshot.deconvolution.residualHistoryMilliJyPerBeam.last, snapshot.deconvolution.peakResidualMilliJyPerBeam)
        XCTAssertTrue(snapshot.runtime.gpuActive)
    }

    func testOutputCubeDisplayKeepsShallowSpectralSlabsVisible() throws {
        let cube = OutputCubeProgress(
            xPixels: 1_024,
            yPixels: 1_024,
            zPlanes: 64,
            activePlaneStart: 0,
            activePlaneEnd: 32
        )

        XCTAssertEqual(cube.activePlaneCount, 32)
        XCTAssertEqual(cube.activePlaneStartFraction, 0.0, accuracy: 0.001)
        XCTAssertEqual(cube.activePlaneEndFraction, 0.5, accuracy: 0.001)
        XCTAssertEqual(cube.zAxisDisplayScale, 0.32, accuracy: 0.001)
    }

    func testOpenImagerProgressMockupSeedsRunningReviewState() throws {
        let store = WorkbenchStore(
            state: FixtureWorkbench.makeState(),
            taskCatalogClient: StubTaskCatalogClient(tasks: [makeTaskCatalogEntry(id: "imager", displayName: "Imager")]),
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImheadTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.openImagerProgressMockup()

        let snapshot = store.debugSnapshot()
        let progress = try XCTUnwrap(snapshot.taskImagerProgress)
        XCTAssertEqual(snapshot.activeTaskID, "imager")
        XCTAssertEqual(store.state.taskRun.state, .running)
        XCTAssertEqual(store.state.taskRun.progress, progress.workEstimate.fraction, accuracy: 0.001)
        XCTAssertEqual(progress.state, .running)
        XCTAssertTrue(progress.runtime.gpuActive)
        XCTAssertEqual(progress.deconvolution.peakResidualMilliJyPerBeam, 2.7, accuracy: 0.001)
        XCTAssertTrue(
            store.state.taskRun.diagnostics.contains {
                $0.contains("scheduled units")
            }
        )
    }

    func testImagerProgressStderrParserBuildsSnapshotAndDiagnostics() throws {
        var parser = ImagerProgressStderrParser()
        let progressJSON = #"{"schema_version":1,"sequence":2,"elapsed_ms":50,"phase":"reading_ms","summary":"reading rows","work":{"completed_units":3,"total_units":10,"unit_label":"scheduled units","basis":"test","confidence":"coarse"},"ms_read":{"total_rows":100,"total_channels":16,"row_start":20,"row_end":40,"channel_start":4,"channel_end":8},"output_cube":{"x_pixels":64,"y_pixels":64,"z_planes":16,"active_plane_start":4,"active_plane_end":8},"uv_coverage":{"u_extent_klambda":2.0,"v_extent_klambda":3.0,"measured":[{"u_klambda":1.0,"v_klambda":-2.0}],"dropped_points":0,"sample_limit":1},"runtime":{"active_threads":2,"total_threads":8,"gpu_active":false,"backend":"auto","active_resources":["source-stream"],"memory":{"memory_target_bytes":17179869184,"planned_active_bytes":17179863154,"source_stream_buffer_bytes":3804104045,"product_scratch_bytes":10945390173,"active_planes":47,"row_block_rows":128704,"memory_target_source":"system_half"}}}"#
        let line = imagerProgressStderrPrefix + progressJSON + "\nplain stderr\n"

        let records = parser.append(line, runID: "imager-7", state: .running)

        XCTAssertEqual(records.count, 2)
        guard case .progress(let progress) = records[0] else {
            return XCTFail("expected progress record")
        }
        XCTAssertEqual(progress.runID, "imager-7")
        XCTAssertEqual(progress.measurementSetWindow.activeRowStart, 20)
        XCTAssertEqual(progress.outputCube.activePlaneStart, 4)
        XCTAssertEqual(progress.uvCoverage.measured.count, 1)
        XCTAssertEqual(progress.uvCoverage.conjugate.first?.uKilolambda, -1.0)
        XCTAssertEqual(progress.uvCoverage.conjugate.first?.vKilolambda, 2.0)
        XCTAssertEqual(progress.uvCoverage.conjugate.first?.weight, 1.0)
        XCTAssertEqual(progress.uvCoverage.droppedPointCount, 0)
        XCTAssertEqual(progress.uvCoverage.sampleLimit, 1)
        XCTAssertEqual(progress.sampledAtLabel, "0.05 s")
        XCTAssertEqual(progress.runtime.activeResourceIDsAreAuthoritative, true)
        let receivedAt = try XCTUnwrap(progress.receivedAt)
        XCTAssertEqual(progress.elapsedLabel(now: receivedAt.addingTimeInterval(2.0)), "2.05 s")
        XCTAssertEqual(progress.runtime.memory?.activePlanes, 47)
        XCTAssertEqual(progress.runtime.memory?.rowBlockRows, 128704)
        XCTAssertEqual(progress.runtime.memory?.memoryTargetSource, "system_half")
        XCTAssertEqual(progress.runtime.activeResourceIDs, ["source-stream"])
        XCTAssertEqual(progress.resourceActivities, [])
        XCTAssertFalse(progress.sourceStreamIsActive)
        guard case .diagnostic(let diagnostic) = records[1] else {
            return XCTFail("expected diagnostic record")
        }
        XCTAssertEqual(diagnostic, "plain stderr")
    }

    func testImagerProgressUsesObservabilityResourcesWhenPresent() throws {
        var parser = ImagerProgressStderrParser()
        let progressJSON = #"{"schema_version":1,"sequence":5,"elapsed_ms":1250,"phase":"residual_refresh","summary":"refreshing residual","ms_read":{"total_rows":1000,"total_channels":32,"row_start":100,"row_end":300,"channel_start":8,"channel_end":16},"output_cube":{"x_pixels":128,"y_pixels":128,"z_planes":16,"active_plane_start":4,"active_plane_end":8},"runtime":{"active_threads":4,"total_threads":8,"gpu_active":true,"backend":"metal","active_resources":["visibility-grid","plane-state"],"memory":{"memory_target_bytes":17179869184,"planned_active_bytes":12884901888,"source_stream_buffer_bytes":3221225472,"product_scratch_bytes":5368709120,"active_planes":4,"row_block_rows":292000,"memory_target_source":"test"}},"observability":{"schema_version":2,"resources":[{"id":"source-stream","label":"Source Stream","state":"idle","lease_count":0,"active_threads":0,"gpu_active":false,"memory":{"planned_bytes":3221225472,"row_block_rows":292000}},{"id":"visibility-grid","label":"Grid/FFT","state":"active","lease_count":1,"active_threads":4,"gpu_active":true,"owner":"residual_refresh"},{"id":"plane-state","label":"Plane State","state":"active","lease_count":1,"active_threads":4,"gpu_active":true,"owner":"residual_refresh","memory":{"active_planes":4}},{"id":"deconvolver","label":"Deconvolver","state":"idle","lease_count":0,"active_threads":0,"gpu_active":false},{"id":"product-scratch","label":"Products","state":"idle","lease_count":0,"active_threads":0,"gpu_active":false,"memory":{"planned_bytes":5368709120}}],"active_spans":[{"id":"residual_refresh","name":"refreshing residual","stage_kind":"residual_refresh","state":"running","parent_id":"run-1","worker_id":"cpu-compute","resource_ids":["visibility-grid","plane-state"],"expected_resource_ids":["visibility-grid","plane-state","deconvolver"],"extent":{"row_start":100,"row_end":300,"channel_start":8,"channel_end":16,"plane_start":4,"plane_end":8},"counters":{"major_cycle":1,"minor_iterations":25},"elapsed_ms":1250}],"recent_spans":[{"id":"source_stream-1","name":"source stream","stage_kind":"source_stream","state":"complete","resource_ids":["source-stream"],"extent":{"row_start":0,"row_end":100},"elapsed_ms":900}],"memory_target_bytes":17179869184,"memory_target_source":"test","memory_ledger":{"entries":[{"kind":"source-buffer","label":"Source stream","resource_id":"source-stream","planned_bytes":3221225472,"row_block_rows":292000,"confidence":"planned"},{"kind":"grid-fft-scratch","label":"Grid / FFT scratch","resource_id":"visibility-grid","confidence":"unknown","note":"not yet attributed"},{"kind":"plane-state","label":"Plane state","resource_id":"plane-state","active_planes":4,"confidence":"unknown"},{"kind":"deconvolver-scratch","label":"Deconvolver scratch","resource_id":"deconvolver","confidence":"unknown"},{"kind":"products","label":"Products","resource_id":"product-scratch","planned_bytes":5368709120,"confidence":"planned"},{"kind":"allocator-runtime","label":"Allocator runtime","resource_id":"process-runtime","confidence":"unknown","note":"allocator caches and runtime overhead are not yet separately sampled"},{"kind":"process-baseline","label":"Process RSS","resource_id":"process-runtime","process_rss_bytes":10737418240,"process_peak_rss_bytes":12884901888,"confidence":"measured"},{"kind":"untracked-resident","label":"Untracked resident","resource_id":"process-runtime","process_rss_bytes":10737418240,"untracked_bytes":10737418240,"confidence":"estimated"}],"planned_total_bytes":8589934592,"tracked_live_total_bytes":0,"tracked_high_water_total_bytes":0,"process_rss_bytes":10737418240,"process_peak_rss_bytes":12884901888,"untracked_resident_bytes":10737418240},"workers":[{"id":"cpu-compute-visibility-grid","label":"CPU Grid/FFT","state":"running-cpu","resource_id":"visibility-grid","span_id":"residual_refresh","active_count":4,"capacity":8},{"id":"cpu-compute-plane-state","label":"CPU Plane State","state":"running-cpu","resource_id":"plane-state","span_id":"residual_refresh","active_count":4,"capacity":8},{"id":"gpu-submit","label":"GPU submit","state":"gpu-submit","resource_id":"visibility-grid","span_id":"residual_refresh","active_count":1,"capacity":1},{"id":"idle","label":"Idle","state":"idle","active_count":4,"capacity":8}],"queues":[{"id":"source-row-block","label":"Source row block","state":"idle","resource_id":"source-stream","len":0,"capacity":1,"bytes":3221225472,"producers_active":false,"consumers_active":true,"blocked_count":0,"confidence":"estimated"},{"id":"worker-dispatch","label":"Worker dispatch","state":"active","resource_id":"worker-queue","capacity":8,"producers_active":true,"consumers_active":true,"blocked_count":0,"confidence":"unknown"},{"id":"cube-product-publisher","label":"Cube product publisher","state":"active","resource_id":"product-scratch","len":2,"capacity":3,"producers_active":true,"consumers_active":false,"blocked_count":1,"confidence":"measured"}]}}"#

        let records = parser.append(imagerProgressStderrPrefix + progressJSON + "\n", runID: "imager-obs", state: .running)

        guard case .progress(let progress) = records.first else {
            return XCTFail("expected progress record")
        }
        let observability = try XCTUnwrap(progress.observability)
        XCTAssertEqual(observability.schemaVersion, 2)
        XCTAssertEqual(observability.resources.count, 5)
        XCTAssertEqual(observability.activeSpans.first?.stageKind, "residual_refresh")
        XCTAssertEqual(observability.activeSpans.first?.parentID, "run-1")
        XCTAssertEqual(observability.activeSpans.first?.workerID, "cpu-compute")
        XCTAssertEqual(observability.activeSpans.first?.resourceIDs, ["visibility-grid", "plane-state"])
        XCTAssertEqual(observability.activeSpans.first?.expectedResourceIDs, ["visibility-grid", "plane-state", "deconvolver"])
        XCTAssertEqual(observability.activeSpans.first?.extent?.rowStart, 100)
        XCTAssertEqual(observability.activeSpans.first?.extent?.rowEnd, 300)
        XCTAssertEqual(observability.activeSpans.first?.extent?.channelStart, 8)
        XCTAssertEqual(observability.activeSpans.first?.extent?.channelEnd, 16)
        XCTAssertEqual(observability.activeSpans.first?.extent?.planeStart, 4)
        XCTAssertEqual(observability.activeSpans.first?.extent?.planeEnd, 8)
        XCTAssertEqual(observability.activeSpans.first?.counters["major_cycle"], 1)
        XCTAssertEqual(observability.activeSpans.first?.counters["minor_iterations"], 25)
        XCTAssertEqual(observability.recentSpans.first?.stageKind, "source_stream")
        XCTAssertEqual(observability.recentSpans.first?.state, "complete")
        XCTAssertEqual(observability.recentSpans.first?.resourceIDs, ["source-stream"])
        let ledger = try XCTUnwrap(observability.memoryLedger)
        XCTAssertEqual(ledger.entries.count, 8)
        XCTAssertEqual(ledger.plannedTotalBytes, 8_589_934_592)
        XCTAssertEqual(ledger.trackedLiveTotalBytes, 0)
        XCTAssertEqual(ledger.trackedHighWaterTotalBytes, 0)
        XCTAssertNil(ledger.entry(for: "source-stream")?.trackedLiveBytes)
        XCTAssertNil(ledger.entry(for: "source-stream")?.highWaterBytes)
        XCTAssertEqual(ledger.untrackedResidentBytes, 10_737_418_240)
        XCTAssertEqual(ledger.entry(for: "visibility-grid")?.confidence, "unknown")
        XCTAssertEqual(ledger.entries.first { $0.kind == "allocator-runtime" }?.confidence, "unknown")
        XCTAssertEqual(observability.workers.first { $0.id == "cpu-compute-visibility-grid" }?.label, "CPU Grid/FFT")
        XCTAssertEqual(observability.workers.first { $0.id == "cpu-compute-visibility-grid" }?.state, "running-cpu")
        XCTAssertEqual(observability.workers.first { $0.id == "cpu-compute-visibility-grid" }?.spanID, "residual_refresh")
        XCTAssertEqual(observability.workers.first { $0.id == "cpu-compute-plane-state" }?.label, "CPU Plane State")
        XCTAssertEqual(observability.workers.first { $0.id == "cpu-compute-plane-state" }?.resourceID, "plane-state")
        XCTAssertEqual(observability.workers.first { $0.id == "gpu-submit" }?.activeCount, 1)
        XCTAssertEqual(observability.queues.first { $0.id == "source-row-block" }?.bytes, 3_221_225_472)
        XCTAssertEqual(observability.queues.first { $0.id == "source-row-block" }?.state, "idle")
        XCTAssertEqual(observability.queues.first { $0.id == "worker-dispatch" }?.confidence, "unknown")
        XCTAssertEqual(observability.queues.first { $0.id == "cube-product-publisher" }?.blockedCount, 1)
        let sourceResource = try XCTUnwrap(progress.resourceActivities.first { $0.id == "source-stream" })
        XCTAssertEqual(sourceResource.detail, "292.0k rows / 3.2 GB planned / queue idle 0/1 p/C")
        XCTAssertEqual(sourceResource.state, .idle)
        XCTAssertFalse(progress.sourceStreamIsActive)
        let gridResource = try XCTUnwrap(progress.resourceActivities.first { $0.id == "visibility-grid" })
        XCTAssertEqual(gridResource.name, "Grid/FFT")
        XCTAssertEqual(gridResource.detail, "owner=residual_refresh / leases 1")
        XCTAssertEqual(gridResource.state, .active)
        XCTAssertEqual(gridResource.activeThreads, 4)
        XCTAssertTrue(gridResource.gpuActive)
        let planeResource = try XCTUnwrap(progress.resourceActivities.first { $0.id == "plane-state" })
        XCTAssertEqual(planeResource.detail, "4 planes / owner=residual_refresh / leases 1")
        XCTAssertEqual(planeResource.sectionStartFraction, 0.25, accuracy: 0.001)
        XCTAssertEqual(planeResource.sectionEndFraction, 0.5, accuracy: 0.001)
        let productResource = try XCTUnwrap(progress.resourceActivities.first { $0.id == "product-scratch" })
        XCTAssertEqual(productResource.detail, "5.4 GB planned / queue active 2/3 1 blocked P/c")
        XCTAssertEqual(productResource.byteFraction, Double(5_368_709_120) / Double(17_179_869_184), accuracy: 0.001)

        let executionState = try XCTUnwrap(progress.executionStateSummary)
        XCTAssertEqual(executionState.subtitle, "1 current / 1 recent")
        XCTAssertEqual(executionState.currentSpans.first?.elapsedLabel, "1.25 s")
        XCTAssertEqual(executionState.currentSpans.first?.detail, "stage=residual_refresh / owns=visibility-grid, plane-state / expects=visibility-grid, plane-state, deconvolver / rows 100-300, channels 8-16, planes 4-8 / parent=run-1 / worker=cpu-compute / major_cycle=1, minor_iterations=25")
        XCTAssertEqual(executionState.recentSpans.first?.detail, "stage=source_stream / owns=source-stream / rows 0-100")
        XCTAssertEqual(executionState.resourceStates.map(\.label), ["active", "idle"])
        XCTAssertEqual(executionState.resourceStates.map(\.value), ["2", "3"])
        XCTAssertEqual(executionState.memory.first { $0.id == "memory-tracked" }?.value, "0 B")
        XCTAssertEqual(executionState.memory.first { $0.id == "memory-tracked" }?.detail, "8.6 GB planned")
        XCTAssertEqual(executionState.memory.first { $0.id == "memory-target" }?.value, "17.2 GB")
        XCTAssertEqual(executionState.memory.first { $0.id == "memory-rss" }?.detail, "12.9 GB peak")
        XCTAssertEqual(executionState.memory.first { $0.id == "memory-allocator" }?.value, "unknown")
        XCTAssertEqual(executionState.memory.first { $0.id == "memory-allocator" }?.detail, "unknown")
        XCTAssertEqual(executionState.memory.first { $0.id == "memory-untracked" }?.value, "10.7 GB")
        XCTAssertEqual(executionState.workers.first { $0.id == "worker-cpu-compute-visibility-grid" }?.value, "4/8")
        XCTAssertEqual(executionState.workers.first { $0.id == "worker-cpu-compute-visibility-grid" }?.detail, "running-cpu / visibility-grid / residual_refresh")
        XCTAssertEqual(executionState.workers.first { $0.id == "worker-cpu-compute-plane-state" }?.value, "4/8")
        XCTAssertEqual(executionState.workers.first { $0.id == "worker-cpu-compute-plane-state" }?.detail, "running-cpu / plane-state / residual_refresh")
        XCTAssertEqual(executionState.queues.first { $0.id == "queue-source-row-block" }?.value, "0/1 / 3.2 GB")
        XCTAssertEqual(executionState.queues.first { $0.id == "queue-source-row-block" }?.detail, "idle / source-stream / estimated / p/C")
        XCTAssertEqual(executionState.queues.first { $0.id == "queue-cube-product-publisher" }?.value, "2/3 / 1 blocked")
        XCTAssertEqual(executionState.queues.first { $0.id == "queue-cube-product-publisher" }?.detail, "active / product-scratch / measured / P/c")
    }

    func testImagerProgressKeepsLastReadChunkWhenComputeEventCarriesSlabExtent() throws {
        var parser = ImagerProgressStderrParser()
        let readJSON = #"{"schema_version":1,"sequence":3,"elapsed_ms":100,"phase":"reading_ms","summary":"reading rows","ms_read":{"total_rows":1000,"total_channels":32,"row_start":100,"row_end":200,"channel_start":8,"channel_end":16},"output_cube":{"x_pixels":128,"y_pixels":128,"z_planes":16,"active_plane_start":4,"active_plane_end":8},"runtime":{"active_threads":1,"total_threads":8,"gpu_active":false,"backend":"source stream"}}"#
        let computeJSON = #"{"schema_version":1,"sequence":4,"elapsed_ms":200,"phase":"backend_execution","summary":"backend execution for slab","ms_read":{"total_rows":1000,"total_channels":32,"row_start":0,"row_end":1000,"channel_start":8,"channel_end":16},"output_cube":{"x_pixels":128,"y_pixels":128,"z_planes":16,"active_plane_start":4,"active_plane_end":8},"runtime":{"active_threads":4,"total_threads":8,"gpu_active":true,"backend":"metal"}}"#

        _ = parser.append(imagerProgressStderrPrefix + readJSON + "\n", runID: "imager-read-window", state: .running)
        let records = parser.append(imagerProgressStderrPrefix + computeJSON + "\n", runID: "imager-read-window", state: .running)

        guard case .progress(let progress) = records.first else {
            return XCTFail("expected progress record")
        }
        XCTAssertEqual(progress.measurementSetWindow.activeRowStart, 100)
        XCTAssertEqual(progress.measurementSetWindow.activeRowEnd, 200)
        XCTAssertEqual(progress.measurementSetWindow.activeChannelStart, 8)
        XCTAssertEqual(progress.measurementSetWindow.activeChannelEnd, 16)
    }

    func testImagerProgressPreservesFullCubeExtentAcrossPerPlaneEvents() throws {
        var parser = ImagerProgressStderrParser()
        let fullCubeJSON = #"{"schema_version":1,"sequence":1,"elapsed_ms":100,"phase":"reading_ms","summary":"full cube extent","output_cube":{"x_pixels":128,"y_pixels":128,"z_planes":16,"active_plane_start":7,"active_plane_end":14},"runtime":{"active_threads":1,"total_threads":8,"gpu_active":false,"backend":"source"}}"#
        let planeJSON = #"{"schema_version":1,"sequence":2,"elapsed_ms":200,"phase":"forming weighted mosaic groups","summary":"single plane event","output_cube":{"x_pixels":128,"y_pixels":128,"z_planes":1,"active_plane_start":0,"active_plane_end":1},"runtime":{"active_threads":1,"total_threads":8,"gpu_active":true,"backend":"metal minor cycle"}}"#

        _ = parser.append(imagerProgressStderrPrefix + fullCubeJSON + "\n", runID: "imager-cube", state: .running)
        let records = parser.append(imagerProgressStderrPrefix + planeJSON + "\n", runID: "imager-cube", state: .running)

        guard case .progress(let progress) = records.first else {
            return XCTFail("expected progress record")
        }
        XCTAssertEqual(progress.outputCube.zPlanes, 16)
        XCTAssertEqual(progress.outputCube.activePlaneStart, 7)
        XCTAssertEqual(progress.outputCube.activePlaneEnd, 14)
        XCTAssertEqual(progress.outputCube.activeRangeLabel, "Freq planes 7-14 / 16 (7 planes)")
    }

    func testImagerProgressPreservesObservedResourceStates() throws {
        var parser = ImagerProgressStderrParser()
        let progressJSON = #"{"schema_version":1,"sequence":6,"elapsed_ms":2000,"phase":"deconvolving and writing products","summary":"phase text mentions work that should not drive state","runtime":{"active_threads":8,"total_threads":8,"gpu_active":true,"backend":"observed-only"},"observability":{"schema_version":2,"resources":[{"id":"source-stream","label":"Source Stream","state":"retained","lease_count":0,"active_threads":0,"gpu_active":false},{"id":"visibility-grid","label":"Grid/FFT","state":"blocked","lease_count":1,"active_threads":0,"gpu_active":false},{"id":"plane-state","label":"Plane State","state":"unknown","lease_count":0,"active_threads":0,"gpu_active":false},{"id":"deconvolver","label":"Deconvolver","state":"active","lease_count":1,"active_threads":6,"gpu_active":true},{"id":"product-scratch","label":"Products","state":"stale","lease_count":0,"active_threads":0,"gpu_active":false}]}}"#

        let records = parser.append(imagerProgressStderrPrefix + progressJSON + "\n", runID: "imager-observed-states", state: .running)

        guard case .progress(let progress) = records.first else {
            return XCTFail("expected progress record")
        }
        XCTAssertFalse(progress.runtime.activeResourceIDsAreAuthoritative)
        XCTAssertEqual(progress.observability?.schemaVersion, 2)
        let statesByResource = Dictionary(uniqueKeysWithValues: progress.resourceActivities.map { ($0.id, $0.observedState) })
        XCTAssertEqual(statesByResource["source-stream"], "retained")
        XCTAssertEqual(statesByResource["visibility-grid"], "blocked")
        XCTAssertEqual(statesByResource["plane-state"], "unknown")
        XCTAssertEqual(statesByResource["deconvolver"], "active")
        XCTAssertEqual(statesByResource["product-scratch"], "stale")
        let typedStatesByResource = Dictionary(uniqueKeysWithValues: progress.resourceActivities.map { ($0.id, $0.state) })
        XCTAssertEqual(typedStatesByResource["source-stream"], .retained)
        XCTAssertEqual(typedStatesByResource["visibility-grid"], .blocked)
        XCTAssertEqual(typedStatesByResource["plane-state"], .unknown)
        XCTAssertEqual(typedStatesByResource["deconvolver"], .active)
        XCTAssertEqual(typedStatesByResource["product-scratch"], .stale)
        XCTAssertEqual(progress.resourceActivities.filter(\.isBusy).map(\.id), ["deconvolver"])
        XCTAssertEqual(progress.resourceActivities.first { $0.id == "deconvolver" }?.activeThreads, 6)
        XCTAssertTrue(progress.resourceActivities.first { $0.id == "deconvolver" }?.gpuActive ?? false)
        XCTAssertEqual(progress.executionStateSummary?.resourceStates.map(\.label), [
            "active",
            "blocked",
            "retained",
            "stale",
            "unknown"
        ])
    }

    func testImagerProgressDoesNotCarryForwardPreviousObservabilitySnapshot() throws {
        var parser = ImagerProgressStderrParser()
        let observedProgressJSON = #"{"schema_version":1,"sequence":7,"elapsed_ms":2000,"phase":"deconvolving","summary":"active observed resource","runtime":{"active_threads":6,"total_threads":8,"gpu_active":false,"backend":"observed"},"observability":{"schema_version":2,"resources":[{"id":"deconvolver","label":"Deconvolver","state":"active","lease_count":1,"active_threads":6,"gpu_active":false}]}}"#
        let runtimeOnlyProgressJSON = #"{"schema_version":1,"sequence":8,"elapsed_ms":2100,"phase":"finished event without observability","summary":"no current observability","runtime":{"active_threads":0,"total_threads":8,"gpu_active":false,"backend":"runtime-only","active_resources":[]}}"#

        let observedRecords = parser.append(imagerProgressStderrPrefix + observedProgressJSON + "\n", runID: "imager-no-carry", state: .running)
        guard case .progress(let observedProgress) = observedRecords.first else {
            return XCTFail("expected observed progress record")
        }
        XCTAssertEqual(observedProgress.resourceActivities.filter(\.isBusy).map(\.id), ["deconvolver"])
        XCTAssertNotNil(observedProgress.observability)

        let runtimeOnlyRecords = parser.append(imagerProgressStderrPrefix + runtimeOnlyProgressJSON + "\n", runID: "imager-no-carry", state: .running)
        guard case .progress(let runtimeOnlyProgress) = runtimeOnlyRecords.first else {
            return XCTFail("expected runtime-only progress record")
        }
        XCTAssertNil(runtimeOnlyProgress.observability)
        XCTAssertEqual(runtimeOnlyProgress.resourceActivities, [])
        XCTAssertNil(runtimeOnlyProgress.executionStateSummary)
        XCTAssertFalse(runtimeOnlyProgress.sourceStreamIsActive)
    }

    func testImagerProgressIgnoresRuntimeResourceOwnershipWhenNoSnapshotExists() throws {
        var parser = ImagerProgressStderrParser()
        let progressJSON = #"{"schema_version":1,"sequence":3,"elapsed_ms":1500,"phase":"reading_ms","summary":"resource ownership","ms_read":{"total_rows":100,"total_channels":16,"row_start":20,"row_end":40,"channel_start":4,"channel_end":8},"output_cube":{"x_pixels":64,"y_pixels":64,"z_planes":16,"active_plane_start":4,"active_plane_end":8},"runtime":{"active_threads":4,"total_threads":8,"gpu_active":true,"backend":"explicit test","active_resources":["visibility-grid","plane-state","product-scratch"],"memory":{"memory_target_bytes":17179869184,"planned_active_bytes":17179863154,"source_stream_buffer_bytes":3804104045,"product_scratch_bytes":10945390173,"active_planes":4,"row_block_rows":128704,"memory_target_source":"system_half"}}}"#

        let records = parser.append(imagerProgressStderrPrefix + progressJSON + "\n", runID: "imager-8", state: .running)

        guard case .progress(let progress) = records.first else {
            return XCTFail("expected progress record")
        }
        XCTAssertEqual(progress.runtime.activeResourceIDs, ["visibility-grid", "plane-state", "product-scratch"])
        XCTAssertEqual(progress.runtime.activeResourceIDsAreAuthoritative, true)
        XCTAssertEqual(progress.resourceActivities, [])
        XCTAssertFalse(progress.sourceStreamIsActive)

        let finishedRecords = parser.append(imagerProgressStderrPrefix + progressJSON + "\n", runID: "imager-8", state: .succeeded)

        guard case .progress(let finishedProgress) = finishedRecords.first else {
            return XCTFail("expected completed progress record")
        }
        XCTAssertEqual(finishedProgress.resourceActivities, [])
        XCTAssertFalse(finishedProgress.sourceStreamIsActive)
    }

    func testImagerProgressEmptyExplicitResourceOwnershipKeepsRowsIdle() throws {
        var parser = ImagerProgressStderrParser()
        let progressJSON = #"{"schema_version":1,"sequence":5,"elapsed_ms":3000,"phase":"refreshing residual","summary":"idle after residual refresh","output_cube":{"x_pixels":64,"y_pixels":64,"z_planes":1,"active_plane_start":0,"active_plane_end":1},"deconvolution":{"phase":"refreshing residual","major_cycle":1,"major_cycle_limit":-1,"minor_iterations":1000,"minor_iteration_limit":3000,"components_cleaned":1000,"peak_residual_mjy_per_beam":2.7,"target_residual_mjy_per_beam":0.0,"residual_history_mjy_per_beam":[3.1,2.7]},"runtime":{"active_threads":0,"total_threads":8,"gpu_active":false,"backend":"idle","active_resources":[],"memory":{"memory_target_bytes":17179869184,"planned_active_bytes":17179863154,"source_stream_buffer_bytes":3804104045,"product_scratch_bytes":10945390173,"active_planes":1,"row_block_rows":128704,"memory_target_source":"system_half"}}}"#

        let records = parser.append(imagerProgressStderrPrefix + progressJSON + "\n", runID: "imager-10", state: .running)

        guard case .progress(let progress) = records.first else {
            return XCTFail("expected progress record")
        }
        XCTAssertEqual(progress.runtime.activeResourceIDs, [])
        XCTAssertEqual(progress.runtime.activeResourceIDsAreAuthoritative, true)
        XCTAssertEqual(progress.resourceActivities, [])
        XCTAssertFalse(progress.sourceStreamIsActive)
    }

    func testImagerProgressDoesNotInferResourcesFromPhaseText() throws {
        var parser = ImagerProgressStderrParser()
        let progressJSON = #"{"schema_version":1,"sequence":6,"elapsed_ms":3500,"phase":"refreshing residual and deconvolving minor cycle","summary":"phase text mentions every old heuristic keyword","output_cube":{"x_pixels":64,"y_pixels":64,"z_planes":1,"active_plane_start":0,"active_plane_end":1},"deconvolution":{"phase":"minor cycle cleaning","major_cycle":1,"major_cycle_limit":-1,"minor_iterations":1000,"minor_iteration_limit":3000,"components_cleaned":1000,"peak_residual_mjy_per_beam":2.7,"target_residual_mjy_per_beam":0.0,"residual_history_mjy_per_beam":[3.1,2.7]},"runtime":{"active_threads":8,"total_threads":8,"gpu_active":true,"backend":"phase-only","memory":{"memory_target_bytes":17179869184,"planned_active_bytes":17179863154,"source_stream_buffer_bytes":3804104045,"product_scratch_bytes":10945390173,"active_planes":1,"row_block_rows":128704,"memory_target_source":"system_half"}}}"#

        let records = parser.append(imagerProgressStderrPrefix + progressJSON + "\n", runID: "imager-no-phase", state: .running)

        guard case .progress(let progress) = records.first else {
            return XCTFail("expected progress record")
        }
        XCTAssertFalse(progress.runtime.activeResourceIDsAreAuthoritative)
        XCTAssertEqual(progress.resourceActivities, [])
        XCTAssertFalse(progress.sourceStreamIsActive)
    }

    func testImagerProgressNestedReplaySourceReadShowsCombinedResourceOwnership() throws {
        var parser = ImagerProgressStderrParser()
        let progressJSON = #"{"schema_version":1,"sequence":4,"elapsed_ms":2750,"phase":"reading_ms","summary":"reading rows during residual replay","ms_read":{"total_rows":100,"total_channels":16,"row_start":20,"row_end":40,"channel_start":4,"channel_end":8},"output_cube":{"x_pixels":64,"y_pixels":64,"z_planes":1,"active_plane_start":0,"active_plane_end":1},"deconvolution":{"phase":"refreshing residual","major_cycle":1,"major_cycle_limit":-1,"minor_iterations":1000,"minor_iteration_limit":3000,"components_cleaned":1000,"peak_residual_mjy_per_beam":2.7,"target_residual_mjy_per_beam":0.0,"residual_history_mjy_per_beam":[3.1,2.7]},"runtime":{"active_threads":1,"total_threads":8,"gpu_active":false,"backend":"source stream","active_resources":["source-stream","visibility-grid","plane-state"],"active_resource_threads":{"source-stream":1,"visibility-grid":4,"plane-state":4},"memory":{"memory_target_bytes":17179869184,"planned_active_bytes":17179863154,"source_stream_buffer_bytes":3804104045,"product_scratch_bytes":10945390173,"active_planes":1,"row_block_rows":128704,"memory_target_source":"system_half"}}}"#

        let records = parser.append(imagerProgressStderrPrefix + progressJSON + "\n", runID: "imager-9", state: .running)

        guard case .progress(let progress) = records.first else {
            return XCTFail("expected progress record")
        }
        XCTAssertEqual(progress.runtime.activeResourceIDs, ["source-stream", "visibility-grid", "plane-state"])
        XCTAssertEqual(progress.runtime.activeResourceThreadCounts["source-stream"], 1)
        XCTAssertEqual(progress.runtime.activeResourceThreadCounts["visibility-grid"], 4)
        XCTAssertEqual(progress.runtime.activeResourceThreadCounts["plane-state"], 4)
        XCTAssertEqual(progress.resourceActivities, [])
        XCTAssertFalse(progress.sourceStreamIsActive)
    }

    func testOpenImagerTaskDoesNotSeedMockProgressBeforeRun() throws {
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
            taskCatalogClient: StubTaskCatalogClient(tasks: [makeImagerTaskCatalogEntry()]),
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImagerTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.openProject(path: "/data")
        store.openImagerTaskForSelectedDataset()

        let snapshot = store.debugSnapshot()
        XCTAssertEqual(snapshot.activeTaskID, "imager")
        XCTAssertNil(snapshot.taskImagerProgress)
        XCTAssertEqual(store.state.taskRun.state, .idle)
        XCTAssertEqual(store.state.genericTaskValues["imager"]?["vis"], "probed.ms")
    }

    func testOpenImagerTaskForImageOpensUnboundSchemaTask() throws {
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
            ),
            taskCatalogClient: StubTaskCatalogClient(tasks: [makeImagerTaskCatalogEntry()]),
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImagerTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.openProject(path: "/data")
        store.selectDataset(imageDataset.id)
        store.openImagerTaskForSelectedDataset()

        XCTAssertEqual(store.state.selectedDatasetID, imageDataset.id)
        XCTAssertEqual(store.state.activeTaskID, "imager")
        XCTAssertEqual(store.state.tabs.first(where: { $0.kind == .task })?.title, "Imager")
        XCTAssertNil(store.state.genericTaskValues["imager"]?["vis"])
        XCTAssertFalse(store.state.lastErrors.contains("Dataset output.image is not a MeasurementSet"))
    }

    func testDirectMeasurementSetLaunchConfiguresFullMosaicSchemaRun() throws {
        let probedDataset = DatasetSummary(
            id: "exact-large-ms",
            name: "large.ms",
            path: "/data/large.ms",
            kind: .measurementSet,
            size: "123456 rows, 2 fields, 3 spw, 4 antennas",
            units: "Jy, Hz, seconds",
            sizeBytes: 34_359_738_368,
            fields: ["0: science", "4: phasecenter"],
            spectralWindows: ["spw 0: 1024 chan, 1.420405 GHz center"],
            antennas: ["ea01", "ea02", "ea03", "ea04"],
            columns: ["UVW", "DATA", "FLAG", "FIELD_ID"],
            dataColumns: ["DATA"],
            subtables: ["ANTENNA (required)", "FIELD (required)", "SPECTRAL_WINDOW (required)"],
            shape: [123456],
            notes: "Recognized by opening the path as a MeasurementSet."
        )
        let probeClient = RecordingDirectMeasurementSetProbeClient(probedDataset: probedDataset)
        let store = WorkbenchStore(
            probeClient: probeClient,
            taskCatalogClient: StubTaskCatalogClient(tasks: [makeImagerTaskCatalogEntry()]),
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImagerTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.openExternalMeasurementSetForImaging(path: "/data/large.ms")
        store.refreshProjectFromDiskIfNeeded(now: Date(timeIntervalSince1970: 2))

        let dataset = try XCTUnwrap(store.state.selectedDataset)
        let values = try XCTUnwrap(
            store.state.genericTaskValues["imager"],
            "parameter session errors: \(store.state.lastErrors)"
        )
        let toggles = try XCTUnwrap(store.state.genericTaskToggles["imager"])
        XCTAssertEqual(store.state.project.source, .directMeasurementSet)
        XCTAssertEqual(probeClient.projectProbeCount, 0)
        XCTAssertEqual(probeClient.pathProbeCount, 1)
        XCTAssertEqual(probeClient.probedPaths, ["/data/large.ms"])
        XCTAssertEqual(dataset.id, "exact-large-ms")
        XCTAssertEqual(dataset.size, "123456 rows, 2 fields, 3 spw, 4 antennas")
        XCTAssertEqual(dataset.sizeBytes, 34_359_738_368)
        XCTAssertEqual(dataset.fields, ["0: science", "4: phasecenter"])
        XCTAssertEqual(dataset.spectralWindows, ["spw 0: 1024 chan, 1.420405 GHz center"])
        XCTAssertEqual(dataset.antennas, ["ea01", "ea02", "ea03", "ea04"])
        XCTAssertEqual(dataset.subtables, ["ANTENNA (required)", "FIELD (required)", "SPECTRAL_WINDOW (required)"])
        XCTAssertTrue(dataset.notes.contains("parent project probe skipped"))
        XCTAssertEqual(store.state.tabs.first(where: { $0.kind == .task })?.title, "Imager: large.ms")
        XCTAssertEqual(values["vis"], "large.ms")
        XCTAssertEqual(values["imagename"], "casa-rs-runs/imager-1/large.ms-imager")
        XCTAssertEqual(values["field"], "none")
        XCTAssertEqual(values["phasecenter_field"], "0")
        XCTAssertEqual(values["specmode"], "cube")
        XCTAssertEqual(values["gridder"], "mosaic")
        XCTAssertEqual(values["interpolation"], "nearest")
        XCTAssertEqual(values["channel_start"], "0")
        XCTAssertEqual(values["channel_count"], "512")
        XCTAssertEqual(values["imsize"], "1024,1024")
        XCTAssertEqual(values["cell"], "1arcsec,1arcsec")
        XCTAssertEqual(values["weighting"], "briggs")
        XCTAssertEqual(values["niter"], "2048")
        XCTAssertEqual(values["threshold"], "0Jy")
        XCTAssertEqual(toggles["dirty_only"], false)
        XCTAssertEqual(toggles["perchanweightdensity"], true)
        XCTAssertEqual(toggles["write_pb"], true)
        XCTAssertEqual(toggles["pbcor"], true)
    }

    func testSchemaDrivenImagerRunUsesGenericTaskClientAndRecordsProducts() throws {
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
        let taskClient = StubGenericTaskClient()
        taskClient.stdout = try makeManagedImagerStdout(
            measurementSet: "probed.ms",
            imagename: "casa-rs-runs/output"
        )
        let store = WorkbenchStore(
            probeClient: probeClient,
            genericTaskClient: taskClient,
            taskCatalogClient: StubTaskCatalogClient(tasks: [makeImagerTaskCatalogEntry()]),
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImagerTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.openProject(path: "/data")
        store.openImagerTaskForSelectedDataset()
        store.selectTask("imager")
        store.setGenericTaskConfirmation(taskID: "imager", confirmed: true)
        store.setGenericTaskValue(taskID: "imager", argumentID: "imagename", value: "casa-rs-runs/output")
        store.setGenericTaskValue(taskID: "imager", argumentID: "imsize", value: "256")
        store.setGenericTaskValue(taskID: "imager", argumentID: "cell", value: "0.25arcsec")
        store.setGenericTaskValue(taskID: "imager", argumentID: "weighting", value: "briggs")
        store.setGenericTaskValue(taskID: "imager", argumentID: "channel_start", value: "2")
        store.setGenericTaskValue(taskID: "imager", argumentID: "channel_count", value: "4")
        store.runTask()

        XCTAssertEqual(taskClient.requests.count, 1)
        let arguments = try ProcessGenericTaskClient.arguments(for: taskClient.requests[0])
        XCTAssertTrue(arguments.contains("--managed-output"))
        XCTAssertTrue(arguments.contains("--progress"))
        XCTAssertTrue(arguments.contains("--progress-max-uv-points"))
        XCTAssertTrue(arguments.contains("16384"))
        XCTAssertTrue(arguments.contains("--ms"))
        XCTAssertTrue(arguments.contains("probed.ms"))
        waitFor("imager completion") {
            store.debugSnapshot().taskState == .succeeded
        }

        let snapshot = store.debugSnapshot()
        XCTAssertEqual(snapshot.taskState, .succeeded)
        XCTAssertTrue(snapshot.taskOutputPaths.contains("/data/casa-rs-runs/output.image"))
        XCTAssertTrue(snapshot.processingHistoryEvents.contains("imager completed"))
        let producedDataset = store.state.project.datasets.first { $0.path == "/data/casa-rs-runs/output.image" }
        XCTAssertEqual(producedDataset?.kind, .imageCube)
        XCTAssertEqual(producedDataset?.size, "256 x 256")
        XCTAssertEqual(producedDataset?.units, "float32")
        XCTAssertEqual(producedDataset?.shape, [256, 256])
        let runID = try XCTUnwrap(store.state.taskRun.runID)
        XCTAssertEqual(snapshot.runProductGroups.count, 1)
        XCTAssertEqual(snapshot.runProductGroups.first?.runID, runID)
        XCTAssertEqual(snapshot.runProductGroups.first?.sourceDatasetID, probedDataset.id)
        XCTAssertEqual(snapshot.runProductGroups.first?.products.first?.label, "Image")
        XCTAssertEqual(snapshot.runProductGroups.first?.products.first?.datasetID, producedDataset?.id)
        XCTAssertTrue(snapshot.processingHistoryEvents.contains("imager completed"))

        let productID = try XCTUnwrap(store.state.runProductGroups.first?.products.first?.id)
        store.openRunProduct(runID: runID, productID: productID)
        XCTAssertEqual(store.state.selectedDatasetID, producedDataset?.id)
        XCTAssertEqual(store.state.tabs.first { $0.id == store.state.activeTabID }?.title, "Image: output.image")
        XCTAssertNoThrow(try store.debugJSON())
    }

    func testSchemaDrivenImagerProgressEventUpdatesRunningTaskSnapshot() throws {
        let probedDataset = DatasetSummary(
            id: "/data/probed.ms",
            name: "probed.ms",
            path: "/data/probed.ms",
            kind: .measurementSet,
            size: "12 rows, 1 fields, 1 spw, 2 antennas",
            units: "Jy, Hz, seconds",
            fields: ["0: Target"],
            spectralWindows: ["spw 0: 4 chan, 1.420000 GHz center"],
            correlations: ["XX", "YY"],
            dataColumns: ["DATA"],
            notes: "Recognized by Rust probe."
        )
        let client = HoldingGenericTaskClient()
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
            genericTaskClient: client,
            taskCatalogClient: StubTaskCatalogClient(tasks: [makeImagerTaskCatalogEntry()]),
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImagerTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.openProject(path: "/data")
        store.openImagerTaskForSelectedDataset()
        store.selectTask("imager")
        store.setGenericTaskConfirmation(taskID: "imager", confirmed: true)
        store.runTask()

        let runID = try XCTUnwrap(store.state.taskRun.runID)
        var progress = ImagerProgressSnapshot.stub(request: ImagerProgressRequest(
            taskID: "imager",
            runID: runID,
            taskState: .running,
            progress: 0.25,
            datasetName: "probed.ms",
            requestSummary: "test"
        ))
        progress.phase = "reading live test rows"
        progress.summary = "live test progress"
        client.emitProgress(progress)

        waitFor("imager progress") {
            store.debugSnapshot().taskImagerProgress?.phase == progress.phase
        }
        XCTAssertEqual(store.state.taskRun.progress, progress.workEstimate.fraction, accuracy: 0.001)
        XCTAssertEqual(store.state.jobs[runID]?.lastEvent, progress.phase)

        store.stopTask()

        let cancelledSnapshot = store.debugSnapshot()
        let retainedProgress = try XCTUnwrap(cancelledSnapshot.taskImagerProgress)
        XCTAssertEqual(client.execution.didCancel, true)
        XCTAssertEqual(cancelledSnapshot.taskState, .cancelled)
        XCTAssertEqual(cancelledSnapshot.jobs.first?.status, .cancelled)
        XCTAssertEqual(retainedProgress.phase, progress.phase)
        XCTAssertEqual(retainedProgress.state, .cancelled)
        XCTAssertLessThan(store.state.taskRun.progress, 1.0)
        XCTAssertLessThan(cancelledSnapshot.jobs.first?.progress ?? 1.0, 1.0)
    }

    func testSucceededImagerRunKeepsFinalLiveProgressSnapshot() throws {
        let probedDataset = DatasetSummary(
            id: "/data/probed.ms",
            name: "probed.ms",
            path: "/data/probed.ms",
            kind: .measurementSet,
            size: "12 rows, 1 fields, 1 spw, 2 antennas",
            units: "Jy, Hz, seconds",
            fields: ["0: Target"],
            spectralWindows: ["spw 0: 4 chan, 1.420000 GHz center"],
            correlations: ["XX", "YY"],
            dataColumns: ["DATA"],
            notes: "Recognized by Rust probe."
        )
        let client = HoldingGenericTaskClient()
        client.stdout = try makeManagedImagerStdout(
            measurementSet: "probed.ms",
            imagename: "casa-rs-runs/output"
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
            genericTaskClient: client,
            taskCatalogClient: StubTaskCatalogClient(tasks: [makeImagerTaskCatalogEntry()]),
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImagerTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.openProject(path: "/data")
        store.openImagerTaskForSelectedDataset()
        store.selectTask("imager")
        store.setGenericTaskConfirmation(taskID: "imager", confirmed: true)
        store.runTask()

        let runID = try XCTUnwrap(store.state.taskRun.runID)
        var progress = ImagerProgressSnapshot.stub(request: ImagerProgressRequest(
            taskID: "imager",
            runID: runID,
            taskState: .running,
            progress: 0.25,
            datasetName: "probed.ms",
            requestSummary: "test"
        ))
        progress.phase = "cleaning cube plane"
        progress.summary = "live progress should remain visible"
        progress.runtime.gpuActive = true
        client.emitProgress(progress)

        waitFor("imager progress before success") {
            store.debugSnapshot().taskImagerProgress?.phase == progress.phase
        }

        try client.emitSucceeded()
        waitFor("imager success") {
            store.debugSnapshot().taskState == .succeeded
        }

        let snapshot = store.debugSnapshot()
        let retainedProgress = try XCTUnwrap(snapshot.taskImagerProgress)
        XCTAssertEqual(snapshot.taskState, .succeeded)
        XCTAssertEqual(retainedProgress.runID, runID)
        XCTAssertEqual(retainedProgress.phase, progress.phase)
        XCTAssertEqual(retainedProgress.summary, progress.summary)
        XCTAssertEqual(retainedProgress.state, .succeeded)
        XCTAssertTrue(retainedProgress.runtime.gpuActive)
    }

    func testBundledSampleImagerDefaultsChooseLineTarget() throws {
        let first = DatasetSummary(
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
                        datasets: [first],
                        source: .probed
                    ),
                    diagnostics: []
                )
            ),
            taskCatalogClient: StubTaskCatalogClient(tasks: [makeImagerTaskCatalogEntry()]),
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImagerTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.openProject(path: "/data")
        store.openImagerTaskForSelectedDataset()

        let values = try XCTUnwrap(
            store.state.genericTaskValues["imager"],
            "parameter session errors: \(store.state.lastErrors)"
        )
        XCTAssertEqual(values["field"], "5")
        XCTAssertEqual(values["phasecenter_field"], "none")
        XCTAssertEqual(values["spw"], "5")
        XCTAssertEqual(values["polarization"], "YY")
    }

    func testTWHyaTutorialImagerDefaultsUseKnownMFSParameters() throws {
        let tutorial = DatasetSummary(
            id: "/data/twhya_calibrated.ms",
            name: "twhya_calibrated.ms",
            path: "/data/twhya_calibrated.ms",
            kind: .measurementSet,
            size: "44772 selected rows, 1 spw",
            units: "Jy, Hz, seconds",
            fields: [
                "0: J1037-295",
                "1: Ceres",
                "2: J1058+0133",
                "3: J1107-4449",
                "4: J1132-5606",
                "5: TW Hya"
            ],
            spectralWindows: [
                "spw 0: 384 chan, 372.533086 GHz center"
            ],
            correlations: ["XX", "YY"],
            dataColumns: ["DATA"],
            notes: "ALMA First Look TW Hya tutorial MeasurementSet."
        )
        let store = WorkbenchStore(
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(
                        name: "TW Hya Tutorial",
                        rootPath: "/data",
                        datasets: [tutorial],
                        source: .probed
                    ),
                    diagnostics: []
                )
            ),
            taskCatalogClient: StubTaskCatalogClient(tasks: [makeImagerTaskCatalogEntry()]),
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImagerTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.openProject(path: "/data")
        store.openImagerTaskForSelectedDataset()

        let values = try XCTUnwrap(store.state.genericTaskValues["imager"])
        XCTAssertEqual(values["field"], "5")
        XCTAssertEqual(values["phasecenter_field"], "none")
        XCTAssertEqual(values["spw"], "0")
        XCTAssertEqual(values["polarization"], "I")
        XCTAssertEqual(values["imsize"], "250,250")
        XCTAssertEqual(values["cell"], "0.1arcsec,0.1arcsec")
        XCTAssertEqual(values["specmode"], "mfs")
        XCTAssertEqual(values["weighting"], "briggs")
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
        XCTAssertEqual(snapshot.measurementSetPlots[probedDataset.id]?.plotDocumentLayerCount, 1)
        XCTAssertEqual(snapshot.measurementSetPlots[probedDataset.id]?.plotDocumentPanelCount, 0)
        XCTAssertEqual(snapshot.measurementSetPlots[probedDataset.id]?.plotDocumentPayloadStrategies, ["inlineDisplayPoints"])
        XCTAssertEqual(plotClient.requests.last?.preset, .uvCoverage)
        XCTAssertNil(plotClient.requests.last?.field)
        XCTAssertNil(plotClient.requests.last?.spectralWindow)
        XCTAssertEqual(plotClient.requests.last?.maxPlotPoints, 10_000_000)
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

        store.setMeasurementSetPlotMaxPoints(100_000, datasetID: probedDataset.id)

        snapshot = store.debugSnapshot()
        XCTAssertEqual(snapshot.measurementSetPlots[probedDataset.id]?.maxPlotPoints, 100_000)
        XCTAssertEqual(snapshot.measurementSetPlots[probedDataset.id]?.status, .idle)
        XCTAssertNil(snapshot.measurementSetPlots[probedDataset.id]?.resultPreset)

        store.runMeasurementSetPlot(datasetID: probedDataset.id)
        waitFor("budgeted plot job to finish") {
            store.debugSnapshot().measurementSetPlots[probedDataset.id]?.status == .ready
        }

        XCTAssertEqual(plotClient.requests.count, 3)
        XCTAssertEqual(plotClient.requests.last?.maxPlotPoints, 100_000)

        store.setMeasurementSetPlotPreset(.amplitudeVsChannel, datasetID: probedDataset.id)
        store.setMeasurementSetPlotSpectralWindow("spw 0: 4 chan, 1.420000 GHz center", datasetID: probedDataset.id)
        store.setMeasurementSetPlotChannelSelection("1~3", datasetID: probedDataset.id)
        store.setMeasurementSetPlotTimerange(">2024/01/01/00:00:00", datasetID: probedDataset.id)
        store.setMeasurementSetPlotUVRange("0~1klambda", datasetID: probedDataset.id)
        store.setMeasurementSetPlotAntenna("ea01&ea02", datasetID: probedDataset.id)
        store.setMeasurementSetPlotScan("1", datasetID: probedDataset.id)
        store.setMeasurementSetPlotArray("0", datasetID: probedDataset.id)
        store.setMeasurementSetPlotObservation("0", datasetID: probedDataset.id)
        store.setMeasurementSetPlotIntent("*OBSERVE_TARGET*", datasetID: probedDataset.id)
        store.setMeasurementSetPlotFeed("0", datasetID: probedDataset.id)
        store.setMeasurementSetPlotMSSelect("ANTENNA1 != ANTENNA2", datasetID: probedDataset.id)
        store.setMeasurementSetPlotAvgChannel(2, datasetID: probedDataset.id)
        store.setMeasurementSetPlotAvgTime(30.5, datasetID: probedDataset.id)
        store.setMeasurementSetPlotAvgScan(true, datasetID: probedDataset.id)
        store.setMeasurementSetPlotAvgField(true, datasetID: probedDataset.id)
        store.setMeasurementSetPlotAvgBaseline(true, datasetID: probedDataset.id)
        store.setMeasurementSetPlotAvgAntenna(true, datasetID: probedDataset.id)
        store.setMeasurementSetPlotAvgSPW(true, datasetID: probedDataset.id)
        store.setMeasurementSetPlotScalarAverage(true, datasetID: probedDataset.id)

        store.runMeasurementSetPlot(datasetID: probedDataset.id)
        waitFor("fully selected plot job to finish") {
            store.debugSnapshot().measurementSetPlots[probedDataset.id]?.resultPreset == .amplitudeVsChannel
        }

        XCTAssertEqual(plotClient.requests.count, 4)
        XCTAssertEqual(plotClient.requests.last?.spectralWindow, "0:1~3")
        XCTAssertEqual(plotClient.requests.last?.timerange, ">2024/01/01/00:00:00")
        XCTAssertEqual(plotClient.requests.last?.uvRange, "0~1klambda")
        XCTAssertEqual(plotClient.requests.last?.antenna, "ea01&ea02")
        XCTAssertEqual(plotClient.requests.last?.scan, "1")
        XCTAssertEqual(plotClient.requests.last?.array, "0")
        XCTAssertEqual(plotClient.requests.last?.observation, "0")
        XCTAssertEqual(plotClient.requests.last?.intent, "*OBSERVE_TARGET*")
        XCTAssertEqual(plotClient.requests.last?.feed, "0")
        XCTAssertEqual(plotClient.requests.last?.msselect, "ANTENNA1 != ANTENNA2")
        XCTAssertEqual(plotClient.requests.last?.avgChannel, 2)
        XCTAssertEqual(plotClient.requests.last?.avgTime, 30.5)
        XCTAssertEqual(plotClient.requests.last?.avgScan, true)
        XCTAssertEqual(plotClient.requests.last?.avgField, true)
        XCTAssertEqual(plotClient.requests.last?.avgBaseline, true)
        XCTAssertEqual(plotClient.requests.last?.avgAntenna, true)
        XCTAssertEqual(plotClient.requests.last?.avgSPW, true)
        XCTAssertEqual(plotClient.requests.last?.scalarAverage, true)

        store.setMeasurementSetPlotMaxPoints(1, datasetID: probedDataset.id)

        snapshot = store.debugSnapshot()
        XCTAssertEqual(
            snapshot.measurementSetPlots[probedDataset.id]?.maxPlotPoints,
            WorkbenchState.minimumMeasurementSetPlotMaxPoints
        )

        store.setMeasurementSetPlotMaxPoints(12_000_000, datasetID: probedDataset.id)

        snapshot = store.debugSnapshot()
        XCTAssertEqual(snapshot.measurementSetPlots[probedDataset.id]?.maxPlotPoints, 12_000_000)
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

    func testCancellingGenericImagerJobIsScopedToThatJob() throws {
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
        let taskClient = HoldingGenericTaskClient()
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
            genericTaskClient: taskClient
        )

        store.openProject(path: "/data")
        store.openDefaultTab(kind: .task)
        store.selectTask("imager")
        store.setGenericTaskConfirmation(taskID: "imager", confirmed: true)
        store.runTask()

        XCTAssertNotNil(
            store.state.taskRun.runID,
            "task did not start: errors=\(store.state.lastErrors) diagnostics=\(store.state.taskRun.diagnostics)"
        )
        let runID = tryUnwrap(store.state.taskRun.runID)
        XCTAssertEqual(store.debugSnapshot().runningJobCount, 1)
        XCTAssertEqual(store.debugSnapshot().jobs.first?.kind, .genericTask)
        XCTAssertEqual(store.debugSnapshot().jobs.first?.status, .running)

        store.stopTask()

        let snapshot = store.debugSnapshot()
        XCTAssertEqual(taskClient.execution.didCancel, true)
        XCTAssertEqual(snapshot.runningJobCount, 0)
        XCTAssertEqual(snapshot.jobs.first?.id, runID)
        XCTAssertEqual(snapshot.jobs.first?.status, .cancelled)
        XCTAssertEqual(snapshot.jobs.first?.cancellationRequested, true)
        XCTAssertLessThan(snapshot.jobs.first?.progress ?? 1.0, 1.0)
        XCTAssertEqual(snapshot.taskState, .cancelled)
        XCTAssertLessThan(store.state.taskRun.progress, 1.0)
        XCTAssertTrue(snapshot.activeJobIDsByTab.isEmpty)

        try taskClient.emitSucceeded()
        XCTAssertEqual(store.debugSnapshot().jobs.first?.status, .cancelled)
        XCTAssertEqual(store.debugSnapshot().taskState, .cancelled)
    }

    func testInterfaceFontSizeIsAdjustableClampedAndPreservedAcrossFixtureOpen() {
        let store = WorkbenchStore(
            state: FixtureWorkbench.makeState(),
            demoProjectClient: StubDemoProjectClient(result: makeDemoProjectProbe(rootPath: "/tmp/tutorial-demo"))
        )

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

    func testOpenDemoProjectStagesRealTutorialProjectAndCleansItUpWhenReplaced() {
        let demoClient = StubDemoProjectClient(result: makeDemoProjectProbe(rootPath: "/tmp/tutorial-demo"))
        let replacementDataset = DatasetSummary(
            id: "/data/replacement.ms",
            name: "replacement.ms",
            path: "/data/replacement.ms",
            kind: .measurementSet,
            size: "4 rows",
            units: "Jy",
            dataColumns: ["DATA"],
            notes: "Replacement real project."
        )
        let store = WorkbenchStore(
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(name: "Replacement", rootPath: "/data", datasets: [replacementDataset], source: .probed),
                    diagnostics: []
                )
            ),
            demoProjectClient: demoClient
        )

        store.openFixtureProject()

        let snapshot = store.debugSnapshot()
        XCTAssertEqual(snapshot.activeProject, "TW Hya Tutorial Demo")
        XCTAssertEqual(snapshot.activeProjectRoot, "/tmp/tutorial-demo")
        XCTAssertEqual(snapshot.activeProjectSource, ProjectSource.probed)
        XCTAssertFalse(store.state.isDemoProject)
        XCTAssertEqual(snapshot.selectedDataset, "twhya_calibrated.ms")
        XCTAssertEqual(snapshot.discoveredDatasets, ["twhya_calibrated.ms", "twhya_cont.image", "twhya_calibrated_ANTENNA.table"])
        XCTAssertEqual(snapshot.openTabs.first, "MS: twhya_calibrated.ms")
        XCTAssertTrue(snapshot.probeDiagnostics.contains("Staged tutorial dataset: alma/first-look/twhya/calibrated-ms"))

        store.openProject(path: "/data")

        XCTAssertEqual(demoClient.cleanedRoots, ["/tmp/tutorial-demo"])
        XCTAssertEqual(store.debugSnapshot().activeProject, "Replacement")
    }

    func testPlotImageCacheIDTracksFullImageBytes() {
        let first = makePlotResult(imageBytes: Data([0x89, 0x50, 1, 2, 3, 0x0a]))
        let second = makePlotResult(imageBytes: Data([0x89, 0x50, 1, 9, 3, 0x0a]))

        XCTAssertNotEqual(first.imageCacheID, second.imageCacheID)
    }

    func testGenericImagerProcessClientDrainsLargeStdoutWhileWaitingForExit() throws {
        let tempRoot = URL(fileURLWithPath: NSTemporaryDirectory(), isDirectory: true)
            .appendingPathComponent("casars-mac-process-client-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tempRoot, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempRoot) }

        let resultURL = tempRoot.appendingPathComponent("result.json")
        let outputPrefix = tempRoot
            .appendingPathComponent("outputs", isDirectory: true)
            .appendingPathComponent("large-stdout")
            .path
        let largeWarning = String(repeating: "x", count: 80_000)
        let resultJSON = try makeManagedImagerStdout(
            measurementSet: "/data/probed.ms",
            imagename: outputPrefix,
            warning: largeWarning
        )
        try resultJSON.write(to: resultURL, atomically: true, encoding: .utf8)

        let helperURL = tempRoot.appendingPathComponent("fake-casars-imager")
        let helperScript = """
        #!/bin/sh
        set -eu
        progress_file=""
        while [ "$#" -gt 0 ]; do
          if [ "$1" = "--progress-jsonl" ]; then
            shift
            progress_file="$1"
          fi
          shift || true
        done
        if [ -n "$progress_file" ]; then
          mkdir -p "$(dirname "$progress_file")"
          printf '%s\\n' '{"schema_version":1,"sequence":1,"elapsed_ms":0,"phase":"starting","summary":"started","work":{"completed_units":0,"total_units":1,"unit_label":"unit","basis":"test","confidence":"exact"},"runtime":{"active_threads":1,"total_threads":1,"gpu_active":false,"backend":"test"}}' > "$progress_file"
        fi
        cat "\(resultURL.path)"
        """
        try helperScript.write(to: helperURL, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: helperURL.path)

        var task = makeImagerTaskCatalogEntry()
        task.binaryName = helperURL.path
        let request = GenericTaskRequest(
            runID: "large-stdout",
            task: task,
            providerInvocation: SurfaceProviderInvocation(
                args: ["--vis", "/data/probed.ms", "--imagename", outputPrefix]
            )
        )
        let client = ProcessGenericTaskClient(
            queue: DispatchQueue(label: "casars.mac.test.large-stdout")
        )
        let lock = NSLock()
        var succeededResult: GenericTaskResult?
        var failedFailure: GenericTaskFailure?
        var progressSnapshot: ImagerProgressSnapshot?

        _ = try client.startTask(request: request) { event in
            lock.lock()
            switch event {
            case .progress(let progress):
                progressSnapshot = progress
            case .succeeded(let result):
                succeededResult = result
            case .failed(let failure), .cancelled(let failure):
                failedFailure = failure
            }
            lock.unlock()
        }

        waitFor("large stdout imager process completion", timeout: 5) {
            lock.lock()
            defer { lock.unlock() }
            return succeededResult != nil || failedFailure != nil
        }

        lock.lock()
        let result = succeededResult
        let failure = failedFailure
        let progress = progressSnapshot
        lock.unlock()
        XCTAssertNil(failure?.message)
        XCTAssertEqual(progress?.phase, "starting")
        XCTAssertEqual(result?.stderr, "")
        let stdout = try XCTUnwrap(result?.stdout)
        let output = try JSONDecoder().decode(ManagedImagingOutput.self, from: Data(stdout.utf8))
        XCTAssertEqual(output.run.warnings.first?.count, largeWarning.count)
        XCTAssertEqual(output.artifacts.first?.path, "\(outputPrefix).image")
    }

    func testGenericImagerProcessClientReadsProgressFromJSONLSideChannel() throws {
        let tempRoot = URL(fileURLWithPath: NSTemporaryDirectory(), isDirectory: true)
            .appendingPathComponent("casars-mac-progress-jsonl-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tempRoot, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempRoot) }

        let resultURL = tempRoot.appendingPathComponent("result.json")
        let outputPrefix = tempRoot
            .appendingPathComponent("outputs", isDirectory: true)
            .appendingPathComponent("jsonl-progress")
            .path
        let resultJSON = try makeManagedImagerStdout(
            measurementSet: "/data/probed.ms",
            imagename: outputPrefix
        )
        try resultJSON.write(to: resultURL, atomically: true, encoding: .utf8)

        let helperURL = tempRoot.appendingPathComponent("fake-casars-imager-jsonl")
        let helperScript = """
        #!/bin/sh
        set -eu
        progress_file=""
        while [ "$#" -gt 0 ]; do
          if [ "$1" = "--progress-jsonl" ]; then
            shift
            progress_file="$1"
          fi
          shift || true
        done
        if [ -n "$progress_file" ]; then
          mkdir -p "$(dirname "$progress_file")"
          printf '%s\\n' '{"schema_version":1,"sequence":1,"elapsed_ms":0,"phase":"jsonl_transport","summary":"side-channel progress","work":{"completed_units":1,"total_units":2,"unit_label":"unit","basis":"test","confidence":"exact"},"runtime":{"active_threads":1,"total_threads":2,"gpu_active":false,"backend":"jsonl","active_resources":["visibility-grid"]}}' > "$progress_file"
        fi
        printf '%s\\n' '\(imagerProgressStderrPrefix){"schema_version":1,"sequence":2,"elapsed_ms":1,"phase":"stderr_transport_should_be_ignored","summary":"legacy stderr progress","runtime":{"active_threads":1,"total_threads":2,"gpu_active":false,"backend":"stderr","active_resources":["source-stream"]}}' >&2
        printf '%s\\n' 'ordinary stderr diagnostic' >&2
        cat "\(resultURL.path)"
        """
        try helperScript.write(to: helperURL, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: helperURL.path)

        var task = makeImagerTaskCatalogEntry()
        task.binaryName = helperURL.path
        let request = GenericTaskRequest(
            runID: "jsonl-progress",
            task: task,
            providerInvocation: SurfaceProviderInvocation(
                args: ["--vis", "/data/probed.ms", "--imagename", outputPrefix]
            ),
            workingDirectoryPath: tempRoot.path
        )
        let client = ProcessGenericTaskClient(
            queue: DispatchQueue(label: "casars.mac.test.jsonl-progress")
        )
        let lock = NSLock()
        var succeededResult: GenericTaskResult?
        var progressSnapshot: ImagerProgressSnapshot?

        _ = try client.startTask(request: request) { event in
            lock.lock()
            switch event {
            case .progress(let progress):
                progressSnapshot = progress
            case .succeeded(let result):
                succeededResult = result
            default:
                break
            }
            lock.unlock()
        }

        waitFor("jsonl progress imager process completion", timeout: 5) {
            lock.lock()
            defer { lock.unlock() }
            return succeededResult != nil
        }

        lock.lock()
        let result = succeededResult
        let progress = progressSnapshot
        lock.unlock()
        let arguments = try XCTUnwrap(result?.arguments)
        XCTAssertTrue(arguments.contains("--progress-jsonl"))
        let progressFlagIndex = try XCTUnwrap(arguments.firstIndex(of: "--progress-jsonl"))
        let progressPath = arguments[progressFlagIndex + 1]
        let progressFilename = URL(fileURLWithPath: progressPath).lastPathComponent
        XCTAssertTrue(progressFilename.hasPrefix("jsonl-progress-pid"), progressFilename)
        XCTAssertTrue(progressFilename.hasSuffix("-imager-progress.jsonl"), progressFilename)
        XCTAssertFalse(progressFilename == "jsonl-progress-imager-progress.jsonl")
        XCTAssertEqual(progress?.phase, "jsonl_transport")
        XCTAssertEqual(progress?.runtime.activeResourceIDs, ["visibility-grid"])
        XCTAssertEqual(result?.stderr, "ordinary stderr diagnostic")
    }

    func testImagerProgressTelemetryFilenameIsCollisionResistant() throws {
        let first = ProcessGenericTaskClient.progressTelemetryFilename(
            runID: "../imager 1",
            processID: 123,
            nonce: try XCTUnwrap(UUID(uuidString: "00000000-0000-0000-0000-000000000001"))
        )
        let second = ProcessGenericTaskClient.progressTelemetryFilename(
            runID: "../imager 1",
            processID: 123,
            nonce: try XCTUnwrap(UUID(uuidString: "00000000-0000-0000-0000-000000000002"))
        )

        XCTAssertEqual(first, "imager-1-pid123-00000000-0000-0000-0000-000000000001-imager-progress.jsonl")
        XCTAssertNotEqual(first, second)
    }

    func testGenericTaskClientFindsBundledAndReleaseHelpers() {
        let bundleExecutable = URL(fileURLWithPath: "/Applications/casars-mac.app/Contents/MacOS/casars-mac")

        XCTAssertEqual(
            ProcessGenericTaskClient.resolvedExecutablePath(
                binaryName: "immoments",
                overrideEnv: "CASARS_IMMOMENTS_BIN",
                environment: ["CASARS_IMMOMENTS_BIN": "/custom/immoments"],
                bundleExecutableURL: bundleExecutable,
                isExecutable: { _ in true }
            ),
            "/custom/immoments"
        )
        XCTAssertEqual(
            ProcessGenericTaskClient.resolvedExecutablePath(
                binaryName: "immoments",
                overrideEnv: "CASARS_IMMOMENTS_BIN",
                environment: [:],
                bundleExecutableURL: bundleExecutable,
                isExecutable: { $0 == "/Applications/casars-mac.app/Contents/MacOS/immoments" }
            ),
            "/Applications/casars-mac.app/Contents/MacOS/immoments"
        )
        XCTAssertEqual(
            ProcessGenericTaskClient.resolvedExecutablePath(
                binaryName: "immoments",
                overrideEnv: "CASARS_IMMOMENTS_BIN",
                environment: ["CASA_RS_REPO_ROOT": "/repo"],
                bundleExecutableURL: nil,
                isExecutable: { $0 == "/repo/target/release/immoments" }
            ),
            "/repo/target/release/immoments"
        )
        XCTAssertEqual(
            ProcessGenericTaskClient.resolvedExecutablePath(
                binaryName: "immoments",
                overrideEnv: "CASARS_IMMOMENTS_BIN",
                environment: [:],
                bundleExecutableURL: nil,
                currentDirectoryPath: "/repo/apps/casars-mac",
                isExecutable: { $0 == "/repo/target/debug/immoments" }
            ),
            "/repo/target/debug/immoments"
        )
    }

    func testGenericTaskClientCanResolveEverySwiftVisibleBundledHelper() throws {
        let bundleExecutable = URL(fileURLWithPath: "/Applications/casars-mac.app/Contents/MacOS/casars-mac")
        let tasks = try UniFFITaskCatalogClient().loadTaskCatalog()
        let binaries = Set(tasks.filter(\.showInSwift).map(\.binaryName))

        for binary in binaries.sorted() {
            let bundledPath = "/Applications/casars-mac.app/Contents/MacOS/\(binary)"
            XCTAssertEqual(
                ProcessGenericTaskClient.resolvedExecutablePath(
                    binaryName: binary,
                    overrideEnv: "__unused_\(binary)",
                    environment: [:],
                    bundleExecutableURL: bundleExecutable,
                    isExecutable: { $0 == bundledPath }
                ),
                bundledPath,
                "Expected \(binary) to resolve from the app bundle"
            )
        }
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

private final class FailingProjectProbeClient: ProjectProbeClient {
    private(set) var projectProbeCount = 0

    func probeProject(path: String) throws -> ProjectFixtureProbe {
        projectProbeCount += 1
        throw NSError(domain: "FailingProjectProbeClient", code: 1)
    }

    func probePath(path: String) throws -> DatasetSummary? {
        throw NSError(domain: "FailingProjectProbeClient", code: 2)
    }
}

private final class RecordingDirectMeasurementSetProbeClient: ProjectProbeClient {
    let probedDataset: DatasetSummary
    private(set) var projectProbeCount = 0
    private(set) var pathProbeCount = 0
    private(set) var probedPaths: [String] = []

    init(probedDataset: DatasetSummary) {
        self.probedDataset = probedDataset
    }

    func probeProject(path: String) throws -> ProjectFixtureProbe {
        projectProbeCount += 1
        throw NSError(domain: "RecordingDirectMeasurementSetProbeClient", code: 1)
    }

    func probePath(path: String) throws -> DatasetSummary? {
        pathProbeCount += 1
        probedPaths.append(path)
        return probedDataset
    }
}

private final class StubDemoProjectClient: DemoProjectClient {
    var result: ProjectFixtureProbe
    var error: Error?
    private(set) var cleanedRoots: [String] = []

    init(result: ProjectFixtureProbe, error: Error? = nil) {
        self.result = result
        self.error = error
    }

    func createDemoProject() throws -> ProjectFixtureProbe {
        if let error {
            throw error
        }
        return result
    }

    func cleanupDemoProject(rootPath: String) {
        cleanedRoots.append(rootPath)
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

private final class StubImageExplorerClient: ImageExplorerClient {
    struct Request {
        var datasetPath: String
        var selectedView: String
        var focus: String
        var planeContentMode: String
        var parameters: ImageExplorerParameters
        var cursorX: Int?
        var cursorY: Int?
        var selectedProfileAxis: Int?
        var nonDisplayIndices: [Int]
        var commands: [ImageExplorerCommand]
        var transientCommands: [ImageExplorerCommand]
    }

    private(set) var paths: [String] = []
    private(set) var requests: [Request] = []
    var snapshot: ImageExplorerSnapshot
    var error: Error?
    var failWhenCommandsAreQueued = false

    init(snapshot: ImageExplorerSnapshot) {
        self.snapshot = snapshot
    }

    func buildSnapshot(request: ImageExplorerSnapshotRequest) throws -> ImageExplorerSnapshot {
        paths.append(request.datasetPath)
        requests.append(
            Request(
                datasetPath: request.datasetPath,
                selectedView: request.selectedView,
                focus: request.focus,
                planeContentMode: request.planeContentMode,
                parameters: request.parameters,
                cursorX: request.cursorX,
                cursorY: request.cursorY,
                selectedProfileAxis: request.selectedProfileAxis,
                nonDisplayIndices: request.nonDisplayIndices,
                commands: request.commands,
                transientCommands: request.transientCommands
            )
        )
        if let error {
            throw error
        }
        if failWhenCommandsAreQueued && (!request.commands.isEmpty || !request.transientCommands.isEmpty) {
            throw NSError(domain: "StubImageExplorerClient", code: 42, userInfo: [NSLocalizedDescriptionKey: "bad region command sequence"])
        }
        var nextSnapshot = snapshot
        if let x = request.cursorX, let y = request.cursorY {
            nextSnapshot.planeCursor = ImageExplorerSnapshot.PlaneCursor(
                sampledX: x,
                sampledY: y,
                pixelX: x,
                pixelY: y
            )
        }
        nextSnapshot.parameters = request.parameters
        nextSnapshot.nonDisplayAxes = snapshot.nonDisplayAxes?.map { axis in
            var nextAxis = axis
            if let position = snapshot.nonDisplayAxes?.firstIndex(where: { $0.axis == axis.axis }),
               request.nonDisplayIndices.indices.contains(position)
            {
                nextAxis.index = request.nonDisplayIndices[position]
                nextAxis.pixel = request.nonDisplayIndices[position]
            }
            return nextAxis
        }
        return nextSnapshot
    }
}

private final class StubTableBrowserClient: TableBrowserClient {
    struct Request {
        var request: TableBrowserSnapshotRequest

        var datasetPath: String { request.datasetPath }
        var selectedView: String { request.selectedView }
    }

    private(set) var paths: [String] = []
    private(set) var requests: [Request] = []
    private(set) var cellWindowRequests: [TableBrowserCellWindowRequest] = []
    var snapshot: TableBrowserSnapshot
    var cellWindow: TableBrowserCellWindowSnapshot

    init(snapshot: TableBrowserSnapshot, cellWindow: TableBrowserCellWindowSnapshot? = nil) {
        self.snapshot = snapshot
        self.cellWindow = cellWindow ?? makeTableBrowserCellWindow(path: snapshot.tablePath)
    }

    func buildSnapshot(request: TableBrowserSnapshotRequest) throws -> TableBrowserSnapshot {
        paths.append(request.datasetPath)
        requests.append(Request(request: request))
        var nextSnapshot = snapshot
        let views = ["overview", "columns", "keywords", "cells", "subtables"]
        var viewIndex = views.firstIndex(of: request.selectedView) ?? 0
        var row = 0
        var column = "DATA"
        var configuredAddress: TableBrowserSnapshot.SelectedAddress?
        for command in request.commands {
            switch command {
            case .configure(let parameters):
                viewIndex = views.firstIndex(of: parameters.view) ?? 0
                row = parameters.rowStart
                if let linkedTable = parameters.linkedTable {
                    nextSnapshot.tablePath = "\(request.datasetPath)/\(linkedTable)"
                }
                switch parameters.bookmark {
                case .cell(let bookmarkRow, let bookmarkColumn):
                    row = bookmarkRow
                    column = bookmarkColumn
                    configuredAddress = TableBrowserSnapshot.SelectedAddress(
                        kind: "cell",
                        tablePath: nextSnapshot.tablePath,
                        row: bookmarkRow,
                        column: bookmarkColumn,
                        keywordPath: nil,
                        valuePath: nil,
                        source: nil,
                        targetPath: nil
                    )
                case .tableKeyword(let path):
                    configuredAddress = TableBrowserSnapshot.SelectedAddress(
                        kind: "table_keyword",
                        tablePath: nextSnapshot.tablePath,
                        row: nil,
                        column: nil,
                        keywordPath: path,
                        valuePath: nil,
                        source: nil,
                        targetPath: nil
                    )
                case .columnKeyword(let owner, let path):
                    configuredAddress = TableBrowserSnapshot.SelectedAddress(
                        kind: "column_keyword",
                        tablePath: nextSnapshot.tablePath,
                        row: nil,
                        column: owner,
                        keywordPath: path,
                        valuePath: nil,
                        source: nil,
                        targetPath: nil
                    )
                case .subtable(let name):
                    configuredAddress = TableBrowserSnapshot.SelectedAddress(
                        kind: "subtable",
                        tablePath: nextSnapshot.tablePath,
                        row: nil,
                        column: nil,
                        keywordPath: nil,
                        valuePath: nil,
                        source: name,
                        targetPath: "\(request.datasetPath)/\(name)"
                    )
                case nil:
                    break
                }
            case .cycleView(let forward):
                viewIndex = (viewIndex + (forward ? 1 : views.count - 1)) % views.count
                row = 0
            case .moveDown(let steps): row += steps
            case .moveUp(let steps): row = max(0, row - steps)
            case .moveRight: column = "DATA"
            case .moveLeft: column = "TIME"
            case .activate:
                viewIndex = 0
                row = 0
            default:
                break
            }
        }
        nextSnapshot.view = views[viewIndex]
        nextSnapshot.focus = request.focus
        if let configuredAddress {
            nextSnapshot.selectedAddress = configuredAddress
        } else {
            switch nextSnapshot.view {
        case "cells":
            nextSnapshot.selectedAddress = TableBrowserSnapshot.SelectedAddress(
                kind: "cell",
                tablePath: request.datasetPath,
                row: row,
                column: column,
                keywordPath: nil,
                valuePath: nil,
                source: nil,
                targetPath: nil
            )
        case "subtables":
            nextSnapshot.selectedAddress = TableBrowserSnapshot.SelectedAddress(
                kind: "subtable",
                tablePath: request.datasetPath,
                row: nil,
                column: nil,
                keywordPath: nil,
                valuePath: nil,
                source: "ANTENNA",
                targetPath: "\(request.datasetPath)/ANTENNA"
            )
        default:
            break
            }
        }
        return nextSnapshot
    }

    func buildCellWindow(request: TableBrowserCellWindowRequest) throws -> TableBrowserCellWindowSnapshot {
        cellWindowRequests.append(request)
        var nextWindow = cellWindow
        nextWindow.tablePath = request.datasetPath
        nextWindow.rowStart = request.rowStart
        nextWindow.columnStart = request.columnStart
        return nextWindow
    }

    func buildCellValue(request: TableBrowserCellValueRequest) throws -> String {
        "row \(request.rowIndex) column \(request.columnIndex)"
    }
}

private func makeDemoProjectProbe(rootPath: String) -> ProjectFixtureProbe {
    let msDataset = DatasetSummary(
        id: "\(rootPath)/twhya_calibrated.ms",
        name: "twhya_calibrated.ms",
        path: "\(rootPath)/twhya_calibrated.ms",
        kind: .measurementSet,
        size: "tutorial rows",
        units: "Jy, Hz, seconds",
        fields: ["5: TW Hya"],
        spectralWindows: ["spw 0: 128 chan, 372.533 GHz center"],
        dataColumns: ["DATA"],
        notes: "ALMA First Look TW Hya tutorial MeasurementSet."
    )
    let imageDataset = DatasetSummary(
        id: "\(rootPath)/twhya_cont.image",
        name: "twhya_cont.image",
        path: "\(rootPath)/twhya_cont.image",
        kind: .imageCube,
        size: "512 x 512",
        units: "Jy/beam",
        shape: [512, 512],
        notes: "ALMA First Look TW Hya tutorial image."
    )
    let tableDataset = DatasetSummary(
        id: "\(rootPath)/twhya_calibrated_ANTENNA.table",
        name: "twhya_calibrated_ANTENNA.table",
        path: "\(rootPath)/twhya_calibrated_ANTENNA.table",
        kind: .table,
        size: "27 rows",
        units: "casacore table",
        columns: ["NAME", "POSITION", "DISH_DIAMETER"],
        shape: [27],
        notes: "ANTENNA subtable copied from the TW Hya tutorial MeasurementSet."
    )
    return ProjectFixtureProbe(
        project: ProjectFixture(
            name: "TW Hya Tutorial Demo",
            rootPath: rootPath,
            datasets: [imageDataset, tableDataset, msDataset],
            source: .probed
        ),
        diagnostics: ["Staged tutorial dataset: alma/first-look/twhya/calibrated-ms"]
    )
}

private func makeImageExplorerSnapshot(nonDisplayIndex: Int = 0) -> ImageExplorerSnapshot {
    ImageExplorerSnapshot(
        statusLine: "Browsing restored.image.",
        activeView: "plane",
        shape: [4, 4, 8],
        inspectorLines: ["Cursor: 1,1", "Value: 2 Jy/beam"],
        contentLines: ["plane content"],
        plane: ImageExplorerSnapshot.Plane(
            width: 2,
            height: 2,
            pixelsU8: [0, 64, 128, 255],
            clipMin: 0,
            clipMax: 3,
            dataMin: 0,
            dataMax: 3,
            valueUnit: "Jy/beam",
            maskedOrNonFiniteCount: 0
        ),
        profile: ImageExplorerSnapshot.Profile(
            axis: 2,
            axisName: "Frequency",
            axisUnit: "Hz",
            valueUnit: "Jy/beam",
            samples: [
                ImageExplorerSnapshot.Profile.Sample(sampleIndex: 0, pixelIndex: 0, value: 1.0, finite: true),
                ImageExplorerSnapshot.Profile.Sample(sampleIndex: 1, pixelIndex: 1, value: 2.0, finite: true)
            ]
        ),
        planeCursor: ImageExplorerSnapshot.PlaneCursor(sampledX: 1, sampledY: 1, pixelX: 1, pixelY: 1),
        nonDisplayAxes: [
            ImageExplorerSnapshot.NonDisplayAxis(
                axis: 2,
                label: "Frequency",
                index: nonDisplayIndex,
                length: 8,
                pixel: nonDisplayIndex
            )
        ],
        region: ImageExplorerSnapshot.Region(label: "active region", shapeCount: 1, closedShapeCount: 1, editing: false),
        savedRegionNames: ["source"],
        maskNames: ["mask0"],
        capabilities: ImageExplorerSnapshot.Capabilities(
            renderablePlane: true,
            worldCoordsAvailable: true,
            pixelOnlyMode: false,
            nonDisplayAxisSelectors: true,
            maskPresent: true
        )
    )
}

private func makeTableBrowserSnapshot(path: String) -> TableBrowserSnapshot {
    TableBrowserSnapshot(
        capabilities: TableBrowserSnapshot.Capabilities(editable: false),
        view: "cells",
        focus: "main",
        tablePath: path,
        breadcrumb: [TableBrowserSnapshot.Breadcrumb(label: "MAIN", path: path)],
        viewport: TableBrowserSnapshot.Viewport(width: 180, height: 48, inspectorHeight: 12),
        statusLine: "Browsing \(path).",
        contentLines: [
            "Cells  row=1/12  col=1/3  focus=Main",
            "row | TIME<f64>[s] | DATA<c64[4x2]> | FLAG<bool> |",
            ">  0 | 0.0 | >[1+0i, ...]< | false |",
            "   1 | 1.0 | [1+1i, ...] | false |"
        ],
        verticalMetrics: TableBrowserSnapshot.NavigationMetrics(
            selectedIndex: 0,
            totalItems: 12,
            viewportItems: 46
        ),
        horizontalMetrics: TableBrowserSnapshot.NavigationMetrics(
            selectedIndex: 1,
            totalItems: 3,
            viewportItems: 3
        ),
        selectedAddress: TableBrowserSnapshot.SelectedAddress(
            kind: "column",
            tablePath: path,
            row: nil,
            column: "DATA",
            keywordPath: nil,
            valuePath: nil,
            source: nil,
            targetPath: nil
        ),
        inspector: TableBrowserSnapshot.Inspector(
            title: "Column DATA",
            trail: [],
            node: .array(
                primitive: "complex64",
                shape: [4, 2],
                totalElements: 8,
                pageStart: 0,
                pageSize: 8,
                elements: []
            ),
            renderedLines: ["Array Complex64[4,2]", "Unit: Jy"]
        )
    )
}

private func makeTableBrowserCellWindow(path: String) -> TableBrowserCellWindowSnapshot {
    TableBrowserCellWindowSnapshot(
        tablePath: path,
        rowCount: 12,
        columnCount: 3,
        rowStart: 0,
        columnStart: 0,
        columns: [
            TableBrowserCellWindowSnapshot.Column(index: 0, name: "TIME", header: "TIME<f64>[s]", summary: "Scalar Float64", width: 14),
            TableBrowserCellWindowSnapshot.Column(index: 1, name: "DATA", header: "DATA<c64[4x2]>", summary: "Array<Complex64> fixed", width: 20),
            TableBrowserCellWindowSnapshot.Column(index: 2, name: "FLAG", header: "FLAG<bool>", summary: "Scalar Bool", width: 10)
        ],
        rows: [
            TableBrowserCellWindowSnapshot.Row(index: 0, cells: [
                TableBrowserCellWindowSnapshot.Cell(columnIndex: 0, display: "0.0", defined: true),
                TableBrowserCellWindowSnapshot.Cell(columnIndex: 1, display: "[1+0i, ...]", defined: true),
                TableBrowserCellWindowSnapshot.Cell(columnIndex: 2, display: "false", defined: true)
            ]),
            TableBrowserCellWindowSnapshot.Row(index: 1, cells: [
                TableBrowserCellWindowSnapshot.Cell(columnIndex: 0, display: "1.0", defined: true),
                TableBrowserCellWindowSnapshot.Cell(columnIndex: 1, display: "[1+1i, ...]", defined: true),
                TableBrowserCellWindowSnapshot.Cell(columnIndex: 2, display: "false", defined: true)
            ])
        ]
    )
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
        plotDocument: makeTestPlotDocument(title: title),
        renderer: "stub renderer",
        imageFormat: "png",
        imageWidth: imageWidth,
        imageHeight: imageHeight,
        imageBytes: imageBytes
    )
}

private func makeTestPlotDocument(title: String = "UV Coverage") -> WorkbenchPlotDocument {
    WorkbenchPlotDocument(
        id: "test-ms-plot",
        title: title,
        subtitle: "Synthetic plot document for tests.",
        axes: [
            WorkbenchPlotAxis(id: "frequency", label: "Frequency", unit: "Hz", range: WorkbenchPlotRange(lower: 0, upper: 3)),
            WorkbenchPlotAxis(id: "amplitude", label: "Amplitude", unit: "", range: WorkbenchPlotRange(lower: 0, upper: 2))
        ],
        layers: [
            WorkbenchPlotLayer(
                id: "target",
                title: "Target",
                kind: .scatter,
                xAxisID: "frequency",
                yAxisID: "amplitude",
                points: [
                    WorkbenchPlotPoint(x: 0, y: 0.2),
                    WorkbenchPlotPoint(x: 1, y: 0.8),
                    WorkbenchPlotPoint(x: 2, y: 1.4)
                ],
                style: WorkbenchPlotLayerStyle(colorHex: "#2563eb", symbolSize: 3, opacity: 0.8),
                provenanceSummary: "Synthetic MeasurementSet samples."
            )
        ]
    )
}

private func makeManagedImagerStdout(
    measurementSet: String,
    imagename: String,
    warning: String = "synthetic warning"
) throws -> String {
    """
    {
      "request": {
        "measurement_set": "\(measurementSet)",
        "imagename": "\(imagename)",
        "spectral_mode": "mfs",
        "weighting": "natural",
        "deconvolver": "hogbom",
        "imsize": 256,
        "cell_arcsec": 1.0,
        "dirty_only": true,
        "output_channels": 1
      },
      "run": {
        "warnings": ["\(warning)"],
        "gridded_samples": 128,
        "major_cycles": 1,
        "minor_iterations": 0,
        "channels": [{"channel_index": 0}]
      },
      "artifacts": [
        {
          "kind": "image",
          "label": "Image",
          "path": "\(imagename).image",
          "exists": true,
          "preview_png_path": "\(imagename).image.png",
          "preview_png_exists": true
        }
      ]
    }
    """
}

private final class StubGenericTaskClient: GenericTaskClient {
    var requests: [GenericTaskRequest] = []
    var stdout = ""
    var stderr = ""

    func startTask(
        request: GenericTaskRequest,
        eventHandler: @escaping (GenericTaskEvent) -> Void
    ) throws -> TaskExecution {
        requests.append(request)
        eventHandler(.succeeded(GenericTaskResult(
            taskID: request.task.id,
            arguments: try ProcessGenericTaskClient.arguments(for: request),
            stdout: stdout,
            stderr: stderr
        )))
        return StubTaskExecution()
    }
}

private struct StubTaskUISchemaClient: TaskUISchemaClient {
    var schema: TaskUISchema

    func loadTaskUISchema(taskID: String) throws -> TaskUISchema {
        schema
    }
}

private struct StubTaskCatalogClient: TaskCatalogClient {
    var tasks: [TaskCatalogEntry]

    func loadTaskCatalog() throws -> [TaskCatalogEntry] {
        tasks
    }
}

private struct StubTaskExecutionMatrixClient: TaskExecutionMatrixClient {
    var rows: [TaskExecutionMatrixRow]

    func loadTaskExecutionMatrix() throws -> TaskExecutionMatrixEnvelope {
        TaskExecutionMatrixEnvelope(
            schemaVersion: 1,
            generatedFor: "test",
            scopeNote: "test",
            rows: rows
        )
    }
}

private func repositoryRootURL() -> URL {
    var url = URL(fileURLWithPath: #filePath)
    for _ in 0..<5 {
        url.deleteLastPathComponent()
    }
    return url
}

private func canonicalJSONObject(_ value: SurfaceParameterValue) -> Any {
    switch value {
    case .bool(let value): value
    case .integer(let value): value
    case .float(let value): value
    case .string(let value): value
    case .array(let values): values.map(canonicalJSONObject)
    case .table(let values): values.mapValues(canonicalJSONObject)
    }
}

private func makeImheadTaskCatalogEntry() -> TaskCatalogEntry {
    makeTaskCatalogEntry(id: "imhead", displayName: "Image Header")
}

private func makeSimobserveTaskCatalogEntry() -> TaskCatalogEntry {
    TaskCatalogEntry(
        id: "simobserve",
        category: "Simulation",
        displayName: "SimObserve",
        binaryName: "simobserve",
        cargoPackage: "casa-ms",
        overrideEnv: "CASARS_SIMOBSERVE_BIN",
        shellKind: "workflow",
        interaction: "one_shot",
        browserKind: nil,
        datasetKinds: ["measurement_set"],
        schemaSource: "binary",
        showInTUI: true,
        showInSwift: true,
        includeInSuite: true
    )
}

private func makeImagerTaskCatalogEntry() -> TaskCatalogEntry {
    TaskCatalogEntry(
        id: "imager",
        category: "Imaging",
        displayName: "Imager",
        binaryName: "casars-imager",
        cargoPackage: "casars-imager",
        overrideEnv: "CASARS_IMAGER_BIN",
        shellKind: "workflow",
        interaction: "one_shot",
        browserKind: nil,
        datasetKinds: ["measurement_set"],
        schemaSource: "binary",
        showInTUI: true,
        showInSwift: true,
        includeInSuite: true
    )
}

private func makeTaskCatalogEntry(id: String, displayName: String) -> TaskCatalogEntry {
    TaskCatalogEntry(
        id: id,
        category: "Images",
        displayName: displayName,
        binaryName: "imexplore",
        cargoPackage: "casa-images",
        overrideEnv: "CASARS_IMEXPLORE_BIN",
        shellKind: "workflow",
        interaction: "one_shot",
        browserKind: nil,
        datasetKinds: ["image_cube"],
        schemaSource: "binary",
        showInTUI: true,
        showInSwift: true,
        includeInSuite: true
    )
}

private func makeImagerTaskUISchema() throws -> TaskUISchema {
    try JSONDecoder().decode(TaskUISchema.self, from: Data("""
    {
      "schema_version": 1,
      "command_id": "imager",
      "invocation_name": "casars-imager",
      "display_name": "Imager",
      "category": "Imaging",
      "summary": "Run CASA-compatible imaging from a MeasurementSet.",
      "usage": "casars-imager --ms PATH --imagename PREFIX --imsize N --cell-arcsec ARCSEC [options]",
      "managed_output": {
        "renderer": "imager-run-v1",
        "stdout_format": "json",
        "inject_arguments": [{"flag":"--managed-output","value":"true"}],
        "raw_stdout_available": true,
        "raw_stderr_available": true
      },
      "arguments": [
        {"id":"vis","label":"MeasurementSet","order":0,"parser":{"kind":"option","flags":["--ms"],"metavar":"PATH","choices":[]},"value_kind":"path","parameter_type":"path","required":true,"default":null,"help":"","group":"Context","advanced":false,"hidden_in_tui":false},
        {"id":"imagename","label":"Image Prefix","order":1,"parser":{"kind":"option","flags":["--imagename"],"metavar":"PREFIX","choices":[]},"value_kind":"path","parameter_type":"output_image_path","required":true,"default":null,"help":"","group":"Products","advanced":false,"hidden_in_tui":false},
        {"id":"imsize","label":"Image Size","order":2,"parser":{"kind":"option","flags":["--imsize"],"metavar":"PIXELS","choices":[]},"value_kind":"string","required":true,"default":"512","help":"","group":"Stage Parameters","advanced":false,"hidden_in_tui":false},
        {"id":"cell","label":"Cell Size","order":3,"parser":{"kind":"option","flags":["--cell-arcsec"],"metavar":"ARCSEC","choices":[]},"value_kind":"string","required":true,"default":"1.0arcsec","help":"","group":"Stage Parameters","advanced":false,"hidden_in_tui":false},
        {"id":"datacolumn","label":"Data Column","order":4,"parser":{"kind":"option","flags":["--datacolumn"],"metavar":"NAME","choices":["DATA","CORRECTED_DATA","MODEL_DATA"]},"value_kind":"choice","required":false,"default":null,"help":"","group":"Context","advanced":false,"hidden_in_tui":false},
        {"id":"field","label":"Fields","order":7,"parser":{"kind":"option","flags":["--field"],"metavar":"IDS","choices":[]},"value_kind":"string","required":false,"default":null,"help":"","group":"Context","advanced":false,"hidden_in_tui":false},
        {"id":"phasecenter_field","label":"Phasecenter Field","order":8,"parser":{"kind":"option","flags":["--phasecenter-field"],"metavar":"ID","choices":[]},"value_kind":"string","required":false,"default":null,"help":"","group":"Context","advanced":false,"hidden_in_tui":false},
        {"id":"spw","label":"SPW","order":10,"parser":{"kind":"option","flags":["--spw"],"metavar":"SEL","choices":[]},"value_kind":"string","required":false,"default":null,"help":"","group":"Context","advanced":false,"hidden_in_tui":false},
        {"id":"channel_start","label":"Channel Start","order":11,"parser":{"kind":"option","flags":["--channel-start"],"metavar":"N","choices":[]},"value_kind":"string","required":false,"default":null,"help":"","group":"Context","advanced":true,"hidden_in_tui":false},
        {"id":"channel_count","label":"Channel Count","order":12,"parser":{"kind":"option","flags":["--channel-count"],"metavar":"N","choices":[]},"value_kind":"string","required":false,"default":null,"help":"","group":"Context","advanced":true,"hidden_in_tui":false},
        {"id":"polarization","label":"Corr / Stokes","order":13,"parser":{"kind":"option","flags":["--corr"],"metavar":"PLANE","choices":["I","Q","U","V","XX","YY","RR","LL"]},"value_kind":"choice","required":false,"default":"I","help":"","group":"Context","advanced":true,"hidden_in_tui":false},
        {"id":"specmode","label":"Spectral Mode","order":20,"parser":{"kind":"option","flags":["--specmode"],"metavar":"MODE","choices":["mfs","cube","cubedata"]},"value_kind":"choice","required":true,"default":"mfs","help":"","group":"Stages","advanced":false,"hidden_in_tui":false},
        {"id":"interpolation","label":"Cube Interp","order":21,"parser":{"kind":"option","flags":["--interpolation"],"metavar":"MODE","choices":["nearest","linear","cubic"]},"value_kind":"choice","required":false,"default":null,"help":"","group":"Stages","advanced":true,"hidden_in_tui":false},
        {"id":"perchanweightdensity","label":"Per-Channel Density","order":23,"parser":{"kind":"toggle","true_flags":["--perchanweightdensity"],"false_flags":["--no-perchanweightdensity"]},"value_kind":"bool","required":false,"default":"cube:true,cubedata:false","help":"","group":"Stages","advanced":true,"hidden_in_tui":false},
        {"id":"dirty_only","label":"Dirty Only","order":30,"parser":{"kind":"toggle","true_flags":["--dirty-only"],"false_flags":[]},"value_kind":"bool","required":false,"default":"false","help":"","group":"Stages","advanced":false,"hidden_in_tui":false},
        {"id":"niter","label":"Iterations","order":31,"parser":{"kind":"option","flags":["--niter"],"metavar":"N","choices":[]},"value_kind":"string","required":false,"default":"0","help":"","group":"Stages","advanced":false,"hidden_in_tui":false},
        {"id":"threshold","label":"Threshold","order":32,"parser":{"kind":"option","flags":["--threshold-jy"],"metavar":"JY","choices":[]},"value_kind":"string","required":false,"default":"0.0Jy","help":"","group":"Stages","advanced":false,"hidden_in_tui":false},
        {"id":"deconvolver","label":"Deconvolver","order":40,"parser":{"kind":"option","flags":["--deconvolver"],"metavar":"MODE","choices":["hogbom","mtmfs","clark","multiscale"]},"value_kind":"choice","required":true,"default":"hogbom","help":"","group":"Stages","advanced":false,"hidden_in_tui":false},
        {"id":"weighting","label":"Weighting","order":50,"parser":{"kind":"option","flags":["--weighting"],"metavar":"MODE","choices":["natural","uniform","briggs","briggsbwtaper"]},"value_kind":"choice","required":true,"default":"natural","help":"","group":"Stages","advanced":false,"hidden_in_tui":false},
        {"id":"write_pb","label":"Primary Beam","order":53,"parser":{"kind":"toggle","true_flags":["--write-pb"],"false_flags":[]},"value_kind":"bool","required":false,"default":"false","help":"","group":"Stages","advanced":true,"hidden_in_tui":false},
        {"id":"pbcor","label":"PB Correct","order":54,"parser":{"kind":"toggle","true_flags":["--pbcor"],"false_flags":[]},"value_kind":"bool","required":false,"default":"false","help":"","group":"Stages","advanced":true,"hidden_in_tui":false},
        {"id":"robust","label":"Robust","order":51,"parser":{"kind":"option","flags":["--robust"],"metavar":"VALUE","choices":[]},"value_kind":"float","required":false,"default":"0.5","help":"","group":"Stages","advanced":false,"hidden_in_tui":false},
        {"id":"gridder","label":"Gridder","order":60,"parser":{"kind":"option","flags":["--gridder"],"metavar":"MODE","choices":["standard","wproject","mosaic"]},"value_kind":"choice","required":true,"default":"standard","help":"","group":"Stages","advanced":false,"hidden_in_tui":false}
      ]
    }
    """.utf8))
}

private func makeImheadTaskUISchema() throws -> TaskUISchema {
    try JSONDecoder().decode(TaskUISchema.self, from: Data("""
    {
      "schema_version": 1,
      "command_id": "imhead",
      "invocation_name": "imexplore",
      "display_name": "Image Header",
      "category": "Images",
      "summary": "Inspect CASA image metadata.",
      "usage": "imexplore imhead <image>",
      "arguments": [
        {"id":"imagename","label":"Image","order":0,"parser":{"kind":"option","flags":["--image"],"metavar":"IMAGE","choices":[]},"value_kind":"path","parameter_type":"path","required":true,"default":null,"help":"","group":"Input","advanced":false,"hidden_in_tui":false},
        {"id":"mode","label":"Mode","order":1,"parser":{"kind":"option","flags":["--mode"],"metavar":"MODE","choices":["summary","list"]},"value_kind":"choice","required":false,"default":"summary","help":"","group":"Output","advanced":false,"hidden_in_tui":false},
        {"id":"hdkey","label":"Header Key","order":2,"parser":{"kind":"option","flags":["--hdkey"],"metavar":"KEY","choices":[]},"value_kind":"string","required":false,"default":"none","help":"","group":"Header","advanced":false,"hidden_in_tui":false},
        {"id":"hdvalue","label":"Header Value","order":3,"parser":{"kind":"option","flags":["--hdvalue"],"metavar":"VALUE","choices":[]},"value_kind":"string","required":false,"default":"none","help":"","group":"Header","advanced":false,"hidden_in_tui":false}
      ]
    }
    """.utf8))
}

private func makeImstatTaskUISchema() throws -> TaskUISchema {
    try JSONDecoder().decode(TaskUISchema.self, from: Data("""
    {
      "schema_version": 1,
      "command_id": "imstat",
      "invocation_name": "imexplore",
      "display_name": "Image Statistics",
      "category": "Images",
      "summary": "Measure CASA image statistics.",
      "usage": "imexplore imstat <image>",
      "arguments": [
        {"id":"image_path","label":"Image","order":0,"parser":{"kind":"option","flags":["--image"],"metavar":"IMAGE","choices":[]},"value_kind":"path","parameter_type":"image_path","required":true,"default":null,"help":"","group":"Input","advanced":false,"hidden_in_tui":false},
        {"id":"region","label":"Region","order":1,"parser":{"kind":"option","flags":["--region"],"metavar":"REGION","choices":[]},"value_kind":"path","parameter_type":"region_path_or_box","required":false,"default":null,"help":"","group":"Input","advanced":false,"hidden_in_tui":false}
      ]
    }
    """.utf8))
}

private final class RecordingSurfaceParameterClient: SurfaceParameterClient {
    struct Write: Equatable {
        var surfaceID: String
        var workspace: String
        var values: [String: SurfaceParameterValue]
        var successful: Bool
    }

    private let base = UniFFISurfaceParameterClient()
    private(set) var writes: [Write] = []
    private(set) var invocations: [String] = []
    var resolveFailure: ((SurfaceParameterPatch, SurfaceParameterPatch) -> Error?)?

    var invocationCount: Int { invocations.count }

    func loadBundle(surfaceID: String) throws -> SurfaceParameterBundle {
        invocations.append("loadBundle")
        return try base.loadBundle(surfaceID: surfaceID)
    }

    func defaults(surfaceID: String) throws -> SurfaceParameterSnapshot {
        invocations.append("defaults")
        return try base.defaults(surfaceID: surfaceID)
    }

    func last(surfaceID _: String, workspace _: String, successful _: Bool) throws -> SurfaceParameterSnapshot? {
        invocations.append("last")
        return nil
    }

    func load(surfaceID: String, profileTOML: String, sourcePath: String) throws -> SurfaceParameterSnapshot {
        invocations.append("load")
        return try base.load(surfaceID: surfaceID, profileTOML: profileTOML, sourcePath: sourcePath)
    }

    func resolve(
        surfaceID: String,
        baseSource: SurfaceParameterBaseSource,
        profileTOML: String?,
        profilePath: String?,
        context: SurfaceParameterPatch,
        override: SurfaceParameterPatch
    ) throws -> SurfaceParameterSnapshot {
        invocations.append("resolve")
        if let error = resolveFailure?(context, override) {
            throw error
        }
        return try base.resolve(
            surfaceID: surfaceID,
            baseSource: baseSource,
            profileTOML: profileTOML,
            profilePath: profilePath,
            context: context,
            override: override
        )
    }

    func save(
        surfaceID: String,
        values: [String: SurfaceParameterValue],
        destinationPath: String
    ) throws -> SurfaceParameterWriteResult {
        invocations.append("save")
        return try base.save(surfaceID: surfaceID, values: values, destinationPath: destinationPath)
    }

    func writeLast(
        surfaceID: String,
        workspace: String,
        values: [String: SurfaceParameterValue],
        successful: Bool
    ) throws -> SurfaceParameterWriteResult {
        invocations.append("writeLast")
        writes.append(Write(
            surfaceID: surfaceID,
            workspace: workspace,
            values: values,
            successful: successful
        ))
        return SurfaceParameterWriteResult(
            path: "\(workspace)/.casa-rs/parameters/\(surfaceID)/\(successful ? "last-successful" : "last").toml",
            bytesWritten: 0,
            managedKind: successful ? "last_successful" : "last"
        )
    }
}

private final class RecordingNotebookPersistenceClient: NotebookPersistenceClient {
    private let base = UniFFINotebookPersistenceClient()
    private(set) var beginRequests: [NotebookBeginRecordingRequest] = []
    private(set) var finalizeRequests: [NotebookFinalizeRecordingRequest] = []
    var beginError: Error?

    func projectCells(source: String) throws -> [NotebookCellState] {
        try base.projectCells(source: source)
    }

    func loadProject(projectRoot: String) throws -> ScientificNotebookProjectState {
        try base.loadProject(projectRoot: projectRoot)
    }

    func create(projectRoot: String, filename: String?, title: String) throws -> NotebookDocumentState {
        try base.create(projectRoot: projectRoot, filename: filename, title: title)
    }

    func save(
        projectRoot: String,
        document: NotebookDocumentState,
        resolution: NotebookConflictResolution
    ) throws -> NotebookSaveResult {
        try base.save(projectRoot: projectRoot, document: document, resolution: resolution)
    }

    func beginRecording(request: NotebookBeginRecordingRequest) throws -> NotebookBeginRecordingResult {
        beginRequests.append(request)
        if let beginError { throw beginError }
        return try base.beginRecording(request: request)
    }

    func finalizeRecording(request: NotebookFinalizeRecordingRequest) throws {
        finalizeRequests.append(request)
        try base.finalizeRecording(request: request)
    }

    func saveVisualization(
        request: NotebookSaveVisualizationEnvelope
    ) throws -> NotebookVisualizationSnapshot {
        try base.saveVisualization(request: request)
    }
}

private final class HoldingGenericTaskClient: GenericTaskClient {
    var requests: [GenericTaskRequest] = []
    var handler: ((GenericTaskEvent) -> Void)?
    var stdout = ""
    var stderr = ""
    let execution = StubTaskExecution()

    func startTask(
        request: GenericTaskRequest,
        eventHandler: @escaping (GenericTaskEvent) -> Void
    ) throws -> TaskExecution {
        requests.append(request)
        handler = eventHandler
        return execution
    }

    func emitProgress(_ progress: ImagerProgressSnapshot) {
        handler?(.progress(progress))
    }

    func emitSucceeded() throws {
        guard let request = requests.last else { return }
        handler?(.succeeded(GenericTaskResult(
            taskID: request.task.id,
            arguments: try ProcessGenericTaskClient.arguments(for: request),
            stdout: stdout,
            stderr: stderr
        )))
    }
}

private func makeSimobserveGenericTaskRequest(rootURL: URL) throws -> GenericTaskRequest {
    GenericTaskRequest(
        runID: "simobserve-1",
        task: makeSimobserveTaskCatalogEntry(),
        providerInvocation: SurfaceProviderInvocation(
            args: ["--json-run", "-"],
            stdin: #"{"kind":"family","request":{"model":"model.image","output_ms":"products/family.ms"}}"#
        ),
        workingDirectoryPath: rootURL.path
    )
}

private func notebookPrototypeDependencies(
    genericTaskClient: GenericTaskClient,
    surfaceParameterClient: SurfaceParameterClient
) -> NotebookPrototypeRuntimeDependencies {
    let denied = NotebookPrototypeRuntimeDependencies.denied
    return NotebookPrototypeRuntimeDependencies(
        probeClient: denied.probeClient,
        demoProjectClient: denied.demoProjectClient,
        plotClient: denied.plotClient,
        imageExplorerClient: denied.imageExplorerClient,
        tableBrowserClient: denied.tableBrowserClient,
        genericTaskClient: genericTaskClient,
        taskUISchemaClient: denied.taskUISchemaClient,
        surfaceParameterClient: surfaceParameterClient
    )
}

private func writeStubSimobserveBinary(rootURL: URL, script: String) throws -> URL {
    let binaryURL = rootURL.appendingPathComponent("simobserve")
    try script.write(to: binaryURL, atomically: true, encoding: .utf8)
    try FileManager.default.setAttributes(
        [.posixPermissions: NSNumber(value: Int16(0o755))],
        ofItemAtPath: binaryURL.path
    )
    return binaryURL
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

private func runCurrentRunLoop(for duration: TimeInterval) {
    let deadline = Date().addingTimeInterval(duration)
    while Date() < deadline {
        RunLoop.current.run(mode: .default, before: min(deadline, Date().addingTimeInterval(0.01)))
    }
}

private func tryUnwrap<T>(_ value: T?, file: StaticString = #filePath, line: UInt = #line) -> T {
    guard let value else {
        XCTFail("Expected non-nil value", file: file, line: line)
        fatalError("Expected non-nil value")
    }
    return value
}

private final class StubTaskExecution: TaskExecution {
    var didCancel = false

    func cancel() {
        didCancel = true
    }
}
