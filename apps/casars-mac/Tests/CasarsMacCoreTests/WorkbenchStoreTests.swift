import Foundation
import XCTest
@testable import CasarsMacCore

final class WorkbenchStoreTests: XCTestCase {
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

    func testTutorialPackContextLoadsTemplateAndInputStatus() throws {
        let packURL = try makeTemporaryTutorialPack(stagedInputPaths: ["twhya_cont.image"])
        defer { removeTemporaryTutorialPack(packURL) }

        let context = try TutorialPackContext.load(path: packURL.path)

        XCTAssertEqual(context.packID, "alma-first-look-image-analysis")
        XCTAssertEqual(context.title, "ALMA First Look: Image Analysis")
        XCTAssertEqual(context.selectedSection?.id, "01-imhead-continuum-header")
        XCTAssertEqual(context.inputs.map(\.status), [.staged, .missing])
        XCTAssertEqual(context.datasetSummaries().map(\.name), ["twhya_cont.image"])
        XCTAssertEqual(
            context.learnerDocsIndex,
            packURL.appendingPathComponent("README.md").standardizedFileURL.path
        )
    }

    func testTutorialPackMeasurementSetInputKeepsMeasurementSetKind() throws {
        let packURL = try makeTemporaryTutorialPack(
            stagedInputPaths: ["twhya_calibrated.ms"],
            templateName: "alma-first-look-imaging.template.json"
        )
        defer { removeTemporaryTutorialPack(packURL) }

        let context = try TutorialPackContext.load(path: packURL.path)
        let dataset = try XCTUnwrap(context.datasetSummaries().first { $0.name == "twhya_calibrated.ms" })

        XCTAssertEqual(dataset.kind, .measurementSet)
        XCTAssertEqual(dataset.path, packURL.appendingPathComponent("twhya_calibrated.ms").standardizedFileURL.path)
    }

    func testOpenTutorialPackPopulatesGuiDebugSnapshot() throws {
        let packURL = try makeTemporaryTutorialPack(stagedInputPaths: ["twhya_cont.image"])
        defer { removeTemporaryTutorialPack(packURL) }
        let store = WorkbenchStore(
            taskCatalogClient: StubTaskCatalogClient(tasks: [makeImheadTaskCatalogEntry()]),
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImheadTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.openTutorialPack(path: packURL.path)
        let snapshot = store.debugSnapshot()

        XCTAssertEqual(snapshot.activeProject, "ALMA First Look: Image Analysis")
        XCTAssertEqual(snapshot.activeProjectSource, .tutorialPack)
        XCTAssertEqual(snapshot.activeProjectRoot, packURL.standardizedFileURL.path)
        XCTAssertEqual(snapshot.openTabs, ["Tutorial"])
        XCTAssertEqual(snapshot.activeTab, "Tutorial")
        XCTAssertEqual(snapshot.selectedDataset, "twhya_cont.image")
        XCTAssertEqual(snapshot.tutorialPack?.packID, "alma-first-look-image-analysis")
        XCTAssertEqual(snapshot.tutorialPack?.selectedSectionID, "01-imhead-continuum-header")
        XCTAssertEqual(snapshot.tutorialPack?.inputs.map(\.status), [.staged, .missing])
        XCTAssertEqual(
            snapshot.tutorialPack.map { Array($0.sections.map(\.id).prefix(4)) },
            [
                "01-imhead-continuum-header",
                "02-imstat-continuum-statistics",
                "03-immoments-n2hp-moment-map",
                "04-exportfits-products"
            ]
        )
        XCTAssertTrue(snapshot.probeDiagnostics.contains("Tutorial input twhya_cont.image: staged"))
        XCTAssertTrue(snapshot.probeDiagnostics.contains("Tutorial input twhya_n2hp.image: missing"))
    }

    func testOpenTutorialPackUsesImageProbeMetadataForInspector() throws {
        let packURL = try makeTemporaryTutorialPack(stagedInputPaths: ["twhya_cont.image"])
        defer { removeTemporaryTutorialPack(packURL) }
        let imageURL = packURL.appendingPathComponent("twhya_cont.image").standardizedFileURL
        let probedDataset = DatasetSummary(
            id: imageURL.path,
            name: "twhya_cont.image",
            path: imageURL.path,
            kind: .imageCube,
            size: "250 x 250 x 1 x 1",
            units: "Jy/beam",
            sizeBytes: 369_373,
            fields: [],
            spectralWindows: [],
            scans: [],
            arrays: [],
            observations: [],
            antennas: [],
            intents: [],
            feeds: [],
            correlations: [],
            columns: ["map"],
            dataColumns: [],
            subtables: [],
            shape: [250, 250, 1, 1],
            notes: "Recognized by opening the path as a casa-rs image.",
            diagnostics: [
                "Pixel type: float32",
                "Direction ref=J2000 axes=Right Ascension/Declination",
                "Beam 0: major=0.5 arcsec minor=0.4 arcsec pa=75 deg"
            ]
        )
        let store = WorkbenchStore(
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(
                        name: "inputs",
                        rootPath: packURL.path,
                        datasets: [probedDataset],
                        source: .probed
                    ),
                    diagnostics: ["probed tutorial inputs"]
                )
            ),
            taskCatalogClient: StubTaskCatalogClient(tasks: [makeImheadTaskCatalogEntry()]),
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImheadTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.openTutorialPack(path: packURL.path)

        let dataset = try XCTUnwrap(store.state.selectedDataset)
        XCTAssertEqual(dataset.name, "twhya_cont.image")
        XCTAssertEqual(dataset.size, "250 x 250 x 1 x 1")
        XCTAssertEqual(dataset.shape, [250, 250, 1, 1])
        XCTAssertEqual(dataset.units, "Jy/beam")
        XCTAssertTrue(dataset.notes.contains("Tutorial pack input: TW Hya continuum image"))
        XCTAssertTrue(dataset.notes.contains("Recognized by opening the path as a casa-rs image."))
        XCTAssertTrue(dataset.diagnostics.contains("registry_key=alma/first-look/twhya/continuum-image"))
        XCTAssertTrue(dataset.diagnostics.contains("Pixel type: float32"))
        XCTAssertTrue(store.state.probeDiagnostics.contains("probed tutorial inputs"))
    }

    func testOpenTutorialPackMarksUnrecognizedStagedImageForInspector() throws {
        let packURL = try makeTemporaryTutorialPack(stagedInputPaths: ["twhya_cont.image"])
        defer { removeTemporaryTutorialPack(packURL) }
        let store = WorkbenchStore(
            probeClient: StubProjectProbeClient(
                result: ProjectFixtureProbe(
                    project: ProjectFixture(
                        name: "inputs",
                        rootPath: packURL.path,
                        datasets: [],
                        source: .probed
                    ),
                    diagnostics: ["input probe completed without recognized datasets"]
                )
            ),
            taskCatalogClient: StubTaskCatalogClient(tasks: [makeImheadTaskCatalogEntry()]),
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImheadTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.openTutorialPack(path: packURL.path)

        let dataset = try XCTUnwrap(store.state.selectedDataset)
        XCTAssertEqual(dataset.name, "twhya_cont.image")
        XCTAssertTrue(
            dataset.diagnostics.contains {
                $0.contains("Image validation failed: cannot open or read CASA image")
            }
        )
        XCTAssertTrue(store.state.probeDiagnostics.contains("input probe completed without recognized datasets"))
    }

    func testOpenTutorialSectionTaskAppliesGuiImheadParameters() throws {
        let packURL = try makeTemporaryTutorialPack(stagedInputPaths: ["twhya_cont.image"])
        defer { removeTemporaryTutorialPack(packURL) }
        let store = WorkbenchStore(
            taskCatalogClient: StubTaskCatalogClient(tasks: [makeImheadTaskCatalogEntry()]),
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImheadTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.openTutorialPack(path: packURL.path)
        store.openTutorialSectionTask("01-imhead-continuum-header")

        let snapshot = store.debugSnapshot()
        XCTAssertEqual(store.state.activeTaskID, "imhead")
        XCTAssertEqual(snapshot.activeTab, "Image Header")
        XCTAssertEqual(snapshot.activeTaskID, "imhead")
        XCTAssertEqual(store.state.tabs.last?.taskID, "imhead")
        XCTAssertEqual(
            store.state.genericTaskValues["imhead"]?["image_path"],
            packURL.appendingPathComponent("twhya_cont.image").standardizedFileURL.path
        )
        XCTAssertEqual(store.state.genericTaskValues["imhead"]?["mode"], "summary")
        XCTAssertEqual(store.state.genericTaskToggles["imhead"]?["json"], true)
        XCTAssertEqual(snapshot.activeTaskValues["mode"], "summary")
        XCTAssertEqual(snapshot.activeTaskToggles["json"], true)
        XCTAssertEqual(store.state.taskRun.requestSummary, "image_path=twhya_cont.image, mode=summary, json=true")
        XCTAssertTrue(store.state.lastErrors.isEmpty)
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
            source: .tutorialPack
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

        XCTAssertNil(store.state.genericTaskValues["imstat"]?["image_path"])
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
            source: .tutorialPack
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
        XCTAssertTrue(applycalSchema.arguments.contains { argument in
            argument.id == "mode" && argument.hiddenInTUI && argument.default == "apply"
        })

        let gencalSchema = try UniFFITaskUISchemaClient().loadTaskUISchema(taskID: "gencal")
        XCTAssertTrue(gencalSchema.arguments.contains { argument in
            argument.id == "caltype"
                && argument.parameterType == "gencal_type"
                && argument.parser.choices?.contains("opac") == true
        })
    }

    func testImagerTaskSchemaExposesTutorialControlsAndManagedOutput() throws {
        let schema = try UniFFITaskUISchemaClient().loadTaskUISchema(taskID: "imager")
        let argumentIDs = Set(schema.arguments.filter { !$0.hiddenInTUI }.map(\.id))
        let tutorialArguments = [
            "ms", "imagename", "imsize", "cell_arcsec", "field", "phasecenter_field",
            "spw", "datacolumn", "specmode", "channel_count", "start", "width",
            "outframe", "restfreq", "deconvolver", "weighting", "robust",
            "gridder", "perchanweightdensity", "restoringbeam", "niter", "nmajor", "gain",
            "threshold_jy", "usemask", "noisethreshold", "sidelobethreshold",
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

    func testGenericTaskArgumentsUseSchemaFlagsChoicesAndToggles() throws {
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
            {"id":"vis","label":"MeasurementSet","order":0,"parser":{"kind":"option","flags":["--vis"],"metavar":"MS","choices":[]},"value_kind":"path","required":true,"default":null,"help":"","group":"Input","advanced":false,"hidden_in_tui":false},
            {"id":"mode","label":"Mode","order":1,"parser":{"kind":"option","flags":["--mode"],"metavar":"MODE","choices":["summary","manual"]},"value_kind":"choice","required":true,"default":"summary","help":"","group":"Flagging","advanced":false,"hidden_in_tui":false},
            {"id":"flagbackup","label":"Backup","order":2,"parser":{"kind":"toggle","true_flags":["--flagbackup"],"false_flags":["--no-flagbackup"]},"value_kind":"bool","required":false,"default":"true","help":"","group":"Safety","advanced":false,"hidden_in_tui":false}
          ]
        }
        """.utf8))
        let request = GenericTaskRequest(
            runID: "run-1",
            task: TaskCatalogEntry(
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
            ),
            schema: schema,
            values: ["vis": "/data/input.ms", "mode": "summary"],
            toggles: ["flagbackup": false]
        )

        XCTAssertEqual(
            try ProcessGenericTaskClient.arguments(for: request),
            ["--vis", "/data/input.ms", "--mode", "summary", "--no-flagbackup"]
        )
    }

    func testGenericTaskArgumentsInvokeHiddenDefaultOptions() throws {
        let schema = try JSONDecoder().decode(TaskUISchema.self, from: Data("""
        {
          "schema_version": 1,
          "command_id": "applycal",
          "invocation_name": "calibrate",
          "display_name": "Applycal",
          "category": "Calibration",
          "summary": "Apply native CASA-style calibration.",
          "usage": "calibrate --mode apply --ms <input.ms>",
          "arguments": [
            {"id":"mode","label":"Mode","order":0,"parser":{"kind":"option","flags":["--mode"],"metavar":"MODE","choices":["apply"]},"value_kind":"choice","required":false,"default":"apply","help":"","group":"Mode","advanced":true,"hidden_in_tui":true},
            {"id":"measurement_set","label":"MeasurementSet","order":1,"parser":{"kind":"option","flags":["--ms"],"metavar":"MS","choices":[]},"value_kind":"path","parameter_type":"measurement_set_path","required":true,"default":null,"help":"","group":"Input","advanced":false,"hidden_in_tui":false},
            {"id":"ui_schema","label":"UI Schema","order":2,"parser":{"kind":"action","flags":["--ui-schema"],"action":"ui_schema"},"value_kind":"bool","required":false,"default":null,"help":"","group":"Meta","advanced":true,"hidden_in_tui":true}
          ]
        }
        """.utf8))
        let request = GenericTaskRequest(
            runID: "run-1",
            task: TaskCatalogEntry(
                id: "applycal",
                category: "Calibration",
                displayName: "Applycal",
                binaryName: "calibrate",
                cargoPackage: "casa-calibration",
                overrideEnv: "CASARS_CALIBRATE_BIN",
                shellKind: "workflow",
                interaction: "one_shot",
                browserKind: nil,
                datasetKinds: ["measurement_set", "calibration_table"],
                schemaSource: "embedded_or_binary",
                showInTUI: true,
                showInSwift: true,
                includeInSuite: true
            ),
            schema: schema,
            values: ["measurement_set": "/data/input.ms"],
            toggles: [:]
        )

        XCTAssertEqual(
            try ProcessGenericTaskClient.arguments(for: request),
            ["--mode", "apply", "--ms", "/data/input.ms"]
        )
    }

    func testGenericTaskArgumentsInvokeHiddenDefaultPositionals() throws {
        let schema = try UniFFITaskUISchemaClient().loadTaskUISchema(taskID: "imhead")
        let request = GenericTaskRequest(
            runID: "run-1",
            task: TaskCatalogEntry(
                id: "imhead",
                category: "Images",
                displayName: "Image Header",
                binaryName: "imexplore",
                cargoPackage: "casa-images",
                overrideEnv: "CASARS_IMEXPLORE_BIN",
                shellKind: "workflow",
                interaction: "one_shot",
                browserKind: nil,
                datasetKinds: ["image"],
                schemaSource: "embedded_or_binary",
                showInTUI: true,
                showInSwift: true,
                includeInSuite: true
            ),
            schema: schema,
            values: ["image_path": "/data/image.im", "mode": "list"],
            toggles: ["json": true]
        )

        XCTAssertEqual(
            try ProcessGenericTaskClient.arguments(for: request),
            ["imhead", "/data/image.im", "--json", "--mode", "list"]
        )
    }

    func testGenericTaskCreatesParentDirectoriesForOutputPaths() throws {
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
            {"id":"fitsimage","label":"FITS","order":1,"parser":{"kind":"positional","metavar":"fitsimage"},"value_kind":"path","parameter_type":"output_fits_path","required":true,"default":null,"help":"","group":"Output","advanced":false,"hidden_in_tui":false},
            {"id":"overwrite","label":"Overwrite","order":2,"parser":{"kind":"toggle","true_flags":["--overwrite"],"false_flags":["--no-overwrite"]},"value_kind":"bool","required":false,"default":"true","help":"","group":"Output","advanced":false,"hidden_in_tui":false}
          ]
        }
        """.utf8))
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
            schema: schema,
            values: ["imagename": "twhya_cont.image", "fitsimage": "casa-rs-runs/twhya_cont.fits"],
            toggles: ["overwrite": true],
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
            source: .tutorialPack
        )
        state.selectedDatasetID = imageURL.path
        state.taskCatalog = [makeImheadTaskCatalogEntry()]
        state.activeTaskID = "imhead"
        state.genericTaskValues["imhead"] = ["image_path": imageURL.path, "mode": "summary"]
        state.genericTaskToggles["imhead"] = ["json": true]

        let store = WorkbenchStore(
            state: state,
            genericTaskClient: taskClient,
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeImheadTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.loadTaskUISchemaIfNeeded("imhead")
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

    func testSimobserveFamilyRequestSavesReopensEditsCanonicalJSON() throws {
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
            taskUISchemaClient: StubTaskUISchemaClient(schema: try makeSimobserveTaskUISchema()),
            taskExecutionMatrixClient: StubTaskExecutionMatrixClient(rows: [])
        )

        store.loadTaskUISchemaIfNeeded("simobserve")
        store.setGenericTaskValue(taskID: "simobserve", argumentID: "request_kind", value: "family")
        store.setGenericTaskValue(taskID: "simobserve", argumentID: "request_json", value: "requests/family.json")
        store.setGenericTaskValue(
            taskID: "simobserve",
            argumentID: "source_model",
            value: #"{"kind":"fits_image","path":"model.fits"}"#
        )
        store.setGenericTaskValue(taskID: "simobserve", argumentID: "telescope", value: "ALMA")
        store.setGenericTaskValue(taskID: "simobserve", argumentID: "array_config", value: "synthetic-aca")
        store.setGenericTaskValue(taskID: "simobserve", argumentID: "band", value: "Band 3")
        store.setGenericTaskValue(taskID: "simobserve", argumentID: "target_ms_size_gib", value: "0.02")
        store.setGenericTaskValue(taskID: "simobserve", argumentID: "output_ms", value: "products/family.ms")
        store.setGenericTaskValue(taskID: "simobserve", argumentID: "polarizations", value: "4")
        store.setGenericTaskValue(taskID: "simobserve", argumentID: "ms_channels", value: "8")
        store.setGenericTaskValue(taskID: "simobserve", argumentID: "image_channels", value: "2")
        store.setGenericTaskValue(taskID: "simobserve", argumentID: "pointing_count", value: "3")
        store.setGenericTaskValue(taskID: "simobserve", argumentID: "imaging_mode", value: "mosaic")
        store.setGenericTaskValue(taskID: "simobserve", argumentID: "worker_policy", value: "fixed")
        store.setGenericTaskValue(taskID: "simobserve", argumentID: "row_workers", value: "2")
        store.setGenericTaskValue(taskID: "simobserve", argumentID: "channel_workers", value: "3")

        let requestURL = rootURL.appendingPathComponent("requests/family.json")
        store.saveActiveGenericTaskRequest(to: requestURL.path)

        let saved = try JSONSerialization.jsonObject(with: Data(contentsOf: requestURL)) as? [String: Any]
        XCTAssertEqual(saved?["kind"] as? String, "family")
        let request = try XCTUnwrap(saved?["request"] as? [String: Any])
        XCTAssertNil(request["request_kind"])
        XCTAssertNil(request["request_json"])
        XCTAssertEqual(request["telescope"] as? String, "ALMA")
        XCTAssertEqual(request["array_config"] as? String, "synthetic-aca")
        XCTAssertEqual(request["polarizations"] as? Int, 4)
        XCTAssertEqual(request["ms_channels"] as? Int, 8)
        XCTAssertEqual(request["worker_policy"] as? String, "fixed")
        XCTAssertEqual(request["row_workers"] as? Int, 2)
        XCTAssertEqual(request["channel_workers"] as? Int, 3)

        store.setGenericTaskValue(taskID: "simobserve", argumentID: "pointing_count", value: "99")
        store.loadGenericTaskRequest(from: requestURL.path)

        XCTAssertEqual(store.state.genericTaskValues["simobserve"]?["request_kind"], "family")
        XCTAssertEqual(store.state.genericTaskValues["simobserve"]?["request_json"], "requests/family.json")
        XCTAssertEqual(store.state.genericTaskValues["simobserve"]?["pointing_count"], "3")
        XCTAssertEqual(store.state.genericTaskValues["simobserve"]?["worker_policy"], "fixed")

        store.setGenericTaskValue(taskID: "simobserve", argumentID: "pointing_count", value: "5")
        store.saveActiveGenericTaskRequest(to: requestURL.path)
        let edited = try JSONSerialization.jsonObject(with: Data(contentsOf: requestURL)) as? [String: Any]
        let editedRequest = try XCTUnwrap(edited?["request"] as? [String: Any])
        XCTAssertEqual(editedRequest["pointing_count"] as? Int, 5)
    }

    func testProcessGenericTaskRunsSimobserveFamilyThroughSavedJsonRun() throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-simobserve-run-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: rootURL) }
        let binaryURL = try writeStubSimobserveBinary(
            rootURL: rootURL,
            script: """
            #!/bin/sh
            if [ "$1" = "--json-run" ]; then
              cat "$2"
              exit 0
            fi
            echo "unexpected arguments: $*" >&2
            exit 2
            """
        )
        setenv("CASARS_SIMOBSERVE_BIN", binaryURL.path, 1)
        defer { unsetenv("CASARS_SIMOBSERVE_BIN") }

        let request = try makeSimobserveFamilyGenericTaskRequest(rootURL: rootURL)
        let client = ProcessGenericTaskClient(queue: DispatchQueue(label: "test.simobserve.family"))
        let semaphore = DispatchSemaphore(value: 0)
        var event: GenericTaskEvent?
        _ = try client.startTask(request: request) {
            event = $0
            semaphore.signal()
        }
        XCTAssertEqual(semaphore.wait(timeout: .now() + 5), .success)

        guard case let .succeeded(result) = event else {
            XCTFail("expected simobserve family success")
            return
        }
        XCTAssertEqual(result.arguments.first, "--json-run")
        let requestJSONPath = try XCTUnwrap(result.requestJSONPath)
        XCTAssertTrue(FileManager.default.fileExists(atPath: requestJSONPath))
        XCTAssertEqual(result.arguments.dropFirst().first, requestJSONPath)
        let payload = try JSONSerialization.jsonObject(with: Data(result.stdout.utf8)) as? [String: Any]
        XCTAssertEqual(payload?["kind"] as? String, "family")
        let familyRequest = try XCTUnwrap(payload?["request"] as? [String: Any])
        XCTAssertEqual(familyRequest["worker_policy"] as? String, "fixed")
        XCTAssertEqual(familyRequest["row_workers"] as? Int, 2)
        XCTAssertEqual(familyRequest["channel_workers"] as? Int, 3)
    }

    func testProcessGenericTaskSurfacesSimobserveFamilyValidationFailure() throws {
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

        let request = try makeSimobserveFamilyGenericTaskRequest(rootURL: rootURL)
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

        XCTAssertEqual(store.state.genericTaskValues["flagdata"]?["vis"], "/data/project/input.ms")
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

    func testGenericImagerArgumentsIncludeTutorialParametersAndManagedOutput() throws {
        let schema = try UniFFITaskUISchemaClient().loadTaskUISchema(taskID: "imager")
        let request = GenericTaskRequest(
            runID: "run-1",
            task: TaskCatalogEntry(
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
            ),
            schema: schema,
            values: [
                "ms": "/data/twhya.ms",
                "imagename": "/data/casa-rs-runs/twhya",
                "imsize": "250",
                "cell_arcsec": "0.08",
                "field": "5",
                "phasecenter_field": "5",
                "spw": "0",
                "datacolumn": "DATA",
                "specmode": "cube",
                "channel_count": "15",
                "start": "0.0km/s",
                "width": "0.5km/s",
                "outframe": "LSRK",
                "restfreq": "372.67249GHz",
                "deconvolver": "mtmfs",
                "weighting": "briggsbwtaper",
                "robust": "0.5",
                "gridder": "wproject",
                "restoringbeam": "common",
                "niter": "1000",
                "nmajor": "4",
                "gain": "0.1",
                "threshold_jy": "0.00015",
                "usemask": "auto-multithresh",
                "noisethreshold": "4.25",
                "sidelobethreshold": "2.0",
                "lownoisethreshold": "1.5",
                "minbeamfrac": "0.3",
                "negativethreshold": "15.0",
                "scales": "0,6,10,30,60",
                "smallscalebias": "0.9",
                "wterm": "wproject",
                "wprojplanes": "-1",
                "nterms": "2",
                "savemodel": "modelcolumn",
                "outlierfile": "/data/outliers.txt",
                "pblimit": "-0.01"
            ],
            toggles: [
                "perchanweightdensity": true,
                "write_pb": true,
                "pbcor": true,
                "write_preview_pngs": false
            ]
        )

        let arguments = try ProcessGenericTaskClient.arguments(for: request)
        XCTAssertTrue(arguments.contains("--managed-output"))
        XCTAssertTrue(arguments.contains("true"))
        for flag in [
            "--specmode", "--channel-count", "--start", "--width", "--outframe",
            "--restfreq", "--deconvolver", "--weighting", "--perchanweightdensity",
            "--restoringbeam", "--nmajor", "--gain", "--threshold-jy", "--usemask",
            "--noisethreshold", "--sidelobethreshold", "--lownoisethreshold",
            "--minbeamfrac", "--negativethreshold", "--scales", "--smallscalebias",
            "--gridder", "--wterm", "--wprojplanes", "--nterms", "--savemodel", "--outlierfile",
            "--write-pb", "--pbcor", "--pblimit", "--no-preview-pngs"
        ] {
            XCTAssertTrue(arguments.contains(flag), "missing \(flag)")
        }
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
        store.runTask()

        XCTAssertTrue(taskClient.requests.isEmpty)
        XCTAssertEqual(store.state.taskRun.state, .failed)
        XCTAssertTrue(store.state.taskRun.diagnostics.contains { $0.contains("Confirm this task") })

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
        XCTAssertEqual(state.selectedView, "cells")
        XCTAssertEqual(state.snapshot?.view, "cells")
        XCTAssertEqual(state.snapshot?.contentLines.first, "Cells  row=1/12  col=1/3  focus=Main")
        XCTAssertEqual(tableClient.paths, ["/data/MAIN"])
        XCTAssertEqual(store.debugSnapshot().tableBrowsers[tableDataset.id]?.inspectorTitle, "Column DATA")

        store.setTableBrowserView("keywords", datasetID: tableDataset.id)
        XCTAssertEqual(store.state.tableBrowsers[tableDataset.id]?.selectedView, "keywords")
        XCTAssertEqual(tableClient.requests.map(\.selectedView), ["cells", "keywords"])
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

        XCTAssertEqual(store.state.activeTabID, "tab-tablebrowser-\(msDataset.id)")
        XCTAssertEqual(store.state.tabs.last?.kind, .tableBrowser)
        XCTAssertEqual(store.state.tabs.last?.title, "Table: example.ms")
        XCTAssertEqual(tableClient.paths, [msDataset.path])
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
        XCTAssertEqual(store.state.genericTaskValues["imager"]?["ms"], "probed.ms")
        XCTAssertEqual(store.state.genericTaskValues["imager"]?["field"], "0")
        XCTAssertEqual(store.state.genericTaskValues["imager"]?["phasecenter_field"], "")
        XCTAssertEqual(store.state.genericTaskValues["imager"]?["spw"], "0")
        XCTAssertEqual(store.state.genericTaskValues["imager"]?["imagename"], "casa-rs-runs/imager-1/probed.ms-imager")
        XCTAssertEqual(store.state.genericTaskToggles["imager"]?["dirty_only"], true)
        XCTAssertTrue(store.state.lastErrors.contains("AI chat is not connected yet"))
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
        XCTAssertEqual(progress.resourceActivities.count, 5)
        let sourceResource = try XCTUnwrap(progress.resourceActivities.first)
        XCTAssertEqual(sourceResource.id, "source-stream")
        XCTAssertEqual(sourceResource.detail, "128.7k rows / 3.8 GB planned")
        XCTAssertEqual(sourceResource.state, .busy)
        XCTAssertEqual(sourceResource.sectionStartFraction, 0.2, accuracy: 0.001)
        XCTAssertEqual(sourceResource.sectionEndFraction, 0.4, accuracy: 0.001)
        XCTAssertEqual(sourceResource.activeThreads, 2)
        let gridResource = try XCTUnwrap(progress.resourceActivities.first { $0.id == "visibility-grid" })
        XCTAssertEqual(gridResource.detail, "shared / 2.4 GB planned")
        XCTAssertEqual(gridResource.state, .idle)
        XCTAssertEqual(gridResource.activeThreads, 0)
        let planeResource = try XCTUnwrap(progress.resourceActivities.first { $0.id == "plane-state" })
        XCTAssertEqual(planeResource.detail, "47 planes / 2.4 GB planned")
        XCTAssertEqual(planeResource.state, .idle)
        XCTAssertEqual(planeResource.activeThreads, 0)
        let deconvolverResource = try XCTUnwrap(progress.resourceActivities.first { $0.id == "deconvolver" })
        XCTAssertEqual(deconvolverResource.detail, "minor / 2.4 GB planned")
        let productResource = try XCTUnwrap(progress.resourceActivities.first { $0.id == "product-scratch" })
        XCTAssertEqual(productResource.detail, "10.9 GB planned")
        XCTAssertTrue(progress.sourceStreamIsActive)
        guard case .diagnostic(let diagnostic) = records[1] else {
            return XCTFail("expected diagnostic record")
        }
        XCTAssertEqual(diagnostic, "plain stderr")
    }

    func testImagerProgressUsesObservabilityResourcesWhenPresent() throws {
        var parser = ImagerProgressStderrParser()
        let progressJSON = #"{"schema_version":1,"sequence":5,"elapsed_ms":1250,"phase":"residual_refresh","summary":"refreshing residual","ms_read":{"total_rows":1000,"total_channels":32,"row_start":100,"row_end":300,"channel_start":8,"channel_end":16},"output_cube":{"x_pixels":128,"y_pixels":128,"z_planes":16,"active_plane_start":4,"active_plane_end":8},"runtime":{"active_threads":4,"total_threads":8,"gpu_active":true,"backend":"metal","active_resources":["visibility-grid","plane-state"],"memory":{"memory_target_bytes":17179869184,"planned_active_bytes":12884901888,"source_stream_buffer_bytes":3221225472,"product_scratch_bytes":5368709120,"active_planes":4,"row_block_rows":292000,"memory_target_source":"test"}},"observability":{"schema_version":1,"resources":[{"id":"source-stream","label":"Source Stream","state":"idle","lease_count":0,"active_threads":0,"gpu_active":false,"memory":{"planned_bytes":3221225472,"row_block_rows":292000}},{"id":"visibility-grid","label":"Grid/FFT","state":"active","lease_count":1,"active_threads":4,"gpu_active":true,"owner":"residual_refresh"},{"id":"plane-state","label":"Plane State","state":"active","lease_count":1,"active_threads":4,"gpu_active":true,"owner":"residual_refresh","memory":{"active_planes":4}},{"id":"deconvolver","label":"Deconvolver","state":"idle","lease_count":0,"active_threads":0,"gpu_active":false},{"id":"product-scratch","label":"Products","state":"idle","lease_count":0,"active_threads":0,"gpu_active":false,"memory":{"planned_bytes":5368709120}}],"active_spans":[{"id":"residual_refresh","name":"refreshing residual","stage_kind":"residual_refresh","state":"running","resource_ids":["visibility-grid","plane-state"],"elapsed_ms":1250}],"memory_target_bytes":17179869184,"memory_target_source":"test","memory_ledger":{"entries":[{"kind":"source-buffer","label":"Source stream","resource_id":"source-stream","planned_bytes":3221225472,"tracked_live_bytes":3221225472,"row_block_rows":292000,"confidence":"planned"},{"kind":"grid-fft-scratch","label":"Grid / FFT scratch","resource_id":"visibility-grid","confidence":"unknown","note":"not yet attributed"},{"kind":"plane-state","label":"Plane state","resource_id":"plane-state","active_planes":4,"confidence":"unknown"},{"kind":"deconvolver-scratch","label":"Deconvolver scratch","resource_id":"deconvolver","confidence":"unknown"},{"kind":"products","label":"Products","resource_id":"product-scratch","planned_bytes":5368709120,"tracked_live_bytes":5368709120,"confidence":"planned"},{"kind":"process-baseline","label":"Process RSS","resource_id":"process-runtime","process_rss_bytes":10737418240,"process_peak_rss_bytes":12884901888,"confidence":"measured"},{"kind":"untracked-resident","label":"Untracked resident","resource_id":"process-runtime","process_rss_bytes":10737418240,"untracked_bytes":2147483648,"confidence":"estimated"}],"planned_total_bytes":8589934592,"tracked_live_total_bytes":8589934592,"tracked_high_water_total_bytes":0,"process_rss_bytes":10737418240,"process_peak_rss_bytes":12884901888,"untracked_resident_bytes":2147483648}}}"#

        let records = parser.append(imagerProgressStderrPrefix + progressJSON + "\n", runID: "imager-obs", state: .running)

        guard case .progress(let progress) = records.first else {
            return XCTFail("expected progress record")
        }
        let observability = try XCTUnwrap(progress.observability)
        XCTAssertEqual(observability.resources.count, 5)
        XCTAssertEqual(observability.activeSpans.first?.resourceIDs, ["visibility-grid", "plane-state"])
        let ledger = try XCTUnwrap(observability.memoryLedger)
        XCTAssertEqual(ledger.entries.count, 7)
        XCTAssertEqual(ledger.trackedLiveTotalBytes, 8_589_934_592)
        XCTAssertEqual(ledger.untrackedResidentBytes, 2_147_483_648)
        XCTAssertEqual(ledger.entry(for: "visibility-grid")?.confidence, "unknown")
        let sourceResource = try XCTUnwrap(progress.resourceActivities.first { $0.id == "source-stream" })
        XCTAssertEqual(sourceResource.detail, "292.0k rows / 3.2 GB planned")
        XCTAssertEqual(sourceResource.state, .idle)
        let gridResource = try XCTUnwrap(progress.resourceActivities.first { $0.id == "visibility-grid" })
        XCTAssertEqual(gridResource.name, "Grid/FFT")
        XCTAssertEqual(gridResource.detail, "residual_refresh")
        XCTAssertEqual(gridResource.state, .busy)
        XCTAssertEqual(gridResource.activeThreads, 4)
        XCTAssertTrue(gridResource.gpuActive)
        let planeResource = try XCTUnwrap(progress.resourceActivities.first { $0.id == "plane-state" })
        XCTAssertEqual(planeResource.detail, "4 planes")
        XCTAssertEqual(planeResource.sectionStartFraction, 0.25, accuracy: 0.001)
        XCTAssertEqual(planeResource.sectionEndFraction, 0.5, accuracy: 0.001)
        let productResource = try XCTUnwrap(progress.resourceActivities.first { $0.id == "product-scratch" })
        XCTAssertEqual(productResource.detail, "5.4 GB planned")
        XCTAssertEqual(productResource.byteFraction, Double(5_368_709_120) / Double(17_179_869_184), accuracy: 0.001)
    }

    func testImagerProgressExplicitResourceOwnershipOverridesPhaseFallback() throws {
        var parser = ImagerProgressStderrParser()
        let progressJSON = #"{"schema_version":1,"sequence":3,"elapsed_ms":1500,"phase":"reading_ms","summary":"resource ownership","ms_read":{"total_rows":100,"total_channels":16,"row_start":20,"row_end":40,"channel_start":4,"channel_end":8},"output_cube":{"x_pixels":64,"y_pixels":64,"z_planes":16,"active_plane_start":4,"active_plane_end":8},"runtime":{"active_threads":4,"total_threads":8,"gpu_active":true,"backend":"explicit test","active_resources":["visibility-grid","plane-state","product-scratch"],"memory":{"memory_target_bytes":17179869184,"planned_active_bytes":17179863154,"source_stream_buffer_bytes":3804104045,"product_scratch_bytes":10945390173,"active_planes":4,"row_block_rows":128704,"memory_target_source":"system_half"}}}"#

        let records = parser.append(imagerProgressStderrPrefix + progressJSON + "\n", runID: "imager-8", state: .running)

        guard case .progress(let progress) = records.first else {
            return XCTFail("expected progress record")
        }
        XCTAssertEqual(progress.runtime.activeResourceIDs, ["visibility-grid", "plane-state", "product-scratch"])
        XCTAssertEqual(progress.runtime.activeResourceIDsAreAuthoritative, true)
        let sourceResource = try XCTUnwrap(progress.resourceActivities.first { $0.id == "source-stream" })
        XCTAssertEqual(sourceResource.state, .idle)
        let gridResource = try XCTUnwrap(progress.resourceActivities.first { $0.id == "visibility-grid" })
        XCTAssertEqual(gridResource.state, .busy)
        XCTAssertEqual(gridResource.activeThreads, 4)
        XCTAssertTrue(gridResource.gpuActive)
        let planeResource = try XCTUnwrap(progress.resourceActivities.first { $0.id == "plane-state" })
        XCTAssertEqual(planeResource.state, .busy)
        let deconvolverResource = try XCTUnwrap(progress.resourceActivities.first { $0.id == "deconvolver" })
        XCTAssertEqual(deconvolverResource.state, .idle)
        let productResource = try XCTUnwrap(progress.resourceActivities.first { $0.id == "product-scratch" })
        XCTAssertEqual(productResource.state, .busy)
        XCTAssertEqual(productResource.activeThreads, 2)

        let finishedRecords = parser.append(imagerProgressStderrPrefix + progressJSON + "\n", runID: "imager-8", state: .succeeded)

        guard case .progress(let finishedProgress) = finishedRecords.first else {
            return XCTFail("expected completed progress record")
        }
        XCTAssertTrue(finishedProgress.resourceActivities.allSatisfy { $0.state == .idle })
        XCTAssertTrue(finishedProgress.resourceActivities.allSatisfy { $0.activeThreads == 0 })
        XCTAssertFalse(finishedProgress.sourceStreamIsActive)
    }

    func testImagerProgressEmptyResourceOwnershipClearsPhaseFallback() throws {
        var parser = ImagerProgressStderrParser()
        let progressJSON = #"{"schema_version":1,"sequence":5,"elapsed_ms":3000,"phase":"refreshing residual","summary":"idle after residual refresh","output_cube":{"x_pixels":64,"y_pixels":64,"z_planes":1,"active_plane_start":0,"active_plane_end":1},"deconvolution":{"phase":"refreshing residual","major_cycle":1,"major_cycle_limit":-1,"minor_iterations":1000,"minor_iteration_limit":3000,"components_cleaned":1000,"peak_residual_mjy_per_beam":2.7,"target_residual_mjy_per_beam":0.0,"residual_history_mjy_per_beam":[3.1,2.7]},"runtime":{"active_threads":0,"total_threads":8,"gpu_active":false,"backend":"idle","active_resources":[],"memory":{"memory_target_bytes":17179869184,"planned_active_bytes":17179863154,"source_stream_buffer_bytes":3804104045,"product_scratch_bytes":10945390173,"active_planes":1,"row_block_rows":128704,"memory_target_source":"system_half"}}}"#

        let records = parser.append(imagerProgressStderrPrefix + progressJSON + "\n", runID: "imager-10", state: .running)

        guard case .progress(let progress) = records.first else {
            return XCTFail("expected progress record")
        }
        XCTAssertEqual(progress.runtime.activeResourceIDs, [])
        XCTAssertEqual(progress.runtime.activeResourceIDsAreAuthoritative, true)
        XCTAssertTrue(progress.resourceActivities.allSatisfy { $0.state == .idle })
        XCTAssertTrue(progress.resourceActivities.allSatisfy { $0.activeThreads == 0 })
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
        XCTAssertEqual(progress.resourceActivities.filter(\.isBusy).map(\.id), [
            "source-stream",
            "visibility-grid",
            "plane-state"
        ])
        XCTAssertEqual(progress.runtime.activeResourceThreadCounts["source-stream"], 1)
        XCTAssertEqual(progress.runtime.activeResourceThreadCounts["visibility-grid"], 4)
        XCTAssertEqual(progress.runtime.activeResourceThreadCounts["plane-state"], 4)
        XCTAssertEqual(progress.resourceActivities.first { $0.id == "source-stream" }?.activeThreads, 1)
        XCTAssertTrue(progress.sourceStreamIsActive)
        XCTAssertEqual(progress.resourceActivities.first { $0.id == "visibility-grid" }?.activeThreads, 4)
        XCTAssertEqual(progress.resourceActivities.first { $0.id == "plane-state" }?.activeThreads, 4)
        XCTAssertEqual(progress.resourceActivities.first { $0.id == "product-scratch" }?.activeThreads, 0)
        XCTAssertEqual(progress.resourceActivities.first { $0.id == "deconvolver" }?.state, .idle)
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
        XCTAssertEqual(store.state.genericTaskValues["imager"]?["ms"], "probed.ms")
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
        XCTAssertNil(store.state.genericTaskValues["imager"]?["ms"])
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
        let values = try XCTUnwrap(store.state.genericTaskValues["imager"])
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
        XCTAssertEqual(values["ms"], "large.ms")
        XCTAssertEqual(values["imagename"], "casa-rs-runs/imager-1/large.ms-imager")
        XCTAssertEqual(values["field"], "")
        XCTAssertEqual(values["phasecenter_field"], "0")
        XCTAssertEqual(values["specmode"], "cube")
        XCTAssertEqual(values["gridder"], "mosaic")
        XCTAssertEqual(values["interpolation"], "nearest")
        XCTAssertEqual(values["channel_start"], "0")
        XCTAssertEqual(values["channel_count"], "512")
        XCTAssertEqual(values["imsize"], "1024")
        XCTAssertEqual(values["cell_arcsec"], "1.0")
        XCTAssertEqual(values["weighting"], "briggs")
        XCTAssertEqual(values["niter"], "2048")
        XCTAssertEqual(values["threshold_jy"], "0.0")
        XCTAssertEqual(toggles["dirty_only"], false)
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
        store.setGenericTaskValue(taskID: "imager", argumentID: "cell_arcsec", value: "0.25")
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

        let values = try XCTUnwrap(store.state.genericTaskValues["imager"])
        XCTAssertEqual(values["field"], "5")
        XCTAssertEqual(values["phasecenter_field"], "")
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
        XCTAssertEqual(values["phasecenter_field"], "")
        XCTAssertEqual(values["spw"], "0")
        XCTAssertEqual(values["polarization"], "I")
        XCTAssertEqual(values["imsize"], "250")
        XCTAssertEqual(values["cell_arcsec"], "0.1")
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
        XCTAssertEqual(plotClient.requests.last?.maxPlotPoints, WorkbenchState.defaultMeasurementSetPlotMaxPoints)
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
        printf '%s\\n' '\(imagerProgressStderrPrefix){"schema_version":1,"sequence":1,"elapsed_ms":0,"phase":"starting","summary":"started","work":{"completed_units":0,"total_units":1,"unit_label":"unit","basis":"test","confidence":"exact"},"runtime":{"active_threads":1,"total_threads":1,"gpu_active":false,"backend":"test"}}' >&2
        cat "\(resultURL.path)"
        """
        try helperScript.write(to: helperURL, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: helperURL.path)

        var task = makeImagerTaskCatalogEntry()
        task.binaryName = helperURL.path
        let request = GenericTaskRequest(
            runID: "large-stdout",
            task: task,
            schema: try makeImagerTaskUISchema(),
            values: [
                "ms": "/data/probed.ms",
                "imagename": outputPrefix,
                "imsize": "256",
                "cell_arcsec": "1.0",
                "specmode": "mfs",
                "weighting": "natural",
                "deconvolver": "hogbom"
            ],
            toggles: ["dirty_only": true]
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
        nextSnapshot.view = request.selectedView
        nextSnapshot.focus = request.focus
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

private func makeTemporaryTutorialPack(
    stagedInputPaths: Set<String> = [],
    templateName: String = "alma-first-look-image-analysis.template.json"
) throws -> URL {
    let fileManager = FileManager.default
    let packURL = fileManager.temporaryDirectory
        .appendingPathComponent("casars-tutorial-pack-\(UUID().uuidString).pack", isDirectory: true)
    try fileManager.createDirectory(at: packURL, withIntermediateDirectories: true)
    let templateURL = repositoryRootURL()
        .appendingPathComponent("resources/tutorial-packs/\(templateName)")
    try fileManager.copyItem(at: templateURL, to: packURL.appendingPathComponent("pack.json"))
    for relativePath in stagedInputPaths {
        let inputURL = packURL.appendingPathComponent(relativePath, isDirectory: true)
        try fileManager.createDirectory(at: inputURL, withIntermediateDirectories: true)
        try "stub tutorial input\n".write(
            to: inputURL.appendingPathComponent("table.dat"),
            atomically: true,
            encoding: .utf8
        )
    }
    return packURL
}

private func removeTemporaryTutorialPack(_ url: URL) {
    try? FileManager.default.removeItem(at: url)
}

private func repositoryRootURL() -> URL {
    var url = URL(fileURLWithPath: #filePath)
    for _ in 0..<5 {
        url.deleteLastPathComponent()
    }
    return url
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
        {"id":"ms","label":"MeasurementSet","order":0,"parser":{"kind":"option","flags":["--ms"],"metavar":"PATH","choices":[]},"value_kind":"path","parameter_type":"measurement_set_path","required":true,"default":null,"help":"","group":"Context","advanced":false,"hidden_in_tui":false},
        {"id":"imagename","label":"Image Prefix","order":1,"parser":{"kind":"option","flags":["--imagename"],"metavar":"PREFIX","choices":[]},"value_kind":"path","parameter_type":"output_image_path","required":true,"default":null,"help":"","group":"Products","advanced":false,"hidden_in_tui":false},
        {"id":"imsize","label":"Image Size","order":2,"parser":{"kind":"option","flags":["--imsize"],"metavar":"PIXELS","choices":[]},"value_kind":"string","required":true,"default":"512","help":"","group":"Stage Parameters","advanced":false,"hidden_in_tui":false},
        {"id":"cell_arcsec","label":"Cell Size","order":3,"parser":{"kind":"option","flags":["--cell-arcsec"],"metavar":"ARCSEC","choices":[]},"value_kind":"float","required":true,"default":"1.0","help":"","group":"Stage Parameters","advanced":false,"hidden_in_tui":false},
        {"id":"datacolumn","label":"Data Column","order":4,"parser":{"kind":"option","flags":["--datacolumn"],"metavar":"NAME","choices":["DATA","CORRECTED_DATA","MODEL_DATA"]},"value_kind":"choice","required":false,"default":null,"help":"","group":"Context","advanced":false,"hidden_in_tui":false},
        {"id":"field","label":"Fields","order":7,"parser":{"kind":"option","flags":["--field"],"metavar":"IDS","choices":[]},"value_kind":"string","required":false,"default":null,"help":"","group":"Context","advanced":false,"hidden_in_tui":false},
        {"id":"phasecenter_field","label":"Phasecenter Field","order":8,"parser":{"kind":"option","flags":["--phasecenter-field"],"metavar":"ID","choices":[]},"value_kind":"string","required":false,"default":null,"help":"","group":"Context","advanced":false,"hidden_in_tui":false},
        {"id":"spw","label":"SPW","order":10,"parser":{"kind":"option","flags":["--spw"],"metavar":"SEL","choices":[]},"value_kind":"string","required":false,"default":null,"help":"","group":"Context","advanced":false,"hidden_in_tui":false},
        {"id":"channel_start","label":"Channel Start","order":11,"parser":{"kind":"option","flags":["--channel-start"],"metavar":"N","choices":[]},"value_kind":"string","required":false,"default":null,"help":"","group":"Context","advanced":true,"hidden_in_tui":false},
        {"id":"channel_count","label":"Channel Count","order":12,"parser":{"kind":"option","flags":["--channel-count"],"metavar":"N","choices":[]},"value_kind":"string","required":false,"default":null,"help":"","group":"Context","advanced":true,"hidden_in_tui":false},
        {"id":"polarization","label":"Corr / Stokes","order":13,"parser":{"kind":"option","flags":["--corr"],"metavar":"PLANE","choices":["I","Q","U","V","XX","YY","RR","LL"]},"value_kind":"choice","required":false,"default":"I","help":"","group":"Context","advanced":true,"hidden_in_tui":false},
        {"id":"specmode","label":"Spectral Mode","order":20,"parser":{"kind":"option","flags":["--specmode"],"metavar":"MODE","choices":["mfs","cube","cubedata"]},"value_kind":"choice","required":true,"default":"mfs","help":"","group":"Stages","advanced":false,"hidden_in_tui":false},
        {"id":"interpolation","label":"Cube Interp","order":21,"parser":{"kind":"option","flags":["--interpolation"],"metavar":"MODE","choices":["nearest","linear","cubic"]},"value_kind":"choice","required":false,"default":null,"help":"","group":"Stages","advanced":true,"hidden_in_tui":false},
        {"id":"dirty_only","label":"Dirty Only","order":30,"parser":{"kind":"toggle","true_flags":["--dirty-only"],"false_flags":[]},"value_kind":"bool","required":false,"default":"false","help":"","group":"Stages","advanced":false,"hidden_in_tui":false},
        {"id":"niter","label":"Iterations","order":31,"parser":{"kind":"option","flags":["--niter"],"metavar":"N","choices":[]},"value_kind":"string","required":false,"default":"0","help":"","group":"Stages","advanced":false,"hidden_in_tui":false},
        {"id":"threshold_jy","label":"Threshold","order":32,"parser":{"kind":"option","flags":["--threshold-jy"],"metavar":"JY","choices":[]},"value_kind":"float","required":false,"default":"0.0","help":"","group":"Stages","advanced":false,"hidden_in_tui":false},
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
        {"id":"image_path","label":"Image","order":0,"parser":{"kind":"option","flags":["--image"],"metavar":"IMAGE","choices":[]},"value_kind":"path","parameter_type":"image_path","required":true,"default":null,"help":"","group":"Input","advanced":false,"hidden_in_tui":false},
        {"id":"mode","label":"Mode","order":1,"parser":{"kind":"option","flags":["--mode"],"metavar":"MODE","choices":["summary","list"]},"value_kind":"choice","required":false,"default":"summary","help":"","group":"Output","advanced":false,"hidden_in_tui":false},
        {"id":"json","label":"JSON","order":2,"parser":{"kind":"toggle","true_flags":["--json"],"false_flags":["--no-json"]},"value_kind":"bool","required":false,"default":"false","help":"","group":"Output","advanced":false,"hidden_in_tui":false}
      ]
    }
    """.utf8))
}

private func makeSimobserveTaskUISchema() throws -> TaskUISchema {
    try JSONDecoder().decode(TaskUISchema.self, from: Data("""
    {
      "schema_version": 1,
      "command_id": "simobserve",
      "invocation_name": "simobserve",
      "display_name": "SimObserve",
      "category": "Simulation",
      "summary": "Generate synthetic MeasurementSets from CLI parameters or saved family JSON.",
      "usage": "simobserve --json-run family.json",
      "arguments": [
        {"id":"request_kind","label":"Request Kind","order":0,"parser":{"kind":"option","flags":[],"metavar":"run|family","choices":["run","family"]},"value_kind":"choice","parameter_type":"simobserve_request_kind","required":false,"default":"run","help":"","group":"Saved JSON","advanced":false,"hidden_in_tui":false},
        {"id":"request_json","label":"Request JSON","order":1,"parser":{"kind":"option","flags":[],"metavar":"PATH","choices":[]},"value_kind":"path","parameter_type":"output_json_path","required":false,"default":".casa-rs/requests/simobserve-family.json","help":"","group":"Saved JSON","advanced":false,"hidden_in_tui":false},
        {"id":"source_model","label":"Source Model","order":2,"parser":{"kind":"option","flags":[],"metavar":"JSON","choices":[]},"value_kind":"string","parameter_type":"json_object","required":false,"default":"{\\"kind\\":\\"analytic_components\\",\\"components\\":[{\\"kind\\":\\"point\\",\\"l_rad\\":0.0,\\"m_rad\\":0.0,\\"spectrum\\":{\\"flux_jy\\":1.0}}]}","help":"","group":"Family Parameters","advanced":false,"hidden_in_tui":false},
        {"id":"telescope","label":"Telescope","order":3,"parser":{"kind":"option","flags":[],"metavar":"NAME","choices":["VLA","ALMA","ACA"]},"value_kind":"choice","parameter_type":"telescope_family","required":false,"default":"VLA","help":"","group":"Family Parameters","advanced":false,"hidden_in_tui":false},
        {"id":"array_config","label":"Array Config","order":4,"parser":{"kind":"option","flags":[],"metavar":"CONFIG","choices":["A","vla.b.cfg","vla.c.cfg","vla.d.cfg","alma.cycle10.5.cfg","aca.cycle10.cfg","synthetic-vla-d","synthetic-alma-compact","synthetic-aca","synthetic-simalma"]},"value_kind":"choice","parameter_type":"array_configuration","required":false,"default":"A","help":"","group":"Family Parameters","advanced":false,"hidden_in_tui":false},
        {"id":"band","label":"Band","order":5,"parser":{"kind":"option","flags":[],"metavar":"BAND","choices":["L","S","C","X","Ku","K","Ka","Q","Band 3","Band 6","Band 7","Band 9"]},"value_kind":"choice","parameter_type":"receiver_band","required":false,"default":"Q","help":"","group":"Family Parameters","advanced":false,"hidden_in_tui":false},
        {"id":"target_ms_size_gib","label":"Target MS Size","order":6,"parser":{"kind":"option","flags":[],"metavar":"GiB","choices":[]},"value_kind":"float","parameter_type":"data_size_gib","required":false,"default":"0.01","help":"","group":"Family Parameters","advanced":false,"hidden_in_tui":false},
        {"id":"output_ms","label":"Family Output MS","order":7,"parser":{"kind":"option","flags":[],"metavar":"PATH","choices":[]},"value_kind":"path","parameter_type":"output_measurement_set_path","required":false,"default":"simobserve-family.ms","help":"","group":"Family Parameters","advanced":false,"hidden_in_tui":false},
        {"id":"polarizations","label":"Polarizations","order":8,"parser":{"kind":"option","flags":[],"metavar":"N","choices":["1","2","4"]},"value_kind":"choice","parameter_type":"integer_count","required":false,"default":"2","help":"","group":"Family Dimensions","advanced":false,"hidden_in_tui":false},
        {"id":"ms_channels","label":"MS Channels","order":9,"parser":{"kind":"option","flags":[],"metavar":"N","choices":[]},"value_kind":"string","parameter_type":"integer_count","required":false,"default":"4","help":"","group":"Family Dimensions","advanced":false,"hidden_in_tui":false},
        {"id":"image_channels","label":"Image Channels","order":10,"parser":{"kind":"option","flags":[],"metavar":"N","choices":[]},"value_kind":"string","parameter_type":"integer_count","required":false,"default":"1","help":"","group":"Family Dimensions","advanced":false,"hidden_in_tui":false},
        {"id":"pointing_count","label":"Pointings","order":11,"parser":{"kind":"option","flags":[],"metavar":"N","choices":[]},"value_kind":"string","parameter_type":"integer_count","required":false,"default":"1","help":"","group":"Family Dimensions","advanced":false,"hidden_in_tui":false},
        {"id":"imaging_mode","label":"Imaging Mode","order":12,"parser":{"kind":"option","flags":[],"metavar":"MODE","choices":["single_field","mfs","mosaic","spectral_cube","cubedata","mt_mfs","simalma","aca"]},"value_kind":"choice","parameter_type":"mode","required":false,"default":"mfs","help":"","group":"Family Dimensions","advanced":false,"hidden_in_tui":false},
        {"id":"worker_policy","label":"Worker Policy","order":13,"parser":{"kind":"option","flags":[],"metavar":"auto|fixed","choices":["auto","fixed"]},"value_kind":"choice","parameter_type":"worker_policy","required":false,"default":"auto","help":"","group":"Family Workers","advanced":false,"hidden_in_tui":false},
        {"id":"row_workers","label":"Row Workers","order":14,"parser":{"kind":"option","flags":[],"metavar":"N","choices":[]},"value_kind":"string","parameter_type":"integer_count","required":false,"default":"","help":"","group":"Family Workers","advanced":false,"hidden_in_tui":false},
        {"id":"channel_workers","label":"Channel Workers","order":15,"parser":{"kind":"option","flags":[],"metavar":"N","choices":[]},"value_kind":"string","parameter_type":"integer_count","required":false,"default":"","help":"","group":"Family Workers","advanced":false,"hidden_in_tui":false},
        {"id":"measure_actual_size","label":"Measure Actual Size","order":16,"parser":{"kind":"option","flags":[],"metavar":"BOOL","choices":[]},"value_kind":"bool","parameter_type":"boolean","required":false,"default":"false","help":"","group":"Family Workers","advanced":false,"hidden_in_tui":false}
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

private func makeSimobserveFamilyGenericTaskRequest(rootURL: URL) throws -> GenericTaskRequest {
    GenericTaskRequest(
        runID: "simobserve-1",
        task: makeSimobserveTaskCatalogEntry(),
        schema: try makeSimobserveTaskUISchema(),
        values: [
            "request_kind": "family",
            "request_json": "requests/family.json",
            "source_model": #"{"kind":"analytic_components","components":[{"kind":"point","l_rad":0.0,"m_rad":0.0,"spectrum":{"flux_jy":1.0}}]}"#,
            "telescope": "VLA",
            "array_config": "synthetic-vla-d",
            "band": "Q",
            "target_ms_size_gib": "0.01",
            "output_ms": "products/family.ms",
            "polarizations": "4",
            "ms_channels": "8",
            "image_channels": "2",
            "pointing_count": "3",
            "imaging_mode": "mosaic",
            "worker_policy": "fixed",
            "row_workers": "2",
            "channel_workers": "3"
        ],
        toggles: [:],
        workingDirectoryPath: rootURL.path
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
