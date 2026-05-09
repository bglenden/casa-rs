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
    }

    func testImagerTaskSchemaExposesTutorialControlsAndManagedOutput() throws {
        let schema = try UniFFITaskUISchemaClient().loadTaskUISchema(taskID: "imager")
        let argumentIDs = Set(schema.arguments.filter { !$0.hiddenInTUI }.map(\.id))
        let tutorialArguments = [
            "ms", "imagename", "imsize", "cell_arcsec", "field", "phasecenter_field",
            "spw", "datacolumn", "specmode", "channel_count", "start", "width",
            "outframe", "restfreq", "deconvolver", "weighting", "robust",
            "perchanweightdensity", "restoringbeam", "niter", "nmajor", "gain",
            "threshold_jy", "usemask", "noisethreshold", "sidelobethreshold",
            "lownoisethreshold", "minbeamfrac", "negativethreshold",
            "deconvolver", "scales", "smallscalebias", "wterm", "wprojplanes",
            "nterms", "savemodel", "outlierfile", "pbcor", "pblimit"
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
            "--wterm", "--wprojplanes", "--nterms", "--savemodel", "--outlierfile",
            "--pbcor", "--pblimit", "--no-preview-pngs"
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

        let fixtureStore = WorkbenchStore.fixture()
        fixtureStore.openDefaultTab(kind: .aiChat)
        fixtureStore.openDefaultTab(kind: .python)
        fixtureStore.openDefaultTab(kind: .task)

        XCTAssertTrue(fixtureStore.state.tabs.contains { $0.kind == .aiChat })
        XCTAssertTrue(fixtureStore.state.tabs.contains { $0.kind == .python })
        XCTAssertTrue(fixtureStore.state.tabs.contains { $0.kind == .task })
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
        let taskClient = StubGenericTaskClient()
        let store = WorkbenchStore(probeClient: probeClient, genericTaskClient: taskClient)

        store.openProject(path: "/data")
        store.openDefaultTab(kind: .task)
        store.setGenericTaskConfirmation(taskID: "imager", confirmed: true)
        store.setDirtyImagingImageSize(256)
        store.setDirtyImagingImageHeight(256)
        store.setDirtyImagingCellArcsec(0.25)
        store.setDirtyImagingWeighting(.briggs)
        store.setDirtyImagingChannelStart("2")
        store.setDirtyImagingChannelCount("4")
        let parameters = try XCTUnwrap(store.state.dirtyImagingTaskParameters)
        let managedRequest = DirtyImagingTaskRequest(runID: "imager-1", parameters: parameters)
        taskClient.stdout = try makeManagedImagerStdout(request: managedRequest)
        store.runTask()

        XCTAssertEqual(taskClient.requests.count, 1)
        let arguments = try ProcessGenericTaskClient.arguments(for: taskClient.requests[0])
        XCTAssertTrue(arguments.contains("--managed-output"))
        XCTAssertTrue(arguments.contains("true"))
        waitFor("managed imager completion") {
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
        XCTAssertEqual(snapshot.runProductGroups.first?.products.first?.label, "Dirty Image")
        XCTAssertEqual(snapshot.runProductGroups.first?.products.first?.datasetID, producedDataset?.id)

        let productID = try XCTUnwrap(store.state.runProductGroups.first?.products.first?.id)
        store.openRunProduct(runID: runID, productID: productID)
        XCTAssertEqual(store.state.selectedDatasetID, producedDataset?.id)
        XCTAssertEqual(store.state.tabs.first { $0.id == store.state.activeTabID }?.title, "Image: output.image")
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

    func testTWHyaTutorialDirtyImagingDefaultsUseKnownMFSParameters() throws {
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
            )
        )

        store.openProject(path: "/data")
        store.openDefaultTab(kind: .task)

        let parameters = try XCTUnwrap(store.state.dirtyImagingTaskParameters)
        XCTAssertEqual(parameters.selectedField, "5: TW Hya")
        XCTAssertEqual(parameters.phaseCenterField, "5: TW Hya")
        XCTAssertEqual(parameters.selectedSpectralWindow, "spw 0: 384 chan, 372.533086 GHz center")
        XCTAssertNil(parameters.correlation)
        XCTAssertEqual(parameters.imageSize, 250)
        XCTAssertEqual(parameters.imageHeight, 250)
        XCTAssertEqual(parameters.cellArcsec, 0.1)

        let request = DirtyImagingTaskRequest(runID: "run-twhya", parameters: parameters)
        let requestJSON = try XCTUnwrap(String(data: try request.encodedImagerJSON(), encoding: .utf8))
        XCTAssertTrue(requestJSON.contains("\"field_ids\" : [\n      5\n    ]"))
        XCTAssertTrue(requestJSON.contains("\"spw_selector\" : \"0\""))
        XCTAssertTrue(requestJSON.contains("\"image_size\" : 250"))
        XCTAssertTrue(requestJSON.contains("\"cell_arcsec\" : 0.1"))
        XCTAssertFalse(requestJSON.contains("\"correlation\""))
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
        let store = WorkbenchStore(probeClient: probeClient)

        store.openProject(path: "/data")
        store.openDefaultTab(kind: .task)
        store.setDirtyImagingImageSize(0)
        store.setDirtyImagingImageHeight(128)
        store.setDirtyImagingCellArcsec(-1)
        let diagnostics = store.state.dirtyImagingTaskParameters?.validationErrors() ?? []

        XCTAssertTrue(diagnostics.contains("Image width must be positive."))
        XCTAssertTrue(diagnostics.contains("Cell size must be a positive finite arcsecond value."))
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
        let diagnostics = store.state.dirtyImagingTaskParameters?.validationErrors() ?? []

        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.imageSize, 512)
        XCTAssertEqual(store.state.dirtyImagingTaskParameters?.imageHeight, 256)
        XCTAssertTrue(diagnostics.contains("Rectangular image sizes are not supported by the current casars-imager backend yet."))
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

    func testCancellingDirtyImagingJobIsScopedToThatJob() throws {
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
        XCTAssertEqual(snapshot.taskState, .cancelled)
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
        XCTAssertEqual(
            ProcessDirtyImagingTaskClient.resolvedExecutablePath(
                environment: ["CASA_RS_REPO_ROOT": "/repo"],
                bundleExecutableURL: nil,
                isExecutable: { $0 == "/repo/target/debug/casars-imager" }
            ),
            "/repo/target/debug/casars-imager"
        )
        XCTAssertEqual(
            ProcessDirtyImagingTaskClient.resolvedExecutablePath(
                environment: [:],
                bundleExecutableURL: nil,
                currentDirectoryPath: "/repo/apps/casars-mac",
                isExecutable: { $0 == "/repo/target/debug/casars-imager" }
            ),
            "/repo/target/debug/casars-imager"
        )
        XCTAssertNil(
            ProcessDirtyImagingTaskClient.resolvedExecutablePath(
                environment: [:],
                bundleExecutableURL: bundleExecutable,
                currentDirectoryPath: "/nowhere",
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

private func makeManagedImagerStdout(request: DirtyImagingTaskRequest) throws -> String {
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
    let encoder = JSONEncoder()
    return String(decoding: try encoder.encode(result), as: UTF8.self)
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

private final class StubGenericTaskClient: GenericTaskClient {
    var requests: [GenericTaskRequest] = []
    var stdout = ""
    var stderr = ""

    func startTask(
        request: GenericTaskRequest,
        eventHandler: @escaping (GenericTaskEvent) -> Void
    ) throws -> DirtyImagingTaskExecution {
        requests.append(request)
        eventHandler(.succeeded(GenericTaskResult(
            taskID: request.task.id,
            arguments: try ProcessGenericTaskClient.arguments(for: request),
            stdout: stdout,
            stderr: stderr
        )))
        return StubDirtyImagingExecution()
    }
}

private struct StubTaskUISchemaClient: TaskUISchemaClient {
    var schema: TaskUISchema

    func loadTaskUISchema(taskID: String) throws -> TaskUISchema {
        schema
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

private final class HoldingGenericTaskClient: GenericTaskClient {
    var requests: [GenericTaskRequest] = []
    var handler: ((GenericTaskEvent) -> Void)?
    let execution = StubDirtyImagingExecution()

    func startTask(
        request: GenericTaskRequest,
        eventHandler: @escaping (GenericTaskEvent) -> Void
    ) throws -> DirtyImagingTaskExecution {
        requests.append(request)
        handler = eventHandler
        return execution
    }

    func emitSucceeded() throws {
        guard let request = requests.last else { return }
        handler?(.succeeded(GenericTaskResult(
            taskID: request.task.id,
            arguments: try ProcessGenericTaskClient.arguments(for: request),
            stdout: "",
            stderr: ""
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
