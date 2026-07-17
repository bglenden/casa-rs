import CasarsMacCore
import AppKit
import Darwin
import Foundation
import SwiftUI

@main
struct CasarsMacApp: App {
    private static let interfaceFontSizeKey = "interfaceFontSize"

    @NSApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate
    @AppStorage(Self.interfaceFontSizeKey) private var interfaceFontSize = WorkbenchState.defaultInterfaceFontSize
    @StateObject private var store: WorkbenchStore
    @State private var didOpenStartupProject = false
    private let startupProjectPath: String?
    private let startupImagerMeasurementSetPath: String?
    private let startupTutorialPackPath: String?
    private let startupTutorialSectionID: String?
    private let startupOpenSelectedDatasetExplorer: Bool
    private let startupImageRegionBoxes: [(Int, Int, Int, Int)]
    private let startupImageRegionExportPath: String?
    private let startupNotebookPrototypeScenario: NotebookPrototypeScenario?
    private let startupPythonPrototypeScenario: PythonPrototypeScenario?
    private let startupTutorialPrototypeScenario: TutorialNotebookPrototypeScenario?
    private let startupAIPrototypeScenario: AIChatPrototypeScenario?
    private let startupShowImagerProgressMockup: Bool
    private let startupOpenImagerTask: Bool
    private let startupRunActiveTask: Bool

    init() {
        let arguments = CommandLine.arguments
        if let error = Self.prototypeLaunchValidationError(arguments: arguments) {
            fputs("invalid prototype launch: \(error)\n", stderr)
            exit(2)
        }
        let initialNotebookPrototypeScenario = Self.notebookPrototypeScenario(arguments: arguments)
            ?? (Self.argumentValue(after: "--capture-kind", in: arguments) == "notebook-prototype"
                ? (Self.argumentValue(after: "--prototype-state", in: arguments) == "external-conflict"
                    ? .externalConflict
                    : .primary)
                : nil)
        let initialPythonPrototypeScenario = Self.pythonPrototypeScenario(arguments: arguments)
            ?? (Self.argumentValue(after: "--capture-kind", in: arguments) == "python-prototype"
                ? Self.pythonPrototypeScenarioValue(
                    Self.argumentValue(after: "--prototype-state", in: arguments)
                )
                : nil)
        let initialTutorialPrototypeScenario = Self.tutorialPrototypeScenario(arguments: arguments)
            ?? (Self.argumentValue(after: "--capture-kind", in: arguments) == "tutorial-prototype"
                ? Self.tutorialPrototypeScenarioValue(
                    Self.argumentValue(after: "--prototype-state", in: arguments)
                )
                : nil)
        let initialAIPrototypeScenario = Self.aiPrototypeScenario(arguments: arguments)
            ?? (Self.argumentValue(after: "--capture-kind", in: arguments) == "ai-prototype"
                ? Self.aiPrototypeScenarioValue(Self.argumentValue(after: "--prototype-state", in: arguments))
                : nil)
        let initialStore = initialNotebookPrototypeScenario
            .map { WorkbenchStore.notebookPrototype(scenario: $0) }
            ?? initialPythonPrototypeScenario.map { WorkbenchStore.pythonPrototype(scenario: $0) }
            ?? initialTutorialPrototypeScenario.map { WorkbenchStore.tutorialPrototype(scenario: $0) }
            ?? initialAIPrototypeScenario.map { WorkbenchStore.aiPrototype(scenario: $0) }
            ?? WorkbenchStore.empty()
        _store = StateObject(
            wrappedValue: initialStore
        )
        if arguments.contains("--capture-gui-evidence") {
            Self.captureGUIEvidence(arguments: arguments)
            exit(0)
        }
        if arguments.contains("--dump-imager-progress-samples") {
            Self.dumpImagerProgressSamples(arguments: arguments)
            exit(0)
        }
        if arguments.contains("--dump-debug-state") {
            Self.dumpDebugState(
                simulateMainFlow: arguments.contains("--simulate-main-flow"),
                tutorialPackPath: Self.argumentValue(after: "--open-tutorial-pack", in: arguments),
                tutorialSectionID: Self.argumentValue(after: "--open-tutorial-section", in: arguments),
                taskValueOverrides: Self.argumentPairs(after: "--set-task-value", in: arguments),
                taskToggleOverrides: Self.argumentPairs(after: "--set-task-toggle", in: arguments),
                runActiveTask: arguments.contains("--run-active-task"),
                openSelectedDatasetExplorer: arguments.contains("--open-selected-dataset-explorer"),
                imageRegionBoxes: Self.regionBoxes(after: "--image-region-box", in: arguments),
                imageRegionExportPath: Self.argumentValue(after: "--export-image-region-file", in: arguments),
                notebookPrototypeScenario: Self.notebookPrototypeScenario(arguments: arguments),
                pythonPrototypeScenario: Self.pythonPrototypeScenario(arguments: arguments),
                tutorialPrototypeScenario: Self.tutorialPrototypeScenario(arguments: arguments),
                aiPrototypeScenario: Self.aiPrototypeScenario(arguments: arguments),
                showImagerProgressMockup: arguments.contains("--show-imager-progress-mockup"),
                imagerMeasurementSetPath: Self.argumentValue(after: "--open-imager-ms", in: arguments),
                projectPath: Self.argumentValue(after: "--probe-project", in: arguments)
                    ?? Self.argumentValue(after: "--open-project", in: arguments)
            )
            exit(0)
        }

        startupProjectPath = Self.argumentValue(after: "--open-project", in: arguments)
            ?? Self.argumentValue(after: "--probe-project", in: arguments)
        startupImagerMeasurementSetPath = Self.argumentValue(after: "--open-imager-ms", in: arguments)
        startupTutorialPackPath = Self.argumentValue(after: "--open-tutorial-pack", in: arguments)
        startupTutorialSectionID = Self.argumentValue(after: "--open-tutorial-section", in: arguments)
        startupOpenSelectedDatasetExplorer = arguments.contains("--open-selected-dataset-explorer")
        startupImageRegionBoxes = Self.regionBoxes(after: "--image-region-box", in: arguments)
        startupImageRegionExportPath = Self.argumentValue(after: "--export-image-region-file", in: arguments)
        startupNotebookPrototypeScenario = Self.notebookPrototypeScenario(arguments: arguments)
        startupPythonPrototypeScenario = Self.pythonPrototypeScenario(arguments: arguments)
        startupTutorialPrototypeScenario = Self.tutorialPrototypeScenario(arguments: arguments)
        startupAIPrototypeScenario = Self.aiPrototypeScenario(arguments: arguments)
        startupShowImagerProgressMockup = arguments.contains("--show-imager-progress-mockup")
        startupOpenImagerTask = arguments.contains("--open-imager-task")
        startupRunActiveTask = arguments.contains("--run-active-task")
        WorkbenchFallbackWindowController.shared.scheduleStartupWindow(arguments: arguments)
    }

    var body: some Scene {
        WindowGroup("casa-rs Workbench") {
            WorkbenchView(store: store)
                .frame(minWidth: 960, minHeight: 640)
                .environment(\.workbenchFontSize, store.state.interfaceFontSize)
                .background(WindowConfigurationView())
                .onAppear {
                    syncStoreFontSizeFromSettings()
                    openStartupProjectIfNeeded()
                }
                .onChange(of: interfaceFontSize) { newValue in
                    store.setInterfaceFontSize(newValue)
                }
                .onChange(of: store.state.interfaceFontSize) { newValue in
                    interfaceFontSize = newValue
                }
        }
        .commands {
            CommandMenu("Workbench") {
                Button("Open Project Directory...") {
                    if let url = ProjectOpenPanel.chooseDirectory() {
                        store.openProject(path: url.path)
                    }
                }
                .keyboardShortcut("o", modifiers: [.command])
                .disabled(store.isPrototypeRuntime)

                Button("Fork Tutorial Template...") {
                    if let url = TutorialTemplateOpenPanel.chooseTemplate() {
                        store.openTutorialTemplate(path: url.path)
                    }
                }
                .keyboardShortcut("t", modifiers: [.command, .shift])
                .disabled(store.isPrototypeRuntime || !store.state.hasProject)

                Button("Open Demo Project") {
                    store.openFixtureProject()
                }
                .keyboardShortcut("o", modifiers: [.command, .shift])
                .disabled(store.isPrototypeRuntime)

                Button("Open AI Chat") {
                    store.openDefaultTab(kind: .aiChat)
                }
                .keyboardShortcut("l", modifiers: [.command, .shift])
                .disabled(store.isPrototypeRuntime)

                Button("Close Active Tab") {
                    store.closeActiveTab()
                }
                .keyboardShortcut("w", modifiers: [.command])
                .disabled(store.state.tabs.isEmpty)

                Button("Toggle Inspector") {
                    store.toggleInspector()
                }
                .keyboardShortcut("i", modifiers: [.command, .option])

                Button(store.state.leftDockCollapsed ? "Show Left Dock" : "Hide Left Dock") {
                    store.toggleLeftDock()
                }
                .keyboardShortcut("s", modifiers: [.command, .option])

                Button(store.state.inspectorCollapsed ? "Show Inspector" : "Hide Inspector") {
                    store.toggleInspector()
                }
                .keyboardShortcut("i", modifiers: [.command, .shift])

                Button("Run Command Query") {
                    store.runCommandQuery()
                }
                .keyboardShortcut("k", modifiers: [.command])
            }

            CommandMenu("Window Layout") {
                Button("Toggle Full Screen") {
                    FullScreenController.toggleFullScreen()
                }
                .keyboardShortcut("f", modifiers: [.command, .control])
            }

            CommandMenu("Display") {
                Button("Increase Font Size") {
                    setStoredInterfaceFontSize(interfaceFontSize + 1)
                }
                .keyboardShortcut("+", modifiers: [.command])
                .disabled(interfaceFontSize >= WorkbenchState.maximumInterfaceFontSize)

                Button("Decrease Font Size") {
                    setStoredInterfaceFontSize(interfaceFontSize - 1)
                }
                .keyboardShortcut("-", modifiers: [.command])
                .disabled(interfaceFontSize <= WorkbenchState.minimumInterfaceFontSize)

                Button("Reset Font Size") {
                    setStoredInterfaceFontSize(WorkbenchState.defaultInterfaceFontSize)
                }
                .keyboardShortcut("0", modifiers: [.command])
            }
        }
        Settings {
            DisplaySettingsView(interfaceFontSize: $interfaceFontSize)
        }
    }

    private struct ImagerProgressSample: Codable {
        var sampleIndex: Int
        var elapsedSeconds: Double
        var taskState: TaskRunState
        var taskProgress: Double
        var runningJobCount: Int
        var runID: String?
        var progress: ImagerProgressWidgetSample?
        var diagnostics: [String]
        var outputPaths: [String]
    }

    private struct ImagerProgressWidgetSample: Codable {
        var source: String
        var state: TaskRunState
        var phase: String
        var summary: String
        var sampledAtLabel: String
        var work: WorkSample
        var measurementSetWindow: MeasurementSetWindowSample
        var outputCube: OutputCubeSample
        var uvCoverage: UVCoverageSample
        var deconvolution: DeconvolutionSample
        var runtime: RuntimeSample
        var resourceActivities: [ResourceActivitySample]

        init(_ snapshot: ImagerProgressSnapshot) {
            source = snapshot.source
            state = snapshot.state
            phase = snapshot.phase
            summary = snapshot.summary
            sampledAtLabel = snapshot.sampledAtLabel
            work = WorkSample(snapshot.workEstimate)
            measurementSetWindow = MeasurementSetWindowSample(snapshot.measurementSetWindow)
            outputCube = OutputCubeSample(snapshot.outputCube)
            uvCoverage = UVCoverageSample(snapshot.uvCoverage)
            deconvolution = DeconvolutionSample(snapshot.deconvolution)
            runtime = RuntimeSample(snapshot.runtime)
            resourceActivities = snapshot.resourceActivities.map(ResourceActivitySample.init)
        }
    }

    private struct WorkSample: Codable {
        var completedUnits: UInt64
        var totalUnits: UInt64
        var fraction: Double
        var unitLabel: String
        var confidence: String

        init(_ work: ImagingWorkEstimate) {
            completedUnits = work.completedUnits
            totalUnits = work.totalUnits
            fraction = work.fraction
            unitLabel = work.unitLabel
            confidence = work.confidence
        }
    }

    private struct MeasurementSetWindowSample: Codable {
        var rowStart: Int
        var rowEnd: Int
        var totalRows: Int
        var channelStart: Int
        var channelEnd: Int
        var totalChannels: Int

        init(_ window: MeasurementSetReadWindowProgress) {
            rowStart = window.activeRowStart
            rowEnd = window.activeRowEnd
            totalRows = window.totalRows
            channelStart = window.activeChannelStart
            channelEnd = window.activeChannelEnd
            totalChannels = window.totalChannels
        }
    }

    private struct OutputCubeSample: Codable {
        var xPixels: Int
        var yPixels: Int
        var zPlanes: Int
        var activePlaneStart: Int
        var activePlaneEnd: Int

        init(_ cube: OutputCubeProgress) {
            xPixels = cube.xPixels
            yPixels = cube.yPixels
            zPlanes = cube.zPlanes
            activePlaneStart = cube.activePlaneStart
            activePlaneEnd = cube.activePlaneEnd
        }
    }

    private struct UVCoverageSample: Codable {
        var uExtentKilolambda: Double
        var vExtentKilolambda: Double
        var measuredPointCount: Int
        var conjugatePointCount: Int
        var accumulatedPointCount: Int
        var droppedPointCount: UInt64
        var sampleLimit: Int

        init(_ coverage: UVCoverageProgress) {
            uExtentKilolambda = coverage.uExtentKilolambda
            vExtentKilolambda = coverage.vExtentKilolambda
            measuredPointCount = coverage.measured.count
            conjugatePointCount = coverage.conjugate.count
            accumulatedPointCount = coverage.accumulatedPointCount
            droppedPointCount = coverage.droppedPointCount
            sampleLimit = coverage.sampleLimit
        }
    }

    private struct DeconvolutionSample: Codable {
        var phase: String
        var majorCycle: Int
        var majorCycleLimit: Int
        var minorIterations: Int
        var minorIterationLimit: Int
        var componentsCleaned: Int
        var peakResidualMilliJyPerBeam: Double
        var targetResidualMilliJyPerBeam: Double
        var residualHistoryCount: Int

        init(_ deconvolution: ImagingDeconvolutionProgress) {
            phase = deconvolution.phase
            majorCycle = deconvolution.majorCycle
            majorCycleLimit = deconvolution.majorCycleLimit
            minorIterations = deconvolution.minorIterations
            minorIterationLimit = deconvolution.minorIterationLimit
            componentsCleaned = deconvolution.componentsCleaned
            peakResidualMilliJyPerBeam = deconvolution.peakResidualMilliJyPerBeam
            targetResidualMilliJyPerBeam = deconvolution.targetResidualMilliJyPerBeam
            residualHistoryCount = deconvolution.residualHistoryMilliJyPerBeam.count
        }
    }

    private struct RuntimeSample: Codable {
        var activeThreads: Int
        var totalThreads: Int
        var gpuActive: Bool
        var backend: String

        init(_ runtime: ImagingRuntimeProgress) {
            activeThreads = runtime.activeThreads
            totalThreads = runtime.totalThreads
            gpuActive = runtime.gpuActive
            backend = runtime.backend
        }
    }

    private struct ResourceActivitySample: Codable {
        var id: String
        var name: String
        var detail: String
        var kind: ImagingResourceActivityKind
        var state: ImagingResourceActivityState
        var observedState: String
        var residentBytes: Int
        var targetBytes: Int
        var sectionStartFraction: Double
        var sectionEndFraction: Double
        var activeThreads: Int
        var totalThreads: Int
        var gpuActive: Bool

        init(_ resource: ImagingResourceActivity) {
            id = resource.id
            name = resource.name
            detail = resource.detail
            kind = resource.kind
            state = resource.state
            observedState = resource.observedState
            residentBytes = resource.residentBytes
            targetBytes = resource.targetBytes
            sectionStartFraction = resource.sectionStartFraction
            sectionEndFraction = resource.sectionEndFraction
            activeThreads = resource.activeThreads
            totalThreads = resource.totalThreads
            gpuActive = resource.gpuActive
        }
    }

    private static func dumpImagerProgressSamples(arguments: [String]) {
        guard let imagerMeasurementSetPath = argumentValue(after: "--open-imager-ms", in: arguments) else {
            fputs("--dump-imager-progress-samples requires --open-imager-ms PATH\n", stderr)
            exit(2)
        }
        let sampleCount = max(1, Int(argumentValue(after: "--sample-count", in: arguments) ?? "8") ?? 8)
        let sampleInterval = max(0.1, Double(argumentValue(after: "--sample-interval", in: arguments) ?? "5") ?? 5)
        let timeoutSeconds = max(
            sampleInterval,
            Double(argumentValue(after: "--sample-timeout", in: arguments) ?? "180") ?? 180
        )

        let store = WorkbenchStore.empty()
        store.setInterfaceFontSize(storedInterfaceFontSize())
        store.openExternalMeasurementSetForImaging(path: imagerMeasurementSetPath)
        applyImagerSchemaOverrides(arguments: arguments, store: store)

        store.setGenericTaskConfirmation(taskID: store.state.activeTaskID, confirmed: true)
        store.runTask()

        let start = Date()
        let deadline = start.addingTimeInterval(timeoutSeconds)
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]

        func emitSample(_ sampleIndex: Int) {
            let sample = ImagerProgressSample(
                sampleIndex: sampleIndex,
                elapsedSeconds: Date().timeIntervalSince(start),
                taskState: store.state.taskRun.state,
                taskProgress: store.state.taskRun.progress,
                runningJobCount: store.state.jobs.values.filter { $0.status == .running || $0.status == .pending }.count,
                runID: store.state.taskRun.runID,
                progress: store.state.taskRun.imagerProgress.map(ImagerProgressWidgetSample.init),
                diagnostics: store.state.taskRun.diagnostics,
                outputPaths: store.state.taskRun.outputPaths
            )
            do {
                let data = try encoder.encode(sample)
                print(String(decoding: data, as: UTF8.self))
                fflush(stdout)
            } catch {
                fputs("failed to encode imager progress sample: \(error)\n", stderr)
                exit(1)
            }
        }

        emitSample(0)
        var emittedSamples = 1
        while emittedSamples < sampleCount && Date() < deadline {
            let nextSample = Date().addingTimeInterval(sampleInterval)
            while Date() < nextSample && Date() < deadline {
                RunLoop.current.run(mode: .default, before: min(nextSample, Date().addingTimeInterval(0.05)))
                if store.state.taskRun.state != .running {
                    break
                }
            }
            emitSample(emittedSamples)
            emittedSamples += 1
            if store.state.taskRun.state != .running {
                break
            }
        }

        while store.state.taskRun.state == .running && Date() < deadline {
            RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.05))
        }
        if store.state.taskRun.state == .running {
            store.stopTask()
            RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.2))
        }
        if emittedSamples == 0 || store.state.taskRun.state != .running {
            emitSample(emittedSamples)
        }
        if store.state.taskRun.state == .failed {
            exit(1)
        }
    }

    private static func dumpDebugState(
        simulateMainFlow: Bool,
        tutorialPackPath: String?,
        tutorialSectionID: String?,
        taskValueOverrides: [(String, String)],
        taskToggleOverrides: [(String, String)],
        runActiveTask: Bool,
        openSelectedDatasetExplorer: Bool,
        imageRegionBoxes: [(Int, Int, Int, Int)],
        imageRegionExportPath: String?,
        notebookPrototypeScenario: NotebookPrototypeScenario?,
        pythonPrototypeScenario: PythonPrototypeScenario?,
        tutorialPrototypeScenario: TutorialNotebookPrototypeScenario?,
        aiPrototypeScenario: AIChatPrototypeScenario?,
        showImagerProgressMockup: Bool,
        imagerMeasurementSetPath: String?,
        projectPath: String?
    ) {
        let store = notebookPrototypeScenario.map { WorkbenchStore.notebookPrototype(scenario: $0) }
            ?? pythonPrototypeScenario.map { WorkbenchStore.pythonPrototype(scenario: $0) }
            ?? tutorialPrototypeScenario.map { WorkbenchStore.tutorialPrototype(scenario: $0) }
            ?? aiPrototypeScenario.map { WorkbenchStore.aiPrototype(scenario: $0) }
            ?? WorkbenchStore.empty()
        store.setInterfaceFontSize(storedInterfaceFontSize())
        if notebookPrototypeScenario == nil, pythonPrototypeScenario == nil,
           tutorialPrototypeScenario == nil, aiPrototypeScenario == nil, let tutorialPackPath {
            if let projectPath { store.openProject(path: projectPath) }
            store.openTutorialTemplate(path: tutorialPackPath)
            if let tutorialSectionID {
                store.openTutorialSectionTask(tutorialSectionID)
            }
        } else if let imagerMeasurementSetPath {
            store.openExternalMeasurementSetForImaging(path: imagerMeasurementSetPath)
            applyImagerSchemaOverrides(arguments: CommandLine.arguments, store: store)
        } else if let projectPath {
            store.openProject(path: projectPath)
        }
        for (argumentID, value) in taskValueOverrides {
            store.setGenericTaskValue(argumentID: argumentID, value: value)
        }
        for (argumentID, value) in taskToggleOverrides {
            store.setGenericTaskToggle(argumentID: argumentID, value: value == "true")
        }
        if runActiveTask {
            store.setGenericTaskConfirmation(taskID: store.state.activeTaskID, confirmed: true)
            store.runTask()
            Self.waitForTaskToFinish(store: store, timeoutSeconds: 120)
        }
        if openSelectedDatasetExplorer || !imageRegionBoxes.isEmpty {
            store.openDefaultTab(kind: .datasetExplorer)
            Self.applyImageRegionBoxes(imageRegionBoxes, store: store)
            Self.exportImageRegionIfNeeded(imageRegionExportPath, store: store)
        }
        if showImagerProgressMockup {
            store.openImagerProgressMockup()
        }
        if simulateMainFlow {
            if tutorialPackPath == nil && projectPath == nil {
                store.openFixtureProject()
            }
            if store.state.isDemoProject {
                store.selectDockMode(.history)
                store.setInspectorCollapsed(true)
                store.applyAIProposal("proposal-spw")
                store.setPythonOwner(.ai)
                store.runTask()
                store.openDefaultTab(kind: .plotSamples)
                store.openDefaultTab(kind: .history)
            } else if let dataset = store.state.selectedDataset, dataset.kind == .measurementSet {
                store.runMeasurementSetPlot(datasetID: dataset.id)
                store.openDefaultTab(kind: .task)
                store.runTask()
                Self.waitForTaskToFinish(store: store, timeoutSeconds: 120)
            }
        }

        do {
            let json = try store.debugJSON()
            print(json)
        } catch {
            fputs("failed to encode debug state: \(error)\n", stderr)
            exit(1)
        }
    }

    private static func applyImageRegionBoxes(
        _ boxes: [(Int, Int, Int, Int)],
        store: WorkbenchStore
    ) {
        guard let datasetID = store.state.selectedDatasetID else { return }
        for (x0, y0, x1, y1) in boxes {
            store.appendImageExplorerRegionCommand(.startRegionShape, datasetID: datasetID)
            store.appendImageExplorerRegionCommand(.appendRegionVertex(x: x0, y: y0), datasetID: datasetID)
            store.appendImageExplorerRegionCommand(.appendRegionVertex(x: x1, y: y0), datasetID: datasetID)
            store.appendImageExplorerRegionCommand(.appendRegionVertex(x: x1, y: y1), datasetID: datasetID)
            store.appendImageExplorerRegionCommand(.appendRegionVertex(x: x0, y: y1), datasetID: datasetID)
            store.appendImageExplorerRegionCommand(.closeRegionShape, datasetID: datasetID)
        }
    }

    private static func exportImageRegionIfNeeded(_ path: String?, store: WorkbenchStore) {
        guard let path else { return }
        guard let datasetID = store.state.selectedDatasetID else { return }
        store.exportImageExplorerRegionFile(datasetID: datasetID, path: path)
    }

    private static func argumentValue(after flag: String, in arguments: [String]) -> String? {
        guard let index = arguments.firstIndex(of: flag), arguments.indices.contains(index + 1) else {
            return nil
        }
        return arguments[index + 1]
    }

    static func prototypeLaunchValidationError(arguments: [String]) -> String? {
        let showKind = argumentValue(after: "--show-prototype", in: arguments)
        if arguments.contains("--show-prototype") {
            guard let showKind, ["notebook", "python", "tutorial", "ai"].contains(showKind) else {
                return "--show-prototype requires: notebook, python, tutorial, or ai"
            }
        }

        let captureKind = argumentValue(after: "--capture-kind", in: arguments)
        let capturePrototypeKind: String? = switch captureKind {
        case "notebook-prototype": "notebook"
        case "python-prototype": "python"
        case "tutorial-prototype": "tutorial"
        case "ai-prototype": "ai"
        default: nil
        }
        if let showKind, let capturePrototypeKind, showKind != capturePrototypeKind {
            return "--show-prototype \(showKind) cannot be combined with --capture-kind \(captureKind ?? "")"
        }

        let prototypeKind = showKind ?? capturePrototypeKind
        if arguments.contains("--prototype-state"), prototypeKind == nil {
            return "--prototype-state requires a notebook, python, tutorial, or ai prototype launch"
        }
        guard let prototypeKind else { return nil }

        if arguments.contains("--prototype-state") {
            let state = argumentValue(after: "--prototype-state", in: arguments)
            let accepted: [String] = switch prototypeKind {
            case "notebook": ["happy-path", "external-conflict"]
            case "python": ["happy-path", "failure", "nonresponsive"]
            case "tutorial": ["happy-path", "checksum-failure", "disk-failure", "offline", "unsafe-archive"]
            case "ai": ["happy-path", "provider-error", "rate-limited", "offline", "tool-failure", "nonresponsive"]
            default: []
            }
            if !accepted.contains(state ?? "") {
                return "--prototype-state for \(prototypeKind) requires: \(accepted.joined(separator: ", "))"
            }
        }

        let prototypeLaunchLabel = showKind.map { "--show-prototype \($0)" }
            ?? "--capture-kind \(captureKind ?? "")"

        let incompatibleFlags = [
            "--run-active-task",
            "--open-selected-dataset-explorer",
            "--open-imager-task",
            "--open-imager-ms",
            "--open-project",
            "--probe-project",
            "--open-tutorial-pack",
            "--open-tutorial-section",
            "--image-region-box",
            "--export-image-region-file",
            "--set-task-value",
            "--set-task-toggle",
            "--simulate-main-flow",
            "--show-imager-progress-mockup",
            "--dump-imager-progress-samples",
            "--imagename",
            "--output-prefix",
            "--image-size",
            "--imsize",
            "--image-width",
            "--image-height",
            "--cell-arcsec",
            "--spectral-mode",
            "--specmode",
            "--channel-start",
            "--channel-count",
            "--niter",
            "--threshold-jy",
            "--dirty-only",
            "--deconvolver",
            "--weighting",
            "--gridder",
            "--standard-mfs-acceleration",
            "--interpolation",
            "--cube-interp",
            "--perchanweightdensity",
            "--per-channel-density",
            "--no-perchanweightdensity",
            "--write-pb",
            "--pbcor",
            "--no-preview-pngs"
        ]
        if let flag = incompatibleFlags.first(where: arguments.contains) {
            return "\(prototypeLaunchLabel) cannot be combined with \(flag)"
        }
        return nil
    }

    fileprivate static func notebookPrototypeScenario(arguments: [String]) -> NotebookPrototypeScenario? {
        guard argumentValue(after: "--show-prototype", in: arguments) == "notebook" else {
            return nil
        }
        switch argumentValue(after: "--prototype-state", in: arguments) {
        case "external-conflict":
            return .externalConflict
        case nil, "happy-path":
            return .primary
        default:
            return nil
        }
    }

    fileprivate static func pythonPrototypeScenario(arguments: [String]) -> PythonPrototypeScenario? {
        guard argumentValue(after: "--show-prototype", in: arguments) == "python" else {
            return nil
        }
        return pythonPrototypeScenarioValue(argumentValue(after: "--prototype-state", in: arguments))
    }

    fileprivate static func pythonPrototypeScenarioValue(_ value: String?) -> PythonPrototypeScenario? {
        switch value {
        case "failure": .failure
        case "nonresponsive": .nonresponsive
        case nil, "happy-path": .primary
        default: nil
        }
    }

    fileprivate static func tutorialPrototypeScenario(arguments: [String]) -> TutorialNotebookPrototypeScenario? {
        guard argumentValue(after: "--show-prototype", in: arguments) == "tutorial" else {
            return nil
        }
        return tutorialPrototypeScenarioValue(argumentValue(after: "--prototype-state", in: arguments))
    }

    fileprivate static func tutorialPrototypeScenarioValue(
        _ value: String?
    ) -> TutorialNotebookPrototypeScenario? {
        switch value {
        case nil, "happy-path": .happyPath
        case "checksum-failure": .checksumFailure
        case "disk-failure": .diskFailure
        case "offline": .offline
        case "unsafe-archive": .unsafeArchive
        default: nil
        }
    }

    fileprivate static func aiPrototypeScenario(arguments: [String]) -> AIChatPrototypeScenario? {
        guard argumentValue(after: "--show-prototype", in: arguments) == "ai" else { return nil }
        return aiPrototypeScenarioValue(argumentValue(after: "--prototype-state", in: arguments))
    }

    fileprivate static func aiPrototypeScenarioValue(_ value: String?) -> AIChatPrototypeScenario? {
        switch value {
        case nil, "happy-path": .primary
        case "provider-error": .providerError
        case "rate-limited": .rateLimited
        case "offline": .offline
        case "tool-failure": .toolFailure
        case "nonresponsive": .nonresponsive
        default: nil
        }
    }

    fileprivate static func applyImagerSchemaOverrides(arguments: [String], store: WorkbenchStore) {
        if let outputPrefix = argumentValue(after: "--imagename", in: arguments)
            ?? argumentValue(after: "--output-prefix", in: arguments) {
            store.setGenericTaskValue(taskID: "imager", argumentID: "imagename", value: outputPrefix)
        }
        if let imageSize = (
            argumentValue(after: "--image-size", in: arguments)
                ?? argumentValue(after: "--imsize", in: arguments)
        ).flatMap(Int.init) {
            store.setGenericTaskValue(taskID: "imager", argumentID: "imsize", value: "\(imageSize)")
        }
        if let imageWidth = argumentValue(after: "--image-width", in: arguments).flatMap(Int.init) {
            store.setGenericTaskValue(taskID: "imager", argumentID: "imsize", value: "\(imageWidth)")
        }
        if let cellArcsec = argumentValue(after: "--cell-arcsec", in: arguments).flatMap(Double.init) {
            store.setGenericTaskValue(taskID: "imager", argumentID: "cell", value: "\(cellArcsec)arcsec")
        }
        if let spectralMode = (
            argumentValue(after: "--spectral-mode", in: arguments)
                ?? argumentValue(after: "--specmode", in: arguments)
        ) {
            store.setGenericTaskValue(taskID: "imager", argumentID: "specmode", value: spectralMode)
        }
        if let channelStart = argumentValue(after: "--channel-start", in: arguments) {
            store.setGenericTaskValue(taskID: "imager", argumentID: "channel_start", value: channelStart)
        }
        if let channelCount = argumentValue(after: "--channel-count", in: arguments) {
            store.setGenericTaskValue(taskID: "imager", argumentID: "channel_count", value: channelCount)
        }
        if let chanchunks = argumentValue(after: "--chanchunks", in: arguments) {
            store.setGenericTaskValue(taskID: "imager", argumentID: "chanchunks", value: chanchunks)
        }
        if let parallel = argumentBool(after: "--parallel", defaultIfPresent: true, in: arguments) {
            store.setGenericTaskToggle(taskID: "imager", argumentID: "parallel", value: parallel)
        } else if argumentBool(after: "--no-parallel", defaultIfPresent: false, in: arguments) != nil {
            store.setGenericTaskToggle(taskID: "imager", argumentID: "parallel", value: false)
        }
        if let niter = argumentValue(after: "--niter", in: arguments).flatMap(Int.init) {
            store.setGenericTaskValue(taskID: "imager", argumentID: "niter", value: "\(niter)")
        }
        if let thresholdJy = argumentValue(after: "--threshold-jy", in: arguments).flatMap(Double.init) {
            store.setGenericTaskValue(taskID: "imager", argumentID: "threshold", value: "\(thresholdJy)Jy")
        }
        if let dirtyOnly = argumentBool(after: "--dirty-only", defaultIfPresent: true, in: arguments) {
            store.setGenericTaskToggle(taskID: "imager", argumentID: "dirty_only", value: dirtyOnly)
        }
        if let deconvolver = argumentValue(after: "--deconvolver", in: arguments) {
            store.setGenericTaskValue(taskID: "imager", argumentID: "deconvolver", value: deconvolver)
        }
        if let weighting = argumentValue(after: "--weighting", in: arguments) {
            store.setGenericTaskValue(taskID: "imager", argumentID: "weighting", value: weighting)
        }
        if let gridder = argumentValue(after: "--gridder", in: arguments) {
            store.setGenericTaskValue(taskID: "imager", argumentID: "gridder", value: gridder)
        }
        if let standardMFSAcceleration = argumentValue(after: "--standard-mfs-acceleration", in: arguments) {
            store.setGenericTaskValue(
                taskID: "imager",
                argumentID: "standard_mfs_acceleration",
                value: standardMFSAcceleration
            )
        }
        if let readAheadBlocks = argumentValue(after: "--imaging-read-ahead-blocks", in: arguments) {
            store.setGenericTaskValue(
                taskID: "imager",
                argumentID: "imaging_read_ahead_blocks",
                value: readAheadBlocks
            )
        }
        if let fftBackend = argumentValue(after: "--imaging-fft-backend", in: arguments) {
            store.setGenericTaskValue(
                taskID: "imager",
                argumentID: "imaging_fft_backend",
                value: fftBackend
            )
        }
        if let interpolation = argumentValue(after: "--interpolation", in: arguments)
            ?? argumentValue(after: "--cube-interp", in: arguments) {
            store.setGenericTaskValue(taskID: "imager", argumentID: "interpolation", value: interpolation)
        }
        if let perChannelDensity = argumentBool(after: "--perchanweightdensity", defaultIfPresent: true, in: arguments)
            ?? argumentBool(after: "--per-channel-density", defaultIfPresent: true, in: arguments) {
            store.setGenericTaskToggle(taskID: "imager", argumentID: "perchanweightdensity", value: perChannelDensity)
        } else if argumentBool(after: "--no-perchanweightdensity", defaultIfPresent: false, in: arguments) != nil {
            store.setGenericTaskToggle(taskID: "imager", argumentID: "perchanweightdensity", value: false)
        }
        if let writePB = argumentBool(after: "--write-pb", defaultIfPresent: true, in: arguments) {
            store.setGenericTaskToggle(taskID: "imager", argumentID: "write_pb", value: writePB)
        }
        if let pbcor = argumentBool(after: "--pbcor", defaultIfPresent: true, in: arguments) {
            store.setGenericTaskToggle(taskID: "imager", argumentID: "pbcor", value: pbcor)
        }
        if arguments.contains("--no-preview-pngs") {
            store.setGenericTaskToggle(taskID: "imager", argumentID: "write_preview_pngs", value: false)
        }
        for (argumentID, value) in argumentPairs(after: "--set-task-value", in: arguments) {
            if store.state.taskUISchemas["imager"]?.arguments.first(where: { $0.id == argumentID })?.parser.kind == "toggle" {
                store.setGenericTaskToggle(taskID: "imager", argumentID: argumentID, value: commandLineBool(value))
            } else {
                store.setGenericTaskValue(taskID: "imager", argumentID: argumentID, value: value)
            }
        }
        for (argumentID, value) in argumentPairs(after: "--set-task-toggle", in: arguments) {
            store.setGenericTaskToggle(taskID: "imager", argumentID: argumentID, value: commandLineBool(value))
        }
    }

    private static func commandLineBool(_ value: String) -> Bool {
        commandLineBoolLiteral(value) ?? false
    }

    private static func argumentBool(after flag: String, defaultIfPresent: Bool, in arguments: [String]) -> Bool? {
        guard let index = arguments.firstIndex(of: flag) else {
            return nil
        }
        let valueIndex = arguments.index(after: index)
        if valueIndex < arguments.endIndex,
           let literal = commandLineBoolLiteral(arguments[valueIndex]) {
            return literal
        }
        return defaultIfPresent
    }

    private static func commandLineBoolLiteral(_ value: String) -> Bool? {
        switch value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
        case "1", "true", "yes", "on":
            return true
        case "0", "false", "no", "off":
            return false
        default:
            return nil
        }
    }

    private static func argumentPairs(after flag: String, in arguments: [String]) -> [(String, String)] {
        var pairs: [(String, String)] = []
        var index = arguments.startIndex
        while index < arguments.endIndex {
            guard arguments[index] == flag else {
                index = arguments.index(after: index)
                continue
            }
            let keyIndex = arguments.index(after: index)
            let valueIndex = arguments.index(keyIndex, offsetBy: 1)
            guard keyIndex < arguments.endIndex, valueIndex < arguments.endIndex else {
                break
            }
            pairs.append((arguments[keyIndex], arguments[valueIndex]))
            index = arguments.index(after: valueIndex)
        }
        return pairs
    }

    private static func regionBoxes(after flag: String, in arguments: [String]) -> [(Int, Int, Int, Int)] {
        var boxes: [(Int, Int, Int, Int)] = []
        var index = arguments.startIndex
        while index < arguments.endIndex {
            guard arguments[index] == flag else {
                index = arguments.index(after: index)
                continue
            }
            let valueIndex = arguments.index(after: index)
            guard valueIndex < arguments.endIndex else { break }
            let parts = arguments[valueIndex].split(separator: ",").compactMap { Int($0.trimmingCharacters(in: .whitespaces)) }
            if parts.count == 4 {
                boxes.append((parts[0], parts[1], parts[2], parts[3]))
            }
            index = arguments.index(after: valueIndex)
        }
        return boxes
    }

    private static func waitForTaskToFinish(store: WorkbenchStore, timeoutSeconds: TimeInterval) {
        let deadline = Date().addingTimeInterval(timeoutSeconds)
        while store.state.taskRun.state == .running && Date() < deadline {
            RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.05))
        }
    }

    private static func waitForMeasurementSetPlot(
        store: WorkbenchStore,
        datasetID: String,
        timeoutSeconds: TimeInterval
    ) -> Bool {
        let deadline = Date().addingTimeInterval(timeoutSeconds)
        while Date() < deadline {
            let plotState = store.state.measurementSetPlots[datasetID]
            if plotState?.status == .ready && plotState?.result != nil {
                return true
            }
            if plotState?.status == .failed {
                return false
            }
            RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.05))
        }
        return false
    }

    private func openStartupProjectIfNeeded() {
        guard !didOpenStartupProject else { return }
        didOpenStartupProject = true
        if startupNotebookPrototypeScenario != nil
            || startupPythonPrototypeScenario != nil
            || startupTutorialPrototypeScenario != nil
            || startupAIPrototypeScenario != nil {
            // The dedicated CLI launch already constructed a fresh immutable
            // prototype runtime. Never transition or reset a production store
            // from the normal startup flow.
            return
        } else if let startupTutorialPackPath {
            if let startupProjectPath { store.openProject(path: startupProjectPath) }
            store.openTutorialTemplate(path: startupTutorialPackPath)
            if let startupTutorialSectionID {
                store.selectTutorialSection(startupTutorialSectionID)
            }
        } else if let startupImagerMeasurementSetPath {
            store.openExternalMeasurementSetForImaging(path: startupImagerMeasurementSetPath)
            Self.applyImagerSchemaOverrides(arguments: CommandLine.arguments, store: store)
        } else if let startupProjectPath {
            store.openProject(path: startupProjectPath)
        }
        if startupOpenSelectedDatasetExplorer || !startupImageRegionBoxes.isEmpty {
            store.openDefaultTab(kind: .datasetExplorer)
            Self.applyImageRegionBoxes(startupImageRegionBoxes, store: store)
            Self.exportImageRegionIfNeeded(startupImageRegionExportPath, store: store)
        }
        if startupShowImagerProgressMockup {
            store.openImagerProgressMockup()
        } else if startupOpenImagerTask && startupImagerMeasurementSetPath == nil {
            store.openImagerTaskForSelectedDataset()
            store.selectTask("imager")
            store.setGenericTaskConfirmation(taskID: "imager", confirmed: true)
            if startupRunActiveTask {
                store.runTask()
            }
        } else if startupRunActiveTask && startupImagerMeasurementSetPath != nil {
            store.selectTask("imager")
            store.setGenericTaskConfirmation(taskID: "imager", confirmed: true)
            store.runTask()
        }
    }

    private func syncStoreFontSizeFromSettings() {
        store.setInterfaceFontSize(interfaceFontSize)
    }

    private func setStoredInterfaceFontSize(_ size: Double) {
        interfaceFontSize = WorkbenchState.clampedInterfaceFontSize(size)
    }

    private static func storedInterfaceFontSize() -> Double {
        guard let value = UserDefaults.standard.object(forKey: Self.interfaceFontSizeKey) as? Double else {
            return WorkbenchState.defaultInterfaceFontSize
        }
        return WorkbenchState.clampedInterfaceFontSize(value)
    }
}

struct DisplaySettingsView: View {
    @Binding var interfaceFontSize: Double

    var body: some View {
        Form {
            Section("Display") {
                HStack {
                    Text("Font size")
                    Spacer()
                    Stepper(
                        value: $interfaceFontSize,
                        in: WorkbenchState.minimumInterfaceFontSize...WorkbenchState.maximumInterfaceFontSize,
                        step: 1
                    ) {
                        Text("\(Int(interfaceFontSize.rounded())) pt")
                            .monospacedDigit()
                            .frame(width: 48, alignment: .trailing)
                    }
                    .accessibilityIdentifier("settings.fontSize.stepper")
                }

                Slider(
                    value: $interfaceFontSize,
                    in: WorkbenchState.minimumInterfaceFontSize...WorkbenchState.maximumInterfaceFontSize,
                    step: 1
                )
                .accessibilityIdentifier("settings.fontSize.slider")

                Button("Reset Font Size") {
                    interfaceFontSize = WorkbenchState.defaultInterfaceFontSize
                }
                .accessibilityIdentifier("settings.fontSize.reset")
            }
        }
        .padding(24)
        .frame(width: 360)
    }
}

final class AppDelegate: NSObject, NSApplicationDelegate {
    func applicationDidFinishLaunching(_ notification: Notification) {
        NSApp.setActivationPolicy(.regular)
        NSApp.activate(ignoringOtherApps: true)
        WorkbenchFallbackWindowController.shared.scheduleStartupWindow(arguments: CommandLine.arguments)
        WorkbenchWindowPlacement.scheduleRepairsForAppWindows()
    }
}

final class WorkbenchFallbackWindowController {
    static let shared = WorkbenchFallbackWindowController()

    private var didScheduleStartupWindow = false
    private var fallbackStore: WorkbenchStore?
    private var fallbackWindow: NSWindow?

    private init() {}

    func scheduleStartupWindow(arguments: [String]) {
        guard !didScheduleStartupWindow else { return }
        didScheduleStartupWindow = true
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) { [weak self] in
            self?.openFallbackWindowIfNeeded(arguments: arguments)
        }
    }

    private func openFallbackWindowIfNeeded(arguments: [String]) {
        guard fallbackWindow == nil else { return }
        if let visibleWindow = NSApp.windows.first(where: { $0.isVisible }) {
            WorkbenchWindowPlacement.apply(to: visibleWindow)
            WorkbenchWindowPlacement.scheduleRepairs(for: visibleWindow)
            visibleWindow.makeKeyAndOrderFront(nil)
            NSApp.activate(ignoringOtherApps: true)
            return
        }

        let notebookPrototypeScenario = CasarsMacApp.notebookPrototypeScenario(arguments: arguments)
        let pythonPrototypeScenario = CasarsMacApp.pythonPrototypeScenario(arguments: arguments)
        let tutorialPrototypeScenario = CasarsMacApp.tutorialPrototypeScenario(arguments: arguments)
        let aiPrototypeScenario = CasarsMacApp.aiPrototypeScenario(arguments: arguments)
        let store = notebookPrototypeScenario.map { WorkbenchStore.notebookPrototype(scenario: $0) }
            ?? pythonPrototypeScenario.map { WorkbenchStore.pythonPrototype(scenario: $0) }
            ?? tutorialPrototypeScenario.map { WorkbenchStore.tutorialPrototype(scenario: $0) }
            ?? aiPrototypeScenario.map { WorkbenchStore.aiPrototype(scenario: $0) }
            ?? WorkbenchStore.empty()
        store.setInterfaceFontSize(Self.storedInterfaceFontSize())
        if notebookPrototypeScenario == nil, pythonPrototypeScenario == nil,
           tutorialPrototypeScenario == nil, aiPrototypeScenario == nil,
           let tutorialPackPath = Self.argumentValue(after: "--open-tutorial-pack", in: arguments) {
            if let projectPath = Self.argumentValue(after: "--open-project", in: arguments)
                ?? Self.argumentValue(after: "--probe-project", in: arguments) {
                store.openProject(path: projectPath)
            }
            store.openTutorialTemplate(path: tutorialPackPath)
            if let tutorialSectionID = Self.argumentValue(after: "--open-tutorial-section", in: arguments) {
                store.selectTutorialSection(tutorialSectionID)
            }
        } else if let imagerMeasurementSetPath = Self.argumentValue(after: "--open-imager-ms", in: arguments) {
            store.openExternalMeasurementSetForImaging(path: imagerMeasurementSetPath)
            CasarsMacApp.applyImagerSchemaOverrides(arguments: arguments, store: store)
        } else if let projectPath = Self.argumentValue(after: "--open-project", in: arguments)
            ?? Self.argumentValue(after: "--probe-project", in: arguments) {
            store.openProject(path: projectPath)
        }
        if notebookPrototypeScenario == nil, pythonPrototypeScenario == nil,
           tutorialPrototypeScenario == nil, aiPrototypeScenario == nil {
            if arguments.contains("--show-imager-progress-mockup") {
                store.openImagerProgressMockup()
            }
            if arguments.contains("--run-active-task") {
                store.selectTask("imager")
                store.setGenericTaskConfirmation(taskID: "imager", confirmed: true)
                store.runTask()
            }
        }

        let rootView = WorkbenchView(store: store)
            .frame(minWidth: 960, minHeight: 640)
            .environment(\.workbenchFontSize, store.state.interfaceFontSize)
        let window = NSWindow(
            contentRect: NSRect(x: 140, y: 120, width: 1280, height: 860),
            styleMask: [.titled, .closable, .miniaturizable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.title = "casa-rs Workbench"
        window.identifier = NSUserInterfaceItemIdentifier("casars-mac-live-workbench-\(UUID().uuidString)")
        window.restorationClass = nil
        WorkbenchWindowPlacement.apply(to: window, forcePreferredFrame: true)
        window.isReleasedWhenClosed = false
        window.contentViewController = NSHostingController(rootView: rootView)
        WorkbenchWindowPlacement.apply(to: window, forcePreferredFrame: true)
        fallbackStore = store
        fallbackWindow = window
        window.makeKeyAndOrderFront(nil)
        window.orderFrontRegardless()
        WorkbenchWindowPlacement.scheduleRepairs(for: window)
        NSApp.activate(ignoringOtherApps: true)
        NSRunningApplication.current.activate(options: [.activateAllWindows, .activateIgnoringOtherApps])
    }

    private static func argumentValue(after flag: String, in arguments: [String]) -> String? {
        guard let index = arguments.firstIndex(of: flag) else { return nil }
        let valueIndex = arguments.index(after: index)
        guard arguments.indices.contains(valueIndex) else { return nil }
        return arguments[valueIndex]
    }

    private static func storedInterfaceFontSize() -> Double {
        let value = UserDefaults.standard.double(forKey: "interfaceFontSize")
        return value == 0 ? WorkbenchState.defaultInterfaceFontSize : WorkbenchState.clampedInterfaceFontSize(value)
    }
}

enum WorkbenchWindowPlacement {
    private static let minimumSize = NSSize(width: 960, height: 640)
    private static let preferredSize = NSSize(width: 1280, height: 860)

    static func apply(to window: NSWindow, forcePreferredFrame: Bool = false) {
        window.title = "casa-rs Workbench"
        window.minSize = minimumSize
        window.contentMinSize = minimumSize
        window.styleMask.insert([.titled, .closable, .miniaturizable, .resizable])
        window.collectionBehavior = [.managed, .fullScreenPrimary]
        window.isRestorable = false
        if forcePreferredFrame {
            setPreferredFrame(window)
        } else {
            repairFrameIfNeeded(window)
        }
    }

    static func scheduleRepairs(for window: NSWindow) {
        for delay in [0.1, 0.5, 1.0, 2.0] {
            DispatchQueue.main.asyncAfter(deadline: .now() + delay) { [weak window] in
                guard let window else { return }
                apply(to: window, forcePreferredFrame: true)
                window.makeKeyAndOrderFront(nil)
            }
        }
    }

    static func scheduleRepairsForAppWindows() {
        for delay in [0.1, 0.5, 1.0, 2.0] {
            DispatchQueue.main.asyncAfter(deadline: .now() + delay) {
                for window in NSApp.windows {
                    apply(to: window)
                    window.makeKeyAndOrderFront(nil)
                }
                NSApp.activate(ignoringOtherApps: true)
            }
        }
    }

    private static func repairFrameIfNeeded(_ window: NSWindow) {
        guard needsFrameRepair(window) else { return }
        setPreferredFrame(window)
    }

    private static func setPreferredFrame(_ window: NSWindow) {
        let visibleFrame = window.screen?.visibleFrame ?? NSScreen.main?.visibleFrame ?? NSRect(
            x: 80,
            y: 80,
            width: preferredSize.width + 160,
            height: preferredSize.height + 160
        )
        let width = min(preferredSize.width, max(minimumSize.width, visibleFrame.width - 40))
        let height = min(preferredSize.height, max(minimumSize.height, visibleFrame.height - 40))
        let origin = NSPoint(
            x: visibleFrame.midX - width / 2,
            y: visibleFrame.midY - height / 2
        )
        window.setFrame(NSRect(origin: origin, size: NSSize(width: width, height: height)), display: true)
    }

    private static func needsFrameRepair(_ window: NSWindow) -> Bool {
        let frame = window.frame
        if frame.width < minimumSize.width || frame.height < minimumSize.height {
            return true
        }
        guard let visibleFrame = window.screen?.visibleFrame ?? NSScreen.main?.visibleFrame else {
            return false
        }
        return !frame.intersection(visibleFrame).contains(frame.center)
    }
}

private extension NSRect {
    var center: NSPoint {
        NSPoint(x: midX, y: midY)
    }
}

enum FullScreenController {
    static func toggleFullScreen() {
        if let window = NSApp.keyWindow ?? NSApp.mainWindow ?? NSApp.windows.first(where: { $0.canBecomeKey }) {
            window.toggleFullScreen(nil)
        }
    }
}

enum ProjectOpenPanel {
    static func chooseDirectory() -> URL? {
        let panel = NSOpenPanel()
        panel.canChooseFiles = false
        panel.canChooseDirectories = true
        panel.treatsFilePackagesAsDirectories = true
        panel.allowsMultipleSelection = false
        panel.prompt = "Open"
        return panel.runModal() == .OK ? panel.url : nil
    }
}

enum TutorialTemplateOpenPanel {
    static func chooseTemplate() -> URL? {
        let panel = NSOpenPanel()
        panel.canChooseFiles = true
        panel.canChooseDirectories = true
        panel.treatsFilePackagesAsDirectories = true
        panel.allowsMultipleSelection = false
        panel.prompt = "Fork Tutorial Template"
        return panel.runModal() == .OK ? panel.url : nil
    }
}

struct WindowConfigurationView: NSViewRepresentable {
    func makeNSView(context: Context) -> WindowConfigurationHost {
        WindowConfigurationHost()
    }

    func updateNSView(_ nsView: WindowConfigurationHost, context: Context) {
        nsView.configureWindowIfPossible()
    }
}

final class WindowConfigurationHost: NSView {
    private var didActivateWindow = false

    override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        configureWindowIfPossible()
    }

    func configureWindowIfPossible() {
        guard let window else { return }

        WorkbenchWindowPlacement.apply(to: window)

        guard !didActivateWindow else { return }
        didActivateWindow = true
        DispatchQueue.main.async { [weak window] in
            NSApp.setActivationPolicy(.regular)
            NSApp.activate(ignoringOtherApps: true)
            if let window {
                WorkbenchWindowPlacement.apply(to: window)
                WorkbenchWindowPlacement.scheduleRepairs(for: window)
            }
            window?.makeKeyAndOrderFront(nil)
        }
    }
}
