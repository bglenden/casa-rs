import Foundation
import CasarsFrontendServices
import OSLog

private let datasetSelectionLogger = Logger(
    subsystem: "org.casa-rs.casars-mac",
    category: "DatasetSelection"
)

public protocol ProjectProbeClient {
    func probeProject(path: String) throws -> ProjectFixtureProbe
    func probePath(path: String) throws -> DatasetSummary?
}

public struct ProjectFixtureProbe: Equatable {
    public var project: ProjectFixture
    public var diagnostics: [String]

    public init(project: ProjectFixture, diagnostics: [String]) {
        self.project = project
        self.diagnostics = diagnostics
    }
}

public struct UniFFIProjectProbeClient: ProjectProbeClient {
    public init() {}

    public func probeProject(path: String) throws -> ProjectFixtureProbe {
        let probe = try CasarsFrontendServices.probeProject(path: path)
        return ProjectFixtureProbe(project: ProjectFixture(probe: probe), diagnostics: probe.diagnostics)
    }

    public func probePath(path: String) throws -> DatasetSummary? {
        try CasarsFrontendServices.probePath(path: path).map(DatasetSummary.init(probe:))
    }
}

public struct MeasurementSetPlotBuildRequest: Equatable {
    public var datasetPath: String
    public var preset: MeasurementSetExplorerPlotPreset
    public var field: String?
    public var spectralWindow: String?
    public var correlation: String?
    public var dataColumn: String
    public var width: UInt32
    public var height: UInt32
    public var maxPlotPoints: UInt64

    public init(
        datasetPath: String,
        preset: MeasurementSetExplorerPlotPreset,
        field: String?,
        spectralWindow: String?,
        correlation: String?,
        dataColumn: String,
        width: UInt32 = 960,
        height: UInt32 = 600,
        maxPlotPoints: UInt64 = 250_000
    ) {
        self.datasetPath = datasetPath
        self.preset = preset
        self.field = field
        self.spectralWindow = spectralWindow
        self.correlation = correlation
        self.dataColumn = dataColumn
        self.width = width
        self.height = height
        self.maxPlotPoints = maxPlotPoints
    }
}

public protocol MeasurementSetPlotClient {
    func buildPlot(request: MeasurementSetPlotBuildRequest) throws -> MeasurementSetPlotResultSummary
}

public struct UniFFIMeasurementSetPlotClient: MeasurementSetPlotClient {
    public init() {}

    public func buildPlot(request: MeasurementSetPlotBuildRequest) throws -> MeasurementSetPlotResultSummary {
        let result = try CasarsFrontendServices.buildMeasurementSetPlot(
            request: CasarsFrontendServices.MeasurementSetPlotRequest(
                datasetPath: request.datasetPath,
                preset: CasarsFrontendServices.MeasurementSetPlotPreset(preset: request.preset),
                field: request.field,
                spectralWindow: request.spectralWindow,
                correlation: request.correlation,
                dataColumn: request.dataColumn,
                width: request.width,
                height: request.height,
                maxPlotPoints: request.maxPlotPoints
            )
        )
        return MeasurementSetPlotResultSummary(result: result)
    }
}

public final class WorkbenchStore: ObservableObject {
    @Published public private(set) var state: WorkbenchState
    private let probeClient: ProjectProbeClient
    private let plotClient: MeasurementSetPlotClient
    private let dirtyImagingClient: DirtyImagingTaskClient
    private let plotQueue = DispatchQueue(label: "casars.mac.ms-plot-job", qos: .userInitiated, attributes: .concurrent)
    private var activeTaskExecutions: [String: DirtyImagingTaskExecution] = [:]

    public init(
        state: WorkbenchState = EmptyWorkbench.makeState(),
        probeClient: ProjectProbeClient = UniFFIProjectProbeClient(),
        plotClient: MeasurementSetPlotClient = UniFFIMeasurementSetPlotClient(),
        dirtyImagingClient: DirtyImagingTaskClient = ProcessDirtyImagingTaskClient()
    ) {
        self.state = state
        self.probeClient = probeClient
        self.plotClient = plotClient
        self.dirtyImagingClient = dirtyImagingClient
    }

    public static func empty() -> WorkbenchStore {
        WorkbenchStore(state: EmptyWorkbench.makeState())
    }

    public static func fixture() -> WorkbenchStore {
        WorkbenchStore(state: FixtureWorkbench.makeState())
    }

    public func openFixtureProject() {
        let interfaceFontSize = state.interfaceFontSize
        state = FixtureWorkbench.makeState()
        state.interfaceFontSize = interfaceFontSize
    }

    public func openProject(path: String) {
        let interfaceFontSize = state.interfaceFontSize
        do {
            let probed = try probeClient.probeProject(path: path)
            state = EmptyWorkbench.makeState(interfaceFontSize: interfaceFontSize)
            state.project = probed.project
            state.probeDiagnostics = probed.diagnostics
            state.selectedDatasetID = probed.project.datasets.first?.id
            state.dockMode = .datasets
            state.leftDockCollapsed = false
            state.inspectorCollapsed = false
            if let dataset = state.selectedDataset {
                openExplorer(for: dataset)
            }
            state.history.append(
                ProcessingHistoryEvent(
                    id: "hist-project-open-\(state.history.count + 1)",
                    timestamp: "probed",
                    title: "Project opened",
                    reason: "Opened real project directory and probed datasets with Rust frontend services.",
                    affectedPaths: [probed.project.rootPath],
                    approval: "user"
                )
            )
        } catch {
            state.lastErrors.append("Open project \(path): \(error)")
        }
    }

    public func selectDockMode(_ mode: DockMode) {
        state.dockMode = mode
        state.leftDockCollapsed = false
    }

    public func selectDataset(_ datasetID: String) {
        let started = DispatchTime.now().uptimeNanoseconds
        let previousDatasetID = state.selectedDatasetID
        guard state.project.datasets.contains(where: { $0.id == datasetID }) else {
            state.lastErrors.append("Unknown dataset \(datasetID)")
            datasetSelectionLogger.error("select_dataset unknown id=\(datasetID, privacy: .public)")
            return
        }
        guard previousDatasetID != datasetID else {
            let elapsedMilliseconds = Double(DispatchTime.now().uptimeNanoseconds - started) / 1_000_000
            datasetSelectionLogger.debug(
                "select_dataset noop id=\(datasetID, privacy: .public) elapsed_ms=\(elapsedMilliseconds, privacy: .public)"
            )
            return
        }

        state.selectedDatasetID = datasetID
        let elapsedMilliseconds = Double(DispatchTime.now().uptimeNanoseconds - started) / 1_000_000
        let datasetCount = state.project.datasets.count
        let inspectorCollapsed = state.inspectorCollapsed
        let activeTabID = state.activeTabID
        datasetSelectionLogger.info(
            "select_dataset changed from=\(previousDatasetID ?? "none", privacy: .public) to=\(datasetID, privacy: .public) dataset_count=\(datasetCount, privacy: .public) inspector_collapsed=\(inspectorCollapsed, privacy: .public) active_tab=\(activeTabID, privacy: .public) elapsed_ms=\(elapsedMilliseconds, privacy: .public)"
        )
    }

    public func openSelectedDatasetExplorer() {
        guard let dataset = state.selectedDataset else {
            state.lastErrors.append("No selected dataset to explore")
            return
        }

        openExplorer(for: dataset)
    }

    public func openDatasetExplorer(_ datasetID: String) {
        guard let dataset = state.project.datasets.first(where: { $0.id == datasetID }) else {
            state.lastErrors.append("Unknown dataset \(datasetID)")
            return
        }

        state.selectedDatasetID = datasetID
        openExplorer(for: dataset)
    }

    public func setMeasurementSetPlotPreset(_ preset: MeasurementSetExplorerPlotPreset, datasetID: String) {
        var plotState = measurementSetPlotState(for: datasetID)
        plotState.preset = preset
        plotState.lastError = nil
        refreshMeasurementSetPlotStateFromCache(&plotState, datasetID: datasetID)
        state.measurementSetPlots[datasetID] = plotState
    }

    public func setMeasurementSetPlotField(_ field: String?, datasetID: String) {
        var plotState = measurementSetPlotState(for: datasetID)
        plotState.selectedField = normalizedPickerValue(field)
        plotState.lastError = nil
        refreshMeasurementSetPlotStateFromCache(&plotState, datasetID: datasetID)
        state.measurementSetPlots[datasetID] = plotState
    }

    public func setMeasurementSetPlotSpectralWindow(_ spectralWindow: String?, datasetID: String) {
        var plotState = measurementSetPlotState(for: datasetID)
        plotState.selectedSpectralWindow = normalizedPickerValue(spectralWindow)
        plotState.lastError = nil
        refreshMeasurementSetPlotStateFromCache(&plotState, datasetID: datasetID)
        state.measurementSetPlots[datasetID] = plotState
    }

    public func setMeasurementSetPlotCorrelation(_ correlation: String?, datasetID: String) {
        var plotState = measurementSetPlotState(for: datasetID)
        plotState.selectedCorrelation = normalizedPickerValue(correlation)
        plotState.lastError = nil
        refreshMeasurementSetPlotStateFromCache(&plotState, datasetID: datasetID)
        state.measurementSetPlots[datasetID] = plotState
    }

    public func setMeasurementSetPlotDataColumn(_ dataColumn: String, datasetID: String) {
        var plotState = measurementSetPlotState(for: datasetID)
        plotState.dataColumn = dataColumn
        plotState.lastError = nil
        refreshMeasurementSetPlotStateFromCache(&plotState, datasetID: datasetID)
        state.measurementSetPlots[datasetID] = plotState
    }

    public func runMeasurementSetPlot(datasetID: String) {
        guard !state.isDemoProject else {
            state.lastErrors.append("Real MeasurementSet plots are not available in the demo project")
            return
        }
        guard let dataset = state.project.datasets.first(where: { $0.id == datasetID }) else {
            state.lastErrors.append("Unknown dataset \(datasetID)")
            return
        }
        guard dataset.kind == .measurementSet else {
            state.lastErrors.append("Dataset \(dataset.name) is not a MeasurementSet")
            return
        }

        var plotState = measurementSetPlotState(for: datasetID)
        if let cached = cachedMeasurementSetPlotResult(for: dataset, plotState: plotState) {
            plotState.result = cached
            plotState.status = .ready
            plotState.lastError = nil
            state.measurementSetPlots[datasetID] = plotState
            return
        }

        let request = MeasurementSetPlotBuildRequest(
            datasetPath: dataset.path,
            preset: plotState.preset,
            field: selectorToken(plotState.selectedField),
            spectralWindow: selectorToken(plotState.selectedSpectralWindow),
            correlation: selectorToken(plotState.selectedCorrelation),
            dataColumn: plotState.dataColumn
        )
        let tabID = dataset.explorerTabID
        let jobID = nextJobID(prefix: "ms-plot")
        startJob(
            WorkbenchJob(
                id: jobID,
                tabID: tabID,
                kind: .measurementSetPlot,
                owner: .user,
                status: .running,
                progress: 0.05,
                title: "Generate \(plotState.preset.title)",
                detail: dataset.name,
                logLines: ["Queued MeasurementSet plot render.", request.preset.title],
                lastEvent: "started"
            )
        )

        plotState.status = .running
        plotState.lastError = nil
        state.measurementSetPlots[datasetID] = plotState

        let requestedPlotState = plotState
        plotQueue.async { [plotClient] in
            do {
                let result = try plotClient.buildPlot(request: request)
                DispatchQueue.main.async { [weak self] in
                    self?.finishMeasurementSetPlotJob(
                        jobID: jobID,
                        dataset: dataset,
                        plotState: requestedPlotState,
                        result: result
                    )
                }
            } catch {
                DispatchQueue.main.async { [weak self] in
                    self?.failMeasurementSetPlotJob(
                        jobID: jobID,
                        datasetID: datasetID,
                        datasetName: dataset.name,
                        error: "\(error)"
                    )
                }
            }
        }
    }

    public func setInspectorCollapsed(_ collapsed: Bool) {
        state.inspectorCollapsed = collapsed
    }

    public func toggleInspector() {
        state.inspectorCollapsed.toggle()
    }

    public func setLeftDockCollapsed(_ collapsed: Bool) {
        state.leftDockCollapsed = collapsed
    }

    public func toggleLeftDock() {
        state.leftDockCollapsed.toggle()
    }

    public func setCommandQuery(_ query: String) {
        state.commandQuery = query
    }

    public func runCommandQuery() {
        let query = state.commandQuery.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !query.isEmpty else {
            openDefaultTab(kind: .aiChat)
            return
        }

        let normalized = query.lowercased()
        if normalized.contains("python") {
            openDefaultTab(kind: .python)
        } else if normalized.contains("history") || normalized.contains("timeline") {
            openDefaultTab(kind: .history)
            selectDockMode(.history)
        } else if normalized.contains("left dock") || normalized.contains("sidebar") {
            setLeftDockCollapsed(false)
        } else if normalized.contains("task") || normalized.contains("calibrate") || normalized.contains("image") || normalized.contains("tclean") {
            openDefaultTab(kind: .task)
        } else if normalized.contains("inspector") {
            setInspectorCollapsed(false)
        } else if normalized.contains("dataset") || normalized.contains("ms") {
            openDefaultTab(kind: .datasetExplorer)
            selectDockMode(.datasets)
        } else {
            appendAIChatMessage(query)
            openDefaultTab(kind: .aiChat)
        }
    }

    public func openTab(_ tab: WorkbenchTab) {
        if !state.tabs.contains(where: { $0.id == tab.id }) {
            state.tabs.append(tab)
        }
        state.activeTabID = tab.id
    }

    public func activateTab(_ tabID: String) {
        guard state.tabs.contains(where: { $0.id == tabID }) else {
            state.lastErrors.append("Unknown tab \(tabID)")
            return
        }
        state.activeTabID = tabID
    }

    public func closeTab(_ tabID: String) {
        guard let index = state.tabs.firstIndex(where: { $0.id == tabID }) else {
            state.lastErrors.append("Unknown tab \(tabID)")
            return
        }

        let wasActive = state.activeTabID == tabID
        state.tabs.remove(at: index)

        guard wasActive else {
            return
        }

        if state.tabs.isEmpty {
            state.activeTabID = ""
        } else {
            let replacementIndex = min(index, state.tabs.count - 1)
            state.activeTabID = state.tabs[replacementIndex].id
        }
    }

    public func closeActiveTab() {
        guard !state.activeTabID.isEmpty else {
            return
        }

        closeTab(state.activeTabID)
    }

    public func openDefaultTab(kind: WorkbenchTabKind) {
        switch kind {
        case .datasetExplorer:
            openSelectedDatasetExplorer()
        case .task:
            if state.isDemoProject {
                openTab(WorkbenchTab(id: "tab-task", title: "Calibrate", kind: .task, datasetID: state.selectedDatasetID))
            } else {
                openDirtyImagingTaskForSelectedDataset()
            }
        case .aiChat:
            guard state.isDemoProject else {
                state.lastErrors.append("AI chat is not connected yet")
                return
            }
            openTab(WorkbenchTab(id: "tab-ai", title: "AI Chat", kind: .aiChat))
        case .python:
            guard state.isDemoProject else {
                state.lastErrors.append("Python is not connected yet")
                return
            }
            openTab(WorkbenchTab(id: "tab-python", title: "Python", kind: .python))
        case .history:
            openTab(WorkbenchTab(id: "tab-history", title: "History", kind: .history))
        }
    }

    public func openDirtyImagingTaskForSelectedDataset() {
        guard state.selectedDataset != nil else {
            state.lastErrors.append("Open a project with a dataset before opening an imaging task")
            return
        }

        if let dataset = state.selectedDataset, dataset.kind == .measurementSet {
            if state.dirtyImagingTaskParameters?.datasetID != dataset.id {
                state.dirtyImagingTaskParameters = defaultDirtyImagingParameters(for: dataset)
                state.taskRun = TaskRun(
                    state: .idle,
                    progress: 0,
                    logLines: ["Dirty imaging task initialized from selected MeasurementSet metadata."],
                    warnings: [],
                    products: [],
                    requestSummary: state.dirtyImagingTaskParameters?.requestSummary
                )
            }

            openTab(
                WorkbenchTab(
                    id: "tab-dirty-imaging-\(dataset.id)",
                    title: "Dirty Image: \(dataset.name)",
                    kind: .task,
                    datasetID: dataset.id
                )
            )
        } else {
            state.dirtyImagingTaskParameters = blankDirtyImagingParameters()
            state.taskRun = TaskRun(
                state: .idle,
                progress: 0,
                logLines: ["Dirty imaging task opened. Select a MeasurementSet before running."],
                warnings: [],
                products: [],
                requestSummary: state.dirtyImagingTaskParameters?.requestSummary
            )

            openTab(
                WorkbenchTab(
                    id: "tab-dirty-imaging-unbound",
                    title: "Dirty Image",
                    kind: .task
                )
            )
        }
    }

    public func setDirtyImagingField(_ field: String?) {
        guard var parameters = state.dirtyImagingTaskParameters else { return }
        parameters.selectedField = normalizedPickerValue(field)
        parameters.phaseCenterField = parameters.selectedField
        updateDirtyImagingParameters(parameters)
    }

    public func setDirtyImagingSpectralWindow(_ spectralWindow: String?) {
        guard var parameters = state.dirtyImagingTaskParameters else { return }
        parameters.selectedSpectralWindow = normalizedPickerValue(spectralWindow)
        updateDirtyImagingParameters(parameters)
    }

    public func setDirtyImagingDataColumn(_ dataColumn: String) {
        guard var parameters = state.dirtyImagingTaskParameters else { return }
        parameters.dataColumn = dataColumn
        updateDirtyImagingParameters(parameters)
    }

    public func setDirtyImagingOutputPrefix(_ outputPrefix: String) {
        guard var parameters = state.dirtyImagingTaskParameters else { return }
        parameters.outputPrefix = outputPrefix
        updateDirtyImagingParameters(parameters)
    }

    public func setDirtyImagingImageSize(_ imageSize: Int) {
        guard var parameters = state.dirtyImagingTaskParameters else { return }
        parameters.imageSize = imageSize
        updateDirtyImagingParameters(parameters)
    }

    public func setDirtyImagingImageHeight(_ imageHeight: Int) {
        guard var parameters = state.dirtyImagingTaskParameters else { return }
        parameters.imageHeight = imageHeight
        updateDirtyImagingParameters(parameters)
    }

    public func adjustDirtyImagingImageWidthToNiceSize() {
        guard var parameters = state.dirtyImagingTaskParameters else { return }
        parameters.imageSize = DirtyImagingTaskParameters.nearestNiceImageDimension(to: parameters.imageSize)
        updateDirtyImagingParameters(parameters)
    }

    public func adjustDirtyImagingImageHeightToNiceSize() {
        guard var parameters = state.dirtyImagingTaskParameters else { return }
        parameters.imageHeight = DirtyImagingTaskParameters.nearestNiceImageDimension(to: parameters.imageHeight)
        updateDirtyImagingParameters(parameters)
    }

    public func setDirtyImagingCellArcsec(_ cellArcsec: Double) {
        guard var parameters = state.dirtyImagingTaskParameters else { return }
        parameters.cellArcsec = cellArcsec
        updateDirtyImagingParameters(parameters)
    }

    public func setDirtyImagingWeighting(_ weighting: DirtyImagingWeighting) {
        guard var parameters = state.dirtyImagingTaskParameters else { return }
        parameters.weighting = weighting
        updateDirtyImagingParameters(parameters)
    }

    public func setDirtyImagingChannelStart(_ channelStart: String) {
        guard var parameters = state.dirtyImagingTaskParameters else { return }
        parameters.channelStart = channelStart
        updateDirtyImagingParameters(parameters)
    }

    public func setDirtyImagingChannelCount(_ channelCount: String) {
        guard var parameters = state.dirtyImagingTaskParameters else { return }
        parameters.channelCount = channelCount
        updateDirtyImagingParameters(parameters)
    }

    public func setDirtyImagingRunReason(_ reason: String) {
        guard var parameters = state.dirtyImagingTaskParameters else { return }
        parameters.runReason = reason
        updateDirtyImagingParameters(parameters)
    }

    public func setDirtyImagingDataset(_ datasetID: String) {
        guard let current = state.dirtyImagingTaskParameters else { return }
        guard !datasetID.isEmpty else {
            state.dirtyImagingTaskParameters = blankDirtyImagingParameters()
            state.dirtyImagingTaskParameters?.imageSize = current.imageSize
            state.dirtyImagingTaskParameters?.imageHeight = current.imageHeight
            state.dirtyImagingTaskParameters?.cellArcsec = current.cellArcsec
            state.dirtyImagingTaskParameters?.weighting = current.weighting
            state.dirtyImagingTaskParameters?.dirtyOnly = current.dirtyOnly
            state.dirtyImagingTaskParameters?.runReason = current.runReason
            state.taskRun = TaskRun(
                state: .idle,
                progress: 0,
                logLines: ["Dirty imaging task input MeasurementSet cleared."],
                warnings: [],
                products: [],
                requestSummary: state.dirtyImagingTaskParameters?.requestSummary
            )
            if let activeIndex = state.tabs.firstIndex(where: { $0.id == state.activeTabID && $0.kind == .task }) {
                state.tabs[activeIndex].title = "Dirty Image"
                state.tabs[activeIndex].datasetID = nil
            }
            return
        }
        guard let dataset = state.project.datasets.first(where: { $0.id == datasetID }) else {
            state.lastErrors.append("Unknown dataset \(datasetID)")
            return
        }
        guard dataset.kind == .measurementSet else {
            state.lastErrors.append("Dataset \(dataset.name) is not a MeasurementSet")
            return
        }

        state.selectedDatasetID = datasetID
        var parameters = defaultDirtyImagingParameters(for: dataset)
        parameters.imageSize = current.imageSize
        parameters.imageHeight = current.imageHeight
        parameters.cellArcsec = current.cellArcsec
        parameters.weighting = current.weighting
        parameters.dirtyOnly = current.dirtyOnly
        parameters.runReason = current.runReason
        state.dirtyImagingTaskParameters = parameters
        state.taskRun = TaskRun(
            state: .idle,
            progress: 0,
            logLines: ["Dirty imaging task input MeasurementSet changed to \(dataset.name)."],
            warnings: [],
            products: [],
            requestSummary: parameters.requestSummary
        )

        if let activeIndex = state.tabs.firstIndex(where: { $0.id == state.activeTabID && $0.kind == .task }) {
            state.tabs[activeIndex].title = "Dirty Image: \(dataset.name)"
            state.tabs[activeIndex].datasetID = dataset.id
        }
    }

    public func applyAIProposal(_ proposalID: String) {
        guard state.isDemoProject else {
            state.lastErrors.append("AI proposals are only available in the demo project")
            return
        }
        guard let index = state.aiProposals.firstIndex(where: { $0.id == proposalID }) else {
            state.lastErrors.append("Unknown AI proposal \(proposalID)")
            return
        }

        state.aiProposals[index].state = .applied
        let proposal = state.aiProposals[index]
        if proposal.parameterName == "Spectral window" {
            state.taskParameters.selectedSpectralWindow = proposal.newValue
        }
        state.history.append(
            ProcessingHistoryEvent(
                id: "hist-\(proposalID)-applied",
                timestamp: "2026-05-04 09:21",
                title: "AI proposal applied",
                reason: proposal.detail,
                affectedPaths: ["task/calibrate.request"],
                approval: "user"
            )
        )
    }

    public func rejectAIProposal(_ proposalID: String) {
        guard state.isDemoProject else {
            state.lastErrors.append("AI proposals are only available in the demo project")
            return
        }
        guard let index = state.aiProposals.firstIndex(where: { $0.id == proposalID }) else {
            state.lastErrors.append("Unknown AI proposal \(proposalID)")
            return
        }

        state.aiProposals[index].state = .rejected
    }

    public func appendAIChatMessage(_ text: String, author: ChatAuthor = .user) {
        guard state.isDemoProject else {
            state.lastErrors.append("AI chat is not connected yet")
            return
        }
        let id = "msg-\(state.aiMessages.count + 1)"
        state.aiMessages.append(AIChatMessage(id: id, author: author, text: text))
    }

    public func setTaskSpectralWindow(_ spectralWindow: String) {
        guard state.isDemoProject else {
            state.lastErrors.append("Task parameters are only available in the demo project")
            return
        }
        state.taskParameters.selectedSpectralWindow = spectralWindow
    }

    public func runTask() {
        if state.isDemoProject {
            state.taskRun = TaskRun(
                state: .completed,
                progress: 1.0,
                logLines: [
                    "Started fixture calibrate dry run.",
                    "Resolved field \(state.taskParameters.selectedField).",
                    "Resolved spectral window \(state.taskParameters.selectedSpectralWindow).",
                    "Recorded fixture product \(state.taskParameters.outputName)."
                ],
                warnings: ["Fixture run: no science data was modified."],
                products: ["project/products/\(state.taskParameters.outputName)"]
            )
            state.history.append(
                ProcessingHistoryEvent(
                    id: "hist-run-\(state.history.count + 1)",
                    timestamp: currentTimestamp(),
                    title: "Fixture task completed",
                    reason: "User ran the dry-run task from the task tab.",
                    affectedPaths: state.taskRun.products,
                    approval: "user"
                )
            )
            return
        }

        guard let parameters = state.dirtyImagingTaskParameters else {
            state.lastErrors.append("Open a dirty-imaging task before running it")
            return
        }
        let validationErrors = parameters.validationErrors()
        guard validationErrors.isEmpty else {
            state.taskRun = TaskRun(
                state: .failed,
                progress: 0,
                logLines: ["Dirty imaging request validation failed."],
                warnings: [],
                products: [],
                diagnostics: validationErrors,
                requestSummary: parameters.requestSummary
            )
            state.lastErrors.append(contentsOf: validationErrors)
            return
        }

        let runID = nextJobID(prefix: "dirty-imaging")
        let request = DirtyImagingTaskRequest(runID: runID, parameters: parameters)
        let tabID = activeTaskTabID(parameters: parameters)
        startJob(
            WorkbenchJob(
                id: runID,
                tabID: tabID,
                kind: .dirtyImagingTask,
                owner: .user,
                status: .running,
                progress: 0.05,
                title: "Dirty imaging",
                detail: parameters.measurementSetPath,
                logLines: ["Starting casars-imager dirty imaging task.", parameters.requestSummary],
                lastEvent: "started"
            )
        )
        state.taskRun = TaskRun(
            runID: runID,
            state: .running,
            progress: 0.05,
            logLines: [
                "Starting casars-imager dirty imaging task.",
                parameters.requestSummary
            ],
            warnings: [],
            products: [],
            diagnostics: [],
            requestSummary: parameters.requestSummary
        )

        do {
            let execution = try dirtyImagingClient.startDirtyImaging(request: request) { [weak self] event in
                self?.handleDirtyImagingEvent(event, runID: runID, jobID: runID)
            }
            if state.jobs[runID]?.status == .running {
                activeTaskExecutions[runID] = execution
            }
        } catch {
            failDirtyImagingJob(runID: runID, message: "Failed to start casars-imager.", diagnostics: ["\(error)"])
            state.lastErrors.append("Start dirty imaging: \(error)")
        }
    }

    public func stopTask() {
        if state.isDemoProject {
            state.taskRun.state = .stopped
            state.taskRun.logLines.append("Stopped fixture task.")
            return
        }

        guard state.taskRun.state == .running, let runID = state.taskRun.runID else {
            state.lastErrors.append("No dirty imaging task is running")
            return
        }
        cancelJob(runID, recordError: false)
        state.history.append(
            ProcessingHistoryEvent(
                id: "hist-run-\(state.history.count + 1)",
                timestamp: currentTimestamp(),
                title: "Dirty imaging cancelled",
                reason: state.dirtyImagingTaskParameters?.runReason ?? "User cancelled the dirty imaging run.",
                affectedPaths: state.taskRun.outputPaths,
                approval: "user"
            )
        )
    }

    public func setPythonOwner(_ owner: PythonOwner) {
        guard state.isDemoProject else {
            state.lastErrors.append("Python is not connected yet")
            return
        }
        state.python.owner = owner
    }

    public func setInterfaceFontSize(_ size: Double) {
        state.interfaceFontSize = WorkbenchState.clampedInterfaceFontSize(size)
    }

    public func adjustInterfaceFontSize(by delta: Double) {
        setInterfaceFontSize(state.interfaceFontSize + delta)
    }

    public func resetInterfaceFontSize() {
        setInterfaceFontSize(WorkbenchState.defaultInterfaceFontSize)
    }

    public func debugSnapshot() -> DebugStateSnapshot {
        DebugStateSnapshot(state: state)
    }

    public func debugJSON(pretty: Bool = true) throws -> String {
        let encoder = JSONEncoder()
        if pretty {
            encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        }
        let data = try encoder.encode(debugSnapshot())
        return String(decoding: data, as: UTF8.self)
    }

    public func cancelJob(_ jobID: String) {
        cancelJob(jobID, recordError: true)
    }

    private func startJob(_ job: WorkbenchJob) {
        if let existingJobID = state.activeJobIDsByTab[job.tabID] {
            cancelJob(existingJobID, recordError: false)
        }
        state.jobs[job.id] = job
        state.activeJobIDsByTab[job.tabID] = job.id
    }

    private func cancelJob(_ jobID: String, recordError: Bool) {
        guard var job = state.jobs[jobID] else {
            if recordError {
                state.lastErrors.append("Unknown job \(jobID)")
            }
            return
        }
        guard job.status == .pending || job.status == .running else {
            return
        }

        job.status = .cancelled
        job.progress = 1.0
        job.cancellationRequested = true
        job.lastEvent = "cancelled"
        job.logLines.append("Cancellation requested.")
        state.jobs[jobID] = job
        if state.activeJobIDsByTab[job.tabID] == jobID {
            state.activeJobIDsByTab.removeValue(forKey: job.tabID)
        }

        switch job.kind {
        case .measurementSetPlot:
            if let datasetID = datasetIDForExplorerTabID(job.tabID),
               var plotState = state.measurementSetPlots[datasetID] {
                plotState.status = .idle
                plotState.lastError = "Cancelled"
                state.measurementSetPlots[datasetID] = plotState
            }
        case .dirtyImagingTask:
            activeTaskExecutions[jobID]?.cancel()
            activeTaskExecutions.removeValue(forKey: jobID)
            if state.taskRun.runID == jobID {
                state.taskRun.state = .cancelled
                state.taskRun.progress = 1.0
                state.taskRun.logLines.append("Cancellation requested for dirty imaging task.")
            }
        }
    }

    private func nextJobID(prefix: String) -> String {
        "\(prefix)-\(state.jobs.count + 1)"
    }

    private func datasetIDForExplorerTabID(_ tabID: String) -> String? {
        let prefix = "tab-explorer-"
        guard tabID.hasPrefix(prefix) else { return nil }
        return String(tabID.dropFirst(prefix.count))
    }

    private func activeTaskTabID(parameters: DirtyImagingTaskParameters) -> String {
        if let activeTab = state.tabs.first(where: { $0.id == state.activeTabID && $0.kind == .task }) {
            return activeTab.id
        }
        if !parameters.datasetID.isEmpty {
            return "tab-dirty-imaging-\(parameters.datasetID)"
        }
        return "tab-dirty-imaging-unbound"
    }

    private func finishMeasurementSetPlotJob(
        jobID: String,
        dataset: DatasetSummary,
        plotState requestedPlotState: MeasurementSetExplorerPlotState,
        result: MeasurementSetPlotResultSummary
    ) {
        guard var job = state.jobs[jobID], job.status != .cancelled else {
            return
        }
        guard state.activeJobIDsByTab[job.tabID] == jobID else {
            return
        }

        cacheMeasurementSetPlotResult(result, for: dataset, plotState: requestedPlotState)
        var currentPlotState = measurementSetPlotState(for: dataset.id)
        refreshMeasurementSetPlotStateFromCache(&currentPlotState, datasetID: dataset.id)
        state.measurementSetPlots[dataset.id] = currentPlotState

        job.status = .succeeded
        job.progress = 1.0
        job.resultSummary = result.summary
        job.lastEvent = "succeeded"
        job.logLines.append("Rendered \(result.renderedPointCount) points.")
        state.jobs[jobID] = job
        state.activeJobIDsByTab.removeValue(forKey: job.tabID)
    }

    private func failMeasurementSetPlotJob(
        jobID: String,
        datasetID: String,
        datasetName: String,
        error: String
    ) {
        guard var job = state.jobs[jobID], job.status != .cancelled else {
            return
        }
        guard state.activeJobIDsByTab[job.tabID] == jobID else {
            return
        }

        var plotState = measurementSetPlotState(for: datasetID)
        plotState.status = .failed
        plotState.lastError = error
        state.measurementSetPlots[datasetID] = plotState

        job.status = .failed
        job.progress = 1.0
        job.error = error
        job.lastEvent = "failed"
        job.logLines.append(error)
        state.jobs[jobID] = job
        state.activeJobIDsByTab.removeValue(forKey: job.tabID)
        state.lastErrors.append("Render plot for \(datasetName): \(error)")
    }

    private func openExplorer(for dataset: DatasetSummary) {
        if dataset.kind == .measurementSet && !state.isDemoProject {
            _ = measurementSetPlotState(for: dataset.id)
        }
        openTab(
            WorkbenchTab(
                id: dataset.explorerTabID,
                title: dataset.explorerTabTitle,
                kind: .datasetExplorer,
                datasetID: dataset.id
            )
        )
    }

    private func measurementSetPlotState(for datasetID: String) -> MeasurementSetExplorerPlotState {
        if let plotState = state.measurementSetPlots[datasetID] {
            return plotState
        }
        guard let dataset = state.project.datasets.first(where: { $0.id == datasetID }) else {
            return MeasurementSetExplorerPlotState(
                datasetID: datasetID,
                preset: .amplitudeVsFrequency,
                selectedField: nil,
                selectedSpectralWindow: nil,
                selectedCorrelation: nil,
                dataColumn: "DATA",
                status: .idle,
                lastError: nil,
                result: nil
            )
        }
        let plotState = MeasurementSetExplorerPlotState.defaultState(for: dataset)
        state.measurementSetPlots[datasetID] = plotState
        return plotState
    }

    private func refreshMeasurementSetPlotStateFromCache(
        _ plotState: inout MeasurementSetExplorerPlotState,
        datasetID: String
    ) {
        guard let dataset = state.project.datasets.first(where: { $0.id == datasetID }),
              let cached = cachedMeasurementSetPlotResult(for: dataset, plotState: plotState)
        else {
            plotState.status = .idle
            plotState.result = nil
            return
        }

        plotState.status = .ready
        plotState.result = cached
    }

    private func cachedMeasurementSetPlotResult(
        for dataset: DatasetSummary,
        plotState: MeasurementSetExplorerPlotState
    ) -> MeasurementSetPlotResultSummary? {
        guard let result = state.measurementSetPlotResultCache[
            measurementSetPlotCacheKey(for: dataset, plotState: plotState)
        ] else {
            return nil
        }
        return result.matches(plotState: plotState) ? result : nil
    }

    private func cacheMeasurementSetPlotResult(
        _ result: MeasurementSetPlotResultSummary,
        for dataset: DatasetSummary,
        plotState: MeasurementSetExplorerPlotState
    ) {
        state.measurementSetPlotResultCache[
            measurementSetPlotCacheKey(for: dataset, plotState: plotState)
        ] = result
    }

    private func measurementSetPlotCacheKey(
        for dataset: DatasetSummary,
        plotState: MeasurementSetExplorerPlotState
    ) -> String {
        [
            "ms-plot",
            dataset.id,
            dataset.path,
            "bytes:\(dataset.sizeBytes)",
            "modified:\(dataset.modifiedUnixSeconds.map(String.init) ?? "unknown")",
            "preset:\(plotState.preset.rawValue)",
            "field:\(plotState.selectedField ?? "all")",
            "spw:\(plotState.selectedSpectralWindow ?? "all")",
            "corr:\(plotState.selectedCorrelation ?? "all")",
            "data:\(plotState.dataColumn)",
            "size:960x600",
            "maxPoints:250000"
        ].joined(separator: "|")
    }

    private func defaultDirtyImagingParameters(for dataset: DatasetSummary) -> DirtyImagingTaskParameters {
        let firstField = dataset.fields.first
        let outputPrefix = defaultDirtyImagingOutputPrefix(for: dataset)
        return DirtyImagingTaskParameters(
            datasetID: dataset.id,
            measurementSetPath: dataset.path,
            outputPrefix: outputPrefix,
            selectedField: firstField,
            phaseCenterField: firstField,
            selectedSpectralWindow: dataset.spectralWindows.first,
            dataColumn: dataset.dataColumns.first ?? "DATA"
        )
    }

    private func blankDirtyImagingParameters() -> DirtyImagingTaskParameters {
        DirtyImagingTaskParameters(
            datasetID: "",
            measurementSetPath: "",
            outputPrefix: defaultDirtyImagingOutputPrefix(baseName: "dirty-image"),
            selectedField: nil,
            phaseCenterField: nil,
            selectedSpectralWindow: nil,
            dataColumn: "DATA",
            runReason: "Initial dirty image from selected MeasurementSet."
        )
    }

    private func defaultDirtyImagingOutputPrefix(for dataset: DatasetSummary) -> String {
        defaultDirtyImagingOutputPrefix(baseName: dataset.name)
    }

    private func defaultDirtyImagingOutputPrefix(baseName: String) -> String {
        let root = state.project.rootPath.isEmpty ? FileManager.default.currentDirectoryPath : state.project.rootPath
        let runDirectory = URL(fileURLWithPath: root)
            .appendingPathComponent("casa-rs-runs", isDirectory: true)
            .appendingPathComponent("dirty-imaging-\(nextDirtyImagingRunIndex())", isDirectory: true)
        return runDirectory.appendingPathComponent("\(sanitizedPathComponent(baseName))-dirty").path
    }

    private func nextDirtyImagingRunIndex() -> Int {
        state.history.filter { $0.title.hasPrefix("Dirty imaging") }.count + 1
    }

    private func sanitizedPathComponent(_ value: String) -> String {
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-_."))
        let scalars = value.unicodeScalars.map { allowed.contains($0) ? Character($0) : "-" }
        let sanitized = String(scalars).trimmingCharacters(in: CharacterSet(charactersIn: "-."))
        return sanitized.isEmpty ? "dataset" : sanitized
    }

    private func updateDirtyImagingParameters(_ parameters: DirtyImagingTaskParameters) {
        state.dirtyImagingTaskParameters = parameters
        if state.taskRun.state == .idle || state.taskRun.state == .failed {
            state.taskRun.requestSummary = parameters.requestSummary
        }
    }

    private func failDirtyImagingJob(runID: String, message: String, diagnostics: [String]) {
        activeTaskExecutions.removeValue(forKey: runID)
        if var job = state.jobs[runID], job.status != .cancelled {
            job.status = .failed
            job.progress = 1.0
            job.error = message
            job.lastEvent = "failed"
            job.logLines.append(message)
            job.logLines.append(contentsOf: diagnostics)
            state.jobs[runID] = job
            if state.activeJobIDsByTab[job.tabID] == runID {
                state.activeJobIDsByTab.removeValue(forKey: job.tabID)
            }
        }

        if state.taskRun.runID == runID {
            state.taskRun = TaskRun(
                runID: runID,
                state: .failed,
                progress: 1.0,
                logLines: ["casars-imager dirty imaging failed.", message],
                warnings: [],
                products: [],
                diagnostics: diagnostics,
                requestSummary: state.dirtyImagingTaskParameters?.requestSummary
            )
        }
    }

    private func finishDirtyImagingJob(runID: String, result: DirtyImagingTaskResult) {
        activeTaskExecutions.removeValue(forKey: runID)
        if var job = state.jobs[runID], job.status != .cancelled {
            job.status = .succeeded
            job.progress = 1.0
            job.resultSummary = result.report.summary
            job.lastEvent = "succeeded"
            job.logLines.append(result.report.summary)
            job.logLines.append("Protocol: \(result.protocolSummary)")
            state.jobs[runID] = job
            if state.activeJobIDsByTab[job.tabID] == runID {
                state.activeJobIDsByTab.removeValue(forKey: job.tabID)
            }
        }
    }

    private func cancelDirtyImagingJob(runID: String, failure: DirtyImagingTaskFailure) {
        activeTaskExecutions.removeValue(forKey: runID)
        if var job = state.jobs[runID] {
            job.status = .cancelled
            job.progress = 1.0
            job.cancellationRequested = true
            job.error = failure.message
            job.lastEvent = "cancelled"
            job.logLines.append(failure.message)
            state.jobs[runID] = job
            if state.activeJobIDsByTab[job.tabID] == runID {
                state.activeJobIDsByTab.removeValue(forKey: job.tabID)
            }
        }
    }

    private func handleDirtyImagingEvent(_ event: DirtyImagingTaskEvent, runID: String, jobID: String) {
        guard Thread.isMainThread else {
            DispatchQueue.main.async { [weak self] in
                self?.handleDirtyImagingEvent(event, runID: runID, jobID: jobID)
            }
            return
        }

        guard state.jobs[jobID]?.status != .cancelled else {
            return
        }

        switch event {
        case .succeeded(let result):
            finishDirtyImagingJob(runID: jobID, result: result)
            if state.taskRun.runID == runID {
                state.taskRun = TaskRun(
                    runID: runID,
                    state: .succeeded,
                    progress: 1.0,
                    logLines: [
                        "casars-imager completed dirty imaging.",
                        result.report.summary,
                        "Protocol: \(result.protocolSummary)"
                    ],
                    warnings: result.report.warnings,
                    products: result.artifacts.map(\.path),
                    diagnostics: result.diagnostics,
                    outputPaths: result.outputPaths,
                    requestSummary: result.request.parameters.requestSummary
                )
            }
            appendProducedDatasets(from: result)
            state.history.append(
                ProcessingHistoryEvent(
                    id: "hist-run-\(state.history.count + 1)",
                    timestamp: currentTimestamp(),
                    title: "Dirty imaging completed",
                    reason: result.request.parameters.runReason,
                    affectedPaths: result.outputPaths,
                    approval: "user"
                )
            )

        case .failed(let failure):
            failDirtyImagingJob(runID: jobID, message: failure.message, diagnostics: failure.diagnostics)
            if state.taskRun.runID == runID {
                state.taskRun.outputPaths = [failure.requestJSONPath, failure.stdoutPath, failure.stderrPath].compactMap { $0 }
            }
            state.lastErrors.append("Dirty imaging failed: \(failure.message)")

        case .cancelled(let failure):
            cancelDirtyImagingJob(runID: jobID, failure: failure)
            if state.taskRun.runID == runID && state.taskRun.state != .cancelled {
                state.taskRun.state = .cancelled
                state.taskRun.progress = 1.0
                state.taskRun.logLines.append(failure.message)
                state.taskRun.outputPaths = [failure.requestJSONPath, failure.stdoutPath, failure.stderrPath].compactMap { $0 }
                state.history.append(
                    ProcessingHistoryEvent(
                        id: "hist-run-\(state.history.count + 1)",
                        timestamp: currentTimestamp(),
                        title: "Dirty imaging cancelled",
                        reason: state.dirtyImagingTaskParameters?.runReason ?? "User cancelled the dirty imaging run.",
                        affectedPaths: state.taskRun.outputPaths,
                        approval: "user"
                    )
                )
            }
        }
    }

    private func appendProducedDatasets(from result: DirtyImagingTaskResult) {
        for artifact in result.artifacts where artifact.exists {
            guard !state.project.datasets.contains(where: { $0.path == artifact.path }) else {
                continue
            }
            if let probed = try? probeClient.probePath(path: artifact.path) {
                state.project.datasets.append(probed)
                continue
            }
            state.project.datasets.append(
                DatasetSummary(
                    id: artifact.path,
                    name: URL(fileURLWithPath: artifact.path).lastPathComponent,
                    path: artifact.path,
                    kind: .imageCube,
                    size: "Unprobed image product",
                    units: "CASA image",
                    notes: "Produced by \(result.request.runID) from \(result.request.parameters.measurementSetPath).",
                    diagnostics: artifact.previewPngExists
                        ? ["preview: \(artifact.previewPngPath ?? "")"]
                        : []
                )
            )
        }
    }

    private func currentTimestamp() -> String {
        ISO8601DateFormatter().string(from: Date())
    }

    private func normalizedPickerValue(_ value: String?) -> String? {
        guard let value = value?.trimmingCharacters(in: .whitespacesAndNewlines), !value.isEmpty, value != "all" else {
            return nil
        }
        return value
    }

    private func selectorToken(_ value: String?) -> String? {
        guard let value = normalizedPickerValue(value) else {
            return nil
        }
        if value.hasPrefix("spw ") {
            let remainder = value.dropFirst(4)
            return String(remainder.prefix { $0.isNumber })
        }
        if let colon = value.firstIndex(of: ":") {
            return String(value[..<colon]).trimmingCharacters(in: .whitespacesAndNewlines)
        }
        return value
    }
}

extension ProjectFixture {
    init(probe: CasarsFrontendServices.ProjectProbe) {
        self.init(
            name: probe.name,
            rootPath: probe.rootPath,
            datasets: probe.datasets.map(DatasetSummary.init(probe:)),
            source: .probed
        )
    }
}

extension DatasetSummary {
    init(probe: CasarsFrontendServices.DatasetProbe) {
        self.init(
            id: probe.id,
            name: probe.name,
            path: probe.path,
            kind: DatasetKind(probeKind: probe.kind),
            size: probe.logicalSize,
            units: probe.units,
            sizeBytes: probe.sizeBytes,
            modifiedUnixSeconds: probe.modifiedUnixSeconds,
            probedUnixSeconds: probe.probedUnixSeconds,
            fields: probe.fields,
            spectralWindows: probe.spectralWindows,
            scans: probe.scans,
            antennas: probe.antennas,
            correlations: probe.correlations,
            columns: probe.columns,
            dataColumns: probe.dataColumns,
            subtables: probe.subtables,
            shape: probe.shape,
            notes: probe.notes,
            diagnostics: probe.diagnostics
        )
    }
}

extension DatasetKind {
    init(probeKind: CasarsFrontendServices.DatasetKind) {
        switch probeKind {
        case .measurementSet:
            self = .measurementSet
        case .image:
            self = .imageCube
        case .table:
            self = .table
        }
    }
}

extension CasarsFrontendServices.MeasurementSetPlotPreset {
    init(preset: MeasurementSetExplorerPlotPreset) {
        switch preset {
        case .uvCoverage:
            self = .uvCoverage
        case .amplitudeVsFrequency:
            self = .amplitudeVsFrequency
        case .amplitudeVsChannel:
            self = .amplitudeVsChannel
        case .amplitudeVsUvDistance:
            self = .amplitudeVsUvDistance
        case .amplitudeVsTime:
            self = .amplitudeVsTime
        }
    }
}

extension MeasurementSetExplorerPlotPreset {
    init(preset: CasarsFrontendServices.MeasurementSetPlotPreset) {
        switch preset {
        case .uvCoverage:
            self = .uvCoverage
        case .amplitudeVsFrequency:
            self = .amplitudeVsFrequency
        case .amplitudeVsChannel:
            self = .amplitudeVsChannel
        case .amplitudeVsUvDistance:
            self = .amplitudeVsUvDistance
        case .amplitudeVsTime:
            self = .amplitudeVsTime
        }
    }
}

extension PlotAxisSummary {
    init(axis: CasarsFrontendServices.PlotAxisMetadata) {
        self.init(id: axis.id, label: axis.label, unit: axis.unit)
    }
}

extension PlotSeriesSummary {
    init(series: CasarsFrontendServices.PlotSeriesMetadata) {
        self.init(
            label: series.label,
            colorGroup: series.colorGroup,
            pointCount: series.pointCount,
            firstRow: series.firstRow,
            lastRow: series.lastRow
        )
    }
}

extension MeasurementSetPlotResultSummary {
    init(result: CasarsFrontendServices.MeasurementSetPlotResult) {
        self.init(
            preset: MeasurementSetExplorerPlotPreset(preset: result.preset),
            presetLabel: result.presetLabel,
            title: result.title,
            summary: result.summary,
            datasetPath: result.datasetPath,
            dataColumn: result.dataColumn,
            selectionSummary: result.selectionSummary,
            xAxis: PlotAxisSummary(axis: result.xAxis),
            yAxis: PlotAxisSummary(axis: result.yAxis),
            series: result.series.map(PlotSeriesSummary.init(series:)),
            requestedMaxPoints: result.sampling.requestedMaxPoints,
            renderedPointCount: result.sampling.renderedPointCount,
            diagnostics: result.sampling.diagnostics,
            renderer: result.render.renderer,
            imageFormat: result.render.imageFormat,
            imageWidth: result.render.width,
            imageHeight: result.render.height,
            imageBytes: result.imageBytes
        )
    }
}
