import Foundation
import CasarsFrontendServices

public protocol ProjectProbeClient {
    func probeProject(path: String) throws -> ProjectFixtureProbe
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

    public init(
        state: WorkbenchState = EmptyWorkbench.makeState(),
        probeClient: ProjectProbeClient = UniFFIProjectProbeClient(),
        plotClient: MeasurementSetPlotClient = UniFFIMeasurementSetPlotClient()
    ) {
        self.state = state
        self.probeClient = probeClient
        self.plotClient = plotClient
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
        guard state.project.datasets.contains(where: { $0.id == datasetID }) else {
            state.lastErrors.append("Unknown dataset \(datasetID)")
            return
        }

        state.selectedDatasetID = datasetID
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
        plotState.status = .idle
        plotState.lastError = nil
        state.measurementSetPlots[datasetID] = plotState
    }

    public func setMeasurementSetPlotField(_ field: String?, datasetID: String) {
        var plotState = measurementSetPlotState(for: datasetID)
        plotState.selectedField = normalizedPickerValue(field)
        plotState.status = .idle
        plotState.lastError = nil
        state.measurementSetPlots[datasetID] = plotState
    }

    public func setMeasurementSetPlotSpectralWindow(_ spectralWindow: String?, datasetID: String) {
        var plotState = measurementSetPlotState(for: datasetID)
        plotState.selectedSpectralWindow = normalizedPickerValue(spectralWindow)
        plotState.status = .idle
        plotState.lastError = nil
        state.measurementSetPlots[datasetID] = plotState
    }

    public func setMeasurementSetPlotCorrelation(_ correlation: String?, datasetID: String) {
        var plotState = measurementSetPlotState(for: datasetID)
        plotState.selectedCorrelation = normalizedPickerValue(correlation)
        plotState.status = .idle
        plotState.lastError = nil
        state.measurementSetPlots[datasetID] = plotState
    }

    public func setMeasurementSetPlotDataColumn(_ dataColumn: String, datasetID: String) {
        var plotState = measurementSetPlotState(for: datasetID)
        plotState.dataColumn = dataColumn
        plotState.status = .idle
        plotState.lastError = nil
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
        plotState.status = .running
        plotState.lastError = nil
        state.measurementSetPlots[datasetID] = plotState

        do {
            let result = try plotClient.buildPlot(
                request: MeasurementSetPlotBuildRequest(
                    datasetPath: dataset.path,
                    preset: plotState.preset,
                    field: selectorToken(plotState.selectedField),
                    spectralWindow: selectorToken(plotState.selectedSpectralWindow),
                    correlation: selectorToken(plotState.selectedCorrelation),
                    dataColumn: plotState.dataColumn
                )
            )
            plotState.result = result
            plotState.status = .ready
            state.measurementSetPlots[datasetID] = plotState
        } catch {
            plotState.status = .failed
            plotState.lastError = "\(error)"
            state.measurementSetPlots[datasetID] = plotState
            state.lastErrors.append("Render plot for \(dataset.name): \(error)")
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
        } else if normalized.contains("task") || normalized.contains("calibrate") {
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
            guard state.isDemoProject else {
                state.lastErrors.append("Task panels are not connected for real projects yet")
                return
            }
            openTab(WorkbenchTab(id: "tab-task", title: "Calibrate", kind: .task, datasetID: state.selectedDatasetID))
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
        guard state.isDemoProject else {
            state.lastErrors.append("Task execution is not connected yet")
            return
        }
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
                timestamp: "2026-05-04 09:24",
                title: "Fixture task completed",
                reason: "User ran the dry-run task from the task tab.",
                affectedPaths: state.taskRun.products,
                approval: "user"
            )
        )
    }

    public func stopTask() {
        guard state.isDemoProject else {
            state.lastErrors.append("Task execution is not connected yet")
            return
        }
        state.taskRun.state = .stopped
        state.taskRun.logLines.append("Stopped fixture task.")
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
