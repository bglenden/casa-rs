import Foundation

public enum DockMode: String, CaseIterable, Codable, Equatable, Identifiable {
    case datasets
    case files
    case history

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .datasets: "Datasets"
        case .files: "Files"
        case .history: "History"
        }
    }

    public var systemImage: String {
        switch self {
        case .datasets: "externaldrive"
        case .files: "folder"
        case .history: "clock.arrow.circlepath"
        }
    }
}

public enum DatasetKind: String, Codable, Equatable {
    case measurementSet
    case imageCube
    case calibrationTable
    case table
    case runProduct

    public var explorerName: String {
        switch self {
        case .measurementSet:
            "MeasurementSet Explorer"
        case .imageCube:
            "Image Explorer"
        case .calibrationTable:
            "Calibration Table Explorer"
        case .table:
            "Table Explorer"
        case .runProduct:
            "Run Product Explorer"
        }
    }

    public var explorerTabPrefix: String {
        switch self {
        case .measurementSet:
            "MS"
        case .imageCube:
            "Image"
        case .calibrationTable:
            "Cal"
        case .table:
            "Table"
        case .runProduct:
            "Product"
        }
    }
}

public struct DatasetSummary: Identifiable, Codable, Equatable {
    public let id: String
    public var name: String
    public var path: String
    public var kind: DatasetKind
    public var size: String
    public var units: String
    public var sizeBytes: UInt64
    public var modifiedUnixSeconds: UInt64?
    public var probedUnixSeconds: UInt64?
    public var fields: [String]
    public var spectralWindows: [String]
    public var scans: [String]
    public var antennas: [String]
    public var correlations: [String]
    public var columns: [String]
    public var dataColumns: [String]
    public var subtables: [String]
    public var shape: [UInt64]
    public var notes: String
    public var diagnostics: [String]

    public init(
        id: String,
        name: String,
        path: String,
        kind: DatasetKind,
        size: String,
        units: String,
        sizeBytes: UInt64 = 0,
        modifiedUnixSeconds: UInt64? = nil,
        probedUnixSeconds: UInt64? = nil,
        fields: [String] = [],
        spectralWindows: [String] = [],
        scans: [String] = [],
        antennas: [String] = [],
        correlations: [String] = [],
        columns: [String] = [],
        dataColumns: [String] = [],
        subtables: [String] = [],
        shape: [UInt64] = [],
        notes: String,
        diagnostics: [String] = []
    ) {
        self.id = id
        self.name = name
        self.path = path
        self.kind = kind
        self.size = size
        self.units = units
        self.sizeBytes = sizeBytes
        self.modifiedUnixSeconds = modifiedUnixSeconds
        self.probedUnixSeconds = probedUnixSeconds
        self.fields = fields
        self.spectralWindows = spectralWindows
        self.scans = scans
        self.antennas = antennas
        self.correlations = correlations
        self.columns = columns
        self.dataColumns = dataColumns
        self.subtables = subtables
        self.shape = shape
        self.notes = notes
        self.diagnostics = diagnostics
    }

    public var explorerTabID: String {
        "tab-explorer-\(id)"
    }

    public var explorerTabTitle: String {
        "\(kind.explorerTabPrefix): \(name)"
    }
}

public struct ProjectFixture: Codable, Equatable {
    public var name: String
    public var rootPath: String
    public var datasets: [DatasetSummary]
    public var source: ProjectSource

    public init(
        name: String,
        rootPath: String,
        datasets: [DatasetSummary],
        source: ProjectSource = .fixture
    ) {
        self.name = name
        self.rootPath = rootPath
        self.datasets = datasets
        self.source = source
    }
}

public enum ProjectSource: String, Codable, Equatable {
    case none
    case fixture
    case probed
}

public extension ProjectSource {
    var isDemo: Bool {
        self == .fixture
    }
}

public enum WorkbenchTabKind: String, Codable, Equatable {
    case datasetExplorer
    case task
    case aiChat
    case python
    case history
}

public struct WorkbenchTab: Identifiable, Codable, Equatable {
    public let id: String
    public var title: String
    public var kind: WorkbenchTabKind
    public var datasetID: String?

    public init(id: String, title: String, kind: WorkbenchTabKind, datasetID: String? = nil) {
        self.id = id
        self.title = title
        self.kind = kind
        self.datasetID = datasetID
    }
}

public enum TaskRunState: String, Codable, Equatable {
    case idle
    case running
    case completed
    case succeeded
    case failed
    case stopped
    case cancelled
}

public struct TaskParameters: Codable, Equatable {
    public var taskName: String
    public var selectedField: String
    public var selectedSpectralWindow: String
    public var outputName: String
    public var dryRun: Bool

    public init(
        taskName: String,
        selectedField: String,
        selectedSpectralWindow: String,
        outputName: String,
        dryRun: Bool
    ) {
        self.taskName = taskName
        self.selectedField = selectedField
        self.selectedSpectralWindow = selectedSpectralWindow
        self.outputName = outputName
        self.dryRun = dryRun
    }
}

public struct TaskRun: Codable, Equatable {
    public var runID: String?
    public var state: TaskRunState
    public var progress: Double
    public var logLines: [String]
    public var warnings: [String]
    public var products: [String]
    public var diagnostics: [String]
    public var outputPaths: [String]
    public var requestSummary: String?

    public init(
        runID: String? = nil,
        state: TaskRunState,
        progress: Double,
        logLines: [String],
        warnings: [String],
        products: [String],
        diagnostics: [String] = [],
        outputPaths: [String] = [],
        requestSummary: String? = nil
    ) {
        self.runID = runID
        self.state = state
        self.progress = progress
        self.logLines = logLines
        self.warnings = warnings
        self.products = products
        self.diagnostics = diagnostics
        self.outputPaths = outputPaths
        self.requestSummary = requestSummary
    }
}

public enum AIProposalState: String, Codable, Equatable {
    case pending
    case applied
    case rejected
}

public struct AIProposal: Identifiable, Codable, Equatable {
    public let id: String
    public var title: String
    public var detail: String
    public var parameterName: String
    public var oldValue: String
    public var newValue: String
    public var state: AIProposalState

    public init(
        id: String,
        title: String,
        detail: String,
        parameterName: String,
        oldValue: String,
        newValue: String,
        state: AIProposalState
    ) {
        self.id = id
        self.title = title
        self.detail = detail
        self.parameterName = parameterName
        self.oldValue = oldValue
        self.newValue = newValue
        self.state = state
    }
}

public enum ChatAuthor: String, Codable, Equatable {
    case user
    case assistant
    case system
}

public struct AIChatMessage: Identifiable, Codable, Equatable {
    public let id: String
    public var author: ChatAuthor
    public var text: String

    public init(id: String, author: ChatAuthor, text: String) {
        self.id = id
        self.author = author
        self.text = text
    }
}

public enum PythonOwner: String, Codable, Equatable {
    case user
    case ai
}

public struct PythonPanelState: Codable, Equatable {
    public var owner: PythonOwner
    public var buffer: String
    public var capturedPlots: [String]

    public init(owner: PythonOwner, buffer: String, capturedPlots: [String]) {
        self.owner = owner
        self.buffer = buffer
        self.capturedPlots = capturedPlots
    }
}

public enum MeasurementSetExplorerPlotPreset: String, CaseIterable, Codable, Equatable, Identifiable {
    case uvCoverage
    case amplitudeVsFrequency
    case amplitudeVsChannel
    case amplitudeVsUvDistance
    case amplitudeVsTime

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .uvCoverage:
            "UV Coverage"
        case .amplitudeVsFrequency:
            "Amplitude vs Frequency"
        case .amplitudeVsChannel:
            "Amplitude vs Channel"
        case .amplitudeVsUvDistance:
            "Amplitude vs UV Distance"
        case .amplitudeVsTime:
            "Amplitude vs Time"
        }
    }
}

public enum MeasurementSetPlotStatus: String, Codable, Equatable {
    case idle
    case running
    case ready
    case failed
}

public struct PlotAxisSummary: Codable, Equatable {
    public var id: String
    public var label: String
    public var unit: String

    public init(id: String, label: String, unit: String) {
        self.id = id
        self.label = label
        self.unit = unit
    }
}

public struct PlotSeriesSummary: Codable, Equatable {
    public var label: String
    public var colorGroup: String
    public var pointCount: UInt64
    public var firstRow: UInt64?
    public var lastRow: UInt64?

    public init(label: String, colorGroup: String, pointCount: UInt64, firstRow: UInt64?, lastRow: UInt64?) {
        self.label = label
        self.colorGroup = colorGroup
        self.pointCount = pointCount
        self.firstRow = firstRow
        self.lastRow = lastRow
    }
}

public struct MeasurementSetPlotResultSummary: Codable, Equatable {
    public var presetLabel: String
    public var title: String
    public var summary: String
    public var datasetPath: String
    public var dataColumn: String
    public var selectionSummary: String
    public var xAxis: PlotAxisSummary
    public var yAxis: PlotAxisSummary
    public var series: [PlotSeriesSummary]
    public var requestedMaxPoints: UInt64
    public var renderedPointCount: UInt64
    public var diagnostics: [String]
    public var renderer: String
    public var imageFormat: String
    public var imageWidth: UInt32
    public var imageHeight: UInt32
    public var imageBytes: Data

    public init(
        presetLabel: String,
        title: String,
        summary: String,
        datasetPath: String,
        dataColumn: String,
        selectionSummary: String,
        xAxis: PlotAxisSummary,
        yAxis: PlotAxisSummary,
        series: [PlotSeriesSummary],
        requestedMaxPoints: UInt64,
        renderedPointCount: UInt64,
        diagnostics: [String],
        renderer: String,
        imageFormat: String,
        imageWidth: UInt32,
        imageHeight: UInt32,
        imageBytes: Data
    ) {
        self.presetLabel = presetLabel
        self.title = title
        self.summary = summary
        self.datasetPath = datasetPath
        self.dataColumn = dataColumn
        self.selectionSummary = selectionSummary
        self.xAxis = xAxis
        self.yAxis = yAxis
        self.series = series
        self.requestedMaxPoints = requestedMaxPoints
        self.renderedPointCount = renderedPointCount
        self.diagnostics = diagnostics
        self.renderer = renderer
        self.imageFormat = imageFormat
        self.imageWidth = imageWidth
        self.imageHeight = imageHeight
        self.imageBytes = imageBytes
    }
}

public struct MeasurementSetExplorerPlotState: Codable, Equatable {
    public var datasetID: String
    public var preset: MeasurementSetExplorerPlotPreset
    public var selectedField: String?
    public var selectedSpectralWindow: String?
    public var selectedCorrelation: String?
    public var dataColumn: String
    public var status: MeasurementSetPlotStatus
    public var lastError: String?
    public var result: MeasurementSetPlotResultSummary?

    public init(
        datasetID: String,
        preset: MeasurementSetExplorerPlotPreset,
        selectedField: String?,
        selectedSpectralWindow: String?,
        selectedCorrelation: String?,
        dataColumn: String,
        status: MeasurementSetPlotStatus,
        lastError: String?,
        result: MeasurementSetPlotResultSummary?
    ) {
        self.datasetID = datasetID
        self.preset = preset
        self.selectedField = selectedField
        self.selectedSpectralWindow = selectedSpectralWindow
        self.selectedCorrelation = selectedCorrelation
        self.dataColumn = dataColumn
        self.status = status
        self.lastError = lastError
        self.result = result
    }

    public static func defaultState(for dataset: DatasetSummary) -> MeasurementSetExplorerPlotState {
        MeasurementSetExplorerPlotState(
            datasetID: dataset.id,
            preset: .uvCoverage,
            selectedField: nil,
            selectedSpectralWindow: nil,
            selectedCorrelation: nil,
            dataColumn: dataset.dataColumns.first ?? "DATA",
            status: .idle,
            lastError: nil,
            result: nil
        )
    }
}

public struct ProcessingHistoryEvent: Identifiable, Codable, Equatable {
    public let id: String
    public var timestamp: String
    public var title: String
    public var reason: String
    public var affectedPaths: [String]
    public var approval: String

    public init(
        id: String,
        timestamp: String,
        title: String,
        reason: String,
        affectedPaths: [String],
        approval: String
    ) {
        self.id = id
        self.timestamp = timestamp
        self.title = title
        self.reason = reason
        self.affectedPaths = affectedPaths
        self.approval = approval
    }
}

public struct WorkbenchState: Codable, Equatable {
    public static let defaultInterfaceFontSize = 13.0
    public static let minimumInterfaceFontSize = 10.0
    public static let maximumInterfaceFontSize = 22.0

    public var project: ProjectFixture
    public var dockMode: DockMode
    public var leftDockCollapsed: Bool
    public var selectedDatasetID: String?
    public var inspectorCollapsed: Bool
    public var tabs: [WorkbenchTab]
    public var activeTabID: String
    public var taskParameters: TaskParameters
    public var dirtyImagingTaskParameters: DirtyImagingTaskParameters?
    public var taskRun: TaskRun
    public var aiMessages: [AIChatMessage]
    public var aiProposals: [AIProposal]
    public var python: PythonPanelState
    public var measurementSetPlots: [String: MeasurementSetExplorerPlotState]
    public var history: [ProcessingHistoryEvent]
    public var commandQuery: String
    public var lastErrors: [String]
    public var probeDiagnostics: [String]
    public var interfaceFontSize: Double

    public init(
        project: ProjectFixture,
        dockMode: DockMode,
        leftDockCollapsed: Bool,
        selectedDatasetID: String?,
        inspectorCollapsed: Bool,
        tabs: [WorkbenchTab],
        activeTabID: String,
        taskParameters: TaskParameters,
        dirtyImagingTaskParameters: DirtyImagingTaskParameters? = nil,
        taskRun: TaskRun,
        aiMessages: [AIChatMessage],
        aiProposals: [AIProposal],
        python: PythonPanelState,
        measurementSetPlots: [String: MeasurementSetExplorerPlotState] = [:],
        history: [ProcessingHistoryEvent],
        commandQuery: String,
        lastErrors: [String],
        probeDiagnostics: [String] = [],
        interfaceFontSize: Double = Self.defaultInterfaceFontSize
    ) {
        self.project = project
        self.dockMode = dockMode
        self.leftDockCollapsed = leftDockCollapsed
        self.selectedDatasetID = selectedDatasetID
        self.inspectorCollapsed = inspectorCollapsed
        self.tabs = tabs
        self.activeTabID = activeTabID
        self.taskParameters = taskParameters
        self.dirtyImagingTaskParameters = dirtyImagingTaskParameters
        self.taskRun = taskRun
        self.aiMessages = aiMessages
        self.aiProposals = aiProposals
        self.python = python
        self.measurementSetPlots = measurementSetPlots
        self.history = history
        self.commandQuery = commandQuery
        self.lastErrors = lastErrors
        self.probeDiagnostics = probeDiagnostics
        self.interfaceFontSize = Self.clampedInterfaceFontSize(interfaceFontSize)
    }

    public var selectedDataset: DatasetSummary? {
        project.datasets.first { $0.id == selectedDatasetID }
    }

    public static func clampedInterfaceFontSize(_ value: Double) -> Double {
        min(maximumInterfaceFontSize, max(minimumInterfaceFontSize, value))
    }

    public var hasProject: Bool {
        project.source != .none
    }

    public var isDemoProject: Bool {
        project.source.isDemo
    }
}

public struct DebugDatasetSnapshot: Codable, Equatable {
    public var name: String
    public var path: String
    public var kind: DatasetKind
    public var size: String
    public var units: String
    public var sizeBytes: UInt64
    public var fields: [String]
    public var spectralWindows: [String]
    public var scans: [String]
    public var antennas: [String]
    public var correlations: [String]
    public var columns: [String]
    public var dataColumns: [String]
    public var subtables: [String]
    public var shape: [UInt64]
    public var diagnostics: [String]

    public init(dataset: DatasetSummary) {
        name = dataset.name
        path = dataset.path
        kind = dataset.kind
        size = dataset.size
        units = dataset.units
        sizeBytes = dataset.sizeBytes
        fields = dataset.fields
        spectralWindows = dataset.spectralWindows
        scans = dataset.scans
        antennas = dataset.antennas
        correlations = dataset.correlations
        columns = dataset.columns
        dataColumns = dataset.dataColumns
        subtables = dataset.subtables
        shape = dataset.shape
        diagnostics = dataset.diagnostics
    }
}

public struct DebugStateSnapshot: Codable, Equatable {
    public var activeProject: String
    public var activeLeftDockMode: DockMode
    public var leftDockCollapsed: Bool
    public var selectedDataset: String?
    public var selectedDatasetSummary: DebugDatasetSnapshot?
    public var activeProjectRoot: String
    public var activeProjectSource: ProjectSource
    public var discoveredDatasets: [String]
    public var probeDiagnostics: [String]
    public var inspectorCollapsed: Bool
    public var openTabs: [String]
    public var activeTab: String
    public var taskState: TaskRunState
    public var taskRequest: DirtyImagingTaskParameters?
    public var taskDiagnostics: [String]
    public var taskOutputPaths: [String]
    public var aiProposalStates: [String: AIProposalState]
    public var pythonOwner: PythonOwner
    public var measurementSetPlots: [String: DebugMeasurementSetPlotSnapshot]
    public var processingHistoryEvents: [String]
    public var commandQuery: String
    public var lastErrors: [String]
    public var interfaceFontSize: Double

    public init(state: WorkbenchState) {
        activeProject = state.project.name
        activeProjectRoot = state.project.rootPath
        activeProjectSource = state.project.source
        activeLeftDockMode = state.dockMode
        leftDockCollapsed = state.leftDockCollapsed
        selectedDataset = state.selectedDataset?.name
        selectedDatasetSummary = state.selectedDataset.map(DebugDatasetSnapshot.init(dataset:))
        discoveredDatasets = state.project.datasets.map(\.name)
        probeDiagnostics = state.probeDiagnostics
        inspectorCollapsed = state.inspectorCollapsed
        openTabs = state.tabs.map(\.title)
        activeTab = state.tabs.first { $0.id == state.activeTabID }?.title ?? state.activeTabID
        taskState = state.taskRun.state
        taskRequest = state.dirtyImagingTaskParameters
        taskDiagnostics = state.taskRun.diagnostics
        taskOutputPaths = state.taskRun.outputPaths
        aiProposalStates = Dictionary(
            uniqueKeysWithValues: state.aiProposals.map { ($0.id, $0.state) }
        )
        pythonOwner = state.python.owner
        measurementSetPlots = Dictionary(
            uniqueKeysWithValues: state.measurementSetPlots.map { datasetID, plotState in
                (datasetID, DebugMeasurementSetPlotSnapshot(plotState: plotState))
            }
        )
        processingHistoryEvents = state.history.map(\.title)
        commandQuery = state.commandQuery
        lastErrors = state.lastErrors
        interfaceFontSize = state.interfaceFontSize
    }
}

public struct DebugMeasurementSetPlotSnapshot: Codable, Equatable {
    public var preset: MeasurementSetExplorerPlotPreset
    public var status: MeasurementSetPlotStatus
    public var selectedField: String?
    public var selectedSpectralWindow: String?
    public var selectedCorrelation: String?
    public var dataColumn: String
    public var lastError: String?
    public var title: String?
    public var xAxis: PlotAxisSummary?
    public var yAxis: PlotAxisSummary?
    public var renderedPointCount: UInt64?
    public var seriesCount: Int?
    public var imageByteCount: Int?
    public var renderer: String?
    public var diagnostics: [String]

    public init(plotState: MeasurementSetExplorerPlotState) {
        preset = plotState.preset
        status = plotState.status
        selectedField = plotState.selectedField
        selectedSpectralWindow = plotState.selectedSpectralWindow
        selectedCorrelation = plotState.selectedCorrelation
        dataColumn = plotState.dataColumn
        lastError = plotState.lastError
        title = plotState.result?.title
        xAxis = plotState.result?.xAxis
        yAxis = plotState.result?.yAxis
        renderedPointCount = plotState.result?.renderedPointCount
        seriesCount = plotState.result?.series.count
        imageByteCount = plotState.result?.imageBytes.count
        renderer = plotState.result?.renderer
        diagnostics = plotState.result?.diagnostics ?? []
    }
}
