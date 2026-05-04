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
    public var fields: [String]
    public var spectralWindows: [String]
    public var scans: [String]
    public var notes: String

    public init(
        id: String,
        name: String,
        path: String,
        kind: DatasetKind,
        size: String,
        units: String,
        fields: [String] = [],
        spectralWindows: [String] = [],
        scans: [String] = [],
        notes: String
    ) {
        self.id = id
        self.name = name
        self.path = path
        self.kind = kind
        self.size = size
        self.units = units
        self.fields = fields
        self.spectralWindows = spectralWindows
        self.scans = scans
        self.notes = notes
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
    case fixture
    case probed
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
    case stopped
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
    public var state: TaskRunState
    public var progress: Double
    public var logLines: [String]
    public var warnings: [String]
    public var products: [String]

    public init(
        state: TaskRunState,
        progress: Double,
        logLines: [String],
        warnings: [String],
        products: [String]
    ) {
        self.state = state
        self.progress = progress
        self.logLines = logLines
        self.warnings = warnings
        self.products = products
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
    public var taskRun: TaskRun
    public var aiMessages: [AIChatMessage]
    public var aiProposals: [AIProposal]
    public var python: PythonPanelState
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
        taskRun: TaskRun,
        aiMessages: [AIChatMessage],
        aiProposals: [AIProposal],
        python: PythonPanelState,
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
        self.taskRun = taskRun
        self.aiMessages = aiMessages
        self.aiProposals = aiProposals
        self.python = python
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
}

public struct DebugStateSnapshot: Codable, Equatable {
    public var activeProject: String
    public var activeLeftDockMode: DockMode
    public var leftDockCollapsed: Bool
    public var selectedDataset: String?
    public var activeProjectRoot: String
    public var activeProjectSource: ProjectSource
    public var discoveredDatasets: [String]
    public var probeDiagnostics: [String]
    public var inspectorCollapsed: Bool
    public var openTabs: [String]
    public var activeTab: String
    public var taskState: TaskRunState
    public var aiProposalStates: [String: AIProposalState]
    public var pythonOwner: PythonOwner
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
        discoveredDatasets = state.project.datasets.map(\.name)
        probeDiagnostics = state.probeDiagnostics
        inspectorCollapsed = state.inspectorCollapsed
        openTabs = state.tabs.map(\.title)
        activeTab = state.tabs.first { $0.id == state.activeTabID }?.title ?? state.activeTabID
        taskState = state.taskRun.state
        aiProposalStates = Dictionary(
            uniqueKeysWithValues: state.aiProposals.map { ($0.id, $0.state) }
        )
        pythonOwner = state.python.owner
        processingHistoryEvents = state.history.map(\.title)
        commandQuery = state.commandQuery
        lastErrors = state.lastErrors
        interfaceFontSize = state.interfaceFontSize
    }
}
