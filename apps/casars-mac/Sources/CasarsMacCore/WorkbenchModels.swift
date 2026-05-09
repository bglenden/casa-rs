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
    public var arrays: [String]
    public var observations: [String]
    public var antennas: [String]
    public var intents: [String]
    public var feeds: [String]
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
        arrays: [String] = [],
        observations: [String] = [],
        antennas: [String] = [],
        intents: [String] = [],
        feeds: [String] = [],
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
        self.arrays = arrays
        self.observations = observations
        self.antennas = antennas
        self.intents = intents
        self.feeds = feeds
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

public struct MeasurementSetUVRangeSummary: Codable, Equatable {
    public var minMeters: Double
    public var maxMeters: Double
    public var minKiloLambda: Double
    public var maxKiloLambda: Double
    public var rowCount: UInt64

    public init(
        minMeters: Double,
        maxMeters: Double,
        minKiloLambda: Double,
        maxKiloLambda: Double,
        rowCount: UInt64
    ) {
        self.minMeters = minMeters
        self.maxMeters = maxMeters
        self.minKiloLambda = minKiloLambda
        self.maxKiloLambda = maxKiloLambda
        self.rowCount = rowCount
    }
}

public struct MeasurementSetTimeRangeSummary: Codable, Equatable {
    public var minSeconds: Double
    public var maxSeconds: Double
    public var rowCount: UInt64

    public init(minSeconds: Double, maxSeconds: Double, rowCount: UInt64) {
        self.minSeconds = minSeconds
        self.maxSeconds = maxSeconds
        self.rowCount = rowCount
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
    case tableBrowser
    case task
    case plotSamples
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

public enum WorkbenchJobKind: String, Codable, Equatable {
    case measurementSetPlot
    case dirtyImagingTask
    case genericTask
}

public enum WorkbenchJobOwner: String, Codable, Equatable {
    case user
    case ai
}

public enum WorkbenchJobStatus: String, Codable, Equatable {
    case pending
    case running
    case succeeded
    case failed
    case cancelled
}

public struct WorkbenchJob: Identifiable, Codable, Equatable {
    public let id: String
    public var tabID: String
    public var kind: WorkbenchJobKind
    public var owner: WorkbenchJobOwner
    public var status: WorkbenchJobStatus
    public var progress: Double
    public var title: String
    public var detail: String
    public var logLines: [String]
    public var resultSummary: String?
    public var error: String?
    public var cancellationRequested: Bool
    public var lastEvent: String

    public init(
        id: String,
        tabID: String,
        kind: WorkbenchJobKind,
        owner: WorkbenchJobOwner,
        status: WorkbenchJobStatus = .pending,
        progress: Double = 0,
        title: String,
        detail: String,
        logLines: [String] = [],
        resultSummary: String? = nil,
        error: String? = nil,
        cancellationRequested: Bool = false,
        lastEvent: String = "created"
    ) {
        self.id = id
        self.tabID = tabID
        self.kind = kind
        self.owner = owner
        self.status = status
        self.progress = progress
        self.title = title
        self.detail = detail
        self.logLines = logLines
        self.resultSummary = resultSummary
        self.error = error
        self.cancellationRequested = cancellationRequested
        self.lastEvent = lastEvent
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

public struct TaskCatalogEnvelope: Codable, Equatable {
    public var schemaVersion: UInt64
    public var tasks: [TaskCatalogEntry]

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case tasks
    }
}

public struct TaskCatalogEntry: Codable, Equatable, Identifiable {
    public var id: String
    public var category: String
    public var displayName: String
    public var binaryName: String
    public var cargoPackage: String
    public var overrideEnv: String
    public var shellKind: String
    public var interaction: String
    public var browserKind: String?
    public var datasetKinds: [String]
    public var schemaSource: String
    public var showInTUI: Bool
    public var showInSwift: Bool
    public var includeInSuite: Bool

    enum CodingKeys: String, CodingKey {
        case id
        case category
        case displayName = "display_name"
        case binaryName = "binary_name"
        case cargoPackage = "cargo_package"
        case overrideEnv = "override_env"
        case shellKind = "shell_kind"
        case interaction
        case browserKind = "browser_kind"
        case datasetKinds = "dataset_kinds"
        case schemaSource = "schema_source"
        case showInTUI = "show_in_tui"
        case showInSwift = "show_in_swift"
        case includeInSuite = "include_in_suite"
    }
}

public struct TaskExecutionMatrixEnvelope: Codable, Equatable {
    public var schemaVersion: UInt64
    public var generatedFor: String
    public var scopeNote: String
    public var rows: [TaskExecutionMatrixRow]

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case generatedFor = "generated_for"
        case scopeNote = "scope_note"
        case rows
    }
}

public struct TaskExecutionMatrixRow: Codable, Equatable, Identifiable {
    public var id: String { taskID }

    public var taskID: String
    public var displayName: String
    public var category: String
    public var catalogPresence: String
    public var binaryName: String
    public var cargoPackage: String
    public var datasetKinds: [String]
    public var suiteInstall: String
    public var localInstall: String
    public var releaseInstall: String
    public var tuiStatus: String
    public var guiStatus: String
    public var optionSource: String
    public var fullControlStatus: String
    public var mutationClass: String
    public var confirmation: String
    public var smokeEvidence: String

    enum CodingKeys: String, CodingKey {
        case taskID = "task_id"
        case displayName = "display_name"
        case category
        case catalogPresence = "catalog_presence"
        case binaryName = "binary_name"
        case cargoPackage = "cargo_package"
        case datasetKinds = "dataset_kinds"
        case suiteInstall = "suite_install"
        case localInstall = "local_install"
        case releaseInstall = "release_install"
        case tuiStatus = "tui_status"
        case guiStatus = "gui_status"
        case optionSource = "option_source"
        case fullControlStatus = "full_control_status"
        case mutationClass = "mutation_class"
        case confirmation
        case smokeEvidence = "smoke_evidence"
    }
}

public struct TaskContextOptionsEnvelope: Codable, Equatable {
    public var schemaVersion: UInt64
    public var datasetPath: String
    public var datasetKind: String
    public var fields: [String]
    public var spectralWindows: [String]
    public var scans: [String]
    public var arrays: [String]
    public var observations: [String]
    public var antennas: [String]
    public var intents: [String]
    public var feeds: [String]
    public var correlations: [String]
    public var columns: [String]
    public var dataColumns: [String]
    public var subtables: [String]
    public var shape: [UInt64]
    public var defaults: [String: String]
    public var diagnostics: [String]

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case datasetPath = "dataset_path"
        case datasetKind = "dataset_kind"
        case fields
        case spectralWindows = "spectral_windows"
        case scans
        case arrays
        case observations
        case antennas
        case intents
        case feeds
        case correlations
        case columns
        case dataColumns = "data_columns"
        case subtables
        case shape
        case defaults
        case diagnostics
    }
}

public struct TaskUISchema: Codable, Equatable {
    public var schemaVersion: UInt64
    public var commandID: String
    public var invocationName: String
    public var displayName: String
    public var category: String
    public var summary: String
    public var usage: String
    public var arguments: [TaskUIArgument]

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case commandID = "command_id"
        case invocationName = "invocation_name"
        case displayName = "display_name"
        case category
        case summary
        case usage
        case arguments
    }
}

public struct TaskUIArgument: Codable, Equatable, Identifiable {
    public var id: String
    public var label: String
    public var order: Int
    public var parser: TaskUIArgumentParser
    public var valueKind: String
    public var required: Bool
    public var `default`: String?
    public var help: String
    public var group: String
    public var advanced: Bool
    public var hiddenInTUI: Bool

    enum CodingKeys: String, CodingKey {
        case id
        case label
        case order
        case parser
        case valueKind = "value_kind"
        case required
        case `default`
        case help
        case group
        case advanced
        case hiddenInTUI = "hidden_in_tui"
    }
}

public struct TaskUIArgumentParser: Codable, Equatable {
    public var kind: String
    public var flags: [String]?
    public var metavar: String?
    public var choices: [String]?
    public var trueFlags: [String]?
    public var falseFlags: [String]?
    public var action: String?
    public var positionalMetavar: String?

    enum CodingKeys: String, CodingKey {
        case kind
        case flags
        case metavar
        case choices
        case trueFlags = "true_flags"
        case falseFlags = "false_flags"
        case action
        case positionalMetavar = "positional_metavar"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        kind = try container.decode(String.self, forKey: .kind)
        flags = try container.decodeIfPresent([String].self, forKey: .flags)
        metavar = try container.decodeIfPresent(String.self, forKey: .metavar)
        choices = try container.decodeIfPresent([String].self, forKey: .choices)
        trueFlags = try container.decodeIfPresent([String].self, forKey: .trueFlags)
        falseFlags = try container.decodeIfPresent([String].self, forKey: .falseFlags)
        action = try container.decodeIfPresent(String.self, forKey: .action)
        positionalMetavar = try container.decodeIfPresent(String.self, forKey: .positionalMetavar)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(kind, forKey: .kind)
        try container.encodeIfPresent(flags, forKey: .flags)
        try container.encodeIfPresent(metavar, forKey: .metavar)
        try container.encodeIfPresent(choices, forKey: .choices)
        try container.encodeIfPresent(trueFlags, forKey: .trueFlags)
        try container.encodeIfPresent(falseFlags, forKey: .falseFlags)
        try container.encodeIfPresent(action, forKey: .action)
        try container.encodeIfPresent(positionalMetavar, forKey: .positionalMetavar)
    }
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

public struct RunProductReference: Identifiable, Codable, Equatable {
    public let id: String
    public var artifactKind: String
    public var label: String
    public var path: String
    public var datasetID: String?
    public var exists: Bool
    public var previewPngPath: String?
    public var previewPngExists: Bool

    public init(
        id: String,
        artifactKind: String,
        label: String,
        path: String,
        datasetID: String?,
        exists: Bool,
        previewPngPath: String?,
        previewPngExists: Bool
    ) {
        self.id = id
        self.artifactKind = artifactKind
        self.label = label
        self.path = path
        self.datasetID = datasetID
        self.exists = exists
        self.previewPngPath = previewPngPath
        self.previewPngExists = previewPngExists
    }
}

public struct RunProductGroup: Identifiable, Codable, Equatable {
    public let id: String
    public var runID: String
    public var title: String
    public var sourceDatasetID: String
    public var sourcePath: String
    public var products: [RunProductReference]
    public var diagnostics: [String]

    public init(
        id: String,
        runID: String,
        title: String,
        sourceDatasetID: String,
        sourcePath: String,
        products: [RunProductReference],
        diagnostics: [String]
    ) {
        self.id = id
        self.runID = runID
        self.title = title
        self.sourceDatasetID = sourceDatasetID
        self.sourcePath = sourcePath
        self.products = products
        self.diagnostics = diagnostics
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
    case antennaLayout
    case scanTimeline
    case spectralWindowCoverage
    case phaseVsTime
    case amplitudePhaseVsTimeStacked
    case weightVsTime
    case sigmaVsTime
    case flagVsTime
    case weightSpectrumVsTime
    case sigmaSpectrumVsTime
    case flagRowVsTime
    case elevationVsTime
    case azimuthVsTime
    case hourAngleVsTime
    case parallacticAngleVsTime
    case azimuthVsElevation
    case amplitudeVsFrequency
    case amplitudeVsChannel
    case phaseVsChannel
    case phaseVsFrequency
    case amplitudeVsVelocity
    case phaseVsVelocity
    case amplitudeVsUvDistance
    case amplitudeVsTime
    case realVsImaginary

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .uvCoverage:
            "UV Coverage"
        case .antennaLayout:
            "Antenna Layout"
        case .scanTimeline:
            "Scan Timeline"
        case .spectralWindowCoverage:
            "Spectral Window Coverage"
        case .phaseVsTime:
            "Phase vs Time"
        case .amplitudePhaseVsTimeStacked:
            "Amplitude / Phase vs Time"
        case .weightVsTime:
            "Weight vs Time"
        case .sigmaVsTime:
            "Sigma vs Time"
        case .flagVsTime:
            "Flag vs Time"
        case .weightSpectrumVsTime:
            "Weight Spectrum vs Time"
        case .sigmaSpectrumVsTime:
            "Sigma Spectrum vs Time"
        case .flagRowVsTime:
            "Flag Row vs Time"
        case .elevationVsTime:
            "Elevation vs Time"
        case .azimuthVsTime:
            "Azimuth vs Time"
        case .hourAngleVsTime:
            "Hour Angle vs Time"
        case .parallacticAngleVsTime:
            "Parallactic Angle vs Time"
        case .azimuthVsElevation:
            "Azimuth vs Elevation"
        case .amplitudeVsFrequency:
            "Amplitude vs Frequency"
        case .amplitudeVsChannel:
            "Amplitude vs Channel"
        case .phaseVsChannel:
            "Phase vs Channel"
        case .phaseVsFrequency:
            "Phase vs Frequency"
        case .amplitudeVsVelocity:
            "Amplitude vs Velocity"
        case .phaseVsVelocity:
            "Phase vs Velocity"
        case .amplitudeVsUvDistance:
            "Amplitude vs UV Distance"
        case .amplitudeVsTime:
            "Amplitude vs Time"
        case .realVsImaginary:
            "Real vs Imaginary"
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
    public var preset: MeasurementSetExplorerPlotPreset
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
    public var plotDocument: WorkbenchPlotDocument
    public var renderer: String
    public var imageFormat: String
    public var imageWidth: UInt32
    public var imageHeight: UInt32
    public var imageBytes: Data
    public var imageCacheID: String

    public init(
        preset: MeasurementSetExplorerPlotPreset,
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
        plotDocument: WorkbenchPlotDocument,
        renderer: String,
        imageFormat: String,
        imageWidth: UInt32,
        imageHeight: UInt32,
        imageBytes: Data
    ) {
        self.preset = preset
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
        self.plotDocument = plotDocument
        self.renderer = renderer
        self.imageFormat = imageFormat
        self.imageWidth = imageWidth
        self.imageHeight = imageHeight
        self.imageBytes = imageBytes
        self.imageCacheID = Self.makeImageCacheID(
            imageFormat: imageFormat,
            imageWidth: imageWidth,
            imageHeight: imageHeight,
            imageBytes: imageBytes
        )
    }

    private static func makeImageCacheID(
        imageFormat: String,
        imageWidth: UInt32,
        imageHeight: UInt32,
        imageBytes: Data
    ) -> String {
        var hash: UInt64 = 1_469_598_103_934_665_603
        for byte in imageBytes {
            hash ^= UInt64(byte)
            hash &*= 1_099_511_628_211
        }
        return "\(imageFormat):\(imageWidth)x\(imageHeight):\(imageBytes.count):\(hash)"
    }
}

public struct MeasurementSetExplorerPlotState: Codable, Equatable {
    public var datasetID: String
    public var preset: MeasurementSetExplorerPlotPreset
    public var selectedField: String?
    public var selectedSpectralWindow: String?
    public var selectedChannelSelection: String?
    public var selectedTimerange: String?
    public var selectedUVRange: String?
    public var selectedAntenna: String?
    public var selectedScan: String?
    public var selectedCorrelation: String?
    public var selectedArray: String?
    public var selectedObservation: String?
    public var selectedIntent: String?
    public var selectedFeed: String?
    public var selectedMSSelect: String?
    public var dataColumn: String
    public var avgChannel: UInt64?
    public var avgTime: Double?
    public var avgScan: Bool
    public var avgField: Bool
    public var avgBaseline: Bool
    public var avgAntenna: Bool
    public var avgSPW: Bool
    public var scalarAverage: Bool
    public var maxPlotPoints: UInt64
    public var status: MeasurementSetPlotStatus
    public var lastError: String?
    public var result: MeasurementSetPlotResultSummary?

    public init(
        datasetID: String,
        preset: MeasurementSetExplorerPlotPreset,
        selectedField: String?,
        selectedSpectralWindow: String?,
        selectedChannelSelection: String? = nil,
        selectedTimerange: String? = nil,
        selectedUVRange: String? = nil,
        selectedAntenna: String? = nil,
        selectedScan: String? = nil,
        selectedCorrelation: String?,
        selectedArray: String? = nil,
        selectedObservation: String? = nil,
        selectedIntent: String? = nil,
        selectedFeed: String? = nil,
        selectedMSSelect: String? = nil,
        dataColumn: String,
        avgChannel: UInt64? = nil,
        avgTime: Double? = nil,
        avgScan: Bool = false,
        avgField: Bool = false,
        avgBaseline: Bool = false,
        avgAntenna: Bool = false,
        avgSPW: Bool = false,
        scalarAverage: Bool = false,
        maxPlotPoints: UInt64 = WorkbenchState.defaultMeasurementSetPlotMaxPoints,
        status: MeasurementSetPlotStatus,
        lastError: String?,
        result: MeasurementSetPlotResultSummary?
    ) {
        self.datasetID = datasetID
        self.preset = preset
        self.selectedField = selectedField
        self.selectedSpectralWindow = selectedSpectralWindow
        self.selectedChannelSelection = selectedChannelSelection
        self.selectedTimerange = selectedTimerange
        self.selectedUVRange = selectedUVRange
        self.selectedAntenna = selectedAntenna
        self.selectedScan = selectedScan
        self.selectedCorrelation = selectedCorrelation
        self.selectedArray = selectedArray
        self.selectedObservation = selectedObservation
        self.selectedIntent = selectedIntent
        self.selectedFeed = selectedFeed
        self.selectedMSSelect = selectedMSSelect
        self.dataColumn = dataColumn
        self.avgChannel = avgChannel
        self.avgTime = avgTime
        self.avgScan = avgScan
        self.avgField = avgField
        self.avgBaseline = avgBaseline
        self.avgAntenna = avgAntenna
        self.avgSPW = avgSPW
        self.scalarAverage = scalarAverage
        self.maxPlotPoints = maxPlotPoints
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
            maxPlotPoints: WorkbenchState.defaultMeasurementSetPlotMaxPoints,
            status: .idle,
            lastError: nil,
            result: nil
        )
    }
}

public enum ExplorerSessionStatus: String, Codable, Equatable {
    case idle
    case ready
    case failed
}

public struct ImageExplorerParameters: Codable, Equatable {
    public var blc: String
    public var trc: String
    public var inc: String
    public var stretch: String
    public var autoscale: String
    public var clipLow: String
    public var clipHigh: String

    public init(
        blc: String = "",
        trc: String = "",
        inc: String = "",
        stretch: String = "percentile99",
        autoscale: String = "per_plane",
        clipLow: String = "",
        clipHigh: String = ""
    ) {
        self.blc = blc
        self.trc = trc
        self.inc = inc
        self.stretch = stretch
        self.autoscale = autoscale
        self.clipLow = clipLow
        self.clipHigh = clipHigh
    }

    enum CodingKeys: String, CodingKey {
        case blc
        case trc
        case inc
        case stretch
        case autoscale
        case clipLow = "clip_low"
        case clipHigh = "clip_high"
    }
}

public enum ImageExplorerColorMap: String, CaseIterable, Codable, Equatable, Identifiable {
    case grayscale
    case viridis
    case inferno
    case magma
    case coolWarm

    public var id: String { rawValue }

    public var label: String {
        switch self {
        case .grayscale:
            return "Grayscale"
        case .viridis:
            return "Viridis"
        case .inferno:
            return "Inferno"
        case .magma:
            return "Magma"
        case .coolWarm:
            return "Cool/Warm"
        }
    }

    public func next() -> Self {
        let cases = Self.allCases
        guard let index = cases.firstIndex(of: self) else {
            return .grayscale
        }
        return cases[(index + 1) % cases.count]
    }
}

public struct ImageExplorerSnapshotRequest: Codable, Equatable {
    public var datasetPath: String
    public var selectedView: String
    public var focus: String
    public var planeContentMode: String
    public var parameters: ImageExplorerParameters
    public var cursorX: Int?
    public var cursorY: Int?
    public var selectedProfileAxis: Int?
    public var nonDisplayIndices: [Int]
    public var commands: [ImageExplorerCommand]
    public var transientCommands: [ImageExplorerCommand]
    public var includeProfile: Bool

    public init(
        datasetPath: String,
        selectedView: String = "plane",
        focus: String = "content",
        planeContentMode: String = "raster",
        parameters: ImageExplorerParameters = ImageExplorerParameters(),
        cursorX: Int? = nil,
        cursorY: Int? = nil,
        selectedProfileAxis: Int? = nil,
        nonDisplayIndices: [Int] = [],
        commands: [ImageExplorerCommand] = [],
        transientCommands: [ImageExplorerCommand] = [],
        includeProfile: Bool = true
    ) {
        self.datasetPath = datasetPath
        self.selectedView = selectedView
        self.focus = focus
        self.planeContentMode = planeContentMode
        self.parameters = parameters
        self.cursorX = cursorX
        self.cursorY = cursorY
        self.selectedProfileAxis = selectedProfileAxis
        self.nonDisplayIndices = nonDisplayIndices
        self.commands = commands
        self.transientCommands = transientCommands
        self.includeProfile = includeProfile
    }

    enum CodingKeys: String, CodingKey {
        case datasetPath = "dataset_path"
        case selectedView = "active_view"
        case focus
        case planeContentMode = "plane_content_mode"
        case parameters
        case cursorX = "cursor_x"
        case cursorY = "cursor_y"
        case selectedProfileAxis = "selected_profile_axis"
        case nonDisplayIndices = "non_display_indices"
        case commands
        case transientCommands = "transient_commands"
        case includeProfile = "include_profile"
    }
}

public struct ImageExplorerCommand: Codable, Equatable {
    public var command: String
    public var x: Int?
    public var y: Int?
    public var name: String?
    public var newName: String?
    public var setDefault: Bool?

    public init(
        command: String,
        x: Int? = nil,
        y: Int? = nil,
        name: String? = nil,
        newName: String? = nil,
        setDefault: Bool? = nil
    ) {
        self.command = command
        self.x = x
        self.y = y
        self.name = name
        self.newName = newName
        self.setDefault = setDefault
    }

    public static let startRegionShape = ImageExplorerCommand(command: "start_region_shape")
    public static func appendRegionVertex(x: Int, y: Int) -> ImageExplorerCommand {
        ImageExplorerCommand(command: "append_region_vertex", x: x, y: y)
    }
    public static let closeRegionShape = ImageExplorerCommand(command: "close_region_shape")
    public static let undoRegionVertex = ImageExplorerCommand(command: "undo_region_vertex")
    public static let cancelRegionShape = ImageExplorerCommand(command: "cancel_region_shape")
    public static let clearRegion = ImageExplorerCommand(command: "clear_region")
    public static let saveRegionDefinition = ImageExplorerCommand(command: "save_region_definition")
    public static let loadNextRegionDefinition = ImageExplorerCommand(command: "load_next_region_definition")
    public static func loadRegionDefinition(name: String) -> ImageExplorerCommand {
        ImageExplorerCommand(command: "load_region_definition", name: name)
    }
    public static func deleteRegionDefinition(name: String) -> ImageExplorerCommand {
        ImageExplorerCommand(command: "delete_region_definition", name: name)
    }
    public static func setDefaultMask(name: String) -> ImageExplorerCommand {
        ImageExplorerCommand(command: "set_default_mask", name: name)
    }
    public static let unsetDefaultMask = ImageExplorerCommand(command: "unset_default_mask")
    public static func deleteMask(name: String) -> ImageExplorerCommand {
        ImageExplorerCommand(command: "delete_mask", name: name)
    }
    public static func writeRegionMask(name: String?, setDefault: Bool) -> ImageExplorerCommand {
        ImageExplorerCommand(command: "write_region_mask", name: name, setDefault: setDefault)
    }

    enum CodingKeys: String, CodingKey {
        case command
        case x
        case y
        case name
        case newName = "new_name"
        case setDefault = "set_default"
    }
}

public struct ImageExplorerSnapshot: Codable, Equatable {
    public struct AxisValue: Codable, Equatable {
        public var name: String
        public var unit: String
        public var value: Double
    }

    public struct Capabilities: Codable, Equatable {
        public var renderablePlane: Bool
        public var worldCoordsAvailable: Bool
        public var pixelOnlyMode: Bool
        public var nonDisplayAxisSelectors: Bool
        public var maskPresent: Bool
        public var complexUnsupported: Bool?

        enum CodingKeys: String, CodingKey {
            case renderablePlane = "renderable_plane"
            case worldCoordsAvailable = "world_coords_available"
            case pixelOnlyMode = "pixel_only_mode"
            case nonDisplayAxisSelectors = "non_display_axis_selectors"
            case maskPresent = "mask_present"
            case complexUnsupported = "complex_unsupported"
        }
    }

    public struct Plane: Codable, Equatable {
        public var width: Int
        public var height: Int
        public var pixelsU8: [UInt8]
        public var clipMin: Double
        public var clipMax: Double
        public var dataMin: Double
        public var dataMax: Double
        public var valueUnit: String
        public var histogramBins: [UInt32]?
        public var maskedOrNonFiniteCount: Int
        public var noFiniteValues: Bool?

        enum CodingKeys: String, CodingKey {
            case width
            case height
            case pixelsU8 = "pixels_u8"
            case clipMin = "clip_min"
            case clipMax = "clip_max"
            case dataMin = "data_min"
            case dataMax = "data_max"
            case valueUnit = "value_unit"
            case histogramBins = "histogram_bins"
            case maskedOrNonFiniteCount = "masked_or_non_finite_count"
            case noFiniteValues = "no_finite_values"
        }
    }

    public struct Profile: Codable, Equatable {
        public struct Sample: Codable, Equatable {
            public var sampleIndex: Int
            public var pixelIndex: Int
            public var value: Double
            public var masked: Bool?
            public var finite: Bool
            public var worldAxis: AxisValue?

            enum CodingKeys: String, CodingKey {
                case sampleIndex = "sample_index"
                case pixelIndex = "pixel_index"
                case value
                case masked
                case finite
                case worldAxis = "world_axis"
            }
        }

        public var axis: Int
        public var axisName: String
        public var axisUnit: String
        public var valueUnit: String
        public var coordType: String?
        public var selectedSampleIndex: Int?
        public var samples: [Sample]

        enum CodingKeys: String, CodingKey {
            case axis
            case axisName = "axis_name"
            case axisUnit = "axis_unit"
            case valueUnit = "value_unit"
            case coordType = "coord_type"
            case selectedSampleIndex = "selected_sample_index"
            case samples
        }
    }

    public struct Region: Codable, Equatable {
        public struct OverlayVertex: Codable, Equatable {
            public var sampledX: Double
            public var sampledY: Double

            enum CodingKeys: String, CodingKey {
                case sampledX = "sampled_x"
                case sampledY = "sampled_y"
            }
        }

        public struct OverlayShape: Codable, Equatable {
            public var vertices: [OverlayVertex]
            public var closed: Bool
        }

        public struct Stats: Codable, Equatable {
            public var pixelCount: Int
            public var median: Double
            public var min: Double
            public var max: Double
            public var mean: Double
            public var sigma: Double
            public var rms: Double
            public var sum: Double
            public var valueUnit: String

            enum CodingKeys: String, CodingKey {
                case pixelCount = "pixel_count"
                case median
                case min
                case max
                case mean
                case sigma
                case rms
                case sum
                case valueUnit = "value_unit"
            }
        }

        public var label: String
        public var shapeCount: Int
        public var closedShapeCount: Int
        public var editing: Bool
        public var activeShapeVertices: Int?
        public var overlayShapes: [OverlayShape]?
        public var stats: Stats?

        enum CodingKeys: String, CodingKey {
            case label
            case shapeCount = "shape_count"
            case closedShapeCount = "closed_shape_count"
            case editing
            case activeShapeVertices = "active_shape_vertices"
            case overlayShapes = "overlay_shapes"
            case stats
        }
    }

    public struct Navigation: Codable, Equatable {
        public var selectedIndex: Int
        public var totalItems: Int
        public var viewportItems: Int

        enum CodingKeys: String, CodingKey {
            case selectedIndex = "selected_index"
            case totalItems = "total_items"
            case viewportItems = "viewport_items"
        }
    }

    public struct DisplayAxis: Codable, Equatable {
        public var axis: Int
        public var name: String
        public var unit: String
        public var blc: Int
        public var trc: Int
        public var inc: Int
        public var sampledLen: Int
        public var worldIncrement: Double?

        enum CodingKeys: String, CodingKey {
            case axis
            case name
            case unit
            case blc
            case trc
            case inc
            case sampledLen = "sampled_len"
            case worldIncrement = "world_increment"
        }
    }

    public struct PlaneCursor: Codable, Equatable {
        public var sampledX: Int
        public var sampledY: Int
        public var pixelX: Int
        public var pixelY: Int

        enum CodingKeys: String, CodingKey {
            case sampledX = "sampled_x"
            case sampledY = "sampled_y"
            case pixelX = "pixel_x"
            case pixelY = "pixel_y"
        }
    }

    public struct NonDisplayAxis: Codable, Equatable, Identifiable {
        public var axis: Int
        public var label: String
        public var index: Int
        public var length: Int
        public var pixel: Int

        public var id: Int { axis }
    }

    public struct Probe: Codable, Equatable {
        public var pixelIndices: [Int]
        public var pixelAxes: [AxisValue]
        public var value: Double
        public var masked: Bool
        public var finite: Bool
        public var worldAxes: [AxisValue]

        enum CodingKeys: String, CodingKey {
            case pixelIndices = "pixel_indices"
            case pixelAxes = "pixel_axes"
            case value
            case masked
            case finite
            case worldAxes = "world_axes"
        }
    }

    public struct BackendTiming: Codable, Equatable {
        public var planeCacheResult: String
        public var cachedPlaneLookupNs: UInt64
        public var planeExtractNs: UInt64
        public var statCollectionNs: UInt64
        public var histogramNs: UInt64
        public var rasterizeNs: UInt64
        public var totalPlaneNs: UInt64
        public var profileCacheHits: UInt64?
        public var profileCacheMisses: UInt64?
        public var profileExtractTotalNs: UInt64?

        enum CodingKeys: String, CodingKey {
            case planeCacheResult = "plane_cache_result"
            case cachedPlaneLookupNs = "cached_plane_lookup_ns"
            case planeExtractNs = "plane_extract_ns"
            case statCollectionNs = "stat_collection_ns"
            case histogramNs = "histogram_ns"
            case rasterizeNs = "rasterize_ns"
            case totalPlaneNs = "total_plane_ns"
            case profileCacheHits = "profile_cache_hits"
            case profileCacheMisses = "profile_cache_misses"
            case profileExtractTotalNs = "profile_extract_total_ns"
        }
    }

    public var statusLine: String
    public var activeView: String
    public var focus: String?
    public var shape: [Int]
    public var parameters: ImageExplorerParameters?
    public var inspectorLines: [String]
    public var contentLines: [String]
    public var navigation: Navigation?
    public var plane: Plane?
    public var probe: Probe?
    public var profile: Profile?
    public var displayAxes: [DisplayAxis]?
    public var planeCursor: PlaneCursor?
    public var nonDisplayAxes: [NonDisplayAxis]?
    public var region: Region?
    public var savedRegionNames: [String]
    public var activeRegionDefinitionName: String?
    public var maskNames: [String]
    public var defaultMaskName: String?
    public var backendTiming: BackendTiming?
    public var capabilities: Capabilities

    enum CodingKeys: String, CodingKey {
        case statusLine = "status_line"
        case activeView = "active_view"
        case focus
        case shape
        case parameters
        case inspectorLines = "inspector_lines"
        case contentLines = "content_lines"
        case navigation
        case plane
        case probe
        case profile
        case displayAxes = "display_axes"
        case planeCursor = "plane_cursor"
        case nonDisplayAxes = "non_display_axes"
        case region
        case savedRegionNames = "saved_region_names"
        case activeRegionDefinitionName = "active_region_definition_name"
        case maskNames = "mask_names"
        case defaultMaskName = "default_mask_name"
        case backendTiming = "backend_timing"
        case capabilities
    }
}

public struct ImageExplorerSessionState: Codable, Equatable {
    public var datasetID: String
    public var selectedView: String
    public var focus: String = "content"
    public var planeContentMode: String = "raster"
    public var planeColorMap: ImageExplorerColorMap = .grayscale
    public var parameters: ImageExplorerParameters = ImageExplorerParameters()
    public var cursorX: Int?
    public var cursorY: Int?
    public var selectedProfileAxis: Int?
    public var nonDisplayIndices: [Int] = []
    public var moviePlaying: Bool = false
    public var movieAxis: Int?
    public var movieFramesPerSecond: Double = 6.0
    public var movieLoop: Bool = true
    public var regionCommands: [ImageExplorerCommand] = []
    public var transientCommands: [ImageExplorerCommand] = []
    public var status: ExplorerSessionStatus
    public var lastError: String?
    public var snapshot: ImageExplorerSnapshot?

    public func snapshotRequest(datasetPath: String) -> ImageExplorerSnapshotRequest {
        ImageExplorerSnapshotRequest(
            datasetPath: datasetPath,
            selectedView: selectedView,
            focus: focus,
            planeContentMode: planeContentMode,
            parameters: parameters,
            cursorX: cursorX,
            cursorY: cursorY,
            selectedProfileAxis: selectedProfileAxis,
            nonDisplayIndices: nonDisplayIndices,
            commands: regionCommands,
            transientCommands: transientCommands,
            includeProfile: true
        )
    }
}

public struct TableBrowserSnapshot: Codable, Equatable {
    public struct Breadcrumb: Codable, Equatable {
        public var label: String
        public var path: String
    }

    public struct Capabilities: Codable, Equatable {
        public var editable: Bool
    }

    public struct Viewport: Codable, Equatable {
        public var width: Int
        public var height: Int
        public var inspectorHeight: Int

        enum CodingKeys: String, CodingKey {
            case width
            case height
            case inspectorHeight = "inspector_height"
        }
    }

    public struct NavigationMetrics: Codable, Equatable {
        public var selectedIndex: Int
        public var totalItems: Int
        public var viewportItems: Int

        enum CodingKeys: String, CodingKey {
            case selectedIndex = "selected_index"
            case totalItems = "total_items"
            case viewportItems = "viewport_items"
        }
    }

    public struct SelectedAddress: Codable, Equatable {
        public var kind: String
        public var tablePath: String?
        public var row: Int?
        public var column: String?
        public var keywordPath: [String]?
        public var valuePath: [ValuePathSegment]?
        public var source: String?
        public var targetPath: String?

        enum CodingKeys: String, CodingKey {
            case kind
            case tablePath = "table_path"
            case row
            case column
            case keywordPath = "keyword_path"
            case valuePath = "value_path"
            case source
            case targetPath = "target_path"
        }
    }

    public struct ValuePathSegment: Codable, Equatable {
        public var segment: String
        public var name: String?
        public var flatIndex: Int?

        enum CodingKeys: String, CodingKey {
            case segment
            case name
            case flatIndex = "flat_index"
        }
    }

    public enum ScalarValue: Codable, Equatable {
        case bool(Bool)
        case int(Int64)
        case uint(UInt64)
        case float(Double)
        case complex(re: Double, im: Double)
        case string(String)
        case unknown(type: String, display: String)

        enum CodingKeys: String, CodingKey {
            case type
            case value
        }

        enum ComplexCodingKeys: String, CodingKey {
            case re
            case im
        }

        public init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            let type = try container.decode(String.self, forKey: .type)
            switch type {
            case "bool":
                self = .bool(try container.decode(Bool.self, forKey: .value))
            case "uint8", "uint16", "uint32":
                self = .uint(try container.decode(UInt64.self, forKey: .value))
            case "int16", "int32", "int64":
                self = .int(try container.decode(Int64.self, forKey: .value))
            case "float32", "float64":
                self = .float(try container.decode(Double.self, forKey: .value))
            case "complex32", "complex64":
                let complex = try container.nestedContainer(keyedBy: ComplexCodingKeys.self, forKey: .value)
                self = .complex(
                    re: try complex.decode(Double.self, forKey: .re),
                    im: try complex.decode(Double.self, forKey: .im)
                )
            case "string":
                self = .string(try container.decode(String.self, forKey: .value))
            default:
                let display = (try? container.decode(String.self, forKey: .value)) ?? ""
                self = .unknown(type: type, display: display)
            }
        }

        public func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            switch self {
            case let .bool(value):
                try container.encode("bool", forKey: .type)
                try container.encode(value, forKey: .value)
            case let .int(value):
                try container.encode("int64", forKey: .type)
                try container.encode(value, forKey: .value)
            case let .uint(value):
                try container.encode("uint32", forKey: .type)
                try container.encode(value, forKey: .value)
            case let .float(value):
                try container.encode("float64", forKey: .type)
                try container.encode(value, forKey: .value)
            case let .complex(re, im):
                try container.encode("complex64", forKey: .type)
                var complex = container.nestedContainer(keyedBy: ComplexCodingKeys.self, forKey: .value)
                try complex.encode(re, forKey: .re)
                try complex.encode(im, forKey: .im)
            case let .string(value):
                try container.encode("string", forKey: .type)
                try container.encode(value, forKey: .value)
            case let .unknown(type, display):
                try container.encode(type, forKey: .type)
                try container.encode(display, forKey: .value)
            }
        }
    }

    public struct ArrayElement: Codable, Equatable {
        public var flatIndex: Int
        public var index: [Int]
        public var value: ScalarValue
        public var selected: Bool

        enum CodingKeys: String, CodingKey {
            case flatIndex = "flat_index"
            case index
            case value
            case selected
        }
    }

    public struct RecordFieldSummary: Codable, Equatable {
        public var name: String
        public var kind: String
        public var summary: String
        public var expandable: Bool
        public var openable: Bool
        public var selected: Bool
    }

    public enum ValueNode: Codable, Equatable {
        case undefined
        case scalar(value: ScalarValue)
        case array(primitive: String, shape: [Int], totalElements: Int, pageStart: Int, pageSize: Int, elements: [ArrayElement])
        case record(totalFields: Int, pageStart: Int, pageSize: Int, fields: [RecordFieldSummary])
        case tableRef(path: String, resolvedPath: String, openable: Bool)

        enum CodingKeys: String, CodingKey {
            case kind
            case value
            case primitive
            case shape
            case totalElements = "total_elements"
            case pageStart = "page_start"
            case pageSize = "page_size"
            case elements
            case totalFields = "total_fields"
            case fields
            case path
            case resolvedPath = "resolved_path"
            case openable
        }

        public init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            let kind = try container.decode(String.self, forKey: .kind)
            switch kind {
            case "undefined":
                self = .undefined
            case "scalar":
                self = .scalar(value: try container.decode(ScalarValue.self, forKey: .value))
            case "array":
                self = .array(
                    primitive: try container.decode(String.self, forKey: .primitive),
                    shape: try container.decode([Int].self, forKey: .shape),
                    totalElements: try container.decode(Int.self, forKey: .totalElements),
                    pageStart: try container.decode(Int.self, forKey: .pageStart),
                    pageSize: try container.decode(Int.self, forKey: .pageSize),
                    elements: try container.decode([ArrayElement].self, forKey: .elements)
                )
            case "record":
                self = .record(
                    totalFields: try container.decode(Int.self, forKey: .totalFields),
                    pageStart: try container.decode(Int.self, forKey: .pageStart),
                    pageSize: try container.decode(Int.self, forKey: .pageSize),
                    fields: try container.decode([RecordFieldSummary].self, forKey: .fields)
                )
            case "table_ref":
                self = .tableRef(
                    path: try container.decode(String.self, forKey: .path),
                    resolvedPath: try container.decode(String.self, forKey: .resolvedPath),
                    openable: try container.decode(Bool.self, forKey: .openable)
                )
            default:
                self = .undefined
            }
        }

        public func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            switch self {
            case .undefined:
                try container.encode("undefined", forKey: .kind)
            case let .scalar(value):
                try container.encode("scalar", forKey: .kind)
                try container.encode(value, forKey: .value)
            case let .array(primitive, shape, totalElements, pageStart, pageSize, elements):
                try container.encode("array", forKey: .kind)
                try container.encode(primitive, forKey: .primitive)
                try container.encode(shape, forKey: .shape)
                try container.encode(totalElements, forKey: .totalElements)
                try container.encode(pageStart, forKey: .pageStart)
                try container.encode(pageSize, forKey: .pageSize)
                try container.encode(elements, forKey: .elements)
            case let .record(totalFields, pageStart, pageSize, fields):
                try container.encode("record", forKey: .kind)
                try container.encode(totalFields, forKey: .totalFields)
                try container.encode(pageStart, forKey: .pageStart)
                try container.encode(pageSize, forKey: .pageSize)
                try container.encode(fields, forKey: .fields)
            case let .tableRef(path, resolvedPath, openable):
                try container.encode("table_ref", forKey: .kind)
                try container.encode(path, forKey: .path)
                try container.encode(resolvedPath, forKey: .resolvedPath)
                try container.encode(openable, forKey: .openable)
            }
        }
    }

    public struct InspectorTrailEntry: Codable, Equatable {
        public var label: String
        public var summary: String
    }

    public struct Inspector: Codable, Equatable {
        public var title: String
        public var trail: [InspectorTrailEntry]
        public var node: ValueNode
        public var renderedLines: [String]

        enum CodingKeys: String, CodingKey {
            case title
            case trail
            case node
            case renderedLines = "rendered_lines"
        }
    }

    public var capabilities: Capabilities?
    public var view: String
    public var focus: String
    public var tablePath: String
    public var breadcrumb: [Breadcrumb]
    public var viewport: Viewport?
    public var statusLine: String
    public var contentLines: [String]
    public var verticalMetrics: NavigationMetrics?
    public var horizontalMetrics: NavigationMetrics?
    public var selectedAddress: SelectedAddress?
    public var inspector: Inspector?

    enum CodingKeys: String, CodingKey {
        case capabilities
        case view
        case focus
        case tablePath = "table_path"
        case breadcrumb
        case viewport
        case statusLine = "status_line"
        case contentLines = "content_lines"
        case verticalMetrics = "vertical_metrics"
        case horizontalMetrics = "horizontal_metrics"
        case selectedAddress = "selected_address"
        case inspector
    }
}

public struct TableBrowserCellWindowSnapshot: Codable, Equatable {
    public struct Column: Codable, Equatable {
        public var index: Int
        public var name: String
        public var header: String
        public var summary: String
        public var width: Int
        public var keywords: [String]

        enum CodingKeys: String, CodingKey {
            case index
            case name
            case header
            case summary
            case width
            case keywords
        }

        public init(
            index: Int,
            name: String,
            header: String,
            summary: String,
            width: Int,
            keywords: [String] = []
        ) {
            self.index = index
            self.name = name
            self.header = header
            self.summary = summary
            self.width = width
            self.keywords = keywords
        }

        public init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            index = try container.decode(Int.self, forKey: .index)
            name = try container.decode(String.self, forKey: .name)
            header = try container.decode(String.self, forKey: .header)
            summary = try container.decode(String.self, forKey: .summary)
            width = try container.decode(Int.self, forKey: .width)
            keywords = try container.decodeIfPresent([String].self, forKey: .keywords) ?? []
        }
    }

    public struct Row: Codable, Equatable {
        public var index: Int
        public var cells: [Cell]
    }

    public struct Cell: Codable, Equatable {
        public var columnIndex: Int
        public var display: String
        public var defined: Bool

        enum CodingKeys: String, CodingKey {
            case columnIndex = "column_index"
            case display
            case defined
        }
    }

    public var tablePath: String
    public var rowCount: Int
    public var columnCount: Int
    public var rowStart: Int
    public var columnStart: Int
    public var columns: [Column]
    public var rows: [Row]

    enum CodingKeys: String, CodingKey {
        case tablePath = "table_path"
        case rowCount = "row_count"
        case columnCount = "column_count"
        case rowStart = "row_start"
        case columnStart = "column_start"
        case columns
        case rows
    }

    public func row(_ index: Int) -> Row? {
        rows.first { $0.index == index }
    }

    public func cell(row rowIndex: Int, column columnIndex: Int) -> Cell? {
        row(rowIndex)?.cells.first { $0.columnIndex == columnIndex }
    }

    public func contains(rowStart requestedRowStart: Int, rowLimit: Int, columnStart requestedColumnStart: Int, columnLimit: Int) -> Bool {
        guard rowLimit > 0, columnLimit > 0 else {
            return true
        }
        let requestedRowEnd = min(requestedRowStart + rowLimit, rowCount)
        let requestedColumnEnd = min(requestedColumnStart + columnLimit, columnCount)
        let rowEnd = rowStart + rows.count
        let columnEnd = columnStart + max(0, rows.first?.cells.count ?? 0)
        return requestedRowStart >= rowStart
            && requestedRowEnd <= rowEnd
            && requestedColumnStart >= columnStart
            && requestedColumnEnd <= columnEnd
    }
}

public enum TableBrowserCommand: Codable, Equatable {
    case setFocus(String)
    case cycleView(forward: Bool)
    case moveUp(steps: Int)
    case moveDown(steps: Int)
    case moveLeft(steps: Int)
    case moveRight(steps: Int)
    case pageUp(pages: Int)
    case pageDown(pages: Int)
    case activate
    case back
    case escape

    enum CodingKeys: String, CodingKey {
        case command
        case focus
        case forward
        case steps
        case pages
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        switch try container.decode(String.self, forKey: .command) {
        case "set_focus":
            self = .setFocus(try container.decode(String.self, forKey: .focus))
        case "cycle_view":
            self = .cycleView(forward: try container.decode(Bool.self, forKey: .forward))
        case "move_up":
            self = .moveUp(steps: try container.decode(Int.self, forKey: .steps))
        case "move_down":
            self = .moveDown(steps: try container.decode(Int.self, forKey: .steps))
        case "move_left":
            self = .moveLeft(steps: try container.decode(Int.self, forKey: .steps))
        case "move_right":
            self = .moveRight(steps: try container.decode(Int.self, forKey: .steps))
        case "page_up":
            self = .pageUp(pages: try container.decode(Int.self, forKey: .pages))
        case "page_down":
            self = .pageDown(pages: try container.decode(Int.self, forKey: .pages))
        case "activate":
            self = .activate
        case "back":
            self = .back
        case "escape":
            self = .escape
        default:
            self = .escape
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case let .setFocus(focus):
            try container.encode("set_focus", forKey: .command)
            try container.encode(focus, forKey: .focus)
        case let .cycleView(forward):
            try container.encode("cycle_view", forKey: .command)
            try container.encode(forward, forKey: .forward)
        case let .moveUp(steps):
            try container.encode("move_up", forKey: .command)
            try container.encode(steps, forKey: .steps)
        case let .moveDown(steps):
            try container.encode("move_down", forKey: .command)
            try container.encode(steps, forKey: .steps)
        case let .moveLeft(steps):
            try container.encode("move_left", forKey: .command)
            try container.encode(steps, forKey: .steps)
        case let .moveRight(steps):
            try container.encode("move_right", forKey: .command)
            try container.encode(steps, forKey: .steps)
        case let .pageUp(pages):
            try container.encode("page_up", forKey: .command)
            try container.encode(pages, forKey: .pages)
        case let .pageDown(pages):
            try container.encode("page_down", forKey: .command)
            try container.encode(pages, forKey: .pages)
        case .activate:
            try container.encode("activate", forKey: .command)
        case .back:
            try container.encode("back", forKey: .command)
        case .escape:
            try container.encode("escape", forKey: .command)
        }
    }
}

public struct TableBrowserSnapshotRequest: Codable, Equatable {
    public var datasetPath: String
    public var width: Int
    public var height: Int
    public var inspectorHeight: Int
    public var selectedView: String
    public var focus: String
    public var commands: [TableBrowserCommand]
    public var transientCommands: [TableBrowserCommand]

    enum CodingKeys: String, CodingKey {
        case datasetPath = "dataset_path"
        case width
        case height
        case inspectorHeight = "inspector_height"
        case selectedView = "selected_view"
        case focus
        case commands
        case transientCommands = "transient_commands"
    }
}

public struct TableBrowserCellWindowRequest: Codable, Equatable {
    public var datasetPath: String
    public var rowStart: Int
    public var rowLimit: Int
    public var columnStart: Int
    public var columnLimit: Int
    public var columnOptions: [TableBrowserColumnDisplayOption] = []

    enum CodingKeys: String, CodingKey {
        case datasetPath = "dataset_path"
        case rowStart = "row_start"
        case rowLimit = "row_limit"
        case columnStart = "column_start"
        case columnLimit = "column_limit"
        case columnOptions = "column_options"
    }
}

public struct TableBrowserColumnDisplayOption: Codable, Equatable {
    public var columnIndex: Int
    public var arrayInlineLimit: Int

    enum CodingKeys: String, CodingKey {
        case columnIndex = "column_index"
        case arrayInlineLimit = "array_inline_limit"
    }
}

public struct TableBrowserCellValueRequest: Codable, Equatable {
    public var datasetPath: String
    public var rowIndex: Int
    public var columnIndex: Int

    enum CodingKeys: String, CodingKey {
        case datasetPath = "dataset_path"
        case rowIndex = "row_index"
        case columnIndex = "column_index"
    }
}

public struct TableBrowserSessionState: Codable, Equatable {
    public var datasetID: String
    public var selectedView: String
    public var focus: String = "main"
    public var commands: [TableBrowserCommand] = []
    public var transientCommands: [TableBrowserCommand] = []
    public var cellWindowRowStart: Int = 0
    public var cellWindowRowLimit: Int = 1024
    public var cellWindowColumnStart: Int = 0
    public var cellWindowColumnLimit: Int = 24
    public var selectedCellRow: Int?
    public var selectedCellColumn: Int?
    public var hiddenCellColumns: Set<Int> = []
    public var cellColumnArrayInlineLimits: [Int: Int] = [:]
    public var status: ExplorerSessionStatus
    public var lastError: String?
    public var snapshot: TableBrowserSnapshot?
    public var cellWindow: TableBrowserCellWindowSnapshot?

    enum CodingKeys: String, CodingKey {
        case datasetID
        case selectedView
        case focus
        case commands
        case transientCommands
        case cellWindowRowStart
        case cellWindowRowLimit
        case cellWindowColumnStart
        case cellWindowColumnLimit
        case selectedCellRow
        case selectedCellColumn
        case hiddenCellColumns
        case cellColumnArrayInlineLimits
        case status
        case lastError
        case snapshot
        case cellWindow
    }

    public init(
        datasetID: String,
        selectedView: String,
        focus: String = "main",
        commands: [TableBrowserCommand] = [],
        transientCommands: [TableBrowserCommand] = [],
        cellWindowRowStart: Int = 0,
        cellWindowRowLimit: Int = 1024,
        cellWindowColumnStart: Int = 0,
        cellWindowColumnLimit: Int = 24,
        selectedCellRow: Int? = nil,
        selectedCellColumn: Int? = nil,
        hiddenCellColumns: Set<Int> = [],
        cellColumnArrayInlineLimits: [Int: Int] = [:],
        status: ExplorerSessionStatus,
        lastError: String?,
        snapshot: TableBrowserSnapshot?,
        cellWindow: TableBrowserCellWindowSnapshot? = nil
    ) {
        self.datasetID = datasetID
        self.selectedView = selectedView
        self.focus = focus
        self.commands = commands
        self.transientCommands = transientCommands
        self.cellWindowRowStart = cellWindowRowStart
        self.cellWindowRowLimit = cellWindowRowLimit
        self.cellWindowColumnStart = cellWindowColumnStart
        self.cellWindowColumnLimit = cellWindowColumnLimit
        self.selectedCellRow = selectedCellRow
        self.selectedCellColumn = selectedCellColumn
        self.hiddenCellColumns = hiddenCellColumns
        self.cellColumnArrayInlineLimits = cellColumnArrayInlineLimits
        self.status = status
        self.lastError = lastError
        self.snapshot = snapshot
        self.cellWindow = cellWindow
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        datasetID = try container.decode(String.self, forKey: .datasetID)
        selectedView = try container.decode(String.self, forKey: .selectedView)
        focus = try container.decodeIfPresent(String.self, forKey: .focus) ?? "main"
        commands = try container.decodeIfPresent([TableBrowserCommand].self, forKey: .commands) ?? []
        transientCommands = try container.decodeIfPresent([TableBrowserCommand].self, forKey: .transientCommands) ?? []
        cellWindowRowStart = try container.decodeIfPresent(Int.self, forKey: .cellWindowRowStart) ?? 0
        cellWindowRowLimit = try container.decodeIfPresent(Int.self, forKey: .cellWindowRowLimit) ?? 1024
        cellWindowColumnStart = try container.decodeIfPresent(Int.self, forKey: .cellWindowColumnStart) ?? 0
        cellWindowColumnLimit = try container.decodeIfPresent(Int.self, forKey: .cellWindowColumnLimit) ?? 24
        selectedCellRow = try container.decodeIfPresent(Int.self, forKey: .selectedCellRow)
        selectedCellColumn = try container.decodeIfPresent(Int.self, forKey: .selectedCellColumn)
        hiddenCellColumns = try container.decodeIfPresent(Set<Int>.self, forKey: .hiddenCellColumns) ?? []
        cellColumnArrayInlineLimits = try container.decodeIfPresent([Int: Int].self, forKey: .cellColumnArrayInlineLimits) ?? [:]
        status = try container.decode(ExplorerSessionStatus.self, forKey: .status)
        lastError = try container.decodeIfPresent(String.self, forKey: .lastError)
        snapshot = try container.decodeIfPresent(TableBrowserSnapshot.self, forKey: .snapshot)
        cellWindow = try container.decodeIfPresent(TableBrowserCellWindowSnapshot.self, forKey: .cellWindow)
    }

    public func snapshotRequest(datasetPath: String) -> TableBrowserSnapshotRequest {
        TableBrowserSnapshotRequest(
            datasetPath: datasetPath,
            width: 180,
            height: 48,
            inspectorHeight: 12,
            selectedView: selectedView,
            focus: focus,
            commands: commands,
            transientCommands: transientCommands
        )
    }

    public func cellWindowRequest(datasetPath: String) -> TableBrowserCellWindowRequest {
        let columnOptions = cellColumnArrayInlineLimits
            .filter { _, limit in limit > 0 }
            .map { columnIndex, limit in
                TableBrowserColumnDisplayOption(columnIndex: columnIndex, arrayInlineLimit: limit)
            }
            .sorted { $0.columnIndex < $1.columnIndex }
        return TableBrowserCellWindowRequest(
            datasetPath: datasetPath,
            rowStart: cellWindowRowStart,
            rowLimit: cellWindowRowLimit,
            columnStart: cellWindowColumnStart,
            columnLimit: cellWindowColumnLimit,
            columnOptions: columnOptions
        )
    }
}

public extension MeasurementSetPlotResultSummary {
    func matches(plotState: MeasurementSetExplorerPlotState) -> Bool {
        preset == plotState.preset
            && Self.canonicalDataColumn(dataColumn) == Self.canonicalDataColumn(plotState.dataColumn)
            && requestedMaxPoints == plotState.maxPlotPoints
    }

    private static func canonicalDataColumn(_ dataColumn: String) -> String {
        dataColumn.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
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
    public static let defaultMeasurementSetPlotMaxPoints: UInt64 = 250_000
    public static let minimumMeasurementSetPlotMaxPoints: UInt64 = 1_000
    public static let warningMeasurementSetPlotMaxPoints: UInt64 = 5_000_000

    public static func parseMeasurementSetPlotMaxPoints(_ text: String) -> UInt64? {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return nil
        }

        let suffixScale: Double
        let numericText: String
        if let suffix = trimmed.last, suffix == "k" || suffix == "K" {
            suffixScale = 1_000
            numericText = String(trimmed.dropLast())
        } else if let suffix = trimmed.last, suffix == "m" || suffix == "M" {
            suffixScale = 1_000_000
            numericText = String(trimmed.dropLast())
        } else {
            suffixScale = 1
            numericText = trimmed
        }

        guard let value = Double(numericText.trimmingCharacters(in: .whitespacesAndNewlines)),
              value.isFinite,
              value > 0,
              value <= Double(UInt64.max)
        else {
            return nil
        }
        return UInt64((value * suffixScale).rounded())
    }

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
    public var measurementSetPlotResultCache: [String: MeasurementSetPlotResultSummary]
    public var imageExplorers: [String: ImageExplorerSessionState]
    public var tableBrowsers: [String: TableBrowserSessionState]
    public var plotDocuments: [WorkbenchPlotDocument]
    public var jobs: [String: WorkbenchJob]
    public var activeJobIDsByTab: [String: String]
    public var runProductGroups: [RunProductGroup]
    public var taskCatalog: [TaskCatalogEntry]
    public var taskExecutionMatrixRows: [TaskExecutionMatrixRow]
    public var activeTaskID: String
    public var taskUISchemas: [String: TaskUISchema]
    public var genericTaskValues: [String: [String: String]]
    public var genericTaskToggles: [String: [String: Bool]]
    public var genericTaskConfirmations: [String: Bool]
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
        measurementSetPlotResultCache: [String: MeasurementSetPlotResultSummary] = [:],
        imageExplorers: [String: ImageExplorerSessionState] = [:],
        tableBrowsers: [String: TableBrowserSessionState] = [:],
        plotDocuments: [WorkbenchPlotDocument] = [],
        jobs: [String: WorkbenchJob] = [:],
        activeJobIDsByTab: [String: String] = [:],
        runProductGroups: [RunProductGroup] = [],
        taskCatalog: [TaskCatalogEntry] = [],
        taskExecutionMatrixRows: [TaskExecutionMatrixRow] = [],
        activeTaskID: String = "imager",
        taskUISchemas: [String: TaskUISchema] = [:],
        genericTaskValues: [String: [String: String]] = [:],
        genericTaskToggles: [String: [String: Bool]] = [:],
        genericTaskConfirmations: [String: Bool] = [:],
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
        self.measurementSetPlotResultCache = measurementSetPlotResultCache
        self.imageExplorers = imageExplorers
        self.tableBrowsers = tableBrowsers
        self.plotDocuments = plotDocuments
        self.jobs = jobs
        self.activeJobIDsByTab = activeJobIDsByTab
        self.runProductGroups = runProductGroups
        self.taskCatalog = taskCatalog
        self.taskExecutionMatrixRows = taskExecutionMatrixRows
        self.activeTaskID = activeTaskID
        self.taskUISchemas = taskUISchemas
        self.genericTaskValues = genericTaskValues
        self.genericTaskToggles = genericTaskToggles
        self.genericTaskConfirmations = genericTaskConfirmations
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
    public var explorerTabs: [DebugExplorerTabSnapshot]
    public var activeTab: String
    public var taskState: TaskRunState
    public var taskRequest: DirtyImagingTaskParameters?
    public var taskDiagnostics: [String]
    public var taskOutputPaths: [String]
    public var taskCatalog: [TaskCatalogEntry]
    public var aiProposalStates: [String: AIProposalState]
    public var pythonOwner: PythonOwner
    public var measurementSetPlots: [String: DebugMeasurementSetPlotSnapshot]
    public var imageExplorers: [String: DebugImageExplorerSnapshot]
    public var tableBrowsers: [String: DebugTableBrowserSnapshot]
    public var workbenchPlots: [DebugWorkbenchPlotSnapshot]
    public var jobs: [DebugWorkbenchJobSnapshot]
    public var activeJobIDsByTab: [String: String]
    public var runningJobCount: Int
    public var runProductGroups: [DebugRunProductGroupSnapshot]
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
        explorerTabs = state.tabs
            .filter { $0.kind == .datasetExplorer }
            .map { DebugExplorerTabSnapshot(tab: $0, state: state) }
        activeTab = state.tabs.first { $0.id == state.activeTabID }?.title ?? state.activeTabID
        taskState = state.taskRun.state
        taskRequest = state.dirtyImagingTaskParameters
        taskDiagnostics = state.taskRun.diagnostics
        taskOutputPaths = state.taskRun.outputPaths
        taskCatalog = state.taskCatalog
        aiProposalStates = Dictionary(
            uniqueKeysWithValues: state.aiProposals.map { ($0.id, $0.state) }
        )
        pythonOwner = state.python.owner
        measurementSetPlots = Dictionary(
            uniqueKeysWithValues: state.measurementSetPlots.map { datasetID, plotState in
                (datasetID, DebugMeasurementSetPlotSnapshot(plotState: plotState))
            }
        )
        imageExplorers = Dictionary(
            uniqueKeysWithValues: state.imageExplorers.map { datasetID, explorerState in
                (datasetID, DebugImageExplorerSnapshot(state: explorerState))
            }
        )
        tableBrowsers = Dictionary(
            uniqueKeysWithValues: state.tableBrowsers.map { datasetID, browserState in
                (datasetID, DebugTableBrowserSnapshot(state: browserState))
            }
        )
        workbenchPlots = state.plotDocuments.map(DebugWorkbenchPlotSnapshot.init(plot:))
        jobs = state.jobs.values
            .sorted { $0.id < $1.id }
            .map(DebugWorkbenchJobSnapshot.init(job:))
        activeJobIDsByTab = state.activeJobIDsByTab
        runningJobCount = state.jobs.values.filter { $0.status == .running || $0.status == .pending }.count
        runProductGroups = state.runProductGroups.map(DebugRunProductGroupSnapshot.init(group:))
        processingHistoryEvents = state.history.map(\.title)
        commandQuery = state.commandQuery
        lastErrors = state.lastErrors
        interfaceFontSize = state.interfaceFontSize
    }
}

public struct DebugExplorerTabSnapshot: Codable, Equatable {
    public var id: String
    public var title: String
    public var datasetID: String?
    public var datasetName: String?
    public var datasetKind: DatasetKind?
    public var path: String?

    public init(tab: WorkbenchTab, state: WorkbenchState) {
        id = tab.id
        title = tab.title
        datasetID = tab.datasetID
        let dataset = tab.datasetID.flatMap { datasetID in
            state.project.datasets.first { $0.id == datasetID }
        }
        datasetName = dataset?.name
        datasetKind = dataset?.kind
        path = dataset?.path
    }
}

public struct DebugRunProductGroupSnapshot: Codable, Equatable {
    public var runID: String
    public var title: String
    public var sourceDatasetID: String
    public var sourcePath: String
    public var products: [DebugRunProductReferenceSnapshot]
    public var diagnostics: [String]

    public init(group: RunProductGroup) {
        runID = group.runID
        title = group.title
        sourceDatasetID = group.sourceDatasetID
        sourcePath = group.sourcePath
        products = group.products.map(DebugRunProductReferenceSnapshot.init(product:))
        diagnostics = group.diagnostics
    }
}

public struct DebugRunProductReferenceSnapshot: Codable, Equatable {
    public var id: String
    public var artifactKind: String
    public var label: String
    public var path: String
    public var datasetID: String?
    public var exists: Bool
    public var previewPngPath: String?
    public var previewPngExists: Bool

    public init(product: RunProductReference) {
        id = product.id
        artifactKind = product.artifactKind
        label = product.label
        path = product.path
        datasetID = product.datasetID
        exists = product.exists
        previewPngPath = product.previewPngPath
        previewPngExists = product.previewPngExists
    }
}

public struct DebugWorkbenchJobSnapshot: Codable, Equatable {
    public var id: String
    public var tabID: String
    public var kind: WorkbenchJobKind
    public var owner: WorkbenchJobOwner
    public var status: WorkbenchJobStatus
    public var progress: Double
    public var title: String
    public var detail: String
    public var logLines: [String]
    public var resultSummary: String?
    public var error: String?
    public var cancellationRequested: Bool
    public var lastEvent: String

    public init(job: WorkbenchJob) {
        id = job.id
        tabID = job.tabID
        kind = job.kind
        owner = job.owner
        status = job.status
        progress = job.progress
        title = job.title
        detail = job.detail
        logLines = job.logLines
        resultSummary = job.resultSummary
        error = job.error
        cancellationRequested = job.cancellationRequested
        lastEvent = job.lastEvent
    }
}

public struct DebugMeasurementSetPlotSnapshot: Codable, Equatable {
    public var preset: MeasurementSetExplorerPlotPreset
    public var status: MeasurementSetPlotStatus
    public var selectedField: String?
    public var selectedSpectralWindow: String?
    public var selectedChannelSelection: String?
    public var selectedTimerange: String?
    public var selectedUVRange: String?
    public var selectedAntenna: String?
    public var selectedScan: String?
    public var selectedCorrelation: String?
    public var selectedArray: String?
    public var selectedObservation: String?
    public var selectedIntent: String?
    public var selectedFeed: String?
    public var selectedMSSelect: String?
    public var dataColumn: String
    public var avgChannel: UInt64?
    public var avgTime: Double?
    public var avgScan: Bool
    public var avgField: Bool
    public var avgBaseline: Bool
    public var avgAntenna: Bool
    public var avgSPW: Bool
    public var scalarAverage: Bool
    public var maxPlotPoints: UInt64
    public var lastError: String?
    public var resultPreset: MeasurementSetExplorerPlotPreset?
    public var title: String?
    public var xAxis: PlotAxisSummary?
    public var yAxis: PlotAxisSummary?
    public var renderedPointCount: UInt64?
    public var seriesCount: Int?
    public var plotDocumentLayerCount: Int?
    public var plotDocumentPanelCount: Int?
    public var plotDocumentPayloadStrategies: [String]
    public var imageByteCount: Int?
    public var renderer: String?
    public var diagnostics: [String]

    public init(plotState: MeasurementSetExplorerPlotState) {
        preset = plotState.preset
        status = plotState.status
        selectedField = plotState.selectedField
        selectedSpectralWindow = plotState.selectedSpectralWindow
        selectedChannelSelection = plotState.selectedChannelSelection
        selectedTimerange = plotState.selectedTimerange
        selectedUVRange = plotState.selectedUVRange
        selectedAntenna = plotState.selectedAntenna
        selectedScan = plotState.selectedScan
        selectedCorrelation = plotState.selectedCorrelation
        selectedArray = plotState.selectedArray
        selectedObservation = plotState.selectedObservation
        selectedIntent = plotState.selectedIntent
        selectedFeed = plotState.selectedFeed
        selectedMSSelect = plotState.selectedMSSelect
        dataColumn = plotState.dataColumn
        avgChannel = plotState.avgChannel
        avgTime = plotState.avgTime
        avgScan = plotState.avgScan
        avgField = plotState.avgField
        avgBaseline = plotState.avgBaseline
        avgAntenna = plotState.avgAntenna
        avgSPW = plotState.avgSPW
        scalarAverage = plotState.scalarAverage
        maxPlotPoints = plotState.maxPlotPoints
        lastError = plotState.lastError
        let visibleResult = plotState.result?.matches(plotState: plotState) == true ? plotState.result : nil
        resultPreset = visibleResult?.preset
        title = visibleResult?.title
        xAxis = visibleResult?.xAxis
        yAxis = visibleResult?.yAxis
        renderedPointCount = visibleResult?.renderedPointCount
        seriesCount = visibleResult?.series.count
        plotDocumentLayerCount = visibleResult?.plotDocument.allLayers.count
        plotDocumentPanelCount = visibleResult?.plotDocument.panels.count
        plotDocumentPayloadStrategies = Self.uniquePayloadStrategies(visibleResult?.plotDocument.allLayers ?? [])
        imageByteCount = visibleResult?.imageBytes.count
        renderer = visibleResult?.renderer
        diagnostics = visibleResult?.diagnostics ?? []
    }

    private static func uniquePayloadStrategies(_ layers: [WorkbenchPlotLayer]) -> [String] {
        var seen = Set<String>()
        var ordered: [String] = []
        for layer in layers {
            let strategy = layer.dataProfile.strategy.rawValue
            guard seen.insert(strategy).inserted else { continue }
            ordered.append(strategy)
        }
        return ordered
    }
}

public struct DebugImageExplorerSnapshot: Codable, Equatable {
    public var status: ExplorerSessionStatus
    public var activeView: String?
    public var selectedView: String
    public var moviePlaying: Bool
    public var movieAxis: Int?
    public var movieFramesPerSecond: Double
    public var shape: [Int]
    public var planeSize: String?
    public var profileSampleCount: Int?
    public var maskCount: Int
    public var savedRegionCount: Int
    public var lastError: String?

    public init(state: ImageExplorerSessionState) {
        status = state.status
        selectedView = state.selectedView
        moviePlaying = state.moviePlaying
        movieAxis = state.movieAxis
        movieFramesPerSecond = state.movieFramesPerSecond
        activeView = state.snapshot?.activeView
        shape = state.snapshot?.shape ?? []
        if let plane = state.snapshot?.plane {
            planeSize = "\(plane.width)x\(plane.height)"
        } else {
            planeSize = nil
        }
        profileSampleCount = state.snapshot?.profile?.samples.count
        maskCount = state.snapshot?.maskNames.count ?? 0
        savedRegionCount = state.snapshot?.savedRegionNames.count ?? 0
        lastError = state.lastError
    }
}

public struct DebugTableBrowserSnapshot: Codable, Equatable {
    public var status: ExplorerSessionStatus
    public var view: String?
    public var selectedView: String
    public var focus: String?
    public var tablePath: String?
    public var breadcrumbDepth: Int
    public var contentLineCount: Int
    public var inspectorTitle: String?
    public var lastError: String?

    public init(state: TableBrowserSessionState) {
        status = state.status
        selectedView = state.selectedView
        view = state.snapshot?.view
        focus = state.snapshot?.focus
        tablePath = state.snapshot?.tablePath
        breadcrumbDepth = state.snapshot?.breadcrumb.count ?? 0
        contentLineCount = state.snapshot?.contentLines.count ?? 0
        inspectorTitle = state.snapshot?.inspector?.title
        lastError = state.lastError
    }
}
