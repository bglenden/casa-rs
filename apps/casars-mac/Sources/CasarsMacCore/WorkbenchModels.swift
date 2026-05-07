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

public struct ImageExplorerSnapshot: Codable, Equatable {
    public struct Capabilities: Codable, Equatable {
        public var renderablePlane: Bool
        public var worldCoordsAvailable: Bool
        public var pixelOnlyMode: Bool
        public var nonDisplayAxisSelectors: Bool
        public var maskPresent: Bool

        enum CodingKeys: String, CodingKey {
            case renderablePlane = "renderable_plane"
            case worldCoordsAvailable = "world_coords_available"
            case pixelOnlyMode = "pixel_only_mode"
            case nonDisplayAxisSelectors = "non_display_axis_selectors"
            case maskPresent = "mask_present"
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
        public var maskedOrNonFiniteCount: Int

        enum CodingKeys: String, CodingKey {
            case width
            case height
            case pixelsU8 = "pixels_u8"
            case clipMin = "clip_min"
            case clipMax = "clip_max"
            case dataMin = "data_min"
            case dataMax = "data_max"
            case valueUnit = "value_unit"
            case maskedOrNonFiniteCount = "masked_or_non_finite_count"
        }
    }

    public struct Profile: Codable, Equatable {
        public struct Sample: Codable, Equatable {
            public var sampleIndex: Int
            public var pixelIndex: Int
            public var value: Double
            public var finite: Bool

            enum CodingKeys: String, CodingKey {
                case sampleIndex = "sample_index"
                case pixelIndex = "pixel_index"
                case value
                case finite
            }
        }

        public var axis: Int
        public var axisName: String
        public var axisUnit: String
        public var valueUnit: String
        public var samples: [Sample]

        enum CodingKeys: String, CodingKey {
            case axis
            case axisName = "axis_name"
            case axisUnit = "axis_unit"
            case valueUnit = "value_unit"
            case samples
        }
    }

    public struct Region: Codable, Equatable {
        public var label: String
        public var shapeCount: Int
        public var closedShapeCount: Int
        public var editing: Bool

        enum CodingKeys: String, CodingKey {
            case label
            case shapeCount = "shape_count"
            case closedShapeCount = "closed_shape_count"
            case editing
        }
    }

    public var statusLine: String
    public var activeView: String
    public var shape: [Int]
    public var inspectorLines: [String]
    public var contentLines: [String]
    public var plane: Plane?
    public var profile: Profile?
    public var region: Region?
    public var savedRegionNames: [String]
    public var maskNames: [String]
    public var capabilities: Capabilities

    enum CodingKeys: String, CodingKey {
        case statusLine = "status_line"
        case activeView = "active_view"
        case shape
        case inspectorLines = "inspector_lines"
        case contentLines = "content_lines"
        case plane
        case profile
        case region
        case savedRegionNames = "saved_region_names"
        case maskNames = "mask_names"
        case capabilities
    }
}

public struct ImageExplorerSessionState: Codable, Equatable {
    public var datasetID: String
    public var selectedView: String
    public var status: ExplorerSessionStatus
    public var lastError: String?
    public var snapshot: ImageExplorerSnapshot?
}

public struct TableBrowserSnapshot: Codable, Equatable {
    public struct Breadcrumb: Codable, Equatable {
        public var label: String
        public var path: String
    }

    public struct Inspector: Codable, Equatable {
        public var title: String
        public var renderedLines: [String]

        enum CodingKeys: String, CodingKey {
            case title
            case renderedLines = "rendered_lines"
        }
    }

    public var view: String
    public var focus: String
    public var tablePath: String
    public var breadcrumb: [Breadcrumb]
    public var statusLine: String
    public var contentLines: [String]
    public var inspector: Inspector?

    enum CodingKeys: String, CodingKey {
        case view
        case focus
        case tablePath = "table_path"
        case breadcrumb
        case statusLine = "status_line"
        case contentLines = "content_lines"
        case inspector
    }
}

public struct TableBrowserSessionState: Codable, Equatable {
    public var datasetID: String
    public var selectedView: String
    public var status: ExplorerSessionStatus
    public var lastError: String?
    public var snapshot: TableBrowserSnapshot?
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
    public var shape: [Int]
    public var planeSize: String?
    public var profileSampleCount: Int?
    public var maskCount: Int
    public var savedRegionCount: Int
    public var lastError: String?

    public init(state: ImageExplorerSessionState) {
        status = state.status
        selectedView = state.selectedView
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
