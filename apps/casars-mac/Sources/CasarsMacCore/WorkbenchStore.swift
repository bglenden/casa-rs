import Foundation
import AppKit
import CasarsFrontendServices
import CryptoKit
import OSLog

private let datasetSelectionLogger = Logger(
    subsystem: "org.casa-rs.casars-mac",
    category: "DatasetSelection"
)

private let measurementSetPlotLogger = Logger(
    subsystem: "org.casa-rs.casars-mac",
    category: "MeasurementSetPlot"
)

private let denseScatterPointThreshold = 8_000

private final class AssistantActionCancellation: @unchecked Sendable {
    private let lock = NSLock()
    private var cancelled = false

    func cancel() { lock.withLock { cancelled = true } }
    var isCancelled: Bool { lock.withLock { cancelled } }
}

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

public protocol ApplicationCatalogClient {
    func loadApplicationCatalog() throws -> [ApplicationCatalogEntry]
}

public struct UniFFIApplicationCatalogClient: ApplicationCatalogClient {
    public init() {}

    public func loadApplicationCatalog() throws -> [ApplicationCatalogEntry] {
        try CasarsFrontendServices.applicationCatalog().applications.filter(\.showInSwift)
    }
}

/// Bootstrap adapters used before the notebook prototype is visible. They
/// deliberately expose no production catalog.
private struct PrototypeApplicationCatalogClient: ApplicationCatalogClient {
    func loadApplicationCatalog() throws -> [ApplicationCatalogEntry] { [] }
}

private struct NotebookPrototypeBoundaryViolation: Error, CustomStringConvertible {
    let boundary: String

    var description: String {
        "The notebook prototype runtime denied the \(boundary) boundary."
    }
}

/// Process-local evidence for the fixture runtime's fail-closed production
/// adapters. This is package-scoped test support, never persisted or exposed as
/// a provider contract.
package enum NotebookPrototypeBoundaryAudit {
    private static let lock = NSLock()
    nonisolated(unsafe) private static var invocations: [String] = []

    package static func reset() {
        lock.withLock { invocations.removeAll(keepingCapacity: true) }
    }

    package static func record(_ boundary: String) {
        lock.withLock { invocations.append(boundary) }
    }

    package static var count: Int {
        lock.withLock { invocations.count }
    }
}

/// Fail-closed production adapters installed only in the immutable notebook
/// prototype runtime. A store guard should reject every route before one of
/// these methods is reached; throwing here is the final containment layer.
private struct NotebookPrototypeDeniedProductionClient:
    ProjectProbeClient,
    DemoProjectClient,
    MeasurementSetPlotClient,
    ImageExplorerClient,
    TableBrowserClient,
    GenericTaskClient,
    TaskUISchemaClient,
    SurfaceParameterClient
{
    private func denied<T>(_ boundary: String) throws -> T {
        NotebookPrototypeBoundaryAudit.record(boundary)
        throw NotebookPrototypeBoundaryViolation(boundary: boundary)
    }

    func probeProject(path: String) throws -> ProjectFixtureProbe {
        try denied("project probe")
    }

    func probePath(path: String) throws -> DatasetSummary? {
        try denied("dataset probe")
    }

    func createDemoProject() throws -> ProjectFixtureProbe {
        try denied("demo project")
    }

    func cleanupDemoProject(rootPath: String) {
        // A prototype runtime never creates a demo root, so cleanup has
        // nothing to do and intentionally performs no filesystem operation.
    }

    func buildPlot(request: MeasurementSetPlotBuildRequest) throws -> MeasurementSetPlotResultSummary {
        try denied("MeasurementSet plot")
    }

    func buildSnapshot(request: ImageExplorerSnapshotRequest) throws -> ImageExplorerSnapshot {
        try denied("image explorer")
    }

    func buildSnapshot(request: TableBrowserSnapshotRequest) throws -> TableBrowserSnapshot {
        try denied("table browser")
    }

    func buildCellWindow(request: TableBrowserCellWindowRequest) throws -> TableBrowserCellWindowSnapshot {
        try denied("table cell window")
    }

    func buildCellValue(request: TableBrowserCellValueRequest) throws -> String {
        try denied("table cell value")
    }

    func startTask(
        request: GenericTaskRequest,
        eventHandler: @escaping (GenericTaskEvent) -> Void
    ) throws -> TaskExecution {
        try denied("task process")
    }

    func loadTaskUISchema(taskID: String) throws -> TaskUiSchema {
        try denied("task schema")
    }

    func loadBundle(surfaceID: String) throws -> SurfaceParameterBundle {
        try denied("parameter bundle")
    }

    func defaults(surfaceID: String) throws -> SurfaceParameterSnapshot {
        try denied("parameter defaults")
    }

    func last(surfaceID: String, workspace: String, successful: Bool) throws -> SurfaceParameterSnapshot? {
        try denied("parameter history")
    }

    func load(surfaceID: String, profileTOML: String, sourcePath: String) throws -> SurfaceParameterSnapshot {
        try denied("parameter profile")
    }

    func resolve(
        surfaceID: String,
        baseSource: SurfaceParameterBaseSource,
        profileTOML: String?,
        profilePath: String?,
        context: SurfaceParameterPatch,
        override: SurfaceParameterPatch
    ) throws -> SurfaceParameterSnapshot {
        try denied("parameter resolution")
    }

    func save(
        surfaceID: String,
        values: [String: SurfaceParameterValue],
        destinationPath: String
    ) throws -> SurfaceParameterWriteResult {
        try denied("parameter save")
    }

    func runSafety(
        surfaceID: String,
        values: [String: SurfaceParameterValue]
    ) throws -> SurfaceRunSafety {
        try denied("parameter run safety")
    }

    func providerInvocation(
        surfaceID: String,
        values: [String: SurfaceParameterValue]
    ) throws -> SurfaceProviderInvocation {
        try denied("provider invocation")
    }
}

/// Package-only injection point for containment tests. Production app code
/// uses `denied`; none of these fixture-runtime dependencies become a public
/// or persisted contract.
package struct NotebookPrototypeRuntimeDependencies {
    let probeClient: ProjectProbeClient
    let demoProjectClient: DemoProjectClient
    let plotClient: MeasurementSetPlotClient
    let imageExplorerClient: ImageExplorerClient
    let tableBrowserClient: TableBrowserClient
    let genericTaskClient: GenericTaskClient
    let taskUISchemaClient: TaskUISchemaClient
    let surfaceParameterClient: SurfaceParameterClient

    package init(
        probeClient: ProjectProbeClient,
        demoProjectClient: DemoProjectClient,
        plotClient: MeasurementSetPlotClient,
        imageExplorerClient: ImageExplorerClient,
        tableBrowserClient: TableBrowserClient,
        genericTaskClient: GenericTaskClient,
        taskUISchemaClient: TaskUISchemaClient,
        surfaceParameterClient: SurfaceParameterClient
    ) {
        self.probeClient = probeClient
        self.demoProjectClient = demoProjectClient
        self.plotClient = plotClient
        self.imageExplorerClient = imageExplorerClient
        self.tableBrowserClient = tableBrowserClient
        self.genericTaskClient = genericTaskClient
        self.taskUISchemaClient = taskUISchemaClient
        self.surfaceParameterClient = surfaceParameterClient
    }

    package static var denied: Self {
        let client = NotebookPrototypeDeniedProductionClient()
        return Self(
            probeClient: client,
            demoProjectClient: client,
            plotClient: client,
            imageExplorerClient: client,
            tableBrowserClient: client,
            genericTaskClient: client,
            taskUISchemaClient: client,
            surfaceParameterClient: client
        )
    }
}

public protocol TaskContextOptionsClient {
    func loadTaskContextOptions(datasetPath: String) throws -> TaskContextOptionsEnvelope
}

public struct UniFFITaskContextOptionsClient: TaskContextOptionsClient {
    public init() {}

    public func loadTaskContextOptions(datasetPath: String) throws -> TaskContextOptionsEnvelope {
        try CasarsFrontendServices.taskContextOptions(datasetPath: datasetPath)
    }
}

public protocol TaskUISchemaClient {
    func loadTaskUISchema(taskID: String) throws -> TaskUiSchema
}

public struct UniFFITaskUISchemaClient: TaskUISchemaClient {
    public init() {}

    public func loadTaskUISchema(taskID: String) throws -> TaskUiSchema {
        try CasarsFrontendServices.taskUiSchema(surfaceId: taskID)
    }
}

public protocol DemoProjectClient {
    func createDemoProject() throws -> ProjectFixtureProbe
    func cleanupDemoProject(rootPath: String)
}

public struct TutorialDemoProjectClient: DemoProjectClient {
    public init() {}

    public func createDemoProject() throws -> ProjectFixtureProbe {
        let fileManager = FileManager.default
        let root = fileManager.temporaryDirectory
            .appendingPathComponent("casars-mac-tutorial-demo-\(UUID().uuidString)", isDirectory: true)
        try fileManager.createDirectory(at: root, withIntermediateDirectories: true)

        do {
            let staged = try stageTutorialDatasets(in: root, fileManager: fileManager)
            guard !staged.isEmpty else {
                throw DemoProjectError.noTutorialDatasets(tutorialRootCandidates().map(\.path))
            }
            let probe = try UniFFIProjectProbeClient().probeProject(path: root.path)
            var project = probe.project
            project.name = "TW Hya Tutorial Demo"
            project.source = .probed
            let diagnostics = probe.diagnostics + staged.map { "Staged tutorial dataset: \($0)" }
            return ProjectFixtureProbe(project: project, diagnostics: diagnostics)
        } catch {
            try? fileManager.removeItem(at: root)
            throw error
        }
    }

    public func cleanupDemoProject(rootPath: String) {
        guard !rootPath.isEmpty else { return }
        try? FileManager.default.removeItem(atPath: rootPath)
    }

    private func stageTutorialDatasets(in root: URL, fileManager: FileManager) throws -> [String] {
        guard let tutorialRoot = tutorialRootCandidates().first(where: { fileManager.fileExists(atPath: $0.path) }) else {
            return []
        }

        var staged: [String] = []
        let twhyaRoot = tutorialRoot
            .appendingPathComponent("tutorial-parity/alma/first-look/twhya", isDirectory: true)

        let calibratedMS = twhyaRoot.appendingPathComponent("twhya_calibrated.ms.tar")
        if fileManager.fileExists(atPath: calibratedMS.path) {
            try extractTarArchive(calibratedMS, into: root)
            staged.append("alma/first-look/twhya/calibrated-ms")

            let antennaTable = root
                .appendingPathComponent("twhya_calibrated.ms", isDirectory: true)
                .appendingPathComponent("ANTENNA", isDirectory: true)
            if fileManager.fileExists(atPath: antennaTable.path) {
                let copy = root.appendingPathComponent("twhya_calibrated_ANTENNA.table", isDirectory: true)
                try copyDirectory(from: antennaTable, to: copy, fileManager: fileManager)
                staged.append("alma/first-look/twhya/calibrated-ms/ANTENNA")
            }
        }

        for imageName in ["twhya_cont.image", "twhya_n2hp.image"] {
            let source = twhyaRoot.appendingPathComponent(imageName, isDirectory: true)
            guard fileManager.fileExists(atPath: source.path) else { continue }
            let destination = root.appendingPathComponent(imageName, isDirectory: true)
            try copyDirectory(from: source, to: destination, fileManager: fileManager)
            staged.append("alma/first-look/twhya/\(imageName)")
        }

        return staged
    }

    private func tutorialRootCandidates() -> [URL] {
        var candidates: [URL] = []
        if let override = ProcessInfo.processInfo.environment["CASA_RS_TUTORIAL_DATA_ROOT"], !override.isEmpty {
            candidates.append(URL(fileURLWithPath: override, isDirectory: true))
        }
        candidates.append(
            URL(fileURLWithPath: NSHomeDirectory(), isDirectory: true)
                .appendingPathComponent("SoftwareProjects/casa-tutorial-data", isDirectory: true)
        )
        return candidates
    }

    private func extractTarArchive(_ archive: URL, into root: URL) throws {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/tar")
        process.arguments = ["-xf", archive.path, "-C", root.path]
        try process.run()
        process.waitUntilExit()
        guard process.terminationStatus == 0 else {
            throw DemoProjectError.tarFailed(archive.path, process.terminationStatus)
        }
    }

    private func copyDirectory(from source: URL, to destination: URL, fileManager: FileManager) throws {
        if fileManager.fileExists(atPath: destination.path) {
            try fileManager.removeItem(at: destination)
        }
        try fileManager.copyItem(at: source, to: destination)
    }
}

public enum DemoProjectError: Error, Equatable, CustomStringConvertible {
    case noTutorialDatasets([String])
    case tarFailed(String, Int32)

    public var description: String {
        switch self {
        case .noTutorialDatasets(let roots):
            "No local tutorial demo datasets found. Stage TW Hya tutorial data under one of: \(roots.joined(separator: ", "))"
        case .tarFailed(let path, let status):
            "Failed to extract tutorial archive \(path) with tar exit status \(status)"
        }
    }
}

public struct MeasurementSetPlotBuildRequest: Equatable {
    public var datasetPath: String
    public var preset: MeasurementSetExplorerPlotPreset
    public var field: String?
    public var spectralWindow: String?
    public var timerange: String?
    public var uvRange: String?
    public var antenna: String?
    public var scan: String?
    public var correlation: String?
    public var array: String?
    public var observation: String?
    public var intent: String?
    public var feed: String?
    public var msselect: String?
    public var dataColumn: String
    public var colorBy: MeasurementSetPlotColorAxis
    public var avgChannel: UInt64?
    public var avgTime: Double?
    public var avgScan: Bool
    public var avgField: Bool
    public var avgBaseline: Bool
    public var avgAntenna: Bool
    public var avgSPW: Bool
    public var scalarAverage: Bool
    public var iterationAxis: MeasurementSetPlotIterationAxis?
    public var width: UInt32
    public var height: UInt32
    public var maxPlotPoints: UInt64

    public init(
        datasetPath: String,
        preset: MeasurementSetExplorerPlotPreset,
        field: String?,
        spectralWindow: String?,
        timerange: String? = nil,
        uvRange: String? = nil,
        antenna: String? = nil,
        scan: String? = nil,
        correlation: String?,
        array: String? = nil,
        observation: String? = nil,
        intent: String? = nil,
        feed: String? = nil,
        msselect: String? = nil,
        dataColumn: String,
        colorBy: MeasurementSetPlotColorAxis = .field,
        avgChannel: UInt64? = nil,
        avgTime: Double? = nil,
        avgScan: Bool = false,
        avgField: Bool = false,
        avgBaseline: Bool = false,
        avgAntenna: Bool = false,
        avgSPW: Bool = false,
        scalarAverage: Bool = false,
        iterationAxis: MeasurementSetPlotIterationAxis? = nil,
        width: UInt32 = 960,
        height: UInt32 = 600,
        maxPlotPoints: UInt64 = WorkbenchState.defaultMeasurementSetPlotMaxPoints
    ) {
        self.datasetPath = datasetPath
        self.preset = preset
        self.field = field
        self.spectralWindow = spectralWindow
        self.timerange = timerange
        self.uvRange = uvRange
        self.antenna = antenna
        self.scan = scan
        self.correlation = correlation
        self.array = array
        self.observation = observation
        self.intent = intent
        self.feed = feed
        self.msselect = msselect
        self.dataColumn = dataColumn
        self.colorBy = colorBy
        self.avgChannel = avgChannel
        self.avgTime = avgTime
        self.avgScan = avgScan
        self.avgField = avgField
        self.avgBaseline = avgBaseline
        self.avgAntenna = avgAntenna
        self.avgSPW = avgSPW
        self.scalarAverage = scalarAverage
        self.iterationAxis = iterationAxis
        self.width = width
        self.height = height
        self.maxPlotPoints = maxPlotPoints
    }
}

public struct MeasurementSetSummaryBuildRequest: Equatable {
    public var datasetPath: String
    public var format: String
    public var field: String?
    public var spectralWindow: String?
    public var timerange: String?
    public var uvRange: String?
    public var antenna: String?
    public var scan: String?
    public var correlation: String?
    public var array: String?
    public var observation: String?
    public var intent: String?
    public var feed: String?
    public var msselect: String?

    public init(
        datasetPath: String,
        format: String = "text",
        field: String? = nil,
        spectralWindow: String? = nil,
        timerange: String? = nil,
        uvRange: String? = nil,
        antenna: String? = nil,
        scan: String? = nil,
        correlation: String? = nil,
        array: String? = nil,
        observation: String? = nil,
        intent: String? = nil,
        feed: String? = nil,
        msselect: String? = nil
    ) {
        self.datasetPath = datasetPath
        self.format = format
        self.field = field
        self.spectralWindow = spectralWindow
        self.timerange = timerange
        self.uvRange = uvRange
        self.antenna = antenna
        self.scan = scan
        self.correlation = correlation
        self.array = array
        self.observation = observation
        self.intent = intent
        self.feed = feed
        self.msselect = msselect
    }
}

public struct MeasurementSetSummaryResultSummary: Equatable {
    public var datasetPath: String
    public var format: String
    public var summaryText: String
    public var selectionSummary: String
    public var diagnostics: [String]

    public init(result: CasarsFrontendServices.MeasurementSetSummaryResult) {
        datasetPath = result.datasetPath
        format = result.format
        summaryText = result.summaryText
        selectionSummary = result.selectionSummary
        diagnostics = result.diagnostics
    }
}

public protocol MeasurementSetSummaryClient {
    func buildSummary(request: MeasurementSetSummaryBuildRequest) throws -> MeasurementSetSummaryResultSummary
}

public protocol MeasurementSetPlotClient {
    func buildPlot(request: MeasurementSetPlotBuildRequest) throws -> MeasurementSetPlotResultSummary
}

public protocol MeasurementSetMetadataClient {
    func probeUVRange(datasetPath: String) throws -> MeasurementSetUVRangeSummary
    func probeTimeRange(datasetPath: String) throws -> MeasurementSetTimeRangeSummary
}

package struct NotebookVisualizationImage {
    package let data: Data
    package let fileExtension: String
    package let mediaType: String
    package let width: UInt32
    package let height: UInt32
    package let renderer: String

    package init(
        data: Data,
        fileExtension: String,
        mediaType: String,
        width: UInt32,
        height: UInt32,
        renderer: String
    ) {
        self.data = data
        self.fileExtension = fileExtension
        self.mediaType = mediaType
        self.width = width
        self.height = height
        self.renderer = renderer
    }
}

public struct UniFFIMeasurementSetPlotClient: MeasurementSetPlotClient {
    public init() {}

    public func buildPlot(request: MeasurementSetPlotBuildRequest) throws -> MeasurementSetPlotResultSummary {
        let ffiStartedAt = Date()
        let result = try CasarsFrontendServices.buildMeasurementSetPlot(
            request: CasarsFrontendServices.MeasurementSetPlotRequest(
                datasetPath: request.datasetPath,
                preset: CasarsFrontendServices.MeasurementSetPlotPreset(preset: request.preset),
                field: request.field,
                spectralWindow: request.spectralWindow,
                timerange: request.timerange,
                uvrange: request.uvRange,
                antenna: request.antenna,
                scan: request.scan,
                correlation: request.correlation,
                array: request.array,
                observation: request.observation,
                intent: request.intent,
                feed: request.feed,
                msselect: request.msselect,
                dataColumn: request.dataColumn,
                colorBy: request.colorBy.protocolValue,
                avgchannel: request.avgChannel,
                avgtime: request.avgTime,
                avgscan: request.avgScan,
                avgfield: request.avgField,
                avgbaseline: request.avgBaseline,
                avgantenna: request.avgAntenna,
                avgspw: request.avgSPW,
                scalar: request.scalarAverage,
                iteraxis: request.iterationAxis?.protocolValue,
                width: request.width,
                height: request.height,
                maxPlotPoints: request.maxPlotPoints
            )
        )
        let ffiElapsed = Date().timeIntervalSince(ffiStartedAt)
        let summaryStartedAt = Date()
        var summary = MeasurementSetPlotResultSummary(result: result)
        let summaryElapsed = Date().timeIntervalSince(summaryStartedAt)
        let totalElapsed = ffiElapsed + summaryElapsed
        let diagnostic = String(
            format: "swift timing: ffi=%.0f ms, summary=%.0f ms, total=%.0f ms",
            ffiElapsed * 1000,
            summaryElapsed * 1000,
            totalElapsed * 1000
        )
        summary.diagnostics.append(diagnostic)
        measurementSetPlotLogger.info("\(diagnostic, privacy: .public)")
        return summary
    }
}

public struct UniFFIMeasurementSetSummaryClient: MeasurementSetSummaryClient {
    public init() {}

    public func buildSummary(request: MeasurementSetSummaryBuildRequest) throws -> MeasurementSetSummaryResultSummary {
        let result = try CasarsFrontendServices.buildMeasurementSetSummary(
            request: CasarsFrontendServices.MeasurementSetSummaryRequest(
                datasetPath: request.datasetPath,
                format: request.format,
                field: request.field,
                spectralWindow: request.spectralWindow,
                timerange: request.timerange,
                uvrange: request.uvRange,
                antenna: request.antenna,
                scan: request.scan,
                correlation: request.correlation,
                array: request.array,
                observation: request.observation,
                intent: request.intent,
                feed: request.feed,
                msselect: request.msselect
            )
        )
        return MeasurementSetSummaryResultSummary(result: result)
    }
}

public struct UniFFIMeasurementSetMetadataClient: MeasurementSetMetadataClient {
    public init() {}

    public func probeUVRange(datasetPath: String) throws -> MeasurementSetUVRangeSummary {
        let probe = try CasarsFrontendServices.probeMeasurementSetUvRange(datasetPath: datasetPath)
        return MeasurementSetUVRangeSummary(
            minMeters: probe.minMeters,
            maxMeters: probe.maxMeters,
            minKiloLambda: probe.minKilolambda,
            maxKiloLambda: probe.maxKilolambda,
            rowCount: probe.rowCount
        )
    }

    public func probeTimeRange(datasetPath: String) throws -> MeasurementSetTimeRangeSummary {
        let probe = try CasarsFrontendServices.probeMeasurementSetTimeRange(datasetPath: datasetPath)
        return MeasurementSetTimeRangeSummary(
            minSeconds: probe.minSeconds,
            maxSeconds: probe.maxSeconds,
            rowCount: probe.rowCount
        )
    }
}

public enum MeasurementSetUVRangeFormatter {
    public static func formatMeters(_ value: Double) -> String {
        format(value)
    }

    public static func formatKiloLambda(_ value: Double) -> String {
        format(value)
    }

    private static func format(_ value: Double) -> String {
        guard value.isFinite else {
            return ""
        }
        if abs(value) >= 1_000 {
            return String(format: "%.0f", value)
        }
        if abs(value) >= 10 {
            return String(format: "%.2f", value)
        }
        return String(format: "%.3g", value)
    }
}

public protocol ImageExplorerClient {
    func buildSnapshot(request: ImageExplorerSnapshotRequest) throws -> ImageExplorerSnapshot
}

public struct UniFFIImageExplorerClient: ImageExplorerClient {
    public init() {}

    public func buildSnapshot(request: ImageExplorerSnapshotRequest) throws -> ImageExplorerSnapshot {
        try CasarsFrontendServices.buildImageExplorerSnapshot(request: request)
    }
}

public protocol TableBrowserClient {
    func buildSnapshot(request: TableBrowserSnapshotRequest) throws -> TableBrowserSnapshot
    func buildCellWindow(request: TableBrowserCellWindowRequest) throws -> TableBrowserCellWindowSnapshot
    func buildCellValue(request: TableBrowserCellValueRequest) throws -> String
}

public struct UniFFITableBrowserClient: TableBrowserClient {
    public init() {}

    public func buildSnapshot(request: TableBrowserSnapshotRequest) throws -> TableBrowserSnapshot {
        try CasarsFrontendServices.buildTableBrowserSnapshot(request: request)
    }

    public func buildCellWindow(request: TableBrowserCellWindowRequest) throws -> TableBrowserCellWindowSnapshot {
        try CasarsFrontendServices.buildTableBrowserCellWindow(request: request)
    }

    public func buildCellValue(request: TableBrowserCellValueRequest) throws -> String {
        try CasarsFrontendServices.buildTableBrowserCellValue(request: request)
    }
}

private enum AssistantNotebookPinError: LocalizedError {
    case invalidTaskSuggestion(String)

    var errorDescription: String? {
        switch self {
        case let .invalidTaskSuggestion(detail):
            "Cannot add the suggested task to the notebook: \(detail)"
        }
    }
}

private enum WorkbenchRuntimeKind {
    case production
    case notebookPrototype
    case pythonPrototype
    case tutorialPrototype
    case aiPrototype
}

package enum AssistantCorpusRefreshRequest {
    case allLayers
    case projectDocuments

    package func merged(with other: Self) -> Self {
        if self == .allLayers || other == .allLayers { return .allLayers }
        return .projectDocuments
    }
}

extension AssistantCorpusRefreshRequest: Equatable {}

public final class WorkbenchStore: ObservableObject {
    @Published public private(set) var state: WorkbenchState
    @Published package private(set) var pythonNotebookRuntime = NotebookPythonRuntimeState()
    private let runtimeKind: WorkbenchRuntimeKind
    private let probeClient: ProjectProbeClient
    private let demoProjectClient: DemoProjectClient
    private let plotClient: MeasurementSetPlotClient
    private let imageExplorerClient: ImageExplorerClient
    private let tableBrowserClient: TableBrowserClient
    private let genericTaskClient: GenericTaskClient
    private let taskUISchemaClient: TaskUISchemaClient
    private let surfaceParameterClient: SurfaceParameterClient
    private let sessionParameterLifecycleClient: SessionParameterLifecycleClient
    private let taskParameterLifecycleClient: TaskParameterLifecycleClient
    private var notebookPersistenceClient: NotebookPersistenceClient
    private var tutorialPersistenceClient: TutorialPersistenceClient
    private var assistantPersistenceClient: AssistantPersistenceClient
    private let assistantController = AssistantController()
    private let imagerProgressSource: ImagerProgressSource
    private let plotQueue = DispatchQueue(label: "casars.mac.ms-plot-job", qos: .userInitiated, attributes: .concurrent)
    private let tableBrowserQueue = DispatchQueue(label: "casars.mac.tablebrowser-cell-window", qos: .userInitiated)
    private let assistantCorpusQueue = DispatchQueue(label: "casars.mac.assistant-corpus", qos: .utility)
    private var projectCorpusWatcher: ProjectCorpusWatcher?
    private var fullyRefreshedAssistantCorpusProject: String?
    private var activeTaskExecutions: [String: TaskExecution] = [:]
    private var notebookAttemptHandles: [String: NotebookAttemptHandle] = [:]
    private var pythonKernels: [String: PersistentPythonKernel] = [:]
    private var pythonKernelStatuses: [String: NotebookPythonKernelStatus] = [:]
    private var pythonExecutableOverride: String?
    private var measurementSetPlotSurfaceRequests: Set<String> = []
    private var tableBrowserCellWindowGenerations: [String: Int] = [:]
    private var temporaryDemoProjectRoot: String?
    private var lastProjectDiskRefresh: Date = .distantPast

    public init(
        state: WorkbenchState = EmptyWorkbench.makeState(),
        probeClient: ProjectProbeClient = UniFFIProjectProbeClient(),
        demoProjectClient: DemoProjectClient = TutorialDemoProjectClient(),
        plotClient: MeasurementSetPlotClient = UniFFIMeasurementSetPlotClient(),
        imageExplorerClient: ImageExplorerClient = UniFFIImageExplorerClient(),
        tableBrowserClient: TableBrowserClient = UniFFITableBrowserClient(),
        genericTaskClient: GenericTaskClient = ProcessGenericTaskClient(),
        applicationCatalogClient: ApplicationCatalogClient = UniFFIApplicationCatalogClient(),
        taskUISchemaClient: TaskUISchemaClient = UniFFITaskUISchemaClient(),
        surfaceParameterClient: SurfaceParameterClient = UniFFISurfaceParameterClient(),
        sessionParameterLifecycleClient: SessionParameterLifecycleClient = UniFFISessionParameterLifecycleClient(),
        taskParameterLifecycleClient: TaskParameterLifecycleClient = UniFFITaskParameterLifecycleClient(),
        imagerProgressSource: ImagerProgressSource = EmptyImagerProgressSource()
    ) {
        var initialState = state
        if initialState.applicationCatalog.isEmpty {
            do {
                initialState.applicationCatalog = try applicationCatalogClient.loadApplicationCatalog()
            } catch {
                initialState.lastErrors.append("Load task catalog: \(error)")
            }
        }
        self.state = initialState
        if initialState.isAIPrototype {
            runtimeKind = .aiPrototype
        } else if initialState.isTutorialPrototype {
            runtimeKind = .tutorialPrototype
        } else if initialState.isNotebookPrototype {
            runtimeKind = .notebookPrototype
        } else if initialState.isPythonPrototype {
            runtimeKind = .pythonPrototype
        } else {
            runtimeKind = .production
        }
        self.probeClient = probeClient
        self.demoProjectClient = demoProjectClient
        self.plotClient = plotClient
        self.imageExplorerClient = imageExplorerClient
        self.tableBrowserClient = tableBrowserClient
        self.genericTaskClient = genericTaskClient
        self.taskUISchemaClient = taskUISchemaClient
        self.surfaceParameterClient = surfaceParameterClient
        self.sessionParameterLifecycleClient = sessionParameterLifecycleClient
        self.taskParameterLifecycleClient = taskParameterLifecycleClient
        notebookPersistenceClient = UniFFINotebookPersistenceClient()
        tutorialPersistenceClient = UniFFITutorialPersistenceClient()
        assistantPersistenceClient = UniFFIAssistantPersistenceClient()
        self.imagerProgressSource = imagerProgressSource
    }

    package func installNotebookPersistenceClientForTesting(_ client: NotebookPersistenceClient) {
        notebookPersistenceClient = client
    }

    package func installTutorialPersistenceClientForTesting(_ client: TutorialPersistenceClient) {
        tutorialPersistenceClient = client
    }

    package func installAssistantPersistenceClientForTesting(_ client: AssistantPersistenceClient) {
        assistantPersistenceClient = client
    }

    package func installAgentSessionForTesting(
        _ session: AgentSession,
        sessionNonce: String? = nil
    ) {
        assistantController.session?.terminate()
        assistantController.session = session
        if let sessionNonce { assistantController.replaceProjectNonce(sessionNonce) }
        assistantController.activeAgentCommand = state.assistantDiscussion?.activeConversation?.profile.agentCommand
        configureAgentSession(session)
    }

    package func installPythonExecutableForTesting(_ path: String) {
        pythonExecutableOverride = path
    }

    package func installAssistantResponseTimeoutForTesting(_ timeout: TimeInterval) {
        assistantController.responseTimeout = timeout
    }

    package func expireAssistantResponseForTesting() {
        handleAssistantResponseTimeout(
            conversationID: state.assistantDiscussion?.activeConversation?.id
        )
    }

    deinit {
        projectCorpusWatcher?.stop()
        assistantController.resetSessionState()
        pythonKernels.values.forEach { $0.terminate() }
        assistantController.session?.terminate()
        guard runtimeKind == .production else { return }
        _ = sessionParameterLifecycleClient.flushAll()
        cleanupTemporaryDemoProject()
    }

    public static func empty() -> WorkbenchStore {
        WorkbenchStore(state: EmptyWorkbench.makeState())
    }

    public static func fixture() -> WorkbenchStore {
        WorkbenchStore(state: FixtureWorkbench.makeState())
    }

    package static func notebookPrototype(
        scenario: NotebookPrototypeScenario = .primary,
        dependencies: NotebookPrototypeRuntimeDependencies = .denied
    ) -> WorkbenchStore {
        NotebookPrototypeBoundaryAudit.reset()
        return WorkbenchStore(
            state: notebookPrototypeState(scenario: scenario),
            probeClient: dependencies.probeClient,
            demoProjectClient: dependencies.demoProjectClient,
            plotClient: dependencies.plotClient,
            imageExplorerClient: dependencies.imageExplorerClient,
            tableBrowserClient: dependencies.tableBrowserClient,
            genericTaskClient: dependencies.genericTaskClient,
            applicationCatalogClient: PrototypeApplicationCatalogClient(),
            taskUISchemaClient: dependencies.taskUISchemaClient,
            surfaceParameterClient: dependencies.surfaceParameterClient,
            imagerProgressSource: EmptyImagerProgressSource()
        )
    }

    package static func pythonPrototype(
        scenario: PythonPrototypeScenario = .primary,
        dependencies: NotebookPrototypeRuntimeDependencies = .denied
    ) -> WorkbenchStore {
        NotebookPrototypeBoundaryAudit.reset()
        return WorkbenchStore(
            state: pythonPrototypeState(scenario: scenario),
            probeClient: dependencies.probeClient,
            demoProjectClient: dependencies.demoProjectClient,
            plotClient: dependencies.plotClient,
            imageExplorerClient: dependencies.imageExplorerClient,
            tableBrowserClient: dependencies.tableBrowserClient,
            genericTaskClient: dependencies.genericTaskClient,
            applicationCatalogClient: PrototypeApplicationCatalogClient(),
            taskUISchemaClient: dependencies.taskUISchemaClient,
            surfaceParameterClient: dependencies.surfaceParameterClient,
            imagerProgressSource: EmptyImagerProgressSource()
        )
    }

    package static func tutorialPrototype(
        scenario: TutorialNotebookPrototypeScenario = .happyPath,
        dependencies: NotebookPrototypeRuntimeDependencies = .denied
    ) -> WorkbenchStore {
        NotebookPrototypeBoundaryAudit.reset()
        return WorkbenchStore(
            state: tutorialPrototypeState(scenario: scenario),
            probeClient: dependencies.probeClient,
            demoProjectClient: dependencies.demoProjectClient,
            plotClient: dependencies.plotClient,
            imageExplorerClient: dependencies.imageExplorerClient,
            tableBrowserClient: dependencies.tableBrowserClient,
            genericTaskClient: dependencies.genericTaskClient,
            applicationCatalogClient: PrototypeApplicationCatalogClient(),
            taskUISchemaClient: dependencies.taskUISchemaClient,
            surfaceParameterClient: dependencies.surfaceParameterClient,
            imagerProgressSource: EmptyImagerProgressSource()
        )
    }

    package static func aiPrototype(
        scenario: AIChatPrototypeScenario = .primary,
        dependencies: NotebookPrototypeRuntimeDependencies = .denied
    ) -> WorkbenchStore {
        NotebookPrototypeBoundaryAudit.reset()
        return WorkbenchStore(
            state: aiPrototypeState(scenario: scenario),
            probeClient: dependencies.probeClient,
            demoProjectClient: dependencies.demoProjectClient,
            plotClient: dependencies.plotClient,
            imageExplorerClient: dependencies.imageExplorerClient,
            tableBrowserClient: dependencies.tableBrowserClient,
            genericTaskClient: dependencies.genericTaskClient,
            applicationCatalogClient: PrototypeApplicationCatalogClient(),
            taskUISchemaClient: dependencies.taskUISchemaClient,
            surfaceParameterClient: dependencies.surfaceParameterClient,
            imagerProgressSource: EmptyImagerProgressSource()
        )
    }

    package var isNotebookPrototypeRuntime: Bool {
        runtimeKind == .notebookPrototype
    }

    package var isPythonPrototypeRuntime: Bool {
        runtimeKind == .pythonPrototype
    }

    package var isTutorialPrototypeRuntime: Bool {
        runtimeKind == .tutorialPrototype
    }

    package var isAIPrototypeRuntime: Bool {
        runtimeKind == .aiPrototype
    }

    package var isPrototypeRuntime: Bool {
        runtimeKind != .production
    }

    package var prototypeProductionBoundaryInvocationCount: Int {
        isPrototypeRuntime ? NotebookPrototypeBoundaryAudit.count : 0
    }

    private static func notebookPrototypeState(
        scenario: NotebookPrototypeScenario,
        interfaceFontSize: Double = WorkbenchState.defaultInterfaceFontSize
    ) -> WorkbenchState {
        var state = EmptyWorkbench.makeState(interfaceFontSize: interfaceFontSize)
        state.project = ProjectFixture(
            name: "TW Hya Reduction",
            rootPath: "/PrototypeProjects/tw-hya-reduction",
            datasets: [
                DatasetSummary(
                    id: "prototype-twhya-ms",
                    name: "twhya_calibrated.ms",
                    path: "data/twhya_calibrated.ms",
                    kind: .measurementSet,
                    size: "2.1 GB fixture",
                    units: "Jy, Hz, seconds",
                    fields: ["TW Hya"],
                    spectralWindows: ["spw 0: continuum", "spw 1: line"],
                    dataColumns: ["DATA", "CORRECTED_DATA"],
                    notes: "Deterministic notebook prototype metadata; no data are opened."
                )
            ],
            source: .fixture
        )
        state.selectedDatasetID = "prototype-twhya-ms"
        state.prototypeNotebook = PrototypeScientificNotebookFixtureAdapter.make(scenario: scenario)
        state.dockMode = .notebooks
        state.leftDockCollapsed = false
        state.inspectorCollapsed = true
        state.tabs = [
            WorkbenchTab(
                id: "tab-scientific-notebook",
                title: state.prototypeNotebook?.filename ?? "Notebook",
                kind: .notebook
            )
        ]
        state.activeTabID = "tab-scientific-notebook"
        return state
    }

    private static func pythonPrototypeState(
        scenario: PythonPrototypeScenario,
        interfaceFontSize: Double = WorkbenchState.defaultInterfaceFontSize
    ) -> WorkbenchState {
        var state = EmptyWorkbench.makeState(interfaceFontSize: interfaceFontSize)
        state.project = ProjectFixture(
            name: "TW Hya Reduction",
            rootPath: "/PrototypeProjects/tw-hya-reduction",
            datasets: [
                DatasetSummary(
                    id: "prototype-twhya-ms",
                    name: "twhya_calibrated.ms",
                    path: "data/twhya_calibrated.ms",
                    kind: .measurementSet,
                    size: "2.1 GB fixture",
                    units: "Jy, Hz, seconds",
                    fields: ["TW Hya"],
                    spectralWindows: ["spw 0: continuum", "spw 1: line"],
                    dataColumns: ["DATA", "CORRECTED_DATA"],
                    notes: "Deterministic Python prototype metadata; no data are opened."
                )
            ],
            source: .fixture
        )
        state.selectedDatasetID = "prototype-twhya-ms"
        state.prototypePython = PrototypePythonFixtureAdapter.make(scenario: scenario)
        state.leftDockCollapsed = true
        state.inspectorCollapsed = true
        state.tabs = [
            WorkbenchTab(
                id: "tab-python-prototype",
                title: "Python · TW Hya analysis",
                kind: .python
            )
        ]
        state.activeTabID = "tab-python-prototype"
        return state
    }

    private static func tutorialPrototypeState(
        scenario: TutorialNotebookPrototypeScenario,
        interfaceFontSize: Double = WorkbenchState.defaultInterfaceFontSize
    ) -> WorkbenchState {
        var state = EmptyWorkbench.makeState(interfaceFontSize: interfaceFontSize)
        state.project = ProjectFixture(
            name: "TW Hya First Look",
            rootPath: "/PrototypeProjects/tw-hya-first-look",
            datasets: [],
            source: .fixture
        )
        state.prototypeTutorial = TutorialNotebookPrototypeFixtureAdapter.make(scenario: scenario)
        state.dockMode = .notebooks
        state.leftDockCollapsed = false
        state.inspectorCollapsed = true
        state.tabs = [
            WorkbenchTab(
                id: "tab-tutorial-prototype",
                title: "Tutorial · TW Hya First Look",
                kind: .notebook
            )
        ]
        state.activeTabID = "tab-tutorial-prototype"
        return state
    }

    private static func aiPrototypeState(
        scenario: AIChatPrototypeScenario,
        interfaceFontSize: Double = WorkbenchState.defaultInterfaceFontSize
    ) -> WorkbenchState {
        var state = EmptyWorkbench.makeState(interfaceFontSize: interfaceFontSize)
        state.project = ProjectFixture(
            name: "TW Hya Reduction",
            rootPath: "/PrototypeProjects/tw-hya-assistant",
            datasets: [
                DatasetSummary(
                    id: "prototype-twhya-ms",
                    name: "twhya_calibrated.ms",
                    path: "data/twhya_calibrated.ms",
                    kind: .measurementSet,
                    size: "2.1 GB fixture",
                    units: "Jy, Hz, seconds",
                    fields: ["TW Hya"],
                    notes: "Deterministic AI prototype metadata; no data are opened."
                )
            ],
            source: .fixture
        )
        state.selectedDatasetID = "prototype-twhya-ms"
        state.prototypeNotebook = PrototypeScientificNotebookFixtureAdapter.make(scenario: .primary)
        state.prototypeAI = PrototypeAIChatFixtureAdapter.make(scenario: scenario)
        state.dockMode = .notebooks
        state.leftDockCollapsed = false
        state.inspectorCollapsed = true
        state.tabs = [
            WorkbenchTab(
                id: "tab-scientific-notebook",
                title: state.prototypeNotebook?.filename ?? "default.md",
                kind: .notebook
            ),
            WorkbenchTab(
                id: "tab-ai-context-task",
                title: "Imager",
                kind: .task,
                datasetID: "prototype-twhya-ms",
                taskID: "imager"
            ),
            WorkbenchTab(
                id: "tab-ai-context-explorer",
                title: "MS: TW Hya",
                kind: .datasetExplorer,
                datasetID: "prototype-twhya-ms"
            ),
            WorkbenchTab(id: "tab-ai-context-python", title: "Python", kind: .python),
            WorkbenchTab(id: "tab-ai-context-history", title: "History", kind: .history),
        ]
        state.activeTabID = "tab-scientific-notebook"
        return state
    }

    public func openFixtureProject() {
        guard !rejectPrototypeProductionAction("Demo projects") else { return }
        let interfaceFontSize = state.interfaceFontSize
        let applicationCatalog = state.applicationCatalog
        cleanupTemporaryDemoProject()
        do {
            let probed = try demoProjectClient.createDemoProject()
            temporaryDemoProjectRoot = probed.project.rootPath
            var project = probed.project
            project.datasets = orderedDemoDatasets(project.datasets)
            state = EmptyWorkbench.makeState(interfaceFontSize: interfaceFontSize)
            state.applicationCatalog = applicationCatalog
            state.project = project
            state.probeDiagnostics = probed.diagnostics
            state.selectedDatasetID = project.datasets.first?.id
            state.dockMode = .datasets
            state.leftDockCollapsed = false
            state.inspectorCollapsed = false
            if let dataset = state.selectedDataset {
                openExplorer(for: dataset)
            }
            state.history.append(
                ProcessingHistoryEvent(
                    id: "hist-demo-project-open",
                    timestamp: "staged",
                    title: "Tutorial demo opened",
                    reason: "Staged local TW Hya tutorial datasets into a temporary project and probed them with Rust frontend services.",
                    affectedPaths: [probed.project.rootPath],
                    approval: "user"
                )
            )
        } catch {
            state = EmptyWorkbench.makeState(interfaceFontSize: interfaceFontSize)
            state.applicationCatalog = applicationCatalog
            state.lastErrors.append("Open tutorial demo project: \(error)")
        }
    }

    public func openProject(path: String) {
        guard !rejectPrototypeProductionAction("Project opening") else { return }
        projectCorpusWatcher?.stop()
        projectCorpusWatcher = nil
        assistantController.corpusCoordinator.reset()
        fullyRefreshedAssistantCorpusProject = nil
        let interfaceFontSize = state.interfaceFontSize
        let applicationCatalog = state.applicationCatalog
        cleanupTemporaryDemoProject()
        do {
            let probed = try probeClient.probeProject(path: path)
            state = EmptyWorkbench.makeState(interfaceFontSize: interfaceFontSize)
            state.applicationCatalog = applicationCatalog
            state.project = probed.project
            state.probeDiagnostics = probed.diagnostics
            state.selectedDatasetID = probed.project.datasets.first?.id
            loadScientificNotebooks()
            state.dockMode = .datasets
            state.leftDockCollapsed = false
            state.inspectorCollapsed = false
            if let dataset = state.selectedDataset {
                openExplorer(for: dataset)
            }
            state.assistantDiscussion = AssistantDiscussionState()
            startProjectCorpusWatcher()
            requestAssistantCorpusRefresh(.projectDocuments)
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


    public func openExternalMeasurementSetForImaging(path: String) {
        guard !rejectPrototypeProductionAction("External MeasurementSet opening") else { return }
        let interfaceFontSize = state.interfaceFontSize
        let applicationCatalog = state.applicationCatalog
        cleanupTemporaryDemoProject()
        let standardizedPath = Self.standardizedDatasetPath(path)
        let url = URL(fileURLWithPath: standardizedPath)
        let rootPath = url.deletingLastPathComponent().path
        let dataset: DatasetSummary
        do {
            guard var probed = try probeClient.probePath(path: standardizedPath),
                  probed.kind == .measurementSet
            else {
                state.lastErrors.append(
                    "Exact MeasurementSet probe did not recognize \(standardizedPath); the dataset was not opened."
                )
                return
            }
            probed.notes += " Opened directly as an imager input; parent project probe skipped."
            probed.diagnostics.append(
                "Direct launch used exact MeasurementSet probe only; parent folder refresh is disabled."
            )
            dataset = probed
        } catch {
            state.lastErrors.append(
                "Exact MeasurementSet probe failed for \(standardizedPath); the dataset was not opened: \(error)"
            )
            return
        }
        state = EmptyWorkbench.makeState(interfaceFontSize: interfaceFontSize)
        state.applicationCatalog = applicationCatalog
        state.project = ProjectFixture(
            name: url.deletingPathExtension().lastPathComponent,
            rootPath: rootPath,
            datasets: [dataset],
            source: .directMeasurementSet
        )
        state.selectedDatasetID = dataset.id
        state.dockMode = .datasets
        state.leftDockCollapsed = false
        state.inspectorCollapsed = false
        openImagerTaskForSelectedDataset()
        seedDirectMeasurementSetImagerDefaults(for: dataset)
        state.history.append(
            ProcessingHistoryEvent(
                id: "hist-project-open-\(state.history.count + 1)",
                timestamp: "direct",
                title: "MeasurementSet opened for imaging",
                reason: "Opened a direct MeasurementSet path for a schema-driven imager run.",
                affectedPaths: [standardizedPath],
                approval: "user"
            )
        )
    }

    public func openTutorialTemplate(path: String) {
        guard !rejectPrototypeProductionAction("Tutorial templates") else { return }
        guard state.hasProject else {
            state.lastErrors.append("Open a project before forking a tutorial template")
            return
        }
        do {
            let supplied = URL(fileURLWithPath: (path as NSString).expandingTildeInPath)
                .standardizedFileURL
            let suppliedIsDirectory = ((try? supplied.resourceValues(forKeys: [.isDirectoryKey]))?
                .isDirectory) == true
            let root = suppliedIsDirectory ? supplied : supplied.deletingLastPathComponent()
            let v1Manifest = root.appendingPathComponent("tutorial.toml")
            let v0Manifest = root.appendingPathComponent("pack.json")
            let templateRoot: URL
            if FileManager.default.fileExists(atPath: v1Manifest.path) {
                templateRoot = root
            } else if FileManager.default.fileExists(atPath: v0Manifest.path) {
                let migrationRoot = URL(fileURLWithPath: state.project.rootPath)
                    .appendingPathComponent(".casa-rs/tutorial-templates", isDirectory: true)
                    .appendingPathComponent("\(root.lastPathComponent)-v1", isDirectory: true)
                if !FileManager.default.fileExists(atPath: migrationRoot.path) {
                    try tutorialPersistenceClient.migrate(
                        packPath: root.path,
                        destination: migrationRoot.path
                    )
                }
                templateRoot = migrationRoot
            } else {
                throw TutorialTemplateLoadError.missingManifest(v1Manifest.path)
            }
            let existing = Set(state.scientificNotebooks?.notebooks.map(\.filename) ?? [])
            let base = root.lastPathComponent.isEmpty ? "Tutorial" : root.lastPathComponent
            var filename = "\(base).md"
            var suffix = 2
            while existing.contains(filename) {
                filename = "\(base)-\(suffix).md"
                suffix += 1
            }
            let forked = try tutorialPersistenceClient.fork(
                projectRoot: state.project.rootPath,
                templatePath: templateRoot.path,
                filename: filename
            )
            loadScientificNotebooks()
            selectScientificNotebook(forked.tutorial.notebookId)
            state.dockMode = .notebooks
            state.leftDockCollapsed = false
            state.inspectorCollapsed = true
            openDefaultTab(kind: .notebook)
            state.history.append(
                ProcessingHistoryEvent(
                    id: "hist-tutorial-fork-\(state.history.count + 1)",
                    timestamp: "forked",
                    title: "Tutorial notebook created",
                    reason: "Forked immutable tutorial \(forked.tutorial.tutorialId) into editable project Markdown.",
                    affectedPaths: ["notebooks/\(filename)"],
                    approval: "user"
                )
            )
        } catch {
            state.lastErrors.append("Open tutorial template \(path): \(error)")
        }
    }

    public func refreshProjectFromDiskIfNeeded(now: Date = Date()) {
        // The UI timer keeps firing in prototype review sessions. Ignore it
        // silently so it cannot touch disk or flood the fixture error log.
        guard runtimeKind == .production else { return }
        guard state.hasProject, !state.project.rootPath.isEmpty else {
            return
        }
        guard state.project.source == .probed else {
            return
        }
        guard now.timeIntervalSince(lastProjectDiskRefresh) >= 1.0 else {
            return
        }
        lastProjectDiskRefresh = now
        refreshProjectFromDisk()
    }

    public func refreshProjectFromDisk() {
        guard !rejectPrototypeProductionAction("Project refresh") else { return }
        guard state.hasProject, !state.project.rootPath.isEmpty else {
            return
        }
        guard state.project.source == .probed else {
            return
        }
        let selectedPath = state.selectedDataset?.path
        do {
            let probe = try probeClient.probeProject(path: state.project.rootPath)
            let refreshed = (
                datasets: projectDatasetsWithLooseFiles(
                    recognizedDatasets: probe.project.datasets,
                    rootPath: state.project.rootPath
                ),
                diagnostics: probe.diagnostics
            )
            state.project.datasets = deduplicatedDatasets(refreshed.datasets)
            state.probeDiagnostics = refreshed.diagnostics
            if let selectedPath,
               let replacement = state.project.datasets.first(where: { $0.path == selectedPath }) {
                state.selectedDatasetID = replacement.id
            } else {
                state.selectedDatasetID = state.project.datasets.first?.id
            }
        } catch {
            state.lastErrors.append("Refresh project \(state.project.rootPath): \(error)")
        }
    }

    private func projectDatasetsWithLooseFiles(
        recognizedDatasets: [DatasetSummary],
        rootPath: String
    ) -> [DatasetSummary] {
        let recognizedPaths = Set(recognizedDatasets.map { Self.standardizedDatasetPath($0.path) })
        let datasetDirectoryPaths = recognizedDatasets
            .filter { dataset in
                dataset.kind != .runProduct && FileManager.default.fileExists(atPath: dataset.path, isDirectory: nil)
            }
            .compactMap { dataset -> String? in
                var isDirectory = ObjCBool(false)
                guard FileManager.default.fileExists(atPath: dataset.path, isDirectory: &isDirectory),
                      isDirectory.boolValue
                else {
                    return nil
                }
                return Self.standardizedDatasetPath(dataset.path)
            }
        let looseFiles = looseProjectFileDatasets(
            rootPath: rootPath,
            recognizedPaths: recognizedPaths,
            datasetDirectoryPaths: datasetDirectoryPaths
        )
        return deduplicatedDatasets(recognizedDatasets + looseFiles)
    }

    private func deduplicatedDatasets(_ datasets: [DatasetSummary]) -> [DatasetSummary] {
        var output: [DatasetSummary] = []
        var indexesByPath: [String: Int] = [:]
        for dataset in datasets {
            let standardizedPath = Self.standardizedDatasetPath(dataset.path)
            if let index = indexesByPath[standardizedPath] {
                output[index] = dataset
            } else {
                indexesByPath[standardizedPath] = output.count
                output.append(dataset)
            }
        }
        return output
    }

    private func looseProjectFileDatasets(
        rootPath: String,
        recognizedPaths: Set<String>,
        datasetDirectoryPaths: [String]
    ) -> [DatasetSummary] {
        let rootURL = URL(fileURLWithPath: rootPath, isDirectory: true).standardizedFileURL
        var output: [DatasetSummary] = []
        var scanned = 0
        scanLooseProjectFiles(
            directory: rootURL,
            rootURL: rootURL,
            depth: 0,
            scanned: &scanned,
            output: &output,
            recognizedPaths: recognizedPaths,
            datasetDirectoryPaths: datasetDirectoryPaths
        )
        return output.sorted { $0.path.localizedStandardCompare($1.path) == .orderedAscending }
    }

    private func scanLooseProjectFiles(
        directory: URL,
        rootURL: URL,
        depth: Int,
        scanned: inout Int,
        output: inout [DatasetSummary],
        recognizedPaths: Set<String>,
        datasetDirectoryPaths: [String]
    ) {
        guard depth <= 5, scanned < 500 else {
            return
        }
        let entries = (try? FileManager.default.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: [.isDirectoryKey, .fileSizeKey],
            options: [.skipsHiddenFiles]
        )) ?? []
        for entry in entries {
            guard scanned < 500 else {
                return
            }
            scanned += 1
            let standardizedPath = Self.standardizedDatasetPath(entry.path)
            if recognizedPaths.contains(standardizedPath) {
                continue
            }
            let values = try? entry.resourceValues(forKeys: [.isDirectoryKey, .fileSizeKey])
            if values?.isDirectory == true {
                if datasetDirectoryPaths.contains(where: { standardizedPath == $0 || standardizedPath.hasPrefix($0 + "/") }) {
                    continue
                }
                if shouldProbeLooseProjectDirectory(entry),
                   let probed = try? probeClient.probePath(path: standardizedPath) {
                    output.append(probed)
                    continue
                }
                scanLooseProjectFiles(
                    directory: entry,
                    rootURL: rootURL,
                    depth: depth + 1,
                    scanned: &scanned,
                    output: &output,
                    recognizedPaths: recognizedPaths,
                    datasetDirectoryPaths: datasetDirectoryPaths
                )
                continue
            }
            if shouldSurfaceLooseRegionFile(entry) {
                let relativePath = projectRelativePath(standardizedPath)
                output.append(DatasetSummary(
                    id: standardizedPath,
                    name: entry.lastPathComponent,
                    path: standardizedPath,
                    kind: .region,
                    size: "region file",
                    units: "CRTF",
                    sizeBytes: UInt64(values?.fileSize ?? 0),
                    notes: "Project region file discovered by disk refresh.",
                    diagnostics: [
                        "Region parameter syntax: --region \(relativePath)",
                        "Inline region syntax: box[[x0pix,y0pix],[x1pix,y1pix]] or world-coordinate CRTF"
                    ]
                ))
                continue
            }
            guard shouldSurfaceLooseProjectFile(entry) else {
                continue
            }
            let relativePath = projectRelativePath(standardizedPath)
            output.append(DatasetSummary(
                id: standardizedPath,
                name: entry.lastPathComponent,
                path: standardizedPath,
                kind: .runProduct,
                size: byteCountString(UInt64(values?.fileSize ?? 0)),
                units: entry.pathExtension.uppercased(),
                sizeBytes: UInt64(values?.fileSize ?? 0),
                notes: "Project file discovered by disk refresh.",
                diagnostics: ["Project-relative path: \(relativePath)"]
            ))
        }
    }

    private func shouldProbeLooseProjectDirectory(_ url: URL) -> Bool {
        isCasacoreTableDirectory(url)
    }

    private func isCasacoreTableDirectory(_ url: URL) -> Bool {
        let tableDatURL = url.appendingPathComponent("table.dat", isDirectory: false)
        var isDirectory = ObjCBool(false)
        guard FileManager.default.fileExists(atPath: tableDatURL.path, isDirectory: &isDirectory),
              !isDirectory.boolValue
        else {
            return false
        }
        return FileManager.default.isReadableFile(atPath: tableDatURL.path)
    }

    private func shouldSurfaceLooseProjectFile(_ url: URL) -> Bool {
        switch url.pathExtension.lowercased() {
        case "fits", "fit", "fts":
            return true
        default:
            return false
        }
    }

    private func shouldSurfaceLooseRegionFile(_ url: URL) -> Bool {
        url.pathExtension.lowercased() == "crtf"
    }

    private func byteCountString(_ bytes: UInt64) -> String {
        ByteCountFormatter.string(fromByteCount: Int64(bytes), countStyle: .file)
    }

    private static func standardizedDatasetPath(_ path: String) -> String {
        URL(fileURLWithPath: path).standardizedFileURL.path
    }

    private func fileSize(path: String) -> UInt64 {
        let attributes = try? FileManager.default.attributesOfItem(atPath: path)
        return (attributes?[.size] as? NSNumber)?.uint64Value ?? 0
    }

    private func selectedOrFirstDataset(kind: DatasetKind) -> DatasetSummary? {
        if let selected = state.selectedDataset, selected.kind == kind {
            return selected
        }
        return state.project.datasets.first { $0.kind == kind }
    }

    private func cleanupTemporaryDemoProject() {
        guard runtimeKind == .production else { return }
        guard let temporaryDemoProjectRoot else { return }
        demoProjectClient.cleanupDemoProject(rootPath: temporaryDemoProjectRoot)
        self.temporaryDemoProjectRoot = nil
    }

    private func orderedDemoDatasets(_ datasets: [DatasetSummary]) -> [DatasetSummary] {
        datasets.sorted { lhs, rhs in
            let leftRank = demoDatasetRank(lhs)
            let rightRank = demoDatasetRank(rhs)
            if leftRank != rightRank {
                return leftRank < rightRank
            }
            return lhs.name.localizedStandardCompare(rhs.name) == .orderedAscending
        }
    }

    private func demoDatasetRank(_ dataset: DatasetSummary) -> Int {
        switch dataset.kind {
        case .measurementSet:
            0
        case .imageCube:
            1
        case .table, .calibrationTable:
            2
        case .region, .runProduct:
            3
        }
    }

    public func selectDockMode(_ mode: DockMode) {
        if mode == .history,
           rejectPrototypeProductionAction("Processing history") {
            return
        }
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
        guard !rejectPrototypeProductionAction("Dataset explorers") else { return }
        guard let dataset = state.selectedDataset else {
            state.lastErrors.append("No selected dataset to explore")
            return
        }

        openExplorer(for: dataset)
    }

    public func openDatasetExplorer(_ datasetID: String) {
        guard !rejectPrototypeProductionAction("Dataset explorers") else { return }
        guard let dataset = state.project.datasets.first(where: { $0.id == datasetID }) else {
            state.lastErrors.append("Unknown dataset \(datasetID)")
            return
        }

        state.selectedDatasetID = datasetID
        openExplorer(for: dataset)
    }

    public func openDatasetTableBrowser(_ datasetID: String) {
        guard !rejectPrototypeProductionAction("Table browsers") else { return }
        guard let dataset = state.project.datasets.first(where: { $0.id == datasetID }) else {
            state.lastErrors.append("Unknown dataset \(datasetID)")
            return
        }
        guard canBrowseAsTable(dataset) else {
            state.lastErrors.append("Dataset \(dataset.name) is not a casacore table")
            return
        }

        state.selectedDatasetID = datasetID
        let tabID = tableBrowserTabID(for: dataset.id)
        openTab(
            WorkbenchTab(
                id: tabID,
                title: "Table: \(dataset.name)",
                kind: .tableBrowser,
                datasetID: dataset.id
            )
        )
        applyParameterContext(
            surfaceID: "tablebrowser",
            instanceID: tabID,
            textValues: ["table": dataset.path],
            preserveOverrides: true
        )
        refreshTableBrowser(datasetID: datasetID)
    }

    public func openTableBrowserPath(_ path: String, sourceDatasetID: String? = nil) {
        guard !rejectPrototypeProductionAction("Table browsers") else { return }
        let normalizedPath = URL(fileURLWithPath: path).standardizedFileURL.path
        if !state.project.datasets.contains(where: { $0.id == normalizedPath }) {
            let sourceName = sourceDatasetID
                .flatMap { id in state.project.datasets.first { $0.id == id }?.name }
            state.project.datasets.append(
                DatasetSummary(
                    id: normalizedPath,
                    name: URL(fileURLWithPath: normalizedPath).lastPathComponent,
                    path: normalizedPath,
                    kind: .table,
                    size: "casacore table",
                    units: "",
                    notes: sourceName.map { "Opened from \($0)." } ?? "Opened from tablebrowser."
                )
            )
        }
        openDatasetTableBrowser(normalizedPath)
    }

    public func openImageExplorerPath(_ path: String, sourceDatasetID: String? = nil) {
        guard !rejectPrototypeProductionAction("Image explorers") else { return }
        let normalizedPath = URL(fileURLWithPath: path).standardizedFileURL.path
        if !state.project.datasets.contains(where: { $0.id == normalizedPath }) {
            if let probed = try? probeClient.probePath(path: normalizedPath), probed.kind == .imageCube {
                state.project.datasets.append(probed)
            } else {
                let sourceName = sourceDatasetID
                    .flatMap { id in state.project.datasets.first { $0.id == id }?.name }
                state.project.datasets.append(
                    DatasetSummary(
                        id: normalizedPath,
                        name: URL(fileURLWithPath: normalizedPath).lastPathComponent,
                        path: normalizedPath,
                        kind: .imageCube,
                        size: "CASA image",
                        units: "",
                        notes: sourceName.map { "Opened from \($0)." } ?? "Opened from image explorer."
                    )
                )
            }
        }
        openDatasetExplorer(normalizedPath)
    }

    public func openRunProduct(runID: String, productID: String) {
        guard !rejectPrototypeProductionAction("Run products") else { return }
        guard let groupIndex = state.runProductGroups.firstIndex(where: { $0.runID == runID }) else {
            state.lastErrors.append("Unknown run \(runID)")
            return
        }
        guard let productIndex = state.runProductGroups[groupIndex].products.firstIndex(where: { $0.id == productID }) else {
            state.lastErrors.append("Unknown product \(productID)")
            return
        }
        var product = state.runProductGroups[groupIndex].products[productIndex]
        if product.datasetID == nil {
            do {
                if let dataset = try probeClient.probePath(path: product.path) {
                    if !state.project.datasets.contains(where: { $0.id == dataset.id }) {
                        state.project.datasets.append(dataset)
                    }
                    product.datasetID = dataset.id
                    product.diagnostic = nil
                    state.runProductGroups[groupIndex].products[productIndex] = product
                }
            } catch {
                product.diagnostic = "Retry probe failed: \(error)"
                state.runProductGroups[groupIndex].products[productIndex] = product
            }
        }
        guard let datasetID = product.datasetID else {
            state.lastErrors.append(
                product.diagnostic ?? "Product \(product.label) is not a recognized dataset"
            )
            return
        }
        openDatasetExplorer(datasetID)
    }

    public func setMeasurementSetPlotPreset(_ preset: MeasurementSetExplorerPlotPreset, datasetID: String) {
        var plotState = measurementSetPlotState(for: datasetID)
        plotState.preset = preset
        plotState.lastError = nil
        refreshMeasurementSetPlotStateFromCache(&plotState, datasetID: datasetID)
        state.measurementSetPlots[datasetID] = plotState
        syncMeasurementSetParameterSession(plotState, datasetID: datasetID)
    }

    public func measurementSetExplorerPlotState(datasetID: String) -> MeasurementSetExplorerPlotState {
        measurementSetPlotState(for: datasetID)
    }

    public func setMeasurementSetPlotField(_ field: String?, datasetID: String) {
        var plotState = measurementSetPlotState(for: datasetID)
        plotState.selectedField = normalizedPickerValue(field)
        plotState.lastError = nil
        refreshMeasurementSetPlotStateFromCache(&plotState, datasetID: datasetID)
        state.measurementSetPlots[datasetID] = plotState
        syncMeasurementSetParameterSession(plotState, datasetID: datasetID)
    }

    public func setMeasurementSetPlotSpectralWindow(_ spectralWindow: String?, datasetID: String) {
        var plotState = measurementSetPlotState(for: datasetID)
        plotState.selectedSpectralWindow = normalizedPickerValue(spectralWindow)
        plotState.lastError = nil
        refreshMeasurementSetPlotStateFromCache(&plotState, datasetID: datasetID)
        state.measurementSetPlots[datasetID] = plotState
        syncMeasurementSetParameterSession(plotState, datasetID: datasetID)
    }

    public func setMeasurementSetPlotChannelSelection(_ channelSelection: String?, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.selectedChannelSelection = normalizedTextSelection(channelSelection)
        }
    }

    public func setMeasurementSetPlotTimerange(_ timerange: String?, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.selectedTimerange = normalizedTextSelection(timerange)
        }
    }

    public func setMeasurementSetPlotUVRange(_ uvRange: String?, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.selectedUVRange = normalizedTextSelection(uvRange)
        }
    }

    public func setMeasurementSetPlotAntenna(_ antenna: String?, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.selectedAntenna = normalizedTextSelection(antenna)
        }
    }

    public func setMeasurementSetPlotScan(_ scan: String?, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.selectedScan = normalizedTextSelection(scan)
        }
    }

    public func setMeasurementSetPlotCorrelation(_ correlation: String?, datasetID: String) {
        var plotState = measurementSetPlotState(for: datasetID)
        plotState.selectedCorrelation = normalizedPickerValue(correlation)
        plotState.lastError = nil
        refreshMeasurementSetPlotStateFromCache(&plotState, datasetID: datasetID)
        state.measurementSetPlots[datasetID] = plotState
        syncMeasurementSetParameterSession(plotState, datasetID: datasetID)
    }

    public func setMeasurementSetPlotArray(_ array: String?, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.selectedArray = normalizedTextSelection(array)
        }
    }

    public func setMeasurementSetPlotObservation(_ observation: String?, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.selectedObservation = normalizedTextSelection(observation)
        }
    }

    public func setMeasurementSetPlotIntent(_ intent: String?, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.selectedIntent = normalizedTextSelection(intent)
        }
    }

    public func setMeasurementSetPlotFeed(_ feed: String?, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.selectedFeed = normalizedTextSelection(feed)
        }
    }

    public func setMeasurementSetPlotMSSelect(_ msselect: String?, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.selectedMSSelect = normalizedTextSelection(msselect)
        }
    }

    public func setMeasurementSetPlotDataColumn(_ dataColumn: String, datasetID: String) {
        var plotState = measurementSetPlotState(for: datasetID)
        plotState.dataColumn = dataColumn
        plotState.lastError = nil
        refreshMeasurementSetPlotStateFromCache(&plotState, datasetID: datasetID)
        state.measurementSetPlots[datasetID] = plotState
        syncMeasurementSetParameterSession(plotState, datasetID: datasetID)
    }

    public func setMeasurementSetPlotColorBy(_ colorBy: MeasurementSetPlotColorAxis, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.colorBy = colorBy
        }
    }

    public func setMeasurementSetPlotAvgChannel(_ avgChannel: UInt64?, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.avgChannel = avgChannel
        }
    }

    public func setMeasurementSetPlotAvgTime(_ avgTime: Double?, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.avgTime = avgTime
        }
    }

    public func setMeasurementSetPlotAvgScan(_ avgScan: Bool, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.avgScan = avgScan
        }
    }

    public func setMeasurementSetPlotAvgField(_ avgField: Bool, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.avgField = avgField
        }
    }

    public func setMeasurementSetPlotAvgBaseline(_ avgBaseline: Bool, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.avgBaseline = avgBaseline
        }
    }

    public func setMeasurementSetPlotAvgAntenna(_ avgAntenna: Bool, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.avgAntenna = avgAntenna
        }
    }

    public func setMeasurementSetPlotAvgSPW(_ avgSPW: Bool, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.avgSPW = avgSPW
        }
    }

    public func setMeasurementSetPlotScalarAverage(_ scalarAverage: Bool, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.scalarAverage = scalarAverage
        }
    }

    public func setMeasurementSetPlotIterationAxis(_ iterationAxis: MeasurementSetPlotIterationAxis?, datasetID: String) {
        updateMeasurementSetPlotState(datasetID: datasetID) { plotState in
            plotState.iterationAxis = iterationAxis
        }
    }

    public func setMeasurementSetPlotMaxPoints(_ maxPlotPoints: UInt64, datasetID: String) {
        var plotState = measurementSetPlotState(for: datasetID)
        plotState.maxPlotPoints = Self.minimumBoundedMeasurementSetPlotMaxPoints(maxPlotPoints)
        plotState.lastError = nil
        refreshMeasurementSetPlotStateFromCache(&plotState, datasetID: datasetID)
        state.measurementSetPlots[datasetID] = plotState
        syncMeasurementSetParameterSession(plotState, datasetID: datasetID)
    }

    public func runMeasurementSetPlot(datasetID: String) {
        guard !rejectPrototypeProductionAction("MeasurementSet plots") else { return }
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
        let instanceID = parameterInstanceID(surfaceID: "msexplore", datasetID: datasetID)
        let parameterSession = parameterSession(surfaceID: "msexplore", instanceID: instanceID)
        let jobID = nextJobID(prefix: "ms-plot")
        if let parameterSession {
            do {
                state.lastErrors.append(contentsOf: try taskParameterLifecycleClient.beforeExecution(
                    attemptID: jobID,
                    surfaceID: "msexplore",
                    workspace: parameterSession.workspace,
                    values: parameterSession.values,
                    enabled: parameterSession.saveLast
                ))
            } catch {
                state.lastErrors.append("Automatic msexplore Last save failed: \(error)")
            }
        }
        if let cached = cachedMeasurementSetPlotResult(for: dataset, plotState: plotState) {
            plotState.result = cached
            plotState.status = .ready
            plotState.lastError = nil
            state.measurementSetPlots[datasetID] = plotState
            state.lastErrors.append(contentsOf: taskParameterLifecycleClient.afterCompletion(
                attemptID: jobID,
                successful: true
            ))
            return
        }

        let request = MeasurementSetPlotBuildRequest(
            datasetPath: dataset.path,
            preset: plotState.preset,
            field: selectorToken(plotState.selectedField),
            spectralWindow: spectralWindowSelectorToken(plotState),
            timerange: plotState.selectedTimerange,
            uvRange: plotState.selectedUVRange,
            antenna: plotState.selectedAntenna,
            scan: plotState.selectedScan,
            correlation: selectorToken(plotState.selectedCorrelation),
            array: plotState.selectedArray,
            observation: plotState.selectedObservation,
            intent: plotState.selectedIntent,
            feed: plotState.selectedFeed,
            msselect: plotState.selectedMSSelect,
            dataColumn: plotState.dataColumn,
            colorBy: plotState.colorBy,
            avgChannel: plotState.avgChannel,
            avgTime: plotState.avgTime,
            avgScan: plotState.avgScan,
            avgField: plotState.avgField,
            avgBaseline: plotState.avgBaseline,
            avgAntenna: plotState.avgAntenna,
            avgSPW: plotState.avgSPW,
            scalarAverage: plotState.scalarAverage,
            iterationAxis: plotState.iterationAxis,
            maxPlotPoints: plotState.maxPlotPoints
        )
        let tabID = dataset.explorerTabID
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
            let startedAt = Date()
            do {
                let result = try plotClient.buildPlot(request: request)
                let elapsedSeconds = Date().timeIntervalSince(startedAt)
                DispatchQueue.main.async { [weak self] in
                    self?.finishMeasurementSetPlotJob(
                        jobID: jobID,
                        dataset: dataset,
                        plotState: requestedPlotState,
                        result: result,
                        elapsedSeconds: elapsedSeconds
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
        let terms = Set(normalized.split { !$0.isLetter && !$0.isNumber }.map(String.init))
        if normalized.contains("plot") || normalized.contains("chart") {
            openDefaultTab(kind: .plotSamples)
        } else if normalized.contains("notebook") || terms.contains("note") || terms.contains("notes") {
            openDefaultTab(kind: .notebook)
        } else if normalized.contains("python") {
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

    /// Keep the deterministic prototype from crossing into any production
    /// explorer, task, schema, parameter, or process adapter.
    @discardableResult
    private func rejectPrototypeProductionAction(_ action: String) -> Bool {
        guard runtimeKind != .production else { return false }
        let prototypeName = switch runtimeKind {
        case .notebookPrototype: "notebook"
        case .pythonPrototype: "Python"
        case .tutorialPrototype: "tutorial"
        case .aiPrototype: "AI"
        case .production: "production"
        }
        state.lastErrors.append("\(action) are unavailable in the in-memory \(prototypeName) prototype")
        return true
    }

    public func openTab(_ tab: WorkbenchTab) {
        if runtimeKind == .notebookPrototype,
           tab.kind != .notebook && !(tab.kind == .task && tab.prototypeReceiptID != nil) {
            _ = rejectPrototypeProductionAction("Production \(tab.kind.rawValue) tabs")
            return
        }
        if runtimeKind == .pythonPrototype, tab.kind != .python {
            _ = rejectPrototypeProductionAction("Production \(tab.kind.rawValue) tabs")
            return
        }
        if runtimeKind == .tutorialPrototype,
           tab.kind != .notebook && !(tab.kind == .task && tab.prototypeReceiptID != nil) {
            _ = rejectPrototypeProductionAction("Production \(tab.kind.rawValue) tabs")
            return
        }
        if runtimeKind == .aiPrototype, tab.kind != .aiChat {
            _ = rejectPrototypeProductionAction("Production \(tab.kind.rawValue) tabs")
            return
        }
        if !state.tabs.contains(where: { $0.id == tab.id }) {
            state.tabs.append(tab)
        }
        if tab.kind == .tableBrowser, let datasetID = tab.datasetID {
            state.selectedDatasetID = datasetID
        }
        state.activeTabID = tab.id
    }

    public func activateTab(_ tabID: String) {
        guard let tab = state.tabs.first(where: { $0.id == tabID }) else {
            state.lastErrors.append("Unknown tab \(tabID)")
            return
        }
        if tab.kind == .tableBrowser, let datasetID = tab.datasetID {
            state.selectedDatasetID = datasetID
        }
        state.activeTabID = tabID
    }

    public func closeTab(_ tabID: String) {
        guard let index = state.tabs.firstIndex(where: { $0.id == tabID }) else {
            state.lastErrors.append("Unknown tab \(tabID)")
            return
        }

        if runtimeKind == .aiPrototype, tabID == "tab-ai-prototype",
           var projection = state.prototypeAI
        {
            projection.setPresentation(.closed)
            state.prototypeAI = projection
        }
        if runtimeKind == .production, tabID == "tab-assistant",
           var discussion = state.assistantDiscussion
        {
            discussion.presentation = .closed
            state.assistantDiscussion = discussion
        }

        let closingSessionKeys = state.parameterSessions.keys.filter { $0.hasPrefix("\(tabID)::") }
        for sessionKey in closingSessionKeys {
            if let session = state.parameterSessions[sessionKey] {
                state.lastErrors.append(contentsOf: sessionParameterLifecycleClient.flush(
                    surfaceID: session.snapshot.surfaceId,
                    workspace: session.workspace
                ))
            }
            state.parameterSessions.removeValue(forKey: sessionKey)
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
            guard !rejectPrototypeProductionAction("Dataset explorers") else { return }
            openSelectedDatasetExplorer()
        case .tableBrowser:
            guard !rejectPrototypeProductionAction("Table browsers") else { return }
            guard let dataset = state.selectedDataset else {
                state.lastErrors.append("No selected dataset to browse")
                return
            }
            openDatasetTableBrowser(dataset.id)
        case .tutorial:
            guard !rejectPrototypeProductionAction("Tutorial tabs") else { return }
            guard let tutorial = state.tutorialProjects.first else {
                state.lastErrors.append("No tutorial notebook is open")
                return
            }
            selectScientificNotebook(tutorial.tutorial.notebookId)
            openDefaultTab(kind: .notebook)
        case .task:
            if state.isTutorialPrototype,
               let task = state.prototypeTutorial?.fixtureTask {
                openPrototypeTutorialTask(taskID: task.id)
                return
            }
            if state.isNotebookPrototype {
                guard let receiptID = state.prototypeNotebook?.selectedReceiptID else {
                    state.lastErrors.append("No prototype notebook task is selected")
                    return
                }
                openPrototypeNotebookTask(receiptID: receiptID)
                return
            }
            if state.isDemoProject {
                openTab(WorkbenchTab(id: "tab-task", title: "Calibrate", kind: .task, datasetID: state.selectedDatasetID))
            } else {
                openTab(WorkbenchTab(id: nextTaskTabID(), title: "Tasks", kind: .task, datasetID: state.selectedDatasetID))
            }
        case .notebook:
            if state.isTutorialPrototype {
                openTab(WorkbenchTab(
                    id: "tab-tutorial-prototype",
                    title: "Tutorial · TW Hya First Look",
                    kind: .notebook
                ))
                return
            }
            if let notebook = state.prototypeNotebook {
                openTab(WorkbenchTab(id: "tab-scientific-notebook", title: notebook.filename, kind: .notebook))
                return
            }
            guard let notebook = state.scientificNotebooks?.activeNotebook else {
                state.lastErrors.append("No notebook is open")
                return
            }
            openTab(WorkbenchTab(id: "tab-scientific-notebook", title: notebook.filename, kind: .notebook))
        case .plotSamples:
            guard !rejectPrototypeProductionAction("Production plot tabs") else { return }
            if state.plotDocuments.isEmpty {
                state.plotDocuments = WorkbenchPlotSamples.all()
            }
            openTab(WorkbenchTab(id: "tab-plot-samples", title: "Plot Samples", kind: .plotSamples))
        case .aiChat:
            if state.isAIPrototype {
                expandAIPrototypeConversation()
                return
            }
            if state.isDemoProject {
                openTab(WorkbenchTab(id: "tab-ai", title: "AI Assistant", kind: .aiChat))
                return
            }
            guard !rejectPrototypeProductionAction("AI chat") else { return }
            guard state.hasProject else {
                state.lastErrors.append("Open a project before starting an AI discussion")
                return
            }
            openAssistantDiscussion(presentation: .tab)
        case .python:
            guard !rejectPrototypeProductionAction("Python") else { return }
            guard state.isDemoProject else {
                state.lastErrors.append("Python is not connected yet")
                return
            }
            openTab(WorkbenchTab(id: "tab-python", title: "Python", kind: .python))
        case .history:
            guard !rejectPrototypeProductionAction("Processing history") else { return }
            openTab(WorkbenchTab(id: "tab-history", title: "History", kind: .history))
        }
    }

    /// Rebuild the fixture projection only inside an already-isolated
    /// prototype runtime. A production store can never transition into this
    /// runtime; callers must relaunch through the dedicated CLI/dev factory.
    package func openScientificNotebookPrototype(scenario: NotebookPrototypeScenario = .primary) {
        guard runtimeKind == .notebookPrototype else {
            state.lastErrors.append(
                "The notebook prototype requires a fresh dedicated CLI/dev launch; the production runtime was not changed."
            )
            return
        }
        state = Self.notebookPrototypeState(
            scenario: scenario,
            interfaceFontSize: state.interfaceFontSize
        )
    }

    package func loadScientificNotebooks() {
        guard runtimeKind == .production, state.hasProject else { return }
        do {
            state.scientificNotebooks = try notebookPersistenceClient.loadProject(
                projectRoot: state.project.rootPath
            )
            pythonNotebookRuntime.notebookID = state.scientificNotebooks?.activeNotebookID
            if let notebookID = state.scientificNotebooks?.activeNotebookID {
                pythonNotebookRuntime.status = pythonKernelStatuses[notebookID]
                    ?? projectPythonEnvironmentStatus
            }
            state.tutorialProjects = try tutorialPersistenceClient.list(
                projectRoot: state.project.rootPath
            )
            loadAssistantDiscussions()
        } catch {
            state.lastErrors.append("Load project notebooks: \(error)")
        }
    }

    package func reviewTutorialAcquisition(datasetID: String, sourceOverride: String? = nil) {
        guard let tutorial = state.activeTutorialProject else { return }
        do {
            state.pendingTutorialAcquisitionPlan = try tutorialPersistenceClient.plan(
                projectRoot: state.project.rootPath,
                notebookID: tutorial.tutorial.notebookId,
                datasetID: datasetID,
                sourceOverride: sourceOverride
            )
        } catch {
            state.lastErrors.append("Review tutorial acquisition: \(error)")
        }
    }

    public func selectTutorialSection(_ sectionID: String) {
        guard state.activeTutorialProject?.tutorial.sections.contains(where: {
            $0.id == sectionID
        }) == true else {
            state.lastErrors.append("Unknown tutorial section \(sectionID)")
            return
        }
    }

    public func openTutorialSectionTask(_ sectionID: String) {
        guard let section = state.activeTutorialProject?.tutorial.sections.first(where: {
            $0.id == sectionID
        }), let cellID = section.cellIds.first else {
            state.lastErrors.append("Tutorial section \(sectionID) has no task cell")
            return
        }
        openScientificNotebookTask(cellID: cellID)
    }

    package func dismissTutorialAcquisitionApproval() {
        state.pendingTutorialAcquisitionPlan = nil
    }

    package func approveTutorialAcquisition(skippedCheckIDs: [String] = []) {
        guard let plan = state.pendingTutorialAcquisitionPlan else { return }
        do {
            let dataset = try tutorialPersistenceClient.begin(
                projectRoot: state.project.rootPath,
                plan: plan,
                approval: TutorialAcquisitionApprovalState(
                    approvalSha256: plan.approvalSha256,
                    allowMissingDigest: plan.missingDigest,
                    skippedCheckIds: skippedCheckIDs
                )
            )
            state.pendingTutorialAcquisitionPlan = nil
            replaceTutorialDataset(dataset)
            advanceTutorialAcquisition(datasetID: dataset.id, generation: dataset.currentGeneration)
        } catch {
            state.lastErrors.append("Start tutorial acquisition: \(error)")
        }
    }

    package func cancelTutorialAcquisition(datasetID: String) {
        guard let tutorial = state.activeTutorialProject,
              let dataset = tutorial.tutorial.datasets.first(where: { $0.id == datasetID })
        else { return }
        performTutorialAction(.cancel, dataset: dataset)
    }

    package func resumeTutorialAcquisition(datasetID: String) {
        performTutorialAction(.resume, datasetID: datasetID)
    }

    package func restartTutorialAcquisition(datasetID: String) {
        performTutorialAction(.restart, datasetID: datasetID)
    }

    package func retryTutorialAcquisition(datasetID: String) {
        performTutorialAction(.retry, datasetID: datasetID)
    }

    private func performTutorialAction(
        _ action: TutorialPersistenceAction,
        datasetID: String
    ) {
        guard let tutorial = state.activeTutorialProject,
              let dataset = tutorial.tutorial.datasets.first(where: { $0.id == datasetID })
        else { return }
        performTutorialAction(action, dataset: dataset)
    }

    private func performTutorialAction(
        _ action: TutorialPersistenceAction,
        dataset: TutorialDatasetState
    ) {
        guard let tutorial = state.activeTutorialProject else { return }
        do {
            let updated = try tutorialPersistenceClient.action(
                action,
                projectRoot: state.project.rootPath,
                notebookID: tutorial.tutorial.notebookId,
                datasetID: dataset.id,
                generation: action == .cancel ? dataset.currentGeneration : nil
            )
            replaceTutorialDataset(updated)
            if updated.phase.isRunning {
                advanceTutorialAcquisition(datasetID: updated.id, generation: updated.currentGeneration)
            }
        } catch {
            state.lastErrors.append("Update tutorial acquisition: \(error)")
            loadScientificNotebooks()
        }
    }

    private func advanceTutorialAcquisition(datasetID: String, generation: UInt64) {
        guard let tutorial = state.activeTutorialProject else { return }
        let projectRoot = state.project.rootPath
        let notebookID = tutorial.tutorial.notebookId
        let persistenceClient = tutorialPersistenceClient
        DispatchQueue.global(qos: .userInitiated).async {
            let result = Result {
                try persistenceClient.action(
                    .advance,
                    projectRoot: projectRoot,
                    notebookID: notebookID,
                    datasetID: datasetID,
                    generation: generation
                )
            }
            DispatchQueue.main.async { [weak self] in
                guard let self else { return }
                guard let current = self.state.activeTutorialProject?.tutorial.datasets.first(
                    where: { $0.id == datasetID }
                ), current.currentGeneration == generation, current.phase.isRunning else {
                    return
                }
                switch result {
                case let .success(dataset):
                    self.replaceTutorialDataset(dataset)
                    if dataset.phase.isRunning {
                        self.advanceTutorialAcquisition(
                            datasetID: dataset.id,
                            generation: dataset.currentGeneration
                        )
                    } else {
                        self.loadScientificNotebooks()
                        self.refreshProjectFromDisk()
                    }
                case let .failure(error):
                    self.state.lastErrors.append("Advance tutorial acquisition: \(error)")
                    self.loadScientificNotebooks()
                }
            }
        }
    }

    private func replaceTutorialDataset(_ dataset: TutorialDatasetState) {
        guard let tutorialIndex = state.tutorialProjects.firstIndex(where: {
            $0.tutorial.notebookId == state.scientificNotebooks?.activeNotebookID
        }), let datasetIndex = state.tutorialProjects[tutorialIndex].tutorial.datasets.firstIndex(
            where: { $0.id == dataset.id }
        ) else { return }
        state.tutorialProjects[tutorialIndex].tutorial.datasets[datasetIndex] = dataset
    }

    package func createScientificNotebook(filename: String? = nil, title: String = "CASA-RS notebook") {
        guard runtimeKind == .production, state.hasProject else { return }
        do {
            let created = try notebookPersistenceClient.create(
                projectRoot: state.project.rootPath,
                filename: filename,
                title: title
            )
            loadScientificNotebooks()
            selectScientificNotebook(created.id)
            openDefaultTab(kind: .notebook)
        } catch {
            state.lastErrors.append("Create project notebook: \(error)")
        }
    }

    package func createNextNamedScientificNotebook() {
        let existing = Set(state.scientificNotebooks?.notebooks.map(\.filename) ?? [])
        var index = 1
        while existing.contains("Notebook-\(index).md") { index += 1 }
        createScientificNotebook(
            filename: "Notebook-\(index).md",
            title: "Notebook \(index)"
        )
    }

    package func selectScientificNotebook(_ notebookID: String) {
        guard var project = state.scientificNotebooks,
              project.notebooks.contains(where: { $0.id == notebookID })
        else { return }
        project.activeNotebookID = notebookID
        state.scientificNotebooks = project
        pythonNotebookRuntime.notebookID = notebookID
        pythonNotebookRuntime.status = pythonKernelStatuses[notebookID]
            ?? projectPythonEnvironmentStatus
        if let tabIndex = state.tabs.firstIndex(where: { $0.kind == .notebook }) {
            state.tabs[tabIndex].title = project.activeNotebook?.filename ?? "Notebook"
            state.activeTabID = state.tabs[tabIndex].id
        } else {
            openDefaultTab(kind: .notebook)
        }
    }

    package var projectPythonEnvironmentStatus: NotebookPythonKernelStatus {
        guard let root = state.scientificNotebooks?.projectRoot else { return .unavailable }
        return FileManager.default.isExecutableFile(atPath: pythonExecutable(root: root))
            ? .ready
            : .unavailable
    }

    package func createOrRepairProjectPythonEnvironment() {
        guard let root = state.scientificNotebooks?.projectRoot else { return }
        pythonNotebookRuntime.status = .starting
        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            let result = Self.createPythonEnvironment(projectRoot: root)
            DispatchQueue.main.async {
                guard let self else { return }
                switch result {
                case .success:
                    self.pythonNotebookRuntime.status = .ready
                case let .failure(error):
                    self.pythonNotebookRuntime.status = .unavailable
                    self.state.lastErrors.append("Create project Python environment: \(error)")
                }
            }
        }
    }

    package func installProjectPythonPlottingPackages() {
        guard let root = state.scientificNotebooks?.projectRoot else { return }
        let executable = pythonExecutable(root: root)
        guard FileManager.default.isExecutableFile(atPath: executable) else {
            state.lastErrors.append("Create or repair the project Python environment first")
            return
        }
        pythonNotebookRuntime.status = .starting
        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            let result = Self.runPythonProcess(
                executable: executable,
                arguments: ["-m", "pip", "install", "--upgrade", "casa-rs-python[plot]"],
                currentDirectory: root
            )
            DispatchQueue.main.async {
                guard let self else { return }
                switch result {
                case .success:
                    self.pythonNotebookRuntime.status = .ready
                case let .failure(error):
                    self.pythonNotebookRuntime.status = .unavailable
                    self.state.lastErrors.append("Install project Python plotting packages: \(error)")
                }
            }
        }
    }

    package func runScientificPythonCell(_ cellID: String) {
        runScientificPythonCell(cellID, completion: nil)
    }

    package func runAllScientificPythonCells() {
        let cells = state.scientificNotebooks?.activeNotebook?.cells
            .filter { $0.kind == "python" }
            .map(\.id) ?? []
        runScientificPythonCells(cells[...])
    }

    package func interruptScientificPythonKernel() {
        guard let notebookID = state.scientificNotebooks?.activeNotebookID,
              let kernel = pythonKernels[notebookID]
        else { return }
        kernel.interrupt()
        DispatchQueue.main.asyncAfter(deadline: .now() + 2) { [weak self, weak kernel] in
            guard let self, let kernel,
                  self.pythonKernelStatuses[notebookID] == .interrupting
            else { return }
            kernel.terminate()
            self.pythonKernelStatuses[notebookID] = .restartRequired
            if self.state.scientificNotebooks?.activeNotebookID == notebookID {
                self.pythonNotebookRuntime.status = .restartRequired
                self.pythonNotebookRuntime.runningCellID = nil
            }
        }
    }

    package func restartScientificPythonKernel() {
        guard let notebookID = state.scientificNotebooks?.activeNotebookID,
              let kernel = pythonKernels[notebookID]
        else { return }
        kernel.restart()
        pythonNotebookRuntime.runningCellID = nil
    }

    package func saveMeasurementSetPlotToNotebook(
        datasetID: String,
        updating visualizationID: String? = nil,
        renderedImage: NotebookVisualizationImage
    ) {
        guard let project = state.scientificNotebooks,
              let plotState = state.measurementSetPlots[datasetID],
              let result = plotState.result
        else {
            state.lastErrors.append("Generate a MeasurementSet plot before saving it to a notebook")
            return
        }
        guard !renderedImage.data.isEmpty else {
            state.lastErrors.append("Render the MeasurementSet plot before saving it to a notebook")
            return
        }
        var reopenState = plotState
        reopenState.result = nil
        reopenState.status = .idle
        reopenState.lastError = nil
        saveVisualizationData(
            renderedImage.data,
            extension: renderedImage.fileExtension,
            title: result.title,
            notebookID: project.activeNotebookID,
            visualizationID: visualizationID,
            sourceReferences: [result.datasetPath],
            surface: "msexplore",
            parameters: Self.jsonValue(reopenState).objectValue ?? [:],
            renderer: renderedImage.renderer,
            mediaType: renderedImage.mediaType,
            width: renderedImage.width,
            height: renderedImage.height,
            settings: ["selection_summary": .string(result.selectionSummary)]
        )
    }

    package func reportMeasurementSetPlotSaveError(datasetID: String, message: String) {
        let diagnostic = "Save MeasurementSet plot to notebook: \(message)"
        state.lastErrors.append(diagnostic)
        state.measurementSetPlots[datasetID]?.lastError = diagnostic
        measurementSetPlotLogger.error("\(diagnostic, privacy: .public)")
    }

    package func saveImageExplorerToNotebook(
        datasetID: String,
        updating visualizationID: String? = nil
    ) {
        guard let project = state.scientificNotebooks,
              let dataset = state.project.datasets.first(where: { $0.id == datasetID }),
              let explorer = state.imageExplorers[datasetID],
              let snapshot = explorer.snapshot,
              let plane = snapshot.plane,
              let png = Self.imagePlanePNG(plane, colorMap: explorer.planeColorMap)
        else {
            state.lastErrors.append("Display an image plane before saving it to a notebook")
            return
        }
        let request = explorer.snapshotRequest(datasetPath: dataset.path)
        saveVisualizationData(
            png,
            extension: "png",
            title: "\(dataset.name) · image plane",
            notebookID: project.activeNotebookID,
            visualizationID: visualizationID,
            sourceReferences: [dataset.path],
            surface: "imexplore",
            parameters: Self.jsonValue(request).objectValue ?? [:],
            renderer: "casa-rs image plane",
            mediaType: "image/png",
            width: UInt32(clamping: plane.width),
            height: UInt32(clamping: plane.height),
            settings: [
                "color_map": .string(explorer.planeColorMap.rawValue),
                "clip_min": .number(plane.clipMin),
                "clip_max": .number(plane.clipMax),
            ]
        )
    }

    package func openNotebookVisualization(_ visualizationID: String) {
        guard let visualization = state.scientificNotebooks?.activeNotebook?.visualizations
            .first(where: { $0.id == visualizationID }),
              let revision = visualization.revisions.last,
              let source = revision.sourceReferences.first
        else { return }
        switch revision.reopen.surface {
        case "msexplore":
            if !state.project.datasets.contains(where: { $0.id == source }),
               let dataset = try? probeClient.probePath(path: source)
            {
                state.project.datasets.append(dataset)
            }
            guard let restored: MeasurementSetExplorerPlotState = Self.decodeJSONValue(
                .object(revision.reopen.parameters)
            ) else { return }
            state.measurementSetPlots[restored.datasetID] = restored
            measurementSetPlotSurfaceRequests.insert(restored.datasetID)
            openDatasetExplorer(restored.datasetID)
        case "imexplore":
            openImageExplorerPath(source)
            guard let request: ImageExplorerSnapshotRequest = Self.decodeJSONValue(
                .object(revision.reopen.parameters)
            ) else { return }
            if var explorer = state.imageExplorers[source] {
                explorer.selectedView = request.selectedView
                explorer.focus = request.focus
                explorer.planeContentMode = request.planeContentMode
                explorer.parameters = request.parameters
                explorer.cursorX = request.cursorX.map(Int.init)
                explorer.cursorY = request.cursorY.map(Int.init)
                explorer.selectedProfileAxis = request.selectedProfileAxis.map(Int.init)
                explorer.nonDisplayIndices = request.nonDisplayIndices.map(Int.init)
                state.imageExplorers[source] = explorer
                refreshImageExplorer(datasetID: source)
            }
        case "assistant_python_plot":
            openTab(WorkbenchTab(id: "tab-python", title: "Python", kind: .python))
        default:
            state.lastErrors.append("Unknown notebook visualization surface \(revision.reopen.surface)")
        }
    }

    package func shouldPresentMeasurementSetPlotSurface(datasetID: String) -> Bool {
        measurementSetPlotSurfaceRequests.contains(datasetID)
    }

    @discardableResult
    private func saveVisualizationData(
        _ data: Data,
        extension fileExtension: String,
        title: String,
        notebookID: String?,
        visualizationID: String?,
        sourceReferences: [String],
        surface: String,
        parameters: [String: JSONValue],
        renderer: String,
        mediaType: String,
        width: UInt32,
        height: UInt32,
        settings: [String: JSONValue]
    ) -> Bool {
        let temporary = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-visualization-\(UUID().uuidString).\(fileExtension)")
        defer { try? FileManager.default.removeItem(at: temporary) }
        do {
            try data.write(to: temporary, options: .atomic)
            _ = try notebookPersistenceClient.saveVisualization(request: NotebookSaveVisualizationEnvelope(
                projectRoot: state.project.rootPath,
                request: NotebookSaveVisualizationRequest(
                    notebookId: notebookID,
                    visualizationId: visualizationID,
                    title: title,
                    sourceAsset: temporary.path,
                    sourceReferences: sourceReferences,
                    reopen: NotebookVisualizationReopenIntent(
                        surface: surface,
                        contractVersion: 1,
                        parameters: parameters,
                        profileToml: nil
                    ),
                    render: NotebookVisualizationRenderMetadata(
                        renderer: renderer,
                        mediaType: mediaType,
                        width: width,
                        height: height,
                        settings: settings
                    )
                )
            ))
            loadScientificNotebooks()
            measurementSetPlotLogger.info(
                "Saved notebook visualization surface=\(surface, privacy: .public) bytes=\(data.count, privacy: .public)"
            )
            return true
        } catch {
            let diagnostic = "Save visualization to notebook: \(error)"
            state.lastErrors.append(diagnostic)
            measurementSetPlotLogger.error("\(diagnostic, privacy: .public)")
            return false
        }
    }

    private static func imagePlanePNG(
        _ plane: ImageExplorerPlane,
        colorMap: ImageExplorerColorMap
    ) -> Data? {
        guard plane.width > 0,
              plane.height > 0,
              plane.pixelsU8.count == Int(plane.width * plane.height),
              let bitmap = NSBitmapImageRep(
                bitmapDataPlanes: nil,
                pixelsWide: Int(plane.width),
                pixelsHigh: Int(plane.height),
                bitsPerSample: 8,
                samplesPerPixel: 4,
                hasAlpha: true,
                isPlanar: false,
                colorSpaceName: .deviceRGB,
                bytesPerRow: Int(plane.width) * 4,
                bitsPerPixel: 32
              ),
              let bytes = bitmap.bitmapData
        else { return nil }
        for (index, value) in plane.pixelsU8.enumerated() {
            let offset = index * 4
            let rgb = imagePlaneRGB(value, colorMap: colorMap)
            bytes[offset] = rgb.red
            bytes[offset + 1] = rgb.green
            bytes[offset + 2] = rgb.blue
            bytes[offset + 3] = 255
        }
        return bitmap.representation(using: .png, properties: [:])
    }

    private static func jsonValue<T: Encodable>(_ value: T) -> JSONValue {
        let encoder = JSONEncoder()
        return (try? encoder.encode(value))
            .flatMap { try? JSONSerialization.jsonObject(with: $0, options: .fragmentsAllowed) }
            .flatMap(JSONValue.foundationJSONValue)
            ?? .object([:])
    }

    private static func decodeJSONValue<T: Decodable>(_ value: JSONValue) -> T? {
        guard let data = try? JSONSerialization.data(
            withJSONObject: value.foundationJSONValue,
            options: .fragmentsAllowed
        ) else {
            return nil
        }
        return try? JSONDecoder().decode(T.self, from: data)
    }

    private func runScientificPythonCells(_ cellIDs: ArraySlice<String>) {
        guard let first = cellIDs.first else { return }
        runScientificPythonCell(first) { [weak self] in
            self?.runScientificPythonCells(cellIDs.dropFirst())
        }
    }

    private func runScientificPythonCell(_ cellID: String, completion: (() -> Void)?) {
        guard runtimeKind == .production,
              let project = state.scientificNotebooks,
              let document = project.activeNotebook,
              let cell = document.cells.first(where: { $0.id == cellID && $0.kind == "python" })
        else { return }
        let source = Self.pythonSource(from: cell.body)
        guard !source.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            state.lastErrors.append("Python cell \(cellID) has no executable source")
            completion?()
            return
        }
        guard let kernel = pythonKernel(notebookID: document.id, projectRoot: project.projectRoot)
        else {
            state.lastErrors.append("Create or repair the project Python environment before running this cell")
            completion?()
            return
        }
        pythonNotebookRuntime.runningCellID = cellID
        kernel.prepare { [weak self, weak kernel] result in
            guard let self, let kernel else { return }
            switch result {
            case let .failure(error):
                self.state.lastErrors.append("Start notebook Python kernel: \(error)")
                self.pythonNotebookRuntime.runningCellID = nil
                completion?()
            case let .success(environment):
                self.beginScientificPythonExecution(
                    cellID: cellID,
                    source: source,
                    environment: environment,
                    project: project,
                    kernel: kernel,
                    completion: completion
                )
            }
        }
    }

    private func beginScientificPythonExecution(
        cellID: String,
        source: String,
        environment: NotebookPythonEnvironmentIdentity,
        project: ScientificNotebookProjectState,
        kernel: PersistentPythonKernel,
        completion: (() -> Void)?
    ) {
        let sourceHash = Self.sha256(source)
        let inputs = state.selectedDataset.map { [$0.path] } ?? []
        let executionInput = NotebookExecutionInput(
            kind: "python",
            details: NotebookPythonExecutionInput(
                source: source,
                sourceSha256: sourceHash,
                authority: "user",
                inputReferences: inputs,
                environment: environment
            )
        )
        do {
            let result = try notebookPersistenceClient.beginRecording(request: NotebookBeginRecordingRequest(
                projectRoot: project.projectRoot,
                policy: .record,
                request: NotebookRecordingRequest(
                    initiatingSurface: "macos_gui",
                    operationId: "python.execute",
                    notebookId: project.activeNotebookID,
                    cellId: cellID,
                    taskIntent: nil,
                    executionInput: executionInput,
                    providerContractVersion: 1,
                    resolvedParameters: [:],
                    runSafety: NotebookRunSafetyRecord(
                        classification: "potentially_mutating_user_python",
                        affectedPaths: [project.projectRoot]
                    ),
                    approvals: [NotebookApprovalRecord(
                        kind: "user_action",
                        actor: "user",
                        timestamp: Self.unixMilliseconds(),
                        contentHash: sourceHash
                    )]
                )
            ))
            guard let handle = result.handle else {
                throw PythonKernelError.protocolFailure(result.warning ?? "recording did not start")
            }
            let assetDirectory = URL(fileURLWithPath: project.projectRoot)
                .appendingPathComponent(".casa-rs/notebook-runs/\(handle.runId)/assets", isDirectory: true)
            kernel.execute(
                executionID: handle.runId,
                source: source,
                artifactDirectory: assetDirectory.path
            ) { [weak self] executionResult in
                self?.finalizeScientificPythonExecution(
                    handle: handle,
                    projectRoot: project.projectRoot,
                    executionResult: executionResult,
                    completion: completion
                )
            }
        } catch {
            state.lastErrors.append("Record notebook Python execution: \(error)")
            pythonNotebookRuntime.runningCellID = nil
            completion?()
        }
    }

    private func finalizeScientificPythonExecution(
        handle: NotebookAttemptHandle,
        projectRoot: String,
        executionResult: Result<NotebookPythonCompletion, Error>,
        completion: (() -> Void)?
    ) {
        let execution: NotebookPythonCompletion
        switch executionResult {
        case let .success(value):
            execution = value
        case let .failure(error):
            execution = NotebookPythonCompletion(
                executionID: handle.runId,
                status: "interrupted",
                outputs: [],
                artifacts: [],
                diagnostic: error.localizedDescription
            )
        }
        let runRoot = URL(fileURLWithPath: projectRoot)
            .appendingPathComponent(".casa-rs/notebook-runs/\(handle.runId)", isDirectory: true)
        let outputPath = runRoot.appendingPathComponent("assets/ordered-output.json")
        var artifacts = execution.artifacts.map {
            NotebookReceiptArtifact(
                role: $0.role,
                path: Self.projectRelativePath($0.path, projectRoot: projectRoot),
                mediaType: $0.mediaType
            )
        }
        if let data = try? JSONEncoder().encode(execution.outputs) {
            try? FileManager.default.createDirectory(
                at: outputPath.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            try? data.write(to: outputPath, options: .atomic)
            artifacts.append(NotebookReceiptArtifact(
                role: "ordered_output",
                path: Self.projectRelativePath(outputPath.path, projectRoot: projectRoot),
                mediaType: "application/json"
            ))
        }
        do {
            try notebookPersistenceClient.finalizeRecording(request: NotebookFinalizeRecordingRequest(
                projectRoot: projectRoot,
                handle: handle,
                finalization: NotebookReceiptFinalization(
                    status: execution.status,
                    finishedAt: Self.unixMilliseconds(),
                    affectedPaths: artifacts.map(\.path),
                    products: [],
                    artifacts: artifacts,
                    diagnostics: execution.diagnostic.map { [$0] } ?? [],
                    stdout: Data(execution.outputs.filter { $0.channel == "stdout" }.map(\.text).joined().utf8),
                    stderr: Data(execution.outputs.filter { $0.channel == "stderr" }.map(\.text).joined().utf8),
                    casaLog: nil
                )
            ))
            loadScientificNotebooks()
        } catch {
            state.lastErrors.append("Finalize notebook Python execution: \(error)")
        }
        pythonNotebookRuntime.runningCellID = nil
        completion?()
    }

    private func pythonKernel(notebookID: String, projectRoot: String) -> PersistentPythonKernel? {
        if let kernel = pythonKernels[notebookID] { return kernel }
        let executable = pythonExecutable(root: projectRoot)
        guard FileManager.default.isExecutableFile(atPath: executable) else { return nil }
        let kernel = PersistentPythonKernel(pythonExecutable: executable, workspace: projectRoot)
        kernel.onStateChange { [weak self] status in
            guard let self else { return }
            self.pythonKernelStatuses[notebookID] = status
            if self.state.scientificNotebooks?.activeNotebookID == notebookID {
                self.pythonNotebookRuntime.notebookID = notebookID
                self.pythonNotebookRuntime.status = status
            }
        }
        pythonKernels[notebookID] = kernel
        return kernel
    }

    private func pythonExecutable(root: String) -> String {
        pythonExecutableOverride
            ?? URL(fileURLWithPath: root)
                .appendingPathComponent(".casa-rs/python/bin/python3")
                .path
    }

    private static func pythonSource(from body: String) -> String {
        var lines = body.split(separator: "\n", omittingEmptySubsequences: false)
        while lines.last?.isEmpty == true { lines.removeLast() }
        guard let first = lines.first,
              first.trimmingCharacters(in: .whitespaces).hasPrefix("```python"),
              let last = lines.last,
              last.trimmingCharacters(in: .whitespaces).hasPrefix("```")
        else { return body }
        return lines.dropFirst().dropLast().joined(separator: "\n") + "\n"
    }

    private static func sha256(_ value: String) -> String {
        SHA256.hash(data: Data(value.utf8)).map { String(format: "%02x", $0) }.joined()
    }

    private static func projectRelativePath(_ path: String, projectRoot: String) -> String {
        let prefix = projectRoot.hasSuffix("/") ? projectRoot : projectRoot + "/"
        return path.hasPrefix(prefix) ? String(path.dropFirst(prefix.count)) : path
    }

    private static func createPythonEnvironment(projectRoot: String) -> Result<Void, Error> {
        let xcrun = Process()
        let stdout = Pipe()
        xcrun.executableURL = URL(fileURLWithPath: "/usr/bin/xcrun")
        xcrun.arguments = ["-f", "python3"]
        xcrun.standardOutput = stdout
        do {
            try xcrun.run()
            xcrun.waitUntilExit()
            guard xcrun.terminationStatus == 0 else {
                return .failure(PythonKernelError.exited(xcrun.terminationStatus))
            }
            let base = String(
                decoding: stdout.fileHandleForReading.readDataToEndOfFile(),
                as: UTF8.self
            ).trimmingCharacters(in: .whitespacesAndNewlines)
            let environment = URL(fileURLWithPath: projectRoot)
                .appendingPathComponent(".casa-rs/python")
                .path
            return runPythonProcess(
                executable: base,
                arguments: ["-m", "venv", "--upgrade-deps", environment],
                currentDirectory: projectRoot
            )
        } catch {
            return .failure(error)
        }
    }

    private static func runPythonProcess(
        executable: String,
        arguments: [String],
        currentDirectory: String
    ) -> Result<Void, Error> {
        let process = Process()
        let stderrURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-python-\(UUID().uuidString).stderr")
        FileManager.default.createFile(atPath: stderrURL.path, contents: nil)
        defer { try? FileManager.default.removeItem(at: stderrURL) }
        process.executableURL = URL(fileURLWithPath: executable)
        process.arguments = arguments
        process.currentDirectoryURL = URL(fileURLWithPath: currentDirectory)
        process.standardOutput = FileHandle.nullDevice
        do {
            let stderr = try FileHandle(forWritingTo: stderrURL)
            defer { try? stderr.close() }
            process.standardError = stderr
            try process.run()
            process.waitUntilExit()
            try stderr.synchronize()
            let stderrData = (try? Data(contentsOf: stderrURL)) ?? Data()
            guard process.terminationStatus == 0 else {
                let message = String(
                    decoding: stderrData,
                    as: UTF8.self
                )
                return .failure(PythonKernelError.protocolFailure(message))
            }
            return .success(())
        } catch {
            return .failure(error)
        }
    }

    package func setScientificNotebookDraft(_ markdown: String) {
        let projectedCells = try? notebookPersistenceClient.projectCells(source: markdown)
        updateActiveScientificNotebook { document in
            document.draftSource = markdown
            document.cells = projectedCells ?? []
            document.conflict = nil
        }
    }

    package func setScientificPythonSource(cellID: String, source: String) {
        guard let document = state.scientificNotebooks?.activeNotebook,
              let cell = document.cells.first(where: { $0.id == cellID && $0.kind == "python" })
        else { return }
        let replacement = "```python\n\(source)\(source.hasSuffix("\n") ? "" : "\n")```\n"
        guard let bodyRange = document.draftSource.range(of: cell.body) else { return }
        var markdown = document.draftSource
        markdown.replaceSubrange(bodyRange, with: replacement)
        setScientificNotebookDraft(markdown)
    }

    package func setScientificNotebookViewMode(_ mode: NotebookDocumentViewMode) {
        updateActiveScientificNotebook { $0.viewMode = mode }
    }

    package func saveScientificNotebook(
        resolution: NotebookConflictResolution = .reject
    ) {
        guard let projectRoot = state.scientificNotebooks?.projectRoot,
              let document = state.scientificNotebooks?.activeNotebook
        else { return }
        do {
            switch try notebookPersistenceClient.save(
                projectRoot: projectRoot,
                document: document,
                resolution: resolution
            ) {
            case let .saved(saved), let .reloaded(saved):
                replaceScientificNotebook(saved)
            case let .conflict(conflict):
                updateActiveScientificNotebook { $0.conflict = conflict }
            }
        } catch {
            state.lastErrors.append("Save notebook \(document.filename): \(error)")
        }
    }

    package func resolveScientificNotebookConflict(keepingDraft: Bool) {
        saveScientificNotebook(resolution: keepingDraft ? .keepLocal : .reloadExternal)
    }

    package func openScientificNotebookTask(cellID: String) {
        guard let project = state.scientificNotebooks,
              let document = project.activeNotebook,
              let cell = document.cells.first(where: { $0.id == cellID }),
              let intent = cell.taskIntent
        else {
            state.lastErrors.append("No task parameters exist for notebook cell \(cellID)")
            return
        }
        guard state.applicationCatalog.contains(where: { $0.id == intent.surface }) else {
            state.lastErrors.append("Notebook task \(intent.surface) is not in the current task catalog")
            return
        }
        if let tutorial = state.activeTutorialProject?.tutorial,
           let section = tutorial.sections.first(where: { $0.cellIds.contains(cellID) }),
           section.datasetIds.contains(where: { datasetID in
               tutorial.datasets.first(where: { $0.id == datasetID })?.staged != true
           })
        {
            state.lastErrors.append(
                "Acquire and verify tutorial section datasets before opening task cell \(cellID)"
            )
            return
        }
        let receipt = document.receipts
            .filter({ $0.cellId == cellID })
            .max(by: { $0.revision < $1.revision })
        let sourcePath = "\(project.projectRoot)/notebooks/\(document.filename)#\(cellID)"

        do {
            let prepared = try prepareNotebookTask(
                intent: intent,
                sourcePath: sourcePath
            )
            let matchingTabs = state.tabs.filter {
                $0.kind == .task && $0.taskID == intent.surface
            }
            if matchingTabs.count == 1,
               let target = matchingTabs.first,
               let current = parameterSession(surfaceID: intent.surface, instanceID: target.id),
               current.snapshot.dirty
            {
                state.pendingNotebookTaskReplacement = NotebookTaskReplacementPreview(
                    targetTabID: target.id,
                    cellID: cellID,
                    sourcePath: sourcePath,
                    intent: intent,
                    receipt: receipt,
                    differences: notebookTaskDifferences(
                        current: current,
                        notebook: prepared.snapshot
                    )
                )
                return
            }

            let targetTabID: String
            if matchingTabs.count == 1, let target = matchingTabs.first {
                targetTabID = target.id
            } else {
                targetTabID = nextTaskTabID()
                openTab(WorkbenchTab(
                    id: targetTabID,
                    title: taskTitle(intent.surface),
                    kind: .task,
                    datasetID: state.selectedDatasetID,
                    taskID: intent.surface
                ))
            }
            installNotebookTask(
                intent: intent,
                receipt: receipt,
                projectRoot: project.projectRoot,
                sourcePath: sourcePath,
                targetTabID: targetTabID,
                bundle: prepared.bundle,
                snapshot: prepared.snapshot
            )
        } catch {
            state.lastErrors.append("Load notebook parameters for \(intent.surface): \(error)")
        }
    }

    package func cancelNotebookTaskReplacement() {
        state.pendingNotebookTaskReplacement = nil
    }

    package func confirmNotebookTaskReplacement() {
        guard let preview = state.pendingNotebookTaskReplacement,
              let projectRoot = state.scientificNotebooks?.projectRoot,
              state.tabs.contains(where: {
                  $0.id == preview.targetTabID
                      && $0.kind == .task
                      && $0.taskID == preview.intent.surface
              })
        else {
            state.pendingNotebookTaskReplacement = nil
            return
        }
        do {
            let prepared = try prepareNotebookTask(
                intent: preview.intent,
                sourcePath: preview.sourcePath
            )
            installNotebookTask(
                intent: preview.intent,
                receipt: preview.receipt,
                projectRoot: projectRoot,
                sourcePath: preview.sourcePath,
                targetTabID: preview.targetTabID,
                bundle: prepared.bundle,
                snapshot: prepared.snapshot
            )
            state.pendingNotebookTaskReplacement = nil
        } catch {
            state.lastErrors.append("Replace task parameters for \(preview.intent.surface): \(error)")
        }
    }

    private func prepareNotebookTask(
        intent: NotebookTaskIntent,
        sourcePath: String
    ) throws -> (bundle: SurfaceParameterBundle, snapshot: SurfaceParameterSnapshot) {
        let bundle = try surfaceParameterClient.loadBundle(surfaceID: intent.surface)
        let snapshot = try surfaceParameterClient.load(
            surfaceID: intent.surface,
            profileTOML: intent.profileTOML,
            sourcePath: sourcePath
        )
        return (bundle, snapshot)
    }

    private func installNotebookTask(
        intent: NotebookTaskIntent,
        receipt: NotebookExecutionReceipt?,
        projectRoot: String,
        sourcePath: String,
        targetTabID: String,
        bundle: SurfaceParameterBundle,
        snapshot: SurfaceParameterSnapshot
    ) {
        state.activeTaskID = intent.surface
        loadTaskUISchemaIfNeeded(intent.surface, instanceID: targetTabID)
        let key = parameterSessionKey(surfaceID: intent.surface, instanceID: targetTabID)
        state.parameterSessions[key] = SurfaceParameterSession(
            bundle: bundle,
            snapshot: snapshot,
            selectedSource: .file,
            baseProfileTOML: intent.profileTOML,
            baseProfilePath: sourcePath,
            workspace: projectRoot,
            saveLast: true
        )
        applySelectedDatasetParameterContext(surfaceID: intent.surface, instanceID: targetTabID)
        activateTab(targetTabID)

        guard let receipt else { return }
        let currentContractVersion = UInt32(clamping: bundle.surface.contractVersion)
        if currentContractVersion != receipt.providerContractVersion {
            state.taskRun.warnings.append(
                "Notebook run used provider contract \(receipt.providerContractVersion); the installed contract is \(currentContractVersion). Review the typed parameter diff before running."
            )
        }
        if let currentSession = parameterSession(surfaceID: intent.surface, instanceID: targetTabID),
           currentSession.values.mapValues(JSONValue.init(parameterValue:)) != receipt.resolvedParameters {
            state.taskRun.warnings.append(
                "Current defaults or project context resolve differently from this historical notebook run."
            )
        }
    }

    private func notebookTaskDifferences(
        current: SurfaceParameterSession,
        notebook: SurfaceParameterSnapshot
    ) -> [NotebookTaskReplacementDiff] {
        var currentValues = current.snapshot.states.compactMapValues(\.value)
            .mapValues(JSONValue.init(parameterValue:))
        for (parameter, draft) in current.draftText {
            currentValues[parameter] = .string(draft)
        }
        let notebookValues = notebook.states.compactMapValues(\.value)
            .mapValues(JSONValue.init(parameterValue:))
        return Set(currentValues.keys).union(notebookValues.keys)
            .sorted()
            .compactMap { parameter in
                let currentValue = currentValues[parameter]
                let notebookValue = notebookValues[parameter]
                guard currentValue != notebookValue else { return nil }
                return NotebookTaskReplacementDiff(
                    parameter: parameter,
                    currentValue: currentValue,
                    notebookValue: notebookValue
                )
            }
    }

    private func updateActiveScientificNotebook(
        _ update: (inout NotebookDocumentState) -> Void
    ) {
        guard var project = state.scientificNotebooks,
              let activeID = project.activeNotebookID,
              let index = project.notebooks.firstIndex(where: { $0.id == activeID })
        else { return }
        update(&project.notebooks[index])
        state.scientificNotebooks = project
    }

    private func replaceScientificNotebook(_ replacement: NotebookDocumentState) {
        guard var project = state.scientificNotebooks,
              let index = project.notebooks.firstIndex(where: { $0.id == replacement.id })
        else { return }
        var document = replacement
        document.viewMode = project.notebooks[index].viewMode
        project.notebooks[index] = document
        project.activeNotebookID = document.id
        state.scientificNotebooks = project
    }

    package func selectPrototypeNotebook(_ notebookID: String) {
        guard runtimeKind == .notebookPrototype else { return }
        guard var projection = state.prototypeNotebook,
              projection.documents.contains(where: { $0.id == notebookID })
        else { return }
        projection.activeNotebookID = notebookID
        state.prototypeNotebook = projection
        if let tabIndex = state.tabs.firstIndex(where: { $0.kind == .notebook }) {
            state.tabs[tabIndex].title = projection.filename
            state.activeTabID = state.tabs[tabIndex].id
        } else {
            openDefaultTab(kind: .notebook)
        }
    }

    package func setPrototypeNotebookDraft(_ markdown: String) {
        guard runtimeKind == .notebookPrototype || runtimeKind == .aiPrototype else { return }
        updateActivePrototypeNotebook { document in
            document.draftMarkdown = markdown
            PrototypeScientificNotebookFixtureAdapter.synchronizeTaskCells(in: &document)
        }
    }

    package func setPrototypeNotebookViewMode(_ viewMode: PrototypeNotebookViewMode) {
        guard runtimeKind == .notebookPrototype || runtimeKind == .aiPrototype else { return }
        updateActivePrototypeNotebook { $0.viewMode = viewMode }
    }

    package func savePrototypeNotebookDraft() {
        guard runtimeKind == .notebookPrototype || runtimeKind == .aiPrototype else { return }
        guard state.prototypeNotebook?.hasExternalConflict == false else {
            state.lastErrors.append("Resolve the simulated external notebook conflict before saving")
            return
        }
        updateActivePrototypeNotebook { $0.savedMarkdown = $0.draftMarkdown }
    }

    package func resolvePrototypeNotebookConflict(keepingDraft: Bool) {
        guard runtimeKind == .notebookPrototype else { return }
        updateActivePrototypeNotebook { document in
            if !keepingDraft {
                document.draftMarkdown = document.savedMarkdown
            }
            PrototypeScientificNotebookFixtureAdapter.synchronizeTaskCells(in: &document)
            document.hasExternalConflict = false
        }
    }

    package func selectPrototypeNotebookReceipt(_ receiptID: String) {
        guard runtimeKind == .notebookPrototype || runtimeKind == .aiPrototype else { return }
        updateActivePrototypeNotebook { document in
            guard document.tasks.contains(where: { $0.id == receiptID }) else { return }
            document.selectedReceiptID = receiptID
        }
    }

    /// Opens an interactive task-shaped tab using only the fixture projection.
    /// No provider schema, parameter, dataset, or task adapter is consulted.
    package func openPrototypeNotebookTask(receiptID: String) {
        guard runtimeKind == .notebookPrototype || runtimeKind == .aiPrototype,
              let receipt = state.prototypeNotebook?.receipts.first(where: { $0.id == receiptID })
        else {
            state.lastErrors.append("Unknown prototype notebook task \(receiptID)")
            return
        }
        selectPrototypeNotebookReceipt(receiptID)
        var tab = WorkbenchTab(
            id: "tab-prototype-task-\(receipt.id)",
            title: receipt.title,
            kind: .task,
            taskID: receipt.taskID
        )
        tab.prototypeReceiptID = receipt.id
        openTab(tab)
    }

    /// Appends a deterministic revision to the selected fixture receipt.
    @discardableResult
    package func restartPrototypeNotebookTask(receiptID: String) -> String? {
        guard runtimeKind == .notebookPrototype,
              var projection = state.prototypeNotebook,
              let documentIndex = projection.documents.firstIndex(where: {
                  $0.tasks.contains(where: { $0.id == receiptID })
              }),
              let receiptIndex = projection.documents[documentIndex].tasks.firstIndex(where: {
                  $0.id == receiptID
              }),
              !projection.documents[documentIndex].tasks[receiptIndex].revisions.contains(where: {
                  $0.status == .running
              })
        else { return nil }
        let fixtureSequence = projection.nextSimulatedRunSequence
        projection.nextSimulatedRunSequence += 1
        let revisionSequence =
            (projection.documents[documentIndex].tasks[receiptIndex].revisions.map(\.sequence).max() ?? 0) + 1
        let revisionID = "execution-simulated-\(fixtureSequence)"
        projection.documents[documentIndex].tasks[receiptIndex].revisions.append(
            PrototypeNotebookExecutionRevision(
                id: revisionID,
                sequence: revisionSequence,
                timestamp: "2026-07-10 prototype \(fixtureSequence)",
                status: .running,
                summary: "Deterministic fixture rerun is in progress.",
                diagnostics: ["Prototype only: no task or project write was started."],
                logLines: ["Validated fixture request.", "Started simulated work."]
            )
        )
        projection.documents[documentIndex].selectedReceiptID = receiptID
        state.prototypeNotebook = projection
        DispatchQueue.main.asyncAfter(deadline: .now() + 5.0) { [weak self] in
            self?.completePrototypeNotebookTaskRevision(
                receiptID: receiptID,
                revisionID: revisionID
            )
        }
        return receiptID
    }

    package func completePrototypeNotebookTaskRun(receiptID: String) {
        guard let revisionID = runningPrototypeRevisionID(receiptID: receiptID) else { return }
        updatePrototypeNotebookRun(receiptID: receiptID, revisionID: revisionID, status: .succeeded)
    }

    /// Deterministic fixture-only completion hook. Delayed callbacks must name
    /// the exact revision they started so an obsolete callback can never
    /// complete a newer retry for the same receipt.
    package func completePrototypeNotebookTaskRevision(
        receiptID: String,
        revisionID: String
    ) {
        guard runtimeKind == .notebookPrototype else { return }
        updatePrototypeNotebookRun(
            receiptID: receiptID,
            revisionID: revisionID,
            status: .succeeded
        )
    }

    package func cancelPrototypeNotebookTaskRun(receiptID: String) {
        guard let revisionID = runningPrototypeRevisionID(receiptID: receiptID) else { return }
        updatePrototypeNotebookRun(receiptID: receiptID, revisionID: revisionID, status: .cancelled)
    }

    package func runningPrototypeRevisionID(receiptID: String) -> String? {
        guard runtimeKind == .notebookPrototype else { return nil }
        return state.prototypeNotebook?.task(receiptID: receiptID)?.revisions
            .last(where: { $0.status == .running })?.id
    }

    private func updateActivePrototypeNotebook(
        _ update: (inout PrototypeNotebookDocumentProjection) -> Void
    ) {
        guard var projection = state.prototypeNotebook,
              let index = projection.documents.firstIndex(where: { $0.id == projection.activeNotebookID })
        else { return }
        update(&projection.documents[index])
        state.prototypeNotebook = projection
    }

    private func updatePrototypeNotebookRun(
        receiptID: String,
        revisionID: String,
        status: PrototypeNotebookReceiptStatus
    ) {
        guard var projection = state.prototypeNotebook,
              let documentIndex = projection.documents.firstIndex(where: {
                  $0.tasks.contains(where: { $0.id == receiptID })
              }),
              let receiptIndex = projection.documents[documentIndex].tasks.firstIndex(where: {
                  $0.id == receiptID
              }),
              let revisionIndex = projection.documents[documentIndex].tasks[receiptIndex].revisions.firstIndex(where: {
                  $0.id == revisionID
              }),
              projection.documents[documentIndex].tasks[receiptIndex].revisions[revisionIndex].status == .running
        else { return }
        var revision = projection.documents[documentIndex].tasks[receiptIndex].revisions[revisionIndex]
        revision.status = status
        switch status {
        case .succeeded:
            revision.summary = "Fixture run completed and registered two simulated products."
            revision.products = [
                "products/\(receiptID)-revision-\(revision.sequence).image",
                "products/\(receiptID)-revision-\(revision.sequence).weight",
            ]
            revision.logLines.append("Completed simulated work without executing a task.")
        case .cancelled:
            revision.summary = "User cancelled the fixture run; no products were registered."
            revision.products = []
            revision.logLines.append("Cancellation acknowledged by the fixture adapter.")
        case .running, .failed:
            break
        }
        projection.documents[documentIndex].tasks[receiptIndex].revisions[revisionIndex] = revision
        state.prototypeNotebook = projection
    }

    package func loadAssistantDiscussions() {
        guard runtimeKind == .production, state.hasProject else { return }
        do {
            let conversations = try assistantPersistenceClient.conversations(
                projectRoot: state.project.rootPath
            )
            var discussion = state.assistantDiscussion ?? AssistantDiscussionState()
            discussion.conversations = conversations
            if !conversations.contains(where: { $0.id == discussion.activeConversationID }) {
                discussion.activeConversationID = conversations.last?.id
            }
            discussion.contexts = assistantOpenTabContexts()
            state.assistantDiscussion = discussion
        } catch {
            let message = "Load assistant conversations: \(error)"
            state.lastErrors.append(message)
            if state.assistantDiscussion == nil { state.assistantDiscussion = AssistantDiscussionState() }
            state.assistantDiscussion?.lastError = message
        }
    }

    package func openAssistantDiscussion(
        presentation: AssistantDiscussionPresentation = .drawer
    ) {
        guard state.hasProject else { return }
        if state.assistantDiscussion == nil
            || state.assistantDiscussion?.presentation == .closed
        {
            loadAssistantDiscussions()
        }
        if state.assistantDiscussion?.activeConversation == nil { newAssistantConversation() }
        state.assistantDiscussion?.presentation = presentation
        if fullyRefreshedAssistantCorpusProject != state.project.rootPath {
            requestAssistantCorpusRefresh(.allLayers)
        }
        prepareAgentSession()
    }

    package func closeAssistantDiscussion() {
        state.assistantDiscussion?.presentation = .closed
    }

    package func dockAssistantDiscussion() {
        openAssistantDiscussion(presentation: .drawer)
    }

    package func expandAssistantDiscussion() {
        openAssistantDiscussion(presentation: .tab)
    }

    package func setAssistantDraft(_ draft: String) {
        updateActiveAssistantConversation { $0.draft = draft }
        scheduleAssistantDraftSave()
    }

    package func setAssistantScrollAnchor(_ messageID: String?) {
        updateActiveAssistantConversation { $0.scrollAnchorMessageId = messageID }
    }

    package func selectAssistantConversation(_ id: String) {
        guard state.assistantDiscussion?.conversations.contains(where: { $0.id == id }) == true else {
            return
        }
        state.assistantDiscussion?.activeConversationID = id
        state.assistantDiscussion?.contexts = assistantOpenTabContexts()
        restartActiveAgentConversation()
    }

    package func newAssistantConversation() {
        do {
            var profile = AssistantSessionProfileState()
            if let model = state.assistantDiscussion?.models.first(where: \.isDefault)
                ?? state.assistantDiscussion?.models.first
            {
                profile.model = model.id
                profile.effort = model.defaultEffort
            }
            let conversation = try assistantPersistenceClient.createConversation(
                projectRoot: state.project.rootPath,
                title: "Project discussion",
                attachment: assistantPrimaryAttachment(),
                profile: profile
            )
            if state.assistantDiscussion == nil { state.assistantDiscussion = AssistantDiscussionState() }
            state.assistantDiscussion?.conversations.append(conversation)
            state.assistantDiscussion?.activeConversationID = conversation.id
            state.assistantDiscussion?.contexts = assistantOpenTabContexts()
            restartActiveAgentConversation()
        } catch {
            recordAssistantError("Create discussion: \(error)")
        }
    }

    package func selectAssistantModel(_ modelID: String) {
        updateActiveAssistantConversation { conversation in
            conversation.profile.model = modelID
            if let model = state.assistantDiscussion?.models.first(where: { $0.id == modelID }) {
                conversation.profile.effort = model.defaultEffort
            }
        }
        persistActiveAssistantConversation()
    }

    package func selectAssistantEffort(_ effort: String) {
        updateActiveAssistantConversation { $0.profile.effort = effort }
        persistActiveAssistantConversation()
    }

    package func selectAssistantAuthority(_ authority: AssistantAuthorityState) {
        updateActiveAssistantConversation {
            $0.profile.authority = authority
            $0.backendSession = nil
        }
        persistActiveAssistantConversation()
        restartActiveAgentConversation()
    }

    package func setAssistantAgentCommand(_ command: String) {
        let command = command.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !command.isEmpty,
              command != state.assistantDiscussion?.activeConversation?.profile.agentCommand
        else { return }
        updateActiveAssistantConversation {
            $0.profile.agentCommand = command
            $0.backendSession = nil
        }
        persistActiveAssistantConversation()
        assistantController.session?.terminate()
        assistantController.session = nil
        assistantController.activeAgentCommand = nil
        assistantController.cancelConversationStart()
        prepareAgentSession()
    }

    package func setAssistantPythonCommand(_ command: String) {
        let command = command.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !command.isEmpty,
              command != state.assistantDiscussion?.activeConversation?.profile.pythonCommand
        else { return }
        updateActiveAssistantConversation {
            $0.profile.pythonCommand = command
            $0.profile.pythonProvenance = nil
            $0.backendSession = nil
        }
        persistActiveAssistantConversation()
        probeAssistantPythonIfNeeded()
        restartActiveAgentConversation()
    }

    package func toggleAssistantContext(_ contextID: String) {
        guard var discussion = state.assistantDiscussion,
              discussion.contexts.contains(where: { $0.id == contextID })
        else { return }
        if discussion.selectedContextIDs.contains(contextID) {
            discussion.selectedContextIDs.remove(contextID)
        } else {
            discussion.selectedContextIDs.insert(contextID)
        }
        let selected = discussion.contexts
            .filter { discussion.selectedContextIDs.contains($0.id) }
            .map(\.id)
        state.assistantDiscussion = discussion
        updateActiveAssistantConversation { $0.selectedContextIds = selected }
        persistActiveAssistantConversation()
        writeAssistantContextProjection()
        scheduleAssistantResponseTimeout()
    }

    package func refreshAssistantDiscussionContexts() {
        refreshAssistantContextItems()
        writeAssistantContextProjection()
    }

    package func refreshAssistantCorpus() {
        requestAssistantCorpusRefresh(.allLayers)
    }

    private func startProjectCorpusWatcher() {
        guard runtimeKind == .production, state.hasProject else { return }
        let watcher = ProjectCorpusWatcher(projectRoot: state.project.rootPath) { [weak self] in
            DispatchQueue.main.async {
                self?.requestAssistantCorpusRefresh(.projectDocuments)
            }
        }
        projectCorpusWatcher = watcher
        watcher.start()
    }

    private func requestAssistantCorpusRefresh(_ request: AssistantCorpusRefreshRequest) {
        guard runtimeKind == .production, state.hasProject else { return }
        if state.assistantDiscussion == nil { state.assistantDiscussion = AssistantDiscussionState() }
        guard let work = assistantController.corpusCoordinator.enqueue(request) else { return }
        runAssistantCorpusRefresh(work)
    }

    private func runAssistantCorpusRefresh(
        _ work: AssistantCorpusReconciliationCoordinator.Work
    ) {
        let projectRoot = state.project.rootPath
        state.assistantDiscussion?.corpusStatus = "Indexing local corpus…"
        state.assistantDiscussion?.corpusIndexReport = nil
        state.assistantDiscussion?.corpusDiagnostics = []
        assistantCorpusQueue.async { [weak self] in
            guard let self else { return }
            var diagnostics: [String] = []
            do {
                let ingestor = AssistantCorpusIngestor()
                let inventory = ingestor.projectDocumentInventory(projectRoot: projectRoot)
                let scope: AssistantCorpusRefreshScope = work.request == .allLayers
                    ? .allLayers : .projectDocuments
                let prepared = try self.assistantPersistenceClient.prepareCorpusReconciliation(
                    projectRoot: projectRoot,
                    sources: inventory.sources,
                    generation: work.generation,
                    scope: scope == .allLayers ? .allLayers : .projectDocuments
                )
                let result = ingestor.collect(
                    projectRoot: projectRoot,
                    projectInventory: inventory,
                    extractProjectPaths: Set(prepared.extractPaths),
                    scope: scope
                )
                diagnostics = result.diagnostics
                diagnostics.append(Self.assistantCorpusMetrics(result.metrics))
                let outcomes = prepared.extractPaths.map { path in
                    AssistantProjectSourceExtractionOutcome(
                        relativePath: path,
                        status: result.failedProjectSources.contains(path) ? .failed : .succeeded,
                        diagnostic: result.failedProjectSources.contains(path)
                            ? "Host extraction produced no stable content" : nil
                    )
                }
                let report = try self.assistantPersistenceClient.applyCorpusReconciliation(
                    projectRoot: projectRoot,
                    prepared: prepared,
                    documents: result.documents,
                    removeMissingLayers: result.refreshedLayers,
                    outcomes: outcomes
                )
                DispatchQueue.main.async {
                    if self.state.project.rootPath == projectRoot,
                       self.assistantController.corpusCoordinator.isCurrent(generation: work.generation) {
                        self.state.assistantDiscussion?.corpusStatus = Self.assistantCorpusStatus(report)
                        self.state.assistantDiscussion?.corpusIndexReport = report
                        self.state.assistantDiscussion?.corpusDiagnostics = diagnostics
                        if work.request == .allLayers {
                            self.fullyRefreshedAssistantCorpusProject = projectRoot
                        }
                    }
                    self.finishAssistantCorpusRefresh(generation: work.generation)
                }
            } catch {
                let retainedDiagnostics = diagnostics
                DispatchQueue.main.async {
                    if self.state.project.rootPath == projectRoot,
                       self.assistantController.corpusCoordinator.isCurrent(generation: work.generation) {
                        self.state.assistantDiscussion?.corpusStatus = "Local corpus refresh failed"
                        self.state.assistantDiscussion?.corpusIndexReport = nil
                        self.state.assistantDiscussion?.corpusDiagnostics = retainedDiagnostics
                        self.recordAssistantError("Refresh corpus: \(error)")
                    }
                    self.finishAssistantCorpusRefresh(generation: work.generation)
                }
            }
        }
    }

    private func finishAssistantCorpusRefresh(generation: UInt64) {
        guard let next = assistantController.corpusCoordinator.finish(generation: generation) else { return }
        runAssistantCorpusRefresh(next)
    }

    private static func assistantCorpusMetrics(_ metrics: AssistantCorpusRefreshMetricsState) -> String {
        "Project refresh: \(metrics.projectMetadataReads) metadata reads, "
            + "\(metrics.projectContentReads) content reads, "
            + "\(metrics.projectPDFExtractions) PDF extractions, "
            + "\(metrics.projectOCRCalls) OCR calls."
    }

    private static func assistantCorpusStatus(_ report: AssistantCorpusIndexReportState) -> String {
        let changed = report.indexedDocuments + report.removedDocuments
        return "Local corpus ready · \(report.chunkCount) chunks · \(changed) changed"
    }

    package func pinAssistantMessage(_ messageID: String) {
        guard let discussion = state.assistantDiscussion,
              let conversation = discussion.activeConversation,
              let message = conversation.messages.first(where: { $0.id == messageID }),
              let notebook = state.scientificNotebooks?.activeNotebook
        else { return }
        do {
            let markdown = try assistantPinMarkdown(
                message,
                conversationID: conversation.id
            )
            let pin = try assistantPersistenceClient.createPin(AssistantCreatePinRequest(
                conversationId: conversation.id,
                notebookId: notebook.id,
                messageId: message.id,
                representation: message.citations.isEmpty ? "answer_only" : "answer_with_citations",
                snapshotContent: markdown
            ))
            appendAssistantMarkdownAtNotebookTail(markdown)
            updateActiveAssistantConversation { transcript in
                guard let index = transcript.messages.firstIndex(where: { $0.id == message.id }) else {
                    return
                }
                transcript.messages[index].pins.append(pin)
            }
            persistActiveAssistantConversation()
        } catch {
            recordAssistantError("Add to notebook: \(error)")
        }
    }

    package func authenticateAssistantAccount() {
        assistantController.session?.requestAccountLogin()
    }

    package func logoutAssistantAccount() {
        assistantController.session?.requestAccountLogout()
    }

    package func sendAssistantPrompt() {
        guard var discussion = state.assistantDiscussion,
              var conversation = discussion.activeConversation
        else { return }
        let prompt = conversation.draft.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !prompt.isEmpty else { return }
        let message = AssistantMessageState(
            id: UUID().uuidString.lowercased(),
            role: "user",
            content: prompt,
            createdAt: assistantController.timestamp,
            agentId: nil,
            model: nil,
            citations: [],
            usedContext: [],
            activities: [],
            taskSuggestions: [],
            pins: []
        )
        conversation.messages.append(message)
        conversation.draft = ""
        conversation.updatedAt = assistantController.timestamp
        if let index = discussion.conversations.firstIndex(where: { $0.id == conversation.id }) {
            discussion.conversations[index] = conversation
        }
        discussion.activity = .streaming
        discussion.streamingText = ""
        discussion.liveActivity = AssistantActivityState(
            id: "request-sent",
            label: "Request sent",
            state: "running",
            summary: nil
        )
        discussion.lastActivityAt = assistantController.timestamp
        discussion.lastError = nil
        state.assistantDiscussion = discussion
        assistantController.beginPrompt()
        refreshAssistantContextItems()
        let selectedContexts = state.assistantDiscussion?.selectedContexts.map(\.id) ?? []
        updateActiveAssistantConversation { $0.selectedContextIds = selectedContexts }
        persistActiveAssistantConversation()
        writeAssistantContextProjection()

        if conversation.backendSession == nil {
            startActiveAgentConversation()
        } else if let threadID = conversation.backendSession?.sessionId {
            assistantController.session?.sendTurn(AgentTurnRequest(
                threadID: threadID,
                text: prompt,
                model: conversation.profile.model,
                effort: conversation.profile.effort
            ))
        }
    }

    package func cancelAssistantResponse() {
        guard let conversation = state.assistantDiscussion?.activeConversation,
              let threadID = conversation.backendSession?.sessionId,
              let turnID = state.assistantDiscussion?.activeTurnID
        else { return }
        assistantController.session?.cancel(threadID: threadID, turnID: turnID)
    }

    package func resolveAssistantApproval(_ decision: String) {
        guard let approval = state.assistantDiscussion?.pendingApproval else { return }
        assistantController.session?.approve(requestID: approval.id, decision: decision)
        state.assistantDiscussion?.pendingApproval = nil
    }

    package func restartAssistantAgent() {
        restartActiveAgentConversation()
    }

    private func prepareAgentSession() {
        let command = state.assistantDiscussion?.activeConversation?.profile.agentCommand ?? "codex"
        if assistantController.session == nil || assistantController.activeAgentCommand != command {
            assistantController.session?.terminate()
            assistantController.cancelConversationStart()
            do {
                let configuration = try AgentSessionConfiguration.discover(
                    preferredAgentCommand: command
                )
                let session: AgentSession = configuration.fixtureMode
                    ? DeterministicAgentSession()
                    : CodexAppServerSession(configuration: configuration)
                assistantController.session = session
                assistantController.activeAgentCommand = command
                configureAgentSession(session)
            } catch {
                recordAssistantError("Agent unavailable: \(error)")
                return
            }
        }
        assistantController.session?.prepare { [weak self] result in
            switch result {
            case .success:
                self?.startActiveAgentConversation()
            case let .failure(error):
                self?.recordAssistantError("Start agent: \(error)")
            }
        }
    }

    private func configureAgentSession(_ session: AgentSession) {
        session.onStateChange { [weak self] activity in
            self?.state.assistantDiscussion?.activity = activity
        }
        session.onEvent { [weak self] event in self?.handleAgentEvent(event) }
    }

    private func startActiveAgentConversation() {
        guard let conversation = state.assistantDiscussion?.activeConversation,
              let session = assistantController.session,
              assistantController.beginConversation()
        else { return }
        probeAssistantPythonIfNeeded()
        assistantController.replaceProjectNonce()
        state.assistantDiscussion?.contexts = assistantOpenTabContexts()
        writeAssistantContextProjection()
        session.startConversation(AgentConversationRequest(
            projectRoot: state.project.rootPath,
            model: conversation.profile.model,
            effort: conversation.profile.effort,
            resumeThreadID: conversation.backendSession?.sessionId,
            runtimeProfile: CasaAgentRuntimeProfile(
                authority: conversation.profile.authority,
                sessionNonce: assistantController.projectNonce,
                pythonCommand: conversation.profile.pythonCommand
            )
        ))
    }

    /// App Server retains project MCP processes for its live threads. Restart it
    /// before changing the nonce-bound runtime profile so only one project MCP
    /// helper is alive for this Workbench session.
    private func restartActiveAgentConversation() {
        assistantController.cancelResponseTimeout()
        assistantController.cancelConversationStart()
        assistantController.session?.restart()
        startActiveAgentConversation()
    }

    private func probeAssistantPythonIfNeeded() {
        guard let conversation = state.assistantDiscussion?.activeConversation,
              conversation.profile.pythonProvenance?.selectedCommand
                != conversation.profile.pythonCommand,
              state.project.rootPath.hasPrefix("/")
        else { return }
        let selectedCommand = conversation.profile.pythonCommand
        let conversationID = conversation.id
        guard let resolvedCommand = AgentSessionConfiguration.resolveExecutable(selectedCommand) else {
            recordAssistantError("Inspect selected Python: executable not found: \(selectedCommand)")
            return
        }
        let kernel = PersistentPythonKernel(
            pythonExecutable: resolvedCommand,
            workspace: state.project.rootPath
        )
        kernel.prepare { [weak self, kernel] result in
            defer { kernel.terminate() }
            guard let self,
                  self.state.assistantDiscussion?.activeConversation?.id == conversationID,
                  self.state.assistantDiscussion?.activeConversation?.profile.pythonCommand
                    == selectedCommand
            else { return }
            switch result {
            case let .success(identity):
                self.updateActiveAssistantConversation {
                    $0.profile.pythonProvenance = AssistantPythonProvenanceState(
                        selectedCommand: selectedCommand,
                        resolvedPath: identity.interpreter,
                        implementation: identity.implementation,
                        version: identity.version,
                        environmentLabel: Self.assistantPythonEnvironmentLabel(identity.interpreter),
                        casaRsVersion: identity.casaRsVersion,
                        packages: identity.packages
                    )
                }
                self.persistActiveAssistantConversation()
            case let .failure(error):
                self.recordAssistantError("Inspect selected Python: \(error)")
            }
        }
    }

    private static func assistantPythonEnvironmentLabel(_ interpreter: String) -> String {
        let executable = URL(fileURLWithPath: interpreter)
        let bin = executable.deletingLastPathComponent()
        let environment = bin.lastPathComponent == "bin" ? bin.deletingLastPathComponent() : bin
        let label = environment.lastPathComponent
        return label.isEmpty ? "System Python" : label
    }

    private func handleAgentEvent(_ event: AgentSessionEvent) {
        guard var discussion = state.assistantDiscussion else { return }
        let effects = assistantController.handle(event, discussion: &discussion)
        state.assistantDiscussion = discussion
        effects.forEach(performAssistantEffect)
    }

    private func performAssistantEffect(_ effect: AssistantControllerEffect) {
        switch effect {
        case .persistConversation:
            persistActiveAssistantConversation()
        case let .sendTurn(request):
            assistantController.session?.sendTurn(request)
        case let .openAuthenticationURL(value):
            if let url = URL(string: value) { NSWorkspace.shared.open(url) }
        case .refreshAccount:
            assistantController.session?.refreshAccount()
        case .restartConversation:
            restartActiveAgentConversation()
        case .scheduleStreamFlush:
            assistantController.scheduleStreamFlush { [weak self] in
                guard let self, var discussion = self.state.assistantDiscussion else { return }
                self.assistantController.flushPendingStream(into: &discussion)
                self.state.assistantDiscussion = discussion
            }
        case let .scheduleResponseTimeout(conversationID):
            assistantController.scheduleResponseTimeout(conversationID: conversationID) { [weak self] id in
                self?.handleAssistantResponseTimeout(conversationID: id)
            }
        }
    }

    private struct AssistantContextDraft {
        var id: String
        var kind: String
        var label: String
        var summary: String
        var excerpt: String
        var active: Bool
    }

    private func assistantOpenTabContextDrafts() -> [AssistantContextDraft] {
        var items: [AssistantContextDraft] = []
        for tab in state.tabs {
            let contextID = assistantContextID(tab)
            let item: AssistantContextDraft
            switch tab.kind {
            case .task:
                let taskID = tab.taskID ?? tab.title
                item = AssistantContextDraft(
                    id: contextID,
                    kind: "task",
                    label: tab.title,
                    summary: "Open task and its current parameters",
                    excerpt: assistantParameterSummary(surfaceID: taskID, instanceID: tab.id),
                    active: tab.id == state.activeTabID
                )
            case .notebook, .tutorial:
                let notebook = state.scientificNotebooks?.activeNotebook
                item = AssistantContextDraft(
                    id: contextID,
                    kind: tab.kind.rawValue,
                    label: notebook?.title ?? tab.title,
                    summary: tab.kind == .tutorial ? "Open tutorial notebook" : "Open scientific notebook",
                    excerpt: notebook?.draftSource ?? "",
                    active: tab.id == state.activeTabID
                )
            case .datasetExplorer, .tableBrowser:
                let dataset = tab.datasetID.flatMap { id in state.project.datasets.first { $0.id == id } }
                item = AssistantContextDraft(
                    id: contextID,
                    kind: "explorer",
                    label: tab.title,
                    summary: dataset.map { "Open \($0.kind.rawValue) dataset at \($0.path)" }
                        ?? "Open dataset explorer",
                    excerpt: assistantDatasetContext(datasetID: tab.datasetID, tabKind: tab.kind),
                    active: tab.id == state.activeTabID
                )
            case .plotSamples:
                let summaries = state.plotDocuments.map {
                    "\($0.title): \($0.subtitle) [\($0.allLayers.count) layers, \($0.panels.count) panels]"
                }
                item = AssistantContextDraft(
                    id: contextID,
                    kind: "plot",
                    label: tab.title,
                    summary: "Open scientific plot tab",
                    excerpt: summaries.joined(separator: "\n"),
                    active: tab.id == state.activeTabID
                )
            case .python:
                item = AssistantContextDraft(
                    id: contextID,
                    kind: "python",
                    label: tab.title,
                    summary: "Open user scientific Python tab",
                    excerpt: state.python.buffer,
                    active: tab.id == state.activeTabID
                )
            case .history:
                item = AssistantContextDraft(
                    id: contextID,
                    kind: "history",
                    label: tab.title,
                    summary: "Open task and plot execution history",
                    excerpt: assistantJSON(Array(state.jobs.values)),
                    active: tab.id == state.activeTabID
                )
            case .aiChat:
                item = AssistantContextDraft(
                    id: contextID,
                    kind: "assistant",
                    label: tab.title,
                    summary: "Current AI discussion tab",
                    excerpt: "The active conversation is already supplied by the agent session.",
                    active: tab.id == state.activeTabID
                )
            }
            items.append(item)
        }
        return items
    }

    private func assistantOpenTabContexts() -> [AssistantContextItemState] {
        let drafts = assistantOpenTabContextDrafts()
        let resourcePlan = assistantResourcePlanForOpenTabs(drafts)
        return drafts.map { draft in
            assistantContext(
                id: draft.id,
                kind: draft.kind,
                label: draft.label,
                summary: draft.summary,
                excerpt: draft.excerpt,
                excerptLimit: resourcePlan.contextUnits[draft.id] ?? 0
            )
        }
    }

    private func assistantContextID(_ tab: WorkbenchTab) -> String {
        switch tab.kind {
        case .task: "task:\(tab.id)"
        case .notebook, .tutorial: "\(tab.kind.rawValue):\(tab.id)"
        case .datasetExplorer, .tableBrowser: "\(tab.kind.rawValue):\(tab.id)"
        case .plotSamples: "plot:\(tab.id)"
        case .python: "python:\(tab.id)"
        case .history: "history:\(tab.id)"
        case .aiChat: "aiChat:\(tab.id)"
        }
    }

    private func assistantResourcePlanForOpenTabs(
        _ drafts: [AssistantContextDraft]? = nil
    ) -> AssistantResourcePlan {
        guard let discussion = state.assistantDiscussion else {
            return .unavailable("Assistant state is unavailable; no context resources were allocated.")
        }
        let selectedModelID = discussion.activeConversation?.profile.model
        let model = discussion.models.first { $0.id == selectedModelID }
            ?? discussion.models.first { $0.isDefault }
        guard let inputUnits = model?.inputCapacityUnits,
              let outputUnits = model?.outputReserveUnits
        else {
            return .unavailable(
                "The active backend did not report input and output capacity; context and corpus retrieval are disabled."
            )
        }
        let encodedConversationUnits = UInt64(
            (try? JSONEncoder().encode(discussion.activeConversation))?.count ?? 0
        )
        let runtimeProfile = CasaAgentRuntimeProfile(
            authority: discussion.activeConversation?.profile.authority ?? .work,
            sessionNonce: assistantController.projectNonce,
            pythonCommand: discussion.activeConversation?.profile.pythonCommand ?? "python3"
        )
        guard let instructionUnits = CodexAppServerSession.instructionResourceUnits(runtimeProfile)
        else {
            return .unavailable("Assistant instruction resource accounting overflowed.")
        }
        let drafts = drafts ?? assistantOpenTabContextDrafts()
        let initialSelection = discussion.contexts.isEmpty
        let requests = drafts.map { draft in
            AssistantContextResourceRequest(
                id: draft.id,
                desiredUnits: AssistantResourcePlanner.encodedStringUnits(draft.excerpt),
                selected: initialSelection || discussion.selectedContextIDs.contains(draft.id),
                active: draft.active
            )
        }
        let selectedDrafts = zip(drafts, requests).compactMap { pair in
            pair.1.selected ? pair.0 : nil
        }
        guard let metadataUnits = assistantContextMetadataUnits(selectedDrafts) else {
            return .unavailable("Assistant context metadata resource accounting failed.")
        }
        do {
            return try AssistantResourcePlanner.plan(
                capacity: AssistantModelCapacity(
                    inputUnits: inputUnits,
                    outputReserveUnits: outputUnits
                ),
                reservations: [
                    AssistantResourceReservation(
                        id: "runtime_instructions",
                        units: instructionUnits
                    ),
                    AssistantResourceReservation(
                        id: "conversation_history",
                        units: encodedConversationUnits
                    ),
                    AssistantResourceReservation(
                        id: "context_metadata",
                        units: metadataUnits
                    ),
                ],
                contexts: requests,
                corpusDesiredUnits: inputUnits
            )
        } catch {
            return .unavailable("Assistant resource planning failed: \(error)")
        }
    }

    private func assistantContextMetadataUnits(
        _ drafts: [AssistantContextDraft]
    ) -> UInt64? {
        let projections = drafts.map {
            AssistantContextTabProjection(
                id: $0.id,
                kind: $0.kind,
                label: $0.label,
                summary: $0.summary,
                excerpt: ""
            )
        }
        let encoder = JSONEncoder()
        encoder.keyEncodingStrategy = .convertToSnakeCase
        guard let data = try? encoder.encode(projections) else { return nil }
        return UInt64(data.count)
    }

    private func refreshAssistantContextItems() {
        guard var discussion = state.assistantDiscussion else { return }
        let previousIDs = Set(discussion.contexts.map(\.id))
        let items = assistantOpenTabContexts()
        let availableIDs = Set(items.map(\.id))
        if previousIDs.isEmpty {
            let persistedIDs = Set(discussion.activeConversation?.selectedContextIds ?? [])
            discussion.selectedContextIDs = persistedIDs.isEmpty
                ? availableIDs
                : persistedIDs.intersection(availableIDs)
        } else {
            let retainedIDs = discussion.selectedContextIDs.intersection(availableIDs)
            let newIDs = availableIDs.subtracting(previousIDs)
            discussion.selectedContextIDs = retainedIDs.union(newIDs)
        }
        discussion.contexts = items
        state.assistantDiscussion = discussion
    }

    private func assistantDatasetContext(datasetID: String?, tabKind: WorkbenchTabKind) -> String {
        guard let datasetID,
              let dataset = state.project.datasets.first(where: { $0.id == datasetID })
        else { return "{}" }
        var sections = [assistantJSON(dataset)]
        if let plot = state.measurementSetPlots[datasetID] {
            sections.append(assistantJSON([
                "preset": plot.preset.rawValue,
                "field": plot.selectedField ?? "",
                "spectral_window": plot.selectedSpectralWindow ?? "",
                "correlation": plot.selectedCorrelation ?? "",
                "data_column": plot.dataColumn,
                "selection": plot.result?.selectionSummary ?? "",
            ]))
        }
        if let image = state.imageExplorers[datasetID] {
            sections.append(assistantJSON(image.parameters))
        }
        if tabKind == .tableBrowser, let table = state.tableBrowsers[datasetID] {
            sections.append(assistantJSON([
                "selected_view": table.selectedView,
                "profile_view": table.profileView,
                "linked_table": table.linkedTable,
                "content_mode": table.contentMode,
            ]))
        }
        return sections.joined(separator: "\n")
    }

    private func assistantJSON<T: Encodable>(_ value: T) -> String {
        (try? String(decoding: JSONEncoder().encode(value), as: UTF8.self)) ?? "{}"
    }

    private func assistantContext(
        id: String,
        kind: String,
        label: String,
        summary: String,
        excerpt: String,
        excerptLimit: UInt64
    ) -> AssistantContextItemState {
        let bounded = AssistantResourcePlanner.truncate(excerpt, unitLimit: excerptLimit)
        return AssistantContextItemState(
            id: id,
            kind: kind,
            label: label,
            summary: summary,
            excerpt: bounded,
            byteCount: UInt64(bounded.utf8.count),
            contentSha256: SHA256.hash(data: Data(bounded.utf8))
                .map { String(format: "%02x", $0) }
                .joined(),
            untrustedEvidence: true
        )
    }

    private func assistantParameterSummary(surfaceID: String, instanceID: String) -> String {
        guard let parameters = state.parameterSessions[parameterSessionKey(
            surfaceID: surfaceID,
            instanceID: instanceID
        )] else { return "{}" }
        let names = Set(parameters.values.keys).union(parameters.draftText.keys)
        return names.sorted().map { name in
            let value = parameters.text(for: name)
            return "\(name) = \(value)"
        }.joined(separator: "\n")
    }

    private func assistantPrimaryAttachment() -> AssistantAttachmentState {
        if let notebook = state.scientificNotebooks?.activeNotebook {
            return AssistantAttachmentState(
                kind: "notebook",
                identifier: notebook.filename,
                label: notebook.title,
                primary: true
            )
        }
        return AssistantAttachmentState(
            kind: "project",
            identifier: state.project.rootPath,
            label: state.project.name,
            primary: true
        )
    }

    private func writeAssistantContextProjection() {
        guard state.hasProject, state.project.rootPath.hasPrefix("/") else { return }
        let root = URL(fileURLWithPath: state.project.rootPath, isDirectory: true)
        let path = root.appendingPathComponent(".casa-rs/assistant-context.json")
        let selected = state.assistantDiscussion?.selectedContexts ?? []
        let resourcePlan = assistantResourcePlanForOpenTabs()
        let projection = AssistantContextProjectionState(
            schemaVersion: 1,
            sessionNonce: assistantController.projectNonce,
            openTabs: selected.map {
                AssistantContextTabProjection(
                    id: $0.id,
                    kind: $0.kind,
                    label: $0.label,
                    summary: $0.summary,
                    excerpt: $0.excerpt
                )
            },
            dataSemantics: selected.filter { ["explorer", "plot"].contains($0.kind) }.map {
                AssistantDataSemanticProjection(
                    id: $0.id,
                    label: $0.label,
                    summary: $0.summary,
                    semantics: $0.excerpt
                )
            },
            receipts: (state.scientificNotebooks?.notebooks ?? []).map {
                AssistantNotebookReceiptsProjection(
                    notebookId: $0.id,
                    notebook: $0.filename,
                    receipts: $0.receipts
                )
            },
            resourcePlan: AssistantContextResourcePlanProjection(
                schemaVersion: 1,
                corpusTextUnits: resourcePlan.corpusUnits,
                diagnostics: resourcePlan.diagnostics
            ),
            actionCatalog: [
                AssistantActionProjection(
                    id: "task.suggest",
                    owner: "casa_rs_mcp",
                    effect: "Open canonical task tab with suggested non-default parameters",
                    requiresUserInteraction: true
                ),
                AssistantActionProjection(
                    id: "notebook.append_assistant_message",
                    owner: "workbench",
                    effect: "Append an immutable AI snapshot to the active notebook tail",
                    requiresUserInteraction: true
                ),
                AssistantActionProjection(
                    id: "plot.save_or_update_notebook",
                    owner: "workbench",
                    effect: "Save a plot snapshot or explicitly update an existing notebook plot",
                    requiresUserInteraction: true
                ),
                AssistantActionProjection(
                    id: "tutorial.acquire_dataset",
                    owner: "workbench",
                    effect: "Run the tutorial dataset acquisition workflow",
                    requiresUserInteraction: true
                ),
            ]
        )
        do {
            try FileManager.default.createDirectory(
                at: path.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            let encoder = JSONEncoder()
            encoder.keyEncodingStrategy = .convertToSnakeCase
            encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
            let data = try encoder.encode(projection)
            try data.write(to: path, options: .atomic)
        } catch {
            recordAssistantError("Write agent context: \(error)")
        }
    }

    private func assistantPinMarkdown(
        _ message: AssistantMessageState,
        conversationID: String
    ) throws -> String {
        var markdown = """


        <!-- casa-rs-ai-pin:v1 conversation=\(conversationID) message=\(message.id) -->
        > [!NOTE]
        > AI discussion snapshot

        \(message.content)
        """
        for (index, citation) in message.citations.enumerated() {
            markdown += "\n\n[\(index + 1)] \(citation.label), \(citation.locator)"
        }
        for suggestion in message.taskSuggestions {
            markdown += try assistantTaskCellMarkdown(suggestion)
        }
        return markdown + "\n"
    }

    private func assistantTaskCellMarkdown(_ suggestion: AssistantTaskSuggestionState) throws -> String {
        let bundle = try surfaceParameterClient.loadBundle(surfaceID: suggestion.taskId)
        var parameters: [String: JSONValue] = [:]
        for (name, text) in suggestion.parameters {
            guard let concept = bundle.concept(for: name) else {
                throw AssistantNotebookPinError.invalidTaskSuggestion(
                    "unknown parameter \(name) for \(suggestion.taskId)"
                )
            }
            parameters[name] = JSONValue(parameterValue: concept.valueDomain.value(from: text))
        }
        let intent = NotebookTaskIntent(
            format: 1,
            surface: suggestion.taskId,
            kind: bundle.surface.kind,
            contract: UInt32(clamping: bundle.surface.contractVersion),
            parameters: parameters
        )
        return """


        <!-- casa-rs-cell:v1 id=\(UUID().uuidString.lowercased()) kind=task -->
        ```toml
        \(intent.profileTOML)```
        <!-- /casa-rs-cell -->
        """
    }

    private func appendAssistantMarkdownAtNotebookTail(_ markdown: String) {
        guard let notebook = state.scientificNotebooks?.activeNotebook else { return }
        setScientificNotebookDraft(
            notebook.draftSource.trimmingCharacters(in: .newlines) + markdown
        )
        saveScientificNotebook()
    }

    private func updateActiveAssistantConversation(
        _ update: (inout AssistantConversationState) -> Void
    ) {
        guard var discussion = state.assistantDiscussion,
              let id = discussion.activeConversationID,
              let index = discussion.conversations.firstIndex(where: { $0.id == id })
        else { return }
        update(&discussion.conversations[index])
        state.assistantDiscussion = discussion
    }

    private func persistActiveAssistantConversation() {
        guard let conversation = state.assistantDiscussion?.activeConversation else { return }
        do {
            try assistantPersistenceClient.saveConversation(
                projectRoot: state.project.rootPath,
                transcript: conversation
            )
        } catch {
            recordAssistantError("Save discussion: \(error)")
        }
    }

    private func scheduleAssistantDraftSave() {
        assistantController.scheduleDraftSave { [weak self] in
            self?.persistActiveAssistantConversation()
        }
    }

    private func recordAssistantError(_ message: String) {
        if state.assistantDiscussion == nil { state.assistantDiscussion = AssistantDiscussionState() }
        guard var discussion = state.assistantDiscussion else { return }
        assistantController.recordError(message, discussion: &discussion)
        state.assistantDiscussion = discussion
    }

    private func scheduleAssistantResponseTimeout() {
        guard state.assistantDiscussion?.activity == .streaming else { return }
        let conversationID = state.assistantDiscussion?.activeConversation?.id
        assistantController.scheduleResponseTimeout(conversationID: conversationID) { [weak self] id in
            self?.handleAssistantResponseTimeout(conversationID: id)
        }
    }

    private func handleAssistantResponseTimeout(conversationID: String?) {
        guard state.assistantDiscussion?.activity == .streaming,
              state.assistantDiscussion?.activeConversation?.id == conversationID
        else { return }
        if let threadID = state.assistantDiscussion?.activeConversation?.backendSession?.sessionId,
           let turnID = state.assistantDiscussion?.activeTurnID
        {
            assistantController.session?.cancel(threadID: threadID, turnID: turnID)
        } else {
            assistantController.session?.restart()
            assistantController.cancelConversationStart()
        }
        state.assistantDiscussion?.activeTurnID = nil
        recordAssistantError(
            "The assistant did not report any activity for two minutes. Restart the agent and try again."
        )
    }

    package func parameterIsAssistantSuggested(
        surfaceID: String,
        instanceID: String,
        name: String
    ) -> Bool {
        assistantController.isParameterSuggested(
            sessionKey: parameterSessionKey(surfaceID: surfaceID, instanceID: instanceID),
            name: name
        )
    }

    private func clearAssistantSuggestedParameters(sessionKey: String, names: Set<String>) {
        assistantController.clearSuggestedParameters(sessionKey: sessionKey, names: names)
    }

    package func openAssistantTaskSuggestion(messageID: String, suggestionID: String) {
        guard let suggestion = state.assistantDiscussion?.activeConversation?.messages
            .first(where: { $0.id == messageID })?.taskSuggestions
            .first(where: { $0.id == suggestionID }),
              state.applicationCatalog.contains(where: { $0.id == suggestion.taskId })
        else {
            recordAssistantError("The suggested task is not available in this build")
            return
        }
        let tabID = "tab-assistant-\(suggestion.taskId)-\(UUID().uuidString.lowercased())"
        guard applyAssistantTaskSuggestion(suggestion, instanceID: tabID) else { return }
        openTab(WorkbenchTab(
            id: tabID,
            title: taskTitle(suggestion.taskId),
            kind: .task,
            taskID: suggestion.taskId
        ))
        selectTask(suggestion.taskId, tabID: tabID)
        assistantController.setSuggestedParameters(
            sessionKey: parameterSessionKey(surfaceID: suggestion.taskId, instanceID: tabID),
            names: Set(suggestion.parameters.keys)
        )
    }

    private func applyAssistantTaskSuggestion(
        _ suggestion: AssistantTaskSuggestionState,
        instanceID: String
    ) -> Bool {
        let sessionKey = parameterSessionKey(
            surfaceID: suggestion.taskId,
            instanceID: instanceID
        )
        do {
            guard let validatedPatch = suggestion.validatedPatch else {
                recordAssistantError("The suggested task has no canonical validated patch")
                return false
            }
            let bundle = try surfaceParameterClient.loadBundle(surfaceID: suggestion.taskId)
            let defaults = try surfaceParameterClient.defaults(surfaceID: suggestion.taskId)
            var session = SurfaceParameterSession(
                bundle: bundle,
                snapshot: defaults,
                selectedSource: .defaults,
                baseProfileTOML: nil,
                baseProfilePath: nil,
                workspace: parameterWorkspacePath()
            )
            session.overridePatch = validatedPatch
            guard resolveParameterSession(
                &session,
                editedParameters: Set(suggestion.parameters.keys)
            ) else { return false }
            let errors = session.snapshot.diagnostics
                .filter { $0.level == "error" }
                .map(\.message)
            guard errors.isEmpty else {
                recordAssistantError(
                    "The suggested \(suggestion.taskId) parameters are not runnable: "
                        + errors.joined(separator: "; ")
                )
                return false
            }
            state.parameterSessions[sessionKey] = session
            return true
        } catch {
            recordAssistantError("Open suggested \(suggestion.taskId) task: \(error)")
            return false
        }
    }

    package func selectAIPrototypeAgent(_ agentID: String) {
        guard runtimeKind == .aiPrototype, var projection = state.prototypeAI else { return }
        projection.selectAgent(agentID)
        state.prototypeAI = projection
    }

    package func setAIPrototypeDraft(_ draft: String) {
        guard runtimeKind == .aiPrototype, var projection = state.prototypeAI else { return }
        projection.setDraft(draft)
        state.prototypeAI = projection
    }

    package func openAIPrototypeDrawer() {
        guard runtimeKind == .aiPrototype, var projection = state.prototypeAI else { return }
        projection.setPresentation(.drawer)
        state.prototypeAI = projection
        if state.activeTabID == "tab-ai-prototype" {
            state.activeTabID = state.tabs.first { $0.kind == .notebook }?.id
                ?? state.tabs.first?.id
                ?? ""
        }
        state.tabs.removeAll { $0.id == "tab-ai-prototype" }
    }

    package func closeAIPrototypeConversation() {
        guard runtimeKind == .aiPrototype, var projection = state.prototypeAI else { return }
        projection.setPresentation(.closed)
        state.prototypeAI = projection
        if state.activeTabID == "tab-ai-prototype" {
            state.activeTabID = state.tabs.first { $0.kind == .notebook }?.id
                ?? state.tabs.first?.id
                ?? ""
        }
        state.tabs.removeAll { $0.id == "tab-ai-prototype" }
    }

    package func expandAIPrototypeConversation() {
        guard runtimeKind == .aiPrototype, var projection = state.prototypeAI else { return }
        projection.setPresentation(.tab)
        state.prototypeAI = projection
        if !state.tabs.contains(where: { $0.id == "tab-ai-prototype" }) {
            state.tabs.append(WorkbenchTab(
                id: "tab-ai-prototype",
                title: "AI · TW Hya discussion",
                kind: .aiChat
            ))
        }
        state.activeTabID = "tab-ai-prototype"
    }

    package func dockAIPrototypeConversation() {
        openAIPrototypeDrawer()
    }

    package func openAIPrototypeTaskSuggestion() {
        guard runtimeKind == .aiPrototype else { return }
        let receiptID = "receipt-imager-cancelled"
        selectPrototypeNotebookReceipt(receiptID)
        if let tabIndex = state.tabs.firstIndex(where: { $0.kind == .task && $0.taskID == "imager" }) {
            state.tabs[tabIndex].prototypeReceiptID = receiptID
            state.activeTabID = state.tabs[tabIndex].id
        } else {
            openPrototypeNotebookTask(receiptID: receiptID)
        }
    }

    package func selectAIPrototypeModel(_ model: String) {
        guard runtimeKind == .aiPrototype, var projection = state.prototypeAI else { return }
        projection.selectModel(model)
        state.prototypeAI = projection
    }

    package func selectAIPrototypeReasoningEffort(_ effort: PrototypeAIReasoningEffort) {
        guard runtimeKind == .aiPrototype, var projection = state.prototypeAI else { return }
        projection.selectReasoningEffort(effort)
        state.prototypeAI = projection
    }

    package func selectAIPrototypeTrustPreset(_ preset: PrototypeAITrustPreset) {
        guard runtimeKind == .aiPrototype, var projection = state.prototypeAI else { return }
        projection.selectTrustPreset(preset)
        state.prototypeAI = projection
    }

    package func selectAIPrototypePythonEnvironment(_ environmentID: String) {
        guard runtimeKind == .aiPrototype, var projection = state.prototypeAI else { return }
        projection.selectPythonEnvironment(environmentID)
        state.prototypeAI = projection
    }

    package func startAIPrototypeIndexing() {
        guard runtimeKind == .aiPrototype, var projection = state.prototypeAI else { return }
        let generation = projection.beginIndexing()
        state.prototypeAI = projection
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.8) { [weak self] in
            guard let self, self.runtimeKind == .aiPrototype,
                  var projection = self.state.prototypeAI
            else { return }
            projection.completeIndexing(generation: generation)
            self.state.prototypeAI = projection
        }
    }

    package func cancelAIPrototypeIndexing() {
        guard runtimeKind == .aiPrototype, var projection = state.prototypeAI else { return }
        projection.cancelIndexing()
        state.prototypeAI = projection
    }

    package func sendAIPrototypePrompt(_ prompt: String) {
        guard runtimeKind == .aiPrototype, var projection = state.prototypeAI,
              let generation = projection.beginResponse(prompt: prompt)
        else { return }
        projection.setDraft("")
        state.prototypeAI = projection
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) { [weak self] in
            guard let self, self.runtimeKind == .aiPrototype,
                  var projection = self.state.prototypeAI
            else { return }
            projection.completeResponse(generation: generation)
            self.state.prototypeAI = projection
        }
    }

    package func cancelAIPrototypeResponse() {
        guard runtimeKind == .aiPrototype, var projection = state.prototypeAI else { return }
        projection.cancelResponse()
        state.prototypeAI = projection
    }

    package func retryAIPrototypeResponse() {
        guard runtimeKind == .aiPrototype, var projection = state.prototypeAI else { return }
        let prompt = projection.activePrompt ?? "Retry the previous question with the same approved context."
        projection.restartResponse()
        guard let generation = projection.beginResponse(prompt: prompt) else {
            state.prototypeAI = projection
            return
        }
        state.prototypeAI = projection
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) { [weak self] in
            guard let self, self.runtimeKind == .aiPrototype,
                  var projection = self.state.prototypeAI
            else { return }
            projection.completeResponse(generation: generation)
            self.state.prototypeAI = projection
        }
    }

    package func restartAIPrototypeWorker() {
        guard runtimeKind == .aiPrototype, var projection = state.prototypeAI else { return }
        projection.restartResponse()
        state.prototypeAI = projection
    }

    package func pinAIPrototypeMessage(_ messageID: String) {
        guard runtimeKind == .aiPrototype, var projection = state.prototypeAI,
              let message = projection.pinMessage(messageID)
        else { return }
        let citations = message.citations.map { "- [\($0.label)] \($0.locator)" }.joined(separator: "\n")
        let appendedMarkdown = """


        ## AI note

        \(message.text)

        Sources:
        \(citations)
        """
        updateActivePrototypeNotebook { document in
            document.draftMarkdown += appendedMarkdown
            document.savedMarkdown = document.draftMarkdown
        }
        state.prototypeAI = projection
        state.activeTabID = state.tabs.first { $0.kind == .notebook }?.id
            ?? state.tabs.first?.id
            ?? ""
    }

    package func selectTutorialPrototypeSection(_ sectionID: String) {
        guard runtimeKind == .tutorialPrototype,
              var tutorial = state.prototypeTutorial,
              tutorial.selectSection(id: sectionID)
        else { return }
        state.prototypeTutorial = tutorial
    }

    package func setTutorialPrototypeDraft(_ markdown: String) {
        guard runtimeKind == .tutorialPrototype,
              var tutorial = state.prototypeTutorial
        else { return }
        updateTutorialLearnerDocument(in: &tutorial) { document in
            document.draftMarkdown = markdown
            PrototypeScientificNotebookFixtureAdapter.synchronizeTaskCells(in: &document)
        }
        state.prototypeTutorial = tutorial
    }

    package func setTutorialPrototypeViewMode(_ viewMode: PrototypeNotebookViewMode) {
        guard runtimeKind == .tutorialPrototype,
              var tutorial = state.prototypeTutorial
        else { return }
        updateTutorialLearnerDocument(in: &tutorial) { $0.viewMode = viewMode }
        state.prototypeTutorial = tutorial
    }

    package func saveTutorialPrototypeDraft() {
        guard runtimeKind == .tutorialPrototype,
              var tutorial = state.prototypeTutorial
        else { return }
        updateTutorialLearnerDocument(in: &tutorial) { $0.savedMarkdown = $0.draftMarkdown }
        state.prototypeTutorial = tutorial
    }

    package func showTutorialPrototypeApproval() {
        guard runtimeKind == .tutorialPrototype,
              var tutorial = state.prototypeTutorial,
              tutorial.showApproval()
        else { return }
        state.prototypeTutorial = tutorial
    }

    package func dismissTutorialPrototypeApproval() {
        guard runtimeKind == .tutorialPrototype,
              var tutorial = state.prototypeTutorial,
              tutorial.dismissApproval()
        else { return }
        state.prototypeTutorial = tutorial
    }

    package func approveTutorialPrototypeAcquisition() {
        guard runtimeKind == .tutorialPrototype,
              var tutorial = state.prototypeTutorial,
              let generation = tutorial.approve()
        else { return }
        state.prototypeTutorial = tutorial
        scheduleTutorialPrototypeAdvance(generation: generation)
    }

    package func cancelTutorialPrototypeAcquisition() {
        guard runtimeKind == .tutorialPrototype,
              var tutorial = state.prototypeTutorial,
              tutorial.cancel()
        else { return }
        state.prototypeTutorial = tutorial
    }

    package func resumeTutorialPrototypeAcquisition() {
        guard runtimeKind == .tutorialPrototype,
              var tutorial = state.prototypeTutorial,
              let generation = tutorial.resume()
        else { return }
        state.prototypeTutorial = tutorial
        scheduleTutorialPrototypeAdvance(generation: generation)
    }

    package func restartTutorialPrototypeAcquisition() {
        guard runtimeKind == .tutorialPrototype,
              var tutorial = state.prototypeTutorial,
              let generation = tutorial.restart()
        else { return }
        state.prototypeTutorial = tutorial
        scheduleTutorialPrototypeAdvance(generation: generation)
    }

    package func retryTutorialPrototypeAcquisition() {
        guard runtimeKind == .tutorialPrototype,
              var tutorial = state.prototypeTutorial,
              let generation = tutorial.retry()
        else { return }
        state.prototypeTutorial = tutorial
        scheduleTutorialPrototypeAdvance(generation: generation)
    }

    package func makeSpaceAndRetryTutorialPrototypeAcquisition() {
        guard runtimeKind == .tutorialPrototype,
              var tutorial = state.prototypeTutorial,
              let generation = tutorial.makeSpaceAndRetry()
        else { return }
        state.prototypeTutorial = tutorial
        scheduleTutorialPrototypeAdvance(generation: generation)
    }

    /// Deterministic hook used by core tests and attempt-bound delayed fixture
    /// callbacks. Obsolete generations are ignored by the projection.
    @discardableResult
    package func advanceTutorialPrototypeAcquisition(generation: Int) -> Bool {
        guard runtimeKind == .tutorialPrototype,
              var tutorial = state.prototypeTutorial,
              tutorial.advance(generation: generation)
        else { return false }
        let shouldContinue = tutorial.dataset.phase.isRunning
        state.prototypeTutorial = tutorial
        if shouldContinue {
            scheduleTutorialPrototypeAdvance(generation: generation)
        }
        return true
    }

    package func openPrototypeTutorialTask(taskID: String) {
        guard runtimeKind == .tutorialPrototype,
              var tutorial = state.prototypeTutorial,
              tutorial.dataset.isReady,
              tutorial.fixtureTask.id == taskID
        else {
            state.lastErrors.append("Tutorial task parameters are unavailable until the fixture dataset is ready")
            return
        }
        if let documentIndex = tutorial.learnerNotebook.documents.firstIndex(where: {
            $0.id == tutorial.learnerNotebook.activeNotebookID
        }) {
            tutorial.learnerNotebook.documents[documentIndex].selectedReceiptID = taskID
        }
        state.prototypeTutorial = tutorial
        var tab = WorkbenchTab(
            id: "tab-prototype-task-\(taskID)",
            title: tutorial.fixtureTask.title,
            kind: .task,
            taskID: tutorial.fixtureTask.taskID
        )
        tab.prototypeReceiptID = taskID
        openTab(tab)
    }

    private func scheduleTutorialPrototypeAdvance(generation: Int) {
        // Leave enough time for a launched-app user or XCUITest to observe and
        // act on each deterministic state before the next fixture callback.
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.4) { [weak self] in
            _ = self?.advanceTutorialPrototypeAcquisition(generation: generation)
        }
    }

    private func updateTutorialLearnerDocument(
        in tutorial: inout TutorialNotebookPrototypeProjection,
        _ update: (inout PrototypeNotebookDocumentProjection) -> Void
    ) {
        let activeID = tutorial.learnerNotebook.activeNotebookID
        guard let index = tutorial.learnerNotebook.documents.firstIndex(where: { $0.id == activeID })
        else { return }
        update(&tutorial.learnerNotebook.documents[index])
    }

    public func openImagerProgressMockup() {
        guard !rejectPrototypeProductionAction("Imager progress mockups") else { return }
        if !state.hasProject {
            openFixtureProject()
        }
        if let dataset = state.project.datasets.first(where: { $0.kind == .measurementSet }) {
            selectDataset(dataset.id)
            openImagerTaskForSelectedDataset()
        } else {
            openDefaultTab(kind: .task)
        }
        selectTask("imager")
        let mockRunID = state.taskRun.runID ?? "imager-progress-mockup"
        let requestSummary = state.taskRun.requestSummary
        let progressSnapshot = ImagerProgressSnapshot.stub(request: ImagerProgressRequest(
            taskID: "imager",
            runID: mockRunID,
            taskState: .running,
            progress: 0,
            datasetName: state.selectedDataset?.name,
            requestSummary: requestSummary
        ))
        let workFraction = progressSnapshot.workEstimate.fraction
        state.taskRun = TaskRun(
            runID: mockRunID,
            state: .running,
            progress: workFraction,
            logLines: ["Imager progress mockup is using deterministic stub telemetry."],
            warnings: [],
            products: [],
            diagnostics: ["Mockup review state: estimated work is computed from scheduled units, not wall-clock lifecycle."],
            requestSummary: requestSummary,
            imagerProgress: progressSnapshot
        )
    }

    public func applyWorkbenchPlotEdit(plotID: String, action: WorkbenchPlotEditAction) {
        guard let index = state.plotDocuments.firstIndex(where: { $0.id == plotID }) else {
            state.lastErrors.append("Unknown workbench plot \(plotID)")
            return
        }
        state.plotDocuments[index].apply(action)
    }

    public func resetWorkbenchPlotSamples() {
        state.plotDocuments = WorkbenchPlotSamples.all()
    }

    public func openImagerTaskForSelectedDataset() {
        guard !rejectPrototypeProductionAction("Production task tabs") else { return }
        guard state.selectedDataset != nil else {
            state.lastErrors.append("Open a project with a dataset before opening an imaging task")
            return
        }
        state.activeTaskID = "imager"

        if let dataset = state.selectedDataset, dataset.kind == .measurementSet {
            let tabID = "tab-imager-\(dataset.id)"
            openTab(
                WorkbenchTab(
                    id: tabID,
                    title: "Imager: \(dataset.name)",
                    kind: .task,
                    datasetID: dataset.id,
                    taskID: "imager"
                )
            )
            loadTaskUISchemaIfNeeded("imager", instanceID: tabID)
            seedImagerTaskDefaults(for: dataset, instanceID: tabID, preserveExistingEdits: false)
            state.taskRun = TaskRun(
                state: .idle,
                progress: 0,
                logLines: ["Imager task initialized from selected MeasurementSet metadata."],
                warnings: [],
                products: [],
                requestSummary: genericTaskRequestSummary(taskID: "imager", instanceID: tabID),
                imagerProgress: nil
            )
        } else {
            let tabID = "tab-imager-unbound"
            openTab(
                WorkbenchTab(
                    id: tabID,
                    title: "Imager",
                    kind: .task,
                    taskID: "imager"
                )
            )
            loadTaskUISchemaIfNeeded("imager", instanceID: tabID)
            state.taskRun = TaskRun(
                state: .idle,
                progress: 0,
                logLines: ["Imager task opened. Select a MeasurementSet before running."],
                warnings: [],
                products: [],
                requestSummary: genericTaskRequestSummary(taskID: "imager", instanceID: tabID),
                imagerProgress: nil
            )
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

    @discardableResult
    public func refreshImageExplorer(datasetID: String) -> Bool {
        guard !rejectPrototypeProductionAction("Image explorers") else { return false }
        guard let dataset = state.project.datasets.first(where: { $0.id == datasetID }) else {
            state.lastErrors.append("Unknown dataset \(datasetID)")
            return false
        }
        guard dataset.kind == .imageCube else {
            state.lastErrors.append("Dataset \(dataset.name) is not an image")
            return false
        }
        let instanceID = state.project.datasets.first(where: { $0.id == datasetID })?.explorerTabID
        if state.imageExplorers[datasetID] == nil {
            applyParameterContext(
                surfaceID: "imexplore",
                instanceID: instanceID,
                textValues: ["image": dataset.path],
                preserveOverrides: true
            )
        }
        guard let explorerState = state.imageExplorers[datasetID]
            ?? profiledImageExplorerState(datasetID: datasetID, instanceID: instanceID)
        else { return false }
        do {
            var nextState = explorerState
            var snapshot = try imageExplorerClient.buildSnapshot(
                request: nextState.snapshotRequest(datasetPath: dataset.path)
            )
            let requestedProfileAxis = nextState.selectedProfileAxis
            nextState.selectedProfileAxis = try Self.resolveImageExplorerAxisSelector(
                nextState.selectedProfileAxisSelector,
                parameter: "profileaxis",
                snapshot: snapshot
            )
            nextState.movieAxis = try Self.resolveImageExplorerAxisSelector(
                nextState.movieAxisSelector,
                parameter: "movieaxis",
                snapshot: snapshot
            )
            if nextState.selectedProfileAxis != requestedProfileAxis {
                snapshot = try imageExplorerClient.buildSnapshot(
                    request: nextState.snapshotRequest(datasetPath: dataset.path)
                )
            }
            applyReadyImageExplorerSnapshot(snapshot, to: &nextState)
            state.imageExplorers[datasetID] = nextState
            acceptSessionParameters("imexplore", instanceID: instanceID)
            return true
        } catch {
            let originalError = error
            if explorerState.hasQueuedImageExplorerCommands {
                var recoveredState = explorerState
                recoveredState.regionCommands = []
                recoveredState.transientCommands = []
                do {
                    let snapshot = try imageExplorerClient.buildSnapshot(
                        request: recoveredState.snapshotRequest(datasetPath: dataset.path)
                    )
                    applyReadyImageExplorerSnapshot(snapshot, to: &recoveredState)
                    state.imageExplorers[datasetID] = recoveredState
                    acceptSessionParameters("imexplore", instanceID: instanceID)
                    state.lastErrors.append(
                        "Cleared invalid image explorer region command sequence for \(dataset.name): \(error)"
                    )
                    return false
                } catch let recoveryError {
                    state.lastErrors.append(
                        "Image explorer command recovery failed for \(dataset.name): \(recoveryError)"
                    )
                }
            }
            var failedState = explorerState
            failedState.status = .failed
            failedState.lastError = "\(originalError)"
            failedState.snapshot = nil
            state.imageExplorers[datasetID] = failedState
            state.lastErrors.append("Open image explorer for \(dataset.name): \(originalError)")
            return false
        }
    }

    private func applyReadyImageExplorerSnapshot(
        _ snapshot: ImageExplorerSnapshot,
        to explorerState: inout ImageExplorerSessionState
    ) {
        explorerState.status = .ready
        explorerState.lastError = nil
        explorerState.snapshot = snapshot
        explorerState.cursorX = snapshot.planeCursor.map { Int($0.pixelX) } ?? explorerState.cursorX
        explorerState.cursorY = snapshot.planeCursor.map { Int($0.pixelY) } ?? explorerState.cursorY
        explorerState.nonDisplayIndices = snapshot.nonDisplayAxes.map { Int($0.index) }
        explorerState.parameters = snapshot.parameters
        explorerState.transientCommands = []
    }

    public func refreshTableBrowser(datasetID: String) {
        guard !rejectPrototypeProductionAction("Table browsers") else { return }
        guard let dataset = state.project.datasets.first(where: { $0.id == datasetID }) else {
            state.lastErrors.append("Unknown dataset \(datasetID)")
            return
        }
        guard canBrowseAsTable(dataset) else {
            state.lastErrors.append("Dataset \(dataset.name) is not a casacore table")
            return
        }
        let instanceID = state.tabs.first(where: { $0.datasetID == datasetID && tab($0, hosts: "tablebrowser") })?.id
        if state.tableBrowsers[datasetID] == nil {
            applyParameterContext(
                surfaceID: "tablebrowser",
                instanceID: instanceID,
                textValues: ["table": dataset.path],
                preserveOverrides: true
            )
        }
        guard var browserState = state.tableBrowsers[datasetID]
            ?? profiledTableBrowserState(datasetID: datasetID, instanceID: instanceID)
        else { return }
        do {
            let snapshot: TableBrowserSnapshot
            if browserState.startupProfilePending {
                snapshot = try applyTableBrowserStartupProfile(
                    datasetPath: dataset.path,
                    browserState: &browserState
                )
                browserState.startupProfilePending = false
            } else {
                snapshot = try tableBrowserClient.buildSnapshot(request: browserState.snapshotRequest(datasetPath: dataset.path))
            }
            var nextState = TableBrowserSessionState(
                datasetID: datasetID,
                selectedView: browserState.selectedView,
                profileView: browserState.profileView,
                bookmark: browserState.bookmark,
                linkedTable: browserState.linkedTable,
                contentMode: browserState.contentMode,
                startupProfilePending: false,
                focus: snapshot.focus,
                commands: browserState.commands,
                cellWindowRowStart: browserState.cellWindowRowStart,
                cellWindowRowLimit: browserState.cellWindowRowLimit,
                cellWindowColumnStart: browserState.cellWindowColumnStart,
                cellWindowColumnLimit: browserState.cellWindowColumnLimit,
                hiddenCellColumns: browserState.hiddenCellColumns,
                cellColumnArrayInlineLimits: browserState.cellColumnArrayInlineLimits,
                status: .ready,
                lastError: nil,
                snapshot: snapshot,
                cellWindow: browserState.cellWindow
            )
            refreshTableBrowserCellWindowIfNeeded(dataset: dataset, browserState: &nextState)
            state.tableBrowsers[datasetID] = nextState
            acceptSessionParameters("tablebrowser", instanceID: instanceID)
        } catch {
            state.tableBrowsers[datasetID] = TableBrowserSessionState(
                datasetID: datasetID,
                selectedView: browserState.selectedView,
                profileView: browserState.profileView,
                bookmark: browserState.bookmark,
                linkedTable: browserState.linkedTable,
                contentMode: browserState.contentMode,
                startupProfilePending: browserState.startupProfilePending,
                focus: browserState.focus,
                commands: browserState.commands,
                cellWindowRowStart: browserState.cellWindowRowStart,
                cellWindowRowLimit: browserState.cellWindowRowLimit,
                cellWindowColumnStart: browserState.cellWindowColumnStart,
                cellWindowColumnLimit: browserState.cellWindowColumnLimit,
                hiddenCellColumns: browserState.hiddenCellColumns,
                cellColumnArrayInlineLimits: browserState.cellColumnArrayInlineLimits,
                status: .failed,
                lastError: "\(error)",
                snapshot: nil,
                cellWindow: browserState.cellWindow
            )
            state.lastErrors.append("Open table browser for \(dataset.name): \(error)")
        }
    }

    public func setImageExplorerView(_ view: String, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.selectedView = view
        setImageExplorerParameterValue(datasetID: datasetID, name: "view", value: .string(view))
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func setImageExplorerFocus(_ focus: String, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.focus = focus
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func setImageExplorerPlaneContentMode(_ mode: String, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.planeContentMode = mode
        setImageExplorerParameterValue(
            datasetID: datasetID,
            name: "contentmode",
            value: .string(mode)
        )
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func setimageExplorerParameters(_ parameters: ImageExplorerParameters, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.parameters = parameters
        for (name, value) in [
            "blc": parameters.blc,
            "trc": parameters.trc,
            "inc": parameters.inc,
            "stretch": parameters.stretch,
            "autoscale": parameters.autoscale,
            "clip_low": parameters.clipLow,
            "clip_high": parameters.clipHigh,
        ] {
            setImageExplorerParameterValue(datasetID: datasetID, name: name, value: .string(value))
        }
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func setImageExplorerColorMap(_ colorMap: ImageExplorerColorMap, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.planeColorMap = colorMap
        state.imageExplorers[datasetID] = explorerState
        setImageExplorerParameterValue(
            datasetID: datasetID,
            name: "colormap",
            value: .string(colorMap == .grayscale ? "gray" : colorMap.rawValue),
            persistImmediately: true
        )
    }

    public func cycleImageExplorerColorMap(datasetID: String) {
        let explorerState = imageExplorerState(datasetID: datasetID)
        setImageExplorerColorMap(explorerState.planeColorMap.next(), datasetID: datasetID)
    }

    public func setImageExplorerManualClip(low: Double, high: Double, datasetID: String) {
        guard low.isFinite, high.isFinite, low < high else {
            state.lastErrors.append("Invalid image clip range")
            return
        }
        var parameters = imageExplorerState(datasetID: datasetID).parameters
        parameters.stretch = "manual"
        parameters.clipLow = Self.formatImageExplorerClipValue(low)
        parameters.clipHigh = Self.formatImageExplorerClipValue(high)
        setimageExplorerParameters(parameters, datasetID: datasetID)
    }

    public func setImageExplorerCursor(x: Int?, y: Int?, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.cursorX = x
        explorerState.cursorY = y
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func stepImageExplorerNonDisplayAxis(axis: Int, delta: Int, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        var indices = explorerState.nonDisplayIndices
        let snapshotAxes = explorerState.snapshot?.nonDisplayAxes ?? []
        let axisPosition = snapshotAxes.firstIndex { Int($0.axis) == axis }
        let snapshotAxis = axisPosition.map { snapshotAxes[$0] }
        let currentIndex = snapshotAxis.map { Int($0.index) }
            ?? axisPosition.flatMap { indices[safe: $0] }
            ?? indices[safe: axis]
            ?? 0
        let length = max(snapshotAxis.map { Int($0.length) } ?? currentIndex + 1, 1)
        let nextIndex = min(max(currentIndex + delta, 0), length - 1)
        if let axisPosition {
            indices = normalizedNonDisplayIndices(from: indices, axes: snapshotAxes)
            indices[axisPosition] = nextIndex
        } else {
            while indices.count <= axis {
                indices.append(0)
            }
            indices[axis] = nextIndex
        }
        explorerState.nonDisplayIndices = indices
        explorerState.selectedProfileAxis = axis
        explorerState.selectedProfileAxisSelector = String(axis)
        state.imageExplorers[datasetID] = explorerState
        setImageExplorerParameterValue(datasetID: datasetID, name: "profileaxis", value: .string(String(axis)))
        refreshImageExplorer(datasetID: datasetID)
    }

    public func setImageExplorerNonDisplayAxisIndex(axis: Int, index: Int, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        var indices = explorerState.nonDisplayIndices
        let snapshotAxes = explorerState.snapshot?.nonDisplayAxes ?? []
        let axisPosition = snapshotAxes.firstIndex { Int($0.axis) == axis }
        let snapshotAxis = axisPosition.map { snapshotAxes[$0] }
        let length = max(snapshotAxis.map { Int($0.length) } ?? index + 1, 1)
        let nextIndex = min(max(index, 0), length - 1)
        if let axisPosition {
            indices = normalizedNonDisplayIndices(from: indices, axes: snapshotAxes)
            indices[axisPosition] = nextIndex
        } else {
            while indices.count <= axis {
                indices.append(0)
            }
            indices[axis] = nextIndex
        }
        explorerState.nonDisplayIndices = indices
        explorerState.selectedProfileAxis = axis
        explorerState.selectedProfileAxisSelector = String(axis)
        state.imageExplorers[datasetID] = explorerState
        setImageExplorerParameterValue(datasetID: datasetID, name: "profileaxis", value: .string(String(axis)))
        refreshImageExplorer(datasetID: datasetID)
    }

    public func startImageExplorerMovie(axis: Int, framesPerSecond: Double?, loop: Bool, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.moviePlaying = true
        explorerState.movieAxis = axis
        explorerState.movieAxisSelector = String(axis)
        if let framesPerSecond {
            explorerState.movieFramesPerSecond = Self.clampedMovieFramesPerSecond(framesPerSecond)
        }
        explorerState.movieLoop = loop
        explorerState.selectedProfileAxis = axis
        explorerState.selectedProfileAxisSelector = String(axis)
        state.imageExplorers[datasetID] = explorerState
        setImageExplorerParameterValue(datasetID: datasetID, name: "movieaxis", value: .string(String(axis)))
        setImageExplorerParameterValue(datasetID: datasetID, name: "profileaxis", value: .string(String(axis)))
        setImageExplorerParameterValue(datasetID: datasetID, name: "loop", value: .bool(loop))
        if let framesPerSecond {
            setImageExplorerParameterValue(
                datasetID: datasetID,
                name: "fps",
                value: .integer(Int64(Self.clampedMovieFramesPerSecond(framesPerSecond).rounded())),
                persistImmediately: true
            )
        } else {
            acceptSessionParameters(
                "imexplore",
                instanceID: parameterInstanceID(surfaceID: "imexplore", datasetID: datasetID)
            )
        }
    }

    public func stopImageExplorerMovie(datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.moviePlaying = false
        state.imageExplorers[datasetID] = explorerState
    }

    public func setImageExplorerMovieFramesPerSecond(_ framesPerSecond: Double, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.movieFramesPerSecond = Self.clampedMovieFramesPerSecond(framesPerSecond)
        state.imageExplorers[datasetID] = explorerState
        setImageExplorerParameterValue(
            datasetID: datasetID,
            name: "fps",
            value: .integer(Int64(explorerState.movieFramesPerSecond.rounded())),
            persistImmediately: true
        )
    }

    public func setImageExplorerMovieLoop(_ loop: Bool, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.movieLoop = loop
        state.imageExplorers[datasetID] = explorerState
        setImageExplorerParameterValue(
            datasetID: datasetID,
            name: "loop",
            value: .bool(loop),
            persistImmediately: true
        )
    }

    public func advanceImageExplorerMovieFrame(datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        guard explorerState.moviePlaying else {
            return
        }
        let axis = explorerState.movieAxis
            ?? explorerState.snapshot?.nonDisplayAxes.first.map { Int($0.axis) }
            ?? explorerState.nonDisplayIndices.indices.first
        guard let axis else {
            explorerState.moviePlaying = false
            state.imageExplorers[datasetID] = explorerState
            return
        }

        var indices = explorerState.nonDisplayIndices
        let snapshotAxes = explorerState.snapshot?.nonDisplayAxes ?? []
        let axisPosition = snapshotAxes.firstIndex { Int($0.axis) == axis }
        let snapshotAxis = axisPosition.map { snapshotAxes[$0] }
        let currentIndex = snapshotAxis.map { Int($0.index) }
            ?? axisPosition.flatMap { indices[safe: $0] }
            ?? indices[safe: axis]
            ?? 0
        let length = max(snapshotAxis.map { Int($0.length) } ?? currentIndex + 1, 1)
        let proposedIndex = currentIndex + 1
        let nextIndex: Int
        if proposedIndex >= length {
            if explorerState.movieLoop {
                nextIndex = 0
            } else {
                explorerState.moviePlaying = false
                state.imageExplorers[datasetID] = explorerState
                return
            }
        } else {
            nextIndex = proposedIndex
        }

        if let axisPosition {
            indices = normalizedNonDisplayIndices(from: indices, axes: snapshotAxes)
            indices[axisPosition] = nextIndex
        } else {
            while indices.count <= axis {
                indices.append(0)
            }
            indices[axis] = nextIndex
        }
        explorerState.nonDisplayIndices = indices
        explorerState.movieAxis = axis
        explorerState.selectedProfileAxis = axis
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func setImageExplorerSelectedProfileAxis(_ axis: Int, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.selectedProfileAxis = axis
        explorerState.selectedProfileAxisSelector = String(axis)
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func appendImageExplorerRegionCommand(_ command: ImageExplorerCommand, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.regionCommands.append(command)
        explorerState.activeRegionFilePath = nil
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func setImageExplorerRegionTool(_ tool: String, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.regionTool = tool
        state.imageExplorers[datasetID] = explorerState
    }

    public func reportImageExplorerRegionError(_ message: String) {
        state.lastErrors.append(message)
    }

    public func setImageExplorerBoxRegion(_ boxText: String, datasetID: String) {
        guard let box = Self.parseImageExplorerPixelBox(boxText) else {
            state.lastErrors.append("Region box must use non-negative x0,y0,x1,y1 pixel coordinates.")
            return
        }
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.regionCommands = Self.imageExplorerBoxRegionCommands(box)
        explorerState.activeRegionFilePath = nil
        explorerState.transientCommands = []
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func appendImageExplorerBoxRegion(_ boxText: String, datasetID: String) {
        guard let box = Self.parseImageExplorerPixelBox(boxText) else {
            state.lastErrors.append("Region box must use non-negative x0,y0,x1,y1 pixel coordinates.")
            return
        }
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.regionCommands.append(contentsOf: Self.imageExplorerBoxRegionCommands(box))
        explorerState.activeRegionFilePath = nil
        explorerState.transientCommands = []
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func setImageExplorerPolygonRegion(
        vertices: [(x: Int, y: Int)],
        closed: Bool = true,
        datasetID: String
    ) {
        guard vertices.count >= (closed ? 3 : 1) else {
            state.lastErrors.append(closed ? "A polygon region needs at least three vertices." : "A region needs at least one vertex.")
            return
        }
        var commands: [ImageExplorerCommand] = [.startRegionShape]
        commands.append(contentsOf: vertices.map { .appendRegionVertex(x: max($0.x, 0), y: max($0.y, 0)) })
        if closed {
            commands.append(.closeRegionShape)
        }
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.regionCommands = commands
        explorerState.activeRegionFilePath = nil
        explorerState.transientCommands = []
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func setImageExplorerRegionShapes(
        _ shapes: [[(x: Int, y: Int)]],
        datasetID: String
    ) {
        var commands: [ImageExplorerCommand] = []
        for vertices in shapes where vertices.count >= 3 {
            commands.append(.startRegionShape)
            commands.append(contentsOf: vertices.map { .appendRegionVertex(x: max($0.x, 0), y: max($0.y, 0)) })
            commands.append(.closeRegionShape)
        }
        guard !commands.isEmpty else {
            state.lastErrors.append("A region needs at least one closed shape.")
            return
        }
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.regionCommands = commands
        explorerState.transientCommands = []
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func deleteImageExplorerRegionShape(index: Int, datasetID: String) {
        let explorerState = imageExplorerState(datasetID: datasetID)
        guard let snapshot = explorerState.snapshot,
              let region = snapshot.region,
              region.overlayShapes.indices.contains(index)
        else {
            state.lastErrors.append("No region shape is available to delete.")
            return
        }
        let remainingShapes = region.overlayShapes.enumerated().compactMap { shapeIndex, shape -> [(x: Int, y: Int)]? in
            guard shapeIndex != index, shape.closed, shape.vertices.count >= 3 else {
                return nil
            }
            return shape.vertices.map { Self.sourcePixel(for: $0, displayAxes: snapshot.displayAxes) }
        }
        if remainingShapes.isEmpty {
            clearImageExplorerRegionCommands(datasetID: datasetID)
        } else {
            setImageExplorerRegionShapes(remainingShapes, datasetID: datasetID)
        }
    }

    public func deleteLastImageExplorerRegionShape(datasetID: String) {
        let explorerState = imageExplorerState(datasetID: datasetID)
        guard let overlayShapes = explorerState.snapshot?.region?.overlayShapes,
              let index = overlayShapes.indices.last
        else {
            state.lastErrors.append("No region shape is available to delete.")
            return
        }
        deleteImageExplorerRegionShape(index: index, datasetID: datasetID)
    }

    private static func sourcePixel(
        for vertex: ImageExplorerRegionOverlayVertex,
        displayAxes: [ImageExplorerDisplayAxis]
    ) -> (x: Int, y: Int) {
        guard let xAxis = displayAxes.first, let yAxis = displayAxes[safe: 1] else {
            return (Int(vertex.sampledX.rounded()), Int(vertex.sampledY.rounded()))
        }
        return (
            x: Int(xAxis.blc) + Int((vertex.sampledX * Double(max(xAxis.inc, 1))).rounded()),
            y: Int(yAxis.blc) + Int((vertex.sampledY * Double(max(yAxis.inc, 1))).rounded())
        )
    }

    private static func imageExplorerBoxRegionCommands(
        _ box: (x0: Int, y0: Int, x1: Int, y1: Int)
    ) -> [ImageExplorerCommand] {
        let x0 = min(box.x0, box.x1)
        let x1 = max(box.x0, box.x1)
        let y0 = min(box.y0, box.y1)
        let y1 = max(box.y0, box.y1)
        return [
            .startRegionShape,
            .appendRegionVertex(x: x0, y: y0),
            .appendRegionVertex(x: x1, y: y0),
            .appendRegionVertex(x: x1, y: y1),
            .appendRegionVertex(x: x0, y: y1),
            .closeRegionShape,
        ]
    }

    private static func parseImageExplorerPixelBox(_ boxText: String) -> (x0: Int, y0: Int, x1: Int, y1: Int)? {
        let trimmed = boxText.trimmingCharacters(in: .whitespacesAndNewlines)
        let value = trimmed.hasPrefix("box:") ? String(trimmed.dropFirst(4)) : trimmed
        let parts = value
            .split(separator: ",", omittingEmptySubsequences: false)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
        guard parts.count == 4,
              let x0 = Int(parts[0]), x0 >= 0,
              let y0 = Int(parts[1]), y0 >= 0,
              let x1 = Int(parts[2]), x1 >= 0,
              let y1 = Int(parts[3]), y1 >= 0
        else {
            return nil
        }
        return (x0, y0, x1, y1)
    }

    public func runImageExplorerCommandOnce(_ command: ImageExplorerCommand, datasetID: String) {
        guard let dataset = state.project.datasets.first(where: { $0.id == datasetID }) else {
            state.lastErrors.append("Unknown dataset \(datasetID)")
            return
        }
        let operation = notebookImageOperation(command: command, dataset: dataset)
        let handle = operation.flatMap {
            beginNotebookOperationRecording(
                operationID: $0.operationID,
                parameters: $0.parameters,
                classification: $0.classification,
                affectedPaths: $0.affectedPaths,
                bypassTabID: dataset.explorerTabID
            )
        }
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.transientCommands.append(command)
        state.imageExplorers[datasetID] = explorerState
        if command.command == "set_default_mask", let name = command.name {
            setImageExplorerParameterValue(datasetID: datasetID, name: "mask", value: .string(name))
        } else if command.command == "unset_default_mask" {
            setImageExplorerParameterValue(datasetID: datasetID, name: "mask", value: .string("none"))
        }
        let errorCount = state.lastErrors.count
        let succeeded = refreshImageExplorer(datasetID: datasetID)
        if let operation {
            let diagnostics = succeeded ? [] : Array(state.lastErrors.dropFirst(errorCount))
            finalizeNotebookOperationRecording(
                handle: handle,
                status: succeeded ? "succeeded" : "failed",
                affectedPaths: operation.affectedPaths,
                products: operation.productPaths,
                diagnostics: diagnostics
            )
        }
    }

    private func notebookImageOperation(
        command: ImageExplorerCommand,
        dataset: DatasetSummary
    ) -> (
        operationID: String,
        parameters: [String: JSONValue],
        classification: String,
        affectedPaths: [String],
        productPaths: [String]
    )? {
        let mutatingCommands: Set<String> = [
            "save_region_definition",
            "rename_region_definition",
            "delete_region_definition",
            "set_default_mask",
            "unset_default_mask",
            "delete_mask",
            "write_region_mask",
        ]
        let exportsRegion = command.command == "export_region_file"
        guard mutatingCommands.contains(command.command) || exportsRegion else { return nil }
        var parameters: [String: JSONValue] = [
            "dataset": .string(dataset.path),
            "command": .string(command.command),
        ]
        if let name = command.name { parameters["name"] = .string(name) }
        if let newName = command.newName { parameters["new_name"] = .string(newName) }
        if let setDefault = command.setDefault { parameters["set_default"] = .bool(setDefault) }
        if let path = command.path { parameters["path"] = .string(path) }
        let affectedPaths = exportsRegion
            ? command.path.map { [$0] } ?? []
            : [dataset.path]
        return (
            operationID: "imexplore.\(command.command)",
            parameters: parameters,
            classification: exportsRegion ? "product_write" : "input_mutation",
            affectedPaths: affectedPaths,
            productPaths: exportsRegion ? affectedPaths : []
        )
    }

    public func loadImageExplorerRegionFile(path: String, datasetID: String) {
        guard !rejectPrototypeProductionAction("Region file loading") else { return }
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.regionCommands = [.loadRegionFile(path: path)]
        explorerState.activeRegionFilePath = Self.normalizedRegionFilePath(path)
        explorerState.transientCommands = []
        state.imageExplorers[datasetID] = explorerState
        setImageExplorerParameterValue(datasetID: datasetID, name: "region", value: .string(path))
        refreshImageExplorer(datasetID: datasetID)
    }

    public func appendImageExplorerRegionFile(path: String, datasetID: String) {
        guard !rejectPrototypeProductionAction("Region file loading") else { return }
        var explorerState = imageExplorerState(datasetID: datasetID)
        let normalizedPath = Self.normalizedRegionFilePath(path)
        if explorerState.activeRegionFilePath == normalizedPath {
            explorerState.regionCommands = [.loadRegionFile(path: path)]
            explorerState.activeRegionFilePath = normalizedPath
        } else {
            let alreadyHasRegion = explorerState.snapshot?.region != nil || !explorerState.regionCommands.isEmpty
            explorerState.regionCommands.append(.appendRegionFile(path: path))
            explorerState.activeRegionFilePath = alreadyHasRegion ? nil : normalizedPath
        }
        explorerState.transientCommands = []
        state.imageExplorers[datasetID] = explorerState
        setImageExplorerParameterValue(
            datasetID: datasetID,
            name: "region",
            value: .string(explorerState.activeRegionFilePath ?? "none")
        )
        refreshImageExplorer(datasetID: datasetID)
    }

    private static func normalizedRegionFilePath(_ path: String) -> String {
        URL(fileURLWithPath: (path as NSString).expandingTildeInPath)
            .standardizedFileURL
            .path
    }

    public func exportImageExplorerRegionFile(datasetID: String, path: String? = nil) {
        guard !rejectPrototypeProductionAction("Region file export") else { return }
        guard let imageDataset = state.project.datasets.first(where: { $0.id == datasetID }) else {
            state.lastErrors.append("Unknown dataset \(datasetID)")
            return
        }
        guard imageDataset.kind == .imageCube else {
            state.lastErrors.append("Dataset \(imageDataset.name) is not an image")
            return
        }
        let exportPath = path ?? defaultRegionExportPath(for: imageDataset)
        runImageExplorerCommandOnce(.exportRegionFile(path: exportPath), datasetID: datasetID)
        guard FileManager.default.fileExists(atPath: exportPath) else {
            return
        }
        registerRegionDataset(path: exportPath, sourceImage: imageDataset)
        loadImageExplorerRegionFile(path: exportPath, datasetID: datasetID)
    }

    public func loadRegionFileIntoImageExplorer(regionDatasetID: String, imageDatasetID: String? = nil) {
        guard let regionDataset = state.project.datasets.first(where: { $0.id == regionDatasetID }) else {
            state.lastErrors.append("Unknown region dataset \(regionDatasetID)")
            return
        }
        guard regionDataset.kind == .region else {
            state.lastErrors.append("Dataset \(regionDataset.name) is not a region file")
            return
        }
        guard let imageDataset = imageDatasetID
            .flatMap({ id in state.project.datasets.first(where: { $0.id == id && $0.kind == .imageCube }) })
            ?? imageDatasetForRegion(regionDataset)
        else {
            state.lastErrors.append("No image dataset is available for region \(regionDataset.name)")
            return
        }

        openDatasetExplorer(imageDataset.id)
        appendImageExplorerRegionFile(path: regionDataset.path, datasetID: imageDataset.id)
    }

    public func clearImageExplorerRegionCommands(datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.regionCommands = []
        explorerState.activeRegionFilePath = nil
        explorerState.transientCommands = [.clearRegion]
        state.imageExplorers[datasetID] = explorerState
        setImageExplorerParameterValue(datasetID: datasetID, name: "region", value: .string("none"))
        refreshImageExplorer(datasetID: datasetID)
    }

    private func defaultRegionExportPath(for dataset: DatasetSummary) -> String {
        let baseURL = URL(fileURLWithPath: state.project.rootPath, isDirectory: true)
        let name = Self.sanitizedRegionFilenameComponent(dataset.name)
        return baseURL
            .appendingPathComponent("\(name)-region.crtf")
            .standardizedFileURL
            .path
    }

    private static func sanitizedRegionFilenameComponent(_ value: String) -> String {
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-_."))
        let name = value.unicodeScalars
            .map { scalar in allowed.contains(scalar) ? String(scalar) : "-" }
            .joined()
            .trimmingCharacters(in: CharacterSet(charactersIn: "-."))
        return name.isEmpty ? "image" : name
    }

    private func registerRegionDataset(path: String, sourceImage: DatasetSummary) {
        let standardizedPath = Self.standardizedDatasetPath(path)
        let region = DatasetSummary(
            id: standardizedPath,
            name: URL(fileURLWithPath: standardizedPath).lastPathComponent,
            path: standardizedPath,
            kind: .region,
            size: "region file",
            units: "pixels",
            sizeBytes: fileSize(path: standardizedPath),
            notes: "Exported region from \(sourceImage.name).",
            diagnostics: [
                "Region source image: \(projectRelativePath(sourceImage.path))",
                "Region parameter syntax: --region \(projectRelativePath(standardizedPath))",
                "Inline region syntax: box[[x0pix,y0pix],[x1pix,y1pix]] or world-coordinate CRTF"
            ]
        )
        if let index = state.project.datasets.firstIndex(where: { $0.id == standardizedPath }) {
            state.project.datasets[index] = region
        } else {
            state.project.datasets.append(region)
        }
    }

    private func imageDatasetForRegion(_ regionDataset: DatasetSummary) -> DatasetSummary? {
        if let source = regionDataset.diagnostics
            .first(where: { $0.hasPrefix("Region source image:") })?
            .dropFirst("Region source image:".count)
            .trimmingCharacters(in: .whitespacesAndNewlines),
           !source.isEmpty {
            let sourcePath = resolveProjectPath(source)
            if let match = state.project.datasets.first(where: { dataset in
                dataset.kind == .imageCube
                    && (Self.standardizedDatasetPath(dataset.path) == Self.standardizedDatasetPath(sourcePath)
                        || dataset.name == source
                        || dataset.path == source)
            }) {
                return match
            }
        }
        return state.selectedDataset.flatMap { selected in
            selected.kind == .imageCube ? selected : nil
        } ?? state.project.datasets.first { $0.kind == .imageCube }
    }

    private func resolveProjectPath(_ path: String) -> String {
        let expanded = (path as NSString).expandingTildeInPath
        if expanded.hasPrefix("/") {
            return expanded
        }
        guard !state.project.rootPath.isEmpty else {
            return expanded
        }
        return URL(fileURLWithPath: state.project.rootPath, isDirectory: true)
            .appendingPathComponent(expanded)
            .standardizedFileURL
            .path
    }

    public func setTableBrowserView(_ view: String, datasetID: String) {
        let instanceID = state.tabs.first(where: { $0.datasetID == datasetID && tab($0, hosts: "tablebrowser") })?.id
        var browserState = state.tableBrowsers[datasetID] ?? TableBrowserSessionState(
            datasetID: datasetID,
            selectedView: Self.canonicalTableBrowserView(nil),
            status: .idle,
            lastError: nil,
            snapshot: nil
        )
        browserState.selectedView = Self.canonicalTableBrowserView(view)
        browserState.profileView = Self.profileTableBrowserView(view) ?? browserState.profileView
        browserState.startupProfilePending = false
        browserState.focus = "main"
        browserState.commands = []
        browserState.transientCommands = []
        state.tableBrowsers[datasetID] = browserState
        if let profileView = Self.profileTableBrowserView(view) {
            setSessionParameterValue(
                surfaceID: "tablebrowser",
                instanceID: instanceID,
                name: "view",
                value: .string(profileView)
            )
        }
        refreshTableBrowser(datasetID: datasetID)
    }

    public func setTableBrowserContentMode(_ contentMode: String, datasetID: String) {
        guard ["auto", "compact", "detailed"].contains(contentMode) else { return }
        let instanceID = state.tabs.first(where: { $0.datasetID == datasetID && tab($0, hosts: "tablebrowser") })?.id
        var browserState = tableBrowserState(datasetID: datasetID)
        browserState.contentMode = contentMode
        state.tableBrowsers[datasetID] = browserState
        setSessionParameterValue(
            surfaceID: "tablebrowser",
            instanceID: instanceID,
            name: "contentmode",
            value: .string(contentMode),
            persistImmediately: true
        )
    }

    public func runTableBrowserCommand(_ command: TableBrowserCommand, datasetID: String) {
        var browserState = state.tableBrowsers[datasetID] ?? TableBrowserSessionState(
            datasetID: datasetID,
            selectedView: Self.canonicalTableBrowserView(nil),
            status: .idle,
            lastError: nil,
            snapshot: nil
        )
        browserState.commands.append(command)
        state.tableBrowsers[datasetID] = browserState
        refreshTableBrowser(datasetID: datasetID)
    }

    public func selectTableBrowserMainItem(index: Int, datasetID: String) {
        guard index >= 0 else {
            return
        }
        var browserState = tableBrowserState(datasetID: datasetID)
        guard let selectedIndex = browserState.snapshot?.verticalMetrics?.selectedIndex else {
            return
        }
        guard appendTableBrowserMove(from: Int(selectedIndex), to: index, into: &browserState) else {
            return
        }
        state.tableBrowsers[datasetID] = browserState
        refreshTableBrowser(datasetID: datasetID)
    }

    public func selectTableBrowserVisibleCell(
        rowIndex: Int?,
        selectedVisibleColumn: Int?,
        targetVisibleColumn: Int?,
        datasetID: String
    ) {
        var browserState = tableBrowserState(datasetID: datasetID)
        var changed = false
        if let rowIndex, rowIndex >= 0 {
            changed = browserState.selectedCellRow != rowIndex || changed
            browserState.selectedCellRow = rowIndex
        }
        if let targetVisibleColumn {
            changed = browserState.selectedCellColumn != targetVisibleColumn || changed
            browserState.selectedCellColumn = targetVisibleColumn
        }
        guard changed else {
            return
        }
        state.tableBrowsers[datasetID] = browserState
    }

    public func requestTableBrowserCellWindow(
        rowStart: Int,
        rowLimit: Int,
        columnStart: Int,
        columnLimit: Int,
        datasetID: String
    ) {
        guard !rejectPrototypeProductionAction("Table cell windows") else { return }
        guard let dataset = state.project.datasets.first(where: { $0.id == datasetID }) else {
            state.lastErrors.append("Unknown dataset \(datasetID)")
            return
        }
        guard canBrowseAsTable(dataset) else {
            state.lastErrors.append("Dataset \(dataset.name) is not a casacore table")
            return
        }
        var browserState = tableBrowserState(datasetID: datasetID)
        browserState.cellWindowRowStart = max(0, rowStart)
        browserState.cellWindowRowLimit = max(1, min(rowLimit, 4096))
        browserState.cellWindowColumnStart = max(0, columnStart)
        browserState.cellWindowColumnLimit = max(1, min(columnLimit, 128))
        state.tableBrowsers[datasetID] = browserState
        scheduleTableBrowserCellWindowLoad(
            datasetID: datasetID,
            dataset: dataset,
            request: browserState.cellWindowRequest(datasetPath: dataset.path)
        )
    }

    public func setTableBrowserColumnHidden(columnIndex: Int, hidden: Bool, datasetID: String) {
        var browserState = tableBrowserState(datasetID: datasetID)
        if hidden {
            browserState.hiddenCellColumns.insert(columnIndex)
        } else {
            browserState.hiddenCellColumns.remove(columnIndex)
        }
        state.tableBrowsers[datasetID] = browserState
    }

    public func setTableBrowserArrayInlineLimit(columnIndex: Int, limit: Int, datasetID: String) {
        guard let dataset = state.project.datasets.first(where: { $0.id == datasetID }) else {
            state.lastErrors.append("Unknown dataset \(datasetID)")
            return
        }
        var browserState = tableBrowserState(datasetID: datasetID)
        if limit > 0 {
            browserState.cellColumnArrayInlineLimits[columnIndex] = limit
        } else {
            browserState.cellColumnArrayInlineLimits.removeValue(forKey: columnIndex)
        }
        state.tableBrowsers[datasetID] = browserState
        scheduleTableBrowserCellWindowLoad(
            datasetID: datasetID,
            dataset: dataset,
            request: browserState.cellWindowRequest(datasetPath: dataset.path)
        )
    }

    public func loadTableBrowserCellValue(
        rowIndex: Int,
        columnIndex: Int,
        datasetID: String,
        completion: @escaping (Result<String, Error>) -> Void
    ) {
        guard !rejectPrototypeProductionAction("Table cell values") else {
            completion(.failure(NotebookPrototypeBoundaryViolation(boundary: "table cell value")))
            return
        }
        guard let dataset = state.project.datasets.first(where: { $0.id == datasetID }) else {
            let error = NSError(
                domain: "CasarsMacCore.WorkbenchStore",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "Unknown dataset \(datasetID)"]
            )
            state.lastErrors.append(error.localizedDescription)
            completion(.failure(error))
            return
        }
        let request = tableBrowserCellValueRequest(
            datasetPath: dataset.path,
            rowIndex: rowIndex,
            columnIndex: columnIndex
        )
        tableBrowserQueue.async { [tableBrowserClient] in
            let result = Result {
                try tableBrowserClient.buildCellValue(request: request)
            }
            DispatchQueue.main.async {
                if case let .failure(error) = result {
                    self.state.lastErrors.append("Copy table cell for \(dataset.name): \(error)")
                }
                completion(result)
            }
        }
    }

    public func openSelectedTableBrowserSubtable(datasetID: String) {
        guard let snapshot = state.tableBrowsers[datasetID]?.snapshot,
              snapshot.selectedAddress?.kind == "subtable",
              let targetPath = snapshot.selectedAddress?.targetPath
        else {
            state.lastErrors.append("No subtable is selected")
            return
        }
        openTableBrowserPath(targetPath, sourceDatasetID: datasetID)
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

    public func selectTask(_ taskID: String, tabID: String? = nil) {
        guard !rejectPrototypeProductionAction("Task selection") else { return }
        guard state.applicationCatalog.contains(where: { $0.id == taskID }) else {
            state.lastErrors.append("Unknown task \(taskID)")
            return
        }
        state.activeTaskID = taskID
        let resolvedTabID = tabID ?? state.activeTabID
        if let index = state.tabs.firstIndex(where: { $0.id == resolvedTabID && $0.kind == .task }) {
            state.tabs[index].taskID = taskID
            state.tabs[index].title = taskTitle(taskID)
        }
        loadTaskUISchemaIfNeeded(taskID, instanceID: resolvedTabID)
        if taskID == "imager", let dataset = state.selectedDataset, dataset.kind == .measurementSet {
            seedImagerTaskDefaults(for: dataset, instanceID: resolvedTabID, preserveExistingEdits: true)
        }
        state.taskRun.imagerProgress = imagerProgressSnapshot(
            taskID: taskID,
            runID: state.taskRun.runID,
            taskState: state.taskRun.state,
            progress: state.taskRun.progress
        )
    }

    public func taskID(forTab tabID: String) -> String {
        guard let tab = state.tabs.first(where: { $0.id == tabID }) else {
            return state.activeTaskID
        }
        guard tab.kind == .task else {
            return state.activeTaskID
        }
        return tab.taskID ?? ""
    }

    public func loadTaskUISchemaIfNeeded(_ taskID: String? = nil, instanceID: String? = nil) {
        guard !rejectPrototypeProductionAction("Task schemas") else { return }
        let resolvedTaskID = taskID ?? state.activeTaskID
        guard !resolvedTaskID.isEmpty else {
            return
        }
        if state.taskUISchemas[resolvedTaskID] == nil {
            do {
                state.taskUISchemas[resolvedTaskID] = try taskUISchemaClient.loadTaskUISchema(taskID: resolvedTaskID)
            } catch {
                state.lastErrors.append("Load task schema for \(resolvedTaskID): \(error)")
                return
            }
        }
        loadParameterSessionIfNeeded(resolvedTaskID, instanceID: instanceID)
        applySelectedDatasetParameterContext(surfaceID: resolvedTaskID, instanceID: instanceID)
        if let schema = state.taskUISchemas[resolvedTaskID] {
            state.taskRun = TaskRun(
                state: .idle,
                progress: 0,
                logLines: ["Loaded \(schema.displayName) parameter contract."],
                warnings: [],
                products: [],
                requestSummary: genericTaskRequestSummary(taskID: resolvedTaskID, instanceID: instanceID),
                imagerProgress: imagerProgressSnapshot(taskID: resolvedTaskID, taskState: .idle, progress: 0)
            )
        }
    }

    public func setGenericTaskValue(
        taskID: String? = nil,
        instanceID: String? = nil,
        argumentID: String,
        value: String
    ) {
        guard !rejectPrototypeProductionAction("Task parameters") else { return }
        let resolvedTaskID = taskID ?? state.activeTaskID
        loadParameterSessionIfNeeded(resolvedTaskID, instanceID: instanceID)
        let sessionKey = parameterSessionKey(surfaceID: resolvedTaskID, instanceID: instanceID)
        guard var session = state.parameterSessions[sessionKey],
              let concept = session.bundle.concept(for: argumentID)
        else {
            state.lastErrors.append("Unknown parameter \(argumentID) for \(resolvedTaskID)")
            return
        }
        let normalized = concept.valueDomain.isPathLike && !Self.isInlineRegionSyntax(value)
            ? projectRelativePath(value)
            : value
        session.overridePatch.removeUnset(argumentID)
        session.overridePatch.values[argumentID] = concept.valueDomain.value(from: normalized)
        session.draftText[argumentID] = normalized
        resolveParameterSession(&session, editedParameters: [argumentID])
        state.parameterSessions[sessionKey] = session
        clearAssistantSuggestedParameters(sessionKey: sessionKey, names: [argumentID])
        state.taskRun.requestSummary = genericTaskRequestSummary(taskID: resolvedTaskID, instanceID: instanceID)
    }

    public func setGenericTaskToggle(
        taskID: String? = nil,
        instanceID: String? = nil,
        argumentID: String,
        value: Bool
    ) {
        guard !rejectPrototypeProductionAction("Task parameters") else { return }
        let resolvedTaskID = taskID ?? state.activeTaskID
        loadParameterSessionIfNeeded(resolvedTaskID, instanceID: instanceID)
        let sessionKey = parameterSessionKey(surfaceID: resolvedTaskID, instanceID: instanceID)
        guard var session = state.parameterSessions[sessionKey] else {
            state.lastErrors.append("Parameter session for \(resolvedTaskID) is unavailable")
            return
        }
        session.overridePatch.removeUnset(argumentID)
        session.overridePatch.values[argumentID] = .bool(value)
        session.draftText.removeValue(forKey: argumentID)
        resolveParameterSession(&session, editedParameters: [argumentID])
        state.parameterSessions[sessionKey] = session
        clearAssistantSuggestedParameters(sessionKey: sessionKey, names: [argumentID])
        state.taskRun.requestSummary = genericTaskRequestSummary(taskID: resolvedTaskID, instanceID: instanceID)
    }

    public func parameterText(surfaceID: String? = nil, instanceID: String? = nil, name: String) -> String {
        parameterSession(surfaceID: surfaceID ?? state.activeTaskID, instanceID: instanceID)?.text(for: name) ?? ""
    }

    public func parameterToggle(surfaceID: String? = nil, instanceID: String? = nil, name: String) -> Bool {
        parameterSession(surfaceID: surfaceID ?? state.activeTaskID, instanceID: instanceID)?.toggle(for: name) ?? false
    }

    public func parameterOrigin(surfaceID: String? = nil, instanceID: String? = nil, name: String) -> String {
        parameterSession(surfaceID: surfaceID ?? state.activeTaskID, instanceID: instanceID)?.origin(for: name) ?? "default"
    }

    public func resetParameter(surfaceID: String? = nil, instanceID: String? = nil, name: String) {
        guard !rejectPrototypeProductionAction("Parameter reset") else { return }
        let resolvedSurfaceID = surfaceID ?? state.activeTaskID
        let sessionKey = parameterSessionKey(surfaceID: resolvedSurfaceID, instanceID: instanceID)
        guard var session = state.parameterSessions[sessionKey] else { return }
        session.overridePatch.values.removeValue(forKey: name)
        session.overridePatch.insertUnset(name)
        session.draftText.removeValue(forKey: name)
        resolveParameterSession(&session, editedParameters: [name])
        state.parameterSessions[sessionKey] = session
        clearAssistantSuggestedParameters(sessionKey: sessionKey, names: [name])
        state.taskRun.requestSummary = genericTaskRequestSummary(taskID: resolvedSurfaceID, instanceID: instanceID)
    }

    public func revertParameters(surfaceID: String? = nil, instanceID: String? = nil) {
        guard !rejectPrototypeProductionAction("Parameter revert") else { return }
        let resolvedSurfaceID = surfaceID ?? state.activeTaskID
        let sessionKey = parameterSessionKey(surfaceID: resolvedSurfaceID, instanceID: instanceID)
        guard var session = state.parameterSessions[sessionKey] else { return }
        session.overridePatch = SurfaceParameterPatch()
        session.draftText = [:]
        resolveParameterSession(&session)
        state.parameterSessions[sessionKey] = session
        assistantController.clearSuggestedParameters(sessionKey: sessionKey)
        state.taskRun.requestSummary = genericTaskRequestSummary(taskID: resolvedSurfaceID, instanceID: instanceID)
    }

    public func setParameterSaveLast(surfaceID: String? = nil, instanceID: String? = nil, enabled: Bool) {
        let resolvedSurfaceID = surfaceID ?? state.activeTaskID
        let sessionKey = parameterSessionKey(surfaceID: resolvedSurfaceID, instanceID: instanceID)
        guard var session = state.parameterSessions[sessionKey] else { return }
        session.saveLast = enabled
        state.parameterSessions[sessionKey] = session
    }

    public func setParameterWorkspace(surfaceID: String? = nil, instanceID: String? = nil, path: String) {
        let resolvedSurfaceID = surfaceID ?? state.activeTaskID
        let sessionKey = parameterSessionKey(surfaceID: resolvedSurfaceID, instanceID: instanceID)
        guard var session = state.parameterSessions[sessionKey] else { return }
        session.workspace = path
        state.parameterSessions[sessionKey] = session
    }

    public func applySurfaceParameterProfile(surfaceID: String, datasetID: String, instanceID: String? = nil) {
        guard !rejectPrototypeProductionAction("Parameter profiles") else { return }
        guard parameterSession(surfaceID: surfaceID, instanceID: instanceID) != nil else { return }
        switch surfaceID {
        case "msexplore":
            state.measurementSetPlots[datasetID] = profiledMeasurementSetPlotState(
                datasetID: datasetID,
                instanceID: instanceID
            )
        case "imexplore":
            guard let profiled = profiledImageExplorerState(datasetID: datasetID, instanceID: instanceID) else { return }
            state.imageExplorers[datasetID] = profiled
            refreshImageExplorer(datasetID: datasetID)
        case "tablebrowser":
            guard let profiled = profiledTableBrowserState(datasetID: datasetID, instanceID: instanceID) else { return }
            state.tableBrowsers[datasetID] = profiled
            refreshTableBrowser(datasetID: datasetID)
        default:
            state.lastErrors.append("Unsupported session parameter surface \(surfaceID)")
        }
    }

    public func selectParameterSource(
        _ source: SurfaceParameterBaseSource,
        surfaceID: String? = nil,
        instanceID: String? = nil,
        profilePath: String? = nil,
        discardEdits: Bool = false
    ) {
        guard !rejectPrototypeProductionAction("Parameter profile loading") else { return }
        let resolvedSurfaceID = surfaceID ?? state.activeTaskID
        let sessionKey = parameterSessionKey(surfaceID: resolvedSurfaceID, instanceID: instanceID)
        if state.parameterSessions[sessionKey]?.snapshot.dirty == true, !discardEdits {
            state.lastErrors.append(
                "Replace edited \(resolvedSurfaceID) parameters only after confirming those edits may be discarded."
            )
            return
        }
        do {
            let bundle = try surfaceParameterClient.loadBundle(surfaceID: resolvedSurfaceID)
            let workspace = state.parameterSessions[sessionKey]?.workspace ?? parameterWorkspacePath()
            let snapshot: SurfaceParameterSnapshot
            let baseTOML: String?
            let basePath: String?
            switch source {
            case .defaults:
                snapshot = try surfaceParameterClient.defaults(surfaceID: resolvedSurfaceID)
                baseTOML = nil
                basePath = nil
            case .last, .lastSuccessful:
                let successful = source == .lastSuccessful
                guard let loaded = try surfaceParameterClient.last(
                    surfaceID: resolvedSurfaceID,
                    workspace: workspace,
                    successful: successful
                ) else {
                    state.lastErrors.append("No \(source.title) profile exists for \(resolvedSurfaceID).")
                    return
                }
                snapshot = loaded
                baseTOML = loaded.profileToml
                basePath = nil
            case .file:
                guard let profilePath else {
                    state.lastErrors.append("A parameter profile path is required.")
                    return
                }
                let profile = try String(contentsOfFile: profilePath, encoding: .utf8)
                snapshot = try surfaceParameterClient.load(
                    surfaceID: resolvedSurfaceID,
                    profileTOML: profile,
                    sourcePath: profilePath
                )
                baseTOML = profile
                basePath = profilePath
            }
            let saveLast = state.parameterSessions[sessionKey]?.saveLast ?? true
            state.parameterSessions[sessionKey] = SurfaceParameterSession(
                bundle: bundle,
                snapshot: snapshot,
                selectedSource: source,
                baseProfileTOML: baseTOML,
                baseProfilePath: basePath,
                workspace: workspace,
                saveLast: saveLast
            )
            applySelectedDatasetParameterContext(surfaceID: resolvedSurfaceID, instanceID: instanceID)
            state.taskRun.requestSummary = genericTaskRequestSummary(taskID: resolvedSurfaceID, instanceID: instanceID)
        } catch {
            state.lastErrors.append("Load \(source.title) parameters for \(resolvedSurfaceID): \(error)")
        }
    }

    public func saveParameterProfile(surfaceID: String? = nil, instanceID: String? = nil, to path: String) {
        guard !rejectPrototypeProductionAction("Parameter profile saving") else { return }
        let resolvedSurfaceID = surfaceID ?? state.activeTaskID
        guard let session = parameterSession(surfaceID: resolvedSurfaceID, instanceID: instanceID) else {
            state.lastErrors.append("Parameter session for \(resolvedSurfaceID) is unavailable")
            return
        }
        do {
            let result = try surfaceParameterClient.save(
                surfaceID: resolvedSurfaceID,
                values: session.values,
                destinationPath: path
            )
            state.taskRun.logLines.append("Saved parameter profile: \(result.path)")
        } catch {
            state.lastErrors.append("Save parameters for \(resolvedSurfaceID): \(error)")
        }
    }

    public func setGenericTaskConfirmation(
        taskID: String? = nil,
        instanceID: String? = nil,
        confirmed: Bool
    ) {
        let surfaceID = taskID ?? state.activeTaskID
        state.genericTaskConfirmations[
            parameterSessionKey(surfaceID: surfaceID, instanceID: instanceID)
        ] = confirmed
    }

    public func setNotebookRecordingBypassOnce(tabID: String, enabled: Bool) {
        if enabled {
            state.notebookRecordingBypassTabs.insert(tabID)
        } else {
            state.notebookRecordingBypassTabs.remove(tabID)
        }
    }

    public func notebookRecordingBypassOnce(tabID: String) -> Bool {
        state.notebookRecordingBypassTabs.contains(tabID)
    }

    public func taskRunSafety(
        taskID: String? = nil,
        instanceID: String? = nil
    ) -> SurfaceRunSafety? {
        guard !rejectPrototypeProductionAction("Task run safety") else { return nil }
        let surfaceID = taskID ?? state.activeTaskID
        loadParameterSessionIfNeeded(surfaceID, instanceID: instanceID)
        guard let session = parameterSession(surfaceID: surfaceID, instanceID: instanceID),
              !session.hasErrors
        else { return nil }
        return try? surfaceParameterClient.runSafety(surfaceID: surfaceID, values: session.values)
    }

    public func taskRequiresConfirmation(taskID: String? = nil, instanceID: String? = nil) -> Bool {
        taskRunSafety(taskID: taskID, instanceID: instanceID)?.requiresInteractiveConfirmation ?? false
    }

    public func taskHasConfirmation(taskID: String? = nil, instanceID: String? = nil) -> Bool {
        let surfaceID = taskID ?? state.activeTaskID
        return state.genericTaskConfirmations[
            parameterSessionKey(surfaceID: surfaceID, instanceID: instanceID)
        ] ?? false
    }

    public func runTask() {
        guard !rejectPrototypeProductionAction("Task execution") else { return }
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

        runGenericTask()
    }

    private func runGenericTask() {
        let taskID = state.activeTaskID
        let instanceID = parameterInstanceID(surfaceID: taskID)
        guard let task = state.applicationCatalog.first(where: { $0.id == taskID }) else {
            state.lastErrors.append("Unknown task \(taskID)")
            return
        }
        if parameterSession(surfaceID: taskID, instanceID: instanceID) == nil {
            loadParameterSessionIfNeeded(taskID, instanceID: instanceID)
            applySelectedDatasetParameterContext(surfaceID: taskID, instanceID: instanceID)
        }
        guard let parameterSession = parameterSession(surfaceID: taskID, instanceID: instanceID) else {
            state.lastErrors.append("Parameter session for \(taskID) is unavailable")
            return
        }
        let runID = nextJobID(prefix: taskID)
        let tabID = state.activeTabID.isEmpty ? "tab-task-\(taskID)" : state.activeTabID
        if parameterSession.hasErrors {
            let messages = parameterSession.snapshot.diagnostics
                .filter { $0.level == "error" }
                .map(\.message)
            beginNotebookTaskRecording(
                runID: runID,
                tabID: tabID,
                taskID: taskID,
                session: parameterSession,
                runSafety: SurfaceRunSafety(
                    classes: [],
                    requiresInteractiveConfirmation: false,
                    requiresOverwriteConfirmation: false,
                    requiresInputMutationConfirmation: false
                )
            )
            finalizeNotebookTaskRecording(runID: runID, status: "failed", diagnostics: messages)
            state.taskRun = TaskRun(
                state: .failed,
                progress: 1.0,
                logLines: [],
                warnings: [],
                products: [],
                diagnostics: messages,
                requestSummary: genericTaskRequestSummary(taskID: taskID, instanceID: instanceID),
                imagerProgress: imagerProgressSnapshot(taskID: taskID, taskState: .failed, progress: 1.0)
            )
            state.lastErrors.append("Resolve parameter errors before running \(task.displayName).")
            return
        }
        let runSafety: SurfaceRunSafety
        do {
            runSafety = try surfaceParameterClient.runSafety(
                surfaceID: taskID,
                values: parameterSession.values
            )
        } catch {
            beginNotebookTaskRecording(
                runID: runID,
                tabID: tabID,
                taskID: taskID,
                session: parameterSession,
                runSafety: SurfaceRunSafety(
                    classes: [],
                    requiresInteractiveConfirmation: false,
                    requiresOverwriteConfirmation: false,
                    requiresInputMutationConfirmation: false
                )
            )
            finalizeNotebookTaskRecording(
                runID: runID,
                status: "failed",
                diagnostics: ["Evaluate run safety: \(error)"]
            )
            state.lastErrors.append("Evaluate run safety for \(taskID): \(error)")
            return
        }
        if runSafety.requiresInteractiveConfirmation
            && !taskHasConfirmation(taskID: taskID, instanceID: instanceID) {
            state.taskRun = TaskRun(
                state: .failed,
                progress: 1.0,
                logLines: [],
                warnings: [],
                products: [],
                diagnostics: ["Confirm catalog-declared run risks before running this task."],
                requestSummary: genericTaskRequestSummary(taskID: taskID, instanceID: instanceID),
                imagerProgress: imagerProgressSnapshot(taskID: taskID, taskState: .failed, progress: 1.0)
            )
            state.lastErrors.append("Confirm \(task.displayName) before running.")
            return
        }
        let providerInvocation: SurfaceProviderInvocation
        do {
            providerInvocation = try surfaceParameterClient.providerInvocation(
                surfaceID: taskID,
                values: parameterSession.values
            )
        } catch {
            beginNotebookTaskRecording(
                runID: runID,
                tabID: tabID,
                taskID: taskID,
                session: parameterSession,
                runSafety: runSafety
            )
            finalizeNotebookTaskRecording(
                runID: runID,
                status: "failed",
                diagnostics: ["Project provider invocation: \(error)"]
            )
            state.lastErrors.append("Project provider invocation for \(taskID): \(error)")
            return
        }
        let summary = genericTaskRequestSummary(taskID: taskID, instanceID: instanceID)
        beginNotebookTaskRecording(
            runID: runID,
            tabID: tabID,
            taskID: taskID,
            session: parameterSession,
            runSafety: runSafety
        )
        startJob(WorkbenchJob(
            id: runID,
            tabID: tabID,
            kind: .genericTask,
            owner: .user,
            status: .running,
            progress: 0.05,
            title: task.displayName,
            detail: summary,
            logLines: ["Starting \(task.executable).", summary],
            lastEvent: "started"
        ))
        state.taskRun = TaskRun(
            runID: runID,
            state: .running,
            progress: 0.05,
            logLines: ["Starting \(task.executable).", summary],
            warnings: [],
            products: [],
            diagnostics: [],
            requestSummary: summary,
            imagerProgress: imagerProgressSnapshot(taskID: taskID, runID: runID, taskState: .running, progress: 0.05)
        )

        do {
            state.taskRun.warnings.append(contentsOf: try taskParameterLifecycleClient.beforeExecution(
                attemptID: runID,
                surfaceID: taskID,
                workspace: parameterSession.workspace,
                values: parameterSession.values,
                enabled: parameterSession.saveLast
            ))
        } catch {
            state.taskRun.warnings.append("Automatic Last save failed: \(error)")
        }

        do {
            let execution = try genericTaskClient.startTask(
                request: GenericTaskRequest(
                    runID: runID,
                    task: task,
                    providerInvocation: providerInvocation,
                    parameterBundle: parameterSession.bundle,
                    parameterValues: parameterSession.values,
                    workingDirectoryPath: state.project.rootPath
                )
            ) { [weak self] event in
                DispatchQueue.main.async {
                    self?.handleGenericTaskEvent(event, runID: runID)
                }
            }
            activeTaskExecutions[runID] = execution
        } catch {
            state.taskRun.warnings.append(contentsOf: taskParameterLifecycleClient.afterCompletion(
                attemptID: runID,
                successful: false
            ))
            finalizeNotebookTaskRecording(
                runID: runID,
                status: "failed",
                diagnostics: ["\(error)"]
            )
            state.taskRun = TaskRun(
                state: .failed,
                progress: 1.0,
                logLines: ["Failed to start \(task.executable)."],
                warnings: [],
                products: [],
                diagnostics: ["\(error)"],
                requestSummary: summary,
                imagerProgress: imagerProgressSnapshot(taskID: taskID, runID: runID, taskState: .failed, progress: 1.0)
            )
            state.lastErrors.append("Start \(task.displayName): \(error)")
        }
    }

    private func beginNotebookTaskRecording(
        runID: String,
        tabID: String,
        taskID: String,
        session: SurfaceParameterSession,
        runSafety: SurfaceRunSafety
    ) {
        guard state.hasProject else { return }
        let bypass = state.notebookRecordingBypassTabs.remove(tabID) != nil
        let explicitParameters = session.snapshot.states.compactMapValues { state -> JSONValue? in
            guard state.explicit, let value = state.value else { return nil }
            return JSONValue(parameterValue: value)
        }
        let resolvedParameters = session.values.mapValues(JSONValue.init(parameterValue:))
        let outputPaths = session.bundle.surface.bindings.compactMap { binding -> String? in
            guard binding.contextRole == "output_product",
                  let value = session.values[binding.name]
            else { return nil }
            return resolvedTaskPathString(value.displayText)
        }
        let intent = NotebookTaskIntent(
            format: 1,
            surface: taskID,
            kind: session.bundle.surface.kind,
            contract: UInt32(clamping: session.bundle.surface.contractVersion),
            parameters: explicitParameters
        )
        let approvals = runSafety.requiresInteractiveConfirmation ? [
            NotebookApprovalRecord(
                kind: "run_safety",
                actor: "user",
                timestamp: Self.unixMilliseconds(),
                contentHash: nil
            )
        ] : []
        do {
            let result = try notebookPersistenceClient.beginRecording(request: NotebookBeginRecordingRequest(
                projectRoot: state.project.rootPath,
                policy: bypass ? .bypassOnce : .record,
                request: NotebookRecordingRequest(
                    initiatingSurface: "gui",
                    operationId: taskID,
                    notebookId: state.scientificNotebooks?.activeNotebookID,
                    cellId: nil,
                    taskIntent: intent,
                    executionInput: nil,
                    providerContractVersion: UInt32(clamping: session.bundle.surface.contractVersion),
                    resolvedParameters: resolvedParameters,
                    runSafety: NotebookRunSafetyRecord(
                        classification: runSafety.classes.map(\.protocolValue).joined(separator: ","),
                        affectedPaths: outputPaths
                    ),
                    approvals: approvals
                )
            ))
            if let handle = result.handle {
                notebookAttemptHandles[runID] = handle
            }
            if let warning = result.warning {
                presentNotebookRecordingWarning(warning)
            }
            loadScientificNotebooks()
        } catch {
            presentNotebookRecordingWarning("could not start: \(error)")
        }
    }

    private func beginNotebookOperationRecording(
        operationID: String,
        parameters: [String: JSONValue],
        classification: String,
        affectedPaths: [String],
        bypassTabID: String
    ) -> NotebookAttemptHandle? {
        guard state.hasProject else { return nil }
        let bypass = state.notebookRecordingBypassTabs.remove(bypassTabID) != nil
        do {
            let result = try notebookPersistenceClient.beginRecording(request: NotebookBeginRecordingRequest(
                projectRoot: state.project.rootPath,
                policy: bypass ? .bypassOnce : .record,
                request: NotebookRecordingRequest(
                    initiatingSurface: "gui",
                    operationId: operationID,
                    notebookId: state.scientificNotebooks?.activeNotebookID,
                    cellId: nil,
                    taskIntent: nil,
                    executionInput: nil,
                    providerContractVersion: 1,
                    resolvedParameters: parameters,
                    runSafety: NotebookRunSafetyRecord(
                        classification: classification,
                        affectedPaths: affectedPaths
                    ),
                    approvals: [NotebookApprovalRecord(
                        kind: "user_action",
                        actor: "user",
                        timestamp: Self.unixMilliseconds(),
                        contentHash: nil
                    )]
                )
            ))
            if let warning = result.warning {
                presentNotebookRecordingWarning(warning)
            }
            return result.handle
        } catch {
            presentNotebookRecordingWarning("could not start \(operationID): \(error)")
            return nil
        }
    }

    private func finalizeNotebookOperationRecording(
        handle: NotebookAttemptHandle?,
        status: String,
        affectedPaths: [String],
        products: [String],
        diagnostics: [String]
    ) {
        guard let handle else { return }
        do {
            try notebookPersistenceClient.finalizeRecording(request: NotebookFinalizeRecordingRequest(
                projectRoot: state.project.rootPath,
                handle: handle,
                finalization: NotebookReceiptFinalization(
                    status: status,
                    finishedAt: Self.unixMilliseconds(),
                    affectedPaths: affectedPaths,
                    products: products.map {
                        NotebookReceiptArtifact(role: "product", path: $0, mediaType: nil)
                    },
                    artifacts: [],
                    diagnostics: diagnostics,
                    stdout: Data(),
                    stderr: Data(),
                    casaLog: Self.configuredCasaLogPath
                )
            ))
            loadScientificNotebooks()
        } catch {
            presentNotebookRecordingWarning("could not finalize operation: \(error)")
        }
    }

    private func finalizeNotebookTaskRecording(
        runID: String,
        status: String,
        affectedPaths: [String] = [],
        products: [String] = [],
        diagnostics: [String] = [],
        stdout: String = "",
        stderr: String = ""
    ) {
        guard let handle = notebookAttemptHandles.removeValue(forKey: runID) else { return }
        do {
            try notebookPersistenceClient.finalizeRecording(request: NotebookFinalizeRecordingRequest(
                projectRoot: state.project.rootPath,
                handle: handle,
                finalization: NotebookReceiptFinalization(
                    status: status,
                    finishedAt: Self.unixMilliseconds(),
                    affectedPaths: affectedPaths,
                    products: products.map {
                        NotebookReceiptArtifact(role: "product", path: $0, mediaType: nil)
                    },
                    artifacts: [],
                    diagnostics: diagnostics,
                    stdout: Data(stdout.utf8),
                    stderr: Data(stderr.utf8),
                    casaLog: Self.configuredCasaLogPath
                )
            ))
            loadScientificNotebooks()
        } catch {
            presentNotebookRecordingWarning("could not finalize: \(error)")
        }
    }

    private func presentNotebookRecordingWarning(_ warning: String) {
        let message = "Notebook recording warning: \(warning)"
        state.taskRun.warnings.append(message)
        state.lastErrors.append(message)
    }

    private static func unixMilliseconds() -> UInt64 {
        UInt64(max(0, Date().timeIntervalSince1970 * 1_000))
    }

    private static var configuredCasaLogPath: String? {
        ProcessInfo.processInfo.environment["CASA_RS_LOG_TABLE"]
    }

    public func stopTask() {
        guard !rejectPrototypeProductionAction("Task cancellation") else { return }
        if state.isDemoProject {
            state.taskRun.state = .stopped
            state.taskRun.logLines.append("Stopped fixture task.")
            return
        }

        guard state.taskRun.state == .running, let runID = state.taskRun.runID else {
            state.lastErrors.append("No task is running")
            return
        }
        cancelJob(runID, recordError: false)
        let title = state.activeTaskID == "imager" ? "Imager cancelled" : "Task cancelled"
        let reason = "User cancelled \(state.activeTaskID)."
        state.history.append(ProcessingHistoryEvent(
            id: "hist-run-\(state.history.count + 1)",
            timestamp: currentTimestamp(),
            title: title,
            reason: reason,
            affectedPaths: state.taskRun.outputPaths,
            approval: "user"
        ))
    }

    package func selectPrototypePythonCell(_ cellID: String) {
        guard runtimeKind == .pythonPrototype,
              state.prototypePython?.cells.contains(where: { $0.id == cellID }) == true
        else { return }
        state.prototypePython?.selectedCellID = cellID
        republishPrototypePythonState()
    }

    package func setPrototypePythonSource(cellID: String, source: String) {
        guard runtimeKind == .pythonPrototype,
              var prototype = state.prototypePython,
              let index = prototype.cells.firstIndex(where: { $0.id == cellID })
        else { return }
        prototype.cells[index].source = source
        state.prototypePython = prototype
    }

    package func approvePrototypePythonSource(cellID: String) {
        guard runtimeKind == .pythonPrototype,
              var prototype = state.prototypePython,
              let index = prototype.cells.firstIndex(where: { $0.id == cellID }),
              prototype.cells[index].owner == .ai
        else { return }
        prototype.cells[index].approvedSourceDigest = prototype.cells[index].sourceDigest
        state.prototypePython = prototype
    }

    package func runPrototypePythonCell(_ cellID: String) {
        guard runtimeKind == .pythonPrototype,
              var prototype = state.prototypePython,
              prototype.kernelState == .ready,
              let index = prototype.cells.firstIndex(where: { $0.id == cellID }),
              prototype.cells[index].approvalIsValid
        else { return }

        let sequence = prototype.nextExecutionSequence
        prototype.nextExecutionSequence = sequence + 1
        prototype.selectedCellID = cellID
        prototype.kernelState = .running
        prototype.runningCellID = cellID
        let digest = prototype.cells[index].sourceDigest
        prototype.cells[index].revisions.append(
            PrototypePythonExecutionRevision(
                id: "python-execution-\(sequence)",
                sequence: sequence,
                status: .running,
                sourceDigest: digest,
                outputs: [PrototypePythonOutputEvent(
                    id: "python-output-\(sequence)-1",
                    order: 1,
                    channel: .stdout,
                    text: "Fixture kernel accepted cell \(cellID)."
                )]
            )
        )
        let nonresponsive = prototype.cells[index].behavior == .nonresponsive
        publishPrototypePythonState(prototype)

        guard !nonresponsive else { return }
        DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) { [weak self] in
            self?.completePrototypePythonCell(cellID: cellID, sequence: sequence)
        }
    }

    package func runAllPrototypePythonCells() {
        guard runtimeKind == .pythonPrototype,
              state.prototypePython?.kernelState == .ready
        else { return }
        let cellIDs = state.prototypePython?.cells
            .filter { $0.behavior != .nonresponsive && $0.approvalIsValid }
            .map(\.id) ?? []
        for cellID in cellIDs {
            let sequence = state.prototypePython?.nextExecutionSequence ?? 1
            state.prototypePython?.nextExecutionSequence = sequence + 1
            appendCompletedPrototypePythonRevision(cellID: cellID, sequence: sequence)
        }
        state.prototypePython?.selectedCellID = cellIDs.last ?? state.prototypePython?.selectedCellID ?? ""
        republishPrototypePythonState()
    }

    package func interruptPrototypePythonKernel() {
        guard runtimeKind == .pythonPrototype,
              state.prototypePython?.kernelState == .running,
              let cellID = state.prototypePython?.runningCellID,
              let cellIndex = state.prototypePython?.cells.firstIndex(where: { $0.id == cellID }),
              let revisionIndex = state.prototypePython?.cells[cellIndex].revisions.lastIndex(where: { $0.status == .running })
        else { return }

        state.prototypePython?.cells[cellIndex].revisions[revisionIndex].status = .interrupted
        state.prototypePython?.cells[cellIndex].revisions[revisionIndex].outputs.append(
            PrototypePythonOutputEvent(
                id: "python-output-interrupt-\(state.prototypePython?.cells[cellIndex].revisions[revisionIndex].sequence ?? 0)",
                order: 2,
                channel: .stderr,
                text: state.prototypePython?.cells[cellIndex].behavior == .nonresponsive
                    ? "Interrupt was ignored; restart is required."
                    : "KeyboardInterrupt"
            )
        )
        let requiresRestart = state.prototypePython?.cells[cellIndex].behavior == .nonresponsive
        state.prototypePython?.kernelState = requiresRestart ? .restartRequired : .ready
        state.prototypePython?.runningCellID = nil
        republishPrototypePythonState()
    }

    package func restartPrototypePythonKernel() {
        guard runtimeKind == .pythonPrototype else { return }
        if state.prototypePython?.kernelState == .running {
            interruptPrototypePythonKernel()
        }
        state.prototypePython?.kernelState = .ready
        state.prototypePython?.runningCellID = nil
        republishPrototypePythonState()
    }

    package func regeneratePrototypePythonPlot(cellID: String) {
        guard runtimeKind == .pythonPrototype,
              var prototype = state.prototypePython,
              prototype.kernelState == .ready,
              let cell = prototype.cells.first(where: { $0.id == cellID }),
              cell.behavior == .plot,
              cell.approvalIsValid
        else { return }
        let sequence = prototype.nextExecutionSequence
        prototype.nextExecutionSequence = sequence + 1
        publishPrototypePythonState(prototype)
        appendCompletedPrototypePythonRevision(cellID: cellID, sequence: sequence)
    }

    package func insertPrototypePythonPlot(cellID: String, plotID: String) {
        guard runtimeKind == .pythonPrototype,
              let cellIndex = state.prototypePython?.cells.firstIndex(where: { $0.id == cellID }),
              let revisionIndex = state.prototypePython?.cells[cellIndex].revisions.firstIndex(where: { $0.plot?.id == plotID })
        else { return }
        state.prototypePython?.cells[cellIndex].revisions[revisionIndex].plot?.insertedInNotebook = true
        republishPrototypePythonState()
    }

    package func openPrototypeExplorer(visualizationID: String) {
        guard runtimeKind == .pythonPrototype,
              let visualization = state.prototypePython?.savedVisualizations.first(where: { $0.id == visualizationID }),
              let revision = visualization.latestRevision
        else { return }
        state.prototypePython?.activeExplorer = PrototypeExplorerSession(
            kind: revision.kind,
            title: revision.title,
            parameters: revision.parameters,
            presentationAspect: revision.presentationAspect,
            targetVisualizationID: visualizationID
        )
        republishPrototypePythonState()
    }

    package func closePrototypeExplorer() {
        guard runtimeKind == .pythonPrototype else { return }
        state.prototypePython?.activeExplorer = nil
        republishPrototypePythonState()
    }

    package func setPrototypeExplorerParameter(id: String, value: String) {
        guard runtimeKind == .pythonPrototype,
              let index = state.prototypePython?.activeExplorer?.parameters.firstIndex(where: { $0.id == id })
        else { return }
        state.prototypePython?.activeExplorer?.parameters[index].value = value
        republishPrototypePythonState()
    }

    package func saveNewPrototypeExplorerVisualization() {
        guard runtimeKind == .pythonPrototype,
              let session = state.prototypePython?.activeExplorer
        else { return }
        let sequence = state.prototypePython?.nextVisualizationSequence ?? 1
        state.prototypePython?.nextVisualizationSequence = sequence + 1
        let visualizationID = "saved-explorer-\(sequence)"
        state.prototypePython?.savedVisualizations.append(PrototypeNotebookVisualization(
            id: visualizationID,
            revisions: [prototypeVisualizationRevision(
                visualizationID: visualizationID,
                sequence: 1,
                session: session
            )]
        ))
        state.prototypePython?.activeExplorer?.targetVisualizationID = visualizationID
        republishPrototypePythonState()
    }

    package func updatePrototypeExplorerVisualization() {
        guard runtimeKind == .pythonPrototype,
              let session = state.prototypePython?.activeExplorer,
              let visualizationID = session.targetVisualizationID,
              let visualizationIndex = state.prototypePython?.savedVisualizations.firstIndex(where: {
                  $0.id == visualizationID
              })
        else { return }
        let sequence = (state.prototypePython?.savedVisualizations[visualizationIndex]
            .latestRevision?.sequence ?? 0) + 1
        state.prototypePython?.savedVisualizations[visualizationIndex].revisions.append(
            prototypeVisualizationRevision(
                visualizationID: visualizationID,
                sequence: sequence,
                session: session
            )
        )
        republishPrototypePythonState()
    }

    package func setPrototypeEnlargedVisualization(_ visualizationID: String?) {
        guard runtimeKind == .pythonPrototype else { return }
        state.prototypePython?.enlargedVisualizationID = visualizationID
        republishPrototypePythonState()
    }

    private func prototypeVisualizationRevision(
        visualizationID: String,
        sequence: Int,
        session: PrototypeExplorerSession
    ) -> PrototypeNotebookVisualizationRevision {
        PrototypeNotebookVisualizationRevision(
            id: "\(visualizationID)-r\(sequence)",
            sequence: sequence,
            title: session.title,
            kind: session.kind,
            parameters: session.parameters,
            assetPath: "notebooks/assets/explorers/\(visualizationID)/r\(sequence).png",
            presentationAspect: session.presentationAspect
        )
    }

    private func completePrototypePythonCell(cellID: String, sequence: Int) {
        guard runtimeKind == .pythonPrototype,
              let prototype = state.prototypePython,
              prototype.kernelState == .running,
              prototype.runningCellID == cellID
        else { return }
        appendCompletedPrototypePythonRevision(cellID: cellID, sequence: sequence, replacingRunning: true)
        guard var completedPrototype = state.prototypePython else { return }
        completedPrototype.kernelState = .ready
        completedPrototype.runningCellID = nil
        publishPrototypePythonState(completedPrototype)
    }

    private func appendCompletedPrototypePythonRevision(
        cellID: String,
        sequence: Int,
        replacingRunning: Bool = false
    ) {
        guard var prototype = state.prototypePython,
              let cellIndex = prototype.cells.firstIndex(where: { $0.id == cellID })
        else { return }
        let cell = prototype.cells[cellIndex]
        let fails = cell.behavior == .failure && cell.source.contains("raise RuntimeError")
        let status: PrototypePythonCellStatus = fails ? .failed : .succeeded
        var outputs = [
            PrototypePythonOutputEvent(
                id: "python-output-\(sequence)-1",
                order: 1,
                channel: .stdout,
                text: fails ? "checking continuum selection" : "Completed deterministic fixture execution."
            )
        ]
        if fails {
            outputs.append(PrototypePythonOutputEvent(
                id: "python-output-\(sequence)-2",
                order: 2,
                channel: .error,
                text: "RuntimeError: fixture: channel selection is empty"
            ))
        } else {
            outputs.append(PrototypePythonOutputEvent(
                id: "python-output-\(sequence)-2",
                order: 2,
                channel: .stderr,
                text: "Fixture environment: casa-rs-python / matplotlib"
            ))
        }
        let plot = cell.behavior == .plot && !fails
            ? PrototypePythonPlotRevision(
                id: "python-plot-\(sequence)",
                sequence: sequence,
                title: cellID == "python-cell-ai"
                    ? "AI proposal · radial profile"
                    : "TW Hya · amplitude vs UV distance",
                pngPath: "notebooks/assets/\(cellID)/execution-\(sequence)/figure-1.png",
                svgPath: "notebooks/assets/\(cellID)/execution-\(sequence)/figure-1.svg",
                presentationAspect: .standardFourThree
            )
            : nil
        let revision = PrototypePythonExecutionRevision(
            id: "python-execution-\(sequence)",
            sequence: sequence,
            status: status,
            sourceDigest: cell.sourceDigest,
            outputs: outputs,
            plot: plot
        )
        if replacingRunning,
           let revisionIndex = prototype.cells[cellIndex].revisions.lastIndex(where: { $0.sequence == sequence })
        {
            prototype.cells[cellIndex].revisions[revisionIndex] = revision
        } else {
            prototype.cells[cellIndex].revisions.append(revision)
        }
        publishPrototypePythonState(prototype)
    }

    private func republishPrototypePythonState() {
        guard let prototype = state.prototypePython else { return }
        publishPrototypePythonState(prototype)
    }

    private func publishPrototypePythonState(_ prototype: PrototypePythonNotebookProjection) {
        var updatedState = state
        updatedState.prototypePython = prototype
        state = updatedState
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

    private func imagerProgressSnapshot(
        taskID: String? = nil,
        runID: String? = nil,
        taskState: TaskRunState? = nil,
        progress: Double? = nil
    ) -> ImagerProgressSnapshot? {
        let resolvedTaskID = taskID ?? state.activeTaskID
        let selectedDatasetName = state.selectedDataset?.name
        let request = ImagerProgressRequest(
            taskID: resolvedTaskID,
            runID: runID ?? state.taskRun.runID,
            taskState: taskState ?? state.taskRun.state,
            progress: progress ?? state.taskRun.progress,
            datasetName: selectedDatasetName,
            requestSummary: state.taskRun.requestSummary
        )
        return imagerProgressSource.snapshot(for: request)
    }

    private func succeededImagerProgressSnapshot(runID: String) -> ImagerProgressSnapshot? {
        terminalImagerProgressSnapshot(
            taskID: "imager",
            runID: runID,
            taskState: .succeeded,
            progress: 1.0
        )
    }

    private func terminalImagerProgressSnapshot(
        taskID: String,
        runID: String,
        taskState: TaskRunState,
        progress: Double
    ) -> ImagerProgressSnapshot? {
        var progressSnapshot = state.taskRun.imagerProgress ?? imagerProgressSnapshot(
            taskID: taskID,
            runID: runID,
            taskState: taskState,
            progress: progress
        )
        progressSnapshot?.runID = runID
        progressSnapshot?.state = taskState
        return progressSnapshot
    }

    private func terminalTaskProgress(from progressSnapshot: ImagerProgressSnapshot?) -> Double {
        if let progressSnapshot {
            return min(1, max(0, progressSnapshot.workEstimate.fraction))
        }
        return min(1, max(0, state.taskRun.progress))
    }

    public func cancelJob(_ jobID: String) {
        guard !rejectPrototypeProductionAction("Job cancellation") else { return }
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
        job.cancellationRequested = true
        job.lastEvent = "cancelled"
        job.logLines.append("Cancellation requested.")
        state.jobs[jobID] = job
        if state.activeJobIDsByTab[job.tabID] == jobID {
            state.activeJobIDsByTab.removeValue(forKey: job.tabID)
        }

        switch job.kind {
        case .measurementSetPlot:
            state.lastErrors.append(contentsOf: taskParameterLifecycleClient.afterCompletion(
                attemptID: jobID,
                successful: false
            ))
            if let datasetID = datasetIDForExplorerTabID(job.tabID),
               var plotState = state.measurementSetPlots[datasetID] {
                plotState.status = .idle
                plotState.lastError = "Cancelled"
                state.measurementSetPlots[datasetID] = plotState
            }
        case .genericTask:
            activeTaskExecutions[jobID]?.cancel()
            activeTaskExecutions.removeValue(forKey: jobID)
            state.taskRun.warnings.append(contentsOf: taskParameterLifecycleClient.afterCompletion(
                attemptID: jobID,
                successful: false
            ))
            finalizeNotebookTaskRecording(
                runID: jobID,
                status: "cancelled",
                diagnostics: ["Cancellation requested by the user."]
            )
            if state.taskRun.runID == jobID {
                let progressSnapshot = terminalImagerProgressSnapshot(
                    taskID: state.activeTaskID,
                    runID: jobID,
                    taskState: .cancelled,
                    progress: state.taskRun.progress
                )
                state.taskRun.state = .cancelled
                state.taskRun.progress = terminalTaskProgress(from: progressSnapshot)
                state.taskRun.logLines.append("Cancellation requested for task.")
                state.taskRun.imagerProgress = progressSnapshot
            }
        }
    }

    private func nextJobID(prefix: String) -> String {
        "\(prefix)-\(state.jobs.count + 1)"
    }

    private func nextTaskTabID() -> String {
        var index = state.tabs.filter { $0.kind == .task }.count + 1
        while state.tabs.contains(where: { $0.id == "tab-tasks-\(index)" }) {
            index += 1
        }
        return "tab-tasks-\(index)"
    }

    private func taskTitle(_ taskID: String) -> String {
        state.applicationCatalog.first { $0.id == taskID }?.displayName ?? "Tasks"
    }

    private func datasetIDForExplorerTabID(_ tabID: String) -> String? {
        let prefix = "tab-explorer-"
        guard tabID.hasPrefix(prefix) else { return nil }
        return String(tabID.dropFirst(prefix.count))
    }

    private func tableBrowserTabID(for datasetID: String) -> String {
        "tab-tablebrowser-\(datasetID)"
    }

    private func canBrowseAsTable(_ dataset: DatasetSummary) -> Bool {
        dataset.kind == .measurementSet || dataset.kind == .table || dataset.kind == .calibrationTable
    }

    private func refreshTableBrowserCellWindowIfNeeded(
        dataset: DatasetSummary,
        browserState: inout TableBrowserSessionState,
        force: Bool = false
    ) {
        guard browserState.selectedView == "cells" || browserState.snapshot?.view == "cells" else {
            browserState.cellWindow = nil
            return
        }
        let request = browserState.cellWindowRequest(datasetPath: dataset.path)
        if !force,
           browserState.cellWindow?.contains(
               rowStart: Int(request.rowStart),
               rowLimit: Int(request.rowLimit),
               columnStart: Int(request.columnStart),
               columnLimit: Int(request.columnLimit)
           ) == true
        {
            return
        }
        scheduleTableBrowserCellWindowLoad(
            datasetID: browserState.datasetID,
            dataset: dataset,
            request: request
        )
    }

    private func scheduleTableBrowserCellWindowLoad(
        datasetID: String,
        dataset: DatasetSummary,
        request: TableBrowserCellWindowRequest
    ) {
        guard runtimeKind == .production else { return }
        let generation = (tableBrowserCellWindowGenerations[datasetID] ?? 0) + 1
        tableBrowserCellWindowGenerations[datasetID] = generation
        tableBrowserQueue.async { [tableBrowserClient] in
            let result = Result {
                try tableBrowserClient.buildCellWindow(request: request)
            }
            DispatchQueue.main.async { [weak self] in
                guard let self,
                      self.tableBrowserCellWindowGenerations[datasetID] == generation,
                      var browserState = self.state.tableBrowsers[datasetID],
                      browserState.cellWindowRequest(datasetPath: dataset.path) == request
                else {
                    return
                }

                switch result {
                case let .success(window):
                    browserState.cellWindow = window
                    browserState.lastError = nil
                case let .failure(error):
                    browserState.lastError = "\(error)"
                    self.state.lastErrors.append("Load table cells for \(dataset.name): \(error)")
                }
                self.state.tableBrowsers[datasetID] = browserState
            }
        }
    }

    private func tableBrowserState(datasetID: String) -> TableBrowserSessionState {
        if let existing = state.tableBrowsers[datasetID] {
            return existing
        }
        if let dataset = state.project.datasets.first(where: { $0.id == datasetID }) {
            let instanceID = state.tabs.first(where: { $0.datasetID == datasetID && tab($0, hosts: "tablebrowser") })?.id
            applyParameterContext(
                surfaceID: "tablebrowser",
                instanceID: instanceID,
                textValues: ["table": dataset.path],
                preserveOverrides: true
            )
            if let profiled = profiledTableBrowserState(datasetID: datasetID, instanceID: instanceID) {
                return profiled
            }
        }
        return TableBrowserSessionState(
            datasetID: datasetID,
            selectedView: "overview",
            status: .failed,
            lastError: "The tablebrowser parameter contract could not be resolved.",
            snapshot: nil
        )
    }

    private static func canonicalTableBrowserView(_ view: String?) -> String {
        switch view?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
        case "summary", "overview":
            "overview"
        case "columns":
            "columns"
        case "keywords":
            "keywords"
        case "subtables":
            "subtables"
        case "rows", "cells":
            "cells"
        default:
            "overview"
        }
    }

    private static func profileTableBrowserView(_ view: String) -> String? {
        switch canonicalTableBrowserView(view) {
        case "overview": "summary"
        case "columns": "columns"
        case "keywords": "keywords"
        case "cells": "rows"
        default: nil
        }
    }

    private func applyTableBrowserStartupProfile(
        datasetPath: String,
        browserState: inout TableBrowserSessionState
    ) throws -> TableBrowserSnapshot {
        browserState.selectedView = "overview"
        browserState.focus = "main"
        browserState.commands = [
            .configure(parameters: try Self.tableBrowserParameters(from: browserState))
        ]
        browserState.transientCommands = []
        return try tableBrowserClient.buildSnapshot(
            request: browserState.snapshotRequest(datasetPath: datasetPath)
        )
    }

    private static func tableBrowserParameters(
        from browserState: TableBrowserSessionState
    ) throws -> TableBrowserParameters {
        let contentMode = browserState.contentMode
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
        guard ["auto", "compact", "detailed"].contains(contentMode) else {
            throw NSError(
                domain: "CasarsMac.TableBrowserProfile",
                code: 1,
                userInfo: [
                    NSLocalizedDescriptionKey:
                        "Unsupported tablebrowser contentmode \(browserState.contentMode.debugDescription)."
                ]
            )
        }
        let linkedTable = browserState.linkedTable.trimmingCharacters(in: .whitespacesAndNewlines)
        return CasarsMacCore.tableBrowserParameters(
            view: canonicalTableBrowserView(browserState.profileView),
            rowStart: max(0, browserState.cellWindowRowStart),
            rowCount: max(1, browserState.cellWindowRowLimit),
            linkedTable: linkedTable.isEmpty || linkedTable.caseInsensitiveCompare("none") == .orderedSame
                ? nil
                : linkedTable,
            bookmark: try parseTableBrowserBookmark(browserState.bookmark),
            contentMode: contentMode
        )
    }

    private static func parseTableBrowserBookmark(_ rawValue: String) throws -> TableBrowserBookmark? {
        let bookmark = rawValue.trimmingCharacters(in: .whitespacesAndNewlines)
        if bookmark.isEmpty || bookmark.caseInsensitiveCompare("none") == .orderedSame {
            return nil
        }
        let parts = bookmark.split(separator: ":", omittingEmptySubsequences: false).map(String.init)
        if parts.count >= 3, parts[0] == "cell", let row = Int(parts[1]) {
            let column = parts.dropFirst(2).joined(separator: ":")
            if row >= 0, !column.isEmpty {
                return .cell(row: UInt64(row), column: column)
            }
        } else if parts.count >= 2, parts[0] == "table-keyword" {
            let path = tableBrowserBookmarkPath(parts.dropFirst().joined(separator: ":"))
            if !path.isEmpty {
                return .tableKeyword(path: path)
            }
        } else if parts.count >= 3, parts[0] == "column-keyword" {
            let column = parts[1]
            let path = tableBrowserBookmarkPath(parts.dropFirst(2).joined(separator: ":"))
            if !column.isEmpty, !path.isEmpty {
                return .columnKeyword(column: column, path: path)
            }
        } else if parts.count >= 2, parts[0] == "subtable" {
            let name = parts.dropFirst().joined(separator: ":")
                .trimmingCharacters(in: .whitespacesAndNewlines)
            if !name.isEmpty {
                return .subtable(name: name)
            }
        }
        throw NSError(
            domain: "CasarsMac.TableBrowserProfile",
            code: 2,
            userInfo: [
                NSLocalizedDescriptionKey:
                    "Invalid tablebrowser bookmark \(bookmark.debugDescription)."
            ]
        )
    }

    private static func tableBrowserBookmarkPath(_ value: String) -> [String] {
        value
            .split(whereSeparator: { $0 == "." || $0 == "/" })
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
    }

    @discardableResult
    private func appendTableBrowserMove(
        from selectedIndex: Int,
        to targetIndex: Int,
        into browserState: inout TableBrowserSessionState
    ) -> Bool {
        let delta = targetIndex - selectedIndex
        if delta > 0 {
            browserState.commands.append(.moveDown(steps: UInt64(delta)))
            return true
        }
        if delta < 0 {
            browserState.commands.append(.moveUp(steps: UInt64(-delta)))
            return true
        }
        return false
    }

    private func parameterWorkspacePath() -> String {
        let root = state.project.rootPath.trimmingCharacters(in: .whitespacesAndNewlines)
        if state.hasProject, !root.isEmpty {
            return root
        }
        return FileManager.default.currentDirectoryPath
    }

    private func parameterInstanceID(surfaceID: String, requested: String? = nil) -> String {
        if let requested, !requested.isEmpty {
            return requested
        }
        if let activeTab = state.tabs.first(where: { $0.id == state.activeTabID }),
           tab(activeTab, hosts: surfaceID) {
            return activeTab.id
        }
        if let matchingTab = state.tabs.first(where: { tab($0, hosts: surfaceID) }) {
            return matchingTab.id
        }
        return "surface-\(surfaceID)"
    }

    private func tab(_ tab: WorkbenchTab, hosts surfaceID: String) -> Bool {
        if tab.kind == .task {
            return tab.taskID == surfaceID
        }
        guard let datasetID = tab.datasetID,
              let dataset = state.project.datasets.first(where: { $0.id == datasetID })
        else { return false }
        switch surfaceID {
        case "msexplore":
            return tab.kind == .datasetExplorer && dataset.kind == .measurementSet
        case "imexplore":
            return tab.kind == .datasetExplorer && dataset.kind == .imageCube
        case "tablebrowser":
            return tab.kind == .tableBrowser
                || (tab.kind == .datasetExplorer
                    && (dataset.kind == .table || dataset.kind == .calibrationTable))
        default:
            return false
        }
    }

    private func parameterSessionKey(surfaceID: String, instanceID: String? = nil) -> String {
        "\(parameterInstanceID(surfaceID: surfaceID, requested: instanceID))::\(surfaceID)"
    }

    private func parameterInstanceID(surfaceID: String, datasetID: String) -> String {
        if let tab = state.tabs.first(where: { $0.datasetID == datasetID && tab($0, hosts: surfaceID) }) {
            return tab.id
        }
        if surfaceID == "tablebrowser" {
            return tableBrowserTabID(for: datasetID)
        }
        return state.project.datasets.first(where: { $0.id == datasetID })?.explorerTabID
            ?? "tab-explorer-\(datasetID)"
    }

    public func parameterSession(
        surfaceID: String,
        instanceID: String? = nil
    ) -> SurfaceParameterSession? {
        state.parameterSessions[parameterSessionKey(surfaceID: surfaceID, instanceID: instanceID)]
    }

    private func loadParameterSessionIfNeeded(_ surfaceID: String, instanceID: String? = nil) {
        guard runtimeKind == .production else { return }
        let sessionKey = parameterSessionKey(surfaceID: surfaceID, instanceID: instanceID)
        guard !surfaceID.isEmpty, state.parameterSessions[sessionKey] == nil else { return }
        do {
            let bundle = try surfaceParameterClient.loadBundle(surfaceID: surfaceID)
            let workspace = parameterWorkspacePath()
            let lastSnapshot: SurfaceParameterSnapshot?
            do {
                lastSnapshot = try surfaceParameterClient.last(
                    surfaceID: surfaceID,
                    workspace: workspace,
                    successful: false
                )
            } catch {
                state.lastErrors.append("Load Last parameters for \(surfaceID): \(error)")
                return
            }
            if let snapshot = lastSnapshot {
                state.parameterSessions[sessionKey] = SurfaceParameterSession(
                    bundle: bundle,
                    snapshot: snapshot,
                    selectedSource: .last,
                    baseProfileTOML: snapshot.profileToml,
                    baseProfilePath: nil,
                    workspace: workspace
                )
                return
            }
            let snapshot = try surfaceParameterClient.defaults(surfaceID: surfaceID)
            state.parameterSessions[sessionKey] = SurfaceParameterSession(
                bundle: bundle,
                snapshot: snapshot,
                selectedSource: .defaults,
                baseProfileTOML: nil,
                baseProfilePath: nil,
                workspace: workspace
            )
        } catch {
            state.lastErrors.append("Load parameter contract for \(surfaceID): \(error)")
        }
    }

    @discardableResult
    private func resolveParameterSession(
        _ session: inout SurfaceParameterSession,
        editedParameters: Set<String> = []
    ) -> Bool {
        guard runtimeKind == .production else { return false }
        do {
            session.snapshot = try surfaceParameterClient.resolve(
                surfaceID: session.bundle.surface.id,
                baseSource: session.selectedSource,
                profileTOML: session.baseProfileTOML,
                profilePath: session.baseProfilePath,
                context: session.contextPatch,
                override: session.overridePatch
            )
            session.draftText = [:]
            return true
        } catch {
            let unresolvedParameters = Set(session.draftText.keys)
                .union(editedParameters)
                .sorted()
            let subject = unresolvedParameters.isEmpty
                ? "parameter draft"
                : "parameter draft for \(unresolvedParameters.joined(separator: ", "))"
            let message = "Could not resolve \(subject): \(error)"
            session.snapshot.diagnostics.removeAll { $0.code == "draft_resolution_failed" }
            session.snapshot.diagnostics.append(SurfaceParameterDiagnostic(
                level: "error",
                code: "draft_resolution_failed",
                message: message,
                parameter: unresolvedParameters.count == 1 ? unresolvedParameters[0] : nil,
                location: nil,
                suggestions: []
            ))
            session.snapshot.dirty = true
            state.lastErrors.append("Resolve parameters for \(session.bundle.surface.id): \(error)")
            return false
        }
    }

    private func applyParameterContext(
        surfaceID: String,
        instanceID: String? = nil,
        textValues: [String: String],
        boolValues: [String: Bool] = [:],
        preserveOverrides: Bool
    ) {
        loadParameterSessionIfNeeded(surfaceID, instanceID: instanceID)
        let sessionKey = parameterSessionKey(surfaceID: surfaceID, instanceID: instanceID)
        guard var session = state.parameterSessions[sessionKey] else { return }
        var editedParameters = Set<String>()
        for (name, text) in textValues {
            guard let concept = session.bundle.concept(for: name) else { continue }
            if preserveOverrides, session.overridePatch.values[name] != nil { continue }
            session.contextPatch.removeUnset(name)
            session.contextPatch.values[name] = concept.valueDomain.value(from: text)
            editedParameters.insert(name)
            if !preserveOverrides {
                session.overridePatch.values.removeValue(forKey: name)
                session.overridePatch.removeUnset(name)
            }
        }
        for (name, value) in boolValues {
            guard session.bundle.concept(for: name) != nil else { continue }
            if preserveOverrides, session.overridePatch.values[name] != nil { continue }
            session.contextPatch.removeUnset(name)
            session.contextPatch.values[name] = .bool(value)
            editedParameters.insert(name)
            if !preserveOverrides {
                session.overridePatch.values.removeValue(forKey: name)
                session.overridePatch.removeUnset(name)
            }
        }
        resolveParameterSession(&session, editedParameters: editedParameters)
        state.parameterSessions[sessionKey] = session
    }

    private func applySelectedDatasetParameterContext(surfaceID: String, instanceID: String? = nil) {
        let sessionKey = parameterSessionKey(surfaceID: surfaceID, instanceID: instanceID)
        guard let dataset = state.selectedDataset,
              let session = state.parameterSessions[sessionKey]
        else { return }
        var suggestions: [String: String] = [:]
        for binding in session.bundle.surface.bindings {
            guard session.snapshot.states[binding.name]?.origin == "default",
                  let concept = session.bundle.concept(for: binding.name)
            else { continue }
            if binding.contextRole == "region_reference", dataset.kind == .region {
                suggestions[binding.name] = projectRelativePath(dataset.path)
                continue
            }
            if concept.semanticRole == "input_data",
               parameterResource(concept.valueDomain.resourceKind, accepts: dataset) {
                suggestions[binding.name] = projectRelativePath(dataset.path)
                continue
            }
            if concept.semanticRole == "output_data",
               session.snapshot.states[binding.name]?.value == nil {
                suggestions[binding.name] = suggestedOutputPath(
                    taskID: surfaceID,
                    parameter: binding.name,
                    resourceKind: concept.valueDomain.resourceKind,
                    dataset: dataset
                )
            }
        }
        guard !suggestions.isEmpty else { return }
        applyParameterContext(
            surfaceID: surfaceID,
            instanceID: instanceID,
            textValues: suggestions,
            preserveOverrides: true
        )
    }

    private func parameterResource(_ resourceKind: String?, accepts dataset: DatasetSummary) -> Bool {
        switch resourceKind {
        case "measurement_set": return dataset.kind == .measurementSet
        case "image": return dataset.kind == .imageCube
        case "table": return dataset.kind == .table || dataset.kind == .calibrationTable
        case "calibration_table": return dataset.kind == .calibrationTable
        case "file": return dataset.kind == .runProduct
        case "any", nil: return false
        default: return false
        }
    }

    private func suggestedOutputPath(
        taskID: String,
        parameter: String,
        resourceKind: String?,
        dataset: DatasetSummary
    ) -> String {
        let stem = [".image", ".ms", ".MS", ".fits", ".fit", ".fts"].reduce(dataset.name) {
            $0.hasSuffix($1) ? String($0.dropLast($1.count)) : $0
        }
        switch resourceKind {
        case "measurement_set": return "\(stem)-\(taskID).ms"
        case "image": return "\(stem)-\(taskID).image"
        case "file" where parameter == "fitsimage": return "\(stem).fits"
        default: return "\(stem)-\(taskID)"
        }
    }

    private func sessionParameterText(_ surfaceID: String, _ name: String, instanceID: String? = nil) -> String? {
        parameterSession(surfaceID: surfaceID, instanceID: instanceID)?.snapshot.states[name]?.value?.displayText
    }

    private func sessionParameterInt(_ surfaceID: String, _ name: String, instanceID: String? = nil) -> Int? {
        guard let value = parameterSession(surfaceID: surfaceID, instanceID: instanceID)?.snapshot.states[name]?.value else { return nil }
        switch value {
        case .integer(let value): return Int(value)
        case .float(let value): return Int(value)
        case .string(let value): return Int(value)
        default: return nil
        }
    }

    private func sessionParameterDouble(_ surfaceID: String, _ name: String, instanceID: String? = nil) -> Double? {
        guard let value = parameterSession(surfaceID: surfaceID, instanceID: instanceID)?.snapshot.states[name]?.value else { return nil }
        switch value {
        case .integer(let value): return Double(value)
        case .float(let value): return value
        case .string(let value): return Double(value)
        default: return nil
        }
    }

    private func sessionParameterBool(_ surfaceID: String, _ name: String, instanceID: String? = nil) -> Bool? {
        parameterSession(surfaceID: surfaceID, instanceID: instanceID)?.snapshot.states[name]?.value?.boolValue
    }

    private func setSessionParameterValue(
        surfaceID: String,
        instanceID: String? = nil,
        name: String,
        value: SurfaceParameterValue,
        persistImmediately: Bool = false
    ) {
        guard runtimeKind == .production else { return }
        loadParameterSessionIfNeeded(surfaceID, instanceID: instanceID)
        let sessionKey = parameterSessionKey(surfaceID: surfaceID, instanceID: instanceID)
        guard var session = state.parameterSessions[sessionKey] else { return }
        session.overridePatch.removeUnset(name)
        session.overridePatch.values[name] = value
        resolveParameterSession(&session, editedParameters: [name])
        state.parameterSessions[sessionKey] = session
        if persistImmediately {
            acceptSessionParameters(surfaceID, instanceID: instanceID)
        }
    }

    private func setImageExplorerParameterValue(
        datasetID: String,
        name: String,
        value: SurfaceParameterValue,
        persistImmediately: Bool = false
    ) {
        setSessionParameterValue(
            surfaceID: "imexplore",
            instanceID: parameterInstanceID(surfaceID: "imexplore", datasetID: datasetID),
            name: name,
            value: value,
            persistImmediately: persistImmediately
        )
    }

    private func acceptSessionParameters(_ surfaceID: String, instanceID: String? = nil) {
        let sessionKey = parameterSessionKey(surfaceID: surfaceID, instanceID: instanceID)
        guard let session = state.parameterSessions[sessionKey],
            session.snapshot.surfaceKind == "session",
            !session.hasErrors
        else { return }
        do {
            state.lastErrors.append(contentsOf: try sessionParameterLifecycleClient.acceptedDurableChange(
                surfaceID: session.snapshot.surfaceId,
                workspace: session.workspace,
                values: session.values,
                enabled: session.saveLast
            ))
        } catch {
            state.lastErrors.append("Automatic Last save failed for \(surfaceID): \(error)")
        }
    }

    private func seedImagerTaskDefaults(
        for dataset: DatasetSummary,
        instanceID: String? = nil,
        preserveExistingEdits: Bool
    ) {
        loadTaskUISchemaIfNeeded("imager", instanceID: instanceID)
        let defaultField = defaultImagerField(for: dataset)
        let defaultSpectralWindow = defaultImagerSpectralWindow(for: dataset)
        let defaultCorrelation = defaultImagerCorrelation(for: dataset)
        var contextValues = [
            "vis": projectRelativePath(dataset.path),
            "imagename": projectRelativePath(defaultImagerOutputPrefix(for: dataset)),
            "datacolumn": dataset.dataColumns.first ?? "DATA",
        ]
        if isTWHyaTutorialDataset(dataset) {
            contextValues["imsize"] = "250"
            contextValues["cell"] = "0.1arcsec"
            contextValues["weighting"] = "briggs"
        }
        if let defaultField {
            contextValues["field"] = selectorToken(defaultField) ?? defaultField
        }
        if let defaultSpectralWindow {
            contextValues["spw"] = selectorToken(defaultSpectralWindow) ?? defaultSpectralWindow
        }
        if let defaultCorrelation {
            contextValues["polarization"] = selectorToken(defaultCorrelation) ?? defaultCorrelation
        }
        applyParameterContext(
            surfaceID: "imager",
            instanceID: instanceID,
            textValues: contextValues,
            boolValues: ["dirty_only": true],
            preserveOverrides: preserveExistingEdits
        )
        state.taskRun.requestSummary = genericTaskRequestSummary(taskID: "imager", instanceID: instanceID)
    }

    private func seedDirectMeasurementSetImagerDefaults(for dataset: DatasetSummary, instanceID: String? = nil) {
        seedImagerTaskDefaults(for: dataset, instanceID: instanceID, preserveExistingEdits: false)
        let phaseCenterField = dataset.fields.count > 1 ? dataset.fields.first.flatMap(selectorToken) : nil
        applyParameterContext(
            surfaceID: "imager",
            instanceID: instanceID,
            textValues: [
                "field": "",
                "phasecenter_field": phaseCenterField ?? "",
                "specmode": "cube",
                "gridder": "mosaic",
                "interpolation": "nearest",
                "channel_start": "0",
                "channel_count": "512",
                "imsize": "1024",
                "cell": "1.0arcsec",
                "weighting": "briggs",
                "robust": "0.5",
                "niter": "2048",
                "threshold": "0.0Jy",
            ],
            boolValues: [
                "dirty_only": false,
                "perchanweightdensity": true,
                "write_pb": true,
                "pbcor": true,
            ],
            preserveOverrides: false
        )
        state.taskRun.requestSummary = genericTaskRequestSummary(taskID: "imager", instanceID: instanceID)
    }

    private func genericTaskRequestSummary(taskID: String, instanceID: String? = nil) -> String {
        guard let session = parameterSession(surfaceID: taskID, instanceID: instanceID) else {
            return "task=\(taskID)"
        }
        return session.bundle.surface.bindings
            .filter { !$0.projections.presentation.hidden }
            .sorted { $0.order < $1.order }
            .compactMap { binding -> String? in
                guard let parameterState = session.snapshot.states[binding.name], parameterState.active else {
                    return nil
                }
                guard let value = parameterState.value else {
                    return parameterState.required ? "\(binding.name)=<required>" : nil
                }
                let display = session.bundle.concept(for: binding.name)?.valueDomain.isPathLike == true
                    ? projectRelativePath(value.displayText)
                    : value.displayText
                return "\(binding.name)=\(display)"
            }
            .joined(separator: ", ")
    }

    private func projectRelativePath(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty, !state.project.rootPath.isEmpty else {
            return path
        }
        let rootURL = URL(fileURLWithPath: state.project.rootPath, isDirectory: true).standardizedFileURL
        let pathURL = URL(fileURLWithPath: (trimmed as NSString).expandingTildeInPath).standardizedFileURL
        let rootPath = rootURL.path
        let absolutePath = pathURL.path
        if absolutePath == rootPath {
            return "."
        }
        let prefix = rootPath.hasSuffix("/") ? rootPath : rootPath + "/"
        if absolutePath.hasPrefix(prefix) {
            return String(absolutePath.dropFirst(prefix.count))
        }
        return path
    }

    private static func isInlineRegionSyntax(_ value: String) -> Bool {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        return trimmed.hasPrefix("box[[")
            || trimmed.hasPrefix("poly [[")
            || trimmed.hasPrefix("box:")
            || trimmed.hasPrefix("pixelbox(")
    }

    public func taskOutputSaveDirectory() -> String {
        if !state.project.rootPath.isEmpty {
            return state.project.rootPath
        }
        return FileManager.default.currentDirectoryPath
    }

    public func taskOutputSaveFilename() -> String {
        let extensionName = activeTaskOutputLooksLikeJSON() ? "json" : "txt"
        return "\(sanitizedPathComponent(state.activeTaskID))-result.\(extensionName)"
    }

    public func parameterProfileDirectory() -> String {
        if !state.project.rootPath.isEmpty {
            return state.project.rootPath
        }
        return FileManager.default.currentDirectoryPath
    }

    public func parameterProfileFilename() -> String {
        "\(sanitizedPathComponent(state.activeTaskID)).toml"
    }

    public func hasSaveableActiveParameterProfile() -> Bool {
        guard let session = parameterSession(
            surfaceID: state.activeTaskID,
            instanceID: parameterInstanceID(surfaceID: state.activeTaskID)
        ) else { return false }
        return !session.hasErrors
    }

    public func saveActiveParameterProfile(to path: String) {
        guard !rejectPrototypeProductionAction("Parameter profile saving") else { return }
        saveParameterProfile(
            surfaceID: state.activeTaskID,
            instanceID: parameterInstanceID(surfaceID: state.activeTaskID),
            to: path
        )
    }

    public func loadActiveParameterProfile(from path: String, discardEdits: Bool = false) {
        guard !rejectPrototypeProductionAction("Parameter profile loading") else { return }
        selectParameterSource(
            .file,
            surfaceID: state.activeTaskID,
            instanceID: parameterInstanceID(surfaceID: state.activeTaskID),
            profilePath: path,
            discardEdits: discardEdits
        )
    }

    public func hasSaveableActiveTaskOutput() -> Bool {
        activeTaskOutput() != nil
    }

    public func saveActiveTaskOutput(to path: String) {
        guard !rejectPrototypeProductionAction("Task output saving") else { return }
        guard let output = activeTaskOutput(), let data = output.data(using: .utf8) else {
            state.lastErrors.append("No task output is available to save.")
            return
        }

        let url = URL(fileURLWithPath: path)
        do {
            try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
            try data.write(to: url, options: .atomic)
            if !state.taskRun.outputPaths.contains(url.path) {
                state.taskRun.outputPaths.append(url.path)
            }
            state.taskRun.logLines.append("Saved output: \(url.path)")
            state.history.append(ProcessingHistoryEvent(
                id: "hist-save-task-output-\(state.history.count + 1)",
                timestamp: currentTimestamp(),
                title: "Saved \(state.activeTaskID) output",
                reason: "User saved the latest \(state.activeTaskID) task output.",
                affectedPaths: [url.path],
                approval: "user"
            ))
        } catch {
            state.lastErrors.append("Save \(state.activeTaskID) output: \(error)")
        }
    }

    private func activeTaskOutput() -> String? {
        let output = state.taskRun.rawOutput?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !output.isEmpty else {
            return nil
        }
        return output
    }

    private func activeTaskOutputLooksLikeJSON() -> Bool {
        guard let output = activeTaskOutput(), let data = output.data(using: .utf8) else {
            return false
        }
        return (try? JSONSerialization.jsonObject(with: data)) != nil
    }

    private func selectorIDValue(_ label: String) -> String? {
        let prefix = label.split(separator: ":", maxSplits: 1).first?.trimmingCharacters(in: .whitespacesAndNewlines)
        return prefix?.split(separator: " ").last.map(String.init)
    }

    private func finishMeasurementSetPlotJob(
        jobID: String,
        dataset: DatasetSummary,
        plotState requestedPlotState: MeasurementSetExplorerPlotState,
        result: MeasurementSetPlotResultSummary,
        elapsedSeconds: TimeInterval
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
        job.logLines.append(
            "Rendered \(result.renderedPointCount) points in \(formatDuration(elapsedSeconds))."
        )
        state.jobs[jobID] = job
        state.activeJobIDsByTab.removeValue(forKey: job.tabID)
        state.lastErrors.append(contentsOf: taskParameterLifecycleClient.afterCompletion(
            attemptID: jobID,
            successful: true
        ))
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
        state.lastErrors.append(contentsOf: taskParameterLifecycleClient.afterCompletion(
            attemptID: jobID,
            successful: false
        ))
        state.lastErrors.append("Render plot for \(datasetName): \(error)")
    }

    private func openExplorer(for dataset: DatasetSummary) {
        let tab = WorkbenchTab(
            id: dataset.explorerTabID,
            title: dataset.explorerTabTitle,
            kind: .datasetExplorer,
            datasetID: dataset.id
        )
        openTab(tab)
        if dataset.kind == .measurementSet && !state.isDemoProject {
            applyParameterContext(
                surfaceID: "msexplore",
                instanceID: tab.id,
                textValues: [
                    "vis": dataset.path,
                    "datacolumn": dataset.dataColumns.first ?? "DATA",
                ],
                preserveOverrides: true
            )
            _ = measurementSetPlotState(for: dataset.id)
        } else if dataset.kind == .imageCube && !state.isDemoProject {
            applyParameterContext(
                surfaceID: "imexplore",
                instanceID: tab.id,
                textValues: ["image": dataset.path],
                preserveOverrides: true
            )
            refreshImageExplorer(datasetID: dataset.id)
        } else if (dataset.kind == .table || dataset.kind == .calibrationTable) && !state.isDemoProject {
            refreshTableBrowser(datasetID: dataset.id)
        }
    }

    private func imageExplorerState(datasetID: String) -> ImageExplorerSessionState {
        if let existing = state.imageExplorers[datasetID] {
            return existing
        }
        if let dataset = state.project.datasets.first(where: { $0.id == datasetID }) {
            let instanceID = parameterInstanceID(surfaceID: "imexplore", datasetID: datasetID)
            applyParameterContext(
                surfaceID: "imexplore",
                instanceID: instanceID,
                textValues: ["image": dataset.path],
                preserveOverrides: true
            )
            if let profiled = profiledImageExplorerState(datasetID: datasetID, instanceID: instanceID) {
                return profiled
            }
        }
        return ImageExplorerSessionState(
            datasetID: datasetID,
            selectedView: "",
            status: .failed,
            lastError: "The imexplore parameter contract could not be resolved.",
            snapshot: nil
        )
    }

    private func profiledImageExplorerState(
        datasetID: String,
        instanceID: String? = nil
    ) -> ImageExplorerSessionState? {
        guard let blc = sessionParameterText("imexplore", "blc", instanceID: instanceID),
              let trc = sessionParameterText("imexplore", "trc", instanceID: instanceID),
              let inc = sessionParameterText("imexplore", "inc", instanceID: instanceID),
              let stretch = sessionParameterText("imexplore", "stretch", instanceID: instanceID),
              let autoscale = sessionParameterText("imexplore", "autoscale", instanceID: instanceID),
              let clipLow = sessionParameterText("imexplore", "clip_low", instanceID: instanceID),
              let clipHigh = sessionParameterText("imexplore", "clip_high", instanceID: instanceID),
              let selectedView = sessionParameterText("imexplore", "view", instanceID: instanceID),
              let contentMode = sessionParameterText("imexplore", "contentmode", instanceID: instanceID),
              let colorMapName = sessionParameterText("imexplore", "colormap", instanceID: instanceID),
              let profileAxis = sessionParameterText("imexplore", "profileaxis", instanceID: instanceID),
              let movieAxis = sessionParameterText("imexplore", "movieaxis", instanceID: instanceID),
              let framesPerSecond = sessionParameterDouble("imexplore", "fps", instanceID: instanceID),
              let movieLoop = sessionParameterBool("imexplore", "loop", instanceID: instanceID),
              let region = sessionParameterText("imexplore", "region", instanceID: instanceID),
              let mask = sessionParameterText("imexplore", "mask", instanceID: instanceID)
        else {
            state.lastErrors.append("The resolved imexplore contract is missing a presentation parameter.")
            return nil
        }
        let parameters = imageExplorerParameters(
            blc: blc,
            trc: trc,
            inc: inc,
            stretch: stretch,
            autoscale: autoscale,
            clipLow: clipLow,
            clipHigh: clipHigh
        )
        let colorMap: ImageExplorerColorMap?
        switch colorMapName {
        case "gray", "grayscale": colorMap = .grayscale
        default: colorMap = ImageExplorerColorMap(rawValue: colorMapName)
        }
        guard let colorMap else {
            state.lastErrors.append("Unsupported imexplore colormap \(colorMapName).")
            return nil
        }
        let regionReference: ImageExplorerRegionReference?
        do {
            regionReference = try Self.parseImageExplorerRegionReference(region)
        } catch {
            state.lastErrors.append("Invalid imexplore region reference: \(error)")
            return nil
        }
        var commands: [ImageExplorerCommand] = []
        if let regionReference {
            commands.append(.setSelectionReference(regionReference))
        }
        if mask != "none", !mask.isEmpty {
            commands.append(.setDefaultMask(name: mask))
        }
        return ImageExplorerSessionState(
            datasetID: datasetID,
            selectedView: selectedView,
            planeContentMode: contentMode,
            planeColorMap: colorMap,
            parameters: parameters,
            selectedProfileAxis: Int(profileAxis.trimmingCharacters(in: .whitespacesAndNewlines)),
            selectedProfileAxisSelector: Self.normalizedImageExplorerAxisSelector(profileAxis),
            movieAxis: Int(movieAxis.trimmingCharacters(in: .whitespacesAndNewlines)),
            movieAxisSelector: Self.normalizedImageExplorerAxisSelector(movieAxis),
            movieFramesPerSecond: Self.clampedMovieFramesPerSecond(framesPerSecond),
            movieLoop: movieLoop,
            profileCommands: commands,
            activeRegionFilePath: regionReference.flatMap { reference in
                if case .file(let path) = reference { return path }
                return nil
            },
            status: .idle,
            lastError: nil,
            snapshot: nil
        )
    }

    private static func normalizedImageExplorerAxisSelector(_ selector: String) -> String? {
        let selector = selector.trimmingCharacters(in: .whitespacesAndNewlines)
        return selector.isEmpty || selector.caseInsensitiveCompare("auto") == .orderedSame
            ? nil
            : selector
    }

    private static func resolveImageExplorerAxisSelector(
        _ selector: String?,
        parameter: String,
        snapshot: ImageExplorerSnapshot
    ) throws -> Int? {
        guard let selector = normalizedImageExplorerAxisSelector(selector ?? "") else {
            return nil
        }
        let selected = snapshot.nonDisplayAxes.first { axis in
            if let index = UInt64(selector), axis.axis == index {
                return true
            }
            return axis.label.caseInsensitiveCompare(selector) == .orderedSame
        }
        guard let selected else {
            let available = snapshot.nonDisplayAxes
                .map { "\($0.label) (\($0.axis))" }
                .joined(separator: ", ")
            throw NSError(
                domain: "CasarsMac.ImageExplorerProfile",
                code: 1,
                userInfo: [
                    NSLocalizedDescriptionKey:
                        "imexplore \(parameter)=\(selector.debugDescription) does not identify a non-display axis; available axes: \(available)"
                ]
            )
        }
        return Int(selected.axis)
    }

    private static func parseImageExplorerRegionReference(
        _ rawValue: String
    ) throws -> ImageExplorerRegionReference? {
        let value = rawValue.trimmingCharacters(in: .whitespacesAndNewlines)
        if value.isEmpty || value.caseInsensitiveCompare("none") == .orderedSame {
            return nil
        }
        if value.hasPrefix("file:") {
            let path = String(value.dropFirst("file:".count))
                .trimmingCharacters(in: .whitespacesAndNewlines)
            guard !path.isEmpty else {
                throw NSError(
                    domain: "CasarsMac.ImageExplorerProfile",
                    code: 2,
                    userInfo: [NSLocalizedDescriptionKey: "region file reference cannot be empty"]
                )
            }
            return .file(path: path)
        }
        if value.hasPrefix("definition:") {
            let name = String(value.dropFirst("definition:".count))
                .trimmingCharacters(in: .whitespacesAndNewlines)
            guard !name.isEmpty else {
                throw NSError(
                    domain: "CasarsMac.ImageExplorerProfile",
                    code: 3,
                    userInfo: [NSLocalizedDescriptionKey: "saved region definition name cannot be empty"]
                )
            }
            return .definition(name: name)
        }
        if value.contains(where: { "[](){}&|=<>!*+;".contains($0) }) {
            return .expression(expression: value)
        }
        if value.contains("/")
            || value.contains("\\")
            || value.hasSuffix(".crtf")
            || value.hasSuffix(".reg")
            || value.hasSuffix(".region")
        {
            return .file(path: value)
        }
        return .definition(name: value)
    }

    private func profiledTableBrowserState(
        datasetID: String,
        instanceID: String? = nil
    ) -> TableBrowserSessionState? {
        guard let profileView = sessionParameterText("tablebrowser", "view", instanceID: instanceID),
              let bookmark = sessionParameterText("tablebrowser", "bookmark", instanceID: instanceID),
              let rowStart = sessionParameterInt("tablebrowser", "rowstart", instanceID: instanceID),
              let rowLimit = sessionParameterInt("tablebrowser", "nrow", instanceID: instanceID),
              let linkedTable = sessionParameterText("tablebrowser", "linkedtable", instanceID: instanceID),
              let contentMode = sessionParameterText("tablebrowser", "contentmode", instanceID: instanceID)
        else {
            state.lastErrors.append("The resolved tablebrowser contract is missing a startup parameter.")
            return nil
        }
        return TableBrowserSessionState(
            datasetID: datasetID,
            selectedView: "overview",
            profileView: profileView,
            bookmark: bookmark,
            linkedTable: linkedTable,
            contentMode: contentMode,
            startupProfilePending: true,
            cellWindowRowStart: rowStart,
            cellWindowRowLimit: max(1, rowLimit),
            status: .idle,
            lastError: nil,
            snapshot: nil
        )
    }

    private func profiledMeasurementSetPlotState(
        datasetID: String,
        instanceID: String? = nil
    ) -> MeasurementSetExplorerPlotState {
        func optionalText(_ name: String) -> String? {
            guard let value = sessionParameterText("msexplore", name, instanceID: instanceID),
                  !value.isEmpty,
                  value != "none"
            else { return nil }
            return value
        }
        let presetText = optionalText("preset")
        let preset = presetText.flatMap(Self.measurementSetPreset(profileValue:)) ?? .uvCoverage
        let colorBy = Self.measurementSetColorAxis(
            profileValue: sessionParameterText("msexplore", "color_by", instanceID: instanceID)
        ) ?? .none
        let iterationAxis = Self.measurementSetIterationAxis(profileValue: optionalText("iteraxis"))
        let spectralSelection = optionalText("spw")
        let spectralParts = spectralSelection?.split(separator: ":", maxSplits: 1).map(String.init) ?? []
        return MeasurementSetExplorerPlotState(
            datasetID: datasetID,
            preset: preset,
            selectedField: optionalText("field"),
            selectedSpectralWindow: spectralParts.first,
            selectedChannelSelection: spectralParts.count == 2 ? spectralParts[1] : nil,
            selectedTimerange: optionalText("timerange"),
            selectedUVRange: optionalText("uvrange"),
            selectedAntenna: optionalText("antenna"),
            selectedScan: optionalText("scan"),
            selectedCorrelation: optionalText("correlation"),
            selectedArray: optionalText("array"),
            selectedObservation: optionalText("observation"),
            selectedIntent: optionalText("intent"),
            selectedFeed: optionalText("feed"),
            selectedMSSelect: optionalText("msselect"),
            dataColumn: sessionParameterText("msexplore", "datacolumn", instanceID: instanceID) ?? "",
            colorBy: colorBy,
            avgChannel: sessionParameterInt("msexplore", "avgchannel", instanceID: instanceID).flatMap(UInt64.init),
            avgTime: sessionParameterDouble("msexplore", "avgtime", instanceID: instanceID),
            avgScan: sessionParameterBool("msexplore", "avgscan", instanceID: instanceID) ?? false,
            avgField: sessionParameterBool("msexplore", "avgfield", instanceID: instanceID) ?? false,
            avgBaseline: sessionParameterBool("msexplore", "avgbaseline", instanceID: instanceID) ?? false,
            avgAntenna: sessionParameterBool("msexplore", "avgantenna", instanceID: instanceID) ?? false,
            avgSPW: sessionParameterBool("msexplore", "avgspw", instanceID: instanceID) ?? false,
            scalarAverage: sessionParameterBool("msexplore", "scalar", instanceID: instanceID) ?? false,
            iterationAxis: iterationAxis,
            maxPlotPoints: sessionParameterInt("msexplore", "max_points", instanceID: instanceID)
                .flatMap(UInt64.init) ?? 0,
            status: .idle,
            lastError: nil,
            result: nil
        )
    }

    private static func measurementSetPreset(profileValue: String) -> MeasurementSetExplorerPlotPreset? {
        let parts = profileValue.split(separator: "_")
        let swiftValue = parts.enumerated().map { index, part in
            index == 0 ? String(part) : part.prefix(1).uppercased() + part.dropFirst()
        }.joined()
        return MeasurementSetExplorerPlotPreset(rawValue: swiftValue)
    }

    private static func measurementSetColorAxis(profileValue: String?) -> MeasurementSetPlotColorAxis? {
        switch profileValue {
        case "none": MeasurementSetPlotColorAxis.none
        case "field": .field
        case "scan": .scan
        case "spw": .spectralWindow
        case "baseline": .baseline
        case "correlation": .correlation
        default: nil
        }
    }

    private static func measurementSetIterationAxis(profileValue: String?) -> MeasurementSetPlotIterationAxis? {
        switch profileValue {
        case "field": .field
        case "scan": .scan
        case "spw": .spectralWindow
        case "correlation": .correlation
        default: nil
        }
    }

    private static func clampedMovieFramesPerSecond(_ framesPerSecond: Double) -> Double {
        guard framesPerSecond.isFinite else {
            return 6.0
        }
        return min(max(framesPerSecond, 0.2), 60.0)
    }

    private static func formatImageExplorerClipValue(_ value: Double) -> String {
        String(format: "%.6g", value)
    }

    private func normalizedNonDisplayIndices(
        from indices: [Int],
        axes: [ImageExplorerNonDisplayAxis]
    ) -> [Int] {
        axes.enumerated().map { position, axis in
            indices[safe: position] ?? Int(axis.index)
        }
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
                maxPlotPoints: WorkbenchState.defaultMeasurementSetPlotMaxPoints,
                status: .idle,
                lastError: nil,
                result: nil
            )
        }
        let instanceID = parameterInstanceID(surfaceID: "msexplore", datasetID: datasetID)
        applyParameterContext(
            surfaceID: "msexplore",
            instanceID: instanceID,
            textValues: [
                "vis": dataset.path,
                "datacolumn": dataset.dataColumns.first ?? "DATA",
            ],
            preserveOverrides: true
        )
        let plotState = profiledMeasurementSetPlotState(datasetID: datasetID, instanceID: instanceID)
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
            "chan:\(plotState.selectedChannelSelection ?? "all")",
            "timerange:\(plotState.selectedTimerange ?? "all")",
            "uvrange:\(plotState.selectedUVRange ?? "all")",
            "antenna:\(plotState.selectedAntenna ?? "all")",
            "scan:\(plotState.selectedScan ?? "all")",
            "corr:\(plotState.selectedCorrelation ?? "all")",
            "array:\(plotState.selectedArray ?? "all")",
            "observation:\(plotState.selectedObservation ?? "all")",
            "intent:\(plotState.selectedIntent ?? "all")",
            "feed:\(plotState.selectedFeed ?? "all")",
            "msselect:\(plotState.selectedMSSelect ?? "all")",
            "data:\(plotState.dataColumn)",
            "colorBy:\(plotState.colorBy.protocolValue)",
            "avgchannel:\(plotState.avgChannel.map { String($0) } ?? "none")",
            "avgtime:\(plotState.avgTime.map { String($0) } ?? "none")",
            "avgscan:\(plotState.avgScan)",
            "avgfield:\(plotState.avgField)",
            "avgbaseline:\(plotState.avgBaseline)",
            "avgantenna:\(plotState.avgAntenna)",
            "avgspw:\(plotState.avgSPW)",
            "scalar:\(plotState.scalarAverage)",
            "iteraxis:\(plotState.iterationAxis?.protocolValue ?? "none")",
            "size:960x600",
            "maxPoints:\(plotState.maxPlotPoints)"
        ].joined(separator: "|")
    }

    private static func minimumBoundedMeasurementSetPlotMaxPoints(_ value: UInt64) -> UInt64 {
        max(WorkbenchState.minimumMeasurementSetPlotMaxPoints, value)
    }

    private func defaultImagerField(for dataset: DatasetSummary) -> String? {
        if isTWHyaTutorialDataset(dataset),
           let tutorialField = dataset.fields.first(where: { selectorToken($0) == "5" }) {
            return tutorialField
        }
        if dataset.name == "mssel_test_small_multifield_spw.ms",
           let sampleField = dataset.fields.first(where: { selectorToken($0) == "5" }) {
            return sampleField
        }
        if let scienceLikeField = dataset.fields.first(where: { field in
            let normalized = field.lowercased()
            return normalized.contains("ngc") || normalized.contains("target")
        }) {
            return scienceLikeField
        }
        return dataset.fields.first
    }

    private func defaultImagerSpectralWindow(for dataset: DatasetSummary) -> String? {
        if isTWHyaTutorialDataset(dataset),
           let tutorialSpectralWindow = dataset.spectralWindows.first(where: { selectorToken($0) == "0" }) {
            return tutorialSpectralWindow
        }
        if dataset.name == "mssel_test_small_multifield_spw.ms",
           let sampleSpectralWindow = dataset.spectralWindows.first(where: { selectorToken($0) == "5" }) {
            return sampleSpectralWindow
        }
        return dataset.spectralWindows.first(where: spectralWindowHasMultipleChannels) ?? dataset.spectralWindows.first
    }

    private func isTWHyaTutorialDataset(_ dataset: DatasetSummary) -> Bool {
        dataset.name.lowercased().contains("twhya_calibrated.ms")
    }

    private func defaultImagerCorrelation(for dataset: DatasetSummary) -> String? {
        let rawCorrelations = dataset.correlations.map { $0.uppercased() }
        if rawCorrelations.count == 1,
           ["XX", "YY", "RR", "LL"].contains(rawCorrelations[0]) {
            return rawCorrelations[0]
        }
        return nil
    }

    private func spectralWindowHasMultipleChannels(_ spectralWindow: String) -> Bool {
        guard let channelCount = spectralWindowChannelCount(spectralWindow) else {
            return false
        }
        return channelCount > 1
    }

    private func spectralWindowChannelCount(_ spectralWindow: String) -> Int? {
        guard let channelRange = spectralWindow.range(of: #"(\d+)\s+chan"#, options: .regularExpression) else {
            return nil
        }
        let token = spectralWindow[channelRange].split(separator: " ").first
        return token.flatMap { Int($0) }
    }

    private func defaultImagerOutputPrefix(for dataset: DatasetSummary) -> String {
        defaultImagerOutputPrefix(baseName: dataset.name)
    }

    private func defaultImagerOutputPrefix(baseName: String) -> String {
        let root = state.project.rootPath.isEmpty ? FileManager.default.currentDirectoryPath : state.project.rootPath
        let runDirectory = URL(fileURLWithPath: root)
            .appendingPathComponent("casa-rs-runs", isDirectory: true)
            .appendingPathComponent("imager-\(nextImagerRunIndex())", isDirectory: true)
        return runDirectory.appendingPathComponent("\(sanitizedPathComponent(baseName))-imager").path
    }

    private func nextImagerRunIndex() -> Int {
        state.history.filter { $0.title.hasPrefix("Imager") }.count + 1
    }

    private func sanitizedPathComponent(_ value: String) -> String {
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-_."))
        let scalars = value.unicodeScalars.map { allowed.contains($0) ? Character($0) : "-" }
        let sanitized = String(scalars).trimmingCharacters(in: CharacterSet(charactersIn: "-."))
        return sanitized.isEmpty ? "dataset" : sanitized
    }

    private func handleGenericTaskEvent(_ event: GenericTaskEvent, runID: String) {
        guard state.jobs[runID]?.status != .cancelled else {
            return
        }
        if case .progress(let progress) = event {
            if var job = state.jobs[runID] {
                job.status = .running
                job.progress = max(0.05, progress.workEstimate.fraction)
                job.lastEvent = progress.phase
                if job.logLines.last != progress.summary {
                    job.logLines.append(progress.summary)
                }
                state.jobs[runID] = job
            }
            if state.taskRun.runID == runID {
                state.taskRun.state = .running
                state.taskRun.progress = max(0.05, progress.workEstimate.fraction)
                state.taskRun.imagerProgress = progress
                if state.taskRun.logLines.last != progress.summary {
                    state.taskRun.logLines.append(progress.summary)
                }
            }
            return
        }
        activeTaskExecutions.removeValue(forKey: runID)
        let taskSucceeded: Bool
        switch event {
        case .succeeded:
            taskSucceeded = true
        case .failed, .cancelled:
            taskSucceeded = false
        case .progress:
            return
        }
        state.taskRun.warnings.append(contentsOf: taskParameterLifecycleClient.afterCompletion(
            attemptID: runID,
            successful: taskSucceeded
        ))
        if var job = state.jobs[runID] {
            if case .cancelled = event {
                job.progress = min(1, max(0, job.progress))
            } else {
                job.progress = 1.0
            }
            if state.activeJobIDsByTab[job.tabID] == runID {
                state.activeJobIDsByTab.removeValue(forKey: job.tabID)
            }
            switch event {
            case .progress:
                return
            case .succeeded(let result):
                job.status = .succeeded
                job.resultSummary = "\(result.taskID) completed"
                job.lastEvent = "succeeded"
                job.logLines.append("Arguments: \(result.arguments.joined(separator: " "))")
                if !result.stdout.isEmpty {
                    job.logLines.append(result.stdout)
                }
                if !result.stderr.isEmpty {
                    job.logLines.append(result.stderr)
                }
                state.jobs[runID] = job
                let completion = result.completion
                if state.taskRun.runID == runID {
                    let progressSnapshot = terminalImagerProgressSnapshot(
                        taskID: result.taskID,
                        runID: runID,
                        taskState: .succeeded,
                        progress: 1.0
                    )
                    state.taskRun = TaskRun(
                        runID: runID,
                        state: .succeeded,
                        progress: 1.0,
                        logLines: [
                            "\(result.taskID) completed.",
                            "Arguments: \(result.arguments.joined(separator: " "))",
                            completion.summary
                        ],
                        warnings: result.stderr.isEmpty ? [] : [result.stderr],
                        products: completion.products.map(\.path),
                        diagnostics: completion.diagnostics + completion.products.compactMap(\.diagnostic),
                        outputPaths: completion.products.map(\.path),
                        requestSummary: state.taskRun.requestSummary,
                        imagerProgress: progressSnapshot,
                        rawOutput: result.stdout
                    )
                }
                let products = ingestTaskProducts(completion.products)
                recordTaskProductGroup(
                    runID: runID,
                    taskID: result.taskID,
                    products: products,
                    diagnostics: completion.diagnostics + products.compactMap(\.diagnostic)
                )
                let affectedPaths = completion.products.filter(\.exists).map(\.path)
                state.history.append(ProcessingHistoryEvent(
                    id: "hist-run-\(state.history.count + 1)",
                    timestamp: currentTimestamp(),
                    title: "\(result.taskID) completed",
                    reason: state.taskRun.requestSummary ?? "User ran \(result.taskID).",
                    affectedPaths: affectedPaths,
                    approval: "user"
                ))
                finalizeNotebookTaskRecording(
                    runID: runID,
                    status: "succeeded",
                    affectedPaths: affectedPaths,
                    products: affectedPaths,
                    diagnostics: state.taskRun.diagnostics,
                    stdout: result.stdout,
                    stderr: result.stderr
                )
            case .failed(let failure):
                job.status = .failed
                job.error = failure.message
                job.lastEvent = "failed"
                job.logLines.append(failure.message)
                job.logLines.append(contentsOf: failure.diagnostics)
                state.jobs[runID] = job
                if state.taskRun.runID == runID {
                    let progressSnapshot = terminalImagerProgressSnapshot(
                        taskID: state.activeTaskID,
                        runID: runID,
                        taskState: .failed,
                        progress: state.taskRun.progress
                    )
                    state.taskRun.state = .failed
                    state.taskRun.progress = 1.0
                    state.taskRun.logLines.append(failure.message)
                    state.taskRun.diagnostics.append(contentsOf: failure.diagnostics)
                    state.taskRun.imagerProgress = progressSnapshot
                }
                state.lastErrors.append("Task failed: \(failure.message)")
                finalizeNotebookTaskRecording(
                    runID: runID,
                    status: "failed",
                    diagnostics: [failure.message] + failure.diagnostics,
                    stderr: failure.diagnostics.joined(separator: "\n")
                )
            case .cancelled(let failure):
                job.status = .cancelled
                job.error = failure.message
                job.lastEvent = "cancelled"
                job.cancellationRequested = true
                job.logLines.append(failure.message)
                state.jobs[runID] = job
                if state.taskRun.runID == runID {
                    let progressSnapshot = terminalImagerProgressSnapshot(
                        taskID: state.activeTaskID,
                        runID: runID,
                        taskState: .cancelled,
                        progress: state.taskRun.progress
                    )
                    state.taskRun.state = .cancelled
                    state.taskRun.progress = terminalTaskProgress(from: progressSnapshot)
                    state.taskRun.logLines.append(failure.message)
                    state.taskRun.diagnostics.append(contentsOf: failure.diagnostics)
                    state.taskRun.imagerProgress = progressSnapshot
                }
                finalizeNotebookTaskRecording(
                    runID: runID,
                    status: "cancelled",
                    diagnostics: [failure.message] + failure.diagnostics,
                    stderr: failure.diagnostics.joined(separator: "\n")
                )
            }
        }
    }

    private func recordTaskProductGroup(
        runID: String,
        taskID: String,
        products: [RunProductReference],
        diagnostics: [String]
    ) {
        guard !products.isEmpty else { return }
        let group = RunProductGroup(
            id: "products-\(runID)",
            runID: runID,
            title: "\(taskID) products",
            sourceDatasetID: state.selectedDatasetID ?? "",
            sourcePath: state.selectedDataset?.path ?? "",
            products: products,
            diagnostics: diagnostics
        )
        if let index = state.runProductGroups.firstIndex(where: { $0.runID == runID }) {
            state.runProductGroups[index] = group
        } else {
            state.runProductGroups.append(group)
        }
    }

    private func ingestTaskProducts(
        _ completionProducts: [CasarsFrontendServices.TaskCompletionProduct]
    ) -> [RunProductReference] {
        var products: [RunProductReference] = []
        for product in completionProducts {
            var datasetID = state.project.datasets.first(where: { $0.path == product.path })?.id
            if datasetID == nil, let probe = product.dataset {
                let dataset = DatasetSummary(probe: probe)
                state.project.datasets.append(dataset)
                datasetID = dataset.id
            }
            products.append(
                RunProductReference(
                    id: product.id,
                    artifactKind: String(describing: product.resourceKind),
                    label: product.label,
                    path: product.path,
                    datasetID: datasetID,
                    exists: product.exists,
                    previewPngPath: product.previewPath,
                    previewPngExists: product.previewExists,
                    diagnostic: product.diagnostic
                )
            )
        }
        return products
    }

    private func resolvedTaskPathString(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return path }
        let expanded = (trimmed as NSString).expandingTildeInPath
        if expanded.hasPrefix("/") {
            return URL(fileURLWithPath: expanded).standardizedFileURL.path
        }
        let root = state.project.rootPath.isEmpty
            ? FileManager.default.currentDirectoryPath
            : state.project.rootPath
        return URL(
            fileURLWithPath: expanded,
            relativeTo: URL(fileURLWithPath: root, isDirectory: true)
        )
        .standardizedFileURL
        .path
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

    private func normalizedTextSelection(_ value: String?) -> String? {
        guard let value = value?.trimmingCharacters(in: .whitespacesAndNewlines), !value.isEmpty, value != "all" else {
            return nil
        }
        return value
    }

    private func updateMeasurementSetPlotState(
        datasetID: String,
        update: (inout MeasurementSetExplorerPlotState) -> Void
    ) {
        var plotState = measurementSetPlotState(for: datasetID)
        update(&plotState)
        plotState.lastError = nil
        refreshMeasurementSetPlotStateFromCache(&plotState, datasetID: datasetID)
        state.measurementSetPlots[datasetID] = plotState
        syncMeasurementSetParameterSession(plotState, datasetID: datasetID)
    }

    private func syncMeasurementSetParameterSession(
        _ plotState: MeasurementSetExplorerPlotState,
        datasetID: String
    ) {
        guard runtimeKind == .production else { return }
        let instanceID = parameterInstanceID(surfaceID: "msexplore", datasetID: datasetID)
        loadParameterSessionIfNeeded("msexplore", instanceID: instanceID)
        let sessionKey = parameterSessionKey(surfaceID: "msexplore", instanceID: instanceID)
        guard var session = state.parameterSessions[sessionKey] else { return }
        let optional: (String?) -> SurfaceParameterValue = { .string($0 ?? "none") }
        let presetValue = Self.snakeCase(plotState.preset.rawValue)
        let values: [String: SurfaceParameterValue] = [
            "preset": .string(presetValue),
            "field": optional(plotState.selectedField),
            "spw": optional(spectralWindowSelectorToken(plotState)),
            "timerange": optional(plotState.selectedTimerange),
            "uvrange": optional(plotState.selectedUVRange),
            "antenna": optional(plotState.selectedAntenna),
            "scan": optional(plotState.selectedScan),
            "correlation": optional(plotState.selectedCorrelation),
            "array": optional(plotState.selectedArray),
            "observation": optional(plotState.selectedObservation),
            "intent": optional(plotState.selectedIntent),
            "feed": optional(plotState.selectedFeed),
            "msselect": optional(plotState.selectedMSSelect),
            "datacolumn": .string(plotState.dataColumn),
            "color_by": .string(plotState.colorBy.protocolValue),
            "avgchannel": plotState.avgChannel.map { .integer(Int64($0)) } ?? .string("none"),
            "avgtime": plotState.avgTime.map(SurfaceParameterValue.float) ?? .string("none"),
            "avgscan": .bool(plotState.avgScan),
            "avgfield": .bool(plotState.avgField),
            "avgbaseline": .bool(plotState.avgBaseline),
            "avgantenna": .bool(plotState.avgAntenna),
            "avgspw": .bool(plotState.avgSPW),
            "scalar": .bool(plotState.scalarAverage),
            "iteraxis": .string(plotState.iterationAxis?.protocolValue ?? "none"),
            "max_points": .integer(Int64(plotState.maxPlotPoints)),
        ]
        for (name, value) in values where session.bundle.concept(for: name) != nil {
            session.overridePatch.removeUnset(name)
            session.overridePatch.values[name] = value
        }
        resolveParameterSession(&session)
        state.parameterSessions[sessionKey] = session
    }

    private static func snakeCase(_ value: String) -> String {
        value.reduce(into: "") { result, character in
            if character.isUppercase {
                if !result.isEmpty { result.append("_") }
                result.append(contentsOf: character.lowercased())
            } else {
                result.append(character)
            }
        }
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

    private func spectralWindowSelectorToken(_ plotState: MeasurementSetExplorerPlotState) -> String? {
        guard let spectralWindow = spectralWindowSelectorToken(plotState.selectedSpectralWindow) else {
            return nil
        }
        guard let channelSelection = plotState.selectedChannelSelection else {
            return spectralWindow
        }
        return "\(spectralWindow):\(channelSelection)"
    }

    private func spectralWindowSelectorToken(_ value: String?) -> String? {
        guard let value = normalizedPickerValue(value) else {
            return nil
        }
        if value.hasPrefix("spw ") {
            let remainder = value.dropFirst(4)
            return String(remainder.prefix { $0.isNumber })
        }
        return value
    }
}

private extension Array {
    subscript(safe index: Int) -> Element? {
        guard indices.contains(index) else { return nil }
        return self[index]
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
            arrays: probe.arrays,
            observations: probe.observations,
            antennas: probe.antennas,
            intents: probe.intents,
            feeds: probe.feeds,
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
        case .antennaLayout:
            self = .antennaLayout
        case .scanTimeline:
            self = .scanTimeline
        case .spectralWindowCoverage:
            self = .spectralWindowCoverage
        case .phaseVsTime:
            self = .phaseVsTime
        case .amplitudePhaseVsTimeStacked:
            self = .amplitudePhaseVsTimeStacked
        case .weightVsTime:
            self = .weightVsTime
        case .sigmaVsTime:
            self = .sigmaVsTime
        case .flagVsTime:
            self = .flagVsTime
        case .weightSpectrumVsTime:
            self = .weightSpectrumVsTime
        case .sigmaSpectrumVsTime:
            self = .sigmaSpectrumVsTime
        case .flagRowVsTime:
            self = .flagRowVsTime
        case .elevationVsTime:
            self = .elevationVsTime
        case .azimuthVsTime:
            self = .azimuthVsTime
        case .hourAngleVsTime:
            self = .hourAngleVsTime
        case .parallacticAngleVsTime:
            self = .parallacticAngleVsTime
        case .azimuthVsElevation:
            self = .azimuthVsElevation
        case .amplitudeVsFrequency:
            self = .amplitudeVsFrequency
        case .amplitudeVsChannel:
            self = .amplitudeVsChannel
        case .phaseVsChannel:
            self = .phaseVsChannel
        case .phaseVsFrequency:
            self = .phaseVsFrequency
        case .amplitudeVsVelocity:
            self = .amplitudeVsVelocity
        case .phaseVsVelocity:
            self = .phaseVsVelocity
        case .amplitudeVsUvDistance:
            self = .amplitudeVsUvDistance
        case .amplitudeVsTime:
            self = .amplitudeVsTime
        case .realVsImaginary:
            self = .realVsImaginary
        }
    }
}

extension MeasurementSetExplorerPlotPreset {
    init(preset: CasarsFrontendServices.MeasurementSetPlotPreset) {
        switch preset {
        case .uvCoverage:
            self = .uvCoverage
        case .antennaLayout:
            self = .antennaLayout
        case .scanTimeline:
            self = .scanTimeline
        case .spectralWindowCoverage:
            self = .spectralWindowCoverage
        case .phaseVsTime:
            self = .phaseVsTime
        case .amplitudePhaseVsTimeStacked:
            self = .amplitudePhaseVsTimeStacked
        case .weightVsTime:
            self = .weightVsTime
        case .sigmaVsTime:
            self = .sigmaVsTime
        case .flagVsTime:
            self = .flagVsTime
        case .weightSpectrumVsTime:
            self = .weightSpectrumVsTime
        case .sigmaSpectrumVsTime:
            self = .sigmaSpectrumVsTime
        case .flagRowVsTime:
            self = .flagRowVsTime
        case .elevationVsTime:
            self = .elevationVsTime
        case .azimuthVsTime:
            self = .azimuthVsTime
        case .hourAngleVsTime:
            self = .hourAngleVsTime
        case .parallacticAngleVsTime:
            self = .parallacticAngleVsTime
        case .azimuthVsElevation:
            self = .azimuthVsElevation
        case .amplitudeVsFrequency:
            self = .amplitudeVsFrequency
        case .amplitudeVsChannel:
            self = .amplitudeVsChannel
        case .phaseVsChannel:
            self = .phaseVsChannel
        case .phaseVsFrequency:
            self = .phaseVsFrequency
        case .amplitudeVsVelocity:
            self = .amplitudeVsVelocity
        case .phaseVsVelocity:
            self = .phaseVsVelocity
        case .amplitudeVsUvDistance:
            self = .amplitudeVsUvDistance
        case .amplitudeVsTime:
            self = .amplitudeVsTime
        case .realVsImaginary:
            self = .realVsImaginary
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
            plotDocument: WorkbenchPlotDocument(payload: result.document),
            renderer: result.render.renderer,
            imageFormat: result.render.imageFormat,
            imageWidth: result.render.width,
            imageHeight: result.render.height,
            imageBytes: result.imageBytes
        )
    }
}

extension WorkbenchPlotDocument {
    init(
        payload: CasarsFrontendServices.PlotDocumentPayload,
        displayMode: WorkbenchPlotDisplayMode = .automatic
    ) {
        self.init(
            id: payload.id,
            title: payload.title,
            subtitle: payload.subtitle,
            headerLines: payload.headerLines,
            axes: payload.axes.map(WorkbenchPlotAxis.init(payload:)),
            layers: payload.layers.enumerated().map { index, layer in
                WorkbenchPlotLayer(payload: layer, paletteIndex: index)
            },
            annotations: payload.annotations.map(WorkbenchPlotAnnotation.init(payload:)),
            panels: payload.panels.map(WorkbenchPlotPanel.init(payload:)),
            showLegend: payload.showLegend,
            displayMode: displayMode
        )
    }
}

extension WorkbenchPlotPanel {
    init(payload: CasarsFrontendServices.PlotDocumentPanel) {
        self.init(
            id: payload.id,
            title: payload.title,
            axes: payload.axes.map(WorkbenchPlotAxis.init(payload:)),
            layers: payload.layers.enumerated().map { index, layer in
                WorkbenchPlotLayer(payload: layer, paletteIndex: index)
            },
            annotations: payload.annotations.map(WorkbenchPlotAnnotation.init(payload:))
        )
    }
}

extension WorkbenchPlotAxis {
    init(payload: CasarsFrontendServices.PlotDocumentAxis) {
        self.init(
            id: payload.id,
            label: payload.label,
            unit: payload.unit,
            range: WorkbenchPlotRange(lower: payload.lower, upper: payload.upper),
            scale: WorkbenchPlotAxisScale(scale: payload.scale),
            laneLabels: payload.laneLabels,
            drawsOnTrailingEdge: payload.drawsOnTrailingEdge
        )
    }
}

extension WorkbenchPlotLayer {
    init(payload: CasarsFrontendServices.PlotDocumentLayer, paletteIndex: Int) {
        let pointCount = min(payload.xValues.count, payload.yValues.count)
        let provenance = payload.provenance.map(WorkbenchPlotPointProvenance.init(payload:))
        let layerKind = WorkbenchPlotLayerKind(kind: payload.kind)
        let inlinePointThreshold = layerKind == .line ? 50_000 : denseScatterPointThreshold
        let points = pointCount <= inlinePointThreshold
            ? (0..<pointCount).map { index in
                WorkbenchPlotPoint(
                    x: payload.xValues[index],
                    y: payload.yValues[index],
                    label: index < payload.pointLabels.count && !payload.pointLabels[index].isEmpty ? payload.pointLabels[index] : nil,
                    symbolSize: index < payload.pointSymbolSizes.count ? payload.pointSymbolSizes[index] : nil,
                    provenance: index < provenance.count ? provenance[index] : nil
                )
            }
            : []
        let pointCloud = pointCount > inlinePointThreshold
            ? WorkbenchPlotPointCloud(
                xValues: Array(payload.xValues.prefix(pointCount)),
                yValues: Array(payload.yValues.prefix(pointCount)),
                provenanceSamples: provenance
            )
            : nil
        let intervalCount = min(
            payload.intervalXStart.count,
            payload.intervalXEnd.count,
            payload.intervalY.count,
            payload.intervalHeight.count
        )
        let intervals = (0..<intervalCount).map { index in
            WorkbenchPlotInterval(
                id: "\(payload.id)-interval-\(index)",
                xStart: payload.intervalXStart[index],
                xEnd: payload.intervalXEnd[index],
                y: payload.intervalY[index],
                height: payload.intervalHeight[index],
                label: index < payload.intervalLabels.count && !payload.intervalLabels[index].isEmpty ? payload.intervalLabels[index] : nil
            )
        }
        self.init(
            id: payload.id,
            title: payload.title,
            kind: layerKind,
            xAxisID: payload.xAxisId,
            yAxisID: payload.yAxisId,
            points: points,
            intervals: intervals,
            pointCloud: pointCloud,
            style: WorkbenchPlotLayerStyle(
                colorHex: WorkbenchPlotLayerStyle.colorHex(for: payload.colorGroup, paletteIndex: paletteIndex),
                symbolSize: payload.symbolSize,
                lineWidth: payload.lineWidth,
                opacity: payload.opacity
            ),
            provenanceSummary: payload.provenanceSummary,
            dataProfile: WorkbenchPlotLayerDataProfile(
                sourceSampleCount: payload.sourceSampleCount,
                displaySampleCount: max(points.count, pointCloud?.count ?? 0, intervals.count),
                pointBudget: layerKind == .line ? 100_000 : denseScatterPointThreshold,
                strategy: WorkbenchPlotPayloadStrategy(payloadStrategy: payload.payloadStrategy),
                sourceDescription: payload.provenanceSummary,
                provenanceKey: payload.colorGroup
            )
        )
    }
}

private func formatDuration(_ seconds: TimeInterval) -> String {
    if seconds < 1.0 {
        return "\(Int((seconds * 1_000).rounded())) ms"
    }
    return String(format: "%.2f s", seconds)
}

extension WorkbenchPlotAnnotation {
    init(payload: CasarsFrontendServices.PlotDocumentAnnotation) {
        self.init(id: payload.id, x: payload.x, y: payload.y, text: payload.text)
    }
}

extension WorkbenchPlotPointProvenance {
    init(payload: CasarsFrontendServices.PlotPointProvenance) {
        self.init(
            row: payload.row,
            source: "row \(payload.row), corr \(payload.corr), chan \(payload.chanStart)..<\(payload.chanEnd)"
        )
    }
}

extension WorkbenchPlotAxisScale {
    init(scale: CasarsFrontendServices.PlotAxisScale) {
        switch scale {
        case .linear:
            self = .linear
        case .log:
            self = .logarithmic
        }
    }
}

extension WorkbenchPlotLayerKind {
    init(kind: CasarsFrontendServices.PlotLayerKind) {
        switch kind {
        case .scatter:
            self = .scatter
        case .line:
            self = .line
        case .interval:
            self = .interval
        }
    }
}

extension WorkbenchPlotPayloadStrategy {
    init(payloadStrategy: CasarsFrontendServices.PlotPayloadStrategy) {
        switch payloadStrategy {
        case .pointCloud:
            self = .viewportLevelOfDetail
        case .intervals:
            self = .inlineDisplayPoints
        }
    }
}

extension WorkbenchPlotLayerStyle {
    static func colorHex(for colorGroup: String, paletteIndex: Int) -> String {
        let palette = ["#2563eb", "#16a34a", "#dc2626", "#9333ea", "#ea580c", "#0891b2", "#7c3aed", "#0f766e"]
        var hash = 5381
        for scalar in colorGroup.unicodeScalars {
            hash = ((hash << 5) &+ hash) &+ Int(scalar.value)
        }
        return palette[abs(hash &+ paletteIndex) % palette.count]
    }
}
