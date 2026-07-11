import Foundation
import CasarsFrontendServices
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

public protocol TaskCatalogClient {
    func loadTaskCatalog() throws -> [TaskCatalogEntry]
}

public struct UniFFITaskCatalogClient: TaskCatalogClient {
    public init() {}

    public func loadTaskCatalog() throws -> [TaskCatalogEntry] {
        let json = try CasarsFrontendServices.taskCatalogJson()
        let data = Data(json.utf8)
        let envelope = try JSONDecoder().decode(TaskCatalogEnvelope.self, from: data)
        return envelope.tasks.filter(\.showInSwift)
    }
}

public protocol TaskExecutionMatrixClient {
    func loadTaskExecutionMatrix() throws -> TaskExecutionMatrixEnvelope
}

public struct UniFFITaskExecutionMatrixClient: TaskExecutionMatrixClient {
    public init() {}

    public func loadTaskExecutionMatrix() throws -> TaskExecutionMatrixEnvelope {
        let json = try CasarsFrontendServices.taskExecutionMatrixJson()
        let data = Data(json.utf8)
        return try JSONDecoder().decode(TaskExecutionMatrixEnvelope.self, from: data)
    }
}

/// Bootstrap adapters used before the notebook prototype is visible. They
/// deliberately expose no production catalog or execution metadata.
private struct PrototypeTaskCatalogClient: TaskCatalogClient {
    func loadTaskCatalog() throws -> [TaskCatalogEntry] { [] }
}

private struct PrototypeTaskExecutionMatrixClient: TaskExecutionMatrixClient {
    func loadTaskExecutionMatrix() throws -> TaskExecutionMatrixEnvelope {
        TaskExecutionMatrixEnvelope(
            schemaVersion: 1,
            generatedFor: "notebook-prototype",
            scopeNote: "fixture-only",
            rows: []
        )
    }
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

    func loadTaskUISchema(taskID: String) throws -> TaskUISchema {
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

    func writeLast(
        surfaceID: String,
        workspace: String,
        values: [String: SurfaceParameterValue],
        successful: Bool
    ) throws -> SurfaceParameterWriteResult {
        try denied("parameter history write")
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
        let json = try CasarsFrontendServices.taskContextOptionsJson(datasetPath: datasetPath)
        let data = Data(json.utf8)
        return try JSONDecoder().decode(TaskContextOptionsEnvelope.self, from: data)
    }
}

public protocol TaskUISchemaClient {
    func loadTaskUISchema(taskID: String) throws -> TaskUISchema
}

public struct UniFFITaskUISchemaClient: TaskUISchemaClient {
    public init() {}

    public func loadTaskUISchema(taskID: String) throws -> TaskUISchema {
        let json = try CasarsFrontendServices.taskUiSchemaJson(taskId: taskID)
        let data = Data(json.utf8)
        return try JSONDecoder().decode(TaskUISchema.self, from: data)
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
        let requestData = try JSONEncoder().encode(request)
        let requestJSON = String(decoding: requestData, as: UTF8.self)
        let json = try CasarsFrontendServices.buildImageExplorerSnapshotFromRequestJson(requestJson: requestJSON)
        return try JSONDecoder().decode(ImageExplorerSnapshot.self, from: Data(json.utf8))
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
        let requestData = try JSONEncoder().encode(request)
        let requestJSON = String(decoding: requestData, as: UTF8.self)
        let json = try CasarsFrontendServices.buildTableBrowserSnapshotFromRequestJson(requestJson: requestJSON)
        return try JSONDecoder().decode(TableBrowserSnapshot.self, from: Data(json.utf8))
    }

    public func buildCellWindow(request: TableBrowserCellWindowRequest) throws -> TableBrowserCellWindowSnapshot {
        let requestData = try JSONEncoder().encode(request)
        let requestJSON = String(decoding: requestData, as: UTF8.self)
        let json = try CasarsFrontendServices.buildTableBrowserCellWindowJson(requestJson: requestJSON)
        return try JSONDecoder().decode(TableBrowserCellWindowSnapshot.self, from: Data(json.utf8))
    }

    public func buildCellValue(request: TableBrowserCellValueRequest) throws -> String {
        let requestData = try JSONEncoder().encode(request)
        let requestJSON = String(decoding: requestData, as: UTF8.self)
        let json = try CasarsFrontendServices.buildTableBrowserCellValueJson(requestJson: requestJSON)
        return try JSONDecoder().decode(String.self, from: Data(json.utf8))
    }
}

private struct TaskParameterAttempt {
    var surfaceID: String
    var workspace: String
    var values: [String: SurfaceParameterValue]
    var saveLast: Bool
}

private struct SessionLastDestination: Hashable {
    var surfaceID: String
    var workspace: String

    init(surfaceID: String, workspace: String) {
        self.surfaceID = surfaceID
        let expanded = (workspace as NSString).expandingTildeInPath
        self.workspace = URL(fileURLWithPath: expanded, isDirectory: true)
            .standardizedFileURL
            .resolvingSymlinksInPath()
            .path
    }
}

private enum WorkbenchRuntimeKind {
    case production
    case notebookPrototype
    case pythonPrototype
}

public final class WorkbenchStore: ObservableObject {
    @Published public private(set) var state: WorkbenchState
    private let runtimeKind: WorkbenchRuntimeKind
    private let probeClient: ProjectProbeClient
    private let demoProjectClient: DemoProjectClient
    private let plotClient: MeasurementSetPlotClient
    private let imageExplorerClient: ImageExplorerClient
    private let tableBrowserClient: TableBrowserClient
    private let genericTaskClient: GenericTaskClient
    private let taskUISchemaClient: TaskUISchemaClient
    private let surfaceParameterClient: SurfaceParameterClient
    private let taskExecutionMatrixClient: TaskExecutionMatrixClient
    private var notebookPersistenceClient: NotebookPersistenceClient
    private let imagerProgressSource: ImagerProgressSource
    private let plotQueue = DispatchQueue(label: "casars.mac.ms-plot-job", qos: .userInitiated, attributes: .concurrent)
    private let tableBrowserQueue = DispatchQueue(label: "casars.mac.tablebrowser-cell-window", qos: .userInitiated)
    private var activeTaskExecutions: [String: TaskExecution] = [:]
    private var taskParameterAttempts: [String: TaskParameterAttempt] = [:]
    private var notebookAttemptHandles: [String: NotebookAttemptHandle] = [:]
    private var measurementSetParameterAttempts: [String: TaskParameterAttempt] = [:]
    private var acceptedSessionParameterValues: [String: [String: SurfaceParameterValue]] = [:]
    private var acceptedSessionParameterSequence: [String: UInt64] = [:]
    private var nextSessionParameterSequence: UInt64 = 0
    private var sessionLastValues: [SessionLastDestination: [String: SurfaceParameterValue]] = [:]
    private var sessionLastSequence: [SessionLastDestination: UInt64] = [:]
    private var sessionLastWrites: [String: DispatchWorkItem] = [:]
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
        taskCatalogClient: TaskCatalogClient = UniFFITaskCatalogClient(),
        taskUISchemaClient: TaskUISchemaClient = UniFFITaskUISchemaClient(),
        surfaceParameterClient: SurfaceParameterClient = UniFFISurfaceParameterClient(),
        taskExecutionMatrixClient: TaskExecutionMatrixClient = UniFFITaskExecutionMatrixClient(),
        imagerProgressSource: ImagerProgressSource = EmptyImagerProgressSource()
    ) {
        var initialState = state
        if initialState.taskCatalog.isEmpty {
            do {
                initialState.taskCatalog = try taskCatalogClient.loadTaskCatalog()
            } catch {
                initialState.lastErrors.append("Load task catalog: \(error)")
            }
        }
        if initialState.taskExecutionMatrixRows.isEmpty {
            do {
                initialState.taskExecutionMatrixRows = try taskExecutionMatrixClient.loadTaskExecutionMatrix().rows
            } catch {
                initialState.lastErrors.append("Load task execution matrix: \(error)")
            }
        }
        self.state = initialState
        if initialState.isNotebookPrototype {
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
        self.taskExecutionMatrixClient = taskExecutionMatrixClient
        notebookPersistenceClient = UniFFINotebookPersistenceClient()
        self.imagerProgressSource = imagerProgressSource
    }

    package func installNotebookPersistenceClientForTesting(_ client: NotebookPersistenceClient) {
        notebookPersistenceClient = client
    }

    deinit {
        guard runtimeKind == .production else { return }
        sessionLastWrites.values.forEach { $0.cancel() }
        for sessionKey in acceptedSessionParameterValues.keys {
            persistSessionLastIfChanged(sessionKey: sessionKey)
        }
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
            taskCatalogClient: PrototypeTaskCatalogClient(),
            taskUISchemaClient: dependencies.taskUISchemaClient,
            surfaceParameterClient: dependencies.surfaceParameterClient,
            taskExecutionMatrixClient: PrototypeTaskExecutionMatrixClient(),
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
            taskCatalogClient: PrototypeTaskCatalogClient(),
            taskUISchemaClient: dependencies.taskUISchemaClient,
            surfaceParameterClient: dependencies.surfaceParameterClient,
            taskExecutionMatrixClient: PrototypeTaskExecutionMatrixClient(),
            imagerProgressSource: EmptyImagerProgressSource()
        )
    }

    package var isNotebookPrototypeRuntime: Bool {
        runtimeKind == .notebookPrototype
    }

    package var isPythonPrototypeRuntime: Bool {
        runtimeKind == .pythonPrototype
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

    public func openFixtureProject() {
        guard !rejectPrototypeProductionAction("Demo projects") else { return }
        let interfaceFontSize = state.interfaceFontSize
        let taskCatalog = state.taskCatalog
        cleanupTemporaryDemoProject()
        do {
            let probed = try demoProjectClient.createDemoProject()
            temporaryDemoProjectRoot = probed.project.rootPath
            var project = probed.project
            project.datasets = orderedDemoDatasets(project.datasets)
            state = EmptyWorkbench.makeState(interfaceFontSize: interfaceFontSize)
            state.taskCatalog = taskCatalog
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
            state.taskCatalog = taskCatalog
            state.lastErrors.append("Open tutorial demo project: \(error)")
        }
    }

    public func openProject(path: String) {
        guard !rejectPrototypeProductionAction("Project opening") else { return }
        let interfaceFontSize = state.interfaceFontSize
        let taskCatalog = state.taskCatalog
        cleanupTemporaryDemoProject()
        do {
            let probed = try probeClient.probeProject(path: path)
            state = EmptyWorkbench.makeState(interfaceFontSize: interfaceFontSize)
            state.taskCatalog = taskCatalog
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
        let taskCatalog = state.taskCatalog
        cleanupTemporaryDemoProject()
        let standardizedPath = Self.standardizedDatasetPath(path)
        let url = URL(fileURLWithPath: standardizedPath)
        let rootPath = url.deletingLastPathComponent().path
        var dataset = DatasetSummary(
            id: standardizedPath,
            name: url.lastPathComponent,
            path: standardizedPath,
            kind: .measurementSet,
            size: "external MeasurementSet",
            units: "Jy, Hz, seconds",
            columns: ["UVW", "DATA", "FLAG"],
            dataColumns: ["DATA"],
            notes: "Opened directly as an imager input without probing the full project tree.",
            diagnostics: [
                "Project tree probe skipped for launch; task request uses exact-path metadata when available."
            ]
        )
        do {
            if var probed = try probeClient.probePath(path: standardizedPath),
               probed.kind == .measurementSet {
                probed.notes += " Opened directly as an imager input; parent project probe skipped."
                probed.diagnostics.append(
                    "Direct launch used exact MeasurementSet probe only; parent folder refresh is disabled."
                )
                dataset = probed
            } else {
                dataset.diagnostics.append(
                    "Exact MeasurementSet probe did not recognize the path; using direct-launch placeholder metadata."
                )
            }
        } catch {
            dataset.diagnostics.append(
                "Exact MeasurementSet probe failed; using direct-launch placeholder metadata: \(error)"
            )
        }
        state = EmptyWorkbench.makeState(interfaceFontSize: interfaceFontSize)
        state.taskCatalog = taskCatalog
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

    public func openTutorialPack(path: String) {
        guard !rejectPrototypeProductionAction("Tutorial packs") else { return }
        let interfaceFontSize = state.interfaceFontSize
        let taskCatalog = state.taskCatalog
        cleanupTemporaryDemoProject()
        do {
            let context = try TutorialPackContext.load(path: path)
            let tutorialDatasets = tutorialPackDatasetSummaries(context: context)
            state = EmptyWorkbench.makeState(interfaceFontSize: interfaceFontSize)
            state.taskCatalog = taskCatalog
            state.project = ProjectFixture(
                name: context.title,
                rootPath: context.rootPath,
                datasets: tutorialDatasets.datasets,
                source: .tutorialPack
            )
            state.tutorialPack = context
            state.probeDiagnostics = tutorialDatasets.diagnostics
            state.selectedDatasetID = state.project.datasets.first?.id
            state.dockMode = .datasets
            state.leftDockCollapsed = false
            state.inspectorCollapsed = false
            openTab(
                WorkbenchTab(
                    id: "tab-tutorial-pack",
                    title: "Tutorial",
                    kind: .tutorial
                )
            )
            state.history.append(
                ProcessingHistoryEvent(
                    id: "hist-tutorial-pack-open",
                    timestamp: "loaded",
                    title: "Tutorial pack opened",
                    reason: "Loaded \(context.tutorialID) from pack.json without creating a durable project history.",
                    affectedPaths: [context.manifestPath],
                    approval: "user"
                )
            )
        } catch {
            state.lastErrors.append("Open tutorial pack \(path): \(error)")
        }
    }

    public func refreshProjectFromDiskIfNeeded(now: Date = Date()) {
        // The UI timer keeps firing in prototype review sessions. Ignore it
        // silently so it cannot touch disk or flood the fixture error log.
        guard runtimeKind == .production else { return }
        guard state.hasProject, !state.project.rootPath.isEmpty else {
            return
        }
        guard state.project.source == .probed || state.tutorialPack != nil else {
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
        guard state.project.source == .probed || state.tutorialPack != nil else {
            return
        }
        let selectedPath = state.selectedDataset?.path
        do {
            let refreshed: (datasets: [DatasetSummary], diagnostics: [String])
            if let context = state.tutorialPack {
                refreshed = tutorialPackDatasetSummaries(context: context)
            } else {
                let probe = try probeClient.probeProject(path: state.project.rootPath)
                refreshed = (
                    datasets: projectDatasetsWithLooseFiles(
                        recognizedDatasets: probe.project.datasets,
                        rootPath: state.project.rootPath
                    ),
                    diagnostics: probe.diagnostics
                )
            }
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

    private func tutorialPackDatasetSummaries(
        context: TutorialPackContext
    ) -> (datasets: [DatasetSummary], diagnostics: [String]) {
        let manifestDatasets = context.datasetSummaries()
        var diagnostics = context.inputs.map { input in
            "Tutorial input \(input.filename): \(input.status.rawValue)"
        }
        do {
            let probe = try probeClient.probeProject(path: context.rootPath)
            diagnostics.append(contentsOf: probe.diagnostics)
            let probedByPath = Dictionary(uniqueKeysWithValues: probe.project.datasets.map { dataset in
                (Self.standardizedDatasetPath(dataset.path), dataset)
            })
            var datasets = manifestDatasets.map { dataset in
                let standardizedPath = Self.standardizedDatasetPath(dataset.path)
                guard var enriched = probedByPath[standardizedPath] else {
                    guard context.inputs.contains(where: {
                        $0.status == .staged && Self.standardizedDatasetPath($0.resolvedPath) == standardizedPath
                    }) else {
                        return dataset
                    }
                    var unrecognized = dataset
                    unrecognized.diagnostics.append(
                        "Image validation failed: cannot open or read CASA image '\(dataset.path)' as a valid casacore image."
                    )
                    return unrecognized
                }
                enriched.notes = "\(dataset.notes)\n\(enriched.notes)"
                enriched.diagnostics = dataset.diagnostics + enriched.diagnostics
                return enriched
            }
            datasets.append(contentsOf: tutorialPackRegionDatasetSummaries(context: context))
            datasets = projectDatasetsWithLooseFiles(
                recognizedDatasets: datasets,
                rootPath: context.rootPath
            )
            return (deduplicatedDatasets(datasets), diagnostics)
        } catch {
            diagnostics.append("Tutorial input metadata probe failed: \(error)")
            return (
                deduplicatedDatasets(
                    projectDatasetsWithLooseFiles(
                        recognizedDatasets: manifestDatasets + tutorialPackRegionDatasetSummaries(context: context),
                        rootPath: context.rootPath
                    )
                ),
                diagnostics
            )
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

    private func tutorialPackRegionDatasetSummaries(context: TutorialPackContext) -> [DatasetSummary] {
        let regionDirectories = [
            URL(fileURLWithPath: context.rootPath, isDirectory: true)
                .standardizedFileURL,
            URL(fileURLWithPath: context.rootPath, isDirectory: true)
                .appendingPathComponent("regions", isDirectory: true),
            URL(fileURLWithPath: context.nativeWorkspacePath, isDirectory: true)
                .standardizedFileURL,
            URL(fileURLWithPath: context.nativeWorkspacePath, isDirectory: true)
                .appendingPathComponent("regions", isDirectory: true),
        ].map(\.standardizedFileURL)
        var files: [URL] = []
        var preferredNames = Set<String>()
        for (index, regionsURL) in regionDirectories.enumerated() {
            let directoryFiles = ((try? FileManager.default.contentsOfDirectory(
                at: regionsURL,
                includingPropertiesForKeys: [.fileSizeKey],
                options: [.skipsHiddenFiles]
            )) ?? [])
                .filter { $0.pathExtension == "crtf" }
            if index == 0 {
                preferredNames.formUnion(directoryFiles.map(\.lastPathComponent))
                files.append(contentsOf: directoryFiles)
            } else {
                files.append(contentsOf: directoryFiles.filter { !preferredNames.contains($0.lastPathComponent) })
            }
        }
        return files
            .sorted { $0.lastPathComponent.localizedStandardCompare($1.lastPathComponent) == .orderedAscending }
            .map { file in
                let path = Self.standardizedDatasetPath(file.path)
                let size = (try? file.resourceValues(forKeys: [.fileSizeKey]).fileSize).map(UInt64.init) ?? 0
                return DatasetSummary(
                    id: path,
                    name: file.lastPathComponent,
                    path: path,
                    kind: .region,
                    size: "region file",
                    units: "pixels",
                    sizeBytes: size,
                    notes: "Tutorial workspace region file.",
                    diagnostics: [
                        "Region parameter syntax: --region \(projectRelativePath(path))",
                        "Inline region syntax: box[[x0pix,y0pix],[x1pix,y1pix]] or world-coordinate CRTF"
                    ]
                )
            }
    }

    private static func standardizedDatasetPath(_ path: String) -> String {
        URL(fileURLWithPath: path).standardizedFileURL.path
    }

    private func fileSize(path: String) -> UInt64 {
        let attributes = try? FileManager.default.attributesOfItem(atPath: path)
        return (attributes?[.size] as? NSNumber)?.uint64Value ?? 0
    }

    public func selectTutorialSection(_ sectionID: String) {
        guard !rejectPrototypeProductionAction("Tutorial navigation") else { return }
        guard var context = state.tutorialPack else {
            state.lastErrors.append("No tutorial pack is open")
            return
        }
        guard context.sections.contains(where: { $0.id == sectionID }) else {
            state.lastErrors.append("Unknown tutorial section \(sectionID)")
            return
        }
        context.selectedSectionID = sectionID
        state.tutorialPack = context
        selectFirstTutorialInputDataset(context.selectedSection, context: context)
    }

    private func selectFirstTutorialInputDataset(
        _ section: TutorialPackSection?,
        context: TutorialPackContext
    ) {
        guard let inputRef = section?.inputRefs.first,
              let input = context.inputs.first(where: { $0.id == inputRef })
        else {
            return
        }
        let inputPath = Self.standardizedDatasetPath(input.resolvedPath)
        if let dataset = state.project.datasets.first(where: { Self.standardizedDatasetPath($0.path) == inputPath }) {
            state.selectedDatasetID = dataset.id
        }
    }

    public func openTutorialSectionTask(_ sectionID: String) {
        guard !rejectPrototypeProductionAction("Tutorial task navigation") else { return }
        selectTutorialSection(sectionID)
        guard let context = state.tutorialPack,
              let section = context.selectedSection
        else {
            return
        }
        guard let guiStep = section.steps.first(where: { $0.surface == "gui" && $0.providerKind == "native-rust" }) else {
            state.lastErrors.append("Tutorial section \(sectionID) does not define a native GUI step")
            return
        }
        applyTutorialPackParameters(guiStep.parameters, taskID: guiStep.taskID, packRoot: context.rootPath)
        if openTutorialExplorerTask(guiStep.taskID) {
            return
        }
        selectTask(guiStep.taskID)
        let tabID = nextTaskTabID()
        openTab(
            WorkbenchTab(
                id: tabID,
                title: taskTitle(guiStep.taskID),
                kind: .task,
                datasetID: state.selectedDatasetID,
                taskID: guiStep.taskID
            )
        )
    }

    private func openTutorialExplorerTask(_ taskID: String) -> Bool {
        switch taskID {
        case "msexplore":
            if let dataset = selectedOrFirstDataset(kind: .measurementSet) {
                openDatasetExplorer(dataset.id)
            } else {
                state.lastErrors.append("Tutorial msexplore step has no MeasurementSet dataset")
            }
            return true
        case "imexplore":
            if let dataset = selectedOrFirstDataset(kind: .imageCube) {
                openDatasetExplorer(dataset.id)
            } else {
                state.lastErrors.append("Tutorial imexplore step has no image dataset")
            }
            return true
        case "tablebrowser":
            if let selected = state.selectedDataset, canBrowseAsTable(selected) {
                openDatasetTableBrowser(selected.id)
            } else if let dataset = state.project.datasets.first(where: canBrowseAsTable) {
                openDatasetTableBrowser(dataset.id)
            } else {
                state.lastErrors.append("Tutorial tablebrowser step has no casacore table dataset")
            }
            return true
        default:
            return false
        }
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
        guard let group = state.runProductGroups.first(where: { $0.runID == runID }) else {
            state.lastErrors.append("Unknown run \(runID)")
            return
        }
        guard let product = group.products.first(where: { $0.id == productID }) else {
            state.lastErrors.append("Unknown product \(productID)")
            return
        }
        guard let datasetID = product.datasetID else {
            state.lastErrors.append("Product \(product.label) is not a recognized dataset")
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
        let parameterAttempt = parameterSession(surfaceID: "msexplore", instanceID: instanceID).map {
            TaskParameterAttempt(
                surfaceID: "msexplore",
                workspace: $0.workspace,
                values: $0.values,
                saveLast: $0.saveLast
            )
        }
        if let parameterAttempt, parameterAttempt.saveLast {
            do {
                _ = try surfaceParameterClient.writeLast(
                    surfaceID: parameterAttempt.surfaceID,
                    workspace: parameterAttempt.workspace,
                    values: parameterAttempt.values,
                    successful: false
                )
            } catch {
                state.lastErrors.append("Automatic msexplore Last save failed: \(error)")
            }
        }
        if let cached = cachedMeasurementSetPlotResult(for: dataset, plotState: plotState) {
            plotState.result = cached
            plotState.status = .ready
            plotState.lastError = nil
            state.measurementSetPlots[datasetID] = plotState
            if let parameterAttempt, parameterAttempt.saveLast {
                do {
                    _ = try surfaceParameterClient.writeLast(
                        surfaceID: parameterAttempt.surfaceID,
                        workspace: parameterAttempt.workspace,
                        values: parameterAttempt.values,
                        successful: true
                    )
                } catch {
                    state.lastErrors.append("Automatic msexplore Last Successful save failed: \(error)")
                }
            }
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
        let jobID = nextJobID(prefix: "ms-plot")
        if let parameterAttempt {
            measurementSetParameterAttempts[jobID] = parameterAttempt
        }
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
        let prototypeName = runtimeKind == .notebookPrototype ? "notebook" : "Python"
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

        let closingSessionKeys = state.parameterSessions.keys.filter { $0.hasPrefix("\(tabID)::") }
        for sessionKey in closingSessionKeys {
            sessionLastWrites[sessionKey]?.cancel()
            persistSessionLastIfChanged(sessionKey: sessionKey)
            sessionLastWrites.removeValue(forKey: sessionKey)
            acceptedSessionParameterValues.removeValue(forKey: sessionKey)
            acceptedSessionParameterSequence.removeValue(forKey: sessionKey)
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
            guard state.tutorialPack != nil else {
                state.lastErrors.append("No tutorial pack is open")
                return
            }
            openTab(WorkbenchTab(id: "tab-tutorial-pack", title: "Tutorial", kind: .tutorial))
        case .task:
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
            guard !rejectPrototypeProductionAction("AI chat") else { return }
            guard state.isDemoProject else {
                state.lastErrors.append("AI chat is not connected yet")
                return
            }
            openTab(WorkbenchTab(id: "tab-ai", title: "AI Chat", kind: .aiChat))
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
        } catch {
            state.lastErrors.append("Load project notebooks: \(error)")
        }
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
        if let tabIndex = state.tabs.firstIndex(where: { $0.kind == .notebook }) {
            state.tabs[tabIndex].title = project.activeNotebook?.filename ?? "Notebook"
            state.activeTabID = state.tabs[tabIndex].id
        } else {
            openDefaultTab(kind: .notebook)
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
        guard state.taskCatalog.contains(where: { $0.id == intent.surface }) else {
            state.lastErrors.append("Notebook task \(intent.surface) is not in the current task catalog")
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
        guard runtimeKind == .notebookPrototype else { return }
        updateActivePrototypeNotebook { document in
            document.draftMarkdown = markdown
            PrototypeScientificNotebookFixtureAdapter.synchronizeTaskCells(in: &document)
        }
    }

    package func setPrototypeNotebookViewMode(_ viewMode: PrototypeNotebookViewMode) {
        guard runtimeKind == .notebookPrototype else { return }
        updateActivePrototypeNotebook { $0.viewMode = viewMode }
    }

    package func savePrototypeNotebookDraft() {
        guard runtimeKind == .notebookPrototype else { return }
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
        guard runtimeKind == .notebookPrototype else { return }
        updateActivePrototypeNotebook { document in
            guard document.tasks.contains(where: { $0.id == receiptID }) else { return }
            document.selectedReceiptID = receiptID
        }
    }

    /// Opens an interactive task-shaped tab using only the fixture projection.
    /// No provider schema, parameter, dataset, or task adapter is consulted.
    package func openPrototypeNotebookTask(receiptID: String) {
        guard runtimeKind == .notebookPrototype,
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
        explorerState.cursorX = snapshot.planeCursor?.pixelX ?? explorerState.cursorX
        explorerState.cursorY = snapshot.planeCursor?.pixelY ?? explorerState.cursorY
        explorerState.nonDisplayIndices = snapshot.nonDisplayAxes?.map(\.index) ?? explorerState.nonDisplayIndices
        if let parameters = snapshot.parameters {
            explorerState.parameters = parameters
        }
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

    public func setImageExplorerParameters(_ parameters: ImageExplorerParameters, datasetID: String) {
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
        setImageExplorerParameters(parameters, datasetID: datasetID)
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
        let axisPosition = snapshotAxes.firstIndex { $0.axis == axis }
        let snapshotAxis = axisPosition.map { snapshotAxes[$0] }
        let currentIndex = snapshotAxis?.index
            ?? axisPosition.flatMap { indices[safe: $0] }
            ?? indices[safe: axis]
            ?? 0
        let length = max(snapshotAxis?.length ?? currentIndex + 1, 1)
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
        let axisPosition = snapshotAxes.firstIndex { $0.axis == axis }
        let snapshotAxis = axisPosition.map { snapshotAxes[$0] }
        let length = max(snapshotAxis?.length ?? index + 1, 1)
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
            ?? explorerState.snapshot?.nonDisplayAxes?.first?.axis
            ?? explorerState.nonDisplayIndices.indices.first
        guard let axis else {
            explorerState.moviePlaying = false
            state.imageExplorers[datasetID] = explorerState
            return
        }

        var indices = explorerState.nonDisplayIndices
        let snapshotAxes = explorerState.snapshot?.nonDisplayAxes ?? []
        let axisPosition = snapshotAxes.firstIndex { $0.axis == axis }
        let snapshotAxis = axisPosition.map { snapshotAxes[$0] }
        let currentIndex = snapshotAxis?.index
            ?? axisPosition.flatMap { indices[safe: $0] }
            ?? indices[safe: axis]
            ?? 0
        let length = max(snapshotAxis?.length ?? currentIndex + 1, 1)
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
              let overlayShapes = region.overlayShapes,
              overlayShapes.indices.contains(index)
        else {
            state.lastErrors.append("No region shape is available to delete.")
            return
        }
        let remainingShapes = overlayShapes.enumerated().compactMap { shapeIndex, shape -> [(x: Int, y: Int)]? in
            guard shapeIndex != index, shape.closed, shape.vertices.count >= 3 else {
                return nil
            }
            return shape.vertices.map { Self.sourcePixel(for: $0, displayAxes: snapshot.displayAxes ?? []) }
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
        for vertex: ImageExplorerSnapshot.Region.OverlayVertex,
        displayAxes: [ImageExplorerSnapshot.DisplayAxis]
    ) -> (x: Int, y: Int) {
        guard let xAxis = displayAxes.first, let yAxis = displayAxes[safe: 1] else {
            return (Int(vertex.sampledX.rounded()), Int(vertex.sampledY.rounded()))
        }
        return (
            x: xAxis.blc + Int((vertex.sampledX * Double(max(xAxis.inc, 1))).rounded()),
            y: yAxis.blc + Int((vertex.sampledY * Double(max(yAxis.inc, 1))).rounded())
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
        let baseURL = URL(
            fileURLWithPath: state.tutorialPack?.rootPath ?? state.project.rootPath,
            isDirectory: true
        )
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
        guard appendTableBrowserMove(from: selectedIndex, to: index, into: &browserState) else {
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
        let request = TableBrowserCellValueRequest(
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
        guard state.taskCatalog.contains(where: { $0.id == taskID }) else {
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
        session.overridePatch.unset.remove(argumentID)
        session.overridePatch.values[argumentID] = concept.valueDomain.value(from: normalized)
        session.draftText[argumentID] = normalized
        resolveParameterSession(&session, editedParameters: [argumentID])
        state.parameterSessions[sessionKey] = session
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
        session.overridePatch.unset.remove(argumentID)
        session.overridePatch.values[argumentID] = .bool(value)
        session.draftText.removeValue(forKey: argumentID)
        resolveParameterSession(&session, editedParameters: [argumentID])
        state.parameterSessions[sessionKey] = session
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
        session.overridePatch.unset.insert(name)
        session.draftText.removeValue(forKey: name)
        resolveParameterSession(&session, editedParameters: [name])
        state.parameterSessions[sessionKey] = session
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
                baseTOML = loaded.profileTOML
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

    public func taskExecutionMatrixRow(taskID: String? = nil) -> TaskExecutionMatrixRow? {
        state.taskExecutionMatrixRows.first { $0.taskID == (taskID ?? state.activeTaskID) }
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
        guard let task = state.taskCatalog.first(where: { $0.id == taskID }) else {
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
            logLines: ["Starting \(task.binaryName).", summary],
            lastEvent: "started"
        ))
        state.taskRun = TaskRun(
            runID: runID,
            state: .running,
            progress: 0.05,
            logLines: ["Starting \(task.binaryName).", summary],
            warnings: [],
            products: [],
            diagnostics: [],
            requestSummary: summary,
            imagerProgress: imagerProgressSnapshot(taskID: taskID, runID: runID, taskState: .running, progress: 0.05)
        )

        let parameterAttempt = TaskParameterAttempt(
            surfaceID: taskID,
            workspace: parameterSession.workspace,
            values: parameterSession.values,
            saveLast: parameterSession.saveLast
        )
        taskParameterAttempts[runID] = parameterAttempt
        if parameterAttempt.saveLast {
            do {
                _ = try surfaceParameterClient.writeLast(
                    surfaceID: taskID,
                    workspace: parameterAttempt.workspace,
                    values: parameterAttempt.values,
                    successful: false
                )
            } catch {
                state.taskRun.warnings.append("Automatic Last save failed: \(error)")
            }
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
            taskParameterAttempts.removeValue(forKey: runID)
            finalizeNotebookTaskRecording(
                runID: runID,
                status: "failed",
                diagnostics: ["\(error)"]
            )
            state.taskRun = TaskRun(
                state: .failed,
                progress: 1.0,
                logLines: ["Failed to start \(task.binaryName)."],
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
                policy: bypass ? "bypass_once" : "record",
                request: NotebookRecordingRequest(
                    initiatingSurface: "gui",
                    operationId: taskID,
                    notebookId: state.scientificNotebooks?.activeNotebookID,
                    cellId: nil,
                    taskIntent: intent,
                    providerContractVersion: UInt32(clamping: session.bundle.surface.contractVersion),
                    resolvedParameters: resolvedParameters,
                    runSafety: NotebookRunSafetyRecord(
                        classification: runSafety.classes.joined(separator: ","),
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
                policy: bypass ? "bypass_once" : "record",
                request: NotebookRecordingRequest(
                    initiatingSurface: "gui",
                    operationId: operationID,
                    notebookId: state.scientificNotebooks?.activeNotebookID,
                    cellId: nil,
                    taskIntent: nil,
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
                    stdout: [],
                    stderr: [],
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
                    stdout: Array(stdout.utf8),
                    stderr: Array(stderr.utf8),
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
    }

    package func setPrototypePythonSource(cellID: String, source: String) {
        guard runtimeKind == .pythonPrototype,
              let index = state.prototypePython?.cells.firstIndex(where: { $0.id == cellID })
        else { return }
        state.prototypePython?.cells[index].source = source
    }

    package func approvePrototypePythonSource(cellID: String) {
        guard runtimeKind == .pythonPrototype,
              let index = state.prototypePython?.cells.firstIndex(where: { $0.id == cellID }),
              state.prototypePython?.cells[index].owner == .ai
        else { return }
        state.prototypePython?.cells[index].approvedSourceDigest =
            state.prototypePython?.cells[index].sourceDigest
    }

    package func runPrototypePythonCell(_ cellID: String) {
        guard runtimeKind == .pythonPrototype,
              state.prototypePython?.kernelState == .ready,
              let index = state.prototypePython?.cells.firstIndex(where: { $0.id == cellID }),
              state.prototypePython?.cells[index].approvalIsValid == true
        else { return }

        let sequence = state.prototypePython?.nextExecutionSequence ?? 1
        state.prototypePython?.nextExecutionSequence = sequence + 1
        state.prototypePython?.selectedCellID = cellID
        state.prototypePython?.kernelState = .running
        state.prototypePython?.runningCellID = cellID
        let digest = state.prototypePython?.cells[index].sourceDigest ?? ""
        state.prototypePython?.cells[index].revisions.append(
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

        guard state.prototypePython?.cells[index].behavior != .nonresponsive else { return }
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
    }

    package func restartPrototypePythonKernel() {
        guard runtimeKind == .pythonPrototype else { return }
        if state.prototypePython?.kernelState == .running {
            interruptPrototypePythonKernel()
        }
        state.prototypePython?.kernelState = .ready
        state.prototypePython?.runningCellID = nil
    }

    package func regeneratePrototypePythonPlot(cellID: String) {
        guard runtimeKind == .pythonPrototype,
              state.prototypePython?.kernelState == .ready,
              let cell = state.prototypePython?.cells.first(where: { $0.id == cellID }),
              cell.behavior == .plot,
              cell.approvalIsValid
        else { return }
        let sequence = state.prototypePython?.nextExecutionSequence ?? 1
        state.prototypePython?.nextExecutionSequence = sequence + 1
        appendCompletedPrototypePythonRevision(cellID: cellID, sequence: sequence)
    }

    package func insertPrototypePythonPlot(cellID: String, plotID: String) {
        guard runtimeKind == .pythonPrototype,
              let cellIndex = state.prototypePython?.cells.firstIndex(where: { $0.id == cellID }),
              let revisionIndex = state.prototypePython?.cells[cellIndex].revisions.firstIndex(where: { $0.plot?.id == plotID })
        else { return }
        state.prototypePython?.cells[cellIndex].revisions[revisionIndex].plot?.insertedInNotebook = true
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
            targetVisualizationID: visualizationID
        )
    }

    package func closePrototypeExplorer() {
        guard runtimeKind == .pythonPrototype else { return }
        state.prototypePython?.activeExplorer = nil
    }

    package func setPrototypeExplorerParameter(id: String, value: String) {
        guard runtimeKind == .pythonPrototype,
              let index = state.prototypePython?.activeExplorer?.parameters.firstIndex(where: { $0.id == id })
        else { return }
        state.prototypePython?.activeExplorer?.parameters[index].value = value
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
    }

    package func setPrototypeEnlargedVisualization(_ visualizationID: String?) {
        guard runtimeKind == .pythonPrototype else { return }
        state.prototypePython?.enlargedVisualizationID = visualizationID
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
            assetPath: "notebooks/assets/explorers/\(visualizationID)/r\(sequence).png"
        )
    }

    private func completePrototypePythonCell(cellID: String, sequence: Int) {
        guard runtimeKind == .pythonPrototype,
              state.prototypePython?.kernelState == .running,
              state.prototypePython?.runningCellID == cellID
        else { return }
        appendCompletedPrototypePythonRevision(cellID: cellID, sequence: sequence, replacingRunning: true)
        state.prototypePython?.kernelState = .ready
        state.prototypePython?.runningCellID = nil
    }

    private func appendCompletedPrototypePythonRevision(
        cellID: String,
        sequence: Int,
        replacingRunning: Bool = false
    ) {
        guard let cellIndex = state.prototypePython?.cells.firstIndex(where: { $0.id == cellID }) else { return }
        let cell = state.prototypePython?.cells[cellIndex]
        let fails = cell?.behavior == .failure && cell?.source.contains("raise RuntimeError") == true
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
        let plot = cell?.behavior == .plot && !fails
            ? PrototypePythonPlotRevision(
                id: "python-plot-\(sequence)",
                sequence: sequence,
                title: cellID == "python-cell-ai"
                    ? "AI proposal · radial profile"
                    : "TW Hya · amplitude vs UV distance",
                pngPath: "notebooks/assets/\(cellID)/execution-\(sequence)/figure-1.png",
                svgPath: "notebooks/assets/\(cellID)/execution-\(sequence)/figure-1.svg"
            )
            : nil
        let revision = PrototypePythonExecutionRevision(
            id: "python-execution-\(sequence)",
            sequence: sequence,
            status: status,
            sourceDigest: cell?.sourceDigest ?? "",
            outputs: outputs,
            plot: plot
        )
        if replacingRunning,
           let revisionIndex = state.prototypePython?.cells[cellIndex].revisions.lastIndex(where: { $0.sequence == sequence })
        {
            state.prototypePython?.cells[cellIndex].revisions[revisionIndex] = revision
        } else {
            state.prototypePython?.cells[cellIndex].revisions.append(revision)
        }
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
            measurementSetParameterAttempts.removeValue(forKey: jobID)
            if let datasetID = datasetIDForExplorerTabID(job.tabID),
               var plotState = state.measurementSetPlots[datasetID] {
                plotState.status = .idle
                plotState.lastError = "Cancelled"
                state.measurementSetPlots[datasetID] = plotState
            }
        case .genericTask:
            activeTaskExecutions[jobID]?.cancel()
            activeTaskExecutions.removeValue(forKey: jobID)
            taskParameterAttempts.removeValue(forKey: jobID)
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
        state.taskCatalog.first { $0.id == taskID }?.displayName ?? "Tasks"
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
               rowStart: request.rowStart,
               rowLimit: request.rowLimit,
               columnStart: request.columnStart,
               columnLimit: request.columnLimit
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
        return TableBrowserParameters(
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
                return .cell(row: row, column: column)
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
            browserState.commands.append(.moveDown(steps: delta))
            return true
        }
        if delta < 0 {
            browserState.commands.append(.moveUp(steps: -delta))
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
            do {
                if let snapshot = try surfaceParameterClient.last(
                    surfaceID: surfaceID,
                    workspace: workspace,
                    successful: false
                ) {
                    state.parameterSessions[sessionKey] = SurfaceParameterSession(
                        bundle: bundle,
                        snapshot: snapshot,
                        selectedSource: .last,
                        baseProfileTOML: snapshot.profileTOML,
                        baseProfilePath: nil,
                        workspace: workspace
                    )
                    return
                }
            } catch {
                state.lastErrors.append(
                    "Last parameters for \(surfaceID) could not be loaded; using Defaults: \(error)"
                )
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
                parameter: unresolvedParameters.count == 1 ? unresolvedParameters[0] : nil
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
            session.contextPatch.unset.remove(name)
            session.contextPatch.values[name] = concept.valueDomain.value(from: text)
            editedParameters.insert(name)
            if !preserveOverrides {
                session.overridePatch.values.removeValue(forKey: name)
                session.overridePatch.unset.remove(name)
            }
        }
        for (name, value) in boolValues {
            guard session.bundle.concept(for: name) != nil else { continue }
            if preserveOverrides, session.overridePatch.values[name] != nil { continue }
            session.contextPatch.unset.remove(name)
            session.contextPatch.values[name] = .bool(value)
            editedParameters.insert(name)
            if !preserveOverrides {
                session.overridePatch.values.removeValue(forKey: name)
                session.overridePatch.unset.remove(name)
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
        session.overridePatch.unset.remove(name)
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
        nextSessionParameterSequence &+= 1
        acceptedSessionParameterValues[sessionKey] = session.values
        acceptedSessionParameterSequence[sessionKey] = nextSessionParameterSequence
        scheduleSessionLast(sessionKey: sessionKey)
    }

    private func scheduleSessionLast(sessionKey: String) {
        sessionLastWrites[sessionKey]?.cancel()
        let work = DispatchWorkItem { [weak self] in
            self?.persistSessionLastIfChanged(sessionKey: sessionKey)
            self?.sessionLastWrites.removeValue(forKey: sessionKey)
        }
        sessionLastWrites[sessionKey] = work
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.3, execute: work)
    }

    private func persistSessionLastIfChanged(sessionKey: String) {
        guard runtimeKind == .production else { return }
        guard let session = state.parameterSessions[sessionKey] else { return }
        let surfaceID = session.snapshot.surfaceID
        let destination = SessionLastDestination(surfaceID: surfaceID, workspace: session.workspace)
        guard session.snapshot.surfaceKind == "session",
              session.saveLast,
              let acceptedValues = acceptedSessionParameterValues[sessionKey],
              let acceptedSequence = acceptedSessionParameterSequence[sessionKey],
              acceptedSequence >= (sessionLastSequence[destination] ?? 0)
        else { return }
        if sessionLastValues[destination] == acceptedValues {
            sessionLastSequence[destination] = acceptedSequence
            return
        }
        do {
            _ = try surfaceParameterClient.writeLast(
                surfaceID: surfaceID,
                workspace: session.workspace,
                values: acceptedValues,
                successful: false
            )
            sessionLastValues[destination] = acceptedValues
            sessionLastSequence[destination] = acceptedSequence
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

    private func applyTutorialPackParameters(
        _ parameters: [String: TutorialPackValue],
        taskID: String,
        packRoot: String
    ) {
        let schema = state.taskUISchemas[taskID]
        let argumentsByID = Dictionary(uniqueKeysWithValues: (schema?.arguments ?? []).map { ($0.id, $0) })
        for (argumentID, value) in parameters {
            if let boolValue = value.boolValue,
               (argumentsByID[argumentID]?.parser.kind == "toggle" || argumentsByID[argumentID] == nil) {
                setGenericTaskToggle(taskID: taskID, argumentID: argumentID, value: boolValue)
                continue
            }
            guard var textValue = value.stringValue else {
                continue
            }
            if shouldResolveTutorialPath(taskID: taskID, argumentID: argumentID, value: textValue) {
                textValue = URL(fileURLWithPath: packRoot, isDirectory: true)
                    .appendingPathComponent(textValue)
                    .standardizedFileURL
                    .path
            }
            setGenericTaskValue(taskID: taskID, argumentID: argumentID, value: textValue)
        }
    }

    private func shouldResolveTutorialPath(taskID: String, argumentID: String, value: String) -> Bool {
        guard !value.isEmpty,
              !value.hasPrefix("/"),
              !value.hasPrefix("~"),
              !value.hasPrefix("http://"),
              !value.hasPrefix("https://"),
              !Self.isInlineRegionSyntax(value)
        else {
            return false
        }
        guard let domain = parameterSession(
            surfaceID: taskID,
            instanceID: state.activeTabID
        )?.bundle.concept(for: argumentID)?.valueDomain else {
            return false
        }
        return domain.isPathLike
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
        let output = state.taskRun.diagnostics.joined(separator: "\n\n").trimmingCharacters(in: .whitespacesAndNewlines)
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
        if let parameterAttempt = measurementSetParameterAttempts.removeValue(forKey: jobID),
           parameterAttempt.saveLast {
            do {
                _ = try surfaceParameterClient.writeLast(
                    surfaceID: parameterAttempt.surfaceID,
                    workspace: parameterAttempt.workspace,
                    values: parameterAttempt.values,
                    successful: true
                )
            } catch {
                state.lastErrors.append("Automatic msexplore Last Successful save failed: \(error)")
            }
        }
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
        measurementSetParameterAttempts.removeValue(forKey: jobID)
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
        let parameters = ImageExplorerParameters(
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
        let selected = snapshot.nonDisplayAxes?.first { axis in
            if let index = Int(selector), axis.axis == index {
                return true
            }
            return axis.label.caseInsensitiveCompare(selector) == .orderedSame
        }
        guard let selected else {
            let available = snapshot.nonDisplayAxes?
                .map { "\($0.label) (\($0.axis))" }
                .joined(separator: ", ") ?? "none"
            throw NSError(
                domain: "CasarsMac.ImageExplorerProfile",
                code: 1,
                userInfo: [
                    NSLocalizedDescriptionKey:
                        "imexplore \(parameter)=\(selector.debugDescription) does not identify a non-display axis; available axes: \(available)"
                ]
            )
        }
        return selected.axis
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
        axes: [ImageExplorerSnapshot.NonDisplayAxis]
    ) -> [Int] {
        axes.enumerated().map { position, axis in
            indices[safe: position] ?? axis.index
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
        let parameterAttempt = taskParameterAttempts.removeValue(forKey: runID)
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
                let managedImagerResult = decodeManagedImagerResult(result)
                if state.taskRun.runID == runID {
                    let progressSnapshot = terminalImagerProgressSnapshot(
                        taskID: result.taskID,
                        runID: runID,
                        taskState: .succeeded,
                        progress: 1.0
                    )
                    if let managedImagerResult {
                        state.taskRun = TaskRun(
                            runID: runID,
                            state: .succeeded,
                            progress: 1.0,
                            logLines: [
                                "\(result.taskID) completed.",
                                "Arguments: \(result.arguments.joined(separator: " "))",
                                managedImagerResult.run.summary
                            ],
                            warnings: result.stderr.isEmpty ? [] : [result.stderr],
                            products: managedImagerResult.artifacts.map(\.path),
                            diagnostics: managedImagerResult.run.warnings,
                            outputPaths: managedImagerResult.outputPaths,
                            requestSummary: state.taskRun.requestSummary,
                            imagerProgress: progressSnapshot
                        )
                    } else {
                        let genericProducts = genericTaskProducts(from: result)
                        let outputPaths = genericProducts.map(\.path)
                        state.taskRun = TaskRun(
                            runID: runID,
                            state: .succeeded,
                            progress: 1.0,
                            logLines: ["\(result.taskID) completed.", "Arguments: \(result.arguments.joined(separator: " "))"],
                            warnings: result.stderr.isEmpty ? [] : [result.stderr],
                            products: genericProducts.map(\.path),
                            diagnostics: result.stdout.isEmpty ? [] : [result.stdout],
                            outputPaths: outputPaths,
                            requestSummary: state.taskRun.requestSummary,
                            imagerProgress: progressSnapshot
                        )
                    }
                }
                let affectedPaths: [String]
                if let managedImagerResult {
                    let products = appendProducedDatasets(from: managedImagerResult)
                    recordRunProductGroup(from: managedImagerResult, products: products)
                    affectedPaths = managedImagerResult.outputPaths
                } else {
                    let products = appendProducedDatasets(from: genericTaskProducts(from: result), runID: runID)
                    recordGenericRunProductGroup(runID: runID, taskID: result.taskID, products: products)
                    affectedPaths = products.map(\.path)
                }
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
                if let parameterAttempt, parameterAttempt.saveLast {
                    do {
                        _ = try surfaceParameterClient.writeLast(
                            surfaceID: parameterAttempt.surfaceID,
                            workspace: parameterAttempt.workspace,
                            values: parameterAttempt.values,
                            successful: true
                        )
                    } catch {
                        state.taskRun.warnings.append("Automatic Last Successful save failed: \(error)")
                    }
                }
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

    private func decodeManagedImagerResult(_ result: GenericTaskResult) -> ManagedImagingOutput? {
        guard result.taskID == "imager",
              !result.stdout.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        else {
            return nil
        }
        guard let output = try? JSONDecoder().decode(ManagedImagingOutput.self, from: Data(result.stdout.utf8)) else {
            return nil
        }
        return resolvedManagedImagingOutput(output)
    }

    private func resolvedManagedImagingOutput(_ output: ManagedImagingOutput) -> ManagedImagingOutput {
        var resolved = output
        resolved.artifacts = output.artifacts.map { artifact in
            var artifact = artifact
            artifact.path = resolvedTaskPathString(artifact.path)
            if let previewPath = artifact.previewPngPath {
                artifact.previewPngPath = resolvedTaskPathString(previewPath)
            }
            return artifact
        }
        return resolved
    }

    private func resolvedTaskPathString(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return path }
        let expanded = (trimmed as NSString).expandingTildeInPath
        if expanded.hasPrefix("/") {
            return URL(fileURLWithPath: expanded).standardizedFileURL.path
        }
        let root = state.project.rootPath.isEmpty ? FileManager.default.currentDirectoryPath : state.project.rootPath
        return URL(
            fileURLWithPath: expanded,
            relativeTo: URL(fileURLWithPath: root, isDirectory: true)
        )
        .standardizedFileURL
        .path
    }

    private func genericTaskProducts(from result: GenericTaskResult) -> [ManagedImagingArtifact] {
        guard let data = result.stdout.data(using: .utf8),
              let payload = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let taskResult = payload["result"] as? [String: Any]
        else {
            return []
        }
        return genericTaskProductKeys(taskID: result.taskID)
            .compactMap { key -> ManagedImagingArtifact? in
                guard let outputPath = taskResult[key] as? String, !outputPath.isEmpty else {
                    return nil
                }
                let outputURL = URL(
                    fileURLWithPath: outputPath,
                    relativeTo: URL(fileURLWithPath: state.project.rootPath, isDirectory: true)
                ).standardizedFileURL
                let path = outputURL.path
                return ManagedImagingArtifact(
                    kind: genericTaskProductKind(taskID: result.taskID, key: key, path: path),
                    label: URL(fileURLWithPath: path).lastPathComponent,
                    path: path,
                    exists: FileManager.default.fileExists(atPath: path),
                    previewPngPath: nil,
                    previewPngExists: false
                )
            }
    }

    private func genericTaskProductKeys(taskID: String) -> [String] {
        switch taskID {
        case "exportfits":
            return ["fitsimage"]
        case "importfits":
            return ["imagename"]
        case "immoments", "impv", "imsubimage", "immath":
            return ["outfile"]
        case "imregrid":
            return ["output"]
        case "feather":
            return ["imagename"]
        case "simobserve":
            return ["output_ms", "manifest_path"]
        default:
            return ["outfile"]
        }
    }

    private func genericTaskProductKind(taskID: String, key: String, path: String) -> String {
        if taskID == "simobserve" && key == "output_ms" {
            return "measurement-set"
        }
        if taskID == "simobserve" && key == "manifest_path" {
            return "json"
        }
        if taskID == "exportfits" || ["fits", "fit", "fts"].contains(URL(fileURLWithPath: path).pathExtension.lowercased()) {
            return "fits"
        }
        if key == "imagename" || key == "outfile" || key == "output" {
            return "casa-image"
        }
        return "run-product"
    }

    private func appendProducedDatasets(from artifacts: [ManagedImagingArtifact], runID: String) -> [RunProductReference] {
        var products: [RunProductReference] = []
        for artifact in artifacts where artifact.exists {
            if let existing = state.project.datasets.first(where: { $0.path == artifact.path }) {
                products.append(runProductReference(artifact: artifact, datasetID: existing.id))
                continue
            }
            if let probed = try? probeClient.probePath(path: artifact.path) {
                state.project.datasets.append(probed)
                products.append(runProductReference(artifact: artifact, datasetID: probed.id))
                continue
            }
            let fallback = DatasetSummary(
                id: artifact.path,
                name: URL(fileURLWithPath: artifact.path).lastPathComponent,
                path: artifact.path,
                kind: fallbackDatasetKind(for: artifact),
                size: "Unprobed \(artifact.kind) product",
                units: fallbackDatasetUnits(for: artifact),
                notes: "Produced by \(runID).",
                diagnostics: []
            )
            state.project.datasets.append(fallback)
            products.append(runProductReference(artifact: artifact, datasetID: fallback.id))
        }
        return products
    }

    private func recordGenericRunProductGroup(runID: String, taskID: String, products: [RunProductReference]) {
        guard !products.isEmpty else { return }
        let group = RunProductGroup(
            id: "products-\(runID)",
            runID: runID,
            title: "\(taskID) products",
            sourceDatasetID: state.selectedDatasetID ?? "",
            sourcePath: state.selectedDataset?.path ?? "",
            products: products,
            diagnostics: []
        )
        if let index = state.runProductGroups.firstIndex(where: { $0.runID == runID }) {
            state.runProductGroups[index] = group
        } else {
            state.runProductGroups.append(group)
        }
    }

    private func appendProducedDatasets(from result: ManagedImagingOutput) -> [RunProductReference] {
        var products: [RunProductReference] = []
        for artifact in result.artifacts where artifact.exists {
            if let existing = state.project.datasets.first(where: { $0.path == artifact.path }) {
                products.append(runProductReference(artifact: artifact, datasetID: existing.id))
                continue
            }
            if let probed = try? probeClient.probePath(path: artifact.path) {
                state.project.datasets.append(probed)
                products.append(runProductReference(artifact: artifact, datasetID: probed.id))
                continue
            }
            let fallback = DatasetSummary(
                id: artifact.path,
                name: URL(fileURLWithPath: artifact.path).lastPathComponent,
                path: artifact.path,
                kind: fallbackDatasetKind(for: artifact),
                size: "Unprobed \(artifact.kind) product",
                units: fallbackDatasetUnits(for: artifact),
                notes: "Produced by imager from \(result.request.measurementSet).",
                diagnostics: artifact.previewPngExists
                    ? ["preview: \(artifact.previewPngPath ?? "")"]
                    : []
            )
            state.project.datasets.append(fallback)
            products.append(runProductReference(artifact: artifact, datasetID: fallback.id))
        }
        return products
    }

    private func recordRunProductGroup(from result: ManagedImagingOutput, products: [RunProductReference]) {
        guard let runID = state.taskRun.runID else { return }
        let group = RunProductGroup(
            id: "products-\(runID)",
            runID: runID,
            title: "Imager products",
            sourceDatasetID: state.selectedDatasetID ?? "",
            sourcePath: result.request.measurementSet,
            products: products,
            diagnostics: result.run.warnings
        )
        if let index = state.runProductGroups.firstIndex(where: { $0.runID == runID }) {
            state.runProductGroups[index] = group
        } else {
            state.runProductGroups.append(group)
        }
    }

    private func runProductReference(artifact: ManagedImagingArtifact, datasetID: String?) -> RunProductReference {
        RunProductReference(
            id: artifact.path,
            artifactKind: artifact.kind,
            label: artifact.label,
            path: artifact.path,
            datasetID: datasetID,
            exists: artifact.exists,
            previewPngPath: artifact.previewPngPath,
            previewPngExists: artifact.previewPngExists
        )
    }

    private func fallbackDatasetKind(for artifact: ManagedImagingArtifact) -> DatasetKind {
        let kind = artifact.kind.lowercased()
        if kind.contains("table") {
            return .table
        }
        if kind.contains("ms") || kind.contains("measurement") {
            return .measurementSet
        }
        if kind.contains("fits") || kind.contains("output") {
            return .runProduct
        }
        return .imageCube
    }

    private func fallbackDatasetUnits(for artifact: ManagedImagingArtifact) -> String {
        let kind = artifact.kind.lowercased()
        if kind.contains("image") {
            return "CASA image"
        }
        if kind.contains("table") {
            return "CASA table"
        }
        return artifact.kind
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
            session.overridePatch.unset.remove(name)
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
                strategy: WorkbenchPlotPayloadStrategy(payloadStrategy: payload.payloadStrategy, fallback: pointCloud == nil ? .inlineDisplayPoints : .viewportLevelOfDetail),
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
    init(payloadStrategy: String, fallback: WorkbenchPlotPayloadStrategy) {
        switch payloadStrategy {
        case "point_cloud":
            self = .viewportLevelOfDetail
        case "intervals":
            self = .inlineDisplayPoints
        case "single_pixel_point_raster":
            self = .singlePixelPointRaster
        case "density_grid":
            self = .densityGrid
        default:
            self = fallback
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
