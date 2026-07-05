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

public final class WorkbenchStore: ObservableObject {
    @Published public private(set) var state: WorkbenchState
    private let probeClient: ProjectProbeClient
    private let demoProjectClient: DemoProjectClient
    private let plotClient: MeasurementSetPlotClient
    private let imageExplorerClient: ImageExplorerClient
    private let tableBrowserClient: TableBrowserClient
    private let genericTaskClient: GenericTaskClient
    private let taskUISchemaClient: TaskUISchemaClient
    private let taskExecutionMatrixClient: TaskExecutionMatrixClient
    private let imagerProgressSource: ImagerProgressSource
    private let plotQueue = DispatchQueue(label: "casars.mac.ms-plot-job", qos: .userInitiated, attributes: .concurrent)
    private let tableBrowserQueue = DispatchQueue(label: "casars.mac.tablebrowser-cell-window", qos: .userInitiated)
    private var activeTaskExecutions: [String: TaskExecution] = [:]
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
        self.probeClient = probeClient
        self.demoProjectClient = demoProjectClient
        self.plotClient = plotClient
        self.imageExplorerClient = imageExplorerClient
        self.tableBrowserClient = tableBrowserClient
        self.genericTaskClient = genericTaskClient
        self.taskUISchemaClient = taskUISchemaClient
        self.taskExecutionMatrixClient = taskExecutionMatrixClient
        self.imagerProgressSource = imagerProgressSource
    }

    deinit {
        cleanupTemporaryDemoProject()
    }

    public static func empty() -> WorkbenchStore {
        WorkbenchStore(state: EmptyWorkbench.makeState())
    }

    public static func fixture() -> WorkbenchStore {
        WorkbenchStore(state: FixtureWorkbench.makeState())
    }

    public func openFixtureProject() {
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

    public func openDatasetTableBrowser(_ datasetID: String) {
        guard let dataset = state.project.datasets.first(where: { $0.id == datasetID }) else {
            state.lastErrors.append("Unknown dataset \(datasetID)")
            return
        }
        guard canBrowseAsTable(dataset) else {
            state.lastErrors.append("Dataset \(dataset.name) is not a casacore table")
            return
        }

        state.selectedDatasetID = datasetID
        refreshTableBrowser(datasetID: datasetID)
        openTab(
            WorkbenchTab(
                id: tableBrowserTabID(for: dataset.id),
                title: "Table: \(dataset.name)",
                kind: .tableBrowser,
                datasetID: dataset.id
            )
        )
    }

    public func openTableBrowserPath(_ path: String, sourceDatasetID: String? = nil) {
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
        if normalized.contains("plot") || normalized.contains("chart") {
            openDefaultTab(kind: .plotSamples)
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

    public func openTab(_ tab: WorkbenchTab) {
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
        case .tableBrowser:
            guard let dataset = state.selectedDataset else {
                state.lastErrors.append("No selected dataset to browse")
                return
            }
            openDatasetTableBrowser(dataset.id)
        case .tutorial:
            guard state.tutorialPack != nil else {
                state.lastErrors.append("No tutorial pack is open")
                return
            }
            openTab(WorkbenchTab(id: "tab-tutorial-pack", title: "Tutorial", kind: .tutorial))
        case .task:
            if state.isDemoProject {
                openTab(WorkbenchTab(id: "tab-task", title: "Calibrate", kind: .task, datasetID: state.selectedDatasetID))
            } else {
                openTab(WorkbenchTab(id: nextTaskTabID(), title: "Tasks", kind: .task, datasetID: state.selectedDatasetID))
            }
        case .plotSamples:
            if state.plotDocuments.isEmpty {
                state.plotDocuments = WorkbenchPlotSamples.all()
            }
            openTab(WorkbenchTab(id: "tab-plot-samples", title: "Plot Samples", kind: .plotSamples))
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

    public func openImagerProgressMockup() {
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
        guard state.selectedDataset != nil else {
            state.lastErrors.append("Open a project with a dataset before opening an imaging task")
            return
        }
        state.activeTaskID = "imager"
        loadTaskUISchemaIfNeeded("imager")

        if let dataset = state.selectedDataset, dataset.kind == .measurementSet {
            seedImagerTaskDefaults(for: dataset, preserveExistingEdits: false)
            state.taskRun = TaskRun(
                state: .idle,
                progress: 0,
                logLines: ["Imager task initialized from selected MeasurementSet metadata."],
                warnings: [],
                products: [],
                requestSummary: genericTaskRequestSummary(taskID: "imager"),
                imagerProgress: nil
            )

            openTab(
                WorkbenchTab(
                    id: "tab-imager-\(dataset.id)",
                    title: "Imager: \(dataset.name)",
                    kind: .task,
                    datasetID: dataset.id,
                    taskID: "imager"
                )
            )
        } else {
            state.taskRun = TaskRun(
                state: .idle,
                progress: 0,
                logLines: ["Imager task opened. Select a MeasurementSet before running."],
                warnings: [],
                products: [],
                requestSummary: genericTaskRequestSummary(taskID: "imager"),
                imagerProgress: nil
            )

            openTab(
                WorkbenchTab(
                    id: "tab-imager-unbound",
                    title: "Imager",
                    kind: .task,
                    taskID: "imager"
                )
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

    public func refreshImageExplorer(datasetID: String) {
        guard let dataset = state.project.datasets.first(where: { $0.id == datasetID }) else {
            state.lastErrors.append("Unknown dataset \(datasetID)")
            return
        }
        guard dataset.kind == .imageCube else {
            state.lastErrors.append("Dataset \(dataset.name) is not an image")
            return
        }
        let explorerState = state.imageExplorers[datasetID] ?? ImageExplorerSessionState(
            datasetID: datasetID,
            selectedView: "plane",
            status: .idle,
            lastError: nil,
            snapshot: nil
        )
        do {
            let snapshot = try imageExplorerClient.buildSnapshot(request: explorerState.snapshotRequest(datasetPath: dataset.path))
            var nextState = explorerState
            applyReadyImageExplorerSnapshot(snapshot, to: &nextState)
            state.imageExplorers[datasetID] = nextState
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
                    state.lastErrors.append(
                        "Cleared invalid image explorer region command sequence for \(dataset.name): \(error)"
                    )
                    return
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
        guard let dataset = state.project.datasets.first(where: { $0.id == datasetID }) else {
            state.lastErrors.append("Unknown dataset \(datasetID)")
            return
        }
        guard canBrowseAsTable(dataset) else {
            state.lastErrors.append("Dataset \(dataset.name) is not a casacore table")
            return
        }
        let browserState = state.tableBrowsers[datasetID] ?? TableBrowserSessionState(
            datasetID: datasetID,
            selectedView: Self.canonicalTableBrowserView(nil),
            status: .idle,
            lastError: nil,
            snapshot: nil
        )
        do {
            let snapshot = try tableBrowserClient.buildSnapshot(request: browserState.snapshotRequest(datasetPath: dataset.path))
            var nextState = TableBrowserSessionState(
                datasetID: datasetID,
                selectedView: snapshot.view,
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
        } catch {
            state.tableBrowsers[datasetID] = TableBrowserSessionState(
                datasetID: datasetID,
                selectedView: browserState.selectedView,
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
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func setImageExplorerParameters(_ parameters: ImageExplorerParameters, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.parameters = parameters
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func setImageExplorerColorMap(_ colorMap: ImageExplorerColorMap, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.planeColorMap = colorMap
        state.imageExplorers[datasetID] = explorerState
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
        state.imageExplorers[datasetID] = explorerState
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
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func startImageExplorerMovie(axis: Int, framesPerSecond: Double?, loop: Bool, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.moviePlaying = true
        explorerState.movieAxis = axis
        if let framesPerSecond {
            explorerState.movieFramesPerSecond = Self.clampedMovieFramesPerSecond(framesPerSecond)
        }
        explorerState.movieLoop = loop
        explorerState.selectedProfileAxis = axis
        state.imageExplorers[datasetID] = explorerState
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
    }

    public func setImageExplorerMovieLoop(_ loop: Bool, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.movieLoop = loop
        state.imageExplorers[datasetID] = explorerState
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
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.transientCommands.append(command)
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func loadImageExplorerRegionFile(path: String, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.regionCommands = [.loadRegionFile(path: path)]
        explorerState.activeRegionFilePath = Self.normalizedRegionFilePath(path)
        explorerState.transientCommands = []
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func appendImageExplorerRegionFile(path: String, datasetID: String) {
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
        refreshImageExplorer(datasetID: datasetID)
    }

    private static func normalizedRegionFilePath(_ path: String) -> String {
        URL(fileURLWithPath: (path as NSString).expandingTildeInPath)
            .standardizedFileURL
            .path
    }

    public func exportImageExplorerRegionFile(datasetID: String, path: String? = nil) {
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
        var browserState = state.tableBrowsers[datasetID] ?? TableBrowserSessionState(
            datasetID: datasetID,
            selectedView: Self.canonicalTableBrowserView(nil),
            status: .idle,
            lastError: nil,
            snapshot: nil
        )
        browserState.selectedView = Self.canonicalTableBrowserView(view)
        browserState.focus = "main"
        browserState.commands = []
        browserState.transientCommands = []
        state.tableBrowsers[datasetID] = browserState
        refreshTableBrowser(datasetID: datasetID)
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
        loadTaskUISchemaIfNeeded(taskID)
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

    public func loadTaskUISchemaIfNeeded(_ taskID: String? = nil) {
        let resolvedTaskID = taskID ?? state.activeTaskID
        guard !resolvedTaskID.isEmpty, state.taskUISchemas[resolvedTaskID] == nil else {
            return
        }
        do {
            let schema = try taskUISchemaClient.loadTaskUISchema(taskID: resolvedTaskID)
            state.taskUISchemas[resolvedTaskID] = schema
            seedGenericTaskDefaults(schema)
            state.taskRun = TaskRun(
                state: .idle,
                progress: 0,
                logLines: ["Loaded \(schema.displayName) task schema."],
                warnings: [],
                products: [],
                requestSummary: genericTaskRequestSummary(taskID: resolvedTaskID),
                imagerProgress: imagerProgressSnapshot(taskID: resolvedTaskID, taskState: .idle, progress: 0)
            )
        } catch {
            state.lastErrors.append("Load task schema for \(resolvedTaskID): \(error)")
        }
    }

    public func setGenericTaskValue(taskID: String? = nil, argumentID: String, value: String) {
        let resolvedTaskID = taskID ?? state.activeTaskID
        var values = state.genericTaskValues[resolvedTaskID] ?? [:]
        let argument = state.taskUISchemas[resolvedTaskID]?.arguments.first { $0.id == argumentID }
        values[argumentID] = normalizedGenericTaskValue(value, for: argument)
        state.genericTaskValues[resolvedTaskID] = values
        state.taskRun.requestSummary = genericTaskRequestSummary(taskID: resolvedTaskID)
    }

    public func setGenericTaskToggle(taskID: String? = nil, argumentID: String, value: Bool) {
        let resolvedTaskID = taskID ?? state.activeTaskID
        var toggles = state.genericTaskToggles[resolvedTaskID] ?? [:]
        toggles[argumentID] = value
        state.genericTaskToggles[resolvedTaskID] = toggles
        state.taskRun.requestSummary = genericTaskRequestSummary(taskID: resolvedTaskID)
    }

    public func setGenericTaskConfirmation(taskID: String? = nil, confirmed: Bool) {
        state.genericTaskConfirmations[taskID ?? state.activeTaskID] = confirmed
    }

    public func taskExecutionMatrixRow(taskID: String? = nil) -> TaskExecutionMatrixRow? {
        state.taskExecutionMatrixRows.first { $0.taskID == (taskID ?? state.activeTaskID) }
    }

    public func taskRequiresConfirmation(taskID: String? = nil) -> Bool {
        guard let row = taskExecutionMatrixRow(taskID: taskID) else {
            return false
        }
        return row.mutationClass != "read_only"
            && row.mutationClass != "launcher"
            && row.mutationClass != "not_applicable"
    }

    public func taskHasConfirmation(taskID: String? = nil) -> Bool {
        state.genericTaskConfirmations[taskID ?? state.activeTaskID] ?? false
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

        runGenericTask()
    }

    private func runGenericTask() {
        let taskID = state.activeTaskID
        guard let task = state.taskCatalog.first(where: { $0.id == taskID }) else {
            state.lastErrors.append("Unknown task \(taskID)")
            return
        }
        if state.taskUISchemas[taskID] == nil {
            loadTaskUISchemaIfNeeded(taskID)
        }
        guard let schema = state.taskUISchemas[taskID] else {
            state.lastErrors.append("Task schema for \(taskID) is not available")
            return
        }
        if taskRequiresConfirmation(taskID: taskID) && !taskHasConfirmation(taskID: taskID) {
            state.taskRun = TaskRun(
                state: .failed,
                progress: 1.0,
                logLines: [],
                warnings: [],
                products: [],
                diagnostics: ["Confirm this task may modify data or create products before running it."],
                requestSummary: genericTaskRequestSummary(taskID: taskID),
                imagerProgress: imagerProgressSnapshot(taskID: taskID, taskState: .failed, progress: 1.0)
            )
            state.lastErrors.append("Confirm \(task.displayName) before running.")
            return
        }

        let values = state.genericTaskValues[taskID] ?? [:]
        let toggles = state.genericTaskToggles[taskID] ?? [:]
        let runID = nextJobID(prefix: taskID)
        let summary = genericTaskRequestSummary(taskID: taskID)
        let tabID = state.activeTabID.isEmpty ? "tab-task-\(taskID)" : state.activeTabID
        startJob(WorkbenchJob(
            id: runID,
            tabID: tabID,
            kind: .genericTask,
            owner: .user,
            status: .running,
            progress: 0.05,
            title: schema.displayName,
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

        do {
            let execution = try genericTaskClient.startTask(
                request: GenericTaskRequest(
                    runID: runID,
                    task: task,
                    schema: schema,
                    values: values,
                    toggles: toggles,
                    workingDirectoryPath: state.project.rootPath
                )
            ) { [weak self] event in
                DispatchQueue.main.async {
                    self?.handleGenericTaskEvent(event, runID: runID)
                }
            }
            activeTaskExecutions[runID] = execution
        } catch {
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

    public func stopTask() {
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
            if let datasetID = datasetIDForExplorerTabID(job.tabID),
               var plotState = state.measurementSetPlots[datasetID] {
                plotState.status = .idle
                plotState.lastError = "Cancelled"
                state.measurementSetPlots[datasetID] = plotState
            }
        case .genericTask:
            activeTaskExecutions[jobID]?.cancel()
            activeTaskExecutions.removeValue(forKey: jobID)
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
        state.tableBrowsers[datasetID] ?? TableBrowserSessionState(
            datasetID: datasetID,
            selectedView: Self.canonicalTableBrowserView(nil),
            status: .idle,
            lastError: nil,
            snapshot: nil
        )
    }

    private static func canonicalTableBrowserView(_ view: String?) -> String {
        switch view?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
        case "keywords":
            "keywords"
        case "subtables":
            "subtables"
        default:
            "cells"
        }
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

    private func seedGenericTaskDefaults(_ schema: TaskUISchema) {
        var values = state.genericTaskValues[schema.commandID] ?? [:]
        var toggles = state.genericTaskToggles[schema.commandID] ?? [:]
        for argument in schema.arguments where !argument.hiddenInTUI {
            switch argument.parser.kind {
            case "toggle":
                if toggles[argument.id] == nil {
                    toggles[argument.id] = argument.default == "true"
                }
            case "option", "positional":
                if values[argument.id] == nil {
                    if let defaultValue = argument.default {
                        values[argument.id] = defaultValue
                    } else if argumentLooksLikeOutput(argument) {
                        values[argument.id] = defaultTaskOutputPath(
                            taskID: schema.commandID,
                            argument: argument
                        )
                    } else if argument.valueKind == "path",
                              let dataset = state.selectedDataset,
                              argumentLooksLikeInputDataset(argument),
                              selectedDataset(dataset, matches: argument) {
                        values[argument.id] = normalizedGenericTaskValue(dataset.path, for: argument)
                    } else if argument.id == "imagename" {
                        values[argument.id] = defaultTaskOutputPath(
                            taskID: schema.commandID,
                            argument: argument
                        )
                    } else if argument.id == "spw",
                              let spectralWindow = state.selectedDataset?.spectralWindows.first {
                        values[argument.id] = spectralWindowSelectorValue(spectralWindow)
                    } else if argument.id == "field",
                              let field = state.selectedDataset?.fields.first {
                        values[argument.id] = selectorIDValue(field) ?? field
                    } else if argument.id == "phasecenter_field",
                              let field = state.selectedDataset?.fields.first {
                        values[argument.id] = selectorIDValue(field) ?? field
                    } else if argument.id == "scan",
                              let scan = state.selectedDataset?.scans.first {
                        values[argument.id] = selectorIDValue(scan) ?? scan
                    } else if argument.id == "antenna",
                              let antenna = state.selectedDataset?.antennas.first {
                        values[argument.id] = antenna
                    } else if argument.id == "correlation",
                              let correlation = state.selectedDataset?.correlations.first {
                        values[argument.id] = correlation
                    } else if argument.id == "datacolumn",
                              let dataColumn = state.selectedDataset?.dataColumns.first {
                        values[argument.id] = dataColumn
                    }
                }
            default:
                break
            }
        }
        state.genericTaskValues[schema.commandID] = values
        state.genericTaskToggles[schema.commandID] = toggles
    }

    private func seedImagerTaskDefaults(for dataset: DatasetSummary, preserveExistingEdits: Bool) {
        loadTaskUISchemaIfNeeded("imager")
        var values = state.genericTaskValues["imager"] ?? [:]
        var toggles = state.genericTaskToggles["imager"] ?? [:]

        func setValue(_ argumentID: String, _ value: String?) {
            guard let value else { return }
            if preserveExistingEdits,
               let existing = values[argumentID],
               !existing.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                return
            }
            let argument = state.taskUISchemas["imager"]?.arguments.first { $0.id == argumentID }
            values[argumentID] = normalizedGenericTaskValue(value, for: argument)
        }

        func setToggle(_ argumentID: String, _ value: Bool) {
            if preserveExistingEdits, toggles[argumentID] != nil {
                return
            }
            toggles[argumentID] = value
        }

        setValue("ms", dataset.path)
        setValue("imagename", defaultImagerOutputPrefix(for: dataset))
        let defaultField = defaultImagerField(for: dataset)
        let defaultSpectralWindow = defaultImagerSpectralWindow(for: dataset)
        let defaultCorrelation = defaultImagerCorrelation(for: dataset)
        setValue("field", selectorToken(defaultField) ?? defaultField)
        values["phasecenter_field"] = ""
        setValue("spw", selectorToken(defaultSpectralWindow) ?? defaultSpectralWindow)
        setValue("datacolumn", dataset.dataColumns.first ?? "DATA")
        setValue("polarization", selectorToken(defaultCorrelation) ?? defaultCorrelation)
        setValue("imsize", isTWHyaTutorialDataset(dataset) ? "250" : "512")
        setValue("cell_arcsec", isTWHyaTutorialDataset(dataset) ? "0.1" : "1.0")
        setValue("specmode", "mfs")
        setValue("deconvolver", "hogbom")
        setValue("weighting", isTWHyaTutorialDataset(dataset) ? "briggs" : "natural")
        setValue("gridder", "standard")
        setValue("robust", "0.5")
        setValue("niter", "0")
        setValue("threshold_jy", "0.0")
        setToggle("dirty_only", true)
        state.genericTaskValues["imager"] = values
        state.genericTaskToggles["imager"] = toggles
        state.taskRun.requestSummary = genericTaskRequestSummary(taskID: "imager")
    }

    private func seedDirectMeasurementSetImagerDefaults(for dataset: DatasetSummary) {
        seedImagerTaskDefaults(for: dataset, preserveExistingEdits: false)
        var values = state.genericTaskValues["imager"] ?? [:]
        var toggles = state.genericTaskToggles["imager"] ?? [:]
        let phaseCenterField = dataset.fields.count > 1 ? dataset.fields.first.flatMap(selectorToken) : nil

        values["field"] = ""
        values["phasecenter_field"] = phaseCenterField ?? ""
        values["specmode"] = "cube"
        values["gridder"] = "mosaic"
        values["interpolation"] = "nearest"
        values["channel_start"] = "0"
        values["channel_count"] = "512"
        values["imsize"] = "1024"
        values["cell_arcsec"] = "1.0"
        values["weighting"] = "briggs"
        values["robust"] = "0.5"
        values["niter"] = "2048"
        values["threshold_jy"] = "0.0"
        toggles["dirty_only"] = false
        toggles["write_pb"] = true
        toggles["pbcor"] = true

        state.genericTaskValues["imager"] = values
        state.genericTaskToggles["imager"] = toggles
        state.taskRun.requestSummary = genericTaskRequestSummary(taskID: "imager")
    }

    private func genericTaskRequestSummary(taskID: String) -> String {
        guard let schema = state.taskUISchemas[taskID] else {
            return "task=\(taskID)"
        }
        let values = state.genericTaskValues[taskID] ?? [:]
        let toggles = state.genericTaskToggles[taskID] ?? [:]
        return schema.arguments
            .filter { !$0.hiddenInTUI }
            .compactMap { argument -> String? in
                if argument.parser.kind == "toggle" {
                    return "\(argument.id)=\(toggles[argument.id] ?? false)"
                }
                guard let value = values[argument.id], !value.isEmpty else {
                    return argument.required ? "\(argument.id)=<required>" : nil
                }
                return "\(argument.id)=\(genericTaskDisplayValue(value, for: argument))"
            }
            .joined(separator: ", ")
    }

    private func genericTaskDisplayValue(_ value: String, for argument: TaskUIArgument) -> String {
        guard argument.valueKind == "path" || argument.parameterType?.contains("path") == true else {
            return value
        }
        return projectRelativePath(value)
    }

    private func normalizedGenericTaskValue(_ value: String, for argument: TaskUIArgument?) -> String {
        guard argument?.valueKind == "path" || argument?.parameterType?.contains("path") == true else {
            return value
        }
        guard !Self.isInlineRegionSyntax(value) else {
            return value
        }
        return projectRelativePath(value)
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

    private func argumentLooksLikeInputDataset(_ argument: TaskUIArgument) -> Bool {
        if argumentLooksLikeOutput(argument) {
            return false
        }
        if argument.parameterType == "fits_path" {
            return true
        }
        return ["ms", "vis", "image", "image_path", "imagename", "table", "infile", "fitsimage", "region"].contains(argument.id)
            || argument.parameterType == "region_path_or_box"
            || argument.label.localizedCaseInsensitiveContains("input")
            || argument.label.localizedCaseInsensitiveContains("measurementset")
    }

    private func argumentLooksLikeOutput(_ argument: TaskUIArgument) -> Bool {
        if argument.parameterType?.hasPrefix("output_") == true {
            return true
        }
        return ["outfile", "output", "outputvis", "outputms", "fitsimage"].contains(argument.id)
            && !["fits_path"].contains(argument.parameterType ?? "")
    }

    private func selectedDataset(_ dataset: DatasetSummary, matches argument: TaskUIArgument) -> Bool {
        if argument.parameterType == "image_path" || ["image", "imagename"].contains(argument.id) {
            return dataset.kind == .imageCube
        }
        if argument.parameterType == "fits_path" {
            return isFitsDataset(dataset)
        }
        if argument.parameterType == "region_path_or_box" || argument.id == "region" {
            return dataset.kind == .region
        }
        if argument.parameterType == "measurement_set_path" || ["ms", "vis"].contains(argument.id) {
            return dataset.kind == .measurementSet
        }
        if ["table_path", "calibration_table_path"].contains(argument.parameterType ?? "") {
            return dataset.kind == .table || dataset.kind == .calibrationTable
        }
        return dataset.kind != .region
    }

    private func isFitsDataset(_ dataset: DatasetSummary) -> Bool {
        guard dataset.kind == .runProduct else {
            return false
        }
        switch URL(fileURLWithPath: dataset.path).pathExtension.lowercased() {
        case "fits", "fit", "fts":
            return true
        default:
            return false
        }
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
            if shouldResolveTutorialPath(argumentID: argumentID, argument: argumentsByID[argumentID], value: textValue) {
                textValue = URL(fileURLWithPath: packRoot, isDirectory: true)
                    .appendingPathComponent(textValue)
                    .standardizedFileURL
                    .path
            }
            setGenericTaskValue(taskID: taskID, argumentID: argumentID, value: textValue)
        }
    }

    private func shouldResolveTutorialPath(argumentID: String, argument: TaskUIArgument?, value: String) -> Bool {
        guard !value.isEmpty,
              !value.hasPrefix("/"),
              !value.hasPrefix("~"),
              !value.hasPrefix("http://"),
              !value.hasPrefix("https://"),
              !Self.isInlineRegionSyntax(value)
        else {
            return false
        }
        if argument?.valueKind == "path" || argument?.parameterType?.contains("path") == true {
            return true
        }
        return [
            "image_path",
            "imagename",
            "fitsimage",
            "outfile",
            "input",
            "output"
        ].contains(argumentID)
    }

    private static func isInlineRegionSyntax(_ value: String) -> Bool {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        return trimmed.hasPrefix("box[[")
            || trimmed.hasPrefix("poly [[")
            || trimmed.hasPrefix("box:")
            || trimmed.hasPrefix("pixelbox(")
    }

    private func defaultTaskOutputPath(taskID: String, argument: TaskUIArgument? = nil) -> String {
        let datasetStem = defaultTaskOutputStem(taskID: taskID)
        let basename: String
        switch argument?.parameterType {
        case "output_fits_path":
            basename = "\(datasetStem).fits"
        case "output_measurement_set_path":
            basename = "\(datasetStem)-\(taskID).ms"
        case "output_image_path":
            basename = "\(datasetStem)-\(taskID).image"
        default:
            basename = "\(datasetStem)-\(taskID)"
        }
        if !state.project.rootPath.isEmpty {
            return basename
        }
        return FileManager.default.temporaryDirectory
            .appendingPathComponent("casa-rs-runs", isDirectory: true)
            .appendingPathComponent(basename)
            .path
    }

    private func defaultTaskOutputStem(taskID: String) -> String {
        let name = state.selectedDataset?.name ?? taskID
        let knownSuffixes = [".image", ".ms", ".MS", ".fits", ".fit", ".fts"]
        let trimmed = knownSuffixes.reduce(name) { partial, suffix in
            partial.hasSuffix(suffix) ? String(partial.dropLast(suffix.count)) : partial
        }
        return trimmed.replacingOccurrences(of: " ", with: "-")
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

    public func taskRequestSaveDirectory() -> String {
        if !state.project.rootPath.isEmpty {
            return state.project.rootPath
        }
        return FileManager.default.currentDirectoryPath
    }

    public func taskRequestSaveFilename() -> String {
        "\(sanitizedPathComponent(state.activeTaskID))-family-request.json"
    }

    public func hasSaveableActiveGenericTaskRequest() -> Bool {
        activeGenericTaskRequest() != nil
    }

    public func saveActiveGenericTaskRequest(to path: String) {
        guard let request = activeGenericTaskRequest() else {
            state.lastErrors.append("No task request is available to save.")
            return
        }
        do {
            let data = try ProcessGenericTaskClient.savedJSONRequestData(for: request)
            let url = URL(fileURLWithPath: path)
            try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
            try data.write(to: url, options: .atomic)
            setGenericTaskValue(taskID: request.task.id, argumentID: "request_json", value: url.path)
            state.taskRun.logLines.append("Saved request: \(url.path)")
            state.history.append(ProcessingHistoryEvent(
                id: "hist-save-task-request-\(state.history.count + 1)",
                timestamp: currentTimestamp(),
                title: "Saved \(request.task.id) request",
                reason: "User saved the current \(request.task.id) task request.",
                affectedPaths: [url.path],
                approval: "user"
            ))
        } catch {
            state.lastErrors.append("Save \(state.activeTaskID) request: \(error)")
        }
    }

    public func loadGenericTaskRequest(from path: String, tabID: String? = nil) {
        let url = URL(fileURLWithPath: path)
        do {
            let data = try Data(contentsOf: url)
            let object = try JSONSerialization.jsonObject(with: data)
            guard let envelope = object as? [String: Any],
                  envelope["kind"] as? String == "family",
                  let request = envelope["request"] as? [String: Any]
            else {
                state.lastErrors.append("Open task request: expected a canonical simobserve family envelope.")
                return
            }
            selectTask("simobserve", tabID: tabID)
            setGenericTaskValue(taskID: "simobserve", argumentID: "request_kind", value: "family")
            setGenericTaskValue(taskID: "simobserve", argumentID: "request_json", value: url.path)
            if let sourceModel = request["source_model"] {
                let sourceData = try JSONSerialization.data(withJSONObject: sourceModel, options: [.sortedKeys])
                setGenericTaskValue(
                    taskID: "simobserve",
                    argumentID: "source_model",
                    value: String(decoding: sourceData, as: UTF8.self)
                )
            }
            for key in [
                "telescope",
                "array_config",
                "band",
                "imaging_mode",
                "worker_policy",
                "output_ms"
            ] {
                if let value = request[key] as? String {
                    setGenericTaskValue(taskID: "simobserve", argumentID: key, value: value)
                }
            }
            for key in [
                "target_ms_size_gib",
                "polarizations",
                "ms_channels",
                "image_channels",
                "pointing_count",
                "row_workers",
                "channel_workers"
            ] {
                if let value = request[key] {
                    setGenericTaskValue(taskID: "simobserve", argumentID: key, value: "\(value)")
                }
            }
            if let value = request["measure_actual_size"] as? Bool {
                setGenericTaskValue(taskID: "simobserve", argumentID: "measure_actual_size", value: value ? "true" : "false")
            }
            state.taskRun.logLines.append("Opened request: \(url.path)")
        } catch {
            state.lastErrors.append("Open task request \(path): \(error)")
        }
    }

    public func hasSaveableActiveTaskOutput() -> Bool {
        activeTaskOutput() != nil
    }

    public func saveActiveTaskOutput(to path: String) {
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

    private func activeGenericTaskRequest() -> GenericTaskRequest? {
        let taskID = state.activeTaskID
        let requestKind = state.genericTaskValues[taskID]?["request_kind"]
            ?? state.taskUISchemas[taskID]?.arguments.first { $0.id == "request_kind" }?.default
        guard let task = state.taskCatalog.first(where: { $0.id == taskID }),
              let schema = state.taskUISchemas[taskID],
              requestKind == "family"
        else {
            return nil
        }
        return GenericTaskRequest(
            runID: "save-\(taskID)",
            task: task,
            schema: schema,
            values: state.genericTaskValues[taskID] ?? [:],
            toggles: state.genericTaskToggles[taskID] ?? [:],
            workingDirectoryPath: state.project.rootPath
        )
    }

    private func spectralWindowSelectorValue(_ label: String) -> String {
        selectorIDValue(label) ?? label
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
        } else if dataset.kind == .imageCube && !state.isDemoProject {
            refreshImageExplorer(datasetID: dataset.id)
        } else if (dataset.kind == .table || dataset.kind == .calibrationTable) && !state.isDemoProject {
            refreshTableBrowser(datasetID: dataset.id)
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

    private func imageExplorerState(datasetID: String) -> ImageExplorerSessionState {
        state.imageExplorers[datasetID] ?? ImageExplorerSessionState(
            datasetID: datasetID,
            selectedView: "plane",
            status: .idle,
            lastError: nil,
            snapshot: nil
        )
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
                        let outputPaths = ([result.requestJSONPath] + genericProducts.map(\.path)).compactMap { $0 }
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
                    affectedPaths = ([result.requestJSONPath] + products.map(\.path)).compactMap { $0 }
                }
                state.history.append(ProcessingHistoryEvent(
                    id: "hist-run-\(state.history.count + 1)",
                    timestamp: currentTimestamp(),
                    title: "\(result.taskID) completed",
                    reason: state.taskRun.requestSummary ?? "User ran \(result.taskID).",
                    affectedPaths: affectedPaths,
                    approval: "user"
                ))
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
