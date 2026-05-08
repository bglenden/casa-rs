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
    public var avgChannel: UInt64?
    public var avgTime: Double?
    public var avgScan: Bool
    public var avgField: Bool
    public var avgBaseline: Bool
    public var avgAntenna: Bool
    public var avgSPW: Bool
    public var scalarAverage: Bool
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
        avgChannel: UInt64? = nil,
        avgTime: Double? = nil,
        avgScan: Bool = false,
        avgField: Bool = false,
        avgBaseline: Bool = false,
        avgAntenna: Bool = false,
        avgSPW: Bool = false,
        scalarAverage: Bool = false,
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
        self.avgChannel = avgChannel
        self.avgTime = avgTime
        self.avgScan = avgScan
        self.avgField = avgField
        self.avgBaseline = avgBaseline
        self.avgAntenna = avgAntenna
        self.avgSPW = avgSPW
        self.scalarAverage = scalarAverage
        self.width = width
        self.height = height
        self.maxPlotPoints = maxPlotPoints
    }
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
                avgchannel: request.avgChannel,
                avgtime: request.avgTime,
                avgscan: request.avgScan,
                avgfield: request.avgField,
                avgbaseline: request.avgBaseline,
                avgantenna: request.avgAntenna,
                avgspw: request.avgSPW,
                scalar: request.scalarAverage,
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
    private let dirtyImagingClient: DirtyImagingTaskClient
    private let plotQueue = DispatchQueue(label: "casars.mac.ms-plot-job", qos: .userInitiated, attributes: .concurrent)
    private let tableBrowserQueue = DispatchQueue(label: "casars.mac.tablebrowser-cell-window", qos: .userInitiated)
    private var activeTaskExecutions: [String: DirtyImagingTaskExecution] = [:]
    private var tableBrowserCellWindowGenerations: [String: Int] = [:]
    private var temporaryDemoProjectRoot: String?

    public init(
        state: WorkbenchState = EmptyWorkbench.makeState(),
        probeClient: ProjectProbeClient = UniFFIProjectProbeClient(),
        demoProjectClient: DemoProjectClient = TutorialDemoProjectClient(),
        plotClient: MeasurementSetPlotClient = UniFFIMeasurementSetPlotClient(),
        imageExplorerClient: ImageExplorerClient = UniFFIImageExplorerClient(),
        tableBrowserClient: TableBrowserClient = UniFFITableBrowserClient(),
        dirtyImagingClient: DirtyImagingTaskClient = ProcessDirtyImagingTaskClient()
    ) {
        self.state = state
        self.probeClient = probeClient
        self.demoProjectClient = demoProjectClient
        self.plotClient = plotClient
        self.imageExplorerClient = imageExplorerClient
        self.tableBrowserClient = tableBrowserClient
        self.dirtyImagingClient = dirtyImagingClient
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
        cleanupTemporaryDemoProject()
        do {
            let probed = try demoProjectClient.createDemoProject()
            temporaryDemoProjectRoot = probed.project.rootPath
            var project = probed.project
            project.datasets = orderedDemoDatasets(project.datasets)
            state = EmptyWorkbench.makeState(interfaceFontSize: interfaceFontSize)
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
            state.lastErrors.append("Open tutorial demo project: \(error)")
        }
    }

    public func openProject(path: String) {
        let interfaceFontSize = state.interfaceFontSize
        cleanupTemporaryDemoProject()
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
        case .runProduct:
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
            avgChannel: plotState.avgChannel,
            avgTime: plotState.avgTime,
            avgScan: plotState.avgScan,
            avgField: plotState.avgField,
            avgBaseline: plotState.avgBaseline,
            avgAntenna: plotState.avgAntenna,
            avgSPW: plotState.avgSPW,
            scalarAverage: plotState.scalarAverage,
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
        case .task:
            if state.isDemoProject {
                openTab(WorkbenchTab(id: "tab-task", title: "Calibrate", kind: .task, datasetID: state.selectedDatasetID))
            } else {
                openDirtyImagingTaskForSelectedDataset()
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

    public func setDirtyImagingCorrelation(_ correlation: String?) {
        guard var parameters = state.dirtyImagingTaskParameters else { return }
        parameters.correlation = normalizedPickerValue(correlation)
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
            nextState.status = .ready
            nextState.lastError = nil
            nextState.snapshot = snapshot
            nextState.cursorX = snapshot.planeCursor?.pixelX ?? nextState.cursorX
            nextState.cursorY = snapshot.planeCursor?.pixelY ?? nextState.cursorY
            nextState.nonDisplayIndices = snapshot.nonDisplayAxes?.map(\.index) ?? nextState.nonDisplayIndices
            if let parameters = snapshot.parameters {
                nextState.parameters = parameters
            }
            nextState.transientCommands = []
            state.imageExplorers[datasetID] = nextState
        } catch {
            var failedState = explorerState
            failedState.status = .failed
            failedState.lastError = "\(error)"
            failedState.snapshot = nil
            state.imageExplorers[datasetID] = failedState
            state.lastErrors.append("Open image explorer for \(dataset.name): \(error)")
        }
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
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func runImageExplorerCommandOnce(_ command: ImageExplorerCommand, datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.transientCommands.append(command)
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
    }

    public func clearImageExplorerRegionCommands(datasetID: String) {
        var explorerState = imageExplorerState(datasetID: datasetID)
        explorerState.regionCommands = []
        explorerState.transientCommands = [.clearRegion]
        state.imageExplorers[datasetID] = explorerState
        refreshImageExplorer(datasetID: datasetID)
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
        if let rowIndex, rowIndex >= 0, let selectedIndex = browserState.snapshot?.verticalMetrics?.selectedIndex {
            changed = appendTableBrowserMove(from: selectedIndex, to: rowIndex, into: &browserState) || changed
        }
        if let selectedVisibleColumn, let targetVisibleColumn {
            let delta = targetVisibleColumn - selectedVisibleColumn
            if delta > 0 {
                browserState.commands.append(.moveRight(steps: delta))
                changed = true
            } else if delta < 0 {
                browserState.commands.append(.moveLeft(steps: -delta))
                changed = true
            }
        }
        guard changed else {
            return
        }
        state.tableBrowsers[datasetID] = browserState
        refreshTableBrowser(datasetID: datasetID)
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
            "avgchannel:\(plotState.avgChannel.map { String($0) } ?? "none")",
            "avgtime:\(plotState.avgTime.map { String($0) } ?? "none")",
            "avgscan:\(plotState.avgScan)",
            "avgfield:\(plotState.avgField)",
            "avgbaseline:\(plotState.avgBaseline)",
            "avgantenna:\(plotState.avgAntenna)",
            "avgspw:\(plotState.avgSPW)",
            "scalar:\(plotState.scalarAverage)",
            "size:960x600",
            "maxPoints:\(plotState.maxPlotPoints)"
        ].joined(separator: "|")
    }

    private static func minimumBoundedMeasurementSetPlotMaxPoints(_ value: UInt64) -> UInt64 {
        max(WorkbenchState.minimumMeasurementSetPlotMaxPoints, value)
    }

    private func defaultDirtyImagingParameters(for dataset: DatasetSummary) -> DirtyImagingTaskParameters {
        let firstField = defaultDirtyImagingField(for: dataset)
        let outputPrefix = defaultDirtyImagingOutputPrefix(for: dataset)
        let isTutorialTWHya = isTWHyaTutorialDataset(dataset)
        return DirtyImagingTaskParameters(
            datasetID: dataset.id,
            measurementSetPath: dataset.path,
            outputPrefix: outputPrefix,
            selectedField: firstField,
            phaseCenterField: firstField,
            selectedSpectralWindow: defaultDirtyImagingSpectralWindow(for: dataset),
            dataColumn: dataset.dataColumns.first ?? "DATA",
            correlation: defaultDirtyImagingCorrelation(for: dataset),
            imageSize: isTutorialTWHya ? 250 : 512,
            cellArcsec: isTutorialTWHya ? 0.1 : 1.0
        )
    }

    private func defaultDirtyImagingField(for dataset: DatasetSummary) -> String? {
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

    private func defaultDirtyImagingSpectralWindow(for dataset: DatasetSummary) -> String? {
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

    private func defaultDirtyImagingCorrelation(for dataset: DatasetSummary) -> String? {
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
            let products = appendProducedDatasets(from: result)
            recordRunProductGroup(from: result, products: products)
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

    private func appendProducedDatasets(from result: DirtyImagingTaskResult) -> [RunProductReference] {
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
                notes: "Produced by \(result.request.runID) from \(result.request.parameters.measurementSetPath).",
                diagnostics: artifact.previewPngExists
                    ? ["preview: \(artifact.previewPngPath ?? "")"]
                    : []
            )
            state.project.datasets.append(fallback)
            products.append(runProductReference(artifact: artifact, datasetID: fallback.id))
        }
        return products
    }

    private func recordRunProductGroup(from result: DirtyImagingTaskResult, products: [RunProductReference]) {
        let parameters = result.request.parameters
        let group = RunProductGroup(
            id: "products-\(result.request.runID)",
            runID: result.request.runID,
            title: "Dirty imaging products",
            sourceDatasetID: parameters.datasetID,
            sourcePath: parameters.measurementSetPath,
            products: products,
            diagnostics: result.diagnostics
        )
        if let index = state.runProductGroups.firstIndex(where: { $0.runID == result.request.runID }) {
            state.runProductGroups[index] = group
        } else {
            state.runProductGroups.append(group)
        }
    }

    private func runProductReference(artifact: DirtyImagingArtifact, datasetID: String?) -> RunProductReference {
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

    private func fallbackDatasetKind(for artifact: DirtyImagingArtifact) -> DatasetKind {
        let kind = artifact.kind.lowercased()
        if kind.contains("table") {
            return .table
        }
        if kind.contains("ms") || kind.contains("measurement") {
            return .measurementSet
        }
        return .imageCube
    }

    private func fallbackDatasetUnits(for artifact: DirtyImagingArtifact) -> String {
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
